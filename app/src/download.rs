use crate::auto_tuning::auto_tune_download;
use crate::command_center::{CommandCenter, CC};
use crate::constants::DOWNLOAD_MULTIPART_THRESHOLD;
use aws_layer::S3Client;
use http_layer::integrity::{reconstructable_multipart_etag, IntegrityAlgorithm};
use http_layer::HyperClient;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedSender};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::{select, task};
use tokio_util::sync::CancellationToken;
use crate::filesystem_checker::is_valid_s3_key_for_local_filesystem;
use util_layer::cc_util::CommandCenterEvent;
use util_layer::constants::METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_DOWNLOAD;
use util_layer::download::{FileCompletionTracker, WriteMsg};
use util_layer::error_codes::UserFacingError;
use util_layer::error_codes::ErrorCode::{DiskIoError, RequestFailure, ServerStateChangeDuringExecution, SkippedInvalidKeysDownload};
use util_layer::retry::retry_on_failure_async;
use util_layer::snow_errors::SnowCopyError;
use util_layer::snowcopy_result::SnowcopyResult;
use util_layer::url_encoder::url_encode_fast;
use util_layer::{DownloadConfig, FileInfo, S3Object};
use SnowCopyError::IntegrityError;

/// Ensures that the parent directory of the given path exists.
/// Uses a cache to avoid redundant filesystem operations.
fn ensure_parent_dirs_exist(file_path: &Path, created_dirs: &mut HashSet<PathBuf>) -> std::io::Result<()> {
    if let Some(parent) = file_path.parent() {
        let parent_buf = parent.to_path_buf();

        // Check if we've already created this directory
        if !created_dirs.contains(&parent_buf) {
            // Create the directory and all its parents if needed
            fs::create_dir_all(&parent_buf)?;
            // Mark this path as created
            created_dirs.insert(parent_buf);
        }
    }
    Ok(())
}

pub async fn download(ctx: Arc<DownloadConfig>) -> SnowcopyResult {
    let cancellation_token = CancellationToken::new();
    let cc = CommandCenter::new_download(&ctx, cancellation_token.clone());
    let (cc_tx, cc_handle) = cc.start();

    let client = Arc::new(S3Client::new(&ctx.endpoint, cc_tx.clone(), ctx.verbose).await);
    let object_channel = client.stream_head_objects_from_bucket_in_separate_task(&ctx.bucket, &ctx.prefix, &ctx.local_path, cancellation_token.clone()).await;
    let mut object_channel = fetch_object_metadata_and_filter_already_downloaded(&ctx, &cc_tx, client, object_channel, cancellation_token.clone()).await;

    let hyper_client = Arc::new(http_layer::build_https_client());
    let (file_sem, req_sem, download_file_concurrency) = auto_tune_download(ctx.clone(), cc_tx.clone());
    let task_sem = Arc::new(Semaphore::new(req_sem.available_permits() * 2));

    let (disk_write_tx, disk_write_rx) = mpsc::channel::<WriteMsg>(2048); //TODO JON: tune + configure buffer size. This matters also for file descriptor limit and memory usage
    spawn_global_writers(disk_write_rx, file_sem.available_permits(), cc_tx.clone(), cancellation_token.clone());

    let mut created_dirs: HashSet<PathBuf> = HashSet::new();
    let mut join_set = JoinSet::new();

    //Process objects from bucket
    while let Some(object) = object_channel.recv().await {
        //Ensure parent directories exist
        if let Err(e) = ensure_parent_dirs_exist(&object.local_path, &mut created_dirs) {
            cc_tx.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(DiskIoError, format!("Failed to create directories for {:?}: {}", object.local_path, e)))).expect("Failed to send message. This should never happen.");
            continue;
        }

        let hyper_client_clone = hyper_client.clone();
        let req_sem = req_sem.clone();
        let task_sem = task_sem.clone();
        let cc_tx = cc_tx.clone();
        let disk_write_tx = disk_write_tx.clone();

        let permit = task_sem.clone().acquire_owned().await.unwrap();
        let config_clone = ctx.clone();
        let cancellation_token = cancellation_token.clone();

        join_set.spawn(async move {
            download_object(hyper_client_clone, config_clone, object, req_sem, cc_tx, download_file_concurrency, disk_write_tx, cancellation_token).await;
            drop(permit);
        });
    }
    //TODO Jon: should we just check for errors here and react to them? But we would still like a general way to deal with errors and present them to the user. It could be that the command center did not care about errors at all. But the functioning program execution only
    //TODO Jon: The object streams could propagate their errors and then we would have a single place that all errors are propagated to.
    //TODO JON: The current design have us calling FATAL_ERROR, but still having to return an error. Its ugly because its hard to understand the error flow.
    while let Some(_) = join_set.join_next().await {};
    drop(disk_write_tx);
    drop(cc_tx);
    cc_handle.await.unwrap()
}

async fn fetch_object_metadata_and_filter_already_downloaded(config: &Arc<DownloadConfig>, cc_tx: &UnboundedSender<CommandCenterEvent>, client: Arc<S3Client>, mut object_channel: Receiver<S3Object>, token: CancellationToken) -> Receiver<S3Object> {
    let (download_tx, download_rx) = mpsc::channel::<S3Object>(METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_DOWNLOAD as usize);

    let checker_client = client.clone();
    let checker_bucket = config.bucket.clone();
    let checker_task_sem = Arc::new(Semaphore::new(200)); //TODO JON: auto tuned?
    let cc_sender = cc_tx.clone();

    tokio::spawn(async move {
        let warning_has_been_sent = Arc::new(AtomicBool::new(false));

        let mut checker_set = JoinSet::new();
        while let Some(object) = object_channel.recv().await {
            if token.is_cancelled() {
                break;
            }
            let cc_sender_clone = cc_sender.clone();

            if !is_valid_s3_key_for_local_filesystem(&object.key){
                cc_sender.send(CommandCenterEvent::Log(format!("WARNING: Skipping invalid S3 key. It cannot be mapped to a local file: {}", &object.key))).expect("Failed to send message. This should never happen.");
                cc_sender.send(CommandCenterEvent::ErrorEvent(UserFacingError::new(SkippedInvalidKeysDownload, format!("One or more S3 objects skipped, as they cannot be mapped to a local file: {}", &object.key)))).expect("Failed to send message. This should never happen.");
                continue;
            }
            let _ = cc_sender.send(CommandCenterEvent::FileDiscovered(object.size as u64));

            let client = checker_client.clone();
            let bucket = checker_bucket.clone();
            let download_tx = download_tx.clone();
            let permit = checker_task_sem.clone().acquire_owned().await.unwrap();
            let warning_has_been_sent = warning_has_been_sent.clone();

            checker_set.spawn(async move {
                let metadata = client.get_s3_metadata(&bucket, &object.key).await;

                match metadata {
                    Ok((exists_at_target, _, mtime, chunk_size, etag, sha256)) => {
                        drop(permit);
                        if !exists_at_target {
                            cc_sender_clone.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(ServerStateChangeDuringExecution, format!("S3 object went missing from s3 server during the execution of the program: {}", object.key)))).expect("Failed to send message. This should never happen.");
                            return;
                        }
                        let mut object = object;
                        object.modified_time = mtime;
                        object.chunk_size = chunk_size;
                        object.etag = etag;
                        object.sha256_checksum = sha256;
                        object.aws_uri_encoded_key = url_encode_fast(&object.key, true);

                        let should_download = should_download_object(&mut object);

                        if should_download {
                            if object.chunk_size.is_none() && object.sha256_checksum.is_none() {
                                let already_sent = warning_has_been_sent.swap(true, Ordering::Relaxed);
                                if !already_sent {
                                    cc_sender_clone.send(CommandCenterEvent::Log("Lack of Snowcopy file metadata detected for some files - file integrity checks are reduced".to_string())).expect("Failed to send message. This should never happen.");
                                }
                            }

                            if let Err(e) = download_tx.send(object).await {
                                cc_sender_clone.send(CommandCenterEvent::Log(format!("Failed to forward object to download queue: {}", e))).expect("Failed to send message. This should never happen.");
                            }
                        } else {
                            // Already downloaded, just update progress for the UI
                            cc_sender_clone.send(CommandCenterEvent::FileAlreadyAtTarget(object.size as u64)).expect("Failed to send message. This should never happen.");
                        }

                    }
                    Err(e) => {
                        cc_sender_clone.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(RequestFailure, format!("Failed to get metadata for object: {}. Error: {}", object.key, e)))).expect("Failed to send message. This should never happen.");
                        return;
                    }
                }
            });
        }
        while let Some(_) = checker_set.join_next().await {}
    });
    download_rx
}

fn should_download_object(object: &mut S3Object) -> bool {
    let result = fs::metadata(&object.local_path);

    let needs_download = if let Ok(metadata) = result {
        if metadata.len() == object.size as u64 {
            if let Some(s3_mtime) = object.modified_time {
                if let Ok(local_mtime) = metadata.modified() {
                    if local_mtime == s3_mtime {
                        false // local copy matches
                    } else {
                        true
                    }
                } else {
                    true
                }
            } else {
                true
            }
        } else {
            true
        }
    } else {
        true
    };
    needs_download
}

async fn download_object(pool_client: Arc<HyperClient>, config: Arc<DownloadConfig>, object: S3Object, req_sem: Arc<Semaphore>, cc_sender: UnboundedSender<CommandCenterEvent>, download_file_concurrency: usize, disk_write_tx: Sender<WriteMsg>, cancellation_token: CancellationToken) {
    let multipart_integrity_verification = !config.md5_checksum_integrity_is_disabled && reconstructable_multipart_etag(&object);

    if (object.size > DOWNLOAD_MULTIPART_THRESHOLD as i64 && !object.sha256_checksum.is_none()) || multipart_integrity_verification { //if sha256_checksum is present without multipart-etag download the simple way
        let file_path = object.local_path.clone();
        let key = object.aws_uri_encoded_key.clone();
        let total_size = object.size as u64;

        let file = match File::create(&file_path) {
            Ok(f) => f,
            Err(e) => {
                cc_sender.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(DiskIoError, format!("Failed to create file {:?}: {}", file_path, e)))).expect("Failed to send message. This should never happen.");
                return;
            }
        };

        if let Err(e) = file.set_len(total_size) {
            cc_sender.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(DiskIoError, format!("Failed to truncate file {:?} to {}: {}", file_path, total_size, e)))).expect("Failed to send message. This should never happen.");
            return;
        }

        let file = Arc::new(file);

        let ideal_chunk_size = total_size.div_ceil(download_file_concurrency as u64);
        let mut chunk_size = std::cmp::max(ideal_chunk_size, DOWNLOAD_MULTIPART_THRESHOLD);
        if multipart_integrity_verification {
            chunk_size = object.chunk_size.unwrap() as u64;
        } else {
            cc_sender.send(CommandCenterEvent::Log(format!("Skipping integrity check for file: {}, etag: {}", key, object.etag.clone().unwrap()))).expect("Failed to send message"); //TODO JON: add a deeper log level
        }
        let num_parts = (total_size + chunk_size - 1) / chunk_size;
        //TODO JON: if we are validating integrity, then the etag suffix is the number of parts. We can fail early here if there is a mismatch between the number of parts and the expected number of parts based on the etag suffix. Not sure how we should handle that case, though

        let hash_algo = if multipart_integrity_verification {
            IntegrityAlgorithm::MD5
        } else {
            IntegrityAlgorithm::None
        };

        let integrity_result = download_files_in_part_and_check_integrity(&pool_client, &config, &object, &req_sem, &cc_sender, &disk_write_tx, multipart_integrity_verification, &key, total_size, file.clone(), chunk_size, num_parts, hash_algo.clone(), cancellation_token.clone()).await;

        if let Err(IntegrityError(msg)) = integrity_result {
            cc_sender.send(CommandCenterEvent::Log(format!("Failed to verify {}: {}. Attempting again as it might have been corrupted during transfer.", object.key, msg))).expect("Failed to send message. This should never happen.");

            let integrity_result = download_files_in_part_and_check_integrity(&pool_client, &config, &object, &req_sem, &cc_sender, &disk_write_tx, multipart_integrity_verification, &key, total_size, file, chunk_size, num_parts, hash_algo, cancellation_token).await;

            if let Err(e) = integrity_result {
                cc_sender.send(CommandCenterEvent::Log(format!("Failed to verify {}: {}. Its likely that there is something with the metadata, the network or the s3 storage system.", object.key, e))).expect("Failed to send message. This should never happen.");
                //TODO JON: exit with different exit code, when we are done. When we figure out how to cancel / exit the program. I would assume that we send this info to the command center
            }

            return;
        }
    } else {
        //simple download
        let file_info = Arc::new(FileInfo {
            file_path: object.local_path.clone(),
            aws_uri_encoded_key: object.aws_uri_encoded_key.clone(),
            key: object.key.clone(),
            file_size: object.size as u64,
            modified_time: object.modified_time,
        });

        let canonical_uri = Arc::new(format!("/{}/{}", &config.bucket, object.aws_uri_encoded_key));
        let query_uri = String::new();

        let integrity_result = download_full_file_and_check_integrity(pool_client.clone(), config.clone(), object.clone(), req_sem.clone(), cc_sender.clone(), disk_write_tx.clone(), file_info.clone(), canonical_uri.clone(), query_uri.clone(), cancellation_token.clone()).await;
        if let Err(IntegrityError(msg)) = integrity_result {
            cc_sender.send(CommandCenterEvent::Log(format!("Failed to verify {}: {}. Attempting again as it might have been corrupted during transfer.", object.key, msg))).expect("Failed to send message. This should never happen.");

            let integrity_result = download_full_file_and_check_integrity(pool_client, config, object.clone(), req_sem, cc_sender.clone(), disk_write_tx, file_info, canonical_uri, query_uri, cancellation_token).await;

            if let Err(e) = integrity_result {
                cc_sender.send(CommandCenterEvent::Log(format!("Failed to verify {}: {}. Its likely that there is something with the metadata, the network or the s3 storage system.", object.key, e))).expect("Failed to send message. This should never happen.");
                //TODO JON: exit with different exit code, when we are done. When we figure out how to cancel / exit the program. I would assume that we send this info to the command center
            }

            return;
        }
    }
}

async fn download_full_file_and_check_integrity(pool_client: Arc<HyperClient>, config: Arc<DownloadConfig>, object: S3Object, req_sem: Arc<Semaphore>, cc_sender: UnboundedSender<CommandCenterEvent>, disk_write_tx: Sender<WriteMsg>, file_info: Arc<FileInfo>, canonical_uri: Arc<String>, query_uri: String, cancellation_token: CancellationToken) -> Result<(), SnowCopyError> {
    let cc_sender_clone = cc_sender.clone();
    let retry_cc_sender = cc_sender.clone();
    let result = select! {
        _ = cancellation_token.cancelled() => {
            return Err(SnowCopyError::Canceled);
        },

        result = retry_on_failure_async(move || {
            let config = config.clone();
            let file_info = file_info.clone();
            let pool_client = pool_client.clone();
            let canonical_uri = canonical_uri.clone();
            let query_uri = query_uri.clone();
            let req_sem = req_sem.clone();
            let disk_write_tx = disk_write_tx.clone();
            let cc_sender_clone = cc_sender_clone.clone();

            async move {
                http_layer::download::get_file(config, file_info, &pool_client, &canonical_uri, &query_uri, req_sem, disk_write_tx, cc_sender_clone).await
            }
        }, retry_cc_sender,format!("Downloading {}", object.key)) => result
    };

    match result {
        Ok(hash) => {
            if let Some(sha256) = hash {
                if object.sha256_checksum.is_some() {
                    let sha256_expected = object.sha256_checksum.unwrap();
                    if sha256_expected != sha256 {
                        let error_msg = format!(
                            "Integrity check failed for {}. Expected {}, got {}",
                            object.key, sha256_expected, sha256
                        );
                        cc_sender.send(CommandCenterEvent::Log(error_msg.clone())).expect("Failed to send message");
                        return Err(IntegrityError(error_msg));
                    } else {
                        cc_sender.send(CommandCenterEvent::FileIntegrityChecked(true)).expect("Failed to send message");
                    }
                } else {
                    cc_sender.send(CommandCenterEvent::Log(format!("Skipping integrity check for file with no SHA256 checksum: {}", object.key))).expect("Failed to send message"); //TODO JON: add a deeper log level
                    //silence, as we don't have info to do the check
                }
            }
        }
        Err(e) => {
            return Err(e);
        }
    }

    Ok(())
}

async fn download_files_in_part_and_check_integrity(pool_client: &Arc<HyperClient>, config: &Arc<DownloadConfig>, object: &S3Object, req_sem: &Arc<Semaphore>, cc_sender: &UnboundedSender<CommandCenterEvent>, disk_write_tx: &Sender<WriteMsg>, multipart_integrity_verification: bool, key: &String, total_size: u64, file: Arc<File>, chunk_size: u64, num_parts: u64, hash_algo: IntegrityAlgorithm, cancellation_token: CancellationToken) -> Result<(), SnowCopyError> {
    let mut download_set = download_file_in_parts(&pool_client, &config, &object, &req_sem, &cc_sender, &disk_write_tx, &config.bucket, &key, total_size, file, chunk_size, num_parts, hash_algo, cancellation_token);

    let mut part_hashes = vec![None; num_parts as usize];
    while let Some(res) = download_set.join_next().await {
        match res {
            Ok(Ok((part_index, _bytes, hash))) => {
                part_hashes[part_index as usize] = hash; //Maintain part order to ensure correct integrity verification
            }
            Ok(Err(SnowCopyError::Canceled)) => {
                return Ok(()); //TODO JON: What is the best option here?
            }
            Ok(Err(e)) => {
                panic!("Part download failed for {}: {}", key, e);
            }
            Err(e) => {
                panic!("Join error for {}: {}", key, e);
            }
        }
    }

    if multipart_integrity_verification {
        let part_md5s: Vec<Vec<u8>> = part_hashes
            .into_iter()
            .map(|h| {
                hex::decode(h.expect("All parts must have a hash if integrity verification is enabled"))
                    .expect("Hash should be valid hex")
            })
            .collect();

        let reconstructed_etag = http_layer::integrity::reconstruct_multipart_etag(&part_md5s);
        let expected_etag = object.etag.as_ref().map(|e| e.trim_matches('"')).unwrap_or("");

        if reconstructed_etag != expected_etag {
            let error_msg = format!(
                "Integrity check failed for {}. Expected {}, got {}",
                key, expected_etag, reconstructed_etag
            );
            cc_sender.send(CommandCenterEvent::Log(error_msg.clone())).expect("Failed to send message");
            return Err(IntegrityError(error_msg));
        } else {
            cc_sender.send(CommandCenterEvent::FileIntegrityChecked(true)).expect("Failed to send message");
        }
    }

    Ok(())
}

fn download_file_in_parts(pool_client: &Arc<HyperClient>, config: &Arc<DownloadConfig>, object: &S3Object, req_sem: &Arc<Semaphore>, cc_sender: &UnboundedSender<CommandCenterEvent>, disk_write_tx: &Sender<WriteMsg>, bucket: &String, key: &String, total_size: u64, file: Arc<File>, chunk_size: u64, num_parts: u64, hash_algo: IntegrityAlgorithm, cancellation_token: CancellationToken) -> JoinSet<Result<(u64, u64, Option<String>), SnowCopyError>> {
    let tracker = Arc::new(FileCompletionTracker {
        pending_bytes_to_be_written: std::sync::atomic::AtomicU64::new(total_size),
        mtime: object.modified_time,
        total_size,
    });

    let mut download_set = JoinSet::new();
    let canonical_uri = Arc::new(format!("/{}/{}", bucket, key));

    for i in 0..num_parts {
        let start = i * chunk_size;
        let end = std::cmp::min(start + chunk_size - 1, total_size - 1);

        let config = config.clone();
        let pool_client = pool_client.clone();
        let req_sem = req_sem.clone();
        let canonical_uri = canonical_uri.clone();
        //let query_uri = String::new(); //TODO JON: when would we need this?
        let file = file.clone();
        let disk_write_tx = disk_write_tx.clone();
        let tracker = tracker.clone();
        let hash_algo = hash_algo.clone();

        let cc_sender = cc_sender.clone();
        let cancellation_token = cancellation_token.clone();
        download_set.spawn(async move {
            let res = select! {
                res = retry_on_failure_async(|| {
                    let config = config.clone();
                    let pool_client = pool_client.clone();
                    let req_sem = req_sem.clone();
                    let canonical_uri = canonical_uri.clone();
                    let query_uri = String::new();
                    let file = file.clone();
                    let disk_write_tx = disk_write_tx.clone();
                    let tracker = tracker.clone();
                    let hash_algo = hash_algo.clone();
                    let cc_sender = cc_sender.clone();

                    async move {
                        http_layer::download::download_file_range(start, end, config, &pool_client, req_sem, &canonical_uri, &query_uri, file, disk_write_tx, tracker, hash_algo, cc_sender).await
                    }
                }, cc_sender.clone(),format!("Downloading part (bytes {} to {}) of {}", start, end, canonical_uri)) => res,

                _ = cancellation_token.cancelled() => {
                    return Err(SnowCopyError::Canceled);
                }
            };

            res.map(|(bytes, hash)| (i, bytes, hash))
        });
    }
    download_set
}


pub fn spawn_global_writers(
    rx: Receiver<WriteMsg>,
    num_writers: usize,
    cc_sender: UnboundedSender<CommandCenterEvent>,
    token: CancellationToken
) {
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    for _ in 0..num_writers {
        let rx = rx.clone();
        let cc_sender = cc_sender.clone();
        let token = token.clone();

        task::spawn_blocking(move || {
            loop {
                if token.is_cancelled() {
                    break;
                }

                let msg = {
                    let mut rx_lock = rx.blocking_lock();
                    rx_lock.blocking_recv()
                };

                match msg {
                    Some((file, offset, data, tracker)) => {
                        if let Err(e) = &file.write_at_offset(&data, offset) {
                            eprintln!("write error: {}", e); //TODO JON: Should we just retry? But what kind of local IO error would benefit from that?
                            //probably panic?
                        } else {
                            tracker.complete_write(&file, data.len(), cc_sender.clone());
                            cc_sender.send(CommandCenterEvent::PartTransferred(data.len() as u64)).unwrap();
                        }
                    }
                    None => break,
                }
            }
        });
    }
}


#[cfg(unix)]
impl WriteAt for std::fs::File {
    fn write_at_offset(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        use std::os::unix::fs::FileExt;
        self.write_at(buf, offset)
    }
}

#[cfg(windows)]
impl WriteAt for File {
    fn write_at_offset(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        use std::os::windows::fs::FileExt;
        self.seek_write(buf, offset)
    }
}

trait WriteAt { //Abstract to make it more testable
    fn write_at_offset(&self, buf: &[u8], offset: u64) -> std::io::Result<usize>;
}