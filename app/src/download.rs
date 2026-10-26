use crate::auto_tuning::auto_tune_download;
use crate::command_center::{CC, CommandCenter};
use crate::constants::DOWNLOAD_MULTIPART_THRESHOLD;
use crate::download_multipart::MultipartDownload;
use crate::download_singlepart::SinglepartDownload;
use crate::filesystem_checker::is_valid_s3_key_for_local_filesystem;
use crate::traits::{DownloadRuntime, DownloadTrait};
use aws_layer::S3Client;
use http_layer::integrity::reconstructable_multipart_etag;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{self, Receiver, UnboundedSender};
use tokio::task::{JoinHandle, JoinSet};
use tokio::task;
use tokio_util::sync::CancellationToken;
use util_layer::cc_util::CommandCenterEvent;
use util_layer::constants::METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_DOWNLOAD;
use util_layer::download::WriteMsg;
use util_layer::error_codes::ErrorCode::{DiskIoError, RequestFailure, ServerStateChangeDuringExecution, SkippedInvalidKeysDownload};
use util_layer::error_codes::UserFacingError;
use util_layer::snowcopy_result::SnowcopyResult;
use util_layer::traits::{HttpExecutor, HyperHttpExecutor};
use util_layer::url_encoder::url_encode_fast;
use util_layer::{DownloadConfig, S3Object};

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

pub async fn run_download(ctx: Arc<DownloadConfig>) -> SnowcopyResult {
    let cancellation_token = CancellationToken::new();
    let cc = CommandCenter::new_download(&ctx, cancellation_token.clone());
    let (cc_tx, cc_handle) = cc.start();

    let client = Arc::new(S3Client::new(&ctx.endpoint, cc_tx.clone(), ctx.verbose).await);
    let hyper_http_executor = Arc::new(HyperHttpExecutor::new(Arc::new(http_layer::build_https_client())));

    download(ctx, client, cc_tx, cc_handle, cancellation_token, hyper_http_executor).await
}

pub async fn download(
    ctx: Arc<DownloadConfig>,
    client: Arc<S3Client>,
    cc_tx: UnboundedSender<CommandCenterEvent>,
    cc_handle: JoinHandle<SnowcopyResult>,
    cancellation_token: CancellationToken,
    http_executor: Arc<dyn HttpExecutor>,
) -> SnowcopyResult {
    let (file_sem, req_sem, download_file_concurrency) = auto_tune_download(ctx.clone(), cc_tx.clone());
    let task_sem = Arc::new(Semaphore::new(req_sem.available_permits() * 2));
    let (disk_write_tx, disk_write_rx) = mpsc::channel::<WriteMsg>(2048); //TODO JON: tune + configure buffer size. This matters also for file descriptor limit and memory usage

    spawn_global_writers(disk_write_rx, file_sem.available_permits(), cc_tx.clone(), cancellation_token.clone());

    let object_channel = client.stream_head_objects_from_bucket_in_separate_task(&ctx.bucket, &ctx.prefix, &ctx.local_path, cancellation_token.clone()).await;
    let mut object_channel = fetch_object_metadata_and_filter_already_downloaded(&ctx, &cc_tx, client, object_channel, cancellation_token.clone()).await;

    let runtime = Arc::new(DownloadRuntime {
        req_sem,
        disk_write_tx: disk_write_tx.clone(),
        cc_tx: cc_tx.clone(),
        cancellation_token,
        download_file_concurrency,
    });

    download_objects_from_stream(ctx, &mut object_channel, task_sem, runtime, http_executor).await;

    drop(disk_write_tx);
    drop(cc_tx);
    cc_handle.await.unwrap()
}

async fn download_objects_from_stream(
    ctx: Arc<DownloadConfig>,
    object_channel: &mut Receiver<S3Object>,
    task_sem: Arc<Semaphore>,
    runtime: Arc<DownloadRuntime>,
    http_executor: Arc<dyn HttpExecutor>,
) {
    let mut created_dirs: HashSet<PathBuf> = HashSet::new();
    let mut join_set = JoinSet::new();

    // Process objects from bucket
    while let Some(object) = object_channel.recv().await {
        // Ensure parent directories exist
        if let Err(e) = ensure_parent_dirs_exist(&object.local_path, &mut created_dirs) {
            runtime.cc_tx.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(
                DiskIoError,
                format!("Failed to create directories for {:?}: {}", object.local_path, e),
            ))).expect("Failed to send message. This should never happen.");
            continue;
        }

        let permit = task_sem.clone().acquire_owned().await.unwrap();
        let ctx = ctx.clone();
        let runtime = runtime.clone();
        let http_executor = http_executor.clone();
        let object_arc = Arc::new(object);

        join_set.spawn(async move {
            download_object(ctx, runtime, http_executor, object_arc).await;
            drop(permit);
        });
    }

    //TODO Jon: should we just check for errors here and react to them? But we would still like a general way to deal with errors and present them to the user. It could be that the command center did not care about errors at all. But the functioning program execution only
    //TODO Jon: The object streams could propagate their errors and then we would have a single place that all errors are propagated to.
    //TODO JON: The current design have us calling FATAL_ERROR, but still having to return an error. Its ugly because its hard to understand the error flow.
    while let Some(_) = join_set.join_next().await {}
}

async fn download_object(
    ctx: Arc<DownloadConfig>,
    runtime: Arc<DownloadRuntime>,
    http_executor: Arc<dyn HttpExecutor>,
    object: Arc<S3Object>,
) {
    let multipart_integrity_verification = ctx.md5_checksum_integrity && reconstructable_multipart_etag(&object);
    let checksum_sha256_not_none = object.sha256_checksum.is_some();

    if (object.size > DOWNLOAD_MULTIPART_THRESHOLD as i64 && checksum_sha256_not_none) || multipart_integrity_verification { //if sha256_checksum is present without multipart-etag download the simple way
        MultipartDownload::new(ctx, runtime, http_executor, object, multipart_integrity_verification)
            .download_file()
            .await;
    } else {
        //simple download
        SinglepartDownload::new(ctx, runtime, http_executor, object)
            .download_file()
            .await;
    }
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