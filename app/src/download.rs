use crate::auto_tuning::auto_tune_semaphores;
use crate::command_center::{CommandCenter, CC};
use crate::constants::DOWNLOAD_MULTIPART_THRESHOLD;
use anyhow::Error;
use aws_layer::{async_head_objects_from_bucket, get_s3_metadata, AWSClient};
use http_layer::{BufferPool, PoolClient};
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedSender};
use tokio::sync::Semaphore;
use util_layer::constants::METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_DOWNLOAD;
use util_layer::download::{FileCompletionTracker, WriteMsg};
use util_layer::retry::retry_on_failure_async;
use util_layer::snow_errors::SnowCopyError;
use util_layer::{build_context, FileInfo, SnowContext, S3Object};

use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::task::JoinSet;
use http_layer::integrity::{reconstructable_multipart_etag, IntegrityAlgorithm};
use util_layer::cc_util::CommandCenterEvent;

/// Ensures that the parent directory of the given path exists.
/// Uses a cache to avoid redundant filesystem operations.
fn ensure_parent_dirs_exist(file_path: &Path, created_dirs: &mut HashSet<std::path::PathBuf>) -> std::io::Result<()> {
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

pub async fn download(bucket: Arc<String>, prefix: &str, dest: &Path, file_limit: Option<usize>, req_limit: Option<usize>, download_concurrency: Option<usize>, silent: bool, stats: bool, config: &util_layer::Config, md5_checksum_integrity: bool) {
    let cc = CommandCenter::new(silent, stats);
    let (cc_tx, cc_handle) = cc.start();

    let client = Arc::new(aws_layer::create_client(config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str(), config.s3_region.as_str()).await);
    let object_channel = async_head_objects_from_bucket(&bucket, &prefix, &dest, client.clone(), cc_tx.clone()).await;
    let mut download_rx = fetch_object_metadata_and_filter_already_downloaded(&bucket, &cc_tx, client, object_channel).await;

    let pool_client = Arc::new(http_layer::build_https_client());
    let snow_context = Arc::new(build_context(config.clone(), md5_checksum_integrity));
    let (file_sem, req_sem, _buffer_pool_size, download_file_concurrency) = auto_tune_semaphores(file_limit, req_limit, download_concurrency, dest, cc_tx.clone(), true);
    let task_sem = Arc::new(Semaphore::new(req_sem.available_permits() * 2));

    let mut created_dirs: HashSet<PathBuf> = HashSet::new();
    let mut join_set = JoinSet::new();

    let (disk_write_tx, disk_write_rx) = mpsc::channel::<WriteMsg>(2048); //TODO JON: tune + configure buffer size. This matters also for file descriptor limit
    http_layer::download::spawn_global_writers(disk_write_rx, file_sem.available_permits(), cc_tx.clone());

    //Process objects from bucket
    while let Some(object) = download_rx.recv().await {
        //Ensure parent directories exist
        if let Err(e) = ensure_parent_dirs_exist(&object.local_path, &mut created_dirs) {
            eprintln!("Failed to create directories for {:?}: {}", object.local_path, e);
            continue;
        }

        let bucket = bucket.clone();
        let pool_client = pool_client.clone();
        let snow_context = snow_context.clone();
        let req_sem = req_sem.clone();
        let task_sem = task_sem.clone();
        let cc_tx = cc_tx.clone();
        let disk_write_tx = disk_write_tx.clone();

        let permit = task_sem.clone().acquire_owned().await.unwrap();
        join_set.spawn(async move {
            download_object(bucket, pool_client, snow_context, object, req_sem, cc_tx, download_file_concurrency, disk_write_tx).await;
            drop(permit);
        });
    }

    while let Some(_) = join_set.join_next().await {}; //TODO Jon: Do I like this more than the join_all in upload?
    drop(disk_write_tx);
    drop(cc_tx);
    cc_handle.await.unwrap();
}

async fn fetch_object_metadata_and_filter_already_downloaded(bucket: &Arc<String>, cc_tx: &UnboundedSender<CommandCenterEvent>, client: Arc<AWSClient>, mut object_channel: Receiver<S3Object>) -> Receiver<S3Object> {
    //TODO JON in this PR: we should give a small warning if we find objects without enough metadata for verification
    let (download_tx, download_rx) = mpsc::channel::<S3Object>(METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_DOWNLOAD as usize);

    let checker_client = client.clone();
    let checker_bucket = bucket.clone();
    let checker_task_sem = Arc::new(Semaphore::new(200));
    let cc_tx_checker = cc_tx.clone();

    tokio::spawn(async move {
        let mut checker_set = JoinSet::new();
        while let Some(object) = object_channel.recv().await {
            let client = checker_client.clone();
            let bucket = checker_bucket.clone();
            let download_tx = download_tx.clone();
            let permit = checker_task_sem.clone().acquire_owned().await.unwrap();
            let cc_tx = cc_tx_checker.clone();

            checker_set.spawn(async move {
                let (_, _, mtime, chunk_size, etag) = get_s3_metadata(&client, &bucket, &object.key).await;
                let mut object = object;
                object.modified_time = mtime;
                object.chunk_size = chunk_size;
                object.etag = etag;

                let should_download = should_download_object(&mut object);

                if should_download {
                    if let Err(e) = download_tx.send(object).await {
                        cc_tx.send(CommandCenterEvent::Log(format!("Failed to forward object to download queue: {}", e))).expect("Failed to send message. This should never happen.");
                    }
                } else {
                    // Already downloaded, just update progress for the UI
                    cc_tx.send(CommandCenterEvent::FileAlreadyAtTarget(object.size as u64)).expect("Failed to send message. This should never happen.");
                }
                drop(permit);
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

async fn download_object(bucket: Arc<String>, pool_client: Arc<PoolClient>, snow_context: Arc<SnowContext>, object: S3Object,  req_sem: Arc<Semaphore>, cc_sender: UnboundedSender<CommandCenterEvent>, download_file_concurrency: usize, disk_write_tx: Sender<WriteMsg>) {
    let multipart_integrity_verification = snow_context.md5_checksum_itegrity_check() && reconstructable_multipart_etag(&object);

    if object.size > DOWNLOAD_MULTIPART_THRESHOLD as i64 || multipart_integrity_verification {
        let file_path = object.local_path.clone();
        let bucket = bucket.clone();
        let key = object.key.clone();
        let total_size = object.size as u64;

        let file = match File::create(&file_path) {
            Ok(f) => f,
            Err(e) => {
                cc_sender.send(CommandCenterEvent::Log(format!("Failed to create file {:?}: {}", file_path, e))).expect("Failed to send message. This should never happen.");
                return; //TODO JON: panic?
            }
        };

        if let Err(e) = file.set_len(total_size) {
            cc_sender.send(CommandCenterEvent::Log(format!("Failed to truncate file {:?} to {}: {}", file_path, total_size, e))).expect("Failed to send message. This should never happen.");
            return; //TODO JON: panic
        }

        let file = Arc::new(file);

        let ideal_chunk_size = total_size.div_ceil(download_file_concurrency as u64);
        let mut chunk_size = std::cmp::max(ideal_chunk_size, DOWNLOAD_MULTIPART_THRESHOLD);
        if multipart_integrity_verification {
            chunk_size = object.chunk_size.unwrap() as u64;
        }
        let num_parts = (total_size + chunk_size - 1) / chunk_size;
        //TODO JON: if we are validating integrity, then the etag suffix is the number of parts. We can fail early here if there is a mismatch between the number of parts and the expected number of parts based on the etag suffix. Not sure how we should handle that case, though

        let tracker = Arc::new(FileCompletionTracker {
            pending_bytes_to_be_written: std::sync::atomic::AtomicU64::new(total_size),
            mtime: object.modified_time,
            total_size,
            cc_sender: cc_sender.clone(),
        });

        let hash_algo = if multipart_integrity_verification {
            IntegrityAlgorithm::MD5
        } else {
            IntegrityAlgorithm::None
        };

        let mut download_set = JoinSet::new();
        let canonical_uri = Arc::new(format!("/{}/{}", &bucket, key));

        for i in 0..num_parts {
            let start = i * chunk_size;
            let end = std::cmp::min(start + chunk_size - 1, total_size - 1);

            let snow_context = snow_context.clone();
            let pool_client = pool_client.clone();
            let req_sem = req_sem.clone();
            let canonical_uri = canonical_uri.clone();
            //let query_uri = String::new(); //TODO JON: when would we need this?
            let file = file.clone();
            let disk_write_tx = disk_write_tx.clone();
            let tracker = tracker.clone();
            let hash_algo = hash_algo.clone();

            let cc_sender = cc_sender.clone();
            download_set.spawn(async move {
                let res = retry_on_failure_async(|| {
                    let snow_context = snow_context.clone();
                    let pool_client = pool_client.clone();
                    let req_sem = req_sem.clone();
                    let canonical_uri = canonical_uri.clone();
                    let query_uri = String::new();
                    let file = file.clone();
                    let disk_write_tx = disk_write_tx.clone();
                    let tracker = tracker.clone();
                    let hash_algo = hash_algo.clone();

                    async move {
                        http_layer::download::download_file_range(start, end, snow_context, &pool_client, req_sem, canonical_uri, &query_uri, file, disk_write_tx, tracker, hash_algo).await
                    }
                }, cc_sender.clone(), |e: &anyhow::Error| {
                    if let Some(snow_error) = e.downcast_ref::<SnowCopyError>() {
                        return snow_error.is_retryable();
                    }
                    false
                }, format!("Downloading part (bytes {} to {}) of {}", start, end, canonical_uri)).await;
                res.map(|(bytes, hash)| (i, bytes, hash))
            });
        }

        let mut part_hashes = vec![None; num_parts as usize];
        while let Some(res) = download_set.join_next().await {
            match res {
                Ok(Ok((i, _bytes, hash))) => {
                    part_hashes[i as usize] = hash;
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
                cc_sender.send(CommandCenterEvent::Log(format!(
                    "Integrity check failed for {}. Expected {}, got {}",
                    key, expected_etag, reconstructed_etag
                ))).expect("Failed to send message");
                // TODO Jon: how to handle failure? Retry this download a single time and if it fails again, then something is probably wrong with the metadata. That I'm unsure how to handle
            } else {
                cc_sender.send(CommandCenterEvent::Log(format!(
                    "Integrity check passed for {}",
                    key
                ))).expect("Failed to send message");
            }
        }
    } else {
        //simple download
        let file_info = Arc::new(FileInfo {
            file_path: object.local_path.clone(),
            key: object.key.clone(),
            file_size: object.size as u64,
            modified_time: object.modified_time,
        });

        let canonical_uri = Arc::new(format!("/{}/{}", &bucket, object.key));
        let query_uri = String::new();

        let cc_sender_retry = cc_sender.clone();
        let result = retry_on_failure_async(move || {
            let snow_context = snow_context.clone();
            let file_info = file_info.clone();
            let pool_client = pool_client.clone();
            let canonical_uri = canonical_uri.clone();
            let query_uri = query_uri.clone();
            let req_sem = req_sem.clone();
            let disk_write_tx = disk_write_tx.clone();
            let cc_sender = cc_sender_retry.clone();
            async move {
                http_layer::download::get_file(snow_context, file_info, &pool_client, canonical_uri, &query_uri, req_sem, disk_write_tx, cc_sender).await
            }
        }, cc_sender.clone(), |e: &anyhow::Error| {
            if let Some(snow_error) = e.downcast_ref::<SnowCopyError>() {
                return snow_error.is_retryable();
            }
            false
        }, format!("Downloading {}", object.key)).await;

        if let Err(e) = result {
            cc_sender.send(CommandCenterEvent::Log(format!("Failed to download {}: {}", object.key, e))).unwrap();
        }
    }
}

#[allow(dead_code)]
async fn download_part_buffered(start: u64, end: u64, snow_context: Arc<SnowContext>, pool_client: &Arc<PoolClient>, req_sem: Arc<Semaphore>, file_sem: Arc<Semaphore>, canonical_uri: Arc<String>, query_uri: &String, file_path: &PathBuf, buffer_pool: Arc<BufferPool>) -> Result<u64, Error> {
    let data = http_layer::get_file_range(snow_context, &pool_client, buffer_pool, canonical_uri, &query_uri, req_sem, start, end).await?;

    let _file_permit = file_sem.acquire().await.expect("Should never fail");
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .await?;

    file.seek(std::io::SeekFrom::Start(start)).await?;
    file.write_all(data.as_ref()).await?;
    file.flush().await?; //We must flush to ensure our mtime update is not overwritten after its been set.
    Ok(data.len as u64)
}

#[allow(dead_code)]
fn update_mod_time_on_file(object: &S3Object, cc_sender: &UnboundedSender<CommandCenterEvent>) { //TODO JON: should we retry? I think this should never fail, in practice, unless some other program takes write of the file and then we dont want to update mtime anyways
    if let Some(mtime) = object.modified_time {
        match fs::OpenOptions::new().write(true).open(&object.local_path) {
            Ok(file) => {
                if let Err(e) = file.set_modified(mtime) {
                    cc_sender.send(CommandCenterEvent::Log(format!("Failed to update mtime for {:?}: {}", object.local_path, e))).expect("Failed to send message. This should never happen.");
                }
            }
            Err(e) => {
                cc_sender.send(CommandCenterEvent::Log(format!("Failed to open file to update mtime for {:?}: {}", object.local_path, e))).expect("Failed to send message. This should never happen.");
            }
        }
    }
}
