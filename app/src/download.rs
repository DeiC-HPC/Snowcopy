use crate::auto_tuning::auto_tune_semaphores;
use crate::command_center::{CommandCenter, CC};
use crate::constants::DOWNLOAD_MULTIPART_THRESHOLD;
use aws_layer::{async_head_objects_from_bucket, get_s3_mtime, AWSClient, S3Object};
use http_layer::{BufferPool, PoolClient};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, UnboundedSender};
use tokio::sync::Semaphore;
use util_layer::{build_context, constants, FileInfo, SnowContext};

use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::task::JoinSet;
use util_layer::cc_util::{CcLoadAndTransfer, CommandCenterEvent};

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

pub async fn download(bucket: Arc<String>, prefix: &str, dest: &Path, file_limit: Option<usize>, req_limit: Option<usize>, silent: bool, stats: bool, config: &util_layer::Config) {
    let cc = CommandCenter::new(silent, stats);
    let (cc_tx, cc_handle) = cc.start();

    let client = Arc::new(aws_layer::create_client(config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str(), config.s3_region.as_str()).await);
    let object_channel = async_head_objects_from_bucket(&bucket, &prefix, &dest, client.clone(), cc_tx.clone()).await;
    let mut download_rx = fetch_object_metadata_and_filter_already_downloaded(&bucket, &cc_tx, client, object_channel).await;

    let pool_client = Arc::new(http_layer::build_https_client());
    let snow_context = Arc::new(build_context(config.clone()));
    let (file_sem, req_sem, buffer_pool_size) = auto_tune_semaphores(file_limit, req_limit, dest, cc_tx.clone());
    let task_sem = Arc::new(Semaphore::new(500));
    let buffer_pool = Arc::new(BufferPool::new(buffer_pool_size, DOWNLOAD_MULTIPART_THRESHOLD as usize));

    let mut created_dirs: HashSet<std::path::PathBuf> = HashSet::new();
    let mut join_set = JoinSet::new();

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
        let file_sem = file_sem.clone();
        let req_sem = req_sem.clone();
        let task_sem = task_sem.clone();
        let cc_tx = cc_tx.clone();
        let buffer_pool = buffer_pool.clone();

        let permit = task_sem.clone().acquire_owned().await.unwrap();
        join_set.spawn(async move {
            download_object(bucket, pool_client, snow_context, object, file_sem, req_sem, cc_tx, buffer_pool).await;
            drop(permit);
        });
    }

    /*
    The question here if there are some benefits in parallel downloads where some chunks are kept
    in memory. Lets imagine that the disk can write 32 things in parallel, can that be saturated by
    32 network requests? I would assume not. The begs the question if one should fill some chunks in
    memory and then write those. This will have the cost of using memory and there is a performance
    penalty maintaining the buffers


     I will implement multipart download, where I try to download ~1 MB slices of files into a buffer.
     The would be interesting. In principle we should try the same strategy as we used in go as well.
     It very hard to predict what will perform best. But it should be interesting.
     */

    while let Some(_) = join_set.join_next().await {}; //TODO Jon: Do I like this more than the join_all in upload?
    drop(cc_tx);
    cc_handle.await.unwrap();
}

async fn fetch_object_metadata_and_filter_already_downloaded(bucket: &Arc<String>, cc_tx: &UnboundedSender<CommandCenterEvent>, client: Arc<AWSClient>, mut object_channel: Receiver<S3Object>) -> mpsc::Receiver<S3Object> {
    let (download_tx, download_rx) = mpsc::channel::<S3Object>(constants::METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_DOWNLOAD as usize);

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
                let s3_mtime = get_s3_mtime(&client, &bucket, &object.key).await;
                let mut object = object;
                object.modified_time = s3_mtime;

                if should_download_object(&mut object) {
                    if let Err(e) = download_tx.send(object).await {
                        let _ = cc_tx.send(CommandCenterEvent::Log(format!("Failed to forward object to download queue: {}", e)));
                    }
                } else {
                    // Already downloaded, just update progress for the UI
                    let _ = cc_tx.send(CommandCenterEvent::FileAlreadyAtTarget(object.size as u64));
                }
                drop(permit);
            });
        }
        while let Some(_) = checker_set.join_next().await {}
    });
    download_rx
}

fn should_download_object(object: &mut S3Object) -> bool {
    if object.modified_time.is_none() {
        return true;
    }

    let needs_download = if let Ok(metadata) = fs::metadata(&object.local_path) {
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

async fn download_object(bucket: Arc<String>, pool_client: Arc<PoolClient>, snow_context: Arc<SnowContext>, object: S3Object, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, cc_sender: UnboundedSender<CommandCenterEvent>, buffer_pool: Arc<BufferPool>) {
    if object.size > DOWNLOAD_MULTIPART_THRESHOLD {
        // fancy pantsy multipart download...
        let file_path = object.local_path.clone();
        let bucket = bucket.clone();
        let key = object.key.clone();
        let total_size = object.size as u64;

        //TODO Jon: Right now we rely on file::seek to extend the file. We should consider if we should not just preallocate the file here.
        //TODO Jon: It works because the specific tokio implementation handles seeking outside the current file limit like we want it to.
        if let Err(e) = tokio::fs::File::create(&file_path).await {
            cc_sender.send(CommandCenterEvent::Log(format!("Failed to create file {:?}: {}", file_path, e))).unwrap();
            return;
        }

        let chunk_size = DOWNLOAD_MULTIPART_THRESHOLD as u64;
        let num_parts = (total_size + chunk_size - 1) / chunk_size;

        let mut download_set = JoinSet::new();

        for i in 0..num_parts {
            let start = i * chunk_size;
            let end = std::cmp::min(start + chunk_size - 1, total_size - 1);

            let snow_context = snow_context.clone();
            let pool_client = pool_client.clone();
            let req_sem = req_sem.clone();
            let file_sem = file_sem.clone();
            let canonical_uri = Arc::new(format!("/{}/{}", &bucket, key));
            let query_uri = String::new();
            let file_path = file_path.clone();

            let buffer_pool = buffer_pool.clone();

            download_set.spawn(async move {
                let data = match http_layer::get_file_range(snow_context, &pool_client, buffer_pool, canonical_uri, &query_uri, req_sem, start, end).await {
                    Ok(d) => d,
                    Err(e) => return Err(e),
                };

                let _file_permit = file_sem.acquire().await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to acquire file semaphore: {}", e)))?;
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&file_path)
                    .await?;

                file.seek(std::io::SeekFrom::Start(start)).await?;
                file.write_all(data.as_ref()).await?;
                file.flush().await?; //We must flush to ensure our mtime update is not overwritten after its been set.
                Ok(data.len as u64)
            });
        }

        while let Some(res) = download_set.join_next().await {
            match res {
                Ok(Ok(bytes)) => {
                    let transfer_metrics = CcLoadAndTransfer { //TODO JON: cleaner metrics. Perhaps optional.
                        amount: bytes,
                        file_sem_work_time_secs: 0.0,
                        file_sem_acquire_time_secs: 0.0,
                        req_sem_work_time_secs: 0.0,
                        req_sem_acquire_time_secs: 0.0,
                    };
                    cc_sender.send(CommandCenterEvent::PartTransferred(transfer_metrics)).unwrap();
                },
                Ok(Err(e)) => {//TODO JON: handle retry
                    cc_sender.send(CommandCenterEvent::Log(format!("Part download failed for {}: {}", key, e))).unwrap();
                }
                Err(e) => {//Should never happen
                    cc_sender.send(CommandCenterEvent::Log(format!("Task join failed for {}: {}", key, e))).unwrap();
                }
            }
        }

        cc_sender.send(CommandCenterEvent::FileTransferred(total_size)).unwrap();
        update_mod_time_on_file(&object, &cc_sender);

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

        match http_layer::get_file(snow_context.clone(), file_info, &pool_client, canonical_uri, &query_uri, file_sem, req_sem).await {
            Ok(_) => {
                let transfer_metrics = CcLoadAndTransfer { //TODO Jon: I would like the metrics part to be more elegant
                    amount : object.size as u64,
                    file_sem_work_time_secs: 0.0,
                    file_sem_acquire_time_secs: 0.0,
                    req_sem_work_time_secs: 0.0,
                    req_sem_acquire_time_secs: 0.0,
                };

                cc_sender.send(CommandCenterEvent::PartTransferred(transfer_metrics)).unwrap();
                cc_sender.send(CommandCenterEvent::FileTransferred(object.size as u64)).unwrap();

                update_mod_time_on_file(&object, &cc_sender);
                //cc_sender.send(CommandCenterEvent::Log(format!("Successfully downloaded {}", object.key))).unwrap();
            }
            Err(e) => {
                cc_sender.send(CommandCenterEvent::Log(format!("Failed to download {}: {}", object.key, e))).unwrap();
            }
        }
    }
}

fn update_mod_time_on_file(object: &S3Object, cc_sender: &UnboundedSender<CommandCenterEvent>) {
    if let Some(mtime) = object.modified_time {
        match fs::OpenOptions::new().write(true).open(&object.local_path) { //TODO JON: is it faster if we get the reference used when writing the file? Could easily be improved in simple download
            Ok(file) => {
                if let Err(e) = file.set_modified(mtime) {
                    cc_sender.send(CommandCenterEvent::Log(format!("Failed to update mtime for {:?}: {}", object.local_path, e))).unwrap();
                }
            }
            Err(e) => {
                cc_sender.send(CommandCenterEvent::Log(format!("Failed to open file to update mtime for {:?}: {}", object.local_path, e))).unwrap();
            }
        }
    }
}
