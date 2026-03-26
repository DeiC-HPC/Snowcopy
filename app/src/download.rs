use crate::read_config;
use aws_layer::{async_head_objects_from_bucket, S3Object};
use std::path::Path;
use std::sync::Arc;
use crate::command_center::{CommandCenter, CC};
use crate::constants::DOWNLOAD_MULTIPART_THRESHOLD;
use std::collections::HashSet;
use std::fs;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Semaphore;
use http_layer::{BufferPool, PoolClient};
use util_layer::{FileInfo, build_context, constants, SnowContext};
use crate::auto_tuning::auto_tune_semaphores;

use tokio::task::JoinSet;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use util_layer::cc::{CcLoadAndTransfer, CommandCenterEvent};

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

#[allow(dead_code)]
pub async fn download(bucket: Arc<String>, prefix: &str, dest: &Path, file_limit: Option<usize>, req_limit: Option<usize>, silent: bool, stats: bool) {
    let config = read_config();
    let cc = CommandCenter::new(silent, stats);
    let (cc_tx, cc_handle) = cc.start();

    let client = Arc::new(aws_layer::create_client(config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str()).await);
    let mut object_channel = async_head_objects_from_bucket(&bucket, &prefix, &dest, client.clone(), cc_tx.clone()).await;


    let pool_client = Arc::new(http_layer::build_https_client());
    let snow_context = Arc::new(build_context(config.clone()));
    let (file_sem, req_sem, buffer_pool_size) = auto_tune_semaphores(req_limit, file_limit, dest, cc_tx.clone());
    let task_sem = Arc::new(Semaphore::new(500));
    let buffer_pool = Arc::new(BufferPool::new(buffer_pool_size, DOWNLOAD_MULTIPART_THRESHOLD as usize));


    let mut created_dirs: HashSet<std::path::PathBuf> = HashSet::new();
    let mut join_set = JoinSet::new();

    // Process objects from channel
    while let Some(object) = object_channel.recv().await {

        // TODO: for each object check size and determine parallel or simple download
        // TODO: truncate file to prepare for download
        // TODO: download
        //println!("Size: {}. Received object: {} -> {:?}", object.size, object.key, object.local_path);

        // Ensure parent directories exist
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

async fn download_object(bucket: Arc<String>, pool_client: Arc<PoolClient>, snow_context: Arc<SnowContext>, object: S3Object, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, cc_sender: UnboundedSender<CommandCenterEvent>, buffer_pool: Arc<BufferPool>) {
    if object.size > DOWNLOAD_MULTIPART_THRESHOLD {
        // fancy pantsy multipart download...
        let file_path = object.local_path.clone();
        let bucket = bucket.clone();
        let key = object.key.clone();
        let total_size = object.size as u64;

        // Truncate file and pre-allocate
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
                    cc_sender.send(CommandCenterEvent::FileTransferred(transfer_metrics)).unwrap();
                },
                Ok(Err(e)) => {//TODO JON: handle retry
                    cc_sender.send(CommandCenterEvent::Log(format!("Part download failed for {}: {}", key, e))).unwrap();
                }
                Err(e) => {//TODO JON: should never happen
                    cc_sender.send(CommandCenterEvent::Log(format!("Task join failed for {}: {}", key, e))).unwrap();
                }
            }
        }

    } else {
        //simple download
        let file_info = Arc::new(FileInfo {
            file_path: object.local_path.clone(),
            key: object.key.clone(),
            file_size: object.size as u64,
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

                cc_sender.send(CommandCenterEvent::FileTransferred(transfer_metrics)).unwrap();
                cc_sender.send(CommandCenterEvent::Log(format!("Successfully downloaded {}", object.key))).unwrap();
            }
            Err(e) => {
                cc_sender.send(CommandCenterEvent::Log(format!("Failed to download {}: {}", object.key, e))).unwrap();
            }
        }
    }
}
