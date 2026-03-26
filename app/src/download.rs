use crate::{auto_tuning, read_config};
use aws_layer::{async_head_objects_from_bucket, S3Object};
use std::path::Path;
use std::sync::Arc;
use crate::command_center::{CommandCenter, CC};
use crate::constants::DOWNLOAD_MULTIPART_THRESHOLD;
use std::collections::HashSet;
use std::fs;
use tokio::sync::Semaphore;
use http_layer::{BufferPool, PoolClient};
use util_layer::{FileInfo, build_context, constants, SnowContext};
use crate::auto_tuning::auto_tune_semaphores;

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
    let mut object_channel = async_head_objects_from_bucket(&bucket, &prefix, &dest, client.clone()).await;


    let pool_client = http_layer::build_https_client();
    let snow_context = Arc::new(build_context(config.clone()));
    let (file_sem, req_sem, buffer_pool_size) = auto_tune_semaphores(req_limit, file_limit, dest, cc_tx.clone());
    let task_sem = Arc::new(Semaphore::new(500));
    let buffer_pool = Arc::new(BufferPool::new(buffer_pool_size, constants::CHUNK_SIZE as usize));


    let mut created_dirs: HashSet<std::path::PathBuf> = HashSet::new();
    // Process objects from channel
    while let Some(object) = object_channel.recv().await {

        // TODO: for each object check size and determine parallel or simple download
        // TODO: truncate file to prepare for download
        // TODO: download
        println!("Size: {}. Received object: {} -> {:?}", object.size, object.key, object.local_path);

        // Ensure parent directories exist
        if let Err(e) = ensure_parent_dirs_exist(&object.local_path, &mut created_dirs) {
            eprintln!("Failed to create directories for {:?}: {}", object.local_path, e);
            continue;
        }

        download_object(&bucket, silent, &pool_client, snow_context.clone(), object, file_sem.clone(), req_sem.clone()).await;
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


    drop(cc_tx);
    cc_handle.await.unwrap();
}

async fn download_object(bucket: &Arc<String>, silent: bool, pool_client: &PoolClient, snow_context: Arc<SnowContext>, object: S3Object, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>) {
    if object.size > DOWNLOAD_MULTIPART_THRESHOLD {
        //
        //fancy pantsy multipart download...
    } else {
        //simple download
        let file_info = Arc::new(FileInfo {
            file_path: object.local_path.clone(),
            key: object.key.clone(),
            file_size: object.size as u64,
        });

        let canonical_uri = Arc::new(format!("/{}/{}", &bucket, object.key));
        let query_uri = String::new();

        match http_layer::get_file(snow_context.clone(), file_info, &pool_client, canonical_uri, &query_uri).await {
            Ok(_) => {
                if !silent {
                    println!("Successfully downloaded {}", object.key);
                }
            }
            Err(e) => {
                eprintln!("Failed to download {}: {}", object.key, e);
            }
        }
    }
}
