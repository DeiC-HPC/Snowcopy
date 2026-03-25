use crate::read_config;
use aws_layer::async_head_objects_from_bucket;
use std::path::Path;
use std::sync::Arc;
use crate::command_center::{CommandCenter, CC};
use crate::constants::DOWNLOAD_MULTIPART_THRESHOLD;
use std::collections::HashSet;
use std::fs;

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
pub async fn download(bucket: Arc<String>, prefix: &str, dest: &Path, _file_limit: Option<usize>, _req_limit: Option<usize>, silent: bool, stats: bool) {
    let config = read_config();
    let cc = CommandCenter::new(silent, stats);
    let (cc_tx, cc_handle) = cc.start();

    let client = aws_layer::create_client(config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str()).await;

    let mut object_channel = async_head_objects_from_bucket(&bucket, &prefix, &dest, client).await;

    // Cache for already created directories
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

        if object.size > DOWNLOAD_MULTIPART_THRESHOLD {
            //
            //fancy pantsy multipart download...
        } else {
            
            //simple download
        }
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
