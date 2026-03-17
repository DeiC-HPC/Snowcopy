use crate::read_config;
use aws_layer::async_head_objects_from_bucket;
use std::path::Path;
use crate::constants::DOWNLOAD_MULTIPART_THRESHOLD;

#[allow(dead_code)]
pub async fn download(bucket: &str, prefix: &str, dest: &Path, file_limit: Option<usize>, req_limit: Option<usize>, silent: bool) {
    let config = read_config();
    let client = aws_layer::create_client(config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str()).await;

    let mut object_channel = async_head_objects_from_bucket(&bucket, &prefix, &dest, client).await;

    // Process objects from channel
    while let Some(object) = object_channel.recv().await {

        // TODO: for each object check size and determine parallel or simple download
        // TODO: create file and needed folders
        // TODO: truncate file to prepare for download
        // TODO: download
        println!("Size: {}. Received object: {} -> {:?}",object.size, object.key, object.local_path);

        if(object.size > DOWNLOAD_MULTIPART_THRESHOLD){
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



}
