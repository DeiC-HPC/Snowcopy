
#[allow(dead_code)]
pub async fn download(bucket: &str, prefix: &str, dest: &str, ) -> Result<(), Box<dyn std::error::Error>>{



    //start task to scan external storage
    //for each object check size and determine parallel or simple download
    //create file and needed folders
    //truncate file to prepare for download
    //download

    /*
    The question here if there are some benefits in parallel downloads where some chunks are kept
    in memory. Lets imagine that the disk can write 32 things in parallel, can that be saturated by
    32 network requests? I would assume not. The begs the question if one should fill some chunks in
    memory and then write those. This will have the cost of using memory and there is a performance
    penalty maintaining the buffers
     */

    Ok(())
}