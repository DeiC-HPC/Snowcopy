use crate::TransferMethod::CephS3;
use serde;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::env;

enum TransferMethod {
    CephS3
}

fn match_transfer_method(method: Option<&str>) -> Option<TransferMethod> {
    match method {
        Some("o1") => Some(TransferMethod::CephS3),
        _Other => None,
    }
}

#[derive(Serialize, Deserialize)]
struct Config {
    bucket: String,
}

struct Args {
    /// Path to the file to upload
    file_path: String,

    /// S3 object key
    object_key: String,

    /// Transfer method
    //#[arg(short, default_value = "o1")]
    transfer_method: String,
}

fn run_s3_ceph(bucket: String, file_name: String, file_path: String) {
    /*
    // Set up AWS region and credentials
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::from_env().region(region_provider).load().await;
    let client = aws_sdk_s3::Client::new(&config);

    // Open the file asynchronously using compio
    let file = File::open(file_path).await.unwrap();
    let (read, buffer) = file.read_to_end_at(Vec::with_capacity(1024), 0).await.unwrap();
    assert_eq!(read, buffer.len());
    let buffer = String::from_utf8(buffer).unwrap();
    let reader = BufReader::new(buffer);

   // Upload the file to S3
    client
        .put_object()
        .bucket(bucket)
        .key(file_name)
        .body(reader)
        .send()
        .await?;

 */
    println!("File uploaded successfully.");
}

//#[compio::main]
fn main() -> Result<()> {
    // Read Commandline Arguments
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1].clone();
    let object_key = &args[2].clone();
    let transfer_method = &args[3].clone();
    let config_path = "config.json";

    // Read config file
    let config_content = std::fs::read_to_string(&config_path);
    match config_content {
        Ok(content) => {
            let config: Config = serde_json::from_str(content.as_str())?;
            
            let method = match_transfer_method(Some(transfer_method)).unwrap();

            match method {
                CephS3 => run_s3_ceph(config.bucket, object_key.clone(), file_path.clone()),
            };
        },
        Err(_) => {
            println!("Error: failed to read config file.");
        }
    }

    Ok(())
}