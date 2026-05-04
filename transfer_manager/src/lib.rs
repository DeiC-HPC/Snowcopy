use std::path::Path;
use std::sync::Arc;
use aws_sdk_s3_transfer_manager::{Client};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_config::Region;

pub async fn upload(bucket: Arc<String>, path: &Path, silent: bool, _stats: bool, config: &util_layer::Config, concurrency: Option<usize>) {
    let region = Region::new(config.s3_region.clone());
    let creds = Credentials::new(
        &config.s3_access_key,
        &config.s3_secret_key,
        None,
        None,
        "manual",
    );

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(region)
        .endpoint_url(&config.s3_url)
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
    let mut tm_config_builder = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3_client);

    if let Some(c) = concurrency {
        tm_config_builder = tm_config_builder.concurrency(aws_sdk_s3_transfer_manager::types::ConcurrencyMode::Explicit(c));
    } else {
        tm_config_builder = tm_config_builder.concurrency(aws_sdk_s3_transfer_manager::types::ConcurrencyMode::Explicit(200));
    }

    let tm_config = tm_config_builder.build();
    let client = Client::new(tm_config);

    if path.is_dir() {
        let handle = match client
            .upload_objects()
            .bucket(bucket.as_str())
            .source(path.to_string_lossy().to_string())
            .recursive(true)
            .send()
            .await {
            Ok(h) => h,
            Err(e) => {
                eprintln!("Failed to initiate directory upload: {:?}", e);
                return;
            }
        };

        if !silent {
            println!("Uploading directory: {:?}", path);
        }

        if let Err(e) = handle.join().await {
            eprintln!("Error during directory upload: {:?}", e);
            return;
        }
    } else {
        let key = match path.file_name()
            .and_then(|n| n.to_str()) {
            Some(k) => k,
            None => {
                eprintln!("Invalid file name: {:?}", path);
                return;
            }
        };

        let stream = match aws_sdk_s3_transfer_manager::io::InputStream::from_path(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to open file {:?}: {:?}", path, e);
                return;
            }
        };

        let handle = match client
            .upload()
            .bucket(bucket.as_str())
            .key(key)
            .body(stream)
            .initiate() {
            Ok(h) => h,
            Err(e) => {
                eprintln!("Failed to initiate file upload: {:?}", e);
                return;
            }
        };

        if !silent {
            println!("Uploading file: {:?}", path);
        }

        if let Err(e) = handle.join().await {
            eprintln!("Error during file upload: {:?}", e);
            return;
        }
    }

    if !silent {
        println!("Upload completed successfully.");
    }
}

pub async fn download(bucket: Arc<String>, path: &Path, silent: bool, _stats: bool, config: &util_layer::Config, concurrency: Option<usize>) {
    let region = Region::new(config.s3_region.clone());
    let creds = Credentials::new(
        &config.s3_access_key,
        &config.s3_secret_key,
        None,
        None,
        "manual",
    );

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(region)
        .endpoint_url(&config.s3_url)
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
    let mut tm_config_builder = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3_client);

    if let Some(c) = concurrency {
        tm_config_builder = tm_config_builder.concurrency(aws_sdk_s3_transfer_manager::types::ConcurrencyMode::Explicit(c));
    } else {
        tm_config_builder = tm_config_builder.concurrency(aws_sdk_s3_transfer_manager::types::ConcurrencyMode::Explicit(200));
    }

    let tm_config = tm_config_builder.build();
    let client = Client::new(tm_config);

    // We decide whether it's a directory download based on whether 'path' already exists as a directory
    // or if it ends with a separator.
    let is_directory_download = path.is_dir() || path.to_string_lossy().ends_with('/') || path.to_string_lossy().ends_with('\\');

    if is_directory_download {
        let handle = match client
            .download_objects()
            .bucket(bucket.as_str())
            .destination(path.to_string_lossy().to_string())
            .send()
            .await {
            Ok(h) => h,
            Err(e) => {
                eprintln!("Failed to initiate directory download: {:?}", e);
                return;
            }
        };

        if !silent {
            println!("Downloading from bucket {} to {:?}", bucket, path);
        }

        if let Err(e) = handle.join().await {
            eprintln!("Error during directory download: {:?}", e);
            return;
        }
    } else {
        // Single file download
        // For a single file download, we assume 'path' is the local file path, 
        // and we use its file name as the S3 key.
        let key = match path.file_name().and_then(|n| n.to_str()) {
            Some(k) => k,
            None => {
                eprintln!("Invalid file name for download: {:?}", path);
                return;
            }
        };

        let mut handle = match client
            .download()
            .bucket(bucket.as_str())
            .key(key)
            .initiate() {
            Ok(h) => h,
            Err(e) => {
                eprintln!("Failed to initiate file download for key {}: {:?}", key, e);
                return;
            }
        };

        if !silent {
            println!("Downloading file {} to {:?}", key, path);
        }

        use tokio::io::AsyncWriteExt;
        let mut file = match tokio::fs::File::create(path).await {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to create file {:?}: {:?}", path, e);
                return;
            }
        };

        while let Some(chunk_result) = handle.body_mut().next().await {
            match chunk_result {
                Ok(chunk) => {
                    if let Err(e) = file.write_all(&chunk.data.into_bytes()).await {
                        eprintln!("Error writing to file {:?}: {:?}", path, e);
                        return;
                    }
                }
                Err(e) => {
                    eprintln!("Error during file download: {:?}", e);
                    return;
                }
            }
        }
    }

    if !silent {
        println!("Download completed successfully.");
    }
}


