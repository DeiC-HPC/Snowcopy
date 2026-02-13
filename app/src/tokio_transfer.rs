use crate::{constants, create_config};
use std::io;
use std::path::{Path, PathBuf};

use aws_layer::{AWSClient, CompletedPartAws};
use futures::future::join_all;
use http_layer::{build_https_client, PoolClient};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Semaphore};
use walkdir::WalkDir;


pub async fn run_s3_ceph_tokio(bucket: &str, path: &Path, file_limit: Option<usize>, req_limit: Option<usize>) {
    let mut rx = walk_directory(path.to_path_buf()).await;

    // Read config file
    let config = create_config();

    let client = aws_layer::create_client(config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str()).await;
    aws_layer::create_bucket(&client, bucket).await.unwrap();

    let hyper_client = build_https_client();
    let (file_sem, req_sem) = auto_tune_semaphores(file_limit, req_limit);
    let mut handles = Vec::new();

    while let Some(result) = rx.recv().await {
        match result {
            Ok(file_info) => {
                let bucket = bucket.to_string();
                let client = client.clone();
                let hyper_client = hyper_client.clone();
                let file_sem_clone = file_sem.clone();
                let req_sem_clone = req_sem.clone();

                let handle = tokio::spawn(async move {
                    let _permit = file_sem_clone.acquire().await.unwrap();
                    upload_file(&bucket, &client, hyper_client, file_info, req_sem_clone.clone()).await;
                });
                handles.push(handle);
            }
            Err(e) => {
                eprintln!("Error walking directory: {}", e);
            }
        }
    }

    join_all(handles).await;
}

fn auto_tune_semaphores(file_limit_override: Option<usize>, req_limit_override: Option<usize>) -> (Arc<Semaphore>, Arc<Semaphore>) {
    let file_limit = if let Some(limit) = file_limit_override {
        println!("Using provided file_limit: {}", limit);
        limit
    } else {
        #[cfg(target_os = "linux")]
        let limit = {
            50
        };

        #[cfg(target_os = "windows")]
        let limit = {
            80
        };

        #[cfg(target_os = "macos")]
        let limit = {
            150
        };

        #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
        let limit = {
            println!("Detected platform: Other");
            150
        };

        limit
    };

    let req_limit = if let Some(limit) = req_limit_override {
        println!("Using provided req_limit: {}", limit);
        limit
    } else {
        #[cfg(target_os = "linux")]
        let limit = {
            50
        };

        #[cfg(target_os = "windows")]
        let limit = {
            80
        };

        #[cfg(target_os = "macos")]
        let limit = {
            150
        };

        #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
        let limit = {
            150
        };

        limit
    };

    let file_sem = Arc::new(Semaphore::new(file_limit));
    let req_sem = Arc::new(Semaphore::new(req_limit));

    println!("Auto-tuning result - file_limit: {}, req_limit: {}", file_limit, req_limit);

    (file_sem, req_sem)
}

async fn upload_file(bucket: &str, aws_client: &AWSClient, hyper_client: PoolClient, file_info: FileInfo, req_sem: Arc<Semaphore>) {
    println!("Processing file: {:?}", file_info.file_path);
    if file_info.file_size > constants::CHUNK_SIZE {
        upload_with_multipart(&bucket, aws_client, &hyper_client, &file_info, req_sem).await;
    } else {
        small_file_upload(bucket, &aws_client, hyper_client, file_info, req_sem).await;
    }
}

async fn upload_with_multipart(bucket: &&str, aws_client: &AWSClient, hyper_client: &PoolClient, file_info: &FileInfo, semaphore: Arc<Semaphore>) {
    let key = file_info.key.clone();
    let create_multipart_upload_output = aws_client.create_multipart_upload().bucket(bucket.clone()).key(key).send().await.unwrap();
    let upload_id = create_multipart_upload_output.upload_id().unwrap();

    let chunk_amount_u64 = (file_info.file_size + constants::CHUNK_SIZE - 1) / constants::CHUNK_SIZE;

    let completed_parts = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    for i in 0..chunk_amount_u64 {
        let start_pos = i * constants::CHUNK_SIZE;
        let amount = std::cmp::min(constants::CHUNK_SIZE, file_info.file_size - start_pos);

        let part_number = (i + 1) as i32;
        let part_presigned = aws_layer::presign_upload_part(aws_client, &bucket, &file_info.key, upload_id, part_number, constants::PRESIGN_LIFETIME).await;

        let hyper_client = hyper_client.clone();
        let file_path = file_info.file_path.clone();
        let presigned_uri = part_presigned.uri().to_string();
        let semaphore = semaphore.clone();
        let completed_parts = completed_parts.clone();

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            let etag = put_file_multipart(
                hyper_client,
                &presigned_uri,
                file_path.to_str().unwrap(),
                start_pos,
                amount,
            ).await;

            if let Some(etag) = etag {
                let completed_part = CompletedPartAws::builder()
                    .e_tag(etag)
                    .part_number(part_number)
                    .build();
                completed_parts.lock().await.push(completed_part);
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let completed_parts = Arc::try_unwrap(completed_parts).unwrap().into_inner();
    aws_layer::complete_multipart_upload(aws_client, bucket, &file_info.key, upload_id, completed_parts).await.unwrap();
}

async fn small_file_upload(bucket: &str, aws_client: &&AWSClient, hyper_client: PoolClient, file_info: FileInfo, req_sem: Arc<Semaphore>) {
    let presigned_request = aws_layer::presign_put(
        &aws_client,
        bucket,
        file_info.key.as_str(),
        constants::PRESIGN_LIFETIME,
    )
        .await;


    let _permit = req_sem.acquire().await.unwrap(); //get lock

    put_file(
        hyper_client.clone(),
        presigned_request.uri(),
        file_info.file_path.to_str().unwrap(),
    )
        .await;
}

async fn put_file_multipart(hyper_client: PoolClient, presigned_url: &str, file_path: &str, start_pos: u64, amount: u64) -> Option<String> {
    match http_layer::put_file_range(presigned_url, file_path, start_pos, amount, &hyper_client).await {
        Ok(etag) => Some(etag?),
        Err(e) => {
            eprintln!("Error putting file chunk {} (start: {}, amount: {}): {}", file_path, start_pos, amount, e);
            panic!("Error putting file chunk {} (start: {}, amount: {}): {}", file_path, start_pos, amount, e);
            None
        }
    }
}

pub struct FileInfo {
    pub file_path: PathBuf,
    pub key: String,
    pub file_size: u64,
}

async fn put_file(hyper_client: PoolClient, url: &str, path: &str) {
    if let Err(e) = http_layer::put_file(url, path, &hyper_client).await {
        eprintln!("Error putting file {}: {}", path, e);
    }
}

pub async fn walk_directory(base_path: PathBuf) -> mpsc::Receiver<Result<FileInfo, io::Error>> {
    let (tx, rx) = mpsc::channel(100);

    tokio::spawn(async move {
        for entry in WalkDir::new(base_path) {
            match entry {
                Ok(entry) => {
                    if entry.file_type().is_file() {
                        let file_size = entry.metadata().unwrap().len();
                        let file_path = entry.path().to_path_buf();
                        let key: String = util_layer::normalize_path(file_path.to_str().unwrap());

                        let file_info = FileInfo { file_path, key, file_size };

                        if tx.send(Ok(file_info)).await.is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    let io_error = io::Error::new(io::ErrorKind::Other, e);
                    if tx.send(Err(io_error)).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    rx
}


