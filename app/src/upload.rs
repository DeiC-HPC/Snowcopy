use crate::{auto_tuning, constants, read_config};
use std::io;
use std::path::{Path, PathBuf};

use crate::ui::UiEvent;
use aws_layer::{AWSClient, CompletedPartAws};
use futures::future::join_all;
use http_layer::{build_https_client, BufferPool, PoolClient};
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, Mutex, Semaphore};
use walkdir::WalkDir;

pub async fn run_s3_ceph_tokio(bucket: &str, path: &Path, file_limit: Option<usize>, req_limit: Option<usize>, silent: bool) {
    let (ui_tx, ui_rx) = mpsc::unbounded_channel::<UiEvent>();
    let mut file_discovery_channel = walk_directory(path.to_path_buf(), ui_tx.clone(), silent).await;

    let config = read_config();

    let client = aws_layer::create_client(config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str()).await;

    let bucket_creation_handle = {
        let client = client.clone();
        let bucket = bucket.to_string();
        tokio::spawn(async move {
            aws_layer::create_bucket(&client, &bucket).await.unwrap()
        })
    };

    let hyper_client = build_https_client();
    let (file_sem, req_sem, buffer_pool) = auto_tuning::auto_tune_semaphores(file_limit, req_limit, silent, path);
    let task_sem = Arc::new(Semaphore::new(500));
    let buffer_pool = Arc::new(BufferPool::new(buffer_pool, constants::CHUNK_SIZE as usize));
    
    let pb = if !silent {
        let pb = ProgressBar::new(0);
        pb.set_style(ProgressStyle::with_template("[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})  Status: {msg:25!} ")
            .unwrap()
            .progress_chars("#>-"));
        pb.set_message("File discovery");
        Some(pb)
    } else {
        None
    };

    // UI task
    let ui_handle = if !silent {
        let pb_ui = pb.as_ref().unwrap().clone();
        Some(crate::ui::start_ui_task(ui_rx, pb_ui))
    } else {
        None
    };

    let mut handles = Vec::new();

    bucket_creation_handle.await.unwrap();

    while let Some(result) = file_discovery_channel.recv().await {
        match result {
            Ok(file_info) => {
                let bucket = bucket.to_string();
                let client = client.clone();
                let hyper_client = hyper_client.clone();
                let file_sem_clone = file_sem.clone();
                let req_sem_clone = req_sem.clone();
                let buffer_pool_clone = buffer_pool.clone();
                let ui_channel = ui_tx.clone();

                let permit = task_sem.clone().acquire_owned().await.unwrap();
                let handle = tokio::spawn(async move {
                    upload_file(&bucket, &client, hyper_client, file_info, file_sem_clone, req_sem_clone, buffer_pool_clone, ui_channel).await;
                    drop(permit);
                });
                handles.push(handle);
            }
            Err(e) => {
                eprintln!("Error walking directory: {}", e);
            }
        }
    }

    drop(ui_tx); //This allows the UI task to exit gracefully when all files have been uploaded

    join_all(handles).await;
    if let Some(ref pb) = pb {
        pb.finish_with_message("Success");
    }

    if let Some(ui_handle) = ui_handle {
        let _ = ui_handle.await;
    }
}


async fn upload_file(bucket: &str, aws_client: &AWSClient, hyper_client: PoolClient, file_info: FileInfo, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<UiEvent>) {
    if file_info.file_size > constants::CHUNK_SIZE {
        upload_with_multipart(&bucket, aws_client, &hyper_client, &file_info, file_sem, req_sem, buffer_pool, sender).await;
    } else {
        small_file_upload(bucket, &aws_client, hyper_client, file_info, file_sem, req_sem, buffer_pool, sender).await;
    }
}

async fn upload_with_multipart(bucket: &&str, aws_client: &AWSClient, hyper_client: &PoolClient, file_info: &FileInfo, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<UiEvent>) {
    let key = file_info.key.clone();
    let create_multipart_upload_output = aws_client.create_multipart_upload().bucket(bucket.to_string()).key(key).send().await.unwrap();
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
        let file_sem_clone = file_sem.clone();
        let req_sem_clone = req_sem.clone();
        let buffer_pool_clone = buffer_pool.clone();
        let completed_parts = completed_parts.clone();
        let sender_cloned = sender.clone();

        let handle = tokio::spawn(async move {
            let etag = put_file_multipart(
                hyper_client,
                &presigned_uri,
                file_path.to_str().unwrap(),
                start_pos,
                amount,
                file_sem_clone,
                req_sem_clone,
                buffer_pool_clone,
            ).await;

            if let Some(etag) = etag {
                let completed_part = CompletedPartAws::builder()
                    .e_tag(etag)
                    .part_number(part_number)
                    .build();
                completed_parts.lock().await.push(completed_part);
            }

            sender_cloned.send(UiEvent::Uploaded(amount)).unwrap();
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let completed_parts = Arc::try_unwrap(completed_parts).unwrap().into_inner();
    aws_layer::complete_multipart_upload(aws_client, bucket, &file_info.key, upload_id, completed_parts).await.unwrap();
}

async fn small_file_upload(bucket: &str, aws_client: &&AWSClient, hyper_client: PoolClient, file_info: FileInfo, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<UiEvent>) {
    let presigned_request = aws_layer::presign_put(
        &aws_client,
        bucket,
        file_info.key.as_str(),
        constants::PRESIGN_LIFETIME,
    )
        .await;

    put_file(
        hyper_client.clone(),
        presigned_request.uri(),
        file_info.file_path.to_str().unwrap(),
        file_sem,
        req_sem,
        buffer_pool,
    )
        .await;

    sender.send(UiEvent::Uploaded(file_info.file_size)).unwrap();
}

async fn put_file_multipart(hyper_client: PoolClient, presigned_url: &str, file_path: &str, start_pos: u64, amount: u64, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>) -> Option<String> {
    match http_layer::put_file_range(presigned_url, file_path, start_pos, amount, &hyper_client, file_sem, req_sem, buffer_pool).await {
        Ok(etag) => Some(etag?),
        Err(e) => {
            eprintln!("Error putting file chunk {} (start: {}, amount: {}): {}", file_path, start_pos, amount, e);
            panic!("Error putting file chunk {} (start: {}, amount: {}): {}", file_path, start_pos, amount, e);
        }
    }
}

pub struct FileInfo {
    pub file_path: PathBuf,
    pub key: String,
    pub file_size: u64,
}

async fn put_file(hyper_client: PoolClient, url: &str, path: &str, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>) {
    if let Err(e) = http_layer::put_file(url, path, &hyper_client, file_sem, req_sem, buffer_pool).await {
        eprintln!("Error putting file {}: {}", path, e);
    }
}

pub async fn walk_directory(base_path: PathBuf, ui_tx: UnboundedSender<UiEvent>, silent: bool) -> mpsc::Receiver<Result<FileInfo, io::Error>> {
    let (tx, rx) = mpsc::channel(5000);

    tokio::task::spawn_blocking(move || {
        for entry in WalkDir::new(base_path).max_open(100) {
            match entry {
                Ok(entry) => {
                    if entry.file_type().is_file() {
                        let file_size = entry.metadata().unwrap().len();
                        let file_path = entry.path().to_path_buf();
                        let key: String = util_layer::normalize_file_path(file_path.to_str().unwrap());

                        if !silent {
                            let add_to_total = UiEvent::AddToTotal(file_size);
                            if ui_tx.send(add_to_total).is_err() {
                                break;
                            }
                        }
                        let file_info = FileInfo { file_path, key, file_size };
                        if tx.blocking_send(Ok(file_info)).is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    let io_error = io::Error::new(io::ErrorKind::Other, e);
                    if tx.blocking_send(Err(io_error)).is_err() {
                        break;
                    }
                }
            }
        }
        ui_tx.send(UiEvent::Message("Uploading".to_string()))
    });

    rx
}