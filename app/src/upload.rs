use crate::{auto_tuning, read_config};
use std::io;
use std::path::{Path, PathBuf};

use util_layer::ui::UiEvent;
use crate::ui::{Ui, ProgressBarUi};
use aws_layer::{AWSClient, CompletedPartAws};
use futures::future::join_all;
use http_layer::{build_https_client, BufferPool, PoolClient};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, Mutex, Semaphore};
use util_layer::{build_context, constants, FileInfo, SnowContext};
use walkdir::WalkDir;

pub async fn run_s3_ceph_tokio(bucket: &str, path: &Path, file_limit: Option<usize>, req_limit: Option<usize>, silent: bool) {
    let (ui, ui_tx, ui_handle) = if silent {
        (None, None, None)
    } else {
        let ui = ProgressBarUi::new();
        let (tx, handle) = ui.start();
        (Some(ui), Some(tx), Some(handle))
    };

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
    let (file_sem, req_sem, buffer_pool) = auto_tuning::auto_tune_semaphores(file_limit, req_limit, silent, path, ui_tx.clone());
    let task_sem = Arc::new(Semaphore::new(500));
    let buffer_pool = Arc::new(BufferPool::new(buffer_pool, constants::CHUNK_SIZE as usize));

    let mut handles = Vec::new();

    bucket_creation_handle.await.unwrap();

    while let Some(result) = file_discovery_channel.recv().await {
        match result {
            Ok(file_info) => {
                let ctx = build_context(config.clone());
                let bucket = bucket.to_string();
                let client = client.clone();
                let hyper_client = hyper_client.clone();
                let file_sem_clone = file_sem.clone();
                let req_sem_clone = req_sem.clone();
                let buffer_pool_clone = buffer_pool.clone();
                let ui_channel = ui_tx.clone();

                let permit = task_sem.clone().acquire_owned().await.unwrap();
                let handle = tokio::spawn(async move {
                    upload_file(&ctx, &bucket, &client, hyper_client, file_info, file_sem_clone, req_sem_clone, buffer_pool_clone, ui_channel).await;
                    drop(permit);
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

async fn upload_file(ctx: &SnowContext,bucket: &str, aws_client: &AWSClient, hyper_client: PoolClient, file_info: FileInfo, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: Option<UnboundedSender<UiEvent>>) {
    if file_info.file_size > constants::CHUNK_SIZE {
        upload_with_multipart(ctx, &bucket, aws_client, &hyper_client, &file_info, file_sem, req_sem, buffer_pool, sender).await;
    } else {
        small_file_upload(ctx, bucket, hyper_client, file_info, file_sem, req_sem, buffer_pool, sender).await;
    }
}

async fn upload_with_multipart(ctx: &SnowContext, bucket: &&str, aws_client: &AWSClient, hyper_client: &PoolClient, file_info: &FileInfo, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: Option<UnboundedSender<UiEvent>>) {
    let key = file_info.key.clone();
    let bucket_local : &str = bucket;
    let create_multipart_upload_output = aws_client.create_multipart_upload().bucket(bucket_local.to_string()).key(key).send().await.unwrap();
    let upload_id = create_multipart_upload_output.upload_id().unwrap();

    let chunk_amount_u64 = (file_info.file_size + constants::CHUNK_SIZE - 1) / constants::CHUNK_SIZE;

    let completed_parts = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    for i in 0..chunk_amount_u64 {
        let start_pos = i * constants::CHUNK_SIZE;
        let amount = std::cmp::min(constants::CHUNK_SIZE, file_info.file_size - start_pos);

        let part_number = (i + 1) as i32;
        let url = format!("{}/{}/{}?partNumber={}&uploadId={}", ctx.s3_url(), bucket_local, file_info.key, part_number, upload_id);

        let hyper_client = hyper_client.clone();
        let file_path = file_info.file_path.clone();
        let file_sem_clone = file_sem.clone();
        let req_sem_clone = req_sem.clone();
        let buffer_pool_clone = buffer_pool.clone();
        let completed_parts = completed_parts.clone();
        let sender_cloned = sender.clone();

        let ctx_cloned = ctx.clone();
        let bucket_cloned = bucket_local.to_string(); // Clone into owned data
        let handle = tokio::spawn(async move {
            let etag = put_file_multipart(
                hyper_client,
                &ctx_cloned,
                &bucket_cloned,
                file_path.to_str().unwrap(),
                start_pos,
                amount,
                file_sem_clone,
                req_sem_clone,
                buffer_pool_clone,
                url,
            ).await;

            if let Some(etag) = etag {
                let completed_part = CompletedPartAws::builder()
                    .e_tag(etag)
                    .part_number(part_number)
                    .build();
                completed_parts.lock().await.push(completed_part);
            }

            if let Some(sender) = sender_cloned {
                sender.send(UiEvent::Uploaded(amount)).unwrap();
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let completed_parts = Arc::try_unwrap(completed_parts).unwrap().into_inner();
    aws_layer::complete_multipart_upload(aws_client, bucket, &file_info.key, upload_id, completed_parts).await.unwrap();
}

async fn small_file_upload(ctx: &SnowContext, bucket: &str, hyper_client: PoolClient, file_info: FileInfo, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: Option<UnboundedSender<UiEvent>>) {
    put_file(
        hyper_client.clone(),
        ctx,
        bucket,
        file_info.clone(),
        file_sem,
        req_sem,
        buffer_pool,
    )
        .await;

    if let Some(sender) = sender {
        sender.send(UiEvent::Uploaded(file_info.file_size)).unwrap();
    }
}

async fn put_file_multipart(hyper_client: PoolClient, ctx: &SnowContext, bucket: &str, file_path: &str, start_pos: u64, amount: u64, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, url: String) -> Option<String> {
    match http_layer::put_file_range(ctx, file_path, bucket, start_pos, amount, &hyper_client, file_sem, req_sem, buffer_pool, url).await {
        Ok(etag) => Some(etag?),
        Err(e) => {
            eprintln!("Error putting file chunk {} (start: {}, amount: {}): {}", file_path, start_pos, amount, e);
            panic!("Error putting file chunk {} (start: {}, amount: {}): {}", file_path, start_pos, amount, e);
        }
    }
}

async fn put_file(hyper_client: PoolClient, ctx: &SnowContext, bucket: &str, file_info: FileInfo, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>) {
    let path = file_info.file_path.to_str();
    if let Err(e) = http_layer::put_file(ctx, bucket, file_info.clone(), &hyper_client, file_sem, req_sem, buffer_pool).await {
        eprintln!("Error putting file {}: {}", path.unwrap(), e);
    }
}

pub async fn walk_directory(base_path: PathBuf, ui_tx: Option<UnboundedSender<UiEvent>>, silent: bool) -> mpsc::Receiver<Result<FileInfo, io::Error>> {
    let (tx, rx) = mpsc::channel(5000);

    tokio::task::spawn_blocking(move || {
        for entry in WalkDir::new(base_path).max_open(100) {
            match entry {
                Ok(entry) => {
                    if entry.file_type().is_file() {
                        let file_size = entry.metadata().unwrap().len();
                        let file_path = entry.path().to_path_buf();
                        let key: String = util_layer::path_to_s3_key(file_path.to_str().unwrap());

                        if !silent {
                            if let Some(ref ui_tx) = ui_tx {
                                let add_to_total = UiEvent::AddToTotal(file_size);
                                if ui_tx.send(add_to_total).is_err() {
                                    break;
                                }
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
        if let Some(ui_tx) = ui_tx {
            let _ = ui_tx.send(UiEvent::Message("Uploading".to_string()));
        }
    });

    rx
}