use crate::command_center::{CommandCenter, CommandCenterEvent, Status, CC};
use crate::{auto_tuning, read_config};
use std::io;
use std::path::{Path, PathBuf};

use aws_layer::{AWSClient, CompletedPartAws};
use futures::future::join_all;
use http_layer::{build_https_client, BufferPool, PoolClient};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, Mutex, Semaphore};
use util_layer::{build_context, constants, FileInfo, SnowContext};
use walkdir::WalkDir;

pub async fn run_s3_ceph_tokio(bucket: Arc<String>, path: &Path, file_limit: Option<usize>, req_limit: Option<usize>, silent: bool, stats: bool) {
    let cc = CommandCenter::new(silent, stats);
    let (cc_tx, cc_handle) = cc.start();

    let mut file_discovery_channel = walk_directory(path.to_path_buf(), cc_tx.clone()).await;

    let config = read_config();

    let client = Arc::new(aws_layer::create_client(config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str()).await);
    let bucket_creation_handle = {
        let client = client.clone();
        let bucket = bucket.clone();
        tokio::spawn(async move {
            aws_layer::create_bucket(&client, bucket).await.unwrap()
        })
    };

    let hyper_client = Arc::new(build_https_client());
    let (file_sem, req_sem, buffer_pool_size) = auto_tuning::auto_tune_semaphores(file_limit, req_limit, path, cc_tx.clone());
    let task_sem = Arc::new(Semaphore::new(500));
    let buffer_pool = Arc::new(BufferPool::new(buffer_pool_size, constants::CHUNK_SIZE as usize));
    let context_arc = Arc::new(build_context(config.clone()));
    let mut handles = Vec::new();

    bucket_creation_handle.await.unwrap();

    while let Some(result) = file_discovery_channel.recv().await {
        match result {
            Ok(file_info) => {
                let ctx = context_arc.clone();
                let bucket = bucket.clone();
                let client = client.clone();
                let hyper_client = hyper_client.clone();
                let file_sem_clone = file_sem.clone();
                let req_sem_clone = req_sem.clone();
                let buffer_pool_clone = buffer_pool.clone();
                let cc_tx_clone = cc_tx.clone();
                let file_info_arc = Arc::new(file_info);

                let permit = task_sem.clone().acquire_owned().await.unwrap();
                let handle = tokio::spawn(async move {
                    upload_file(ctx, bucket, client, hyper_client, file_info_arc, file_sem_clone, req_sem_clone, buffer_pool_clone, cc_tx_clone).await;
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
    drop(cc_tx);
    cc_handle.await.unwrap();
}

async fn upload_file(ctx: Arc<SnowContext>, bucket: Arc<String>, aws_client: Arc<AWSClient>, hyper_client: Arc<PoolClient>, file_info: Arc<FileInfo>, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<CommandCenterEvent>) {
    if file_info.file_size > constants::CHUNK_SIZE {
        upload_with_multipart(ctx, bucket, aws_client, hyper_client, file_info, file_sem, req_sem, buffer_pool, sender).await;
    } else {
        small_file_upload(ctx, bucket, hyper_client, file_info, file_sem, req_sem, buffer_pool, sender).await;
    }
}

async fn upload_with_multipart(ctx: Arc<SnowContext>, bucket: Arc<String>, aws_client: Arc<AWSClient>, hyper_client: Arc<PoolClient>, file_info: Arc<FileInfo>, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<CommandCenterEvent>) {
    let create_multipart_upload_output = aws_client.create_multipart_upload().bucket(bucket.as_ref()).key(&*file_info.key).send().await.unwrap();
    let upload_id = create_multipart_upload_output.upload_id().unwrap();

    let chunk_amount_u64 = (file_info.file_size + constants::CHUNK_SIZE - 1) / constants::CHUNK_SIZE;

    let completed_parts = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    let canonical_uri = Arc::new(format!("/{}/{}", bucket, file_info.key));

    for i in 0..chunk_amount_u64 {
        let start_pos = i * constants::CHUNK_SIZE;
        let amount = std::cmp::min(constants::CHUNK_SIZE, file_info.file_size - start_pos);

        let part_number = (i + 1) as i32;
        let query_uri = format!("?partNumber={}&uploadId={}", part_number, upload_id);

        let hyper_client = hyper_client.clone();
        let file_info_clone = file_info.clone();
        let file_sem_clone = file_sem.clone();
        let req_sem_clone = req_sem.clone();
        let buffer_pool_clone = buffer_pool.clone();
        let completed_parts = completed_parts.clone();
        let sender_cloned = sender.clone();

        let ctx_cloned = ctx.clone();
        let bucket_cloned = bucket.clone();
        let canonical_uri_cloned = canonical_uri.clone();
        let handle = tokio::spawn(async move {
            let result = http_layer::put_file_range(ctx_cloned, bucket_cloned, file_info_clone.clone(), start_pos, amount, &hyper_client, file_sem_clone, req_sem_clone, buffer_pool_clone, canonical_uri_cloned, &query_uri).await;
            match result {
                Ok((etag, metrics)) => {
                    let completed_part = CompletedPartAws::builder()
                        .e_tag(etag)
                        .part_number(part_number)
                        .build();
                    completed_parts.lock().await.push(completed_part);

                    sender_cloned.send(CommandCenterEvent::FileTransferred(metrics)).unwrap();
                }
                Err(e) => {
                    eprintln!("Error uploading part {} of file {}: {}", part_number, file_info_clone.file_path.to_str().unwrap(), e);
                    return; //TODO JON: handling
                }
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let completed_parts = Arc::try_unwrap(completed_parts).unwrap().into_inner();
    aws_layer::complete_multipart_upload(&aws_client, &bucket, &*file_info.key, upload_id, completed_parts).await.unwrap();
}

async fn small_file_upload(ctx: Arc<SnowContext>, bucket: Arc<String>, hyper_client: Arc<PoolClient>, file_info: Arc<FileInfo>, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<CommandCenterEvent>) {
    let canonical_uri = Arc::new(format!("/{}/{}", bucket, file_info.key));
    let query_uri = "".to_string();

    let result = http_layer::put_file(ctx, bucket, file_info.clone(), &hyper_client, file_sem, req_sem, buffer_pool, canonical_uri, &query_uri).await;
    match result {
        Ok(command_center_event) => sender.send(CommandCenterEvent::FileTransferred(command_center_event)).unwrap(),
        Err(e) => eprintln!("Error uploading file {}: {}", file_info.file_path.to_str().unwrap(), e), //TODO JON: send error to command_center
    }
}

pub async fn walk_directory(base_path: PathBuf, cc_tx: UnboundedSender<CommandCenterEvent>) -> mpsc::Receiver<Result<FileInfo, io::Error>> {
    let (tx, rx) = mpsc::channel(5000);

    if base_path.is_file() {
        let file_size = base_path.metadata().unwrap().len();

        if cc_tx.send(CommandCenterEvent::FileDiscovered(file_size)).is_err() {
            //TODO JON: print error? it should never fail.
        }

        let key = base_path.file_name().unwrap().to_str().unwrap().to_string();
        let file_info = FileInfo { file_path : base_path, key, file_size };
        tx.send(Ok(file_info)).await.unwrap();

        return rx;
    }

    let base_path_for_relative = base_path.clone();
    tokio::task::spawn_blocking(move || {
        for entry in WalkDir::new(base_path).max_open(100) {
            match entry {
                Ok(entry) => {
                    if entry.file_type().is_file() {
                        let file_size = entry.metadata().unwrap().len();
                        let file_path = entry.path().to_path_buf();

                        let relative_path = file_path.strip_prefix(&base_path_for_relative)
                            .expect("base_path_for_relative should be a prefix of file_path. This error should never occur.");

                        let key: String = util_layer::path_to_s3_key(relative_path.to_str().unwrap());

                        if cc_tx.send(CommandCenterEvent::FileDiscovered(file_size)).is_err() {
                            //TODO JON: print error? it should never fail.
                        }

                        let file_info = FileInfo { file_path, key, file_size };
                        if tx.blocking_send(Ok(file_info)).is_err() {
                            //TODO JON: print error? it should never fail.
                        }
                    }
                }
                Err(e) => {
                    let io_error = io::Error::new(io::ErrorKind::Other, e);
                    if tx.blocking_send(Err(io_error)).is_err() {
                        //TODO JON: print error? it should never fail.
                    }
                }
            }
        }

        let _ = cc_tx.send(CommandCenterEvent::StatusChanged(Status::Transferring));

    });

    rx
}