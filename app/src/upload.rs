use crate::command_center::{CommandCenter, CC};
use crate::{auto_tuning};
use std::io;
use std::path::{Path, PathBuf};

use aws_layer::{get_s3_mtime, AWSClient, CompletedPartAws};
use futures::future::join_all;
use http_layer::{build_https_client, BufferPool, PoolClient};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use util_layer::cc_util::{CommandCenterEvent, Status};
use util_layer::snow_errors::SnowCopyError;
use util_layer::retry::retry_on_failure_async;
use util_layer::{build_context, constants, Config, FileInfo, SnowContext};
use walkdir::WalkDir;

pub async fn run_s3_ceph_tokio(bucket: Arc<String>, path: &Path, file_limit: Option<usize>, req_limit: Option<usize>, silent: bool, stats: bool, config: &Config, destructive: bool, trailing: bool) {
    let cc = CommandCenter::new(silent, stats);
    let (cc_tx, cc_handle) = cc.start();

    let mut files_to_upload_channel = walk_directory(path.to_path_buf(), cc_tx.clone()).await;

    let client = Arc::new(aws_layer::create_client(config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str(), config.s3_region.as_str()).await);
    let bucket_creation_handle = create_bucket_if_not_exist(&bucket, &client);

    let hyper_client = Arc::new(build_https_client());
    let (file_sem, req_sem, buffer_pool_size) = auto_tuning::auto_tune_semaphores(file_limit, req_limit, path, cc_tx.clone());
    let task_sem = Arc::new(Semaphore::new(500));
    let buffer_pool = Arc::new(BufferPool::new(buffer_pool_size, constants::CHUNK_SIZE as usize));
    let context_arc = Arc::new(build_context(config.clone()));
    let mut handles = Vec::new();

    let bucket_created = bucket_creation_handle.await.unwrap();

    if !bucket_created {
        //We did not create the bucket, so check for already uploaded files
        files_to_upload_channel = check_already_exists(&bucket, &cc_tx, client.clone(), files_to_upload_channel, req_sem.clone(), destructive);
    }
    while let Some(result) = files_to_upload_channel.recv().await {
        upload_file_async_and_add_to_handles(result, context_arc.clone(), bucket.clone(), client.clone(), hyper_client.clone(), file_sem.clone(), req_sem.clone(), task_sem.clone(), buffer_pool.clone(), cc_tx.clone(), &mut handles, trailing).await;
    }

    join_all(handles).await;
    drop(cc_tx);
    cc_handle.await.unwrap();
}

fn create_bucket_if_not_exist(bucket: &Arc<String>, client: &Arc<AWSClient>) -> JoinHandle<bool> {
    let bucket_creation_handle = {
        let client = client.clone();
        let bucket = bucket.clone();
        tokio::spawn(async move {
            match aws_layer::create_bucket(&client, bucket).await {
                Ok(Some(_)) => true, // Bucket created
                Ok(None) => false, // Bucket already exists
                Err(e) => {
                    panic!("Failed to create bucket: {:?}", e);
                }
            }
        })
    };
    bucket_creation_handle
}

async fn upload_file_async_and_add_to_handles(
    result: Result<FileInfo, io::Error>,
    ctx: Arc<SnowContext>,
    bucket: Arc<String>,
    client: Arc<AWSClient>,
    hyper_client: Arc<PoolClient>,
    file_sem: Arc<Semaphore>,
    req_sem: Arc<Semaphore>,
    task_sem: Arc<Semaphore>,
    buffer_pool: Arc<BufferPool>,
    cc_tx: UnboundedSender<CommandCenterEvent>,
    handles: &mut Vec<JoinHandle<()>>,
    trailing: bool,
) {
    match result {
        Ok(file_info) => {
            let file_info_arc = Arc::new(file_info);
            let permit = task_sem.acquire_owned().await.unwrap();
            let handle = tokio::spawn(async move {
                upload_file(ctx, bucket, client, hyper_client, file_info_arc, file_sem, req_sem, buffer_pool, cc_tx, trailing).await;
                drop(permit);
            });
            handles.push(handle);
        }
        Err(e) => {
            panic!("Error walking directory: {}", e);
        }
    }
}

async fn upload_file(ctx: Arc<SnowContext>, bucket: Arc<String>, aws_client: Arc<AWSClient>, hyper_client: Arc<PoolClient>, file_info: Arc<FileInfo>, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<CommandCenterEvent>, trailing: bool) {
    if file_info.file_size > constants::CHUNK_SIZE {
        upload_with_multipart(ctx, bucket, aws_client, hyper_client, file_info, file_sem, req_sem, buffer_pool, sender, trailing).await;
    } else {
        small_file_upload(ctx, bucket, hyper_client, file_info, file_sem, req_sem, buffer_pool, sender, trailing).await;
    }
}

async fn upload_with_multipart(ctx: Arc<SnowContext>, bucket: Arc<String>, aws_client: Arc<AWSClient>, hyper_client: Arc<PoolClient>, file_info: Arc<FileInfo>, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<CommandCenterEvent>, trailing: bool) {
    let mut create_multipart_upload = aws_client.create_multipart_upload().bucket(bucket.as_ref()).key(&*file_info.key);

    if let Some(mtime) = file_info.modified_time {
        create_multipart_upload = create_multipart_upload.metadata("mtime", util_layer::time::time_to_float_string(mtime));
    }

    let create_multipart_upload_output = create_multipart_upload.send().await.unwrap();
    let upload_id = create_multipart_upload_output.upload_id().unwrap();

    let chunk_amount_u64 = (file_info.file_size + constants::CHUNK_SIZE - 1) / constants::CHUNK_SIZE;

    let completed_parts = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    let canonical_uri = Arc::new(format!("/{}/{}", bucket, file_info.key));

    for i in 0..chunk_amount_u64 {
        let start_pos = i * constants::CHUNK_SIZE;
        let amount = std::cmp::min(constants::CHUNK_SIZE, file_info.file_size - start_pos);

        let part_number = (i + 1) as i32;
        let query_uri = format!("partNumber={}&uploadId={}", part_number, upload_id);

        let hyper_client = hyper_client.clone();
        let file_info_clone = file_info.clone();
        let file_sem_clone = file_sem.clone();
        let req_sem_clone = req_sem.clone();
        let buffer_pool_clone = buffer_pool.clone();
        let completed_parts = completed_parts.clone();
        let sender_cloned = sender.clone();

        let ctx_cloned = ctx.clone();
        let canonical_uri_cloned = canonical_uri.clone();
        let handle = tokio::spawn(async move {
            let result = retry_on_failure_async(|| {
                let ctx = ctx_cloned.clone();
                let file_info = file_info_clone.clone();
                let hyper_client = hyper_client.clone();
                let file_sem = file_sem_clone.clone();
                let req_sem = req_sem_clone.clone();
                let buffer_pool = buffer_pool_clone.clone();
                let canonical_uri = canonical_uri_cloned.clone();
                let query_uri = query_uri.clone();
                async move {
                    http_layer::put_file_range(ctx, file_info, start_pos, amount, &hyper_client, file_sem, req_sem, buffer_pool, canonical_uri, &query_uri, trailing).await
                }
            }, sender_cloned.clone(), |e: &anyhow::Error| {
                if let Some(snow_error) = e.downcast_ref::<SnowCopyError>() {
                    return snow_error.is_retryable();
                }
                false
            }, format!("Uploading part {} of {}", part_number, file_info_clone.key)).await;

            match result {
                Ok((etag, metrics)) => {
                    let completed_part = CompletedPartAws::builder()
                        .e_tag(etag)
                        .part_number(part_number)
                        .build();
                    completed_parts.lock().await.push(completed_part);

                    sender_cloned.send(CommandCenterEvent::PartTransferred(metrics)).unwrap();
                }
                Err(e) => {
                    panic!("Error uploading part {} of file {}: {}", part_number, file_info_clone.file_path.to_str().unwrap(), e); //TODO JON: is this the way?
                }
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let completed_parts = Arc::try_unwrap(completed_parts).unwrap().into_inner();
    aws_layer::complete_multipart_upload(&aws_client, &bucket, &*file_info.key, upload_id, completed_parts).await.unwrap();
    let _ = sender.send(CommandCenterEvent::FileTransferred(file_info.file_size));
}

async fn small_file_upload(ctx: Arc<SnowContext>, bucket: Arc<String>, hyper_client: Arc<PoolClient>, file_info: Arc<FileInfo>, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<CommandCenterEvent>, trailing: bool) {
    let canonical_uri = Arc::new(format!("/{}/{}", bucket, file_info.key));
    let query_uri = "".to_string();

    let result = retry_on_failure_async(|| {
        let ctx = ctx.clone();
        let file_info = file_info.clone();
        let hyper_client = hyper_client.clone();
        let file_sem = file_sem.clone();
        let req_sem = req_sem.clone();
        let buffer_pool = buffer_pool.clone();
        let canonical_uri = canonical_uri.clone();
        let query_uri = query_uri.clone();
        async move {
            http_layer::put_file(ctx, file_info, &hyper_client, file_sem, req_sem, buffer_pool, canonical_uri, &query_uri, trailing).await
        }
    }, sender.clone(), |e: &anyhow::Error| {
        if let Some(snow_err) = e.downcast_ref::<SnowCopyError>() {
            return snow_err.is_retryable();
        }
        false
    }, format!("Uploading {}", file_info.key)).await;

    match result {
        Ok(metrics) => {
            let file_size = file_info.file_size;
            sender.send(CommandCenterEvent::PartTransferred(metrics)).unwrap();
            sender.send(CommandCenterEvent::FileTransferred(file_size)).unwrap();
        },
        Err(e) => panic!("Error uploading file {}: {}", file_info.file_path.to_str().unwrap(), e),
    }
}

fn check_already_exists(bucket: &Arc<String>, cc_tx: &UnboundedSender<CommandCenterEvent>, client: Arc<AWSClient>, mut file_discovery_channel: Receiver<Result<FileInfo, io::Error>>, _req_sem: Arc<Semaphore>, destructive: bool) -> Receiver<Result<FileInfo, io::Error>> {
    let (upload_tx, upload_rx) = mpsc::channel::<Result<FileInfo, io::Error>>(constants::METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_UPLOAD as usize);

    let checker_client = client.clone();
    let checker_bucket = bucket.clone();
    let checker_task_sem = Arc::new(Semaphore::new(200)); //TODO Jon: we want this to be very fast, but there is also an issue of too many connections and exceeding ulimit. I'm unsure how this should be handled. Furthermore, how can the users understand? Is it really a problem with many connections? _req_sem was very slow on my local pc.
    let cc_tx_checker = cc_tx.clone();

    tokio::spawn(async move {
        let mut checker_set = JoinSet::new();
        while let Some(result) = file_discovery_channel.recv().await {
            match result {
                Ok(file_info) => {
                    let client = checker_client.clone();
                    let bucket = checker_bucket.clone();
                    let upload_tx = upload_tx.clone();
                    let permit = checker_task_sem.clone().acquire_owned().await.unwrap();
                    let cc_tx = cc_tx_checker.clone();

                    checker_set.spawn(async move {
                        let (exists_at_target, size, s3_mtime) = get_s3_mtime(&client, &bucket, &file_info.key).await;

                        if should_upload(&file_info, size, s3_mtime).await {
                            if exists_at_target && !destructive {
                                cc_tx.send(CommandCenterEvent::NonDestructiveMode(format!("Skipping upload of already existing file {:?}", file_info.file_path))).expect("Failed to send message to command center. Should never happen.");
                            } else {
                                upload_tx.send(Ok(file_info)).await.expect("Should never fail.")
                            }
                        } else {
                            // Already uploaded, just let CC and ui know. We are done with the file
                            cc_tx.send(CommandCenterEvent::FileAlreadyAtTarget(file_info.file_size)).expect("Should never fail.");
                        }
                        drop(permit);
                    });
                }
                Err(e) => {
                    upload_tx.send(Err(e)).await.expect("Should never fail.");
                }
            }
        }
        while let Some(_) = checker_set.join_next().await {}
    });
    upload_rx
}

async fn should_upload(file_info: &FileInfo,  size: Option<i64>, s3_mtime: Option<SystemTime>) -> bool {
    let needs_upload = if let Some(s3_mtime) = s3_mtime {
        if let Some(size) = size {
            //Same size and mtime -> its the same file.
            !(size as u64 == file_info.file_size && s3_mtime.duration_since(file_info.modified_time.unwrap()).unwrap().as_secs() == 0)
        } else {
            true
        }
    } else {
        true
    };
    needs_upload
}

pub async fn walk_directory(base_path: PathBuf, cc_tx: UnboundedSender<CommandCenterEvent>) -> mpsc::Receiver<Result<FileInfo, io::Error>> {
    let (tx, rx) = mpsc::channel(constants::DISCOVERY_LIMIT_UPLOAD as usize);

    if base_path.is_file() {
        let metadata = base_path.metadata().unwrap();
        let file_size = metadata.len();
        let modified_time = metadata.modified().ok();

        if cc_tx.send(CommandCenterEvent::FileDiscovered(file_size)).is_err() {
            //TODO JON: print error? it should never fail.
        }

        let key = base_path.file_name().unwrap().to_str().unwrap().to_string();
        let file_info = FileInfo { file_path : base_path, key, file_size, modified_time };
        tx.send(Ok(file_info)).await.unwrap();
        let _ = cc_tx.send(CommandCenterEvent::StatusChanged(Status::Transferring));

        return rx;
    }

    let base_path_for_relative = base_path.clone();
    tokio::task::spawn_blocking(move || {
        for entry in WalkDir::new(base_path).follow_links(false).max_open(100) {
            match entry {
                Ok(entry) => {
                    if entry.path_is_symlink() {
                        cc_tx.send(CommandCenterEvent::Log(format!("SKIPPING symbolic link: {}", entry.path().display()))).expect("Failed to send message to command center. Should never happen.");
                    } else if entry.file_type().is_file() {
                        let metadata = entry.metadata().unwrap();
                        let file_size = metadata.len();
                        let modified_time = metadata.modified().ok();
                        let file_path = entry.path().to_path_buf();

                        let relative_path = file_path.strip_prefix(&base_path_for_relative)
                            .expect("base_path_for_relative should be a prefix of file_path. This error should never occur.");

                        let key: String = util_layer::path_to_s3_key(relative_path.to_str().unwrap());

                        cc_tx.send(CommandCenterEvent::FileDiscovered(file_size)).expect("Failed to send message to command center. Should never happen.");

                        let file_info = FileInfo { file_path, key, file_size, modified_time };
                        tx.blocking_send(Ok(file_info)).expect("Failed to send message. Should never happen.");
                    }
                }
                Err(e) => {
                    let io_error = io::Error::new(io::ErrorKind::Other, e); //TODO Jon, is this correct error type? I guess there might be some permission issues that could hit here? Not sure how to handle it.
                    tx.blocking_send(Err(io_error)).expect("Failed to send message. Should never happen.");
                }
            }
        }

        cc_tx.send(CommandCenterEvent::StatusChanged(Status::Transferring)).expect("Failed to send message to command center. Should never happen.");

    });

    rx
}