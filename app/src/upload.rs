use crate::auto_tuning;
use crate::command_center::{CommandCenter, CC};
use std::fs::File;
use std::io;
use std::io::Error;
use std::path::PathBuf;

use aws_layer::{CompletedPartAws, S3Client};
use http_layer::{BufferPool, HyperClient};
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use util_layer::cc_util::{CommandCenterEvent, Status};
use util_layer::constants;
use util_layer::constants::{ALPHA_FILE_SIZE_UPLOAD_LIMIT, CHUNK_SIZE};
use util_layer::error_codes::ErrorCode::{AlphaFileSizeLimitExceeded, DiskIoError, RequestFailure, Unknown};
use util_layer::error_codes::UserFacingError;
use util_layer::retry::retry_on_failure_async;
use util_layer::snow_errors::SnowCopyError;
use util_layer::snowcopy_result::SnowcopyResult;
use util_layer::url_encoder::url_encode_fast;
use util_layer::{FileInfo, UploadConfig};
use walkdir::WalkDir;

pub async fn upload(ctx: Arc<UploadConfig>) -> SnowcopyResult {
    if !ctx.local_path.to_path_buf().exists(){
        return SnowcopyResult::with_fatal(UserFacingError::new(DiskIoError, format!("Local path does not exist: {}", ctx.local_path.to_string_lossy())));
    }

    let cancellation_token = CancellationToken::new();
    let cc = CommandCenter::new_upload(&ctx, cancellation_token.clone());
    let (cc_tx, cc_handle) = cc.start();

    let mut files_to_upload_stream = walk_directory_async(ctx.local_path.to_path_buf(), cc_tx.clone(), cancellation_token.clone()).await;

    let s3client = Arc::new(S3Client::new(&ctx.endpoint, cc_tx.clone(), ctx.verbose).await);
    let bucket_creation_handle = create_bucket_if_not_exist(&ctx, &s3client);

    let hyper_client = http_layer::build_https_client();
    let (active_file_io_sem, req_sem, open_files_sem, buffer_pool_size) = auto_tuning::auto_tune_upload(ctx.clone(), cc_tx.clone());

    let task_sem = Arc::new(Semaphore::new(open_files_sem.available_permits()));
    let buffer_pool = BufferPool::new(buffer_pool_size, CHUNK_SIZE as usize);

    let bucket_created = match bucket_creation_handle.await {
        Ok(Ok(created)) => created,
        Ok(Err(e)) => {
            return SnowcopyResult::with_fatal(UserFacingError::new(RequestFailure, format!("Bucket creation failed: {}", e)));
        }
        Err(join_error) => {
            return SnowcopyResult::with_fatal(UserFacingError::new(Unknown, format!("Task join error during bucket creation: {:?}", join_error)));
        }
    };

    if !bucket_created {
        //We did not create the bucket, so check for already uploaded files
        files_to_upload_stream = filter_already_existing_files(&ctx, &cc_tx, &s3client, files_to_upload_stream, cancellation_token.clone());
    }

    let runtime = Arc::new(UploadRuntime { //TODO JON: I'm unsure if the internals should be wrapped in Arc as well.
        file_sem : Arc::new(active_file_io_sem),
        req_sem,
        hyper_client,
        buffer_pool : Arc::new(buffer_pool),
        cc_tx: cc_tx.clone(),
        cancellation_token,
    });

    upload_files_from_stream(ctx, &mut files_to_upload_stream, task_sem, runtime, s3client).await;

    drop(cc_tx);
    cc_handle.await.unwrap()
}

async fn upload_files_from_stream(ctx: Arc<UploadConfig>, files_to_upload_stream: &mut Receiver<Result<FileInfo, Error>>, task_sem: Arc<Semaphore>, runtime: Arc<UploadRuntime>, aws_client: Arc<S3Client>) {
    let mut join_set = JoinSet::new();
    loop {
        tokio::select! {
            result = files_to_upload_stream.recv() => {
                match result {
                    Some(Ok(file_info)) => {
                        let file_info_arc = Arc::new(file_info);
                        let ctx = ctx.clone();
                        let runtime = runtime.clone();
                        let aws_client = aws_client.clone();

                        let permit = task_sem.clone().acquire_owned().await.unwrap();
                        join_set.spawn(async move {
                            upload_file(ctx, aws_client, runtime, file_info_arc).await;
                            drop(permit);
                        });
                    }
                    Some(Err(e)) => {
                        runtime.cc_tx.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(DiskIoError, format!("Error walking directory: {}", e)))).expect("Failed to send message to command center. Should never happen.");
                    }
                    None => break,
                }
            }
            _ = runtime.cancellation_token.cancelled() => {
                break;
            }
        }
    }
    while let Some(_) = join_set.join_next().await {};
}

fn create_bucket_if_not_exist(ctx: &Arc<UploadConfig>, client: &Arc<S3Client>) -> JoinHandle<Result<bool, SnowCopyError>> {
    let bucket_creation_handle = {
        let client = client.clone();
        let bucket = ctx.bucket.clone();
        tokio::spawn(async move {
            match client.create_bucket(&bucket).await {
                Ok(Some(_)) => Ok(true), // Bucket created
                Ok(None) => Ok(false), // Bucket already exists
                Err(e) => Err(e),
            }
        })
    };
    bucket_creation_handle
}

async fn upload_file(ctx: Arc<UploadConfig>, aws_client: Arc<S3Client>, runtime: Arc<UploadRuntime>, file_info: Arc<FileInfo>) {
    let file = File::open(&*file_info.file_path);

    if let Err(e) = file { //Hopefully, we should be on top of ulimit through auto-tuning
        runtime.cc_tx.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(DiskIoError, format!("Error opening file {}: {}", file_info.file_path.to_str().unwrap(), e)))).expect("Failed to send message to command center. Should never happen.");
        return;
    }
    let file = Arc::new(file.unwrap());

    if file_info.file_size > CHUNK_SIZE {
        upload_with_multipart(ctx, aws_client, runtime, file_info, file).await;
    } else {
        small_file_upload(ctx, runtime, file_info, file).await;
    }
}

async fn upload_with_multipart(ctx: Arc<UploadConfig>, aws_client: Arc<S3Client>, runtime: Arc<UploadRuntime>, file_info: Arc<FileInfo>, file: Arc<File>) {
    let chunk_amount = (file_info.file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;

    let upload_id = aws_client.create_multipart_upload(&ctx.bucket, &*file_info.key, file_info.modified_time, CHUNK_SIZE).await.unwrap();

    let completed_parts: Arc<Vec<OnceLock<CompletedPartAws>>> = Arc::new(
        (0..chunk_amount)
            .map(|_| OnceLock::new())
            .collect()
    );

    upload_each_chunk_with_retry(&ctx, &runtime, &file_info, file, &upload_id, chunk_amount, &completed_parts).await;

    let completed_parts: Vec<CompletedPartAws> = Arc::try_unwrap(completed_parts).expect("Failed to unwrap Arc, there is a bug in our flow")
        .into_iter()
        .map(|slot| slot.into_inner().unwrap())
        .collect();

    aws_client.complete_multipart_upload(&ctx.bucket, &*file_info.key, &upload_id, completed_parts).await.unwrap();
    let _ = runtime.cc_tx.send(CommandCenterEvent::FileTransferred(file_info.file_size));
}

async fn upload_each_chunk_with_retry(ctx: &Arc<UploadConfig>, runtime: &Arc<UploadRuntime>, file_info: &Arc<FileInfo>, file: Arc<File>, upload_id: &String, chunk_amount_u64: u64, completed_parts: &Arc<Vec<OnceLock<CompletedPartAws>>>) {
    let mut join_set = JoinSet::new();

    let canonical_uri = Arc::new(format!("/{}/{}", ctx.bucket, file_info.aws_uri_encoded_key));

    for i in 0..chunk_amount_u64 {
        if runtime.cancellation_token.is_cancelled() {
            break;
        }
        let start_pos = i * CHUNK_SIZE;
        let amount = std::cmp::min(CHUNK_SIZE, file_info.file_size - start_pos);

        let part_number = (i + 1) as i32;
        let query_uri = format!("partNumber={}&uploadId={}", part_number, upload_id);

        let runtime_clone = runtime.clone();
        let file_info_clone = file_info.clone();
        let completed_parts = completed_parts.clone();

        let ctx_cloned = ctx.clone();
        let canonical_uri_cloned = canonical_uri.clone();
        let file = file.clone();

        join_set.spawn(async move {
            let result = tokio::select! {
                res = retry_on_failure_async(|| {
                    let ctx = ctx_cloned.clone();
                    let file_info = file_info_clone.clone();
                    let runtime = runtime_clone.clone();
                    let canonical_uri = canonical_uri_cloned.clone();
                    let query_uri = query_uri.clone();
                    let file = file.clone();

                    async move {
                        if ctx.trailing {
                            http_layer::upload_trailing::put_file_streaming(ctx, file, file_info, start_pos, amount, &runtime.hyper_client, runtime.file_sem.clone(), &runtime.req_sem, &canonical_uri, &query_uri,  runtime.cc_tx.clone()).await
                        } else {
                            http_layer::put_file_range(ctx, file, file_info, start_pos, amount, &runtime.hyper_client, &runtime.file_sem, &runtime.req_sem, runtime.buffer_pool.clone(), &canonical_uri, &query_uri, runtime.cc_tx.clone()).await
                        }
                    }
                },
                    runtime_clone.cc_tx.clone(),
                    format!("Uploading part {} of {}", part_number, file_info_clone.aws_uri_encoded_key)) => res,

                _ = runtime_clone.cancellation_token.cancelled() => {
                    return;
                }
            };

            match result {
                Ok((etag, metrics)) => {
                    let completed_part = CompletedPartAws::builder()
                        .e_tag(etag)
                        .part_number(part_number)
                        .build();

                    match completed_parts[part_number as usize - 1].set(completed_part) { //0 vs 1 indexed
                        Ok(_) => {}
                        Err(e) => {
                            CommandCenterEvent::FatalErrorEvent(UserFacingError::new(RequestFailure, format!("Error uploading part {} of file {}: {:?}", part_number, file_info_clone.file_path.to_str().unwrap(), e)));
                            return;
                        }
                    }
                    runtime_clone.cc_tx.send(CommandCenterEvent::PartTransferred(metrics)).expect("Failed to send message to command center. Should never happen.");
                }
                Err(e) => {
                    CommandCenterEvent::FatalErrorEvent(UserFacingError::new(RequestFailure, format!("Error uploading part {} of file {}: {}", part_number, file_info_clone.file_path.to_str().unwrap(), e)));
                }
            }
        });
    }

    while let Some(_) = join_set.join_next().await {}
}


async fn small_file_upload(ctx: Arc<UploadConfig>, runtime: Arc<UploadRuntime>, file_info: Arc<FileInfo>, file: Arc<File>) {
    let canonical_uri = Arc::new(format!("/{}/{}", ctx.bucket, file_info.aws_uri_encoded_key));
    let query_uri = "".to_string();

    let result = tokio::select! {
        res = retry_on_failure_async(|| {
            let ctx = ctx.clone();
            let amount = file_info.file_size;
            let file_info = file_info.clone();
            let runtime = runtime.clone();
            let canonical_uri = canonical_uri.clone();
            let query_uri = query_uri.clone();
            let file = file.clone();

            async move {
                if ctx.trailing_small {
                    http_layer::upload_trailing::put_file_streaming(ctx, file, file_info, 0, amount, &runtime.hyper_client, runtime.file_sem.clone(), &runtime.req_sem, &canonical_uri, &query_uri, runtime.cc_tx.clone()).await
                } else {
                    http_layer::put_file(ctx, file, file_info, &runtime.hyper_client, &runtime.file_sem, &runtime.req_sem, runtime.buffer_pool.clone(), &canonical_uri, &query_uri, runtime.cc_tx.clone()).await
                }
            }
        }, runtime.cc_tx.clone(),
            format!("Uploading {}", file_info.aws_uri_encoded_key)) => res,

        _ = runtime.cancellation_token.cancelled() => {
            return;
        }
    };

    match result {
        Ok(metrics) => {
            let file_size = file_info.file_size;
            runtime.cc_tx.send(CommandCenterEvent::PartTransferred(metrics.1)).expect("Failed to send message to command center. Should never happen.");
            runtime.cc_tx.send(CommandCenterEvent::FileTransferred(file_size)).expect("Failed to send message to command center. Should never happen.");
        },
        Err(e) => panic!("Error uploading file {}: {}", file_info.file_path.to_str().unwrap(), e),
    }
}

fn filter_already_existing_files(ctx: &Arc<UploadConfig>, cc_tx: &UnboundedSender<CommandCenterEvent>, client: &Arc<S3Client>, mut file_discovery_channel: Receiver<Result<FileInfo, io::Error>>, cancellation_token: CancellationToken) -> Receiver<Result<FileInfo, io::Error>> {
    let (upload_tx, upload_rx) = mpsc::channel::<Result<FileInfo, io::Error>>(constants::METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_UPLOAD as usize);

    let checker_client = client.clone();
    let checker_bucket = ctx.bucket.clone();
    let checker_task_sem = Arc::new(Semaphore::new(200)); //TODO Jon: we want this to be very fast, but there is also an issue of too many connections and exceeding ulimit. I'm unsure how this should be handled. Furthermore, how can the users understand? Is it really a problem with many connections? _req_sem was very slow on my local pc.
    let cc_tx_checker = cc_tx.clone();

    tokio::spawn(async move {
        let mut checker_set = JoinSet::new();
        loop {
            tokio::select! {
                result = file_discovery_channel.recv() => {
                    match result {
                        Some(Ok(file_info)) => {
                            let client = checker_client.clone();
                            let bucket = checker_bucket.clone();
                            let upload_tx = upload_tx.clone();
                            let permit = checker_task_sem.clone().acquire_owned().await.unwrap();
                            let cc_tx = cc_tx_checker.clone();
                            let cancellation_token = cancellation_token.clone();

                            checker_set.spawn(async move {
                                let metadata = tokio::select! {
                                    res = client.get_s3_metadata(&bucket, &file_info.key) => res,
                                    _ = cancellation_token.cancelled() => {
                                        drop(permit);
                                        return;
                                    }
                                };

                                match metadata {
                                    Ok(metadata) => {
                                        let (_exists_at_target, size, s3_mtime, _chunk_size, _etag, _sha256_checksum) = metadata;
                                        if should_upload(&file_info, size, s3_mtime).await {
                                            upload_tx.send(Ok(file_info)).await.expect("Should never fail.")
                                        } else {
                                            // Already uploaded, just let CC and ui know. We are done with the file
                                            cc_tx.send(CommandCenterEvent::FileAlreadyAtTarget(file_info.file_size)).expect("Should never fail.");
                                        }
                                       drop(permit);
                                    }
                                    Err(e) => {
                                        cc_tx.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(RequestFailure, format!("Error getting metadata for file {}: {}", file_info.file_path.to_str().unwrap(), e)))).expect("Should never fail.");
                                    }
                                }
                            });
                        }
                        Some(Err(e)) => {
                            upload_tx.send(Err(e)).await.expect("Should never fail.");
                        }
                        None => break,
                    }
                }
                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
        }
        while let Some(_) = checker_set.join_next().await {}
    });
    upload_rx
}

async fn should_upload(file_info: &FileInfo, size: Option<i64>, s3_mtime: Option<SystemTime>) -> bool {
    let Some(size) = size else {
        return true;
    };

    if size as u64 != file_info.file_size {
        return true;
    }

    let Some(s3_mtime) = s3_mtime else {
        return true;
    };

    let Some(local_mtime) = file_info.modified_time else {
        return true;
    };

    match s3_mtime.duration_since(local_mtime) {
        Ok(diff) => diff.as_secs() != 0 || diff.subsec_nanos() != 0, //Same size and mtime -> its the same file.
        Err(_) => true,
    }
}

pub async fn walk_directory_async(base_path: PathBuf, cc_tx: UnboundedSender<CommandCenterEvent>, cancellation_token: CancellationToken) -> mpsc::Receiver<Result<FileInfo, io::Error>> {
    let (tx, rx) = mpsc::channel(constants::DISCOVERY_LIMIT_UPLOAD as usize);

    if base_path.is_file() {
        let metadata = base_path.metadata().unwrap();
        let file_size = metadata.len();
        let modified_time = metadata.modified().ok();

        if report_file_too_large(&base_path, &cc_tx, file_size, ALPHA_FILE_SIZE_UPLOAD_LIMIT) {
            return rx;
        }

        cc_tx.send(CommandCenterEvent::FileDiscovered(file_size)).expect("Failed to send message to command center. Should never happen.");

        let key = base_path.file_name().unwrap().to_string_lossy().into_owned();
        let aws_uri_encoded_key = url_encode_fast(&key, true);
        let file_info = FileInfo {
            file_path: base_path,
            aws_uri_encoded_key,
            key: String::from(key),
            file_size,
            modified_time,
        };
        tx.send(Ok(file_info)).await.unwrap();
        let _ = cc_tx.send(CommandCenterEvent::StatusChanged(Status::Transferring));

        return rx;
    }

    let base_path_for_relative = base_path.clone();
    tokio::task::spawn_blocking(move || {
        for entry in WalkDir::new(base_path).follow_links(false).max_open(100) {
            if cancellation_token.is_cancelled() {
                return;
            }
            match entry {
                Ok(entry) => {
                    if entry.path_is_symlink() {
                        cc_tx.send(CommandCenterEvent::Log(format!("SKIPPING symbolic link: {}", entry.path().display()))).expect("Failed to send message to command center. Should never happen.");
                    } else if entry.file_type().is_file() {
                        let metadata = entry.metadata().unwrap();
                        let file_size = metadata.len();
                        let modified_time = metadata.modified().ok();
                        let file_path = entry.path().to_path_buf();

                        if report_file_too_large(&file_path, &cc_tx, file_size, ALPHA_FILE_SIZE_UPLOAD_LIMIT) {
                            continue
                        }

                        let relative_path = file_path.strip_prefix(&base_path_for_relative)
                            .expect("base_path_for_relative should be a prefix of file_path. This error should never occur.");

                        let key = &util_layer::path_to_s3_key(relative_path.to_str().unwrap());
                        let aws_uri_encoded_key: String = url_encode_fast(key, true);

                        cc_tx.send(CommandCenterEvent::FileDiscovered(file_size)).expect("Failed to send message to command center. Should never happen.");

                        let file_info = FileInfo { file_path, aws_uri_encoded_key, key: String::from(key), file_size, modified_time };
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

fn report_file_too_large(base_path: &PathBuf, cc_tx: &UnboundedSender<CommandCenterEvent>, file_size: u64, file_size_limit: u64) -> bool {
    if file_size > file_size_limit {
        cc_tx.send(CommandCenterEvent::Log(format!("SKIPPING file {} because it is too large ({} bytes). This is only an alpha limitation.", base_path.display(), file_size))).expect("Failed to send message to command center. Should never happen.");
        cc_tx.send(CommandCenterEvent::ErrorEvent(UserFacingError::new(AlphaFileSizeLimitExceeded, format!("One more files skipped due to size limit of {} bytes (~50GB) in alpha", file_size_limit)))).expect("Failed to send message. This should never happen.");

        return true;
    }
    false
}

pub struct UploadRuntime {
    pub file_sem: Arc<Semaphore>,
    pub req_sem: Semaphore,
    pub hyper_client: HyperClient,
    pub buffer_pool: Arc<BufferPool>,
    pub cc_tx: UnboundedSender<CommandCenterEvent>,
    pub cancellation_token: CancellationToken
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sends_limit_error_event() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        report_file_too_large(
            &PathBuf::from("large_file.zip"),
            &tx,
            100,
            50, //limit lower than file
        );

        //log event
        let event = rx.try_recv().unwrap();
        match event {
            CommandCenterEvent::Log(msg) => {
                assert!(msg.contains("large_file.zip"));
            }
            _ => panic!("Expected Log event"),
        }

        //error event
        let event = rx.try_recv().unwrap();
        match event {
            CommandCenterEvent::ErrorEvent(error) => {
                assert_eq!(error.code(), AlphaFileSizeLimitExceeded);
            }
            _ => panic!("Expected Error event"),
        }
    }
}