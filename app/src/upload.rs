use crate::traits::{UploadRuntime, UploadTrait};
use crate::{auto_tuning, bucket, upload};
use aws_layer::{CompletedPartAws, S3Client};
use http_layer::upload_trailing::create_streaming_req_trailing_checksum;
use http_layer::{BufferPool, build_https_client, create_in_memory_body_put_with_hashes};
use std::io;
use std::io::Error;
use std::path::PathBuf;
use std::sync::Arc;

use crate::command_center::{CC, CommandCenter};
use crate::upload_multipart::MultipartUpload;
use crate::upload_singlepart::SingleUpload;
use std::sync::OnceLock;
use std::time::SystemTime;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use util_layer::cc_util::{CommandCenterEvent, Status};
use util_layer::constants;
use util_layer::constants::CHUNK_SIZE;
use util_layer::error_codes::ErrorCode::{DiskIoError, RequestFailure, Unknown};
use util_layer::error_codes::UserFacingError;
use util_layer::retry::retry_on_failure_async;
use util_layer::snowcopy_result::SnowcopyResult;
use util_layer::traits::{FileObject, HttpExecutor, HyperClient, HyperHttpExecutor, ImplFileHandler};
use util_layer::url_encoder::url_encode_fast;
use util_layer::{FileInfo, UploadConfig};
use walkdir::WalkDir;

pub(crate) async fn run_upload(config: &Arc<UploadConfig>) -> SnowcopyResult {
    let cancellation_token = CancellationToken::new();
    let cc = CommandCenter::new_upload(&config, cancellation_token.clone());
    let (cc_tx, cc_handle) = cc.start();
    let s3client = Arc::new(S3Client::new(&config.endpoint, cc_tx.clone(), config.verbose).await);
    let hyper_client = Arc::new(build_https_client());

    upload::upload(config.clone(), s3client, cc_tx, cc_handle, cancellation_token, hyper_client).await
}

pub(crate) async fn upload(ctx: Arc<UploadConfig>, s3client: Arc<S3Client>, cc_tx: UnboundedSender<CommandCenterEvent>, cc_handle: JoinHandle<SnowcopyResult>, cancellation_token: CancellationToken, hyper_client: Arc<HyperClient>) -> SnowcopyResult {
    if !ctx.local_path.to_path_buf().exists(){
        return SnowcopyResult::with_fatal(UserFacingError::new(DiskIoError, format!("Local path does not exist: {}", ctx.local_path.to_string_lossy())));
    }

    let mut files_to_upload_stream = walk_directory_async(ctx.local_path.to_path_buf(), cc_tx.clone(), cancellation_token.clone()).await;

    let (active_file_io_sem, req_sem, open_files_sem, buffer_pool_size) = auto_tuning::auto_tune_upload(ctx.clone(), cc_tx.clone());

    let task_sem = Arc::new(Semaphore::new(open_files_sem.available_permits()));
    let buffer_pool = BufferPool::new(buffer_pool_size, CHUNK_SIZE as usize);

    let bucket_creation_handle = bucket::create_bucket_if_not_exist(&ctx, &s3client);

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
        buffer_pool : Arc::new(buffer_pool),
        cc_tx: cc_tx.clone(),
        cancellation_token,
    });

    upload_files_from_stream(ctx, &mut files_to_upload_stream, task_sem, runtime, s3client, hyper_client).await;

    drop(cc_tx);
    cc_handle.await.unwrap()
}

async fn upload_files_from_stream(ctx: Arc<UploadConfig>, files_to_upload_stream: &mut Receiver<Result<FileInfo, Error>>, task_sem: Arc<Semaphore>, runtime: Arc<UploadRuntime>, aws_client: Arc<S3Client>, hyper_client: Arc<HyperClient>) {
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
                        let hyper_http_executor = HyperHttpExecutor::new(hyper_client.clone());
                        if file_info_arc.file_size > CHUNK_SIZE {
                            let multipart_upload = Box::new(MultipartUpload::new(ctx.clone(), file_info_arc.clone(), runtime.clone(), aws_client.clone(), Arc::new(hyper_http_executor), Arc::new(ImplFileHandler{})));
                            let permit = task_sem.clone().acquire_owned().await.unwrap();
                            join_set.spawn(async move {
                                multipart_upload.upload_file().await;
                                drop(permit);
                            });
                        } else {
                            let single_upload = Box::new(SingleUpload::new(ctx.clone(), file_info_arc.clone(), runtime.clone(), Arc::new(hyper_http_executor), Arc::new(ImplFileHandler{})));
                            let permit = task_sem.clone().acquire_owned().await.unwrap();
                            join_set.spawn(async move {
                                single_upload.upload_file().await;
                                drop(permit);
                            });
                        }
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

pub(crate) async fn upload_each_chunk_with_retry(ctx: &Arc<UploadConfig>, runtime: &Arc<UploadRuntime>, file_info: &Arc<FileInfo>, http_executor: Arc<dyn HttpExecutor>, file: Arc<dyn FileObject>, upload_id: &String, chunk_amount_u64: u64, completed_parts: &Arc<Vec<OnceLock<CompletedPartAws>>>) {
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
        let http_executor_cloned = http_executor.clone();

        join_set.spawn(async move {
            let result = tokio::select! {
                res = retry_on_failure_async(|| {
                    let ctx = ctx_cloned.clone();
                    let file_info = file_info_clone.clone();
                    let runtime = runtime_clone.clone();
                    let canonical_uri = canonical_uri_cloned.clone();
                    let query_uri = query_uri.clone();
                    let file = file.clone();
                    let http_executor = http_executor_cloned.clone();

                    async move {
                        if ctx.trailing {
                            let req_result = create_streaming_req_trailing_checksum(ctx, &canonical_uri, &query_uri, file, start_pos, amount, file_info.modified_time, runtime.file_sem.clone());
                            http_layer::upload_trailing::put_file_streaming(req_result.unwrap(), amount, http_executor, &runtime.req_sem).await
                        } else {
                            let req = create_in_memory_body_put_with_hashes(file, start_pos, amount, ctx, file_info, &runtime.file_sem, runtime.buffer_pool.clone(), &canonical_uri, &query_uri).await?;
                            http_layer::put_file_range(amount, http_executor, &runtime.req_sem, req).await
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

#[cfg(test)]
mod tests {
    use crate::traits::UploadRuntime;
    use crate::upload::UploadTrait;
    use crate::upload_multipart::MultipartUpload;
    use crate::upload_singlepart::SingleUpload;
    use http_layer::BufferPool;
    use hyper::http::HeaderValue;
    use hyper::{HeaderMap, StatusCode};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tokio::sync::mpsc::UnboundedSender;
    use tokio_util::sync::CancellationToken;
    use util_layer::cc_util::CommandCenterEvent;
    use util_layer::error_codes::ErrorCode::DiskIoError;
    use util_layer::traits::{MockFileHandler, MockFileObject, MockHttpExecutor};
    use util_layer::{EndpointConfig, FileInfo, HttpExecutorResponse, TransferMethod, UploadConfig};

    const MOCK_OPEN_FILE_ERROR_MSG: &'static str = "mock open failure";

    // SinglePart
    #[tokio::test]
    async fn single_upload_sends_fatal_error_when_file_open_fails() {
        let (cc_tx, mut cc_rx) = tokio::sync::mpsc::unbounded_channel();

        let file_size = 1;

        let config = Arc::new(test_upload_config(None));
        let file_info = test_file_info(file_size);
        let runtime = test_upload_runtime(cc_tx.clone());

        let mut mock_file_handler = MockFileHandler::new();
        mock_file_handler.expect_open_file().returning(|_| Err(std::io::Error::new(std::io::ErrorKind::Other, MOCK_OPEN_FILE_ERROR_MSG)));

        let single_upload = SingleUpload::new(
            config,
            file_info,
            runtime,
            Arc::new(mock_http_executor(file_size as usize)),
            Arc::new(mock_file_handler),
        );

        single_upload.upload_file().await;

        let event = cc_rx.recv().await.expect("expected fatal error event");

        match event {
            CommandCenterEvent::FatalErrorEvent(error) => {
                assert_eq!(error.code(), DiskIoError);
                assert!(
                    error.message().contains(MOCK_OPEN_FILE_ERROR_MSG),
                    "fatal error should mention the underlying open failure"
                );
            }
            _ => panic!("expected FatalErrorEvent"),
        }
    }

    #[tokio::test]
    async fn single_upload_passes() {
        let (cc_tx, mut cc_rx) = tokio::sync::mpsc::unbounded_channel();

        let expected_file_size = 1;

        let config = Arc::new(test_upload_config(None));
        let file_info = test_file_info(expected_file_size);
        let runtime = test_upload_runtime(cc_tx.clone());

        let mut mock_file_handler = MockFileHandler::new();

        mock_file_handler.expect_open_file()
            .times(1)
            .returning(move |_| Ok(Arc::new(mock_file(expected_file_size.clone() as usize))));

        let mock_http_executor = mock_http_executor(expected_file_size as usize);

        let single_upload = SingleUpload::new(
            config,
            file_info,
            runtime,
            Arc::new(mock_http_executor),
            Arc::new(mock_file_handler),
        );

        single_upload.upload_file().await;
        let mut saw_part_transferred = false;
        let mut saw_file_transferred = false;
        let mut saw_fatal_error;

        while let Ok(event) = cc_rx.try_recv() {
            match event {
                CommandCenterEvent::PartTransferred(_) => {
                    saw_part_transferred = true;
                }
                CommandCenterEvent::FileTransferred(bytes) => {
                    saw_file_transferred = true;
                    assert_eq!(bytes, expected_file_size);
                }
                CommandCenterEvent::FatalErrorEvent(err) => {
                    saw_fatal_error = true;
                    assert!(!saw_fatal_error, "unexpected fatal error: {:?}", err);
                }
                _ => {}
            }
        }

        assert!(saw_part_transferred);
        assert!(saw_file_transferred);
    }

    // Multipart
    #[tokio::test]
    async fn multipart_upload_sends_fatal_error_when_file_open_fails() {
        let (cc_tx, mut cc_rx) = tokio::sync::mpsc::unbounded_channel();

        let file_size = util_layer::constants::CHUNK_SIZE + 1; // Must be > CHUNK_SIZE for multipart

        let config = Arc::new(test_upload_config(None));
        let file_info = test_file_info(file_size);
        let runtime = test_upload_runtime(cc_tx.clone());
        let mock_s3_client = Arc::new(aws_layer::S3Client::new(&config.endpoint, cc_tx.clone(), false).await);

        let mut mock_file_handler = MockFileHandler::new();
        mock_file_handler.expect_open_file().returning(|_| Err(std::io::Error::new(std::io::ErrorKind::Other, MOCK_OPEN_FILE_ERROR_MSG)));

        let multipart_upload = MultipartUpload::new(
            config,
            file_info,
            runtime,
            mock_s3_client,
            Arc::new(mock_http_executor(file_size as usize)),
            Arc::new(mock_file_handler),
        );

        multipart_upload.upload_file().await;

        let event = cc_rx.recv().await.expect("expected fatal error event");

        match event {
            CommandCenterEvent::FatalErrorEvent(error) => {
                assert_eq!(error.code(), DiskIoError);
                assert!(
                    error.message().contains(MOCK_OPEN_FILE_ERROR_MSG),
                    "fatal error should mention the underlying open failure"
                );
            }
            _ => panic!("expected FatalErrorEvent"),
        }
    }

    #[tokio::test]
    #[ignore]
    async fn multipart_upload_passes() {
        let (cc_tx, mut cc_rx) = tokio::sync::mpsc::unbounded_channel();

        let expected_file_size = util_layer::constants::CHUNK_SIZE + 1; // Must be > CHUNK_SIZE for multipart

        let config = Arc::new(test_upload_config(None));
        let file_info = test_file_info(expected_file_size);
        let runtime = test_upload_runtime(cc_tx.clone());
        let mock_s3_client = Arc::new(aws_layer::S3Client::new(&config.endpoint, cc_tx.clone(), false).await);  // TODO LND: Mock S3Client proper

        let mut mock_file_handler = MockFileHandler::new();

        mock_file_handler.expect_open_file()
            .times(1)
            .returning(move |_| Ok(Arc::new(mock_file(expected_file_size.clone() as usize))));

        let mock_http_executor = mock_http_executor(expected_file_size as usize);

        let multipart_upload = MultipartUpload::new(
            config,
            file_info,
            runtime,
            mock_s3_client,
            Arc::new(mock_http_executor),
            Arc::new(mock_file_handler),
        );

        multipart_upload.upload_file().await;
        let mut saw_part_transferred = false;
        let mut saw_file_transferred = false;

        while let Ok(event) = cc_rx.try_recv() {
            match event {
                CommandCenterEvent::PartTransferred(_) => {
                    saw_part_transferred = true;
                }
                CommandCenterEvent::FileTransferred(bytes) => {
                    saw_file_transferred = true;
                    assert_eq!(bytes, expected_file_size);
                }
                CommandCenterEvent::FatalErrorEvent(err) => {
                    panic!("unexpected fatal error: {:?}", err);
                }
                _ => {}
            }
        }

        assert!(saw_part_transferred);
        assert!(saw_file_transferred);
    }

    // Test Helper functions
    fn test_upload_config(config: Option<UploadConfig>) -> UploadConfig {
        config.unwrap_or(
            UploadConfig {
                endpoint: EndpointConfig {
                    s3_access_key: String::new(),
                    s3_secret_key: String::new(),
                    s3_url: String::from("https://localhost.test"),
                    s3_region: String::from("us-east-1"),
                },
                md5_checksum_upload: false,
                transfer_method: TransferMethod::TokioSelfMade,
                file_limit_overwrite: None,
                req_limit_overwrite: None,
                silent: false,
                stats: false,
                trailing_chunk_size: 0,
                trailing: false,
                trailing_small: false,
                open_file_limit_overwrite: None,
                bucket: String::new(),
                local_path: PathBuf::new(),
                use_unsafe_http: true,
                verbose: false,
            }
        )
    }

    fn test_file_info(file_size: u64) -> Arc<FileInfo> {
        Arc::new(FileInfo{
            file_path: PathBuf::new(),
            file_size,
            key: String::new(),
            aws_uri_encoded_key: String::new(),
            modified_time: None,
        })
    }

    fn test_upload_runtime(cc_tx: UnboundedSender<CommandCenterEvent>) -> Arc<UploadRuntime> {
        Arc::new(UploadRuntime {
            buffer_pool: Arc::new(BufferPool::new(1, 1)),
            file_sem: Arc::new(Semaphore::new(1)),
            cancellation_token: CancellationToken::new(),
            cc_tx,
            req_sem: Semaphore::new(1),
        })
    }

    fn mock_http_executor(file_size: usize) -> MockHttpExecutor {
        let mut mock_http_executor = MockHttpExecutor::new();
        mock_http_executor.expect_execute_await().returning(move |_|
            Box::pin(async move {
                let mut headers = HeaderMap::new();
                headers.insert("etag", HeaderValue::from_str("etagValue").unwrap());
                Ok(
                    HttpExecutorResponse {
                        status: StatusCode::OK,
                        headers,
                        body: vec![0; file_size],
                    }
                )
            })
        );
        mock_http_executor
    }

    fn mock_file(len: usize) -> MockFileObject {
        let mut mock_file = MockFileObject::new();
        mock_file.expect_read_at_offset().returning(move |_, _| Ok(len));
        mock_file.expect_length().returning(move || Ok(len as u64));
        mock_file
    }
}