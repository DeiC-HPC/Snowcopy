use crate::auto_tuning::auto_tune_download;
use crate::command_center::{CC, CommandCenter};
use crate::constants::DOWNLOAD_MULTIPART_THRESHOLD;
use crate::download_multipart::MultipartDownload;
use crate::download_singlepart::SinglepartDownload;
use crate::filesystem_checker::is_valid_s3_key_for_local_filesystem;
use crate::traits::{DownloadRuntime, DownloadTrait};
use aws_layer::S3Client;
use http_layer::integrity::reconstructable_multipart_etag;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{self, Receiver, UnboundedSender};
use tokio::task::{JoinHandle, JoinSet};
use tokio::task;
use tokio_util::sync::CancellationToken;
use util_layer::cc_util::CommandCenterEvent;
use util_layer::constants::METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_DOWNLOAD;
use util_layer::download::WriteMsg;
use util_layer::error_codes::ErrorCode::{DiskIoError, RequestFailure, ServerStateChangeDuringExecution, SkippedInvalidKeysDownload};
use util_layer::error_codes::UserFacingError;
use util_layer::snowcopy_result::SnowcopyResult;
use util_layer::traits::{HttpExecutor, HyperHttpExecutor};
use util_layer::url_encoder::url_encode_fast;
use util_layer::{DownloadConfig, S3Object};

/// Ensures that the parent directory of the given path exists.
/// Uses a cache to avoid redundant filesystem operations.
fn ensure_parent_dirs_exist(file_path: &Path, created_dirs: &mut HashSet<PathBuf>) -> std::io::Result<()> {
    if let Some(parent) = file_path.parent() {
        let parent_buf = parent.to_path_buf();

        // Check if we've already created this directory
        if !created_dirs.contains(&parent_buf) {
            // Create the directory and all its parents if needed
            fs::create_dir_all(&parent_buf)?;
            // Mark this path as created
            created_dirs.insert(parent_buf);
        }
    }
    Ok(())
}

pub async fn run_download(ctx: Arc<DownloadConfig>) -> SnowcopyResult {
    let cancellation_token = CancellationToken::new();
    let cc = CommandCenter::new_download(&ctx, cancellation_token.clone());
    let (cc_tx, cc_handle) = cc.start();

    let client = Arc::new(S3Client::new(&ctx.endpoint, cc_tx.clone(), ctx.verbose).await);
    let hyper_http_executor = Arc::new(HyperHttpExecutor::new(Arc::new(http_layer::build_https_client())));

    download(ctx, client, cc_tx, cc_handle, cancellation_token, hyper_http_executor).await
}

pub async fn download(
    ctx: Arc<DownloadConfig>,
    client: Arc<S3Client>,
    cc_tx: UnboundedSender<CommandCenterEvent>,
    cc_handle: JoinHandle<SnowcopyResult>,
    cancellation_token: CancellationToken,
    http_executor: Arc<dyn HttpExecutor>,
) -> SnowcopyResult {
    let (file_sem, req_sem, download_file_concurrency) = auto_tune_download(ctx.clone(), cc_tx.clone());
    let task_sem = Arc::new(Semaphore::new(req_sem.available_permits() * 2));
    let (disk_write_tx, disk_write_rx) = mpsc::channel::<WriteMsg>(2048); //TODO JON: tune + configure buffer size. This matters also for file descriptor limit and memory usage

    spawn_global_writers(disk_write_rx, file_sem.available_permits(), cc_tx.clone(), cancellation_token.clone());

    let object_channel = client.stream_head_objects_from_bucket_in_separate_task(&ctx.bucket, &ctx.prefix, &ctx.local_path, cancellation_token.clone()).await;
    let mut object_channel = fetch_object_metadata_and_filter_already_downloaded(&ctx, &cc_tx, client, object_channel, cancellation_token.clone()).await;

    let runtime = Arc::new(DownloadRuntime {
        req_sem,
        disk_write_tx: disk_write_tx.clone(),
        cc_tx: cc_tx.clone(),
        cancellation_token,
        download_file_concurrency,
    });

    download_objects_from_stream(ctx, &mut object_channel, task_sem, runtime, http_executor).await;

    drop(disk_write_tx);
    drop(cc_tx);
    cc_handle.await.unwrap()
}

async fn download_objects_from_stream(
    ctx: Arc<DownloadConfig>,
    object_channel: &mut Receiver<S3Object>,
    task_sem: Arc<Semaphore>,
    runtime: Arc<DownloadRuntime>,
    http_executor: Arc<dyn HttpExecutor>,
) {
    let mut created_dirs: HashSet<PathBuf> = HashSet::new();
    let mut join_set = JoinSet::new();

    // Process objects from bucket
    while let Some(object) = object_channel.recv().await {
        // Ensure parent directories exist
        if let Err(e) = ensure_parent_dirs_exist(&object.local_path, &mut created_dirs) {
            runtime.cc_tx.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(
                DiskIoError,
                format!("Failed to create directories for {:?}: {}", object.local_path, e),
            ))).expect("Failed to send message. This should never happen.");
            continue;
        }

        let permit = task_sem.clone().acquire_owned().await.unwrap();
        let ctx = ctx.clone();
        let runtime = runtime.clone();
        let http_executor = http_executor.clone();
        let object_arc = Arc::new(object);

        join_set.spawn(async move {
            download_object(ctx, runtime, http_executor, object_arc).await;
            drop(permit);
        });
    }

    //TODO Jon: should we just check for errors here and react to them? But we would still like a general way to deal with errors and present them to the user. It could be that the command center did not care about errors at all. But the functioning program execution only
    //TODO Jon: The object streams could propagate their errors and then we would have a single place that all errors are propagated to.
    //TODO JON: The current design have us calling FATAL_ERROR, but still having to return an error. Its ugly because its hard to understand the error flow.
    while let Some(_) = join_set.join_next().await {}
}

async fn download_object(
    ctx: Arc<DownloadConfig>,
    runtime: Arc<DownloadRuntime>,
    http_executor: Arc<dyn HttpExecutor>,
    object: Arc<S3Object>,
) {
    let multipart_integrity_verification = ctx.md5_checksum_integrity && reconstructable_multipart_etag(&object);
    let checksum_sha256_not_none = object.sha256_checksum.is_some();

    if (object.size > DOWNLOAD_MULTIPART_THRESHOLD as i64 && checksum_sha256_not_none) || multipart_integrity_verification { //if sha256_checksum is present without multipart-etag download the simple way
        MultipartDownload::new(ctx, runtime, http_executor, object, multipart_integrity_verification)
            .download_file()
            .await;
    } else {
        //simple download
        SinglepartDownload::new(ctx, runtime, http_executor, object)
            .download_file()
            .await;
    }
}

async fn fetch_object_metadata_and_filter_already_downloaded(config: &Arc<DownloadConfig>, cc_tx: &UnboundedSender<CommandCenterEvent>, client: Arc<S3Client>, mut object_channel: Receiver<S3Object>, token: CancellationToken) -> Receiver<S3Object> {
    let (download_tx, download_rx) = mpsc::channel::<S3Object>(METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_DOWNLOAD as usize);

    let checker_client = client.clone();
    let checker_bucket = config.bucket.clone();
    let checker_task_sem = Arc::new(Semaphore::new(200)); //TODO JON: auto tuned?
    let cc_sender = cc_tx.clone();

    tokio::spawn(async move {
        let warning_has_been_sent = Arc::new(AtomicBool::new(false));

        let mut checker_set = JoinSet::new();
        while let Some(object) = object_channel.recv().await {
            if token.is_cancelled() {
                break;
            }
            let cc_sender_clone = cc_sender.clone();

            if !is_valid_s3_key_for_local_filesystem(&object.key){
                cc_sender.send(CommandCenterEvent::Log(format!("WARNING: Skipping invalid S3 key. It cannot be mapped to a local file: {}", &object.key))).expect("Failed to send message. This should never happen.");
                cc_sender.send(CommandCenterEvent::ErrorEvent(UserFacingError::new(SkippedInvalidKeysDownload, format!("One or more S3 objects skipped, as they cannot be mapped to a local file: {}", &object.key)))).expect("Failed to send message. This should never happen.");
                continue;
            }
            let _ = cc_sender.send(CommandCenterEvent::FileDiscovered(object.size as u64));

            let client = checker_client.clone();
            let bucket = checker_bucket.clone();
            let download_tx = download_tx.clone();
            let permit = checker_task_sem.clone().acquire_owned().await.unwrap();
            let warning_has_been_sent = warning_has_been_sent.clone();

            checker_set.spawn(async move {
                let metadata = client.get_s3_metadata(&bucket, &object.key).await;

                match metadata {
                    Ok((exists_at_target, _, mtime, chunk_size, etag, sha256)) => {
                        drop(permit);
                        if !exists_at_target {
                            cc_sender_clone.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(ServerStateChangeDuringExecution, format!("S3 object went missing from s3 server during the execution of the program: {}", object.key)))).expect("Failed to send message. This should never happen.");
                            return;
                        }
                        let mut object = object;
                        object.modified_time = mtime;
                        object.chunk_size = chunk_size;
                        object.etag = etag;
                        object.sha256_checksum = sha256;
                        object.aws_uri_encoded_key = url_encode_fast(&object.key, true);

                        let should_download = should_download_object(&mut object);

                        if should_download {
                            if object.chunk_size.is_none() && object.sha256_checksum.is_none() {
                                let already_sent = warning_has_been_sent.swap(true, Ordering::Relaxed);
                                if !already_sent {
                                    cc_sender_clone.send(CommandCenterEvent::Log("Lack of Snowcopy file metadata detected for some files - file integrity checks are reduced".to_string())).expect("Failed to send message. This should never happen.");
                                }
                            }

                            if let Err(e) = download_tx.send(object).await {
                                cc_sender_clone.send(CommandCenterEvent::Log(format!("Failed to forward object to download queue: {}", e))).expect("Failed to send message. This should never happen.");
                            }
                        } else {
                            // Already downloaded, just update progress for the UI
                            cc_sender_clone.send(CommandCenterEvent::FileAlreadyAtTarget(object.size as u64)).expect("Failed to send message. This should never happen.");
                        }
                    }
                    Err(e) => {
                        cc_sender_clone.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(RequestFailure, format!("Failed to get metadata for object: {}. Error: {}", object.key, e)))).expect("Failed to send message. This should never happen.");
                        return;
                    }
                }
            });
        }
        while let Some(_) = checker_set.join_next().await {}
    });
    download_rx
}

fn should_download_object(object: &mut S3Object) -> bool {
    let result = fs::metadata(&object.local_path);

    let needs_download = if let Ok(metadata) = result {
        if metadata.len() == object.size as u64 {
            if let Some(s3_mtime) = object.modified_time {
                if let Ok(local_mtime) = metadata.modified() {
                    if local_mtime == s3_mtime {
                        false // local copy matches
                    } else {
                        true
                    }
                } else {
                    true
                }
            } else {
                true
            }
        } else {
            true
        }
    } else {
        true
    };
    needs_download
}

pub fn spawn_global_writers(
    rx: Receiver<WriteMsg>,
    num_writers: usize,
    cc_sender: UnboundedSender<CommandCenterEvent>,
    token: CancellationToken
) {
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    for _ in 0..num_writers {
        let rx = rx.clone();
        let cc_sender = cc_sender.clone();
        let token = token.clone();

        task::spawn_blocking(move || {
            loop {
                if token.is_cancelled() {
                    break;
                }

                let msg = {
                    let mut rx_lock = rx.blocking_lock();
                    rx_lock.blocking_recv()
                };

                match msg {
                    Some((file, offset, data, tracker)) => {
                        if let Err(e) = &file.write_at_offset(&data, offset) {
                            eprintln!("write error: {}", e); //TODO JON: Should we just retry? But what kind of local IO error would benefit from that?
                            //probably panic?
                        } else {
                            tracker.complete_write(&file, data.len(), cc_sender.clone());
                            cc_sender.send(CommandCenterEvent::PartTransferred(data.len() as u64)).unwrap();
                        }
                    }
                    None => break,
                }
            }
        });
    }
}


#[cfg(unix)]
impl WriteAt for std::fs::File {
    fn write_at_offset(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        use std::os::unix::fs::FileExt;
        self.write_at(buf, offset)
    }
}

#[cfg(windows)]
impl WriteAt for File {
    fn write_at_offset(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        use std::os::windows::fs::FileExt;
        self.seek_write(buf, offset)
    }
}

trait WriteAt { //Abstract to make it more testable
    fn write_at_offset(&self, buf: &[u8], offset: u64) -> std::io::Result<usize>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download_multipart::MultipartDownload;
    use crate::download_singlepart::SinglepartDownload;
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::http::HeaderValue;
    use hyper::{HeaderMap, StatusCode};
    use openssl::hash::MessageDigest;
    use std::collections::HashSet;
    use std::convert::Infallible;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tokio::sync::Semaphore;
    use tokio::sync::mpsc::{self, UnboundedSender};
    use tokio_util::sync::CancellationToken;
    use util_layer::cc_util::CommandCenterEvent;
    use util_layer::download::WriteMsg;
    use util_layer::traits::MockHttpExecutor;
    use util_layer::{
        DownloadConfig, EndpointConfig, HttpExecutorResponse, HttpExecutorStreamingResponse,
        S3Object, TransferMethod,
    };

    // Helper functions
    fn test_download_config(config: Option<DownloadConfig>) -> DownloadConfig {
        config.unwrap_or(DownloadConfig {
            endpoint: EndpointConfig {
                s3_access_key: String::new(),
                s3_secret_key: String::new(),
                s3_url: String::from("https://localhost.test"),
                s3_region: String::from("us-east-1"),
            },
            md5_checksum_integrity: true,
            transfer_method: TransferMethod::TokioSelfMade,
            file_limit_overwrite: None,
            req_limit_overwrite: None,
            download_concurrency_overwrite: None,
            silent: false,
            stats: false,
            use_unsafe_http: true,
            verbose: false,
            open_file_limit_overwrite: None,
            bucket: String::from("test-bucket"),
            local_path: PathBuf::new(),
            prefix: String::new(),
        })
    }

    fn test_s3_object(
        key: &str,
        size: i64,
        sha256: Option<String>,
        etag: Option<String>,
    ) -> Arc<S3Object> {
        Arc::new(S3Object {
            key: key.to_string(),
            aws_uri_encoded_key: key.to_string(),
            local_path: PathBuf::from(key),
            size,
            modified_time: None,
            chunk_size: None,
            etag,
            sha256_checksum: sha256,
        })
    }

    fn test_download_runtime(
        cc_tx: UnboundedSender<CommandCenterEvent>,
        disk_write_tx: mpsc::Sender<WriteMsg>,
    ) -> Arc<DownloadRuntime> {
        Arc::new(DownloadRuntime {
            req_sem: Arc::new(Semaphore::new(10)),
            disk_write_tx,
            cc_tx,
            cancellation_token: CancellationToken::new(),
            download_file_concurrency: 2,
        })
    }

    fn mock_http_executor(response_body: Vec<u8>) -> MockHttpExecutor {
        let mut mock_http_executor = MockHttpExecutor::new();
        let body_bytes = Bytes::from(response_body.clone());
        let body_vec = response_body;

        mock_http_executor.expect_execute_await().returning(move |_| {
            let body = body_vec.clone();
            Box::pin(async move {
                let mut headers = HeaderMap::new();
                headers.insert("etag", HeaderValue::from_str("etagValue").unwrap());
                Ok(HttpExecutorResponse {
                    status: StatusCode::OK,
                    headers,
                    body,
                })
            })
        });

        mock_http_executor.expect_execute_stream().returning(move |_| {
            let body = body_bytes.clone();
            Box::pin(async move {
                let mut headers = HeaderMap::new();
                headers.insert("etag", HeaderValue::from_str("etagValue").unwrap());

                let body = Full::new(body)
                    .map_err(|never: Infallible| match never {})
                    .boxed();

                Ok(HttpExecutorStreamingResponse {
                    status: StatusCode::OK,
                    headers,
                    body,
                })
            })
        });

        mock_http_executor
    }

    // Singlepart tests
    #[tokio::test]
    async fn singlepart_download_passes() {
        let (cc_tx, mut cc_rx) = mpsc::unbounded_channel();
        let (disk_write_tx, mut disk_write_rx) = mpsc::channel(100);

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("singlepart_test.bin");

        let body = vec![1u8, 2, 3, 4, 5];
        let file_size = body.len() as i64;
        let sha256 = hex::encode(openssl::hash::hash(MessageDigest::sha256(), &body).unwrap());

        let mut object = (*test_s3_object("singlepart_test.bin", file_size, Some(sha256), None)).clone();
        object.local_path = file_path;
        let object = Arc::new(object);

        let config = Arc::new(test_download_config(None));
        let runtime = test_download_runtime(cc_tx, disk_write_tx);
        let mock_http = mock_http_executor(body.clone());

        let download = SinglepartDownload::new(config, runtime, Arc::new(mock_http), object);
        download.download_file().await;

        let mut received_bytes = Vec::new();
        while let Ok((_file, offset, data, _tracker)) = disk_write_rx.try_recv() {
            assert_eq!(offset, 0);
            received_bytes.extend_from_slice(&data);
        }
        assert_eq!(received_bytes, body);

        let mut saw_integrity_checked = false;

        while let Ok(event) = cc_rx.try_recv() {
            match event {
                CommandCenterEvent::FileIntegrityChecked(true) => {
                    saw_integrity_checked = true;
                }
                CommandCenterEvent::FatalErrorEvent(err) => {
                    panic!("unexpected fatal error: {:?}", err);
                }
                _ => {}
            }
        }

        assert!(saw_integrity_checked);
    }

    #[tokio::test]
    async fn singlepart_download_integrity_mismatch_retries_and_logs() {
        let (cc_tx, mut cc_rx) = mpsc::unbounded_channel();
        let (disk_write_tx, mut _disk_write_rx) = mpsc::channel(100);

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("singlepart_mismatch.bin");

        let body = vec![1u8, 2, 3, 4, 5];
        let file_size = body.len() as i64;
        let expected_wrong_sha256 = "0000000000000000000000000000000000000000000000000000000000000000".to_string();

        let mut object = (*test_s3_object("singlepart_mismatch.bin", file_size, Some(expected_wrong_sha256), None)).clone();
        object.local_path = file_path;
        let object = Arc::new(object);

        let config = Arc::new(test_download_config(None));
        let runtime = test_download_runtime(cc_tx, disk_write_tx);
        let mock_http = mock_http_executor(body.clone());

        let download = SinglepartDownload::new(config, runtime, Arc::new(mock_http), object);
        download.download_file().await;

        let mut log_messages = Vec::new();
        let mut saw_integrity_checked = false;

        while let Ok(event) = cc_rx.try_recv() {
            match event {
                CommandCenterEvent::Log(msg) => {
                    log_messages.push(msg);
                }
                CommandCenterEvent::FileIntegrityChecked(true) => {
                    saw_integrity_checked = true;
                }
                _ => {}
            }
        }

        assert!(!saw_integrity_checked, "integrity check should not pass on mismatch");
        assert!(
            log_messages.iter().any(|msg| msg.contains("Integrity check failed")),
            "expected integrity check failed log"
        );
        assert!(
            log_messages.iter().any(|msg| msg.contains("Attempting again")),
            "expected retry log"
        );
    }

    #[tokio::test]
    async fn singlepart_download_handles_cancellation() {
        let (cc_tx, _cc_rx) = mpsc::unbounded_channel();
        let (disk_write_tx, _disk_write_rx) = mpsc::channel(100);

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("singlepart_cancelled.bin");

        let body = vec![1u8, 2, 3, 4, 5];
        let file_size = body.len() as i64;

        let mut object = (*test_s3_object("singlepart_cancelled.bin", file_size, None, None)).clone();
        object.local_path = file_path;
        let object = Arc::new(object);

        let config = Arc::new(test_download_config(None));
        let runtime = test_download_runtime(cc_tx, disk_write_tx);
        runtime.cancellation_token.cancel();

        let mock_http = mock_http_executor(body.clone());
        let download = SinglepartDownload::new(config, runtime, Arc::new(mock_http), object);

        // Should return cleanly without panic or hang
        download.download_file().await;
    }

    // Multipart tests
    #[tokio::test]
    async fn multipart_download_passes() {
        let (cc_tx, mut cc_rx) = mpsc::unbounded_channel();
        let (disk_write_tx, mut disk_write_rx) = mpsc::channel(100);

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("multipart_test.bin");

        let chunk_size = 10usize;
        let num_parts = 2;
        let total_size = (chunk_size * num_parts) as i64;
        let part_body = vec![42u8; chunk_size];

        let part_md5 = openssl::hash::hash(MessageDigest::md5(), &part_body).unwrap().to_vec();
        let expected_etag = http_layer::integrity::reconstruct_multipart_etag(&[part_md5.clone(), part_md5.clone()]);

        let mut object = (*test_s3_object("multipart_test.bin", total_size, None, Some(format!("\"{}\"", expected_etag)))).clone();
        object.local_path = file_path;
        object.chunk_size = Some(chunk_size);
        let object = Arc::new(object);

        let config = Arc::new(test_download_config(None));
        let runtime = test_download_runtime(cc_tx, disk_write_tx);
        let mock_http = mock_http_executor(part_body);

        let download = MultipartDownload::new(config, runtime, Arc::new(mock_http), object, true);
        download.download_file().await;

        let mut received_offsets = Vec::new();
        let mut total_bytes_received = 0;
        while let Ok((_file, offset, data, _tracker)) = disk_write_rx.try_recv() {
            received_offsets.push(offset);
            total_bytes_received += data.len();
        }
        received_offsets.sort();
        assert_eq!(received_offsets, vec![0, 10]);
        assert_eq!(total_bytes_received, total_size as usize);

        let mut saw_integrity_checked = false;
        while let Ok(event) = cc_rx.try_recv() {
            match event {
                CommandCenterEvent::FileIntegrityChecked(true) => {
                    saw_integrity_checked = true;
                }
                CommandCenterEvent::FatalErrorEvent(err) => {
                    panic!("unexpected fatal error: {:?}", err);
                }
                _ => {}
            }
        }
        assert!(saw_integrity_checked);
    }

    #[tokio::test]
    async fn multipart_download_etag_verification_failure() {
        let (cc_tx, mut cc_rx) = mpsc::unbounded_channel();
        let (disk_write_tx, mut _disk_write_rx) = mpsc::channel(100);

        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("multipart_mismatch.bin");

        let chunk_size = 10usize;
        let num_parts = 2;
        let total_size = (chunk_size * num_parts) as i64;
        let part_body = vec![42u8; chunk_size];

        let invalid_etag = "\"00000000000000000000000000000000-2\"".to_string();

        let mut object = (*test_s3_object("multipart_mismatch.bin", total_size, None, Some(invalid_etag))).clone();
        object.local_path = file_path;
        object.chunk_size = Some(chunk_size);
        let object = Arc::new(object);

        let config = Arc::new(test_download_config(None));
        let runtime = test_download_runtime(cc_tx, disk_write_tx);
        let mock_http = mock_http_executor(part_body);

        let download = MultipartDownload::new(config, runtime, Arc::new(mock_http), object, true);
        download.download_file().await;

        let mut log_messages = Vec::new();
        let mut saw_integrity_checked = false;

        while let Ok(event) = cc_rx.try_recv() {
            match event {
                CommandCenterEvent::Log(msg) => {
                    log_messages.push(msg);
                }
                CommandCenterEvent::FileIntegrityChecked(true) => {
                    saw_integrity_checked = true;
                }
                _ => {}
            }
        }

        assert!(!saw_integrity_checked);
        assert!(
            log_messages.iter().any(|msg| msg.contains("Integrity check failed")),
            "expected integrity check failure log"
        );
        assert!(
            log_messages.iter().any(|msg| msg.contains("Attempting again")),
            "expected retry log"
        );
    }

    // Utility / Metadata Filtering tests
    #[test]
    fn should_download_object_when_file_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("non_existent_file.bin");

        let mut object = (*test_s3_object("non_existent_file.bin", 100, None, None)).clone();
        object.local_path = file_path;

        assert!(should_download_object(&mut object));
    }

    #[test]
    fn should_download_object_skips_when_size_and_mtime_match() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("existing_matching.bin");
        fs::write(&file_path, b"0123456789").unwrap();

        let metadata = fs::metadata(&file_path).unwrap();
        let mtime = metadata.modified().unwrap();

        let mut object = (*test_s3_object("existing_matching.bin", 10, None, None)).clone();
        object.local_path = file_path;
        object.modified_time = Some(mtime);

        assert!(!should_download_object(&mut object));
    }

    #[test]
    fn should_download_object_downloads_when_size_or_mtime_differs() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("existing_diff.bin");
        fs::write(&file_path, b"0123456789").unwrap();

        let metadata = fs::metadata(&file_path).unwrap();
        let mtime = metadata.modified().unwrap();

        // Size differs
        let mut object_diff_size = (*test_s3_object("existing_diff.bin", 20, None, None)).clone();
        object_diff_size.local_path = file_path.clone();
        object_diff_size.modified_time = Some(mtime);
        assert!(should_download_object(&mut object_diff_size));

        // Mtime differs
        let mut object_diff_mtime = (*test_s3_object("existing_diff.bin", 10, None, None)).clone();
        object_diff_mtime.local_path = file_path.clone();
        object_diff_mtime.modified_time = Some(SystemTime::UNIX_EPOCH);
        assert!(should_download_object(&mut object_diff_mtime));

        // Mtime missing on S3 object
        let mut object_no_mtime = (*test_s3_object("existing_diff.bin", 10, None, None)).clone();
        object_no_mtime.local_path = file_path;
        object_no_mtime.modified_time = None;
        assert!(should_download_object(&mut object_no_mtime));
    }

    #[test]
    fn ensure_parent_dirs_exist_creates_directories_and_caches() {
        let temp_dir = tempfile::tempdir().unwrap();
        let target_file = temp_dir.path().join("dir_a").join("dir_b").join("file.bin");
        let mut created_dirs = HashSet::new();

        let parent = target_file.parent().unwrap().to_path_buf();
        assert!(!parent.exists());

        ensure_parent_dirs_exist(&target_file, &mut created_dirs).unwrap();

        assert!(parent.exists());
        assert!(created_dirs.contains(&parent));

        // Second call uses cache and succeeds
        ensure_parent_dirs_exist(&target_file, &mut created_dirs).unwrap();
    }
}