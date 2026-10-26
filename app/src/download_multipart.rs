use crate::traits::{DownloadRuntime, DownloadTrait};
use http_layer::integrity::IntegrityAlgorithm;
use std::fs::File;
use std::sync::Arc;
use tokio::select;
use tokio::task::JoinSet;
use util_layer::cc_util::CommandCenterEvent;
use util_layer::constants::DOWNLOAD_MULTIPART_THRESHOLD;
use util_layer::download::FileCompletionTracker;
use util_layer::error_codes::ErrorCode::DiskIoError;
use util_layer::error_codes::UserFacingError;
use util_layer::retry::retry_on_failure_async;
use util_layer::snow_errors::SnowCopyError;
use util_layer::snow_errors::SnowCopyError::IntegrityError;
use util_layer::traits::HttpExecutor;
use util_layer::{DownloadConfig, S3Object};

pub struct MultipartDownload {
    ctx: Arc<DownloadConfig>,
    runtime: Arc<DownloadRuntime>,
    http_executor: Arc<dyn HttpExecutor>,
    object: Arc<S3Object>,
    multipart_integrity_verification: bool,
}

impl MultipartDownload {
    pub(crate) fn new(
        ctx: Arc<DownloadConfig>,
        runtime: Arc<DownloadRuntime>,
        http_executor: Arc<dyn HttpExecutor>,
        object: Arc<S3Object>,
        multipart_integrity_verification: bool,
    ) -> Self {
        Self {
            ctx,
            runtime,
            http_executor,
            object,
            multipart_integrity_verification,
        }
    }

    async fn download_files_in_part_and_check_integrity(
        &self,
        file: Arc<File>,
        chunk_size: u64,
        num_parts: u64,
        hash_algo: IntegrityAlgorithm,
    ) -> Result<(), SnowCopyError> {
        let mut download_set = self.download_file_in_parts(file, chunk_size, num_parts, hash_algo);

        let mut part_hashes = vec![None; num_parts as usize];
        while let Some(res) = download_set.join_next().await {
            match res {
                Ok(Ok((part_index, _bytes, hash))) => {
                    part_hashes[part_index as usize] = hash; //Maintain part order to ensure correct integrity verification
                }
                Ok(Err(SnowCopyError::Canceled)) => {
                    return Ok(()); //TODO JON: What is the best option here?
                }
                Ok(Err(e)) => {
                    panic!("Part download failed for {}: {}", self.object.aws_uri_encoded_key, e);
                }
                Err(e) => {
                    panic!("Join error for {}: {}", self.object.aws_uri_encoded_key, e);
                }
            }
        }

        if self.multipart_integrity_verification {
            let part_md5s: Vec<Vec<u8>> = part_hashes
                .into_iter()
                .map(|h| {
                    hex::decode(h.expect("All parts must have a hash if integrity verification is enabled"))
                        .expect("Hash should be valid hex")
                })
                .collect();

            let reconstructed_etag = http_layer::integrity::reconstruct_multipart_etag(&part_md5s);
            let expected_etag = self.object.etag.as_deref().map(|e| e.trim_matches('"')).unwrap_or("");

            if reconstructed_etag != expected_etag {
                let error_msg = format!(
                    "Integrity check failed for {}. Expected {}, got {}",
                    self.object.aws_uri_encoded_key, expected_etag, reconstructed_etag
                );
                self.runtime.cc_tx.send(CommandCenterEvent::Log(error_msg.clone())).expect("Failed to send message");
                return Err(IntegrityError(error_msg));
            } else {
                self.runtime.cc_tx.send(CommandCenterEvent::FileIntegrityChecked(true)).expect("Failed to send message");
            }
        }

        Ok(())
    }

    fn download_file_in_parts(
        &self,
        file: Arc<File>,
        chunk_size: u64,
        num_parts: u64,
        hash_algo: IntegrityAlgorithm,
    ) -> JoinSet<Result<(u64, u64, Option<String>), SnowCopyError>> {
        let total_size = self.object.size as u64;
        let tracker = Arc::new(FileCompletionTracker {
            pending_bytes_to_be_written: std::sync::atomic::AtomicU64::new(total_size),
            mtime: self.object.modified_time,
            total_size,
        });

        let mut download_set = JoinSet::new();
        let canonical_uri = Arc::new(format!("/{}/{}", self.ctx.bucket, self.object.aws_uri_encoded_key));

        for i in 0..num_parts {
            let start = i * chunk_size;
            let end = std::cmp::min(start + chunk_size - 1, total_size - 1);

            let config = self.ctx.clone();
            let http_executor = self.http_executor.clone();
            let req_sem = self.runtime.req_sem.clone();
            let canonical_uri = canonical_uri.clone();
            //let query_uri = String::new(); //TODO JON: when would we need this?
            let file = file.clone();
            let disk_write_tx = self.runtime.disk_write_tx.clone();
            let tracker = tracker.clone();
            let hash_algo = hash_algo.clone();

            let cc_sender = self.runtime.cc_tx.clone();
            let cancellation_token = self.runtime.cancellation_token.clone();
            download_set.spawn(async move {
                let res = select! {
                    res = retry_on_failure_async(|| {
                        let config = config.clone();
                        let http_executor = http_executor.clone();
                        let req_sem = req_sem.clone();
                        let canonical_uri = canonical_uri.clone();
                        let query_uri = String::new();
                        let file = file.clone();
                        let disk_write_tx = disk_write_tx.clone();
                        let tracker = tracker.clone();
                        let hash_algo = hash_algo.clone();
                        let cc_sender = cc_sender.clone();

                        async move {
                            http_layer::download::download_file_range(start, end, config, &http_executor, req_sem, &canonical_uri, &query_uri, file, disk_write_tx, tracker, hash_algo, cc_sender).await
                        }
                    }, cc_sender.clone(), format!("Downloading part (bytes {} to {}) of {}", start, end, canonical_uri)) => res,

                    _ = cancellation_token.cancelled() => {
                        return Err(SnowCopyError::Canceled);
                    }
                };

                res.map(|(bytes, hash)| (i, bytes, hash))
            });
        }
        download_set
    }
}

impl DownloadTrait for MultipartDownload {
    async fn download_file(&self) {
        let file_path = self.object.local_path.clone();
        let key = self.object.aws_uri_encoded_key.clone();
        let total_size = self.object.size as u64;

        let file = match File::create(&file_path) {
            Ok(f) => f,
            Err(e) => {
                self.runtime.cc_tx.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(DiskIoError, format!("Failed to create file {:?}: {}", file_path, e)))).expect("Failed to send message. This should never happen.");
                return;
            }
        };

        if let Err(e) = file.set_len(total_size) {
            self.runtime.cc_tx.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(DiskIoError, format!("Failed to truncate file {:?} to {}: {}", file_path, total_size, e)))).expect("Failed to send message. This should never happen.");
            return;
        }

        let file = Arc::new(file);

        let ideal_chunk_size = total_size.div_ceil(self.runtime.download_file_concurrency as u64);
        let mut chunk_size = std::cmp::max(ideal_chunk_size, DOWNLOAD_MULTIPART_THRESHOLD);
        if self.multipart_integrity_verification {
            chunk_size = self.object.chunk_size.unwrap() as u64;
        } else {
            self.runtime.cc_tx.send(CommandCenterEvent::Log(format!("Skipping integrity check for file: {}, etag: {}", key, self.object.etag.clone().unwrap()))).expect("Failed to send message"); //TODO JON: add a deeper log level
        }
        let num_parts = (total_size + chunk_size - 1) / chunk_size;
        //TODO JON: if we are validating integrity, then the etag suffix is the number of parts. We can fail early here if there is a mismatch between the number of parts and the expected number of parts based on the etag suffix. Not sure how we should handle that case, though

        let hash_algo = if self.multipart_integrity_verification {
            IntegrityAlgorithm::MD5
        } else {
            IntegrityAlgorithm::None
        };

        let integrity_result = self.download_files_in_part_and_check_integrity(file.clone(), chunk_size, num_parts, hash_algo.clone()).await;

        if let Err(IntegrityError(msg)) = integrity_result {
            self.runtime.cc_tx.send(CommandCenterEvent::Log(format!("Failed to verify {}: {}. Attempting again as it might have been corrupted during transfer.", self.object.key, msg))).expect("Failed to send message. This should never happen.");

            let integrity_result = self.download_files_in_part_and_check_integrity(file, chunk_size, num_parts, hash_algo).await;

            if let Err(e) = integrity_result {
                self.runtime.cc_tx.send(CommandCenterEvent::Log(format!("Failed to verify {}: {}. Its likely that there is something with the metadata, the network or the s3 storage system.", self.object.key, e))).expect("Failed to send message. This should never happen.");
                //TODO JON: exit with different exit code, when we are done. When we figure out how to cancel / exit the program. I would assume that we send this info to the command center
            }

            return;
        }
    }
}