use crate::traits::{DownloadRuntime, DownloadTrait};
use std::sync::Arc;
use tokio::select;
use util_layer::cc_util::CommandCenterEvent;
use util_layer::retry::retry_on_failure_async;
use util_layer::snow_errors::SnowCopyError;
use util_layer::snow_errors::SnowCopyError::IntegrityError;
use util_layer::traits::HttpExecutor;
use util_layer::{DownloadConfig, FileInfo, S3Object};

pub struct SinglepartDownload {
    ctx: Arc<DownloadConfig>,
    runtime: Arc<DownloadRuntime>,
    http_executor: Arc<dyn HttpExecutor>,
    object: Arc<S3Object>,
}

impl SinglepartDownload {
    pub(crate) fn new(
        ctx: Arc<DownloadConfig>,
        runtime: Arc<DownloadRuntime>,
        http_executor: Arc<dyn HttpExecutor>,
        object: Arc<S3Object>,
    ) -> Self {
        Self {
            ctx,
            runtime,
            http_executor,
            object,
        }
    }
}

impl DownloadTrait for SinglepartDownload {
    async fn download_file(&self) {
        let integrity_result = download_full_file_and_check_integrity(self).await;
        if let Err(IntegrityError(msg)) = integrity_result {
            self.runtime.cc_tx.send(CommandCenterEvent::Log(format!("Failed to verify {}: {}. Attempting again as it might have been corrupted during transfer.", self.object.key, msg))).expect("Failed to send message. This should never happen.");

            let integrity_result = download_full_file_and_check_integrity(self).await;

            if let Err(e) = integrity_result {
                self.runtime.cc_tx.send(CommandCenterEvent::Log(format!("Failed to verify {}: {}. Its likely that there is something with the metadata, the network or the s3 storage system.", self.object.key, e))).expect("Failed to send message. This should never happen.");
                //TODO JON: exit with different exit code, when we are done. When we figure out how to cancel / exit the program. I would assume that we send this info to the command center
            }
        }
    }
}

async fn download_full_file_and_check_integrity(download: &SinglepartDownload) -> Result<(), SnowCopyError> {
    let cc_sender_clone = download.runtime.cc_tx.clone();
    let retry_cc_sender = cc_sender_clone.clone();
    let canonical_uri = format!("/{}/{}", download.ctx.bucket, download.object.aws_uri_encoded_key);
    let query_uri = String::new();
    let file_info = Arc::new(FileInfo {
        file_path: download.object.local_path.clone(),
        aws_uri_encoded_key: download.object.aws_uri_encoded_key.clone(),
        key: download.object.key.clone(),
        file_size: download.object.size as u64,
        modified_time: download.object.modified_time,
    });

    let result = select! {
        _ = download.runtime.cancellation_token.cancelled() => {
            return Err(SnowCopyError::Canceled);
        },

        result = retry_on_failure_async(move || {
            let config = download.ctx.clone();
            let file_info = file_info.clone();
            let pool_client = download.http_executor.clone();
            let canonical_uri = canonical_uri.clone();
            let query_uri = query_uri.clone();
            let req_sem = download.runtime.req_sem.clone();
            let disk_write_tx = download.runtime.disk_write_tx.clone();
            let cc_sender_clone = cc_sender_clone.clone();

            async move {
                http_layer::download::get_file(config, file_info, &pool_client, &canonical_uri, &query_uri, req_sem, disk_write_tx, cc_sender_clone).await
            }
        }, retry_cc_sender, format!("Downloading {}", download.object.key)) => result
    };

    match result {
        Ok(hash) => {
            if let Some(sha256) = hash {
                if let Some(ref sha256_expected) = download.object.sha256_checksum {
                    if *sha256_expected != sha256 {
                        let error_msg = format!(
                            "Integrity check failed for {}. Expected {}, got {}",
                            download.object.key, sha256_expected, sha256
                        );
                        download.runtime.cc_tx.send(CommandCenterEvent::Log(error_msg.clone())).expect("Failed to send message");
                        return Err(IntegrityError(error_msg));
                    } else {
                        download.runtime.cc_tx.send(CommandCenterEvent::FileIntegrityChecked(true)).expect("Failed to send message");
                    }
                } else {
                    download.runtime.cc_tx.send(CommandCenterEvent::Log("Skipping integrity check for file with no SHA256 checksum".to_string())).expect("Failed to send message"); //TODO JON: add a deeper log level
                    //silence, as we don't have info to do the check
                }
            }
        }
        Err(e) => {
            return Err(e);
        }
    }
    Ok(())
}