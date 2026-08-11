use crate::traits::{UploadRuntime, UploadTrait};
use http_layer::create_in_memory_body_put_with_hashes;
use http_layer::upload_trailing::create_streaming_req_trailing_checksum;
use std::pin::Pin;
use std::sync::Arc;
use util_layer::cc_util::CommandCenterEvent;
use util_layer::retry::retry_on_failure_async;
use util_layer::traits::{FileHandler, FileObject, HttpExecutor};
use util_layer::{FileInfo, UploadConfig};
use util_layer::error_codes::ErrorCode::DiskIoError;
use util_layer::error_codes::UserFacingError;

pub struct SingleUpload {
    ctx: Arc<UploadConfig>,
    runtime: Arc<UploadRuntime>,
    file_info: Arc<FileInfo>,
    http_executor: Arc<dyn HttpExecutor>,
    file_handler: Arc<dyn FileHandler>,
}

impl SingleUpload {
    pub(crate) fn new(ctx: Arc<UploadConfig>, file_info: Arc<FileInfo>, runtime: Arc<UploadRuntime>, http_executor: Arc<dyn HttpExecutor>, file_handler: Arc<dyn FileHandler>) -> Self {
        Self {
            ctx,
            runtime,
            file_info,
            http_executor,
            file_handler,
        }
    }
}

impl UploadTrait for SingleUpload {
    fn upload_file(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let file = self.file_handler.open_file(&*self.file_info.file_path);

            if let Err(e) = file { //Hopefully, we should be on top of ulimit through auto-tuning
                self.runtime.cc_tx.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(DiskIoError,format!("Error opening file {}: {}", self.file_info.file_path.to_str().unwrap(), e)))).expect("Failed to send message to command center. Should never happen.");
                return;
            }
            let file_ref = file.unwrap();
            small_file_upload(self, file_ref).await;
        })
    }
}

pub(crate) async fn small_file_upload(upload: &SingleUpload, file: Arc<dyn FileObject>) {
    let canonical_uri = Arc::new(format!("/{}/{}", upload.ctx.bucket, upload.file_info.aws_uri_encoded_key));
    let query_uri = "".to_string();

    let result = tokio::select! {
        res = retry_on_failure_async(|| {
            let ctx = upload.ctx.clone();
            let amount = upload.file_info.file_size;
            let file_info = upload.file_info.clone();
            let runtime = upload.runtime.clone();
            let canonical_uri = canonical_uri.clone();
            let query_uri = query_uri.clone();
            let file = file.clone();

            async move {
                if ctx.trailing_small {
                    let trailing_chunk_size = ctx.trailing_chunk_size;
                    let req_result = create_streaming_req_trailing_checksum(ctx, &canonical_uri, &query_uri, file, 0, amount, file_info.modified_time, runtime.file_sem.clone(), trailing_chunk_size);
                    http_layer::upload_trailing::put_file_streaming(req_result.unwrap(), amount, upload.http_executor.clone(), &runtime.req_sem).await
                } else {
                    let file_amount = file.length()?;
                    let req = create_in_memory_body_put_with_hashes(file, 0, file_amount, ctx, file_info.clone(), &runtime.file_sem, runtime.buffer_pool.clone(), &*canonical_uri, &query_uri).await?;
                    http_layer::put_file(req, file_info, upload.http_executor.clone(), &runtime.req_sem).await
                }
            }
        }, upload.runtime.cc_tx.clone(),
            format!("Uploading {}", upload.file_info.aws_uri_encoded_key)) => res,

        _ = upload.runtime.cancellation_token.cancelled() => {
            return;
        }
    };

    match result {
        Ok(metrics) => {
            let file_size = upload.file_info.file_size;
            upload.runtime.cc_tx.send(CommandCenterEvent::PartTransferred(metrics.1)).expect("Failed to send message to command center. Should never happen.");
            upload.runtime.cc_tx.send(CommandCenterEvent::FileTransferred(file_size)).expect("Failed to send message to command center. Should never happen.");
        },
        Err(e) => panic!("Error uploading file {}: {}", upload.file_info.file_path.to_str().unwrap(), e),
    }
}