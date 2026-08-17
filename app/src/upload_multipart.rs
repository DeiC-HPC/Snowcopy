use crate::traits::{UploadRuntime, UploadTrait};
use crate::upload;
use aws_layer::{CompletedPartAws, S3Client};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use util_layer::cc_util::CommandCenterEvent;
use util_layer::constants::CHUNK_SIZE;
use util_layer::error_codes::ErrorCode::DiskIoError;
use util_layer::error_codes::UserFacingError;
use util_layer::traits::{FileHandler, FileObject, HttpExecutor};
use util_layer::{FileInfo, UploadConfig};

pub struct MultipartUpload {
    ctx: Arc<UploadConfig>,
    runtime: Arc<UploadRuntime>,
    file_info: Arc<FileInfo>,
    aws_client: Arc<S3Client>,
    http_executor: Arc<dyn HttpExecutor>,
    file_handler: Arc<dyn FileHandler>,
}

impl MultipartUpload {
    pub(crate) fn new(ctx: Arc<UploadConfig>, file_info: Arc<FileInfo>, runtime: Arc<UploadRuntime>, aws_client: Arc<S3Client>, http_executor: Arc<dyn HttpExecutor>, file_handler: Arc<dyn FileHandler>) -> Self {
        Self {
            ctx,
            runtime,
            file_info,
            aws_client,
            http_executor,
            file_handler,
        }
    }
}

impl UploadTrait for MultipartUpload {
    fn upload_file(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let file_result = self.file_handler.open_file(&*self.file_info.file_path);

            if let Err(e) = file_result { //Hopefully, we should be on top of ulimit through auto-tuning
                self.runtime.cc_tx.send(CommandCenterEvent::FatalErrorEvent(UserFacingError::new(DiskIoError, format!("Error opening file {}: {}", self.file_info.file_path.to_str().unwrap(), e)))).expect("Failed to send message to command center. Should never happen.");
                return;
            }
            let file_object = file_result.unwrap();
            upload_with_multipart(self, file_object).await;
        })
    }
}

async fn upload_with_multipart(multipart: &MultipartUpload, file: Arc<dyn FileObject>) {
    let chunk_amount = (multipart.file_info.file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;

    let upload_id = multipart.aws_client.create_multipart_upload(&multipart.ctx.bucket, &*multipart.file_info.key, multipart.file_info.modified_time, CHUNK_SIZE).await.unwrap();

    let completed_parts: Arc<Vec<OnceLock<CompletedPartAws>>> = Arc::new(
        (0..chunk_amount)
            .map(|_| OnceLock::new())
            .collect()
    );

    upload::upload_each_chunk_with_retry(&multipart.ctx, &multipart.runtime, &multipart.file_info, multipart.http_executor.clone(), file, &upload_id, chunk_amount, &completed_parts).await;

    let completed_parts: Vec<CompletedPartAws> = Arc::try_unwrap(completed_parts).expect("Failed to unwrap Arc, there is a bug in our flow")
        .into_iter()
        .map(|slot| slot.into_inner().unwrap())
        .collect();

    multipart.aws_client.complete_multipart_upload(&multipart.ctx.bucket, &*multipart.file_info.key, &upload_id, completed_parts).await.unwrap();
    let _ = multipart.runtime.cc_tx.send(CommandCenterEvent::FileTransferred(multipart.file_info.file_size));
}