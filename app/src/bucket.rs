use aws_layer::S3Client;
use std::sync::Arc;
use tokio::task::JoinHandle;
use util_layer::UploadConfig;
use util_layer::snow_errors::SnowCopyError;

pub fn create_bucket_if_not_exist(ctx: &Arc<UploadConfig>, client: &Arc<S3Client>) -> JoinHandle<Result<bool, SnowCopyError>> {
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