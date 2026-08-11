use aws_layer::S3Client;
use std::sync::Arc;
use tokio::task::JoinHandle;
use util_layer::UploadConfig;
use util_layer::error_codes::FatalError;
use util_layer::error_codes::FatalErrorCode::{RequestFailure, Unknown};
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

pub async fn check_for_bucket(ctx: &Arc<UploadConfig>, s3client: &Arc<S3Client>) -> Result<bool, Option<FatalError>> {
    let bucket_creation_handle = create_bucket_if_not_exist(&ctx, &s3client);
    let bucket_created = match bucket_creation_handle.await {
        Ok(Ok(created)) => created,
        Ok(Err(e)) => {
            return Err(Some(FatalError::new(RequestFailure, format!("Bucket creation failed: {}", e))));
        }
        Err(join_error) => {
            return Err(Some(FatalError::new(Unknown, format!("Task join error during bucket creation: {:?}", join_error))));
        }
    };
    Ok(bucket_created)
}