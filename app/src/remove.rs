use crate::auto_tuning::auto_tune_delete;
use crate::command_center::{CommandCenter, CC};
use aws_layer::S3Client;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use util_layer::error_codes::UserFacingError;
use util_layer::error_codes::ErrorCode::RequestFailure;
use util_layer::snowcopy_result::SnowcopyResult;
use util_layer::RemoveConfig;

pub async fn remove(ctx: RemoveConfig) -> SnowcopyResult {
    let cancellation_token = CancellationToken::new();
    let cc = CommandCenter::new_remove(&ctx, cancellation_token.clone(), ctx.verbose);
    let (cc_tx, cc_handle) = cc.start();

    let s3client = Arc::new(S3Client::new(&ctx.endpoint, cc_tx.clone(), ctx.verbose).await);

    let mut object_channel = s3client.stream_head_objects_from_bucket_in_separate_task(&ctx.bucket, "", &PathBuf::new(), cancellation_token.clone()).await; //TODO JON: add prefix later

    let req_sem = Arc::new(auto_tune_delete(&ctx, cc_tx.clone()));
    let mut join_set = tokio::task::JoinSet::new();

    println!("Deleting bucket {}...", ctx.bucket);
    while let Some(object) = object_channel.recv().await {
        let s3client_clone = s3client.clone();
        let bucket = ctx.bucket.clone();
        let cc_tx_clone = cc_tx.clone();
        let sem_clone = req_sem.clone();

        join_set.spawn(async move {
            let _permit = sem_clone.acquire().await.expect("Semaphore closed");
            if let Err(e) = s3client_clone.delete_object(&bucket, &object.key).await {
                cc_tx_clone.send(util_layer::cc_util::CommandCenterEvent::FatalErrorEvent(UserFacingError::new(RequestFailure, format!("Failed to delete object {}: {}", object.key, e)))).expect("Failed to send fatal error event");
            }
        });
    }

    while let Some(_) = join_set.join_next().await {}

    if let Err(e) = s3client.delete_bucket(&ctx.bucket).await {
        cc_tx.send(util_layer::cc_util::CommandCenterEvent::FatalErrorEvent(UserFacingError::new(RequestFailure, format!("Failed to delete bucket {}: {}", ctx.bucket, e)))).expect("Failed to send fatal error event");
    }

    drop(s3client);
    drop(cc_tx);
    cc_handle.await.unwrap()
}