use crate::command_center::{CommandCenter, CC};
use aws_layer::S3Client;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use util_layer::error_codes::UserFacingError;
use util_layer::snowcopy_result::SnowcopyResult;
use util_layer::ListConfig;

pub(crate) async fn list(ctx: ListConfig) -> SnowcopyResult {
    let cancellation_token = CancellationToken::new();
    let cc = CommandCenter::new_list(&ctx, cancellation_token.clone());
    let (cc_tx, cc_handle) = cc.start();

    let s3client = Arc::new(S3Client::new(&ctx.endpoint, cc_tx.clone(), ctx.verbose).await);

    if let Some(bucket) = &ctx.bucket { //list bucket contents
        let mut object_channel = s3client.stream_head_objects_from_bucket_in_separate_task(bucket, "", &PathBuf::new(), cancellation_token.clone()).await; //TODO JON: add prefix later

        while let Some(object) = object_channel.recv().await {
            println!("{}", object.key);
        }
    } else { //list buckets
        match s3client.list_buckets().await {
            Ok(buckets) => {
                for bucket in buckets {
                    println!("{}", bucket);
                }
            }
            Err(e) => {
                let _ = cc_tx.send(util_layer::cc_util::CommandCenterEvent::FatalErrorEvent(UserFacingError::new(util_layer::error_codes::ErrorCode::RequestFailure, format!("Failed to list buckets: {}", e))));
            }
        }
    }

    drop(s3client);
    drop(cc_tx);
    cc_handle.await.unwrap()
}