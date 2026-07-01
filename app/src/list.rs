use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use aws_layer::S3Client;
use util_layer::error_codes::FatalError;
use util_layer::ListConfig;
use crate::command_center::{CommandCenter, CC};

pub(crate) async fn list_bucket(ctx: ListConfig) -> Option<FatalError> {
    let cancellation_token = CancellationToken::new();
    let cc = CommandCenter::new_list(&ctx, cancellation_token.clone());
    let (cc_tx, cc_handle) = cc.start();

    let s3client = Arc::new(S3Client::new(&ctx.endpoint, cc_tx.clone()).await);

    let mut object_channel = s3client.stream_head_objects_from_bucket_in_separate_task(&ctx.bucket, "", &PathBuf::new(), cancellation_token.clone()).await; //TODO JON: add prefix later

    while let Some(object) = object_channel.recv().await {
        println!("{}", object.key);
    }

    drop(s3client);
    drop(cc_tx);
    cc_handle.await.unwrap()
}