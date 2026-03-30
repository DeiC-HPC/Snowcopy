mod s3example_error;

use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::{Builder, Credentials, RequestChecksumCalculation, ResponseChecksumValidation};
use aws_sdk_s3::operation::create_bucket::CreateBucketOutput;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use s3example_error::S3ExampleError;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use util_layer::cc::Status::Transferring;
use util_layer::cc::CommandCenterEvent;
use util_layer::time::float_string_to_time;

pub type AWSClient = Client;
pub type CompletedPartAws = CompletedPart;

/// Complete a multipart upload by providing all parts.
pub async fn complete_multipart_upload(
    client: &AWSClient,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<CompletedPart>,
) -> std::result::Result<(), S3ExampleError> {
    let completed_multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();

    match client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await {
        Ok(_) => {
            Ok(())
        },
        Err(e) => {
            if let Some(status) = e.raw_response().map(|r| r.status()) {
                eprintln!("HTTP status code: {}", status);
            }
            eprintln!("Error completing multipart upload for key {}: {:?}", key, e);
            Err(S3ExampleError::from(e))
        }
    }
}


pub async fn create_client(access_key: &str, secret_key: &str, url: &str) -> AWSClient {
    let region = Region::new("us-east-1"); //TODO should constants be in some lov level crate everyone can reference?

    let creds = Credentials::new(
        access_key,
        secret_key,
        None,               // optional session token
        None,               // optional expiration
        "manual-credentials", // provider name (for debugging)
    );

    let shared_config = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .credentials_provider(creds)
        .region(region.clone())
        .endpoint_url(url)
        .load()
        .await;


    let timeout_config = TimeoutConfig::builder()
        .operation_timeout(Duration::from_secs(120))
        .build();


    let s3_conf = Builder::from(&shared_config)
        .force_path_style(true)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .timeout_config(timeout_config)
        .build();


    let client = AWSClient::from_conf(s3_conf);
    client
}

pub async fn create_bucket(
    client: &AWSClient,
    bucket_name: Arc<String>,
) -> Result<Option<CreateBucketOutput>, S3ExampleError> {
    let create = client
        .create_bucket()
        .bucket(bucket_name.as_str())
        .send()
        .await;

    // BucketAlreadyExists and BucketAlreadyOwnedByYou are not problems for this task.
    create.map(Some).or_else(|err| {
        if err
            .as_service_error()
            .map(|se| se.is_bucket_already_exists() || se.is_bucket_already_owned_by_you())
            == Some(true)
        {
            Ok(None)
        } else {
            Err(S3ExampleError::from(err))
        }
    })
}



/// S3 Hello World Example using the AWS SDK for Rust.
///
/// This example lists the objects in a bucket, uploads an object to that bucket,
/// and then retrieves the object and prints some S3 information about the object.
/// This shows a number of S3 features, including how to use built-in paginators
/// for large data sets.
///
/// # Arguments
///
/// * `client` - an S3 client configured appropriately for the environment.
/// * `bucket` - the bucket name that the object will be uploaded to. Must be present in the region the `client` is configured to use.
/// * `filename` - a reference to a path that will be read and uploaded to S3.
/// * `key` - the string key that the object will be uploaded as inside the bucket.
#[allow(dead_code)]
async fn list_bucket(
    client: &AWSClient,
    bucket: &str,
) -> std::result::Result<(), S3ExampleError> {
    // List the buckets in this account
    let mut objects = client
        .list_objects_v2()
        .bucket(bucket)
        .into_paginator()
        .send();

    println!("key\tetag\tlast_modified\tstorage_class");
    while let Some(Ok(object)) = objects.next().await {
        for item in object.contents() {
            println!(
                "{}\t{}\t{}\t{}",
                item.key().unwrap_or_default(),
                item.e_tag().unwrap_or_default(),
                item.last_modified()
                    .map(|lm| format!("{lm}"))
                    .unwrap_or_default(),
                item.storage_class()
                    .map(|sc| format!("{sc}"))
                    .unwrap_or_default()
            );
        }
    }

    Ok(())
}

pub async fn async_head_objects_from_bucket(bucket: &str, prefix: &str, dest: &Path, client: Arc<AWSClient>, sender: UnboundedSender<CommandCenterEvent>) ->  mpsc::Receiver<S3Object> {
    // Create channel with buffer size of 5000
    let (tx, rx) = mpsc::channel::<S3Object>(5000);

    let bucket_clone = bucket.to_owned();
    let prefix_clone = prefix.to_owned();
    let dest_clone = dest.to_owned();

    tokio::spawn(async move {
        // List objects with pagination
        let mut paginator = client
            .list_objects_v2()
            .bucket(bucket_clone)
            .prefix(prefix_clone.clone())
            .into_paginator()
            .send();

        while let Some(result) = paginator.next().await {
            match result {
                Ok(output) => {
                    for object in output.contents() {
                        if let (Some(key), Some(size)) = (object.key(), object.size) {
                            // Map S3 key to local file path
                            let local_path = util_layer::s3_key_to_path(dest_clone.as_path(), key);

                            let s3_object = S3Object {
                                key: key.to_string(),
                                local_path,
                                size,
                                modified_time : None
                            };

                            // Send object through channel
                            if tx.send(s3_object).await.is_err() {
                                eprintln!("Receiver dropped, stopping scan");
                                return;
                            }
                            let _ = sender.send(CommandCenterEvent::FileDiscovered(size as u64));

                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error listing objects: {:?}", e);
                    break;
                }
            }
        }

        let _ = sender.send(CommandCenterEvent::StatusChanged(Transferring));
    });

    rx
}

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub local_path: PathBuf,
    pub size: i64,
    pub modified_time: Option<SystemTime>,
}

pub async fn get_s3_mtime(client: &AWSClient, bucket: &str, key: &str) -> Option<SystemTime> {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(resp) => {
            if let Some(mtime_str) = resp.metadata().and_then(|m| m.get("mtime")) {
                if let Ok(st) = float_string_to_time(mtime_str) {
                    return Some(st);
                }
            }
            resp.last_modified().and_then(|dt| {
                let seconds = dt.secs();
                let nanoseconds = dt.subsec_nanos();
                SystemTime::UNIX_EPOCH.checked_add(Duration::new(seconds as u64, nanoseconds))
            })
        }
        Err(_) => None,
    }
}