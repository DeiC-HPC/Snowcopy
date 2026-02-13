mod s3example_error;

use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::{Builder, Credentials, RequestChecksumCalculation, ResponseChecksumValidation};
use aws_sdk_s3::operation::create_bucket::CreateBucketOutput;
use aws_sdk_s3::presigning::{PresignedRequest, PresigningConfig};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use s3example_error::S3ExampleError;
use std::time::Duration;

pub type AWSClient = Client;
pub type CompletedPartAws = CompletedPart;

pub async fn presign_upload_part(
    client: &AWSClient,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    expires_in: u64,
) -> PresignedRequest {
    let expires_in = Duration::from_secs(expires_in);
    let result = PresigningConfig::expires_in(expires_in).expect("Failed to create presigning config");
    let presigned_request = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .presigned(result)
        .await.unwrap();

    presigned_request
}

/// Generate a URL for a presigned GET request.
pub async fn presign_put(
    client: &AWSClient,
    bucket: &str,
    key: &str,
    expires_in: u64,
) -> PresignedRequest {
    let expires_in = Duration::from_secs(expires_in);
    let result = PresigningConfig::expires_in(expires_in).expect("Failed to create presigning config");
    let presigned_request = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .presigned(result)
        .await.unwrap();

    presigned_request
}

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
    bucket_name: &str,
) -> std::result::Result<Option<CreateBucketOutput>, S3ExampleError> {
    let create = client
        .create_bucket()
        .bucket(bucket_name)
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