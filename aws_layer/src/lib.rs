mod s3example_error;

use aws_config::meta::region::RegionProviderChain;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::{Builder, Credentials, ProvideCredentials, RequestChecksumCalculation, ResponseChecksumValidation};
use aws_sdk_s3::operation::create_bucket::CreateBucketOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use s3example_error::S3Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use util_layer::cc_util::CommandCenterEvent;
use util_layer::cc_util::Status::Transferring;
use util_layer::constants::{MULTIPART_UPLOAD_METADATA_CHUNK_AMOUNT_KEY, PAYLOAD_SHA256_METADATA_KEY};
use util_layer::time::float_string_to_time;
use util_layer::{constants, EndpointConfig, S3Object};
use util_layer::url_encoder::url_encode_fast;

pub type AWSClient = Client;
pub type CompletedPartAws = CompletedPart;

/// Complete a multipart upload by providing all parts.
pub async fn complete_multipart_upload(
    client: &AWSClient,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<CompletedPart>,
) -> std::result::Result<(), S3Error> {
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
            Err(S3Error::from(e))
        }
    }
}

pub async fn create_client_from_config_endpoint(config: &EndpointConfig) -> AWSClient {
    create_client(&config.s3_access_key, &config.s3_secret_key, &config.s3_url, &config.s3_region).await
}

pub async fn create_client(access_key: &str, secret_key: &str, url: &str, region: &str) -> AWSClient {
    let region = Region::new(region.to_string());

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

    let retry_config = RetryConfig::standard()
        .with_max_attempts(1);

    let s3_conf = Builder::from(&shared_config)
        .force_path_style(true)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .timeout_config(timeout_config)
        .retry_config(retry_config)
        .build();


    let client = AWSClient::from_conf(s3_conf);
    client
}

pub async fn create_bucket(
    client: &AWSClient,
    bucket_name: &str,
) -> Result<Option<CreateBucketOutput>, S3Error> {
    match client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => {
            Ok(None)
        }
        Err(err) => {
            let service_err = err.into_service_error();
            if service_err.is_not_found() {
                let create = client
                    .create_bucket()
                    .bucket(bucket_name)
                    .send()
                    .await;

                create.map(Some).map_err(S3Error::from)
            } else {
                Err(S3Error::from(service_err))
            }
        }
    }
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
async fn list_bucket( //TODO JON: use for ls?
    client: &AWSClient,
    bucket: &str,
) -> std::result::Result<(), S3Error> {
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


pub async fn async_head_objects_from_bucket(bucket: &str, prefix: &str, dest: &Path, client: Arc<AWSClient>, sender: UnboundedSender<CommandCenterEvent>, token: CancellationToken) ->  mpsc::Receiver<S3Object> {
    let (tx, rx) = mpsc::channel::<S3Object>(constants::DISCOVERY_LIMIT_DOWNLOAD as usize);

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

        let mut join_set = JoinSet::new();

        while let Some(result) = paginator.next().await {
            if token.is_cancelled() {
                break;
            }
            match result {
                Ok(output) => {
                    let tx_clone = tx.clone();
                    let sender_clone = sender.clone();
                    let dest_clone_inner = dest_clone.clone();

                    join_set.spawn(async move {
                        for object in output.contents() {
                            if let (Some(key), Some(size)) = (object.key(), object.size) {
                                // Map S3 key to local file path
                                let local_path = util_layer::s3_key_to_path(dest_clone_inner.as_path(), key);

                                let s3_object = S3Object {
                                    key: key.to_string(),
                                    aws_uri_encoded_key: url_encode_fast(key, true),
                                    local_path,
                                    size,
                                    modified_time : None, //No fallback value. Either we have the one from s3 metadata or we dont care
                                    chunk_size: None,
                                    etag: None,
                                    sha256_checksum: None,
                                };

                                let _ = sender_clone.send(CommandCenterEvent::FileDiscovered(size as u64));
                                // Send object through channel
                                tx_clone.send(s3_object).await.expect("Failed to send S3 object to channel");
                            }
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Error listing objects: {:?}", e); //TODO JON: we should stop the transfer here
                    break;
                }
            }
        }

        // Wait for all processing tasks to complete
        while let Some(_) = join_set.join_next().await {}

        let _ = sender.send(CommandCenterEvent::StatusChanged(Transferring));
    });

    rx
}

pub async fn get_s3_metadata(client: &AWSClient, bucket: &str, key: &str) -> (bool, Option<i64>, Option<SystemTime>, Option<usize>, Option<String>, Option<String>) {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(resp) => {
            let size = resp.content_length();
            let option = resp.metadata();
            let etag = resp.e_tag().map(|e| e.to_string());

            let chunk_size = option
                .and_then(|m| m.get(MULTIPART_UPLOAD_METADATA_CHUNK_AMOUNT_KEY))
                .and_then(|chunk_size| chunk_size.parse::<usize>().ok());

            //read sha256 checksum from response first and fall back on metadata
            let mut sha256_checksum = resp.checksum_sha256().map(|c| c.to_string());
            if sha256_checksum.is_none() {
                sha256_checksum = option
                    .and_then(|m| m.get(PAYLOAD_SHA256_METADATA_KEY))
                    .map(|s| s.to_string());
            }

            let mtime = if let Some(mtime_str) = option.and_then(|m| m.get("mtime")) {
                if let Ok(st) = float_string_to_time(mtime_str) {
                    Some(st)
                } else {
                    None
                }
            } else {
                None
            };
            (true, size, mtime, chunk_size, etag, sha256_checksum)
        }
        Err(_) => (false, None, None, None, None, None),
    }
}

/*
    TODO Jon: For endpoints that support get /key/partNumber=X, then we do not need to rely on our own metadata to reconstruct the etag.
    The thing is that we do not know if all s3 clones support it, so it should be part of autodetection. Ceph does, so there is that. Minio, too.
    It would be way more elegant since no extra metadata is needed. I'll leave this in for when we need it for autodetection.

    let resp = get_object(&client.clone(), bucket.clone().as_str(), "BigFile-1.bin", Some(1)).await;
    println!("{:?}", resp);

*/
pub async fn get_object(
    client: &AWSClient,
    bucket: &str,
    key: &str,
    part_number: Option<i32>,
) -> std::result::Result<GetObjectOutput, S3Error> {
    let mut request = client.get_object().bucket(bucket).key(key);
    if let Some(part) = part_number {
        request = request.part_number(part);
    }
    request.send().await.map_err(S3Error::from)
}

pub async fn head_object(
    client: &AWSClient,
    bucket: &str,
    key: &str,
    part_number: Option<i32>,
) -> std::result::Result<HeadObjectOutput, S3Error> {
    let mut request = client.head_object().bucket(bucket).key(key);
    if let Some(part) = part_number {
        request = request.part_number(part);
    }
    request.send().await.map_err(S3Error::from)
}

pub async fn load_aws_config_parts() -> (Credentials, Region, Option<String>) {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(RegionProviderChain::default_provider().or_else("us-east-1"))
        .load()
        .await;

    let credentials = sdk_config
        .credentials_provider()
        .expect("No credentials provider found")
        .provide_credentials()
        .await
        .expect("Failed to load AWS credentials");

    let region = sdk_config.region().expect("Never fails.").clone(); //This has a fallback to some metadata server called IMDS, which wastes time but cannot easily be disabled. We should consider how to handle it when we do the real config. Perhaps just reading the environment ourselves.
    let endpoint = sdk_config.endpoint_url().map(|s| s.to_string());

    (credentials, region, endpoint)
}