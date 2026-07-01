use aws_config::meta::region::RegionProviderChain;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::{Builder, Credentials, ProvideCredentials, RequestChecksumCalculation, ResponseChecksumValidation};
use aws_sdk_s3::operation::create_bucket::CreateBucketOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;
use util_layer::cc_util::CommandCenterEvent;
use util_layer::cc_util::Status::Transferring;
use util_layer::constants::{MULTIPART_UPLOAD_METADATA_UNIFORM_CHUNK_SIZE_KEY, PAYLOAD_SHA256_METADATA_KEY};
use util_layer::snow_errors::{S3Error, SnowCopyError};
use util_layer::time::float_string_to_time;
use util_layer::url_encoder::url_encode_fast;
use util_layer::{constants, EndpointConfig, S3Object};
use util_layer::error_codes::FatalError;
use util_layer::error_codes::FatalErrorCode::RequestFailure;

pub struct S3Client {
    aws_sdk_client: Arc<Client>,
    cc_tx: UnboundedSender<CommandCenterEvent>,
}

impl S3Client {
    pub async fn new(config: &EndpointConfig, cc_tx: UnboundedSender<CommandCenterEvent>) -> Self {
        Self {
            aws_sdk_client: Arc::new(create_client_from_config_endpoint(config).await), cc_tx
        }
    }

    fn aws_sdk_client(&self) -> &Client {
        &self.aws_sdk_client
    }

    pub async fn create_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Option<CreateBucketOutput>, SnowCopyError> {
        util_layer::retry::retry_on_failure_async(
            || async { create_bucket(self.aws_sdk_client(), bucket_name).await },
            self.cc_tx.clone(),
            format!("Creating bucket {}", bucket_name),
        )
        .await
    }

    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<(), SnowCopyError> {
        util_layer::retry::retry_on_failure_async(
            || async { complete_multipart_upload(self.aws_sdk_client(), bucket, key, upload_id, parts.clone()).await },
            self.cc_tx.clone(),
            format!("Completing multipart upload for key {}", key),
        )
        .await
    }

    pub async fn get_s3_metadata(&self, bucket: &str, key: &str) -> Result<(bool, Option<i64>, Option<SystemTime>, Option<usize>, Option<String>, Option<String>), SnowCopyError> {
        util_layer::retry::retry_on_failure_async(
            || async {
                get_s3_metadata(self.aws_sdk_client(), bucket, key).await
            },
            self.cc_tx.clone(),
            format!("Fetching metadata for key {}", key),
        ).await
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        part_number: Option<i32>,
    ) -> Result<GetObjectOutput, SnowCopyError> {
        util_layer::retry::retry_on_failure_async(
            || async { get_object(self.aws_sdk_client(), bucket, key, part_number).await },
            self.cc_tx.clone(),
            format!("Getting object {} (part {:?})", key, part_number),
        )
        .await
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), SnowCopyError> {
        util_layer::retry::retry_on_failure_async(
            || async { delete_object(self.aws_sdk_client(), bucket, key).await },
            self.cc_tx.clone(),
            format!("Deleting object {}", key),
        )
        .await
    }

    pub async fn delete_bucket(
        &self,
        bucket: &str,
    ) -> Result<(), SnowCopyError> {
        util_layer::retry::retry_on_failure_async(
            || async { delete_bucket(self.aws_sdk_client(), bucket).await },
            self.cc_tx.clone(),
            format!("Deleting bucket {}", bucket),
        )
        .await
    }

    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        mtime: Option<SystemTime>,
        uniform_chunk_size: u64,
    ) -> Result<String, SnowCopyError> {
        util_layer::retry::retry_on_failure_async(
            || async {
                let mut request = self.aws_sdk_client().create_multipart_upload().bucket(bucket).key(key);
                if let Some(mtime) = mtime {
                    request = request.metadata("mtime", util_layer::time::time_to_float_string(mtime));
                }
                request = request.metadata(MULTIPART_UPLOAD_METADATA_UNIFORM_CHUNK_SIZE_KEY, uniform_chunk_size.to_string());
                let output = request.send().await?;
                output.upload_id()
                    .map(|id| id.to_string())
                    .ok_or_else(|| SnowCopyError::RetryableError("Missing upload_id".to_string()))
            },
            self.cc_tx.clone(),
            format!("Starting multipart upload for key {}", key),
        )
        .await
    }

    pub async fn stream_head_objects_from_bucket_in_separate_task(&self, bucket: &str, prefix: &str, dest: &Path, cancellation_token: CancellationToken) ->  mpsc::Receiver<S3Object> {
        let (tx, rx) = mpsc::channel::<S3Object>(constants::DISCOVERY_LIMIT_DOWNLOAD as usize);
        let (page_tx, mut page_rx) = mpsc::channel::<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output>(3); //1 page is at most 1000 results

        let bucket_clone = bucket.to_owned();
        let prefix_clone = prefix.to_owned();
        let dest_clone = dest.to_owned();
        let cc_tx = self.cc_tx.clone();
        let client = self.aws_sdk_client.clone();

        let cancellation_token_fetch = cancellation_token.clone();
        let cc_tx_fetch = cc_tx.clone();
        let bucket_fetch = bucket_clone.clone();
        let prefix_fetch = prefix_clone.clone();

        // Fetching task
        tokio::spawn(async move {
            let mut next_page_continuation_token: Option<String> = None;

            loop {
                if cancellation_token_fetch.is_cancelled() {
                    break;
                }

                let bucket_clone_inner = bucket_fetch.clone();
                let prefix_clone_inner = prefix_fetch.clone();
                let client_inner = client.clone();
                let next_page_continuation_token_clone = next_page_continuation_token.clone();

                match util_layer::retry::retry_on_failure_async(
                    move || {
                        let bucket_clone_inner = bucket_clone_inner.clone();
                        let prefix_clone_inner = prefix_clone_inner.clone();
                        let client_inner = client_inner.clone();
                        let token_clone = next_page_continuation_token_clone.clone();
                        async move {
                            let mut request = client_inner
                                .list_objects_v2()
                                .bucket(bucket_clone_inner)
                                .prefix(prefix_clone_inner);
                            
                            if let Some(t) = token_clone {
                                request = request.continuation_token(t);
                            }

                            request.send().await.map_err(SnowCopyError::from)
                        }
                    },
                    cc_tx_fetch.clone(),
                    format!("Listing objects in bucket {} ", bucket_fetch),
                ).await {
                    Ok(output) => {
                        next_page_continuation_token = output.next_continuation_token().map(|s| s.to_string());
                        
                        if page_tx.send(output).await.is_err() {
                            break;
                        }

                        if next_page_continuation_token.is_none() {
                            break;
                        }
                    }
                    Err(e) => {
                        cc_tx_fetch.send(CommandCenterEvent::FatalErrorEvent(FatalError::new(RequestFailure, e.to_string()))).expect("Failed to send fatal error event");
                        break;
                    }
                }
            }
        });

        // Processing task
        tokio::spawn(async move {
            while let Some(output) = page_rx.recv().await {
                if cancellation_token.is_cancelled() {
                    break;
                }

                let contents = output.contents().to_vec();

                for object in contents {
                    if let (Some(key), Some(size)) = (object.key(), object.size) {
                        let local_path = util_layer::s3_key_to_path(dest_clone.as_path(), key);

                        let s3_object = S3Object {
                            key: key.to_string(),
                            aws_uri_encoded_key: url_encode_fast(key, true),
                            local_path,
                            size,
                            modified_time: None,
                            chunk_size: None,
                            etag: None,
                            sha256_checksum: None,
                        };

                        let _ = cc_tx.send(CommandCenterEvent::FileDiscovered(size as u64));
                        tx.send(s3_object).await.expect("Failed to send S3 object to channel");
                    }
                }
            }

            let _ = cc_tx.send(CommandCenterEvent::StatusChanged(Transferring));
        });

        rx
    }
}

pub type CompletedPartAws = CompletedPart;

/// Complete a multipart upload by providing all parts.
async fn complete_multipart_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<CompletedPart>,
) -> std::result::Result<(), SnowCopyError> {
    let completed_multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| SnowCopyError::from(e))
}

async fn delete_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<(), SnowCopyError> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| SnowCopyError::from(e))
}

async fn delete_bucket(
    client: &Client,
    bucket: &str,
) -> Result<(), SnowCopyError> {
    client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .map(|_| ())
        .map_err(|e| SnowCopyError::from(e))
}

async fn create_client_from_config_endpoint(config: &EndpointConfig) -> Client {
    create_aws_sdk_client(&config.s3_access_key, &config.s3_secret_key, &config.s3_url, &config.s3_region).await
}

async fn create_aws_sdk_client(access_key: &str, secret_key: &str, url: &str, region: &str) -> Client {
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


    let client = Client::from_conf(s3_conf);
    client
}

async fn create_bucket(
    client: &Client,
    bucket_name: &str,
) -> Result<Option<CreateBucketOutput>, SnowCopyError> {
    match client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => {
            Ok(None)
        }
        Err(err) => {
            if let Some(service_err) = err.as_service_error() {
                if service_err.is_not_found() {
                    let create = client
                        .create_bucket()
                        .bucket(bucket_name)
                        .send()
                        .await?;

                    return Ok(Some(create));
                }
            }
            Err(err.into())
        }
    }
}



async fn get_s3_metadata(client: &Client, bucket: &str, key: &str) -> Result<(bool, Option<i64>, Option<SystemTime>, Option<usize>, Option<String>, Option<String>), SnowCopyError> {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(resp) => {
            let size = resp.content_length();
            let option = resp.metadata();
            let etag = resp.e_tag().map(|e| e.to_string());

            let chunk_size = option
                .and_then(|m| m.get(MULTIPART_UPLOAD_METADATA_UNIFORM_CHUNK_SIZE_KEY))
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
            Ok((true, size, mtime, chunk_size, etag, sha256_checksum))
        }
        Err(err) => {
            let err = SnowCopyError::from(err);

            if matches!(err, SnowCopyError::Http(404) | SnowCopyError::S3(S3Error::NoSuchKey)) { //Not found/404 is okay, just return
                Ok((false, None, None, None, None, None))
            } else { //General error
                Err(err)
            }
        },
    }
}

/*
    TODO Jon: For endpoints that support get /key/partNumber=X, then we do not need to rely on our own metadata to reconstruct the etag.
    The thing is that we do not know if all s3 clones support it, so it should be part of autodetection. Ceph does, so there is that. Minio, too.
    It would be way more elegant since no extra metadata is needed. I'll leave this in for when we need it for autodetection.

    let resp = get_object(&client.clone(), bucket.clone().as_str(), "BigFile-1.bin", Some(1)).await;
    println!("{:?}", resp);

*/
async fn get_object(
    client: &Client,
    bucket: &str,
    key: &str,
    part_number: Option<i32>,
) -> std::result::Result<GetObjectOutput, SnowCopyError> {
    let mut request = client.get_object().bucket(bucket).key(key);
    if let Some(part) = part_number {
        request = request.part_number(part);
    }
    Ok(request.send().await?)
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