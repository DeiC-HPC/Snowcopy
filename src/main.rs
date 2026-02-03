mod s3example_error;
mod constants;
mod signer;

use crate::TransferMethod::CephS3;
use serde;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::{fs, io};

use aws_sdk_s3::config::{Credentials, Region};
use std::time::Instant;

use crate::s3example_error::S3ExampleError;
use crate::CompioReply::{PutFile};
use crate::CompioReply::PutMultipart;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::operation::create_bucket::CreateBucketOutput;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::presigning::{PresignedRequest, PresigningConfig};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::{config::{
    Builder as S3ConfigBuilder,
    RequestChecksumCalculation,
    ResponseChecksumValidation,
}, Client as AWSClient};
use clap::{Arg, ArgMatches, Command};
use compio::fs::{File, OpenOptions};
use compio::io::AsyncReadAt;
use cyper::{Body, Client, Request, RequestBuilder};
use futures::future::join_all;
use http::{Method, StatusCode};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;
use tracing_subscriber::{fmt, EnvFilter};
use url::Url;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use compio::BufResult;
use futures_util::stream;

use hmac::{Hmac, Mac};
use hex;
//TODO jon should this be something from the compio crate?
use sha2::{Digest, Sha256};

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = Instant::now();
    init_logging();

    // Read Commandline Arguments
    let matches = parse_arguments();
    let bucket = matches.get_one::<String>("bucket").expect("required");
    let path_as_string = matches.get_one::<String>("path").expect("required");
    let transfer_method = matches.get_one::<String>("o").expect("required");

    println!("Bucket (-b): {}", bucket);
    println!("Path: {}", path_as_string);
    let path = Path::new(path_as_string);

    let method = match_transfer_method(Some(transfer_method)).expect(format!("Transfer method {} unknown", transfer_method).as_str());

    match method {
        CephS3 => run_s3_ceph(bucket, path),
    }.await;

    let elapsed = start_time.elapsed();
    println!("Program execution time: {:.2?}", elapsed);

    Ok(())
}

enum TransferMethod {
    CephS3,
}

fn match_transfer_method(method: Option<&str>) -> Option<TransferMethod> {
    match method {
        Some("o1") => Some(TransferMethod::CephS3),
        _other => None,
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Config {
    s3_access_key: String,
    s3_secret_key: String,
    s3_url: String
}


fn walk_dir_files(dir: impl AsRef<Path>, mut on_file: impl FnMut(&Path) -> io::Result<()>) -> io::Result<()> {
    fn recurse(dir: &Path, on_file: &mut dyn FnMut(&Path) -> io::Result<()>) -> io::Result<()> {
        for entry_res in fs::read_dir(dir)? {
            let entry = entry_res?;
            let path = entry.path();

            // Follow directories; process files
            if path.is_dir() {
                recurse(&path, on_file)?;
            } else if path.is_file() {
                on_file(&path)?;
            }
        }
        Ok(())
    }

    if dir.as_ref().is_file() {
        on_file(&dir.as_ref().to_path_buf())
    } else {
        recurse(dir.as_ref(), &mut on_file)
    }
}

async fn run_s3_ceph(bucket: &str, path: &Path) {
    let (compio_tx, mut reply_rx) = start_compio_worker();

    // Read config file
    let config = create_config();


    let client = create_client(Region::new(constants::DEFAULT_REGION), config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str()).await;
    create_bucket(&client, bucket).await.unwrap();

    let root = PathBuf::from(path);
    let mut upload_object_future_group = Vec::new();
    let mut completed_file_count = 0;

    // Track multipart uploads: path -> (upload_id, key, total_parts, collected_parts)
    let mut multipart_state: HashMap<String, (String, String, i32, Vec<CompletedPart>)> = HashMap::new();

    walk_dir_files(&root, |file_path| {
        let metadata = fs::metadata(file_path)?;
        let size = metadata.len();

        let key_clone = normalize_path(file_path.to_str().unwrap()).to_string();
        let bucket_clone = bucket.to_string();
        let client_clone = client.clone();
        let compio_tx_clone = compio_tx.clone();
        let file_path_str = file_path.to_str().unwrap().to_string();

        let future = async move {
            if size > constants::CHUNK_SIZE {
                async move {
                    //Start
                    let output: CreateMultipartUploadOutput = client_clone.create_multipart_upload().bucket(bucket_clone.clone()).key(key_clone.clone()).send().await.unwrap();

                    let upload_id = output.upload_id().unwrap();

                    let chunk_amount_u64 = (size + constants::CHUNK_SIZE - 1) / constants::CHUNK_SIZE;
                    let total_parts = chunk_amount_u64 as i32;

                    for i in 0..chunk_amount_u64 {
                        let start_pos = i * constants::CHUNK_SIZE;
                        let end_pos = std::cmp::min((i + 1) * constants::CHUNK_SIZE, size);

                        let part_number = (i + 1) as i32;
                        let part_presigned = presign_upload_part(&client_clone, &bucket_clone, &key_clone, upload_id, part_number, constants::PRESIGN_LIFETIME).await;

                        compio_tx_clone.send(CompioCmd::PutMultipart { url: part_presigned.uri().into(), path: file_path_str.clone(), start_pos, end_pos, part_number, upload_id: upload_id.to_string(), total_parts }).await.expect("Failed to send command");
                    }
                }.await
            } else {
                let presigned = presign_put_object(&client_clone, &bucket_clone, &key_clone, constants::PRESIGN_LIFETIME).await;
                compio_tx_clone.send(CompioCmd::PutFile {
                    url: presigned.uri().into(),
                    path: file_path_str,
                }).await.expect("Failed to send command");
            }
        };

        upload_object_future_group.push(future);

        Ok(())
    }).expect("Iterating over path failed");

    let upload_count = upload_object_future_group.len();
    println!("Starting upload of {} files", upload_count);
    join_all(upload_object_future_group).await;
    while let Some(evt) = reply_rx.recv().await {
        let config_clone = config.clone();

        match evt {
            PutFile { path } => {
            println!("{:?} uploaded.", path);
                completed_file_count = completed_file_count + 1;
            }
            PutMultipart { path, part_number, etag, upload_id, total_parts } => {
                println!("{:?} uploaded. Part number: {}, etag: {}", path, part_number, etag);

                let entry = multipart_state.entry(path.clone()).or_insert((upload_id.clone(), String::new(), total_parts, Vec::new()));

                // Store the key from the path if not already set
                if entry.1.is_empty() {
                    entry.1 = path.clone();
                }

                // Add the completed part
                entry.3.push(CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build());

                // Check if all parts have been received
                if entry.3.len() == total_parts as usize {
                    println!("All parts received for {:?}, completing multipart upload", path);

                    // Sort parts by part number
                    entry.3.sort_by_key(|p| p.part_number());

                    let (upload_id_val, key_val, _, parts) = multipart_state.remove(&path).unwrap();

                    /*
                    let bucket = String::from(bucket);
                    compio_tx.clone().send(CompioCmd::CompleteMultipart { key: key_val.clone(), upload_id: upload_id_val.clone(), parts: parts.clone(), config : config_clone, bucket }).await.expect("Failed to send command");
                    */

                    complete_multipart_upload(&client, bucket, &key_val, &upload_id_val, parts).await.expect("Failed to complete multipart upload");

                    completed_file_count = completed_file_count + 1;
                }
            }

            /*
            CompioReply::CompleteMultipart { .. } => {
                completed_file_count = completed_file_count + 1;
            }

             */
        }

        println!("Completed files {} of {}", completed_file_count, upload_count);

        if completed_file_count == upload_count {
            break;
        }
    }

    list_bucket(&client, bucket).await.unwrap();
}

fn normalize_path(path: &str) -> String {
    // Step 1: Normalize using Path components
    let cleaned: PathBuf = Path::new(path).components().collect();

    // Step 2: Convert to string and force `/` separators
    let mut s = cleaned.to_string_lossy().replace('\\', "/");

    // Step 3: Remove all leading "./" and "../"
    loop {
        if s.starts_with("./") {
            s = s[2..].to_string();
        } else if s.starts_with("../") {
            s = s[3..].to_string();
        } else {
            break;
        }
    }

    s
}


/// Complete a multipart upload by providing all parts.
async fn complete_multipart_upload(
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
            println!("Completed multipart upload for key: {}", key);
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

async fn put_file(url: &str, path: &str, client: Client) {
    // Open the file asynchronously with Compio (io_uring on Linux).
    let file = match File::open(Path::new(path)).await {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{url} ERROR opening {path}: {e}");
            return;
        }
    };

    // Stream the file directly as the HTTP request body.
    // Cyper implements Body::from(compio::fs::File)
    let body = Body::from(file); // [1](https://docs.rs/compio/latest/compio/fs/struct.Metadata.html)

    let builder = RequestBuilder::new(client, Request::new(Method::PUT, Url::parse(url).unwrap()));

    match builder.body(body).send().await {
        Ok(_res) => {
            // Optionally read the response
            //let bytes = res.into_body().read_to_end().await.unwrap_or_default();

        }
        Err(e) => {
            eprintln!("{url} ERROR: {e}");
        }
    }
}


/// Stream 'len' bytes from 'file' starting at 'start', in chunks up to 'chunk_size'.
/// The stream yields `Bytes` and is accepted by `cyper::Body::stream`.
fn file_slice_stream(
    file: compio::fs::File,  // <-- not Arc
    start: u64,
    len: u64,
    chunk_size: usize,
) -> impl futures_util::Stream<Item = cyper::Result<Bytes>> + 'static {
    stream::unfold((file, start, len), move |(file, mut pos, mut remaining)| async move {
        if remaining == 0 {
            return None;
        }

        let to_read = remaining.min(chunk_size as u64) as usize;
        let buf = Vec::with_capacity(to_read);

        match file.read_at(buf, pos).await {
            BufResult(Ok(n), mut buf) => {
                if n == 0 {
                    return None;
                }
                unsafe { buf.set_len(n); }
                let chunk = Bytes::from(buf);
                pos += n as u64;
                remaining -= n as u64;
                Some((Ok(chunk), (file, pos, remaining)))
            }
            BufResult(Err(e), _) => {
                Some((Err(cyper::Error::from(e)), (file, pos, 0)))
            }
        }

    })
}



async fn put_file_multipart(url: &str, path: &str, start_pos: u64, end_pos: u64, client: Client) -> String {
    let file = OpenOptions::new().read(true).open(path).await.unwrap();
    let stream = file_slice_stream(file, start_pos, end_pos - start_pos, 256 * 1024);

    let stream = send_wrapper::SendWrapper::new(stream);

    let req = client
        .put(url).unwrap()
        .body(Body::stream(stream))
        .build();

    let resp = client.execute(req).await.unwrap();
    if resp.status() != StatusCode::OK {
        println!("{}", resp.status());
    }
    resp.headers().get("etag").unwrap().to_str().unwrap().to_string()
}

enum CompioCmd {
    PutFile { url: String, path: String },
    PutMultipart { url: String, path: String, start_pos: u64, end_pos: u64, part_number: i32, upload_id: String, total_parts: i32 },
    //CompleteMultipart {key: String, upload_id: String, parts: Vec<CompletedPart> , config: Config, bucket: String},
}

enum CompioReply {
    PutFile { path: String },
    PutMultipart { path: String, part_number: i32, etag: String, upload_id: String, total_parts: i32 },
    //CompleteMultipart { key: String, upload_id: String},
}

fn start_compio_worker() -> (Sender<CompioCmd>, Receiver<CompioReply>) {
    let (tx, mut rx) = mpsc::channel::<CompioCmd>(64);
    let (reply_tx, reply_rx) = mpsc::channel::<CompioReply>(64);

    thread::spawn(move || {
        // Compio must live on this thread
        let rt = compio::runtime::Runtime::new().expect("compio runtime");
        rt.block_on(async move {
            let client = cyper::Client::builder().use_rustls_default().build();
            let semaphore = Arc::new(Semaphore::new(100));

            while let Some(cmd) = rx.recv().await {

                //println!("Compio consumed command");

                let client_clone = client.clone();
                let reply_tx_clone = reply_tx.clone();
                let sem_clone = semaphore.clone();

                compio::runtime::spawn(async move {
                    let _permit = sem_clone.acquire().await.unwrap();

                    match cmd {
                        CompioCmd::PutFile { url, path } => {
                            println!("Putting file {}", path);

                            put_file(&url, &path, client_clone).await;
                            let _ = reply_tx_clone.send(PutFile { path }).await;
                        },
                        CompioCmd::PutMultipart { url, path, start_pos, end_pos, part_number, upload_id, total_parts } => {
                            println!("Putting part {} of {} for {}", part_number, total_parts, path);

                            let etag = put_file_multipart(&url, &path, start_pos, end_pos, client_clone).await;

                            let _ = reply_tx_clone.send(PutMultipart { path, part_number, etag, upload_id, total_parts }).await;
                        },

                        /*
                        CompioCmd::CompleteMultipart { key, upload_id, parts, config, bucket } => {
                            println!("Completing multipart upload for {}", key);
                            complete_multipart_upload_manual(&client_clone, &bucket, &key, &upload_id, parts, &config).await.expect("Failed to complete multipart upload manually");
                            let _ = reply_tx_clone.send(CompleteMultipart {key, upload_id }).await;
                        }
                        */

                    }
                }).detach();
            }
        });
    });

    (tx, reply_rx)
}

/// Generate a URL for a presigned GET request.
async fn presign_put_object(
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

/// Generate a presigned URL for uploading a specific part in a multipart upload.
async fn presign_upload_part(
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

fn create_config() -> Config {
    let config: Config = serde_json::from_str(
        std::fs::read_to_string(&constants::CONFIG_PATH)
            .expect("Error: failed to read config file.")
            .as_str()
    ).expect("Error: failed to deserialize config file.");
    config
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

fn parse_arguments() -> ArgMatches {
    let matches = Command::new("snowplowRust")
        .version("0.1.0")
        .about("Parses a -b argument and a path")
        .arg(
            Arg::new("bucket")
                .short('b')                 // allows -b
                .long("bucket")             // also allows --bucket
                .value_name("NAME")
                .help("Bucket (e.g., 'jon-test')")
                .required(true)
                .num_args(1),
        )
        .arg(
            Arg::new("path")
                .help("Input path")
                .required(true)
                .value_name("PATH")
                //.index(1),
        )
        .arg(
            Arg::new("o")
                .help("Transfer method")
                .required(false)
                .default_value("o1")
                //.index(1),
        )
        .after_help("Example:\n  snowplowRust -b jon-test testData2")
        .get_matches();
    matches
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

fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

async fn create_client(region: Region, access_key: &str, secret_key: &str, url: &str) -> AWSClient {
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


    let s3_conf = S3ConfigBuilder::from(&shared_config)
        .force_path_style(true)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .timeout_config(timeout_config)
        .build();


    let client = AWSClient::from_conf(s3_conf);
    client
}

