mod s3example_error;

use crate::TransferMethod::CephS3;
use serde;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::{fs, io};

use aws_sdk_s3::config::{Credentials, Region};

use crate::s3example_error::S3ExampleError;
use aws_config::BehaviorVersion;
use aws_sdk_s3::operation::create_bucket::CreateBucketOutput;
use aws_sdk_s3::presigning::{PresignedRequest, PresigningConfig};
use aws_sdk_s3::{config::{
    Builder as S3ConfigBuilder,
    RequestChecksumCalculation,
    ResponseChecksumValidation,
}, Client as AWSClient};
use clap::{Arg, ArgMatches, Command};
use compio::fs::File;
use cyper::{Body, Client, Request, RequestBuilder};
use futures::future::join_all;
use http::Method;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use tokio;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing_subscriber::{fmt, EnvFilter};
use url::Url;

static CONFIG_PATH: &str = "config.json";
static DEFAULT_REGION:&str = "us-east-1";

#[tokio::main]
async fn main() -> Result<()> {
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

#[derive(Serialize, Deserialize)]
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

    let client = create_client(Region::new(DEFAULT_REGION), config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str()).await;
    create_bucket(&client, bucket).await.unwrap();

    let root = PathBuf::from(path);
    let mut upload_object_future_group = Vec::new();
    let mut completed_file_count = 0;

    walk_dir_files(&root, |file_path| {
        let metadata = fs::metadata(file_path)?;
        let size = metadata.len();

        println!("{}: {} bytes", file_path.display(), size);
        let key_clone = file_path.file_name().unwrap().to_str().unwrap().to_string();
        let bucket_clone = bucket.to_string();
        let client_clone = client.clone();
        let compio_tx_clone = compio_tx.clone();
        let file_path_str = file_path.to_str().unwrap().to_string();


        let future = async move {
            let presigned = presign_put_object(&client_clone, &bucket_clone, &key_clone, 900).await;
            println!("Presign url: {}", presigned.uri());

            compio_tx_clone.send(CompioCmd::PutFile {
                url: presigned.uri().into(),
                path: file_path_str,
            }).await.expect("Failed to send command");

        };

        upload_object_future_group.push(future);

        Ok(())
    }).expect("Iterating over path failed");

    println!("Starting upload of {} files", upload_object_future_group.len());
    join_all(upload_object_future_group).await;
    while let Some(evt) = reply_rx.recv().await {
        println!("{:?} uploaded.", evt);
        completed_file_count = completed_file_count + 1;

        if completed_file_count == 3 { //TODO JON: fix :)
            break;
        }
    }

    //TODO: Start multipart upload with aws client: https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/rustv1/examples/s3/src/bin/s3-multipart-upload.rs#L48
    //TODO: read large file in chunks
    //TODO: End multipart upload with aws client


    list_bucket(&client, bucket).await.unwrap();

    println!("File uploaded successfully.");
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
            println!("{url} PUT OK");
        }
        Err(e) => {
            eprintln!("{url} ERROR: {e}");
        }
    }
}

enum CompioCmd {
    PutFile { url: String, path: String },
}

fn start_compio_worker() -> (Sender<CompioCmd>, Receiver<String>) {
    let (tx, mut rx) = mpsc::channel::<CompioCmd>(64);
    let (reply_tx, reply_rx) = mpsc::channel::<String>(64);

    thread::spawn(move || {
        // Compio must live on this thread
        let rt = compio::runtime::Runtime::new().expect("compio runtime");
        rt.block_on(async move {
            let client = cyper::Client::builder().use_rustls_default().build();

            while let Some(cmd) = rx.recv().await {
                match cmd {
                    CompioCmd::PutFile { url, path } => {
                        put_file(&url, &path, client.clone()).await;
                        let _ = reply_tx.send(path.clone()).await;
                    }
                }
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

    println!("Object URI: {}", presigned_request.uri());
    let valid_until = chrono::offset::Local::now() + expires_in;
    println!("Valid until: {valid_until}");

    presigned_request
}

fn create_config() -> Config {
    let config: Config = serde_json::from_str(
        std::fs::read_to_string(&CONFIG_PATH)
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

    let s3_conf = S3ConfigBuilder::from(&shared_config)
        .force_path_style(true)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .build();


    let client = AWSClient::from_conf(s3_conf);
    client
}

