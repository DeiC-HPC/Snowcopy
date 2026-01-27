mod s3example_error;

use crate::TransferMethod::CephS3;
use serde;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::env;

use aws_sdk_s3::config::{Credentials, Region};

use aws_sdk_s3::{Client as S3Client, config::{
    Builder as S3ConfigBuilder,
    RequestChecksumCalculation,
    ResponseChecksumValidation,
}, Client};

use clap::{Arg, ArgMatches, Command};
use std::path::Path;
use aws_config::BehaviorVersion;
use aws_sdk_s3::operation::create_bucket::CreateBucketOutput;
use tracing_subscriber::{EnvFilter, fmt};
use crate::s3example_error::S3ExampleError;

static CONFIG_PATH: &str = "config.json";
static DEFAULT_REGION:&str = "us-east-1";

//#[compio::main]
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
    CephS3
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

struct Args {
    /// Path to the file to upload
    file_path: String,

    /// S3 object key
    object_key: String,

    /// Transfer method
    //#[arg(short, default_value = "o1")]
    transfer_method: String,
}

async fn run_s3_ceph(bucket: &str, path: &Path) {
    //TODO: iterate over all files in path instead of assuming only one exists
            //Upload files iterated over
    //TODO: Make presigner work for PUT request and use hyper client to manually make putrequest for a single file: https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/rustv1/examples/s3/src/bin/get-object-presigned.rs
    //TODO: Try to parallize with combio, possibly replacing tokio from main, since combio is an async runtime as well
    //TODO: Start multipart upload with aws client: https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/rustv1/examples/s3/src/bin/s3-multipart-upload.rs#L48
    //TODO: read large file in chunks
    //TODO: End multipart upload with aws client


    // Read config file
    let config: Config = serde_json::from_str(
        std::fs::read_to_string(&CONFIG_PATH)
            .expect("Error: failed to read config file.")
            .as_str()
    ).expect("Error: failed to deserialize config file.");

    let client = create_client(Region::new(DEFAULT_REGION), config.s3_access_key.as_str(), config.s3_secret_key.as_str()).await;

    create_bucket(&client, bucket, &Region::new(DEFAULT_REGION)).await.unwrap();
    list_bucket_and_upload_object(&client, bucket, path, path.file_name().unwrap().to_str().unwrap()).await.unwrap();


    /*
    // Set up AWS region and credentials
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::from_env().region(region_provider).load().await;
    let client = aws_sdk_s3::Client::new(&config);

    // Open the file asynchronously using compio
    let file = File::open(file_path).await.unwrap();
    let (read, buffer) = file.read_to_end_at(Vec::with_capacity(1024), 0).await.unwrap();
    assert_eq!(read, buffer.len());
    let buffer = String::from_utf8(buffer).unwrap();
    let reader = BufReader::new(buffer);

   // Upload the file to S3
    client
        .put_object()
        .bucket(bucket)
        .key(file_name)
        .body(reader)
        .send()
        .await?;

 */
    println!("File uploaded successfully.");
}

pub async fn create_bucket(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    region: &aws_config::Region,
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
async fn list_bucket_and_upload_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    filepath: &Path,
    key: &str,
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

    // Prepare a ByteStream around the file, and upload the object using that ByteStream.
    let body = aws_sdk_s3::primitives::ByteStream::from_path(filepath)
        .await
        .map_err(|err| {
            s3example_error::S3ExampleError::new(format!(
                "Failed to create bytestream for {filepath:?} ({err:?})"
            ))
        })?;
    let resp = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;
    println!("etag: {}", resp.e_tag().unwrap());


    /*
    // Retrieve the just-uploaded object.
    let resp = client.get_object().bucket(bucket).key(key).send().await?;
    println!("etag: {}", resp.e_tag().unwrap_or("(missing)"));
     */

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

async fn create_client(region: Region, access_key: &str, secret_key: &str) -> Client {
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
        .endpoint_url("https://lumidata.eu")
        .load()
        .await;

    let s3_conf = S3ConfigBuilder::from(&shared_config)
        .force_path_style(true)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .build();


    let client = S3Client::from_conf(s3_conf);
    client
}

