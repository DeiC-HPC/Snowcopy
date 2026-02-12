mod constants;
mod signer;
mod tokio_transfer;
mod old_compio_code;

use serde;
use serde::{Deserialize, Serialize};
use serde_json::Result;

use std::time::Instant;

use clap::{Arg, ArgMatches, Command};

use std::path::Path;
use tokio;
use tracing_subscriber::{fmt, EnvFilter};


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
        TransferMethod::CephS3 => panic!("functionality removed"), //run_s3_ceph_compio(bucket, path).await,
        TransferMethod::CephS3Tokio => tokio_transfer::run_s3_ceph_tokio(bucket, path).await,
    };

    let elapsed = start_time.elapsed();
    println!("Program execution time: {:.2?}", elapsed);

    Ok(())
}

enum TransferMethod {
    CephS3,
    CephS3Tokio,
}

fn match_transfer_method(method: Option<&str>) -> Option<TransferMethod> {
    match method {
        Some("o1") => Some(TransferMethod::CephS3),
        Some("o2") => Some(TransferMethod::CephS3Tokio),
        _other => None,
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Config {
    s3_access_key: String,
    s3_secret_key: String,
    s3_url: String
}

fn create_config() -> Config {
    let config: Config = serde_json::from_str(
        std::fs::read_to_string(&constants::CONFIG_PATH)
            .expect("Error: failed to read config file.")
            .as_str()
    ).expect("Error: failed to deserialize config file.");
    config
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
                .short('o')
                .required(false)
                .default_value("o2")
            //.index(1),
        )
        .after_help("Example:\n  snowplowRust -b jon-test testData2")
        .get_matches();
    matches
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=info,hyper_util=info"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}