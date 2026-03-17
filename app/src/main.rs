mod upload;
mod auto_tuning;
mod ui;
mod download;
mod command_center;

use serde_json::Result;

use std::time::Instant;

use clap::{Arg, ArgMatches, Command};

use std::path::Path;
use tokio;
use tracing_subscriber::{fmt, EnvFilter};
use util_layer::{constants, Config};

// Avoid musl's default allocator due to lackluster performance on systems with many cores
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;


#[tokio::main]
async fn main() -> Result<()> {
    let start_time = Instant::now();
    init_logging();

    // Read Commandline Arguments
    let matches = parse_arguments();
    let bucket = matches.get_one::<String>("bucket").expect("required");
    let path_as_string = matches.get_one::<String>("path").expect("required");
    let transfer_method = matches.get_one::<String>("o").expect("required");
    let file_limit = matches.get_one::<usize>("file_limit").copied();
    let req_limit = matches.get_one::<usize>("req_limit").copied();
    let silent = matches.get_flag("silent");
    let download = matches.get_flag("download");


    //println!("Bucket (-b): {}", bucket);
    //println!("Path: {}", path_as_string);
    let path = Path::new(path_as_string);

    let method = match_transfer_method(Some(transfer_method)).expect(format!("Transfer method {} unknown", transfer_method).as_str());

    if download { //TODO Jon: are we planning on supporting multiple implementations in the actual production code? I think not. At least not for first release. Hmm, maybe a hdd and ssd mode
        download::download(bucket, "", path, file_limit, req_limit, silent).await;
    } else {
        match method {
            TransferMethod::CephS3Tokio => upload::run_s3_ceph_tokio(bucket, path, file_limit, req_limit, silent).await
        };
    }

    let elapsed = start_time.elapsed();
    if !silent {
        println!("Program execution time: {:.2?}", elapsed);
    }

    Ok(())
}

enum TransferMethod {
    CephS3Tokio,
}

fn match_transfer_method(method: Option<&str>) -> Option<TransferMethod> {
    match method {
        Some("o2") => Some(TransferMethod::CephS3Tokio),
        _other => None,
    }
}

fn read_config() -> Config {
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
        .arg(
            Arg::new("file_limit")
                .short('f')
                .long("file-limit")
                .value_name("NUMBER")
                .help("Maximum number of files to process")
                .required(false)
                .value_parser(clap::value_parser!(usize))
                .num_args(1),
        )
        .arg(
            Arg::new("req_limit")
                .short('r')
                .long("req-limit")
                .value_name("NUMBER")
                .help("Maximum number of concurrent requests")
                .required(false)
                .value_parser(clap::value_parser!(usize))
                .num_args(1),
        )
        .arg(
            Arg::new("silent")
                .short('s')
                .long("silent")
                .help("Suppress output messages")
                .required(false)
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("download")
                .short('d')
                .long("download")
                .help("Download mode")
                .required(false)
                .action(clap::ArgAction::SetTrue),
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