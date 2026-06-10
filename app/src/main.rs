mod upload;
mod auto_tuning;
mod ui;
mod download;
mod command_center;
mod config_and_parameter_parsing;

use std::time::Instant;

use clap::{Arg, ArgMatches, Command};

use std::sync::Arc;
use tokio;
use tracing_subscriber::{fmt, EnvFilter};
use util_layer::{constants, EndpointConfig, Mode};
use util_layer::pattern::PathPattern;

// Avoid musl's default allocator due to lackluster performance on systems with many cores
#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;


#[tokio::main]
async fn main() {
    let start_time = Instant::now();
    init_logging();

    // Read Commandline Arguments
    let matches = parse_arguments();

    match matches.subcommand() {
        Some(("config", _)) => {
            if let Err(e) = config::run_tui() {
                eprintln!("Error running config TUI: {}", e);
                std::process::exit(1);
            }
            return;
        }
        Some(("rm", _)) => {
            // Empty implementation as requested
            return;
        }
        Some(("ls", _)) => {
            // Empty implementation as requested
            return;
        }
        Some(("engine", _)) => {
            // Empty implementation as requested
            return;
        }
        None => {} // Continue to upload/download logic
        _ => unreachable!(),
    }

    let src_str = match matches.get_one::<String>("src") {
        Some(s) => s,
        None => {
            eprintln!("Error: src and dest are required for upload/download.");
            std::process::exit(1);
        }
    };
    let dest_str = match matches.get_one::<String>("dest") {
        Some(s) => s,
        None => {
            eprintln!("Error: src and dest are required for upload/download.");
            std::process::exit(1);
        }
    };

    let src = PathPattern::parse(src_str);
    let dest = PathPattern::parse(dest_str);

    let transfer_method = matches.get_one::<String>("o").expect("required");
    let file_limit = matches.get_one::<usize>("file_limit").copied();
    let req_limit = matches.get_one::<usize>("req_limit").copied();
    let download_concurrency = matches.get_one::<usize>("download_concurrency").copied();
    let silent = matches.get_flag("silent");
    let stats = matches.get_flag("stats");
    let md5_checksum_integrity = matches
        .get_one::<bool>("md5_checksum_integrity")
        .copied()
        .unwrap_or(true);

    let trailing = matches.get_flag("trailing");    // TODO LND: Temporary flag - should be a force-negation in release when CC starts setting it autonomously
    let trailing_small = matches.get_flag("trailing-small");    // TODO LND: Temporary flag - should be a force-negation in release when CC starts setting it autonomously
    let trailing_chunk_size = matches.get_one::<usize>("trailing-chunk-size").copied(); // TODO LND: Possibly a temporary flag
    let open_file_sem_limit = matches.get_one::<usize>("open-file-limit").copied();
    let method = match_transfer_method(Some(transfer_method.as_str())).expect(format!("Transfer method {} unknown", transfer_method).as_str());

    let (mode, profile, bucket, local_path) = match (&src, &dest) {
        (
            PathPattern::S3 {
                profile, bucket, ..
            },
            PathPattern::Local(path),
        ) => (Mode::Download, profile, bucket, path),

        (
            PathPattern::Local(path),
            PathPattern::S3 {
                profile, bucket, ..
            },
        ) => (Mode::Upload, profile, bucket, path),

        _ => {
            eprintln!("Error: One argument must be an S3 pattern (profile:bucket/path) and the other must be a local path.");
            std::process::exit(1);
        }
    };

    let endpoint = match gather_credentials(profile).await {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading configuration for profile '{}': {}", profile, e);
            std::process::exit(1);
        }
    };

    let trailing_chunk_size = trailing_chunk_size.unwrap_or(8 * 1024 * 1024) as u64;
    let open_file_sem_limit = Arc::new(tokio::sync::Semaphore::new(open_file_sem_limit.unwrap_or(100)));

    let snow_context = Arc::new(util_layer::build_context(
        endpoint,
        md5_checksum_integrity,
        transfer_method.clone(),
        file_limit,
        req_limit,
        download_concurrency,
        silent,
        stats,
        trailing_chunk_size,
        trailing,
        trailing_small,
        open_file_sem_limit,
        mode,
        bucket.to_string(),
        local_path.to_path_buf(),
    ));

    match mode {
        Mode::Download => {
            match method {
                TransferMethod::CephS3Tokio => download::download(snow_context).await,
                TransferMethod::TransferManager => transfer_manager::download(snow_context).await,
            }
        }
        Mode::Upload => {
            match method {
                TransferMethod::CephS3Tokio => upload::upload(snow_context).await,
                TransferMethod::TransferManager => transfer_manager::upload(snow_context).await,
            };
        }
        Mode::List => {}
        Mode::Remove => {}
    }

    let elapsed = start_time.elapsed();
    if !silent {
        println!("Program execution time: {:.2?}", elapsed);
    }
}

async fn gather_credentials(profile_name: &str) -> anyhow::Result<EndpointConfig> {
    let app_config = config::load_config()?;

    let matches: Vec<_> = app_config
        .endpoints
        .iter()
        .filter(|e| e.name == profile_name)
        .collect();

    if matches.is_empty() {
        anyhow::bail!("Profile '{}' not found in configuration", profile_name);
    }

    if matches.len() > 1 {
        anyhow::bail!("Profile '{}' is not unique in configuration", profile_name);
    }

    if let Some(endpoint_config) = &matches[0].config {
        return Ok(endpoint_config.clone());
    }

    anyhow::bail!("Profile '{}' has no configuration", profile_name)
}

enum TransferMethod {
    CephS3Tokio,
    TransferManager,
}

fn parse_arguments() -> ArgMatches {
    let matches = Command::new("snowcopy")
        .version("0.1.0-alpha.1")
        .about("Fast S3 copy tool")
        .subcommand_required(false)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("config")
                .about("Open the TUI for configuration management")
        )
        .subcommand(
            Command::new("rm")
                .about("Remove a resource")
                .arg(Arg::new("path").required(true).index(1))
        )
        .subcommand(
            Command::new("ls")
                .about("List resources")
                .arg(Arg::new("path").required(true).index(1))
        )
        .subcommand(
            Command::new("engine")
                .about("Engine command")
        )
        .arg(
            Arg::new("src")
                .help("Source path (local or profile:bucket/key)")
                .required(false)
                .index(1),
        )
        .arg(
            Arg::new("dest")
                .help("Destination path (local or profile:bucket/key)")
                .required(false)
                .index(2),
        )
        .arg(
            Arg::new("o")
                .help("Transfer method")
                .short('o')
                .required(false)
                .default_value("o1")
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
            Arg::new("download_concurrency")
                .short('c')
                .long("download-concurrency")
                .value_name("NUMBER")
                .help("Maximum number of concurrent downloads per file")
                .required(false)
                .value_parser(clap::value_parser!(usize))
                .num_args(1),
        )
        .arg(
            Arg::new("silent")
                .short('q')
                .long("silent")
                .help("Suppress output messages")
                .required(false)
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("stats")
                .long("stats")
                .help("Enable statistics output")
                .required(false)
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("md5_checksum_integrity")
                .long("disable-md5-checksum-integrity")
                .help("Disable MD5 checksum validation")
                .required(false)
                .default_value("true")
                .action(clap::ArgAction::SetFalse),
        )
        .arg(
            Arg::new("trailing")
                .long("trailing")
                .help("Trailing header mode (uses trailing headers for S3 operations)")
                .required(false)
                .action(clap::ArgAction::SetTrue)
        )
        .arg(
            Arg::new("trailing-small")
                .long("trailing-small")
                .help("Trailing header mode for small files (uses trailing headers for S3 operations)")
                .required(false)
                .action(clap::ArgAction::SetTrue)
        )
        .arg(
            Arg::new("trailing-chunk-size")
                .long("trailing-chunk-size")
                .value_name("NUMBER")
                .help("Size of chunks used for trailing header operations in kB")
                .required(false)
                .value_parser(clap::value_parser!(usize))
                .num_args(1),
        )
        .arg(
            Arg::new("open-file-limit")
                .long("open-file-limit")
                .value_name("NUMBER")
                .help("Limit of simultaneously open file descriptors")
                .required(false)
                .value_parser(clap::value_parser!(usize))
                .num_args(1),
        )
        .after_help("Examples:\n  up:   snowcopy file.txt profile:bucket\n  down: snowcopy profile:bucket/file.txt .")
        .get_matches();
    matches
}

fn match_transfer_method(method: Option<&str>) -> Option<TransferMethod> {
    match method {
        Some("o1") => Some(TransferMethod::CephS3Tokio),
        Some("o2") => Some(TransferMethod::TransferManager),
        _other => None,
    }
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hyper=info,hyper_util=info"));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}