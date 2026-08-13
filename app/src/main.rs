pub mod constants {
    pub use util_layer::constants::*;
}

mod upload;
mod auto_tuning;
mod ui;
mod download;
mod command_center;
mod config_and_parameter_parsing;
mod error_print;
mod list;
mod remove;
mod filesystem_checker;
mod traits;
mod bucket;
mod upload_singlepart;
mod upload_multipart;

use std::sync::Arc;
use std::time::Instant;

use crate::config_and_parameter_parsing::{parse_arguments, read_parameters_and_config_to_create_snow_config};
use crate::error_print::{print_error, print_fatal_error};
use tokio;
use tracing_subscriber::{EnvFilter, fmt};
use util_layer::snowcopy_result::SnowcopyResult;
use util_layer::{SnowConfig, TransferMethod};

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
    let snow_config = read_parameters_and_config_to_create_snow_config(&matches).await;

    let result = match snow_config {
        SnowConfig::Download(config) => {
            let config = Arc::new(config);
            let silent = config.silent;
            let res = match config.transfer_method {
                TransferMethod::TokioSelfMade => download::download(config).await,
                TransferMethod::TransferManager => {
                    transfer_manager::download(config).await;
                    SnowcopyResult{fatal_error: None, error: None}
                }
            };
            if !silent {
                println!("Program execution time: {:.2?}", start_time.elapsed());
            }
            res
        }
        SnowConfig::Upload(config) => {
            let config = Arc::new(config);
            let silent = config.silent;
            let res = match config.transfer_method {
                TransferMethod::TokioSelfMade => upload::run_upload(&config).await,
                TransferMethod::TransferManager => {
                    transfer_manager::upload(config).await;
                    SnowcopyResult{fatal_error: None, error: None}
                }
            };
            if !silent {
                println!("Program execution time: {:.2?}", start_time.elapsed());
            }
            res
        }
        SnowConfig::List(config) => {
            list::list(config).await
        }
        
        SnowConfig::Remove(config) => {
            let silent = config.silent;
            let res = remove::remove(config).await;
            if !silent {
                println!("Program execution time: {:.2?}", start_time.elapsed());
            }
            res
        }
        SnowConfig::Config(config) => {
            config::run_tui(config)
        }
    };

    if result.fatal_error.is_some() {
        let error = result.fatal_error.unwrap();
        print_fatal_error(&error);
        std::process::exit(error.code().as_i32());
    } else if result.error.is_some() {
        let error = result.error.unwrap();
        print_error(&error);
        std::process::exit(error.code().as_i32());
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