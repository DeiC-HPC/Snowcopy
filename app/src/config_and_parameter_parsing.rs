use crate::bucket_and_path_parser::ParsedInputPath;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use util_layer::{ConfigModeConfig, DownloadConfig, EndpointConfig, ListConfig, RemoveConfig, SnowConfig, TransferMethod, UploadConfig};
use util_layer::error_codes::ErrorCode::HttpUnsafeError;

#[derive(Parser)]
#[command(
name = "snowcopy",
version = "0.1.0-alpha.1",
about = "Fast S3 copy tool - ALPHA, use with caution.",
after_help = "Examples:\n  upload:   snowcopy cp my-folder profile:bucket\n  upload:   snowcopy cp file.txt profile:bucket\n  download: snowcopy cp profile:bucket ."
)]
struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[arg(long, global = true, help = "Path to a config file to use instead of the default")]
    pub config: Option<PathBuf>,

    #[arg(long, global = true, help = "Allow using insecure HTTP endpoints")]
    pub use_unsafe_http: bool,

    #[arg(short = 'v', long, global = true, help = "Verbose mode (print http headers)")]
    pub verbose: bool,

    #[arg(short = 'q', long, global = true, help = "Silent mode (no output)")]
    pub quiet: bool,

    #[arg(short = 'r', long, help = "Maximum number of concurrent requests")]
    pub req_limit: Option<usize>,
}

#[derive(clap::Args, Debug)]
pub struct UploadOptions {
    #[arg(long, help = "Enable trailing header mode on large files")]
    pub trailing: bool,

    #[arg(long, help = "Enable trailing header mode on small files")]
    pub trailing_small: bool,

    #[arg(long, help = "Size of chunks used for trailing header operations in kB", default_value = "1024")]
    pub trailing_chunk_size: u64,
}

#[derive(clap::Args, Debug)]
pub struct DownloadOptions {
    #[arg(short = 'c', long, help = "Maximum number of concurrent downloads per file")]
    pub download_single_file_concurrency_limit: Option<usize>,
}

#[derive(clap::Args, Debug)]
pub struct TransferOptions {
    #[arg(short = 'f', long, help = "Maximum number of files to do read/write operations on at once")]
    pub file_limit: Option<usize>,

    #[arg(long, help = "Maximum number of file descriptors used simultaneously")]
    pub open_file_limit: Option<usize>,

    #[arg(long, help = "Prints statistics about the transfer")]
    pub stats: bool,

    #[arg(long, help = "Disable MD5 checksum validation")]
    pub disable_md5_checksum: bool,

    #[arg(long, help = "Transfer method", default_value = "o1")]
    pub transfer_method: TransferMethod,
}


#[derive(Subcommand)]
enum Commands {
    #[command(about = "Open the TUI for configuration management")]
    Config,

    #[command(about = "List buckets and bucket contents")]
    Ls {
        #[arg(help = "Either profile: or profile:bucket (if bucket is empty, all buckets are listed)")]
        input_path: ParsedInputPath,
    },

    #[command(about = "Remove/delete a bucket along with ALL its contents")]
    Rm {
        #[arg(help = "profile:bucket")]
        input_path: ParsedInputPath,
    },

    #[command(about = "Upload or download files")]
    Cp {
        #[arg(value_name = "Source", help = "Either a local folder/file or profile:bucket")]
        src: ParsedInputPath,
        #[arg(value_name = "Destination", help = "Either a local folder or profile:bucket")]
        dest: ParsedInputPath,

        #[command(flatten)]
        transfer: TransferOptions,

        #[command(flatten)]
        upload: UploadOptions,

        #[command(flatten)]
        download: DownloadOptions,
    },
}

pub async fn read_and_validate_command_line_arguments() -> SnowConfig {
    let cli = Cli::parse();

    match cli.command {
        Commands::Config => {
            SnowConfig::Config(ConfigModeConfig { silent : cli.quiet, config_path : cli.config })
        }

        Commands::Ls { input_path } => {
            match input_path {
                ParsedInputPath::S3Server { profile } => {
                    let endpoint = load_endpoint_and_stop_program_with_error_if_it_fails(&profile, cli.config.as_ref() , cli.use_unsafe_http);

                    SnowConfig::List(ListConfig {
                        endpoint,
                        bucket : None,
                        req_limit: cli.req_limit,
                        verbose : cli.verbose,
                        use_unsafe_http : cli.use_unsafe_http,
                    })
                }

                ParsedInputPath::S3Bucket { profile, bucket, .. } => {
                    let endpoint = load_endpoint_and_stop_program_with_error_if_it_fails(&profile, cli.config.as_ref(), cli.use_unsafe_http);

                    SnowConfig::List(ListConfig {
                        endpoint,
                        bucket : Some(bucket),
                        req_limit: cli.req_limit,
                        verbose : cli.verbose,
                        use_unsafe_http : cli.use_unsafe_http,
                    })
                }

                _ => {
                    eprintln!("Error: ls requires either profile: or profile:bucket (if bucket is empty, all buckets are listed)");
                    std::process::exit(1);
                }
            }
        }

        Commands::Rm { input_path } => {
            match input_path {
                ParsedInputPath::S3Bucket { profile, bucket, key } => {
                    if key.is_some() {
                        eprintln!("Error: rm does not support deleting individual objects at this time");
                        std::process::exit(1);
                    }

                    let endpoint = load_endpoint_and_stop_program_with_error_if_it_fails(&profile, cli.config.as_ref(), cli.use_unsafe_http);

                    SnowConfig::Remove(RemoveConfig {
                        endpoint,
                        bucket,
                        silent : cli.quiet,
                        verbose : cli.verbose,
                        use_unsafe_http : cli.use_unsafe_http,
                        req_limit_overwrite : cli.req_limit,
                    })
                }

                _ => {
                    eprintln!("Error: ls requires either profile: or profile:bucket (if bucket is empty, all buckets are listed)");
                    std::process::exit(1);
                }
            }
        }

        Commands::Cp { src, dest, transfer, upload, download } => {
            match (&src, &dest) {
                (ParsedInputPath::Local( local_path), ParsedInputPath::S3Bucket { profile, bucket, key }) => {
                    let endpoint = load_endpoint_and_stop_program_with_error_if_it_fails(&profile, cli.config.as_ref(), cli.use_unsafe_http);

                    SnowConfig::Upload(UploadConfig {
                        endpoint: endpoint.clone(),
                        md5_checksum_integrity_is_disabled: transfer.disable_md5_checksum,
                        transfer_method : transfer.transfer_method,
                        file_limit_overwrite : transfer.file_limit,
                        req_limit_overwrite : cli.req_limit,
                        silent : cli.quiet,
                        verbose : cli.verbose,
                        stats : transfer.stats,
                        use_unsafe_http : cli.use_unsafe_http,
                        trailing_chunk_size: upload.trailing_chunk_size ,
                        trailing : upload.trailing,
                        trailing_small : upload.trailing_small,
                        open_file_limit_overwrite : transfer.open_file_limit,
                        bucket : bucket.clone(),
                        local_path : local_path.clone(),
                    })
                }

                (ParsedInputPath::S3Bucket { profile, bucket, .. }, ParsedInputPath::Local(local_path)) => {
                    let endpoint = load_endpoint_and_stop_program_with_error_if_it_fails(&profile, cli.config.as_ref(), cli.use_unsafe_http);

                    SnowConfig::Download(DownloadConfig {
                        endpoint: endpoint.clone(),
                        md5_checksum_integrity_is_disabled: transfer.disable_md5_checksum,
                        transfer_method : transfer.transfer_method,
                        file_limit_overwrite : transfer.file_limit,
                        req_limit_overwrite  : cli.req_limit,
                        download_concurrency_overwrite : download.download_single_file_concurrency_limit,
                        silent : cli.quiet,
                        verbose : cli.verbose,
                        stats : transfer.stats,
                        use_unsafe_http : cli.use_unsafe_http,
                        open_file_limit_overwrite : transfer.open_file_limit,
                        bucket : bucket.clone(),
                        local_path: local_path.clone(),
                        prefix : String::new() //TODO Jon: add support for prefix
                    })
                }

                _ => {
                    eprintln!("Invalid source/destination combination. Use \"snowcopy cp <src> <dest>\", where <src> and <dest> are either a local folder or a profile:bucket.");
                    std::process::exit(1);
                }
            }
        }
    }
}
fn load_endpoint_and_stop_program_with_error_if_it_fails(profile: &String, config_path: Option<&std::path::PathBuf>, http_allowed: bool) -> EndpointConfig {
    let endpoint = match gather_credentials(&profile, config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading configuration for profile '{}': {}\nRun \"snowcopy config\" to configure profiles", profile, e);
            std::process::exit(1);
        }
    };
    check_http_safeguard(&endpoint, http_allowed);
    endpoint
}

fn check_http_safeguard(endpoint: &EndpointConfig, use_unsafe_http: bool) {
    if !use_unsafe_http {
        if let util_layer::url_parser::S3EndpointType::HTTP = util_layer::url_parser::validate_s3_endpoint(&endpoint.s3_url) {
            eprintln!("Error: The endpoint {} uses the insecure HTTP protocol. \
            Using HTTP can expose your data and credentials to potential security risks such as man-in-the-middle attacks. \
            It is strongly recommended to use HTTPS instead. \
            If you still wish to proceed, use the --use-unsafe-http flag.", endpoint.s3_url);
            std::process::exit(HttpUnsafeError.as_i32());
        }
    }
}

pub fn gather_credentials(profile_name: &str, config_path: Option<&std::path::PathBuf>) -> anyhow::Result<EndpointConfig> {
    let app_config = config::load_config(config_path.cloned())?;

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
