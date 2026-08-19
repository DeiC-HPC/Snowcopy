use crate::bucket_and_path_parser::ParsedInputPath;
use clap::{Parser, Subcommand};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use util_layer::{ConfigModeConfig, DownloadConfig, EndpointConfig, ListConfig, RemoveConfig, SnowConfig, TransferMethod, UploadConfig};
use util_layer::error_codes::ErrorCode::HttpUnsafeError;

#[derive(Debug)]
pub enum CliError {
    UnsafeHttp(String),
    ProfileNotFound(String),
    ProfileNotUnique(String),
    ProfileNoConfig(String),
    ConfigError(String),
    InvalidCommand(String),
    RmNotSupportedForKey,
}

impl CliError {
    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::UnsafeHttp(_) => HttpUnsafeError.as_i32(),
            _ => 1,
        }
    }
}

impl Display for CliError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::UnsafeHttp(url) => write!(f, "Error: The endpoint {} uses the insecure HTTP protocol. \
            Using HTTP can expose your data and credentials to potential security risks such as man-in-the-middle attacks. \
            It is strongly recommended to use HTTPS instead. \
            If you still wish to proceed, use the --use-unsafe-http flag.", url),
            CliError::ProfileNotFound(profile) => write!(f, "Error loading configuration for profile '{}': Profile not found in configuration\nRun \"snowcopy config\" to configure profiles", profile),
            CliError::ProfileNotUnique(profile) => write!(f, "Error loading configuration for profile '{}': Profile is not unique in configuration", profile),
            CliError::ProfileNoConfig(profile) => write!(f, "Error loading configuration for profile '{}': Profile has no configuration", profile),
            CliError::ConfigError(msg) => write!(f, "Error loading configuration: {}", msg),
            CliError::InvalidCommand(msg) => write!(f, "Error: {}", msg),
            CliError::RmNotSupportedForKey => write!(f, "Error: rm does not support deleting individual objects at this time"),
        }
    }
}

pub trait CredentialProvider {
    fn gather(&self, profile_name: &str, config_path: Option<&PathBuf>) -> Result<EndpointConfig, CliError>;
}

pub struct FileCredentialProvider;

impl CredentialProvider for FileCredentialProvider {
    fn gather(&self, profile_name: &str, config_path: Option<&PathBuf>) -> Result<EndpointConfig, CliError> {
        gather_credentials(profile_name, config_path)
    }
}

#[derive(Parser)]
#[command(
name = "snowcopy",
version = "0.1.0-alpha.1",
about = "Fast S3 copy tool - ALPHA, use with caution.",
after_help = "Examples:\n  upload:   snowcopy cp my-folder profile:bucket\n  upload:   snowcopy cp file.txt profile:bucket\n  download: snowcopy cp profile:bucket ."
)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, global = true, help = "Path to a config file to use instead of the default")]
    pub config: Option<PathBuf>,

    #[arg(long, global = true, help = "Allow using insecure HTTP endpoints")]
    pub use_unsafe_http: bool,

    #[arg(short = 'v', long, global = true, help = "Verbose mode (print http headers)")]
    pub verbose: bool,

    #[arg(short = 'q', long, global = true, help = "Quiet/silent mode (no output)")]
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
    let provider = FileCredentialProvider;

    build_config(cli, &provider).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(e.exit_code());
    })
}

pub fn build_config(cli: Cli, provider: &dyn CredentialProvider) -> Result<SnowConfig, CliError> {
    match cli.command {
        Commands::Config => {
            Ok(SnowConfig::Config(ConfigModeConfig { quiet: cli.quiet, config_path : cli.config }))
        }

        Commands::Ls { input_path } => {
            match input_path {
                ParsedInputPath::S3Server { profile } => {
                    let endpoint = provider.gather(&profile, cli.config.as_ref())?;
                    check_http_safeguard(&endpoint, cli.use_unsafe_http)?;

                    Ok(SnowConfig::List(ListConfig {
                        endpoint,
                        bucket : None,
                        req_limit: cli.req_limit,
                        verbose : cli.verbose,
                        use_unsafe_http : cli.use_unsafe_http,
                    }))
                }

                ParsedInputPath::S3Bucket { profile, bucket, .. } => {
                    let endpoint = provider.gather(&profile, cli.config.as_ref())?;
                    check_http_safeguard(&endpoint, cli.use_unsafe_http)?;

                    Ok(SnowConfig::List(ListConfig {
                        endpoint,
                        bucket : Some(bucket),
                        req_limit: cli.req_limit,
                        verbose : cli.verbose,
                        use_unsafe_http : cli.use_unsafe_http,
                    }))
                }

                _ => {
                    Err(CliError::InvalidCommand("ls requires either profile: or profile:bucket (if bucket is empty, all buckets are listed)".to_string()))
                }
            }
        }

        Commands::Rm { input_path } => {
            match input_path {
                ParsedInputPath::S3Bucket { profile, bucket, key } => {
                    if key.is_some() {
                        return Err(CliError::RmNotSupportedForKey);
                    }

                    let endpoint = provider.gather(&profile, cli.config.as_ref())?;
                    check_http_safeguard(&endpoint, cli.use_unsafe_http)?;

                    Ok(SnowConfig::Remove(RemoveConfig {
                        endpoint,
                        bucket,
                        quiet: cli.quiet,
                        verbose : cli.verbose,
                        use_unsafe_http : cli.use_unsafe_http,
                        req_limit_overwrite : cli.req_limit,
                    }))
                }

                _ => {
                    Err(CliError::InvalidCommand("ls requires either profile: or profile:bucket (if bucket is empty, all buckets are listed)".to_string()))
                }
            }
        }

        Commands::Cp { src, dest, transfer, upload, download } => {
            match (&src, &dest) {
                (ParsedInputPath::Local( local_path), ParsedInputPath::S3Bucket { profile, bucket, .. }) => {
                    let endpoint = provider.gather(&profile, cli.config.as_ref())?;
                    check_http_safeguard(&endpoint, cli.use_unsafe_http)?;

                    Ok(SnowConfig::Upload(UploadConfig {
                        endpoint: endpoint.clone(),
                        md5_checksum_integrity_is_disabled: transfer.disable_md5_checksum,
                        transfer_method : transfer.transfer_method,
                        file_limit_overwrite : transfer.file_limit,
                        req_limit_overwrite : cli.req_limit,
                        quiet: cli.quiet,
                        verbose : cli.verbose,
                        stats : transfer.stats,
                        use_unsafe_http : cli.use_unsafe_http,
                        trailing_chunk_size: upload.trailing_chunk_size ,
                        trailing : upload.trailing,
                        trailing_small : upload.trailing_small,
                        open_file_limit_overwrite : transfer.open_file_limit,
                        bucket : bucket.clone(),
                        local_path : local_path.clone(),
                    }))
                }

                (ParsedInputPath::S3Bucket { profile, bucket, .. }, ParsedInputPath::Local(local_path)) => {
                    let endpoint = provider.gather(&profile, cli.config.as_ref())?;
                    check_http_safeguard(&endpoint, cli.use_unsafe_http)?;

                    Ok(SnowConfig::Download(DownloadConfig {
                        endpoint: endpoint.clone(),
                        md5_checksum_integrity_is_disabled: transfer.disable_md5_checksum,
                        transfer_method : transfer.transfer_method,
                        file_limit_overwrite : transfer.file_limit,
                        req_limit_overwrite  : cli.req_limit,
                        download_concurrency_overwrite : download.download_single_file_concurrency_limit,
                        quiet: cli.quiet,
                        verbose : cli.verbose,
                        stats : transfer.stats,
                        use_unsafe_http : cli.use_unsafe_http,
                        open_file_limit_overwrite : transfer.open_file_limit,
                        bucket : bucket.clone(),
                        local_path: local_path.clone(),
                        prefix : String::new() //TODO Jon: add support for prefix
                    }))
                }

                _ => {
                    Err(CliError::InvalidCommand("Invalid source/destination combination. Use \"snowcopy cp <src> <dest>\", where <src> and <dest> are either a local folder or a profile:bucket.".to_string()))
                }
            }
        }
    }
}

fn check_http_safeguard(endpoint: &EndpointConfig, use_unsafe_http: bool) -> Result<(), CliError> {
    if !use_unsafe_http {
        if let util_layer::url_parser::S3EndpointType::HTTP = util_layer::url_parser::validate_s3_endpoint(&endpoint.s3_url) {
            return Err(CliError::UnsafeHttp(endpoint.s3_url.clone()));
        }
    }
    Ok(())
}

pub fn gather_credentials(profile_name: &str, config_path: Option<&std::path::PathBuf>) -> Result<EndpointConfig, CliError> {
    let app_config = config::load_config(config_path.cloned()).map_err(|e| CliError::ConfigError(e.to_string()))?;

    let matches: Vec<_> = app_config
        .endpoints
        .iter()
        .filter(|e| e.name == profile_name)
        .collect();

    if matches.is_empty() {
        return Err(CliError::ProfileNotFound(profile_name.to_string()));
    }

    if matches.len() > 1 {
        return Err(CliError::ProfileNotUnique(profile_name.to_string()));
    }

    if let Some(endpoint_config) = &matches[0].config {
        return Ok(endpoint_config.clone());
    }

    Err(CliError::ProfileNoConfig(profile_name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket_and_path_parser::ParsedInputPath;

    const PROFILE: &str = "lumi";
    const HTTP_PROFILE: &str = "http-profile";
    const NOT_EXISTING_PROFILE: &str = "I-Dont-Exist";
    struct MockProvider;
    impl CredentialProvider for MockProvider {
        fn gather(&self, profile_name: &str, _config_path: Option<&PathBuf>) -> Result<EndpointConfig, CliError> {
            if profile_name == PROFILE {
                Ok(EndpointConfig {
                    s3_url: "https://lumidata.eu".to_string(),
                    ..Default::default()
                })
            } else if profile_name == HTTP_PROFILE {
                Ok(EndpointConfig {
                    s3_url: "http://localhost:9000".to_string(),
                    ..Default::default()
                })
            } else {
                Err(CliError::ProfileNotFound(profile_name.to_string()))
            }
        }
    }

    #[test]
    fn parses_ls_bucket() {
        let cli = Cli::try_parse_from([
            "snowcopy",
            "ls",
            &format!("{PROFILE}:mybucket"),
        ])
        .unwrap();

        match cli.command {
            Commands::Ls { input_path } => {
                match input_path {
                    ParsedInputPath::S3Bucket { profile, bucket, .. } => {
                        assert_eq!(profile, PROFILE);
                        assert_eq!(bucket, "mybucket");
                    }
                    _ => panic!("Expected S3Bucket"),
                }
            }
            _ => panic!("wrong command"),
        }
    }

    #[test]
    fn ls_bucket_creates_list_config() {
        let cli = Cli::try_parse_from([
            "snowcopy",
            "--req-limit", "20",
            "ls",
            &format!("{PROFILE}:data"),
        ]).unwrap();

        let provider = MockProvider;
        let result = build_config(cli, &provider).unwrap();

        match result {
            SnowConfig::List(cfg) => {
                assert_eq!(cfg.bucket, Some("data".to_string()));
                assert_eq!(cfg.req_limit, Some(20));
            }
            _ => panic!("expected List config"),
        }
    }

    #[test]
    fn rejects_http_endpoint() {
        let cli = Cli::try_parse_from([
            "snowcopy",
            "ls",
            &format!("{HTTP_PROFILE}:data"),
        ]).unwrap();

        let provider = MockProvider;
        let result = build_config(cli, &provider);

        assert!(matches!(result, Err(CliError::UnsafeHttp(_))));
    }

    #[test]
    fn accepts_http_endpoint_with_flag() {
        let cli = Cli::try_parse_from([
            "snowcopy",
            "--use-unsafe-http",
            "ls",
            &format!("{HTTP_PROFILE}:data"),
        ]).unwrap();

        let provider = MockProvider;
        let result = build_config(cli, &provider).unwrap();
        assert!(matches!(result, SnowConfig::List(_)));
    }

    #[test]
    fn rm_rejects_key() {
        let cli = Cli::try_parse_from([
            "snowcopy",
            "rm",
            &format!("{PROFILE}:bucket/key")
        ]).unwrap();

        let provider = MockProvider;
        let result = build_config(cli, &provider);
        assert!(matches!(result, Err(CliError::RmNotSupportedForKey)));
    }

    #[test]
    fn cp_local_to_s3() {
        let cli = Cli::try_parse_from([
            "snowcopy",
            "cp",
            "local_path",
            &format!("{PROFILE}:bucket"),
        ]).unwrap();

        let provider = MockProvider;
        let result = build_config(cli, &provider).unwrap();

        match result {
            SnowConfig::Upload(cfg) => {
                assert_eq!(cfg.bucket, "bucket");
                assert_eq!(cfg.local_path, PathBuf::from("local_path"));
            }
            _ => panic!("expected Upload config"),
        }
    }
}
