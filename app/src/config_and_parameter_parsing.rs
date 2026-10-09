use crate::bucket_and_path_parser::ParsedInputPath;
use clap::{Parser, Subcommand};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use util_layer::{ConfigModeConfig, DownloadConfig, EndpointConfig, ListConfig, RemoveConfig, SnowConfig, UploadConfig};
use util_layer::error_codes::ErrorCode::{HttpUnsafeError, Unknown};

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
            _ => Unknown.as_i32(),
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
after_help = "First time users should run 'snowcopy config' to configure a S3 server.\n\nExamples:\n  upload:   snowcopy cp my-folder profile:bucket\n  upload:   snowcopy cp file.txt profile:bucket\n  download: snowcopy cp profile:bucket ."
)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, global = true, help = "Path to a config file to use instead of the default")]
    pub config: Option<PathBuf>,
}

#[derive(clap::Args, Debug)]
pub struct RequestOptions {
    #[arg(short = 'q', long, help = "Quiet/silent mode (no output)")]
    pub quiet: bool,

    #[arg(short = 'r', long, help = "Maximum number of concurrent requests")]
    pub req_limit: Option<usize>,
}

#[derive(clap::Args, Debug)]
pub struct SafetyOptions {
    #[arg(long, help = "Allow using insecure HTTP endpoints")]
    pub use_unsafe_http: bool,
}

#[derive(clap::Args, Debug)]
pub struct UploadOptions {
    #[arg(long, help = "Enable trailing header mode on large files")]
    pub trailing: bool,

    #[arg(long, help = "Size of chunks used for trailing header operations in kB", default_value = "1024")]
    pub trailing_chunk_size: u64,

    #[arg(long, help = "Enable trailing header mode on small files")]
    pub trailing_small: bool,
}

#[derive(clap::Args, Debug)]
pub struct DownloadOptions {
    #[arg(short = 'c', long, help = "Maximum number of concurrent downloads per file")]
    pub download_single_file_concurrency_limit: Option<usize>,
}

#[derive(clap::Args, Debug)]
pub struct TransferOptions {
    #[arg(long, help = "Disable MD5 checksum validation")]
    pub disable_md5_checksum: bool,

    #[arg(short = 'f', long, help = "Maximum number of files to do read/write operations on at once")]
    pub file_limit: Option<usize>,

    #[arg(long, help = "Maximum number of file descriptors used simultaneously")]
    pub open_file_limit: Option<usize>,

    #[arg(long, help = "Prints statistics about the transfer")]
    pub stats: bool,

    #[arg(short = 'v', long, help = "Verbose mode (print http headers)")]
    pub verbose: bool, //TODO JON: Eventually, this should be on all subcommands besides config
}


#[derive(Subcommand)]
enum Commands {
    #[command(about = "Open the TUI for profile configuration management")]
    Config,

    #[command(about = "Upload or download files")]
    Cp {
        #[arg(value_name = "Source", help = "Either a local folder/file or profile:bucket")]
        src: ParsedInputPath,
        #[arg(value_name = "Destination", help = "Either a local folder or profile:bucket")]
        dest: ParsedInputPath,

        #[command(flatten)]
        safety: SafetyOptions,

        #[command(flatten)]
        request: RequestOptions,

        #[command(flatten)]
        transfer: TransferOptions,

        #[command(flatten)]
        upload: UploadOptions,

        #[command(flatten)]
        download: DownloadOptions,
    },

    #[command(about = "List buckets and bucket contents")]
    Ls {
        #[arg(help = "Either profile: or profile:bucket (if bucket is empty, all buckets are listed)")]
        input_path: ParsedInputPath,

        #[command(flatten)]
        safety: SafetyOptions,
    },

    #[command(about = "Remove/delete a bucket along with ALL its contents")]
    Rm {
        #[arg(help = "profile:bucket")]
        input_path: ParsedInputPath,

        #[command(flatten)]
        safety: SafetyOptions,

        #[command(flatten)]
        request: RequestOptions,
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
            Ok(SnowConfig::Config(ConfigModeConfig { config_path : cli.config }))
        }

        Commands::Ls { input_path, safety } => {
            match input_path {
                ParsedInputPath::S3Server { profile } => {
                    let endpoint = provider.gather(&profile, cli.config.as_ref())?;
                    check_http_safeguard(&endpoint, safety.use_unsafe_http)?;

                    Ok(SnowConfig::List(ListConfig {
                        endpoint,
                        bucket : None,
                        verbose: false,
                        use_unsafe_http : safety.use_unsafe_http,
                    }))
                }

                ParsedInputPath::S3Bucket { profile, bucket, .. } => {
                    let endpoint = provider.gather(&profile, cli.config.as_ref())?;
                    check_http_safeguard(&endpoint, safety.use_unsafe_http)?;

                    Ok(SnowConfig::List(ListConfig {
                        endpoint,
                        bucket : Some(bucket),
                        verbose: false,
                        use_unsafe_http : safety.use_unsafe_http,
                    }))
                }

                _ => {
                    Err(CliError::InvalidCommand("ls requires either profile: or profile:bucket (if bucket is empty, all buckets are listed)".to_string()))
                }
            }
        }

        Commands::Rm { input_path, safety, request } => {
            match input_path {
                ParsedInputPath::S3Bucket { profile, bucket, key } => {
                    if key.is_some() {
                        return Err(CliError::RmNotSupportedForKey);
                    }

                    let endpoint = provider.gather(&profile, cli.config.as_ref())?;
                    check_http_safeguard(&endpoint, safety.use_unsafe_http)?;

                    Ok(SnowConfig::Remove(RemoveConfig {
                        endpoint,
                        bucket,
                        quiet: request.quiet,
                        verbose : false,
                        use_unsafe_http : safety.use_unsafe_http,
                        req_limit_overwrite : request.req_limit,
                    }))
                }

                _ => {
                    Err(CliError::InvalidCommand("rm requires profile:bucket".to_string()))
                }
            }
        }

        Commands::Cp { src, dest, safety, request, transfer, upload, download } => {
            match (&src, &dest) {
                (ParsedInputPath::Local( local_path), ParsedInputPath::S3Bucket { profile, bucket, .. }) => {
                    let endpoint = provider.gather(&profile, cli.config.as_ref())?;
                    check_http_safeguard(&endpoint, safety.use_unsafe_http)?;

                    Ok(SnowConfig::Upload(UploadConfig {
                        endpoint: endpoint.clone(),
                        md5_checksum_integrity_is_disabled: transfer.disable_md5_checksum,
                        file_limit_overwrite : transfer.file_limit,
                        req_limit_overwrite : request.req_limit,
                        quiet: request.quiet,
                        verbose : transfer.verbose,
                        stats : transfer.stats,
                        use_unsafe_http : safety.use_unsafe_http,
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
                    check_http_safeguard(&endpoint, safety.use_unsafe_http)?;

                    Ok(SnowConfig::Download(DownloadConfig {
                        endpoint: endpoint.clone(),
                        md5_checksum_integrity_is_disabled: transfer.disable_md5_checksum,
                        file_limit_overwrite : transfer.file_limit,
                        req_limit_overwrite  : request.req_limit,
                        download_concurrency_overwrite : download.download_single_file_concurrency_limit,
                        quiet: request.quiet,
                        verbose : transfer.verbose,
                        stats : transfer.stats,
                        use_unsafe_http : safety.use_unsafe_http,
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
    fn profile_exists_returns_endpoint() {
        let cli = Cli::try_parse_from([
            "snowcopy",
            "ls",
            &format!("{PROFILE}:"),
        ]).unwrap();

        let provider = MockProvider;
        let result = build_config(cli, &provider).unwrap();

        match result {
            SnowConfig::List(cfg) => {
                assert_eq!(cfg.endpoint.s3_url, "https://lumidata.eu");
            }
            _ => panic!("expected List config"),
        }
    }

    #[test]
    fn profile_missing_returns_error() {
        let cli = Cli::try_parse_from([
            "snowcopy",
            "ls",
            &format!("{NOT_EXISTING_PROFILE}:"),
        ]).unwrap();

        let provider = MockProvider;
        let result = build_config(cli, &provider);

        assert!(matches!(result, Err(CliError::ProfileNotFound(p)) if p == NOT_EXISTING_PROFILE));
    }

    #[test]
    fn http_safeguard_ls() {
        let provider = MockProvider;

        // HTTP endpoint + --use-unsafe-http absent => error
        let cli = Cli::try_parse_from(["snowcopy", "ls", &format!("{HTTP_PROFILE}:")]).unwrap();
        assert!(matches!(build_config(cli, &provider), Err(CliError::UnsafeHttp(_))));

        // HTTP endpoint + --use-unsafe-http present => success
        let cli = Cli::try_parse_from(["snowcopy", "ls", &format!("{HTTP_PROFILE}:"), "--use-unsafe-http"]).unwrap();
        assert!(build_config(cli, &provider).is_ok());

        // HTTPS endpoint => success
        let cli = Cli::try_parse_from(["snowcopy", "ls", &format!("{PROFILE}:")]).unwrap();
        assert!(build_config(cli, &provider).is_ok());
    }

    #[test]
    fn http_safeguard_rm() {
        let provider = MockProvider;

        // HTTP endpoint + --use-unsafe-http absent => error
        let cli = Cli::try_parse_from(["snowcopy", "rm", &format!("{HTTP_PROFILE}:bucket")]).unwrap();
        assert!(matches!(build_config(cli, &provider), Err(CliError::UnsafeHttp(_))));

        // HTTP endpoint + --use-unsafe-http present => success
        let cli = Cli::try_parse_from(["snowcopy", "rm", &format!("{HTTP_PROFILE}:bucket"), "--use-unsafe-http"]).unwrap();
        assert!(build_config(cli, &provider).is_ok());

        // HTTPS endpoint => success
        let cli = Cli::try_parse_from(["snowcopy", "rm", &format!("{PROFILE}:bucket")]).unwrap();
        assert!(build_config(cli, &provider).is_ok());
    }

    #[test]
    fn http_safeguard_cp_upload() {
        let provider = MockProvider;

        // HTTP endpoint + --use-unsafe-http absent => error
        let cli = Cli::try_parse_from(["snowcopy", "cp", "local", &format!("{HTTP_PROFILE}:bucket")]).unwrap();
        assert!(matches!(build_config(cli, &provider), Err(CliError::UnsafeHttp(_))));

        // HTTP endpoint + --use-unsafe-http present => success
        let cli = Cli::try_parse_from(["snowcopy", "cp", "local", &format!("{HTTP_PROFILE}:bucket"), "--use-unsafe-http"]).unwrap();
        assert!(build_config(cli, &provider).is_ok());

        // HTTPS endpoint => success
        let cli = Cli::try_parse_from(["snowcopy", "cp", "local", &format!("{PROFILE}:bucket")]).unwrap();
        assert!(build_config(cli, &provider).is_ok());
    }

    #[test]
    fn http_safeguard_cp_download() {
        let provider = MockProvider;

        // HTTP endpoint + --use-unsafe-http absent => error
        let cli = Cli::try_parse_from(["snowcopy", "cp", &format!("{HTTP_PROFILE}:bucket"), "local"]).unwrap();
        assert!(matches!(build_config(cli, &provider), Err(CliError::UnsafeHttp(_))));

        // HTTP endpoint + --use-unsafe-http present => success
        let cli = Cli::try_parse_from(["snowcopy", "cp", &format!("{HTTP_PROFILE}:bucket"), "local", "--use-unsafe-http"]).unwrap();
        assert!(build_config(cli, &provider).is_ok());

        // HTTPS endpoint => success
        let cli = Cli::try_parse_from(["snowcopy", "cp", &format!("{PROFILE}:bucket"), "local"]).unwrap();
        assert!(build_config(cli, &provider).is_ok());
    }

    #[test]
    fn validate_failures() {
        let provider = MockProvider;

        // rm profile:bucket/key
        let cli = Cli::try_parse_from(["snowcopy", "rm", &format!("{PROFILE}:bucket/key")]).unwrap();
        assert!(matches!(build_config(cli, &provider), Err(CliError::RmNotSupportedForKey)));

        // ls local_folder
        let cli = Cli::try_parse_from(["snowcopy", "ls", "local_folder"]).unwrap();
        assert!(matches!(build_config(cli, &provider), Err(CliError::InvalidCommand(_))));

        // cp local -> local
        let cli = Cli::try_parse_from(["snowcopy", "cp", "local1", "local2"]).unwrap();
        assert!(matches!(build_config(cli, &provider), Err(CliError::InvalidCommand(_))));

        // cp bucket -> bucket
        let cli = Cli::try_parse_from(["snowcopy", "cp", &format!("{PROFILE}:bucket1"), &format!("{PROFILE}:bucket2")]).unwrap();
        assert!(matches!(build_config(cli, &provider), Err(CliError::InvalidCommand(_))));
    }

    #[test]
    fn validate_success() {
        let provider = MockProvider;

        // snowcopy cp local bucket
        let cli = Cli::try_parse_from(["snowcopy", "cp", "local", &format!("{PROFILE}:bucket")]).unwrap();
        assert!(matches!(build_config(cli, &provider).unwrap(), SnowConfig::Upload(_)));

        // snowcopy cp bucket local
        let cli = Cli::try_parse_from(["snowcopy", "cp", &format!("{PROFILE}:bucket"), "local"]).unwrap();
        assert!(matches!(build_config(cli, &provider).unwrap(), SnowConfig::Download(_)));

        // snowcopy ls profile:
        let cli = Cli::try_parse_from(["snowcopy", "ls", &format!("{PROFILE}:")]).unwrap();
        let res = build_config(cli, &provider).unwrap();
        if let SnowConfig::List(cfg) = res {
            assert!(cfg.bucket.is_none());
        } else {
            panic!("Expected List config");
        }

        // snowcopy ls profile:bucket
        let cli = Cli::try_parse_from(["snowcopy", "ls", &format!("{PROFILE}:bucket")]).unwrap();
        let res = build_config(cli, &provider).unwrap();
        if let SnowConfig::List(cfg) = res {
            assert_eq!(cfg.bucket, Some("bucket".to_string()));
        } else {
            panic!("Expected List config");
        }

        // snowcopy rm profile:bucket
        let cli = Cli::try_parse_from(["snowcopy", "rm", &format!("{PROFILE}:bucket")]).unwrap();
        assert!(matches!(build_config(cli, &provider).unwrap(), SnowConfig::Remove(_)));
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
            Commands::Ls { input_path, .. } => {
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
            "ls",
            &format!("{PROFILE}:data"),
        ]).unwrap();

        let provider = MockProvider;
        let result = build_config(cli, &provider).unwrap();

        match result {
            SnowConfig::List(cfg) => {
                assert_eq!(cfg.bucket, Some("data".to_string()));
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
            "ls",
            &format!("{HTTP_PROFILE}:data"),
            "--use-unsafe-http",
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
