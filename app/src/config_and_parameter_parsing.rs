use clap::{Arg, ArgMatches, Command};
use util_layer::bucket_and_path_parser::PathPattern;
use util_layer::{ConfigModeConfig, DownloadConfig, EndpointConfig, ListConfig, Mode, RemoveConfig, SnowConfig, TransferMethod, UploadConfig};

pub async fn read_parameters_and_config_to_create_snow_config(matches: &ArgMatches) -> SnowConfig {
    let config_path = matches.get_one::<String>("config").map(std::path::PathBuf::from);

    let (mode, sub_matches) = match matches.subcommand() {
        Some(("config", m)) => (Mode::Config, Some(m)),
        Some(("rm", m)) => (Mode::Remove, Some(m)),
        Some(("ls", m)) => (Mode::List, Some(m)),

        _ => { //we are transferring files. Figure out if it's upload or download based on path patterns
            if matches.get_one::<String>("src").is_some() && matches.get_one::<String>("dest").is_some() {
                // Determine if it's upload or download based on path patterns
                let src_str = matches.get_one::<String>("src").unwrap();
                let dest_str = matches.get_one::<String>("dest").unwrap();
                let src = PathPattern::parse(src_str);
                let dest = PathPattern::parse(dest_str);

                match (&src, &dest) {
                    (PathPattern::S3 { .. }, PathPattern::Local(_)) => (Mode::Download, None),
                    (PathPattern::Local(_), PathPattern::S3 { .. }) => (Mode::Upload, None),
                    _ => {
                        eprintln!("Error: Invalid source/destination combination for upload/download.");
                        std::process::exit(1);
                    }
                }
            } else {
                eprintln!("Error: No valid command or src/dest provided.");
                std::process::exit(1);
            }
        }
    };

    let silent = matches.get_flag("silent");
    let req_limit_overwrite = matches.get_one::<usize>("req_limit").copied();

    match mode {
        Mode::Config => {
            SnowConfig::Config(ConfigModeConfig { silent, config_path })
        }
        Mode::List | Mode::Remove => {
            let sub_m = sub_matches.expect("subcommand matches required for ls/rm");
            let path_str = sub_m.get_one::<String>("path").expect("path is required for ls/rm");
            let pattern = PathPattern::parse(path_str);
            let (profile, bucket) = match pattern {
                PathPattern::S3 { profile, bucket, .. } => {
                    if let Err(e) = PathPattern::validate_bucket_name(&bucket) {
                        eprintln!("Error: Invalid bucket name '{}': {}", bucket, e);
                        std::process::exit(1);
                    }
                    (profile, bucket)
                }
                PathPattern::Local(_) => {
                    eprintln!("Error: ls and rm require an S3 path (profile:bucket/)");
                    std::process::exit(1);
                }
            };

            let endpoint = load_endpoint_and_stop_program_with_error_if_it_fails(&profile, config_path.as_ref()).await;

            if mode == Mode::List {
                SnowConfig::List(ListConfig {
                    endpoint,
                    bucket,
                    req_limit: req_limit_overwrite,
                })
            } else {
                SnowConfig::Remove(RemoveConfig {
                    endpoint,
                    bucket,
                    silent,
                    req_limit: req_limit_overwrite,
                })
            }
        }
        Mode::Upload | Mode::Download => {
            let transfer_method_str = matches.get_one::<String>("o").map(|s| s.as_str()).unwrap_or("o1");
            let file_limit_overwrite = matches.get_one::<usize>("file_limit").copied();
            let download_concurrency_overwrite = matches.get_one::<usize>("download_concurrency").copied();
            let stats = matches.get_flag("stats");
            let md5_checksum_integrity = matches.get_one::<bool>("md5_checksum_integrity").copied().unwrap_or(true);
            let trailing = matches.get_flag("trailing");
            let trailing_small = matches.get_flag("trailing-small");
            let trailing_chunk_size_val = matches.get_one::<usize>("trailing-chunk-size").copied().unwrap_or(1 * 1024 * 1024); //TODO JON: We should not have this value here.
            let open_file_limit_overwrite = matches.get_one::<usize>("open-file-limit").copied();
            let transfer_method = TransferMethod::from_str(transfer_method_str).expect(format!("Transfer method {} unknown", transfer_method_str).as_str());
            let prefix = "".to_string(); //TODO JON: We will add prefixing after release

            let src_str = matches.get_one::<String>("src").unwrap();
            let dest_str = matches.get_one::<String>("dest").unwrap();
            let src = PathPattern::parse(src_str);
            let dest = PathPattern::parse(dest_str);

            let (profile, bucket, local_path) = match mode {
                Mode::Download => {
                    if let (PathPattern::S3 { profile, bucket, .. }, PathPattern::Local(path)) = (src, dest) {
                        if let Err(e) = PathPattern::validate_bucket_name(&bucket) {
                            eprintln!("Error: Invalid bucket name '{}': {}", bucket, e);
                            std::process::exit(1);
                        }
                        (profile, bucket, path.to_path_buf())
                    } else { unreachable!() }
                }
                Mode::Upload => {
                    if let (PathPattern::Local(path), PathPattern::S3 { profile, bucket, .. }) = (src, dest) {
                        if let Err(e) = PathPattern::validate_bucket_name(&bucket) {
                            eprintln!("Error: Invalid bucket name '{}': {}", bucket, e);
                            std::process::exit(1);
                        }
                        (profile, bucket, path.to_path_buf())
                    } else { unreachable!() }
                }
                _ => unreachable!(),
            };

            let endpoint = load_endpoint_and_stop_program_with_error_if_it_fails(&profile, config_path.as_ref()).await;

            if mode == Mode::Upload {
                SnowConfig::Upload(UploadConfig {
                    endpoint,
                    md5_checksum_upload: md5_checksum_integrity,
                    transfer_method,
                    file_limit_overwrite,
                    req_limit_overwrite,
                    silent,
                    stats,
                    trailing_chunk_size: trailing_chunk_size_val as u64,
                    trailing,
                    trailing_small,
                    open_file_limit_overwrite,
                    bucket,
                    local_path,
                })
            } else {
                SnowConfig::Download(DownloadConfig {
                    endpoint,
                    md5_checksum_integrity,
                    transfer_method,
                    file_limit_overwrite,
                    req_limit_overwrite,
                    download_concurrency_overwrite,
                    silent,
                    stats,
                    open_file_limit_overwrite,
                    bucket,
                    local_path,
                    prefix
                })
            }
        }
    }
}

async fn load_endpoint_and_stop_program_with_error_if_it_fails(profile: &String, config_path: Option<&std::path::PathBuf>) -> EndpointConfig {
    let endpoint = match gather_credentials(&profile, config_path).await {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading configuration for profile '{}': {}\nRun \"snowcopy config\" to configure profiles", profile, e);
            std::process::exit(1);
        }
    };
    endpoint
}

pub async fn gather_credentials(profile_name: &str, config_path: Option<&std::path::PathBuf>) -> anyhow::Result<EndpointConfig> {
    let app_config = if let Some(path) = config_path {
        config::load_config_from_path(path.clone())?
    } else {
        config::load_config()?
    };

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

pub fn parse_arguments() -> ArgMatches {
    let matches = Command::new("snowcopy")
        .version("0.1.0-alpha.1")
        .about("Fast S3 copy tool - ALPHA, use with caution.")
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
            Arg::new("config")
                .long("config")
                .value_name("FILE")
                .help("Path to a config file to use instead of the default")
                .required(false)
                .num_args(1)
                .global(true),
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
            Arg::new("silent")
                .short('s')
                .long("silent")
                .help("Silent mode (no output)")
                .action(clap::ArgAction::SetTrue),
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
        .after_help("Examples:\n  up:   snowcopy my-folder profile:bucket\n  up:   snowcopy file.txt profile:bucket\n  down: snowcopy profile:bucket .")
        .get_matches();
    matches
}



