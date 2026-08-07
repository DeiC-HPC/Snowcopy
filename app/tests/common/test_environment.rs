use crate::common::test_constants::*;
use std::path::{Path, PathBuf};
use std::{env, fs};
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;
use crate::common::proxy_handler::ProxyHandler;
use crate::common::test_file_handler::TestFileHandler;
use crate::common::test_snowcopy_runner::SnowcopyRunnerOutput;

#[allow(dead_code)]
pub struct TestEnv {
    pub minio_host_port: u16,
    pub mitm_host_port: u16,
    pub minio: ContainerAsync<GenericImage>,
    pub mitm: ContainerAsync<GenericImage>,
    pub test_file_handler: TestFileHandler,
    pub unique_bucket: String,
    pub proxy_handler: ProxyHandler,
}

impl TestEnv {
    pub fn run_snowcopy_command(&self, command: String) -> SnowcopyRunnerOutput {
        let output = assert_cmd::Command::cargo_bin("snowcopy")
            .unwrap()
            .arg("--config")
            .arg(self.test_file_handler.config_path())
            .arg("--use-unsafe-http")
            .arg("-v")
            .args(command.split_whitespace())
            .output()
            .expect("failed to execute snowcopy");

        SnowcopyRunnerOutput::new(output)
    }

    pub fn snowcopy_upload_test_upload_folder(&self) -> SnowcopyRunnerOutput {
        let upload = format!("{} {}:{}", self.test_file_handler.upload_folder().to_str().unwrap(), DEFAULT_S3_PROFILE, self.unique_bucket);
        let output = self.run_snowcopy_command(upload);
        assert!(output.is_success(), "Upload failed: {}", output.get_combined_logs());

        output
    }

    pub fn snowcopy_download_bucket_to_download_folder(&self) -> SnowcopyRunnerOutput {
        let download = format!("{}:{} {}", DEFAULT_S3_PROFILE, self.unique_bucket, self.test_file_handler.download_folder().to_str().unwrap());
        let output = self.run_snowcopy_command(download);
        assert!(output.is_success(), "Upload failed: {}", output.get_combined_logs());

        output
    }
}

pub(crate) async fn get_default_test_env() -> TestEnv {
    println!("Starting minio and mitmproxy in docker...");
    // 1. Start MinIO
    let minio_container = GenericImage::new("minio/minio", "latest")
        .with_exposed_port(S3_DOCKER_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("API: "))
        .with_env_var("MINIO_ROOT_USER", S3_ACCESS_KEY)
        .with_env_var("MINIO_ROOT_PASSWORD", S3_SECRET_KEY)
        .with_network(DOCKER_NETWORK_NAME)
        .with_container_name("minio-real")
        .with_cmd([
            "server", "/data",
            "--console-address", ":9001",
        ])
        .start()
        .await
        .expect("Failed to start MinIO container");

    let minio_host_port = minio_container.get_host_port_ipv4(S3_DOCKER_PORT).await.unwrap();

    // 2. Start mitmproxy
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    let script_path = PathBuf::from(manifest_dir).join("tests").join("common").join("mitm_scripts").join("proxy_addon.py");

    let mitm_container: ContainerAsync<GenericImage> = GenericImage::new("mitmproxy/mitmproxy", "latest")
        .with_exposed_port(PROXY_DOCKER_PORT.tcp())
        .with_exposed_port(CONTROL_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout(ADDON_STARTUP_LOG_STRING))
        .with_mount(Mount::bind_mount(
            script_path.to_str().unwrap(),
            "/home/mitmproxy/script.py",
        ))
        .with_network(DOCKER_NETWORK_NAME)
        .with_container_name("mitm-proxy")
        .with_cmd([
            "mitmdump",
            "-s", "/home/mitmproxy/script.py",
            "--mode", "reverse:http://minio-real:9000",
            "--set", "ssl_insecure=true",
            "--set", "termlog=true",
            "--set", "keep_host_header=true",
            "--listen-host", "0.0.0.0",
            "--listen-port", String::from(PROXY_DOCKER_PORT.to_string()).as_str()
        ])
        .start()
        .await
        .expect("Failed to start mitmproxy container");

    let mitm_host_port = mitm_container.get_host_port_ipv4(PROXY_DOCKER_PORT).await.unwrap();
    let proxy_endpoint = format!("http://127.0.0.1:{}", mitm_host_port);

    let mitm_handler_port = mitm_container.get_host_port_ipv4(CONTROL_PORT).await.unwrap();
    let proxy_handler = ProxyHandler::new(format!("http://127.0.0.1:{}", mitm_handler_port));

    let test_file_handler = TestFileHandler::new();
    write_config_file(test_file_handler.config_path(), S3_ACCESS_KEY, S3_SECRET_KEY, &proxy_endpoint, DEFAULT_S3_PROFILE);
    let unique_bucket = format!("test-bucket-{}", Uuid::new_v4());

    TestEnv {
        minio_host_port,
        mitm_host_port,
        minio: minio_container,
        mitm: mitm_container,
        test_file_handler,
        unique_bucket,
        proxy_handler,
    }
}

fn write_config_file(config_path: &Path, s3_access_key: &str, s3_secret_key: &str, s3_url: &str, name : &str) {
    let config_content = serde_json::json!({
        "version": 1,
        "global": {
            "s3_region": "us-east-1"
        },
        "endpoints": [
            {
                "name": name,
                "config": {
                    "s3_access_key": s3_access_key,
                    "s3_secret_key": s3_secret_key,
                    "s3_url": s3_url,
                    "s3_region": "us-east-1"
                }
            }
        ]
    });

    fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    fs::write(config_path, serde_json::to_string_pretty(&config_content).unwrap()).unwrap();
}