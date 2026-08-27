use crate::common::proxy_handler::ProxyHandler;
use crate::common::test_constants::*;
use crate::common::test_file_handler::TestFileHandler;
use crate::common::test_snowcopy_runner::SnowcopyOutput;
use std::path::PathBuf;
use std::env;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

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
    ///Split command into arguments on whitespace
    pub fn run_snowcopy_command(&self, command: String) -> SnowcopyOutput {
        self.run_snowcopy_args_in_dir(command.split_whitespace(), ".")
    }

    /// This function allows arguments containing whitespace
    pub fn run_snowcopy_args_in_dir<I, S>(&self, args: I, current_dir: impl AsRef<std::path::Path>) -> SnowcopyOutput
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let output = assert_cmd::Command::cargo_bin("snowcopy")
            .unwrap()
            .current_dir(current_dir)
            .arg("--config")
            .arg(self.test_file_handler.config_path())
            .args(args)
            .arg("--use-unsafe-http")
            .arg("-v")
            .output()
            .expect("failed to execute snowcopy");

        SnowcopyOutput::new(output)
    }

    pub fn snowcopy_upload_test_upload_folder(&self) -> SnowcopyOutput {
        let upload = format!("cp {} {}:{}", self.test_file_handler.upload_folder().to_str().unwrap(), DEFAULT_S3_PROFILE, self.unique_bucket);
        let output = self.run_snowcopy_command(upload);
        assert!(output.is_success(), "Upload failed: {}", output.get_combined_logs());

        output
    }

    pub fn snowcopy_upload_test_upload_folder_trailing(&self) -> SnowcopyOutput {
        let upload = format!("cp {} {}:{} --trailing --trailing-small", self.test_file_handler.upload_folder().to_str().unwrap(), DEFAULT_S3_PROFILE, self.unique_bucket);
        let output = self.run_snowcopy_command(upload);
        assert!(output.is_success(), "Upload failed: {}", output.get_combined_logs());

        output
    }

    pub fn snowcopy_download_bucket_to_download_folder(&self) -> SnowcopyOutput {
        let download = format!("cp {}:{} {}", DEFAULT_S3_PROFILE, self.unique_bucket, self.test_file_handler.download_folder().to_str().unwrap());
        let output = self.run_snowcopy_command(download);
        assert!(output.is_success(), "Download command failed: {}", output.get_combined_logs());

        output
    }

    pub fn snowcopy_list_bucket(&self) -> SnowcopyOutput {
        let list = format!("ls {}:{}", DEFAULT_S3_PROFILE, self.unique_bucket);
        let output = self.run_snowcopy_command(list);
        assert!(output.is_success(), "List failed: {}", output.get_combined_logs());

        output
    }

    pub fn snowcopy_list_all_buckets_on_server(&self) -> SnowcopyOutput {
        let list = format!("ls {}:", DEFAULT_S3_PROFILE);
        let output = self.run_snowcopy_command(list);
        assert!(output.is_success(), "List failed: {}", output.get_combined_logs());

        output
    }
    pub fn snowcopy_delete_bucket(&self) -> SnowcopyOutput {
        let delete = format!("rm {}:{}", DEFAULT_S3_PROFILE, self.unique_bucket);
        let output = self.run_snowcopy_command(delete);
        assert!(output.is_success(), "Delete failed: {}", output.get_combined_logs());

        output
    }
    pub fn get_minio_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.minio_host_port)
    }

    pub fn get_mitmproxy_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.mitm_host_port)
    }
}

pub(crate) async fn get_default_test_env() -> TestEnv {
    let mino_name = format!("minio-real-{}", Uuid::new_v4());
    println!("Starting minio and mitmproxy in docker...");
    // 1. Start MinIO
    let minio_container = GenericImage::new("minio/minio", "latest")
        .with_exposed_port(S3_DOCKER_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("API: "))
        .with_env_var("MINIO_ROOT_USER", S3_ACCESS_KEY)
        .with_env_var("MINIO_ROOT_PASSWORD", S3_SECRET_KEY)
        .with_network(DOCKER_NETWORK_NAME)
        .with_container_name(mino_name.clone())
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

    let mitm_name = format!("mitm-proxy-{}", Uuid::new_v4());
    let mitm_container: ContainerAsync<GenericImage> = GenericImage::new("mitmproxy/mitmproxy", "latest")
        .with_exposed_port(PROXY_DOCKER_PORT.tcp())
        .with_exposed_port(CONTROL_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout(ADDON_STARTUP_LOG_STRING))
        .with_mount(Mount::bind_mount(
            script_path.to_str().unwrap(),
            "/home/mitmproxy/script.py",
        ))
        .with_network(DOCKER_NETWORK_NAME)
        .with_container_name(&mitm_name)
        .with_cmd([
            "mitmdump",
            "-s", "/home/mitmproxy/script.py",
            "--mode", format!("reverse:http://{}:9000", mino_name).as_str(),
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
    test_file_handler.write_config_file(S3_ACCESS_KEY, S3_SECRET_KEY, &proxy_endpoint, DEFAULT_S3_PROFILE);
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
