use std::{env, fs};
use std::path::PathBuf;

struct TestEnv {
    pub config_path: PathBuf,
    pub minio_host_port: u16,
    pub mitm_host_port: u16,
    pub minio: ContainerAsync<GenericImage>,
    pub mitm: ContainerAsync<GenericImage>,
}

async fn get_test_env() -> TestEnv {
    println!("Starting minio and mitmproxy in docker...");

    let network_name = "snowplow-test-net";

    // 1. Start MinIO
    let minio_container = GenericImage::new("minio/minio", "latest")
        .with_exposed_port(9000_u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr("API: "))
        .with_env_var("MINIO_ROOT_USER", "minioadmin")
        .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_network(network_name)
        .with_container_name("minio-real")
        .with_cmd([
            "server", "/data",
            "--console-address", ":9001",
        ])
        .start()
        .await
        .expect("Failed to start MinIO container");

    let minio_host_port = minio_container.get_host_port_ipv4(9000).await.unwrap();

    // 2. Start mitmproxy
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    let script_path = PathBuf::from(manifest_dir).join("tests").join("corrupt.py");

    let mitm_container: ContainerAsync<GenericImage> = GenericImage::new("mitmproxy/mitmproxy", "latest")
        .with_exposed_port(8080_u16.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Corruptor initialized"))
        .with_mount(Mount::bind_mount(
            script_path.to_str().unwrap(),
            "/home/mitmproxy/corrupt.py",
        ))
        .with_env_var("TARGET_KEY", "corruption_test.txt")
        .with_network(network_name)
        .with_container_name("mitm-proxy")
        .with_cmd([
            "mitmdump",
            "-s", "/home/mitmproxy/corrupt.py",
            "--mode", "reverse:http://minio-real:9000",
            "--set", "ssl_insecure=true",
            "--set", "termlog=true",
            "--set", "keep_host_header=true",
            "--listen-host", "0.0.0.0",
            "--listen-port", "8080"
        ])
        .start()
        .await
        .expect("Failed to start mitmproxy container");

    let mitm_host_port = mitm_container.get_host_port_ipv4(8080).await.unwrap();
    let proxy_endpoint = format!("http://127.0.0.1:{}", mitm_host_port);

    // Create a temporary config file for snowcopy
    let temp_dir = env::temp_dir();
    let config_path = temp_dir.join(format!("snowcopy_corruption_test_config_{}.json", mitm_host_port));
    let config_content = serde_json::json!({
        "version": 1,
        "global": {
            "s3_region": "us-east-1"
        },
        "endpoints": [
            {
                "name": "corruptionTest",
                "config": {
                    "s3_access_key": "minioadmin",
                    "s3_secret_key": "minioadmin",
                    "s3_url": proxy_endpoint,
                    "s3_region": "us-east-1"
                }
            }
        ]
    });

    fs::write(&config_path, serde_json::to_string_pretty(&config_content).unwrap()).unwrap();

    TestEnv {
        config_path,
        minio_host_port,
        mitm_host_port,
        minio: minio_container,
        mitm: mitm_container,
    }
}