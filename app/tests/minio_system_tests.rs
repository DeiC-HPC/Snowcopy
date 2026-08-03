use assert_cmd::Command;
use std::env;
use std::fs;
use std::path::PathBuf;
use testcontainers::core::WaitFor;
use testcontainers::{
    core::IntoContainerPort,
    runners::AsyncRunner,
    ContainerAsync,
    GenericImage,
    ImageExt,
};

struct TestEnv {
    pub _endpoint: String,
    pub config_path: PathBuf,
    pub _container: ContainerAsync<GenericImage>,
}

async fn get_test_env() -> TestEnv {
    println!("Starting minio s3 server in docker for PR system test.");

    let container = GenericImage::new("minio/minio", "latest")
        .with_exposed_port(9000_u16.tcp())
        .with_exposed_port(9001_u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr("API: "))    //Wait for s3 service to be ready
        .with_env_var("MINIO_ROOT_USER", "minioadmin")
        .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_cmd([
                      "server","/data",             //starts the server
                      "--console-address", ":9001", //enables a UI that can be used while debugging the test.
                  ])
        .start()
        .await
        .expect("Failed to start MinIO container. Do you have docker running?");

    let api_port = container.get_host_port_ipv4(9000).await.unwrap();
    let ui_port = container.get_host_port_ipv4(9001).await.unwrap();

    let endpoint = format!("http://127.0.0.1:{}", api_port);
    println!("API: {}", endpoint);
    println!("UI : http://127.0.0.1:{ui_port} . User/pass: minioadmin/minioadmin");

    // Create a temporary config file for snowcopy
    let temp_dir = env::temp_dir();
    let config_path = temp_dir.join(format!("snowcopy_test_config_{}.json", api_port));

    let config_content = serde_json::json!({
            "version": 1,
            "global": {
                "s3_region": "us-east-1"
            },
            "endpoints": [
                {
                    "name": "systemTest",
                    "config": {
                        "s3_access_key": "minioadmin",
                        "s3_secret_key": "minioadmin",
                        "s3_url": endpoint,
                        "s3_region": "us-east-1",
                        "trailing_small": false
                    }
                }
            ]
        });

    fs::write(&config_path, serde_json::to_string_pretty(&config_content).unwrap()).unwrap();
    println!("Created test config at: {}", config_path.display());

    TestEnv {
        _endpoint: endpoint,
        config_path,
        _container: container,
    }
}

#[tokio::test]
async fn test_s3_client() { //TODO JON: after making a test or two more, default operations should be put in some helper functions. The docker start really fast, but 40 tests might still benefit from a shared container
    let env = get_test_env().await;

    // Create a dummy file to upload
    let temp_dir = std::env::temp_dir();
    let test_file = "textTest.txt";
    let test_file_path = temp_dir.join(test_file);
    let test_content = "This text file should be uploaded to S3 and downloaded back.";
    fs::write(&test_file_path, test_content).unwrap();

    // Get original file metadata
    let original_metadata = fs::metadata(&test_file_path).unwrap();
    let uploaded_mtime = original_metadata.modified().unwrap();

    let bucket = "systemTest:somenewbucket";
    let upload_output = Command::cargo_bin("snowcopy")
        .unwrap()
        .arg("--config")
        .arg(&env.config_path)
        .arg(&test_file_path)
        .arg(bucket)
        .arg("--use-unsafe-http")
        //.arg("-v")
        .output()
        .expect("failed to execute upload");

    println!("Upload stdout: {}", String::from_utf8_lossy(&upload_output.stdout));
    println!("Upload stderr: {}", String::from_utf8_lossy(&upload_output.stderr));
    assert!(upload_output.status.success(), "Upload failed");

    //List bucket:
    let list_output = Command::cargo_bin("snowcopy")
        .unwrap()
        .arg("ls")
        .arg("--config")
        .arg(&env.config_path)
        .arg(bucket)
        .arg("--use-unsafe-http")
        .output()
        .expect("failed to execute upload");

    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    println!("List stdout: {}", list_stdout);
    println!("List stderr: {}", String::from_utf8_lossy(&list_output.stderr));
    assert!(list_output.status.success(), "List failed");
    assert_eq!(list_stdout.trim(),
        test_file,
        "List output should equal test_file name"
    );

    let download_folder = "folder";
    let download_path = temp_dir.join(download_folder);
    let download_output = Command::cargo_bin("snowcopy")
        .unwrap()
        .arg("--config")
        .arg(&env.config_path)
        .arg(format!("{}/{}", bucket, test_file))
        .arg(&download_path)
        .arg("--use-unsafe-http")
        //.arg("-v")
        .output()
        .expect("failed to execute download");

    println!("Download stdout: {}", String::from_utf8_lossy(&download_output.stdout));
    println!("Download stderr: {}", String::from_utf8_lossy(&download_output.stderr));
    assert!(download_output.status.success(), "Download failed");

    // Verify downloaded file exists
    let downloaded_file = download_path.join(test_file);
    assert!(downloaded_file.exists(), "Downloaded file should exist at {}", downloaded_file.display());

    // Verify contents match
    let downloaded_content = fs::read_to_string(&downloaded_file).unwrap();
    assert_eq!(downloaded_content, test_content, "Downloaded file content should match original");

    // Verify mtime matches exactly
    let downloaded_metadata = fs::metadata(&downloaded_file).unwrap();
    let downloaded_mtime = downloaded_metadata.modified().unwrap();

    assert_eq!(
        uploaded_mtime,
        downloaded_mtime,
        "Downloaded file mtime should exactly match the uploaded mtime"
    );

}
