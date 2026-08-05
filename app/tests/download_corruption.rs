use std::fs;
use std::io::{Read, Write};


#[tokio::test]
async fn test_download_corruption_retry() {
    let env = get_test_env().await;

    // Create a dummy file to upload
    let temp_dir = std::env::temp_dir();
    let test_file = "corruption_test.txt";
    let test_file_path = temp_dir.join(test_file);
    let test_content = "This file will be corrupted on the first download attempt but should succeed on retry.";
    fs::write(&test_file_path, test_content).unwrap();

    // Get original file metadata
    let original_metadata = fs::metadata(&test_file_path).unwrap();
    let uploaded_mtime = original_metadata.modified().unwrap();

    let bucket = "corruption-test-bucket";
    let bucket_spec = format!("corruptionTest:{}", bucket);

    println!("Uploading file through mitmproxy...");
    let upload_output = assert_cmd::Command::cargo_bin("snowcopy")
        .unwrap()
        .arg("--config")
        .arg(&env.config_path)
        .arg(&test_file_path)
        .arg(&bucket_spec)
        .arg("--use-unsafe-http")
        .arg("-v")
        .output()
        .expect("failed to execute upload");

    assert!(upload_output.status.success(), "Upload failed: {}", String::from_utf8_lossy(&upload_output.stderr));

    println!("Attempting download with corruption...");
    let download_dir = temp_dir.join("corruption_download");
    if download_dir.exists() {
        fs::remove_dir_all(&download_dir).unwrap();
    }
    fs::create_dir_all(&download_dir).unwrap();

    let download_output = assert_cmd::Command::cargo_bin("snowcopy")
        .unwrap()
        .arg("--config")
        .arg(&env.config_path)
        .arg(format!("{}/{}", bucket_spec, test_file))
        .arg(&download_dir)
        .arg("--use-unsafe-http")
        .arg("-v") // Verbose to see retry logs
        .output()
        .expect("failed to execute download");

    let stdout = String::from_utf8_lossy(&download_output.stdout);
    let stderr = String::from_utf8_lossy(&download_output.stderr);
    println!("Download stdout: {}", stdout);
    println!("Download stderr: {}", stderr);

    assert!(download_output.status.success(), "Download failed even after retry");

    let combined_output = format!("{}\n{}", stdout, stderr);

    // Verify that the retry happened
    assert!(
        combined_output.contains("Attempting again as it might have been corrupted during transfer"),
        "Retry log not found in output"
    );
    // Verify downloaded file exists and content matches
    let downloaded_file = download_dir.join(test_file);
    assert!(downloaded_file.exists(), "Downloaded file should exist");
    let downloaded_content = fs::read(&downloaded_file).unwrap();
    assert_eq!(downloaded_content, test_content.as_bytes(), "Downloaded content mismatch");

    // Verify mtime matches
    let downloaded_metadata = fs::metadata(&downloaded_file).unwrap();
    let downloaded_mtime = downloaded_metadata.modified().unwrap();
    
    assert_eq!(uploaded_mtime, downloaded_mtime, "mtime mismatch");

    // Cleanup
    let _ = fs::remove_file(&test_file_path);
    let _ = fs::remove_dir_all(&download_dir);
    let _ = fs::remove_file(&env.config_path);
}
