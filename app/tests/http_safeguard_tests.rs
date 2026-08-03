use assert_cmd::Command;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_http_safeguard() {
    let temp_dir = tempdir().unwrap();
    let config_path = temp_dir.path().join("config.json");

    let config_content = serde_json::json!({
        "version": 1,
        "global": {
            "s3_region": "us-east-1"
        },
        "endpoints": [
            {
                "name": "http_profile",
                "config": {
                    "s3_access_key": "key",
                    "s3_secret_key": "secret",
                    "s3_url": "http://example.com",
                    "s3_region": "us-east-1"
                }
            },
            {
                "name": "https_profile",
                "config": {
                    "s3_access_key": "key",
                    "s3_secret_key": "secret",
                    "s3_url": "https://example.com",
                    "s3_region": "us-east-1"
                }
            }
        ]
    });

    fs::write(&config_path, serde_json::to_string_pretty(&config_content).unwrap()).unwrap();

    // Test failure without flag for HTTP
    let output = Command::cargo_bin("snowcopy")
        .unwrap()
        .arg("--config")
        .arg(&config_path)
        .arg("ls")
        .arg("http_profile:bucket")
        .output()
        .expect("failed to execute snowcopy");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("STDOUT: {}", stdout);
    println!("STDERR: {}", stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("uses the insecure HTTP protocol"));
    assert!(stderr.contains("use the --use-unsafe-http flag"));

    // Test success with flag for HTTP (it might fail later because example.com is not an S3 server, but it should pass the safeguard)
    let output = Command::cargo_bin("snowcopy")
        .unwrap()
        .arg("--config")
        .arg(&config_path)
        .arg("--use-unsafe-http")
        .arg("ls")
        .arg("http_profile:bucket")
        .output()
        .expect("failed to execute snowcopy");

    let stderr = String::from_utf8_lossy(&output.stderr);
    // It should not fail because of HTTP protocol now
    assert!(!stderr.contains("uses the insecure HTTP protocol"));

    // Test success without flag for HTTPS
    let output = Command::cargo_bin("snowcopy")
        .unwrap()
        .arg("--config")
        .arg(&config_path)
        .arg("ls")
        .arg("https_profile:bucket")
        .output()
        .expect("failed to execute snowcopy");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("uses the insecure HTTP protocol"));
}
