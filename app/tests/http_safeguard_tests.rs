use assert_cmd::Command;
mod common;

use crate::common::test_file_handler::TestFileHandler;
use common::test_snowcopy_runner::SnowcopyOutput;
use util_layer::error_codes::FatalErrorCode::HttpUnsafeError;

#[test]
fn test_http_safeguard() {
    let file_handler = TestFileHandler::new();

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

    file_handler.write_config(&config_content);

    // Test failure without flag for HTTP
    let output = Command::cargo_bin("snowcopy")
        .unwrap()
        .arg("--config")
        .arg(file_handler.config_path())
        .arg("ls")
        .arg("http_profile:bucket")
        .output()
        .expect("failed to execute snowcopy");

    let output = SnowcopyOutput::new(output);
    assert!(output.is_failure(HttpUnsafeError), "{}", output.get_combined_logs());
    assert!(output.get_stderr().contains("uses the insecure HTTP protocol"));
    assert!(output.get_stderr().contains("use the --use-unsafe-http flag"));

    // Test success with flag for HTTP (it might fail later because example.com is not an S3 server, but it should pass the safeguard)
    let output = Command::cargo_bin("snowcopy")
        .unwrap()
        .arg("--config")
        .arg(file_handler.config_path())
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
        .arg(file_handler.config_path())
        .arg("ls")
        .arg("https_profile:bucket")
        .output()
        .expect("failed to execute snowcopy");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("uses the insecure HTTP protocol"));
}
