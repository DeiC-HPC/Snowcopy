mod common;

use crate::common::test_constants::{DEFAULT_S3_PROFILE, S3_ACCESS_KEY, S3_SECRET_KEY};
use common::test_environment::get_default_test_env;
use util_layer::error_codes::ErrorCode;
use util_layer::snow_errors::S3Error;

#[tokio::test]
async fn test_incorrect_access_and_secret_keys_in_config() {
    let env = get_default_test_env().await;

    env.test_file_handler.write_config_file("WRONG ACCESS KEY", S3_SECRET_KEY, env.get_minio_url().as_str(), DEFAULT_S3_PROFILE);
    let output = env.run_snowcopy_command(format!("ls {}:{}", DEFAULT_S3_PROFILE, env.unique_bucket));
    println!("{:?}", output.get_combined_log_lines());
    assert!(output.get_combined_logs().contains(S3Error::InvalidAccessKeyId.to_string().as_str()));
    assert!(output.is_failure(ErrorCode::RequestFailure));

    env.test_file_handler.write_config_file(S3_ACCESS_KEY, "WRONG SECRET KEY", env.get_minio_url().as_str(), DEFAULT_S3_PROFILE);
    let output = env.run_snowcopy_command(format!("ls {}:{}", DEFAULT_S3_PROFILE, env.unique_bucket));
    assert!(output.get_combined_logs().contains(S3Error::SignatureDoesNotMatch.to_string().as_str()));
    assert!(output.is_failure(ErrorCode::RequestFailure))
}