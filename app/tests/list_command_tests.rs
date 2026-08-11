mod common;

use std::collections::HashMap;
use common::test_environment::get_default_test_env;
use util_layer::error_codes::ErrorCode;
use crate::common::test_constants::{DEFAULT_S3_PROFILE, S3_ACCESS_KEY, S3_SECRET_KEY};

#[tokio::test]
async fn test_list_bucket_command() {
    let env = get_default_test_env().await;

    let files = HashMap::from([
        ("test file to list.txt", 1),
        ("some_folder/test file to list.txt", 1),
        ("some!folder/sdaas dsad asd _retrieve.txt", 1),
        ("some folder/some folder_and_retrieve.txt", 1),
        ("s/f/g/j/j/g", 1),
    ]);

    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&files);
    env.snowcopy_upload_test_upload_folder();

    let output = env.snowcopy_list_bucket();
    files.iter()
        .for_each(|(file, _)| assert!(output.get_combined_logs().contains(file)));
    assert_eq!(output.get_combined_log_lines().len(), files.len(), "{}", output.get_combined_logs());
}

#[tokio::test]
async fn test_list_all_buckets_command() {
    let env = get_default_test_env().await;

    let buckets : Vec<&str> = vec![
        "test-bucket-1",
        "test-bucket-2",
        "test-bucket-3"
    ];

    let s3_client = common::s3_test_client::create_aws_sdk_client(S3_ACCESS_KEY, S3_SECRET_KEY, format!("http://127.0.0.1:{}", env.minio_host_port).as_str(), "us-east-1").await;
    for bucket in &buckets {
        s3_client.create_bucket().bucket(*bucket).send().await.unwrap();
    }

    let output = env.snowcopy_list_all_buckets_on_server();
    buckets.iter()
        .for_each(|bucket| assert!(output.get_combined_logs().contains(bucket)));
    assert_eq!(output.get_combined_log_lines().len(), buckets.len(), "{}", output.get_combined_logs());

    //delete one bucket
    let output = env.run_snowcopy_command(format!("rm {}:{}", DEFAULT_S3_PROFILE, buckets[0]));
    assert!(output.is_success());

    //list now contains one less
    let output = env.snowcopy_list_all_buckets_on_server();
    assert_eq!(output.get_combined_log_lines().len(), buckets.len() - 1, "{}", output.get_combined_logs());
}


#[tokio::test]
async fn test_list_non_existing_bucket_should_result_in_fatal_error() {
    let env = get_default_test_env().await;

    let output = env.run_snowcopy_command(format!("ls {}:this-bucket-does-not-exist", DEFAULT_S3_PROFILE));
    assert!(output.is_failure(ErrorCode::RequestFailure));
}