mod common;

use std::collections::HashMap;
use common::test_environment::get_default_test_env;
use crate::common::test_constants::{S3_ACCESS_KEY, S3_SECRET_KEY};

#[tokio::test]
async fn test_remove_bucket_command() {
    let env = get_default_test_env().await;

    let files = HashMap::from([
        ("test file to list.txt", 1),
        ("some_folder/test file to list.txt", 1),
    ]);

    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&files);
    env.snowcopy_upload_test_upload_folder();

    //Bucket exists
    let output = env.snowcopy_list_all_buckets_on_server();
    assert!(output.get_combined_logs().contains(env.unique_bucket.as_str()));

    env.snowcopy_delete_bucket();

    //Bucket is deleted
    let output = env.snowcopy_list_all_buckets_on_server();
    assert!(! output.get_combined_logs().contains(env.unique_bucket.as_str()));
}

#[tokio::test]
async fn test_remove_empty_bucket_command() {
    let env = get_default_test_env().await;

    let s3_client = common::s3_test_client::create_aws_sdk_client(S3_ACCESS_KEY, S3_SECRET_KEY, format!("http://127.0.0.1:{}", env.minio_host_port).as_str(), "us-east-1").await;
    s3_client.create_bucket().bucket(&env.unique_bucket).send().await.unwrap();

    //Bucket exists
    let output = env.snowcopy_list_all_buckets_on_server();
    assert!(output.get_combined_logs().contains(&env.unique_bucket.as_str()));

    env.snowcopy_delete_bucket();

    //Bucket is deleted
    let output = env.snowcopy_list_all_buckets_on_server();
    assert!(! output.get_combined_logs().contains(&env.unique_bucket.as_str()));
}