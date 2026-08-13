mod common;

use std::collections::HashMap;
use aws_sdk_s3::primitives::ByteStream;
use common::test_environment::get_default_test_env;
use util_layer::constants::{CHUNK_SIZE, DOWNLOAD_MULTIPART_THRESHOLD};
use crate::common::test_constants::{S3_ACCESS_KEY, S3_SECRET_KEY};

#[tokio::test]
async fn test_download_of_files_uploaded_by_other_tools() {
    let env = get_default_test_env().await;

    let files_uploaded_by_snowcopy_with_metadata_for_integrity_checking = HashMap::from([
        ("small-file.txt", 1),
        ("large-file.txt", (CHUNK_SIZE as usize / 1024) * 2 + 1000),
    ]);
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&files_uploaded_by_snowcopy_with_metadata_for_integrity_checking);
    env.snowcopy_upload_test_upload_folder();

    let s3_client = common::s3_test_client::create_aws_sdk_client(S3_ACCESS_KEY, S3_SECRET_KEY, env.get_minio_url().as_str(), "us-east-1").await;
    s3_client.put_object().bucket(&env.unique_bucket).key("object with no metadata.txt").body(ByteStream::from("test".as_bytes().to_vec())).send().await.unwrap();
    s3_client.put_object().bucket(&env.unique_bucket).key("large multipart downloaded object with no metadata.txt").body(ByteStream::from(vec![0; (DOWNLOAD_MULTIPART_THRESHOLD as usize / 1024) * 2 + 1000])).send().await.unwrap();

    let output = env.snowcopy_download_bucket_to_download_folder();
    assert!(output.get_combined_logs().contains("Transferred: 4/4 files"));
    assert!(output.get_combined_logs().contains("Total files integrity checked: 2/4"));
    assert_eq!(output.get_combined_log_lines().iter().filter(|line| line.contains("file integrity checks are reduced")).count(), 1);
}