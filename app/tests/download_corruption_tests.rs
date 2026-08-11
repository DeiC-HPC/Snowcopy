use std::collections::HashMap;
use std::io::{Read, Write};

mod common;
use crate::common::test_constants::RETRY_ATTEMPT_MESSAGE;
use common::test_environment::get_default_test_env;
use util_layer::constants::CHUNK_SIZE;

#[tokio::test]
async fn test_download_corruption_retry() {
    let env = get_default_test_env().await;

    let test_file_name = "corruption_test.txt";
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&HashMap::from([
        (test_file_name, 1),
    ]));
    env.snowcopy_upload_test_upload_folder();

    env.proxy_handler.corrupt_the_body_of_request_x_times("GET", test_file_name,1).await.unwrap();

    let download_output = env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
    assert!(download_output.get_combined_logs().contains(RETRY_ATTEMPT_MESSAGE));
    //assert!(download_output.get_combined_logs().contains("Transferred: 1/1 files"));
}

#[tokio::test]
async fn test_download_corruption_retry_on_multipart_uploaded_file() {
    let env = get_default_test_env().await;

    let test_file_name = "corruption_test.txt";
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&HashMap::from([
        (test_file_name, (CHUNK_SIZE as usize / 1024) * 2 + 1000), //divide 1024 for kb
    ]));
    let output = env.snowcopy_upload_test_upload_folder();
    assert!(output.file_is_uploaded_with_multipart(test_file_name));

    env.proxy_handler.corrupt_the_body_of_request_x_times("GET", test_file_name,1).await.unwrap();

    let download_output = env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
    assert!(download_output.get_combined_logs().contains(RETRY_ATTEMPT_MESSAGE));
    //assert!(download_output.get_combined_logs().contains("Transferred: 1/1 files")); //TODO JON: enable and fix LF-154
}
