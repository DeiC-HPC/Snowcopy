use util_layer::error_codes::ErrorCode;

mod common;

use crate::common::test_constants::{DEFAULT_S3_PROFILE, S3_ACCESS_KEY, S3_SECRET_KEY};
use aws_sdk_s3::primitives::ByteStream;

use crate::common::test_environment::get_default_test_env;

#[cfg(windows)] //Linux is more liberal regarding filenames. There should be no legal s3 key that can mess up on linux
#[tokio::test]
async fn test_download_and_list_of_objects_with_illegal_chars() {
    let env = get_default_test_env().await;

    let legal_object = "legal object that should be downloaded even though another object with and illegal key is found";
    let invalid_keys : Vec<&str> = vec![
        "object with char not legal on local system <. It should still be listed",
        ">",
        "COM1", //Legacy windows way to connect to port through a file
        "con", //Legacy windows way to access reference the console through a file
        ":",
        "\"",
        "\\",
        "|",
        "?",
        "*",
    ];

    let s3_client = common::s3_test_client::create_aws_sdk_client(S3_ACCESS_KEY, S3_SECRET_KEY, format!("http://127.0.0.1:{}", env.minio_host_port).as_str(), "us-east-1").await;
    s3_client.create_bucket().bucket(&env.unique_bucket).send().await.unwrap();
    s3_client.put_object().bucket(&env.unique_bucket).key(legal_object).body(ByteStream::from("test".as_bytes().to_vec())).send().await.unwrap();
    for invalid_key in &invalid_keys {
        s3_client.put_object().bucket(&env.unique_bucket).key(*invalid_key).body(ByteStream::from("test".as_bytes().to_vec())).send().await.unwrap();
    }

    //List
    let output = env.snowcopy_list_bucket();
    assert!(output.get_combined_logs().contains(legal_object));
    for invalid_key in &invalid_keys {
        assert!(output.get_combined_logs().contains(*invalid_key));
    }

    //Download
    let download_bucket_to_download_folder = format!("cp {}:{} {} -v", DEFAULT_S3_PROFILE, env.unique_bucket, env.test_file_handler.download_folder().to_str().unwrap());
    let output = env.run_snowcopy_command(download_bucket_to_download_folder);

    assert!(env.test_file_handler.download_folder_contains_file(legal_object));
    assert_eq!(env.test_file_handler.download_folder_amount_of_files(), 1);
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"));
    assert!(output.get_combined_logs().contains("Skipping invalid S3 key"));
    assert_eq!(output.get_combined_log_lines().iter().filter(|line| line.contains("Skipping invalid S3 key")).count(), invalid_keys.len());
    assert_eq!(output.get_status().code().unwrap(), ErrorCode::SkippedInvalidKeysDownload.as_i32(), "Snowcopy should exit with non-zero exit code when downloading objects with illegal characters in their names");
}
