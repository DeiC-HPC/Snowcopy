mod common;

use common::test_environment::get_default_test_env;
use std::collections::HashMap;
use aws_sdk_s3::primitives::ByteStream;
use util_layer::constants::CHUNK_SIZE;
use crate::common::test_constants::{S3_ACCESS_KEY, S3_SECRET_KEY};

#[tokio::test]
async fn test_upload_with_multipart() {
    let env = get_default_test_env().await;

    let large_file_name = "large-file-hitting-multipart-path.txt";
    let files = HashMap::from([
        ("small-file.txt", 1),
        (large_file_name, (CHUNK_SIZE as usize / 1024) * 2 + 1000),
    ]);
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&files);

    let output = env.snowcopy_upload_test_upload_folder();
    assert!(output.file_is_uploaded_with_multipart(large_file_name));

    env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
}

#[tokio::test]
async fn test_upload_with_multipart_trailing() {
    let env = get_default_test_env().await;

    let large_file_name = "large-file-hitting-multipart-path.txt";
    let files = HashMap::from([
        ("small-file.txt", 1),
        (large_file_name, (CHUNK_SIZE as usize / 1024) * 2 + 1000),
    ]);
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&files);

    let output = env.snowcopy_upload_test_upload_folder_trailing();
    assert!(output.file_is_uploaded_with_multipart(large_file_name));

    env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
}

#[tokio::test]
async fn test_upload_with_subfolders() {
    let env = get_default_test_env().await;

    let files_map = HashMap::from([
        ("small-file.txt", 1),
        ("large file", (CHUNK_SIZE as usize / 1024) * 2 + 1000),
        ("subfolder/small-file.txt", 1),
        ("subfolder/large file", (CHUNK_SIZE as usize / 1024) * 2 + 1000),
        ("subfolder/sub/sub/sub/small-file.txt", 1),
        ("subfolder/sub/sub/sub/large file", (CHUNK_SIZE as usize / 1024) * 2 + 1000),

        ("subfolder/su b/s ub/su b/small - file.txt", 1),
    ]);
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&files_map);

    env.snowcopy_upload_test_upload_folder();

    env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());

    let download_folder = env.test_file_handler.download_folder();
    for file_path in files_map.keys() {
        let full_path = download_folder.join(file_path);
        assert!(full_path.exists(), "File {:?} should exist in download folder", file_path);
    }
}

#[tokio::test]
async fn test_upload_and_download_with_empty_file() {
    let env = get_default_test_env().await;
    let files_map = HashMap::from([
        ("small-file.txt", 0),
    ]);

    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&files_map);
    let output = env.snowcopy_upload_test_upload_folder();
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"));

    let output = env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"));
}

#[tokio::test]
async fn test_upload_and_download_retries_on_failures_multipart() {
    let env = get_default_test_env().await;

    let file_name = "corruption_test.txt";
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&HashMap::from([
        (file_name, (CHUNK_SIZE as usize / 1024) * 2 + 1000), //divide 1024 for kb
    ]));

    //corrupt upload requests
    env.proxy_handler.corrupt_the_body_of_response_x_times("POST", file_name, 1).await.unwrap();
    env.proxy_handler.truncate_response_x_times("POST", file_name, 1, 50).await.unwrap();
    env.proxy_handler.cut_connection_x_times("PUT", file_name, 1).await.unwrap();

    let output = env.snowcopy_upload_test_upload_folder();
    assert!(output.file_is_uploaded_with_multipart(file_name));
    assert!(output.get_combined_logs().contains("Total retries: 3"));
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"));

    //corrupt download requests
    env.proxy_handler.truncate_response_x_times("GET", file_name, 1, 50).await.unwrap();
    env.proxy_handler.cut_connection_x_times("GET", file_name, 1).await.unwrap();

    let output = env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
    assert!(output.get_combined_logs().contains("Total retries: 2"));
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files")); //TODO JON: enable and fix LF-154
}

#[tokio::test]
async fn test_upload_and_download_retries_on_failures_small_file() {
    let env = get_default_test_env().await;

    let file_name = "corruption_test.txt";
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&HashMap::from([
        (file_name, 1)
    ]));

    //corrupt upload requests
    env.proxy_handler.cut_connection_x_times("PUT", file_name, 2).await.unwrap();

    let output = env.snowcopy_upload_test_upload_folder();
    assert!(output.get_combined_logs().contains("Total retries: 2")); //two connection cuts
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"));

    //corrupt download requests
    env.proxy_handler.truncate_response_x_times("GET", file_name, 1, 50).await.unwrap();
    env.proxy_handler.cut_connection_x_times("GET", file_name, 1).await.unwrap();

    let output = env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
    assert!(output.get_combined_logs().contains("Total retries: 2"));
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files")); //TODO JON: enable and fix LF-154
}

#[tokio::test]
async fn test_upload_and_download_retries_on_failures_multipart_trailing() {
    let env = get_default_test_env().await;

    let file_name = "corruption_test.txt";
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&HashMap::from([
        (file_name, (CHUNK_SIZE as usize / 1024) * 2 + 1000), //divide 1024 for kb
    ]));

    //corrupt upload requests
    env.proxy_handler.corrupt_the_body_of_response_x_times("POST", file_name, 1).await.unwrap();
    env.proxy_handler.truncate_response_x_times("POST", file_name, 1, 50).await.unwrap();
    env.proxy_handler.cut_connection_x_times("PUT", file_name, 1).await.unwrap();

    let output = env.snowcopy_upload_test_upload_folder_trailing();
    assert!(output.file_is_uploaded_with_multipart(file_name));
    assert!(output.get_combined_logs().contains("Total retries: 3"));
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"));

    //corrupt download requests
    env.proxy_handler.truncate_response_x_times("GET", file_name, 1, 50).await.unwrap();
    env.proxy_handler.cut_connection_x_times("GET", file_name, 1).await.unwrap();

    let output = env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
    assert!(output.get_combined_logs().contains("Total retries: 2"));
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files")); //TODO JON: enable and fix LF-154
}

#[tokio::test]
async fn test_upload_and_download_retries_on_failures_small_file_trailing() {
    let env = get_default_test_env().await;

    let file_name = "corruption_test.txt";
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&HashMap::from([
        (file_name, 1)
    ]));

    //corrupt upload requests
    env.proxy_handler.cut_connection_x_times("PUT", file_name, 2).await.unwrap();

    let output = env.snowcopy_upload_test_upload_folder_trailing();
    assert!(output.get_combined_logs().contains("Total retries: 2")); //two connection cuts
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"));

    //corrupt download requests
    env.proxy_handler.truncate_response_x_times("GET", file_name, 1, 50).await.unwrap();
    env.proxy_handler.cut_connection_x_times("GET", file_name, 1).await.unwrap();

    let output = env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
    assert!(output.get_combined_logs().contains("Total retries: 2"));
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files")); //TODO JON: enable and fix LF-154
}

#[tokio::test]
async fn test_upload_notices_change_in_already_uploaded_file() {
    let env = get_default_test_env().await;

    let file_path = env.test_file_handler.upload_folder().join("file.txt");
    std::fs::write(&file_path, "Initial file").expect("Failed to create changes.txt");

    let output = env.snowcopy_upload_test_upload_folder();
    let counts = env.proxy_handler.get_counts().await.unwrap();
    assert_eq!(counts.PUT, 2, "{}", output.get_combined_logs()); //bucket + file = 2 PUT
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"), "{}", output.get_combined_logs());

    let output = env.snowcopy_upload_test_upload_folder();
    let counts = env.proxy_handler.get_counts().await.unwrap();
    assert_eq!(counts.PUT, 2, "{}", output.get_combined_logs()); //File not uploaded as its identical
    assert!(output.get_combined_logs().contains("Transferred: 0/0 files"), "{}", output.get_combined_logs());
    assert!(output.get_combined_logs().contains("(1/1 files) | Existing 1 files"), "{}", output.get_combined_logs());

    std::fs::write(&file_path, "CHANGED file").expect("Failed to create changes.txt");
    let output = env.snowcopy_upload_test_upload_folder();
    let counts = env.proxy_handler.get_counts().await.unwrap();
    assert_eq!(counts.PUT, 3, "{}", output.get_combined_logs()); //File uploaded as its changed
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"), "{}", output.get_combined_logs());
    println!("{}", output.get_combined_logs());
}

#[tokio::test]
async fn test_download_notices_change_in_already_downloaded_file() {
    let env = get_default_test_env().await;

    let key = "object key";

    let s3_client = common::s3_test_client::create_aws_sdk_client(S3_ACCESS_KEY, S3_SECRET_KEY, format!("http://127.0.0.1:{}", env.minio_host_port).as_str(), "us-east-1").await;
    s3_client.create_bucket().bucket(&env.unique_bucket).send().await.unwrap();
    s3_client.put_object().bucket(&env.unique_bucket).key(key).body(ByteStream::from("Initial value".as_bytes().to_vec())).metadata("mtime", "1790671696.0000000").send().await.unwrap();

    let output = env.snowcopy_download_bucket_to_download_folder();
    let counts = env.proxy_handler.get_counts().await.unwrap();
    assert_eq!(counts.GET, 2, "{}", output.get_combined_logs()); //one to list objects, one to download the file
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"), "{}", output.get_combined_logs());

    let output = env.snowcopy_download_bucket_to_download_folder();
    let counts = env.proxy_handler.get_counts().await.unwrap();
    assert_eq!(counts.GET, 3, "{}", output.get_combined_logs()); //one to list objects
    assert!(output.get_combined_logs().contains("Transferred: 0/0 files"), "{}", output.get_combined_logs());
    assert!(output.get_combined_logs().contains("(1/1 files) | Existing 1 files"), "{}", output.get_combined_logs());

    s3_client.put_object().bucket(&env.unique_bucket).key(key).body(ByteStream::from("CHANGED VALUE".as_bytes().to_vec())).metadata("mtime", "1790671696.1111111").send().await.unwrap();

    let output = env.snowcopy_download_bucket_to_download_folder();
    let counts = env.proxy_handler.get_counts().await.unwrap();
    assert_eq!(counts.GET, 5, "{}", output.get_combined_logs()); //one to list objects, one to download the file
    assert!(output.get_combined_logs().contains("Transferred: 1/1 files"), "{}", output.get_combined_logs());
}