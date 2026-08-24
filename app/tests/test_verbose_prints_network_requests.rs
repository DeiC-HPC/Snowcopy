mod common;

use crate::common::test_environment::get_default_test_env;
use std::collections::HashMap;
use regex::Regex;
use util_layer::constants::CHUNK_SIZE;
use crate::common::proxy_handler::RequestCounts;
use crate::common::test_snowcopy_runner::SnowcopyOutput;

#[tokio::test]
async fn test_verbose_mode_prints_all_requests() {
    let env = get_default_test_env().await;

    let large_file_name = "large-file-hitting-multipart-path.txt";
    let files = HashMap::from([
        ("small-file.txt", 1),
        (large_file_name, (CHUNK_SIZE as usize / 1024) * 2 + 1000),
    ]);
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&files);

    //upload with bucket creation
    let output = env.snowcopy_upload_test_upload_folder();
    let log_reqs = count_request_methods(&output);
    let proxy_reqs = env.proxy_handler.get_counts().await.unwrap();
    println!("{}", output.get_combined_logs());
    assert_eq!(log_reqs, proxy_reqs);

    //upload where data already exists at target
    env.proxy_handler.reset_counts().await.unwrap();
    let output = env.snowcopy_upload_test_upload_folder();
    let log_reqs = count_request_methods(&output);
    let proxy_reqs = env.proxy_handler.get_counts().await.unwrap();
    println!("{}", output.get_combined_logs());
    assert_eq!(log_reqs, proxy_reqs);

    //download data
    env.proxy_handler.reset_counts().await.unwrap();
    let output = env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
    let log_reqs = count_request_methods(&output);
    let proxy_reqs = env.proxy_handler.get_counts().await.unwrap();
    println!("{}", output.get_combined_logs());
    assert_eq!(log_reqs, proxy_reqs);

    //download data where it already exists
    env.proxy_handler.reset_counts().await.unwrap();
    let output = env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());
    let log_reqs = count_request_methods(&output);
    let proxy_reqs = env.proxy_handler.get_counts().await.unwrap();
    println!("{}", output.get_combined_logs());
    assert_eq!(log_reqs, proxy_reqs);
}

#[tokio::test]
async fn test_verbose_mode_prints_all_requests_trailing() {
    let env = get_default_test_env().await;

    let large_file_name = "large-file-hitting-multipart-path.txt";
    let files = HashMap::from([
        ("small-file.txt", 1),
        (large_file_name, (CHUNK_SIZE as usize / 1024) * 2 + 1000),
    ]);
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&files);

    //upload with bucket creation
    let output = env.snowcopy_upload_test_upload_folder_trailing();
    let log_reqs = count_request_methods(&output);
    let proxy_reqs = env.proxy_handler.get_counts().await.unwrap();
    println!("{}", output.get_combined_logs());
    assert_eq!(log_reqs, proxy_reqs);

    //upload where data already exists at target
    env.proxy_handler.reset_counts().await.unwrap();
    let output = env.snowcopy_upload_test_upload_folder_trailing();
    let log_reqs = count_request_methods(&output);
    let proxy_reqs = env.proxy_handler.get_counts().await.unwrap();
    println!("{}", output.get_combined_logs());
    assert_eq!(log_reqs, proxy_reqs);
}

fn count_request_methods(output: &SnowcopyOutput) -> RequestCounts {
    let mut count = RequestCounts {
        GET: 0,
        POST: 0,
        PUT: 0,
        HEAD: 0,
        DELETE: 0,
    };

    let re = Regex::new(r"method(?:=|:\s+)(HEAD|GET|POST|PUT|DELETE)").unwrap();
    for line in output.get_combined_log_lines() {
        if let Some(caps) = re.captures(line) {
            match &caps[1] {
                "HEAD" => count.HEAD += 1,
                "GET"  => count.GET += 1,
                "POST" => count.POST += 1,
                "PUT"  => count.PUT += 1,
                "DELETE"  => count.DELETE += 1,
                _ => {}
            }
        }
    }

    count
}