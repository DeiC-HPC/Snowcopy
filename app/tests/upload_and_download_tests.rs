mod common;

use common::test_environment::get_default_test_env;
use std::collections::HashMap;
use util_layer::constants::CHUNK_SIZE;

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
