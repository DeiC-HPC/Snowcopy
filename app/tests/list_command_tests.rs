mod common;

use std::collections::HashMap;
use common::test_environment::get_default_test_env;

#[tokio::test]
async fn test_list_bucket_command() {
    let env = get_default_test_env().await;

    let files = HashMap::from([
        ("test_file_to_send_and_retrieve.txt", 1),
        ("some_folder/test_file_to_send_and_retrieve.txt", 1),
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