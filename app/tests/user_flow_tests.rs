use std::collections::HashMap;

mod common;
use common::test_environment::get_default_test_env;

#[tokio::test]
async fn test_long_user_flow() {
    let env = get_default_test_env().await;

    let files = HashMap::from([
        ("test_file_to_send_and_retrieve.txt", 1),
        ("some_folder/test_file_to_send_and_retrieve.txt", 1),
    ]);
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&files);

    env.snowcopy_upload_test_upload_folder();

    env.snowcopy_download_bucket_to_download_folder();
    assert!(env.test_file_handler.assert_upload_and_download_folders_match());

    //uploading the same set of files, should not transfer anything
    let output = env.snowcopy_upload_test_upload_folder();
    assert!(output.get_combined_logs().contains("Transferred: 0/0 files"));
    assert!(output.get_combined_logs().contains(format!("Existing {} files", files.len()).as_str()));

    //downloading the same set of files, should not transfer anything
    let output = env.snowcopy_download_bucket_to_download_folder();
    assert!(output.get_combined_logs().contains("Transferred: 0/0 files"));
    assert!(output.get_combined_logs().contains(format!("Existing {} files", files.len()).as_str()));

    //List
    let output = env.snowcopy_list_bucket();
    println!("Logs:\n{}\nlog end", output.get_combined_logs());
    files.iter()
        .for_each(|(file, _)| assert!(output.get_combined_logs().contains(file)));
    assert_eq!(output.get_combined_log_lines().len(), 3, "{}", output.get_combined_logs()); //2 + final new line

    //List all buckets
    let output = env.snowcopy_list_all_buckets_on_server();
    assert!(output.get_combined_logs().contains(env.unique_bucket.as_str()));

    //delete
    env.snowcopy_delete_bucket();

    //List after delete
    let output = env.snowcopy_list_all_buckets_on_server();
    assert!(! output.get_combined_logs().contains(env.unique_bucket.as_str()));
}
