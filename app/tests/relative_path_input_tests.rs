mod common;

use std::collections::HashMap;
use common::test_environment::get_default_test_env;
use util_layer::constants::CHUNK_SIZE;
use util_layer::error_codes::ErrorCode;
use crate::common::test_constants::DEFAULT_S3_PROFILE;
use crate::common::test_environment::TestEnv;

#[tokio::test]
async fn test_relative_path_input_should_work() {
    let env = get_default_test_env().await;

    let files_map = HashMap::from([
        ("small-file.txt", 1),
        ("large file 2", (CHUNK_SIZE as usize / 1024) * 2 + 1000),
        ("subfolder/small-file-inner.txt", 1),
        ("subfolder/large-file", (CHUNK_SIZE as usize / 1024) * 2 + 1000),
        ("subfolder/sub/sub/sub/small-file.txt", 1),
        ("subfolder/sub/sub/sub/large file", (CHUNK_SIZE as usize / 1024) * 2 + 1000),
    ]);
    test_expected_amount_of_files_uploaded("/subfolder", ".", 4, &env, &files_map);
    test_expected_amount_of_files_uploaded("/subfolder", "..", 6, &env, &files_map);
    test_expected_amount_of_files_uploaded("/subfolder", "large-file", 1, &env, &files_map);
    test_expected_amount_of_files_uploaded("/subfolder", "../small-file.txt", 1, &env, &files_map);

}

fn test_expected_amount_of_files_uploaded(execution_folder: &str, input_path: &str, expected_amount_of_uploads: i32, env: &TestEnv, files_map: &HashMap<&str, usize>) {
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(files_map);

    let target = format!("{}:{}", DEFAULT_S3_PROFILE, env.unique_bucket);
    let execution_dir = format!(
        "{}{}",
        env.test_file_handler.upload_folder().to_str().unwrap(),
        execution_folder
    );

    let output = env.run_snowcopy_args_in_dir(["cp", input_path, target.as_str()], execution_dir);
    assert!(output.is_success(), "Upload failed: {}", output.get_combined_logs());
    assert!(output
            .get_combined_logs()
            .contains(format!("Transferred: {}/{} files", expected_amount_of_uploads, expected_amount_of_uploads).as_str()),
        "Unexpected transfer count. Logs: {}",
        output.get_combined_logs()
    );
    env.snowcopy_delete_bucket();
}