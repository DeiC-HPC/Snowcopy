mod common;

use crate::common::test_constants::DEFAULT_S3_PROFILE;
use crate::common::test_environment::TestEnv;
use common::test_environment::get_default_test_env;
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;
use util_layer::constants::CHUNK_SIZE;

#[tokio::test]
async fn test_relative_path_upload_input_should_work() {
    let env = get_default_test_env().await;

    let files_map = HashMap::from([
        ("small-file.txt", 1),
        ("large file 2", (CHUNK_SIZE as usize / 1024) * 2 + 1000),
        ("subfolder/small-file-inner.txt", 1),
        ("subfolder/large file 3", (CHUNK_SIZE as usize / 1024) * 2 + 1000),
        ("subfolder/sub/sub/sub/small-file.txt", 1),
        ("subfolder/sub/sub/sub/large file", (CHUNK_SIZE as usize / 1024) * 2 + 1000),
    ]);
    test_expected_amount_of_files_uploaded("/subfolder", ".", 4, &env, &files_map);
    test_expected_amount_of_files_uploaded("/subfolder", "..", 6, &env, &files_map);
    test_expected_amount_of_files_uploaded("/subfolder", "large file 3", 1, &env, &files_map);
    test_expected_amount_of_files_uploaded("/subfolder", "../small-file.txt", 1, &env, &files_map);

    test_expected_amount_of_files_uploaded("/subfolder/sub", ".", 2, &env, &files_map);
    test_expected_amount_of_files_uploaded("/subfolder/sub", "..", 4, &env, &files_map);
    test_expected_amount_of_files_uploaded("/subfolder/sub", "../..", 6, &env, &files_map);
    test_expected_amount_of_files_uploaded("/subfolder/sub", "../../subfolder/sub/sub/sub/small-file.txt", 1, &env, &files_map);
    test_expected_amount_of_files_uploaded("/subfolder/sub", "../sub/sub/sub/large file", 1, &env, &files_map);
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

#[tokio::test]
async fn test_relative_path_download_input_should_work() {
    let env = get_default_test_env().await;

    let file = "small-file.txt";
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&HashMap::from([
        (file, 1)
    ]));
    env.snowcopy_upload_test_upload_folder();

    let work_path = format!("{}/workdir", env.test_file_handler.download_folder().to_str().unwrap());
    fs::create_dir_all(&work_path).await.unwrap();

    let target = format!("{}:{}", DEFAULT_S3_PROFILE, env.unique_bucket);
    let output = env.run_snowcopy_args_in_dir(["cp", target.as_str(), "some-folder"], work_path.as_str());
    assert!(output.is_success(), "Download failed: {}", output.get_combined_logs());
    fs::File::open(Path::new(&work_path).join("some-folder").join(file)).await.expect("File should be found");

    let output = env.run_snowcopy_args_in_dir(["cp", target.as_str(), "sub/sub folder/sub.txt"], work_path.as_str());
    assert!(output.is_success(), "Download failed: {}", output.get_combined_logs());
    fs::File::open(Path::new(&work_path).join("sub/sub folder/sub.txt").join(file)).await.expect("File should be found");

    let output = env.run_snowcopy_args_in_dir(["cp", target.as_str(), "../sub folder/folder.with.in.name"], work_path.as_str());
    assert!(output.is_success(), "Download failed: {}", output.get_combined_logs());
    fs::File::open(Path::new(&work_path).join("../sub folder/folder.with.in.name").join(file)).await.expect("File should be found");
}