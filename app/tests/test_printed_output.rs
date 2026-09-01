use crate::common::test_environment::get_default_test_env;
use std::collections::HashMap;
use util_layer::error_codes::ErrorCode::RequestFailure;
use crate::common::test_constants::DEFAULT_S3_PROFILE;

mod common;

#[tokio::test]
async fn test_error_message_is_printed_after_progress_bar() {
    let env = get_default_test_env().await;

    //setup fatal error from server
    let headers = HashMap::from([
        ("Content-Type".to_string(), "application/xml".to_string()),
        ("x-amz-request-id".to_string(), "test-request-id".to_string()),
        ("x-amz-id-2".to_string(), "test-host-id".to_string()),
    ]);

    let filename = "lol.txt";
    env.proxy_handler.fake_response_x_times(
        "PUT",
        filename,
        1,
        403,
        headers,
        r#"<Error><Code>403</Code><Message>Injected test error</Message></Error>"#.to_string(),
    ).await.unwrap();

    //Attempt upload to server
    env.test_file_handler.create_files_in_upload_folder_with_sizes_in_kb(&HashMap::from([
        (filename, 1),
    ]));
    let output = env.run_snowcopy_command(format!("cp {} {}:{} -v", env.test_file_handler.upload_folder().to_str().unwrap(), DEFAULT_S3_PROFILE, env.unique_bucket));
    assert!(output.is_failure(RequestFailure));
    let err_log = output.get_stderr();
    assert!(err_log.contains("FATAL ERROR"));
    assert!(err_log.contains(" Transfer: ")); //We have a progress bar
    assert!(err_log.find("========================") > err_log.rfind(" Transfer: ")); //last transfer before erro message
    assert!(err_log.find("FATAL ERROR") > err_log.rfind(" Transfer: "));
}
