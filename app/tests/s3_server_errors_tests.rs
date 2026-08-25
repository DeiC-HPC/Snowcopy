mod common;

use crate::common::test_constants::DEFAULT_S3_PROFILE;
use common::test_environment::get_default_test_env;
use std::collections::HashMap;
use util_layer::constants::CHUNK_SIZE;
use util_layer::error_codes::ErrorCode;
use crate::common::test_environment::TestEnv;

#[tokio::test]
async fn retryable_s3_errors_are_retried_and_eventually_succeed_upload_and_download() {
    for scenario in all_file_scenarios() {
        for code in RETRYABLE_S3_CODES {
            let env = get_default_test_env().await;

            env.proxy_handler.fake_response_x_times(
                "PUT",
                scenario.name,
                1,
                status_for_s3_code(code),
                error_headers(),
                s3_error_xml(code),
            ).await.unwrap();

            let output = upload_file_for_scenario(&env, scenario);
            assert!(
                output.is_success(),
                "{code} should have been retried for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );
            assert!(
                output.get_combined_logs().contains("Transferred: 1/1 files"),
                "Upload transfer count missing for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );
            assert!(
                output.get_combined_logs().contains("Total retries: 1"),
                "Upload retry count missing for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );

            env.proxy_handler.fake_response_x_times(
                "GET",
                scenario.name,
                1,
                status_for_s3_code(code),
                error_headers(),
                s3_error_xml(code),
            ).await.unwrap();

            let output = env.snowcopy_download_bucket_to_download_folder();

            assert!(env.test_file_handler.assert_upload_and_download_folders_match());
            assert!(
                output.get_combined_logs().contains("Transferred: 1/1 files"),
                "Download transfer count missing for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );
            assert!(
                output.get_combined_logs().contains("Total retries: 1"),
                "Download retry count missing for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );
        }
    }
}

#[tokio::test]
async fn non_retryable_s3_errors_fail_without_retrying_upload() {
    for scenario in all_file_scenarios() {
        for code in NON_RETRYABLE_S3_CODES_TO_SMOKE_TEST {
            let env = get_default_test_env().await;

            env.proxy_handler.fake_response_x_times(
                "PUT",
                scenario.name,
                1,
                status_for_s3_code(code),
                error_headers(),
                s3_error_xml(code),
            ).await.unwrap();

            env.test_file_handler
                .create_files_in_upload_folder_with_sizes_in_kb(&HashMap::from([
                    (scenario.name, scenario.file_size_kb),
                ]));

            let output = run_upload_for_scenario_expect_failure(&env, scenario);

            assert!(
                output.is_failure(ErrorCode::RequestFailure),
                "{code} should have failed for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );
            assert!(
                output.get_combined_logs().contains("Transferred: 0/1 files"),
                "Unexpected upload transfer count for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );
            assert!(
                !output.get_combined_logs().contains("Total retries:"),
                "Non-retryable upload error should not retry for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );
        }
    }
}

#[tokio::test]
async fn non_retryable_s3_errors_fail_without_retrying_download() {
    for scenario in all_file_scenarios() {
        for code in NON_RETRYABLE_S3_CODES_TO_SMOKE_TEST {
            let env = get_default_test_env().await;

            upload_file_for_scenario(&env, scenario);

            env.proxy_handler.fake_response_x_times(
                "GET",
                scenario.name,
                1,
                status_for_s3_code(code),
                error_headers(),
                s3_error_xml(code),
            ).await.unwrap();

            let download = format!(
                "cp {}:{} {}",
                DEFAULT_S3_PROFILE,
                env.unique_bucket,
                env.test_file_handler.download_folder().to_str().unwrap()
            );
            let output = env.run_snowcopy_command(download);

            assert!(
                output.is_failure(ErrorCode::RequestFailure),
                "{code} should have failed for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );
            assert!(
                output.get_combined_logs().contains("Transferred: 0/1 files"),
                "Unexpected download transfer count for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );
            assert!(
                !output.get_combined_logs().contains("Total retries:"),
                "Non-retryable download error should not retry for scenario `{}`. Logs:\n{}",
                scenario.name,
                output.get_combined_logs()
            );
        }
    }
}

#[derive(Clone, Copy)]
enum UploadMode {
    Standard,
    Trailing,
}

#[derive(Clone, Copy)]
struct FileScenario {
    name: &'static str,
    file_size_kb: usize,
    upload_mode: UploadMode,
    expect_multipart_upload: bool,
}

impl FileScenario {
    fn small_standard() -> Self {
        Self {
            name: "small-standard-file.txt",
            file_size_kb: 1,
            upload_mode: UploadMode::Standard,
            expect_multipart_upload: false,
        }
    }

    fn multipart_standard() -> Self {
        Self {
            name: "multipart-standard-file.txt",
            file_size_kb: multipart_file_size_kb(),
            upload_mode: UploadMode::Standard,
            expect_multipart_upload: true,
        }
    }

    fn small_trailing() -> Self {
        Self {
            name: "small-trailing-file.txt",
            file_size_kb: 1,
            upload_mode: UploadMode::Trailing,
            expect_multipart_upload: false,
        }
    }

    fn multipart_trailing() -> Self {
        Self {
            name: "multipart-trailing-file.txt",
            file_size_kb: multipart_file_size_kb(),
            upload_mode: UploadMode::Trailing,
            expect_multipart_upload: true,
        }
    }
}

fn multipart_file_size_kb() -> usize {
    (CHUNK_SIZE as usize / 1024) * 2 + 1000
}

fn all_file_scenarios() -> [FileScenario; 4] {
    [
        FileScenario::small_standard(),
        FileScenario::multipart_standard(),
        FileScenario::small_trailing(),
        FileScenario::multipart_trailing(),
    ]
}

fn run_upload_for_scenario_expect_failure(
    env: &TestEnv,
    scenario: FileScenario,
) -> crate::common::test_snowcopy_runner::SnowcopyOutput {
    let mut args = vec![
        "cp".to_string(),
        env.test_file_handler.upload_folder().to_str().unwrap().to_string(),
        format!("{}:{}", DEFAULT_S3_PROFILE, env.unique_bucket),
    ];

    if matches!(scenario.upload_mode, UploadMode::Trailing) {
        args.push("--trailing".to_string());
        args.push("--trailing-small".to_string());
    }

    env.run_snowcopy_args_in_dir(args, ".")
}

fn upload_file_for_scenario(
    env: &TestEnv,
    scenario: FileScenario,
) -> crate::common::test_snowcopy_runner::SnowcopyOutput {
    env.test_file_handler
        .create_files_in_upload_folder_with_sizes_in_kb(&HashMap::from([
            (scenario.name, scenario.file_size_kb),
        ]));

    let output =  match scenario.upload_mode {
        UploadMode::Standard => env.snowcopy_upload_test_upload_folder(),
        UploadMode::Trailing => env.snowcopy_upload_test_upload_folder_trailing()
    };

    if scenario.expect_multipart_upload {
        assert!(
            output.file_is_uploaded_with_multipart(scenario.name),
            "Expected multipart upload for scenario `{}`. Logs:\n{}",
            scenario.name,
            output.get_combined_logs()
        );
    }

    output
}

fn error_headers() -> HashMap<String, String> {
    let headers = HashMap::from([
        ("Content-Type".to_string(), "application/xml".to_string()),
        ("x-amz-request-id".to_string(), "test-request-id".to_string()),
        ("x-amz-id-2".to_string(), "test-host-id".to_string()),
    ]);
    headers
}

const RETRYABLE_S3_CODES: &[&str] = &[
    "InternalError",
    "SlowDown",
    //"ServiceUnavailable",TODO JON: enable after LF-169
    //"RequestTimeout",
    //"OperationAborted",
    //"RequestTimeTooSkewed",
];

const NON_RETRYABLE_S3_CODES_TO_SMOKE_TEST: &[&str] = &[
    "AccessDenied",
    "NoSuchKey",
    //"InvalidAccessKeyId", TODO JON: enable after LF-169
    //"InvalidRequest",
    //"MalformedXML",
];


fn s3_error_xml(code: &str) -> String {
    format!(
        r#"<Error><Code>{code}</Code><Message>Injected test error</Message></Error>"#
    )
}

//https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html
fn status_for_s3_code(code: &str) -> u16 {
    match code {
        "InternalError" => 500,
        "ServiceUnavailable" => 503,
        "SlowDown" => 503,
        "RequestTimeout" => 400,
        "OperationAborted" => 409,
        "RequestTimeTooSkewed" => 403,
        "AccessDenied" => 403,
        "NoSuchKey" => 404,
        "InvalidAccessKeyId" => 403,
        "InvalidRequest" => 400,
        "MalformedXML" => 400,
        _ => !panic!("Unknown S3 error code: {}", code),
    }
}