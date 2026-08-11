use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::{Builder, Credentials, RequestChecksumCalculation, ResponseChecksumValidation};
use aws_sdk_s3::Client;

pub async fn create_aws_sdk_client(access_key: &str, secret_key: &str, url: &str, region: &str) -> Client {
    let region = Region::new(region.to_string());

    let creds = Credentials::new(
        access_key,
        secret_key,
        None,               // optional session token
        None,               // optional expiration
        "manual-credentials", // provider name (for debugging)
    );

    let shared_config = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .credentials_provider(creds)
        .region(region.clone())
        .endpoint_url(url)
        .load()
        .await;

    let s3_conf = Builder::from(&shared_config)
        .force_path_style(true)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .build();

    let client = Client::from_conf(s3_conf);
    client
}