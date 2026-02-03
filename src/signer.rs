use aws_sdk_s3::types::CompletedPart;
use cyper::{Body, Client};
use url::Url;
use crate::{constants, Config};
use crate::s3example_error::S3ExampleError;

/// Complete a multipart upload by providing all parts.
async fn complete_multipart_upload_manual(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<CompletedPart>,
    config: &Config,
) -> std::result::Result<(), S3ExampleError> {
    // Build XML body
    let mut xml_parts = String::new();
    for part in parts {
        xml_parts.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            part.part_number().unwrap_or(0),
            part.e_tag().unwrap_or("")
        ));
    }

    let xml_body = format!(
        "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
        xml_parts
    );

    // Parse endpoint URL
    let endpoint_url = Url::parse(&config.s3_url).map_err(|e| {
        S3ExampleError::new(format!("Failed to parse S3 URL: {}", e))
    })?;

    let host = endpoint_url.host_str().unwrap();
    let uri = format!("/{}/{}", bucket, key);
    let query_string = format!("uploadId={}", upload_id);

    // Sign the request
    let (authorization, amz_date) = aws_v4_sign_with_s3v4(
        &config.s3_access_key,
        &config.s3_secret_key,
        constants::DEFAULT_REGION,
        "POST",
        host,
        &uri,
        &query_string,
    );

    // Build the full URL
    let full_url = format!("{}{}?{}", config.s3_url, uri, query_string);

    // Create and send the request
    let req = client
        .post(&full_url)
        .map_err(|e| S3ExampleError::new(format!("Failed to create request: {}", e)))?
        .header("Host", host).unwrap()
        .header("x-amz-date", &amz_date).unwrap()
        .header("Authorization", &authorization).unwrap()
        .header("Content-Type", "application/xml").unwrap()
        .body(Body::from(xml_body))
        .build();

    let resp = client.execute(req).await.map_err(|e| {
        S3ExampleError::new(format!("Failed to execute request: {}", e))
    })?;

    if resp.status().is_success() {
        println!("Completed multipart upload for key: {}", key);
        Ok(())
    } else {
        eprintln!("HTTP status code: {}", resp.status());
        Err(S3ExampleError::new(format!(
            "Failed to complete multipart upload for key {}: status {}",
            key, resp.status()
        )))
    }
}

/// Alternative AWS V4 signing using the s3v4 crate

pub fn aws_v4_sign_with_s3v4(
    access_key: &str,
    secret_key: &str,
    region: &str,
    method: &str,
    host: &str,
    canonical_uri: &str,
    canonical_querystring: &str,
) -> (String, String) {

    // Build URL
    let url = if canonical_querystring.is_empty() {
        format!("https://{}{}", host, canonical_uri)
    } else {
        format!("https://{}{}?{}", host, canonical_uri, canonical_querystring)
    };
    let url = Url::parse(&url).unwrap();

    // Create signature using s3v4::signature(...)
    let signature = s3v4::signature(
        &url,
        method,
        access_key,
        secret_key,
        region,
        "s3",
        "UNSIGNED-PAYLOAD",    // can also be "UNSIGNED-PAYLOAD"
    )
        .expect("Signing failed");

    // Extract the Authorization header created by s3v4
    let authorization = signature.auth_header.clone();

    (authorization, signature.date_time)
}