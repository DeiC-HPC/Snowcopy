use ring::{digest, hmac};
use std::collections::HashMap;
use url::Url;

pub struct S3Credentials {
    pub s3_secret_key: String,
    pub s3_access_key: String,
}

pub struct CredentialScope {
    pub(crate) timestamp: chrono::DateTime<chrono::Utc>,
    pub(crate) region: String,
    pub(crate) service: String,
}
const AMZ_CONTENT: &str = "x-amz-content-";

/// Builds SigV4 Authorization for S3 and also returns the computed signature.
/// signed_headers map MUST contain "host", "x-amz-content-sha256", "x-amz-date".
pub fn get_s3_auth_header_and_signature(
    method: &str,
    bucket: &str,
    credentials: &S3Credentials,
    credential_scope: &CredentialScope,
    key: &str,
    algoritm: &str,
    signed_headers: &HashMap<String, String>,
    url: &Url,
) -> (String, String) {
    let mut canonical_uri = String::with_capacity(bucket.len() + key.len() + 2);
    canonical_uri.push('/');
    canonical_uri.push_str(bucket);
    canonical_uri.push('/');
    canonical_uri.push_str(key);

    let canonical_query_string = url.query().unwrap_or("").replace('+', "%20");

    let mut headers: Vec<(&str, &str)> = Vec::with_capacity(signed_headers.len());
    for (k, v) in signed_headers {
        headers.push((k, v));
    }
    headers.sort_unstable_by(|a, b| a.0.cmp(b.0));

    let mut canonical_headers = String::with_capacity(256);
    let mut signed_headers_str = String::with_capacity(128);

    for (i, (k, v)) in headers.iter().enumerate() {
        if i > 0 {
            signed_headers_str.push(';');
        }
        signed_headers_str.push_str(k);

        canonical_headers.push_str(k);
        canonical_headers.push(':');
        canonical_headers.push_str(v.trim());
        canonical_headers.push('\n');
    }

    let payload_sha256 = signed_headers
        .get(&get_amz_content_header(algoritm))
        .expect("x-amz-content-XXX header matching algorithm is required");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        canonical_uri,
        canonical_query_string,
        canonical_headers,
        signed_headers_str,
        payload_sha256
    );

    let date_y_m_d = credential_scope.timestamp.format("%Y%m%d").to_string();
    let credential_scope_str = format!(
        "{}/{}/{}/aws4_request",
        date_y_m_d,
        credential_scope.region,
        credential_scope.service
    );

    let hex_sha256 = sha256_hex(canonical_request.as_bytes());

    let timestamp_iso = credential_scope.timestamp.format("%Y%m%dT%H%M%SZ").to_string();
    let string_to_sign = format!(
        "{}\n{}\n{}\n{}",
        algoritm,
        timestamp_iso,
        credential_scope_str,
        hex_sha256
    );

    let signing_key = s3_signing_key(&credentials.s3_secret_key, &date_y_m_d, credential_scope.region.as_str());
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    let auth_header = format!(
        "{} Credential={}/{}, SignedHeaders={}, Signature={}",
        algoritm, credentials.s3_access_key, credential_scope_str, signed_headers_str, signature
    );

    (auth_header, signature)
}

pub fn get_s3_auth_header(
    method: &str,
    bucket: &str,
    credentials: &S3Credentials,
    credential_scope: &CredentialScope,
    key: &str,
    algoritm: &str,
    signed_headers: &HashMap<String, String>,
    url: &Url,
) -> String {
    let (auth, _) =
        get_s3_auth_header_and_signature(method, bucket, credentials, credential_scope, key, algoritm, signed_headers, url);
    auth
}

pub(crate) fn get_amz_content_header(algorithm: &str) -> String {
    // Extract the hash algorithm from "AWS4-HMAC-SHA256" -> "sha256"
    let hash_algo = algorithm
        .strip_prefix("AWS4-HMAC-")
        .unwrap_or(algorithm)
        .to_lowercase();

    format!("{}{}", AMZ_CONTENT, hash_algo)
}

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    let actual_hash = digest::digest(&digest::SHA256, data);
    hex::encode(actual_hash)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let signature = hmac::sign(&s_key, data);
    let mut res = [0u8; 32];
    res.copy_from_slice(signature.as_ref());
    res
}

fn s3_signing_key(
    secret_key: &str,
    date_y_m_d: &str,
    region: &str,
) -> [u8; 32] {
    let mut k_secret = String::with_capacity(4 + secret_key.len());
    k_secret.push_str("AWS4");
    k_secret.push_str(secret_key);

    let k_date = hmac_sha256(k_secret.as_bytes(), date_y_m_d.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}