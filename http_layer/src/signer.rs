use openssl::sha::Sha256;
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
    let canonical_uri = format!("/{}/{}", bucket, key);
    let canonical_query_string = url.query().unwrap_or("").replace("+", "%20");

    let mut headers: Vec<(String, String)> = signed_headers
        .iter()
        .map(|(k, v)| (k.to_lowercase().trim().to_string(), v.trim().to_string()))
        .collect();
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let mut canonical_headers = String::new();
    let mut signed_keys = Vec::new();
    for (k, v) in &headers {
        canonical_headers.push_str(k);
        canonical_headers.push(':');
        canonical_headers.push_str(v);
        canonical_headers.push('\n');
        signed_keys.push(k.as_str());
    }
    let signed_headers_str = signed_keys.join(";");

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

    let credential_scope_str = s3_credential_scope_format(credential_scope);
    //eprintln!("Credential Scope: {}", credential_scope_str);

    let mut sha256 = Sha256::new();
    sha256.update(canonical_request.as_bytes());
    let hex_sha256 = hex::encode(sha256.finish());

    let string_to_sign = format!(
        "{}\n{}\n{}\n{}",
        algoritm,
        credential_scope.timestamp.format("%Y%m%dT%H%M%SZ"),
        credential_scope_str,
        hex_sha256
    );
    //eprintln!("String to sign: {}", string_to_sign);


    let signing_key = s3_signing_key(&credentials.s3_secret_key, credential_scope.timestamp, credential_scope.region.as_str());
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
    let mut hash = Sha256::new();
    Sha256::update(&mut hash, data);
    hex::encode(hash.finish())
}

// fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
//     let mut mac = Sha256::new();
//     mac.update(key);
//     mac.update(data);
//     mac.finish().to_vec()
// }
use hmac::{Hmac, Mac};
use sha2::Sha256 as Sha256_2;

type HmacSha256 = Hmac<Sha256_2>;
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("valid HMAC key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn s3_credential_scope_format(credential_scope: &CredentialScope) -> String {
    format!(
        "{}/{}/{}/aws4_request",
        credential_scope.timestamp.format("%Y%m%d"),
        credential_scope.region,
        credential_scope.service
    )
}

fn s3_signing_key(
    secret_key: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
    region: &str,
) -> Vec<u8> {
    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256(k_secret.as_bytes(), timestamp.format("%Y%m%d").to_string().as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}