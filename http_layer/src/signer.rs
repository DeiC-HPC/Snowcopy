use openssl::hash::{Hasher, MessageDigest};
use openssl::pkey::PKey;
use openssl::sign::Signer;
use openssl::sha::sha256;
use std::collections::HashMap;
use std::sync::Arc;
use util_layer::constants::{AMZ_CHUNK_SIGNATURE_HEADER, AMZ_TRAILER_SIGNATURE, AWS_CHUNK_PAYLOAD_ALGORITHM, AWS_CHUNK_TRAILER_ALGORITHM, EMPTY_PAYLOAD_SHA256};
use util_layer::SnowContext;

pub struct CredentialScope {
    pub(crate) timestamp: chrono::DateTime<chrono::Utc>,
    pub(crate) region: String,
    pub(crate) service: String,
}
const AMZ_CONTENT: &str = "x-amz-content-";

pub fn create_sha256_hasher() -> Hasher {
    Hasher::new(MessageDigest::sha256()).expect("Failed to create SHA256 hasher")
}

pub fn finish_sha256_hex(mut hasher: Hasher) -> String {
    let digest = hasher.finish().expect("Failed to finish SHA256 hashing");
    hex::encode(digest)
}

/// Builds SigV4 Authorization for S3 and also returns the computed signature.
/// signed_headers map MUST contain "host", "x-amz-content-sha256", "x-amz-date".
pub fn get_s3_auth_header_and_signature(
    method: &str,
    credentials: Arc<SnowContext>,
    credential_scope: &CredentialScope,
    algorithm: &str,
    signed_headers_sorted: &HashMap<String, String>,
    canonical_uri: Arc<String>,
    query_url: &String,
) -> (String, String) {
    let mut headers: Vec<(&str, &str)> = Vec::with_capacity(signed_headers_sorted.len());
    for (k, v) in signed_headers_sorted {
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

    let payload_sha256 = signed_headers_sorted
        .get(&get_amz_content_header(algorithm))
        .expect("x-amz-content-XXX header matching algorithm is required");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        canonical_uri,
        query_url,
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
        algorithm,
        timestamp_iso,
        credential_scope_str,
        hex_sha256
    );

    let signing_key = s3_signing_key(credentials.s3_secret_key(), &date_y_m_d, credential_scope.region.as_str());
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    let auth_header = format!(
        "{} Credential={}/{}, SignedHeaders={}, Signature={}",
        algorithm, credentials.s3_access_key(), credential_scope_str, signed_headers_str, signature
    );

    (auth_header, signature)
}

pub fn get_s3_auth_header(
    method: &str,
    credentials: Arc<SnowContext>,
    credential_scope: &CredentialScope,
    algorithm: &str,
    signed_headers: &HashMap<String, String>,
    canonical_uri: Arc<String>,
    query_uri: &String,
) -> String {
    let (auth, _) =
        get_s3_auth_header_and_signature(method, credentials, credential_scope, algorithm, signed_headers, canonical_uri, query_uri);
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
    let actual_hash = sha256(data);
    hex::encode(actual_hash)
}

pub(crate) fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let pkey = PKey::hmac(key).expect("Failed to create HMAC key");
    let mut signer = Signer::new(MessageDigest::sha256(), &pkey).expect("Failed to create HMAC signer");
    signer.update(data).expect("Failed to update HMAC signer");
    let signature = signer.sign_to_vec().expect("Failed to sign HMAC");
    let mut res = [0u8; 32];
    res.copy_from_slice(&signature);
    res
}

pub fn s3_signing_key(
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

/// Signs a chunk for AWS chunked encoding (trailing headers)
pub fn sign_chunk(
    signing_key: &[u8; 32],
    amz_date: &str,
    credential_scope: &str,
    prev_signature: &str,
    chunk_data: &[u8],
) -> String {
    let chunk_hash = sha256_hex(chunk_data);

    let string_to_sign = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        AWS_CHUNK_PAYLOAD_ALGORITHM,
        amz_date,
        credential_scope,
        prev_signature,
        EMPTY_PAYLOAD_SHA256,
        chunk_hash
    );

    hex::encode(hmac_sha256(signing_key, string_to_sign.as_bytes()))
}

/// Signs a trailer for AWS chunked encoding with trailing headers
pub fn sign_trailer(
    signing_key: &[u8; 32],
    amz_date: &str,
    credential_scope: &str,
    prev_signature: &str,
    trailer_name: &str,
    trailer_value: &str,
) -> String {
    // The canonicalized trailer string is "name:value\n" (single LF), then SHA256-hex of that
    let trailer_line = format!("{}:{}\n", trailer_name.to_lowercase(), trailer_value);
    let trailer_hash = sha256_hex(trailer_line.as_bytes());

    let string_to_sign = format!(
        "{}\n{}\n{}\n{}\n{}",
        AWS_CHUNK_TRAILER_ALGORITHM,
        amz_date,
        credential_scope,
        prev_signature,
        trailer_hash
    );

    hex::encode(hmac_sha256(signing_key, string_to_sign.as_bytes()))
}

// TODO LND: This can be compressed - no need to show our work, just give us the number
/// Calculates the total encoded Content-Length for aws-chunked + SigV4 streaming with trailing headers
pub fn aws_chunked_trailer_encoded_length(
    decoded_len: u64,
    chunk_size: usize,
    trailer_name: &str,
    trailer_value_len: usize,
) -> u64 {
    let mut total: u64 = 0;

    // Signature header overhead: ";chunk-signature=<64-hex-chars>"
    let sig_header_overhead: u64 = 1 + AMZ_CHUNK_SIGNATURE_HEADER.len() as u64 + 1 + 64;

    // 1) Payload chunks
    let mut remain = decoded_len;
    while remain > 0 {
        let n = std::cmp::min(chunk_size as u64, remain);
        remain -= n;

        let hex_digits = format!("{:x}", n).len() as u64;

        total += hex_digits;         // "<hexlen>"
        total += sig_header_overhead; // ";chunk-signature=<sig>"
        total += 2;                  // "\r\n" after chunk header line
        total += n;                  // chunk data
        total += 2;                  // "\r\n" after chunk data
    }

    // 2) Final 0-byte data chunk: "0;chunk-signature=<64-hex>\r\n"
    total += 1;                  // "0"
    total += sig_header_overhead; // ";chunk-signature=<sig>"
    total += 2;                  // "\r\n" after header line

    // 3) Trailing headers (raw, NOT in a chunk wrapper):
    //    "<trailerName>:<value>\r\n"
    //    "x-amz-trailer-signature:<64-hex-sig>\r\n"
    //    "\r\n"
    let lower_trailer_name = trailer_name.to_lowercase();
    let checksum_line_len: u64 = lower_trailer_name.len() as u64 + 1 + trailer_value_len as u64 + 2; // name + ":" + value + "\r\n"
    let trailer_sig_line_len: u64 = AMZ_TRAILER_SIGNATURE.len() as u64 + 1 + 64 + 2; // name + ":" + 64-hex + "\r\n"

    total += checksum_line_len;
    total += trailer_sig_line_len;
    total += 2; // final "\r\n"

    total
}