use openssl::hash::{Hasher, MessageDigest};
use openssl::pkey::PKey;
use openssl::sha::sha256;
use openssl::sign::Signer;
use std::collections::HashMap;
use util_layer::constants::{AMZ_CHUNK_SIGNATURE_HEADER, AMZ_TRAILER_SIGNATURE, AWS_CHUNK_PAYLOAD_ALGORITHM, AWS_CHUNK_TRAILER_ALGORITHM, EMPTY_PAYLOAD_SHA256};
use util_layer::EndpointConfig;

pub struct CredentialScope {
    pub(crate) timestamp: chrono::DateTime<chrono::Utc>,
    pub(crate) region: String,
    pub(crate) service: String,
}
const AMZ_CONTENT: &str = "x-amz-content-";

pub fn create_sha256_hasher() -> Hasher {
    Hasher::new(MessageDigest::sha256()).expect("Failed to create SHA256 hasher")
}

/// Signing key for AWS chunked encoding
pub struct ChunkSigner {
    pkey: PKey<openssl::pkey::Private>,
}

impl ChunkSigner {
    pub fn new(signing_key: &[u8;32]) -> Self {
        Self {
            pkey: PKey::hmac(signing_key).expect("Failed to create HMAC key"),
        }
    }

    pub fn sign(&self, data: &[u8]) -> [u8; 32] {
        let mut signer = Signer::new(MessageDigest::sha256(), &self.pkey)
            .expect("Failed to create HMAC signer");
        signer.update(data).expect("Failed to update HMAC signer");
        let signature = signer.sign_to_vec().expect("Failed to sign HMAC");
        let mut res = [0u8; 32];
        res.copy_from_slice(&signature);
        res
    }

    pub fn sign_chunk(
        &self,
        amz_date: &str,
        credential_scope: &str,
        prev_signature: &str,
        chunk_hash: &str,
    ) -> String {
        let string_to_sign = push_elements_to_string(&[AWS_CHUNK_PAYLOAD_ALGORITHM, amz_date, credential_scope, prev_signature, EMPTY_PAYLOAD_SHA256, chunk_hash]);

        hex::encode(self.sign(string_to_sign.as_bytes()))
    }

    /// Signs a trailer for AWS chunked encoding with trailing headers
    pub fn sign_trailer(
        &self,
        amz_date: &str,
        credential_scope: &str,
        prev_signature: &str,
        trailer_name: &str,
        trailer_value: &str,
    ) -> String {
        // The canonicalized trailer string is "name:value\n" (single LF), then SHA256-hex of that
        let trailer_line = format!("{}:{}\n", trailer_name.to_lowercase(), trailer_value);
        let trailer_hash = sha256_hex(trailer_line.as_bytes());

        let string_to_sign = push_elements_to_string(&[AWS_CHUNK_TRAILER_ALGORITHM, amz_date, credential_scope, prev_signature, trailer_hash.as_str()]);

        hex::encode(self.sign(string_to_sign.as_bytes()))
    }
}

pub fn finish_sha256_hex(mut hasher: Hasher) -> String {
    let digest = hasher.finish().expect("Failed to finish SHA256 hashing");
    hex::encode(digest)
}

/// Builds SigV4 Authorization for S3 and also returns the computed signature.
/// signed_headers map MUST contain "host", "x-amz-content-sha256", "x-amz-date".
pub fn get_s3_auth_header_and_signature(
    method: &str,
    config: &EndpointConfig,
    credential_scope: &CredentialScope,
    algorithm: &str,
    signed_headers_sorted: &HashMap<String, String>,
    canonical_uri: &String,
    query_url: &String,
) -> (String, String, [u8; 32]) {
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
    let string_to_sign = push_elements_to_string(
        &[algorithm,
        timestamp_iso.as_str(),
        credential_scope_str.as_str(),
        hex_sha256.as_str()]
    );


    let signing_key = s3_signing_key(&config.s3_secret_key, &date_y_m_d, credential_scope.region.as_str());
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    let auth_header = format!(
        "{} Credential={}/{}, SignedHeaders={}, Signature={}",
        algorithm, config.s3_access_key, credential_scope_str, signed_headers_str, signature
    );

    (auth_header, signature, signing_key)
}

pub fn get_s3_auth_header(
    method: &str,
    config: &EndpointConfig,
    credential_scope: &CredentialScope,
    algorithm: &str,
    signed_headers: &HashMap<String, String>,
    canonical_uri: &String,
    query_uri: &String,
) -> String {
    let (auth, _, _) =
        get_s3_auth_header_and_signature(method, config, credential_scope, algorithm, signed_headers, canonical_uri, query_uri);
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

/// Calculates the total encoded Content-Length for aws-chunked + SigV4 streaming with trailing headers
pub fn aws_chunked_trailer_encoded_length(
    decoded_len: u64,
    chunk_size: u64,
    trailer_name: &str,
    trailer_value_len: usize,
) -> u64 {
    let mut total: u64 = 0;

    // Signature header overhead: ";chunk-signature=<64-hex-chars>"
    let sig_header_overhead: u64 = 1 + AMZ_CHUNK_SIGNATURE_HEADER.len() as u64 + 1 + 64;

    // 1) Payload chunks
    let mut remain = decoded_len;
    while remain > 0 {
        let n = std::cmp::min(chunk_size, remain);
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
    //total += 2;                  // "\r\n" empty line before trailers

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

/**
 * Pushes a list of elements to a string, separated by newline characters.
 */
fn push_elements_to_string(elements: &[&str]) -> String {
    let capacity = elements.iter().map(|element| element.len()).sum::<usize>()
        + elements.len().saturating_sub(1);
    let mut string_to_sign = String::with_capacity(capacity);

    for (index, element) in elements.iter().enumerate() {
        if index > 0 {
            string_to_sign.push('\n');
        }
        string_to_sign.push_str(element);
    }
    string_to_sign
}

#[cfg(test)]
mod tests {
    use crate::signer::*;

    
    #[test]
    fn test_push_elements_to_string() {
        let elements = ["element1", "element2", "element3"];
        let result = push_elements_to_string(&elements);
        assert_eq!(result, "element1\nelement2\nelement3");
    }

    #[test]
    fn test_aws_streaming_trailers_example() {
        // AWS Example from https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming-trailers.html
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let amz_date = "20130524T000000Z";
        let date_y_m_d = "20130524";
        let region = "us-east-1";
        let credential_scope = "20130524/us-east-1/s3/aws4_request";
        let seed_signature = "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e";

        let signing_key = s3_signing_key(secret_key, date_y_m_d, region);
        let signer = ChunkSigner::new(&signing_key);

        // Chunk 1 (65536 bytes of 'a')
        // In the example, the hash of 65536 'a's is bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a
        let chunk1_hash = "bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a";
        let chunk1_sig = signer.sign_chunk(amz_date, credential_scope, seed_signature, chunk1_hash);
        assert_eq!(chunk1_sig, "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2");

        // Chunk 2 (1024 bytes of 'a')
        // Hash of 1024 'a's is 2edc986847e209b4016e141a6dc8716d3207350f416969382d431539bf292e4a
        let chunk2_hash = "2edc986847e209b4016e141a6dc8716d3207350f416969382d431539bf292e4a";
        let chunk2_sig = signer.sign_chunk(amz_date, credential_scope, &chunk1_sig, chunk2_hash);
        assert_eq!(chunk2_sig, "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7");

        // Chunk 3 (0-byte data chunk)
        // Hash of 0 bytes is EMPTY_PAYLOAD_SHA256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        let chunk3_hash = EMPTY_PAYLOAD_SHA256;
        let chunk3_sig = signer.sign_chunk(amz_date, credential_scope, &chunk2_sig, chunk3_hash);
        assert_eq!(chunk3_sig, "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992");

        // Chunk 4: Trailing headers
        // Trailer name: x-amz-checksum-crc32c, value: sOO8/Q==
        let trailer_name = "x-amz-checksum-crc32c";
        let trailer_value = "sOO8/Q==";
        let trailer_sig = signer.sign_trailer(amz_date, credential_scope, &chunk3_sig, trailer_name, trailer_value);
        assert_eq!(trailer_sig, "d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435");
    }

    #[test]
    fn test_aws_chunked_trailer_encoded_length_example() {
        // Based on the example: AWS Example from https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming-trailers.html
        // x-amz-decoded-content-length: 66560 (65536 + 1024)
        // chunk-size: 65536 (10000 hex)
        // trailer: x-amz-checksum-crc32c, value len: 8 ("sOO8/Q==")
        let decoded_len = 66560;
        let chunk_size = 65536;
        let trailer_name = "x-amz-checksum-crc32c";
        let trailer_value_len = 8;

        let total_len = aws_chunked_trailer_encoded_length(decoded_len, chunk_size, trailer_name, trailer_value_len);

        assert_eq!(total_len, 66946);
    }
}