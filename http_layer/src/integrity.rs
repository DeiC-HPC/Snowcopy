use openssl::hash::{Hasher, MessageDigest};
use util_layer::S3Object;

#[derive(Clone)]
pub enum IntegrityAlgorithm {
    None,
    SHA256,
    MD5,
}

/// Determines if the object has an ETag composed of multiple parts and we have part size metadata,
/// such that we can reconstruct the ETag on the way down to compare it.
pub fn reconstructable_multipart_etag(object: &S3Object) -> bool {
    // S3 multipart ETags typically look like "digest-N" where N is the number of parts.
    // We also need the chunk size to know how the object was split.
    if let Some(etag) = &object.etag {
        if etag.contains("-") && object.chunk_size.is_some() {
            return true;
        }
    }
    false
}

/// Reconstructs the multipart ETag from the MD5 digests of its parts.
/// part_md5s: A slice of MD5 digests (binary) for each part in order.
pub fn reconstruct_multipart_etag(part_md5s: &[Vec<u8>]) -> String {
    let mut hasher = Hasher::new(MessageDigest::md5()).expect("Failed to create MD5 hasher");
    for md5 in part_md5s {
        hasher.update(md5).expect("Failed to update MD5 hasher");
    }
    let final_md5 = hasher.finish().expect("Failed to finish MD5 hasher");
    format!("{}-{}", hex::encode(final_md5), part_md5s.len())
}

#[cfg(test)]
mod tests {
    use util_layer::S3Object;
    use super::*;

    #[test]
    fn test_reconstruct_multipart_etag() {
        // MD5 of "part1" is 9687e148e916c025553155708892789d
        let part1_md5 = hex::decode("9687e148e916c025553155708892789d").unwrap();
        // MD5 of "part2" is a30b3554e2f89c368f51a7e28945625c
        let part2_md5 = hex::decode("a30b3554e2f89c368f51a7e28945625c").unwrap();
        
        // Final MD5 of concatenated (9687e148e916c025553155708892789da30b3554e2f89c368f51a7e28945625c)
        // is f92c0a65778070b85bf59dbd4bfaec7c
        let etag = reconstruct_multipart_etag(&[part1_md5, part2_md5]);
        assert_eq!(etag, "f92c0a65778070b85bf59dbd4bfaec7c-2");
    }

    #[test]
    fn test_can_reconstruct_etag() {
        let mut obj = S3Object {
            key: "test".to_string(),
            local_path: std::path::PathBuf::from("test"),
            size: 100,
            modified_time: None,
            chunk_size: Some(10),
            etag: Some("\"538c298069d27f8a718747f48170c8f1-2\"".to_string()),
        };
        assert!(reconstructable_multipart_etag(&obj));

        obj.chunk_size = None;
        assert!(!reconstructable_multipart_etag(&obj));

        obj.chunk_size = Some(10);
        obj.etag = Some("\"538c298069d27f8a718747f48170c8f1\"".to_string());
        assert!(!reconstructable_multipart_etag(&obj));
    }
}