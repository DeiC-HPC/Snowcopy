use std::path::PathBuf;

#[derive(Debug, PartialEq)]
pub enum PathPattern {
    Local(PathBuf),
    S3 {
        profile: String,
        bucket: String,
        key: Option<String>,
    },
}

impl PathPattern {
    pub fn parse(input: &str) -> Self {
        if let Some(colon_idx) = input.find(':') {
            // Check if it looks like a Windows drive letter (e.g., C:\)
            if colon_idx == 1 && input.len() > 2 && (input.as_bytes()[2] == b'\\' || input.as_bytes()[2] == b'/') {
                return PathPattern::Local(PathBuf::from(input));
            }

            let profile = input[..colon_idx].to_string();
            let remainder = &input[colon_idx + 1..];
            
            if let Some(slash_idx) = remainder.find('/') {
                let bucket = remainder[..slash_idx].to_string();
                let key = &remainder[slash_idx + 1..];
                let key = if key.is_empty() { None } else { Some(key.to_string()) };
                PathPattern::S3 { profile, bucket, key }
            } else {
                PathPattern::S3 {
                    profile,
                    bucket: remainder.to_string(),
                    key: None,
                }
            }
        } else {
            PathPattern::Local(PathBuf::from(input))
        }
    }

    /// Validates an S3 bucket name according to AWS rules.
    /// Returns Ok(()) if valid, or a descriptive error message if invalid.
    ///
    /// Rules:
    /// - 3-63 characters long.
    /// - Lowercase letters, numbers, dots (.), and hyphens (-).
    /// - Start and end with a letter or number.
    /// - Not formatted as an IP address.
    /// - No adjacent periods (..).
    /// - Not start with 'xn--', 'sthree-', or 'amzn-s3-demo-'.
    /// - Not end with '-s3alias', '--ol-s3', '.mrap', '--x-s3', or '--table-s3'.
    pub fn validate_bucket_name(bucket: &str) -> Result<(), String> {
        //TODO JON: Should we even care about aws special names? https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
        //TODO JON: I'm unsure if a user would need to upload/download to the special buckets directly, which would mean they are valid
        if bucket.len() < 3 || bucket.len() > 63 {
            return Err("Bucket name must be between 3 and 63 characters long".to_string());
        }

        if !bucket.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-') {
            return Err("Bucket name can only contain lowercase letters, numbers, dots (.), and hyphens (-)".to_string());
        }

        let first_char = bucket.chars().next().unwrap();
        let last_char = bucket.chars().last().unwrap();
        if !(first_char.is_ascii_lowercase() || first_char.is_ascii_digit()) {
            return Err("Bucket name must start with a lowercase letter or number".to_string());
        }
        if !(last_char.is_ascii_lowercase() || last_char.is_ascii_digit()) {
            return Err("Bucket name must end with a lowercase letter or number".to_string());
        }

        if bucket.contains("..") {
            return Err("Bucket name must not contain adjacent periods (..)".to_string());
        }

        // IP address check (v4)
        let parts: Vec<&str> = bucket.split('.').collect();
        if parts.len() == 4 && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty()) {
            let is_ip = parts.iter().all(|p| p.parse::<u8>().is_ok());
            if is_ip {
                return Err("Bucket name must not be formatted as an IP address".to_string());
            }
        }

        // Reserved prefixes
        if bucket.starts_with("xn--") {
            return Err("Bucket name must not start with 'xn--'".to_string());
        }
        if bucket.starts_with("sthree-") {
            return Err("Bucket name must not start with 'sthree-'".to_string());
        }
        if bucket.starts_with("amzn-s3-demo-") {
            return Err("Bucket name must not start with 'amzn-s3-demo-'".to_string());
        }

        // Reserved suffixes
        if bucket.ends_with("-s3alias") {
            return Err("Bucket name must not end with '-s3alias'".to_string());
        }
        if bucket.ends_with("--ol-s3") {
            return Err("Bucket name must not end with '--ol-s3'".to_string());
        }
        if bucket.ends_with(".mrap") {
            return Err("Bucket name must not end with '.mrap'".to_string());
        }
        if bucket.ends_with("--x-s3") {
            return Err("Bucket name must not end with '--x-s3'".to_string());
        }
        if bucket.ends_with("--table-s3") {
            return Err("Bucket name must not end with '--table-s3'".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_local() {
        assert_eq!(PathPattern::parse("../some-folder/file"), PathPattern::Local(PathBuf::from("../some-folder/file")));
        assert_eq!(PathPattern::parse("C:\\Users\\test"), PathPattern::Local(PathBuf::from("C:\\Users\\test")));
    }

    #[test]
    fn test_parse_s3() {
        assert_eq!(
            PathPattern::parse("lumi:my-bucket"),
            PathPattern::S3 {
                profile: "lumi".to_string(),
                bucket: "my-bucket".to_string(),
                key: None
            }
        );
        assert_eq!(
            PathPattern::parse("lumi:my-bucket/some/path"),
            PathPattern::S3 {
                profile: "lumi".to_string(),
                bucket: "my-bucket".to_string(),
                key: Some("some/path".to_string())
            }
        );
        assert_eq!(
            PathPattern::parse("lumi:my-bucket/"),
            PathPattern::S3 {
                profile: "lumi".to_string(),
                bucket: "my-bucket".to_string(),
                key: None
            }
        );
    }

    #[test]
    fn test_validate_bucket_name() {
        assert!(PathPattern::validate_bucket_name("my-bucket").is_ok());
        assert!(PathPattern::validate_bucket_name("my.bucket").is_ok());
        assert!(PathPattern::validate_bucket_name("123-bucket").is_ok());
        assert!(PathPattern::validate_bucket_name("bucket-123").is_ok());

        assert!(PathPattern::validate_bucket_name("ab").is_err()); // Too short
        assert!(PathPattern::validate_bucket_name(&"a".repeat(64)).is_err()); // Too long
        assert!(PathPattern::validate_bucket_name("MyBucket").is_err()); // Uppercase
        assert!(PathPattern::validate_bucket_name("-bucket").is_err()); // Starts with hyphen
        assert!(PathPattern::validate_bucket_name("bucket-").is_err()); // Ends with hyphen
        assert!(PathPattern::validate_bucket_name(".bucket").is_err()); // Starts with dot
        assert!(PathPattern::validate_bucket_name("bucket.").is_err()); // Ends with dot
        assert!(PathPattern::validate_bucket_name("bucket..name").is_err()); // Adjacent dots
        assert!(PathPattern::validate_bucket_name("192.168.1.1").is_err()); // IP address
        assert!(PathPattern::validate_bucket_name("xn--bucket").is_err()); // Reserved prefix
        assert!(PathPattern::validate_bucket_name("sthree-bucket").is_err()); // Reserved prefix
        assert!(PathPattern::validate_bucket_name("amzn-s3-demo-bucket").is_err()); // Reserved prefix
        assert!(PathPattern::validate_bucket_name("bucket-s3alias").is_err()); // Reserved suffix
        assert!(PathPattern::validate_bucket_name("bucket--ol-s3").is_err()); // Reserved suffix
        assert!(PathPattern::validate_bucket_name("bucket.mrap").is_err()); // Reserved suffix
        assert!(PathPattern::validate_bucket_name("bucket--x-s3").is_err()); // Reserved suffix
        assert!(PathPattern::validate_bucket_name("bucket--table-s3").is_err()); // Reserved suffix
    }
}
