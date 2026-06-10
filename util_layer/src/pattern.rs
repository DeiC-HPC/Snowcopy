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
}
