pub mod ui;

use std::path::{Path, PathBuf};
use std::time::SystemTime;
use serde::{Deserialize, Serialize};
use url::Url;

pub mod constants;
pub mod cc_util;
pub mod time;

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_url: String
}

pub fn build_context(config: Config) -> SnowContext {
    let ctx = SnowContext {
        s3_access_key: config.s3_access_key,
        s3_secret_key: config.s3_secret_key,
        s3_host: Url::parse(config.s3_url.as_str()).unwrap().host_str().unwrap().to_string(),
    };
    ctx
}
#[derive(Clone)]
pub struct SnowContext {
    s3_access_key: String,
    s3_secret_key: String,
    s3_host: String,
}

impl SnowContext {
    pub fn s3_access_key(&self) -> &str {
        &self.s3_access_key
    }

    pub fn s3_secret_key(&self) -> &str {
        &self.s3_secret_key
    }

    pub fn s3_host(&self) -> &str {
        &self.s3_host
    }
}

#[derive(Clone)]
pub struct FileInfo {
    pub file_path: PathBuf,
    pub key: String,
    pub file_size: u64,
    pub modified_time: Option<SystemTime>,
}

pub fn path_to_s3_key(path: &str) -> String {
    use std::path::Component;

    let path = Path::new(path);
    let mut result = String::with_capacity(path.as_os_str().len());
    let mut components = Vec::new();

    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                components.pop();
            }
            Component::Normal(c) => {
                components.push(c);
            }
        }
    }

    for (i, component) in components.into_iter().enumerate() {
        if i > 0 {
            result.push('/');
        }
        if let Some(s) = component.to_str() {
            result.push_str(s);
        } else {
            result.push_str(&component.to_string_lossy());
        }
    }
    result
}

pub fn s3_key_to_path(base_path: &Path, key: &str) -> PathBuf {
    let mut path = PathBuf::from(base_path);

    // Split S3 key by forward slashes and push each component to the path
    for component in key.split('/') {
        if !component.is_empty() {
            path.push(component);
        }
    }

    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        let test_cases = vec![
            ("./path/to/file", "path/to/file"),
            ("../path/to/file", "path/to/file"),
            ("/path/to/file", "path/to/file"),
            ("path/to/file", "path/to/file"),
            ("./path/../to/file", "to/file"),
            ("./../path/to/file", "path/to/file"),
            ("///path/to/file", "path/to/file"),
            ("./././path", "path"),
            ("./path/t.o/.fi.le", "path/t.o/.fi.le"),
            ("", ""),

            ("C:\\path\\to\\file", "path/to/file"),
            ("C:/path/to/file", "path/to/file"),
            ("D:\\Users\\username\\file", "Users/username/file"),
            ("E:/Program Files/app", "Program Files/app"),
            ("C:\\path\\..\\to\\file", "to/file"),
            ("C:\\.\\path\\to\\file", "path/to/file"),
            ("path\\to\\file", "path/to/file"),
            (".\\path\\to\\file", "path/to/file"),
            ("..\\path\\to\\file", "path/to/file"),
            (".\\path\\..\\to\\file", "to/file"),
            (".\\.\\.\\path", "path"),
            ("..\\..\\path\\to\\file", "path/to/file"),
            (".\\..\\path\\to\\file", "path/to/file"),
        ];

        for (input, expected) in test_cases {
            assert_eq!(
                path_to_s3_key(input),
                expected,
                "Failed for input: {}",
                input
            );
        }
    }

    #[test]
    fn test_s3_key_to_path() {
        let base_path = Path::new("testDataDownload");

        let test_cases = vec![
            ("testData1/RandomSizeSmallFile-998.bin", "testDataDownload/testData1/RandomSizeSmallFile-998.bin"),
            ("testData1/someFolder/RandomSizeSmallFile-1.bin", "testDataDownload/testData1/someFolder/RandomSizeSmallFile-1.bin"),
            ("simple.txt", "testDataDownload/simple.txt"),
            ("folder/subfolder/file.bin", "testDataDownload/folder/subfolder/file.bin"),
        ];

        for (key, expected) in test_cases {
            let result = s3_key_to_path(base_path, key);
            let expected_path = Path::new(expected);
            assert_eq!(
                result.as_path(),
                expected_path,
                "Failed for key: {}",
                key
            );
        }
    }
}

