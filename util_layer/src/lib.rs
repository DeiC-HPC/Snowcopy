pub mod ui;
pub mod constants;
pub mod cc_util;
pub mod time;
pub mod retry;
pub mod bucket_and_path_parser;
pub mod snow_errors;
pub mod download;
pub mod url_encoder;
pub mod error_codes;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::string::ToString;
use std::time::SystemTime;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Mode {
    Upload,
    Download,
    List,
    Remove,
    Config,
}

pub fn default_region() -> String {
    "us-east-1".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EndpointConfig {
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_url: String,
    #[serde(default = "default_region")]
    pub s3_region: String,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_url: String::new(),
            s3_region: default_region(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Endpoint {
    pub name: String,
    pub config: Option<EndpointConfig>,
    pub settings: Option<HashMap<String, serde_json::Value>>,
}

impl Endpoint {
    pub fn is_complete(&self) -> bool {
        if self.name.trim().is_empty() {
            return false;
        }
        if let Some(config) = &self.config {
            !config.s3_access_key.trim().is_empty()
                && !config.s3_secret_key.trim().is_empty()
                && !config.s3_url.trim().is_empty()
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMethod {
    TokioSelfMade,
    TransferManager,
}

impl TransferMethod {
    pub fn from_str(method: &str) -> Option<Self> {
        match method {
            "o1" => Some(TransferMethod::TokioSelfMade),
            "o2" => Some(TransferMethod::TransferManager),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UploadConfig {
    pub endpoint: EndpointConfig,
    pub md5_checksum_upload: bool,
    pub transfer_method: TransferMethod,
    pub file_limit_overwrite: Option<usize>,
    pub req_limit_overwrite: Option<usize>,
    pub silent: bool,
    pub stats: bool,
    pub trailing_chunk_size: u64,
    pub trailing: bool,
    pub trailing_small: bool,
    pub open_file_limit_overwrite: Option<usize>,
    pub bucket: String,
    pub local_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub endpoint: EndpointConfig,
    pub md5_checksum_integrity: bool,
    pub transfer_method: TransferMethod,
    pub file_limit_overwrite: Option<usize>,
    pub req_limit_overwrite: Option<usize>,
    pub download_concurrency_overwrite: Option<usize>,
    pub silent: bool,
    pub stats: bool,
    pub open_file_limit_overwrite: Option<usize>,
    pub bucket: String,
    pub local_path: PathBuf,
    pub prefix: String,
}

#[derive(Debug, Clone)]
pub struct ListConfig {
    pub endpoint: EndpointConfig,
    pub bucket: String,
    pub req_limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct RemoveConfig {
    pub endpoint: EndpointConfig,
    pub bucket: String,
    pub silent: bool,
    pub req_limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ConfigModeConfig {
    pub silent: bool,
}

pub trait S3ConfigTrait {
    fn endpoint(&self) -> &EndpointConfig;
}

impl S3ConfigTrait for UploadConfig {
    fn endpoint(&self) -> &EndpointConfig {
        &self.endpoint
    }
}

impl S3ConfigTrait for DownloadConfig {
    fn endpoint(&self) -> &EndpointConfig {
        &self.endpoint
    }
}

impl S3ConfigTrait for ListConfig {
    fn endpoint(&self) -> &EndpointConfig {
        &self.endpoint
    }
}

impl S3ConfigTrait for RemoveConfig {
    fn endpoint(&self) -> &EndpointConfig {
        &self.endpoint
    }
}

pub enum SnowConfig {
    Upload(UploadConfig),
    Download(DownloadConfig),
    List(ListConfig),
    Remove(RemoveConfig),
    Config(ConfigModeConfig),
}

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub aws_uri_encoded_key: String,
    pub local_path: PathBuf,
    pub size: i64,
    pub modified_time: Option<SystemTime>,
    pub chunk_size: Option<usize>,
    pub etag: Option<String>,
    pub sha256_checksum: Option<String>,
}

#[derive(Clone)]
pub struct FileInfo {
    pub file_path: PathBuf,
    pub aws_uri_encoded_key: String,
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
    #[cfg(target_os = "windows")]
    fn test_normalize_path_windows() {
        let test_cases = vec![
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
    #[cfg(target_os = "linux")]
    fn test_normalize_path_linux() {
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

