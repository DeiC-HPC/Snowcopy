pub mod ui;
pub mod constants;
pub mod cc_util;
pub mod time;
pub mod retry;
pub mod pattern;
pub mod snow_errors;
pub mod download;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use url::Url;

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum Mode {
    Upload,
    Download,
    List,
    Remove,
}

pub fn default_region() -> String {
    "us-east-1".to_string()
}

pub fn default_region_option() -> Option<String> {
    Some(default_region())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EndpointConfig {
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_url: String,
    #[serde(default = "default_region_option")]
    pub s3_region: Option<String>,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_url: String::new(),
            s3_region: default_region_option(),
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

pub fn build_context(
    endpoint: EndpointConfig,
    md5_checksum_upload: bool,
    transfer_method: String,
    file_limit: Option<usize>,
    req_limit: Option<usize>,
    download_concurrency: Option<usize>,
    silent: bool,
    stats: bool,
    trailing_chunk_size: u64,
    trailing: bool,
    trailing_small: bool,
    open_file_sem_limit: Arc<Semaphore>,
    mode: Mode,
    bucket: String,
    local_path: PathBuf,
) -> SnowContext {
    let region = endpoint.s3_region.unwrap_or_else(|| "us-east-1".to_string());
    let ctx = SnowContext {
        s3_access_key: endpoint.s3_access_key,
        s3_secret_key: endpoint.s3_secret_key,
        s3_host: Url::parse(endpoint.s3_url.as_str()).unwrap().host_str().unwrap().to_string(),
        s3_url: endpoint.s3_url,
        s3_region: region,
        trailing_chunk_size,
        trailing,
        trailing_small,
        open_file_sem_limit,
        md5_checksum_upload,
        transfer_method,
        file_limit,
        req_limit,
        download_concurrency,
        silent,
        stats,
        mode,
        bucket,
        local_path,
    };
    ctx
}

#[derive(Clone)]
pub struct SnowContext {
    s3_access_key: String,
    s3_secret_key: String,
    s3_host: String,
    s3_url: String,
    s3_region: String,
    pub trailing_chunk_size: u64,
    pub trailing: bool,
    pub trailing_small: bool,
    pub open_file_sem_limit: Arc<Semaphore>,
    md5_checksum_upload: bool,
    pub transfer_method: String,
    pub file_limit: Option<usize>,
    pub req_limit: Option<usize>,
    pub download_concurrency: Option<usize>,
    pub silent: bool,
    pub stats: bool,
    pub mode: Mode,
    pub bucket: String,
    pub local_path: PathBuf,
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

    pub fn s3_url(&self) -> &str {
        &self.s3_url
    }

    pub fn s3_region(&self) -> &str {
        &self.s3_region
    }

    pub fn md5_checksum_integrity_check(&self) -> bool {
        self.md5_checksum_upload
    }

    pub fn trailing_chunk_size(&self) -> u64 {
        self.trailing_chunk_size
    }

    pub fn trailing(&self) -> bool {
        self.trailing
    }

    pub fn trailing_small(&self) -> bool {
        self.trailing_small
    }

    pub fn open_file_sem_limit(&self) -> &Arc<Semaphore> {
        &self.open_file_sem_limit
    }
}

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
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

