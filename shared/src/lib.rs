use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
        s3_url: config.s3_url,
    };
    ctx
}
#[derive(Clone)]
pub struct SnowContext {
    s3_access_key: String,
    s3_secret_key: String,
    s3_url: String,
}

impl SnowContext {
    pub fn s3_access_key(&self) -> &str {
        &self.s3_access_key
    }

    pub fn s3_secret_key(&self) -> &str {
        &self.s3_secret_key
    }

    pub fn s3_url(&self) -> &str {
        &self.s3_url
    }
}

#[derive(Clone)]
pub struct FileInfo {
    pub file_path: PathBuf,
    pub key: String,
    pub file_size: u64,
}