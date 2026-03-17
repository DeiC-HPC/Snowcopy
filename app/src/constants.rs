pub const CONFIG_PATH: &str = "config.json";
//pub const DEFAULT_REGION:&str = "us-east-1"; //TODO: should exist somewhere that aws_layer can access
pub const CHUNK_SIZE: u64 = 5 * 1024 * 1024;

pub const DOWNLOAD_MULTIPART_THRESHOLD: i64 = 20 * 1024 * 1024;
pub const DOWNLOAD_MULTIPART_CHUNK_SIZE: u64 = 5 * 1024 * 1024;
pub const PRESIGN_LIFETIME: u64 = 900;

pub const MAX_REQUEST_AMOUNT: usize = 400;