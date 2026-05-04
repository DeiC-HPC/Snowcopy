pub const CONFIG_PATH: &str = "config.json";
//pub const DEFAULT_REGION:&str = "us-east-1"; //TODO: should exist somewhere that aws_layer can access
pub const CHUNK_SIZE: u64 = 5 * 1024 * 1024;
pub const DOWNLOAD_MULTIPART_THRESHOLD: i64 = 4 * 1024 * 1024;
pub const PRESIGN_LIFETIME: u64 = 900;

pub const MAX_REQUEST_AMOUNT: usize = 400;

pub const AWS_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const EMPTY_PAYLOAD_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";


pub const DISCOVERY_LIMIT_DOWNLOAD: u64 = 5000;
pub const METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_DOWNLOAD: u64 = 1000;
pub const DISCOVERY_LIMIT_UPLOAD: u64 = 5000;
pub const METADATA_FETCH_AND_COMPARISON_CHANNEL_LIMIT_UPLOAD: u64 = 1000;
pub const MAX_RETRY: u32 = 10;

//Trailing header
pub const AWS_CHUNK_PAYLOAD_ALGORITHM: &str = "AWS4-HMAC-SHA256-PAYLOAD";
pub const AWS_CHUNK_TRAILER_ALGORITHM: &str = "AWS4-HMAC-SHA256-TRAILER";
pub const AMZ_STREAMING_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub const AMZ_CHUNK_SIGNATURE_HEADER: &str = "chunk-signature";
pub const AMZ_CHECKSUM_SHA256: &str = "x-amz-checksum-sha256";
pub const AMZ_TRAILER_SIGNATURE: &str = "x-amz-trailer-signature";
pub const AWS_TRAILER_CHUNK_SIZE: usize = 5 * 1024 * 1024;