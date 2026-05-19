
use std::sync::Arc;
use std::fs::File;
use std::time::SystemTime;
use bytes::Bytes;

pub type WriteMsg = (Arc<File>, u64, Bytes);
pub type WriteAndMtimeMsg = (Arc<File>, u64, Bytes, SystemTime);