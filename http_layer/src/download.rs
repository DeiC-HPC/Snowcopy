use crate::integrity::IntegrityAlgorithm;
use crate::{create_and_sign_req, map_s3_error, send_request, signer, HyperClient};
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::Method;
use std::cmp;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::sync::Semaphore;
use util_layer::constants::{AWS_ALGORITHM, DOWNLOAD_CHUNK_SIZE, DOWNLOAD_DIRECT_WRITE_FRAME_THRESHOLD, EMPTY_PAYLOAD_SHA256};
use util_layer::download::{FileCompletionTracker, WriteMsg};
use util_layer::snow_errors::SnowCopyError;
use util_layer::{DownloadConfig, FileInfo};
use util_layer::cc_util::CommandCenterEvent;

pub async fn download_file_range(start: u64, end: u64, config: Arc<DownloadConfig>, pool_client: &Arc<HyperClient>, req_sem: Arc<Semaphore>, canonical_uri: &String, query_uri: &String, fd : Arc<File>, disk_write_tx: Sender<WriteMsg>, tracker: Arc<FileCompletionTracker>, hash_algo : IntegrityAlgorithm, cc_tx: UnboundedSender<CommandCenterEvent>) -> Result<(u64, Option<String>), SnowCopyError> {
    let range = format!("bytes={}-{}", start, end);
    let mut header_map : HashMap<String, String> = HashMap::new();
    header_map.insert(signer::get_amz_content_header(AWS_ALGORITHM), EMPTY_PAYLOAD_SHA256.to_string());
    header_map.insert("range".to_string(), range);

    let req = create_and_sign_req(config.clone(), canonical_uri, query_uri, None, Method::GET, &header_map)?;

    let req_permit = req_sem.acquire().await.expect("Should never fail");
    let resp = send_request(pool_client, req, cc_tx, config.verbose).await?;

    let status = resp.status();
    if !status.is_success() && status != hyper::StatusCode::PARTIAL_CONTENT {
        let s3_err = map_s3_error(resp).await;
        drop(req_permit);
        return Err(s3_err.into());
    }

    let mut body = resp.into_body();

    let (bytes_read, hash) = download_body_to_buffer_and_offload_to_disk_writers(start, fd, disk_write_tx, &mut body, tracker, (end - start + 1) as usize, hash_algo).await?;

    if bytes_read != end - start + 1 {
        return Err(SnowCopyError::RetryableError(format!("Partial download failed. Expected {} bytes, got {}", end - start + 1, bytes_read)));
    }

    drop(req_permit);
    Ok((bytes_read, hash))
}

async fn download_body_to_buffer_and_offload_to_disk_writers(start: u64, fd: Arc<File>, disk_write_tx: Sender<WriteMsg>, body: &mut Incoming, tracker: Arc<FileCompletionTracker>, file_size: usize, hash_algo : IntegrityAlgorithm) -> Result<(u64, Option<String>), SnowCopyError> {
    let mut bytes_read = 0u64;

    let mut buffer = Vec::with_capacity(cmp::min(DOWNLOAD_CHUNK_SIZE, file_size));
    let mut buffer_start = start;

    let mut hasher = match hash_algo {
        IntegrityAlgorithm::MD5 => Some(openssl::hash::Hasher::new(openssl::hash::MessageDigest::md5()).map_err(|e| SnowCopyError::from(e))?),
        IntegrityAlgorithm::SHA256 => Some(openssl::hash::Hasher::new(openssl::hash::MessageDigest::sha256()).map_err(|e| SnowCopyError::from(e))?),
        IntegrityAlgorithm::None => None,
    };

    while let Some(frame) = body.frame().await {
        let frame = frame?;

        if let Ok(data) = frame.into_data() {
            if let Some(ref mut h) = hasher {
                h.update(&data).map_err(|e| SnowCopyError::from(e))?;
            }

            //Large enough tcp frame that we can just pass it directly to disk writers
            if data.len() >= DOWNLOAD_DIRECT_WRITE_FRAME_THRESHOLD { //we can save time by not copying data into the buffer
                if !buffer.is_empty() {
                    //flush the current buffer before sending the new frame directly to disk writers
                    let buffered = std::mem::replace(&mut buffer, Vec::with_capacity(cmp::min(DOWNLOAD_CHUNK_SIZE, file_size)));

                    disk_write_tx
                        .send((fd.clone(), buffer_start, Bytes::from(buffered), tracker.clone()))
                        .await?;
                }

                let offset = start + bytes_read;
                bytes_read += data.len() as u64;

                disk_write_tx
                    .send((fd.clone(), offset, data, tracker.clone()))
                    .await?;

                continue;
            }

            if buffer.is_empty() { //this is only empty when we have just flushed it, in that case we should set the start position for the next buffer
                buffer_start = start + bytes_read;
            }

            buffer.extend_from_slice(&data);
            bytes_read += data.len() as u64;

            if buffer.len() >= DOWNLOAD_CHUNK_SIZE {
                let data = std::mem::replace(
                    &mut buffer,
                    Vec::with_capacity(cmp::min(DOWNLOAD_CHUNK_SIZE, file_size)),
                );

                disk_write_tx
                    .send((fd.clone(), buffer_start, Bytes::from(data), tracker.clone()))
                    .await?;
            }
        }
    }

    if !buffer.is_empty() {
        disk_write_tx
            .send((fd, buffer_start, Bytes::from(buffer), tracker))
            .await?;
    }

    let hash_result = if let Some(mut h) = hasher {
        let digest  = h.finish()?;
        Some(hex::encode(digest))
    } else {
        None
    };

    Ok((bytes_read, hash_result))
}



pub async fn get_file(config: Arc<DownloadConfig>, file_info: Arc<FileInfo>, client: &HyperClient, canonical_uri: &String, query_uri: &String, req_sem: Arc<Semaphore>, disk_write_tx: Sender<WriteMsg>, cc_sender: UnboundedSender<CommandCenterEvent>) -> Result<Option<String>, SnowCopyError> {
    let mut header_map : HashMap<String, String> = HashMap::new();
    header_map.insert(signer::get_amz_content_header(AWS_ALGORITHM), EMPTY_PAYLOAD_SHA256.to_string());

    let req = create_and_sign_req(config.clone(), canonical_uri, query_uri,  None, Method::GET, &header_map)?;

    let req_permit = req_sem.acquire().await.expect("Should never fail");
    let resp = send_request(client, req, cc_sender, config.verbose).await?;

    let status = resp.status();
    if !status.is_success() {
        let s3_err = map_s3_error(resp).await;
        return Err(s3_err.into());
    }

    let mut body = resp.into_body();
    let path = file_info.file_path.clone();

    // Open file once
    let file = File::create(&path)?;
    let file_arc = Arc::new(file);

    let tracker = Arc::new(FileCompletionTracker {
        pending_bytes_to_be_written: std::sync::atomic::AtomicU64::new(file_info.file_size),
        mtime: file_info.modified_time,
        total_size: file_info.file_size,
    });

    let (_bytes_read, hash_value) = download_body_to_buffer_and_offload_to_disk_writers(0, file_arc.clone(), disk_write_tx, &mut body, tracker, file_info.file_size as usize, IntegrityAlgorithm::SHA256).await?;

    drop(req_permit);

    Ok(hash_value)
}