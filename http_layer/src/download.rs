use crate::{create_and_sign_req, map_s3_error, signer, PoolClient};
use anyhow::Error;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::Method;
use std::cmp;
use std::collections::HashMap;
use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedSender};
use tokio::sync::Semaphore;
use tokio::task;
use util_layer::cc_util::{CcLoadAndTransfer, CommandCenterEvent};
use util_layer::constants::{AWS_ALGORITHM, DOWNLOAD_CHUNK_SIZE, DOWNLOAD_DIRECT_WRITE_FRAME_THRESHOLD, EMPTY_PAYLOAD_SHA256};
use util_layer::download::{FileCompletionTracker, WriteMsg};
use util_layer::{FileInfo, SnowContext};
use crate::integrity::IntegrityAlgorithm;

pub async fn download_file_range(start: u64, end: u64, snow_context: Arc<SnowContext>, pool_client: &Arc<PoolClient>, req_sem: Arc<Semaphore>, canonical_uri: Arc<String>, query_uri: &String, fd : Arc<File>, disk_write_tx: Sender<WriteMsg>, tracker: Arc<FileCompletionTracker>, hash_algo : IntegrityAlgorithm) -> std::result::Result<(u64, Option<String>), Error> {
    let range = format!("bytes={}-{}", start, end);
    let mut header_map : HashMap<String, String> = HashMap::new();
    header_map.insert(signer::get_amz_content_header(AWS_ALGORITHM), EMPTY_PAYLOAD_SHA256.to_string());
    header_map.insert("range".to_string(), range);

    let req = create_and_sign_req(snow_context.clone(), canonical_uri.clone(), query_uri, None, Method::GET, &header_map)?;

    let req_permit = req_sem.acquire().await.expect("Should never fail");
    let resp = pool_client.request(req).await?;

    let status = resp.status();
    if !status.is_success() && status != hyper::StatusCode::PARTIAL_CONTENT {
        let s3_err = map_s3_error(resp).await;
        drop(req_permit);
        return Err(s3_err.into());
    }

    let mut body = resp.into_body();

    let (bytes_read, hash) = download_body_to_buffer_and_offload_to_disk_writers(start, fd, disk_write_tx, &mut body, tracker, (end - start + 1) as usize, hash_algo).await?;

    if bytes_read != end - start + 1 {
        return Err(anyhow::anyhow!("Partial download failed. Expected {} bytes, got {}", end - start + 1, bytes_read));
    }

    drop(req_permit);
    Ok((bytes_read, hash))
}

async fn download_body_to_buffer_and_offload_to_disk_writers(start: u64, fd: Arc<File>, disk_write_tx: Sender<WriteMsg>, body: &mut Incoming, tracker: Arc<FileCompletionTracker>, file_size: usize, hash_algo : IntegrityAlgorithm) -> Result<(u64, Option<String>), Error> {
    let mut bytes_read = 0u64;

    let mut buffer = Vec::with_capacity(cmp::min(DOWNLOAD_CHUNK_SIZE, file_size));
    let mut buffer_start = start;

    let mut hasher = match hash_algo {
        IntegrityAlgorithm::MD5 => Some(openssl::hash::Hasher::new(openssl::hash::MessageDigest::md5()).map_err(|e| Error::from(e))?),
        IntegrityAlgorithm::SHA256 => Some(openssl::hash::Hasher::new(openssl::hash::MessageDigest::sha256()).map_err(|e| Error::from(e))?),
        IntegrityAlgorithm::None => None,
    };

    while let Some(frame) = body.frame().await {
        let frame = frame?;

        if let Ok(data) = frame.into_data() {
            if let Some(ref mut h) = hasher {
                h.update(&data).map_err(|e| Error::from(e))?;
            }

            //Large enough tcp frame that we can just pass it directly to disk writers
            if data.len() >= DOWNLOAD_DIRECT_WRITE_FRAME_THRESHOLD { //we can save time by not copying data into buffer
                if !buffer.is_empty() {
                    //flush current buffer before new frame directly to disk writers
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
        let digest = h.finish().map_err(|e| Error::from(e))?;
        Some(hex::encode(digest))
    } else {
        None
    };

    Ok((bytes_read, hash_result))
}

#[cfg(unix)]
fn write_at_offset(file: &File, data: &[u8], offset: u64) -> std::io::Result<usize> {
    file.write_at(data, offset)
}

#[cfg(windows)]
fn write_at_offset(file: &File, data: &[u8], offset: u64) -> std::io::Result<usize> {
    file.seek_write(data, offset)
}

pub async fn get_file(ctx: Arc<SnowContext>, file_info: Arc<FileInfo>, client: &PoolClient, canonical_uri: Arc<String>, query_uri: &String, req_sem: Arc<Semaphore>, disk_write_tx: Sender<WriteMsg>, cc_sender: UnboundedSender<CommandCenterEvent>) -> Result<Option<String>, Error> {
    let mut header_map : HashMap<String, String> = HashMap::new();
    header_map.insert(signer::get_amz_content_header(AWS_ALGORITHM), EMPTY_PAYLOAD_SHA256.to_string());

    let req = create_and_sign_req(ctx, canonical_uri, query_uri,  None, Method::GET, &header_map)?;

    let req_permit = req_sem.acquire().await.expect("Should never fail");
    let resp = client.request(req).await?;

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

pub fn spawn_global_writers(
    rx: Receiver<WriteMsg>,
    num_writers: usize,
    cc_sender: UnboundedSender<CommandCenterEvent>,
) {
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    for _ in 0..num_writers {
        let rx = rx.clone();
        let cc_sender = cc_sender.clone();

        task::spawn_blocking(move || {
            loop {
                let msg = {
                    let mut rx_lock = rx.blocking_lock();
                    rx_lock.blocking_recv()
                };

                match msg {
                    Some((file, offset, data, tracker)) => {
                        if let Err(e) = write_at_offset(&file, &data, offset) {
                            eprintln!("write error: {}", e); //TODO JON: Should we just retry? But what kind of local IO error would benefit from that?
                            //probably panic?
                        } else {
                            tracker.complete_write(&file, data.len(), cc_sender.clone());

                            let transfer_metrics = CcLoadAndTransfer { //TODO JON: cleaner metrics. Perhaps optional.
                                amount: data.len() as u64,
                                file_sem_work_time_secs: 0.0,
                                file_sem_acquire_time_secs: 0.0,
                                req_sem_work_time_secs: 0.0,
                                req_sem_acquire_time_secs: 0.0,
                            };

                            cc_sender.send(CommandCenterEvent::PartTransferred(transfer_metrics)).unwrap();

                        }
                    }
                    None => break,
                }
            }
        });
    }
}
