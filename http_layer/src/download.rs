use crate::{create_and_sign_req, map_s3_error, signer, PoolClient};
use anyhow::Error;
use http_body_util::BodyExt;
use hyper::Method;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task;
use util_layer::constants::{AWS_ALGORITHM, EMPTY_PAYLOAD_SHA256};
use util_layer::{FileInfo, SnowContext};

pub async fn download_file_range(start: u64, end: u64, snow_context: Arc<SnowContext>, pool_client: &Arc<PoolClient>, req_sem: Arc<Semaphore>, canonical_uri: Arc<String>, query_uri: &String, fd : Arc<File>) -> std::result::Result<u64, Error> {
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
    let mut bytes_written = 0u64;

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| anyhow::anyhow!("Body error: {}", e))?;

        if let Ok(data) = frame.into_data() {
            let offset = start + bytes_written;
            let data_length = data.len();

            let file = fd.clone();

            task::spawn_blocking(move || -> std::io::Result<()> {
                write_at_offset(&file, &data, offset)?;
                Ok(())
            }).await??;

            bytes_written += data_length as u64;
        }
    }

    if bytes_written != end - start + 1 {
        return Err(anyhow::anyhow!("Partial download failed. Expected {} bytes, got {}", end - start + 1, bytes_written));
    }

    drop(req_permit);
    Ok(bytes_written)
}


#[cfg(unix)]
fn write_at_offset(file: &File, data: &[u8], offset: u64) -> std::io::Result<usize> {
    file.write_at(data, offset)
}

#[cfg(windows)]
fn write_at_offset(file: &File, data: &[u8], offset: u64) -> std::io::Result<usize> {
    file.seek_write(data, offset)
}

pub async fn get_file(ctx: Arc<SnowContext>, file_info: Arc<FileInfo>, client: &PoolClient, canonical_uri: Arc<String>, query_uri: &String, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>) -> Result<(), Error> {
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

    let file_permit = file_sem.acquire().await.expect("Should never fail");

    let mut body = resp.into_body();
    let path = file_info.file_path.clone();

    // Open file once in blocking pool
    let mut file = task::spawn_blocking(move || File::create(&path))
        .await??;

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| anyhow::anyhow!("Body error: {}", e))?;

        if let Ok(data) = frame.into_data() {
            let chunk = data.to_vec();

            // Write chunk in blocking pool
            file = task::spawn_blocking(move || -> std::io::Result<File> {
                file.write_all(&chunk)?;
                Ok(file)
            })
                .await??;
        }
    }

    // Final flush and close happen in the blocking pool.
    task::spawn_blocking(move || -> std::io::Result<()> {
        file.flush()?;

        if let Some(mtime) = file_info.modified_time {
            file.set_modified(mtime)?;
        }

        drop(file);
        Ok(())
    })
        .await??;

    drop(file_permit);
    drop(req_permit);

    Ok(())
}