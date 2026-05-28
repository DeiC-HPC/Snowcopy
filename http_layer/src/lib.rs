mod logging;
mod signer;

use crate::signer::{sha256_hex, ChunkSigner, CredentialScope};
use anyhow::Result;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Body, Frame};
use hyper::{Method, Request, Uri};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::io::Write;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::io::ReaderStream;
use util_layer::cc_util::CcLoadAndTransfer;
use util_layer::constants::{AMZ_CHECKSUM_SHA256, AMZ_CHUNK_SIGNATURE_HEADER, AMZ_STREAMING_TRAILER, AMZ_TRAILER_SIGNATURE, AWS_ALGORITHM, CHUNKSIGNATURE_LENGTH, EMPTY_PAYLOAD_SHA256};
use util_layer::snow_errors::{S3Error, SnowCopyError};
use util_layer::{FileInfo, SnowContext, TrailingInfo};

const READ_BUFFER_SIZE: usize = 256 * 1024;
pub struct BufferPool {
    semaphore: Semaphore,
    buffers: Mutex<Vec<Vec<u8>>>,
    chunk_size: usize,
}

impl BufferPool {
    pub fn new(total_size: usize, chunk_size: usize) -> Self {
        let count = total_size / chunk_size;
        let mut buffers = Vec::with_capacity(count);
        for _ in 0..count {
            buffers.push(vec![0u8; chunk_size]);
        }
        Self {
            semaphore: Semaphore::new(count),
            buffers: Mutex::new(buffers),
            chunk_size,
        }
    }

    pub async fn get(&self) -> Vec<u8> {
        let _permit = self.semaphore.acquire().await.unwrap();
        // We forget the permit because we will release it when returning the buffer
        _permit.forget();
        let mut buffers = self.buffers.lock().await;
        buffers.pop().unwrap_or_else(|| vec![0u8; self.chunk_size])
    }

    pub async fn return_buffer(&self, buffer: Vec<u8>) {
        let mut buffers = self.buffers.lock().await;
        buffers.push(buffer);
        self.semaphore.add_permits(1);
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

// Public type aliases so app can use the exact type
pub type Https = hyper_rustls::HttpsConnector<HttpConnector>;
pub type StreamingBody = BoxBody<Bytes, std::io::Error>;
pub type PoolClient = Client<Https, StreamingBody>;

pub fn build_https_client() -> PoolClient {
    use hyper_rustls::HttpsConnectorBuilder;

    let mut http = HttpConnector::new();
    http.enforce_http(false); // allow https
    http.set_nodelay(true);

    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .expect("no native root CA certificates found")
        .https_only()
        .enable_http1()
        //.enable_http2()
        .wrap_connector(http);

    Client::builder(TokioExecutor::new()).build(https)
}

pub async fn put_file(ctx: Arc<SnowContext>, file_info: Arc<FileInfo>, client: &PoolClient, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, canonical_uri : Arc<String>, query_uri : &String) -> Result<CcLoadAndTransfer> {
    let (req, file_work_elapsed, file_permit_elapsed) = create_in_memory_body_put_with_hashes(0, None, ctx, file_info.clone(), file_sem, buffer_pool, canonical_uri, query_uri).await?;

    let req_permit_start = Instant::now();
    let permit = req_sem.acquire().await?;
    let req_permit_elapsed = req_permit_start.elapsed();

    let req_work_start = Instant::now();
    let resp = client.request(req).await?;
    let req_work_elapsed = req_work_start.elapsed();

    let status = resp.status();

    if !status.is_success() {
        let s3_err = map_s3_error(resp).await;
        drop(permit);
        return Err(s3_err.into());
    }

    //drain body so the connection can be returned to the pool.
    resp.into_body().collect().await?;
    drop(permit);

    let transfer_metrics = CcLoadAndTransfer {
        amount : file_info.file_size,
        file_sem_work_time_secs: file_work_elapsed.as_secs_f64(),
        file_sem_acquire_time_secs: file_permit_elapsed.as_secs_f64(),
        req_sem_work_time_secs: req_work_elapsed.as_secs_f64(),
        req_sem_acquire_time_secs: req_permit_elapsed.as_secs_f64(),
    };
    Ok(transfer_metrics)
}

pub async fn put_file_range(ctx: Arc<SnowContext>, file_info: Arc<FileInfo>, from: u64, amount: u64, client: &PoolClient, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, canonical_uri: Arc<String>, query_uri: &String) -> Result<(String, CcLoadAndTransfer)> {
    let (req, file_work_elapsed, file_permit_elapsed) = create_in_memory_body_put_with_hashes(from, Some(amount), ctx, file_info, file_sem, buffer_pool, canonical_uri, query_uri).await?;

    let req_permit_start = Instant::now();
    let permit = req_sem.acquire().await.expect("Should never fail");
    let req_permit_elapsed = req_permit_start.elapsed();

    let req_work_start = Instant::now();
    let resp = client.request(req).await?; //send_with_logging(client, req).await?;
    let req_work_elapsed = req_work_start.elapsed();
    drop(permit);

    let status = resp.status();
    let etag = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if !status.is_success() {
        let s3_err = map_s3_error(resp).await;
        return Err(s3_err.into());
    }

    // Drain body so the connection can be returned to the pool.
    resp.into_body().collect().await?;

    if etag.is_none() {
        return Err(SnowCopyError::MissingRequiredHeader("etag").into());
    }

    let transfer_metrics = CcLoadAndTransfer {
        amount,
        file_sem_work_time_secs: file_work_elapsed.as_secs_f64(),
        file_sem_acquire_time_secs: file_permit_elapsed.as_secs_f64(),
        req_sem_work_time_secs: req_work_elapsed.as_secs_f64(),
        req_sem_acquire_time_secs: req_permit_elapsed.as_secs_f64(),
    };

    Ok((etag.unwrap(), transfer_metrics))
}

pub async fn put_file_streaming(ctx: Arc<SnowContext>, file_info: Arc<FileInfo>, client: &PoolClient, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, canonical_uri: Arc<String>, query_uri: &String, trailing_info: TrailingInfo) -> Result<CcLoadAndTransfer> {
    let file_permit_start = Instant::now();
    let open_file_lock = trailing_info.open_file_sem_limit.acquire().await?;
    let file_permit_elapsed = file_permit_start.elapsed();

    let file_work_start = Instant::now();
    let file = File::open(&*file_info.file_path).await?;

    let req = create_streaming_req_trailing_checksum(
        ctx.clone(),
        canonical_uri.clone(),
        query_uri,
        file,
        file_info.file_size,
        file_sem,
        trailing_info.trailing_chunk_size,
    )?;

    let file_work_elapsed = file_work_start.elapsed();

    let req_permit_start = Instant::now();
    let permit = req_sem.acquire().await?;
    let req_permit_elapsed = req_permit_start.elapsed();

    let req_work_start = Instant::now();
    let resp = client.request(req).await?;
    let req_work_elapsed = req_work_start.elapsed();

    drop(permit);
    drop(open_file_lock);

    let status = resp.status();

    if !status.is_success() {
        let s3_err = map_s3_error(resp).await;
        return Err(s3_err.into());
    }

    resp.into_body().collect().await?;

    let transfer_metrics = CcLoadAndTransfer {
        amount : file_info.file_size,
        file_sem_work_time_secs: file_work_elapsed.as_secs_f64(),
        file_sem_acquire_time_secs: file_permit_elapsed.as_secs_f64(),
        req_sem_work_time_secs: req_work_elapsed.as_secs_f64(),
        req_sem_acquire_time_secs: req_permit_elapsed.as_secs_f64(),
    };
    Ok(transfer_metrics)
}

/// Upload a file range using streaming for trailing headers.
pub async fn put_file_range_streaming(
    ctx: Arc<SnowContext>,
    file_info: Arc<FileInfo>,
    from: u64,
    amount: u64,
    client: &PoolClient,
    file_sem: Arc<Semaphore>,
    req_sem: Arc<Semaphore>,
    canonical_uri: Arc<String>,
    query_uri: &String,
    trailing_info: TrailingInfo
) -> Result<(String, CcLoadAndTransfer)> {
    let file_permit_start = Instant::now();
    let open_file_lock = trailing_info.open_file_sem_limit.acquire().await?;
    let file_permit_elapsed = file_permit_start.elapsed();

    let file_work_start = Instant::now();
    let mut file = File::open(&*file_info.file_path).await?;

    if from > 0 {
        file.seek(std::io::SeekFrom::Start(from)).await?;
    }

    let req = create_streaming_req_trailing_checksum(
        ctx.clone(),
        canonical_uri.clone(),
        query_uri,
        file,
        amount,
        file_sem,
        trailing_info.trailing_chunk_size
    )?;

    let file_work_elapsed = file_work_start.elapsed();

    let req_permit_start = Instant::now();
    let permit = req_sem.acquire().await?;
    let req_permit_elapsed = req_permit_start.elapsed();

    let req_work_start = Instant::now();
    let resp = client.request(req).await?;
    let req_work_elapsed = req_work_start.elapsed();

    drop(permit);
    drop(open_file_lock);

    let status = resp.status();
    let etag = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if !status.is_success() {
        let s3_err = map_s3_error(resp).await;
        return Err(s3_err.into());
    }

    resp.into_body().collect().await?;

    if etag.is_none() {
        return Err(SnowCopyError::MissingRequiredHeader("etag").into());
    }

    Ok((etag.unwrap(), CcLoadAndTransfer {
        amount,
        file_sem_work_time_secs: file_work_elapsed.as_secs_f64(),
        file_sem_acquire_time_secs: file_permit_elapsed.as_secs_f64(),
        req_sem_work_time_secs: req_work_elapsed.as_secs_f64(),
        req_sem_acquire_time_secs: req_permit_elapsed.as_secs_f64(),
    }))
}

#[allow(dead_code)]
async fn create_streaming_body_put(amount: u64, uri: Uri, file: File) -> hyper::http::Result<Request<StreamingBody>> { //TODO Jon: use for trailer
    // Take only the bytes in the range [from, to]
    let limited_reader = file.take(amount);

    // Convert to a stream of Frames
    let byte_stream = ReaderStream::with_capacity(limited_reader, READ_BUFFER_SIZE).map_ok(Frame::data);
    let body: StreamingBody = StreamBody::new(byte_stream).boxed();

    Request::builder()
        .method(Method::PUT)
        .uri(uri)
        .header("content-type", "application/octet-stream")
        .header("content-length", amount.to_string())
        .body(body)
}

async fn create_in_memory_body_put_with_hashes(start: u64, amount_option: Option<u64>, ctx: Arc<SnowContext>, file_info: Arc<FileInfo>, file_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, canonical_uri: Arc<String>, query_uri: &String) -> Result<(Request<StreamingBody>, std::time::Duration, std::time::Duration)> {
    let file_permit_start = Instant::now();
    let file_lock = file_sem.acquire().await?;
    let file_permit_elapsed = file_permit_start.elapsed();

    let file_work_start = Instant::now();
    let mut file = File::open(&*file_info.file_path).await?;

    let amount = match amount_option {
        Some(v) => v,
        None => file.metadata().await?.len(),
    };

    if start > 0 {
        file.seek(std::io::SeekFrom::Start(start)).await?;
    }

    let mut buffer = buffer_pool.get().await;
    if buffer.len() < amount as usize {
        buffer.resize(amount as usize, 0u8);
    }

    let mut total_read = 0usize;
    let chunk_size = READ_BUFFER_SIZE.min(amount as usize);

    let mut hasher = signer::create_sha256_hasher();

    while total_read < amount as usize {
        let remaining = amount as usize - total_read;
        let to_read = remaining.min(chunk_size);
        let chunk = &mut buffer[total_read..total_read + to_read];

        file.read_exact(chunk).await?;
        hasher.update(chunk).expect("Failed to update SHA256 hasher");
        total_read += to_read;
    }
    drop(file);
    let file_work_elapsed = file_work_start.elapsed();
    drop(file_lock);

    let sha256_hex = signer::finish_sha256_hex(hasher);

    let mtime_str = file_info.modified_time.map(util_layer::time::time_to_float_string);

    let body_buffer = PooledBuffer {
        buffer: Some(buffer),
        pool: buffer_pool.clone(),
        len: amount as usize,
    };

    let mut header_map: HashMap<String, String> = HashMap::new();
    header_map.insert("content-type".to_string(), "application/octet-stream".to_string());
    header_map.insert("content-length".to_string(), amount.to_string());
    header_map.insert(
        signer::get_amz_content_header(AWS_ALGORITHM),
        sha256_hex,
    );
    if let Some(mtime_str) = mtime_str {
        header_map.insert("x-amz-meta-mtime".to_string(), mtime_str);
    }

    let bytes = Bytes::from_owner(body_buffer);
    let req = create_and_sign_req(ctx, canonical_uri, query_uri, Some(&bytes), Method::PUT, &header_map)?;

    Ok((req, file_work_elapsed, file_permit_elapsed))
}

pub async fn get_file(ctx: Arc<SnowContext>, file_info: Arc<FileInfo>, client: &PoolClient, canonical_uri: Arc<String>, query_uri: &String, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>) -> Result<()> {
    let mut header_map : HashMap<String, String> = HashMap::new();
    header_map.insert(signer::get_amz_content_header(AWS_ALGORITHM), EMPTY_PAYLOAD_SHA256.to_string());

    let req = create_and_sign_req(ctx, canonical_uri, query_uri, None, Method::GET, &header_map)?;

    let req_permit = req_sem.acquire().await.expect("Should never fail");
    let resp = client.request(req).await?;
    
    let status = resp.status();
    if !status.is_success() {
        let s3_err = map_s3_error(resp).await;
        return Err(s3_err.into());
    }

    let file_permit = file_sem.acquire().await.expect("Should never fail");
    let mut file = File::create(&file_info.file_path).await?;
    let mut body = resp.into_body();

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| anyhow::anyhow!("Body error: {}", e))?;
        if let Some(data) = frame.data_ref() {
            file.write_all(data).await?;
        }
    }
    file.flush().await?;
    drop(file_permit);
    drop(req_permit);

    Ok(())
}

pub async fn get_file_range(ctx: Arc<SnowContext>, client: &PoolClient, buffer_pool: Arc<BufferPool>, canonical_uri: Arc<String>, query_uri: &String, req_sem: Arc<Semaphore>, start: u64, end: u64) -> Result<PooledBuffer> {
    let total_range_size = (end - start + 1) as usize;
    let mut buffer = buffer_pool.get().await;
    let buffer_ptr = buffer.as_mut_ptr() as usize;

    // Split into 4 sub-requests
    let num_sub_parts = 4; //TODO Jon: configurable/change dynamically?
    let sub_part_size = (total_range_size + num_sub_parts - 1) / num_sub_parts;

    let mut join_set: JoinSet<Result<()>> = JoinSet::new();

    for i in 0..num_sub_parts {
        let sub_start = start + (i * sub_part_size) as u64;
        if sub_start > end {
            break;
        }
        let sub_end = std::cmp::min(sub_start + sub_part_size as u64 - 1, end);
        let sub_range = format!("bytes={}-{}", sub_start, sub_end);

        let ctx = ctx.clone();
        let client = client.clone();
        let canonical_uri = canonical_uri.clone();
        let query_uri = query_uri.clone();
        let req_sem = req_sem.clone();

        join_set.spawn(async move {
            let mut header_map : HashMap<String, String> = HashMap::new();
            header_map.insert(signer::get_amz_content_header(AWS_ALGORITHM), EMPTY_PAYLOAD_SHA256.to_string());
            header_map.insert("range".to_string(), sub_range);

            let req = create_and_sign_req(ctx, canonical_uri, &query_uri, None, Method::GET, &header_map)?;
            let _permit = req_sem.acquire().await.expect("Should never fail");
            let resp = client.request(req).await?;

            let status = resp.status();

            if !status.is_success() && status != hyper::StatusCode::PARTIAL_CONTENT {
                let s3_err = map_s3_error(resp).await;
                return Err(s3_err.into());
            }

            let mut body = resp.into_body();
            let mut sub_data_len = 0usize;
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|e| anyhow::anyhow!("Body error: {}", e))?;
                if let Some(data) = frame.data_ref() {
                    let offset = (sub_start - start) as usize + sub_data_len;
                    // SAFETY: each spawn writes to a unique non-overlapping range in the buffer.
                    //TODO JON: This is something we should consider carefully. But I think the functionality is nice
                    //and I trust that it is very testable. Thus we should be confident that there are no memory leaks and that
                    // we don't write into wrong memory.
                    //Ideally we can rewrite the code in some way where there is no copying. Perhaps just do 1MB slices?
                    //Dunno, having less writers running simultaneously, I think, is to our benefit. We are faster than the
                    //golang implementation where each file is split up into 5MB chunks and a goroutine is assigned the transfer
                    //and disk writing for each chunk. I like the idea here better, where one disk writer is supported by multiple
                    //networking threads
                    unsafe {
                        let dst = (buffer_ptr as *mut u8).add(offset);
                        std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
                    }
                    sub_data_len += data.len();
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(join_err) => return Err(anyhow::anyhow!("Task join error: {}", join_err)),
        }
    }

    Ok(PooledBuffer {
        buffer: Some(buffer),
        pool: buffer_pool,
        len: total_range_size,
    })
}


fn create_and_sign_req(ctx: Arc<SnowContext>, canonical_uri: Arc<String>, query_uri: &String, payload: Option<&Bytes>, method: Method, header_map: &HashMap<String, String>) -> Result<Request<StreamingBody>> {
    let credential_scope = CredentialScope {
        timestamp: chrono::Utc::now(),
        region: ctx.s3_region().to_string(),
        service: "s3".to_string(),
    };

    let mut signed_headers : HashMap<String, String> = HashMap::new();
    signed_headers.insert("host".to_string(), ctx.s3_host().to_string());
    signed_headers.insert("x-amz-date".to_string(), credential_scope.timestamp.format("%Y%m%dT%H%M%SZ").to_string());

    for (key, value) in header_map {
        signed_headers.insert(key.to_string(), value.to_string());
    }

    let auth_header = signer::get_s3_auth_header(
        method.as_str(),
        ctx.clone(),
        &credential_scope,
        AWS_ALGORITHM,
        &signed_headers,
        canonical_uri.clone(),
        &query_uri,
    );

    let uri = format_query(&ctx, canonical_uri, query_uri);

    let body : StreamingBody = match payload {
        None => {
            http_body_util::Empty::new()
                .map_err(|inf: Infallible| match inf {})
                .boxed()
        }
        Some(bytes) => {
            Full::new(bytes.clone())
                .map_err(|inf: Infallible| match inf {})
                .boxed()
        }
    };

    let mut req_builder = Request::builder()
        .method(method)
        .uri(uri);

    for (header_name, header_value) in &signed_headers {
        req_builder = req_builder.header(header_name, header_value);
    }

    let req = req_builder
        .header("authorization", auth_header)
        .body(body).expect("Failed to create request. This should never happen.");
    Ok(req)
}

fn create_streaming_req_trailing_checksum(
    ctx: Arc<SnowContext>,
    canonical_uri: Arc<String>,
    query_uri: &String,
    file: File,
    file_size: u64,
    file_sem: Arc<Semaphore>,
    chunk_size: u64,
) -> Result<Request<StreamingBody>> {
    let credential_scope = CredentialScope {
        timestamp: chrono::Utc::now(),
        region: ctx.s3_region().to_string(),
        service: "s3".to_string(),
    };

    let amz_date = credential_scope.timestamp.format("%Y%m%dT%H%M%SZ").to_string();
    let date_y_m_d = credential_scope.timestamp.format("%Y%m%d").to_string();
    let credential_scope_str = format!(
        "{}/{}/s3/aws4_request",
        date_y_m_d,
        credential_scope.region
    );

    let mut signed_headers: HashMap<String, String> = HashMap::new();
    signed_headers.insert("host".to_string(), ctx.s3_host().to_string());
    signed_headers.insert("x-amz-date".to_string(), amz_date.clone());
    signed_headers.insert("content-encoding".to_string(), "aws-chunked".to_string());
    signed_headers.insert("content-type".to_string(), "application/octet-stream".to_string());
    signed_headers.insert(
        signer::get_amz_content_header(AWS_ALGORITHM),
        AMZ_STREAMING_TRAILER.to_string(),
    );
    signed_headers.insert("x-amz-decoded-content-length".to_string(), file_size.to_string());
    signed_headers.insert("x-amz-trailer".to_string(), AMZ_CHECKSUM_SHA256.to_string());

    let encoded_len = signer::aws_chunked_trailer_encoded_length(
        file_size,
        chunk_size,
        AMZ_CHECKSUM_SHA256,
        44,
    );
    signed_headers.insert("content-length".to_string(), encoded_len.to_string());

    let (auth_header, seed_signature) = signer::get_s3_auth_header_and_signature(
        "PUT",
        ctx.clone(),
        &credential_scope,
        AWS_ALGORITHM,
        &signed_headers,
        canonical_uri.clone(),
        query_uri,
    );

    let uri = format_query(&ctx, canonical_uri, query_uri);

    let signing_key = signer::s3_signing_key(
        ctx.s3_secret_key(),
        &date_y_m_d,
        ctx.s3_region(),
    );

    let streaming_body = StreamingFileChunkedBody::new(
        file,
        file_size,
        file_sem,
        signing_key,
        amz_date,
        credential_scope_str,
        seed_signature,
        AMZ_CHECKSUM_SHA256.to_string(),
        chunk_size
    );

    let body: StreamingBody = streaming_body.into_streaming_body();

    let mut req_builder = Request::builder()
        .method(Method::PUT)
        .uri(uri);

    for (header_name, header_value) in &signed_headers {
        req_builder = req_builder.header(header_name, header_value);
    }

    let req = req_builder
        .header("authorization", auth_header)
        .body(body)
        .expect("Failed to create request");

    Ok(req)
}

fn format_query(ctx: &Arc<SnowContext>, canonical_uri: Arc<String>, query_uri: &String) -> String {
    if query_uri.is_empty() {
        format!("https://{}{}", ctx.s3_host(), canonical_uri)
    } else {
        format!("https://{}{}?{}", ctx.s3_host(), canonical_uri, query_uri)
    }
}

pub struct PooledBuffer {
    pub buffer: Option<Vec<u8>>,
    pub pool: Arc<BufferPool>,
    pub len: usize,
}

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer.as_ref().expect("buffer should be present until drop")[..self.len]
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buffer.take() {
            let pool = self.pool.clone();
            tokio::spawn(async move {
                pool.return_buffer(buf).await;
            });
        }
    }
}

/// Streaming AWS Chunked body that reads directly from file.
/// Avoids buffering the entire file in memory - reads chunks on demand.
/// Used for large file uploads with trailing headers.
pub struct StreamingFileChunkedBody {
    inner: StreamingFileChunkedBodyInner,
}

/// Inner state that can be safely mutated
pub struct StreamingFileChunkedBodyInner {
    /// The raw payload data
    file: Option<Pin<Box<tokio::fs::File>>>,
    /// Remaining bytes to read from the file
    remaining: u64,
    /// Semaphore to protect file access
    file_sem: Arc<Semaphore>,
    /// Chunk signer with premade Signing key for chunk signatures
    chunk_signer: ChunkSigner,
    /// AMZ date string (format: 20060102T150405Z)
    amz_date: String,
    /// Credential scope string
    credential_scope: String,
    /// Previous signature (starts with seed signature)
    prev_signature: String,
    /// Trailer name (e.g., "x-amz-checksum-sha256")
    trailer_name: String,
    /// Running SHA256 hash of the payload
    checksum_hasher: Option<openssl::hash::Hasher>,
    /// Cached checksum
    final_checksum: Option<String>,
    /// Buffer for holding read data
    read_buf: Vec<u8>,
    /// Current read position in buf
    state: StreamingChunkedState,
    /// Chunk size (in kB)
    chunk_size: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum StreamingChunkedState {
    /// Emitting data chunks
    Reading,
    /// Emitting the final chunk
    FinalChunk,
    /// Emitting trailing headers
    Trailers,
    /// Done
    Done,
}

impl StreamingFileChunkedBody {
    pub fn new(
        file: File,
        file_size: u64,
        file_sem: Arc<Semaphore>,
        signing_key: [u8; 32],
        amz_date: String,
        credential_scope: String,
        seed_signature: String,
        trailer_name: String,
        chunk_size: u64,
    ) -> Self {
        Self {
            inner: StreamingFileChunkedBodyInner {
                file: Some(Box::pin(file)),
                remaining: file_size,
                file_sem,
                chunk_signer: ChunkSigner::new(&signing_key),
                amz_date,
                credential_scope,
                prev_signature: seed_signature,
                trailer_name,
                checksum_hasher: Some(signer::create_sha256_hasher()),
                final_checksum: None,
                read_buf: Vec::with_capacity((chunk_size + 256) as usize),
                state: StreamingChunkedState::Reading,
                chunk_size,
            }
        }
    }

    pub fn into_streaming_body(self) -> StreamingBody {
        BodyExt::boxed(self)
    }

    fn extract_bytes_read_digits(bytes_read: usize) -> usize {
        match bytes_read {
            0..=0xF => 1,
            0..=0xFF => 2,
            0..=0xFFF => 3,
            0..=0xFFFF => 4,
            0..=0xF_FFFF => 5,
            0..=0xFF_FFFF => 6,
            0..=0xFFF_FFFF => 7,
            0..=0xFFFF_FFFF => 8,
            0..=0xF_FFFF_FFFF => 9,
            _ => 10,    // TODO LND: What is the max digit size for a max chunk size?
        }
    }
}

impl StreamingFileChunkedBodyInner {

    fn format_data_chunk(&mut self, data_index: usize, bytes_read: usize) {

        let chunk_data = &self.read_buf[data_index..data_index + bytes_read];

        let chunk_hash = sha256_hex(chunk_data);

        let chunk_sig = self.chunk_signer.sign_chunk(
            &self.amz_date,
            &self.credential_scope,
            &self.prev_signature,
            chunk_hash.as_str(),
        );

        // Prepend 0..data_index
        write!(
            &mut self.read_buf[0..],
            "{:x};{}={}\r\n",
            bytes_read,
            AMZ_CHUNK_SIGNATURE_HEADER,
            &chunk_sig
        ).unwrap();
        
        // Append data_index+bytes_read..EOF
        self.read_buf[data_index + bytes_read] = b'\r';
        self.read_buf[data_index + bytes_read + 1] = b'\n';
        self.prev_signature = chunk_sig;
    }

    fn format_final_chunk(&mut self) {
        // Finalize the checksum now, before we need it for trailers
        if let Some(mut hasher) = self.checksum_hasher.take() {
            let digest = hasher.finish().expect("Failed to finish hasher");
            self.final_checksum = Some(BASE64_STANDARD.encode(&digest));
        }

        let empty_sig = self.chunk_signer.sign_chunk(
            &self.amz_date,
            &self.credential_scope,
            &self.prev_signature,
            EMPTY_PAYLOAD_SHA256,
        );
        self.prev_signature = empty_sig.clone();

        write!(
            &mut self.read_buf,
            "0;{}={}\r\n\r\n",
            AMZ_CHUNK_SIGNATURE_HEADER,
            empty_sig
        ).unwrap();
    }

    fn format_trailers(&mut self) {
        let checksum_b64 = self.final_checksum.as_ref()
            .expect("Checksum should have been computed in format_final_chunk");

        let trailer_sig = self.chunk_signer.sign_trailer(
            &self.amz_date,
            &self.credential_scope,
            &self.prev_signature,
            &self.trailer_name,
            checksum_b64,
        );

        write!(
            &mut self.read_buf,
            "{}:{}\r\n{}:{}\r\n\r\n",
            self.trailer_name.to_lowercase(),
            checksum_b64,
            AMZ_TRAILER_SIGNATURE,
            trailer_sig
        ).unwrap();
    }
}

impl Unpin for StreamingFileChunkedBody {}

impl Body for StreamingFileChunkedBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let inner = &mut self.inner;

        loop {  //TODO LND: Find out a way to speed up the loops (less time waiting)
            match inner.state {
                StreamingChunkedState::Reading => {
                    inner.read_buf.clear();
                    if inner.remaining == 0 {
                        inner.state = StreamingChunkedState::FinalChunk;
                        continue;
                    }

                    // Get the file - we need to pin it for poll_read
                    let file_lock = inner.file_sem.try_acquire();
                    if !file_lock.is_ok() {
                        return Poll::Pending;
                    }
                    let file = inner.file.as_mut().expect("file should exist").as_mut();

                    let to_read = std::cmp::min(inner.chunk_size, inner.remaining) as usize;
                    let to_read_digits = Self::extract_bytes_read_digits(to_read);
                    let data_index = to_read_digits + CHUNKSIGNATURE_LENGTH;            // "<data_index>;chunk-signature=<FixedLengthChunkSignature>\r\n"
                    inner.read_buf.resize(data_index + to_read + 2, 0);     // Resize to ChunkSignature + Data length + "\r\n"
                    let buf = &mut inner.read_buf[data_index..data_index + to_read];    // Buf is offset to reserve space for chunk signature
                    let mut read_buf = tokio::io::ReadBuf::new(buf);

                    match file.poll_read(cx, &mut read_buf) {
                        Poll::Ready(Ok(())) => {
                            let bytes_read = read_buf.filled().len();
                            drop(file_lock);

                            if bytes_read != to_read {
                                panic!("Expected to read {} bytes, but read {} bytes", to_read, bytes_read);
                            }

                            if bytes_read == 0 {
                                // EOF
                                inner.state = StreamingChunkedState::FinalChunk;
                                continue;
                            }

                            inner.checksum_hasher.as_mut().expect("Hasher should have been created").update(read_buf.filled()).expect("Failed to update SHA256 hasher");

                            inner.remaining -= bytes_read as u64;
                            inner.format_data_chunk(data_index, bytes_read);

                            let data = std::mem::take(&mut inner.read_buf);
                            return Poll::Ready(Some(Ok(Frame::data(Bytes::from(data)))));
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }

                StreamingChunkedState::FinalChunk => {
                    inner.format_final_chunk();
                    inner.state = StreamingChunkedState::Trailers;
                    let data = std::mem::take(&mut inner.read_buf);
                    return Poll::Ready(Some(Ok(Frame::data(Bytes::from(data)))));
                }

                StreamingChunkedState::Trailers => {
                    inner.format_trailers();
                    inner.state = StreamingChunkedState::Done;
                    let data = std::mem::take(&mut inner.read_buf);
                    return Poll::Ready(Some(Ok(Frame::data(Bytes::from(data)))));
                }

                StreamingChunkedState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

pub async fn send_with_logging(
    client: &crate::PoolClient,
    req: hyper::Request<crate::StreamingBody>,
) -> Result<hyper::Response<hyper::body::Incoming>> {
    logging::log_request_headers(&req);
    let resp = client.request(req).await?;
    logging::log_response_headers(&resp);
    Ok(resp)
}

pub async fn map_s3_error(resp: hyper::Response<hyper::body::Incoming>) -> SnowCopyError {
    let status = resp.status();
    let body_bytes = match resp.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => return SnowCopyError::Hyper(e),
    };

    let body_str = match String::from_utf8(body_bytes.to_vec()) {
        Ok(s) => s,
        Err(e) => return SnowCopyError::Parse(format!("Body was not valid UTF-8: {}", e)),
    };

    if let Some(s3_err) = S3Error::from_xml(&body_str) {
        SnowCopyError::S3(s3_err)
    } else {
        SnowCopyError::Http(status.as_u16())
    }
}