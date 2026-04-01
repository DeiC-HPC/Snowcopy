mod logging;
mod signer;

use crate::signer::CredentialScope;
use anyhow::Result;
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper::{Method, Request, Uri};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::io::ReaderStream;
use util_layer::cc::CcLoadAndTransfer;
use util_layer::constants::{AWS_ALGORITHM, EMPTY_PAYLOAD_SHA256};
use util_layer::{FileInfo, SnowContext};

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
    drop(permit);

    let status = resp.status();

    //drain body so the connection can be returned to the pool.
    resp.into_body().collect().await?;

    if !status.is_success() {
        anyhow::bail!("PUT failed: {}", status);
    }

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
    let permit = req_sem.acquire().await?;
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

    // Drain body so the connection can be returned to the pool.
    resp.into_body().collect().await?;

    if !status.is_success() || etag.is_none() {
        anyhow::bail!("PUT range failed: {}", status);
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
    // Ensure the buffer is large enough if amount > chunk_size, 
    // though in this app amount is expected to be <= CHUNK_SIZE.
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

    // We use a custom struct to return the buffer to the pool when dropped
    let body_buffer = PooledBuffer {
        buffer: Some(buffer),
        pool: buffer_pool.clone(),
        len: amount as usize,
    };

    let bytes = Bytes::from_owner(body_buffer);

    let req = create_and_sign_req(ctx, canonical_uri, query_uri, amount, &bytes, sha256_hex, mtime_str)?;

    Ok((req, file_work_elapsed, file_permit_elapsed))
}

pub async fn get_file(ctx: Arc<SnowContext>, file_info: Arc<FileInfo>, client: &PoolClient, canonical_uri: Arc<String>, query_uri: &String, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>) -> Result<()> {
    let req = create_and_sign_get_req(ctx, canonical_uri, query_uri, None)?;

    let req_permit = req_sem.acquire().await?;
    let resp = client.request(req).await?;


    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("GET failed: {}", status);
    }

    let file_permit = file_sem.acquire().await?;
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

    let mut join_set = JoinSet::new();

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
            let req = create_and_sign_get_req(ctx, canonical_uri, &query_uri, Some(sub_range))?;
            let _permit = req_sem.acquire().await?;
            let resp = client.request(req).await?;

            let status = resp.status();
            if !status.is_success() && status != hyper::StatusCode::PARTIAL_CONTENT {
                anyhow::bail!("GET sub-range failed: {}", status);
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
            Ok(())
        });
    }

    while let Some(res) = join_set.join_next().await {
        res.map_err(|e| anyhow::anyhow!("Task join error: {}", e))??;
    }

    Ok(PooledBuffer {
        buffer: Some(buffer),
        pool: buffer_pool,
        len: total_range_size,
    })
}

//TODO Jon: this should be combined with create_and_sign_req to some extent. There is som code duplication.
fn create_and_sign_get_req(ctx: Arc<SnowContext>, canonical_uri: Arc<String>, query_uri: &String, range: Option<String>) -> Result<Request<StreamingBody>> {
    let credential_scope = CredentialScope {
        timestamp: chrono::Utc::now(),
        region: "us-east-1".to_string(),
        service: "s3".to_string(),
    };

    let mut signed_headers = HashMap::new();
    signed_headers.insert("host".to_string(), ctx.s3_host().to_string());
    signed_headers.insert("x-amz-date".to_string(), credential_scope.timestamp.format("%Y%m%dT%H%M%SZ").to_string());
    signed_headers.insert(
        signer::get_amz_content_header(AWS_ALGORITHM),
        EMPTY_PAYLOAD_SHA256.to_string(),
    );
    if let Some(ref r) = range {
        signed_headers.insert("range".to_string(), r.clone());
    }

    let auth_header = signer::get_s3_auth_header(
        Method::GET.as_str(),
        ctx.clone(),
        &credential_scope,
        AWS_ALGORITHM,
        &signed_headers,
        canonical_uri.clone(),
        &query_uri,
    );

    let uri = if query_uri.is_empty() {
        format!("https://{}{}", ctx.s3_host(), canonical_uri)
    } else {
        format!("https://{}{}?{}", ctx.s3_host(), canonical_uri, query_uri)
    };

    let mut req_builder = Request::builder()
        .method(Method::GET)
        .uri(uri);

    for (header_name, header_value) in &signed_headers {
        req_builder = req_builder.header(header_name, header_value);
    }

    let body: StreamingBody = http_body_util::Empty::new()
        .map_err(|inf: Infallible| match inf {})
        .boxed();

    let req = req_builder
        .header("authorization", auth_header)
        .body(body)?;

    Ok(req)
}

fn create_and_sign_req(ctx: Arc<SnowContext>, canonical_uri: Arc<String>, query_uri: &String, amount: u64, bytes_for_hash: &Bytes, sha256_hex: String, mtime: Option<String>) -> Result<Request<StreamingBody>> {
/*
    I want to delete this. However, the problem is that if we want to make some checksum calculation of
    the entire multipart file, then we need to provide md5, such that we can check that big file's
    Etag is a md5 hash of all the part's md5 hashes.
    TODO Jon: This should be a parameter option.

    let mut hasher = Hasher::new(MessageDigest::md5())?;
    hasher.update(&bytes_for_hash)?;
    let digest = hasher.finish()?;
    let md5_hash = general_purpose::STANDARD.encode(digest);
*/
    let body: StreamingBody = Full::new(bytes_for_hash.clone())
        .map_err(|inf: Infallible| match inf {})
        .boxed();

    let credential_scope = CredentialScope {
        timestamp: chrono::Utc::now(),
        region: "us-east-1".to_string(),
        service: "s3".to_string(),
    };


    let mut signed_headers = HashMap::new(); //TODO Jon: usr &str here instead of strings.
    signed_headers.insert("host".to_string(), ctx.s3_host().to_string());
    signed_headers.insert("content-type".to_string(), "application/octet-stream".to_string());
    signed_headers.insert("content-length".to_string(), amount.to_string());
    //signed_headers.insert("content-md5".to_string(), md5_hash.clone());
    signed_headers.insert("x-amz-date".to_string(), credential_scope.timestamp.format("%Y%m%dT%H%M%SZ").to_string());
    signed_headers.insert(
        signer::get_amz_content_header(AWS_ALGORITHM),
        sha256_hex,
    );

    if let Some(mtime_str) = mtime {
        signed_headers.insert("x-amz-meta-mtime".to_string(), mtime_str);
    }

    let auth_header = signer::get_s3_auth_header(
        Method::PUT.as_str(),
        ctx.clone(),
        &credential_scope,
        AWS_ALGORITHM,
        &signed_headers,
        canonical_uri.clone(),
        &query_uri,
    );

    let uri = if query_uri.is_empty() {
        format!("https://{}{}", ctx.s3_host(), canonical_uri)
    } else {
        format!("https://{}{}?{}", ctx.s3_host(), canonical_uri, query_uri)
    };
    let mut req_builder = Request::builder()
        .method(Method::PUT)
        .uri(uri);

    for (header_name, header_value) in &signed_headers {
        req_builder = req_builder.header(header_name, header_value);
    }

    let req = req_builder
        .header("authorization", auth_header)
        .body(body)?;
    Ok(req)
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

pub async fn send_with_logging(
    client: &crate::PoolClient,
    req: hyper::Request<crate::StreamingBody>,
) -> Result<hyper::Response<hyper::body::Incoming>> {
    logging::log_request_headers(&req);
    let resp = client.request(req).await?;
    logging::log_response_headers(&resp);
    Ok(resp)
}