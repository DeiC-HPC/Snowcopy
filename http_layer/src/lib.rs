mod logging;
mod signer;

use crate::signer::{CredentialScope, S3Credentials};
use anyhow::Result;
use base64::engine::general_purpose;
use base64::Engine;
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
use openssl::hash::{Hasher, MessageDigest};
use util_layer::{FileInfo, SnowContext};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::time::Instant;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::io::ReaderStream;
use url::Url;
use util_layer::constants::AWS_ALGORITHM;
use util_layer::cc::CcLoadAndTransfer;

// Optimized buffer size for file reading (256 KB)
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

pub async fn put_file(ctx: &SnowContext, bucket: &str, file_info: FileInfo, client: &PoolClient, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>) -> Result<CcLoadAndTransfer> {
    let url = format!("{}/{}/{}", ctx.s3_url(), bucket, file_info.key).parse::<Url>()? ;
    let (req, file_work_elapsed, file_permit_elapsed) = create_in_memory_body_put_with_hashes(0, None, ctx, bucket, file_info.clone(), file_sem, buffer_pool, &url).await?;

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

pub async fn put_file_range(ctx: &SnowContext, bucket: &str, file_info: FileInfo, from: u64, amount: u64, client: &PoolClient, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, url_str: String) -> Result<(String, CcLoadAndTransfer)> {
    //let req = create_streaming_body_put(amount, uri, file).await?;
    let url :Url = url_str.parse()?;
    let (req, file_work_elapsed, file_permit_elapsed) = create_in_memory_body_put_with_hashes(from, Some(amount), ctx, bucket, file_info, file_sem, buffer_pool, &url).await?;

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

async fn create_in_memory_body_put_with_hashes(start: u64, amount_option: Option<u64>, ctx: &SnowContext, bucket: &str, file_info: FileInfo, file_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, url: &Url) -> Result<(Request<StreamingBody>, std::time::Duration, std::time::Duration)> {
    let file_permit_start = Instant::now();
    let file_lock = file_sem.acquire().await?;
    let file_permit_elapsed = file_permit_start.elapsed();

    let file_work_start = Instant::now();
    let mut file = File::open(file_info.file_path).await?;

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

    while total_read < amount as usize {
        let remaining = amount as usize - total_read;
        let to_read = remaining.min(chunk_size);
        let chunk = &mut buffer[total_read..total_read + to_read];

        file.read_exact(chunk).await?;
        total_read += to_read;
    }
    drop(file);
    let file_work_elapsed = file_work_start.elapsed();
    drop(file_lock);

    // We use a custom struct to return the buffer to the pool when dropped
    let body_buffer = PooledBuffer {
        buffer: Some(buffer),
        pool: buffer_pool.clone(),
        len: amount as usize,
    };

    let bytes = Bytes::from_owner(body_buffer);

    let bytes_for_hash = bytes.clone();     // cheap clone (shared backing storage)

    let mut hasher = Hasher::new(MessageDigest::md5())?;
    hasher.update(&bytes_for_hash)?;
    let digest = hasher.finish()?;
    let md5_hash = general_purpose::STANDARD.encode(digest);

    let body: StreamingBody = Full::new(bytes)
        .map_err(|inf: Infallible| match inf {})
        .boxed();

    let credentials = S3Credentials {
        s3_access_key: ctx.s3_access_key().to_string(),
        s3_secret_key: ctx.s3_secret_key().to_string(),
    };

    let credential_scope = CredentialScope {
        timestamp: chrono::Utc::now(),
        region: "us-east-1".to_string(),
        service: "s3".to_string(),
    };

    let mut signed_headers = HashMap::new();
    signed_headers.insert("host".to_string(), url.host_str().unwrap_or("").to_string());
    signed_headers.insert("content-type".to_string(), "application/octet-stream".to_string());
    signed_headers.insert("content-length".to_string(), amount.to_string());
    signed_headers.insert("content-md5".to_string(), md5_hash.clone());
    signed_headers.insert("x-amz-date".to_string(), credential_scope.timestamp.format("%Y%m%dT%H%M%SZ").to_string());
    signed_headers.insert(
        signer::get_amz_content_header(AWS_ALGORITHM),
        signer::sha256_hex(&bytes_for_hash),
    );

    let auth_header = signer::get_s3_auth_header(
        Method::PUT.as_str(),
        bucket,
        &credentials,
        &credential_scope,
        file_info.key.as_str(),
        AWS_ALGORITHM,
        &signed_headers,
        &url,
    );

    let mut req_builder = Request::builder()
        .method(Method::PUT)
        .uri(url.as_str());

    for (header_name, header_value) in &signed_headers {
        req_builder = req_builder.header(header_name, header_value);
    }

    let req = req_builder
        .header("authorization", auth_header)
        .body(body)?;

    Ok((req, file_work_elapsed, file_permit_elapsed))
}

struct PooledBuffer {
    buffer: Option<Vec<u8>>,
    pool: Arc<BufferPool>,
    len: usize,
}

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer.as_ref().expect("buffer should be present until drop")[..self.len]
    }
}

// We need to implement AsRef<[u8]> for Arc<PooledBuffer> if we want to use it with Bytes::from_owner
// But wait, Arc already implements AsRef<T>.
// The error says: "the trait bound `Arc<PooledBuffer>: AsRef<[u8]>` is not satisfied"
// Rust's blanket impl: impl<T: ?Sized, A: Allocator> AsRef<T> for Arc<T, A>
// So Arc<PooledBuffer> implements AsRef<PooledBuffer>.
// It does NOT automatically implement AsRef<[u8]> even if PooledBuffer implements AsRef<[u8]>.

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