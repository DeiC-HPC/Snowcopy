mod logging;

use std::convert::Infallible;
use std::sync::Arc;
use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose;
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody, Full};
use hyper::body::Frame;
use hyper::{Method, Request, Uri};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use openssl::hash::{Hasher, MessageDigest};
use tokio::sync::{Semaphore, Mutex};

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

pub async fn put_file(url: &str, path: &str, client: &PoolClient, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>) -> Result<()> {
    let uri: Uri = url.parse()?;

    //let req = create_streaming_body_put(len, uri, file).await?;
    let req = create_in_memory_body_put_with_hashes(0, None, uri, path, file_sem, buffer_pool).await?;

    let permit = req_sem.acquire().await?;
    let resp = client.request(req).await?;
    drop(permit);

    let status = resp.status();

    //drain body so the connection can be returned to the pool.
    resp.into_body().collect().await?;

    if !status.is_success() {
        anyhow::bail!("PUT failed: {}", status);
    }
    Ok(())
}

pub async fn put_file_range(url: &str, path: &str, from: u64, amount: u64, client: &PoolClient, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>) -> Result<Option<String>> {
    let uri: Uri = url.parse()?;

    //let req = create_streaming_body_put(amount, uri, file).await?;
    let req = create_in_memory_body_put_with_hashes(from, Some(amount), uri, path, file_sem, buffer_pool).await?;

    let permit = req_sem.acquire().await?;
    let resp = client.request(req).await?; //send_with_logging(client, req).await?;
    drop(permit);

    let status = resp.status();
    let etag = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Drain body so the connection can be returned to the pool.
    resp.into_body().collect().await?;

    if !status.is_success() {
        anyhow::bail!("PUT range failed: {}", status);
    }

    Ok(etag)
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

async fn create_in_memory_body_put_with_hashes(start: u64, amount_option: Option<u64>, uri: Uri, path: &str, file_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>) -> Result<Request<StreamingBody>> {
    let file_lock = file_sem.acquire().await?;
    let mut file = File::open(path).await?;

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

    let req = Request::builder()
        .method(Method::PUT)
        .uri(uri)
        .header("content-type", "application/octet-stream")
        .header("content-length", amount.to_string())
        .header("content-md5", md5_hash)
        .body(body)?;

    Ok(req)
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