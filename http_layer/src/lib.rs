mod logging;
mod signer;
pub mod download;
pub mod integrity;
pub mod upload_trailing;
pub mod filesystem_checker;

use crate::signer::CredentialScope;
use anyhow::Result;
use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use openssl::base64;
use openssl::hash::{Hasher, MessageDigest};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use url::Url;
use util_layer::constants::{AWS_ALGORITHM, EMPTY_PAYLOAD_SHA256};
use util_layer::snow_errors::{S3Error, SnowCopyError};
use util_layer::{DownloadConfig, FileInfo, S3ConfigTrait, UploadConfig};

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
pub type HyperClient = Client<Https, StreamingBody>;

pub fn build_https_client() -> HyperClient {
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

pub async fn put_file(ctx: Arc<UploadConfig>, file_info: Arc<FileInfo>, client: &HyperClient, file_sem: &Semaphore, req_sem: &Semaphore, buffer_pool: Arc<BufferPool>, canonical_uri: &String, query_uri: &String) -> Result<(String, u64)> {
    let req = create_in_memory_body_put_with_hashes(0, None, ctx, file_info.clone(), file_sem, buffer_pool, canonical_uri, query_uri).await?;

    let permit = req_sem.acquire().await?;
    let resp = client.request(req).await?;

    let status = resp.status();
    if !status.is_success() {
        let s3_err = map_s3_error(resp).await;
        drop(permit);
        return Err(s3_err.into());
    }

    let etag = resp // TODO LND: Unused currently
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    //drain body so the connection can be returned to the pool.
    resp.into_body().collect().await?;
    drop(permit);

    Ok((etag.unwrap(), file_info.file_size))
}

pub async fn put_file_range(ctx: Arc<UploadConfig>, file_info: Arc<FileInfo>, from: u64, amount: u64, client: &HyperClient, file_sem: &Semaphore, req_sem: &Semaphore, buffer_pool: Arc<BufferPool>, canonical_uri: &String, query_uri: &String) -> Result<(String, u64)> {
    let req = create_in_memory_body_put_with_hashes(from, Some(amount), ctx, file_info, file_sem, buffer_pool, canonical_uri, query_uri).await?;

    let permit = req_sem.acquire().await.expect("Should never fail");

    let resp = client.request(req).await?; //send_with_logging(client, req).await?;
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

    Ok((etag.unwrap(), amount))
}

async fn create_in_memory_body_put_with_hashes(start: u64, amount_option: Option<u64>, ctx: Arc<UploadConfig>, file_info: Arc<FileInfo>, file_sem: &Semaphore, buffer_pool: Arc<BufferPool>, canonical_uri: &String, query_uri: &String) -> Result<(Request<StreamingBody>)> {
    let file_lock = file_sem.acquire().await?;

    let mut file = File::open(&*file_info.file_path).await?;

    let amount = match amount_option {
        Some(v) => v,
        None => file.metadata().await?.len(),
    };

    if start > 0 {
        file.seek(std::io::SeekFrom::Start(start)).await?;
    }

    // Ensure the buffer is large enough if amount > chunk_size,
    // though in this app amount is expected to be <= CHUNK_SIZE.
    //TODO JON: We have to be careful here, as we might end up using more memory than we intend. We should have a known limit from the beginning or use multiple chunks.
    let mut buffer = buffer_pool.get().await;
    if buffer.len() < amount as usize {
        buffer.resize(amount as usize, 0u8);
    }

    let mut total_read = 0usize;
    let chunk_size = READ_BUFFER_SIZE.min(amount as usize);

    //TODO JON: Its very possible that we should wrap this in a spawn_blocking and use a single file descriptor for the upload.

    let mut md5_hasher: Option<Hasher> = if ctx.md5_checksum_upload {
        Some(Hasher::new(MessageDigest::md5())?)
    } else {
        None
    };
    let mut sha_hasher = signer::create_sha256_hasher();

    while total_read < amount as usize { //read small chunks, so the block is less significant when hashing.
        let remaining = amount as usize - total_read;
        let to_read = remaining.min(chunk_size);
        let chunk = &mut buffer[total_read..total_read + to_read];

        file.read_exact(chunk).await?;
        sha_hasher.update(chunk).expect("Failed to update SHA256 hasher");
        if let Some(ref mut md5_hasher) = md5_hasher {
            md5_hasher.update(chunk).expect("Failed to update MD5 hasher");
        }

        total_read += to_read;
    }
    drop(file);
    drop(file_lock);

    let sha256_hex = signer::finish_sha256_hex(sha_hasher);
    let md5_base64 = if let Some(mut md5_hasher) = md5_hasher {
        let md5 = md5_hasher.finish()?;
        Some(base64::encode_block(&md5))
    } else {
        None
    };

    let mtime_str = file_info.modified_time.map(util_layer::time::time_to_float_string);

    // We use a custom struct to return the buffer to the pool when dropped
    let body_buffer = PooledBuffer {
        buffer: Some(buffer),
        pool: buffer_pool.clone(),
        len: amount as usize,
    };

    let mut header_map : HashMap<String, String> = HashMap::new();

    if let Some(md5_base64) = md5_base64 {
        header_map.insert("content-md5".to_string(), md5_base64);
    }
    header_map.insert("content-type".to_string(), "application/octet-stream".to_string());
    header_map.insert("content-length".to_string(), amount.to_string());
    header_map.insert(
        signer::get_amz_content_header(AWS_ALGORITHM),
        sha256_hex.clone(),
    );
    header_map.insert("x-amz-checksum-sha256".to_string(), sha256_hex.clone()); //AWS and some clones will check against this
    header_map.insert("x-amz-meta-checksum-sha256".to_string(), sha256_hex); //Store as metadata, since clones cannot be trusted
    if let Some(mtime_str) = mtime_str {
        header_map.insert("x-amz-meta-mtime".to_string(), mtime_str);
    }

    let bytes = Bytes::from_owner(body_buffer);
    let req = create_and_sign_req(ctx, canonical_uri, query_uri, Some(&bytes), Method::PUT, &header_map)?;

    Ok(req)
}

pub async fn get_file_range(ctx: Arc<DownloadConfig>, client: &HyperClient, buffer_pool: Arc<BufferPool>, canonical_uri: Arc<String>, query_uri: &String, req_sem: Arc<Semaphore>, start: u64, end: u64) -> Result<PooledBuffer> {
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

            let req = create_and_sign_req(ctx, &canonical_uri, &query_uri, None, Method::GET, &header_map)?;
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
                    //TODO JON: I believe we don't need the unsafe. If we decide to go this direction, then slices should be used instead

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


fn create_and_sign_req<T: S3ConfigTrait>(ctx: Arc<T>, canonical_uri: &String, query_uri: &String, payload: Option<&Bytes>, method: Method, header_map: &HashMap<String, String>) -> Result<Request<StreamingBody>> {
    let endpoint = ctx.endpoint();
    let url_parsed = Url::parse(&endpoint.s3_url).expect("Invalid S3 URL");
    let host = url_parsed.host_str().unwrap_or("");

    let credential_scope = CredentialScope {
        timestamp: chrono::Utc::now(),
        region: endpoint.s3_region.clone(),
        service: "s3".to_string(),
    };

    let mut signed_headers : HashMap<String, String> = HashMap::new();
    signed_headers.insert("host".to_string(), host.to_string());
    signed_headers.insert("x-amz-date".to_string(), credential_scope.timestamp.format("%Y%m%dT%H%M%SZ").to_string());

    for (key, value) in header_map {
        signed_headers.insert(key.to_string(), value.to_string());
    }

    let auth_header = signer::get_s3_auth_header(
        method.as_str(),
        endpoint,
        &credential_scope,
        AWS_ALGORITHM,
        &signed_headers,
        canonical_uri,
        &query_uri,
    );

    let uri = format_query(ctx, canonical_uri, query_uri);

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

fn format_query<T: S3ConfigTrait>(ctx: Arc<T>, canonical_uri: &String, query_uri: &String) -> String {
    let endpoint = ctx.endpoint();
    let url_parsed = Url::parse(&endpoint.s3_url).expect("Invalid S3 URL");
    let host = url_parsed.host_str().unwrap_or("");
    if query_uri.is_empty() {
        format!("https://{}{}", host, canonical_uri)
    } else {
        format!("https://{}{}?{}", host, canonical_uri, query_uri)
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

pub async fn send_with_logging(
    client: &HyperClient,
    req: hyper::Request<StreamingBody>,
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
