mod logging;

use std::convert::Infallible;
use anyhow::Result;
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
use sha2::{Sha256, Digest as ShaDigest};
use md5;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};


// Optimized buffer size for file reading (256 KB)
const READ_BUFFER_SIZE: usize = 256 * 1024;

// Public type aliases so app can use the exact type
pub type Https = hyper_rustls::HttpsConnector<HttpConnector>;
pub type StreamingBody = BoxBody<Bytes, std::io::Error>;
pub type PoolClient = Client<Https, StreamingBody>;

pub fn build_https_client() -> PoolClient {
    use hyper_rustls::HttpsConnectorBuilder;

    let mut http = HttpConnector::new();
    http.enforce_http(false); // allow https

    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .expect("no native root CA certificates found")
        .https_only()
        .enable_http1()
        //.enable_http2()
        .wrap_connector(http);

    Client::builder(TokioExecutor::new()).build(https)
}

pub async fn put_file(url: &str, path: &str, client: &PoolClient) -> Result<()> {
    let uri: Uri = url.parse()?;
    let file = File::open(path).await?;
    let len = file.metadata().await?.len();

    //let req = create_streaming_body_put(len, uri, file).await?;
    let req = create_in_memory_body_put_with_hashes(len, uri, file).await?;
    let resp = client.request(req).await?; //send_with_logging(client, req).await?;
    let status = resp.status();

    //drain body so the connection can be returned to the pool.
    resp.into_body().collect().await?;

    if !status.is_success() {
        anyhow::bail!("PUT failed: {}", status);
    }
    Ok(())
}

pub async fn put_file_range(url: &str, path: &str, from: u64, amount: u64, client: &PoolClient) -> Result<Option<String>> {
    let uri: Uri = url.parse()?;
    let mut file = File::open(path).await?;

    // Seek to the starting position
    file.seek(std::io::SeekFrom::Start(from)).await?;

    //let req = create_streaming_body_put(amount, uri, file).await?;
    let req = create_in_memory_body_put_with_hashes(amount, uri, file).await?;

    let resp = send_with_logging(client, req).await?;

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

async fn create_streaming_body_put(amount: u64, uri: Uri, file: File) -> hyper::http::Result<Request<StreamingBody>> {
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
        .header("connection", "keep-alive")
        .body(body)

}

async fn create_in_memory_body_put_with_hashes(amount: u64, uri: Uri, mut file: File) -> Result<Request<StreamingBody>> {

    let mut buffer = vec![0u8; amount as usize];

    file.read_exact(&mut buffer).await?;
    
    // Hash efficiently
    let sha256_hash = {
        let mut h = Sha256::new();
        h.update(&buffer);
        hex::encode(h.finalize())
    };

    let md5_hash = {
        let mut h = md5::Context::new();
        h.consume(&buffer);
        BASE64.encode(h.finalize().0)
    };

    // Build hyper body (convert Infallible -> std::io::Error to match StreamingBody)
    let body: StreamingBody = Full::new(Bytes::from(buffer))
        .map_err(|inf: Infallible| match inf {})
        .boxed();

    let req = Request::builder()
        .method(Method::PUT)
        .uri(uri)
        .header("content-type", "application/octet-stream")
        .header("content-length", amount.to_string())
        .header("connection", "keep-alive")
        //.header("X-Amz-Content-Sha256", sha256_hash)
        .header("content-md5", md5_hash)
        .body(body)?;

    Ok(req)
}

pub async fn send_with_logging(
    client: &crate::PoolClient,
    req: hyper::Request<crate::StreamingBody>,
) -> Result<hyper::Response<hyper::body::Incoming
>> {
    logging::log_request_headers(&req);
    let resp = client.request(req).await?;
    logging::log_response_headers(&resp);
    Ok(resp)
}