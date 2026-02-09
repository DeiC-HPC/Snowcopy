use anyhow::Result;
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use hyper::{Method, Request, Uri};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

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

    let byte_stream = ReaderStream::with_capacity(file, READ_BUFFER_SIZE).map_ok(Frame::data);
    let body: StreamingBody = StreamBody::new(byte_stream).boxed();

    let req = Request::builder()
        .method(Method::PUT)
        .uri(uri)
        .header("content-type", "application/octet-stream")
        .header("content-length", len.to_string())
        .header("connection", "keep-alive")
        .body(body)?;

    let resp = client.request(req).await?;
    let status = resp.status();

    //drain body so the connection can be returned to the pool.
    resp.into_body().collect().await?;

    if !status.is_success() {
        anyhow::bail!("PUT failed: {}", status);
    }
    Ok(())
}

pub async fn put_file_range(url: &str, path: &str, from: u64, amount: u64, hyper_client: &PoolClient) -> Result<Option<String>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let uri: Uri = url.parse()?;
    let mut file = File::open(path).await?;

    // Seek to the starting position
    file.seek(std::io::SeekFrom::Start(from)).await?;

    // Take only the bytes in the range [from, to]
    let limited_reader = file.take(amount);

    // Convert to a stream of Frames
    let byte_stream = ReaderStream::with_capacity(limited_reader, READ_BUFFER_SIZE).map_ok(Frame::data);
    let body: StreamingBody = StreamBody::new(byte_stream).boxed();

    let req = Request::builder()
        .method(Method::PUT)
        .uri(uri)
        .header("content-type", "application/octet-stream")
        .header("content-length", amount.to_string())
        .header("connection", "keep-alive")
        .body(body)?;

    let resp = hyper_client.request(req).await?;

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