use crate::signer::{ChunkSigner, CredentialScope, sha256_hex};
use crate::{StreamingBody, signer};
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::{Body, Frame};
use hyper::{Method, Request};
use std::collections::HashMap;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::SystemTime;
use tokio::sync::Semaphore;
use util_layer::UploadConfig;
use util_layer::constants::{AMZ_CHECKSUM_SHA256, AMZ_CHUNK_SIGNATURE_HEADER, AMZ_STREAMING_TRAILER, AMZ_TRAILER_SIGNATURE, AWS_ALGORITHM, CHUNK_SIGNATURE_LENGTH, EMPTY_PAYLOAD_SHA256, TRAILER_HASH_LENGTH};
use util_layer::snow_errors::SnowCopyError;
use util_layer::traits::{FileTrait, HttpExecutor};

/// Upload a file using streaming for trailing headers.
pub async fn put_file_streaming(
    req: Request<StreamingBody>,
    amount: u64,
    client: Arc<dyn HttpExecutor>,
    req_sem: &Semaphore,
) -> Result<(String, u64), SnowCopyError> {

    let permit = req_sem.acquire().await?;
    let resp = client.execute_await(req).await.map_err(|e| SnowCopyError::Hyper(e))?;

    drop(permit);

    let status = resp.status;
    let etag = resp
        .headers
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if !status.is_success() {
        let s3_err = crate::map_s3_error_from_http_executor_response(resp).await;
        return Err(s3_err.into());
    }

    if etag.is_none() {
        return Err(SnowCopyError::MissingRequiredHeader("etag").into());
    }

    Ok((etag.unwrap(), amount))
}

pub fn create_streaming_req_trailing_checksum(
    ctx: Arc<UploadConfig>,
    canonical_uri: &String,
    query_uri: &String,
    file: Arc<dyn FileTrait>,
    file_offset: u64,
    amount: u64,
    modified_time: Option<SystemTime>,
    file_sem: Arc<Semaphore>,
    chunk_size: u64,
) -> Result<Request<StreamingBody>, SnowCopyError> {
    let credential_scope = CredentialScope {
        timestamp: chrono::Utc::now(),
        region: ctx.endpoint.s3_region.clone(),
        service: "s3".to_string(),
    };

    let amz_date = credential_scope.timestamp.format("%Y%m%dT%H%M%SZ").to_string();
    let date_y_m_d = credential_scope.timestamp.format("%Y%m%d").to_string();
    let credential_scope_str = format!(
        "{}/{}/s3/aws4_request",
        date_y_m_d,
        credential_scope.region
    );

    let mtime_str = modified_time.map(util_layer::time::time_to_float_string);

    let mut signed_headers: HashMap<String, String> = HashMap::new();
    let url_parsed = url::Url::parse(&ctx.endpoint.s3_url).expect("Invalid S3 URL");
    let host = url_parsed.host_str().unwrap_or("").to_string();
    signed_headers.insert("host".to_string(), host);
    signed_headers.insert("x-amz-date".to_string(), amz_date.clone());
    signed_headers.insert("content-encoding".to_string(), "aws-chunked".to_string());
    signed_headers.insert("content-type".to_string(), "application/octet-stream".to_string());
    if let Some(mtime_str) = mtime_str {
        signed_headers.insert("x-amz-meta-mtime".to_string(), mtime_str);
    }
    signed_headers.insert(
        signer::get_amz_content_header(AWS_ALGORITHM),
        AMZ_STREAMING_TRAILER.to_string(),
    );
    signed_headers.insert("x-amz-decoded-content-length".to_string(), amount.to_string());
    signed_headers.insert("x-amz-trailer".to_string(), AMZ_CHECKSUM_SHA256.to_string());

    let encoded_len = signer::aws_chunked_trailer_encoded_length(
        amount,
        chunk_size,
        AMZ_CHECKSUM_SHA256,    // TODO LND: We might want to support multiple checksums in the future (with their own trailer hash length)
        TRAILER_HASH_LENGTH,    // The length of the trailing checksum
    );
    signed_headers.insert("content-length".to_string(), encoded_len.to_string());

    let (auth_header, seed_signature, signing_key) = signer::get_s3_auth_header_and_signature(
        "PUT",
        &ctx.endpoint,
        &credential_scope,
        AWS_ALGORITHM,
        &signed_headers,
        canonical_uri,
        query_uri,
    );

    let uri = crate::format_query(ctx.clone(), canonical_uri, query_uri);

    let streaming_body = StreamingFileChunkedBody::new( // TODO LND REFACTOR: Iff drawing logic out, then this is a good candidate to separate to improve readability
        file,
        file_offset,
        amount,
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

impl Unpin for StreamingFileChunkedBody {}

/// Inner state that can be safely mutated
pub struct StreamingFileChunkedBodyInner {
    /// The raw payload data
    file: Arc<dyn FileTrait>,
    /// Offset in the file to start reading from
    file_offset: u64,
    /// Remaining bytes to read from the file
    remaining: u64,
    /// Semaphore to protect file access
    file_sem: Arc<Semaphore>,       // TODO LND REFACTOR: Is this only necessary for some scenarios (non-multipart uploads)?
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
    /// Buffer for holding the entirety of the chunk to be sent
    chunk_buf: Vec<u8>,
    /// Current read position in buf
    state: StreamingChunkedState,
    /// Chunk size (in kB)
    chunk_size: u64,    // TODO LND REFACTOR: Should avoid having 2 concepts with the same name - trailing chunk size and upload chunk size (the one that determines whether a file is multipart or not)
}

impl StreamingFileChunkedBodyInner {
    /// Formats the data chunk on read_buf in the following order: Length of data sent, chunk signature, and then the data of the chunk and a "\r\n".
    /// Data is put in the middle during read to avoid copying the data twice.
    fn format_data_chunk(&mut self, data_index: usize, bytes_read: usize) {
        let chunk_data = &self.chunk_buf[data_index..data_index + bytes_read];
        let chunk_hash = sha256_hex(chunk_data);

        let chunk_sig = self.chunk_signer.sign_chunk(
            &self.amz_date,
            &self.credential_scope,
            &self.prev_signature,
            chunk_hash.as_str(),
        );

        // Build header directly into the buffer prefix
        // Format: "{:x};chunk-signature={}\r\n" - hex length of chunk data, chunk signature and then line endings
        let mut pos = 0;

        // Write hex length of chunk data
        pos += find_length_of_chunk_data_as_hex(&mut self.chunk_buf[pos..], bytes_read);

        // Write ";chunk-signature="
        self.chunk_buf[pos] = b';';
        pos += 1;
        let header_bytes = AMZ_CHUNK_SIGNATURE_HEADER.as_bytes();
        self.chunk_buf[pos..pos + header_bytes.len()].copy_from_slice(header_bytes);
        pos += header_bytes.len();
        self.chunk_buf[pos] = b'=';
        pos += 1;

        // Write signature (64 hex chars)
        let sig_bytes = chunk_sig.as_bytes();
        self.chunk_buf[pos..pos + sig_bytes.len()].copy_from_slice(sig_bytes);
        pos += sig_bytes.len();

        // Write "\r\n" before the chunk data section
        self.chunk_buf[pos] = b'\r';
        self.chunk_buf[pos + 1] = b'\n';

        // Append "\r\n" after the chunk data section
        self.chunk_buf[data_index + bytes_read] = b'\r';
        self.chunk_buf[data_index + bytes_read + 1] = b'\n';
        self.prev_signature = chunk_sig;
    }

    fn format_final_chunk(&mut self) {
        // Finalize the checksum now, before we need it for trailers
        if let Some(mut hasher) = self.checksum_hasher.take() {
            let digest = hasher.finish().expect("Failed to finish hasher");
            self.final_checksum = Some(openssl::base64::encode_block(&digest));
        }

        let empty_sig = self.chunk_signer.sign_chunk(
            &self.amz_date,
            &self.credential_scope,
            &self.prev_signature,
            EMPTY_PAYLOAD_SHA256,
        );
        self.prev_signature = empty_sig.clone();

        write!(
            &mut self.chunk_buf,
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
            &mut self.chunk_buf,
            "{}:{}\r\n{}:{}\r\n\r\n",
            self.trailer_name.to_lowercase(),
            checksum_b64,
            AMZ_TRAILER_SIGNATURE,
            trailer_sig
        ).unwrap();
    }
}

impl Body for StreamingFileChunkedBody {
    type Data = Bytes;
    type Error = std::io::Error;

    //TODO JON: this should be rewritten completely. We should have a spawn_blocking that reads frames into a queue.
    //TODO JON: this poll frame should be async, so it should wait on the queue for ready frames.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let inner = &mut self.inner;

        loop {
            match inner.state {
                StreamingChunkedState::Reading => {
                    inner.chunk_buf.clear();
                    if inner.remaining == 0 {
                        inner.state = StreamingChunkedState::FinalChunk;
                        continue;
                    }

                    let file_lock = inner.file_sem.acquire();
                    let file = inner.file.clone();

                    let to_read = std::cmp::min(inner.chunk_size, inner.remaining) as usize;
                    let to_read_digits = hex_digit_count(to_read);
                    let data_index = to_read_digits + CHUNK_SIGNATURE_LENGTH;            // "<data_index>;chunk-signature=<FixedLengthChunkSignature>\r\n"
                    inner.chunk_buf.resize(data_index + to_read + 2, 0);     // Resize to ChunkSignature + Data length + "\r\n"
                    let buf = &mut inner.chunk_buf[data_index..data_index + to_read];    // Buf is offset to reserve space for chunk signature

                    let bytes_read = file.read_at_offset(buf, inner.file_offset)?;
                    inner.file_offset += bytes_read as u64; //We need to update the offset after each read.
                    drop(file_lock);

                    if bytes_read != to_read {
                        return Poll::Ready(Some(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            format!("Expected to read {} bytes, but read {} bytes", to_read, bytes_read),
                        ))));
                    }

                    if bytes_read == 0 { //TODO JON: We have this information but are wasting a system call to check that we are done
                        // EOF
                        inner.state = StreamingChunkedState::FinalChunk;
                        continue;
                    }

                    inner.checksum_hasher.as_mut().expect("Hasher should have been created").update(buf).expect("Failed to update SHA256 hasher");

                    inner.remaining -= bytes_read as u64;
                    inner.format_data_chunk(data_index, bytes_read);

                    let data = std::mem::take(&mut inner.chunk_buf);
                    return Poll::Ready(Some(Ok(Frame::data(Bytes::from(data)))));
                }

                StreamingChunkedState::FinalChunk => {
                    inner.format_final_chunk();
                    inner.state = StreamingChunkedState::Trailers;
                    let data = std::mem::take(&mut inner.chunk_buf);
                    return Poll::Ready(Some(Ok(Frame::data(Bytes::from(data)))));
                }

                StreamingChunkedState::Trailers => {
                    inner.format_trailers();
                    inner.state = StreamingChunkedState::Done;
                    let data = std::mem::take(&mut inner.chunk_buf);
                    return Poll::Ready(Some(Ok(Frame::data(Bytes::from(data)))));
                }

                StreamingChunkedState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

/// Streaming AWS Chunked body that reads directly from file.
/// Avoids buffering the entire file in memory - reads chunks on demand.
/// Used for large file uploads with trailing headers.
pub struct StreamingFileChunkedBody {
    inner: StreamingFileChunkedBodyInner,
}

impl StreamingFileChunkedBody {
    pub fn new(
        file: Arc<dyn FileTrait>,
        file_offset: u64,
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
                file,
                file_offset,
                remaining: file_size,
                file_sem,
                chunk_signer: ChunkSigner::new(&signing_key),
                amz_date,
                credential_scope,
                prev_signature: seed_signature,
                trailer_name,
                checksum_hasher: Some(signer::create_sha256_hasher()),
                final_checksum: None,
                chunk_buf: Vec::with_capacity((chunk_size) as usize + CHUNK_SIGNATURE_LENGTH + 11),   // Size of chunk data + chunk signature + max digits size + "\r\n"
                state: StreamingChunkedState::Reading,
                chunk_size,
            }
        }
    }

    pub fn into_streaming_body(self) -> StreamingBody {
        BodyExt::boxed(self)
    }
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

/// Finds the number of digits in hex to represent the given length of a chunk of data.
#[inline]
fn find_length_of_chunk_data_as_hex(buf: &mut [u8], mut n: usize) -> usize {
    if n == 0 {
        buf[0] = b'0';
        return 1;
    }

    // Find number of hex digits needed
    let digits = ((usize::BITS - n.leading_zeros() + 3) / 4) as usize;

    // Write digits in reverse
    for i in (0..digits).rev() {
        let digit = (n & 0xF) as u8;
        buf[i] = if digit < 10 { b'0' + digit } else { b'a' + digit - 10 };
        n >>= 4;
    }
    digits
}

/// Finds the number of hex digits needed to represent the given value.
fn hex_digit_count(value: usize) -> usize {
    match value {
        0..=0xF => 1,
        0..=0xFF => 2,
        0..=0xFFF => 3,
        0..=0xFFFF => 4,
        0..=0xF_FFFF => 5,
        0..=0xFF_FFFF => 6,
        0..=0xFFF_FFFF => 7,
        0..=0xFFFF_FFFF => 8,
        0..=0xF_FFFF_FFFF => 9,
        _ => panic!("Value size too large for hex digit count: {}", value),
    }
}