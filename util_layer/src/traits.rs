use std::fs::File;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use http_body_util::BodyExt;
use hyper::{Error, Request};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use mockall::automock;
use crate::{HttpExecutorResponse, StreamingBody};

#[automock]
pub trait FileObject: Send + Sync {
    fn open(file_path: &Path) -> io::Result<Arc<dyn FileObject>>
    where
        Self: Sized;

    fn read_at_offset(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    
    fn length(&self) -> io::Result<u64>;
}

pub struct ImplFileObject {
    file: File
}

impl ImplFileObject {
}

impl FileObject for ImplFileObject {
    fn open(file_path: &Path) -> io::Result<Arc<dyn FileObject>> {
        Ok(Arc::new(Self {
            file: File::open(file_path)?,
        }))
    }

    #[cfg(unix)]
    fn read_at_offset(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        use std::os::unix::fs::FileExt;
        self.file.read_at(buf, offset)
    }
    
    #[cfg(windows)]
    fn read_at_offset(&self, buf: &mut[u8], offset: u64) -> std::io::Result<usize> {
        use std::os::windows::fs::FileExt;
        self.file.seek_read(buf, offset)
    }

    fn length(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

#[automock]
pub trait FileHandler: Send + Sync {
    fn open_file(&self, file_path: &Path) -> io::Result<Arc<dyn FileObject>>;
}

pub struct ImplFileHandler {}

impl FileHandler for ImplFileHandler {
    fn open_file(&self, file_path: &Path) -> io::Result<Arc<dyn FileObject>> {
        ImplFileObject::open(file_path)
    }
}

#[automock]
pub trait HttpExecutor: Send + Sync {
    fn execute_await(&self, req: Request<StreamingBody>) -> Pin<Box<dyn Future<Output = Result<HttpExecutorResponse, Error>> + Send + 'static>>;
}

// Public type aliases so app can use the exact type
pub type Https = hyper_rustls::HttpsConnector<HttpConnector>;
pub type HyperClient = Client<Https, StreamingBody>;
pub struct HyperHttpExecutor {
    client: Arc<HyperClient>,
}

impl HttpExecutor for HyperHttpExecutor {
    fn execute_await(&self, req: Request<StreamingBody>) -> Pin<Box<dyn Future<Output = Result<HttpExecutorResponse, Error>> + Send + 'static>> {
        let response = self.client.request(req);
        Box::pin(async move {
            let response = response.await.unwrap();

            let status = response.status();
            let headers = response.headers().clone();

            let body = response
                .into_body()
                .collect()
                .await
                .map(|collected| collected.to_bytes().to_vec())
                .unwrap_or_default();

            Ok(HttpExecutorResponse {
                status,
                headers,
                body,
            })
        })
    }
}

impl HyperHttpExecutor {
    pub fn new(client: Arc<HyperClient>) -> Self {
        HyperHttpExecutor {
            client,
        }
    }
}
