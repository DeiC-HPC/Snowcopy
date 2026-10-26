use http_layer::BufferPool;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio_util::sync::CancellationToken;
use util_layer::cc_util::CommandCenterEvent;
use util_layer::download::WriteMsg;

pub trait UploadTrait {
    fn upload_file(&self) -> Pin<Box<dyn Future<Output=()> + Send + '_>>;
}

pub struct UploadRuntime {
    pub file_sem: Arc<Semaphore>,
    pub req_sem: Semaphore,
    pub buffer_pool: Arc<BufferPool>,
    pub cc_tx: UnboundedSender<CommandCenterEvent>,
    pub cancellation_token: CancellationToken
}

pub trait DownloadTrait {
    async fn download_file(&self);
}

pub struct DownloadRuntime {
    pub req_sem: Arc<Semaphore>,
    pub disk_write_tx: Sender<WriteMsg>,
    pub cc_tx: UnboundedSender<CommandCenterEvent>,
    pub cancellation_token: CancellationToken,
    pub download_file_concurrency: usize,
}