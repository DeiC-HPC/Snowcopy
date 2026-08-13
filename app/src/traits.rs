use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use http_layer::BufferPool;
use util_layer::cc_util::CommandCenterEvent;

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