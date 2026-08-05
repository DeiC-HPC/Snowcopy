use std::pin::Pin;

pub trait UploadTrait {
    fn upload_file(&self) -> Pin<Box<dyn Future<Output=()> + Send + '_>>;
}
