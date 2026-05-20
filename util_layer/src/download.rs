
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fs::File;
use std::io::Write;
use std::time::SystemTime;
use bytes::Bytes;
use crate::cc_util::CommandCenterEvent;
use tokio::sync::mpsc::UnboundedSender;

pub struct FileCompletionTracker {
    pub pending_writes: AtomicUsize,
    pub mtime: Option<SystemTime>,
    pub total_size: u64,
    pub cc_sender: UnboundedSender<CommandCenterEvent>,
}

impl FileCompletionTracker {
    pub fn complete_write(&self, mut file: &File) {
        if self.pending_writes.fetch_sub(1, Ordering::SeqCst) == 1 {
            // This was the last write
            if let Some(mtime) = self.mtime {
                file.flush().expect("Should never fail");
                let _ = file.set_modified(mtime); //I expect this to never fail, but we don't want to react to any error
            }
            self.cc_sender.send(CommandCenterEvent::FileTransferred(self.total_size)).expect("Should never fail");
        }
    }
}

pub type WriteMsg = (Arc<File>, u64, Bytes, Arc<FileCompletionTracker>);