
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::fs::File;
use std::io::Write;
use std::time::SystemTime;
use bytes::Bytes;
use crate::cc_util::CommandCenterEvent;
use tokio::sync::mpsc::UnboundedSender;

pub struct FileCompletionTracker {
    pub pending_bytes_to_be_written: std::sync::atomic::AtomicU64,
    pub mtime: Option<SystemTime>,
    pub total_size: u64,
}

impl FileCompletionTracker {
    pub fn complete_write(&self, mut file: &File, bytes_written: usize, cc_sender: UnboundedSender<CommandCenterEvent>) {
        //bookkeep written bytes, check if all bytes have been written
        if self.pending_bytes_to_be_written.fetch_sub(bytes_written as u64, Ordering::SeqCst) == bytes_written as u64 {
            // This was the last write
            if let Some(mtime) = self.mtime {
                file.flush().expect("Should never fail");
                let _ = file.set_modified(mtime); //I expect this to never fail, but we don't want to react to any error
            }
            cc_sender.send(CommandCenterEvent::FileTransferred()).expect("Should never fail");
        }
    }

    pub fn roll_back_written_bytes_tracker_by_amount(&self, amount: u64) {
        self.pending_bytes_to_be_written.fetch_add(amount, Ordering::SeqCst);
    }
}

pub type WriteMsg = (Arc<File>, u64, Bytes, Arc<FileCompletionTracker>);