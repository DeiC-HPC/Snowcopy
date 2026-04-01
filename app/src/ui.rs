use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::Instant;
pub(crate) use util_layer::ui::UiEvent;

pub trait Ui {
    fn start(&self) -> (UnboundedSender<UiEvent>, JoinHandle<()>);
}

struct ProgressBarUiInner {
    mp: MultiProgress,
    overall_pb: ProgressBar,
    transfer_pb: ProgressBar,
    started: AtomicBool,
    completed: AtomicBool,
}

pub struct ProgressBarUi {
    inner: Arc<ProgressBarUiInner>,
}

impl ProgressBarUi {
    pub fn new() -> Self {
        let mp = MultiProgress::new();
        let overall_pb = mp.add(ProgressBar::new(0));
        let transfer_pb = mp.add(ProgressBar::new(0));

        overall_pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {msg}  {wide_bar:.green/blue} {percent}% ",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        overall_pb.set_message("At target: 0 B/0 B | 0/0 files | preexisting: 0 B");

        transfer_pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {prefix}: {msg} {wide_bar:.cyan/blue} {percent}%",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        transfer_pb.set_prefix("Discovering");

        Self {
            inner: Arc::new(ProgressBarUiInner {
                mp,
                overall_pb,
                transfer_pb,
                started: AtomicBool::new(false),
                completed: AtomicBool::new(false),
            }),
        }
    }

    pub fn finish(&self, msg: &str) {
        if self.inner.started.load(Ordering::SeqCst) && !self.inner.completed.load(Ordering::SeqCst) {
            eprintln!("Warning: finish() called before UI thread has completed. This may result in incorrect reporting of progress.");
        }

        self.inner.transfer_pb.set_prefix(msg.to_string());
        self.inner.transfer_pb.finish();
        self.inner.overall_pb.finish();
    }

    fn overall_pb_message(files_preexisting: u64, data_preexisting: u64, total_files_discovered: u64, total_data_discovered: u64, files_at_target: u64, data_at_target: u64) -> String {
        format!(
            "Total: {}/{} ({}/{} files) | Existing {} files, {}",
            indicatif::BinaryBytes(data_at_target),
            indicatif::BinaryBytes(total_data_discovered),
            files_at_target, total_files_discovered,
            files_preexisting,
            indicatif::BinaryBytes(data_preexisting),
        )
    }
}

impl Ui for ProgressBarUi {
    fn start(&self) -> (UnboundedSender<UiEvent>, JoinHandle<()>) {
        let (ui_tx, mut ui_rx) = tokio::sync::mpsc::unbounded_channel::<UiEvent>();
        let inner = self.inner.clone();
        inner.started.store(true, Ordering::SeqCst);
        let handle = tokio::task::spawn_blocking(move || {
            let mut last_update = Instant::now();
            let mut accumulated_bytes: u64 = 0;
            let mut total_bytes_to_transfer: u64 = 0;
            let mut current_bytes_transferred: u64 = 0;
            let mut files_to_transfer: u64 = 0;
            let mut files_transferred: u64 = 0;

            let mut files_preexisting: u64 = 0;
            let mut data_preexisting: u64 = 0;
            let mut total_files_discovered: u64 = 0;
            let mut total_data_discovered: u64 = 0;
            let mut discovery_finished = false;

            let update_interval = Duration::from_millis(200);

            while let Some(ev) = ui_rx.blocking_recv() {
                match ev {
                    UiEvent::AddToTotal(delta) => {
                        total_bytes_to_transfer += delta;
                        files_to_transfer += 1;
                        inner.transfer_pb.set_length(total_bytes_to_transfer);
                    }
                    UiEvent::Uploaded(delta) => {
                        accumulated_bytes = accumulated_bytes.saturating_add(delta);
                        current_bytes_transferred = current_bytes_transferred.saturating_add(delta);
                    }
                    UiEvent::FileTransferred(_bytes) => {
                        files_transferred += 1;
                    }
                    UiEvent::FileAlreadyAtTarget(bytes) => {
                        files_preexisting += 1;
                        data_preexisting += bytes;
                    }
                    UiEvent::TotalDiscovered(files, bytes) => {
                        total_files_discovered = files;
                        total_data_discovered = bytes;
                        if discovery_finished {
                            inner.overall_pb.set_length(total_data_discovered);
                        } else {
                            inner.overall_pb.set_length(total_data_discovered + 1);
                        }
                    }
                    UiEvent::DiscoveryFinished => {
                        discovery_finished = true;
                        inner.overall_pb.set_length(total_data_discovered);
                    }
                    UiEvent::Message(msg) => {
                        inner.transfer_pb.set_prefix(msg);
                    }
                    UiEvent::Println(msg) => {
                        inner.mp.println(msg).unwrap();
                    }
                }

                let now = Instant::now();
                if now.duration_since(last_update) >= update_interval {
                    // Update transfer progress bar
                    if accumulated_bytes > 0 {
                        inner.transfer_pb.inc(accumulated_bytes);
                        accumulated_bytes = 0;
                    }

                    inner.transfer_pb.set_message(format!(
                        "{}/{} files | {}/{} ({}/s)",
                        files_transferred, files_to_transfer,
                        indicatif::BinaryBytes(current_bytes_transferred),
                        indicatif::BinaryBytes(total_bytes_to_transfer),
                        indicatif::BinaryBytes(inner.transfer_pb.per_sec() as u64),
                    ));

                    // Update overall progress bar
                    let files_at_target = files_preexisting + files_transferred;
                    let data_at_target = data_preexisting + current_bytes_transferred;

                    inner.overall_pb.set_position(data_at_target);

                    inner.overall_pb.set_message(Self::overall_pb_message(files_preexisting, data_preexisting, total_files_discovered, total_data_discovered, files_at_target, data_at_target));

                    last_update = now;
                }
            }

            // Final update
            let files_at_target = files_preexisting + files_transferred;
            let data_at_target = data_preexisting + current_bytes_transferred;

            inner.overall_pb.set_position(data_at_target);
            inner.overall_pb.set_message(Self::overall_pb_message(files_preexisting, data_preexisting, total_files_discovered, total_data_discovered, files_at_target, data_at_target));

            if accumulated_bytes > 0 {
                inner.transfer_pb.inc(accumulated_bytes);
            }

            inner.transfer_pb.set_message(format!(
                "{}/{} files | {}/{} ({}/s)",
                files_transferred, files_to_transfer,
                indicatif::BinaryBytes(current_bytes_transferred),
                indicatif::BinaryBytes(total_bytes_to_transfer),
                indicatif::BinaryBytes(inner.transfer_pb.per_sec() as u64),
            ));

            inner.completed.store(true, Ordering::SeqCst);
        });
        (ui_tx, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pb_ui_total() {
        let ui = ProgressBarUi::new();
        let (ui_tx, handle) = ui.start();
        let message_content = "test".to_string();

        ui_tx.send(UiEvent::Uploaded(10)).unwrap();
        ui_tx.send(UiEvent::AddToTotal(1000)).unwrap();
        ui_tx.send(UiEvent::Uploaded(1000)).unwrap();
        ui_tx.send(UiEvent::AddToTotal(10)).unwrap();
        ui_tx.send(UiEvent::Message(message_content.clone())).unwrap();
        ui_tx.send(UiEvent::TotalDiscovered(2, 2000)).unwrap();
        
        // Before DiscoveryFinished, length should be total_data_discovered + 1
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(ui.inner.overall_pb.length().unwrap(), 2001);
        
        ui_tx.send(UiEvent::DiscoveryFinished).unwrap();

        drop(ui_tx);
        handle.await.unwrap();

        assert_eq!(ui.inner.transfer_pb.position(), 1010);
        assert_eq!(ui.inner.transfer_pb.length().unwrap(), 1010);
        assert_eq!(ui.inner.overall_pb.length().unwrap(), 2000);
        assert_eq!(ui.inner.overall_pb.message(), "At target: 1010 B/1.95 KiB (0/2 files) | preexisting: 0 B");
        // We don't assert the exact throughput as it's timing dependent in tests
        assert!(ui.inner.transfer_pb.message().starts_with("0/2 files | 1010 B/1010 B"));
        assert_eq!(ui.inner.transfer_pb.prefix(), "test"); // The last message sent was "test"
    }
}