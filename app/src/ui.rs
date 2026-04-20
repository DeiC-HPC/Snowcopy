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
    destination_checker_pb: ProgressBar,
    transfer_pb: ProgressBar,
    started: AtomicBool,
    completed: AtomicBool,
    destination_checker_visible: AtomicBool,
}

pub struct ProgressBarUi {
    inner: Arc<ProgressBarUiInner>,
}

impl ProgressBarUi {
    pub fn new() -> Self {
        let mp = MultiProgress::new();
        let destination_checker_pb = ProgressBar::hidden();
        let transfer_pb = mp.add(ProgressBar::new(0));

        destination_checker_pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] Total:    {bar:30.green/blue.} {percent}% {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        destination_checker_pb.set_message("At target: 0 B/0 B | 0/0 files | preexisting: 0 B");

        transfer_pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] Transfer: {bar:30.cyan/blue.} {percent}% {prefix}: {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        transfer_pb.set_prefix("Discovering");

        Self {
            inner: Arc::new(ProgressBarUiInner {
                mp,
                destination_checker_pb,
                transfer_pb,
                started: AtomicBool::new(false),
                completed: AtomicBool::new(false),
                destination_checker_visible: AtomicBool::new(false),
            }),
        }
    }

    pub fn finish(&self, msg: &str) {
        if self.inner.started.load(Ordering::SeqCst) && !self.inner.completed.load(Ordering::SeqCst) {
            eprintln!("Warning: finish() called before UI thread has completed. This may result in incorrect reporting of progress.");
        }

        self.inner.transfer_pb.set_prefix(msg.to_string());
        self.inner.transfer_pb.finish();
        self.inner.destination_checker_pb.finish();
    }

    fn format_overall_pb_message(files_preexisting: u64, data_preexisting: u64, total_files_discovered: u64, total_data_discovered: u64, files_at_target: u64, data_at_target: u64) -> String {
        format!(
            "At target: {}/{} ({}/{} files) | Existing {} files, {}",
            indicatif::BinaryBytes(data_at_target),
            indicatif::BinaryBytes(total_data_discovered),
            files_at_target, total_files_discovered,
            files_preexisting,
            indicatif::BinaryBytes(data_preexisting),
        )
    }

    fn show_destination_checker_bar_if_not_already_visible(inner: &Arc<ProgressBarUiInner>) {
        if !inner.destination_checker_visible.load(Ordering::SeqCst) {
            inner.mp.insert(0, inner.destination_checker_pb.clone());
            inner.destination_checker_visible.store(true, Ordering::SeqCst);
        }
    }

    fn format_transfer_message(inner: &Arc<ProgressBarUiInner>, total_bytes_to_transfer: u64, current_bytes_transferred: u64, files_to_transfer: u64, files_transferred: u64) -> String {
        format!(
            "{}/{} files | {}/{} ({}/s)",
            files_transferred, files_to_transfer,
            indicatif::BinaryBytes(current_bytes_transferred),
            indicatif::BinaryBytes(total_bytes_to_transfer),
            indicatif::BinaryBytes(inner.transfer_pb.per_sec() as u64),
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
            let mut current_bytes_transferred: u64 = 0;
            let mut files_transferred: u64 = 0;

            let mut files_preexisting: u64 = 0;
            let mut data_preexisting: u64 = 0;
            let mut total_files_discovered: u64 = 0;
            let mut total_data_discovered: u64 = 0;
            let mut _discovery_finished = false;
            
            //Save last values so we know if we need to render something new.
            let mut last_destination_checker_msg = String::new();
            let mut last_transfer_msg = String::new();
            let mut last_destination_checker_position: u64 = 0;
            let mut last_destination_checker_length: u64 = 0;
            let mut last_transfer_length: u64 = 0;

            let update_interval = Duration::from_millis(250);

            while let Some(ev) = ui_rx.blocking_recv() {
                match ev {
                    UiEvent::TransferredData(delta) => {
                        accumulated_bytes = accumulated_bytes.saturating_add(delta);
                        current_bytes_transferred = current_bytes_transferred.saturating_add(delta);
                    }
                    UiEvent::FileTransferred(_bytes) => {
                        files_transferred += 1;
                    }
                    UiEvent::FileAlreadyAtTarget(bytes) => {
                        files_preexisting += 1;
                        data_preexisting += bytes;
                        Self::show_destination_checker_bar_if_not_already_visible(&inner); //only shown if something exists externally already
                    }
                    UiEvent::FileDiscovered(bytes) => {
                        total_files_discovered += 1;
                        total_data_discovered += bytes;
                    }
                    UiEvent::DiscoveryFinished => {
                        _discovery_finished = true; //TODO Jon: I'm unsure which class should take responsibility for reacting to this.
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
                    let files_to_transfer = total_files_discovered - files_preexisting;
                    let total_bytes_to_transfer = total_data_discovered - data_preexisting;

                    // Update destination checker progress bar
                    let files_at_target = files_preexisting + files_transferred;
                    let data_at_target = data_preexisting + current_bytes_transferred;

                    if data_at_target != last_destination_checker_position {
                        inner.destination_checker_pb.set_position(data_at_target);
                        last_destination_checker_position = data_at_target;
                    }
                    let new_destination_msg = Self::format_overall_pb_message(files_preexisting, data_preexisting, total_files_discovered, total_data_discovered, files_at_target, data_at_target);
                    if last_destination_checker_msg != new_destination_msg {
                        inner.destination_checker_pb.set_message(new_destination_msg.clone());
                        last_destination_checker_msg = new_destination_msg;
                    }
                    if total_data_discovered != last_destination_checker_length {
                        inner.destination_checker_pb.set_length(total_data_discovered);
                        last_destination_checker_length = total_data_discovered;
                    }

                    if total_bytes_to_transfer != last_transfer_length {
                        inner.transfer_pb.set_length(total_bytes_to_transfer);
                        last_transfer_length = total_bytes_to_transfer;
                    }
                    // Update transfer progress bar
                    if accumulated_bytes > 0 {
                        inner.transfer_pb.inc(accumulated_bytes);
                        accumulated_bytes = 0;
                    }
                    let new_transfer_msg = Self::format_transfer_message(&inner, total_bytes_to_transfer, current_bytes_transferred, files_to_transfer, files_transferred);
                    if last_transfer_msg != new_transfer_msg {
                        inner.transfer_pb.set_message(new_transfer_msg.clone());
                        last_transfer_msg = new_transfer_msg;
                    }

                    last_update = now;
                }
            }

            // Final update
            let files_at_target = files_preexisting + files_transferred;
            let data_at_target = data_preexisting + current_bytes_transferred;
            let files_to_transfer = total_files_discovered - files_preexisting;
            let total_bytes_to_transfer = total_data_discovered - data_preexisting;

            if data_at_target != last_destination_checker_position {
                inner.destination_checker_pb.set_position(data_at_target);
            }
            let new_destination_msg = Self::format_overall_pb_message(files_preexisting, data_preexisting, total_files_discovered, total_data_discovered, files_at_target, data_at_target);
            if last_destination_checker_msg != new_destination_msg {
                inner.destination_checker_pb.set_message(new_destination_msg);
            }
            if total_data_discovered != last_destination_checker_length {
                inner.destination_checker_pb.set_length(total_data_discovered);
            }

            if accumulated_bytes > 0 {
                inner.transfer_pb.inc(accumulated_bytes);
            }
            let new_transfer_msg = Self::format_transfer_message(&inner, total_bytes_to_transfer, current_bytes_transferred, files_to_transfer, files_transferred);
            if last_transfer_msg != new_transfer_msg {
                inner.transfer_pb.set_message(new_transfer_msg);
            }

            inner.completed.store(true, Ordering::SeqCst);
        });
        (ui_tx, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_pb_ui_visibility() {
        let ui = ProgressBarUi::new();
        let (ui_tx, handle) = ui.start();

        // Initially hidden
        assert!(!ui.inner.destination_checker_visible.load(Ordering::SeqCst));

        // After some uploads, still hidden
        ui_tx.send(UiEvent::FileDiscovered(100)).unwrap();
        ui_tx.send(UiEvent::FileTransferred(100)).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!ui.inner.destination_checker_visible.load(Ordering::SeqCst));

        // After FileAlreadyAtTarget, it should become visible
        ui_tx.send(UiEvent::FileAlreadyAtTarget(100)).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(ui.inner.destination_checker_visible.load(Ordering::SeqCst));

        drop(ui_tx);
        handle.await.unwrap();
    }
}