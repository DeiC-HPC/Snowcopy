use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio::time::Instant;
pub(crate) use util_layer::ui::UiEvent;

pub trait Ui {
    fn start(&self) -> (UnboundedSender<UiEvent>, JoinHandle<()>);
}

struct ProgressBarUiInner {
    pb: ProgressBar,
    started: AtomicBool,
    completed: AtomicBool,
}

pub struct ProgressBarUi {
    inner: Arc<ProgressBarUiInner>,
}

impl ProgressBarUi {
    pub fn new() -> Self {
        let pb = ProgressBar::new(0);
        pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})  Status: {msg:25!} ",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        pb.set_message("Discovery");

        Self {
            inner: Arc::new(ProgressBarUiInner {
                pb,
                started: AtomicBool::new(false),
                completed: AtomicBool::new(false),
            }),
        }
    }

    pub fn finish(&self, msg: &str) {
        if self.inner.started.load(Ordering::SeqCst) && !self.inner.completed.load(Ordering::SeqCst) {
            eprintln!("Warning: finish() called before UI thread has completed. This may result in incorrect reporting of progress.");
        }

        self.inner.pb.finish_with_message(msg.to_string());
    }
}

impl Ui for ProgressBarUi {
    fn start(&self) -> (UnboundedSender<UiEvent>, JoinHandle<()>) {
        let (ui_tx, mut ui_rx) = tokio::sync::mpsc::unbounded_channel::<UiEvent>();
        let inner = self.inner.clone();
        inner.started.store(true, Ordering::SeqCst);
        let handle = tokio::task::spawn_blocking(move || {
            let mut total_set = false;
            let mut total: u64 = 0;
            let mut last_update = Instant::now();
            let mut accumulated_bytes: u64 = 0;
            let mut accumulated_total: u64 = 0;
            let update_interval = Duration::from_millis(200);

            while let Some(ev) = ui_rx.blocking_recv() {
                match ev {
                    UiEvent::AddToTotal(delta) => {
                        total = total.saturating_add(delta);
                        accumulated_total = accumulated_total.saturating_add(delta);
                    }
                    UiEvent::Uploaded(delta) => {
                        accumulated_bytes = accumulated_bytes.saturating_add(delta);
                    }
                    UiEvent::Message(msg) => {
                        inner.pb.set_message(msg);
                    }
                    UiEvent::Println(msg) => {
                        inner.pb.println(msg);
                    }
                }

                let now = Instant::now();
                if now.duration_since(last_update) >= update_interval {
                    // Flush accumulated total
                    if accumulated_total > 0 {
                        if !total_set {
                            inner.pb.set_length(total);
                            total_set = true;
                        } else {
                            inner.pb.inc_length(accumulated_total);
                        }
                        accumulated_total = 0;
                        thread::sleep(Duration::from_millis(10)); //limit flickering
                    }

                    // Flush accumulated bytes
                    if accumulated_bytes > 0 {
                        inner.pb.inc(accumulated_bytes);
                        accumulated_bytes = 0;
                    }

                    last_update = now;
                }
            }

            // Final update to ensure all bytes and total are accounted for
            if accumulated_total > 0 {
                if !total_set {
                    inner.pb.set_length(total);
                } else {
                    inner.pb.inc_length(accumulated_total);
                }
            }
            if accumulated_bytes > 0 {
                inner.pb.inc(accumulated_bytes);
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
    async fn test_pb_ui_total() {
        let ui = ProgressBarUi::new();
        let (ui_tx, handle) = ui.start();
        let message_content = "test".to_string();

        ui_tx.send(UiEvent::Uploaded(10)).unwrap();
        ui_tx.send(UiEvent::AddToTotal(1000)).unwrap();
        ui_tx.send(UiEvent::Uploaded(1000)).unwrap();
        ui_tx.send(UiEvent::AddToTotal(10)).unwrap();
        ui_tx.send(UiEvent::Message(message_content.clone())).unwrap();

        drop(ui_tx);
        handle.await.unwrap();

        assert_eq!(ui.inner.pb.position(), 1010);
        assert_eq!(ui.inner.pb.length().unwrap(), 1010);
        assert_eq!(ui.inner.pb.message(), message_content);
    }
}