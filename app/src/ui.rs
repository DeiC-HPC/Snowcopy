use std::thread;
use std::time::Duration;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio::time::Instant;
pub(crate) use util_layer::ui::UiEvent;

pub trait Ui {
    fn start(&self) -> (UnboundedSender<UiEvent>, JoinHandle<()>);
}

pub struct ProgressBarUi {
    pb: ProgressBar,
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

        Self { pb }
    }

    pub fn finish(&self, msg: &str) {
        self.pb.finish_with_message(msg.to_string());
    }
}

impl Ui for ProgressBarUi {
    fn start(&self) -> (UnboundedSender<UiEvent>, JoinHandle<()>) {
        let (ui_tx, mut ui_rx) = tokio::sync::mpsc::unbounded_channel::<UiEvent>();
        let pb_ui = self.pb.clone();
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
                        pb_ui.set_message(msg);
                    }
                    UiEvent::Println(msg) => {
                        pb_ui.println(msg);
                    }
                }

                let now = Instant::now();
                if now.duration_since(last_update) >= update_interval {
                    // Flush accumulated total
                    if accumulated_total > 0 {
                        if !total_set {
                            pb_ui.set_length(total);
                            total_set = true;
                        } else {
                            pb_ui.inc_length(accumulated_total);
                        }
                        accumulated_total = 0;
                        thread::sleep(Duration::from_millis(10)); //limit flickering
                    }

                    // Flush accumulated bytes
                    if accumulated_bytes > 0 {
                        pb_ui.inc(accumulated_bytes);
                        accumulated_bytes = 0;
                    }

                    last_update = now;
                }
            }

            // Final update to ensure all bytes and total are accounted for
            if accumulated_total > 0 {
                if !total_set {
                    pb_ui.set_length(total);
                } else {
                    pb_ui.inc_length(accumulated_total);
                }
            }
            if accumulated_bytes > 0 {
                pb_ui.inc(accumulated_bytes);
            }
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

        assert_eq!(ui.pb.position(), 1010);
        assert_eq!(ui.pb.length().unwrap(), 1010);
        assert_eq!(ui.pb.message(), message_content);
    }
}