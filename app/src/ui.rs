use std::thread;
use std::time::Duration;
use indicatif::ProgressBar;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub enum UiEvent {
    AddToTotal(u64),
    Uploaded(u64),
    Message(String),
}
pub fn start_ui_task(mut ui_rx: UnboundedReceiver<UiEvent>, pb_ui: ProgressBar) -> JoinHandle<()> {
    let ui_handle = tokio::task::spawn_blocking(move || {
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
    ui_handle
}