use chrono::Local;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use std::io::IsTerminal;
pub(crate) use util_layer::ui::UiEvent;

struct ProgressBarUiInner {
    mp: MultiProgress,
    destination_checker_pb: ProgressBar,
    transfer_pb: ProgressBar,
    started: AtomicBool,
    completed: AtomicBool,
    destination_checker_visible: AtomicBool,
    is_terminal: bool,
    state: Mutex<UiState>,
}

#[derive(Default, Debug, Clone)]
pub struct UiState {
    pub accumulated_bytes: u64,
    pub current_bytes_transferred: u64,
    pub files_transferred: u64,
    pub files_preexisting: u64,
    pub data_preexisting: u64,
    pub total_files_discovered: u64,
    pub total_data_discovered: u64,
    pub message: String,
}

impl UiState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn handle_event(&mut self, ev: UiEvent) {
        match ev {
            UiEvent::TransferredData(delta) => {
                self.accumulated_bytes = self.accumulated_bytes.saturating_add(delta);
                self.current_bytes_transferred = self.current_bytes_transferred.saturating_add(delta);
            }
            UiEvent::FileTransferred() => {
                self.files_transferred += 1;
            }
            UiEvent::FileAlreadyAtTarget(bytes) => {
                self.files_preexisting += 1;
                self.data_preexisting += bytes;
            }
            UiEvent::FileDiscovered(bytes) => {
                self.total_files_discovered += 1;
                self.total_data_discovered += bytes;
            }
            UiEvent::Message(msg) => {
                self.message = msg;
            }
            UiEvent::Println(_) => {}
        }
    }

    pub fn files_to_transfer(&self) -> u64 {
        self.total_files_discovered.saturating_sub(self.files_preexisting)
    }

    pub fn total_bytes_to_transfer(&self) -> u64 {
        self.total_data_discovered.saturating_sub(self.data_preexisting)
    }

    pub fn files_at_target(&self) -> u64 {
        self.files_preexisting + self.files_transferred
    }

    pub fn data_at_target(&self) -> u64 {
        self.data_preexisting + self.current_bytes_transferred
    }
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

        let is_terminal = std::io::stderr().is_terminal(); //TODO JON: perhaps hide progress bars?
        Self {
            inner: Arc::new(ProgressBarUiInner {
                mp,
                destination_checker_pb,
                transfer_pb,
                started: AtomicBool::new(false),
                completed: AtomicBool::new(false),
                destination_checker_visible: AtomicBool::new(false),
                is_terminal,
                state: Mutex::new(UiState::new()),
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

    fn format_total_pb_message(files_preexisting: u64, data_preexisting: u64, total_files_discovered: u64, total_data_discovered: u64, files_at_target: u64, data_at_target: u64) -> String {
        format!(
            "At target: {}/{} ({}/{} files) | Existing {} files, {}",
            indicatif::BinaryBytes(data_at_target),
            indicatif::BinaryBytes(total_data_discovered),
            files_at_target, total_files_discovered,
            files_preexisting,
            indicatif::BinaryBytes(data_preexisting),
        )
    }

    fn show_destination_checker_bar_if_needed(inner: &Arc<ProgressBarUiInner>, files_preexisting: u64) {
        if files_preexisting > 0 && !inner.destination_checker_visible.load(Ordering::SeqCst) {
            inner.mp.insert(0, inner.destination_checker_pb.clone());
            inner.destination_checker_visible.store(true, Ordering::SeqCst);
        }
    }

    fn format_transfer_pb_message(inner: &Arc<ProgressBarUiInner>, total_bytes_to_transfer: u64, current_bytes_transferred: u64, files_to_transfer: u64, files_transferred: u64) -> String {
        format!(
            "{}/{} files | {}/{} ({}/s)",
            files_transferred, files_to_transfer,
            indicatif::BinaryBytes(current_bytes_transferred),
            indicatif::BinaryBytes(total_bytes_to_transfer),
            indicatif::BinaryBytes(inner.transfer_pb.per_sec() as u64),
        )
    }

    fn render(&self) {
        let state = self.inner.state.lock().unwrap().clone();
        let inner = &self.inner;

        let files_to_transfer = state.files_to_transfer();
        let total_bytes_to_transfer = state.total_bytes_to_transfer();

        // Update destination checker progress bar
        let files_at_target = state.files_at_target();
        let data_at_target = state.data_at_target();

        Self::show_destination_checker_bar_if_needed(inner, state.files_preexisting);

        inner.destination_checker_pb.set_position(data_at_target);
        inner.destination_checker_pb.set_length(state.total_data_discovered);
        inner.destination_checker_pb.set_message(Self::format_total_pb_message(
            state.files_preexisting,
            state.data_preexisting,
            state.total_files_discovered,
            state.total_data_discovered,
            files_at_target,
            data_at_target,
        ));

        inner.transfer_pb.set_length(total_bytes_to_transfer);
        inner.transfer_pb.set_position(state.current_bytes_transferred);
        inner.transfer_pb.set_prefix(state.message.clone());
        inner.transfer_pb.set_message(Self::format_transfer_pb_message(
            inner,
            total_bytes_to_transfer,
            state.current_bytes_transferred,
            files_to_transfer,
            state.files_transferred,
        ));
    }

    pub fn start(&self) -> (UnboundedSender<UiEvent>, JoinHandle<()>) {
        let (ui_tx, mut ui_rx) = tokio::sync::mpsc::unbounded_channel::<UiEvent>();
        let inner = self.inner.clone();
        inner.started.store(true, Ordering::SeqCst);

        let ui_clone = ProgressBarUi { inner: inner.clone() };
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(250));

            loop {
                tokio::select! {
                    ev = ui_rx.recv() => {
                        let ev = match ev {
                            Some(ev) => ev,
                            None => break,
                        };
                        match &ev {
                            UiEvent::Println(msg) => {
                                let timestamp = Local::now().format("%H:%M:%S");
                                if inner.is_terminal {
                                    inner.mp.println(format!("[{}] {}", timestamp, msg)).unwrap();
                                } else {
                                    println!("[{}] {}", timestamp, msg);
                                }
                            }
                            _ => {}
                        }
                        inner.state.lock().unwrap().handle_event(ev);
                    }
                    _ = ticker.tick() => {
                        ui_clone.render();
                    }
                }
            }

            // Final update
            ui_clone.render();

            inner.completed.store(true, Ordering::SeqCst);
            if !inner.is_terminal {
                if inner.destination_checker_visible.load(Ordering::SeqCst) {
                    eprintln!("[{}] Total:    {}", Local::now().format("%H:%M:%S"), inner.destination_checker_pb.message());
                }
                eprintln!("[{}] Transfer: {}: {}", Local::now().format("%H:%M:%S"), inner.transfer_pb.prefix(), inner.transfer_pb.message());
            }
        });
        (ui_tx, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ui_state_discovery_increases_amount() {
        let mut state = UiState::new();
        state.handle_event(UiEvent::FileDiscovered(100));
        assert_eq!(state.total_files_discovered, 1);
        assert_eq!(state.total_data_discovered, 100);
        assert_eq!(state.total_bytes_to_transfer(), 100);

        state.handle_event(UiEvent::FileDiscovered(200));
        assert_eq!(state.total_files_discovered, 2);
        assert_eq!(state.total_data_discovered, 300);
        assert_eq!(state.total_bytes_to_transfer(), 300);
    }

    #[test]
    fn test_ui_state_file_transferred_updates_bar() {
        let mut state = UiState::new();
        state.handle_event(UiEvent::FileDiscovered(100));
        state.handle_event(UiEvent::TransferredData(100));
        state.handle_event(UiEvent::FileTransferred());
        assert_eq!(state.files_transferred, 1);
        assert_eq!(state.current_bytes_transferred, 100);
        assert_eq!(state.files_at_target(), 1);
        assert_eq!(state.data_at_target(), 100);
    }

    #[test]
    fn test_ui_state_file_already_at_target() {
        let mut state = UiState::new();
        state.handle_event(UiEvent::FileDiscovered(100));
        state.handle_event(UiEvent::FileAlreadyAtTarget(100));
        assert_eq!(state.files_preexisting, 1);
        assert_eq!(state.data_preexisting, 100);
        assert_eq!(state.total_bytes_to_transfer(), 0);
        assert_eq!(state.files_at_target(), 1);
        assert_eq!(state.data_at_target(), 100);
    }

    #[test]
    fn test_ui_state_message_sent_is_captured() {
        let mut state = UiState::new();
        state.handle_event(UiEvent::Message("Transferring...".to_string()));
        assert_eq!(state.message, "Transferring...");
    }

    #[test]
    fn test_ui_state_progress_ranges() {
        let mut state = UiState::new();
        state.handle_event(UiEvent::FileDiscovered(100)); // Total: 1 file, 100 bytes
        state.handle_event(UiEvent::FileDiscovered(200)); // Total: 2 files, 300 bytes
        
        state.handle_event(UiEvent::FileAlreadyAtTarget(100)); // 1 file pre-existing
        
        // Transfer bar should have 300 - 100 = 200 bytes to transfer
        assert_eq!(state.total_bytes_to_transfer(), 200);
        
        // Destination bar should have 300 bytes total range
        assert_eq!(state.total_data_discovered, 300);
        
        state.handle_event(UiEvent::TransferredData(50));
        // Destination bar position: 100 (pre-existing) + 50 (transferred) = 150
        assert_eq!(state.data_at_target(), 150);
    }

    #[tokio::test]
    async fn test_pb_ui_visibility() {
        let ui = ProgressBarUi::new();
        let (ui_tx, handle) = ui.start();

        // Initially hidden
        assert!(!ui.inner.destination_checker_visible.load(Ordering::SeqCst));

        ui_tx.send(UiEvent::FileDiscovered(100)).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        // Still hidden as files_preexisting is 0
        assert!(!ui.inner.destination_checker_visible.load(Ordering::SeqCst));

        // Now trigger preexisting file
        ui_tx.send(UiEvent::FileAlreadyAtTarget(50)).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        
        // Should be visible now
        assert!(ui.inner.destination_checker_visible.load(Ordering::SeqCst));

        drop(ui_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_pb_ui_actual_rendered_output() {
        use indicatif::{InMemoryTerm, ProgressDrawTarget};
        
        let term = InMemoryTerm::new(10, 100);
        let mp = MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(term.clone())));
        
        let destination_checker_pb = mp.add(ProgressBar::new(80));
        let transfer_pb = mp.add(ProgressBar::new(80));

        destination_checker_pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] Total:    {bar:30.green/blue.} {percent}% {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        destination_checker_pb.set_message("At target: 150 B/300 B (2/2 files) | Existing 1 files, 100 B");
        destination_checker_pb.set_position(150);

        transfer_pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] Transfer: {bar:30.cyan/blue.} {percent}% {prefix}: {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        transfer_pb.set_prefix("Transferring");
        transfer_pb.set_message("1/1 files | 50 B/200 B (0 B/s)");
        transfer_pb.set_position(50);

        // Tick to ensure it renders
        destination_checker_pb.tick();
        transfer_pb.tick();

        let contents = term.contents();
        assert!(contents.contains("Total:"));
        assert!(contents.contains("Transfer:"));
        assert!(contents.contains("Transferring: 1/1 files | 50 B/200 B"));
        assert!(contents.contains("At target: 150 B/300 B"));
        assert!(contents.contains("#####"));
    }

    #[tokio::test]
    async fn test_ui_message_updates_transfer_prefix() {
        let ui = ProgressBarUi::new();
        let (ui_tx, handle) = ui.start();

        ui_tx.send(UiEvent::Message("Transferring file X".to_string())).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        
        assert_eq!(ui.inner.transfer_pb.prefix(), "Transferring file X");

        drop(ui_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_ui_println_does_not_panic() {
        let ui = ProgressBarUi::new();
        let (ui_tx, handle) = ui.start();

        ui_tx.send(UiEvent::Println("Some informational message".to_string())).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        drop(ui_tx);
        handle.await.unwrap();
    }
}