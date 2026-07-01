use std::cmp::PartialEq;
use crate::ui::{ProgressBarUi, Ui, UiEvent};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use util_layer::cc_util::{CommandCenterEvent, Status};
use util_layer::constants::MAX_RETRY;
use util_layer::error_codes::{FatalError};

struct NoopUiSender;

impl NoopUiSender {
    fn send(&self, _event: UiEvent) -> Result<(), ()> {
        Ok(())
    }
}

enum UiSender {
    Real(UnboundedSender<UiEvent>),
    Noop(NoopUiSender),
}

#[derive(PartialEq, Eq, Clone)]
pub enum Mode {
    Upload,
    Download,
    List,
    Remove,
}

impl UiSender {
    fn send(&self, event: UiEvent) -> Result<(), ()> {
        match self {
            UiSender::Real(tx) => tx.send(event).map_err(|_| ()),
            UiSender::Noop(noop) => noop.send(event),
        }
    }
}

pub trait CC {
    fn start(&self) -> (UnboundedSender<CommandCenterEvent>, JoinHandle<Option<FatalError>>);
}

pub struct CommandCenter {
    silent : bool,
    stats: bool,
    mode: Mode,
    cancellation_token_of_all_ongoing_tasks: tokio_util::sync::CancellationToken,
}


impl CommandCenter {
    pub fn new_upload(config: &util_layer::UploadConfig, cancellation_token_of_all_ongoing_tasks : tokio_util::sync::CancellationToken) -> Self {
        CommandCenter {
            silent: config.silent,
            stats: config.stats,
            mode: Mode::Upload,
            cancellation_token_of_all_ongoing_tasks
        }
    }

    pub fn new_download(config: &util_layer::DownloadConfig, cancellation_token_of_all_ongoing_tasks : tokio_util::sync::CancellationToken) -> Self {
        CommandCenter {
            silent: config.silent,
            stats: config.stats,
            mode: Mode::Download,
            cancellation_token_of_all_ongoing_tasks
        }
    }

    #[allow(dead_code)]
    pub fn new_list(config: &util_layer::ListConfig, cancellation_token_of_all_ongoing_tasks : tokio_util::sync::CancellationToken) -> Self {
        CommandCenter {
            silent: config.silent,
            stats: false,
            mode: Mode::List,
            cancellation_token_of_all_ongoing_tasks
        }
    }

    #[allow(dead_code)]
    pub fn new_remove(config: &util_layer::RemoveConfig, cancellation_token_of_all_ongoing_tasks : tokio_util::sync::CancellationToken) -> Self {
        CommandCenter {
            silent: config.silent,
            stats: false,
            mode: Mode::Remove,
            cancellation_token_of_all_ongoing_tasks
        }
    }
}

impl CC for CommandCenter {
    fn start(&self) -> (UnboundedSender<CommandCenterEvent>, JoinHandle<Option<FatalError>>) {
        let (cc_tx, mut cc_rx) = tokio::sync::mpsc::unbounded_channel::<CommandCenterEvent>();

        let (ui, ui_tx, ui_handle) = if self.silent {
            (None, UiSender::Noop(NoopUiSender), None)
        } else {
            let ui = ProgressBarUi::new();
            let (tx, handle) = ui.start();
            (Some(ui), UiSender::Real(tx), Some(handle))
        };

        let mode = self.mode.clone();
        let stats = self.stats;
        let cancellation_token_of_all_ongoing_tasks = self.cancellation_token_of_all_ongoing_tasks.clone();
        let handle = tokio::task::spawn(async move {
            let mut file_transferred_count = 0u64;
            let mut total_integrity_checked_files = 0u64;
            let mut total_retries = 0u64;
            let mut fatal_error : Option<FatalError> = None;

            let _ = ui_tx.send(UiEvent::Message(Status::Discovering.to_string()));

            while let Some(ev) = cc_rx.recv().await {
                match ev {
                    CommandCenterEvent::PartTransferred(amount) => {
                        let _ = ui_tx.send(UiEvent::TransferredData(amount));
                    }
                    CommandCenterEvent::FileTransferred(bytes) => {
                        file_transferred_count += 1;
                        let _ = ui_tx.send(UiEvent::FileTransferred(bytes));
                    }
                    CommandCenterEvent::FileDiscovered(bytes) => {
                        let _ = ui_tx.send(UiEvent::FileDiscovered(bytes));
                    }
                    CommandCenterEvent::FileAlreadyAtTarget(bytes) => {
                        let _ = ui_tx.send(UiEvent::FileAlreadyAtTarget(bytes));
                    }
                    CommandCenterEvent::StatusChanged(status) => {
                        if let Status::Transferring = status { //TODO JON: Who should be responsible of this knowledge? CC should now, but its the discovery threads, that knows, whom sends the message.
                            // When we switch to transferring, we know that all discovered files
                            // are either already at target or will be transferred.
                            let _ = ui_tx.send(UiEvent::DiscoveryFinished);
                        }
                        let _ = ui_tx.send(UiEvent::Message(status.to_string()));
                    }
                    CommandCenterEvent::Log(string) => {
                        let _ = ui_tx.send(UiEvent::Println(string));
                    }
                    CommandCenterEvent::RetryRequested(tx, attempts, _time, message) => { //TODO Jon: later we should add some metrics and use them to get an idea of the entire stack
                        if attempts > MAX_RETRY {
                            let _ = ui_tx.send(UiEvent::Println(format!(
                                "{}: Retry amount {} exceeded MAX_RETRY {}. Not retrying.",
                                message, attempts, MAX_RETRY
                            )));
                            drop(tx);  //tx is dropped; thus the retryer knows it cannot continue. 
                            continue;
                        }
                        total_retries += 1;
                        let initial_delay = Duration::from_millis(100);
                        let delay = (initial_delay * 2u32.pow(attempts - 1)).min(Duration::from_secs(60)); //max delay is 60 seconds

                        let _ = tx.send(delay);
                    }
                    CommandCenterEvent::FileIntegrityChecked(checked) => { //TODO JON: This could be part of FileTransferred, but it have some issues with getting the information elegantly to the independent writer threads
                        if checked{
                            total_integrity_checked_files += 1;
                        }
                    }
                    CommandCenterEvent::FatalErrorEvent(err) => {
                        let _ = ui_tx.send(UiEvent::Println("FATAL ERROR: Shutting down.".to_string()));
                        cancellation_token_of_all_ongoing_tasks.cancel();
                        if fatal_error.is_none() {
                            fatal_error = Some(err);
                        }
                    }
                }
            }

            //We are out of the loop, the program is terminating:
            if stats {
                ui_tx.send(UiEvent::Println(format!("Total files completed: {}", file_transferred_count))).expect("Should never fail");
            }

            drop(ui_tx); //This allows the UI task to exit gracefully
            if let Some(ui_handle) = ui_handle {
                let _ = ui_handle.await;
            }
            if let Some(ui) = ui {
                ui.finish(&Status::Finished.to_string()); //This must be done after the UI thread is done, as we experienced incorrect reporting due to race conditions otherwise
            }

            if mode == Mode::Download && file_transferred_count > 0 && file_transferred_count != total_integrity_checked_files {
                println!("Total files integrity checked: {}/{}", total_integrity_checked_files, file_transferred_count);
            }
            if total_retries > 0 { //TODO Jon: Should this just be part of UI bar? Yeeesss
                println!("Total retries: {}", total_retries);
            }

            fatal_error
        });

        (cc_tx, handle)
    }
}




