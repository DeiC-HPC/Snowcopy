use std::cmp::PartialEq;
use crate::ui::{ProgressBarUi, Ui, UiEvent};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use util_layer::cc_util::{CommandCenterEvent, Status};
use util_layer::constants::MAX_RETRY;

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
    fn start(&self) -> (UnboundedSender<CommandCenterEvent>, JoinHandle<()>);
}

pub struct CommandCenter {
    silent : bool,
    stats: bool,
    mode: Mode,
}


impl CommandCenter {
    pub fn new_upload(config: &util_layer::UploadConfig) -> Self {
        CommandCenter {
            silent: config.silent,
            stats: config.stats,
            mode: Mode::Upload,
        }
    }

    pub fn new_download(config: &util_layer::DownloadConfig) -> Self {
        CommandCenter {
            silent: config.silent,
            stats: config.stats,
            mode: Mode::Download,
        }
    }

    #[allow(dead_code)]
    pub fn new_list(config: &util_layer::ListConfig) -> Self {
        CommandCenter {
            silent: config.silent,
            stats: false,
            mode: Mode::List,
        }
    }

    #[allow(dead_code)]
    pub fn new_remove(config: &util_layer::RemoveConfig) -> Self {
        CommandCenter {
            silent: config.silent,
            stats: false,
            mode: Mode::Remove,
        }
    }
}

impl CC for CommandCenter {
    fn start(&self) -> (UnboundedSender<CommandCenterEvent>, JoinHandle<()>) {
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
        let handle = tokio::task::spawn(async move {
            let mut file_transferred_count = 0u64;
            let mut total_integrity_checked_files = 0u64;
            let mut total_retries = 0u64;
            let mut total_file_permit_time = 0f64;
            let mut total_req_permit_time = 0f64;
            let mut total_file_work_time = 0f64;
            let mut total_req_work_time = 0f64;

            let _ = ui_tx.send(UiEvent::Message(Status::Discovering.to_string()));

            while let Some(ev) = cc_rx.recv().await {
                match ev {
                    CommandCenterEvent::PartTransferred(metrics) => {
                        total_file_permit_time += metrics.file_sem_acquire_time_secs;
                        total_req_permit_time += metrics.req_sem_acquire_time_secs;
                        total_file_work_time += metrics.file_sem_work_time_secs;
                        total_req_work_time += metrics.req_sem_work_time_secs;

                        let _ = ui_tx.send(UiEvent::TransferredData(metrics.amount));
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
                            panic!("{}: Retry amount {} exceeded MAX_RETRY {}", message, attempts, MAX_RETRY);
                        }
                        total_retries += 1;
                        let initial_delay = Duration::from_secs(1);
                        let delay = initial_delay * 2u32.pow(attempts - 1);
                        let _ = tx.send(delay);
                    }
                    CommandCenterEvent::FileIntegrityChecked(checked) => { //TODO JON: This could be part of FileTransferred, but it have some issues with getting the information elegantly to the independent writer threads
                        if checked{
                            total_integrity_checked_files += 1;
                        }
                    }
                }
            }

            //We are out of the loop, the program is terminating:

            if stats {
                ui_tx.send(UiEvent::Println(format!("Total files completed: {}", file_transferred_count))).expect("Should never fail");
                ui_tx.send(UiEvent::Println(format!("Total time spent acquiring file permits: {:.2}s", total_file_permit_time))).expect("Should never fail"); //TODO Jon: Is this metric actually relevant?
                ui_tx.send(UiEvent::Println(format!("Total time spent acquiring request permits: {:.2}s", total_req_permit_time))).expect("Should never fail");
                ui_tx.send(UiEvent::Println(format!("Total time spent working on disk I/O: {:.2}s", total_file_work_time))).expect("Should never fail");
                ui_tx.send(UiEvent::Println(format!("Total time spent working on request I/O: {:.2}s", total_req_work_time))).expect("Should never fail");
            }

            drop(ui_tx); //This allows the UI task to exit gracefully when all files have been uploaded
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

        });


        (cc_tx, handle)
    }
}




