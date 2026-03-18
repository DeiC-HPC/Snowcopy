use std::fmt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use crate::ui::{ProgressBarUi, Ui, UiEvent};

pub enum CommandCenterEvent {
    FileTransferred(u64),
    FileDiscovered(u64),
    StatusChanged(Status),
    Log(String),
}

pub enum Status {
    Idle,
    Transferring,
    Discovering,
    Finished,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Idle => write!(f, "Idle"),
            Status::Transferring => write!(f, "Transferring"),
            Status::Discovering => write!(f, "Discovering"),
            Status::Finished => write!(f, "Finished"),
        }
    }
}

pub trait CC {
    fn start(&self) -> (UnboundedSender<CommandCenterEvent>, JoinHandle<()>);
}

pub struct CommandCenter {
    silent : bool,
}

impl CommandCenter {
    pub fn new(silent : bool) -> Self {
        CommandCenter { silent }
    }

}

impl CC for CommandCenter {
    fn start(&self) -> (UnboundedSender<CommandCenterEvent>, JoinHandle<()>) {
        let (cc_tx, mut cc_rx) = tokio::sync::mpsc::unbounded_channel::<CommandCenterEvent>();

        let (ui, ui_tx, ui_handle) = if self.silent {
            (None, None, None)
        } else {
            let ui = ProgressBarUi::new();
            let (tx, handle) = ui.start();
            (Some(ui), Some(tx), Some(handle))
        };

        let handle = tokio::task::spawn(async move {
            let mut file_transferred_count = 0u64;
            while let Some(ev) = cc_rx.recv().await {
                match ev {
                    CommandCenterEvent::FileTransferred(bytes) => { //TODO Jon: collect metrics
                        file_transferred_count += 1;

                        if let Some(ref ui_tx) = ui_tx {
                            ui_tx.send(UiEvent::Uploaded(bytes)).unwrap();
                        }
                    }
                    CommandCenterEvent::FileDiscovered(bytes) => {
                        if let Some(ref ui_tx) = ui_tx {
                            ui_tx.send(UiEvent::AddToTotal(bytes)).unwrap();
                        }
                    }
                    CommandCenterEvent::StatusChanged(status) => {
                        if let Some(ref ui_tx) = ui_tx {
                            ui_tx.send(UiEvent::Message(status.to_string())).unwrap();
                        }
                    }
                    CommandCenterEvent::Log(string) => {
                        if let Some(ref ui_tx) = ui_tx {
                            ui_tx.send(UiEvent::Println(string)).unwrap();
                        }
                    }
                }
            }

            if let Some(ref ui_tx) = ui_tx {
                ui_tx.send(UiEvent::Println(format!("Total files/chunks transferred: {}", file_transferred_count))).unwrap();
            }

            drop(ui_tx); //This allows the UI task to exit gracefully when all files have been uploaded

            if let Some(ui_handle) = ui_handle {
                let _ = ui_handle.await;
            }

            if let Some(ui) = ui {
                ui.finish("Finished"); //This must be done after the UI thread is done, as we experienced incorrect reporting due to race conditions otherwise
            }
        });


        (cc_tx, handle)
    }
}




