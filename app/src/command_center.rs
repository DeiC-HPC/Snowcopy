use std::fmt;
use tokio::sync::mpsc::{UnboundedSender};
use tokio::task::JoinHandle;
use crate::ui::{ProgressBarUi, Ui, UiEvent};

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

impl UiSender {
    fn send(&self, event: UiEvent) -> Result<(), ()> {
        match self {
            UiSender::Real(tx) => tx.send(event).map_err(|_| ()),
            UiSender::Noop(noop) => noop.send(event),
        }
    }
}

pub enum CommandCenterEvent {
    FileTransferred(u64),
    FileDiscovered(u64),
    StatusChanged(Status),
    Log(String),
}

#[allow(dead_code)] //TODO Jon: not all status are used currently
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
            (None, UiSender::Noop(NoopUiSender), None)
        } else {
            let ui = ProgressBarUi::new();
            let (tx, handle) = ui.start();
            (Some(ui), UiSender::Real(tx), Some(handle))
        };

        let handle = tokio::task::spawn(async move {
            let mut file_transferred_count = 0u64;
            while let Some(ev) = cc_rx.recv().await {
                match ev {
                    CommandCenterEvent::FileTransferred(bytes) => { //TODO Jon: collect metrics
                        file_transferred_count += 1;

                        let _ = ui_tx.send(UiEvent::Uploaded(bytes));
                    }
                    CommandCenterEvent::FileDiscovered(bytes) => {
                        let _ = ui_tx.send(UiEvent::AddToTotal(bytes));
                    }
                    CommandCenterEvent::StatusChanged(status) => {
                        let _ = ui_tx.send(UiEvent::Message(status.to_string()));
                    }
                    CommandCenterEvent::Log(string) => {
                        let _ = ui_tx.send(UiEvent::Println(string));
                    }
                }
            }

            let _ = ui_tx.send(UiEvent::Println(format!("Total files/chunks transferred: {}", file_transferred_count)));

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




