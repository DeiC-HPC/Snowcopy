use crate::ui::{ProgressBarUi, Ui, UiEvent};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use util_layer::cc_util::{CommandCenterEvent, Status};

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

pub trait CC {
    fn start(&self) -> (UnboundedSender<CommandCenterEvent>, JoinHandle<()>);
}

pub struct CommandCenter {
    silent : bool,
    stats: bool,
}

impl CommandCenter {
    pub fn new(silent : bool, stats: bool) -> Self {
        CommandCenter { silent, stats }
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

        let stats = self.stats;
        let handle = tokio::task::spawn(async move {
            let mut file_transferred_count = 0u64;
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
                }
            }

            if stats {
                let _ = ui_tx.send(UiEvent::Println(format!("Total files completed: {}", file_transferred_count)));
                let _ = ui_tx.send(UiEvent::Println(format!("Total time spent acquiring file permits: {:.2}s", total_file_permit_time))); //TODO Jon: Is this metric actually relevant?
                let _ = ui_tx.send(UiEvent::Println(format!("Total time spent acquiring request permits: {:.2}s", total_req_permit_time)));
                let _ = ui_tx.send(UiEvent::Println(format!("Total time spent working on disk I/O: {:.2}s", total_file_work_time)));
                let _ = ui_tx.send(UiEvent::Println(format!("Total time spent working on request I/O: {:.2}s", total_req_work_time)));
            }

            drop(ui_tx); //This allows the UI task to exit gracefully when all files have been uploaded
            if let Some(ui_handle) = ui_handle {
                let _ = ui_handle.await;
            }
            if let Some(ui) = ui {
                ui.finish(&Status::Finished.to_string()); //This must be done after the UI thread is done, as we experienced incorrect reporting due to race conditions otherwise
            }
        });


        (cc_tx, handle)
    }
}




