use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use crate::ui::UiEvent;

pub enum CommandCenterEvent {
    FileTransferred(u64,u64,u64),
}

pub struct CommandCenter {
    pub ui_rx: UnboundedReceiver<UiEvent>,
}

pub fn start_command_center_task(mut ui_tx: UnboundedSender<UiEvent>) -> JoinHandle<()> {
    let command_handle = tokio::task::spawn_blocking(move || {
        
    });

    command_handle
}

