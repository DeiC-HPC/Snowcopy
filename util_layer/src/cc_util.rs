use std::fmt;
use std::time::{SystemTime, Duration};
use tokio::sync::oneshot;
use crate::error_codes::{UserFacingError};

pub enum CommandCenterEvent {
    PartTransferred(u64),
    FileTransferred(),
    FileDiscovered(u64),
    FileAlreadyAtTarget(u64),
    StatusChanged(Status),
    Log(String),
    RetryRequested(oneshot::Sender<Duration>, u32, SystemTime, String),
    FileIntegrityChecked(bool),
    FatalErrorEvent(UserFacingError),
    ErrorEvent(UserFacingError),
}

pub enum Status {
    Transferring,
    Discovering,
    Finished,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Transferring => write!(f, "Transferred"),
            Status::Discovering => write!(f, "Discovering files"),
            Status::Finished => write!(f, "Finished"),
        }
    }
}