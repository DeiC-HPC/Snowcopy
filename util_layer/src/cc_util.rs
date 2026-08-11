use std::fmt;
use std::time::{SystemTime, Duration};
use tokio::sync::oneshot;
use crate::error_codes::{UserFacingError};

pub enum CommandCenterEvent {
    PartTransferred(u64),
    FileTransferred(u64),
    FileDiscovered(u64),
    FileAlreadyAtTarget(u64),
    StatusChanged(Status),
    Log(String),
    RetryRequested(oneshot::Sender<Duration>, u32, SystemTime, String),
    FileIntegrityChecked(bool), //JON: Should have key for some logging?
    FatalErrorEvent(UserFacingError), //TODO JON: also add metadata for some logging?
    ErrorEvent(UserFacingError),
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
            Status::Transferring => write!(f, "Transferred"),
            Status::Discovering => write!(f, "Discovering files"),
            Status::Finished => write!(f, "Finished"),
        }
    }
}