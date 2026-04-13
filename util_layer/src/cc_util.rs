use std::fmt;

pub struct CcLoadAndTransfer { //TODO Jon: The Java way would be to encapsulate this data. Should that be done here with impl and traits?
    pub amount : u64,
    pub file_sem_work_time_secs: f64,
    pub file_sem_acquire_time_secs: f64,
    pub req_sem_work_time_secs: f64,
    pub req_sem_acquire_time_secs: f64,
}

pub enum CommandCenterEvent {
    PartTransferred(CcLoadAndTransfer),
    FileTransferred(u64),
    FileDiscovered(u64),
    FileAlreadyAtTarget(u64),
    StatusChanged(Status),
    Log(String),
    NonDestructiveMode(String),
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