#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum FatalErrorCode {
    DiskIoError = 1,
    ServerStateChangeDuringExecution = 2,
    RequestFailure = 3,
    Unknown = 4,
}

impl FatalErrorCode {
    pub fn as_i32(&self) -> i32 {
        *self as i32
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FatalError {
    code: FatalErrorCode,
    message: String,
}

impl FatalError {
    pub fn new(code: FatalErrorCode, message: String) -> Self {
        Self { code, message }
    }

    pub fn code(&self) -> FatalErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}