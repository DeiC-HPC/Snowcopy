#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorCode {
    Unknown = 1,
    ServerStateChangeDuringExecution = 2,
    RequestFailure = 3,
    DiskIoError = 4,
    HttpUnsafeError = 5,
    SkippedInvalidKeysDownload = 6,
    AlphaFileSizeLimitExceeded = 7,
}

impl ErrorCode {
    pub fn as_i32(&self) -> i32 {
        *self as i32
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserFacingError {
    code: ErrorCode,
    message: String,
}

impl UserFacingError {
    pub fn new(code: ErrorCode, message: String) -> Self {
        Self { code, message }
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}