use std::fmt;
use std::error::Error;
use std::io;
use hyper;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use crate::retry::RetryableError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Error {
    AccessControlListNotSupported,
    AccessDenied,
    AccessPointAlreadyOwnedByYou,
    AccountProblem,
    AllAccessDisabled,
    AmbiguousGrantByEmailAddress,
    AuthorizationHeaderMalformed,
    BadDigest,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    CredentialsNotSupported,
    CrossLocationLoggingProhibited,
    EntityTooLarge,
    EntityTooSmall,
    ExpiredToken,
    IllegalLocationConstraintException,
    IllegalVersioningConfigurationException,
    IncompleteBody,
    IncorrectNumberOfFilesInPostRequest,
    InlineDataTooLarge,
    InternalError,
    InvalidAccessKeyId,
    InvalidAddressingHeader,
    InvalidArgument,
    InvalidBucketName,
    InvalidBucketState,
    InvalidDigest,
    InvalidEncryptionAlgorithmError,
    InvalidLocationConstraint,
    InvalidObjectState,
    InvalidPart,
    InvalidPartOrder,
    InvalidPayer,
    InvalidRange,
    InvalidRequest,
    InvalidSecurity,
    InvalidSOAPRequest,
    InvalidStorageClass,
    InvalidTargetBucketForLogging,
    InvalidToken,
    InvalidURI,
    KeyTooLongError,
    MalformedACLError,
    MalformedPOSTRequest,
    MalformedXML,
    MaxMessageSizeExceeded,
    MaxPostSizeExceeded,
    MetadataTooLarge,
    MethodNotAllowed,
    MissingContentLength,
    MissingRequestBodyError,
    MissingSecurityElement,
    MissingSecurityHeader,
    NoLoggingStatusForKey,
    NoSuchBucket,
    NoSuchBucketPolicy,
    NoSuchKey,
    NoSuchLifecycleConfiguration,
    NoSuchUpload,
    NoSuchVersion,
    NotImplemented,
    NotSignedUp,
    OperationAborted,
    PermanentRedirect,
    PreconditionFailed,
    Redirect,
    RestoreAlreadyInProgress,
    RequestIsNotMultiPartContent,
    RequestTimeout,
    RequestTimeTooSkewed,
    RequestTorrentOfBucketError,
    SignatureDoesNotMatch,
    ServiceUnavailable,
    SlowDown,
    TemporaryRedirect,
    TokenRefreshRequired,
    TooManyBuckets,
    UnexpectedContent,
    UnparsableHeaders,
    UserKeyMustBeSpecified,
}

#[derive(Debug)]
pub enum SnowCopyError {
    /// Service-level S3 error.
    /// The response was successfully received and parsed, and contains an S3-specific error code.
    S3(S3Error),
    /// AWS SDK Error
    Aws(String),
    /// Protocol-level response.
    /// The request failed with an HTTP status code, but no S3-specific error code was found in the body.
    Http(u16),
    /// Transport failure (e.g., connection issues).
    Transport(String),
    /// IO failure.
    Io(io::Error),
    /// Parsing failure (e.g., body wasn't valid UTF-8, XML parse failed).
    Parse(String),
    /// Hyper error.
    Hyper(hyper::Error),
    /// The server returned a successful response, but it was missing data required by the client.
    MissingRequiredHeader(&'static str),
    /// Generic retryable error
    RetryableError(String),
    /// Integrity Error
    IntegrityError(String),
    /// The operation was canceled.
    Canceled,
    /// If we cannot convert the error into a SnowCopyError.
    Unknown,
}

impl fmt::Display for SnowCopyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnowCopyError::S3(e) => write!(f, "S3 service error: {}", e),
            SnowCopyError::Aws(m) => write!(f, "AWS SDK error: {}", m),
            SnowCopyError::Http(s) => write!(f, "HTTP protocol error (status {}): no S3 error code", s),
            SnowCopyError::Transport(m) => write!(f, "Transport failure: {}", m),
            SnowCopyError::Io(e) => write!(f, "IO failure: {}", e),
            SnowCopyError::Parse(m) => write!(f, "Parsing failure: {}", m),
            SnowCopyError::Hyper(e) => write!(f, "Hyper error: {}", e),
            SnowCopyError::MissingRequiredHeader(header) => write!(f, "Invalid response: missing required `{}` header", header),
            SnowCopyError::RetryableError(s) => write!(f, "Retryable error: {}", s),
            SnowCopyError::IntegrityError(s) => write!(f, "Integrity error: {}", s),
            SnowCopyError::Canceled => write!(f, "Operation canceled"),
            SnowCopyError::Unknown => write!(f, "Unknown error"),
        }
    }
}

impl Error for SnowCopyError {}

impl RetryableError for SnowCopyError {
    fn is_retryable(&self) -> bool {
        match self {
            SnowCopyError::S3(e) => e.is_retryable(),
            SnowCopyError::Aws(_) => true, // Conservatively retry AWS SDK errors
            SnowCopyError::Http(s) => {
                // We want to retry on most HTTP status codes.
                // If there is a bit corrupted in a status 200 code response, we still want to retry.
                // If everything goes well, then we should receive a nice S3 response we can act on.
                // But http errors are general failure in this context, so we retry them all.
                // Thus, we retry on everything else than 4xx where we are sure that resending
                // does not change anything.

                // 400 (Bad Request): The client sent an invalid request. Fix the issue in the request before trying again.
                // 401 (Unauthorized): Authentication is required or failed. Fix the authentication issue before trying again (e.g. get a fresh token).
                // 403 (Forbidden): The client lacks permission to access the resource. Retrying will not change the server’s response. Fix the authorization issue such as getting a new a token with additional scopes before trying again.
                // 404 (Not Found): The requested resource does not exist. Retrying will not succeed unless the resource becomes available later due to some background processing.
                // 405 (Method Not Allowed): The HTTP method used is not supported. Retrying with the same method will not resolve the issue.
                // 409 (Conflict): Indicates a conflict in the request such as unique constraints for referential integrity. Retrying without addressing the conflict will continue to fail.
                // 422 (Unprocessable Entity): The server understands the request but cannot process it due to semantic errors. Fix the issue in the request before trying again.

                !matches!(*s, 400 | 401 | 403 | 404 | 405 | 409 | 422 )
            }
            SnowCopyError::Transport(_) => {
                // Transport errors like "connection closed before full body" are often retryable
                true
            }
            SnowCopyError::Io(e) => {
                // Retryable IO errors:
                match e.kind() {
                    io::ErrorKind::ConnectionReset |
                    io::ErrorKind::ConnectionAborted |
                    io::ErrorKind::NotConnected |
                    io::ErrorKind::BrokenPipe |
                    io::ErrorKind::TimedOut |
                    io::ErrorKind::Interrupted |
                    io::ErrorKind::WouldBlock |
                    io::ErrorKind::UnexpectedEof |
                    io::ErrorKind::WriteZero |
                    io::ErrorKind::ResourceBusy => true,
                    _ => false,
                }
            }
            SnowCopyError::Parse(_) => {
                false
            }
            SnowCopyError::Hyper(_) => {
                // Hyper errors can be retryable depending on the cause, but for simplicity
                // many are treated as retryable in this context (like connection resets)
                //TODO JON: is there any case where this is meaningless to retry?
                true
            }
            SnowCopyError::MissingRequiredHeader(_) => {
                // For multipart UploadPart, a missing ETag means we cannot complete
                // the multipart upload. Retrying the part is usually safe.
                true
            }

            SnowCopyError::RetryableError(_) => {
                true
            }

            SnowCopyError::IntegrityError(_) => {
                true
            }

            SnowCopyError::Canceled => false,

            SnowCopyError::Unknown => false,
        }
    }
}

impl From<io::Error> for SnowCopyError {
    fn from(err: io::Error) -> Self {
        SnowCopyError::Io(err)
    }
}

impl From<hyper::Error> for SnowCopyError {
    fn from(err: hyper::Error) -> Self {
        SnowCopyError::Hyper(err)
    }
}

impl From<tokio::sync::AcquireError> for SnowCopyError {
    fn from(err: tokio::sync::AcquireError) -> Self {
        SnowCopyError::Transport(err.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for SnowCopyError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        SnowCopyError::Transport(err.to_string())
    }
}

impl From<openssl::error::ErrorStack> for SnowCopyError {
    fn from(err: openssl::error::ErrorStack) -> Self {
        SnowCopyError::Parse(err.to_string())
    }
}

impl From<hyper_util::client::legacy::Error> for SnowCopyError {
    fn from(err: hyper_util::client::legacy::Error) -> Self {
        SnowCopyError::Transport(err.to_string())
    }
}

impl<E> From<SdkError<E>> for SnowCopyError
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    fn from(err: SdkError<E>) -> Self {
        let status = err
            .raw_response()
            .map(|response| response.status().as_u16());

        match err {
            SdkError::ServiceError(service_err) => {
                let source = service_err.err();
                let code = source.code().unwrap_or("unknown code");
                let message = source.message().unwrap_or("missing reason");

                if let Some(s3_err) = S3Error::from_code(code) {
                    return SnowCopyError::S3(s3_err);
                }

                match status {
                    Some(403) => SnowCopyError::S3(S3Error::AccessDenied),
                    Some(404) => SnowCopyError::Http(404),
                    Some(status) => SnowCopyError::Http(status),
                    None => SnowCopyError::Aws(format!(
                        "S3 service error: code {}, message: {}, details: {:?}",
                        code,
                        message,
                        source
                    )),
                }
            }

            SdkError::ResponseError(response_err) => {
                match status {
                    Some(status) => SnowCopyError::Http(status),
                    None => SnowCopyError::Aws(format!(
                        "AWS SDK response error: {:?}",
                        response_err
                    )),
                }
            }

            SdkError::DispatchFailure(dispatch_err) => {
                SnowCopyError::Transport(format!(
                    "AWS SDK dispatch failure: {:?}. This often means the endpoint URL is wrong, DNS failed, TLS failed, or the server closed the connection.",
                    dispatch_err
                ))
            }

            SdkError::TimeoutError(timeout_err) => {
                SnowCopyError::Transport(format!(
                    "AWS SDK timeout: {:?}",
                    timeout_err
                ))
            }

            SdkError::ConstructionFailure(construction_err) => {
                SnowCopyError::Aws(format!(
                    "AWS SDK request construction failure: {:?}",
                    construction_err
                ))
            }

            other => {
                SnowCopyError::Aws(format!(
                    "AWS SDK error: {}",
                    other
                ))
            }
        }
    }
}
impl From<aws_sdk_s3::Error> for SnowCopyError {
    fn from(err: aws_sdk_s3::Error) -> Self {
        let code = err.code().unwrap_or("unknown code");
        if let Some(s3_err) = S3Error::from_code(code) {
            SnowCopyError::S3(s3_err)
        } else {
            SnowCopyError::Aws(format!(
                "{}: {}",
                code,
                err.message().unwrap_or("missing reason")
            ))
        }
    }
}

impl S3Error {
    pub fn from_code(code: &str) -> Option<Self> {
        match code {
            "AccessControlListNotSupported" => Some(S3Error::AccessControlListNotSupported),
            "AccessDenied" => Some(S3Error::AccessDenied),
            "AccessPointAlreadyOwnedByYou" => Some(S3Error::AccessPointAlreadyOwnedByYou),
            "AccountProblem" => Some(S3Error::AccountProblem),
            "AllAccessDisabled" => Some(S3Error::AllAccessDisabled),
            "AmbiguousGrantByEmailAddress" => Some(S3Error::AmbiguousGrantByEmailAddress),
            "AuthorizationHeaderMalformed" => Some(S3Error::AuthorizationHeaderMalformed),
            "BadDigest" => Some(S3Error::BadDigest),
            "BucketAlreadyExists" => Some(S3Error::BucketAlreadyExists),
            "BucketAlreadyOwnedByYou" => Some(S3Error::BucketAlreadyOwnedByYou),
            "BucketNotEmpty" => Some(S3Error::BucketNotEmpty),
            "CredentialsNotSupported" => Some(S3Error::CredentialsNotSupported),
            "CrossLocationLoggingProhibited" => Some(S3Error::CrossLocationLoggingProhibited),
            "EntityTooLarge" => Some(S3Error::EntityTooLarge),
            "EntityTooSmall" => Some(S3Error::EntityTooSmall),
            "ExpiredToken" => Some(S3Error::ExpiredToken),
            "IllegalLocationConstraintException" => Some(S3Error::IllegalLocationConstraintException),
            "IllegalVersioningConfigurationException" => Some(S3Error::IllegalVersioningConfigurationException),
            "IncompleteBody" => Some(S3Error::IncompleteBody),
            "IncorrectNumberOfFilesInPostRequest" => Some(S3Error::IncorrectNumberOfFilesInPostRequest),
            "InlineDataTooLarge" => Some(S3Error::InlineDataTooLarge),
            "InternalError" => Some(S3Error::InternalError),
            "InvalidAccessKeyId" => Some(S3Error::InvalidAccessKeyId),
            "InvalidAddressingHeader" => Some(S3Error::InvalidAddressingHeader),
            "InvalidArgument" => Some(S3Error::InvalidArgument),
            "InvalidBucketName" => Some(S3Error::InvalidBucketName),
            "InvalidBucketState" => Some(S3Error::InvalidBucketState),
            "InvalidDigest" => Some(S3Error::InvalidDigest),
            "InvalidEncryptionAlgorithmError" => Some(S3Error::InvalidEncryptionAlgorithmError),
            "InvalidLocationConstraint" => Some(S3Error::InvalidLocationConstraint),
            "InvalidObjectState" => Some(S3Error::InvalidObjectState),
            "InvalidPart" => Some(S3Error::InvalidPart),
            "InvalidPartOrder" => Some(S3Error::InvalidPartOrder),
            "InvalidPayer" => Some(S3Error::InvalidPayer),
            "InvalidRange" => Some(S3Error::InvalidRange),
            "InvalidRequest" => Some(S3Error::InvalidRequest),
            "InvalidSecurity" => Some(S3Error::InvalidSecurity),
            "InvalidSOAPRequest" => Some(S3Error::InvalidSOAPRequest),
            "InvalidStorageClass" => Some(S3Error::InvalidStorageClass),
            "InvalidTargetBucketForLogging" => Some(S3Error::InvalidTargetBucketForLogging),
            "InvalidToken" => Some(S3Error::InvalidToken),
            "InvalidURI" => Some(S3Error::InvalidURI),
            "KeyTooLongError" => Some(S3Error::KeyTooLongError),
            "MalformedACLError" => Some(S3Error::MalformedACLError),
            "MalformedPOSTRequest" => Some(S3Error::MalformedPOSTRequest),
            "MalformedXML" => Some(S3Error::MalformedXML),
            "MaxMessageSizeExceeded" => Some(S3Error::MaxMessageSizeExceeded),
            "MaxPostSizeExceeded" => Some(S3Error::MaxPostSizeExceeded),
            "MetadataTooLarge" => Some(S3Error::MetadataTooLarge),
            "MethodNotAllowed" => Some(S3Error::MethodNotAllowed),
            "MissingContentLength" => Some(S3Error::MissingContentLength),
            "MissingRequestBodyError" => Some(S3Error::MissingRequestBodyError),
            "MissingSecurityElement" => Some(S3Error::MissingSecurityElement),
            "MissingSecurityHeader" => Some(S3Error::MissingSecurityHeader),
            "NoLoggingStatusForKey" => Some(S3Error::NoLoggingStatusForKey),
            "NoSuchBucket" => Some(S3Error::NoSuchBucket),
            "NoSuchBucketPolicy" => Some(S3Error::NoSuchBucketPolicy),
            "NoSuchKey" => Some(S3Error::NoSuchKey),
            "NoSuchLifecycleConfiguration" => Some(S3Error::NoSuchLifecycleConfiguration),
            "NoSuchUpload" => Some(S3Error::NoSuchUpload),
            "NoSuchVersion" => Some(S3Error::NoSuchVersion),
            "NotImplemented" => Some(S3Error::NotImplemented),
            "NotSignedUp" => Some(S3Error::NotSignedUp),
            "OperationAborted" => Some(S3Error::OperationAborted),
            "PermanentRedirect" => Some(S3Error::PermanentRedirect),
            "PreconditionFailed" => Some(S3Error::PreconditionFailed),
            "Redirect" => Some(S3Error::Redirect),
            "RestoreAlreadyInProgress" => Some(S3Error::RestoreAlreadyInProgress),
            "RequestIsNotMultiPartContent" => Some(S3Error::RequestIsNotMultiPartContent),
            "RequestTimeout" => Some(S3Error::RequestTimeout),
            "RequestTimeTooSkewed" => Some(S3Error::RequestTimeTooSkewed),
            "RequestTorrentOfBucketError" => Some(S3Error::RequestTorrentOfBucketError),
            "SignatureDoesNotMatch" => Some(S3Error::SignatureDoesNotMatch),
            "ServiceUnavailable" => Some(S3Error::ServiceUnavailable),
            "SlowDown" => Some(S3Error::SlowDown),
            "TemporaryRedirect" => Some(S3Error::TemporaryRedirect),
            "TokenRefreshRequired" => Some(S3Error::TokenRefreshRequired),
            "TooManyBuckets" => Some(S3Error::TooManyBuckets),
            "UnexpectedContent" => Some(S3Error::UnexpectedContent),
            "UnparsableHeaders" => Some(S3Error::UnparsableHeaders),
            "UserKeyMustBeSpecified" => Some(S3Error::UserKeyMustBeSpecified),
            _ => None,
        }
    }

    pub fn from_xml(xml: &str) -> Option<Self> {
        // Basic extraction of <Code>CodeValue</Code>
        let start_tag = "<Code>";
        let end_tag = "</Code>";

        if let Some(start_pos) = xml.find(start_tag) {
            let start_index = start_pos + start_tag.len();
            if let Some(end_pos) = xml[start_index..].find(end_tag) {
                let code = &xml[start_index..start_index + end_pos];
                return Self::from_code(code.trim());
            }
        }
        None
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            S3Error::InternalError |
            S3Error::ServiceUnavailable |
            S3Error::SlowDown |
            S3Error::RequestTimeout |
            S3Error::OperationAborted |
            S3Error::RequestTimeTooSkewed => true,
            _ => false,
        }
    }
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            S3Error::AccessControlListNotSupported => "The bucket does not allow ACLs.",
            S3Error::AccessDenied => "Access Denied. Likely endpoint misconfiguration.",
            S3Error::AccessPointAlreadyOwnedByYou => "An access point with an identical name already exists in your account.",
            S3Error::AccountProblem => "There is a problem with your AWS account that prevents the action from completing successfully.",
            S3Error::AllAccessDisabled => "All access to this Amazon S3 resource has been disabled.",
            S3Error::AmbiguousGrantByEmailAddress => "The email address you provided is associated with more than one account.",
            S3Error::AuthorizationHeaderMalformed => "The authorization header you provided is invalid.",
            S3Error::BadDigest => "The Content-MD5 you specified did not match what we received.",
            S3Error::BucketAlreadyExists => "The requested bucket name is not available.",
            S3Error::BucketAlreadyOwnedByYou => "The bucket you tried to create already exists, and you own it.",
            S3Error::BucketNotEmpty => "The bucket you tried to delete is not empty.",
            S3Error::CredentialsNotSupported => "This request does not support credentials.",
            S3Error::CrossLocationLoggingProhibited => "Cross-location logging not allowed.",
            S3Error::EntityTooLarge => "Your proposed upload exceeds the maximum allowed object size.",
            S3Error::EntityTooSmall => "Your proposed upload is smaller than the minimum allowed object size.",
            S3Error::ExpiredToken => "The provided token has expired.",
            S3Error::IllegalLocationConstraintException => "You are trying to create a bucket in a Region that you have not been enabled for.",
            S3Error::IllegalVersioningConfigurationException => "Indicates that the versioning configuration specified in the request is invalid.",
            S3Error::IncompleteBody => "You did not provide the number of bytes specified by the Content-Length HTTP header.",
            S3Error::IncorrectNumberOfFilesInPostRequest => "POST requires exactly one file upload per request.",
            S3Error::InlineDataTooLarge => "Inline data exceeds the maximum allowed size.",
            S3Error::InternalError => "We encountered an internal error. Please try again.",
            S3Error::InvalidAccessKeyId => "The AWS access key ID you provided does not exist in our records. Is your S3 Access Key correctly configured?",
            S3Error::InvalidAddressingHeader => "You must address the anonymous role.",
            S3Error::InvalidArgument => "Invalid Argument",
            S3Error::InvalidBucketName => "The specified bucket is not valid.",
            S3Error::InvalidBucketState => "The request is not valid with the current state of the bucket.",
            S3Error::InvalidDigest => "The Content-MD5 you specified is not valid.",
            S3Error::InvalidEncryptionAlgorithmError => "The encryption request you specified is not valid. The valid value is AES256.",
            S3Error::InvalidLocationConstraint => "The specified location constraint is not valid.",
            S3Error::InvalidObjectState => "The storage class of the object you are trying to access is not valid for this operation.",
            S3Error::InvalidPart => "One or more of the specified parts could not be found.",
            S3Error::InvalidPartOrder => "The list of parts was not in ascending order.",
            S3Error::InvalidPayer => "All access to this object has been disabled.",
            S3Error::InvalidRange => "The requested range cannot be satisfied.",
            S3Error::InvalidRequest => "Invalid Request",
            S3Error::InvalidSecurity => "The provided security credentials are not valid.",
            S3Error::InvalidSOAPRequest => "The SOAP request is invalid.",
            S3Error::InvalidStorageClass => "The storage class you specified is not valid.",
            S3Error::InvalidTargetBucketForLogging => "The target bucket for logging does not exist, is not owned by you, or does not have the appropriate permissions set.",
            S3Error::InvalidToken => "The provided token is malformed or otherwise invalid.",
            S3Error::InvalidURI => "Could not parse the specified URI.",
            S3Error::KeyTooLongError => "Your key is too long.",
            S3Error::MalformedACLError => "The XML you provided was not well-formed or did not validate against our published schema.",
            S3Error::MalformedPOSTRequest => "The body of your POST request is not well-formed multipart/form-data.",
            S3Error::MalformedXML => "The XML you provided was not well-formed or did not validate against our published schema.",
            S3Error::MaxMessageSizeExceeded => "Your request was too big.",
            S3Error::MaxPostSizeExceeded => "Your POST request was too big.",
            S3Error::MetadataTooLarge => "Your metadata headers exceed the maximum allowed size.",
            S3Error::MethodNotAllowed => "The specified method is not allowed against this resource.",
            S3Error::MissingContentLength => "You must provide the Content-Length HTTP header.",
            S3Error::MissingRequestBodyError => "Request body is empty.",
            S3Error::MissingSecurityElement => "The request is missing a required XML element.",
            S3Error::MissingSecurityHeader => "Your request is missing a required header.",
            S3Error::NoLoggingStatusForKey => "There is no logging status for the specified key.",
            S3Error::NoSuchBucket => "The specified bucket does not exist.",
            S3Error::NoSuchBucketPolicy => "The specified bucket does not have a bucket policy.",
            S3Error::NoSuchKey => "The specified key does not exist.",
            S3Error::NoSuchLifecycleConfiguration => "The specified bucket does not have a lifecycle configuration.",
            S3Error::NoSuchUpload => "The specified multipart upload does not exist.",
            S3Error::NoSuchVersion => "The specified version does not exist.",
            S3Error::NotImplemented => "A header you provided implies functionality that is not implemented.",
            S3Error::NotSignedUp => "Your account is not signed up for the S3 service.",
            S3Error::OperationAborted => "A conflicting conditional operation is currently in progress against this resource.",
            S3Error::PermanentRedirect => "The bucket you are attempting to access must be addressed using the specified endpoint.",
            S3Error::PreconditionFailed => "At least one of the preconditions you specified did not hold.",
            S3Error::Redirect => "Temporary redirect.",
            S3Error::RestoreAlreadyInProgress => "Object restore is already in progress.",
            S3Error::RequestIsNotMultiPartContent => "Bucket POST must be of the enclosure-type multipart/form-data.",
            S3Error::RequestTimeout => "Your socket connection to the server was not read from or written to within the timeout period.",
            S3Error::RequestTimeTooSkewed => "The difference between the requested time and the server's time is too large.",
            S3Error::RequestTorrentOfBucketError => "Requesting the torrent file of a bucket is not permitted.",
            S3Error::SignatureDoesNotMatch => "The request signature the s3 server calculated does not match the signature snowcopy provided. Is your S3 Secret Key correctly configured?",
            S3Error::ServiceUnavailable => "Reduce your request rate.",
            S3Error::SlowDown => "Reduce your request rate.",
            S3Error::TemporaryRedirect => "You are being redirected to the bucket while optimization is taking place.",
            S3Error::TokenRefreshRequired => "The provided token must be refreshed.",
            S3Error::TooManyBuckets => "You have attempted to create more buckets than allowed.",
            S3Error::UnexpectedContent => "This request does not support content.",
            S3Error::UnparsableHeaders => "Your headers were not well-formed.",
            S3Error::UserKeyMustBeSpecified => "The bucket POST must contain the specified field name.",
        };
        write!(f, "{}", msg)
    }
}

impl Error for S3Error {}
