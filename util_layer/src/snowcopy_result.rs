use crate::error_codes::UserFacingError;

pub struct SnowcopyResult {
    pub fatal_error : Option<UserFacingError>,
    pub error : Option<UserFacingError>,
}

impl SnowcopyResult {
    pub fn with_fatal(error: UserFacingError) -> SnowcopyResult {
        SnowcopyResult{fatal_error: Some(error), error: None}
    }

    pub fn with_non_fatal(error: UserFacingError) -> SnowcopyResult {
        SnowcopyResult{fatal_error: None, error: Some(error)}
    }
}