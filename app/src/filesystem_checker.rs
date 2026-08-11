/// Characters that are generally invalid in Windows filenames.
/// < > : " / \ | ? *
#[cfg(windows)]
const WINDOWS_INVALID_CHARS: &[char] = &['<', '>', ':', '"', '/', '\\', '|', '?', '*'];

/// Reserved names in Windows that cannot be used as filenames (case-insensitive).
/// https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file?redirectedfrom=MSDN
#[cfg(windows)]
const WINDOWS_RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Checks if an S3 key (or a component of it) is valid for the local filesystem.
/// This checks against the rules of the current operating system.
pub fn is_valid_local_path_component(component: &str) -> bool {
    if component.is_empty() {
        return false;
    }

    #[cfg(windows)]
    {
        //c < 32 checks for ascii control characters like \r and \n
        if component.chars().any(|c| WINDOWS_INVALID_CHARS.contains(&c) || (c as u32) < 32) {
            return false;
        }

        // Check for reserved names in Windows
        let upper_component = component.to_uppercase();
        // Reserved names are invalid even with an extension (e.g., NUL.txt)
        let base_name = if let Some(dot_pos) = upper_component.find('.') {
            &upper_component[..dot_pos]
        } else {
            &upper_component
        };

        if WINDOWS_RESERVED_NAMES.contains(&base_name) {
            return false;
        }

        // Windows does not allow filenames to end with a space or a period
        if component.ends_with(' ') || component.ends_with('.') {
            return false;
        }
    }

    #[cfg(not(windows))]
    {
        // On Unix-like systems (Linux, macOS), the only invalid characters are '/' and NUL.
        // Since we split by '/', we only need to check for NUL characters.
        if component.chars().any(|c| c == '\0') {
            return false;
        }
    }

    true
}

/// Checks if an entire S3 key is safe to be converted to a local path.
/// An S3 key is treated as a relative path where '/' are directory separators.
pub fn is_valid_s3_key_for_local_filesystem(key: &str) -> bool {
    if key.is_empty() {
        return false;
    }

    // S3 keys can start with '/', but we usually treat them as relative
    let key = key.strip_prefix('/').unwrap_or(key);

    for component in key.split('/') {
        if !component.is_empty() && !is_valid_local_path_component(component) {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_component() {
        assert!(is_valid_local_path_component("hello.txt"));
        assert!(is_valid_local_path_component("my-file_123"));

        #[cfg(windows)]
        {
            assert!(!is_valid_local_path_component("file*.txt"));
            assert!(!is_valid_local_path_component("con.txt"));
            assert!(!is_valid_local_path_component("trailing "));
            assert!(!is_valid_local_path_component("trailing."));
        }

        #[cfg(not(windows))]
        {
            // These are valid on Linux/macOS but invalid on Windows
            assert!(is_valid_local_path_component("file*.txt"));
            assert!(is_valid_local_path_component("con.txt"));
            assert!(is_valid_local_path_component("trailing "));
            assert!(is_valid_local_path_component("trailing."));
            // NUL is still invalid
            assert!(!is_valid_local_path_component("file\0.txt"));
        }
    }

    #[test]
    fn test_valid_s3_key() {
        assert!(is_valid_s3_key_for_local_filesystem("path/to/file.txt"));
        assert!(is_valid_s3_key_for_local_filesystem("/path/to/file.txt"));

        #[cfg(windows)]
        {
            assert!(!is_valid_s3_key_for_local_filesystem("path/to/con/file.txt"));
            assert!(!is_valid_s3_key_for_local_filesystem("path/to/file*.txt"));
        }

        #[cfg(not(windows))]
        {
            assert!(is_valid_s3_key_for_local_filesystem("path/to/con/file.txt"));
            assert!(is_valid_s3_key_for_local_filesystem("path/to/file*.txt"));
        }
    }
}
