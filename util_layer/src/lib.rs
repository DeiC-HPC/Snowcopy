use std::path::{Path, PathBuf};

pub fn normalize_path(path: &str) -> String {
    // Step 1: Normalize using Path components
    let cleaned: PathBuf = Path::new(path).components().collect();

    // Step 2: Convert to string and force `/` separators
    let mut s = cleaned.to_string_lossy().replace('\\', "/");

    // Step 3: Remove all leading "./" and "../"
    loop {
        if s.starts_with("/") {
            s = s[1..].to_string();
        } else if s.starts_with("./") {
            s = s[2..].to_string();
        } else if s.starts_with("../") {
            s = s[3..].to_string();
        } else {
            break;
        }
    }

    s
}