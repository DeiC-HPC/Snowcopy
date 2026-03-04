use std::path::Path;

pub fn normalize_file_path(path: &str) -> String {
    use std::path::Component;

    let mut components = Vec::new();
    let mut drive_letter = None;

    // Split into components and normalize
    for component in Path::new(path).components() {
        match component {
            Component::Prefix(prefix) => {
                // Keep the drive letter (e.g., "C:")
                let prefix_str = prefix.as_os_str().to_string_lossy();
                if prefix_str.len() >= 2 && prefix_str.chars().nth(1) == Some(':') {
                    drive_letter = Some(prefix_str[0..2].to_uppercase());
                }
            }
            Component::RootDir => {
                // Ignore leading root directory (equivalent to removing leading "/")
            }
            Component::CurDir => {
                // Ignore "."
            }
            Component::ParentDir => {
                // Pop the last component for ".."
                components.pop();
            }
            Component::Normal(c) => {
                components.push(c.to_string_lossy().to_string());
            }
        }
    }

    let mut result = String::new();
    if let Some(ref drive) = drive_letter {
        result.push_str(drive);
        result.push('/');
    }

    result.push_str(&components.join("/"));

    // If it started with drive letter, we might have added a trailing slash if components were empty.
    // But join("/") on empty components would result in "".
    // If we have a drive letter and no components, result is "C:/".
    // Let's check the test cases: ("C:\\path\\..\\to\\file", "C:/to/file")
    // If components is ["to", "file"], result is "C:/to/file". Correct.
    // If path was "C:\\", result is "C:/".
    
    // Wait, if result is just "C:/", should it be "C:/" or "C:"?
    // Test case: ("C:\\path\\to\\file", "C:/path/to/file")
    
    // Let's trim trailing slash if it's not the root
    if result.ends_with('/') && result.len() > 3 {
        result.pop();
    }
    
    // One more thing: if the input was empty, it should be empty.
    // If path was "/", result should be empty based on test case ("/path/to/file", "path/to/file")
    
    // Let's handle the empty drive letter case carefully.
    if drive_letter.is_some() && components.is_empty() {
        return format!("{}:/", drive_letter.as_ref().unwrap().chars().next().unwrap());
    }

    // Removing leading slash if it exists (it shouldn't with this approach)
    if result.starts_with('/') {
        result.remove(0);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        let test_cases = vec![
            ("./path/to/file", "path/to/file"),
            ("../path/to/file", "path/to/file"),
            ("/path/to/file", "path/to/file"),
            ("path/to/file", "path/to/file"),
            ("./path/../to/file", "to/file"),
            ("path\\to\\file", "path/to/file"),
            ("./../path/to/file", "path/to/file"),
            ("///path/to/file", "path/to/file"),
            ("./././path", "path"),
            ("", ""),
            // Windows absolute paths with drive letters
            ("C:\\path\\to\\file", "C:/path/to/file"),
            ("C:/path/to/file", "C:/path/to/file"),
            ("D:\\Users\\username\\file", "D:/Users/username/file"),
            ("E:/Program Files/app", "E:/Program Files/app"),
            ("C:\\path\\..\\to\\file", "C:/to/file"),
            ("C:\\.\\path\\to\\file", "C:/path/to/file"),
            // Windows relative paths with backslashes
            (".\\path\\to\\file", "path/to/file"),
            ("..\\path\\to\\file", "path/to/file"),
            (".\\path\\..\\to\\file", "to/file"),
            (".\\.\\.\\path", "path"),
            ("..\\..\\path\\to\\file", "path/to/file"),
            (".\\..\\path\\to\\file", "path/to/file"),
        ];

        for (input, expected) in test_cases {
            assert_eq!(
                normalize_file_path(input),
                expected,
                "Failed for input: {}",
                input
            );
        }
    }
}

