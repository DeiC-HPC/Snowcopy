use std::path::Path;

pub fn path_to_s3_key(path: &str) -> String {
    use std::path::Component;

    let mut components = Vec::new();

    // Split into components and normalize
    for component in Path::new(path).components() {
        match component {
            Component::Prefix(_prefix) => {
                // Ignore drive letters and prefixes
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

    components.join("/")
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
            ("./../path/to/file", "path/to/file"),
            ("///path/to/file", "path/to/file"),
            ("./././path", "path"),
            ("./path/t.o/.fi.le", "path/t.o/.fi.le"),
            ("", ""),

            ("C:\\path\\to\\file", "path/to/file"),
            ("C:/path/to/file", "path/to/file"),
            ("D:\\Users\\username\\file", "Users/username/file"),
            ("E:/Program Files/app", "Program Files/app"),
            ("C:\\path\\..\\to\\file", "to/file"),
            ("C:\\.\\path\\to\\file", "path/to/file"),
            ("path\\to\\file", "path/to/file"),
            (".\\path\\to\\file", "path/to/file"),
            ("..\\path\\to\\file", "path/to/file"),
            (".\\path\\..\\to\\file", "to/file"),
            (".\\.\\.\\path", "path"),
            ("..\\..\\path\\to\\file", "path/to/file"),
            (".\\..\\path\\to\\file", "path/to/file"),
        ];

        for (input, expected) in test_cases {
            assert_eq!(
                path_to_s3_key(input),
                expected,
                "Failed for input: {}",
                input
            );
        }
    }
}

