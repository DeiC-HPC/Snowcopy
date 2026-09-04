use crate::common::test_constants::SNOWCOPY_TEST_DATA_BASE_DIR_PREFIX;
use crate::common::test_file_util;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

pub struct TestFileHandler {
    config_path: PathBuf,
    base_path: PathBuf,
    upload_folder: PathBuf,
    download_folder: PathBuf,
    data_folder: PathBuf,
}
#[allow(dead_code)]
impl TestFileHandler {
    pub fn new() -> Self {
        let base_path =std::env::temp_dir().join(SNOWCOPY_TEST_DATA_BASE_DIR_PREFIX).join(Uuid::new_v4().to_string());

        let instance = Self {
            upload_folder: base_path.join("upload"),
            download_folder: base_path.join("download"),
            config_path: base_path.join("config.json"),
            data_folder: base_path.join("data"),
            base_path,
        };

        fs::create_dir_all(&instance.data_folder).unwrap();

        instance
    }

    pub fn config_path(&self) -> &std::path::Path {
        &self.config_path
    }

    pub fn upload_folder(&self) -> &std::path::Path {
        &self.upload_folder
    }

    pub fn data_folder(&self) -> &std::path::Path {
        &self.data_folder
    }

    /// Creates test files in the upload folder.
    ///
    /// The map key is the file path relative to the upload folder, and the value
    /// is the number of 1 KB text lines to write to the file.
    ///
    /// Missing parent directories are created automatically.
    ///
    /// Example:
    /// - `("file.txt", 10)` creates a file with size 10 KB.
    /// - `("subfolder/file.txt", 100)` creates a file with size 100 KB.
    pub fn create_files_in_upload_folder_with_sizes_in_kb(
        &self,
        files: &HashMap<&str, usize>,
    ) {
        let prefix = "This line is 1 kb ";
        let line_of_size_1kb = format!("{}{}\n", prefix, "a".repeat(1024 - prefix.len() - 1));
        assert_eq!(line_of_size_1kb.len(), 1024);

        for (file_name, line_count) in files {
            let file_path = self.upload_folder.join(file_name);
            std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
            let content = std::iter::repeat_n(line_of_size_1kb.as_str(), *line_count)
                .collect::<String>();
            std::fs::write(file_path, content).unwrap();
        }
    }

    pub fn download_folder(&self) -> &std::path::Path {
        &self.download_folder
    }

    pub fn download_folder_is_empty(&self) -> bool {
        if !self.download_folder.exists() {
            return true;
        }

        std::fs::read_dir(&self.download_folder).unwrap().next().is_none()
    }

    pub fn download_folder_contains_file(&self, file : &str) -> bool {
        self.download_folder.join(file).exists()
    }

    pub fn download_folder_amount_of_files(&self) -> usize {
        count_files(&self.download_folder)
    }

    /// Verifies that upload and download folders contain exactly the same files,
    /// folder structure, file contents, and modification timestamps.
    pub fn assert_upload_and_download_folders_match(&self) -> bool {
        let upload_files = test_file_util::collect_files(&self.upload_folder);
        let download_files = test_file_util::collect_files(&self.download_folder);

        let upload_paths: HashSet<_> = upload_files.keys().cloned().collect();
        let download_paths: HashSet<_> = download_files.keys().cloned().collect();

        assert_eq!(
            upload_paths, download_paths,
            "Upload and download folder structures differ"
        );

        for relative_path in upload_paths {
            let upload_file = upload_files.get(&relative_path).unwrap();
            let download_file = download_files.get(&relative_path).unwrap();

            test_file_util::assert_files_match(upload_file, download_file);
        }

        true //This is only here, so that we can wrap this function in assert!(). If a missmatch has happened a panic will be raised.
    }

    /// Verifies that the specified files match between upload and download
    /// folders, including content and modification timestamp.
    pub fn assert_selected_files_match<S>(&self, files: &[S])
    where
        S: AsRef<str>,
    {
        for relative_path in files {
            let relative_path = relative_path.as_ref();

            let upload_file = self.upload_folder.join(relative_path);
            let download_file = self.download_folder.join(relative_path);

            assert!(
                upload_file.exists(),
                "Upload file does not exist: {}",
                relative_path
            );

            assert!(
                download_file.exists(),
                "Download file does not exist: {}",
                relative_path
            );

            test_file_util::assert_files_match(&upload_file, &download_file);
        }
    }

    pub fn write_config(&self, contents: &Value) {
        fs::create_dir_all(&self.config_path.parent().unwrap()).unwrap();
        fs::write(&self.config_path, serde_json::to_string_pretty(contents).unwrap()).unwrap();
    }

    pub fn write_config_file(&self, s3_access_key: &str, s3_secret_key: &str, s3_url: &str, name : &str) {
        let config_content = serde_json::json!({
            "version": 1,
            "global": {
                "s3_region": "us-east-1"
            },
            "endpoints": [
                {
                    "name": name,
                    "config": {
                        "s3_access_key": s3_access_key,
                        "s3_secret_key": s3_secret_key,
                        "s3_url": s3_url,
                        "s3_region": "us-east-1"
                    }
                }
            ]
        });

        Self::write_config(&self, &config_content);
    }
}

impl Drop for TestFileHandler {
    fn drop(&mut self) {
        println!("Cleaning up TestFileHandler");
        std::fs::remove_dir_all(self.base_path.clone()).ok();
    }
}


fn count_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                count_files(&path)
            } else {
                1
            }
        })
        .sum()
}