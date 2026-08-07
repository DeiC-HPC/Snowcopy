use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use crate::common::test_constants::SNOWCOPY_TEST_DATA_BASE_DIR_PREFIX;
use std::path::PathBuf;
use crate::common::{test_file_handler, test_file_util};

pub struct TestFileHandler {
    config_path: PathBuf,
    base_path: PathBuf,
    upload_folder: PathBuf,
    download_folder: PathBuf,
}

impl TestFileHandler {
    pub fn new() -> Self {
        let base_path =std::env::temp_dir().join(SNOWCOPY_TEST_DATA_BASE_DIR_PREFIX).join(Uuid::new_v4().to_string());

        Self {
            upload_folder: base_path.join("upload"),
            download_folder: base_path.join("download"),
            config_path: base_path.join("config.json"),
            base_path,
        }
    }

    pub fn config_path(&self) -> &std::path::Path {
        &self.config_path
    }

    pub fn upload_folder(&self) -> &std::path::Path {
        &self.upload_folder
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
        files: HashMap<&str, usize>,
    ) {
        let prefix = "This line is 1 kb ";
        let line_of_size_1kb = format!("{}{}\n", prefix, "a".repeat(1024 - prefix.len() - 1));
        assert_eq!(line_of_size_1kb.len(), 1024);

        for (file_name, line_count) in files {
            let file_path = self.upload_folder.join(file_name);
            std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
            let content = std::iter::repeat_n(line_of_size_1kb.as_str(), line_count)
                .collect::<String>();
            std::fs::write(file_path, content).unwrap();
        }
    }

    pub fn download_folder(&self) -> &std::path::Path {
        &self.download_folder
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
}

impl Drop for TestFileHandler {
    fn drop(&mut self) {
        println!("Cleaning up TestFileHandler");
        std::fs::remove_dir_all(self.base_path.clone()).ok();
    }
}