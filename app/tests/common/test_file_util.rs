use std::collections::{HashMap};
use std::path::{Path, PathBuf};
use std::time::SystemTime;


pub(crate) fn assert_files_match(upload_file: &Path, download_file: &Path) {
    //println!("DEBUG: Comparing files: {} and {}", upload_file.display(), download_file.display());

    let upload_content = std::fs::read(upload_file).unwrap();
    let download_content = std::fs::read(download_file).unwrap();

    assert_eq!(
        upload_content,
        download_content,
        "File content differs: {:?}",
        upload_file
    );

    let upload_modified = normalized_modified_time(upload_file);
    let download_modified = normalized_modified_time(download_file);

    assert_eq!(
        upload_modified,
        download_modified,
        "Modification time differs: {:?}",
        upload_file
    );
}

pub(crate) fn normalized_modified_time(path: &Path) -> SystemTime {
    std::fs::metadata(path)
        .unwrap()
        .modified()
        .unwrap()
}

pub(crate) fn collect_files(root: &Path) -> HashMap<PathBuf, PathBuf> {
    fn recurse(
        root: &Path,
        current: &Path,
        files: &mut HashMap<PathBuf, PathBuf>,
    ) {
        for entry in std::fs::read_dir(current).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();

            if path.is_dir() {
                recurse(root, &path, files);
            } else {
                let relative = path.strip_prefix(root).unwrap().to_path_buf();
                files.insert(relative, path);
            }
        }
    }

    let mut files = HashMap::new();

    if root.exists() {
        recurse(root, root, &mut files);
    }

    files
}