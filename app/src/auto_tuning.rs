use crate::constants::MAX_REQUEST_AMOUNT;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sysinfo::{Disks, Networks, System};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Semaphore;
use util_layer::cc_util::CommandCenterEvent;
use util_layer::{DownloadConfig, UploadConfig};

// Network speed detection is supported on Linux more easily than on Windows and MacOS. TODO: take a second look at this
fn read_network_speed(_interface_name: &str) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        use std::fs;

        let speed_path = format!("/sys/class/net/{}/speed", _interface_name);
        fs::read_to_string(speed_path)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
    }

    #[cfg(target_os = "windows")]
    {
        None
    }

    #[cfg(target_os = "macos")]
    {
        None
    }

    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {

        None
    }
}

pub fn auto_tune_upload(config: Arc<UploadConfig>, cc_tx: UnboundedSender<CommandCenterEvent>) -> (Semaphore, Semaphore, Semaphore, usize) {
    let (mut file_limit, mut req_limit, mut open_files_sem, buffer_pool_size, _) = auto_tune_internal(&config.local_path, cc_tx.clone());

    if let Some(override_value) = config.file_limit_overwrite { // TODO LND REFACTOR: Type guards here! Ensure proper error handling of it
        file_limit = override_value;
    }
    if let Some(override_value) = config.req_limit_overwrite {
        req_limit = override_value;
    }
    if let Some(override_value) = config.open_file_limit_overwrite {
        open_files_sem = override_value;
    }

    let _ = cc_tx.send(CommandCenterEvent::Log(format!(
        "<Init. tuning: file_limit: {}, req_limit: {}, buffer_pool_size: {:.2} GiB>",
        file_limit,
        req_limit,
        buffer_pool_size as f64 / (1024.0 * 1024.0 * 1024.0)
    )));

    let file_sem = Semaphore::new(file_limit);
    let req_sem = Semaphore::new(req_limit);
    let open_files_sem = Semaphore::new(open_files_sem);

    (file_sem, req_sem, open_files_sem, buffer_pool_size)
}

pub fn auto_tune_download(config: Arc<DownloadConfig>, cc_tx: UnboundedSender<CommandCenterEvent>) -> (Arc<Semaphore>, Arc<Semaphore>, usize) {
    let (mut file_limit, mut req_limit, _, _buffer_pool_size, download_file_concurrency) = auto_tune_internal(&config.local_path, cc_tx.clone());

    if let Some(override_value) = config.file_limit_overwrite {
        file_limit = override_value;
    }
    if let Some(override_value) = config.req_limit_overwrite {
        req_limit = override_value;
    }

    let _ = cc_tx.send(CommandCenterEvent::Log(format!(
        "<Init. tuning: file_limit: {}, req_limit: {}, download file concurrency: {}>",
        file_limit,
        req_limit,
        download_file_concurrency
    )));

    let file_sem = Arc::new(Semaphore::new(file_limit));
    let req_sem = Arc::new(Semaphore::new(req_limit));
    (file_sem, req_sem, download_file_concurrency)
}

pub fn auto_tune_delete(config: &util_layer::RemoveConfig, cc_tx: UnboundedSender<CommandCenterEvent>) -> Semaphore {
    let current_path = PathBuf::from(".");
    let (_, mut req_limit, _, _, _) = auto_tune_internal(&current_path, cc_tx.clone());

    if let Some(override_value) = config.req_limit_overwrite {
        req_limit = override_value;
    }

    Semaphore::new(req_limit)
}

fn auto_tune_internal(  // TODO LND REFACTOR: Instead of testing the entire auto_tune_internal, extract the logic into testable smaller functions
    path: &Path,
    cc_tx: UnboundedSender<CommandCenterEvent>,
) -> (usize, usize, usize, usize, usize) {
    let (cpu_cores, max_network_speed, disk_type, available_memory, total_memory, filesystem) = detect(path);

    let buffer_pool_size: usize = {
        if available_memory > 16 * 1024 * 1024 * 1024  {
            4 * 1024 * 1024 * 1024
        } else if available_memory > 4 * 1024 * 1024 * 1024 {
            2 * 1024 * 1024 * 1024 //TODO jon: This should have a relationship with req_limit
        } else {
            (available_memory / 2) as usize
        }
    };

    let file_limit = { //TODO Jon: should filesystem be considered here?
        let mut limit = match disk_type {
            Some(sysinfo::DiskKind::HDD) => 8,
            Some(sysinfo::DiskKind::SSD) | None | _ => 16,
        };

        //The following is simply rough heuristics of big systems.
        if cpu_cores > 100 {
            limit = limit * 2;
        } else if cpu_cores > 32 {
            limit = (limit as f64 * 1.5) as usize;
        }
        if let Some(speed) = max_network_speed {
            if speed >= 10000 {
                limit = limit * 2;
            }
        }

        if let Some(speed) = max_network_speed {
            if speed >= 100000 {
                limit = limit * 2;
            }
        }
        limit
    };

    let req_limit = {
        let mut limit = 0;
        if max_network_speed.is_some() {
            if let Some(speed) = max_network_speed {
                limit = 32;
                if speed >= 10000 {
                    limit = limit * 4;
                }
                if speed >= 20000 {
                    limit = (limit as f32 * 1.5) as usize;
                }
                if speed >= 40000 {
                    limit = (limit as f32 * 1.3) as usize;
                }
                if speed >= 100000 {
                    limit = (limit as f32 * 1.3) as usize;
                }
            }

            if total_memory > 100 * 1024 * 1024 * 1024 { //big server
                limit = (limit as f32 * 1.1) as usize;
            }

            let req_limit_by_buffer_size = buffer_pool_size / util_layer::constants::CHUNK_SIZE as usize;
            if limit > req_limit_by_buffer_size {
                limit = req_limit_by_buffer_size;
            }

            if limit > MAX_REQUEST_AMOUNT {
                limit = MAX_REQUEST_AMOUNT;
            }
        } else {
            //we are likely not on a linux pc
            limit = 32;
            if cpu_cores > 16 {
                limit = limit * 2;
            }
        }

        limit
    };

    let open_file_limit = req_limit + file_limit + 100; // TODO LND: This is not a permanent value - it should be using known ulimit constraints and adjusted based on other semaphores
    let download_file_concurrency = {
        match disk_type {
            Some(sysinfo::DiskKind::HDD) => 20,
            _ => 100,
        }
    };

    let _ = cc_tx.send(CommandCenterEvent::Log(format!(
        "<Hardware detected: CPU cores: {}, Disk type: {}, File system: {}, Available memory: {:.2} GiB, Network speed: {} Mbps>",
        cpu_cores,
        disk_type.map(|dt| format!("{:?}", dt)).unwrap_or_else(|| "N/A".to_string()),
        filesystem,
        available_memory as f64 / (1024.0 * 1024.0 * 1024.0),
        max_network_speed.map(|s| format!("{}", s)).unwrap_or_else(|| "N/A".to_string())
    )));

    (file_limit, req_limit, open_file_limit, buffer_pool_size, download_file_concurrency)
}

fn canonicalize_for_compare(path: &Path) -> PathBuf {
    let p = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());

    #[cfg(windows)]
    {
        use std::path::{Component, Prefix, PrefixComponent};

        fn de_verbatim_prefix(pc: PrefixComponent<'_>) -> PathBuf {
            match pc.kind() {
                // \\?\C:\...  ->  C:\...
                Prefix::VerbatimDisk(disk) => {
                    let drive = format!("{}:", disk as char);
                    PathBuf::from(drive)
                }
                // \\?\UNC\server\share\...  ->  \\server\share\...
                Prefix::VerbatimUNC(server, share) => {
                    let mut out = PathBuf::from(r"\\");
                    out.push(server);
                    out.push(share);
                    out
                }
                // Already non-verbatim (Disk/UNC/etc.) or something exotic: keep as-is.
                _ => PathBuf::from(pc.as_os_str()),
            }
        }

        let mut comps = p.components();
        match comps.next() {
            Some(Component::Prefix(prefix)) => {
                let mut out = de_verbatim_prefix(prefix);
                for c in comps {
                    out.push(c.as_os_str());
                }
                out
            }
            _ => p,
        }
    }

    #[cfg(not(windows))]
    {
        p
    }
}

fn detect(path: &Path) -> (usize, Option<u64>, Option<sysinfo::DiskKind>, u64, u64, String) {
    let mut sys = System::new();
    sys.refresh_cpu_all();
    let cpu_cores = sys.cpus().len();
    sys.refresh_memory();
    let available_memory = sys.available_memory();
    let total_memory = sys.total_memory();

    let disks = Disks::new_with_refreshed_list();
    let canonical_path = canonicalize_for_compare(path);
    let mut disk_kind_for_path: Option<sysinfo::DiskKind> = None;
    let mut fs_for_path: String = "N/A".to_string();
    for disk in &disks {
        if canonical_path.starts_with(disk.mount_point()) {
            disk_kind_for_path = Some(disk.kind());
            fs_for_path = disk.file_system().to_str().unwrap_or_else(|| "N/A").to_string();
            break;
        }
    }

    // Network interfaces name, total data received and total data transmitted:
    let networks = Networks::new_with_refreshed_list();

    let mut max_network_speed: Option<u64> = None;
    for (interface_name, data) in &networks { //TODO Jon: we can actually check for networking errors here: data.errors_on_transmitted() it might be interesting.
        // Skip loopback interface
        if interface_name == "lo" {
            continue;
        }

        if data.total_transmitted() > 0 {
            if let Some(speed) = read_network_speed(interface_name) {
                max_network_speed = Some(max_network_speed.map_or(speed, |max| max.max(speed)));
            }
        }
    }

    (cpu_cores, max_network_speed, disk_kind_for_path, available_memory, total_memory, fs_for_path)
}