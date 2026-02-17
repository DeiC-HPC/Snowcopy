use crate::{constants, read_config};
use std::io;
use std::path::{Path, PathBuf};

use crate::constants::CHUNK_SIZE;
use aws_layer::{AWSClient, CompletedPartAws};
use futures::future::join_all;
use http_layer::{build_https_client, BufferPool, PoolClient};
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::thread;
use std::thread::available_parallelism;
use std::time::Duration;
use sysinfo::{
    Disks, Networks, System,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use walkdir::WalkDir;

pub enum UiEvent {
    AddToTotal(u64),
    Uploaded(u64),
    Message(String),
}

pub async fn run_s3_ceph_tokio(bucket: &str, path: &Path, file_limit: Option<usize>, req_limit: Option<usize>, silent: bool) {
    let (ui_tx, ui_rx) = mpsc::unbounded_channel::<UiEvent>();
    let mut file_discovery_channel = walk_directory(path.to_path_buf(), ui_tx.clone(), silent).await;

    let config = read_config();

    let client = aws_layer::create_client(config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str()).await;

    let bucket_creation_handle = {
        let client = client.clone();
        let bucket = bucket.to_string();
        tokio::spawn(async move {
            aws_layer::create_bucket(&client, &bucket).await.unwrap()
        })
    };

    let hyper_client = build_https_client();
    let (file_sem, req_sem, buffer_pool) = auto_tune_semaphores(file_limit, req_limit, silent, path);
    let task_sem = Arc::new(Semaphore::new(500));
    let buffer_pool = Arc::new(BufferPool::new(buffer_pool, constants::CHUNK_SIZE as usize));
    
    let pb = if !silent {
        let pb = ProgressBar::new(0);
        pb.set_style(ProgressStyle::with_template("[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})  Status: {msg:25!} ")
            .unwrap()
            .progress_chars("#>-"));
        pb.set_message("File discovery");
        Some(pb)
    } else {
        None
    };

    // UI task
    let ui_handle = if !silent {
        let pb_ui = pb.as_ref().unwrap().clone();
        Some(start_ui_task(ui_rx, pb_ui))
    } else {
        None
    };

    let mut handles = Vec::new();

    bucket_creation_handle.await.unwrap();

    while let Some(result) = file_discovery_channel.recv().await {
        match result {
            Ok(file_info) => {
                let bucket = bucket.to_string();
                let client = client.clone();
                let hyper_client = hyper_client.clone();
                let file_sem_clone = file_sem.clone();
                let req_sem_clone = req_sem.clone();
                let buffer_pool_clone = buffer_pool.clone();
                let ui_channel = ui_tx.clone();

                let permit = task_sem.clone().acquire_owned().await.unwrap();
                let handle = tokio::spawn(async move {
                    upload_file(&bucket, &client, hyper_client, file_info, file_sem_clone, req_sem_clone, buffer_pool_clone, ui_channel).await;
                    drop(permit);
                });
                handles.push(handle);
            }
            Err(e) => {
                eprintln!("Error walking directory: {}", e);
            }
        }
    }

    drop(ui_tx); //This allows the UI task to exit gracefully when all files have been uploaded

    join_all(handles).await;
    if let Some(ref pb) = pb {
        pb.finish_with_message("Success");
    }

    if let Some(ui_handle) = ui_handle {
        let _ = ui_handle.await;
    }
}
fn start_ui_task(mut ui_rx: UnboundedReceiver<UiEvent>, pb_ui: ProgressBar) -> JoinHandle<()> {
    let ui_handle = tokio::task::spawn_blocking(move || {
        let mut total_set = false;
        let mut total: u64 = 0;
        let mut last_update = Instant::now();
        let mut accumulated_bytes: u64 = 0;
        let mut accumulated_total: u64 = 0;
        let update_interval = Duration::from_millis(200);

        while let Some(ev) = ui_rx.blocking_recv() {
            match ev {
                UiEvent::AddToTotal(delta) => {
                    total = total.saturating_add(delta);
                    accumulated_total = accumulated_total.saturating_add(delta);
                }
                UiEvent::Uploaded(delta) => {
                    accumulated_bytes = accumulated_bytes.saturating_add(delta);
                }
                UiEvent::Message(msg) => {
                    pb_ui.set_message(msg);
                }
            }

            let now = Instant::now();
            if now.duration_since(last_update) >= update_interval {
                // Flush accumulated total
                if accumulated_total > 0 {
                    if !total_set {
                        pb_ui.set_length(total);
                        total_set = true;
                    } else {
                        pb_ui.inc_length(accumulated_total);
                    }
                    accumulated_total = 0;
                    thread::sleep(Duration::from_millis(10)); //limit flickering
                }

                // Flush accumulated bytes
                if accumulated_bytes > 0 {
                    pb_ui.inc(accumulated_bytes);
                    accumulated_bytes = 0;
                }

                last_update = now;
            }
        }

        // Final update to ensure all bytes and total are accounted for
        if accumulated_total > 0 {
            if !total_set {
                pb_ui.set_length(total);
            } else {
                pb_ui.inc_length(accumulated_total);
            }
        }
        if accumulated_bytes > 0 {
            pb_ui.inc(accumulated_bytes);
        }
    });
    ui_handle
}

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

fn auto_tune_semaphores(file_limit_override: Option<usize>, req_limit_override: Option<usize>, silent: bool, path: &Path) -> (Arc<Semaphore>, Arc<Semaphore>, usize) {
    let (cpu_cores, max_network_speed, disk_type,  available_memory) = detect(path);

    let buffer_pool_size: usize = {
        if available_memory > 6 * 1024 * 1024 * 1024 {
            2 * 1024 * 1024 * 1024 //TODO jon: This should have a relationship with req_limit
        } else {
            (available_memory / 2) as usize
        }
    };

    let file_limit = { //TODO Jon: should filesystem be considered here?
        if file_limit_override.is_some() {
            file_limit_override.unwrap()
        } else {
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
            limit
        }
    };

    let req_limit = {
        if req_limit_override.is_some() {
            req_limit_override.unwrap()
        } else {
            let mut limit = 0;
            if max_network_speed.is_some() {
                if let Some(speed) = max_network_speed {
                    limit = 32;
                    if speed >= 10000 {
                        limit = limit * 4;
                    }
                    if speed >= 40000 {
                        limit = (limit as f32 * 1.5) as usize;
                    }
                    if speed >= 100000 {
                        limit = (limit as f32 * 1.5) as usize;
                    }
                }

                let req_limit_by_buffer_size = buffer_pool_size / CHUNK_SIZE as usize;
                if limit > req_limit_by_buffer_size {
                    limit = req_limit_by_buffer_size;
                }
            } else {
                //we are likely not on a linux pc
                limit = 32;
                if cpu_cores > 16 {
                    limit = limit * 2;
                }
            }

            limit
        }
    };


    let file_sem = Arc::new(Semaphore::new(file_limit));
    let req_sem = Arc::new(Semaphore::new(req_limit));

    if !silent {
        println!("<Hardware detected: CPU cores: {}, Disk type: {}, Available memory: {:.2} GiB, Network speed: {} Mbps>",
                 cpu_cores, disk_type.map(|dt| format!("{:?}", dt)).unwrap_or_else(|| "N/A".to_string()), available_memory as f64 / (1024.0 * 1024.0 * 1024.0), max_network_speed.map(|s| format!("{}", s)).unwrap_or_else(|| "N/A".to_string()));
        println!("<Init. tuning: file_limit: {}, req_limit: {}, buffer_pool_size: {:.2} GiB>", file_limit, req_limit, buffer_pool_size as f64 / (1024.0 * 1024.0 * 1024.0));
    }

    (file_sem, req_sem, buffer_pool_size)
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

fn detect(path: &Path) -> (usize, Option<u64>, Option<sysinfo::DiskKind>, u64) {
    let cpu_cores = available_parallelism().unwrap().get();
    
    let disks = Disks::new_with_refreshed_list();
    let canonical_path = canonicalize_for_compare(path);
    let mut disk_kind_for_path: Option<sysinfo::DiskKind> = None;
    for disk in &disks {
        if canonical_path.starts_with(disk.mount_point()) {
            disk_kind_for_path = Some(disk.kind());
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

    let mut sys = System::new();
    sys.refresh_memory();
    let available_memory = sys.available_memory();


    (cpu_cores, max_network_speed, disk_kind_for_path, available_memory)
}

async fn upload_file(bucket: &str, aws_client: &AWSClient, hyper_client: PoolClient, file_info: FileInfo, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<UiEvent>) {
    if file_info.file_size > constants::CHUNK_SIZE {
        upload_with_multipart(&bucket, aws_client, &hyper_client, &file_info, file_sem, req_sem, buffer_pool, sender).await;
    } else {
        small_file_upload(bucket, &aws_client, hyper_client, file_info, file_sem, req_sem, buffer_pool, sender).await;
    }
}

async fn upload_with_multipart(bucket: &&str, aws_client: &AWSClient, hyper_client: &PoolClient, file_info: &FileInfo, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<UiEvent>) {
    let key = file_info.key.clone();
    let create_multipart_upload_output = aws_client.create_multipart_upload().bucket(bucket.to_string()).key(key).send().await.unwrap();
    let upload_id = create_multipart_upload_output.upload_id().unwrap();

    let chunk_amount_u64 = (file_info.file_size + constants::CHUNK_SIZE - 1) / constants::CHUNK_SIZE;

    let completed_parts = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    for i in 0..chunk_amount_u64 {
        let start_pos = i * constants::CHUNK_SIZE;
        let amount = std::cmp::min(constants::CHUNK_SIZE, file_info.file_size - start_pos);

        let part_number = (i + 1) as i32;
        let part_presigned = aws_layer::presign_upload_part(aws_client, &bucket, &file_info.key, upload_id, part_number, constants::PRESIGN_LIFETIME).await;

        let hyper_client = hyper_client.clone();
        let file_path = file_info.file_path.clone();
        let presigned_uri = part_presigned.uri().to_string();
        let file_sem_clone = file_sem.clone();
        let req_sem_clone = req_sem.clone();
        let buffer_pool_clone = buffer_pool.clone();
        let completed_parts = completed_parts.clone();
        let sender_cloned = sender.clone();

        let handle = tokio::spawn(async move {
            let etag = put_file_multipart(
                hyper_client,
                &presigned_uri,
                file_path.to_str().unwrap(),
                start_pos,
                amount,
                file_sem_clone,
                req_sem_clone,
                buffer_pool_clone,
            ).await;

            if let Some(etag) = etag {
                let completed_part = CompletedPartAws::builder()
                    .e_tag(etag)
                    .part_number(part_number)
                    .build();
                completed_parts.lock().await.push(completed_part);
            }

            sender_cloned.send(UiEvent::Uploaded(amount)).unwrap();
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let completed_parts = Arc::try_unwrap(completed_parts).unwrap().into_inner();
    aws_layer::complete_multipart_upload(aws_client, bucket, &file_info.key, upload_id, completed_parts).await.unwrap();
}

async fn small_file_upload(bucket: &str, aws_client: &&AWSClient, hyper_client: PoolClient, file_info: FileInfo, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>, sender: UnboundedSender<UiEvent>) {
    let presigned_request = aws_layer::presign_put(
        &aws_client,
        bucket,
        file_info.key.as_str(),
        constants::PRESIGN_LIFETIME,
    )
        .await;

    put_file(
        hyper_client.clone(),
        presigned_request.uri(),
        file_info.file_path.to_str().unwrap(),
        file_sem,
        req_sem,
        buffer_pool,
    )
        .await;

    sender.send(UiEvent::Uploaded(file_info.file_size)).unwrap();
}

async fn put_file_multipart(hyper_client: PoolClient, presigned_url: &str, file_path: &str, start_pos: u64, amount: u64, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>) -> Option<String> {
    match http_layer::put_file_range(presigned_url, file_path, start_pos, amount, &hyper_client, file_sem, req_sem, buffer_pool).await {
        Ok(etag) => Some(etag?),
        Err(e) => {
            eprintln!("Error putting file chunk {} (start: {}, amount: {}): {}", file_path, start_pos, amount, e);
            panic!("Error putting file chunk {} (start: {}, amount: {}): {}", file_path, start_pos, amount, e);
        }
    }
}

pub struct FileInfo {
    pub file_path: PathBuf,
    pub key: String,
    pub file_size: u64,
}

async fn put_file(hyper_client: PoolClient, url: &str, path: &str, file_sem: Arc<Semaphore>, req_sem: Arc<Semaphore>, buffer_pool: Arc<BufferPool>) {
    if let Err(e) = http_layer::put_file(url, path, &hyper_client, file_sem, req_sem, buffer_pool).await {
        eprintln!("Error putting file {}: {}", path, e);
    }
}

pub async fn walk_directory(base_path: PathBuf, ui_tx: UnboundedSender<UiEvent>, silent: bool) -> mpsc::Receiver<Result<FileInfo, io::Error>> {
    let (tx, rx) = mpsc::channel(5000);

    tokio::task::spawn_blocking(move || {
        for entry in WalkDir::new(base_path).max_open(100) {
            match entry {
                Ok(entry) => {
                    if entry.file_type().is_file() {
                        let file_size = entry.metadata().unwrap().len();
                        let file_path = entry.path().to_path_buf();
                        let key: String = util_layer::normalize_path(file_path.to_str().unwrap());

                        if !silent {
                            let add_to_total = UiEvent::AddToTotal(file_size);
                            if ui_tx.send(add_to_total).is_err() {
                                break;
                            }
                        }
                        let file_info = FileInfo { file_path, key, file_size };
                        if tx.blocking_send(Ok(file_info)).is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    let io_error = io::Error::new(io::ErrorKind::Other, e);
                    if tx.blocking_send(Err(io_error)).is_err() {
                        break;
                    }
                }
            }
        }
        ui_tx.send(UiEvent::Message("Uploading".to_string()))
    });

    rx
}