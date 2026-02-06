//TODO put this in its own crate if it should survive.
//Probably missing some imports here.

/*
use compio::fs::{File, OpenOptions};
use compio::io::AsyncReadAt;
use cyper::{Body, Client, Request, RequestBuilder};
use futures::future::join_all;
use http::{Method, StatusCode};
use compio::BufResult;
use futures_util::stream;



async fn put_file_multipart(url: &str, path: &str, start_pos: u64, end_pos: u64, client: Client) -> String {
    let file = OpenOptions::new().read(true).open(path).await.unwrap();
    let stream = file_slice_stream(file, start_pos, end_pos - start_pos, 1024 * 1024);

    let stream = send_wrapper::SendWrapper::new(stream);

    let req = client
        .put(url).unwrap()
        .body(Body::stream(stream))
        .build();

    let resp = client.execute(req).await.unwrap();
    if resp.status() != StatusCode::OK {
        println!("{}", resp.status());
    }
    resp.headers().get("etag").unwrap().to_str().unwrap().to_string()
}


fn start_workers(num_workers: usize, permits : usize)
                     -> (mpsc::Sender<CompioCmd>, mpsc::Receiver<CompioReply>)
{
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<CompioCmd>(256);
    let (reply_tx, reply_rx) = mpsc::channel::<CompioReply>(256);

    let global_sem = Arc::new(Semaphore::new(permits));
    //let client = cyper::Client::builder().use_rustls_default().build();

    // Each worker gets its own inbound queue
    let mut worker_senders = Vec::new();

    for _ in 0..num_workers {
        let (w_tx, w_rx) = mpsc::channel::<CompioCmd>(64);
        worker_senders.push(w_tx);

        // Start each worker thread
        start_compio_worker(w_rx, reply_tx.clone(), global_sem.clone(), cyper::Client::builder().use_rustls_default().build()); //TODO Jon: can we use one client?
    }

    // A dispatcher task that owns cmd_rx
    tokio::spawn(async move {
        let mut round_robin = 0;

        while let Some(cmd) = cmd_rx.recv().await {
            let idx = round_robin % worker_senders.len();
            round_robin += 1;

            let _ = worker_senders[idx].send(cmd).await;
        }
    });

    (cmd_tx, reply_rx)
}


async fn run_s3_ceph_compio(bucket: &str, path: &Path) {
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let (cmd_tx, mut reply_rx) = start_workers(std::cmp::max(num_cpus -1, 1), 100);

    // Read config file
    let config = create_config();

    let client = s3_helper::create_client(Region::new(constants::DEFAULT_REGION), config.s3_access_key.as_str(), config.s3_secret_key.as_str(), config.s3_url.as_str()).await;
    s3_helper::create_bucket(&client, bucket).await.unwrap();

    let root = PathBuf::from(path);
    let mut upload_object_future_group = Vec::new();
    let mut completed_file_count = 0;

    // Track multipart uploads: path -> (upload_id, key, total_parts, collected_parts)
    let mut multipart_state: HashMap<String, (String, String, i32, Vec<CompletedPart>)> = HashMap::new();

    walk_dir_files(&root, |file_path| {
        let metadata = fs::metadata(file_path)?;
        let size = metadata.len();

        let key_clone = normalize_path(file_path.to_str().unwrap()).to_string();
        let bucket_clone = bucket.to_string();
        let client_clone = client.clone();
        let cmd_tx = cmd_tx.clone();
        let file_path_str = file_path.to_str().unwrap().to_string();

        let future = async move {
            if size > constants::CHUNK_SIZE {
                async move {
                    //Start
                    let output: CreateMultipartUploadOutput = client_clone.create_multipart_upload().bucket(bucket_clone.clone()).key(key_clone.clone()).send().await.unwrap();

                    let upload_id = output.upload_id().unwrap();

                    let chunk_amount_u64 = (size + constants::CHUNK_SIZE - 1) / constants::CHUNK_SIZE;
                    let total_parts = chunk_amount_u64 as i32;

                    for i in 0..chunk_amount_u64 {
                        let start_pos = i * constants::CHUNK_SIZE;
                        let end_pos = std::cmp::min((i + 1) * constants::CHUNK_SIZE, size);

                        let part_number = (i + 1) as i32;
                        let part_presigned = presign_upload_part(&client_clone, &bucket_clone, &key_clone, upload_id, part_number, constants::PRESIGN_LIFETIME).await;


                        cmd_tx.send(CompioCmd::PutMultipart { url: part_presigned.uri().into(), path: file_path_str.clone(), start_pos, end_pos, part_number, upload_id: upload_id.to_string(), total_parts}).await.expect("Failed to send command");
                    }
                }.await
            } else {
                let presigned = presign_put_object(&client_clone, &bucket_clone, &key_clone, constants::PRESIGN_LIFETIME).await;
                cmd_tx.send(CompioCmd::PutFile {
                    url: presigned.uri().into(),
                    path: file_path_str
                }).await.expect("Failed to send command");
            }
        };

        upload_object_future_group.push(future);

        Ok(())
    }).expect("Iterating over path failed");

    let upload_count = upload_object_future_group.len();
    println!("Starting upload of {} files", upload_count);
    join_all(upload_object_future_group).await;
    while let Some(evt) = reply_rx.recv().await {
        let config_clone = config.clone();

        match evt {
            PutFile { path } => {
                println!("{:?} uploaded.", path);
                completed_file_count = completed_file_count + 1;
                println!("Completed files {} of {}", completed_file_count, upload_count);
            }
            PutMultipart { path, part_number, etag, upload_id, total_parts } => {
                //println!("{:?} uploaded. Part number: {}, etag: {}", path, part_number, etag);

                let entry = multipart_state.entry(path.clone()).or_insert((upload_id.clone(), String::new(), total_parts, Vec::new()));

                // Store the key from the path if not already set
                if entry.1.is_empty() {
                    entry.1 = path.clone();
                }

                // Add the completed part
                entry.3.push(CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build());

                // Check if all parts have been received
                if entry.3.len() == total_parts as usize {
                    //println!("All parts received for {:?}, completing multipart upload", path);

                    // Sort parts by part number
                    entry.3.sort_by_key(|p| p.part_number());

                    let (upload_id_val, key_val, _, parts) = multipart_state.remove(&path).unwrap();

                    /*
                    let bucket = String::from(bucket);
                    compio_tx.clone().send(CompioCmd::CompleteMultipart { key: key_val.clone(), upload_id: upload_id_val.clone(), parts: parts.clone(), config : config_clone, bucket }).await.expect("Failed to send command");
                    */

                    complete_multipart_upload(&client, bucket, &key_val, &upload_id_val, parts).await.expect("Failed to complete multipart upload");

                    completed_file_count = completed_file_count + 1;
                    println!("{:?} uploaded.", path);
                    println!("Completed files {} of {}", completed_file_count, upload_count);

                }
            }

            /*
            CompioReply::CompleteMultipart { .. } => {
                completed_file_count = completed_file_count + 1;

            }

             */
        }


        if completed_file_count == upload_count {
            break;
        }
    }
}

async fn put_file(url: &str, path: &str, client: Client) {
    // Open the file asynchronously with Compio (io_uring on Linux).
    let file = match File::open(Path::new(path)).await {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{url} ERROR opening {path}: {e}");
            return;
        }
    };

    // Stream the file directly as the HTTP request body.
    // Cyper implements Body::from(compio::fs::File)
    let body = Body::from(file); // [1](https://docs.rs/compio/latest/compio/fs/struct.Metadata.html)

    let builder = RequestBuilder::new(client, Request::new(Method::PUT, Url::parse(url).unwrap()));

    match builder.body(body).send().await {
        Ok(_res) => {
            // Optionally read the response
            //let bytes = res.into_body().read_to_end().await.unwrap_or_default();

        }
        Err(e) => {
            eprintln!("{url} ERROR: {e}");
        }
    }
}

/// Stream 'len' bytes from 'file' starting at 'start', in chunks up to 'chunk_size'.
/// The stream yields `Bytes` and is accepted by `cyper::Body::stream`.
fn file_slice_stream(
    file: compio::fs::File,  // <-- not Arc
    start: u64,
    len: u64,
    chunk_size: usize,
) -> impl futures_util::Stream<Item = cyper::Result<Bytes>> + 'static {
    stream::unfold((file, start, len), move |(file, mut pos, mut remaining)| async move {
        if remaining == 0 {
            return None;
        }

        let to_read = remaining.min(chunk_size as u64) as usize;
        let buf = Vec::with_capacity(to_read);

        match file.read_at(buf, pos).await {
            BufResult(Ok(n), mut buf) => {
                if n == 0 {
                    return None;
                }
                unsafe { buf.set_len(n); }
                let chunk = Bytes::from(buf);
                pos += n as u64;
                remaining -= n as u64;
                Some((Ok(chunk), (file, pos, remaining)))
            }
            BufResult(Err(e), _) => {
                Some((Err(cyper::Error::from(e)), (file, pos, 0)))
            }
        }

    })
}

fn walk_dir_files(dir: impl AsRef<Path>, mut on_file: impl FnMut(&Path) -> io::Result<()>) -> io::Result<()> {
    fn recurse(dir: &Path, on_file: &mut dyn FnMut(&Path) -> io::Result<()>) -> io::Result<()> {
        for entry_res in fs::read_dir(dir)? {
            let entry = entry_res?;
            let path = entry.path();

            // Follow directories; process files
            if path.is_dir() {
                recurse(&path, on_file)?;
            } else if path.is_file() {
                on_file(&path)?;
            }
        }
        Ok(())
    }

    if dir.as_ref().is_file() {
        on_file(&dir.as_ref().to_path_buf())
    } else {
        recurse(dir.as_ref(), &mut on_file)
    }
}
 */
