mod collection_message_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    const CHAT_ID: i64 = 123_456_789;
    const JOB_LABEL: &str = "Bilibili download";

    fn create_collection_queue_with_job(
        label: &str,
        task_id: &str,
        chat_id: i64,
        job_id: u64,
        job: JobRequest,
    ) -> (PathBuf, AppConfig, Arc<QueueManager>) {
        let queue_root = temp_main_test_dir(label);
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = queue_root.join("videos");
        config.downloads.pdf_dir = queue_root.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).expect("video root should create");
        fs::create_dir_all(&config.downloads.pdf_dir).expect("PDF root should create");

        let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
        let task = TaskRecord::new(
            task_id.to_string(),
            job_id as i64,
            job_id as i64 + 1_000,
            chat_id,
            Some(42),
            0,
            job,
        );
        assert!(queue.create(task).expect("collection task should persist"));

        (queue_root, config, queue)
    }

    fn create_running_collection_queue(
        label: &str,
        task_id: &str,
        chat_id: i64,
        job_id: u64,
    ) -> (PathBuf, AppConfig, Arc<QueueManager>) {
        let (queue_root, config, queue) = create_collection_queue_with_job(
            label,
            task_id,
            chat_id,
            job_id,
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV1234567890".to_string(),
                selection: None,
            },
        );
        queue
            .set_status(task_id, TaskStatus::Queued, None)
            .expect("collection task should enter the queue");
        let started = queue
            .begin_run_if_generation(task_id, 0)
            .expect("generation zero should start")
            .expect("queued collection should enter running state");
        assert_eq!(started.status, TaskStatus::Running);
        assert_eq!(started.generation, 0);

        (queue_root, config, queue)
    }

    fn progress_context(
        task_id: &str,
        chat_id: i64,
        job_id: u64,
        generation: u64,
        status_message_id: Option<i64>,
    ) -> JobProgressContext {
        JobProgressContext {
            chat_id,
            job_id,
            task_id: task_id.to_string(),
            job_label: JOB_LABEL,
            is_collection: true,
            status_message_id,
            update_interval: Duration::from_millis(1),
            generation,
        }
    }

    fn finalization_context<'a>(
        telegram: &'a TelegramClient,
        queue: &'a QueueManager,
        task_id: &'a str,
        generation: u64,
        chat_id: i64,
        job_id: u64,
    ) -> ProgressDeliveryContext<'a> {
        ProgressDeliveryContext {
            telegram,
            queue,
            task_id,
            generation,
            chat_id,
            job_id,
            job_label: JOB_LABEL,
        }
    }

    fn bot_context(
        telegram: TelegramClient,
        config: AppConfig,
        queue: Arc<QueueManager>,
        next_job_id: u64,
    ) -> BotContext {
        BotContext {
            telegram,
            config: Arc::new(config),
            job_dispatch: JobDispatch {
                download_semaphore: Arc::new(Semaphore::new(1)),
                duplicate_scan_semaphore: Arc::new(Semaphore::new(1)),
            },
            next_job_id: Arc::new(AtomicU64::new(next_job_id)),
            queue,
            queue_start_retry_delay: Duration::ZERO,
        }
    }

    fn queued_only_manifest() -> BilibiliCollectionManifest {
        let mut manifest = test_collection_manifest(1);
        manifest.skipped_entries = 0;
        manifest.planned_entries = 1;
        let entry = &mut manifest.entries[0];
        entry.status = BilibiliCollectionEntryStatus::Queued;
        entry.video = Some("1080P 1920x1080 60fps H.264".to_string());
        entry.audio = Some("Japanese AAC 128 kbps".to_string());
        entry.estimated_media = "240.0 MiB".to_string();
        manifest
    }

    fn all_already_present_manifest(entry_count: u32) -> BilibiliCollectionManifest {
        let mut manifest = test_collection_manifest(entry_count);
        manifest.skipped_entries = entry_count as usize;
        manifest.planned_entries = 0;
        manifest.estimated_media = "0 bytes to download".to_string();
        for entry in &mut manifest.entries {
            entry.status = BilibiliCollectionEntryStatus::AlreadyPresent;
            entry.video = None;
            entry.audio = None;
            entry.estimated_media = "already present".to_string();
        }
        manifest
    }

    fn collection_progress(manifest: &BilibiliCollectionManifest) -> JobProgress {
        let entry = test_collection_entry_progress(manifest, 1);
        JobProgress {
            message: "BBDown-rust: downloading collection entry".to_string(),
            resolved_summary: None,
            collection: Some(test_collection_snapshot(manifest, 0, Some(entry))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn track_single_entry_collection(
        telegram: &TelegramClient,
        queue: Arc<QueueManager>,
        task_id: &str,
        chat_id: i64,
        job_id: u64,
        generation: u64,
        manifest: BilibiliCollectionManifest,
        start_entry: bool,
        complete_entry: bool,
        fail: bool,
        requests: &mut mpsc::UnboundedReceiver<FakeTelegramRequest>,
    ) -> Option<CollectionProgressDelivery> {
        let started_at = std::time::Instant::now();
        let (progress, progress_rx) = job_progress_channel();
        let mut progress_task = tokio::spawn(forward_progress(
            telegram.clone(),
            Arc::clone(&queue),
            progress_context(task_id, chat_id, job_id, generation, None),
            progress_rx,
        ));

        let mut lifecycle_event_count = 0;
        let mut progress_update_count = 0;
        progress.send_lifecycle(JobProgressLifecycleEvent::Resolved {
            snapshot: test_collection_snapshot(&manifest, 0, None),
            manifest: manifest.clone(),
        });
        lifecycle_event_count += 1;

        let entry = test_collection_entry_progress(&manifest, 1);
        if start_entry {
            progress.send_lifecycle(JobProgressLifecycleEvent::EntryStarted {
                entry: entry.clone(),
                snapshot: test_collection_snapshot(&manifest, 0, Some(entry.clone())),
            });
            lifecycle_event_count += 1;
            progress.send_replace(Some(collection_progress(&manifest)));
            progress_update_count += 1;
        }

        if complete_entry {
            progress.send_lifecycle(JobProgressLifecycleEvent::EntryCompleted {
                entry,
                file_count: 1,
                snapshot: test_collection_snapshot(&manifest, 1, None),
            });
            lifecycle_event_count += 1;
            progress.send_lifecycle(JobProgressLifecycleEvent::Completed {
                snapshot: test_collection_snapshot(&manifest, 1, None),
            });
            lifecycle_event_count += 1;
        } else if fail {
            progress.send_lifecycle(JobProgressLifecycleEvent::Failed {
                snapshot: test_collection_snapshot(&manifest, 0, start_entry.then_some(entry)),
            });
            lifecycle_event_count += 1;
        }

        drop(progress);
        match tokio_timeout(Duration::from_secs(5), &mut progress_task).await {
            Ok(result) => result.expect("collection progress task should not panic"),
            Err(_) => {
                progress_task.abort();
                let _ = progress_task.await;
                let observed = take_fake_telegram_requests(requests);
                let sends = observed
                    .iter()
                    .filter(|request| request.method == "sendMessage")
                    .count();
                let edits = observed
                    .iter()
                    .filter(|request| request.method == "editMessageText")
                    .count();
                let last_request = observed
                    .last()
                    .map(|request| {
                        format!(
                            "{} message_id={:?}",
                            request.method,
                            request.body["message_id"].as_i64()
                        )
                    })
                    .unwrap_or_else(|| "none".to_string());
                panic!(
                    "collection progress forwarding timed out after {:?}; queued {lifecycle_event_count} lifecycle events and {progress_update_count} progress updates; observed {sends} sends and {edits} edits; last request: {last_request}",
                    started_at.elapsed()
                );
            }
        }
    }

    fn sent_entry_message_ids(requests: &[FakeTelegramRequest]) -> HashMap<u32, i64> {
        let mut entry_ids = HashMap::new();
        for (send_ordinal, request) in requests
            .iter()
            .filter(|request| request.method == "sendMessage")
            .enumerate()
        {
            let message_id = 1_000_i64 + send_ordinal as i64;
            let text = request.body["text"].as_str().unwrap_or_default();
            if let Some(index) = text.lines().find_map(|line| {
                let value = line.trim().strip_prefix("Entry: ")?;
                value.split_once('/')?.0.parse::<u32>().ok()
            }) {
                assert!(
                    entry_ids.insert(index, message_id).is_none(),
                    "collection entry {index} should receive only one initial message"
                );
            }
        }
        entry_ids
    }

    async fn spawn_fake_telegram_api_with_one_entry_edit_failure() -> (
        TelegramClient,
        mpsc::UnboundedReceiver<FakeTelegramRequest>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
        Arc<AtomicBool>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake Telegram API should bind localhost");
        let address = listener
            .local_addr()
            .expect("fake Telegram API should expose its address");
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let failed_once = Arc::new(AtomicBool::new(false));
        let failed_once_for_server = Arc::clone(&failed_once);
        let server = tokio::spawn(async move {
            let mut next_message_id = 1_000_i64;
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => return Ok(()),
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.context("fake Telegram API accept failed")?;
                        let request = read_fake_telegram_request(&mut stream).await?;
                        let fail_this_edit = request.method == "editMessageText"
                            && request.body["message_id"].as_i64() == Some(1_001)
                            && !failed_once_for_server.swap(true, Ordering::SeqCst);
                        let response = if fail_this_edit {
                            let payload = serde_json::json!({
                                "ok": false,
                                "error_code": 500,
                                "description": "simulated transient edit failure"
                            })
                            .to_string();
                            format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
                                payload.len()
                            )
                        } else {
                            fake_telegram_response(&request, &mut next_message_id, Vec::new())
                        };
                        stream
                            .write_all(response.as_bytes())
                            .await
                            .context("fake Telegram API response write failed")?;
                        let _ = requests_tx.send(request);
                    }
                }
            }
        });
        let token = AppConfig::for_test().telegram.token;
        (
            TelegramClient::with_test_api_base_url(token, format!("http://{address}")),
            requests_rx,
            shutdown_tx,
            server,
            failed_once,
        )
    }

    async fn spawn_fake_telegram_api_with_one_final_main_edit_failure(
        final_prefix: &'static str,
    ) -> (
        TelegramClient,
        mpsc::UnboundedReceiver<FakeTelegramRequest>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
        Arc<AtomicBool>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake Telegram API should bind localhost");
        let address = listener
            .local_addr()
            .expect("fake Telegram API should expose its address");
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let failed_once = Arc::new(AtomicBool::new(false));
        let failed_once_for_server = Arc::clone(&failed_once);
        let server = tokio::spawn(async move {
            let mut next_message_id = 1_000_i64;
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => return Ok(()),
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.context("fake Telegram API accept failed")?;
                        let request = read_fake_telegram_request(&mut stream).await?;
                        let is_final_main_edit = request.method == "editMessageText"
                            && request.body["message_id"].as_i64() == Some(1_000)
                            && request.body["text"].as_str().is_some_and(|text| text.starts_with(final_prefix));
                        let fail_this_edit = is_final_main_edit
                            && !failed_once_for_server.swap(true, Ordering::SeqCst);
                        let response = if fail_this_edit {
                            let payload = serde_json::json!({
                                "ok": false,
                                "error_code": 500,
                                "description": "simulated transient final edit failure"
                            })
                            .to_string();
                            format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
                                payload.len()
                            )
                        } else {
                            fake_telegram_response(&request, &mut next_message_id, Vec::new())
                        };
                        stream
                            .write_all(response.as_bytes())
                            .await
                            .context("fake Telegram API response write failed")?;
                        let _ = requests_tx.send(request);
                    }
                }
            }
        });
        let token = AppConfig::for_test().telegram.token;
        (
            TelegramClient::with_test_api_base_url(token, format!("http://{address}")),
            requests_rx,
            shutdown_tx,
            server,
            failed_once,
        )
    }

    async fn spawn_fake_telegram_api_with_two_terminal_entry_edit_failures() -> (
        TelegramClient,
        mpsc::UnboundedReceiver<FakeTelegramRequest>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake Telegram API should bind localhost");
        let address = listener
            .local_addr()
            .expect("fake Telegram API should expose its address");
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let terminal_edit_attempts = Arc::new(AtomicUsize::new(0));
        let terminal_edit_attempts_for_server = Arc::clone(&terminal_edit_attempts);
        let server = tokio::spawn(async move {
            let mut next_message_id = 1_000_i64;
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => return Ok(()),
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.context("fake Telegram API accept failed")?;
                        let request = read_fake_telegram_request(&mut stream).await?;
                        let is_terminal_entry_edit = request.method == "editMessageText"
                            && request.body["message_id"].as_i64() == Some(1_001)
                            && request.body["text"]
                                .as_str()
                                .is_some_and(|text| text.contains("Downloaded, published, and verified."));
                        let fail_this_edit = is_terminal_entry_edit
                            && terminal_edit_attempts_for_server.fetch_add(1, Ordering::SeqCst) < 2;
                        let response = if fail_this_edit {
                            let payload = serde_json::json!({
                                "ok": false,
                                "error_code": 500,
                                "description": "simulated transient terminal entry edit failure"
                            })
                            .to_string();
                            format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
                                payload.len()
                            )
                        } else {
                            fake_telegram_response(&request, &mut next_message_id, Vec::new())
                        };
                        stream
                            .write_all(response.as_bytes())
                            .await
                            .context("fake Telegram API response write failed")?;
                        let _ = requests_tx.send(request);
                    }
                }
            }
        });
        let token = AppConfig::for_test().telegram.token;
        (
            TelegramClient::with_test_api_base_url(token, format!("http://{address}")),
            requests_rx,
            shutdown_tx,
            server,
            terminal_edit_attempts,
        )
    }

    async fn spawn_fake_telegram_api_with_blocked_failed_terminal_entry_edit() -> (
        TelegramClient,
        mpsc::UnboundedReceiver<(FakeTelegramRequest, oneshot::Sender<String>)>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
        Arc<AtomicUsize>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake Telegram API should bind localhost");
        let address = listener
            .local_addr()
            .expect("fake Telegram API should expose its address");
        let (terminal_tx, terminal_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let send_attempts = Arc::new(AtomicUsize::new(0));
        let send_attempts_for_server = Arc::clone(&send_attempts);
        let server = tokio::spawn(async move {
            let mut next_message_id = 1_000_i64;
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => return Ok(()),
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.context("fake Telegram API accept failed")?;
                        let request = read_fake_telegram_request(&mut stream).await?;
                        if request.method == "sendMessage" {
                            send_attempts_for_server.fetch_add(1, Ordering::SeqCst);
                        }
                        let is_failed_terminal_edit = request.method == "editMessageText"
                            && request.body["message_id"].as_i64() == Some(1_001)
                            && request.body["text"]
                                .as_str()
                                .is_some_and(|text| text.contains("Downloaded, but collection publication or final verification failed."));
                        let response = if is_failed_terminal_edit {
                            let (response_tx, response_rx) = oneshot::channel();
                            if terminal_tx.send((request, response_tx)).is_err() {
                                return Ok(());
                            }
                            response_rx
                                .await
                                .context("test should release the terminal edit response")?
                        } else {
                            fake_telegram_response(&request, &mut next_message_id, Vec::new())
                        };
                        stream
                            .write_all(response.as_bytes())
                            .await
                            .context("fake Telegram API response write failed")?;
                    }
                }
            }
        });
        let token = AppConfig::for_test().telegram.token;
        (
            TelegramClient::with_test_api_base_url(token, format!("http://{address}")),
            terminal_rx,
            shutdown_tx,
            server,
            send_attempts,
        )
    }

    async fn spawn_controllable_fake_telegram_api() -> (
        TelegramClient,
        mpsc::UnboundedReceiver<(FakeTelegramRequest, oneshot::Sender<String>)>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake Telegram API should bind localhost");
        let address = listener
            .local_addr()
            .expect("fake Telegram API should expose its address");
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => return Ok(()),
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.context("fake Telegram API accept failed")?;
                        let requests_tx = requests_tx.clone();
                        tokio::spawn(async move {
                            let Ok(request) = read_fake_telegram_request(&mut stream).await else {
                                return;
                            };
                            let (response_tx, response_rx) = oneshot::channel::<String>();
                            if requests_tx.send((request, response_tx)).is_err() {
                                return;
                            }
                            if let Ok(response) = response_rx.await {
                                let _ = stream.write_all(response.as_bytes()).await;
                            }
                        });
                    }
                }
            }
        });
        let token = AppConfig::for_test().telegram.token;
        (
            TelegramClient::with_test_api_base_url(token, format!("http://{address}")),
            requests_rx,
            shutdown_tx,
            server,
        )
    }

    async fn receive_controllable_request(
        requests: &mut mpsc::UnboundedReceiver<(FakeTelegramRequest, oneshot::Sender<String>)>,
    ) -> (FakeTelegramRequest, oneshot::Sender<String>) {
        tokio_timeout(Duration::from_secs(5), requests.recv())
            .await
            .expect("controlled fake Telegram request should arrive")
            .expect("controlled fake Telegram request channel should remain open")
    }

    fn controlled_telegram_response(payload: serde_json::Value) -> String {
        let payload = payload.to_string();
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
            payload.len()
        )
    }

    fn controlled_send_message_response(message_id: i64, chat_id: i64) -> String {
        controlled_telegram_response(serde_json::json!({
            "ok": true,
            "result": {
                "message_id": message_id,
                "chat": { "id": chat_id, "type": "private" }
            }
        }))
    }

    fn controlled_success_response() -> String {
        controlled_telegram_response(serde_json::json!({ "ok": true, "result": true }))
    }

    fn controlled_failure_response() -> String {
        controlled_telegram_response(serde_json::json!({
            "ok": false,
            "error_code": 500,
            "description": "simulated delayed prompt delivery failure"
        }))
    }

    fn inline_keyboard_callback_data(request: &FakeTelegramRequest, suffix: &str) -> String {
        request.body["reply_markup"]["inline_keyboard"]
            .as_array()
            .expect("prompt should have an inline keyboard")
            .iter()
            .flat_map(|row| row.as_array().into_iter().flatten())
            .filter_map(|button| button["callback_data"].as_str())
            .find(|data| data.ends_with(suffix))
            .expect("prompt keyboard should contain the requested callback action")
            .to_string()
    }

    pub(super) async fn receive_request(
        requests: &mut mpsc::UnboundedReceiver<FakeTelegramRequest>,
    ) -> FakeTelegramRequest {
        tokio_timeout(Duration::from_secs(5), requests.recv())
            .await
            .expect("fake Telegram request should arrive")
            .expect("fake Telegram request channel should remain open")
    }

    #[tokio::test]
    async fn collection_e2e_sends_one_main_and_37_entry_messages() {
        let (telegram, mut requests, _updates, shutdown, server) = spawn_fake_telegram_api().await;
        let (queue_root, _config, queue) = create_running_collection_queue(
            "collection-37-entry-messages",
            "task-collection-37-entry-messages",
            CHAT_ID,
            37,
        );
        let task_id = "task-collection-37-entry-messages";
        let manifest = test_collection_manifest(37);
        let (progress, progress_rx) = job_progress_channel();
        let mut progress_task = tokio::spawn(forward_progress(
            telegram.clone(),
            Arc::clone(&queue),
            progress_context(task_id, CHAT_ID, 37, 0, None),
            progress_rx,
        ));

        progress.send_lifecycle(JobProgressLifecycleEvent::Resolved {
            manifest: manifest.clone(),
            snapshot: test_collection_snapshot(&manifest, 0, None),
        });
        let mut lifecycle_requests: Vec<FakeTelegramRequest> = Vec::new();
        let mut completed = 0;
        for entry in manifest
            .entries
            .iter()
            .filter(|entry| entry.status == BilibiliCollectionEntryStatus::Queued)
        {
            let entry = test_collection_entry_progress(&manifest, entry.index);
            progress.send_lifecycle(JobProgressLifecycleEvent::EntryStarted {
                entry: entry.clone(),
                snapshot: test_collection_snapshot(&manifest, completed, Some(entry.clone())),
            });
            let entry_message_id = loop {
                let request = receive_request(&mut requests).await;
                let send_message_id = if request.method == "sendMessage" {
                    Some(
                        1_000_i64
                            + lifecycle_requests
                                .iter()
                                .filter(|request| request.method == "sendMessage")
                                .count() as i64,
                    )
                } else {
                    None
                };
                let sent_entry_index = request.body["text"].as_str().and_then(|text| {
                    text.lines().find_map(|line| {
                        line.trim()
                            .strip_prefix("Entry: ")?
                            .split_once('/')?
                            .0
                            .parse::<u32>()
                            .ok()
                    })
                });
                lifecycle_requests.push(request);
                if sent_entry_index == Some(entry.index) {
                    break send_message_id
                        .expect("new collection entry status should use sendMessage");
                }
            };
            progress.send_replace(Some(JobProgress {
                message: format!("BBDown-rust: downloading {}", entry.title),
                resolved_summary: None,
                collection: Some(test_collection_snapshot(
                    &manifest,
                    completed,
                    Some(entry.clone()),
                )),
            }));
            loop {
                let request = receive_request(&mut requests).await;
                let is_entry_progress_edit = request.method == "editMessageText"
                    && request.body["message_id"].as_i64() == Some(entry_message_id)
                    && request.body["text"].as_str().is_some_and(|text| {
                        text.contains(&format!("BBDown-rust: downloading Entry {}", entry.index))
                    });
                lifecycle_requests.push(request);
                if is_entry_progress_edit {
                    break;
                }
            }
            let completed_entries = completed + 1;
            progress.send_lifecycle(JobProgressLifecycleEvent::EntryCompleted {
                entry: entry.clone(),
                file_count: 1,
                snapshot: test_collection_snapshot(&manifest, completed_entries, None),
            });
            loop {
                let request = receive_request(&mut requests).await;
                let is_entry_completion_edit = request.method == "editMessageText"
                    && request.body["message_id"].as_i64() == Some(entry_message_id)
                    && request.body["text"].as_str().is_some_and(|text| {
                        text.contains(
                            "Download complete; awaiting final publication and verification.",
                        )
                    });
                lifecycle_requests.push(request);
                if is_entry_completion_edit {
                    break;
                }
            }
            completed = completed_entries;
        }
        progress.send_lifecycle(JobProgressLifecycleEvent::Completed {
            snapshot: test_collection_snapshot(&manifest, completed, None),
        });
        drop(progress);
        let delivery = match tokio_timeout(Duration::from_secs(30), &mut progress_task).await {
            Ok(result) => result
                .expect("collection progress task should not panic")
                .expect("resolved collection should produce a delivery tracker"),
            Err(_) => {
                progress_task.abort();
                let _ = progress_task.await;
                let requests = take_fake_telegram_requests(&mut requests);
                let sends = requests
                    .iter()
                    .filter(|request| request.method == "sendMessage")
                    .count();
                let edits = requests
                    .iter()
                    .filter(|request| request.method == "editMessageText")
                    .count();
                let last_request = requests
                    .last()
                    .map(|request| {
                        format!(
                            "{} message_id={}",
                            request.method,
                            request.body["message_id"]
                                .as_i64()
                                .map_or_else(|| "none".to_string(), |id| id.to_string())
                        )
                    })
                    .unwrap_or_else(|| "none".to_string());
                drop(queue);
                stop_fake_telegram_api(shutdown, server).await;
                let _ = fs::remove_dir_all(queue_root);
                panic!(
                    "37-entry forwarding timed out after {sends} sends and {edits} edits; last request: {last_request}"
                );
            }
        };

        assert_eq!(delivery.entries.len(), 37);
        lifecycle_requests.extend(take_fake_telegram_requests(&mut requests));
        let sends = lifecycle_requests
            .iter()
            .filter(|request| request.method == "sendMessage")
            .collect::<Vec<_>>();
        assert_eq!(sends.len(), 38, "one main message plus 37 entry messages");
        assert!(sends.iter().all(|request| {
            request.body["text"]
                .as_str()
                .is_some_and(|text| !text.starts_with("Collection details for job #"))
        }));

        let entry_message_ids = sent_entry_message_ids(&lifecycle_requests);
        assert_eq!(entry_message_ids.len(), 37);
        let mut entry_indexes = entry_message_ids.keys().copied().collect::<Vec<_>>();
        entry_indexes.sort_unstable();
        assert_eq!(entry_indexes, (1..=37).collect::<Vec<_>>());

        let sent_ids = (1_000_i64..1_000_i64 + sends.len() as i64).collect::<Vec<_>>();
        let edits = lifecycle_requests
            .iter()
            .filter(|request| request.method == "editMessageText")
            .collect::<Vec<_>>();
        assert!(
            !edits.is_empty(),
            "progress and final states should edit messages"
        );
        assert!(edits.iter().all(|request| {
            request.body["message_id"]
                .as_i64()
                .is_some_and(|message_id| sent_ids.contains(&message_id))
        }));
        for index in 1..=36 {
            let message_id = entry_message_ids[&index];
            let entry_edits = edits
                .iter()
                .filter(|request| request.body["message_id"].as_i64() == Some(message_id))
                .collect::<Vec<_>>();
            assert!(
                entry_edits.iter().any(|request| {
                    request.body["text"].as_str().is_some_and(|text| {
                        text.contains(&format!("BBDown-rust: downloading Entry {index}"))
                    })
                }),
                "entry {index} progress should edit its original message"
            );
            assert!(
                entry_edits.iter().any(|request| {
                    request.body["text"].as_str().is_some_and(|text| {
                        text.contains(
                            "Download complete; awaiting final publication and verification.",
                        )
                    })
                }),
                "entry {index} completion should edit the same original message"
            );
        }
        assert_eq!(
            queue
                .get(task_id)
                .expect("collection record should load")
                .expect("collection task should remain persisted")
                .media_entries_completed,
            37,
            "the 36 completed entries and one already-present entry should be counted"
        );

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn collection_resume_advances_generation_and_ignores_stale_events() {
        let (telegram, mut requests, _updates, shutdown, server) = spawn_fake_telegram_api().await;
        let task_id = "task-collection-generation-resume";
        let chat_id = 456_123_789;
        let (queue_root, config, queue) =
            create_running_collection_queue("collection-generation-resume", task_id, chat_id, 41);
        let manifest = queued_only_manifest();
        let first_delivery = track_single_entry_collection(
            &telegram,
            Arc::clone(&queue),
            task_id,
            chat_id,
            41,
            0,
            manifest.clone(),
            true,
            false,
            false,
            &mut requests,
        )
        .await
        .expect("first generation should create collection delivery state");
        assert!(first_delivery.entries.contains_key(&1));
        let first_requests = take_fake_telegram_requests(&mut requests);
        let first_main_message_id = queue
            .get(task_id)
            .expect("first generation task should load")
            .expect("first generation task should exist")
            .status_message_id
            .expect("first queued overview should persist its Telegram message id");
        assert!(first_requests.iter().any(|request| {
            request.method == "sendMessage"
                && request.body["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("Entry: 1/1"))
        }));

        drop(queue);
        let queue = Arc::new(QueueManager::open(&config).expect("queue should reopen"));
        let interrupted = queue
            .get(task_id)
            .expect("reopened task should load")
            .expect("reopened task should exist");
        assert_eq!(interrupted.status, TaskStatus::Interrupted);
        assert_eq!(interrupted.status_message_id, Some(first_main_message_id));
        let resumed = queue
            .claim_resume(task_id, false)
            .expect("interrupted collection should be claimable")
            .expect("interrupted collection should resume");
        assert_eq!(resumed.generation, 1);
        assert_eq!(resumed.status_message_id, None);
        queue
            .set_status(task_id, TaskStatus::Queued, None)
            .expect("resumed collection should enter the queue");
        assert!(
            queue
                .begin_run_if_generation(task_id, 1)
                .expect("generation one should begin")
                .is_some()
        );

        let counters_before_stale_event = queue
            .get(task_id)
            .expect("resumed task should load")
            .expect("resumed task should exist");
        let _stale_delivery = track_single_entry_collection(
            &telegram,
            Arc::clone(&queue),
            task_id,
            chat_id,
            41,
            0,
            manifest.clone(),
            true,
            true,
            false,
            &mut requests,
        )
        .await;
        assert!(
            take_fake_telegram_requests(&mut requests).is_empty(),
            "stale events must not send or edit messages into the resumed generation"
        );
        let counters_after_stale_event = queue
            .get(task_id)
            .expect("resumed task should reload")
            .expect("resumed task should remain persisted");
        assert_eq!(
            counters_after_stale_event.media_entries_completed,
            counters_before_stale_event.media_entries_completed
        );
        assert_eq!(counters_after_stale_event.generation, 1);

        let new_delivery = track_single_entry_collection(
            &telegram,
            Arc::clone(&queue),
            task_id,
            chat_id,
            41,
            1,
            manifest,
            true,
            true,
            false,
            &mut requests,
        )
        .await
        .expect("new generation should create collection delivery state");
        assert!(new_delivery.entries.contains_key(&1));
        let new_requests = take_fake_telegram_requests(&mut requests);
        let new_sends = new_requests
            .iter()
            .filter(|request| request.method == "sendMessage")
            .collect::<Vec<_>>();
        assert_eq!(
            new_sends.len(),
            2,
            "a resumed generation should send a new main and video status"
        );
        let new_message_ids = (1_000_i64
            + first_requests
                .iter()
                .filter(|request| request.method == "sendMessage")
                .count() as i64
            ..1_000_i64
                + first_requests
                    .iter()
                    .filter(|request| request.method == "sendMessage")
                    .count() as i64
                + new_sends.len() as i64)
            .collect::<Vec<_>>();
        assert!(!new_message_ids.contains(&first_main_message_id));
        assert!(
            new_message_ids
                .iter()
                .all(|message_id| *message_id != 1_001)
        );
        assert!(
            new_requests
                .iter()
                .filter(|request| request.method == "editMessageText")
                .all(|request| {
                    request.body["message_id"]
                        .as_i64()
                        .is_some_and(|message_id| new_message_ids.contains(&message_id))
                })
        );
        let resumed_task = queue
            .get(task_id)
            .expect("resumed task should load after progress")
            .expect("resumed task should exist after progress");
        assert_eq!(resumed_task.generation, 1);
        assert_eq!(resumed_task.status_message_id, Some(new_message_ids[0]));

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn collection_failure_cancel_and_verified_results_edit_existing_messages() {
        let (telegram, mut requests, _updates, shutdown, server) = spawn_fake_telegram_api().await;

        let failure_id = "task-collection-failure-final";
        let (failure_root, _failure_config, failure_queue) =
            create_running_collection_queue("collection-failure-final", failure_id, 610_001, 51);
        let failed_delivery = track_single_entry_collection(
            &telegram,
            Arc::clone(&failure_queue),
            failure_id,
            610_001,
            51,
            0,
            queued_only_manifest(),
            true,
            false,
            true,
            &mut requests,
        )
        .await
        .expect("failed collection should retain delivery state");
        failure_queue
            .fail_if_generation(failure_id, 0, "simulated collection failure".to_string())
            .expect("failed state should persist")
            .expect("current generation should fail");
        finalize_collection_messages(
            finalization_context(&telegram, &failure_queue, failure_id, 0, 610_001, 51),
            failed_delivery,
            "Failed job #51: Bilibili download\nsimulated collection failure".to_string(),
            CollectionFinalOutcome::Failed,
        )
        .await;
        let failure_requests = take_fake_telegram_requests(&mut requests);
        let failure_sends = failure_requests
            .iter()
            .filter(|request| request.method == "sendMessage")
            .count();
        assert_eq!(
            failure_sends, 2,
            "failure should not create replacement messages"
        );
        let failure_send_ids = sent_entry_message_ids(&failure_requests);
        assert_eq!(failure_send_ids.len(), 1);
        let failure_message_ids = (1_000_i64..1_002).collect::<Vec<_>>();
        assert!(
            failure_requests
                .iter()
                .filter(|request| request.method == "editMessageText")
                .all(|request| {
                    request.body["message_id"]
                        .as_i64()
                        .is_some_and(|message_id| failure_message_ids.contains(&message_id))
                })
        );
        assert!(failure_requests.iter().any(|request| {
            request.method == "editMessageText"
                && request.body["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("Failed job #51"))
        }));
        assert!(failure_requests.iter().any(|request| {
            request.method == "editMessageText"
                && request.body["message_id"].as_i64() == Some(1_001)
                && request.body["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("Failed"))
        }));
        drop(failure_queue);
        let _ = fs::remove_dir_all(failure_root);

        let cancel_id = "task-collection-cancel-before-entry";
        let (cancel_root, _cancel_config, cancel_queue) = create_running_collection_queue(
            "collection-cancel-before-entry",
            cancel_id,
            610_002,
            52,
        );
        let cancelled_delivery = track_single_entry_collection(
            &telegram,
            Arc::clone(&cancel_queue),
            cancel_id,
            610_002,
            52,
            0,
            queued_only_manifest(),
            false,
            false,
            true,
            &mut requests,
        )
        .await
        .expect("resolved collection should keep its main message tracker");
        cancel_queue
            .cancel_if_current(cancel_id, 610_002, &[TaskStatus::Running])
            .expect("collection should cancel")
            .expect("running collection should be canceled");
        finalize_collection_messages(
            finalization_context(&telegram, &cancel_queue, cancel_id, 0, 610_002, 52),
            cancelled_delivery,
            "Canceled job #52: Bilibili download".to_string(),
            CollectionFinalOutcome::Cancelled,
        )
        .await;
        let cancel_requests = take_fake_telegram_requests(&mut requests);
        assert_eq!(
            cancel_requests
                .iter()
                .filter(|request| request.method == "sendMessage")
                .count(),
            1,
            "canceling before the first entry starts should send no video status"
        );
        let cancel_main_id = 1_002_i64;
        assert!(
            cancel_requests
                .iter()
                .filter(|request| request.method == "editMessageText")
                .all(|request| { request.body["message_id"].as_i64() == Some(cancel_main_id) })
        );
        assert!(cancel_requests.iter().any(|request| {
            request.method == "editMessageText"
                && request.body["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("Canceled job #52"))
        }));
        drop(cancel_queue);
        let _ = fs::remove_dir_all(cancel_root);

        let verified_id = "task-collection-verified-final";
        let (verified_root, verified_config, verified_queue) =
            create_running_collection_queue("collection-verified-final", verified_id, 610_003, 53);
        let verified_delivery = track_single_entry_collection(
            &telegram,
            Arc::clone(&verified_queue),
            verified_id,
            610_003,
            53,
            0,
            queued_only_manifest(),
            true,
            true,
            false,
            &mut requests,
        )
        .await
        .expect("verified collection should keep delivery state");
        assert!(
            verified_queue
                .begin_verification_if_generation(verified_id, 0)
                .expect("verification should begin")
                .is_some()
        );
        let verified_output = verified_config.downloads.video_dir.join("verified.mp4");
        assert!(
            verified_queue
                .complete_if_generation(
                    verified_id,
                    0,
                    verified_output.display().to_string(),
                    &[],
                    BTreeMap::new()
                )
                .expect("mock verification should complete")
                .is_some()
        );
        finalize_collection_messages(
            finalization_context(&telegram, &verified_queue, verified_id, 0, 610_003, 53),
            verified_delivery,
            "Finished job #53: Bilibili download\nSaved: mock output".to_string(),
            CollectionFinalOutcome::Verified,
        )
        .await;
        let verified_requests = take_fake_telegram_requests(&mut requests);
        assert_eq!(
            verified_requests
                .iter()
                .filter(|request| request.method == "sendMessage")
                .count(),
            2,
            "verification should edit existing main and video messages"
        );
        assert!(verified_requests.iter().any(|request| {
            request.method == "editMessageText"
                && request.body["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("Finished job #53"))
        }));
        assert!(verified_requests.iter().any(|request| {
            request.method == "editMessageText"
                && request.body["message_id"].as_i64() == Some(1_004)
                && request.body["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("verified") || text.contains("Verified"))
        }));
        assert!(
            verified_requests
                .iter()
                .filter(|request| request.method == "editMessageText")
                .all(|request| {
                    request.body["message_id"]
                        .as_i64()
                        .is_some_and(|message_id| [1_003, 1_004].contains(&message_id))
                })
        );
        drop(verified_queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(verified_root);
    }

    #[tokio::test]
    async fn temporary_collection_edit_failure_retries_the_same_message_id() {
        let (telegram, mut requests, shutdown, server, failed_once) =
            spawn_fake_telegram_api_with_one_entry_edit_failure().await;
        let (queue_root, _config, queue) = create_running_collection_queue(
            "collection-edit-retry-same-id",
            "task-collection-edit-retry-same-id",
            CHAT_ID,
            61,
        );
        let task_id = "task-collection-edit-retry-same-id";
        let manifest = queued_only_manifest();
        let (progress, progress_rx) = job_progress_channel();
        let progress_task = tokio::spawn(forward_progress(
            telegram.clone(),
            Arc::clone(&queue),
            progress_context(task_id, CHAT_ID, 61, 0, None),
            progress_rx,
        ));
        progress.send_lifecycle(JobProgressLifecycleEvent::Resolved {
            manifest: manifest.clone(),
            snapshot: test_collection_snapshot(&manifest, 0, None),
        });
        let entry = test_collection_entry_progress(&manifest, 1);
        progress.send_lifecycle(JobProgressLifecycleEvent::EntryStarted {
            entry: entry.clone(),
            snapshot: test_collection_snapshot(&manifest, 0, Some(entry.clone())),
        });
        let mut observed_requests = Vec::new();
        loop {
            let request = receive_request(&mut requests).await;
            let is_entry_send = request.method == "sendMessage"
                && request.body["text"]
                    .as_str()
                    .is_some_and(|text| text.lines().any(|line| line.trim() == "Entry: 1/1"));
            observed_requests.push(request);
            if is_entry_send {
                break;
            }
        }
        progress.send_replace(Some(collection_progress(&manifest)));

        loop {
            let request = receive_request(&mut requests).await;
            let is_entry_edit = request.method == "editMessageText"
                && request.body["message_id"].as_i64() == Some(1_001);
            observed_requests.push(request);
            if is_entry_edit {
                break;
            }
        }
        assert!(
            failed_once.load(Ordering::SeqCst),
            "the mock should fail one entry edit"
        );

        progress.send_replace(Some(JobProgress {
            message: "BBDown-rust: processing collection entry".to_string(),
            resolved_summary: None,
            collection: Some(test_collection_snapshot(&manifest, 0, Some(entry.clone()))),
        }));
        loop {
            let request = receive_request(&mut requests).await;
            let is_retry = request.method == "editMessageText"
                && request.body["message_id"].as_i64() == Some(1_001);
            observed_requests.push(request);
            if is_retry {
                break;
            }
        }

        progress.send_lifecycle(JobProgressLifecycleEvent::EntryCompleted {
            entry,
            file_count: 1,
            snapshot: test_collection_snapshot(&manifest, 1, None),
        });
        progress.send_lifecycle(JobProgressLifecycleEvent::Completed {
            snapshot: test_collection_snapshot(&manifest, 1, None),
        });
        drop(progress);
        let delivery = tokio_timeout(Duration::from_secs(5), progress_task)
            .await
            .expect("collection retry tracker should finish")
            .expect("collection progress task should not panic")
            .expect("resolved collection should retain delivery state");
        assert!(delivery.entries.contains_key(&1));

        observed_requests.extend(take_fake_telegram_requests(&mut requests));
        assert_eq!(
            observed_requests
                .iter()
                .filter(|request| request.method == "sendMessage")
                .count(),
            2,
            "a failed edit must not create a duplicate message"
        );
        let entry_edits = observed_requests
            .iter()
            .filter(|request| {
                request.method == "editMessageText"
                    && request.body["message_id"].as_i64() == Some(1_001)
            })
            .count();
        assert!(
            entry_edits >= 3,
            "the same entry message should receive retry and final edits"
        );
        assert_eq!(
            observed_requests
                .iter()
                .filter(|request| request.method == "editMessageText")
                .filter_map(|request| request.body["message_id"].as_i64())
                .filter(|message_id| *message_id == 1_001)
                .collect::<Vec<_>>(),
            vec![1_001; entry_edits],
            "every entry edit should keep targeting the original message id"
        );

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn queueing_confirmed_collection_edits_selection_prompt_and_cancels_blocked_worker() {
        let (telegram, mut requests, _updates, shutdown, server) = spawn_fake_telegram_api().await;
        let chat_id = 654_321_987;
        let selection_prompt_id = telegram
            .send_message(
                chat_id,
                "Choose which collection entries to download.".to_string(),
            )
            .await
            .expect("selection prompt should send");
        let prompt_requests = take_fake_telegram_requests(&mut requests);
        assert_eq!(prompt_requests.len(), 1);
        assert_eq!(prompt_requests[0].method, "sendMessage");

        let task_id = "task-confirmed-collection-queue-transition";
        let job_id = 71;
        let job = JobRequest::Bilibili {
            url: "https://space.bilibili.com/210798/channel/collectiondetail?sid=167822"
                .to_string(),
            selection: Some(BilibiliSelection::All),
        };
        assert!(is_confirmed_bilibili_ugc_collection_job(&job));
        let (queue_root, config, queue) = create_collection_queue_with_job(
            "confirmed-collection-queue-transition",
            task_id,
            chat_id,
            job_id,
            job.clone(),
        );
        queue
            .set_status(task_id, TaskStatus::Queued, None)
            .expect("confirmed collection should persist as queued");
        queue
            .set_status_message_id(task_id, selection_prompt_id)
            .expect("selection prompt should be associated with the task");

        queue_queued_task(
            BotContext {
                telegram: telegram.clone(),
                config: Arc::new(config),
                job_dispatch: JobDispatch {
                    download_semaphore: Arc::new(Semaphore::new(0)),
                    duplicate_scan_semaphore: Arc::new(Semaphore::new(1)),
                },
                next_job_id: Arc::new(AtomicU64::new(job_id + 1)),
                queue: Arc::clone(&queue),
                queue_start_retry_delay: Duration::ZERO,
            },
            chat_id,
            job_id,
            task_id.to_string(),
            job,
            JobRunMode::Direct,
            None,
        )
        .await;

        let queued_requests = take_fake_telegram_requests(&mut requests);
        assert_eq!(
            queued_requests.len(),
            1,
            "queue transition should make one Telegram call"
        );
        assert_eq!(queued_requests[0].method, "editMessageText");
        assert_eq!(
            queued_requests[0].body["message_id"].as_i64(),
            Some(selection_prompt_id),
            "the queued collection should reuse the selection prompt"
        );
        assert!(
            queued_requests[0].body["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("Queued job #71"))
        );
        let queued_record = queue
            .get(task_id)
            .expect("queued task should load")
            .expect("queued task should remain persisted");
        assert_eq!(queued_record.status_message_id, Some(selection_prompt_id));

        queue
            .cancel_for_chat(task_id, chat_id)
            .expect("blocked collection should cancel")
            .expect("queued collection should be cancellable");
        let cancelled_request = receive_request(&mut requests).await;
        assert_eq!(cancelled_request.method, "editMessageText");
        assert_eq!(
            cancelled_request.body["message_id"].as_i64(),
            Some(selection_prompt_id),
            "cancellation should also edit the same collection main message"
        );
        assert!(
            cancelled_request.body["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("Canceled job #71"))
        );
        let cancelled_record = queue
            .get(task_id)
            .expect("cancelled task should load")
            .expect("cancelled task should remain persisted");
        assert_eq!(cancelled_record.status, TaskStatus::Cancelled);
        assert_eq!(
            cancelled_record.status_message_id,
            Some(selection_prompt_id)
        );

        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn collection_final_main_edit_retries_same_id_without_duplicate_send() {
        let final_prefix = "Finished job #62";
        let (telegram, mut requests, shutdown, server, failed_once) =
            spawn_fake_telegram_api_with_one_final_main_edit_failure(final_prefix).await;
        let task_id = "task-collection-final-main-edit-retry";
        let (queue_root, config, queue) = create_running_collection_queue(
            "collection-final-main-edit-retry",
            task_id,
            CHAT_ID,
            62,
        );
        let delivery = track_single_entry_collection(
            &telegram,
            Arc::clone(&queue),
            task_id,
            CHAT_ID,
            62,
            0,
            queued_only_manifest(),
            true,
            true,
            false,
            &mut requests,
        )
        .await
        .expect("resolved collection should retain message delivery state");
        assert!(
            queue
                .begin_verification_if_generation(task_id, 0)
                .expect("verification should begin")
                .is_some()
        );
        assert!(
            queue
                .complete_if_generation(
                    task_id,
                    0,
                    config
                        .downloads
                        .video_dir
                        .join("mock-final-output")
                        .display()
                        .to_string(),
                    &[],
                    BTreeMap::new(),
                )
                .expect("mock verification should complete")
                .is_some()
        );

        finalize_collection_messages(
            finalization_context(&telegram, &queue, task_id, 0, CHAT_ID, 62),
            delivery,
            "Finished job #62: Bilibili download\nSaved: mock output".to_string(),
            CollectionFinalOutcome::Verified,
        )
        .await;

        assert!(
            failed_once.load(Ordering::SeqCst),
            "the fake Telegram API should fail the first final main edit"
        );
        let observed = take_fake_telegram_requests(&mut requests);
        let sends = observed
            .iter()
            .filter(|request| request.method == "sendMessage")
            .count();
        assert_eq!(sends, 2, "a failed final edit must not send a replacement");
        let final_edits = observed
            .iter()
            .filter(|request| {
                request.method == "editMessageText"
                    && request.body["text"]
                        .as_str()
                        .is_some_and(|text| text.starts_with(final_prefix))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            final_edits.len(),
            2,
            "the failed final edit should receive one bounded retry"
        );
        assert!(
            final_edits
                .iter()
                .all(|request| request.body["message_id"].as_i64() == Some(1_000)),
            "every final edit attempt should target the original main message"
        );
        assert_eq!(
            queue
                .get(task_id)
                .expect("completed task should load")
                .expect("completed task should remain")
                .status_message_id,
            Some(1_000)
        );

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn verified_terminal_entry_edit_retries_same_id_without_duplicate_send_e2e() {
        let (telegram, mut requests, shutdown, server, terminal_edit_attempts) =
            spawn_fake_telegram_api_with_two_terminal_entry_edit_failures().await;
        let task_id = "task-collection-terminal-entry-edit-retry";
        let (queue_root, config, queue) = create_running_collection_queue(
            "collection-terminal-entry-edit-retry",
            task_id,
            CHAT_ID,
            82,
        );
        let delivery = track_single_entry_collection(
            &telegram,
            Arc::clone(&queue),
            task_id,
            CHAT_ID,
            82,
            0,
            queued_only_manifest(),
            true,
            true,
            false,
            &mut requests,
        )
        .await
        .expect("resolved collection should retain terminal entry delivery state");
        assert!(
            queue
                .begin_verification_if_generation(task_id, 0)
                .expect("verification should begin")
                .is_some()
        );
        assert!(
            queue
                .complete_if_generation(
                    task_id,
                    0,
                    config
                        .downloads
                        .video_dir
                        .join("mock-terminal-entry-output")
                        .display()
                        .to_string(),
                    &[],
                    BTreeMap::new(),
                )
                .expect("mock verification should complete")
                .is_some()
        );

        finalize_collection_messages(
            finalization_context(&telegram, &queue, task_id, 0, CHAT_ID, 82),
            delivery,
            "Finished job #82: Bilibili download\nSaved: mock output".to_string(),
            CollectionFinalOutcome::Verified,
        )
        .await;

        assert_eq!(
            terminal_edit_attempts.load(Ordering::SeqCst),
            3,
            "two transient failures should be followed by one successful bounded retry"
        );
        let observed = take_fake_telegram_requests(&mut requests);
        let sends = observed
            .iter()
            .filter(|request| request.method == "sendMessage")
            .count();
        assert_eq!(
            sends, 2,
            "terminal edit failures must not send replacement main or entry messages"
        );
        let terminal_edits = observed
            .iter()
            .filter(|request| {
                request.method == "editMessageText"
                    && request.body["text"]
                        .as_str()
                        .is_some_and(|text| text.contains("Downloaded, published, and verified."))
            })
            .collect::<Vec<_>>();
        assert_eq!(terminal_edits.len(), 3);
        assert!(
            terminal_edits
                .iter()
                .all(|request| request.body["message_id"].as_i64() == Some(1_001))
        );
        assert!(observed.iter().any(|request| {
            request.method == "editMessageText"
                && request.body["message_id"].as_i64() == Some(1_000)
                && request.body["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("Finished job #82"))
        }));
        let completed = queue
            .get(task_id)
            .expect("completed collection should load")
            .expect("completed collection should remain");
        assert_eq!(completed.status, TaskStatus::Completed);

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn failed_terminal_entry_retry_stops_after_generation_changes_e2e() {
        let (telegram, mut terminal_requests, shutdown, server, send_attempts) =
            spawn_fake_telegram_api_with_blocked_failed_terminal_entry_edit().await;
        let task_id = "task-collection-terminal-entry-stale-retry";
        let (queue_root, _config, queue) = create_running_collection_queue(
            "collection-terminal-entry-stale-retry",
            task_id,
            CHAT_ID,
            83,
        );
        let (_unused_sender, mut unused_requests) = mpsc::unbounded_channel();
        let delivery = track_single_entry_collection(
            &telegram,
            Arc::clone(&queue),
            task_id,
            CHAT_ID,
            83,
            0,
            queued_only_manifest(),
            true,
            true,
            false,
            &mut unused_requests,
        )
        .await
        .expect("resolved collection should retain terminal entry delivery state");
        queue
            .fail_if_generation(task_id, 0, "simulated verification failure".to_string())
            .expect("collection failure should persist")
            .expect("generation zero should still be current");

        let finalizer_telegram = telegram.clone();
        let finalizer_queue = Arc::clone(&queue);
        let finalizer_task_id = task_id.to_string();
        let mut finalizer = tokio::spawn(async move {
            finalize_collection_messages(
                finalization_context(
                    &finalizer_telegram,
                    &finalizer_queue,
                    &finalizer_task_id,
                    0,
                    CHAT_ID,
                    83,
                ),
                delivery,
                "Failed job #83: Bilibili download\nsimulated verification failure".to_string(),
                CollectionFinalOutcome::Failed,
            )
            .await;
        });
        let (terminal_edit, terminal_response) =
            receive_controllable_request(&mut terminal_requests).await;
        assert_eq!(terminal_edit.method, "editMessageText");
        assert_eq!(terminal_edit.body["message_id"].as_i64(), Some(1_001));
        assert!(terminal_edit.body["text"].as_str().is_some_and(|text| {
            text.contains("Downloaded, but collection publication or final verification failed.")
        }));

        let resumed = queue
            .claim_resume(task_id, true)
            .expect("failed collection should be retryable")
            .expect("retry should create a new generation during the old terminal edit");
        assert_eq!(resumed.generation, 1);
        terminal_response
            .send(controlled_failure_response())
            .expect("old-generation terminal edit should receive its transient failure");

        let mut unexpected_retry = false;
        match tokio_timeout(Duration::from_secs(3), &mut finalizer).await {
            Ok(result) => result.expect("old-generation finalizer should finish"),
            Err(_) => {
                if let Ok(Some((_request, response))) =
                    tokio_timeout(Duration::from_secs(1), terminal_requests.recv()).await
                {
                    unexpected_retry = true;
                    let _ = response.send(controlled_success_response());
                    tokio_timeout(Duration::from_secs(3), &mut finalizer)
                        .await
                        .expect("finalizer should finish after releasing an unexpected retry")
                        .expect("old-generation finalizer should not panic");
                } else {
                    finalizer.abort();
                    let _ = finalizer.await;
                    panic!("old-generation finalizer timed out without a retry request");
                }
            }
        }
        assert!(
            !unexpected_retry && terminal_requests.try_recv().is_err(),
            "generation change during a failed terminal edit must stop further attempts"
        );
        assert_eq!(
            send_attempts.load(Ordering::SeqCst),
            2,
            "stale terminal edit handling must not send a replacement message"
        );
        let current = queue
            .get(task_id)
            .expect("resumed collection should load")
            .expect("resumed collection should remain");
        assert_eq!(current.generation, 1);
        assert_eq!(current.status, TaskStatus::Preparing);
        assert_eq!(current.status_message_id, None);

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn all_already_present_collection_edits_main_details_and_every_video_status() {
        let (telegram, mut requests, _updates, shutdown, server) = spawn_fake_telegram_api().await;
        let task_id = "task-collection-all-already-present";
        let (queue_root, config, queue) =
            create_running_collection_queue("collection-all-already-present", task_id, CHAT_ID, 63);
        let manifest = all_already_present_manifest(7);
        let (progress, progress_rx) = job_progress_channel();
        let progress_task = tokio::spawn(forward_progress(
            telegram.clone(),
            Arc::clone(&queue),
            progress_context(task_id, CHAT_ID, 63, 0, None),
            progress_rx,
        ));
        progress.send_lifecycle(JobProgressLifecycleEvent::Resolved {
            snapshot: test_collection_snapshot(&manifest, 0, None),
            manifest: manifest.clone(),
        });
        progress.send_lifecycle(JobProgressLifecycleEvent::Completed {
            snapshot: test_collection_snapshot(&manifest, 0, None),
        });
        drop(progress);
        let delivery = tokio_timeout(Duration::from_secs(30), progress_task)
            .await
            .expect("all-present collection progress should finish")
            .expect("collection progress task should not panic")
            .expect("resolved all-present manifest should create a delivery tracker");
        assert_eq!(delivery.entries.len(), 7);
        assert!(delivery.entries.values().all(|entry| {
            entry.state == CollectionEntryDeliveryState::SkippedPendingVerification
        }));
        let main_message_id = delivery
            .main_delivery
            .message_id()
            .expect("resolved collection should have one main message");
        let details_token = delivery
            .details_token
            .expect("multiple entries should register main-message details paging");
        assert!(
            queue
                .begin_verification_if_generation(task_id, 0)
                .expect("verification should begin")
                .is_some()
        );
        assert!(
            queue
                .complete_if_generation(
                    task_id,
                    0,
                    config
                        .downloads
                        .video_dir
                        .join("already-present-output")
                        .display()
                        .to_string(),
                    &[],
                    BTreeMap::new(),
                )
                .expect("all-present collection should complete")
                .is_some()
        );
        finalize_collection_messages(
            finalization_context(&telegram, &queue, task_id, 0, CHAT_ID, 63),
            delivery,
            "Finished job #63: Bilibili download\nAll selected videos are already present."
                .to_string(),
            CollectionFinalOutcome::Verified,
        )
        .await;

        let lifecycle_requests = take_fake_telegram_requests(&mut requests);
        let sends = lifecycle_requests
            .iter()
            .filter(|request| request.method == "sendMessage")
            .collect::<Vec<_>>();
        assert_eq!(
            sends.len(),
            8,
            "all selected existing videos should have one status each plus one main message"
        );
        let main_send = sends
            .iter()
            .find(|request| {
                request.body["text"]
                    .as_str()
                    .is_some_and(|text| !text.contains("Entry: "))
            })
            .expect("one send should be the collection main message");
        assert!(main_send.body["text"].as_str().is_some_and(|text| {
            text.contains("Example collection")
                && text.contains("Sync: 7 total; 7 already present; 0 queued")
        }));
        let entry_ids = sent_entry_message_ids(&lifecycle_requests);
        assert_eq!(entry_ids.len(), 7);
        for index in 1..=7 {
            let message_id = entry_ids
                .get(&index)
                .copied()
                .expect("each selected existing video should have its own message");
            assert!(lifecycle_requests.iter().any(|request| {
                request.method == "editMessageText"
                    && request.body["message_id"].as_i64() == Some(message_id)
                    && request.body["text"].as_str().is_some_and(|text| {
                        text.contains("Existing video verified; download skipped.")
                    })
            }));
        }
        assert!(lifecycle_requests.iter().any(|request| {
            request.method == "editMessageText"
                && request.body["message_id"].as_i64() == Some(main_message_id)
                && request.body["text"].as_str().is_some_and(|text| {
                    text.starts_with("Finished job #63") && text.contains("Entry 1")
                })
        }));

        handle_callback_query(
            bot_context(telegram.clone(), config, Arc::clone(&queue), 64),
            crate::telegram::CallbackQuery {
                id: "all-present-details-page".to_string(),
                data: Some(collection_details_callback_data(details_token, 1)),
                message: Some(crate::telegram::Message {
                    message_id: main_message_id,
                    chat: crate::telegram::Chat {
                        id: CHAT_ID,
                        kind: Some("private".to_string()),
                    },
                    text: None,
                    from: None,
                }),
            },
        )
        .await;
        let details_requests = take_fake_telegram_requests(&mut requests);
        assert_eq!(
            details_requests
                .iter()
                .filter(|request| request.method == "sendMessage")
                .count(),
            0,
            "details paging should edit the collection main message instead of sending another"
        );
        assert!(details_requests.iter().any(|request| {
            request.method == "editMessageText"
                && request.body["message_id"].as_i64() == Some(main_message_id)
                && request.body["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("Entries 6-7 of 7 (page 2/2)"))
        }));

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn selection_prompt_response_after_resume_cannot_replace_new_generation_or_apply_old_callback()
     {
        let (telegram, mut requests, shutdown, server) =
            spawn_controllable_fake_telegram_api().await;
        let task_id = "task-selection-prompt-generation-race";
        let job = JobRequest::Bilibili {
            url: "https://space.bilibili.com/210798/channel/collectiondetail?sid=167822"
                .to_string(),
            selection: None,
        };
        let (queue_root, config, queue) = create_collection_queue_with_job(
            "selection-prompt-generation-race",
            task_id,
            CHAT_ID,
            64,
            job.clone(),
        );
        queue
            .set_status(task_id, TaskStatus::Preparing, None)
            .expect("selection task should enter preparation");
        let old_prompt_context =
            bot_context(telegram.clone(), config.clone(), Arc::clone(&queue), 65);
        let old_task_id = task_id.to_string();
        let old_job = job.clone();
        let old_prompt = tokio::spawn(async move {
            prompt_bilibili_selection(
                &old_prompt_context,
                CHAT_ID,
                64,
                old_task_id,
                0,
                old_job,
                BilibiliSelectionPrompt::UgcCollection,
            )
            .await;
        });
        let (old_request, old_response) = receive_controllable_request(&mut requests).await;
        assert_eq!(old_request.method, "sendMessage");
        let old_callback_data = inline_keyboard_callback_data(&old_request, ":all");

        let resumed = queue
            .claim_resume(task_id, false)
            .expect("awaiting selection should be resumable while Telegram send is in flight")
            .expect("old selection generation should resume");
        assert_eq!(resumed.generation, 1);
        let new_prompt_context =
            bot_context(telegram.clone(), config.clone(), Arc::clone(&queue), 66);
        let new_task_id = task_id.to_string();
        let new_job = job.clone();
        let new_prompt = tokio::spawn(async move {
            prompt_bilibili_selection(
                &new_prompt_context,
                CHAT_ID,
                64,
                new_task_id,
                1,
                new_job,
                BilibiliSelectionPrompt::UgcCollection,
            )
            .await;
        });
        let (new_request, new_response) = receive_controllable_request(&mut requests).await;
        assert_eq!(new_request.method, "sendMessage");
        let new_callback_data = inline_keyboard_callback_data(&new_request, ":all");
        assert_ne!(old_callback_data, new_callback_data);
        new_response
            .send(controlled_send_message_response(1_101, CHAT_ID))
            .expect("new generation response should reach its waiting prompt");
        new_prompt
            .await
            .expect("new generation selection prompt should finish");

        let current = queue
            .get(task_id)
            .expect("current task should load")
            .expect("current task should remain");
        assert_eq!(current.generation, 1);
        assert_eq!(current.status, TaskStatus::AwaitingSelection);
        assert_eq!(current.status_message_id, Some(1_101));

        old_response
            .send(controlled_send_message_response(1_100, CHAT_ID))
            .expect("late old response should reach its waiting prompt");
        old_prompt
            .await
            .expect("late old selection prompt should finish without changing state");
        let after_late_response = queue
            .get(task_id)
            .expect("task after late response should load")
            .expect("task after late response should remain");
        assert_eq!(after_late_response.generation, 1);
        assert_eq!(after_late_response.status, TaskStatus::AwaitingSelection);
        assert_eq!(after_late_response.status_message_id, Some(1_101));

        let stale_callback = tokio::spawn(handle_callback_query(
            bot_context(telegram, config, Arc::clone(&queue), 67),
            crate::telegram::CallbackQuery {
                id: "late-old-selection-callback".to_string(),
                data: Some(old_callback_data),
                message: Some(crate::telegram::Message {
                    message_id: 1_100,
                    chat: crate::telegram::Chat {
                        id: CHAT_ID,
                        kind: Some("private".to_string()),
                    },
                    text: None,
                    from: None,
                }),
            },
        ));
        let (callback_request, callback_response) =
            receive_controllable_request(&mut requests).await;
        assert_eq!(callback_request.method, "answerCallbackQuery");
        assert_eq!(
            callback_request.body["text"].as_str(),
            Some("This choice has expired.")
        );
        callback_response
            .send(controlled_success_response())
            .expect("expired callback acknowledgement should be accepted");
        stale_callback
            .await
            .expect("stale callback handler should finish");
        assert!(
            requests.try_recv().is_err(),
            "a stale callback must not edit or send another message"
        );
        let after_stale_callback = queue
            .get(task_id)
            .expect("task after stale callback should load")
            .expect("task after stale callback should remain");
        assert_eq!(after_stale_callback.generation, 1);
        assert_eq!(after_stale_callback.status, TaskStatus::AwaitingSelection);
        assert_eq!(after_stale_callback.status_message_id, Some(1_101));

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn stale_duplicate_callback_and_delayed_old_prompt_failure_cannot_change_current_generation()
     {
        let (telegram, mut requests, shutdown, server) =
            spawn_controllable_fake_telegram_api().await;
        let task_id = "task-duplicate-prompt-generation-race";
        let job = JobRequest::Bilibili {
            url: "https://www.bilibili.com/video/BV1234567890".to_string(),
            selection: Some(BilibiliSelection::All),
        };
        let duplicate = VideoDuplicate {
            identity: crate::downloader::VideoIdentity {
                provider: crate::downloader::VideoProvider::Bilibili,
                id: "cid-generation-fixture".to_string(),
            },
            existing_videos: vec![PathBuf::from("/fixture/already-downloaded.mp4")],
            overwrite_confirmation: None,
        };
        let (queue_root, config, queue) = create_collection_queue_with_job(
            "duplicate-prompt-generation-race",
            task_id,
            CHAT_ID,
            68,
            job.clone(),
        );
        queue
            .set_status(task_id, TaskStatus::Preparing, None)
            .expect("duplicate task should enter preparation");

        let old_task_id = task_id.to_string();
        let old_job = job.clone();
        let old_duplicate = duplicate.clone();
        let old_telegram = telegram.clone();
        let old_queue = Arc::clone(&queue);
        let old_prompt = tokio::spawn(async move {
            prompt_duplicate_choice(
                &old_telegram,
                &old_queue,
                CHAT_ID,
                68,
                old_task_id,
                0,
                old_job,
                old_duplicate,
                Duration::ZERO,
            )
            .await;
        });
        let (first_request, first_response) = receive_controllable_request(&mut requests).await;
        assert_eq!(first_request.method, "sendMessage");
        let first_cancel_callback = inline_keyboard_callback_data(&first_request, ":cancel");
        first_response
            .send(controlled_send_message_response(2_100, CHAT_ID))
            .expect("first duplicate prompt should receive its response");
        old_prompt
            .await
            .expect("first duplicate prompt should finish");
        let first_prompt_record = queue
            .get(task_id)
            .expect("first prompt task should load")
            .expect("first prompt task should remain");
        assert_eq!(first_prompt_record.generation, 0);
        assert_eq!(
            first_prompt_record.status,
            TaskStatus::AwaitingDuplicateChoice
        );
        assert_eq!(first_prompt_record.status_message_id, Some(2_100));

        let resumed = queue
            .claim_resume(task_id, false)
            .expect("duplicate prompt should be resumable")
            .expect("first duplicate generation should resume");
        assert_eq!(resumed.generation, 1);
        let delayed_task_id = task_id.to_string();
        let delayed_job = job.clone();
        let delayed_duplicate = duplicate.clone();
        let delayed_telegram = telegram.clone();
        let delayed_queue = Arc::clone(&queue);
        let delayed_prompt = tokio::spawn(async move {
            prompt_duplicate_choice(
                &delayed_telegram,
                &delayed_queue,
                CHAT_ID,
                68,
                delayed_task_id,
                1,
                delayed_job,
                delayed_duplicate,
                Duration::ZERO,
            )
            .await;
        });
        let (delayed_request, delayed_response) = receive_controllable_request(&mut requests).await;
        assert_eq!(delayed_request.method, "sendMessage");
        let during_delayed_prompt = queue
            .get(task_id)
            .expect("delayed generation task should load")
            .expect("delayed generation task should remain");
        assert_eq!(during_delayed_prompt.generation, 1);
        assert_eq!(
            during_delayed_prompt.status,
            TaskStatus::AwaitingDuplicateChoice
        );
        assert_eq!(during_delayed_prompt.status_message_id, None);

        let stale_callback = tokio::spawn(handle_callback_query(
            bot_context(telegram.clone(), config.clone(), Arc::clone(&queue), 69),
            crate::telegram::CallbackQuery {
                id: "old-duplicate-cancel".to_string(),
                data: Some(first_cancel_callback),
                message: Some(crate::telegram::Message {
                    message_id: 2_100,
                    chat: crate::telegram::Chat {
                        id: CHAT_ID,
                        kind: Some("private".to_string()),
                    },
                    text: None,
                    from: None,
                }),
            },
        ));
        let (callback_request, callback_response) =
            receive_controllable_request(&mut requests).await;
        assert_eq!(callback_request.method, "answerCallbackQuery");
        assert_eq!(
            callback_request.body["text"].as_str(),
            Some("This choice has expired.")
        );
        callback_response
            .send(controlled_success_response())
            .expect("stale callback acknowledgement should succeed");
        stale_callback
            .await
            .expect("stale duplicate callback should finish");
        let after_stale_callback = queue
            .get(task_id)
            .expect("task after stale callback should load")
            .expect("task after stale callback should remain");
        assert_eq!(after_stale_callback.generation, 1);
        assert_eq!(
            after_stale_callback.status,
            TaskStatus::AwaitingDuplicateChoice
        );
        assert_eq!(after_stale_callback.status_message_id, None);

        let resumed_again = queue
            .claim_resume(task_id, false)
            .expect("delayed duplicate generation should be resumable")
            .expect("delayed duplicate generation should resume");
        assert_eq!(resumed_again.generation, 2);
        let current_task_id = task_id.to_string();
        let current_job = job.clone();
        let current_duplicate = duplicate.clone();
        let current_telegram = telegram.clone();
        let current_queue = Arc::clone(&queue);
        let current_prompt = tokio::spawn(async move {
            prompt_duplicate_choice(
                &current_telegram,
                &current_queue,
                CHAT_ID,
                68,
                current_task_id,
                2,
                current_job,
                current_duplicate,
                Duration::ZERO,
            )
            .await;
        });
        let (current_request, current_response) = receive_controllable_request(&mut requests).await;
        assert_eq!(current_request.method, "sendMessage");
        current_response
            .send(controlled_send_message_response(2_102, CHAT_ID))
            .expect("current duplicate prompt response should succeed");
        current_prompt
            .await
            .expect("current duplicate prompt should finish");

        delayed_response
            .send(controlled_failure_response())
            .expect("old in-flight prompt should receive its delayed failure");
        delayed_prompt
            .await
            .expect("old failed prompt should finish without canceling current generation");
        assert!(
            requests.try_recv().is_err(),
            "an obsolete prompt failure must not emit a replacement or failure notice"
        );
        let current = queue
            .get(task_id)
            .expect("current duplicate task should load")
            .expect("current duplicate task should remain");
        assert_eq!(current.generation, 2);
        assert_eq!(current.status, TaskStatus::AwaitingDuplicateChoice);
        assert_eq!(current.status_message_id, Some(2_102));
        assert_eq!(current.job, job);

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn collection_cancel_during_queued_edit_finishes_original_message_e2e() {
        let (telegram, mut requests, shutdown, server) =
            spawn_controllable_fake_telegram_api().await;
        let task_id = "collection-cancel-during-queued-edit";
        let job = JobRequest::Bilibili {
            url: "https://space.bilibili.com/210798/channel/collectiondetail?sid=167822"
                .to_string(),
            selection: Some(BilibiliSelection::All),
        };
        let (queue_root, config, queue) = create_collection_queue_with_job(
            "collection-cancel-during-queued-edit",
            task_id,
            CHAT_ID,
            71,
            job.clone(),
        );
        queue
            .set_status(task_id, TaskStatus::Queued, None)
            .expect("collection task should enter the queue");
        queue
            .set_status_message_id_if_generation(task_id, 0, 4_100)
            .expect("queued message ID should persist")
            .expect("queued task generation should still match");

        let mut context = bot_context(telegram, config, Arc::clone(&queue), 72);
        context.job_dispatch.download_semaphore = Arc::new(Semaphore::new(0));
        let queued_task = tokio::spawn(queue_queued_task(
            context,
            CHAT_ID,
            71,
            task_id.to_string(),
            job,
            JobRunMode::Direct,
            Some(0),
        ));

        let (queued_edit, queued_response) = receive_controllable_request(&mut requests).await;
        assert_eq!(queued_edit.method, "editMessageText");
        assert_eq!(queued_edit.body["message_id"].as_i64(), Some(4_100));
        assert!(
            queued_edit.body["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("Queued job #71"))
        );

        let cancelled = queue
            .cancel_for_chat_if_generation(task_id, 0, CHAT_ID)
            .expect("queued cancellation should persist")
            .expect("queued task should remain cancellable while its edit is in flight");
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        queued_response
            .send(controlled_success_response())
            .expect("delayed queued edit response should be released");
        queued_task
            .await
            .expect("queued message delivery should finish after release");

        let (cancelled_edit, cancelled_response) =
            receive_controllable_request(&mut requests).await;
        assert_eq!(cancelled_edit.method, "editMessageText");
        assert_eq!(cancelled_edit.body["message_id"].as_i64(), Some(4_100));
        assert!(
            cancelled_edit.body["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("Canceled job #71"))
        );
        cancelled_response
            .send(controlled_success_response())
            .expect("canceled main-message edit should succeed");
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(
            requests.try_recv().is_err(),
            "cancellation must not send a replacement message"
        );
        let current = queue
            .get(task_id)
            .expect("canceled task should load")
            .expect("canceled task should remain in the queue history");
        assert_eq!(current.status, TaskStatus::Cancelled);
        assert_eq!(current.status_message_id, Some(4_100));

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn collection_prompt_message_association_retries_and_queued_delivery_reuses_id_e2e() {
        let (telegram, mut requests, shutdown, server) =
            spawn_controllable_fake_telegram_api().await;
        let task_id = "collection-prompt-association-file-provider-retry";
        let (queue_root, config, queue, file_provider) = generation_guard_collection_queue(
            "collection-prompt-association-file-provider-retry",
            task_id,
            73,
            CHAT_ID,
        );
        queue
            .set_status(task_id, TaskStatus::Preparing, None)
            .expect("selection callback should already be in preparation");
        let before_retry = queue
            .get(task_id)
            .expect("preparing task should load")
            .expect("preparing task should exist");
        assert_eq!(before_retry.status, TaskStatus::Preparing);
        assert_eq!(before_retry.status_message_id, None);

        let reads_before = file_provider.read_paths().len();
        file_provider.fail_next_read("simulated transient callback association read failure");
        let associated = set_status_message_id_if_generation_and_status_with_retry_delay(
            &queue,
            task_id,
            0,
            &[TaskStatus::Preparing],
            4_200,
            Duration::ZERO,
        )
        .await
        .expect("association helper should retry a File Provider read failure")
        .expect("current Preparing generation should retain its prompt ID");
        assert_eq!(associated.status, TaskStatus::Preparing);
        assert_eq!(associated.status_message_id, Some(4_200));
        assert!(
            file_provider.read_paths().len() >= reads_before + 2,
            "one failed lookup and one successful retry should both be observed"
        );

        queue
            .set_status(task_id, TaskStatus::Queued, None)
            .expect("selected collection should enter the queue");
        let job = associated.job.clone();
        let mut context = bot_context(telegram, config, Arc::clone(&queue), 74);
        context.queue_start_retry_delay = Duration::ZERO;
        context.job_dispatch.download_semaphore = Arc::new(Semaphore::new(0));
        let queued_task = tokio::spawn(queue_queued_task(
            context,
            CHAT_ID,
            73,
            task_id.to_string(),
            job,
            JobRunMode::Direct,
            Some(0),
        ));

        let (queued_edit, queued_response) = receive_controllable_request(&mut requests).await;
        assert_eq!(queued_edit.method, "editMessageText");
        assert_eq!(queued_edit.body["message_id"].as_i64(), Some(4_200));
        assert!(
            queued_edit.body["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("Queued job #73"))
        );
        queued_response
            .send(controlled_success_response())
            .expect("queued edit should complete on the callback prompt");
        queued_task
            .await
            .expect("queue transition should finish without another status send");

        let cancelled = queue
            .cancel_for_chat_if_generation(task_id, 0, CHAT_ID)
            .expect("blocked worker cancellation should persist")
            .expect("queued task should be cancellable");
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        let (cancelled_edit, cancelled_response) =
            receive_controllable_request(&mut requests).await;
        assert_eq!(cancelled_edit.method, "editMessageText");
        assert_eq!(cancelled_edit.body["message_id"].as_i64(), Some(4_200));
        assert!(
            cancelled_edit.body["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("Canceled job #73"))
        );
        cancelled_response
            .send(controlled_success_response())
            .expect("canceled prompt edit should succeed");
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(
            requests.try_recv().is_err(),
            "association retry must not create a second prompt"
        );
        let current = queue
            .get(task_id)
            .expect("queued task should load")
            .expect("queued task should remain");
        assert_eq!(current.generation, 0);
        assert_eq!(current.status, TaskStatus::Cancelled);
        assert_eq!(current.status_message_id, Some(4_200));

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }

    #[tokio::test]
    async fn old_generation_queue_actions_cannot_mutate_current_tasks_e2e() {
        let (telegram, mut requests, shutdown, server) =
            spawn_controllable_fake_telegram_api().await;
        let task_id = "old-generation-queue-cancel";
        let job = JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
        };
        let (queue_root, mut config, queue) = create_collection_queue_with_job(
            "old-generation-queue-actions",
            task_id,
            CHAT_ID,
            75,
            job.clone(),
        );
        config.telegram.allow_all_chats = true;

        let cases = [
            (
                "old-generation-queue-cancel",
                "cancel",
                TaskStatus::Queued,
                "Task is no longer cancellable.",
            ),
            (
                "old-generation-queue-confirm",
                "confirm",
                TaskStatus::AwaitingConfirmation,
                "There is no pending plan change to confirm.",
            ),
            (
                "old-generation-queue-resume",
                "resume",
                TaskStatus::Interrupted,
                "Task state changed; refresh /queue.",
            ),
            (
                "old-generation-queue-retry",
                "retry",
                TaskStatus::Failed,
                "Task state changed; refresh /queue.",
            ),
        ];

        for (index, (id, _, current_status, _)) in cases.iter().enumerate() {
            if index > 0 {
                assert!(
                    queue
                        .create(TaskRecord::new(
                            (*id).to_string(),
                            75 + index as i64,
                            1_075 + index as i64,
                            CHAT_ID,
                            Some(42),
                            0,
                            job.clone(),
                        ))
                        .expect("stale-action task should persist")
                );
            }
            queue
                .set_status(id, TaskStatus::AwaitingSelection, None)
                .expect("each task should start from a resumable prompt state");
            let resumed = queue
                .claim_resume(id, false)
                .expect("test setup should advance the generation")
                .expect("awaiting-selection task should resume");
            assert_eq!(resumed.generation, 1);
            queue
                .set_status(id, *current_status, None)
                .expect("current generation should enter the action-specific state");
            queue
                .set_status_message_id_if_generation(id, 1, 5_000 + index as i64)
                .expect("current status message ID should persist")
                .expect("current task generation should match");
        }

        for (index, (id, action, _, expected_answer)) in cases.iter().enumerate() {
            let before = queue
                .get(id)
                .expect("current task should load before stale callback")
                .expect("current task should exist before stale callback");
            let context = bot_context(telegram.clone(), config.clone(), Arc::clone(&queue), 80);
            let callback = crate::telegram::CallbackQuery {
                id: format!("stale-queue-action-{index}"),
                data: Some(queue_callback_data(action, id, 0)),
                message: Some(crate::telegram::Message {
                    message_id: 6_000 + index as i64,
                    chat: crate::telegram::Chat {
                        id: CHAT_ID,
                        kind: Some("private".to_string()),
                    },
                    text: None,
                    from: None,
                }),
            };
            let handler = tokio::spawn(handle_callback_query(context, callback));
            let (request, response) = receive_controllable_request(&mut requests).await;
            assert_eq!(request.method, "answerCallbackQuery");
            assert_eq!(request.body["text"].as_str(), Some(*expected_answer));
            response
                .send(controlled_success_response())
                .expect("stale callback acknowledgement should succeed");
            handler
                .await
                .expect("stale queue callback handler should finish");
            assert!(
                requests.try_recv().is_err(),
                "stale {action} must not send or edit a task message"
            );

            let after = queue
                .get(id)
                .expect("current task should load after stale callback")
                .expect("current task should remain after stale callback");
            assert_eq!(after.generation, before.generation);
            assert_eq!(after.status, before.status);
            assert_eq!(after.status_message_id, before.status_message_id);
            assert_eq!(after.job, before.job);
        }

        drop(queue);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(queue_root);
    }
}
