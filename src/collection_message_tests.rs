mod collection_message_tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

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
    ) -> Option<CollectionProgressDelivery> {
        let (progress, progress_rx) = job_progress_channel();
        let progress_task = tokio::spawn(forward_progress(
            telegram.clone(),
            Arc::clone(&queue),
            progress_context(task_id, chat_id, job_id, generation, None),
            progress_rx,
        ));

        progress.send_lifecycle(JobProgressLifecycleEvent::Resolved {
            snapshot: test_collection_snapshot(&manifest, 0, None),
            manifest: manifest.clone(),
        });

        let entry = test_collection_entry_progress(&manifest, 1);
        if start_entry {
            progress.send_lifecycle(JobProgressLifecycleEvent::EntryStarted {
                entry: entry.clone(),
                snapshot: test_collection_snapshot(&manifest, 0, Some(entry.clone())),
            });
            progress.send_replace(Some(collection_progress(&manifest)));
        }

        if complete_entry {
            progress.send_lifecycle(JobProgressLifecycleEvent::EntryCompleted {
                entry,
                file_count: 1,
                snapshot: test_collection_snapshot(&manifest, 1, None),
            });
            progress.send_lifecycle(JobProgressLifecycleEvent::Completed {
                snapshot: test_collection_snapshot(&manifest, 1, None),
            });
        } else if fail {
            progress.send_lifecycle(JobProgressLifecycleEvent::Failed {
                snapshot: test_collection_snapshot(&manifest, 0, start_entry.then_some(entry)),
            });
        }

        drop(progress);
        tokio_timeout(Duration::from_secs(5), progress_task)
            .await
            .expect("collection progress forwarding should finish")
            .expect("collection progress task should not panic")
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
}
