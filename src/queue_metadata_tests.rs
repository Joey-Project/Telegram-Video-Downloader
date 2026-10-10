mod queue_metadata_tests {
    use super::*;

    const CHAT_ID: i64 = 123_456_789;
    const BVID: &str = "BV12TRrBcEP8";

    fn queue_fixture(label: &str) -> (PathBuf, AppConfig, Arc<QueueManager>, JobRequest) {
        let root = temp_main_test_dir(label);
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = root.join("videos");
        config.downloads.pdf_dir = root.join("pdfs");
        config.bilibili.auth.state_path = root.join("state.json");
        config.bilibili.auth.credential_file = root.join("credentials.json");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        let queue = Arc::new(QueueManager::open(&config).unwrap());
        let job = JobRequest::Bilibili {
            url: format!("https://www.bilibili.com/video/{BVID}"),
            selection: None,
        };
        (root, config, queue, job)
    }

    async fn spawn_metadata_api(
        deny_streams: bool,
    ) -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, mut shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => accepted,
                };
                let (mut stream, _) = accepted.unwrap();
                let request = read_fake_telegram_request(&mut stream).await.unwrap();
                let body = match request.method.as_str() {
                    "view" => serde_json::json!({"code": 0, "data": {
                        "aid": 170001, "bvid": BVID, "title": "Queue fixture video",
                        "pages": [{"page": 1, "cid": 9988, "part": "Fixture part"}]
                    }}),
                    "playurl" if deny_streams => serde_json::json!({
                        "code": -10403, "message": "This video is not available in your region"
                    }),
                    "playurl" => serde_json::json!({"code": 0, "data": {"dash": {
                        "video": [{"id": 80, "baseUrl": format!("http://{address}/video"),
                            "width": 1920, "height": 1080, "codecs": "avc1",
                            "size": 5 * 1024 * 1024}],
                        "audio": [{"id": 30280, "baseUrl": format!("http://{address}/audio"),
                            "codecs": "mp4a", "size": 1024 * 1024}]
                    }}}),
                    "tags" => serde_json::json!({"code": 0, "data": []}),
                    _ => serde_json::json!({"code": 0, "data": {"subtitle": {"subtitles": []}}}),
                }.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("--api-base=http://{address}"), shutdown, server)
    }

    #[tokio::test]
    async fn duplicate_choice_details_survive_restart_and_keep_plan_unaccepted() {
        let (root, mut config, queue, job) = queue_fixture("queue-duplicate-metadata");
        let (endpoint, stop_api, api) = spawn_metadata_api(false).await;
        config.bilibili.global_args = vec![endpoint];
        fs::write(config.downloads.video_dir.join(format!("{BVID}.mp4")), b"existing").unwrap();
        let id = "duplicate-preview";
        let mut task = TaskRecord::new(id.to_string(), 1, 2, CHAT_ID, None, 0, job.clone());
        task.status = TaskStatus::Preparing;
        queue.create(task).unwrap();
        let (telegram, mut requests, _, shutdown, server) = spawn_fake_telegram_api().await;
        let context = BotContext {
            telegram,
            config: Arc::new(config.clone()),
            job_dispatch: JobDispatch {
                download_semaphore: Arc::new(Semaphore::new(1)),
                duplicate_scan_semaphore: Arc::new(Semaphore::new(1)),
            },
            next_job_id: Arc::new(AtomicU64::new(1)),
            queue: Arc::clone(&queue),
            queue_start_retry_delay: Duration::ZERO,
        };
        tokio_timeout(Duration::from_secs(10), process_job_after_duplicate_check(
            context, CHAT_ID, 1, id.to_string(), 0, job,
        )).await.unwrap();
        let task = queue.get(id).unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::AwaitingDuplicateChoice);
        assert!(task.plan.is_none());
        let display = queue_task_display(&task);
        assert_eq!(display.title, "Queue fixture video");
        assert_eq!(display.quality, "1920x1080");
        assert_eq!(display.size, "6.0 MiB");
        assert!(take_fake_telegram_requests(&mut requests).iter().any(|request| {
            request.body["text"].as_str().is_some_and(|text| text.contains("Existing video found"))
        }));
        // The pending choice owns its duplicate file binding, but not the queue.
        drop(queue);
        let reopened = QueueManager::open(&config).unwrap();
        let task = reopened.get(id).unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Interrupted);
        assert!(task.plan.is_none());
        let (text, keyboard) = render_queue_page(&reopened, CHAT_ID, QueueCommand {
            history: false, page: 0,
        }).unwrap();
        assert!(text.contains("Title: Queue fixture video"));
        assert!(text.contains("1920x1080 · 6.0 MiB"));
        let button = &keyboard.unwrap().inline_keyboard[0][0];
        assert!(button.text.contains("Queue fixture video"));
        assert!(matches!(parse_queue_callback_data(&button.callback_data),
            Some(QueueCallbackAction::Resume { generation: 0, .. })));
        pending_duplicate_jobs().lock().await.retain(|_, pending| pending.task_id != id);
        drop(reopened);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = stop_api.send(());
        api.await.unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn duplicate_preview_denial_keeps_basic_title_and_choice() {
        let (root, mut config, queue, job) = queue_fixture("queue-denied-duplicate-preview");
        let (endpoint, stop_api, api) = spawn_metadata_api(true).await;
        config.bilibili.global_args = vec![endpoint];
        fs::write(config.downloads.video_dir.join(format!("{BVID}.mp4")), b"existing").unwrap();
        let check = find_video_duplicate_with_probe(&config, &job).await.unwrap();
        assert!(check.duplicate.is_some());
        let metadata = check.display_metadata.unwrap();
        assert_eq!(metadata.title.as_deref(), Some("Queue fixture video"));
        assert!(metadata.selected_format_ids.is_empty());
        let _ = stop_api.send(());
        api.await.unwrap();
        drop(queue);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn denied_plan_retains_basic_title_and_queue_explains_unknown_streams() {
        let (root, mut config, queue, job) = queue_fixture("queue-denied-metadata");
        let (endpoint, stop_api, api) = spawn_metadata_api(true).await;
        config.bilibili.global_args = vec![endpoint];
        let id = "denied-preview";
        let mut task = TaskRecord::new(id.to_string(), 1, 2, CHAT_ID, None, 0, job.clone());
        task.status = TaskStatus::Running;
        queue.create(task).unwrap();
        let error = tokio_timeout(Duration::from_secs(10), inspect_and_preserve_job_plan(
            &config, &queue, id, 0, &job,
        )).await.unwrap().expect_err("denied streams must remain a failed plan");
        queue.fail_if_generation(id, 0, format!("{error:#}")).unwrap();
        let task = queue.get(id).unwrap().unwrap();
        assert!(task.plan.is_none());
        let display = queue_task_display(&task);
        assert_eq!(display.title, "Queue fixture video");
        assert_eq!(display.quality, "unknown");
        assert_eq!(display.size, "unknown");
        let (text, keyboard) = render_queue_page(&queue, CHAT_ID, QueueCommand {
            history: false, page: 0,
        }).unwrap();
        assert!(text.contains("API returned code -10403"), "{text}");
        assert!(keyboard.unwrap().inline_keyboard[0][0].text.starts_with("Retry"));
        let _ = stop_api.send(());
        api.await.unwrap();
        drop(queue);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn failed_plan_metadata_timeout_includes_credential_semaphore_wait() {
        let (root, _, queue, job) = queue_fixture("queue-blocked-metadata");
        let id = "blocked-preview";
        let mut task = TaskRecord::new(id.to_string(), 1, 2, CHAT_ID, None, 0, job);
        task.status = TaskStatus::Running;
        queue.create(task).unwrap();
        let credential_sync = Semaphore::new(1);
        let blocked_permit = credential_sync.acquire().await.unwrap();
        let metadata_probe = async {
            let _permit = credential_sync.acquire().await.unwrap();
            panic!("blocked credential work must not start");
        };
        let started = Instant::now();
        let error = tokio_timeout(
            Duration::from_secs(10),
            preserve_job_plan_result(
                &queue,
                id,
                0,
                Err(anyhow::anyhow!("Original stream planning error")),
                metadata_probe,
            ),
        )
        .await
        .expect("metadata fallback must finish within its own budget")
        .expect_err("metadata timeout must preserve the original plan error");
        assert_eq!(started.elapsed(), Duration::from_secs(5));
        assert_eq!(error.to_string(), "Original stream planning error");
        queue.fail_if_generation(id, 0, error.to_string()).unwrap();
        let task = queue.get(id).unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.error.as_deref(), Some("Original stream planning error"));
        assert!(task.display_metadata.is_none());
        drop(blocked_permit);
        drop(queue);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn preview_updates_reject_stale_generation_and_clear_on_selection_change() {
        let (root, config, queue, job) = queue_fixture("queue-metadata-generation");
        let id = "generation-preview";
        let mut task = TaskRecord::new(id.to_string(), 1, 2, CHAT_ID, None, 0, job);
        task.status = TaskStatus::Preparing;
        queue.create(task.clone()).unwrap();
        let metadata = PlanValidationSnapshot {title: Some("Preview".to_string()), ..Default::default()};
        assert!(queue.set_display_metadata_if_generation(id, 1, metadata.clone()).unwrap().is_none());
        queue.set_display_metadata_if_generation(id, 0, metadata.clone()).unwrap().unwrap();
        let changed_job = JobRequest::Bilibili {
            url: task.original_url.clone(), selection: Some(BilibiliSelection::Page(2)),
        };
        queue.update_job_if_generation(id, 0, &[TaskStatus::Preparing], changed_job, TaskStatus::Preparing).unwrap().unwrap();
        assert!(queue.get(id).unwrap().unwrap().display_metadata.is_none());
        queue.set_status(id, TaskStatus::Running, None).unwrap();
        queue.set_display_metadata_if_generation(id, 0, metadata).unwrap().unwrap();
        let actual = PlanValidationSnapshot {title: Some("Current plan".to_string()), ..Default::default()};
        let (_, changes) = queue.set_plan_if_generation(id, 0, actual).unwrap().unwrap();
        assert!(changes.is_empty(), "preview must not count as an accepted plan");
        assert_eq!(queue_task_display(&queue.get(id).unwrap().unwrap()).title, "Current plan");
        let mut legacy = serde_json::to_value(task).unwrap();
        legacy.as_object_mut().unwrap().remove("display_metadata");
        assert!(serde_json::from_value::<TaskRecord>(legacy).unwrap().display_metadata.is_none());
        drop(queue);
        let _ = config;
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn missing_pdf_metadata_uses_document_notice() {
        let mut task = TaskRecord::new(
            "document-notice".to_string(), 1, 2, CHAT_ID, None, 0,
            JobRequest::Pdf { url: "https://example.com/document.pdf".to_string() },
        );
        for status in [TaskStatus::Received, TaskStatus::Cancelled] {
            task.status = status;
            assert_eq!(
                queue_task_detail_notice(&task).as_deref(),
                Some("Document information is not available yet."),
            );
        }
    }

    #[test]
    fn failed_episode_notice_keeps_episode_index_and_api_code() {
        let mut task = TaskRecord::new(
            "episode-notice".to_string(), 1, 2, CHAT_ID, None, 0,
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".to_string(),
                selection: Some(BilibiliSelection::All),
            },
        );
        task.status = TaskStatus::Failed;
        task.error = Some(format!(
            "{}: episode index 203 planning failed: API returned code -10403: {}",
            "Outer planning context".repeat(10), "Denied".repeat(50),
        ));
        let notice = queue_task_detail_notice(&task).unwrap();
        assert!(notice.starts_with("episode index 203 planning failed: API returned code -10403:"));
        assert!(notice.encode_utf16().count() <= QUEUE_TASK_DETAIL_NOTICE_UNITS);
    }

    #[test]
    fn failed_queue_page_keeps_all_buttons_with_long_details() {
        let (root, _, queue, job) = queue_fixture("queue-metadata-page-limit");
        for ordinal in 0..10 {
            let mut task = TaskRecord::new(format!("long-failed-task-{ordinal}"), ordinal, ordinal, CHAT_ID, None, 0, job.clone());
            task.status = TaskStatus::Failed;
            task.error = Some(format!("API returned code -10403: {}", "😀".repeat(200)));
            task.display_metadata = Some(PlanValidationSnapshot {
                title: Some("👨‍👩‍👧‍👦".repeat(100)), ..Default::default()
            });
            task.media_entries_total = usize::MAX;
            task.media_entries_completed = usize::MAX;
            task.media_entries_failed = usize::MAX;
            queue.create(task).unwrap();
        }
        let (text, keyboard) = render_queue_page(&queue, CHAT_ID, QueueCommand {
            history: false, page: 0,
        }).unwrap();
        assert!(text.encode_utf16().count() <= QUEUE_PAGE_MAX_TEXT_UNITS);
        assert_eq!(text.matches("Details:").count(), 10);
        assert_eq!(keyboard.expect("error text must not remove task actions").inline_keyboard.len(), 10);
        drop(queue);
        fs::remove_dir_all(root).unwrap();
    }
}
