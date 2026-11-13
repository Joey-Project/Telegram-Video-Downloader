mod music_bot_tests {
    use super::*;
    use crate::music_manager::tests::{DownloadGate, MockBackend, TestDirectory, TestHttpServer};

    fn update(id: i64, sender: i64, document: bool, name: &str, size: u64) -> Update {
        let mime_type = if name.to_ascii_lowercase().ends_with(".flac") {
            "audio/flac"
        } else {
            "audio/mpeg"
        };
        let mut message = serde_json::json!({
            "message_id": id,
            "chat": {"id": sender, "type": "private"},
            "from": {"id": sender, "is_bot": false}
        });
        message[if document { "document" } else { "audio" }] = serde_json::json!({
            "file_id": "music-file-fixture",
            "file_unique_id": "music-unique-fixture",
            "file_name": name,
            "file_size": size,
            // Telegram may return a mismatched hint for older M4A uploads.
            "mime_type": mime_type
        });
        serde_json::from_value(serde_json::json!({"update_id": id, "message": message})).unwrap()
    }

    fn context(
        config: AppConfig,
        telegram: TelegramClient,
        music: Option<Arc<music_runtime::MusicService>>,
    ) -> BotContext {
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        BotContext {
            queue: Arc::new(QueueManager::open(&config).unwrap()),
            config: Arc::new(config),
            telegram,
            music,
            job_dispatch: JobDispatch {
                download_semaphore: Arc::new(Semaphore::new(1)),
                duplicate_scan_semaphore: Arc::new(Semaphore::new(1)),
            },
            next_job_id: Arc::new(AtomicU64::new(1)),
            queue_start_retry_delay: Duration::ZERO,
        }
    }

    fn config_for(directory: &TestDirectory) -> AppConfig {
        let settings = directory.settings();
        let root = settings.state_dir.parent().unwrap();
        let mut config = AppConfig::for_test();
        config.telegram.allowed_chat_ids = vec![42];
        config.downloads.video_dir = root.join("videos");
        config.downloads.pdf_dir = root.join("pdfs");
        config.music = settings;
        config
    }

    #[test]
    fn uploaded_audio_and_documents_keep_large_file_metadata() {
        for (document, name) in [
            (false, "track.M4A"),
            (true, "track.mp3"),
            (true, "track.aac"),
            (true, "track.FLAC"),
        ] {
            let message = update(8, 42, document, name, 32 * 1024 * 1024)
                .message
                .unwrap();
            let upload = music_upload_from_message(8, &message, 512 * 1024 * 1024)
                .unwrap()
                .unwrap();
            assert_eq!(upload.file_size, 32 * 1024 * 1024);
            assert_eq!(upload.file_name, name);
            assert_eq!(upload.chat_id, 42);
            assert_eq!(upload.user_id, 42);
        }
        let mut inferred = update(9, 42, false, "track.flac", 32 * 1024 * 1024)
            .message
            .unwrap();
        inferred.audio.as_mut().unwrap().file_name = None;
        let upload = music_upload_from_message(9, &inferred, 512 * 1024 * 1024)
            .unwrap()
            .unwrap();
        assert_eq!(upload.file_name, "audio.flac");
        assert_eq!(upload.mime_type.as_deref(), Some("audio/flac"));

        let oversized = update(8, 42, false, "track.m4a", 513 * 1024 * 1024)
            .message
            .unwrap();
        assert!(music_upload_from_message(8, &oversized, 512 * 1024 * 1024).is_err());
        let pdf = update(8, 42, true, "notes.pdf", 24).message.unwrap();
        assert!(
            music_upload_from_message(8, &pdf, 512 * 1024 * 1024)
                .unwrap()
                .is_none()
        );
        let mut malformed = update(8, 42, false, "track.m4a", 24).message.unwrap();
        malformed.audio.as_mut().unwrap().file_id = "x".repeat(4097);
        assert!(music_upload_from_message(8, &malformed, 512 * 1024 * 1024).is_err());
        malformed.audio.as_mut().unwrap().file_id = "music-file-fixture".into();
        malformed.audio.as_mut().unwrap().mime_type = Some("audio/\ninvalid".into());
        assert!(music_upload_from_message(8, &malformed, 512 * 1024 * 1024).is_err());
    }

    #[tokio::test]
    async fn public_link_access_does_not_authorize_music_uploads() {
        let directory = TestDirectory::new();
        let server = TestHttpServer::start().await;
        let mut config = config_for(&directory);
        config.telegram.allow_all_chats = true;
        let backend = MockBackend::new(config.music.archive_dir.clone());
        let (stop, shutdown) = watch::channel(false);
        let service = music_runtime::MusicService::with_backend(
            config.music.clone(),
            server.client.clone(),
            backend.clone(),
            shutdown,
        );
        service.start();
        let context = context(config, server.client.clone(), Some(service));
        let mut offset = None;
        process_telegram_updates_with_context(
            &context,
            vec![update(8, 43, false, "track.m4a", 24)],
            &mut offset,
        )
        .await
        .unwrap();
        assert_eq!(offset, Some(9));
        assert_eq!(backend.downloads.load(Ordering::Relaxed), 0);
        assert_eq!(server.count_method("sendMessage"), 0);
        stop.send(true).unwrap();
        server.stop();
    }

    #[tokio::test]
    async fn music_download_leaves_help_responsive_and_replay_does_not_duplicate_it() {
        let directory = TestDirectory::new();
        let server = TestHttpServer::start().await;
        let config = config_for(&directory);
        let backend = MockBackend::new(config.music.archive_dir.clone());
        let gate = Arc::new(DownloadGate::default());
        *backend.download_gate.lock().unwrap() = Some(Arc::clone(&gate));
        let (stop, shutdown) = watch::channel(false);
        let service = music_runtime::MusicService::with_backend(
            config.music.clone(),
            server.client.clone(),
            backend.clone(),
            shutdown,
        );
        service.start();
        let context = context(config, server.client.clone(), Some(service));
        let mut offset = None;
        tokio_timeout(
            Duration::from_secs(10),
            process_telegram_updates_with_context(
                &context,
                vec![update(1, 42, false, "track.m4a", 24)],
                &mut offset,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(offset, Some(2));
        tokio_timeout(Duration::from_secs(8), async {
            while backend.downloads.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let help = serde_json::from_value(serde_json::json!({
            "update_id": 2, "message": {"message_id": 2, "chat": {"id": 42, "type": "private"}, "from": {"id": 42}, "text": "/help"}
        })).unwrap();
        tokio_timeout(
            Duration::from_secs(2),
            process_telegram_updates_with_context(&context, vec![help], &mut offset),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(server.wait_for_text("Telegram Local Downloader Bot").await);
        assert_eq!(
            backend.adds.load(Ordering::Relaxed),
            0,
            "download is still held by its gate"
        );
        let mut replay_offset = None;
        process_telegram_updates_with_context(
            &context,
            vec![update(1, 42, false, "track.m4a", 24)],
            &mut replay_offset,
        )
        .await
        .unwrap();
        assert_eq!(backend.downloads.load(Ordering::Relaxed), 1);
        gate.release();
        tokio_timeout(Duration::from_secs(8), async {
            while backend.adds.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(backend.adds.load(Ordering::Relaxed), 1);
        stop.send(true).unwrap();
        server.stop();
    }

    #[tokio::test]
    async fn full_music_queue_does_not_block_later_telegram_commands() {
        let directory = TestDirectory::new();
        let server = TestHttpServer::start().await;
        let config = config_for(&directory);
        music_manager::tests::seed_full_active_queue(&config.music);
        let backend = MockBackend::new(config.music.archive_dir.clone());
        let gate = Arc::new(DownloadGate::default());
        *backend.download_gate.lock().unwrap() = Some(Arc::clone(&gate));
        let (stop, shutdown) = watch::channel(false);
        let service = music_runtime::MusicService::with_backend(
            config.music.clone(),
            server.client.clone(),
            backend.clone(),
            shutdown,
        );
        service.start();
        let context = context(config, server.client.clone(), Some(service));
        let help = serde_json::from_value(serde_json::json!({
            "update_id": 100_002,
            "message": {
                "message_id": 100_002,
                "chat": {"id": 42, "type": "private"},
                "from": {"id": 42},
                "text": "/help"
            }
        }))
        .unwrap();
        let mut offset = None;
        tokio_timeout(
            Duration::from_secs(10),
            process_telegram_updates_with_context(
                &context,
                vec![update(100_001, 42, false, "overflow.m4a", 24), help],
                &mut offset,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(offset, Some(100_003));
        assert!(server.wait_for_text("The Music queue is full").await);
        assert!(server.wait_for_text("Telegram Local Downloader Bot").await);
        assert!(backend.downloads.load(Ordering::Relaxed) <= 1);
        assert_eq!(backend.adds.load(Ordering::Relaxed), 0);
        stop.send(true).unwrap();
        gate.release();
        server.stop();
    }

    #[tokio::test]
    async fn failed_music_admission_preserves_the_polling_offset() {
        let directory = TestDirectory::new();
        let server = TestHttpServer::start().await;
        let config = config_for(&directory);
        fs::write(&config.music.state_dir, b"not a directory").unwrap();
        let backend = MockBackend::new(config.music.archive_dir.clone());
        let (stop, shutdown) = watch::channel(false);
        let service = music_runtime::MusicService::with_backend(
            config.music.clone(),
            server.client.clone(),
            backend.clone(),
            shutdown,
        );
        service.start();
        let context = context(config, server.client.clone(), Some(service));
        let mut offset = None;
        let failure = tokio_timeout(
            Duration::from_secs(8),
            process_telegram_updates_with_context(
                &context,
                vec![update(1, 42, false, "track.m4a", 24)],
                &mut offset,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(failure.is::<MusicAdmissionFailure>());
        assert_eq!(offset, None);
        assert_eq!(backend.downloads.load(Ordering::Relaxed), 0);
        stop.send(true).unwrap();
        server.stop();
    }
}
