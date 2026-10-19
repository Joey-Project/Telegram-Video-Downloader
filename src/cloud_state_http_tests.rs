#[tokio::test]
async fn cloud_state_reporter_retries_transient_failure_with_same_content_and_syncs_later_change() {
    let root = temp_cloud_bot_dir("state-reporter-transient");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    cloud_state
        .state_post_statuses
        .lock()
        .expect("fake state status lock should not poison")
        .extend([503, 200, 200]);

    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("initial"));
    let reporter = tokio::spawn(cloud_state_reporter(runtime.clone()));
    runtime.state_notify.notify_one();

    let posts = wait_for_state_posts(&cloud_state, 2).await;
    assert_eq!(posts[0]["status"], 503);
    assert_eq!(posts[1]["status"], 200);
    assert_eq!(posts[0]["state_version"], posts[1]["state_version"]);
    assert_eq!(posts[0]["content_hash"], posts[1]["content_hash"]);
    assert_ne!(
        posts[0]["reported_at"], posts[1]["reported_at"],
        "a retry may refresh reported_at while preserving idempotent content"
    );

    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("updated"));
    runtime.state_notify.notify_one();
    let posts = wait_for_state_posts(&cloud_state, 3).await;
    assert_eq!(posts[2]["status"], 200);
    assert_eq!(posts[2]["revision"], "updated");
    assert!(posts[2]["state_version"].as_u64() > posts[1]["state_version"].as_u64());
    assert_ne!(posts[2]["content_hash"], posts[1]["content_hash"]);

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_reporter_publishes_scan_failure_with_cached_snapshot_then_clears_it() {
    let root = temp_cloud_bot_dir("state-reporter-scan-warning");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    cloud_state
        .state_post_statuses
        .lock()
        .expect("fake state status lock should not poison")
        .extend([200, 200, 200]);

    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    let mut cached_snapshot = empty_cloud_test_library_snapshot("cached-library-revision");
    cached_snapshot.scanned_at = 1_800_000_123;
    cached_snapshot.items.push(LibraryItem {
        id: "cached-item-id".to_string(),
        title: "cached title".to_string(),
        relative_path: "cached.mp4".to_string(),
        bytes: 123,
        width: Some(1920),
        height: Some(1080),
        codec: Some("h264".to_string()),
        source_ids: Vec::new(),
        collection: None,
        attachments: Vec::new(),
        authoritative_nfo_path: None,
        confidence: Confidence::High,
        metadata_status: MetadataStatus::Complete,
        integrity_status: IntegrityStatus::Verified,
        presence: Presence::Present,
    });
    cached_snapshot.warnings = vec!["existing library warning".to_string()];
    *runtime.library_snapshot.lock().await = Some(cached_snapshot.clone());

    let reporter = tokio::spawn(cloud_state_reporter_with_heartbeat(
        runtime.clone(),
        Duration::from_secs(300),
    ));
    runtime.state_notify.notify_one();
    let posts = wait_for_state_posts(&cloud_state, 1).await;
    assert_eq!(posts[0]["status"], 200);
    assert_eq!(posts[0]["revision"], "cached-library-revision");
    assert_eq!(posts[0]["library_scanned_at"], 1_800_000_123_u64);
    assert_eq!(posts[0]["library_item_count"], 1);
    assert_eq!(posts[0]["library_warning_count"], 1);
    assert_eq!(posts[0]["library_scan_warning"], serde_json::Value::Null);

    let scan_result =
        run_cloud_library_operation::<()>(&runtime, "fixture library scan failed", move |_, _| {
            Err(anyhow::anyhow!("temporary provider enumeration failure"))
        })
        .await;
    assert!(scan_result.is_err(), "the simulated scan should fail");
    let posts = wait_for_state_posts(&cloud_state, 2).await;
    assert_eq!(posts[1]["status"], 200);
    assert!(posts[1]["state_version"].as_u64() > posts[0]["state_version"].as_u64());
    assert_ne!(posts[1]["content_hash"], posts[0]["content_hash"]);
    assert_eq!(posts[1]["revision"], posts[0]["revision"]);
    assert_eq!(
        posts[1]["library_scanned_at"],
        posts[0]["library_scanned_at"]
    );
    assert_eq!(posts[1]["library_item_count"], 1);
    assert_eq!(
        posts[1]["library_items_hash"],
        posts[0]["library_items_hash"]
    );
    assert_eq!(posts[1]["library_warning_count"], 2);
    assert_eq!(posts[1]["library_incomplete"], true);
    assert!(
        posts[1]["library_scan_warning"]
            .as_str()
            .is_some_and(|warning| warning.contains("temporary provider enumeration failure"))
    );
    assert!(
        posts[1]["library_warnings"]
            .as_array()
            .is_some_and(|warnings| warnings.iter().any(|warning| {
                warning
                    .as_str()
                    .is_some_and(|text| text.contains("last successful snapshot is retained"))
            }))
    );

    let recovery_snapshot = cached_snapshot;
    run_cloud_library_operation::<()>(
        &runtime,
        "fixture library scan recovery failed",
        move |_, _| Ok(((), recovery_snapshot)),
    )
    .await
    .expect("the simulated recovery scan should publish successfully");
    let posts = wait_for_state_posts(&cloud_state, 3).await;
    assert_eq!(posts[2]["status"], 200);
    assert!(posts[2]["state_version"].as_u64() > posts[1]["state_version"].as_u64());
    assert_ne!(posts[2]["content_hash"], posts[1]["content_hash"]);
    assert_eq!(posts[2]["revision"], posts[0]["revision"]);
    assert_eq!(
        posts[2]["library_scanned_at"],
        posts[0]["library_scanned_at"]
    );
    assert_eq!(posts[2]["library_item_count"], 1);
    assert_eq!(
        posts[2]["library_items_hash"],
        posts[0]["library_items_hash"]
    );
    assert_eq!(posts[2]["library_warning_count"], 1);
    assert_eq!(posts[2]["library_scan_warning"], serde_json::Value::Null);
    assert_eq!(posts[2]["library_warnings"][0], "existing library warning");
    assert_eq!(posts[2]["content_hash"], posts[0]["content_hash"]);

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_reporter_retries_413_with_new_incomplete_snapshot_then_syncs_changes() {
    let root = temp_cloud_bot_dir("state-reporter-413");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    cloud_state
        .state_post_statuses
        .lock()
        .expect("fake state status lock should not poison")
        .extend([413, 413, 200, 200, 200]);

    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    let mut library = empty_cloud_test_library_snapshot("oversized");
    let large_title = "x".repeat(256 * 1024);
    library.items = (0..8)
        .map(|index| LibraryItem {
            id: format!("large-item-{index}"),
            title: large_title.clone(),
            relative_path: format!("large-item-{index}.mp4"),
            bytes: 1,
            width: None,
            height: None,
            codec: None,
            source_ids: Vec::new(),
            collection: None,
            attachments: Vec::new(),
            authoritative_nfo_path: None,
            confidence: Confidence::High,
            metadata_status: MetadataStatus::Complete,
            integrity_status: IntegrityStatus::Verified,
            presence: Presence::Present,
        })
        .collect();
    *runtime.library_snapshot.lock().await = Some(library);
    let reporter = tokio::spawn(cloud_state_reporter_with_heartbeat(
        runtime.clone(),
        Duration::from_millis(500),
    ));
    runtime.state_notify.notify_one();

    let posts = tokio::time::timeout(
        Duration::from_secs(20),
        wait_for_state_posts(&cloud_state, 3),
    )
    .await
    .expect("under-soft-cap 413 responses should progressively reduce the snapshot");
    assert_eq!(posts[0]["status"], 413);
    assert_eq!(posts[1]["status"], 413);
    assert_eq!(posts[2]["status"], 200);
    assert!(posts[1]["state_version"].as_u64() > posts[0]["state_version"].as_u64());
    assert!(posts[2]["state_version"].as_u64() > posts[1]["state_version"].as_u64());
    assert_eq!(posts[0]["library_item_count"], 8);
    let first_trimmed_count = posts[1]["library_item_count"]
        .as_u64()
        .expect("first reduced snapshot should report an item count");
    let second_trimmed_count = posts[2]["library_item_count"]
        .as_u64()
        .expect("second reduced snapshot should report an item count");
    assert!(first_trimmed_count > 0 && first_trimmed_count < 8);
    assert!(second_trimmed_count > 0 && second_trimmed_count < first_trimmed_count);
    assert_ne!(posts[0]["library_hash"], posts[1]["library_hash"]);
    assert_ne!(posts[1]["library_hash"], posts[2]["library_hash"]);
    assert_eq!(posts[2]["library_incomplete"], true);
    assert_ne!(
        posts[2]["library_ready"].as_bool(),
        Some(false),
        "a trimmed scanned snapshot must not be reported as an unscanned library"
    );
    assert!(
        posts[2]["library_warnings"]
            .as_array()
            .is_some_and(|warnings| !warnings.is_empty())
    );

    let posts = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_state_posts(&cloud_state, 4),
    )
    .await
    .expect("accepted reduced snapshots should remain healthy on heartbeat");
    assert_eq!(posts[3]["status"], 200);
    assert_eq!(posts[3]["state_version"], posts[2]["state_version"]);
    assert_eq!(posts[3]["content_hash"], posts[2]["content_hash"]);
    assert_ne!(posts[3]["reported_at"], posts[2]["reported_at"]);

    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("after-413"));
    runtime.state_notify.notify_one();
    let posts = wait_for_state_posts(&cloud_state, 5).await;
    assert_eq!(posts[4]["status"], 200);
    assert_eq!(posts[4]["revision"], "after-413");
    assert!(posts[4]["state_version"].as_u64() > posts[3]["state_version"].as_u64());
    assert_ne!(posts[4]["content_hash"], posts[3]["content_hash"]);

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_reporter_bounds_large_library_warnings_and_categories_with_zero_or_few_items()
{
    let root = temp_cloud_bot_dir("state-reporter-library-metadata-budget");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    cloud_state
        .state_post_statuses
        .lock()
        .expect("fake state status lock should not poison")
        .extend([413, 200, 413, 200, 413, 200, 200]);

    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    let reporter = tokio::spawn(cloud_state_reporter_with_heartbeat(
        runtime.clone(),
        Duration::from_secs(300),
    ));

    let mut expected_posts = 0;
    for item_count in [0_usize, 1, 3] {
        let mut snapshot =
            empty_cloud_test_library_snapshot(&format!("metadata-budget-items-{item_count}"));
        snapshot.scanned_at = 1_800_000_456;
        snapshot.categories = (0..96)
            .map(|index| format!("category-{index:03}-{}", "x".repeat(499)))
            .collect();
        snapshot
            .categories
            .push(format!("category-overlong-{}", "z".repeat(600)));
        snapshot.warnings = (0..96)
            .map(|index| format!("warning-{index}: {}", "w".repeat(16 * 1024)))
            .collect();
        snapshot.items = (0..item_count)
            .map(|index| LibraryItem {
                id: format!("metadata-budget-item-{index}"),
                title: format!("fixture item {index}"),
                relative_path: format!("fixture-{index}.mp4"),
                bytes: index as u64 + 1,
                width: None,
                height: None,
                codec: None,
                source_ids: Vec::new(),
                collection: None,
                attachments: Vec::new(),
                authoritative_nfo_path: None,
                confidence: Confidence::High,
                metadata_status: MetadataStatus::Complete,
                integrity_status: IntegrityStatus::Verified,
                presence: Presence::Present,
            })
            .collect();
        *runtime.library_snapshot.lock().await = Some(snapshot);
        runtime.state_notify.notify_one();

        let posts = tokio::time::timeout(
            Duration::from_secs(30),
            wait_for_state_posts(&cloud_state, expected_posts + 2),
        )
        .await
        .expect("warnings and categories should compact after an oversized-state response");
        let rejected = &posts[expected_posts];
        let accepted = &posts[expected_posts + 1];
        assert_eq!(rejected["status"], 413);
        assert_eq!(accepted["status"], 200);
        assert!(accepted["state_version"].as_u64() > rejected["state_version"].as_u64());
        assert_ne!(accepted["library_hash"], rejected["library_hash"]);
        assert_eq!(
            accepted["library_item_count"].as_u64(),
            Some(item_count as u64)
        );
        assert_eq!(
            accepted["library_items_hash"],
            rejected["library_items_hash"]
        );
        assert_eq!(accepted["library_incomplete"], true);
        assert!(
            rejected["library_warning_bytes"]
                .as_u64()
                .unwrap_or_default()
                > 24 * 1024
        );
        assert!(
            rejected["library_category_bytes"]
                .as_u64()
                .unwrap_or_default()
                > 32 * 1024
        );
        assert_eq!(rejected["library_category_count"], 97);
        assert!(
            accepted["library_warning_count"]
                .as_u64()
                .unwrap_or_default()
                <= 64
        );
        assert!(
            accepted["library_warning_bytes"]
                .as_u64()
                .unwrap_or_default()
                <= 24 * 1024
        );
        assert!(
            accepted["library_category_count"]
                .as_u64()
                .unwrap_or_default()
                > 0
        );
        assert!(
            accepted["library_category_count"]
                .as_u64()
                .unwrap_or_default()
                < 97
        );
        assert!(
            accepted["library_category_bytes"]
                .as_u64()
                .unwrap_or_default()
                <= 32 * 1024
        );
        assert_eq!(accepted["library_categories_are_fixture_paths"], true);
        assert_eq!(accepted["library_category_omission_summary"], true);
        assert_eq!(accepted["library_warning_compaction_summary"], true);
        assert_eq!(accepted["library_scanned_at"], 1_800_000_456_u64);
        expected_posts += 2;
    }

    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot(
        "after-library-metadata-compaction",
    ));
    runtime.state_notify.notify_one();
    let posts = wait_for_state_posts(&cloud_state, expected_posts + 1).await;
    assert_eq!(posts[expected_posts]["status"], 200);
    assert_eq!(
        posts[expected_posts]["revision"],
        "after-library-metadata-compaction"
    );
    assert!(
        posts[expected_posts]["state_version"].as_u64()
            > posts[expected_posts - 1]["state_version"].as_u64()
    );
    assert_ne!(
        posts[expected_posts]["content_hash"],
        posts[expected_posts - 1]["content_hash"]
    );

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_reporter_compacts_operation_receipts_before_library_and_keeps_latest_import() {
    let root = temp_cloud_bot_dir("state-reporter-operation-budget");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    cloud_state
        .state_post_statuses
        .lock()
        .expect("fake state status lock should not poison")
        .extend([413, 200, 200]);

    for seq in 1..=8 {
        let mut request = cloud_test_request(seq, "legacy_import");
        request.payload = serde_json::json!({"text":""});
        inbox
            .persist(request)
            .expect("legacy import request should persist");
        let result = serde_json::json!({
            "report": {"accepted": 24, "unmatched": 0},
            "hints": (0..24).map(|index| serde_json::json!({
                "source_id": format!("bvid:BV1{seq:08}{index:02}"),
                "relative_path": format!("library/{seq}-{index}.mp4"),
                "candidate_title": format!("{}-{}", seq, "x".repeat(900)),
            })).collect::<Vec<_>>(),
            "candidates": (0..24).map(|index| serde_json::json!({
                "item_id": format!("item-{seq}-{index}"),
                "title": "y".repeat(900),
            })).collect::<Vec<_>>(),
        });
        inbox
            .persist_dispatch_result(seq, InboxStatus::Completed, Some(result), None)
            .expect("legacy import result should persist");
    }

    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("same"));

    let reporter = tokio::spawn(cloud_state_reporter_with_heartbeat(
        runtime.clone(),
        Duration::from_millis(500),
    ));
    runtime.state_notify.notify_one();

    let posts = tokio::time::timeout(
        Duration::from_secs(8),
        wait_for_state_posts(&cloud_state, 3),
    )
    .await
    .expect("operation compaction should recover and heartbeat without another 413");
    assert_eq!(posts[0]["status"], 413);
    assert_eq!(posts[1]["status"], 200);
    assert!(posts[1]["state_version"].as_u64() > posts[0]["state_version"].as_u64());
    assert_eq!(posts[0]["operation_count"], 8);
    assert_eq!(posts[1]["operation_count"], 8);
    assert!(posts[0]["operation_bytes"].as_u64().unwrap_or_default() <= 2 * 1024 * 1024);
    assert!(
        posts[1]["operation_bytes"].as_u64().unwrap_or_default()
            < posts[0]["operation_bytes"].as_u64().unwrap_or_default()
    );
    assert_ne!(posts[0]["operation_hash"], posts[1]["operation_hash"]);
    assert_eq!(posts[1]["library_hash"], posts[0]["library_hash"]);

    assert_eq!(posts[2]["status"], 200);
    assert_eq!(posts[2]["state_version"], posts[1]["state_version"]);
    assert_eq!(posts[2]["content_hash"], posts[1]["content_hash"]);
    assert_ne!(posts[2]["reported_at"], posts[1]["reported_at"]);

    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot(
        "updated-after-compaction",
    ));
    runtime.state_notify.notify_one();
    let posts = wait_for_state_posts(&cloud_state, 4).await;
    assert_eq!(posts[3]["status"], 200);
    assert_eq!(posts[3]["revision"], "updated-after-compaction");
    assert!(posts[3]["state_version"].as_u64() > posts[2]["state_version"].as_u64());
    assert_ne!(posts[3]["content_hash"], posts[2]["content_hash"]);

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_reporter_omits_oversized_items_and_recovers_full_source_after_413() {
    let root = temp_cloud_bot_dir("state-reporter-413-zero-prefix-recovery");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    cloud_state
        .state_post_statuses
        .lock()
        .expect("fake state status lock should not poison")
        .extend([413, 200, 200, 200, 200]);

    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    let mut library = empty_cloud_test_library_snapshot("full-source");
    library.items.push(LibraryItem {
        id: "oversized-first-item".to_string(),
        title: "x".repeat(128 * 1024),
        relative_path: "oversized-first-item.mp4".to_string(),
        bytes: 1,
        width: None,
        height: None,
        codec: None,
        source_ids: Vec::new(),
        collection: None,
        attachments: Vec::new(),
        authoritative_nfo_path: None,
        confidence: Confidence::High,
        metadata_status: MetadataStatus::Complete,
        integrity_status: IntegrityStatus::Verified,
        presence: Presence::Present,
    });
    library.items.push(LibraryItem {
        id: "small-later-item".to_string(),
        title: "small item".to_string(),
        relative_path: "small-later-item.mp4".to_string(),
        bytes: 2,
        width: None,
        height: None,
        codec: None,
        source_ids: Vec::new(),
        collection: None,
        attachments: Vec::new(),
        authoritative_nfo_path: None,
        confidence: Confidence::High,
        metadata_status: MetadataStatus::Complete,
        integrity_status: IntegrityStatus::Verified,
        presence: Presence::Present,
    });
    let original_library = library.clone();
    *runtime.library_snapshot.lock().await = Some(original_library.clone());
    let reporter = tokio::spawn(cloud_state_reporter_with_heartbeat(
        runtime.clone(),
        Duration::from_millis(500),
    ));
    runtime.state_notify.notify_one();

    let posts = wait_for_state_posts(&cloud_state, 3).await;
    assert_eq!(posts[0]["status"], 413);
    assert_eq!(posts[1]["status"], 200);
    assert!(posts[1]["state_version"].as_u64() > posts[0]["state_version"].as_u64());
    assert_ne!(posts[1]["library_ready"].as_bool(), Some(false));
    assert_eq!(posts[1]["library_incomplete"], true);
    assert_eq!(posts[1]["library_item_count"], 0);
    assert!(posts[1]["library_warning_count"].as_u64().unwrap_or(0) > 0);
    assert!(
        posts[1]["library_warnings"]
            .as_array()
            .is_some_and(|warnings| warnings.iter().any(|warning| {
                warning
                    .as_str()
                    .is_some_and(|text| text.contains("omitted 2 library item(s)"))
            }))
    );
    assert_eq!(posts[2]["status"], 200);
    assert_eq!(posts[2]["state_version"], posts[1]["state_version"]);
    assert_eq!(posts[2]["content_hash"], posts[1]["content_hash"]);
    assert_eq!(posts[2]["library_item_count"], 0);

    let mut changed_library = empty_cloud_test_library_snapshot("changed-source");
    changed_library.items.push(LibraryItem {
        id: "changed-item".to_string(),
        title: "changed source item".to_string(),
        relative_path: "changed-item.mp4".to_string(),
        bytes: 3,
        width: None,
        height: None,
        codec: None,
        source_ids: Vec::new(),
        collection: None,
        attachments: Vec::new(),
        authoritative_nfo_path: None,
        confidence: Confidence::High,
        metadata_status: MetadataStatus::Complete,
        integrity_status: IntegrityStatus::Verified,
        presence: Presence::Present,
    });
    *runtime.library_snapshot.lock().await = Some(changed_library);
    runtime.state_notify.notify_one();
    let posts = wait_for_state_posts(&cloud_state, 4).await;
    assert_eq!(posts[3]["status"], 200);
    assert_eq!(posts[3]["revision"], "changed-source");
    assert_eq!(posts[3]["library_item_count"], 1);

    *runtime.library_snapshot.lock().await = Some(original_library.clone());
    runtime.state_notify.notify_one();
    let posts = wait_for_state_posts(&cloud_state, 5).await;
    assert_eq!(posts[4]["status"], 200);
    assert_eq!(posts[4]["revision"], "full-source");
    assert_eq!(posts[4]["library_item_count"], 2);
    assert!(posts[4]["state_version"].as_u64() > posts[3]["state_version"].as_u64());
    {
        let cached_library = runtime.library_snapshot.lock().await;
        let cached_library = cached_library
            .as_ref()
            .expect("full local library should remain cached");
        assert_eq!(cached_library.items.len(), original_library.items.len());
        assert_eq!(
            cached_library.items[0].title,
            original_library.items[0].title
        );
    }

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_reporter_advances_same_version_conflict_and_heartbeats_the_accepted_content() {
    let root = temp_cloud_bot_dir("state-reporter-version-conflict-same-version");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    queue_state_post_response(&cloud_state, 409, Some("1"));
    queue_state_post_response(&cloud_state, 200, None);
    queue_state_post_response(&cloud_state, 200, None);

    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    *runtime.library_snapshot.lock().await =
        Some(empty_cloud_test_library_snapshot("same-version-conflict"));
    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_millis(250),
        Duration::from_millis(100),
        Duration::from_millis(500),
    ));
    runtime.state_notify.notify_one();

    let first = wait_for_state_posts(&cloud_state, 1).await;
    assert_eq!(first[0]["status"], 409);
    assert_eq!(first[0]["state_version"], 1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        1,
        "a version conflict must be paced before allocating and posting a retry"
    );

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 2),
    )
    .await
    .expect("the state-version conflict should recover");
    assert_eq!(posts[1]["status"], 200);
    assert_eq!(posts[1]["state_version"], 2);
    assert_eq!(posts[0]["content_hash"], posts[1]["content_hash"]);
    assert_ne!(posts[0]["reported_at"], posts[1]["reported_at"]);

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 3),
    )
    .await
    .expect("the accepted state should continue heartbeating");
    assert_eq!(posts[2]["status"], 200);
    assert_eq!(posts[2]["state_version"], posts[1]["state_version"]);
    assert_eq!(posts[2]["content_hash"], posts[1]["content_hash"]);
    assert_ne!(posts[2]["reported_at"], posts[1]["reported_at"]);

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_reporter_recovers_after_lost_version_file_and_continues_after_restart() {
    let root = temp_cloud_bot_dir("state-reporter-version-file-lost");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    assert_eq!(
        inbox
            .next_state_version()
            .expect("initial state version should persist"),
        1
    );
    let version_path = config
        .downloads
        .video_dir
        .join(".telegram-video-downloader-cloud")
        .join(".state-version.json");
    std::fs::remove_file(&version_path)
        .expect("the test should simulate a lost local version file");

    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    queue_state_post_response(&cloud_state, 409, Some("50"));
    queue_state_post_response(&cloud_state, 200, None);

    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    *runtime.library_snapshot.lock().await =
        Some(empty_cloud_test_library_snapshot("remote-version"));
    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_secs(10),
        Duration::from_millis(100),
        Duration::from_millis(500),
    ));
    runtime.state_notify.notify_one();

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 2),
    )
    .await
    .expect("the reporter should advance beyond the remotely stored version");
    assert_eq!(posts[0]["state_version"], 1);
    assert_eq!(posts[1]["state_version"], 51);
    assert_eq!(posts[0]["content_hash"], posts[1]["content_hash"]);

    reporter.abort();
    let _ = reporter.await;
    drop(runtime);
    drop(inbox);

    let reopened_inbox = Arc::new(
        CloudInbox::open(&config.downloads.video_dir)
            .expect("cloud inbox should reopen with its version counter"),
    );
    let mut restarted_runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&reopened_inbox),
    );
    restarted_runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    *restarted_runtime.library_snapshot.lock().await =
        Some(empty_cloud_test_library_snapshot("after-restart"));
    let restarted_reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        restarted_runtime.clone(),
        Duration::from_secs(10),
        Duration::from_millis(100),
        Duration::from_millis(500),
    ));
    restarted_runtime.state_notify.notify_one();

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 3),
    )
    .await
    .expect("the reopened reporter should continue the persisted version sequence");
    assert_eq!(posts[2]["status"], 200);
    assert_eq!(posts[2]["state_version"], 52);
    assert_eq!(posts[2]["revision"], "after-restart");
    assert_ne!(posts[2]["content_hash"], posts[1]["content_hash"]);

    restarted_reporter.abort();
    let _ = restarted_reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(restarted_runtime);
    drop(reopened_inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_reporter_retries_unknown_conflicts_and_paces_version_exhaustion() {
    let root = temp_cloud_bot_dir("state-reporter-version-hint-invalid");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    queue_state_post_response(&cloud_state, 409, None);
    queue_state_post_response(&cloud_state, 409, Some("not-a-version"));
    queue_state_post_response(&cloud_state, 200, None);
    queue_state_post_response(
        &cloud_state,
        409,
        Some(&MAX_CLOUD_STATE_VERSION.to_string()),
    );
    queue_state_post_response(&cloud_state, 200, None);

    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    *runtime.library_snapshot.lock().await =
        Some(empty_cloud_test_library_snapshot("unknown-conflict"));
    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_millis(250),
        Duration::from_millis(100),
        Duration::from_millis(500),
    ));
    runtime.state_notify.notify_one();

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 1),
    )
    .await
    .expect("the initial state post should reach the fake server");
    assert_eq!(posts[0]["status"], 409);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        1,
        "a missing hint should retain the bounded retry delay"
    );

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 3),
    )
    .await
    .expect("missing and invalid version hints should retry the same state");
    for post in &posts[..3] {
        assert_eq!(post["state_version"], 1);
        assert_eq!(post["content_hash"], posts[0]["content_hash"]);
    }
    assert_eq!(posts[1]["status"], 409);
    assert_eq!(posts[2]["status"], 200);

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 4),
    )
    .await
    .expect("the heartbeat should exercise the maximum remote version hint");
    assert_eq!(posts[3]["status"], 409);
    assert_eq!(posts[3]["state_version"], 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        4,
        "a maximum remote hint must wait for the longer exhaustion backoff"
    );
    let version_path = config
        .downloads
        .video_dir
        .join(".telegram-video-downloader-cloud")
        .join(".state-version.json");
    let persisted: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&version_path).expect("local version should persist"),
    )
    .expect("local version should be valid JSON");
    assert_eq!(
        persisted["state_version"], 1,
        "an unincrementable remote hint must not poison the local counter"
    );

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 5),
    )
    .await
    .expect("the pending state should retry after the server version is repaired");
    assert_eq!(posts[4]["status"], 200);
    assert_eq!(posts[4]["state_version"], 1);
    assert_eq!(posts[4]["content_hash"], posts[0]["content_hash"]);

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_reporter_paces_when_local_state_version_is_exhausted() {
    let root = temp_cloud_bot_dir("state-reporter-local-version-exhausted");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    assert_eq!(
        inbox
            .next_state_version_after(MAX_CLOUD_STATE_VERSION - 1)
            .expect("the maximum safe version should persist"),
        MAX_CLOUD_STATE_VERSION
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    *runtime.library_snapshot.lock().await =
        Some(empty_cloud_test_library_snapshot("local-counter-exhausted"));
    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_secs(10),
        Duration::from_millis(50),
        Duration::from_millis(500),
    ));
    runtime.state_notify.notify_one();

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .is_empty(),
        "an exhausted local counter must not wrap or post a duplicate version"
    );
    let version_path = config
        .downloads
        .video_dir
        .join(".telegram-video-downloader-cloud")
        .join(".state-version.json");
    let persisted: serde_json::Value = serde_json::from_slice(
        &std::fs::read(version_path).expect("max local version should remain readable"),
    )
    .expect("max local version should remain valid JSON");
    assert_eq!(persisted["state_version"], MAX_CLOUD_STATE_VERSION);

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

fn queue_state_post_response(
    state: &Arc<FakeCloudApiState>,
    status: u16,
    version_header: Option<&str>,
) {
    state
        .state_post_statuses
        .lock()
        .expect("fake state status lock should not poison")
        .push(status);
    let headers = version_header
        .map(|version| vec![("X-Cloud-State-Version".to_string(), version.to_string())])
        .unwrap_or_default();
    state
        .state_post_response_headers
        .lock()
        .expect("fake state response-header lock should not poison")
        .push(headers);
}

fn cloud_test_request(seq: u64, kind: &str) -> CloudRequest {
    CloudRequest {
        seq,
        kind: kind.to_string(),
        payload: serde_json::json!({"text":""}),
        created_at: now_millis(),
        extra: serde_json::Map::new(),
    }
}
