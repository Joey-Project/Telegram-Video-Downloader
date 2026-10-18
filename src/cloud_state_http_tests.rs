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
async fn cloud_state_reporter_stops_retrying_413_when_no_smaller_snapshot_is_possible() {
    let root = temp_cloud_bot_dir("state-reporter-413-no-progress");
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
        .push(413);

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
    let mut library = empty_cloud_test_library_snapshot("single-large-item");
    library.items.push(LibraryItem {
        id: "only-item".to_string(),
        title: "x".repeat(128 * 1024),
        relative_path: "only-item.mp4".to_string(),
        bytes: 1,
        width: None,
        height: None,
        codec: None,
        source_ids: Vec::new(),
        collection: None,
        attachments: Vec::new(),
        confidence: Confidence::High,
        metadata_status: MetadataStatus::Complete,
        integrity_status: IntegrityStatus::Verified,
        presence: Presence::Present,
    });
    *runtime.library_snapshot.lock().await = Some(library);
    let reporter = tokio::spawn(cloud_state_reporter_with_heartbeat(
        runtime.clone(),
        Duration::from_millis(100),
    ));
    runtime.state_notify.notify_one();

    let posts = wait_for_state_posts(&cloud_state, 1).await;
    assert_eq!(posts[0]["status"], 413);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let posts = cloud_state
        .state_posts
        .lock()
        .expect("fake state post lock should not poison")
        .clone();
    assert_eq!(posts.len(), 1, "an unreducible source must not be retried");

    let version_path = config
        .downloads
        .video_dir
        .join(".telegram-video-downloader-cloud")
        .join(".state-version.json");
    let version: serde_json::Value =
        serde_json::from_slice(&std::fs::read(version_path).expect("state version should persist"))
            .expect("state version should be valid JSON");
    assert_eq!(
        version["state_version"], 1,
        "no retry version should be allocated"
    );

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
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
