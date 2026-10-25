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
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
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
    let viewer_id = "22222222-2222-4222-8222-222222222222".to_string();
    apply_viewer_interest(
        &runtime,
        crate::cloud::TEST_DATABASE_EPOCH,
        vec![CloudViewerInterest {
            viewer_id: viewer_id.clone(),
            surface: crate::cloud::CloudViewerSurface::File,
            watch: false,
            expires_at: now_millis() + crate::cloud::MAX_VIEWER_INTEREST_LIFETIME_MILLIS,
        }],
    )
    .await
    .expect("visible viewer should be eligible for an explicit refresh");
    let reporter = tokio::spawn(cloud_state_reporter(runtime.clone()));
    runtime.state_notify.notify_one();

    let posts = wait_for_state_posts(&cloud_state, 2).await;
    assert_eq!(posts[0]["wire_format"], "chunked-v1");
    assert_eq!(posts[0]["status"], 503);
    assert_eq!(posts[1]["status"], 200);
    assert_eq!(posts[0]["state_version"], posts[1]["state_version"]);
    assert_eq!(posts[0]["content_hash"], posts[1]["content_hash"]);
    assert_ne!(
        posts[0]["reported_at"], posts[1]["reported_at"],
        "a retry may refresh reported_at while preserving idempotent content"
    );

    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("updated"));
    let refresh = SnapshotRefreshRequest {
        viewer_id,
        request_id: "33333333-3333-4333-8333-333333333333".to_string(),
        database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
        reason: crate::cloud::SnapshotRefreshReason::Manual,
        scope: crate::cloud::SnapshotRefreshScope::Operations,
        library_id: None,
        expires_at: now_millis() + crate::cloud::MAX_SNAPSHOT_REFRESH_LIFETIME_MILLIS,
    };
    enqueue_snapshot_refresh(&runtime, refresh)
        .await
        .expect("manual refresh should admit the changed cached library revision");
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
async fn bootstrap_retries_after_transient_version_file_failure_without_viewers() {
    let root = temp_cloud_bot_dir("state-reporter-bootstrap-version-retry");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let version_path = config
        .downloads
        .video_dir
        .join(".telegram-video-downloader-cloud")
        .join("epochs")
        .join(crate::cloud::TEST_DATABASE_EPOCH)
        .join(".state-version.json");
    fs::write(&version_path, b"{ malformed")
        .expect("test should inject a temporary malformed local version record");
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
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
    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("bootstrap"));
    assert!(runtime.viewer_interests.lock().await.is_empty());
    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_secs(60 * 60),
        Duration::from_millis(100),
        Duration::from_millis(200),
    ));

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .is_empty(),
        "the transient version-file failure should happen before any state post"
    );
    fs::remove_file(&version_path).expect("test should repair the temporary version record");
    runtime.state_notify.notify_one();
    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 1),
    )
    .await
    .expect("unconsumed bootstrap should retry after the version-file repair");
    assert_eq!(posts[0]["status"], 200);
    assert_eq!(posts[0]["revision"], "bootstrap");
    assert!(runtime.viewer_interests.lock().await.is_empty());

    reporter.abort();
    let _ = reporter.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn cloud_state_retry_backoff_grows_to_a_cap_and_resets_after_success() {
    let base = Duration::from_secs(5);
    let cap = Duration::from_secs(300);
    let mut backoff = CloudStateRetryBackoff::new(base, cap);
    let expected_ceilings = [5, 10, 20, 40, 80, 160, 300, 300];

    for seconds in expected_ceilings {
        let ceiling = backoff.ceiling();
        assert_eq!(ceiling, Duration::from_secs(seconds));
        let delay = backoff.next_delay();
        assert!(
            delay >= ceiling / 2,
            "jitter must not collapse to a tight retry"
        );
        assert!(
            delay <= ceiling,
            "jitter must stay below the current ceiling"
        );
    }

    backoff.reset();

    assert_eq!(backoff.ceiling(), base);
    let first_retry_after_reset = backoff.next_delay();
    assert!(first_retry_after_reset >= base / 2);
    assert!(first_retry_after_reset <= base);
}

#[test]
fn effective_cloud_idle_reconciliation_deadline_is_fixed_at_one_hour() {
    let now = Instant::now();
    assert_eq!(
        CLOUD_IDLE_RECONCILIATION_INTERVAL,
        Duration::from_secs(3600)
    );
    assert_eq!(
        remaining_cloud_reconciliation_delay(None, now),
        Duration::ZERO,
        "a never-successful runtime must reconcile immediately"
    );
    assert_eq!(
        remaining_cloud_reconciliation_delay(Some(now), now),
        Duration::from_secs(3600)
    );
    assert_eq!(
        remaining_cloud_reconciliation_delay(Some(now), now + Duration::from_secs(1800)),
        Duration::from_secs(1800)
    );
    assert_eq!(
        remaining_cloud_reconciliation_delay(Some(now), now + Duration::from_secs(3600)),
        Duration::ZERO
    );
}

#[tokio::test]
async fn failed_idle_backlog_reads_use_exponential_backoff_without_advancing_success_time() {
    let root = temp_cloud_bot_dir("idle-backlog-read-backoff");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    cloud_state
        .fetch_statuses
        .lock()
        .expect("fake fetch-status lock should not poison")
        .extend([503, 503, 200]);
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
    let base = Duration::from_millis(200);
    let fallback = tokio::spawn(cloud_rest_fallback_with_retry_policy(
        runtime.clone(),
        base,
        Duration::from_millis(800),
    ));

    wait_for_mock_fetch_count(&cloud_state, 1).await;
    assert!(runtime.last_backlog_reconciled_at.lock().await.is_none());

    wait_for_mock_fetch_count(&cloud_state, 2).await;
    let fetch_times = cloud_state
        .fetch_times
        .lock()
        .expect("fake fetch-time lock should not poison")
        .clone();
    let first_retry = fetch_times[1].duration_since(fetch_times[0]);
    assert!(
        first_retry >= base / 2,
        "the first retry should be jitter-paced"
    );
    assert!(runtime.last_backlog_reconciled_at.lock().await.is_none());

    wait_for_mock_fetch_count(&cloud_state, 3).await;
    let fetch_times = cloud_state
        .fetch_times
        .lock()
        .expect("fake fetch-time lock should not poison")
        .clone();
    let second_retry = fetch_times[2].duration_since(fetch_times[1]);
    assert!(
        second_retry >= base,
        "the second retry should use the doubled delay"
    );
    wait_for_idle_backlog_reconciliation(&runtime, &cloud_state, 3, None).await;
    println!(
        "failed idle GET backoff metrics: GETs=3 (2 failures +1 success), retry gaps={}ms and{}ms",
        first_retry.as_millis(),
        second_retry.as_millis()
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        cloud_state.fetch_count.load(AtomicOrdering::SeqCst),
        3,
        "one successful reconciliation should return fallback to the hourly deadline"
    );

    fallback.abort();
    let _ = fallback.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_retry_backoff_grows_resets_and_ignores_notifications_while_pending() {
    let root = temp_cloud_bot_dir("state-reporter-backoff-reset");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    cloud_state
        .state_post_statuses
        .lock()
        .expect("fake state status lock should not poison")
        .extend([503, 503, 200, 503, 200]);

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
    let retry_base = Duration::from_millis(120);
    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_secs(30),
        retry_base,
        Duration::from_secs(1),
    ));
    runtime.state_notify.notify_one();

    let posts = wait_for_state_posts(&cloud_state, 1).await;
    assert_eq!(posts[0]["status"], 503);
    runtime.state_notify.notify_one();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        1,
        "state changes must not bypass a pending delivery's retry delay"
    );

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 3),
    )
    .await
    .expect("the pending content should retry and recover");
    for post in &posts[..3] {
        assert_eq!(post["state_version"], posts[0]["state_version"]);
        assert_eq!(post["content_hash"], posts[0]["content_hash"]);
    }
    assert_eq!(posts[1]["status"], 503);
    assert_eq!(posts[2]["status"], 200);
    let first_retry_delay =
        posts[1]["reported_at"].as_u64().unwrap() - posts[0]["reported_at"].as_u64().unwrap();
    let second_retry_delay =
        posts[2]["reported_at"].as_u64().unwrap() - posts[1]["reported_at"].as_u64().unwrap();
    assert!(first_retry_delay >= (retry_base / 2).as_millis() as u64);
    assert!(second_retry_delay >= retry_base.as_millis() as u64);

    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("updated"));
    enqueue_test_operations_refresh(&runtime, "33333333-3333-4333-8333-000000000030")
        .await
        .expect("explicit refresh should admit the later cached revision");
    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 5),
    )
    .await
    .expect("the changed snapshot should be delivered after the prior success");
    assert_eq!(posts[3]["status"], 503);
    assert_eq!(posts[3]["revision"], "updated");
    assert_eq!(posts[4]["status"], 200);
    assert_eq!(posts[3]["state_version"], posts[4]["state_version"]);
    assert_eq!(posts[3]["content_hash"], posts[4]["content_hash"]);
    let delay_after_success =
        posts[4]["reported_at"].as_u64().unwrap() - posts[3]["reported_at"].as_u64().unwrap();
    assert!(
        delay_after_success >= (retry_base / 2).as_millis() as u64,
        "the retry after a successful delivery should use the reset first-step range"
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
async fn cloud_state_reporter_retries_bad_request_with_fresh_timestamp_and_backoff() {
    let root = temp_cloud_bot_dir("state-reporter-clock-skew");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    queue_state_post_response(&cloud_state, 400, None);
    queue_state_post_response(&cloud_state, 400, None);
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
        Some(empty_cloud_test_library_snapshot("clock-skew-retry"));
    let retry_backoff = Duration::from_millis(250);
    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_secs(30),
        retry_backoff,
        Duration::from_secs(1),
    ));
    runtime.state_notify.notify_one();

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 3),
    )
    .await
    .expect("HTTP 400 should remain retryable so a corrected clock can recover");
    assert_eq!(posts[0]["status"], 400);
    assert_eq!(posts[1]["status"], 400);
    assert_eq!(posts[2]["status"], 200);
    assert_eq!(posts[0]["state_version"], posts[1]["state_version"]);
    assert_eq!(posts[1]["state_version"], posts[2]["state_version"]);
    assert_eq!(posts[0]["content_hash"], posts[1]["content_hash"]);
    assert_eq!(posts[1]["content_hash"], posts[2]["content_hash"]);

    let first_reported_at = posts[0]["reported_at"]
        .as_u64()
        .expect("first post should include a millisecond timestamp");
    let second_reported_at = posts[1]["reported_at"]
        .as_u64()
        .expect("retry should include a millisecond timestamp");
    let third_reported_at = posts[2]["reported_at"]
        .as_u64()
        .expect("successful retry should include a millisecond timestamp");
    assert_ne!(first_reported_at, second_reported_at);
    assert_ne!(second_reported_at, third_reported_at);
    assert!(
        first_reported_at.abs_diff(second_reported_at) >= 100,
        "repeated HTTP 400 responses should respect the retry backoff"
    );
    assert!(
        second_reported_at.abs_diff(third_reported_at) >= 100,
        "a stable HTTP 400 should not trigger a tight retry loop"
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
async fn cloud_state_reporter_publishes_scan_failure_with_cached_snapshot_then_clears_it() {
    let root = temp_cloud_bot_dir("state-reporter-scan-warning");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
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
        organization_status: None,
        attachments: Vec::new(),
        authoritative_nfo_path: None,
        confidence: Confidence::High,
        metadata_status: MetadataStatus::Complete,
        integrity_status: IntegrityStatus::Verified,
        presence: Presence::Present,
        physical_identity: None,
    });
    cached_snapshot.warnings = vec!["existing library warning".to_string()];
    let mut unavailable_projection =
        serde_json::to_value(&cached_snapshot).expect("cached library snapshot should serialize");
    for item in unavailable_projection["items"]
        .as_array_mut()
        .expect("cached snapshot items should be an array")
    {
        item["presence"] = serde_json::Value::String("unavailable".to_string());
    }
    let unavailable_projection_hash = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(
                unavailable_projection["items"]
                    .as_array()
                    .expect("unavailable projected items should remain an array")
            )
            .expect("unavailable projected items should serialize"),
        )
    );
    *runtime.library_snapshot.lock().await = Some(cached_snapshot.clone());

    let reporter = tokio::spawn(cloud_state_reporter_with_heartbeat(
        runtime.clone(),
        Duration::from_secs(300),
    ));
    runtime.state_notify.notify_one();
    let posts = wait_for_state_posts(&cloud_state, 1).await;
    assert_eq!(posts[0]["status"], 200);
    assert_eq!(
        posts[0]["library_descriptor_status"][0]["has_nested_snapshot"], false,
        "the top-level default snapshot should not be serialized a second time in its descriptor"
    );
    assert_eq!(posts[0]["library_descriptor_status"][0]["media_count"], 1);
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
    enqueue_test_operations_refresh(&runtime, "44444444-4444-4444-8444-444444444444")
        .await
        .expect("manual refresh should publish the retained unavailable projection");
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
        posts[1]["library_items_hash"], unavailable_projection_hash,
        "the cached media metadata should remain intact while unavailable presence is projected"
    );
    assert_ne!(
        posts[1]["library_items_hash"], posts[0]["library_items_hash"],
        "the unavailable projection should mark cached items as unavailable"
    );
    assert_eq!(posts[1]["library_warning_count"], 2);
    assert_eq!(posts[1]["library_incomplete"], true);
    assert_eq!(
        posts[1]["library_descriptor_status"][0]["availability"],
        "unavailable"
    );
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
    enqueue_test_operations_refresh(&runtime, "55555555-5555-4555-8555-555555555555")
        .await
        .expect("manual refresh should publish the recovered cached projection");
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
    assert_eq!(
        posts[2]["library_descriptor_status"][0]["availability"],
        "available"
    );
    assert_eq!(
        posts[2]["library_descriptor_status"][0]["has_nested_snapshot"],
        false
    );
    assert!(
        posts[2]["library_descriptor_status"][0]["free_bytes"]
            .as_u64()
            .is_some()
    );
    let initial_field_hashes = posts[0]["state_field_hashes"]
        .as_object()
        .expect("initial state field hashes should be an object");
    let recovered_field_hashes = posts[2]["state_field_hashes"]
        .as_object()
        .expect("recovered state field hashes should be an object");
    let changed_fields = initial_field_hashes
        .iter()
        .filter(|(key, value)| recovered_field_hashes.get(*key) != Some(*value))
        .map(|(key, _)| key.as_str())
        .collect::<Vec<_>>();
    assert!(
        changed_fields.is_empty() || (changed_fields.len() == 1 && changed_fields[0] == "library"),
        "recovery may only refresh the library component's free-space metadata; changed fields: {changed_fields:?}"
    );
    assert_eq!(
        posts[2]["stable_content_hash"], posts[0]["stable_content_hash"],
        "all state content other than dynamic per-library free-space metadata should match"
    );
    let mut initial_descriptor_status = posts[0]["library_descriptor_status"].clone();
    let mut recovered_descriptor_status = posts[2]["library_descriptor_status"].clone();
    for descriptors in [
        &mut initial_descriptor_status,
        &mut recovered_descriptor_status,
    ] {
        for descriptor in descriptors
            .as_array_mut()
            .expect("descriptor status should be an array")
        {
            descriptor
                .as_object_mut()
                .expect("descriptor status should be an object")
                .remove("free_bytes");
        }
    }
    assert_eq!(
        recovered_descriptor_status, initial_descriptor_status,
        "availability and all stable per-library descriptor fields should recover unchanged"
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
async fn cloud_state_reporter_retries_413_with_new_incomplete_snapshot_then_syncs_changes() {
    let root = temp_cloud_bot_dir("state-reporter-413");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
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
            organization_status: None,
            attachments: Vec::new(),
            authoritative_nfo_path: None,
            confidence: Confidence::High,
            metadata_status: MetadataStatus::Complete,
            integrity_status: IntegrityStatus::Verified,
            presence: Presence::Present,
            physical_identity: None,
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
    assert!(posts[3]["state_version"].as_u64() > posts[2]["state_version"].as_u64());
    assert_eq!(
        posts[3]["library_item_count"], 8,
        "the expiry attempt rebuilds from the retained full local source"
    );
    assert_ne!(posts[3]["content_hash"], posts[2]["content_hash"]);
    assert_ne!(posts[3]["reported_at"], posts[2]["reported_at"]);

    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("after-413"));
    enqueue_test_operations_refresh(&runtime, "33333333-3333-4333-8333-000000000413")
        .await
        .expect("explicit refresh should publish the changed source after 413 recovery");
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
async fn irreducible_413_is_paced_but_explicit_refresh_bypasses_failure_cooldown() {
    let root = temp_cloud_bot_dir("state-reporter-413-admission-cooldown");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    cloud_state
        .state_post_statuses
        .lock()
        .expect("fake state status lock should not poison")
        .extend([413, 413, 413]);

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
    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("irreducible"));
    let (writer, mut results) = mpsc::channel(4);
    *runtime.websocket_writer.lock().await = Some(writer);
    let viewer_id = "22222222-2222-4222-8222-222222222222".to_string();
    apply_viewer_interest(
        &runtime,
        crate::cloud::TEST_DATABASE_EPOCH,
        vec![CloudViewerInterest {
            viewer_id: viewer_id.clone(),
            surface: crate::cloud::CloudViewerSurface::File,
            watch: false,
            expires_at: now_millis() + crate::cloud::MAX_VIEWER_INTEREST_LIFETIME_MILLIS,
        }],
    )
    .await
    .expect("viewer should be eligible for an explicit refresh");

    let expiry = Duration::from_secs(2);
    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        expiry,
        Duration::from_millis(20),
        Duration::from_millis(100),
    ));
    let posts = wait_for_state_posts(&cloud_state, 1).await;
    assert_eq!(posts[0]["status"], 413);
    assert_eq!(
        inbox.last_successful_publication_at().unwrap(),
        None,
        "a rejected snapshot must not be recorded as a successful expiry refresh"
    );

    runtime.state_notify.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        1,
        "an unchanged local notification must not spin another irreducible post"
    );

    let request = SnapshotRefreshRequest {
        viewer_id,
        request_id: "33333333-3333-4333-8333-333333333333".to_string(),
        database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
        reason: crate::cloud::SnapshotRefreshReason::Manual,
        scope: crate::cloud::SnapshotRefreshScope::Operations,
        library_id: None,
        expires_at: now_millis() + crate::cloud::MAX_SNAPSHOT_REFRESH_LIFETIME_MILLIS,
    };
    enqueue_snapshot_refresh(&runtime, request.clone())
        .await
        .expect("explicit manual refresh should queue during cooldown");
    runtime.state_notify.notify_one();
    let posts = tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_state_posts(&cloud_state, 2),
    )
    .await
    .expect("explicit refresh should bypass the admission cooldown");
    assert_eq!(posts[1]["status"], 413);
    assert_eq!(posts[1]["snapshot_notify_viewers"], "1");
    let result = tokio::time::timeout(Duration::from_secs(1), results.recv())
        .await
        .expect("failed explicit refresh should return a result")
        .expect("refresh result channel should stay open");
    assert_eq!(result["request_id"], request.request_id);
    assert_eq!(result["status"], "failed");
    assert_eq!(result["error_code"], "payload_too_large");

    runtime.state_notify.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        2,
        "local notifications after an explicit rejection must remain paced too"
    );
    let posts = tokio::time::timeout(
        Duration::from_secs(4),
        wait_for_state_posts(&cloud_state, 3),
    )
    .await
    .expect("the failed admission should retry once at the expiry deadline");
    assert_eq!(posts[2]["status"], 413);
    assert!(
        posts[2]["state_version"].as_u64() > posts[1]["state_version"].as_u64(),
        "the bounded expiry attempt should allocate a fresh version"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        3,
        "a repeated irreducible expiry rejection should establish a new bounded cooldown"
    );
    assert_eq!(inbox.last_successful_publication_at().unwrap(), None);

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
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
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
    *runtime.library_snapshot.lock().await = Some(metadata_budget_library_snapshot(0));
    enqueue_test_operations_refresh(&runtime, "33333333-3333-4333-8333-000000000001")
        .await
        .expect("first explicit refresh should admit the baseline metadata fixture");
    let reporter = tokio::spawn(cloud_state_reporter_with_heartbeat(
        runtime.clone(),
        Duration::from_secs(300),
    ));

    let mut expected_posts = 0;
    for item_count in [0_usize, 1, 3] {
        if item_count > 0 {
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
                    organization_status: None,
                    attachments: Vec::new(),
                    authoritative_nfo_path: None,
                    confidence: Confidence::High,
                    metadata_status: MetadataStatus::Complete,
                    integrity_status: IntegrityStatus::Verified,
                    presence: Presence::Present,
                    physical_identity: None,
                })
                .collect();
            *runtime.library_snapshot.lock().await = Some(snapshot);
            enqueue_test_operations_refresh(
                &runtime,
                &format!("33333333-3333-4333-8333-{:012x}", item_count + 1),
            )
            .await
            .expect("explicit refresh should publish each changed metadata fixture");
        }

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
    enqueue_test_operations_refresh(&runtime, "33333333-3333-4333-8333-000000001000")
        .await
        .expect("explicit refresh should publish the post-compaction revision");
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
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
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
    assert!(posts[2]["state_version"].as_u64() > posts[1]["state_version"].as_u64());
    assert_ne!(posts[2]["content_hash"], posts[1]["content_hash"]);
    assert_eq!(posts[2]["operation_count"], 8);
    assert!(
        posts[2]["operation_bytes"].as_u64().unwrap_or_default()
            >= posts[0]["operation_bytes"].as_u64().unwrap_or_default(),
        "the hourly expiry rebuild should start from the unmodified local receipts"
    );
    assert_ne!(posts[2]["reported_at"], posts[1]["reported_at"]);

    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot(
        "updated-after-compaction",
    ));
    enqueue_test_operations_refresh(&runtime, "33333333-3333-4333-8333-000000001413")
        .await
        .expect("explicit refresh should publish the changed cached library revision");
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
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
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
        organization_status: None,
        attachments: Vec::new(),
        authoritative_nfo_path: None,
        confidence: Confidence::High,
        metadata_status: MetadataStatus::Complete,
        integrity_status: IntegrityStatus::Verified,
        presence: Presence::Present,
        physical_identity: None,
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
        organization_status: None,
        attachments: Vec::new(),
        authoritative_nfo_path: None,
        confidence: Confidence::High,
        metadata_status: MetadataStatus::Complete,
        integrity_status: IntegrityStatus::Verified,
        presence: Presence::Present,
        physical_identity: None,
    });
    let original_library = library.clone();
    *runtime.library_snapshot.lock().await = Some(original_library.clone());
    let reporter = tokio::spawn(cloud_state_reporter_with_heartbeat(
        runtime.clone(),
        Duration::from_secs(300),
    ));
    runtime.state_notify.notify_one();

    let posts = wait_for_state_posts(&cloud_state, 2).await;
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
        organization_status: None,
        attachments: Vec::new(),
        authoritative_nfo_path: None,
        confidence: Confidence::High,
        metadata_status: MetadataStatus::Complete,
        integrity_status: IntegrityStatus::Verified,
        presence: Presence::Present,
        physical_identity: None,
    });
    *runtime.library_snapshot.lock().await = Some(changed_library);
    enqueue_test_operations_refresh(&runtime, "33333333-3333-4333-8333-000000000004")
        .await
        .expect("explicit refresh should publish the changed local source");
    let posts = wait_for_state_posts(&cloud_state, 3).await;
    assert_eq!(posts[2]["status"], 200);
    assert_eq!(posts[2]["revision"], "changed-source");
    assert_eq!(posts[2]["library_item_count"], 1);

    *runtime.library_snapshot.lock().await = Some(original_library.clone());
    enqueue_test_operations_refresh(&runtime, "33333333-3333-4333-8333-000000000005")
        .await
        .expect("explicit refresh should restore the retained full local source");
    let posts = wait_for_state_posts(&cloud_state, 4).await;
    assert_eq!(posts[3]["status"], 200);
    assert_eq!(posts[3]["revision"], "full-source");
    assert_eq!(posts[3]["library_item_count"], 2);
    assert!(posts[3]["state_version"].as_u64() > posts[2]["state_version"].as_u64());
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
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
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
    let chunk_uploads_after_conflict = cloud_state.state_chunk_posts.load(AtomicOrdering::SeqCst);
    assert!(chunk_uploads_after_conflict > 0);
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
    assert_eq!(
        cloud_state.state_chunk_posts.load(AtomicOrdering::SeqCst),
        chunk_uploads_after_conflict,
        "version rebases must reuse durable content-addressed chunks"
    );

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 3),
    )
    .await
    .expect("the accepted state should continue heartbeating");
    assert_eq!(posts[2]["status"], 200);
    assert!(posts[2]["state_version"].as_u64() > posts[1]["state_version"].as_u64());
    assert_eq!(posts[2]["revision"], posts[1]["revision"]);
    assert!(posts[2]["content_hash"].as_str().is_some());
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
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    assert_eq!(
        inbox
            .next_state_version()
            .expect("initial state version should persist"),
        1
    );
    let version_path = runtime_test_inbox_directory(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
    )
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
        open_runtime_test_inbox(&config.downloads.video_dir)
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
    enqueue_test_operations_refresh(&restarted_runtime, "33333333-3333-4333-8333-000000000052")
        .await
        .expect("explicit refresh should publish the changed post-restart revision");
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
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
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
    let retry_backoff = Duration::from_millis(100);
    let version_exhausted_backoff = Duration::from_millis(500);
    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_millis(250),
        retry_backoff,
        version_exhausted_backoff,
    ));
    runtime.state_notify.notify_one();

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 1),
    )
    .await
    .expect("the initial state post should reach the fake server");
    assert_eq!(posts[0]["status"], 409);

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
    for (earlier, later) in [(0, 1), (1, 2)] {
        let earlier_reported_at = posts[earlier]["reported_at"]
            .as_u64()
            .expect("reported_at should be Unix milliseconds");
        let later_reported_at = posts[later]["reported_at"]
            .as_u64()
            .expect("reported_at should be Unix milliseconds");
        assert!(
            later_reported_at.abs_diff(earlier_reported_at)
                >= (retry_backoff / 2).as_millis() as u64,
            "unknown conflicts must wait at least half of the current jitter ceiling"
        );
    }

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 4),
    )
    .await
    .expect("the heartbeat should exercise the maximum remote version hint");
    assert_eq!(posts[3]["status"], 409);
    assert!(posts[3]["state_version"].as_u64() > posts[2]["state_version"].as_u64());
    let version_path = runtime_test_inbox_directory(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
    )
    .join(".state-version.json");
    let persisted: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&version_path).expect("local version should persist"),
    )
    .expect("local version should be valid JSON");
    assert_eq!(
        persisted["state_version"], posts[3]["state_version"],
        "an unincrementable remote hint must not poison the local counter"
    );

    let posts = tokio::time::timeout(
        Duration::from_secs(3),
        wait_for_state_posts(&cloud_state, 5),
    )
    .await
    .expect("the pending state should retry after the server version is repaired");
    assert_eq!(posts[4]["status"], 200);
    assert_eq!(posts[4]["state_version"], posts[3]["state_version"]);
    assert_eq!(posts[4]["content_hash"], posts[3]["content_hash"]);
    let exhausted_at = posts[3]["reported_at"]
        .as_u64()
        .expect("reported_at should be Unix milliseconds");
    let retried_at = posts[4]["reported_at"]
        .as_u64()
        .expect("reported_at should be Unix milliseconds");
    assert!(
        retried_at.abs_diff(exhausted_at) >= version_exhausted_backoff.as_millis() as u64,
        "maximum remote version hints must use the longer exhaustion backoff"
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
async fn cloud_state_reporter_paces_when_local_state_version_is_exhausted() {
    let root = temp_cloud_bot_dir("state-reporter-local-version-exhausted");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
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
    let version_path = runtime_test_inbox_directory(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
    )
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

#[tokio::test]
async fn cloud_state_reporter_stops_when_remote_database_epoch_changes() {
    let root = temp_cloud_bot_dir("state-reporter-epoch-change");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    cloud_state
        .state_post_statuses
        .lock()
        .expect("fake state status lock should not poison")
        .push(409);
    cloud_state
        .state_post_response_headers
        .lock()
        .expect("fake state response-header lock should not poison")
        .push(vec![(
            "X-Cloud-Database-Epoch".to_string(),
            "fedcba9876543210fedcba9876543210".to_string(),
        )]);

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
    runtime
        .library_snapshot
        .lock()
        .await
        .replace(empty_cloud_test_library_snapshot("epoch-change"));

    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_secs(60),
        Duration::from_millis(20),
        Duration::from_millis(100),
    ));
    runtime.state_notify.notify_one();

    let posts = wait_for_state_posts(&cloud_state, 1).await;
    assert_eq!(posts[0]["status"], 409);
    tokio::time::timeout(Duration::from_secs(2), reporter)
        .await
        .expect("reporter should stop after an epoch mismatch")
        .expect("reporter task should join");
    assert!(runtime.epoch_fatal.is_signaled());
    assert!(!runtime.reconciliation_complete.load(Ordering::Acquire));
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        1,
        "a fatal epoch mismatch must not retry state posts on the old runtime"
    );

    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_state_chunk_cache_retries_partial_uploads_and_clears_after_manifest_428() {
    let root = temp_cloud_bot_dir("state-chunk-cache-recovery");
    let (cloud_base_url, cloud_server_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(root.clone(), Vec::new()).await;
    cloud_server_state
        .state_chunk_statuses
        .lock()
        .expect("fake chunk-status lock should not poison")
        .extend([200, 503, 200]);
    let client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    let library = serde_json::json!({
        "revision": "chunk-cache-library",
        "scanned_at": 1,
        "items": [{"id": "chunk-cache-item", "title": "x".repeat(40_000)}],
        "categories": [],
        "warnings": [],
    });
    let state = direct_chunk_test_state(&library);

    let first = client.post_state(&state).await;
    assert!(matches!(
        first,
        Err(CloudStatePostError::HttpStatus(status))
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE
    ));
    let first_chunk_upload_count = cloud_server_state
        .state_chunk_posts
        .load(AtomicOrdering::SeqCst);
    assert_eq!(
        first_chunk_upload_count, 3,
        "fixture should span three distinct chunks"
    );
    assert!(
        cloud_server_state
            .state_posts
            .lock()
            .expect("fake state-post lock should not poison")
            .is_empty(),
        "a failed chunk must prevent manifest publication"
    );

    client
        .post_state(&state)
        .await
        .expect("retry should reuse successful chunks and upload the missing chunk");
    assert_eq!(
        cloud_server_state
            .state_chunk_posts
            .load(AtomicOrdering::SeqCst),
        first_chunk_upload_count + 1
    );
    assert_eq!(
        cloud_server_state.state_posts.lock().unwrap()[0]["wire_format"],
        "chunked-v1"
    );

    cloud_server_state
        .state_chunks
        .lock()
        .expect("fake stored-chunk lock should not poison")
        .clear();
    let missing_chunks = client.post_state(&state).await;
    assert!(matches!(
        missing_chunks,
        Err(CloudStatePostError::HttpStatus(status))
            if status == reqwest::StatusCode::PRECONDITION_REQUIRED
    ));
    assert!(
        CloudStatePostError::HttpStatus(reqwest::StatusCode::PRECONDITION_REQUIRED).is_retryable()
    );
    assert_eq!(
        cloud_server_state
            .state_chunk_posts
            .load(AtomicOrdering::SeqCst),
        first_chunk_upload_count + 1,
        "the client should observe missing chunks before reuploading"
    );

    client
        .post_state(&state)
        .await
        .expect("428 must clear the epoch cache so the next attempt restores chunks");
    assert_eq!(
        cloud_server_state
            .state_chunk_posts
            .load(AtomicOrdering::SeqCst),
        first_chunk_upload_count + 1 + 3
    );
    assert_eq!(
        cloud_server_state
            .state_posts
            .lock()
            .unwrap()
            .last()
            .unwrap()["status"],
        200
    );

    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    let _ = std::fs::remove_dir_all(root);
}

fn direct_chunk_test_state(library: &serde_json::Value) -> CloudState<'_> {
    CloudState {
        state_version: 1,
        revision: Some("chunk-cache-state"),
        reported_at: now_millis(),
        library,
        previews: &[],
        operations: &[],
        tasks: &[],
        settings: CloudSettings {
            download_dir: "/media",
            applied: None,
        },
    }
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

fn persist_test_operation_result(inbox: &CloudInbox, seq: u64, summary: &str) {
    if inbox
        .record(seq)
        .expect("test operation record should be readable")
        .is_none()
    {
        inbox
            .persist(CloudRequest {
                seq,
                kind: "test_operation".to_string(),
                payload: serde_json::json!({}),
                created_at: now_millis(),
                extra: serde_json::Map::new(),
            })
            .expect("test operation should persist");
    }
    inbox
        .update(seq, |record| {
            record.status = InboxStatus::Completed;
            record.result = Some(serde_json::json!({"summary": summary}));
        })
        .expect("test operation result should persist");
}

async fn enqueue_test_operations_refresh(
    runtime: &CloudBotRuntime,
    request_id: &str,
) -> Result<()> {
    let viewer_id = "22222222-2222-4222-8222-222222222222".to_string();
    let now = now_millis();
    apply_viewer_interest(
        runtime,
        crate::cloud::TEST_DATABASE_EPOCH,
        vec![CloudViewerInterest {
            viewer_id: viewer_id.clone(),
            surface: crate::cloud::CloudViewerSurface::File,
            watch: false,
            expires_at: now + crate::cloud::MAX_VIEWER_INTEREST_LIFETIME_MILLIS,
        }],
    )
    .await?;
    enqueue_snapshot_refresh(
        runtime,
        SnapshotRefreshRequest {
            viewer_id,
            request_id: request_id.to_string(),
            database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
            reason: crate::cloud::SnapshotRefreshReason::Manual,
            scope: crate::cloud::SnapshotRefreshScope::Operations,
            library_id: None,
            expires_at: now + crate::cloud::MAX_SNAPSHOT_REFRESH_LIFETIME_MILLIS,
        },
    )
    .await?;
    runtime.state_notify.notify_one();
    Ok(())
}

fn metadata_budget_library_snapshot(item_count: usize) -> LibrarySnapshot {
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
            organization_status: None,
            attachments: Vec::new(),
            authoritative_nfo_path: None,
            confidence: Confidence::High,
            metadata_status: MetadataStatus::Complete,
            integrity_status: IntegrityStatus::Verified,
            presence: Presence::Present,
            physical_identity: None,
        })
        .collect();
    snapshot
}

async fn wait_for_reported_publication(
    runtime: &CloudBotRuntime,
    cloud_state: &Arc<FakeCloudApiState>,
    index: usize,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let reported_at = cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .get(index)
            .and_then(|post| post["reported_at"].as_u64());
        if let Some(reported_at) = reported_at
            && last_successful_cloud_publication_at(runtime).await.ok() == Some(Some(reported_at))
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "manifest {index} should complete its HTTP acknowledgement and local expiry bookkeeping"
        );
        tokio::task::yield_now().await;
    }
}

async fn wait_for_idle_backlog_reconciliation(
    runtime: &CloudBotRuntime,
    cloud_state: &Arc<FakeCloudApiState>,
    expected_fetches: usize,
    previous: Option<Instant>,
) -> Instant {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let fetches = cloud_state.fetch_count.load(AtomicOrdering::SeqCst);
        let last_successful = *runtime.last_backlog_reconciled_at.lock().await;
        if fetches >= expected_fetches
            && last_successful.is_some_and(|current| previous.is_none_or(|old| current > old))
        {
            return last_successful.expect("successful reconciliation marker should exist");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "hourly authoritative GET should finish its real HTTP and queue bookkeeping work"
        );
        tokio::task::yield_now().await;
    }
}

async fn wait_for_mock_fetch_count(state: &Arc<FakeCloudApiState>, expected: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if state.fetch_count.load(AtomicOrdering::SeqCst) >= expected {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "mock Worker should receive fetch {expected}"
        );
        tokio::task::yield_now().await;
    }
}

#[test]
fn watched_operation_fingerprint_ignores_library_capacity_and_inventory_noise() {
    let mut snapshot = CloudStateSnapshot {
        revision: "revision-1".to_string(),
        library: serde_json::json!({
            "free_bytes": 100,
            "availability": "available",
            "items": [{"id":"a"}]
        }),
        previews: Vec::new(),
        operations: vec![serde_json::json!({"seq": 7, "status":"completed"})],
        tasks: vec![serde_json::json!({"id":"task-1", "status":"running"})],
        download_dir: "/tmp/videos".to_string(),
        settings_applied: serde_json::json!({"default_library_id":"videos"}),
    };
    let baseline = snapshot
        .watch_fingerprint()
        .expect("operation fingerprint should compute");

    snapshot.library["free_bytes"] = serde_json::json!(200);
    snapshot.library["availability"] = serde_json::json!("offline");
    snapshot.library["items"] = serde_json::json!([]);
    snapshot.revision = "revision-2".to_string();
    assert_eq!(
        snapshot.watch_fingerprint().unwrap(),
        baseline,
        "capacity and media-library inventory are not operation-watch changes"
    );

    snapshot
        .operations
        .push(serde_json::json!({"seq": 8, "status":"completed"}));
    assert_ne!(snapshot.watch_fingerprint().unwrap(), baseline);
}

#[tokio::test]
async fn restart_watch_and_disconnect_gate_semantic_snapshot_publication() {
    let root = temp_cloud_bot_dir("snapshot-watch-restart-disconnect");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let initial_inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    let initial_runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&initial_inbox),
    );
    *initial_runtime.library_snapshot.lock().await =
        Some(empty_cloud_test_library_snapshot("cached"));
    let baseline = build_cloud_state_snapshot(&initial_runtime)
        .await
        .expect("baseline state should build")
        .expect("local state should exist")
        .watch_fingerprint()
        .expect("baseline operations should fingerprint");
    initial_inbox
        .record_successful_publication_at(now_millis(), &baseline)
        .expect("previous publication baseline should persist");
    drop(initial_runtime);
    drop(initial_inbox);

    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir)
            .expect("persisted cloud inbox should reopen after restart"),
    );
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
    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("cached"));

    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_secs(60 * 60),
        Duration::from_millis(20),
        Duration::from_millis(100),
    ));
    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot(
        "capacity-noise-without-viewer",
    ));
    runtime.state_notify.notify_one();
    persist_test_operation_result(&inbox, 710, "completed while no viewer watched");
    runtime.state_notify.notify_one();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        0,
        "local receipt changes without a visible watcher must stay local"
    );

    let now = now_millis();
    apply_viewer_interest(
        &runtime,
        crate::cloud::TEST_DATABASE_EPOCH,
        vec![CloudViewerInterest {
            viewer_id: "22222222-2222-4222-8222-222222222222".to_string(),
            surface: crate::cloud::CloudViewerSurface::File,
            watch: true,
            expires_at: now + crate::cloud::MAX_VIEWER_INTEREST_LIFETIME_MILLIS,
        }],
    )
    .await
    .expect("fresh watch interest should apply");
    let posts = wait_for_state_posts(&cloud_state, 1).await;
    assert_eq!(posts[0]["status"], 200);
    assert_eq!(posts[0]["snapshot_notify_viewers"], "1");
    assert_eq!(posts[0]["operation_count"], 1);

    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot(
        "capacity-noise-while-watched",
    ));
    runtime.state_notify.notify_one();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        1,
        "library-only changes while watched must not trigger another publication"
    );

    suspend_cloud_viewers(&runtime).await;
    persist_test_operation_result(&inbox, 711, "completed while WebSocket was disconnected");
    runtime.state_notify.notify_one();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        1,
        "a disconnected WebSocket must revoke stale watch interest before local changes arrive"
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
async fn explicit_selected_library_scan_failure_returns_failed_result_and_keeps_cached_snapshot() {
    let root = temp_cloud_bot_dir("snapshot-selected-scan-failure");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
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
    let cached = empty_cloud_test_library_snapshot("preserved-cache");
    *runtime.library_snapshot.lock().await = Some(cached.clone());
    *runtime
        .test_library_refresh_failure
        .lock()
        .expect("refresh-failure hook should be healthy") =
        Some("simulated selected-library scan failure".to_string());

    let (writer, mut results) = mpsc::channel(4);
    *runtime.websocket_writer.lock().await = Some(writer);
    let viewer_id = "22222222-2222-4222-8222-222222222222".to_string();
    let now = now_millis();
    apply_viewer_interest(
        &runtime,
        crate::cloud::TEST_DATABASE_EPOCH,
        vec![CloudViewerInterest {
            viewer_id: viewer_id.clone(),
            surface: crate::cloud::CloudViewerSurface::File,
            watch: false,
            expires_at: now + crate::cloud::MAX_VIEWER_INTEREST_LIFETIME_MILLIS,
        }],
    )
    .await
    .expect("viewer interest should apply");
    let successful_request = SnapshotRefreshRequest {
        viewer_id,
        request_id: "33333333-3333-4333-8333-333333333333".to_string(),
        database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
        reason: crate::cloud::SnapshotRefreshReason::Manual,
        scope: crate::cloud::SnapshotRefreshScope::Operations,
        library_id: None,
        expires_at: now + crate::cloud::MAX_SNAPSHOT_REFRESH_LIFETIME_MILLIS,
    };
    let request = SnapshotRefreshRequest {
        request_id: "11111111-1111-4111-8111-111111111111".to_string(),
        scope: crate::cloud::SnapshotRefreshScope::Library,
        ..successful_request.clone()
    };
    enqueue_snapshot_refresh(&runtime, successful_request.clone())
        .await
        .expect("valid operations refresh should queue");
    enqueue_snapshot_refresh(&runtime, request.clone())
        .await
        .expect("valid explicit refresh should queue");
    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_secs(60 * 60),
        Duration::from_millis(20),
        Duration::from_millis(100),
    ));

    let posts = wait_for_state_posts(&cloud_state, 1).await;
    let result = tokio::time::timeout(Duration::from_secs(5), results.recv())
        .await
        .expect("operations refresh result should be returned")
        .expect("refresh result channel should remain open");
    let failed_result = tokio::time::timeout(Duration::from_secs(5), results.recv())
        .await
        .expect("selected-library failure result should be returned")
        .expect("refresh result channel should remain open");
    assert_eq!(posts[0]["status"], 200);
    assert_eq!(posts[0]["snapshot_notify_viewers"], "1");
    assert_eq!(result["type"], "refresh_result");
    assert_eq!(result["request_id"], successful_request.request_id);
    assert_eq!(result["status"], "published");
    assert_eq!(result["reported_at"], posts[0]["reported_at"]);
    assert_eq!(result["state_version"], posts[0]["state_version"]);
    assert_eq!(failed_result["request_id"], request.request_id);
    assert_eq!(failed_result["status"], "failed");
    assert_eq!(failed_result["error_code"], "reference_refresh_failed");
    assert!(failed_result.get("reported_at").is_none());
    assert_eq!(
        runtime
            .library_snapshot
            .lock()
            .await
            .as_ref()
            .expect("failed scan should retain the cached snapshot")
            .revision,
        cached.revision
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
async fn idle_snapshot_and_inbox_http_counts_stay_hourly_for_24_hours() {
    let root = temp_cloud_bot_dir("snapshot-hourly-http-counts-24h");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
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
    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("idle"));
    let source_fingerprint = build_cloud_state_snapshot(&runtime)
        .await
        .expect("baseline state should build")
        .expect("baseline local state should exist")
        .watch_fingerprint()
        .expect("baseline operation fingerprint should build");
    inbox
        .record_successful_publication_at(now_millis(), &source_fingerprint)
        .expect("recent prior publication should persist");
    sync_cloud_backlog(&runtime)
        .await
        .expect("initial authoritative backlog reconciliation should succeed");
    assert_eq!(cloud_state.fetch_count.load(AtomicOrdering::SeqCst), 1);

    let reporter = tokio::spawn(cloud_state_reporter(runtime.clone()));
    let fallback = tokio::spawn(cloud_rest_fallback(runtime.clone(), 30));
    tokio::time::sleep(Duration::from_millis(50)).await;
    tokio::time::pause();
    for hour in 1..=24 {
        let previous_get = *runtime.last_backlog_reconciled_at.lock().await;
        tokio::time::advance(Duration::from_secs(60 * 60)).await;
        tokio::time::resume();
        wait_for_idle_backlog_reconciliation(&runtime, &cloud_state, hour + 1, previous_get).await;
        wait_for_reported_publication(&runtime, &cloud_state, hour - 1).await;
        let posts = cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .clone();
        assert_eq!(
            posts.len(),
            hour,
            "exactly one manifest post per idle expiry"
        );
        assert!(posts.iter().all(|post| post["status"] == 200));
        assert_eq!(posts[hour - 1]["snapshot_notify_viewers"], "0");
        tokio::time::pause();
    }
    let chunk_posts = cloud_state.state_chunk_posts.load(AtomicOrdering::SeqCst);
    assert_eq!(cloud_state.fetch_count.load(AtomicOrdering::SeqCst), 25);
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        24,
        "a 24-hour idle window should make24 periodic manifests"
    );
    println!(
        "24h idle HTTP metrics: authoritative GETs=25 (one startup plus24 hourly), manifest POSTs=24, chunk PUTs={chunk_posts}"
    );

    reporter.abort();
    fallback.abort();
    let _ = reporter.await;
    let _ = fallback.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn watched_progress_publications_do_not_postpone_hourly_backlog_reconciliation() {
    let root = temp_cloud_bot_dir("snapshot-watched-progress-inbox-fallback");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
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
    *runtime.library_snapshot.lock().await = Some(empty_cloud_test_library_snapshot("progress"));
    let source_fingerprint = build_cloud_state_snapshot(&runtime)
        .await
        .expect("baseline state should build")
        .expect("baseline local state should exist")
        .watch_fingerprint()
        .expect("baseline operations should fingerprint");
    inbox
        .record_successful_publication_at(now_millis(), &source_fingerprint)
        .expect("previous publication should persist");
    sync_cloud_backlog(&runtime)
        .await
        .expect("startup backlog reconciliation should succeed");

    let now = now_millis();
    apply_viewer_interest(
        &runtime,
        crate::cloud::TEST_DATABASE_EPOCH,
        vec![CloudViewerInterest {
            viewer_id: "22222222-2222-4222-8222-222222222222".to_string(),
            surface: crate::cloud::CloudViewerSurface::File,
            watch: true,
            expires_at: now + crate::cloud::MAX_VIEWER_INTEREST_LIFETIME_MILLIS,
        }],
    )
    .await
    .expect("visible operation watch should apply");
    let reporter = tokio::spawn(cloud_state_reporter(runtime.clone()));
    let fallback = tokio::spawn(cloud_rest_fallback(runtime.clone(), 30));
    // Let the reporter load its persisted deadlines and let the fallback settle while HTTP and
    // File Provider work run on the real clock before advancing simulated hours.
    tokio::time::sleep(Duration::from_millis(50)).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    assert!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .is_empty(),
        "watch activation with identical published operations should not post"
    );

    for progress in 1..=6 {
        let previous_get = *runtime.last_backlog_reconciled_at.lock().await;
        tokio::time::advance(Duration::from_secs(30 * 60)).await;
        persist_test_operation_result(&inbox, 730, &format!("progress-{progress}"));
        runtime.state_notify.notify_one();
        tokio::time::resume();
        wait_for_reported_publication(&runtime, &cloud_state, progress - 1).await;
        if progress % 2 == 0 {
            let expected_fetches = 1 + progress / 2;
            wait_for_idle_backlog_reconciliation(
                &runtime,
                &cloud_state,
                expected_fetches,
                previous_get,
            )
            .await;
            assert_eq!(
                cloud_state.fetch_count.load(AtomicOrdering::SeqCst),
                expected_fetches
            );
        } else {
            assert_eq!(
                cloud_state.fetch_count.load(AtomicOrdering::SeqCst),
                1 + progress / 2,
                "an extra idle GET must not occur before the next hour"
            );
        }
        tokio::time::pause();
    }
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake state post lock should not poison")
            .len(),
        6,
        "one semantic progress post should publish per half hour"
    );
    assert_eq!(
        cloud_state.fetch_count.load(AtomicOrdering::SeqCst),
        4,
        "three hours of watched publications still require only three idle GETs plus startup"
    );

    reporter.abort();
    fallback.abort();
    let _ = reporter.await;
    let _ = fallback.await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn saturated_websocket_writer_fails_fast_instead_of_blocking_the_receive_loop() {
    let root = temp_cloud_bot_dir("snapshot-websocket-writer-saturated");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let (writer, mut reader) = mpsc::channel(1);
    writer
        .try_send(serde_json::json!({"already":"queued"}))
        .expect("test should fill the single writer slot");
    *runtime.websocket_writer.lock().await = Some(writer);

    let send = tokio::time::timeout(
        Duration::from_millis(50),
        send_cloud_websocket_json(&runtime, &serde_json::json!({"next":"result"})),
    )
    .await
    .expect("saturated result admission must not await the writer loop");
    assert!(send.is_err());
    assert_eq!(
        reader
            .try_recv()
            .expect("original bounded item should remain queued"),
        serde_json::json!({"already":"queued"})
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn opted_in_websocket_applies_snapshot_controls_and_cancels_pending_refresh_on_disconnect() {
    let root = temp_cloud_bot_dir("snapshot-control-websocket-e2e");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("local fake WebSocket listener should bind");
    let address = listener
        .local_addr()
        .expect("fake WebSocket listener should expose its address");
    let observed_header = Arc::new(StdMutex::new(None));
    let server_observed_header = Arc::clone(&observed_header);
    let (close_server, wait_to_close) = oneshot::channel();
    let viewer_id = "22222222-2222-4222-8222-222222222222".to_string();
    let request_id = "11111111-1111-4111-8111-111111111111".to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .context("fake snapshot-control WebSocket should accept")?;
        let mut socket = tokio_tungstenite::accept_hdr_async(
            stream,
            move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                  response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                *server_observed_header
                    .lock()
                    .expect("WebSocket header capture should not poison") = request
                    .headers()
                    .get("X-Cloud-Snapshot-Control")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                Ok(response)
            },
        )
        .await
        .context("fake snapshot-control handshake should complete")?;
        let now = now_millis();
        let interest = crate::cloud::CloudSnapshotControl::ViewerInterest {
            database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
            viewers: vec![CloudViewerInterest {
                viewer_id: viewer_id.clone(),
                surface: crate::cloud::CloudViewerSurface::File,
                watch: true,
                expires_at: now + crate::cloud::MAX_VIEWER_INTEREST_LIFETIME_MILLIS,
            }],
        };
        socket
            .send(WsMessage::Text(
                serde_json::to_string(&interest)
                    .context("viewer-interest frame should serialize")?
                    .into(),
            ))
            .await
            .context("viewer-interest frame should send")?;
        let refresh = crate::cloud::CloudSnapshotControl::SnapshotRefresh(SnapshotRefreshRequest {
            viewer_id,
            request_id,
            database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
            reason: crate::cloud::SnapshotRefreshReason::Manual,
            scope: crate::cloud::SnapshotRefreshScope::Operations,
            library_id: None,
            expires_at: now + crate::cloud::MAX_SNAPSHOT_REFRESH_LIFETIME_MILLIS,
        });
        socket
            .send(WsMessage::Text(
                serde_json::to_string(&refresh)
                    .context("snapshot-refresh frame should serialize")?
                    .into(),
            ))
            .await
            .context("snapshot-refresh frame should send")?;
        wait_to_close
            .await
            .context("test should release fake WebSocket close")?;
        socket
            .send(WsMessage::Close(None))
            .await
            .context("fake WebSocket should close")?;
        Ok::<_, anyhow::Error>(())
    });

    let mut runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&format!("http://{address}/"))
        .expect("test cloud client should use the fake WebSocket");
    let mut socket = runtime
        .client
        .connect_websocket()
        .await
        .expect("new Mac WebSocket should connect with control opt-in");
    let session_runtime = runtime.clone();
    let session = tokio::spawn(async move {
        receive_cloud_websocket_session(&session_runtime, &mut socket, None).await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if runtime.snapshot_refreshes.lock().await.pending.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("snapshot-refresh control should reach the bounded local queue");
    assert!(
        runtime
            .viewer_interests
            .lock()
            .await
            .contains_key("22222222-2222-4222-8222-222222222222")
    );

    close_server
        .send(())
        .expect("fake WebSocket close should be released");
    tokio::time::timeout(Duration::from_secs(5), session)
        .await
        .expect("local WebSocket receive session should stop after disconnect")
        .expect("local WebSocket session task should not panic")
        .expect("clean fake WebSocket disconnect should return successfully");
    server
        .await
        .expect("fake WebSocket task should not panic")
        .expect("fake WebSocket task should complete");
    assert_eq!(
        observed_header
            .lock()
            .expect("WebSocket header capture should not poison")
            .as_deref(),
        Some("v1")
    );
    assert!(runtime.viewer_interests.lock().await.is_empty());
    let seen = runtime.snapshot_refreshes.lock().await;
    let refresh_result = seen
        .seen
        .get(&(
            "22222222-2222-4222-8222-222222222222".to_string(),
            "11111111-1111-4111-8111-111111111111".to_string(),
        ))
        .and_then(|seen| seen.result.as_ref())
        .expect("disconnect should retain an unavailable result for reconnect replay");
    assert_eq!(refresh_result.status, "unavailable");
    assert_eq!(
        refresh_result.error_code.as_deref(),
        Some("transport_disconnected")
    );

    drop(seen);
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}
