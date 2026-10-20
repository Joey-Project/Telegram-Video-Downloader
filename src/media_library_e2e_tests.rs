use super::*;

struct MigrationFixture {
    root: PathBuf,
    video_root: PathBuf,
    archive_root: PathBuf,
    config: AppConfig,
    source_library_id: String,
    target_library_id: String,
}

fn create_migration_fixture(
    label: &str,
    epoch: &str,
) -> (MigrationFixture, Arc<QueueManager>, Arc<CloudInbox>) {
    let root = temp_cloud_bot_dir(label);
    fs::create_dir_all(&root).expect("migration fixture root should create");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("migration fixture roots should create");
    config.downloads.video_dir =
        fs::canonicalize(&config.downloads.video_dir).expect("video root should canonicalize");
    config.downloads.pdf_dir =
        fs::canonicalize(&config.downloads.pdf_dir).expect("PDF root should canonicalize");

    let archive_root = root.join("archive");
    fs::create_dir_all(&archive_root).expect("archive root should create");
    let archive_root = fs::canonicalize(archive_root).expect("archive root should canonicalize");
    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let source_library_id = queue
        .default_video_destination()
        .expect("default video destination should resolve")
        .library_id;
    let target = queue
        .register_video_library("Archive", &archive_root, None, false, false)
        .expect("archive library should register");
    let inbox = Arc::new(
        CloudInbox::open_for_epoch(&config.downloads.video_dir, epoch)
            .expect("cloud inbox should open for the requested epoch"),
    );
    let fixture = MigrationFixture {
        root,
        video_root: config.downloads.video_dir.clone(),
        archive_root,
        config,
        source_library_id,
        target_library_id: target.id,
    };
    (fixture, queue, inbox)
}

fn migration_engine_for_test(
    runtime: &CloudBotRuntime,
    source_library_id: &str,
    target_library_id: &str,
) -> crate::library_migration::MigrationEngine {
    let manager = |library_id: &str| {
        let (scoped, _) = crate::media_library_bot::scoped_runtime(runtime, Some(library_id))
            .expect("registered library should scope");
        Arc::new(
            crate::open_cloud_library_manager(&scoped.bot, Arc::clone(&runtime.inbox), None)
                .expect("library manager should open"),
        )
    };
    crate::library_migration::MigrationEngine::new(
        source_library_id,
        manager(source_library_id),
        target_library_id,
        manager(target_library_id),
    )
    .expect("migration engine should bind both registered libraries")
}

async fn hold_migration_output_slot(fixture: &MigrationFixture) -> downloader::VideoMigrationGuard {
    // Block only the shared in-process writer slot. Preview scans must remain able to acquire
    // the actual source/target flocks before the background runner starts waiting for its slot.
    let blocker = fixture.root.join("output-slot-blocker");
    fs::create_dir_all(&blocker).unwrap();
    downloader::hold_video_outputs_for_migration(&[blocker])
        .await
        .unwrap()
}

async fn create_and_confirm_migration(
    runtime: &CloudBotRuntime,
    preview_seq: u64,
    confirm_seq: u64,
    source_library_id: &str,
    target_library_id: &str,
) -> serde_json::Value {
    let preview_record = run_library_management_test_request(
        runtime,
        preview_seq,
        "library_migration_preview",
        serde_json::json!({
            "source_library_id": source_library_id,
            "target_library_id": target_library_id,
            "scope": "library",
            "conflict": "skip",
            "include_incomplete_tasks": false,
        }),
    )
    .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = preview_record
        .result
        .expect("migration preview should have a durable result");
    let confirmation = run_library_management_test_request(
        runtime,
        confirm_seq,
        "library_migration_confirm",
        serde_json::json!({
            "preview_id": preview["id"],
            "revision": preview["revision"],
        }),
    )
    .await;
    assert_eq!(
        confirmation.status,
        InboxStatus::Completed,
        "{:?}",
        confirmation.error
    );
    preview
}

async fn wait_for_migration_status(
    runtime: &CloudBotRuntime,
    migration_id: &str,
    mut matches: impl FnMut(&crate::library_migration::MigrationStatus) -> bool,
) -> crate::library_migration::MigrationStatus {
    let result = tokio::time::timeout(MIGRATION_TEST_COMPLETION_TIMEOUT, async {
        loop {
            if let Some(status) = runtime.migrations.lock().await.get(migration_id).cloned()
                && matches(&status)
            {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    match result {
        Ok(status) => status,
        Err(_) => {
            let diagnostic = migration_timeout_diagnostic(runtime, migration_id).await;
            panic!("migration should reach the requested status: {diagnostic}");
        }
    }
}

async fn migration_timeout_diagnostic(runtime: &CloudBotRuntime, migration_id: &str) -> String {
    let in_memory = runtime
        .migrations
        .lock()
        .await
        .get(migration_id)
        .map(|status| format!("{status:?}"))
        .unwrap_or_else(|| "none".to_string());
    let runner_active = runtime
        .migration_runners
        .lock()
        .await
        .contains(migration_id);

    let lookup_runtime = runtime.clone();
    let lookup_id = migration_id.to_string();
    let lookup = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(
            move || -> anyhow::Result<Option<crate::library_migration::MigrationStatus>> {
                let registry = lookup_runtime.bot.queue.registry_snapshot()?;
                let mut failures = Vec::new();
                for library in registry.libraries {
                    let result =
                        (|| -> anyhow::Result<Vec<crate::library_migration::MigrationStatus>> {
                            let (scoped, _) = crate::media_library_bot::scoped_runtime(
                                &lookup_runtime,
                                Some(&library.id),
                            )?;
                            let manager = crate::open_cloud_library_manager(
                                &scoped.bot,
                                Arc::clone(&lookup_runtime.inbox),
                                None,
                            )?;
                            crate::library_migration::MigrationEngine::list_for_source(&manager)
                        })();
                    match result {
                        Ok(statuses) => {
                            if let Some(status) =
                                statuses.into_iter().find(|status| status.id == lookup_id)
                            {
                                return Ok(Some(status));
                            }
                        }
                        Err(error) => failures.push(format!("{}: {error:#}", library.id)),
                    }
                }
                if failures.is_empty() {
                    Ok(None)
                } else {
                    anyhow::bail!(
                        "no durable status found; library lookups failed: {}",
                        failures.join("; ")
                    )
                }
            },
        ),
    )
    .await;
    let durable = match lookup {
        Err(_) => "lookup timed out after 5 seconds".to_string(),
        Ok(Err(error)) => format!("lookup task failed: {error}"),
        Ok(Ok(Err(error))) => format!("lookup failed: {error:#}"),
        Ok(Ok(Ok(None))) => "not found in durable library state".to_string(),
        Ok(Ok(Ok(Some(status)))) => format!("{status:?}"),
    };
    format!(
        "id={migration_id}, runner_active={runner_active}, in_memory={in_memory}, durable={durable}"
    )
}

async fn wait_for_migration_runner(runtime: &CloudBotRuntime, migration_id: &str, running: bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let contains = runtime
                .migration_runners
                .lock()
                .await
                .contains(migration_id);
            if contains == running {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("migration runner should reach the requested state");
}

#[tokio::test]
async fn migration_pause_survives_runtime_restart_until_explicit_resume() {
    let (telegram, mut telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) =
        create_migration_fixture("migration-pause-restart", crate::cloud::TEST_DATABASE_EPOCH);
    let source_media = fixture.video_root.join("pause-resume.mp4");
    fs::write(&source_media, b"pause and resume migration fixture")
        .expect("source media should write");
    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let output_guard = hold_migration_output_slot(&fixture).await;
    let preview = create_and_confirm_migration(
        &runtime,
        1_101,
        1_102,
        &fixture.source_library_id,
        &fixture.target_library_id,
    )
    .await;
    let migration_id = preview["id"]
        .as_str()
        .expect("preview should contain a migration ID")
        .to_string();
    wait_for_migration_runner(&runtime, &migration_id, true).await;

    let scan = run_library_management_test_request(
        &runtime,
        1_103,
        "library_scan",
        serde_json::json!({"library_id": fixture.source_library_id}),
    )
    .await;
    assert_eq!(scan.status, InboxStatus::Failed, "{:?}", scan.error);
    assert!(
        scan.error
            .as_deref()
            .is_some_and(|error| error.contains("active migration")),
        "scan should fail promptly while the migration runner owns this library: {:?}",
        scan.error
    );

    let paused = run_library_management_test_request(
        &runtime,
        1_104,
        "library_migration_control",
        serde_json::json!({"migration_id": migration_id, "control": "pause"}),
    )
    .await;
    assert_eq!(paused.status, InboxStatus::Completed, "{:?}", paused.error);
    drop(output_guard);
    wait_for_migration_runner(&runtime, &migration_id, false).await;
    let paused_status = wait_for_migration_status(&runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Paused
    })
    .await;
    assert_eq!(paused_status.completed_items, 0);
    assert!(source_media.exists());
    assert!(!fixture.archive_root.join("pause-resume.mp4").exists());

    dispatcher.abort();
    let _ = dispatcher.await;
    runtime.signal_epoch_fatal();
    drop(runtime);
    drop(inbox);
    drop(queue);

    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reopen after runtime drop"),
    );
    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, crate::cloud::TEST_DATABASE_EPOCH)
            .expect("same-epoch inbox should reopen"),
    );
    let reopened_runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&reopened_queue),
        Arc::clone(&reopened_inbox),
    );
    let reopened_dispatcher = tokio::spawn(cloud_request_dispatcher(reopened_runtime.clone()));
    let migration_watcher = tokio::spawn(crate::library_migration_bot::watch(
        reopened_runtime.clone(),
    ));
    let discovered = wait_for_migration_status(&reopened_runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Paused
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        discovered.status,
        crate::library_migration::MigrationRunStatus::Paused
    );
    assert!(
        !reopened_runtime
            .migration_runners
            .lock()
            .await
            .contains(&migration_id)
    );
    assert!(
        source_media.exists(),
        "paused job must not auto-resume on restart"
    );

    let resumed = run_library_management_test_request(
        &reopened_runtime,
        1_105,
        "library_migration_control",
        serde_json::json!({"migration_id": migration_id, "control": "resume"}),
    )
    .await;
    assert_eq!(
        resumed.status,
        InboxStatus::Completed,
        "{:?}",
        resumed.error
    );
    let completed = wait_for_migration_status(&reopened_runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Completed
    })
    .await;
    assert_eq!(completed.success_count, 1);
    wait_for_migration_runner(&reopened_runtime, &migration_id, false).await;
    assert!(!source_media.exists());
    assert_eq!(
        fs::read(fixture.archive_root.join("pause-resume.mp4"))
            .expect("resumed media should publish"),
        b"pause and resume migration fixture"
    );

    let telegram_requests = take_cloud_test_telegram_requests(&mut telegram_requests);
    let sent = telegram_requests
        .iter()
        .find(|request| request.method == "sendMessage")
        .expect("confirmation should send one durable migration message");
    let chat_id = sent.body["chat_id"].as_i64().unwrap();
    let message_id = sent.body["message_id"].as_i64().unwrap_or(1_000);
    assert!(telegram_requests.iter().any(|request| {
        request.method == "editMessageText"
            && request.body["chat_id"].as_i64() == Some(chat_id)
            && request.body["message_id"].as_i64() == Some(message_id)
    }));

    reopened_runtime.signal_epoch_fatal();
    migration_watcher
        .await
        .expect("migration watcher should stop after runtime shutdown");
    reopened_dispatcher.abort();
    let _ = reopened_dispatcher.await;
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn migration_pauses_when_new_media_appears_before_leased_task_handoff() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-late-media-before-task-handoff",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let original_media = fixture.video_root.join("original.mp4");
    let original_metadata = fixture.video_root.join("original.nfo");
    fs::write(&original_media, b"approved original media").expect("original media should write");
    fs::write(
        &original_metadata,
        b"<movie><title>Original</title></movie>",
    )
    .expect("original metadata should write");

    let retained = downloader::create_test_retained_video_staging(&fixture.video_root)
        .expect("retained staging directory should be created");
    fs::write(retained.join("segment.part"), b"manual task payload")
        .expect("manual task fragment should write");
    let mut failed_task = TaskRecord::new(
        "late-media-manual-task".into(),
        1_601,
        1_601,
        123_456_789,
        Some(123_456_789),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=abcdefghijk".into(),
        },
    );
    failed_task.status = TaskStatus::Failed;
    failed_task.error = Some("synthetic interrupted transfer".into());
    failed_task.staging_attempts.push(retained.clone());
    queue
        .create(failed_task)
        .expect("failed manual task should persist in source queue");

    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let output_guard = hold_migration_output_slot(&fixture).await;
    let preview_record = run_library_management_test_request(
        &runtime,
        1_601,
        "library_migration_preview",
        serde_json::json!({
            "source_library_id": fixture.source_library_id,
            "target_library_id": fixture.target_library_id,
            "scope": "library",
            "conflict": "skip",
            "include_incomplete_tasks": true,
        }),
    )
    .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = preview_record
        .result
        .expect("migration preview should have a durable result");
    assert_eq!(preview["item_count"], 1);
    assert_eq!(preview["incomplete_task_count"], 1);
    let migration_id = preview["id"]
        .as_str()
        .expect("preview should contain a migration ID")
        .to_string();
    let confirmation = run_library_management_test_request(
        &runtime,
        1_602,
        "library_migration_confirm",
        serde_json::json!({
            "preview_id": preview["id"],
            "revision": preview["revision"],
        }),
    )
    .await;
    assert_eq!(
        confirmation.status,
        InboxStatus::Completed,
        "{:?}",
        confirmation.error
    );
    wait_for_migration_runner(&runtime, &migration_id, true).await;

    let task_before_lease = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let task = queue
                .get("late-media-manual-task")
                .expect("manual task should remain readable")
                .expect("manual task should remain registered");
            if task.destination_transfer.is_some() {
                break task;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("task handoff token should attach while migration waits for output lease");
    let transfer = task_before_lease
        .destination_transfer
        .as_ref()
        .expect("handoff token should be attached before lease acquisition");
    assert_eq!(transfer.target.library_id, fixture.target_library_id);
    assert_eq!(
        task_before_lease.destination_transfer_phase,
        Some(crate::media_libraries::TaskDestinationTransferPhase::Copying)
    );

    let late_media = fixture.video_root.join("late-added.mp4");
    let late_metadata = fixture.video_root.join("late-added.nfo");
    fs::write(&late_media, b"media added after preview")
        .expect("late media should write while output lease is blocked");
    fs::write(&late_metadata, b"<movie><title>Late</title></movie>")
        .expect("late metadata should write while output lease is blocked");
    drop(output_guard);

    let paused = wait_for_migration_status(&runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Paused
    })
    .await;
    wait_for_migration_runner(&runtime, &migration_id, false).await;
    assert!(
        paused.warnings.iter().any(|warning| {
            warning.contains("fresh migration preview")
                && warning.contains("source library gained media")
        }),
        "pause should explain that the expanded library needs a fresh preview: {:?}",
        paused.warnings
    );
    assert_eq!(paused.success_count, 0);
    assert!(original_media.exists());
    assert!(original_metadata.exists());
    assert!(late_media.exists());
    assert!(late_metadata.exists());
    assert!(!fixture.archive_root.join("original.mp4").exists());
    assert!(!fixture.archive_root.join("original.nfo").exists());
    assert!(!fixture.archive_root.join("late-added.mp4").exists());
    assert!(!fixture.archive_root.join("late-added.nfo").exists());

    let stopped = run_library_management_test_request(
        &runtime,
        1_603,
        "library_migration_control",
        serde_json::json!({"migration_id": migration_id, "control": "stop_remaining"}),
    )
    .await;
    assert_eq!(
        stopped.status,
        InboxStatus::Completed,
        "{:?}",
        stopped.error
    );
    let final_status = wait_for_migration_status(&runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Stopped
    })
    .await;
    assert_eq!(final_status.success_count, 0);
    let task_after = queue
        .get("late-media-manual-task")
        .expect("manual task should remain readable after stop")
        .expect("manual task should remain registered after stop");
    assert_eq!(task_after.status, TaskStatus::Failed);
    assert_eq!(
        task_after.destination.as_ref().unwrap().library_id,
        fixture.source_library_id
    );
    assert!(task_after.destination_transfer.is_none());
    assert!(task_after.destination_transfer_phase.is_none());
    assert_eq!(task_after.staging_attempts.first(), Some(&retained));
    assert_eq!(
        fs::read(retained.join("segment.part")).unwrap(),
        b"manual task payload",
        "stopping the paused migration should leave the failed task payload in source"
    );
    assert!(original_media.exists());
    assert!(original_metadata.exists());
    assert!(late_media.exists());
    assert!(late_metadata.exists());
    assert!(!fixture.archive_root.join("original.mp4").exists());
    assert!(!fixture.archive_root.join("late-added.mp4").exists());

    dispatcher.abort();
    let _ = dispatcher.await;
    runtime.signal_epoch_fatal();
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn migration_stop_remaining_keeps_completed_and_remaining_media_and_queue_state() {
    let (telegram, mut telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-stop-remaining",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    for (name, bytes) in [
        ("Alpha.mp4", b"first migration item".as_slice()),
        ("Beta.mp4", b"second migration item".as_slice()),
    ] {
        fs::write(fixture.video_root.join(name), bytes).expect("source item should write");
    }
    let mut unrelated_task = TaskRecord::new(
        "stop-remaining-queue-task".to_string(),
        7,
        7,
        123_456_789,
        Some(123_456_789),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=abcdefghijk".to_string(),
        },
    );
    unrelated_task.status = TaskStatus::Queued;
    queue
        .create(unrelated_task)
        .expect("unrelated queued task should persist");
    let task_before = queue
        .get("stop-remaining-queue-task")
        .expect("queued task should load")
        .expect("queued task should exist");
    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let output_guard = hold_migration_output_slot(&fixture).await;
    let preview = create_and_confirm_migration(
        &runtime,
        1_201,
        1_202,
        &fixture.source_library_id,
        &fixture.target_library_id,
    )
    .await;
    let migration_id = preview["id"].as_str().unwrap().to_string();
    wait_for_migration_runner(&runtime, &migration_id, true).await;

    let engine = migration_engine_for_test(
        &runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    let mut partially_completed = None;
    for _ in 0..32 {
        let worker = engine.clone();
        let id = migration_id.clone();
        let status = tokio::task::spawn_blocking(move || worker.step(&id))
            .await
            .expect("one migration checkpoint should join")
            .expect("one migration checkpoint should succeed");
        if status.completed_items > 0 {
            partially_completed = Some(status);
            break;
        }
    }
    let partial = partially_completed.expect("one media item should complete before the next");
    assert_eq!(partial.completed_items, 1);
    assert!(
        partial
            .items
            .iter()
            .any(|item| { item.status == crate::library_migration::MigrationItemPhase::Complete })
    );
    assert!(partial.items.iter().any(|item| {
        !matches!(
            item.status,
            crate::library_migration::MigrationItemPhase::Complete
                | crate::library_migration::MigrationItemPhase::Skipped
                | crate::library_migration::MigrationItemPhase::Failed
        )
    }));

    let stopped = run_library_management_test_request(
        &runtime,
        1_203,
        "library_migration_control",
        serde_json::json!({"migration_id": migration_id, "control": "stop_remaining"}),
    )
    .await;
    assert_eq!(
        stopped.status,
        InboxStatus::Completed,
        "{:?}",
        stopped.error
    );
    drop(output_guard);
    wait_for_migration_runner(&runtime, &migration_id, false).await;
    let final_status = wait_for_migration_status(&runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Stopped
    })
    .await;
    assert_eq!(final_status.success_count, 1);
    assert_eq!(final_status.skipped_count, 1);
    assert_eq!(final_status.failed_count, 0);
    for item in &final_status.items {
        let source = fixture.video_root.join(&item.source_path);
        let target = fixture.archive_root.join(&item.target_path);
        match item.status {
            crate::library_migration::MigrationItemPhase::Complete => {
                assert!(!source.exists(), "completed source should be cleaned");
                assert!(target.exists(), "completed target should remain published");
            }
            crate::library_migration::MigrationItemPhase::Skipped => {
                assert!(source.exists(), "stopped item should remain in the source");
                assert!(!target.exists(), "stopped item should not be published");
            }
            other => panic!("unexpected final item state: {other:?}"),
        }
    }
    let task_after = queue
        .get("stop-remaining-queue-task")
        .expect("queued task should still load")
        .expect("queued task should remain registered");
    assert_eq!(task_after.status, TaskStatus::Queued);
    assert_eq!(task_after.generation, task_before.generation);
    assert_eq!(task_after.destination, task_before.destination);

    dispatcher.abort();
    let _ = dispatcher.await;
    runtime.signal_epoch_fatal();
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = take_cloud_test_telegram_requests(&mut telegram_requests);
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn cloud_epoch_change_pauses_migration_until_resume_and_reuses_status_message() {
    const NEXT_DATABASE_EPOCH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let (telegram, mut telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) =
        create_migration_fixture("migration-epoch-change", crate::cloud::TEST_DATABASE_EPOCH);
    let source_media = fixture.video_root.join("epoch-bound.mp4");
    fs::write(&source_media, b"migration bound to the old epoch")
        .expect("source media should write");
    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let output_guard = hold_migration_output_slot(&fixture).await;
    let preview = create_and_confirm_migration(
        &runtime,
        1_301,
        1_302,
        &fixture.source_library_id,
        &fixture.target_library_id,
    )
    .await;
    let migration_id = preview["id"].as_str().unwrap().to_string();
    wait_for_migration_runner(&runtime, &migration_id, true).await;
    let initial_messages = take_cloud_test_telegram_requests(&mut telegram_requests);
    let initial_message = initial_messages
        .iter()
        .find(|request| request.method == "sendMessage")
        .expect("confirmation should send a progress message");
    let chat_id = initial_message.body["chat_id"].as_i64().unwrap();
    let message_id = initial_message.body["message_id"].as_i64().unwrap_or(1_000);

    runtime.signal_epoch_fatal();
    wait_for_migration_runner(&runtime, &migration_id, false).await;
    drop(output_guard);
    let queued_engine = migration_engine_for_test(
        &runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    let id = migration_id.clone();
    let persisted = tokio::task::spawn_blocking(move || queued_engine.status(&id))
        .await
        .expect("old-epoch migration lookup should join")
        .expect("old-epoch migration lookup should succeed")
        .expect("confirmed migration should remain durable");
    assert_eq!(
        persisted.status,
        crate::library_migration::MigrationRunStatus::Queued
    );
    assert!(source_media.exists());

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);

    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reopen after epoch change"),
    );
    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, NEXT_DATABASE_EPOCH)
            .expect("new-epoch inbox should open"),
    );
    let reopened_runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&reopened_queue),
        Arc::clone(&reopened_inbox),
    );
    let reopened_dispatcher = tokio::spawn(cloud_request_dispatcher(reopened_runtime.clone()));
    let migration_watcher = tokio::spawn(crate::library_migration_bot::watch(
        reopened_runtime.clone(),
    ));
    let paused = wait_for_migration_status(&reopened_runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Paused
    })
    .await;
    wait_for_migration_runner(&reopened_runtime, &migration_id, false).await;
    assert!(
        source_media.exists(),
        "changed cloud epoch must block auto recovery"
    );
    assert!(!fixture.archive_root.join("epoch-bound.mp4").exists());
    assert_eq!(
        paused.status,
        crate::library_migration::MigrationRunStatus::Paused
    );
    assert!(take_cloud_test_telegram_requests(&mut telegram_requests).is_empty());

    let resumed = run_library_management_test_request(
        &reopened_runtime,
        1,
        "library_migration_control",
        serde_json::json!({"migration_id": migration_id, "control": "resume"}),
    )
    .await;
    assert_eq!(
        resumed.status,
        InboxStatus::Completed,
        "{:?}",
        resumed.error
    );
    let completed = wait_for_migration_status(&reopened_runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Completed
    })
    .await;
    assert_eq!(completed.success_count, 1);
    wait_for_migration_runner(&reopened_runtime, &migration_id, false).await;
    assert!(!source_media.exists());
    assert!(fixture.archive_root.join("epoch-bound.mp4").exists());

    let telegram_requests = take_cloud_test_telegram_requests(&mut telegram_requests);
    assert!(telegram_requests.iter().any(|request| {
        request.method == "editMessageText"
            && request.body["chat_id"].as_i64() == Some(chat_id)
            && request.body["message_id"].as_i64() == Some(message_id)
    }));

    reopened_runtime.signal_epoch_fatal();
    migration_watcher
        .await
        .expect("migration watcher should stop after runtime shutdown");
    reopened_dispatcher.abort();
    let _ = reopened_dispatcher.await;
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn migration_resumes_private_task_fragment_mid_copy_after_same_epoch_restart() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-private-fragment-mid-copy-restart",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let retained = downloader::create_test_retained_video_staging(&fixture.video_root)
        .expect("retained staging directory should be created");
    let fragment = vec![0xa7; 16 * 1024 * 1024 + 137];
    fs::write(retained.join("segment.part"), &fragment)
        .expect("synthetic incomplete media fragment should be written");
    let task_id = "migration-mid-copy-task";
    let mut task = TaskRecord::new(
        task_id.into(),
        1_501,
        1_501,
        123_456_789,
        Some(123_456_789),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=abcdefghijk".into(),
        },
    );
    task.status = TaskStatus::Failed;
    task.error = Some("synthetic interrupted video transfer".to_string());
    task.staging_attempts.push(retained.clone());
    queue
        .create(task)
        .expect("failed task with retained staging should persist");

    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let snapshot = queue
        .list_incomplete_task_transfer_snapshots(&fixture.source_library_id, 16)
        .expect("incomplete task should be included in the frozen preview")[0]
        .clone();
    let engine = migration_engine_for_test(
        &runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    let preview = engine
        .preview(&crate::library_migration::MigrationRequest {
            scope: crate::library_migration::MigrationScope::Library,
            item_ids: Vec::new(),
            conflict: crate::library::ConflictPolicy::Skip,
            item_conflicts: Default::default(),
            include_incomplete_tasks: true,
            set_target_default_on_success: false,
            task_impacts: vec![crate::library_migration::MigrationTaskImpact {
                task_id: snapshot.record.id.clone(),
                generation: snapshot.record.generation,
                revision: snapshot.record.revision,
                staging_bytes: snapshot.staging_bytes,
                staging_file_count: snapshot.staging_file_count,
                published_item_ids: Vec::new(),
            }],
            warnings: Vec::new(),
        })
        .expect("migration preview should bind the retained task impact");
    assert_eq!(preview.item_count, 0, "test fixture has no media bundles");
    assert_eq!(preview.incomplete_task_count, 1);
    assert_eq!(preview.total_bytes, snapshot.staging_bytes);
    assert!(preview.total_bytes >= fragment.len() as u64);
    engine
        .confirm(&preview.id, &preview.revision)
        .expect("confirmed migration should be durable before running");
    engine
        .set_runtime_context(
            &preview.id,
            crate::library_migration::MigrationRuntimeContext {
                database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
                preview_seq: 1_501,
                chat_id: None,
                message_id: None,
                delivery: crate::library_migration::MigrationDeliveryState::Pending,
            },
        )
        .expect("migration should carry the current cloud epoch for recovery");

    let target = queue
        .registry_snapshot()
        .expect("media library registry should load")
        .libraries
        .into_iter()
        .find(|library| library.id == fixture.target_library_id)
        .expect("target library should remain registered");
    let token = queue
        .begin_task_destination_transfer(
            task_id,
            &preview.id,
            snapshot.record.generation,
            snapshot.record.revision,
            crate::media_libraries::TaskDestination {
                library_id: target.id,
                root: target.root,
                category: snapshot.destination.category.clone(),
            },
        )
        .expect("queue should persist the task transfer token");
    queue
        .wait_for_task_destination_transfer(&token)
        .await
        .expect("task writer should settle before plan creation");
    let plan = queue
        .task_transfer_plan(&token)
        .expect("queue should bind retained files and controls into a transfer plan");
    assert!(plan.files.iter().any(|file| {
        file.kind == crate::media_libraries::TaskTransferEntryKind::File
            && file.source_relative.ends_with("segment.part")
            && file.size_bytes == fragment.len() as u64
    }));
    assert!(plan.controls.iter().any(|control| {
        control.kind == crate::media_libraries::TaskTransferControlKind::Retention
    }));
    engine
        .attach_task_transfer(&preview.id, plan.clone())
        .expect("engine should durably attach the queue transfer plan");

    let migration_id = preview.id.clone();
    let partial = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let worker = engine.clone();
            let id = migration_id.clone();
            let status = tokio::task::spawn_blocking(move || worker.step(&id))
                .await
                .expect("migration checkpoint should join")
                .expect("migration checkpoint should succeed");
            assert_ne!(
                status.status,
                crate::library_migration::MigrationRunStatus::Failed,
                "private task transfer should not fail: {status:?}"
            );
            assert!(status.total_bytes > 0);
            if status.copied_bytes > 0 && status.copied_bytes < status.total_bytes {
                assert_eq!(status.task_transfers.len(), 1);
                assert_eq!(status.task_transfers[0].completed_files, 0);
                assert!(!status.task_transfers[0].copy_complete);
                return status;
            }
        }
    })
    .await
    .expect("migration should stop between bounded fragment-copy chunks");
    assert_eq!(
        partial.status,
        crate::library_migration::MigrationRunStatus::Running
    );
    assert!(partial.copied_bytes < fragment.len() as u64);
    assert_eq!(
        fs::read(retained.join("segment.part")).unwrap(),
        fragment,
        "a mid-copy restart must leave the source fragment intact"
    );
    let source_root = crate::safe_fs::RootedFs::new(&fixture.video_root)
        .expect("source library root should bind");
    let source_job_identity = source_root
        .entry_identity(&retained)
        .expect("source staging identity should load")
        .expect("source staging directory should remain present");
    let source_controls = downloader::inspect_video_staging_for_migration(
        &source_root,
        retained.strip_prefix(&fixture.video_root).unwrap(),
        source_job_identity,
        &plan.controls,
    )
    .expect("source staging controls should remain valid while the fragment is partial");
    assert_eq!(source_controls.len(), plan.controls.len());

    runtime.signal_epoch_fatal();
    drop(engine);
    drop(runtime);
    drop(inbox);
    drop(queue);

    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reopen after partial copy"),
    );
    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, crate::cloud::TEST_DATABASE_EPOCH)
            .expect("same-epoch cloud inbox should reopen"),
    );
    let reopened_runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&reopened_queue),
        Arc::clone(&reopened_inbox),
    );
    let migration_watcher = tokio::spawn(crate::library_migration_bot::watch(
        reopened_runtime.clone(),
    ));
    let recovered = wait_for_migration_status(&reopened_runtime, &preview.id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Completed
    })
    .await;
    wait_for_migration_runner(&reopened_runtime, &preview.id, false).await;
    assert_eq!(
        recovered.success_count, 0,
        "task handoffs are not media bundles"
    );
    assert_eq!(recovered.failed_count, 0);
    assert_eq!(recovered.task_transfers.len(), 1);
    assert!(recovered.task_transfers[0].commit_acked);
    assert!(recovered.task_transfers[0].copy_complete);
    assert!(
        !retained.join("segment.part").exists(),
        "the source fragment is cleaned only after its target handoff succeeds"
    );
    let recovered_task = reopened_queue
        .get(task_id)
        .expect("recovered task should load")
        .expect("task should remain in the target queue");
    assert_eq!(recovered_task.status, TaskStatus::Failed);
    assert_eq!(
        recovered_task.destination.as_ref().unwrap().library_id,
        fixture.target_library_id
    );
    assert!(recovered_task.destination_transfer.is_none());
    let target_staging = recovered_task
        .staging_attempts
        .first()
        .expect("recovered task should retain the migrated staging attempt");
    assert!(target_staging.starts_with(&fixture.archive_root));
    assert_eq!(
        fs::read(target_staging.join("segment.part")).unwrap(),
        fragment,
        "reopened migration should finish the exact target fragment"
    );

    let target_root = crate::safe_fs::RootedFs::new(&fixture.archive_root)
        .expect("target library root should bind");
    let target_job_identity = target_root
        .entry_identity(target_staging)
        .expect("target staging identity should load")
        .expect("target staging directory should remain present");
    let target_controls = plan
        .controls
        .iter()
        .map(|control| {
            let relative = control.target_relative.clone();
            let entry = target_root
                .bind_entry(&target_root.logical_root_path().join(&relative), false)
                .expect("target control should bind");
            let identity = target_root
                .bound_entry_identity(&entry)
                .expect("target control identity should load")
                .expect("target control file should remain present");
            let file = target_root
                .open_bound_file_if_identity(&entry, identity)
                .expect("target control should open");
            let policy = file
                .access_policy()
                .expect("target control policy should load");
            let sha256 = file
                .sha256()
                .expect("target control digest should load")
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            crate::media_libraries::TaskTransferControl {
                source_relative: relative.clone(),
                target_relative: relative,
                kind: control.kind,
                device: identity.device(),
                inode: identity.inode(),
                size_bytes: file.byte_len().expect("target control size should load"),
                owner: policy.owner,
                group: policy.group,
                mode: policy.mode,
                link_count: Some(policy.link_count),
                sha256,
            }
        })
        .collect::<Vec<_>>();
    let validated_target_controls = downloader::inspect_video_staging_for_migration(
        &target_root,
        target_staging.strip_prefix(&fixture.archive_root).unwrap(),
        target_job_identity,
        &target_controls,
    )
    .expect("recovered target staging controls should validate");
    assert_eq!(validated_target_controls.len(), target_controls.len());

    reopened_runtime.signal_epoch_fatal();
    migration_watcher
        .await
        .expect("migration watcher should stop after runtime shutdown");
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn migration_recovers_target_installed_task_before_engine_ack_after_restart() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-target-installed-before-ack",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let retained = downloader::create_test_retained_video_staging(&fixture.video_root)
        .expect("retained staging directory should be created");
    let fragment = vec![0x5a; 8 * 1024 * 1024 + 137];
    fs::write(retained.join("segment.part"), &fragment)
        .expect("synthetic incomplete media fragment should be written");
    let mut task = TaskRecord::new(
        "migration-target-installed-task".into(),
        1_401,
        1_401,
        123_456_789,
        Some(123_456_789),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=abcdefghijk".into(),
        },
    );
    task.status = TaskStatus::Failed;
    task.error = Some("synthetic interrupted video transfer".to_string());
    task.staging_attempts.push(retained.clone());
    queue
        .create(task)
        .expect("failed task with retained staging should persist");

    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let snapshot = queue
        .list_incomplete_task_transfer_snapshots(&fixture.source_library_id, 16)
        .expect("incomplete task should be included in the frozen preview")[0]
        .clone();
    let engine = migration_engine_for_test(
        &runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    let preview = engine
        .preview(&crate::library_migration::MigrationRequest {
            scope: crate::library_migration::MigrationScope::Library,
            item_ids: Vec::new(),
            conflict: crate::library::ConflictPolicy::Skip,
            item_conflicts: Default::default(),
            include_incomplete_tasks: true,
            set_target_default_on_success: false,
            task_impacts: vec![crate::library_migration::MigrationTaskImpact {
                task_id: snapshot.record.id.clone(),
                generation: snapshot.record.generation,
                revision: snapshot.record.revision,
                staging_bytes: snapshot.staging_bytes,
                staging_file_count: snapshot.staging_file_count,
                published_item_ids: Vec::new(),
            }],
            warnings: Vec::new(),
        })
        .expect("migration preview should bind the retained task impact");
    let queued = engine
        .confirm(&preview.id, &preview.revision)
        .expect("confirmed migration should be durable before running");
    assert_eq!(
        queued.status,
        crate::library_migration::MigrationRunStatus::Queued
    );
    engine
        .set_runtime_context(
            &preview.id,
            crate::library_migration::MigrationRuntimeContext {
                database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
                preview_seq: 1_401,
                chat_id: None,
                message_id: None,
                delivery: crate::library_migration::MigrationDeliveryState::Pending,
            },
        )
        .expect("migration should carry the current cloud epoch for recovery");

    let target = queue
        .registry_snapshot()
        .expect("media library registry should load")
        .libraries
        .into_iter()
        .find(|library| library.id == fixture.target_library_id)
        .expect("target library should remain registered");
    let token = queue
        .begin_task_destination_transfer(
            &snapshot.record.id,
            &preview.id,
            snapshot.record.generation,
            snapshot.record.revision,
            crate::media_libraries::TaskDestination {
                library_id: target.id,
                root: target.root,
                category: snapshot.destination.category.clone(),
            },
        )
        .expect("queue should persist the task transfer token");
    queue
        .wait_for_task_destination_transfer(&token)
        .await
        .expect("task writer should settle before plan creation");
    let plan = queue
        .task_transfer_plan(&token)
        .expect("queue should bind retained files and controls into a transfer plan");
    assert!(plan.files.iter().any(|file| {
        file.source_relative.ends_with("segment.part") && file.size_bytes == fragment.len() as u64
    }));
    assert!(plan.controls.iter().any(|control| {
        control.kind == crate::media_libraries::TaskTransferControlKind::Retention
    }));
    engine
        .attach_task_transfer(&preview.id, plan.clone())
        .expect("engine should durably attach the queue transfer plan");

    let migration_id = preview.id.clone();
    let copied = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let worker = engine.clone();
            let id = migration_id.clone();
            let status = tokio::task::spawn_blocking(move || worker.step(&id))
                .await
                .expect("migration checkpoint should join")
                .expect("migration checkpoint should succeed");
            assert_ne!(
                status.status,
                crate::library_migration::MigrationRunStatus::Failed,
                "private task transfer should not fail: {status:?}"
            );
            if status.awaiting_task_commit {
                return status;
            }
        }
    })
    .await
    .expect("private task fragment should finish bounded copy and verification checkpoints");
    let saved_plans = engine
        .task_transfer_plans(&preview.id)
        .expect("durable migration should retain the exact attached plan");
    assert_eq!(saved_plans.len(), 1);
    assert_eq!(saved_plans[0].transfer.transfer_id, token.transfer_id);
    let installed = queue
        .commit_task_destination_transfer(&saved_plans[0])
        .expect("target queue record should install before engine acknowledgment");
    assert_eq!(
        installed.destination_transfer_phase,
        Some(crate::media_libraries::TaskDestinationTransferPhase::TargetInstalled)
    );
    assert_eq!(
        installed.destination.as_ref().unwrap().library_id,
        fixture.target_library_id
    );
    let target_staging = installed
        .staging_attempts
        .first()
        .expect("installed task should point at the migrated staging attempt")
        .clone();
    assert!(target_staging.starts_with(&fixture.archive_root));
    assert_eq!(
        fs::read(target_staging.join("segment.part")).unwrap(),
        fragment
    );
    assert!(retained.join("segment.part").exists());
    queue
        .verify_task_transfer_plan(&saved_plans[0])
        .expect("recovery controls and both retained fragment copies should validate");
    let awaiting = engine
        .status(&preview.id)
        .expect("migration status should remain readable before restart")
        .expect("confirmed migration should retain status");
    assert!(awaiting.awaiting_task_commit);
    assert!(!awaiting.task_transfers[0].commit_acked);
    assert_eq!(
        copied.status,
        crate::library_migration::MigrationRunStatus::Running
    );

    runtime.signal_epoch_fatal();
    drop(engine);
    drop(runtime);
    drop(inbox);
    drop(queue);

    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reopen with target installed"),
    );
    let target_record = reopened_queue
        .get(&snapshot.record.id)
        .expect("installed target task should resolve")
        .expect("target record should remain durable after restart");
    assert_eq!(
        target_record.destination_transfer_phase,
        Some(crate::media_libraries::TaskDestinationTransferPhase::TargetInstalled)
    );
    assert_eq!(
        fs::read(target_staging.join("segment.part")).unwrap(),
        fragment
    );
    assert!(retained.join("segment.part").exists());
    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, crate::cloud::TEST_DATABASE_EPOCH)
            .expect("same-epoch cloud inbox should reopen"),
    );
    let reopened_runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&reopened_queue),
        Arc::clone(&reopened_inbox),
    );
    let migration_watcher = tokio::spawn(crate::library_migration_bot::watch(
        reopened_runtime.clone(),
    ));
    let recovered = wait_for_migration_status(&reopened_runtime, &preview.id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Completed
    })
    .await;
    wait_for_migration_runner(&reopened_runtime, &preview.id, false).await;
    assert_eq!(recovered.success_count, 0);
    assert_eq!(recovered.failed_count, 0);
    assert!(recovered.task_transfers[0].commit_acked);
    let recovered_task = reopened_queue
        .get(&snapshot.record.id)
        .expect("recovered task should load")
        .expect("task should remain in target queue");
    assert_eq!(recovered_task.status, TaskStatus::Failed);
    assert_eq!(
        recovered_task.destination.as_ref().unwrap().library_id,
        fixture.target_library_id
    );
    assert!(recovered_task.destination_transfer.is_none());
    assert!(recovered_task.destination_transfer_phase.is_none());
    assert_eq!(
        recovered_task.staging_attempts.first(),
        Some(&target_staging)
    );
    assert_eq!(
        fs::read(target_staging.join("segment.part")).unwrap(),
        fragment
    );

    reopened_runtime.signal_epoch_fatal();
    migration_watcher
        .await
        .expect("migration watcher should stop after runtime shutdown");
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn stopped_migration_reconciles_pending_task_transfer_after_restart() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-stopped-task-transfer-restart",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let retained = downloader::create_test_retained_video_staging(&fixture.video_root)
        .expect("retained staging directory should be created");
    fs::write(retained.join("segment.part"), b"stopped task payload")
        .expect("synthetic task payload should write");
    let task_id = "migration-stopped-task-transfer";
    let mut task = TaskRecord::new(
        task_id.into(),
        1_801,
        1_801,
        123_456_789,
        Some(123_456_789),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=abcdefghijk".into(),
        },
    );
    task.status = TaskStatus::Failed;
    task.error = Some("synthetic interrupted video transfer".to_string());
    task.staging_attempts.push(retained.clone());
    queue
        .create(task)
        .expect("failed task with retained staging should persist");

    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let snapshot = queue
        .list_incomplete_task_transfer_snapshots(&fixture.source_library_id, 16)
        .expect("failed task should be included in the preview")[0]
        .clone();
    let engine = migration_engine_for_test(
        &runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    let preview = engine
        .preview(&crate::library_migration::MigrationRequest {
            scope: crate::library_migration::MigrationScope::Library,
            item_ids: Vec::new(),
            conflict: crate::library::ConflictPolicy::Skip,
            item_conflicts: Default::default(),
            include_incomplete_tasks: true,
            set_target_default_on_success: false,
            task_impacts: vec![crate::library_migration::MigrationTaskImpact {
                task_id: task_id.to_string(),
                generation: snapshot.record.generation,
                revision: snapshot.record.revision,
                staging_bytes: snapshot.staging_bytes,
                staging_file_count: snapshot.staging_file_count,
                published_item_ids: Vec::new(),
            }],
            warnings: Vec::new(),
        })
        .expect("preview should bind the unfinished task");
    engine
        .confirm(&preview.id, &preview.revision)
        .expect("migration should persist before any task transfer");
    engine
        .set_runtime_context(
            &preview.id,
            crate::library_migration::MigrationRuntimeContext {
                database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
                preview_seq: 1_801,
                chat_id: None,
                message_id: None,
                delivery: crate::library_migration::MigrationDeliveryState::Pending,
            },
        )
        .expect("migration should persist its current cloud epoch");
    let target = queue
        .registry_snapshot()
        .expect("library registry should load")
        .libraries
        .into_iter()
        .find(|library| library.id == fixture.target_library_id)
        .expect("target library should remain registered");
    let token = queue
        .begin_task_destination_transfer(
            task_id,
            &preview.id,
            snapshot.record.generation,
            snapshot.record.revision,
            crate::media_libraries::TaskDestination {
                library_id: target.id,
                root: target.root,
                category: snapshot.destination.category.clone(),
            },
        )
        .expect("queue should durably mark the task transfer pending");
    queue
        .wait_for_task_destination_transfer(&token)
        .await
        .expect("task writer should settle before the simulated crash");
    let stopped = engine
        .control(
            &preview.id,
            crate::library_migration::MigrationControl::StopRemaining,
        )
        .expect("stop should be durably recorded");
    assert_eq!(
        stopped.status,
        crate::library_migration::MigrationRunStatus::Stopped
    );
    assert!(
        queue
            .get(task_id)
            .expect("task should remain readable")
            .expect("task should remain registered")
            .destination_transfer
            .is_some(),
        "the crash window should leave the task token pending"
    );

    runtime.signal_epoch_fatal();
    drop(engine);
    drop(runtime);
    drop(inbox);
    drop(queue);

    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reopen after stop crash"),
    );
    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, crate::cloud::TEST_DATABASE_EPOCH)
            .expect("same-epoch inbox should reopen"),
    );
    let reopened_runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&reopened_queue),
        Arc::clone(&reopened_inbox),
    );
    let watcher = tokio::spawn(crate::library_migration_bot::watch(
        reopened_runtime.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let current = reopened_queue
                .get(task_id)
                .expect("recovered task should load")
                .expect("recovered task should remain registered");
            if current.destination_transfer.is_none() {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Stopped recovery should reconcile the pending transfer token");
    let restored = reopened_queue
        .get(task_id)
        .expect("restored task should load")
        .expect("restored task should remain registered");
    assert_eq!(restored.status, TaskStatus::Failed);
    assert_eq!(
        restored.destination.as_ref().unwrap().library_id,
        fixture.source_library_id
    );
    assert_eq!(restored.staging_attempts.first(), Some(&retained));
    assert_eq!(
        fs::read(retained.join("segment.part")).unwrap(),
        b"stopped task payload"
    );
    let recovered_status = wait_for_migration_status(&reopened_runtime, &preview.id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Stopped
    })
    .await;
    assert_eq!(
        recovered_status.status,
        crate::library_migration::MigrationRunStatus::Stopped
    );

    reopened_runtime.signal_epoch_fatal();
    watcher
        .await
        .expect("migration watcher should stop after runtime shutdown");
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn migration_default_update_preserves_a_newer_user_library_choice() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-default-update-cas",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    fs::write(
        fixture.video_root.join("default-cas.mp4"),
        b"CAS migration media",
    )
    .expect("source media should write");
    let later_default_root = fixture.root.join("later-default");
    let later_default = queue
        .register_video_library("Later default", &later_default_root, None, false, true)
        .expect("later user-selected library should register");
    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let output_guard = hold_migration_output_slot(&fixture).await;
    let preview_record = run_library_management_test_request(
        &runtime,
        1_901,
        "library_migration_preview",
        serde_json::json!({
            "source_library_id": fixture.source_library_id,
            "target_library_id": fixture.target_library_id,
            "scope": "library",
            "conflict": "skip",
            "set_target_default_on_success": true,
        }),
    )
    .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = preview_record.result.unwrap();
    let confirmation = run_library_management_test_request(
        &runtime,
        1_902,
        "library_migration_confirm",
        serde_json::json!({
            "preview_id": preview["id"],
            "revision": preview["revision"],
        }),
    )
    .await;
    assert_eq!(
        confirmation.status,
        InboxStatus::Completed,
        "{:?}",
        confirmation.error
    );
    let migration_id = preview["id"].as_str().unwrap().to_string();
    wait_for_migration_runner(&runtime, &migration_id, true).await;

    let engine = migration_engine_for_test(
        &runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    let expected_revision = queue
        .registry_snapshot()
        .expect("library registry should load")
        .revision;
    engine
        .bind_default_update_expected_revision(&migration_id, expected_revision)
        .expect("first registry revision should be frozen before execution");
    queue
        .set_default_video_library(&later_default.id)
        .expect("user should be able to choose a different default during migration");
    drop(output_guard);

    let completed = wait_for_migration_status(&runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Completed
            && status.default_update_resolved
    })
    .await;
    wait_for_migration_runner(&runtime, &migration_id, false).await;
    assert!(!completed.default_update_applied);
    assert!(
        completed
            .warnings
            .iter()
            .any(|warning| warning.contains("settings changed"))
    );
    assert_eq!(
        queue
            .default_video_destination()
            .expect("default destination should still resolve")
            .library_id,
        later_default.id,
        "migration CAS must preserve the user's newer default choice"
    );

    runtime.signal_epoch_fatal();
    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn refreshed_migration_preview_stales_old_inbox_receipt_and_confirmation() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-preview-supersession-restart-boundary",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    fs::write(
        fixture.video_root.join("refresh.mp4"),
        b"refresh source media",
    )
    .expect("source media should write");
    fs::write(
        fixture.archive_root.join("refresh.mp4"),
        b"existing target media",
    )
    .expect("conflicting target media should write");
    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let first = run_library_management_test_request(
        &runtime,
        2_001,
        "library_migration_preview",
        serde_json::json!({
            "source_library_id": fixture.source_library_id,
            "target_library_id": fixture.target_library_id,
            "scope": "library",
            "conflict": "skip",
        }),
    )
    .await;
    assert_eq!(
        first.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        first.error
    );
    let first_preview = first.result.expect("initial preview should be persisted");
    let item_id = first_preview["items"][0]["item_id"]
        .as_str()
        .expect("preview should expose its selected media ID")
        .to_string();

    let refreshed = run_library_management_test_request(
        &runtime,
        2_002,
        "library_migration_preview",
        serde_json::json!({
            "source_library_id": fixture.source_library_id,
            "target_library_id": fixture.target_library_id,
            "scope": "library",
            "conflict": "skip",
            "item_conflicts": {item_id: "keep_both"},
            "supersedes_preview_seq": 2_001,
            "supersedes_preview_id": first_preview["id"],
            "supersedes_revision": first_preview["revision"],
        }),
    )
    .await;
    assert_eq!(
        refreshed.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        refreshed.error
    );
    let refreshed_preview = refreshed.result.expect("refreshed preview should persist");
    assert_ne!(refreshed_preview["id"], first_preview["id"]);
    assert_eq!(
        inbox.record(2_001).unwrap().unwrap().status,
        InboxStatus::Stale,
        "the durable prior inbox receipt should become stale with the refresh"
    );
    let reopened_engine = migration_engine_for_test(
        &runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    let durable_old_confirmation = reopened_engine
        .confirm(
            first_preview["id"].as_str().unwrap(),
            first_preview["revision"].as_str().unwrap(),
        )
        .expect_err("persisted supersession should survive reopening the library managers");
    assert!(durable_old_confirmation.to_string().contains("superseded"));
    let old_confirmation = run_library_management_test_request(
        &runtime,
        2_003,
        "library_migration_confirm",
        serde_json::json!({
            "preview_id": first_preview["id"],
            "revision": first_preview["revision"],
        }),
    )
    .await;
    assert_eq!(old_confirmation.status, InboxStatus::Failed);
    let new_confirmation = run_library_management_test_request(
        &runtime,
        2_004,
        "library_migration_confirm",
        serde_json::json!({
            "preview_id": refreshed_preview["id"],
            "revision": refreshed_preview["revision"],
        }),
    )
    .await;
    assert_eq!(
        new_confirmation.status,
        InboxStatus::Completed,
        "{:?}",
        new_confirmation.error
    );
    let migration_id = refreshed_preview["id"].as_str().unwrap().to_string();
    let completed = wait_for_migration_status(&runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Completed
    })
    .await;
    assert_eq!(completed.success_count, 1);
    assert!(fixture.archive_root.join("refresh.mp4").exists());
    assert!(fixture.archive_root.join("refresh (2).mp4").exists());
    wait_for_migration_runner(&runtime, &migration_id, false).await;

    runtime.signal_epoch_fatal();
    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}
