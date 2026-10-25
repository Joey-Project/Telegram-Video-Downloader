use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::media_library_e2e_tests::{create_migration_fixture, scan_file_item_id};
use super::*;
use crate::file_provider::{FileProviderAccessError, QueueFileProvider};
use crate::library::{PhysicalItemIdentity, Presence};
use anyhow::Result;
use serde_json::Value;

const SNAPSHOT_TEST_CHAT_ID: i64 = 123_456_789;

async fn wait_for_snapshot_migration_completion(
    runtime: &CloudBotRuntime,
    migration_id: &str,
) -> crate::library_migration::MigrationStatus {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let status = runtime.migrations.lock().await.get(migration_id).cloned();
            if let Some(status) = status
                && status.status == crate::library_migration::MigrationRunStatus::Completed
            {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("mock migration should complete within the test deadline")
}

async fn wait_for_snapshot_migration_runner_idle(runtime: &CloudBotRuntime, migration_id: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if !runtime
                .migration_runners
                .lock()
                .await
                .contains(migration_id)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("mock migration runner should settle before scanning");
}

async fn rescan_snapshot_library(runtime: &CloudBotRuntime, seq: u64, library_id: &str) -> Value {
    let record = run_library_management_test_request(
        runtime,
        seq,
        "library_scan",
        serde_json::json!({ "library_id": library_id }),
    )
    .await;
    assert_eq!(record.status, InboxStatus::Completed, "{:?}", record.error);
    record
        .result
        .expect("library scan should return its snapshot")
}

async fn start_selected_snapshot_migration(
    runtime: &CloudBotRuntime,
    seq: u64,
    source_library_id: &str,
    target_library_id: &str,
    item_id: &str,
) -> String {
    let preview_record = run_library_management_test_request(
        runtime,
        seq,
        "library_migration_preview",
        bound_migration_preview_payload(
            &runtime.bot.queue,
            source_library_id,
            target_library_id,
            serde_json::json!({
                "scope": "selected",
                "item_ids": [item_id],
                "conflict": "skip",
                "include_incomplete_tasks": false,
            }),
        ),
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
        .expect("migration preview should persist before confirmation");
    let migration_id = preview["id"]
        .as_str()
        .expect("migration preview should contain its durable ID")
        .to_string();
    let confirmation = run_library_management_test_request(
        runtime,
        seq + 1,
        "library_migration_confirm",
        serde_json::json!({ "preview_id": preview["id"], "revision": preview["revision"] }),
    )
    .await;
    assert_eq!(
        confirmation.status,
        InboxStatus::Completed,
        "{:?}",
        confirmation.error
    );
    migration_id
}

fn snapshot_item<'a>(items: &'a [Value], item_id: &str) -> &'a Value {
    items
        .iter()
        .find(|item| item["id"] == item_id)
        .expect("snapshot should retain the requested library item")
}

fn snapshot_library_view<'a>(library: &'a Value, library_id: &str) -> &'a Value {
    library["libraries"]
        .as_array()
        .expect("cloud library snapshot should include registered libraries")
        .iter()
        .find(|view| view["id"] == library_id)
        .expect("cloud snapshot should include the registered library view")
}

fn snapshot_library_items(library: &Value, library_id: &str) -> Vec<Value> {
    if library["default_library_id"] == library_id {
        return library["items"]
            .as_array()
            .expect("default library snapshot should retain its top-level items")
            .clone();
    }
    snapshot_library_view(library, library_id)["snapshot"]["items"]
        .as_array()
        .expect("non-default library snapshot should retain nested items")
        .clone()
}

async fn cached_item_physical_identity(
    runtime: &CloudBotRuntime,
    library_id: &str,
    item_id: &str,
) -> PhysicalItemIdentity {
    runtime
        .library_snapshots
        .lock()
        .await
        .get(library_id)
        .expect("library scan should cache its snapshot")
        .items
        .iter()
        .find(|item| item.id == item_id)
        .expect("cached snapshot should retain the requested item")
        .physical_identity
        .expect("scanned file should carry its in-memory physical identity")
}

async fn inject_cached_item_physical_identity(
    runtime: &CloudBotRuntime,
    library_id: &str,
    item_id: &str,
    identity: PhysicalItemIdentity,
) {
    let mut snapshots = runtime.library_snapshots.lock().await;
    let item = snapshots
        .get_mut(library_id)
        .expect("library scan should cache its snapshot")
        .items
        .iter_mut()
        .find(|item| item.id == item_id)
        .expect("cached snapshot should retain the requested item");
    item.physical_identity = Some(identity);
}

fn same_object_with_different_birth(mut identity: PhysicalItemIdentity) -> PhysicalItemIdentity {
    assert!(
        identity.has_complete_birth(),
        "fixture should expose complete physical creation time"
    );
    identity.birth_seconds = Some(
        identity
            .birth_seconds
            .expect("fixture should expose complete physical creation time")
            .saturating_add(1),
    );
    identity
}

#[tokio::test]
async fn cloud_snapshot_projects_video_library_ids_and_authorized_incomplete_counts() {
    let root = temp_cloud_bot_dir("library-snapshot-task-projection");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.telegram.allowed_chat_ids = vec![SNAPSHOT_TEST_CHAT_ID];
    config
        .ensure_runtime_dirs()
        .expect("snapshot fixture roots should create");
    let archive_root = root.join("archive");
    fs::create_dir_all(&archive_root).expect("archive root should create");
    fs::write(archive_root.join("queued.mp4"), b"queued media")
        .expect("queued fixture media should write");

    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let source_id = queue
        .default_video_destination()
        .expect("default video library should resolve")
        .library_id;
    let archive = queue
        .register_video_library("Archive", &archive_root, None, false, false)
        .expect("non-default library should register");
    let archive_destination = queue
        .video_destination(&archive.id, None)
        .expect("archive destination should resolve");

    let mut queued = TaskRecord::new(
        "snapshot-queued-video".to_string(),
        1,
        1,
        SNAPSHOT_TEST_CHAT_ID,
        Some(SNAPSHOT_TEST_CHAT_ID),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=abcdefghijk".to_string(),
        },
    );
    queued.status = TaskStatus::Queued;
    queued.destination = Some(archive_destination);
    // A malformed recorded staging path must not hide the task count. The queue summary API
    // reports the count and marks the byte estimate unknown for this library.
    queued
        .staging_attempts
        .push(archive_root.join("queued.mp4"));
    queue
        .create(queued)
        .expect("queued video task should persist");

    let mut legacy = TaskRecord::new(
        "snapshot-legacy-failed-video".to_string(),
        2,
        2,
        SNAPSHOT_TEST_CHAT_ID,
        Some(SNAPSHOT_TEST_CHAT_ID),
        1,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=zyxwvutsrqp".to_string(),
        },
    );
    legacy.status = TaskStatus::Failed;
    legacy.error = Some("synthetic interrupted video".to_string());
    queue
        .create(legacy)
        .expect("legacy-shaped video task should persist before migration simulation");
    let legacy_path = config
        .downloads
        .video_dir
        .join(".telegram-video-downloader-queue")
        .join("task-snapshot-legacy-failed-video.json");
    let mut legacy_record: serde_json::Value =
        serde_json::from_slice(&fs::read(&legacy_path).expect("legacy task should read"))
            .expect("legacy task JSON should decode");
    legacy_record
        .as_object_mut()
        .expect("task record should be an object")
        .remove("destination");
    fs::write(
        &legacy_path,
        serde_json::to_vec(&legacy_record).expect("legacy record should encode"),
    )
    .expect("legacy task should be rewritten without a frozen destination");

    let mut pdf = TaskRecord::new(
        "snapshot-pdf-failed".to_string(),
        3,
        3,
        SNAPSHOT_TEST_CHAT_ID,
        Some(SNAPSHOT_TEST_CHAT_ID),
        2,
        JobRequest::Pdf {
            url: "https://example.invalid/document.pdf".to_string(),
        },
    );
    pdf.status = TaskStatus::Failed;
    queue.create(pdf).expect("PDF task should persist");

    let mut unauthorized = TaskRecord::new(
        "snapshot-unauthorized-video".to_string(),
        4,
        4,
        9_999,
        Some(9_999),
        3,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=mnopqrstuvw".to_string(),
        },
    );
    unauthorized.status = TaskStatus::Failed;
    unauthorized.destination = Some(archive_destination_for_test(&queue, &archive.id));
    queue
        .create(unauthorized)
        .expect("unauthorized task should persist");

    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            "test-token".to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let source_cache = empty_cloud_test_library_snapshot("source-cached");
    let archive_cache = empty_cloud_test_library_snapshot("archive-cached");
    *runtime.library_snapshot.lock().await = Some(source_cache.clone());
    runtime
        .library_snapshots
        .lock()
        .await
        .insert(source_id.clone(), source_cache);
    runtime
        .library_snapshots
        .lock()
        .await
        .insert(archive.id.clone(), archive_cache);

    let snapshot = build_cloud_state_snapshot(&runtime)
        .await
        .expect("cloud snapshot should build")
        .expect("runtime should publish a snapshot");
    let tasks = snapshot.tasks.as_slice();
    assert_eq!(
        tasks.len(),
        3,
        "authorized active tasks should be projected"
    );
    let task_by_id = tasks
        .iter()
        .map(|task| (task["id"].as_str().unwrap(), task))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(task_by_id["snapshot-queued-video"]["kind"], "video");
    assert_eq!(
        task_by_id["snapshot-queued-video"]["library_id"],
        archive.id
    );
    assert_eq!(task_by_id["snapshot-legacy-failed-video"]["kind"], "video");
    assert_eq!(
        task_by_id["snapshot-legacy-failed-video"]["library_id"],
        source_id
    );
    assert_eq!(task_by_id["snapshot-pdf-failed"]["kind"], "pdf");
    assert!(task_by_id["snapshot-pdf-failed"]["library_id"].is_null());
    assert!(task_by_id["snapshot-queued-video"].get("root").is_none());
    assert!(!task_by_id.contains_key("snapshot-unauthorized-video"));

    let libraries = snapshot.library["libraries"]
        .as_array()
        .expect("library descriptors should be projected");
    let source_view = libraries
        .iter()
        .find(|view| view["id"] == source_id)
        .expect("source library view should exist");
    let archive_view = libraries
        .iter()
        .find(|view| view["id"] == archive.id)
        .expect("archive library view should exist");
    assert_eq!(source_view["incomplete_task_count"], 1);
    assert_eq!(archive_view["incomplete_task_count"], 1);
    assert!(
        source_view["staging_bytes"].as_u64().is_some(),
        "legacy task without fragments should have a known estimate"
    );
    assert!(
        archive_view["staging_bytes"].is_null(),
        "one malformed recorded staging path should preserve count but make bytes unknown"
    );

    let mut allow_all_runtime = runtime.clone();
    let mut allow_all_config = (*runtime.bot.config).clone();
    allow_all_config.telegram.allow_all_chats = true;
    allow_all_runtime.bot.config = Arc::new(allow_all_config);
    let private_snapshot = build_cloud_state_snapshot(&allow_all_runtime)
        .await
        .expect("allow-all snapshot should build")
        .expect("runtime should publish an allow-all snapshot");
    assert!(private_snapshot.tasks.is_empty());
    for view in private_snapshot.library["libraries"].as_array().unwrap() {
        assert!(view["incomplete_task_count"].is_null());
        assert!(view["staging_bytes"].is_null());
    }

    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_snapshot_compacts_large_migration_task_details_with_stable_aggregates() {
    let root = temp_cloud_bot_dir("library-snapshot-migration-budget");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.telegram.allowed_chat_ids = vec![SNAPSHOT_TEST_CHAT_ID];
    config
        .ensure_runtime_dirs()
        .expect("snapshot fixture roots should create");
    let archive_root = root.join("archive");
    fs::create_dir_all(&archive_root).expect("archive root should create");

    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let source_id = queue
        .default_video_destination()
        .expect("default video library should resolve")
        .library_id;
    let archive = queue
        .register_video_library("Archive", &archive_root, None, false, false)
        .expect("archive library should register");
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            "test-token".to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );

    // Keep the hydrated history at its 32-entry bound. Each record has no media items,
    // many task handoffs, and conflict overrides, so transfer rows alone exceed the
    // upload soft limit before the reporter's reducer runs.
    let large_error = "synthetic task transfer failure detail ".repeat(480);
    for migration_index in 0..32 {
        let migration_id = format!("migration-budget-{migration_index:02}");
        let mut task_transfers = Vec::new();
        for transfer_index in 0..4 {
            task_transfers.push(serde_json::json!({
                "task_id": format!("completed-task-{migration_index}-{transfer_index}"),
                "transfer_id": format!("completed-transfer-{migration_index}-{transfer_index}"),
                "completed_files": 1,
                "total_files": 1,
                "copy_complete": true,
                "commit_acked": true,
                "cleaned_source_files": 1,
                "cleanup_total_files": 1,
                "source_cleanup_complete": true,
                "error": null,
            }));
        }
        for transfer_index in 0..2 {
            task_transfers.push(serde_json::json!({
                "task_id": format!("active-task-{migration_index}-{transfer_index}"),
                "transfer_id": format!("active-transfer-{migration_index}-{transfer_index}"),
                "completed_files": 0,
                "total_files": 2,
                "copy_complete": false,
                "commit_acked": false,
                "cleaned_source_files": 0,
                "cleanup_total_files": 2,
                "source_cleanup_complete": false,
                "error": null,
            }));
        }
        task_transfers.push(serde_json::json!({
            "task_id": format!("acked-pending-cleanup-task-{migration_index}"),
            "transfer_id": format!("acked-pending-cleanup-transfer-{migration_index}"),
            "completed_files": 1,
            "total_files": 1,
            "copy_complete": true,
            "commit_acked": true,
            "cleaned_source_files": 0,
            "cleanup_total_files": 1,
            "source_cleanup_complete": false,
            "error": null,
        }));
        for transfer_index in 0..24 {
            task_transfers.push(serde_json::json!({
                "task_id": format!("failed-task-{migration_index}-{transfer_index}"),
                "transfer_id": format!("failed-transfer-{migration_index}-{transfer_index}"),
                "completed_files": 0,
                "total_files": 2,
                "copy_complete": false,
                "commit_acked": false,
                "cleaned_source_files": 0,
                "cleanup_total_files": 2,
                "source_cleanup_complete": false,
                "error": large_error,
            }));
        }
        let item_conflicts = (0..128)
            .map(|index| (format!("media-item-{migration_index}-{index}"), "keep_both"))
            .collect::<BTreeMap<_, _>>();
        let status = serde_json::from_value::<crate::library_migration::MigrationStatus>(
            serde_json::json!({
                "id": migration_id,
                "preview_id": format!("preview-{migration_index}"),
                "revision": "revision-1",
                "created_at": migration_index + 1,
                "status": "partial",
                "source_library_id": source_id,
                "target_library_id": archive.id,
                "scope": "library",
                "conflict": "skip",
                "item_conflicts": item_conflicts,
                "include_incomplete_tasks": true,
                "set_target_default_on_success": false,
                "default_update_applied": false,
                "default_update_resolved": false,
                "total_items": 0,
                "completed_items": 0,
                "success_count": 0,
                "skipped_count": 0,
                "failed_count": 0,
                "total_files": 0,
                "completed_files": 0,
                "copied_bytes": 0,
                "total_bytes": 0,
                "required_space_bytes": 0,
                "available_space_bytes": 0,
                "incomplete_task_count": 27,
                "staging_bytes": 0,
                "awaiting_task_plans": false,
                "awaiting_task_commit": false,
                "warnings": [],
                "items": [],
                "task_transfers": task_transfers,
            }),
        )
        .expect("migration status fixture should match the persisted wire contract");
        runtime
            .migrations
            .lock()
            .await
            .insert(status.id.clone(), status);
    }

    let mut snapshot = build_cloud_state_snapshot(&runtime)
        .await
        .expect("hydrated cloud snapshot should build")
        .expect("runtime should publish a snapshot");
    assert_eq!(snapshot.library["migrations"].as_array().unwrap().len(), 32);
    let initial_size = snapshot
        .encoded_size(1)
        .expect("hydrated state size should serialize");
    assert!(
        initial_size > CLOUD_STATE_UPLOAD_SOFT_LIMIT_BYTES,
        "fixture must exceed the upload limit before compaction: {initial_size} bytes"
    );

    assert!(
        snapshot
            .reduce_after_payload_too_large(1, CLOUD_STATE_UPLOAD_SOFT_LIMIT_BYTES)
            .expect("default upload-budget reduction should succeed")
    );
    assert!(
        snapshot
            .encoded_size(1)
            .expect("reduced state should serialize")
            <= CLOUD_STATE_UPLOAD_SOFT_LIMIT_BYTES
    );
    for migration in snapshot.library["migrations"].as_array().unwrap() {
        let transfers = migration["task_transfers"].as_array().unwrap();
        assert_eq!(transfers.len(), 27);
        assert!(
            transfers.iter().any(|transfer| {
                transfer["commit_acked"] == true && transfer["source_cleanup_complete"] == false
            }),
            "queue acknowledgement alone must not compact an unfinished source cleanup"
        );
        assert!(
            transfers.iter().all(|transfer| {
                transfer["commit_acked"] != true || transfer["source_cleanup_complete"] != true
            }),
            "fully acknowledged and cleaned handoffs should compact away"
        );
        assert_eq!(migration["truncated_task_transfer_error_count"], 24);
    }

    // Follow-up 413 reductions act on the already-trimmed in-memory snapshot. Aggregates
    // must remain totals, while omitted counts track only rows no longer visible.
    for budget in [256 * 1024, 128 * 1024] {
        snapshot
            .reduce_after_payload_too_large(1, budget)
            .expect("smaller remote upload budgets should keep reducing the snapshot");
        let final_size = snapshot
            .encoded_size(1)
            .expect("reduced state should serialize");
        assert!(
            final_size <= budget,
            "final hydrated state is {final_size} bytes for {budget}-byte budget"
        );
        let migrations = snapshot.library["migrations"].as_array().unwrap();
        assert_eq!(migrations.len(), 32);
        for migration in migrations {
            let transfers = migration["task_transfers"].as_array().unwrap();
            let visible_errors = transfers
                .iter()
                .filter(|transfer| {
                    transfer["error"]
                        .as_str()
                        .is_some_and(|error| !error.trim().is_empty())
                })
                .count() as u64;
            assert_eq!(migration["task_transfer_count"], 31);
            assert_eq!(migration["task_transfer_error_count"], 24);
            assert_eq!(
                migration["omitted_task_transfer_count"].as_u64(),
                Some(31 - transfers.len() as u64)
            );
            assert_eq!(
                migration["omitted_task_transfer_error_count"].as_u64(),
                Some(24 - visible_errors)
            );
            assert_eq!(migration["conflict_override_count"], 128);
            assert_eq!(migration["omitted_conflict_override_count"], 128);
            assert_eq!(migration["item_conflicts"], serde_json::json!({}));
            assert_eq!(migration["items"], serde_json::json!([]));
            assert_eq!(migration["incomplete_task_count"], 27);
            assert_eq!(migration["truncated_task_transfer_error_count"], 24);
            assert!(migration["details_incomplete"].as_bool() == Some(true));
            assert!(
                migration["warnings"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|warning| warning
                        .as_str()
                        .is_some_and(|text| text.contains("summarized")))
            );
            assert!(
                transfers
                    .iter()
                    .all(|transfer| transfer["commit_acked"] != true
                        || transfer["source_cleanup_complete"] != true),
                "fully acknowledged and cleaned handoffs should compact away"
            );
        }
    }

    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn moved_history_follows_verified_migration_targets_after_restart_and_degrades_safely() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "moved-history-provenance",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let final_root = fixture.root.join("final-archive");
    fs::create_dir_all(&final_root).expect("final archive root should create");
    let final_library = queue
        .register_video_library("Final Archive", &final_root, None, false, false)
        .expect("final archive should register");
    let media_bytes = b"synthetic moved-history video bytes";
    let nfo_bytes = b"<movie><title>Moved history fixture</title></movie>";
    fs::write(fixture.video_root.join("clip.mp4"), media_bytes).expect("source media should write");
    fs::write(fixture.video_root.join("clip.nfo"), nfo_bytes).expect("source sidecar should write");

    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let source_scan = rescan_snapshot_library(&runtime, 9_101, &fixture.source_library_id).await;
    let original_source_id = source_scan["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["relative_path"] == "clip.mp4" && item["presence"] == "present")
        .expect("source scan should identify the present media")["id"]
        .as_str()
        .expect("source item should have its persistent identity")
        .to_string();
    let first_migration_id = start_selected_snapshot_migration(
        &runtime,
        9_102,
        &fixture.source_library_id,
        &fixture.target_library_id,
        &original_source_id,
    )
    .await;
    let first_status = wait_for_snapshot_migration_completion(&runtime, &first_migration_id).await;
    assert_eq!(first_status.success_count, 1);
    wait_for_snapshot_migration_runner_idle(&runtime, &first_migration_id).await;
    assert!(!fixture.video_root.join("clip.mp4").exists());
    assert!(!fixture.video_root.join("clip.nfo").exists());
    assert_eq!(
        fs::read(fixture.archive_root.join("clip.mp4")).unwrap(),
        media_bytes
    );
    assert_eq!(
        fs::read(fixture.archive_root.join("clip.nfo")).unwrap(),
        nfo_bytes
    );

    runtime.signal_epoch_fatal();
    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);

    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reload after simulated restart"),
    );
    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, crate::cloud::TEST_DATABASE_EPOCH)
            .expect("same-epoch inbox should reload after simulated restart"),
    );
    let reopened_runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&reopened_queue),
        Arc::clone(&reopened_inbox),
    );
    let reopened_dispatcher = tokio::spawn(cloud_request_dispatcher(reopened_runtime.clone()));

    assert!(
        reopened_runtime.library_snapshots.lock().await.is_empty(),
        "a simulated cold start must begin without cached library scans"
    );
    let legacy_engine = crate::cloud_bot_tests::media_library_e2e_tests::migration_engine_for_test(
        &reopened_runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    legacy_engine
        .make_job_legacy_for_test(&first_migration_id)
        .expect("test should strip the optional target birth from the completed durable job");
    let legacy_status = legacy_engine
        .status(&first_migration_id)
        .expect("legacy status should load")
        .expect("legacy job should remain present");
    assert_eq!(legacy_status.items[0].target_physical_identity, None);
    drop(legacy_engine);

    let migration_lease = crate::library_migration_bot::acquire_library_io(
        &reopened_runtime,
        &[
            fixture.source_library_id.clone(),
            fixture.target_library_id.clone(),
        ],
    )
    .await
    .expect("test should hold both library gates exclusively");
    {
        let runner_waiting_notify =
            Arc::clone(&reopened_runtime.test_migration_runner_gate_waiting);
        let runner_waiting = runner_waiting_notify.notified();
        tokio::pin!(runner_waiting);
        runner_waiting.as_mut().enable();
        crate::library_migration_bot::discover_for_test(&reopened_runtime)
            .await
            .expect("discovery should schedule the durable completed-job runner");
        tokio::time::timeout(Duration::from_secs(10), &mut runner_waiting)
            .await
            .expect("completed-job runner should wait for the held exclusive lease");
    }

    let refresh_waiting_notify = Arc::clone(&reopened_runtime.test_management_admission_waiting);
    let refresh_waiting = refresh_waiting_notify.notified();
    tokio::pin!(refresh_waiting);
    refresh_waiting.as_mut().enable();
    let refresh_runtime = reopened_runtime.clone();
    let mut refresh_task =
        tokio::spawn(
            async move { crate::media_library_bot::refresh_libraries(&refresh_runtime).await },
        );
    tokio::time::timeout(Duration::from_secs(30), &mut refresh_waiting)
        .await
        .expect("cold-start refresh should reach the contended management admission gate");
    assert!(
        !refresh_task.is_finished(),
        "background refresh must wait until the migration lease is released"
    );
    assert!(
        reopened_runtime.library_snapshots.lock().await.is_empty(),
        "refresh must not publish a target scan while waiting for migration admission"
    );

    let interactive_scan = run_library_management_test_request(
        &reopened_runtime,
        9_111,
        "library_scan",
        serde_json::json!({ "library_id": fixture.target_library_id }),
    )
    .await;
    assert_eq!(interactive_scan.status, InboxStatus::Failed);
    assert!(
        interactive_scan
            .error
            .as_deref()
            .is_some_and(|error| error.contains("active migration")),
        "interactive scan should remain fail-fast while the exclusive lease is held: {:?}",
        interactive_scan.error
    );

    drop(migration_lease);
    wait_for_snapshot_migration_runner_idle(&reopened_runtime, &first_migration_id).await;
    tokio::time::timeout(Duration::from_secs(60), &mut refresh_task)
        .await
        .expect("cold-start background refresh should complete after migration releases admission")
        .expect("cold-start background refresh task should not panic")
        .expect("cold-start background refresh should scan the registered libraries");

    let first_refresh_snapshot = build_cloud_state_snapshot(&reopened_runtime)
        .await
        .expect("cloud snapshot should include the cold-start refresh")
        .expect("runtime should publish a cloud snapshot");
    let source_after_first_move =
        snapshot_library_items(&first_refresh_snapshot.library, &fixture.source_library_id);
    let first_source_history_item = snapshot_item(&source_after_first_move, &original_source_id);
    assert_eq!(first_source_history_item["presence"], "missing");
    assert!(
        first_source_history_item.get("moved_to").is_none(),
        "legacy history without a birth proof must stay unlinked until bounded verification"
    );
    let first_target_items =
        snapshot_library_items(&first_refresh_snapshot.library, &fixture.target_library_id);
    let first_target_item = first_target_items
        .iter()
        .find(|item| item["relative_path"] == "clip.mp4" && item["presence"] == "present")
        .expect("cold-start refresh should discover the migrated target item");
    let first_target_id = first_target_item["id"]
        .as_str()
        .expect("migrated target should have a stable item identity")
        .to_string();
    assert_ne!(first_target_id, original_source_id);
    let first_target_identity_before_rename = cached_item_physical_identity(
        &reopened_runtime,
        &fixture.target_library_id,
        &first_target_id,
    )
    .await;
    assert!(
        first_target_identity_before_rename.has_complete_birth(),
        "the background target scan should retain its in-memory creation identity"
    );
    let persisted_first_status = reopened_runtime
        .migrations
        .lock()
        .await
        .get(&first_migration_id)
        .cloned()
        .expect("discovery should hydrate the completed migration status");
    assert_eq!(
        persisted_first_status.items[0].target_item_id.as_deref(),
        Some(first_target_id.as_str()),
        "the durable migration should retain the verified target item identity"
    );
    assert_eq!(
        persisted_first_status.items[0].target_physical_identity,
        None
    );

    crate::library_migration_bot::discover_for_test(&reopened_runtime)
        .await
        .expect("a discovery pass after target refresh should retry legacy verification");
    wait_for_snapshot_migration_runner_idle(&reopened_runtime, &first_migration_id).await;
    let upgraded_first_status = reopened_runtime
        .migrations
        .lock()
        .await
        .get(&first_migration_id)
        .cloned()
        .expect("background verification should publish upgraded migration status");
    assert_eq!(
        upgraded_first_status.items[0].target_physical_identity,
        Some(first_target_identity_before_rename),
        "bounded verification should persist the exact target creation identity"
    );
    let upgraded_projection = build_cloud_state_snapshot(&reopened_runtime)
        .await
        .expect("upgraded snapshot should build")
        .expect("runtime should publish an upgraded snapshot");
    let upgraded_source_items =
        snapshot_library_items(&upgraded_projection.library, &fixture.source_library_id);
    let upgraded_source_item = snapshot_item(&upgraded_source_items, &original_source_id);
    assert_eq!(upgraded_source_item["presence"], "missing");
    assert_eq!(
        upgraded_source_item["moved_to"]["library_id"],
        fixture.target_library_id
    );
    assert_eq!(upgraded_source_item["moved_to"]["library_name"], "Archive");
    assert_eq!(upgraded_source_item["moved_to"]["item_id"], first_target_id);
    assert_eq!(
        upgraded_source_item["moved_to"]["relative_path"],
        "clip.mp4"
    );

    let renamed_relative_path = "renamed.mp4";
    fs::rename(
        fixture.archive_root.join("clip.mp4"),
        fixture.archive_root.join(renamed_relative_path),
    )
    .expect("migrated target media should rename within its library");
    fs::rename(
        fixture.archive_root.join("clip.nfo"),
        fixture.archive_root.join("renamed.nfo"),
    )
    .expect("migrated target sidecar should rename with its media");
    let renamed_target_id = scan_file_item_id(
        &reopened_runtime,
        9_113,
        &fixture.target_library_id,
        renamed_relative_path,
    )
    .await;
    assert_eq!(
        renamed_target_id, first_target_id,
        "renaming within one library should preserve the physical item identity"
    );
    let first_target_identity_after_rename = cached_item_physical_identity(
        &reopened_runtime,
        &fixture.target_library_id,
        &renamed_target_id,
    )
    .await;
    assert_eq!(
        first_target_identity_after_rename, first_target_identity_before_rename,
        "an in-library rename should preserve the item's creation identity"
    );
    let persisted_first_status = reopened_runtime
        .migrations
        .lock()
        .await
        .get(&first_migration_id)
        .cloned()
        .expect("discovery should hydrate the completed migration status");
    assert_eq!(
        persisted_first_status.items[0].target_item_id.as_deref(),
        Some(first_target_id.as_str()),
        "the durable migration should retain the verified target item identity"
    );
    assert_eq!(
        persisted_first_status.items[0].target_physical_identity,
        Some(first_target_identity_after_rename),
        "the reloaded job's physical target proof should match the renamed file"
    );
    let after_first_snapshot = build_cloud_state_snapshot(&reopened_runtime)
        .await
        .expect("cloud snapshot should include reloaded migration history")
        .expect("runtime should publish a cloud snapshot");
    let first_source_view =
        snapshot_library_items(&after_first_snapshot.library, &fixture.source_library_id);
    let source_history_item = snapshot_item(&first_source_view, &original_source_id);
    assert_eq!(source_history_item["presence"], "missing");
    assert_eq!(
        source_history_item["moved_to"]["library_id"],
        fixture.target_library_id
    );
    assert_eq!(source_history_item["moved_to"]["library_name"], "Archive");
    assert_eq!(source_history_item["moved_to"]["item_id"], first_target_id);
    assert_eq!(
        source_history_item["moved_to"]["relative_path"], renamed_relative_path,
        "moved history should use the current path after an in-library rename"
    );

    let legacy_engine = crate::cloud_bot_tests::media_library_e2e_tests::migration_engine_for_test(
        &reopened_runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    legacy_engine
        .make_job_legacy_for_test(&first_migration_id)
        .expect("test should strip the optional target birth from the completed durable job");
    let legacy_status = legacy_engine
        .status(&first_migration_id)
        .expect("legacy status should load")
        .expect("legacy job should remain present");
    assert_eq!(legacy_status.items[0].target_physical_identity, None);
    drop(legacy_engine);

    // Hold read admission so the renamed legacy target stays unverified before its bounded hash.
    let renamed_legacy_lease = crate::library_migration_bot::try_acquire_library_io(
        &reopened_runtime,
        &[
            fixture.source_library_id.clone(),
            fixture.target_library_id.clone(),
        ],
    )
    .await
    .expect("test should hold the libraries before the renamed legacy upgrade starts");
    {
        let runner_waiting_notify =
            Arc::clone(&reopened_runtime.test_migration_runner_gate_waiting);
        let runner_waiting = runner_waiting_notify.notified();
        tokio::pin!(runner_waiting);
        runner_waiting.as_mut().enable();
        crate::library_migration_bot::discover_for_test(&reopened_runtime)
            .await
            .expect("discovery should schedule renamed legacy completed-job verification");
        tokio::time::timeout(Duration::from_secs(10), &mut runner_waiting)
            .await
            .expect("legacy verifier should wait for the held library lease");
    }
    let legacy_projection = build_cloud_state_snapshot(&reopened_runtime)
        .await
        .expect("legacy snapshot should build before its hash upgrade")
        .expect("runtime should publish a legacy snapshot");
    let legacy_source_items =
        snapshot_library_items(&legacy_projection.library, &fixture.source_library_id);
    assert!(
        snapshot_item(&legacy_source_items, &original_source_id)
            .get("moved_to")
            .is_none(),
        "a durable target identity without a birth proof must not be trusted yet"
    );
    drop(renamed_legacy_lease);
    wait_for_snapshot_migration_runner_idle(&reopened_runtime, &first_migration_id).await;
    let upgraded_first_status = reopened_runtime
        .migrations
        .lock()
        .await
        .get(&first_migration_id)
        .cloned()
        .expect("background verification should publish upgraded migration status");
    assert_eq!(
        upgraded_first_status.items[0].target_physical_identity,
        Some(first_target_identity_after_rename),
        "bounded verification should persist the exact target creation identity"
    );
    let upgraded_projection = build_cloud_state_snapshot(&reopened_runtime)
        .await
        .expect("upgraded snapshot should build")
        .expect("runtime should publish the upgraded snapshot");
    let upgraded_source_items =
        snapshot_library_items(&upgraded_projection.library, &fixture.source_library_id);
    assert_eq!(
        snapshot_item(&upgraded_source_items, &original_source_id)["moved_to"]["relative_path"],
        renamed_relative_path,
        "verified legacy history should recover its link at the current target path"
    );

    let second_source_id = scan_file_item_id(
        &reopened_runtime,
        9_114,
        &fixture.target_library_id,
        renamed_relative_path,
    )
    .await;
    assert_eq!(second_source_id, first_target_id);
    let second_migration_id = start_selected_snapshot_migration(
        &reopened_runtime,
        9_115,
        &fixture.target_library_id,
        &final_library.id,
        &second_source_id,
    )
    .await;
    let second_status =
        wait_for_snapshot_migration_completion(&reopened_runtime, &second_migration_id).await;
    assert_eq!(second_status.success_count, 1);
    assert_eq!(
        second_status.items[0].source_physical_identity,
        Some(first_target_identity_after_rename),
        "the second migration should bind the renamed item's original physical identity"
    );
    wait_for_snapshot_migration_runner_idle(&reopened_runtime, &second_migration_id).await;
    assert!(!fixture.archive_root.join(renamed_relative_path).exists());
    assert!(!fixture.archive_root.join("renamed.nfo").exists());
    assert_eq!(
        fs::read(final_root.join(renamed_relative_path)).unwrap(),
        media_bytes
    );
    assert_eq!(fs::read(final_root.join("renamed.nfo")).unwrap(), nfo_bytes);

    reopened_runtime.signal_epoch_fatal();
    reopened_dispatcher.abort();
    let _ = reopened_dispatcher.await;
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);

    let final_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reload after the second move"),
    );
    let final_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, crate::cloud::TEST_DATABASE_EPOCH)
            .expect("cloud inbox should reopen after the second move"),
    );
    let final_runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&final_queue),
        Arc::clone(&final_inbox),
    );
    let final_dispatcher = tokio::spawn(cloud_request_dispatcher(final_runtime.clone()));
    crate::library_migration_bot::discover_for_test(&final_runtime)
        .await
        .expect("both durable migration jobs should reload after restart");
    wait_for_snapshot_migration_runner_idle(&final_runtime, &first_migration_id).await;
    wait_for_snapshot_migration_runner_idle(&final_runtime, &second_migration_id).await;
    let reloaded_first_status = final_runtime
        .migrations
        .lock()
        .await
        .get(&first_migration_id)
        .cloned()
        .expect("the upgraded first job should reload after the second restart");
    assert_eq!(
        reloaded_first_status.items[0].target_physical_identity,
        Some(first_target_identity_after_rename),
        "the legacy birth upgrade should persist across a later process restart"
    );

    let source_final_scan =
        rescan_snapshot_library(&final_runtime, 9_121, &fixture.source_library_id).await;
    let intermediate_final_scan =
        rescan_snapshot_library(&final_runtime, 9_122, &fixture.target_library_id).await;
    let final_target_scan = rescan_snapshot_library(&final_runtime, 9_123, &final_library.id).await;
    let final_target_id = final_target_scan["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| {
            item["relative_path"] == renamed_relative_path && item["presence"] == "present"
        })
        .expect("final target scan should identify the current migrated media")["id"]
        .as_str()
        .expect("final target should have its own item identity")
        .to_string();
    let final_target_identity =
        cached_item_physical_identity(&final_runtime, &final_library.id, &final_target_id).await;
    assert_ne!(final_target_id, first_target_id);
    assert_eq!(
        second_status.items[0].target_physical_identity,
        Some(final_target_identity),
        "the second durable migration should bind the present final target"
    );
    let final_history_snapshot = build_cloud_state_snapshot(&final_runtime)
        .await
        .expect("cloud snapshot should rebuild from reloaded history and fresh scans")
        .expect("runtime should publish a cloud snapshot");
    let source_items =
        snapshot_library_items(&final_history_snapshot.library, &fixture.source_library_id);
    let intermediate_items =
        snapshot_library_items(&final_history_snapshot.library, &fixture.target_library_id);
    let source_history_item = snapshot_item(&source_items, &original_source_id);
    let intermediate_history_item = snapshot_item(&intermediate_items, &first_target_id);
    assert_eq!(
        cached_item_physical_identity(
            &final_runtime,
            &fixture.target_library_id,
            &first_target_id,
        )
        .await,
        first_target_identity_after_rename,
        "a missing intermediate history entry should reload its indexed birth identity"
    );
    for history_item in [source_history_item, intermediate_history_item] {
        assert_eq!(history_item["presence"], "missing");
        assert_eq!(history_item["moved_to"]["library_id"], final_library.id);
        assert_eq!(history_item["moved_to"]["library_name"], "Final Archive");
        assert_eq!(history_item["moved_to"]["item_id"], final_target_id);
        assert_eq!(
            history_item["moved_to"]["relative_path"],
            renamed_relative_path
        );
    }
    let durable_history = final_history_snapshot.library["migrations"]
        .as_array()
        .expect("snapshot should expose durable migration history");
    let durable_second_job = durable_history
        .iter()
        .find(|job| job["id"] == second_migration_id)
        .expect("reloaded snapshot should include the second migration job");
    assert_eq!(
        durable_second_job["items"][0]["target_item_id"],
        final_target_id
    );
    assert!(
        durable_second_job["items"][0]
            .get("target_physical_identity")
            .is_none(),
        "internal physical proof must never appear in serialized migration history"
    );
    let durable_first_job = durable_history
        .iter()
        .find(|job| job["id"] == first_migration_id)
        .expect("reloaded snapshot should include the upgraded first migration job");
    assert_eq!(
        durable_first_job["items"][0]["target_item_id"],
        first_target_id
    );
    assert!(
        durable_first_job["items"][0]
            .get("target_physical_identity")
            .is_none(),
        "legacy birth evidence should remain internal after restart"
    );
    assert_eq!(source_final_scan["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        intermediate_final_scan["items"].as_array().unwrap().len(),
        1
    );

    // Keep the deterministic item ID, path, device, and inode fixed while changing only the
    // captured creation time. This models inode reuse without relying on the filesystem to reuse
    // an inode during the test.
    let replacement_final_identity = same_object_with_different_birth(final_target_identity);
    assert_eq!(
        replacement_final_identity.device,
        final_target_identity.device
    );
    assert_eq!(
        replacement_final_identity.inode,
        final_target_identity.inode
    );
    assert_ne!(
        replacement_final_identity.birth_seconds,
        final_target_identity.birth_seconds
    );
    inject_cached_item_physical_identity(
        &final_runtime,
        &final_library.id,
        &final_target_id,
        replacement_final_identity,
    )
    .await;
    let changed_birth_target_snapshot = build_cloud_state_snapshot(&final_runtime)
        .await
        .expect("snapshot should rebuild after deterministic creation-time replacement")
        .expect("runtime should publish after deterministic creation-time replacement");
    for (library_id, history_id) in [
        (&fixture.source_library_id, &original_source_id),
        (&fixture.target_library_id, &first_target_id),
    ] {
        let items = snapshot_library_items(&changed_birth_target_snapshot.library, library_id);
        assert!(
            snapshot_item(&items, history_id).get("moved_to").is_none(),
            "same ID/path/device/inode with a different creation time must break the moved-to chain"
        );
    }
    inject_cached_item_physical_identity(
        &final_runtime,
        &final_library.id,
        &final_target_id,
        final_target_identity,
    )
    .await;

    // Simulate B being replaced with the same stable ID/path/device/inode but a different birth
    // before B→C was recorded. The injected B→C source proof matches that replacement, so B can
    // still point to C; A→B's old target proof must not chain through it.
    let original_second_source_identity = second_status.items[0]
        .source_physical_identity
        .expect("second migration should retain B's physical source proof");
    let replaced_intermediate_identity =
        same_object_with_different_birth(original_second_source_identity);
    assert_eq!(
        replaced_intermediate_identity.device,
        original_second_source_identity.device
    );
    assert_eq!(
        replaced_intermediate_identity.inode,
        original_second_source_identity.inode
    );
    assert_ne!(
        replaced_intermediate_identity.birth_seconds,
        original_second_source_identity.birth_seconds
    );
    inject_cached_item_physical_identity(
        &final_runtime,
        &fixture.target_library_id,
        &first_target_id,
        replaced_intermediate_identity,
    )
    .await;
    {
        let mut migrations = final_runtime.migrations.lock().await;
        let status = migrations
            .get_mut(&second_migration_id)
            .expect("reloaded second migration should remain published");
        status.items[0].source_physical_identity = Some(replaced_intermediate_identity);
    }
    let replaced_intermediate_snapshot = build_cloud_state_snapshot(&final_runtime)
        .await
        .expect("snapshot should rebuild after deterministic intermediate replacement")
        .expect("runtime should publish after deterministic intermediate replacement");
    let replaced_intermediate_items = snapshot_library_items(
        &replaced_intermediate_snapshot.library,
        &fixture.target_library_id,
    );
    assert_eq!(
        snapshot_item(&replaced_intermediate_items, &first_target_id)["moved_to"]["item_id"],
        final_target_id,
        "the B→C edge should remain valid for the injected replacement identity"
    );
    let replaced_source_items = snapshot_library_items(
        &replaced_intermediate_snapshot.library,
        &fixture.source_library_id,
    );
    assert!(
        snapshot_item(&replaced_source_items, &original_source_id)
            .get("moved_to")
            .is_none(),
        "A→B must not chain through B's replacement creation identity"
    );
    inject_cached_item_physical_identity(
        &final_runtime,
        &fixture.target_library_id,
        &first_target_id,
        original_second_source_identity,
    )
    .await;
    {
        let mut migrations = final_runtime.migrations.lock().await;
        let status = migrations
            .get_mut(&second_migration_id)
            .expect("reloaded second migration should remain published");
        status.items[0].source_physical_identity = Some(original_second_source_identity);
    }

    // A missing target and a replacement at the same path must not keep an old link alive.
    fs::remove_file(final_root.join(renamed_relative_path))
        .expect("final target media should be removable");
    let missing_target_scan =
        rescan_snapshot_library(&final_runtime, 9_124, &final_library.id).await;
    let missing_target_id = missing_target_scan["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["relative_path"] == renamed_relative_path)
        .expect("target scan should retain the missing media history")["id"]
        .as_str()
        .expect("missing target history should retain its item identity")
        .to_string();
    assert_eq!(missing_target_id, final_target_id);
    let missing_target_snapshot = build_cloud_state_snapshot(&final_runtime)
        .await
        .expect("snapshot should rebuild after target removal")
        .expect("runtime should publish after target removal");
    for library_id in [&fixture.source_library_id, &fixture.target_library_id] {
        let items = snapshot_library_items(&missing_target_snapshot.library, library_id);
        assert!(
            snapshot_item(
                &items,
                if library_id == &fixture.source_library_id {
                    &original_source_id
                } else {
                    &first_target_id
                }
            )
            .get("moved_to")
            .is_none(),
            "missing final target must invalidate every earlier moved-to link"
        );
    }

    fs::write(
        final_root.join(renamed_relative_path),
        b"replacement media at the same path",
    )
    .expect("replacement target media should write");
    let replaced_target_scan =
        rescan_snapshot_library(&final_runtime, 9_125, &final_library.id).await;
    let replacement_target_id = replaced_target_scan["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| {
            item["relative_path"] == renamed_relative_path && item["presence"] == "present"
        })
        .expect("replacement target should scan as present")["id"]
        .as_str()
        .expect("replacement should receive a physical-copy identity")
        .to_string();
    assert_ne!(replacement_target_id, final_target_id);
    let replaced_target_snapshot = build_cloud_state_snapshot(&final_runtime)
        .await
        .expect("snapshot should rebuild after target replacement")
        .expect("runtime should publish after target replacement");
    for library_id in [&fixture.source_library_id, &fixture.target_library_id] {
        let items = snapshot_library_items(&replaced_target_snapshot.library, library_id);
        let historical_id = if library_id == &fixture.source_library_id {
            &original_source_id
        } else {
            &first_target_id
        };
        assert!(
            snapshot_item(&items, historical_id)
                .get("moved_to")
                .is_none(),
            "a different physical target at the same path must not retain a moved-to link"
        );
    }

    final_runtime.library_unavailable.lock().await.insert(
        final_library.id.clone(),
        "synthetic target scan unavailable".to_string(),
    );
    let unavailable_target_snapshot = build_cloud_state_snapshot(&final_runtime)
        .await
        .expect("snapshot should retain cached data while marking the target unavailable")
        .expect("runtime should publish with an unavailable target");
    assert_eq!(
        snapshot_library_view(&unavailable_target_snapshot.library, &final_library.id)["availability"],
        "unavailable"
    );
    for library_id in [&fixture.source_library_id, &fixture.target_library_id] {
        let items = snapshot_library_items(&unavailable_target_snapshot.library, library_id);
        let historical_id = if library_id == &fixture.source_library_id {
            &original_source_id
        } else {
            &first_target_id
        };
        assert!(
            snapshot_item(&items, historical_id)
                .get("moved_to")
                .is_none(),
            "an unavailable target must invalidate every earlier moved-to link"
        );
    }
    final_runtime
        .library_unavailable
        .lock()
        .await
        .remove(&final_library.id);

    fs::write(fixture.video_root.join("clip.mp4"), media_bytes)
        .expect("reappearing source media should write");
    let reappeared_scan =
        rescan_snapshot_library(&final_runtime, 9_126, &fixture.source_library_id).await;
    let reappeared_item = reappeared_scan["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["relative_path"] == "clip.mp4" && item["presence"] == "present")
        .expect("reappeared source should scan as present");
    assert!(
        reappeared_item.get("moved_to").is_none(),
        "a source path that reappears must not be reported as a missing moved item"
    );
    let reappeared_snapshot = build_cloud_state_snapshot(&final_runtime)
        .await
        .expect("snapshot should rebuild after source reappearance")
        .expect("runtime should publish after source reappearance");
    let source_items =
        snapshot_library_items(&reappeared_snapshot.library, &fixture.source_library_id);
    let source_path_item = source_items
        .iter()
        .find(|item| item["relative_path"] == "clip.mp4" && item["presence"] == "present")
        .expect("source snapshot should show the reappeared media");
    assert!(source_path_item.get("moved_to").is_none());

    final_runtime.signal_epoch_fatal();
    final_dispatcher.abort();
    let _ = final_dispatcher.await;
    drop(final_runtime);
    drop(final_inbox);
    drop(final_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[test]
fn cloud_snapshot_reduction_converges_across_maximum_migration_history() {
    let migrations = (0..32)
        .map(|migration_index| {
            let items = (0..2048)
                .map(|item_index| {
                    serde_json::json!({
                        "item_id": format!("bundle-{migration_index}-{item_index}"),
                        "status": "copying",
                        "source_path": format!("source/{}-{}.mp4", "s".repeat(24), item_index),
                        "target_path": format!("target/{}-{}.mp4", "t".repeat(24), item_index),
                        "bytes": 4096,
                        "files": [],
                        "error": null,
                    })
                })
                .collect::<Vec<_>>();
            let task_transfers = (0..1024)
                .map(|transfer_index| {
                    serde_json::json!({
                        "task_id": format!("task-{migration_index}-{transfer_index}"),
                        "transfer_id": format!("transfer-{migration_index}-{transfer_index}"),
                        "completed_files": 0,
                        "total_files": 1,
                        "copy_complete": false,
                        "commit_acked": false,
                        "cleaned_source_files": 0,
                        "cleanup_total_files": 1,
                        "source_cleanup_complete": false,
                        "error": null,
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "id": format!("bounded-history-{migration_index:02}"),
                "status": "running",
                "source_library_id": "source",
                "target_library_id": "target",
                "items": items,
                "task_transfers": task_transfers,
            })
        })
        .collect::<Vec<_>>();
    let mut snapshot = CloudStateSnapshot {
        revision: "bounded-migration-history".to_string(),
        library: serde_json::json!({
            "revision": "bounded-migration-history",
            "scanned_at": 1_790_000_000_u64,
            "root_label": "source",
            "items": [],
            "categories": [],
            "warnings": [],
            "migrations": migrations,
        }),
        previews: Vec::new(),
        operations: Vec::new(),
        tasks: Vec::new(),
        download_dir: "/tmp/source".to_string(),
        settings_applied: serde_json::json!({
            "revision": "settings-revision",
            "default_library_id": "source",
            "default_category": "",
            "libraries": [],
        }),
    };
    let initial_size = snapshot
        .encoded_size(1)
        .expect("large snapshot should serialize");
    let budget = 18 * 1024;
    assert!(initial_size > budget);
    assert!(
        snapshot
            .reduce_after_payload_too_large(1, budget)
            .expect("bounded history should reduce")
    );

    let final_size = snapshot
        .encoded_size(1)
        .expect("reduced history should serialize");
    assert!(
        final_size <= budget,
        "reducer should reach the upload budget across all migration arrays: {final_size} > {budget}"
    );
    let retained = snapshot.library["migrations"].as_array().unwrap();
    assert_eq!(retained.len(), 32);
    for migration in retained {
        let items = migration["items"].as_array().unwrap();
        let transfers = migration["task_transfers"].as_array().unwrap();
        assert!(items.len() <= 1, "large media details should be bounded");
        assert_eq!(
            migration["omitted_item_details"].as_u64(),
            Some(2048 - items.len() as u64)
        );
        assert_eq!(migration["task_transfer_count"], 1024);
        assert_eq!(
            migration["omitted_task_transfer_count"].as_u64(),
            Some(1024 - transfers.len() as u64)
        );
    }
}

fn archive_destination_for_test(
    queue: &QueueManager,
    library_id: &str,
) -> crate::media_libraries::TaskDestination {
    queue
        .video_destination(library_id, None)
        .expect("archive destination should resolve")
}

struct FailOnceOnPathProvider {
    path: PathBuf,
    armed: AtomicBool,
    failures: AtomicUsize,
    root_reads: AtomicUsize,
}

impl FailOnceOnPathProvider {
    fn new(path: PathBuf) -> Self {
        Self {
            path: fs::canonicalize(&path).unwrap_or(path),
            armed: AtomicBool::new(false),
            failures: AtomicUsize::new(0),
            root_reads: AtomicUsize::new(0),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl QueueFileProvider for FailOnceOnPathProvider {
    fn coordinate_read(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        let normalized_path = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        if normalized_path == self.path {
            self.root_reads.fetch_add(1, Ordering::SeqCst);
            if self.armed.swap(false, Ordering::SeqCst) {
                self.failures.fetch_add(1, Ordering::SeqCst);
                return Err(FileProviderAccessError::new(
                    path,
                    "read",
                    "synthetic media-library root open failure",
                )
                .into());
            }
        }
        accessor(path)
    }

    fn coordinate_write(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        accessor(path)
    }

    fn coordinate_move(
        &self,
        source: &Path,
        destination: &Path,
        accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
    ) -> Result<()> {
        accessor(source, destination)
    }
}

#[tokio::test]
async fn failed_non_default_library_open_recovers_from_binding_refresh_without_scanning() {
    let root = temp_cloud_bot_dir("library-snapshot-scoped-open-recovery");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.telegram.allowed_chat_ids = vec![SNAPSHOT_TEST_CHAT_ID];
    config
        .ensure_runtime_dirs()
        .expect("snapshot fixture roots should create");
    let archive_root = root.join("archive");
    let offline_root = root.join("offline");
    fs::create_dir_all(&archive_root).expect("archive root should create");
    fs::create_dir_all(&offline_root).expect("offline root should create");
    let child_media = archive_root.join("clip.mp4");
    fs::write(&child_media, b"synthetic media bytes").expect("child media should write");
    fs::write(
        archive_root.join("clip.nfo"),
        b"<movie><title>Cached archive item</title></movie>",
    )
    .expect("child metadata should write");
    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let default_id = queue
        .default_video_destination()
        .expect("default video library should resolve")
        .library_id;
    let archive = queue
        .register_video_library("Archive", &archive_root, None, false, false)
        .expect("archive library should register");
    let offline = queue
        .register_video_library("Offline", &offline_root, None, false, false)
        .expect("offline library should register");
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            "test-token".to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );

    let default_failure =
        run_cloud_library_operation::<()>(&runtime, "synthetic default scan failed", |_, _| {
            Err(anyhow::anyhow!("synthetic default scan failure"))
        })
        .await;
    assert!(default_failure.is_err());
    let default_warning = runtime
        .library_scan_warning
        .lock()
        .await
        .clone()
        .expect("default scan failure should set the global warning");

    let provider = Arc::new(FailOnceOnPathProvider::new(archive_root.clone()));
    let (archive_runtime, scoped_id) =
        media_library_bot::scoped_runtime(&runtime, Some(&archive.id))
            .expect("archive runtime should scope");
    assert_eq!(scoped_id, archive.id);
    let scan = |provider: Arc<FailOnceOnPathProvider>| {
        let runtime = archive_runtime.clone();
        async move {
            run_cloud_library_operation(
                &runtime,
                "archive library open failed",
                move |context, inbox| {
                    let manager =
                        open_cloud_library_manager(&context, inbox, Some(provider.clone()))?;
                    let snapshot = manager.scan(&[])?;
                    Ok((snapshot.clone(), snapshot))
                },
            )
            .await
        }
    };

    let first = scan(Arc::clone(&provider))
        .await
        .expect("non-default library scan should succeed");
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].presence, Presence::Present);
    let cached = runtime
        .library_snapshots
        .lock()
        .await
        .get(&archive.id)
        .cloned()
        .expect("successful archive snapshot should be cached");
    assert_eq!(cached.items[0].relative_path, "clip.mp4");
    assert_eq!(
        runtime.library_scan_warning.lock().await.as_deref(),
        Some(default_warning.as_str()),
        "a non-default library success must preserve the default warning"
    );

    provider.arm();
    let failed = scan(Arc::clone(&provider)).await;
    assert!(
        failed.is_err(),
        "the injected library-root read denial should fail manager open"
    );
    assert_eq!(
        provider.failures.load(Ordering::SeqCst),
        1,
        "the provider should inject exactly one library-root denial"
    );
    assert!(
        runtime
            .library_unavailable
            .lock()
            .await
            .contains_key(&archive.id),
        "the selected non-default library should be marked unavailable"
    );
    assert_eq!(
        runtime.library_snapshots.lock().await.get(&archive.id),
        Some(&cached),
        "the failed scan should retain the last successful cached snapshot"
    );
    assert_eq!(
        runtime.library_scan_warning.lock().await.as_deref(),
        Some(default_warning.as_str()),
        "a non-default failure must not replace the default library warning"
    );

    let applied =
        media_library_bot::applied_settings(&queue).expect("library settings should load");
    let projected = media_library_bot::attach_library_views(
        &runtime,
        &applied,
        serde_json::to_value(empty_cloud_test_library_snapshot("legacy"))
            .expect("legacy snapshot should encode"),
    )
    .await
    .expect("scoped library views should build after scan failure");
    let archive_view = projected["libraries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|view| view["id"] == archive.id)
        .unwrap();
    assert_eq!(archive_view["availability"], "unavailable");
    assert_eq!(archive_view["snapshot"]["revision"], cached.revision);
    assert_eq!(
        archive_view["snapshot"]["items"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        archive_view["snapshot"]["items"][0]["presence"],
        "unavailable"
    );

    fs::remove_dir_all(&offline_root).expect("offline root should become unavailable");
    queue
        .refresh_video_library(&offline.id)
        .expect("offline root should update its availability status");
    assert!(
        !queue
            .video_library_statuses()
            .expect("queue availability should load")
            .get(&offline.id)
            .expect("offline library should retain a status")
            .available
    );
    runtime.library_unavailable.lock().await.insert(
        offline.id.clone(),
        "offline library remains unavailable".to_string(),
    );

    let provider_root_reads_before_binding_refresh = provider.root_reads.load(Ordering::SeqCst);
    media_library_bot::refresh_library_binding(&runtime, &archive.id)
        .await
        .expect("selected binding refresh should recover the available archive");
    assert_eq!(
        provider.failures.load(Ordering::SeqCst),
        1,
        "the binding refresh should not repeat the injected denial"
    );
    assert_eq!(
        provider.root_reads.load(Ordering::SeqCst),
        provider_root_reads_before_binding_refresh,
        "binding-only recovery must not reopen the scan provider or run a full scan"
    );
    assert_eq!(
        runtime.library_snapshots.lock().await.get(&archive.id),
        Some(&cached),
        "binding-only recovery must preserve the last-good snapshot without rewriting it"
    );
    assert!(
        !runtime
            .library_unavailable
            .lock()
            .await
            .contains_key(&archive.id),
        "a successful scoped recovery should clear only the archive warning"
    );
    assert!(
        runtime
            .library_unavailable
            .lock()
            .await
            .contains_key(&default_id),
        "selected archive recovery must preserve the unselected default warning"
    );
    assert_eq!(
        runtime.library_scan_warning.lock().await.as_deref(),
        Some(default_warning.as_str())
    );
    assert_eq!(
        runtime
            .library_unavailable
            .lock()
            .await
            .get(&offline.id)
            .map(String::as_str),
        Some("offline library remains unavailable"),
        "selected refresh must preserve warnings for unselected unavailable roots"
    );

    let recovered_projection = media_library_bot::attach_library_views(
        &runtime,
        &applied,
        serde_json::to_value(empty_cloud_test_library_snapshot("legacy"))
            .expect("legacy snapshot should encode"),
    )
    .await
    .expect("scoped library views should publish the binding recovery");
    let recovered_archive_view = snapshot_library_view(&recovered_projection, &archive.id);
    assert_eq!(recovered_archive_view["availability"], "available");
    assert_eq!(
        recovered_archive_view["snapshot"]["revision"],
        cached.revision
    );
    assert_eq!(
        recovered_archive_view["snapshot"]["items"][0]["presence"], "present",
        "the cached item should become accessible after availability recovers"
    );

    let provider_root_reads_before_all_refresh = provider.root_reads.load(Ordering::SeqCst);
    media_library_bot::refresh_library_bindings(&runtime)
        .await
        .expect("all-library binding refresh should complete");
    assert_eq!(
        provider.root_reads.load(Ordering::SeqCst),
        provider_root_reads_before_all_refresh,
        "all-library binding refresh must not invoke the full-scan provider"
    );
    assert_eq!(
        runtime.library_snapshots.lock().await.get(&archive.id),
        Some(&cached),
        "all-library binding refresh must preserve the last-good snapshot"
    );
    assert!(
        !runtime
            .library_unavailable
            .lock()
            .await
            .contains_key(&default_id),
        "all-library refresh should clear a verified available default warning"
    );
    assert_eq!(runtime.library_scan_warning.lock().await.as_deref(), None);
    assert_eq!(
        runtime
            .library_unavailable
            .lock()
            .await
            .get(&offline.id)
            .map(String::as_str),
        Some("offline library remains unavailable"),
        "all-library refresh must preserve warnings for roots still unavailable"
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn media_library_remove_clears_reused_id_cache_after_scan_publication() {
    let root = temp_cloud_bot_dir("media-library-remove-reused-id-cache");
    let video_root = root.join("videos");
    let archive_root = root.join("archive");
    let retired_root = root.join("archive-retired");
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = video_root.clone();
    config.downloads.pdf_dir = root.join("pdfs");
    config.telegram.allowed_chat_ids = vec![SNAPSHOT_TEST_CHAT_ID];
    config
        .ensure_runtime_dirs()
        .expect("test library roots should create");
    fs::write(video_root.join("source.mp4"), b"source media").expect("source media should write");
    fs::create_dir(&archive_root).expect("archive library should create");
    fs::write(archive_root.join("old.mp4"), b"old archive media")
        .expect("old archive media should write");
    fs::write(
        archive_root.join("old.nfo"),
        b"<movie><title>Old archive metadata</title></movie>",
    )
    .expect("old archive metadata should write");

    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let source_id = queue
        .default_video_destination()
        .expect("source library should resolve")
        .library_id;
    let archive = queue
        .register_video_library("Archive", &archive_root, None, false, false)
        .expect("archive library should register");
    let initial_settings = media_library_bot::applied_settings(&queue)
        .expect("initial library bindings should serialize");
    let binding_for = |settings: &Value, library_id: &str| {
        settings["libraries"]
            .as_array()
            .expect("settings should include library descriptors")
            .iter()
            .find(|view| view["id"] == library_id)
            .and_then(|view| view["root_binding"].as_str())
            .expect("registered root should expose its binding")
            .to_owned()
    };
    let source_binding = binding_for(&initial_settings, &source_id);
    let archive_binding = binding_for(&initial_settings, &archive.id);
    let inbox = Arc::new(open_runtime_test_inbox(&video_root).expect("cloud inbox should open"));
    let runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));

    let source_scan = run_library_management_test_request(
        &runtime,
        8_101,
        "library_scan",
        serde_json::json!({"library_id": source_id}),
    )
    .await;
    assert_eq!(source_scan.status, InboxStatus::Completed);
    let source_snapshot = runtime
        .library_snapshots
        .lock()
        .await
        .get(&source_id)
        .cloned()
        .expect("the default source cache should be available for fallback");

    let set_archive_default = run_library_management_test_request(
        &runtime,
        8_102,
        "library_set_default",
        serde_json::json!({"library_id": archive.id, "root_binding": archive_binding}),
    )
    .await;
    assert_eq!(set_archive_default.status, InboxStatus::Completed);
    let old_scan = run_library_management_test_request(
        &runtime,
        8_103,
        "library_scan",
        serde_json::json!({"library_id": archive.id}),
    )
    .await;
    assert_eq!(
        old_scan.status,
        InboxStatus::Completed,
        "{:?}",
        old_scan.error
    );
    let old_cached = runtime
        .library_snapshots
        .lock()
        .await
        .get(&archive.id)
        .cloned()
        .expect("the old archive scan should populate its cache");
    assert_eq!(old_cached.items.len(), 1);
    assert_eq!(old_cached.items[0].relative_path, "old.mp4");

    let old_applied = media_library_bot::applied_settings(&queue)
        .expect("applied library settings should serialize");
    let old_binding = old_applied["libraries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|view| view["id"] == archive.id)
        .and_then(|view| view["root_binding"].as_str())
        .expect("bound roots should publish an opaque identity token")
        .to_string();
    assert_eq!(old_binding, archive_binding);
    assert_eq!(old_binding.len(), 67);
    assert!(old_binding.starts_with("v1:"));
    assert!(
        old_binding[3..]
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    );
    fs::write(
        archive_root.join("ordinary-child.txt"),
        b"ordinary child churn",
    )
    .expect("same-root child churn should remain permitted");
    let after_child_churn = media_library_bot::applied_settings(&queue)
        .expect("settings should remain available after child churn");
    assert_eq!(
        after_child_churn["libraries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|view| view["id"] == archive.id)
            .unwrap()["root_binding"],
        old_binding,
        "ordinary child-entry changes must not change the root object token"
    );
    let attached = media_library_bot::attach_library_views(
        &runtime,
        &old_applied,
        serde_json::to_value(empty_cloud_test_library_snapshot("legacy"))
            .expect("legacy snapshot should serialize"),
    )
    .await
    .expect("library views should attach");
    let old_view = attached["libraries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|view| view["id"] == archive.id)
        .unwrap();
    assert_eq!(old_view["root_binding"], old_binding);

    let (archive_runtime, _) = media_library_bot::scoped_runtime(&runtime, Some(&archive.id))
        .expect("archive runtime should scope");
    let failed_scan = run_cloud_library_operation::<()>(
        &archive_runtime,
        "synthetic archive scan failure",
        |_, _| Err(anyhow::anyhow!("synthetic archive scan failure")),
    )
    .await;
    assert!(failed_scan.is_err());
    let archive_warning = runtime
        .library_unavailable
        .lock()
        .await
        .get(&archive.id)
        .cloned()
        .expect("failed scan should mark the archive unavailable");
    let global_before_failed_remove = runtime.library_snapshot.lock().await.clone();
    let warning_before_failed_remove = runtime.library_scan_warning.lock().await.clone();
    assert_eq!(
        warning_before_failed_remove.as_deref(),
        Some(archive_warning.as_str()),
        "a failed default scan should populate the legacy warning before refresh"
    );

    let failed_remove = run_library_management_test_request(
        &runtime,
        8_104,
        "library_remove",
        serde_json::json!({"library_id": archive.id, "root_binding": old_binding}),
    )
    .await;
    assert_eq!(failed_remove.status, InboxStatus::Failed);
    assert!(
        runtime
            .library_snapshots
            .lock()
            .await
            .get(&archive.id)
            .is_some_and(|snapshot| snapshot == &old_cached),
        "a failed removal must preserve the cached library snapshot"
    );
    assert!(
        !runtime
            .library_unavailable
            .lock()
            .await
            .contains_key(&archive.id),
        "the pre-dispatch binding refresh should clear a stale marker for the available root"
    );
    assert_eq!(
        runtime.library_snapshot.lock().await.as_ref(),
        global_before_failed_remove.as_ref(),
        "a failed removal must not alter the legacy snapshot"
    );
    assert_eq!(
        runtime.library_scan_warning.lock().await.as_ref(),
        None,
        "the pre-dispatch binding refresh should clear the stale default warning"
    );

    let restore_source_default = run_library_management_test_request(
        &runtime,
        8_105,
        "library_set_default",
        serde_json::json!({"library_id": source_id, "root_binding": source_binding}),
    )
    .await;
    assert_eq!(restore_source_default.status, InboxStatus::Completed);

    // Hold a real scan after it has opened and read the old root but before it publishes its
    // snapshot. The remove dispatch is polled until it blocks on the same gate, so this ordering
    // remains deterministic under a slow CI worker.
    let (scan_read_tx, scan_read_rx) = oneshot::channel();
    let (release_scan_tx, release_scan_rx) = std::sync::mpsc::channel();
    let scan_runtime = archive_runtime.clone();
    let scan_task = tokio::spawn(async move {
        run_cloud_library_operation(
            &scan_runtime,
            "archive scan publication fixture failed",
            move |context, inbox| {
                let manager = open_cloud_library_manager(&context, inbox, None)?;
                let snapshot = manager.scan(&[])?;
                drop(manager);
                scan_read_tx
                    .send(())
                    .map_err(|_| anyhow::anyhow!("scan synchronization receiver closed"))?;
                release_scan_rx
                    .recv()
                    .map_err(|_| anyhow::anyhow!("scan release sender closed"))?;
                Ok((snapshot.clone(), snapshot))
            },
        )
        .await
    });
    scan_read_rx
        .await
        .expect("scan should reach the pre-publication checkpoint");
    assert!(runtime.library_scan_publication_gate.try_lock().is_err());

    let request = CloudRequest {
        seq: 8_106,
        kind: "library_remove".to_string(),
        payload: serde_json::json!({"library_id": archive.id, "root_binding": old_binding}),
        created_at: now_millis(),
        extra: serde_json::Map::new(),
    };
    let remove_runtime = runtime.clone();
    let (remove_pending_tx, remove_pending_rx) = oneshot::channel();
    let remove_task = tokio::spawn(async move {
        let mut dispatch_future = Box::pin(media_library_bot::dispatch(&remove_runtime, &request));
        let mut pending_sender = Some(remove_pending_tx);
        futures_util::future::poll_fn(move |context| {
            let polled = dispatch_future.as_mut().poll(context);
            if let Some(sender) = pending_sender.take() {
                let _ = sender.send(polled.is_pending());
            }
            polled
        })
        .await
    });
    assert!(
        remove_pending_rx
            .await
            .expect("remove should report its first poll state"),
        "removal must wait behind the scan publication gate"
    );
    assert!(
        queue
            .registry_snapshot()
            .expect("registry should remain readable while removal waits")
            .libraries
            .iter()
            .any(|library| library.id == archive.id),
        "the scan must retain the old registration until its snapshot is published"
    );
    release_scan_tx
        .send(())
        .expect("scan should be released for publication");
    scan_task
        .await
        .expect("scan worker should join")
        .expect("scan should publish successfully");
    let remove_outcome = remove_task
        .await
        .expect("remove dispatch should join")
        .expect("remove request should be handled")
        .expect("remove should complete after the scan releases the gate");
    assert_eq!(remove_outcome.status, InboxStatus::Completed);
    assert!(
        !runtime
            .library_snapshots
            .lock()
            .await
            .contains_key(&archive.id),
        "successful removal must clear the old per-library snapshot"
    );
    assert!(
        !runtime
            .library_unavailable
            .lock()
            .await
            .contains_key(&archive.id),
        "successful removal must clear the old per-library warning"
    );
    assert_eq!(
        runtime.library_snapshot.lock().await.as_ref(),
        Some(&source_snapshot),
        "removing a secondary library should mirror the still-registered default snapshot"
    );
    assert!(runtime.library_scan_warning.lock().await.is_none());

    let old_media = fs::read(archive_root.join("old.mp4")).unwrap();
    let old_metadata = fs::read(archive_root.join("old.nfo")).unwrap();
    fs::rename(&archive_root, &retired_root).expect("old root object should be retained");
    fs::create_dir(&archive_root).expect("replacement should use the same path");
    fs::write(archive_root.join("new.mp4"), b"replacement archive media")
        .expect("replacement media should write");
    fs::write(
        archive_root.join("new.nfo"),
        b"<movie><title>Replacement archive metadata</title></movie>",
    )
    .expect("replacement metadata should write");

    let registered = run_library_management_test_request(
        &runtime,
        8_107,
        "library_register",
        serde_json::json!({
            "name":"Archive", "path":archive_root,
            "create_directory":false, "set_default":false,
        }),
    )
    .await;
    assert_eq!(
        registered.status,
        InboxStatus::Completed,
        "{:?}",
        registered.error
    );
    let readded_settings =
        media_library_bot::applied_settings(&queue).expect("replacement settings should serialize");
    let new_binding = readded_settings["libraries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|view| view["id"] == archive.id)
        .and_then(|view| view["root_binding"].as_str())
        .expect("replacement root should have a binding token");
    assert_ne!(new_binding, old_binding);
    assert!(
        !runtime
            .library_snapshots
            .lock()
            .await
            .contains_key(&archive.id)
    );
    assert!(
        !runtime
            .library_unavailable
            .lock()
            .await
            .contains_key(&archive.id)
    );

    let set_readded_default = run_library_management_test_request(
        &runtime,
        8_108,
        "library_set_default",
        serde_json::json!({"library_id": archive.id, "root_binding": new_binding}),
    )
    .await;
    assert_eq!(set_readded_default.status, InboxStatus::Completed);
    let readded_default_settings = media_library_bot::applied_settings(&queue)
        .expect("replacement default settings should serialize");
    let projected_without_scan = media_library_bot::attach_library_views(
        &runtime,
        &readded_default_settings,
        serde_json::to_value(empty_cloud_test_library_snapshot("legacy"))
            .expect("legacy snapshot should serialize"),
    )
    .await
    .expect("replacement library view should build before scanning");
    let replacement_view = projected_without_scan["libraries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|view| view["id"] == archive.id)
        .unwrap();
    assert_eq!(replacement_view["root_binding"], new_binding);
    assert_eq!(replacement_view["availability"], "unknown");
    assert_eq!(replacement_view["media_count"], Value::Null);
    assert!(
        replacement_view.get("snapshot").is_none(),
        "the default snapshot should be present only at the top level"
    );
    assert_eq!(projected_without_scan["default_library_id"], archive.id);
    assert!(
        projected_without_scan["items"]
            .as_array()
            .unwrap()
            .is_empty(),
        "the top-level selected view must not expose the old default snapshot"
    );
    assert_eq!(projected_without_scan["ready"], false);
    assert_ne!(
        projected_without_scan["items"], old_view["snapshot"]["items"],
        "reusing the same path-derived ID must not leak the old root's media list"
    );

    let new_scan = run_library_management_test_request(
        &runtime,
        8_109,
        "library_scan",
        serde_json::json!({"library_id": archive.id}),
    )
    .await;
    assert_eq!(
        new_scan.status,
        InboxStatus::Completed,
        "{:?}",
        new_scan.error
    );
    let new_items = new_scan.result.unwrap()["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["relative_path"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(new_items, vec!["new.mp4"]);
    assert_eq!(fs::read(retired_root.join("old.mp4")).unwrap(), old_media);
    assert_eq!(
        fs::read(retired_root.join("old.nfo")).unwrap(),
        old_metadata
    );
    assert_eq!(
        fs::read(archive_root.join("new.mp4")).unwrap(),
        b"replacement archive media"
    );
    assert_eq!(
        fs::read(archive_root.join("new.nfo")).unwrap(),
        b"<movie><title>Replacement archive metadata</title></movie>"
    );
    let refreshed = runtime
        .library_snapshots
        .lock()
        .await
        .get(&archive.id)
        .cloned()
        .expect("replacement scan should populate a fresh cache");
    assert_eq!(refreshed.items.len(), 1);
    assert_eq!(refreshed.items[0].relative_path, "new.mp4");

    runtime.signal_epoch_fatal();
    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(root);
}
