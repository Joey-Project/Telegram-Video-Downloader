use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::*;
use crate::file_provider::{FileProviderAccessError, QueueFileProvider};
use crate::library::Presence;
use anyhow::Result;
use serde_json::Value;

const SNAPSHOT_TEST_CHAT_ID: i64 = 123_456_789;

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
}

impl FailOnceOnPathProvider {
    fn new(path: PathBuf) -> Self {
        Self {
            path: fs::canonicalize(&path).unwrap_or(path),
            armed: AtomicBool::new(false),
            failures: AtomicUsize::new(0),
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
        if normalized_path == self.path && self.armed.swap(false, Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(FileProviderAccessError::new(
                path,
                "read",
                "synthetic media-library root open failure",
            )
            .into());
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
async fn failed_non_default_library_open_keeps_cache_unavailable_until_scoped_recovery() {
    let root = temp_cloud_bot_dir("library-snapshot-scoped-open-recovery");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.telegram.allowed_chat_ids = vec![SNAPSHOT_TEST_CHAT_ID];
    config
        .ensure_runtime_dirs()
        .expect("snapshot fixture roots should create");
    let archive_root = root.join("archive");
    fs::create_dir_all(&archive_root).expect("archive root should create");
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

    let recovered = scan(Arc::clone(&provider))
        .await
        .expect("the next non-default scan should recover");
    assert_eq!(
        provider.failures.load(Ordering::SeqCst),
        1,
        "the healthy recovery scan should not trigger another denial"
    );
    assert_eq!(recovered.items[0].presence, Presence::Present);
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
        "recovery in another library must preserve the default library warning"
    );
    assert_eq!(
        runtime.library_scan_warning.lock().await.as_deref(),
        Some(default_warning.as_str())
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
    assert_eq!(
        runtime.library_unavailable.lock().await.get(&archive.id),
        Some(&archive_warning),
        "a failed removal must preserve its scoped unavailable marker"
    );
    assert_eq!(
        runtime.library_snapshot.lock().await.as_ref(),
        global_before_failed_remove.as_ref(),
        "a failed removal must not alter the legacy snapshot"
    );
    assert_eq!(
        runtime.library_scan_warning.lock().await.as_ref(),
        warning_before_failed_remove.as_ref(),
        "a failed removal must not alter the global warning"
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
