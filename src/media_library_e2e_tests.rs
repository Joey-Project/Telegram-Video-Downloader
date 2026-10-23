use super::*;

const MANAGEMENT_DISPATCH_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

// The dispatcher detaches each request child; its sole permit lives until the child drops the
// runtime clone passed through dispatch_cloud_record, so acquiring it drains that clone.
async fn wait_for_management_dispatch_idle(runtime: &CloudBotRuntime) {
    let permit = tokio_timeout(
        MANAGEMENT_DISPATCH_DRAIN_TIMEOUT,
        runtime.management_dispatch.acquire(),
    )
    .await
    .expect("detached management dispatch should finish before queue restart")
    .expect("management dispatch semaphore should remain open");
    drop(permit);
}

pub(super) struct MigrationFixture {
    pub(super) root: PathBuf,
    pub(super) video_root: PathBuf,
    pub(super) archive_root: PathBuf,
    pub(super) config: AppConfig,
    pub(super) source_library_id: String,
    pub(super) target_library_id: String,
}

pub(super) fn create_migration_fixture(
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

fn library_root_binding(queue: &QueueManager, library_id: &str) -> String {
    queue
        .registry_snapshot()
        .expect("library registry should load")
        .libraries
        .into_iter()
        .find(|library| library.id == library_id)
        .expect("library should be registered")
        .root_binding_token()
        .expect("registered library should have a bound root token")
}

pub(super) async fn scan_file_item_id(
    runtime: &CloudBotRuntime,
    seq: u64,
    library_id: &str,
    relative_path: &str,
) -> String {
    let scan = run_library_management_test_request(
        runtime,
        seq,
        "library_scan",
        serde_json::json!({ "library_id": library_id }),
    )
    .await;
    assert_eq!(scan.status, InboxStatus::Completed, "{:?}", scan.error);
    scan.result.expect("library scan should return a snapshot")["items"]
        .as_array()
        .expect("library snapshot should include items")
        .iter()
        .find(|item| item["relative_path"] == relative_path)
        .expect("requested media should appear in the library snapshot")["id"]
        .as_str()
        .expect("library item should have a stable identifier")
        .to_string()
}

fn snapshot_library_metadata_tree(
    root: &Path,
) -> std::collections::BTreeMap<PathBuf, (bool, Vec<u8>)> {
    fn walk(
        base: &Path,
        current: &Path,
        entries: &mut std::collections::BTreeMap<PathBuf, (bool, Vec<u8>)>,
    ) {
        let mut children = fs::read_dir(current)
            .expect("library metadata directory should remain readable")
            .map(|entry| entry.expect("library metadata entry should load").path())
            .collect::<Vec<_>>();
        children.sort();
        for path in children {
            let kind = fs::symlink_metadata(&path)
                .expect("library metadata entry identity should load")
                .file_type();
            assert!(
                !kind.is_symlink(),
                "fixture metadata must not contain symlinks"
            );
            let relative = path
                .strip_prefix(base)
                .expect("metadata entries should remain under the library root")
                .to_path_buf();
            if kind.is_dir() {
                entries.insert(relative.clone(), (true, Vec::new()));
                walk(base, &path, entries);
            } else {
                assert!(
                    kind.is_file(),
                    "library metadata entries should be regular files"
                );
                entries.insert(
                    relative,
                    (
                        false,
                        fs::read(&path).expect("library metadata file should read"),
                    ),
                );
            }
        }
    }

    let metadata_root = root.join(".telegram-video-downloader-library");
    let mut entries = std::collections::BTreeMap::new();
    if metadata_root.exists() {
        walk(&metadata_root, &metadata_root, &mut entries);
    }
    entries
}

pub(super) fn migration_engine_for_test(
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
        bound_migration_preview_payload(
            &runtime.bot.queue,
            source_library_id,
            target_library_id,
            serde_json::json!({
                "scope": "library",
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

#[tokio::test]
async fn expired_migration_preview_projects_stale_without_hiding_a_live_preview() {
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-preview-expiry-projection",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    fs::write(fixture.video_root.join("clip.mp4"), b"preview source media")
        .expect("source media should write");
    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let payload = bound_migration_preview_payload(
        &queue,
        &fixture.source_library_id,
        &fixture.target_library_id,
        serde_json::json!({
            "scope": "library",
            "conflict": "skip",
            "include_incomplete_tasks": false,
        }),
    );
    let expired_record = run_library_management_test_request(
        &runtime,
        3_301,
        "library_migration_preview",
        payload.clone(),
    )
    .await;
    assert_eq!(
        expired_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        expired_record.error
    );
    let expired_preview = expired_record.result.unwrap();
    let expired_id = expired_preview["id"]
        .as_str()
        .expect("migration preview should have an ID")
        .to_string();
    let expired_revision = expired_preview["revision"]
        .as_str()
        .expect("migration preview should have a revision")
        .to_string();

    let live_record = run_library_management_test_request(
        &runtime,
        3_302,
        "library_migration_preview",
        payload.clone(),
    )
    .await;
    assert_eq!(
        live_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        live_record.error
    );
    let live_preview = live_record.result.unwrap();
    let live_id = live_preview["id"]
        .as_str()
        .expect("live migration preview should have an ID")
        .to_string();
    let live_revision = live_preview["revision"]
        .as_str()
        .expect("live migration preview should have a revision")
        .to_string();

    let unknown_record = run_library_management_test_request(
        &runtime,
        3_303,
        "library_migration_preview",
        payload.clone(),
    )
    .await;
    assert_eq!(unknown_record.status, InboxStatus::AwaitingConfirmation);
    inbox
        .update(3_303, |record| {
            if let Some(payload) = record.request.payload.as_object_mut() {
                payload.remove("source_library_id");
            }
            if let Some(result) = record
                .result
                .as_mut()
                .and_then(serde_json::Value::as_object_mut)
            {
                result.remove("source_library_id");
            }
        })
        .expect("fixture should make the source binding unavailable to projection");

    let expired_path = expire_migration_preview_for_test(&fixture.video_root, &expired_id);
    let snapshot = build_cloud_state_snapshot(&runtime)
        .await
        .expect("snapshot should check migration preview age")
        .expect("cloud state should be available");

    let expired_operation = snapshot
        .operations
        .iter()
        .find(|operation| operation["seq"] == 3_301)
        .expect("expired migration operation should remain visible in history");
    assert_eq!(expired_operation["status"], "stale");
    assert!(
        expired_operation["error"]
            .as_str()
            .is_some_and(|error| error.contains("expired")),
        "the operation should explain why confirmation is disabled"
    );
    assert!(
        snapshot
            .previews
            .iter()
            .all(|preview| preview["seq"] != 3_301),
        "stale migration previews must be absent from the actionable preview list"
    );
    assert!(
        expired_path.exists(),
        "the stale projection should work before rotating maintenance retires the file"
    );

    let live_operation = snapshot
        .operations
        .iter()
        .find(|operation| operation["seq"] == 3_302)
        .expect("live migration operation should remain visible");
    assert_eq!(live_operation["status"], "awaiting_confirmation");
    assert!(snapshot.previews.iter().any(|preview| {
        preview["seq"] == 3_302
            && preview["status"] == "awaiting_confirmation"
            && preview["result"]["id"] == live_id
            && preview["revision"] == live_revision
    }));
    let unknown_operation = snapshot
        .operations
        .iter()
        .find(|operation| operation["seq"] == 3_303)
        .expect("unbound migration operation should remain visible");
    assert_eq!(unknown_operation["status"], "awaiting_confirmation");
    assert!(
        snapshot.library["warnings"]
            .as_array()
            .is_some_and(|warnings| {
                warnings.iter().any(|warning| {
                    warning.as_str().is_some_and(|text| {
                        text.contains("could not verify whether a pending preview")
                    })
                })
            })
    );
    assert_eq!(
        expired_revision.as_str(),
        expired_preview["revision"].as_str().unwrap(),
        "the expiry check is bound to the original inbox preview revision"
    );

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn migration_preview_and_discovery_reject_replaced_registered_roots() {
    let root = temp_cloud_bot_dir("migration-root-identity-gate");
    let video_root = root.join("videos");
    let pdf_root = root.join("pdfs");
    let source_root = root.join("source");
    let displaced_source = root.join("source-original");
    let replacement_source = root.join("source-replacement");
    let target_root = root.join("target");
    let displaced_target = root.join("target-original");
    let replacement_target = root.join("target-replacement");
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = video_root.clone();
    config.downloads.pdf_dir = pdf_root;
    config
        .ensure_runtime_dirs()
        .expect("stable inbox and PDF roots should create");
    fs::create_dir(&source_root).expect("source library should create");
    fs::create_dir(&target_root).expect("target library should create");
    fs::write(source_root.join("clip.mp4"), b"migration source media")
        .expect("source media should write");
    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let source_id = queue
        .register_video_library("Migration source", &source_root, None, false, false)
        .expect("secondary source should register")
        .id;
    let target_id = queue
        .register_video_library("Migration target", &target_root, None, false, false)
        .expect("secondary target should register")
        .id;
    let inbox =
        Arc::new(open_runtime_test_inbox(&video_root).expect("stable cloud inbox should open"));
    let runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));

    let preview_payload = || {
        bound_migration_preview_payload(
            &queue,
            &source_id,
            &target_id,
            serde_json::json!({
                "scope": "library",
                "conflict": "skip",
                "include_incomplete_tasks": false,
            }),
        )
    };
    let baseline = run_library_management_test_request(
        &runtime,
        4_101,
        "library_migration_preview",
        preview_payload(),
    )
    .await;
    assert_eq!(
        baseline.status,
        InboxStatus::AwaitingConfirmation,
        "baseline migration preview should open the registered roots: {:?}",
        baseline.error
    );
    assert!(
        source_root
            .join(".telegram-video-downloader-library")
            .exists()
    );
    assert!(
        target_root
            .join(".telegram-video-downloader-library")
            .exists()
    );

    fs::rename(&source_root, &displaced_source).expect("source root should be displaced");
    fs::create_dir(&source_root).expect("replacement source root should create");
    fs::write(source_root.join("replacement.mp4"), b"replacement source")
        .expect("replacement source media should write");
    let rejected_source = run_library_management_test_request(
        &runtime,
        4_102,
        "library_migration_preview",
        preview_payload(),
    )
    .await;
    assert_eq!(rejected_source.status, InboxStatus::Failed);
    assert!(rejected_source.error.as_deref().is_some_and(|error| {
        error.contains("unavailable") || error.contains("different root object")
    }));
    crate::library_migration_bot::discover_for_test(&runtime)
        .await
        .expect("discovery should skip an unavailable registered root");
    assert!(
        !source_root
            .join(".telegram-video-downloader-library")
            .exists(),
        "preview and discovery must not create library metadata on a replacement source inode"
    );
    assert!(
        !source_root
            .join(".telegram-video-downloader-queue")
            .exists(),
        "preview and discovery must not initialize queue metadata on a replacement source inode"
    );
    assert_eq!(
        fs::read(source_root.join("replacement.mp4")).unwrap(),
        b"replacement source"
    );
    fs::rename(&source_root, &replacement_source).expect("replacement source should move aside");
    fs::rename(&displaced_source, &source_root).expect("original source root should be restored");
    fs::write(source_root.join("ordinary-child-churn.mp4"), b"new child")
        .expect("same-root child churn should be allowed");
    let restored_source = run_library_management_test_request(
        &runtime,
        4_103,
        "library_migration_preview",
        preview_payload(),
    )
    .await;
    assert_eq!(
        restored_source.status,
        InboxStatus::AwaitingConfirmation,
        "restoring the same registered source object with ordinary child changes should work: {:?}",
        restored_source.error
    );

    fs::rename(&target_root, &displaced_target).expect("target root should be displaced");
    fs::create_dir(&target_root).expect("replacement target root should create");
    fs::write(target_root.join("replacement.mp4"), b"replacement target")
        .expect("replacement target media should write");
    let rejected_target = run_library_management_test_request(
        &runtime,
        4_104,
        "library_migration_preview",
        preview_payload(),
    )
    .await;
    assert_eq!(rejected_target.status, InboxStatus::Failed);
    assert!(rejected_target.error.as_deref().is_some_and(|error| {
        error.contains("unavailable") || error.contains("different root object")
    }));
    crate::library_migration_bot::discover_for_test(&runtime)
        .await
        .expect("discovery should skip an unavailable target root");
    assert!(
        !target_root
            .join(".telegram-video-downloader-library")
            .exists(),
        "preview and discovery must not create library metadata on a replacement target inode"
    );
    assert!(
        !target_root
            .join(".telegram-video-downloader-queue")
            .exists(),
        "preview and discovery must not initialize queue metadata on a replacement target inode"
    );
    assert_eq!(
        fs::read(target_root.join("replacement.mp4")).unwrap(),
        b"replacement target"
    );
    fs::rename(&target_root, &replacement_target).expect("replacement target should move aside");
    fs::rename(&displaced_target, &target_root).expect("original target root should be restored");
    fs::write(
        target_root.join("ordinary-target-churn.mp4"),
        b"new target child",
    )
    .expect("same-root target child churn should be allowed");
    let restored_target = run_library_management_test_request(
        &runtime,
        4_105,
        "library_migration_preview",
        preview_payload(),
    )
    .await;
    assert_eq!(
        restored_target.status,
        InboxStatus::AwaitingConfirmation,
        "restoring the same registered target object with ordinary child changes should work: {:?}",
        restored_target.error
    );

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn migration_preview_requires_displayed_bindings_before_opening_managers() {
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-preview-required-root-bindings",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let source_metadata_before = snapshot_library_metadata_tree(&fixture.video_root);
    let target_metadata_before = snapshot_library_metadata_tree(&fixture.archive_root);
    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let source_binding = library_root_binding(&queue, &fixture.source_library_id);
    let target_binding = library_root_binding(&queue, &fixture.target_library_id);

    let mut cases = vec![
        (
            4_201,
            serde_json::json!({
                "source_library_id": fixture.source_library_id,
                "target_library_id": fixture.target_library_id,
                "scope": "library", "conflict": "skip",
            }),
            "both displayed root bindings are required",
        ),
        (
            4_202,
            serde_json::json!({
                "source_library_id": fixture.source_library_id,
                "source_root_binding": source_binding,
                "target_library_id": fixture.target_library_id,
                "scope": "library", "conflict": "skip",
            }),
            "the target displayed root binding is required",
        ),
        (
            4_203,
            serde_json::json!({
                "source_library_id": fixture.source_library_id,
                "source_root_binding": "v1:bad",
                "target_library_id": fixture.target_library_id,
                "target_root_binding": target_binding,
                "scope": "library", "conflict": "skip",
            }),
            "malformed source binding must be rejected",
        ),
        (
            4_204,
            serde_json::json!({
                "source_library_id": fixture.source_library_id,
                "source_root_binding": source_binding,
                "target_library_id": fixture.target_library_id,
                "target_root_binding": "V1:INVALID",
                "scope": "library", "conflict": "skip",
            }),
            "malformed target binding must be rejected",
        ),
    ];
    for (seq, payload, context) in cases.drain(..) {
        let record = run_library_management_test_request(
            &runtime,
            seq,
            "library_migration_preview",
            payload,
        )
        .await;
        assert_eq!(
            record.status,
            InboxStatus::Failed,
            "{context}: {:?}",
            record.error
        );
        assert!(record.error.is_some(), "{context} should be actionable");
    }
    assert_eq!(
        snapshot_library_metadata_tree(&fixture.video_root),
        source_metadata_before,
        "missing/malformed tokens must not initialize or mutate source library metadata"
    );
    assert_eq!(
        snapshot_library_metadata_tree(&fixture.archive_root),
        target_metadata_before,
        "missing/malformed tokens must not initialize or mutate target library metadata"
    );

    let valid = run_library_management_test_request(
        &runtime,
        4_205,
        "library_migration_preview",
        bound_migration_preview_payload(
            &queue,
            &fixture.source_library_id,
            &fixture.target_library_id,
            serde_json::json!({"scope":"library", "conflict":"skip"}),
        ),
    )
    .await;
    assert_eq!(
        valid.status,
        InboxStatus::AwaitingConfirmation,
        "current displayed root bindings should authorize a preview: {:?}",
        valid.error
    );
    let result = valid
        .result
        .expect("valid preview should persist its result");
    assert_eq!(result["source_root_binding"], source_binding);
    assert_eq!(result["target_root_binding"], target_binding);

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
async fn migration_preview_binding_pair_survives_restart_and_same_root_reregistration() {
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-preview-binding-restart-reregistration",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    fs::write(
        fixture.video_root.join("root-bound.mp4"),
        b"bound preview media",
    )
    .expect("preview source media should write");
    let first_source_binding = library_root_binding(&queue, &fixture.source_library_id);
    let first_target_binding = library_root_binding(&queue, &fixture.target_library_id);
    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let first = run_library_management_test_request(
        &runtime,
        5_101,
        "library_migration_preview",
        bound_migration_preview_payload(
            &queue,
            &fixture.source_library_id,
            &fixture.target_library_id,
            serde_json::json!({"scope":"library", "conflict":"skip"}),
        ),
    )
    .await;
    assert_eq!(
        first.status,
        InboxStatus::AwaitingConfirmation,
        "initial preview should persist: {:?}",
        first.error
    );
    let first_result = first.result.expect("initial preview result should persist");
    assert_eq!(first_result["source_root_binding"], first_source_binding);
    assert_eq!(first_result["target_root_binding"], first_target_binding);

    dispatcher.abort();
    let _ = dispatcher.await;
    runtime.signal_epoch_fatal();
    drop(runtime);
    drop(inbox);

    queue
        .remove_video_library(&fixture.target_library_id)
        .expect("an unreferenced preview should not prevent test unregister");
    let target = queue
        .register_video_library("Archive", &fixture.archive_root, None, false, false)
        .expect("same root object should be allowed to register again");
    assert_eq!(target.id, fixture.target_library_id);
    let current_source_binding = library_root_binding(&queue, &fixture.source_library_id);
    let current_target_binding = library_root_binding(&queue, &fixture.target_library_id);
    assert_eq!(current_source_binding, first_source_binding);
    assert_ne!(
        current_target_binding, first_target_binding,
        "re-registration must create a new displayed registration identity even for the same inode"
    );

    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, crate::cloud::TEST_DATABASE_EPOCH)
            .expect("same-epoch inbox should reopen"),
    );
    let reopened_runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&reopened_inbox),
    );
    let reopened_dispatcher = tokio::spawn(cloud_request_dispatcher(reopened_runtime.clone()));
    let source_metadata_before = snapshot_library_metadata_tree(&fixture.video_root);
    let target_metadata_before = snapshot_library_metadata_tree(&fixture.archive_root);

    let stale_refresh = run_library_management_test_request(
        &reopened_runtime,
        5_102,
        "library_migration_preview",
        bound_migration_preview_payload(
            &queue,
            &fixture.source_library_id,
            &fixture.target_library_id,
            serde_json::json!({
                "scope":"library", "conflict":"skip",
                "supersedes_preview_seq":5_101,
                "supersedes_preview_id":first_result["id"],
                "supersedes_revision":first_result["revision"],
            }),
        ),
    )
    .await;
    assert_eq!(stale_refresh.status, InboxStatus::Failed);
    assert!(stale_refresh.error.as_deref().is_some_and(|error| {
        error.contains("root identity changed") || error.contains("fresh preview")
    }));
    assert_eq!(
        reopened_inbox.record(5_101).unwrap().unwrap().status,
        InboxStatus::AwaitingConfirmation,
        "a rejected refresh must not invalidate the persisted preview for its old pair"
    );

    let stale_confirmation = run_library_management_test_request(
        &reopened_runtime,
        5_103,
        "library_migration_confirm",
        serde_json::json!({
            "preview_id": first_result["id"], "revision": first_result["revision"],
        }),
    )
    .await;
    assert_eq!(stale_confirmation.status, InboxStatus::Failed);
    assert!(
        stale_confirmation
            .error
            .as_deref()
            .is_some_and(|error| { error.contains("root identity changed") })
    );
    assert_eq!(
        snapshot_library_metadata_tree(&fixture.video_root),
        source_metadata_before,
        "stale refresh/confirmation must not reopen or mutate source library metadata"
    );
    assert_eq!(
        snapshot_library_metadata_tree(&fixture.archive_root),
        target_metadata_before,
        "stale refresh/confirmation must not open or mutate re-registered target metadata"
    );

    let fresh_preview = run_library_management_test_request(
        &reopened_runtime,
        5_104,
        "library_migration_preview",
        bound_migration_preview_payload(
            &queue,
            &fixture.source_library_id,
            &fixture.target_library_id,
            serde_json::json!({"scope":"library", "conflict":"skip"}),
        ),
    )
    .await;
    assert_eq!(
        fresh_preview.status,
        InboxStatus::AwaitingConfirmation,
        "a fresh request with the new pair should work: {:?}",
        fresh_preview.error
    );
    let fresh_result = fresh_preview.result.unwrap();
    assert_eq!(fresh_result["source_root_binding"], current_source_binding);
    assert_eq!(fresh_result["target_root_binding"], current_target_binding);

    reopened_dispatcher.abort();
    let _ = reopened_dispatcher.await;
    reopened_runtime.signal_epoch_fatal();
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn cloud_manager_open_rejects_root_replaced_after_queue_validation() {
    let root = temp_cloud_bot_dir("cloud-manager-bound-root-race");
    let video_root = root.join("videos");
    let displaced_root = root.join("videos-original");
    let replacement_root = root.join("videos-replacement");
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = video_root.clone();
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("configured roots should create");
    let original_media = b"original media must remain on the registered inode";
    let original_nfo = b"original sidecar metadata";
    fs::write(video_root.join("clip.mp4"), original_media).expect("registered media should write");
    fs::write(video_root.join("clip.nfo"), original_nfo).expect("registered sidecar should write");
    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&video_root).expect("cloud inbox should open on the stable root"),
    );
    let runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));

    let baseline = open_cloud_library_manager(&runtime.bot, Arc::clone(&inbox), None)
        .expect("manager should open on the registered root");
    let snapshot = baseline
        .migration_scan()
        .expect("baseline scan should see registered media");
    assert_eq!(snapshot.items.len(), 1);
    drop(baseline);

    let replacement_media = b"replacement media must not be touched";
    let replacement_nfo = b"replacement sidecar must not be touched";
    let opened =
        open_cloud_library_manager_with_test_hook(&runtime.bot, Arc::clone(&inbox), None, || {
            fs::rename(&video_root, &displaced_root)?;
            fs::create_dir(&video_root)?;
            fs::write(video_root.join("clip.mp4"), replacement_media)?;
            fs::write(video_root.join("clip.nfo"), replacement_nfo)?;
            Ok(())
        });
    assert!(
        opened.is_err(),
        "manager initialization must reject replacement after the queue returned its bound root"
    );

    for private_name in [
        ".telegram-video-downloader-library",
        ".telegram-video-downloader-queue",
        ".telegram-video-downloader.lock",
        ".telegram-video-downloader-staging",
    ] {
        assert!(
            !video_root.join(private_name).exists(),
            "replacement root must not receive {private_name}"
        );
    }
    assert_eq!(
        fs::read(video_root.join("clip.mp4")).unwrap(),
        replacement_media
    );
    assert_eq!(
        fs::read(video_root.join("clip.nfo")).unwrap(),
        replacement_nfo
    );
    assert_eq!(
        fs::read(displaced_root.join("clip.mp4")).unwrap(),
        original_media
    );
    assert_eq!(
        fs::read(displaced_root.join("clip.nfo")).unwrap(),
        original_nfo
    );

    fs::rename(&video_root, &replacement_root).expect("replacement should move aside");
    fs::rename(&displaced_root, &video_root).expect("registered directory object should restore");
    fs::write(video_root.join("ordinary-child-churn.mp4"), b"new child")
        .expect("same-root child churn should remain allowed");
    let restored = open_cloud_library_manager(&runtime.bot, Arc::clone(&inbox), None)
        .expect("manager should reopen after restoring the registered root object");
    let snapshot = restored
        .migration_scan()
        .expect("restored root should scan after ordinary child churn");
    assert_eq!(snapshot.items.len(), 2);

    drop(restored);
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&root);
}

fn expire_migration_preview_for_test(video_root: &Path, preview_id: &str) -> PathBuf {
    let path = video_root
        .join(".telegram-video-downloader-library")
        .join(format!("migration-preview-{preview_id}.json"));
    let bytes = fs::read(&path).expect("persisted migration preview should be readable");
    let mut document: serde_json::Value =
        serde_json::from_slice(&bytes).expect("persisted migration preview should be JSON");
    document["created_at"] = serde_json::json!(0);
    fs::write(
        &path,
        serde_json::to_vec(&document).expect("expired preview should serialize"),
    )
    .expect("preview should be aged in its private test fixture");
    path
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
        serde_json::json!({
            "library_id": fixture.source_library_id,
        }),
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
async fn migration_discovery_skips_damaged_record_and_recovers_other_library_after_restart() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-discovery-isolates-bad-source-record",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let secondary_root = fixture.root.join("secondary-source");
    fs::create_dir(&secondary_root).expect("secondary source root should create");
    let secondary_id = queue
        .register_video_library("Secondary", &secondary_root, None, false, false)
        .expect("secondary source library should register")
        .id;
    let source_media = secondary_root.join("recoverable.mp4");
    fs::write(&source_media, b"recoverable migration after restart")
        .expect("secondary source media should write");

    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let (damaged_source_runtime, _) =
        crate::media_library_bot::scoped_runtime(&runtime, Some(&fixture.source_library_id))
            .expect("default source should scope");
    let damaged_source = crate::open_cloud_library_manager(
        &damaged_source_runtime.bot,
        Arc::clone(&runtime.inbox),
        None,
    )
    .expect("default source manager should open");
    let damaged_record_name = "migration-job-damaged-discovery.json";
    let damaged_record_bytes = b"{ malformed durable migration record";
    damaged_source
        .migration_write_state(damaged_record_name, damaged_record_bytes)
        .expect("malformed discovery record should be persisted");

    let engine = migration_engine_for_test(&runtime, &secondary_id, &fixture.target_library_id);
    let preview = engine
        .preview(&crate::library_migration::MigrationRequest {
            scope: crate::library_migration::MigrationScope::Library,
            item_ids: Vec::new(),
            conflict: crate::library::ConflictPolicy::Skip,
            item_conflicts: Default::default(),
            include_incomplete_tasks: false,
            set_target_default_on_success: false,
            task_impacts: Vec::new(),
            warnings: Vec::new(),
        })
        .expect("secondary migration preview should persist");
    engine
        .confirm(&preview.id, &preview.revision)
        .expect("healthy migration should be confirmed before restart");
    engine
        .set_runtime_context(
            &preview.id,
            crate::library_migration::MigrationRuntimeContext {
                database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
                preview_seq: 4_901,
                chat_id: None,
                message_id: None,
                delivery: crate::library_migration::MigrationDeliveryState::Pending,
            },
        )
        .expect("healthy migration should retain its restart epoch");
    drop(engine);
    drop(damaged_source);
    drop(damaged_source_runtime);
    drop(runtime);
    drop(inbox);
    drop(queue);

    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reopen after runtime restart"),
    );
    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, crate::cloud::TEST_DATABASE_EPOCH)
            .expect("same-epoch inbox should reopen"),
    );
    let reopened_runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&reopened_queue),
        Arc::clone(&reopened_inbox),
    );

    crate::library_migration_bot::discover_for_test(&reopened_runtime)
        .await
        .expect("one damaged source record should not abort discovery");
    let warning = reopened_runtime
        .migration_discovery_warnings
        .lock()
        .await
        .warnings
        .iter()
        .find(|warning| {
            warning.source_library_id == fixture.source_library_id
                && warning.record_name == damaged_record_name
                && warning.kind
                    == crate::library_migration::MigrationDiscoveryWarningKind::Malformed
        })
        .cloned()
        .expect("damaged source record should be explicitly reported");
    assert!(warning.message.contains("malformed"));
    assert_eq!(
        damaged_record_bytes.as_slice(),
        fs::read(
            fixture
                .video_root
                .join(".telegram-video-downloader-library")
                .join(damaged_record_name)
        )
        .expect("damaged migration record should remain untouched")
    );

    let discovered = wait_for_migration_status(&reopened_runtime, &preview.id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Completed
    })
    .await;
    assert_eq!(discovered.source_library_id, secondary_id);
    assert_eq!(discovered.success_count, 1);
    wait_for_migration_runner(&reopened_runtime, &preview.id, false).await;
    assert!(
        !source_media.exists(),
        "healthy recovered job should clean its exact source after publication"
    );
    assert_eq!(
        fs::read(fixture.archive_root.join("recoverable.mp4"))
            .expect("healthy recovered destination should exist"),
        b"recoverable migration after restart"
    );

    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn library_io_admission_serializes_cancelled_root_work_and_migration_runtime() {
    let (fixture, queue, inbox) = create_migration_fixture(
        "library-io-admission-runtime",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let unrelated_root = fixture.root.join("unrelated");
    fs::create_dir_all(&unrelated_root).expect("unrelated library root should create");
    let unrelated_library = queue
        .register_video_library("Independent", &unrelated_root, None, false, false)
        .expect("unrelated library should register");
    let telegram = TelegramClient::with_test_api_base_url(
        TEST_TELEGRAM_TOKEN.to_string(),
        "http://127.0.0.1:1".to_string(),
    );
    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );

    // Persist a confirmed migration without starting it. Discovery below is the real runtime
    // start path, and deterministic runner notifications expose its admission boundary.
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
            include_incomplete_tasks: false,
            set_target_default_on_success: false,
            task_impacts: Vec::new(),
            warnings: Vec::new(),
        })
        .expect("migration preview should persist");
    engine
        .confirm_with_default_revision(&preview.id, &preview.revision, None)
        .expect("migration should be durably confirmed");
    engine
        .set_runtime_context(
            &preview.id,
            crate::library_migration::MigrationRuntimeContext {
                database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
                preview_seq: 49_001,
                chat_id: None,
                message_id: None,
                delivery: crate::library_migration::MigrationDeliveryState::Pending,
            },
        )
        .expect("migration should retain its current epoch");
    drop(engine);

    let output_guard = hold_migration_output_slot(&fixture).await;
    let (operation_started_tx, operation_started_rx) = oneshot::channel();
    let (release_operation_tx, release_operation_rx) = std::sync::mpsc::sync_channel(1);
    let (operation_finished_tx, operation_finished_rx) = std::sync::mpsc::channel();
    let operation_runtime = runtime.clone();
    let operation = tokio::spawn(async move {
        crate::run_cloud_library_operation(
            &operation_runtime,
            "admission fixture scan worker failed",
            move |context, inbox| {
                let _ = operation_started_tx.send(());
                release_operation_rx
                    .recv_timeout(Duration::from_secs(30))
                    .context("test did not release the root scan worker")?;
                let snapshot = crate::scan_cloud_library(&context, inbox)?;
                let _ = operation_finished_tx.send(());
                Ok(((), snapshot))
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), operation_started_rx)
        .await
        .expect("root scan should enter its blocking worker")
        .expect("root scan start signal should be delivered");

    // Cancelling the request must not release the root admission while spawn_blocking still
    // owns filesystem work. A separately registered library remains independently admissible.
    operation.abort();
    assert!(
        operation
            .await
            .expect_err("request should be cancelled")
            .is_cancelled()
    );
    let unrelated_lease = crate::library_migration_bot::try_acquire_library_io(
        &runtime,
        std::slice::from_ref(&unrelated_library.id),
    )
    .await
    .expect("an unrelated library must remain independently admissible");
    drop(unrelated_lease);

    let (discovery, runner_acquired_before_release) = {
        let runner_waiting = runtime.test_migration_runner_gate_waiting.notified();
        tokio::pin!(runner_waiting);
        runner_waiting.as_mut().enable();
        let runner_acquired = runtime.test_migration_runner_gate_acquired.notified();
        tokio::pin!(runner_acquired);
        runner_acquired.as_mut().enable();
        let discovery_runtime = runtime.clone();
        let discovery = tokio::spawn(async move {
            crate::library_migration_bot::discover_for_test(&discovery_runtime).await
        });
        tokio::time::timeout(Duration::from_secs(10), &mut runner_waiting)
            .await
            .expect("discovery should start the confirmed migration and reach its gate wait");
        let acquired_while_root_worker_blocked =
            tokio::time::timeout(Duration::from_millis(50), &mut runner_acquired)
                .await
                .is_ok();
        (discovery, acquired_while_root_worker_blocked)
    };
    discovery
        .await
        .expect("migration discovery task should join")
        .expect("migration discovery should succeed");

    {
        let runner_acquired = runtime.test_migration_runner_gate_acquired.notified();
        tokio::pin!(runner_acquired);
        runner_acquired.as_mut().enable();
        release_operation_tx
            .send(())
            .expect("test should release the blocked root worker");
        tokio::task::spawn_blocking(move || {
            operation_finished_rx.recv_timeout(Duration::from_secs(10))
        })
        .await
        .expect("root worker completion waiter should join")
        .expect("root worker should finish after release");
    }
    if !runner_acquired_before_release {
        tokio::time::timeout(
            Duration::from_secs(10),
            runtime.test_migration_runner_gate_acquired.notified(),
        )
        .await
        .expect("migration runner should acquire source and target gates after root work ends");
    }
    assert!(
        !runner_acquired_before_release,
        "the cancelled scan's blocking worker must retain source admission until it exits"
    );

    let assert_library_operation_blocked = |library_id: String| {
        let (scoped, _) = crate::media_library_bot::scoped_runtime(&runtime, Some(&library_id))
            .expect("registered library should scope");
        async move {
            let invoked = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let invoked_by_worker = Arc::clone(&invoked);
            let result = crate::run_cloud_library_operation(
                &scoped,
                "blocked-operation fixture worker failed",
                move |context, inbox| {
                    invoked_by_worker.store(true, std::sync::atomic::Ordering::SeqCst);
                    let snapshot = crate::scan_cloud_library(&context, inbox)?;
                    Ok(((), snapshot))
                },
            )
            .await;
            assert!(
                result.is_err(),
                "migration-owned libraries must reject file operations"
            );
            assert!(
                !invoked.load(std::sync::atomic::Ordering::SeqCst),
                "a rejected operation must not enter its root worker"
            );
        }
    };
    assert_library_operation_blocked(fixture.source_library_id.clone()).await;
    assert_library_operation_blocked(fixture.target_library_id.clone()).await;

    let (unrelated_runtime, _) =
        crate::media_library_bot::scoped_runtime(&runtime, Some(&unrelated_library.id))
            .expect("unrelated library should scope");
    let unrelated_scan = crate::run_cloud_library_operation(
        &unrelated_runtime,
        "independent library scan worker failed",
        |context, inbox| {
            let snapshot = crate::scan_cloud_library(&context, inbox)?;
            Ok(((), snapshot))
        },
    )
    .await;
    unrelated_scan.expect("a migration must not block an unrelated library scan");

    drop(output_guard);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let changed = runtime.state_notify.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if runtime
                .migrations
                .lock()
                .await
                .get(&preview.id)
                .is_some_and(|status| {
                    status.status == crate::library_migration::MigrationRunStatus::Completed
                })
            {
                break;
            }
            changed.await;
        }
    })
    .await
    .expect("migration should complete after the output-slot blocker is released");

    runtime.signal_epoch_fatal();
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn terminal_migration_keeps_library_registered_until_queue_handoff_ack_after_restart() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-terminal-pending-handoff-removal",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let source_staging = downloader::create_test_retained_video_staging(&fixture.video_root)
        .expect("retained source staging directory should be created");
    let source_fragment = source_staging.join("segment.part");
    fs::write(&source_fragment, b"terminal handoff fragment")
        .expect("source task fragment should write");
    let staging_relative = source_staging
        .strip_prefix(&fixture.video_root)
        .expect("staging directory should be under the source library")
        .to_path_buf();
    let target_staging = fixture.archive_root.join(&staging_relative);
    let task_id = "migration-terminal-pending-handoff-task";
    let mut task = TaskRecord::new(
        task_id.into(),
        2_801,
        2_801,
        123_456_789,
        Some(123_456_789),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=abcdefghijk".into(),
        },
    );
    task.status = TaskStatus::Failed;
    task.error = Some("synthetic interrupted video transfer".to_string());
    task.staging_attempts.push(source_staging.clone());
    queue
        .create(task)
        .expect("failed task should persist in the source queue");

    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let snapshot = queue
        .list_incomplete_task_transfer_snapshots(&fixture.source_library_id, 16)
        .expect("failed task should be included in the migration preview")
        .into_iter()
        .find(|snapshot| snapshot.record.id == task_id)
        .expect("unfinished task should remain bound to the source library");
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
        .expect("migration preview should bind the unfinished task");
    engine
        .confirm(&preview.id, &preview.revision)
        .expect("confirmed migration should be durable before running");
    engine
        .set_runtime_context(
            &preview.id,
            crate::library_migration::MigrationRuntimeContext {
                database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
                preview_seq: 2_801,
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
        .expect("queue should persist the transfer token");
    queue
        .wait_for_task_destination_transfer(&token)
        .await
        .expect("task writer should settle before plan creation");
    let plan = queue
        .task_transfer_plan(&token)
        .expect("queue should bind the queue transfer plan");
    engine
        .attach_task_transfer(&preview.id, plan.clone())
        .expect("migration should durably attach the transfer plan");

    let migration_id = preview.id.clone();
    let awaiting_commit = tokio::time::timeout(Duration::from_secs(30), async {
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
    .expect("task transfer should reach its queue commit checkpoint");
    assert!(awaiting_commit.awaiting_task_commit);
    let installed = queue
        .commit_task_destination_transfer(&plan)
        .expect("target queue record should install before engine acknowledgment");
    assert_eq!(
        installed.destination_transfer_phase,
        Some(crate::media_libraries::TaskDestinationTransferPhase::TargetInstalled)
    );
    engine
        .ack_task_transfer(&preview.id, &token.transfer_id)
        .expect("engine should persist queue handoff acknowledgment");

    let terminal = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let worker = engine.clone();
            let id = preview.id.clone();
            let status = tokio::task::spawn_blocking(move || worker.step(&id))
                .await
                .expect("post-ack checkpoint should join")
                .expect("post-ack checkpoint should succeed");
            if status.status == crate::library_migration::MigrationRunStatus::Completed {
                return status;
            }
            assert_ne!(
                status.status,
                crate::library_migration::MigrationRunStatus::Failed,
                "post-ack migration should remain recoverable: {status:?}"
            );
        }
    })
    .await
    .expect("migration should complete before the separate queue token ACK");
    assert_eq!(
        terminal.status,
        crate::library_migration::MigrationRunStatus::Completed
    );
    let target_record = queue
        .get(task_id)
        .expect("installed target task should load")
        .expect("task should be stored in the target queue");
    assert_eq!(
        target_record.staging_attempts.first(),
        Some(&target_staging),
        "the installed target record should point at the copied staging directory"
    );
    assert_eq!(
        fs::read(target_staging.join("segment.part")).unwrap(),
        b"terminal handoff fragment",
        "target fragments must be complete before the queue handoff"
    );
    assert_eq!(
        target_record
            .destination_transfer
            .as_ref()
            .map(|transfer| transfer.migration_id.as_str()),
        Some(preview.id.as_str()),
        "Completed migration must retain the durable queue token until recovery ACK"
    );

    queue
        .set_default_video_library(&fixture.target_library_id)
        .expect("source must no longer be the default before removal");
    runtime.signal_epoch_fatal();
    drop(engine);
    drop(runtime);
    drop(inbox);
    drop(queue);

    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reopen after target install"),
    );
    let reopened_target_record = reopened_queue
        .get(task_id)
        .expect("installed target task should load after restart")
        .expect("task should remain stored in the target queue");
    assert_eq!(
        reopened_target_record
            .destination
            .as_ref()
            .map(|destination| destination.library_id.as_str()),
        Some(fixture.target_library_id.as_str())
    );
    assert_eq!(
        reopened_target_record
            .destination_transfer
            .as_ref()
            .map(|transfer| transfer.migration_id.as_str()),
        Some(preview.id.as_str()),
        "reopened target record must retain the durable migration token"
    );
    assert_eq!(
        reopened_target_record.staging_attempts.first(),
        Some(&target_staging),
        "the target queue record must retain the exact rebased staging path"
    );
    assert!(source_staging.exists());
    assert_eq!(
        fs::read(target_staging.join("segment.part")).unwrap(),
        b"terminal handoff fragment"
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
    let dispatcher = tokio::spawn(cloud_request_dispatcher(reopened_runtime.clone()));
    // Hold the process-wide output slot so startup can durably install its exact queue ACK but
    // cannot yet remove source controls. This proves queue startup remains blocked until the
    // source cleanup checkpoint completes.
    let cleanup_blocker = hold_migration_output_slot(&fixture).await;

    let premature_removal = run_library_management_test_request(
        &reopened_runtime,
        2_802,
        "library_remove",
        serde_json::json!({
            "library_id": fixture.source_library_id,
            "root_binding": library_root_binding(&reopened_queue, &fixture.source_library_id),
        }),
    )
    .await;
    assert_eq!(
        premature_removal.status,
        InboxStatus::Failed,
        "a terminal migration with a pending queue handoff must retain the registration"
    );
    assert!(
        premature_removal
            .error
            .as_deref()
            .is_some_and(|error| error.contains("task destination handoff")),
        "removal should identify the pending queue handoff: {:?}",
        premature_removal.error
    );
    assert!(
        reopened_queue
            .registry_snapshot()
            .expect("registry should remain readable")
            .libraries
            .iter()
            .any(|library| library.id == fixture.source_library_id)
    );

    let migration_watcher = tokio::spawn(crate::library_migration_bot::watch(
        reopened_runtime.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let current = reopened_queue
                .get(task_id)
                .expect("recovered target task should load")
                .expect("task should remain in the target queue");
            if current.destination_transfer.is_none() && current.destination_transfer_ack.is_some()
            {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("recovery should retain its exact queue ACK while source cleanup waits");
    assert!(source_staging.exists());
    assert!(target_staging.join("segment.part").exists());
    let capture_source_staging = || {
        let mut entries = fs::read_dir(&source_staging)
            .expect("source staging should remain readable")
            .map(|entry| {
                let entry = entry.expect("source staging entry should be readable");
                let file_type = entry
                    .file_type()
                    .expect("source staging entry type should be readable");
                let bytes = file_type
                    .is_file()
                    .then(|| fs::read(entry.path()).expect("source staging control should read"));
                Ok::<_, std::io::Error>((entry.file_name(), bytes))
            })
            .collect::<std::io::Result<Vec<_>>>()
            .expect("source staging contents should enumerate");
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    };
    let source_staging_before_refused_removal = capture_source_staging();
    let target_fragment_before_refused_removal =
        fs::read(target_staging.join("segment.part")).unwrap();
    let cleanup_blocked_removal = run_library_management_test_request(
        &reopened_runtime,
        2_804,
        "library_remove",
        serde_json::json!({
            "library_id": fixture.source_library_id,
            "root_binding": library_root_binding(&reopened_queue, &fixture.source_library_id),
        }),
    )
    .await;
    assert_eq!(cleanup_blocked_removal.status, InboxStatus::Failed);
    assert!(
        cleanup_blocked_removal
            .error
            .as_deref()
            .is_some_and(|error| error.contains("active migration")),
        "removal during the active recovery lease should be refused by admission: {:?}",
        cleanup_blocked_removal.error
    );
    assert!(
        reopened_queue
            .registry_snapshot()
            .expect("registry should remain readable during cleanup")
            .libraries
            .iter()
            .any(|library| library.id == fixture.source_library_id),
        "the source registration must remain while recovery owns its gate"
    );
    assert_eq!(
        capture_source_staging(),
        source_staging_before_refused_removal,
        "refused removal must preserve the source staging entries during cleanup"
    );
    assert_eq!(
        fs::read(target_staging.join("segment.part")).unwrap(),
        target_fragment_before_refused_removal,
        "refused removal must preserve the installed target fragment"
    );
    drop(cleanup_blocker);
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let current = reopened_queue
                .get(task_id)
                .expect("recovered target task should load")
                .expect("task should remain in the target queue");
            if current.destination_transfer.is_none() && current.destination_transfer_ack.is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("migration recovery should finish source cleanup before releasing the task");
    let acknowledged_record = reopened_queue
        .get(task_id)
        .expect("acknowledged target task should load")
        .expect("task should remain stored in the target queue");
    assert!(acknowledged_record.destination_transfer.is_none());
    assert!(acknowledged_record.destination_transfer_ack.is_none());
    assert!(acknowledged_record.destination_transfer_phase.is_none());
    assert_eq!(
        acknowledged_record
            .destination
            .as_ref()
            .map(|destination| destination.library_id.as_str()),
        Some(fixture.target_library_id.as_str()),
        "queue ACK should retain the task at its target destination"
    );
    assert_eq!(
        acknowledged_record
            .destination
            .as_ref()
            .map(|destination| destination.root.as_path()),
        Some(fixture.archive_root.as_path())
    );
    assert_eq!(acknowledged_record.status, TaskStatus::Failed);
    assert_eq!(
        acknowledged_record.staging_attempts.first(),
        Some(&target_staging),
        "the queue should resume with the rebased target staging path after cleanup"
    );
    assert!(
        !source_staging.exists(),
        "the exact source job tree should be removed only after the durable final ACK"
    );
    assert!(target_staging.join(".owner.json").exists());
    assert!(target_staging.join(".retained.json").exists());
    assert_eq!(
        fs::read(target_staging.join("segment.part")).unwrap(),
        b"terminal handoff fragment",
        "source cleanup must preserve the target fragment"
    );
    let startup_lease = downloader::hold_video_outputs_for_migration(&[
        fixture.video_root.clone(),
        fixture.archive_root.clone(),
    ])
    .await
    .expect("a later startup should acquire both roots without a source staging orphan");
    drop(startup_lease);
    assert!(
        !source_staging.exists(),
        "startup recovery must not recreate or retain the removed source job tree"
    );

    let (source_scoped, _) = crate::media_library_bot::scoped_runtime(
        &reopened_runtime,
        Some(&fixture.source_library_id),
    )
    .expect("source library should remain registered until cleanup is complete");
    let source_manager = crate::open_cloud_library_manager(
        &source_scoped.bot,
        Arc::clone(&reopened_runtime.inbox),
        None,
    )
    .expect("cleaned source manager should reopen");
    let source_after_cleanup = source_manager
        .migration_scan()
        .expect("cleaned source library should reopen without recovery errors");
    assert!(
        !source_after_cleanup
            .warnings
            .iter()
            .any(|warning| warning.contains("staging") || warning.contains("recovery")),
        "startup after source cleanup should not report a staging recovery orphan: {:?}",
        source_after_cleanup.warnings
    );

    let recovered_removal = run_library_management_test_request(
        &reopened_runtime,
        2_805,
        "library_remove",
        serde_json::json!({
            "library_id": fixture.source_library_id,
            "root_binding": library_root_binding(&reopened_queue, &fixture.source_library_id),
        }),
    )
    .await;
    assert_eq!(
        recovered_removal.status,
        InboxStatus::Completed,
        "source removal should succeed after queue ACK: {:?}",
        recovered_removal.error
    );
    assert!(
        !reopened_queue
            .registry_snapshot()
            .expect("registry should remain readable after removal")
            .libraries
            .iter()
            .any(|library| library.id == fixture.source_library_id)
    );
    assert_eq!(
        reopened_queue
            .get(task_id)
            .expect("target task should remain readable after source removal")
            .expect("queue transfer should retain the target task")
            .status,
        TaskStatus::Failed
    );

    reopened_runtime.signal_epoch_fatal();
    migration_watcher
        .await
        .expect("migration watcher should stop after runtime shutdown");
    dispatcher.abort();
    let _ = dispatcher.await;
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
        bound_migration_preview_payload(
            &queue,
            &fixture.source_library_id,
            &fixture.target_library_id,
            serde_json::json!({
                "scope": "library",
                "conflict": "skip",
                "include_incomplete_tasks": true,
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
    // The durable stop status may be stored before the detached runner finishes reconciling
    // its queue handoff. Wait for the runner boundary before asserting the final token state.
    wait_for_migration_runner(&runtime, &migration_id, false).await;
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
        telegram.clone(),
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
        bound_migration_preview_payload(
            &queue,
            &fixture.source_library_id,
            &fixture.target_library_id,
            serde_json::json!({
                "scope": "library",
                "conflict": "skip",
                "set_target_default_on_success": true,
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
    let preview = preview_record.result.unwrap();
    let confirmation_baseline = queue
        .registry_snapshot()
        .expect("library settings should load before confirmation")
        .revision;
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
    assert_eq!(
        engine
            .default_update_expected_revision(&migration_id)
            .expect("confirmation should persist its settings baseline"),
        Some(confirmation_baseline),
        "the baseline belongs to the durable confirmation, before runner lease waits"
    );
    queue
        .set_default_video_library(&later_default.id)
        .expect("user should be able to choose a different default during migration");

    // Simulate process restart while the migration is still waiting for the source/target output
    // lease. The confirmed settings revision must survive, rather than being rebound to the
    // already-changed default when the watcher resumes the job.
    dispatcher.abort();
    let _ = dispatcher.await;
    runtime.signal_epoch_fatal();
    wait_for_migration_runner(&runtime, &migration_id, false).await;
    drop(engine);
    drop(runtime);
    drop(inbox);
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&queue) > 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("detached migration runner should release its queue owner before restart");
    drop(queue);

    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reopen after runtime restart"),
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
    wait_for_migration_runner(&reopened_runtime, &migration_id, true).await;
    let reopened_engine = migration_engine_for_test(
        &reopened_runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    assert_eq!(
        reopened_engine
            .default_update_expected_revision(&migration_id)
            .expect("confirmed revision should survive restart"),
        Some(confirmation_baseline)
    );
    drop(output_guard);

    let completed = wait_for_migration_status(&reopened_runtime, &migration_id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Completed
            && status.default_update_resolved
    })
    .await;
    wait_for_migration_runner(&reopened_runtime, &migration_id, false).await;
    assert!(!completed.default_update_applied);
    assert!(
        completed
            .warnings
            .iter()
            .any(|warning| warning.contains("settings changed"))
    );
    assert_eq!(
        reopened_queue
            .default_video_destination()
            .expect("default destination should still resolve")
            .library_id,
        later_default.id,
        "migration CAS must preserve the user's newer default choice"
    );

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
        bound_migration_preview_payload(
            &queue,
            &fixture.source_library_id,
            &fixture.target_library_id,
            serde_json::json!({
                "scope": "library",
                "conflict": "skip",
            }),
        ),
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
        bound_migration_preview_payload(
            &queue,
            &fixture.source_library_id,
            &fixture.target_library_id,
            serde_json::json!({
                "scope": "library",
                "conflict": "skip",
                "item_conflicts": {item_id: "keep_both"},
                "supersedes_preview_seq": 2_001,
                "supersedes_preview_id": first_preview["id"],
                "supersedes_revision": first_preview["revision"],
            }),
        ),
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

#[tokio::test]
async fn completed_default_finalization_requires_new_epoch_authorization_and_blocks_removal() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-terminal-default-epoch-finalization",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
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
            include_incomplete_tasks: false,
            set_target_default_on_success: true,
            task_impacts: Vec::new(),
            warnings: Vec::new(),
        })
        .expect("empty media library should produce a default-update preview");
    let expected_settings_revision = queue
        .registry_snapshot()
        .expect("library registry should load before confirmation")
        .revision;
    engine
        .confirm_with_default_revision(
            &preview.id,
            &preview.revision,
            Some(expected_settings_revision),
        )
        .expect("default update should be bound to the confirmation settings revision");
    engine
        .set_runtime_context(
            &preview.id,
            crate::library_migration::MigrationRuntimeContext {
                database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
                preview_seq: 3_101,
                chat_id: None,
                message_id: None,
                delivery: crate::library_migration::MigrationDeliveryState::Pending,
            },
        )
        .expect("migration should save the original cloud epoch");
    let mut terminal = None;
    for _ in 0..8 {
        let worker = engine.clone();
        let id = preview.id.clone();
        let status = tokio::task::spawn_blocking(move || worker.step(&id))
            .await
            .expect("empty migration checkpoint should join")
            .expect("empty migration checkpoint should succeed");
        if status.status == crate::library_migration::MigrationRunStatus::Completed {
            terminal = Some(status);
            break;
        }
    }
    let terminal = terminal.expect("empty migration should reach Completed");
    assert!(terminal.terminal_finalization_pending);
    assert!(!terminal.default_update_resolved);

    runtime.signal_epoch_fatal();
    drop(engine);
    drop(runtime);
    drop(inbox);
    drop(queue);

    let changed_epoch = "fedcba9876543210fedcba9876543210";
    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reopen in the changed epoch"),
    );
    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, changed_epoch)
            .expect("new cloud database epoch should open"),
    );
    let reopened_runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&reopened_queue),
        Arc::clone(&reopened_inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(reopened_runtime.clone()));
    let watcher = tokio::spawn(crate::library_migration_bot::watch(
        reopened_runtime.clone(),
    ));
    let discovered = wait_for_migration_status(&reopened_runtime, &preview.id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Completed
            && status.terminal_finalization_pending
    })
    .await;
    wait_for_migration_runner(&reopened_runtime, &preview.id, false).await;
    assert!(!discovered.default_update_resolved);
    let reopened_engine = migration_engine_for_test(
        &reopened_runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    assert_eq!(
        reopened_engine
            .runtime_context(&preview.id)
            .expect("recovery context should load")
            .expect("recovery context should remain present")
            .database_epoch,
        crate::cloud::TEST_DATABASE_EPOCH,
        "discovery must not silently reauthorize terminal effects across an epoch change"
    );

    let remove_before_finalize = run_library_management_test_request(
        &reopened_runtime,
        3_102,
        "library_remove",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": library_root_binding(&reopened_queue, &fixture.target_library_id),
        }),
    )
    .await;
    assert_eq!(remove_before_finalize.status, InboxStatus::Failed);
    assert!(
        remove_before_finalize
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("pending terminal migration finalization")
    );

    let stale_pause = run_library_management_test_request(
        &reopened_runtime,
        3_103,
        "library_migration_control",
        serde_json::json!({"migration_id": preview.id, "control": "pause"}),
    )
    .await;
    assert_eq!(stale_pause.status, InboxStatus::Completed);
    assert_eq!(
        reopened_engine
            .runtime_context(&preview.id)
            .expect("recovery context should remain readable")
            .expect("recovery context should remain present")
            .database_epoch,
        crate::cloud::TEST_DATABASE_EPOCH,
        "an ordinary stale terminal control must not reauthorize the default update"
    );

    let accepted = run_library_management_test_request(
        &reopened_runtime,
        3_104,
        "library_migration_control",
        serde_json::json!({
            "migration_id": preview.id,
            "control": "finalize_terminal",
        }),
    )
    .await;
    assert_eq!(
        accepted.status,
        InboxStatus::Completed,
        "{:?}",
        accepted.error
    );
    assert_eq!(accepted.result.as_ref().unwrap()["accepted"], true);
    assert_eq!(
        accepted.result.as_ref().unwrap()["migration"]["status"],
        "completed",
        "the inbox receipt acknowledges accepted finalization while background settings work runs"
    );

    let finalized = wait_for_migration_status(&reopened_runtime, &preview.id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Completed
            && status.default_update_resolved
            && !status.terminal_finalization_pending
    })
    .await;
    wait_for_migration_runner(&reopened_runtime, &preview.id, false).await;
    assert!(finalized.default_update_applied);
    assert_eq!(
        reopened_queue
            .default_video_destination()
            .expect("updated default should resolve")
            .library_id,
        fixture.target_library_id,
        "explicit terminal reauthorization should apply the saved settings CAS"
    );

    reopened_runtime.signal_epoch_fatal();
    watcher
        .await
        .expect("migration watcher should stop after runtime shutdown");
    dispatcher.abort();
    let _ = dispatcher.await;
    drop(reopened_engine);
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn stopped_target_installed_handoff_reauthorizes_once_and_keeps_dispatch_responsive() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "migration-stopped-target-installed-epoch-finalization",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let retained = downloader::create_test_retained_video_staging(&fixture.video_root)
        .expect("retained task staging should be created");
    let fragment = b"stopped installed-before-engine-ack fragment";
    fs::write(retained.join("segment.part"), fragment).expect("task fragment should be written");
    let source_media = fixture.video_root.join("stopped-unack.mp4");
    fs::write(&source_media, b"media remains after Stop remaining")
        .expect("source media should be written");
    let task_id = "migration-stopped-target-installed-unacked-task";
    let mut task = TaskRecord::new(
        task_id.into(),
        3_201,
        3_201,
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
        .expect("failed task should persist in its source library");

    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let snapshot = queue
        .list_incomplete_task_transfer_snapshots(&fixture.source_library_id, 16)
        .expect("failed task should be available to migration")
        .into_iter()
        .find(|snapshot| snapshot.record.id == task_id)
        .expect("failed task should remain bound to the source library");
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
        .expect("preview should bind media and the incomplete task");
    engine
        .confirm(&preview.id, &preview.revision)
        .expect("migration should persist before copying task data");
    engine
        .set_runtime_context(
            &preview.id,
            crate::library_migration::MigrationRuntimeContext {
                database_epoch: crate::cloud::TEST_DATABASE_EPOCH.to_string(),
                preview_seq: 3_201,
                chat_id: None,
                message_id: None,
                delivery: crate::library_migration::MigrationDeliveryState::Pending,
            },
        )
        .expect("migration should save its original cloud epoch");
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
        .expect("queue should persist the task transfer token");
    queue
        .wait_for_task_destination_transfer(&token)
        .await
        .expect("task writer should settle before plan creation");
    let plan = queue
        .task_transfer_plan(&token)
        .expect("queue should bind the source task payload");
    engine
        .attach_task_transfer(&preview.id, plan.clone())
        .expect("migration should persist the exact task plan");
    let task_copy = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let worker = engine.clone();
            let id = preview.id.clone();
            let status = tokio::task::spawn_blocking(move || worker.step(&id))
                .await
                .expect("task copy checkpoint should join")
                .expect("task copy checkpoint should succeed");
            assert_ne!(
                status.status,
                crate::library_migration::MigrationRunStatus::Failed,
                "task plan should remain recoverable: {status:?}"
            );
            if status.awaiting_task_commit {
                break status;
            }
        }
    })
    .await
    .expect("task copy should reach target queue installation");
    assert!(task_copy.awaiting_task_commit);
    let installed = queue
        .commit_task_destination_transfer(&plan)
        .expect("target queue record should install before engine acknowledgment");
    assert_eq!(
        installed.destination_transfer_phase,
        Some(crate::media_libraries::TaskDestinationTransferPhase::TargetInstalled)
    );
    let target_staging = installed.staging_attempts[0].clone();
    assert_eq!(
        fs::read(target_staging.join("segment.part")).unwrap(),
        fragment
    );
    let stopped = engine
        .control(
            &preview.id,
            crate::library_migration::MigrationControl::StopRemaining,
        )
        .expect("stop intent should persist after target installation");
    assert_eq!(
        stopped.status,
        crate::library_migration::MigrationRunStatus::Stopped
    );
    assert!(
        queue
            .has_pending_task_destination_transfer_for_migration(&preview.id)
            .expect("exact queue scan should see the target-installed handoff")
    );
    assert!(
        !stopped.terminal_finalization_pending,
        "the engine ACK is intentionally missing; exact queue proof must supply the pending state"
    );
    assert!(source_media.exists());
    assert_eq!(
        fs::read(&source_media).unwrap(),
        b"media remains after Stop remaining"
    );
    assert!(retained.join("segment.part").exists());

    runtime.signal_epoch_fatal();
    drop(engine);
    drop(runtime);
    drop(inbox);
    drop(queue);

    let changed_epoch = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config).expect("queue should reopen after stopped crash"),
    );
    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, changed_epoch)
            .expect("new database epoch should open"),
    );
    let reopened_runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&reopened_queue),
        Arc::clone(&reopened_inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(reopened_runtime.clone()));
    let watcher = tokio::spawn(crate::library_migration_bot::watch(
        reopened_runtime.clone(),
    ));
    let reopened_engine = migration_engine_for_test(
        &reopened_runtime,
        &fixture.source_library_id,
        &fixture.target_library_id,
    );
    let discovered = wait_for_migration_status(&reopened_runtime, &preview.id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Stopped
            && status.terminal_finalization_pending
    })
    .await;
    assert_eq!(
        discovered.status,
        crate::library_migration::MigrationRunStatus::Stopped
    );
    wait_for_migration_runner(&reopened_runtime, &preview.id, false).await;
    let retained_token = reopened_queue
        .get(task_id)
        .expect("task should load after reopen")
        .expect("task should remain in the queue");
    assert!(retained_token.destination_transfer.is_some());
    assert!(retained_token.destination_transfer_ack.is_none());
    assert_eq!(
        fs::read(retained.join("segment.part")).unwrap(),
        fragment,
        "epoch discovery must not clean source payload or auto-install its queue acknowledgment"
    );
    assert!(
        source_media.exists(),
        "stopped media source must remain untouched"
    );
    assert_eq!(
        fs::read(&source_media).unwrap(),
        b"media remains after Stop remaining"
    );

    let output_guard = hold_migration_output_slot(&fixture).await;
    let accepted = run_library_management_test_request(
        &reopened_runtime,
        3_202,
        "library_migration_control",
        serde_json::json!({
            "migration_id": preview.id,
            "control": "finalize_terminal",
        }),
    )
    .await;
    assert_eq!(
        accepted.status,
        InboxStatus::Completed,
        "{:?}",
        accepted.error
    );
    assert_eq!(accepted.result.as_ref().unwrap()["accepted"], true);

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let record = reopened_queue
                .get(task_id)
                .expect("queue task should remain readable")
                .expect("queue task should remain present");
            let worker = reopened_engine.clone();
            let id = preview.id.clone();
            let transfers = tokio::task::spawn_blocking(move || {
                worker.task_transfer_statuses(&id)
            })
            .await
            .expect("task transfer checkpoint read should join")
            .expect("task transfer checkpoint should load");
            if record.destination_transfer_ack.is_some()
                && transfers.first().is_some_and(|transfer| transfer.commit_acked)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background finalizer should install and persist the target queue acknowledgment before waiting for output lease");
    assert!(
        reopened_runtime
            .migration_runners
            .lock()
            .await
            .contains(&preview.id)
    );

    let second_control = tokio::time::timeout(
        Duration::from_secs(5),
        run_library_management_test_request(
            &reopened_runtime,
            3_203,
            "library_migration_control",
            serde_json::json!({"migration_id": preview.id, "control": "pause"}),
        ),
    )
    .await
    .expect("dispatcher should process a harmless control while cleanup waits on the output lease");
    assert_eq!(second_control.status, InboxStatus::Completed);
    assert_eq!(
        second_control.result.as_ref().unwrap()["migration"]["status"],
        "stopped",
        "ordinary Pause on a terminal migration is a safe no-op"
    );
    drop(output_guard);

    let finalized = wait_for_migration_status(&reopened_runtime, &preview.id, |status| {
        status.status == crate::library_migration::MigrationRunStatus::Stopped
            && !status.terminal_finalization_pending
            && status
                .task_transfers
                .first()
                .is_some_and(|transfer| transfer.source_cleanup_complete)
    })
    .await;
    wait_for_migration_runner(&reopened_runtime, &preview.id, false).await;
    assert_eq!(
        finalized.status,
        crate::library_migration::MigrationRunStatus::Stopped
    );
    let task = reopened_queue
        .get(task_id)
        .expect("finalized task should load")
        .expect("finalized task should remain present");
    assert_eq!(task.status, TaskStatus::Failed);
    assert_eq!(
        task.destination.as_ref().unwrap().library_id,
        fixture.target_library_id
    );
    assert!(task.destination_transfer.is_none());
    assert!(task.destination_transfer_phase.is_none());
    assert!(task.destination_transfer_ack.is_none());
    assert!(!retained.join("segment.part").exists());
    assert_eq!(
        fs::read(target_staging.join("segment.part")).unwrap(),
        fragment
    );
    assert!(
        source_media.exists(),
        "terminal finalizer must not resume the stopped media plan"
    );
    assert_eq!(
        fs::read(&source_media).unwrap(),
        b"media remains after Stop remaining"
    );
    assert_eq!(
        fs::read(fixture.archive_root.join("stopped-unack.mp4")).unwrap(),
        b"media remains after Stop remaining",
        "already-published target copies remain available after Stop remaining"
    );
    assert_eq!(
        reopened_queue
            .default_video_destination()
            .expect("source default should remain available")
            .library_id,
        fixture.source_library_id,
        "Stopped finalization must not apply a success-only default update"
    );

    reopened_runtime.signal_epoch_fatal();
    watcher
        .await
        .expect("migration watcher should stop after runtime shutdown");
    dispatcher.abort();
    let _ = dispatcher.await;
    drop(reopened_engine);
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn stale_settings_bindings_cannot_mutate_a_reregistered_same_root() {
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "stale-settings-registration-binding",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let archive_media = fixture.archive_root.join("keep.mp4");
    fs::write(&archive_media, b"registered library B media")
        .expect("archive media should be available for the stale-action checks");
    let first_root = crate::safe_fs::RootedFs::new(&fixture.archive_root)
        .expect("A root should open")
        .root_identity();
    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));

    let first_binding = library_root_binding(&queue, &fixture.target_library_id);
    let removed_first = run_library_management_test_request(
        &runtime,
        31_001,
        "library_remove",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": first_binding,
        }),
    )
    .await;
    assert_eq!(
        removed_first.status,
        InboxStatus::Completed,
        "{:?}",
        removed_first.error
    );

    let scoped_metadata = fixture
        .archive_root
        .join(".telegram-video-downloader-library");
    if scoped_metadata.exists() {
        fs::remove_dir_all(&scoped_metadata)
            .expect("the migration preflight metadata from A should be removed in this fixture");
    }
    assert!(
        !scoped_metadata.exists(),
        "the same-root B fixture starts without library-manager metadata"
    );

    let registered_second = run_library_management_test_request(
        &runtime,
        31_002,
        "library_register",
        serde_json::json!({
            "name": "Archive B",
            "path": fixture.archive_root,
            "set_default": false,
            "create_directory": false,
        }),
    )
    .await;
    assert_eq!(
        registered_second.status,
        InboxStatus::Completed,
        "{:?}",
        registered_second.error
    );
    let registered_b = queue
        .registry_snapshot()
        .expect("registry should list B")
        .libraries
        .into_iter()
        .find(|library| library.name == "Archive B")
        .expect("B should be registered");
    assert_eq!(
        registered_b.id, fixture.target_library_id,
        "the path-derived ID is intentionally reused"
    );
    let second_root = crate::safe_fs::RootedFs::new(&fixture.archive_root)
        .expect("same-root B should open")
        .root_identity();
    assert_eq!(
        queue
            .registry_snapshot()
            .unwrap()
            .libraries
            .iter()
            .find(|library| library.id == fixture.target_library_id)
            .and_then(|library| library.root_binding_token())
            .as_deref(),
        Some(registered_b.root_binding_token().unwrap().as_str())
    );
    let second_binding = registered_b
        .root_binding_token()
        .expect("B should have a displayed registration identity");
    assert_ne!(
        second_binding, first_binding,
        "a new registration of the same directory object gets a fresh opaque identity"
    );
    assert_eq!(
        first_root, second_root,
        "A and B use the same directory object in this regression"
    );

    let child = fixture.archive_root.join("ordinary-child");
    fs::create_dir(&child).expect("ordinary child should create");
    fs::remove_dir(&child).expect("ordinary child should remove");
    assert_eq!(
        library_root_binding(&queue, &fixture.target_library_id),
        second_binding,
        "child-entry churn does not invalidate B's displayed identity"
    );

    let registry_path = crate::media_libraries::registry_path(None, &fixture.video_root)
        .expect("registry path should resolve");
    let before_stale = queue.registry_snapshot().unwrap();
    let registry_bytes_before = fs::read(&registry_path).expect("registry bytes should read");
    let stale_actions = [
        (
            "library_set_default",
            serde_json::json!({
                "library_id": fixture.target_library_id,
                "root_binding": first_binding,
            }),
        ),
        (
            "library_set_category",
            serde_json::json!({
                "library_id": fixture.target_library_id,
                "root_binding": first_binding,
                "default_category": "Stale category",
            }),
        ),
        (
            "library_rename",
            serde_json::json!({
                "library_id": fixture.target_library_id,
                "root_binding": first_binding,
                "name": "Stale name",
            }),
        ),
        (
            "library_remove",
            serde_json::json!({
                "library_id": fixture.target_library_id,
                "root_binding": first_binding,
            }),
        ),
    ];
    for (offset, (kind, payload)) in stale_actions.into_iter().enumerate() {
        let result =
            run_library_management_test_request(&runtime, 31_010 + offset as u64, kind, payload)
                .await;
        assert_eq!(
            result.status,
            InboxStatus::Failed,
            "{kind} must reject A's token"
        );
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|error| error.contains("library root identity changed")),
            "{kind} should report a stale displayed identity: {:?}",
            result.error
        );
    }

    for (seq, payload) in [
        (
            31_020,
            serde_json::json!({
                "library_id": fixture.target_library_id,
                "name": "Missing binding",
            }),
        ),
        (
            31_021,
            serde_json::json!({
                "library_id": fixture.target_library_id,
                "root_binding": "v1:not-a-token",
                "default_category": "Invalid binding",
            }),
        ),
    ] {
        let result = run_library_management_test_request(
            &runtime,
            seq,
            if seq == 31_020 {
                "library_rename"
            } else {
                "library_set_category"
            },
            payload,
        )
        .await;
        assert_eq!(result.status, InboxStatus::Failed);
    }

    assert_eq!(queue.registry_snapshot().unwrap(), before_stale);
    assert_eq!(
        fs::read(&registry_path).unwrap(),
        registry_bytes_before,
        "stale or incomplete actions do not rewrite the registry"
    );
    assert!(
        !scoped_metadata.exists(),
        "stale removal must reject before opening B's library manager"
    );
    assert_eq!(
        fs::read(&archive_media).unwrap(),
        b"registered library B media",
        "stale actions preserve B's media bytes"
    );

    let selected_b = run_library_management_test_request(
        &runtime,
        31_030,
        "library_set_default",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": second_binding,
        }),
    )
    .await;
    assert_eq!(
        selected_b.status,
        InboxStatus::Completed,
        "{:?}",
        selected_b.error
    );
    let category_b = run_library_management_test_request(
        &runtime,
        31_031,
        "library_set_category",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": second_binding,
            "default_category": "Films",
        }),
    )
    .await;
    assert_eq!(
        category_b.status,
        InboxStatus::Completed,
        "{:?}",
        category_b.error
    );
    let rename_b = run_library_management_test_request(
        &runtime,
        31_032,
        "library_rename",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": second_binding,
            "name": "Archive B renamed",
        }),
    )
    .await;
    assert_eq!(
        rename_b.status,
        InboxStatus::Completed,
        "{:?}",
        rename_b.error
    );
    assert_eq!(
        library_root_binding(&queue, &fixture.target_library_id),
        second_binding,
        "the token remains stable across category, name, and default changes"
    );

    let source_binding = library_root_binding(&queue, &fixture.source_library_id);
    let restore_default = run_library_management_test_request(
        &runtime,
        31_033,
        "library_set_default",
        serde_json::json!({
            "library_id": fixture.source_library_id,
            "root_binding": source_binding,
        }),
    )
    .await;
    assert_eq!(restore_default.status, InboxStatus::Completed);
    let remove_b = run_library_management_test_request(
        &runtime,
        31_034,
        "library_remove",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": second_binding,
        }),
    )
    .await;
    assert_eq!(
        remove_b.status,
        InboxStatus::Completed,
        "{:?}",
        remove_b.error
    );
    assert_eq!(
        fs::read(&archive_media).unwrap(),
        b"registered library B media"
    );

    let displaced_archive = fixture.root.join("archive-displaced");
    fs::rename(&fixture.archive_root, &displaced_archive)
        .expect("removed B root should be movable in the replacement fixture");
    fs::create_dir(&fixture.archive_root)
        .expect("replacement root should reuse the registered pathname");
    let replacement_media = fixture.archive_root.join("replacement.mp4");
    fs::write(&replacement_media, b"replacement library media")
        .expect("replacement media should be available");
    let registered_c = run_library_management_test_request(
        &runtime,
        31_035,
        "library_register",
        serde_json::json!({
            "name": "Archive C",
            "path": fixture.archive_root,
            "set_default": false,
            "create_directory": false,
        }),
    )
    .await;
    assert_eq!(
        registered_c.status,
        InboxStatus::Completed,
        "{:?}",
        registered_c.error
    );
    let replacement_library = queue
        .registry_snapshot()
        .expect("replacement registration should appear")
        .libraries
        .into_iter()
        .find(|library| library.id == fixture.target_library_id)
        .expect("path-derived ID should be reused for replacement C");
    let replacement_binding = replacement_library
        .root_binding_token()
        .expect("replacement C should have an identity token");
    assert_ne!(
        replacement_binding, second_binding,
        "the replacement object and registration must not reuse B's token"
    );
    let replacement_registry = queue.registry_snapshot().unwrap();
    let replacement_registry_bytes = fs::read(&registry_path).unwrap();
    let replacement_metadata = fixture
        .archive_root
        .join(".telegram-video-downloader-library");
    assert!(!replacement_metadata.exists());
    let stale_b_rename = run_library_management_test_request(
        &runtime,
        31_036,
        "library_rename",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": second_binding,
            "name": "Stale B rename",
        }),
    )
    .await;
    assert_eq!(stale_b_rename.status, InboxStatus::Failed);
    assert!(
        stale_b_rename
            .error
            .as_deref()
            .is_some_and(|error| error.contains("library root identity changed"))
    );
    assert_eq!(queue.registry_snapshot().unwrap(), replacement_registry);
    assert_eq!(
        fs::read(&registry_path).unwrap(),
        replacement_registry_bytes
    );
    assert!(
        !replacement_metadata.exists(),
        "a stale token must not open the replacement library manager"
    );
    assert_eq!(
        fs::read(&replacement_media).unwrap(),
        b"replacement library media"
    );
    let remove_c = run_library_management_test_request(
        &runtime,
        31_037,
        "library_remove",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": replacement_binding,
        }),
    )
    .await;
    assert_eq!(
        remove_c.status,
        InboxStatus::Completed,
        "{:?}",
        remove_c.error
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
async fn file_preview_requires_current_registration_binding_and_confirms_saved_binding_after_restart()
 {
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "file-preview-same-root-binding",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let media = fixture.archive_root.join("clip.mp4");
    let nfo = fixture.archive_root.join("clip.nfo");
    let original_media = b"same-root registration media";
    let original_nfo = b"<movie><title>Original title</title></movie>";
    fs::write(&media, original_media).expect("source media should write");
    fs::write(&nfo, original_nfo).expect("source NFO should write");
    let first_root_identity = crate::safe_fs::RootedFs::new(&fixture.archive_root)
        .expect("first registration root should open")
        .root_identity();
    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));

    let first_binding = library_root_binding(&queue, &fixture.target_library_id);
    let first_item_id =
        scan_file_item_id(&runtime, 32_001, &fixture.target_library_id, "clip.mp4").await;
    let removed_first = run_library_management_test_request(
        &runtime,
        32_002,
        "library_remove",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": first_binding,
        }),
    )
    .await;
    assert_eq!(
        removed_first.status,
        InboxStatus::Completed,
        "{:?}",
        removed_first.error
    );

    let manager_metadata = fixture
        .archive_root
        .join(".telegram-video-downloader-library");
    if manager_metadata.exists() {
        fs::remove_dir_all(&manager_metadata)
            .expect("A's manager metadata should be absent before B's stale previews");
    }
    let registered_second = run_library_management_test_request(
        &runtime,
        32_003,
        "library_register",
        serde_json::json!({
            "name": "Archive B",
            "path": fixture.archive_root,
            "set_default": false,
            "create_directory": false,
        }),
    )
    .await;
    assert_eq!(
        registered_second.status,
        InboxStatus::Completed,
        "{:?}",
        registered_second.error
    );
    let registered_b = queue
        .registry_snapshot()
        .expect("B registration should be readable")
        .libraries
        .into_iter()
        .find(|library| library.id == fixture.target_library_id)
        .expect("B should reuse the path-derived ID");
    let second_binding = registered_b
        .root_binding_token()
        .expect("B should expose a registration token");
    assert_ne!(
        second_binding, first_binding,
        "unregistering and registering the same directory creates a new binding"
    );
    assert_eq!(
        first_root_identity,
        crate::safe_fs::RootedFs::new(&fixture.archive_root)
            .expect("same directory object should still open")
            .root_identity(),
        "A and B use the same directory object"
    );

    let stale_move = run_library_management_test_request(
        &runtime,
        32_004,
        "file_preview",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": first_binding,
            "item_ids": [first_item_id],
            "target_relative_dir": "stale-move",
            "conflict": "skip",
        }),
    )
    .await;
    assert_eq!(stale_move.status, InboxStatus::Failed);
    assert!(
        stale_move
            .error
            .as_deref()
            .is_some_and(|error| error.contains("library root identity changed"))
    );
    let stale_patch = run_library_management_test_request(
        &runtime,
        32_005,
        "file_preview",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": first_binding,
            "item_ids": [first_item_id],
            "metadata_patches": [{ "item_id": first_item_id, "title": "Stale title" }],
        }),
    )
    .await;
    assert_eq!(stale_patch.status, InboxStatus::Failed);
    assert!(
        stale_patch
            .error
            .as_deref()
            .is_some_and(|error| error.contains("library root identity changed"))
    );
    for (seq, payload) in [
        (
            32_006,
            serde_json::json!({
                "library_id": fixture.target_library_id,
                "item_ids": [first_item_id],
                "target_relative_dir": "missing-binding",
            }),
        ),
        (
            32_007,
            serde_json::json!({
                "library_id": fixture.target_library_id,
                "root_binding": "v1:not-a-token",
                "item_ids": [first_item_id],
                "metadata_patches": [{ "item_id": first_item_id, "title": "Invalid binding" }],
            }),
        ),
    ] {
        let invalid =
            run_library_management_test_request(&runtime, seq, "file_preview", payload).await;
        assert_eq!(invalid.status, InboxStatus::Failed);
        assert!(
            invalid
                .error
                .as_deref()
                .is_some_and(|error| error.contains("root_binding"))
        );
    }
    assert!(
        !manager_metadata.exists(),
        "stale and invalid previews must reject before manager initialization"
    );
    assert_eq!(
        snapshot_library_metadata_tree(&fixture.archive_root),
        std::collections::BTreeMap::new()
    );
    assert_eq!(fs::read(&media).unwrap(), original_media);
    assert_eq!(fs::read(&nfo).unwrap(), original_nfo);
    assert!(!fixture.archive_root.join("stale-move/clip.mp4").exists());

    let child = fixture.archive_root.join("ordinary-child");
    fs::create_dir(&child).expect("ordinary child should create");
    fs::remove_dir(&child).expect("ordinary child should remove");
    assert_eq!(
        library_root_binding(&queue, &fixture.target_library_id),
        second_binding
    );
    let second_item_id =
        scan_file_item_id(&runtime, 32_008, &fixture.target_library_id, "clip.mp4").await;
    assert_eq!(
        second_item_id, first_item_id,
        "the same media keeps its stable item ID"
    );
    let valid_preview = run_library_management_test_request(
        &runtime,
        32_009,
        "file_preview",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": second_binding,
            "item_ids": [second_item_id],
            "metadata_patches": [{ "item_id": second_item_id, "title": "Verified B title" }],
        }),
    )
    .await;
    assert_eq!(
        valid_preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        valid_preview.error
    );
    let valid_result = valid_preview.result.expect("B preview should be durable");
    assert_eq!(valid_result["library_id"], fixture.target_library_id);
    assert_eq!(valid_result["root_binding"], second_binding);
    let preview_id = valid_result["preview_id"].as_str().unwrap().to_string();
    let revision = valid_result["revision"].as_str().unwrap().to_string();
    let persisted_result = inbox.record(32_009).unwrap().unwrap().result.unwrap();
    assert_eq!(persisted_result["library_id"], fixture.target_library_id);
    assert_eq!(persisted_result["root_binding"], second_binding);

    dispatcher.abort();
    let _ = dispatcher.await;
    runtime.signal_epoch_fatal();
    wait_for_management_dispatch_idle(&runtime).await;
    drop(runtime);
    drop(inbox);
    drop(queue);

    let reopened_queue =
        Arc::new(QueueManager::open(&fixture.config).expect("queue should reopen"));
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
    let confirmed = run_library_management_test_request(
        &reopened_runtime,
        32_010,
        "file_confirm",
        serde_json::json!({ "preview_id": preview_id, "revision": revision }),
    )
    .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    assert_eq!(
        library_root_binding(&reopened_queue, &fixture.target_library_id),
        second_binding
    );
    assert!(
        fs::read_to_string(&nfo)
            .expect("confirmed B metadata should read")
            .contains("Verified B title")
    );

    reopened_dispatcher.abort();
    let _ = reopened_dispatcher.await;
    reopened_runtime.signal_epoch_fatal();
    wait_for_management_dispatch_idle(&reopened_runtime).await;
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn file_preview_and_saved_confirmation_reject_replaced_root_binding() {
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "file-preview-replaced-root-binding",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let media = fixture.archive_root.join("clip.mp4");
    let nfo = fixture.archive_root.join("clip.nfo");
    fs::write(&media, b"original root media").expect("A media should write");
    fs::write(&nfo, b"<movie><title>Original A</title></movie>").expect("A NFO should write");
    let first_root_identity = crate::safe_fs::RootedFs::new(&fixture.archive_root)
        .expect("A root should open")
        .root_identity();
    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let first_binding = library_root_binding(&queue, &fixture.target_library_id);
    let first_item_id =
        scan_file_item_id(&runtime, 33_001, &fixture.target_library_id, "clip.mp4").await;
    let valid_preview = run_library_management_test_request(
        &runtime,
        33_002,
        "file_preview",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": first_binding,
            "item_ids": [first_item_id],
            "target_relative_dir": "pending-move",
            "conflict": "skip",
        }),
    )
    .await;
    assert_eq!(
        valid_preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        valid_preview.error
    );
    let preview_result = valid_preview.result.expect("A preview should persist");
    assert_eq!(preview_result["library_id"], fixture.target_library_id);
    assert_eq!(preview_result["root_binding"], first_binding);
    let preview_id = preview_result["preview_id"].as_str().unwrap().to_string();
    let revision = preview_result["revision"].as_str().unwrap().to_string();

    let removed_first = run_library_management_test_request(
        &runtime,
        33_003,
        "library_remove",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": first_binding,
        }),
    )
    .await;
    assert_eq!(
        removed_first.status,
        InboxStatus::Completed,
        "{:?}",
        removed_first.error
    );
    let displaced_root = fixture.root.join("archive-displaced");
    fs::rename(&fixture.archive_root, &displaced_root).expect("A root should move aside");
    fs::create_dir(&fixture.archive_root).expect("replacement root should reuse A's path");
    fs::write(&media, b"replacement root media").expect("replacement media should write");
    fs::write(&nfo, b"<movie><title>Replacement C</title></movie>")
        .expect("replacement NFO should write");
    let registered_second = run_library_management_test_request(
        &runtime,
        33_004,
        "library_register",
        serde_json::json!({
            "name": "Archive C",
            "path": fixture.archive_root,
            "set_default": false,
            "create_directory": false,
        }),
    )
    .await;
    assert_eq!(
        registered_second.status,
        InboxStatus::Completed,
        "{:?}",
        registered_second.error
    );
    let registered_c = queue
        .registry_snapshot()
        .unwrap()
        .libraries
        .into_iter()
        .find(|library| library.id == fixture.target_library_id)
        .expect("C should reuse the path-derived library ID");
    let second_binding = registered_c.root_binding_token().unwrap();
    assert_ne!(
        first_binding, second_binding,
        "replacement registration changes the token"
    );
    assert_ne!(
        first_root_identity,
        crate::safe_fs::RootedFs::new(&fixture.archive_root)
            .expect("replacement root should open")
            .root_identity(),
        "replacement root is a different directory object"
    );
    let metadata_root = fixture
        .archive_root
        .join(".telegram-video-downloader-library");
    assert!(
        !metadata_root.exists(),
        "registering C should not create manager metadata"
    );
    let replacement_entries_before = fs::read_dir(&fixture.archive_root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<std::collections::BTreeSet<_>>();
    let registry_before =
        fs::read(crate::media_libraries::registry_path(None, &fixture.video_root).unwrap())
            .unwrap();

    for (seq, payload, expected_error) in [
        (
            33_005,
            serde_json::json!({
                "library_id": fixture.target_library_id,
                "root_binding": first_binding,
                "item_ids": [first_item_id],
                "target_relative_dir": "stale-move",
                "conflict": "skip",
            }),
            "library root identity changed",
        ),
        (
            33_006,
            serde_json::json!({
                "library_id": fixture.target_library_id,
                "root_binding": first_binding,
                "item_ids": [first_item_id],
                "metadata_patches": [{ "item_id": first_item_id, "title": "Stale title" }],
            }),
            "library root identity changed",
        ),
    ] {
        let stale =
            run_library_management_test_request(&runtime, seq, "file_preview", payload).await;
        assert_eq!(stale.status, InboxStatus::Failed);
        assert!(
            stale
                .error
                .as_deref()
                .is_some_and(|error| error.contains(expected_error))
        );
        assert!(
            !metadata_root.exists(),
            "stale preview must reject before manager initialization"
        );
        assert_eq!(fs::read(&media).unwrap(), b"replacement root media");
        assert_eq!(
            fs::read(&nfo).unwrap(),
            b"<movie><title>Replacement C</title></movie>"
        );
        assert!(!fixture.archive_root.join("stale-move/clip.mp4").exists());
    }
    let stale_import = run_library_management_test_request(
        &runtime,
        33_007,
        "legacy_import",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": first_binding,
            "text": "https://www.bilibili.com/video/BV1xx411c7mD",
        }),
    )
    .await;
    assert_eq!(stale_import.status, InboxStatus::Failed);
    assert!(
        stale_import
            .error
            .as_deref()
            .is_some_and(|error| error.contains("library root identity changed"))
    );
    assert!(
        !metadata_root.exists(),
        "stale legacy import must reject before replacement manager initialization"
    );
    assert_eq!(fs::read(&media).unwrap(), b"replacement root media");
    assert_eq!(
        fs::read(&nfo).unwrap(),
        b"<movie><title>Replacement C</title></movie>"
    );
    let projected_state = build_cloud_state_snapshot(&runtime)
        .await
        .expect("state projection should classify the saved A preview without opening B")
        .expect("cloud state should remain available");
    assert!(
        projected_state
            .previews
            .iter()
            .all(|preview| preview["preview_id"] != preview_id),
        "the stale A preview must not be projected as actionable for replacement C"
    );
    assert!(
        !metadata_root.exists(),
        "projecting the saved A preview must not initialize replacement C metadata"
    );
    assert_eq!(fs::read(&media).unwrap(), b"replacement root media");
    assert_eq!(
        fs::read(&nfo).unwrap(),
        b"<movie><title>Replacement C</title></movie>"
    );
    assert_eq!(
        fs::read_dir(&fixture.archive_root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<std::collections::BTreeSet<_>>(),
        replacement_entries_before,
        "stale preview/import should not add replacement-root metadata"
    );
    assert_eq!(
        fs::read(crate::media_libraries::registry_path(None, &fixture.video_root).unwrap())
            .unwrap(),
        registry_before,
        "stale preview should not rewrite the registration"
    );

    dispatcher.abort();
    let _ = dispatcher.await;
    runtime.signal_epoch_fatal();
    drop(runtime);
    drop(inbox);
    drop(queue);

    let reopened_queue =
        Arc::new(QueueManager::open(&fixture.config).expect("queue should reopen"));
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
    let stale_confirm = run_library_management_test_request(
        &reopened_runtime,
        33_008,
        "file_confirm",
        serde_json::json!({ "preview_id": preview_id, "revision": revision }),
    )
    .await;
    assert_eq!(stale_confirm.status, InboxStatus::Failed);
    assert!(
        stale_confirm
            .error
            .as_deref()
            .is_some_and(|error| error.contains("library root identity changed"))
    );
    assert!(
        !metadata_root.exists(),
        "stale confirmation must reject before manager initialization"
    );
    assert_eq!(fs::read(&media).unwrap(), b"replacement root media");
    assert_eq!(
        fs::read(&nfo).unwrap(),
        b"<movie><title>Replacement C</title></movie>"
    );

    let child = fixture.archive_root.join("ordinary-child");
    fs::create_dir(&child).expect("ordinary child should create");
    fs::remove_dir(&child).expect("ordinary child should remove");
    assert_eq!(
        library_root_binding(&reopened_queue, &fixture.target_library_id),
        second_binding
    );
    let replacement_item_id = scan_file_item_id(
        &reopened_runtime,
        33_009,
        &fixture.target_library_id,
        "clip.mp4",
    )
    .await;
    let valid_replacement_preview = run_library_management_test_request(
        &reopened_runtime,
        33_010,
        "file_preview",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": second_binding,
            "item_ids": [replacement_item_id],
            "metadata_patches": [{ "item_id": replacement_item_id, "title": "Verified C title" }],
        }),
    )
    .await;
    assert_eq!(
        valid_replacement_preview.status,
        InboxStatus::AwaitingConfirmation
    );
    assert_eq!(
        valid_replacement_preview.result.as_ref().unwrap()["library_id"],
        fixture.target_library_id
    );
    assert_eq!(
        valid_replacement_preview.result.as_ref().unwrap()["root_binding"],
        second_binding
    );
    let replacement_result = valid_replacement_preview.result.unwrap();
    let confirmed_replacement = run_library_management_test_request(
        &reopened_runtime,
        33_011,
        "file_confirm",
        serde_json::json!({
            "preview_id": replacement_result["preview_id"],
            "revision": replacement_result["revision"],
        }),
    )
    .await;
    assert_eq!(
        confirmed_replacement.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed_replacement.error
    );
    assert!(
        fs::read_to_string(&nfo)
            .expect("confirmed C metadata should read")
            .contains("Verified C title")
    );

    reopened_dispatcher.abort();
    let _ = reopened_dispatcher.await;
    reopened_runtime.signal_epoch_fatal();
    drop(reopened_runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}
