use super::media_library_e2e_tests::{MigrationFixture, create_migration_fixture};
use super::*;

use std::collections::BTreeMap;

fn media_registry_path(fixture: &MigrationFixture) -> PathBuf {
    crate::media_libraries::registry_path(None, &fixture.video_root)
        .expect("fixture registry path should resolve")
}

fn legacy_v1_binding_for_root(library_id: &str, root: &Path) -> String {
    let identity = crate::safe_fs::RootedFs::new(root)
        .expect("registered test root should open")
        .root_identity();
    let registry: serde_json::Value = serde_json::from_slice(
        &fs::read(media_registry_path_for_root(root)).expect("test registry should read"),
    )
    .expect("test registry should parse");
    let nonce = registry["libraries"]
        .as_array()
        .expect("registry should contain libraries")
        .iter()
        .find(|library| library["id"] == library_id)
        .and_then(|library| library.get("registration_nonce"))
        .and_then(serde_json::Value::as_str);

    let mut digest = Sha256::new();
    digest.update(b"telegram-video-downloader/media-library-root-binding/v1\0");
    digest.update((library_id.len() as u64).to_be_bytes());
    digest.update(library_id.as_bytes());
    digest.update(identity.device().to_be_bytes());
    digest.update(identity.inode().to_be_bytes());
    if let Some(nonce) = nonce {
        digest.update(b"registration-incarnation/v1\0");
        digest.update(nonce.as_bytes());
    }
    let token = digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("v1:{token}")
}

fn media_registry_path_for_root(root: &Path) -> PathBuf {
    crate::media_libraries::registry_path(None, root)
        .expect("registry path for test root should resolve")
}

fn persist_stale_legacy_device(fixture: &MigrationFixture, library_id: &str) {
    let identity = crate::safe_fs::RootedFs::new(&fixture.archive_root)
        .expect("registered archive should open")
        .root_identity();
    let path = media_registry_path(fixture);
    let mut registry: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).expect("registry should read"))
            .expect("registry should parse");
    let library = registry["libraries"]
        .as_array_mut()
        .expect("registry should contain libraries")
        .iter_mut()
        .find(|library| library["id"] == library_id)
        .expect("archive should remain registered");
    // Simulate a reboot where the mounted volume reports a different device number while its
    // absolute mount path and inode remain stable. The legacy tuple must not decide availability.
    library["root_device"] = serde_json::json!(identity.device() ^ 1);
    library["root_inode"] = serde_json::json!(identity.inode());
    fs::write(
        path,
        serde_json::to_vec(&registry).expect("stale registry should serialize"),
    )
    .expect("stale legacy tuple should persist in the isolated fixture");
}

fn direct_tree_snapshot(root: &Path) -> BTreeMap<String, Option<Vec<u8>>> {
    let mut snapshot = BTreeMap::new();
    for entry in fs::read_dir(root).expect("test root should be readable") {
        let entry = entry.expect("test root entry should read");
        let name = entry
            .file_name()
            .into_string()
            .expect("test entry name should be UTF-8");
        let kind = entry.file_type().expect("test entry type should read");
        let bytes = if kind.is_file() {
            Some(fs::read(entry.path()).expect("test file should read"))
        } else {
            None
        };
        snapshot.insert(name, bytes);
    }
    snapshot
}

#[test]
fn same_path_restart_rebinds_stale_legacy_tuple_and_preserves_library_and_task_state() {
    let (fixture, queue, inbox) = create_migration_fixture(
        "path-identity-stale-device-reopen",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    fs::create_dir(fixture.archive_root.join("Films"))
        .expect("default category should exist in the fixture");
    let media_bytes = b"path-bound archive media";
    let nfo_bytes = b"<movie><title>Path identity fixture</title></movie>";
    fs::write(fixture.archive_root.join("clip.mp4"), media_bytes)
        .expect("fixture media should write");
    fs::write(fixture.archive_root.join("clip.nfo"), nfo_bytes).expect("fixture NFO should write");

    queue
        .set_default_video_library(&fixture.target_library_id)
        .expect("archive can become the default library");
    queue
        .set_video_library_category(&fixture.target_library_id, Some(Path::new("Films")))
        .expect("archive category should persist");
    let task_id = "path-identity-preserved-task";
    queue
        .create(TaskRecord::new(
            task_id.to_string(),
            51_001,
            51_001,
            515_001,
            Some(515_001),
            0,
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=abcdefghijk".to_string(),
            },
        ))
        .expect("fixture task should persist in the registered archive");
    let original_task = queue
        .get(task_id)
        .expect("fixture task should load")
        .expect("fixture task should exist");
    let original_token = super::cloud_test_root_binding(&queue, &fixture.target_library_id);
    assert!(
        original_token.starts_with("v2:"),
        "new registrations use path tokens"
    );

    persist_stale_legacy_device(&fixture, &fixture.target_library_id);
    drop(inbox);
    drop(queue);

    let reopened = QueueManager::open(&fixture.config)
        .expect("same registered path should rebind after device-number drift");
    let snapshot = reopened
        .registry_snapshot()
        .expect("registered library settings should reopen");
    assert_eq!(snapshot.default_library_id, fixture.target_library_id);
    assert_eq!(
        snapshot.default_category.as_deref(),
        Some(Path::new("Films"))
    );
    let archive = snapshot
        .libraries
        .iter()
        .find(|library| library.id == fixture.target_library_id)
        .expect("archive registration should remain present");
    assert_eq!(archive.name, "Archive");
    assert_eq!(archive.root, fixture.archive_root);
    assert_eq!(
        archive.default_category.as_deref(),
        Some(Path::new("Films"))
    );
    assert!(reopened.video_library_statuses().unwrap()[&fixture.target_library_id].available);
    assert_eq!(
        super::cloud_test_root_binding(&reopened, &fixture.target_library_id),
        original_token,
        "serialized device drift does not change the path-bound token"
    );

    let reopened_task = reopened
        .get(task_id)
        .expect("preserved task should load")
        .expect("task record should remain present");
    assert_eq!(reopened_task.id, original_task.id);
    assert_eq!(reopened_task.original_url, original_task.original_url);
    assert_eq!(reopened_task.destination, original_task.destination);
    assert_eq!(
        reopened_task
            .destination
            .as_ref()
            .map(|destination| destination.library_id.as_str()),
        Some(fixture.target_library_id.as_str())
    );
    assert_eq!(
        reopened_task
            .destination
            .as_ref()
            .and_then(|destination| destination.category.as_deref()),
        Some(Path::new("Films"))
    );
    assert_eq!(
        fs::read(fixture.archive_root.join("clip.mp4")).unwrap(),
        media_bytes
    );
    assert_eq!(
        fs::read(fixture.archive_root.join("clip.nfo")).unwrap(),
        nfo_bytes
    );

    drop(reopened);
    let _ = fs::remove_dir_all(&fixture.root);
}

#[tokio::test]
async fn old_v1_binding_cannot_start_preview_after_same_path_reopen_mock_telegram_e2e() {
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "path-identity-v1-preview-rejection",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let media_path = fixture.archive_root.join("clip.mp4");
    let nfo_path = fixture.archive_root.join("clip.nfo");
    let media_bytes = b"preview-only archive media";
    let nfo_bytes = b"<movie><title>Preview must not apply</title></movie>";
    fs::write(&media_path, media_bytes).expect("fixture media should write");
    fs::write(&nfo_path, nfo_bytes).expect("fixture NFO should write");
    let legacy_v1 = legacy_v1_binding_for_root(&fixture.target_library_id, &fixture.archive_root);
    persist_stale_legacy_device(&fixture, &fixture.target_library_id);
    drop(inbox);
    drop(queue);

    let reopened_queue = Arc::new(
        QueueManager::open(&fixture.config)
            .expect("same path should reopen despite stale legacy device metadata"),
    );
    let current_v2 = super::cloud_test_root_binding(&reopened_queue, &fixture.target_library_id);
    assert!(legacy_v1.starts_with("v1:"));
    assert!(current_v2.starts_with("v2:"));
    assert_ne!(legacy_v1, current_v2);

    // Obtain the same stable item ID the cloud preview accepts without invoking the separate
    // library_scan request, whose legacy PGC backfill may update NFO files.
    let local_snapshot = LibraryManager::open(&fixture.archive_root)
        .expect("fixture library should open")
        .scan(&[])
        .expect("fixture library should scan");
    let item_id = local_snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("fixture video should appear in the scan")
        .id
        .clone();

    let reopened_inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, crate::cloud::TEST_DATABASE_EPOCH)
            .expect("same-epoch inbox should reopen"),
    );
    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&reopened_queue),
        Arc::clone(&reopened_inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let stale = run_library_management_test_request(
        &runtime,
        51_101,
        "file_preview",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": legacy_v1,
            "item_ids": [item_id],
            "target_relative_dir": "stale-preview",
            "conflict": "skip",
        }),
    )
    .await;
    assert_eq!(stale.status, InboxStatus::Failed, "{:?}", stale.error);
    assert!(
        stale.error.as_deref().is_some_and(|error| {
            error.contains("root_binding") || error.contains("root identity changed")
        }),
        "the old token should fail the current root-binding check: {:?}",
        stale.error
    );

    let current = run_library_management_test_request(
        &runtime,
        51_102,
        "file_preview",
        serde_json::json!({
            "library_id": fixture.target_library_id,
            "root_binding": current_v2,
            "item_ids": [item_id],
            "target_relative_dir": "pending-preview",
            "conflict": "skip",
        }),
    )
    .await;
    assert_eq!(
        current.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        current.error
    );
    assert_eq!(current.result.as_ref().unwrap()["root_binding"], current_v2);
    assert_eq!(fs::read(&media_path).unwrap(), media_bytes);
    assert_eq!(fs::read(&nfo_path).unwrap(), nfo_bytes);
    assert!(
        !fixture
            .archive_root
            .join("pending-preview/clip.mp4")
            .exists()
    );
    assert!(!fixture.archive_root.join("stale-preview/clip.mp4").exists());

    dispatcher.abort();
    let _ = dispatcher.await;
    runtime.signal_epoch_fatal();
    drop(runtime);
    drop(reopened_inbox);
    drop(reopened_queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(&fixture.root);
}

#[test]
fn missing_registered_root_stays_offline_without_creating_its_path() {
    let (fixture, queue, inbox) = create_migration_fixture(
        "path-identity-missing-root",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let before_token = super::cloud_test_root_binding(&queue, &fixture.target_library_id);
    drop(inbox);
    drop(queue);
    let parked_root = fixture.root.join("archive-while-offline");
    fs::rename(&fixture.archive_root, &parked_root)
        .expect("fixture root should be absent during the simulated restart");

    let reopened = QueueManager::open(&fixture.config)
        .expect("an unavailable registered library should not block queue startup");
    let snapshot = reopened.registry_snapshot().unwrap();
    let registered = snapshot
        .libraries
        .iter()
        .find(|library| library.id == fixture.target_library_id)
        .expect("missing library registration should remain saved");
    assert_eq!(registered.root, fixture.archive_root);
    assert_eq!(
        super::cloud_test_root_binding(&reopened, &fixture.target_library_id),
        before_token
    );
    assert!(!reopened.video_library_statuses().unwrap()[&fixture.target_library_id].available);
    assert!(
        !fixture.archive_root.exists(),
        "startup must not create a missing root"
    );
    assert!(
        parked_root.is_dir(),
        "the moved fixture root should remain untouched"
    );

    drop(reopened);
    let _ = fs::remove_dir_all(&fixture.root);
}

#[test]
fn active_queue_rejects_physical_replacement_but_accepts_ordinary_child_churn() {
    let (fixture, queue, inbox) = create_migration_fixture(
        "path-identity-active-replacement",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let original_media = b"original queue root media";
    let original_nfo = b"<movie>original root</movie>";
    fs::write(fixture.archive_root.join("clip.mp4"), original_media)
        .expect("original media should write");
    fs::write(fixture.archive_root.join("clip.nfo"), original_nfo)
        .expect("original NFO should write");
    let token = super::cloud_test_root_binding(&queue, &fixture.target_library_id);
    queue
        .verified_video_library_root(&fixture.target_library_id)
        .expect("original active root should verify");

    let child = fixture.archive_root.join("ordinary-child");
    fs::create_dir(&child).expect("ordinary child should create");
    fs::remove_dir(&child).expect("ordinary child should be removable");
    assert_eq!(
        super::cloud_test_root_binding(&queue, &fixture.target_library_id),
        token,
        "child changes do not affect the registered path token"
    );
    queue
        .verified_video_library_root(&fixture.target_library_id)
        .expect("ordinary child churn should not invalidate the active root");

    let displaced_root = fixture.root.join("archive-displaced");
    fs::rename(&fixture.archive_root, &displaced_root)
        .expect("active root should move aside for the replacement check");
    fs::create_dir(&fixture.archive_root).expect("replacement should reuse the registered path");
    fs::write(
        fixture.archive_root.join("replacement.mp4"),
        b"replacement media",
    )
    .expect("replacement media should write");
    fs::write(
        fixture.archive_root.join("replacement.nfo"),
        b"<movie>new root</movie>",
    )
    .expect("replacement NFO should write");
    let replacement_before = direct_tree_snapshot(&fixture.archive_root);
    assert_eq!(
        super::cloud_test_root_binding(&queue, &fixture.target_library_id),
        token,
        "path token remains stable while the active descriptor is stale"
    );
    assert!(
        queue
            .verified_video_library_root(&fixture.target_library_id)
            .is_err(),
        "the active QueueManager must reject a different root object at the same path"
    );
    assert_eq!(
        direct_tree_snapshot(&fixture.archive_root),
        replacement_before
    );
    assert_eq!(
        fs::read(displaced_root.join("clip.mp4")).unwrap(),
        original_media
    );
    assert_eq!(
        fs::read(displaced_root.join("clip.nfo")).unwrap(),
        original_nfo
    );
    assert_eq!(
        fs::read(fixture.archive_root.join("replacement.mp4")).unwrap(),
        b"replacement media"
    );
    assert_eq!(
        fs::read(fixture.archive_root.join("replacement.nfo")).unwrap(),
        b"<movie>new root</movie>"
    );
    assert!(
        !fixture
            .archive_root
            .join(".telegram-video-downloader-library")
            .exists(),
        "rejecting the replacement must not initialize queue metadata in it"
    );

    drop(inbox);
    drop(queue);
    let _ = fs::remove_dir_all(&fixture.root);
}

#[cfg(unix)]
#[test]
fn active_queue_rejects_symlink_replacement_without_touching_its_target() {
    let (fixture, queue, inbox) = create_migration_fixture(
        "path-identity-active-symlink",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let original_media = b"original symlink root media";
    fs::write(fixture.archive_root.join("clip.mp4"), original_media)
        .expect("original media should write");
    let token = super::cloud_test_root_binding(&queue, &fixture.target_library_id);
    queue
        .verified_video_library_root(&fixture.target_library_id)
        .expect("original active root should verify");

    let displaced_root = fixture.root.join("archive-displaced");
    let replacement_target = fixture.root.join("symlink-target");
    fs::rename(&fixture.archive_root, &displaced_root)
        .expect("active root should move aside for the symlink check");
    fs::create_dir(&replacement_target).expect("replacement target should create");
    fs::write(
        replacement_target.join("replacement.mp4"),
        b"symlink target media",
    )
    .expect("replacement media should write");
    fs::write(
        replacement_target.join("replacement.nfo"),
        b"<movie>target</movie>",
    )
    .expect("replacement NFO should write");
    let target_before = direct_tree_snapshot(&replacement_target);
    std::os::unix::fs::symlink(&replacement_target, &fixture.archive_root)
        .expect("symlink should replace the registered path");

    assert_eq!(
        super::cloud_test_root_binding(&queue, &fixture.target_library_id),
        token,
        "path token remains independent from the new path target"
    );
    assert!(
        queue
            .verified_video_library_root(&fixture.target_library_id)
            .is_err(),
        "an active queue must reject a symlink substituted at its registered root"
    );
    assert_eq!(direct_tree_snapshot(&replacement_target), target_before);
    assert!(
        fs::symlink_metadata(&fixture.archive_root)
            .expect("substitute should remain a symlink")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        fs::read(displaced_root.join("clip.mp4")).unwrap(),
        original_media
    );

    drop(inbox);
    drop(queue);
    let _ = fs::remove_dir_all(&fixture.root);
}
