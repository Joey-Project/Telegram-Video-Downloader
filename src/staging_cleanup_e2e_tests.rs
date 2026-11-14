use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::{Value, json};

use crate::cloud_bot_tests::media_library_e2e_tests::create_migration_fixture;
use crate::cloud_bot_tests::{
    cloud_test_root_binding, make_runtime, run_library_management_test_request,
    spawn_fake_telegram_api, stop_fake_telegram_api,
};

use super::*;

fn staged_task(
    queue: &QueueManager,
    root: &Path,
    library_id: &str,
    id: &str,
    status: TaskStatus,
) -> PathBuf {
    let stage = crate::downloader::create_test_incomplete_video_staging(root)
        .expect("partial staging should create");
    fs::write(stage.join("video.part"), b"retained download fragments")
        .expect("partial file should write");
    let mut record = TaskRecord::new(
        id.to_string(),
        1,
        2,
        123_456_789,
        Some(3),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=cleanup-e2e".to_string(),
        },
    );
    record.destination = Some(
        queue
            .video_destination(library_id, None)
            .expect("task destination should bind"),
    );
    record.status = status;
    record.staging_attempts.push(stage.clone());
    assert!(queue.create(record).expect("staged task should persist"));
    stage
}

fn scan_rows(record: &InboxRecord) -> &[Value] {
    record.result.as_ref().expect("scan result should persist")["scan"]["items"]
        .as_array()
        .expect("scan should contain staging rows")
}

fn selected_id(record: &InboxRecord, task_id: &str) -> String {
    let row = scan_rows(record)
        .iter()
        .find(|row| row["task_id"] == task_id)
        .expect("associated staging should be listed");
    assert_eq!(row["eligible"], true, "{row}");
    row["id"]
        .as_str()
        .expect("staging should have ID")
        .to_string()
}

fn preview_reference(record: &InboxRecord, operation: &str) -> Value {
    let result = record
        .result
        .as_ref()
        .expect("preview result should persist");
    json!({
        "operation": operation,
        "library_id": result["library_id"],
        "root_binding": result["root_binding"],
        "preview_seq": record.request.seq,
        "preview_id": result["preview_id"],
        "revision": result["revision"],
    })
}

async fn stop_dispatcher(runtime: &CloudBotRuntime, dispatcher: tokio::task::JoinHandle<()>) {
    dispatcher.abort();
    let _ = dispatcher.await;
    let permit = tokio_timeout(
        Duration::from_secs(5),
        runtime.management_dispatch.acquire(),
    )
    .await
    .expect("management request should drain")
    .expect("management admission should stay open");
    drop(permit);
}

#[tokio::test]
async fn staging_cleanup_scan_preview_and_cancel_preserve_files_and_queue_e2e() {
    let (telegram, mut telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) =
        create_migration_fixture("staging-cleanup-cancel", crate::cloud::TEST_DATABASE_EPOCH);
    let failed = staged_task(
        &queue,
        &fixture.archive_root,
        &fixture.target_library_id,
        "cleanup-failed",
        TaskStatus::Failed,
    );
    let active = staged_task(
        &queue,
        &fixture.archive_root,
        &fixture.target_library_id,
        "cleanup-running",
        TaskStatus::Running,
    );
    let completed = crate::downloader::create_test_retained_video_staging(&fixture.archive_root)
        .expect("completed unpublished staging should create");
    fs::write(completed.join("complete.mp4"), b"downloaded complete media")
        .expect("completed unpublished output should write");
    fs::write(
        fixture.archive_root.join("published.mp4"),
        b"published output",
    )
    .expect("published media should write");
    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let binding = cloud_test_root_binding(&queue, &fixture.target_library_id);
    let scan = run_library_management_test_request(
        &runtime,
        1,
        "staging_cleanup",
        json!({
            "operation": "scan", "library_id": fixture.target_library_id, "root_binding": binding,
        }),
    )
    .await;
    assert_eq!(scan.status, InboxStatus::Completed, "{:?}", scan.error);
    let id = selected_id(&scan, "cleanup-failed");
    let active_row = scan_rows(&scan)
        .iter()
        .find(|row| row["task_id"] == "cleanup-running")
        .expect("active staging should be visible with a reason");
    assert_eq!(active_row["eligible"], false);
    assert!(
        active_row["reason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty())
    );
    let preview = run_library_management_test_request(
        &runtime,
        2,
        "staging_cleanup",
        json!({
            "operation": "preview", "library_id": fixture.target_library_id,
            "root_binding": binding, "item_ids": [id],
        }),
    )
    .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    assert_eq!(
        fs::read(failed.join("video.part")).unwrap(),
        b"retained download fragments"
    );
    let snapshot = build_cloud_state_snapshot(&runtime)
        .await
        .expect("cloud state should build")
        .expect("cloud state should exist");
    let public_preview = snapshot
        .previews
        .iter()
        .find(|entry| entry["seq"] == 2)
        .expect("cleanup preview should be discoverable after reload");
    assert_eq!(public_preview["kind"], "staging_cleanup");
    let public_json = serde_json::to_string(&(snapshot.previews, snapshot.operations)).unwrap();
    for private_field in ["cleanup_preview", "inventory_sha256", "owner_identity"] {
        assert!(
            !public_json.contains(private_field),
            "local cleanup proof must not reach the cloud"
        );
    }
    let cancel_payload = preview_reference(&preview, "cancel");
    let offline_root = fixture.root.join("archive-offline");
    fs::rename(&fixture.archive_root, &offline_root).expect("secondary library should go offline");
    let cancel =
        run_library_management_test_request(&runtime, 3, "staging_cleanup", cancel_payload).await;
    assert_eq!(cancel.status, InboxStatus::Completed, "{:?}", cancel.error);
    fs::rename(&offline_root, &fixture.archive_root).expect("secondary library should reconnect");
    let confirm = run_library_management_test_request(
        &runtime,
        4,
        "staging_cleanup",
        preview_reference(&preview, "confirm"),
    )
    .await;
    assert!(matches!(
        confirm.status,
        InboxStatus::Failed | InboxStatus::Stale
    ));
    assert!(failed.join("video.part").exists());
    assert!(active.join("video.part").exists());
    assert!(completed.join("complete.mp4").exists());
    assert_eq!(
        fs::read(fixture.archive_root.join("published.mp4")).unwrap(),
        b"published output"
    );
    assert!(
        queue
            .get("cleanup-failed")
            .expect("task should load")
            .is_some()
    );
    stop_dispatcher(&runtime, dispatcher).await;
    assert!(
        crate::cloud_bot_tests::take_cloud_test_telegram_requests(&mut telegram_requests)
            .is_empty()
    );
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    fs::remove_dir_all(&fixture.root).expect("synthetic library fixture should clean up");
}

#[tokio::test]
async fn staging_cleanup_confirm_after_restart_removes_only_reviewed_partial_files_e2e() {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) =
        create_migration_fixture("staging-cleanup-restart", crate::cloud::TEST_DATABASE_EPOCH);
    let selected = staged_task(
        &queue,
        &fixture.video_root,
        &fixture.source_library_id,
        "cleanup-selected",
        TaskStatus::Interrupted,
    );
    let retained = staged_task(
        &queue,
        &fixture.video_root,
        &fixture.source_library_id,
        "cleanup-unselected",
        TaskStatus::Failed,
    );
    let owner_lock = fixture
        .video_root
        .join(".telegram-video-downloader-queue/owner.lock");
    #[cfg(unix)]
    let owner_identity = {
        let metadata = fs::metadata(&owner_lock).unwrap();
        (metadata.dev(), metadata.ino())
    };
    let runtime = make_runtime(
        telegram.clone(),
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let binding = cloud_test_root_binding(&queue, &fixture.source_library_id);
    let scan = run_library_management_test_request(
        &runtime,
        1,
        "staging_cleanup",
        json!({
            "operation": "scan", "library_id": fixture.source_library_id, "root_binding": binding,
        }),
    )
    .await;
    assert_eq!(scan.status, InboxStatus::Completed, "{:?}", scan.error);
    let preview = run_library_management_test_request(
        &runtime,
        2,
        "staging_cleanup",
        json!({
            "operation": "preview", "library_id": fixture.source_library_id,
            "root_binding": binding, "item_ids": [selected_id(&scan, "cleanup-selected")],
        }),
    )
    .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let confirm_payload = preview_reference(&preview, "confirm");
    stop_dispatcher(&runtime, dispatcher).await;
    drop(runtime);
    drop(queue);
    drop(inbox);

    let queue = Arc::new(QueueManager::open(&fixture.config).expect("queue should reopen"));
    let inbox = Arc::new(
        CloudInbox::open_for_epoch(&fixture.video_root, crate::cloud::TEST_DATABASE_EPOCH)
            .expect("inbox should reopen"),
    );
    let runtime = make_runtime(telegram, fixture.config.clone(), Arc::clone(&queue), inbox);
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let confirm = run_library_management_test_request(
        &runtime,
        3,
        "staging_cleanup",
        confirm_payload.clone(),
    )
    .await;
    assert_eq!(
        confirm.status,
        InboxStatus::Completed,
        "{:?}",
        confirm.error
    );
    assert!(
        !selected.exists(),
        "confirmed partial directory should be removed"
    );
    assert_eq!(
        fs::read(retained.join("video.part")).unwrap(),
        b"retained download fragments"
    );
    assert!(
        queue
            .get("cleanup-selected")
            .expect("cleaned task should load")
            .is_some()
    );
    assert!(
        queue
            .get("cleanup-unselected")
            .expect("retained task should load")
            .is_some()
    );
    #[cfg(unix)]
    {
        let metadata = fs::metadata(&owner_lock).unwrap();
        assert_eq!((metadata.dev(), metadata.ino()), owner_identity);
    }
    let replay =
        run_library_management_test_request(&runtime, 4, "staging_cleanup", confirm_payload).await;
    assert!(matches!(
        replay.status,
        InboxStatus::Completed | InboxStatus::Failed | InboxStatus::Stale
    ));
    assert_eq!(
        fs::read(retained.join("video.part")).unwrap(),
        b"retained download fragments"
    );
    stop_dispatcher(&runtime, dispatcher).await;
    drop(runtime);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    fs::remove_dir_all(&fixture.root).expect("synthetic restarted library fixture should clean up");
}

#[tokio::test]
async fn staging_cleanup_task_resumed_after_preview_is_protected_e2e() {
    let (telegram, _telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) =
        create_migration_fixture("staging-cleanup-resumed", crate::cloud::TEST_DATABASE_EPOCH);
    let selected = staged_task(
        &queue,
        &fixture.video_root,
        &fixture.source_library_id,
        "cleanup-resumed",
        TaskStatus::Interrupted,
    );
    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let binding = cloud_test_root_binding(&queue, &fixture.source_library_id);
    let scan = run_library_management_test_request(
        &runtime,
        1,
        "staging_cleanup",
        json!({
            "operation": "scan", "library_id": fixture.source_library_id, "root_binding": binding,
        }),
    )
    .await;
    assert_eq!(scan.status, InboxStatus::Completed, "{:?}", scan.error);
    let preview = run_library_management_test_request(
        &runtime,
        2,
        "staging_cleanup",
        json!({
            "operation": "preview", "library_id": fixture.source_library_id,
            "root_binding": binding, "item_ids": [selected_id(&scan, "cleanup-resumed")],
        }),
    )
    .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    queue
        .set_status("cleanup-resumed", TaskStatus::Preparing, None)
        .expect("task should resume preparation");
    let confirm = run_library_management_test_request(
        &runtime,
        3,
        "staging_cleanup",
        preview_reference(&preview, "confirm"),
    )
    .await;
    assert_eq!(
        confirm.status,
        InboxStatus::Failed,
        "stale preview must not delete a resumed task: {:?}",
        confirm.result
    );
    assert_eq!(
        fs::read(selected.join("video.part")).unwrap(),
        b"retained download fragments"
    );
    assert_eq!(
        queue.get("cleanup-resumed").unwrap().unwrap().status,
        TaskStatus::Preparing
    );
    stop_dispatcher(&runtime, dispatcher).await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    fs::remove_dir_all(&fixture.root).expect("synthetic resumed library fixture should clean up");
}
