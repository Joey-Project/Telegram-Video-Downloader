use std::fs::{self, FileTimes};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use tempfile::TempDir;

use crate::config::AppConfig;
use crate::queue::{QueueManager, TaskRecord, TaskStatus};
use crate::router::JobRequest;
use crate::staging_cleanup::{CleanupPreview, StagingCleanupBusy, StagingScan};

const CHAT_ID: i64 = 123_456_789;

struct Fixture {
    queue: QueueManager,
    _directory: TempDir,
    root: PathBuf,
    library_id: String,
    binding: String,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("create isolated cleanup fixture");
        let base = fs::canonicalize(directory.path()).expect("resolve fixture root");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = base.join("videos");
        config.downloads.pdf_dir = base.join("pdfs");
        config
            .ensure_runtime_dirs()
            .expect("create fixture media roots");
        let queue = QueueManager::open(&config).expect("open fixture queue");
        let library_id = queue.default_video_destination().unwrap().library_id;
        let binding = crate::cloud_bot_tests::cloud_test_root_binding(&queue, &library_id);
        Self {
            queue,
            _directory: directory,
            root: config.downloads.video_dir,
            library_id,
            binding,
        }
    }

    fn associate(&self, id: &str, stage: &Path, status: TaskStatus) {
        self.associate_for_chat(id, stage, status, CHAT_ID);
    }

    fn associate_for_chat(&self, id: &str, stage: &Path, status: TaskStatus, chat_id: i64) {
        let mut record = TaskRecord::new(
            id.to_string(),
            1,
            2,
            chat_id,
            Some(3),
            0,
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=cleanup-fixture".to_string(),
            },
        );
        record.status = status;
        record.staging_attempts.push(stage.to_path_buf());
        assert!(self.queue.create(record).expect("persist staged task"));
    }

    fn partial(&self, id: &str, status: TaskStatus) -> PathBuf {
        let stage = crate::downloader::create_test_incomplete_video_staging(&self.root)
            .expect("create retained incomplete staging");
        fs::write(stage.join("video.part"), b"partial source bytes").unwrap();
        self.associate(id, &stage, status);
        stage
    }

    fn scan(&self) -> StagingScan {
        with_idle_output(|| {
            self.queue
                .scan_staging_cleanup(&self.library_id, &self.binding, &[CHAT_ID])
        })
        .expect("scan recorded staging")
    }

    fn preview(&self, id: &str) -> CleanupPreview {
        let scan = self.scan();
        let item = scan
            .items
            .iter()
            .find(|item| item.task_id == id)
            .expect("find staged task");
        assert!(item.eligible, "{:?}", item.reason);
        with_idle_output(|| {
            self.queue.preview_staging_cleanup(
                &self.library_id,
                &self.binding,
                &[CHAT_ID],
                std::slice::from_ref(&item.id),
            )
        })
        .expect("preview safe staging cleanup")
    }

    fn assert_rejected(&self, preview: &CleanupPreview) {
        let result = with_idle_output(|| self.queue.confirm_staging_cleanup(preview, &[CHAT_ID]));
        assert!(
            result.is_err() || !result.unwrap().failed.is_empty(),
            "changed staging must be preserved"
        );
    }
}

// Other real-filesystem tests share the production video-output slot. Wait only for its typed
// contention marker, never hide validation failures or retry a destructive outcome.
fn with_idle_output<T>(mut action: impl FnMut() -> Result<T>) -> Result<T> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match action() {
            Err(error) if error.is::<StagingCleanupBusy>() && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            result => return result,
        }
    }
}

#[test]
fn staging_cleanup_completed_unpublished_download_and_unassociated_job_stay_protected() {
    let fixture = Fixture::new();
    let complete = crate::downloader::create_test_retained_video_staging(&fixture.root).unwrap();
    fs::write(complete.join("complete.mp4"), b"completed download").unwrap();
    fixture.associate("completed-unpublished", &complete, TaskStatus::Failed);
    let orphan = crate::downloader::create_test_incomplete_video_staging(&fixture.root).unwrap();
    fs::write(orphan.join("orphan.part"), b"unassociated fragments").unwrap();
    let scan = fixture.scan();
    let row = scan
        .items
        .iter()
        .find(|item| item.task_id == "completed-unpublished")
        .unwrap();
    assert!(!row.eligible);
    assert!(row.reason.as_ref().is_some_and(|reason| !reason.is_empty()));
    assert!(
        with_idle_output(|| fixture.queue.preview_staging_cleanup(
            &fixture.library_id,
            &fixture.binding,
            &[CHAT_ID],
            std::slice::from_ref(&row.id)
        ))
        .is_err()
    );
    assert_eq!(
        fs::read(complete.join("complete.mp4")).unwrap(),
        b"completed download"
    );
    assert_eq!(
        fs::read(orphan.join("orphan.part")).unwrap(),
        b"unassociated fragments"
    );
}

#[test]
fn staging_cleanup_child_added_after_preview_is_preserved() {
    let fixture = Fixture::new();
    let stage = fixture.partial("inventory-changed", TaskStatus::Interrupted);
    let preview = fixture.preview("inventory-changed");
    fs::write(stage.join("new-file.mp4"), b"new unreviewed contents").unwrap();
    fixture.assert_rejected(&preview);
    assert_eq!(
        fs::read(stage.join("video.part")).unwrap(),
        b"partial source bytes"
    );
    assert_eq!(
        fs::read(stage.join("new-file.mp4")).unwrap(),
        b"new unreviewed contents"
    );
}

#[test]
fn staging_cleanup_benign_timestamp_and_temporary_child_churn_do_not_replace_owned_objects() {
    let fixture = Fixture::new();
    let stage = fixture.partial("benign-metadata", TaskStatus::Failed);
    let preview = fixture.preview("benign-metadata");
    let file_birth = fs::metadata(stage.join("video.part"))
        .unwrap()
        .created()
        .unwrap();
    let directory_birth = fs::metadata(&stage).unwrap().created().unwrap();
    // On this filesystem, setting mtime before birth also rewrites birth. A future
    // mtime isolates the cosmetic timestamp change from the protected identity.
    let modified = SystemTime::now() + Duration::from_secs(86_400);
    fs::File::options()
        .write(true)
        .open(stage.join("video.part"))
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();
    fs::write(stage.join("temporary-observer"), b"benign entry churn").unwrap();
    fs::remove_file(stage.join("temporary-observer")).unwrap();
    fs::File::open(&stage)
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();
    assert_eq!(
        fs::metadata(stage.join("video.part"))
            .unwrap()
            .created()
            .unwrap(),
        file_birth
    );
    assert_eq!(
        fs::metadata(&stage).unwrap().created().unwrap(),
        directory_birth
    );
    let result = with_idle_output(|| fixture.queue.confirm_staging_cleanup(&preview, &[CHAT_ID]))
        .expect("benign metadata changes should preserve object identity");
    assert!(result.failed.is_empty(), "{:?}", result.failed);
    assert_eq!(result.removed.len(), 1);
    assert!(!stage.exists());
    assert!(fixture.queue.get("benign-metadata").unwrap().is_some());
}

#[cfg(unix)]
#[test]
fn staging_cleanup_symlink_and_hard_link_cannot_delete_published_media() {
    use std::os::unix::fs::symlink;
    let fixture = Fixture::new();
    let published = fixture.root.join("published.mp4");
    fs::write(&published, b"published media outside staging").unwrap();
    let symlinked = fixture.partial("symlinked", TaskStatus::Failed);
    symlink(&published, symlinked.join("linked.mp4")).unwrap();
    let hard_linked = fixture.partial("hard-linked", TaskStatus::Failed);
    fs::hard_link(&published, hard_linked.join("linked.mp4")).unwrap();
    let scan = fixture.scan();
    for id in ["symlinked", "hard-linked"] {
        let row = scan.items.iter().find(|item| item.task_id == id).unwrap();
        assert!(!row.eligible, "{id} must be protected");
        assert!(row.reason.as_ref().is_some_and(|reason| !reason.is_empty()));
    }
    assert_eq!(
        fs::read(published).unwrap(),
        b"published media outside staging"
    );
}

#[test]
fn staging_cleanup_directory_replacement_after_preview_preserves_both_objects() {
    let fixture = Fixture::new();
    let stage = fixture.partial("replaced-directory", TaskStatus::Failed);
    let preview = fixture.preview("replaced-directory");
    let old_stage = stage.with_file_name("retained-original-directory");
    fs::rename(&stage, &old_stage).unwrap();
    fs::create_dir(&stage).unwrap();
    fs::write(stage.join("unrelated.txt"), b"replacement object").unwrap();
    fixture.assert_rejected(&preview);
    assert_eq!(
        fs::read(stage.join("unrelated.txt")).unwrap(),
        b"replacement object"
    );
    assert_eq!(
        fs::read(old_stage.join("video.part")).unwrap(),
        b"partial source bytes"
    );
}

#[test]
fn staging_cleanup_rejects_unknown_task_ids_root_binding_and_unallowed_chat() {
    let fixture = Fixture::new();
    let stage = fixture.partial("scoped-task", TaskStatus::Failed);
    let preview = fixture.preview("scoped-task");
    assert!(
        with_idle_output(|| fixture.queue.preview_staging_cleanup(
            &fixture.library_id,
            &fixture.binding,
            &[CHAT_ID],
            &["unknown-stage".to_string()]
        ))
        .is_err()
    );
    let wrong_binding = format!("v1:{}", "0".repeat(64));
    assert!(
        fixture
            .queue
            .scan_staging_cleanup(&fixture.library_id, &wrong_binding, &[CHAT_ID])
            .is_err()
    );
    let result = with_idle_output(|| {
        fixture
            .queue
            .confirm_staging_cleanup(&preview, &[CHAT_ID + 1])
    });
    assert!(result.is_err() || !result.unwrap().failed.is_empty());
    assert_eq!(
        fs::read(stage.join("video.part")).unwrap(),
        b"partial source bytes"
    );
}

#[test]
fn staging_cleanup_cancelled_and_partial_tasks_keep_records_after_explicit_cleanup() {
    let fixture = Fixture::new();
    for (id, status) in [
        ("cancelled-stage", TaskStatus::Cancelled),
        ("partial-stage", TaskStatus::PartiallyCompleted),
    ] {
        let stage = fixture.partial(id, status);
        let preview = fixture.preview(id);
        let result =
            with_idle_output(|| fixture.queue.confirm_staging_cleanup(&preview, &[CHAT_ID]))
                .expect("inert unsuccessful task should permit explicit cleanup");
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert!(!stage.exists());
        assert_eq!(fixture.queue.get(id).unwrap().unwrap().status, status);
    }
}

#[test]
fn staging_cleanup_shared_attempt_cannot_be_deleted_through_an_inactive_alias() {
    let fixture = Fixture::new();
    let stage = fixture.partial("inactive-alias", TaskStatus::Failed);
    fixture.associate("active-owner", &stage, TaskStatus::Running);
    let scan = fixture.scan();
    let shared = scan
        .items
        .iter()
        .find(|item| Path::new(&item.relative_path) == stage.strip_prefix(&fixture.root).unwrap())
        .expect("shared directory should remain visible as one protected row");
    assert!(
        !shared.eligible,
        "ambiguous task ownership must stay protected"
    );
    assert!(
        with_idle_output(|| fixture.queue.preview_staging_cleanup(
            &fixture.library_id,
            &fixture.binding,
            &[CHAT_ID],
            std::slice::from_ref(&shared.id)
        ))
        .is_err()
    );
    assert_eq!(
        fs::read(stage.join("video.part")).unwrap(),
        b"partial source bytes"
    );
}

#[test]
fn staging_cleanup_unlisted_chat_alias_is_protected_at_scan_and_confirmation() {
    for alias_after_preview in [false, true] {
        let fixture = Fixture::new();
        let stage = fixture.partial("visible-task", TaskStatus::Failed);
        let preview = alias_after_preview.then(|| fixture.preview("visible-task"));
        fixture.associate_for_chat(
            "private-other-chat",
            &stage,
            TaskStatus::Running,
            CHAT_ID + 1,
        );
        if let Some(preview) = preview {
            fixture.assert_rejected(&preview);
        } else {
            let scan = fixture.scan();
            let visible = scan
                .items
                .iter()
                .find(|item| item.task_id == "visible-task")
                .unwrap();
            assert!(
                !visible.eligible,
                "ownership outside the UI filter still protects the directory"
            );
            assert!(
                !scan
                    .items
                    .iter()
                    .any(|item| item.task_id == "private-other-chat")
            );
        }
        assert_eq!(
            fs::read(stage.join("video.part")).unwrap(),
            b"partial source bytes"
        );
    }
}

#[test]
fn staging_cleanup_multiple_attempts_of_one_task_are_cleaned_together_and_replay_is_noop() {
    let fixture = Fixture::new();
    let first = fixture.partial("multiple-attempts", TaskStatus::Failed);
    let second = crate::downloader::create_test_incomplete_video_staging(&fixture.root).unwrap();
    fs::write(second.join("second.part"), b"another retained attempt").unwrap();
    fixture
        .queue
        .record_staging_path_if_generation("multiple-attempts", 0, second.clone())
        .expect("second attempt should persist")
        .expect("task should remain present");
    let scan = fixture.scan();
    let ids = scan
        .items
        .iter()
        .filter(|item| item.task_id == "multiple-attempts")
        .map(|item| {
            assert!(item.eligible, "{:?}", item.reason);
            item.id.clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 2);
    let preview = with_idle_output(|| {
        fixture.queue.preview_staging_cleanup(
            &fixture.library_id,
            &fixture.binding,
            &[CHAT_ID],
            &ids,
        )
    })
    .unwrap();
    let result =
        with_idle_output(|| fixture.queue.confirm_staging_cleanup(&preview, &[CHAT_ID])).unwrap();
    assert!(result.failed.is_empty(), "{:?}", result.failed);
    assert_eq!(result.removed.len(), 2);
    assert!(!first.exists());
    assert!(!second.exists());
    let replay = with_idle_output(|| fixture.queue.confirm_staging_cleanup(&preview, &[CHAT_ID]))
        .expect("a fully applied cleanup should be safe to repeat after a lost completion receipt");
    assert!(replay.failed.is_empty());
    assert_eq!(replay.bytes, 0);
    assert_eq!(
        fixture
            .queue
            .get("multiple-attempts")
            .unwrap()
            .unwrap()
            .status,
        TaskStatus::Failed
    );
}
