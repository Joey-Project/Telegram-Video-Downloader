use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::file_provider::MockQueueFileProvider;
use sha2::{Digest, Sha256};

use super::*;

const MEDIA_BYTES: &[u8] = b"synthetic migration media payload";

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "telegram-video-downloader-migration-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("synthetic migration root should create");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    _source_root: TestRoot,
    _target_root: TestRoot,
    source_path: PathBuf,
    target_root_path: PathBuf,
    engine: MigrationEngine,
    migration_id: String,
}

impl Fixture {
    fn new(label: &str) -> Self {
        Self::with_default_update(label, false)
    }

    fn with_default_update(label: &str, set_target_default_on_success: bool) -> Self {
        let source_root = TestRoot::new(&format!("{label}-source"));
        let target_root = TestRoot::new(&format!("{label}-target"));
        let source_path = source_root.path().join("clip.mp4");
        fs::write(&source_path, MEDIA_BYTES).expect("synthetic media should write");
        let source = Arc::new(
            LibraryManager::open_with_provider(
                source_root.path(),
                Arc::new(MockQueueFileProvider::default()),
            )
            .expect("source library should open"),
        );
        let target = Arc::new(
            LibraryManager::open_with_provider(
                target_root.path(),
                Arc::new(MockQueueFileProvider::default()),
            )
            .expect("target library should open"),
        );
        let engine = MigrationEngine::new("source-library", source, "target-library", target)
            .expect("migration engine should open");
        let snapshot = engine
            .source
            .migration_scan()
            .expect("source library should scan");
        let item_id = snapshot
            .items
            .iter()
            .find(|item| item.relative_path == "clip.mp4" && item.presence == Presence::Present)
            .expect("synthetic media should be indexed")
            .id
            .clone();
        let preview = engine
            .preview(&MigrationRequest {
                scope: MigrationScope::Selected,
                item_ids: vec![item_id],
                conflict: ConflictPolicy::Skip,
                item_conflicts: BTreeMap::new(),
                include_incomplete_tasks: false,
                set_target_default_on_success,
                task_impacts: Vec::new(),
                warnings: Vec::new(),
            })
            .expect("preview should be created");
        let migration_id = preview.id.clone();
        if set_target_default_on_success {
            engine
                .confirm_with_default_revision(&preview.id, &preview.revision, Some(17))
                .expect("preview should confirm with its registry revision");
        } else {
            engine
                .confirm(&preview.id, &preview.revision)
                .expect("preview should confirm");
        }
        Self {
            _source_root: source_root,
            _target_root: target_root,
            source_path,
            target_root_path: engine.target.migration_root_path().to_path_buf(),
            engine,
            migration_id,
        }
    }

    fn target_path(&self) -> PathBuf {
        let job = self
            .engine
            .load_job(&self.migration_id)
            .expect("migration state should load");
        self.target_root_path
            .join(&job.groups[0].files[0].plan.target_path)
    }

    fn drive_to_source_cleanup(&self) -> JobDocument {
        for _ in 0..32 {
            let job = self
                .engine
                .load_job(&self.migration_id)
                .expect("migration state should load");
            if job.groups[0].status == MigrationItemPhase::CleaningSource {
                return job;
            }
            self.engine
                .step(&self.migration_id)
                .expect("migration step should complete");
        }
        panic!("migration did not reach source cleanup");
    }

    fn drive_to_completion(&self) -> MigrationStatus {
        for _ in 0..40 {
            let status = self
                .engine
                .status(&self.migration_id)
                .expect("migration status should load")
                .expect("migration should exist");
            if status.status == MigrationRunStatus::Completed {
                return status;
            }
            assert!(!matches!(
                status.status,
                MigrationRunStatus::Partial | MigrationRunStatus::Failed
            ));
            self.engine
                .step(&self.migration_id)
                .expect("migration step should complete");
        }
        panic!("migration did not complete");
    }

    fn assert_source_retained_after_target_mutation(&self, mutate: impl FnOnce(&Path)) {
        self.drive_to_source_cleanup();
        mutate(&self.target_path());
        for _ in 0..3 {
            self.engine
                .step(&self.migration_id)
                .expect("migration step should persist its failure state");
        }
        let job = self
            .engine
            .load_job(&self.migration_id)
            .expect("migration state should load");
        assert_eq!(job.groups[0].status, MigrationItemPhase::Failed);
        assert!(self.source_path.exists(), "source must remain recoverable");
    }

    fn assert_source_retained_after_source_mutation(&self, mutate: impl FnOnce(&Path)) {
        self.drive_to_source_cleanup();
        mutate(&self.source_path);
        for _ in 0..3 {
            self.engine
                .step(&self.migration_id)
                .expect("migration step should persist its failure state");
        }
        let job = self
            .engine
            .load_job(&self.migration_id)
            .expect("migration state should load");
        assert_eq!(job.groups[0].status, MigrationItemPhase::Failed);
        assert!(
            self.source_path.exists(),
            "changed source must remain present"
        );
    }
}

fn replace_with_same_bytes(path: &Path) {
    let replacement = path.with_extension("replacement");
    fs::write(&replacement, MEDIA_BYTES).expect("replacement file should write");
    let old_identity = fs::metadata(path).expect("old file should stat").ino();
    let new_identity = fs::metadata(&replacement)
        .expect("replacement should stat")
        .ino();
    assert_ne!(old_identity, new_identity, "fixture must use a new inode");
    fs::remove_file(path).expect("old file should unlink");
    fs::rename(replacement, path).expect("replacement should install");
}

#[test]
fn published_target_inode_replacement_keeps_source() {
    let fixture = Fixture::new("target-replacement");
    fixture.assert_source_retained_after_target_mutation(replace_with_same_bytes);
}

#[test]
fn published_target_content_or_mode_change_keeps_source() {
    let content_fixture = Fixture::new("target-content-change");
    content_fixture.assert_source_retained_after_target_mutation(|path| {
        fs::write(path, b"changed target content").expect("target content should change");
    });

    let mode_fixture = Fixture::new("target-mode-change");
    mode_fixture.assert_source_retained_after_target_mutation(|path| {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .expect("target mode should change");
    });
}

#[test]
fn source_inode_content_or_access_change_keeps_source() {
    let inode_fixture = Fixture::new("source-replacement");
    inode_fixture.assert_source_retained_after_source_mutation(replace_with_same_bytes);

    let content_fixture = Fixture::new("source-content-change");
    content_fixture.assert_source_retained_after_source_mutation(|path| {
        fs::write(path, b"changed source content").expect("source content should change");
    });

    let access_fixture = Fixture::new("source-mode-change");
    access_fixture.assert_source_retained_after_source_mutation(|path| {
        let current_mode = fs::metadata(path)
            .expect("source should stat")
            .permissions()
            .mode();
        let mode = if current_mode & 0o777 == 0o600 {
            0o644
        } else {
            0o600
        };
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
            .expect("source mode should change");
    });
}

#[test]
fn unrelated_source_directory_entry_churn_does_not_block_cleanup() {
    let fixture = Fixture::new("benign-directory-churn");
    fixture.drive_to_source_cleanup();
    let unrelated = fixture.source_path.with_file_name("unrelated.txt");
    fs::write(&unrelated, b"unrelated directory entry").expect("sibling should write");
    let status = fixture.drive_to_completion();
    assert_eq!(status.success_count, 1);
    assert!(!fixture.source_path.exists());
    assert!(unrelated.exists());
}

#[test]
fn uncheckpointed_orphan_stage_is_preserved_and_retried_at_a_fresh_name() {
    let fixture = Fixture::new("orphan-stage");
    let job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("migration state should load");
    let staging_name = job.groups[0].files[0].staging_name.clone();
    let staging_path = fixture
        .target_root_path
        .join(".telegram-video-downloader-library")
        .join(format!("{staging_name}.partial"));
    fixture
        .engine
        .target
        .migration_create_staging_file(&staging_name)
        .expect("orphan stage should create");
    fs::write(&staging_path, b"uncheckpointed orphan").expect("orphan stage content should write");

    fixture
        .engine
        .step(&fixture.migration_id)
        .expect("orphan detection should advance checkpoint");
    let job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("migration state should load");
    assert_eq!(job.groups[0].files[0].staging_attempt, 1);
    let status = fixture.drive_to_completion();
    assert_eq!(status.success_count, 1);
    assert_eq!(
        fs::read(&staging_path).expect("orphan should remain"),
        b"uncheckpointed orphan"
    );
    assert!(fixture.target_path().exists());
}

#[test]
fn refreshing_preview_durably_supersedes_the_old_revision() {
    let source_root = TestRoot::new("superseded-preview-source");
    let target_root = TestRoot::new("superseded-preview-target");
    let source_path = source_root.path().join("clip.mp4");
    fs::write(&source_path, MEDIA_BYTES).expect("synthetic media should write");
    let source = Arc::new(
        LibraryManager::open_with_provider(
            source_root.path(),
            Arc::new(MockQueueFileProvider::default()),
        )
        .expect("source library should open"),
    );
    let target = Arc::new(
        LibraryManager::open_with_provider(
            target_root.path(),
            Arc::new(MockQueueFileProvider::default()),
        )
        .expect("target library should open"),
    );
    let engine = MigrationEngine::new("source-library", source, "target-library", target)
        .expect("migration engine should open");
    let snapshot = engine
        .source
        .migration_scan()
        .expect("source library should scan");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4" && item.presence == Presence::Present)
        .expect("synthetic media should be indexed")
        .id
        .clone();
    let request = MigrationRequest {
        scope: MigrationScope::Selected,
        item_ids: vec![item_id],
        conflict: ConflictPolicy::Skip,
        item_conflicts: BTreeMap::new(),
        include_incomplete_tasks: false,
        set_target_default_on_success: false,
        task_impacts: Vec::new(),
        warnings: Vec::new(),
    };
    let old = engine
        .preview(&request)
        .expect("first preview should be created");

    let mut invalid_refresh = request.clone();
    invalid_refresh.item_ids = vec!["missing-item".to_string()];
    assert!(
        engine
            .preview_replacing(&invalid_refresh, &old.id, &old.revision)
            .is_err()
    );
    let mut old_document: PreviewDocument = decode_state(
        &engine
            .source
            .migration_read_state(&preview_state_name(&old.id), MAX_STATE_BYTES)
            .expect("old preview should remain readable")
            .expect("old preview should remain persisted"),
        "migration preview",
    )
    .expect("old preview should decode");
    old_document.superseded_by = Some("missing-refresh-document".to_string());
    engine
        .source
        .migration_write_state(
            &preview_state_name(&old.id),
            &encode_state(&old_document).unwrap(),
        )
        .expect("simulated crash tombstone should persist");
    let refreshed = engine
        .preview_replacing(&request, &old.id, &old.revision)
        .expect("a retry should recover from a tombstone whose new document was not saved");
    assert_ne!(refreshed.id, old.id);
    let replayed = engine
        .preview_replacing(&request, &old.id, &old.revision)
        .expect("an inbox replay should reuse the exact persisted refresh receipt");
    assert_eq!(replayed.id, refreshed.id);
    let stale = engine
        .confirm(&old.id, &old.revision)
        .expect_err("the superseded preview must no longer confirm");
    assert!(stale.to_string().contains("superseded"));
    assert_eq!(
        engine
            .confirm(&refreshed.id, &refreshed.revision)
            .expect("the refreshed preview should remain confirmable")
            .status,
        MigrationRunStatus::Queued
    );
}

#[test]
fn default_library_revision_latches_once_for_the_migration_run() {
    let fixture = Fixture::with_default_update("default-revision-latch", true);
    let revision = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("migration job should load")
        .preview
        .revision;
    fixture
        .engine
        .confirm_with_default_revision(&fixture.migration_id, &revision, Some(29))
        .expect("duplicate confirmation should retain the original CAS baseline");
    assert_eq!(
        fixture
            .engine
            .default_update_expected_revision(&fixture.migration_id)
            .expect("saved CAS revision should load"),
        Some(17)
    );
}

#[test]
fn publication_rename_before_checkpoint_recovers_only_the_captured_inode() {
    let fixture = Fixture::new("publish-before-save");
    for _ in 0..16 {
        let job = fixture
            .engine
            .load_job(&fixture.migration_id)
            .expect("migration state should load");
        if job.groups[0].files[0].target_access_policy.is_some() {
            break;
        }
        fixture
            .engine
            .step(&fixture.migration_id)
            .expect("migration step should complete");
    }
    let mut unsaved = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("migration state should load");
    assert!(unsaved.groups[0].files[0].target_identity.is_some());
    fixture
        .engine
        .step_group(&mut unsaved.groups[0], false)
        .expect("publication should rename the verified staging inode");
    assert!(unsaved.groups[0].files[0].published);
    drop(unsaved); // Simulate process loss before the job journal is rewritten.

    let persisted = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("last durable checkpoint should load");
    assert!(!persisted.groups[0].files[0].published);
    let expected_target_identity = persisted.groups[0].files[0].target_identity;
    assert_eq!(
        fs::metadata(fixture.target_path())
            .expect("renamed target should exist")
            .ino(),
        expected_target_identity
            .expect("target identity checkpoint should exist")
            .inode
    );
    fixture
        .engine
        .step(&fixture.migration_id)
        .expect("restart should recognize the exact published inode");
    let recovered = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("recovered migration state should load");
    assert!(recovered.groups[0].files[0].published);
    assert!(fixture.source_path.exists());
}

#[test]
fn source_unlink_after_durable_intent_reconciles_after_restart() {
    let fixture = Fixture::new("unlink-before-save");
    fixture.drive_to_source_cleanup();
    fixture
        .engine
        .step(&fixture.migration_id)
        .expect("cleanup intent should persist");
    let unsaved = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("cleanup intent checkpoint should load");
    let file = &unsaved.groups[0].files[0];
    assert!(file.source_cleanup_intent);
    fixture
        .engine
        .source
        .migration_remove_source(&file.plan.source_path, &file.plan.proof)
        .expect("exact source should remove after durable intent");
    drop(unsaved); // Simulate process loss before source_removed is saved.

    fixture
        .engine
        .step(&fixture.migration_id)
        .expect("restart should reconcile the exact missing source");
    let reconciled = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("reconciled migration state should load");
    assert!(reconciled.groups[0].files[0].source_removed);
    assert_eq!(
        fixture.drive_to_completion().status,
        MigrationRunStatus::Completed
    );
}

#[test]
fn pause_and_resume_after_publication_keeps_the_saved_target_proof() {
    let fixture = Fixture::new("pause-resume-published");
    fixture.drive_to_source_cleanup();
    fixture
        .engine
        .control(&fixture.migration_id, MigrationControl::Pause)
        .expect("published migration should pause");
    fixture
        .engine
        .control(&fixture.migration_id, MigrationControl::Resume)
        .expect("published migration should resume");
    assert_eq!(fixture.drive_to_completion().success_count, 1);
    assert!(!fixture.source_path.exists());
}

#[test]
fn source_only_pause_persists_when_target_root_is_unavailable() {
    let fixture = Fixture::new("source-only-pause");
    let context = MigrationRuntimeContext {
        database_epoch: "synthetic-epoch".to_string(),
        preview_seq: 41,
        chat_id: Some(1001),
        message_id: Some(2002),
        delivery: MigrationDeliveryState::Sent,
    };
    fixture
        .engine
        .set_runtime_context(&fixture.migration_id, context.clone())
        .expect("runtime context should persist");

    let configured_target = fixture._target_root.path().to_path_buf();
    let unavailable_target = configured_target.with_extension("offline");
    fs::rename(&configured_target, &unavailable_target)
        .expect("target library should be temporarily moved out of its configured path");
    let reopen = LibraryManager::open_with_provider(
        &configured_target,
        Arc::new(MockQueueFileProvider::default()),
    );
    assert!(reopen.is_err(), "offline target should not reopen");

    let (status, saved_context) = MigrationEngine::pause_for_source(
        &fixture.engine.source,
        &fixture.migration_id,
        "target root unavailable during migration",
    )
    .expect("source-only pause should persist without opening the target");
    assert_eq!(status.status, MigrationRunStatus::Paused);
    assert_eq!(saved_context, Some(context));
    assert!(
        status
            .warnings
            .iter()
            .any(|warning| warning.contains("unavailable"))
    );

    fs::rename(&unavailable_target, &configured_target)
        .expect("target library should be restored for fixture cleanup");
    assert_eq!(
        fixture
            .engine
            .status(&fixture.migration_id)
            .expect("migration should load after target restore")
            .expect("migration should exist")
            .status,
        MigrationRunStatus::Paused
    );
}

#[test]
fn publication_conflict_keeps_stage_private_and_retry_finishes_after_removal() {
    let fixture = Fixture::new("publish-conflict-retry");
    for _ in 0..16 {
        let job = fixture
            .engine
            .load_job(&fixture.migration_id)
            .expect("migration state should load");
        if job.groups[0].files[0].target_access_policy.is_some() {
            break;
        }
        fixture
            .engine
            .step(&fixture.migration_id)
            .expect("migration step should complete");
    }
    let job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("migration state should load");
    let staging_name = job.groups[0].files[0].staging_name.clone();
    let staging_path = fixture
        .target_root_path
        .join(".telegram-video-downloader-library")
        .join(format!("{staging_name}.partial"));
    let target_path = fixture.target_path();
    fs::write(&target_path, b"raced target conflict")
        .expect("target conflict should appear before publication");
    fixture
        .engine
        .step(&fixture.migration_id)
        .expect("conflict should be detected without replacing the target");
    fixture
        .engine
        .step(&fixture.migration_id)
        .expect("failed group should reach a retryable terminal state");
    let failed = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("failed migration should load");
    assert_eq!(failed.groups[0].status, MigrationItemPhase::Failed);
    assert_eq!(
        fs::metadata(&staging_path)
            .expect("private stage should remain after conflict")
            .permissions()
            .mode()
            & 0o777,
        0o600,
        "a failed no-replace publication must leave the stage private"
    );
    assert!(
        fixture.source_path.exists(),
        "source must remain during conflict"
    );

    fs::remove_file(&target_path).expect("fixture target conflict should be removed");
    fixture
        .engine
        .control(&fixture.migration_id, MigrationControl::RetryFailed)
        .expect("failed publication should be retryable");
    assert_eq!(fixture.drive_to_completion().success_count, 1);
    assert!(!fixture.source_path.exists());
    assert_eq!(
        fs::read(target_path).expect("published target should read"),
        MEDIA_BYTES
    );
}

#[test]
fn handoff_attention_moves_stopped_job_to_partial_without_removing_source() {
    let fixture = Fixture::new("handoff-attention-stopped");
    let stopped = fixture
        .engine
        .control(&fixture.migration_id, MigrationControl::StopRemaining)
        .expect("stop should be durable before handoff recovery");
    assert_eq!(stopped.status, MigrationRunStatus::Stopped);

    let attention = fixture
        .engine
        .mark_task_handoff_migration_attention(
            &fixture.migration_id,
            "queue acknowledgment cannot be proven after restart",
        )
        .expect("ambiguous handoff should be persisted for explicit attention");
    assert_eq!(attention.status, MigrationRunStatus::Partial);
    assert!(
        fixture.source_path.exists(),
        "uncertain source must be preserved"
    );
    assert!(
        attention
            .warnings
            .iter()
            .any(|warning| warning.contains("cannot be proven"))
    );

    let stopped_job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("stopped migration should remain durable");
    assert!(stopped_job.stop_remaining_latched);
    assert_eq!(stopped_job.groups[0].status, MigrationItemPhase::Skipped);
    let retried = fixture
        .engine
        .control(&fixture.migration_id, MigrationControl::RetryFailed)
        .expect("explicit retry should enter terminal handoff recovery only");
    assert_eq!(retried.status, MigrationRunStatus::Stopped);
    let after_retry = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("retry checkpoint should remain durable");
    assert_eq!(after_retry.groups[0].status, MigrationItemPhase::Skipped);
    assert!(
        fixture.source_path.exists(),
        "retry must not move stopped media"
    );
    assert_eq!(
        fixture
            .engine
            .step(&fixture.migration_id)
            .expect("Stopped is terminal to media execution")
            .status,
        MigrationRunStatus::Stopped
    );
}

#[test]
fn handoff_attention_moves_completed_job_to_partial() {
    let fixture = Fixture::new("handoff-attention-completed");
    assert_eq!(
        fixture.drive_to_completion().status,
        MigrationRunStatus::Completed
    );

    let attention = fixture
        .engine
        .mark_task_handoff_migration_attention(
            &fixture.migration_id,
            "legacy task handoff is missing its exact queue acknowledgment",
        )
        .expect("legacy handoff ambiguity should be persisted for explicit attention");
    assert_eq!(attention.status, MigrationRunStatus::Partial);
    assert!(
        attention
            .warnings
            .iter()
            .any(|warning| warning.contains("missing its exact queue acknowledgment"))
    );
}

struct TaskCleanupFixture {
    _source_root: TestRoot,
    _target_root: TestRoot,
    staging_path: PathBuf,
    payload_path: PathBuf,
    engine: MigrationEngine,
    migration_id: String,
    transfer_id: String,
    payload_index: usize,
    directory_index: usize,
}

impl TaskCleanupFixture {
    fn new(label: &str) -> Self {
        let source_root = TestRoot::new(&format!("{label}-source"));
        let target_root = TestRoot::new(&format!("{label}-target"));
        let staging_path = fs::canonicalize(
            crate::downloader::create_test_retained_video_staging(source_root.path())
                .expect("retained owned staging directory should create"),
        )
        .expect("retained staging path should canonicalize");
        let payload_path = staging_path.join("segment.part");
        fs::write(&payload_path, b"synthetic retained fragment")
            .expect("retained fragment should write");

        let source = Arc::new(
            LibraryManager::open_with_provider(
                source_root.path(),
                Arc::new(MockQueueFileProvider::default()),
            )
            .expect("source library should open"),
        );
        let target = Arc::new(
            LibraryManager::open_with_provider(
                target_root.path(),
                Arc::new(MockQueueFileProvider::default()),
            )
            .expect("target library should open"),
        );
        let engine = MigrationEngine::new("source-library", source, "target-library", target)
            .expect("migration engine should open");
        let payload_bytes = fs::read(&payload_path).expect("retained fragment should read");
        let payload_hash = test_sha256(&payload_bytes);
        let staging_relative = staging_path
            .strip_prefix(engine.source.migration_root_path())
            .expect("staging directory should be below the source library")
            .to_path_buf();
        let payload_relative = staging_relative.join("segment.part");
        let payload_metadata = fs::metadata(&payload_path).expect("retained fragment should stat");
        let owner_control = task_control_proof(
            TaskTransferControlKind::Owner,
            &staging_path.join(".owner.json"),
            staging_relative.join(".owner.json"),
        );
        let retention_control = task_control_proof(
            TaskTransferControlKind::Retention,
            &staging_path.join(".retained.json"),
            staging_relative.join(".retained.json"),
        );
        let task_id = "task-cleanup-fixture".to_string();
        let impact = MigrationTaskImpact {
            task_id: task_id.clone(),
            generation: 7,
            revision: 11,
            staging_bytes: payload_metadata.len(),
            staging_file_count: 1,
            published_item_ids: Vec::new(),
        };
        let preview = engine
            .preview(&MigrationRequest {
                scope: MigrationScope::Library,
                item_ids: Vec::new(),
                conflict: ConflictPolicy::Skip,
                item_conflicts: BTreeMap::new(),
                include_incomplete_tasks: true,
                set_target_default_on_success: false,
                task_impacts: vec![impact],
                warnings: Vec::new(),
            })
            .expect("task-only library migration preview should create");
        let migration_id = preview.id.clone();
        let transfer_id = "transfer-cleanup-fixture".to_string();
        engine
            .confirm(&preview.id, &preview.revision)
            .expect("task-only migration should confirm");

        let source_destination = crate::media_libraries::TaskDestination {
            library_id: "source-library".to_string(),
            root: engine.source.migration_root_path().to_path_buf(),
            category: None,
        };
        let target_destination = crate::media_libraries::TaskDestination {
            library_id: "target-library".to_string(),
            root: engine.target.migration_root_path().to_path_buf(),
            category: None,
        };
        let mut payload_file = task_file_proof(
            &payload_path,
            payload_relative.clone(),
            TaskTransferEntryKind::File,
        );
        payload_file.chunk_sha256 = vec![payload_hash.clone()];
        let plan = TaskTransferPlan {
            transfer: crate::media_libraries::TaskDestinationTransfer {
                transfer_id: transfer_id.clone(),
                migration_id: migration_id.clone(),
                task_id,
                started_generation: 7,
                generation: 8,
                revision: 12,
                source: source_destination,
                target: target_destination,
                previous_status: crate::queue::TaskStatus::Failed,
                previous_error: Some("synthetic failure".to_string()),
                previous_cancel_requested: false,
                pause_generation: Some(7),
                plan_fingerprint: Some("synthetic-task-plan".to_string()),
            },
            source_root: engine.source.migration_root_path().to_path_buf(),
            target_root: engine.target.migration_root_path().to_path_buf(),
            queue_record_source: PathBuf::from(
                ".telegram-video-downloader-queue/task-task-cleanup-fixture.json",
            ),
            queue_record_target: PathBuf::from(
                ".telegram-video-downloader-queue/task-task-cleanup-fixture.json",
            ),
            files: vec![
                task_file_proof(
                    &staging_path,
                    staging_relative,
                    TaskTransferEntryKind::Directory,
                ),
                payload_file,
            ],
            controls: vec![owner_control, retention_control],
            published_path_rewrites: BTreeMap::new(),
            total_bytes: payload_bytes.len() as u64,
            fingerprint: "synthetic-task-plan".to_string(),
        };
        engine
            .attach_task_transfer(&migration_id, plan)
            .expect("exact private staging plan should attach");
        let job = engine
            .load_job(&migration_id)
            .expect("attached migration job should load");
        let payload_index = job.task_transfers[0]
            .plan
            .files
            .iter()
            .position(|file| file.kind == TaskTransferEntryKind::File)
            .expect("plan should include its fragment");
        let directory_index = job.task_transfers[0]
            .plan
            .files
            .iter()
            .position(|file| file.kind == TaskTransferEntryKind::Directory)
            .expect("plan should include its job directory");

        Self {
            _source_root: source_root,
            _target_root: target_root,
            staging_path,
            payload_path,
            engine,
            migration_id,
            transfer_id,
            payload_index,
            directory_index,
        }
    }

    fn copy_and_ack(&self) {
        self.copy_until_waiting_for_commit();
        self.ack_target_record();
        self.engine
            .ack_task_handoff_final(&self.migration_id, &self.transfer_id)
            .expect("queue token acknowledgment should persist");
    }

    fn copy_until_waiting_for_commit(&self) {
        for _ in 0..32 {
            let status = self
                .engine
                .step(&self.migration_id)
                .expect("task transfer step should persist");
            if status.awaiting_task_commit {
                break;
            }
            assert_ne!(status.status, MigrationRunStatus::Failed);
        }
        assert!(
            self.engine
                .status(&self.migration_id)
                .expect("migration status should load")
                .expect("migration status should exist")
                .awaiting_task_commit
        );
    }

    fn ack_target_record(&self) {
        self.engine
            .ack_task_transfer(&self.migration_id, &self.transfer_id)
            .expect("target record installation should acknowledge");
    }

    fn save_payload_cleanup_step(&self) {
        let mut job = self
            .engine
            .load_job(&self.migration_id)
            .expect("task cleanup job should load");
        self.engine
            .step_task_payload_cleanup(&mut job, 0, self.payload_index)
            .expect("task payload cleanup step should succeed");
        self.engine
            .save_job(&job)
            .expect("task payload cleanup checkpoint should save");
    }

    fn mark_stopped_with_cleanup_error(&self) {
        let mut job = self
            .engine
            .load_job(&self.migration_id)
            .expect("task cleanup job should load");
        job.status = MigrationRunStatus::Stopped;
        job.stop_remaining_latched = true;
        job.task_transfers[0].error = Some("synthetic source cleanup error".to_string());
        self.engine
            .save_job(&job)
            .expect("stopped cleanup error should persist");
    }

    fn source_controls(&self) -> Vec<TaskTransferControl> {
        self.engine
            .task_transfer_plans(&self.migration_id)
            .expect("task transfer plan should load")
            .into_iter()
            .next()
            .expect("task transfer plan should exist")
            .controls
    }

    fn source_directories(&self) -> Vec<TaskTransferFile> {
        self.engine
            .task_transfer_plans(&self.migration_id)
            .expect("task transfer plan should load")
            .into_iter()
            .next()
            .expect("task transfer plan should exist")
            .files
            .into_iter()
            .filter(|file| file.kind == TaskTransferEntryKind::Directory)
            .collect()
    }

    fn cleanup_step(&self, lease: &crate::downloader::VideoMigrationGuard) -> MigrationStatus {
        self.engine
            .step_task_source_cleanup_with_output_lease(
                &self.migration_id,
                &self.transfer_id,
                lease,
            )
            .expect("source cleanup checkpoint should persist")
    }

    async fn output_lease(&self) -> crate::downloader::VideoMigrationGuard {
        crate::downloader::hold_video_outputs_for_migration(&[
            self.engine.source.migration_root_path().to_path_buf(),
            self.engine.target.migration_root_path().to_path_buf(),
        ])
        .await
        .expect("both migration roots should acquire their output leases")
    }

    fn task_payload_target(&self) -> PathBuf {
        let plan = self
            .engine
            .task_transfer_plans(&self.migration_id)
            .expect("task transfer plan should load")
            .into_iter()
            .next()
            .expect("task transfer plan should exist");
        self.engine
            .target
            .migration_root_path()
            .join(&plan.files[self.payload_index].target_relative)
    }
}

fn test_sha256(bytes: &[u8]) -> String {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    format_sha256(&digest)
}

fn task_file_proof(
    path: &Path,
    relative: PathBuf,
    kind: TaskTransferEntryKind,
) -> TaskTransferFile {
    let metadata = fs::metadata(path).expect("task transfer entry should stat");
    let bytes = if kind == TaskTransferEntryKind::File {
        fs::read(path).expect("task transfer payload should read")
    } else {
        Vec::new()
    };
    TaskTransferFile {
        source_relative: relative.clone(),
        target_relative: relative,
        kind,
        device: metadata.dev(),
        inode: metadata.ino(),
        size_bytes: if kind == TaskTransferEntryKind::File {
            metadata.len()
        } else {
            0
        },
        owner: metadata.uid(),
        group: metadata.gid(),
        mode: metadata.mode() & 0o7777,
        link_count: Some(metadata.nlink()),
        sha256: (kind == TaskTransferEntryKind::File).then(|| test_sha256(&bytes)),
        chunk_sha256: if kind == TaskTransferEntryKind::File && !bytes.is_empty() {
            vec![test_sha256(&bytes)]
        } else {
            Vec::new()
        },
    }
}

fn task_control_proof(
    kind: TaskTransferControlKind,
    path: &Path,
    relative: PathBuf,
) -> TaskTransferControl {
    let metadata = fs::metadata(path).expect("task control should stat");
    let bytes = fs::read(path).expect("task control should read");
    TaskTransferControl {
        source_relative: relative.clone(),
        target_relative: relative,
        kind,
        device: metadata.dev(),
        inode: metadata.ino(),
        size_bytes: metadata.len(),
        owner: metadata.uid(),
        group: metadata.gid(),
        mode: metadata.mode() & 0o7777,
        link_count: Some(metadata.nlink()),
        sha256: test_sha256(&bytes),
    }
}

#[tokio::test]
async fn final_ack_cleanup_replays_owner_and_directory_unlinks_after_restart() {
    let fixture = TaskCleanupFixture::new("task-cleanup-replay");
    fixture.copy_and_ack();

    fixture.save_payload_cleanup_step();
    let payload_job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("payload cleanup intent should load");
    let payload_checkpoint = &payload_job.task_transfers[0].entries[fixture.payload_index];
    assert!(payload_checkpoint.source_cleanup_intent);
    assert!(!payload_checkpoint.source_removed);
    let payload_proof = payload_job.task_transfers[0].plan.files[fixture.payload_index].clone();
    fixture
        .engine
        .source
        .migration_task_remove_source_file(&payload_proof)
        .expect("exact fragment should unlink after its durable intent");
    drop(payload_job); // Simulate loss before the payload-removal journal save.
    fixture.save_payload_cleanup_step();
    assert!(!fixture.payload_path.exists());
    assert_eq!(
        fs::read(fixture.task_payload_target()).expect("verified target fragment should remain"),
        b"synthetic retained fragment"
    );

    fixture.mark_stopped_with_cleanup_error();
    let lease = fixture.output_lease().await;
    assert!(
        fixture
            .engine
            .task_handoff_cleanup_pending(&fixture.migration_id, "source-library")
            .expect("source cleanup status should load")
    );

    let owner = fixture
        .source_controls()
        .into_iter()
        .find(|control| control.kind == TaskTransferControlKind::Owner)
        .expect("source plan should bind its owner control");
    let owner_index = fixture
        .source_controls()
        .iter()
        .position(|control| control.kind == TaskTransferControlKind::Owner)
        .expect("source plan should bind its owner control");
    let mut cleanup_job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("control cleanup checkpoint should load");
    for _ in 0..8 {
        fixture.cleanup_step(&lease);
        cleanup_job = fixture
            .engine
            .load_job(&fixture.migration_id)
            .expect("control cleanup checkpoint should load");
        let checkpoint = &cleanup_job.task_transfers[0].controls[owner_index];
        if checkpoint.source_cleanup_intent && !checkpoint.source_removed {
            break;
        }
    }
    let owner_checkpoint = &cleanup_job.task_transfers[0].controls[owner_index];
    assert!(owner_checkpoint.source_cleanup_intent);
    assert!(!owner_checkpoint.source_removed);
    fixture
        .engine
        .source
        .migration_task_remove_source_control(&owner)
        .expect("exact owner control should unlink after its durable intent");
    drop(cleanup_job); // Simulate loss after unlink, before recording source_removed.
    drop(lease);

    // Reacquiring the migration lease must preserve and allow exact reconciliation of the
    // ownerless planned job directory; ordinary recovery must not erase it as an orphan.
    let lease = fixture.output_lease().await;
    fixture.cleanup_step(&lease);
    assert!(!fixture.staging_path.join(".owner.json").exists());
    assert!(fixture.staging_path.exists());

    let directory = fixture.source_directories()[fixture.directory_index].clone();
    assert_eq!(
        fixture
            .engine
            .source
            .migration_root_path()
            .join(&directory.source_relative),
        fixture.staging_path
    );
    fixture.cleanup_step(&lease); // Persist intent for the now-empty job directory.
    let transient = fixture.staging_path.join("transient-child");
    fs::write(&transient, b"benign directory churn").expect("transient child should write");
    fs::remove_file(&transient).expect("transient child should remove");
    fixture.cleanup_step(&lease); // Remove the exact empty directory despite mtime/ctime churn.
    assert!(!fixture.staging_path.exists());
    let status = fixture.cleanup_step(&lease); // Reconcile rmdir before the checkpoint save.
    let job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("completed cleanup checkpoint should load");
    let transfer = &job.task_transfers[0];
    assert!(transfer.entries[fixture.directory_index].source_removed);
    assert!(
        transfer
            .controls
            .iter()
            .all(|checkpoint| checkpoint.source_removed)
    );
    assert!(
        transfer.error.is_none(),
        "a fully recovered cleanup clears its stale error"
    );
    assert_eq!(status.status, MigrationRunStatus::Stopped);
    assert!(status.task_transfers[0].source_cleanup_complete);
    assert!(
        !fixture
            .engine
            .task_handoff_cleanup_pending(&fixture.migration_id, "source-library")
            .expect("finished cleanup status should load")
    );
    assert_eq!(
        fs::read(fixture.task_payload_target()).expect("target fragment should remain"),
        b"synthetic retained fragment"
    );
}

#[tokio::test]
async fn replaced_task_control_and_unplanned_child_are_preserved() {
    let control_fixture = TaskCleanupFixture::new("task-control-replacement");
    control_fixture.copy_and_ack();
    for _ in 0..3 {
        control_fixture.save_payload_cleanup_step();
    }
    control_fixture.mark_stopped_with_cleanup_error();
    let lease = control_fixture.output_lease().await;
    let retention = control_fixture
        .source_controls()
        .into_iter()
        .find(|control| control.kind == TaskTransferControlKind::Retention)
        .expect("source plan should bind its retention control");
    control_fixture.cleanup_step(&lease); // Persist the exact retention unlink intent.
    let retention_path = control_fixture
        .engine
        .source
        .migration_root_path()
        .join(&retention.source_relative);
    let original_bytes = fs::read(&retention_path).expect("retention marker should read");
    let replacement_path = retention_path.with_extension("replacement");
    fs::write(&replacement_path, &original_bytes).expect("same-content replacement should write");
    fs::set_permissions(&replacement_path, fs::Permissions::from_mode(0o600))
        .expect("replacement control should remain private");
    let replacement_inode = fs::metadata(&replacement_path)
        .expect("replacement should stat")
        .ino();
    assert_ne!(
        replacement_inode, retention.inode,
        "replacement must use a new inode"
    );
    fs::rename(&replacement_path, &retention_path)
        .expect("replacement control should install at the planned path");

    let status = control_fixture.cleanup_step(&lease);
    assert_eq!(status.status, MigrationRunStatus::Partial);
    assert!(
        status.task_transfers[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("replaced"))
    );
    assert_eq!(fs::read(&retention_path).unwrap(), original_bytes);
    assert!(control_fixture.staging_path.exists());
    assert!(
        control_fixture
            .engine
            .task_handoff_cleanup_pending(&control_fixture.migration_id, "source-library")
            .expect("replaced control should remain pending")
    );
    assert_eq!(
        control_fixture
            .engine
            .control(&control_fixture.migration_id, MigrationControl::RetryFailed)
            .expect("cleanup retry should preserve the stop latch")
            .status,
        MigrationRunStatus::Stopped
    );
    assert_eq!(
        control_fixture.cleanup_step(&lease).status,
        MigrationRunStatus::Partial,
        "retry must continue refusing the replaced support file"
    );
    drop(lease);

    let child_fixture = TaskCleanupFixture::new("task-unknown-child");
    child_fixture.copy_and_ack();
    for _ in 0..3 {
        child_fixture.save_payload_cleanup_step();
    }
    child_fixture.mark_stopped_with_cleanup_error();
    let lease = child_fixture.output_lease().await;
    let mut directory_intent_saved = false;
    for _ in 0..16 {
        child_fixture.cleanup_step(&lease);
        let job = child_fixture
            .engine
            .load_job(&child_fixture.migration_id)
            .expect("cleanup job should load");
        let transfer = &job.task_transfers[0];
        if transfer
            .controls
            .iter()
            .all(|checkpoint| checkpoint.source_removed)
            && transfer.entries[child_fixture.directory_index].source_cleanup_intent
        {
            directory_intent_saved = true;
            break;
        }
    }
    assert!(
        directory_intent_saved,
        "job directory cleanup intent should persist"
    );
    let unknown = child_fixture.staging_path.join("unknown-child");
    fs::write(&unknown, b"not in the transfer manifest").expect("unknown child should write");
    let status = child_fixture.cleanup_step(&lease);
    assert_eq!(status.status, MigrationRunStatus::Partial);
    assert!(
        status.task_transfers[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("unplanned entries"))
    );
    assert_eq!(fs::read(&unknown).unwrap(), b"not in the transfer manifest");
    assert!(child_fixture.staging_path.exists());
}

#[tokio::test]
async fn legacy_completed_handoff_without_final_ack_keeps_source_controls() {
    let fixture = TaskCleanupFixture::new("legacy-handoff-no-final-ack");
    fixture.copy_until_waiting_for_commit();
    fixture.ack_target_record();
    let mut job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("target-installed migration should load");
    assert!(job.task_commit_acked.contains(&fixture.transfer_id));
    assert!(!job.task_final_queue_acked.contains(&fixture.transfer_id));
    job.status = MigrationRunStatus::Completed;
    fixture
        .engine
        .save_job(&job)
        .expect("legacy completed checkpoint should persist");

    let attention = fixture
        .engine
        .mark_task_handoff_attention(
            &fixture.migration_id,
            &fixture.transfer_id,
            "legacy completed handoff has no exact final queue acknowledgment",
        )
        .expect("unproven legacy handoff should become explicit attention");
    assert_eq!(attention.status, MigrationRunStatus::Partial);
    assert!(fixture.payload_path.exists());
    for control in fixture.source_controls() {
        assert!(
            fixture
                .engine
                .source
                .migration_root_path()
                .join(control.source_relative)
                .exists()
        );
    }
    assert!(
        fixture
            .engine
            .task_handoff_cleanup_pending(&fixture.migration_id, "target-library")
            .expect("ambiguous handoff should remain pending")
    );

    let lease = fixture.output_lease().await;
    assert!(
        fixture
            .engine
            .step_task_source_cleanup_with_output_lease(
                &fixture.migration_id,
                &fixture.transfer_id,
                &lease,
            )
            .is_err()
    );
    assert!(fixture.payload_path.exists());
    assert!(fixture.staging_path.join(".owner.json").exists());
}
