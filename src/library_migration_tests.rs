use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::file_provider::MockQueueFileProvider;

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
                set_target_default_on_success: false,
                task_impacts: Vec::new(),
                warnings: Vec::new(),
            })
            .expect("preview should be created");
        let migration_id = preview.id.clone();
        engine
            .confirm(&preview.id, &preview.revision)
            .expect("preview should confirm");
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
