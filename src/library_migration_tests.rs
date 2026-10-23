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

fn reopen_fixture_engine(fixture: &Fixture) -> MigrationEngine {
    let source = Arc::new(
        LibraryManager::open_with_provider(
            fixture._source_root.path(),
            Arc::new(MockQueueFileProvider::default()),
        )
        .expect("source library should reopen after simulated restart"),
    );
    let target = Arc::new(
        LibraryManager::open_with_provider(
            fixture._target_root.path(),
            Arc::new(MockQueueFileProvider::default()),
        )
        .expect("target library should reopen after simulated restart"),
    );
    MigrationEngine::new("source-library", source, "target-library", target)
        .expect("migration engine should reopen after simulated restart")
}

#[test]
fn legacy_target_birth_requires_a_bounded_hash_and_persists_across_restart() {
    let fixture = Fixture::new("legacy-target-birth-upgrade");
    fixture.drive_to_completion();
    let target_snapshot = fixture
        .engine
        .target
        .migration_scan()
        .expect("target should scan after the completed migration");
    let target_item = target_snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4" && item.presence == Presence::Present)
        .expect("current target should be present");
    let target_identity = target_item
        .physical_identity
        .expect("present target should have its physical identity");
    assert!(target_identity.has_complete_birth());

    fixture
        .engine
        .make_job_legacy_for_test(&fixture.migration_id)
        .expect("test should remove the optional target birth from the durable job");
    let legacy_status = fixture
        .engine
        .status(&fixture.migration_id)
        .expect("legacy status should load")
        .expect("legacy job should remain present");
    assert_eq!(legacy_status.items[0].target_physical_identity, None);

    let upgrade = fixture
        .engine
        .upgrade_one_legacy_target_birth(&fixture.migration_id, &target_snapshot)
        .expect("legacy target should receive a bounded content verification");
    assert!(upgrade.attempted);
    assert!(!upgrade.remaining);
    assert_eq!(
        upgrade.status.items[0].target_physical_identity,
        Some(target_identity),
        "only a matching size/content/access/object proof should restore the target birth"
    );
    let upgraded_job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("upgraded durable job should load");
    let upgraded_primary = &upgraded_job.groups[0].files[0];
    assert_eq!(
        upgraded_primary.target_birth_seconds,
        target_identity.birth_seconds
    );
    assert_eq!(
        upgraded_primary.target_birth_nanoseconds,
        target_identity.birth_nanoseconds
    );
    assert_eq!(upgraded_primary.legacy_birth_verification_attempt, None);

    let reopened = reopen_fixture_engine(&fixture);
    let reloaded_status = reopened
        .status(&fixture.migration_id)
        .expect("reloaded status should load")
        .expect("reloaded job should remain present");
    assert_eq!(
        reloaded_status.items[0].target_physical_identity,
        Some(target_identity),
        "the verified target birth should survive an engine restart"
    );
    let no_op_upgrade = reopened
        .upgrade_one_legacy_target_birth(&fixture.migration_id, &target_snapshot)
        .expect("an already upgraded job should be skipped without another hash");
    assert!(!no_op_upgrade.attempted);
    assert!(!no_op_upgrade.remaining);
    assert_eq!(no_op_upgrade.status, reloaded_status);
}

#[test]
fn legacy_target_birth_wrong_sha_is_rejected_without_repeating_the_hash() {
    let fixture = Fixture::new("legacy-target-birth-wrong-sha");
    fixture.drive_to_completion();
    let target_snapshot = fixture
        .engine
        .target
        .migration_scan()
        .expect("target should scan after the completed migration");
    let target_item = target_snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4" && item.presence == Presence::Present)
        .expect("current target should be present");
    let target_identity = target_item
        .physical_identity
        .expect("present target should have its physical identity");
    assert!(target_identity.has_complete_birth());

    fixture
        .engine
        .make_job_legacy_for_test(&fixture.migration_id)
        .expect("test should remove the optional target birth from the durable job");
    fixture
        .engine
        .set_wrong_legacy_primary_sha_for_test(&fixture.migration_id)
        .expect("test should corrupt the saved content proof");
    let rejected = fixture
        .engine
        .upgrade_one_legacy_target_birth(&fixture.migration_id, &target_snapshot)
        .expect("wrong saved SHA should be recorded as a safe mismatch");
    assert!(rejected.attempted);
    assert!(!rejected.remaining);
    assert_eq!(rejected.status.items[0].target_physical_identity, None);
    let rejected_job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("mismatch checkpoint should persist");
    assert_eq!(
        rejected_job.groups[0].files[0].legacy_birth_verification_attempt,
        Some(target_identity),
        "the mismatch should be keyed to the observed physical fingerprint"
    );

    let repeated = fixture
        .engine
        .upgrade_one_legacy_target_birth(&fixture.migration_id, &target_snapshot)
        .expect("the same failed physical object should not be hashed again");
    assert!(!repeated.attempted);
    assert!(!repeated.remaining);
    assert_eq!(repeated.status.items[0].target_physical_identity, None);
    let repeated_job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("repeated no-op should keep the checkpoint readable");
    assert_eq!(
        repeated_job.groups[0].files[0].legacy_birth_verification_attempt,
        Some(target_identity)
    );
}

fn unconfirmed_preview(label: &str) -> (TestRoot, TestRoot, MigrationEngine, MigrationPreview) {
    let source_root = TestRoot::new(&format!("{label}-source"));
    let target_root = TestRoot::new(&format!("{label}-target"));
    fs::write(source_root.path().join("clip.mp4"), MEDIA_BYTES)
        .expect("synthetic media should write");
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
    (source_root, target_root, engine, preview)
}

#[test]
fn inbox_preview_confirmability_detects_expiry_and_preserves_unknown_reads() {
    let (_source_root, _target_root, engine, preview) =
        unconfirmed_preview("inbox-preview-confirmability");
    let key = (preview.id.clone(), preview.revision.clone());
    assert_eq!(
        MigrationEngine::preview_confirmability_for_inbox(
            engine.source.as_ref(),
            std::slice::from_ref(&key)
        )
        .expect("preview check should complete"),
        vec![MigrationPreviewConfirmability::Confirmable]
    );
    assert_eq!(
        MigrationEngine::preview_confirmability_for_inbox(
            engine.source.as_ref(),
            &[(preview.id.clone(), "old-revision".to_string())],
        )
        .expect("revision mismatch should be a definite stale result"),
        vec![MigrationPreviewConfirmability::DefinitelyUnconfirmable]
    );

    let name = preview_state_name(&preview.id);
    let bytes = engine
        .source
        .migration_read_state(&name, MAX_STATE_BYTES)
        .expect("preview should be readable")
        .expect("preview should be persisted");
    let mut document: PreviewDocument =
        decode_state(&bytes, "migration preview").expect("preview should decode");
    document.superseded_by = Some("refreshed-preview".to_string());
    engine
        .source
        .migration_write_state(&name, &encode_state(&document).unwrap())
        .expect("superseded preview marker should persist");
    assert_eq!(
        MigrationEngine::preview_confirmability_for_inbox(
            engine.source.as_ref(),
            std::slice::from_ref(&key),
        )
        .expect("superseded preview check should complete"),
        vec![MigrationPreviewConfirmability::DefinitelyUnconfirmable]
    );
    document.superseded_by = None;
    document.created_at = 0;
    engine
        .source
        .migration_write_state(&name, &encode_state(&document).unwrap())
        .expect("expired preview should remain persisted before maintenance");
    assert!(
        engine
            .source
            .migration_read_state(&name, MAX_STATE_BYTES)
            .expect("expired preview should remain readable")
            .is_some(),
        "the check must recognize expiration before the rotating maintenance pass retires it"
    );
    assert_eq!(
        MigrationEngine::preview_confirmability_for_inbox(engine.source.as_ref(), &[key])
            .expect("expired preview check should complete"),
        vec![MigrationPreviewConfirmability::DefinitelyUnconfirmable]
    );

    let path = engine
        .source
        .migration_root_path()
        .join(".telegram-video-downloader-library")
        .join(&name);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
        .expect("fixture should make the preview state fail its private-file policy");
    assert_eq!(
        MigrationEngine::preview_confirmability_for_inbox(
            engine.source.as_ref(),
            &[(preview.id, preview.revision)],
        )
        .expect("a per-preview read error should be represented as unknown"),
        vec![MigrationPreviewConfirmability::Unknown],
        "an unreadable expired preview must not be mislabeled as definitely missing"
    );
}

#[test]
fn inbox_preview_confirmability_rejects_a_confirmed_job_as_a_new_preview() {
    let (_source_root, _target_root, engine, preview) =
        unconfirmed_preview("inbox-preview-confirmed-job");
    engine
        .confirm(&preview.id, &preview.revision)
        .expect("preview should confirm");
    assert_eq!(
        MigrationEngine::preview_confirmability_for_inbox(
            engine.source.as_ref(),
            &[(preview.id, preview.revision)],
        )
        .expect("job-backed preview check should complete"),
        vec![MigrationPreviewConfirmability::DefinitelyUnconfirmable]
    );
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
fn refreshing_preview_writes_candidate_before_supersession_and_recovers_after_restart() {
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

    let old_snapshot = engine
        .source
        .migration_read_state_snapshot(&preview_state_name(&old.id), MAX_STATE_BYTES)
        .expect("old preview snapshot should read")
        .expect("old preview should remain persisted");
    let old_document: PreviewDocument = decode_state(&old_snapshot.contents, "migration preview")
        .expect("old preview should decode");
    let refresh_identity = engine
        .preview_refresh_identity(
            &request,
            &old_document.source,
            &old_document.target,
            &old.id,
            &old.revision,
            &old_snapshot.proof.sha256,
        )
        .expect("refresh identity should be deterministic");

    engine.set_test_preview_refresh_fault(PreviewRefreshFault::BeforeCandidateWrite);
    assert!(
        engine
            .preview_replacing(&request, &old.id, &old.revision)
            .is_err()
    );
    let after_write_failure = engine
        .source
        .migration_read_state_snapshot(&preview_state_name(&old.id), MAX_STATE_BYTES)
        .expect("old preview should remain readable after candidate write failure")
        .expect("old preview should remain persisted after candidate write failure");
    assert_eq!(after_write_failure.proof.sha256, old_snapshot.proof.sha256);
    assert!(
        engine
            .source
            .migration_read_state(
                &preview_state_name(&refresh_identity.candidate_id),
                MAX_STATE_BYTES
            )
            .expect("candidate lookup should succeed")
            .is_none()
    );
    assert_eq!(
        preview_confirmability_for_inbox_key(&engine.source, &old.id, &old.revision),
        MigrationPreviewConfirmability::Confirmable,
        "a failed new-preview write must leave the prior preview confirmable"
    );

    engine.set_test_preview_refresh_fault(PreviewRefreshFault::AfterCandidateWrite);
    assert!(
        engine
            .preview_replacing(&request, &old.id, &old.revision)
            .is_err()
    );
    let old_after_candidate = engine
        .source
        .migration_read_state_snapshot(&preview_state_name(&old.id), MAX_STATE_BYTES)
        .expect("old preview should remain readable after candidate checkpoint")
        .expect("old preview should remain persisted after candidate checkpoint");
    assert_eq!(old_after_candidate.proof.sha256, old_snapshot.proof.sha256);
    let candidate_before_restart = engine
        .source
        .migration_read_state_snapshot(
            &preview_state_name(&refresh_identity.candidate_id),
            MAX_STATE_BYTES,
        )
        .expect("candidate snapshot should read after simulated interruption")
        .expect("candidate should have been written before interruption");
    assert_eq!(
        preview_confirmability_for_inbox_key(
            &engine.source,
            &refresh_identity.candidate_id,
            PREVIEW_REVISION,
        ),
        MigrationPreviewConfirmability::DefinitelyUnconfirmable,
        "a candidate cannot be confirmed until its old-preview tombstone is durable"
    );

    drop(engine);
    let source = Arc::new(
        LibraryManager::open_with_provider(
            source_root.path(),
            Arc::new(MockQueueFileProvider::default()),
        )
        .expect("source library should reopen after simulated restart"),
    );
    let target = Arc::new(
        LibraryManager::open_with_provider(
            target_root.path(),
            Arc::new(MockQueueFileProvider::default()),
        )
        .expect("target library should reopen after simulated restart"),
    );
    let engine = MigrationEngine::new("source-library", source, "target-library", target)
        .expect("migration engine should reopen after simulated restart");
    let refreshed = engine
        .preview_replacing(&request, &old.id, &old.revision)
        .expect("retry should finish the candidate-first supersession after restart");
    assert_eq!(refreshed.id, refresh_identity.candidate_id);
    let candidate_after_replay = engine
        .source
        .migration_read_state_snapshot(
            &preview_state_name(&refresh_identity.candidate_id),
            MAX_STATE_BYTES,
        )
        .expect("replayed candidate snapshot should read")
        .expect("replayed candidate should remain persisted");
    assert_eq!(
        candidate_after_replay.contents,
        candidate_before_restart.contents
    );
    assert_ne!(
        candidate_after_replay.proof.inode, candidate_before_restart.proof.inode,
        "replay should atomically re-persist the exact candidate before tombstoning its parent"
    );
    let replayed = engine
        .preview_replacing(&request, &old.id, &old.revision)
        .expect("an inbox replay should reuse the exact durable refresh receipt");
    assert_eq!(replayed.id, refreshed.id);
    let compacted: PreviewDocument = decode_state(
        &engine
            .source
            .migration_read_state(&preview_state_name(&old.id), MAX_STATE_BYTES)
            .expect("superseded preview should remain readable")
            .expect("superseded preview tombstone should remain persisted"),
        "migration preview tombstone",
    )
    .expect("superseded preview tombstone should decode");
    assert!(preview_is_compact_tombstone(&compacted));
    assert_eq!(
        compacted.superseded_by.as_deref(),
        Some(refreshed.id.as_str())
    );
    assert_eq!(
        compacted.superseded_preview_sha256.as_deref(),
        Some(old_snapshot.proof.sha256.as_str())
    );
    assert_eq!(
        preview_confirmability_for_inbox_key(&engine.source, &old.id, &old.revision),
        MigrationPreviewConfirmability::DefinitelyUnconfirmable
    );
    assert_eq!(
        preview_confirmability_for_inbox_key(&engine.source, &refreshed.id, &refreshed.revision,),
        MigrationPreviewConfirmability::Confirmable
    );
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
fn invalid_task_conflict_refresh_preserves_old_preview_before_any_candidate_write() {
    let source_root = TestRoot::new("task-conflict-refresh-source");
    let target_root = TestRoot::new("task-conflict-refresh-target");
    fs::write(source_root.path().join("clip.mp4"), MEDIA_BYTES).expect("source media should write");
    fs::write(
        target_root.path().join("clip.mp4"),
        b"existing target media",
    )
    .expect("conflicting target media should write");
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
    let task_impact = MigrationTaskImpact {
        task_id: "unfinished-task".to_string(),
        generation: 7,
        revision: 3,
        staging_bytes: 0,
        staging_file_count: 0,
        published_item_ids: vec![item_id.clone()],
    };
    let mut request = MigrationRequest {
        scope: MigrationScope::Selected,
        item_ids: vec![item_id],
        conflict: ConflictPolicy::KeepBoth,
        item_conflicts: BTreeMap::new(),
        include_incomplete_tasks: true,
        set_target_default_on_success: false,
        task_impacts: vec![task_impact],
        warnings: Vec::new(),
    };
    let old = engine
        .preview(&request)
        .expect("keep-both preview should move every unfinished task member");
    let old_snapshot = engine
        .source
        .migration_read_state_snapshot(&preview_state_name(&old.id), MAX_STATE_BYTES)
        .expect("old preview should read")
        .expect("old preview should remain persisted");

    request.conflict = ConflictPolicy::Skip;
    let old_document: PreviewDocument =
        decode_state(&old_snapshot.contents, "migration preview").expect("preview should decode");
    let identity = engine
        .preview_refresh_identity(
            &request,
            &old_document.source,
            &old_document.target,
            &old.id,
            &old.revision,
            &old_snapshot.proof.sha256,
        )
        .expect("replacement preview identity should be deterministic");
    let error = engine
        .preview_replacing(&request, &old.id, &old.revision)
        .expect_err("a skipped published member must reject refresh before persistence");
    assert!(
        error
            .to_string()
            .contains("would stay in the source library")
    );
    let old_after = engine
        .source
        .migration_read_state_snapshot(&preview_state_name(&old.id), MAX_STATE_BYTES)
        .expect("old preview should remain readable")
        .expect("old preview should not be retired by invalid refresh");
    assert_eq!(old_after.contents, old_snapshot.contents);
    assert!(
        engine
            .source
            .migration_read_state(&preview_state_name(&identity.candidate_id), MAX_STATE_BYTES)
            .expect("candidate lookup should succeed")
            .is_none()
    );
    assert_eq!(
        MigrationEngine::preview_confirmability_for_inbox(
            engine.source.as_ref(),
            &[(old.id.clone(), old.revision.clone())],
        )
        .expect("old preview confirmation status should remain readable"),
        vec![MigrationPreviewConfirmability::Confirmable]
    );
}

#[test]
fn migration_preview_applies_scan_completeness_to_the_requested_scope() {
    let source_root = TestRoot::new("scan-completeness-preview-source");
    let target_root = TestRoot::new("scan-completeness-preview-target");
    for directory in ["A", "AB", "B"] {
        fs::create_dir(source_root.path().join(directory)).expect("source category should create");
        fs::write(
            source_root.path().join(directory).join("clip.mp4"),
            MEDIA_BYTES,
        )
        .expect("synthetic media should write");
    }
    fs::write(
        source_root.path().join("AB").join("clip.nfo"),
        "<movie><title>Fixture</title></movie>",
    )
    .expect("synthetic sidecar should write");
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
    let baseline = engine
        .source
        .migration_scan_with_completeness()
        .expect("baseline source scan should complete");
    let item_id = |path: &str| {
        baseline
            .snapshot
            .items
            .iter()
            .find(|item| item.relative_path == path && item.presence == Presence::Present)
            .expect("fixture media should be indexed")
            .id
            .clone()
    };
    let selected_request = |id: String| MigrationRequest {
        scope: MigrationScope::Selected,
        item_ids: vec![id],
        conflict: ConflictPolicy::Skip,
        item_conflicts: BTreeMap::new(),
        include_incomplete_tasks: false,
        set_target_default_on_success: false,
        task_impacts: Vec::new(),
        warnings: Vec::new(),
    };

    // A known incomplete sibling directory must not be confused with a path that merely shares
    // its textual prefix. The selected bundle includes both the video and its sidecar.
    engine.source.mark_next_scan_directory_incomplete("A");
    let selected_sibling = engine
        .preview(&selected_request(item_id("AB/clip.mp4")))
        .expect("a complete selected bundle outside the incomplete sibling should preview");
    assert_eq!(selected_sibling.item_count, 1);
    assert_eq!(selected_sibling.file_count, 2);

    engine.source.mark_next_scan_directory_incomplete("AB");
    let incomplete_bundle = engine
        .source
        .migration_scan_with_completeness()
        .expect("injected incomplete bundle scan should return its scope proof");
    assert!(
        validate_migration_scan_paths(
            &incomplete_bundle,
            &MigrationScope::Selected,
            ["AB/clip.mp4", "AB/clip.nfo"],
        )
        .is_err()
    );
    assert!(
        validate_migration_scan_paths(
            &incomplete_bundle,
            &MigrationScope::Selected,
            ["ABad/clip.mp4"],
        )
        .is_ok(),
        "directory checks must compare path components, not string prefixes"
    );

    engine.source.mark_next_scan_directory_incomplete("AB");
    assert!(
        engine
            .preview(&selected_request(item_id("AB/clip.mp4")))
            .is_err(),
        "a selected bundle in an incomplete directory must be refused"
    );

    engine.source.mark_next_scan_scope_unknown();
    let unknown_scope = engine
        .preview(&selected_request(item_id("A/clip.mp4")))
        .expect_err("a selected preview must fail when scan truncation has unknown scope");
    assert!(
        unknown_scope
            .to_string()
            .contains("source scan scope is incomplete")
    );

    engine.source.mark_next_scan_directory_incomplete("B");
    let whole_library = engine
        .preview(&MigrationRequest {
            scope: MigrationScope::Library,
            item_ids: Vec::new(),
            conflict: ConflictPolicy::Skip,
            item_conflicts: BTreeMap::new(),
            include_incomplete_tasks: false,
            set_target_default_on_success: false,
            task_impacts: Vec::new(),
            warnings: Vec::new(),
        })
        .expect_err("whole-library preview must reject a partial source scan");
    assert!(whole_library.to_string().contains("complete source scan"));
}

#[test]
fn whole_library_execution_revalidates_scan_completeness_before_copy() {
    let source_root = TestRoot::new("scan-completeness-execution-source");
    let target_root = TestRoot::new("scan-completeness-execution-target");
    for directory in ["A", "B"] {
        fs::create_dir(source_root.path().join(directory)).expect("source category should create");
        fs::write(
            source_root.path().join(directory).join("clip.mp4"),
            MEDIA_BYTES,
        )
        .expect("synthetic media should write");
    }
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
    let preview = engine
        .preview(&MigrationRequest {
            scope: MigrationScope::Library,
            item_ids: Vec::new(),
            conflict: ConflictPolicy::Skip,
            item_conflicts: BTreeMap::new(),
            include_incomplete_tasks: false,
            set_target_default_on_success: false,
            task_impacts: Vec::new(),
            warnings: Vec::new(),
        })
        .expect("complete library should preview");
    let status = engine
        .confirm(&preview.id, &preview.revision)
        .expect("complete preview should confirm");
    engine.source.mark_next_scan_directory_incomplete("B");
    let error = engine
        .step(&status.id)
        .expect_err("execution must revalidate whole-library completeness");
    assert!(error.to_string().contains("complete source scan"));

    let job = engine
        .load_job(&status.id)
        .expect("job should remain durable");
    assert!(!job.execution_revalidated);
    assert_eq!(job.status, MigrationRunStatus::Queued);
    assert!(
        job.groups
            .iter()
            .all(|group| group.status == MigrationItemPhase::Copying)
    );
    assert!(source_root.path().join("A/clip.mp4").exists());
    assert!(source_root.path().join("B/clip.mp4").exists());
    assert!(!target_root.path().join("A/clip.mp4").exists());
    assert!(!target_root.path().join("B/clip.mp4").exists());
}

#[test]
fn selected_execution_revalidates_the_entire_bundle_directory_before_copy() {
    let source_root = TestRoot::new("scan-completeness-selected-source");
    let target_root = TestRoot::new("scan-completeness-selected-target");
    let category = source_root.path().join("Category");
    fs::create_dir(&category).expect("source category should create");
    fs::write(category.join("clip.mp4"), MEDIA_BYTES).expect("synthetic media should write");
    fs::write(
        category.join("clip.nfo"),
        "<movie><title>Fixture</title></movie>",
    )
    .expect("synthetic sidecar should write");
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
    let baseline = engine
        .source
        .migration_scan_with_completeness()
        .expect("baseline source scan should complete");
    let item_id = baseline
        .snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "Category/clip.mp4")
        .expect("selected video should be indexed")
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
        .expect("selected bundle should preview from a complete scan");
    let status = engine
        .confirm(&preview.id, &preview.revision)
        .expect("selected preview should confirm");
    engine
        .source
        .mark_next_scan_directory_incomplete("Category");
    let error = engine
        .step(&status.id)
        .expect_err("selected execution must reject an incomplete bundle directory");
    assert!(error.to_string().contains("incomplete source directory"));

    let job = engine
        .load_job(&status.id)
        .expect("job should remain durable");
    assert!(!job.execution_revalidated);
    assert_eq!(job.status, MigrationRunStatus::Queued);
    assert!(category.join("clip.mp4").exists());
    assert!(category.join("clip.nfo").exists());
    assert!(!target_root.path().join("Category/clip.mp4").exists());
    assert!(!target_root.path().join("Category/clip.nfo").exists());
}

#[test]
fn confirmation_retires_preview_after_job_save_and_replay_uses_job_fallback() {
    let fixture = Fixture::new("confirmed-preview-retirement");
    let job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("confirmed migration job should remain durable");
    let preview_name = preview_state_name(&fixture.migration_id);
    assert!(job.confirmed_preview_proof.is_some());
    assert!(
        fixture
            .engine
            .source
            .migration_read_state(&preview_name, MAX_STATE_BYTES)
            .expect("preview lookup should work")
            .is_none()
    );

    let replay = fixture
        .engine
        .confirm(&fixture.migration_id, &job.preview.revision)
        .expect("duplicate confirmation should replay from the durable job");
    assert_eq!(replay.status, MigrationRunStatus::Queued);
    let located =
        MigrationEngine::find_preview_for_source(&fixture.engine.source, &fixture.migration_id)
            .expect("job fallback should validate")
            .expect("confirmed job should remain discoverable after preview retirement");
    assert_eq!(located.preview.id, fixture.migration_id);
    assert_eq!(located.bindings.target_library_id, "target-library");
    assert!(
        fixture
            .engine
            .source
            .migration_read_state(&job_state_name(&fixture.migration_id), MAX_STATE_BYTES)
            .expect("job lookup should work")
            .is_some()
    );
}

#[test]
fn preview_maintenance_retires_expired_candidates_and_rotates_past_malformed_files() {
    let (_source_root, _target_root, engine, preview) =
        unconfirmed_preview("preview-maintenance-rotation");
    let malformed_name = "migration-preview-000-malformed.json";
    engine
        .source
        .migration_write_state(malformed_name, b"not a preview document")
        .expect("malformed candidate should be private and persisted");

    let stale_id = "zzz-expired-candidate";
    let preview_bytes = engine
        .source
        .migration_read_state(&preview_state_name(&preview.id), MAX_STATE_BYTES)
        .expect("preview read should succeed")
        .expect("preview should exist");
    let mut expired: PreviewDocument =
        decode_state(&preview_bytes, "migration preview").expect("preview should decode");
    expired.preview.id = stale_id.to_string();
    expired.created_at = unix_seconds().saturating_sub(PREVIEW_RETENTION_SECONDS + 1);
    let expired_name = preview_state_name(stale_id);
    engine
        .source
        .migration_write_state(&expired_name, &encode_state(&expired).unwrap())
        .expect("expired candidate should be private and persisted");

    let mut saw_malformed = false;
    let mut retired_expired = false;
    for _ in 0..3 {
        let report = MigrationEngine::maintain_source(&engine.source)
            .expect("one bounded preview maintenance pass should finish");
        assert!(report.examined);
        saw_malformed |= report
            .warning
            .as_deref()
            .is_some_and(|warning| warning.contains("malformed preview"));
        retired_expired |= report.retired;
    }
    assert!(
        saw_malformed,
        "malformed candidates should be diagnosed and retained"
    );
    assert!(
        retired_expired,
        "rotation should reach the later expired candidate"
    );
    assert!(
        engine
            .source
            .migration_read_state(&expired_name, MAX_STATE_BYTES)
            .expect("expired candidate lookup should succeed")
            .is_none()
    );
    assert!(
        engine
            .source
            .migration_read_state(malformed_name, MAX_STATE_BYTES)
            .expect("malformed candidate lookup should succeed")
            .is_some()
    );
}

#[test]
fn preview_retirement_proof_preserves_same_content_replacement() {
    let (_source_root, _target_root, engine, preview) =
        unconfirmed_preview("preview-retirement-replacement");
    let name = preview_state_name(&preview.id);
    let snapshot = engine
        .source
        .migration_read_state_snapshot(&name, MAX_STATE_BYTES)
        .expect("preview snapshot should read")
        .expect("preview should exist");
    engine
        .source
        .migration_write_state(&name, &snapshot.contents)
        .expect("same-content replacement should install as a new state inode");
    let replacement = engine
        .source
        .migration_read_state_snapshot(&name, MAX_STATE_BYTES)
        .expect("replacement snapshot should read")
        .expect("replacement should exist");
    assert_ne!(snapshot.proof.inode, replacement.proof.inode);
    assert_eq!(snapshot.proof.sha256, replacement.proof.sha256);
    assert!(
        engine
            .source
            .migration_remove_state_if_proof(&name, &snapshot.proof)
            .is_err()
    );
    assert_eq!(
        engine
            .source
            .migration_read_state(&name, MAX_STATE_BYTES)
            .expect("replacement should remain readable")
            .expect("replacement must be retained"),
        snapshot.contents
    );
}

#[test]
fn confirmed_preview_replay_keeps_a_later_replacement_file() {
    let (_source_root, _target_root, engine, preview) =
        unconfirmed_preview("confirmed-preview-replacement");
    let name = preview_state_name(&preview.id);
    let original = engine
        .source
        .migration_read_state(&name, MAX_STATE_BYTES)
        .expect("preview should read")
        .expect("preview should exist");
    engine
        .confirm(&preview.id, &preview.revision)
        .expect("confirmation should persist the job before retiring preview");
    engine
        .source
        .migration_write_state(&name, &original)
        .expect("replacement state should install under the retired name");
    let replacement = engine
        .source
        .migration_read_state_snapshot(&name, MAX_STATE_BYTES)
        .expect("replacement should read")
        .expect("replacement should exist");
    let replay = engine
        .confirm(&preview.id, &preview.revision)
        .expect("confirmation replay should remain idempotent");
    assert_eq!(replay.status, MigrationRunStatus::Queued);
    assert!(
        engine
            .source
            .migration_read_state(&name, MAX_STATE_BYTES)
            .expect("replacement should remain readable")
            .is_some()
    );
    assert_ne!(
        replacement.proof.inode,
        engine
            .load_job(&preview.id)
            .expect("confirmed job should remain")
            .confirmed_preview_proof
            .unwrap()
            .inode
    );
}

#[test]
fn preview_maintenance_retains_expired_candidate_with_changed_access_policy() {
    let (_source_root, _target_root, engine, preview) =
        unconfirmed_preview("preview-maintenance-access-policy");
    let name = preview_state_name(&preview.id);
    let bytes = engine
        .source
        .migration_read_state(&name, MAX_STATE_BYTES)
        .expect("preview should read")
        .expect("preview should exist");
    let mut document: PreviewDocument =
        decode_state(&bytes, "migration preview").expect("preview should decode");
    document.created_at = unix_seconds().saturating_sub(PREVIEW_RETENTION_SECONDS + 1);
    let expired_bytes = encode_state(&document).expect("expired preview should encode");
    engine
        .source
        .migration_write_state(&name, &expired_bytes)
        .expect("expired preview should persist");
    let path = engine
        .source
        .migration_root_path()
        .join(".telegram-video-downloader-library")
        .join(&name);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
        .expect("fixture should change the preview access mode");

    let report = MigrationEngine::maintain_source(&engine.source)
        .expect("maintenance should report a candidate-local access failure");
    assert!(report.warning.is_some());
    assert_eq!(
        fs::read(&path).expect("unreadable preview should remain"),
        expired_bytes
    );
    assert_eq!(
        fs::metadata(&path)
            .expect("preview metadata should remain")
            .mode()
            & 0o777,
        0o644
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
fn terminal_finalization_updates_only_epoch_and_preserves_default_baseline() {
    let fixture = Fixture::with_default_update("terminal-finalization-default", true);
    let completed = fixture.drive_to_completion();
    assert!(completed.terminal_finalization_pending);
    assert!(
        fixture
            .engine
            .reauthorize_terminal_finalization(&fixture.migration_id, "epoch-after-restart", false)
            .is_err(),
        "pending terminal work without runtime context must fail closed"
    );
    fixture
        .engine
        .set_runtime_context(
            &fixture.migration_id,
            MigrationRuntimeContext {
                database_epoch: "old-epoch".to_string(),
                preview_seq: 9,
                chat_id: Some(55),
                message_id: Some(88),
                delivery: MigrationDeliveryState::Sent,
            },
        )
        .expect("terminal runtime context should persist");

    let reauthorized = fixture
        .engine
        .reauthorize_terminal_finalization(&fixture.migration_id, "epoch-after-restart", false)
        .expect("terminal epoch refresh should succeed")
        .expect("terminal job should still exist");
    assert_eq!(reauthorized.status, MigrationRunStatus::Completed);
    assert!(reauthorized.terminal_finalization_pending);
    let job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("terminal job should remain durable");
    assert_eq!(
        job.runtime_context.as_ref().unwrap().database_epoch,
        "epoch-after-restart"
    );
    assert_eq!(job.default_update_expected_revision, Some(17));
    assert!(!job.default_update_resolved);
    assert!(!job.stop_remaining_latched);

    fixture
        .engine
        .mark_default_update_resolved(&fixture.migration_id, false, Some("newer setting won"))
        .expect("default decision should resolve once");
    let settled = fixture
        .engine
        .reauthorize_terminal_finalization(&fixture.migration_id, "later-epoch", false)
        .expect("settled terminal replay should be harmless")
        .expect("job should remain available");
    assert_eq!(settled.status, MigrationRunStatus::Completed);
    assert!(!settled.terminal_finalization_pending);
    assert_eq!(
        fixture
            .engine
            .runtime_context(&fixture.migration_id)
            .expect("runtime context should load")
            .unwrap()
            .database_epoch,
        "epoch-after-restart",
        "settled no-op must not refresh terminal context again"
    );
}

#[test]
fn ordinary_control_in_epoch_is_atomic_and_terminal_replay_keeps_saved_epoch() {
    let fixture = Fixture::new("ordinary-control-epoch");
    fixture
        .engine
        .set_runtime_context(
            &fixture.migration_id,
            MigrationRuntimeContext {
                database_epoch: "initial-epoch".to_string(),
                preview_seq: 11,
                chat_id: Some(55),
                message_id: Some(90),
                delivery: MigrationDeliveryState::Sent,
            },
        )
        .expect("runtime context should persist");

    let (paused, terminal_noop) = fixture
        .engine
        .control_in_epoch(
            &fixture.migration_id,
            MigrationControl::Pause,
            "current-epoch",
        )
        .expect("ordinary control and epoch refresh should persist together");
    assert!(!terminal_noop);
    assert_eq!(paused.status, MigrationRunStatus::Paused);
    assert_eq!(
        fixture
            .engine
            .runtime_context(&fixture.migration_id)
            .expect("runtime context should load")
            .unwrap()
            .database_epoch,
        "current-epoch"
    );

    let (queued, terminal_noop) = fixture
        .engine
        .control_in_epoch(
            &fixture.migration_id,
            MigrationControl::Resume,
            "current-epoch",
        )
        .expect("resume should remain an ordinary nonterminal control");
    assert!(!terminal_noop);
    assert_eq!(queued.status, MigrationRunStatus::Queued);
    assert_eq!(
        fixture.drive_to_completion().status,
        MigrationRunStatus::Completed
    );

    let (completed, terminal_noop) = fixture
        .engine
        .control_in_epoch(
            &fixture.migration_id,
            MigrationControl::Pause,
            "retired-epoch",
        )
        .expect("a stale ordinary control should be a terminal no-op");
    assert!(terminal_noop);
    assert_eq!(completed.status, MigrationRunStatus::Completed);
    assert_eq!(
        fixture
            .engine
            .runtime_context(&fixture.migration_id)
            .expect("terminal runtime context should load")
            .unwrap()
            .database_epoch,
        "current-epoch",
        "ordinary terminal controls must not renew the saved epoch"
    );
    assert!(
        fixture
            .engine
            .control_in_epoch(
                &fixture.migration_id,
                MigrationControl::FinalizeTerminal,
                "retired-epoch",
            )
            .is_err()
    );
}

#[test]
fn stopped_terminal_finalization_requires_queue_proof_and_keeps_stop_latch() {
    let fixture = Fixture::new("terminal-finalization-stopped");
    let stopped = fixture
        .engine
        .control(&fixture.migration_id, MigrationControl::StopRemaining)
        .expect("migration should stop before execution");
    assert_eq!(stopped.status, MigrationRunStatus::Stopped);
    fixture
        .engine
        .set_runtime_context(
            &fixture.migration_id,
            MigrationRuntimeContext {
                database_epoch: "old-epoch".to_string(),
                preview_seq: 10,
                chat_id: Some(55),
                message_id: Some(89),
                delivery: MigrationDeliveryState::Sent,
            },
        )
        .expect("stopped runtime context should persist");
    let reauthorized = fixture
        .engine
        .reauthorize_terminal_finalization(&fixture.migration_id, "new-epoch", true)
        .expect("fresh queue proof should authorize terminal cleanup")
        .expect("stopped migration should remain available");
    assert_eq!(reauthorized.status, MigrationRunStatus::Stopped);
    assert!(!reauthorized.terminal_finalization_pending);
    let job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("stopped job should remain durable");
    assert!(job.stop_remaining_latched);
    assert_eq!(
        job.runtime_context.as_ref().unwrap().database_epoch,
        "new-epoch"
    );
    assert!(job.groups.iter().all(is_group_terminal));
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
async fn stopped_final_ack_cleanup_removes_verified_payload_without_resuming_media() {
    let fixture = TaskCleanupFixture::new("stopped-final-ack-payload-cleanup");
    fixture.copy_and_ack();
    fixture.mark_stopped_with_cleanup_error();
    let lease = fixture.output_lease().await;
    let unrelated = fixture
        .staging_path
        .parent()
        .expect("owned job should be below the staging root")
        .join("unrelated-migration-sibling");
    fs::write(&unrelated, b"preserve this unrelated sibling")
        .expect("unrelated staging sibling should write");

    let mut status = fixture
        .engine
        .status(&fixture.migration_id)
        .expect("migration status should read")
        .expect("migration should exist");
    for _ in 0..24 {
        status = fixture.cleanup_step(&lease);
        assert_eq!(
            status.status,
            MigrationRunStatus::Stopped,
            "final-ACK cleanup must preserve the user's stop intent"
        );
        if status.task_transfers[0].source_cleanup_complete {
            break;
        }
    }

    assert!(status.task_transfers[0].source_cleanup_complete);
    assert_eq!(status.total_items, 0);
    assert_eq!(status.completed_items, 0);
    assert_eq!(status.success_count, 0);
    assert_eq!(status.failed_count, 0);
    assert!(!fixture.payload_path.exists());
    assert!(!fixture.staging_path.exists());
    assert_eq!(
        fs::read(fixture.task_payload_target()).expect("verified target payload should remain"),
        b"synthetic retained fragment"
    );
    assert_eq!(
        fs::read(&unrelated).expect("unrelated sibling must be retained"),
        b"preserve this unrelated sibling"
    );
    let job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("stopped migration should remain durable");
    assert!(job.stop_remaining_latched);
    assert!(
        job.groups.is_empty(),
        "cleanup must not resume media groups"
    );
    assert!(
        !fixture
            .engine
            .task_handoff_cleanup_pending(&fixture.migration_id, "source-library")
            .expect("completed source cleanup should no longer be pending")
    );
}

#[tokio::test]
async fn stopped_final_ack_cleanup_preserves_a_replaced_source_payload() {
    let fixture = TaskCleanupFixture::new("stopped-final-ack-payload-replacement");
    fixture.copy_and_ack();
    fixture.mark_stopped_with_cleanup_error();
    let lease = fixture.output_lease().await;

    let first = fixture.cleanup_step(&lease);
    assert_eq!(first.status, MigrationRunStatus::Stopped);
    let job = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("cleanup intent should be durable");
    assert!(job.task_transfers[0].entries[fixture.payload_index].source_cleanup_intent);
    assert!(!job.task_transfers[0].entries[fixture.payload_index].source_removed);

    let replacement = fixture.payload_path.with_extension("replacement");
    fs::write(&replacement, b"replacement source fragment")
        .expect("replacement fragment should write");
    fs::rename(&replacement, &fixture.payload_path)
        .expect("replacement fragment should install at the source path");
    let status = fixture.cleanup_step(&lease);
    assert_eq!(status.status, MigrationRunStatus::Partial);
    assert!(
        status.task_transfers[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("replaced"))
    );
    assert_eq!(
        fs::read(&fixture.payload_path).expect("replacement source must be preserved"),
        b"replacement source fragment"
    );
    assert_eq!(
        fs::read(fixture.task_payload_target()).expect("verified target remains available"),
        b"synthetic retained fragment"
    );
    assert!(fixture.staging_path.exists());
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

#[test]
fn migration_discovery_isolates_bad_job_records_and_preserves_them() {
    use std::os::unix::fs::OpenOptionsExt;

    let fixture = Fixture::new("discovery-record-isolation");
    let source = fixture.engine.source.as_ref();
    let malformed_name = job_state_name("malformed");
    source
        .migration_write_state(&malformed_name, b"not a migration job")
        .expect("malformed record should be writable for the fixture");

    let mut mismatched = fixture
        .engine
        .load_job(&fixture.migration_id)
        .expect("valid migration should load");
    mismatched.preview.id = "source-mismatch".to_string();
    mismatched.source.device = mismatched.source.device.saturating_add(1);
    let mismatch_name = job_state_name("source-mismatch");
    source
        .migration_write_state(
            &mismatch_name,
            &serde_json::to_vec(&mismatched).expect("mismatched job should serialize"),
        )
        .expect("mismatched record should be writable for the fixture");

    let oversized_name = job_state_name("oversized");
    let oversized_path = source
        .migration_root_path()
        .join(".telegram-video-downloader-library")
        .join(&oversized_name);
    let oversized_file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&oversized_path)
        .expect("oversized record should create");
    oversized_file
        .set_len(MAX_STATE_BYTES as u64 + 1)
        .expect("sparse oversized record should extend");
    drop(oversized_file);

    let disappeared_name = job_state_name("disappeared");
    let mut names = source
        .migration_state_names()
        .expect("migration records should enumerate");
    names.push(disappeared_name.clone());
    let report =
        MigrationEngine::list_for_discovery_with_reader(source, "source-library", names, |name| {
            if name == disappeared_name {
                Ok(None)
            } else {
                source.migration_read_state(name, MAX_STATE_BYTES)
            }
        });

    assert_eq!(report.statuses.len(), 1);
    assert_eq!(report.statuses[0].id, fixture.migration_id);
    assert_eq!(report.omitted_count, 0);
    let kinds = report
        .warnings
        .iter()
        .map(|warning| (warning.record_name.as_str(), warning.kind))
        .collect::<std::collections::BTreeSet<_>>();
    assert!(kinds.contains(&(
        malformed_name.as_str(),
        MigrationDiscoveryWarningKind::Malformed
    )));
    assert!(kinds.contains(&(
        oversized_name.as_str(),
        MigrationDiscoveryWarningKind::Oversized
    )));
    assert!(
        kinds.contains(&(
            mismatch_name.as_str(),
            MigrationDiscoveryWarningKind::SourceBindingMismatch
        )),
        "unexpected discovery warnings: {:?}",
        report.warnings
    );
    assert!(kinds.contains(&(
        disappeared_name.as_str(),
        MigrationDiscoveryWarningKind::Disappeared
    )));
    assert!(report.warnings.iter().all(|warning| {
        warning.source_library_id == "source-library"
            && warning.message.len() <= MIGRATION_DISCOVERY_MESSAGE_BYTES
            && warning.record_name.len() <= MIGRATION_DISCOVERY_RECORD_NAME_BYTES
    }));

    assert_eq!(
        source
            .migration_read_state(&malformed_name, MAX_STATE_BYTES)
            .expect("malformed record should remain readable")
            .as_deref(),
        Some(b"not a migration job".as_slice())
    );
    assert_eq!(
        fs::metadata(&oversized_path)
            .expect("oversized record should remain present")
            .len(),
        MAX_STATE_BYTES as u64 + 1
    );
    assert!(
        MigrationEngine::list_for_source(source).is_err(),
        "strict dependency listing must continue to fail closed"
    );
}

#[test]
fn migration_discovery_warning_projection_is_bounded_and_sorted() {
    let mut warnings = MigrationDiscoveryWarnings::default();
    for index in (0..MAX_MIGRATION_DISCOVERY_WARNINGS + 8).rev() {
        warnings.push(MigrationDiscoveryWarning::new(
            &format!("library-{index:02}"),
            &format!("migration-job-{index:02}.json"),
            MigrationDiscoveryWarningKind::Malformed,
        ));
    }
    warnings.push(MigrationDiscoveryWarning::new(
        &format!("{}é", "x".repeat(127)),
        &format!("{}é", "record".repeat(30)),
        MigrationDiscoveryWarningKind::Unreadable,
    ));

    assert_eq!(warnings.warnings.len(), MAX_MIGRATION_DISCOVERY_WARNINGS);
    assert_eq!(warnings.omitted_count, 9);
    assert!(
        warnings
            .warnings
            .windows(2)
            .all(|pair| { discovery_warning_order(&pair[0], &pair[1]).is_le() })
    );
    assert!(warnings.warnings.iter().all(|warning| {
        warning.source_library_id.len() <= MIGRATION_DISCOVERY_LIBRARY_ID_BYTES
            && warning.record_name.len() <= MIGRATION_DISCOVERY_RECORD_NAME_BYTES
            && warning.message.len() <= MIGRATION_DISCOVERY_MESSAGE_BYTES
    }));
}

#[test]
fn migration_record_warning_classifier_ignores_unrelated_execution_errors() {
    assert_eq!(
        migration_record_warning_kind(
            "task disappeared while migration was acquiring output leases"
        ),
        None,
        "queue-task failures must not be presented as damaged migration records"
    );
    assert_eq!(
        migration_record_warning_kind("migration task staging file exceeded its size limit"),
        None,
        "task payload failures must remain task-transfer errors"
    );
    assert_eq!(
        migration_record_warning_kind("migration job does not exist in this source library"),
        Some(MigrationDiscoveryWarningKind::Disappeared)
    );
    assert_eq!(
        migration_record_warning_kind("media-library state file exceeds its size limit"),
        Some(MigrationDiscoveryWarningKind::Oversized)
    );
    assert_eq!(
        migration_record_warning_kind(
            "persisted migration source binding no longer matches this library"
        ),
        Some(MigrationDiscoveryWarningKind::SourceBindingMismatch)
    );
}
