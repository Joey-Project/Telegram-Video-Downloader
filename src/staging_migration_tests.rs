use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::media_libraries::{TaskDestination, TaskDestinationTransfer};
use crate::queue::{QueueManager, TaskRecord};

use super::*;

fn test_root(label: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock should be after the epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "telegram-video-downloader-staging-migration-{label}-{}-{stamp}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("test root should be created");
    root
}

struct QueueFixture {
    directory: PathBuf,
    source_root: PathBuf,
    target_root: PathBuf,
    queue: QueueManager,
    target: TaskDestination,
}

fn queue_fixture(label: &str) -> QueueFixture {
    let directory = test_root(label);
    let source_root = directory.join("videos");
    let target_root = directory.join("archive");
    let pdf_root = directory.join("pdfs");
    fs::create_dir_all(&source_root).expect("source library should be created");
    fs::create_dir_all(&target_root).expect("target library should be created");
    fs::create_dir_all(&pdf_root).expect("PDF root should be created");

    let mut config = AppConfig::for_test();
    config.downloads.video_dir = source_root.clone();
    config.downloads.pdf_dir = pdf_root;
    let queue = QueueManager::open(&config).expect("queue should open");
    let library = queue
        .register_video_library("Archive", &target_root, None, false, false)
        .expect("target library should register");
    let target = queue
        .video_destination(&library.id, None)
        .expect("target destination should resolve");

    QueueFixture {
        directory,
        source_root,
        target_root,
        queue,
        target,
    }
}

fn create_task_with_staging(fixture: &QueueFixture, id: &str, staging_path: &Path) {
    let task = TaskRecord::new(
        id.to_string(),
        1,
        2,
        123_456_789,
        Some(3),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=staging-migration-test".to_string(),
        },
    );
    assert!(fixture.queue.create(task).expect("task should persist"));
    fixture
        .queue
        .record_staging_path_if_generation(id, 0, staging_path.to_path_buf())
        .expect("staging path should persist")
        .expect("task should remain present");
}

fn begin_transfer(fixture: &QueueFixture, id: &str) -> TaskDestinationTransfer {
    let record = fixture
        .queue
        .get(id)
        .expect("task should be readable")
        .expect("task should exist");
    fixture
        .queue
        .begin_task_destination_transfer(
            id,
            &format!("migration-{id}"),
            record.generation,
            record.revision,
            fixture.target.clone(),
        )
        .expect("task transfer should begin")
}

fn task_queue_record_path(root: &Path, id: &str) -> PathBuf {
    root.join(".telegram-video-downloader-queue")
        .join(format!("task-{id}.json"))
}

fn mark_video_recovery_dirty(root: &RootedFs) {
    video_recovery_state_file(root)
        .expect("video recovery state should open")
        .0
        .write_state(VIDEO_RECOVERY_STATE_DIRTY)
        .expect("video recovery state should be dirty before simulated restart");
}

fn digest_hex(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn capture_control(
    root: &RootedFs,
    job_relative: &Path,
    kind: TaskTransferControlKind,
) -> TaskTransferControl {
    let leaf = migration_control_leaf(kind);
    let relative = job_relative.join(leaf);
    let path = root.logical_root_path().join(&relative);
    let file = root
        .open_bound_file(&path)
        .expect("control open should succeed")
        .expect("control should exist");
    let policy = file.access_policy().expect("control policy should read");
    TaskTransferControl {
        source_relative: relative.clone(),
        target_relative: relative,
        kind,
        device: file.identity().device(),
        inode: file.identity().inode(),
        size_bytes: file.byte_len().expect("control size should read"),
        owner: policy.owner,
        group: policy.group,
        mode: policy.mode,
        link_count: Some(policy.link_count),
        sha256: digest_hex(file.sha256().expect("control digest should read")),
    }
}

fn controls_for(
    root: &RootedFs,
    job_relative: &Path,
    kinds: &[TaskTransferControlKind],
) -> Vec<TaskTransferControl> {
    kinds
        .iter()
        .copied()
        .map(|kind| capture_control(root, job_relative, kind))
        .collect()
}

fn migration_control_leaf(kind: TaskTransferControlKind) -> &'static str {
    match kind {
        TaskTransferControlKind::Owner => VIDEO_STAGING_OWNER_FILE_NAME,
        TaskTransferControlKind::Retention => VIDEO_STAGING_RETENTION_FILE_NAME,
        TaskTransferControlKind::PublicationManifest => VIDEO_STAGING_PUBLICATION_MANIFEST_NAME,
        TaskTransferControlKind::WorkerRequest => BILIBILI_WORKER_REQUEST_FILE_NAME,
        TaskTransferControlKind::WorkerLifecycle => BILIBILI_WORKER_LIFECYCLE_FILE_NAME,
        TaskTransferControlKind::WorkerLifecycleTransition => {
            BILIBILI_WORKER_LIFECYCLE_TRANSITION_FILE_NAME
        }
        TaskTransferControlKind::BbdownConfig => "BBDown.config",
    }
}

#[test]
fn rebinds_real_worker_lifecycle_controls() {
    let directory = test_root("control-rebind");
    let source_path = directory.join("source");
    let target_path = directory.join("target");
    fs::create_dir_all(&source_path).expect("source root should be created");
    fs::create_dir_all(&target_path).expect("target root should be created");
    let source = RootedFs::new(&source_path).expect("source root should bind");
    let target = RootedFs::new(&target_path).expect("target root should bind");
    let staging = create_video_staging_dir(&source).expect("source staging job should create");
    staging.preserve_for_recovery();
    staging
        .retain_for_manual_recovery("source recovery provenance")
        .expect("source retention proof should persist");
    persist_bilibili_worker_lifecycle(&staging).expect("actual Bilibili lifecycle should persist");

    let job_relative = staging
        .path()
        .strip_prefix(source.logical_root_path())
        .expect("staging path should be rooted")
        .to_path_buf();
    let source_owner: VideoStagingOwner = serde_json::from_slice(
        &source
            .open_bound_file(&staging.path().join(VIDEO_STAGING_OWNER_FILE_NAME))
            .unwrap()
            .unwrap()
            .read_limited(VIDEO_STAGING_OWNER_LIMIT)
            .unwrap(),
    )
    .expect("source owner should parse");
    let source_lifecycle: BilibiliWorkerLifecycle = serde_json::from_slice(
        &source
            .open_bound_file(&staging.path().join(BILIBILI_WORKER_LIFECYCLE_FILE_NAME))
            .unwrap()
            .unwrap()
            .read_limited(BILIBILI_WORKER_LIFECYCLE_LIMIT)
            .unwrap(),
    )
    .expect("source lifecycle should parse");

    let controls = controls_for(
        &source,
        &job_relative,
        &[
            TaskTransferControlKind::Owner,
            TaskTransferControlKind::Retention,
            TaskTransferControlKind::WorkerLifecycle,
        ],
    );

    let transfer_id = "control-rebind-test";
    let target_identity = prepare_video_staging_for_migration(&target, &job_relative, transfer_id)
        .expect("target owner and retention should install before rebind");
    let receipts = rebind_video_staging_controls_for_migration(
        &source,
        &target,
        &job_relative,
        transfer_id,
        staging.identity,
        &controls,
    )
    .expect("known source controls should rebind to target identities");

    let target_job = target.logical_root_path().join(&job_relative);
    let target_owner: VideoStagingOwner = serde_json::from_slice(
        &target
            .open_bound_file(&target_job.join(VIDEO_STAGING_OWNER_FILE_NAME))
            .unwrap()
            .unwrap()
            .read_limited(VIDEO_STAGING_OWNER_LIMIT)
            .unwrap(),
    )
    .expect("target owner should parse");
    let target_lifecycle: BilibiliWorkerLifecycle = serde_json::from_slice(
        &target
            .open_bound_file(&target_job.join(BILIBILI_WORKER_LIFECYCLE_FILE_NAME))
            .unwrap()
            .unwrap()
            .read_limited(BILIBILI_WORKER_LIFECYCLE_LIMIT)
            .unwrap(),
    )
    .expect("target lifecycle should parse");
    assert_eq!(target_owner.root_device, target.root_identity().device());
    assert_eq!(target_owner.root_inode, target.root_identity().inode());
    assert_eq!(target_owner.staging_device, target_identity.device());
    assert_eq!(target_owner.staging_inode, target_identity.inode());
    assert_eq!(
        target_lifecycle.root_device,
        target.root_identity().device()
    );
    assert_eq!(target_lifecycle.root_inode, target.root_identity().inode());
    assert_eq!(target_lifecycle.staging_device, target_identity.device());
    assert_eq!(target_lifecycle.staging_inode, target_identity.inode());
    assert_eq!(target_lifecycle.phase, source_lifecycle.phase);
    assert_ne!(target_owner.root_inode, source_owner.root_inode);
    assert_ne!(target_owner.staging_inode, source_owner.staging_inode);
    assert_eq!(
        retained_video_staging_reason(&target, &target_job, target_identity).unwrap(),
        Some(migration_staging_retention_reason(transfer_id))
    );
    assert!(receipts.iter().any(|receipt| {
        receipt.kind == TaskTransferControlKind::WorkerLifecycle
            && receipt.relative_path == job_relative.join(BILIBILI_WORKER_LIFECYCLE_FILE_NAME)
    }));
    inspect_video_staging_for_migration(&source, &job_relative, staging.identity, &controls)
        .expect("source controls should remain intact until ACK");

    drop(staging);
    let _ = fs::remove_dir_all(directory);
}

#[test]
fn production_completed_lifecycle_transition_rebinds_as_valid_target_pair() {
    let directory = test_root("completed-lifecycle-transition");
    let source_path = directory.join("source");
    let target_path = directory.join("target");
    fs::create_dir_all(&source_path).expect("source root should be created");
    fs::create_dir_all(&target_path).expect("target root should be created");
    let source = RootedFs::new(&source_path).expect("source root should bind");
    let target = RootedFs::new(&target_path).expect("target root should bind");
    let staging = create_video_staging_dir(&source).expect("source staging job should create");
    staging.preserve_for_recovery();
    staging
        .retain_for_manual_recovery("source recovery provenance")
        .expect("source retention proof should persist");
    persist_bilibili_worker_lifecycle(&staging).expect("launch lifecycle should persist");
    let launch_lifecycle: BilibiliWorkerLifecycle = serde_json::from_slice(
        &source
            .open_bound_file(&staging.path().join(BILIBILI_WORKER_LIFECYCLE_FILE_NAME))
            .unwrap()
            .unwrap()
            .read_limited(BILIBILI_WORKER_LIFECYCLE_LIMIT)
            .unwrap(),
    )
    .expect("launch lifecycle should parse before completion");

    let output_lock = video_output_lock_file(&source).expect("output lock should bind");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = source_path.clone();
    let request = build_bilibili_worker_request(
        &config,
        &source_path,
        "https://www.bilibili.com/video/BV123",
        None,
        BilibiliWorkerRequestExpectations::default(),
        &staging,
        &output_lock,
    );
    let source_worker_root =
        RootedFs::new(staging.path()).expect("worker staging root should bind");
    persist_bilibili_worker_core_download_lifecycle(&source_worker_root, &request)
        .expect("production completion transition should persist");
    assert!(
        !staging
            .path()
            .join(BILIBILI_WORKER_LIFECYCLE_TRANSITION_FILE_NAME)
            .exists(),
        "successful atomic completion should remove its transition file"
    );
    // Model interruption after the production atomic exchange but before it removes the
    // displaced launch record: main is completed while `.next` is the old launch record.
    // Both records use the production encoder and the identities bound by production.
    source
        .create_new_bound_file(
            &staging
                .path()
                .join(BILIBILI_WORKER_LIFECYCLE_TRANSITION_FILE_NAME),
            &encode_bilibili_worker_lifecycle(&launch_lifecycle).unwrap(),
            0o600,
        )
        .expect("post-exchange lifecycle transition should persist as a crash fixture");
    assert_eq!(
        bilibili_worker_lifecycle_phase(
            &source,
            staging.path(),
            source.root_identity().device(),
            source.root_identity().inode(),
            staging.identity.device(),
            staging.identity.inode(),
        )
        .expect("source lifecycle pair should validate"),
        Some(BilibiliWorkerLifecyclePhase::CoreDownloadCompleted)
    );

    let job_relative = staging
        .path()
        .strip_prefix(source.logical_root_path())
        .expect("staging path should be rooted")
        .to_path_buf();
    let controls = controls_for(
        &source,
        &job_relative,
        &[
            TaskTransferControlKind::Owner,
            TaskTransferControlKind::Retention,
            TaskTransferControlKind::WorkerLifecycle,
            TaskTransferControlKind::WorkerLifecycleTransition,
        ],
    );
    inspect_video_staging_for_migration(&source, &job_relative, staging.identity, &controls)
        .expect("valid completed transition pair should be migration-ready");

    let transfer_id = "completed-lifecycle-transition-test";
    let target_identity = prepare_video_staging_for_migration(&target, &job_relative, transfer_id)
        .expect("target owner and retention should install before rebind");
    let receipts = rebind_video_staging_controls_for_migration(
        &source,
        &target,
        &job_relative,
        transfer_id,
        staging.identity,
        &controls,
    )
    .expect("completed lifecycle controls should rebind");

    let target_job = target.logical_root_path().join(&job_relative);
    let target_lifecycle: BilibiliWorkerLifecycle = serde_json::from_slice(
        &target
            .open_bound_file(&target_job.join(BILIBILI_WORKER_LIFECYCLE_FILE_NAME))
            .unwrap()
            .unwrap()
            .read_limited(BILIBILI_WORKER_LIFECYCLE_LIMIT)
            .unwrap(),
    )
    .expect("target lifecycle should parse");
    let target_transition: BilibiliWorkerLifecycle = serde_json::from_slice(
        &target
            .open_bound_file(&target_job.join(BILIBILI_WORKER_LIFECYCLE_TRANSITION_FILE_NAME))
            .unwrap()
            .unwrap()
            .read_limited(BILIBILI_WORKER_LIFECYCLE_LIMIT)
            .unwrap(),
    )
    .expect("target transition should parse");
    for lifecycle in [&target_lifecycle, &target_transition] {
        assert_eq!(lifecycle.root_device, target.root_identity().device());
        assert_eq!(lifecycle.root_inode, target.root_identity().inode());
        assert_eq!(lifecycle.staging_device, target_identity.device());
        assert_eq!(lifecycle.staging_inode, target_identity.inode());
    }
    assert_eq!(
        target_lifecycle.phase,
        BilibiliWorkerLifecyclePhase::CoreDownloadCompleted
    );
    assert_eq!(
        target_transition.phase,
        BilibiliWorkerLifecyclePhase::Launched
    );
    assert_eq!(
        bilibili_worker_lifecycle_phase(
            &target,
            &target_job,
            target.root_identity().device(),
            target.root_identity().inode(),
            target_identity.device(),
            target_identity.inode(),
        )
        .expect("rebound lifecycle pair should validate"),
        Some(BilibiliWorkerLifecyclePhase::CoreDownloadCompleted)
    );
    assert!(receipts.iter().any(|receipt| {
        receipt.kind == TaskTransferControlKind::WorkerLifecycleTransition
            && receipt.relative_path
                == job_relative.join(BILIBILI_WORKER_LIFECYCLE_TRANSITION_FILE_NAME)
    }));
    let target_controls = controls_for(
        &target,
        &job_relative,
        &[
            TaskTransferControlKind::Owner,
            TaskTransferControlKind::Retention,
            TaskTransferControlKind::WorkerLifecycle,
            TaskTransferControlKind::WorkerLifecycleTransition,
        ],
    );
    inspect_video_staging_for_migration(&target, &job_relative, target_identity, &target_controls)
        .expect("rebound target lifecycle controls should pass source-plan validation");
    inspect_video_staging_for_migration(&source, &job_relative, staging.identity, &controls)
        .expect("source lifecycle controls should remain intact until ACK");

    drop(output_lock);
    drop(staging);
    let _ = fs::remove_dir_all(directory);
}

#[test]
fn malformed_lifecycle_transition_pair_is_rejected_for_migration() {
    let directory = test_root("malformed-lifecycle-transition");
    let source_path = directory.join("source");
    fs::create_dir_all(&source_path).expect("source root should be created");
    let source = RootedFs::new(&source_path).expect("source root should bind");
    let staging = create_video_staging_dir(&source).expect("source staging job should create");
    staging.preserve_for_recovery();
    staging
        .retain_for_manual_recovery("source recovery provenance")
        .expect("source retention proof should persist");
    persist_bilibili_worker_lifecycle(&staging).expect("launch lifecycle should persist");
    let invalid_transition = BilibiliWorkerLifecycle {
        version: BILIBILI_WORKER_LIFECYCLE_VERSION,
        root_device: source.root_identity().device(),
        root_inode: source.root_identity().inode(),
        staging_device: staging.identity.device(),
        staging_inode: staging.identity.inode(),
        phase: BilibiliWorkerLifecyclePhase::Launched,
    };
    source
        .create_new_bound_file(
            &staging
                .path()
                .join(BILIBILI_WORKER_LIFECYCLE_TRANSITION_FILE_NAME),
            &encode_bilibili_worker_lifecycle(&invalid_transition).unwrap(),
            0o600,
        )
        .expect("invalid same-phase transition should persist as a fixture");

    let job_relative = staging
        .path()
        .strip_prefix(source.logical_root_path())
        .expect("staging path should be rooted")
        .to_path_buf();
    let controls = controls_for(
        &source,
        &job_relative,
        &[
            TaskTransferControlKind::Owner,
            TaskTransferControlKind::Retention,
            TaskTransferControlKind::WorkerLifecycle,
            TaskTransferControlKind::WorkerLifecycleTransition,
        ],
    );
    let error =
        inspect_video_staging_for_migration(&source, &job_relative, staging.identity, &controls)
            .expect_err("unresolved same-phase transition must not migrate");
    assert!(format!("{error:#}").contains("invalid phase pair"));

    drop(staging);
    let _ = fs::remove_dir_all(directory);
}

#[test]
fn legacy_path_bearing_bbdown_config_is_rejected_and_retained_at_source() {
    let fixture = queue_fixture("legacy-bbdown-config-reject");
    let source = RootedFs::new(&fixture.source_root).expect("source root should bind");
    let staging = create_video_staging_dir(&source).expect("source staging job should create");
    staging.preserve_for_recovery();
    staging
        .retain_for_manual_recovery("source recovery provenance")
        .expect("source retention proof should persist");
    let config_bytes = format!("--output-dir {}\n", fixture.source_root.display()).into_bytes();
    let config_path = staging.path().join("BBDown.config");
    fs::write(&config_path, &config_bytes).expect("legacy config fixture should write");
    #[cfg(unix)]
    fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600))
        .expect("legacy config should become private");

    let id = "legacy-bbdown-config-reject-task";
    create_task_with_staging(&fixture, id, staging.path());
    let transfer = begin_transfer(&fixture, id);
    let error = fixture
        .queue
        .task_transfer_plan(&transfer)
        .expect_err("opaque path-bearing legacy config must block migration");
    assert!(format!("{error:#}").contains("resolved by staging recovery"));
    assert!(
        !task_queue_record_path(&fixture.target_root, id).exists(),
        "legacy config must block before destination queue installation"
    );
    assert_eq!(
        fs::read(&config_path).expect("source legacy config should remain intact"),
        config_bytes
    );
    fixture
        .queue
        .abort_task_destination_transfer(&transfer)
        .expect("failed transfer should abort cleanly");

    drop(staging);
    drop(fixture.queue);
    let _ = fs::remove_dir_all(fixture.directory);
}

#[tokio::test]
async fn migration_preflight_preserves_unretained_owned_fragment() {
    let directory = test_root("preflight-unretained-fragment");
    let source_path = directory.join("source");
    fs::create_dir_all(&source_path).expect("source root should be created");
    let root = RootedFs::new(&source_path).expect("source root should bind");
    let staging = create_video_staging_dir(&root).expect("owned staging job should create");
    staging.preserve_for_recovery();
    let job_path = staging.path().to_path_buf();
    let fragment = job_path.join("segment.part");
    let fragment_bytes = b"incomplete source fragment";
    fs::write(&fragment, fragment_bytes).expect("source fragment should write");
    mark_video_recovery_dirty(&root);
    drop(staging);
    drop(root);

    let error = hold_video_outputs_for_migration(std::slice::from_ref(&source_path))
        .await
        .err()
        .expect("migration guard should refuse destructive recovery");
    assert!(
        format!("{error:#}").contains("migration preflight refused destructive staging recovery")
    );
    assert!(job_path.is_dir(), "the owned staging directory must remain");
    assert_eq!(
        fs::read(&fragment).expect("fragment should survive migration refusal"),
        fragment_bytes
    );
    let _ = fs::remove_dir_all(directory);
}

#[tokio::test]
async fn migration_preflight_preserves_pending_publication_and_fragment() {
    let directory = test_root("preflight-pending-publication");
    let source_path = directory.join("source");
    fs::create_dir_all(&source_path).expect("source root should be created");
    let root = RootedFs::new(&source_path).expect("source root should bind");
    let staging = create_video_staging_dir(&root).expect("owned staging job should create");
    staging.preserve_for_recovery();
    let job_path = staging.path().to_path_buf();
    let staged_video = job_path.join("Episode.mkv");
    let video_bytes = b"pending publication payload";
    fs::write(&staged_video, video_bytes).expect("staged video should write");
    let fragment = job_path.join("segment.part");
    let fragment_bytes = b"incomplete source fragment";
    fs::write(&fragment, fragment_bytes).expect("source fragment should write");
    let mut publication = staged_move_plan(
        &job_path,
        &source_path,
        std::slice::from_ref(&staged_video),
        None,
        StagedPrimaryMediaKind::Video,
    )
    .expect("pending publication plan should build");
    prepare_staged_publication_with_limits_and_sync(
        &root,
        &staging,
        &mut publication,
        StagedOverwriteCommit::None,
        VIDEO_STAGING_PUBLICATION_MAX_STEPS,
        VIDEO_STAGING_PUBLICATION_MANIFEST_LIMIT,
        &mut |_, bound_directory| {
            bound_directory.sync_all()?;
            Ok(())
        },
    )
    .expect("production publication code should write its real manifest");
    let manifest_path = job_path.join(VIDEO_STAGING_PUBLICATION_MANIFEST_NAME);
    let manifest_bytes = fs::read(&manifest_path).expect("publication manifest should persist");
    let target_video = source_path.join("Episode.mkv");
    mark_video_recovery_dirty(&root);
    drop(staging);
    drop(root);

    let error = hold_video_outputs_for_migration(std::slice::from_ref(&source_path))
        .await
        .err()
        .expect("migration guard should refuse destructive publication recovery");
    assert!(
        format!("{error:#}").contains("migration preflight refused destructive staging recovery")
    );
    assert!(job_path.is_dir(), "the owned staging directory must remain");
    assert_eq!(
        fs::read(&staged_video).expect("staged video should survive migration refusal"),
        video_bytes
    );
    assert_eq!(
        fs::read(&fragment).expect("fragment should survive migration refusal"),
        fragment_bytes
    );
    assert_eq!(
        fs::read(&manifest_path).expect("publication manifest should survive refusal"),
        manifest_bytes
    );
    assert!(
        !target_video.exists(),
        "startup recovery must not publish into the source library after refusal"
    );
    let _ = fs::remove_dir_all(directory);
}

#[tokio::test]
async fn migration_preflight_preserves_path_bearing_config_and_fragment() {
    let directory = test_root("preflight-path-bearing-config");
    let source_path = directory.join("source");
    fs::create_dir_all(&source_path).expect("source root should be created");
    let root = RootedFs::new(&source_path).expect("source root should bind");
    let staging = create_video_staging_dir(&root).expect("owned staging job should create");
    staging.preserve_for_recovery();
    staging
        .retain_for_manual_recovery("source recovery provenance")
        .expect("retained staging should persist");
    let job_path = staging.path().to_path_buf();
    let fragment = job_path.join("segment.part");
    let fragment_bytes = b"unfinished downloader fragment";
    fs::write(&fragment, fragment_bytes).expect("source fragment should write");
    let config_path = job_path.join("BBDown.config");
    let config_bytes = format!("--output-dir {}\n", source_path.display()).into_bytes();
    fs::write(&config_path, &config_bytes).expect("path-bearing config should write");
    #[cfg(unix)]
    fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600))
        .expect("path-bearing config should be private");
    mark_video_recovery_dirty(&root);
    drop(staging);
    drop(root);

    let error = hold_video_outputs_for_migration(std::slice::from_ref(&source_path))
        .await
        .err()
        .expect("migration guard should refuse destructive recovery of path-bound controls");
    let error = format!("{error:#}");
    assert!(error.contains("migration preflight refused destructive staging recovery"));
    assert!(error.contains("legacy/path-bound staging support files"));
    assert!(job_path.is_dir(), "the retained job directory must remain");
    assert_eq!(
        fs::read(&fragment).expect("fragment should survive migration refusal"),
        fragment_bytes
    );
    assert_eq!(
        fs::read(&config_path).expect("path-bearing config should survive migration refusal"),
        config_bytes
    );
    let _ = fs::remove_dir_all(directory);
}

#[test]
fn real_worker_request_is_rejected_before_target_queue_install() {
    let fixture = queue_fixture("worker-request-reject");
    let source = RootedFs::new(&fixture.source_root).expect("source root should bind");
    let staging = create_video_staging_dir(&source).expect("staging should create");
    staging.preserve_for_recovery();
    persist_bilibili_worker_lifecycle(&staging).expect("worker launch lifecycle should persist");
    let output_lock = video_output_lock_file(&source).expect("shared output lock should bind");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = fixture.source_root.clone();
    let request = build_bilibili_worker_request(
        &config,
        &fixture.source_root,
        "https://www.bilibili.com/video/BV123",
        None,
        BilibiliWorkerRequestExpectations::default(),
        &staging,
        &output_lock,
    );
    let request_bytes = serde_json::to_vec(&request).expect("actual worker request should encode");
    source
        .create_new_bound_file(
            &staging.path().join(BILIBILI_WORKER_REQUEST_FILE_NAME),
            &request_bytes,
            0o600,
        )
        .expect("real-format worker request should persist");

    let id = "worker-request-reject-task";
    create_task_with_staging(&fixture, id, staging.path());
    let transfer = begin_transfer(&fixture, id);
    let error = fixture
        .queue
        .task_transfer_plan(&transfer)
        .expect_err("ephemeral worker request must block plan creation");
    assert!(format!("{error:#}").contains("ephemeral Bilibili worker request"));
    assert!(
        !task_queue_record_path(&fixture.target_root, id).exists(),
        "a rejected source request must not install a destination queue record"
    );
    assert!(
        fixture
            .queue
            .get(id)
            .unwrap()
            .unwrap()
            .destination_transfer
            .is_some()
    );
    fixture
        .queue
        .abort_task_destination_transfer(&transfer)
        .expect("failed transfer should abort cleanly");

    drop(output_lock);
    drop(staging);
    drop(fixture.queue);
    let _ = fs::remove_dir_all(fixture.directory);
}

#[test]
fn actual_staged_publication_manifest_is_rejected_before_target_queue_install() {
    let fixture = queue_fixture("publication-manifest-reject");
    let source = RootedFs::new(&fixture.source_root).expect("source root should bind");
    let staging = create_video_staging_dir(&source).expect("staging should create");
    staging.preserve_for_recovery();
    let staged_video = staging.path().join("Episode.mkv");
    fs::write(&staged_video, b"pending publication payload").expect("staged media should write");
    let mut publication = staged_move_plan(
        staging.path(),
        &fixture.source_root,
        std::slice::from_ref(&staged_video),
        None,
        StagedPrimaryMediaKind::Video,
    )
    .expect("real staged publication plan should build");
    prepare_staged_publication_with_limits_and_sync(
        &source,
        &staging,
        &mut publication,
        StagedOverwriteCommit::None,
        VIDEO_STAGING_PUBLICATION_MAX_STEPS,
        VIDEO_STAGING_PUBLICATION_MANIFEST_LIMIT,
        &mut |_, directory| {
            directory.sync_all()?;
            Ok(())
        },
    )
    .expect("production publication code should write its real manifest");
    assert!(
        staging
            .path()
            .join(VIDEO_STAGING_PUBLICATION_MANIFEST_NAME)
            .is_file()
    );

    let id = "publication-manifest-reject-task";
    create_task_with_staging(&fixture, id, staging.path());
    let transfer = begin_transfer(&fixture, id);
    let error = fixture
        .queue
        .task_transfer_plan(&transfer)
        .expect_err("unresolved publication manifest must block plan creation");
    assert!(format!("{error:#}").contains("unresolved publication manifest"));
    assert!(
        !task_queue_record_path(&fixture.target_root, id).exists(),
        "an unresolved publication must not install a destination queue record"
    );
    assert!(
        fixture
            .queue
            .get(id)
            .unwrap()
            .unwrap()
            .destination_transfer
            .is_some()
    );
    fixture
        .queue
        .abort_task_destination_transfer(&transfer)
        .expect("failed transfer should abort cleanly");

    drop(staging);
    drop(fixture.queue);
    let _ = fs::remove_dir_all(fixture.directory);
}
