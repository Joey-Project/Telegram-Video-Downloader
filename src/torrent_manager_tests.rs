use super::*;
use crate::torrent_types::{
    TorrentExisting, TorrentIdentity, TorrentMagnet, TorrentMusicJobStatus, TorrentMusicStage,
};

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use tempfile::TempDir;

#[path = "torrent_admission_race_tests.rs"]
mod admission_race;
#[path = "torrent_batch_isolation_tests.rs"]
mod batch_isolation;
#[path = "torrent_card_recovery_tests.rs"]
mod card_recovery;
#[path = "torrent_metadata_retry_tests.rs"]
mod metadata_retry;
#[path = "torrent_refresh_recovery_tests.rs"]
mod refresh_recovery;
#[path = "torrent_worker_limit_tests.rs"]
mod worker_limit;

struct TestBackendState {
    visible_candidates: Vec<TorrentRemoteSnapshot>,
    accepted_on_submit: Option<TorrentRemoteSnapshot>,
    submitted_count: usize,
    submitted_bytes: Vec<Vec<u8>>,
    magnet_submissions: Vec<(String, PathBuf)>,
    default_save_path: PathBuf,
    exported_metainfo: Option<Vec<u8>>,
    export_failures: usize,
    export_count: usize,
    sent_cards: usize,
    updated_cards: Vec<TorrentSnapshot>,
    music_batches: Vec<(String, Vec<CompletedTorrentFile>)>,
    music_lookup_calls: usize,
    music_lookup_failures: usize,
    complete_music_status: bool,
}

struct TestBackend {
    raw_torrent: Vec<u8>,
    state: Mutex<TestBackendState>,
    next_card_id: AtomicI64,
}

impl TestBackend {
    fn new(
        raw_torrent: Vec<u8>,
        visible_candidates: Vec<TorrentRemoteSnapshot>,
        accepted_on_submit: Option<TorrentRemoteSnapshot>,
    ) -> Self {
        Self {
            raw_torrent,
            state: Mutex::new(TestBackendState {
                visible_candidates,
                accepted_on_submit,
                submitted_count: 0,
                submitted_bytes: Vec::new(),
                magnet_submissions: Vec::new(),
                default_save_path: std::env::temp_dir().join("mock-qbittorrent-default"),
                exported_metainfo: None,
                export_failures: 0,
                export_count: 0,
                sent_cards: 0,
                updated_cards: Vec::new(),
                music_batches: Vec::new(),
                music_lookup_calls: 0,
                music_lookup_failures: 0,
                complete_music_status: false,
            }),
            next_card_id: AtomicI64::new(100),
        }
    }

    fn submitted_count(&self) -> usize {
        self.state.lock().unwrap().submitted_count
    }

    fn music_batches(&self) -> Vec<(String, Vec<CompletedTorrentFile>)> {
        self.state.lock().unwrap().music_batches.clone()
    }

    fn configure_magnet(
        &self,
        default_save_path: PathBuf,
        exported_metainfo: Option<Vec<u8>>,
        export_failures: usize,
    ) {
        let mut state = self.state.lock().unwrap();
        state.default_save_path = default_save_path;
        state.exported_metainfo = exported_metainfo;
        state.export_failures = export_failures;
    }

    fn magnet_submissions(&self) -> Vec<(String, PathBuf)> {
        self.state.lock().unwrap().magnet_submissions.clone()
    }

    fn export_count(&self) -> usize {
        self.state.lock().unwrap().export_count
    }

    fn set_visible_candidate(&self, remote: TorrentRemoteSnapshot) {
        self.state.lock().unwrap().visible_candidates = vec![remote];
    }

    fn report_music_complete(&self) {
        self.state.lock().unwrap().complete_music_status = true;
    }

    fn fail_next_music_lookups(&self, count: usize) {
        self.state.lock().unwrap().music_lookup_failures = count;
    }

    fn music_lookup_calls(&self) -> usize {
        self.state.lock().unwrap().music_lookup_calls
    }
}

impl TorrentBackend for TestBackend {
    fn download_torrent(
        &self,
        _upload: TorrentUpload,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<u8>> {
        let bytes = self.raw_torrent.clone();
        Box::pin(async move { Ok(bytes) })
    }

    fn lookup(
        &self,
        _identity: TorrentIdentity,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentRemoteSnapshot>> {
        let candidates = self.state.lock().unwrap().visible_candidates.clone();
        Box::pin(async move { Ok(candidates) })
    }

    fn submit(
        &self,
        raw_torrent: Vec<u8>,
        _file_name: String,
    ) -> crate::torrent_types::TorrentFuture<'_, SubmitResult> {
        let mut state = self.state.lock().unwrap();
        state.submitted_count += 1;
        state.submitted_bytes.push(raw_torrent);
        if let Some(remote) = state.accepted_on_submit.take() {
            state.visible_candidates = vec![remote];
        }
        // Model an API timeout after qBittorrent accepted the add request. The manager must
        // persist intent and reconcile by identity after restart instead of posting again.
        Box::pin(async { anyhow::bail!("mocked response lost after acceptance") })
    }

    fn default_save_path(&self) -> crate::torrent_types::TorrentFuture<'_, PathBuf> {
        let path = self.state.lock().unwrap().default_save_path.clone();
        Box::pin(async move { Ok(path) })
    }

    fn submit_magnet(
        &self,
        uri: String,
        save_path: PathBuf,
    ) -> crate::torrent_types::TorrentFuture<'_, SubmitResult> {
        let mut state = self.state.lock().unwrap();
        state.magnet_submissions.push((uri, save_path));
        if let Some(remote) = state.accepted_on_submit.take() {
            state.visible_candidates = vec![remote];
        }
        Box::pin(async { anyhow::bail!("mocked magnet response lost after acceptance") })
    }

    fn export_metainfo(&self, _hash: String) -> crate::torrent_types::TorrentFuture<'_, Vec<u8>> {
        let mut state = self.state.lock().unwrap();
        state.export_count += 1;
        if state.export_failures > 0 {
            state.export_failures -= 1;
            return Box::pin(async { anyhow::bail!("mocked metadata is not ready") });
        }
        let bytes = state.exported_metainfo.clone();
        Box::pin(async move { bytes.ok_or_else(|| anyhow!("mock metadata is unavailable")) })
    }

    fn refresh(
        &self,
        _hash: String,
    ) -> crate::torrent_types::TorrentFuture<'_, TorrentRemoteSnapshot> {
        let remote = self
            .state
            .lock()
            .unwrap()
            .visible_candidates
            .first()
            .cloned();
        Box::pin(async move { remote.ok_or_else(|| anyhow!("mock torrent disappeared")) })
    }

    fn send_card(
        &self,
        _snapshot: TorrentSnapshot,
    ) -> crate::torrent_types::TorrentFuture<'_, i64> {
        self.state.lock().unwrap().sent_cards += 1;
        let id = self.next_card_id.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move { Ok(id) })
    }

    fn update_card(
        &self,
        snapshot: TorrentSnapshot,
    ) -> crate::torrent_types::TorrentFuture<'_, ()> {
        self.state.lock().unwrap().updated_cards.push(snapshot);
        Box::pin(async { Ok(()) })
    }

    fn enqueue_music(
        &self,
        _chat_id: i64,
        _user_id: i64,
        batch_id: String,
        files: Vec<CompletedTorrentFile>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        let mut state = self.state.lock().unwrap();
        state.music_batches.push((batch_id, files.clone()));
        let refs = files
            .into_iter()
            .enumerate()
            .map(|(index, file)| TorrentMusicJobRef {
                source_key: file.source_key,
                job_id: format!("music-job-{index}"),
            })
            .collect();
        Box::pin(async move { Ok(refs) })
    }

    fn lookup_music_jobs(
        &self,
        _chat_id: i64,
        _user_id: i64,
        source_keys: Vec<String>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        let mut state = self.state.lock().unwrap();
        state.music_lookup_calls += 1;
        if state.music_lookup_failures > 0 {
            state.music_lookup_failures -= 1;
            return Box::pin(async { anyhow::bail!("mock local Music lookup failure") });
        }
        let refs = state
            .music_batches
            .iter()
            .flat_map(|(_, files)| {
                files
                    .iter()
                    .enumerate()
                    .filter(|(_, file)| source_keys.contains(&file.source_key))
                    .map(|(index, file)| TorrentMusicJobRef {
                        source_key: file.source_key.clone(),
                        job_id: format!("music-job-{index}"),
                    })
            })
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(refs) })
    }

    fn music_status(
        &self,
        _chat_id: i64,
        job_ids: Vec<String>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentMusicJobStatus>> {
        let state = self.state.lock().unwrap();
        let statuses = if state.complete_music_status {
            state
                .music_batches
                .iter()
                .flat_map(|(_, files)| files.iter().enumerate())
                .map(|(index, file)| TorrentMusicJobStatus {
                    job_id: format!("music-job-{index}"),
                    source_key: file.source_key.clone(),
                    stage: TorrentMusicStage::Complete,
                    error: None,
                })
                .filter(|status| job_ids.contains(&status.job_id))
                .collect()
        } else {
            Vec::new()
        };
        Box::pin(async move { Ok(statuses) })
    }
}

struct Fixture {
    _directory: TempDir,
    state_dir: PathBuf,
    source_root: PathBuf,
    raw_torrent: Vec<u8>,
    metainfo: TorrentMetainfo,
    upload: TorrentUpload,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("create torrent test directory");
        let root = directory
            .path()
            .canonicalize()
            .expect("canonicalize torrent test directory");
        let state_dir = root.join("torrent-state");
        let source_root = root.join("qbit-downloads");
        let raw_torrent = two_file_torrent();
        let metainfo = parse_metainfo(&raw_torrent, DEFAULT_MAX_TORRENT_BYTES, DEFAULT_MAX_FILES)
            .expect("parse valid torrent fixture");
        let upload = TorrentUpload {
            update_id: 81,
            chat_id: 9001,
            message_id: 71,
            user_id: 7001,
            file_id: "mock-file-id".to_string(),
            file_unique_id: "mock-unique-file-id".to_string(),
            file_name: "fixture.torrent".to_string(),
            file_size: raw_torrent.len() as u64,
        };
        Self {
            _directory: directory,
            state_dir,
            source_root,
            raw_torrent,
            metainfo,
            upload,
        }
    }

    fn settings(&self) -> TorrentSettings {
        TorrentSettings {
            enabled: true,
            state_dir: self.state_dir.clone(),
            poll_seconds: 60,
            ..TorrentSettings::default()
        }
    }

    fn backend(
        &self,
        candidates: Vec<TorrentRemoteSnapshot>,
        accepted_on_submit: Option<TorrentRemoteSnapshot>,
    ) -> Arc<TestBackend> {
        Arc::new(TestBackend::new(
            self.raw_torrent.clone(),
            candidates,
            accepted_on_submit,
        ))
    }
}

fn two_file_torrent() -> Vec<u8> {
    two_file_torrent_with_private(false)
}

fn two_file_torrent_with_private(private: bool) -> Vec<u8> {
    let files = vec![
        dict(vec![
            (b"length".to_vec(), integer(4)),
            (
                b"path".to_vec(),
                list(vec![text("album"), text("song.mp3")]),
            ),
        ]),
        dict(vec![
            (b"length".to_vec(), integer(5)),
            (
                b"path".to_vec(),
                list(vec![text("album"), text("notes.txt")]),
            ),
        ]),
    ];
    let mut info_entries = vec![
        (b"files".to_vec(), list(files)),
        (b"name".to_vec(), text("release")),
        (b"piece length".to_vec(), integer(16_384)),
        (b"pieces".to_vec(), bencode_bytes(&[7; 20])),
    ];
    if private {
        info_entries.push((b"private".to_vec(), integer(1)));
    }
    let info = dict(info_entries);
    dict(vec![(b"info".to_vec(), info)])
}

fn remote_snapshot(
    metainfo: &TorrentMetainfo,
    save_path: &Path,
    file_progress: &[f64],
    progress: f64,
    amount_left: u64,
) -> TorrentRemoteSnapshot {
    TorrentRemoteSnapshot {
        hash: metainfo.identity.v1.clone().expect("v1 fixture hash"),
        info_hash_v1: metainfo.identity.v1.clone(),
        info_hash_v2: metainfo.identity.v2.clone(),
        name: metainfo.name.clone(),
        state: if amount_left == 0 {
            "uploading"
        } else {
            "downloading"
        }
        .to_string(),
        progress,
        save_path: save_path.to_path_buf(),
        content_path: None,
        total_size: metainfo.total_size,
        amount_left,
        files: metainfo
            .files
            .iter()
            .enumerate()
            .map(|(index, file)| RemoteTorrentFile {
                index: file.index,
                relative_path: file.relative_path.clone(),
                size: file.size,
                progress: file_progress[index],
                priority: 1,
            })
            .collect(),
    }
}

fn bencode_bytes(value: &[u8]) -> Vec<u8> {
    let mut output = format!("{}:", value.len()).into_bytes();
    output.extend_from_slice(value);
    output
}

fn text(value: &str) -> Vec<u8> {
    bencode_bytes(value.as_bytes())
}

fn integer(value: i64) -> Vec<u8> {
    format!("i{value}e").into_bytes()
}

fn list(values: Vec<Vec<u8>>) -> Vec<u8> {
    let mut output = vec![b'l'];
    for value in values {
        output.extend(value);
    }
    output.push(b'e');
    output
}

fn dict(mut entries: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<u8> {
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    let mut output = vec![b'd'];
    for (key, value) in entries {
        output.extend(bencode_bytes(&key));
        output.extend(value);
    }
    output.push(b'e');
    output
}

async fn parse_task(manager: &TorrentManager, upload: TorrentUpload) -> String {
    let queued = manager.enqueue(upload).expect("enqueue torrent");
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    queued.task_id
}

fn magnet_upload(fixture: &Fixture, update_id: i64, message_id: i64) -> TorrentMagnet {
    let v1 = fixture
        .metainfo
        .identity
        .v1
        .as_deref()
        .expect("fixture v1 identity");
    TorrentMagnet {
        update_id,
        chat_id: fixture.upload.chat_id,
        message_id,
        user_id: fixture.upload.chat_id,
        uri: format!("magnet:?xt=urn:btih:{v1}&dn=release&so=0"),
    }
}

fn existing_source(_fixture: &Fixture, hash: String, user_id: i64) -> TorrentExisting {
    TorrentExisting {
        chat_id: user_id,
        user_id,
        message_id: 900,
        hash,
    }
}

fn alternate_single_file_torrent() -> Vec<u8> {
    let info = dict(vec![
        (b"length".to_vec(), integer(3)),
        (b"name".to_vec(), text("other.mp3")),
        (b"piece length".to_vec(), integer(16_384)),
        (b"pieces".to_vec(), bencode_bytes(&[8; 20])),
    ]);
    dict(vec![(b"info".to_vec(), info)])
}

#[tokio::test]
async fn existing_or_ambiguous_qbittorrent_candidates_are_never_submitted() {
    let fixture = Fixture::new();
    let existing = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[0.5, 1.0], 0.7, 2);
    let backend = fixture.backend(vec![existing.clone()], None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;
    manager.process_task_once_for_test(&task_id).await;
    assert_eq!(backend.submitted_count(), 0);
    assert_eq!(
        manager
            .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
            .unwrap()
            .phase,
        TorrentPhase::Downloading
    );

    let duplicate_backend = fixture.backend(vec![existing.clone(), existing], None);
    let duplicate_manager = TorrentManager::open(
        TorrentSettings {
            state_dir: fixture.state_dir.join("ambiguous"),
            ..fixture.settings()
        },
        duplicate_backend.clone(),
    )
    .expect("open separate ambiguous manager");
    let task_id = parse_task(&duplicate_manager, fixture.upload.clone()).await;
    duplicate_manager.process_task_once_for_test(&task_id).await;
    let snapshot = duplicate_manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    assert_eq!(snapshot.phase, TorrentPhase::Ambiguous);
    assert_eq!(duplicate_backend.submitted_count(), 0);
}

#[tokio::test]
async fn existing_catalog_adoption_is_read_only_verified_and_replay_safe() {
    let fixture = Fixture::new();
    let private_raw = two_file_torrent_with_private(true);
    let private_meta = parse_metainfo(&private_raw, DEFAULT_MAX_TORRENT_BYTES, DEFAULT_MAX_FILES)
        .expect("parse private torrent fixture");
    let existing = remote_snapshot(&private_meta, &fixture.source_root, &[0.5, 1.0], 0.7, 2);
    let expected_hash = existing.hash.clone();
    let expected_path = existing.save_path.clone();
    let backend = Arc::new(TestBackend::new(private_raw.clone(), vec![existing], None));
    backend.configure_magnet(fixture.source_root.clone(), Some(private_raw), 0);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");

    let first = manager
        .adopt_existing(
            existing_source(&fixture, expected_hash.clone(), fixture.upload.user_id),
            "catalog:request-1".to_string(),
        )
        .await
        .expect("adopt existing qBittorrent task");
    assert!(first.created);
    assert_eq!(first.snapshot.upload_message_id, 900);
    assert_eq!(first.snapshot.phase, TorrentPhase::DownloadingMetainfo);
    assert!(first.snapshot.files.is_empty());
    let initial_generation = manager
        .store
        .task(&first.task_id)
        .unwrap()
        .unwrap()
        .card_generation;

    let replay = manager
        .adopt_existing(
            existing_source(&fixture, expected_hash.clone(), fixture.upload.user_id),
            "catalog:request-1".to_string(),
        )
        .await
        .expect("replay catalog request");
    assert_eq!(replay.task_id, first.task_id);
    assert_eq!(
        manager
            .store
            .task(&replay.task_id)
            .unwrap()
            .unwrap()
            .card_generation,
        initial_generation
    );

    let next_generation = manager
        .adopt_existing(
            existing_source(&fixture, expected_hash.clone(), fixture.upload.user_id),
            "catalog:request-2".to_string(),
        )
        .await
        .expect("open another catalog generation");
    assert_eq!(next_generation.task_id, first.task_id);
    assert_eq!(
        manager
            .store
            .task(&next_generation.task_id)
            .unwrap()
            .unwrap()
            .card_generation,
        initial_generation + 1
    );

    assert!(
        manager
            .adopt_existing(
                existing_source(&fixture, expected_hash.clone(), fixture.upload.user_id + 1),
                "catalog:wrong-owner".to_string()
            )
            .await
            .is_err()
    );

    manager.process_task_once_for_test(&first.task_id).await;
    manager.process_task_once_for_test(&first.task_id).await;
    let verified = manager
        .snapshot(
            fixture.upload.user_id,
            fixture.upload.user_id,
            &first.task_id,
        )
        .expect("read verified task");
    let stored = manager
        .store
        .task(&first.task_id)
        .expect("read task state")
        .expect("task exists");
    assert_eq!(verified.phase, TorrentPhase::Downloading);
    assert_eq!(stored.remote.as_ref().unwrap().save_path, expected_path);
    assert!(stored.metainfo.as_ref().unwrap().private);
    assert_eq!(backend.submitted_count(), 0);
    assert!(backend.magnet_submissions().is_empty());

    let replayed_verified = manager
        .adopt_existing(
            existing_source(&fixture, expected_hash.clone(), fixture.upload.user_id),
            "catalog:request-2".to_string(),
        )
        .await
        .expect("replay the current catalog generation");
    assert_eq!(
        replayed_verified.snapshot.inventory_revision,
        verified.inventory_revision
    );
    assert_eq!(
        replayed_verified.snapshot.selection_revision,
        verified.selection_revision
    );

    let card_id = verified.card_message_id.expect("delivered task card");
    let selected = manager
        .apply_action(
            fixture.upload.user_id,
            fixture.upload.user_id,
            card_id,
            &first.task_id,
            verified.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .expect("select one file");
    let confirmed = manager
        .apply_action(
            fixture.upload.user_id,
            fixture.upload.user_id,
            card_id,
            &first.task_id,
            selected.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("confirm the current inventory");
    let old_inventory_revision = confirmed.inventory_revision;
    let old_selection_revision = confirmed.selection_revision;

    let moved_path = fixture.source_root.join("moved-downloads");
    let moved_remote = remote_snapshot(&private_meta, &moved_path, &[1.0, 1.0], 1.0, 0);
    backend.set_visible_candidate(moved_remote);
    let refreshed = manager
        .adopt_existing(
            existing_source(&fixture, expected_hash, fixture.upload.user_id),
            "catalog:request-3".to_string(),
        )
        .await
        .expect("reopen card after qBittorrent moved the task");
    assert_eq!(refreshed.snapshot.phase, TorrentPhase::Completed);
    assert_eq!(refreshed.snapshot.progress, 1.0);
    assert!(refreshed.snapshot.inventory_revision > old_inventory_revision);
    assert!(refreshed.snapshot.selection_revision > old_selection_revision);
    assert_eq!(
        manager
            .store
            .task(&first.task_id)
            .unwrap()
            .unwrap()
            .remote
            .unwrap()
            .save_path,
        moved_path
    );
    let stored_after_refresh = manager.store.task(&first.task_id).unwrap().unwrap();
    let old_confirmation = stored_after_refresh.imports.get(&0).unwrap();
    assert_ne!(
        old_confirmation.confirmed_inventory_revision,
        stored_after_refresh.inventory_revision
    );
    manager.process_task_once_for_test(&first.task_id).await;
    manager.process_task_once_for_test(&first.task_id).await;
    assert!(
        backend.music_batches().is_empty(),
        "a previous inventory confirmation cannot enqueue files after revalidation"
    );
}

#[tokio::test]
async fn existing_catalog_missing_or_unverified_metadata_stays_unselectable() {
    let fixture = Fixture::new();
    let missing_backend = fixture.backend(Vec::new(), None);
    let missing_manager = TorrentManager::open(
        TorrentSettings {
            state_dir: fixture.state_dir.join("missing-existing"),
            ..fixture.settings()
        },
        missing_backend,
    )
    .expect("open missing task manager");
    assert!(
        missing_manager
            .adopt_existing(
                existing_source(
                    &fixture,
                    fixture.metainfo.identity.v1.clone().unwrap(),
                    fixture.upload.user_id
                ),
                "catalog:missing".to_string()
            )
            .await
            .is_err()
    );

    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[0.0, 0.0], 0.0, 9);
    let pending_backend = fixture.backend(vec![remote], None);
    pending_backend.configure_magnet(
        fixture.source_root.clone(),
        Some(fixture.raw_torrent.clone()),
        1,
    );
    let pending_manager = TorrentManager::open(
        TorrentSettings {
            state_dir: fixture.state_dir.join("pending-existing"),
            ..fixture.settings()
        },
        pending_backend.clone(),
    )
    .expect("open pending metadata manager");
    let queued = pending_manager
        .adopt_existing(
            existing_source(
                &fixture,
                fixture.metainfo.identity.v1.clone().unwrap(),
                fixture.upload.user_id,
            ),
            "catalog:pending".to_string(),
        )
        .await
        .expect("adopt task waiting for metadata");
    pending_manager
        .process_task_once_for_test(&queued.task_id)
        .await;
    pending_manager
        .process_task_once_for_test(&queued.task_id)
        .await;
    let waiting = pending_manager
        .snapshot(
            fixture.upload.user_id,
            fixture.upload.user_id,
            &queued.task_id,
        )
        .expect("read waiting task");
    assert_eq!(waiting.phase, TorrentPhase::DownloadingMetainfo);
    assert!(waiting.files.is_empty());
    assert_eq!(waiting.selected_count, 0);
    assert_eq!(pending_backend.submitted_count(), 0);

    let wrong_export = fixture.backend(
        vec![remote_snapshot(
            &fixture.metainfo,
            &fixture.source_root,
            &[1.0, 1.0],
            1.0,
            0,
        )],
        None,
    );
    wrong_export.configure_magnet(
        fixture.source_root.clone(),
        Some(alternate_single_file_torrent()),
        0,
    );
    let wrong_manager = TorrentManager::open(
        TorrentSettings {
            state_dir: fixture.state_dir.join("wrong-existing-metadata"),
            ..fixture.settings()
        },
        wrong_export.clone(),
    )
    .expect("open identity mismatch manager");
    let wrong = wrong_manager
        .adopt_existing(
            existing_source(
                &fixture,
                fixture.metainfo.identity.v1.clone().unwrap(),
                fixture.upload.user_id,
            ),
            "catalog:wrong-export".to_string(),
        )
        .await
        .expect("adopt task before export validation");
    wrong_manager
        .process_task_once_for_test(&wrong.task_id)
        .await;
    wrong_manager
        .process_task_once_for_test(&wrong.task_id)
        .await;
    let rejected = wrong_manager
        .snapshot(
            fixture.upload.user_id,
            fixture.upload.user_id,
            &wrong.task_id,
        )
        .expect("read rejected task");
    assert_eq!(rejected.phase, TorrentPhase::Invalid);
    assert!(rejected.files.is_empty());
    assert_eq!(wrong_export.submitted_count(), 0);
}

#[tokio::test]
async fn verified_existing_catalog_task_can_import_and_refresh_music_status() {
    let fixture = Fixture::new();
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    let hash = remote.hash.clone();
    let backend = fixture.backend(vec![remote], None);
    backend.configure_magnet(
        fixture.source_root.clone(),
        Some(fixture.raw_torrent.clone()),
        0,
    );
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let queued = manager
        .adopt_existing(
            existing_source(&fixture, hash, fixture.upload.user_id),
            "catalog:music-import".to_string(),
        )
        .await
        .expect("adopt existing task");
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    let ready = manager
        .snapshot(
            fixture.upload.user_id,
            fixture.upload.user_id,
            &queued.task_id,
        )
        .expect("read verified task");
    let card_id = ready.card_message_id.expect("task card delivered");
    let selected = manager
        .apply_action(
            fixture.upload.user_id,
            fixture.upload.user_id,
            card_id,
            &queued.task_id,
            ready.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .expect("select music file");
    manager
        .apply_action(
            fixture.upload.user_id,
            fixture.upload.user_id,
            card_id,
            &queued.task_id,
            selected.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("confirm selected file");

    manager.process_task_once_for_test(&queued.task_id).await;
    assert_eq!(backend.music_batches().len(), 1);
    backend.report_music_complete();
    manager.refresh_music_status_for_test(&queued.task_id).await;
    let imported = manager
        .snapshot(
            fixture.upload.user_id,
            fixture.upload.user_id,
            &queued.task_id,
        )
        .expect("read completed import");
    assert_eq!(
        imported.files[0].music_stage,
        Some(TorrentMusicStage::Complete)
    );
    assert_eq!(backend.submitted_count(), 0);
}

#[test]
fn queue_state_without_the_existing_source_field_still_loads() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend).expect("open manager");
    manager
        .enqueue(fixture.upload.clone())
        .expect("enqueue upload");
    let mut old_state = serde_json::to_value(manager.store.snapshot().expect("read state"))
        .expect("serialize current state");
    for task in old_state["tasks"].as_object_mut().unwrap().values_mut() {
        task.as_object_mut().unwrap().remove("existing");
    }
    let restored: StoredState =
        serde_json::from_value(old_state).expect("load state written before existing was added");
    assert!(restored.tasks.values().all(|task| task.existing.is_none()));
}

#[tokio::test]
async fn existing_catalog_task_survives_manager_restart() {
    let fixture = Fixture::new();
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[0.0, 0.0], 0.0, 9);
    let hash = remote.hash.clone();
    let backend = fixture.backend(vec![remote], None);
    let settings = fixture.settings();
    let manager = TorrentManager::open(settings.clone(), backend.clone()).expect("open manager");
    let queued = manager
        .adopt_existing(
            existing_source(&fixture, hash, fixture.upload.user_id),
            "catalog:restart".to_string(),
        )
        .await
        .expect("adopt existing catalog task");

    let state = manager.store.snapshot().expect("read persisted state");
    validate_state(&state, &manager.settings, &manager.store.metainfo_dir)
        .expect("validate state containing exactly one existing source");
    drop(manager);

    let restarted =
        TorrentManager::open(settings, backend).expect("reopen manager with existing task");
    let restored = restarted
        .snapshot(
            fixture.upload.user_id,
            fixture.upload.user_id,
            &queued.task_id,
        )
        .expect("read restored catalog task");
    assert_eq!(restored.upload_message_id, 900);
    assert_eq!(restored.phase, TorrentPhase::DownloadingMetainfo);
    assert!(restored.files.is_empty());
}

#[tokio::test]
async fn restart_reconciles_unknown_add_outcome_without_a_second_post() {
    let fixture = Fixture::new();
    let accepted = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[0.5, 1.0], 0.7, 2);
    let backend = fixture.backend(Vec::new(), Some(accepted));
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;
    manager.process_task_once_for_test(&task_id).await;
    let after_submit = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    assert_eq!(after_submit.phase, TorrentPhase::ReconciliationPending);
    assert_eq!(backend.submitted_count(), 1);
    drop(manager);

    let restarted =
        TorrentManager::open(fixture.settings(), backend.clone()).expect("reopen manager");
    restarted.process_task_once_for_test(&task_id).await;
    let reconciled = restarted
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    assert_eq!(reconciled.phase, TorrentPhase::Downloading);
    assert_eq!(
        backend.submitted_count(),
        1,
        "recovery must only perform a read-only lookup"
    );
}

#[tokio::test]
async fn magnet_unknown_submit_reconciles_after_restart_without_a_second_post() {
    let fixture = Fixture::new();
    let accepted = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    let backend = fixture.backend(Vec::new(), Some(accepted));
    backend.configure_magnet(
        fixture.source_root.clone(),
        Some(fixture.raw_torrent.clone()),
        0,
    );
    let settings = fixture.settings();
    let manager = TorrentManager::open(settings.clone(), backend.clone()).expect("open manager");
    let queued = manager
        .enqueue_magnet(magnet_upload(&fixture, 301, 401))
        .expect("admit valid magnet source");
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    let submissions = backend.magnet_submissions();
    assert_eq!(submissions.len(), 1);
    assert!(
        !submissions[0].0.contains("so="),
        "partial selection is removed"
    );
    assert_eq!(
        submissions[0].1,
        fixture
            .source_root
            .join("Telegram Downloads")
            .join(&queued.task_id)
    );
    let pending = manager
        .snapshot(
            fixture.upload.chat_id,
            fixture.upload.chat_id,
            &queued.task_id,
        )
        .expect("read pending submission state");
    assert_eq!(pending.phase, TorrentPhase::ReconciliationPending);
    drop(manager);

    let restarted = TorrentManager::open(settings, backend.clone()).expect("reopen manager");
    restarted.process_task_once_for_test(&queued.task_id).await;
    let reconciled = restarted
        .snapshot(
            fixture.upload.chat_id,
            fixture.upload.chat_id,
            &queued.task_id,
        )
        .expect("read reconciled magnet task");
    assert_eq!(reconciled.phase, TorrentPhase::Completed);
    assert_eq!(reconciled.files.len(), 2);
    assert_eq!(backend.magnet_submissions().len(), 1);
}

#[tokio::test]
async fn magnet_metadata_wait_and_mismatched_export_never_expose_selectable_files() {
    let fixture = Fixture::new();
    let existing = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[0.0, 0.0], 0.0, 9);
    let backend = fixture.backend(vec![existing.clone()], None);
    backend.configure_magnet(
        fixture.source_root.clone(),
        Some(fixture.raw_torrent.clone()),
        1,
    );
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let queued = manager
        .enqueue_magnet(magnet_upload(&fixture, 302, 402))
        .expect("admit existing magnet");
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    let waiting = manager
        .snapshot(
            fixture.upload.chat_id,
            fixture.upload.chat_id,
            &queued.task_id,
        )
        .expect("read metadata waiting state");
    assert_eq!(waiting.phase, TorrentPhase::DownloadingMetainfo);
    assert_eq!(backend.export_count(), 1);
    assert!(
        waiting.files.is_empty(),
        "unverified metadata has no file inventory"
    );
    assert!(
        manager
            .apply_action(
                fixture.upload.chat_id,
                fixture.upload.chat_id,
                waiting.card_message_id.unwrap(),
                &queued.task_id,
                waiting.inventory_revision,
                TorrentAction::SetFile {
                    index: 0,
                    selected: true
                },
            )
            .is_err()
    );
    assert!(backend.magnet_submissions().is_empty());

    let mismatch_backend = fixture.backend(vec![existing], None);
    mismatch_backend.configure_magnet(
        fixture.source_root.clone(),
        Some(alternate_single_file_torrent()),
        0,
    );
    let mismatch_manager = TorrentManager::open(
        TorrentSettings {
            state_dir: fixture.state_dir.join("mismatched-export"),
            ..fixture.settings()
        },
        mismatch_backend.clone(),
    )
    .expect("open mismatch manager");
    let mismatch = mismatch_manager
        .enqueue_magnet(magnet_upload(&fixture, 303, 403))
        .expect("admit another magnet task");
    mismatch_manager
        .process_task_once_for_test(&mismatch.task_id)
        .await;
    mismatch_manager
        .process_task_once_for_test(&mismatch.task_id)
        .await;
    let rejected = mismatch_manager
        .snapshot(
            fixture.upload.chat_id,
            fixture.upload.chat_id,
            &mismatch.task_id,
        )
        .expect("read rejected mismatch task");
    assert_eq!(rejected.phase, TorrentPhase::Invalid);
    assert_eq!(mismatch_backend.export_count(), 1);
    assert!(rejected.files.is_empty());
    assert!(mismatch_backend.magnet_submissions().is_empty());
}

#[tokio::test]
async fn same_magnet_hash_from_different_messages_is_associated_as_two_origins() {
    let fixture = Fixture::new();
    let existing = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    let backend = fixture.backend(vec![existing], None);
    backend.configure_magnet(
        fixture.source_root.clone(),
        Some(fixture.raw_torrent.clone()),
        0,
    );
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let first = manager
        .enqueue_magnet(magnet_upload(&fixture, 304, 404))
        .expect("admit first origin");
    let second = manager
        .enqueue_magnet(magnet_upload(&fixture, 305, 405))
        .expect("admit second message origin");
    assert_ne!(first.task_id, second.task_id);
    assert!(first.created && second.created);

    for task_id in [&first.task_id, &second.task_id] {
        manager.process_task_once_for_test(task_id).await;
        manager.process_task_once_for_test(task_id).await;
    }
    let listed = manager
        .list(fixture.upload.chat_id)
        .expect("list both origins");
    assert_eq!(listed.len(), 2);
    assert!(
        listed
            .iter()
            .all(|task| task.phase == TorrentPhase::Completed)
    );
    assert!(backend.magnet_submissions().is_empty());
}

#[tokio::test]
async fn owner_and_selection_revisions_protect_import_confirmation() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend).expect("open manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;
    let partial = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[0.5, 1.0], 0.7, 2);
    manager
        .attach_remote(&task_id, partial.clone())
        .expect("attach remote snapshot");
    let snapshot = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    assert!(
        manager
            .apply_action(
                fixture.upload.chat_id,
                fixture.upload.user_id + 1,
                snapshot.card_message_id.unwrap(),
                &task_id,
                snapshot.inventory_revision,
                TorrentAction::SetFile {
                    index: 0,
                    selected: true
                },
            )
            .is_err()
    );

    let selected = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            snapshot.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .expect("select the Music file");
    manager
        .attach_remote(
            &task_id,
            remote_snapshot(
                &fixture.metainfo,
                &fixture.source_root,
                &[0.75, 1.0],
                0.8,
                1,
            ),
        )
        .expect("progress refresh keeps inventory stable");
    let after_progress = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    assert_eq!(
        after_progress.selection_revision,
        selected.selection_revision
    );
    let imported = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            selected.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("progress-only updates do not stale selection confirmation");
    let cleared = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            imported.inventory_revision,
            TorrentAction::ClearSelection,
        )
        .expect("clear selection");
    assert!(
        manager
            .apply_action(
                fixture.upload.chat_id,
                fixture.upload.user_id,
                snapshot.card_message_id.unwrap(),
                &task_id,
                imported.selection_revision,
                TorrentAction::ImportSelected,
            )
            .is_err(),
        "a second device's selection change must stale the confirmation"
    );
    assert!(cleared.selection_revision > imported.selection_revision);
    assert!(
        manager
            .apply_action(
                fixture.upload.chat_id,
                fixture.upload.user_id,
                snapshot.card_message_id.unwrap(),
                &task_id,
                0,
                TorrentAction::Refresh,
            )
            .is_ok(),
        "Refresh can recover a card with a stale inventory revision"
    );
}

#[tokio::test]
async fn review_selection_paginates_and_resets_on_selection_or_browse_changes() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend).expect("open manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;
    let metainfo = TorrentMetainfo {
        identity: fixture.metainfo.identity.clone(),
        raw_sha256: fixture.metainfo.raw_sha256.clone(),
        name: fixture.metainfo.name.clone(),
        files: (0..9)
            .map(|index| MetainfoFile {
                index,
                relative_path: PathBuf::from("album").join(format!("track-{index:02}.mp3")),
                size: 1,
                is_padding: false,
            })
            .collect(),
        total_size: 9,
        private: false,
    };
    manager
        .store
        .transact(|state| {
            let task = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("torrent test task disappeared"))?;
            task.metainfo = Some(metainfo.clone());
            Ok(((), true))
        })
        .expect("seed the review inventory");
    manager
        .attach_remote(
            &task_id,
            remote_snapshot(&metainfo, &fixture.source_root, &[1.0; 9], 1.0, 0),
        )
        .expect("attach the completed review inventory");

    let mut snapshot = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    for index in 0..9 {
        snapshot = manager
            .apply_action(
                fixture.upload.chat_id,
                fixture.upload.user_id,
                snapshot.card_message_id.unwrap(),
                &task_id,
                snapshot.inventory_revision,
                TorrentAction::SetFile {
                    index,
                    selected: true,
                },
            )
            .expect("select a reviewable audio file");
    }
    snapshot = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            snapshot.selection_revision,
            TorrentAction::ReviewSelection { page: 2 },
        )
        .expect("open the third review page");
    assert!(snapshot.review_selection);
    assert_eq!(snapshot.cursor_page, 2);
    assert!(
        manager
            .apply_action(
                fixture.upload.chat_id,
                fixture.upload.user_id,
                snapshot.card_message_id.unwrap(),
                &task_id,
                snapshot.selection_revision,
                TorrentAction::ReviewSelection { page: 3 },
            )
            .is_err(),
        "pages beyond the selected-file inventory are rejected"
    );

    let selection_revision = snapshot.selection_revision;
    manager
        .attach_remote(
            &task_id,
            remote_snapshot(&metainfo, &fixture.source_root, &[0.9; 9], 0.9, 1),
        )
        .expect("progress-only refresh preserves the inventory");
    snapshot = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    assert!(snapshot.review_selection);
    assert_eq!(snapshot.cursor_page, 2);
    assert_eq!(snapshot.selection_revision, selection_revision);

    snapshot = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            snapshot.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: false,
            },
        )
        .expect("changing the selected set returns to its first page");
    assert!(!snapshot.review_selection);
    assert_eq!(snapshot.cursor_page, 0);
    assert_eq!(snapshot.selected_count, 8);

    snapshot = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            snapshot.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .expect("restore the original selection");
    snapshot = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            snapshot.selection_revision,
            TorrentAction::ReviewSelection { page: 2 },
        )
        .expect("reopen the third review page");
    snapshot = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            snapshot.inventory_revision,
            TorrentAction::Browse {
                folder_id: 0,
                page: 0,
            },
        )
        .expect("return to normal file browsing");
    assert!(!snapshot.review_selection);
    assert_eq!(snapshot.cursor_folder_id, 0);
    assert_eq!(snapshot.cursor_page, 0);
}

#[tokio::test]
async fn selected_subset_waits_for_completion_then_enqueues_once_without_deleting_payloads() {
    let fixture = Fixture::new();
    fs::create_dir_all(fixture.source_root.join("album")).expect("create mock qBittorrent folder");
    let payload = fixture.source_root.join("album/song.mp3");
    fs::write(&payload, b"mock music bytes").expect("create mock payload");
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;
    let mut metainfo = fixture.metainfo.clone();
    metainfo.files[1].relative_path = PathBuf::from("album/other.mp3");
    manager
        .store
        .transact(|state| {
            let task = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("torrent test task disappeared"))?;
            task.metainfo = Some(metainfo.clone());
            Ok(((), true))
        })
        .expect("add a second supported track for reconfirmation coverage");
    manager
        .attach_remote(
            &task_id,
            remote_snapshot(&metainfo, &fixture.source_root, &[0.5, 1.0], 0.7, 2),
        )
        .expect("attach downloading torrent");
    let snapshot = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    let selected = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            snapshot.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .expect("select only the audio file");
    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            selected.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("confirm selection");
    manager.process_task_once_for_test(&task_id).await;
    assert!(
        backend.music_batches().is_empty(),
        "an incomplete selected file must wait"
    );

    manager
        .attach_remote(
            &task_id,
            remote_snapshot(&metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0),
        )
        .expect("attach completed torrent");
    manager.process_task_once_for_test(&task_id).await;
    manager.process_task_once_for_test(&task_id).await;
    let batches = backend.music_batches();
    assert_eq!(
        batches.len(),
        1,
        "stable source keys keep replay idempotent"
    );
    assert_eq!(
        batches[0].1.len(),
        1,
        "unselected files must not be imported"
    );
    assert_eq!(
        batches[0].1[0].relative_path,
        PathBuf::from("album/song.mp3")
    );
    assert_eq!(batches[0].1[0].source_root, fixture.source_root);
    assert!(
        payload.exists(),
        "torrent payload remains owned by qBittorrent"
    );

    manager
        .store
        .transact(|state| {
            let task = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("torrent test task disappeared"))?;
            let import = task
                .imports
                .get_mut(&0)
                .ok_or_else(|| anyhow!("first Music import disappeared"))?;
            import.music_stage = Some(TorrentMusicStage::Complete);
            task.phase = aggregate_phase(task);
            touch(task);
            Ok(((), true))
        })
        .expect("mark the existing Music job complete");
    let completed = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .expect("read completed Music job");
    assert!(!completed.files[0].needs_reconfirm);
    let replayed = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            completed.card_message_id.unwrap(),
            &task_id,
            completed.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("replay the completed import confirmation");
    assert_eq!(replayed.phase, TorrentPhase::Imported);
    assert_eq!(
        backend.music_batches().len(),
        1,
        "replay keeps the existing Music job"
    );

    let changed_selection = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            completed.card_message_id.unwrap(),
            &task_id,
            completed.inventory_revision,
            TorrentAction::SetFile {
                index: 1,
                selected: true,
            },
        )
        .expect("select a second track");
    assert!(!changed_selection.files[0].needs_reconfirm);
    let confirmed_new_track = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            completed.card_message_id.unwrap(),
            &task_id,
            changed_selection.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("confirm the newly selected track");
    assert_eq!(confirmed_new_track.phase, TorrentPhase::ImportRequested);
    assert_eq!(backend.music_batches().len(), 1);
}

#[tokio::test]
async fn clearing_unadmitted_import_restores_completed_torrent_phase() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend).expect("open manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;
    manager
        .attach_remote(
            &task_id,
            remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0),
        )
        .expect("attach completed torrent");
    let snapshot = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    let selected = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            snapshot.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .expect("select a completed audio file");
    let pending = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            selected.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("confirm import without admitting a Music job");
    assert_eq!(pending.phase, TorrentPhase::ImportRequested);
    assert_eq!(pending.import_count, 0);

    let cleared = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.unwrap(),
            &task_id,
            pending.inventory_revision,
            TorrentAction::ClearSelection,
        )
        .expect("clear the unadmitted import request");
    assert_eq!(cleared.phase, TorrentPhase::Completed);
    assert_eq!(cleared.selected_count, 0);
    assert_eq!(cleared.import_count, 0);
}

#[tokio::test]
async fn snapshot_import_count_includes_only_completed_music_jobs() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend).expect("open manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;

    manager
        .store
        .transact(|state| {
            let task = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("torrent test task disappeared"))?;
            for (index, stage) in [
                TorrentMusicStage::Pending,
                TorrentMusicStage::Complete,
                TorrentMusicStage::Failed,
                TorrentMusicStage::Cancelled,
                TorrentMusicStage::OutcomeUnknown,
            ]
            .into_iter()
            .enumerate()
            {
                let source_key = format!("music-source-{index}");
                task.imports.insert(
                    index as u32,
                    StoredImport {
                        source_key: source_key.clone(),
                        confirmed_inventory_revision: task.inventory_revision,
                        confirmed_selection_revision: task.selection_revision,
                        enqueue_intent: false,
                        batch_id: Some("music-batch".into()),
                        music_job: Some(TorrentMusicJobRef {
                            source_key,
                            job_id: format!("music-job-{index}"),
                        }),
                        music_stage: Some(stage),
                        error: None,
                        attempts: 0,
                        next_retry_at: 0,
                    },
                );
            }
            Ok(((), true))
        })
        .expect("store Music stages");

    let snapshot = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .expect("read Music import snapshot");
    assert_eq!(snapshot.import_count, 1);
}

#[tokio::test]
async fn root_folder_selection_handles_nearly_five_thousand_files_once_per_index() {
    const FILE_COUNT: usize = 5_000;
    const AUDIO_COUNT: usize = 4_990;
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend).expect("open manager");
    let queued = manager
        .enqueue(fixture.upload.clone())
        .expect("enqueue large inventory task");
    let files = (0..FILE_COUNT)
        .map(|index| {
            let extension = if index < AUDIO_COUNT { "mp3" } else { "txt" };
            MetainfoFile {
                index: index as u32,
                relative_path: PathBuf::from(format!("folder-{index:05}"))
                    .join(format!("file.{extension}")),
                size: 1,
                is_padding: index == 0,
            }
        })
        .collect::<Vec<_>>();
    let metainfo = TorrentMetainfo {
        identity: fixture.metainfo.identity.clone(),
        raw_sha256: "0".repeat(64),
        name: "large-inventory".to_string(),
        files,
        total_size: FILE_COUNT as u64,
        private: false,
    };
    manager
        .store
        .transact(|state| {
            let task = state
                .tasks
                .get_mut(&queued.task_id)
                .ok_or_else(|| anyhow!("torrent test task disappeared"))?;
            task.metainfo = Some(metainfo);
            task.inventory_revision = 1;
            task.selection_revision = 1;
            task.card_send_state = CardSendState::Delivered;
            task.card_message_id = Some(100);
            task.phase = TorrentPhase::SelectingFiles;
            Ok(((), true))
        })
        .expect("seed a bounded 5000-file inventory");

    let snapshot = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            100,
            &queued.task_id,
            1,
            TorrentAction::SetFolder {
                folder_id: 0,
                selected: true,
            },
        )
        .expect("bulk select the root folder");
    assert_eq!(snapshot.files.len(), FILE_COUNT);
    assert_eq!(snapshot.folders.len(), FILE_COUNT + 1);
    assert_eq!(snapshot.folders[0].file_count as usize, FILE_COUNT);
    assert_eq!(snapshot.selected_count, AUDIO_COUNT - 1);
    let first_page = crate::torrent_runtime::widget_view(&snapshot);
    assert_eq!(first_page.page_count, 1_250);
    assert_eq!(first_page.entries.len(), 4);
    assert!(crate::torrent_widget::render_torrent_widget(&first_page).is_ok());
    assert!(
        snapshot
            .files
            .iter()
            .filter(|file| !file.supported_music)
            .all(|file| !file.selected)
    );
}

#[cfg(target_os = "macos")]
#[test]
fn state_path_accepts_only_known_system_aliases_and_rejects_other_symlink_ancestors() {
    let path = Path::new("/tmp").join(format!("torrent-state-alias-{}", std::process::id()));
    let resolved = create_private_directory_path(&path).expect("resolve macOS /tmp alias");
    assert!(resolved.starts_with("/private/tmp"));
    fs::remove_dir_all(&resolved).expect("remove alias fixture");

    let directory = tempfile::tempdir().expect("create symlink fixture");
    let actual = directory.path().join("actual");
    let alias = directory.path().join("alias");
    fs::create_dir(&actual).expect("create actual parent");
    std::os::unix::fs::symlink(&actual, &alias).expect("create untrusted alias");
    assert!(create_private_directory_path(&alias.join("state")).is_err());
}
