use super::*;
use crate::torrent_types::{TorrentIdentity, TorrentMusicJobStatus};

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use tempfile::TempDir;

struct TestBackendState {
    visible_candidates: Vec<TorrentRemoteSnapshot>,
    accepted_on_submit: Option<TorrentRemoteSnapshot>,
    submitted_count: usize,
    submitted_bytes: Vec<Vec<u8>>,
    sent_cards: usize,
    updated_cards: Vec<TorrentSnapshot>,
    music_batches: Vec<(String, Vec<CompletedTorrentFile>)>,
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
                sent_cards: 0,
                updated_cards: Vec::new(),
                music_batches: Vec::new(),
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

    fn music_status(
        &self,
        _chat_id: i64,
        _job_ids: Vec<String>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentMusicJobStatus>> {
        Box::pin(async { Ok(Vec::new()) })
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
    let info = dict(vec![
        (b"files".to_vec(), list(files)),
        (b"name".to_vec(), text("release")),
        (b"piece length".to_vec(), integer(16_384)),
        (b"pieces".to_vec(), bencode_bytes(&[7; 20])),
    ]);
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
