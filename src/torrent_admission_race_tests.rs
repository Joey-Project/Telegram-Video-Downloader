use super::*;

use crate::torrent_types::TorrentFuture;
use std::sync::Mutex as StdMutex;
use tokio::sync::oneshot;

struct BlockingEnqueueBackend {
    inner: Arc<TestBackend>,
    entered: StdMutex<Option<oneshot::Sender<()>>>,
    release: StdMutex<Option<oneshot::Receiver<()>>>,
}

impl TorrentBackend for BlockingEnqueueBackend {
    fn download_torrent(&self, upload: TorrentUpload) -> TorrentFuture<'_, Vec<u8>> {
        self.inner.download_torrent(upload)
    }

    fn lookup(&self, identity: TorrentIdentity) -> TorrentFuture<'_, Vec<TorrentRemoteSnapshot>> {
        self.inner.lookup(identity)
    }

    fn submit(&self, raw_torrent: Vec<u8>, file_name: String) -> TorrentFuture<'_, SubmitResult> {
        self.inner.submit(raw_torrent, file_name)
    }

    fn refresh(&self, hash: String) -> TorrentFuture<'_, TorrentRemoteSnapshot> {
        self.inner.refresh(hash)
    }

    fn send_card(&self, snapshot: TorrentSnapshot) -> TorrentFuture<'_, i64> {
        self.inner.send_card(snapshot)
    }

    fn update_card(&self, snapshot: TorrentSnapshot) -> TorrentFuture<'_, ()> {
        self.inner.update_card(snapshot)
    }

    fn enqueue_music(
        &self,
        chat_id: i64,
        user_id: i64,
        batch_id: String,
        files: Vec<CompletedTorrentFile>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        let entered = self.entered.lock().unwrap().take();
        let release = self.release.lock().unwrap().take();
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            if let Some(release) = release {
                let _ = release.await;
            }
            inner.enqueue_music(chat_id, user_id, batch_id, files).await
        })
    }

    fn music_status(
        &self,
        chat_id: i64,
        job_ids: Vec<String>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobStatus>> {
        self.inner.music_status(chat_id, job_ids)
    }

    fn lookup_music_jobs(
        &self,
        chat_id: i64,
        user_id: i64,
        source_keys: Vec<String>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        self.inner.lookup_music_jobs(chat_id, user_id, source_keys)
    }
}

async fn prepare_completed_import(
    fixture: &Fixture,
    manager: &Arc<TorrentManager>,
) -> (String, TorrentSnapshot) {
    let task_id = parse_task(manager, fixture.upload.clone()).await;
    manager
        .attach_remote(
            &task_id,
            remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0),
        )
        .expect("attach completed torrent inventory");
    let initial = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    let selected = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            initial.card_message_id.expect("task card should be sent"),
            &task_id,
            initial.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .expect("select completed audio file");
    let confirmed = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            initial.card_message_id.expect("task card should be sent"),
            &task_id,
            selected.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("confirm selected audio file");
    (task_id, confirmed)
}

#[tokio::test]
async fn clearing_selection_before_enqueue_intent_does_not_admit_music() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let (task_id, confirmed) = prepare_completed_import(&fixture, &manager).await;
    let stale_task = manager.store.task(&task_id).unwrap().unwrap();

    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            confirmed.card_message_id.expect("task card should be sent"),
            &task_id,
            confirmed.inventory_revision,
            TorrentAction::ClearSelection,
        )
        .expect("clear the selection before enqueue intent");

    manager.enqueue_ready_imports(stale_task).await;

    assert!(
        backend.music_batches().is_empty(),
        "a stale captured selection must not be sent to Music"
    );
    let current = manager.store.task(&task_id).unwrap().unwrap();
    assert!(current.selected_indexes.is_empty());
    assert!(current.imports.is_empty());
}

#[tokio::test]
async fn clearing_selection_after_enqueue_intent_keeps_submitted_job_tracked_once() {
    let fixture = Fixture::new();
    let inner = fixture.backend(Vec::new(), None);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let backend = Arc::new(BlockingEnqueueBackend {
        inner: Arc::clone(&inner),
        entered: StdMutex::new(Some(entered_tx)),
        release: StdMutex::new(Some(release_rx)),
    });
    let backend_port: Arc<dyn TorrentBackend> = backend;
    let manager = TorrentManager::open(fixture.settings(), backend_port).expect("open manager");
    let (task_id, confirmed) = prepare_completed_import(&fixture, &manager).await;
    let captured_task = manager.store.task(&task_id).unwrap().unwrap();

    let worker_manager = Arc::clone(&manager);
    let worker = tokio::spawn(async move {
        worker_manager.enqueue_ready_imports(captured_task).await;
    });
    entered_rx
        .await
        .expect("enqueue should reach Music after durable intent");

    let intent_state = manager.store.task(&task_id).unwrap().unwrap();
    assert!(intent_state.imports[&0].enqueue_intent);
    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            confirmed.card_message_id.expect("task card should be sent"),
            &task_id,
            confirmed.inventory_revision,
            TorrentAction::ClearSelection,
        )
        .expect("clear selection after enqueue intent is durable");
    let cleared = manager.store.task(&task_id).unwrap().unwrap();
    assert!(cleared.selected_indexes.is_empty());
    assert!(cleared.imports[&0].enqueue_intent);
    let source_key = cleared.imports[&0].source_key.clone();

    release_tx.send(()).expect("release Music enqueue barrier");
    worker.await.expect("enqueue worker should finish");

    let tracked = manager.store.task(&task_id).unwrap().unwrap();
    let import = &tracked.imports[&0];
    assert_eq!(import.source_key, source_key);
    assert!(import.music_job.is_some());
    assert!(!import.enqueue_intent);
    assert_eq!(inner.music_batches().len(), 1);

    manager.enqueue_ready_imports(tracked).await;
    assert_eq!(
        inner.music_batches().len(),
        1,
        "cleared selection must not enqueue the same source a second time"
    );
}

#[tokio::test]
async fn inventory_change_after_enqueue_intent_keeps_submitted_job_tracked() {
    let fixture = Fixture::new();
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    let inner = fixture.backend(vec![remote.clone()], None);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let backend = Arc::new(BlockingEnqueueBackend {
        inner: Arc::clone(&inner),
        entered: StdMutex::new(Some(entered_tx)),
        release: StdMutex::new(Some(release_rx)),
    });
    let backend_port: Arc<dyn TorrentBackend> = backend;
    let manager = TorrentManager::open(fixture.settings(), backend_port).expect("open manager");
    let (task_id, _) = prepare_completed_import(&fixture, &manager).await;
    let captured_task = manager.store.task(&task_id).unwrap().unwrap();

    let worker_manager = Arc::clone(&manager);
    let worker = tokio::spawn(async move {
        worker_manager.enqueue_ready_imports(captured_task).await;
    });
    entered_rx
        .await
        .expect("enqueue should reach Music after durable intent");

    let intent_state = manager.store.task(&task_id).unwrap().unwrap();
    let expected_source_key = intent_state.imports[&0].source_key.clone();
    let mut refreshed_remote = remote;
    refreshed_remote.save_path = fixture.source_root.join("refreshed-download-root");
    manager
        .attach_remote(&task_id, refreshed_remote)
        .expect("attach changed qBittorrent download root");
    let changed = manager.store.task(&task_id).unwrap().unwrap();
    assert!(changed.inventory_revision > intent_state.inventory_revision);

    release_tx.send(()).expect("release Music enqueue barrier");
    worker.await.expect("enqueue worker should finish");

    let tracked = manager.store.task(&task_id).unwrap().unwrap();
    let import = &tracked.imports[&0];
    assert_eq!(import.source_key, expected_source_key);
    assert!(import.music_job.is_some());
    assert!(!import.enqueue_intent);
    assert_eq!(inner.music_batches().len(), 1);
}

#[tokio::test]
async fn explicit_torrent_refresh_reconciles_terminal_music_job_once() {
    let fixture = Fixture::new();
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    let backend = fixture.backend(vec![remote.clone()], None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let (task_id, confirmed) = prepare_completed_import(&fixture, &manager).await;
    manager.process_task_once_for_test(&task_id).await;
    assert_eq!(backend.music_batches().len(), 1);

    backend.report_music_complete();
    manager
        .store
        .transact(|state| {
            let task = state.tasks.get_mut(&task_id).expect("task remains stored");
            let import = task.imports.get_mut(&0).expect("Music import exists");
            import.music_stage = Some(TorrentMusicStage::Failed);
            import.error = Some("Music previously failed".into());
            task.next_music_status_at = None;
            Ok(((), true))
        })
        .expect("simulate cached failed Music status");
    let cached = manager.store.task(&task_id).unwrap().unwrap();
    assert!(!has_pending_music_jobs(&cached));

    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            confirmed.card_message_id.expect("task card should be sent"),
            &task_id,
            confirmed.inventory_revision,
            TorrentAction::Refresh,
        )
        .expect("explicit Refresh should request one Music status read");
    manager.process_task_once_for_test(&task_id).await;

    let refreshed_remote = manager.store.task(&task_id).unwrap().unwrap();
    assert!(!refreshed_remote.manual_refresh);
    assert!(
        task_has_action_due(&refreshed_remote, unix_seconds()),
        "the due Music status read must remain dispatchable for a terminal cached job"
    );
    manager.process_task_once_for_test(&task_id).await;

    let current = manager.store.task(&task_id).unwrap().unwrap();
    assert_eq!(
        current.imports[&0].music_stage,
        Some(TorrentMusicStage::Complete)
    );
    assert!(current.next_music_status_at.is_none());
    let mut terminal_poll_check = current.clone();
    terminal_poll_check.card_render_pending = false;
    terminal_poll_check.next_action_at = u64::MAX;
    assert!(
        !task_has_action_due(&terminal_poll_check, unix_seconds()),
        "a terminal Music job should not keep scheduling status polls"
    );
    assert_eq!(backend.music_batches().len(), 1);
}

fn persist_orphan_intent(manager: &TorrentManager, task_id: &str, batch_id: &str) {
    manager
        .store
        .transact(|state| {
            let task = state.tasks.get_mut(task_id).expect("task remains stored");
            let request = task.imports.get_mut(&0).expect("selected import exists");
            request.music_job = None;
            request.music_stage = None;
            request.enqueue_intent = true;
            request.batch_id = Some(batch_id.to_string());
            request.next_retry_at = 0;
            Ok(())
        })
        .expect("persist interrupted Music enqueue intent");
}

fn clear_import_selection(fixture: &Fixture, manager: &TorrentManager, task_id: &str) {
    let snapshot = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, task_id)
        .unwrap();
    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.expect("task card should be sent"),
            task_id,
            snapshot.inventory_revision,
            TorrentAction::ClearSelection,
        )
        .expect("clear selection while enqueue intent is durable");
}

fn two_audio_fixture() -> Fixture {
    let mut fixture = Fixture::new();
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
                list(vec![text("album"), text("other.mp3")]),
            ),
        ]),
    ];
    let info = dict(vec![
        (b"files".to_vec(), list(files)),
        (b"name".to_vec(), text("album")),
        (b"piece length".to_vec(), integer(16_384)),
        (b"pieces".to_vec(), bencode_bytes(&[8; 20])),
    ]);
    fixture.raw_torrent = dict(vec![(b"info".to_vec(), info)]);
    fixture.metainfo = parse_metainfo(
        &fixture.raw_torrent,
        DEFAULT_MAX_TORRENT_BYTES,
        DEFAULT_MAX_FILES,
    )
    .expect("parse two-audio-file torrent");
    fixture.upload.file_size = fixture.raw_torrent.len() as u64;
    fixture
}

#[tokio::test]
async fn reopened_orphan_intent_binds_existing_music_job_without_reenqueue() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let (task_id, _) = prepare_completed_import(&fixture, &manager).await;
    let ready = manager.store.task(&task_id).unwrap().unwrap();
    manager.enqueue_ready_imports(ready).await;
    let admitted = manager.store.task(&task_id).unwrap().unwrap();
    let batch_id = admitted.imports[&0]
        .batch_id
        .clone()
        .expect("Music admission should retain its batch ID");
    persist_orphan_intent(&manager, &task_id, &batch_id);
    clear_import_selection(&fixture, &manager, &task_id);
    drop(manager);

    let reopened =
        TorrentManager::open(fixture.settings(), backend.clone()).expect("reopen manager");
    reopened.process_task_once_for_test(&task_id).await;
    let tracked = reopened.store.task(&task_id).unwrap().unwrap();
    let request = &tracked.imports[&0];
    assert!(tracked.selected_indexes.is_empty());
    assert!(request.music_job.is_some());
    assert!(!request.enqueue_intent);
    assert_eq!(backend.music_batches().len(), 1);
    assert_eq!(backend.music_lookup_calls(), 1);

    reopened.process_task_once_for_test(&task_id).await;
    let idle = reopened.store.task(&task_id).unwrap().unwrap();
    assert!(!task_has_action_due(&idle, unix_seconds()));
    assert_eq!(backend.music_batches().len(), 1);
}

#[tokio::test]
async fn reopened_missing_orphan_intent_is_retired_without_admission() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let (task_id, _) = prepare_completed_import(&fixture, &manager).await;
    persist_orphan_intent(&manager, &task_id, "missing-music-batch");
    clear_import_selection(&fixture, &manager, &task_id);
    drop(manager);

    let reopened =
        TorrentManager::open(fixture.settings(), backend.clone()).expect("reopen manager");
    reopened.process_task_once_for_test(&task_id).await;
    let reconciled = reopened.store.task(&task_id).unwrap().unwrap();
    assert!(reconciled.selected_indexes.is_empty());
    assert!(reconciled.imports.is_empty());
    assert!(backend.music_batches().is_empty());
    assert_eq!(backend.music_lookup_calls(), 1);
}

#[tokio::test]
async fn reopened_missing_selected_intent_requires_fresh_confirmation() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let (task_id, _) = prepare_completed_import(&fixture, &manager).await;
    persist_orphan_intent(&manager, &task_id, "missing-selected-music-batch");
    drop(manager);

    let reopened =
        TorrentManager::open(fixture.settings(), backend.clone()).expect("reopen manager");
    reopened.process_task_once_for_test(&task_id).await;
    let reconciled = reopened.store.task(&task_id).unwrap().unwrap();
    let request = &reconciled.imports[&0];
    assert!(reconciled.selected_indexes.contains(&0));
    assert!(!request.enqueue_intent);
    assert!(request.batch_id.is_none());
    assert_eq!(request.confirmed_selection_revision, 0);
    assert!(request.error.is_some());
    assert!(!reopened.has_ready_imports(&reconciled));
    assert!(backend.music_batches().is_empty());
}

#[tokio::test]
async fn orphan_music_lookup_failure_is_backed_off_without_worker_hotloop() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let (task_id, _) = prepare_completed_import(&fixture, &manager).await;
    persist_orphan_intent(&manager, &task_id, "lookup-failure-music-batch");
    clear_import_selection(&fixture, &manager, &task_id);
    backend.fail_next_music_lookups(1);
    drop(manager);

    let reopened =
        TorrentManager::open(fixture.settings(), backend.clone()).expect("reopen manager");
    reopened.process_task_once_for_test(&task_id).await;
    let deferred = reopened.store.task(&task_id).unwrap().unwrap();
    assert!(deferred.imports[&0].enqueue_intent);
    assert!(deferred.imports[&0].next_retry_at > unix_seconds());
    assert_eq!(backend.music_lookup_calls(), 1);

    reopened.process_task_once_for_test(&task_id).await;
    let idle = reopened.store.task(&task_id).unwrap().unwrap();
    assert_eq!(backend.music_lookup_calls(), 1);
    assert!(!task_has_action_due(&idle, unix_seconds()));
    assert!(backend.music_batches().is_empty());
}

#[tokio::test]
async fn recovered_intent_does_not_block_newly_completed_selected_file() {
    let fixture = two_audio_fixture();
    let incomplete_remote =
        remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 0.5], 0.8, 1);
    let backend = fixture.backend(vec![incomplete_remote.clone()], None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;
    manager
        .attach_remote(&task_id, incomplete_remote)
        .expect("attach one-complete one-pending file inventory");
    let initial = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    let card_id = initial.card_message_id.expect("task card should be sent");
    let selected_first = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            card_id,
            &task_id,
            initial.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .expect("select first completed file");
    let selected_both = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            card_id,
            &task_id,
            selected_first.inventory_revision,
            TorrentAction::SetFile {
                index: 1,
                selected: true,
            },
        )
        .expect("select second pending file");
    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            card_id,
            &task_id,
            selected_both.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("confirm both selected files");
    manager
        .enqueue_ready_imports(manager.store.task(&task_id).unwrap().unwrap())
        .await;
    assert_eq!(backend.music_batches().len(), 1);
    let first_admission = manager.store.task(&task_id).unwrap().unwrap();
    let batch_id = first_admission.imports[&0]
        .batch_id
        .clone()
        .expect("first file should have a durable batch ID");
    manager
        .store
        .transact(|state| {
            let task = state.tasks.get_mut(&task_id).expect("task remains stored");
            let request = task.imports.get_mut(&0).expect("first import exists");
            request.music_job = None;
            request.music_stage = None;
            request.enqueue_intent = true;
            request.next_retry_at = 0;
            task.next_refresh_at = Some(0);
            Ok(())
        })
        .expect("persist in-flight first-file intent before crash");
    drop(manager);

    backend.set_visible_candidate(remote_snapshot(
        &fixture.metainfo,
        &fixture.source_root,
        &[1.0, 1.0],
        1.0,
        0,
    ));
    let reopened =
        TorrentManager::open(fixture.settings(), backend.clone()).expect("reopen manager");
    reopened.process_task_once_for_test(&task_id).await;
    reopened.process_task_once_for_test(&task_id).await;

    let reconciled = reopened.store.task(&task_id).unwrap().unwrap();
    assert!(reconciled.imports[&0].music_job.is_some());
    assert!(!reconciled.imports[&0].enqueue_intent);
    assert!(reconciled.imports[&1].music_job.is_none());
    assert_eq!(backend.music_lookup_calls(), 1);
    assert_eq!(backend.music_batches().len(), 1);

    reopened.process_task_once_for_test(&task_id).await;
    let batches = backend.music_batches();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].0, batch_id);
    assert_eq!(
        batches[0]
            .1
            .iter()
            .map(|file| file.source_key.as_str())
            .collect::<Vec<_>>(),
        vec![reconciled.imports[&0].source_key.as_str()]
    );
    assert_eq!(
        batches[1]
            .1
            .iter()
            .map(|file| file.source_key.as_str())
            .collect::<Vec<_>>(),
        vec![reconciled.imports[&1].source_key.as_str()]
    );
    assert_ne!(batches[0].0, batches[1].0);
}
