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
