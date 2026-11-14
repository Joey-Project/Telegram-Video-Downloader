use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

struct FailDownloadBackend {
    inner: Arc<TestBackend>,
    failures_left: Mutex<usize>,
    download_calls: AtomicUsize,
}

impl FailDownloadBackend {
    fn new(inner: Arc<TestBackend>, failures: usize) -> Self {
        Self {
            inner,
            failures_left: Mutex::new(failures),
            download_calls: AtomicUsize::new(0),
        }
    }

    fn download_calls(&self) -> usize {
        self.download_calls.load(Ordering::SeqCst)
    }
}

impl TorrentBackend for FailDownloadBackend {
    fn download_torrent(
        &self,
        upload: TorrentUpload,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<u8>> {
        self.download_calls.fetch_add(1, Ordering::SeqCst);
        let fail = {
            let mut left = self.failures_left.lock().unwrap();
            if *left == 0 {
                false
            } else {
                *left -= 1;
                true
            }
        };
        if fail {
            Box::pin(async { anyhow::bail!("mocked upload retrieval failure") })
        } else {
            self.inner.download_torrent(upload)
        }
    }

    fn lookup(
        &self,
        identity: TorrentIdentity,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentRemoteSnapshot>> {
        self.inner.lookup(identity)
    }

    fn submit(
        &self,
        raw: Vec<u8>,
        name: String,
    ) -> crate::torrent_types::TorrentFuture<'_, SubmitResult> {
        self.inner.submit(raw, name)
    }

    fn refresh(
        &self,
        hash: String,
    ) -> crate::torrent_types::TorrentFuture<'_, TorrentRemoteSnapshot> {
        self.inner.refresh(hash)
    }

    fn send_card(&self, snapshot: TorrentSnapshot) -> crate::torrent_types::TorrentFuture<'_, i64> {
        self.inner.send_card(snapshot)
    }

    fn update_card(
        &self,
        snapshot: TorrentSnapshot,
    ) -> crate::torrent_types::TorrentFuture<'_, ()> {
        self.inner.update_card(snapshot)
    }

    fn enqueue_music(
        &self,
        chat_id: i64,
        user_id: i64,
        batch_id: String,
        files: Vec<CompletedTorrentFile>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        self.inner.enqueue_music(chat_id, user_id, batch_id, files)
    }

    fn music_status(
        &self,
        chat_id: i64,
        job_ids: Vec<String>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentMusicJobStatus>> {
        self.inner.music_status(chat_id, job_ids)
    }
}

fn make_card_redraw_due(manager: &TorrentManager, task_id: &str) {
    manager
        .store
        .transact(|state| {
            let task = state
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            task.card_render_pending = true;
            task.next_card_update_at = unix_seconds();
            Ok(((), true))
        })
        .expect("schedule card redraw");
}

fn make_retry_due(manager: &TorrentManager, task_id: &str) {
    manager
        .store
        .transact(|state| {
            let task = state
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            task.next_action_at = unix_seconds();
            Ok(((), true))
        })
        .expect("make prerequisite retry due");
}

#[tokio::test]
async fn upload_metadata_retry_waits_through_card_redraw() {
    let fixture = Fixture::new();
    let inner = fixture.backend(Vec::new(), None);
    let backend = Arc::new(FailDownloadBackend::new(inner.clone(), 1));
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let queued = manager
        .enqueue(fixture.upload.clone())
        .expect("enqueue upload");

    manager.process_task_once_for_test(&queued.task_id).await;
    let card = manager.store.task(&queued.task_id).unwrap().unwrap();
    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            card.card_message_id.expect("card was delivered"),
            &queued.task_id,
            card.inventory_revision,
            TorrentAction::Refresh,
        )
        .expect("request one explicit refresh");
    manager.process_task_once_for_test(&queued.task_id).await;
    assert_eq!(backend.download_calls(), 1);
    let deferred = manager.store.task(&queued.task_id).unwrap().unwrap();
    assert!(deferred.next_action_at > unix_seconds());
    assert!(
        !deferred.manual_refresh,
        "failed metadata refresh must consume the one-shot manual trigger"
    );
    let redraw_count_before = inner.state.lock().unwrap().updated_cards.len();

    make_card_redraw_due(&manager, &queued.task_id);
    assert!(task_has_action_due(
        &manager.store.task(&queued.task_id).unwrap().unwrap(),
        unix_seconds()
    ));
    manager.process_task_once_for_test(&queued.task_id).await;
    assert_eq!(
        backend.download_calls(),
        1,
        "redraw must not retry retrieval"
    );
    assert_eq!(
        inner.state.lock().unwrap().updated_cards.len(),
        redraw_count_before + 1,
        "due card redraw must still update the card"
    );

    make_retry_due(&manager, &queued.task_id);
    manager.process_task_once_for_test(&queued.task_id).await;
    assert_eq!(backend.download_calls(), 2, "retrieval resumes when due");
    assert!(
        manager
            .store
            .task(&queued.task_id)
            .unwrap()
            .unwrap()
            .metainfo
            .is_some()
    );
}

#[tokio::test]
async fn magnet_metadata_export_waits_through_card_redraw() {
    let fixture = Fixture::new();
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    let backend = fixture.backend(Vec::new(), Some(remote));
    backend.configure_magnet(
        fixture.source_root.clone(),
        Some(fixture.raw_torrent.clone()),
        1,
    );
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let queued = manager
        .enqueue_magnet(magnet_upload(&fixture, 93, 73))
        .expect("enqueue magnet");

    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    assert_eq!(backend.export_count(), 1);
    let deferred = manager.store.task(&queued.task_id).unwrap().unwrap();
    assert!(deferred.next_action_at > unix_seconds());
    assert!(deferred.metainfo.is_none());
    let redraw_count_before = backend.state.lock().unwrap().updated_cards.len();

    make_card_redraw_due(&manager, &queued.task_id);
    assert!(task_has_action_due(
        &manager.store.task(&queued.task_id).unwrap().unwrap(),
        unix_seconds()
    ));
    manager.process_task_once_for_test(&queued.task_id).await;
    assert_eq!(backend.export_count(), 1, "redraw must not retry export");
    assert_eq!(backend.magnet_submissions().len(), 1);
    assert_eq!(
        backend.state.lock().unwrap().updated_cards.len(),
        redraw_count_before + 1,
        "due card redraw must still update the card"
    );

    make_retry_due(&manager, &queued.task_id);
    manager.process_task_once_for_test(&queued.task_id).await;
    assert_eq!(
        backend.export_count(),
        2,
        "metadata export resumes when due"
    );
    assert!(
        manager
            .store
            .task(&queued.task_id)
            .unwrap()
            .unwrap()
            .metainfo
            .is_some()
    );
}
