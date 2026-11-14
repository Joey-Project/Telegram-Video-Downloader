use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Semaphore;

struct BlockingMusicBackend {
    inner: TestBackend,
    release: Semaphore,
    in_music: AtomicUsize,
    peak_music: AtomicUsize,
    started_music: AtomicUsize,
}

impl BlockingMusicBackend {
    fn new(inner: TestBackend) -> Self {
        Self {
            inner,
            release: Semaphore::new(0),
            in_music: AtomicUsize::new(0),
            peak_music: AtomicUsize::new(0),
            started_music: AtomicUsize::new(0),
        }
    }

    fn started_count(&self) -> usize {
        self.started_music.load(Ordering::SeqCst)
    }

    fn peak_music(&self) -> usize {
        self.peak_music.load(Ordering::SeqCst)
    }
}

impl TorrentBackend for BlockingMusicBackend {
    fn download_torrent(
        &self,
        upload: TorrentUpload,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<u8>> {
        self.inner.download_torrent(upload)
    }

    fn lookup(
        &self,
        identity: TorrentIdentity,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentRemoteSnapshot>> {
        self.inner.lookup(identity)
    }

    fn submit(
        &self,
        raw_torrent: Vec<u8>,
        file_name: String,
    ) -> crate::torrent_types::TorrentFuture<'_, SubmitResult> {
        self.inner.submit(raw_torrent, file_name)
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
        Box::pin(async move {
            let active = self.in_music.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_music.fetch_max(active, Ordering::SeqCst);
            self.started_music.fetch_add(1, Ordering::SeqCst);
            match self.release.acquire().await {
                Ok(permit) => permit.forget(),
                Err(_) => {
                    self.in_music.fetch_sub(1, Ordering::SeqCst);
                    anyhow::bail!("Music gate closed unexpectedly")
                }
            }
            self.in_music.fetch_sub(1, Ordering::SeqCst);
            self.inner
                .enqueue_music(chat_id, user_id, batch_id, files)
                .await
        })
    }

    fn music_status(
        &self,
        chat_id: i64,
        job_ids: Vec<String>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentMusicJobStatus>> {
        self.inner.music_status(chat_id, job_ids)
    }
}

struct ReleaseMusicGateOnDrop(Arc<BlockingMusicBackend>);

impl Drop for ReleaseMusicGateOnDrop {
    fn drop(&mut self) {
        self.0.release.close();
    }
}

async fn drive_until_music_count(
    manager: &Arc<TorrentManager>,
    backend: &BlockingMusicBackend,
    target: usize,
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while backend.started_count() < target {
            manager.dispatch_ready().await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("ready tasks should reach the blocked Music backend");
}

#[tokio::test]
async fn dispatch_caps_total_active_workers_while_intents_are_blocked() {
    let fixture = Fixture::new();
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    let inner = TestBackend::new(fixture.raw_torrent.clone(), vec![remote], None);
    let backend = Arc::new(BlockingMusicBackend::new(inner));
    let _release_gate_on_failure = ReleaseMusicGateOnDrop(backend.clone());
    let manager =
        TorrentManager::open(fixture.settings(), backend.clone()).expect("open torrent manager");
    let chat_id = fixture.upload.chat_id;
    let user_id = fixture.upload.user_id;

    // Prepare more ready imports than the worker cap. Each fixture task is parsed and attached
    // synchronously so only the dispatcher's background workers contend for the mock gate.
    for index in 0..40_i64 {
        let mut upload = fixture.upload.clone();
        upload.update_id += index;
        upload.message_id += index;
        let queued = manager.enqueue(upload).expect("enqueue task");
        manager.process_task_once_for_test(&queued.task_id).await;
        manager.process_task_once_for_test(&queued.task_id).await;
        manager.process_task_once_for_test(&queued.task_id).await;

        let ready = manager
            .snapshot(chat_id, user_id, &queued.task_id)
            .expect("read task snapshot");
        let card_id = ready.card_message_id.expect("task card was delivered");
        let selected = manager
            .apply_action(
                chat_id,
                user_id,
                card_id,
                &queued.task_id,
                ready.inventory_revision,
                TorrentAction::SetFile {
                    index: 0,
                    selected: true,
                },
            )
            .expect("select completed music file");
        manager
            .apply_action(
                chat_id,
                user_id,
                card_id,
                &queued.task_id,
                selected.selection_revision,
                TorrentAction::ImportSelected,
            )
            .expect("confirm selected file");
    }

    drive_until_music_count(&manager, &backend, MAX_ACTIVE_WORKERS).await;
    assert_eq!(backend.started_count(), MAX_ACTIVE_WORKERS);
    assert_eq!(
        manager.active_workers.lock().await.len(),
        MAX_ACTIVE_WORKERS
    );
    assert_eq!(backend.peak_music(), MAX_ACTIVE_WORKERS);

    // Keep the blocked calls' task IDs out of the due list while the remaining tasks stay due,
    // reproducing the case where a per-dispatch `.take(16)` would admit another worker batch.
    let active_ids = manager
        .active_workers
        .lock()
        .await
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    manager
        .store
        .transact(|state| {
            for task_id in &active_ids {
                let task = state
                    .tasks
                    .get_mut(task_id)
                    .ok_or_else(|| anyhow!("active test task disappeared"))?;
                task.next_action_at = u64::MAX;
                task.next_refresh_at = None;
                task.manual_refresh = false;
                task.card_render_pending = false;
                task.next_music_status_at = None;
                for request in task.imports.values_mut() {
                    if request.music_job.is_none() {
                        request.next_retry_at = u64::MAX;
                    }
                }
            }
            Ok(((), true))
        })
        .expect("hold active task deadlines while testing admission");

    let due = manager
        .store
        .ready_task_ids(unix_seconds())
        .expect("read due test tasks");
    assert!(active_ids.iter().all(|task_id| !due.contains(task_id)));
    assert!(!due.is_empty(), "queued tasks should remain due");

    manager.dispatch_ready().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(backend.started_count(), MAX_ACTIVE_WORKERS);
    assert_eq!(
        manager.active_workers.lock().await.len(),
        MAX_ACTIVE_WORKERS
    );
    assert_eq!(backend.peak_music(), MAX_ACTIVE_WORKERS);

    backend.release.add_permits(MAX_ACTIVE_WORKERS);
    drive_until_music_count(&manager, &backend, MAX_ACTIVE_WORKERS + 1).await;
    assert!(backend.peak_music() <= MAX_ACTIVE_WORKERS);

    // Release any newly admitted mock calls and wait for their worker wrappers to return.
    backend.release.add_permits(64);
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if manager.active_workers.lock().await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("completed workers should release their slots");
    assert!(backend.peak_music() <= MAX_ACTIVE_WORKERS);
}
