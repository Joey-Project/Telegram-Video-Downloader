use super::*;

struct BatchFailureBackend {
    inner: Arc<TestBackend>,
    calls: Mutex<Vec<Vec<String>>>,
}

impl BatchFailureBackend {
    fn calls(&self) -> Vec<Vec<String>> {
        self.calls.lock().unwrap().clone()
    }
}

impl TorrentBackend for BatchFailureBackend {
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
        let paths = files
            .iter()
            .map(|file| file.relative_path.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        self.calls.lock().unwrap().push(paths.clone());
        if files.len() > 1 || paths.first().is_some_and(|path| path.ends_with("bad.mp3")) {
            return Box::pin(async { anyhow::bail!("mock Music rejected this batch") });
        }
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

fn two_music_file_fixture() -> Fixture {
    let mut fixture = Fixture::new();
    let files = list(vec![
        dict(vec![
            (b"length".to_vec(), integer(4)),
            (b"path".to_vec(), list(vec![text("album"), text("bad.mp3")])),
        ]),
        dict(vec![
            (b"length".to_vec(), integer(5)),
            (
                b"path".to_vec(),
                list(vec![text("album"), text("good.m4a")]),
            ),
        ]),
    ]);
    let info = dict(vec![
        (b"files".to_vec(), files),
        (b"name".to_vec(), text("release")),
        (b"piece length".to_vec(), integer(16_384)),
        (b"pieces".to_vec(), bencode_bytes(&[7; 20])),
    ]);
    fixture.raw_torrent = dict(vec![(b"info".to_vec(), info)]);
    fixture.metainfo = parse_metainfo(
        &fixture.raw_torrent,
        DEFAULT_MAX_TORRENT_BYTES,
        DEFAULT_MAX_FILES,
    )
    .expect("parse two-audio-file fixture");
    fixture.upload.file_size = fixture.raw_torrent.len() as u64;
    fixture
}

async fn confirm_both_music_files(fixture: &Fixture, manager: &Arc<TorrentManager>) -> String {
    let task_id = parse_task(manager, fixture.upload.clone()).await;
    manager
        .attach_remote(
            &task_id,
            remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0),
        )
        .expect("attach completed inventory");
    let initial = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .unwrap();
    let card_id = initial.card_message_id.expect("task card should be sent");
    let first = manager
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
        .expect("select bad fixture file");
    let second = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            card_id,
            &task_id,
            first.inventory_revision,
            TorrentAction::SetFile {
                index: 1,
                selected: true,
            },
        )
        .expect("select healthy fixture file");
    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            card_id,
            &task_id,
            second.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("confirm both selected files");
    task_id
}

#[tokio::test]
async fn failed_batch_retries_one_source_at_a_time_and_admits_healthy_file() {
    let fixture = two_music_file_fixture();
    let inner = fixture.backend(Vec::new(), None);
    let backend = Arc::new(BatchFailureBackend {
        inner: inner.clone(),
        calls: Mutex::new(Vec::new()),
    });
    let backend_port: Arc<dyn TorrentBackend> = backend.clone();
    let manager = TorrentManager::open(fixture.settings(), backend_port).expect("open manager");
    let task_id = confirm_both_music_files(&fixture, &manager).await;

    manager
        .enqueue_ready_imports(manager.store.task(&task_id).unwrap().unwrap())
        .await;
    let initial_batches = backend.calls();
    assert_eq!(initial_batches.len(), 1);
    assert_eq!(initial_batches[0].len(), 2, "first attempt remains batched");

    manager
        .store
        .transact(|state| {
            let task = state.tasks.get_mut(&task_id).expect("task is stored");
            for request in task.imports.values_mut() {
                request.next_retry_at = 0;
            }
            Ok(())
        })
        .expect("make failed sources due");
    manager
        .enqueue_ready_imports(manager.store.task(&task_id).unwrap().unwrap())
        .await;
    manager
        .enqueue_ready_imports(manager.store.task(&task_id).unwrap().unwrap())
        .await;

    let batches = backend.calls();
    assert_eq!(batches.len(), 3);
    assert_eq!(batches[1], vec!["album/bad.mp3"]);
    assert_eq!(batches[2], vec!["album/good.m4a"]);
    let task = manager.store.task(&task_id).unwrap().unwrap();
    assert!(task.imports[&0].music_job.is_none());
    assert!(task.imports[&0].attempts >= 2);
    assert!(task.imports[&0].next_retry_at > unix_seconds());
    assert!(task.imports[&1].music_job.is_some());
    assert_eq!(inner.music_batches().len(), 1);
    assert_eq!(inner.music_batches()[0].1.len(), 1);
    assert_eq!(
        inner.music_batches()[0].1[0].relative_path,
        PathBuf::from("album/good.m4a")
    );
}
