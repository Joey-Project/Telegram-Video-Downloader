use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use tempfile::TempDir;
use tokio::sync::watch;

use crate::torrent_runtime::TorrentService;
use crate::torrent_types::{
    CompletedTorrentFile, RemoteTorrentFile, SubmitResult, TorrentAction, TorrentBackend,
    TorrentExisting, TorrentFuture, TorrentIdentity, TorrentMetainfo, TorrentMusicJobRef,
    TorrentMusicJobStatus, TorrentRemoteSnapshot, TorrentSettings, TorrentSnapshot, TorrentUpload,
};

pub(super) struct MiniAppTestFixture {
    pub owner_chat: i64,
    pub service: Arc<TorrentService>,
    pub backend: Arc<MiniAppTestBackend>,
    pub temp: TempDir,
    pub task_id: String,
    pub hash: String,
    pub source_root: PathBuf,
    pub shutdown: watch::Sender<bool>,
}

pub(super) struct MiniAppTestBackend {
    raw_metainfo: Vec<u8>,
    remote: Mutex<TorrentRemoteSnapshot>,
    music_payloads: Mutex<Vec<Vec<CompletedTorrentFile>>>,
    sent_cards: Mutex<usize>,
}

struct MiniAppTestEnvironment {
    temp: TempDir,
    source_root: PathBuf,
    backend: Arc<MiniAppTestBackend>,
    service: Arc<TorrentService>,
    shutdown: watch::Sender<bool>,
    hash: String,
}

impl MiniAppTestBackend {
    pub fn music_payloads(&self) -> Vec<Vec<CompletedTorrentFile>> {
        self.music_payloads
            .lock()
            .expect("music payload lock")
            .clone()
    }

    pub fn sent_cards(&self) -> usize {
        *self.sent_cards.lock().expect("card count lock")
    }

    pub fn pause_completion(&self) {
        let mut remote = self.remote.lock().expect("remote lock");
        remote.progress = 0.5;
        remote.amount_left = remote.total_size / 2;
        for file in &mut remote.files {
            file.progress = 0.5;
        }
        remote.state = "downloading".to_string();
    }
}

impl TorrentBackend for MiniAppTestBackend {
    fn download_torrent(&self, _upload: TorrentUpload) -> TorrentFuture<'_, Vec<u8>> {
        Box::pin(async { anyhow::bail!("unexpected torrent download") })
    }

    fn lookup(&self, _identity: TorrentIdentity) -> TorrentFuture<'_, Vec<TorrentRemoteSnapshot>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_torrents(
        &self,
        offset: u32,
        limit: u32,
    ) -> TorrentFuture<'_, Vec<TorrentRemoteSnapshot>> {
        let remote = self.remote.lock().expect("remote lock").clone();
        Box::pin(async move {
            Ok(if offset == 0 && limit > 0 {
                vec![remote]
            } else {
                Vec::new()
            })
        })
    }

    fn set_active(&self, _hash: String, active: bool) -> TorrentFuture<'_, ()> {
        let mut remote = self.remote.lock().expect("remote lock");
        remote.state = if active { "uploading" } else { "pausedUP" }.to_string();
        Box::pin(async { Ok(()) })
    }

    fn submit(&self, _raw_torrent: Vec<u8>, _file_name: String) -> TorrentFuture<'_, SubmitResult> {
        Box::pin(async { anyhow::bail!("unexpected torrent submission") })
    }

    fn export_metainfo(&self, _hash: String) -> TorrentFuture<'_, Vec<u8>> {
        let bytes = self.raw_metainfo.clone();
        Box::pin(async move { Ok(bytes) })
    }

    fn refresh(&self, hash: String) -> TorrentFuture<'_, TorrentRemoteSnapshot> {
        let remote = self.remote.lock().expect("remote lock").clone();
        Box::pin(async move {
            anyhow::ensure!(remote.hash.eq_ignore_ascii_case(&hash), "wrong test hash");
            Ok(remote)
        })
    }

    fn send_card(&self, _snapshot: TorrentSnapshot) -> TorrentFuture<'_, i64> {
        *self.sent_cards.lock().expect("card count lock") += 1;
        Box::pin(async { Ok(555) })
    }

    fn update_card(&self, _snapshot: TorrentSnapshot) -> TorrentFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn enqueue_music(
        &self,
        _chat_id: i64,
        _user_id: i64,
        _batch_id: String,
        files: Vec<CompletedTorrentFile>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        self.music_payloads
            .lock()
            .expect("music payload lock")
            .push(files.clone());
        let jobs = files
            .into_iter()
            .map(|file| TorrentMusicJobRef {
                job_id: format!("job-{}", file.source_key),
                source_key: file.source_key,
            })
            .collect();
        Box::pin(async move { Ok(jobs) })
    }

    fn lookup_music_jobs(
        &self,
        _chat_id: i64,
        _user_id: i64,
        _source_keys: Vec<String>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn music_status(
        &self,
        _chat_id: i64,
        _job_ids: Vec<String>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobStatus>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

pub(super) async fn miniapp_test_fixture() -> Result<MiniAppTestFixture> {
    let MiniAppTestEnvironment {
        temp,
        source_root,
        backend,
        service,
        shutdown,
        hash,
    } = miniapp_test_environment()?;
    let snapshot = service.miniapp_adopt(17, hash.clone()).await?;
    anyhow::ensure!(
        !snapshot.files.is_empty(),
        "test torrent inventory was not prepared"
    );
    Ok(MiniAppTestFixture {
        owner_chat: 17,
        service,
        backend,
        temp,
        task_id: snapshot.task_id,
        hash,
        source_root,
        shutdown,
    })
}

fn miniapp_test_environment() -> Result<MiniAppTestEnvironment> {
    let temp = tempfile::tempdir()?;
    let source_root = temp.path().join("qbit-source");
    std::fs::create_dir_all(source_root.join("album"))?;
    std::fs::write(source_root.join("album/first.flac"), b"first")?;
    std::fs::write(source_root.join("album/second.flac"), b"second")?;
    let raw_metainfo = metainfo_fixture();
    let metainfo = crate::torrent_metainfo::parse_metainfo(&raw_metainfo, 1024 * 1024, 5000)?;
    let hash = metainfo.identity.v1.clone().expect("v1 test identity");
    let remote = remote_fixture(&metainfo, &source_root);
    let backend = Arc::new(MiniAppTestBackend {
        raw_metainfo,
        remote: Mutex::new(remote),
        music_payloads: Mutex::new(Vec::new()),
        sent_cards: Mutex::new(0),
    });
    let settings = TorrentSettings {
        enabled: true,
        state_dir: temp.path().join("state"),
        metainfo_dir: Some(temp.path().join("metainfo")),
        poll_seconds: 60,
        ..TorrentSettings::default()
    };
    let (shutdown, receiver) = watch::channel(false);
    let service = TorrentService::with_backend(settings, backend.clone(), receiver);
    service.start();
    Ok(MiniAppTestEnvironment {
        temp,
        source_root,
        backend,
        service,
        shutdown,
        hash,
    })
}

#[tokio::test]
async fn review_is_read_only_and_confirm_is_revision_bound_replay_safe_and_selected_only() {
    let fixture = miniapp_test_fixture().await.expect("Mini App fixture");
    let snapshot = fixture
        .service
        .miniapp_detail(17, fixture.task_id.clone())
        .await
        .expect("initial detail");
    assert_eq!(snapshot.selected_count, 0);

    let review = fixture
        .service
        .handle_miniapp_action(
            17,
            "review-request:10",
            &serde_json::json!({
                "operation": "review",
                "task_id": fixture.task_id,
                "inventory_revision": snapshot.inventory_revision,
                "selection_revision": snapshot.selection_revision,
                "selected_indexes": [0],
            }),
        )
        .await
        .expect("read-only review");
    assert_eq!(review["selected_indexes"], serde_json::json!([0]));
    assert_eq!(review["selected_count"], 1);

    let still_unselected = fixture
        .service
        .miniapp_detail(17, fixture.task_id.clone())
        .await
        .expect("detail after preview");
    assert_eq!(still_unselected.selected_count, 0);
    assert_eq!(still_unselected.import_count, 0);

    let confirm_payload = serde_json::json!({
        "operation": "confirm",
        "task_id": fixture.task_id,
        "review_id": review["review_id"],
        "inventory_revision": review["inventory_revision"],
        "selection_revision": review["selection_revision"],
        "selected_indexes": review["selected_indexes"],
    });
    let confirmed = fixture
        .service
        .handle_miniapp_action(17, "confirm-request:11", &confirm_payload)
        .await
        .expect("atomic confirm");
    assert_eq!(confirmed["confirmed"], true);
    assert_eq!(confirmed["replayed"], false);
    assert_eq!(confirmed["selected_count"], 1);

    let replay = fixture
        .service
        .handle_miniapp_action(17, "confirm-request:11", &confirm_payload)
        .await
        .expect("idempotent replay");
    assert_eq!(replay["replayed"], true);
    assert!(
        fixture
            .service
            .handle_miniapp_action(17, "stale-confirm:12", &confirm_payload)
            .await
            .is_err()
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while fixture.backend.music_payloads().is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let payloads = fixture.backend.music_payloads();
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].len(), 1);
    assert_eq!(
        payloads[0][0].relative_path,
        PathBuf::from("album/first.flac")
    );
    assert_eq!(payloads[0][0].source_root, fixture.source_root);
    assert!(
        fixture
            .source_root
            .join(&payloads[0][0].relative_path)
            .is_file()
    );
    fixture.shutdown.send(true).expect("stop Mini App fixture");
}

#[tokio::test]
async fn hash_adoption_reuses_owner_task_without_cards_or_cross_owner_access() {
    let fixture = miniapp_test_fixture().await.expect("Mini App fixture");
    let before = fixture.backend.sent_cards();
    let first = fixture
        .service
        .miniapp_adopt(17, fixture.hash.clone())
        .await
        .expect("repeat hash adoption");
    assert_eq!(first.task_id, fixture.task_id);
    assert_eq!(fixture.backend.sent_cards(), before);
    let wrong_owner = fixture
        .service
        .miniapp_adopt(18, fixture.hash.clone())
        .await;
    assert!(wrong_owner.is_err());
    fixture.shutdown.send(true).expect("stop Mini App fixture");
}

#[tokio::test]
async fn miniapp_reuses_native_owner_selection_and_pending_import_without_new_card() {
    let MiniAppTestEnvironment {
        temp,
        backend,
        service,
        shutdown,
        hash,
        ..
    } = miniapp_test_environment().expect("test environment");
    backend.pause_completion();
    let native = service
        .adopt_existing(
            TorrentExisting {
                chat_id: 17,
                user_id: 17,
                message_id: 900,
                hash: hash.clone(),
            },
            "native-open:1".to_string(),
        )
        .await
        .expect("native qB adoption");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut ready = None;
    while tokio::time::Instant::now() < deadline {
        if let Some(snapshot) = service
            .list(17)
            .await
            .expect("list native task")
            .into_iter()
            .find(|snapshot| snapshot.task_id == native.task_id)
            && snapshot.card_message_id.is_some()
            && snapshot
                .files
                .first()
                .is_some_and(|file| file.supported_music)
        {
            ready = Some(snapshot);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let snapshot = ready.expect("native card and verified inventory");
    let card_id = snapshot.card_message_id.expect("native card id");
    let selected = service
        .apply_action(
            17,
            17,
            card_id,
            snapshot.task_id.clone(),
            snapshot.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .await
        .expect("native file selection");
    let imported = service
        .apply_action(
            17,
            17,
            card_id,
            selected.task_id.clone(),
            selected.selection_revision,
            TorrentAction::ImportSelected,
        )
        .await
        .expect("native import confirmation");
    assert!(imported.files[0].selected);
    assert_eq!(
        backend.music_payloads().len(),
        0,
        "incomplete file must wait"
    );
    let cards_before = backend.sent_cards();

    let reused = service
        .miniapp_adopt(17, hash)
        .await
        .expect("Mini App reuse of existing native task");
    assert_eq!(reused.task_id, native.task_id);
    assert!(reused.files[0].selected);
    assert_eq!(
        reused.files[0].music_stage,
        Some(crate::torrent_types::TorrentMusicStage::Pending)
    );
    assert!(!reused.files[0].needs_reconfirm);
    assert_eq!(backend.sent_cards(), cards_before);
    assert_eq!(service.list(17).await.expect("owner task list").len(), 1);
    shutdown.send(true).expect("stop native Mini App fixture");
    drop(temp);
}

fn remote_fixture(metainfo: &TorrentMetainfo, source_root: &Path) -> TorrentRemoteSnapshot {
    TorrentRemoteSnapshot {
        hash: metainfo.identity.v1.clone().expect("v1 fixture hash"),
        info_hash_v1: metainfo.identity.v1.clone(),
        info_hash_v2: metainfo.identity.v2.clone(),
        name: metainfo.name.clone(),
        state: "uploading".to_string(),
        progress: 1.0,
        save_path: source_root.to_path_buf(),
        content_path: None,
        total_size: metainfo.total_size,
        amount_left: 0,
        files: metainfo
            .files
            .iter()
            .map(|file| RemoteTorrentFile {
                index: file.index,
                relative_path: file.relative_path.clone(),
                size: file.size,
                progress: 1.0,
                priority: 1,
            })
            .collect(),
    }
}

fn metainfo_fixture() -> Vec<u8> {
    let files = ["first.flac", "second.flac"]
        .into_iter()
        .map(|name| {
            dict(vec![
                (b"length".to_vec(), integer(5)),
                (
                    b"path".to_vec(),
                    list(vec![bytes(b"album"), bytes(name.as_bytes())]),
                ),
            ])
        })
        .collect();
    let info = dict(vec![
        (b"files".to_vec(), list(files)),
        (b"name".to_vec(), bytes(b"release")),
        (b"piece length".to_vec(), integer(16_384)),
        (b"pieces".to_vec(), bytes(&[7; 20])),
    ]);
    dict(vec![(b"info".to_vec(), info)])
}

fn dict(mut values: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<u8> {
    values.sort_by(|left, right| left.0.cmp(&right.0));
    let mut output = vec![b'd'];
    for (key, value) in values {
        output.extend(bytes(&key));
        output.extend(value);
    }
    output.push(b'e');
    output
}

fn list(values: Vec<Vec<u8>>) -> Vec<u8> {
    let mut output = vec![b'l'];
    for value in values {
        output.extend(value);
    }
    output.push(b'e');
    output
}

fn bytes(value: &[u8]) -> Vec<u8> {
    let mut output = format!("{}:", value.len()).into_bytes();
    output.extend_from_slice(value);
    output
}

fn integer(value: u64) -> Vec<u8> {
    format!("i{value}e").into_bytes()
}
