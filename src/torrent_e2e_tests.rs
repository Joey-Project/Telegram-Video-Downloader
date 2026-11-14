//! Cross-module torrent-to-Music integration tests with loopback-only fake services.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;

use crate::config::AppConfig;
use crate::music_manager::MusicManager;
use crate::music_runtime::MusicService;
use crate::music_types::{
    ArchiveState, AudioBytes, JobStage, MusicBackend, MusicCancellation, MusicSettings, MusicTrack,
    MusicUpload, PreparedAudio,
};
use crate::queue::QueueManager;
use crate::telegram::TelegramClient;
use crate::torrent_manager::TorrentManager;
use crate::torrent_runtime::TorrentService;
use crate::torrent_types::{
    DEFAULT_MAX_FILES, DEFAULT_MAX_TORRENT_BYTES, TorrentPhase, TorrentSettings,
};
use crate::{BotContext, JobDispatch, process_telegram_updates_with_context};

const CHAT_ID: i64 = 123_456_789;
const CARD_MESSAGE_ID: i64 = 701;
const MAX_TEST_WAIT: Duration = Duration::from_secs(20);

#[derive(Clone)]
struct TorrentFileFixture {
    path: String,
    bytes: Vec<u8>,
}

struct MockHttpState {
    torrent_bytes: Vec<u8>,
    torrent_hash: String,
    save_path: PathBuf,
    files: Vec<TorrentFileFixture>,
    submitted: AtomicBool,
    complete: AtomicBool,
    qbit_adds: AtomicUsize,
    send_widgets: AtomicUsize,
    widget_data_edits: AtomicUsize,
    caption_edits: AtomicUsize,
    send_messages: AtomicUsize,
    text_edits: AtomicUsize,
}

struct MockHttpServer {
    base_url: String,
    state: Arc<MockHttpState>,
    task: JoinHandle<()>,
}

impl MockHttpServer {
    async fn start(
        torrent_bytes: Vec<u8>,
        torrent_hash: String,
        save_path: PathBuf,
        files: Vec<TorrentFileFixture>,
    ) -> Self {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("local mock listener should bind");
        let address = listener
            .local_addr()
            .expect("mock listener should have a local address");
        let state = Arc::new(MockHttpState {
            torrent_bytes,
            torrent_hash,
            save_path,
            files,
            submitted: AtomicBool::new(false),
            complete: AtomicBool::new(false),
            qbit_adds: AtomicUsize::new(0),
            send_widgets: AtomicUsize::new(0),
            widget_data_edits: AtomicUsize::new(0),
            caption_edits: AtomicUsize::new(0),
            send_messages: AtomicUsize::new(0),
            text_edits: AtomicUsize::new(0),
        });
        let task_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let state = Arc::clone(&task_state);
                tokio::spawn(async move {
                    let _ = serve_one(stream, state).await;
                });
            }
        });
        Self {
            base_url: format!("http://{address}"),
            state,
            task,
        }
    }

    fn telegram(&self) -> TelegramClient {
        // synthetic-token-fixtures: joey-private-v3 / api-key-a, active
        TelegramClient::with_test_api_base_url(
            "codex_synth_v1_api_key_a".to_string(),
            self.base_url.clone(),
        )
    }
}

impl Drop for MockHttpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct MockRequest {
    method: String,
    path: String,
    headers: String,
    body: Vec<u8>,
}

async fn serve_one(mut stream: TcpStream, state: Arc<MockHttpState>) -> io::Result<()> {
    let Some(request) = read_request(&mut stream).await? else {
        return Ok(());
    };
    if request.path.starts_with("/api/v2/app/version") {
        return respond(&mut stream, 200, "text/plain", b"v5.0.0").await;
    }
    if request.path.starts_with("/api/v2/torrents/info") {
        let body = if state.submitted.load(Ordering::Acquire) {
            let complete = state.complete.load(Ordering::Acquire);
            let total_size = state
                .files
                .iter()
                .map(|file| file.bytes.len() as u64)
                .sum::<u64>();
            serde_json::to_vec(&vec![serde_json::json!({
                "hash": state.torrent_hash,
                "name": "Album",
                "state": if complete { "uploading" } else { "downloading" },
                "progress": if complete { 1.0 } else { 0.6 },
                "save_path": state.save_path,
                "content_path": state.save_path.join("Album"),
                "total_size": total_size,
                "amount_left": if complete { 0 } else { 1 },
            })])
            .expect("qBittorrent response should serialize")
        } else {
            b"[]".to_vec()
        };
        return respond(&mut stream, 200, "application/json", &body).await;
    }
    if request.path.starts_with("/api/v2/torrents/files") {
        let complete = state.complete.load(Ordering::Acquire);
        let body = serde_json::to_vec(
            &state
                .files
                .iter()
                .enumerate()
                .map(|(index, file)| {
                    let progress = if complete || index != 0 { 1.0 } else { 0.5 };
                    serde_json::json!({
                        "index": index,
                        "name": file.path,
                        "size": file.bytes.len(),
                        "progress": progress,
                        "priority": 1,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .expect("qBittorrent file response should serialize");
        return respond(&mut stream, 200, "application/json", &body).await;
    }
    if request.method == "POST" && request.path.starts_with("/api/v2/torrents/add") {
        assert!(
            request
                .headers
                .to_ascii_lowercase()
                .contains("content-type: multipart/form-data")
        );
        assert!(
            !request.body.is_empty(),
            "qBittorrent add should carry torrent bytes"
        );
        state.qbit_adds.fetch_add(1, Ordering::Relaxed);
        state.submitted.store(true, Ordering::Release);
        return respond(&mut stream, 200, "text/plain", b"Ok.").await;
    }

    if request.path.ends_with("/getFile") {
        let body = serde_json::to_vec(&serde_json::json!({
            "ok": true,
            "result": {
                "file_size": state.torrent_bytes.len(),
                "file_path": "fixtures/album.torrent",
            }
        }))
        .expect("Telegram file metadata should serialize");
        return respond(&mut stream, 200, "application/json", &body).await;
    }
    if request
        .path
        .ends_with("/file/botcodex_synth_v1_api_key_a/fixtures/album.torrent")
    {
        return respond(
            &mut stream,
            200,
            "application/octet-stream",
            &state.torrent_bytes,
        )
        .await;
    }
    if request.path.ends_with("/sendWidget") {
        let generation = state.send_widgets.fetch_add(1, Ordering::Relaxed);
        let body = serde_json::to_vec(&serde_json::json!({
            "ok": true,
            "result": {
                "message_id": CARD_MESSAGE_ID + generation as i64,
                "chat": {"id": CHAT_ID, "type": "private"},
            }
        }))
        .expect("Telegram widget response should serialize");
        return respond(&mut stream, 200, "application/json", &body).await;
    }
    if request.path.ends_with("/editMessageWidgetData") {
        state.widget_data_edits.fetch_add(1, Ordering::Relaxed);
        return respond(
            &mut stream,
            200,
            "application/json",
            br#"{"ok":true,"result":true}"#,
        )
        .await;
    }
    if request.path.ends_with("/editMessageCaption") {
        state.caption_edits.fetch_add(1, Ordering::Relaxed);
        return respond(
            &mut stream,
            200,
            "application/json",
            br#"{"ok":true,"result":true}"#,
        )
        .await;
    }
    if request.path.ends_with("/answerWidgetQuery") {
        return respond(
            &mut stream,
            200,
            "application/json",
            br#"{"ok":true,"result":true}"#,
        )
        .await;
    }
    if request.path.ends_with("/sendMessage") {
        state.send_messages.fetch_add(1, Ordering::Relaxed);
        return respond(
            &mut stream,
            200,
            "application/json",
            br#"{"ok":true,"result":{"message_id":702,"chat":{"id":123456789,"type":"private"}}}"#,
        )
        .await;
    }
    if request.path.ends_with("/editMessageText") {
        state.text_edits.fetch_add(1, Ordering::Relaxed);
        return respond(
            &mut stream,
            200,
            "application/json",
            br#"{"ok":true,"result":true}"#,
        )
        .await;
    }
    respond(
        &mut stream,
        404,
        "application/json",
        br#"{"ok":false,"description":"not found"}"#,
    )
    .await
}

async fn read_request(stream: &mut TcpStream) -> io::Result<Option<MockRequest>> {
    const MAX_HEADER_BYTES: usize = 32 * 1024;
    const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
    let mut received = Vec::new();
    let mut chunk = [0_u8; 8192];
    let header_end = loop {
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Ok(None);
        }
        received.extend_from_slice(&chunk[..count]);
        if let Some(index) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break index + 4;
        }
        if received.len() > MAX_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mock request headers too large",
            ));
        }
    };
    let header_text = String::from_utf8_lossy(&received[..header_end]).to_string();
    let request_line = header_text.lines().next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let content_length = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    if content_length > MAX_BODY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mock request body too large",
        ));
    }
    while received.len() < header_end.saturating_add(content_length) {
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "mock request body ended early",
            ));
        }
        received.extend_from_slice(&chunk[..count]);
    }
    Ok(Some(MockRequest {
        method,
        path,
        headers: header_text,
        body: received[header_end..header_end + content_length].to_vec(),
    }))
}

async fn respond(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Error",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nConnection: close\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await
}

#[derive(Clone)]
struct FakeMusicBackend {
    archive_dir: PathBuf,
    staging_dir: PathBuf,
    library_dir: PathBuf,
    add_calls: Arc<AtomicUsize>,
    prepared_sources: Arc<Mutex<Vec<Vec<u8>>>>,
    cleanup_jobs: Arc<Mutex<Vec<String>>>,
    tracks: Arc<Mutex<HashMap<String, MusicTrack>>>,
}

impl FakeMusicBackend {
    fn new(archive_dir: PathBuf, staging_dir: PathBuf, library_dir: PathBuf) -> Self {
        fs::create_dir_all(&archive_dir).expect("fake archive directory should exist");
        fs::create_dir_all(&library_dir).expect("fake library directory should exist");
        Self {
            archive_dir,
            staging_dir,
            library_dir,
            add_calls: Arc::new(AtomicUsize::new(0)),
            prepared_sources: Arc::new(Mutex::new(Vec::new())),
            cleanup_jobs: Arc::new(Mutex::new(Vec::new())),
            tracks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl MusicBackend for FakeMusicBackend {
    fn download(
        &self,
        _upload: &MusicUpload,
        _destination: &Path,
        _cancel: &MusicCancellation,
    ) -> Result<()> {
        bail!("the torrent integration must use its typed local source")
    }

    fn prepare(
        &self,
        original: &Path,
        working_dir: &Path,
        cancel: &MusicCancellation,
    ) -> Result<PreparedAudio> {
        if cancel.is_cancelled() {
            bail!("fake Music preparation was cancelled");
        }
        let original_bytes = fs::read(original).context("read private Music snapshot")?;
        self.prepared_sources
            .lock()
            .expect("prepared source log should not be poisoned")
            .push(original_bytes.clone());
        let import_path = working_dir.join("prepared.m4a");
        fs::write(&import_path, &original_bytes).context("write fake prepared audio")?;
        Ok(PreparedAudio {
            conversion: None,
            original: audio_bytes(original, &original_bytes),
            import: audio_bytes(&import_path, &original_bytes),
        })
    }

    fn archive(&self, audio: &PreparedAudio, _original_name: &str) -> Result<ArchiveState> {
        let path = self.archive_dir.join("verified-source.archive");
        fs::copy(&audio.original.path, &path).context("copy fake archive bytes")?;
        Ok(ArchiveState {
            path,
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: false,
        })
    }

    fn add_track(&self, audio: &PreparedAudio) -> Result<String> {
        self.add_calls.fetch_add(1, Ordering::Relaxed);
        let persistent_id = "0123456789ABCDEF".to_string();
        let location = self.library_dir.join("selected.m4a");
        fs::copy(&audio.import.path, &location).context("copy fake Music library item")?;
        let track = MusicTrack {
            persistent_id: persistent_id.clone(),
            location: Some(location),
            cloud_status: "uploaded".into(),
        };
        self.tracks
            .lock()
            .expect("fake Music track map should not be poisoned")
            .insert(persistent_id.clone(), track);
        Ok(persistent_id)
    }

    fn query_track(&self, persistent_id: &str) -> Result<MusicTrack> {
        self.tracks
            .lock()
            .expect("fake Music track map should not be poisoned")
            .get(persistent_id)
            .cloned()
            .context("fake Music track was not found")
    }

    fn verify_copy(&self, audio: &PreparedAudio, track: &MusicTrack) -> Result<()> {
        let location = track
            .location
            .as_ref()
            .context("fake Music track location is missing")?;
        let expected = fs::read(&audio.import.path)?;
        ensure_bytes_equal(&expected, &fs::read(location)?)
    }

    fn archive_status(&self, path: &Path) -> Result<ArchiveState> {
        if !path.starts_with(&self.archive_dir) || !path.is_file() {
            bail!("fake archive is missing or outside its managed directory");
        }
        Ok(ArchiveState {
            path: path.to_path_buf(),
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: false,
        })
    }

    fn evict_archive(&self, archive: &ArchiveState, _track: &MusicTrack) -> Result<ArchiveState> {
        Ok(archive.clone())
    }

    fn cleanup_staging(
        &self,
        job_id: &str,
        audio: &PreparedAudio,
        _archive: &ArchiveState,
        _track: &MusicTrack,
    ) -> Result<()> {
        let job_dir = audio
            .original
            .path
            .parent()
            .context("fake prepared source has no job directory")?;
        if job_dir != self.staging_dir.join(job_id) {
            bail!("fake cleanup was asked to delete outside the job staging subtree");
        }
        fs::remove_dir_all(job_dir).context("remove only the private Music job subtree")?;
        self.cleanup_jobs
            .lock()
            .expect("fake cleanup log should not be poisoned")
            .push(job_id.to_string());
        Ok(())
    }
}

fn audio_bytes(path: &Path, bytes: &[u8]) -> AudioBytes {
    AudioBytes {
        path: path.to_path_buf(),
        sha256: sha256_hex(bytes),
        size: bytes.len() as u64,
    }
}

fn ensure_bytes_equal(expected: &[u8], actual: &[u8]) -> Result<()> {
    if expected != actual {
        bail!("fake Music library bytes differ from the prepared source");
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sha1_hex(bytes: &[u8]) -> String {
    Sha1::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn bencode_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    output.extend_from_slice(bytes);
}

fn bencode_integer(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(format!("i{value}e").as_bytes());
}

fn make_torrent(files: &[TorrentFileFixture]) -> (Vec<u8>, String) {
    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"files");
    info.push(b'l');
    for file in files {
        let mut components = file.path.split('/');
        let mut entry = Vec::new();
        entry.push(b'd');
        bencode_bytes(&mut entry, b"length");
        bencode_integer(&mut entry, file.bytes.len() as u64);
        bencode_bytes(&mut entry, b"path");
        entry.push(b'l');
        for component in components.by_ref() {
            bencode_bytes(&mut entry, component.as_bytes());
        }
        entry.extend_from_slice(b"ee");
        info.extend_from_slice(&entry);
    }
    info.push(b'e');
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, b"Album");
    bencode_bytes(&mut info, b"piece length");
    bencode_integer(&mut info, 16_384);
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &[7_u8; 20]);
    info.push(b'e');

    let hash = sha1_hex(&info);
    let mut torrent = Vec::new();
    torrent.push(b'd');
    bencode_bytes(&mut torrent, b"info");
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    (torrent, hash)
}

fn torrent_settings(base_url: String, state_dir: PathBuf) -> TorrentSettings {
    TorrentSettings {
        enabled: true,
        base_url,
        username: None,
        password: None,
        state_dir,
        max_torrent_bytes: DEFAULT_MAX_TORRENT_BYTES,
        max_files: DEFAULT_MAX_FILES,
        poll_seconds: 1,
        request_timeout_seconds: 3,
        max_api_response_bytes: 64 * 1024,
    }
}

fn private_music_settings(root: &Path) -> MusicSettings {
    let music_root = root.join("music");
    fs::create_dir_all(&music_root).expect("Music fixture root should exist");
    let staging_dir = music_root.join("staging");
    fs::create_dir(&staging_dir).expect("Music staging fixture should be created");
    fs::set_permissions(&staging_dir, fs::Permissions::from_mode(0o700))
        .expect("Music staging fixture should be private");
    MusicSettings {
        enabled: true,
        state_dir: music_root.join("state"),
        staging_dir,
        archive_dir: music_root.join("archive"),
        max_file_bytes: 1024 * 1024,
        cloud_poll_seconds: 1,
        ..MusicSettings::default()
    }
}

fn build_context(
    telegram: TelegramClient,
    config: AppConfig,
    torrent: Arc<TorrentService>,
) -> BotContext {
    let queue = Arc::new(QueueManager::open(&config).expect("test video queue should open"));
    BotContext {
        telegram,
        config: Arc::new(config),
        job_dispatch: JobDispatch {
            download_semaphore: Arc::new(Semaphore::new(1)),
            duplicate_scan_semaphore: Arc::new(Semaphore::new(1)),
        },
        next_job_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        queue,
        queue_start_retry_delay: crate::QUEUE_START_FILE_PROVIDER_RETRY_DELAY,
        music: None,
        torrent: Some(torrent),
    }
}

async fn process_update(context: &BotContext, update: serde_json::Value, offset: &mut Option<i64>) {
    let update = serde_json::from_value(update).expect("Telegram update fixture should decode");
    process_telegram_updates_with_context(context, vec![update], offset)
        .await
        .expect("Telegram update should be processed");
}

struct WidgetUpdateFixture<'a> {
    update_id: i64,
    query_id: &'a str,
    sender_id: i64,
    message_id: i64,
    task_id: &'a str,
    revision: u64,
    action: &'a str,
}

async fn process_widget(
    context: &BotContext,
    offset: &mut Option<i64>,
    fixture: WidgetUpdateFixture<'_>,
) {
    process_update(
        context,
        serde_json::json!({
            "update_id": fixture.update_id,
            "widget_query": {
                "id": fixture.query_id,
                "from": {"id": fixture.sender_id},
                "chat": {"id": CHAT_ID, "type": "private"},
                "chat_instance": "private-test",
                "message_id": fixture.message_id,
                "type": {"type": "data"},
                "data": format!(
                    "t1:{}:{:016x}:{}",
                    fixture.task_id, fixture.revision, fixture.action
                ),
            }
        }),
        offset,
    )
    .await;
}

async fn wait_for_snapshot(
    service: &TorrentService,
    chat_id: i64,
    predicate: impl Fn(&crate::torrent_types::TorrentSnapshot) -> bool,
    expected: &str,
) -> crate::torrent_types::TorrentSnapshot {
    tokio::time::timeout(MAX_TEST_WAIT, async {
        loop {
            if let Some(snapshot) = service
                .list(chat_id)
                .await
                .expect("torrent task list should be readable")
                .into_iter()
                .find(|snapshot| predicate(snapshot))
            {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {expected}"))
}

async fn wait_for_music_cleanup(backend: &FakeMusicBackend) -> String {
    tokio::time::timeout(MAX_TEST_WAIT, async {
        loop {
            if let Some(job_id) = backend
                .cleanup_jobs
                .lock()
                .expect("fake cleanup list should not be poisoned")
                .first()
                .cloned()
            {
                return job_id;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Music should complete and clean its private job subtree")
}

async fn wait_for_card_edits(mock: &MockHttpServer) {
    tokio::time::timeout(MAX_TEST_WAIT, async {
        loop {
            if mock.state.widget_data_edits.load(Ordering::Acquire) > 0
                && mock.state.caption_edits.load(Ordering::Acquire) > 0
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("torrent card should edit both compact data and caption after delivery");
}

async fn wait_until_reopenable_music(
    settings: MusicSettings,
    telegram: TelegramClient,
    backend: Arc<dyn MusicBackend>,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(manager) =
                MusicManager::open(settings.clone(), telegram.clone(), backend.clone())
            {
                drop(manager);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("Music manager should release its state lock after shutdown");
}

async fn wait_until_reopenable_torrent(settings: TorrentSettings) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(manager) =
                TorrentManager::open(settings.clone(), Arc::new(ReopenProbeBackend))
            {
                drop(manager);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("torrent manager should release its state lock after shutdown");
}

struct ReopenProbeBackend;

impl crate::torrent_types::TorrentBackend for ReopenProbeBackend {
    fn download_torrent(
        &self,
        _upload: crate::torrent_types::TorrentUpload,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<u8>> {
        Box::pin(async { bail!("reopen probe does not perform network work") })
    }

    fn lookup(
        &self,
        _identity: crate::torrent_types::TorrentIdentity,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<crate::torrent_types::TorrentRemoteSnapshot>>
    {
        Box::pin(async { bail!("reopen probe does not perform network work") })
    }

    fn submit(
        &self,
        _bytes: Vec<u8>,
        _name: String,
    ) -> crate::torrent_types::TorrentFuture<'_, crate::torrent_types::SubmitResult> {
        Box::pin(async { bail!("reopen probe does not perform network work") })
    }

    fn refresh(
        &self,
        _hash: String,
    ) -> crate::torrent_types::TorrentFuture<'_, crate::torrent_types::TorrentRemoteSnapshot> {
        Box::pin(async { bail!("reopen probe does not perform network work") })
    }

    fn send_card(
        &self,
        _snapshot: crate::torrent_types::TorrentSnapshot,
    ) -> crate::torrent_types::TorrentFuture<'_, i64> {
        Box::pin(async { bail!("reopen probe does not perform network work") })
    }

    fn update_card(
        &self,
        _snapshot: crate::torrent_types::TorrentSnapshot,
    ) -> crate::torrent_types::TorrentFuture<'_, ()> {
        Box::pin(async { bail!("reopen probe does not perform network work") })
    }

    fn enqueue_music(
        &self,
        _chat_id: i64,
        _user_id: i64,
        _batch_id: String,
        _files: Vec<crate::torrent_types::CompletedTorrentFile>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<crate::torrent_types::TorrentMusicJobRef>>
    {
        Box::pin(async { bail!("reopen probe does not perform network work") })
    }

    fn music_status(
        &self,
        _chat_id: i64,
        _job_ids: Vec<String>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<crate::torrent_types::TorrentMusicJobStatus>>
    {
        Box::pin(async { bail!("reopen probe does not perform network work") })
    }
}

fn unique_test_root() -> PathBuf {
    static NEXT_ROOT: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "torrent-music-e2e-{}-{}",
        std::process::id(),
        NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
    ))
}

#[tokio::test]
async fn telegram_torrent_selection_snapshots_and_imports_only_the_confirmed_complete_file() {
    let root = unique_test_root();
    fs::create_dir_all(&root).expect("test root should be created");
    let source_root = root.join("qbit-downloads");
    fs::create_dir_all(source_root.join("Album")).expect("qBittorrent download root should exist");

    let fixtures = vec![
        TorrentFileFixture {
            path: "Album/selected.m4a".into(),
            bytes: b"synthetic selected M4A bytes for the local import path".to_vec(),
        },
        TorrentFileFixture {
            path: "Album/not-selected.mp3".into(),
            bytes: b"synthetic non-selected MP3 bytes".to_vec(),
        },
        TorrentFileFixture {
            path: "Album/unsupported.mp4".into(),
            bytes: b"synthetic video bytes".to_vec(),
        },
    ];
    let mut source_identities = Vec::new();
    for file in &fixtures {
        let source = source_root.join(&file.path);
        fs::write(&source, &file.bytes).expect("qBittorrent fixture file should be written");
        let metadata = fs::metadata(&source).expect("source identity should be readable");
        source_identities.push((metadata.dev(), metadata.ino(), file.bytes.clone()));
    }
    let (torrent_bytes, torrent_hash) = make_torrent(&fixtures);
    let parsed = crate::torrent_metainfo::parse_metainfo(
        &torrent_bytes,
        DEFAULT_MAX_TORRENT_BYTES,
        DEFAULT_MAX_FILES,
    )
    .expect("synthetic metainfo should be valid");
    assert_eq!(parsed.identity.v1.as_deref(), Some(torrent_hash.as_str()));

    let mock = MockHttpServer::start(
        torrent_bytes.clone(),
        torrent_hash,
        source_root.clone(),
        fixtures.clone(),
    )
    .await;
    let telegram = mock.telegram();
    let music_settings = private_music_settings(&root);
    let sentinel = music_settings
        .staging_dir
        .join("keep-existing-staging-data");
    fs::write(&sentinel, b"unrelated Music staging data").expect("staging sentinel should exist");
    let backend = FakeMusicBackend::new(
        music_settings.archive_dir.clone(),
        music_settings.staging_dir.clone(),
        root.join("fake-music-library"),
    );
    let backend_dyn: Arc<dyn MusicBackend> = Arc::new(backend.clone());

    let (music_shutdown_tx, music_shutdown_rx) = watch::channel(false);
    let music = MusicService::with_backend(
        music_settings.clone(),
        telegram.clone(),
        Arc::clone(&backend_dyn),
        music_shutdown_rx,
    );
    music.start();

    let torrent_settings = torrent_settings(mock.base_url.clone(), root.join("qbit-state"));
    let (torrent_shutdown_tx, torrent_shutdown_rx) = watch::channel(false);
    let torrent = TorrentService::new(
        torrent_settings.clone(),
        telegram.clone(),
        Some(Arc::clone(&music)),
        music_settings.max_file_bytes,
        torrent_shutdown_rx,
    )
    .expect("torrent service should use the loopback qBittorrent client");
    torrent.start();

    let mut config = AppConfig::for_test();
    config.telegram.allowed_chat_ids = vec![CHAT_ID];
    config.downloads.video_dir = root.join("video-queue");
    config.downloads.pdf_dir = root.join("pdf-queue");
    fs::create_dir_all(&config.downloads.video_dir).expect("video queue root should exist");
    fs::create_dir_all(&config.downloads.pdf_dir).expect("PDF queue root should exist");
    let context = build_context(telegram.clone(), config, Arc::clone(&torrent));
    let mut offset = None;
    let upload_update = serde_json::json!({
        "update_id": 5001,
        "message": {
            "message_id": 610,
            "chat": {"id": CHAT_ID, "type": "private"},
            "from": {"id": CHAT_ID},
            "document": {
                "file_id": "torrent-e2e-file",
                "file_unique_id": "torrent-e2e-file-unique",
                "file_name": "album.torrent",
                "file_size": torrent_bytes.len(),
                "mime_type": "application/x-bittorrent",
            }
        }
    });
    process_update(&context, upload_update.clone(), &mut offset).await;

    let initial = wait_for_snapshot(
        &torrent,
        CHAT_ID,
        |snapshot| {
            snapshot.card_message_id == Some(CARD_MESSAGE_ID)
                && snapshot.phase == TorrentPhase::Downloading
                && snapshot.state.is_some()
                && snapshot.files.len() == 3
        },
        "qBittorrent identity reconciliation and file inventory",
    )
    .await;
    wait_for_card_edits(&mock).await;
    assert_eq!(initial.phase, TorrentPhase::Downloading);
    assert!(!initial.files[0].complete);
    assert!(initial.files[0].supported_music);
    assert!(initial.files[1].supported_music);
    assert!(!initial.files[2].supported_music);
    assert_eq!(mock.state.qbit_adds.load(Ordering::Relaxed), 1);
    assert_eq!(mock.state.send_widgets.load(Ordering::Relaxed), 1);
    assert!(mock.state.widget_data_edits.load(Ordering::Relaxed) > 0);
    assert!(mock.state.caption_edits.load(Ordering::Relaxed) > 0);

    let task_id = initial.task_id.clone();
    let inventory_revision = initial.inventory_revision;
    let selection_revision = initial.selection_revision;
    process_widget(
        &context,
        &mut offset,
        WidgetUpdateFixture {
            update_id: 5002,
            query_id: "foreign-widget-query",
            sender_id: CHAT_ID + 1,
            message_id: CARD_MESSAGE_ID,
            task_id: &task_id,
            revision: inventory_revision,
            action: "f:0:1",
        },
    )
    .await;
    process_widget(
        &context,
        &mut offset,
        WidgetUpdateFixture {
            update_id: 5003,
            query_id: "stale-card-query",
            sender_id: CHAT_ID,
            message_id: CARD_MESSAGE_ID + 1,
            task_id: &task_id,
            revision: inventory_revision,
            action: "f:0:1",
        },
    )
    .await;

    process_widget(
        &context,
        &mut offset,
        WidgetUpdateFixture {
            update_id: 5004,
            query_id: "select-one-supported-file",
            sender_id: CHAT_ID,
            message_id: CARD_MESSAGE_ID,
            task_id: &task_id,
            revision: inventory_revision,
            action: "f:0:1",
        },
    )
    .await;
    let selected = torrent
        .list(CHAT_ID)
        .await
        .expect("task should remain readable")
        .into_iter()
        .find(|snapshot| snapshot.task_id == task_id)
        .expect("selected torrent task should remain present");
    assert_eq!(
        selected
            .files
            .iter()
            .filter(|file| file.selected)
            .map(|file| file.index)
            .collect::<Vec<_>>(),
        vec![0]
    );
    assert!(selected.selection_revision > selection_revision);

    process_widget(
        &context,
        &mut offset,
        WidgetUpdateFixture {
            update_id: 5005,
            query_id: "stale-selection-revision",
            sender_id: CHAT_ID,
            message_id: CARD_MESSAGE_ID,
            task_id: &task_id,
            revision: selection_revision,
            action: "i",
        },
    )
    .await;
    process_widget(
        &context,
        &mut offset,
        WidgetUpdateFixture {
            update_id: 5006,
            query_id: "unsupported-file-selection",
            sender_id: CHAT_ID,
            message_id: CARD_MESSAGE_ID,
            task_id: &task_id,
            revision: selected.inventory_revision,
            action: "f:2:1",
        },
    )
    .await;
    process_widget(
        &context,
        &mut offset,
        WidgetUpdateFixture {
            update_id: 5007,
            query_id: "confirm-selected-incomplete-file",
            sender_id: CHAT_ID,
            message_id: CARD_MESSAGE_ID,
            task_id: &task_id,
            revision: selected.selection_revision,
            action: "i",
        },
    )
    .await;
    let manager = torrent
        .manager()
        .await
        .expect("torrent manager should remain open while the selected file is incomplete");
    manager.process_task_once_for_test(&task_id).await;
    let waiting = torrent
        .list(CHAT_ID)
        .await
        .expect("incomplete import request should remain readable")
        .into_iter()
        .find(|snapshot| snapshot.task_id == task_id)
        .expect("incomplete import task should remain present");
    assert!(!waiting.files[0].complete);
    assert_eq!(
        waiting.files[0].music_stage,
        Some(crate::torrent_types::TorrentMusicStage::Pending),
        "the confirmed source should remain pending until qBittorrent reports completion"
    );
    assert_eq!(
        backend.add_calls.load(Ordering::Relaxed),
        0,
        "incomplete source must wait before Music add"
    );
    assert!(
        backend
            .prepared_sources
            .lock()
            .expect("prepared source log should not be poisoned")
            .is_empty()
    );
    drop(manager);

    mock.state.complete.store(true, Ordering::Release);
    process_widget(
        &context,
        &mut offset,
        WidgetUpdateFixture {
            update_id: 5008,
            query_id: "refresh-completed-selected-file",
            sender_id: CHAT_ID,
            message_id: CARD_MESSAGE_ID,
            task_id: &task_id,
            revision: selected.inventory_revision,
            action: "r",
        },
    )
    .await;
    let completed = wait_for_snapshot(
        &torrent,
        CHAT_ID,
        |snapshot| {
            snapshot.task_id == task_id
                && snapshot.files.len() == fixtures.len()
                && snapshot.files.iter().all(|file| file.complete)
        },
        "qBittorrent completion refresh",
    )
    .await;
    assert!(completed.files.iter().all(|file| file.complete));

    let job_id = wait_for_music_cleanup(&backend).await;
    assert_eq!(backend.add_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        backend
            .prepared_sources
            .lock()
            .expect("prepared source log should not be poisoned")
            .as_slice(),
        &[fixtures[0].bytes.clone()],
        "only the selected qBittorrent file should reach Music preparation"
    );
    assert!(
        sentinel.is_file(),
        "completed cleanup must leave unrelated staging data alone"
    );
    assert!(
        !music_settings.staging_dir.join(&job_id).exists(),
        "completed cleanup should remove its own private snapshot subtree"
    );
    let archive = music_settings.archive_dir.join("verified-source.archive");
    assert_eq!(
        fs::read(archive).expect("fake archive should remain in the Music archive"),
        fixtures[0].bytes
    );
    for (file, (device, inode, original_bytes)) in fixtures.iter().zip(&source_identities) {
        let source = source_root.join(&file.path);
        let metadata = fs::metadata(&source).expect("qBittorrent source must remain present");
        assert_eq!((metadata.dev(), metadata.ino()), (*device, *inode));
        assert_eq!(
            fs::read(source).expect("qBittorrent source bytes should remain readable"),
            *original_bytes
        );
    }

    let music_status = music
        .local_job_status(CHAT_ID, std::slice::from_ref(&job_id))
        .await
        .expect("local Music status should be readable without Telegram messages");
    assert_eq!(music_status.len(), 1);
    assert_eq!(music_status[0].stage, JobStage::Complete);
    let manager = torrent
        .manager()
        .await
        .expect("torrent manager should remain open");
    manager.refresh_music_status_for_test(&task_id).await;
    let imported = wait_for_snapshot(
        &torrent,
        CHAT_ID,
        |snapshot| snapshot.task_id == task_id && snapshot.phase == TorrentPhase::Imported,
        "aggregate local Music completion",
    )
    .await;
    assert_eq!(
        imported
            .files
            .iter()
            .filter(|file| file.music_stage.is_some())
            .map(|file| file.index)
            .collect::<Vec<_>>(),
        vec![0]
    );

    process_update(&context, upload_update, &mut offset).await;
    process_widget(
        &context,
        &mut offset,
        WidgetUpdateFixture {
            update_id: 5009,
            query_id: "replay-import-confirmation",
            sender_id: CHAT_ID,
            message_id: CARD_MESSAGE_ID,
            task_id: &task_id,
            revision: imported.selection_revision,
            action: "i",
        },
    )
    .await;
    assert_eq!(
        mock.state.qbit_adds.load(Ordering::Relaxed),
        1,
        "replayed Telegram update must not resubmit the torrent"
    );
    assert_eq!(
        backend.add_calls.load(Ordering::Relaxed),
        1,
        "replayed confirmation must not add the Music track twice"
    );
    assert_eq!(
        mock.state.send_messages.load(Ordering::Relaxed),
        0,
        "local Music jobs must not send per-track status messages"
    );
    assert_eq!(
        mock.state.text_edits.load(Ordering::Relaxed),
        0,
        "local Music jobs must not edit per-track status messages"
    );

    drop(manager);
    let _ = torrent_shutdown_tx.send(true);
    let _ = music_shutdown_tx.send(true);
    drop(context);
    drop(torrent);
    drop(music);
    wait_until_reopenable_music(
        music_settings.clone(),
        telegram.clone(),
        Arc::clone(&backend_dyn),
    )
    .await;
    wait_until_reopenable_torrent(torrent_settings.clone()).await;
    let reopened_torrent =
        TorrentManager::open(torrent_settings.clone(), Arc::new(ReopenProbeBackend))
            .expect("torrent state should reopen after the service stops");
    let persisted = reopened_torrent
        .list(CHAT_ID)
        .expect("reopened torrent state should list its persisted task");
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].phase, TorrentPhase::Imported);
    drop(reopened_torrent);

    let (music_shutdown_tx, music_shutdown_rx) = watch::channel(false);
    let restarted_music = MusicService::with_backend(
        music_settings.clone(),
        telegram.clone(),
        Arc::clone(&backend_dyn),
        music_shutdown_rx,
    );
    restarted_music.start();
    let (torrent_shutdown_tx, torrent_shutdown_rx) = watch::channel(false);
    let restarted_torrent = TorrentService::new(
        torrent_settings.clone(),
        telegram.clone(),
        Some(Arc::clone(&restarted_music)),
        music_settings.max_file_bytes,
        torrent_shutdown_rx,
    )
    .expect("torrent service should restart from its saved state");
    restarted_torrent.start();
    let mut restarted_config = AppConfig::for_test();
    restarted_config.telegram.allowed_chat_ids = vec![CHAT_ID];
    restarted_config.downloads.video_dir = root.join("video-queue");
    restarted_config.downloads.pdf_dir = root.join("pdf-queue");
    let restarted_context = build_context(
        telegram.clone(),
        restarted_config,
        Arc::clone(&restarted_torrent),
    );
    let restarted_status = restarted_music
        .local_job_status(CHAT_ID, std::slice::from_ref(&job_id))
        .await
        .expect("restarted Music state should expose the completed local job");
    assert_eq!(restarted_status.len(), 1);
    assert_eq!(restarted_status[0].stage, JobStage::Complete);
    let mut restarted_offset = None;
    process_update(
        &restarted_context,
        serde_json::json!({
            "update_id": 5001,
            "message": {
                "message_id": 610,
                "chat": {"id": CHAT_ID, "type": "private"},
                "from": {"id": CHAT_ID},
                "document": {
                    "file_id": "torrent-e2e-file",
                    "file_unique_id": "torrent-e2e-file-unique",
                    "file_name": "album.torrent",
                    "file_size": torrent_bytes.len(),
                    "mime_type": "application/x-bittorrent",
                }
            }
        }),
        &mut restarted_offset,
    )
    .await;
    let reopened_snapshot = wait_for_snapshot(
        &restarted_torrent,
        CHAT_ID,
        |snapshot| snapshot.task_id == task_id,
        "restarted torrent task",
    )
    .await;
    assert_eq!(reopened_snapshot.card_message_id, Some(CARD_MESSAGE_ID));
    process_update(
        &restarted_context,
        serde_json::json!({
            "update_id": 5011,
            "message": {
                "message_id": 611,
                "chat": {"id": CHAT_ID, "type": "private"},
                "from": {"id": CHAT_ID},
                "text": format!("/torrents {task_id}"),
            }
        }),
        &mut restarted_offset,
    )
    .await;
    let reopened_card = wait_for_snapshot(
        &restarted_torrent,
        CHAT_ID,
        |snapshot| {
            snapshot.task_id == task_id
                && snapshot
                    .card_message_id
                    .is_some_and(|message_id| message_id > CARD_MESSAGE_ID)
        },
        "reopened card generation after restart",
    )
    .await;
    process_widget(
        &restarted_context,
        &mut restarted_offset,
        WidgetUpdateFixture {
            update_id: 5012,
            query_id: "callback-from-old-card-generation",
            sender_id: CHAT_ID,
            message_id: CARD_MESSAGE_ID,
            task_id: &task_id,
            revision: reopened_card.selection_revision,
            action: "i",
        },
    )
    .await;
    process_widget(
        &restarted_context,
        &mut restarted_offset,
        WidgetUpdateFixture {
            update_id: 5013,
            query_id: "restart-import-confirmation",
            sender_id: CHAT_ID,
            message_id: reopened_card
                .card_message_id
                .expect("reopened card should have a message ID"),
            task_id: &task_id,
            revision: reopened_card.selection_revision,
            action: "i",
        },
    )
    .await;
    assert_eq!(
        mock.state.qbit_adds.load(Ordering::Relaxed),
        1,
        "restarted state must not submit qBittorrent twice"
    );
    assert_eq!(
        backend.add_calls.load(Ordering::Relaxed),
        1,
        "restarted Music state must not add the track twice"
    );
    for (file, (device, inode, original_bytes)) in fixtures.iter().zip(&source_identities) {
        let source = source_root.join(&file.path);
        let metadata = fs::metadata(&source).expect("qBittorrent source must survive restart");
        assert_eq!((metadata.dev(), metadata.ino()), (*device, *inode));
        assert_eq!(
            fs::read(source).expect("source bytes should survive restart"),
            *original_bytes
        );
    }

    let _ = torrent_shutdown_tx.send(true);
    let _ = music_shutdown_tx.send(true);
    drop(restarted_context);
    drop(restarted_torrent);
    drop(restarted_music);
    wait_until_reopenable_music(
        music_settings.clone(),
        telegram.clone(),
        Arc::clone(&backend_dyn),
    )
    .await;
    wait_until_reopenable_torrent(torrent_settings).await;
    let _ = fs::remove_dir_all(&root);
}
