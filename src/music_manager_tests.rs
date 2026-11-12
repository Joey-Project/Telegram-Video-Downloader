use super::*;

use std::collections::VecDeque;
use std::sync::Condvar;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct TestDirectory(PathBuf);

impl TestDirectory {
    pub(crate) fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "music-manager-test-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(path.join("media")).expect("test media root should be created");
        fs::create_dir_all(path.join("media/archive"))
            .expect("test archive root should be created");
        Self(path)
    }

    pub(crate) fn settings(&self) -> MusicSettings {
        MusicSettings {
            enabled: true,
            state_dir: self.0.join("media/.music-state"),
            staging_dir: self.0.join("media/.music-staging"),
            archive_dir: self.0.join("media/archive"),
            max_file_bytes: 1024 * 1024,
            cloud_poll_seconds: 1,
            ..MusicSettings::default()
        }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct TestHttpState {
    requests: Mutex<Vec<(String, String, Option<i64>)>>,
    fail_edit_containing: Mutex<Option<(String, Option<usize>)>>,
    fail_send: AtomicBool,
    next_message_id: AtomicU64,
}

pub(crate) struct TestHttpServer {
    pub(crate) client: TelegramClient,
    state: Arc<TestHttpState>,
    task: JoinHandle<()>,
}

impl TestHttpServer {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test Telegram listener should bind");
        let address = listener
            .local_addr()
            .expect("listener address should exist");
        let state = Arc::new(TestHttpState::default());
        let state_for_task = Arc::clone(&state);
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let state = Arc::clone(&state_for_task);
                tokio::spawn(async move { handle_http_request(socket, state).await });
            }
        });
        const SYNTHETIC_TELEGRAM_API_KEY: &str = "codex_synth_v1_api_key_a"; // synthetic-token-fixtures: joey-private-v3 / api-key-a, active
        Self {
            client: TelegramClient::with_test_api_base_url(
                SYNTHETIC_TELEGRAM_API_KEY.to_string(),
                format!("http://{address}"),
            ),
            state,
            task,
        }
    }

    pub(crate) async fn wait_for_text(&self, text: &str) -> bool {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if self
                    .state
                    .requests
                    .lock()
                    .expect("request list should not be poisoned")
                    .iter()
                    .any(|(_, request_text, _)| request_text.contains(text))
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok()
    }

    pub(crate) fn count_method(&self, method: &str) -> usize {
        self.state
            .requests
            .lock()
            .expect("request list should not be poisoned")
            .iter()
            .filter(|(path, _, _)| path.ends_with(method))
            .count()
    }

    pub(crate) fn count_text_for_method(&self, method: &str, text: &str) -> usize {
        self.state
            .requests
            .lock()
            .expect("request list should not be poisoned")
            .iter()
            .filter(|(path, request_text, _)| path.ends_with(method) && request_text.contains(text))
            .count()
    }

    pub(crate) fn edited_message_ids(&self) -> Vec<i64> {
        self.state
            .requests
            .lock()
            .expect("request list should not be poisoned")
            .iter()
            .filter_map(|(path, _, message_id)| {
                (path.ends_with("editMessageText"))
                    .then_some(*message_id)
                    .flatten()
            })
            .collect()
    }

    pub(crate) fn fail_edits_containing(&self, text: &str) {
        *self
            .state
            .fail_edit_containing
            .lock()
            .expect("failure selector should not be poisoned") = Some((text.to_string(), None));
    }

    pub(crate) fn fail_next_edit_containing(&self, text: &str) {
        *self
            .state
            .fail_edit_containing
            .lock()
            .expect("failure selector should not be poisoned") = Some((text.to_string(), Some(1)));
    }

    pub(crate) fn fail_sends(&self, fail: bool) {
        self.state.fail_send.store(fail, Ordering::Release);
    }

    pub(crate) fn stop(self) {
        self.task.abort();
    }
}

async fn handle_http_request(mut socket: tokio::net::TcpStream, state: Arc<TestHttpState>) {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let (header_end, content_length) = loop {
        let count = match socket.read(&mut buffer).await {
            Ok(count) if count > 0 => count,
            _ => return,
        };
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = header
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            break (header_end + 4, content_length);
        }
    };
    while bytes.len() < header_end.saturating_add(content_length) {
        let count = match socket.read(&mut buffer).await {
            Ok(count) if count > 0 => count,
            _ => return,
        };
        bytes.extend_from_slice(&buffer[..count]);
    }
    let header = String::from_utf8_lossy(&bytes[..header_end]);
    let path = header
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or_default()
        .to_string();
    let body = &bytes[header_end..header_end + content_length];
    let json: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    let text = json
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let message_id = json.get("message_id").and_then(serde_json::Value::as_i64);
    state
        .requests
        .lock()
        .expect("request list should not be poisoned")
        .push((path.clone(), text.clone(), message_id));

    let fail = if path.ends_with("sendMessage") {
        state.fail_send.load(Ordering::Acquire)
    } else if path.ends_with("editMessageText") {
        let mut selector = state
            .fail_edit_containing
            .lock()
            .expect("failure selector should not be poisoned");
        match selector.as_mut() {
            Some((needle, None)) if text.contains(needle.as_str()) => true,
            Some((needle, Some(remaining))) if *remaining > 0 && text.contains(needle.as_str()) => {
                *remaining -= 1;
                true
            }
            _ => false,
        }
    } else {
        false
    };
    let response_body = if fail {
        r#"{"ok":false,"description":"synthetic delivery failure"}"#.to_string()
    } else if path.ends_with("sendMessage") {
        let message_id = state.next_message_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!(
            r#"{{"ok":true,"result":{{"message_id":{message_id},"chat":{{"id":1,"type":"private"}},"text":"status"}}}}"#
        )
    } else {
        r#"{"ok":true,"result":true}"#.to_string()
    };
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

#[derive(Default)]
pub(crate) struct DownloadGate {
    started: (Mutex<bool>, Condvar),
    released: (Mutex<bool>, Condvar),
}

impl DownloadGate {
    pub(crate) fn wait_until_started(&self) {
        let (lock, condvar) = &self.started;
        let mut started = lock.lock().expect("download gate should not be poisoned");
        while !*started {
            started = condvar
                .wait(started)
                .expect("download gate should not be poisoned");
        }
    }

    pub(crate) fn release(&self) {
        let (lock, condvar) = &self.released;
        *lock.lock().expect("download gate should not be poisoned") = true;
        condvar.notify_all();
    }
}

pub(crate) struct MockBackend {
    pub(crate) archive_root: PathBuf,
    pub(crate) original_sha: Mutex<String>,
    pub(crate) import_sha: Mutex<String>,
    pub(crate) downloads: AtomicUsize,
    pub(crate) archives: AtomicUsize,
    pub(crate) adds: AtomicUsize,
    pub(crate) queries: AtomicUsize,
    pub(crate) verifies: AtomicUsize,
    pub(crate) archive_status_reads: AtomicUsize,
    pub(crate) evictions: AtomicUsize,
    pub(crate) ready_failure: AtomicBool,
    pub(crate) add_failure: AtomicBool,
    pub(crate) defer_eviction: AtomicBool,
    pub(crate) cloud_status: Mutex<String>,
    pub(crate) archive_states: Mutex<VecDeque<ArchiveState>>,
    pub(crate) archive_default: Mutex<ArchiveState>,
    pub(crate) download_gate: Mutex<Option<Arc<DownloadGate>>>,
    pub(crate) download_failure: Mutex<Option<MusicDownloadFailure>>,
}

impl MockBackend {
    pub(crate) fn new(archive_root: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            archive_default: Mutex::new(ArchiveState {
                path: archive_root.join("music-default.m4a"),
                uploaded: true,
                uploading: false,
                downloading: false,
                conflicts: false,
                local_bytes: true,
            }),
            archive_root,
            original_sha: Mutex::new("a".repeat(64)),
            import_sha: Mutex::new("b".repeat(64)),
            downloads: AtomicUsize::new(0),
            archives: AtomicUsize::new(0),
            adds: AtomicUsize::new(0),
            queries: AtomicUsize::new(0),
            verifies: AtomicUsize::new(0),
            archive_status_reads: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
            ready_failure: AtomicBool::new(false),
            add_failure: AtomicBool::new(false),
            defer_eviction: AtomicBool::new(false),
            cloud_status: Mutex::new("uploaded".to_string()),
            archive_states: Mutex::new(VecDeque::new()),
            download_gate: Mutex::new(None),
            download_failure: Mutex::new(None),
        })
    }

    pub(crate) fn set_archive_states(&self, states: impl IntoIterator<Item = ArchiveState>) {
        *self
            .archive_states
            .lock()
            .expect("archive state queue should not be poisoned") = states.into_iter().collect();
    }

    pub(crate) fn set_archive_default(&self, state: ArchiveState) {
        *self
            .archive_default
            .lock()
            .expect("archive default should not be poisoned") = state;
    }
}

impl MusicBackend for MockBackend {
    fn download(
        &self,
        _upload: &MusicUpload,
        destination: &Path,
        cancel: &MusicCancellation,
    ) -> Result<()> {
        self.downloads.fetch_add(1, Ordering::Relaxed);
        if let Some(failure) = *self
            .download_failure
            .lock()
            .expect("download failure should not be poisoned")
        {
            return Err(failure.into());
        }
        if let Some(gate) = self
            .download_gate
            .lock()
            .expect("download gate should not be poisoned")
            .clone()
        {
            fs::write(destination, b"retained partial bytes")?;
            let (started_lock, started_condvar) = &gate.started;
            *started_lock
                .lock()
                .expect("download gate should not be poisoned") = true;
            started_condvar.notify_all();
            let (release_lock, release_condvar) = &gate.released;
            let mut released = release_lock
                .lock()
                .expect("download gate should not be poisoned");
            while !*released && !cancel.is_cancelled() {
                let (next, _) = release_condvar
                    .wait_timeout(released, Duration::from_millis(10))
                    .expect("download gate should not be poisoned");
                released = next;
            }
            if cancel.is_cancelled() {
                bail!("synthetic cancellation after a retained partial download");
            }
        }
        fs::write(destination, b"complete original audio!")?;
        Ok(())
    }

    fn prepare(
        &self,
        original: &Path,
        working_dir: &Path,
        cancel: &MusicCancellation,
    ) -> Result<PreparedAudio> {
        if cancel.is_cancelled() {
            bail!("synthetic cancellation before audio preparation");
        }
        let import_path = working_dir.join("prepared.m4a");
        fs::write(&import_path, b"prepared import bytes")?;
        Ok(PreparedAudio {
            original: AudioBytes {
                path: original.to_path_buf(),
                sha256: self
                    .original_sha
                    .lock()
                    .expect("original hash should not be poisoned")
                    .clone(),
                size: 24,
            },
            import: AudioBytes {
                path: import_path,
                sha256: self
                    .import_sha
                    .lock()
                    .expect("import hash should not be poisoned")
                    .clone(),
                size: 22,
            },
        })
    }

    fn archive(&self, _audio: &PreparedAudio, original_name: &str) -> Result<ArchiveState> {
        self.archives.fetch_add(1, Ordering::Relaxed);
        let path = self.archive_root.join(original_name);
        fs::write(&path, b"archive bytes")?;
        let mut state = self
            .archive_default
            .lock()
            .expect("archive default should not be poisoned")
            .clone();
        state.path = path;
        Ok(state)
    }

    fn check_music_ready(&self) -> Result<()> {
        if self.ready_failure.load(Ordering::Acquire) {
            bail!("synthetic Music authorization is missing");
        }
        Ok(())
    }

    fn add_track(&self, _audio: &PreparedAudio) -> Result<String> {
        self.adds.fetch_add(1, Ordering::Relaxed);
        if self.add_failure.load(Ordering::Acquire) {
            bail!("synthetic ambiguous Music add failure");
        }
        Ok("persistent-id-1".to_string())
    }

    fn query_track(&self, persistent_id: &str) -> Result<MusicTrack> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        Ok(MusicTrack {
            persistent_id: persistent_id.to_string(),
            location: Some(PathBuf::from("/Music/Accepted Track.m4a")),
            cloud_status: self
                .cloud_status
                .lock()
                .expect("cloud status should not be poisoned")
                .clone(),
        })
    }

    fn verify_copy(&self, _audio: &PreparedAudio, _track: &MusicTrack) -> Result<()> {
        self.verifies.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn archive_status(&self, path: &Path) -> Result<ArchiveState> {
        self.archive_status_reads.fetch_add(1, Ordering::Relaxed);
        if let Some(mut state) = self
            .archive_states
            .lock()
            .expect("archive state queue should not be poisoned")
            .pop_front()
        {
            state.path = path.to_path_buf();
            return Ok(state);
        }
        let mut state = self
            .archive_default
            .lock()
            .expect("archive default should not be poisoned")
            .clone();
        state.path = path.to_path_buf();
        Ok(state)
    }

    fn evict_archive(&self, archive: &ArchiveState, _track: &MusicTrack) -> Result<ArchiveState> {
        self.evictions.fetch_add(1, Ordering::Relaxed);
        Ok(ArchiveState {
            local_bytes: self.defer_eviction.load(Ordering::Acquire),
            ..archive.clone()
        })
    }
}

pub(crate) fn upload(message_id: i64, file_unique_id: &str) -> MusicUpload {
    MusicUpload {
        update_id: message_id,
        chat_id: 42,
        message_id,
        user_id: 42,
        file_id: format!("file_id_{message_id}"),
        file_unique_id: file_unique_id.to_string(),
        file_name: format!("recording-{message_id}.M4A"),
        file_size: 24,
        mime_type: Some("audio/mp4".to_string()),
    }
}

pub(crate) async fn open_manager(
    settings: MusicSettings,
    server: &TestHttpServer,
    backend: Arc<dyn MusicBackend>,
) -> Arc<MusicManager> {
    MusicManager::open(settings, server.client.clone(), backend).expect("music manager should open")
}

async fn wait_for_stage(manager: &MusicManager, job_id: &str, stage: JobStage) -> StoredJob {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(job) = manager.store.job(job_id).expect("job should read")
                && job.stage == stage
            {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("job did not reach expected stage")
}

async fn wait_for_complete(manager: &MusicManager, job_id: &str) -> StoredJob {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(job) = manager.store.job(job_id).expect("job should read")
                && job.stage == JobStage::Complete
            {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("job did not complete")
}

fn shutdown_channel() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

#[tokio::test]
async fn admission_is_durable_idempotent_and_rejects_conflicting_origin_metadata() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let settings = fixture.settings();
    let manager = open_manager(settings.clone(), &server, backend).await;

    let first = manager
        .enqueue(upload(10, "unique-10"))
        .await
        .expect("first upload should be admitted");
    assert!(first.created);
    let replay = manager
        .enqueue(upload(10, "unique-10"))
        .await
        .expect("replayed upload should be recognized");
    assert!(!replay.created);
    assert_eq!(first.job_id, replay.job_id);

    let mut conflict = upload(10, "unique-10");
    conflict.file_id = "different_file_id".to_string();
    assert!(manager.enqueue(conflict).await.is_err());
    drop(manager);

    let restarted = open_manager(
        settings,
        &server,
        MockBackend::new(fixture.settings().archive_dir),
    )
    .await;
    assert!(
        restarted
            .store
            .job(&first.job_id)
            .expect("job should read")
            .is_some()
    );
    let summary = restarted
        .summary(42)
        .await
        .expect("summary should be available");
    assert!(summary.contains(&first.job_id));
    server.stop();
}

#[tokio::test]
async fn restart_preserves_failed_and_cancelled_jobs_while_resuming_queued_work() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let settings = fixture.settings();
    let manager = open_manager(
        settings.clone(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let failed = manager.enqueue(upload(12, "failed-12")).await.unwrap();
    let cancelled = manager.enqueue(upload(13, "cancelled-13")).await.unwrap();
    let queued = manager.enqueue(upload(14, "queued-14")).await.unwrap();
    manager
        .store
        .set_job(&failed.job_id, |job| {
            job.stage = JobStage::Failed;
            job.error = Some("A safe failure reason.".to_string());
        })
        .expect("failed state should persist");
    manager
        .store
        .set_job(&cancelled.job_id, |job| {
            job.stage = JobStage::Cancelled;
            job.cancel_requested = true;
        })
        .expect("cancelled state should persist");
    drop(manager);

    let restarted = open_manager(settings, &server, backend.clone() as Arc<dyn MusicBackend>).await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = restarted.start(receiver);
    wait_for_complete(&restarted, &queued.job_id).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        restarted.store.job(&failed.job_id).unwrap().unwrap().stage,
        JobStage::Failed
    );
    assert_eq!(
        restarted
            .store
            .job(&cancelled.job_id)
            .unwrap()
            .unwrap()
            .stage,
        JobStage::Cancelled
    );
    assert_eq!(backend.downloads.load(Ordering::Relaxed), 1);
    assert_eq!(backend.archives.load(Ordering::Relaxed), 1);
    assert_eq!(backend.adds.load(Ordering::Relaxed), 1);

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn only_one_manager_can_own_a_music_state_directory() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let settings = fixture.settings();
    let first = MusicManager::open(
        settings.clone(),
        server.client.clone(),
        MockBackend::new(settings.archive_dir.clone()),
    )
    .expect("first manager should acquire ownership");
    let second = MusicManager::open(
        settings.clone(),
        server.client.clone(),
        MockBackend::new(settings.archive_dir),
    );
    assert!(second.is_err());
    drop(first);
    server.stop();
}

#[tokio::test]
async fn blocked_download_does_not_block_durable_admission_of_another_job() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let gate = Arc::new(DownloadGate::default());
    *backend
        .download_gate
        .lock()
        .expect("download gate should not be poisoned") = Some(Arc::clone(&gate));
    let manager = open_manager(fixture.settings(), &server, backend).await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let first = manager
        .enqueue(upload(20, "unique-20"))
        .await
        .expect("first job should be admitted");
    let gate_for_wait = Arc::clone(&gate);
    tokio::task::spawn_blocking(move || gate_for_wait.wait_until_started())
        .await
        .expect("download wait should finish");
    let second = tokio::time::timeout(
        Duration::from_secs(2),
        manager.enqueue(upload(21, "unique-21")),
    )
    .await
    .expect("second durable admission must not wait for download")
    .expect("second job should be admitted");
    assert!(second.created);
    assert!(
        manager
            .store
            .job(&second.job_id)
            .expect("job should read")
            .is_some()
    );

    gate.release();
    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
    let _ = first;
}

#[tokio::test]
async fn an_existing_complete_original_is_prepared_without_redownloading() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let admission = manager.enqueue(upload(25, "unique-25")).await.unwrap();
    let job_dir = manager.settings.staging_dir.join(&admission.job_id);
    ensure_private_directory(&job_dir, manager.store.provider.as_ref()).unwrap();
    let original_path = job_dir.join("original.m4a");
    fs::write(&original_path, b"complete original audio!").unwrap();

    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);
    wait_for_complete(&manager, &admission.job_id).await;
    assert_eq!(backend.downloads.load(Ordering::Relaxed), 0);
    assert_eq!(
        manager
            .store
            .job(&admission.job_id)
            .unwrap()
            .unwrap()
            .download_path
            .as_deref(),
        Some(original_path.as_path())
    );

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn original_hash_dedupe_reuses_saved_import_metadata_and_archive_without_second_add() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let first = manager.enqueue(upload(30, "unique-30")).await.unwrap();
    wait_for_complete(&manager, &first.job_id).await;
    let second = manager.enqueue(upload(31, "unique-31")).await.unwrap();
    wait_for_complete(&manager, &second.job_id).await;

    assert_eq!(backend.archives.load(Ordering::Relaxed), 1);
    assert_eq!(backend.adds.load(Ordering::Relaxed), 1);
    assert_eq!(backend.verifies.load(Ordering::Relaxed), 4);
    assert_eq!(manager.store.snapshot().unwrap().content_catalog.len(), 1);

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn saved_pid_survives_a_failed_status_edit_after_import() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    server.fail_edits_containing("checking Music.");
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let admission = manager.enqueue(upload(40, "unique-40")).await.unwrap();
    assert!(server.wait_for_text("checking Music.").await);
    let state = manager.store.snapshot().expect("state should read");
    let job = state
        .jobs
        .get(&admission.job_id)
        .expect("job should persist");
    assert_eq!(job.persistent_id.as_deref(), Some("persistent-id-1"));
    let catalog = state
        .content_catalog
        .get(&"a".repeat(64))
        .expect("content catalog entry should persist");
    assert_eq!(catalog.persistent_id.as_deref(), Some("persistent-id-1"));

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn ambiguous_add_is_never_retried_and_duplicate_content_never_adds_again() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    backend.add_failure.store(true, Ordering::Release);
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let first = manager.enqueue(upload(50, "unique-50")).await.unwrap();
    wait_for_stage(&manager, &first.job_id, JobStage::OutcomeUnknown).await;
    assert!(manager.retry(42, &first.job_id).await.is_err());
    let second = manager.enqueue(upload(51, "unique-51")).await.unwrap();
    wait_for_stage(&manager, &second.job_id, JobStage::OutcomeUnknown).await;
    assert_eq!(backend.adds.load(Ordering::Relaxed), 1);
    assert_eq!(backend.archives.load(Ordering::Relaxed), 1);
    let catalog = manager.store.snapshot().unwrap().content_catalog;
    assert!(catalog.get(&"a".repeat(64)).unwrap().add_intent);
    assert!(
        catalog
            .get(&"a".repeat(64))
            .unwrap()
            .persistent_id
            .is_none()
    );

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn unknown_music_cloud_status_waits_without_archive_verification_or_eviction() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    *backend
        .cloud_status
        .lock()
        .expect("cloud status should not be poisoned") = "unknown".to_string();
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let admission = manager.enqueue(upload(60, "unique-60")).await.unwrap();
    wait_for_stage(&manager, &admission.job_id, JobStage::WaitingMusicCloud).await;
    assert_eq!(backend.archive_status_reads.load(Ordering::Relaxed), 0);
    assert_eq!(backend.verifies.load(Ordering::Relaxed), 0);
    assert_eq!(backend.evictions.load(Ordering::Relaxed), 0);

    *backend
        .cloud_status
        .lock()
        .expect("cloud status should not be poisoned") = "uploaded".to_string();
    wait_for_complete(&manager, &admission.job_id).await;
    assert_eq!(backend.evictions.load(Ordering::Relaxed), 1);

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn archive_upload_and_conflict_gates_precede_eviction() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let pending = ArchiveState {
        path: fixture.settings().archive_dir.join("pending.m4a"),
        uploaded: false,
        uploading: true,
        downloading: false,
        conflicts: false,
        local_bytes: true,
    };
    let stable = ArchiveState {
        path: fixture.settings().archive_dir.join("stable.m4a"),
        uploaded: true,
        uploading: false,
        downloading: false,
        conflicts: false,
        local_bytes: true,
    };
    let conflicted = ArchiveState {
        path: fixture.settings().archive_dir.join("conflicted.m4a"),
        uploaded: true,
        uploading: false,
        downloading: false,
        conflicts: true,
        local_bytes: true,
    };
    backend.set_archive_states([pending.clone(), conflicted, stable]);
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let admission = manager.enqueue(upload(70, "unique-70")).await.unwrap();
    wait_for_stage(&manager, &admission.job_id, JobStage::WaitingArchiveCloud).await;
    assert_eq!(backend.evictions.load(Ordering::Relaxed), 0);
    wait_for_stage(&manager, &admission.job_id, JobStage::Failed).await;
    assert_eq!(backend.evictions.load(Ordering::Relaxed), 0);
    manager.retry(42, &admission.job_id).await.unwrap();
    wait_for_complete(&manager, &admission.job_id).await;
    assert_eq!(backend.evictions.load(Ordering::Relaxed), 1);
    assert!(backend.verifies.load(Ordering::Relaxed) >= 2);

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn unchanged_archive_eviction_waits_for_a_later_safe_retry() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    backend.defer_eviction.store(true, Ordering::Release);
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let admission = manager.enqueue(upload(75, "unique-75")).await.unwrap();
    wait_for_stage(&manager, &admission.job_id, JobStage::WaitingArchiveCloud).await;
    assert_eq!(backend.evictions.load(Ordering::Relaxed), 1);
    backend.defer_eviction.store(false, Ordering::Release);
    wait_for_complete(&manager, &admission.job_id).await;
    assert_eq!(backend.evictions.load(Ordering::Relaxed), 2);

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn preflight_failure_is_retryable_without_a_catalog_add_intent() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    backend.ready_failure.store(true, Ordering::Release);
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let admission = manager.enqueue(upload(80, "unique-80")).await.unwrap();
    wait_for_stage(&manager, &admission.job_id, JobStage::Failed).await;
    assert!(
        !manager
            .store
            .snapshot()
            .unwrap()
            .content_catalog
            .get(&"a".repeat(64))
            .unwrap()
            .add_intent
    );

    backend.ready_failure.store(false, Ordering::Release);
    manager.retry(42, &admission.job_id).await.unwrap();
    wait_for_complete(&manager, &admission.job_id).await;
    assert_eq!(backend.adds.load(Ordering::Relaxed), 1);

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn cancellation_keeps_partial_download_and_does_not_clean_fragments() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let gate = Arc::new(DownloadGate::default());
    *backend
        .download_gate
        .lock()
        .expect("download gate should not be poisoned") = Some(Arc::clone(&gate));
    let manager = open_manager(fixture.settings(), &server, backend).await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let admission = manager.enqueue(upload(90, "unique-90")).await.unwrap();
    let gate_for_wait = Arc::clone(&gate);
    tokio::task::spawn_blocking(move || gate_for_wait.wait_until_started())
        .await
        .expect("download should start");
    let in_progress = manager
        .store
        .job(&admission.job_id)
        .expect("job should read")
        .expect("job should exist");
    let original_path = in_progress
        .download_path
        .expect("stable download path should persist");
    manager.cancel(42, &admission.job_id).await.unwrap();
    gate.release();
    wait_for_stage(&manager, &admission.job_id, JobStage::Cancelled).await;
    assert_eq!(fs::read(&original_path).unwrap(), b"retained partial bytes");

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn expired_telegram_file_reference_guides_resend_without_archiving_or_importing() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    *backend
        .download_failure
        .lock()
        .expect("download failure should not be poisoned") =
        Some(MusicDownloadFailure::RefreshRequired);
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let admission = manager.enqueue(upload(95, "unique-95")).await.unwrap();
    let failed = wait_for_stage(&manager, &admission.job_id, JobStage::Failed).await;
    let guidance = MusicDownloadFailure::RefreshRequired.safe_message();
    assert_eq!(failed.error.as_deref(), Some(guidance));
    assert!(
        server
            .wait_for_text("ask the sender to resend the audio")
            .await
    );
    assert_eq!(backend.downloads.load(Ordering::Relaxed), 1);
    assert_eq!(backend.archives.load(Ordering::Relaxed), 0);
    assert_eq!(backend.adds.load(Ordering::Relaxed), 0);

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn failed_uncertain_status_send_is_not_repeated() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    server.fail_sends(true);
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let manager = open_manager(fixture.settings(), &server, backend).await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let admission = manager.enqueue(upload(100, "unique-100")).await.unwrap();
    wait_for_complete(&manager, &admission.job_id).await;
    assert_eq!(server.count_method("sendMessage"), 1);
    assert_eq!(server.count_method("editMessageText"), 0);

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn failed_terminal_status_edit_retries_in_place_until_confirmed() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    server.fail_next_edit_containing("complete.");
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend as Arc<dyn MusicBackend>,
    )
    .await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let admission = manager.enqueue(upload(105, "unique-105")).await.unwrap();
    wait_for_complete(&manager, &admission.job_id).await;
    let confirmed = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let job = manager
                .store
                .job(&admission.job_id)
                .expect("job should read")
                .expect("job should exist");
            if job
                .status_text
                .as_deref()
                .is_some_and(|text| text.ends_with("complete."))
                && job.status_pending_text.is_none()
            {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("terminal status edit should eventually be confirmed");
    assert!(
        confirmed
            .status_text
            .as_deref()
            .unwrap()
            .ends_with("complete.")
    );
    assert_eq!(server.count_method("sendMessage"), 1);
    assert_eq!(
        server.count_text_for_method("editMessageText", "complete."),
        2,
        "one transient failure should be followed by one successful retry"
    );
    let edit_ids = server.edited_message_ids();
    assert!(!edit_ids.is_empty());
    assert!(edit_ids.iter().all(|message_id| *message_id == edit_ids[0]));

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn completed_catalog_seed_reuses_saved_import_and_archive_without_importing() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    *backend
        .original_sha
        .lock()
        .expect("original hash should not be poisoned") = "c".repeat(64);
    *backend
        .import_sha
        .lock()
        .expect("import hash should not be poisoned") = "d".repeat(64);
    let settings = fixture.settings();
    let manager = open_manager(
        settings.clone(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let prepared_path = settings.staging_dir.join("existing-import.m4a");
    fs::create_dir_all(&settings.staging_dir).unwrap();
    fs::write(&prepared_path, b"accepted import").unwrap();
    let archive_path = settings.archive_dir.join("old6.m4a");
    fs::write(&archive_path, b"accepted archive").unwrap();
    let audio = PreparedAudio {
        original: AudioBytes {
            path: settings.staging_dir.join("existing-original.m4a"),
            sha256: "c".repeat(64),
            size: 100,
        },
        import: AudioBytes {
            path: prepared_path,
            sha256: "d".repeat(64),
            size: 90,
        },
    };
    let archive = ArchiveState {
        path: archive_path,
        uploaded: true,
        uploading: false,
        downloading: false,
        conflicts: false,
        local_bytes: false,
    };
    backend.set_archive_default(archive.clone());
    let track = MusicTrack {
        persistent_id: "persistent-id-seeded".to_string(),
        location: Some(PathBuf::from("/Music/Existing.m4a")),
        cloud_status: "matched".to_string(),
    };
    manager
        .seed_completed(&audio, &archive, &track)
        .await
        .expect("verified accepted content should seed the catalog");
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);
    let admission = manager.enqueue(upload(110, "unique-110")).await.unwrap();
    wait_for_complete(&manager, &admission.job_id).await;
    let job = manager.store.job(&admission.job_id).unwrap().unwrap();
    assert_eq!(job.persistent_id.as_deref(), Some("persistent-id-seeded"));
    assert_eq!(job.prepared.as_ref().unwrap().import.sha256, "d".repeat(64));
    assert_eq!(
        job.archive.as_ref().unwrap().path,
        settings.archive_dir.join("old6.m4a")
    );
    assert_eq!(backend.adds.load(Ordering::Relaxed), 0);
    assert_eq!(backend.archives.load(Ordering::Relaxed), 0);
    assert_eq!(backend.evictions.load(Ordering::Relaxed), 0);

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}
