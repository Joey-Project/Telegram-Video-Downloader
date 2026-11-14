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
        if let Ok(metadata) = fs::symlink_metadata(destination)
            && metadata.nlink() > 1
        {
            if metadata.nlink() == 2
                && metadata.is_file()
                && metadata.len() == 24
                && fs::read(destination)? == b"complete original audio!"
            {
                // Model the helper's known receipt, inode check and partial unlink.
                let partial = destination.with_file_name("helper-completed.part");
                let partial_metadata = fs::symlink_metadata(&partial)?;
                if !partial_metadata.is_file()
                    || partial_metadata.dev() != metadata.dev()
                    || partial_metadata.ino() != metadata.ino()
                {
                    bail!("synthetic helper rejected a mismatched publication partial");
                }
                fs::remove_file(partial)?;
                return Ok(());
            }
            bail!("synthetic helper rejected an unverified linked destination");
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
            conversion: None,
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

#[cfg(target_os = "macos")]
struct NativeFlacConversionBackend {
    native: crate::music_native::NativeMusicBackend,
    source_fixture: PathBuf,
    archive_root: PathBuf,
    music_copy: PathBuf,
    downloads: AtomicUsize,
    preparations: AtomicUsize,
    readiness_checks: AtomicUsize,
    adds: AtomicUsize,
    verifications: AtomicUsize,
    evictions: AtomicUsize,
    prepare_summary: std::sync::Mutex<Option<String>>,
}

#[cfg(target_os = "macos")]
impl NativeFlacConversionBackend {
    fn new(settings: MusicSettings, source_fixture: PathBuf, root: &Path) -> Self {
        Self {
            native: crate::music_native::NativeMusicBackend::new(settings.clone()),
            source_fixture,
            archive_root: settings.archive_dir,
            music_copy: root.join("mock-music-library/converted.m4a"),
            downloads: AtomicUsize::new(0),
            preparations: AtomicUsize::new(0),
            readiness_checks: AtomicUsize::new(0),
            adds: AtomicUsize::new(0),
            verifications: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
            prepare_summary: std::sync::Mutex::new(None),
        }
    }
}

#[cfg(target_os = "macos")]
impl MusicBackend for NativeFlacConversionBackend {
    fn download(
        &self,
        _upload: &MusicUpload,
        destination: &Path,
        _cancel: &MusicCancellation,
    ) -> Result<()> {
        self.downloads.fetch_add(1, Ordering::Relaxed);
        fs::copy(&self.source_fixture, destination)?;
        Ok(())
    }

    fn prepare(
        &self,
        original: &Path,
        working_dir: &Path,
        cancel: &MusicCancellation,
    ) -> Result<PreparedAudio> {
        self.preparations.fetch_add(1, Ordering::Relaxed);
        let result = self.native.prepare(original, working_dir, cancel);
        let summary = match &result {
            Ok(prepared) => match &prepared.conversion {
                Some(report) => format!(
                    "Ok: original={} bytes sha256={}, import={} bytes sha256={}, converter={} -> {} bits, {} Hz, {} channels, {} frames, bit_exact={}, output_path={}, warnings={:?}, mapped={:?}, preserved={:?}",
                    prepared.original.size,
                    prepared.original.sha256,
                    prepared.import.size,
                    prepared.import.sha256,
                    report.source.bits_per_sample,
                    report.output.bits_per_sample,
                    report.source.sample_rate,
                    report.source.channels,
                    report.source.frames,
                    report.audio_bit_exact,
                    report.output_path.display(),
                    report.warnings,
                    report.metadata.mapped,
                    report.metadata.preserved,
                ),
                None => format!(
                    "Ok without conversion report: original={} bytes, import={} bytes",
                    prepared.original.size, prepared.import.size
                ),
            },
            Err(error) => format!("Err: {error:#}"),
        };
        *self
            .prepare_summary
            .lock()
            .expect("prepare summary should not be poisoned") = Some(summary);
        result
    }

    fn archive(&self, audio: &PreparedAudio, original_name: &str) -> Result<ArchiveState> {
        if !Path::new(original_name)
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("flac"))
        {
            bail!("the original FLAC extension was not retained for the archive");
        }
        fs::create_dir_all(&self.archive_root)?;
        let filename = crate::music_native::archive_filename(original_name, &audio.original)?;
        let path = self.archive_root.join(filename);
        fs::copy(&audio.original.path, &path)?;
        Ok(ArchiveState {
            path,
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: true,
        })
    }

    fn check_music_ready(&self) -> Result<()> {
        if self.readiness_checks.fetch_add(1, Ordering::Relaxed) == 0 {
            bail!("synthetic Music readiness failure before add intent");
        }
        Ok(())
    }

    fn check_import_source(&self, audio: &PreparedAudio) -> Result<()> {
        if !audio
            .import
            .path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("m4a"))
            || fs::metadata(&audio.import.path)?.len() == 0
        {
            bail!("converted ALAC import file is missing");
        }
        Ok(())
    }

    fn add_track(&self, audio: &PreparedAudio) -> Result<String> {
        self.adds.fetch_add(1, Ordering::Relaxed);
        if !audio
            .import
            .path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("m4a"))
        {
            bail!("Music received a non-M4A import file");
        }
        fs::create_dir_all(self.music_copy.parent().unwrap())?;
        fs::copy(&audio.import.path, &self.music_copy)?;
        Ok("persistent-id-1".to_string())
    }

    fn query_track(&self, persistent_id: &str) -> Result<MusicTrack> {
        Ok(MusicTrack {
            persistent_id: persistent_id.to_string(),
            location: Some(self.music_copy.clone()),
            cloud_status: "uploaded".to_string(),
        })
    }

    fn verify_copy(&self, audio: &PreparedAudio, track: &MusicTrack) -> Result<()> {
        self.verifications.fetch_add(1, Ordering::Relaxed);
        let location = track
            .location
            .as_deref()
            .context("mock Music track has no local file")?;
        if fs::read(&audio.import.path)? != fs::read(location)? {
            bail!("mock Music copy differs from the converted ALAC bytes");
        }
        Ok(())
    }

    fn archive_status(&self, path: &Path) -> Result<ArchiveState> {
        Ok(ArchiveState {
            path: path.to_path_buf(),
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: true,
        })
    }

    fn evict_archive(&self, archive: &ArchiveState, _track: &MusicTrack) -> Result<ArchiveState> {
        self.evictions.fetch_add(1, Ordering::Relaxed);
        Ok(ArchiveState {
            local_bytes: false,
            ..archive.clone()
        })
    }
}

#[cfg(target_os = "macos")]
fn configured_flac_converter() -> Option<PathBuf> {
    let Some(path) = std::env::var_os("FLAC_ALAC_TEST_CONVERTER").map(PathBuf::from) else {
        if std::env::var_os("CI").is_some() {
            panic!("FLAC_ALAC_TEST_CONVERTER is required in CI");
        }
        eprintln!("SKIP: FLAC_ALAC_TEST_CONVERTER is unset; native FLAC manager E2E was not run");
        return None;
    };
    assert!(
        path.is_file(),
        "FLAC_ALAC_TEST_CONVERTER does not name a file: {}",
        path.display()
    );
    Some(fs::canonicalize(path).expect("converter path should canonicalize"))
}

#[cfg(target_os = "macos")]
fn absolute_test_tool(name: &str) -> PathBuf {
    let search_path = std::env::var_os("PATH").expect("PATH should be available for test tools");
    for directory in std::env::split_paths(&search_path) {
        let candidate = directory.join(name);
        if candidate.is_file() {
            return fs::canonicalize(&candidate)
                .unwrap_or_else(|error| panic!("test tool {name} should canonicalize: {error}"));
        }
    }
    panic!("required test tool {name} was not found on PATH");
}

#[cfg(target_os = "macos")]
fn create_eight_bit_stereo_flac(directory: &Path) -> PathBuf {
    let wave = directory.join("lossless source.wav");
    let flac = directory.join("lossless source.flac");
    let channels = 2_u16;
    let sample_rate = 48_000_u32;
    let bits_per_sample = 8_u16;
    let frames = 960_u32;
    let pcm = (0..frames * u32::from(channels))
        .map(|index| ((index * 37 + 3) & 0xff) as u8)
        .collect::<Vec<_>>();
    let block_align = channels * (bits_per_sample / 8);
    let byte_rate = sample_rate * u32::from(block_align);
    let riff_size = 36_u32 + pcm.len() as u32;
    let mut bytes = Vec::with_capacity(44 + pcm.len());
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&riff_size.to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16_u32.to_le_bytes());
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&channels.to_le_bytes());
    bytes.extend_from_slice(&sample_rate.to_le_bytes());
    bytes.extend_from_slice(&byte_rate.to_le_bytes());
    bytes.extend_from_slice(&block_align.to_le_bytes());
    bytes.extend_from_slice(&bits_per_sample.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&(pcm.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&pcm);
    fs::write(&wave, bytes).expect("test WAV should be written");

    let encoded = std::process::Command::new("flac")
        .args(["--silent", "--verify", "--output-name"])
        .arg(&flac)
        .arg(&wave)
        .output()
        .expect("flac fixture encoder should be installed");
    assert!(
        encoded.status.success(),
        "flac fixture encoding failed: {}",
        String::from_utf8_lossy(&encoded.stderr)
    );
    let tagged = std::process::Command::new("metaflac")
        .arg("--set-tag=TITLE=FLAC integration title")
        .arg("--set-tag=ARTIST=Native ALAC test artist")
        .arg(&flac)
        .output()
        .expect("metaflac fixture tagger should be installed");
    assert!(
        tagged.status.success(),
        "metaflac fixture tagging failed: {}",
        String::from_utf8_lossy(&tagged.stderr)
    );
    flac
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

fn stored_job(upload: MusicUpload, stage: JobStage, created_at: u64) -> StoredJob {
    let (job_id, origin_key) = job_identity(&upload);
    StoredJob {
        job_id,
        origin_key,
        upload,
        stage,
        created_at,
        updated_at: created_at,
        attempt: 0,
        prepare_attempt: 0,
        archive_attempt: 0,
        download_path: None,
        download_complete: false,
        prepared: None,
        archive: None,
        persistent_id: None,
        track: None,
        local_copy_verified: false,
        add_intent: false,
        cancel_requested: false,
        next_poll_at: None,
        error: None,
        status_message_id: None,
        status_send_attempted: false,
        status_text: None,
        status_pending_text: None,
        status_edit_attempts: 0,
        status_next_attempt_at: None,
    }
}

/// Seed a full valid active queue in one atomic state write for Telegram intake tests.
pub(crate) fn seed_full_active_queue(settings: &MusicSettings) {
    let store = MusicStore::open(settings, platform_queue_file_provider())
        .expect("music store should open for capacity fixture");
    store
        .transact(|state| {
            let now = unix_seconds();
            for index in 0..MAX_ACTIVE_JOBS {
                let upload = upload(10_000 + index as i64, &format!("capacity-{index}"));
                let job = stored_job(upload, JobStage::Queued, now);
                state.jobs.insert(job.job_id.clone(), job);
            }
            Ok(())
        })
        .expect("full active queue should persist in one transaction");
}

fn seed_full_completed_history(settings: &MusicSettings) -> (String, String, PathBuf) {
    let store = MusicStore::open(settings, platform_queue_file_provider())
        .expect("music store should open for completed-history fixture");
    let failed_upload = upload(20_001, "history-1");
    let (failed_job_id, _) = job_identity(&failed_upload);
    let failed_job_dir = settings.staging_dir.join(&failed_job_id);
    ensure_private_directory(&failed_job_dir, store.provider.as_ref())
        .expect("failed job staging should be private");
    let retained_partial = failed_job_dir.join("original.m4a");
    fs::write(&retained_partial, b"retained partial bytes")
        .expect("failed job partial download should persist");

    let first_job_id = store
        .transact(|state| {
            let now = unix_seconds();
            let mut first_job_id = String::new();
            for index in 0..MAX_JOBS {
                let upload = upload(20_000 + index as i64, &format!("history-{index}"));
                let mut job = stored_job(
                    upload,
                    if index == 1 {
                        JobStage::Failed
                    } else {
                        JobStage::Complete
                    },
                    now.saturating_sub((MAX_JOBS - index) as u64),
                );
                if index == 1 {
                    job.attempt = 1;
                    job.download_path = Some(retained_partial.clone());
                    job.error = Some("The partial download was retained for retry.".to_string());
                } else {
                    let original_sha = if index == 0 {
                        "a".repeat(64)
                    } else {
                        format!("{:064x}", index + 1)
                    };
                    let prepared = PreparedAudio {
                        conversion: None,
                        original: AudioBytes {
                            path: settings.staging_dir.join(format!("source-{index}.m4a")),
                            sha256: original_sha.clone(),
                            size: 24,
                        },
                        import: AudioBytes {
                            path: settings.staging_dir.join(format!("import-{index}.m4a")),
                            sha256: "b".repeat(64),
                            size: 22,
                        },
                    };
                    let archive = ArchiveState {
                        path: settings.archive_dir.join(format!("seed-{index}.m4a")),
                        uploaded: true,
                        uploading: false,
                        downloading: false,
                        conflicts: false,
                        local_bytes: false,
                    };
                    let persistent_id = format!("seed-pid-{index}");
                    job.prepared = Some(prepared.clone());
                    job.archive = Some(archive.clone());
                    job.persistent_id = Some(persistent_id.clone());
                    job.track = Some(MusicTrack {
                        persistent_id: persistent_id.clone(),
                        location: Some(PathBuf::from(format!("/Music/Seed-{index}.m4a"))),
                        cloud_status: "uploaded".to_string(),
                    });
                    job.local_copy_verified = true;
                    job.add_intent = true;
                    job.status_message_id = Some(index as i64 + 1);
                    job.status_send_attempted = true;
                    job.status_text = Some(render_status(&job));
                    if index == 0 {
                        first_job_id = job.job_id.clone();
                    }
                    state.content_catalog.insert(
                        original_sha,
                        CatalogEntry {
                            add_intent: true,
                            persistent_id: Some(persistent_id),
                            prepared,
                            archive,
                        },
                    );
                }
                state.jobs.insert(job.job_id.clone(), job);
            }
            Ok(first_job_id)
        })
        .expect("bounded completed history should persist in one transaction");
    (first_job_id, failed_job_id, retained_partial)
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
async fn music_manager_archives_using_telegram_original_filename() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let settings = fixture.settings();
    let backend = MockBackend::new(settings.archive_dir.clone());
    let manager = open_manager(
        settings.clone(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let mut original = upload(901, "original-filename-901");
    original.file_name = "01. 愛♡スクリ～ム! 中文 한국어 Русский Français Italiano.M4A".to_string();
    let expected_name = original.file_name.clone();
    let admission = manager
        .enqueue(original)
        .await
        .expect("multilingual music upload should enqueue");
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);
    let complete = wait_for_complete(&manager, &admission.job_id).await;
    assert_eq!(
        complete.archive.as_ref().unwrap().path,
        settings.archive_dir.join(expected_name)
    );
    assert_eq!(backend.archives.load(Ordering::Relaxed), 1);
    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn music_archive_retry_preserves_the_previous_partial_and_original_title() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let settings = fixture.settings();
    let backend = MockBackend::new(settings.archive_dir.clone());
    let manager = open_manager(
        settings.clone(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let mut original = upload(902, "original-filename-902");
    original.file_name = "01. 愛♡スクリ～ム!.m4a".to_string();
    let original_name = original.file_name.clone();
    let admission = manager
        .enqueue(original)
        .await
        .expect("music upload should enqueue");
    manager
        .store
        .set_job(&admission.job_id, |job| {
            job.archive_attempt = 1;
        })
        .expect("previous interrupted archive attempt should persist");
    let old_partial = settings.archive_dir.join(&original_name);
    fs::write(&old_partial, b"previous partial archive").expect("previous partial should exist");
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);
    let complete = wait_for_complete(&manager, &admission.job_id).await;
    assert_eq!(
        complete.archive.as_ref().unwrap().path,
        settings
            .archive_dir
            .join(format!("[retry 2] {original_name}"))
    );
    assert_eq!(fs::read(old_partial).unwrap(), b"previous partial archive");
    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
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
async fn full_active_queue_returns_typed_capacity_but_accepts_replays() {
    let fixture = TestDirectory::new();
    let settings = fixture.settings();
    seed_full_active_queue(&settings);
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(settings.archive_dir.clone());
    let manager = open_manager(settings, &server, backend).await;

    let replay = manager
        .enqueue(upload(10_000, "capacity-0"))
        .await
        .expect("a replay must remain idempotent when the queue is full");
    assert!(!replay.created);
    let error = manager
        .enqueue(upload(50_000, "capacity-overflow"))
        .await
        .expect_err("a new job must be rejected when the active queue is full");
    assert!(error.is::<MusicCapacityFull>());
    let state = manager.store.snapshot().expect("state should read");
    assert_eq!(state.jobs.len(), MAX_JOBS);
    assert_eq!(
        state
            .jobs
            .values()
            .filter(|job| !job.stage.is_terminal())
            .count(),
        MAX_ACTIVE_JOBS
    );

    server.stop();
}

#[tokio::test]
async fn completed_history_eviction_preserves_catalog_and_failed_partials() {
    let fixture = TestDirectory::new();
    let settings = fixture.settings();
    let (evicted_job_id, failed_job_id, retained_partial) = seed_full_completed_history(&settings);
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(settings.archive_dir.clone());
    let manager = open_manager(settings, &server, backend.clone() as Arc<dyn MusicBackend>).await;
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);

    let admission = manager
        .enqueue(upload(30_000, "catalog-after-history-eviction"))
        .await
        .expect("a safely evictable completed record should make room");
    assert!(admission.created);
    wait_for_complete(&manager, &admission.job_id).await;

    let state = manager.store.snapshot().expect("state should read");
    assert_eq!(state.jobs.len(), MAX_JOBS);
    assert!(!state.jobs.contains_key(&evicted_job_id));
    let failed = state
        .jobs
        .get(&failed_job_id)
        .expect("failed retry history must not be evicted");
    assert_eq!(failed.stage, JobStage::Failed);
    assert_eq!(
        failed.download_path.as_deref(),
        Some(retained_partial.as_path())
    );
    assert!(
        retained_partial.exists(),
        "the partial download must remain"
    );
    let original_sha = "a".repeat(64);
    assert!(state.content_catalog.contains_key(&original_sha));
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
async fn lowering_admission_limit_keeps_historic_jobs_prepared_audio_and_catalog_valid() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let settings = fixture.settings();
    let backend = MockBackend::new(settings.archive_dir.clone());
    let manager = open_manager(
        settings.clone(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let admission = manager
        .enqueue(upload(28, "retained-large-job"))
        .await
        .unwrap();
    let prepared = PreparedAudio {
        conversion: None,
        original: AudioBytes {
            path: settings
                .staging_dir
                .join(&admission.job_id)
                .join("original.m4a"),
            sha256: "a".repeat(64),
            size: 24,
        },
        import: AudioBytes {
            path: settings
                .staging_dir
                .join(&admission.job_id)
                .join("prepared.m4a"),
            sha256: "b".repeat(64),
            size: 22,
        },
    };
    let archive = ArchiveState {
        path: settings.archive_dir.join("retained-large-job.m4a"),
        uploaded: true,
        uploading: false,
        downloading: false,
        conflicts: false,
        local_bytes: false,
    };
    let track = MusicTrack {
        persistent_id: "persistent-id-retained".to_string(),
        location: Some(PathBuf::from("/Music/Retained.m4a")),
        cloud_status: "uploaded".to_string(),
    };
    manager
        .store
        .transact(|state| {
            {
                let job = state
                    .jobs
                    .get_mut(&admission.job_id)
                    .expect("admitted job should exist");
                job.stage = JobStage::Complete;
                job.download_path = Some(prepared.original.path.clone());
                job.download_complete = true;
                job.prepared = Some(prepared.clone());
                job.archive = Some(archive.clone());
                job.persistent_id = Some(track.persistent_id.clone());
                job.track = Some(track.clone());
                job.local_copy_verified = true;
                job.add_intent = true;
                job.status_message_id = Some(28);
                job.status_send_attempted = true;
                job.status_text = Some(render_status(job));
            }
            state.content_catalog.insert(
                "a".repeat(64),
                CatalogEntry {
                    add_intent: true,
                    persistent_id: Some(track.persistent_id.clone()),
                    prepared: prepared.clone(),
                    archive: archive.clone(),
                },
            );
            Ok(())
        })
        .expect("historical prepared state should persist under the original limit");
    drop(manager);

    let mut lowered_settings = settings;
    lowered_settings.max_file_bytes = 1;
    let reopened = open_manager(lowered_settings, &server, backend as Arc<dyn MusicBackend>).await;
    let retained = reopened
        .store
        .job(&admission.job_id)
        .expect("retained job should read")
        .expect("retained job should remain in history");
    assert_eq!(retained.stage, JobStage::Complete);
    assert_eq!(retained.upload.file_size, 24);
    assert_eq!(retained.prepared.as_ref().unwrap().original.size, 24);
    assert_eq!(
        reopened.store.snapshot().unwrap().content_catalog.len(),
        1,
        "historical catalog metadata should also survive the lower limit"
    );

    let error = reopened
        .enqueue(upload(29, "above-lowered-limit"))
        .await
        .expect_err("new admission must still obey the lowered configured limit");
    assert!(!error.is::<MusicCapacityFull>());
    assert!(format!("{error:#}").contains("configured limit"));
    drop(reopened);
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
async fn two_link_original_is_recovered_through_the_trusted_download_helper() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let admission = manager
        .enqueue(upload(26, "unique-26-linked"))
        .await
        .unwrap();
    let job_dir = manager.settings.staging_dir.join(&admission.job_id);
    ensure_private_directory(&job_dir, manager.store.provider.as_ref()).unwrap();
    let original_path = job_dir.join("original.m4a");
    fs::write(&original_path, b"complete original audio!").unwrap();
    let published_link = job_dir.join("helper-completed.part");
    fs::hard_link(&original_path, &published_link).unwrap();
    assert_eq!(
        std::os::unix::fs::MetadataExt::nlink(&fs::symlink_metadata(&original_path).unwrap()),
        2
    );

    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);
    wait_for_complete(&manager, &admission.job_id).await;
    assert_eq!(backend.downloads.load(Ordering::Relaxed), 1);
    assert_eq!(backend.adds.load(Ordering::Relaxed), 1);
    assert_eq!(
        std::os::unix::fs::MetadataExt::nlink(&fs::symlink_metadata(&original_path).unwrap()),
        1,
        "the helper must remove the verified partial before import"
    );

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("worker should stop")
        .expect("worker should join");
    server.stop();
}

#[tokio::test]
async fn unverified_two_link_original_is_preserved_and_never_imported() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let backend = MockBackend::new(fixture.settings().archive_dir.clone());
    let manager = open_manager(
        fixture.settings(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let admission = manager
        .enqueue(upload(27, "unique-27-linked-tamper"))
        .await
        .unwrap();
    let job_dir = manager.settings.staging_dir.join(&admission.job_id);
    ensure_private_directory(&job_dir, manager.store.provider.as_ref()).unwrap();
    let original_path = job_dir.join("original.m4a");
    fs::write(&original_path, b"tampered linked bytes").unwrap();
    let published_link = job_dir.join("helper-completed.part");
    fs::hard_link(&original_path, &published_link).unwrap();

    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);
    wait_for_stage(&manager, &admission.job_id, JobStage::Failed).await;
    assert_eq!(fs::read(&original_path).unwrap(), b"tampered linked bytes");
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
        conversion: None,
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

fn accepted_seed_record(
    settings: &MusicSettings,
    original_digit: char,
    import_digit: char,
    persistent_id: &str,
    archive_name: &str,
    converted: bool,
) -> (PreparedAudio, ArchiveState, MusicTrack) {
    let original_sha = original_digit.to_string().repeat(64);
    let import_sha = if converted {
        import_digit.to_string().repeat(64)
    } else {
        original_sha.clone()
    };
    let original_extension = if converted { "flac" } else { "m4a" };
    let import_size = if converted { 120 } else { 100 };
    let original_path = settings
        .staging_dir
        .join(format!("accepted-{original_digit}.{original_extension}"));
    let import_path = settings
        .staging_dir
        .join(format!("accepted-{import_digit}.m4a"));
    let conversion = converted.then(|| flac_alac_converter::types::ConversionReport {
        version: flac_alac_converter::types::PROTOCOL_VERSION,
        source: flac_alac_converter::types::AudioSpec {
            sample_rate: 48_000,
            channels: 2,
            bits_per_sample: 24,
            frames: 100,
        },
        output: flac_alac_converter::types::AudioSpec {
            sample_rate: 48_000,
            channels: 2,
            bits_per_sample: 24,
            frames: 100,
        },
        output_path: import_path.clone(),
        input_sha256: original_sha.clone(),
        output_sha256: import_sha.clone(),
        pcm_sha256: "e".repeat(64),
        audio_bit_exact: true,
        metadata: flac_alac_converter::types::MetadataReport::default(),
        warnings: Vec::new(),
    });
    (
        PreparedAudio {
            original: AudioBytes {
                path: original_path,
                sha256: original_sha,
                size: 100,
            },
            import: AudioBytes {
                path: import_path,
                sha256: import_sha,
                size: import_size,
            },
            conversion,
        },
        ArchiveState {
            path: settings.archive_dir.join(archive_name),
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: false,
        },
        MusicTrack {
            persistent_id: persistent_id.to_string(),
            location: Some(PathBuf::from("/Music/accepted-seed.m4a")),
            cloud_status: "matched".to_string(),
        },
    )
}

#[tokio::test]
async fn accepted_seed_batch_previews_applies_and_replays_converted_and_legacy_records() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let settings = fixture.settings();
    let backend = MockBackend::new(settings.archive_dir.clone());
    let manager = open_manager(settings.clone(), &server, backend as Arc<dyn MusicBackend>).await;
    let converted = accepted_seed_record(
        &settings,
        'a',
        'b',
        "0123456789ABCDEF",
        "accepted-flac.flac",
        true,
    );
    let legacy = accepted_seed_record(
        &settings,
        'c',
        'c',
        "FEDCBA9876543210",
        "accepted-m4a.m4a",
        false,
    );
    assert_ne!(converted.0.original.sha256, converted.0.import.sha256);
    assert_eq!(legacy.0.original.sha256, legacy.0.import.sha256);
    let records = vec![converted, legacy];
    let state_path = settings.state_dir.join("jobs.json");
    let before_bytes = fs::read(&state_path).expect("read baseline manager state");
    let before_state = manager
        .store
        .snapshot()
        .expect("read baseline manager state");

    manager
        .seed_completed_batch(&records, true)
        .await
        .expect("read-only preview should validate the full seed batch");
    assert_eq!(fs::read(&state_path).unwrap(), before_bytes);
    assert_eq!(
        serde_json::to_value(manager.store.snapshot().unwrap().jobs).unwrap(),
        serde_json::to_value(before_state.jobs).unwrap()
    );
    assert!(manager.store.snapshot().unwrap().content_catalog.is_empty());

    manager
        .seed_completed_batch(&records, false)
        .await
        .expect("verified converted and legacy records should seed atomically");
    let after_apply_bytes = fs::read(&state_path).expect("read seeded manager state");
    let state = manager.store.snapshot().expect("read seeded manager state");
    assert_eq!(state.content_catalog.len(), 2);
    assert!(state.jobs.is_empty());
    assert_eq!(
        state
            .content_catalog
            .get(&records[0].0.original.sha256)
            .unwrap()
            .prepared
            .import
            .sha256,
        records[0].0.import.sha256
    );

    let mut replay = records.clone();
    for (_, archive, _) in &mut replay {
        archive.local_bytes = !archive.local_bytes;
    }
    manager
        .seed_completed_batch(&replay, false)
        .await
        .expect("exact records remain idempotent across local archive hydration flags");
    assert_eq!(fs::read(&state_path).unwrap(), after_apply_bytes);
    assert_eq!(manager.store.snapshot().unwrap().content_catalog.len(), 2);
    assert_eq!(manager.store.snapshot().unwrap().jobs.len(), 0);
    server.stop();
}

#[tokio::test]
async fn invalid_late_record_or_catalog_conflict_does_not_partially_seed_batch() {
    let fixture = TestDirectory::new();
    let server = TestHttpServer::start().await;
    let settings = fixture.settings();
    let backend = MockBackend::new(settings.archive_dir.clone());
    let manager = open_manager(settings.clone(), &server, backend as Arc<dyn MusicBackend>).await;
    let accepted = accepted_seed_record(
        &settings,
        'a',
        'b',
        "0123456789ABCDEF",
        "accepted-existing.flac",
        true,
    );
    manager
        .seed_completed_batch(std::slice::from_ref(&accepted), false)
        .await
        .expect("baseline accepted record should seed");
    let state_path = settings.state_dir.join("jobs.json");
    let before_bytes = fs::read(&state_path).expect("read seeded manager state");
    let before_state = manager.store.snapshot().expect("read seeded manager state");

    let first = accepted_seed_record(
        &settings,
        'c',
        'd',
        "1111222233334444",
        "accepted-first.flac",
        true,
    );
    let mut invalid_report = accepted_seed_record(
        &settings,
        'e',
        'f',
        "5555666677778888",
        "accepted-invalid.flac",
        true,
    );
    invalid_report.0.conversion.as_mut().unwrap().output_sha256 = "0".repeat(64);
    assert!(
        manager
            .seed_completed_batch(&[first.clone(), invalid_report], false)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&state_path).unwrap(), before_bytes);
    assert_eq!(
        serde_json::to_value(manager.store.snapshot().unwrap().content_catalog).unwrap(),
        serde_json::to_value(before_state.content_catalog.clone()).unwrap()
    );

    let conflict = accepted_seed_record(
        &settings,
        'a',
        'f',
        "9999AAAABBBBCCCC",
        "accepted-existing.flac",
        true,
    );
    assert!(
        manager
            .seed_completed_batch(&[first, conflict], false)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&state_path).unwrap(), before_bytes);
    let after_state = manager.store.snapshot().unwrap();
    assert_eq!(after_state.content_catalog.len(), 1);
    assert_eq!(after_state.jobs.len(), 0);
    server.stop();
}

#[test]
fn legacy_prepared_audio_without_conversion_report_remains_readable() {
    let mut value = serde_json::to_value(PreparedAudio {
        conversion: None,
        original: AudioBytes {
            path: PathBuf::from("/private/original.m4a"),
            sha256: "a".repeat(64),
            size: 24,
        },
        import: AudioBytes {
            path: PathBuf::from("/private/import.m4a"),
            sha256: "b".repeat(64),
            size: 22,
        },
    })
    .expect("prepared audio should serialize");
    value
        .as_object_mut()
        .expect("prepared audio should serialize as an object")
        .remove("conversion");

    let restored: PreparedAudio =
        serde_json::from_value(value).expect("legacy prepared audio should still deserialize");
    assert!(restored.conversion.is_none());
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn real_flac_conversion_survives_manager_restart_and_keeps_original_archive() {
    let Some(converter) = configured_flac_converter() else {
        return;
    };
    let fixture = TestDirectory::new();
    let canonical_root = fs::canonicalize(&fixture.0)
        .expect("temporary music fixture root should resolve without aliases");
    let fixture_root = canonical_root.join("native-flac-fixture");
    fs::create_dir_all(&fixture_root).expect("FLAC fixture directory should be created");
    let flac_path = create_eight_bit_stereo_flac(&fixture_root);
    let source_bytes = fs::read(&flac_path).expect("generated FLAC source should be readable");

    let mut settings = fixture.settings();
    settings.state_dir = canonical_root.join("media/.music-state");
    settings.staging_dir = canonical_root.join("media/.music-staging");
    settings.archive_dir = canonical_root.join("media/archive");
    settings.converter = converter;
    settings.ffmpeg = absolute_test_tool("ffmpeg");
    settings.ffprobe = absolute_test_tool("ffprobe");
    let backend = Arc::new(NativeFlacConversionBackend::new(
        settings.clone(),
        flac_path,
        &canonical_root,
    ));
    let backend_trait: Arc<dyn MusicBackend> = backend.clone();
    let server = TestHttpServer::start().await;
    let manager = open_manager(settings.clone(), &server, backend_trait).await;

    let mut flac_upload = upload(111, "flac-manager-e2e");
    flac_upload.file_name =
        "01. 愛♡スクリ～ム! 中文 한국어 Русский Français Italiano.FLAC".to_string();
    flac_upload.file_size = source_bytes.len() as u64;
    flac_upload.mime_type = Some("audio/flac".to_string());
    let admission = manager.enqueue(flac_upload).await.unwrap();
    let (shutdown, receiver) = shutdown_channel();
    let worker = manager.start(receiver);
    let failed = wait_for_stage(&manager, &admission.job_id, JobStage::Failed).await;

    let prepared = failed.prepared.as_ref().unwrap_or_else(|| {
        let attempt_directory = settings
            .staging_dir
            .join(&admission.job_id)
            .join(format!("prepare-attempt-{}", failed.prepare_attempt));
        let failure_record = fs::read_to_string(attempt_directory.join("conversion-failure.json"))
            .unwrap_or_else(|error| format!("unavailable: {error}"));
        let backend_prepare = backend
            .prepare_summary
            .lock()
            .expect("prepare summary should not be poisoned")
            .clone()
            .unwrap_or_else(|| "not called".to_string());
        panic!(
            "native conversion should be persisted before the retryable Music preflight; stage={:?}, manager_error={:?}, download_complete={}, prepare_attempt={}, download_path={:?}, backend_prepare={backend_prepare}, conversion_failure={failure_record}",
            failed.stage,
            failed.error,
            failed.download_complete,
            failed.prepare_attempt,
            failed.download_path,
        );
    });
    let report = prepared
        .conversion
        .as_ref()
        .expect("native FLAC conversion report should be persisted");
    assert!(report.audio_bit_exact);
    assert_eq!(report.source.bits_per_sample, 8);
    assert_eq!(report.output.bits_per_sample, 16);
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.code == "precision_promoted")
    );
    assert!(report.metadata.mapped.iter().any(|field| field == "title"));
    assert!(
        report
            .metadata
            .preserved
            .iter()
            .any(|field| field == "FLAC comment TITLE")
    );
    let archive = failed
        .archive
        .as_ref()
        .expect("original should be archived before import");
    assert!(
        archive
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".flac")
    );
    assert!(
        archive
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("01. 愛♡スクリ～ム! 中文 한국어 Русский Français Italiano-")
    );
    assert_eq!(fs::read(&archive.path).unwrap(), source_bytes);
    assert_eq!(fs::read(&prepared.original.path).unwrap(), source_bytes);
    assert_eq!(
        prepared
            .import
            .path
            .extension()
            .and_then(|extension| extension.to_str()),
        Some("m4a")
    );
    assert_ne!(prepared.original.sha256, prepared.import.sha256);
    assert!(
        server
            .wait_for_text("Promoted 8-bit FLAC samples to 16-bit ALAC")
            .await,
        "the precision promotion warning should be visible in Telegram status"
    );
    let warning_edits_before_restart = server.count_text_for_method(
        "editMessageText",
        "Promoted 8-bit FLAC samples to 16-bit ALAC",
    );

    let mut tampered = prepared.clone();
    tampered.conversion.as_mut().unwrap().output_sha256 = "0".repeat(64);
    assert!(validate_prepared_audio(&tampered, settings.max_file_bytes).is_err());

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("first manager worker should stop")
        .expect("first manager worker should join");
    drop(manager);

    let reopened = open_manager(settings, &server, backend.clone() as Arc<dyn MusicBackend>).await;
    let persisted = reopened
        .store
        .job(&admission.job_id)
        .unwrap()
        .expect("failed job should survive a process restart");
    assert_eq!(persisted.stage, JobStage::Failed);
    assert!(persisted.prepared.as_ref().unwrap().conversion.is_some());
    reopened.retry(42, &admission.job_id).await.unwrap();
    let (shutdown, receiver) = shutdown_channel();
    let worker = reopened.start(receiver);
    let complete = wait_for_complete(&reopened, &admission.job_id).await;
    let completed_audio = complete.prepared.as_ref().unwrap();
    assert!(completed_audio.conversion.as_ref().unwrap().audio_bit_exact);
    assert_eq!(backend.downloads.load(Ordering::Relaxed), 1);
    assert_eq!(backend.preparations.load(Ordering::Relaxed), 1);
    assert_eq!(backend.adds.load(Ordering::Relaxed), 1);
    assert!(backend.verifications.load(Ordering::Relaxed) >= 2);
    assert_eq!(backend.evictions.load(Ordering::Relaxed), 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.count_text_for_method(
            "editMessageText",
            "Promoted 8-bit FLAC samples to 16-bit ALAC",
        ) <= warning_edits_before_restart
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the persisted conversion warning should be edited again after restart and retry");
    assert_eq!(
        fs::read(&complete.archive.as_ref().unwrap().path).unwrap(),
        source_bytes
    );
    assert_eq!(
        fs::read(&backend.music_copy).unwrap(),
        fs::read(&completed_audio.import.path).unwrap()
    );

    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("reopened manager worker should stop")
        .expect("reopened manager worker should join");
    server.stop();
}
