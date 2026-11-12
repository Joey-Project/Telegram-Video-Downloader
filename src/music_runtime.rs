//! Composition of the audited transfer helper and fixed-identity native adapters.

use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use tokio::sync::{Notify, OnceCell, oneshot, watch};

use crate::music_manager::{Admission, MusicManager};
use crate::music_native::NativeMusicBackend;
use crate::music_transfer::MusicTransfer;
use crate::music_types::{
    ArchiveState, MusicBackend, MusicCancellation, MusicSettings, MusicTrack, MusicUpload,
    PreparedAudio,
};
use crate::telegram::TelegramClient;

const MAX_SUPPORTED_SOURCE_BYTES: u64 = 512 * 1024 * 1024;
/// Music metadata startup never holds up the ordinary Telegram receiver.
pub struct MusicService {
    manager: OnceCell<Arc<MusicManager>>,
    ready: Notify,
    started: AtomicBool,
    authorizing: AtomicBool,
    settings: MusicSettings,
    telegram: TelegramClient,
    backend: Arc<dyn MusicBackend>,
    shutdown: watch::Receiver<bool>,
}

impl MusicService {
    pub fn new(
        settings: MusicSettings,
        telegram: TelegramClient,
        bot_token: String,
        shutdown: watch::Receiver<bool>,
    ) -> Arc<Self> {
        let backend = Arc::new(RuntimeMusicBackend::new(settings.clone(), bot_token));
        Self::with_backend(settings, telegram, backend, shutdown)
    }

    pub(crate) fn with_backend(
        settings: MusicSettings,
        telegram: TelegramClient,
        backend: Arc<dyn MusicBackend>,
        shutdown: watch::Receiver<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            manager: OnceCell::new(),
            ready: Notify::new(),
            started: AtomicBool::new(false),
            authorizing: AtomicBool::new(false),
            settings,
            telegram,
            backend,
            shutdown,
        })
    }

    pub fn start(self: &Arc<Self>) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let mut shutdown = service.shutdown.clone();
            loop {
                if *shutdown.borrow() {
                    return;
                }
                let settings = service.settings.clone();
                let telegram = service.telegram.clone();
                let backend = Arc::clone(&service.backend);
                let (sender, receiver) = oneshot::channel();
                // Keep one outstanding native file operation. Cancellation must not
                // launch replacement threads while the previous provider call is stuck.
                let spawned = std::thread::Builder::new()
                    .name("music-state-startup".into())
                    .spawn(move || {
                        let _ = sender.send(MusicManager::open(settings, telegram, backend));
                    });
                if spawned.is_err() {
                    tracing::warn!("could not start music state worker");
                    return;
                }
                let opened = tokio::select! {
                    _ = shutdown.changed() => return,
                    result = receiver => result,
                };
                match opened {
                    Ok(Ok(manager)) => {
                        if service.manager.set(Arc::clone(&manager)).is_ok() {
                            manager.start(service.shutdown.clone());
                        }
                        service.ready.notify_waiters();
                        return;
                    }
                    _ => tracing::warn!(
                        "music state is unavailable; retaining tasks for a later startup retry"
                    ),
                }
                tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {},
                }
            }
        });
    }

    async fn manager(&self) -> Result<Arc<MusicManager>> {
        let notified = self.ready.notified();
        if let Some(manager) = self.manager.get() {
            return Ok(Arc::clone(manager));
        }
        tokio::time::timeout(Duration::from_secs(5), notified)
            .await
            .context("music queue is not ready; message remains pending")?;
        self.manager
            .get()
            .cloned()
            .context("music queue is not ready")
    }

    pub async fn enqueue(&self, upload: MusicUpload) -> Result<Admission> {
        self.manager().await?.enqueue(upload).await
    }

    pub async fn summary(&self, chat_id: i64) -> Result<String> {
        match self.manager.get() {
            Some(manager) => manager.summary(chat_id).await,
            None => Ok("Music tasks are starting. Existing tasks and staging files are retained; try /music again shortly.".into()),
        }
    }

    pub async fn retry(&self, chat_id: i64, job_id: &str) -> Result<()> {
        self.manager().await?.retry(chat_id, job_id).await
    }

    pub async fn cancel(&self, chat_id: i64, job_id: &str) -> Result<()> {
        self.manager().await?.cancel(chat_id, job_id).await
    }

    pub fn authorize(self: &Arc<Self>, chat_id: i64, status_message_id: i64) -> bool {
        if self.authorizing.swap(true, Ordering::AcqRel) {
            return false;
        }
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let (sender, receiver) = oneshot::channel();
            let spawned = std::thread::Builder::new()
                .name("music-explicit-authorization".into())
                .spawn(move || {
                    let _ = sender.send(request_permission_for_profile());
                });
            let mut shutdown = service.shutdown.clone();
            let result = if spawned.is_err() {
                None
            } else {
                tokio::select! {
                    _ = shutdown.changed() => None,
                    result = receiver => result.ok(),
                }
            };
            if !*shutdown.borrow() {
                let text = match result {
                    Some(Ok(value))
                        if value
                            .get("musicPermission")
                            .and_then(serde_json::Value::as_str)
                            == Some("granted") =>
                    {
                        "Music permission is granted. Use /music retry TASK_ID to resume a waiting import."
                    }
                    Some(Ok(_)) => {
                        "Music permission is not granted. Check macOS Automation settings for this bot, then request /music authorize again."
                    }
                    _ => {
                        "Music authorization could not finish. No music was added; check the Mac and try /music authorize again."
                    }
                };
                let _ = service
                    .telegram
                    .edit_message_text(chat_id, status_message_id, text.into())
                    .await;
            }
            service.authorizing.store(false, Ordering::Release);
        });
        true
    }
}

pub fn request_permission_for_profile() -> Result<serde_json::Value> {
    #[cfg(feature = "test-runtime")]
    {
        crate::music_automation::run_cli("authorize")
    }
    #[cfg(not(feature = "test-runtime"))]
    {
        crate::music_automation::request_runtime_permission()
    }
}

pub struct RuntimeMusicBackend {
    native: NativeMusicBackend,
    transfer: MusicTransfer,
}

#[derive(Deserialize)]
struct AcceptedProof {
    schema: u8,
    accepted: bool,
    items: Vec<AcceptedTrack>,
}

#[derive(Deserialize)]
struct AcceptedTrack {
    sha256: String,
    size_bytes: u64,
    persistent_id: String,
    archive_path: std::path::PathBuf,
    full_decode_passed: bool,
    music_copy_hash_verified: bool,
    archive_uploaded: bool,
}

#[derive(Deserialize)]
struct AcceptedNativeReceipt {
    schema: u8,
    state: String,
    source_path: std::path::PathBuf,
    sha256: String,
    size: u64,
    track_persistent_id: Option<String>,
}

fn account_home() -> Result<std::path::PathBuf> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt;
    let mut account: libc::passwd = unsafe { std::mem::zeroed() };
    let mut storage = vec![0_u8; 16 * 1024];
    let mut found = std::ptr::null_mut();
    let result = unsafe {
        libc::getpwuid_r(
            libc::getuid(),
            &mut account,
            storage.as_mut_ptr().cast(),
            storage.len(),
            &mut found,
        )
    };
    if result != 0 || found.is_null() || account.pw_dir.is_null() {
        bail!("cannot resolve account home for Music runtime");
    }
    let path = std::path::PathBuf::from(std::ffi::OsStr::from_bytes(
        unsafe { CStr::from_ptr(account.pw_dir) }.to_bytes(),
    ));
    if !path.is_absolute() {
        bail!("account home must be absolute");
    }
    Ok(path)
}

#[cfg(all(feature = "test-runtime", not(test)))]
pub fn validate_test_profile(config: &crate::config::AppConfig) -> Result<()> {
    #[derive(Deserialize)]
    struct TestBot {
        bot_token: String,
    }
    #[derive(Deserialize)]
    struct TestIdentity {
        test_user_id: i64,
        test_bot_id: i64,
    }
    let home = account_home()?;
    let directory = home.join(".config/telegram-video-downloader");
    let bot: TestBot = read_private_proof(&directory.join("e2e-bot.json"))?;
    let identity: TestIdentity = read_private_proof(&directory.join("e2e-identity.json"))?;
    let worker: serde_json::Value = read_private_proof(&directory.join("e2e/wrangler.jsonc"))?;
    let secrets: serde_json::Value =
        read_private_proof(&directory.join("e2e/worker-secrets.json"))?;
    let routes = worker
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .context("independent test Worker has no routes")?;
    if routes.len() != 1
        || routes[0]
            .get("custom_domain")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
    {
        bail!("independent test Worker must have one custom domain");
    }
    let host = routes[0]
        .get("pattern")
        .and_then(serde_json::Value::as_str)
        .context("invalid independent test domain")?;
    let origin = format!("https://{host}");
    let cloud = config
        .cloud
        .as_ref()
        .context("test-runtime requires its independent Cloudflare Worker")?;
    let media = home.join(".local/share/telegram-video-downloader/e2e");
    let test_video = media.join("video");
    if bot.bot_token != config.telegram.token
        || identity.test_user_id <= 0
        || identity.test_bot_id <= 0
        || bot.bot_token.split(':').next() != Some(identity.test_bot_id.to_string().as_str())
        || config.telegram.allow_all_chats
        || config.telegram.allowed_chat_ids != vec![identity.test_user_id]
        || cloud.worker_url.trim_end_matches('/') != origin
        || worker
            .pointer("/vars/OWNER_USER_ID")
            .and_then(serde_json::Value::as_str)
            != Some(identity.test_user_id.to_string().as_str())
        || secrets.get("BOT_TOKEN").and_then(serde_json::Value::as_str)
            != Some(bot.bot_token.as_str())
        || secrets
            .get("LOCAL_SHARED_SECRET")
            .and_then(serde_json::Value::as_str)
            != Some(cloud.resolved_shared_secret()?.as_str())
        || config.downloads.video_dir != test_video
        || config.downloads.pdf_dir != media.join("pdf")
        || !config.music.state_dir.starts_with(&test_video)
        || !config.music.staging_dir.starts_with(&test_video)
        || !config
            .bilibili
            .auth
            .state_path
            .starts_with(directory.join("e2e"))
        || !config
            .bilibili
            .auth
            .credential_file
            .starts_with(directory.join("e2e"))
    {
        bail!("test-runtime must use the configured independent test bot, owner and Worker");
    }
    Ok(())
}

fn read_private_proof<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let parent = std::fs::symlink_metadata(path.parent().context("Music proof has no parent")?)?;
    if !parent.is_dir()
        || parent.file_type().is_symlink()
        || parent.uid() != unsafe { libc::getuid() }
        || parent.mode() & 0o777 != 0o700
    {
        bail!("Music proof directory must be private and owned");
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::getuid() }
        || metadata.mode() & 0o777 != 0o600
        || metadata.len() > 128 * 1024
    {
        bail!("accepted Music proof must be a private owned regular file up to 128 KiB");
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let opened = file.metadata()?;
    if (opened.dev(), opened.ino()) != (metadata.dev(), metadata.ino()) {
        bail!("Music proof changed while opening");
    }
    let mut bytes = Vec::new();
    file.take(128 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 128 * 1024 {
        bail!("Music proof is too large");
    }
    serde_json::from_slice(&bytes).context("invalid accepted Music proof")
}

/// Explicit operator handover for the already accepted real test tracks; never adds tracks.
pub async fn seed_accepted_records(
    config: crate::config::AppConfig,
    proof: std::path::PathBuf,
) -> Result<usize> {
    if !config.music.enabled {
        bail!("enable music imports before seeding accepted tracks");
    }
    let settings = config.music.clone();
    let telegram = TelegramClient::new(config.telegram.token.clone());
    let backend: Arc<dyn MusicBackend> = Arc::new(RuntimeMusicBackend::new(
        settings.clone(),
        config.telegram.token.clone(),
    ));
    let (manager, records) = tokio::task::spawn_blocking(move || -> Result<_> {
        use crate::music_types::AudioBytes;
        let proof: AcceptedProof = read_private_proof(&proof)?;
        if proof.schema != 1 || !proof.accepted || proof.items.is_empty() || proof.items.len() > 64
        {
            bail!("invalid accepted Music proof scope");
        }
        let home = account_home()?;
        // The native source receipts stay at the established test profile path.
        let native_root = home.join(".local/lib/telegram-video-downloader-test/music-import");
        backend.check_music_ready()?;
        let mut records = Vec::new();
        for item in proof.items {
            if !item.full_decode_passed
                || !item.music_copy_hash_verified
                || !item.archive_uploaded
                || item.sha256.len() != 64
                || !item.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
                || item.size_bytes == 0
                || item.size_bytes > MAX_SUPPORTED_SOURCE_BYTES
                || item.archive_path.parent() != Some(settings.archive_dir.as_path())
            {
                bail!("accepted Music track has an invalid content or archive identity");
            }
            let digest = item.sha256.to_ascii_lowercase();
            let saved: AcceptedNativeReceipt =
                read_private_proof(&native_root.join("receipts").join(format!("{digest}.json")))?;
            if saved.schema != 1
                || saved.state != "complete"
                || saved.sha256 != digest
                || saved.size != item.size_bytes
                || saved.track_persistent_id.as_deref() != Some(item.persistent_id.as_str())
                || saved.source_path != native_root.join("sources").join(format!("{digest}.m4a"))
            {
                bail!("accepted Music track differs from its saved native receipt");
            }
            let audio = PreparedAudio {
                original: AudioBytes {
                    path: item.archive_path.clone(),
                    sha256: digest.clone(),
                    size: item.size_bytes,
                },
                import: AudioBytes {
                    path: saved.source_path,
                    sha256: digest,
                    size: item.size_bytes,
                },
            };
            let track = backend.query_track(&item.persistent_id)?;
            if !track.cloud_complete() {
                bail!("accepted Music track no longer has confirmed cloud status");
            }
            backend.verify_copy(&audio, &track)?;
            let archive = backend.archive_status(&item.archive_path)?;
            if !archive.uploaded || archive.uploading || archive.downloading || archive.conflicts {
                bail!("accepted archive is not confirmed uploaded and stable");
            }
            records.push((audio, archive, track));
        }
        let manager = MusicManager::open(settings, telegram, backend)?;
        Ok((manager, records))
    })
    .await
    .context("accepted Music verification worker failed")??;
    let count = records.len();
    for (audio, archive, track) in records {
        manager.seed_completed(&audio, &archive, &track).await?;
    }
    Ok(count)
}

impl RuntimeMusicBackend {
    pub fn new(mut settings: MusicSettings, bot_token: String) -> Self {
        // The manager enforces the current admission limit for new uploads.
        // The backend must still process retained jobs admitted under a prior
        // larger limit, up to the on-disk format ceiling.
        settings.max_file_bytes = MAX_SUPPORTED_SOURCE_BYTES;
        Self {
            native: NativeMusicBackend::new(settings.clone()),
            transfer: MusicTransfer::new(settings, bot_token),
        }
    }
}

impl MusicBackend for RuntimeMusicBackend {
    fn download(
        &self,
        upload: &MusicUpload,
        destination: &Path,
        cancel: &MusicCancellation,
    ) -> Result<()> {
        self.transfer.download(upload, destination, cancel)
    }

    fn prepare(
        &self,
        original: &Path,
        working_dir: &Path,
        cancel: &MusicCancellation,
    ) -> Result<PreparedAudio> {
        self.native.prepare(original, working_dir, cancel)
    }

    fn archive(&self, audio: &PreparedAudio, original_name: &str) -> Result<ArchiveState> {
        self.native.archive(audio, original_name)
    }

    fn check_music_ready(&self) -> Result<()> {
        self.native.check_music_ready()
    }

    fn check_import_source(&self, audio: &PreparedAudio) -> Result<()> {
        self.native.check_import_source(audio)
    }

    fn add_track(&self, audio: &PreparedAudio) -> Result<String> {
        self.native.add_track(audio)
    }

    fn query_track(&self, persistent_id: &str) -> Result<MusicTrack> {
        self.native.query_track(persistent_id)
    }

    fn verify_copy(&self, audio: &PreparedAudio, track: &MusicTrack) -> Result<()> {
        self.native.verify_copy(audio, track)
    }

    fn archive_status(&self, path: &Path) -> Result<ArchiveState> {
        self.native.archive_status(path)
    }

    fn evict_archive(&self, archive: &ArchiveState, track: &MusicTrack) -> Result<ArchiveState> {
        self.native.evict_archive(archive, track)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::time::Instant;

    fn find_test_tool(name: &str) -> Option<PathBuf> {
        let path = std::env::var_os("PATH")?;
        std::env::split_paths(&path)
            .filter(|directory| directory.is_absolute())
            .map(|directory| directory.join(name))
            .find(|candidate| fs::metadata(candidate).is_ok_and(|metadata| metadata.is_file()))
    }

    fn generate_silent_m4a(ffmpeg: &Path, output: &Path) -> Result<()> {
        let mut child = Command::new(ffmpeg)
            .args([
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "anullsrc=r=44100:cl=stereo",
                "-t",
                "0.25",
                "-c:a",
                "aac",
                "-movflags",
                "+faststart",
                "-f",
                "ipod",
                "-n",
            ])
            .arg(output)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("start ffmpeg for the file-only Music fixture")?;
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    bail!("ffmpeg fixture generation exceeded 10 seconds");
                }
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(error).context("poll bounded ffmpeg fixture process");
                }
            }
        };
        if !status.success() {
            bail!("ffmpeg fixture generation failed with {status}");
        }
        let size = fs::metadata(output)
            .context("inspect generated M4A fixture")?
            .len();
        if size == 0 || size > 2 * 1024 * 1024 {
            bail!("generated M4A fixture has unexpected size {size}");
        }
        Ok(())
    }

    #[test]
    fn backend_prepares_retained_audio_above_current_admission_limit() {
        let Some(ffmpeg) = find_test_tool("ffmpeg") else {
            eprintln!("SKIP: ffmpeg is unavailable; retained-audio regression was not generated");
            return;
        };
        let Some(ffprobe) = find_test_tool("ffprobe") else {
            eprintln!("SKIP: ffprobe is unavailable; retained-audio regression was not generated");
            return;
        };

        let temp_parent = fs::canonicalize(std::env::temp_dir())
            .expect("resolve a local temp directory without symlink components");
        let directory = tempfile::tempdir_in(temp_parent).expect("create private test directory");
        let source = directory.path().join("retained-silence.m4a");
        generate_silent_m4a(&ffmpeg, &source).expect("generate bounded file-only M4A fixture");

        let current_admission_limit = 1;
        let source_size = fs::metadata(&source).expect("inspect M4A fixture").len();
        assert!(source_size > current_admission_limit);
        let settings = MusicSettings {
            enabled: true,
            state_dir: directory.path().join("state"),
            staging_dir: directory.path().join("staging"),
            archive_dir: directory.path().join("archive"),
            credentials_path: directory.path().join("credentials.json"),
            helper_python: directory.path().join("python3"),
            transfer_helper: directory.path().join("music_transfer.py"),
            ffmpeg,
            ffprobe,
            max_file_bytes: current_admission_limit,
            cloud_poll_seconds: 60,
        };
        let backend = RuntimeMusicBackend::new(settings, "codex_synth_v1_api_key_a".to_string());
        let work_dir = directory.path().join("work");
        fs::create_dir(&work_dir).expect("create isolated prepare work directory");

        let prepared = backend
            .prepare(&source, &work_dir, &MusicCancellation::default())
            .expect("codec-validate retained audio above the current admission limit");
        assert_eq!(prepared.original.size, source_size);
        assert!(prepared.original.size > current_admission_limit);
        assert_eq!(prepared.original.sha256, prepared.import.sha256);
        assert_eq!(prepared.original.size, prepared.import.size);
        assert_eq!(prepared.original.path, source);
        assert_eq!(prepared.import.path, source);
    }
}
