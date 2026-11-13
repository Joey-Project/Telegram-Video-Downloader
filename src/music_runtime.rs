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

use crate::music_manager::{Admission, MusicManager, validate_prepared_audio};
use crate::music_native::NativeMusicBackend;
use crate::music_transfer::MusicTransfer;
use crate::music_types::{
    ArchiveState, AudioBytes, MusicBackend, MusicCancellation, MusicSettings, MusicTrack,
    MusicUpload, PreparedAudio,
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

#[derive(Clone, Deserialize)]
struct AcceptedProof {
    schema: u8,
    accepted: bool,
    items: Vec<AcceptedTrack>,
}

#[derive(Clone, Deserialize)]
struct AcceptedTrack {
    sha256: String,
    size_bytes: u64,
    persistent_id: String,
    archive_path: std::path::PathBuf,
    full_decode_passed: bool,
    music_copy_hash_verified: bool,
    archive_uploaded: bool,
    #[serde(default)]
    prepared_audio: Option<PreparedAudio>,
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

fn verify_accepted_records(
    proof: AcceptedProof,
    settings: &MusicSettings,
    native_root: &Path,
    backend: &dyn MusicBackend,
    mut verify_retained_audio: impl FnMut(&AudioBytes) -> Result<()>,
) -> Result<Vec<(PreparedAudio, ArchiveState, MusicTrack)>> {
    if proof.schema != 1 || !proof.accepted || proof.items.is_empty() || proof.items.len() > 64 {
        bail!("invalid accepted Music proof scope");
    }

    let mut prepared_records = Vec::with_capacity(proof.items.len());
    let mut seen_hashes = std::collections::BTreeSet::new();
    let mut seen_persistent_ids = std::collections::BTreeSet::new();
    for item in proof.items {
        if !item.full_decode_passed
            || !item.music_copy_hash_verified
            || !item.archive_uploaded
            || item.sha256.len() != 64
            || !item.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            || item.size_bytes == 0
            || item.size_bytes > MAX_SUPPORTED_SOURCE_BYTES
            || item.archive_path.parent() != Some(settings.archive_dir.as_path())
            || item.persistent_id.len() != 16
            || !item
                .persistent_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("accepted Music track has an invalid content or archive identity");
        }
        let digest = item.sha256.to_ascii_lowercase();
        if !seen_hashes.insert(digest.clone())
            || !seen_persistent_ids.insert(item.persistent_id.clone())
        {
            bail!("accepted Music proof repeats a content hash or persistent ID");
        }

        let audio = if let Some(audio) = item.prepared_audio.clone() {
            validate_prepared_audio(&audio, MAX_SUPPORTED_SOURCE_BYTES)?;
            let report = audio
                .conversion
                .as_ref()
                .context("accepted converted audio is missing its conversion report")?;
            let original_extension = audio
                .original
                .path
                .extension()
                .and_then(std::ffi::OsStr::to_str);
            let import_extension = audio
                .import
                .path
                .extension()
                .and_then(std::ffi::OsStr::to_str);
            if !audio.original.sha256.eq_ignore_ascii_case(&digest)
                || audio.original.size != item.size_bytes
                || audio
                    .original
                    .sha256
                    .eq_ignore_ascii_case(&audio.import.sha256)
                || !original_extension.is_some_and(|extension| {
                    extension.eq_ignore_ascii_case("flac") || extension.eq_ignore_ascii_case("wav")
                })
                || !import_extension.is_some_and(|extension| extension.eq_ignore_ascii_case("m4a"))
                || !report.audio_bit_exact
            {
                bail!("accepted converted audio differs from its original and ALAC identities");
            }
            verify_retained_audio(&audio.original)?;
            verify_retained_audio(&audio.import)?;
            audio
        } else {
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
                conversion: None,
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
            validate_prepared_audio(&audio, MAX_SUPPORTED_SOURCE_BYTES)?;
            // Legacy receipts identify the retained same-hash M4A import. Their original
            // path is the archive path and may be dataless after successful upload.
            verify_retained_audio(&audio.import)?;
            audio
        };
        prepared_records.push((item, audio));
    }

    backend.check_music_ready()?;
    let mut records = Vec::with_capacity(prepared_records.len());
    for (item, audio) in prepared_records {
        let track = backend.query_track(&item.persistent_id)?;
        if track.persistent_id != item.persistent_id {
            bail!("Music returned a different persistent ID than the accepted record");
        }
        if !track.cloud_complete() {
            bail!("accepted Music track no longer has confirmed cloud status");
        }
        backend.verify_copy(&audio, &track)?;
        let archive = backend.archive_status(&item.archive_path)?;
        if archive.path != item.archive_path
            || !archive.uploaded
            || archive.uploading
            || archive.downloading
            || archive.conflicts
        {
            bail!("accepted archive is not confirmed uploaded and stable");
        }
        records.push((audio, archive, track));
    }
    Ok(records)
}

/// Explicit operator handover for the already accepted real test tracks; never adds tracks.
pub async fn seed_accepted_records(
    config: crate::config::AppConfig,
    proof: std::path::PathBuf,
    dry_run: bool,
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
    let verifier = NativeMusicBackend::new(settings.clone());
    let (manager, records) = tokio::task::spawn_blocking(move || -> Result<_> {
        let proof: AcceptedProof = read_private_proof(&proof)?;
        let home = account_home()?;
        // The native source receipts stay at the established test profile path.
        let native_root = home.join(".local/lib/telegram-video-downloader-test/music-import");
        let records =
            verify_accepted_records(proof, &settings, &native_root, backend.as_ref(), |audio| {
                verifier.verify_retained_audio(audio)
            })?;
        let manager = MusicManager::open_for_seed(settings, telegram, backend)?;
        Ok((manager, records))
    })
    .await
    .context("accepted Music verification worker failed")??;
    let count = records.len();
    manager.seed_completed_batch(&records, dry_run).await?;
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
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    struct AcceptedTestBackend {
        music_copy: PathBuf,
        persistent_id_override: Mutex<Option<String>>,
        archive_flags: Mutex<(bool, bool, bool, bool)>,
        queried_ids: Mutex<Vec<String>>,
        prepares: AtomicUsize,
        adds: AtomicUsize,
    }

    impl AcceptedTestBackend {
        fn new(music_copy: PathBuf) -> Self {
            Self {
                music_copy,
                persistent_id_override: Mutex::new(None),
                archive_flags: Mutex::new((true, false, false, false)),
                queried_ids: Mutex::new(Vec::new()),
                prepares: AtomicUsize::new(0),
                adds: AtomicUsize::new(0),
            }
        }
    }

    impl MusicBackend for AcceptedTestBackend {
        fn download(&self, _: &MusicUpload, _: &Path, _: &MusicCancellation) -> Result<()> {
            bail!("accepted-record verification must not download audio")
        }

        fn prepare(&self, _: &Path, _: &Path, _: &MusicCancellation) -> Result<PreparedAudio> {
            self.prepares.fetch_add(1, Ordering::Relaxed);
            bail!("accepted-record verification must not reconvert audio")
        }

        fn archive(&self, _: &PreparedAudio, _: &str) -> Result<ArchiveState> {
            bail!("accepted-record verification must not create archives")
        }

        fn add_track(&self, _: &PreparedAudio) -> Result<String> {
            self.adds.fetch_add(1, Ordering::Relaxed);
            bail!("accepted-record verification must not add Music tracks")
        }

        fn query_track(&self, persistent_id: &str) -> Result<MusicTrack> {
            self.queried_ids
                .lock()
                .expect("query list should not be poisoned")
                .push(persistent_id.to_string());
            let returned_id = self
                .persistent_id_override
                .lock()
                .expect("PID override should not be poisoned")
                .clone()
                .unwrap_or_else(|| persistent_id.to_string());
            Ok(MusicTrack {
                persistent_id: returned_id,
                location: Some(self.music_copy.clone()),
                cloud_status: "matched".to_string(),
            })
        }

        fn verify_copy(&self, audio: &PreparedAudio, track: &MusicTrack) -> Result<()> {
            anyhow::ensure!(track.location.as_deref() == Some(self.music_copy.as_path()));
            let import = fs::read(&audio.import.path)?;
            let copy = fs::read(&self.music_copy)?;
            anyhow::ensure!(
                import == copy,
                "Music copy bytes differ from the saved import"
            );
            let digest = format!("{:x}", Sha256::digest(&copy));
            anyhow::ensure!(digest == audio.import.sha256);
            anyhow::ensure!(copy.len() as u64 == audio.import.size);
            Ok(())
        }

        fn archive_status(&self, path: &Path) -> Result<ArchiveState> {
            let (uploaded, uploading, downloading, conflicts) = *self
                .archive_flags
                .lock()
                .expect("archive flags should not be poisoned");
            Ok(ArchiveState {
                path: path.to_path_buf(),
                uploaded,
                uploading,
                downloading,
                conflicts,
                local_bytes: false,
            })
        }

        fn evict_archive(&self, _: &ArchiveState, _: &MusicTrack) -> Result<ArchiveState> {
            bail!("accepted-record verification must not evict archives")
        }
    }

    struct AcceptedProofFixture {
        _temp: tempfile::TempDir,
        settings: MusicSettings,
        native_root: PathBuf,
        backend: Arc<AcceptedTestBackend>,
        verifier: NativeMusicBackend,
    }

    impl AcceptedProofFixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().expect("create accepted-proof fixture");
            let root = temp
                .path()
                .canonicalize()
                .expect("canonicalize fixture root");
            let settings = MusicSettings {
                enabled: true,
                state_dir: root.join("state"),
                staging_dir: root.join("staging"),
                archive_dir: root.join("archive"),
                max_file_bytes: MAX_SUPPORTED_SOURCE_BYTES,
                ..MusicSettings::default()
            };
            fs::create_dir_all(&settings.staging_dir).expect("create fixture staging root");
            fs::create_dir_all(&settings.archive_dir).expect("create fixture archive root");
            let native_root = root.join("test-profile/music-import");
            fs::create_dir_all(&native_root).expect("create test native root");
            let music_copy = root.join("Music/accepted-copy.m4a");
            fs::create_dir_all(music_copy.parent().unwrap()).expect("create Music copy root");
            let backend = Arc::new(AcceptedTestBackend::new(music_copy.clone()));
            let verifier = NativeMusicBackend::new(settings.clone());
            Self {
                _temp: temp,
                settings,
                native_root,
                backend,
                verifier,
            }
        }

        fn typed_proof(&self) -> (AcceptedProof, PreparedAudio, PathBuf, PathBuf) {
            let original_path = self.settings.staging_dir.join("accepted/original.FLAC");
            let import_path = self.settings.staging_dir.join("accepted/import.m4a");
            fs::create_dir_all(original_path.parent().unwrap())
                .expect("create accepted source directory");
            let original_bytes = b"previously accepted FLAC source bytes";
            let import_bytes = b"previously accepted ALAC import bytes";
            fs::write(&original_path, original_bytes).expect("write accepted FLAC source");
            fs::write(&import_path, import_bytes).expect("write accepted ALAC import");
            let original_sha = format!("{:x}", Sha256::digest(original_bytes));
            let import_sha = format!("{:x}", Sha256::digest(import_bytes));
            fs::write(&self.backend.music_copy, import_bytes)
                .expect("write Music-owned copy fixture");

            let audio = PreparedAudio {
                original: AudioBytes {
                    path: original_path.clone(),
                    sha256: original_sha.clone(),
                    size: original_bytes.len() as u64,
                },
                import: AudioBytes {
                    path: import_path.clone(),
                    sha256: import_sha.clone(),
                    size: import_bytes.len() as u64,
                },
                conversion: Some(flac_alac_converter::types::ConversionReport {
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
                }),
            };
            let persistent_id = "1234567890ABCDEF".to_string();
            let item = AcceptedTrack {
                sha256: original_sha,
                size_bytes: original_bytes.len() as u64,
                persistent_id,
                archive_path: self.settings.archive_dir.join("accepted-original.flac"),
                full_decode_passed: true,
                music_copy_hash_verified: true,
                archive_uploaded: true,
                prepared_audio: Some(audio.clone()),
            };
            (
                AcceptedProof {
                    schema: 1,
                    accepted: true,
                    items: vec![item],
                },
                audio,
                original_path,
                import_path,
            )
        }

        fn verify(
            &self,
            proof: AcceptedProof,
        ) -> Result<Vec<(PreparedAudio, ArchiveState, MusicTrack)>> {
            verify_accepted_records(
                proof,
                &self.settings,
                &self.native_root,
                self.backend.as_ref(),
                |audio| self.verifier.verify_retained_audio(audio),
            )
        }

        fn assert_no_add_or_prepare(&self) {
            assert_eq!(self.backend.prepares.load(Ordering::Relaxed), 0);
            assert_eq!(self.backend.adds.load(Ordering::Relaxed), 0);
        }
    }

    fn make_private_directory(path: &Path) {
        fs::create_dir_all(path).expect("create private proof directory");
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .expect("set private proof directory mode");
    }

    fn write_private_receipt(path: &Path, bytes: &[u8], persistent_id: &str) -> PathBuf {
        let source_root = path.join("sources");
        let receipt_root = path.join("receipts");
        make_private_directory(&source_root);
        make_private_directory(&receipt_root);
        let sha256 = format!("{:x}", Sha256::digest(bytes));
        let source_path = source_root.join(format!("{sha256}.m4a"));
        fs::write(&source_path, bytes).expect("write retained legacy M4A source");
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600))
            .expect("set retained M4A source mode");
        let receipt = serde_json::json!({
            "schema": 1,
            "state": "complete",
            "source_path": source_path,
            "sha256": sha256,
            "size": bytes.len(),
            "track_persistent_id": persistent_id,
        });
        let receipt_path = receipt_root.join(format!("{sha256}.json"));
        fs::write(
            &receipt_path,
            serde_json::to_vec(&receipt).expect("encode native receipt"),
        )
        .expect("write private native receipt");
        fs::set_permissions(&receipt_path, fs::Permissions::from_mode(0o600))
            .expect("set private native receipt mode");
        receipt_path
    }

    #[test]
    fn typed_accepted_flac_uses_distinct_original_and_import_hashes_and_saved_pid() {
        let fixture = AcceptedProofFixture::new();
        let (proof, audio, _, _) = fixture.typed_proof();
        assert_ne!(audio.original.sha256, audio.import.sha256);

        let records = fixture
            .verify(proof)
            .expect("a fully verified converted record should pass handoff checks");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0.original.sha256, audio.original.sha256);
        assert_eq!(records[0].0.import.sha256, audio.import.sha256);
        assert!(records[0].0.conversion.is_some());
        assert_eq!(
            records[0].1.path,
            fixture.settings.archive_dir.join("accepted-original.flac")
        );
        assert_eq!(records[0].2.persistent_id, "1234567890ABCDEF");
        assert_eq!(
            *fixture.backend.queried_ids.lock().unwrap(),
            vec!["1234567890ABCDEF".to_string()]
        );
        fixture.assert_no_add_or_prepare();
    }

    #[test]
    fn legacy_accepted_m4a_uses_same_hash_receipt_without_conversion() {
        let fixture = AcceptedProofFixture::new();
        let persistent_id = "0011223344556677";
        let source_bytes = b"previously accepted legacy M4A bytes";
        write_private_receipt(&fixture.native_root, source_bytes, persistent_id);
        fs::write(&fixture.backend.music_copy, source_bytes)
            .expect("write legacy Music copy fixture");
        let digest = format!("{:x}", Sha256::digest(source_bytes));
        let archive_path = fixture.settings.archive_dir.join("legacy-accepted.m4a");
        let proof = AcceptedProof {
            schema: 1,
            accepted: true,
            items: vec![AcceptedTrack {
                sha256: digest.clone(),
                size_bytes: source_bytes.len() as u64,
                persistent_id: persistent_id.to_string(),
                archive_path: archive_path.clone(),
                full_decode_passed: true,
                music_copy_hash_verified: true,
                archive_uploaded: true,
                prepared_audio: None,
            }],
        };

        let records = fixture
            .verify(proof)
            .expect("the original receipt-backed M4A proof should remain accepted");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0.original.sha256, digest);
        assert_eq!(records[0].0.import.sha256, records[0].0.original.sha256);
        assert!(records[0].0.conversion.is_none());
        assert_eq!(records[0].0.original.path, archive_path);
        assert_eq!(
            records[0].0.import.path,
            fixture
                .native_root
                .join("sources")
                .join(format!("{}.m4a", records[0].0.import.sha256))
        );
        fixture.assert_no_add_or_prepare();
    }

    #[test]
    fn accepted_converted_proof_rejects_tampered_sources_and_report_before_query() {
        for tamper in ["original", "import", "report"] {
            let fixture = AcceptedProofFixture::new();
            let (mut proof, _, original_path, import_path) = fixture.typed_proof();
            match tamper {
                "original" => fs::write(original_path, b"tampered original bytes")
                    .expect("tamper original source fixture"),
                "import" => fs::write(import_path, b"tampered import bytes")
                    .expect("tamper import source fixture"),
                "report" => {
                    proof.items[0]
                        .prepared_audio
                        .as_mut()
                        .unwrap()
                        .conversion
                        .as_mut()
                        .unwrap()
                        .output_sha256 = "0".repeat(64);
                }
                _ => unreachable!(),
            }

            assert!(
                fixture.verify(proof).is_err(),
                "{tamper} tampering must be rejected"
            );
            assert!(fixture.backend.queried_ids.lock().unwrap().is_empty());
            fixture.assert_no_add_or_prepare();
        }
    }

    #[test]
    fn accepted_proof_rejects_wrong_music_pid_and_unstable_archive_without_importing() {
        let fixture = AcceptedProofFixture::new();
        let (proof, _, _, _) = fixture.typed_proof();
        *fixture.backend.persistent_id_override.lock().unwrap() =
            Some("FEDCBA0987654321".to_string());
        assert!(fixture.verify(proof).is_err());
        assert_eq!(
            *fixture.backend.queried_ids.lock().unwrap(),
            vec!["1234567890ABCDEF".to_string()]
        );
        fixture.assert_no_add_or_prepare();

        let fixture = AcceptedProofFixture::new();
        let (proof, _, _, _) = fixture.typed_proof();
        *fixture.backend.archive_flags.lock().unwrap() = (true, true, false, false);
        assert!(fixture.verify(proof).is_err());
        fixture.assert_no_add_or_prepare();
    }

    #[test]
    fn duplicate_accepted_proof_records_are_rejected_before_music_query() {
        let fixture = AcceptedProofFixture::new();
        let (mut proof, _, _, _) = fixture.typed_proof();
        proof.items.push(proof.items[0].clone());

        assert!(fixture.verify(proof).is_err());
        assert!(fixture.backend.queried_ids.lock().unwrap().is_empty());
        fixture.assert_no_add_or_prepare();
    }

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
            ..MusicSettings::default()
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
