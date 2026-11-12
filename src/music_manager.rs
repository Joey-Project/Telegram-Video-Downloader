//! Durable, single-worker music upload orchestration.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use rustix::fs::FlockOperation;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::file_provider::{QueueFileProvider, platform_queue_file_provider};
use crate::music_types::{
    ArchiveState, AudioBytes, MusicBackend, MusicCancellation, MusicDownloadFailure, MusicSettings,
    MusicTrack, MusicUpload, PreparedAudio,
};
use crate::telegram::TelegramClient;

const STATE_FILE: &str = "jobs.json";
const OWNER_LOCK_FILE: &str = "owner.lock";
const STATE_VERSION: u32 = 1;
const MAX_STATE_BYTES: usize = 16 * 1024 * 1024;
const MAX_JOBS: usize = 1_000;
const MAX_CATALOG_ENTRIES: usize = 2_048;
const MAX_ID_BYTES: usize = 256;
const MAX_NAME_BYTES: usize = 512;
const MAX_FILE_ID_BYTES: usize = 4_096;
const MAX_UNIQUE_ID_BYTES: usize = 512;
const MAX_IMPORT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_CONFIGURED_FILE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_STATUS_EDIT_ATTEMPTS: u8 = 6;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Admission {
    pub job_id: String,
    pub created: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum JobStage {
    Queued,
    Downloading,
    Preparing,
    Archiving,
    CatalogDedupe,
    Importing,
    Querying,
    WaitingMusicCloud,
    VerifyingCopy,
    ArchiveUploading,
    WaitingArchiveCloud,
    Evicting,
    Complete,
    Failed,
    Cancelled,
    OutcomeUnknown,
}

impl JobStage {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Failed | Self::Cancelled | Self::OutcomeUnknown
        )
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Preparing => "preparing audio",
            Self::Archiving => "saving archive copy",
            Self::CatalogDedupe => "checking existing music",
            Self::Importing => "importing into Music",
            Self::Querying => "checking Music",
            Self::WaitingMusicCloud => "waiting for Music cloud sync",
            Self::VerifyingCopy => "verifying the Music copy",
            Self::ArchiveUploading => "waiting for archive upload",
            Self::WaitingArchiveCloud => "waiting for archive upload",
            Self::Evicting => "evicting verified archive bytes",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::OutcomeUnknown => "import outcome unknown; no automatic retry",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredJob {
    job_id: String,
    origin_key: String,
    upload: MusicUpload,
    stage: JobStage,
    created_at: u64,
    updated_at: u64,
    attempt: u32,
    prepare_attempt: u32,
    archive_attempt: u32,
    download_path: Option<PathBuf>,
    download_complete: bool,
    prepared: Option<PreparedAudio>,
    archive: Option<ArchiveState>,
    persistent_id: Option<String>,
    track: Option<MusicTrack>,
    local_copy_verified: bool,
    add_intent: bool,
    cancel_requested: bool,
    next_poll_at: Option<u64>,
    error: Option<String>,
    status_message_id: Option<i64>,
    status_send_attempted: bool,
    #[serde(default)]
    status_text: Option<String>,
    #[serde(default)]
    status_pending_text: Option<String>,
    #[serde(default)]
    status_edit_attempts: u8,
    #[serde(default)]
    status_next_attempt_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogEntry {
    add_intent: bool,
    persistent_id: Option<String>,
    prepared: PreparedAudio,
    archive: ArchiveState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredState {
    version: u32,
    jobs: BTreeMap<String, StoredJob>,
    content_catalog: BTreeMap<String, CatalogEntry>,
}

impl Default for StoredState {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            jobs: BTreeMap::new(),
            content_catalog: BTreeMap::new(),
        }
    }
}

struct MusicStore {
    state_path: PathBuf,
    archive_dir: PathBuf,
    max_file_bytes: u64,
    provider: Arc<dyn QueueFileProvider>,
    state: Mutex<StoredState>,
    _owner_lock: File,
}

impl MusicStore {
    fn open(settings: &MusicSettings, provider: Arc<dyn QueueFileProvider>) -> Result<Self> {
        ensure_private_directory(&settings.state_dir, provider.as_ref())?;
        ensure_private_directory(&settings.staging_dir, provider.as_ref())?;

        let owner_lock_path = settings.state_dir.join(OWNER_LOCK_FILE);
        ensure_private_lock_file(&owner_lock_path, provider.as_ref())?;
        let owner_lock = acquire_owner_lock(&owner_lock_path, provider.as_ref())?;

        let state_path = settings.state_dir.join(STATE_FILE);
        let state = read_state(&state_path, provider.as_ref())?.unwrap_or_default();
        validate_stored_state(&state, settings.max_file_bytes, &settings.archive_dir)?;
        let store = Self {
            state_path,
            archive_dir: settings.archive_dir.clone(),
            max_file_bytes: settings.max_file_bytes,
            provider,
            state: Mutex::new(state),
            _owner_lock: owner_lock,
        };
        store.recover_interrupted_jobs()?;
        Ok(store)
    }

    fn recover_interrupted_jobs(&self) -> Result<()> {
        self.transact(|state| {
            for job in state.jobs.values_mut() {
                if job.stage.is_terminal() {
                    continue;
                }
                if job.add_intent && job.persistent_id.is_some() {
                    // A known PID always resumes query-only, including failed verification and
                    // stale cloud status. Retained source and archive paths are metadata only.
                    job.stage = JobStage::Querying;
                    job.track = None;
                    job.local_copy_verified = false;
                    job.next_poll_at = None;
                    job.error = None;
                    job.updated_at = unix_seconds();
                    continue;
                }
                if job.add_intent && job.persistent_id.is_none() {
                    job.stage = JobStage::OutcomeUnknown;
                    job.error = Some(
                        "The import may have completed. No automatic re-import will occur."
                            .to_string(),
                    );
                    job.next_poll_at = None;
                } else if !matches!(
                    &job.stage,
                    JobStage::WaitingMusicCloud | JobStage::WaitingArchiveCloud
                ) {
                    job.stage = JobStage::Queued;
                    job.next_poll_at = None;
                }
                job.updated_at = unix_seconds();
            }
            Ok(())
        })
    }

    fn transact<T>(&self, update: impl FnOnce(&mut StoredState) -> Result<T>) -> Result<T> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("music job state lock was poisoned"))?;
        let mut next = guard.clone();
        let output = update(&mut next)?;
        validate_stored_state(&next, self.max_file_bytes, &self.archive_dir)?;
        write_state_atomic(&self.state_path, &next, self.provider.as_ref())?;
        *guard = next;
        Ok(output)
    }

    fn snapshot(&self) -> Result<StoredState> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| anyhow!("music job state lock was poisoned"))
    }

    fn job(&self, job_id: &str) -> Result<Option<StoredJob>> {
        Ok(self.snapshot()?.jobs.get(job_id).cloned())
    }

    fn enqueue(&self, upload: MusicUpload, settings: &MusicSettings) -> Result<Admission> {
        validate_upload(&upload, settings)?;
        let (job_id, origin_key) = job_identity(&upload);
        self.transact(|state| {
            if let Some(existing) = state.jobs.get(&job_id) {
                if existing.origin_key != origin_key || existing.upload != upload {
                    bail!("conflicting metadata was supplied for an existing music upload");
                }
                return Ok(Admission {
                    job_id,
                    created: false,
                });
            }
            if state.jobs.len() >= MAX_JOBS {
                bail!("music job capacity has been reached");
            }
            let now = unix_seconds();
            state.jobs.insert(
                job_id.clone(),
                StoredJob {
                    job_id: job_id.clone(),
                    origin_key,
                    upload,
                    stage: JobStage::Queued,
                    created_at: now,
                    updated_at: now,
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
                },
            );
            Ok(Admission {
                job_id,
                created: true,
            })
        })
    }

    fn set_job(&self, job_id: &str, update: impl FnOnce(&mut StoredJob)) -> Result<()> {
        self.transact(|state| {
            let job = state
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| anyhow!("music job disappeared"))?;
            update(job);
            job.updated_at = unix_seconds();
            Ok(())
        })
    }

    fn save_prepared(&self, job_id: &str, prepared: PreparedAudio) -> Result<()> {
        self.transact(|state| {
            let sha = normalize_sha256(&prepared.original.sha256)?;
            let job = state
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| anyhow!("music job disappeared"))?;
            job.prepared = Some(prepared);
            job.stage = JobStage::CatalogDedupe;
            job.next_poll_at = None;
            job.error = None;
            job.updated_at = unix_seconds();
            // Normalize the hash stored in the prepared result before it becomes the catalog key.
            let prepared = job
                .prepared
                .as_mut()
                .expect("prepared audio was just stored");
            prepared.original.sha256 = sha.clone();
            prepared.import.sha256 = normalize_sha256(&prepared.import.sha256)?;
            Ok(())
        })
    }

    fn begin_import(&self, job_id: &str, sha: &str) -> Result<ImportDecision> {
        self.transact(|state| {
            if state
                .jobs
                .get(job_id)
                .ok_or_else(|| anyhow!("music job disappeared"))?
                .cancel_requested
            {
                return Ok(ImportDecision::Cancelled);
            }
            let entry = state
                .content_catalog
                .get(sha)
                .ok_or_else(|| anyhow!("prepared content is missing from the catalog"))?;
            if let Some(persistent_id) = &entry.persistent_id {
                return Ok(ImportDecision::Reuse(persistent_id.clone()));
            }
            if entry.add_intent {
                return Ok(ImportDecision::Unknown);
            }
            state
                .content_catalog
                .get_mut(sha)
                .expect("catalog entry was just checked")
                .add_intent = true;
            let job = state
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| anyhow!("music job disappeared"))?;
            job.stage = JobStage::Importing;
            job.add_intent = true;
            job.error = None;
            job.next_poll_at = None;
            job.updated_at = unix_seconds();
            Ok(ImportDecision::Add)
        })
    }

    fn record_import_pid(&self, job_id: &str, sha: &str, persistent_id: String) -> Result<()> {
        self.transact(|state| {
            let catalog = state
                .content_catalog
                .get_mut(sha)
                .ok_or_else(|| anyhow!("music content add intent disappeared"))?;
            catalog.add_intent = true;
            catalog.persistent_id = Some(persistent_id.clone());
            let job = state
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| anyhow!("music job disappeared"))?;
            job.persistent_id = Some(persistent_id);
            job.add_intent = true;
            job.stage = JobStage::Querying;
            job.next_poll_at = None;
            job.error = None;
            job.updated_at = unix_seconds();
            Ok(())
        })
    }

    fn seed_completed(
        &self,
        audio: &PreparedAudio,
        archive: &ArchiveState,
        track: &MusicTrack,
        settings: &MusicSettings,
    ) -> Result<()> {
        validate_prepared_audio(audio, settings.max_file_bytes)?;
        validate_track(track)?;
        validate_archive_state(archive, &settings.archive_dir)?;
        if !track.cloud_complete() {
            bail!("completed catalog seed requires uploaded or matched Music status");
        }
        if !archive.uploaded || archive.uploading || archive.downloading || archive.conflicts {
            bail!("completed catalog seed requires a stable uploaded archive");
        }
        let sha = normalize_sha256(&audio.original.sha256)?;
        let persistent_id = track.persistent_id.clone();
        let mut prepared = audio.clone();
        prepared.original.sha256 = sha.clone();
        prepared.import.sha256 = normalize_sha256(&prepared.import.sha256)?;
        self.transact(|state| {
            if let Some(existing) = state.content_catalog.get(&sha) {
                if existing.persistent_id.as_deref() != Some(&persistent_id) {
                    bail!("content catalog already maps this hash to a different Music track");
                }
                return Ok(());
            }
            if state.content_catalog.len() >= MAX_CATALOG_ENTRIES {
                bail!("music content catalog capacity has been reached");
            }
            state.content_catalog.insert(
                sha,
                CatalogEntry {
                    add_intent: true,
                    persistent_id: Some(persistent_id),
                    prepared: prepared.clone(),
                    archive: archive.clone(),
                },
            );
            Ok(())
        })
    }

    fn retry(&self, chat_id: i64, job_id: &str) -> Result<()> {
        self.transact(|state| {
            let job = state
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| anyhow!("music job was not found"))?;
            if job.upload.chat_id != chat_id {
                bail!("music job was not found in this chat");
            }
            if job.stage == JobStage::OutcomeUnknown {
                bail!("the import outcome is unknown and cannot be retried automatically");
            }
            if !matches!(&job.stage, JobStage::Failed | JobStage::Cancelled) {
                bail!("only failed or cancelled music jobs can be retried");
            }
            if job.add_intent && job.persistent_id.is_none() {
                bail!("the import outcome is unknown and cannot be retried automatically");
            }
            job.cancel_requested = false;
            job.error = None;
            job.next_poll_at = None;
            job.stage = if job.persistent_id.is_some() {
                job.track = None;
                job.local_copy_verified = false;
                JobStage::Querying
            } else if job.prepared.is_some() {
                JobStage::CatalogDedupe
            } else if job.download_complete {
                JobStage::Preparing
            } else {
                JobStage::Queued
            };
            job.updated_at = unix_seconds();
            Ok(())
        })
    }

    fn cancel(&self, chat_id: i64, job_id: &str) -> Result<bool> {
        self.transact(|state| {
            let job = state
                .jobs
                .get_mut(job_id)
                .ok_or_else(|| anyhow!("music job was not found"))?;
            if job.upload.chat_id != chat_id {
                bail!("music job was not found in this chat");
            }
            if job.stage.is_terminal() {
                return Ok(false);
            }
            job.cancel_requested = true;
            if !job.add_intent {
                job.stage = JobStage::Cancelled;
                job.next_poll_at = None;
                job.error = None;
            }
            job.updated_at = unix_seconds();
            Ok(!job.add_intent)
        })
    }

    fn next_work(&self, now: u64) -> Result<Option<WorkItem>> {
        let state = self.snapshot()?;
        let mut earliest_poll = None;
        for job in state.jobs.values() {
            match status_schedule(job, now) {
                StatusSchedule::Due => return Ok(Some(WorkItem::Status(job.job_id.clone()))),
                StatusSchedule::At(timestamp) => {
                    earliest_poll =
                        Some(earliest_poll.map_or(timestamp, |old: u64| old.min(timestamp)));
                }
                StatusSchedule::None => {}
            }
            if job.stage.is_terminal() {
                continue;
            }
            if let Some(next_poll_at) = job.next_poll_at
                && next_poll_at > now
            {
                earliest_poll =
                    Some(earliest_poll.map_or(next_poll_at, |old: u64| old.min(next_poll_at)));
                continue;
            }
            return Ok(Some(WorkItem::Job(job.job_id.clone())));
        }
        Ok(earliest_poll.map(WorkItem::WaitUntil))
    }

    fn summary(&self, chat_id: i64) -> Result<String> {
        let state = self.snapshot()?;
        let mut jobs: Vec<_> = state
            .jobs
            .values()
            .filter(|job| job.upload.chat_id == chat_id)
            .collect();
        jobs.sort_by_key(|job| std::cmp::Reverse(job.created_at));
        jobs.truncate(20);
        if jobs.is_empty() {
            return Ok("No music jobs in this chat.".to_string());
        }
        let mut lines = vec!["Music jobs:".to_string()];
        for job in jobs {
            let suffix = job.error.as_deref().unwrap_or_default();
            if suffix.is_empty() {
                lines.push(format!("{} — {}", job.job_id, job.stage.label()));
            } else {
                lines.push(format!("{} — {} ({suffix})", job.job_id, job.stage.label()));
            }
        }
        Ok(lines.join("\n"))
    }
}

enum ImportDecision {
    Add,
    Reuse(String),
    Unknown,
    Cancelled,
}

enum WorkItem {
    Job(String),
    Status(String),
    WaitUntil(u64),
}

enum StatusSchedule {
    None,
    Due,
    At(u64),
}

struct WorkerStartedGuard(Arc<AtomicBool>);

impl Drop for WorkerStartedGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Owns durable music jobs and a dedicated serial worker.
pub struct MusicManager {
    settings: MusicSettings,
    store: Arc<MusicStore>,
    telegram: TelegramClient,
    backend: Arc<dyn MusicBackend>,
    notify: Notify,
    cancellations: Mutex<BTreeMap<String, MusicCancellation>>,
    worker_started: Arc<AtomicBool>,
}

impl MusicManager {
    /// Opens the independent private state and acquires its process-wide owner lock.
    pub fn open(
        settings: MusicSettings,
        telegram: TelegramClient,
        backend: Arc<dyn MusicBackend>,
    ) -> Result<Arc<Self>> {
        if !settings.enabled {
            bail!("music processing is disabled");
        }
        validate_settings(&settings)?;
        let provider = platform_queue_file_provider();
        let store = MusicStore::open(&settings, provider)?;
        Ok(Arc::new(Self {
            settings,
            store: Arc::new(store),
            telegram,
            backend,
            notify: Notify::new(),
            cancellations: Mutex::new(BTreeMap::new()),
            worker_started: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// Inserts a Telegram upload durably before returning to the update dispatcher.
    pub async fn enqueue(self: &Arc<Self>, upload: MusicUpload) -> Result<Admission> {
        let manager = Arc::clone(self);
        let admission =
            tokio::task::spawn_blocking(move || manager.store.enqueue(upload, &manager.settings))
                .await
                .context("music admission worker stopped")??;
        self.notify.notify_one();
        Ok(admission)
    }

    /// Starts one serial background worker and returns its task handle.
    pub fn start(self: &Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        let manager = Arc::clone(self);
        if manager
            .worker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return tokio::spawn(async {});
        }
        tokio::spawn(async move {
            let _started_guard = WorkerStartedGuard(Arc::clone(&manager.worker_started));
            loop {
                if *shutdown.borrow() {
                    manager.cancel_active_pre_import();
                    return;
                }
                let work = match manager.store.next_work(unix_seconds()) {
                    Ok(work) => work,
                    Err(_) => {
                        tokio::select! {
                            _ = manager.notify.notified() => {},
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    manager.cancel_active_pre_import();
                                    return;
                                }
                            }
                        }
                        continue;
                    }
                };
                match work {
                    Some(WorkItem::Job(job_id)) => {
                        let worker = Arc::clone(&manager);
                        let mut task =
                            tokio::spawn(async move { worker.process_job(job_id).await });
                        tokio::select! {
                            _ = &mut task => {},
                            changed = shutdown.changed() => {
                                manager.cancel_active_pre_import();
                                let _ = task.await;
                                if changed.is_err() || *shutdown.borrow() {
                                    return;
                                }
                            }
                        }
                    }
                    Some(WorkItem::Status(job_id)) => manager.update_status(&job_id).await,
                    Some(WorkItem::WaitUntil(timestamp)) => {
                        let delay =
                            Duration::from_secs(timestamp.saturating_sub(unix_seconds()).max(1));
                        tokio::select! {
                            _ = manager.notify.notified() => {},
                            _ = tokio::time::sleep(delay) => {},
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    manager.cancel_active_pre_import();
                                    return;
                                }
                            }
                        }
                    }
                    None => {
                        tokio::select! {
                            _ = manager.notify.notified() => {},
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    manager.cancel_active_pre_import();
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    pub async fn summary(&self, chat_id: i64) -> Result<String> {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || store.summary(chat_id))
            .await
            .context("music summary worker stopped")?
    }

    pub async fn retry(&self, chat_id: i64, job_id: &str) -> Result<()> {
        let store = Arc::clone(&self.store);
        let job_id = job_id.to_string();
        tokio::task::spawn_blocking(move || store.retry(chat_id, &job_id))
            .await
            .context("music retry worker stopped")??;
        self.notify.notify_one();
        Ok(())
    }

    pub async fn cancel(&self, chat_id: i64, job_id: &str) -> Result<()> {
        let store = Arc::clone(&self.store);
        let job_id = job_id.to_string();
        let cancellation_id = job_id.clone();
        let should_cancel_backend =
            tokio::task::spawn_blocking(move || store.cancel(chat_id, &cancellation_id))
                .await
                .context("music cancel worker stopped")??;
        if should_cancel_backend
            && let Ok(cancellations) = self.cancellations.lock()
            && let Some(cancellation) = cancellations.get(&job_id)
        {
            cancellation.cancel();
        }
        self.notify.notify_one();
        Ok(())
    }

    /// Adds a previously accepted, fully verified track to the content catalog without import.
    /// The caller must validate its native acceptance evidence before calling this method.
    pub async fn seed_completed(
        &self,
        audio: &PreparedAudio,
        archive: &ArchiveState,
        track: &MusicTrack,
    ) -> Result<()> {
        let audio = audio.clone();
        let archive = archive.clone();
        let track = track.clone();
        let store = Arc::clone(&self.store);
        let settings = self.settings.clone();
        tokio::task::spawn_blocking(move || {
            store.seed_completed(&audio, &archive, &track, &settings)
        })
        .await
        .context("music catalog seed worker stopped")??;
        Ok(())
    }

    async fn process_job(self: &Arc<Self>, job_id: String) {
        let cancellation = MusicCancellation::default();
        if let Ok(mut active) = self.cancellations.lock() {
            active.insert(job_id.clone(), cancellation.clone());
        }
        let _ = self.update_status(&job_id).await;

        loop {
            let job = match self.store.job(&job_id) {
                Ok(Some(job)) => job,
                _ => break,
            };
            if job.stage.is_terminal() {
                break;
            }
            if cancellation.is_cancelled() && !job.cancel_requested && !job.add_intent {
                break;
            }
            if job.cancel_requested && !job.add_intent {
                let _ = self.store.set_job(&job_id, |job| {
                    job.stage = JobStage::Cancelled;
                    job.next_poll_at = None;
                    job.error = None;
                });
                let _ = self.update_status(&job_id).await;
                break;
            }

            if !job.download_complete {
                self.download_job(&job_id, &job, &cancellation).await;
                continue;
            }
            if job.prepared.is_none() {
                self.prepare_job(&job_id, &job, &cancellation).await;
                continue;
            }
            if job.archive.is_none() && job.persistent_id.is_none() {
                self.resolve_catalog(&job_id, &job).await;
                continue;
            }
            if job.persistent_id.is_none() {
                self.import_or_reuse(&job_id, &job, &cancellation).await;
                continue;
            }
            if job.track.is_none()
                || (job
                    .track
                    .as_ref()
                    .is_some_and(|track| !track.cloud_complete())
                    && (job
                        .next_poll_at
                        .is_none_or(|timestamp| timestamp <= unix_seconds())))
            {
                self.query_music(&job_id, &job).await;
                continue;
            }
            let track = job.track.as_ref().expect("track was checked above");
            if !track.cloud_complete() {
                let next_poll = unix_seconds().saturating_add(self.settings.cloud_poll_seconds);
                let _ = self.store.set_job(&job_id, |job| {
                    job.stage = JobStage::WaitingMusicCloud;
                    job.next_poll_at = Some(next_poll);
                });
                let _ = self.update_status(&job_id).await;
                break;
            }
            if !job.local_copy_verified {
                self.verify_music_copy(&job_id, &job).await;
                continue;
            }
            self.check_archive_upload(&job_id, &job).await;
            break;
        }

        if let Ok(mut active) = self.cancellations.lock() {
            active.remove(&job_id);
        }
    }

    async fn download_job(&self, job_id: &str, job: &StoredJob, cancellation: &MusicCancellation) {
        let extension = match supported_extension(&job.upload.file_name) {
            Some(extension) => extension,
            None => {
                self.fail_job(job_id, "Only .m4a, .mp3, and .aac files are accepted.")
                    .await;
                return;
            }
        };
        let job_dir = self.settings.staging_dir.join(job_id);
        if ensure_private_directory(&job_dir, self.store.provider.as_ref()).is_err() {
            self.fail_job(job_id, "The private staging directory could not be opened.")
                .await;
            return;
        }
        let destination = job_dir.join(format!("original.{extension}"));
        match completed_download(&destination, job.upload.file_size) {
            Ok(true) => {
                if self
                    .store
                    .set_job(job_id, |current| {
                        current.download_path = Some(destination.clone());
                        current.download_complete = true;
                        current.stage = JobStage::Preparing;
                        current.error = None;
                    })
                    .is_ok()
                {
                    let _ = self.update_status(job_id).await;
                }
                return;
            }
            Ok(false) => {}
            Err(_) => {
                self.fail_job(
                    job_id,
                    "The retained Telegram audio file is invalid; it was preserved for inspection.",
                )
                .await;
                return;
            }
        }
        let attempt = job.attempt.saturating_add(1);
        if self
            .store
            .set_job(job_id, |current| {
                current.stage = JobStage::Downloading;
                current.attempt = attempt;
                current.download_path = Some(destination.clone());
                current.download_complete = false;
                current.next_poll_at = None;
                current.error = None;
            })
            .is_err()
        {
            return;
        }
        let _ = self.update_status(job_id).await;

        let backend = Arc::clone(&self.backend);
        let upload = job.upload.clone();
        let destination_for_backend = destination.clone();
        let cancellation = cancellation.clone();
        let result = run_blocking(move || {
            backend.download(&upload, &destination_for_backend, &cancellation)
        })
        .await;
        if let Err(error) = result {
            if self.job_cancel_requested(job_id) && !self.job_add_intent(job_id) {
                let _ = self.store.set_job(job_id, |current| {
                    current.stage = JobStage::Cancelled;
                    current.error = None;
                    current.next_poll_at = None;
                });
                let _ = self.update_status(job_id).await;
            } else {
                let safe_message = error
                    .downcast_ref::<MusicDownloadFailure>()
                    .map(|failure| failure.safe_message())
                    .unwrap_or("The Telegram audio download failed; retry is available.");
                self.fail_job(job_id, safe_message).await;
            }
            return;
        }
        if !matches!(
            completed_download(&destination, job.upload.file_size),
            Ok(true)
        ) {
            self.fail_job(
                job_id,
                "The downloaded audio is incomplete; its partial file was retained for retry.",
            )
            .await;
            return;
        }
        if self.job_cancel_requested(job_id) && !self.job_add_intent(job_id) {
            let _ = self.store.set_job(job_id, |current| {
                current.stage = JobStage::Cancelled;
                current.error = None;
                current.next_poll_at = None;
            });
            let _ = self.update_status(job_id).await;
            return;
        }
        if self
            .store
            .set_job(job_id, |current| {
                current.download_complete = true;
                current.stage = JobStage::Preparing;
            })
            .is_ok()
        {
            let _ = self.update_status(job_id).await;
        }
    }

    async fn prepare_job(&self, job_id: &str, job: &StoredJob, cancellation: &MusicCancellation) {
        let Some(original) = job.download_path.clone() else {
            self.fail_job(
                job_id,
                "The downloaded audio record is missing; retry is available.",
            )
            .await;
            return;
        };
        let prepare_attempt = job.prepare_attempt.saturating_add(1);
        let work_dir = self
            .settings
            .staging_dir
            .join(job_id)
            .join(format!("prepare-attempt-{prepare_attempt}"));
        if ensure_private_directory(&work_dir, self.store.provider.as_ref()).is_err() {
            self.fail_job(
                job_id,
                "The private audio preparation directory could not be opened.",
            )
            .await;
            return;
        }
        if self
            .store
            .set_job(job_id, |current| {
                current.stage = JobStage::Preparing;
                current.prepare_attempt = prepare_attempt;
                current.next_poll_at = None;
                current.error = None;
            })
            .is_err()
        {
            return;
        }
        let _ = self.update_status(job_id).await;

        let backend = Arc::clone(&self.backend);
        let cancellation = cancellation.clone();
        let prepared =
            run_blocking(move || backend.prepare(&original, &work_dir, &cancellation)).await;
        let prepared = match prepared {
            Ok(prepared)
                if validate_prepared_audio(&prepared, self.settings.max_file_bytes).is_ok()
                    && prepared.original.size == job.upload.file_size =>
            {
                prepared
            }
            _ => {
                if self.job_cancel_requested(job_id) && !self.job_add_intent(job_id) {
                    let _ = self.store.set_job(job_id, |current| {
                        current.stage = JobStage::Cancelled;
                        current.error = None;
                        current.next_poll_at = None;
                    });
                    let _ = self.update_status(job_id).await;
                } else {
                    self.fail_job(job_id, "Audio preparation failed; retry is available.")
                        .await;
                }
                return;
            }
        };
        if self.store.save_prepared(job_id, prepared).is_ok() {
            let _ = self.update_status(job_id).await;
        }
    }

    async fn archive_job(&self, job_id: &str, job: &StoredJob) {
        let Some(prepared) = job.prepared.clone() else {
            return;
        };
        let archive_attempt = job.archive_attempt.saturating_add(1);
        if self
            .store
            .set_job(job_id, |current| {
                current.stage = JobStage::Archiving;
                current.archive_attempt = archive_attempt;
                current.next_poll_at = None;
                current.error = None;
            })
            .is_err()
        {
            return;
        }
        let _ = self.update_status(job_id).await;
        let extension = supported_extension(&job.upload.file_name).unwrap_or("m4a");
        let archive_name = format!("music-{job_id}-attempt-{archive_attempt}.{extension}");
        let backend = Arc::clone(&self.backend);
        let archive = run_blocking(move || backend.archive(&prepared, &archive_name)).await;
        match archive {
            Ok(archive) if validate_archive_state(&archive, &self.settings.archive_dir).is_ok() => {
                let prepared = job.prepared.clone().expect("prepared audio");
                let sha = match normalize_sha256(&prepared.original.sha256) {
                    Ok(sha) => sha,
                    Err(_) => {
                        self.fail_job(job_id, "The original audio checksum is invalid.")
                            .await;
                        return;
                    }
                };
                let archive_for_catalog = archive.clone();
                let saved = self.store.transact(|state| {
                    let entry = state
                        .content_catalog
                        .entry(sha)
                        .or_insert_with(|| CatalogEntry {
                            add_intent: false,
                            persistent_id: None,
                            prepared: prepared.clone(),
                            archive: archive_for_catalog.clone(),
                        });
                    let job_prepared = entry.prepared.clone();
                    let job_archive = entry.archive.clone();
                    let job_persistent_id = entry.persistent_id.clone();
                    let job_add_intent = entry.add_intent;
                    let import_unknown = entry.add_intent && entry.persistent_id.is_none();
                    let job = state
                        .jobs
                        .get_mut(job_id)
                        .ok_or_else(|| anyhow!("music job disappeared"))?;
                    job.prepared = Some(job_prepared);
                    job.archive = Some(job_archive);
                    job.persistent_id = job_persistent_id;
                    job.add_intent = job_add_intent;
                    job.stage = if import_unknown {
                        JobStage::OutcomeUnknown
                    } else if job.persistent_id.is_some() {
                        JobStage::Querying
                    } else {
                        JobStage::CatalogDedupe
                    };
                    job.error = import_unknown.then(|| {
                        "The import may have completed. No automatic re-import will occur."
                            .to_string()
                    });
                    job.updated_at = unix_seconds();
                    Ok(())
                });
                if saved.is_ok() {
                    let _ = self.update_status(job_id).await;
                }
            }
            _ => {
                self.fail_job(
                    job_id,
                    "The archive copy could not be created; retry is available.",
                )
                .await
            }
        }
    }

    async fn resolve_catalog(&self, job_id: &str, job: &StoredJob) {
        let Some(prepared) = job.prepared.as_ref() else {
            return;
        };
        let sha = match normalize_sha256(&prepared.original.sha256) {
            Ok(sha) => sha,
            Err(_) => {
                self.fail_job(job_id, "The original audio checksum is invalid.")
                    .await;
                return;
            }
        };
        let entry = match self.store.snapshot() {
            Ok(state) => state.content_catalog.get(&sha).cloned(),
            Err(_) => None,
        };
        let Some(entry) = entry else {
            let catalog_full = self
                .store
                .snapshot()
                .map(|state| state.content_catalog.len() >= MAX_CATALOG_ENTRIES)
                .unwrap_or(true);
            if catalog_full {
                self.fail_job(
                    job_id,
                    "The music content catalog is full; no archive was created.",
                )
                .await;
                return;
            }
            self.archive_job(job_id, job).await;
            return;
        };
        let unknown = entry.add_intent && entry.persistent_id.is_none();
        if self
            .store
            .set_job(job_id, |current| {
                // An original-content match reuses its accepted import bytes and archive.
                current.prepared = Some(entry.prepared.clone());
                current.archive = Some(entry.archive.clone());
                current.persistent_id = entry.persistent_id.clone();
                current.add_intent = entry.add_intent;
                current.stage = if unknown {
                    JobStage::OutcomeUnknown
                } else if entry.persistent_id.is_some() {
                    JobStage::Querying
                } else {
                    JobStage::CatalogDedupe
                };
                current.error = unknown.then(|| {
                    "The import may have completed. No automatic re-import will occur.".to_string()
                });
                current.next_poll_at = None;
            })
            .is_ok()
        {
            let _ = self.update_status(job_id).await;
        }
    }

    async fn import_or_reuse(
        &self,
        job_id: &str,
        job: &StoredJob,
        cancellation: &MusicCancellation,
    ) {
        let Some(prepared) = job.prepared.clone() else {
            return;
        };
        let sha = match normalize_sha256(&prepared.original.sha256) {
            Ok(sha) => sha,
            Err(_) => {
                self.fail_job(job_id, "The original audio checksum is invalid.")
                    .await;
                return;
            }
        };
        let catalog_entry = match self.store.snapshot() {
            Ok(state) => state.content_catalog.get(&sha).cloned(),
            Err(_) => None,
        };
        let Some(catalog_entry) = catalog_entry else {
            self.fail_job(
                job_id,
                "The prepared content is missing from the catalog; retry is available.",
            )
            .await;
            return;
        };
        if let Some(persistent_id) = catalog_entry.persistent_id {
            self.finish_import_decision(job_id, ImportDecision::Reuse(persistent_id))
                .await;
            return;
        }
        if catalog_entry.add_intent {
            self.finish_import_decision(job_id, ImportDecision::Unknown)
                .await;
            return;
        }

        let backend = Arc::clone(&self.backend);
        let ready = run_blocking(move || backend.check_music_ready()).await;
        if ready.is_err() {
            self.fail_job(
                job_id,
                "Music is not ready or authorized; resolve that state and retry.",
            )
            .await;
            return;
        }
        if cancellation.is_cancelled() {
            return;
        }
        let backend = Arc::clone(&self.backend);
        let audio_for_source_check = prepared.clone();
        if run_blocking(move || backend.check_import_source(&audio_for_source_check))
            .await
            .is_err()
        {
            self.fail_job(
                job_id,
                "The import source is not available; restore it and retry.",
            )
            .await;
            return;
        }
        if cancellation.is_cancelled() && !self.job_cancel_requested(job_id) {
            return;
        }

        let decision = match self.store.begin_import(job_id, &sha) {
            Ok(decision) => decision,
            Err(_) => {
                self.fail_job(
                    job_id,
                    "The content catalog could not be updated; retry is available.",
                )
                .await;
                return;
            }
        };
        if !matches!(&decision, ImportDecision::Add) {
            self.finish_import_decision(job_id, decision).await;
            return;
        }

        let backend = Arc::clone(&self.backend);
        let audio = prepared.clone();
        let result = run_blocking(move || backend.add_track(&audio)).await;
        match result.and_then(|pid| {
            validate_persistent_id(&pid)?;
            Ok(pid)
        }) {
            Ok(persistent_id) => {
                // The durable PID must be committed before any status edit or later query.
                if self
                    .store
                    .record_import_pid(job_id, &sha, persistent_id)
                    .is_ok()
                {
                    let _ = self.update_status(job_id).await;
                }
            }
            Err(_) => {
                let _ = self.store.set_job(job_id, |current| {
                    current.stage = JobStage::OutcomeUnknown;
                    current.error = Some(
                        "The import may have completed. No automatic re-import will occur."
                            .to_string(),
                    );
                    current.next_poll_at = None;
                });
                let _ = self.update_status(job_id).await;
            }
        }
    }

    async fn finish_import_decision(&self, job_id: &str, decision: ImportDecision) {
        match decision {
            ImportDecision::Add => (),
            ImportDecision::Cancelled => {
                if self
                    .store
                    .set_job(job_id, |job| {
                        job.stage = JobStage::Cancelled;
                        job.next_poll_at = None;
                        job.error = None;
                    })
                    .is_ok()
                {
                    let _ = self.update_status(job_id).await;
                }
            }
            ImportDecision::Reuse(persistent_id) => {
                if validate_persistent_id(&persistent_id).is_err() {
                    self.fail_job(job_id, "The saved Music track ID is invalid.")
                        .await;
                    return;
                }
                if self
                    .store
                    .set_job(job_id, |job| {
                        job.persistent_id = Some(persistent_id);
                        job.add_intent = true;
                        job.stage = JobStage::Querying;
                        job.next_poll_at = None;
                        job.error = None;
                    })
                    .is_ok()
                {
                    let _ = self.update_status(job_id).await;
                }
            }
            ImportDecision::Unknown => {
                let _ = self.store.set_job(job_id, |job| {
                    job.stage = JobStage::OutcomeUnknown;
                    job.add_intent = true;
                    job.error = Some(
                        "The import may have completed. No automatic re-import will occur."
                            .to_string(),
                    );
                    job.next_poll_at = None;
                });
                let _ = self.update_status(job_id).await;
            }
        }
    }

    async fn query_music(&self, job_id: &str, job: &StoredJob) {
        let Some(persistent_id) = job.persistent_id.clone() else {
            return;
        };
        if self
            .store
            .set_job(job_id, |current| {
                current.stage = JobStage::Querying;
                current.next_poll_at = None;
                current.error = None;
            })
            .is_err()
        {
            return;
        }
        let backend = Arc::clone(&self.backend);
        let track = run_blocking(move || backend.query_track(&persistent_id)).await;
        let track = match track.and_then(|track| {
            validate_track(&track)?;
            Ok(track)
        }) {
            Ok(track) => track,
            Err(_) => {
                self.fail_job(
                    job_id,
                    "Music status could not be read; retry is available.",
                )
                .await;
                return;
            }
        };
        let completed = track.cloud_complete();
        if self
            .store
            .set_job(job_id, |current| {
                current.track = Some(track);
                current.stage = if completed {
                    JobStage::VerifyingCopy
                } else {
                    JobStage::WaitingMusicCloud
                };
                current.next_poll_at = (!completed)
                    .then(|| unix_seconds().saturating_add(self.settings.cloud_poll_seconds));
            })
            .is_ok()
        {
            let _ = self.update_status(job_id).await;
        }
    }

    async fn verify_music_copy(&self, job_id: &str, job: &StoredJob) {
        let (Some(prepared), Some(track)) = (job.prepared.clone(), job.track.clone()) else {
            return;
        };
        if self
            .store
            .set_job(job_id, |current| current.stage = JobStage::VerifyingCopy)
            .is_err()
        {
            return;
        }
        let _ = self.update_status(job_id).await;
        let backend = Arc::clone(&self.backend);
        if run_blocking(move || backend.verify_copy(&prepared, &track))
            .await
            .is_err()
        {
            self.fail_job(
                job_id,
                "The independent Music copy could not be verified; retry is available.",
            )
            .await;
            return;
        }
        if self
            .store
            .set_job(job_id, |current| {
                current.local_copy_verified = true;
                current.stage = JobStage::ArchiveUploading;
                current.error = None;
            })
            .is_ok()
        {
            let _ = self.update_status(job_id).await;
        }
    }

    async fn check_archive_upload(&self, job_id: &str, job: &StoredJob) {
        let (Some(archive), Some(track), Some(prepared)) =
            (job.archive.clone(), job.track.clone(), job.prepared.clone())
        else {
            return;
        };
        if !job.local_copy_verified {
            self.fail_job(
                job_id,
                "The Music copy is not verified; archive eviction was blocked.",
            )
            .await;
            return;
        }
        let path = archive.path.clone();
        let backend = Arc::clone(&self.backend);
        let status = run_blocking(move || backend.archive_status(&path)).await;
        let status = match status.and_then(|status| {
            validate_archive_state(&status, &self.settings.archive_dir)?;
            Ok(status)
        }) {
            Ok(status) => status,
            Err(_) => {
                self.fail_job(
                    job_id,
                    "Archive upload status could not be read; retry is available.",
                )
                .await;
                return;
            }
        };
        if status.conflicts {
            self.fail_job(
                job_id,
                "The archive has a cloud conflict; resolve it before retrying.",
            )
            .await;
            return;
        }
        if !status.uploaded || status.uploading || status.downloading {
            let next_poll = unix_seconds().saturating_add(self.settings.cloud_poll_seconds);
            if self
                .store
                .set_job(job_id, |current| {
                    current.archive = Some(status);
                    current.stage = JobStage::WaitingArchiveCloud;
                    current.next_poll_at = Some(next_poll);
                })
                .is_ok()
            {
                let _ = self.update_status(job_id).await;
            }
            return;
        }

        if status.local_bytes {
            if self
                .store
                .set_job(job_id, |current| {
                    current.archive = Some(status.clone());
                    current.stage = JobStage::Evicting;
                    current.local_copy_verified = true;
                })
                .is_err()
            {
                return;
            }
            let _ = self.update_status(job_id).await;
            let backend = Arc::clone(&self.backend);
            let prepared_for_verify = prepared.clone();
            let track_for_verify = track.clone();
            if run_blocking(move || backend.verify_copy(&prepared_for_verify, &track_for_verify))
                .await
                .is_err()
            {
                self.fail_job(
                    job_id,
                    "The independent Music copy could not be verified; archive eviction was blocked.",
                )
                .await;
                return;
            }
            let backend = Arc::clone(&self.backend);
            let evicted = run_blocking(move || backend.evict_archive(&status, &track)).await;
            match evicted.and_then(|after| {
                validate_archive_state(&after, &self.settings.archive_dir)?;
                Ok(after)
            }) {
                Ok(after) if after.conflicts => {
                    self.fail_job(
                        job_id,
                        "The archive has a cloud conflict; resolve it before retrying.",
                    )
                    .await;
                }
                Ok(after)
                    if after.local_bytes
                        || !after.uploaded
                        || after.uploading
                        || after.downloading =>
                {
                    let next_poll = unix_seconds().saturating_add(self.settings.cloud_poll_seconds);
                    if self
                        .store
                        .set_job(job_id, |current| {
                            current.archive = Some(after);
                            current.stage = JobStage::WaitingArchiveCloud;
                            current.next_poll_at = Some(next_poll);
                        })
                        .is_ok()
                    {
                        let _ = self.update_status(job_id).await;
                    }
                }
                Ok(after) => {
                    if self
                        .store
                        .set_job(job_id, |current| {
                            current.archive = Some(after);
                            current.stage = JobStage::Complete;
                            current.next_poll_at = None;
                            current.error = None;
                        })
                        .is_ok()
                    {
                        let _ = self.update_status(job_id).await;
                    }
                }
                Err(_) => {
                    self.fail_job(
                        job_id,
                        "Archive eviction failed; the verified Music copy is retained.",
                    )
                    .await
                }
            }
        } else {
            let backend = Arc::clone(&self.backend);
            let prepared_for_verify = prepared.clone();
            let track_for_verify = track.clone();
            if run_blocking(move || backend.verify_copy(&prepared_for_verify, &track_for_verify))
                .await
                .is_err()
            {
                self.fail_job(
                    job_id,
                    "The independent Music copy could not be verified; retry is available.",
                )
                .await;
                return;
            }
            if self
                .store
                .set_job(job_id, |current| {
                    current.archive = Some(status);
                    current.stage = JobStage::Complete;
                    current.local_copy_verified = true;
                    current.next_poll_at = None;
                    current.error = None;
                })
                .is_ok()
            {
                let _ = self.update_status(job_id).await;
            }
        }
    }

    async fn fail_job(&self, job_id: &str, safe_message: &str) {
        let message = safe_message.to_string();
        if self
            .store
            .set_job(job_id, |job| {
                job.stage = JobStage::Failed;
                job.error = Some(message);
                job.next_poll_at = None;
            })
            .is_ok()
        {
            let _ = self.update_status(job_id).await;
        }
    }

    async fn update_status(&self, job_id: &str) {
        let job = match self.store.job(job_id) {
            Ok(Some(job)) => job,
            _ => return,
        };
        let text = render_status(&job);
        if job.status_message_id.is_none() {
            if job.status_send_attempted {
                return;
            }
            if self
                .store
                .set_job(job_id, |current| {
                    current.status_send_attempted = true;
                })
                .is_err()
            {
                return;
            }
            // An uncertain send is never repeated. The attempt marker is durable before I/O.
            let sent_text = text.clone();
            if let Ok(message_id) = self.telegram.send_message(job.upload.chat_id, text).await
                && message_id > 0
            {
                let _ = self.store.set_job(job_id, |current| {
                    current.status_message_id = Some(message_id);
                    current.status_text = Some(sent_text.clone());
                });
            }
            return;
        }
        if job.status_text.as_deref() == Some(text.as_str()) {
            return;
        }
        let message_id = job.status_message_id.expect("message ID was checked");
        let same_pending_text = job.status_pending_text.as_deref() == Some(text.as_str());
        if same_pending_text
            && (job.status_edit_attempts >= MAX_STATUS_EDIT_ATTEMPTS
                || job
                    .status_next_attempt_at
                    .is_some_and(|timestamp| timestamp > unix_seconds()))
        {
            return;
        }
        let attempt = if same_pending_text {
            job.status_edit_attempts.saturating_add(1)
        } else {
            1
        };
        let next_attempt_at = (attempt < MAX_STATUS_EDIT_ATTEMPTS)
            .then(|| unix_seconds().saturating_add(status_edit_backoff_seconds(attempt)));
        if self
            .store
            .set_job(job_id, |current| {
                current.status_pending_text = Some(text.clone());
                current.status_edit_attempts = attempt;
                current.status_next_attempt_at = next_attempt_at;
            })
            .is_err()
        {
            return;
        }
        // Edits are idempotent. Persist each bounded attempt and its retry deadline first;
        // keep status_text as the last text confirmed by Telegram.
        if self
            .telegram
            .edit_message_text(job.upload.chat_id, message_id, text.clone())
            .await
            .is_ok()
        {
            let _ = self.store.set_job(job_id, |current| {
                current.status_text = Some(text.clone());
                if current.status_pending_text.as_deref() == Some(text.as_str()) {
                    current.status_pending_text = None;
                    current.status_edit_attempts = 0;
                    current.status_next_attempt_at = None;
                }
            });
        }
    }

    fn job_cancel_requested(&self, job_id: &str) -> bool {
        self.store
            .job(job_id)
            .ok()
            .flatten()
            .is_some_and(|job| job.cancel_requested)
    }

    fn job_add_intent(&self, job_id: &str) -> bool {
        self.store
            .job(job_id)
            .ok()
            .flatten()
            .is_some_and(|job| job.add_intent)
    }

    fn cancel_active_pre_import(&self) {
        let active = match self.cancellations.lock() {
            Ok(active) => active.clone(),
            Err(_) => return,
        };
        for (job_id, cancellation) in active {
            if !self.job_add_intent(&job_id) {
                cancellation.cancel();
            }
        }
    }
}

#[cfg(test)]
#[path = "music_manager_tests.rs"]
pub(crate) mod tests;

async fn run_blocking<T, F>(action: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(action)
        .await
        .map_err(|_| anyhow!("music backend worker stopped"))?
}

fn validate_settings(settings: &MusicSettings) -> Result<()> {
    if settings.max_file_bytes == 0 || settings.max_file_bytes > MAX_CONFIGURED_FILE_BYTES {
        bail!("music max_file_bytes must be between 1 byte and 512 MiB");
    }
    if !(1..=3_600).contains(&settings.cloud_poll_seconds) {
        bail!("music cloud_poll_seconds must be between 1 and 3600");
    }
    for path in [
        &settings.state_dir,
        &settings.staging_dir,
        &settings.archive_dir,
    ] {
        if !path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            bail!("music directories must be absolute normalized paths");
        }
    }
    if paths_overlap(&settings.state_dir, &settings.staging_dir)
        || paths_overlap(&settings.state_dir, &settings.archive_dir)
        || paths_overlap(&settings.staging_dir, &settings.archive_dir)
    {
        bail!("music state, staging, and archive directories must be separate");
    }
    Ok(())
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn validate_upload(upload: &MusicUpload, settings: &MusicSettings) -> Result<()> {
    if upload.chat_id == 0 || upload.message_id <= 0 || upload.user_id <= 0 || upload.update_id < 0
    {
        bail!("music upload identifiers are invalid");
    }
    if upload.file_id.is_empty()
        || upload.file_id.len() > MAX_FILE_ID_BYTES
        || upload.file_id.chars().any(char::is_control)
    {
        bail!("music upload file identifier is invalid");
    }
    if upload.file_unique_id.is_empty()
        || upload.file_unique_id.len() > MAX_UNIQUE_ID_BYTES
        || upload.file_unique_id.chars().any(char::is_control)
    {
        bail!("music upload unique identifier is invalid");
    }
    if upload.file_name.is_empty()
        || upload.file_name.len() > MAX_NAME_BYTES
        || upload.file_name.chars().any(char::is_control)
        || supported_extension(&upload.file_name).is_none()
    {
        bail!("music upload filename must end in .m4a, .mp3, or .aac");
    }
    if upload.file_size == 0 || upload.file_size > settings.max_file_bytes {
        bail!("music upload size is outside the configured limit");
    }
    if upload
        .mime_type
        .as_ref()
        .is_some_and(|mime| mime.len() > 256 || mime.chars().any(char::is_control))
    {
        bail!("music upload MIME type is invalid");
    }
    Ok(())
}

fn supported_extension(file_name: &str) -> Option<&'static str> {
    let extension = file_name.rsplit_once('.')?.1;
    if extension.eq_ignore_ascii_case("m4a") {
        Some("m4a")
    } else if extension.eq_ignore_ascii_case("mp3") {
        Some("mp3")
    } else if extension.eq_ignore_ascii_case("aac") {
        Some("aac")
    } else {
        None
    }
}

fn completed_download(path: &Path, expected_size: u64) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).context("failed to inspect retained music download"),
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.nlink() != 1
        || metadata.len() > expected_size
    {
        bail!("retained music download is not an owned regular file of an allowed size");
    }
    Ok(metadata.len() == expected_size)
}

fn job_identity(upload: &MusicUpload) -> (String, String) {
    let mut digest = Sha256::new();
    digest.update(b"music-job-origin-v1\0");
    digest.update(upload.chat_id.to_be_bytes());
    digest.update(upload.message_id.to_be_bytes());
    digest.update((upload.file_unique_id.len() as u64).to_be_bytes());
    digest.update(upload.file_unique_id.as_bytes());
    let digest = format!("{:x}", digest.finalize());
    (format!("mj1-{}", &digest[..24]), digest)
}

fn validate_prepared_audio(audio: &PreparedAudio, max_original_bytes: u64) -> Result<()> {
    validate_audio_bytes(&audio.original, max_original_bytes)?;
    validate_audio_bytes(&audio.import, MAX_IMPORT_BYTES)?;
    Ok(())
}

fn validate_audio_bytes(audio: &AudioBytes, max_bytes: u64) -> Result<()> {
    normalize_sha256(&audio.sha256)?;
    if !audio.path.is_absolute()
        || audio
            .path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        || audio.size == 0
        || audio.size > max_bytes
    {
        bail!("prepared audio metadata is invalid");
    }
    Ok(())
}

fn normalize_sha256(value: &str) -> Result<String> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("audio checksum is invalid");
    }
    Ok(value.to_ascii_lowercase())
}

fn validate_track(track: &MusicTrack) -> Result<()> {
    validate_persistent_id(&track.persistent_id)?;
    if track.cloud_status.is_empty()
        || track.cloud_status.len() > 64
        || !track
            .cloud_status
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        bail!("Music cloud status is invalid");
    }
    if track
        .location
        .as_ref()
        .is_some_and(|path| !path.is_absolute())
    {
        bail!("Music track location is invalid");
    }
    Ok(())
}

fn validate_persistent_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_ID_BYTES || value.chars().any(char::is_control) {
        bail!("Music persistent ID is invalid");
    }
    Ok(())
}

fn validate_archive_state(archive: &ArchiveState, archive_dir: &Path) -> Result<()> {
    if !archive.path.is_absolute()
        || archive
            .path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        || !archive.path.starts_with(archive_dir)
    {
        bail!("archive path is outside the configured archive directory");
    }
    Ok(())
}

fn validate_stored_state(
    state: &StoredState,
    max_file_bytes: u64,
    archive_dir: &Path,
) -> Result<()> {
    if state.version != STATE_VERSION
        || state.jobs.len() > MAX_JOBS
        || state.content_catalog.len() > MAX_CATALOG_ENTRIES
    {
        bail!("music state has an unsupported version or exceeds its limits");
    }
    for (job_id, job) in &state.jobs {
        if job_id != &job.job_id || job_id.len() > MAX_ID_BYTES {
            bail!("music state contains an invalid job ID");
        }
        validate_upload_shape(&job.upload, max_file_bytes)?;
        let (expected_id, expected_origin) = job_identity(&job.upload);
        if job_id != &expected_id || job.origin_key != expected_origin {
            bail!("music state contains a mismatched upload origin");
        }
        if job
            .prepared
            .as_ref()
            .is_some_and(|audio| validate_prepared_audio(audio, max_file_bytes).is_err())
        {
            bail!("music state contains invalid prepared audio metadata");
        }
        if job
            .persistent_id
            .as_deref()
            .is_some_and(|pid| validate_persistent_id(pid).is_err())
        {
            bail!("music state contains an invalid persistent ID");
        }
        if job.persistent_id.is_some() && !job.add_intent {
            bail!("music state contains a PID without a durable add intent");
        }
        if job.add_intent && (job.prepared.is_none() || job.archive.is_none()) {
            bail!("music add intent is missing its prepared content or archive metadata");
        }
        if job.stage == JobStage::OutcomeUnknown && (!job.add_intent || job.persistent_id.is_some())
        {
            bail!("music state has an invalid unknown import outcome");
        }
        if job
            .track
            .as_ref()
            .is_some_and(|track| validate_track(track).is_err())
        {
            bail!("music state contains invalid Music track metadata");
        }
        if job
            .archive
            .as_ref()
            .is_some_and(|archive| validate_archive_state(archive, archive_dir).is_err())
        {
            bail!("music state contains an invalid archive path");
        }
        if job
            .download_path
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            bail!("music state contains an invalid download path");
        }
        if job.error.as_ref().is_some_and(|error| error.len() > 512) {
            bail!("music state contains an oversized safe error");
        }
        if job
            .status_text
            .as_ref()
            .is_some_and(|text| text.len() > 1_024)
        {
            bail!("music state contains an oversized Telegram status");
        }
        if job
            .status_pending_text
            .as_ref()
            .is_some_and(|text| text.len() > 1_024)
            || job.status_edit_attempts > MAX_STATUS_EDIT_ATTEMPTS
            || (job.status_pending_text.is_none()
                && (job.status_edit_attempts != 0 || job.status_next_attempt_at.is_some()))
            || (job.status_edit_attempts >= MAX_STATUS_EDIT_ATTEMPTS
                && job.status_next_attempt_at.is_some())
        {
            bail!("music state contains invalid Telegram edit retry metadata");
        }
        if job
            .status_message_id
            .is_some_and(|message_id| message_id <= 0)
        {
            bail!("music state contains an invalid Telegram status message ID");
        }
    }
    for (sha, entry) in &state.content_catalog {
        let normalized_key = normalize_sha256(sha)?;
        let normalized_original = normalize_sha256(&entry.prepared.original.sha256)?;
        if normalized_key.as_str() != sha || normalized_original != normalized_key {
            bail!("music catalog key does not match its original content hash");
        }
        validate_prepared_audio(&entry.prepared, max_file_bytes)?;
        if !entry.archive.path.is_absolute()
            || entry
                .archive
                .path
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            bail!("music catalog contains an invalid archive path");
        }
        validate_archive_state(&entry.archive, archive_dir)?;
        if let Some(pid) = &entry.persistent_id {
            validate_persistent_id(pid)?;
        }
        if !entry.add_intent && entry.persistent_id.is_some() {
            bail!("music catalog contains a PID without a durable add intent");
        }
    }
    Ok(())
}

pub(crate) fn validate_upload_shape(upload: &MusicUpload, max_file_bytes: u64) -> Result<()> {
    // Stored records retain the same bounds as newly admitted updates.
    let settings = MusicSettings {
        enabled: true,
        state_dir: PathBuf::from("/state"),
        staging_dir: PathBuf::from("/staging"),
        archive_dir: PathBuf::from("/archive"),
        max_file_bytes,
        ..MusicSettings::default()
    };
    validate_upload(upload, &settings)
}

fn render_status(job: &StoredJob) -> String {
    match &job.error {
        Some(error) => format!(
            "Music upload {}: {}. {error}",
            job.job_id,
            job.stage.label()
        ),
        None => format!("Music upload {}: {}.", job.job_id, job.stage.label()),
    }
}

fn status_schedule(job: &StoredJob, now: u64) -> StatusSchedule {
    if job.status_message_id.is_none() {
        return if job.status_send_attempted {
            StatusSchedule::None
        } else {
            StatusSchedule::Due
        };
    }
    let desired = render_status(job);
    if job.status_text.as_deref() == Some(desired.as_str()) {
        return StatusSchedule::None;
    }
    if job.status_pending_text.as_deref() != Some(desired.as_str()) {
        return StatusSchedule::Due;
    }
    if job.status_edit_attempts >= MAX_STATUS_EDIT_ATTEMPTS {
        return StatusSchedule::None;
    }
    match job.status_next_attempt_at {
        Some(timestamp) if timestamp > now => StatusSchedule::At(timestamp),
        _ => StatusSchedule::Due,
    }
}

fn status_edit_backoff_seconds(attempt: u8) -> u64 {
    1_u64 << u32::from(attempt.saturating_sub(1).min(8))
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn validate_private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).with_context(|| {
        format!(
            "failed to inspect private music directory {}",
            path.display()
        )
    })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o7777 != 0o700
    {
        bail!("music directory must be an owner-private directory");
    }
    Ok(())
}

fn ensure_private_directory(path: &Path, provider: &dyn QueueFileProvider) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("music directory has no parent"))?;
    ensure_directory_path_exists(parent, provider)?;
    ensure_private_directory_entry(path, provider)
}

fn ensure_directory_path_exists(path: &Path, provider: &dyn QueueFileProvider) -> Result<()> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => return Ok(()),
        Ok(_) => bail!("music directory parent is not a directory"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("failed to inspect music directory parent"),
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("music directory parent does not exist"))?;
    if !fs::metadata(parent)
        .context("music directory parent must already exist")?
        .is_dir()
    {
        bail!("music directory parent must already exist");
    }
    ensure_private_directory_entry(path, provider)
}

fn ensure_private_directory_entry(path: &Path, provider: &dyn QueueFileProvider) -> Result<()> {
    let mut accessed = false;
    let mut ensure = |coordinated_path: &Path| -> Result<()> {
        match fs::symlink_metadata(coordinated_path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                match builder.create(coordinated_path) {
                    Ok(()) => sync_parent(coordinated_path)?,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => {
                        return Err(error).context("failed to create private music directory");
                    }
                }
            }
            Err(error) => return Err(error).context("failed to inspect private music directory"),
        }
        validate_private_directory(coordinated_path)?;
        accessed = true;
        Ok(())
    };
    provider.coordinate_write(path, &mut ensure)?;
    if !accessed {
        bail!("File Provider did not supply a coordinated music directory accessor");
    }
    Ok(())
}

fn ensure_private_lock_file(path: &Path, provider: &dyn QueueFileProvider) -> Result<()> {
    let mut accessed = false;
    let mut ensure = |coordinated_path: &Path| -> Result<()> {
        match fs::symlink_metadata(coordinated_path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(coordinated_path)
                    .context("failed to create private music owner lock")?;
                file.sync_all().context("failed to sync music owner lock")?;
                sync_parent(coordinated_path)?;
            }
            Err(error) => return Err(error).context("failed to inspect music owner lock"),
        }
        let file = open_private_file(coordinated_path, true)?;
        validate_private_file(&file, 0o600)?;
        accessed = true;
        Ok(())
    };
    provider.coordinate_write(path, &mut ensure)?;
    if !accessed {
        bail!("File Provider did not supply a coordinated music lock accessor");
    }
    Ok(())
}

fn acquire_owner_lock(path: &Path, provider: &dyn QueueFileProvider) -> Result<File> {
    let mut lock = None;
    let mut open_and_lock = |coordinated_path: &Path| -> Result<()> {
        let file = open_private_file(coordinated_path, true)?;
        rustix::fs::flock(file.as_fd(), FlockOperation::NonBlockingLockExclusive)
            .map_err(|error| anyhow!(std::io::Error::from_raw_os_error(error.raw_os_error())))
            .context("music job state already has a live owner")?;
        validate_private_file(&file, 0o600)?;
        lock = Some(file);
        Ok(())
    };
    provider.coordinate_read(path, &mut open_and_lock)?;
    lock.ok_or_else(|| anyhow!("File Provider did not supply a coordinated music owner lock"))
}

fn open_private_file(path: &Path, writable: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(writable)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .context("failed to open private music state file")?;
    validate_private_file(&file, 0o600)?;
    Ok(file)
}

fn validate_private_file(file: &File, mode: u32) -> Result<()> {
    let metadata = file
        .metadata()
        .context("failed to inspect private music state file")?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o7777 != mode
        || metadata.nlink() != 1
    {
        bail!("music state file must be a single-link owner-private regular file");
    }
    Ok(())
}

fn read_state(path: &Path, provider: &dyn QueueFileProvider) -> Result<Option<StoredState>> {
    let mut result = None;
    let mut read = |coordinated_path: &Path| -> Result<()> {
        let file = match open_private_file(coordinated_path, false) {
            Ok(file) => file,
            Err(error)
                if error.chain().any(|cause| {
                    cause
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
                }) =>
            {
                result = Some(None);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if file.metadata()?.len() > MAX_STATE_BYTES as u64 {
            bail!("music state file exceeds its read limit");
        }
        let mut reader = file.take((MAX_STATE_BYTES + 1) as u64);
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .context("failed to read music state")?;
        if bytes.len() > MAX_STATE_BYTES {
            bail!("music state file exceeds its read limit");
        }
        result = Some(Some(
            serde_json::from_slice(&bytes).context("failed to parse music state")?,
        ));
        Ok(())
    };
    provider.coordinate_read(path, &mut read)?;
    result.ok_or_else(|| anyhow!("File Provider did not supply a coordinated music state accessor"))
}

fn write_state_atomic(
    path: &Path,
    state: &StoredState,
    provider: &dyn QueueFileProvider,
) -> Result<()> {
    let bytes = serde_json::to_vec(state).context("failed to encode music state")?;
    if bytes.len() > MAX_STATE_BYTES {
        bail!("music state exceeds its serialized size limit");
    }
    let mut written = false;
    let mut write = |coordinated_path: &Path| -> Result<()> {
        match fs::symlink_metadata(coordinated_path) {
            Ok(metadata)
                if metadata.file_type().is_symlink()
                    || !metadata.is_file()
                    || metadata.uid() != unsafe { libc::geteuid() }
                    || metadata.permissions().mode() & 0o7777 != 0o600
                    || metadata.nlink() != 1 =>
            {
                bail!("existing music state file is not owner-private");
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("failed to inspect existing music state"),
        }
        let parent = coordinated_path
            .parent()
            .ok_or_else(|| anyhow!("music state file has no parent"))?;
        let mut opened = None;
        let mut temporary = None;
        for _ in 0..8 {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let candidate = parent.join(format!(
                ".jobs.{}.{}.{}.tmp",
                std::process::id(),
                nonce,
                sequence
            ));
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&candidate)
            {
                Ok(file) => {
                    opened = Some(file);
                    temporary = Some(candidate);
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error).context("failed to create temporary music state");
                }
            }
        }
        let temporary =
            temporary.ok_or_else(|| anyhow!("could not allocate a unique music state temp"))?;
        let mut file = opened.ok_or_else(|| anyhow!("music state temp file was not opened"))?;
        let result = (|| -> Result<()> {
            file.write_all(&bytes)
                .context("failed to write music state")?;
            file.sync_all().context("failed to sync music state")?;
            validate_private_file(&file, 0o600)?;
            drop(file);
            fs::rename(&temporary, coordinated_path).context("failed to replace music state")?;
            sync_parent(coordinated_path)?;
            written = true;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    };
    provider.coordinate_write(path, &mut write)?;
    if !written {
        bail!("File Provider did not supply a coordinated music state accessor");
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("music state path has no parent"))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .context("failed to sync music state directory")
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn deterministic_job_key_uses_origin_identity_and_supported_extensions_are_folded() {
        let upload = sample_upload("ALBUM.M4A");
        assert_eq!(job_identity(&upload), job_identity(&upload));
        assert_eq!(supported_extension("ALBUM.M4A"), Some("m4a"));
        assert_eq!(supported_extension("voice.Mp3"), Some("mp3"));
        assert_eq!(supported_extension("voice.aAc"), Some("aac"));
        assert_eq!(supported_extension("voice.wav"), None);
    }

    #[test]
    fn archive_path_must_stay_inside_archive_root() {
        let root = Path::new("/tmp/archive");
        let state = ArchiveState {
            path: root.join("music-a.m4a"),
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: true,
        };
        assert!(validate_archive_state(&state, root).is_ok());
        let outside = ArchiveState {
            path: PathBuf::from("/tmp/archive-copy/music-a.m4a"),
            ..state
        };
        assert!(validate_archive_state(&outside, root).is_err());
    }

    fn sample_upload(file_name: &str) -> MusicUpload {
        MusicUpload {
            update_id: 1,
            chat_id: 2,
            message_id: 3,
            user_id: 2,
            file_id: "file_id_1".to_string(),
            file_unique_id: "unique_id_1".to_string(),
            file_name: file_name.to_string(),
            file_size: 10,
            mime_type: Some("audio/mp4".to_string()),
        }
    }
}
