use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

use crate::config::AppConfig;
use crate::router::JobRequest;
use crate::safe_fs::{BoundFile, EntryIdentity, RootedFs};

const QUEUE_DIRECTORY: &str = ".telegram-video-downloader-queue";
const TERMINAL_HISTORY_DIRECTORY: &str = "history";
const INDEX_FILE: &str = "index.json";
const QUEUE_OPERATION_LOCK_FILE: &str = "claims.lock";
const LEGACY_INDEX_VERSION: u32 = 1;
const INDEX_VERSION: u32 = 2;
const TASK_RECORD_VERSION: u32 = 1;
const MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_ACTIVE_RECORDS: usize = 20_000;
const MAX_HISTORY_INDEX_ENTRIES: usize = 10_000;
const MAX_PERSISTED_MEDIA_HASHES: usize = 128;
const MAX_PERSISTED_MEDIA_HASH_BYTES: usize = 256 * 1024;
const QUEUE_PAGE_SIZE: usize = 10;
static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Received,
    Preparing,
    AwaitingSelection,
    AwaitingConfirmation,
    AwaitingDuplicateChoice,
    Queued,
    Running,
    Verifying,
    Interrupted,
    Failed,
    Cancelled,
    Completed,
}

impl TaskStatus {
    pub fn is_unfinished(self) -> bool {
        matches!(
            self,
            Self::Received
                | Self::Preparing
                | Self::AwaitingSelection
                | Self::AwaitingConfirmation
                | Self::AwaitingDuplicateChoice
                | Self::Queued
                | Self::Running
                | Self::Verifying
                | Self::Interrupted
        )
    }

    pub fn is_history(self) -> bool {
        matches!(self, Self::Cancelled | Self::Completed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PlanSize {
    pub subject: String,
    pub bytes: u64,
    pub provenance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryMediaHashManifest {
    pub file_count: usize,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PlanValidationSnapshot {
    pub stable_media_ids: Vec<String>,
    pub selected_format_ids: Vec<String>,
    pub exact_sizes: Vec<PlanSize>,
    pub approximate_sizes: Vec<PlanSize>,
    pub resolution_codecs: Vec<String>,
    pub title: Option<String>,
}

impl PlanValidationSnapshot {
    pub fn blocking_differences(&self, current: &Self) -> Vec<&'static str> {
        let mut differences = Vec::new();
        if self.stable_media_ids != current.stable_media_ids {
            differences.push("media identity");
        }
        if self.selected_format_ids != current.selected_format_ids {
            differences.push("selected format");
        }
        if self.exact_sizes != current.exact_sizes {
            differences.push("exact size");
        }
        if self.resolution_codecs != current.resolution_codecs {
            differences.push("media properties");
        }
        differences
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub schema_version: u32,
    pub id: String,
    pub update_id: i64,
    pub message_id: i64,
    #[serde(default)]
    pub status_message_id: Option<i64>,
    pub chat_id: i64,
    pub submitter_user_id: Option<i64>,
    pub ordinal: usize,
    pub original_url: String,
    pub url_was_sanitized: bool,
    pub job: JobRequest,
    pub status: TaskStatus,
    #[serde(default)]
    pub cancel_requested: bool,
    pub plan: Option<PlanValidationSnapshot>,
    pub proposed_plan: Option<PlanValidationSnapshot>,
    pub saved_location: Option<String>,
    #[serde(default)]
    pub primary_media_hashes: BTreeMap<String, String>,
    #[serde(default)]
    pub primary_media_hash_manifest: Option<PrimaryMediaHashManifest>,
    pub staging_attempts: Vec<PathBuf>,
    pub media_entries_total: usize,
    pub media_entries_completed: usize,
    pub media_entries_failed: usize,
    pub error: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    pub revision: u64,
    pub activity_revision: u64,
    pub user_actions: u64,
}

#[derive(Debug, Clone)]
pub struct RestartSummary {
    pub chat_id: i64,
    pub interrupted_jobs: usize,
    pub recently_completed_jobs: usize,
    pub recently_failed_jobs: usize,
    pub completed_entries: usize,
    pub remaining_entries: usize,
    pub failed_entries: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoreIndex {
    version: u32,
    #[serde(default)]
    tasks: BTreeMap<String, TaskIndexEntry>,
    #[serde(default)]
    chat_activity: BTreeMap<String, ChatActivity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskIndexEntry {
    chat_id: i64,
    record_path: PathBuf,
    #[serde(default)]
    move_target: Option<PathBuf>,
    revision: u64,
    updated_at: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct ChatActivity {
    revision: u64,
    notified_revision: u64,
}

#[derive(Clone)]
struct DownloadStore {
    root_path: PathBuf,
    root_aliases: Vec<PathBuf>,
    root: RootedFs,
    queue_dir: PathBuf,
    queue_identity: EntryIdentity,
    operation_lock_path: PathBuf,
}

struct QueueOperationLock {
    _file: BoundFile,
}

pub struct QueueManager {
    video: DownloadStore,
    pdf: DownloadStore,
    operation_lock: Mutex<()>,
    cancellations: Mutex<HashMap<String, Arc<Notify>>>,
    #[cfg(test)]
    interrupt_after_sidecar_move: AtomicBool,
}

impl TaskRecord {
    pub fn new(
        id: String,
        update_id: i64,
        message_id: i64,
        chat_id: i64,
        submitter_user_id: Option<i64>,
        ordinal: usize,
        job: JobRequest,
    ) -> Self {
        let original_url = match &job {
            JobRequest::Bilibili { url, .. }
            | JobRequest::Youtube { url }
            | JobRequest::Pdf { url } => url.clone(),
        };
        let now = unix_time();
        Self {
            schema_version: TASK_RECORD_VERSION,
            id,
            update_id,
            message_id,
            status_message_id: None,
            chat_id,
            submitter_user_id,
            ordinal,
            original_url,
            url_was_sanitized: false,
            job,
            status: TaskStatus::Received,
            cancel_requested: false,
            plan: None,
            proposed_plan: None,
            saved_location: None,
            primary_media_hashes: BTreeMap::new(),
            primary_media_hash_manifest: None,
            staging_attempts: Vec::new(),
            media_entries_total: 1,
            media_entries_completed: 0,
            media_entries_failed: 0,
            error: None,
            created_at: now,
            updated_at: now,
            revision: 1,
            activity_revision: 0,
            user_actions: 0,
        }
    }
}

impl QueueManager {
    pub fn open(config: &AppConfig) -> Result<Self> {
        let mut video = DownloadStore::new(&config.downloads.video_dir)?;
        let pdf_root = RootedFs::new(&config.downloads.pdf_dir)?;
        pdf_root.validate_configured_root()?;
        let pdf = if video.root.root_identity() == pdf_root.root_identity() {
            let pdf_alias = pdf_root.logical_root_path().to_path_buf();
            if !video.root_aliases.contains(&pdf_alias) {
                video.root_aliases.push(pdf_alias);
            }
            video.clone()
        } else {
            DownloadStore::from_root(pdf_root)?
        };
        let manager = Self {
            video,
            pdf,
            operation_lock: Mutex::new(()),
            cancellations: Mutex::new(HashMap::new()),
            #[cfg(test)]
            interrupt_after_sidecar_move: AtomicBool::new(false),
        };
        let _video_operation_lock = manager.video.lock_queue_operations()?;
        let _pdf_operation_lock = if manager.video.shares_root(&manager.pdf) {
            None
        } else {
            Some(manager.pdf.lock_queue_operations()?)
        };
        manager.recover_interrupted_tasks()?;
        Ok(manager)
    }

    pub fn create(&self, mut task: TaskRecord) -> Result<bool> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let _video_operation_lock = self.video.lock_queue_operations()?;
        let _pdf_operation_lock = if self.video.shares_root(&self.pdf) {
            None
        } else {
            Some(self.pdf.lock_queue_operations()?)
        };
        if self.find_record_unlocked(&task.id)?.is_some() {
            return Ok(false);
        }
        let store = self.store_for_job(&task.job);
        store.create_task(&mut task)?;
        Ok(true)
    }

    pub fn get(&self, id: &str) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        self.find_record_unlocked(id)
    }

    pub fn list(&self, chat_id: i64, history: bool, page: usize) -> Result<Vec<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let mut records = self.video.list_records()?;
        if !self.video.shares_root(&self.pdf) {
            records.extend(self.pdf.list_records()?);
        }
        records.retain(|record| {
            record.chat_id == chat_id
                && if history {
                    record.status.is_history()
                } else {
                    !record.status.is_history()
                }
        });
        records.sort_by_key(|record| std::cmp::Reverse(record.updated_at));
        let start = page.saturating_mul(QUEUE_PAGE_SIZE);
        Ok(records
            .into_iter()
            .skip(start)
            .take(QUEUE_PAGE_SIZE)
            .collect())
    }

    pub fn page_count(&self, chat_id: i64, history: bool) -> Result<usize> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let mut records = self.video.list_records()?;
        if !self.video.shares_root(&self.pdf) {
            records.extend(self.pdf.list_records()?);
        }
        let count = records
            .iter()
            .filter(|record| {
                record.chat_id == chat_id
                    && if history {
                        record.status.is_history()
                    } else {
                        !record.status.is_history()
                    }
            })
            .count();
        Ok(count.div_ceil(QUEUE_PAGE_SIZE).max(1))
    }

    pub fn claim_resume(&self, id: &str, retry_failed: bool) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let video_operation_lock = self.video.lock_queue_operations()?;
        if let Some((record, entry)) = self.video.get_task(id)? {
            return Self::claim_resume_in_store(&self.video, retry_failed, record, entry);
        }
        drop(video_operation_lock);
        if self.video.shares_root(&self.pdf) {
            return Ok(None);
        }
        let _pdf_operation_lock = self.pdf.lock_queue_operations()?;
        let Some((record, entry)) = self.pdf.get_task(id)? else {
            return Ok(None);
        };
        Self::claim_resume_in_store(&self.pdf, retry_failed, record, entry)
    }

    fn claim_resume_in_store(
        store: &DownloadStore,
        retry_failed: bool,
        mut record: TaskRecord,
        entry: TaskIndexEntry,
    ) -> Result<Option<TaskRecord>> {
        let allowed = if retry_failed {
            record.status == TaskStatus::Failed
        } else {
            matches!(
                record.status,
                TaskStatus::Received
                    | TaskStatus::AwaitingSelection
                    | TaskStatus::AwaitingConfirmation
                    | TaskStatus::AwaitingDuplicateChoice
                    | TaskStatus::Interrupted
            )
        };
        if !allowed {
            return Ok(None);
        }
        record.status = TaskStatus::Preparing;
        record.cancel_requested = false;
        record.error = None;
        record.user_actions = record.user_actions.saturating_add(1);
        store.save_mutated_record(record, entry, true).map(Some)
    }

    pub fn update_job(&self, id: &str, job: JobRequest, status: TaskStatus) -> Result<TaskRecord> {
        let (job, url_was_sanitized) = sanitize_job_for_storage(job);
        self.update(id, true, |record| {
            if matches!(record.status, TaskStatus::Cancelled | TaskStatus::Completed) {
                bail!("task {id} is already terminal");
            }
            record.original_url = job_url(&job).to_string();
            record.url_was_sanitized |= url_was_sanitized;
            record.job = job;
            record.status = status;
            record.cancel_requested = false;
            record.error = None;
            Ok(())
        })
    }

    pub fn set_status(
        &self,
        id: &str,
        status: TaskStatus,
        error: Option<String>,
    ) -> Result<TaskRecord> {
        self.update(id, true, |record| {
            if record.status == TaskStatus::Cancelled && status != TaskStatus::Cancelled {
                bail!("task {id} was canceled");
            }
            if record.status == TaskStatus::Completed && status != TaskStatus::Completed {
                bail!("task {id} is already complete");
            }
            record.status = status;
            if !matches!(
                status,
                TaskStatus::Running | TaskStatus::AwaitingConfirmation
            ) {
                record.cancel_requested = false;
            }
            record.error = error;
            Ok(())
        })
    }

    pub fn begin_run(&self, id: &str) -> Result<bool> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(false);
        };
        if record.status != TaskStatus::Queued {
            return Ok(false);
        }
        record.status = TaskStatus::Running;
        record.cancel_requested = false;
        record.error = None;
        store.save_mutated_record(record, entry, true)?;
        Ok(true)
    }

    pub fn begin_verification(&self, id: &str) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if record.status != TaskStatus::Running {
            return Ok(None);
        }
        record.status = TaskStatus::Verifying;
        record.cancel_requested = false;
        store.save_mutated_record(record, entry, true).map(Some)
    }

    pub fn set_plan(
        &self,
        id: &str,
        current: PlanValidationSnapshot,
    ) -> Result<(TaskRecord, Vec<&'static str>)> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            bail!("persistent task {id} was not found");
        };
        if record.status != TaskStatus::Running {
            bail!("task {id} changed state before plan validation completed");
        }
        let mut differences = Vec::new();
        {
            if let Some(previous) = &record.plan {
                differences = previous.blocking_differences(&current);
                if !differences.is_empty() {
                    record.proposed_plan = Some(current.clone());
                    record.status = TaskStatus::AwaitingConfirmation;
                } else {
                    record.plan = Some(current);
                    record.proposed_plan = None;
                }
            } else {
                record.plan = Some(current);
            }
        }
        let record = store.save_mutated_record(record, entry, !differences.is_empty())?;
        Ok((record, differences))
    }

    pub fn accept_proposed_plan(&self, id: &str) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if record.status != TaskStatus::AwaitingConfirmation {
            return Ok(None);
        }
        let Some(plan) = record.proposed_plan.take() else {
            return Ok(None);
        };
        record.plan = Some(plan);
        record.status = TaskStatus::Preparing;
        record.cancel_requested = false;
        record.user_actions = record.user_actions.saturating_add(1);
        store.save_mutated_record(record, entry, true).map(Some)
    }

    pub fn set_collection_progress(
        &self,
        id: &str,
        total: usize,
        completed: usize,
        failed: usize,
    ) -> Result<TaskRecord> {
        self.update(id, true, |record| {
            record.media_entries_total = total.max(1);
            record.media_entries_completed = completed.min(record.media_entries_total);
            record.media_entries_failed = failed.min(record.media_entries_total);
            Ok(())
        })
    }

    pub fn record_staging_path(&self, id: &str, path: PathBuf) -> Result<TaskRecord> {
        self.update(id, false, |record| {
            if !record.staging_attempts.contains(&path) {
                record.staging_attempts.push(path);
            }
            Ok(())
        })
    }

    pub fn set_status_message_id(&self, id: &str, message_id: i64) -> Result<TaskRecord> {
        self.update(id, false, |record| {
            record.status_message_id = Some(message_id);
            Ok(())
        })
    }

    pub fn register_cancellation(&self, id: &str) -> Result<Arc<Notify>> {
        let notify = Arc::new(Notify::new());
        let mut active = self.cancellations.lock().map_err(poisoned_lock)?;
        active.insert(id.to_string(), Arc::clone(&notify));
        Ok(notify)
    }

    pub fn unregister_cancellation(&self, id: &str) -> Result<()> {
        self.cancellations.lock().map_err(poisoned_lock)?.remove(id);
        Ok(())
    }

    pub fn notify_cancel(&self, id: &str) -> Result<()> {
        if let Some(notify) = self.cancellations.lock().map_err(poisoned_lock)?.get(id) {
            notify.notify_one();
        }
        Ok(())
    }

    pub fn complete(
        &self,
        id: &str,
        saved_location: String,
        media_paths: &[PathBuf],
        hashes: BTreeMap<String, String>,
    ) -> Result<TaskRecord> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, mut entry)) = self.find_record_entry_unlocked(id)? else {
            bail!("persistent task {id} was missing at completion");
        };
        if record.status != TaskStatus::Verifying {
            bail!("task {id} changed state before output verification completed");
        }
        let media_paths = media_paths
            .iter()
            .map(|path| store.normalize_media_path(path))
            .collect::<Result<Vec<_>>>()?;
        record.status = TaskStatus::Completed;
        record.error = None;
        record.saved_location = Some(saved_location);
        let (persisted_hashes, hash_manifest) = persist_media_hashes(hashes)?;
        record.primary_media_hashes = persisted_hashes;
        record.primary_media_hash_manifest = hash_manifest;
        if record.media_entries_total <= 1 {
            record.media_entries_total = 1;
            record.media_entries_completed = 1;
        } else {
            record.media_entries_completed = record.media_entries_total;
            record.media_entries_failed = 0;
        }
        let record_path = entry.record_path.clone();
        let destination = sidecar_destination(&store.root_path, &record, &media_paths)?;
        record.cancel_requested = false;
        let Some(destination) = destination else {
            return store.save_mutated_record(record, entry, true);
        };
        record.updated_at = unix_time();
        record.revision = record.revision.saturating_add(1);
        entry.move_target = Some(destination.clone());
        entry.revision = record.revision;
        entry.updated_at = record.updated_at;
        store.write_record(&record_path, &record)?;
        store.save_index_task(id, entry.clone())?;
        store.move_record_to_sidecar(id, &record_path, &destination)?;
        #[cfg(test)]
        if self.interrupt_after_sidecar_move.load(Ordering::Relaxed) {
            bail!("simulated interruption after task sidecar migration");
        }
        entry.record_path = destination;
        entry.move_target = None;
        store.save_mutated_record(record, entry, true)
    }

    pub fn fail(&self, id: &str, message: String) -> Result<TaskRecord> {
        self.update(id, true, |record| {
            if matches!(record.status, TaskStatus::Cancelled | TaskStatus::Completed) {
                bail!("task {id} is already terminal");
            }
            record.status = TaskStatus::Failed;
            record.cancel_requested = false;
            record.error = Some(message);
            if record.media_entries_total <= 1 && record.media_entries_completed == 0 {
                record.media_entries_failed = 1;
            }
            Ok(())
        })
    }

    pub fn cancel(&self, id: &str, chat_id: i64) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if record.chat_id != chat_id
            || !record.status.is_unfinished()
            || record.status == TaskStatus::Verifying
        {
            return Ok(None);
        }
        if record.status == TaskStatus::Running {
            record.cancel_requested = true;
            record.user_actions = record.user_actions.saturating_add(1);
            let result = store.save_mutated_record(record, entry, false)?;
            drop(_guard);
            self.notify_cancel(id)?;
            return Ok(Some(result));
        }
        record.status = TaskStatus::Cancelled;
        record.cancel_requested = false;
        record.error = None;
        record.user_actions = record.user_actions.saturating_add(1);
        let result = store.save_mutated_record(record, entry, true)?;
        drop(_guard);
        self.notify_cancel(id)?;
        Ok(Some(result))
    }

    pub fn finish_cancellation(&self, id: &str) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if !record.cancel_requested
            || !matches!(
                record.status,
                TaskStatus::Running | TaskStatus::AwaitingConfirmation
            )
        {
            return Ok(None);
        }
        record.status = TaskStatus::Cancelled;
        record.cancel_requested = false;
        record.error = None;
        store.save_mutated_record(record, entry, true).map(Some)
    }

    pub fn cancel_for_chat(&self, id: &str, chat_id: i64) -> Result<Option<TaskRecord>> {
        self.cancel(id, chat_id)
    }

    pub fn validate_chat(&self, id: &str, chat_id: i64) -> Result<Option<TaskRecord>> {
        Ok(self.get(id)?.filter(|record| record.chat_id == chat_id))
    }

    pub fn startup_summaries(&self) -> Result<Vec<RestartSummary>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let video = self.video.restart_summary_data()?;
        let pdf = self.pdf.restart_summary_data()?;
        let mut summaries = BTreeMap::<i64, RestartSummary>::new();
        let data = if self.video.shares_root(&self.pdf) {
            vec![video]
        } else {
            vec![video, pdf]
        };
        for (records, activities) in data {
            for (chat_id, activity) in activities {
                if activity.revision <= activity.notified_revision {
                    continue;
                }
                let chat_records = records
                    .iter()
                    .filter(|record| record.chat_id == chat_id)
                    .collect::<Vec<_>>();
                if !chat_records
                    .iter()
                    .any(|record| record.status.is_unfinished())
                {
                    continue;
                }
                let summary = summaries.entry(chat_id).or_insert(RestartSummary {
                    chat_id,
                    interrupted_jobs: 0,
                    recently_completed_jobs: 0,
                    recently_failed_jobs: 0,
                    completed_entries: 0,
                    remaining_entries: 0,
                    failed_entries: 0,
                });
                for record in chat_records {
                    if record.status.is_unfinished() {
                        summary.interrupted_jobs += 1;
                        summary.completed_entries += record.media_entries_completed;
                        summary.failed_entries += record.media_entries_failed;
                        summary.remaining_entries += record
                            .media_entries_total
                            .saturating_sub(record.media_entries_completed)
                            .saturating_sub(record.media_entries_failed);
                    } else if record.activity_revision > activity.notified_revision {
                        match record.status {
                            TaskStatus::Completed => summary.recently_completed_jobs += 1,
                            TaskStatus::Failed => summary.recently_failed_jobs += 1,
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(summaries.into_values().collect())
    }

    pub fn mark_restart_summary_sent(&self, chat_id: i64) -> Result<()> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        self.video.mark_notified(chat_id)?;
        if !self.video.shares_root(&self.pdf) {
            self.pdf.mark_notified(chat_id)?;
        }
        Ok(())
    }

    fn update<F>(&self, id: &str, activity: bool, mutate: F) -> Result<TaskRecord>
    where
        F: FnOnce(&mut TaskRecord) -> Result<()>,
    {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            bail!("persistent task {id} was not found");
        };
        mutate(&mut record)?;
        store.save_mutated_record(record, entry, activity)
    }

    fn find_record_unlocked(&self, id: &str) -> Result<Option<TaskRecord>> {
        Ok(self
            .find_record_entry_unlocked(id)?
            .map(|(_, record, _)| record))
    }

    fn find_record_entry_unlocked(
        &self,
        id: &str,
    ) -> Result<Option<(&DownloadStore, TaskRecord, TaskIndexEntry)>> {
        if let Some((record, entry)) = self.video.get_task(id)? {
            return Ok(Some((&self.video, record, entry)));
        }
        if let Some((record, entry)) = self.pdf.get_task(id)? {
            return Ok(Some((&self.pdf, record, entry)));
        }
        Ok(None)
    }

    fn store_for_job(&self, job: &JobRequest) -> &DownloadStore {
        if matches!(job, JobRequest::Pdf { .. }) {
            &self.pdf
        } else {
            &self.video
        }
    }

    fn recover_interrupted_tasks(&self) -> Result<()> {
        let stores = if self.video.shares_root(&self.pdf) {
            vec![&self.video]
        } else {
            vec![&self.video, &self.pdf]
        };
        for store in stores {
            for mut record in store.list_records()? {
                if record.status.is_unfinished() && record.status != TaskStatus::Interrupted {
                    let Some((current, entry)) = store.get_task(&record.id)? else {
                        continue;
                    };
                    record = current;
                    record.status = TaskStatus::Interrupted;
                    record.cancel_requested = false;
                    record.error = Some("Task was interrupted by process restart.".to_string());
                    store.save_mutated_record(record, entry, true)?;
                }
            }
        }
        Ok(())
    }
}

pub fn hash_primary_media(
    root_path: &Path,
    media_paths: &[PathBuf],
) -> Result<BTreeMap<String, String>> {
    let root = RootedFs::new(root_path)
        .with_context(|| format!("failed to bind download root {}", root_path.display()))?;
    root.validate_configured_root()?;
    let logical_root = root.logical_root_path().to_path_buf();
    let canonical_root = root.root_path().to_path_buf();
    let mut hashes = BTreeMap::new();
    for path in media_paths {
        let bound_path = if path.starts_with(&logical_root) {
            path.clone()
        } else if path.starts_with(&canonical_root) {
            logical_root.join(path.strip_prefix(&canonical_root)?)
        } else {
            bail!("published media path is outside its configured download root");
        };
        let file = root.open_bound_file(&bound_path)?.ok_or_else(|| {
            anyhow!(
                "published media disappeared before hashing: {}",
                path.display()
            )
        })?;
        let identity = file.identity();
        let file = file.duplicate_std_file()?;
        hashes.insert(
            path.display().to_string(),
            sha256_file(&root, &bound_path, file, identity)?,
        );
    }
    Ok(hashes)
}

impl DownloadStore {
    fn new(root_path: &Path) -> Result<Self> {
        Self::from_root(RootedFs::new(root_path)?)
    }

    fn from_root(root: RootedFs) -> Result<Self> {
        root.validate_configured_root()?;
        let queue_dir = root.logical_root_path().join(QUEUE_DIRECTORY);
        let identity = root.create_dir(&queue_dir, 0o700)?;
        let queue_identity = match identity {
            Some(identity) => identity,
            None => root
                .entry_identity(&queue_dir)?
                .ok_or_else(|| anyhow!("task queue directory disappeared"))?,
        };
        let queue_entry = root.bind_entry(&queue_dir, false)?;
        root.validate_private_bound_directory(&queue_entry, queue_identity, 0o700)
            .context("task queue directory must be owner-private")?;
        let root_path = root.logical_root_path().to_path_buf();
        let store = Self {
            root_path: root_path.clone(),
            root_aliases: vec![root_path],
            operation_lock_path: queue_dir.join(QUEUE_OPERATION_LOCK_FILE),
            root,
            queue_dir,
            queue_identity,
        };
        store.ensure_operation_lock_file()?;
        let _operation_lock = store.lock_queue_operations()?;
        if store
            .read_private_file(&store.index_path(), MAX_INDEX_BYTES)?
            .is_none()
        {
            store.write_private_file(
                &store.index_path(),
                &serde_json::to_vec(&StoreIndex {
                    version: INDEX_VERSION,
                    ..StoreIndex::default()
                })?,
            )?;
        }
        store.reconcile_index()?;
        Ok(store)
    }

    fn shares_root(&self, other: &Self) -> bool {
        self.root.root_identity() == other.root.root_identity()
    }

    fn ensure_operation_lock_file(&self) -> Result<()> {
        if let Some(file) = self.root.open_bound_file(&self.operation_lock_path)? {
            file.validate_private_single_link(0o600)?;
            return Ok(());
        }
        if let Err(create_error) =
            self.root
                .create_new_bound_file(&self.operation_lock_path, &[], 0o600)
        {
            let Some(file) = self.root.open_bound_file(&self.operation_lock_path)? else {
                return Err(create_error);
            };
            file.validate_private_single_link(0o600)?;
        }
        Ok(())
    }

    fn lock_queue_operations(&self) -> Result<QueueOperationLock> {
        self.ensure_private_directory()?;
        let entry = self.root.bind_entry(&self.operation_lock_path, false)?;
        let file = self
            .root
            .open_bound_file(&self.operation_lock_path)?
            .ok_or_else(|| anyhow!("task queue operation lock file is missing"))?;
        let identity = file.identity();
        file.validate_private_single_link(0o600)?;
        file.lock_exclusive()?;
        if self.root.bound_entry_identity(&entry)? != Some(identity) {
            bail!("task queue claim lock file was replaced while locking");
        }
        file.validate_private_single_link(0o600)?;
        Ok(QueueOperationLock { _file: file })
    }

    fn normalize_media_path(&self, path: &Path) -> Result<PathBuf> {
        let relative = self
            .root_aliases
            .iter()
            .find_map(|root| path.strip_prefix(root).ok())
            .or_else(|| path.strip_prefix(self.root.root_path()).ok())
            .ok_or_else(|| {
                anyhow!("published media path is outside its configured download root")
            })?;
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            bail!("published media path is not a file within its configured download root");
        }
        Ok(self.root_path.join(relative))
    }

    fn ensure_private_directory(&self) -> Result<()> {
        let entry = self.root.bind_entry(&self.queue_dir, false)?;
        self.root
            .validate_private_bound_directory(&entry, self.queue_identity, 0o700)
    }

    fn index_path(&self) -> PathBuf {
        self.queue_dir.join(INDEX_FILE)
    }

    fn task_path(&self, id: &str) -> Result<PathBuf> {
        validate_task_id(id)?;
        Ok(self.queue_dir.join(format!("task-{id}.json")))
    }

    fn terminal_history_path(&self, id: &str) -> Result<PathBuf> {
        validate_task_id(id)?;
        Ok(self
            .queue_dir
            .join(TERMINAL_HISTORY_DIRECTORY)
            .join(format!("task-{id}.json")))
    }

    fn ensure_private_history_directory(&self) -> Result<()> {
        self.ensure_private_directory()?;
        let path = self.queue_dir.join(TERMINAL_HISTORY_DIRECTORY);
        let identity = match self.root.create_dir(&path, 0o700)? {
            Some(identity) => identity,
            None => self
                .root
                .entry_identity(&path)?
                .ok_or_else(|| anyhow!("task history directory disappeared"))?,
        };
        let entry = self.root.bind_entry(&path, false)?;
        self.root
            .validate_private_bound_directory(&entry, identity, 0o700)
            .context("task history directory must be owner-private")
    }

    fn read_private_file(&self, path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
        self.ensure_private_directory()?;
        let Some(file) = self.root.open_bound_file(path)? else {
            return Ok(None);
        };
        file.validate_private_single_link(0o600)
            .with_context(|| format!("private task file is unsafe: {}", path.display()))?;
        if file.byte_len()? > limit as u64 {
            bail!(
                "task queue file exceeds its {limit}-byte limit: {}",
                path.display()
            );
        }
        Ok(Some(file.read_limited(limit)?))
    }

    fn write_private_file(&self, path: &Path, contents: &[u8]) -> Result<()> {
        self.ensure_private_directory()?;
        if contents.len() > MAX_RECORD_BYTES.max(MAX_INDEX_BYTES) {
            bail!("task queue record exceeds the configured size limit");
        }
        let temporary = temporary_sibling(path);
        if let Some(file) = self.root.open_bound_file(path)? {
            file.validate_private_single_link(0o600)?;
            let entry = self.root.bind_entry(path, false)?;
            self.root.replace_bound_file_atomically_if_identity(
                &entry,
                file.identity(),
                &temporary,
                contents,
                0o600,
            )?;
        } else {
            let (source, identity) = self
                .root
                .create_new_bound_file(&temporary, contents, 0o600)?;
            let destination = self.root.bind_entry(path, false)?;
            self.root.rename_via_bound_parents_noreplace_if_identity(
                &source,
                &destination,
                identity,
            )?;
        }
        Ok(())
    }

    fn read_index(&self) -> Result<StoreIndex> {
        let bytes = self
            .read_private_file(&self.index_path(), MAX_INDEX_BYTES)?
            .ok_or_else(|| anyhow!("task queue index is missing"))?;
        let mut index: StoreIndex =
            serde_json::from_slice(&bytes).context("invalid task queue index")?;
        match index.version {
            INDEX_VERSION => {
                for entry in index.tasks.values_mut() {
                    entry.record_path = self.resolve_relative_index_path(&entry.record_path)?;
                    entry.move_target = entry
                        .move_target
                        .as_deref()
                        .map(|path| self.resolve_relative_index_path(path))
                        .transpose()?;
                }
            }
            LEGACY_INDEX_VERSION => {
                for entry in index.tasks.values_mut() {
                    entry.record_path = self.resolve_legacy_index_path(&entry.record_path)?;
                    entry.move_target = entry
                        .move_target
                        .as_deref()
                        .map(|path| self.resolve_legacy_index_path(path))
                        .transpose()?;
                }
                index.version = INDEX_VERSION;
            }
            version => bail!("unsupported task queue index version {version}"),
        }
        Ok(index)
    }

    fn write_index(&self, index: &StoreIndex) -> Result<()> {
        let mut stored_index = index.clone();
        stored_index.version = INDEX_VERSION;
        self.prune_history_index(&mut stored_index);
        let mut bytes = self.encode_index(&stored_index)?;
        while bytes.len() > MAX_INDEX_BYTES {
            let average_entry_bytes = bytes.len() / stored_index.tasks.len().max(1);
            let excess_bytes = bytes.len() - MAX_INDEX_BYTES;
            let remove_count = (excess_bytes / average_entry_bytes.max(1) + 1).max(1);
            let mut removed = 0;
            while removed < remove_count
                && self.remove_oldest_history_index_entry(&mut stored_index)
            {
                removed += 1;
            }
            if removed == 0 {
                bail!("task queue index exceeds its size limit");
            }
            bytes = self.encode_index(&stored_index)?;
        }
        self.write_private_file(&self.index_path(), &bytes)
    }

    fn encode_index(&self, index: &StoreIndex) -> Result<Vec<u8>> {
        let mut encoded_index = index.clone();
        for entry in encoded_index.tasks.values_mut() {
            entry.record_path = self.make_relative_index_path(&entry.record_path)?;
            entry.move_target = entry
                .move_target
                .as_deref()
                .map(|path| self.make_relative_index_path(path))
                .transpose()?;
        }
        serde_json::to_vec(&encoded_index).context("failed to encode task queue index")
    }

    fn prune_history_index(&self, index: &mut StoreIndex) {
        let mut history = index
            .tasks
            .iter()
            .filter(|(_, entry)| self.is_history_index_entry(entry))
            .map(|(id, entry)| (entry.updated_at, id.clone()))
            .collect::<Vec<_>>();
        history.sort();
        let excess = history.len().saturating_sub(MAX_HISTORY_INDEX_ENTRIES);
        for (_, id) in history.into_iter().take(excess) {
            index.tasks.remove(&id);
        }
    }

    fn remove_oldest_history_index_entry(&self, index: &mut StoreIndex) -> bool {
        let oldest = index
            .tasks
            .iter()
            .filter(|(_, entry)| self.is_history_index_entry(entry))
            .min_by(|(left_id, left), (right_id, right)| {
                (left.updated_at, *left_id).cmp(&(right.updated_at, *right_id))
            })
            .map(|(id, _)| id.clone());
        oldest.is_some_and(|id| index.tasks.remove(&id).is_some())
    }

    fn is_history_index_entry(&self, entry: &TaskIndexEntry) -> bool {
        entry.move_target.is_none() && entry.record_path.parent() != Some(self.queue_dir.as_path())
    }

    fn resolve_relative_index_path(&self, path: &Path) -> Result<PathBuf> {
        validate_root_relative_index_path(path)?;
        Ok(self.root_path.join(path))
    }

    fn resolve_legacy_index_path(&self, path: &Path) -> Result<PathBuf> {
        if !path.is_absolute() {
            bail!("legacy task queue index path is not absolute");
        }
        let relative = if let Ok(relative) = path.strip_prefix(&self.root_path) {
            relative.to_path_buf()
        } else {
            let parent = path
                .parent()
                .ok_or_else(|| anyhow!("legacy task queue index path has no parent"))?;
            // Only migrate aliases whose parent resolves inside the root bound by RootedFs.
            self.root.validate_configured_root()?;
            let canonical_parent = std::fs::canonicalize(parent).with_context(|| {
                format!("failed to resolve legacy queue path {}", path.display())
            })?;
            self.root.validate_configured_root()?;
            let relative_parent = canonical_parent
                .strip_prefix(self.root.root_path())
                .with_context(|| {
                    format!(
                        "legacy queue path is outside the configured download root: {}",
                        path.display()
                    )
                })?;
            relative_parent.join(
                path.file_name()
                    .ok_or_else(|| anyhow!("legacy task queue index path has no filename"))?,
            )
        };
        validate_root_relative_index_path(&relative)?;
        Ok(self.root_path.join(relative))
    }

    fn make_relative_index_path(&self, path: &Path) -> Result<PathBuf> {
        let relative = path.strip_prefix(&self.root_path).with_context(|| {
            format!(
                "task queue index path is outside the configured download root: {}",
                path.display()
            )
        })?;
        validate_root_relative_index_path(relative)?;
        Ok(relative.to_path_buf())
    }

    fn read_record(&self, path: &Path) -> Result<Option<TaskRecord>> {
        let Some(bytes) = self.read_private_file(path, MAX_RECORD_BYTES)? else {
            return Ok(None);
        };
        let record: TaskRecord = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid task queue record {}", path.display()))?;
        if record.schema_version != TASK_RECORD_VERSION {
            bail!("unsupported task record version for task {}", record.id);
        }
        Ok(Some(record))
    }

    fn write_record(&self, path: &Path, record: &TaskRecord) -> Result<()> {
        let bytes =
            serde_json::to_vec(record).context("failed to encode persistent task record")?;
        if bytes.len() > MAX_RECORD_BYTES {
            bail!("persistent task record exceeds its size limit");
        }
        self.write_private_file(path, &bytes)
    }

    fn create_task(&self, task: &mut TaskRecord) -> Result<()> {
        let mut index = self.read_index()?;
        if index.tasks.contains_key(&task.id) {
            return Ok(());
        }
        task.activity_revision = bump_chat_activity(&mut index, task.chat_id);
        let path = self.task_path(&task.id)?;
        self.write_record(&path, task)?;
        index.tasks.insert(
            task.id.clone(),
            TaskIndexEntry {
                chat_id: task.chat_id,
                record_path: path,
                move_target: None,
                revision: task.revision,
                updated_at: task.updated_at,
            },
        );
        self.write_index(&index)
    }

    fn list_records(&self) -> Result<Vec<TaskRecord>> {
        let mut index = self.read_index()?;
        self.scan_active_records(&mut index)?;
        self.reconcile_move_targets(&mut index)?;
        self.write_index(&index)?;
        let mut records = Vec::with_capacity(index.tasks.len());
        for (id, entry) in &index.tasks {
            if let Some(record) = self.read_record(&entry.record_path)? {
                if record.id != *id || record.chat_id != entry.chat_id {
                    bail!("task index points to a mismatched record for task {id}");
                }
                records.push(record);
            }
        }
        Ok(records)
    }

    fn get_task(&self, id: &str) -> Result<Option<(TaskRecord, TaskIndexEntry)>> {
        let mut index = self.read_index()?;
        self.scan_active_records(&mut index)?;
        self.reconcile_move_targets(&mut index)?;
        if !index.tasks.contains_key(id) {
            self.write_index(&index)?;
            return Ok(None);
        }
        self.write_index(&index)?;
        let entry = index.tasks.get(id).expect("checked above").clone();
        let Some(record) = self.read_record(&entry.record_path)? else {
            return Ok(None);
        };
        if record.id != id || record.chat_id != entry.chat_id {
            bail!("task index points to a mismatched record for task {id}");
        }
        Ok(Some((record, entry)))
    }

    fn save_mutated_record(
        &self,
        mut record: TaskRecord,
        mut entry: TaskIndexEntry,
        activity: bool,
    ) -> Result<TaskRecord> {
        record.updated_at = unix_time();
        record.revision = record.revision.saturating_add(1);
        let mut index = self.read_index()?;
        if activity {
            record.activity_revision = bump_chat_activity(&mut index, record.chat_id);
        }
        self.write_record(&entry.record_path, &record)?;
        entry.revision = record.revision;
        entry.updated_at = record.updated_at;
        if matches!(record.status, TaskStatus::Cancelled | TaskStatus::Completed)
            && entry.record_path.parent() == Some(self.queue_dir.as_path())
        {
            let destination = self.terminal_history_path(&record.id)?;
            self.ensure_private_history_directory()?;
            entry.move_target = Some(destination.clone());
            index.tasks.insert(record.id.clone(), entry.clone());
            self.write_index(&index)?;
            self.move_record_to_sidecar(&record.id, &entry.record_path, &destination)?;
            entry.record_path = destination;
            entry.move_target = None;
        }
        index.tasks.insert(record.id.clone(), entry);
        self.write_index(&index)?;
        Ok(record)
    }

    fn save_index_task(&self, id: &str, entry: TaskIndexEntry) -> Result<()> {
        let mut index = self.read_index()?;
        index.tasks.insert(id.to_string(), entry);
        self.write_index(&index)
    }

    fn mark_notified(&self, chat_id: i64) -> Result<()> {
        let mut index = self.read_index()?;
        if let Some(activity) = index.chat_activity.get_mut(&chat_id.to_string()) {
            activity.notified_revision = activity.revision;
            self.write_index(&index)?;
        }
        Ok(())
    }

    fn restart_summary_data(&self) -> Result<(Vec<TaskRecord>, BTreeMap<i64, ChatActivity>)> {
        let records = self.list_records()?;
        let index = self.read_index()?;
        let activities = index
            .chat_activity
            .into_iter()
            .filter_map(|(chat, activity)| chat.parse().ok().map(|chat| (chat, activity)))
            .collect();
        Ok((records, activities))
    }

    fn reconcile_index(&self) -> Result<()> {
        let mut index = self.read_index()?;
        self.scan_active_records(&mut index)?;
        self.reconcile_move_targets(&mut index)?;
        self.write_index(&index)
    }

    fn scan_active_records(&self, index: &mut StoreIndex) -> Result<()> {
        self.ensure_private_directory()?;
        let queue_entry = self.root.bind_entry(&self.queue_dir, false)?;
        let entries = self
            .root
            .list_bound_directory(&queue_entry, self.queue_identity)?;
        if entries.len() > MAX_ACTIVE_RECORDS {
            bail!("task queue contains more than {MAX_ACTIVE_RECORDS} entries");
        }
        for (name, identity) in entries {
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(id) = name
                .strip_prefix("task-")
                .and_then(|name| name.strip_suffix(".json"))
            else {
                continue;
            };
            validate_task_id(id)?;
            if !identity.is_file() || index.tasks.contains_key(id) {
                continue;
            }
            let path = self.task_path(id)?;
            if let Some(record) = self.read_record(&path)? {
                if record.id != id {
                    bail!(
                        "orphan task filename and record ID disagree: {}",
                        path.display()
                    );
                }
                index.tasks.insert(
                    id.to_string(),
                    TaskIndexEntry {
                        chat_id: record.chat_id,
                        record_path: path,
                        move_target: None,
                        revision: record.revision,
                        updated_at: record.updated_at,
                    },
                );
                let activity = index
                    .chat_activity
                    .entry(record.chat_id.to_string())
                    .or_default();
                activity.revision = activity.revision.max(record.activity_revision);
            }
        }
        Ok(())
    }

    fn reconcile_move_targets(&self, index: &mut StoreIndex) -> Result<()> {
        let mut changed = false;
        for (id, entry) in &mut index.tasks {
            let Some(target) = entry.move_target.clone() else {
                continue;
            };
            let source_exists = self.root.entry_exists(&entry.record_path)?;
            let target_exists = self.root.entry_exists(&target)?;
            if source_exists {
                let Some(source_record) = self.read_record(&entry.record_path)? else {
                    continue;
                };
                if source_record.id != *id {
                    bail!(
                        "sidecar recovery found a mismatched task ID at {}",
                        entry.record_path.display()
                    );
                }
            }
            if source_exists && !target_exists {
                self.move_record_to_sidecar(id, &entry.record_path, &target)?;
            }
            if !source_exists && !target_exists {
                continue;
            }
            let Some(record) = self.read_record(&target)? else {
                continue;
            };
            if record.id != *id {
                bail!(
                    "sidecar recovery found a mismatched task ID at {}",
                    target.display()
                );
            }
            if source_exists && target_exists {
                self.move_record_to_sidecar(id, &entry.record_path, &target)?;
            }
            entry.record_path = target;
            entry.move_target = None;
            changed = true;
        }
        if changed {
            self.write_index(index)?;
        }
        Ok(())
    }

    fn move_record_to_sidecar(
        &self,
        id: &str,
        source_path: &Path,
        target_path: &Path,
    ) -> Result<()> {
        self.ensure_private_directory()?;
        let Some(file) = self.root.open_bound_file(source_path)? else {
            if let Some(record) = self.read_record(target_path)?
                && record.id == id
            {
                return Ok(());
            }
            bail!("task record disappeared before sidecar migration");
        };
        file.validate_private_single_link(0o600)?;
        if self.root.entry_exists(target_path)? {
            let Some(existing) = self.read_record(target_path)? else {
                bail!("task sidecar target exists but is unreadable");
            };
            if existing.id != id {
                bail!("refusing to overwrite an unrelated task sidecar");
            }
            let source = self.root.bind_entry(source_path, false)?;
            self.root
                .remove_bound_file_if_identity(&source, file.identity())?;
            return Ok(());
        }
        let source = self.root.bind_entry(source_path, false)?;
        let target = self.root.bind_entry(target_path, false)?;
        self.root
            .rename_via_bound_parents_noreplace_if_identity(&source, &target, file.identity())
    }
}

fn bump_chat_activity(index: &mut StoreIndex, chat_id: i64) -> u64 {
    let activity = index.chat_activity.entry(chat_id.to_string()).or_default();
    activity.revision = activity.revision.saturating_add(1);
    activity.revision
}

fn sidecar_destination(
    root: &Path,
    record: &TaskRecord,
    media_paths: &[PathBuf],
) -> Result<Option<PathBuf>> {
    let mut parents = media_paths
        .iter()
        .filter_map(|path| path.parent().map(Path::to_path_buf))
        .collect::<Vec<_>>();
    if parents.is_empty()
        && let Some(location) = &record.saved_location
    {
        let path = Path::new(location);
        if path.is_dir() {
            parents.push(path.to_path_buf());
        } else if let Some(parent) = path.parent() {
            parents.push(parent.to_path_buf());
        }
    }
    let Some(mut parent) = parents.first().cloned() else {
        return Ok(None);
    };
    while !parents
        .iter()
        .all(|candidate| candidate.starts_with(&parent))
    {
        if !parent.pop() {
            return Ok(None);
        }
    }
    if !parent.starts_with(root) {
        bail!("published media path is outside its configured download root");
    }
    let relative_parent = parent
        .strip_prefix(root)
        .context("failed to make task sidecar path relative to download root")?;
    Ok(Some(root.join(relative_parent).join(format!(
        ".telegram-video-downloader-task-{}.json",
        record.id
    ))))
}

// Protected property: hash the same regular-file object that remains published at the selected
// path. No-follow open plus device/inode checks protect object identity, while size brackets the
// read. If timestamps move, a second digest distinguishes a benign metadata touch from changed
// content; failed reads and missing paths remain errors rather than being treated as mismatches.
fn sha256_file(
    root: &RootedFs,
    path: &Path,
    mut file: std::fs::File,
    identity: EntryIdentity,
) -> Result<String> {
    #[cfg(unix)]
    let (before_size, before_timestamps) = {
        use std::os::unix::fs::MetadataExt;
        let before = file.metadata()?;
        if !before.is_file()
            || before.uid() != unsafe { libc::geteuid() }
            || before.dev() != identity.device()
            || before.ino() != identity.inode()
        {
            bail!(
                "published media is not the selected regular file owned by the current user: {}",
                path.display()
            );
        }
        root.validate_configured_root()?;
        if root.entry_identity(path)? != Some(identity) {
            bail!(
                "published media path was replaced before hashing: {}",
                path.display()
            );
        }
        if before.len() == 0 {
            bail!("published media is empty: {}", path.display());
        }
        (
            before.len(),
            (
                before.mtime(),
                before.mtime_nsec(),
                before.ctime(),
                before.ctime_nsec(),
            ),
        )
    };
    #[cfg(not(unix))]
    let before_size = {
        let before = file.metadata()?;
        if !before.is_file() || !identity.is_file() {
            bail!(
                "published media is not the selected regular file: {}",
                path.display()
            );
        }
        root.validate_configured_root()?;
        if root.entry_identity(path)? != Some(identity) {
            bail!(
                "published media path was replaced before hashing: {}",
                path.display()
            );
        }
        if before.len() == 0 {
            bail!("published media is empty: {}", path.display());
        }
        before.len()
    };

    let first_digest = hash_open_file(&mut file)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let after = file.metadata()?;
        root.validate_configured_root()?;
        if (after.dev(), after.ino(), after.len())
            != (identity.device(), identity.inode(), before_size)
            || root.entry_identity(path)? != Some(identity)
        {
            bail!(
                "published media changed identity or size while hashing: {}",
                path.display()
            );
        }
        let timestamps_changed = (
            after.mtime(),
            after.mtime_nsec(),
            after.ctime(),
            after.ctime_nsec(),
        ) != before_timestamps;
        if timestamps_changed {
            file.seek(SeekFrom::Start(0))?;
            let second_digest = hash_open_file(&mut file)?;
            let verified = file.metadata()?;
            root.validate_configured_root()?;
            if second_digest != first_digest
                || (verified.dev(), verified.ino(), verified.len())
                    != (identity.device(), identity.inode(), before_size)
                || root.entry_identity(path)? != Some(identity)
            {
                bail!(
                    "published media content changed while hashing: {}",
                    path.display()
                );
            }
        }
    }
    #[cfg(not(unix))]
    {
        root.validate_configured_root()?;
        if file.metadata()?.len() != before_size || root.entry_identity(path)? != Some(identity) {
            bail!(
                "published media changed size or identity while hashing: {}",
                path.display()
            );
        }
    }
    Ok(first_digest)
}

fn hash_open_file(file: &mut std::fs::File) -> Result<String> {
    file.seek(SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn temporary_sibling(path: &Path) -> PathBuf {
    let serial = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    path.with_file_name(format!(".{name}.tmp-{}-{serial}", std::process::id()))
}

fn persist_media_hashes(
    hashes: BTreeMap<String, String>,
) -> Result<(BTreeMap<String, String>, Option<PrimaryMediaHashManifest>)> {
    if hashes.len() <= MAX_PERSISTED_MEDIA_HASHES
        && serde_json::to_vec(&hashes)?.len() <= MAX_PERSISTED_MEDIA_HASH_BYTES
    {
        return Ok((hashes, None));
    }

    let file_count = hashes.len();
    let mut hasher = Sha256::new();
    hasher.update(b"telegram-video-downloader-primary-media-manifest-v1\0");
    for (path, digest) in &hashes {
        let path_bytes = path.as_bytes();
        let digest_bytes = digest.as_bytes();
        hasher.update((path_bytes.len() as u64).to_be_bytes());
        hasher.update(path_bytes);
        hasher.update((digest_bytes.len() as u64).to_be_bytes());
        hasher.update(digest_bytes);
    }
    let sha256 = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok((
        BTreeMap::new(),
        Some(PrimaryMediaHashManifest { file_count, sha256 }),
    ))
}

fn validate_task_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        bail!("invalid persistent task ID");
    }
    Ok(())
}

fn validate_root_relative_index_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("task queue index path must be root-relative");
    }
    Ok(())
}

pub fn sanitize_job_for_storage(job: JobRequest) -> (JobRequest, bool) {
    match job {
        JobRequest::Bilibili { url, selection } => {
            let (url, changed) = sanitize_url(&url);
            (JobRequest::Bilibili { url, selection }, changed)
        }
        JobRequest::Youtube { url } => {
            let (url, changed) = sanitize_url(&url);
            (JobRequest::Youtube { url }, changed)
        }
        JobRequest::Pdf { url } => {
            let (url, changed) = sanitize_url(&url);
            (JobRequest::Pdf { url }, changed)
        }
    }
}

fn sanitize_url(raw: &str) -> (String, bool) {
    let Ok(mut url) = url::Url::parse(raw) else {
        return (raw.to_string(), false);
    };
    let mut changed = false;
    if !url.username().is_empty() {
        let _ = url.set_username("");
        changed = true;
    }
    if url.password().is_some() {
        let _ = url.set_password(None);
        changed = true;
    }
    let query = url
        .query_pairs()
        .filter(|(key, _)| {
            let key = key.to_ascii_lowercase();
            let sensitive = [
                "token",
                "access_key",
                "accesskey",
                "refresh",
                "auth",
                "cookie",
                "session",
                "password",
                "passwd",
                "signature",
                "credential",
                "secret",
            ]
            .iter()
            .any(|needle| key.contains(needle));
            changed |= sensitive;
            !sensitive
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    if changed {
        url.set_query(None);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
    }
    (url.to_string(), changed)
}

fn job_url(job: &JobRequest) -> &str {
    match job {
        JobRequest::Bilibili { url, .. }
        | JobRequest::Youtube { url }
        | JobRequest::Pdf { url } => url,
    }
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn poisoned_lock<T>(_: std::sync::PoisonError<T>) -> anyhow::Error {
    anyhow!("persistent task queue lock was poisoned")
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_queue_root(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("telegram-video-downloader-{label}-{unique}"))
    }

    fn test_task(id: &str, job: JobRequest) -> TaskRecord {
        TaskRecord::new(id.to_string(), 1, 2, 123_456_789, Some(3), 0, job)
    }

    fn spawn_queue_operation_child(
        root: &Path,
        operation: &str,
        slot: &str,
    ) -> std::process::Child {
        std::process::Command::new(std::env::current_exe().expect("test binary should resolve"))
            .arg("--ignored")
            .arg("--exact")
            .arg("queue::tests::cross_process_queue_operation_child")
            .arg("--nocapture")
            .env("TVD_QUEUE_OPERATION_CHILD_ROOT", root)
            .env("TVD_QUEUE_OPERATION_CHILD_MODE", operation)
            .env("TVD_QUEUE_OPERATION_CHILD_SLOT", slot)
            .spawn()
            .expect("queue operation child process should start")
    }

    fn wait_for_child_marker(child: &mut std::process::Child, path: &Path, label: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !path.exists() {
            if let Some(status) = child.try_wait().expect("child status should read") {
                panic!("{label} exited before writing its ready marker: {status}");
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("{label} did not reach its queue operation");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn aliased_video_and_pdf_roots_share_records_and_normalize_canonical_outputs() {
        let temp_root = temp_queue_root("aliased-queue-roots");
        let physical_root = temp_root.join("physical");
        let video_alias = temp_root.join("video");
        let pdf_alias = temp_root.join("pdf");
        fs::create_dir_all(&physical_root).expect("physical download root should create");
        symlink(&physical_root, &video_alias).expect("video root alias should create");
        symlink(&physical_root, &pdf_alias).expect("PDF root alias should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_alias;
        config.downloads.pdf_dir = pdf_alias;
        let queue = QueueManager::open(&config).expect("aliased queue roots should open");

        let tasks = [
            (
                "task-pdf-alias-1",
                "pdf-output.pdf",
                JobRequest::Pdf {
                    url: "https://example.invalid/document.pdf".to_string(),
                },
            ),
            (
                "task-bilibili-alias-1",
                "collection-output.mp4",
                JobRequest::Bilibili {
                    url: "https://www.bilibili.com/video/BV1234567890".to_string(),
                    selection: None,
                },
            ),
        ];
        for (id, filename, job) in tasks {
            assert!(
                queue
                    .create(test_task(id, job))
                    .expect("task should persist")
            );
            queue
                .set_status(id, TaskStatus::Running, None)
                .expect("task should start");
            queue
                .begin_verification(id)
                .expect("verification should begin")
                .expect("task should still exist");

            let media_path = physical_root.join(filename);
            fs::write(&media_path, b"verified media")
                .expect("published media fixture should write");
            let canonical_media_path =
                fs::canonicalize(&media_path).expect("published media path should canonicalize");
            queue
                .complete(
                    id,
                    canonical_media_path.display().to_string(),
                    std::slice::from_ref(&canonical_media_path),
                    BTreeMap::new(),
                )
                .expect("PDF and collection outputs should complete from canonical paths");
        }

        assert!(
            queue
                .list(123_456_789, false, 0)
                .expect("active queue should load")
                .is_empty()
        );
        let history = queue
            .list(123_456_789, true, 0)
            .expect("history should load from the shared store");
        assert_eq!(
            history.len(),
            2,
            "shared queue records must not be duplicated"
        );
        assert!(
            history
                .iter()
                .all(|record| record.status == TaskStatus::Completed)
        );
        assert_eq!(
            queue
                .page_count(123_456_789, true)
                .expect("page count should load"),
            1
        );

        drop(queue);
        let reopened = QueueManager::open(&config).expect("aliased queue should reopen");
        assert_eq!(
            reopened
                .list(123_456_789, true, 0)
                .expect("reopened history should load")
                .len(),
            2
        );
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn legacy_absolute_record_path_migrates_across_root_alias_restart() {
        let temp_root = temp_queue_root("legacy-index-alias-migration");
        let video_root = temp_root.join("videos");
        let video_alias = temp_root.join("video-alias");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        symlink(&video_root, &video_alias).expect("video root alias should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root.clone();
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-legacy-index-alias-1";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Running, None)
            .expect("task should start");
        queue
            .begin_verification(id)
            .expect("verification should begin")
            .expect("task should still exist");

        let media_dir = video_root.join("completed");
        fs::create_dir_all(&media_dir).expect("media directory should create");
        let media_path = media_dir.join("output.mp4");
        fs::write(&media_path, b"verified media").expect("media fixture should write");
        queue
            .complete(
                id,
                media_path.display().to_string(),
                std::slice::from_ref(&media_path),
                BTreeMap::new(),
            )
            .expect("task should complete");

        let index_path = video_root.join(QUEUE_DIRECTORY).join(INDEX_FILE);
        let mut legacy_index: serde_json::Value = serde_json::from_slice(
            &fs::read(&index_path).expect("task queue index should be readable"),
        )
        .expect("task queue index should be valid JSON");
        legacy_index["version"] = serde_json::json!(LEGACY_INDEX_VERSION);
        legacy_index["tasks"][id]["record_path"] = serde_json::json!(
            media_dir
                .join(format!(".telegram-video-downloader-task-{id}.json"))
                .display()
                .to_string()
        );
        fs::write(
            &index_path,
            serde_json::to_vec(&legacy_index).expect("legacy index should encode"),
        )
        .expect("legacy queue index should write");
        drop(queue);

        config.downloads.video_dir = video_alias.clone();
        let reopened = QueueManager::open(&config)
            .expect("legacy index should migrate through the configured root alias");
        let recovered = reopened
            .get(id)
            .expect("migrated task should load")
            .expect("migrated task should exist");
        assert_eq!(recovered.status, TaskStatus::Completed);

        let migrated_index: serde_json::Value = serde_json::from_slice(
            &fs::read(video_alias.join(QUEUE_DIRECTORY).join(INDEX_FILE))
                .expect("migrated queue index should be readable"),
        )
        .expect("migrated queue index should be valid JSON");
        assert_eq!(migrated_index["version"], INDEX_VERSION);
        let stored_path = migrated_index["tasks"][id]["record_path"]
            .as_str()
            .expect("record path should be serialized as a string");
        assert!(!Path::new(stored_path).is_absolute());
        assert_eq!(
            stored_path,
            "completed/.telegram-video-downloader-task-task-legacy-index-alias-1.json"
        );

        drop(reopened);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn concurrent_resume_claims_across_processes_start_only_once() {
        use std::sync::mpsc::{TryRecvError, sync_channel};

        let temp_root = temp_queue_root("concurrent-resume-claims");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let initial = QueueManager::open(&config).expect("task queue should open");
        let id = "task-concurrent-resume-1";
        assert!(
            initial
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        initial
            .set_status(id, TaskStatus::Interrupted, None)
            .expect("task should be interrupted");
        drop(initial);

        let first = Arc::new(QueueManager::open(&config).expect("first manager should open"));
        let second = Arc::new(QueueManager::open(&config).expect("second manager should open"));
        let mut child = spawn_queue_operation_child(&temp_root, "claim", "child");
        wait_for_child_marker(
            &mut child,
            &temp_root.join("claim-child-ready"),
            "claim child",
        );
        let held_operation_lock = first
            .video
            .lock_queue_operations()
            .expect("parent process should lock queue operations");
        fs::write(temp_root.join("claim-start"), b"start")
            .expect("claim start marker should write");

        let (claim_tx, claim_rx) = sync_channel(1);
        let second_manager = Arc::clone(&second);
        let second_claim = std::thread::spawn(move || {
            let result = second_manager
                .claim_resume(id, false)
                .expect("second manager claim should complete");
            claim_tx
                .send(result.is_some())
                .expect("claim result receiver should remain available");
        });
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            child
                .try_wait()
                .expect("claim child status should read")
                .is_none(),
            "a separate process must wait for the queue operation lock"
        );
        assert!(
            matches!(claim_rx.try_recv(), Err(TryRecvError::Empty)),
            "a second manager must wait for the queue operation lock"
        );

        drop(held_operation_lock);
        let output = child
            .wait_with_output()
            .expect("claim child output should collect");
        assert!(
            output.status.success(),
            "claim child failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let child_claimed = fs::read(temp_root.join("claim-child-result"))
            .expect("claim child should persist its result")
            == b"claimed";
        let manager_claimed = claim_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("second manager claim should finish");
        second_claim
            .join()
            .expect("second manager claim thread should finish");
        assert_eq!(
            usize::from(child_claimed) + usize::from(manager_claimed),
            1,
            "only one process may claim the interrupted task"
        );
        assert_eq!(
            first
                .get(id)
                .expect("claimed task should load")
                .expect("claimed task should remain")
                .status,
            TaskStatus::Preparing
        );

        drop(first);
        drop(second);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn concurrent_task_creates_across_processes_start_only_once() {
        let temp_root = temp_queue_root("concurrent-task-creates");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root.clone();
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let mut first = spawn_queue_operation_child(&temp_root, "create", "first");
        let mut second = spawn_queue_operation_child(&temp_root, "create", "second");
        wait_for_child_marker(
            &mut first,
            &temp_root.join("create-first-ready"),
            "first create child",
        );
        wait_for_child_marker(
            &mut second,
            &temp_root.join("create-second-ready"),
            "second create child",
        );

        let held_operation_lock = queue
            .video
            .lock_queue_operations()
            .expect("parent process should lock queue operations");
        fs::write(temp_root.join("create-start"), b"start")
            .expect("create start marker should write");
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            first
                .try_wait()
                .expect("first create child status should read")
                .is_none()
                && second
                    .try_wait()
                    .expect("second create child status should read")
                    .is_none(),
            "both task creators must wait for the queue operation lock"
        );
        assert!(
            !temp_root.join("create-first-result").exists()
                && !temp_root.join("create-second-result").exists(),
            "task create results must not be written while another process owns the lock"
        );

        drop(held_operation_lock);
        for (slot, child) in [("first", first), ("second", second)] {
            let output = child
                .wait_with_output()
                .expect("create child output should collect");
            assert!(
                output.status.success(),
                "{slot} create child failed:\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let created = ["first", "second"]
            .iter()
            .filter(|slot| {
                fs::read(temp_root.join(format!("create-{slot}-result")))
                    .expect("create child result should exist")
                    == b"created"
            })
            .count();
        assert_eq!(created, 1, "only one process may create a task ID");
        assert!(
            queue
                .get("task-concurrent-create-1")
                .expect("created task should load")
                .is_some(),
            "the single winning create should persist a task record"
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    #[ignore = "spawned by cross-process queue operation tests"]
    fn cross_process_queue_operation_child() {
        use std::time::Instant;

        let root = PathBuf::from(
            std::env::var_os("TVD_QUEUE_OPERATION_CHILD_ROOT")
                .expect("queue operation child root must be provided by the parent test"),
        );
        let operation = std::env::var("TVD_QUEUE_OPERATION_CHILD_MODE")
            .expect("queue operation child mode must be provided");
        let slot = std::env::var("TVD_QUEUE_OPERATION_CHILD_SLOT")
            .expect("queue operation child slot must be provided");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = root.join("videos");
        config.downloads.pdf_dir = root.join("pdfs");
        let queue = QueueManager::open(&config).expect("child queue should open");
        fs::write(root.join(format!("{operation}-{slot}-ready")), b"ready")
            .expect("queue operation child ready marker should write");
        let start_path = root.join(format!("{operation}-start"));
        let start_deadline = Instant::now() + Duration::from_secs(5);
        while !start_path.exists() {
            assert!(
                Instant::now() < start_deadline,
                "parent should release the {operation} child barrier"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        let result = match operation.as_str() {
            "claim" => queue
                .claim_resume("task-concurrent-resume-1", false)
                .expect("child claim should complete")
                .is_some(),
            "create" => queue
                .create(test_task(
                    "task-concurrent-create-1",
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                    },
                ))
                .expect("child create should complete"),
            _ => panic!("unsupported queue operation child mode {operation}"),
        };
        fs::write(
            root.join(format!("{operation}-{slot}-result")),
            if operation == "create" {
                if result {
                    &b"created"[..]
                } else {
                    &b"existing"[..]
                }
            } else if result {
                &b"claimed"[..]
            } else {
                &b"not-claimed"[..]
            },
        )
        .expect("queue operation child result should write");
    }

    #[test]
    fn completed_history_index_keeps_recent_entries_without_deleting_sidecars() {
        let temp_root = temp_queue_root("bounded-completed-history-index");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let published_root = video_root.join("published");
        fs::create_dir_all(&published_root).expect("published directory should create");
        let oldest_sidecar =
            published_root.join(".telegram-video-downloader-task-completed-00000.json");
        fs::write(&oldest_sidecar, b"retained historical task record")
            .expect("oldest sidecar should write");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root.clone();
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let mut index = StoreIndex {
            version: INDEX_VERSION,
            ..StoreIndex::default()
        };
        for number in 0..(MAX_HISTORY_INDEX_ENTRIES + 3) {
            let id = format!("completed-{number:05}");
            let record_path = if number == 0 {
                oldest_sidecar.clone()
            } else {
                published_root.join(format!(".telegram-video-downloader-task-{id}.json"))
            };
            index.tasks.insert(
                id,
                TaskIndexEntry {
                    chat_id: 123_456_789,
                    record_path,
                    move_target: None,
                    revision: 1,
                    updated_at: number as u64,
                },
            );
        }
        let active_id = "task-old-but-active";
        index.tasks.insert(
            active_id.to_string(),
            TaskIndexEntry {
                chat_id: 123_456_789,
                record_path: queue
                    .video
                    .task_path(active_id)
                    .expect("active task path should resolve"),
                move_target: None,
                revision: 1,
                updated_at: 0,
            },
        );

        queue
            .video
            .write_index(&index)
            .expect("history index should remain bounded");
        let stored = queue
            .video
            .read_index()
            .expect("bounded task queue index should read");
        assert_eq!(stored.tasks.len(), MAX_HISTORY_INDEX_ENTRIES + 1);
        assert!(!stored.tasks.contains_key("completed-00000"));
        assert!(!stored.tasks.contains_key("completed-00001"));
        assert!(!stored.tasks.contains_key("completed-00002"));
        assert!(stored.tasks.contains_key("completed-00003"));
        assert!(
            stored
                .tasks
                .contains_key(&format!("completed-{:05}", MAX_HISTORY_INDEX_ENTRIES + 2))
        );
        assert!(stored.tasks.contains_key(active_id));
        assert!(
            oldest_sidecar.is_file(),
            "pruning must retain the sidecar file"
        );
        assert!(
            fs::metadata(video_root.join(QUEUE_DIRECTORY).join(INDEX_FILE))
                .expect("bounded index metadata should read")
                .len()
                <= MAX_INDEX_BYTES as u64
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn cancelled_task_moves_to_private_history_and_survives_restart() {
        let temp_root = temp_queue_root("cancelled-task-private-history");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root.clone();
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-cancelled-private-history";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                    },
                ))
                .expect("task should persist")
        );

        let cancelled = queue
            .cancel(id, 123_456_789)
            .expect("task cancellation should persist")
            .expect("received task should be cancellable");
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        let source_path = queue
            .video
            .task_path(id)
            .expect("active task path should resolve");
        let history_path = queue
            .video
            .terminal_history_path(id)
            .expect("history task path should resolve");
        assert!(
            !source_path.exists(),
            "terminal record should leave active queue"
        );
        assert!(
            history_path.is_file(),
            "terminal record should be retained in history"
        );
        assert_eq!(
            queue
                .video
                .read_index()
                .expect("queue index should read")
                .tasks[id]
                .record_path,
            history_path
        );

        drop(queue);
        let reopened = QueueManager::open(&config).expect("task queue should reopen");
        let recovered = reopened
            .get(id)
            .expect("cancelled task should load")
            .expect("cancelled task should remain indexed");
        assert_eq!(recovered.status, TaskStatus::Cancelled);
        assert!(
            history_path.is_file(),
            "restart recovery must retain history file"
        );

        drop(reopened);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn interrupted_terminal_history_move_recovers_on_restart() {
        let temp_root = temp_queue_root("interrupted-terminal-history-move");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-interrupted-terminal-history-move";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        let (mut record, mut entry) = queue
            .video
            .get_task(id)
            .expect("task should load")
            .expect("task should exist");
        record.status = TaskStatus::Cancelled;
        record.cancel_requested = false;
        record.revision = record.revision.saturating_add(1);
        let destination = queue
            .video
            .terminal_history_path(id)
            .expect("history task path should resolve");
        queue
            .video
            .ensure_private_history_directory()
            .expect("private history directory should exist");
        queue
            .video
            .write_record(&entry.record_path, &record)
            .expect("terminal task record should persist before migration");
        entry.move_target = Some(destination.clone());
        entry.revision = record.revision;
        entry.updated_at = record.updated_at;
        queue
            .video
            .save_index_task(id, entry.clone())
            .expect("move intent should persist before migration");

        drop(queue);
        let reopened = QueueManager::open(&config).expect("interrupted move should recover");
        let recovered = reopened
            .get(id)
            .expect("cancelled task should load")
            .expect("cancelled task should remain indexed");
        assert_eq!(recovered.status, TaskStatus::Cancelled);
        assert!(
            !entry.record_path.exists(),
            "recovery should clear active record"
        );
        assert!(
            destination.is_file(),
            "recovery should finish history migration"
        );
        assert_eq!(
            reopened
                .video
                .read_index()
                .expect("queue index should read")
                .tasks[id]
                .move_target,
            None
        );

        drop(reopened);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn large_primary_media_hashes_are_compacted_before_task_persistence() {
        let temp_root = temp_queue_root("large-primary-media-hashes");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root.clone();
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-large-primary-media-hashes-1";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Running, None)
            .expect("task should start");
        queue
            .begin_verification(id)
            .expect("verification should begin")
            .expect("task should still exist");

        let media_path = video_root.join("output.mp4");
        fs::write(&media_path, b"verified media").expect("media fixture should write");
        let digest = "a".repeat(64);
        let hashes = (0..8_000)
            .map(|index| {
                (
                    format!("collection/item-{index:05}/{}", "x".repeat(192)),
                    digest.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert!(
            serde_json::to_vec(&hashes)
                .expect("large hash map should serialize")
                .len()
                > MAX_RECORD_BYTES,
            "fixture should reproduce a hash map larger than the task record limit"
        );

        let completed = queue
            .complete(
                id,
                media_path.display().to_string(),
                std::slice::from_ref(&media_path),
                hashes,
            )
            .expect("large hash map should not block output completion");
        assert_eq!(completed.status, TaskStatus::Completed);
        assert!(completed.primary_media_hashes.is_empty());
        let manifest = completed
            .primary_media_hash_manifest
            .as_ref()
            .expect("large hash map should be represented by a compact manifest");
        assert_eq!(manifest.file_count, 8_000);
        assert_eq!(manifest.sha256.len(), 64);
        drop(queue);

        let reopened = QueueManager::open(&config).expect("task queue should reopen");
        let recovered = reopened
            .get(id)
            .expect("completed task should load")
            .expect("completed task should remain available");
        assert_eq!(recovered.status, TaskStatus::Completed);
        assert_eq!(recovered.primary_media_hashes.len(), 0);
        assert_eq!(
            recovered.primary_media_hash_manifest,
            completed.primary_media_hash_manifest
        );
        assert!(
            serde_json::to_vec(&recovered)
                .expect("compacted task record should serialize")
                .len()
                <= MAX_RECORD_BYTES
        );

        drop(reopened);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn running_cancel_request_does_not_block_published_output_completion() {
        let temp_root = temp_queue_root("running-cancel-verification");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root.clone();
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-running-cancel-1";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Queued, None)
            .expect("task should queue");
        assert!(queue.begin_run(id).expect("task should begin running"));
        let media_dir = video_root.join("completed");
        fs::create_dir_all(&media_dir).expect("media directory should create");
        let media_path = media_dir.join("output.mp4");
        fs::write(&media_path, b"verified media").expect("media fixture should write");
        let cancel = queue
            .register_cancellation(id)
            .expect("cancellation should register");

        let requested = queue
            .cancel_for_chat(id, 123_456_789)
            .expect("cancellation request should persist")
            .expect("running task should accept cancellation request");
        assert_eq!(requested.status, TaskStatus::Running);
        tokio::time::timeout(Duration::from_secs(1), cancel.notified())
            .await
            .expect("running task should receive cancellation");

        let verifying = queue
            .begin_verification(id)
            .expect("published result should enter verification")
            .expect("running task should still exist");
        assert_eq!(verifying.status, TaskStatus::Verifying);
        let completed = queue
            .complete(
                id,
                media_path.display().to_string(),
                std::slice::from_ref(&media_path),
                BTreeMap::new(),
            )
            .expect("published output should complete despite the late cancellation request");
        assert_eq!(completed.status, TaskStatus::Completed);
        assert!(
            queue
                .finish_cancellation(id)
                .expect("completed task should not be canceled")
                .is_none()
        );

        let _ = queue.unregister_cancellation(id);
        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn completed_task_survives_restart_after_sidecar_move() {
        let temp_root = temp_queue_root("completed-sidecar-recovery");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root.clone();
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-sidecar-recovery-1";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Running, None)
            .expect("task should start");
        queue
            .begin_verification(id)
            .expect("verification should begin")
            .expect("task should still exist");

        let media_dir = video_root.join("completed");
        fs::create_dir_all(&media_dir).expect("media directory should create");
        let media_path = media_dir.join("output.mp4");
        fs::write(&media_path, b"verified media").expect("media fixture should write");
        queue
            .interrupt_after_sidecar_move
            .store(true, Ordering::Relaxed);
        let error = queue
            .complete(
                id,
                media_path.display().to_string(),
                std::slice::from_ref(&media_path),
                BTreeMap::new(),
            )
            .expect_err("the simulated process stop should interrupt completion");
        assert!(
            error
                .to_string()
                .contains("simulated interruption after task sidecar migration")
        );
        drop(queue);

        let reopened = QueueManager::open(&config).expect("queue should recover after restart");
        let recovered = reopened
            .get(id)
            .expect("recovered task should load")
            .expect("recovered task should exist");
        assert_eq!(recovered.status, TaskStatus::Completed);
        assert_eq!(
            reopened
                .list(123_456_789, true, 0)
                .expect("history should load")
                .len(),
            1
        );
        assert!(
            reopened
                .list(123_456_789, false, 0)
                .expect("active queue should load")
                .is_empty()
        );
        let _ = fs::remove_dir_all(temp_root);
    }
}
