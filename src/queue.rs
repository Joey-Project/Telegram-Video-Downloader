use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
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
use crate::safe_fs::{EntryIdentity, RootedFs};

const QUEUE_DIRECTORY: &str = ".telegram-video-downloader-queue";
const INDEX_FILE: &str = "index.json";
const STORE_VERSION: u32 = 1;
const MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_ACTIVE_RECORDS: usize = 20_000;
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
    pub plan: Option<PlanValidationSnapshot>,
    pub proposed_plan: Option<PlanValidationSnapshot>,
    pub saved_location: Option<String>,
    pub primary_media_hashes: BTreeMap<String, String>,
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

#[derive(Debug, Default, Serialize, Deserialize)]
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

struct DownloadStore {
    root_path: PathBuf,
    root: RootedFs,
    queue_dir: PathBuf,
    queue_identity: EntryIdentity,
}

pub struct QueueManager {
    video: DownloadStore,
    pdf: DownloadStore,
    operation_lock: Mutex<()>,
    cancellations: Mutex<HashMap<String, Arc<Notify>>>,
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
            schema_version: STORE_VERSION,
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
            plan: None,
            proposed_plan: None,
            saved_location: None,
            primary_media_hashes: BTreeMap::new(),
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
        let manager = Self {
            video: DownloadStore::new(&config.downloads.video_dir)?,
            pdf: DownloadStore::new(&config.downloads.pdf_dir)?,
            operation_lock: Mutex::new(()),
            cancellations: Mutex::new(HashMap::new()),
        };
        manager.recover_interrupted_tasks()?;
        Ok(manager)
    }

    pub fn create(&self, mut task: TaskRecord) -> Result<bool> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
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
        if self.video.root_path != self.pdf.root_path {
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
        if self.video.root_path != self.pdf.root_path {
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
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        let allowed = if retry_failed {
            record.status == TaskStatus::Failed
        } else {
            matches!(
                record.status,
                TaskStatus::Received
                    | TaskStatus::Preparing
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
        for path in media_paths {
            if !path.starts_with(&store.root_path) {
                bail!("published media path is outside its configured download root");
            }
        }
        record.status = TaskStatus::Verifying;
        record.error = None;
        record.saved_location = Some(saved_location);
        record.primary_media_hashes = hashes;
        if record.media_entries_total <= 1 {
            record.media_entries_total = 1;
            record.media_entries_completed = 1;
        } else {
            record.media_entries_completed = record.media_entries_total;
            record.media_entries_failed = 0;
        }
        let record_path = entry.record_path.clone();
        store.write_record(&record_path, &record)?;
        entry.revision = record.revision;
        entry.updated_at = record.updated_at;

        let destination = sidecar_destination(&store.root_path, &record, media_paths)?;
        let Some(destination) = destination else {
            record.status = TaskStatus::Completed;
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
        entry.record_path = destination;
        entry.move_target = None;
        record.status = TaskStatus::Completed;
        store.save_mutated_record(record, entry, true)
    }

    pub fn fail(&self, id: &str, message: String) -> Result<TaskRecord> {
        self.update(id, true, |record| {
            if matches!(record.status, TaskStatus::Cancelled | TaskStatus::Completed) {
                bail!("task {id} is already terminal");
            }
            record.status = TaskStatus::Failed;
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
        record.status = TaskStatus::Cancelled;
        record.error = None;
        record.user_actions = record.user_actions.saturating_add(1);
        let result = store.save_mutated_record(record, entry, true)?;
        drop(_guard);
        self.notify_cancel(id)?;
        Ok(Some(result))
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
        let data = if self.video.root_path == self.pdf.root_path {
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
        if self.video.root_path != self.pdf.root_path {
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
        let stores = if self.video.root_path == self.pdf.root_path {
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
        let root = RootedFs::new(root_path)?;
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
        let store = Self {
            root_path: root.logical_root_path().to_path_buf(),
            root,
            queue_dir,
            queue_identity,
        };
        if store
            .read_private_file(&store.index_path(), MAX_INDEX_BYTES)?
            .is_none()
        {
            store.write_private_file(
                &store.index_path(),
                &serde_json::to_vec(&StoreIndex {
                    version: STORE_VERSION,
                    ..StoreIndex::default()
                })?,
            )?;
        }
        store.reconcile_index()?;
        Ok(store)
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
        let index: StoreIndex =
            serde_json::from_slice(&bytes).context("invalid task queue index")?;
        if index.version != STORE_VERSION {
            bail!("unsupported task queue index version {}", index.version);
        }
        Ok(index)
    }

    fn write_index(&self, index: &StoreIndex) -> Result<()> {
        let bytes = serde_json::to_vec(index).context("failed to encode task queue index")?;
        if bytes.len() > MAX_INDEX_BYTES {
            bail!("task queue index exceeds its size limit");
        }
        self.write_private_file(&self.index_path(), &bytes)
    }

    fn read_record(&self, path: &Path) -> Result<Option<TaskRecord>> {
        let Some(bytes) = self.read_private_file(path, MAX_RECORD_BYTES)? else {
            return Ok(None);
        };
        let record: TaskRecord = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid task queue record {}", path.display()))?;
        if record.schema_version != STORE_VERSION {
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
            if self.root.entry_exists(&entry.record_path)? {
                continue;
            }
            if let Some(target) = &entry.move_target
                && self.root.entry_exists(target)?
            {
                let Some(record) = self.read_record(target)? else {
                    continue;
                };
                if record.id != *id {
                    bail!(
                        "sidecar recovery found a mismatched task ID at {}",
                        target.display()
                    );
                }
                entry.record_path = target.clone();
                entry.move_target = None;
                changed = true;
            }
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
