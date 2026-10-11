use std::collections::{BTreeMap, BTreeSet, HashMap};
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

use crate::bilibili_attempt::{
    BilibiliAttemptEntryState, BilibiliAttemptOutcome, BilibiliAttemptSnapshot,
    BilibiliAttemptUpdate,
};
use crate::config::AppConfig;
use crate::file_provider::{
    QueueFileProvider, classify_deadlock_error, is_deadlock_error, is_file_provider_access_error,
    platform_queue_file_provider,
};
use crate::router::JobRequest;
use crate::safe_fs::{BoundFile, EntryIdentity, RootedFs};

const QUEUE_DIRECTORY: &str = ".telegram-video-downloader-queue";
const TERMINAL_HISTORY_DIRECTORY: &str = "history";
const INDEX_FILE: &str = "index.json";
const QUEUE_OPERATION_LOCK_FILE: &str = "claims.lock";
const QUEUE_OWNER_LOCK_FILE: &str = "owner.lock";
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SidecarMoveStatus {
    Moved,
    AlreadyAtTarget,
    Missing,
}

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
    PartiallyCompleted,
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
                | Self::PartiallyCompleted
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
pub struct BilibiliGroupEntryIdentity {
    pub index: u32,
    pub title: String,
    pub stable_media_id: String,
}

/// The reviewed collection output directory state. Existing directories are bound by their
/// root-relative path, device, inode, and directory type: these signals protect the selected
/// directory object and its access path across confirmation/restart. Child contents and mutable
/// metadata such as modification time are intentionally excluded so child churn and normal
/// File Provider materialization do not invalidate the reviewed object. `Untracked` is also the
/// serde default for legacy plans that did not persist this identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum BilibiliCollectionDirectorySnapshot {
    #[default]
    Untracked,
    Missing {
        path: String,
    },
    Existing {
        path: String,
        device: u64,
        inode: u64,
        is_directory: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PlanValidationSnapshot {
    pub stable_media_ids: Vec<String>,
    pub selected_format_ids: Vec<String>,
    pub exact_sizes: Vec<PlanSize>,
    pub approximate_sizes: Vec<PlanSize>,
    pub resolution_codecs: Vec<String>,
    pub title: Option<String>,
    #[serde(default)]
    pub media_profile: Option<String>,
    #[serde(default)]
    pub bilibili_group_entries: Vec<BilibiliGroupEntryIdentity>,
    #[serde(default)]
    pub bilibili_collection_directory: BilibiliCollectionDirectorySnapshot,
}

impl PlanValidationSnapshot {
    pub fn blocking_differences(&self, current: &Self) -> Vec<&'static str> {
        let mut differences = Vec::new();
        if self.stable_media_ids != current.stable_media_ids {
            differences.push("media identity");
        }
        let previous_episode_numbers = self
            .bilibili_group_entries
            .iter()
            .map(|entry| (entry.stable_media_id.as_str(), entry.index))
            .collect::<BTreeMap<_, _>>();
        let current_episode_numbers = current
            .bilibili_group_entries
            .iter()
            .map(|entry| (entry.stable_media_id.as_str(), entry.index))
            .collect::<BTreeMap<_, _>>();
        if previous_episode_numbers != current_episode_numbers {
            differences.push("episode numbering");
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
        if self.media_profile != current.media_profile {
            differences.push("download requirements");
        }
        if self.bilibili_collection_directory != current.bilibili_collection_directory {
            differences.push("collection output directory");
        }
        differences
    }

    fn media_requirements_match_for(&self, current: &Self, stable_media_id: &str) -> bool {
        self.media_profile == current.media_profile
            && self
                .selected_format_ids
                .iter()
                .filter(|value| has_media_id_prefix(value, stable_media_id))
                .eq(current
                    .selected_format_ids
                    .iter()
                    .filter(|value| has_media_id_prefix(value, stable_media_id)))
            && self
                .exact_sizes
                .iter()
                .filter(|value| has_media_id_prefix(&value.subject, stable_media_id))
                .eq(current
                    .exact_sizes
                    .iter()
                    .filter(|value| has_media_id_prefix(&value.subject, stable_media_id)))
            && self
                .resolution_codecs
                .iter()
                .filter(|value| has_media_id_prefix(value, stable_media_id))
                .eq(current
                    .resolution_codecs
                    .iter()
                    .filter(|value| has_media_id_prefix(value, stable_media_id)))
    }
}

fn has_media_id_prefix(value: &str, stable_media_id: &str) -> bool {
    value
        .strip_prefix(stable_media_id)
        .is_some_and(|suffix| suffix.starts_with(':'))
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
    // Display-only details never authorize a download or replace plan revalidation.
    #[serde(default)]
    pub display_metadata: Option<PlanValidationSnapshot>,
    pub saved_location: Option<String>,
    #[serde(default)]
    pub primary_media_hashes: BTreeMap<String, String>,
    #[serde(default)]
    pub primary_media_hash_manifest: Option<PrimaryMediaHashManifest>,
    pub staging_attempts: Vec<PathBuf>,
    pub media_entries_total: usize,
    pub media_entries_completed: usize,
    pub media_entries_failed: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bilibili_attempt: Option<BilibiliAttemptSnapshot>,
    pub error: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    pub revision: u64,
    #[serde(default)]
    pub generation: u64,
    pub activity_revision: u64,
    pub user_actions: u64,
}

#[derive(Debug, Clone)]
pub struct RestartSummary {
    pub chat_id: i64,
    pub interrupted_jobs: usize,
    pub partially_completed_jobs: usize,
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
    file_provider: Arc<dyn QueueFileProvider>,
}

struct QueueOperationLock {
    _file: BoundFile,
}

struct QueueOwnerLock {
    _file: BoundFile,
}

pub struct QueueManager {
    video: DownloadStore,
    pdf: DownloadStore,
    _video_owner_lock: QueueOwnerLock,
    _pdf_owner_lock: Option<QueueOwnerLock>,
    operation_lock: Mutex<()>,
    cancellations: Mutex<HashMap<String, CancellationRegistration>>,
    #[cfg(test)]
    interrupt_after_sidecar_move: AtomicBool,
}

struct CancellationRegistration {
    generation: Option<u64>,
    notify: Arc<Notify>,
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
            display_metadata: None,
            saved_location: None,
            primary_media_hashes: BTreeMap::new(),
            primary_media_hash_manifest: None,
            staging_attempts: Vec::new(),
            media_entries_total: 1,
            media_entries_completed: 0,
            media_entries_failed: 0,
            bilibili_attempt: None,
            error: None,
            created_at: now,
            updated_at: now,
            revision: 1,
            generation: 0,
            activity_revision: 0,
            user_actions: 0,
        }
    }
}

fn refresh_bilibili_attempt_counts(record: &mut TaskRecord) {
    let Some(snapshot) = record.bilibili_attempt.as_mut() else {
        return;
    };
    if snapshot.independent_entries {
        let skipped_entries = snapshot
            .entries
            .iter()
            .filter(|entry| entry.already_present)
            .count();
        snapshot.skipped_entries = skipped_entries;
    }
    let total = if snapshot.independent_entries {
        snapshot.total_entries()
    } else {
        snapshot
            .total_entries()
            .saturating_add(snapshot.skipped_entries)
    }
    .max(1);
    let verified = snapshot
        .entries
        .iter()
        .filter(|entry| entry.state == BilibiliAttemptEntryState::Verified)
        .count();
    record.media_entries_total = total;
    // Already-present entries remain complete; downloaded entries count only after verification.
    record.media_entries_completed = if snapshot.independent_entries {
        verified.min(total)
    } else {
        snapshot.skipped_entries.saturating_add(verified).min(total)
    };
    record.media_entries_failed = snapshot.failed_entries().min(total);
}

fn reconcile_accepted_bilibili_attempt(
    record: &mut TaskRecord,
    previous_plan: Option<&PlanValidationSnapshot>,
    accepted_plan: &PlanValidationSnapshot,
) {
    let Some(mut attempt) = record.bilibili_attempt.take() else {
        return;
    };
    if !attempt.independent_entries || accepted_plan.bilibili_group_entries.is_empty() {
        record.media_entries_total = 1;
        record.media_entries_completed = 0;
        record.media_entries_failed = 0;
        return;
    }

    let old_profile_matches_attempt =
        previous_plan.is_some_and(|plan| plan.media_profile == attempt.media_profile);
    let old_plan_contains = |stable_media_id: &str| {
        previous_plan.is_some_and(|plan| {
            plan.stable_media_ids.iter().any(|id| id == stable_media_id)
                || plan
                    .bilibili_group_entries
                    .iter()
                    .any(|entry| entry.stable_media_id == stable_media_id)
        })
    };
    let old_attempt_contains = |stable_media_id: &str| {
        attempt
            .entries
            .iter()
            .any(|entry| entry.stable_media_id == stable_media_id)
    };

    let mut preserved_verified_ids = BTreeSet::new();
    let mut requires_redownload_ids = BTreeSet::new();
    for identity in &accepted_plan.bilibili_group_entries {
        let stable_media_id = identity.stable_media_id.as_str();
        let old_identity_exists =
            old_plan_contains(stable_media_id) || old_attempt_contains(stable_media_id);
        let requirements_match = old_profile_matches_attempt
            && previous_plan.is_some_and(|old_plan| {
                old_plan.media_requirements_match_for(accepted_plan, stable_media_id)
                    && old_plan_contains(stable_media_id)
            });
        let previous_entry = attempt
            .entries
            .iter()
            .find(|entry| entry.stable_media_id == stable_media_id);
        if requirements_match
            && previous_entry.is_some_and(|entry| {
                entry.state == BilibiliAttemptEntryState::Verified && !entry.requires_redownload
            })
        {
            preserved_verified_ids.insert(stable_media_id.to_string());
        }
        if old_identity_exists
            && (!requirements_match
                || previous_entry.is_some_and(|entry| entry.requires_redownload))
        {
            requires_redownload_ids.insert(stable_media_id.to_string());
        }
    }

    let title = accepted_plan
        .title
        .clone()
        .unwrap_or_else(|| attempt.title.clone());
    let entries = accepted_plan
        .bilibili_group_entries
        .iter()
        .map(|entry| {
            (
                entry.index,
                entry.title.clone(),
                entry.stable_media_id.clone(),
            )
        })
        .collect::<Vec<_>>();
    attempt.reconcile_independent_inventory(
        title,
        accepted_plan.media_profile.clone(),
        entries,
        &preserved_verified_ids,
        &requires_redownload_ids,
    );
    record.bilibili_attempt = Some(attempt);
    refresh_bilibili_attempt_counts(record);
}

fn terminal_bilibili_attempt_outcome(status: TaskStatus) -> Option<BilibiliAttemptOutcome> {
    match status {
        TaskStatus::Failed => Some(BilibiliAttemptOutcome::Failed),
        TaskStatus::PartiallyCompleted => Some(BilibiliAttemptOutcome::PartiallyCompleted),
        TaskStatus::Cancelled => Some(BilibiliAttemptOutcome::Cancelled),
        TaskStatus::Completed => Some(BilibiliAttemptOutcome::Verified),
        TaskStatus::Interrupted => Some(BilibiliAttemptOutcome::Interrupted),
        _ => None,
    }
}

fn finish_bilibili_attempt(record: &mut TaskRecord, outcome: BilibiliAttemptOutcome) {
    if let Some(snapshot) = record.bilibili_attempt.as_mut() {
        snapshot.finish(outcome);
        refresh_bilibili_attempt_counts(record);
    }
}

impl QueueManager {
    pub fn open(config: &AppConfig) -> Result<Self> {
        Self::open_with_file_provider(config, platform_queue_file_provider())
    }

    pub(crate) fn open_with_file_provider(
        config: &AppConfig,
        file_provider: Arc<dyn QueueFileProvider>,
    ) -> Result<Self> {
        let video_root =
            coordinate_directory_root(file_provider.as_ref(), &config.downloads.video_dir)?;
        let pdf_root =
            coordinate_directory_root(file_provider.as_ref(), &config.downloads.pdf_dir)?;
        let mut video = DownloadStore::from_root(video_root, Arc::clone(&file_provider))?;
        let pdf = if video.root.root_identity() == pdf_root.root_identity() {
            let pdf_alias = pdf_root.logical_root_path().to_path_buf();
            if !video.root_aliases.contains(&pdf_alias) {
                video.root_aliases.push(pdf_alias);
            }
            video.clone()
        } else {
            DownloadStore::from_root(pdf_root, file_provider)?
        };
        // Protected property: a live queue manager exclusively owns each backing download root,
        // so no second process can mutate its task index or recover its running tasks. The
        // descriptor-bound advisory lock is held for this manager's full lifetime.
        let video_owner_lock = video.lock_queue_owner()?;
        let pdf_owner_lock = if video.shares_root(&pdf) {
            None
        } else {
            Some(pdf.lock_queue_owner()?)
        };
        let manager = Self {
            video,
            pdf,
            _video_owner_lock: video_owner_lock,
            _pdf_owner_lock: pdf_owner_lock,
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
        manager.video.ensure_index()?;
        if !manager.video.shares_root(&manager.pdf) {
            manager.pdf.ensure_index()?;
        }
        manager.video.reconcile_index()?;
        if !manager.video.shares_root(&manager.pdf) {
            manager.pdf.reconcile_index()?;
        }
        manager.recover_interrupted_tasks()?;
        Ok(manager)
    }

    pub fn create(&self, mut task: TaskRecord) -> Result<bool> {
        if let Some(snapshot) = task.bilibili_attempt.as_mut() {
            snapshot.sanitize_errors();
        }
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

    pub fn get_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
    ) -> Result<Option<TaskRecord>> {
        Ok(self
            .get(id)?
            .filter(|record| record.generation == expected_generation))
    }

    pub fn generation_matches(&self, id: &str, expected_generation: u64) -> Result<bool> {
        Ok(self
            .get(id)?
            .is_some_and(|record| record.generation == expected_generation))
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

    #[cfg(test)]
    pub fn claim_resume(&self, id: &str, retry_failed: bool) -> Result<Option<TaskRecord>> {
        self.claim_resume_with_constraints(id, None, None, retry_failed)
    }

    pub fn claim_resume_for_chat_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        chat_id: i64,
        retry_failed: bool,
    ) -> Result<Option<TaskRecord>> {
        self.claim_resume_with_constraints(
            id,
            Some(expected_generation),
            Some(chat_id),
            retry_failed,
        )
    }

    fn claim_resume_with_constraints(
        &self,
        id: &str,
        expected_generation: Option<u64>,
        expected_chat_id: Option<i64>,
        retry_failed: bool,
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let video_operation_lock = self.video.lock_queue_operations()?;
        if let Some((record, entry)) = self.video.get_task(id)? {
            return Self::claim_resume_in_store(
                &self.video,
                retry_failed,
                expected_generation,
                expected_chat_id,
                record,
                entry,
            );
        }
        drop(video_operation_lock);
        if self.video.shares_root(&self.pdf) {
            return Ok(None);
        }
        let _pdf_operation_lock = self.pdf.lock_queue_operations()?;
        let Some((record, entry)) = self.pdf.get_task(id)? else {
            return Ok(None);
        };
        Self::claim_resume_in_store(
            &self.pdf,
            retry_failed,
            expected_generation,
            expected_chat_id,
            record,
            entry,
        )
    }

    fn claim_resume_in_store(
        store: &DownloadStore,
        retry_failed: bool,
        expected_generation: Option<u64>,
        expected_chat_id: Option<i64>,
        mut record: TaskRecord,
        entry: TaskIndexEntry,
    ) -> Result<Option<TaskRecord>> {
        if expected_generation.is_some_and(|generation| record.generation != generation)
            || expected_chat_id.is_some_and(|chat_id| record.chat_id != chat_id)
        {
            return Ok(None);
        }
        let resumable_failed_group = !retry_failed
            && record.status == TaskStatus::Failed
            && record.bilibili_attempt.as_ref().is_some_and(|snapshot| {
                snapshot.independent_entries
                    && snapshot.verified_entries() == 0
                    && snapshot.entries.iter().any(|entry| {
                        matches!(
                            entry.state,
                            BilibiliAttemptEntryState::Pending
                                | BilibiliAttemptEntryState::Downloading
                                | BilibiliAttemptEntryState::Downloaded
                                | BilibiliAttemptEntryState::Interrupted
                        )
                    })
            });
        let allowed = if retry_failed {
            matches!(
                record.status,
                TaskStatus::Failed | TaskStatus::PartiallyCompleted
            )
        } else {
            matches!(
                record.status,
                TaskStatus::Received
                    | TaskStatus::AwaitingSelection
                    | TaskStatus::AwaitingConfirmation
                    | TaskStatus::AwaitingDuplicateChoice
                    | TaskStatus::PartiallyCompleted
                    | TaskStatus::Interrupted
            ) || resumable_failed_group
        };
        if !allowed {
            return Ok(None);
        }
        if retry_failed
            && record.bilibili_attempt.as_ref().is_some_and(|snapshot| {
                snapshot.independent_entries && snapshot.failed_entries() == 0
            })
        {
            return Ok(None);
        }
        record.generation = record
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("task {} generation overflow", record.id))?;
        record.status = TaskStatus::Preparing;
        record.cancel_requested = false;
        record.error = None;
        record.status_message_id = None;
        let independent_entries = record
            .bilibili_attempt
            .as_ref()
            .is_some_and(|snapshot| snapshot.independent_entries);
        if independent_entries {
            if let Some(snapshot) = record.bilibili_attempt.as_mut() {
                snapshot.prepare_resume(retry_failed);
                snapshot.sanitize_errors();
            }
            refresh_bilibili_attempt_counts(&mut record);
        } else if record.bilibili_attempt.take().is_some() {
            record.media_entries_completed = 0;
            record.media_entries_failed = 0;
        }
        record.user_actions = record.user_actions.saturating_add(1);
        store.save_mutated_record(record, entry, true).map(Some)
    }

    pub fn claim_episode_retry_for_chat_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        chat_id: i64,
        index: u32,
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let video_operation_lock = self.video.lock_queue_operations()?;
        if let Some((record, entry)) = self.video.get_task(id)? {
            return Self::claim_episode_retry_in_store(
                &self.video,
                record,
                entry,
                expected_generation,
                chat_id,
                index,
            );
        }
        drop(video_operation_lock);
        if self.video.shares_root(&self.pdf) {
            return Ok(None);
        }
        let _pdf_operation_lock = self.pdf.lock_queue_operations()?;
        let Some((record, entry)) = self.pdf.get_task(id)? else {
            return Ok(None);
        };
        Self::claim_episode_retry_in_store(
            &self.pdf,
            record,
            entry,
            expected_generation,
            chat_id,
            index,
        )
    }

    fn claim_episode_retry_in_store(
        store: &DownloadStore,
        mut record: TaskRecord,
        entry: TaskIndexEntry,
        expected_generation: u64,
        chat_id: i64,
        index: u32,
    ) -> Result<Option<TaskRecord>> {
        if record.generation != expected_generation
            || record.chat_id != chat_id
            || !matches!(
                record.status,
                TaskStatus::Failed | TaskStatus::PartiallyCompleted | TaskStatus::Interrupted
            )
        {
            return Ok(None);
        }
        let Some(snapshot) = record.bilibili_attempt.as_mut() else {
            return Ok(None);
        };
        if !snapshot.prepare_episode_retry(index) {
            return Ok(None);
        }
        record.generation = record
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("task {} generation overflow", record.id))?;
        record.status = TaskStatus::Preparing;
        record.cancel_requested = false;
        record.error = None;
        record.status_message_id = None;
        refresh_bilibili_attempt_counts(&mut record);
        record.user_actions = record.user_actions.saturating_add(1);
        store.save_mutated_record(record, entry, true).map(Some)
    }

    #[cfg(test)]
    pub fn update_job_if_current(
        &self,
        id: &str,
        expected_statuses: &[TaskStatus],
        job: JobRequest,
        status: TaskStatus,
    ) -> Result<Option<TaskRecord>> {
        let (job, url_was_sanitized) = sanitize_job_for_storage(job);
        self.update_if_current(id, expected_statuses, |record| {
            if matches!(record.status, TaskStatus::Cancelled | TaskStatus::Completed) {
                bail!("task {id} is already terminal");
            }
            record.original_url = job_url(&job).to_string();
            record.url_was_sanitized |= url_was_sanitized;
            if record.job != job {
                record.display_metadata = None;
            }
            record.job = job;
            record.status = status;
            record.cancel_requested = false;
            record.error = None;
            Ok(())
        })
    }

    pub fn update_job_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        expected_statuses: &[TaskStatus],
        job: JobRequest,
        status: TaskStatus,
    ) -> Result<Option<TaskRecord>> {
        let (job, url_was_sanitized) = sanitize_job_for_storage(job);
        self.update_if_generation_and_status(
            id,
            expected_generation,
            expected_statuses,
            true,
            |record| {
                if matches!(record.status, TaskStatus::Cancelled | TaskStatus::Completed) {
                    bail!("task {id} is already terminal");
                }
                record.original_url = job_url(&job).to_string();
                record.url_was_sanitized |= url_was_sanitized;
                if record.job != job {
                    record.display_metadata = None;
                }
                record.job = job;
                record.status = status;
                record.cancel_requested = false;
                record.error = None;
                Ok(())
            },
        )
    }

    #[cfg(test)]
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

    pub fn set_status_if_current(
        &self,
        id: &str,
        expected_statuses: &[TaskStatus],
        status: TaskStatus,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_current(id, expected_statuses, |record| {
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
            record.error = None;
            Ok(())
        })
    }

    pub fn set_status_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        expected_statuses: &[TaskStatus],
        status: TaskStatus,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_generation_and_status(
            id,
            expected_generation,
            expected_statuses,
            true,
            |record| {
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
                record.error = None;
                Ok(())
            },
        )
    }

    #[cfg(test)]
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

    pub fn begin_run_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if record.generation != expected_generation || record.status != TaskStatus::Queued {
            return Ok(None);
        }
        record.status = TaskStatus::Running;
        record.cancel_requested = false;
        record.error = None;
        store.save_mutated_record(record, entry, true).map(Some)
    }

    #[cfg(test)]
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

    pub fn begin_verification_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if record.generation != expected_generation || record.status != TaskStatus::Running {
            return Ok(None);
        }
        record.status = TaskStatus::Verifying;
        record.cancel_requested = false;
        store.save_mutated_record(record, entry, true).map(Some)
    }

    pub fn set_display_metadata_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        metadata: PlanValidationSnapshot,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_generation_and_status(
            id,
            expected_generation,
            &[TaskStatus::Preparing, TaskStatus::Running],
            false,
            |record| {
                record.display_metadata = Some(metadata);
                Ok(())
            },
        )
    }

    pub fn set_plan_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        current: PlanValidationSnapshot,
    ) -> Result<Option<(TaskRecord, Vec<&'static str>)>> {
        self.set_plan_if_generation_inner(id, Some(expected_generation), current)
    }

    fn set_plan_if_generation_inner(
        &self,
        id: &str,
        expected_generation: Option<u64>,
        current: PlanValidationSnapshot,
    ) -> Result<Option<(TaskRecord, Vec<&'static str>)>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            if expected_generation.is_some() {
                return Ok(None);
            }
            bail!("persistent task {id} was not found");
        };
        if expected_generation.is_some_and(|generation| record.generation != generation) {
            return Ok(None);
        }
        if record.status != TaskStatus::Running {
            if expected_generation.is_some() {
                return Ok(None);
            }
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
        Ok(Some((record, differences)))
    }

    pub fn accept_proposed_plan_for_chat_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        chat_id: i64,
    ) -> Result<Option<TaskRecord>> {
        self.accept_proposed_plan_with_constraints(id, Some(expected_generation), Some(chat_id))
    }

    fn accept_proposed_plan_with_constraints(
        &self,
        id: &str,
        expected_generation: Option<u64>,
        expected_chat_id: Option<i64>,
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if expected_generation.is_some_and(|generation| record.generation != generation)
            || expected_chat_id.is_some_and(|chat_id| record.chat_id != chat_id)
            || record.status != TaskStatus::AwaitingConfirmation
        {
            return Ok(None);
        }
        let Some(plan) = record.proposed_plan.take() else {
            return Ok(None);
        };
        let generation = record.generation;
        let previous_plan = record.plan.clone();
        reconcile_accepted_bilibili_attempt(&mut record, previous_plan.as_ref(), &plan);
        record.plan = Some(plan);
        record.status = TaskStatus::Preparing;
        record.cancel_requested = false;
        record.user_actions = record.user_actions.saturating_add(1);
        let record = store.save_mutated_record(record, entry, true)?;
        let mut cancellations = self.cancellations.lock().map_err(poisoned_lock)?;
        if cancellations
            .get(id)
            .is_some_and(|registration| registration.generation == Some(generation))
        {
            cancellations.remove(id);
        }
        Ok(Some(record))
    }

    #[cfg(test)]
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

    pub fn set_collection_progress_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        total: usize,
        completed: usize,
        failed: usize,
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if record.generation != expected_generation {
            return Ok(None);
        }
        if record.bilibili_attempt.is_some() {
            // Episode attempt state owns these counters once it is available.
            return Ok(Some(record));
        }
        record.media_entries_total = total.max(1);
        record.media_entries_completed = completed.min(record.media_entries_total);
        record.media_entries_failed = failed.min(record.media_entries_total);
        store.save_mutated_record(record, entry, true).map(Some)
    }

    pub fn set_bilibili_attempt_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        mut snapshot: BilibiliAttemptSnapshot,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_generation(id, expected_generation, true, |record| {
            if record.status == TaskStatus::Completed && snapshot.independent_entries {
                return Ok(());
            }
            snapshot.sanitize_errors();
            if let Some(outcome) = terminal_bilibili_attempt_outcome(record.status) {
                snapshot.finish(outcome);
            }
            record.bilibili_attempt = Some(snapshot);
            refresh_bilibili_attempt_counts(record);
            Ok(())
        })
    }

    #[cfg(test)]
    pub fn update_bilibili_attempt_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        index: u32,
        update: &BilibiliAttemptUpdate,
    ) -> Result<Option<TaskRecord>> {
        self.update_bilibili_attempt_batch_if_generation(
            id,
            expected_generation,
            &[(index, update.clone())],
        )
    }

    pub fn update_bilibili_attempt_batch_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        updates: &[(u32, BilibiliAttemptUpdate)],
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if record.generation != expected_generation {
            return Ok(None);
        }
        if updates.is_empty() {
            return Ok(Some(record));
        }
        let Some(snapshot) = record.bilibili_attempt.as_mut() else {
            return Ok(Some(record));
        };
        if snapshot.outcome.is_some() {
            return Ok(Some(record));
        }
        for (index, update) in updates {
            update.apply(snapshot, *index);
        }
        refresh_bilibili_attempt_counts(&mut record);
        store.save_mutated_record(record, entry, true).map(Some)
    }

    pub fn record_staging_path_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        path: PathBuf,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_generation(id, expected_generation, false, |record| {
            if !record.staging_attempts.contains(&path) {
                record.staging_attempts.push(path);
            }
            Ok(())
        })
    }

    #[cfg(test)]
    pub fn set_status_message_id(&self, id: &str, message_id: i64) -> Result<TaskRecord> {
        self.update(id, false, |record| {
            record.status_message_id = Some(message_id);
            Ok(())
        })
    }

    pub fn set_status_message_id_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        message_id: i64,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_generation(id, expected_generation, false, |record| {
            record.status_message_id = Some(message_id);
            Ok(())
        })
    }

    pub fn set_status_message_id_if_generation_and_status(
        &self,
        id: &str,
        expected_generation: u64,
        expected_statuses: &[TaskStatus],
        message_id: i64,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_generation_and_status(
            id,
            expected_generation,
            expected_statuses,
            false,
            |record| {
                record.status_message_id = Some(message_id);
                Ok(())
            },
        )
    }

    #[cfg(test)]
    pub fn register_cancellation(&self, id: &str) -> Result<Arc<Notify>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let notify = Arc::new(Notify::new());
        let mut active = self.cancellations.lock().map_err(poisoned_lock)?;
        active.insert(
            id.to_string(),
            CancellationRegistration {
                generation: None,
                notify: Arc::clone(&notify),
            },
        );
        Ok(notify)
    }

    pub fn register_cancellation_if_queued_generation(
        &self,
        id: &str,
        expected_generation: u64,
    ) -> Result<Option<(TaskRecord, Arc<Notify>)>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some(record) = self.find_record_unlocked(id)? else {
            return Ok(None);
        };
        if record.generation != expected_generation || record.status != TaskStatus::Queued {
            return Ok(None);
        }

        let mut active = self.cancellations.lock().map_err(poisoned_lock)?;
        if let Some(current) = active
            .get(id)
            .filter(|current| current.generation == Some(expected_generation))
        {
            return Ok(Some((record, Arc::clone(&current.notify))));
        }

        let notify = Arc::new(Notify::new());
        active.insert(
            id.to_string(),
            CancellationRegistration {
                generation: Some(expected_generation),
                notify: Arc::clone(&notify),
            },
        );
        Ok(Some((record, notify)))
    }

    #[cfg(test)]
    pub fn unregister_cancellation(&self, id: &str) -> Result<()> {
        self.cancellations.lock().map_err(poisoned_lock)?.remove(id);
        Ok(())
    }

    /// Remove a cancellation token only while it remains the current registration.
    pub fn unregister_cancellation_if_current(&self, id: &str, notify: &Arc<Notify>) -> Result<()> {
        let mut active = self.cancellations.lock().map_err(poisoned_lock)?;
        if active
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(&current.notify, notify))
        {
            active.remove(id);
        }
        Ok(())
    }

    fn cancellation_target_unlocked(&self, id: &str) -> Result<Option<Arc<Notify>>> {
        Ok(self
            .cancellations
            .lock()
            .map_err(poisoned_lock)?
            .get(id)
            .map(|registration| Arc::clone(&registration.notify)))
    }

    #[cfg(test)]
    pub fn complete(
        &self,
        id: &str,
        saved_location: String,
        media_paths: &[PathBuf],
        hashes: BTreeMap<String, String>,
    ) -> Result<TaskRecord> {
        self.complete_if_generation_inner(id, None, saved_location, media_paths, hashes)?
            .ok_or_else(|| anyhow!("persistent task {id} was missing at completion"))
    }

    pub fn complete_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        saved_location: String,
        media_paths: &[PathBuf],
        hashes: BTreeMap<String, String>,
    ) -> Result<Option<TaskRecord>> {
        self.complete_if_generation_inner(
            id,
            Some(expected_generation),
            saved_location,
            media_paths,
            hashes,
        )
    }

    fn complete_if_generation_inner(
        &self,
        id: &str,
        expected_generation: Option<u64>,
        saved_location: String,
        media_paths: &[PathBuf],
        hashes: BTreeMap<String, String>,
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, mut entry)) = self.find_record_entry_unlocked(id)? else {
            if expected_generation.is_some() {
                return Ok(None);
            }
            bail!("persistent task {id} was missing at completion");
        };
        if expected_generation.is_some_and(|generation| record.generation != generation) {
            return Ok(None);
        }
        let media_paths = media_paths
            .iter()
            .map(|path| store.normalize_media_path(path))
            .collect::<Result<Vec<_>>>()?;
        let (persisted_hashes, hash_manifest) = persist_media_hashes(hashes)?;
        if record.status == TaskStatus::Completed {
            if record.saved_location.as_deref() != Some(saved_location.as_str())
                || record.primary_media_hashes != persisted_hashes
                || record.primary_media_hash_manifest != hash_manifest
            {
                bail!("task {id} was already completed with different published outputs");
            }
            let destination = sidecar_destination(&store.root_path, &record, &media_paths)?;
            return self
                .finish_completed_record_sidecar(store, record, entry, destination)
                .map(Some);
        }
        if record.status != TaskStatus::Verifying {
            bail!("task {id} changed state before output verification completed");
        }
        if let Some(snapshot) = record.bilibili_attempt.as_ref()
            && snapshot.independent_entries
            && snapshot
                .entries
                .iter()
                .any(|entry| entry.state != BilibiliAttemptEntryState::Verified)
        {
            bail!("task {id} has Bilibili entries that are not individually verified");
        }
        record.status = TaskStatus::Completed;
        let has_bilibili_attempt = record.bilibili_attempt.is_some();
        if let Some(snapshot) = record.bilibili_attempt.as_mut() {
            snapshot.finish(BilibiliAttemptOutcome::Verified);
        }
        record.error = None;
        record.saved_location = Some(saved_location);
        record.primary_media_hashes = persisted_hashes;
        record.primary_media_hash_manifest = hash_manifest;
        if has_bilibili_attempt {
            refresh_bilibili_attempt_counts(&mut record);
        } else if record.media_entries_total <= 1 {
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
            return self
                .finish_completed_record_sidecar(store, record, entry, None)
                .map(Some);
        };
        record.updated_at = unix_time();
        record.revision = record.revision.saturating_add(1);
        entry.move_target = Some(destination.clone());
        entry.revision = record.revision;
        entry.updated_at = record.updated_at;
        store.write_record(&record_path, &record)?;
        store.save_index_task(id, entry.clone())?;
        self.finish_completed_record_sidecar(store, record, entry, Some(destination))
            .map(Some)
    }

    fn finish_completed_record_sidecar(
        &self,
        store: &DownloadStore,
        record: TaskRecord,
        mut entry: TaskIndexEntry,
        destination: Option<PathBuf>,
    ) -> Result<TaskRecord> {
        let Some(destination) = destination else {
            if entry.record_path.parent() == Some(store.queue_dir.as_path()) {
                return store.save_mutated_record(record, entry, true);
            }
            return Ok(record);
        };
        if let Some(pending_target) = &entry.move_target {
            if pending_target != &destination {
                bail!(
                    "task {} has a different pending sidecar destination",
                    record.id
                );
            }
        } else if entry.record_path != destination {
            entry.move_target = Some(destination.clone());
            entry.revision = record.revision;
            entry.updated_at = record.updated_at;
            store.save_index_task(&record.id, entry.clone())?;
        }
        if entry.record_path == destination {
            if entry.move_target.is_some() {
                entry.move_target = None;
                return store.save_mutated_record(record, entry, true);
            }
            return Ok(record);
        }
        match store.move_record_to_sidecar(&record.id, &entry.record_path, &destination) {
            Ok(SidecarMoveStatus::Moved | SidecarMoveStatus::AlreadyAtTarget) => {}
            Ok(SidecarMoveStatus::Missing) => {
                bail!("task record disappeared before sidecar migration")
            }
            Err(error) if is_file_provider_access_error(&error) => {
                tracing::warn!(
                    task_id = %record.id,
                    error = %error,
                    "published task is complete; retaining its pending sidecar migration for recovery"
                );
                return Ok(record);
            }
            Err(error) => return Err(error),
        }
        #[cfg(test)]
        if self.interrupt_after_sidecar_move.load(Ordering::Relaxed) {
            bail!("simulated interruption after task sidecar migration");
        }
        entry.record_path = destination;
        entry.move_target = None;
        store.save_mutated_record(record, entry, true)
    }

    pub fn fail_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        message: String,
    ) -> Result<Option<TaskRecord>> {
        self.fail_with_bilibili_attempt_if_generation(id, expected_generation, message, None)
    }

    pub fn fail_with_bilibili_attempt_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        message: String,
        snapshot: Option<BilibiliAttemptSnapshot>,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_generation(id, expected_generation, true, |record| {
            if matches!(record.status, TaskStatus::Cancelled | TaskStatus::Completed) {
                bail!("task {id} is already terminal");
            }
            if let Some(mut snapshot) = snapshot {
                snapshot.sanitize_errors();
                record.bilibili_attempt = Some(snapshot);
            }
            let has_verified_entries = record.bilibili_attempt.as_ref().is_some_and(|snapshot| {
                snapshot.independent_entries && snapshot.verified_entries() > 0
            });
            record.status = if has_verified_entries {
                TaskStatus::PartiallyCompleted
            } else {
                TaskStatus::Failed
            };
            record.cancel_requested = false;
            record.error = Some(message);
            let has_bilibili_attempt = record.bilibili_attempt.is_some();
            let outcome = if has_verified_entries {
                BilibiliAttemptOutcome::PartiallyCompleted
            } else {
                BilibiliAttemptOutcome::Failed
            };
            finish_bilibili_attempt(record, outcome);
            if !has_bilibili_attempt
                && record.media_entries_total <= 1
                && record.media_entries_completed == 0
            {
                record.media_entries_failed = 1;
            }
            Ok(())
        })
    }

    pub fn interrupt_with_bilibili_attempt_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        message: String,
        snapshot: Option<BilibiliAttemptSnapshot>,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_generation(id, expected_generation, true, |record| {
            if matches!(record.status, TaskStatus::Cancelled | TaskStatus::Completed) {
                bail!("task {id} is already terminal");
            }
            if let Some(mut snapshot) = snapshot {
                snapshot.sanitize_errors();
                record.bilibili_attempt = Some(snapshot);
            }
            record.status = TaskStatus::Interrupted;
            record.cancel_requested = false;
            record.error = Some(message);
            finish_bilibili_attempt(record, BilibiliAttemptOutcome::Interrupted);
            Ok(())
        })
    }

    #[cfg(test)]
    pub fn cancel(&self, id: &str, chat_id: i64) -> Result<Option<TaskRecord>> {
        self.cancel_matching(id, None, chat_id, None, true, true)
    }

    pub fn cancel_for_chat_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        chat_id: i64,
    ) -> Result<Option<TaskRecord>> {
        self.cancel_matching(id, Some(expected_generation), chat_id, None, true, true)
    }

    #[cfg(test)]
    pub fn cancel_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        chat_id: i64,
    ) -> Result<Option<TaskRecord>> {
        self.cancel_matching(id, Some(expected_generation), chat_id, None, true, true)
    }

    #[cfg(test)]
    pub fn cancel_if_current(
        &self,
        id: &str,
        chat_id: i64,
        expected_statuses: &[TaskStatus],
    ) -> Result<Option<TaskRecord>> {
        self.cancel_matching(id, None, chat_id, Some(expected_statuses), false, false)
    }

    pub fn cancel_if_generation_and_status(
        &self,
        id: &str,
        expected_generation: u64,
        chat_id: i64,
        expected_statuses: &[TaskStatus],
    ) -> Result<Option<TaskRecord>> {
        self.cancel_matching(
            id,
            Some(expected_generation),
            chat_id,
            Some(expected_statuses),
            false,
            true,
        )
    }

    fn cancel_matching(
        &self,
        id: &str,
        expected_generation: Option<u64>,
        chat_id: i64,
        expected_statuses: Option<&[TaskStatus]>,
        reject_verifying: bool,
        running_sets_request: bool,
    ) -> Result<Option<TaskRecord>> {
        let Some((record, cancellation_target)) = self.cancel_matching_and_capture(
            id,
            expected_generation,
            chat_id,
            expected_statuses,
            reject_verifying,
            running_sets_request,
        )?
        else {
            return Ok(None);
        };
        if let Some(notify) = cancellation_target {
            notify.notify_one();
        }
        Ok(Some(record))
    }

    // Keep the captured token with the persisted transition so a later generation cannot
    // replace the map entry before the caller delivers this cancellation notification.
    fn cancel_matching_and_capture(
        &self,
        id: &str,
        expected_generation: Option<u64>,
        chat_id: i64,
        expected_statuses: Option<&[TaskStatus]>,
        reject_verifying: bool,
        running_sets_request: bool,
    ) -> Result<Option<(TaskRecord, Option<Arc<Notify>>)>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if expected_generation.is_some_and(|generation| record.generation != generation)
            || record.chat_id != chat_id
            || expected_statuses.is_some_and(|statuses| !statuses.contains(&record.status))
            || !record.status.is_unfinished()
            || (reject_verifying && record.status == TaskStatus::Verifying)
        {
            return Ok(None);
        }

        let cancellation_target = self.cancellation_target_unlocked(id)?;
        if running_sets_request && record.status == TaskStatus::Running {
            record.cancel_requested = true;
            record.user_actions = record.user_actions.saturating_add(1);
            let result = store.save_mutated_record(record, entry, false)?;
            drop(_guard);
            return Ok(Some((result, cancellation_target)));
        }
        record.status = TaskStatus::Cancelled;
        record.cancel_requested = false;
        record.error = None;
        record.user_actions = record.user_actions.saturating_add(1);
        finish_bilibili_attempt(&mut record, BilibiliAttemptOutcome::Cancelled);
        let result = store.save_mutated_record(record, entry, true)?;
        drop(_guard);
        Ok(Some((result, cancellation_target)))
    }

    #[cfg(test)]
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
        finish_bilibili_attempt(&mut record, BilibiliAttemptOutcome::Cancelled);
        store.save_mutated_record(record, entry, true).map(Some)
    }

    pub fn finish_cancellation_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
    ) -> Result<Option<TaskRecord>> {
        self.finish_cancellation_with_bilibili_attempt_if_generation(id, expected_generation, None)
    }

    pub fn finish_cancellation_with_bilibili_attempt_if_generation(
        &self,
        id: &str,
        expected_generation: u64,
        snapshot: Option<BilibiliAttemptSnapshot>,
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if record.generation != expected_generation
            || !record.cancel_requested
            || !matches!(
                record.status,
                TaskStatus::Running | TaskStatus::AwaitingConfirmation
            )
        {
            return Ok(None);
        }
        if let Some(snapshot) = snapshot {
            record.bilibili_attempt = Some(snapshot);
        }
        record.status = TaskStatus::Cancelled;
        record.cancel_requested = false;
        record.error = None;
        finish_bilibili_attempt(&mut record, BilibiliAttemptOutcome::Cancelled);
        store.save_mutated_record(record, entry, true).map(Some)
    }

    #[cfg(test)]
    pub fn cancel_for_chat(&self, id: &str, chat_id: i64) -> Result<Option<TaskRecord>> {
        self.cancel(id, chat_id)
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
                    partially_completed_jobs: 0,
                    recently_completed_jobs: 0,
                    recently_failed_jobs: 0,
                    completed_entries: 0,
                    remaining_entries: 0,
                    failed_entries: 0,
                });
                for record in chat_records {
                    if record.status == TaskStatus::PartiallyCompleted {
                        summary.partially_completed_jobs += 1;
                        summary.completed_entries += record.media_entries_completed;
                        summary.failed_entries += record.media_entries_failed;
                        summary.remaining_entries += record
                            .media_entries_total
                            .saturating_sub(record.media_entries_completed)
                            .saturating_sub(record.media_entries_failed);
                    } else if record.status.is_unfinished() {
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

    #[cfg(test)]
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

    fn update_if_current<F>(
        &self,
        id: &str,
        expected_statuses: &[TaskStatus],
        mutate: F,
    ) -> Result<Option<TaskRecord>>
    where
        F: FnOnce(&mut TaskRecord) -> Result<()>,
    {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if !expected_statuses.contains(&record.status) {
            return Ok(None);
        }
        mutate(&mut record)?;
        store.save_mutated_record(record, entry, true).map(Some)
    }

    fn update_if_generation<F>(
        &self,
        id: &str,
        expected_generation: u64,
        activity: bool,
        mutate: F,
    ) -> Result<Option<TaskRecord>>
    where
        F: FnOnce(&mut TaskRecord) -> Result<()>,
    {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if record.generation != expected_generation {
            return Ok(None);
        }
        mutate(&mut record)?;
        store.save_mutated_record(record, entry, activity).map(Some)
    }

    fn update_if_generation_and_status<F>(
        &self,
        id: &str,
        expected_generation: u64,
        expected_statuses: &[TaskStatus],
        activity: bool,
        mutate: F,
    ) -> Result<Option<TaskRecord>>
    where
        F: FnOnce(&mut TaskRecord) -> Result<()>,
    {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        if record.generation != expected_generation || !expected_statuses.contains(&record.status) {
            return Ok(None);
        }
        mutate(&mut record)?;
        store.save_mutated_record(record, entry, activity).map(Some)
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
                if record.status.is_unfinished()
                    && !matches!(
                        record.status,
                        TaskStatus::Interrupted | TaskStatus::PartiallyCompleted
                    )
                {
                    let Some((current, entry)) = store.get_task(&record.id)? else {
                        continue;
                    };
                    record = current;
                    if record.cancel_requested {
                        record.status = TaskStatus::Cancelled;
                        record.cancel_requested = false;
                        record.error = None;
                    } else {
                        record.status = TaskStatus::Interrupted;
                        record.cancel_requested = false;
                        record.error = Some("Task was interrupted by process restart.".to_string());
                    }
                    let outcome = if record.status == TaskStatus::Cancelled {
                        BilibiliAttemptOutcome::Cancelled
                    } else {
                        BilibiliAttemptOutcome::Interrupted
                    };
                    finish_bilibili_attempt(&mut record, outcome);
                    store.save_mutated_record(record, entry, true)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub fn hash_primary_media(
    root_path: &Path,
    media_paths: &[PathBuf],
) -> Result<BTreeMap<String, String>> {
    hash_primary_media_with_expected_identities(root_path, media_paths, &BTreeMap::new())
}

pub(crate) fn hash_primary_media_with_expected_identities(
    root_path: &Path,
    media_paths: &[PathBuf],
    expected: &BTreeMap<PathBuf, EntryIdentity>,
) -> Result<BTreeMap<String, String>> {
    if expected.keys().any(|path| !media_paths.contains(path)) {
        bail!("expected media identity is not included in the published report");
    }
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
        // Protect object identity (device, inode, and type) on the same descriptor
        // used for hashing. Directory churn does not change this property.
        if expected
            .get(path)
            .is_some_and(|expected| *expected != identity)
        {
            bail!(
                "published media identity changed before hashing: {}",
                path.display()
            );
        }
        let file = file.duplicate_std_file()?;
        hashes.insert(
            path.display().to_string(),
            sha256_file(&root, &bound_path, file, identity)?,
        );
    }
    Ok(hashes)
}

fn coordinate_directory_root(
    file_provider: &dyn QueueFileProvider,
    path: &Path,
) -> Result<RootedFs> {
    let mut bound_root = None;
    let mut list_entries = |coordinated_path: &Path| -> Result<()> {
        // Protected property: retain the identity of the configured download-root directory
        // opened while File Provider coordination holds access. Check that the coordinator's
        // accessor names that same root, enumerate it through its descriptor, and reuse this
        // bound root after the accessor returns instead of resolving the pathname again.
        let root = RootedFs::new(path)?;
        let rooted_accessor = coordinated_path_under_root(&root, coordinated_path)?;
        if rooted_accessor != root.logical_root_path() {
            bail!("coordinated download directory is outside its configured root");
        }
        let _ = root.list_root_directory()?;
        bound_root = Some(root);
        Ok(())
    };
    file_provider
        .coordinate_read(path, &mut list_entries)
        .map_err(|error| classify_deadlock_error(path, "read", error))?;
    bound_root.ok_or_else(|| anyhow!("File Provider did not supply a coordinated download root"))
}

fn coordinate_private_directory_creation(
    file_provider: &dyn QueueFileProvider,
    root: &RootedFs,
    path: &Path,
    mode: u16,
) -> Result<()> {
    let mut created = false;
    let mut create_directory = |coordinated_path: &Path| -> Result<()> {
        let rooted_accessor = coordinated_path_under_root(root, coordinated_path)?;
        if rooted_accessor == root.logical_root_path() {
            bail!("coordinated queue directory resolved to the download root");
        }
        let _ = root.create_dir(&rooted_accessor, mode)?;
        let entry = root.bind_entry(&rooted_accessor, false)?;
        let identity = root
            .bound_entry_identity(&entry)?
            .ok_or_else(|| anyhow!("coordinated task queue directory disappeared"))?;
        root.validate_private_bound_directory(&entry, identity, mode)
            .context("task queue directory must be owner-private")?;
        created = true;
        Ok(())
    };
    file_provider
        .coordinate_write(path, &mut create_directory)
        .map_err(|error| classify_deadlock_error(path, "write", error))?;
    if !created {
        bail!("File Provider did not supply a coordinated directory accessor");
    }
    Ok(())
}

fn coordinate_bound_directory_listing(
    file_provider: &dyn QueueFileProvider,
    root: &RootedFs,
    path: &Path,
) -> Result<(PathBuf, EntryIdentity)> {
    let mut result = None;
    let mut list_entries = |coordinated_path: &Path| -> Result<()> {
        let rooted_accessor = coordinated_path_under_root(root, coordinated_path)?;
        if rooted_accessor == root.logical_root_path() {
            bail!("coordinated queue directory resolved to the download root");
        }
        let entry = root.bind_entry(&rooted_accessor, false)?;
        let identity = root
            .bound_entry_identity(&entry)?
            .ok_or_else(|| anyhow!("coordinated task queue directory disappeared"))?;
        root.validate_private_bound_directory(&entry, identity, 0o700)
            .context("task queue directory must be owner-private")?;
        let _ = root.list_bound_directory(&entry, identity)?;
        root.validate_private_bound_directory(&entry, identity, 0o700)
            .context("task queue directory changed during coordinated listing")?;
        result = Some((rooted_accessor, identity));
        Ok(())
    };
    file_provider
        .coordinate_read(path, &mut list_entries)
        .map_err(|error| classify_deadlock_error(path, "read", error))?;
    result.ok_or_else(|| anyhow!("File Provider did not supply a coordinated directory"))
}

fn coordinated_path_under_root(root: &RootedFs, coordinated_path: &Path) -> Result<PathBuf> {
    // The configured root is the access-policy boundary. File Provider may return either its
    // configured spelling or the canonical URL for that rooted item; reject escapes and
    // non-normal components before translating back to the logical path used by RootedFs.
    let relative = coordinated_path
        .strip_prefix(root.logical_root_path())
        .or_else(|_| coordinated_path.strip_prefix(root.root_path()))
        .with_context(|| {
            format!(
                "coordinated File Provider URL is outside the configured download root: {}",
                coordinated_path.display()
            )
        })?;
    let mut normalized = PathBuf::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!(
                "coordinated File Provider URL contains an invalid path component: {}",
                coordinated_path.display()
            );
        };
        normalized.push(name);
    }
    Ok(root.logical_root_path().join(normalized))
}

impl DownloadStore {
    fn from_root(root: RootedFs, file_provider: Arc<dyn QueueFileProvider>) -> Result<Self> {
        let requested_queue_dir = root.logical_root_path().join(QUEUE_DIRECTORY);
        coordinate_private_directory_creation(
            file_provider.as_ref(),
            &root,
            &requested_queue_dir,
            0o700,
        )?;
        // File Provider may replace a directory placeholder while enumerating it.
        // Bind the current directory identity only after its children are available.
        let (queue_dir, queue_identity) = coordinate_bound_directory_listing(
            file_provider.as_ref(),
            &root,
            &requested_queue_dir,
        )?;
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
            file_provider,
        };
        store.ensure_operation_lock_file()?;
        Ok(store)
    }

    fn shares_root(&self, other: &Self) -> bool {
        self.root.root_identity() == other.root.root_identity()
    }

    fn ensure_operation_lock_file(&self) -> Result<()> {
        self.ensure_coordinated_lock_file(&self.operation_lock_path)
    }

    fn ensure_coordinated_lock_file(&self, path: &Path) -> Result<()> {
        if self.open_coordinated_lock_file(path)?.is_some() {
            return Ok(());
        }

        let mut created = false;
        let mut create = |accessor_path: &Path| -> Result<()> {
            let coordinated_path = self.coordinated_queue_file_path(accessor_path)?;
            if coordinated_path != path {
                bail!("coordinated task queue lock path changed during creation");
            }
            self.ensure_private_directory()?;
            let file = match self
                .root
                .create_new_bound_file(&coordinated_path, &[], 0o600)
            {
                Ok(_) => self
                    .root
                    .open_bound_file(&coordinated_path)?
                    .ok_or_else(|| anyhow!("created task queue lock file disappeared"))?,
                Err(create_error) => {
                    let Some(file) = self.root.open_bound_file(&coordinated_path)? else {
                        return Err(create_error);
                    };
                    file
                }
            };
            let identity = file.identity();
            file.validate_private_single_link(0o600)?;
            let entry = self.root.bind_entry(&coordinated_path, false)?;
            if self.root.bound_entry_identity(&entry)? != Some(identity) {
                bail!("task queue lock file changed during coordinated creation");
            }
            file.validate_private_single_link(0o600)?;
            self.ensure_private_directory()?;
            created = true;
            Ok(())
        };
        self.file_provider
            .coordinate_write(path, &mut create)
            .map_err(|error| classify_deadlock_error(path, "write", error))?;
        if !created {
            bail!("File Provider did not supply a coordinated lock-file accessor");
        }
        Ok(())
    }

    fn open_coordinated_lock_file(&self, path: &Path) -> Result<Option<BoundFile>> {
        let mut opened = None;
        let mut open = |accessor_path: &Path| -> Result<()> {
            let coordinated_path = self.coordinated_queue_file_path(accessor_path)?;
            if coordinated_path != path {
                bail!("coordinated task queue lock path changed during access");
            }
            self.ensure_private_directory()?;
            let Some(file) = self.root.open_bound_file(&coordinated_path)? else {
                opened = Some(None);
                return Ok(());
            };
            let identity = file.identity();
            file.validate_private_single_link(0o600)?;
            let entry = self.root.bind_entry(&coordinated_path, false)?;
            if self.root.bound_entry_identity(&entry)? != Some(identity) {
                bail!("task queue lock file changed during coordinated access");
            }
            file.validate_private_single_link(0o600)?;
            self.ensure_private_directory()?;
            opened = Some(Some(file));
            Ok(())
        };
        self.file_provider
            .coordinate_read(path, &mut open)
            .map_err(|error| classify_deadlock_error(path, "read", error))?;
        opened
            .ok_or_else(|| anyhow!("File Provider did not supply a coordinated lock-file accessor"))
    }

    fn validate_coordinated_lock_file(&self, path: &Path, expected: EntryIdentity) -> Result<()> {
        let mut validated = false;
        let mut validate = |accessor_path: &Path| -> Result<()> {
            let coordinated_path = self.coordinated_queue_file_path(accessor_path)?;
            if coordinated_path != path {
                bail!("coordinated task queue lock path changed while locking");
            }
            self.ensure_private_directory()?;
            let entry = self.root.bind_entry(&coordinated_path, false)?;
            let file = self
                .root
                .open_bound_file(&coordinated_path)?
                .ok_or_else(|| anyhow!("task queue lock file disappeared while locking"))?;
            if file.identity() != expected
                || self.root.bound_entry_identity(&entry)? != Some(expected)
            {
                bail!("task queue lock file was replaced while locking");
            }
            file.validate_private_single_link(0o600)?;
            self.ensure_private_directory()?;
            validated = true;
            Ok(())
        };
        self.file_provider
            .coordinate_read(path, &mut validate)
            .map_err(|error| classify_deadlock_error(path, "read", error))?;
        if !validated {
            bail!("File Provider did not supply a coordinated lock-file validation");
        }
        Ok(())
    }

    fn lock_queue_owner(&self) -> Result<QueueOwnerLock> {
        let path = self.queue_dir.join(QUEUE_OWNER_LOCK_FILE);
        self.ensure_coordinated_lock_file(&path)?;
        let file = self
            .open_coordinated_lock_file(&path)?
            .ok_or_else(|| anyhow!("task queue owner lock file is missing"))?;
        let identity = file.identity();
        file.validate_private_single_link(0o600)?;
        if !file.try_lock_exclusive()? {
            bail!(
                "task queue already has a live owner for {}",
                self.root_path.display()
            );
        }
        file.validate_private_single_link(0o600)?;
        self.validate_coordinated_lock_file(&path, identity)?;
        Ok(QueueOwnerLock { _file: file })
    }

    fn lock_queue_operations(&self) -> Result<QueueOperationLock> {
        self.ensure_coordinated_lock_file(&self.operation_lock_path)?;
        let file = self
            .open_coordinated_lock_file(&self.operation_lock_path)?
            .ok_or_else(|| anyhow!("task queue operation lock file is missing"))?;
        let identity = file.identity();
        file.validate_private_single_link(0o600)?;
        file.lock_exclusive()?;
        file.validate_private_single_link(0o600)?;
        self.validate_coordinated_lock_file(&self.operation_lock_path, identity)?;
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
        // This metadata check must run inside a File Provider accessor callback. A cloud-only
        // queue directory may reject opening or inspecting its parent before coordination.
        let entry = self.root.bind_entry(&self.queue_dir, false)?;
        self.root
            .validate_private_bound_directory(&entry, self.queue_identity, 0o700)
    }

    fn coordinated_queue_file_path(&self, accessor_path: &Path) -> Result<PathBuf> {
        let rooted_path = coordinated_path_under_root(&self.root, accessor_path)?;
        let relative = rooted_path.strip_prefix(self.root.logical_root_path())?;
        if relative.as_os_str().is_empty() {
            bail!("coordinated URL does not name a task queue file");
        }
        Ok(rooted_path)
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
        // A cloud placeholder may be replaced as it materializes. Only use the
        // pre-coordination lookup to preserve the missing-file case; bind and
        // validate the actual object inside the coordinated accessor below.
        match self.root.entry_identity(path) {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(None),
            // A provider can reject metadata lookup until coordinated access
            // asks it to make the path available. Do not treat that as missing.
            Err(error) if is_deadlock_error(&error) => {}
            Err(error) => return Err(error),
        }

        let mut contents = None;
        let mut read = |accessor_path: &Path| -> Result<()> {
            let coordinated_path = self.coordinated_queue_file_path(accessor_path)?;
            self.ensure_private_directory()?;
            let file = self
                .root
                .open_bound_file(&coordinated_path)?
                .ok_or_else(|| anyhow!("task queue file disappeared during coordinated access"))?;
            file.validate_private_single_link(0o600).with_context(|| {
                format!(
                    "private task file is unsafe: {}",
                    coordinated_path.display()
                )
            })?;
            if file.byte_len()? > limit as u64 {
                bail!(
                    "task queue file exceeds its {limit}-byte limit: {}",
                    coordinated_path.display()
                );
            }
            contents = Some(file.read_limited(limit).with_context(|| {
                format!(
                    "failed to read task queue file {}",
                    coordinated_path.display()
                )
            })?);
            Ok(())
        };
        self.file_provider
            .coordinate_read(path, &mut read)
            .map_err(|error| classify_deadlock_error(path, "read", error))?;
        Ok(contents)
    }

    fn write_private_file(&self, path: &Path, contents: &[u8]) -> Result<()> {
        if contents.len() > MAX_RECORD_BYTES.max(MAX_INDEX_BYTES) {
            bail!("task queue record exceeds the configured size limit");
        }
        let mut write = |accessor_path: &Path| -> Result<()> {
            let coordinated_path = self.coordinated_queue_file_path(accessor_path)?;
            self.ensure_private_directory()?;
            let temporary = temporary_sibling(&coordinated_path);
            if let Some(file) = self.root.open_bound_file(&coordinated_path)? {
                file.validate_private_single_link(0o600)?;
                let entry = self.root.bind_entry(&coordinated_path, false)?;
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
                let destination = self.root.bind_entry(&coordinated_path, false)?;
                self.root.rename_via_bound_parents_noreplace_if_identity(
                    &source,
                    &destination,
                    identity,
                )?;
            }
            Ok(())
        };
        self.file_provider
            .coordinate_write(path, &mut write)
            .map_err(|error| classify_deadlock_error(path, "write", error))
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
            entry.move_target = Some(destination.clone());
            index.tasks.insert(record.id.clone(), entry.clone());
            self.write_index(&index)?;
            match self.move_record_to_sidecar(&record.id, &entry.record_path, &destination)? {
                SidecarMoveStatus::Moved | SidecarMoveStatus::AlreadyAtTarget => {}
                SidecarMoveStatus::Missing => {
                    bail!("task record disappeared before sidecar migration")
                }
            }
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

    fn ensure_index(&self) -> Result<()> {
        if self
            .read_private_file(&self.index_path(), MAX_INDEX_BYTES)?
            .is_none()
        {
            self.write_private_file(
                &self.index_path(),
                &serde_json::to_vec(&StoreIndex {
                    version: INDEX_VERSION,
                    ..StoreIndex::default()
                })?,
            )?;
        }
        Ok(())
    }

    fn scan_active_records(&self, index: &mut StoreIndex) -> Result<()> {
        let entries = self.list_coordinated_queue_directory()?;
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

    fn list_coordinated_queue_directory(&self) -> Result<Vec<(std::ffi::OsString, EntryIdentity)>> {
        let mut entries = None;
        let mut list_entries = |coordinated_path: &Path| -> Result<()> {
            let rooted_accessor = coordinated_path_under_root(&self.root, coordinated_path)?;
            if rooted_accessor != self.queue_dir {
                bail!("coordinated task queue directory changed during listing");
            }
            let queue_entry = self.root.bind_entry(&rooted_accessor, false)?;
            let identity = self
                .root
                .bound_entry_identity(&queue_entry)?
                .ok_or_else(|| anyhow!("coordinated task queue directory disappeared"))?;
            if identity != self.queue_identity {
                bail!("task queue directory identity changed during coordinated access");
            }
            self.root
                .validate_private_bound_directory(&queue_entry, self.queue_identity, 0o700)
                .context("task queue directory must remain owner-private")?;
            entries = Some(
                self.root
                    .list_bound_directory(&queue_entry, self.queue_identity)?,
            );
            self.root
                .validate_private_bound_directory(&queue_entry, self.queue_identity, 0o700)
                .context("task queue directory changed during coordinated listing")?;
            Ok(())
        };
        self.file_provider
            .coordinate_read(&self.queue_dir, &mut list_entries)
            .map_err(|error| classify_deadlock_error(&self.queue_dir, "read", error))?;
        entries.ok_or_else(|| anyhow!("File Provider did not supply a coordinated queue listing"))
    }

    fn reconcile_move_targets(&self, index: &mut StoreIndex) -> Result<()> {
        let mut changed = false;
        for (id, entry) in &mut index.tasks {
            let Some(target) = entry.move_target.clone() else {
                continue;
            };
            let move_status = self.move_record_to_sidecar(id, &entry.record_path, &target)?;
            if move_status == SidecarMoveStatus::Missing {
                continue;
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
    ) -> Result<SidecarMoveStatus> {
        let history_directory = self.queue_dir.join(TERMINAL_HISTORY_DIRECTORY);
        if target_path.parent() == Some(history_directory.as_path()) {
            self.ensure_private_history_directory_coordinated()?;
        }
        let mut outcome = None;
        let mut move_entries = |coordinated_source: &Path,
                                coordinated_target: &Path|
         -> Result<()> {
            let source_path_from_provider = self.coordinated_queue_file_path(coordinated_source)?;
            let target_path_from_provider = self.coordinated_queue_file_path(coordinated_target)?;
            if source_path_from_provider != source_path || target_path_from_provider != target_path
            {
                bail!("File Provider changed a task sidecar migration path");
            }
            self.ensure_private_directory()?;

            // Both source and destination are inspected only while the paired move is
            // coordinated. Descriptor identity and owner-private checks protect the files
            // selected for migration; path metadata outside this callback is not trusted.
            let source_file = self.root.open_bound_file(source_path)?;
            let target_file = self.root.open_bound_file(target_path)?;
            let target_entry = target_file
                .as_ref()
                .map(|_| self.root.bind_entry(target_path, false))
                .transpose()?;
            if let Some(file) = &source_file {
                let source_record = self.read_coordinated_record(source_path, file)?;
                if source_record.id != id {
                    bail!(
                        "sidecar recovery found a mismatched task ID at {}",
                        source_path.display()
                    );
                }
            }
            if let Some(file) = &target_file {
                let target_record = self.read_coordinated_record(target_path, file)?;
                if target_record.id != id {
                    bail!(
                        "sidecar recovery found a mismatched task ID at {}",
                        target_path.display()
                    );
                }
                self.validate_coordinated_record_entry(
                    target_path,
                    file,
                    target_entry
                        .as_ref()
                        .expect("an opened target file should have a bound directory entry"),
                )?;
            }

            match (&source_file, &target_file) {
                (Some(source_file), None) => {
                    source_file.validate_private_single_link(0o600)?;
                    let source = self.root.bind_entry(source_path, false)?;
                    let target = self.root.bind_entry(target_path, false)?;
                    self.root.rename_via_bound_parents_noreplace_if_identity(
                        &source,
                        &target,
                        source_file.identity(),
                    )?;
                    outcome = Some(SidecarMoveStatus::Moved);
                }
                (Some(source_file), Some(target_file)) => {
                    source_file.validate_private_single_link(0o600)?;
                    let source = self.root.bind_entry(source_path, false)?;
                    let target_entry = target_entry
                        .as_ref()
                        .expect("matched target file should have a bound directory entry");
                    self.validate_coordinated_record_entry(target_path, target_file, target_entry)?;
                    self.root
                        .remove_bound_file_if_identity(&source, source_file.identity())?;
                    self.validate_coordinated_record_entry(target_path, target_file, target_entry)?;
                    outcome = Some(SidecarMoveStatus::AlreadyAtTarget);
                }
                (None, Some(target_file)) => {
                    self.validate_coordinated_record_entry(
                        target_path,
                        target_file,
                        target_entry
                            .as_ref()
                            .expect("matched target file should have a bound directory entry"),
                    )?;
                    outcome = Some(SidecarMoveStatus::AlreadyAtTarget);
                }
                (None, None) => outcome = Some(SidecarMoveStatus::Missing),
            }
            Ok(())
        };
        self.file_provider
            .coordinate_move(source_path, target_path, &mut move_entries)
            .map_err(|error| classify_deadlock_error(source_path, "move", error))?;
        outcome.ok_or_else(|| anyhow!("File Provider did not supply a coordinated move"))
    }

    fn ensure_private_history_directory_coordinated(&self) -> Result<()> {
        let path = self.queue_dir.join(TERMINAL_HISTORY_DIRECTORY);
        let mut ensure = |coordinated_path: &Path| -> Result<()> {
            let coordinated_path = self.coordinated_queue_file_path(coordinated_path)?;
            if coordinated_path != path {
                bail!("File Provider changed the task history directory path");
            }
            self.ensure_private_history_directory()
        };
        self.file_provider
            .coordinate_write(&path, &mut ensure)
            .map_err(|error| classify_deadlock_error(&path, "write", error))
    }

    fn read_coordinated_record(&self, path: &Path, file: &BoundFile) -> Result<TaskRecord> {
        file.validate_private_single_link(0o600)
            .with_context(|| format!("private task record is unsafe: {}", path.display()))?;
        if file.byte_len()? > MAX_RECORD_BYTES as u64 {
            bail!(
                "task queue file exceeds its {MAX_RECORD_BYTES}-byte limit: {}",
                path.display()
            );
        }
        let bytes = file
            .read_limited(MAX_RECORD_BYTES)
            .with_context(|| format!("failed to read task queue file {}", path.display()))?;
        let record: TaskRecord = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid task queue record {}", path.display()))?;
        if record.schema_version != TASK_RECORD_VERSION {
            bail!("unsupported task record version for task {}", record.id);
        }
        Ok(record)
    }

    fn validate_coordinated_record_entry(
        &self,
        path: &Path,
        file: &BoundFile,
        entry: &crate::safe_fs::BoundEntry,
    ) -> Result<()> {
        // Descriptor identity selects the record object, while owner/mode/link checks preserve
        // its private access policy. The path check ensures recovery will not discard the active
        // copy after the destination name has been replaced; timestamps are not mutation signals.
        file.validate_private_single_link(0o600)?;
        match self.root.bound_entry_identity(entry)? {
            Some(identity) if identity == file.identity() => {}
            Some(_) => bail!(
                "coordinated task record path was replaced during migration: {}",
                path.display()
            ),
            None => bail!(
                "coordinated task record disappeared during migration: {}",
                path.display()
            ),
        }
        Ok(())
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

    use crate::file_provider::MockQueueFileProvider;

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

    fn grouped_attempt() -> BilibiliAttemptSnapshot {
        BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [
                (16, "Episode 16".into(), Some("ep-16".into())),
                (17, "Episode 17".into(), Some("ep-17".into())),
                (18, "Episode 18".into(), Some("ep-18".into())),
            ],
            0,
        )
    }

    fn grouped_plan(
        title: &str,
        media_profile: Option<&str>,
        entries: &[(u32, &str, &str)],
    ) -> PlanValidationSnapshot {
        let identities = entries
            .iter()
            .map(
                |(index, title, stable_media_id)| BilibiliGroupEntryIdentity {
                    index: *index,
                    title: (*title).to_string(),
                    stable_media_id: (*stable_media_id).to_string(),
                },
            )
            .collect::<Vec<_>>();
        PlanValidationSnapshot {
            stable_media_ids: identities
                .iter()
                .map(|entry| entry.stable_media_id.clone())
                .collect(),
            title: Some(title.to_string()),
            media_profile: media_profile.map(str::to_string),
            bilibili_group_entries: identities,
            ..PlanValidationSnapshot::default()
        }
    }

    fn bilibili_task(id: &str) -> TaskRecord {
        test_task(
            id,
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".into(),
                selection: Some(crate::router::BilibiliSelection::All),
            },
        )
    }

    fn test_video_config(directory: &Path) -> AppConfig {
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = directory.join("videos");
        config.downloads.pdf_dir = directory.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        config
    }

    #[test]
    fn grouped_partial_attempt_survives_reload_and_failed_only_retry() {
        let directory = temp_queue_root("grouped-partial-retry");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-partial-retry");
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();

        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, grouped_attempt())
            .unwrap()
            .unwrap();
        let published_path = config.downloads.video_dir.join("episode-16.mp4");
        let published_hash = "a".repeat(64);
        queue
            .update_bilibili_attempt_batch_if_generation(
                &task.id,
                0,
                &[
                    (
                        16,
                        BilibiliAttemptUpdate::Verified {
                            media_paths: vec![published_path.clone()],
                            hashes: BTreeMap::from([(
                                published_path.display().to_string(),
                                published_hash.clone(),
                            )]),
                        },
                    ),
                    (
                        17,
                        BilibiliAttemptUpdate::Failure {
                            error: "bad SESSDATA=secret".into(),
                            attempts: 2,
                        },
                    ),
                ],
            )
            .unwrap()
            .unwrap();
        let partial = queue
            .fail_if_generation(&task.id, 0, "episode failed".into())
            .unwrap()
            .unwrap();
        assert_eq!(partial.status, TaskStatus::PartiallyCompleted);
        assert_eq!(partial.media_entries_total, 3);
        assert_eq!(partial.media_entries_completed, 1);
        assert_eq!(partial.media_entries_failed, 1);
        let snapshot = partial.bilibili_attempt.unwrap();
        assert_eq!(
            snapshot.outcome,
            Some(BilibiliAttemptOutcome::PartiallyCompleted)
        );
        assert_eq!(
            snapshot.entries[1].error.as_deref(),
            Some("bad SESSDATA=<redacted>")
        );
        drop(queue);

        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        let saved_snapshot = restored.bilibili_attempt.as_ref().unwrap();
        assert_eq!(
            saved_snapshot.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(saved_snapshot.entries[0].stable_media_id, "ep-16");
        assert_eq!(
            saved_snapshot.entries[0].published_media_paths,
            vec![published_path]
        );
        assert_eq!(
            saved_snapshot.entries[0]
                .published_media_hashes
                .values()
                .next(),
            Some(&published_hash)
        );
        assert_eq!(saved_snapshot.entries[1].attempts, 2);
        assert_eq!(
            saved_snapshot.entries[2].state,
            BilibiliAttemptEntryState::Pending
        );

        let retry = queue.claim_resume(&task.id, true).unwrap().unwrap();
        assert_eq!(retry.status, TaskStatus::Preparing);
        assert_eq!(retry.generation, 1);
        assert_eq!(retry.media_entries_total, 3);
        assert_eq!(retry.media_entries_completed, 1);
        assert_eq!(retry.media_entries_failed, 0);
        let resumed = retry.bilibili_attempt.unwrap();
        assert_eq!(resumed.retry_indices, Some(vec![17]));
        assert_eq!(
            resumed.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(resumed.entries[0].published_media_paths.len(), 1);
        assert_eq!(resumed.entries[1].state, BilibiliAttemptEntryState::Pending);
        assert_eq!(resumed.entries[1].attempts, 2);
        assert_eq!(
            resumed.entries[1].error.as_deref(),
            Some("bad SESSDATA=<redacted>")
        );
        assert_eq!(resumed.entries[2].state, BilibiliAttemptEntryState::Pending);
        assert!(
            queue
                .update_bilibili_attempt_if_generation(
                    &task.id,
                    0,
                    18,
                    &BilibiliAttemptUpdate::Failed,
                )
                .unwrap()
                .is_none()
        );
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn stable_partial_attempt_is_not_reported_as_interrupted_after_restart() {
        let directory = temp_queue_root("partial-restart-summary");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("partial-restart-summary");
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();
        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, grouped_attempt())
            .unwrap()
            .unwrap();
        queue
            .update_bilibili_attempt_batch_if_generation(
                &task.id,
                0,
                &[
                    (
                        16,
                        BilibiliAttemptUpdate::Verified {
                            media_paths: vec![config.downloads.video_dir.join("episode-16.mp4")],
                            hashes: BTreeMap::new(),
                        },
                    ),
                    (
                        17,
                        BilibiliAttemptUpdate::Failure {
                            error: "episode failed".into(),
                            attempts: 1,
                        },
                    ),
                ],
            )
            .unwrap()
            .unwrap();
        let partial = queue
            .fail_if_generation(&task.id, 0, "episode failed".into())
            .unwrap()
            .unwrap();
        assert_eq!(partial.status, TaskStatus::PartiallyCompleted);
        drop(queue);

        let queue = QueueManager::open(&config).unwrap();
        let recovered = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(recovered.status, TaskStatus::PartiallyCompleted);
        assert!(recovered.status.is_unfinished());
        let summaries = queue.startup_summaries().unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].interrupted_jobs, 0);
        assert_eq!(summaries[0].partially_completed_jobs, 1);
        assert_eq!(summaries[0].completed_entries, 1);
        assert_eq!(summaries[0].failed_entries, 1);
        assert_eq!(summaries[0].remaining_entries, 1);
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn accepted_group_inventory_rebuilds_entries_by_stable_media_id() {
        let directory = temp_queue_root("grouped-inventory-reconcile");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-inventory-reconcile");
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();

        let old_plan = grouped_plan(
            "Season",
            Some("mode=all;quality=high"),
            &[(1, "Episode A", "media-a"), (2, "Episode B", "media-b")],
        );
        queue
            .set_plan_if_generation(&task.id, 0, old_plan)
            .unwrap()
            .unwrap();
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [
                (1, "Episode A".into(), Some("media-a".into())),
                (2, "Episode B".into(), Some("media-b".into())),
            ],
            0,
        );
        attempt.media_profile = Some("mode=all;quality=high".into());
        let episode_a = config.downloads.video_dir.join("episode-a.mp4");
        attempt.entry_verified_with_outputs(
            1,
            vec![episode_a.clone()],
            BTreeMap::from([(episode_a.display().to_string(), "a".repeat(64))]),
        );
        attempt.entry_failed_with_error(2, 2, "temporary media error".into());
        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, attempt)
            .unwrap()
            .unwrap();

        let proposed = grouped_plan(
            "Season revised",
            Some("mode=all;quality=high"),
            &[
                (1, "Episode B", "media-b"),
                (2, "Episode A", "media-a"),
                (3, "Episode C", "media-c"),
            ],
        );
        let differences = queue
            .set_plan_if_generation(&task.id, 0, proposed)
            .unwrap()
            .unwrap()
            .1;
        assert!(differences.contains(&"media identity"));

        let accepted = queue
            .accept_proposed_plan_for_chat_if_generation(&task.id, 0, task.chat_id)
            .unwrap()
            .unwrap();
        let snapshot = accepted.bilibili_attempt.unwrap();
        assert_eq!(snapshot.title, "Season revised");
        assert_eq!(
            snapshot.media_profile.as_deref(),
            Some("mode=all;quality=high")
        );
        assert_eq!(snapshot.retry_indices, None);
        assert_eq!(snapshot.entries.len(), 3);
        assert_eq!(snapshot.entries[0].index, 1);
        assert_eq!(snapshot.entries[0].stable_media_id, "media-b");
        assert_eq!(
            snapshot.entries[0].state,
            BilibiliAttemptEntryState::Pending
        );
        assert_eq!(snapshot.entries[0].attempts, 2);
        assert_eq!(
            snapshot.entries[0].error.as_deref(),
            Some("temporary media error")
        );
        assert_eq!(snapshot.entries[1].index, 2);
        assert_eq!(snapshot.entries[1].stable_media_id, "media-a");
        assert_eq!(
            snapshot.entries[1].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(snapshot.entries[1].published_media_paths, vec![episode_a]);
        assert_eq!(snapshot.entries[2].stable_media_id, "media-c");
        assert_eq!(
            snapshot.entries[2].state,
            BilibiliAttemptEntryState::Pending
        );
        assert!(!snapshot.entries[2].requires_redownload);
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn accepting_new_media_profile_invalidates_legacy_group_receipts() {
        let directory = temp_queue_root("grouped-profile-reconcile");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-profile-reconcile");
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();

        let old_plan = grouped_plan(
            "Season",
            None,
            &[(1, "Episode A", "media-a"), (2, "Episode B", "media-b")],
        );
        queue
            .set_plan_if_generation(&task.id, 0, old_plan)
            .unwrap()
            .unwrap();
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [
                (1, "Episode A".into(), Some("media-a".into())),
                (2, "Episode B".into(), Some("media-b".into())),
            ],
            0,
        );
        let episode_a = config.downloads.video_dir.join("episode-a.mp4");
        attempt.entry_verified_with_outputs(
            1,
            vec![episode_a],
            BTreeMap::from([("episode-a".into(), "b".repeat(64))]),
        );
        attempt.entry_started(2);
        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, attempt)
            .unwrap()
            .unwrap();

        let accepted_profile = "mode=audio-only;quality=preferred";
        let proposed = grouped_plan(
            "Season",
            Some(accepted_profile),
            &[(1, "Episode A", "media-a"), (2, "Episode B", "media-b")],
        );
        let differences = queue
            .set_plan_if_generation(&task.id, 0, proposed)
            .unwrap()
            .unwrap()
            .1;
        assert!(differences.contains(&"download requirements"));

        let accepted = queue
            .accept_proposed_plan_for_chat_if_generation(&task.id, 0, task.chat_id)
            .unwrap()
            .unwrap();
        let snapshot = accepted.bilibili_attempt.unwrap();
        assert_eq!(snapshot.media_profile.as_deref(), Some(accepted_profile));
        assert!(snapshot.entries.iter().all(|entry| {
            entry.state == BilibiliAttemptEntryState::Pending
                && entry.requires_redownload
                && entry.published_media_paths.is_empty()
                && entry.published_media_hashes.is_empty()
        }));
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn legacy_plan_validation_snapshots_default_new_group_fields() {
        let legacy = serde_json::json!({
            "stable_media_ids": ["media-a"],
            "selected_format_ids": [],
            "exact_sizes": [],
            "approximate_sizes": [],
            "resolution_codecs": [],
            "title": "Season"
        });
        let snapshot: PlanValidationSnapshot = serde_json::from_value(legacy).unwrap();
        assert_eq!(snapshot.media_profile, None);
        assert!(snapshot.bilibili_group_entries.is_empty());
    }

    #[test]
    fn plan_validation_blocks_episode_renumbering_but_not_title_changes() {
        let original = grouped_plan(
            "Original title",
            Some("mode=all;quality=high"),
            &[(16, "Episode A", "media-a"), (17, "Episode B", "media-b")],
        );
        let title_changed = grouped_plan(
            "Updated title",
            Some("mode=all;quality=high"),
            &[(16, "Updated A", "media-a"), (17, "Updated B", "media-b")],
        );
        assert!(original.blocking_differences(&title_changed).is_empty());

        let renumbered = grouped_plan(
            "Original title",
            Some("mode=all;quality=high"),
            &[(101, "Episode A", "media-a"), (102, "Episode B", "media-b")],
        );
        assert_eq!(
            original.blocking_differences(&renumbered),
            vec!["episode numbering"]
        );
    }

    #[test]
    fn accepted_group_inventory_reindexes_verified_receipt_by_stable_id() {
        let directory = temp_queue_root("grouped-inventory-reindex");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-inventory-reindex");
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();

        let media_profile = "mode=all;quality=high";
        let old_plan = grouped_plan(
            "Season",
            Some(media_profile),
            &[(16, "Episode A", "media-a"), (17, "Episode B", "media-b")],
        );
        queue
            .set_plan_if_generation(&task.id, 0, old_plan)
            .unwrap()
            .unwrap();
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [
                (16, "Episode A".into(), Some("media-a".into())),
                (17, "Episode B".into(), Some("media-b".into())),
            ],
            0,
        );
        attempt.media_profile = Some(media_profile.into());
        let published_path = config.downloads.video_dir.join("episode-a.mp4");
        attempt.entry_verified_with_outputs(
            16,
            vec![published_path.clone()],
            BTreeMap::from([(published_path.display().to_string(), "c".repeat(64))]),
        );
        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, attempt)
            .unwrap()
            .unwrap();

        let renumbered = grouped_plan(
            "Season",
            Some(media_profile),
            &[(101, "Episode A", "media-a"), (102, "Episode B", "media-b")],
        );
        let differences = queue
            .set_plan_if_generation(&task.id, 0, renumbered)
            .unwrap()
            .unwrap()
            .1;
        assert!(differences.contains(&"episode numbering"));
        let accepted = queue
            .accept_proposed_plan_for_chat_if_generation(&task.id, 0, task.chat_id)
            .unwrap()
            .unwrap();
        let snapshot = accepted.bilibili_attempt.unwrap();
        assert_eq!(snapshot.entries[0].index, 101);
        assert_eq!(snapshot.entries[0].stable_media_id, "media-a");
        assert_eq!(
            snapshot.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(
            snapshot.entries[0].published_media_paths,
            vec![published_path]
        );
        assert_eq!(snapshot.entries[1].index, 102);
        assert_eq!(snapshot.entries[1].stable_media_id, "media-b");
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn accepted_plan_and_reload_preserve_scoped_retry_and_redownload_markers() {
        let directory = temp_queue_root("grouped-scoped-retry-reconcile");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-scoped-retry-reconcile");
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();

        let old_profile = "mode=all;quality=high";
        let old_plan = grouped_plan(
            "Season",
            Some(old_profile),
            &[(1, "Episode A", "media-a"), (2, "Episode B", "media-b")],
        );
        queue
            .set_plan_if_generation(&task.id, 0, old_plan)
            .unwrap()
            .unwrap();
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [
                (1, "Episode A".into(), Some("media-a".into())),
                (2, "Episode B".into(), Some("media-b".into())),
            ],
            0,
        );
        attempt.media_profile = Some(old_profile.into());
        attempt.entry_failed_with_error(1, 2, "retry target failed".into());
        attempt.entry_verified_with_outputs(
            2,
            vec![config.downloads.video_dir.join("episode-b.mp4")],
            BTreeMap::from([("episode-b".into(), "d".repeat(64))]),
        );
        attempt.prepare_resume(true);
        assert_eq!(attempt.retry_indices, Some(vec![1]));
        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, attempt)
            .unwrap()
            .unwrap();

        let new_profile = "mode=audio-only;quality=preferred";
        let proposed = grouped_plan(
            "Season revised",
            Some(new_profile),
            &[
                (20, "Episode B", "media-b"),
                (10, "Episode A", "media-a"),
                (30, "Episode C", "media-c"),
            ],
        );
        let differences = queue
            .set_plan_if_generation(&task.id, 0, proposed)
            .unwrap()
            .unwrap()
            .1;
        assert!(differences.contains(&"episode numbering"));
        assert!(differences.contains(&"download requirements"));

        let accepted = queue
            .accept_proposed_plan_for_chat_if_generation(&task.id, 0, task.chat_id)
            .unwrap()
            .unwrap();
        let accepted_snapshot = accepted.bilibili_attempt.unwrap();
        assert_eq!(accepted_snapshot.retry_indices, Some(vec![10]));
        assert_eq!(accepted_snapshot.entries[0].index, 20);
        assert_eq!(accepted_snapshot.entries[0].stable_media_id, "media-b");
        assert_eq!(
            accepted_snapshot.entries[0].state,
            BilibiliAttemptEntryState::Pending
        );
        assert!(accepted_snapshot.entries[0].requires_redownload);
        assert!(
            accepted_snapshot.entries[0]
                .published_media_paths
                .is_empty()
        );
        assert_eq!(accepted_snapshot.entries[1].index, 10);
        assert_eq!(accepted_snapshot.entries[1].stable_media_id, "media-a");
        assert_eq!(
            accepted_snapshot.entries[1].state,
            BilibiliAttemptEntryState::Pending
        );
        assert_eq!(accepted_snapshot.entries[2].index, 30);
        assert_eq!(accepted_snapshot.entries[2].stable_media_id, "media-c");
        assert!(!accepted_snapshot.entries[2].requires_redownload);
        drop(queue);

        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(restored.status, TaskStatus::Interrupted);
        let restored_snapshot = restored.bilibili_attempt.unwrap();
        assert_eq!(restored_snapshot.retry_indices, Some(vec![10]));
        assert_eq!(
            restored_snapshot.entries[0].state,
            BilibiliAttemptEntryState::Pending
        );
        assert!(restored_snapshot.entries[0].requires_redownload);
        assert!(
            restored_snapshot.entries[0]
                .published_media_paths
                .is_empty()
        );
        assert_eq!(
            restored_snapshot.entries[1].state,
            BilibiliAttemptEntryState::Pending
        );
        assert!(restored_snapshot.entries[1].requires_redownload);
        assert_eq!(
            restored_snapshot.entries[2].state,
            BilibiliAttemptEntryState::Pending
        );
        assert!(!restored_snapshot.entries[2].requires_redownload);
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn grouped_counts_already_present_entries_once_with_verified_results() {
        let directory = temp_queue_root("grouped-already-present-counts");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-already-present-counts");
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();

        let snapshot = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [
                (16, "Episode 16".into(), Some("ep-16".into())),
                (17, "Episode 17".into(), Some("ep-17".into())),
            ],
            0,
        );
        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, snapshot)
            .unwrap()
            .unwrap();

        let skipped_path = config.downloads.video_dir.join("episode-16.mp4");
        queue
            .update_bilibili_attempt_batch_if_generation(
                &task.id,
                0,
                &[
                    (
                        16,
                        BilibiliAttemptUpdate::AlreadyPresent {
                            media_paths: vec![skipped_path.clone()],
                            hashes: BTreeMap::from([(
                                skipped_path.display().to_string(),
                                "a".repeat(64),
                            )]),
                        },
                    ),
                    (
                        17,
                        BilibiliAttemptUpdate::Failure {
                            error: "episode 17 failed".into(),
                            attempts: 1,
                        },
                    ),
                ],
            )
            .unwrap()
            .unwrap();
        let partial = queue
            .fail_if_generation(&task.id, 0, "episode 17 failed".into())
            .unwrap()
            .unwrap();

        assert_eq!(partial.status, TaskStatus::PartiallyCompleted);
        assert_eq!(partial.media_entries_total, 2);
        assert_eq!(partial.media_entries_completed, 1);
        assert_eq!(partial.media_entries_failed, 1);
        let saved = partial.bilibili_attempt.unwrap();
        assert_eq!(saved.skipped_entries, 1);
        assert!(saved.entries[0].already_present);
        assert_eq!(saved.entries[0].state, BilibiliAttemptEntryState::Verified);
        assert_eq!(saved.entries[1].state, BilibiliAttemptEntryState::Failed);

        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn grouped_single_episode_retry_preserves_other_failures_and_rejects_stale_updates() {
        let directory = temp_queue_root("grouped-episode-retry");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-episode-retry");
        task.status = TaskStatus::PartiallyCompleted;
        let mut snapshot = grouped_attempt();
        let published_path = config.downloads.video_dir.join("episode-16.mp4");
        let published_hash = "b".repeat(64);
        snapshot.entry_verified_with_outputs(
            16,
            vec![published_path.clone()],
            BTreeMap::from([(published_path.display().to_string(), published_hash.clone())]),
        );
        snapshot.entry_failed_with_error(17, 2, "failure 17".into());
        snapshot.entry_failed_with_error(18, 3, "failure 18".into());
        snapshot.finish(BilibiliAttemptOutcome::PartiallyCompleted);
        task.bilibili_attempt = Some(snapshot);
        task.media_entries_total = 3;
        task.media_entries_completed = 1;
        task.media_entries_failed = 2;
        queue.create(task.clone()).unwrap();

        let retry = queue
            .claim_episode_retry_for_chat_if_generation(&task.id, 0, task.chat_id, 17)
            .unwrap()
            .unwrap();
        assert_eq!(retry.status, TaskStatus::Preparing);
        assert_eq!(retry.generation, 1);
        assert_eq!(retry.media_entries_total, 3);
        assert_eq!(retry.media_entries_completed, 1);
        assert_eq!(retry.media_entries_failed, 1);
        let snapshot = retry.bilibili_attempt.unwrap();
        assert_eq!(snapshot.retry_indices, Some(vec![17]));
        assert_eq!(snapshot.outcome, None);
        assert_eq!(
            snapshot.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(
            snapshot.entries[0].published_media_paths,
            vec![published_path]
        );
        assert_eq!(
            snapshot.entries[0].published_media_hashes.values().next(),
            Some(&published_hash)
        );
        assert_eq!(
            snapshot.entries[1].state,
            BilibiliAttemptEntryState::Pending
        );
        assert_eq!(snapshot.entries[1].error, None);
        assert_eq!(snapshot.entries[2].state, BilibiliAttemptEntryState::Failed);
        assert_eq!(snapshot.entries[2].error.as_deref(), Some("failure 18"));
        assert!(
            queue
                .update_bilibili_attempt_if_generation(
                    &task.id,
                    0,
                    17,
                    &BilibiliAttemptUpdate::Failure {
                        error: "stale failure".into(),
                        attempts: 9,
                    },
                )
                .unwrap()
                .is_none()
        );
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn grouped_failed_only_retry_rejects_a_snapshot_without_failed_entries() {
        let directory = temp_queue_root("grouped-empty-failed-retry");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-empty-failed-retry");
        task.status = TaskStatus::PartiallyCompleted;
        let mut snapshot = grouped_attempt();
        snapshot.entry_verified(16);
        snapshot.finish(BilibiliAttemptOutcome::PartiallyCompleted);
        task.bilibili_attempt = Some(snapshot);
        queue.create(task.clone()).unwrap();

        assert!(queue.claim_resume(&task.id, true).unwrap().is_none());
        let current = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(current.generation, 0);
        assert_eq!(current.status, TaskStatus::PartiallyCompleted);
        assert_eq!(current.bilibili_attempt.unwrap().retry_indices, None);
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failed_group_with_pending_entries_can_resume_without_verified_entries() {
        let directory = temp_queue_root("grouped-failed-resume-pending");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let task = bilibili_task("grouped-failed-resume-pending");
        queue.create(task.clone()).unwrap();

        let mut snapshot = grouped_attempt();
        snapshot.entry_failed_with_error(16, 1, "episode 16 failed".into());
        snapshot.finish(BilibiliAttemptOutcome::Failed);
        let failed = queue
            .fail_with_bilibili_attempt_if_generation(
                &task.id,
                0,
                "episode retry failed".into(),
                Some(snapshot),
            )
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, TaskStatus::Failed);
        let before = failed.bilibili_attempt.as_ref().unwrap();
        assert_eq!(before.verified_entries(), 0);
        assert_eq!(before.entries[0].state, BilibiliAttemptEntryState::Failed);
        assert_eq!(before.entries[1].state, BilibiliAttemptEntryState::Pending);

        let resumed = queue.claim_resume(&task.id, false).unwrap().unwrap();
        assert_eq!(resumed.status, TaskStatus::Preparing);
        assert_eq!(resumed.generation, 1);
        assert_eq!(
            resumed.bilibili_attempt.as_ref().unwrap().retry_indices,
            None
        );
        assert_eq!(
            resumed.bilibili_attempt.as_ref().unwrap().entries[0].state,
            BilibiliAttemptEntryState::Pending
        );
        assert_eq!(
            resumed.bilibili_attempt.as_ref().unwrap().entries[1].state,
            BilibiliAttemptEntryState::Pending
        );
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn legacy_failed_task_cannot_use_normal_resume() {
        let directory = temp_queue_root("legacy-failed-normal-resume");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("legacy-failed-normal-resume");
        task.status = TaskStatus::Failed;
        task.error = Some("legacy failure".into());
        let mut snapshot =
            BilibiliAttemptSnapshot::new("Legacy season".into(), [(1, "Legacy episode".into())], 0);
        snapshot.finish(BilibiliAttemptOutcome::Failed);
        task.bilibili_attempt = Some(snapshot);
        queue.create(task.clone()).unwrap();

        assert!(queue.claim_resume(&task.id, false).unwrap().is_none());
        let current = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(current.status, TaskStatus::Failed);
        assert_eq!(current.generation, 0);
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn grouped_completion_requires_every_selected_entry_to_be_verified() {
        let directory = temp_queue_root("grouped-completion-guard");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-completion-guard");
        task.status = TaskStatus::Verifying;
        let mut snapshot = grouped_attempt();
        snapshot.entry_verified(16);
        task.bilibili_attempt = Some(snapshot);
        queue.create(task.clone()).unwrap();

        let error = queue
            .complete(&task.id, "videos".into(), &[], BTreeMap::new())
            .unwrap_err();
        assert!(error.to_string().contains("not individually verified"));
        let current = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(current.status, TaskStatus::Verifying);
        let snapshot = current.bilibili_attempt.unwrap();
        assert_eq!(
            snapshot.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(
            snapshot.entries[1].state,
            BilibiliAttemptEntryState::Pending
        );
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn grouped_interruption_resume_resets_unfinished_entries_but_keeps_verified_outputs() {
        let directory = temp_queue_root("grouped-interruption-resume");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-interruption-resume");
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();
        let mut snapshot = grouped_attempt();
        let published_path = config.downloads.video_dir.join("episode-16.mp4");
        snapshot.entry_verified_with_outputs(
            16,
            vec![published_path.clone()],
            BTreeMap::from([(published_path.display().to_string(), "c".repeat(64))]),
        );
        snapshot.retry_indices = Some(vec![17]);
        snapshot.entry_failed_with_error(17, 1, "episode failed".into());
        snapshot.entry_started(18);
        let interrupted = queue
            .interrupt_with_bilibili_attempt_if_generation(
                &task.id,
                0,
                "Bilibili login is required".into(),
                Some(snapshot),
            )
            .unwrap()
            .unwrap();
        assert_eq!(interrupted.status, TaskStatus::Interrupted);
        assert_eq!(interrupted.media_entries_total, 3);
        assert_eq!(interrupted.media_entries_completed, 1);
        assert_eq!(interrupted.media_entries_failed, 1);
        drop(queue);

        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(restored.status, TaskStatus::Interrupted);
        assert_eq!(
            restored.bilibili_attempt.as_ref().unwrap().retry_indices,
            Some(vec![17])
        );
        let resume = queue.claim_resume(&task.id, false).unwrap().unwrap();
        assert_eq!(resume.generation, 1);
        assert_eq!(resume.status, TaskStatus::Preparing);
        let snapshot = resume.bilibili_attempt.unwrap();
        assert_eq!(snapshot.retry_indices, None);
        assert_eq!(
            snapshot.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(
            snapshot.entries[0].published_media_paths,
            vec![published_path]
        );
        assert_eq!(
            snapshot.entries[1].state,
            BilibiliAttemptEntryState::Pending
        );
        assert_eq!(snapshot.entries[1].error.as_deref(), Some("episode failed"));
        assert_eq!(
            snapshot.entries[2].state,
            BilibiliAttemptEntryState::Pending
        );
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn bilibili_attempt_persists_partial_results_and_retry_resets_only_current_generation() {
        use crate::router::BilibiliSelection;
        let directory = temp_queue_root("episode-results");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = directory.join("videos");
        config.downloads.pdf_dir = directory.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        let queue = QueueManager::open(&config).unwrap();
        let mut task = test_task(
            "episode-results",
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".into(),
                selection: Some(BilibiliSelection::All),
            },
        );
        task.status = TaskStatus::Running;
        task.media_entries_completed = 2;
        queue.create(task.clone()).unwrap();
        let snapshot = BilibiliAttemptSnapshot::new(
            "Season".into(),
            [
                (16, "Episode 16".into()),
                (17, "Episode 17".into()),
                (18, "Episode 18".into()),
            ],
            200,
        );
        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, snapshot.clone())
            .unwrap()
            .unwrap();
        let initial = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(initial.media_entries_completed, 200);
        queue
            .update_bilibili_attempt_if_generation(
                &task.id,
                0,
                16,
                &BilibiliAttemptUpdate::Completed,
            )
            .unwrap()
            .unwrap();
        queue
            .update_bilibili_attempt_if_generation(&task.id, 0, 17, &BilibiliAttemptUpdate::Started)
            .unwrap()
            .unwrap();
        queue
            .update_bilibili_attempt_if_generation(
                &task.id,
                0,
                17,
                &BilibiliAttemptUpdate::Stage {
                    stage: "danmaku".into(),
                },
            )
            .unwrap()
            .unwrap();
        let before_legacy_collection_event = queue.get(&task.id).unwrap().unwrap();
        let interleaved = queue
            .set_collection_progress_if_generation(&task.id, 0, 203, 203, 0)
            .unwrap()
            .unwrap();
        assert_eq!(
            interleaved.revision,
            before_legacy_collection_event.revision
        );
        assert_eq!(interleaved.media_entries_total, 203);
        assert_eq!(interleaved.media_entries_completed, 200);
        assert!(
            queue
                .set_bilibili_attempt_if_generation(&task.id, 1, snapshot)
                .unwrap()
                .is_none()
        );
        let failed_record = queue
            .fail_if_generation(&task.id, 0, "HTTP 403".into())
            .unwrap()
            .unwrap();
        assert_eq!(failed_record.status, TaskStatus::Failed);
        let failed = failed_record.bilibili_attempt.clone().unwrap();
        assert_eq!(failed.outcome, Some(BilibiliAttemptOutcome::Failed));
        assert_eq!(failed_record.media_entries_completed, 200);
        assert_eq!(failed_record.media_entries_failed, 1);
        drop(queue);
        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(restored.bilibili_attempt, Some(failed));
        assert_eq!(restored.media_entries_total, 203);
        assert_eq!(
            restored.media_entries_completed, 200,
            "downloaded streams are not verified published videos"
        );
        assert_eq!(restored.media_entries_failed, 1);
        let retry = queue.claim_resume(&task.id, true).unwrap().unwrap();
        assert_eq!(retry.generation, 1);
        assert!(retry.bilibili_attempt.is_none());
        assert_eq!(retry.media_entries_failed, 0);
        assert!(
            queue
                .update_bilibili_attempt_if_generation(
                    &task.id,
                    0,
                    18,
                    &BilibiliAttemptUpdate::Failed
                )
                .unwrap()
                .is_none()
        );
        assert!(
            queue
                .update_bilibili_attempt_batch_if_generation(
                    &task.id,
                    0,
                    &[(18, BilibiliAttemptUpdate::Failed)],
                )
                .unwrap()
                .is_none()
        );
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn bilibili_attempt_failure_persists_latest_snapshot_atomically() {
        use crate::router::BilibiliSelection;
        let directory = temp_queue_root("episode-results-atomic-failure");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = directory.join("videos");
        config.downloads.pdf_dir = directory.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        let queue = QueueManager::open(&config).unwrap();
        let mut task = test_task(
            "episode-atomic-failure",
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".into(),
                selection: Some(BilibiliSelection::All),
            },
        );
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();

        // This is the latest in-memory progress state; the event forwarder may not have saved it yet.
        let mut snapshot = BilibiliAttemptSnapshot::new(
            "Season".into(),
            [(16, "Episode 16".into()), (17, "Episode 17".into())],
            2,
        );
        snapshot.entry_completed(16);
        snapshot.entry_started(17);
        let failed = queue
            .fail_with_bilibili_attempt_if_generation(
                &task.id,
                0,
                "HTTP 403".into(),
                Some(snapshot),
            )
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, TaskStatus::Failed);
        assert_eq!(failed.media_entries_total, 4);
        assert_eq!(failed.media_entries_completed, 2);
        assert_eq!(failed.media_entries_failed, 1);
        let attempt = failed.bilibili_attempt.unwrap();
        assert_eq!(attempt.outcome, Some(BilibiliAttemptOutcome::Failed));
        assert_eq!(attempt.downloaded_entries(), 1);
        assert_eq!(attempt.failed_entries(), 1);

        assert!(
            queue
                .fail_with_bilibili_attempt_if_generation(&task.id, 1, "stale".into(), None)
                .unwrap()
                .is_none()
        );
        drop(queue);

        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(restored.status, TaskStatus::Failed);
        assert_eq!(
            restored.bilibili_attempt.unwrap().outcome,
            Some(BilibiliAttemptOutcome::Failed)
        );
        assert_eq!(restored.media_entries_completed, 2);
        assert_eq!(restored.media_entries_failed, 1);
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn bilibili_attempt_cancel_restart_keeps_downloaded_entries_out_of_completed_count() {
        use crate::router::BilibiliSelection;
        let directory = temp_queue_root("episode-results-cancel-restart");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = directory.join("videos");
        config.downloads.pdf_dir = directory.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        let queue = QueueManager::open(&config).unwrap();
        let mut task = test_task(
            "episode-cancel-restart",
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".into(),
                selection: Some(BilibiliSelection::All),
            },
        );
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();
        let snapshot = BilibiliAttemptSnapshot::new(
            "Season".into(),
            [
                (16, "Episode 16".into()),
                (17, "Episode 17".into()),
                (18, "Episode 18".into()),
            ],
            1,
        );
        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, snapshot)
            .unwrap()
            .unwrap();
        queue
            .update_bilibili_attempt_batch_if_generation(
                &task.id,
                0,
                &[
                    (16, BilibiliAttemptUpdate::Completed),
                    (17, BilibiliAttemptUpdate::Started),
                ],
            )
            .unwrap()
            .unwrap();
        let before_legacy_event = queue.get(&task.id).unwrap().unwrap();
        let after_legacy_event = queue
            .set_collection_progress_if_generation(&task.id, 0, 4, 4, 0)
            .unwrap()
            .unwrap();
        assert_eq!(after_legacy_event.revision, before_legacy_event.revision);
        assert_eq!(after_legacy_event.media_entries_total, 4);
        assert_eq!(after_legacy_event.media_entries_completed, 1);
        let cancellation_requested = queue
            .cancel_if_generation(&task.id, 0, task.chat_id)
            .unwrap()
            .unwrap();
        assert_eq!(cancellation_requested.status, TaskStatus::Running);
        assert!(cancellation_requested.cancel_requested);
        let mut fresh_snapshot = BilibiliAttemptSnapshot::new(
            "Season".into(),
            [
                (16, "Episode 16".into()),
                (17, "Episode 17".into()),
                (18, "Episode 18".into()),
            ],
            1,
        );
        fresh_snapshot.entry_completed(16);
        fresh_snapshot.entry_started(17);
        assert!(
            queue
                .finish_cancellation_with_bilibili_attempt_if_generation(
                    &task.id,
                    1,
                    Some(fresh_snapshot.clone()),
                )
                .unwrap()
                .is_none()
        );
        let cancelled = queue
            .finish_cancellation_with_bilibili_attempt_if_generation(
                &task.id,
                0,
                Some(fresh_snapshot.clone()),
            )
            .unwrap()
            .unwrap();
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        assert_eq!(
            cancelled.bilibili_attempt.as_ref().unwrap().outcome,
            Some(BilibiliAttemptOutcome::Cancelled)
        );
        assert_eq!(cancelled.media_entries_completed, 1);
        assert_eq!(cancelled.media_entries_failed, 0);
        // A lifecycle snapshot already in flight after cancellation must retain the terminal outcome.
        let delayed_snapshot = queue
            .set_bilibili_attempt_if_generation(&task.id, 0, fresh_snapshot)
            .unwrap()
            .unwrap();
        assert_eq!(delayed_snapshot.status, TaskStatus::Cancelled);
        assert_eq!(
            delayed_snapshot.bilibili_attempt.as_ref().unwrap().outcome,
            Some(BilibiliAttemptOutcome::Cancelled)
        );
        drop(queue);

        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        let attempt = restored.bilibili_attempt.unwrap();
        assert_eq!(restored.status, TaskStatus::Cancelled);
        assert_eq!(attempt.outcome, Some(BilibiliAttemptOutcome::Cancelled));
        assert_eq!(attempt.downloaded_entries(), 1);
        assert_eq!(attempt.failed_entries(), 0);
        assert_eq!(restored.media_entries_total, 4);
        assert_eq!(restored.media_entries_completed, 1);
        assert_eq!(restored.media_entries_failed, 0);

        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn parent_cancel_overrides_guard_interruption_and_survives_late_snapshot_and_reload() {
        use crate::router::BilibiliSelection;
        let directory = temp_queue_root("episode-group-cancel-after-interrupt");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = directory.join("videos");
        config.downloads.pdf_dir = directory.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        let queue = QueueManager::open(&config).unwrap();
        let mut task = test_task(
            "episode-group-cancel-after-interrupt",
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".into(),
                selection: Some(BilibiliSelection::All),
            },
        );
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();

        // The group guard's Drop records Interrupted before the parent cancellation is finalized.
        let mut guard_snapshot = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [
                (16, "Episode 16".into(), Some("ep-16".into())),
                (17, "Episode 17".into(), Some("ep-17".into())),
                (18, "Episode 18".into(), Some("ep-18".into())),
            ],
            0,
        );
        guard_snapshot.entry_verified(16);
        guard_snapshot.entry_started(17);
        guard_snapshot.finish(BilibiliAttemptOutcome::Interrupted);
        assert_eq!(
            guard_snapshot.outcome,
            Some(BilibiliAttemptOutcome::Interrupted)
        );
        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, guard_snapshot.clone())
            .unwrap()
            .unwrap();

        let requested = queue
            .cancel_if_generation(&task.id, 0, task.chat_id)
            .unwrap()
            .unwrap();
        assert!(requested.cancel_requested);
        let cancelled = queue
            .finish_cancellation_with_bilibili_attempt_if_generation(&task.id, 0, None)
            .unwrap()
            .unwrap();
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        assert_eq!(
            cancelled.bilibili_attempt.as_ref().unwrap().outcome,
            Some(BilibiliAttemptOutcome::Cancelled)
        );

        // An in-flight guard snapshot arriving after cancellation must keep the parent result.
        let delayed = queue
            .set_bilibili_attempt_if_generation(&task.id, 0, guard_snapshot)
            .unwrap()
            .unwrap();
        assert_eq!(delayed.status, TaskStatus::Cancelled);
        assert_eq!(
            delayed.bilibili_attempt.as_ref().unwrap().outcome,
            Some(BilibiliAttemptOutcome::Cancelled)
        );
        assert!(
            delayed
                .bilibili_attempt
                .as_ref()
                .unwrap()
                .summary()
                .contains("Outcome: cancelled")
        );

        drop(queue);
        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(restored.status, TaskStatus::Cancelled);
        let attempt = restored.bilibili_attempt.unwrap();
        assert_eq!(attempt.outcome, Some(BilibiliAttemptOutcome::Cancelled));
        assert_eq!(attempt.verified_entries(), 1);
        for state in [
            BilibiliAttemptEntryState::Interrupted,
            BilibiliAttemptEntryState::Pending,
        ] {
            assert_eq!(
                attempt
                    .entries
                    .iter()
                    .filter(|entry| entry.state == state)
                    .count(),
                1
            );
        }
        assert!(attempt.summary().contains("Outcome: cancelled"));
        assert!(!attempt.summary().contains("Outcome: interrupted"));

        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn parent_partial_finalization_replaces_group_verified_outcome_and_reloads() {
        use crate::router::BilibiliSelection;
        let directory = temp_queue_root("episode-group-finalization-partial");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = directory.join("videos");
        config.downloads.pdf_dir = directory.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        let queue = QueueManager::open(&config).unwrap();
        let mut task = test_task(
            "episode-group-finalization-partial",
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".into(),
                selection: Some(BilibiliSelection::All),
            },
        );
        task.status = TaskStatus::Running;
        let mut snapshot = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [(16, "Episode 16".into(), Some("ep-16".into()))],
            0,
        );
        snapshot.entry_verified(16);
        snapshot.finish(BilibiliAttemptOutcome::Verified);
        task.bilibili_attempt = Some(snapshot);
        queue.create(task.clone()).unwrap();

        let partial = queue
            .fail_with_bilibili_attempt_if_generation(
                &task.id,
                0,
                "final hash validation failed".into(),
                None,
            )
            .unwrap()
            .unwrap();
        assert_eq!(partial.status, TaskStatus::PartiallyCompleted);
        assert_eq!(
            partial.bilibili_attempt.as_ref().unwrap().outcome,
            Some(BilibiliAttemptOutcome::PartiallyCompleted)
        );
        assert!(
            partial
                .bilibili_attempt
                .as_ref()
                .unwrap()
                .summary()
                .contains("Outcome: partially completed")
        );

        drop(queue);
        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(restored.status, TaskStatus::PartiallyCompleted);
        let attempt = restored.bilibili_attempt.unwrap();
        assert_eq!(
            attempt.outcome,
            Some(BilibiliAttemptOutcome::PartiallyCompleted)
        );
        assert_eq!(attempt.verified_entries(), 1);
        assert!(attempt.summary().contains("Outcome: partially completed"));

        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn restart_replaces_group_verified_outcome_with_interrupted_summary() {
        use crate::router::BilibiliSelection;
        let directory = temp_queue_root("episode-group-finalization-restart");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = directory.join("videos");
        config.downloads.pdf_dir = directory.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        let queue = QueueManager::open(&config).unwrap();
        let mut task = test_task(
            "episode-group-finalization-restart",
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".into(),
                selection: Some(BilibiliSelection::All),
            },
        );
        task.status = TaskStatus::Running;
        let mut snapshot = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [(16, "Episode 16".into(), Some("ep-16".into()))],
            0,
        );
        snapshot.entry_verified(16);
        snapshot.finish(BilibiliAttemptOutcome::Verified);
        task.bilibili_attempt = Some(snapshot);
        queue.create(task.clone()).unwrap();
        drop(queue);

        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(restored.status, TaskStatus::Interrupted);
        let attempt = restored.bilibili_attempt.unwrap();
        assert_eq!(attempt.outcome, Some(BilibiliAttemptOutcome::Interrupted));
        assert_eq!(attempt.verified_entries(), 1);
        assert!(attempt.summary().contains("Outcome: interrupted"));
        assert!(!attempt.summary().contains("Outcome: verified"));

        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn bilibili_attempt_restart_preserves_downloaded_entries_without_reporting_them_verified() {
        let directory = temp_queue_root("episode-results-restart");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = directory.join("videos");
        config.downloads.pdf_dir = directory.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        let queue = QueueManager::open(&config).unwrap();
        let mut task = test_task(
            "episode-restart",
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".into(),
                selection: Some(crate::router::BilibiliSelection::All),
            },
        );
        task.status = TaskStatus::Running;
        let mut snapshot = BilibiliAttemptSnapshot::new(
            "Season".into(),
            [(16, "Episode 16".into()), (17, "Episode 17".into())],
            0,
        );
        snapshot.entry_completed(16);
        snapshot.entry_started(17);
        task.bilibili_attempt = Some(snapshot);
        queue.create(task.clone()).unwrap();
        drop(queue);
        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(restored.status, TaskStatus::Interrupted);
        let snapshot = restored.bilibili_attempt.unwrap();
        assert_eq!(snapshot.outcome, Some(BilibiliAttemptOutcome::Interrupted));
        assert_eq!(snapshot.downloaded_entries(), 1);
        assert_eq!(snapshot.failed_entries(), 0);
        assert!(snapshot.summary().contains("interrupted: 1"));
        assert!(snapshot.summary().contains("verified: 0"));
        let mut value = serde_json::to_value(task).unwrap();
        value.as_object_mut().unwrap().remove("bilibili_attempt");
        assert!(
            serde_json::from_value::<TaskRecord>(value)
                .unwrap()
                .bilibili_attempt
                .is_none()
        );
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn hashing_expected_media_accepts_directory_churn_and_rejects_replacement() {
        let directory = temp_queue_root("hash-expected-replacement");
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("episode.mp4");
        fs::write(&path, b"original episode").unwrap();
        let root = RootedFs::new(&directory).unwrap();
        let original = root.open_bound_file(&path).unwrap().unwrap();
        let expected = BTreeMap::from([(path.clone(), original.identity())]);
        fs::write(directory.join("unrelated.nfo"), b"directory churn").unwrap();
        let paths = vec![path.clone()];
        let hashes =
            hash_primary_media_with_expected_identities(&directory, &paths, &expected).unwrap();
        assert_eq!(hashes, hash_primary_media(&directory, &paths).unwrap());

        fs::rename(&path, directory.join("original.mp4")).unwrap();
        fs::write(&path, b"replacement episode").unwrap();
        let error =
            hash_primary_media_with_expected_identities(&directory, &paths, &expected).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("identity changed before hashing")
        );
        fs::remove_file(&path).unwrap();
        let error =
            hash_primary_media_with_expected_identities(&directory, &paths, &expected).unwrap_err();
        assert!(error.to_string().contains("disappeared before hashing"));
        fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn hashing_expected_media_rejects_unreported_evidence() {
        let directory = temp_queue_root("hash-unreported-evidence");
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("episode.mp4");
        fs::write(&path, b"episode").unwrap();
        let root = RootedFs::new(&directory).unwrap();
        let media = root.open_bound_file(&path).unwrap().unwrap();
        let expected = BTreeMap::from([(path, media.identity())]);
        let error =
            hash_primary_media_with_expected_identities(&directory, &[], &expected).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("not included in the published report")
        );
        fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn hashing_skipped_worker_media_under_a_symlink_configured_root() {
        let directory = temp_queue_root("hash-worker-root-alias");
        let physical = directory.join("physical");
        let logical = directory.join("configured");
        fs::create_dir_all(&physical).unwrap();
        symlink(&physical, &logical).unwrap();
        let path = logical.join("episode.mp4");
        fs::write(&path, b"existing episode").unwrap();
        let root = RootedFs::new(&logical).unwrap();
        let identity = root.open_bound_file(&path).unwrap().unwrap().identity();
        let report: crate::downloader::JobReport = serde_json::from_value(serde_json::json!({
            "saved_location": path,
            "details": "existing season entry",
            "primary_media_paths": [path],
            "existing_bilibili_evidence": {
                "skipped": [{
                    "path": root.root_path().join("episode.mp4"),
                    "device": identity.device(),
                    "inode": identity.inode(),
                    "sidecars": [],
                    "cid": 203,
                    "epid": 247472,
                    "content_stamp": {
                        "size": 16,
                        "modified_seconds": 0,
                        "modified_nanoseconds": 0,
                        "changed_seconds": 0,
                        "changed_nanoseconds": 0
                    }
                }]
            }
        }))
        .unwrap();
        let expected =
            crate::downloader::existing_bilibili_report_media_identities(&root, &report).unwrap();
        assert_eq!(expected.get(&path), Some(&identity));
        let hashes = hash_primary_media_with_expected_identities(
            &logical,
            &report.primary_media_paths,
            &expected,
        )
        .unwrap();
        assert_eq!(hashes, hash_primary_media(&logical, &[path]).unwrap());
        fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn missing_season_selection_survives_queue_reopen_and_stale_choice() {
        use crate::router::BilibiliSelection;

        let temp_root = temp_queue_root("missing-season-choice");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = temp_root.join("videos");
        config.downloads.pdf_dir = temp_root.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        let queue = QueueManager::open(&config).unwrap();
        let mut task = test_task(
            "missing-season",
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".to_string(),
                selection: Some(BilibiliSelection::All),
            },
        );
        task.status = TaskStatus::AwaitingDuplicateChoice;
        queue.create(task.clone()).unwrap();
        let JobRequest::Bilibili { url, .. } = &task.job else {
            unreachable!()
        };
        let missing_job = JobRequest::Bilibili {
            url: url.clone(),
            selection: Some(BilibiliSelection::Missing),
        };
        let saved = queue
            .update_job_if_generation(
                &task.id,
                task.generation,
                &[TaskStatus::AwaitingDuplicateChoice],
                missing_job.clone(),
                TaskStatus::Queued,
            )
            .unwrap()
            .expect("current skip decision should persist atomically");
        assert_eq!(saved.job, missing_job);
        assert!(
            queue
                .update_job_if_generation(
                    &task.id,
                    task.generation,
                    &[TaskStatus::AwaitingDuplicateChoice],
                    task.job,
                    TaskStatus::Queued,
                )
                .unwrap()
                .is_none(),
            "repeated choice must not reset skipping"
        );
        drop(queue);
        let reopened = QueueManager::open(&config).unwrap();
        let restored = reopened.get(&task.id).unwrap().unwrap();
        assert_eq!(restored.job, missing_job);
        assert!(!restored.cancel_requested);
        drop(reopened);
        fs::remove_dir_all(temp_root).unwrap();
    }

    #[test]
    fn stale_cancellation_cleanup_preserves_replacement_token() {
        let temp_root = temp_queue_root("cancellation-cleanup-identity");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-cancellation-cleanup-identity";

        let old_token = queue
            .register_cancellation(id)
            .expect("first cancellation token should register");
        let current_token = queue
            .register_cancellation(id)
            .expect("replacement cancellation token should register");

        queue
            .unregister_cancellation_if_current(id, &old_token)
            .expect("stale cleanup should succeed without removing the current token");
        {
            let active = queue
                .cancellations
                .lock()
                .expect("cancellation map should remain available");
            let registered = active
                .get(id)
                .expect("replacement token should remain registered");
            assert!(Arc::ptr_eq(&registered.notify, &current_token));
        }

        queue
            .unregister_cancellation_if_current(id, &current_token)
            .expect("current cleanup should succeed");
        assert!(
            !queue
                .cancellations
                .lock()
                .expect("cancellation map should remain available")
                .contains_key(id)
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn cancellation_registration_requires_current_queued_generation() {
        let temp_root = temp_queue_root("queued-generation-cancellation-registration");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-queued-generation-cancellation-registration";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=registration".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Queued, None)
            .expect("task should queue");

        let previous = queue
            .register_cancellation(id)
            .expect("existing token should register");
        assert!(
            queue
                .register_cancellation_if_queued_generation(id, 1)
                .expect("stale generation should be checked")
                .is_none()
        );
        {
            let active = queue
                .cancellations
                .lock()
                .expect("cancellation map should remain available");
            assert!(Arc::ptr_eq(
                &active.get(id).expect("existing token should remain").notify,
                &previous
            ));
        }

        let (record, current) = queue
            .register_cancellation_if_queued_generation(id, 0)
            .expect("current queued generation should register")
            .expect("current queued task should remain available");
        assert_eq!(record.status, TaskStatus::Queued);
        assert_eq!(record.generation, 0);
        let (same_record, repeated) = queue
            .register_cancellation_if_queued_generation(id, 0)
            .expect("repeated registration should succeed")
            .expect("queued task should remain available");
        assert_eq!(same_record.generation, 0);
        assert!(Arc::ptr_eq(&current, &repeated));

        queue
            .set_status(id, TaskStatus::Cancelled, None)
            .expect("task should cancel");
        assert!(
            queue
                .register_cancellation_if_queued_generation(id, 0)
                .expect("terminal status should be checked")
                .is_none()
        );
        {
            let active = queue
                .cancellations
                .lock()
                .expect("cancellation map should remain available");
            assert!(Arc::ptr_eq(
                &active.get(id).expect("current token should remain").notify,
                &current
            ));
        }

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn cancellation_registration_preserves_file_provider_read_errors() {
        let temp_root = temp_queue_root("queued-generation-cancellation-read-error");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let provider = Arc::new(MockQueueFileProvider::default());
        let queue = QueueManager::open_with_file_provider(&config, provider.clone())
            .expect("task queue should open");
        let id = "task-queued-generation-cancellation-read-error";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=read-error".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Queued, None)
            .expect("task should queue");
        provider.fail_next_read("registration read failed");

        let error = match queue.register_cancellation_if_queued_generation(id, 0) {
            Ok(_) => panic!("file provider read failure should propagate"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("registration read failed"));
        assert!(
            queue
                .cancellations
                .lock()
                .expect("cancellation map should remain available")
                .get(id)
                .is_none()
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn callback_transitions_require_current_generation_and_chat() {
        let temp_root = temp_queue_root("generation-guarded-callbacks");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-generation-guarded-callbacks";
        let chat_id = 123_456_789;
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=callback-guard".to_string(),
                    },
                ))
                .expect("task should persist")
        );

        assert!(
            queue
                .claim_resume_for_chat_if_generation(id, 1, chat_id, false)
                .expect("stale resume generation should be checked")
                .is_none()
        );
        assert!(
            queue
                .claim_resume_for_chat_if_generation(id, 0, chat_id + 1, false)
                .expect("foreign chat should be checked")
                .is_none()
        );
        let resumed = queue
            .claim_resume_for_chat_if_generation(id, 0, chat_id, false)
            .expect("current resume callback should be checked")
            .expect("current generation and chat should claim resume");
        assert_eq!(resumed.generation, 1);
        assert_eq!(resumed.status, TaskStatus::Preparing);
        assert!(
            queue
                .claim_resume_for_chat_if_generation(id, 0, chat_id, false)
                .expect("stale resumed generation should be checked")
                .is_none()
        );

        queue
            .set_status(id, TaskStatus::Running, None)
            .expect("resumed task should run");
        let original_plan = PlanValidationSnapshot {
            stable_media_ids: vec!["original".to_string()],
            ..PlanValidationSnapshot::default()
        };
        queue
            .set_plan_if_generation(id, 1, original_plan)
            .expect("initial validation should succeed")
            .expect("current task should accept initial validation");
        let proposed_plan = PlanValidationSnapshot {
            stable_media_ids: vec!["changed".to_string()],
            ..PlanValidationSnapshot::default()
        };
        queue
            .set_plan_if_generation(id, 1, proposed_plan)
            .expect("proposed validation should succeed")
            .expect("current task should propose changed plan");
        assert!(
            queue
                .accept_proposed_plan_for_chat_if_generation(id, 0, chat_id)
                .expect("stale confirmation generation should be checked")
                .is_none()
        );
        assert!(
            queue
                .accept_proposed_plan_for_chat_if_generation(id, 1, chat_id + 1)
                .expect("foreign confirmation chat should be checked")
                .is_none()
        );
        assert_eq!(
            queue
                .get(id)
                .expect("task should remain readable")
                .unwrap()
                .status,
            TaskStatus::AwaitingConfirmation
        );
        let confirmed = queue
            .accept_proposed_plan_for_chat_if_generation(id, 1, chat_id)
            .expect("current confirmation callback should be checked")
            .expect("current generation and chat should accept the plan");
        assert_eq!(confirmed.status, TaskStatus::Preparing);

        assert!(
            queue
                .cancel_for_chat_if_generation(id, 0, chat_id)
                .expect("stale cancel generation should be checked")
                .is_none()
        );
        assert!(
            queue
                .cancel_for_chat_if_generation(id, 1, chat_id + 1)
                .expect("foreign cancel chat should be checked")
                .is_none()
        );
        assert_eq!(
            queue
                .get(id)
                .expect("task should remain readable")
                .unwrap()
                .status,
            TaskStatus::Preparing
        );
        let cancelled = queue
            .cancel_for_chat_if_generation(id, 1, chat_id)
            .expect("current cancel callback should be checked")
            .expect("current generation and chat should cancel the task");
        assert_eq!(cancelled.status, TaskStatus::Cancelled);

        let retry_id = "task-generation-guarded-callback-retry";
        assert!(
            queue
                .create(test_task(
                    retry_id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=callback-retry".to_string(),
                    },
                ))
                .expect("retry task should persist")
        );
        queue
            .set_status(retry_id, TaskStatus::Failed, Some("retry me".to_string()))
            .expect("retry task should fail");
        assert!(
            queue
                .claim_resume_for_chat_if_generation(retry_id, 0, chat_id + 1, true)
                .expect("foreign retry chat should be checked")
                .is_none()
        );
        assert!(
            queue
                .claim_resume_for_chat_if_generation(retry_id, 1, chat_id, true)
                .expect("stale retry generation should be checked")
                .is_none()
        );
        let retried = queue
            .claim_resume_for_chat_if_generation(retry_id, 0, chat_id, true)
            .expect("current retry callback should be checked")
            .expect("current generation and chat should claim retry");
        assert_eq!(retried.generation, 1);
        assert_eq!(retried.status, TaskStatus::Preparing);

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn accepting_plan_retires_old_token_before_next_queued_run() {
        let temp_root = temp_queue_root("confirmed-plan-cancellation-token");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let file_provider = Arc::new(MockQueueFileProvider::default());
        let queue = QueueManager::open_with_file_provider(&config, file_provider)
            .expect("task queue should open");
        let id = "task-confirmed-plan-cancellation-token";
        let chat_id = 123_456_789;
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=confirmed-plan-token".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Queued, None)
            .expect("task should queue for its first run");
        let (_, old_token) = queue
            .register_cancellation_if_queued_generation(id, 0)
            .expect("first-run cancellation token should register")
            .expect("task should still be queued");
        queue
            .begin_run_if_generation(id, 0)
            .expect("first run should begin")
            .expect("generation zero should be current");
        queue
            .set_plan_if_generation(
                id,
                0,
                PlanValidationSnapshot {
                    stable_media_ids: vec!["old-plan".to_string()],
                    ..PlanValidationSnapshot::default()
                },
            )
            .expect("initial plan should validate")
            .expect("task should remain current");
        queue
            .set_plan_if_generation(
                id,
                0,
                PlanValidationSnapshot {
                    stable_media_ids: vec!["new-plan".to_string()],
                    ..PlanValidationSnapshot::default()
                },
            )
            .expect("changed plan should be recorded")
            .expect("task should await plan confirmation");

        queue
            .accept_proposed_plan_for_chat_if_generation(id, 0, chat_id)
            .expect("plan confirmation should persist")
            .expect("current plan should be accepted");
        queue
            .set_status_if_generation(id, 0, &[TaskStatus::Preparing], TaskStatus::Queued)
            .expect("confirmed task should queue for its next run")
            .expect("same generation should still be current");
        let (_, new_token) = queue
            .register_cancellation_if_queued_generation(id, 0)
            .expect("next-run token should register")
            .expect("confirmed task should be queued");
        assert!(!Arc::ptr_eq(&old_token, &new_token));

        queue
            .unregister_cancellation_if_current(id, &old_token)
            .expect("old worker cleanup should complete");
        {
            let active = queue
                .cancellations
                .lock()
                .expect("cancellation map should remain available");
            assert!(Arc::ptr_eq(
                &active
                    .get(id)
                    .expect("new worker cancellation token should remain registered")
                    .notify,
                &new_token
            ));
        }

        let cancelled = queue
            .cancel_for_chat_if_generation(id, 0, chat_id)
            .expect("cancellation should persist")
            .expect("current queued generation should be cancellable");
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        tokio::time::timeout(Duration::from_millis(100), new_token.notified())
            .await
            .expect("cancellation should notify the new worker token");

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn cancellation_notifies_the_captured_token_after_generation_replacement() {
        let temp_root = temp_queue_root("cancellation-token-generation-interleaving");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-cancellation-token-generation-interleaving";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=token-interleaving".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Queued, None)
            .expect("task should queue");
        let (_, old_token) = queue
            .register_cancellation_if_queued_generation(id, 0)
            .expect("current token should register")
            .expect("queued task should remain available");
        queue
            .begin_run_if_generation(id, 0)
            .expect("task should start for generation zero")
            .expect("current queued task should start");

        let (cancelled, captured) = queue
            .cancel_matching_and_capture(id, Some(0), 123_456_789, None, true, true)
            .expect("cancellation should persist")
            .expect("running task should accept cancellation");
        assert!(cancelled.cancel_requested);
        let captured = captured.expect("active generation token should be captured");
        assert!(Arc::ptr_eq(&captured, &old_token));

        queue
            .fail_if_generation(id, 0, "resume fixture".to_string())
            .expect("test task should become retryable")
            .expect("generation zero should remain current");
        queue
            .claim_resume(id, true)
            .expect("retry should claim generation one")
            .expect("failed task should be retryable");
        queue
            .set_status(id, TaskStatus::Queued, None)
            .expect("retried task should queue");
        let (_, new_token) = queue
            .register_cancellation_if_queued_generation(id, 1)
            .expect("new generation token should register")
            .expect("generation one should be queued");
        assert!(!Arc::ptr_eq(&captured, &new_token));

        captured.notify_one();
        tokio::time::timeout(Duration::from_millis(100), captured.notified())
            .await
            .expect("captured old token should receive the cancellation");
        assert!(
            tokio::time::timeout(Duration::from_millis(5), new_token.notified())
                .await
                .is_err()
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn task_records_without_generation_deserialize_as_generation_zero() {
        let task = test_task(
            "task-legacy-generation",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=legacy".to_string(),
            },
        );
        let mut json = serde_json::to_value(task).expect("task should serialize");
        let fields = json.as_object_mut().expect("task JSON should be an object");
        fields.remove("generation");

        let task: TaskRecord = serde_json::from_value(json)
            .expect("legacy task record without generation should deserialize");
        assert_eq!(task.generation, 0);
    }

    #[test]
    fn claim_resume_advances_generation_and_clears_status_message_once() {
        let temp_root = temp_queue_root("resume-generation");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-resume-generation";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=resume".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Interrupted, None)
            .expect("task should be interrupted");
        queue
            .set_status_message_id(id, 42)
            .expect("status message should persist");

        let resumed = queue
            .claim_resume(id, false)
            .expect("interrupted task should be claimable")
            .expect("claim should return the task");
        assert_eq!(resumed.generation, 1);
        assert_eq!(resumed.status, TaskStatus::Preparing);
        assert_eq!(resumed.status_message_id, None);

        assert!(
            queue
                .claim_resume(id, false)
                .expect("task already claimed should not claim again")
                .is_none()
        );
        let current = queue
            .get(id)
            .expect("current task should load")
            .expect("task should remain present");
        assert_eq!(current.generation, 1);
        assert_eq!(current.revision, resumed.revision);
        assert_eq!(current.status_message_id, None);

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn stale_generation_updates_do_not_write_to_the_current_task() {
        let temp_root = temp_queue_root("stale-generation-update");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-stale-generation-update";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Bilibili {
                        url: "https://www.bilibili.com/video/BV1xx411c7mD".to_string(),
                        selection: None,
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Failed, Some("retry me".to_string()))
            .expect("task should be failed");
        queue
            .set_status_message_id(id, 43)
            .expect("status message should persist");
        queue
            .set_collection_progress(id, 5, 2, 1)
            .expect("collection progress should persist");
        let resumed = queue
            .claim_resume(id, true)
            .expect("failed task should be retryable")
            .expect("retry should be claimed");
        let revision = resumed.revision;

        assert!(queue.get_if_generation(id, 0).unwrap().is_none());
        assert!(!queue.generation_matches(id, 0).unwrap());
        assert!(queue.generation_matches(id, 1).unwrap());
        assert!(
            queue
                .set_status_message_id_if_generation(id, 0, 99)
                .unwrap()
                .is_none()
        );
        assert!(
            queue
                .set_collection_progress_if_generation(id, 0, 99, 98, 97)
                .unwrap()
                .is_none()
        );
        assert!(
            queue
                .record_staging_path_if_generation(id, 0, PathBuf::from("old-staging"))
                .unwrap()
                .is_none()
        );
        assert!(
            queue
                .set_plan_if_generation(id, 0, PlanValidationSnapshot::default())
                .unwrap()
                .is_none()
        );
        assert!(queue.begin_run_if_generation(id, 0).unwrap().is_none());
        assert!(
            queue
                .begin_verification_if_generation(id, 0)
                .unwrap()
                .is_none()
        );
        assert!(
            queue
                .finish_cancellation_if_generation(id, 0)
                .unwrap()
                .is_none()
        );
        assert!(
            queue
                .cancel_if_generation(id, 0, 123_456_789)
                .unwrap()
                .is_none()
        );
        assert!(
            queue
                .fail_if_generation(id, 0, "old worker failed".to_string())
                .unwrap()
                .is_none()
        );
        assert!(
            queue
                .complete_if_generation(id, 0, "stale-output".to_string(), &[], BTreeMap::new())
                .unwrap()
                .is_none()
        );

        let current = queue
            .get(id)
            .expect("current task should load")
            .expect("task should remain present");
        assert_eq!(current.revision, revision);
        assert_eq!(current.generation, 1);
        assert_eq!(current.status, TaskStatus::Preparing);
        assert_eq!(current.status_message_id, None);
        assert_eq!(current.media_entries_total, 5);
        assert_eq!(current.media_entries_completed, 2);
        assert_eq!(current.media_entries_failed, 1);
        assert!(current.staging_attempts.is_empty());

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn prompt_transitions_require_matching_generation_and_status() {
        let temp_root = temp_queue_root("prompt-generation-status-guards");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-prompt-generation-status-guards";
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=prompt-guard".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::AwaitingSelection, None)
            .expect("task should await selection");
        let replacement_job = JobRequest::Bilibili {
            url: "https://www.bilibili.com/video/BV1234567890".to_string(),
            selection: None,
        };

        assert!(
            queue
                .set_status_if_generation(
                    id,
                    1,
                    &[TaskStatus::AwaitingSelection],
                    TaskStatus::Queued,
                )
                .expect("stale status transition should not fail")
                .is_none()
        );
        assert!(
            queue
                .set_status_if_generation(
                    id,
                    0,
                    &[TaskStatus::AwaitingDuplicateChoice],
                    TaskStatus::Queued,
                )
                .expect("wrong-status transition should not fail")
                .is_none()
        );
        assert!(
            queue
                .update_job_if_generation(
                    id,
                    1,
                    &[TaskStatus::AwaitingSelection],
                    replacement_job.clone(),
                    TaskStatus::Queued,
                )
                .expect("stale job update should not fail")
                .is_none()
        );
        assert!(
            queue
                .update_job_if_generation(
                    id,
                    0,
                    &[TaskStatus::AwaitingDuplicateChoice],
                    replacement_job.clone(),
                    TaskStatus::Queued,
                )
                .expect("wrong-status job update should not fail")
                .is_none()
        );
        assert!(
            queue
                .set_status_message_id_if_generation_and_status(
                    id,
                    1,
                    &[TaskStatus::AwaitingSelection],
                    99,
                )
                .expect("stale prompt message association should not fail")
                .is_none()
        );
        assert!(
            queue
                .set_status_message_id_if_generation_and_status(
                    id,
                    0,
                    &[TaskStatus::AwaitingDuplicateChoice],
                    99,
                )
                .expect("wrong-status prompt message association should not fail")
                .is_none()
        );
        assert!(
            queue
                .cancel_if_generation_and_status(
                    id,
                    1,
                    123_456_789,
                    &[TaskStatus::AwaitingSelection],
                )
                .expect("stale prompt cancellation should not fail")
                .is_none()
        );
        assert!(
            queue
                .cancel_if_generation_and_status(
                    id,
                    0,
                    123_456_789,
                    &[TaskStatus::AwaitingDuplicateChoice],
                )
                .expect("wrong-status prompt cancellation should not fail")
                .is_none()
        );

        let unchanged = queue
            .get(id)
            .expect("task should load after guarded no-ops")
            .expect("task should remain present");
        assert_eq!(unchanged.status, TaskStatus::AwaitingSelection);
        assert_eq!(unchanged.status_message_id, None);
        assert!(matches!(unchanged.job, JobRequest::Youtube { .. }));

        let updated = queue
            .update_job_if_generation(
                id,
                0,
                &[TaskStatus::AwaitingSelection],
                replacement_job,
                TaskStatus::AwaitingDuplicateChoice,
            )
            .expect("current prompt job update should succeed")
            .expect("current prompt state should match");
        assert_eq!(updated.status, TaskStatus::AwaitingDuplicateChoice);
        assert!(updated.original_url.contains("BV1234567890"));
        assert!(
            queue
                .set_status_message_id_if_generation_and_status(
                    id,
                    0,
                    &[TaskStatus::AwaitingDuplicateChoice],
                    123,
                )
                .expect("current prompt message association should succeed")
                .is_some()
        );
        assert!(
            queue
                .set_status_if_generation(
                    id,
                    0,
                    &[TaskStatus::AwaitingDuplicateChoice],
                    TaskStatus::AwaitingSelection,
                )
                .expect("current prompt transition should succeed")
                .is_some()
        );
        let cancelled = queue
            .cancel_if_generation_and_status(id, 0, 123_456_789, &[TaskStatus::AwaitingSelection])
            .expect("current prompt cancellation should succeed")
            .expect("current prompt state should match");
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        assert!(
            queue
                .cancel_if_generation_and_status(
                    id,
                    0,
                    123_456_789,
                    &[TaskStatus::AwaitingSelection],
                )
                .expect("terminal prompt cancellation should not fail")
                .is_none()
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    fn spawn_queue_owner_child(root: &Path, slot: &str) -> std::process::Child {
        std::process::Command::new(std::env::current_exe().expect("test binary should resolve"))
            .arg("--ignored")
            .arg("--exact")
            .arg("queue::tests::cross_process_queue_owner_child")
            .arg("--nocapture")
            .env("TVD_QUEUE_OPERATION_CHILD_ROOT", root)
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
    fn queue_lock_files_and_queue_scans_use_coordinated_reads() {
        let temp_root = temp_queue_root("coordinated-queue-scans");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root.clone();
        config.downloads.pdf_dir = pdf_root.clone();
        let file_provider = Arc::new(MockQueueFileProvider::default());
        let file_provider_trait: Arc<dyn QueueFileProvider> = file_provider.clone();
        let queue = QueueManager::open_with_file_provider(&config, file_provider_trait)
            .expect("queue should open with the mock File Provider");
        let startup_writes = file_provider.write_paths();
        let startup_reads = file_provider.read_paths();
        for queue_root in [&video_root, &pdf_root] {
            let queue_dir = queue_root.join(QUEUE_DIRECTORY);
            assert!(
                startup_writes.contains(&queue_dir),
                "queue directory creation should use coordinated writes: {startup_writes:?}"
            );
            assert!(
                startup_reads.contains(&queue_dir.join(QUEUE_OPERATION_LOCK_FILE)),
                "operation lock access should use coordinated reads: {startup_reads:?}"
            );
            assert!(
                startup_reads.contains(&queue_dir.join(QUEUE_OWNER_LOCK_FILE)),
                "owner lock access should use coordinated reads: {startup_reads:?}"
            );
        }
        let reads_before_listing = file_provider.read_paths().len();

        queue
            .list(123_456_789, false, 0)
            .expect("queue listing should scan both stores");

        let reads = file_provider.read_paths();
        let listing_reads = &reads[reads_before_listing..];
        assert!(
            listing_reads.contains(&video_root.join(QUEUE_DIRECTORY)),
            "video queue directory scans should use coordinated reads: {listing_reads:?}"
        );
        assert!(
            listing_reads.contains(&pdf_root.join(QUEUE_DIRECTORY)),
            "PDF queue directory scans should use coordinated reads: {listing_reads:?}"
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn coordinated_accessor_urls_are_rebased_and_stay_within_the_download_root() {
        let temp_root = temp_queue_root("file-provider-accessor-url");
        let physical_video_root = temp_root.join("physical-videos");
        let video_alias = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&physical_video_root).expect("physical video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        symlink(&physical_video_root, &video_alias).expect("video root alias should create");

        let rooted = RootedFs::new(&video_alias).expect("video root should bind");
        let canonical_video_root = rooted.root_path().to_path_buf();
        let accessor_record = canonical_video_root
            .join(QUEUE_DIRECTORY)
            .join("task-accessor.json");
        assert_eq!(
            coordinated_path_under_root(&rooted, &accessor_record)
                .expect("canonical accessor path should map under its logical root"),
            video_alias.join(QUEUE_DIRECTORY).join("task-accessor.json")
        );
        assert!(coordinated_path_under_root(&rooted, &temp_root.join("outside.json")).is_err());
        assert!(
            coordinated_path_under_root(&rooted, &rooted.root_path().join("../outside.json"))
                .is_err()
        );

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_alias.clone();
        config.downloads.pdf_dir = pdf_root;
        let file_provider = Arc::new(MockQueueFileProvider::default());
        file_provider.rewrite_paths_under(&video_alias, &canonical_video_root);
        let file_provider_trait: Arc<dyn QueueFileProvider> = file_provider.clone();
        let queue = QueueManager::open_with_file_provider(&config, file_provider_trait)
            .expect("queue should accept canonical accessor URLs within the configured root");
        let task = test_task(
            "task-canonical-accessor-url",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
            },
        );
        assert!(
            queue
                .create(task)
                .expect("task should write through accessor URL")
        );
        assert!(
            queue
                .get("task-canonical-accessor-url")
                .expect("task should read through accessor URL")
                .is_some()
        );
        assert!(
            file_provider
                .accessor_paths()
                .iter()
                .any(|path| path.starts_with(&canonical_video_root)),
            "mock provider should pass its alternate canonical URL to accessors"
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn download_root_is_bound_before_file_provider_accessor_returns() {
        let temp_root = temp_queue_root("file-provider-bound-download-root");
        let physical_video_root = temp_root.join("physical-videos");
        let replacement_video_root = temp_root.join("replacement-videos");
        let video_alias = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&physical_video_root).expect("physical video root should create");
        fs::create_dir_all(&replacement_video_root).expect("replacement video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        symlink(&physical_video_root, &video_alias).expect("video root alias should create");
        let expected_identity = RootedFs::new(&physical_video_root)
            .expect("physical video root should bind")
            .root_identity();

        let file_provider = Arc::new(MockQueueFileProvider::default());
        file_provider.replace_symlink_after_next_read(&video_alias, &replacement_video_root);
        let root = coordinate_directory_root(file_provider.as_ref(), &video_alias)
            .expect("coordinated root should be bound before access returns");

        // root_identity comes from the held directory descriptor, so comparing it with a
        // descriptor opened on the original target checks object identity. Canonicalizing the
        // alias separately only confirms the mock retargeted the path after the callback.
        assert_eq!(
            root.root_identity(),
            expected_identity,
            "the returned root must retain the object opened inside the coordinator callback"
        );
        assert!(
            fs::canonicalize(&video_alias)
                .expect("mock provider should replace the configured alias")
                == fs::canonicalize(&replacement_video_root)
                    .expect("replacement root should resolve"),
            "the test must replace the alias after the coordinator callback returns"
        );

        drop(root);
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
    fn concurrent_queue_managers_across_processes_are_rejected() {
        let temp_root = temp_queue_root("concurrent-queue-owner");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("primary queue owner should open");
        let task_id = "task-live-queue-owner";
        assert!(
            queue
                .create(test_task(
                    task_id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                    },
                ))
                .expect("live task should persist")
        );
        queue
            .set_status(task_id, TaskStatus::Running, None)
            .expect("task should be running before the second process starts");
        let mut child = spawn_queue_owner_child(&temp_root, "busy");
        let result_path = temp_root.join("owner-busy-result");
        wait_for_child_marker(&mut child, &result_path, "second queue owner");
        let output = child
            .wait_with_output()
            .expect("second owner output should collect");
        assert!(
            output.status.success(),
            "owner probe failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read(&result_path).expect("owner probe should persist its result"),
            b"busy",
            "a second process must not start recovery or task work for a live owner"
        );
        assert_eq!(
            queue
                .get(task_id)
                .expect("live task should load")
                .expect("live task should remain")
                .status,
            TaskStatus::Running,
            "the rejected process must not mark a live task interrupted"
        );

        drop(queue);
        let mut child = spawn_queue_owner_child(&temp_root, "released");
        let result_path = temp_root.join("owner-released-result");
        wait_for_child_marker(&mut child, &result_path, "released queue owner");
        let output = child
            .wait_with_output()
            .expect("released owner output should collect");
        assert!(
            output.status.success(),
            "released owner should open successfully"
        );
        assert_eq!(
            fs::read(&result_path).expect("released owner should persist its result"),
            b"opened"
        );
        let recovered = QueueManager::open(&config).expect("released owner should allow recovery");
        assert_eq!(
            recovered
                .get(task_id)
                .expect("task should load after owner release")
                .expect("task should remain after owner release")
                .status,
            TaskStatus::Interrupted,
            "a later process should recover the task after the owner exits"
        );
        drop(recovered);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn second_queue_manager_in_same_process_is_rejected() {
        let temp_root = temp_queue_root("duplicate-queue-owner");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root.clone();
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let error = match QueueManager::open(&config) {
            Ok(_) => panic!("a second manager must not acquire the queue owner lock"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("already has a live owner"));

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn stale_selection_transitions_cannot_requeue_a_resumed_task() {
        let temp_root = temp_queue_root("stale-selection-transition");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-stale-selection-transition";
        let initial_job = JobRequest::Bilibili {
            url: "https://www.bilibili.com/video/BV1xx411c7mD".to_string(),
            selection: None,
        };
        assert!(
            queue
                .create(test_task(id, initial_job.clone()))
                .expect("task should persist")
        );
        assert!(
            queue
                .update_job_if_current(
                    id,
                    &[TaskStatus::Received],
                    initial_job.clone(),
                    TaskStatus::AwaitingSelection,
                )
                .expect("selection prompt state should persist")
                .is_some()
        );
        assert!(
            queue
                .claim_resume(id, false)
                .expect("task resume should be claimed")
                .is_some()
        );
        queue
            .set_status(id, TaskStatus::Running, None)
            .expect("resumed task should start running");

        assert!(
            queue
                .update_job_if_current(
                    id,
                    &[TaskStatus::AwaitingSelection],
                    initial_job,
                    TaskStatus::Preparing,
                )
                .expect("stale Bilibili selection should be ignored")
                .is_none()
        );
        assert!(
            queue
                .set_status_if_current(
                    id,
                    &[TaskStatus::AwaitingDuplicateChoice],
                    TaskStatus::Queued,
                )
                .expect("stale duplicate choice should be ignored")
                .is_none()
        );
        assert_eq!(
            queue
                .get(id)
                .expect("running task should load")
                .expect("running task should remain")
                .status,
            TaskStatus::Running
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    #[ignore = "spawned by cross-process queue owner tests"]
    fn cross_process_queue_owner_child() {
        let root = PathBuf::from(
            std::env::var_os("TVD_QUEUE_OPERATION_CHILD_ROOT")
                .expect("queue owner child root must be provided by the parent test"),
        );
        let slot = std::env::var("TVD_QUEUE_OPERATION_CHILD_SLOT")
            .expect("queue owner child slot must be provided");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = root.join("videos");
        config.downloads.pdf_dir = root.join("pdfs");
        let result = match QueueManager::open(&config) {
            Ok(queue) => {
                drop(queue);
                b"opened".as_slice()
            }
            Err(error) if error.to_string().contains("already has a live owner") => {
                b"busy".as_slice()
            }
            Err(error) => panic!("queue owner probe failed unexpectedly: {error:#}"),
        };
        fs::write(root.join(format!("owner-{slot}-result")), result)
            .expect("queue owner child result should write");
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
    fn restart_finishes_a_persisted_cancellation_request() {
        let temp_root = temp_queue_root("restart-pending-cancellation");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-restart-pending-cancellation";
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
            .expect("task should start running");
        let requested = queue
            .cancel(id, 123_456_789)
            .expect("cancellation should persist")
            .expect("running task should accept cancellation request");
        assert_eq!(requested.status, TaskStatus::Running);
        assert!(requested.cancel_requested);

        drop(queue);
        let reopened = QueueManager::open(&config).expect("cancellation should recover");
        let recovered = reopened
            .get(id)
            .expect("cancelled task should load")
            .expect("cancelled task should remain in history");
        assert_eq!(recovered.status, TaskStatus::Cancelled);
        assert!(!recovered.cancel_requested);
        assert!(
            reopened
                .video
                .terminal_history_path(id)
                .expect("history path should resolve")
                .is_file(),
            "recovered cancellation should archive its task record"
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
        let file_provider = Arc::new(MockQueueFileProvider::default());
        let file_provider_trait: Arc<dyn QueueFileProvider> = file_provider.clone();
        let reopened = QueueManager::open_with_file_provider(&config, file_provider_trait)
            .expect("interrupted move should recover");
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
        assert_eq!(
            file_provider.move_paths(),
            vec![(entry.record_path.clone(), destination.clone())],
            "restart recovery should coordinate the source and destination together"
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

    #[test]
    fn published_completion_survives_temporary_sidecar_move_failure() {
        let temp_root = temp_queue_root("completed-sidecar-provider-retry");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root.clone();
        config.downloads.pdf_dir = pdf_root;
        let file_provider = Arc::new(MockQueueFileProvider::default());
        let file_provider_trait: Arc<dyn QueueFileProvider> = file_provider.clone();
        let queue = QueueManager::open_with_file_provider(&config, file_provider_trait)
            .expect("queue should open with the mock File Provider");
        let id = "task-completed-sidecar-provider-retry";
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
        let media_path = video_root.join("completed-output.mp4");
        fs::write(&media_path, b"verified media").expect("published media fixture should write");
        file_provider.fail_next_move("simulated temporary File Provider move failure");

        let completed = queue
            .complete(
                id,
                media_path.display().to_string(),
                std::slice::from_ref(&media_path),
                BTreeMap::new(),
            )
            .expect("published output should succeed while archival migration remains pending");
        assert_eq!(completed.status, TaskStatus::Completed);
        let active_path = queue
            .video
            .task_path(id)
            .expect("active task path should resolve");
        let destination = queue
            .video
            .root_path
            .join(".telegram-video-downloader-task-task-completed-sidecar-provider-retry.json");
        assert!(
            active_path.is_file(),
            "the active record should remain recoverable"
        );
        assert!(
            !destination.exists(),
            "the sidecar should remain pending until File Provider access recovers"
        );
        assert_eq!(
            queue
                .video
                .read_index()
                .expect("queue index should read")
                .tasks[id]
                .move_target,
            Some(destination.clone()),
            "the durable move intent should survive the provider failure"
        );

        let recovered = queue
            .get(id)
            .expect("queue access should retry the pending sidecar migration")
            .expect("completed task should remain indexed");
        assert_eq!(recovered.status, TaskStatus::Completed);
        assert!(
            !active_path.exists(),
            "recovery should remove the active record"
        );
        assert!(destination.is_file(), "recovery should install the sidecar");
        assert_eq!(
            file_provider.move_paths(),
            vec![
                (active_path.clone(), destination.clone()),
                (active_path, destination),
            ],
            "completion and recovery should both coordinate both migration paths"
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }
}
