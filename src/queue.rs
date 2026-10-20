use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

use crate::bilibili_attempt::{
    BilibiliAttemptEntry, BilibiliAttemptEntryState, BilibiliAttemptOutcome,
    BilibiliAttemptSnapshot, BilibiliAttemptUpdate,
};
use crate::config::AppConfig;
use crate::file_provider::{
    QueueFileProvider, classify_deadlock_error, is_deadlock_error, is_file_provider_access_error,
    platform_queue_file_provider,
};
use crate::media_libraries::{
    MediaLibrary, MediaLibraryRegistryStore, MediaLibrarySnapshot, MediaLibraryStatus,
    MigrationDefaultUpdateOutcome, StagingMigrationControlReceipt, TaskDestination,
    TaskDestinationTransfer, TaskDestinationTransferPhase, TaskTransferControl,
    TaskTransferControlKind, TaskTransferEntryKind, TaskTransferFile, TaskTransferPlan,
    TaskTransferSnapshot, destination_for_library, prepare_library_root, validate_category,
    validate_name, validate_task_destination,
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
const MAX_TASK_TRANSFER_ENTRIES: usize = 20_000;
const MAX_TASK_TRANSFER_BYTES: u64 = 1 << 50;
const MAX_TASK_TRANSFER_PLAN_BYTES: usize = 8 * 1024 * 1024;
const TASK_TRANSFER_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const VIDEO_STAGING_DIRECTORY: &str = ".telegram-video-downloader-staging";
const VIDEO_STAGING_JOB_PREFIX: &str = "job-";
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncompleteTaskTransferSummary {
    pub task_count: usize,
    pub staging_bytes: Option<u64>,
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
    /// Internal task-lifetime identity for durable Bilibili publication receipts.
    #[serde(default)]
    pub bilibili_publication_namespace: Option<String>,
    /// Entries whose stream availability probe completed, including entries with no streams.
    #[serde(default)]
    pub bilibili_resolved_stream_ids: Vec<String>,
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
        let previous_resolved_stream_ids = self
            .bilibili_resolved_stream_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let current_resolved_stream_ids = current
            .bilibili_resolved_stream_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if previous_resolved_stream_ids != current_resolved_stream_ids {
            differences.push("resolved stream availability");
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
        let previous_streams_resolved = self
            .bilibili_resolved_stream_ids
            .iter()
            .any(|id| id == stable_media_id);
        let current_streams_resolved = current
            .bilibili_resolved_stream_ids
            .iter()
            .any(|id| id == stable_media_id);
        let previous_has_stream_evidence = self
            .selected_format_ids
            .iter()
            .any(|value| has_media_id_prefix(value, stable_media_id))
            || self
                .exact_sizes
                .iter()
                .any(|value| has_media_id_prefix(&value.subject, stable_media_id))
            || self
                .resolution_codecs
                .iter()
                .any(|value| has_media_id_prefix(value, stable_media_id));
        let newly_resolved_without_old_evidence =
            !previous_streams_resolved && current_streams_resolved && !previous_has_stream_evidence;

        // A newly completed probe can add per-ID fields to a plan that had no stream evidence.
        // Ignore only those newly discovered fields in that case. Legacy plans with known
        // format, size, or codec evidence still compare every persisted field, and losing a
        // previously completed probe remains incompatible with reusing its receipt.
        let stream_details_match = newly_resolved_without_old_evidence
            || (self
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
                        .filter(|value| has_media_id_prefix(value, stable_media_id))));

        (!previous_streams_resolved || current_streams_resolved)
            && self.media_profile == current.media_profile
            && stream_details_match
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
    /// Frozen video destination selected when the task is enqueued. Legacy records omit it
    /// and are resolved against the store that contains the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<TaskDestination>,
    /// Set while an explicit cross-library migration owns this task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_transfer: Option<TaskDestinationTransfer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_transfer_phase: Option<TaskDestinationTransferPhase>,
    /// Exact durable proof that the queue side of the last handoff was acknowledged. Kept until
    /// the migration engine durably records its final queue acknowledgment and completes source
    /// staging cleanup, so normal task actions cannot race the cleanup after a restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) destination_transfer_ack: Option<TaskDestinationTransfer>,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct StoreIndex {
    version: u32,
    #[serde(default)]
    tasks: BTreeMap<String, TaskIndexEntry>,
    #[serde(default)]
    chat_activity: BTreeMap<String, ChatActivity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TaskIndexEntry {
    chat_id: i64,
    record_path: PathBuf,
    #[serde(default)]
    move_target: Option<PathBuf>,
    revision: u64,
    updated_at: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    state_change_notify: Arc<Notify>,
}

struct QueueOperationLock {
    _file: BoundFile,
}

struct QueueOwnerLock {
    _file: BoundFile,
}

pub struct QueueManager {
    /// Configured video root retained for compatibility with existing in-module callers.
    /// Task lookup and dispatch use `video_libraries`.
    #[cfg(test)]
    video: DownloadStore,
    pdf: DownloadStore,
    _pdf_owner_lock: Option<Arc<QueueOwnerLock>>,
    video_libraries: RwLock<Vec<RegisteredVideoStore>>,
    media_libraries: Mutex<MediaLibraryRegistryStore>,
    video_statuses: RwLock<BTreeMap<String, MediaLibraryStatus>>,
    operation_lock: Mutex<()>,
    cancellations: Mutex<HashMap<String, CancellationRegistration>>,
    publication_notify: Arc<Notify>,
    state_change_notify: Arc<Notify>,
    #[cfg(test)]
    interrupt_after_sidecar_move: AtomicBool,
}

struct RegisteredVideoStore {
    library_id: String,
    store: DownloadStore,
    available: bool,
    _owner_lock: Option<Arc<QueueOwnerLock>>,
}

struct CancellationRegistration {
    generation: Option<u64>,
    notify: Arc<Notify>,
}

impl TaskRecord {
    /// Stable, private namespace for durable publication receipts owned by this logical task.
    /// It survives resume generations and changes for separately queued tasks.
    pub fn bilibili_publication_namespace(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"bilibili-publication-namespace-v1\0");
        digest.update((self.id.len() as u64).to_be_bytes());
        digest.update(self.id.as_bytes());
        digest.update(self.chat_id.to_be_bytes());
        digest.update(self.created_at.to_be_bytes());
        format!("bpn1-{:x}", digest.finalize())
    }

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
            destination: None,
            destination_transfer: None,
            destination_transfer_phase: None,
            destination_transfer_ack: None,
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

fn has_verified_bilibili_output_receipt(entry: &BilibiliAttemptEntry) -> bool {
    entry.state == BilibiliAttemptEntryState::Verified
        && !entry.stable_media_id.trim().is_empty()
        && !entry.published_media_paths.is_empty()
        && !entry.published_media_hashes.is_empty()
        && entry.published_media_paths.iter().all(|path| {
            entry
                .published_media_hashes
                .contains_key(path.to_string_lossy().as_ref())
        })
}

type VerifiedBilibiliReceiptKey = (u32, String, Vec<(PathBuf, String)>);

fn verified_bilibili_receipt_keys(record: &TaskRecord) -> BTreeSet<VerifiedBilibiliReceiptKey> {
    record
        .bilibili_attempt
        .iter()
        .flat_map(|attempt| &attempt.entries)
        .filter(|entry| has_verified_bilibili_output_receipt(entry))
        .map(|entry| {
            let mut outputs = entry
                .published_media_paths
                .iter()
                .filter_map(|path| {
                    entry
                        .published_media_hashes
                        .get(path.to_string_lossy().as_ref())
                        .map(|hash| (path.clone(), hash.clone()))
                })
                .collect::<Vec<_>>();
            outputs.sort();
            (entry.index, entry.stable_media_id.clone(), outputs)
        })
        .collect()
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
    attempt.bilibili_publication_namespace = Some(
        accepted_plan
            .bilibili_publication_namespace
            .clone()
            .unwrap_or_else(|| record.bilibili_publication_namespace()),
    );

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

fn stamp_bilibili_publication_namespace(
    record: &TaskRecord,
    snapshot: &mut BilibiliAttemptSnapshot,
) {
    if snapshot.independent_entries {
        snapshot.bilibili_publication_namespace = Some(
            record
                .plan
                .as_ref()
                .and_then(|plan| plan.bilibili_publication_namespace.clone())
                .unwrap_or_else(|| record.bilibili_publication_namespace()),
        );
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
        let state_change_notify = Arc::new(Notify::new());
        let mut media_libraries =
            MediaLibraryRegistryStore::open(config.config_file(), &config.downloads.video_dir)?;
        let library_snapshot = media_libraries.snapshot();
        let mut video_libraries = Vec::new();
        let mut video_statuses = BTreeMap::new();
        #[cfg(test)]
        let mut compatibility_video = None;

        // Open each registered library independently. An unplugged root is retained in the
        // settings snapshot and status map, while online roots continue serving their tasks.
        for library in &library_snapshot.libraries {
            let root_binding = media_libraries.root_binding(&library.id);
            let opened = (|| -> Result<(DownloadStore, Option<Arc<QueueOwnerLock>>)> {
                let root = coordinate_directory_root(file_provider.as_ref(), &library.root)?;
                if root_binding.is_some_and(|binding| {
                    binding != (root.root_identity().device(), root.root_identity().inode())
                }) {
                    bail!("media library root object identity changed; it remains unavailable");
                }
                let store = DownloadStore::from_root(
                    root,
                    Arc::clone(&file_provider),
                    Arc::clone(&state_change_notify),
                )?;
                let owner_lock = Some(Arc::new(store.lock_queue_owner()?));
                let operation_lock = store.lock_queue_operations()?;
                store.ensure_index()?;
                store.reconcile_index()?;
                drop(operation_lock);
                Ok((store, owner_lock))
            })();

            match opened {
                Ok((store, owner_lock)) => {
                    if let Err(error) = media_libraries.bind_root_identity(
                        &library.id,
                        store.root.root_identity().device(),
                        store.root.root_identity().inode(),
                    ) {
                        video_statuses.insert(
                            library.id.clone(),
                            MediaLibraryStatus {
                                available: false,
                                available_space_bytes: None,
                                diagnostic: Some(error.to_string()),
                            },
                        );
                        continue;
                    }
                    let available_space_bytes = store.root.available_space_bytes().ok();
                    video_statuses.insert(
                        library.id.clone(),
                        MediaLibraryStatus {
                            available: true,
                            available_space_bytes,
                            diagnostic: None,
                        },
                    );
                    #[cfg(test)]
                    if store.root_path == config.downloads.video_dir {
                        compatibility_video = Some(store.clone());
                    }
                    video_libraries.push(RegisteredVideoStore {
                        library_id: library.id.clone(),
                        store,
                        available: true,
                        _owner_lock: owner_lock,
                    });
                }
                Err(error) => {
                    video_statuses.insert(
                        library.id.clone(),
                        MediaLibraryStatus {
                            available: false,
                            available_space_bytes: None,
                            diagnostic: Some(error.to_string()),
                        },
                    );
                    tracing::warn!(library_id = %library.id, error = %error, "media library is unavailable; keeping its settings entry");
                }
            }
        }
        // Open the configured PDF root after registered video roots so their queue state keeps
        // its established startup priority. If both configurations resolve to the same directory
        // object, share the already-owned video store and keep the PDF spelling as an accepted
        // output alias instead of creating a second queue or owner lock.
        let pdf_root =
            coordinate_directory_root(file_provider.as_ref(), &config.downloads.pdf_dir)?;
        let shared_video = video_libraries
            .iter()
            .find(|registered| registered.store.root.root_identity() == pdf_root.root_identity())
            .map(|registered| (registered.store.clone(), registered._owner_lock.clone()));
        let (pdf, pdf_owner_lock) = if let Some((mut store, owner_lock)) = shared_video {
            store.add_root_alias(pdf_root.logical_root_path());
            let owner_lock = owner_lock
                .ok_or_else(|| anyhow!("shared video/PDF queue root has no retained owner lock"))?;
            (store, Some(owner_lock))
        } else {
            let store = DownloadStore::from_root(
                pdf_root,
                Arc::clone(&file_provider),
                Arc::clone(&state_change_notify),
            )?;
            let owner_lock = Some(Arc::new(store.lock_queue_owner()?));
            let operation_lock = store.lock_queue_operations()?;
            store.ensure_index()?;
            store.reconcile_index()?;
            drop(operation_lock);
            (store, owner_lock)
        };

        // Keep a usable compatibility handle for existing queue internals and tests. Task
        // operations resolve their actual store from `video_libraries` below.
        #[cfg(test)]
        let video = compatibility_video
            .or_else(|| {
                video_libraries
                    .first()
                    .map(|registered| registered.store.clone())
            })
            .unwrap_or_else(|| pdf.clone());
        let manager = Self {
            #[cfg(test)]
            video,
            pdf,
            _pdf_owner_lock: pdf_owner_lock,
            video_libraries: RwLock::new(video_libraries),
            media_libraries: Mutex::new(media_libraries),
            video_statuses: RwLock::new(video_statuses),
            operation_lock: Mutex::new(()),
            cancellations: Mutex::new(HashMap::new()),
            publication_notify: Arc::new(Notify::new()),
            state_change_notify,
            #[cfg(test)]
            interrupt_after_sidecar_move: AtomicBool::new(false),
        };
        manager.reconcile_task_destination_transfers()?;
        manager.recover_interrupted_tasks()?;
        Ok(manager)
    }

    /// Subscribe to newly published outputs without polling or rescanning task history.
    pub fn publication_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.publication_notify)
    }

    /// Subscribe to durable queue status or progress changes without requesting a library scan.
    pub fn state_change_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.state_change_notify)
    }

    pub fn registry_snapshot(&self) -> Result<MediaLibrarySnapshot> {
        Ok(self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .snapshot())
    }

    #[cfg(test)]
    pub fn default_video_destination(&self) -> Result<TaskDestination> {
        let destination = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .default_destination()?;
        self.ensure_video_library_online(&destination.library_id)?;
        Ok(destination)
    }

    pub fn video_destination(
        &self,
        library_id: &str,
        category_override: Option<&Path>,
    ) -> Result<TaskDestination> {
        let libraries = self.media_libraries.lock().map_err(poisoned_lock)?;
        let destination =
            destination_for_library(&libraries.snapshot(), library_id, category_override)?;
        drop(libraries);
        self.ensure_video_library_online(library_id)?;
        Ok(destination)
    }

    fn ensure_video_library_online(&self, library_id: &str) -> Result<()> {
        let available = self
            .video_statuses
            .read()
            .map_err(poisoned_lock)?
            .get(library_id)
            .is_some_and(|status| status.available);
        if !available {
            self.refresh_video_library(library_id)?;
        }
        if self
            .video_statuses
            .read()
            .map_err(poisoned_lock)?
            .get(library_id)
            .is_some_and(|status| status.available)
        {
            Ok(())
        } else {
            let detail = self
                .video_statuses
                .read()
                .map_err(poisoned_lock)?
                .get(library_id)
                .and_then(|status| status.diagnostic.clone())
                .unwrap_or_else(|| "media library is currently unavailable".to_string());
            bail!("media library {library_id} is unavailable: {detail}");
        }
    }

    fn require_video_library_available(&self, library_id: &str) -> Result<()> {
        let statuses = self.video_statuses.read().map_err(poisoned_lock)?;
        let status = statuses
            .get(library_id)
            .ok_or_else(|| anyhow!("media library {library_id} has no availability status"))?;
        if status.available {
            return Ok(());
        }
        let detail = status
            .diagnostic
            .as_deref()
            .unwrap_or("media library is currently unavailable");
        bail!("media library {library_id} is unavailable: {detail}");
    }

    #[cfg(test)]
    pub fn default_video_config(&self, base: &AppConfig) -> Result<AppConfig> {
        let destination = self.default_video_destination()?;
        Ok(config_for_destination(base, &destination))
    }

    pub fn task_destination(&self, task_id: &str) -> Result<Option<TaskDestination>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, record, _)) = self.find_record_entry_unlocked(task_id)? else {
            return Ok(None);
        };
        if matches!(record.job, JobRequest::Pdf { .. }) {
            return Ok(None);
        }
        if let Some(destination) = record.destination {
            return Ok(Some(destination));
        }
        let libraries = self.media_libraries.lock().map_err(poisoned_lock)?;
        let library = libraries
            .snapshot()
            .libraries
            .into_iter()
            .find(|library| library.root == store.root_path)
            .ok_or_else(|| {
                anyhow!(
                    "legacy task {} belongs to an unregistered video root",
                    record.id
                )
            })?;
        // Legacy task destinations predate categories; retaining an empty category preserves
        // their original output directory even if the library now has a default category.
        Ok(Some(TaskDestination {
            library_id: library.id,
            root: store.root_path,
            category: None,
        }))
    }

    pub fn task_config(&self, base: &AppConfig, task_id: &str) -> Result<Option<AppConfig>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, record)) = self.find_record_entry_read_only_unlocked(task_id)? else {
            self.ensure_task_config_lookup_complete_unlocked(task_id)?;
            return Ok(None);
        };
        if matches!(record.job, JobRequest::Pdf { .. }) {
            self.validate_pdf_task_config_root_unlocked()?;
            return Ok(Some(base.clone()));
        }
        let (destination, library) = match record.destination {
            Some(destination) => {
                let library = self
                    .registry_snapshot()?
                    .libraries
                    .into_iter()
                    .find(|library| library.id == destination.library_id)
                    .ok_or_else(|| {
                        anyhow!(
                            "task {} destination refers to an unregistered media library",
                            record.id
                        )
                    })?;
                if !same_trusted_root_path(&destination.root, &library.root)? {
                    bail!(
                        "task {} destination root no longer matches its registered media library",
                        record.id
                    );
                }
                (destination, library)
            }
            None => {
                let library = self.library_for_legacy_task_store(&store)?;
                (
                    TaskDestination {
                        library_id: library.id.clone(),
                        root: store.root_path.clone(),
                        category: None,
                    },
                    library,
                )
            }
        };

        self.validate_video_library_for_task_unlocked(&store, &library)?;
        Ok(Some(config_for_destination(base, &destination)))
    }

    fn find_record_entry_read_only_unlocked(
        &self,
        task_id: &str,
    ) -> Result<Option<(DownloadStore, TaskRecord)>> {
        let registered = self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .map(|registered| (registered.library_id.clone(), registered.store.clone()))
            .collect::<Vec<_>>();
        let mut pdf_is_video_root = false;
        let mut first_video_read_error = None;
        for (library_id, store) in registered {
            pdf_is_video_root |= store.shares_root(&self.pdf);
            match store.peek_task(task_id) {
                Ok(Some((record, _))) => return Ok(Some((store, record))),
                Ok(None) => {}
                Err(error) => {
                    self.note_video_store_read_error(&library_id, &store, &error);
                    if first_video_read_error.is_none() {
                        first_video_read_error = Some(error.context(format!(
                            "failed to read task {task_id} from media library {library_id}"
                        )));
                    }
                }
            }
        }
        if !pdf_is_video_root {
            match self.pdf.peek_task(task_id) {
                Ok(Some((record, _))) => return Ok(Some((self.pdf.clone(), record))),
                Ok(None) => {}
                Err(error) if first_video_read_error.is_none() => {
                    first_video_read_error = Some(
                        error.context(format!("failed to read task {task_id} from the PDF queue")),
                    );
                }
                Err(_) => {}
            }
        }
        if let Some(error) = first_video_read_error {
            return Err(error.context("task lookup could not be completed across queue roots"));
        }
        Ok(None)
    }

    fn ensure_task_config_lookup_complete_unlocked(&self, task_id: &str) -> Result<()> {
        if let Some((library_id, diagnostic)) = self
            .video_statuses
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .find(|(_, status)| !status.available)
            .map(|(library_id, status)| {
                (
                    library_id.clone(),
                    status
                        .diagnostic
                        .clone()
                        .unwrap_or_else(|| "media library is unavailable".to_string()),
                )
            })
        {
            bail!(
                "cannot determine whether task {task_id} exists while media library {library_id} is unavailable: {diagnostic}"
            );
        }
        Ok(())
    }

    fn library_for_legacy_task_store(&self, store: &DownloadStore) -> Result<MediaLibrary> {
        let identity = store.root.root_identity();
        let registry = self.media_libraries.lock().map_err(poisoned_lock)?;
        for library in registry.snapshot().libraries {
            match registry.root_binding(&library.id) {
                Some((device, inode))
                    if (device, inode) == (identity.device(), identity.inode()) =>
                {
                    return Ok(library);
                }
                Some(_) => {}
                None if same_trusted_root_path(&library.root, &store.root_path)? => {
                    return Ok(library);
                }
                None => {}
            }
        }
        bail!("legacy task queue root is not bound to a registered media library")
    }

    fn validate_video_library_for_task_unlocked(
        &self,
        store: &DownloadStore,
        library: &MediaLibrary,
    ) -> Result<()> {
        let store_identity = store.root.root_identity();
        let binding = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .root_binding(&library.id);
        match binding {
            Some((device, inode))
                if (device, inode) != (store_identity.device(), store_identity.inode()) =>
            {
                bail!(
                    "media library {} queue root no longer matches its persisted object identity",
                    library.id
                );
            }
            Some(_) => {}
            None => {
                // Legacy registries without an inode binding may be adopted only after a fresh
                // coordinated lookup confirms the already-open queue root. This check happens
                // before refresh can recover or create queue state.
                let current =
                    coordinate_directory_root(self.pdf.file_provider.as_ref(), &library.root)?;
                if current.root_identity() != store_identity {
                    bail!(
                        "media library {} path no longer names its open queue root",
                        library.id
                    );
                }
                self.media_libraries
                    .lock()
                    .map_err(poisoned_lock)?
                    .bind_root_identity(
                        &library.id,
                        store_identity.device(),
                        store_identity.inode(),
                    )?;
            }
        }

        // Refresh first re-coordinates the registered path and compares it with the persisted
        // device/inode before opening an owner lock or touching queue indexes. It intentionally
        // ignores child contents and mutable timestamps.
        self.refresh_video_library_unlocked(library.clone())?;
        self.require_video_library_available(&library.id)?;
        let current_binding = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .root_binding(&library.id);
        if current_binding != Some((store_identity.device(), store_identity.inode())) {
            bail!(
                "media library {} queue root no longer matches its persisted object identity",
                library.id
            );
        }
        let current_store_identity = self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .find(|registered| registered.library_id == library.id)
            .map(|registered| registered.store.root.root_identity());
        if current_store_identity != Some(store_identity) {
            bail!(
                "media library {} queue store is unavailable or bound to a different root object",
                library.id
            );
        }
        Ok(())
    }

    /// Verify the registered queue owner before changing which library receives new tasks.
    /// Root device/inode is the protected property; child entries, media contents, and timestamps
    /// are intentionally not part of this check so ordinary directory churn remains valid.
    fn validate_video_library_for_settings_mutation_unlocked(
        &self,
        library_id: &str,
    ) -> Result<()> {
        let library = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .library(library_id)
            .cloned()
            .ok_or_else(|| anyhow!("unknown media library {library_id}"))?;

        let existing_store = self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .find(|registered| registered.library_id == library_id)
            .map(|registered| registered.store.clone());
        if let Some(store) = existing_store {
            self.validate_video_library_for_task_unlocked(&store, &library)?;
        } else {
            // Offline roots are retained in settings without a live store. Reopening is safe
            // only because refresh validates any persisted root identity before queue recovery
            // or owner-lock creation.
            self.refresh_video_library_unlocked(library.clone())?;
            self.require_video_library_available(library_id)?;
            let store = self
                .video_libraries
                .read()
                .map_err(poisoned_lock)?
                .iter()
                .find(|registered| registered.library_id == library_id && registered.available)
                .map(|registered| registered.store.clone())
                .ok_or_else(|| {
                    anyhow!("media library {library_id} has no available queue store")
                })?;
            self.validate_video_library_for_task_unlocked(&store, &library)?;
        }

        let store = self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .find(|registered| registered.library_id == library_id && registered.available)
            .map(|registered| registered.store.clone())
            .ok_or_else(|| anyhow!("media library {library_id} has no available queue store"))?;
        let current_root =
            coordinate_directory_root(self.pdf.file_provider.as_ref(), &library.root)?;
        let identity = current_root.root_identity();
        let binding = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .root_binding(library_id);
        if binding != Some((identity.device(), identity.inode()))
            || store.root.root_identity() != identity
        {
            bail!("media library {library_id} root no longer matches its persisted queue identity");
        }
        Ok(())
    }

    fn validate_pdf_task_config_root_unlocked(&self) -> Result<()> {
        self.pdf.root.validate_configured_root()?;
        let identity = self.pdf.root.root_identity();
        let library = {
            let registry = self.media_libraries.lock().map_err(poisoned_lock)?;
            registry.snapshot().libraries.into_iter().find(|library| {
                registry.root_binding(&library.id) == Some((identity.device(), identity.inode()))
            })
        };
        if let Some(library) = library {
            self.refresh_video_library_unlocked(library.clone())?;
            self.require_video_library_available(&library.id)?;
            let binding = self
                .media_libraries
                .lock()
                .map_err(poisoned_lock)?
                .root_binding(&library.id);
            if binding != Some((identity.device(), identity.inode())) {
                bail!(
                    "PDF queue root no longer matches registered media library {} identity",
                    library.id
                );
            }
        }
        Ok(())
    }

    pub fn video_library_statuses(&self) -> Result<BTreeMap<String, MediaLibraryStatus>> {
        Ok(self.video_statuses.read().map_err(poisoned_lock)?.clone())
    }

    /// Reopen registered roots that were offline at startup and refresh cached capacity/status.
    /// A path whose current directory object differs from its persisted binding stays offline.
    pub fn refresh_video_libraries(&self) -> Result<()> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let libraries = self.registry_snapshot()?.libraries;
        for library in libraries {
            self.refresh_video_library_unlocked(library)?;
        }
        self.state_change_notify.notify_one();
        Ok(())
    }

    /// Reconnect just one selected library. Task submission uses this only when its frozen or
    /// default destination is cached offline, avoiding probes of unrelated mounted volumes.
    pub fn refresh_video_library(&self, library_id: &str) -> Result<()> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let library = self
            .registry_snapshot()?
            .libraries
            .into_iter()
            .find(|library| library.id == library_id)
            .ok_or_else(|| anyhow!("unknown media library {library_id}"))?;
        self.refresh_video_library_unlocked(library)?;
        self.state_change_notify.notify_one();
        Ok(())
    }

    fn refresh_video_library_unlocked(&self, library: MediaLibrary) -> Result<()> {
        let root = match coordinate_directory_root(self.pdf.file_provider.as_ref(), &library.root) {
            Ok(root) => root,
            Err(error) => {
                self.set_video_store_available(&library.id, false)?;
                self.set_video_library_status(
                    &library.id,
                    MediaLibraryStatus {
                        available: false,
                        available_space_bytes: None,
                        diagnostic: Some(error.to_string()),
                    },
                )?;
                return Ok(());
            }
        };
        let identity = root.root_identity();
        let binding = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .root_binding(&library.id);
        if binding
            .is_some_and(|(device, inode)| (device, inode) != (identity.device(), identity.inode()))
        {
            self.set_video_store_available(&library.id, false)?;
            self.set_video_library_status(
                &library.id,
                MediaLibraryStatus {
                    available: false,
                    available_space_bytes: None,
                    diagnostic: Some(
                        "registered path now names a different root object; it remains unavailable"
                            .to_string(),
                    ),
                },
            )?;
            return Ok(());
        }

        let existing = self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .find(|registered| registered.library_id == library.id)
            .map(|registered| registered.store.clone());
        let store = if let Some(existing) = existing {
            if existing.root.root_identity() != identity {
                self.set_video_store_available(&library.id, false)?;
                self.set_video_library_status(
                    &library.id,
                    MediaLibraryStatus {
                        available: false,
                        available_space_bytes: None,
                        diagnostic: Some(
                            "registered root identity no longer matches the open queue store"
                                .to_string(),
                        ),
                    },
                )?;
                return Ok(());
            }
            self.set_video_store_available(&library.id, true)?;
            existing
        } else {
            let opened = (|| -> Result<(DownloadStore, Option<Arc<QueueOwnerLock>>)> {
                let store = if identity == self.pdf.root.root_identity() {
                    self.pdf.clone()
                } else {
                    DownloadStore::from_root(
                        root,
                        Arc::clone(&self.pdf.file_provider),
                        Arc::clone(&self.state_change_notify),
                    )?
                };
                let owner_lock = if store.shares_root(&self.pdf) {
                    None
                } else {
                    Some(Arc::new(store.lock_queue_owner()?))
                };
                let operation_lock = store.lock_queue_operations()?;
                store.ensure_index()?;
                store.reconcile_index()?;
                drop(operation_lock);
                self.media_libraries
                    .lock()
                    .map_err(poisoned_lock)?
                    .bind_root_identity(&library.id, identity.device(), identity.inode())?;
                Ok((store, owner_lock))
            })();
            match opened {
                Ok((store, owner_lock)) => {
                    self.video_libraries.write().map_err(poisoned_lock)?.push(
                        RegisteredVideoStore {
                            library_id: library.id.clone(),
                            store: store.clone(),
                            available: true,
                            _owner_lock: owner_lock,
                        },
                    );
                    store
                }
                Err(error) => {
                    self.set_video_store_available(&library.id, false)?;
                    self.set_video_library_status(
                        &library.id,
                        MediaLibraryStatus {
                            available: false,
                            available_space_bytes: None,
                            diagnostic: Some(error.to_string()),
                        },
                    )?;
                    return Ok(());
                }
            }
        };
        self.set_video_library_status(
            &library.id,
            MediaLibraryStatus {
                available: true,
                available_space_bytes: store.root.available_space_bytes().ok(),
                diagnostic: None,
            },
        )?;
        Ok(())
    }

    fn set_video_store_available(&self, library_id: &str, available: bool) -> Result<()> {
        if let Some(registered) = self
            .video_libraries
            .write()
            .map_err(poisoned_lock)?
            .iter_mut()
            .find(|registered| registered.library_id == library_id)
        {
            registered.available = available;
        }
        Ok(())
    }

    fn set_video_library_status(&self, library_id: &str, status: MediaLibraryStatus) -> Result<()> {
        self.video_statuses
            .write()
            .map_err(poisoned_lock)?
            .insert(library_id.to_string(), status);
        Ok(())
    }

    pub fn register_video_library(
        &self,
        name: &str,
        root: &Path,
        default_category: Option<&Path>,
        set_default: bool,
        create_directory: bool,
    ) -> Result<MediaLibrary> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let name = validate_name(name)?;
        let default_category = validate_category(default_category)?;
        let requested_root = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .preflight_add(&name, root, default_category.as_deref(), create_directory)?;
        let registry_snapshot = self.registry_snapshot()?;
        let pdf_identity = self.pdf.root.root_identity();
        let pdf_has_registered_video_owner = {
            let registry = self.media_libraries.lock().map_err(poisoned_lock)?;
            registry_snapshot.libraries.iter().any(|library| {
                registry.root_binding(&library.id)
                    == Some((pdf_identity.device(), pdf_identity.inode()))
            })
        };
        if !pdf_has_registered_video_owner
            && (roots_overlap_queue_root(&requested_root, &self.pdf.root_path)
                || roots_overlap_queue_root(&requested_root, self.pdf.root.logical_root_path()))
        {
            bail!("video media libraries may not overlap the configured PDF root");
        }
        // The preflight above rejects overlap with online and offline video roots before an
        // explicit create-directory request can have filesystem effects.
        let canonical_root = prepare_library_root(root, create_directory)?;
        let root_fs = coordinate_directory_root(self.pdf.file_provider.as_ref(), &canonical_root)?;
        let root_identity = root_fs.root_identity();
        let existing_library = registry_snapshot.libraries.iter().find(|library| {
            crate::media_libraries::normalize_trusted_macos_alias(&library.root)
                .is_ok_and(|registered| registered == canonical_root)
        });
        let identity_alias = {
            let registry = self.media_libraries.lock().map_err(poisoned_lock)?;
            registry_snapshot.libraries.iter().find(|library| {
                registry.root_binding(&library.id)
                    == Some((root_identity.device(), root_identity.inode()))
            })
        };
        if existing_library.is_none() && identity_alias.is_some() {
            bail!("media library root object is already registered at another path");
        }
        let existing_binding = if let Some(library) = existing_library {
            self.media_libraries
                .lock()
                .map_err(poisoned_lock)?
                .root_binding(&library.id)
        } else {
            None
        };
        if let Some((device, inode)) = existing_binding
            && (device, inode) != (root_identity.device(), root_identity.inode())
        {
            bail!("registered path now names a different root object; it remains unavailable");
        }
        if RootedFs::new(&canonical_root)?.root_identity() != root_identity {
            bail!("media library root identity changed during registration");
        }

        // Open and own a newly registered root before publishing its registry entry. This keeps
        // owner-lock failures from leaving a second process's active queue root registered here.
        let existing_store = if let Some(existing_library) = existing_library {
            self.video_libraries
                .read()
                .map_err(poisoned_lock)?
                .iter()
                .find(|registered| registered.library_id == existing_library.id)
                .map(|registered| registered.store.clone())
        } else {
            None
        };
        if let Some(store) = &existing_store
            && store.root.root_identity() != root_identity
        {
            bail!("registered root identity no longer matches its open queue store");
        }
        let prepared_store = if existing_store.is_some() {
            None
        } else if root_identity == self.pdf.root.root_identity() {
            bail!("video media libraries may not alias the configured PDF root");
        } else {
            let store = DownloadStore::from_root(
                root_fs,
                Arc::clone(&self.pdf.file_provider),
                Arc::clone(&self.state_change_notify),
            )?;
            let owner = Arc::new(store.lock_queue_owner()?);
            let operation = store.lock_queue_operations()?;
            store.ensure_index()?;
            store.reconcile_index()?;
            drop(operation);
            Some((store, Some(owner)))
        };

        let library = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .add_with_root_identity(
                &name,
                &canonical_root,
                default_category.as_deref(),
                set_default,
                root_identity,
            )?;
        if existing_store.is_some() {
            self.set_video_store_available(&library.id, true)?;
        }
        if let Some((store, owner_lock)) = prepared_store {
            self.video_libraries
                .write()
                .map_err(poisoned_lock)?
                .push(RegisteredVideoStore {
                    library_id: library.id.clone(),
                    store,
                    available: true,
                    _owner_lock: owner_lock,
                });
        }
        let available_space_bytes = self
            .video_stores()?
            .into_iter()
            .find(|store| store.root_path == library.root)
            .and_then(|store| store.root.available_space_bytes().ok());
        self.video_statuses.write().map_err(poisoned_lock)?.insert(
            library.id.clone(),
            MediaLibraryStatus {
                available: true,
                available_space_bytes,
                diagnostic: None,
            },
        );
        self.state_change_notify.notify_one();
        Ok(library)
    }

    pub fn set_default_video_library(&self, library_id: &str) -> Result<MediaLibrarySnapshot> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        self.validate_video_library_for_settings_mutation_unlocked(library_id)?;
        let snapshot = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .set_default(library_id)?;
        self.state_change_notify.notify_one();
        Ok(snapshot)
    }

    pub fn set_default_video_library_for_migration_if_revision(
        &self,
        library_id: &str,
        migration_id: &str,
        expected_revision: u64,
    ) -> Result<MigrationDefaultUpdateOutcome> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let (resolved, revision_matches) = {
            let registry = self.media_libraries.lock().map_err(poisoned_lock)?;
            (
                registry.resolved_migration_default(migration_id)?,
                registry.snapshot().revision == expected_revision,
            )
        };
        if let Some(outcome) = resolved {
            self.state_change_notify.notify_one();
            return Ok(outcome);
        }
        // A stale migration must durably latch its skipped CAS result even when the selected
        // root is unavailable. Only an operation that can change the default needs root proof.
        if revision_matches {
            self.validate_video_library_for_settings_mutation_unlocked(library_id)?;
        }
        let outcome = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .set_default_for_migration_if_revision(library_id, migration_id, expected_revision)?;
        self.state_change_notify.notify_one();
        Ok(outcome)
    }

    pub fn set_video_library_category(
        &self,
        library_id: &str,
        category: Option<&Path>,
    ) -> Result<MediaLibrarySnapshot> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        self.validate_video_library_for_settings_mutation_unlocked(library_id)?;
        let snapshot = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .set_category(library_id, category)?;
        self.state_change_notify.notify_one();
        Ok(snapshot)
    }

    pub fn rename_video_library(
        &self,
        library_id: &str,
        name: &str,
    ) -> Result<MediaLibrarySnapshot> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let snapshot = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .rename(library_id, name)?;
        self.state_change_notify.notify_one();
        Ok(snapshot)
    }

    pub fn remove_video_library(&self, library_id: &str) -> Result<MediaLibrarySnapshot> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let library = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .library(library_id)
            .cloned();
        let Some(library) = library else {
            return self.registry_snapshot();
        };
        let store = self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .find(|registered| registered.library_id == library_id && registered.available)
            .map(|registered| registered.store.clone())
            .ok_or_else(|| {
                anyhow!("media library is offline; task dependencies cannot be checked")
            })?;
        let legacy_root_matches = same_trusted_root_path(&store.root_path, &library.root)?;
        if store.list_records()?.iter().any(|record| {
            !record.status.is_history()
                && !matches!(&record.job, JobRequest::Pdf { .. })
                && record.destination.as_ref().map_or_else(
                    || legacy_root_matches,
                    |destination| destination.library_id == library_id,
                )
        }) {
            bail!("media library still has video tasks that depend on it");
        }
        let snapshot = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .remove(library_id)?;
        self.video_libraries
            .write()
            .map_err(poisoned_lock)?
            .retain(|registered| registered.library_id != library_id);
        self.video_statuses
            .write()
            .map_err(poisoned_lock)?
            .remove(library_id);
        self.state_change_notify.notify_one();
        Ok(snapshot)
    }

    pub fn list_incomplete_task_transfer_snapshots(
        &self,
        source_library_id: &str,
        max_records: usize,
    ) -> Result<Vec<TaskTransferSnapshot>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let libraries = self.media_libraries.lock().map_err(poisoned_lock)?;
        let library = libraries
            .library(source_library_id)
            .cloned()
            .ok_or_else(|| anyhow!("unknown media library {source_library_id}"))?;
        let store = self
            .video_stores()?
            .into_iter()
            .find(|store| store.root_path == library.root)
            .ok_or_else(|| anyhow!("media library is currently unavailable"))?;
        let records = store.list_records()?;
        let limit = max_records.min(MAX_ACTIVE_RECORDS);
        let mut snapshots = Vec::new();
        for record in records.into_iter().filter(|record| {
            !matches!(record.status, TaskStatus::Completed | TaskStatus::Cancelled)
                && if let Some(destination) = &record.destination {
                    destination.library_id == source_library_id
                } else {
                    store.root_path == library.root
                        && !matches!(&record.job, JobRequest::Pdf { .. })
                }
        }) {
            if snapshots.len() >= limit {
                bail!("unfinished task selection exceeds the {limit}-record transfer limit");
            }
            let destination = record
                .destination
                .clone()
                .unwrap_or_else(|| TaskDestination {
                    library_id: source_library_id.to_string(),
                    root: library.root.clone(),
                    category: None,
                });
            let (staging_bytes, staging_file_count) =
                estimate_record_staging(&store.root, &store.root_path, &record)?;
            snapshots.push(TaskTransferSnapshot {
                record,
                destination,
                staging_bytes,
                staging_file_count,
            });
        }
        Ok(snapshots)
    }

    /// Return the bounded, authorized summary used by cloud settings projections. The task set
    /// is counted from active queue-index entries only; history sidecars are intentionally out of
    /// scope. A per-task staging estimate failure keeps the task count but makes the aggregate
    /// byte estimate unknown.
    pub fn incomplete_task_transfer_summary(
        &self,
        library_id: &str,
        allowed_chat_ids: &[i64],
    ) -> Result<IncompleteTaskTransferSummary> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        if allowed_chat_ids.is_empty() {
            return Ok(IncompleteTaskTransferSummary {
                task_count: 0,
                staging_bytes: Some(0),
            });
        }
        let library = self
            .registry_snapshot()?
            .libraries
            .into_iter()
            .find(|library| library.id == library_id)
            .ok_or_else(|| anyhow!("unknown media library {library_id}"))?;
        let store = self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .find(|registered| registered.library_id == library_id && registered.available)
            .map(|registered| registered.store.clone())
            .ok_or_else(|| anyhow!("media library {library_id} is currently unavailable"))?;

        let store_identity = store.root.root_identity();
        let binding = self
            .media_libraries
            .lock()
            .map_err(poisoned_lock)?
            .root_binding(library_id);
        if binding != Some((store_identity.device(), store_identity.inode())) {
            bail!("media library {library_id} queue root identity is unavailable or changed");
        }
        let current_root =
            coordinate_directory_root(self.pdf.file_provider.as_ref(), &library.root)?;
        if current_root.root_identity() != store_identity {
            bail!("media library {library_id} path now names a different root object");
        }

        let records = store.active_records_for_chats_complete(allowed_chat_ids)?;
        let mut task_count = 0_usize;
        let mut staging_bytes = Some(0_u64);
        for record in records {
            if record.status.is_history() || matches!(&record.job, JobRequest::Pdf { .. }) {
                continue;
            }
            match record.destination.as_ref() {
                Some(destination) if destination.library_id == library_id => {
                    if !same_trusted_root_path(&destination.root, &library.root)? {
                        bail!(
                            "task {} destination root does not match media library {library_id}",
                            record.id
                        );
                    }
                }
                Some(_) => continue,
                None => {
                    // A legacy record is owned by the active queue store that supplied it.
                    // Compare object identity rather than path spelling so trusted macOS aliases
                    // keep the same ownership semantics.
                    if store.root.root_identity() != current_root.root_identity() {
                        bail!(
                            "legacy task {} has an unverified media library root",
                            record.id
                        );
                    }
                }
            }
            task_count += 1;
            if staging_bytes.is_some() {
                match estimate_record_staging(&store.root, &store.root_path, &record) {
                    Ok((bytes, _)) => {
                        staging_bytes = staging_bytes.and_then(|total| total.checked_add(bytes));
                    }
                    Err(_) => staging_bytes = None,
                }
            }
        }
        Ok(IncompleteTaskTransferSummary {
            task_count,
            staging_bytes,
        })
    }

    /// Check durable queue records for a transfer owned by `migration_id` without reconciling or
    /// rewriting queue state. Terminal records are included because the engine may still need to
    /// acknowledge or clean a transfer after its user-facing migration is stopped or completed.
    /// Any offline, identity-mismatched, or unreadable registered store makes a negative answer
    /// uncertain and is returned as an error.
    pub fn has_pending_task_destination_transfer_for_migration(
        &self,
        migration_id: &str,
    ) -> Result<bool> {
        validate_task_transfer_migration_id(migration_id)?;
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;

        let (libraries, bindings) = {
            let registry = self.media_libraries.lock().map_err(poisoned_lock)?;
            let libraries = registry.snapshot().libraries;
            let bindings = libraries
                .iter()
                .map(|library| (library.id.clone(), registry.root_binding(&library.id)))
                .collect::<BTreeMap<_, _>>();
            (libraries, bindings)
        };
        let stores = self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .map(|registered| {
                (
                    registered.library_id.clone(),
                    (registered.store.clone(), registered.available),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let statuses = self.video_statuses.read().map_err(poisoned_lock)?.clone();

        for library in libraries {
            let library_id = library.id.clone();
            let (store, store_available) = stores.get(&library_id).ok_or_else(|| {
                anyhow!(
                    "cannot verify migration tokens: media library {library_id} has no queue store"
                )
            })?;
            if !store_available {
                bail!("cannot verify migration tokens: media library {library_id} is unavailable");
            }
            let status = statuses.get(&library_id).ok_or_else(|| {
                anyhow!("cannot verify migration tokens: media library {library_id} has no availability status")
            })?;
            if !status.available {
                bail!(
                    "cannot verify migration tokens: media library {library_id} is unavailable: {}",
                    status
                        .diagnostic
                        .as_deref()
                        .unwrap_or("availability was not confirmed")
                );
            }
            let expected_binding = bindings
                .get(&library_id)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    anyhow!(
                        "cannot verify migration tokens: media library {library_id} has no persisted root identity"
                    )
                })?;
            let store_identity = store.root.root_identity();
            if expected_binding != (store_identity.device(), store_identity.inode()) {
                bail!(
                    "cannot verify migration tokens: media library {library_id} queue store identity changed"
                );
            }
            let current_root = coordinate_directory_root(
                self.pdf.file_provider.as_ref(),
                &library.root,
            )
            .with_context(|| {
                format!(
                    "cannot verify migration tokens: media library {library_id} root is unavailable"
                )
            })?;
            if current_root.root_identity() != store_identity {
                bail!(
                    "cannot verify migration tokens: media library {library_id} path names a different root object"
                );
            }

            let index = store
                .read_index()
                .with_context(|| format!("failed to inspect {library_id} task index"))?;
            if index.tasks.len() > MAX_ACTIVE_RECORDS + MAX_HISTORY_INDEX_ENTRIES {
                bail!(
                    "cannot verify migration tokens: media library {library_id} task index exceeds its record limit"
                );
            }
            for (task_id, entry) in index.tasks {
                validate_task_id(&task_id)?;
                let mut paths = vec![entry.record_path];
                if let Some(move_target) = entry.move_target
                    && !paths.contains(&move_target)
                {
                    paths.push(move_target);
                }
                let mut found_record = false;
                for path in paths {
                    let Some(record) = store.read_record(&path).with_context(|| {
                        format!("failed to inspect task {task_id} in media library {library_id}")
                    })?
                    else {
                        continue;
                    };
                    if record.id != task_id || record.chat_id != entry.chat_id {
                        bail!(
                            "media library {library_id} task index points to a mismatched record for task {task_id}"
                        );
                    }
                    found_record = true;
                    if record
                        .destination_transfer
                        .as_ref()
                        .is_some_and(|transfer| transfer.migration_id == migration_id)
                        || record
                            .destination_transfer_ack
                            .as_ref()
                            .is_some_and(|transfer| transfer.migration_id == migration_id)
                    {
                        return Ok(true);
                    }
                }
                if !found_record {
                    bail!(
                        "media library {library_id} task index points to missing records for task {task_id}"
                    );
                }
            }
        }
        Ok(false)
    }

    pub fn begin_task_destination_transfer(
        &self,
        task_id: &str,
        migration_id: &str,
        expected_generation: u64,
        expected_revision: u64,
        target: TaskDestination,
    ) -> Result<TaskDestinationTransfer> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((source_store, mut record, entry)) = self.find_record_entry_unlocked(task_id)?
        else {
            bail!("task {task_id} was not found");
        };
        validate_task_transfer_migration_id(migration_id)?;
        if record.destination_transfer_ack.is_some() {
            bail!("task {task_id} is awaiting durable media migration handoff finalization");
        }
        if let Some(existing) = record.destination_transfer.as_ref() {
            if existing.target == target && existing.migration_id == migration_id {
                return Ok(existing.clone());
            }
            bail!("task {task_id} already has a transfer owned by another migration");
        }
        if record.generation != expected_generation || record.revision != expected_revision {
            bail!("task {task_id} changed after the migration preview");
        }
        if matches!(record.job, JobRequest::Pdf { .. })
            || matches!(record.status, TaskStatus::Completed | TaskStatus::Cancelled)
        {
            bail!("task {task_id} is not eligible for video library migration");
        }

        let snapshot = self.registry_snapshot()?;
        validate_task_destination(&snapshot, &target)?;
        let target_store = self
            .video_stores()?
            .into_iter()
            .find(|store| store.root_path == target.root)
            .ok_or_else(|| anyhow!("target media library is currently unavailable"))?;
        if source_store.shares_root(&target_store) {
            bail!("task migration requires distinct source and target library roots");
        }
        let source = match record.destination.clone() {
            Some(destination) => destination,
            None => {
                let library = snapshot
                    .libraries
                    .iter()
                    .find(|library| library.root == source_store.root_path)
                    .ok_or_else(|| anyhow!("task source root is no longer registered"))?;
                TaskDestination {
                    library_id: library.id.clone(),
                    root: source_store.root_path.clone(),
                    category: None,
                }
            }
        };
        if source.root != source_store.root_path {
            bail!("task destination does not match the queue root that owns its record");
        }
        let started_generation = record.generation;
        let previous_status = record.status;
        let previous_error = record.error.clone();
        let previous_cancel_requested = record.cancel_requested;
        let active_registration = self
            .cancellations
            .lock()
            .map_err(poisoned_lock)?
            .get(task_id)
            .filter(|registration| {
                registration.generation == Some(started_generation)
                    || registration.generation.is_none()
            })
            .map(|registration| Arc::clone(&registration.notify));
        let active_status = matches!(
            previous_status,
            TaskStatus::Preparing | TaskStatus::Running | TaskStatus::Verifying
        );
        let pause_generation =
            (active_status || active_registration.is_some()).then_some(started_generation);
        if pause_generation.is_some() {
            record.generation = record
                .generation
                .checked_add(1)
                .ok_or_else(|| anyhow!("task {task_id} generation overflow"))?;
            record.status = TaskStatus::Interrupted;
            record.cancel_requested = false;
            record.error = Some("Paused for media library transfer.".to_string());
        }
        let transfer = TaskDestinationTransfer {
            transfer_id: format!(
                "xfer-{}-{}-{}",
                unix_time(),
                TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed),
                task_id
            ),
            migration_id: migration_id.to_string(),
            task_id: task_id.to_string(),
            started_generation,
            generation: record.generation,
            revision: record.revision.saturating_add(1),
            source,
            target,
            previous_status,
            previous_error,
            previous_cancel_requested,
            pause_generation,
            plan_fingerprint: None,
        };
        record.destination_transfer = Some(transfer.clone());
        record.destination_transfer_phase = Some(TaskDestinationTransferPhase::Copying);
        record.destination_transfer_ack = None;
        let persisted = source_store.save_mutated_record(record, entry, false)?;
        let transfer = persisted
            .destination_transfer
            .expect("saved task retains its destination transfer token");
        drop(_guard);
        if let Some(notify) = active_registration {
            notify.notify_one();
        }
        self.state_change_notify.notify_one();
        Ok(transfer)
    }

    pub async fn wait_for_task_destination_transfer(
        &self,
        transfer: &TaskDestinationTransfer,
    ) -> Result<()> {
        loop {
            let changed = self.state_change_notify.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let Some(pause_generation) = transfer.pause_generation else {
                return Ok(());
            };
            let still_registered = {
                let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
                let Some((_, record, _)) = self.find_record_entry_unlocked(&transfer.task_id)?
                else {
                    bail!(
                        "task {} disappeared while pausing for transfer",
                        transfer.task_id
                    );
                };
                if record
                    .destination_transfer
                    .as_ref()
                    .map(|value| value.transfer_id.as_str())
                    != Some(transfer.transfer_id.as_str())
                {
                    bail!(
                        "task {} transfer token changed while waiting",
                        transfer.task_id
                    );
                }
                self.cancellations
                    .lock()
                    .map_err(poisoned_lock)?
                    .get(&transfer.task_id)
                    .is_some_and(|registration| {
                        registration.generation == Some(pause_generation)
                            || registration.generation.is_none()
                    })
            };
            if !still_registered {
                return Ok(());
            }
            changed.await;
        }
    }

    #[cfg(test)]
    pub fn task_transfer_plan(
        &self,
        transfer: &TaskDestinationTransfer,
    ) -> Result<TaskTransferPlan> {
        self.task_transfer_plan_with_published_paths(transfer, &BTreeMap::new())
    }

    pub fn task_transfer_plan_with_published_paths(
        &self,
        transfer: &TaskDestinationTransfer,
        published_path_rewrites: &BTreeMap<PathBuf, PathBuf>,
    ) -> Result<TaskTransferPlan> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let source_store = self
            .store_for_root(&transfer.source.root)?
            .ok_or_else(|| anyhow!("task transfer source library is unavailable"))?;
        let Some((mut record, entry)) = source_store.get_task(&transfer.task_id)? else {
            bail!("task {} is no longer in its source queue", transfer.task_id);
        };
        let current = record
            .destination_transfer
            .as_ref()
            .ok_or_else(|| anyhow!("task {} has no pending transfer", transfer.task_id))?;
        if current.transfer_id != transfer.transfer_id
            || current.migration_id != transfer.migration_id
            || current.target != transfer.target
        {
            bail!("task {} transfer token does not match", transfer.task_id);
        }
        if current.pause_generation.is_some() && self.cancellation_is_registered(current)? {
            bail!(
                "task {} still has an active writer registered",
                transfer.task_id
            );
        }
        let plan =
            build_task_transfer_plan(&source_store, &record, current, published_path_rewrites)?;
        if let Some(expected) = &current.plan_fingerprint {
            if expected != &plan.fingerprint {
                bail!(
                    "task {} staging changed after its transfer plan was recorded",
                    transfer.task_id
                );
            }
            return Ok(plan);
        }
        record
            .destination_transfer
            .as_mut()
            .expect("transfer token was checked above")
            .plan_fingerprint = Some(plan.fingerprint.clone());
        let saved = source_store.save_mutated_record(record, entry, false)?;
        let mut plan = plan;
        plan.transfer = saved
            .destination_transfer
            .ok_or_else(|| anyhow!("task transfer token disappeared while saving its plan"))?;
        Ok(plan)
    }

    pub fn verify_task_transfer_plan(&self, plan: &TaskTransferPlan) -> Result<()> {
        validate_task_transfer_plan_shape(plan)?;
        let source_store = self
            .store_for_root(&plan.source_root)?
            .ok_or_else(|| anyhow!("task transfer source library is unavailable"))?;
        let target_store = self
            .store_for_root(&plan.target_root)?
            .ok_or_else(|| anyhow!("task transfer target library is unavailable"))?;
        let source_record = source_store.get_task(&plan.transfer.task_id)?;
        let target_record = target_store.get_task(&plan.transfer.task_id)?;
        let matching = |record: &TaskRecord| {
            record.destination_transfer.as_ref() == Some(&plan.transfer)
                && plan.transfer.plan_fingerprint.as_deref() == Some(&plan.fingerprint)
        };
        if !source_record
            .as_ref()
            .is_some_and(|(record, _)| matching(record))
            && !target_record
                .as_ref()
                .is_some_and(|(record, _)| matching(record))
        {
            bail!("task transfer plan no longer matches either queue record");
        }
        for file in &plan.files {
            verify_transfer_file(&source_store.root, &source_store.root_path, file, true)?;
            verify_transfer_file(&target_store.root, &target_store.root_path, file, false)?;
        }
        for control in &plan.controls {
            verify_transfer_control(&source_store.root, &source_store.root_path, control)?;
        }
        verify_transfer_tree(&source_store.root, &source_store.root_path, plan, true)?;
        for job_relative in transfer_staging_jobs(plan)? {
            let source_job = source_store
                .root
                .entry_identity(&source_store.root_path.join(&job_relative))?
                .context("task transfer source staging job disappeared")?;
            let job_controls = plan
                .controls
                .iter()
                .filter(|control| control.source_relative.parent() == Some(job_relative.as_path()))
                .cloned()
                .collect::<Vec<_>>();
            let receipts = crate::downloader::inspect_video_staging_for_migration(
                &source_store.root,
                &job_relative,
                source_job,
                &job_controls,
            )?;
            verify_source_control_receipts(
                &source_store.root,
                &source_store.root_path,
                &job_controls,
                &receipts,
            )?;
        }
        for job_relative in transfer_staging_jobs(plan)? {
            let source_job = source_store
                .root
                .entry_identity(&source_store.root_path.join(&job_relative))?
                .context("task transfer source staging job disappeared")?;
            let job_controls = plan
                .controls
                .iter()
                .filter(|control| control.source_relative.parent() == Some(job_relative.as_path()))
                .cloned()
                .collect::<Vec<_>>();
            let receipts = crate::downloader::rebind_video_staging_controls_for_migration(
                &source_store.root,
                &target_store.root,
                &job_relative,
                &plan.transfer.transfer_id,
                source_job,
                &job_controls,
            )?;
            verify_rebound_control_receipts(
                &target_store.root,
                &target_store.root_path,
                &job_relative,
                &job_controls,
                &receipts,
            )?;
        }
        verify_transfer_tree(&target_store.root, &target_store.root_path, plan, false)?;
        Ok(())
    }

    pub fn commit_task_destination_transfer(&self, plan: &TaskTransferPlan) -> Result<TaskRecord> {
        self.verify_task_transfer_plan(plan)?;
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let source_store = self
            .store_for_root(&plan.source_root)?
            .ok_or_else(|| anyhow!("task transfer source library is unavailable"))?;
        let target_store = self
            .store_for_root(&plan.target_root)?
            .ok_or_else(|| anyhow!("task transfer target library is unavailable"))?;
        // Revalidate under the queue operation lock immediately before changing record ownership.
        // This also rebinds the target controls idempotently if a prior commit attempt stopped
        // after writing the target staging tree but before installing its queue record.
        self.verify_task_transfer_plan(plan)?;
        if let Some((target_record, _)) = target_store.get_task(&plan.transfer.task_id)? {
            if target_record.destination_transfer.as_ref() == Some(&plan.transfer)
                && target_record.destination_transfer_phase
                    == Some(TaskDestinationTransferPhase::TargetInstalled)
            {
                if source_store.get_task(&plan.transfer.task_id)?.is_some() {
                    source_store.remove_task_for_transfer(&plan.transfer)?;
                }
                return Ok(target_record);
            }
            bail!("target queue already contains a different task with this ID");
        }
        let Some((source_record, _source_entry)) = source_store.get_task(&plan.transfer.task_id)?
        else {
            bail!(
                "task {} is missing from its source queue",
                plan.transfer.task_id
            );
        };
        let current = source_record
            .destination_transfer
            .as_ref()
            .ok_or_else(|| anyhow!("task {} has no pending transfer", plan.transfer.task_id))?;
        if current.transfer_id != plan.transfer.transfer_id
            || current.migration_id != plan.transfer.migration_id
            || current.plan_fingerprint.as_deref() != Some(&plan.fingerprint)
        {
            bail!(
                "task {} transfer plan does not match its source record",
                plan.transfer.task_id
            );
        }

        let mut target_record = source_record.clone();
        rebase_task_paths(
            &mut target_record,
            &plan.source_root,
            &plan.target_root,
            &plan.published_path_rewrites,
        )?;
        target_record.destination = Some(plan.transfer.target.clone());
        target_record.destination_transfer_phase =
            Some(TaskDestinationTransferPhase::TargetInstalled);
        target_record.updated_at = unix_time();
        target_record.revision = source_record.revision.saturating_add(1);
        target_record.activity_revision = 0;
        target_store.create_task(&mut target_record)?;
        source_store.remove_task_for_transfer(&plan.transfer)?;
        let installed = target_store
            .get_task(&plan.transfer.task_id)?
            .map(|(record, _)| record)
            .ok_or_else(|| anyhow!("target queue record disappeared after task transfer"))?;
        self.state_change_notify.notify_one();
        Ok(installed)
    }

    pub fn abort_task_destination_transfer(
        &self,
        transfer: &TaskDestinationTransfer,
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) =
            self.find_record_entry_unlocked(&transfer.task_id)?
        else {
            return Ok(None);
        };
        let Some(current) = record.destination_transfer.as_ref() else {
            return Ok(Some(record));
        };
        if current.transfer_id != transfer.transfer_id
            || current.migration_id != transfer.migration_id
        {
            bail!("task destination transfer is owned by another migration");
        }
        if record.destination_transfer_phase == Some(TaskDestinationTransferPhase::TargetInstalled)
            || record.destination.as_ref() == Some(&transfer.target)
        {
            bail!("an installed task transfer cannot be aborted");
        }
        let current = current.clone();
        restore_after_task_transfer(&mut record, &current);
        let saved = store.save_mutated_record(record, entry, true)?;
        self.state_change_notify.notify_one();
        Ok(Some(saved))
    }

    pub fn ack_task_destination_transfer(
        &self,
        transfer: &TaskDestinationTransfer,
    ) -> Result<Option<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let target_store = self.store_for_root(&transfer.target.root)?;
        let source_store = self.store_for_root(&transfer.source.root)?;
        let Some(store) = target_store
            .filter(|store| store.get_task(&transfer.task_id).ok().flatten().is_some())
            .or(source_store)
        else {
            return Ok(None);
        };
        let Some((mut record, entry)) = store.get_task(&transfer.task_id)? else {
            return Ok(None);
        };
        let Some(current) = record.destination_transfer.as_ref() else {
            if record.destination.as_ref() == Some(&transfer.target)
                && record.destination_transfer_ack.as_ref() == Some(transfer)
            {
                return Ok(Some(record));
            }
            bail!("task has no exact matching destination transfer acknowledgment");
        };
        if current != transfer {
            bail!("task destination transfer is owned by another migration");
        }
        if record.destination.as_ref() != Some(&transfer.target) {
            bail!("task destination transfer was not committed to its target queue");
        }
        let current = current.clone();
        restore_after_task_transfer(&mut record, &current);
        record.destination_transfer_ack = Some(current);
        let saved = store.save_mutated_record(record, entry, true)?;
        Ok(Some(saved))
    }

    /// Clear the exact handoff receipt only after the migration engine has durably recorded its
    /// final queue acknowledgment and completed source payload, control, and directory cleanup.
    /// Until then, the receipt keeps task actions and destination changes blocked across restarts.
    pub fn clear_task_destination_transfer_ack(
        &self,
        transfer: &TaskDestinationTransfer,
    ) -> Result<()> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) =
            self.find_record_entry_unlocked(&transfer.task_id)?
        else {
            bail!("task disappeared before its handoff acknowledgment was consumed");
        };
        match record.destination_transfer_ack.as_ref() {
            Some(current) if current == transfer => {
                record.destination_transfer_ack = None;
                store.save_mutated_record(record, entry, false)?;
                self.state_change_notify.notify_one();
                Ok(())
            }
            None => Ok(()),
            Some(_) => bail!("task has a different destination transfer acknowledgment"),
        }
    }

    pub fn create(&self, mut task: TaskRecord) -> Result<bool> {
        if let Some(snapshot) = task.bilibili_attempt.as_mut() {
            snapshot.sanitize_errors();
        }
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        if !matches!(task.job, JobRequest::Pdf { .. }) {
            let libraries = self.media_libraries.lock().map_err(poisoned_lock)?;
            if task.destination.is_none() {
                task.destination = Some(libraries.default_destination()?);
            }
            validate_task_destination(
                &libraries.snapshot(),
                task.destination
                    .as_ref()
                    .expect("video task destination was frozen"),
            )?;
        }
        if self.find_record_unlocked(&task.id)?.is_some() {
            return Ok(false);
        }
        let store = self.store_for_task(&task)?;
        let _operation = store.lock_queue_operations()?;
        store.create_task(&mut task)?;
        self.state_change_notify.notify_one();
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
        let mut records = self.all_records_isolated()?;
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

    /// Return one stable bounded snapshot of active tasks for explicitly allowed chats.
    /// The operation lock covers both queue stores and selection so task updates cannot reorder
    /// records between offset pages. Equal timestamps retain the old per-chat list order.
    pub fn active_records_for_chats(
        &self,
        chat_ids: &[i64],
        max_records: usize,
    ) -> Result<Vec<TaskRecord>> {
        self.active_records_for_chats_with_snapshot_hook(chat_ids, max_records, || {})
    }

    #[cfg(test)]
    fn active_records_for_chats_with_test_hook<F>(
        &self,
        chat_ids: &[i64],
        max_records: usize,
        after_snapshot: F,
    ) -> Result<Vec<TaskRecord>>
    where
        F: FnOnce(),
    {
        self.active_records_for_chats_with_snapshot_hook(chat_ids, max_records, after_snapshot)
    }

    fn active_records_for_chats_with_snapshot_hook<F>(
        &self,
        chat_ids: &[i64],
        max_records: usize,
        after_snapshot: F,
    ) -> Result<Vec<TaskRecord>>
    where
        F: FnOnce(),
    {
        if chat_ids.is_empty() || max_records == 0 {
            return Ok(Vec::new());
        }
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        // History sidecars stay in the index for pagination, but a task-summary refresh only
        // needs a bounded set of entries that the index still locates in the active queue.
        // Reading every history record here made each progress update pay for the full history.
        let mut records = Vec::new();
        for (library_id, store) in self.registered_video_stores()? {
            match store.active_summary_records_for_chats(chat_ids, max_records) {
                Ok(library_records) => records.extend(library_records),
                Err(error) => {
                    self.note_video_store_read_error(&library_id, &store, &error);
                    tracing::warn!(library_id = %library_id, error = %error, "could not read media library queue while summarizing tasks");
                }
            }
        }
        if !self
            .video_stores()?
            .iter()
            .any(|store| store.shares_root(&self.pdf))
        {
            records.extend(
                self.pdf
                    .active_summary_records_for_chats(chat_ids, max_records)?,
            );
        }
        after_snapshot();

        // Match the previous per-chat page concatenation order for stable timestamp ties, while
        // avoiding duplicate publication if a configured chat ID appears more than once.
        let mut chat_order = HashMap::with_capacity(chat_ids.len());
        for (index, chat_id) in chat_ids.iter().enumerate() {
            chat_order.entry(*chat_id).or_insert(index);
        }
        let mut selected = records
            .into_iter()
            .enumerate()
            .filter_map(|(record_order, record)| {
                if record.status.is_history() {
                    return None;
                }
                let configured_chat_order = *chat_order.get(&record.chat_id)?;
                Some((configured_chat_order, record_order, record))
            })
            .collect::<Vec<_>>();
        selected.sort_by_key(|(configured_chat_order, record_order, record)| {
            (
                std::cmp::Reverse(record.updated_at),
                *configured_chat_order,
                *record_order,
            )
        });
        selected.truncate(max_records);
        Ok(selected.into_iter().map(|(_, _, record)| record).collect())
    }

    /// Return legacy completed records and tasks with verified per-entry output receipts.
    ///
    /// This is used to seed the media library's source hints. It intentionally does not filter
    /// by chat ID because the local library belongs to the single configured bot instance and
    /// `allow_all_chats` has no finite configured chat list.
    pub fn published_records(&self) -> Result<Vec<TaskRecord>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let mut records = self.all_records_isolated()?;
        records.retain(|record| {
            if record.bilibili_attempt.is_some() {
                !verified_bilibili_receipt_keys(record).is_empty()
            } else {
                record.status == TaskStatus::Completed && record.saved_location.is_some()
            }
        });
        records.sort_by_key(|record| std::cmp::Reverse(record.updated_at));
        Ok(records)
    }

    pub fn page_count(&self, chat_id: i64, history: bool) -> Result<usize> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let records = self.all_records_isolated()?;
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
        let Some((store, record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        ensure_task_transfer_not_pending(&record)?;
        let _store_operation_lock = store.lock_queue_operations()?;
        Self::claim_resume_in_store(
            &store,
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
        let Some((store, record, entry)) = self.find_record_entry_unlocked(id)? else {
            return Ok(None);
        };
        ensure_task_transfer_not_pending(&record)?;
        let _store_operation_lock = store.lock_queue_operations()?;
        Self::claim_episode_retry_in_store(
            &store,
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
        if task_transfer_or_final_ack_pending(&record) {
            return Ok(false);
        }
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
        if task_transfer_or_final_ack_pending(&record) {
            return Ok(None);
        }
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
        if task_transfer_or_final_ack_pending(&record) {
            return Ok(None);
        }
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
        if task_transfer_or_final_ack_pending(&record) {
            return Ok(None);
        }
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
        mut current: PlanValidationSnapshot,
    ) -> Result<Option<(TaskRecord, Vec<&'static str>)>> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        let Some((store, mut record, entry)) = self.find_record_entry_unlocked(id)? else {
            if expected_generation.is_some() {
                return Ok(None);
            }
            bail!("persistent task {id} was not found");
        };
        if task_transfer_or_final_ack_pending(&record) {
            if expected_generation.is_some() {
                return Ok(None);
            }
            ensure_task_transfer_not_pending(&record)?;
        }
        if expected_generation.is_some_and(|generation| record.generation != generation) {
            return Ok(None);
        }
        if record.status != TaskStatus::Running {
            if expected_generation.is_some() {
                return Ok(None);
            }
            bail!("task {id} changed state before plan validation completed");
        }
        if !current.bilibili_group_entries.is_empty() {
            current.bilibili_publication_namespace = Some(record.bilibili_publication_namespace());
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
        ensure_task_transfer_not_pending(&record)?;
        if expected_generation.is_some_and(|generation| record.generation != generation)
            || expected_chat_id.is_some_and(|chat_id| record.chat_id != chat_id)
            || record.status != TaskStatus::AwaitingConfirmation
        {
            return Ok(None);
        }
        let Some(mut plan) = record.proposed_plan.take() else {
            return Ok(None);
        };
        if !plan.bilibili_group_entries.is_empty() {
            plan.bilibili_publication_namespace = Some(record.bilibili_publication_namespace());
        }
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
        if task_transfer_or_final_ack_pending(&record) || record.generation != expected_generation {
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
        let mut added_receipt = false;
        let result = self.update_if_generation(id, expected_generation, true, |record| {
            let previous_receipts = verified_bilibili_receipt_keys(record);
            if record.status == TaskStatus::Completed && snapshot.independent_entries {
                return Ok(());
            }
            snapshot.sanitize_errors();
            stamp_bilibili_publication_namespace(record, &mut snapshot);
            if let Some(outcome) = terminal_bilibili_attempt_outcome(record.status) {
                snapshot.finish(outcome);
            }
            record.bilibili_attempt = Some(snapshot);
            refresh_bilibili_attempt_counts(record);
            added_receipt = verified_bilibili_receipt_keys(record)
                .difference(&previous_receipts)
                .next()
                .is_some();
            Ok(())
        })?;
        if result.is_some() && added_receipt {
            self.publication_notify.notify_one();
        }
        Ok(result)
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
        if task_transfer_or_final_ack_pending(&record) || record.generation != expected_generation {
            return Ok(None);
        }
        if updates.is_empty() {
            return Ok(Some(record));
        }
        let previous_receipts = verified_bilibili_receipt_keys(&record);
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
        let added_receipt = verified_bilibili_receipt_keys(&record)
            .difference(&previous_receipts)
            .next()
            .is_some();
        let record = store.save_mutated_record(record, entry, true)?;
        if added_receipt {
            self.publication_notify.notify_one();
        }
        Ok(Some(record))
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
        if let Some(record) = self.find_record_unlocked(id)? {
            ensure_task_transfer_not_pending(&record)?;
        }
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
        if task_transfer_or_final_ack_pending(&record)
            || record.generation != expected_generation
            || record.status != TaskStatus::Queued
        {
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
        self.state_change_notify.notify_waiters();
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
            drop(active);
            self.state_change_notify.notify_waiters();
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
        let result = self.complete_if_generation_unnotified(
            id,
            expected_generation,
            saved_location,
            media_paths,
            hashes,
        )?;
        if result.as_ref().is_some_and(|record| {
            record.status == TaskStatus::Completed && record.saved_location.is_some()
        }) {
            self.publication_notify.notify_one();
        }
        Ok(result)
    }

    fn complete_if_generation_unnotified(
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
        if task_transfer_or_final_ack_pending(&record) {
            return Ok(None);
        }
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
                .finish_completed_record_sidecar(&store, record, entry, destination)
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
                .finish_completed_record_sidecar(&store, record, entry, None)
                .map(Some);
        };
        record.updated_at = unix_time();
        record.revision = record.revision.saturating_add(1);
        entry.move_target = Some(destination.clone());
        entry.revision = record.revision;
        entry.updated_at = record.updated_at;
        store.write_record(&record_path, &record)?;
        store.save_index_task(id, entry.clone())?;
        self.finish_completed_record_sidecar(&store, record, entry, Some(destination))
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
            store.cleanup_terminal_bilibili_group_receipts(&record);
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
            store.cleanup_terminal_bilibili_group_receipts(&record);
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
                store.cleanup_terminal_bilibili_group_receipts(&record);
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
        self.fail_with_bilibili_attempt_if_generation_inner(
            id,
            expected_generation,
            message,
            snapshot,
            false,
        )
    }

    pub fn fail_with_bilibili_attempt_if_generation_unless_cancelled(
        &self,
        id: &str,
        expected_generation: u64,
        message: String,
        snapshot: Option<BilibiliAttemptSnapshot>,
    ) -> Result<Option<TaskRecord>> {
        self.fail_with_bilibili_attempt_if_generation_inner(
            id,
            expected_generation,
            message,
            snapshot,
            true,
        )
    }

    fn fail_with_bilibili_attempt_if_generation_inner(
        &self,
        id: &str,
        expected_generation: u64,
        message: String,
        snapshot: Option<BilibiliAttemptSnapshot>,
        reject_pending_cancellation: bool,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_generation(id, expected_generation, true, |record| {
            if matches!(record.status, TaskStatus::Cancelled | TaskStatus::Completed) {
                bail!("task {id} is already terminal");
            }
            if reject_pending_cancellation && record.cancel_requested {
                bail!("task {id} has a pending cancellation request");
            }
            if let Some(mut snapshot) = snapshot {
                snapshot.sanitize_errors();
                stamp_bilibili_publication_namespace(record, &mut snapshot);
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
        self.interrupt_with_bilibili_attempt_if_generation_inner(
            id,
            expected_generation,
            message,
            snapshot,
            false,
        )
    }

    pub fn interrupt_with_bilibili_attempt_if_generation_unless_cancelled(
        &self,
        id: &str,
        expected_generation: u64,
        message: String,
        snapshot: Option<BilibiliAttemptSnapshot>,
    ) -> Result<Option<TaskRecord>> {
        self.interrupt_with_bilibili_attempt_if_generation_inner(
            id,
            expected_generation,
            message,
            snapshot,
            true,
        )
    }

    fn interrupt_with_bilibili_attempt_if_generation_inner(
        &self,
        id: &str,
        expected_generation: u64,
        message: String,
        snapshot: Option<BilibiliAttemptSnapshot>,
        reject_pending_cancellation: bool,
    ) -> Result<Option<TaskRecord>> {
        self.update_if_generation(id, expected_generation, true, |record| {
            if matches!(record.status, TaskStatus::Cancelled | TaskStatus::Completed) {
                bail!("task {id} is already terminal");
            }
            if reject_pending_cancellation && record.cancel_requested {
                bail!("task {id} has a pending cancellation request");
            }
            if let Some(mut snapshot) = snapshot {
                snapshot.sanitize_errors();
                stamp_bilibili_publication_namespace(record, &mut snapshot);
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
        ensure_task_transfer_not_pending(&record)?;
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
        if task_transfer_or_final_ack_pending(&record) {
            return Ok(None);
        }
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
        if task_transfer_or_final_ack_pending(&record) {
            return Ok(None);
        }
        if record.generation != expected_generation
            || !record.cancel_requested
            || !matches!(
                record.status,
                TaskStatus::Running | TaskStatus::AwaitingConfirmation
            )
        {
            return Ok(None);
        }
        if let Some(mut snapshot) = snapshot {
            stamp_bilibili_publication_namespace(&record, &mut snapshot);
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
        let mut summaries = BTreeMap::<i64, RestartSummary>::new();
        for store in self.task_stores()? {
            let (records, activities) = store.restart_summary_data()?;
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
        for store in self.task_stores()? {
            store.mark_notified(chat_id)?;
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
        ensure_task_transfer_not_pending(&record)?;
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
        if task_transfer_or_final_ack_pending(&record) {
            return Ok(None);
        }
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
        if task_transfer_or_final_ack_pending(&record) {
            return Ok(None);
        }
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
        if task_transfer_or_final_ack_pending(&record) {
            return Ok(None);
        }
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
    ) -> Result<Option<(DownloadStore, TaskRecord, TaskIndexEntry)>> {
        let mut pdf_is_video_root = false;
        let mut first_video_read_error = None;
        for (library_id, store) in self.registered_video_stores()? {
            pdf_is_video_root |= store.shares_root(&self.pdf);
            match store.get_task(id) {
                Ok(Some((record, entry))) => return Ok(Some((store, record, entry))),
                Ok(None) => {}
                Err(error) => {
                    self.note_video_store_read_error(&library_id, &store, &error);
                    tracing::warn!(library_id = %library_id, error = %error, "could not read media library queue while resolving a task");
                    if first_video_read_error.is_none() {
                        first_video_read_error = Some(error);
                    }
                }
            }
        }
        if !pdf_is_video_root && let Some((record, entry)) = self.pdf.get_task(id)? {
            return Ok(Some((self.pdf.clone(), record, entry)));
        }
        match first_video_read_error {
            Some(error) => Err(error.context("failed to search available media library queues")),
            None => Ok(None),
        }
    }

    fn store_for_task(&self, task: &TaskRecord) -> Result<DownloadStore> {
        if matches!(task.job, JobRequest::Pdf { .. }) {
            return Ok(self.pdf.clone());
        }
        let destination = task
            .destination
            .as_ref()
            .ok_or_else(|| anyhow!("video task {} has no frozen destination", task.id))?;
        self.video_stores()?
            .into_iter()
            .find(|store| store.root_path == destination.root)
            .ok_or_else(|| {
                anyhow!(
                    "video library {} is currently unavailable",
                    destination.library_id
                )
            })
    }

    fn video_stores(&self) -> Result<Vec<DownloadStore>> {
        Ok(self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .filter(|registered| registered.available)
            .map(|registered| registered.store.clone())
            .collect())
    }

    fn store_for_root(&self, root: &Path) -> Result<Option<DownloadStore>> {
        Ok(self
            .video_stores()?
            .into_iter()
            .find(|store| store.root_path == root))
    }

    fn cancellation_is_registered(&self, transfer: &TaskDestinationTransfer) -> Result<bool> {
        let Some(generation) = transfer.pause_generation else {
            return Ok(false);
        };
        Ok(self
            .cancellations
            .lock()
            .map_err(poisoned_lock)?
            .get(&transfer.task_id)
            .is_some_and(|registration| {
                registration.generation == Some(generation) || registration.generation.is_none()
            }))
    }

    fn registered_video_stores(&self) -> Result<Vec<(String, DownloadStore)>> {
        Ok(self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .filter(|registered| registered.available)
            .map(|registered| (registered.library_id.clone(), registered.store.clone()))
            .collect())
    }

    fn task_stores(&self) -> Result<Vec<DownloadStore>> {
        let mut stores = self.video_stores()?;
        if !stores.iter().any(|store| store.shares_root(&self.pdf)) {
            stores.push(self.pdf.clone());
        }
        Ok(stores)
    }

    fn all_records_isolated(&self) -> Result<Vec<TaskRecord>> {
        let mut records = Vec::new();
        let mut first_retryable_read_error = None;
        for (library_id, store) in self.registered_video_stores()? {
            match store.list_records() {
                Ok(library_records) => records.extend(library_records),
                Err(error) => {
                    tracing::warn!(library_id = %library_id, error = %error, "could not read media library queue while listing tasks");
                    let root_is_available =
                        self.note_video_store_read_error(&library_id, &store, &error);
                    if root_is_available && first_retryable_read_error.is_none() {
                        first_retryable_read_error = Some(error);
                    }
                }
            }
        }
        if !self
            .video_stores()?
            .iter()
            .any(|store| store.shares_root(&self.pdf))
        {
            records.extend(self.pdf.list_records()?);
        }
        if records.is_empty()
            && let Some(error) = first_retryable_read_error
        {
            return Err(error.context("failed to read task queue from available media libraries"));
        }
        Ok(records)
    }

    /// A queue-file read can fail while File Provider is materializing one entry even though
    /// the registered root is still the same reachable directory. Keep that store searchable
    /// for the next request; only retire it from the live set when its bound root no longer
    /// validates (for example, the volume is gone or the path names a replacement directory).
    fn note_video_store_read_error(
        &self,
        library_id: &str,
        store: &DownloadStore,
        error: &anyhow::Error,
    ) -> bool {
        if let Err(root_error) = store.root.validate_configured_root() {
            let diagnostic = anyhow!(
                "queue read failed: {error:#}; registered root is unavailable: {root_error:#}"
            );
            self.note_video_store_unavailable(library_id, &diagnostic);
            false
        } else {
            true
        }
    }

    fn note_video_store_unavailable(&self, library_id: &str, error: &anyhow::Error) {
        if let Ok(mut libraries) = self.video_libraries.write()
            && let Some(library) = libraries
                .iter_mut()
                .find(|registered| registered.library_id == library_id)
        {
            library.available = false;
        }
        if let Ok(mut statuses) = self.video_statuses.write()
            && let Some(status) = statuses.get_mut(library_id)
        {
            status.available = false;
            status.available_space_bytes = None;
            status.diagnostic = Some(error.to_string());
        }
    }

    /// Finish the target-first/source-second queue-record handoff before restart recovery can
    /// change generations or statuses. A target record is accepted only when it carries the
    /// exact durable transfer token and TargetInstalled phase.
    fn reconcile_task_destination_transfers(&self) -> Result<()> {
        let _guard = self.operation_lock.lock().map_err(poisoned_lock)?;
        for (library_id, source_store) in self.registered_video_stores()? {
            let records = match source_store.list_records() {
                Ok(records) => records,
                Err(error) => {
                    self.note_video_store_read_error(&library_id, &source_store, &error);
                    tracing::warn!(library_id = %library_id, error = %error, "could not read media library queue during task-transfer reconciliation");
                    continue;
                }
            };
            for record in records {
                let Some(transfer) = record.destination_transfer.as_ref() else {
                    continue;
                };
                if transfer.source.root != source_store.root_path {
                    continue;
                }
                let Some(target_store) = self.store_for_root(&transfer.target.root)? else {
                    // Preserve the transfer token while its destination volume is unavailable.
                    continue;
                };
                match target_store.get_task(&transfer.task_id)? {
                    Some((target_record, _)) => {
                        let target_is_exact = target_record.destination.as_ref()
                            == Some(&transfer.target)
                            && target_record.destination_transfer.as_ref() == Some(transfer)
                            && target_record.destination_transfer_phase
                                == Some(TaskDestinationTransferPhase::TargetInstalled);
                        if !target_is_exact {
                            bail!(
                                "target queue contains a conflicting record for task {} during transfer reconciliation",
                                transfer.task_id
                            );
                        }
                        source_store.remove_task_for_transfer(transfer)?;
                    }
                    None if record.destination_transfer_phase
                        == Some(TaskDestinationTransferPhase::TargetInstalled) =>
                    {
                        bail!(
                            "task {} records a completed transfer handoff but its target record is missing",
                            transfer.task_id
                        );
                    }
                    None => {}
                }
            }
        }
        Ok(())
    }

    fn recover_interrupted_tasks(&self) -> Result<()> {
        for store in self.task_stores()? {
            if let Err(error) =
                crate::downloader::recover_bilibili_group_receipt_removals(&store.root)
            {
                tracing::warn!(error = %error, root = %store.root_path.display(), "failed to recover interrupted Bilibili receipt cleanup; retained for retry");
            }
            let records = match store.list_records() {
                Ok(records) => records,
                Err(error) => {
                    tracing::warn!(error = %error, root = %store.root_path.display(), "skipping unavailable task queue during restart recovery");
                    continue;
                }
            };
            for mut record in records {
                if task_transfer_or_final_ack_pending(&record) {
                    continue;
                }
                store.cleanup_terminal_bilibili_group_receipts(&record);
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
    let normalized_logical_root =
        crate::media_libraries::normalize_trusted_macos_alias(&logical_root)?;
    let normalized_canonical_root =
        crate::media_libraries::normalize_trusted_macos_alias(&canonical_root)?;
    let mut hashes = BTreeMap::new();
    for path in media_paths {
        let normalized_path = crate::media_libraries::normalize_trusted_macos_alias(path)?;
        let bound_path =
            if let Ok(relative) = normalized_path.strip_prefix(&normalized_logical_root) {
                logical_root.join(relative)
            } else if let Ok(relative) = normalized_path.strip_prefix(&normalized_canonical_root) {
                logical_root.join(relative)
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

fn config_for_destination(base: &AppConfig, destination: &TaskDestination) -> AppConfig {
    let mut config = base.clone();
    let logical_root = preserve_configured_root_alias(base, &destination.root);
    config.set_video_storage_root(logical_root.clone());
    config.downloads.video_dir = destination
        .category
        .as_ref()
        .map(|category| logical_root.join(category))
        .unwrap_or(logical_root);
    config
}

fn same_trusted_root_path(left: &Path, right: &Path) -> Result<bool> {
    Ok(crate::media_libraries::normalize_trusted_macos_alias(left)?
        == crate::media_libraries::normalize_trusted_macos_alias(right)?)
}

fn preserve_configured_root_alias(base: &AppConfig, canonical_root: &Path) -> PathBuf {
    let configured = base.video_storage_root();
    if std::fs::canonicalize(configured).is_ok_and(|resolved| resolved == canonical_root) {
        configured.to_path_buf()
    } else {
        preserve_trusted_system_alias(configured, canonical_root)
    }
}

fn preserve_trusted_system_alias(configured: &Path, canonical_root: &Path) -> PathBuf {
    for (alias, target) in [
        (Path::new("/var"), Path::new("/private/var")),
        (Path::new("/tmp"), Path::new("/private/tmp")),
        (Path::new("/etc"), Path::new("/private/etc")),
    ] {
        let Ok(configured_suffix) = configured.strip_prefix(alias) else {
            continue;
        };
        let Ok(canonical_configured) =
            crate::media_libraries::normalize_trusted_macos_alias(configured)
        else {
            continue;
        };
        if !canonical_configured.starts_with(target) || configured_suffix.as_os_str().is_empty() {
            continue;
        }
        if let Ok(suffix) = canonical_root.strip_prefix(target) {
            return alias.join(suffix);
        }
    }
    canonical_root.to_path_buf()
}

fn validate_task_transfer_migration_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("migration id must contain 1 to 128 ASCII letters, digits, '-' or '_'");
    }
    Ok(())
}

fn staging_job_relative(root: &RootedFs, root_path: &Path, recorded: &Path) -> Result<PathBuf> {
    if !recorded.is_absolute() {
        bail!("recorded task staging path is not absolute");
    }
    let relative = recorded
        .strip_prefix(root_path)
        .or_else(|_| recorded.strip_prefix(root.logical_root_path()))
        .or_else(|_| recorded.strip_prefix(root.root_path()))
        .map(Path::to_path_buf)
        .or_else(|_| {
            let resolved = std::fs::canonicalize(recorded).with_context(|| {
                format!(
                    "failed to resolve recorded staging path {}",
                    recorded.display()
                )
            })?;
            resolved
                .strip_prefix(root.root_path())
                .map(Path::to_path_buf)
                .map_err(|_| anyhow!("recorded staging path is outside the source library"))
        })?;
    let components = relative.components().collect::<Vec<_>>();
    if components.len() < 2
        || !matches!(components[0], Component::Normal(name) if name == VIDEO_STAGING_DIRECTORY)
    {
        bail!("recorded staging path is outside the private video staging directory");
    }
    let Component::Normal(job_name) = components[1] else {
        bail!("recorded video staging job path is not normalized");
    };
    let job_name = job_name
        .to_str()
        .ok_or_else(|| anyhow!("recorded video staging job name is not UTF-8"))?;
    if !job_name.starts_with(VIDEO_STAGING_JOB_PREFIX)
        || components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("recorded video staging path is not an exact job directory path");
    }
    Ok(PathBuf::from(VIDEO_STAGING_DIRECTORY).join(job_name))
}

fn record_staging_jobs(
    root: &RootedFs,
    root_path: &Path,
    record: &TaskRecord,
) -> Result<Vec<PathBuf>> {
    let mut jobs = BTreeSet::new();
    for recorded in &record.staging_attempts {
        jobs.insert(staging_job_relative(root, root_path, recorded)?);
    }
    Ok(jobs.into_iter().collect())
}

fn staging_directory_children(
    root: &RootedFs,
    absolute: &Path,
    relative: &Path,
    expected: EntryIdentity,
) -> Result<Vec<(std::ffi::OsString, EntryIdentity)>> {
    let entry = root.bind_entry(absolute, false)?;
    let directory = root.open_bound_directory(&entry, expected)?;
    let (owner, _group, mode) = directory.access_policy()?;
    validate_staging_directory_policy(relative, owner, mode)?;
    if mode > u16::MAX as u32 {
        bail!(
            "video staging directory mode is unsupported: {}",
            relative.display()
        );
    }
    let mut cursor = root.start_bound_directory_cursor(&entry, expected, mode as u16)?;
    let mut children = Vec::new();
    loop {
        let page = root.peek_bound_directory_cursor(&mut cursor, 256, mode as u16)?;
        match page.entry {
            Some(crate::safe_fs::BoundDirectoryCursorEntry::Present { name, identity }) => {
                if children.len() >= MAX_TASK_TRANSFER_ENTRIES {
                    bail!("task staging contains too many filesystem entries");
                }
                children.push((name, identity));
                root.consume_bound_directory_cursor_entry(&mut cursor);
            }
            Some(crate::safe_fs::BoundDirectoryCursorEntry::Unreadable { name, error }) => {
                bail!(
                    "could not inspect staging entry {}: {error}",
                    relative.join(name).display()
                );
            }
            None if page.exhausted => break,
            None => continue,
        }
    }
    Ok(children)
}

fn validate_staging_directory_policy(relative: &Path, owner: u32, mode: u32) -> Result<()> {
    let components = relative.components().count();
    let is_job_root = components == 2;
    if owner != unsafe { libc::geteuid() }
        || mode & 0o6000 != 0
        || (is_job_root && mode & 0o077 != 0)
    {
        bail!(
            "video staging directory access policy is unsafe: {}",
            relative.display()
        );
    }
    Ok(())
}

fn validate_staging_payload_file_policy(
    relative: &Path,
    owner: u32,
    mode: u32,
    link_count: u64,
) -> Result<()> {
    if owner != unsafe { libc::geteuid() } || mode & 0o6000 != 0 || link_count != 1 {
        bail!(
            "task staging payload is not owner-owned and single-linked: {}",
            relative.display()
        );
    }
    Ok(())
}

fn estimate_record_staging(
    root: &RootedFs,
    root_path: &Path,
    record: &TaskRecord,
) -> Result<(u64, usize)> {
    let mut total_bytes = 0_u64;
    let mut file_count = 0_usize;
    let mut entries_seen = 0_usize;
    let jobs = record_staging_jobs(root, root_path, record)?;
    let mut pending = Vec::new();
    for job in jobs {
        let absolute = root_path.join(&job);
        if let Some(identity) = root.entry_identity_if_parent_exists(&absolute)? {
            if !identity.is_dir() {
                bail!(
                    "recorded video staging job is not a directory: {}",
                    job.display()
                );
            }
            pending.push((job, identity, 0_usize));
        }
    }
    while let Some((relative, identity, depth)) = pending.pop() {
        if depth > 32 || entries_seen >= MAX_TASK_TRANSFER_ENTRIES {
            bail!("task staging exceeds its traversal limits");
        }
        entries_seen += 1;
        let absolute = root_path.join(&relative);
        if identity.is_dir() {
            for (name, child_identity) in
                staging_directory_children(root, &absolute, &relative, identity)?
            {
                if !child_identity.is_dir() && !child_identity.is_file() {
                    bail!(
                        "task staging contains an unsupported filesystem entry: {}",
                        relative.join(name).display()
                    );
                }
                pending.push((relative.join(name), child_identity, depth + 1));
            }
        } else if identity.is_file() {
            let entry = root.bind_entry(&absolute, false)?;
            let file = root.open_bound_file_if_identity(&entry, identity)?;
            let policy = file.access_policy()?;
            validate_staging_payload_file_policy(
                &relative,
                policy.owner,
                policy.mode,
                policy.link_count,
            )?;
            total_bytes = total_bytes
                .checked_add(file.byte_len()?)
                .ok_or_else(|| anyhow!("task staging byte count overflow"))?;
            if total_bytes > MAX_TASK_TRANSFER_BYTES {
                bail!("task staging exceeds its byte limit");
            }
            file_count += 1;
        }
    }
    Ok((total_bytes, file_count))
}

fn control_kind_for_path(path: &Path) -> Option<TaskTransferControlKind> {
    match path.file_name()?.to_str()? {
        ".owner.json" => Some(TaskTransferControlKind::Owner),
        ".retained.json" => Some(TaskTransferControlKind::Retention),
        ".publication.json" => Some(TaskTransferControlKind::PublicationManifest),
        ".bilibili-worker.json" => Some(TaskTransferControlKind::WorkerRequest),
        ".bilibili-worker-lifecycle.json" => Some(TaskTransferControlKind::WorkerLifecycle),
        ".bilibili-worker-lifecycle.next.json" => {
            Some(TaskTransferControlKind::WorkerLifecycleTransition)
        }
        "BBDown.config" => Some(TaskTransferControlKind::BbdownConfig),
        _ => None,
    }
}

fn digest_hex(digest: [u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn file_sha256_chunks(file: &BoundFile, expected_size: u64) -> Result<(String, Vec<String>)> {
    let initial_policy = file.access_policy()?;
    let initial_size = file.byte_len()?;
    if initial_size != expected_size {
        bail!("task staging file length changed before hashing");
    }
    let mut reader = file.duplicate_std_file()?;
    reader.seek(SeekFrom::Start(0))?;
    let mut full = Sha256::new();
    let mut chunks = Vec::new();
    let mut buffer = vec![0_u8; TASK_TRANSFER_CHUNK_BYTES];
    let mut total = 0_u64;
    loop {
        let mut chunk_len = 0_usize;
        while chunk_len < buffer.len() {
            let count = reader.read(&mut buffer[chunk_len..])?;
            if count == 0 {
                break;
            }
            chunk_len += count;
        }
        if chunk_len == 0 {
            break;
        }
        total = total
            .checked_add(chunk_len as u64)
            .ok_or_else(|| anyhow!("task staging file size overflow during hashing"))?;
        if total > expected_size {
            bail!("task staging file grew during hashing");
        }
        full.update(&buffer[..chunk_len]);
        chunks.push(digest_hex(Sha256::digest(&buffer[..chunk_len]).into()));
        if chunk_len < buffer.len() {
            break;
        }
    }
    if total != expected_size
        || file.byte_len()? != initial_size
        || file.access_policy()? != initial_policy
    {
        bail!(
            "task staging file identity, content length, or access policy changed during hashing"
        );
    }
    file.validate_identity()?;
    Ok((digest_hex(full.finalize().into()), chunks))
}

// These values are the exact metadata/proof inputs assembled by the caller for one planned
// staging entry. Keeping them explicit avoids bundling independent filesystem identities.
#[allow(clippy::too_many_arguments)]
fn push_transfer_entry(
    root: &RootedFs,
    root_path: &Path,
    relative: PathBuf,
    identity: EntryIdentity,
    job_relative: &Path,
    files: &mut Vec<TaskTransferFile>,
    controls: &mut Vec<TaskTransferControl>,
    total_bytes: &mut u64,
) -> Result<()> {
    if files.len() + controls.len() >= MAX_TASK_TRANSFER_ENTRIES {
        bail!("task staging contains too many filesystem entries");
    }
    let absolute = root_path.join(&relative);
    let entry = root.bind_entry(&absolute, false)?;
    if root.bound_entry_identity(&entry)? != Some(identity) {
        bail!(
            "task staging entry identity changed: {}",
            relative.display()
        );
    }
    if identity.is_dir() {
        let directory = root.open_bound_directory(&entry, identity)?;
        let (owner, group, mode) = directory.access_policy()?;
        validate_staging_directory_policy(&relative, owner, mode)?;
        files.push(TaskTransferFile {
            source_relative: relative.clone(),
            target_relative: relative,
            kind: TaskTransferEntryKind::Directory,
            device: identity.device(),
            inode: identity.inode(),
            size_bytes: 0,
            owner,
            group,
            mode,
            link_count: None,
            sha256: None,
            chunk_sha256: Vec::new(),
        });
    } else if identity.is_file() {
        let file = root.open_bound_file_if_identity(&entry, identity)?;
        let policy = file.access_policy()?;
        let size_bytes = file.byte_len()?;
        if job_relative.parent().is_none() {
            bail!("task staging job path is malformed");
        }
        let is_direct_job_child = relative.parent() == Some(job_relative);
        if is_direct_job_child && let Some(kind) = control_kind_for_path(&relative) {
            // Migration controls are stricter than downloaded payload data: their bytes and
            // inode bindings are interpreted by the recovery protocol.
            file.validate_private_single_link(0o600)?;
            controls.push(TaskTransferControl {
                source_relative: relative.clone(),
                target_relative: relative,
                kind,
                device: identity.device(),
                inode: identity.inode(),
                size_bytes,
                owner: policy.owner,
                group: policy.group,
                mode: policy.mode,
                link_count: Some(policy.link_count),
                sha256: digest_hex(file.sha256()?),
            });
        } else {
            validate_staging_payload_file_policy(
                &relative,
                policy.owner,
                policy.mode,
                policy.link_count,
            )?;
            *total_bytes = total_bytes
                .checked_add(size_bytes)
                .ok_or_else(|| anyhow!("task transfer byte count overflow"))?;
            if *total_bytes > MAX_TASK_TRANSFER_BYTES {
                bail!("task transfer exceeds its byte limit");
            }
            let (sha256, chunk_sha256) = file_sha256_chunks(&file, size_bytes)?;
            files.push(TaskTransferFile {
                source_relative: relative.clone(),
                target_relative: relative,
                kind: TaskTransferEntryKind::File,
                device: identity.device(),
                inode: identity.inode(),
                size_bytes,
                owner: policy.owner,
                group: policy.group,
                mode: policy.mode,
                link_count: Some(policy.link_count),
                sha256: Some(sha256),
                chunk_sha256,
            });
        }
    } else {
        bail!(
            "task staging contains an unsupported filesystem entry: {}",
            relative.display()
        );
    }
    Ok(())
}

fn build_task_transfer_plan(
    store: &DownloadStore,
    record: &TaskRecord,
    transfer: &TaskDestinationTransfer,
    published_path_rewrites: &BTreeMap<PathBuf, PathBuf>,
) -> Result<TaskTransferPlan> {
    if record.staging_attempts.len() > MAX_TASK_TRANSFER_ENTRIES {
        bail!("task has too many recorded staging paths");
    }
    let job_roots = record_staging_jobs(&store.root, &store.root_path, record)?;
    let mut files = Vec::new();
    let mut controls = Vec::new();
    let mut total_bytes = 0_u64;
    let mut entries_seen = 0_usize;
    for job_relative in job_roots {
        let job_absolute = store.root_path.join(&job_relative);
        let Some(job_identity) = store.root.entry_identity_if_parent_exists(&job_absolute)? else {
            continue;
        };
        if !job_identity.is_dir() {
            bail!(
                "recorded video staging job is not a directory: {}",
                job_relative.display()
            );
        }
        let controls_before_job = controls.len();
        let mut pending = vec![(job_relative.clone(), job_identity, 0_usize)];
        while let Some((relative, identity, depth)) = pending.pop() {
            if depth > 32 || entries_seen >= MAX_TASK_TRANSFER_ENTRIES {
                bail!("task staging exceeds its traversal limits");
            }
            entries_seen += 1;
            push_transfer_entry(
                &store.root,
                &store.root_path,
                relative.clone(),
                identity,
                &job_relative,
                &mut files,
                &mut controls,
                &mut total_bytes,
            )?;
            if identity.is_dir() {
                for (name, child_identity) in staging_directory_children(
                    &store.root,
                    &store.root_path.join(&relative),
                    &relative,
                    identity,
                )? {
                    if !child_identity.is_dir() && !child_identity.is_file() {
                        bail!(
                            "task staging contains an unsupported filesystem entry: {}",
                            relative.join(name).display()
                        );
                    }
                    pending.push((relative.join(name), child_identity, depth + 1));
                }
            }
        }
        let job_controls = controls[controls_before_job..].to_vec();
        let receipts = crate::downloader::inspect_video_staging_for_migration(
            &store.root,
            &job_relative,
            job_identity,
            &job_controls,
        )?;
        verify_source_control_receipts(&store.root, &store.root_path, &job_controls, &receipts)?;
    }
    files.sort_by(|left, right| left.source_relative.cmp(&right.source_relative));
    controls.sort_by(|left, right| left.source_relative.cmp(&right.source_relative));
    let mut plan = TaskTransferPlan {
        transfer: transfer.clone(),
        source_root: transfer.source.root.clone(),
        target_root: transfer.target.root.clone(),
        queue_record_source: store
            .task_path(&record.id)?
            .strip_prefix(&store.root_path)?
            .to_path_buf(),
        queue_record_target: PathBuf::from(QUEUE_DIRECTORY)
            .join(format!("task-{}.json", record.id)),
        files,
        controls,
        published_path_rewrites: published_path_rewrites.clone(),
        total_bytes,
        fingerprint: String::new(),
    };
    plan.transfer.plan_fingerprint = None;
    let mut hasher = Sha256::new();
    hasher.update(b"telegram-video-task-transfer-plan-v1\0");
    hasher.update(serde_json::to_vec(&plan)?);
    plan.fingerprint = digest_hex(hasher.finalize().into());
    plan.transfer.plan_fingerprint = Some(plan.fingerprint.clone());
    if serde_json::to_vec(&plan)?.len() > MAX_TASK_TRANSFER_PLAN_BYTES {
        bail!("task transfer plan exceeds its size limit");
    }
    Ok(plan)
}

fn transfer_plan_fingerprint(plan: &TaskTransferPlan) -> Result<String> {
    let mut canonical = plan.clone();
    canonical.fingerprint.clear();
    canonical.transfer.plan_fingerprint = None;
    let mut hasher = Sha256::new();
    hasher.update(b"telegram-video-task-transfer-plan-v1\0");
    hasher.update(serde_json::to_vec(&canonical)?);
    Ok(digest_hex(hasher.finalize().into()))
}

fn validate_transfer_relative(path: &Path) -> Result<()> {
    let components = path.components().collect::<Vec<_>>();
    if path.is_absolute()
        || components.len() < 2
        || !matches!(components[0], Component::Normal(name) if name == VIDEO_STAGING_DIRECTORY)
        || !matches!(components[1], Component::Normal(name) if name.to_str().is_some_and(|name| name.starts_with(VIDEO_STAGING_JOB_PREFIX)))
        || components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("task transfer path is outside its exact private staging job");
    }
    Ok(())
}

fn validate_task_transfer_plan_shape(plan: &TaskTransferPlan) -> Result<()> {
    validate_task_transfer_migration_id(&plan.transfer.migration_id)?;
    validate_task_id(&plan.transfer.task_id)?;
    if plan.source_root != plan.transfer.source.root
        || plan.target_root != plan.transfer.target.root
        || plan.source_root == plan.target_root
    {
        bail!("task transfer plan roots do not match the frozen transfer token");
    }
    let expected_queue_relative =
        PathBuf::from(QUEUE_DIRECTORY).join(format!("task-{}.json", plan.transfer.task_id));
    if plan.queue_record_source != expected_queue_relative
        || plan.queue_record_target != expected_queue_relative
    {
        bail!("task transfer plan queue record path is not the exact task record");
    }
    if plan.files.len().saturating_add(plan.controls.len()) > MAX_TASK_TRANSFER_ENTRIES
        || plan.total_bytes > MAX_TASK_TRANSFER_BYTES
    {
        bail!("task transfer plan exceeds its entry or byte limit");
    }
    let mut paths = BTreeSet::new();
    let mut total_bytes = 0_u64;
    for file in &plan.files {
        validate_transfer_relative(&file.source_relative)?;
        validate_transfer_relative(&file.target_relative)?;
        match file.kind {
            TaskTransferEntryKind::Directory => {
                validate_staging_directory_policy(&file.source_relative, file.owner, file.mode)?;
            }
            TaskTransferEntryKind::File => {
                validate_staging_payload_file_policy(
                    &file.source_relative,
                    file.owner,
                    file.mode,
                    file.link_count.unwrap_or(0),
                )?;
            }
        }
        if file.source_relative != file.target_relative
            || !paths.insert(file.source_relative.clone())
        {
            bail!("task transfer plan contains a duplicate or rewritten staging path");
        }
        if file.source_relative.parent().is_some_and(|parent| {
            parent.components().count() == 2
                && control_kind_for_path(&file.source_relative).is_some()
        }) {
            bail!("task transfer plan treats a staging control as ordinary payload");
        }
        if file.kind == TaskTransferEntryKind::File {
            total_bytes = total_bytes
                .checked_add(file.size_bytes)
                .ok_or_else(|| anyhow!("task transfer plan byte count overflow"))?;
            if file.sha256.as_deref().is_none_or(str::is_empty)
                || file.chunk_sha256.len()
                    != (file.size_bytes as usize).div_ceil(TASK_TRANSFER_CHUNK_BYTES)
            {
                bail!("task transfer plan has incomplete file content proofs");
            }
        } else if file.size_bytes != 0 || file.sha256.is_some() || !file.chunk_sha256.is_empty() {
            bail!("task transfer directory proof contains file content fields");
        }
    }
    for control in &plan.controls {
        validate_transfer_relative(&control.source_relative)?;
        validate_transfer_relative(&control.target_relative)?;
        if control.source_relative != control.target_relative
            || !paths.insert(control.source_relative.clone())
        {
            bail!("task transfer plan contains a duplicate or rewritten control path");
        }
        if control.source_relative.parent().is_none_or(|parent| {
            parent.components().count() != 2
                || control_kind_for_path(&control.source_relative) != Some(control.kind)
        }) {
            bail!("task transfer plan has a control outside the direct job directory");
        }
    }
    if total_bytes != plan.total_bytes {
        bail!("task transfer plan byte total does not match its file list");
    }
    let _ = transfer_staging_jobs(plan)?;
    for (source, target) in &plan.published_path_rewrites {
        if !source.is_absolute()
            || !target.is_absolute()
            || !source.starts_with(&plan.source_root)
            || !target.starts_with(&plan.target_root)
        {
            bail!("published path rewrite escapes its task transfer roots");
        }
    }
    if transfer_plan_fingerprint(plan)? != plan.fingerprint {
        bail!("task transfer plan fingerprint does not match its contents");
    }
    Ok(())
}

fn verify_transfer_file(
    root: &RootedFs,
    root_path: &Path,
    proof: &TaskTransferFile,
    source: bool,
) -> Result<()> {
    let relative = if source {
        &proof.source_relative
    } else {
        &proof.target_relative
    };
    validate_transfer_relative(relative)?;
    let absolute = root_path.join(relative);
    let entry = root.bind_entry(&absolute, false)?;
    let identity = root
        .bound_entry_identity(&entry)?
        .ok_or_else(|| anyhow!("task staging entry is missing: {}", relative.display()))?;
    match proof.kind {
        TaskTransferEntryKind::Directory => {
            if !identity.is_dir() {
                bail!(
                    "task staging directory was replaced: {}",
                    relative.display()
                );
            }
            if source && (identity.device() != proof.device || identity.inode() != proof.inode) {
                bail!(
                    "task staging directory identity changed: {}",
                    relative.display()
                );
            }
            let directory = root.open_bound_directory(&entry, identity)?;
            let (owner, group, mode) = directory.access_policy()?;
            if (owner, group, mode) != (proof.owner, proof.group, proof.mode) {
                bail!(
                    "task staging directory access policy changed: {}",
                    relative.display()
                );
            }
            validate_staging_directory_policy(relative, owner, mode)?;
        }
        TaskTransferEntryKind::File => {
            if !identity.is_file() {
                bail!("task staging file was replaced: {}", relative.display());
            }
            if source && (identity.device() != proof.device || identity.inode() != proof.inode) {
                bail!("task staging file identity changed: {}", relative.display());
            }
            let file = root.open_bound_file_if_identity(&entry, identity)?;
            let policy = file.access_policy()?;
            if file.byte_len()? != proof.size_bytes
                || (policy.owner, policy.group, policy.mode)
                    != (proof.owner, proof.group, proof.mode)
                || policy.link_count != proof.link_count.unwrap_or(1)
            {
                bail!(
                    "task staging file size or access policy changed: {}",
                    relative.display()
                );
            }
            validate_staging_payload_file_policy(
                relative,
                policy.owner,
                policy.mode,
                policy.link_count,
            )?;
            let (sha256, chunks) = file_sha256_chunks(&file, proof.size_bytes)?;
            if Some(sha256) != proof.sha256 || chunks != proof.chunk_sha256 {
                bail!("task staging file content changed: {}", relative.display());
            }
        }
    }
    Ok(())
}

fn verify_transfer_control(
    root: &RootedFs,
    root_path: &Path,
    proof: &TaskTransferControl,
) -> Result<()> {
    validate_transfer_relative(&proof.source_relative)?;
    let absolute = root_path.join(&proof.source_relative);
    let entry = root.bind_entry(&absolute, false)?;
    let identity = root.bound_entry_identity(&entry)?.ok_or_else(|| {
        anyhow!(
            "task staging control is missing: {}",
            proof.source_relative.display()
        )
    })?;
    if !identity.is_file() || identity.device() != proof.device || identity.inode() != proof.inode {
        bail!(
            "task staging control identity changed: {}",
            proof.source_relative.display()
        );
    }
    let file = root.open_bound_file_if_identity(&entry, identity)?;
    let policy = file.access_policy()?;
    if file.byte_len()? != proof.size_bytes
        || (policy.owner, policy.group, policy.mode) != (proof.owner, proof.group, proof.mode)
        || policy.link_count != proof.link_count.unwrap_or(1)
        || digest_hex(file.sha256()?) != proof.sha256
    {
        bail!(
            "task staging control content or access policy changed: {}",
            proof.source_relative.display()
        );
    }
    Ok(())
}

fn verify_control_receipt(
    root: &RootedFs,
    root_path: &Path,
    receipt: &StagingMigrationControlReceipt,
    expected_kind: TaskTransferControlKind,
    expected_relative: &Path,
) -> Result<()> {
    if receipt.kind != expected_kind || receipt.relative_path != expected_relative {
        bail!("rebuilt task staging control proof names the wrong object");
    }
    let absolute = root_path.join(expected_relative);
    let entry = root.bind_entry(&absolute, false)?;
    let identity = root.bound_entry_identity(&entry)?.ok_or_else(|| {
        anyhow!(
            "rebuilt task staging control is missing: {}",
            expected_relative.display()
        )
    })?;
    if !identity.is_file()
        || identity.device() != receipt.device
        || identity.inode() != receipt.inode
    {
        bail!(
            "rebuilt task staging control identity changed: {}",
            expected_relative.display()
        );
    }
    let file = root.open_bound_file_if_identity(&entry, identity)?;
    let policy = file.access_policy()?;
    if file.byte_len()? != receipt.size_bytes
        || (policy.owner, policy.group, policy.mode, policy.link_count)
            != (
                receipt.owner,
                receipt.group,
                receipt.mode,
                receipt.link_count,
            )
        || digest_hex(file.sha256()?) != receipt.sha256
    {
        bail!("rebuilt task staging control proof did not match its bound file");
    }
    Ok(())
}

fn verify_source_control_receipts(
    root: &RootedFs,
    root_path: &Path,
    controls: &[TaskTransferControl],
    receipts: &[StagingMigrationControlReceipt],
) -> Result<()> {
    if controls.len() != receipts.len() {
        bail!("source staging control inventory changed during migration validation");
    }
    let mut seen = BTreeSet::new();
    for receipt in receipts {
        if !seen.insert(receipt.relative_path.clone()) {
            bail!("source staging inspector returned a duplicate control receipt");
        }
        let control = controls
            .iter()
            .find(|control| {
                control.kind == receipt.kind && control.source_relative == receipt.relative_path
            })
            .ok_or_else(|| anyhow!("source staging inspector returned an unexpected control"))?;
        if (
            receipt.device,
            receipt.inode,
            receipt.size_bytes,
            receipt.owner,
            receipt.group,
            receipt.mode,
            receipt.link_count,
            receipt.sha256.as_str(),
        ) != (
            control.device,
            control.inode,
            control.size_bytes,
            control.owner,
            control.group,
            control.mode,
            control.link_count.unwrap_or(1),
            control.sha256.as_str(),
        ) {
            bail!("source staging control proof changed during migration validation");
        }
        verify_control_receipt(
            root,
            root_path,
            receipt,
            control.kind,
            &control.source_relative,
        )?;
    }
    Ok(())
}

fn verify_rebound_control_receipts(
    root: &RootedFs,
    root_path: &Path,
    job_relative: &Path,
    source_controls: &[TaskTransferControl],
    receipts: &[StagingMigrationControlReceipt],
) -> Result<()> {
    let mut expected = BTreeMap::<PathBuf, TaskTransferControlKind>::new();
    for control in source_controls {
        if expected
            .insert(control.target_relative.clone(), control.kind)
            .is_some()
        {
            bail!("task transfer plan contains duplicate target controls");
        }
    }
    for (filename, kind) in [
        (".owner.json", TaskTransferControlKind::Owner),
        (".retained.json", TaskTransferControlKind::Retention),
    ] {
        expected.entry(job_relative.join(filename)).or_insert(kind);
    }
    if receipts.len() != expected.len() {
        bail!("destination staging rebinder returned an incomplete control inventory");
    }
    let mut seen = BTreeSet::new();
    for receipt in receipts {
        if !seen.insert(receipt.relative_path.clone()) {
            bail!("destination staging rebinder returned a duplicate control receipt");
        }
        let expected_kind = expected
            .get(&receipt.relative_path)
            .copied()
            .ok_or_else(|| {
                anyhow!("destination staging rebinder returned an unexpected control")
            })?;
        verify_control_receipt(
            root,
            root_path,
            receipt,
            expected_kind,
            &receipt.relative_path,
        )?;
    }
    Ok(())
}

fn transfer_staging_jobs(plan: &TaskTransferPlan) -> Result<Vec<PathBuf>> {
    let mut jobs = BTreeSet::new();
    for path in plan
        .files
        .iter()
        .map(|file| &file.source_relative)
        .chain(plan.controls.iter().map(|control| &control.source_relative))
    {
        validate_transfer_relative(path)?;
        let mut components = path.components();
        let Component::Normal(directory) = components
            .next()
            .ok_or_else(|| anyhow!("task transfer path has no staging directory"))?
        else {
            bail!("task transfer path has an invalid staging directory");
        };
        let Component::Normal(job_name) = components
            .next()
            .ok_or_else(|| anyhow!("task transfer path has no job directory"))?
        else {
            bail!("task transfer path has an invalid job directory");
        };
        if directory != VIDEO_STAGING_DIRECTORY
            || !job_name
                .to_str()
                .is_some_and(|name| name.starts_with(VIDEO_STAGING_JOB_PREFIX))
        {
            bail!("task transfer path is outside its exact private staging job");
        }
        jobs.insert(PathBuf::from(VIDEO_STAGING_DIRECTORY).join(job_name));
    }
    for job in &jobs {
        if !plan.files.iter().any(|file| {
            file.kind == TaskTransferEntryKind::Directory && file.source_relative == *job
        }) {
            bail!("task transfer plan is missing its staging job directory proof");
        }
    }
    Ok(jobs.into_iter().collect())
}

fn verify_transfer_tree(
    root: &RootedFs,
    root_path: &Path,
    plan: &TaskTransferPlan,
    source: bool,
) -> Result<()> {
    let jobs = transfer_staging_jobs(plan)?;
    let mut expected = BTreeSet::new();
    for file in &plan.files {
        expected.insert(if source {
            file.source_relative.clone()
        } else {
            file.target_relative.clone()
        });
    }
    for control in &plan.controls {
        expected.insert(if source {
            control.source_relative.clone()
        } else {
            control.target_relative.clone()
        });
    }
    if !source {
        for job in &jobs {
            expected.insert(job.join(".owner.json"));
            expected.insert(job.join(".retained.json"));
        }
    }

    let mut actual = BTreeSet::new();
    let mut pending = Vec::new();
    for job in jobs {
        let absolute = root_path.join(&job);
        let identity = root
            .entry_identity(&absolute)?
            .ok_or_else(|| anyhow!("task staging job is missing: {}", job.display()))?;
        if !identity.is_dir() {
            bail!("task staging job was replaced: {}", job.display());
        }
        pending.push((job, identity, 0_usize));
    }
    let mut visited = 0_usize;
    while let Some((relative, identity, depth)) = pending.pop() {
        if depth > 32 || visited >= MAX_TASK_TRANSFER_ENTRIES {
            bail!("task staging exceeds its transfer-tree verification limits");
        }
        visited += 1;
        if !actual.insert(relative.clone()) {
            bail!("task staging contains duplicate paths during verification");
        }
        if identity.is_dir() {
            for (name, child_identity) in
                staging_directory_children(root, &root_path.join(&relative), &relative, identity)?
            {
                if !child_identity.is_dir() && !child_identity.is_file() {
                    bail!(
                        "task staging contains an unsupported entry: {}",
                        relative.join(name).display()
                    );
                }
                pending.push((relative.join(name), child_identity, depth + 1));
            }
        }
    }
    if actual != expected {
        let unexpected = actual.difference(&expected).next();
        let missing = expected.difference(&actual).next();
        bail!(
            "task staging tree no longer matches its transfer plan (unexpected: {}; missing: {})",
            unexpected.map_or_else(|| "none".to_string(), |path| path.display().to_string()),
            missing.map_or_else(|| "none".to_string(), |path| path.display().to_string()),
        );
    }
    Ok(())
}

fn rewrite_published_path(
    path: &Path,
    source_root: &Path,
    target_root: &Path,
    rewrites: &BTreeMap<PathBuf, PathBuf>,
) -> PathBuf {
    let normalized_path = match crate::media_libraries::normalize_trusted_macos_alias(path) {
        Ok(path) => path,
        Err(_) => return path.to_path_buf(),
    };
    if let Some((_, suffix, target)) = rewrites
        .iter()
        .filter_map(|(source, target)| {
            let source = crate::media_libraries::normalize_trusted_macos_alias(source).ok()?;
            normalized_path
                .strip_prefix(&source)
                .ok()
                .map(|suffix| (source.components().count(), suffix, target))
        })
        .max_by_key(|(source_depth, _, _)| *source_depth)
        && suffix
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return target.join(suffix);
    }
    let normalized_source_root =
        match crate::media_libraries::normalize_trusted_macos_alias(source_root) {
            Ok(root) => root,
            Err(_) => return path.to_path_buf(),
        };
    normalized_path
        .strip_prefix(&normalized_source_root)
        .ok()
        .filter(|relative| {
            relative
                .components()
                .all(|component| matches!(component, Component::Normal(_)))
        })
        .map(|relative| target_root.join(relative))
        .unwrap_or_else(|| path.to_path_buf())
}

fn rebase_path_string(
    value: &str,
    source_root: &Path,
    target_root: &Path,
    rewrites: &BTreeMap<PathBuf, PathBuf>,
) -> String {
    let path = Path::new(value);
    if !path.is_absolute() {
        return value.to_string();
    }
    rewrite_published_path(path, source_root, target_root, rewrites)
        .display()
        .to_string()
}

fn rebase_collection_snapshot(
    snapshot: &mut Option<PlanValidationSnapshot>,
    source_root: &Path,
    target_root: &Path,
    rewrites: &BTreeMap<PathBuf, PathBuf>,
) {
    let Some(snapshot) = snapshot else {
        return;
    };
    match &mut snapshot.bilibili_collection_directory {
        BilibiliCollectionDirectorySnapshot::Missing { path }
        | BilibiliCollectionDirectorySnapshot::Existing { path, .. } => {
            *path = rebase_path_string(path, source_root, target_root, rewrites);
        }
        BilibiliCollectionDirectorySnapshot::Untracked => {}
    }
}

fn rebase_task_paths(
    record: &mut TaskRecord,
    source_root: &Path,
    target_root: &Path,
    rewrites: &BTreeMap<PathBuf, PathBuf>,
) -> Result<()> {
    if record.primary_media_hash_manifest.is_some() && !record.primary_media_hashes.is_empty() {
        bail!("task has inconsistent compacted media hash state and cannot be transferred");
    }
    if record.primary_media_hash_manifest.is_some() && !rewrites.is_empty() {
        bail!("task has compacted media hashes that cannot be safely rebased");
    }
    record.staging_attempts = record
        .staging_attempts
        .iter()
        .map(|path| rewrite_published_path(path, source_root, target_root, &BTreeMap::new()))
        .collect();
    if let Some(location) = record.saved_location.as_mut() {
        *location = rebase_path_string(location, source_root, target_root, rewrites);
    }
    let mut hashes = BTreeMap::new();
    for (path, digest) in std::mem::take(&mut record.primary_media_hashes) {
        let rebased = rebase_path_string(&path, source_root, target_root, rewrites);
        hashes.insert(rebased, digest);
    }
    record.primary_media_hashes = hashes;
    rebase_collection_snapshot(&mut record.plan, source_root, target_root, rewrites);
    rebase_collection_snapshot(
        &mut record.proposed_plan,
        source_root,
        target_root,
        rewrites,
    );
    rebase_collection_snapshot(
        &mut record.display_metadata,
        source_root,
        target_root,
        rewrites,
    );
    if let Some(attempt) = record.bilibili_attempt.as_mut() {
        for entry in &mut attempt.entries {
            for path in &mut entry.published_media_paths {
                *path = rewrite_published_path(path, source_root, target_root, rewrites);
            }
            let mut entry_hashes = BTreeMap::new();
            for (path, digest) in std::mem::take(&mut entry.published_media_hashes) {
                entry_hashes.insert(
                    rebase_path_string(&path, source_root, target_root, rewrites),
                    digest,
                );
            }
            entry.published_media_hashes = entry_hashes;
        }
    }
    Ok(())
}

fn restore_after_task_transfer(record: &mut TaskRecord, transfer: &TaskDestinationTransfer) {
    if transfer.pause_generation.is_some()
        && matches!(
            transfer.previous_status,
            TaskStatus::Preparing | TaskStatus::Running | TaskStatus::Verifying
        )
    {
        record.status = TaskStatus::Queued;
        record.error = transfer.previous_error.clone();
        record.cancel_requested = false;
    } else {
        record.status = transfer.previous_status;
        record.error = transfer.previous_error.clone();
        record.cancel_requested = transfer.previous_cancel_requested;
    }
    record.destination_transfer = None;
    record.destination_transfer_phase = None;
}

fn task_transfer_or_final_ack_pending(record: &TaskRecord) -> bool {
    record.destination_transfer.is_some() || record.destination_transfer_ack.is_some()
}

fn ensure_task_transfer_not_pending(record: &TaskRecord) -> Result<()> {
    if let Some(transfer) = record.destination_transfer.as_ref() {
        bail!(
            "task {} is reserved by media migration {}; finish or stop that migration first",
            record.id,
            transfer.migration_id
        );
    }
    if let Some(transfer) = record.destination_transfer_ack.as_ref() {
        bail!(
            "task {} is awaiting durable handoff finalization for media migration {}",
            record.id,
            transfer.migration_id
        );
    }
    Ok(())
}

fn roots_overlap_queue_root(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
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
    // Normalize only the verified macOS system aliases (/var, /tmp, and /etc). File Provider
    // may choose a different spelling than the configured URL for the same system-rooted item.
    let coordinated_path = crate::media_libraries::normalize_trusted_macos_alias(coordinated_path)?;
    let logical_root =
        crate::media_libraries::normalize_trusted_macos_alias(root.logical_root_path())?;
    let canonical_root = crate::media_libraries::normalize_trusted_macos_alias(root.root_path())?;
    let relative = coordinated_path
        .strip_prefix(&logical_root)
        .or_else(|_| coordinated_path.strip_prefix(&canonical_root))
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
    fn from_root(
        root: RootedFs,
        file_provider: Arc<dyn QueueFileProvider>,
        state_change_notify: Arc<Notify>,
    ) -> Result<Self> {
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
            state_change_notify,
        };
        store.ensure_operation_lock_file()?;
        Ok(store)
    }

    fn shares_root(&self, other: &Self) -> bool {
        self.root.root_identity() == other.root.root_identity()
    }

    fn add_root_alias(&mut self, path: &Path) {
        if !self.root_aliases.iter().any(|alias| alias == path) {
            self.root_aliases.push(path.to_path_buf());
        }
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
        let normalized_path = crate::media_libraries::normalize_trusted_macos_alias(path)?;
        let relative = self
            .root_aliases
            .iter()
            .filter_map(|root| crate::media_libraries::normalize_trusted_macos_alias(root).ok())
            .find_map(|root| normalized_path.strip_prefix(root).ok())
            .or_else(|| normalized_path.strip_prefix(self.root.root_path()).ok())
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

    fn cleanup_terminal_bilibili_group_receipts(&self, record: &TaskRecord) {
        if !matches!(record.status, TaskStatus::Completed | TaskStatus::Cancelled)
            || !(record
                .bilibili_attempt
                .as_ref()
                .is_some_and(|attempt| attempt.independent_entries)
                || record
                    .plan
                    .as_ref()
                    .is_some_and(|plan| !plan.bilibili_group_entries.is_empty())
                || record
                    .proposed_plan
                    .as_ref()
                    .is_some_and(|plan| !plan.bilibili_group_entries.is_empty()))
        {
            return;
        }

        let namespace = record.bilibili_publication_namespace();
        if let Err(error) =
            crate::downloader::cleanup_bilibili_group_publication_receipts(&self.root, &namespace)
        {
            tracing::warn!(
                task_id = %record.id,
                error = %error,
                "failed to clean terminal Bilibili publication receipts; queue recovery will retry"
            );
        }
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
        let original_index = index.clone();
        self.scan_active_records(&mut index, false)?;
        self.reconcile_move_targets(&mut index)?;
        if index != original_index {
            self.write_index(&index)?;
        }
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

    fn active_summary_records_for_chats(
        &self,
        chat_ids: &[i64],
        max_records: usize,
    ) -> Result<Vec<TaskRecord>> {
        self.active_summary_records_for_chats_inner(chat_ids, max_records, false)
    }

    fn active_records_for_chats_complete(&self, chat_ids: &[i64]) -> Result<Vec<TaskRecord>> {
        self.active_summary_records_for_chats_inner(chat_ids, MAX_ACTIVE_RECORDS, true)
    }

    fn active_summary_records_for_chats_inner(
        &self,
        chat_ids: &[i64],
        max_records: usize,
        reject_overflow: bool,
    ) -> Result<Vec<TaskRecord>> {
        if chat_ids.is_empty() || max_records == 0 {
            return Ok(Vec::new());
        }

        let chat_order =
            chat_ids
                .iter()
                .enumerate()
                .fold(HashMap::new(), |mut order, (index, chat_id)| {
                    order.entry(*chat_id).or_insert(index);
                    order
                });
        let index = self.read_index()?;
        let mut candidates = index
            .tasks
            .iter()
            .filter(|(_, entry)| {
                chat_order.contains_key(&entry.chat_id)
                    && entry.move_target.is_none()
                    && entry.record_path.parent() == Some(self.queue_dir.as_path())
            })
            .collect::<Vec<_>>();
        if reject_overflow && candidates.len() > max_records {
            bail!("task queue exceeds its {max_records}-record summary limit");
        }
        // The index's updated_at value is maintained with each record write. Task IDs provide
        // the same stable within-store tie order as StoreIndex's BTreeMap iteration.
        candidates.sort_by(|(left_id, left), (right_id, right)| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| chat_order[&left.chat_id].cmp(&chat_order[&right.chat_id]))
                .then_with(|| left_id.cmp(right_id))
        });

        let mut records = Vec::with_capacity(max_records.min(candidates.len()));
        for (id, entry) in candidates.into_iter().take(max_records) {
            let Some(record) = self.read_record(&entry.record_path)? else {
                continue;
            };
            if record.id != *id || record.chat_id != entry.chat_id {
                bail!("task index points to a mismatched record for task {id}");
            }
            records.push(record);
        }
        Ok(records)
    }

    fn get_task(&self, id: &str) -> Result<Option<(TaskRecord, TaskIndexEntry)>> {
        let mut index = self.read_index()?;
        let original_index = index.clone();
        self.scan_active_records(&mut index, false)?;
        self.reconcile_move_targets(&mut index)?;
        if index != original_index {
            self.write_index(&index)?;
        }
        if !index.tasks.contains_key(id) {
            return Ok(None);
        }
        let entry = index.tasks.get(id).expect("checked above").clone();
        let Some(record) = self.read_record(&entry.record_path)? else {
            return Ok(None);
        };
        if record.id != id || record.chat_id != entry.chat_id {
            bail!("task index points to a mismatched record for task {id}");
        }
        Ok(Some((record, entry)))
    }

    /// Read a single task through the persisted index without scanning the queue directory or
    /// reconciling pending moves. Callers which need to validate a library's persisted root
    /// identity first use this to avoid recovery/index writes against an unverified path.
    fn peek_task(&self, id: &str) -> Result<Option<(TaskRecord, TaskIndexEntry)>> {
        let index = self.read_index()?;
        let Some(entry) = index.tasks.get(id).cloned() else {
            return Ok(None);
        };
        let mut paths = vec![entry.record_path.clone()];
        if let Some(target) = &entry.move_target {
            paths.push(target.clone());
        }
        // During terminal sidecar migration, the index may durably name both the original and
        // target paths. Prefer whichever file exists without replaying the move or rewriting the
        // index before task_config has revalidated the registered root identity.
        for path in paths {
            if let Some(record) = self.read_record(&path)? {
                if record.id != id || record.chat_id != entry.chat_id {
                    bail!("task index points to a mismatched record for task {id}");
                }
                return Ok(Some((record, entry)));
            }
        }
        bail!("task index points to missing records for task {id}")
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
        self.state_change_notify.notify_one();
        self.cleanup_terminal_bilibili_group_receipts(&record);
        Ok(record)
    }

    fn remove_task_for_transfer(&self, transfer: &TaskDestinationTransfer) -> Result<()> {
        let mut index = self.read_index()?;
        let Some(index_entry) = index.tasks.get(&transfer.task_id).cloned() else {
            return Ok(());
        };
        if index_entry.record_path.parent() != Some(self.queue_dir.as_path())
            || index_entry.move_target.is_some()
        {
            bail!("task transfer source record is not in the active queue directory");
        }
        let Some(record) = self.read_record(&index_entry.record_path)? else {
            index.tasks.remove(&transfer.task_id);
            return self.write_index(&index);
        };
        if !record.destination_transfer.as_ref().is_some_and(|current| {
            current.transfer_id == transfer.transfer_id
                && current.migration_id == transfer.migration_id
                && current.source == transfer.source
                && current.target == transfer.target
        }) || !matches!(
            record.destination_transfer_phase,
            Some(
                TaskDestinationTransferPhase::Copying
                    | TaskDestinationTransferPhase::TargetInstalled
            )
        ) {
            bail!("task transfer source record does not carry the matching handoff token");
        }
        let entry = self.root.bind_entry(&index_entry.record_path, false)?;
        let identity = self
            .root
            .bound_entry_identity(&entry)?
            .ok_or_else(|| anyhow!("task transfer source record disappeared"))?;
        if !identity.is_file() {
            bail!("task transfer source record is not a regular file");
        }
        self.ensure_private_directory()?;
        self.root.remove_bound_file_if_identity(&entry, identity)?;
        index.tasks.remove(&transfer.task_id);
        self.write_index(&index)
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
        let original_index = index.clone();
        self.scan_active_records(&mut index, true)?;
        // Persist repaired metadata and any terminal move intent before moving files. This keeps
        // the existing crash-safe intent -> rename -> final-index sequence intact.
        if index != original_index {
            self.write_index(&index)?;
        }
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

    fn scan_active_records(
        &self,
        index: &mut StoreIndex,
        reconcile_indexed_records: bool,
    ) -> Result<()> {
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
            if !identity.is_file() {
                continue;
            }
            let path = self.task_path(id)?;
            if let Some(entry) = index.tasks.get_mut(id) {
                // Ordinary lookups keep the established bounded behavior and only discover
                // orphan records. During startup reconciliation, repair index metadata from
                // each already-indexed active queue file. This single bounded directory pass
                // prevents stale terminal timestamps from occupying every summary slot.
                if reconcile_indexed_records
                    && entry.record_path == path
                    && entry.move_target.is_none()
                    && let Some(record) = self.read_record(&path)?
                {
                    if record.id != id || record.chat_id != entry.chat_id {
                        bail!("task index points to a mismatched record for task {id}");
                    }
                    entry.revision = record.revision;
                    entry.updated_at = record.updated_at;
                    if record.status.is_history() {
                        entry.move_target = Some(self.terminal_history_path(&record.id)?);
                    }
                    let activity = index
                        .chat_activity
                        .entry(record.chat_id.to_string())
                        .or_default();
                    activity.revision = activity.revision.max(record.activity_revision);
                }
                continue;
            }
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
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::sync::mpsc;
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

    fn task_transfer_fixture(label: &str) -> (PathBuf, AppConfig, QueueManager, TaskDestination) {
        let directory = temp_queue_root(label);
        let config = test_video_config(&directory);
        let target_root = directory.join("archive");
        fs::create_dir_all(&target_root).expect("target library should create");
        let queue = QueueManager::open(&config).expect("queue should open");
        let library = queue
            .register_video_library("Archive", &target_root, None, false, false)
            .expect("target library should register");
        let destination = queue
            .video_destination(&library.id, None)
            .expect("target destination should resolve");
        (directory, config, queue, destination)
    }

    fn create_transfer_test_task(queue: &QueueManager, id: &str, status: TaskStatus) -> TaskRecord {
        assert!(
            queue
                .create(test_task(
                    id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=transfer-test".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        queue
            .update(id, false, |record| {
                record.status = status;
                record.error = Some(format!("saved {status:?} error"));
                record.cancel_requested = false;
                if status == TaskStatus::AwaitingConfirmation {
                    record.proposed_plan = Some(PlanValidationSnapshot::default());
                }
                Ok(())
            })
            .expect("task state should persist")
    }

    fn begin_test_transfer(
        queue: &QueueManager,
        id: &str,
        migration_id: &str,
        target: &TaskDestination,
    ) -> TaskDestinationTransfer {
        let record = queue
            .get(id)
            .expect("task should be readable")
            .expect("task should exist");
        queue
            .begin_task_destination_transfer(
                id,
                migration_id,
                record.generation,
                record.revision,
                target.clone(),
            )
            .expect("transfer should begin")
    }

    #[test]
    fn task_destination_is_frozen_across_default_switch_and_reopen() {
        let temp_root = temp_queue_root("frozen-library-destination");
        let mut config = test_video_config(&temp_root);
        let archive_root = temp_root.join("archive");
        fs::create_dir_all(&archive_root).expect("archive root should create");
        let provider: Arc<dyn QueueFileProvider> = Arc::new(MockQueueFileProvider::default());
        let first_id = "task-library-before-switch";
        let second_id = "task-library-after-switch";
        let first_destination;
        let archive_id;
        {
            let queue = QueueManager::open_with_file_provider(&config, Arc::clone(&provider))
                .expect("queue should open");
            first_destination = queue
                .default_video_destination()
                .expect("default video destination should resolve");
            assert!(
                queue
                    .create(test_task(
                        first_id,
                        JobRequest::Youtube {
                            url: "https://www.youtube.com/watch?v=first".to_string(),
                        },
                    ))
                    .expect("first task should persist")
            );

            let archive = queue
                .register_video_library(
                    "Archive",
                    &archive_root,
                    Some(Path::new("Shows")),
                    true,
                    false,
                )
                .expect("archive library should register");
            archive_id = archive.id;
            assert!(
                queue
                    .create(test_task(
                        second_id,
                        JobRequest::Youtube {
                            url: "https://www.youtube.com/watch?v=second".to_string(),
                        },
                    ))
                    .expect("second task should persist")
            );

            let old_destination = queue
                .task_destination(first_id)
                .expect("first task lookup should succeed")
                .expect("first task should have a destination");
            assert_eq!(old_destination, first_destination);
            let second_destination = queue
                .task_destination(second_id)
                .expect("second task lookup should succeed")
                .expect("second task should have a destination");
            assert_eq!(second_destination.library_id, archive_id);
            assert_eq!(second_destination.category, Some(PathBuf::from("Shows")));
            let selected_config = queue
                .task_config(&config, second_id)
                .expect("task config should resolve")
                .expect("video task should have a config");
            assert_eq!(selected_config.video_storage_root(), archive_root);
            assert_eq!(
                selected_config.downloads.video_dir,
                archive_root.join("Shows")
            );
            assert_eq!(queue.list(123_456_789, false, 0).unwrap().len(), 2);
        }

        config.downloads.video_dir = temp_root.join("videos");
        let queue = QueueManager::open_with_file_provider(&config, provider)
            .expect("queue should reopen with both registered roots");
        assert_eq!(
            queue.task_destination(first_id).unwrap().unwrap(),
            first_destination
        );
        let second_destination = queue.task_destination(second_id).unwrap().unwrap();
        assert_eq!(second_destination.library_id, archive_id);
        assert_eq!(queue.list(123_456_789, false, 0).unwrap().len(), 2);
        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn removing_library_blocks_failed_video_tasks_but_ignores_terminal_and_pdf_tasks() {
        let temp_root = temp_queue_root("remove-library-task-dependencies");
        let shared_root = temp_root.join("videos");
        let archive_root = temp_root.join("archive");
        fs::create_dir_all(&shared_root).expect("shared root should create");
        fs::create_dir_all(&archive_root).expect("archive root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = shared_root.clone();
        config.downloads.pdf_dir = shared_root.clone();

        let queue = QueueManager::open(&config).expect("shared queue root should open");
        let removed_library_id = queue.registry_snapshot().unwrap().default_library_id;
        let removed_library = queue
            .registry_snapshot()
            .unwrap()
            .libraries
            .into_iter()
            .find(|library| library.id == removed_library_id)
            .expect("configured library should be registered");
        queue
            .register_video_library("Archive", &archive_root, None, true, false)
            .expect("archive library should become the default");

        let mut failed = test_task(
            "remove-library-failed-video",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=remove-failed".to_string(),
            },
        );
        failed.destination = Some(TaskDestination {
            library_id: removed_library_id.clone(),
            root: removed_library.root.clone(),
            category: None,
        });
        assert!(queue.create(failed).unwrap());
        queue
            .update("remove-library-failed-video", false, |record| {
                record.status = TaskStatus::Failed;
                Ok(())
            })
            .unwrap();

        let mut completed = test_task(
            "remove-library-completed-video",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=remove-completed".to_string(),
            },
        );
        completed.destination = Some(TaskDestination {
            library_id: removed_library_id.clone(),
            root: removed_library.root.clone(),
            category: None,
        });
        assert!(queue.create(completed).unwrap());
        queue
            .update("remove-library-completed-video", false, |record| {
                record.status = TaskStatus::Completed;
                Ok(())
            })
            .unwrap();

        let mut cancelled = test_task(
            "remove-library-cancelled-video",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=remove-cancelled".to_string(),
            },
        );
        cancelled.destination = Some(TaskDestination {
            library_id: removed_library_id.clone(),
            root: removed_library.root.clone(),
            category: None,
        });
        assert!(queue.create(cancelled).unwrap());
        queue
            .update("remove-library-cancelled-video", false, |record| {
                record.status = TaskStatus::Cancelled;
                Ok(())
            })
            .unwrap();

        assert!(
            queue
                .create(test_task(
                    "remove-library-failed-pdf",
                    JobRequest::Pdf {
                        url: "https://example.invalid/remove.pdf".to_string(),
                    },
                ))
                .unwrap()
        );
        queue
            .update("remove-library-failed-pdf", false, |record| {
                record.status = TaskStatus::Failed;
                Ok(())
            })
            .unwrap();

        let error = queue
            .remove_video_library(&removed_library_id)
            .expect_err("failed video task remains retryable and must retain its library");
        assert!(error.to_string().contains("video tasks"));
        assert!(
            queue
                .registry_snapshot()
                .unwrap()
                .libraries
                .iter()
                .any(|library| library.id == removed_library_id)
        );

        queue
            .update("remove-library-failed-video", false, |record| {
                record.status = TaskStatus::Cancelled;
                Ok(())
            })
            .unwrap();
        let mut legacy = test_task(
            "remove-library-legacy-failed-video",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=remove-legacy".to_string(),
            },
        );
        legacy.destination = Some(TaskDestination {
            library_id: removed_library_id.clone(),
            root: removed_library.root.clone(),
            category: None,
        });
        assert!(queue.create(legacy).unwrap());
        queue
            .update("remove-library-legacy-failed-video", false, |record| {
                record.status = TaskStatus::Failed;
                record.destination = None;
                Ok(())
            })
            .unwrap();
        assert!(
            queue
                .remove_video_library(&removed_library_id)
                .expect_err("legacy failed video task still depends on its original root")
                .to_string()
                .contains("video tasks")
        );

        queue
            .update("remove-library-legacy-failed-video", false, |record| {
                record.status = TaskStatus::Completed;
                Ok(())
            })
            .unwrap();
        queue
            .remove_video_library(&removed_library_id)
            .expect("completed/cancelled video and failed PDF tasks do not depend on the library");
        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn task_config_rejects_replaced_registered_root_without_writing_replacement() {
        let temp_root = temp_queue_root("task-config-replaced-root");
        let config = test_video_config(&temp_root);
        let archive_root = temp_root.join("archive");
        let displaced_root = temp_root.join("archive-original");
        fs::create_dir_all(&archive_root).expect("archive root should create");
        let queue = QueueManager::open(&config).expect("queue should open");
        let archive = queue
            .register_video_library("Archive", &archive_root, None, true, false)
            .expect("archive library should register");
        let mut task = test_task(
            "task-config-replaced-root",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=replaced-root".to_string(),
            },
        );
        task.destination = Some(TaskDestination {
            library_id: archive.id.clone(),
            root: archive.root.clone(),
            category: None,
        });
        assert!(queue.create(task).expect("task should persist"));
        let old_record_path = displaced_root
            .join(QUEUE_DIRECTORY)
            .join("task-task-config-replaced-root.json");
        let original_record_path = archive_root
            .join(QUEUE_DIRECTORY)
            .join("task-task-config-replaced-root.json");
        let original_record = fs::read(&original_record_path).expect("record should exist");

        fs::rename(&archive_root, &displaced_root).expect("original library should be renamed");
        fs::create_dir(&archive_root).expect("replacement path should create");
        fs::write(archive_root.join("keep.txt"), b"replacement sentinel")
            .expect("replacement sentinel should write");

        let error = queue
            .task_config(&config, "task-config-replaced-root")
            .expect_err("task config must reject a replacement root object");
        assert!(
            error.to_string().contains("root")
                || error.to_string().contains("available")
                || error.to_string().contains("configured output"),
            "unexpected root validation error: {error:#}"
        );
        assert_eq!(
            fs::read(old_record_path).expect("original task record should remain intact"),
            original_record,
        );
        let replacement_entries = fs::read_dir(&archive_root)
            .expect("replacement should remain readable")
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(
            replacement_entries,
            vec![std::ffi::OsString::from("keep.txt")]
        );
        assert!(!archive_root.join(QUEUE_DIRECTORY).exists());
        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn default_change_rejects_replaced_registered_root_and_preserves_cas_outcomes() {
        let temp_root = temp_queue_root("default-replaced-root");
        let config = test_video_config(&temp_root);
        let archive_root = temp_root.join("archive");
        let displaced_root = temp_root.join("archive-original");
        fs::create_dir_all(&archive_root).expect("archive root should create");
        let queue = QueueManager::open(&config).expect("queue should open");
        let archive = queue
            .register_video_library("Archive", &archive_root, None, false, false)
            .expect("archive library should register");
        let before = queue
            .registry_snapshot()
            .expect("registry snapshot should load");

        fs::rename(&archive_root, &displaced_root).expect("original root should be displaced");
        fs::create_dir(&archive_root).expect("replacement root should create");
        fs::write(archive_root.join("keep.txt"), b"replacement sentinel")
            .expect("replacement sentinel should write");

        assert!(
            queue
                .register_video_library("Archive", &archive_root, None, true, false)
                .is_err(),
            "duplicate registration must not bypass the root identity gate"
        );
        assert!(queue.set_default_video_library(&archive.id).is_err());
        assert_eq!(
            queue.registry_snapshot().expect("snapshot should load"),
            before,
            "a rejected default change must leave the settings revision and default unchanged"
        );
        assert!(displaced_root.join(QUEUE_DIRECTORY).exists());
        assert_eq!(
            fs::read_dir(&archive_root)
                .expect("replacement remains readable")
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>(),
            vec![std::ffi::OsString::from("keep.txt")],
            "default validation must not create queue state in the replacement"
        );

        assert!(
            queue
                .set_default_video_library_for_migration_if_revision(
                    &archive.id,
                    "migration-default-replaced-root",
                    before.revision,
                )
                .is_err()
        );
        assert_eq!(
            queue.registry_snapshot().expect("snapshot should load"),
            before,
            "a failed identity proof must not resolve or mutate the migration CAS"
        );

        let stale_outcome = queue
            .set_default_video_library_for_migration_if_revision(
                &archive.id,
                "migration-default-stale-offline",
                before
                    .revision
                    .checked_add(1)
                    .expect("settings revision has room for a stale test value"),
            )
            .expect("stale CAS should latch without requiring an offline root");
        assert!(matches!(
            stale_outcome,
            MigrationDefaultUpdateOutcome::RevisionChanged { .. }
        ));
        let newer = temp_root.join("newer");
        fs::create_dir_all(&newer).expect("new healthy root should create");
        let newer_library = queue
            .register_video_library("Newer", &newer, None, false, false)
            .expect("new library should register");
        queue
            .set_default_video_library(&newer_library.id)
            .expect("healthy new library should become default");
        let replay = queue
            .set_default_video_library_for_migration_if_revision(
                &archive.id,
                "migration-default-stale-offline",
                before
                    .revision
                    .checked_add(1)
                    .expect("settings revision has room for a stale test value"),
            )
            .expect("resolved stale CAS should replay without touching the old root");
        let MigrationDefaultUpdateOutcome::RevisionChanged { snapshot } = replay else {
            panic!("stale migration CAS must keep its resolved skipped outcome");
        };
        assert_eq!(snapshot.default_library_id, newer_library.id);
        assert_eq!(
            queue.registry_snapshot().unwrap().default_library_id,
            newer_library.id,
            "replaying a skipped migration must preserve the user's later default choice"
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn default_change_allows_registered_root_child_churn() {
        let temp_root = temp_queue_root("default-child-churn");
        let config = test_video_config(&temp_root);
        let archive_root = temp_root.join("archive");
        fs::create_dir_all(&archive_root).expect("archive root should create");
        let queue = QueueManager::open(&config).expect("queue should open");
        let archive = queue
            .register_video_library("Archive", &archive_root, None, false, false)
            .expect("archive library should register");

        fs::create_dir(archive_root.join("added-child")).expect("child should create");
        fs::write(archive_root.join("normal.txt"), b"benign child churn")
            .expect("child file should create");
        fs::remove_dir(archive_root.join("added-child")).expect("child should remove");

        let before = queue.registry_snapshot().unwrap();
        let changed = queue
            .set_default_video_library(&archive.id)
            .expect("same root object should remain usable after child churn");
        assert_eq!(changed.default_library_id, archive.id);
        assert_eq!(changed.revision, before.revision + 1);
        assert_eq!(changed.default_category, None);

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn category_change_rejects_replaced_registered_root_without_mutating_settings() {
        let temp_root = temp_queue_root("category-replaced-root");
        let config = test_video_config(&temp_root);
        let archive_root = temp_root.join("archive");
        let displaced_root = temp_root.join("archive-original");
        fs::create_dir_all(&archive_root).expect("archive root should create");
        let queue = QueueManager::open(&config).expect("queue should open");
        let archive = queue
            .register_video_library("Archive", &archive_root, None, false, false)
            .expect("archive library should register");
        let before = queue
            .registry_snapshot()
            .expect("library settings should load");

        fs::rename(&archive_root, &displaced_root).expect("original root should be displaced");
        fs::create_dir(&archive_root).expect("replacement root should create");
        fs::write(archive_root.join("keep.txt"), b"replacement sentinel")
            .expect("replacement sentinel should write");

        let error = queue
            .set_video_library_category(&archive.id, Some(Path::new("Shows")))
            .expect_err("category change must reject a replacement root object");
        assert!(
            error.to_string().contains("root") || error.to_string().contains("available"),
            "unexpected root validation error: {error:#}"
        );
        assert_eq!(
            queue
                .registry_snapshot()
                .expect("library settings should remain readable"),
            before,
            "rejected category change must preserve settings and revision"
        );
        assert!(displaced_root.join(QUEUE_DIRECTORY).exists());
        assert_eq!(
            fs::read_dir(&archive_root)
                .expect("replacement remains readable")
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>(),
            vec![std::ffi::OsString::from("keep.txt")],
            "category validation must not mutate the replacement root"
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn category_change_allows_registered_root_child_churn() {
        let temp_root = temp_queue_root("category-child-churn");
        let config = test_video_config(&temp_root);
        let archive_root = temp_root.join("archive");
        fs::create_dir_all(&archive_root).expect("archive root should create");
        let queue = QueueManager::open(&config).expect("queue should open");
        let archive = queue
            .register_video_library("Archive", &archive_root, None, false, false)
            .expect("archive library should register");

        fs::create_dir(archive_root.join("added-child")).expect("child should create");
        fs::write(archive_root.join("normal.txt"), b"benign child churn")
            .expect("child file should write");
        fs::remove_dir(archive_root.join("added-child")).expect("child should remove");

        let before = queue
            .registry_snapshot()
            .expect("library settings should load");
        let changed = queue
            .set_video_library_category(&archive.id, Some(Path::new("Shows")))
            .expect("same root object should remain usable after child churn");
        assert_eq!(changed.revision, before.revision + 1);
        assert_eq!(
            changed
                .libraries
                .iter()
                .find(|library| library.id == archive.id)
                .and_then(|library| library.default_category.as_deref()),
            Some(Path::new("Shows"))
        );

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn pending_transfer_query_includes_history_and_refuses_uncertain_stores() {
        let (directory, _config, queue, target) = task_transfer_fixture("pending-token-query");
        let id = "pending-token-query-terminal";
        create_transfer_test_task(&queue, id, TaskStatus::Failed);
        let transfer = begin_test_transfer(&queue, id, "migration-pending-token-query", &target);
        // Model a terminal sidecar left with its token by a crash. This is not a user action;
        // write through the store primitive so the fixture exercises the history index path.
        let (store, mut record, entry) = queue
            .find_record_entry_unlocked(id)
            .expect("task lookup should succeed")
            .expect("task should exist");
        record.status = TaskStatus::Completed;
        store
            .save_mutated_record(record, entry, false)
            .expect("terminal task should move to its durable history sidecar");
        assert!(
            queue
                .has_pending_task_destination_transfer_for_migration(&transfer.migration_id)
                .expect("pending transfer token should be found in history")
        );
        assert!(
            !queue
                .has_pending_task_destination_transfer_for_migration("migration-unrelated-query")
                .expect("all stores should be verifiable for a negative result")
        );

        let receipt_task_id = "pending-token-query-receipt";
        create_transfer_test_task(&queue, receipt_task_id, TaskStatus::Failed);
        let receipt_transfer = begin_test_transfer(
            &queue,
            receipt_task_id,
            "migration-pending-receipt-query",
            &target,
        );
        let plan = queue
            .task_transfer_plan(&receipt_transfer)
            .expect("empty staging task should have a durable transfer plan");
        queue
            .commit_task_destination_transfer(&plan)
            .expect("target task should install");
        queue
            .ack_task_destination_transfer(&plan.transfer)
            .expect("queue handoff receipt should persist");
        assert!(
            queue
                .has_pending_task_destination_transfer_for_migration(&receipt_transfer.migration_id)
                .expect("unconsumed queue ACK receipt remains a pending handoff")
        );
        queue
            .clear_task_destination_transfer_ack(&plan.transfer)
            .expect("completed source cleanup should consume the exact queue receipt");
        assert!(
            !queue
                .has_pending_task_destination_transfer_for_migration(&receipt_transfer.migration_id)
                .expect("cleared queue receipt should leave no pending handoff")
        );

        let mut stores = queue.video_libraries.write().unwrap();
        let target_store = stores
            .iter_mut()
            .find(|registered| registered.library_id == target.library_id)
            .expect("target store should be registered");
        target_store.available = false;
        drop(stores);
        queue
            .video_statuses
            .write()
            .unwrap()
            .get_mut(&target.library_id)
            .expect("target status should exist")
            .available = false;
        assert!(
            queue
                .has_pending_task_destination_transfer_for_migration("migration-unrelated-query")
                .is_err(),
            "an offline store must not be reported as having no token"
        );

        drop(queue);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn task_config_allows_child_directory_churn_and_legacy_destinations() {
        let temp_root = temp_queue_root("task-config-child-churn");
        let config = test_video_config(&temp_root);
        let archive_root = temp_root.join("archive");
        fs::create_dir_all(archive_root.join("Shows"))
            .expect("category and archive roots should create");
        let queue = QueueManager::open(&config).expect("queue should open");
        let archive = queue
            .register_video_library(
                "Archive",
                &archive_root,
                Some(Path::new("Shows")),
                true,
                false,
            )
            .expect("archive library should register");

        let mut current = test_task(
            "task-config-child-churn",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=child-churn".to_string(),
            },
        );
        current.destination = Some(TaskDestination {
            library_id: archive.id.clone(),
            root: archive.root.clone(),
            category: Some(PathBuf::from("Shows")),
        });
        assert!(queue.create(current).unwrap());
        fs::create_dir(archive_root.join("unrelated-new-child"))
            .expect("unrelated child should be added");
        fs::write(archive_root.join("unrelated.txt"), b"normal child churn")
            .expect("unrelated child file should be added");
        fs::remove_dir(archive_root.join("unrelated-new-child"))
            .expect("unrelated child directory should be removed");
        let config_for_current = queue
            .task_config(&config, "task-config-child-churn")
            .expect("normal child churn should not invalidate root identity")
            .expect("task config should exist");
        assert_eq!(
            config_for_current.downloads.video_dir,
            archive_root.join("Shows")
        );

        let mut legacy = test_task(
            "task-config-legacy-destination",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=legacy-destination".to_string(),
            },
        );
        legacy.destination = Some(TaskDestination {
            library_id: archive.id.clone(),
            root: archive.root.clone(),
            category: Some(PathBuf::from("Shows")),
        });
        assert!(queue.create(legacy).unwrap());
        queue
            .update("task-config-legacy-destination", false, |record| {
                record.destination = None;
                Ok(())
            })
            .expect("legacy destination shape should persist");
        let legacy_config = queue
            .task_config(&config, "task-config-legacy-destination")
            .expect("legacy destination should resolve through its bound queue root")
            .expect("legacy task config should exist");
        assert_eq!(legacy_config.video_storage_root(), archive_root);
        assert_eq!(legacy_config.downloads.video_dir, archive_root);
        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn task_config_reads_known_task_during_pending_history_sidecar_move_without_replaying_it() {
        let temp_root = temp_queue_root("task-config-pending-sidecar-move");
        let config = test_video_config(&temp_root);
        let queue = QueueManager::open(&config).expect("queue should open");
        assert!(
            queue
                .create(test_task(
                    "task-config-pending-sidecar-move",
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=pending-sidecar-move".to_string(),
                    },
                ))
                .unwrap()
        );
        let store = queue
            .video_stores()
            .unwrap()
            .into_iter()
            .next()
            .expect("configured video store should exist");
        let mut index = store.read_index().unwrap();
        let entry = index
            .tasks
            .get_mut("task-config-pending-sidecar-move")
            .expect("task should be indexed");
        let source_path = entry.record_path.clone();
        let target_path = store
            .terminal_history_path("task-config-pending-sidecar-move")
            .unwrap();
        let mut record = store.read_record(&source_path).unwrap().unwrap();
        record.status = TaskStatus::Completed;
        store.write_record(&source_path, &record).unwrap();
        entry.move_target = Some(target_path.clone());
        store.write_index(&index).unwrap();

        queue
            .task_config(&config, "task-config-pending-sidecar-move")
            .expect("indexed task should remain visible during sidecar move recovery")
            .expect("task config should resolve from the existing source record");
        let after = store.read_index().unwrap();
        assert_eq!(
            after.tasks["task-config-pending-sidecar-move"].move_target,
            Some(target_path.clone()),
            "task_config must not replay the pending index move"
        );
        assert!(source_path.exists());
        assert!(!target_path.exists());
        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn incomplete_task_transfer_summary_counts_authorized_failed_and_legacy_tasks() {
        let temp_root = temp_queue_root("incomplete-task-transfer-summary");
        let shared_root = temp_root.join("shared");
        fs::create_dir_all(&shared_root).expect("shared root should create");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = shared_root.clone();
        config.downloads.pdf_dir = shared_root;
        let queue = QueueManager::open(&config).expect("shared queue root should open");
        let library_id = queue.registry_snapshot().unwrap().default_library_id;
        let destination = queue
            .default_video_destination()
            .expect("default library destination should resolve");

        let mut failed = test_task(
            "summary-failed-video",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=summary-failed".to_string(),
            },
        );
        failed.destination = Some(destination.clone());
        assert!(queue.create(failed).unwrap());
        queue
            .update("summary-failed-video", false, |record| {
                record.status = TaskStatus::Failed;
                Ok(())
            })
            .unwrap();

        let mut legacy = test_task(
            "summary-legacy-video",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=summary-legacy".to_string(),
            },
        );
        legacy.destination = Some(destination.clone());
        assert!(queue.create(legacy).unwrap());
        queue
            .update("summary-legacy-video", false, |record| {
                record.status = TaskStatus::Failed;
                record.destination = None;
                record
                    .staging_attempts
                    .push(PathBuf::from("/outside/staging/job-bad"));
                Ok(())
            })
            .unwrap();

        let pdf = test_task(
            "summary-failed-pdf",
            JobRequest::Pdf {
                url: "https://example.invalid/summary.pdf".to_string(),
            },
        );
        assert!(queue.create(pdf).unwrap());
        queue
            .update("summary-failed-pdf", false, |record| {
                record.status = TaskStatus::Failed;
                Ok(())
            })
            .unwrap();

        let mut completed = test_task(
            "summary-completed-video",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=summary-completed".to_string(),
            },
        );
        completed.destination = Some(destination.clone());
        assert!(queue.create(completed).unwrap());
        queue
            .update("summary-completed-video", false, |record| {
                record.status = TaskStatus::Completed;
                Ok(())
            })
            .unwrap();

        let mut cancelled = test_task(
            "summary-cancelled-video",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=summary-cancelled".to_string(),
            },
        );
        cancelled.destination = Some(destination);
        assert!(queue.create(cancelled).unwrap());
        queue
            .update("summary-cancelled-video", false, |record| {
                record.status = TaskStatus::Cancelled;
                Ok(())
            })
            .unwrap();

        let mut unauthorized = test_task(
            "summary-unauthorized-video",
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=summary-unauthorized".to_string(),
            },
        );
        unauthorized.chat_id = 999;
        unauthorized.destination = Some(queue.default_video_destination().unwrap());
        assert!(queue.create(unauthorized).unwrap());
        queue
            .update("summary-unauthorized-video", false, |record| {
                record.status = TaskStatus::Failed;
                Ok(())
            })
            .unwrap();

        let summary = queue
            .incomplete_task_transfer_summary(&library_id, &[123_456_789])
            .expect("authorized bounded summary should resolve");
        assert_eq!(summary.task_count, 2, "failed and legacy video tasks count");
        assert_eq!(
            summary.staging_bytes, None,
            "bad staging estimate is unknown"
        );

        let unauthorized_summary = queue
            .incomplete_task_transfer_summary(&library_id, &[999])
            .expect("authorized chat filter should resolve");
        assert_eq!(unauthorized_summary.task_count, 1);
        assert_eq!(unauthorized_summary.staging_bytes, Some(0));
        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
    }

    fn add_private_group_receipt_fixture(
        root: &RootedFs,
        publication_namespace: &str,
        filename: &str,
    ) -> PathBuf {
        let directory = crate::downloader::bilibili_group_receipt_namespace_path_for_test(
            root,
            publication_namespace,
        )
        .expect("private receipt namespace should create");
        let path = directory.join(filename);
        fs::write(&path, b"{}").expect("receipt fixture should write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .expect("receipt fixture should be owner-private");
        path
    }

    fn attach_grouped_attempt(queue: &QueueManager, id: &str) -> TaskRecord {
        queue
            .set_bilibili_attempt_if_generation(id, 0, grouped_attempt())
            .expect("independent grouped attempt should persist")
            .expect("task should still exist")
    }

    fn verified_grouped_attempt() -> BilibiliAttemptSnapshot {
        let mut attempt = grouped_attempt();
        for index in [16, 17, 18] {
            attempt.entry_verified(index);
        }
        attempt
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
        assert_eq!(snapshot.bilibili_publication_namespace, None);
        assert!(snapshot.bilibili_resolved_stream_ids.is_empty());
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
        let mut namespaced = original.clone();
        namespaced.bilibili_publication_namespace = Some("internal-task-scope".into());
        assert!(original.blocking_differences(&namespaced).is_empty());

        let mut resolved = original.clone();
        resolved.bilibili_resolved_stream_ids = vec!["media-a".into(), "media-b".into()];
        let mut reordered_resolved = resolved.clone();
        reordered_resolved.bilibili_resolved_stream_ids = vec!["media-b".into(), "media-a".into()];
        assert!(
            resolved
                .blocking_differences(&reordered_resolved)
                .is_empty()
        );
        assert!(resolved.media_requirements_match_for(&reordered_resolved, "media-a"));

        let mut no_resolved_probe = resolved.clone();
        no_resolved_probe.bilibili_resolved_stream_ids.clear();
        assert_eq!(
            resolved.blocking_differences(&no_resolved_probe),
            vec!["resolved stream availability"]
        );
        assert!(!resolved.media_requirements_match_for(&no_resolved_probe, "media-a"));
        assert!(
            resolved.media_requirements_match_for(&no_resolved_probe, "media-not-in-either-plan")
        );

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
    fn newly_resolved_streams_preserve_receipts_only_when_known_requirements_match() {
        let unknown = grouped_plan(
            "Season",
            Some("mode=all;quality=high"),
            &[(1, "Episode A", "media-a")],
        );
        let mut newly_resolved = unknown.clone();
        newly_resolved.bilibili_resolved_stream_ids = vec!["media-a".into()];
        newly_resolved.selected_format_ids = vec!["media-a:format-720".into()];
        newly_resolved.exact_sizes = vec![PlanSize {
            subject: "media-a:primary".into(),
            bytes: 1_024,
            provenance: "probe".into(),
        }];
        newly_resolved.resolution_codecs = vec!["media-a:1080p:h264".into()];
        assert!(unknown.media_requirements_match_for(&newly_resolved, "media-a"));

        let mut no_longer_resolved = newly_resolved.clone();
        no_longer_resolved.bilibili_resolved_stream_ids.clear();
        assert!(!newly_resolved.media_requirements_match_for(&no_longer_resolved, "media-a"));

        // Older plans could persist media requirements before recording probe completion.
        let mut legacy = unknown;
        legacy.selected_format_ids = vec!["media-a:format-720".into()];
        legacy.exact_sizes = vec![PlanSize {
            subject: "media-a:primary".into(),
            bytes: 1_024,
            provenance: "probe".into(),
        }];
        legacy.resolution_codecs = vec!["media-a:1080p:h264".into()];

        let mut legacy_resolved = legacy.clone();
        legacy_resolved.bilibili_resolved_stream_ids = vec!["media-a".into()];
        assert!(legacy.media_requirements_match_for(&legacy_resolved, "media-a"));

        let mut changed_format = legacy_resolved.clone();
        changed_format.selected_format_ids = vec!["media-a:format-1080".into()];
        assert!(!legacy.media_requirements_match_for(&changed_format, "media-a"));

        let mut changed_size = legacy_resolved.clone();
        changed_size.exact_sizes[0].bytes += 1;
        assert!(!legacy.media_requirements_match_for(&changed_size, "media-a"));

        let mut changed_codec = legacy_resolved;
        changed_codec.resolution_codecs = vec!["media-a:1080p:av1".into()];
        assert!(!legacy.media_requirements_match_for(&changed_codec, "media-a"));
    }

    #[test]
    fn accepting_newly_resolved_streams_preserves_compatible_receipts_and_counts() {
        let directory = temp_queue_root("grouped-newly-resolved-reconcile");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("grouped-newly-resolved-reconcile");
        task.status = TaskStatus::Running;
        queue.create(task.clone()).unwrap();

        let profile = "mode=all;quality=high";
        let entries = [
            (1, "Episode A", "media-a"),
            (2, "Episode B", "media-b"),
            (3, "Episode C", "media-c"),
        ];
        let old_plan = grouped_plan("Season", Some(profile), &entries);
        queue
            .set_plan_if_generation(&task.id, 0, old_plan.clone())
            .unwrap()
            .unwrap();

        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            entries
                .iter()
                .map(|(index, title, id)| (*index, (*title).into(), Some((*id).into()))),
            0,
        );
        attempt.media_profile = Some(profile.into());
        let downloaded_path = config.downloads.video_dir.join("episode-a.mp4");
        let already_present_path = config.downloads.video_dir.join("episode-b.mp4");
        let downloaded_hash = "a".repeat(64);
        let already_present_hash = "b".repeat(64);
        attempt.entry_verified_with_outputs(
            1,
            vec![downloaded_path.clone()],
            BTreeMap::from([(
                downloaded_path.display().to_string(),
                downloaded_hash.clone(),
            )]),
        );
        attempt.entry_already_present_with_outputs(
            2,
            vec![already_present_path.clone()],
            BTreeMap::from([(
                already_present_path.display().to_string(),
                already_present_hash.clone(),
            )]),
        );
        attempt.entries[2].requires_redownload = true;
        queue
            .set_bilibili_attempt_if_generation(&task.id, 0, attempt)
            .unwrap()
            .unwrap();

        let mut proposed = old_plan;
        proposed.bilibili_resolved_stream_ids =
            entries.iter().map(|(_, _, id)| (*id).into()).collect();
        proposed.selected_format_ids = entries
            .iter()
            .map(|(_, _, id)| format!("{id}:format-720"))
            .collect();
        proposed.exact_sizes = entries
            .iter()
            .map(|(_, _, id)| PlanSize {
                subject: format!("{id}:primary"),
                bytes: 1_024,
                provenance: "probe".into(),
            })
            .collect();
        proposed.resolution_codecs = entries
            .iter()
            .map(|(_, _, id)| format!("{id}:1080p:h264"))
            .collect();
        let differences = queue
            .set_plan_if_generation(&task.id, 0, proposed)
            .unwrap()
            .unwrap()
            .1;
        assert_eq!(
            differences,
            vec![
                "resolved stream availability",
                "selected format",
                "exact size",
                "media properties"
            ]
        );

        let accepted = queue
            .accept_proposed_plan_for_chat_if_generation(&task.id, 0, task.chat_id)
            .unwrap()
            .unwrap();
        assert_eq!(accepted.media_entries_total, 3);
        assert_eq!(accepted.media_entries_completed, 2);
        assert_eq!(accepted.media_entries_failed, 0);
        let snapshot = accepted.bilibili_attempt.unwrap();
        assert_eq!(snapshot.skipped_entries, 1);
        assert_eq!(
            snapshot.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert!(!snapshot.entries[0].already_present);
        assert_eq!(
            snapshot.entries[0]
                .published_media_hashes
                .get(&downloaded_path.display().to_string()),
            Some(&downloaded_hash)
        );
        assert_eq!(
            snapshot.entries[0].published_media_paths,
            vec![downloaded_path]
        );
        assert_eq!(
            snapshot.entries[1].state,
            BilibiliAttemptEntryState::Verified
        );
        assert!(snapshot.entries[1].already_present);
        assert_eq!(
            snapshot.entries[1]
                .published_media_hashes
                .get(&already_present_path.display().to_string()),
            Some(&already_present_hash)
        );
        assert_eq!(
            snapshot.entries[1].published_media_paths,
            vec![already_present_path]
        );
        assert_eq!(
            snapshot.entries[2].state,
            BilibiliAttemptEntryState::Pending
        );
        assert!(snapshot.entries[2].requires_redownload);

        let verified = queue
            .update_bilibili_attempt_if_generation(
                &task.id,
                0,
                3,
                &BilibiliAttemptUpdate::Verified {
                    media_paths: vec![config.downloads.video_dir.join("episode-c.mp4")],
                    hashes: BTreeMap::new(),
                },
            )
            .unwrap()
            .unwrap();
        let verified_snapshot = verified.bilibili_attempt.unwrap();
        assert_eq!(verified.media_entries_completed, 3);
        assert!(!verified_snapshot.entries[2].requires_redownload);
        drop(queue);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn group_publication_namespace_is_task_scoped_and_survives_acceptance_and_reload() {
        let directory = temp_queue_root("group-publication-namespace");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let mut task = bilibili_task("group-publication-namespace");
        task.status = TaskStatus::Running;
        let namespace = task.bilibili_publication_namespace();
        assert_ne!(
            namespace,
            bilibili_task("another-publication-task").bilibili_publication_namespace()
        );
        queue.create(task.clone()).unwrap();

        let media_profile = "mode=all;quality=high";
        let old_plan = grouped_plan(
            "Season",
            Some(media_profile),
            &[(16, "Episode A", "media-a")],
        );
        let planned = queue
            .set_plan_if_generation(&task.id, 0, old_plan)
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(
            planned
                .plan
                .as_ref()
                .unwrap()
                .bilibili_publication_namespace
                .as_deref(),
            Some(namespace.as_str())
        );

        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [(16, "Episode A".into(), Some("media-a".into()))],
            0,
        );
        attempt.media_profile = Some(media_profile.into());
        let saved = queue
            .set_bilibili_attempt_if_generation(&task.id, 0, attempt)
            .unwrap()
            .unwrap();
        assert_eq!(
            saved
                .bilibili_attempt
                .as_ref()
                .unwrap()
                .bilibili_publication_namespace
                .as_deref(),
            Some(namespace.as_str())
        );

        let proposed = grouped_plan(
            "Season",
            Some(media_profile),
            &[(101, "Episode A", "media-a")],
        );
        let proposed_record = queue
            .set_plan_if_generation(&task.id, 0, proposed)
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(proposed_record.status, TaskStatus::AwaitingConfirmation);
        assert_eq!(
            proposed_record
                .proposed_plan
                .as_ref()
                .unwrap()
                .bilibili_publication_namespace
                .as_deref(),
            Some(namespace.as_str())
        );

        let accepted = queue
            .accept_proposed_plan_for_chat_if_generation(&task.id, 0, task.chat_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            accepted
                .plan
                .as_ref()
                .unwrap()
                .bilibili_publication_namespace
                .as_deref(),
            Some(namespace.as_str())
        );
        assert_eq!(
            accepted
                .bilibili_attempt
                .as_ref()
                .unwrap()
                .bilibili_publication_namespace
                .as_deref(),
            Some(namespace.as_str())
        );
        drop(queue);

        let queue = QueueManager::open(&config).unwrap();
        let restored = queue.get(&task.id).unwrap().unwrap();
        assert_eq!(
            restored
                .plan
                .as_ref()
                .unwrap()
                .bilibili_publication_namespace
                .as_deref(),
            Some(namespace.as_str())
        );
        assert_eq!(
            restored
                .bilibili_attempt
                .as_ref()
                .unwrap()
                .bilibili_publication_namespace
                .as_deref(),
            Some(namespace.as_str())
        );
        let resumed = queue.claim_resume(&task.id, false).unwrap().unwrap();
        assert_eq!(resumed.generation, 1);
        assert_eq!(
            resumed
                .plan
                .as_ref()
                .unwrap()
                .bilibili_publication_namespace
                .as_deref(),
            Some(namespace.as_str())
        );
        assert_eq!(
            resumed
                .bilibili_attempt
                .as_ref()
                .unwrap()
                .bilibili_publication_namespace
                .as_deref(),
            Some(namespace.as_str())
        );

        drop(queue);
        fs::remove_dir_all(directory).unwrap();
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
    fn parent_cancel_overrides_failed_group_summaries_and_survives_late_snapshot_and_reload() {
        use crate::router::BilibiliSelection;

        let directory = temp_queue_root("episode-group-cancel-after-failure");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).unwrap();
        let cases = [
            ("episode-group-cancel-partial", true),
            ("episode-group-cancel-failed", false),
        ];

        for (task_id, partially_completed) in cases {
            let mut task = test_task(
                task_id,
                JobRequest::Bilibili {
                    url: "https://www.bilibili.com/bangumi/media/md1376".into(),
                    selection: Some(BilibiliSelection::All),
                },
            );
            task.status = TaskStatus::Running;
            queue.create(task.clone()).unwrap();

            let mut snapshot = BilibiliAttemptSnapshot::new_independent(
                "Season".into(),
                if partially_completed {
                    vec![
                        (16, "Episode 16".into(), Some("ep-16".into())),
                        (17, "Episode 17".into(), Some("ep-17".into())),
                    ]
                } else {
                    vec![(17, "Episode 17".into(), Some("ep-17".into()))]
                },
                0,
            );
            if partially_completed {
                snapshot.entry_verified(16);
            }
            snapshot.entry_failed_with_error(17, 2, "episode download failed".into());
            snapshot.finish(if partially_completed {
                BilibiliAttemptOutcome::PartiallyCompleted
            } else {
                BilibiliAttemptOutcome::Failed
            });
            queue
                .set_bilibili_attempt_if_generation(&task.id, 0, snapshot.clone())
                .unwrap()
                .unwrap();

            let requested = queue
                .cancel_if_generation(&task.id, 0, task.chat_id)
                .unwrap()
                .unwrap();
            assert!(requested.cancel_requested);

            let cancelled = queue
                .finish_cancellation_with_bilibili_attempt_if_generation(
                    &task.id,
                    0,
                    Some(snapshot.clone()),
                )
                .unwrap()
                .unwrap();
            assert_eq!(cancelled.status, TaskStatus::Cancelled);
            assert_eq!(
                cancelled.bilibili_attempt.as_ref().unwrap().outcome,
                Some(BilibiliAttemptOutcome::Cancelled)
            );
            assert!(
                cancelled
                    .bilibili_attempt
                    .as_ref()
                    .unwrap()
                    .summary()
                    .contains("Outcome: cancelled")
            );

            // A late pre-cancel failure snapshot cannot make the summary disagree with queue state.
            let delayed = queue
                .set_bilibili_attempt_if_generation(&task.id, 0, snapshot)
                .unwrap()
                .unwrap();
            assert_eq!(delayed.status, TaskStatus::Cancelled);
            assert_eq!(
                delayed.bilibili_attempt.as_ref().unwrap().outcome,
                Some(BilibiliAttemptOutcome::Cancelled)
            );
        }

        drop(queue);
        let queue = QueueManager::open(&config).unwrap();
        for (task_id, partially_completed) in cases {
            let restored = queue.get(task_id).unwrap().unwrap();
            assert_eq!(restored.status, TaskStatus::Cancelled);
            let attempt = restored.bilibili_attempt.unwrap();
            assert_eq!(attempt.outcome, Some(BilibiliAttemptOutcome::Cancelled));
            assert_eq!(attempt.failed_entries(), 1);
            assert_eq!(
                attempt.verified_entries(),
                if partially_completed { 1 } else { 0 }
            );
            assert!(attempt.summary().contains("Outcome: cancelled"));
            assert!(!attempt.summary().contains("Outcome: failed"));
            assert!(!attempt.summary().contains("Outcome: partially completed"));
        }

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
    fn terminal_failure_variants_do_not_supersede_pending_cancellation() {
        let temp_root = temp_queue_root("terminal-failure-cancellation-guard");
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = temp_root.join("videos");
        config.downloads.pdf_dir = temp_root.join("pdfs");
        fs::create_dir_all(&config.downloads.video_dir).unwrap();
        fs::create_dir_all(&config.downloads.pdf_dir).unwrap();
        let queue = QueueManager::open(&config).unwrap();
        let task_id = "terminal-failure-cancellation-guard";
        let mut task = test_task(
            task_id,
            JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md1376".into(),
                selection: Some(crate::router::BilibiliSelection::All),
            },
        );
        task.status = TaskStatus::Running;
        assert!(queue.create(task).unwrap());
        let requested = queue.cancel(task_id, 123_456_789).unwrap().unwrap();
        assert!(requested.cancel_requested);

        let failure = queue
            .fail_with_bilibili_attempt_if_generation_unless_cancelled(
                task_id,
                0,
                "worker failed".into(),
                None,
            )
            .unwrap_err();
        assert!(format!("{failure:#}").contains("pending cancellation request"));
        let interruption = queue
            .interrupt_with_bilibili_attempt_if_generation_unless_cancelled(
                task_id,
                0,
                "worker interrupted".into(),
                None,
            )
            .unwrap_err();
        assert!(format!("{interruption:#}").contains("pending cancellation request"));

        let current = queue.get(task_id).unwrap().unwrap();
        assert_eq!(current.status, TaskStatus::Running);
        assert!(current.cancel_requested);

        drop(queue);
        let _ = fs::remove_dir_all(temp_root);
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
        assert_eq!(
            queue
                .video_stores()
                .expect("video stores should remain readable")
                .len(),
            1,
            "a transient queue-file read error must not retire a reachable registered root"
        );
        let (retried_record, _) = queue
            .register_cancellation_if_queued_generation(id, 0)
            .expect("retry should read the task after the one-shot provider failure")
            .expect("the original queued task should remain searchable");
        assert_eq!(retried_record.id, id);

        provider.fail_next_read("listing read failed");
        let error = queue
            .all_records_isolated()
            .expect_err("a transient read must not look like an empty task queue");
        assert!(format!("{error:#}").contains("listing read failed"));
        assert_eq!(
            queue
                .all_records_isolated()
                .expect("queue listing should recover on retry")
                .iter()
                .filter(|record| record.id == id)
                .count(),
            1
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
    fn unchanged_queue_reads_do_not_rewrite_the_index() {
        let temp_root = temp_queue_root("unchanged-queue-read-index");
        let video_root = temp_root.join("videos");
        let pdf_root = temp_root.join("pdfs");
        fs::create_dir_all(&video_root).expect("video root should create");
        fs::create_dir_all(&pdf_root).expect("PDF root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = video_root;
        config.downloads.pdf_dir = pdf_root;
        let file_provider = Arc::new(MockQueueFileProvider::default());
        let provider: Arc<dyn QueueFileProvider> = file_provider.clone();
        let queue = QueueManager::open_with_file_provider(&config, provider)
            .expect("queue should open with the mock File Provider");
        let task_id = "unchanged-queue-read-index";
        assert!(
            queue
                .create(test_task(
                    task_id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
                    },
                ))
                .expect("task should persist")
        );
        let writes_before_reads = file_provider.write_paths().len();

        queue
            .get(task_id)
            .expect("task lookup should succeed")
            .expect("task should remain indexed");
        queue
            .get("missing-task")
            .expect("missing task lookup should succeed");
        queue
            .list(123_456_789, false, 0)
            .expect("queue listing should succeed");

        assert_eq!(
            file_provider.write_paths().len(),
            writes_before_reads,
            "ordinary reads should not write an unchanged queue index"
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

        #[cfg(target_os = "macos")]
        {
            // File Provider may return /var/... even when RootedFs retained the canonical
            // /private/var/... root. Accept that verified system alias and map it back to the
            // configured logical spelling.
            let temp_path = std::env::temp_dir();
            let system_root = RootedFs::new(&temp_path).expect("system temp root should bind");
            if let Ok(suffix) = system_root.root_path().strip_prefix("/private/var") {
                let provider_alias = Path::new("/var").join(suffix).join("provider-child");
                assert_eq!(
                    coordinated_path_under_root(&system_root, &provider_alias)
                        .expect("verified /var accessor alias should map under root"),
                    system_root.logical_root_path().join("provider-child")
                );
            }
        }

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

    #[cfg(target_os = "macos")]
    #[test]
    fn task_transfer_rewrites_file_provider_var_paths_and_keep_both_names() {
        let source_root = Path::new("/private/var/folders/test/videos");
        let target_root = Path::new("/private/var/folders/test/archive");
        let old_group = Path::new("/private/var/folders/test/videos/Season");
        let renamed_group = Path::new("/private/var/folders/test/archive/Season (1)");
        let source_path = Path::new("/var/folders/test/videos/Season/Episode 1.mkv");
        let rewrites = BTreeMap::from([(old_group.to_path_buf(), renamed_group.to_path_buf())]);

        assert_eq!(
            rewrite_published_path(source_path, source_root, target_root, &rewrites),
            renamed_group.join("Episode 1.mkv"),
            "a File Provider /var spelling must follow the exact keep-both rename"
        );
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
    fn removing_shared_video_library_retains_pdf_queue_owner_across_processes() {
        let temp_root = temp_queue_root("removed-shared-video-pdf-owner");
        let shared_root = temp_root.join("videos");
        let archive_root = temp_root.join("archive");
        fs::create_dir_all(&shared_root).expect("shared queue root should create");
        fs::create_dir_all(&archive_root).expect("archive root should create");

        let mut config = AppConfig::for_test();
        config.downloads.video_dir = shared_root.clone();
        config.downloads.pdf_dir = shared_root;
        let queue = QueueManager::open(&config).expect("shared video/PDF root should open");
        let removed_library_id = queue.registry_snapshot().unwrap().default_library_id;
        let archive = queue
            .register_video_library("Archive", &archive_root, None, false, false)
            .expect("archive library should register");
        queue
            .set_default_video_library(&archive.id)
            .expect("archive should become default before removal");
        queue
            .remove_video_library(&removed_library_id)
            .expect("old shared video library should be removable");

        let mut child = spawn_queue_owner_child(&temp_root, "pdf-shared");
        let result_path = temp_root.join("owner-pdf-shared-result");
        wait_for_child_marker(&mut child, &result_path, "second shared-root owner");
        let output = child
            .wait_with_output()
            .expect("shared-root owner output should collect");
        assert!(
            output.status.success(),
            "owner probe failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read(&result_path).expect("owner probe should persist its result"),
            b"busy",
            "removing video settings must not release the PDF queue owner lock"
        );

        drop(queue);
        let mut child = spawn_queue_owner_child(&temp_root, "pdf-shared-released");
        let result_path = temp_root.join("owner-pdf-shared-released-result");
        wait_for_child_marker(&mut child, &result_path, "released shared-root owner");
        let output = child
            .wait_with_output()
            .expect("released shared-root owner output should collect");
        assert!(output.status.success());
        assert_eq!(
            fs::read(&result_path).expect("owner probe should persist its result"),
            b"opened",
            "the shared PDF queue owner lock should release when its manager drops"
        );

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
        config.downloads.pdf_dir = if slot == "pdf-shared" || slot == "pdf-shared-released" {
            root.join("videos")
        } else {
            root.join("pdfs")
        };
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
    fn terminal_independent_group_records_clean_only_their_receipts() {
        let temp_root = temp_queue_root("terminal-group-receipt-cleanup");
        let config = test_video_config(&temp_root);
        let queue = QueueManager::open(&config).expect("task queue should open");

        let cancelled_id = "task-receipt-cleanup-cancelled";
        assert!(
            queue
                .create(bilibili_task(cancelled_id))
                .expect("cancelled task should persist")
        );
        let cancelled = attach_grouped_attempt(&queue, cancelled_id);
        let cancelled_receipt = add_private_group_receipt_fixture(
            &queue.video.root,
            &cancelled.bilibili_publication_namespace(),
            &format!("{}.json", "a".repeat(64)),
        );

        let completed_id = "task-receipt-cleanup-completed";
        assert!(
            queue
                .create(bilibili_task(completed_id))
                .expect("completed task should persist")
        );
        queue
            .set_status(completed_id, TaskStatus::Running, None)
            .expect("completed task should start");
        queue
            .set_bilibili_attempt_if_generation(completed_id, 0, verified_grouped_attempt())
            .expect("verified grouped attempt should persist")
            .expect("completed task should still exist");
        let completed = queue.get(completed_id).expect("task should load").unwrap();
        let completed_receipt = add_private_group_receipt_fixture(
            &queue.video.root,
            &completed.bilibili_publication_namespace(),
            &format!(".{}.next", "b".repeat(64)),
        );
        queue
            .begin_verification(completed_id)
            .expect("verification should begin")
            .expect("completed task should still exist");

        let unrelated_id = "task-receipt-cleanup-unrelated";
        assert!(
            queue
                .create(bilibili_task(unrelated_id))
                .expect("unrelated task should persist")
        );
        let unrelated = attach_grouped_attempt(&queue, unrelated_id);
        let unrelated_receipt = add_private_group_receipt_fixture(
            &queue.video.root,
            &unrelated.bilibili_publication_namespace(),
            &format!("{}.json", "c".repeat(64)),
        );

        let cancelled = queue
            .cancel(cancelled_id, 123_456_789)
            .expect("cancellation should persist")
            .expect("cancelled task should still exist");
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        assert!(!cancelled_receipt.exists());
        assert!(completed_receipt.is_file());
        assert!(unrelated_receipt.is_file());

        let published_media = queue.video.root_path.join("published-episode.mp4");
        fs::write(
            &published_media,
            b"published media should survive receipt cleanup",
        )
        .expect("test publication should write");
        let completed = queue
            .complete(
                completed_id,
                queue.video.root_path.display().to_string(),
                std::slice::from_ref(&published_media),
                BTreeMap::new(),
            )
            .expect("verified group should complete");
        assert_eq!(completed.status, TaskStatus::Completed);
        assert!(!completed_receipt.exists());
        assert!(unrelated_receipt.is_file());
        assert!(published_media.is_file());

        drop(queue);
        fs::remove_dir_all(temp_root).expect("test directory should clean");
    }

    #[test]
    fn cancelled_group_plan_cleans_receipts_when_attempt_snapshot_is_missing() {
        let temp_root = temp_queue_root("cancelled-group-plan-without-attempt");
        let config = test_video_config(&temp_root);
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-cancelled-group-plan-without-attempt";
        assert!(
            queue
                .create(bilibili_task(id))
                .expect("task should persist")
        );
        queue
            .set_status(id, TaskStatus::Running, None)
            .expect("task should start");
        let planned = queue
            .set_plan_if_generation(
                id,
                0,
                grouped_plan(
                    "Season",
                    None,
                    &[(16, "Episode A", "media-a"), (17, "Episode B", "media-b")],
                ),
            )
            .expect("accepted group plan should persist")
            .expect("task should still exist")
            .0;
        assert!(planned.bilibili_attempt.is_none());
        let receipt = add_private_group_receipt_fixture(
            &queue.video.root,
            &planned.bilibili_publication_namespace(),
            &format!("{}.json", "3".repeat(64)),
        );
        queue
            .set_status(id, TaskStatus::AwaitingConfirmation, None)
            .expect("task should leave active running state");

        let cancelled = queue
            .cancel(id, 123_456_789)
            .expect("cancellation should persist")
            .expect("task should be cancelled");
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        assert!(!receipt.exists());

        drop(queue);
        fs::remove_dir_all(temp_root).expect("test directory should clean");
    }

    #[test]
    fn nonterminal_group_records_retain_receipts() {
        let temp_root = temp_queue_root("nonterminal-group-receipt-retention");
        let config = test_video_config(&temp_root);
        let queue = QueueManager::open(&config).expect("task queue should open");
        let cases = [
            ("task-receipt-retain-failed", TaskStatus::Failed, "d"),
            (
                "task-receipt-retain-partial",
                TaskStatus::PartiallyCompleted,
                "a",
            ),
            (
                "task-receipt-retain-interrupted",
                TaskStatus::Interrupted,
                "e",
            ),
        ];
        let mut receipts = Vec::new();
        for (id, status, hex) in cases {
            assert!(
                queue
                    .create(bilibili_task(id))
                    .expect("task should persist")
            );
            let record = attach_grouped_attempt(&queue, id);
            receipts.push(add_private_group_receipt_fixture(
                &queue.video.root,
                &record.bilibili_publication_namespace(),
                &format!("{}.json", hex.repeat(64)),
            ));
            queue
                .set_status(id, status, None)
                .expect("nonterminal status should persist");
        }
        assert!(receipts.iter().all(|receipt| receipt.is_file()));

        drop(queue);
        fs::remove_dir_all(temp_root).expect("test directory should clean");
    }

    #[test]
    fn terminal_receipt_cleanup_waits_for_durable_queue_index_write() {
        let temp_root = temp_queue_root("terminal-receipt-index-write-failure");
        let config = test_video_config(&temp_root);
        let file_provider = Arc::new(MockQueueFileProvider::default());
        let file_provider_trait: Arc<dyn QueueFileProvider> = file_provider.clone();
        let queue = QueueManager::open_with_file_provider(&config, file_provider_trait)
            .expect("task queue should open");
        let id = "task-receipt-index-write-failure";
        assert!(
            queue
                .create(bilibili_task(id))
                .expect("task should persist")
        );
        let record = attach_grouped_attempt(&queue, id);
        let receipt = add_private_group_receipt_fixture(
            &queue.video.root,
            &record.bilibili_publication_namespace(),
            &format!("{}.json", "f".repeat(64)),
        );

        // The cancellation lookup writes the first index; the record write succeeds next, then
        // the terminal move intent index write fails. Cleanup must wait for all durable writes.
        file_provider.fail_write_after(2, "injected terminal index write failure");
        assert!(queue.cancel(id, 123_456_789).is_err());
        let persisted = queue
            .video
            .read_record(&queue.video.task_path(id).unwrap())
            .expect("terminal record should read")
            .expect("terminal record should remain on disk");
        assert_eq!(persisted.status, TaskStatus::Cancelled);
        assert!(receipt.is_file(), "failed index write must retain receipts");

        drop(queue);
        fs::remove_dir_all(temp_root).expect("test directory should clean");
    }

    #[test]
    fn startup_retries_cleanup_for_terminal_history_records() {
        let temp_root = temp_queue_root("startup-terminal-receipt-cleanup");
        let config = test_video_config(&temp_root);
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-startup-terminal-receipt-cleanup";
        assert!(
            queue
                .create(bilibili_task(id))
                .expect("task should persist")
        );
        let record = attach_grouped_attempt(&queue, id);
        let receipt = add_private_group_receipt_fixture(
            &queue.video.root,
            &record.bilibili_publication_namespace(),
            &format!("{}.json", "1".repeat(64)),
        );

        let (mut persisted, mut entry) = queue
            .video
            .get_task(id)
            .expect("task should load")
            .expect("task should exist");
        persisted.status = TaskStatus::Completed;
        persisted.revision = persisted.revision.saturating_add(1);
        let source = entry.record_path.clone();
        let destination = queue
            .video
            .terminal_history_path(id)
            .expect("history path should resolve");
        queue
            .video
            .ensure_private_history_directory()
            .expect("history directory should be private");
        queue
            .video
            .write_record(&source, &persisted)
            .expect("terminal record should persist");
        entry.move_target = Some(destination.clone());
        entry.revision = persisted.revision;
        entry.updated_at = persisted.updated_at;
        queue
            .video
            .save_index_task(id, entry.clone())
            .expect("terminal move intent should persist");
        assert_eq!(
            queue
                .video
                .move_record_to_sidecar(id, &source, &destination)
                .expect("terminal history move should succeed"),
            SidecarMoveStatus::Moved
        );
        entry.record_path = destination;
        entry.move_target = None;
        queue
            .video
            .save_index_task(id, entry)
            .expect("terminal history index should persist");
        assert!(receipt.is_file());

        drop(queue);
        let reopened = QueueManager::open(&config).expect("terminal history should recover");
        assert!(
            !receipt.exists(),
            "startup should retry cleanup for already-terminal history records"
        );
        let recovered = reopened
            .get(id)
            .expect("terminal history record should load")
            .expect("terminal history record should remain indexed");
        assert_eq!(recovered.status, TaskStatus::Completed);

        drop(reopened);
        fs::remove_dir_all(temp_root).expect("test directory should clean");
    }

    #[test]
    fn receipt_cleanup_failure_does_not_fail_terminal_persistence_and_retries_at_startup() {
        let temp_root = temp_queue_root("receipt-cleanup-failure-retry");
        let config = test_video_config(&temp_root);
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-receipt-cleanup-failure-retry";
        assert!(
            queue
                .create(bilibili_task(id))
                .expect("task should persist")
        );
        let record = attach_grouped_attempt(&queue, id);
        let namespace = record.bilibili_publication_namespace();
        let receipt = add_private_group_receipt_fixture(
            &queue.video.root,
            &namespace,
            &format!("{}.json", "4".repeat(64)),
        );
        let namespace_directory =
            crate::downloader::bilibili_group_receipt_namespace_path_for_test(
                &queue.video.root,
                &namespace,
            )
            .expect("receipt namespace should exist");
        let unexpected = namespace_directory.join("unrecognized-entry");
        fs::write(&unexpected, b"retain until recovery")
            .expect("unexpected namespace entry should write");

        let cancelled = queue
            .cancel(id, 123_456_789)
            .expect("cleanup failure should not fail terminal persistence")
            .expect("task should remain available");
        assert_eq!(cancelled.status, TaskStatus::Cancelled);
        assert!(receipt.is_file());
        assert!(unexpected.is_file());
        fs::remove_file(&unexpected).expect("unexpected fixture should remove before restart");

        drop(queue);
        let reopened = QueueManager::open(&config).expect("terminal task should recover");
        assert!(
            !receipt.exists(),
            "startup should retry a cleanup that previously failed"
        );
        assert_eq!(
            reopened
                .get(id)
                .expect("terminal history should load")
                .expect("cancelled task should remain indexed")
                .status,
            TaskStatus::Cancelled
        );

        drop(reopened);
        fs::remove_dir_all(temp_root).expect("test directory should clean");
    }

    #[test]
    fn stale_generation_does_not_clean_group_receipts() {
        let temp_root = temp_queue_root("stale-generation-receipt-retention");
        let config = test_video_config(&temp_root);
        let queue = QueueManager::open(&config).expect("task queue should open");
        let id = "task-stale-generation-receipt-retention";
        assert!(
            queue
                .create(bilibili_task(id))
                .expect("task should persist")
        );
        let record = attach_grouped_attempt(&queue, id);
        let receipt = add_private_group_receipt_fixture(
            &queue.video.root,
            &record.bilibili_publication_namespace(),
            &format!("{}.json", "2".repeat(64)),
        );

        assert!(
            queue
                .complete_if_generation(id, 1, "stale completion".into(), &[], BTreeMap::new())
                .expect("stale completion should be ignored")
                .is_none()
        );
        assert!(receipt.is_file());

        drop(queue);
        fs::remove_dir_all(temp_root).expect("test directory should clean");
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

    #[tokio::test]
    async fn queue_progress_notifies_state_without_requesting_library_publication() {
        let directory = temp_queue_root("state-notify");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).expect("queue should open");
        let state_changed = queue.state_change_notify();
        let published = queue.publication_notify();
        let id = "state-notify-task";

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
        tokio::time::timeout(Duration::from_secs(1), state_changed.notified())
            .await
            .expect("task creation should notify cloud state");

        queue
            .set_status(id, TaskStatus::Running, None)
            .expect("task status should persist");
        tokio::time::timeout(Duration::from_secs(1), state_changed.notified())
            .await
            .expect("status change should notify cloud state");

        let progress = queue
            .set_collection_progress(id, 5, 3, 1)
            .expect("task progress should persist");
        assert_eq!(progress.media_entries_completed, 3);
        tokio::time::timeout(Duration::from_secs(1), state_changed.notified())
            .await
            .expect("progress change should notify cloud state");
        assert!(
            tokio::time::timeout(Duration::from_millis(10), published.notified())
                .await
                .is_err(),
            "ordinary queue progress must not request a library publication scan"
        );

        drop(queue);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn active_chat_snapshot_sorts_once_across_allowed_chats_and_filters_history() {
        let directory = temp_queue_root("active-chat-snapshot-order");
        let config = test_video_config(&directory);
        let queue = QueueManager::open(&config).expect("queue should open");
        let make_task = |id: &str, chat_id: i64, status: TaskStatus, updated_at| {
            let mut task = TaskRecord::new(
                id.to_string(),
                1,
                2,
                chat_id,
                Some(3),
                0,
                JobRequest::Youtube {
                    url: format!("https://example.invalid/{id}"),
                },
            );
            task.status = status;
            task.updated_at = updated_at;
            task
        };

        for task in [
            make_task("a-video", 10, TaskStatus::Received, 100),
            make_task("b-video", 20, TaskStatus::Failed, 100),
            make_task("c-video", 10, TaskStatus::Completed, 200),
            make_task("d-video", 30, TaskStatus::Running, 300),
            make_task("e-video", 10, TaskStatus::AwaitingConfirmation, 100),
        ] {
            assert!(queue.create(task).expect("fixture task should persist"));
        }

        let records = queue
            .active_records_for_chats(&[20, 10, 20], 100)
            .expect("allowed-chat snapshot should load");
        assert_eq!(
            records
                .iter()
                .map(|record| record.id.as_str())
                .collect::<Vec<_>>(),
            vec!["b-video", "a-video", "e-video"],
            "global timestamp ordering should keep configured-chat stable ties without duplicates"
        );
        assert!(
            records
                .iter()
                .all(|record| record.chat_id == 10 || record.chat_id == 20)
        );
        assert!(records.iter().all(|record| !record.status.is_history()));

        drop(queue);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn active_chat_snapshot_reads_only_bounded_indexed_candidates() {
        const HISTORY_COUNT: usize = 1_024;

        let directory = temp_queue_root("active-chat-snapshot-bounded-reads");
        let config = test_video_config(&directory);
        let provider = Arc::new(MockQueueFileProvider::default());
        let queue = QueueManager::open_with_file_provider(&config, provider.clone())
            .expect("queue should open");
        queue
            .video
            .ensure_private_history_directory()
            .expect("history directory should be private");

        let mut index = queue.video.read_index().expect("queue index should load");
        for number in 0..HISTORY_COUNT {
            let id = format!("history-{number:05}");
            let mut record = test_task(
                &id,
                JobRequest::Youtube {
                    url: format!("https://example.invalid/{id}"),
                },
            );
            record.status = TaskStatus::Completed;
            record.updated_at = 10_000 + number as u64;
            let path = queue
                .video
                .terminal_history_path(&id)
                .expect("history path should be valid");
            queue
                .video
                .write_record(&path, &record)
                .expect("history record should persist");
            index.tasks.insert(
                id,
                TaskIndexEntry {
                    chat_id: record.chat_id,
                    record_path: path,
                    move_target: None,
                    revision: record.revision,
                    updated_at: record.updated_at,
                },
            );
        }

        for (id, status, updated_at) in [
            ("active-old", TaskStatus::Received, 100),
            ("active-middle", TaskStatus::Running, 200),
            ("active-newest", TaskStatus::AwaitingConfirmation, 300),
        ] {
            let mut record = test_task(
                id,
                JobRequest::Youtube {
                    url: format!("https://example.invalid/{id}"),
                },
            );
            record.status = status;
            record.updated_at = updated_at;
            let path = queue
                .video
                .task_path(id)
                .expect("active task path should be valid");
            queue
                .video
                .write_record(&path, &record)
                .expect("active record should persist");
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
        }

        let mut pdf_index = queue.pdf.read_index().expect("PDF index should load");
        for (id, updated_at) in [("pdf-old", 150), ("pdf-newest", 400)] {
            let mut record = test_task(
                id,
                JobRequest::Pdf {
                    url: format!("https://example.invalid/{id}"),
                },
            );
            record.status = TaskStatus::Queued;
            record.updated_at = updated_at;
            let path = queue
                .pdf
                .task_path(id)
                .expect("PDF task path should be valid");
            queue
                .pdf
                .write_record(&path, &record)
                .expect("PDF record should persist");
            pdf_index.tasks.insert(
                id.to_string(),
                TaskIndexEntry {
                    chat_id: record.chat_id,
                    record_path: path,
                    move_target: None,
                    revision: record.revision,
                    updated_at: record.updated_at,
                },
            );
        }
        queue
            .pdf
            .write_index(&pdf_index)
            .expect("PDF index should persist");

        queue
            .video
            .write_index(&index)
            .expect("queue index should persist");

        let reads_before = provider.read_paths();
        let records = queue
            .active_records_for_chats(&[123_456_789], 2)
            .expect("bounded active summary should load");
        let reads_after = provider.read_paths();
        let summary_reads = &reads_after[reads_before.len()..];
        let record_reads = summary_reads
            .iter()
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("task-"))
            })
            .collect::<Vec<_>>();

        assert_eq!(
            records
                .iter()
                .map(|record| record.id.as_str())
                .collect::<Vec<_>>(),
            vec!["pdf-newest", "active-newest"]
        );
        assert_eq!(
            record_reads.len(),
            4,
            "each independent store opens at most the requested candidate count"
        );
        assert!(record_reads.iter().all(|path| {
            path.parent() == Some(queue.video.queue_dir.as_path())
                || path.parent() == Some(queue.pdf.queue_dir.as_path())
        }));
        assert_eq!(index.tasks.len(), HISTORY_COUNT + 3);
        assert_eq!(pdf_index.tasks.len(), 2);

        drop(queue);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn startup_reconciles_terminal_records_before_bounded_active_summary() {
        const STALE_TERMINAL_COUNT: usize = 120;

        let directory = temp_queue_root("startup-reconcile-terminal-active-index");
        let config = test_video_config(&directory);
        let provider = Arc::new(MockQueueFileProvider::default());
        let queue = QueueManager::open_with_file_provider(&config, provider.clone())
            .expect("queue should open");

        let mut index = queue.video.read_index().expect("queue index should load");
        for number in 0..STALE_TERMINAL_COUNT {
            let id = format!("stale-terminal-{number:03}");
            let mut record = test_task(
                &id,
                JobRequest::Youtube {
                    url: format!("https://example.invalid/{id}"),
                },
            );
            record.status = if number % 2 == 0 {
                TaskStatus::Completed
            } else {
                TaskStatus::Cancelled
            };
            record.revision = 2;
            record.updated_at = 30_000 + number as u64;
            let path = queue
                .video
                .task_path(&id)
                .expect("active task path should be valid");
            queue
                .video
                .write_record(&path, &record)
                .expect("terminal record should persist before its stale index update");
            index.tasks.insert(
                id,
                TaskIndexEntry {
                    chat_id: record.chat_id,
                    record_path: path,
                    move_target: None,
                    revision: 1,
                    // The index still reflects its older active state. These 120 entries sort
                    // ahead of the live task and would fill the summary's 100-record read cap.
                    updated_at: 20_000 + number as u64,
                },
            );
        }

        let mut active = test_task(
            "still-active-after-terminal-crash",
            JobRequest::Youtube {
                url: "https://example.invalid/still-active-after-terminal-crash".to_string(),
            },
        );
        active.status = TaskStatus::Queued;
        active.updated_at = 10_000;
        let active_path = queue
            .video
            .task_path(&active.id)
            .expect("active task path should be valid");
        queue
            .video
            .write_record(&active_path, &active)
            .expect("active task should persist");
        index.tasks.insert(
            active.id.clone(),
            TaskIndexEntry {
                chat_id: active.chat_id,
                record_path: active_path,
                move_target: None,
                revision: active.revision,
                updated_at: active.updated_at,
            },
        );
        queue
            .video
            .write_index(&index)
            .expect("crash-state index should persist");

        assert!(
            queue
                .active_records_for_chats(&[123_456_789], 1)
                .expect("pre-restart bounded summary should load")
                .is_empty(),
            "stale terminal entries reproduce the crash window by occupying the one-record cap"
        );
        drop(queue);

        let reopened = QueueManager::open_with_file_provider(&config, provider.clone())
            .expect("startup should reconcile the durable terminal records");
        let repaired = reopened
            .video
            .read_index()
            .expect("reconciled index should load");
        for number in 0..STALE_TERMINAL_COUNT {
            let id = format!("stale-terminal-{number:03}");
            let entry = repaired
                .tasks
                .get(&id)
                .expect("terminal task should remain available in history");
            assert_eq!(
                entry.record_path,
                reopened
                    .video
                    .terminal_history_path(&id)
                    .expect("history path should be valid")
            );
            assert_eq!(entry.move_target, None);
            assert_eq!(entry.revision, 2);
            assert_eq!(entry.updated_at, 30_000 + number as u64);
        }

        let reads_before_summary = provider.read_paths();
        let visible = reopened
            .active_records_for_chats(&[123_456_789], 1)
            .expect("repaired active summary should load");
        let reads_after_summary = provider.read_paths();
        let summary_reads = &reads_after_summary[reads_before_summary.len()..];
        let task_reads = summary_reads
            .iter()
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("task-"))
            })
            .collect::<Vec<_>>();

        assert_eq!(
            visible
                .iter()
                .map(|record| record.id.as_str())
                .collect::<Vec<_>>(),
            vec!["still-active-after-terminal-crash"]
        );
        assert_eq!(
            task_reads.len(),
            1,
            "summary should read only its one selected task"
        );
        assert!(task_reads.iter().all(|path| {
            path.parent() == Some(reopened.video.queue_dir.as_path())
                || path.parent() == Some(reopened.pdf.queue_dir.as_path())
        }));
        assert!(
            task_reads.iter().all(|path| {
                path.parent()
                    != Some(
                        reopened
                            .video
                            .queue_dir
                            .join(TERMINAL_HISTORY_DIRECTORY)
                            .as_path(),
                    )
            }),
            "bounded active summaries must not read terminal history records"
        );

        drop(reopened);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn active_chat_snapshot_keeps_updates_out_until_selection_finishes() {
        let directory = temp_queue_root("active-chat-snapshot-lock");
        let config = test_video_config(&directory);
        let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
        let task_id = "active-chat-snapshot-lock-task";
        assert!(
            queue
                .create(test_task(
                    task_id,
                    JobRequest::Youtube {
                        url: "https://www.youtube.com/watch?v=abcdefghijk".to_string(),
                    },
                ))
                .expect("task should persist")
        );

        let (snapshot_entered_tx, snapshot_entered_rx) = mpsc::sync_channel(0);
        let (release_snapshot_tx, release_snapshot_rx) = mpsc::sync_channel(0);
        let snapshot_queue = Arc::clone(&queue);
        let snapshot = std::thread::spawn(move || {
            snapshot_queue.active_records_for_chats_with_test_hook(&[123_456_789], 100, || {
                snapshot_entered_tx
                    .send(())
                    .expect("test should observe the held queue snapshot");
                release_snapshot_rx
                    .recv()
                    .expect("test should release the queue snapshot");
            })
        });

        let snapshot_reached_selection = snapshot_entered_rx.recv().is_ok();
        let (writer_lock_state_tx, writer_lock_state_rx) = mpsc::sync_channel(0);
        let writer_queue = Arc::clone(&queue);
        let writer = std::thread::spawn(move || {
            let selection_holds_lock = match writer_queue.operation_lock.try_lock() {
                Ok(guard) => {
                    drop(guard);
                    false
                }
                Err(std::sync::TryLockError::WouldBlock) => true,
                Err(std::sync::TryLockError::Poisoned(_)) => false,
            };
            let _ = writer_lock_state_tx.send(selection_holds_lock);
            writer_queue.set_status(task_id, TaskStatus::Queued, None)
        });
        let update_was_blocked = writer_lock_state_rx.recv().unwrap_or(false);

        // Always release and join both workers before asserting, so a failed lock assertion
        // cannot strand a thread at the deterministic barrier.
        let _ = release_snapshot_tx.send(());
        let snapshot_result = snapshot.join();
        let writer_result = writer.join();

        assert!(
            snapshot_reached_selection,
            "snapshot should reach selection"
        );
        assert!(
            update_was_blocked,
            "the queue update must wait while the same snapshot lock is held"
        );
        let captured = snapshot_result
            .expect("snapshot worker should join")
            .expect("active task snapshot should succeed");
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].status, TaskStatus::Received);
        let updated = writer_result
            .expect("queue update worker should join")
            .expect("queue update should succeed after selection releases the lock");
        assert_eq!(updated.status, TaskStatus::Queued);

        drop(queue);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn pending_destination_transfer_blocks_user_actions_without_mutating_records() {
        let (directory, _config, queue, target) = task_transfer_fixture("transfer-action-guards");
        let cases = [
            ("transfer-guard-failed", TaskStatus::Failed),
            ("transfer-guard-interrupted", TaskStatus::Interrupted),
            ("transfer-guard-queued", TaskStatus::Queued),
        ];

        for (index, (id, status)) in cases.into_iter().enumerate() {
            create_transfer_test_task(&queue, id, status);
            let transfer =
                begin_test_transfer(&queue, id, &format!("migration-guard-{index}"), &target);
            let before = queue
                .get(id)
                .expect("task should remain readable")
                .expect("task should remain present");
            let before_json = serde_json::to_value(&before).expect("task should serialize");

            match status {
                TaskStatus::Failed => assert!(
                    queue
                        .claim_resume_for_chat_if_generation(
                            id,
                            before.generation,
                            before.chat_id,
                            true
                        )
                        .is_err()
                ),
                TaskStatus::Interrupted => assert!(
                    queue
                        .claim_resume_for_chat_if_generation(
                            id,
                            before.generation,
                            before.chat_id,
                            false,
                        )
                        .is_err()
                ),
                TaskStatus::Queued => assert!(
                    queue
                        .begin_run_if_generation(id, before.generation)
                        .expect("run guard should not fail")
                        .is_none()
                ),
                _ => unreachable!("test cases cover failed, interrupted, and queued tasks"),
            }
            assert_eq!(
                serde_json::to_value(
                    queue
                        .get(id)
                        .expect("task should remain readable")
                        .expect("task should remain present")
                )
                .expect("task should serialize"),
                before_json,
                "blocked action must not mutate {status:?} task {id}"
            );

            assert!(
                queue
                    .cancel_for_chat_if_generation(id, before.generation, before.chat_id)
                    .is_err()
            );
            assert_eq!(
                serde_json::to_value(
                    queue
                        .get(id)
                        .expect("task should remain readable")
                        .expect("task should remain present")
                )
                .expect("task should serialize"),
                before_json,
                "blocked cancel must not mutate {status:?} task {id}"
            );

            assert!(
                queue
                    .abort_task_destination_transfer(&transfer)
                    .expect("transfer should abort")
                    .is_some()
            );
        }

        let id = "transfer-guard-confirmation";
        create_transfer_test_task(&queue, id, TaskStatus::AwaitingConfirmation);
        let transfer = begin_test_transfer(&queue, id, "migration-guard-confirm", &target);
        let before = queue.get(id).unwrap().unwrap();
        let before_json = serde_json::to_value(&before).unwrap();
        assert!(
            queue
                .accept_proposed_plan_for_chat_if_generation(id, before.generation, before.chat_id)
                .is_err()
        );
        assert_eq!(
            serde_json::to_value(queue.get(id).unwrap().unwrap()).unwrap(),
            before_json,
            "blocked confirmation must leave the pending task unchanged"
        );
        queue.abort_task_destination_transfer(&transfer).unwrap();

        drop(queue);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn task_transfer_abort_and_ack_restore_prior_task_states() {
        let (directory, _config, queue, target) = task_transfer_fixture("transfer-restoration");
        let statuses = [
            TaskStatus::Queued,
            TaskStatus::Preparing,
            TaskStatus::Running,
            TaskStatus::Verifying,
            TaskStatus::Failed,
            TaskStatus::Interrupted,
        ];

        for (index, status) in statuses.into_iter().enumerate() {
            let abort_id = format!("transfer-abort-{index}");
            let previous = create_transfer_test_task(&queue, &abort_id, status);
            let token = begin_test_transfer(
                &queue,
                &abort_id,
                &format!("migration-abort-{index}"),
                &target,
            );
            let paused = queue.get(&abort_id).unwrap().unwrap();
            if matches!(
                status,
                TaskStatus::Preparing | TaskStatus::Running | TaskStatus::Verifying
            ) {
                assert_eq!(paused.status, TaskStatus::Interrupted);
                assert_eq!(paused.generation, previous.generation + 1);
                assert_eq!(token.pause_generation, Some(previous.generation));
            } else {
                assert_eq!(paused.status, status);
                assert_eq!(token.pause_generation, None);
            }
            let aborted = queue
                .abort_task_destination_transfer(&token)
                .unwrap()
                .unwrap();
            let restored_status = if matches!(
                status,
                TaskStatus::Preparing | TaskStatus::Running | TaskStatus::Verifying
            ) {
                TaskStatus::Queued
            } else {
                status
            };
            assert_eq!(aborted.status, restored_status);
            assert_eq!(aborted.error, previous.error);
            assert!(aborted.destination_transfer.is_none());

            let ack_id = format!("transfer-ack-{index}");
            let previous = create_transfer_test_task(&queue, &ack_id, status);
            let token =
                begin_test_transfer(&queue, &ack_id, &format!("migration-ack-{index}"), &target);
            let plan = queue
                .task_transfer_plan(&token)
                .expect("empty task staging should produce a transfer plan");
            queue
                .commit_task_destination_transfer(&plan)
                .expect("target-first queue handoff should install the task");
            let acknowledged = queue
                .ack_task_destination_transfer(&plan.transfer)
                .expect("transfer ACK should persist")
                .expect("target task should remain present");
            assert_eq!(acknowledged.status, restored_status);
            assert_eq!(acknowledged.error, previous.error);
            assert_eq!(acknowledged.destination.as_ref(), Some(&target));
            assert!(acknowledged.destination_transfer.is_none());
            assert_eq!(
                acknowledged.destination_transfer_ack.as_ref(),
                Some(&plan.transfer),
                "queue ACK must retain an exact receipt until source cleanup completes"
            );
            let acknowledged_json =
                serde_json::to_value(&acknowledged).expect("acknowledged task should serialize");
            match restored_status {
                TaskStatus::Failed => assert!(
                    queue
                        .claim_resume_for_chat_if_generation(
                            &ack_id,
                            acknowledged.generation,
                            acknowledged.chat_id,
                            true,
                        )
                        .is_err()
                ),
                TaskStatus::Interrupted => assert!(
                    queue
                        .claim_resume_for_chat_if_generation(
                            &ack_id,
                            acknowledged.generation,
                            acknowledged.chat_id,
                            false,
                        )
                        .is_err()
                ),
                TaskStatus::Queued => assert!(
                    queue
                        .begin_run_if_generation(&ack_id, acknowledged.generation)
                        .expect("run guard should not fail")
                        .is_none()
                ),
                _ => unreachable!("transfer test restores to an actionable or manual state"),
            }
            assert!(
                queue
                    .cancel_for_chat_if_generation(
                        &ack_id,
                        acknowledged.generation,
                        acknowledged.chat_id,
                    )
                    .is_err()
            );
            assert_eq!(
                serde_json::to_value(queue.get(&ack_id).unwrap().unwrap()).unwrap(),
                acknowledged_json,
                "queued dispatch, resume, and cancel must stay blocked until source cleanup"
            );
            assert!(
                queue
                    .begin_task_destination_transfer(
                        &ack_id,
                        "migration-after-queue-ack",
                        acknowledged.generation,
                        acknowledged.revision,
                        target.clone(),
                    )
                    .expect_err("a new transfer must not overwrite an unconsumed ACK receipt")
                    .to_string()
                    .contains("finalization")
            );
            assert_eq!(
                queue
                    .ack_task_destination_transfer(&plan.transfer)
                    .expect("exact ACK replay should be idempotent")
                    .as_ref()
                    .and_then(|record| record.destination_transfer_ack.as_ref()),
                Some(&plan.transfer)
            );
            queue
                .clear_task_destination_transfer_ack(&plan.transfer)
                .expect("completed source cleanup should allow exact receipt consumption");
            assert!(
                queue
                    .get(&ack_id)
                    .unwrap()
                    .unwrap()
                    .destination_transfer_ack
                    .is_none()
            );
        }

        drop(queue);
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn cancellation_registration_removal_wakes_transfer_waiter() {
        let (directory, _config, queue, target) =
            task_transfer_fixture("transfer-cancellation-wakeup");
        let queue = Arc::new(queue);
        let id = "transfer-cancellation-wakeup-task";
        create_transfer_test_task(&queue, id, TaskStatus::Queued);
        let (record, notify) = queue
            .register_cancellation_if_queued_generation(id, 0)
            .expect("cancellation registration should be checked")
            .expect("queued task should register its worker");
        let transfer = queue
            .begin_task_destination_transfer(
                id,
                "migration-cancellation-wakeup",
                record.generation,
                record.revision,
                target,
            )
            .expect("transfer should request an active worker pause");
        assert_eq!(transfer.pause_generation, Some(record.generation));

        let wait_queue = Arc::clone(&queue);
        let wait_transfer = transfer.clone();
        let waiter = tokio::spawn(async move {
            wait_queue
                .wait_for_task_destination_transfer(&wait_transfer)
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "waiter should observe the active writer"
        );
        queue
            .unregister_cancellation_if_current(id, &notify)
            .expect("worker completion should unregister its token");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("wait should wake after cancellation acknowledgement")
            .expect("wait task should join")
            .expect("transfer should settle after writer unregisters");

        queue.abort_task_destination_transfer(&transfer).unwrap();
        drop(queue);
        let _ = fs::remove_dir_all(directory);
    }

    fn install_target_record_for_transfer_replay(
        queue: &QueueManager,
        plan: &TaskTransferPlan,
        mismatch_transfer_id: bool,
    ) {
        let source = queue
            .store_for_root(&plan.source_root)
            .expect("source lookup should succeed")
            .expect("source store should be available");
        let target = queue
            .store_for_root(&plan.target_root)
            .expect("target lookup should succeed")
            .expect("target store should be available");
        let (source_record, _) = source
            .get_task(&plan.transfer.task_id)
            .unwrap()
            .expect("source task should remain installed");
        let mut target_record = source_record.clone();
        rebase_task_paths(
            &mut target_record,
            &plan.source_root,
            &plan.target_root,
            &plan.published_path_rewrites,
        )
        .expect("target record paths should rebase");
        target_record.destination = Some(plan.transfer.target.clone());
        target_record.destination_transfer_phase =
            Some(TaskDestinationTransferPhase::TargetInstalled);
        if mismatch_transfer_id {
            target_record
                .destination_transfer
                .as_mut()
                .expect("source task should contain its transfer")
                .transfer_id
                .push_str("-mismatch");
        }
        target_record.revision = source_record.revision.saturating_add(1);
        target_record.activity_revision = 0;
        target
            .create_task(&mut target_record)
            .expect("simulated target record should persist");
    }

    #[test]
    fn startup_reconciles_only_exact_dual_store_transfer_records() {
        let (directory, config, queue, target) =
            task_transfer_fixture("transfer-startup-exact-reconcile");
        let id = "transfer-startup-exact-task";
        create_transfer_test_task(&queue, id, TaskStatus::Queued);
        let token = begin_test_transfer(&queue, id, "migration-startup-exact", &target);
        let plan = queue.task_transfer_plan(&token).unwrap();
        install_target_record_for_transfer_replay(&queue, &plan, false);
        drop(queue);

        let reopened = QueueManager::open(&config)
            .expect("startup should finish an exact target-installed handoff");
        assert!(
            reopened
                .store_for_root(&token.source.root)
                .unwrap()
                .unwrap()
                .get_task(id)
                .unwrap()
                .is_none()
        );
        let installed = reopened
            .store_for_root(&token.target.root)
            .unwrap()
            .unwrap()
            .get_task(id)
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(installed.destination.as_ref(), Some(&target));
        assert_eq!(
            installed.destination_transfer.as_ref(),
            Some(&plan.transfer)
        );
        drop(reopened);
        let _ = fs::remove_dir_all(directory);

        let (directory, config, queue, target) =
            task_transfer_fixture("transfer-startup-mismatch-reconcile");
        let id = "transfer-startup-mismatch-task";
        create_transfer_test_task(&queue, id, TaskStatus::Queued);
        let token = begin_test_transfer(&queue, id, "migration-startup-mismatch", &target);
        let plan = queue.task_transfer_plan(&token).unwrap();
        install_target_record_for_transfer_replay(&queue, &plan, true);
        drop(queue);

        let error = match QueueManager::open(&config) {
            Ok(reopened) => {
                drop(reopened);
                panic!("startup must reject a target record with a different transfer token");
            }
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("conflicting record"));
        let source_record: TaskRecord = serde_json::from_slice(
            &fs::read(
                token
                    .source
                    .root
                    .join(QUEUE_DIRECTORY)
                    .join(format!("task-{id}.json")),
            )
            .expect("source record should remain after mismatch rejection"),
        )
        .expect("source record should parse");
        let target_record: TaskRecord = serde_json::from_slice(
            &fs::read(
                token
                    .target
                    .root
                    .join(QUEUE_DIRECTORY)
                    .join(format!("task-{id}.json")),
            )
            .expect("mismatched target record should remain for recovery"),
        )
        .expect("mismatched target record should parse");
        assert_eq!(
            source_record.destination_transfer.as_ref(),
            Some(&plan.transfer)
        );
        assert_ne!(
            target_record.destination_transfer.as_ref(),
            Some(&plan.transfer),
            "startup must never treat a mismatched target token as installed"
        );
        let _ = fs::remove_dir_all(directory);
    }
}
