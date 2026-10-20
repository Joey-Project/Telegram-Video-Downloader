//! Durable, checkpointed moves between registered media libraries.
//!
//! A migration is intentionally a sequence of per-file checkpoints rather than an atomic batch:
//! each copy is staged and verified before publication, every member of a media bundle is
//! published before source cleanup starts, and the primary media file is removed last.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::file_manager::{
    LibraryManager, MigrationPlanOutcome, MigrationPlannedFile, MigrationPlannedGroup,
};
use crate::library::{ConflictPolicy, LibrarySnapshot, Presence};
use crate::media_libraries::{TaskTransferEntryKind, TaskTransferFile, TaskTransferPlan};
use crate::safe_fs::{BoundFileAccessPolicy, EntryIdentity, RootedFs};

const PREVIEW_STATE_PREFIX: &str = "migration-preview-";
const JOB_STATE_PREFIX: &str = "migration-job-";
const STATE_SUFFIX: &str = ".json";
const MAX_MIGRATION_FILES: usize = 100_000;
const MAX_TASK_IMPACTS: usize = 10_000;
const COPY_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
const MAX_STATE_BYTES: usize = 64 * 1024 * 1024;
const PREVIEW_REVISION: &str = "1";
static MIGRATION_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MigrationScope {
    Library,
    Selected,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationTaskImpact {
    pub(crate) task_id: String,
    pub(crate) generation: u64,
    pub(crate) revision: u64,
    pub(crate) staging_bytes: u64,
    pub(crate) staging_file_count: usize,
    #[serde(default)]
    pub(crate) published_item_ids: Vec<String>,
}

/// All values that affect the selected migration are frozen in the durable preview.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationRequest {
    pub(crate) scope: MigrationScope,
    #[serde(default)]
    pub(crate) item_ids: Vec<String>,
    pub(crate) conflict: ConflictPolicy,
    #[serde(default)]
    pub(crate) item_conflicts: BTreeMap<String, ConflictPolicy>,
    #[serde(default)]
    pub(crate) include_incomplete_tasks: bool,
    #[serde(default)]
    pub(crate) set_target_default_on_success: bool,
    #[serde(default)]
    pub(crate) task_impacts: Vec<MigrationTaskImpact>,
    #[serde(default)]
    pub(crate) warnings: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MigrationControl {
    Pause,
    Resume,
    StopRemaining,
    RetryFailed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MigrationRunStatus {
    Queued,
    Running,
    Paused,
    Stopped,
    Completed,
    Partial,
    Failed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MigrationItemPhase {
    Copying,
    Verifying,
    Publishing,
    CleaningSource,
    Complete,
    Skipped,
    Failed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MigrationFailureKind {
    SourceChanged,
    SourceUnavailable,
    TargetChanged,
    TargetConflict,
    InsufficientSpace,
    CopyFailed,
    VerificationFailed,
    PublicationFailed,
    CleanupFailed,
    TaskTransferFailed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationPreviewFile {
    pub(crate) source_path: String,
    pub(crate) target_path: String,
    pub(crate) size_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationPreviewItem {
    pub(crate) item_id: String,
    pub(crate) status: MigrationItemPhase,
    pub(crate) conflict: bool,
    pub(crate) source_path: String,
    pub(crate) target_path: String,
    pub(crate) bytes: u64,
    pub(crate) files: Vec<MigrationPreviewFile>,
}

/// Stable external preview contract. Keep `id` and the string `revision` names for cloud
/// confirmation receipts; request options are echoed so the UI can show exactly what is bound.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationPreview {
    pub(crate) id: String,
    pub(crate) revision: String,
    pub(crate) source_library_id: String,
    pub(crate) target_library_id: String,
    pub(crate) scope: MigrationScope,
    pub(crate) item_ids: Vec<String>,
    pub(crate) conflict: ConflictPolicy,
    #[serde(default)]
    pub(crate) item_conflicts: BTreeMap<String, ConflictPolicy>,
    pub(crate) include_incomplete_tasks: bool,
    pub(crate) set_target_default_on_success: bool,
    pub(crate) item_count: usize,
    pub(crate) file_count: usize,
    pub(crate) total_bytes: u64,
    pub(crate) required_space_bytes: u64,
    pub(crate) available_space_bytes: u64,
    pub(crate) conflict_count: usize,
    pub(crate) incomplete_task_count: usize,
    pub(crate) staging_bytes: u64,
    pub(crate) warnings: Vec<String>,
    pub(crate) items: Vec<MigrationPreviewItem>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationFileStatus {
    pub(crate) source_path: String,
    pub(crate) target_path: String,
    pub(crate) size_bytes: u64,
    pub(crate) status: MigrationItemPhase,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationStatusItem {
    pub(crate) item_id: String,
    pub(crate) status: MigrationItemPhase,
    pub(crate) source_path: String,
    pub(crate) target_path: String,
    pub(crate) bytes: u64,
    #[serde(default)]
    pub(crate) error: Option<String>,
    #[serde(default)]
    pub(crate) failure_kind: Option<MigrationFailureKind>,
    pub(crate) files: Vec<MigrationFileStatus>,
}

/// Runtime status never includes serialized task plans, queue tokens, or Telegram identifiers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationStatus {
    pub(crate) id: String,
    pub(crate) preview_id: String,
    pub(crate) revision: String,
    pub(crate) status: MigrationRunStatus,
    pub(crate) source_library_id: String,
    pub(crate) target_library_id: String,
    pub(crate) scope: MigrationScope,
    pub(crate) conflict: ConflictPolicy,
    #[serde(default)]
    pub(crate) item_conflicts: BTreeMap<String, ConflictPolicy>,
    pub(crate) include_incomplete_tasks: bool,
    pub(crate) set_target_default_on_success: bool,
    pub(crate) default_update_applied: bool,
    pub(crate) total_items: usize,
    pub(crate) completed_items: usize,
    pub(crate) success_count: usize,
    pub(crate) skipped_count: usize,
    pub(crate) failed_count: usize,
    pub(crate) total_files: usize,
    pub(crate) completed_files: usize,
    pub(crate) copied_bytes: u64,
    pub(crate) total_bytes: u64,
    pub(crate) required_space_bytes: u64,
    pub(crate) available_space_bytes: u64,
    pub(crate) incomplete_task_count: usize,
    pub(crate) staging_bytes: u64,
    pub(crate) awaiting_task_plans: bool,
    pub(crate) awaiting_task_commit: bool,
    pub(crate) warnings: Vec<String>,
    pub(crate) items: Vec<MigrationStatusItem>,
    pub(crate) task_transfers: Vec<MigrationTaskTransferStatus>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MigrationDeliveryState {
    Pending,
    Sent,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationRuntimeContext {
    pub(crate) database_epoch: String,
    pub(crate) preview_seq: u64,
    pub(crate) chat_id: Option<i64>,
    pub(crate) message_id: Option<i64>,
    pub(crate) delivery: MigrationDeliveryState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationTaskTransferStatus {
    pub(crate) task_id: String,
    pub(crate) transfer_id: String,
    pub(crate) completed_files: usize,
    pub(crate) total_files: usize,
    pub(crate) copy_complete: bool,
    pub(crate) commit_acked: bool,
    pub(crate) cleaned_source_files: usize,
    pub(crate) cleanup_total_files: usize,
    pub(crate) source_cleanup_complete: bool,
    pub(crate) error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PersistedMigrationBindings {
    pub(crate) source_library_id: String,
    pub(crate) source_root: PathBuf,
    pub(crate) source_device: u64,
    pub(crate) source_inode: u64,
    pub(crate) target_library_id: String,
    pub(crate) target_root: PathBuf,
    pub(crate) target_device: u64,
    pub(crate) target_inode: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PersistedMigrationPreview {
    pub(crate) preview: MigrationPreview,
    pub(crate) bindings: PersistedMigrationBindings,
}

#[derive(Clone)]
pub(crate) struct MigrationEngine {
    source_library_id: String,
    source: Arc<LibraryManager>,
    target_library_id: String,
    target: Arc<LibraryManager>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RootBinding {
    library_id: String,
    root: PathBuf,
    device: u64,
    inode: u64,
}

fn task_plan_root_matches_binding(
    plan_root: &Path,
    frozen_destination_root: &Path,
    binding: &RootBinding,
) -> Result<bool> {
    if plan_root != frozen_destination_root {
        return Ok(false);
    }
    let root = RootedFs::new(plan_root)?;
    root.validate_configured_root()?;
    let identity = root.root_identity();
    Ok(root.root_path() == binding.root
        && identity.device() == binding.device
        && identity.inode() == binding.inode)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PreviewDocument {
    format_version: u32,
    created_at: u64,
    source: RootBinding,
    target: RootBinding,
    request: MigrationRequest,
    preview: MigrationPreview,
    plan: Vec<MigrationPlannedGroup>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct JobDocument {
    format_version: u32,
    created_at: u64,
    source: RootBinding,
    target: RootBinding,
    preview: MigrationPreview,
    request: MigrationRequest,
    status: MigrationRunStatus,
    groups: Vec<GroupCheckpoint>,
    runtime_context: Option<MigrationRuntimeContext>,
    #[serde(default)]
    awaiting_task_commit: bool,
    #[serde(default)]
    execution_revalidated: bool,
    #[serde(default)]
    task_transfers: Vec<TaskTransferCheckpoint>,
    #[serde(default)]
    task_commit_acked: Vec<String>,
    #[serde(default)]
    default_update_applied: bool,
    #[serde(default)]
    runtime_warnings: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct GroupCheckpoint {
    plan: MigrationPlannedGroup,
    status: MigrationItemPhase,
    files: Vec<FileCheckpoint>,
    error: Option<String>,
    failure_kind: Option<MigrationFailureKind>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FileCheckpoint {
    plan: MigrationPlannedFile,
    staging_name: String,
    #[serde(default)]
    staging_attempt: u32,
    staging_identity: Option<SerializedIdentity>,
    copied_bytes: u64,
    #[serde(default)]
    chunk_sha256: Vec<String>,
    verified: bool,
    #[serde(default)]
    verified_chunks: usize,
    published: bool,
    #[serde(default)]
    target_identity: Option<SerializedIdentity>,
    #[serde(default)]
    target_access_policy: Option<SerializedAccessPolicy>,
    source_removed: bool,
    #[serde(default)]
    source_cleanup_intent: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TaskTransferCheckpoint {
    plan: TaskTransferPlan,
    entries: Vec<TaskFileCheckpoint>,
    copy_complete: bool,
    error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TaskFileCheckpoint {
    staging_attempt: u32,
    staging_identity: Option<SerializedIdentity>,
    target_identity: Option<SerializedIdentity>,
    copied_bytes: u64,
    #[serde(default)]
    source_verified_chunks: usize,
    verified_chunks: usize,
    #[serde(default)]
    target_verified_chunks: usize,
    #[serde(default)]
    directory_intent: bool,
    published: bool,
    #[serde(default)]
    source_cleanup_intent: bool,
    #[serde(default)]
    source_removed: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SerializedIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SerializedAccessPolicy {
    owner: u32,
    group: u32,
    mode: u32,
    link_count: u64,
}

impl MigrationEngine {
    /// Pauses a durable job using only its source library. This keeps an interrupted job safe
    /// when its target is offline or cannot be reopened during recovery.
    pub(crate) fn pause_for_source(
        source: &LibraryManager,
        migration_id: &str,
        warning: &str,
    ) -> Result<(MigrationStatus, Option<MigrationRuntimeContext>)> {
        validate_migration_id(migration_id)?;
        let _state_lock = source.migration_state_lock()?;
        let bytes = source
            .migration_read_state(&job_state_name(migration_id), MAX_STATE_BYTES)?
            .ok_or_else(|| anyhow!("migration job does not exist in this source library"))?;
        let mut job: JobDocument = decode_state(&bytes, "migration job")?;
        let identity = source.migration_root_identity();
        if job.preview.id != migration_id
            || job.source.library_id.is_empty()
            || job.source.root != source.migration_root_path()
            || job.source.device != identity.device()
            || job.source.inode != identity.inode()
        {
            bail!("persisted migration source binding no longer matches this library");
        }
        if matches!(
            job.status,
            MigrationRunStatus::Queued | MigrationRunStatus::Running
        ) {
            job.status = MigrationRunStatus::Paused;
        }
        record_runtime_warning(&mut job, warning);
        source.migration_write_state(&job_state_name(migration_id), &encode_state(&job)?)?;
        let context = job.runtime_context.clone();
        Ok((status_from_job(&job), context))
    }

    pub(crate) fn new(
        source_library_id: impl Into<String>,
        source: Arc<LibraryManager>,
        target_library_id: impl Into<String>,
        target: Arc<LibraryManager>,
    ) -> Result<Self> {
        let source_library_id = source_library_id.into();
        let target_library_id = target_library_id.into();
        if source_library_id.is_empty() || target_library_id.is_empty() {
            bail!("migration requires registered source and target library IDs");
        }
        let source_root = source.migration_root_identity();
        let target_root = target.migration_root_identity();
        if source.migration_root_path() == target.migration_root_path()
            || (source_root.device() == target_root.device()
                && source_root.inode() == target_root.inode())
        {
            bail!("source and target media libraries refer to the same root");
        }
        Ok(Self {
            source_library_id,
            source,
            target_library_id,
            target,
        })
    }

    pub(crate) fn preview(&self, request: &MigrationRequest) -> Result<MigrationPreview> {
        validate_request(request)?;
        let snapshot = self.source.migration_scan()?;
        let item_ids = selected_item_ids(request, &snapshot)?;
        let selected = item_ids.iter().map(String::as_str).collect::<HashSet<_>>();
        if request
            .item_conflicts
            .keys()
            .any(|id| !selected.contains(id.as_str()))
        {
            bail!("a conflict override refers to media outside the selected migration scope");
        }
        let plan = if item_ids.is_empty() {
            Vec::new()
        } else {
            self.source.plan_migration_groups(
                &self.target,
                &snapshot,
                &item_ids,
                request.conflict,
                &request.item_conflicts,
            )?
        };
        let (preview, plan) = self.make_preview(request, item_ids, plan)?;
        let document = PreviewDocument {
            format_version: 1,
            created_at: unix_seconds(),
            source: self.source_binding(),
            target: self.target_binding(),
            request: request.clone(),
            preview: preview.clone(),
            plan,
        };
        let bytes = encode_state(&document)?;
        self.source
            .migration_write_state(&preview_state_name(&preview.id), &bytes)?;
        Ok(preview)
    }

    /// Confirms a preview by persisting a queued manifest. It performs no media copy, so the
    /// cloud dispatcher can return quickly and let a background loop call `step`.
    pub(crate) fn confirm(&self, preview_id: &str, revision: &str) -> Result<MigrationStatus> {
        let _state_lock = self.source.migration_state_lock()?;
        let job_name = job_state_name(preview_id);
        if let Some(bytes) = self
            .source
            .migration_read_state(&job_name, MAX_STATE_BYTES)?
        {
            let job: JobDocument = decode_state(&bytes, "migration job")?;
            self.validate_job_bindings(&job)?;
            if job.preview.id != preview_id || job.preview.revision != revision {
                bail!("migration preview ID or revision does not match confirmation");
            }
            return Ok(status_from_job(&job));
        }
        let bytes = self
            .source
            .migration_read_state(&preview_state_name(preview_id), MAX_STATE_BYTES)?
            .ok_or_else(|| anyhow!("migration preview expired or does not exist"))?;
        let document: PreviewDocument = decode_state(&bytes, "migration preview")?;
        if document.preview.id != preview_id || document.preview.revision != revision {
            bail!("migration preview ID or revision does not match confirmation");
        }
        self.validate_preview_bindings(&document)?;
        if document.created_at.saturating_add(24 * 60 * 60) < unix_seconds() {
            bail!("migration preview expired; create a fresh preview");
        }

        let groups = document
            .plan
            .iter()
            .cloned()
            .enumerate()
            .map(|(group_index, plan)| {
                let status = match plan.outcome {
                    MigrationPlanOutcome::Move => MigrationItemPhase::Copying,
                    MigrationPlanOutcome::Skip => MigrationItemPhase::Skipped,
                };
                let files = plan
                    .files
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(index, file)| FileCheckpoint {
                        staging_name: format!(
                            "migration-stage-{}-{}-{index}",
                            group_index,
                            compact_id(&document.preview.id)
                        ),
                        staging_attempt: 0,
                        plan: file,
                        staging_identity: None,
                        copied_bytes: 0,
                        chunk_sha256: Vec::new(),
                        verified: false,
                        verified_chunks: 0,
                        published: false,
                        target_identity: None,
                        target_access_policy: None,
                        source_removed: false,
                        source_cleanup_intent: false,
                    })
                    .collect();
                GroupCheckpoint {
                    plan,
                    status,
                    files,
                    error: None,
                    failure_kind: None,
                }
            })
            .collect();
        let mut job = JobDocument {
            format_version: 1,
            created_at: unix_seconds(),
            source: document.source,
            target: document.target,
            preview: document.preview,
            request: document.request,
            status: MigrationRunStatus::Queued,
            groups,
            runtime_context: None,
            awaiting_task_commit: false,
            execution_revalidated: false,
            task_transfers: Vec::new(),
            task_commit_acked: Vec::new(),
            default_update_applied: false,
            runtime_warnings: Vec::new(),
        };
        let status = status_from_job(&job);
        job.status = status.status;
        self.save_job(&job)?;
        Ok(status)
    }

    /// Performs at most one copy chunk, verification, publication, or source cleanup checkpoint.
    #[cfg(test)]
    pub(crate) fn step(&self, migration_id: &str) -> Result<MigrationStatus> {
        self.step_inner(migration_id, None)
    }

    pub(crate) fn step_with_output_lease(
        &self,
        migration_id: &str,
        lease: &crate::downloader::VideoMigrationGuard,
    ) -> Result<MigrationStatus> {
        self.source.migration_validate_output_lease(lease)?;
        self.target.migration_validate_output_lease(lease)?;
        self.step_inner(migration_id, Some(lease))
    }

    fn step_inner(
        &self,
        migration_id: &str,
        output_lease: Option<&crate::downloader::VideoMigrationGuard>,
    ) -> Result<MigrationStatus> {
        let needs_revalidation = {
            let _state_lock = self.source.migration_state_lock()?;
            let job = self.load_job(migration_id)?;
            self.validate_job_bindings(&job)?;
            if is_job_held_or_terminal(job.status) {
                return Ok(status_from_job(&job));
            }
            !job.execution_revalidated
        };
        if needs_revalidation {
            // Keep the state lock free during discovery so pause/stop can take effect while the
            // source library is being scanned. A second locked read below closes that race.
            let job = self.load_job(migration_id)?;
            self.validate_job_bindings(&job)?;
            self.revalidate_job(&job, output_lease)?;
            let _state_lock = self.source.migration_state_lock()?;
            let mut current = self.load_job(migration_id)?;
            self.validate_job_bindings(&current)?;
            if is_job_held_or_terminal(current.status) {
                return Ok(status_from_job(&current));
            }
            current.execution_revalidated = true;
            self.save_job(&current)?;
        }

        let _state_lock = self.source.migration_state_lock()?;
        let mut job = self.load_job(migration_id)?;
        self.validate_job_bindings(&job)?;
        if is_job_held_or_terminal(job.status) {
            return Ok(status_from_job(&job));
        }
        if job.request.include_incomplete_tasks && !all_task_plans_attached(&job) {
            job.status = MigrationRunStatus::Queued;
            self.save_job(&job)?;
            return Ok(status_from_job(&job));
        }
        if job.status == MigrationRunStatus::Queued {
            job.status = MigrationRunStatus::Running;
        }

        // Publish the selected media and every sidecar before queue destinations are changed.
        // Keep all sources until the queue has durably acknowledged its target record.
        let selected_publish_group = job.groups.iter().position(|group| {
            matches!(
                group.status,
                MigrationItemPhase::Copying
                    | MigrationItemPhase::Verifying
                    | MigrationItemPhase::Publishing
            ) || (!job.request.include_incomplete_tasks
                && group.status == MigrationItemPhase::CleaningSource)
        });
        if let Some(index) = selected_publish_group {
            let result = self.step_group(
                &mut job.groups[index],
                !job.request.include_incomplete_tasks,
            );
            if let Err(error) = result {
                let group = &mut job.groups[index];
                group.status = MigrationItemPhase::Failed;
                group.failure_kind = Some(classify_failure(&error));
                group.error = Some(format!("{error:#}"));
            }
            self.save_job(&job)?;
            return Ok(status_from_job(&job));
        }

        if job.request.include_incomplete_tasks {
            let media_ready = job.groups.iter().all(|group| {
                matches!(
                    group.status,
                    MigrationItemPhase::CleaningSource
                        | MigrationItemPhase::Complete
                        | MigrationItemPhase::Skipped
                )
            });
            if !media_ready {
                job.status = MigrationRunStatus::Failed;
                self.save_job(&job)?;
                return Ok(status_from_job(&job));
            }
            if !all_task_plans_attached(&job) {
                // Queue orchestration captures the exact quiesced task plans after confirmation.
                // Published media remains available at both roots until that handoff is ready.
                job.status = MigrationRunStatus::Queued;
                self.save_job(&job)?;
                return Ok(status_from_job(&job));
            }
            if !all_task_transfers_acked(&job) {
                if job.awaiting_task_commit {
                    return Ok(status_from_job(&job));
                }
                if let Some(index) = job
                    .task_transfers
                    .iter()
                    .position(|transfer| !transfer.copy_complete && transfer.error.is_none())
                {
                    job.status = MigrationRunStatus::Running;
                    if let Err(error) = self.step_task_transfer(&mut job, index) {
                        let checkpoint = &mut job.task_transfers[index];
                        checkpoint.error = Some(format!("{error:#}"));
                        job.status = MigrationRunStatus::Failed;
                    }
                    if job
                        .task_transfers
                        .iter()
                        .all(|transfer| transfer.copy_complete)
                    {
                        job.awaiting_task_commit = true;
                    }
                    self.save_job(&job)?;
                    return Ok(status_from_job(&job));
                }
                if job
                    .task_transfers
                    .iter()
                    .all(|transfer| transfer.copy_complete)
                {
                    job.awaiting_task_commit = true;
                    job.status = MigrationRunStatus::Running;
                } else {
                    job.status = MigrationRunStatus::Failed;
                }
                self.save_job(&job)?;
                return Ok(status_from_job(&job));
            }
        }

        if job.request.include_incomplete_tasks && all_task_transfers_acked(&job) {
            let pending_cleanup = job
                .task_transfers
                .iter()
                .enumerate()
                .filter(|(_, transfer)| {
                    job.task_commit_acked
                        .iter()
                        .any(|id| id == &transfer.plan.transfer.transfer_id)
                        && transfer.error.is_none()
                })
                .find_map(|(task_index, transfer)| {
                    transfer
                        .plan
                        .files
                        .iter()
                        .zip(&transfer.entries)
                        .position(|(file, entry)| {
                            file.kind == TaskTransferEntryKind::File && !entry.source_removed
                        })
                        .map(|file_index| (task_index, file_index))
                });
            if let Some((task_index, file_index)) = pending_cleanup {
                job.status = MigrationRunStatus::Running;
                if let Err(error) = self.step_task_payload_cleanup(&mut job, task_index, file_index)
                {
                    let transfer = &mut job.task_transfers[task_index];
                    transfer.error = Some(format!("{error:#}"));
                    job.status = MigrationRunStatus::Partial;
                }
                self.save_job(&job)?;
                return Ok(status_from_job(&job));
            }
            if !all_task_payloads_removed(&job) {
                // A cleanup failure remains reviewable/retryable; do not clean media sources or
                // claim terminal success while acknowledged task payloads remain behind.
                job.status = MigrationRunStatus::Partial;
                self.save_job(&job)?;
                return Ok(status_from_job(&job));
            }
        }

        let allow_media_cleanup = !job.request.include_incomplete_tasks
            || (all_task_transfers_acked(&job) && all_task_payloads_removed(&job));
        if allow_media_cleanup
            && let Some(index) = job
                .groups
                .iter()
                .position(|group| group.status == MigrationItemPhase::CleaningSource)
            && let Err(error) = self.step_group(&mut job.groups[index], true)
        {
            let group = &mut job.groups[index];
            group.status = MigrationItemPhase::Failed;
            group.failure_kind = Some(classify_failure(&error));
            group.error = Some(format!("{error:#}"));
        }
        if job.groups.iter().all(is_group_terminal)
            && (!job.request.include_incomplete_tasks
                || (all_task_transfers_acked(&job) && all_task_payloads_removed(&job)))
        {
            let failed = job
                .groups
                .iter()
                .any(|group| group.status == MigrationItemPhase::Failed);
            let any_complete = job.groups.iter().any(|group| {
                matches!(
                    group.status,
                    MigrationItemPhase::Complete | MigrationItemPhase::Skipped
                )
            });
            job.status = if failed && any_complete {
                MigrationRunStatus::Partial
            } else if failed {
                MigrationRunStatus::Failed
            } else {
                MigrationRunStatus::Completed
            };
        }
        self.save_job(&job)?;
        Ok(status_from_job(&job))
    }

    pub(crate) fn control(
        &self,
        migration_id: &str,
        control: MigrationControl,
    ) -> Result<MigrationStatus> {
        let _state_lock = self.source.migration_state_lock()?;
        let mut job = self.load_job(migration_id)?;
        self.validate_job_bindings(&job)?;
        match control {
            MigrationControl::Pause => {
                if matches!(
                    job.status,
                    MigrationRunStatus::Queued | MigrationRunStatus::Running
                ) {
                    job.status = MigrationRunStatus::Paused;
                }
            }
            MigrationControl::Resume => {
                if job.status == MigrationRunStatus::Paused {
                    job.status = MigrationRunStatus::Queued;
                }
            }
            MigrationControl::StopRemaining => {
                if matches!(
                    job.status,
                    MigrationRunStatus::Queued
                        | MigrationRunStatus::Running
                        | MigrationRunStatus::Paused
                        | MigrationRunStatus::Partial
                        | MigrationRunStatus::Failed
                ) {
                    for group in &mut job.groups {
                        if !is_group_terminal(group) {
                            if group.files.iter().all(|file| file.source_removed) {
                                group.status = MigrationItemPhase::Complete;
                                group.error = None;
                                group.failure_kind = None;
                            } else if group.files.iter().any(|file| file.source_removed) {
                                group.status = MigrationItemPhase::Failed;
                                group.failure_kind = Some(MigrationFailureKind::CleanupFailed);
                                group.error = Some("stopped after partial source cleanup; published targets and remaining source files are preserved".to_string());
                            } else {
                                group.status = MigrationItemPhase::Skipped;
                                group.error = Some("stopped; source files remain in the original library; any already published target copies are preserved".to_string());
                            }
                        }
                    }
                    job.status = MigrationRunStatus::Stopped;
                }
            }
            MigrationControl::RetryFailed => {
                if matches!(
                    job.status,
                    MigrationRunStatus::Partial | MigrationRunStatus::Failed
                ) {
                    for group in &mut job.groups {
                        if group.status == MigrationItemPhase::Failed {
                            group.status = if group.files.iter().all(|file| file.published) {
                                MigrationItemPhase::CleaningSource
                            } else if group.files.iter().any(|file| file.verified) {
                                MigrationItemPhase::Publishing
                            } else {
                                MigrationItemPhase::Copying
                            };
                            group.error = None;
                            group.failure_kind = None;
                        }
                    }
                    for transfer in &mut job.task_transfers {
                        if transfer.error.is_some() {
                            transfer.error = None;
                        }
                    }
                    job.status = MigrationRunStatus::Queued;
                }
            }
        }
        self.save_job(&job)?;
        Ok(status_from_job(&job))
    }

    pub(crate) fn status(&self, migration_id: &str) -> Result<Option<MigrationStatus>> {
        let Some(bytes) = self
            .source
            .migration_read_state(&job_state_name(migration_id), MAX_STATE_BYTES)?
        else {
            return Ok(None);
        };
        let job: JobDocument = decode_state(&bytes, "migration job")?;
        self.validate_job_bindings(&job)?;
        Ok(Some(status_from_job(&job)))
    }

    pub(crate) fn attach_task_transfer(
        &self,
        migration_id: &str,
        plan: TaskTransferPlan,
    ) -> Result<MigrationStatus> {
        let _state_lock = self.source.migration_state_lock()?;
        let mut job = self.load_job(migration_id)?;
        self.validate_job_bindings(&job)?;
        if !job.request.include_incomplete_tasks {
            bail!("migration preview did not include incomplete task transfer");
        }
        let impact = job
            .request
            .task_impacts
            .iter()
            .find(|impact| impact.task_id == plan.transfer.task_id)
            .ok_or_else(|| anyhow!("task transfer is not bound to this migration preview"))?;
        if impact.generation != plan.transfer.started_generation {
            bail!("task generation changed after migration preview; create a fresh preview");
        }
        if !task_plan_root_matches_binding(
            &plan.source_root,
            &plan.transfer.source.root,
            &job.source,
        )? || !task_plan_root_matches_binding(
            &plan.target_root,
            &plan.transfer.target.root,
            &job.target,
        )? {
            bail!("task transfer plan root does not match the migration library binding");
        }
        if plan.transfer.migration_id != migration_id
            || plan.transfer.source.library_id != job.source.library_id
            || plan.transfer.target.library_id != job.target.library_id
        {
            bail!("task transfer source or target does not match the migration preview");
        }
        if job
            .task_transfers
            .iter()
            .any(|existing| existing.plan.transfer.transfer_id == plan.transfer.transfer_id)
        {
            return Ok(status_from_job(&job));
        }
        if job
            .task_transfers
            .iter()
            .any(|existing| existing.plan.transfer.task_id == plan.transfer.task_id)
        {
            bail!("task already has a transfer plan in this migration");
        }
        if plan.files.len() > MAX_MIGRATION_FILES {
            bail!("task transfer plan exceeds its bounded file limit");
        }
        let planned_bytes = plan
            .files
            .iter()
            .filter(|file| file.kind == TaskTransferEntryKind::File)
            .try_fold(0_u64, |sum, file| sum.checked_add(file.size_bytes))
            .ok_or_else(|| anyhow!("task transfer byte count overflowed"))?;
        if planned_bytes != plan.total_bytes {
            bail!("task transfer plan byte count does not match its file list");
        }
        validate_task_plan_paths(&plan)?;
        let entries = plan
            .files
            .iter()
            .map(|file| {
                if file.kind == TaskTransferEntryKind::File {
                    validate_task_transfer_file_proof(file)?;
                }
                Ok(TaskFileCheckpoint {
                    staging_attempt: 0,
                    staging_identity: None,
                    target_identity: None,
                    copied_bytes: 0,
                    source_verified_chunks: 0,
                    verified_chunks: 0,
                    target_verified_chunks: 0,
                    directory_intent: false,
                    published: false,
                    source_cleanup_intent: false,
                    source_removed: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let checkpoint = TaskTransferCheckpoint {
            plan,
            entries,
            copy_complete: false,
            error: None,
        };
        let mut all_plans = job.task_transfers.clone();
        all_plans.push(checkpoint);
        let actual_staging_bytes = all_plans
            .iter()
            .try_fold(0_u64, |sum, checkpoint| {
                sum.checked_add(checkpoint.plan.total_bytes)
            })
            .ok_or_else(|| anyhow!("task transfer byte count overflowed"))?;
        if actual_staging_bytes > job.preview.staging_bytes {
            let extra = actual_staging_bytes - job.preview.staging_bytes;
            job.preview.total_bytes = job
                .preview
                .total_bytes
                .checked_add(extra)
                .ok_or_else(|| anyhow!("migration byte count overflowed"))?;
            job.preview.required_space_bytes = job
                .preview
                .required_space_bytes
                .checked_add(extra)
                .ok_or_else(|| anyhow!("migration required space overflowed"))?;
        } else {
            let reduction = job.preview.staging_bytes - actual_staging_bytes;
            job.preview.total_bytes = job.preview.total_bytes.saturating_sub(reduction);
            job.preview.required_space_bytes =
                job.preview.required_space_bytes.saturating_sub(reduction);
        }
        job.preview.staging_bytes = actual_staging_bytes;
        if job.preview.required_space_bytes > self.target.migration_available_space_bytes()? {
            bail!("insufficient target free space for the confirmed task transfer plan");
        }
        job.task_transfers
            .push(all_plans.pop().expect("new task plan was appended"));
        self.save_job(&job)?;
        Ok(status_from_job(&job))
    }

    /// Returns the private durable plan documents to the queue coordinator for replay/commit.
    pub(crate) fn task_transfer_plans(&self, migration_id: &str) -> Result<Vec<TaskTransferPlan>> {
        let job = self.load_job(migration_id)?;
        self.validate_job_bindings(&job)?;
        Ok(job
            .task_transfers
            .into_iter()
            .map(|checkpoint| checkpoint.plan)
            .collect())
    }

    pub(crate) fn task_transfer_statuses(
        &self,
        migration_id: &str,
    ) -> Result<Vec<MigrationTaskTransferStatus>> {
        let job = self.load_job(migration_id)?;
        self.validate_job_bindings(&job)?;
        Ok(task_transfer_statuses_from_job(&job))
    }

    /// Call only after QueueManager has installed the target record and acknowledged its
    /// persisted transfer intent. Source staging cleanup is gated by this durable callback.
    pub(crate) fn ack_task_transfer(
        &self,
        migration_id: &str,
        transfer_id: &str,
    ) -> Result<MigrationStatus> {
        let _state_lock = self.source.migration_state_lock()?;
        let mut job = self.load_job(migration_id)?;
        self.validate_job_bindings(&job)?;
        if !job
            .task_transfers
            .iter()
            .any(|checkpoint| checkpoint.plan.transfer.transfer_id == transfer_id)
        {
            bail!("task transfer acknowledgment is not bound to this migration");
        }
        if !job
            .task_commit_acked
            .iter()
            .any(|saved| saved == transfer_id)
        {
            job.task_commit_acked.push(transfer_id.to_string());
        }
        if all_task_transfers_acked(&job) {
            job.awaiting_task_commit = false;
        }
        self.save_job(&job)?;
        Ok(status_from_job(&job))
    }

    /// Returns durable status using only the source root. The destination may be offline.
    pub(crate) fn list_for_source(source: &LibraryManager) -> Result<Vec<MigrationStatus>> {
        let names = source.migration_state_names()?;
        let mut statuses = Vec::new();
        for name in names
            .into_iter()
            .filter(|name| name.starts_with(JOB_STATE_PREFIX))
        {
            let bytes = source
                .migration_read_state(&name, MAX_STATE_BYTES)?
                .ok_or_else(|| anyhow!("migration status disappeared while listing"))?;
            let job: JobDocument = decode_state(&bytes, "migration job")?;
            let source_identity = source.migration_root_identity();
            if job.source.device != source_identity.device()
                || job.source.inode != source_identity.inode()
                || job.source.root != source.migration_root_path()
            {
                bail!("persisted migration source binding no longer matches this library");
            }
            statuses.push(status_from_job(&job));
        }
        statuses.sort_by(|left, right| right.id.cmp(&left.id));
        Ok(statuses)
    }

    pub(crate) fn set_runtime_context(
        &self,
        migration_id: &str,
        context: MigrationRuntimeContext,
    ) -> Result<MigrationStatus> {
        let _state_lock = self.source.migration_state_lock()?;
        let mut job = self.load_job(migration_id)?;
        self.validate_job_bindings(&job)?;
        job.runtime_context = Some(context);
        self.save_job(&job)?;
        Ok(status_from_job(&job))
    }

    pub(crate) fn runtime_context(
        &self,
        migration_id: &str,
    ) -> Result<Option<MigrationRuntimeContext>> {
        let Some(bytes) = self
            .source
            .migration_read_state(&job_state_name(migration_id), MAX_STATE_BYTES)?
        else {
            return Ok(None);
        };
        let job: JobDocument = decode_state(&bytes, "migration job")?;
        self.validate_job_bindings(&job)?;
        Ok(job.runtime_context)
    }

    /// Records the idempotent registry update after the caller applies it. The registry uses
    /// the migration ID as its own durable idempotency key, so a crash between the two writes is
    /// safe to replay.
    pub(crate) fn mark_default_update_applied(
        &self,
        migration_id: &str,
    ) -> Result<MigrationStatus> {
        let _state_lock = self.source.migration_state_lock()?;
        let mut job = self.load_job(migration_id)?;
        self.validate_job_bindings(&job)?;
        if job.status != MigrationRunStatus::Completed || !job.preview.set_target_default_on_success
        {
            bail!("migration is not eligible for its requested default-library update");
        }
        if !job.default_update_applied {
            job.default_update_applied = true;
            self.save_job(&job)?;
        }
        Ok(status_from_job(&job))
    }

    pub(crate) fn task_impacts(&self, migration_id: &str) -> Result<Vec<MigrationTaskImpact>> {
        if let Some(bytes) = self
            .source
            .migration_read_state(&job_state_name(migration_id), MAX_STATE_BYTES)?
        {
            let job: JobDocument = decode_state(&bytes, "migration job")?;
            self.validate_job_bindings(&job)?;
            return Ok(job.request.task_impacts);
        }
        if Self::find_preview_for_source(&self.source, migration_id)?.is_none() {
            return Ok(Vec::new());
        }
        let bytes = self
            .source
            .migration_read_state(&preview_state_name(migration_id), MAX_STATE_BYTES)?
            .ok_or_else(|| anyhow!("migration preview disappeared while reading task impact"))?;
        let document: PreviewDocument = decode_state(&bytes, "migration preview")?;
        Ok(document.request.task_impacts)
    }

    pub(crate) fn find_preview_for_source(
        source: &LibraryManager,
        preview_id: &str,
    ) -> Result<Option<PersistedMigrationPreview>> {
        let Some(bytes) =
            source.migration_read_state(&preview_state_name(preview_id), MAX_STATE_BYTES)?
        else {
            return Ok(None);
        };
        let document: PreviewDocument = decode_state(&bytes, "migration preview")?;
        if document.preview.id != preview_id {
            bail!("persisted migration preview ID does not match its filename");
        }
        let source_identity = source.migration_root_identity();
        if document.source.root != source.migration_root_path()
            || document.source.device != source_identity.device()
            || document.source.inode != source_identity.inode()
        {
            bail!("persisted migration source binding no longer matches this library");
        }
        Ok(Some(PersistedMigrationPreview {
            preview: document.preview,
            bindings: PersistedMigrationBindings {
                source_library_id: document.source.library_id,
                source_root: document.source.root,
                source_device: document.source.device,
                source_inode: document.source.inode,
                target_library_id: document.target.library_id,
                target_root: document.target.root,
                target_device: document.target.device,
                target_inode: document.target.inode,
            },
        }))
    }

    fn step_group(&self, group: &mut GroupCheckpoint, allow_cleanup: bool) -> Result<()> {
        if group.status == MigrationItemPhase::Copying
            || group.status == MigrationItemPhase::Verifying
            || group.status == MigrationItemPhase::Publishing
        {
            for file in &mut group.files {
                if file.published {
                    continue;
                }
                let staging_name = media_staging_name(file);
                let Some(identity) = file.staging_identity else {
                    if self
                        .target
                        .migration_staging_entry_identity(&staging_name)?
                        .is_some()
                    {
                        // A stage created immediately before a crash is intentionally retained.
                        // Never adopt or delete an uncheckpointed object; move to a fresh name.
                        file.staging_attempt = file
                            .staging_attempt
                            .checked_add(1)
                            .ok_or_else(|| anyhow!("migration staging retry counter overflowed"))?;
                        return Ok(());
                    }
                    let identity = self.target.migration_create_staging_file(&staging_name)?;
                    file.staging_identity = Some(SerializedIdentity::from(identity));
                    group.status = MigrationItemPhase::Copying;
                    return Ok(());
                };
                let staging_identity = identity.to_entry_identity();
                if file.copied_bytes < file.plan.proof.size {
                    let chunk = self.source.migration_copy_chunk(
                        &self.target,
                        &file.plan.source_path,
                        &staging_name,
                        &file.plan.proof,
                        staging_identity,
                        file.copied_bytes,
                        COPY_CHUNK_BYTES,
                    )?;
                    let chunk_index = usize::try_from(file.copied_bytes / COPY_CHUNK_BYTES)
                        .context("migration copy chunk index does not fit in memory")?;
                    let chunk_sha256 = format_sha256(&chunk.sha256);
                    if chunk_index == file.chunk_sha256.len() {
                        file.chunk_sha256.push(chunk_sha256);
                    } else if file
                        .chunk_sha256
                        .get(chunk_index)
                        .is_none_or(|saved| !saved.eq_ignore_ascii_case(&chunk_sha256))
                    {
                        bail!("migration copy chunk proof changed after restart");
                    }
                    file.copied_bytes = file
                        .copied_bytes
                        .checked_add(chunk.bytes)
                        .ok_or_else(|| anyhow!("migration copied byte counter overflowed"))?;
                    group.status = MigrationItemPhase::Copying;
                    return Ok(());
                }
                if !file.verified {
                    let chunk_count = media_verification_chunk_count(file.plan.proof.size);
                    if file.verified_chunks < chunk_count {
                        let chunk_index = file.verified_chunks;
                        let (offset, length, expected_sha256) =
                            media_chunk_bounds(&file.plan.proof, &file.chunk_sha256, chunk_index)?;
                        let source_sha256 = self.source.migration_file_chunk_sha256(
                            &file.plan.source_path,
                            &file.plan.proof,
                            offset,
                            length,
                        )?;
                        let target_sha256 = self.target.migration_staging_chunk_sha256(
                            &staging_name,
                            staging_identity,
                            file.plan.proof.size,
                            offset,
                            length,
                        )?;
                        if !source_sha256.eq_ignore_ascii_case(&expected_sha256)
                            || !target_sha256.eq_ignore_ascii_case(&expected_sha256)
                        {
                            bail!(
                                "migration source or staging content changed during chunk verification"
                            );
                        }
                        file.verified_chunks += 1;
                        if file.verified_chunks == chunk_count {
                            file.verified = true;
                        }
                        group.status = MigrationItemPhase::Verifying;
                        return Ok(());
                    }
                    file.verified = true;
                    group.status = MigrationItemPhase::Verifying;
                    return Ok(());
                }
                if !file.published {
                    if file.target_access_policy.is_none() {
                        let staged_access = self
                            .target
                            .migration_staging_access_policy(&staging_name, staging_identity)?;
                        file.target_identity = Some(SerializedIdentity::from(staging_identity));
                        file.target_access_policy = Some(SerializedAccessPolicy {
                            owner: staged_access.owner,
                            group: staged_access.group,
                            mode: file.plan.proof.mode & 0o777,
                            link_count: staged_access.link_count,
                        });
                        return Ok(());
                    }
                    let target_identity = file
                        .target_identity
                        .ok_or_else(|| anyhow!("migration target identity checkpoint is missing"))?
                        .to_entry_identity();
                    let target_access = file
                        .target_access_policy
                        .ok_or_else(|| {
                            anyhow!("migration target access-policy checkpoint is missing")
                        })?
                        .to_bound_file_access_policy();
                    if let Some(existing) = self
                        .target
                        .migration_path_identity(&file.plan.target_path)?
                    {
                        if existing != target_identity
                            || !self.target.migration_target_matches_exact(
                                &file.plan.target_path,
                                target_identity,
                                file.plan.proof.size,
                                target_access,
                                &file.plan.proof.sha256,
                            )?
                        {
                            bail!(
                                "published migration target does not match its saved inode and content proof"
                            );
                        }
                    } else {
                        let published_identity = self.target.migration_publish_staging_file(
                            &staging_name,
                            staging_identity,
                            &file.plan.target_path,
                            &file.plan.proof,
                        )?;
                        if published_identity != target_identity
                            || !self.target.migration_target_matches_exact(
                                &file.plan.target_path,
                                target_identity,
                                file.plan.proof.size,
                                target_access,
                                &file.plan.proof.sha256,
                            )?
                        {
                            bail!(
                                "published migration target failed exact inode, access, or content verification"
                            );
                        }
                    }
                    file.published = true;
                    group.status = MigrationItemPhase::Publishing;
                    return Ok(());
                }
            }
            group.status = MigrationItemPhase::CleaningSource;
            return Ok(());
        }

        if group.status == MigrationItemPhase::CleaningSource {
            if !allow_cleanup {
                return Ok(());
            }
            // The primary file is the first member in a plan and is deliberately cleaned last.
            for file in group.files.iter_mut().rev() {
                if file.source_removed {
                    continue;
                }
                let target_identity = file
                    .target_identity
                    .ok_or_else(|| {
                        anyhow!("published migration file is missing its target identity")
                    })?
                    .to_entry_identity();
                let target_access = file
                    .target_access_policy
                    .ok_or_else(|| {
                        anyhow!("published migration file is missing its access-policy proof")
                    })?
                    .to_bound_file_access_policy();
                if !self.target.migration_target_matches_exact(
                    &file.plan.target_path,
                    target_identity,
                    file.plan.proof.size,
                    target_access,
                    &file.plan.proof.sha256,
                )? {
                    bail!("verified target changed before source cleanup");
                }
                let source_identity = self
                    .source
                    .migration_path_identity(&file.plan.source_path)?;
                if source_identity.is_none() {
                    if file.source_cleanup_intent {
                        // Reconcile a crash after exact-source removal but before its journal save.
                        file.source_removed = true;
                        return Ok(());
                    }
                    bail!("migration source disappeared before its cleanup checkpoint");
                }
                if source_identity != Some(file.plan.proof.entry_identity()) {
                    bail!("migration source was replaced before cleanup");
                }
                if !file.source_cleanup_intent {
                    file.source_cleanup_intent = true;
                    return Ok(());
                }
                self.source
                    .migration_remove_source(&file.plan.source_path, &file.plan.proof)?;
                file.source_removed = true;
                return Ok(());
            }
            group.status = MigrationItemPhase::Complete;
        }
        Ok(())
    }

    fn step_task_transfer(&self, job: &mut JobDocument, task_index: usize) -> Result<()> {
        let migration_id = job.preview.id.clone();
        let checkpoint = job
            .task_transfers
            .get_mut(task_index)
            .ok_or_else(|| anyhow!("task transfer checkpoint disappeared"))?;
        if checkpoint.entries.len() != checkpoint.plan.files.len() {
            bail!("task transfer checkpoint does not match its file plan");
        }
        if checkpoint.copy_complete {
            return Ok(());
        }
        if checkpoint.plan.files.is_empty() {
            checkpoint.copy_complete = true;
            return Ok(());
        }

        let Some(file_index) = (0..checkpoint.plan.files.len()).find(|index| {
            let file = &checkpoint.plan.files[*index];
            let entry = &checkpoint.entries[*index];
            !task_file_checkpoint_complete(file, entry)
                && task_parent_directories_ready(&checkpoint.plan, &checkpoint.entries, file)
        }) else {
            if checkpoint
                .plan
                .files
                .iter()
                .zip(&checkpoint.entries)
                .all(|(file, entry)| task_file_checkpoint_complete(file, entry))
            {
                checkpoint.copy_complete = true;
                return Ok(());
            }
            bail!("task transfer file plan has an unready or missing parent directory");
        };

        let plan_file = checkpoint.plan.files[file_index].clone();
        let transfer_id = checkpoint.plan.transfer.transfer_id.clone();
        let entry = &mut checkpoint.entries[file_index];
        if plan_file.kind == TaskTransferEntryKind::Directory {
            if !self
                .source
                .migration_task_source_directory_matches(&plan_file)?
            {
                bail!("task staging source directory identity or access policy changed");
            }
            if let Some(identity) = entry.target_identity {
                let identity = directory_identity(identity);
                if !self.target.migration_task_target_directory_matches(
                    &plan_file.target_relative,
                    identity,
                    &plan_file,
                )? {
                    bail!("task staging target directory identity or access policy changed");
                }
                entry.published = true;
                return Ok(());
            }
            let existing = self
                .target
                .migration_task_path_identity(&plan_file.target_relative)?;
            if !entry.directory_intent {
                if existing.is_some() {
                    bail!("task staging target directory conflicts with an existing entry");
                }
                entry.directory_intent = true;
                return Ok(());
            }

            let job_root = task_staging_job_root(&plan_file.target_relative)?;
            let target_identity = if job_root {
                self.target
                    .migration_prepare_task_staging_job(&plan_file.target_relative, &transfer_id)?
            } else if let Some(identity) = existing {
                // If interrupted after mkdir but before saving the identity, adopt only an empty
                // directory with the exact planned policy. No file payload is written before its
                // parent directory checkpoint is durable.
                if !identity.is_dir()
                    || !self.target.migration_task_target_directory_matches(
                        &plan_file.target_relative,
                        identity,
                        &plan_file,
                    )?
                    || !self
                        .target
                        .migration_task_directory_is_empty(&plan_file.target_relative, identity)?
                {
                    bail!(
                        "uncheckpointed task target directory is not an empty matching directory"
                    );
                }
                identity
            } else {
                self.target
                    .migration_task_create_directory(&plan_file.target_relative, plan_file.mode)?
                    .ok_or_else(|| anyhow!("task staging directory appeared during creation"))?
            };
            if !self.target.migration_task_target_directory_matches(
                &plan_file.target_relative,
                target_identity,
                &plan_file,
            )? {
                bail!("task staging target directory access policy could not be preserved");
            }
            entry.target_identity = Some(SerializedIdentity::from(target_identity));
            entry.published = true;
            return Ok(());
        }

        if plan_file.kind != TaskTransferEntryKind::File {
            bail!("task transfer plan contains an unknown entry kind");
        }
        validate_task_transfer_file_proof(&plan_file)?;
        let chunk_count = task_verification_chunk_count(&plan_file);
        let target_identity = entry
            .target_identity
            .map(SerializedIdentity::to_entry_identity);

        if entry.published {
            let target_identity = target_identity
                .ok_or_else(|| anyhow!("published task file is missing its target identity"))?;
            if entry.target_verified_chunks >= chunk_count {
                return Ok(());
            }
            let chunk_index = entry.target_verified_chunks;
            let (offset, length, expected_sha256) = task_chunk_bounds(&plan_file, chunk_index)?;
            let actual = self.target.migration_task_chunk_sha256(
                &plan_file.target_relative,
                target_identity,
                plan_file.size_bytes,
                task_transfer_access_policy(&plan_file),
                offset,
                length,
            )?;
            if !actual.eq_ignore_ascii_case(&expected_sha256) {
                bail!("published task target content changed during chunk verification");
            }
            entry.target_verified_chunks += 1;
            return Ok(());
        }

        let staging_name = task_staging_name(
            &migration_id,
            &transfer_id,
            task_index,
            file_index,
            entry.staging_attempt,
        );
        let destination_identity = self
            .target
            .migration_task_path_identity(&plan_file.target_relative)?;
        if let Some(staging_identity) = entry
            .staging_identity
            .map(SerializedIdentity::to_entry_identity)
        {
            if let Some(target_identity) = destination_identity {
                if target_identity == staging_identity {
                    entry.target_identity = Some(SerializedIdentity::from(target_identity));
                    entry.published = true;
                    entry.target_verified_chunks = 0;
                    return Ok(());
                }
                bail!("task target conflicts with an existing entry before publication");
            }

            let Some(actual_staging_identity) =
                self.target.migration_staging_identity(&staging_name)?
            else {
                bail!("task migration staging file disappeared before publication");
            };
            if actual_staging_identity != staging_identity {
                bail!("task migration staging file identity changed");
            }
            if entry.copied_bytes < plan_file.size_bytes {
                let chunk_index = usize::try_from(entry.copied_bytes / COPY_CHUNK_BYTES)
                    .context("task copy chunk index does not fit in memory")?;
                let expected_sha256 = task_chunk_hash(&plan_file, chunk_index)?;
                let chunk = self.source.migration_copy_task_chunk(
                    &self.target,
                    &plan_file,
                    &staging_name,
                    staging_identity,
                    entry.copied_bytes,
                    COPY_CHUNK_BYTES,
                )?;
                if !format_sha256(&chunk.sha256).eq_ignore_ascii_case(&expected_sha256) {
                    bail!("task staging source content changed during copy");
                }
                entry.copied_bytes = entry
                    .copied_bytes
                    .checked_add(chunk.bytes)
                    .ok_or_else(|| anyhow!("task copied byte counter overflowed"))?;
                if entry.copied_bytes > plan_file.size_bytes {
                    bail!("task copied byte count exceeds its file size");
                }
                return Ok(());
            }

            if entry.source_verified_chunks < chunk_count {
                let chunk_index = entry.source_verified_chunks;
                let (offset, length, expected_sha256) = task_chunk_bounds(&plan_file, chunk_index)?;
                let actual = self.source.migration_task_chunk_sha256(
                    &plan_file.source_relative,
                    EntryIdentity::regular_file(plan_file.device, plan_file.inode),
                    plan_file.size_bytes,
                    task_transfer_access_policy(&plan_file),
                    offset,
                    length,
                )?;
                if !actual.eq_ignore_ascii_case(&expected_sha256) {
                    bail!("task staging source content changed after copy");
                }
                entry.source_verified_chunks += 1;
                return Ok(());
            }

            if entry.verified_chunks < chunk_count {
                let chunk_index = entry.verified_chunks;
                let (offset, length, expected_sha256) = task_chunk_bounds(&plan_file, chunk_index)?;
                let actual = self.target.migration_staging_chunk_sha256(
                    &staging_name,
                    staging_identity,
                    plan_file.size_bytes,
                    offset,
                    length,
                )?;
                if !actual.eq_ignore_ascii_case(&expected_sha256) {
                    bail!("task migration staging content failed chunk verification");
                }
                entry.verified_chunks += 1;
                return Ok(());
            }

            let published_identity = self.target.migration_task_publish_file(
                &staging_name,
                staging_identity,
                &plan_file.target_relative,
                &plan_file,
            )?;
            entry.target_identity = Some(SerializedIdentity::from(published_identity));
            entry.published = true;
            entry.target_verified_chunks = 0;
            return Ok(());
        }

        if destination_identity.is_some() {
            bail!("task target conflicts with an existing entry before copy");
        }
        if self
            .target
            .migration_staging_identity(&staging_name)?
            .is_some()
        {
            // A stage that exists without a persisted inode checkpoint may be an interrupted
            // creation or an unrelated object. Retain it and move to a fresh durable name.
            entry.staging_attempt = entry
                .staging_attempt
                .checked_add(1)
                .ok_or_else(|| anyhow!("task staging retry counter overflowed"))?;
            return Ok(());
        }
        let staging_identity = self.target.migration_create_staging_file(&staging_name)?;
        entry.staging_identity = Some(SerializedIdentity::from(staging_identity));
        entry.copied_bytes = 0;
        entry.source_verified_chunks = 0;
        entry.verified_chunks = 0;
        entry.target_verified_chunks = 0;
        Ok(())
    }

    fn step_task_payload_cleanup(
        &self,
        job: &mut JobDocument,
        task_index: usize,
        file_index: usize,
    ) -> Result<()> {
        let transfer_id = job
            .task_transfers
            .get(task_index)
            .map(|checkpoint| checkpoint.plan.transfer.transfer_id.clone())
            .ok_or_else(|| anyhow!("task transfer checkpoint disappeared during cleanup"))?;
        if !job.task_commit_acked.iter().any(|id| id == &transfer_id) {
            bail!("task payload cleanup is not authorized before queue acknowledgment");
        }
        let checkpoint = job
            .task_transfers
            .get_mut(task_index)
            .ok_or_else(|| anyhow!("task transfer checkpoint disappeared during cleanup"))?;
        let plan_file = checkpoint
            .plan
            .files
            .get(file_index)
            .cloned()
            .ok_or_else(|| anyhow!("task source cleanup file disappeared from its plan"))?;
        if plan_file.kind != TaskTransferEntryKind::File {
            bail!("task source cleanup selected a non-file entry");
        }
        validate_task_transfer_file_proof(&plan_file)?;
        let entry = checkpoint
            .entries
            .get_mut(file_index)
            .ok_or_else(|| anyhow!("task source cleanup checkpoint disappeared"))?;
        if entry.source_removed {
            return Ok(());
        }

        let target_identity = entry
            .target_identity
            .map(SerializedIdentity::to_entry_identity)
            .ok_or_else(|| anyhow!("task target identity is missing before source cleanup"))?;
        if !entry.published
            || entry.target_verified_chunks != task_verification_chunk_count(&plan_file)
            || !self.target.migration_task_target_matches(
                &plan_file.target_relative,
                target_identity,
                &plan_file,
            )?
        {
            bail!("verified task target changed before source payload cleanup");
        }

        let source_identity = self
            .source
            .migration_task_path_identity(&plan_file.source_relative)?;
        if source_identity.is_none() {
            if entry.source_cleanup_intent {
                // Reconcile unlink after the intent was durably saved but before this checkpoint.
                entry.source_removed = true;
                return Ok(());
            }
            bail!("task source payload disappeared before cleanup intent");
        }
        if source_identity
            != Some(EntryIdentity::regular_file(
                plan_file.device,
                plan_file.inode,
            ))
        {
            bail!("task source payload was replaced before cleanup");
        }
        if !entry.source_cleanup_intent {
            entry.source_cleanup_intent = true;
            return Ok(());
        }
        self.source.migration_task_remove_source_file(&plan_file)?;
        entry.source_removed = true;
        Ok(())
    }

    fn make_preview(
        &self,
        request: &MigrationRequest,
        item_ids: Vec<String>,
        plan: Vec<MigrationPlannedGroup>,
    ) -> Result<(MigrationPreview, Vec<MigrationPlannedGroup>)> {
        let id = new_migration_id();
        let incomplete_task_count = request.task_impacts.len();
        let staging_bytes = request
            .task_impacts
            .iter()
            .try_fold(0_u64, |sum, task| sum.checked_add(task.staging_bytes))
            .ok_or_else(|| anyhow!("incomplete task staging byte count overflowed"))?;
        let staging_file_count = request
            .task_impacts
            .iter()
            .try_fold(0_usize, |sum, task| {
                sum.checked_add(task.staging_file_count)
            })
            .ok_or_else(|| anyhow!("incomplete task staging file count overflowed"))?;
        let mut total_bytes = 0_u64;
        let mut required_space_bytes = 0_u64;
        let mut file_count = if request.include_incomplete_tasks {
            staging_file_count
        } else {
            0
        };
        let mut conflict_count = 0_usize;
        let mut items = Vec::with_capacity(plan.len());
        for group in &plan {
            if group.conflict {
                conflict_count += 1;
            }
            let group_bytes = group
                .files
                .iter()
                .try_fold(0_u64, |sum, file| sum.checked_add(file.proof.size))
                .ok_or_else(|| anyhow!("media bundle byte count overflowed"))?;
            total_bytes = total_bytes
                .checked_add(group_bytes)
                .ok_or_else(|| anyhow!("migration byte count overflowed"))?;
            file_count = file_count
                .checked_add(group.files.len())
                .ok_or_else(|| anyhow!("migration file count overflowed"))?;
            if group.outcome == MigrationPlanOutcome::Move {
                required_space_bytes = required_space_bytes
                    .checked_add(group_bytes)
                    .ok_or_else(|| anyhow!("migration required space overflowed"))?;
            }
            let status = if group.outcome == MigrationPlanOutcome::Skip {
                MigrationItemPhase::Skipped
            } else {
                MigrationItemPhase::Copying
            };
            items.push(MigrationPreviewItem {
                item_id: group.item_id.clone(),
                status,
                conflict: group.conflict,
                source_path: group.source_path.clone(),
                target_path: group.target_path.clone(),
                bytes: group_bytes,
                files: group
                    .files
                    .iter()
                    .map(|file| MigrationPreviewFile {
                        source_path: file.source_path.clone(),
                        target_path: file.target_path.clone(),
                        size_bytes: file.proof.size,
                    })
                    .collect(),
            });
        }
        if request.include_incomplete_tasks {
            total_bytes = total_bytes
                .checked_add(staging_bytes)
                .ok_or_else(|| anyhow!("migration byte count overflowed"))?;
            required_space_bytes = required_space_bytes
                .checked_add(staging_bytes)
                .ok_or_else(|| anyhow!("migration required space overflowed"))?;
        }
        if plan.len() > 50_000 || file_count > MAX_MIGRATION_FILES {
            bail!("migration plan exceeds the bounded item or file limit");
        }
        let available_space_bytes = self.target.migration_available_space_bytes()?;
        let mut warnings = request.warnings.clone();
        warnings.truncate(64);
        let preview = MigrationPreview {
            id,
            revision: PREVIEW_REVISION.to_string(),
            source_library_id: self.source_library_id.clone(),
            target_library_id: self.target_library_id.clone(),
            scope: request.scope.clone(),
            item_ids,
            conflict: request.conflict,
            item_conflicts: request.item_conflicts.clone(),
            include_incomplete_tasks: request.include_incomplete_tasks,
            set_target_default_on_success: request.set_target_default_on_success,
            item_count: plan.len(),
            file_count,
            total_bytes,
            required_space_bytes,
            available_space_bytes,
            conflict_count,
            incomplete_task_count,
            staging_bytes,
            warnings,
            items,
        };
        Ok((preview, plan))
    }

    fn revalidate_preview(
        &self,
        document: &PreviewDocument,
        output_lease: Option<&crate::downloader::VideoMigrationGuard>,
    ) -> Result<()> {
        let available = self.target.migration_available_space_bytes()?;
        if document.preview.required_space_bytes > available {
            bail!("insufficient target free space for the confirmed migration plan");
        }
        let snapshot = match output_lease {
            Some(lease) => self.source.migration_scan_with_output_lease(lease)?,
            None => self.source.migration_scan()?,
        };
        for group in &document.plan {
            let current_item = snapshot
                .items
                .iter()
                .find(|item| item.id == group.item_id && item.presence == Presence::Present)
                .ok_or_else(|| anyhow!("source media changed or disappeared after preview"))?;
            if current_item.relative_path != group.source_path {
                bail!("source media path changed after preview");
            }
            let mut expected_attachments = group
                .files
                .iter()
                .filter(|file| file.source_path != group.source_path)
                .map(|file| file.source_path.clone())
                .collect::<Vec<_>>();
            let mut current_attachments = current_item
                .attachments
                .iter()
                .map(|attachment| attachment.relative_path.clone())
                .collect::<Vec<_>>();
            expected_attachments.sort();
            current_attachments.sort();
            if current_attachments != expected_attachments {
                bail!("media sidecar grouping changed after preview");
            }
            for file in &group.files {
                if !self
                    .source
                    .migration_file_identity_matches(&file.source_path, &file.proof)?
                {
                    bail!("source identity or access policy changed after preview");
                }
                if group.outcome == MigrationPlanOutcome::Move
                    && self
                        .target
                        .migration_path_identity(&file.target_path)?
                        .is_some()
                {
                    bail!(
                        "migration target conflict appeared after preview: {}",
                        file.target_path
                    );
                }
            }
        }
        Ok(())
    }

    fn revalidate_job(
        &self,
        job: &JobDocument,
        output_lease: Option<&crate::downloader::VideoMigrationGuard>,
    ) -> Result<()> {
        let document = PreviewDocument {
            format_version: 1,
            created_at: job.created_at,
            source: job.source.clone(),
            target: job.target.clone(),
            request: job.request.clone(),
            preview: job.preview.clone(),
            plan: job.groups.iter().map(|group| group.plan.clone()).collect(),
        };
        self.revalidate_preview(&document, output_lease)
    }

    fn source_binding(&self) -> RootBinding {
        let identity = self.source.migration_root_identity();
        RootBinding {
            library_id: self.source_library_id.clone(),
            root: self.source.migration_root_path().to_path_buf(),
            device: identity.device(),
            inode: identity.inode(),
        }
    }

    fn target_binding(&self) -> RootBinding {
        let identity = self.target.migration_root_identity();
        RootBinding {
            library_id: self.target_library_id.clone(),
            root: self.target.migration_root_path().to_path_buf(),
            device: identity.device(),
            inode: identity.inode(),
        }
    }

    fn validate_preview_bindings(&self, document: &PreviewDocument) -> Result<()> {
        if document.source.library_id != self.source_library_id
            || document.target.library_id != self.target_library_id
            || document.source.root != self.source.migration_root_path()
            || document.target.root != self.target.migration_root_path()
            || document.source.device != self.source.migration_root_identity().device()
            || document.source.inode != self.source.migration_root_identity().inode()
            || document.target.device != self.target.migration_root_identity().device()
            || document.target.inode != self.target.migration_root_identity().inode()
        {
            bail!("migration preview library binding changed");
        }
        Ok(())
    }

    fn validate_job_bindings(&self, job: &JobDocument) -> Result<()> {
        if job.source.library_id != self.source_library_id
            || job.target.library_id != self.target_library_id
            || job.source.root != self.source.migration_root_path()
            || job.target.root != self.target.migration_root_path()
            || job.source.device != self.source.migration_root_identity().device()
            || job.source.inode != self.source.migration_root_identity().inode()
            || job.target.device != self.target.migration_root_identity().device()
            || job.target.inode != self.target.migration_root_identity().inode()
        {
            bail!("persisted migration library binding changed");
        }
        Ok(())
    }

    fn load_job(&self, id: &str) -> Result<JobDocument> {
        let bytes = self
            .source
            .migration_read_state(&job_state_name(id), MAX_STATE_BYTES)?
            .ok_or_else(|| anyhow!("migration job does not exist"))?;
        decode_state(&bytes, "migration job")
    }

    fn save_job(&self, job: &JobDocument) -> Result<()> {
        let name = job_state_name(&job.preview.id);
        self.source
            .migration_write_state(&name, &encode_state(job)?)
    }
}

impl SerializedIdentity {
    fn from(identity: crate::safe_fs::EntryIdentity) -> Self {
        Self {
            device: identity.device(),
            inode: identity.inode(),
        }
    }

    fn to_entry_identity(self) -> crate::safe_fs::EntryIdentity {
        crate::safe_fs::EntryIdentity::regular_file(self.device, self.inode)
    }
}

impl SerializedAccessPolicy {
    fn to_bound_file_access_policy(self) -> BoundFileAccessPolicy {
        BoundFileAccessPolicy {
            owner: self.owner,
            group: self.group,
            mode: self.mode,
            link_count: self.link_count,
        }
    }
}

fn media_staging_name(file: &FileCheckpoint) -> String {
    if file.staging_attempt == 0 {
        file.staging_name.clone()
    } else {
        format!("{}-attempt-{}", file.staging_name, file.staging_attempt)
    }
}

fn validate_request(request: &MigrationRequest) -> Result<()> {
    if request.item_conflicts.len() > 10_000
        || request
            .item_conflicts
            .keys()
            .any(|id| id.is_empty() || id.len() > 256)
    {
        bail!("migration conflict overrides exceed their bounded limit");
    }
    if request.task_impacts.len() > MAX_TASK_IMPACTS {
        bail!("migration task impact list exceeds its bounded limit");
    }
    let mut seen = HashSet::new();
    if request
        .task_impacts
        .iter()
        .any(|task| task.task_id.is_empty() || !seen.insert(task.task_id.as_str()))
    {
        bail!("migration task impact list contains an empty or duplicate task ID");
    }
    if request.scope == MigrationScope::Selected && request.item_ids.is_empty() {
        bail!("select at least one media item to migrate");
    }
    if request.item_ids.len() > 50_000 {
        bail!("selected migration item list exceeds its bounded limit");
    }
    Ok(())
}

fn selected_item_ids(
    request: &MigrationRequest,
    snapshot: &LibrarySnapshot,
) -> Result<Vec<String>> {
    match &request.scope {
        MigrationScope::Library => {
            if !request.item_ids.is_empty() {
                bail!("whole-library migration does not accept selected item IDs");
            }
            Ok(snapshot
                .items
                .iter()
                .filter(|item| item.presence == Presence::Present)
                .map(|item| item.id.clone())
                .collect())
        }
        MigrationScope::Selected => {
            let mut ids = request.item_ids.clone();
            ids.sort();
            ids.dedup();
            if ids.len() != request.item_ids.len() {
                bail!("a media item was selected more than once for migration");
            }
            Ok(ids)
        }
    }
}

fn status_from_job(job: &JobDocument) -> MigrationStatus {
    let task_transfers = task_transfer_statuses_from_job(job);
    let items = job
        .groups
        .iter()
        .map(|group| MigrationStatusItem {
            item_id: group.plan.item_id.clone(),
            status: group.status,
            source_path: group.plan.source_path.clone(),
            target_path: group.plan.target_path.clone(),
            bytes: group.plan.files.iter().map(|file| file.proof.size).sum(),
            error: group.error.clone(),
            failure_kind: group.failure_kind,
            files: group
                .files
                .iter()
                .map(|file| MigrationFileStatus {
                    source_path: file.plan.source_path.clone(),
                    target_path: file.plan.target_path.clone(),
                    size_bytes: file.plan.proof.size,
                    status: if group.status == MigrationItemPhase::Failed {
                        MigrationItemPhase::Failed
                    } else if file.source_removed {
                        MigrationItemPhase::Complete
                    } else if !file.published && file.verified {
                        MigrationItemPhase::Publishing
                    } else if file.published {
                        MigrationItemPhase::CleaningSource
                    } else if file.copied_bytes > 0 {
                        MigrationItemPhase::Copying
                    } else {
                        group.status
                    },
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    let media_success_count = job
        .groups
        .iter()
        .filter(|group| group.status == MigrationItemPhase::Complete)
        .count();
    let skipped_count = job
        .groups
        .iter()
        .filter(|group| group.status == MigrationItemPhase::Skipped)
        .count();
    let media_failed_count = job
        .groups
        .iter()
        .filter(|group| group.status == MigrationItemPhase::Failed)
        .count();
    let success_count = media_success_count;
    let failed_count = media_failed_count;
    let completed_items = media_success_count + skipped_count;
    let media_completed_files = job
        .groups
        .iter()
        .map(|group| {
            if group.status == MigrationItemPhase::Skipped {
                group.files.len()
            } else {
                group
                    .files
                    .iter()
                    .filter(|file| file.source_removed)
                    .count()
            }
        })
        .sum::<usize>();
    let completed_files = media_completed_files
        + task_transfers
            .iter()
            .map(|task| task.completed_files)
            .sum::<usize>();
    let copied_media_bytes = job
        .groups
        .iter()
        .flat_map(|group| group.files.iter())
        .map(|file| file.copied_bytes.min(file.plan.proof.size))
        .sum::<u64>();
    let copied_task_bytes = job
        .task_transfers
        .iter()
        .flat_map(|transfer| transfer.entries.iter())
        .map(|entry| entry.copied_bytes)
        .sum::<u64>();
    let copied_bytes = copied_media_bytes.saturating_add(copied_task_bytes);
    let total_files = job
        .groups
        .iter()
        .map(|group| group.files.len())
        .sum::<usize>()
        + job
            .task_transfers
            .iter()
            .flat_map(|transfer| transfer.plan.files.iter())
            .filter(|file| file.kind == TaskTransferEntryKind::File)
            .count();
    let total_bytes = job.preview.total_bytes;
    MigrationStatus {
        id: job.preview.id.clone(),
        preview_id: job.preview.id.clone(),
        revision: job.preview.revision.clone(),
        status: job.status,
        source_library_id: job.preview.source_library_id.clone(),
        target_library_id: job.preview.target_library_id.clone(),
        scope: job.preview.scope.clone(),
        conflict: job.preview.conflict,
        item_conflicts: job.preview.item_conflicts.clone(),
        include_incomplete_tasks: job.preview.include_incomplete_tasks,
        set_target_default_on_success: job.preview.set_target_default_on_success,
        default_update_applied: job.default_update_applied,
        total_items: job.groups.len(),
        completed_items,
        success_count,
        skipped_count,
        failed_count,
        total_files,
        completed_files,
        copied_bytes,
        total_bytes,
        required_space_bytes: job.preview.required_space_bytes,
        available_space_bytes: job.preview.available_space_bytes,
        incomplete_task_count: job.preview.incomplete_task_count,
        staging_bytes: job.preview.staging_bytes,
        awaiting_task_plans: !all_task_plans_attached(job),
        awaiting_task_commit: job.awaiting_task_commit,
        warnings: status_warnings(job),
        items,
        task_transfers,
    }
}

fn status_warnings(job: &JobDocument) -> Vec<String> {
    let mut warnings = job.preview.warnings.clone();
    warnings.extend(job.runtime_warnings.iter().cloned());
    warnings.truncate(64);
    warnings
}

fn record_runtime_warning(job: &mut JobDocument, warning: &str) {
    let warning = redact_runtime_warning(warning);
    if !warning.is_empty() && !job.runtime_warnings.contains(&warning) {
        if job.runtime_warnings.len() >= 16 {
            job.runtime_warnings.remove(0);
        }
        job.runtime_warnings.push(warning);
    }
}

fn validate_migration_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        bail!("migration ID has an invalid format");
    }
    Ok(())
}

fn redact_runtime_warning(warning: &str) -> String {
    const SENSITIVE_KEYS: [&str; 6] = [
        "token",
        "cookie",
        "authorization",
        "password",
        "secret",
        "api_key",
    ];
    let mut redacted = String::new();
    for token in warning.split_whitespace() {
        let safe = if token.starts_with('/') || token.starts_with("~/") {
            "[path]".to_string()
        } else if token.contains("://") {
            "[url]".to_string()
        } else if let Some((key, _)) = token.split_once('=')
            && SENSITIVE_KEYS
                .iter()
                .any(|sensitive| key.to_ascii_lowercase().contains(sensitive))
        {
            format!("{key}=[redacted]")
        } else {
            token
                .chars()
                .filter(|character| !character.is_control())
                .collect::<String>()
        };
        if safe.is_empty() {
            continue;
        }
        if !redacted.is_empty() {
            redacted.push(' ');
        }
        for character in safe.chars() {
            if redacted.len() + character.len_utf8() > 1024 {
                return redacted;
            }
            redacted.push(character);
        }
    }
    redacted
}

fn task_transfer_statuses_from_job(job: &JobDocument) -> Vec<MigrationTaskTransferStatus> {
    job.task_transfers
        .iter()
        .map(|checkpoint| {
            let plan = &checkpoint.plan;
            let completed_files = plan
                .files
                .iter()
                .zip(&checkpoint.entries)
                .filter(|(file, entry)| {
                    file.kind == TaskTransferEntryKind::File
                        && entry.published
                        && entry.target_verified_chunks == task_verification_chunk_count(file)
                })
                .count();
            MigrationTaskTransferStatus {
                task_id: plan.transfer.task_id.clone(),
                transfer_id: plan.transfer.transfer_id.clone(),
                completed_files,
                total_files: plan
                    .files
                    .iter()
                    .filter(|file| file.kind == TaskTransferEntryKind::File)
                    .count(),
                copy_complete: checkpoint.copy_complete,
                commit_acked: job.task_commit_acked.contains(&plan.transfer.transfer_id),
                cleaned_source_files: plan
                    .files
                    .iter()
                    .zip(&checkpoint.entries)
                    .filter(|(file, entry)| {
                        file.kind == TaskTransferEntryKind::File && entry.source_removed
                    })
                    .count(),
                cleanup_total_files: plan
                    .files
                    .iter()
                    .filter(|file| file.kind == TaskTransferEntryKind::File)
                    .count(),
                source_cleanup_complete: plan
                    .files
                    .iter()
                    .zip(&checkpoint.entries)
                    .filter(|(file, _)| file.kind == TaskTransferEntryKind::File)
                    .all(|(_, entry)| entry.source_removed),
                error: checkpoint.error.clone(),
            }
        })
        .collect()
}

fn is_group_terminal(group: &GroupCheckpoint) -> bool {
    matches!(
        group.status,
        MigrationItemPhase::Complete | MigrationItemPhase::Skipped | MigrationItemPhase::Failed
    )
}

fn is_job_held_or_terminal(status: MigrationRunStatus) -> bool {
    matches!(
        status,
        MigrationRunStatus::Paused
            | MigrationRunStatus::Stopped
            | MigrationRunStatus::Completed
            | MigrationRunStatus::Partial
            | MigrationRunStatus::Failed
    )
}

fn all_task_transfers_acked(job: &JobDocument) -> bool {
    if !job.request.include_incomplete_tasks {
        return true;
    }
    job.task_transfers.len() == job.request.task_impacts.len()
        && job.task_transfers.iter().all(|plan| {
            job.task_commit_acked
                .iter()
                .any(|transfer_id| transfer_id == &plan.plan.transfer.transfer_id)
        })
        && job.request.task_impacts.iter().all(|impact| {
            job.task_transfers
                .iter()
                .any(|plan| plan.plan.transfer.task_id == impact.task_id)
        })
}

fn all_task_payloads_removed(job: &JobDocument) -> bool {
    !job.request.include_incomplete_tasks
        || job.task_transfers.iter().all(|transfer| {
            transfer
                .plan
                .files
                .iter()
                .zip(&transfer.entries)
                .filter(|(file, _)| file.kind == TaskTransferEntryKind::File)
                .all(|(_, entry)| entry.source_removed)
        })
}

fn all_task_plans_attached(job: &JobDocument) -> bool {
    !job.request.include_incomplete_tasks
        || (job.task_transfers.len() == job.request.task_impacts.len()
            && job.request.task_impacts.iter().all(|impact| {
                job.task_transfers
                    .iter()
                    .any(|plan| plan.plan.transfer.task_id == impact.task_id)
            }))
}

fn validate_task_plan_paths(plan: &TaskTransferPlan) -> Result<()> {
    if plan.files.len() > MAX_MIGRATION_FILES || plan.controls.len() > 64 {
        bail!("task transfer manifest exceeds its bounded entry limit");
    }
    let mut seen = HashSet::new();
    for (source, target) in plan
        .files
        .iter()
        .map(|file| (&file.source_relative, &file.target_relative))
        .chain(
            plan.controls
                .iter()
                .map(|control| (&control.source_relative, &control.target_relative)),
        )
    {
        if source != target || !safe_task_staging_relative(source) {
            bail!("task transfer path is outside its exact private staging namespace");
        }
        if !seen.insert(source.clone()) {
            bail!("task transfer manifest contains a duplicate path");
        }
    }
    if !safe_queue_record_relative(&plan.queue_record_source)
        || plan.queue_record_source != plan.queue_record_target
    {
        bail!("task queue record path is outside its expected private namespace");
    }
    Ok(())
}

fn safe_task_staging_relative(path: &Path) -> bool {
    if path.as_os_str().len() > 2048 {
        return false;
    }
    let mut components = path.components();
    let Some(std::path::Component::Normal(root)) = components.next() else {
        return false;
    };
    if root != ".telegram-video-downloader-staging" {
        return false;
    }
    let Some(std::path::Component::Normal(job)) = components.next() else {
        return false;
    };
    if !job.to_string_lossy().starts_with("job-") {
        return false;
    }
    components.all(|component| matches!(component, std::path::Component::Normal(_)))
}

fn validate_task_transfer_file_proof(file: &TaskTransferFile) -> Result<()> {
    if file.kind != TaskTransferEntryKind::File {
        bail!("task file proof requested for a directory entry");
    }
    if file.inode == 0 || file.device == 0 {
        bail!("task transfer file is missing its source identity");
    }
    let digest = file
        .sha256
        .as_deref()
        .ok_or_else(|| anyhow!("task transfer file is missing its full SHA-256 proof"))?;
    if !is_sha256_hex(digest) {
        bail!("task transfer file has an invalid full SHA-256 proof");
    }
    let expected_chunks = task_transfer_chunk_count(file.size_bytes);
    if file.chunk_sha256.len() != expected_chunks {
        bail!("task transfer file chunk proof does not match its size");
    }
    if file
        .chunk_sha256
        .iter()
        .any(|digest| !is_sha256_hex(digest))
    {
        bail!("task transfer file has an invalid chunk SHA-256 proof");
    }
    if file.owner != unsafe { libc::geteuid() } as u32 {
        bail!("task transfer file is not owned by the current user");
    }
    if file.link_count != Some(1) {
        bail!("task transfer file has an unexpected hard-link count");
    }
    if file.mode & !0o7777 != 0 || file.mode & 0o6000 != 0 {
        bail!("task transfer file has an invalid or set-id access mode");
    }
    if file.size_bytes == 0
        && !digest.eq_ignore_ascii_case(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
    {
        bail!("empty task transfer file has a nonempty content digest");
    }
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn format_sha256(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn task_transfer_chunk_count(size_bytes: u64) -> usize {
    let chunks = size_bytes.div_ceil(COPY_CHUNK_BYTES);
    usize::try_from(chunks).unwrap_or(usize::MAX)
}

fn media_verification_chunk_count(size_bytes: u64) -> usize {
    // Empty media files still need an explicit proof step for their empty SHA-256 digest.
    let chunks = size_bytes.div_ceil(COPY_CHUNK_BYTES);
    usize::try_from(chunks).unwrap_or(usize::MAX).max(1)
}

fn media_chunk_bounds(
    proof: &crate::file_manager::MigrationFileProof,
    chunk_sha256: &[String],
    chunk_index: usize,
) -> Result<(u64, u64, String)> {
    if proof.size == 0 {
        if chunk_index != 0 {
            bail!("empty migration file has no payload chunks");
        }
        return Ok((0, 0, proof.sha256.clone()));
    }
    let chunk_index = u64::try_from(chunk_index)
        .context("migration verification chunk index does not fit in a file offset")?;
    let offset = chunk_index
        .checked_mul(COPY_CHUNK_BYTES)
        .ok_or_else(|| anyhow!("migration verification offset overflowed"))?;
    if offset >= proof.size {
        bail!("migration verification chunk is outside its file");
    }
    let length = (proof.size - offset).min(COPY_CHUNK_BYTES);
    let index = usize::try_from(chunk_index)
        .context("migration verification chunk index does not fit in memory")?;
    let digest = chunk_sha256
        .get(index)
        .cloned()
        .ok_or_else(|| anyhow!("migration copy chunk proof is missing"))?;
    Ok((offset, length, digest))
}

fn task_verification_chunk_count(file: &TaskTransferFile) -> usize {
    // An empty file has no payload chunks, but still gets one zero-byte digest check so that
    // the source, staged object, and published object each have an explicit verification point.
    task_transfer_chunk_count(file.size_bytes).max(1)
}

fn task_chunk_hash(file: &TaskTransferFile, chunk_index: usize) -> Result<String> {
    if file.size_bytes == 0 && chunk_index == 0 {
        return file
            .sha256
            .clone()
            .ok_or_else(|| anyhow!("empty task file is missing its content digest"));
    }
    file.chunk_sha256
        .get(chunk_index)
        .cloned()
        .ok_or_else(|| anyhow!("task transfer chunk proof is missing"))
}

fn task_chunk_bounds(file: &TaskTransferFile, chunk_index: usize) -> Result<(u64, u64, String)> {
    if file.size_bytes == 0 {
        if chunk_index != 0 {
            bail!("empty task file has no payload chunks");
        }
        return Ok((0, 0, task_chunk_hash(file, chunk_index)?));
    }
    let chunk_index = u64::try_from(chunk_index)
        .context("task verification chunk index does not fit in a file offset")?;
    let offset = chunk_index
        .checked_mul(COPY_CHUNK_BYTES)
        .ok_or_else(|| anyhow!("task verification offset overflowed"))?;
    if offset >= file.size_bytes {
        bail!("task verification chunk is outside its file");
    }
    let length = (file.size_bytes - offset).min(COPY_CHUNK_BYTES);
    let index = usize::try_from(chunk_index)
        .context("task verification chunk index does not fit in memory")?;
    Ok((offset, length, task_chunk_hash(file, index)?))
}

fn task_transfer_access_policy(file: &TaskTransferFile) -> BoundFileAccessPolicy {
    BoundFileAccessPolicy {
        owner: file.owner,
        group: file.group,
        mode: file.mode,
        link_count: file.link_count.unwrap_or_default(),
    }
}

fn task_file_checkpoint_complete(file: &TaskTransferFile, entry: &TaskFileCheckpoint) -> bool {
    if !entry.published || entry.target_identity.is_none() {
        return false;
    }
    match file.kind {
        TaskTransferEntryKind::Directory => true,
        TaskTransferEntryKind::File => {
            entry.target_verified_chunks == task_verification_chunk_count(file)
        }
    }
}

fn task_parent_directories_ready(
    plan: &TaskTransferPlan,
    entries: &[TaskFileCheckpoint],
    file: &TaskTransferFile,
) -> bool {
    if file.kind == TaskTransferEntryKind::Directory
        && task_staging_job_root(&file.target_relative).unwrap_or(false)
    {
        return true;
    }
    let Some(mut parent) = file.target_relative.parent() else {
        return false;
    };
    loop {
        let depth = parent.components().count();
        if depth < 2 {
            return false;
        }
        let Some((_, checkpoint)) = plan.files.iter().zip(entries).find(|(candidate, _)| {
            candidate.kind == TaskTransferEntryKind::Directory
                && candidate.target_relative == parent
        }) else {
            return false;
        };
        if !checkpoint.published || checkpoint.target_identity.is_none() {
            return false;
        }
        if depth == 2 {
            return task_staging_job_root(parent).unwrap_or(false);
        }
        let Some(next_parent) = parent.parent() else {
            return false;
        };
        parent = next_parent;
    }
}

fn task_staging_job_root(path: &Path) -> Result<bool> {
    let components = path.components().collect::<Vec<_>>();
    if components.len() != 2 {
        return Ok(false);
    }
    let Some(std::path::Component::Normal(root)) = components.first() else {
        return Ok(false);
    };
    let Some(std::path::Component::Normal(job)) = components.get(1) else {
        return Ok(false);
    };
    Ok(root.to_str() == Some(".telegram-video-downloader-staging")
        && job.to_str().is_some_and(|name| name.starts_with("job-")))
}

fn directory_identity(identity: SerializedIdentity) -> EntryIdentity {
    EntryIdentity::directory(identity.device, identity.inode)
}

fn task_staging_name(
    migration_id: &str,
    transfer_id: &str,
    task_index: usize,
    file_index: usize,
    attempt: u32,
) -> String {
    fn safe_component(value: &str) -> String {
        let safe = value
            .bytes()
            .filter(|byte| byte.is_ascii_alphanumeric() || *byte == b'-')
            .take(16)
            .map(char::from)
            .collect::<String>();
        if safe.is_empty() {
            "id".to_string()
        } else {
            safe
        }
    }

    format!(
        "migration-stage-task-{}-{}-{task_index}-{file_index}-{attempt}",
        safe_component(migration_id),
        safe_component(transfer_id)
    )
}

fn safe_queue_record_relative(path: &Path) -> bool {
    if path.as_os_str().len() > 2048 {
        return false;
    }
    let components = path.components().collect::<Vec<_>>();
    if components
        .iter()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return false;
    }
    let component = |index: usize| match components.get(index) {
        Some(std::path::Component::Normal(name)) => name.to_string_lossy(),
        _ => std::borrow::Cow::Borrowed(""),
    };
    (components.len() == 2
        && component(0) == ".telegram-video-downloader-queue"
        && component(1).starts_with("task-")
        && component(1).ends_with(".json"))
        || (components.len() == 3
            && component(0) == ".telegram-video-downloader-queue"
            && component(1) == "history"
            && component(2).starts_with("task-")
            && component(2).ends_with(".json"))
        || (components.len() == 1
            && component(0).starts_with(".telegram-video-downloader-task-")
            && component(0).ends_with(".json"))
}

fn classify_failure(error: &anyhow::Error) -> MigrationFailureKind {
    let message = format!("{error:#}").to_ascii_lowercase();
    if message.contains("source") && message.contains("changed") {
        MigrationFailureKind::SourceChanged
    } else if message.contains("source")
        && (message.contains("missing")
            || message.contains("permission")
            || message.contains("unavailable"))
    {
        MigrationFailureKind::SourceUnavailable
    } else if message.contains("conflict") || message.contains("already exists") {
        MigrationFailureKind::TargetConflict
    } else if message.contains("target") && message.contains("changed") {
        MigrationFailureKind::TargetChanged
    } else if message.contains("space") {
        MigrationFailureKind::InsufficientSpace
    } else if message.contains("verification") || message.contains("failed size/content") {
        MigrationFailureKind::VerificationFailed
    } else if message.contains("publication") || message.contains("publish") {
        MigrationFailureKind::PublicationFailed
    } else if message.contains("cleanup") || message.contains("source file changed before") {
        MigrationFailureKind::CleanupFailed
    } else {
        MigrationFailureKind::CopyFailed
    }
}

fn encode_state(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).context("failed to encode durable migration state")?;
    if bytes.len() > MAX_STATE_BYTES {
        bail!("durable migration state exceeds its size limit");
    }
    Ok(bytes)
}

fn decode_state<T: for<'de> Deserialize<'de>>(bytes: &[u8], label: &str) -> Result<T> {
    serde_json::from_slice(bytes).with_context(|| format!("failed to parse {label}"))
}

fn preview_state_name(id: &str) -> String {
    format!("{PREVIEW_STATE_PREFIX}{id}{STATE_SUFFIX}")
}

fn job_state_name(id: &str) -> String {
    format!("{JOB_STATE_PREFIX}{id}{STATE_SUFFIX}")
}

fn new_migration_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = MIGRATION_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:x}-{:x}-{:x}", std::process::id(), nanos, counter)
}

fn compact_id(id: &str) -> String {
    id.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .take(48)
        .collect()
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(all(test, unix))]
#[path = "library_migration_tests.rs"]
mod migration_fault_tests;
