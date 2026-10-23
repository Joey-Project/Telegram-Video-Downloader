//! Cloud commands and background scheduling for durable media-library migrations.

use std::sync::Mutex as StdMutex;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use super::*;
use crate::file_manager::{LibraryManager, MigrationScanResult};
use crate::library::Presence;
use crate::library_migration::{
    MigrationControl, MigrationDeliveryState, MigrationDiscoveryWarning,
    MigrationDiscoveryWarningKind, MigrationDiscoveryWarnings, MigrationEngine, MigrationPreview,
    MigrationRequest, MigrationRunStatus, MigrationRuntimeContext, MigrationScope, MigrationStatus,
    MigrationTaskImpact, validate_migration_scan_paths,
};
use crate::media_libraries::TaskDestination;
use crate::safe_fs::RootedFs;
use serde::{Deserialize, Serialize};
use serde_json::json;

const MAX_MIGRATION_TASKS: usize = 10_000;

/// Owns one async admission lock per registered library ID. Management takes a shared permit;
/// migration execution and finalization take an exclusive permit.
///
/// Gates are acquired in sorted ID order, so migrations involving two libraries cannot
/// deadlock with another migration involving the same pair in reverse order.
#[derive(Clone, Default)]
pub(crate) struct LibraryIoGateRegistry {
    gates: Arc<Mutex<BTreeMap<String, Arc<RwLock<()>>>>>,
}

enum LibraryIoPermit {
    Management { _guard: OwnedRwLockReadGuard<()> },
    Migration { _guard: OwnedRwLockWriteGuard<()> },
}

pub(crate) struct LibraryIoGuard {
    _guards: Vec<LibraryIoPermit>,
}

pub(crate) type LibraryIoLease = Arc<LibraryIoGuard>;

impl LibraryIoGateRegistry {
    async fn gates_for(&self, library_ids: &[String]) -> Result<Vec<Arc<RwLock<()>>>> {
        let ids = library_ids
            .iter()
            .filter(|library_id| !library_id.is_empty())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if ids.is_empty() {
            bail!("library I/O admission requires at least one library ID");
        }
        let mut gates = self.gates.lock().await;
        Ok(ids
            .into_iter()
            .map(|library_id| {
                Arc::clone(
                    gates
                        .entry(library_id)
                        .or_insert_with(|| Arc::new(RwLock::new(()))),
                )
            })
            .collect())
    }

    pub(crate) async fn acquire(&self, library_ids: &[String]) -> Result<LibraryIoLease> {
        let gates = self.gates_for(library_ids).await?;
        let mut guards = Vec::with_capacity(gates.len());
        for gate in gates {
            guards.push(LibraryIoPermit::Migration {
                _guard: gate.write_owned().await,
            });
        }
        Ok(Arc::new(LibraryIoGuard { _guards: guards }))
    }

    pub(crate) async fn try_acquire(&self, library_ids: &[String]) -> Result<LibraryIoLease> {
        let gates = self.gates_for(library_ids).await?;
        let mut guards = Vec::with_capacity(gates.len());
        for gate in gates {
            match gate.try_read_owned() {
                Ok(guard) => guards.push(LibraryIoPermit::Management { _guard: guard }),
                Err(_) => bail!("this library has an active migration; retry after it finishes"),
            }
        }
        Ok(Arc::new(LibraryIoGuard { _guards: guards }))
    }

    pub(crate) async fn acquire_management(
        &self,
        library_ids: &[String],
        contention_notify: Option<Arc<Notify>>,
    ) -> Result<LibraryIoLease> {
        let gates = self.gates_for(library_ids).await?;
        let mut guards = Vec::with_capacity(gates.len());
        for gate in gates {
            let guard = match Arc::clone(&gate).try_read_owned() {
                Ok(guard) => guard,
                Err(_) => {
                    if let Some(notify) = &contention_notify {
                        notify.notify_one();
                    }
                    gate.read_owned().await
                }
            };
            guards.push(LibraryIoPermit::Management { _guard: guard });
        }
        Ok(Arc::new(LibraryIoGuard { _guards: guards }))
    }
}

pub(crate) async fn try_acquire_library_io(
    runtime: &CloudBotRuntime,
    library_ids: &[String],
) -> Result<LibraryIoLease> {
    runtime.library_io_gates.try_acquire(library_ids).await
}

pub(crate) async fn acquire_management_library_io(
    runtime: &CloudBotRuntime,
    library_ids: &[String],
) -> Result<LibraryIoLease> {
    if runtime.epoch_fatal.is_signaled() {
        bail!("cloud database epoch changed; library publication is stopped");
    }
    #[cfg(test)]
    let contention_notify = Some(Arc::clone(&runtime.test_management_admission_waiting));
    #[cfg(not(test))]
    let contention_notify = None;
    let lease = tokio::select! {
        _ = runtime.epoch_fatal.notified() => {
            bail!("cloud database epoch changed; library publication is stopped");
        }
        lease = runtime.library_io_gates.acquire_management(library_ids, contention_notify) => lease?,
    };
    if runtime.epoch_fatal.is_signaled() {
        drop(lease);
        bail!("cloud database epoch changed; library publication is stopped");
    }
    Ok(lease)
}

pub(crate) async fn acquire_library_io(
    runtime: &CloudBotRuntime,
    library_ids: &[String],
) -> Result<LibraryIoLease> {
    runtime.library_io_gates.acquire(library_ids).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviewPayload {
    source_library_id: String,
    source_root_binding: String,
    target_library_id: String,
    target_root_binding: String,
    scope: MigrationScope,
    #[serde(default)]
    item_ids: Vec<String>,
    #[serde(default)]
    conflict: Option<ConflictPolicy>,
    #[serde(default)]
    item_conflicts: BTreeMap<String, ConflictPolicy>,
    #[serde(default)]
    include_incomplete_tasks: bool,
    #[serde(default)]
    set_target_default_on_success: bool,
    #[serde(default)]
    supersedes_preview_seq: Option<u64>,
    #[serde(default)]
    supersedes_preview_id: Option<String>,
    #[serde(default)]
    supersedes_revision: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BoundMigrationPreview {
    #[serde(flatten)]
    preview: MigrationPreview,
    source_root_binding: String,
    target_root_binding: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmPayload {
    preview_id: String,
    revision: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlPayload {
    migration_id: String,
    control: MigrationControl,
}

fn manager(runtime: &CloudBotRuntime, id: &str) -> Result<Arc<LibraryManager>> {
    // LibraryManager::open can create/update recovery metadata. Refresh the registered queue
    // store first so an offline or identity-replaced path is rejected before opening that root.
    super::refresh_and_require_library_queue_available(&runtime.bot.queue, id)?;
    let (scoped, _) = media_library_bot::scoped_runtime(runtime, Some(id))?;
    Ok(Arc::new(open_cloud_library_manager(
        &scoped.bot,
        Arc::clone(&runtime.inbox),
        None,
    )?))
}

fn manager_for_verified_root(
    runtime: &CloudBotRuntime,
    root: RootedFs,
) -> Result<Arc<LibraryManager>> {
    #[cfg(test)]
    let provider = runtime
        .test_file_provider
        .clone()
        .unwrap_or_else(crate::file_provider::platform_queue_file_provider);
    #[cfg(not(test))]
    let provider = crate::file_provider::platform_queue_file_provider();
    let preview_cleanup_gate: Arc<dyn crate::file_manager::PreviewCleanupGate> =
        runtime.inbox.clone();
    Ok(Arc::new(
        LibraryManager::open_with_bound_root_and_provider_and_preview_cleanup_gate(
            root,
            provider,
            preview_cleanup_gate,
        )?,
    ))
}

fn engine(runtime: &CloudBotRuntime, source: &str, target: &str) -> Result<MigrationEngine> {
    MigrationEngine::new(
        source,
        manager(runtime, source)?,
        target,
        manager(runtime, target)?,
    )
}

#[cfg(test)]
pub(super) async fn discover_for_test(runtime: &CloudBotRuntime) -> Result<()> {
    discover(runtime).await
}

pub(super) fn ensure_library_removable(runtime: &CloudBotRuntime, library_id: &str) -> Result<()> {
    let libraries = runtime.bot.queue.registry_snapshot()?.libraries;
    if !libraries.iter().any(|library| library.id == library_id) {
        return Ok(());
    }
    for library in libraries {
        let source = manager(runtime, &library.id).context(
            "all media libraries must be reachable to verify migration dependencies before unregistering",
        )?;
        for status in MigrationEngine::list_for_source(&source)? {
            if status.source_library_id != library_id && status.target_library_id != library_id {
                continue;
            }
            if !matches!(
                status.status,
                MigrationRunStatus::Completed | MigrationRunStatus::Stopped
            ) {
                bail!(
                    "finish or stop migration {} before removing this library registration",
                    status.id
                );
            }
            // This guard runs inside the settings operation's spawn_blocking worker. The queue
            // scan is bounded and must fail closed when any registered store cannot be proven.
            if runtime
                .bot
                .queue
                .has_pending_task_destination_transfer_for_migration(&status.id)?
            {
                bail!(
                    "finish the pending task destination handoff for migration {} before removing this library registration",
                    status.id
                );
            }
            let engine = engine(
                runtime,
                &status.source_library_id,
                &status.target_library_id,
            )?;
            if status.terminal_finalization_pending {
                bail!(
                    "finish pending terminal migration finalization for migration {} before removing this library registration",
                    status.id
                );
            }
            if engine.task_handoff_cleanup_pending(&status.id, library_id)? {
                bail!(
                    "finish source staging cleanup for migration {} before removing this library registration",
                    status.id
                );
            }
        }
    }
    Ok(())
}

fn task_published_relative_paths(task: &TaskRecord, root: &Path) -> Vec<String> {
    let mut paths = published_primary_relative_paths(task, root);
    if let Some(attempt) = task.bilibili_attempt.as_ref() {
        for entry in &attempt.entries {
            paths.extend(published_entry_primary_relative_paths(entry, root));
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

fn validate_task_scan_scope(
    scan: &MigrationScanResult,
    scope: &MigrationScope,
    task: &TaskRecord,
    root: &Path,
) -> Result<()> {
    let paths = task_published_relative_paths(task, root);
    validate_migration_scan_paths(scan, scope, paths.iter().map(String::as_str))
}

fn validate_task_transfer_scan_scope(
    scan: &MigrationScanResult,
    scope: &MigrationScope,
    plan: &crate::media_libraries::TaskTransferPlan,
) -> Result<()> {
    let paths = plan
        .published_path_rewrites
        .keys()
        .map(|path| {
            let relative = path
                .strip_prefix(&plan.source_root)
                .context("published task path is outside its frozen source root")?;
            relative
                .to_str()
                .context("published task path is not valid UTF-8")
        })
        .collect::<Result<Vec<_>>>()?;
    validate_migration_scan_paths(scan, scope, paths)
}

fn item_ids_for_task(snapshot: &LibrarySnapshot, task: &TaskRecord, root: &Path) -> Vec<String> {
    let paths = task_published_relative_paths(task, root);
    snapshot
        .items
        .iter()
        .filter(|item| paths.contains(&item.relative_path))
        .map(|item| item.id.clone())
        .collect()
}

fn build_preview(
    runtime: &CloudBotRuntime,
    payload: PreviewPayload,
) -> Result<BoundMigrationPreview> {
    let supersession = match (
        payload.supersedes_preview_seq,
        payload.supersedes_preview_id.as_deref(),
        payload.supersedes_revision.as_deref(),
    ) {
        (None, None, None) => None,
        (Some(seq), Some(id), Some(revision))
            if seq > 0 && seq <= 9_007_199_254_740_991 && !revision.is_empty() =>
        {
            let record = runtime
                .inbox
                .record(seq)?
                .context("the migration preview being refreshed is unavailable")?;
            if record.request.kind != "library_migration_preview"
                || record.status != InboxStatus::AwaitingConfirmation
                || record
                    .result
                    .as_ref()
                    .and_then(|result| result.get("id"))
                    .and_then(serde_json::Value::as_str)
                    != Some(id)
                || record
                    .result
                    .as_ref()
                    .and_then(|result| result.get("revision"))
                    .and_then(serde_json::Value::as_str)
                    != Some(revision)
            {
                bail!("the migration preview being refreshed is no longer awaiting confirmation");
            }
            let prior = serde_json::from_value::<BoundMigrationPreview>(
                record
                    .result
                    .clone()
                    .context("the saved migration preview is missing its root identity")?,
            )
            .context("the saved migration preview is missing its root identity")?;
            if prior.preview.source_library_id != payload.source_library_id
                || prior.preview.target_library_id != payload.target_library_id
                || prior.source_root_binding != payload.source_root_binding
                || prior.target_root_binding != payload.target_root_binding
            {
                bail!("the migration preview root identity changed; create a fresh preview");
            }
            Some((seq, id.to_string(), revision.to_string()))
        }
        _ => bail!(
            "supersedes_preview_seq, supersedes_preview_id, and supersedes_revision must be supplied together"
        ),
    };
    let (source_root, target_root) = runtime
        .bot
        .queue
        .verified_video_library_roots_with_bindings(
            &payload.source_library_id,
            &payload.source_root_binding,
            &payload.target_library_id,
            &payload.target_root_binding,
        )?;
    let source = manager_for_verified_root(runtime, source_root)?;
    let target = manager_for_verified_root(runtime, target_root)?;
    let scan = source.migration_scan_with_completeness()?;
    validate_migration_scan_paths(&scan, &payload.scope, std::iter::empty())?;
    let snapshot = &scan.snapshot;
    let tasks = runtime
        .bot
        .queue
        .list_incomplete_task_transfer_snapshots(&payload.source_library_id, MAX_MIGRATION_TASKS)?;
    let mut selected: HashSet<String> = payload.item_ids.iter().cloned().collect();
    let mut impacts = Vec::new();
    let mut expanded = false;
    for task in tasks {
        let paths = task_published_relative_paths(&task.record, &task.destination.root);
        let published = item_ids_for_task(snapshot, &task.record, &task.destination.root);
        let affected = payload.scope == MigrationScope::Library
            || published.iter().any(|id| selected.contains(id));
        if !affected {
            continue;
        }
        validate_migration_scan_paths(&scan, &payload.scope, paths.iter().map(String::as_str))?;
        if task.record.destination_transfer.is_some()
            || task.record.destination_transfer_ack.is_some()
        {
            bail!(
                "an affected task already belongs to another migration; finish or stop that migration first"
            );
        }
        if payload.include_incomplete_tasks {
            for id in &published {
                expanded |= selected.insert(id.clone());
            }
        }
        impacts.push(MigrationTaskImpact {
            task_id: task.record.id,
            generation: task.record.generation,
            revision: task.record.revision,
            staging_bytes: task.staging_bytes,
            staging_file_count: task.staging_file_count,
            published_item_ids: published,
        });
    }
    let mut item_ids = if payload.scope == MigrationScope::Library {
        Vec::new()
    } else {
        selected.into_iter().collect()
    };
    item_ids.sort();
    let mut warnings = Vec::new();
    if !impacts.is_empty() {
        warnings.push("Unfinished-task sizes are estimates; exact retained files are bound after confirmed writers stop.".to_string());
    }
    if expanded && payload.scope == MigrationScope::Selected {
        warnings.push("The selection includes every published member of the affected unfinished task, so its recovery remains in one library.".to_string());
    }
    let engine = MigrationEngine::new(
        payload.source_library_id,
        source,
        payload.target_library_id,
        target,
    )?;
    let request = MigrationRequest {
        scope: payload.scope,
        item_ids,
        conflict: payload.conflict.unwrap_or(ConflictPolicy::Skip),
        item_conflicts: payload.item_conflicts,
        include_incomplete_tasks: payload.include_incomplete_tasks,
        set_target_default_on_success: payload.set_target_default_on_success,
        task_impacts: impacts,
        warnings,
    };
    let preview = match &supersession {
        Some((_, old_id, old_revision)) => {
            engine.preview_replacing(&request, old_id, old_revision)?
        }
        None => engine.preview(&request)?,
    };
    if let Some((seq, id, revision)) = supersession {
        let mut changed = false;
        runtime.inbox.update(seq, |record| {
            if record.request.kind == "library_migration_preview"
                && record.status == InboxStatus::AwaitingConfirmation
                && record
                    .result
                    .as_ref()
                    .and_then(|result| result.get("id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(id.as_str())
                && record
                    .result
                    .as_ref()
                    .and_then(|result| result.get("revision"))
                    .and_then(serde_json::Value::as_str)
                    == Some(revision.as_str())
            {
                record.status = InboxStatus::Stale;
                record.error =
                    Some("Preview is stale; use the refreshed migration preview.".to_string());
                changed = true;
            }
        })?;
        if !changed {
            bail!("the migration preview being refreshed changed before it could be invalidated");
        }
    }
    Ok(BoundMigrationPreview {
        preview,
        source_root_binding: payload.source_root_binding,
        target_root_binding: payload.target_root_binding,
    })
}

async fn publish_status(runtime: &CloudBotRuntime, mut status: MigrationStatus) {
    if matches!(
        status.status,
        MigrationRunStatus::Completed | MigrationRunStatus::Stopped
    ) {
        let queue = Arc::clone(&runtime.bot.queue);
        let migration_id = status.id.clone();
        match tokio::task::spawn_blocking(move || {
            queue.has_pending_task_destination_transfer_for_migration(&migration_id)
        })
        .await
        {
            Ok(Ok(pending)) => status.terminal_finalization_pending |= pending,
            Ok(Err(error)) => {
                warn!(error=%redact_sensitive_text(&format!("{error:#}")), migration_id=%status.id, "could not verify terminal migration queue handoff state");
                mark_terminal_finalization_attention(&mut status);
            }
            Err(error) => {
                warn!(error=%redact_sensitive_text(&format!("{error:#}")), migration_id=%status.id, "could not join terminal migration queue handoff check");
                mark_terminal_finalization_attention(&mut status);
            }
        }
    }
    store_status(runtime, status).await;
}

async fn store_status(runtime: &CloudBotRuntime, status: MigrationStatus) {
    runtime
        .migrations
        .lock()
        .await
        .insert(status.id.clone(), status);
    runtime.state_notify.notify_one();
}

fn mark_terminal_finalization_attention(status: &mut MigrationStatus) {
    status.terminal_finalization_pending = true;
    const WARNING: &str = "Migration needs attention: the participating queue stores could not be checked. Retry terminal finalization after they are available.";
    if !status.warnings.iter().any(|warning| warning == WARNING) {
        status.warnings.push(WARNING.to_string());
    }
}

fn locate_preview_for_source(
    source: &LibraryManager,
    id: &str,
    revision: &str,
    source_library_id: &str,
    target_library_id: &str,
) -> Result<MigrationPreview> {
    let persisted = MigrationEngine::find_preview_for_source(source, id)?
        .context("migration preview is unavailable; create a new preview")?
        .preview;
    if persisted.id != id
        || persisted.revision != revision
        || persisted.source_library_id != source_library_id
        || persisted.target_library_id != target_library_id
    {
        bail!("saved migration preview identity does not match its confirmed inbox result");
    }
    Ok(persisted)
}

fn locate_preview(runtime: &CloudBotRuntime, id: &str) -> Result<MigrationPreview> {
    for library in runtime.bot.queue.registry_snapshot()?.libraries {
        let source = match manager(runtime, &library.id) {
            Ok(source) => source,
            Err(_) => continue,
        };
        if let Some(preview) = MigrationEngine::find_preview_for_source(&source, id)? {
            return Ok(preview.preview);
        }
    }
    bail!("migration preview {id} is unavailable")
}

pub(super) async fn dispatch(
    runtime: &CloudBotRuntime,
    request: &CloudRequest,
) -> Option<Result<CloudDispatchOutcome>> {
    if !matches!(
        request.kind.as_str(),
        "library_migration_preview" | "library_migration_confirm" | "library_migration_control"
    ) {
        return None;
    }
    Some(dispatch_inner(runtime, request).await)
}

async fn dispatch_inner(
    runtime: &CloudBotRuntime,
    request: &CloudRequest,
) -> Result<CloudDispatchOutcome> {
    match request.kind.as_str() {
        "library_migration_preview" => {
            let payload: PreviewPayload = serde_json::from_value(request.payload.clone())?;
            let io_lease = try_acquire_library_io(
                runtime,
                &[
                    payload.source_library_id.clone(),
                    payload.target_library_id.clone(),
                ],
            )
            .await?;
            let publication_guard = Arc::clone(&runtime.library_scan_publication_gate)
                .lock_owned()
                .await;
            let superseded_seq = payload.supersedes_preview_seq;
            let worker = runtime.clone();
            let blocking_io_lease = Arc::clone(&io_lease);
            let (preview, _publication_guard) = tokio::task::spawn_blocking(move || {
                let _io_lease = blocking_io_lease;
                let preview = build_preview(&worker, payload);
                (preview, publication_guard)
            })
            .await?;
            let preview = preview?;
            Ok(CloudDispatchOutcome {
                status: InboxStatus::AwaitingConfirmation,
                result: Some(serde_json::to_value(preview)?),
                error: None,
                related_preview: superseded_seq.map(|seq| (seq, InboxStatus::Stale)),
            })
        }
        "library_migration_confirm" => {
            let payload: ConfirmPayload = serde_json::from_value(request.payload.clone())?;
            let record = cloud_awaiting_confirmation_record(
                runtime,
                payload.preview_id.clone(),
                request.seq,
            )
            .await?
            .context("the migration preview is no longer awaiting confirmation")?;
            if record.request.kind != "library_migration_preview" {
                bail!("a file-operation preview cannot authorize a library migration");
            }
            let bound_preview: BoundMigrationPreview = serde_json::from_value(
                record
                    .result
                    .clone()
                    .context("migration preview result is missing")?,
            )
            .context("saved migration preview has no root binding; create a fresh preview")?;
            let preview = bound_preview.preview;
            if preview.id != payload.preview_id || preview.revision != payload.revision {
                bail!("migration preview ID or revision does not match confirmation");
            }
            let io_lease = try_acquire_library_io(
                runtime,
                &[
                    preview.source_library_id.clone(),
                    preview.target_library_id.clone(),
                ],
            )
            .await?;
            let publication_guard = Arc::clone(&runtime.library_scan_publication_gate)
                .lock_owned()
                .await;
            let worker = runtime.clone();
            let id = payload.preview_id.clone();
            let revision = payload.revision;
            let source_library_id = preview.source_library_id.clone();
            let target_library_id = preview.target_library_id.clone();
            let source_root_binding = bound_preview.source_root_binding;
            let target_root_binding = bound_preview.target_root_binding;
            let blocking_io_lease = Arc::clone(&io_lease);
            let migration = tokio::task::spawn_blocking(move || {
                let _io_lease = blocking_io_lease;
                let (source_root, target_root) = worker
                    .bot
                    .queue
                    .verified_video_library_roots_with_bindings(
                        &source_library_id,
                        &source_root_binding,
                        &target_library_id,
                        &target_root_binding,
                    )?;
                let source = manager_for_verified_root(&worker, source_root)?;
                let target = manager_for_verified_root(&worker, target_root)?;
                let persisted = locate_preview_for_source(
                    &source,
                    &id,
                    &revision,
                    &source_library_id,
                    &target_library_id,
                )?;
                let engine =
                    MigrationEngine::new(&source_library_id, source, &target_library_id, target)?;
                let expected_default_revision = if persisted.set_target_default_on_success {
                    Some(worker.bot.queue.registry_snapshot()?.revision)
                } else {
                    None
                };
                let status = engine.confirm_with_default_revision(
                    &id,
                    &revision,
                    expected_default_revision,
                )?;
                Ok::<_, anyhow::Error>((engine, status, publication_guard))
            })
            .await??;
            let (engine, status, _publication_guard) = migration;
            ensure_message_context(runtime, &engine, &status, record.request.seq).await?;
            publish_status(runtime, status.clone()).await;
            start(runtime.clone(), status.clone()).await;
            Ok(CloudDispatchOutcome {
                status: InboxStatus::Completed,
                result: Some(
                    json!({"preview_id": payload.preview_id,"revision": preview.revision,"migration": status}),
                ),
                error: None,
                related_preview: Some((record.request.seq, InboxStatus::Completed)),
            })
        }
        "library_migration_control" => {
            let payload: ControlPayload = serde_json::from_value(request.payload.clone())?;
            if payload.control == MigrationControl::FinalizeTerminal {
                return finalize_terminal_control(runtime, payload.migration_id).await;
            }
            let worker = runtime.clone();
            let current_epoch = runtime.inbox.database_epoch().to_string();
            let (engine, status, terminal_noop) = tokio::task::spawn_blocking(move || {
                let preview = locate_preview(&worker, &payload.migration_id)?;
                let engine = engine(
                    &worker,
                    &preview.source_library_id,
                    &preview.target_library_id,
                )?;
                // Status validation, epoch refresh, and the control update share the engine's
                // migration lock so a concurrent terminal checkpoint cannot turn a stale
                // ordinary control into terminal finalization authorization.
                let (status, terminal_noop) = engine.control_in_epoch(
                    &payload.migration_id,
                    payload.control,
                    &current_epoch,
                )?;
                Ok::<_, anyhow::Error>((engine, status, terminal_noop))
            })
            .await??;
            let status = read_status(&engine, &status.id).await?.unwrap_or(status);
            publish_status(runtime, status.clone()).await;
            if !terminal_noop {
                start(runtime.clone(), status.clone()).await;
            }
            Ok(CloudDispatchOutcome {
                status: InboxStatus::Completed,
                result: Some(json!({"migration":status})),
                error: None,
                related_preview: None,
            })
        }
        _ => unreachable!(),
    }
}

async fn finalize_terminal_control(
    runtime: &CloudBotRuntime,
    migration_id: String,
) -> Result<CloudDispatchOutcome> {
    let worker = runtime.clone();
    let worker_id = migration_id.clone();
    let current_epoch = runtime.inbox.database_epoch().to_string();
    let (engine, mut status, queue_pending, queue_warning, engine_pending, authorization_current) =
        tokio::task::spawn_blocking(move || {
            let preview = locate_preview(&worker, &worker_id)?;
            let engine = engine(
                &worker,
                &preview.source_library_id,
                &preview.target_library_id,
            )?;
            let current = engine
                .status(&worker_id)?
                .context("migration status is unavailable")?;
            if !matches!(
                current.status,
                MigrationRunStatus::Completed | MigrationRunStatus::Stopped
            ) {
                bail!("terminal finalization is available only for completed or stopped migrations");
            }
            let (queue_pending, queue_warning) = match worker
                .bot
                .queue
                .has_pending_task_destination_transfer_for_migration(&worker_id)
            {
                Ok(pending) => (pending, None),
                Err(error) => {
                    warn!(error=%redact_sensitive_text(&format!("{error:#}")), migration_id=%worker_id, "could not verify queue state before terminal migration finalization");
                    // Even when the engine has pending default/cleanup work, queue state must
                    // be checked successfully before refreshing the epoch. This keeps an
                    // unavailable queue from authorizing any terminal side effect.
                    let warning = "Migration needs attention: the queue transfer state is unavailable. Retry terminal finalization after queue storage is available.";
                    let status = engine.record_finalization_warning(&worker_id, warning)?;
                    return Ok::<_, anyhow::Error>((
                        engine,
                        status,
                        false,
                        Some(warning.to_string()),
                        current.terminal_finalization_pending,
                        false,
                    ));
                }
            };
            let engine_pending = current.terminal_finalization_pending;
            let mut status = engine
                .reauthorize_terminal_finalization(&worker_id, &current_epoch, queue_pending)?
                .context("no terminal migration finalization is pending")?;
            if let Some(warning) = queue_warning.as_deref() {
                status = engine.record_finalization_warning(&worker_id, warning)?;
                status.terminal_finalization_pending = true;
            }
            status.terminal_finalization_pending |= queue_pending;
            Ok::<_, anyhow::Error>((
                engine,
                status,
                queue_pending,
                queue_warning,
                engine_pending,
                true,
            ))
        })
        .await??;

    if let Some(warning) = queue_warning.as_deref() {
        if status.warnings.iter().all(|saved| saved != warning) {
            status.warnings.push(warning.to_string());
        }
        status.terminal_finalization_pending = true;
    }
    store_status(runtime, status.clone()).await;

    let should_finalize = authorization_current && (engine_pending || queue_pending);
    if should_finalize {
        let inserted = runtime
            .migration_runners
            .lock()
            .await
            .insert(migration_id.clone());
        if inserted {
            let worker_runtime = runtime.clone();
            let worker_engine = engine.clone();
            let worker_status = status.clone();
            let worker_id = migration_id.clone();
            let library_ids = vec![
                status.source_library_id.clone(),
                status.target_library_id.clone(),
            ];
            tokio::spawn(async move {
                #[cfg(test)]
                worker_runtime
                    .test_migration_runner_gate_waiting
                    .notify_one();
                let io_lease = match acquire_library_io(&worker_runtime, &library_ids).await {
                    Ok(io_lease) => io_lease,
                    Err(error) => {
                        warn!(migration_id=%worker_id,error=%redact_sensitive_text(&format!("{error:#}")),"could not acquire library admission for terminal migration finalization");
                        worker_runtime
                            .migration_runners
                            .lock()
                            .await
                            .remove(&worker_id);
                        return;
                    }
                };
                #[cfg(test)]
                worker_runtime
                    .test_migration_runner_gate_acquired
                    .notify_one();
                run_terminal_finalizer(
                    worker_runtime,
                    worker_engine,
                    worker_status,
                    worker_id.clone(),
                    io_lease,
                )
                .await;
            });
        }
    }

    Ok(CloudDispatchOutcome {
        status: InboxStatus::Completed,
        result: Some(json!({"migration":status,"accepted":true})),
        error: None,
        related_preview: None,
    })
}

async fn run_terminal_finalizer(
    runtime: CloudBotRuntime,
    engine: MigrationEngine,
    initial: MigrationStatus,
    migration_id: String,
    _io_lease: LibraryIoLease,
) {
    let result = async {
        let mut status = finish_task_transfers(&runtime, &engine, &initial, None).await?;
        if status.status == MigrationRunStatus::Completed {
            apply_success_default(&runtime, &engine, &status).await?;
        }
        if let Some(current) = read_status(&engine, &migration_id).await? {
            status = current;
        }
        edit_status(&runtime, &engine, &status).await?;
        publish_status(&runtime, status).await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(error) = result {
        let warning = redact_sensitive_text(&format!(
            "Migration needs attention: terminal finalization could not finish: {error:#}"
        ));
        warn!(migration_id=%migration_id,error=%warning,"terminal migration finalization needs attention");
        let worker = engine.clone();
        let id = migration_id.clone();
        let saved = warning.clone();
        let saved_status =
            tokio::task::spawn_blocking(move || worker.record_finalization_warning(&id, &saved))
                .await;
        let mut status = match saved_status {
            Ok(Ok(status)) => status,
            Ok(Err(save_error)) => {
                warn!(migration_id=%migration_id,error=%redact_sensitive_text(&format!("{save_error:#}")),"could not persist terminal finalization warning");
                read_status(&engine, &migration_id)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(initial)
            }
            Err(join_error) => {
                warn!(migration_id=%migration_id,error=%redact_sensitive_text(&format!("{join_error:#}")),"could not join terminal finalization warning update");
                read_status(&engine, &migration_id)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(initial)
            }
        };
        if !status.warnings.contains(&warning) {
            status.warnings.push(warning);
        }
        status.terminal_finalization_pending = true;
        edit_status(&runtime, &engine, &status).await.ok();
        publish_status(&runtime, status).await;
    }

    runtime.migration_runners.lock().await.remove(&migration_id);
}

async fn ensure_message_context(
    runtime: &CloudBotRuntime,
    engine: &MigrationEngine,
    status: &MigrationStatus,
    seq: u64,
) -> Result<()> {
    let worker = engine.clone();
    let id = status.id.clone();
    if tokio::task::spawn_blocking(move || worker.runtime_context(&id))
        .await??
        .is_some()
    {
        return Ok(());
    }
    let mut context = MigrationRuntimeContext {
        database_epoch: runtime.inbox.database_epoch().to_string(),
        preview_seq: seq,
        chat_id: configured_cloud_management_chat_id(&runtime.bot.config),
        message_id: None,
        delivery: MigrationDeliveryState::Unknown,
    };
    let worker = engine.clone();
    let id = status.id.clone();
    let saved = context.clone();
    tokio::task::spawn_blocking(move || worker.set_runtime_context(&id, saved)).await??;
    context.message_id = begin_cloud_batch_message(runtime, seq, &status.id).await?;
    if context.message_id.is_some() {
        context.delivery = MigrationDeliveryState::Sent;
    }
    let worker = engine.clone();
    let id = status.id.clone();
    tokio::task::spawn_blocking(move || worker.set_runtime_context(&id, context)).await??;
    Ok(())
}

async fn start(runtime: CloudBotRuntime, status: MigrationStatus) {
    if !matches!(
        status.status,
        MigrationRunStatus::Queued
            | MigrationRunStatus::Running
            | MigrationRunStatus::Stopped
            | MigrationRunStatus::Completed
    ) {
        return;
    }
    if !runtime
        .migration_runners
        .lock()
        .await
        .insert(status.id.clone())
    {
        return;
    }
    tokio::spawn(async move {
        let id = status.id.clone();
        let source = status.source_library_id.clone();
        let library_ids = vec![
            status.source_library_id.clone(),
            status.target_library_id.clone(),
        ];
        #[cfg(test)]
        runtime.test_migration_runner_gate_waiting.notify_one();
        let io_lease = match acquire_library_io(&runtime, &library_ids).await {
            Ok(io_lease) => io_lease,
            Err(error) => {
                warn!(migration_id=%id,error=%redact_sensitive_text(&format!("{error:#}")),"could not acquire library admission for migration runner");
                runtime.migration_runners.lock().await.remove(&id);
                return;
            }
        };
        #[cfg(test)]
        runtime.test_migration_runner_gate_acquired.notify_one();
        if let Err(error) = run(&runtime, status, Arc::clone(&io_lease)).await {
            warn!(migration_id=%id,error=%redact_sensitive_text(&format!("{error:#}")),"media-library migration paused for recovery");
            if let Some(kind) =
                crate::library_migration::migration_record_warning_kind(&format!("{error:#}"))
            {
                let record_name = if matches!(
                    kind,
                    MigrationDiscoveryWarningKind::SourceUnavailable
                        | MigrationDiscoveryWarningKind::MaintenanceFailed
                        | MigrationDiscoveryWarningKind::EnumerationFailed
                ) {
                    String::new()
                } else {
                    format!("migration-job-{id}.json")
                };
                runtime
                    .migration_discovery_warnings
                    .lock()
                    .await
                    .push(MigrationDiscoveryWarning::new(&source, &record_name, kind));
                runtime.state_notify.notify_one();
            }
            let worker = runtime.clone();
            let lookup = id.clone();
            let diagnostic =
                redact_sensitive_text(&format!("Migration needs attention: {error:#}"));
            let paused = tokio::task::spawn_blocking(move || {
                let source = manager(&worker, &source)?;
                MigrationEngine::pause_for_source(&source, &lookup, &diagnostic)
            })
            .await;
            if let Ok(Ok((status, context))) = paused {
                if let Some(context) = context {
                    edit_status_with_context(&runtime, &context, &status).await;
                }
                publish_status(&runtime, status).await;
            } else if let Some(status) = runtime.migrations.lock().await.get_mut(&id) {
                status.status = MigrationRunStatus::Paused;
                status.warnings.push(redact_sensitive_text(&format!(
                    "Migration needs attention: {error:#}. The runner stopped, but its pause could not be saved; reconnect the source library before continuing."
                )));
            }
            runtime.state_notify.notify_one();
        }
        runtime.migration_runners.lock().await.remove(&id);
        drop(io_lease);
    });
}

pub(super) async fn watch(runtime: CloudBotRuntime) {
    let mut tick = interval(Duration::from_secs(15));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! { _=runtime.epoch_fatal.notified()=>return, _=tick.tick()=>{} }
        if let Err(error) = discover(&runtime).await {
            warn!(error=%redact_sensitive_text(&format!("{error:#}")),"could not refresh durable library migrations");
        }
    }
}

async fn discover(runtime: &CloudBotRuntime) -> Result<()> {
    let worker = runtime.clone();
    let discovered = tokio::task::spawn_blocking(move || {
        let mut discovered = MigrationDiscoveryWarnings::default();
        let libraries = match worker.bot.queue.registry_snapshot() {
            Ok(snapshot) => snapshot.libraries,
            Err(error) => {
                warn!(error=%redact_sensitive_text(&format!("{error:#}")), "media-library registry could not be enumerated for migration discovery");
                discovered.push(MigrationDiscoveryWarning::new(
                    "",
                    "",
                    MigrationDiscoveryWarningKind::EnumerationFailed,
                ));
                return (Vec::new(), discovered);
            }
        };
        let mut statuses = Vec::new();
        for library in libraries {
            let source = match manager(&worker, &library.id) {
                Ok(source) => source,
                Err(error) => {
                    warn!(library_id=%library.id, error=%redact_sensitive_text(&format!("{error:#}")), "media-library root is unavailable during migration discovery");
                    discovered.push(MigrationDiscoveryWarning::new(
                        &library.id,
                        "",
                        MigrationDiscoveryWarningKind::SourceUnavailable,
                    ));
                    continue;
                }
            };
            match MigrationEngine::maintain_source(&source) {
                Ok(report) => {
                    if let Some(warning) = report.warning {
                        warn!(library_id=%library.id, warning=%redact_sensitive_text(&warning), "media-library preview maintenance needs attention");
                        discovered.push(MigrationDiscoveryWarning::new(
                            &library.id,
                            "",
                            MigrationDiscoveryWarningKind::MaintenanceFailed,
                        ));
                    }
                }
                Err(error) => {
                    warn!(library_id=%library.id, error=%redact_sensitive_text(&format!("{error:#}")), "media-library preview maintenance could not complete");
                    discovered.push(MigrationDiscoveryWarning::new(
                        &library.id,
                        "",
                        MigrationDiscoveryWarningKind::MaintenanceFailed,
                    ));
                }
            }
            let report = MigrationEngine::list_for_discovery(&source, &library.id);
            statuses.extend(report.statuses.iter().cloned());
            discovered.include_report(report);
        }
        (statuses, discovered)
    })
    .await;
    let discovered = match discovered {
        Ok(discovered) => discovered,
        Err(error) => {
            let mut warnings = MigrationDiscoveryWarnings::default();
            warnings.push(MigrationDiscoveryWarning::new(
                "",
                "",
                MigrationDiscoveryWarningKind::EnumerationFailed,
            ));
            *runtime.migration_discovery_warnings.lock().await = warnings;
            return Err(error).context("migration discovery worker failed");
        }
    };
    *runtime.migration_discovery_warnings.lock().await = discovered.1;
    for status in discovered.0 {
        publish_status(runtime, status.clone()).await;
        start(runtime.clone(), status).await;
    }
    Ok(())
}

async fn edit_status(
    runtime: &CloudBotRuntime,
    engine: &MigrationEngine,
    status: &MigrationStatus,
) -> Result<()> {
    let worker = engine.clone();
    let id = status.id.clone();
    let context = tokio::task::spawn_blocking(move || worker.runtime_context(&id)).await??;
    if let Some(context) = context {
        edit_status_with_context(runtime, &context, status).await;
    }
    Ok(())
}

async fn edit_status_with_context(
    runtime: &CloudBotRuntime,
    context: &MigrationRuntimeContext,
    status: &MigrationStatus,
) {
    if context.database_epoch == runtime.inbox.database_epoch()
        && context.chat_id == configured_cloud_management_chat_id(&runtime.bot.config)
        && let (Some(chat), Some(message)) = (context.chat_id, context.message_id)
    {
        let mut text = format!(
            "Library migration {}: {:?}\n{} completed, {} skipped, {} failed; {} / {} copied.\nSee /file for actual locations and recovery controls.",
            status.id,
            status.status,
            status.success_count,
            status.skipped_count,
            status.failed_count,
            human_bytes(status.copied_bytes),
            human_bytes(status.total_bytes)
        );
        if let Some(warning) = status
            .warnings
            .iter()
            .rev()
            .find(|warning| warning.starts_with("Migration needs attention:"))
        {
            text.push_str(&format!("\n{}", truncate(warning)));
        }
        if let Err(error) = runtime
            .bot
            .telegram
            .edit_message_text(chat, message, truncate(&text))
            .await
        {
            warn!(error=%error,"could not update library migration Telegram status");
        }
    }
}

async fn run(
    runtime: &CloudBotRuntime,
    initial: MigrationStatus,
    io_lease: LibraryIoLease,
) -> Result<()> {
    let worker = runtime.clone();
    let source_id = initial.source_library_id.clone();
    let target_id = initial.target_library_id.clone();
    let engine =
        tokio::task::spawn_blocking(move || engine(&worker, &source_id, &target_id)).await??;
    let id = initial.id.clone();
    let worker = engine.clone();
    let lookup = id.clone();
    let context = tokio::task::spawn_blocking(move || worker.runtime_context(&lookup)).await??;
    if context
        .as_ref()
        .is_none_or(|context| context.database_epoch != runtime.inbox.database_epoch())
    {
        let worker = engine.clone();
        let lookup = id.clone();
        let status =
            tokio::task::spawn_blocking(move || worker.control(&lookup, MigrationControl::Pause))
                .await??;
        publish_status(runtime, status).await;
        return Ok(());
    }
    if initial.status == MigrationRunStatus::Stopped {
        let status = finish_task_transfers(runtime, &engine, &initial, None).await?;
        let status =
            upgrade_legacy_target_birth_once(runtime, &engine, &status, Arc::clone(&io_lease))
                .await;
        publish_status(runtime, status).await;
        return Ok(());
    }
    if initial.status == MigrationRunStatus::Completed {
        let status = finish_task_transfers(runtime, &engine, &initial, None).await?;
        let status =
            upgrade_legacy_target_birth_once(runtime, &engine, &status, Arc::clone(&io_lease))
                .await;
        if status.status == MigrationRunStatus::Completed {
            apply_success_default(runtime, &engine, &status).await?;
        }
        publish_status(runtime, status).await;
        return Ok(());
    }
    // Task writers must acknowledge cancellation before either output lease is acquired.
    if !prepare_task_transfers(runtime, &engine, &initial).await? {
        if let Some(status) = read_status(&engine, &id).await? {
            let status = finish_task_transfers(runtime, &engine, &status, None).await?;
            publish_status(runtime, status).await;
        }
        return Ok(());
    }
    let settings = runtime.bot.queue.registry_snapshot()?;
    let mut roots = settings
        .libraries
        .into_iter()
        .filter(|library| {
            library.id == initial.source_library_id || library.id == initial.target_library_id
        })
        .map(|library| library.root)
        .collect::<Vec<_>>();
    roots.sort();
    let acquire = downloader::hold_video_outputs_for_migration(&roots);
    tokio::pin!(acquire);
    let guards = loop {
        tokio::select! {
            _=runtime.epoch_fatal.notified()=>return Ok(()),
            result=&mut acquire=>break result?,
            _=sleep(Duration::from_millis(250))=>{
                if let Some(status)=read_status(&engine,&id).await?
                    && !matches!(status.status,MigrationRunStatus::Queued|MigrationRunStatus::Running)
                {
                    let status = finish_task_transfers(runtime,&engine,&status,None).await?;
                    publish_status(runtime,status).await;
                    return Ok(());
                }
            }
        }
    };
    let guards = Arc::new(StdMutex::new(guards));
    attach_task_plans(runtime, &engine, &initial, Arc::clone(&guards)).await?;
    let mut last_edit = Instant::now() - Duration::from_secs(5);
    loop {
        if runtime.epoch_fatal.is_signaled() {
            return Ok(());
        }
        let worker = engine.clone();
        let lookup = id.clone();
        let lease = Arc::clone(&guards);
        let status = tokio::task::spawn_blocking(move || {
            let lease = lease
                .lock()
                .map_err(|_| anyhow::anyhow!("migration output lease is poisoned"))?;
            worker.step_with_output_lease(&lookup, &lease)
        })
        .await??;
        if status.awaiting_task_commit {
            commit_task_transfers(runtime, &engine, &status).await?;
        }
        let terminal = !matches!(
            status.status,
            MigrationRunStatus::Queued | MigrationRunStatus::Running
        );
        runtime
            .migrations
            .lock()
            .await
            .insert(status.id.clone(), status.clone());
        // Copy checkpoints may complete faster than a cloud round trip. Keep local progress
        // current, but coalesce snapshot uploads and Telegram edits to a bounded cadence.
        if terminal || last_edit.elapsed() >= Duration::from_secs(5) {
            runtime.state_notify.notify_one();
            edit_status(runtime, &engine, &status).await?;
            last_edit = Instant::now();
        }
        if terminal {
            let status =
                finish_task_transfers(runtime, &engine, &status, Some(Arc::clone(&guards))).await?;
            let status =
                upgrade_legacy_target_birth_once(runtime, &engine, &status, Arc::clone(&io_lease))
                    .await;
            if status.status == MigrationRunStatus::Completed {
                apply_success_default(runtime, &engine, &status).await?;
            }
            publish_status(runtime, status).await;
            drop(guards);
            runtime.bot.queue.publication_notify().notify_one();
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}

async fn upgrade_legacy_target_birth_once(
    runtime: &CloudBotRuntime,
    engine: &MigrationEngine,
    status: &MigrationStatus,
    io_lease: LibraryIoLease,
) -> MigrationStatus {
    if !matches!(
        status.status,
        MigrationRunStatus::Completed | MigrationRunStatus::Stopped
    ) {
        return status.clone();
    }
    let unavailable = runtime.library_unavailable.lock().await;
    if unavailable.contains_key(&status.target_library_id) {
        return status.clone();
    }
    drop(unavailable);
    let target_snapshot = runtime
        .library_snapshots
        .lock()
        .await
        .get(&status.target_library_id)
        .cloned();
    let Some(target_snapshot) = target_snapshot else {
        return status.clone();
    };

    let worker = engine.clone();
    let id = status.id.clone();
    let target_id = status.target_library_id.clone();
    let queue = Arc::clone(&runtime.bot.queue);
    let retained_io_lease = Arc::clone(&io_lease);
    match tokio::task::spawn_blocking(move || {
        let _io_lease = retained_io_lease;
        let availability = queue.video_library_statuses()?;
        if !availability
            .get(&target_id)
            .is_some_and(|library| library.available)
        {
            return Ok(None);
        }
        worker
            .upgrade_one_legacy_target_birth(&id, &target_snapshot)
            .map(Some)
    })
    .await
    {
        Ok(Ok(Some(upgrade))) => {
            if upgrade.remaining {
                tracing::debug!(migration_id=%status.id, "legacy target verification will continue on a later discovery pass");
            }
            if upgrade.attempted {
                runtime.state_notify.notify_one();
            }
            upgrade.status
        }
        Ok(Ok(None)) => status.clone(),
        Ok(Err(error)) => {
            // A failed read, unavailable provider, or CAS race leaves the job status intact.
            // Another discovery pass can retry once the target scan is usable.
            warn!(migration_id=%status.id,error=%redact_sensitive_text(&format!("{error:#}")),"legacy migration target proof upgrade did not complete");
            status.clone()
        }
        Err(error) => {
            warn!(migration_id=%status.id,error=%error,"legacy migration target proof worker failed");
            status.clone()
        }
    }
}

async fn read_status(engine: &MigrationEngine, id: &str) -> Result<Option<MigrationStatus>> {
    let worker = engine.clone();
    let id = id.to_string();
    tokio::task::spawn_blocking(move || worker.status(&id)).await?
}

async fn apply_success_default(
    runtime: &CloudBotRuntime,
    engine: &MigrationEngine,
    status: &MigrationStatus,
) -> Result<()> {
    if status.status == MigrationRunStatus::Completed
        && status.set_target_default_on_success
        && !status.default_update_resolved
        && !status.default_update_applied
    {
        let id = status.id.clone();
        let expected_revision = engine.default_update_expected_revision(&id)?;
        let Some(expected_revision) = expected_revision else {
            let worker = engine.clone();
            tokio::task::spawn_blocking(move || {
                worker.mark_default_update_resolved(
                    &id,
                    false,
                    Some("The default library was not changed because this completed migration has no saved settings revision."),
                )
            })
            .await??;
            if let Some(status) = read_status(engine, &status.id).await? {
                publish_status(runtime, status).await;
            }
            return Ok(());
        };
        let queue = Arc::clone(&runtime.bot.queue);
        let target = status.target_library_id.clone();
        let migration_id = id.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            queue.set_default_video_library_for_migration_if_revision(
                &target,
                &migration_id,
                expected_revision,
            )
        })
        .await??;
        let (applied, warning) = match outcome {
            crate::media_libraries::MigrationDefaultUpdateOutcome::Applied { .. } => (true, None),
            crate::media_libraries::MigrationDefaultUpdateOutcome::RevisionChanged { .. } => (
                false,
                Some(
                    "The requested default library was left unchanged because library settings changed during migration.",
                ),
            ),
        };
        let worker = engine.clone();
        tokio::task::spawn_blocking(move || {
            worker.mark_default_update_resolved(&id, applied, warning)
        })
        .await??;
        if let Some(status) = read_status(engine, &status.id).await? {
            publish_status(runtime, status).await;
        }
    }
    Ok(())
}

// Queue-transfer coordination follows below. The durable engine owns file checkpoints;
// QueueManager alone owns queue record/index installation and token restoration.

async fn task_plans(
    engine: &MigrationEngine,
    id: &str,
) -> Result<Vec<crate::media_libraries::TaskTransferPlan>> {
    let worker = engine.clone();
    let id = id.to_string();
    tokio::task::spawn_blocking(move || worker.task_transfer_plans(&id)).await?
}

async fn prepare_task_transfers(
    runtime: &CloudBotRuntime,
    engine: &MigrationEngine,
    status: &MigrationStatus,
) -> Result<bool> {
    if !status.include_incomplete_tasks {
        return Ok(true);
    }
    let worker = engine.clone();
    let id = status.id.clone();
    let impacts = tokio::task::spawn_blocking(move || worker.task_impacts(&id)).await??;
    let attached = task_plans(engine, &status.id).await?;
    let worker = engine.clone();
    let id = status.id.clone();
    let transfer_statuses =
        tokio::task::spawn_blocking(move || worker.task_transfer_statuses(&id)).await??;
    let acknowledged = transfer_statuses
        .iter()
        .filter(|transfer| transfer.commit_acked)
        .map(|transfer| transfer.transfer_id.as_str())
        .collect::<HashSet<_>>();
    let cleanup_completed = transfer_statuses
        .iter()
        .filter(|transfer| transfer.source_cleanup_complete)
        .map(|transfer| transfer.transfer_id.as_str())
        .collect::<HashSet<_>>();
    let worker = runtime.clone();
    let source = status.source_library_id.clone();
    let target_id = status.target_library_id.clone();
    let (scan, tasks, target) = tokio::task::spawn_blocking(move || {
        let scan = manager(&worker, &source)?.migration_scan_with_completeness()?;
        let tasks = worker
            .bot
            .queue
            .list_incomplete_task_transfer_snapshots(&source, MAX_MIGRATION_TASKS)?;
        let target = worker
            .bot
            .queue
            .registry_snapshot()?
            .libraries
            .into_iter()
            .find(|library| library.id == target_id)
            .context("target library is no longer registered")?;
        Ok::<_, anyhow::Error>((scan, tasks, target))
    })
    .await??;
    validate_migration_scan_paths(&scan, &status.scope, std::iter::empty())?;
    let approved_paths = status
        .items
        .iter()
        .flat_map(|item| item.files.iter().map(|file| file.source_path.as_str()));
    validate_migration_scan_paths(&scan, &status.scope, approved_paths)?;
    let snapshot = &scan.snapshot;
    let approved: HashSet<_> = status
        .items
        .iter()
        .map(|item| item.item_id.as_str())
        .collect();
    for impact in impacts {
        if let Some(current) = read_status(engine, &status.id).await?
            && !matches!(
                current.status,
                MigrationRunStatus::Queued | MigrationRunStatus::Running
            )
        {
            return Ok(false);
        }
        // A crash may occur after the target queue record is installed but before the
        // migration checkpoint acknowledges it. Reuse that exact persisted handoff;
        // the task no longer belongs to the source-only incomplete-task listing.
        if let Some(plan) = attached
            .iter()
            .find(|plan| plan.transfer.task_id == impact.task_id)
        {
            if cleanup_completed.contains(plan.transfer.transfer_id.as_str()) {
                continue;
            }
            validate_task_transfer_scan_scope(&scan, &status.scope, plan)?;
            let queue = Arc::clone(&runtime.bot.queue);
            let plan = plan.clone();
            let acknowledged = acknowledged.contains(plan.transfer.transfer_id.as_str());
            let worker = engine.clone();
            let installed = tokio::task::spawn_blocking(move || {
                let Some(task) = queue.get(&plan.transfer.task_id)? else {
                    return Ok(false);
                };
                if task.destination.as_ref() != Some(&plan.transfer.target) {
                    return Ok(false);
                }
                if task.destination_transfer_ack.as_ref() == Some(&plan.transfer) {
                    if task.destination_transfer.is_some()
                        || task.destination_transfer_phase.is_some()
                    {
                        bail!("queue ACK receipt coexists with an active task transfer token");
                    }
                    if !acknowledged {
                        worker.ack_task_transfer(
                            &plan.transfer.migration_id,
                            &plan.transfer.transfer_id,
                        )?;
                    }
                    return Ok::<_, anyhow::Error>(true);
                }
                if task.destination_transfer_phase
                    != Some(crate::media_libraries::TaskDestinationTransferPhase::TargetInstalled)
                    || task.destination_transfer.as_ref() != Some(&plan.transfer)
                {
                    bail!("installed task does not match its durable migration handoff");
                }
                if !acknowledged {
                    queue.verify_task_transfer_plan(&plan)?;
                }
                Ok::<_, anyhow::Error>(true)
            })
            .await??;
            if installed {
                continue;
            }
        }
        let task = tasks
            .iter()
            .find(|task| task.record.id == impact.task_id)
            .context("an unfinished task changed or completed; create a fresh migration preview")?;
        validate_task_scan_scope(&scan, &status.scope, &task.record, &task.destination.root)?;
        if task.record.destination_transfer.is_none() && task.record.generation != impact.generation
        {
            bail!("unfinished task generation changed; create a fresh migration preview");
        }
        let current_ids = item_ids_for_task(snapshot, &task.record, &task.destination.root);
        if current_ids.iter().any(|id| !approved.contains(id.as_str())) {
            bail!(
                "an unfinished task published media outside the approved preview; create a fresh preview"
            );
        }
        let queue = Arc::clone(&runtime.bot.queue);
        let task = task.clone();
        let migration_id = status.id.clone();
        let destination = TaskDestination {
            library_id: target.id.clone(),
            root: target.root.clone(),
            category: task.destination.category.clone(),
        };
        let token = tokio::task::spawn_blocking(move || {
            queue.begin_task_destination_transfer(
                &task.record.id,
                &migration_id,
                task.record.generation,
                task.record.revision,
                destination,
            )
        })
        .await??;
        let settled = runtime.bot.queue.wait_for_task_destination_transfer(&token);
        tokio::pin!(settled);
        loop {
            tokio::select! {
                _=runtime.epoch_fatal.notified()=>return Ok(false),
                result=&mut settled=>{result?;break;},
                _=sleep(Duration::from_millis(250))=>{
                    if let Some(current)=read_status(engine,&status.id).await?
                        && !matches!(current.status,MigrationRunStatus::Queued|MigrationRunStatus::Running) {return Ok(false);}
                }
            }
        }
    }
    Ok(true)
}

async fn attach_task_plans(
    runtime: &CloudBotRuntime,
    engine: &MigrationEngine,
    status: &MigrationStatus,
    output_lease: Arc<StdMutex<downloader::VideoMigrationGuard>>,
) -> Result<()> {
    if !status.include_incomplete_tasks {
        return Ok(());
    }
    let worker = engine.clone();
    let id = status.id.clone();
    let impacts = tokio::task::spawn_blocking(move || worker.task_impacts(&id)).await??;
    let attached = task_plans(engine, &status.id).await?;
    let worker = runtime.clone();
    let source_id = status.source_library_id.clone();
    let scan = tokio::task::spawn_blocking(move || {
        let source = manager(&worker, &source_id)?;
        let lease = output_lease
            .lock()
            .map_err(|_| anyhow::anyhow!("migration output lease is poisoned"))?;
        source.migration_scan_with_output_lease_and_completeness(&lease)
    })
    .await??;
    let plan_paths = status
        .items
        .iter()
        .flat_map(|item| item.files.iter().map(|file| file.source_path.as_str()));
    validate_migration_scan_paths(&scan, &status.scope, plan_paths)?;
    let snapshot = &scan.snapshot;
    let approved = status
        .items
        .iter()
        .map(|item| item.item_id.clone())
        .collect::<HashSet<_>>();
    // Cancellation and dirty-publication recovery can finish work after the initial
    // task scan. Bind task plans only after checking the settled, leased source view.
    // A resumed job may already have removed approved media; only extra present media
    // is a scope expansion, so missing completed members do not invalidate recovery.
    if status.scope == MigrationScope::Library
        && snapshot
            .items
            .iter()
            .any(|item| item.presence == Presence::Present && !approved.contains(&item.id))
    {
        bail!(
            "source library gained media outside the approved preview; create a fresh migration preview"
        );
    }
    let libraries = runtime.bot.queue.registry_snapshot()?.libraries;
    let source_root = libraries
        .iter()
        .find(|library| library.id == status.source_library_id)
        .context("source library is no longer registered")?
        .root
        .clone();
    let target_root = libraries
        .iter()
        .find(|library| library.id == status.target_library_id)
        .context("target library is no longer registered")?
        .root
        .clone();
    for impact in impacts {
        if let Some(plan) = attached
            .iter()
            .find(|plan| plan.transfer.task_id == impact.task_id)
        {
            validate_task_transfer_scan_scope(&scan, &status.scope, plan)?;
            continue;
        }
        let queue = Arc::clone(&runtime.bot.queue);
        let id = impact.task_id.clone();
        let snapshot = scan.snapshot.clone();
        let scan = scan.clone();
        let scope = status.scope.clone();
        let approved = approved.clone();
        let source = source_root.clone();
        let rewrites = status
            .items
            .iter()
            .filter(|item| impact.published_item_ids.contains(&item.item_id))
            .flat_map(|item| item.files.iter())
            .map(|file| {
                (
                    source_root.join(&file.source_path),
                    target_root.join(&file.target_path),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let plan = tokio::task::spawn_blocking(move || {
            let task = queue
                .get(&id)?
                .context("task disappeared while migration was acquiring output leases")?;
            validate_task_scan_scope(&scan, &scope, &task, &source)?;
            if item_ids_for_task(&snapshot, &task, &source)
                .iter()
                .any(|id| !approved.contains(id))
            {
                bail!("an unfinished task published media outside the approved preview; create a fresh migration preview");
            }
            let token = task
                .destination_transfer
                .context("task transfer ownership disappeared")?;
            queue.task_transfer_plan_with_published_paths(&token, &rewrites)
        })
        .await??;
        let worker = engine.clone();
        let id = status.id.clone();
        tokio::task::spawn_blocking(move || worker.attach_task_transfer(&id, plan)).await??;
    }
    Ok(())
}

async fn commit_task_transfers(
    runtime: &CloudBotRuntime,
    engine: &MigrationEngine,
    status: &MigrationStatus,
) -> Result<()> {
    for plan in task_plans(engine, &status.id).await? {
        let queue = Arc::clone(&runtime.bot.queue);
        let saved = plan.clone();
        tokio::task::spawn_blocking(move || queue.commit_task_destination_transfer(&saved))
            .await??;
        let worker = engine.clone();
        let id = status.id.clone();
        let transfer = plan.transfer.transfer_id;
        tokio::task::spawn_blocking(move || worker.ack_task_transfer(&id, &transfer)).await??;
    }
    Ok(())
}

async fn restore_task(runtime: &CloudBotRuntime, task: Option<TaskRecord>) -> Result<()> {
    if let Some(task) = task
        && task.status == TaskStatus::Queued
    {
        let id = task.id.clone();
        let generation = task.generation;
        let context = task_bot_context(runtime.bot.clone(), &id, generation).await?;
        let job_id = context.next_job_id.fetch_add(1, Ordering::Relaxed);
        queue_queued_task(
            context,
            task.chat_id,
            job_id,
            id,
            task.job,
            JobRunMode::StagedKeepBoth,
            Some(generation),
        )
        .await;
    }
    Ok(())
}

async fn finish_task_transfers(
    runtime: &CloudBotRuntime,
    engine: &MigrationEngine,
    status: &MigrationStatus,
    held_lease: Option<Arc<StdMutex<downloader::VideoMigrationGuard>>>,
) -> Result<MigrationStatus> {
    if !matches!(
        status.status,
        MigrationRunStatus::Completed | MigrationRunStatus::Stopped
    ) {
        return Ok(status.clone());
    }
    let worker = engine.clone();
    let id = status.id.clone();
    let current_epoch = runtime.inbox.database_epoch().to_string();
    let authorized = tokio::task::spawn_blocking(move || {
        Ok::<_, anyhow::Error>(
            worker
                .runtime_context(&id)?
                .is_some_and(|context| context.database_epoch == current_epoch),
        )
    })
    .await??;
    if !authorized {
        // A terminal record from another database epoch is visible but cannot authorize queue
        // rescheduling or destructive source cleanup until a current-epoch control is received.
        return Ok(status.clone());
    }
    let worker = engine.clone();
    let id = status.id.clone();
    let (plans, impacts, transfer_statuses) = tokio::task::spawn_blocking(move || {
        Ok::<_, anyhow::Error>((
            worker.task_transfer_plans(&id)?,
            worker.task_impacts(&id)?,
            worker.task_transfer_statuses(&id)?,
        ))
    })
    .await??;
    let plan_task_ids = plans
        .iter()
        .map(|plan| plan.transfer.task_id.as_str())
        .collect::<HashSet<_>>();

    // Confirmation may be stopped after `begin_task_destination_transfer` but before its plan
    // reaches the migration manifest. Such an uninstalled token is safe to abort; a completed
    // job or an installed target without its exact plan is an attention state, never an inferred
    // acknowledgment.
    if status.include_incomplete_tasks {
        for impact in impacts
            .iter()
            .filter(|impact| !plan_task_ids.contains(impact.task_id.as_str()))
        {
            let queue = Arc::clone(&runtime.bot.queue);
            let task_id = impact.task_id.clone();
            let migration_id = status.id.clone();
            let stopped = status.status == MigrationRunStatus::Stopped;
            let resolution = tokio::task::spawn_blocking(move || {
                let Some(current) = queue.get(&task_id)? else {
                    return Ok::<_, anyhow::Error>(UnplannedTransferResolution::NoRecord);
                };
                let Some(token) = current.destination_transfer.clone() else {
                    if let Some(receipt) = current.destination_transfer_ack.as_ref() {
                        return Ok(UnplannedTransferResolution::Attention {
                            transfer_id: Some(receipt.transfer_id.clone()),
                            message: "queue handoff receipt has no matching migration file plan".to_string(),
                        });
                    }
                    if stopped {
                        return Ok(UnplannedTransferResolution::NoRecord);
                    }
                    return Ok(UnplannedTransferResolution::Attention {
                        transfer_id: None,
                        message: "completed migration has an affected task with no durable transfer plan or queue acknowledgment".to_string(),
                    });
                };
                if token.migration_id != migration_id {
                    return Ok(UnplannedTransferResolution::Attention {
                        transfer_id: None,
                        message: "task transfer is owned by a different migration".to_string(),
                    });
                }
                let installed = current.destination.as_ref() == Some(&token.target)
                    && current.destination_transfer_phase
                        == Some(crate::media_libraries::TaskDestinationTransferPhase::TargetInstalled);
                if stopped && !installed {
                    return Ok(UnplannedTransferResolution::Aborted(Box::new(
                        queue.abort_task_destination_transfer(&token)?,
                    )));
                }
                Ok(UnplannedTransferResolution::Attention {
                    transfer_id: Some(token.transfer_id),
                    message: "task transfer reached a terminal migration without its exact saved file plan".to_string(),
                })
            })
            .await??;
            match resolution {
                UnplannedTransferResolution::Aborted(task) => {
                    restore_task(runtime, *task).await?;
                }
                UnplannedTransferResolution::Attention {
                    transfer_id: Some(transfer_id),
                    message,
                } => {
                    return mark_task_handoff_attention(
                        runtime,
                        engine,
                        status,
                        &transfer_id,
                        &message,
                    )
                    .await;
                }
                UnplannedTransferResolution::Attention {
                    transfer_id: None,
                    message,
                } => {
                    return mark_task_handoff_migration_attention(engine, status, &message).await;
                }
                UnplannedTransferResolution::NoRecord => {}
            }
        }
    }

    for plan in &plans {
        let transfer = plan.transfer.clone();
        let transfer_status = transfer_statuses
            .iter()
            .find(|saved| saved.transfer_id == transfer.transfer_id);
        let Some(transfer_status) = transfer_status else {
            return mark_task_handoff_attention(
                runtime,
                engine,
                status,
                &transfer.transfer_id,
                "migration manifest has no transfer status for its saved task plan",
            )
            .await;
        };
        let commit_acked = transfer_status.commit_acked;
        let worker = engine.clone();
        let migration_id = status.id.clone();
        let transfer_id = transfer.transfer_id.clone();
        let queue = Arc::clone(&runtime.bot.queue);
        let stopped = status.status == MigrationRunStatus::Stopped;
        let plan = plan.clone();
        let resolution = tokio::task::spawn_blocking(move || {
            let final_acknowledged =
                worker.task_handoff_final_acknowledged(&migration_id, &transfer_id)?;
            let current = queue.get(&plan.transfer.task_id)?;
            if final_acknowledged {
                if let Some(record) = current.as_ref()
                    && record
                        .destination_transfer_ack
                        .as_ref()
                        .is_some_and(|saved| saved != &plan.transfer)
                {
                    return Ok::<_, anyhow::Error>(TaskHandoffResolution::Attention(
                        "task has a different unconsumed transfer acknowledgment".to_string(),
                    ));
                }
                if current
                    .as_ref()
                    .is_some_and(|record| record.destination_transfer.is_some())
                {
                    return Ok(TaskHandoffResolution::Attention(
                        "engine final acknowledgment exists while the queue still has a pending transfer token".to_string(),
                    ));
                }
                return Ok(TaskHandoffResolution::Finalized);
            }
            let Some(current) = current else {
                return Ok(TaskHandoffResolution::Attention(
                    "task record is absent and no durable queue acknowledgment proves its handoff".to_string(),
                ));
            };
            if let Some(ack) = current.destination_transfer_ack.as_ref()
                && ack != &plan.transfer
            {
                return Ok(TaskHandoffResolution::Attention(
                    "task has a different unconsumed transfer acknowledgment".to_string(),
                ));
            }
            if let Some(token) = current.destination_transfer.as_ref() {
                if token != &plan.transfer {
                    return Ok(TaskHandoffResolution::Attention(
                        "task destination transfer does not match its saved migration plan".to_string(),
                    ));
                }
                let installed = current.destination.as_ref() == Some(&plan.transfer.target)
                    && current.destination_transfer_phase
                        == Some(crate::media_libraries::TaskDestinationTransferPhase::TargetInstalled);
                if !installed {
                    if stopped && !commit_acked {
                        return Ok(TaskHandoffResolution::Aborted(Box::new(
                            queue.abort_task_destination_transfer(&plan.transfer)?,
                        )));
                    }
                    return Ok(TaskHandoffResolution::Attention(
                        "completed migration task has not installed its target queue record".to_string(),
                    ));
                }
                if !commit_acked {
                    queue.verify_task_transfer_plan(&plan)?;
                    queue.commit_task_destination_transfer(&plan)?;
                    worker.ack_task_transfer(&migration_id, &transfer_id)?;
                }
                let acknowledged = queue
                    .ack_task_destination_transfer(&plan.transfer)?
                    .ok_or_else(|| anyhow::anyhow!("target task disappeared during queue acknowledgment"))?;
                if acknowledged.destination_transfer_ack.as_ref() != Some(&plan.transfer) {
                    return Ok(TaskHandoffResolution::Attention(
                        "queue did not retain the exact handoff acknowledgment receipt".to_string(),
                    ));
                }
                worker.ack_task_handoff_final(&migration_id, &transfer_id)?;
                return Ok(TaskHandoffResolution::Finalized);
            }

            if current
                .destination_transfer_ack
                .as_ref()
                .is_some_and(|ack| ack == &plan.transfer)
                && current.destination.as_ref() == Some(&plan.transfer.target)
            {
                // The exact receipt survives a crash between queue ACK and engine ACK. It is
                // sufficient to replay the engine's earlier target-install acknowledgment.
                if !commit_acked {
                    worker.ack_task_transfer(&migration_id, &transfer_id)?;
                }
                worker.ack_task_handoff_final(&migration_id, &transfer_id)?;
                return Ok(TaskHandoffResolution::Finalized);
            }

            if stopped
                && !commit_acked
                && current.destination.as_ref() == Some(&plan.transfer.source)
            {
                return Ok(TaskHandoffResolution::Aborted(Box::new(Some(current))));
            }
            Ok(TaskHandoffResolution::Attention(
                "task has neither the exact pending transfer nor its durable queue acknowledgment receipt".to_string(),
            ))
        })
        .await??;

        match resolution {
            TaskHandoffResolution::Aborted(task) => {
                restore_task(runtime, *task).await?;
            }
            TaskHandoffResolution::Attention(message) => {
                return mark_task_handoff_attention(
                    runtime,
                    engine,
                    status,
                    &transfer.transfer_id,
                    &message,
                )
                .await;
            }
            TaskHandoffResolution::Finalized => {}
        }
    }

    let mut current_status = read_status(engine, &status.id)
        .await?
        .unwrap_or_else(|| status.clone());
    let cleanup_pending = match task_handoff_cleanup_pending(engine, &current_status).await {
        Ok(pending) => pending,
        Err(error) => {
            return mark_task_handoff_migration_attention(
                engine,
                &current_status,
                &format!("could not verify whether source staging cleanup remains: {error:#}"),
            )
            .await;
        }
    };
    if cleanup_pending {
        let lease = match held_lease {
            Some(lease) => lease,
            None => {
                match acquire_task_cleanup_lease(runtime, &current_status).await {
                    Ok(lease) => lease,
                    Err(_) if runtime.epoch_fatal.is_signaled() => return Ok(current_status),
                    Err(error) => {
                        return mark_task_handoff_migration_attention(
                        engine,
                        &current_status,
                        &format!("could not acquire output lease for source staging cleanup: {error:#}"),
                    )
                    .await;
                    }
                }
            }
        };
        let mut last_update = Instant::now();
        loop {
            let pending_statuses = task_transfer_statuses(engine, &current_status.id).await?;
            let pending_ids = pending_statuses
                .iter()
                .filter(|transfer| transfer.commit_acked && !transfer.source_cleanup_complete)
                .map(|transfer| transfer.transfer_id.clone())
                .collect::<Vec<_>>();
            if pending_ids.is_empty() {
                return mark_task_handoff_migration_attention(
                    engine,
                    &current_status,
                    "migration reports cleanup pending but has no unfinished saved task cleanup checkpoint",
                )
                .await;
            }
            for transfer_id in pending_ids {
                let worker = engine.clone();
                let migration_id = current_status.id.clone();
                let worker_transfer_id = transfer_id.clone();
                let lease = Arc::clone(&lease);
                let result = tokio::task::spawn_blocking(move || {
                    let lease = lease
                        .lock()
                        .map_err(|_| anyhow::anyhow!("migration output lease is poisoned"))?;
                    worker.step_task_source_cleanup_with_output_lease(
                        &migration_id,
                        &worker_transfer_id,
                        &lease,
                    )
                })
                .await?;
                current_status = match result {
                    Ok(status) => status,
                    Err(error) => {
                        return mark_task_handoff_attention(
                            runtime,
                            engine,
                            &current_status,
                            &transfer_id,
                            &format!("source staging cleanup needs attention: {error:#}"),
                        )
                        .await;
                    }
                };
                runtime
                    .migrations
                    .lock()
                    .await
                    .insert(current_status.id.clone(), current_status.clone());
                if current_status.status == MigrationRunStatus::Partial {
                    runtime.state_notify.notify_one();
                    return Ok(current_status);
                }
                if last_update.elapsed() >= Duration::from_secs(5) {
                    runtime.state_notify.notify_one();
                    edit_status(runtime, engine, &current_status).await?;
                    last_update = Instant::now();
                }
                tokio::task::yield_now().await;
            }
            let still_pending = match task_handoff_cleanup_pending(engine, &current_status).await {
                Ok(pending) => pending,
                Err(error) => {
                    return mark_task_handoff_migration_attention(
                        engine,
                        &current_status,
                        &format!("could not verify source staging cleanup progress: {error:#}"),
                    )
                    .await;
                }
            };
            if !still_pending {
                break;
            }
            if runtime.epoch_fatal.is_signaled() {
                return Ok(current_status);
            }
        }
    }

    // Keep each exact queue receipt until all source payload, control, and directory checkpoints
    // are durable. This keeps normal queue actions and task startup from racing source cleanup.
    for plan in plans {
        let queue = Arc::clone(&runtime.bot.queue);
        let transfer = plan.transfer.clone();
        let task = tokio::task::spawn_blocking(move || {
            let Some(current) = queue.get(&transfer.task_id)? else {
                return Ok::<_, anyhow::Error>(None);
            };
            if current
                .destination_transfer_ack
                .as_ref()
                .is_some_and(|saved| saved == &transfer)
            {
                queue.clear_task_destination_transfer_ack(&transfer)?;
                return queue.get(&transfer.task_id);
            }
            if current.destination_transfer_ack.is_some() {
                bail!("task has a different handoff receipt after source cleanup");
            }
            Ok(None)
        })
        .await??;
        restore_task(runtime, task).await?;
    }
    if let Some(status) = read_status(engine, &status.id).await? {
        Ok(status)
    } else {
        Ok(current_status)
    }
}

#[derive(Debug)]
enum UnplannedTransferResolution {
    NoRecord,
    Aborted(Box<Option<TaskRecord>>),
    Attention {
        transfer_id: Option<String>,
        message: String,
    },
}

#[derive(Debug)]
enum TaskHandoffResolution {
    Finalized,
    Aborted(Box<Option<TaskRecord>>),
    Attention(String),
}

async fn task_transfer_statuses(
    engine: &MigrationEngine,
    migration_id: &str,
) -> Result<Vec<crate::library_migration::MigrationTaskTransferStatus>> {
    let worker = engine.clone();
    let id = migration_id.to_string();
    tokio::task::spawn_blocking(move || worker.task_transfer_statuses(&id)).await?
}

async fn task_handoff_cleanup_pending(
    engine: &MigrationEngine,
    status: &MigrationStatus,
) -> Result<bool> {
    let worker = engine.clone();
    let id = status.id.clone();
    let source_library_id = status.source_library_id.clone();
    tokio::task::spawn_blocking(move || {
        worker.task_handoff_cleanup_pending(&id, &source_library_id)
    })
    .await?
}

async fn acquire_task_cleanup_lease(
    runtime: &CloudBotRuntime,
    status: &MigrationStatus,
) -> Result<Arc<StdMutex<downloader::VideoMigrationGuard>>> {
    let queue = Arc::clone(&runtime.bot.queue);
    let source = status.source_library_id.clone();
    let target = status.target_library_id.clone();
    let roots: Vec<PathBuf> = tokio::task::spawn_blocking(move || {
        let libraries = queue.registry_snapshot()?.libraries;
        let mut roots = Vec::with_capacity(2);
        for id in [&source, &target] {
            roots.push(
                libraries
                    .iter()
                    .find(|library| library.id == *id)
                    .map(|library| library.root.clone())
                    .ok_or_else(|| {
                        anyhow::anyhow!("migration library {id} is no longer registered")
                    })?,
            );
        }
        roots.sort();
        roots.dedup();
        if roots.len() != 2 {
            bail!("migration cleanup roots no longer identify two distinct libraries");
        }
        Ok::<_, anyhow::Error>(roots)
    })
    .await??;
    let guard = tokio::select! {
        _=runtime.epoch_fatal.notified()=>bail!("application is stopping before task handoff cleanup can acquire its output lease"),
        result=downloader::hold_video_outputs_for_migration(&roots)=>result?,
    };
    Ok(Arc::new(StdMutex::new(guard)))
}

async fn mark_task_handoff_attention(
    runtime: &CloudBotRuntime,
    engine: &MigrationEngine,
    status: &MigrationStatus,
    transfer_id: &str,
    message: &str,
) -> Result<MigrationStatus> {
    let worker = engine.clone();
    let migration_id = status.id.clone();
    let transfer_id = transfer_id.to_string();
    let message = message.to_string();
    let status = tokio::task::spawn_blocking(move || {
        worker.mark_task_handoff_attention(&migration_id, &transfer_id, &message)
    })
    .await??;
    runtime
        .migrations
        .lock()
        .await
        .insert(status.id.clone(), status.clone());
    runtime.state_notify.notify_one();
    Ok(status)
}

async fn mark_task_handoff_migration_attention(
    engine: &MigrationEngine,
    status: &MigrationStatus,
    message: &str,
) -> Result<MigrationStatus> {
    let worker = engine.clone();
    let migration_id = status.id.clone();
    let message = message.to_string();
    tokio::task::spawn_blocking(move || {
        worker.mark_task_handoff_migration_attention(&migration_id, &message)
    })
    .await?
}

#[cfg(test)]
mod completeness_tests {
    use std::collections::BTreeSet;

    use super::*;

    fn scan(complete: bool, incomplete: &[&str], scope_unknown: bool) -> MigrationScanResult {
        MigrationScanResult {
            snapshot: LibrarySnapshot {
                revision: "revision".to_string(),
                scanned_at: 0,
                root_label: "source".to_string(),
                items: Vec::new(),
                categories: Vec::new(),
                warnings: Vec::new(),
            },
            complete,
            incomplete_directories: incomplete
                .iter()
                .map(|path| (*path).to_string())
                .collect::<BTreeSet<_>>(),
            scope_unknown,
        }
    }

    fn task_with_published_path(path: &str) -> TaskRecord {
        let mut task = TaskRecord::new(
            "task-1".to_string(),
            1,
            1,
            42,
            None,
            0,
            JobRequest::Youtube {
                url: "https://youtu.be/fixture".to_string(),
            },
        );
        task.primary_media_hashes
            .insert(path.to_string(), "fixture-hash".to_string());
        task
    }

    #[test]
    fn migration_bot_task_scan_rejects_published_paths_in_incomplete_subtrees() {
        let task = task_with_published_path("Selected/clip.mp4");
        let scan = scan(false, &["Selected"], false);

        let error = validate_task_scan_scope(
            &scan,
            &MigrationScope::Selected,
            &task,
            Path::new("/source"),
        )
        .expect_err("a published task path inside an incomplete subtree must fail closed");
        assert!(error.to_string().contains("incomplete source directory"));
    }

    #[test]
    fn migration_bot_task_scan_allows_unrelated_incomplete_subtrees_by_component() {
        let task = task_with_published_path("Selected-archive/clip.mp4");
        let scan = scan(false, &["Selected"], false);

        validate_task_scan_scope(
            &scan,
            &MigrationScope::Selected,
            &task,
            Path::new("/source"),
        )
        .expect("an incomplete sibling path must not overlap by string prefix");
    }

    #[test]
    fn migration_bot_task_scan_requires_complete_library_and_known_selected_scope() {
        let task = task_with_published_path("Selected/clip.mp4");
        let localized = scan(false, &["Unrelated"], false);
        assert!(
            validate_task_scan_scope(
                &localized,
                &MigrationScope::Library,
                &task,
                Path::new("/source"),
            )
            .is_err()
        );

        let unknown = scan(false, &[], true);
        assert!(
            validate_task_scan_scope(
                &unknown,
                &MigrationScope::Selected,
                &task,
                Path::new("/source"),
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod library_io_gate_tests {
    use super::*;

    #[tokio::test]
    async fn library_io_gate_is_scoped_to_sorted_unique_library_ids() {
        let gates = LibraryIoGateRegistry::default();
        let first_management = gates
            .try_acquire(&["library-b".to_string(), "library-a".to_string()])
            .await
            .expect("management should acquire shared permits for both libraries");

        let second_management = gates
            .try_acquire(&["library-a".to_string()])
            .await
            .expect("same-library management operations may queue on publication");
        let unrelated_management = gates
            .try_acquire(&["library-b".to_string()])
            .await
            .expect("a second management operation may share the read permit");

        drop(first_management);
        drop(second_management);
        drop(unrelated_management);

        let migration = gates
            .acquire(&["library-b".to_string(), "library-a".to_string()])
            .await
            .expect("migration should acquire exclusive permits for both libraries");
        let active_migration = gates
            .try_acquire(&["library-a".to_string()])
            .await
            .err()
            .expect("management should fail promptly while migration is active");
        assert!(active_migration.to_string().contains("active migration"));
        let unrelated = gates
            .try_acquire(&["library-c".to_string()])
            .await
            .expect("an unrelated library should remain available during migration");
        drop(unrelated);
        drop(migration);

        gates
            .try_acquire(&[
                "library-a".to_string(),
                "library-b".to_string(),
                "library-a".to_string(),
            ])
            .await
            .expect("released, duplicate IDs should be acquired once in sorted order");
    }

    #[tokio::test]
    async fn reversed_multi_library_waiters_do_not_deadlock() {
        let gates = LibraryIoGateRegistry::default();
        let left = gates.clone();
        let right = gates;
        let waiters = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(
                async {
                    left.acquire(&["library-a".to_string(), "library-b".to_string()])
                        .await
                        .expect("forward-order waiter should acquire both gates");
                },
                async {
                    right
                        .acquire(&["library-b".to_string(), "library-a".to_string()])
                        .await
                        .expect("reverse-order waiter should acquire both gates");
                },
            )
        })
        .await;
        assert!(
            waiters.is_ok(),
            "sorted gate acquisition should not deadlock"
        );
    }
}
