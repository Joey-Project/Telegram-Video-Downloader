use super::*;

use crate::downloader::{
    VideoStagingCleanupLease, VideoStagingCleanupRecoveryStatus, VideoStagingCleanupSnapshot,
    try_hold_video_staging_cleanup,
};
use crate::staging_cleanup::{
    CleanupFailure, CleanupPreview, CleanupProof, CleanupResult, MAX_STAGING_CLEANUP_ITEMS,
    MAX_STAGING_CLEANUP_PATH_BYTES, MAX_STAGING_CLEANUP_PREVIEW_BYTES,
    MAX_STAGING_CLEANUP_SCAN_BYTES, MAX_STAGING_CLEANUP_TITLE_BYTES, StagingItem, StagingScan,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_STAGING_CLEANUP_QUEUE_RECORDS: usize = 20_000;
const MAX_STAGING_CLEANUP_QUEUE_RECORD_BYTES: usize = 64 * 1024 * 1024;
const MAX_STAGING_CLEANUP_ASSOCIATIONS: usize = 100_000;
const MAX_STAGING_CLEANUP_INVENTORY_ENTRIES: usize = 100_000;
const MAX_STAGING_CLEANUP_SCAN_WARNINGS: usize = 24;
const MAX_STAGING_CLEANUP_SCAN_WARNING_BYTES: usize = 256;
const MAX_STAGING_CLEANUP_SCAN_MILLIS: u64 = 15_000;
static STAGING_CLEANUP_PREVIEW_COUNTER: AtomicU64 = AtomicU64::new(0);

struct IndexedStagingRecord {
    record: TaskRecord,
    base_reason: Option<String>,
    permitted: bool,
}

struct StagingCandidate {
    item: StagingItem,
    proof: Option<CleanupProof>,
}

struct StagingCandidateSet {
    items: Vec<StagingCandidate>,
    warnings: Vec<String>,
    truncated: bool,
    // Queue-to-staging ownership only; directory inventory limits are tracked by `truncated`.
    ownership_complete: bool,
}

impl QueueManager {
    pub(crate) fn scan_staging_cleanup(
        &self,
        library_id: &str,
        root_binding: &str,
        allowed_chat_ids: &[i64],
    ) -> Result<StagingScan> {
        self.scan_staging_cleanup_with_inventory_limit(
            library_id,
            root_binding,
            allowed_chat_ids,
            MAX_STAGING_CLEANUP_INVENTORY_ENTRIES,
        )
    }

    #[cfg(test)]
    pub(crate) fn scan_staging_cleanup_with_inventory_limit_for_test(
        &self,
        library_id: &str,
        root_binding: &str,
        allowed_chat_ids: &[i64],
        inventory_entry_limit: usize,
    ) -> Result<StagingScan> {
        self.scan_staging_cleanup_with_inventory_limit(
            library_id,
            root_binding,
            allowed_chat_ids,
            inventory_entry_limit,
        )
    }

    fn scan_staging_cleanup_with_inventory_limit(
        &self,
        library_id: &str,
        root_binding: &str,
        allowed_chat_ids: &[i64],
        inventory_entry_limit: usize,
    ) -> Result<StagingScan> {
        let initial_store = self.staging_cleanup_store(library_id, root_binding)?;
        let lease = try_hold_video_staging_cleanup(&initial_store.root)?;
        let _operation = self.operation_lock.lock().map_err(poisoned_lock)?;
        let (library, store) =
            self.staging_cleanup_library_store_unlocked(library_id, root_binding)?;
        require_same_cleanup_root(&initial_store, &store, &lease)?;
        let _queue_operations = store.lock_queue_operations()?;
        lease.validate()?;
        let deadline = Instant::now() + Duration::from_millis(MAX_STAGING_CLEANUP_SCAN_MILLIS);
        let recovery_status = lease.staging_cleanup_recovery_status_until(deadline)?;

        let candidate_set = self.collect_staging_candidates(
            &library,
            &store,
            allowed_chat_ids,
            &lease,
            &recovery_status,
            deadline,
            inventory_entry_limit,
        )?;
        let mut items = candidate_set
            .items
            .into_iter()
            .map(|candidate| candidate.item)
            .collect::<Vec<_>>();
        let mut warnings = candidate_set.warnings;
        let mut truncated = candidate_set.truncated;
        if items.len() > MAX_STAGING_CLEANUP_ITEMS {
            items.truncate(MAX_STAGING_CLEANUP_ITEMS);
            truncated = true;
            push_cleanup_warning(
                &mut warnings,
                "The staging list exceeded its item limit; refresh after narrowing the library.",
            );
        }

        let mut scan = StagingScan {
            library_id: library_id.to_string(),
            root_binding: root_binding.to_string(),
            revision: String::new(),
            scanned_at: staging_cleanup_now_millis(),
            items,
            warnings,
            truncated,
        };
        scan.revision = staging_scan_revision(&scan)?;
        fit_staging_scan_to_budget(&mut scan)?;
        lease.validate()?;
        Ok(scan)
    }

    pub(crate) fn preview_staging_cleanup(
        &self,
        library_id: &str,
        root_binding: &str,
        allowed_chat_ids: &[i64],
        item_ids: &[String],
    ) -> Result<CleanupPreview> {
        self.preview_staging_cleanup_with_inventory_limit(
            library_id,
            root_binding,
            allowed_chat_ids,
            item_ids,
            MAX_STAGING_CLEANUP_INVENTORY_ENTRIES,
        )
    }

    #[cfg(test)]
    pub(crate) fn preview_staging_cleanup_with_inventory_limit_for_test(
        &self,
        library_id: &str,
        root_binding: &str,
        allowed_chat_ids: &[i64],
        item_ids: &[String],
        inventory_entry_limit: usize,
    ) -> Result<CleanupPreview> {
        self.preview_staging_cleanup_with_inventory_limit(
            library_id,
            root_binding,
            allowed_chat_ids,
            item_ids,
            inventory_entry_limit,
        )
    }

    fn preview_staging_cleanup_with_inventory_limit(
        &self,
        library_id: &str,
        root_binding: &str,
        allowed_chat_ids: &[i64],
        item_ids: &[String],
        inventory_entry_limit: usize,
    ) -> Result<CleanupPreview> {
        validate_staging_cleanup_selection(item_ids)?;
        let initial_store = self.staging_cleanup_store(library_id, root_binding)?;
        let lease = try_hold_video_staging_cleanup(&initial_store.root)?;
        let _operation = self.operation_lock.lock().map_err(poisoned_lock)?;
        let (library, store) =
            self.staging_cleanup_library_store_unlocked(library_id, root_binding)?;
        require_same_cleanup_root(&initial_store, &store, &lease)?;
        let _queue_operations = store.lock_queue_operations()?;
        lease.validate()?;
        let deadline = Instant::now() + Duration::from_millis(MAX_STAGING_CLEANUP_SCAN_MILLIS);
        let recovery_status = lease.staging_cleanup_recovery_status_until(deadline)?;
        if !recovery_status.is_safe() {
            bail!(
                "{} Staging cleanup preview is unavailable.",
                recovery_status
                    .reason()
                    .unwrap_or("Video output recovery is unresolved.")
            );
        }

        let candidate_set = self.collect_staging_candidates(
            &library,
            &store,
            allowed_chat_ids,
            &lease,
            &recovery_status,
            deadline,
            inventory_entry_limit,
        )?;
        if !candidate_set.ownership_complete {
            bail!(
                "could not verify every authorized queue record; refresh the staging scan before previewing"
            );
        }

        let mut by_id = candidate_set
            .items
            .into_iter()
            .map(|candidate| (candidate.item.id.clone(), candidate))
            .collect::<HashMap<_, _>>();
        let mut selected_items = Vec::with_capacity(item_ids.len());
        let mut proofs = Vec::with_capacity(item_ids.len());
        let mut total_bytes = 0_u64;
        for id in item_ids {
            let candidate = by_id.remove(id).ok_or_else(|| {
                anyhow!(
                    "selected staging directory is unavailable or outside the bounded inventory; \
                     refresh and select a displayed row"
                )
            })?;
            let Some(proof) = candidate.proof else {
                bail!(
                    "selected staging directory is protected or no longer eligible; refresh the scan"
                );
            };
            total_bytes = total_bytes
                .checked_add(proof.bytes)
                .ok_or_else(|| anyhow!("selected staging byte total is too large"))?;
            selected_items.push(candidate.item);
            proofs.push(proof);
        }

        let revision =
            staging_preview_revision(library_id, root_binding, &selected_items, &proofs)?;
        let created_at = staging_cleanup_now_millis();
        let unique = STAGING_CLEANUP_PREVIEW_COUNTER.fetch_add(1, Ordering::Relaxed);
        let id = staging_cleanup_digest(&(
            "staging-cleanup-preview-id-v1",
            library_id,
            root_binding,
            revision.as_str(),
            created_at,
            unique,
        ))?;
        let preview = CleanupPreview::new(
            format!("scp1-{id}"),
            revision,
            library_id.to_string(),
            root_binding.to_string(),
            created_at,
            selected_items,
            total_bytes,
            proofs,
        );
        let encoded = serde_json::to_vec(&preview)
            .context("failed to serialize the complete staging cleanup plan")?;
        if encoded.len() > MAX_STAGING_CLEANUP_PREVIEW_BYTES {
            bail!(
                "the complete cleanup preview exceeds its durable size limit; select fewer staging directories"
            );
        }
        lease.validate()?;
        Ok(preview)
    }

    pub(crate) fn confirm_staging_cleanup(
        &self,
        preview: &CleanupPreview,
        allowed_chat_ids: &[i64],
    ) -> Result<CleanupResult> {
        validate_saved_staging_preview(preview)?;
        if allowed_chat_ids.is_empty() {
            bail!("staging cleanup is unavailable without an authorized chat");
        }
        let initial_store =
            self.staging_cleanup_store(&preview.library_id, &preview.root_binding)?;
        let mut lease = try_hold_video_staging_cleanup(&initial_store.root)?;
        let _operation = self.operation_lock.lock().map_err(poisoned_lock)?;
        let (library, store) = self
            .staging_cleanup_library_store_unlocked(&preview.library_id, &preview.root_binding)?;
        require_same_cleanup_root(&initial_store, &store, &lease)?;
        let _queue_operations = store.lock_queue_operations()?;
        lease.validate()?;
        let deadline = Instant::now() + Duration::from_millis(MAX_STAGING_CLEANUP_SCAN_MILLIS);
        let recovery_status = lease.staging_cleanup_recovery_status_until(deadline)?;
        if !recovery_status.is_safe() {
            bail!(
                "{} Staging cleanup confirmation is unavailable.",
                recovery_status
                    .reason()
                    .unwrap_or("Video output recovery is unresolved.")
            );
        }

        let (records, warnings, truncated, ownership_complete) = read_indexed_staging_records(
            self,
            &store,
            &library.root,
            &library.id,
            allowed_chat_ids,
            deadline,
        )?;
        if !ownership_complete {
            bail!("the authorized queue ownership scan is incomplete; this preview is stale");
        }
        let associations =
            build_complete_staging_associations(&records, &store.root, &store.root_path, deadline)?;
        let records_by_task = records
            .iter()
            .enumerate()
            .map(|(index, indexed)| (indexed.record.id.as_str(), index))
            .collect::<HashMap<_, _>>();
        let index = store.read_index()?;
        let _ = (warnings, truncated);
        let mut removal_jobs = Vec::with_capacity(preview.items.len());
        let mut removal_identity = HashMap::with_capacity(preview.items.len());
        let mut current_items = Vec::with_capacity(preview.items.len());
        let mut current_proofs = Vec::with_capacity(preview.items.len());
        let mut total_bytes = 0_u64;
        let mut already_removed = Vec::with_capacity(preview.items.len());
        let mut seen_paths = HashSet::with_capacity(preview.items.len());
        let pending_quarantines = lease
            .pending_cleanup_quarantine_relatives()?
            .into_iter()
            .collect::<HashSet<_>>();

        for (item, proof) in preview.items.iter().zip(preview.proofs()) {
            if Instant::now() >= deadline {
                bail!("cleanup confirmation exceeded its bounded queue scan time");
            }
            if !seen_paths.insert(proof.relative_path.clone()) || item.id.is_empty() {
                bail!("the retained cleanup plan contains duplicate or invalid selections");
            }
            let Some(task_indices) = associations.get(Path::new(&proof.relative_path)) else {
                bail!("the selected staging directory is no longer recorded by a task");
            };
            if task_indices.len() != 1
                || records[task_indices[0]].record.id != proof.task_id
                || !records[task_indices[0]].permitted
            {
                bail!("the selected staging directory has ambiguous or unauthorized ownership");
            }
            let record_index = *records_by_task
                .get(proof.task_id.as_str())
                .ok_or_else(|| anyhow!("the selected task disappeared from the ownership scan"))?;
            let indexed = &records[record_index];
            let record = &indexed.record;
            if indexed.base_reason.is_some()
                || record.generation != proof.task_generation
                || record.revision != proof.task_revision
                || record.chat_id != proof.task_chat_id
                || record.status != proof.task_status
                || record.id != proof.task_id
                || index.tasks.get(&proof.task_id).is_none_or(|entry| {
                    entry.move_target.is_some()
                        || entry.chat_id != proof.task_chat_id
                        || entry.revision != proof.task_revision
                        || entry.updated_at != record.updated_at
                })
            {
                bail!("the task is active or protected from staging cleanup");
            }

            if !record_staging_jobs(&store.root, &store.root_path, record)?
                .contains(&PathBuf::from(&proof.relative_path))
            {
                bail!("the task no longer records the selected staging directory");
            }
            total_bytes = total_bytes
                .checked_add(proof.bytes)
                .ok_or_else(|| anyhow!("selected staging byte total is too large"))?;
            let relative = PathBuf::from(&proof.relative_path);
            let absolute = lease.root().logical_root_path().join(&relative);
            if lease.root().entry_identity(&absolute)?.is_none() {
                if pending_quarantines.contains(&relative) {
                    bail!("the selected staging directory is pending cleanup recovery");
                }
                current_items.push(item.clone());
                current_proofs.push(proof.clone());
                already_removed.push(item.id.clone());
                continue;
            }

            let snapshot = lease
                .snapshot_job_for_cleanup(Path::new(&proof.relative_path), &recovery_status)?;
            let fresh_item = staging_item_for(record, &proof.relative_path, Some(&snapshot), None)?;
            let fresh_proof = cleanup_proof_for(record, &proof.relative_path, &snapshot);
            if &fresh_item != item || &fresh_proof != proof {
                bail!("a selected staging directory or its queue record changed after preview");
            }
            current_items.push(fresh_item);
            current_proofs.push(fresh_proof);
            removal_identity.insert(relative.clone(), (item.id.clone(), snapshot.bytes));
            removal_jobs.push((relative, snapshot));
        }

        if total_bytes != preview.total_bytes
            || staging_preview_revision(
                &preview.library_id,
                &preview.root_binding,
                &current_items,
                &current_proofs,
            )? != preview.revision
        {
            bail!("the cleanup plan no longer matches the selected staging directories");
        }
        lease.validate()?;

        let removals = lease.remove_jobs(&removal_jobs, &recovery_status);
        let mut result = CleanupResult {
            removed: already_removed,
            ..CleanupResult::default()
        };
        for (relative, outcome) in removals {
            let Some((id, bytes)) = removal_identity.remove(&relative) else {
                continue;
            };
            match outcome {
                Ok(()) => {
                    result.removed.push(id);
                    result.bytes = result.bytes.saturating_add(bytes);
                }
                Err(error) => result.failed.push(CleanupFailure {
                    id,
                    error: safe_cleanup_failure_reason(&error),
                }),
            }
        }
        if !result.removed.is_empty() {
            self.state_change_notify.notify_waiters();
        }
        Ok(result)
    }

    fn staging_cleanup_store(&self, library_id: &str, root_binding: &str) -> Result<DownloadStore> {
        self.require_video_library_root_binding_unlocked(library_id, root_binding)?;
        let (library, root_identity) = {
            let registry = self.media_libraries.lock().map_err(poisoned_lock)?;
            let library = registry
                .library(library_id)
                .cloned()
                .ok_or_else(|| anyhow!("unknown media library {library_id}"))?;
            let identity = registry
                .root_binding(library_id)
                .ok_or_else(|| anyhow!("media library root identity is unavailable"))?;
            (library, identity)
        };
        let store = self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .find(|registered| registered.library_id == library_id && registered.available)
            .map(|registered| registered.store.clone())
            .ok_or_else(|| anyhow!("media library {library_id} is currently unavailable"))?;
        if (
            store.root.root_identity().device(),
            store.root.root_identity().inode(),
        ) != root_identity
            || !same_trusted_root_path(&store.root_path, &library.root)?
        {
            bail!("media library root identity changed; refresh settings and retry");
        }
        store.root.validate_configured_root()?;
        Ok(store)
    }

    fn staging_cleanup_library_store_unlocked(
        &self,
        library_id: &str,
        root_binding: &str,
    ) -> Result<(MediaLibrary, DownloadStore)> {
        self.require_video_library_root_binding_unlocked(library_id, root_binding)?;
        let (library, root_identity) = {
            let registry = self.media_libraries.lock().map_err(poisoned_lock)?;
            let library = registry
                .library(library_id)
                .cloned()
                .ok_or_else(|| anyhow!("unknown media library {library_id}"))?;
            let identity = registry
                .root_binding(library_id)
                .ok_or_else(|| anyhow!("media library root identity is unavailable"))?;
            (library, identity)
        };
        let store = self
            .video_libraries
            .read()
            .map_err(poisoned_lock)?
            .iter()
            .find(|registered| registered.library_id == library_id && registered.available)
            .map(|registered| registered.store.clone())
            .ok_or_else(|| anyhow!("media library {library_id} is currently unavailable"))?;
        if (
            store.root.root_identity().device(),
            store.root.root_identity().inode(),
        ) != root_identity
            || !same_trusted_root_path(&store.root_path, &library.root)?
        {
            bail!("media library root identity changed; refresh settings and retry");
        }
        store.root.validate_configured_root()?;
        Ok((library, store))
    }

    // Keep the ownership scope, authenticated lease and independent inventory budget explicit.
    #[allow(clippy::too_many_arguments)]
    fn collect_staging_candidates(
        &self,
        library: &MediaLibrary,
        store: &DownloadStore,
        allowed_chat_ids: &[i64],
        lease: &VideoStagingCleanupLease,
        recovery_status: &VideoStagingCleanupRecoveryStatus,
        deadline: Instant,
        inventory_entry_limit: usize,
    ) -> Result<StagingCandidateSet> {
        let (records, mut warnings, mut truncated, mut ownership_complete) =
            read_indexed_staging_records(
                self,
                store,
                &library.root,
                &library.id,
                allowed_chat_ids,
                deadline,
            )?;
        let current_jobs = lease.list_job_relatives()?;
        let pending_jobs = lease.pending_cleanup_quarantine_relatives()?;
        let pending = pending_jobs.iter().cloned().collect::<HashSet<_>>();
        let mut associations: HashMap<PathBuf, Vec<usize>> = HashMap::new();
        let mut association_count = 0_usize;

        for (index, authorized) in records.iter().enumerate() {
            if Instant::now() >= deadline {
                truncated = true;
                ownership_complete = false;
                push_cleanup_warning(
                    &mut warnings,
                    "The authorized queue scan reached its time limit; some staging ownership may be omitted.",
                );
                break;
            }
            match record_staging_jobs(&store.root, &store.root_path, &authorized.record) {
                Ok(paths) => {
                    for relative in paths {
                        association_count = association_count.saturating_add(1);
                        if association_count > MAX_STAGING_CLEANUP_ASSOCIATIONS {
                            truncated = true;
                            ownership_complete = false;
                            push_cleanup_warning(
                                &mut warnings,
                                "The staging association limit was reached; some tasks require a narrower scan.",
                            );
                            break;
                        }
                        associations.entry(relative).or_default().push(index);
                    }
                }
                Err(_) => {
                    truncated = true;
                    ownership_complete = false;
                    push_cleanup_warning(
                        &mut warnings,
                        "A task has an invalid recorded staging path; its directories remain protected.",
                    );
                }
            }
            if association_count > MAX_STAGING_CLEANUP_ASSOCIATIONS {
                break;
            }
        }

        let mut all_jobs = current_jobs.into_iter().collect::<HashSet<_>>();
        all_jobs.extend(pending_jobs);
        let mut relatives = all_jobs.into_iter().collect::<Vec<_>>();
        relatives.sort();
        if !recovery_status.is_safe() {
            truncated = true;
            push_cleanup_warning(
                &mut warnings,
                recovery_status.reason().unwrap_or(
                    "Unresolved video output recovery protects every retained staging directory.",
                ),
            );
        } else if recovery_status.was_dirty_ordinary() {
            push_cleanup_warning(
                &mut warnings,
                "A bounded audit verified that the dirty marker reflects ordinary retained \
                 downloads only.",
            );
        }

        let mut candidates = Vec::new();
        let mut inventory_entry_count = 0_usize;
        let mut has_unassociated_jobs = false;
        for relative in relatives {
            if Instant::now() >= deadline {
                truncated = true;
                push_cleanup_warning(
                    &mut warnings,
                    "The staging inventory reached its time limit; later directories were omitted.",
                );
                break;
            }
            let Some(task_indices) = associations.get(&relative) else {
                // Unassociated job directories are intentionally invisible to cleanup.
                has_unassociated_jobs = true;
                continue;
            };
            let relative_string = match relative.to_str() {
                Some(path) if path.len() <= MAX_STAGING_CLEANUP_PATH_BYTES => path.to_string(),
                _ => {
                    truncated = true;
                    push_cleanup_warning(
                        &mut warnings,
                        "A staging path exceeded the display limit and remains protected.",
                    );
                    continue;
                }
            };
            let Some(display_index) = task_indices
                .iter()
                .copied()
                .find(|index| records[*index].permitted)
            else {
                continue;
            };
            if task_indices.len() != 1 {
                let authorized = &records[display_index];
                let item = staging_item_for(
                    &authorized.record,
                    &relative_string,
                    None,
                    Some("Multiple queue records reference this staging directory."),
                )?;
                candidates.push(StagingCandidate { item, proof: None });
                continue;
            }
            let authorized = &records[display_index];
            let mut protected_reason = authorized.base_reason.as_deref();
            let is_pending_quarantine = pending.contains(&relative);
            if is_pending_quarantine {
                protected_reason = Some("A previous confirmed removal is pending recovery.");
            } else if !recovery_status.is_safe() {
                protected_reason =
                    Some("Video output recovery is unresolved; this directory is protected.");
            }

            let snapshot = if is_pending_quarantine || !recovery_status.is_safe() {
                None
            } else {
                match lease.snapshot_job_for_cleanup(&relative, recovery_status) {
                    Ok(snapshot) => Some(snapshot),
                    Err(error) if crate::file_provider::is_file_provider_access_error(&error) => {
                        return Err(error);
                    }
                    Err(error) => {
                        protected_reason = Some(safe_snapshot_failure_reason(&error));
                        push_cleanup_warning(
                            &mut warnings,
                            "Some associated staging directories failed ownership or recovery checks.",
                        );
                        None
                    }
                }
            };
            if let Some(snapshot) = snapshot.as_ref() {
                inventory_entry_count =
                    inventory_entry_count.saturating_add(snapshot.inventory_entry_count);
                if inventory_entry_count > inventory_entry_limit {
                    truncated = true;
                    protected_reason = Some(
                        "The inventory limit was reached; only earlier displayed rows are selectable.",
                    );
                    push_cleanup_warning(
                        &mut warnings,
                        "The staging inventory limit was reached; later rows remain protected.",
                    );
                }
            }
            let item = staging_item_for(
                &authorized.record,
                &relative_string,
                snapshot.as_ref(),
                protected_reason,
            )?;
            let proof = if item.eligible {
                snapshot.as_ref().map(|snapshot| {
                    cleanup_proof_for(&authorized.record, &relative_string, snapshot)
                })
            } else {
                None
            };
            candidates.push(StagingCandidate { item, proof });
            if inventory_entry_count > inventory_entry_limit {
                break;
            }
        }
        if !ownership_complete {
            for candidate in &mut candidates {
                candidate.item.eligible = false;
                candidate.proof = None;
                let reason = candidate
                    .item
                    .reason
                    .as_deref()
                    .map(|reason| format!("Queue ownership is incomplete; {reason}"))
                    .unwrap_or_else(|| {
                        "Queue ownership is incomplete; refresh before selecting this row."
                            .to_string()
                    });
                candidate.item.reason =
                    Some(bounded_text(&reason, MAX_STAGING_CLEANUP_TITLE_BYTES));
            }
            push_cleanup_warning(
                &mut warnings,
                "Queue ownership is incomplete; preview is blocked until a complete scan succeeds.",
            );
        } else if truncated {
            push_cleanup_warning(
                &mut warnings,
                "The bounded inventory is incomplete; preview only displayed eligible rows.",
            );
        }
        if has_unassociated_jobs {
            push_cleanup_warning(
                &mut warnings,
                "Unassociated staging directories are protected and omitted.",
            );
        }
        lease.validate()?;
        Ok(StagingCandidateSet {
            items: candidates,
            warnings,
            truncated,
            ownership_complete,
        })
    }
}

fn read_indexed_staging_records(
    manager: &QueueManager,
    store: &DownloadStore,
    library_root: &Path,
    library_id: &str,
    allowed_chat_ids: &[i64],
    deadline: Instant,
) -> Result<(Vec<IndexedStagingRecord>, Vec<String>, bool, bool)> {
    let mut warnings = Vec::new();
    let mut truncated = false;
    let mut ownership_complete = true;
    if allowed_chat_ids.is_empty() {
        return Ok((Vec::new(), warnings, false, true));
    }
    let allowed = allowed_chat_ids.iter().copied().collect::<HashSet<_>>();
    let active_cancellations = manager
        .cancellations
        .lock()
        .map_err(poisoned_lock)?
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    let index = store.read_index()?;
    let mut records = Vec::new();
    let mut bytes_read = 0_usize;
    for (records_read, (task_id, entry)) in index.tasks.iter().enumerate() {
        if Instant::now() >= deadline
            || records_read >= MAX_STAGING_CLEANUP_QUEUE_RECORDS
            || bytes_read >= MAX_STAGING_CLEANUP_QUEUE_RECORD_BYTES
        {
            truncated = true;
            ownership_complete = false;
            push_cleanup_warning(
                &mut warnings,
                "The task index exceeded the cleanup scan budget; some staging ownership remains protected.",
            );
            break;
        }
        if entry.move_target.is_some() {
            truncated = true;
            ownership_complete = false;
            push_cleanup_warning(
                &mut warnings,
                "A task is participating in queue recovery or media transfer; staging ownership remains protected.",
            );
            continue;
        }
        let active_path = store.task_path(task_id)?;
        let history_path = store.terminal_history_path(task_id)?;
        let expected_path = if entry.record_path == active_path {
            active_path
        } else if entry.record_path == history_path {
            history_path
        } else {
            truncated = true;
            ownership_complete = false;
            push_cleanup_warning(
                &mut warnings,
                "A task index entry has an unexpected record path; its staging remains protected.",
            );
            continue;
        };
        let Some(contents) = store.read_private_file(&expected_path, MAX_RECORD_BYTES)? else {
            truncated = true;
            ownership_complete = false;
            push_cleanup_warning(
                &mut warnings,
                "A queue record is missing; staging cleanup ownership is incomplete.",
            );
            continue;
        };
        bytes_read = bytes_read.saturating_add(contents.len());
        if bytes_read > MAX_STAGING_CLEANUP_QUEUE_RECORD_BYTES {
            truncated = true;
            ownership_complete = false;
            push_cleanup_warning(
                &mut warnings,
                "Task records exceeded the cleanup byte budget; some staging ownership remains protected.",
            );
            break;
        }
        let record: TaskRecord = match serde_json::from_slice(&contents) {
            Ok(record) => record,
            Err(_) => {
                truncated = true;
                ownership_complete = false;
                push_cleanup_warning(
                    &mut warnings,
                    "A queue record is unreadable; staging cleanup ownership is incomplete.",
                );
                continue;
            }
        };
        if record.schema_version != TASK_RECORD_VERSION
            || record.id != *task_id
            || record.chat_id != entry.chat_id
            || record.revision != entry.revision
            || record.updated_at != entry.updated_at
        {
            truncated = true;
            ownership_complete = false;
            push_cleanup_warning(
                &mut warnings,
                "A queue record did not match its index; staging ownership remains protected.",
            );
            continue;
        }

        let cancellation_active = active_cancellations.contains(&record.id);
        let permitted = allowed.contains(&record.chat_id);
        let base_reason = if permitted {
            staging_cleanup_task_reason(&record, library_root, library_id, cancellation_active)?
        } else {
            Some("Task belongs to an unauthorized chat.".to_string())
        };
        records.push(IndexedStagingRecord {
            record,
            base_reason,
            permitted,
        });
    }
    Ok((records, warnings, truncated, ownership_complete))
}

fn build_complete_staging_associations(
    records: &[IndexedStagingRecord],
    root: &RootedFs,
    root_path: &Path,
    deadline: Instant,
) -> Result<HashMap<PathBuf, Vec<usize>>> {
    let mut associations = HashMap::new();
    let mut association_count = 0_usize;
    for (index, indexed) in records.iter().enumerate() {
        if Instant::now() >= deadline {
            bail!("the bounded staging ownership scan exceeded its time limit");
        }
        let paths = record_staging_jobs(root, root_path, &indexed.record)
            .context("a task has an invalid recorded staging path")?;
        for relative in paths {
            association_count = association_count.saturating_add(1);
            if association_count > MAX_STAGING_CLEANUP_ASSOCIATIONS {
                bail!("the bounded staging association limit was exceeded");
            }
            associations
                .entry(relative)
                .or_insert_with(Vec::new)
                .push(index);
        }
    }
    Ok(associations)
}

fn staging_cleanup_task_reason(
    record: &TaskRecord,
    library_root: &Path,
    library_id: &str,
    cancellation_active: bool,
) -> Result<Option<String>> {
    if cancellation_active || (record.cancel_requested && record.status != TaskStatus::Cancelled) {
        return Ok(Some("A cancellation is active for this task.".to_string()));
    }
    if record.destination_transfer.is_some()
        || record.destination_transfer_phase.is_some()
        || record.destination_transfer_ack.is_some()
    {
        return Ok(Some(
            "This task is reserved by a media library transfer.".to_string(),
        ));
    }
    if let Some(destination) = &record.destination
        && (destination.library_id != library_id
            || !same_trusted_root_path(&destination.root, library_root)?)
    {
        return Ok(Some(
            "This task is registered to a different media library.".to_string(),
        ));
    }
    if matches!(&record.job, JobRequest::Pdf { .. }) {
        return Ok(Some(
            "PDF task staging is outside video cleanup.".to_string(),
        ));
    }
    if record.saved_location.is_some() {
        return Ok(Some(
            "This task has a published media location and is protected.".to_string(),
        ));
    }
    if !matches!(
        record.status,
        TaskStatus::Interrupted
            | TaskStatus::Failed
            | TaskStatus::Cancelled
            | TaskStatus::PartiallyCompleted
    ) {
        return Ok(Some(if record.status == TaskStatus::Completed {
            "Completed media is protected from staging cleanup.".to_string()
        } else {
            "This task is active or has not failed; its staging is protected.".to_string()
        }));
    }
    Ok(None)
}

fn staging_item_for(
    record: &TaskRecord,
    relative_path: &str,
    snapshot: Option<&VideoStagingCleanupSnapshot>,
    reason: Option<&str>,
) -> Result<StagingItem> {
    let title = staging_task_title(record);
    let mut item = StagingItem {
        id: String::new(),
        title,
        task_id: record.id.clone(),
        task_status: record.status,
        relative_path: relative_path.to_string(),
        bytes: snapshot.map(|snapshot| snapshot.bytes),
        file_count: snapshot.map(|snapshot| snapshot.file_count),
        eligible: reason.is_none() && snapshot.is_some(),
        reason: reason.map(|reason| bounded_text(reason, MAX_STAGING_CLEANUP_TITLE_BYTES)),
    };
    item.id = staging_item_id(record, relative_path, snapshot)?;
    Ok(item)
}

fn cleanup_proof_for(
    record: &TaskRecord,
    relative_path: &str,
    snapshot: &VideoStagingCleanupSnapshot,
) -> CleanupProof {
    CleanupProof {
        task_id: record.id.clone(),
        task_generation: record.generation,
        task_revision: record.revision,
        task_status: record.status,
        task_chat_id: record.chat_id,
        relative_path: relative_path.to_string(),
        root_identity: snapshot.root_identity,
        control_identity: snapshot.control_identity,
        control_owner_identity: snapshot.control_owner_identity,
        output_lock_identity: snapshot.output_lock_identity,
        recovery_state_identity: snapshot.recovery_state_identity,
        staging_root_identity: snapshot.staging_root_identity,
        staging_identity: snapshot.staging_identity,
        owner_identity: snapshot.owner_identity,
        inventory_sha256: snapshot.inventory_sha256.clone(),
        inventory_entry_count: snapshot.inventory_entry_count,
        bytes: snapshot.bytes,
        file_count: snapshot.file_count,
    }
}

fn staging_item_id(
    record: &TaskRecord,
    relative_path: &str,
    snapshot: Option<&VideoStagingCleanupSnapshot>,
) -> Result<String> {
    let snapshot_identity = snapshot.map(|snapshot| {
        (
            snapshot.root_identity,
            snapshot.control_identity,
            snapshot.control_owner_identity,
            snapshot.output_lock_identity,
            snapshot.recovery_state_identity,
            snapshot.staging_root_identity,
            snapshot.staging_identity,
            snapshot.owner_identity,
            snapshot.inventory_sha256.as_str(),
            snapshot.inventory_entry_count,
            snapshot.bytes,
            snapshot.file_count,
        )
    });
    let digest = staging_cleanup_digest(&(
        "staging-cleanup-item-v1",
        record.id.as_str(),
        record.generation,
        record.revision,
        record.status,
        relative_path,
        snapshot_identity,
    ))?;
    Ok(format!("stg1-{digest}"))
}

fn staging_scan_revision(scan: &StagingScan) -> Result<String> {
    staging_cleanup_digest(&(
        "staging-cleanup-scan-v1",
        scan.library_id.as_str(),
        scan.root_binding.as_str(),
        &scan.items,
        scan.truncated,
    ))
}

fn staging_preview_revision(
    library_id: &str,
    root_binding: &str,
    items: &[StagingItem],
    proofs: &[CleanupProof],
) -> Result<String> {
    staging_cleanup_digest(&(
        "staging-cleanup-preview-v1",
        library_id,
        root_binding,
        items,
        proofs,
    ))
}

fn staging_cleanup_digest<T: serde::Serialize>(value: &T) -> Result<String> {
    let serialized =
        serde_json::to_vec(value).context("failed to encode staging cleanup identity")?;
    let digest = Sha256::digest(serialized);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn staging_task_title(record: &TaskRecord) -> String {
    let title = record
        .display_metadata
        .as_ref()
        .and_then(|metadata| metadata.title.as_deref())
        .or_else(|| record.plan.as_ref().and_then(|plan| plan.title.as_deref()))
        .or_else(|| {
            record
                .proposed_plan
                .as_ref()
                .and_then(|plan| plan.title.as_deref())
        });
    title
        .map(|value| bounded_text(value, MAX_STAGING_CLEANUP_TITLE_BYTES))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("Video task {}", record.ordinal))
}

fn bounded_text(value: &str, max_bytes: usize) -> String {
    let mut bounded = String::new();
    for character in value.chars().filter(|character| !character.is_control()) {
        if bounded.len().saturating_add(character.len_utf8()) > max_bytes {
            break;
        }
        bounded.push(character);
    }
    bounded.trim().to_string()
}

fn safe_snapshot_failure_reason(error: &anyhow::Error) -> &'static str {
    let message = format!("{error:#}").to_ascii_lowercase();
    if message.contains("core download completed") {
        "The video completed its download and is protected for publication recovery."
    } else if message.contains("worker request marker") {
        "A worker request marker is present; this directory is protected."
    } else if message.contains("pending recovery") || message.contains("quarantine") {
        "A prior cleanup or output recovery is pending."
    } else {
        "The staging directory failed ownership or recovery checks."
    }
}

fn safe_cleanup_failure_reason(error: &anyhow::Error) -> String {
    let message = format!("{error:#}").to_ascii_lowercase();
    let safe = if message.contains("changed after") || message.contains("changed while") {
        "The staging directory changed after preview."
    } else if message.contains("pending recovery") || message.contains("quarantine") {
        "A confirmed removal is pending recovery."
    } else {
        "The staging directory failed final safety checks."
    };
    safe.to_string()
}

fn push_cleanup_warning(warnings: &mut Vec<String>, warning: &str) {
    if warnings.len() >= MAX_STAGING_CLEANUP_SCAN_WARNINGS
        || warnings.iter().any(|existing| existing == warning)
    {
        return;
    }
    warnings.push(bounded_text(
        warning,
        MAX_STAGING_CLEANUP_SCAN_WARNING_BYTES,
    ));
}

fn fit_staging_scan_to_budget(scan: &mut StagingScan) -> Result<()> {
    let encoded_size = |scan: &StagingScan| -> Result<usize> {
        serde_json::to_vec(scan)
            .map(|bytes| bytes.len())
            .context("failed to serialize bounded staging scan")
    };
    while encoded_size(scan)? > MAX_STAGING_CLEANUP_SCAN_BYTES && !scan.items.is_empty() {
        scan.items.pop();
        scan.truncated = true;
        push_cleanup_warning(
            &mut scan.warnings,
            "The staging list exceeded its receipt size limit; trailing rows were omitted.",
        );
        scan.revision = staging_scan_revision(scan)?;
    }
    if encoded_size(scan)? > MAX_STAGING_CLEANUP_SCAN_BYTES {
        bail!("the bounded staging scan exceeds its durable receipt size limit");
    }
    Ok(())
}

fn validate_staging_cleanup_selection(item_ids: &[String]) -> Result<()> {
    if item_ids.is_empty() || item_ids.len() > MAX_STAGING_CLEANUP_ITEMS {
        bail!("select between 1 and {MAX_STAGING_CLEANUP_ITEMS} staging directories");
    }
    let mut ids = HashSet::with_capacity(item_ids.len());
    for id in item_ids {
        if id.len() > 80 || !id.starts_with("stg1-") || !ids.insert(id) {
            bail!("staging selection contains an invalid or duplicate item ID");
        }
    }
    Ok(())
}

fn validate_saved_staging_preview(preview: &CleanupPreview) -> Result<()> {
    let count = preview.items.len();
    if count == 0
        || count > MAX_STAGING_CLEANUP_ITEMS
        || count != preview.proofs().len()
        || !preview.id.starts_with("scp1-")
        || preview.id.len() > 80
    {
        bail!("the retained cleanup preview has an invalid shape");
    }
    let mut ids = HashSet::with_capacity(count);
    let mut paths = HashSet::with_capacity(count);
    let mut total_bytes = 0_u64;
    let mut total_inventory_entries = 0_usize;
    for (item, proof) in preview.items.iter().zip(preview.proofs()) {
        if !item.eligible
            || item.id.is_empty()
            || !ids.insert(item.id.as_str())
            || item.task_id != proof.task_id
            || item.task_status != proof.task_status
            || item.relative_path != proof.relative_path
            || item.bytes != Some(proof.bytes)
            || item.file_count != Some(proof.file_count)
            || proof.inventory_entry_count > 4_096
            || !paths.insert(proof.relative_path.as_str())
        {
            bail!("the retained cleanup preview contains an invalid selection proof");
        }
        total_bytes = total_bytes
            .checked_add(proof.bytes)
            .ok_or_else(|| anyhow!("retained cleanup byte total is too large"))?;
        total_inventory_entries =
            total_inventory_entries.saturating_add(proof.inventory_entry_count);
        if total_inventory_entries > MAX_STAGING_CLEANUP_INVENTORY_ENTRIES {
            bail!("retained cleanup inventory exceeds its global confirmation limit");
        }
    }
    if total_bytes != preview.total_bytes
        || staging_preview_revision(
            &preview.library_id,
            &preview.root_binding,
            &preview.items,
            preview.proofs(),
        )? != preview.revision
        || serde_json::to_vec(preview)
            .context("failed to measure retained cleanup preview")?
            .len()
            > MAX_STAGING_CLEANUP_PREVIEW_BYTES
    {
        bail!("the retained cleanup preview failed its integrity checks");
    }
    Ok(())
}

fn require_same_cleanup_root(
    before: &DownloadStore,
    current: &DownloadStore,
    lease: &VideoStagingCleanupLease,
) -> Result<()> {
    if before.root.root_identity() != current.root.root_identity()
        || current.root.root_identity() != lease.root().root_identity()
        || before.queue_identity != current.queue_identity
    {
        bail!("video library or queue identity changed during staging cleanup");
    }
    Ok(())
}

fn staging_cleanup_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
