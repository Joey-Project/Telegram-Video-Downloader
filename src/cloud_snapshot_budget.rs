use std::io::{self, Write};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::cloud::{InboxRecord, InboxStatus};
use crate::library::{ConflictPolicy, MoveOutcome, MovePreview};
use crate::library_migration::{MigrationScope, MigrationStatus};

const MAX_OPERATION_SNAPSHOT_RECORDS: usize = 100;
const MAX_OPERATION_SNAPSHOT_BYTES: usize = 2 * 1024 * 1024;
const MAX_COMPACT_OPERATION_RESULT_BYTES: usize = 2 * 1024;
pub(crate) const MAX_METADATA_RECOVERY_RESULT_BYTES: usize = 512 * 1024;
const MAX_OPERATION_ERROR_JSON_BYTES: usize = 1024;
const MAX_OPERATION_KIND_JSON_BYTES: usize = 128;
const MAX_CLOUD_REQUEST_ID_BYTES: usize = 256;
const MAX_PREVIEW_SNAPSHOT_RECORDS: usize = 100;
const MAX_PREVIEW_SNAPSHOT_BYTES: usize = 3 * 1024 * 1024;
const MAX_PREVIEW_ID_JSON_BYTES: usize = 128;
const MAX_LIBRARY_ID_BYTES: usize = 128;
const MAX_LIBRARY_ID_JSON_BYTES: usize = MAX_LIBRARY_ID_BYTES * 6 + 2;
const MAX_MIGRATION_PREVIEW_SUMMARY_BYTES: usize = 12 * 1024;
const MAX_MIGRATION_PREVIEW_SAMPLE_ITEMS: usize = 4;
const MAX_MIGRATION_PREVIEW_SAMPLE_ID_JSON_BYTES: usize = 128;
const MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES: usize = 384;
const MAX_MIGRATION_SUMMARY_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const MAX_MIGRATION_STATUS_SUMMARY_BYTES: usize = 12 * 1024;
const MAX_MIGRATION_STATUS_SAMPLE_WARNINGS: usize = 2;
pub(crate) const MAX_FILE_PREVIEW_SUMMARY_BYTES: usize = 12 * 1024;
const MAX_FILE_PREVIEW_SUMMARY_SAMPLE_ITEMS: usize = 4;
const MAX_FILE_PREVIEW_SUMMARY_SAMPLE_FILES: usize = 2;
const MAX_FILE_PREVIEW_SUMMARY_SAMPLE_PATCHES: usize = 4;
const MAX_FILE_PREVIEW_TARGET_JSON_BYTES: usize = 6 * 1024 + 2;
const MAX_FILE_PREVIEW_TOTAL_BYTES_JSON_BYTES: usize = 20;
const MAX_FILE_CONFIRM_SUMMARY_SAMPLE_ITEMS: usize = 4;
const MAX_FILE_CONFIRM_SUMMARY_SAMPLE_WARNINGS: usize = 2;
const MAX_FILE_CONFIRM_SUMMARY_ERROR_JSON_BYTES: usize = 512;
const MAX_FILE_CONFIRM_SUMMARY_WARNING_JSON_BYTES: usize = 512;

const OPERATION_DETAILS_OMITTED_REASON: &str =
    "Older operation details were omitted to keep the cloud state within its size limit.";
const PREVIEW_DETAILS_OMITTED_REASON: &str =
    "Preview details exceeded the cloud snapshot budget. Reload the preview before confirming.";
const PREVIEW_DETAILS_UNAVAILABLE_REASON: &str =
    "Preview details are unavailable in this cloud snapshot. Reload the preview before confirming.";
const MIGRATION_SUMMARY_REASON: &str = "A bounded summary is shown. Start confirms the complete saved migration plan; item and file details are omitted.";
const FILE_PREVIEW_SUMMARY_MESSAGE: &str = "A bounded sample is shown. Confirm applies the complete saved batch; some long sample paths may be shortened.";
const PREVIEWS_OMITTED_REASON: &str = "Some pending previews were omitted from this snapshot. Refresh pending previews before confirming.";

/// Build a bounded receipt summary from a complete file preview while retaining its exact
/// identity and binding. The persisted file-manager preview remains the source of truth for
/// confirmation; this receipt contains display totals and samples only.
pub(crate) fn compact_file_preview_receipt_result(record: &InboxRecord) -> Option<Value> {
    if record.request.kind != "file_preview" {
        return None;
    }
    let result = record.result.as_ref()?;
    let preview_value = result.get("preview")?;
    if preview_value
        .get("file_preview_summary")
        .and_then(Value::as_bool)
        == Some(true)
    {
        return None;
    }
    let preview: MovePreview = serde_json::from_value(preview_value.clone()).ok()?;
    let (library_id, root_binding) = file_preview_request_identity(record)?;
    if result.get("preview_id").and_then(Value::as_str) != Some(preview.id.as_str())
        || result.get("revision").and_then(Value::as_str) != Some(preview.revision.as_str())
        || result.get("library_id").and_then(Value::as_str) != Some(library_id)
        || result.get("root_binding").and_then(Value::as_str) != Some(root_binding)
    {
        return None;
    }

    let metadata_only = preview.target_relative_dir.is_none();
    let selected_count = requested_file_preview_selection_count(record, metadata_only)?;
    if metadata_only {
        if selected_count != preview.metadata_patches.len() || !preview.items.is_empty() {
            return None;
        }
    } else if selected_count != preview.items.len() {
        return None;
    }
    let preview_summary =
        build_file_preview_summary(&preview, selected_count, library_id, root_binding)?;
    let compact = json!({
        "preview_id": preview.id,
        "revision": preview.revision,
        "library_id": library_id,
        "root_binding": root_binding,
        "details_incomplete": true,
        "result_truncated": true,
        "preview": preview_summary,
    });
    json_size_within(&compact, MAX_FILE_PREVIEW_SUMMARY_BYTES).map(|_| compact)
}

/// Return whether a persisted compact file preview summary is internally consistent with the
/// request and receipt bindings. Confirmation uses this only to authorize looking up the full
/// local preview by ID/revision; no summary field is used to reconstruct an execution plan.
pub(crate) fn is_valid_compact_file_preview_result(record: &InboxRecord) -> bool {
    let Some(result) = record.result.as_ref() else {
        return false;
    };
    validated_file_preview_summary(record, result).is_some()
}

fn validated_file_preview_summary<'a>(
    record: &InboxRecord,
    result: &'a Value,
) -> Option<&'a Value> {
    if record.request.kind != "file_preview"
        || result.get("details_incomplete").and_then(Value::as_bool) != Some(true)
        || result.get("result_truncated").and_then(Value::as_bool) != Some(true)
    {
        return None;
    }
    let summary = result.get("preview")?;
    if summary.get("file_preview_summary").and_then(Value::as_bool) != Some(true)
        || summary.get("summary_version").and_then(Value::as_u64) != Some(1)
        || summary.get("items").is_some()
        || summary.get("sample_items")?.as_array()?.len() > MAX_FILE_PREVIEW_SUMMARY_SAMPLE_ITEMS
        || summary.get("sample_metadata_patches")?.as_array()?.len()
            > MAX_FILE_PREVIEW_SUMMARY_SAMPLE_PATCHES
        || json_size_within(summary, MAX_FILE_PREVIEW_SUMMARY_BYTES).is_none()
    {
        return None;
    }

    let (library_id, root_binding) = file_preview_request_identity(record)?;
    let preview_id = result.get("preview_id")?.as_str()?;
    let revision = result.get("revision")?.as_str()?;
    if summary.get("id")?.as_str()? != preview_id
        || summary.get("revision")?.as_str()? != revision
        || summary.get("library_id")?.as_str()? != library_id
        || summary.get("root_binding")?.as_str()? != root_binding
        || result.get("library_id")?.as_str()? != library_id
        || result.get("root_binding")?.as_str()? != root_binding
    {
        return None;
    }

    let selected_count = summary.get("selected_count")?.as_u64()? as usize;
    let metadata_only = summary.get("metadata_only")?.as_bool()?;
    if selected_count > MAX_FILE_PREVIEW_SELECTION_ITEMS
        || requested_file_preview_selection_count(record, metadata_only)? != selected_count
    {
        return None;
    }
    let target = summary.get("target_relative_dir")?;
    if metadata_only != target.is_null()
        || (!target.is_null() && target.as_str().is_none())
        || summary.get("rename")?.as_bool().is_none()
        || !matches!(summary.get("conflict")?.as_str()?, "skip" | "keep_both")
    {
        return None;
    }

    let move_item_count = summary.get("move_item_count")?.as_u64()? as usize;
    let file_count = summary.get("file_count")?.as_u64()? as usize;
    let total_bytes = summary.get("total_bytes")?.as_str()?;
    if total_bytes.is_empty()
        || total_bytes.len() > MAX_FILE_PREVIEW_TOTAL_BYTES_JSON_BYTES
        || !total_bytes.bytes().all(|byte| byte.is_ascii_digit())
        || total_bytes
            .parse::<u128>()
            .ok()
            .is_none_or(|parsed| parsed.to_string() != total_bytes)
    {
        return None;
    }
    let outcomes = summary.get("outcome_counts")?.as_object()?;
    let moves = outcomes.get("move")?.as_u64()? as usize;
    let skips = outcomes.get("skip")?.as_u64()? as usize;
    let no_ops = outcomes.get("no_op")?.as_u64()? as usize;
    if moves.saturating_add(skips).saturating_add(no_ops) != move_item_count
        || summary.get("conflict_count")?.as_u64()? as usize != skips
        || move_item_count != if metadata_only { 0 } else { selected_count }
    {
        return None;
    }

    let metadata_patch_count = summary.get("metadata_patch_count")?.as_u64()? as usize;
    let changed_patch_count = summary.get("changed_metadata_patch_count")?.as_u64()? as usize;
    if metadata_patch_count > MAX_FILE_PREVIEW_SELECTION_ITEMS
        || changed_patch_count > metadata_patch_count
        || metadata_patch_count != requested_metadata_patch_count(record)?
    {
        return None;
    }
    let sample_items = summary.get("sample_items")?.as_array()?;
    let sample_metadata_patches = summary.get("sample_metadata_patches")?.as_array()?;
    let sampled_file_count = validate_file_preview_sample_items(sample_items)?;
    validate_file_preview_sample_patches(sample_metadata_patches)?;
    if move_item_count < sample_items.len()
        || file_count < sampled_file_count
        || metadata_patch_count < sample_metadata_patches.len()
        || summary.get("omitted_item_count")?.as_u64()? as usize
            != move_item_count.saturating_sub(sample_items.len())
        || summary.get("omitted_file_count")?.as_u64()? as usize
            != file_count.saturating_sub(sampled_file_count)
        || summary.get("omitted_metadata_patch_count")?.as_u64()? as usize
            != metadata_patch_count.saturating_sub(sample_metadata_patches.len())
        || summary.get("summary_message")?.as_str()?.is_empty()
    {
        return None;
    }
    Some(summary)
}

const MAX_FILE_PREVIEW_SELECTION_ITEMS: usize = 5_000;

fn file_preview_request_identity(record: &InboxRecord) -> Option<(&str, &str)> {
    let payload_library_id = record.request.payload.get("library_id")?.as_str()?;
    let payload_root_binding = record.request.payload.get("root_binding")?.as_str()?;
    let result = record.result.as_ref()?;
    if !valid_library_id(payload_library_id)
        || !valid_root_binding_token(payload_root_binding)
        || result.get("library_id")?.as_str()? != payload_library_id
        || result.get("root_binding")?.as_str()? != payload_root_binding
    {
        return None;
    }
    Some((payload_library_id, payload_root_binding))
}

fn requested_file_preview_selection_count(
    record: &InboxRecord,
    metadata_only: bool,
) -> Option<usize> {
    let request = &record.request.payload;
    let count = if metadata_only {
        requested_metadata_patch_count(record)?
    } else {
        request.get("item_ids")?.as_array()?.len()
    };
    (count <= MAX_FILE_PREVIEW_SELECTION_ITEMS).then_some(count)
}

fn requested_metadata_patch_count(record: &InboxRecord) -> Option<usize> {
    let Some(value) = record.request.payload.get("metadata_patches") else {
        return Some(0);
    };
    let patches = value.as_array()?;
    (patches.len() <= MAX_FILE_PREVIEW_SELECTION_ITEMS).then_some(patches.len())
}

fn build_file_preview_summary(
    preview: &MovePreview,
    selected_count: usize,
    library_id: &str,
    root_binding: &str,
) -> Option<Value> {
    if preview.id.is_empty()
        || preview.revision.is_empty()
        || !valid_library_id(library_id)
        || !valid_root_binding_token(root_binding)
    {
        return None;
    }
    let metadata_only = preview.target_relative_dir.is_none();
    let target_relative_dir = match preview.target_relative_dir.as_deref() {
        Some(target) => {
            let bounded = bounded_json_string(target, MAX_FILE_PREVIEW_TARGET_JSON_BYTES)?;
            (bounded == target).then_some(Value::String(bounded))?
        }
        None => Value::Null,
    };
    let mut move_count = 0usize;
    let mut skip_count = 0usize;
    let mut no_op_count = 0usize;
    let mut file_count = 0usize;
    let mut total_bytes = 0u128;
    for item in &preview.items {
        match item.outcome {
            MoveOutcome::Move => move_count = move_count.saturating_add(1),
            MoveOutcome::Skip => skip_count = skip_count.saturating_add(1),
            MoveOutcome::NoOp => no_op_count = no_op_count.saturating_add(1),
        }
        file_count = file_count.saturating_add(item.files.len());
        total_bytes = item.files.iter().fold(total_bytes, |total, file| {
            total.saturating_add(u128::from(file.bytes))
        });
    }
    let total_bytes = total_bytes.to_string();
    let total_bytes = bounded_json_string(&total_bytes, MAX_FILE_PREVIEW_TOTAL_BYTES_JSON_BYTES)?;

    let mut sample_items = Vec::with_capacity(
        preview
            .items
            .len()
            .min(MAX_FILE_PREVIEW_SUMMARY_SAMPLE_ITEMS),
    );
    let mut sampled_file_count = 0usize;
    for item in preview
        .items
        .iter()
        .take(MAX_FILE_PREVIEW_SUMMARY_SAMPLE_ITEMS)
    {
        let item_id =
            bounded_json_string(&item.item_id, MAX_MIGRATION_PREVIEW_SAMPLE_ID_JSON_BYTES)?;
        let source_path = bounded_json_string(
            &item.source_path,
            MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES,
        )?;
        let target_path = bounded_json_string(
            &item.target_path,
            MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES,
        )?;
        let sample_files = item
            .files
            .iter()
            .take(MAX_FILE_PREVIEW_SUMMARY_SAMPLE_FILES)
            .map(|file| {
                Some(json!({
                    "source_path": bounded_json_string(&file.source_path, MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES)?,
                    "target_path": bounded_json_string(&file.target_path, MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES)?,
                    "bytes": file.bytes.to_string(),
                }))
            })
            .collect::<Option<Vec<_>>>()?;
        sampled_file_count = sampled_file_count.saturating_add(sample_files.len());
        sample_items.push(json!({
            "item_id": item_id,
            "source_path": source_path,
            "target_path": target_path,
            "outcome": move_outcome_name(item.outcome),
            "file_count": item.files.len(),
            "sample_files": sample_files,
            "omitted_file_count": item.files.len().saturating_sub(sample_files.len()),
        }));
    }
    let mut sample_metadata_patches = preview
        .metadata_patches
        .iter()
        .take(MAX_FILE_PREVIEW_SUMMARY_SAMPLE_PATCHES)
        .map(|patch| {
            Some(json!({
                "item_id": bounded_json_string(&patch.item_id, MAX_MIGRATION_PREVIEW_SAMPLE_ID_JSON_BYTES)?,
                "nfo_path": bounded_json_string(&patch.nfo_path, MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES)?,
                "changed": patch.changed,
            }))
        })
        .collect::<Option<Vec<_>>>()?;

    let changed_metadata_patch_count = preview
        .metadata_patches
        .iter()
        .filter(|patch| patch.changed)
        .count();
    let make_summary = |sample_items: &[Value],
                        sample_metadata_patches: &[Value],
                        sampled_file_count: usize| {
        json!({
            "file_preview_summary": true,
            "summary_version": 1,
            "id": preview.id,
            "revision": preview.revision,
            "library_id": library_id,
            "root_binding": root_binding,
            "selected_count": selected_count,
            "metadata_only": metadata_only,
            "target_relative_dir": target_relative_dir,
            "rename": preview.rename,
            "conflict": match preview.conflict {
                crate::library::ConflictPolicy::Skip => "skip",
                crate::library::ConflictPolicy::KeepBoth => "keep_both",
            },
            "move_item_count": preview.items.len(),
            "file_count": file_count,
            "total_bytes": total_bytes,
            "outcome_counts": { "move": move_count, "skip": skip_count, "no_op": no_op_count },
            "conflict_count": skip_count,
            "metadata_patch_count": preview.metadata_patches.len(),
            "changed_metadata_patch_count": changed_metadata_patch_count,
            "sample_items": sample_items,
            "sample_metadata_patches": sample_metadata_patches,
            "omitted_item_count": preview.items.len().saturating_sub(sample_items.len()),
            "omitted_file_count": file_count.saturating_sub(sampled_file_count),
            "omitted_metadata_patch_count": preview.metadata_patches.len().saturating_sub(sample_metadata_patches.len()),
            "summary_message": FILE_PREVIEW_SUMMARY_MESSAGE,
        })
    };

    loop {
        let summary = make_summary(&sample_items, &sample_metadata_patches, sampled_file_count);
        if json_size_within(
            &summary,
            MAX_FILE_PREVIEW_SUMMARY_BYTES.saturating_sub(2 * 1024),
        )
        .is_some()
        {
            return Some(summary);
        }
        if let Some(item) = sample_items.pop() {
            let omitted_from_item = item.get("sample_files")?.as_array()?.len();
            sampled_file_count = sampled_file_count.saturating_sub(omitted_from_item);
        } else if !sample_metadata_patches.is_empty() {
            sample_metadata_patches.pop();
        } else {
            return None;
        }
    }
}

fn move_outcome_name(outcome: MoveOutcome) -> &'static str {
    match outcome {
        MoveOutcome::Move => "move",
        MoveOutcome::Skip => "skip",
        MoveOutcome::NoOp => "no_op",
    }
}

/// Compact an oversized completed/failed batch result while preserving its original terminal
/// status. Item details are samples only; the full execution result remains in local batch state.
pub(crate) fn compact_file_confirm_receipt_result(record: &InboxRecord) -> Option<Value> {
    if record.request.kind != "file_confirm" {
        return None;
    }
    let result = record.result.as_ref()?;
    let preview_id = result.get("preview_id")?.as_str()?;
    let revision = result.get("revision")?.as_str()?;
    let original_operation = result.get("operation")?;
    let operation_id = original_operation.get("id")?.as_str()?;
    let operation_revision = original_operation.get("revision")?.as_str()?;
    if preview_id != operation_id || revision != operation_revision {
        return None;
    }
    let status = original_operation.get("status")?.as_str()?;
    if !matches!(status, "complete" | "partial" | "failed") {
        return None;
    }
    let retryable_access_error = original_operation
        .get("retryable_access_error")?
        .as_bool()?;
    let items = original_operation.get("items")?.as_array()?;
    if items.len() > MAX_FILE_PREVIEW_SELECTION_ITEMS {
        return None;
    }
    let warnings = original_operation.get("warnings")?.as_array()?;

    let mut outcome_counts = Map::new();
    outcome_counts.insert("move".to_string(), json!(0));
    outcome_counts.insert("skip".to_string(), json!(0));
    outcome_counts.insert("no_op".to_string(), json!(0));
    let mut file_count = 0u64;
    let mut moved_file_count = 0u64;
    let mut metadata_patched_item_count = 0u64;
    let mut error_count = 0u64;
    for item in items {
        let outcome = item.get("outcome")?.as_str()?;
        let count = outcome_counts.get_mut(outcome)?;
        *count = json!(count.as_u64()?.saturating_add(1));
        file_count = file_count.saturating_add(item.get("total_files")?.as_u64()?);
        moved_file_count = moved_file_count.saturating_add(item.get("moved_files")?.as_u64()?);
        if item.get("metadata_patched")?.as_bool()? {
            metadata_patched_item_count = metadata_patched_item_count.saturating_add(1);
        }
        if !item.get("error")?.is_null() {
            error_count = error_count.saturating_add(1);
        }
    }

    let mut sample_items =
        Vec::with_capacity(items.len().min(MAX_FILE_CONFIRM_SUMMARY_SAMPLE_ITEMS));
    for item in items.iter().take(MAX_FILE_CONFIRM_SUMMARY_SAMPLE_ITEMS) {
        let error = item.get("error")?.as_str().and_then(|message| {
            bounded_json_string(message, MAX_FILE_CONFIRM_SUMMARY_ERROR_JSON_BYTES)
        });
        sample_items.push(json!({
            "item_id": bounded_json_string(item.get("item_id")?.as_str()?, MAX_MIGRATION_PREVIEW_SAMPLE_ID_JSON_BYTES)?,
            "outcome": item.get("outcome")?,
            "moved_files": item.get("moved_files")?,
            "total_files": item.get("total_files")?,
            "metadata_patched": item.get("metadata_patched")?,
            "error": error,
        }));
    }
    let mut sample_warnings = warnings
        .iter()
        .take(MAX_FILE_CONFIRM_SUMMARY_SAMPLE_WARNINGS)
        .map(|warning| {
            bounded_json_string(
                warning.as_str()?,
                MAX_FILE_CONFIRM_SUMMARY_WARNING_JSON_BYTES,
            )
        })
        .collect::<Option<Vec<_>>>()?;
    let warning_count = warnings.len();

    let make_operation = |sample_items: &[Value], sample_warnings: &[String]| {
        json!({
            "file_confirm_summary": true,
            "summary_version": 1,
            "id": operation_id,
            "revision": operation_revision,
            "status": status,
            "retryable_access_error": retryable_access_error,
            "selected_count": items.len(),
            "outcome_counts": outcome_counts,
            "file_count": file_count,
            "moved_file_count": moved_file_count,
            "metadata_patched_item_count": metadata_patched_item_count,
            "error_count": error_count,
            "warning_count": warning_count,
            "sample_items": sample_items,
            "omitted_item_count": items.len().saturating_sub(sample_items.len()),
            "sample_warnings": sample_warnings,
            "omitted_warning_count": warning_count.saturating_sub(sample_warnings.len()),
            "summary_message": "A bounded sample is shown; the operation result and complete per-item details are saved locally.",
        })
    };
    loop {
        let compact_operation = make_operation(&sample_items, &sample_warnings);
        let mut compact = json!({
            "preview_id": preview_id,
            "revision": revision,
            "operation": compact_operation,
            "details_incomplete": true,
            "result_truncated": true,
        });
        for field in ["library_refresh_warning", "telegram_progress_warning"] {
            if let Some(warning) = result.get(field).and_then(Value::as_str)
                && let Some(bounded) =
                    bounded_json_string(warning, MAX_FILE_CONFIRM_SUMMARY_WARNING_JSON_BYTES)
            {
                compact[field] = Value::String(bounded);
            }
        }
        if json_size_within(&compact, MAX_FILE_PREVIEW_SUMMARY_BYTES).is_some() {
            return Some(compact);
        }
        if !sample_items.is_empty() {
            sample_items.pop();
        } else if !sample_warnings.is_empty() {
            sample_warnings.pop();
        } else {
            return None;
        }
    }
}

fn validate_file_preview_sample_items(items: &[Value]) -> Option<usize> {
    let mut sampled_files = 0usize;
    for item in items {
        if item.get("item_id")?.as_str()?.is_empty()
            || item.get("source_path")?.as_str()?.is_empty()
            || item.get("target_path")?.as_str()?.is_empty()
            || !matches!(item.get("outcome")?.as_str()?, "move" | "skip" | "no_op")
        {
            return None;
        }
        let file_count = item.get("file_count")?.as_u64()? as usize;
        let files = item.get("sample_files")?.as_array()?;
        if files.len() > MAX_FILE_PREVIEW_SUMMARY_SAMPLE_FILES
            || item.get("omitted_file_count")?.as_u64()? as usize
                != file_count.saturating_sub(files.len())
        {
            return None;
        }
        for file in files {
            let valid_bytes = match file.get("bytes")? {
                Value::Number(value) => value.as_u64().is_some(),
                Value::String(value) => value
                    .parse::<u64>()
                    .ok()
                    .is_some_and(|bytes| bytes.to_string() == *value),
                _ => false,
            };
            if file.get("source_path")?.as_str()?.is_empty()
                || file.get("target_path")?.as_str()?.is_empty()
                || !valid_bytes
            {
                return None;
            }
        }
        sampled_files = sampled_files.saturating_add(files.len());
    }
    Some(sampled_files)
}

fn validate_file_preview_sample_patches(patches: &[Value]) -> Option<()> {
    for patch in patches {
        if patch.get("item_id")?.as_str()?.is_empty()
            || patch.get("nfo_path")?.as_str()?.is_empty()
            || patch.get("changed")?.as_bool().is_none()
        {
            return None;
        }
    }
    Some(())
}
/// Reserve bounded actionable recovery and legacy-import results before other receipts.
pub(crate) fn operation_snapshots(records: &[&InboxRecord]) -> Result<Vec<Value>> {
    let mut selected = records
        .iter()
        .copied()
        .filter(|record| record.request.kind != "telegram")
        .collect::<Vec<_>>();
    selected.sort_by_key(|record| record.request.seq);
    if selected.len() > MAX_OPERATION_SNAPSHOT_RECORDS {
        selected.drain(..selected.len() - MAX_OPERATION_SNAPSHOT_RECORDS);
    }

    let mut snapshots = selected
        .iter()
        .map(|record| operation_snapshot(record, false))
        .collect::<Vec<_>>();
    let mut total_bytes = array_json_size(&snapshots, MAX_OPERATION_SNAPSHOT_BYTES)
        .ok_or_else(|| anyhow!("compact cloud operation receipts exceed their size budget"))?;

    // An active recovery report must remain actionable even when newer receipts are large.
    let mut prioritized_recovery = None;
    for (index, record) in selected.iter().enumerate().rev() {
        if record.request.kind == "metadata_recover"
            && record.status == InboxStatus::Completed
            && record
                .result
                .as_ref()
                .is_some_and(is_usable_metadata_recovery_result)
            && restore_full_operation_if_fits(index, record, &mut snapshots, &mut total_bytes)
        {
            prioritized_recovery = Some(index);
            break;
        }
    }

    // Reserve the newest restorable completed import before large newer receipts can consume the
    // budget. If it cannot fit, try older complete imports before falling back to recency.
    let mut prioritized_legacy_import = None;
    for (index, record) in selected.iter().enumerate().rev() {
        if !is_usable_completed_legacy_import_record(record) {
            continue;
        }
        if restore_full_operation_if_fits(index, record, &mut snapshots, &mut total_bytes) {
            prioritized_legacy_import = Some(index);
            break;
        }
    }

    // Allocate the remaining budget to other full results newest first.
    for (index, record) in selected.iter().enumerate().rev() {
        if Some(index) == prioritized_legacy_import || Some(index) == prioritized_recovery {
            continue;
        }
        restore_full_operation_if_fits(index, record, &mut snapshots, &mut total_bytes);
    }

    Ok(snapshots)
}

fn restore_full_operation_if_fits(
    index: usize,
    record: &InboxRecord,
    snapshots: &mut [Value],
    total_bytes: &mut usize,
) -> bool {
    let full = operation_snapshot(record, true);
    let compact_size = json_size_within(&snapshots[index], MAX_OPERATION_SNAPSHOT_BYTES);
    let full_size = json_size_within(&full, MAX_OPERATION_SNAPSHOT_BYTES);
    let (Some(compact_size), Some(full_size)) = (compact_size, full_size) else {
        return false;
    };
    let replaced_size = (*total_bytes).saturating_sub(compact_size);
    let next_total = replaced_size.saturating_add(full_size);
    if next_total <= MAX_OPERATION_SNAPSHOT_BYTES {
        snapshots[index] = full;
        *total_bytes = next_total;
        true
    } else {
        false
    }
}

fn is_usable_completed_legacy_import_record(record: &InboxRecord) -> bool {
    record.request.kind == "legacy_import"
        && record.status == InboxStatus::Completed
        && record
            .result
            .as_ref()
            .is_some_and(is_usable_legacy_import_result)
}

fn is_usable_legacy_import_result(result: &Value) -> bool {
    let has_library_binding = result
        .get("library_id")
        .and_then(Value::as_str)
        .is_some_and(valid_library_id)
        && result
            .get("root_binding")
            .and_then(Value::as_str)
            .is_some_and(valid_root_binding_token);
    let has_hints = result
        .get("hints")
        .and_then(Value::as_array)
        .is_some_and(|hints| !hints.is_empty());
    let has_candidates = result.get("candidates").and_then(Value::as_array).is_some();
    let result_marked_incomplete = ["incomplete", "details_incomplete", "result_truncated"]
        .iter()
        .any(|field| result.get(field).and_then(Value::as_bool) == Some(true));
    has_library_binding && has_hints && has_candidates && !result_marked_incomplete
}

fn is_usable_metadata_recovery_result(result: &Value) -> bool {
    let Some(recovery) = result.get("recovery") else {
        return false;
    };
    result
        .get("library_id")
        .and_then(Value::as_str)
        .is_some_and(valid_library_id)
        && result
            .get("root_binding")
            .and_then(Value::as_str)
            .is_some_and(valid_root_binding_token)
        && matches!(
            result.get("scope").and_then(Value::as_str),
            Some("selected" | "library")
        )
        && recovery
            .get("revision")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
        && recovery
            .get("items")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty() && items.len() <= 100)
        && [result, recovery].iter().all(|value| {
            !["incomplete", "details_incomplete", "result_truncated"]
                .iter()
                .any(|field| value.get(field).and_then(Value::as_bool) == Some(true))
        })
        && json_size_within(result, MAX_METADATA_RECOVERY_RESULT_BYTES).is_some()
}

/// Compact large historical receipts after an upload-size rejection.
///
/// The newest usable completed legacy-import result stays intact because its hints are the manual
/// mapping entry point in the Mini App. Failed or explicitly incomplete imports do not reserve the
/// budget. The latest bounded recovery report is also retained for its candidate/patch choices.
/// Other details are replaced with a small identity summary.
pub(crate) fn compact_operation_receipts(operations: &mut [Value]) -> bool {
    let latest_legacy_import = operations
        .iter()
        .enumerate()
        .filter(|&(_, operation)| is_usable_completed_legacy_import_snapshot(operation))
        .map(|(index, operation)| {
            (
                index,
                operation.get("seq").and_then(Value::as_u64).unwrap_or(0),
            )
        })
        .max_by_key(|(_, seq)| *seq)
        .map(|(index, _)| index);

    let latest_recovery = operations
        .iter()
        .enumerate()
        .filter(|(_, operation)| {
            operation.get("kind").and_then(Value::as_str) == Some("metadata_recover")
                && operation.get("status").and_then(Value::as_str) == Some("completed")
                && !["details_incomplete", "result_truncated"]
                    .iter()
                    .any(|field| operation.get(field).and_then(Value::as_bool) == Some(true))
                && operation
                    .get("result")
                    .is_some_and(is_usable_metadata_recovery_result)
        })
        .max_by_key(|(_, operation)| operation.get("seq").and_then(Value::as_u64).unwrap_or(0))
        .map(|(index, _)| index);

    let mut changed = false;
    for (index, operation) in operations.iter_mut().enumerate() {
        let Some(fields) = operation.as_object_mut() else {
            continue;
        };

        changed |= bound_operation_metadata(fields);
        if Some(index) == latest_legacy_import || Some(index) == latest_recovery {
            continue;
        }

        let Some(original) = fields.get("result").cloned() else {
            continue;
        };
        if json_size_within(&original, MAX_COMPACT_OPERATION_RESULT_BYTES).is_some() {
            continue;
        }

        let compacted = compact_result_summary(&original);
        if fields.get("result") != Some(&compacted) {
            fields.insert("result".to_string(), compacted);
            changed = true;
        }
        if fields.get("details_incomplete") != Some(&Value::Bool(true)) {
            fields.insert("details_incomplete".to_string(), Value::Bool(true));
            changed = true;
        }
        if fields.get("result_truncated") != Some(&Value::Bool(true)) {
            fields.insert("result_truncated".to_string(), Value::Bool(true));
            changed = true;
        }
    }

    changed
}

fn is_usable_completed_legacy_import_snapshot(operation: &Value) -> bool {
    operation.get("kind").and_then(Value::as_str) == Some("legacy_import")
        && operation.get("status").and_then(Value::as_str) == Some("completed")
        && !["details_incomplete", "result_truncated"]
            .iter()
            .any(|field| operation.get(field).and_then(Value::as_bool) == Some(true))
        && operation
            .get("result")
            .is_some_and(is_usable_legacy_import_result)
}

/// Build the latest active previews, with exact incomplete placeholders for omitted details.
pub(crate) fn preview_snapshots(
    records: &[&InboxRecord],
    omitted_preview_count: usize,
) -> Vec<Value> {
    let mut active = records
        .iter()
        .copied()
        .filter(|record| {
            (matches!(
                record.request.kind.as_str(),
                "file_preview" | "library_migration_preview"
            ) || record.request.is_staging_cleanup_preview())
                && record.status == InboxStatus::AwaitingConfirmation
        })
        .collect::<Vec<_>>();
    active.sort_by_key(|record| record.request.seq);

    // Do not publish a file preview without both its library ID and original root identity.
    // Legacy receipts remain visible only in the bounded operation history and cannot be used
    // to confirm against a later registration that reused the same path-derived ID.
    let invalid_file_preview_binding_count = active
        .iter()
        .filter(|record| {
            record.request.kind == "file_preview" && !file_preview_has_valid_root_binding(record)
        })
        .count();
    let invalid_staging_cleanup_preview_count = active
        .iter()
        .filter(|record| {
            record.request.is_staging_cleanup_preview()
                && staging_cleanup_preview_snapshot(record).is_none()
        })
        .count();
    active.retain(|record| {
        (record.request.kind != "file_preview" || file_preview_has_valid_root_binding(record))
            && (!record.request.is_staging_cleanup_preview()
                || staging_cleanup_preview_snapshot(record).is_some())
    });

    let locally_omitted = active.len().saturating_sub(MAX_PREVIEW_SNAPSHOT_RECORDS);
    if locally_omitted > 0 {
        active.drain(..locally_omitted);
    }
    let omitted_count = omitted_preview_count
        .saturating_add(invalid_file_preview_binding_count)
        .saturating_add(invalid_staging_cleanup_preview_count)
        .saturating_add(locally_omitted);

    // Reserve one bounded, actionable fallback for each valid migration preview before any full
    // preview can consume the shared budget. The cloud still confirms only the exact persisted
    // ID/revision; sample paths and omission counts never reconstruct or alter the local plan.
    let mut snapshots = active
        .iter()
        .map(|record| {
            let result = record.result.as_ref();
            if record.request.is_staging_cleanup_preview()
                && let Some(snapshot) = staging_cleanup_preview_snapshot(record)
            {
                return snapshot;
            }
            if record.request.kind == "file_preview"
                && let Some(result) = result
                && let Some(summary) = validated_file_preview_summary(record, result)
            {
                return file_preview_summary_snapshot(record, result, summary);
            }
            if record.request.kind == "library_migration_preview"
                && let Some(result) = result
                && validated_migration_preview_summary(record, result).is_some()
            {
                return result.clone();
            }
            if record.request.kind == "library_migration_preview"
                && let Some(result) = result
                && let Some(summary) = compact_migration_preview_summary(record, result)
            {
                return summary;
            }

            let is_migration = record.request.kind == "library_migration_preview";
            let details_present = result.is_some_and(|value| {
                if is_migration {
                    value.get("id").is_some() && value.get("items").is_some()
                } else {
                    value.get("preview").is_some()
                }
            });
            let reason = if details_present {
                PREVIEW_DETAILS_OMITTED_REASON
            } else {
                PREVIEW_DETAILS_UNAVAILABLE_REASON
            };
            let mut placeholder = preview_placeholder(
                record,
                preview_identity_value(result, "preview_id"),
                preview_identity_value(result, "revision"),
                reason,
            );
            if let Some(fields) = placeholder.as_object_mut() {
                fields.insert("kind".to_string(), json!(record.request.kind));
                if record.request.kind == "file_preview" {
                    if let Some(request_id) = file_preview_request_id(record) {
                        fields.insert("request_id".to_string(), json!(request_id));
                    }
                    if let FilePreviewLibraryId::Valid(library_id) = file_preview_library_id(record)
                    {
                        fields.insert("library_id".to_string(), json!(library_id));
                        if let Some(root_binding) = file_preview_root_binding(record) {
                            fields.insert("root_binding".to_string(), json!(root_binding));
                        }
                    }
                }
                if record.request.kind == "library_migration_preview"
                    && let Some(result) = result
                {
                    for (field, max_json_bytes) in [
                        ("source_library_id", 256),
                        ("source_root_binding", 80),
                        ("target_library_id", 256),
                        ("target_root_binding", 80),
                        ("scope", 64),
                        ("conflict", 64),
                        ("include_incomplete_tasks", 8),
                        ("set_target_default_on_success", 8),
                    ] {
                        if let Some(value) =
                            exact_bounded_scalar_field(result, field, max_json_bytes)
                        {
                            if matches!(field, "source_root_binding" | "target_root_binding")
                                && value
                                    .as_str()
                                    .is_none_or(|binding| !valid_root_binding_token(binding))
                            {
                                continue;
                            }
                            fields.insert(field.to_string(), value);
                        }
                    }
                }
            }
            placeholder
        })
        .collect::<Vec<_>>();
    if omitted_count > 0 {
        snapshots.push(json!({
            "preview_incomplete": true,
            "incomplete": true,
            "reload_required": true,
            "omitted_count": omitted_count,
            "reason": PREVIEWS_OMITTED_REASON,
        }));
    }
    let mut total_bytes = array_json_size(&snapshots, MAX_PREVIEW_SNAPSHOT_BYTES)
        .expect("preview placeholders should fit the bounded snapshot budget");

    for (index, record) in active.iter().enumerate().rev() {
        if record.request.is_staging_cleanup_preview() {
            continue;
        }
        let result = record.result.as_ref();
        if record.request.kind == "file_preview"
            && result.is_some_and(|result| {
                result
                    .get("preview")
                    .and_then(|preview| preview.get("file_preview_summary"))
                    .and_then(Value::as_bool)
                    == Some(true)
            })
        {
            continue;
        }
        if record.request.kind == "library_migration_preview"
            && result
                .is_some_and(|result| validated_migration_preview_summary(record, result).is_some())
        {
            continue;
        }
        let is_migration = record.request.kind == "library_migration_preview";
        let Some(details) = result.and_then(|value| {
            if is_migration {
                Some(value)
            } else {
                value.get("preview")
            }
        }) else {
            continue;
        };
        let preview_id = preview_identity_value(result, "preview_id");
        let revision = preview_identity_value(result, "revision");
        let placeholder_bytes = json_size_within(&snapshots[index], MAX_PREVIEW_SNAPSHOT_BYTES)
            .expect("preview placeholder should fit the bounded snapshot budget");
        let remaining = MAX_PREVIEW_SNAPSHOT_BYTES
            .saturating_sub(total_bytes.saturating_sub(placeholder_bytes));
        if json_size_within(details, remaining).is_none() {
            continue;
        }

        let mut candidate = if is_migration {
            json!({
                "seq": record.request.seq,
                "kind": record.request.kind,
                "preview_id": preview_id,
                "revision": revision,
                "status": "awaiting_confirmation",
                "result": details,
            })
        } else {
            json!({
                "seq": record.request.seq,
                "kind": record.request.kind,
                "preview_id": preview_id,
                "revision": revision,
                "status": "awaiting_confirmation",
                "preview": details,
            })
        };
        if !is_migration && let Some(fields) = candidate.as_object_mut() {
            if let Some(request_id) = file_preview_request_id(record) {
                fields.insert("request_id".to_string(), json!(request_id));
            }
            if let FilePreviewLibraryId::Valid(library_id) = file_preview_library_id(record) {
                fields.insert("library_id".to_string(), json!(library_id));
                if let Some(root_binding) = file_preview_root_binding(record) {
                    fields.insert("root_binding".to_string(), json!(root_binding));
                }
            }
        }
        let Some(candidate_bytes) = json_size_within(&candidate, remaining) else {
            continue;
        };
        let next_total = total_bytes
            .saturating_sub(placeholder_bytes)
            .saturating_add(candidate_bytes);
        if next_total <= MAX_PREVIEW_SNAPSHOT_BYTES {
            snapshots[index] = candidate;
            total_bytes = next_total;
        }
    }
    snapshots
}

fn file_preview_summary_snapshot(record: &InboxRecord, result: &Value, summary: &Value) -> Value {
    json!({
        "seq": record.request.seq,
        "kind": record.request.kind,
        "preview_id": result.get("preview_id"),
        "revision": result.get("revision"),
        "status": "awaiting_confirmation",
        "request_id": file_preview_request_id(record),
        "library_id": result.get("library_id"),
        "root_binding": result.get("root_binding"),
        "preview": summary,
        "incomplete": true,
        "preview_incomplete": true,
        "reload_required": true,
        "details_incomplete": true,
        "result_truncated": true,
        "reason": FILE_PREVIEW_SUMMARY_MESSAGE,
    })
}

/// The existing `migration_summary: true` envelope is the v1 migration preview receipt. Its
/// typed shape is also the only compact form accepted by migration confirmation; it never
/// supplies the execution plan, which is loaded from the source library's private state file.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MigrationPreviewSummaryItemV1 {
    pub(crate) item_id: String,
    pub(crate) source_path: String,
    pub(crate) target_path: String,
    pub(crate) bytes: u64,
    pub(crate) conflict: bool,
    pub(crate) status: String,
    pub(crate) outcome: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MigrationPreviewSummaryV1 {
    pub(crate) id: String,
    pub(crate) revision: String,
    pub(crate) source_library_id: String,
    pub(crate) source_root_binding: String,
    pub(crate) target_library_id: String,
    pub(crate) target_root_binding: String,
    pub(crate) scope: MigrationScope,
    pub(crate) conflict: ConflictPolicy,
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
    pub(crate) omitted_item_id_count: usize,
    pub(crate) omitted_conflict_choice_count: usize,
    pub(crate) omitted_warning_count: usize,
    pub(crate) omitted_item_count: usize,
    pub(crate) omitted_file_count: usize,
    pub(crate) sample_items: Vec<MigrationPreviewSummaryItemV1>,
}

/// Validate the exact v1 compact migration-preview envelope used by snapshots and receipts.
/// Callers that use its bindings for confirmation must also compare them with the originating
/// request and the complete preview persisted by MigrationEngine.
pub(crate) fn validated_migration_preview_summary(
    record: &InboxRecord,
    result: &Value,
) -> Option<MigrationPreviewSummaryV1> {
    let result_fields = result.as_object()?;
    if record.request.kind != "library_migration_preview"
        || record.status != InboxStatus::AwaitingConfirmation
        || result_fields.len() != 8
        || result_fields.keys().any(|field| {
            !matches!(
                field.as_str(),
                "seq"
                    | "kind"
                    | "preview_id"
                    | "revision"
                    | "status"
                    | "migration_summary"
                    | "reason"
                    | "result"
            )
        })
        || result.get("seq").and_then(Value::as_u64) != Some(record.request.seq)
        || result.get("kind").and_then(Value::as_str) != Some("library_migration_preview")
        || result.get("status").and_then(Value::as_str) != Some("awaiting_confirmation")
        || result.get("migration_summary").and_then(Value::as_bool) != Some(true)
        || result.get("reason").and_then(Value::as_str) != Some(MIGRATION_SUMMARY_REASON)
        || json_size_within(result, MAX_MIGRATION_PREVIEW_SUMMARY_BYTES).is_none()
    {
        return None;
    }

    let summary: MigrationPreviewSummaryV1 =
        serde_json::from_value(result.get("result")?.clone()).ok()?;
    let preview_id = result.get("preview_id")?.as_str()?;
    let revision = result.get("revision")?.as_str()?;
    if summary.id != preview_id
        || summary.revision != revision
        || !valid_library_id(&summary.source_library_id)
        || !valid_library_id(&summary.target_library_id)
        || !valid_root_binding_token(&summary.source_root_binding)
        || !valid_root_binding_token(&summary.target_root_binding)
        || summary.id.is_empty()
        || summary.revision.is_empty()
        || summary.id.chars().any(char::is_control)
        || summary.revision.chars().any(char::is_control)
        || summary.id.len() > MAX_PREVIEW_ID_JSON_BYTES
        || summary.revision.len() > MAX_PREVIEW_ID_JSON_BYTES
        || summary.sample_items.len() > MAX_MIGRATION_PREVIEW_SAMPLE_ITEMS
        || summary.item_count as u128 > MAX_MIGRATION_SUMMARY_SAFE_INTEGER as u128
        || summary.file_count as u128 > MAX_MIGRATION_SUMMARY_SAFE_INTEGER as u128
        || summary.conflict_count as u128 > MAX_MIGRATION_SUMMARY_SAFE_INTEGER as u128
        || summary.incomplete_task_count as u128 > MAX_MIGRATION_SUMMARY_SAFE_INTEGER as u128
        || summary.omitted_item_id_count as u128 > MAX_MIGRATION_SUMMARY_SAFE_INTEGER as u128
        || summary.omitted_conflict_choice_count as u128
            > MAX_MIGRATION_SUMMARY_SAFE_INTEGER as u128
        || summary.omitted_warning_count as u128 > MAX_MIGRATION_SUMMARY_SAFE_INTEGER as u128
        || summary.omitted_item_count as u128 > MAX_MIGRATION_SUMMARY_SAFE_INTEGER as u128
        || summary.omitted_file_count as u128 > MAX_MIGRATION_SUMMARY_SAFE_INTEGER as u128
        || summary.item_count.checked_sub(summary.sample_items.len())
            != Some(summary.omitted_item_count)
        || summary.omitted_file_count != summary.file_count
        || summary.omitted_item_id_count > summary.item_count
        || summary.omitted_conflict_choice_count > summary.item_count
        || summary.conflict_count > summary.item_count
        || summary.total_bytes > MAX_MIGRATION_SUMMARY_SAFE_INTEGER
        || summary.required_space_bytes > MAX_MIGRATION_SUMMARY_SAFE_INTEGER
        || summary.available_space_bytes > MAX_MIGRATION_SUMMARY_SAFE_INTEGER
        || summary.staging_bytes > MAX_MIGRATION_SUMMARY_SAFE_INTEGER
    {
        return None;
    }

    let mut seen_ids = std::collections::HashSet::with_capacity(summary.sample_items.len());
    for item in &summary.sample_items {
        if item.item_id.is_empty()
            || item.bytes > MAX_MIGRATION_SUMMARY_SAFE_INTEGER
            || json_size_within(
                &Value::String(item.item_id.clone()),
                MAX_MIGRATION_PREVIEW_SAMPLE_ID_JSON_BYTES,
            )
            .is_none()
            || item.source_path.is_empty()
            || json_size_within(
                &Value::String(item.source_path.clone()),
                MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES,
            )
            .is_none()
            || item.target_path.is_empty()
            || json_size_within(
                &Value::String(item.target_path.clone()),
                MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES,
            )
            .is_none()
            || !seen_ids.insert(item.item_id.as_str())
            || !matches!(
                (item.status.as_str(), item.outcome.as_str()),
                ("skipped", "skip") | ("copying", "move")
            )
        {
            return None;
        }
    }
    Some(summary)
}

/// Compact a complete migration preview before its oversized result is persisted. The
/// resulting envelope is the same actionable v1 summary already consumed by the Worker UI.
pub(crate) fn compact_library_migration_receipt_result(record: &InboxRecord) -> Option<Value> {
    let result = record.result.as_ref()?;
    match record.request.kind.as_str() {
        "library_migration_preview" => {
            let compact = compact_migration_preview_summary(record, result)?;
            validated_migration_preview_summary(record, &compact)?;
            Some(compact)
        }
        "library_migration_confirm" | "library_migration_control" => {
            compact_migration_status_receipt_result(record, result)
        }
        _ => None,
    }
}

fn compact_migration_status_receipt_result(record: &InboxRecord, result: &Value) -> Option<Value> {
    if !matches!(record.status, InboxStatus::Completed | InboxStatus::Failed) {
        return None;
    }
    let status: MigrationStatus = serde_json::from_value(result.get("migration")?.clone()).ok()?;
    if status.id.is_empty()
        || status.preview_id.is_empty()
        || status.revision.is_empty()
        || result
            .get("preview_id")
            .and_then(Value::as_str)
            .is_some_and(|preview_id| preview_id != status.preview_id.as_str())
        || result
            .get("revision")
            .and_then(Value::as_str)
            .is_some_and(|revision| revision != status.revision.as_str())
        || (record.request.kind == "library_migration_confirm"
            && (result.get("preview_id").and_then(Value::as_str)
                != Some(status.preview_id.as_str())
                || result.get("revision").and_then(Value::as_str)
                    != Some(status.revision.as_str())))
    {
        return None;
    }

    let result_object = result.as_object()?;
    if result_object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "preview_id" | "revision" | "migration" | "accepted"
        )
    }) || result
        .get("accepted")
        .is_some_and(|accepted| accepted.as_bool().is_none())
    {
        return None;
    }

    let mut compact_status = json!({
        "migration_status_summary": true,
        "summary_version": 1,
        "id": status.id,
        "preview_id": status.preview_id,
        "revision": status.revision,
        "created_at": status.created_at,
        "status": status.status,
        "source_library_id": status.source_library_id,
        "target_library_id": status.target_library_id,
        "scope": status.scope,
        "conflict": status.conflict,
        "include_incomplete_tasks": status.include_incomplete_tasks,
        "set_target_default_on_success": status.set_target_default_on_success,
        "default_update_applied": status.default_update_applied,
        "default_update_resolved": status.default_update_resolved,
        "terminal_finalization_pending": status.terminal_finalization_pending,
        "total_items": status.total_items,
        "completed_items": status.completed_items,
        "success_count": status.success_count,
        "skipped_count": status.skipped_count,
        "failed_count": status.failed_count,
        "total_files": status.total_files,
        "completed_files": status.completed_files,
        "copied_bytes": status.copied_bytes,
        "total_bytes": status.total_bytes,
        "required_space_bytes": status.required_space_bytes,
        "available_space_bytes": status.available_space_bytes,
        "incomplete_task_count": status.incomplete_task_count,
        "staging_bytes": status.staging_bytes,
        "awaiting_task_plans": status.awaiting_task_plans,
        "awaiting_task_commit": status.awaiting_task_commit,
        "omitted_item_count": status.items.len(),
        "omitted_item_conflict_count": status.item_conflicts.len(),
        "omitted_warning_count": status.warnings.len(),
        "omitted_task_transfer_count": status.task_transfers.len(),
        "details_incomplete": true,
        "result_truncated": true,
    });
    let compact_status_object = compact_status.as_object_mut()?;
    let warning_samples = status
        .warnings
        .iter()
        .take(MAX_MIGRATION_STATUS_SAMPLE_WARNINGS)
        .filter_map(|warning| bounded_json_string(warning, 512))
        .map(Value::String)
        .collect::<Vec<_>>();
    compact_status_object.insert("sample_warnings".to_string(), Value::Array(warning_samples));

    let mut compact = json!({
        "migration": compact_status,
        "details_incomplete": true,
        "result_truncated": true,
    });
    let compact_object = compact.as_object_mut()?;
    if record.request.kind == "library_migration_confirm" {
        compact_object.insert("preview_id".to_string(), json!(status.preview_id));
        compact_object.insert("revision".to_string(), json!(status.revision));
    }
    if let Some(accepted) = result.get("accepted") {
        compact_object.insert("accepted".to_string(), accepted.clone());
    }
    json_size_within(&compact, MAX_MIGRATION_STATUS_SUMMARY_BYTES).map(|_| compact)
}

fn compact_migration_preview_summary(record: &InboxRecord, result: &Value) -> Option<Value> {
    let preview_id = compact_identity(result, "preview_id")?;
    let revision = compact_identity(result, "revision")?;
    let preview_id_text = preview_id.as_str()?;
    let revision_text = revision.as_str()?;

    let id = exact_bounded_scalar_field(result, "id", MAX_PREVIEW_ID_JSON_BYTES)?;
    let result_revision =
        exact_bounded_scalar_field(result, "revision", MAX_PREVIEW_ID_JSON_BYTES)?;
    if id.as_str()? != preview_id_text || result_revision.as_str()? != revision_text {
        return None;
    }

    let source_library_id = exact_bounded_scalar_field(result, "source_library_id", 256)?;
    let target_library_id = exact_bounded_scalar_field(result, "target_library_id", 256)?;
    let source_root_binding = exact_bounded_scalar_field(result, "source_root_binding", 80)?;
    let target_root_binding = exact_bounded_scalar_field(result, "target_root_binding", 80)?;
    let scope = exact_bounded_scalar_field(result, "scope", 64)?;
    let conflict = exact_bounded_scalar_field(result, "conflict", 64)?;
    let include_incomplete_tasks =
        exact_bounded_scalar_field(result, "include_incomplete_tasks", 8)?;
    let set_target_default_on_success =
        exact_bounded_scalar_field(result, "set_target_default_on_success", 8)?;
    if source_library_id.as_str().is_none()
        || target_library_id.as_str().is_none()
        || !valid_root_binding_token(source_root_binding.as_str()?)
        || !valid_root_binding_token(target_root_binding.as_str()?)
        || scope.as_str().is_none()
        || conflict.as_str().is_none()
        || include_incomplete_tasks.as_bool().is_none()
        || set_target_default_on_success.as_bool().is_none()
    {
        return None;
    }

    let scalar_fields = [
        "item_count",
        "file_count",
        "total_bytes",
        "required_space_bytes",
        "available_space_bytes",
        "conflict_count",
        "incomplete_task_count",
        "staging_bytes",
    ];
    let mut aggregates = Map::new();
    for field in scalar_fields {
        let value = exact_bounded_scalar_field(result, field, 32)?;
        value.as_u64()?;
        aggregates.insert(field.to_string(), value);
    }

    let items = result.get("items")?.as_array()?;
    let item_count = usize::try_from(aggregates.get("item_count")?.as_u64()?).ok()?;
    let file_count = usize::try_from(aggregates.get("file_count")?.as_u64()?).ok()?;
    if item_count != items.len() {
        return None;
    }
    let item_ids = result.get("item_ids")?.as_array()?;
    if item_ids.len() != item_count || item_ids.iter().any(|item_id| item_id.as_str().is_none()) {
        return None;
    }
    let item_conflicts = result.get("item_conflicts")?.as_object()?;
    let warnings = result.get("warnings")?.as_array()?;

    let mut sample_items = Vec::with_capacity(items.len().min(MAX_MIGRATION_PREVIEW_SAMPLE_ITEMS));
    let mut sampled_item_ids = 0usize;
    for item in items.iter().take(MAX_MIGRATION_PREVIEW_SAMPLE_ITEMS) {
        let Some(original_item_id) = item.get("item_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(item_id) =
            bounded_json_string(original_item_id, MAX_MIGRATION_PREVIEW_SAMPLE_ID_JSON_BYTES)
        else {
            continue;
        };
        let Some(source_path) = item
            .get("source_path")
            .and_then(Value::as_str)
            .and_then(|value| {
                bounded_json_string(value, MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES)
            })
        else {
            continue;
        };
        let Some(target_path) = item
            .get("target_path")
            .and_then(Value::as_str)
            .and_then(|value| {
                bounded_json_string(value, MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES)
            })
        else {
            continue;
        };
        if !item_ids
            .iter()
            .any(|candidate| candidate.as_str() == Some(original_item_id))
        {
            return None;
        }
        let bytes = item.get("bytes")?.as_u64()?;
        let conflict = item.get("conflict")?.as_bool()?;
        let status = item.get("status")?.as_str()?;
        let outcome = match status {
            "skipped" => "skip",
            "copying" => "move",
            _ => return None,
        };
        if item_id == original_item_id {
            sampled_item_ids = sampled_item_ids.saturating_add(1);
        }
        sample_items.push(json!({
            "item_id": item_id,
            "source_path": source_path,
            "target_path": target_path,
            "bytes": bytes,
            "conflict": conflict,
            "status": status,
            "outcome": outcome,
        }));
    }

    let mut summary = json!({
        "id": id,
        "revision": result_revision,
        "source_library_id": source_library_id,
        "source_root_binding": source_root_binding,
        "target_library_id": target_library_id,
        "target_root_binding": target_root_binding,
        "scope": scope,
        "conflict": conflict,
        "include_incomplete_tasks": include_incomplete_tasks,
        "set_target_default_on_success": set_target_default_on_success,
        "omitted_item_id_count": item_ids.len().saturating_sub(sampled_item_ids),
        "omitted_conflict_choice_count": item_conflicts.len(),
        "omitted_warning_count": warnings.len(),
        "omitted_item_count": item_count.saturating_sub(sample_items.len()),
        "omitted_file_count": file_count,
        "sample_items": sample_items,
    });
    if let Some(fields) = summary.as_object_mut() {
        fields.extend(aggregates);
    }
    let envelope = json!({
        "seq": record.request.seq,
        "kind": record.request.kind,
        "preview_id": preview_id,
        "revision": revision,
        "status": "awaiting_confirmation",
        "migration_summary": true,
        "reason": MIGRATION_SUMMARY_REASON,
        "result": summary,
    });
    json_size_within(&envelope, MAX_MIGRATION_PREVIEW_SUMMARY_BYTES)?;
    Some(envelope)
}

fn valid_root_binding_token(value: &str) -> bool {
    value.len() == 67
        && value.starts_with("v1:")
        && value[3..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_library_id(value: &str) -> bool {
    !value.trim().is_empty()
        && value.trim() == value
        && value.len() <= MAX_LIBRARY_ID_BYTES
        && !value.chars().any(char::is_control)
        && json_size_within(&Value::String(value.to_string()), MAX_LIBRARY_ID_JSON_BYTES).is_some()
}

fn exact_bounded_scalar_field(result: &Value, field: &str, max_json_bytes: usize) -> Option<Value> {
    let candidate = result.get(field)?;
    let compacted = compact_scalar_value(candidate, max_json_bytes)?;
    (compacted == *candidate).then_some(compacted)
}

fn metadata_recovery_receipt_identity(record: &InboxRecord) -> Option<Value> {
    if record.request.kind != "metadata_recover" {
        return None;
    }
    let payload = &record.request.payload;
    let library_id = payload.get("library_id")?.as_str()?;
    let root_binding = payload.get("root_binding")?.as_str()?;
    let scope = payload.get("scope")?.as_str()?;
    if !valid_library_id(library_id)
        || !valid_root_binding_token(root_binding)
        || !matches!(scope, "selected" | "library")
    {
        return None;
    }
    Some(json!({
        "library_id": library_id,
        "root_binding": root_binding,
        "scope": scope,
    }))
}

fn staging_cleanup_request_identity(record: &InboxRecord) -> Option<Map<String, Value>> {
    let operation = record.request.staging_cleanup_operation()?;
    if !matches!(operation, "scan" | "preview" | "confirm" | "cancel") {
        return None;
    }
    let payload = &record.request.payload;
    let library_id = payload.get("library_id")?.as_str()?;
    let root_binding = payload.get("root_binding")?.as_str()?;
    if !valid_library_id(library_id) || !valid_root_binding_token(root_binding) {
        return None;
    }
    let mut identity = Map::new();
    identity.insert("operation".into(), json!(operation));
    identity.insert("library_id".into(), json!(library_id));
    identity.insert("root_binding".into(), json!(root_binding));
    if matches!(operation, "confirm" | "cancel") {
        let preview_seq = payload.get("preview_seq")?.as_u64()?;
        if preview_seq == 0 || preview_seq > MAX_MIGRATION_SUMMARY_SAFE_INTEGER {
            return None;
        }
        identity.insert("preview_seq".into(), json!(preview_seq));
        for (field, max_bytes) in [("preview_id", 128), ("revision", 256)] {
            let value = payload.get(field)?.as_str()?;
            if value.is_empty()
                || value.len() > max_bytes
                || value.trim() != value
                || value.chars().any(char::is_control)
            {
                return None;
            }
            identity.insert(field.into(), json!(value));
        }
    }
    Some(identity)
}

fn operation_snapshot(record: &InboxRecord, include_full_result: bool) -> Value {
    let result = record.result.as_ref().map(|result| {
        if record.request.kind == "library_scan" {
            library_scan_receipt(result)
        } else if record.request.kind == "staging_cleanup" {
            let public_result = staging_cleanup_public_result(result);
            if include_full_result {
                public_result
            } else {
                compact_result_summary(&public_result)
            }
        } else if include_full_result {
            result.clone()
        } else {
            compact_result_summary(result)
        }
    });
    let result_was_compacted =
        (!include_full_result && record.request.kind != "library_scan" && record.result.is_some())
            || (record.request.kind == "file_preview"
                && record
                    .result
                    .as_ref()
                    .and_then(|result| result.get("preview"))
                    .and_then(|preview| preview.get("file_preview_summary"))
                    .and_then(Value::as_bool)
                    == Some(true))
            || (record.request.kind == "file_confirm"
                && record
                    .result
                    .as_ref()
                    .and_then(|result| result.get("operation"))
                    .and_then(|operation| operation.get("file_confirm_summary"))
                    .and_then(Value::as_bool)
                    == Some(true));
    let kind = bounded_operation_kind(&record.request.kind);
    let error = bounded_operation_error(record.error.as_deref());
    let kind_truncated = kind != record.request.kind;
    let error_truncated = error.as_deref() != record.error.as_deref();
    let mut operation = json!({
        "seq": record.request.seq,
        "kind": kind,
        "status": status_name(record.status),
        "created_at": record.request.created_at,
        "updated_at": record.updated_at,
        "attempts": record.attempts,
        "next_attempt_at": record.next_attempt_at,
        "result": result,
        "error": error,
    });
    if let Some(identity) = staging_cleanup_request_identity(record)
        && let Some(fields) = operation.as_object_mut()
    {
        fields.extend(identity);
    }
    if record.request.kind == "file_preview"
        && let Some(fields) = operation.as_object_mut()
    {
        if let Some(request_id) = file_preview_request_id(record) {
            fields.insert("request_id".to_string(), json!(request_id));
        }
        if let Some((library_id, root_binding)) = file_preview_library_identity(record) {
            fields.insert("library_id".to_string(), json!(library_id));
            fields.insert("root_binding".to_string(), json!(root_binding));
        }
    }
    if let Some(identity) = metadata_recovery_receipt_identity(record) {
        operation["recovery_identity"] = identity;
    }
    if result_was_compacted || kind_truncated || error_truncated {
        let fields = operation
            .as_object_mut()
            .expect("operation snapshot object should be created by JSON macro");
        if result_was_compacted {
            fields.insert("details_incomplete".to_string(), Value::Bool(true));
            fields.insert("result_truncated".to_string(), Value::Bool(true));
        }
        if kind_truncated {
            fields.insert("kind_truncated".to_string(), Value::Bool(true));
        }
        if error_truncated {
            fields.insert("error_truncated".to_string(), Value::Bool(true));
        }
    }
    operation
}

fn staging_cleanup_public_result(result: &Value) -> Value {
    let mut public_result = result.clone();
    strip_staging_cleanup_proofs(&mut public_result);
    public_result
}

fn strip_staging_cleanup_proofs(value: &mut Value) {
    match value {
        Value::Array(values) => {
            for value in values {
                strip_staging_cleanup_proofs(value);
            }
        }
        Value::Object(fields) => {
            fields.remove("proofs");
            fields.remove("cleanup_preview");
            for value in fields.values_mut() {
                strip_staging_cleanup_proofs(value);
            }
        }
        _ => {}
    }
}

fn staging_cleanup_preview_snapshot(record: &InboxRecord) -> Option<Value> {
    if !record.request.is_staging_cleanup_preview()
        || record.status != InboxStatus::AwaitingConfirmation
    {
        return None;
    }
    let result = record.result.as_ref()?;
    if result.get("operation").and_then(Value::as_str) != Some("preview") {
        return None;
    }
    let preview = result.get("preview")?;
    let preview_id = result.get("preview_id")?.as_str()?;
    let revision = result.get("revision")?.as_str()?;
    let library_id = result.get("library_id")?.as_str()?;
    let root_binding = result.get("root_binding")?.as_str()?;
    if !valid_library_id(library_id)
        || !valid_root_binding_token(root_binding)
        || record
            .request
            .payload
            .get("library_id")
            .and_then(Value::as_str)
            != Some(library_id)
        || record
            .request
            .payload
            .get("root_binding")
            .and_then(Value::as_str)
            != Some(root_binding)
        || preview.get("id").and_then(Value::as_str) != Some(preview_id)
        || preview.get("revision").and_then(Value::as_str) != Some(revision)
        || preview.get("library_id").and_then(Value::as_str) != Some(library_id)
        || preview.get("root_binding").and_then(Value::as_str) != Some(root_binding)
    {
        return None;
    }
    let mut public_preview = preview.clone();
    strip_staging_cleanup_proofs(&mut public_preview);
    json_size_within(&public_preview, MAX_PREVIEW_SNAPSHOT_BYTES)?;
    Some(json!({
        "seq": record.request.seq,
        "kind": "staging_cleanup",
        "preview_id": preview_id,
        "revision": revision,
        "status": "awaiting_confirmation",
        "library_id": library_id,
        "root_binding": root_binding,
        "preview": public_preview,
    }))
}

fn library_scan_receipt(result: &Value) -> Value {
    let mut receipt = Map::new();
    if let Some(revision) = result.get("revision") {
        receipt.insert("revision".to_string(), revision.clone());
    }
    if let Some(scanned_at) = result.get("scanned_at") {
        receipt.insert("scanned_at".to_string(), scanned_at.clone());
    }
    Value::Object(receipt)
}

fn bound_operation_metadata(fields: &mut Map<String, Value>) -> bool {
    let mut changed = false;
    if let Some(kind) = fields.get("kind").and_then(Value::as_str)
        && let Some(bounded) = bounded_json_string(kind, MAX_OPERATION_KIND_JSON_BYTES)
        && bounded != kind
    {
        fields.insert("kind".to_string(), Value::String(bounded));
        fields.insert("kind_truncated".to_string(), Value::Bool(true));
        changed = true;
    }
    if let Some(error) = fields.get("error").and_then(Value::as_str)
        && let Some(bounded) = bounded_json_string(error, MAX_OPERATION_ERROR_JSON_BYTES)
        && bounded != error
    {
        fields.insert("error".to_string(), Value::String(bounded));
        fields.insert("error_truncated".to_string(), Value::Bool(true));
        changed = true;
    }
    changed
}

fn compact_result_summary(result: &Value) -> Value {
    let mut summary = Map::new();
    summary.insert("incomplete".to_string(), Value::Bool(true));
    summary.insert(
        "reason".to_string(),
        Value::String(OPERATION_DETAILS_OMITTED_REASON.to_string()),
    );

    for (key, value) in [
        ("preview_id", compact_identity(result, "preview_id")),
        ("revision", compact_identity(result, "revision")),
        (
            "library_id",
            compact_scalar_field(result, "library_id", MAX_LIBRARY_ID_BYTES * 6 + 2),
        ),
        (
            "root_binding",
            compact_scalar_field(result, "root_binding", 80),
        ),
        ("status", compact_scalar_field(result, "status", 128)),
        ("error", compact_scalar_field(result, "error", 512)),
        ("scanned_at", compact_scalar_field(result, "scanned_at", 64)),
    ] {
        let Some(value) = value else {
            continue;
        };
        summary.insert(key.to_string(), value);
        if json_size_within(
            &Value::Object(summary.clone()),
            MAX_COMPACT_OPERATION_RESULT_BYTES,
        )
        .is_none()
        {
            summary.remove(key);
        }
    }

    let summary = Value::Object(summary);
    if json_size_within(&summary, MAX_COMPACT_OPERATION_RESULT_BYTES).is_some() {
        summary
    } else {
        json!({"incomplete": true})
    }
}

fn compact_identity(result: &Value, field: &str) -> Option<Value> {
    let preview = result.get("preview");
    let candidate = result
        .get(field)
        .or_else(|| preview.and_then(|value| value.get(field)))
        .or_else(|| {
            (field == "preview_id")
                .then(|| preview.and_then(|value| value.get("id")))
                .flatten()
        })
        .or_else(|| (field == "preview_id").then(|| result.get("id")).flatten())?;
    let value = compact_scalar_value(candidate, MAX_PREVIEW_ID_JSON_BYTES)?;
    (candidate == &value).then_some(value)
}

fn compact_scalar_field(result: &Value, field: &str, max_json_bytes: usize) -> Option<Value> {
    let candidate = result
        .get(field)
        .or_else(|| result.get("operation").and_then(|value| value.get(field)))?;
    compact_scalar_value(candidate, max_json_bytes)
}

fn compact_scalar_value(value: &Value, max_json_bytes: usize) -> Option<Value> {
    match value {
        Value::String(text) => bounded_json_string(text, max_json_bytes).map(Value::String),
        Value::Bool(_) | Value::Number(_) | Value::Null => {
            json_size_within(value, max_json_bytes).map(|_| value.clone())
        }
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn preview_identity_value(result: Option<&Value>, field: &str) -> Value {
    result
        .and_then(|value| {
            value
                .get(field)
                .or_else(|| value.get("preview").and_then(|preview| preview.get(field)))
                .or_else(|| {
                    (field == "preview_id")
                        .then(|| value.get("preview").and_then(|preview| preview.get("id")))
                        .flatten()
                })
                .or_else(|| (field == "preview_id").then(|| value.get("id")).flatten())
        })
        .and_then(identity_scalar_value)
        .unwrap_or(Value::Null)
}

fn identity_scalar_value(value: &Value) -> Option<Value> {
    match value {
        Value::String(_) | Value::Bool(_) | Value::Number(_) | Value::Null => {
            json_size_within(value, MAX_PREVIEW_ID_JSON_BYTES).map(|_| value.clone())
        }
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn preview_placeholder(
    record: &InboxRecord,
    preview_id: Value,
    revision: Value,
    reason: &str,
) -> Value {
    json!({
        "seq": record.request.seq,
        "preview_id": preview_id,
        "revision": revision,
        "status": "awaiting_confirmation",
        "preview_incomplete": true,
        "incomplete": true,
        "reload_required": true,
        "reason": reason,
    })
}

enum FilePreviewLibraryId<'a> {
    Absent,
    Valid(&'a str),
    Invalid,
}

fn file_preview_library_id(record: &InboxRecord) -> FilePreviewLibraryId<'_> {
    let Some(value) = record
        .result
        .as_ref()
        .and_then(|result| result.get("library_id"))
    else {
        return FilePreviewLibraryId::Absent;
    };
    let Some(library_id) = value.as_str() else {
        return FilePreviewLibraryId::Invalid;
    };
    if !valid_library_id(library_id) {
        FilePreviewLibraryId::Invalid
    } else {
        FilePreviewLibraryId::Valid(library_id)
    }
}

fn file_preview_root_binding(record: &InboxRecord) -> Option<&str> {
    record
        .result
        .as_ref()?
        .get("root_binding")?
        .as_str()
        .filter(|binding| valid_root_binding_token(binding))
}

fn file_preview_library_identity(record: &InboxRecord) -> Option<(&str, &str)> {
    if record.request.kind != "file_preview" {
        return None;
    }

    let payload = &record.request.payload;
    let has_payload_identity =
        payload.get("library_id").is_some() || payload.get("root_binding").is_some();
    let identity = if has_payload_identity {
        Some((payload.get("library_id")?, payload.get("root_binding")?))
    } else {
        let result = record.result.as_ref()?;
        Some((result.get("library_id")?, result.get("root_binding")?))
    }?;
    let (library_id, root_binding) = (identity.0.as_str()?, identity.1.as_str()?);
    (valid_library_id(library_id) && valid_root_binding_token(root_binding))
        .then_some((library_id, root_binding))
}

fn file_preview_request_id(record: &InboxRecord) -> Option<&str> {
    if record.request.kind != "file_preview" {
        return None;
    }
    let request_id = record.request.extra.get("request_id")?.as_str()?;
    (!request_id.is_empty()
        && request_id.trim() == request_id
        && request_id.len() <= MAX_CLOUD_REQUEST_ID_BYTES
        && !request_id.chars().any(char::is_control))
    .then_some(request_id)
}

fn file_preview_has_valid_root_binding(record: &InboxRecord) -> bool {
    matches!(
        file_preview_library_id(record),
        FilePreviewLibraryId::Valid(_)
    ) && file_preview_root_binding(record).is_some()
}

fn bounded_operation_kind(kind: &str) -> String {
    bounded_json_string(kind, MAX_OPERATION_KIND_JSON_BYTES).unwrap_or_default()
}

fn bounded_operation_error(error: Option<&str>) -> Option<String> {
    error.and_then(|value| bounded_json_string(value, MAX_OPERATION_ERROR_JSON_BYTES))
}

fn bounded_json_string(input: &str, max_json_bytes: usize) -> Option<String> {
    if json_size_within(&Value::String(input.to_string()), max_json_bytes).is_some() {
        return Some(input.to_string());
    }
    if max_json_bytes <= 2 {
        return None;
    }

    // Six output bytes per input byte is a safe upper bound for JSON-escaped control chars.
    let max_prefix_bytes = max_json_bytes.saturating_sub(5) / 6;
    let mut end = input.len().min(max_prefix_bytes);
    while !input.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    let mut bounded = input[..end].to_string();
    if end < input.len() {
        bounded.push('…');
    }
    if json_size_within(&Value::String(bounded.clone()), max_json_bytes).is_some() {
        Some(bounded)
    } else {
        None
    }
}

fn status_name(status: InboxStatus) -> &'static str {
    match status {
        InboxStatus::Pending => "pending",
        InboxStatus::Processing => "processing",
        InboxStatus::AwaitingConfirmation => "awaiting_confirmation",
        InboxStatus::Completed => "completed",
        InboxStatus::Retryable => "retryable",
        InboxStatus::Failed => "failed",
        InboxStatus::Uncertain => "uncertain",
        InboxStatus::Stale => "stale",
        InboxStatus::Cancelled => "cancelled",
    }
}

fn json_size_within<T: Serialize>(value: &T, limit: usize) -> Option<usize> {
    let mut counter = LimitedJsonCounter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value).ok()?;
    Some(counter.bytes)
}

fn array_json_size(values: &[Value], limit: usize) -> Option<usize> {
    let mut bytes = 2usize;
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            bytes = bytes.saturating_add(1);
        }
        let remaining = limit.saturating_sub(bytes);
        bytes = bytes.saturating_add(json_size_within(value, remaining)?);
        if bytes > limit {
            return None;
        }
    }
    Some(bytes)
}

struct LimitedJsonCounter {
    bytes: usize,
    limit: usize,
}

impl Write for LimitedJsonCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next = self.bytes.saturating_add(buffer.len());
        if next > self.limit {
            return Err(io::Error::other("serialized JSON exceeds its size budget"));
        }
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::CloudRequest;

    const MAX_COMPACT_OPERATION_ARRAY_BYTES: usize = 512 * 1024;
    const TEST_ROOT_BINDING: &str =
        "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn record(
        seq: u64,
        kind: &str,
        status: InboxStatus,
        result: Option<Value>,
        error: Option<String>,
    ) -> InboxRecord {
        let mut result = result;
        if kind == "file_preview"
            && let Some(fields) = result.as_mut().and_then(Value::as_object_mut)
        {
            fields
                .entry("library_id")
                .or_insert_with(|| json!("ml-test-root"));
            fields
                .entry("root_binding")
                .or_insert_with(|| json!(TEST_ROOT_BINDING));
        }
        InboxRecord {
            schema_version: 1,
            request: CloudRequest {
                seq,
                kind: kind.to_string(),
                payload: json!({}),
                created_at: seq * 10,
                extra: Default::default(),
            },
            status,
            attempts: 1,
            received_at: None,
            updated_at: seq * 10 + 1,
            next_attempt_at: 0,
            error,
            result,
        }
    }

    fn file_preview_with_request(
        seq: u64,
        status: InboxStatus,
        result: Option<Value>,
        request_id: Value,
    ) -> InboxRecord {
        let mut receipt = record(seq, "file_preview", status, result, None);
        receipt.request.payload = json!({
            "library_id": "ml-source-root",
            "root_binding": TEST_ROOT_BINDING,
        });
        receipt
            .request
            .extra
            .insert("request_id".to_string(), request_id);
        receipt
    }

    fn generated_file_preview_record(seq: u64, item_count: usize) -> InboxRecord {
        let item_ids = (0..item_count)
            .map(|index| format!("item-{index:04}"))
            .collect::<Vec<_>>();
        let items = item_ids
            .iter()
            .enumerate()
            .map(|(index, item_id)| {
                let files = (0..4)
                    .map(|file_index| {
                        let extension = ["mkv", "nfo", "srt", "jpg"][file_index];
                        json!({
                            "source_path": format!("source/{index}/{}.{}", "影片附件名字".repeat(10), extension),
                            "target_path": format!("target/{index}/{}.{}", "影片附件名字".repeat(10), extension),
                            "bytes": 256,
                            "identity": "stable-file-identity",
                        })
                    })
                    .collect::<Vec<_>>();
                json!({
                    "item_id": item_id,
                    "source_path": format!("source/{index}/video.mkv"),
                    "target_path": format!("target/{index}/video.mkv"),
                    "outcome": "move",
                    "files": files,
                    "reason": null,
                })
            })
            .collect::<Vec<_>>();
        let preview_id = format!("preview-{seq}");
        let revision = format!("revision-{seq}");
        let mut receipt = record(
            seq,
            "file_preview",
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "preview_id": preview_id,
                "revision": revision,
                "library_id": "ml-test-root",
                "root_binding": TEST_ROOT_BINDING,
                "preview": {
                    "id": format!("preview-{seq}"),
                    "revision": format!("revision-{seq}"),
                    "target_relative_dir": "Moved",
                    "rename": false,
                    "conflict": "skip",
                    "items": items,
                    "metadata_patches": [],
                },
            })),
            None,
        );
        receipt.request.payload = json!({
            "library_id": "ml-test-root",
            "root_binding": TEST_ROOT_BINDING,
            "item_ids": item_ids,
            "target_relative_dir": "Moved",
            "rename": false,
            "conflict": "skip",
            "metadata_patches": [],
        });
        receipt
    }

    fn generated_file_confirm_record(
        seq: u64,
        item_count: usize,
        include_item_errors: bool,
    ) -> InboxRecord {
        let items = (0..item_count)
            .map(|index| {
                json!({
                    "item_id": format!("item-{index:04}-{}", "x".repeat(220)),
                    "outcome": "move",
                    "moved_files": 4,
                    "total_files": 4,
                    "metadata_patched": index % 2 == 0,
                    "error": include_item_errors.then(|| "temporary synthetic access failure".to_string()),
                })
            })
            .collect::<Vec<_>>();
        let id = format!("preview-{seq}");
        let revision = format!("revision-{seq}");
        record(
            seq,
            "file_confirm",
            InboxStatus::Completed,
            Some(json!({
                "preview_id": id,
                "revision": revision,
                "operation": {
                    "id": format!("preview-{seq}"),
                    "revision": format!("revision-{seq}"),
                    "status": "complete",
                    "retryable_access_error": false,
                    "items": items,
                    "warnings": [],
                },
                "library_refresh_warning": null,
                "telegram_progress_warning": null,
            })),
            None,
        )
    }

    fn generated_metadata_only_preview_record(seq: u64, item_count: usize) -> InboxRecord {
        let request_patches = (0..item_count)
            .map(|index| {
                json!({
                    "item_id": format!("item-{index:04}"),
                    "title": "Metadata title",
                })
            })
            .collect::<Vec<_>>();
        let preview_patches = (0..item_count)
            .map(|index| {
                json!({
                    "item_id": format!("item-{index:04}"),
                    "nfo_path": format!("item-{index:04}.nfo"),
                    "changed": false,
                    "added_source_ids": [],
                    "changes": [],
                })
            })
            .collect::<Vec<_>>();
        let id = format!("preview-{seq}");
        let revision = format!("revision-{seq}");
        let mut receipt = record(
            seq,
            "file_preview",
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "preview_id": id,
                "revision": revision,
                "library_id": "ml-test-root",
                "root_binding": TEST_ROOT_BINDING,
                "preview": {
                    "id": format!("preview-{seq}"),
                    "revision": format!("revision-{seq}"),
                    "target_relative_dir": null,
                    "rename": false,
                    "conflict": "skip",
                    "items": [],
                    "metadata_patches": preview_patches,
                },
            })),
            None,
        );
        receipt.request.payload = json!({
            "library_id": "ml-test-root",
            "root_binding": TEST_ROOT_BINDING,
            "metadata_patches": request_patches,
        });
        receipt
    }

    fn legacy_import_result(payload_bytes: Option<usize>, warnings: &[&str]) -> Value {
        let candidates = payload_bytes
            .map(|payload_bytes| {
                vec![json!({
                    "hint_index": 0,
                    "item_id": "item-1",
                    "payload": "x".repeat(payload_bytes),
                })]
            })
            .unwrap_or_default();
        json!({
            "library_id": "ml-test-root",
            "root_binding": TEST_ROOT_BINDING,
            "hints": [{"hint_index": 0, "title": "Star Wars", "source_ids": []}],
            "candidates": candidates,
            "warnings": warnings,
        })
    }

    fn metadata_recovery_result(reason_bytes: usize) -> Value {
        json!({
            "library_id": "ml-test-root",
            "root_binding": TEST_ROOT_BINDING,
            "scope": "library",
            "recovery": {
                "revision": "recovery-revision",
                "items": [{
                    "item_id": "recovery-item",
                    "title": "Recovered media",
                    "relative_path": "media.mp4",
                    "status": "ready",
                    "reason": "x".repeat(reason_bytes),
                    "patch": {"item_id": "recovery-item", "title": "Recovered media", "fill_missing_only": true}
                }],
                "total": 1,
                "remaining": 0,
                "next_cursor": null
            }
        })
    }

    #[test]
    fn recovery_receipts_publish_bounded_binding_before_completion_without_payload() {
        for (index, status) in [
            InboxStatus::Pending,
            InboxStatus::Processing,
            InboxStatus::AwaitingConfirmation,
            InboxStatus::Retryable,
            InboxStatus::Uncertain,
            InboxStatus::Failed,
            InboxStatus::Stale,
            InboxStatus::Completed,
        ]
        .into_iter()
        .enumerate()
        {
            let mut receipt = record(index as u64 + 1, "metadata_recover", status, None, None);
            receipt.request.payload = json!({
                "library_id": "ml-test-root",
                "root_binding": TEST_ROOT_BINDING,
                "scope": "selected",
                "item_ids": ["private-item-marker"],
                "private": "private-payload-marker".repeat(4096),
            });
            let mut snapshots = operation_snapshots(&[&receipt]).unwrap();
            let expected = json!({
                "library_id": "ml-test-root",
                "root_binding": TEST_ROOT_BINDING,
                "scope": "selected",
            });
            assert_eq!(snapshots[0]["recovery_identity"], expected);
            assert!(snapshots[0]["result"].is_null());
            let encoded = serde_json::to_string(&snapshots).unwrap();
            assert!(!encoded.contains("private-item-marker"));
            assert!(!encoded.contains("private-payload-marker"));
            assert!(encoded.len() < 2048);
            compact_operation_receipts(&mut snapshots);
            assert_eq!(snapshots[0]["recovery_identity"], expected);
        }
    }

    #[test]
    fn recovery_receipt_identity_rejects_invalid_bindings_and_unrelated_kinds() {
        let valid = json!({
            "library_id": "ml-test-root", "root_binding": TEST_ROOT_BINDING, "scope": "library",
        });
        for (field, invalid) in [
            ("library_id", json!(" ")),
            ("library_id", json!("x".repeat(MAX_LIBRARY_ID_BYTES + 1))),
            ("library_id", json!("control\ncharacter")),
            ("root_binding", json!("unverified-root")),
            ("root_binding", Value::Null),
            ("scope", json!("unknown")),
        ] {
            let mut receipt = record(1, "metadata_recover", InboxStatus::Failed, None, None);
            receipt.request.payload = valid.clone();
            receipt.request.payload[field] = invalid;
            assert!(
                operation_snapshot(&receipt, false)
                    .get("recovery_identity")
                    .is_none()
            );
        }
        let mut unrelated = record(2, "library_scan", InboxStatus::Processing, None, None);
        unrelated.request.payload = valid;
        assert!(
            operation_snapshot(&unrelated, false)
                .get("recovery_identity")
                .is_none()
        );
    }

    #[test]
    fn recovery_report_survives_newer_large_receipts_and_upload_compaction() {
        let latest = metadata_recovery_result(100 * 1024);
        let records = [
            record(
                1,
                "metadata_recover",
                InboxStatus::Completed,
                Some(metadata_recovery_result(90 * 1024)),
                None,
            ),
            record(
                2,
                "metadata_recover",
                InboxStatus::Completed,
                Some(latest.clone()),
                None,
            ),
            record(
                3,
                "legacy_import",
                InboxStatus::Completed,
                Some(legacy_import_result(Some(900 * 1024), &[])),
                None,
            ),
            record(
                4,
                "file_confirm",
                InboxStatus::Completed,
                Some(json!({"payload": "x".repeat(1500 * 1024)})),
                None,
            ),
        ];
        let mut snapshots = operation_snapshots(&records.iter().collect::<Vec<_>>())
            .expect("bounded actionable reports should fit");
        assert_eq!(snapshots[1]["result"], latest);
        assert_eq!(snapshots[2]["result"]["hints"].as_array().unwrap().len(), 1);
        assert_ne!(snapshots[1]["details_incomplete"], true);
        assert!(compact_operation_receipts(&mut snapshots));
        assert_eq!(snapshots[1]["result"], latest);
        assert_ne!(snapshots[1]["details_incomplete"], true);
        assert_eq!(snapshots[0]["details_incomplete"], true);
        assert!(array_json_size(&snapshots, MAX_OPERATION_SNAPSHOT_BYTES).is_some());
    }

    #[test]
    fn recovery_compaction_does_not_reserve_unbound_oversized_or_incomplete_results() {
        let valid = metadata_recovery_result(4096);
        let mut unbound = metadata_recovery_result(4096);
        unbound["root_binding"] = json!("invalid-binding");
        let oversized = metadata_recovery_result(MAX_METADATA_RECOVERY_RESULT_BYTES);
        let mut incomplete = metadata_recovery_result(4096);
        incomplete["recovery"]["incomplete"] = json!(true);
        let mut snapshots = vec![
            json!({"seq": 1, "kind": "metadata_recover", "status": "completed", "result": valid.clone()}),
            json!({"seq": 2, "kind": "metadata_recover", "status": "completed", "result": unbound}),
            json!({"seq": 3, "kind": "metadata_recover", "status": "completed", "result": oversized}),
            json!({"seq": 4, "kind": "metadata_recover", "status": "completed", "result": incomplete}),
        ];
        assert!(compact_operation_receipts(&mut snapshots));
        assert_eq!(snapshots[0]["result"], valid);
        for snapshot in &snapshots[1..] {
            assert_eq!(snapshot["details_incomplete"], true);
            assert_eq!(snapshot["result_truncated"], true);
            assert!(snapshot["result"].get("recovery").is_none());
        }
    }

    fn migration_preview_result(
        seq: u64,
        item_count: usize,
        files_per_item: usize,
        nested_path_bytes: usize,
        sample_path_bytes: usize,
    ) -> Value {
        let item_ids = (0..item_count)
            .map(|index| format!("media-item-{seq}-{index}"))
            .collect::<Vec<_>>();
        let item_conflicts = item_ids
            .iter()
            .take(3)
            .map(|item_id| (item_id.clone(), json!("keep_both")))
            .collect::<Map<String, Value>>();
        let items = item_ids
            .iter()
            .enumerate()
            .map(|(index, item_id)| {
                let nested_path = "nested/".repeat(nested_path_bytes / 7 + 1);
                let files = (0..files_per_item)
                    .map(|file_index| {
                        json!({
                            "source_path": format!("source/{nested_path}{file_index}.mkv"),
                            "target_path": format!("target/{nested_path}{file_index}.mkv"),
                            "size_bytes": 1024,
                        })
                    })
                    .collect::<Vec<_>>();
                let sample_path = "segment/".repeat(sample_path_bytes / 8 + 1);
                json!({
                    "item_id": item_id,
                    "status": if index == 0 { "skipped" } else { "copying" },
                    "conflict": index < 3,
                    "source_path": format!("source/{sample_path}{index}.mkv"),
                    "target_path": format!("target/{sample_path}{index}.mkv"),
                    "bytes": 1024,
                    "files": files,
                })
            })
            .collect::<Vec<_>>();
        json!({
            "id": format!("migration-preview-{seq}"),
            "revision": format!("migration-revision-{seq}"),
            "source_library_id": "library-source",
            "source_root_binding": format!("v1:{}", "a".repeat(64)),
            "target_library_id": "library-target",
            "target_root_binding": format!("v1:{}", "b".repeat(64)),
            "scope": "library",
            "item_ids": item_ids,
            "conflict": "skip",
            "item_conflicts": item_conflicts,
            "include_incomplete_tasks": true,
            "set_target_default_on_success": false,
            "item_count": item_count,
            "file_count": item_count
                .saturating_mul(files_per_item)
                .saturating_add(2),
            "total_bytes": item_count.saturating_mul(1024).saturating_add(4096) as u64,
            "required_space_bytes": item_count
                .saturating_mul(1024)
                .saturating_add(4096) as u64,
            "available_space_bytes": 8 * 1024 * 1024 * 1024_u64,
            "conflict_count": item_count.min(3),
            "incomplete_task_count": 2,
            "staging_bytes": 4096,
            "warnings": ["first warning", "second warning"],
            "items": items,
        })
    }

    fn object_size(value: &Value) -> usize {
        serde_json::to_vec(value)
            .expect("JSON value should serialize")
            .len()
    }

    #[test]
    fn compacts_large_operation_receipts_within_budget_and_keeps_identity() {
        let records = (1..=100)
            .map(|seq| {
                record(
                    seq,
                    "file_confirm",
                    InboxStatus::Failed,
                    Some(json!({
                        "preview_id": format!("preview-{seq}"),
                        "revision": format!("revision-{seq}"),
                        "status": "partial",
                        "error": format!("result error {seq}"),
                        "payload": "x".repeat(8 * 1024),
                    })),
                    Some(format!("operation error {seq}")),
                )
            })
            .collect::<Vec<_>>();
        let refs = records.iter().collect::<Vec<_>>();
        let mut operations = operation_snapshots(&refs).expect("operations should be built");

        assert!(compact_operation_receipts(&mut operations));
        let encoded = serde_json::to_vec(&operations).expect("operations should serialize");
        assert!(encoded.len() <= MAX_COMPACT_OPERATION_ARRAY_BYTES);
        assert_eq!(operations.len(), 100);
        for (index, operation) in operations.iter().enumerate() {
            let expected_seq = (index + 1) as u64;
            assert_eq!(operation["seq"], expected_seq);
            assert_eq!(operation["status"], "failed");
            assert_eq!(
                operation["error"],
                format!("operation error {expected_seq}")
            );
            assert_eq!(
                operation["result"]["preview_id"],
                format!("preview-{expected_seq}")
            );
            assert_eq!(
                operation["result"]["revision"],
                format!("revision-{expected_seq}")
            );
            assert_eq!(operation["result"]["status"], "partial");
            assert_eq!(operation["details_incomplete"], true);
            assert_eq!(operation["result_truncated"], true);
            assert!(object_size(&operation["result"]) <= MAX_COMPACT_OPERATION_RESULT_BYTES);
        }
        assert!(!compact_operation_receipts(&mut operations));
    }

    #[test]
    fn keeps_latest_large_legacy_import_and_bounds_older_history() {
        let records = (1..=5)
            .map(|seq| {
                let kind = if seq == 5 {
                    "legacy_import"
                } else {
                    "file_confirm"
                };
                record(
                    seq,
                    kind,
                    InboxStatus::Completed,
                    Some(json!({
                        "library_id": "ml-test-root",
                        "root_binding": TEST_ROOT_BINDING,
                        "preview_id": format!("preview-{seq}"),
                        "revision": format!("revision-{seq}"),
                        "candidates": ["x".repeat(900 * 1024)],
                    })),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let refs = records.iter().collect::<Vec<_>>();
        let operations = operation_snapshots(&refs).expect("operations should be built");
        let encoded = serde_json::to_vec(&operations).expect("operations should serialize");

        assert!(encoded.len() <= MAX_OPERATION_SNAPSHOT_BYTES);
        assert_eq!(operations.len(), 5);
        assert_eq!(operations[4]["kind"], "legacy_import");
        assert_eq!(
            operations[4]["result"]["candidates"][0]
                .as_str()
                .unwrap()
                .len(),
            900 * 1024
        );
        assert_eq!(operations[4]["result_truncated"], Value::Null);
        assert_eq!(operations[4]["details_incomplete"], Value::Null);
        assert!(
            operations[0..3]
                .iter()
                .any(|operation| operation["result_truncated"] == true)
        );
    }

    #[test]
    fn compacted_legacy_import_keeps_its_original_library_root_binding() {
        let records = (1..=2)
            .map(|seq| {
                record(
                    seq,
                    "legacy_import",
                    InboxStatus::Completed,
                    Some(legacy_import_result(Some(900 * 1024), &[])),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let refs = records.iter().collect::<Vec<_>>();
        let mut operations = operation_snapshots(&refs).expect("operations should be built");

        assert!(compact_operation_receipts(&mut operations));

        assert_eq!(operations[0]["result"]["incomplete"], true);
        assert_eq!(operations[0]["result"]["library_id"], "ml-test-root");
        assert_eq!(operations[0]["result"]["root_binding"], TEST_ROOT_BINDING);
    }

    #[test]
    fn reserves_latest_complete_legacy_import_before_newer_large_confirm_receipts() {
        let mut records = vec![record(
            1,
            "legacy_import",
            InboxStatus::Completed,
            Some(legacy_import_result(Some(900 * 1024), &[])),
            None,
        )];
        records.extend((2..=5).map(|seq| {
            record(
                seq,
                "file_confirm",
                InboxStatus::Completed,
                Some(json!({"payload": "x".repeat(700 * 1024)})),
                None,
            )
        }));
        let refs = records.iter().collect::<Vec<_>>();

        let operations = operation_snapshots(&refs).expect("operations should be built");
        let encoded = serde_json::to_vec(&operations).expect("operations should serialize");

        assert!(encoded.len() <= MAX_OPERATION_SNAPSHOT_BYTES);
        assert_eq!(operations.len(), 5);
        assert_eq!(operations[0]["seq"], 1);
        assert_eq!(operations[0]["kind"], "legacy_import");
        assert_eq!(
            operations[0]["result"]["candidates"][0]["payload"]
                .as_str()
                .unwrap()
                .len(),
            900 * 1024
        );
        assert_ne!(operations[0]["details_incomplete"], true);
        assert_ne!(operations[0]["result_truncated"], true);
        assert!(
            operations[1..]
                .iter()
                .any(|operation| operation["details_incomplete"] == true)
        );
    }

    #[test]
    fn failed_and_explicitly_incomplete_imports_do_not_take_priority() {
        let records = [
            record(
                1,
                "legacy_import",
                InboxStatus::Completed,
                Some(legacy_import_result(Some(850 * 1024), &[])),
                None,
            ),
            record(
                2,
                "legacy_import",
                InboxStatus::Failed,
                Some(legacy_import_result(Some(800 * 1024), &[])),
                Some("import failed".to_string()),
            ),
            record(
                3,
                "legacy_import",
                InboxStatus::Completed,
                Some({
                    let mut result = legacy_import_result(Some(800 * 1024), &[]);
                    result["details_incomplete"] = Value::Bool(true);
                    result
                }),
                None,
            ),
            record(
                4,
                "file_confirm",
                InboxStatus::Completed,
                Some(json!({"payload": "x".repeat(700 * 1024)})),
                None,
            ),
            record(
                5,
                "file_confirm",
                InboxStatus::Completed,
                Some(json!({"payload": "x".repeat(700 * 1024)})),
                None,
            ),
        ];
        let refs = records.iter().collect::<Vec<_>>();

        let operations = operation_snapshots(&refs).expect("operations should be built");
        let encoded = serde_json::to_vec(&operations).expect("operations should serialize");

        assert!(encoded.len() <= MAX_OPERATION_SNAPSHOT_BYTES);
        assert_eq!(operations[0]["seq"], 1);
        assert_eq!(operations[0]["status"], "completed");
        assert_eq!(
            operations[0]["result"]["candidates"][0]["payload"]
                .as_str()
                .unwrap()
                .len(),
            850 * 1024
        );
        assert_eq!(operations[1]["status"], "failed");
        assert_eq!(operations[1]["details_incomplete"], true);
        assert_eq!(operations[2]["status"], "completed");
        assert_eq!(operations[2]["details_incomplete"], true);
        assert_eq!(operations[2]["result_truncated"], true);
    }

    #[test]
    fn prioritizes_title_only_legacy_import_with_no_candidates_at_the_budget_boundary() {
        let mut title_result = legacy_import_result(None, &[]);
        title_result["hints"] = Value::Array(
            (0..8)
                .map(|hint_index| {
                    let title = format!("Star Wars {hint_index} {}", "x".repeat(380));
                    assert!(title.chars().count() <= 500);
                    json!({
                        "hint_index": hint_index,
                        "title": title,
                        "source_ids": [],
                    })
                })
                .collect(),
        );
        let title_only = record(
            1,
            "legacy_import",
            InboxStatus::Completed,
            Some(title_result),
            None,
        );
        let compact_confirms = [2, 3].map(|seq| {
            record(
                seq,
                "file_confirm",
                InboxStatus::Completed,
                Some(json!({"payload": ""})),
                None,
            )
        });
        let base_records = [&title_only, &compact_confirms[0], &compact_confirms[1]];
        let base_snapshots = base_records
            .iter()
            .map(|record| operation_snapshot(record, false))
            .collect::<Vec<_>>();
        let initial_bytes = array_json_size(&base_snapshots, MAX_OPERATION_SNAPSHOT_BYTES)
            .expect("compact receipts should fit");
        let title_full_bytes = json_size_within(
            &operation_snapshot(&title_only, true),
            MAX_OPERATION_SNAPSHOT_BYTES,
        )
        .unwrap();
        let title_compact_bytes =
            json_size_within(&base_snapshots[0], MAX_OPERATION_SNAPSHOT_BYTES).unwrap();
        let confirm_full_base_bytes = json_size_within(
            &operation_snapshot(&compact_confirms[0], true),
            MAX_OPERATION_SNAPSHOT_BYTES,
        )
        .unwrap();
        let confirm_compact_bytes =
            json_size_within(&base_snapshots[1], MAX_OPERATION_SNAPSHOT_BYTES).unwrap();
        let two_confirm_payload_bytes = MAX_OPERATION_SNAPSHOT_BYTES
            .saturating_sub(1)
            .saturating_sub(initial_bytes)
            .saturating_add(confirm_compact_bytes.saturating_mul(2))
            .saturating_sub(confirm_full_base_bytes.saturating_mul(2));
        let payload_bytes = two_confirm_payload_bytes / 2;
        assert!(payload_bytes > 0);
        let confirm_records = [2, 3].map(|seq| {
            record(
                seq,
                "file_confirm",
                InboxStatus::Completed,
                Some(json!({"payload": "x".repeat(payload_bytes)})),
                None,
            )
        });
        let refs = [&title_only, &confirm_records[0], &confirm_records[1]];
        let mut confirms_full_without_import = vec![
            base_snapshots[0].clone(),
            operation_snapshot(&confirm_records[0], true),
            operation_snapshot(&confirm_records[1], true),
        ];
        assert!(
            array_json_size(&confirms_full_without_import, MAX_OPERATION_SNAPSHOT_BYTES).is_some()
        );
        confirms_full_without_import[0] = operation_snapshot(&title_only, true);
        assert!(
            array_json_size(&confirms_full_without_import, MAX_OPERATION_SNAPSHOT_BYTES).is_none()
        );

        let operations = operation_snapshots(&refs).expect("operations should be built");
        let encoded = serde_json::to_vec(&operations).expect("operations should serialize");

        assert!(encoded.len() <= MAX_OPERATION_SNAPSHOT_BYTES);
        assert_eq!(
            operations[0]["result"]["hints"].as_array().unwrap().len(),
            8
        );
        assert!(
            operations[0]["result"]["hints"][0]["title"]
                .as_str()
                .unwrap()
                .starts_with("Star Wars 0 ")
        );
        assert_eq!(operations[0]["result"]["candidates"], json!([]));
        assert_ne!(operations[0]["details_incomplete"], true);
        assert_eq!(
            operations[1..]
                .iter()
                .filter(|operation| operation["details_incomplete"] != true)
                .count(),
            1
        );
        assert!(title_full_bytes > title_compact_bytes);
    }

    #[test]
    fn compact_receipts_keep_the_latest_usable_import_over_failed_and_incomplete_imports() {
        let records = [
            record(
                1,
                "legacy_import",
                InboxStatus::Completed,
                Some(legacy_import_result(Some(300 * 1024), &[])),
                None,
            ),
            record(
                2,
                "legacy_import",
                InboxStatus::Failed,
                Some(legacy_import_result(Some(200 * 1024), &[])),
                Some("import failed".to_string()),
            ),
            record(
                3,
                "legacy_import",
                InboxStatus::Completed,
                Some({
                    let mut result = legacy_import_result(Some(200 * 1024), &[]);
                    result["result_truncated"] = Value::Bool(true);
                    result
                }),
                None,
            ),
        ];
        let refs = records.iter().collect::<Vec<_>>();
        let mut operations = operation_snapshots(&refs).expect("operations should be built");

        assert!(compact_operation_receipts(&mut operations));

        let encoded = serde_json::to_vec(&operations).expect("operations should serialize");
        assert!(encoded.len() <= MAX_COMPACT_OPERATION_ARRAY_BYTES);
        assert_eq!(operations[0]["status"], "completed");
        assert_eq!(
            operations[0]["result"]["candidates"][0]["payload"]
                .as_str()
                .unwrap()
                .len(),
            300 * 1024
        );
        assert_ne!(operations[0]["details_incomplete"], true);
        assert_eq!(operations[1]["status"], "failed");
        assert_eq!(operations[1]["details_incomplete"], true);
        assert_eq!(operations[2]["details_incomplete"], true);
        assert_eq!(operations[2]["result_truncated"], true);
    }

    #[test]
    fn keeps_a_usable_truncated_hint_prefix_with_its_warning() {
        let warning = "Legacy import output was truncated to fit the local receipt size limit. The first 1 of 2 hints and their candidates remain available; re-import a smaller selection to access omitted hints.";
        let import = record(
            1,
            "legacy_import",
            InboxStatus::Completed,
            Some(legacy_import_result(Some(900 * 1024), &[warning])),
            None,
        );
        let newer_confirm = record(
            2,
            "file_confirm",
            InboxStatus::Completed,
            Some(json!({"payload": "x".repeat(1300 * 1024)})),
            None,
        );

        let operations =
            operation_snapshots(&[&import, &newer_confirm]).expect("operations should be built");
        let encoded = serde_json::to_vec(&operations).expect("operations should serialize");

        assert!(encoded.len() <= MAX_OPERATION_SNAPSHOT_BYTES);
        assert_eq!(operations[0]["status"], "completed");
        assert_eq!(
            operations[0]["result"]["hints"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            operations[0]["result"]["candidates"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(operations[0]["result"]["warnings"][0], warning);
        assert_ne!(operations[0]["details_incomplete"], true);
        assert_ne!(operations[0]["result_truncated"], true);
        assert_eq!(operations[1]["details_incomplete"], true);
    }

    #[test]
    fn preserves_small_full_results_in_recent_operations() {
        let records = (1..=3)
            .map(|seq| {
                record(
                    seq,
                    "legacy_import",
                    InboxStatus::Completed,
                    Some(json!({
                        "hints": {"title": "title"},
                        "candidates": [{"title": "mapped", "source_ids": ["av123456789"]}],
                    })),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let refs = records.iter().collect::<Vec<_>>();
        let operations = operation_snapshots(&refs).expect("operations should be built");

        assert_eq!(operations[2]["result"]["hints"]["title"], "title");
        assert_eq!(
            operations[2]["result"]["candidates"][0]["source_ids"][0],
            "av123456789"
        );
        assert_eq!(operations[2]["result_truncated"], Value::Null);
    }

    #[test]
    fn keeps_small_active_previews_within_budget() {
        let records = (1..=3)
            .map(|seq| {
                record(
                    seq,
                    "file_preview",
                    InboxStatus::AwaitingConfirmation,
                    Some(json!({
                        "preview_id": format!("preview-{seq}"),
                        "revision": format!("revision-{seq}"),
                        "library_id": format!("library-{seq}"),
                        "preview": {"id": format!("preview-{seq}"), "items": [{"path": format!("clip-{seq}.mp4")}]},
                    })),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let refs = records.iter().collect::<Vec<_>>();
        let previews = preview_snapshots(&refs, 0);

        assert_eq!(previews.len(), 3);
        assert_eq!(previews[0]["seq"], 1);
        assert_eq!(previews[0]["library_id"], "library-1");
        assert_eq!(previews[0]["root_binding"], TEST_ROOT_BINDING);
        assert_eq!(previews[2]["preview"]["items"][0]["path"], "clip-3.mp4");
        assert_eq!(previews[2]["library_id"], "library-3");
        assert_eq!(previews[2]["root_binding"], TEST_ROOT_BINDING);
        assert!(
            previews
                .iter()
                .all(|preview| preview["preview_incomplete"] != true)
        );
        assert!(previews.iter().map(object_size).sum::<usize>() <= MAX_PREVIEW_SNAPSHOT_BYTES);
        assert!(serde_json::to_vec(&previews).unwrap().len() <= MAX_PREVIEW_SNAPSHOT_BYTES);
    }

    #[test]
    fn cancelled_file_previews_remain_in_operation_history_not_active_previews() {
        let cancelled = record(
            46,
            "file_preview",
            InboxStatus::Cancelled,
            Some(json!({
                "preview_id": "preview-cancelled-history",
                "revision": "revision-cancelled-history",
                "library_id": "ml-test-root",
                "preview": {"items": [{"path": "clip.mp4"}]},
            })),
            Some("This preview was cancelled before confirmation.".to_string()),
        );

        let operations = operation_snapshots(&[&cancelled]).expect("history should fit");
        let previews = preview_snapshots(&[&cancelled], 0);

        assert_eq!(operations[0]["status"], "cancelled");
        assert_eq!(
            operations[0]["result"]["preview_id"],
            "preview-cancelled-history"
        );
        assert!(previews.is_empty());
    }

    #[test]
    fn staging_cleanup_pending_identity_survives_replay_without_publishing_payload() {
        for operation_kind in ["confirm", "cancel"] {
            for status in [
                InboxStatus::Pending,
                InboxStatus::Processing,
                InboxStatus::Retryable,
            ] {
                let mut receipt = record(53, "staging_cleanup", status, None, None);
                receipt.request.payload = json!({
                    "operation": operation_kind,
                    "library_id": "ml-test-root",
                    "root_binding": TEST_ROOT_BINDING,
                    "preview_seq": 52,
                    "preview_id": "staging-preview-52",
                    "revision": "staging-revision-52",
                });
                let replayed: InboxRecord = serde_json::from_slice(
                    &serde_json::to_vec(&receipt).expect("pending receipt should persist"),
                )
                .expect("pending receipt should replay");
                for full in [false, true] {
                    let published = operation_snapshot(&replayed, full);
                    for field in [
                        "operation",
                        "library_id",
                        "root_binding",
                        "preview_seq",
                        "preview_id",
                        "revision",
                    ] {
                        assert_eq!(published[field], receipt.request.payload[field]);
                    }
                    assert_eq!(published["status"], status_name(status));
                    assert!(published.get("payload").is_none());
                    assert!(serde_json::to_vec(&published).unwrap().len() < 1024);
                }
                receipt.request.payload["preview_id"] = json!("x".repeat(129));
                let malformed = operation_snapshot(&receipt, false);
                assert!(malformed.get("preview_id").is_none());
                assert!(malformed.get("operation").is_none());
            }
        }
    }

    #[test]
    fn staging_cleanup_snapshots_never_publish_local_identity_proofs() {
        let mut receipt = record(
            52,
            "staging_cleanup",
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "operation": "preview",
                "preview_id": "staging-preview-52",
                "revision": "staging-revision-52",
                "library_id": "ml-test-root",
                "root_binding": TEST_ROOT_BINDING,
                "preview": {
                    "id": "staging-preview-52",
                    "revision": "staging-revision-52",
                    "library_id": "ml-test-root",
                    "root_binding": TEST_ROOT_BINDING,
                    "created_at": 520,
                    "item_count": 1,
                    "omitted_item_count": 0,
                    "total_bytes": 2048,
                    "sample_items": [{"id": "stage-1", "relative_path": "video-1.mp4"}],
                },
                "cleanup_preview": {
                    "proofs": [{
                        "owner_identity": "private-owner-identity",
                        "control_identity": "private-control-identity",
                        "inventory_sha256": "private-inventory-digest",
                    }],
                },
            })),
            None,
        );
        receipt.request.payload = json!({
            "operation": "preview",
            "library_id": "ml-test-root",
            "root_binding": TEST_ROOT_BINDING,
        });

        let operations = operation_snapshots(&[&receipt]).expect("operation should fit");
        let previews = preview_snapshots(&[&receipt], 0);
        assert_eq!(operations[0]["operation"], "preview");
        assert_eq!(operations[0]["result"]["preview"]["item_count"], 1);
        assert!(previews[0]["preview"]["sample_items"].is_array());

        for published in [&operations[0], &previews[0]] {
            let serialized = serde_json::to_string(published).expect("snapshot should serialize");
            for private_field in [
                "proofs",
                "cleanup_preview",
                "owner_identity",
                "control_identity",
                "inventory_sha256",
                "private-owner-identity",
                "private-control-identity",
                "private-inventory-digest",
            ] {
                assert!(
                    !serialized.contains(private_field),
                    "published staging snapshot included {private_field}"
                );
            }
        }
    }

    #[test]
    fn file_preview_snapshot_keeps_source_library_binding_after_operation_compaction_and_replay() {
        let record = record(
            31,
            "file_preview",
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "preview_id": "preview-source-bound",
                "revision": "revision-source-bound",
                "library_id": "ml-source-root",
                "preview": {"items": "x".repeat(2 * 1024 * 1024)},
            })),
            None,
        );
        let persisted = serde_json::to_vec(&record).expect("inbox record should serialize");
        let reopened: InboxRecord =
            serde_json::from_slice(&persisted).expect("inbox record should replay");

        let operations = operation_snapshots(&[&reopened]).expect("operations should fit");
        assert_eq!(operations[0]["details_incomplete"], true);
        assert_eq!(operations[0]["result"]["incomplete"], true);
        assert_eq!(operations[0]["result"]["library_id"], "ml-source-root");
        assert_eq!(operations[0]["result"]["root_binding"], TEST_ROOT_BINDING);

        let previews = preview_snapshots(&[&reopened], 0);
        assert_eq!(previews.len(), 1);
        assert_eq!(previews[0]["library_id"], "ml-source-root");
        assert_eq!(
            previews[0]["preview"]["items"].as_str().unwrap().len(),
            2 * 1024 * 1024
        );
        assert!(serde_json::to_vec(&previews).unwrap().len() <= MAX_PREVIEW_SNAPSHOT_BYTES);
    }

    #[test]
    fn file_preview_request_identity_survives_pending_failure_compaction_and_replay() {
        let request_id = "action:00000000-0000-4000-8000-000000000001";
        for (seq, status) in [(41, InboxStatus::Pending), (42, InboxStatus::Failed)] {
            let receipt = file_preview_with_request(seq, status, None, json!(request_id));
            let operation = operation_snapshot(&receipt, false);
            assert_eq!(operation["request_id"], request_id);
            assert_eq!(operation["library_id"], "ml-source-root");
            assert_eq!(operation["root_binding"], TEST_ROOT_BINDING);
            assert_eq!(operation["status"], status_name(status));
        }

        let receipt = file_preview_with_request(
            43,
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "preview_id": "preview-source-bound",
                "revision": "revision-source-bound",
                "library_id": "ml-result-root",
                "root_binding": "v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "preview": {"items": "x".repeat(2 * 1024 * 1024)},
            })),
            json!(request_id),
        );
        let persisted = serde_json::to_vec(&receipt).expect("inbox record should serialize");
        let reopened: InboxRecord =
            serde_json::from_slice(&persisted).expect("inbox record should replay");

        let operations = operation_snapshots(&[&reopened]).expect("operations should fit");
        assert_eq!(operations[0]["request_id"], request_id);
        assert_eq!(operations[0]["library_id"], "ml-source-root");
        assert_eq!(operations[0]["root_binding"], TEST_ROOT_BINDING);
        assert_eq!(operations[0]["details_incomplete"], true);
        assert_eq!(operations[0]["result_truncated"], true);
        assert!(serde_json::to_vec(&operations).unwrap().len() <= MAX_OPERATION_SNAPSHOT_BYTES);
    }

    #[test]
    fn file_preview_request_identity_is_published_for_full_and_placeholder_previews() {
        let request_id = "action:00000000-0000-4000-8000-000000000002";
        let full = file_preview_with_request(
            44,
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "preview_id": "preview-full",
                "revision": "revision-full",
                "library_id": "ml-source-root",
                "preview": {"items": [{"path": "clip.mp4"}]},
            })),
            json!(request_id),
        );
        let full_preview = preview_snapshots(&[&full], 0);
        assert_eq!(full_preview[0]["request_id"], request_id);
        assert_eq!(full_preview[0]["library_id"], "ml-source-root");
        assert_eq!(full_preview[0]["root_binding"], TEST_ROOT_BINDING);
        assert!(full_preview[0].get("preview").is_some());

        let placeholder = file_preview_with_request(
            45,
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "preview_id": "preview-placeholder",
                "revision": "revision-placeholder",
                "library_id": "ml-source-root",
                "preview": {"items": "x".repeat(MAX_PREVIEW_SNAPSHOT_BYTES + 1)},
            })),
            json!(request_id),
        );
        let placeholder_preview = preview_snapshots(&[&placeholder], 0);
        assert_eq!(placeholder_preview[0]["request_id"], request_id);
        assert_eq!(placeholder_preview[0]["library_id"], "ml-source-root");
        assert_eq!(placeholder_preview[0]["root_binding"], TEST_ROOT_BINDING);
        assert_eq!(placeholder_preview[0]["preview_incomplete"], true);
        assert!(placeholder_preview[0].get("preview").is_none());
    }

    #[test]
    fn file_preview_request_id_is_omitted_when_invalid_and_respects_snapshot_budgets() {
        let invalid_request_ids = [
            json!(""),
            json!("   "),
            json!("x".repeat(MAX_CLOUD_REQUEST_ID_BYTES + 1)),
            json!("bad\nrequest"),
            json!(17),
        ];
        for (index, request_id) in invalid_request_ids.into_iter().enumerate() {
            let receipt = file_preview_with_request(
                50 + index as u64,
                InboxStatus::AwaitingConfirmation,
                Some(json!({
                    "preview_id": format!("preview-invalid-{index}"),
                    "revision": "revision-invalid",
                    "library_id": "ml-source-root",
                    "preview": {"items": []},
                })),
                request_id,
            );
            let operations = operation_snapshots(&[&receipt]).expect("operations should fit");
            let previews = preview_snapshots(&[&receipt], 0);
            assert!(operations[0].get("request_id").is_none());
            assert!(previews[0].get("request_id").is_none());
        }

        let long_valid_id = "\"".repeat(MAX_CLOUD_REQUEST_ID_BYTES);
        let receipts = (1..=MAX_OPERATION_SNAPSHOT_RECORDS)
            .map(|seq| {
                file_preview_with_request(
                    seq as u64,
                    InboxStatus::AwaitingConfirmation,
                    Some(json!({
                        "preview_id": format!("preview-budget-{seq}"),
                        "revision": "revision-budget",
                        "library_id": "ml-source-root",
                        "preview": {"items": []},
                    })),
                    json!(long_valid_id),
                )
            })
            .collect::<Vec<_>>();
        let refs = receipts.iter().collect::<Vec<_>>();
        let operations = operation_snapshots(&refs).expect("operations should fit");
        let previews = preview_snapshots(&refs, 0);
        assert_eq!(operations.len(), MAX_OPERATION_SNAPSHOT_RECORDS);
        assert_eq!(previews.len(), MAX_PREVIEW_SNAPSHOT_RECORDS);
        assert!(serde_json::to_vec(&operations).unwrap().len() <= MAX_OPERATION_SNAPSHOT_BYTES);
        assert!(serde_json::to_vec(&previews).unwrap().len() <= MAX_PREVIEW_SNAPSHOT_BYTES);
    }

    #[test]
    fn file_preview_can_remain_in_snapshot_after_its_operation_is_compacted_away() {
        let preview = file_preview_with_request(
            1,
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "preview_id": "preview-independent",
                "revision": "revision-independent",
                "library_id": "ml-source-root",
                "preview": {"items": []},
            })),
            json!("action:00000000-0000-4000-8000-000000000003"),
        );
        let newer = (2..=101)
            .map(|seq| record(seq, "library_scan", InboxStatus::Completed, None, None))
            .collect::<Vec<_>>();
        let mut records = vec![preview];
        records.extend(newer);
        let refs = records.iter().collect::<Vec<_>>();

        let operations = operation_snapshots(&refs).expect("operations should fit");
        let previews = preview_snapshots(&refs, 0);
        assert_eq!(operations.len(), MAX_OPERATION_SNAPSHOT_RECORDS);
        assert!(operations.iter().all(|operation| operation["seq"] != 1));
        assert_eq!(previews.len(), 1);
        assert_eq!(previews[0]["seq"], 1);
        assert_eq!(
            previews[0]["request_id"],
            "action:00000000-0000-4000-8000-000000000003"
        );
    }

    #[test]
    fn file_preview_placeholder_keeps_source_library_binding_when_details_do_not_fit() {
        let record = record(
            32,
            "file_preview",
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "preview_id": "preview-placeholder-bound",
                "revision": "revision-placeholder-bound",
                "library_id": "ml-source-root",
                "preview": {"items": "x".repeat(MAX_PREVIEW_SNAPSHOT_BYTES + 1)},
            })),
            None,
        );

        let previews = preview_snapshots(&[&record], 0);

        assert_eq!(previews.len(), 1);
        assert_eq!(previews[0]["kind"], "file_preview");
        assert_eq!(previews[0]["library_id"], "ml-source-root");
        assert_eq!(previews[0]["root_binding"], TEST_ROOT_BINDING);
        assert_eq!(previews[0]["preview_id"], "preview-placeholder-bound");
        assert_eq!(previews[0]["preview_incomplete"], true);
        assert_eq!(previews[0]["reload_required"], true);
        assert!(previews[0].get("preview").is_none());
    }

    #[test]
    fn file_preview_keeps_absent_legacy_binding_distinct_from_malformed_bindings() {
        let mut legacy = record(
            40,
            "file_preview",
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "preview_id": "legacy-preview",
                "revision": "legacy-revision",
                "preview": {"items": []},
            })),
            None,
        );
        let legacy_result = legacy.result.as_mut().unwrap().as_object_mut().unwrap();
        legacy_result.remove("library_id");
        legacy_result.remove("root_binding");
        let malformed_results = [
            Value::Null,
            json!(42),
            json!(""),
            json!(" "),
            json!("x".repeat(MAX_LIBRARY_ID_BYTES + 1)),
            json!("bad\nroot"),
        ];
        let malformed_count = malformed_results.len();
        let malformed = malformed_results
            .into_iter()
            .enumerate()
            .map(|(index, library_id)| {
                record(
                    41 + index as u64,
                    "file_preview",
                    InboxStatus::AwaitingConfirmation,
                    Some(json!({
                        "preview_id": format!("malformed-preview-{index}"),
                        "revision": format!("malformed-revision-{index}"),
                        "library_id": library_id,
                        "preview": {"items": []},
                    })),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let malformed_tokens = [Value::Null, json!("v1:short"), json!("v1:")]
            .into_iter()
            .enumerate()
            .map(|(index, root_binding)| {
                record(
                    50 + index as u64,
                    "file_preview",
                    InboxStatus::AwaitingConfirmation,
                    Some(json!({
                        "preview_id": format!("malformed-token-preview-{index}"),
                        "revision": format!("malformed-token-revision-{index}"),
                        "library_id": "ml-test-root",
                        "root_binding": root_binding,
                        "preview": {"items": []},
                    })),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let malformed_token_count = malformed_tokens.len();
        let records = std::iter::once(&legacy)
            .chain(malformed.iter())
            .chain(malformed_tokens.iter())
            .collect::<Vec<_>>();

        let previews = preview_snapshots(&records, 0);

        assert_eq!(previews.len(), 1);
        assert_eq!(previews[0]["preview_incomplete"], true);
        assert_eq!(
            previews[0]["omitted_count"],
            malformed_count + malformed_token_count + 1
        );
        for index in 0..malformed_count {
            assert!(
                previews
                    .iter()
                    .all(|preview| preview["preview_id"] != format!("malformed-preview-{index}"))
            );
        }
        for index in 0..malformed_token_count {
            assert!(
                previews
                    .iter()
                    .all(|preview| preview["preview_id"]
                        != format!("malformed-token-preview-{index}"))
            );
        }
    }

    #[test]
    fn keeps_actionable_migration_preview_result_in_kind_tagged_envelope() {
        let result = json!({
            "id": "migration-preview-1",
            "revision": "migration-revision-1",
            "source_library_id": "library-main",
            "source_root_binding": format!("v1:{}", "a".repeat(64)),
            "target_library_id": "library-archive",
            "target_root_binding": format!("v1:{}", "b".repeat(64)),
            "scope": "selected",
            "item_ids": ["video-1"],
            "conflict": "skip",
            "include_incomplete_tasks": false,
            "set_target_default_on_success": false,
            "item_count": 1,
            "file_count": 2,
            "total_bytes": 2048,
            "required_space_bytes": 4096,
            "available_space_bytes": 8192,
            "conflict_count": 0,
            "incomplete_task_count": 3,
            "staging_bytes": 1024,
            "items": [{
                "item_id": "video-1",
                "source_path": "Collections/Series/video.mp4",
                "target_path": "Collections/Series/video.mp4",
                "bytes": 2048,
                "outcome": "move"
            }]
        });
        let record = record(
            24,
            "library_migration_preview",
            InboxStatus::AwaitingConfirmation,
            Some(result.clone()),
            None,
        );

        let previews = preview_snapshots(&[&record], 0);

        assert_eq!(previews.len(), 1);
        assert_eq!(previews[0]["kind"], "library_migration_preview");
        assert_eq!(previews[0]["preview_id"], "migration-preview-1");
        assert_eq!(previews[0]["revision"], "migration-revision-1");
        assert_eq!(previews[0]["status"], "awaiting_confirmation");
        assert_eq!(previews[0]["result"], result);
        assert!(previews[0].get("preview").is_none());
        assert!(serde_json::to_vec(&previews).unwrap().len() <= MAX_PREVIEW_SNAPSHOT_BYTES);
    }

    #[test]
    fn oversized_whole_library_migration_keeps_a_bounded_actionable_summary() {
        let result = migration_preview_result(42, 120, 8, 2200, 700);
        assert!(object_size(&result) > MAX_PREVIEW_SNAPSHOT_BYTES);
        let record = record(
            42,
            "library_migration_preview",
            InboxStatus::AwaitingConfirmation,
            Some(result),
            None,
        );

        let previews = preview_snapshots(&[&record], 0);
        let snapshot = &previews[0];
        let summary = &snapshot["result"];
        let samples = summary["sample_items"]
            .as_array()
            .expect("summary samples should be present");

        assert_eq!(snapshot["kind"], "library_migration_preview");
        assert_eq!(snapshot["migration_summary"], true);
        assert_eq!(snapshot["preview_id"], "migration-preview-42");
        assert_eq!(snapshot["revision"], "migration-revision-42");
        assert_eq!(snapshot["status"], "awaiting_confirmation");
        assert_eq!(summary["id"], "migration-preview-42");
        assert_eq!(summary["revision"], "migration-revision-42");
        assert_eq!(summary["source_library_id"], "library-source");
        assert_eq!(
            summary["source_root_binding"],
            format!("v1:{}", "a".repeat(64))
        );
        assert_eq!(summary["target_library_id"], "library-target");
        assert_eq!(
            summary["target_root_binding"],
            format!("v1:{}", "b".repeat(64))
        );
        assert_eq!(summary["scope"], "library");
        assert_eq!(summary["conflict"], "skip");
        assert_eq!(summary["include_incomplete_tasks"], true);
        assert_eq!(summary["set_target_default_on_success"], false);
        assert_eq!(summary["item_count"], 120);
        assert_eq!(summary["file_count"], 962);
        assert_eq!(summary["total_bytes"], 124 * 1024);
        assert_eq!(summary["required_space_bytes"], 124 * 1024);
        assert_eq!(summary["available_space_bytes"], 8 * 1024 * 1024 * 1024_u64);
        assert_eq!(summary["conflict_count"], 3);
        assert_eq!(summary["incomplete_task_count"], 2);
        assert_eq!(summary["staging_bytes"], 4096);
        assert_eq!(summary["omitted_item_count"], 116);
        assert_eq!(summary["omitted_file_count"], 962);
        assert_eq!(summary["omitted_item_id_count"], 116);
        assert_eq!(summary["omitted_conflict_choice_count"], 3);
        assert_eq!(summary["omitted_warning_count"], 2);
        assert_eq!(samples.len(), MAX_MIGRATION_PREVIEW_SAMPLE_ITEMS);
        assert_eq!(samples[0]["outcome"], "skip");
        assert_eq!(samples[0]["status"], "skipped");
        assert_eq!(samples[1]["outcome"], "move");
        assert_eq!(samples[1]["status"], "copying");
        for sample in samples {
            assert!(sample.get("files").is_none());
            for field in ["source_path", "target_path"] {
                let path = sample[field].as_str().unwrap();
                assert!(path.ends_with('…'));
                assert!(
                    json_size_within(
                        &Value::String(path.to_string()),
                        MAX_MIGRATION_PREVIEW_SAMPLE_PATH_JSON_BYTES
                    )
                    .is_some()
                );
            }
        }
        for omitted_detail in ["items", "item_ids", "item_conflicts", "warnings"] {
            assert!(summary.get(omitted_detail).is_none());
        }
        assert!(snapshot.get("reload_required").is_none());
        assert!(snapshot.get("preview_incomplete").is_none());
        assert!(object_size(snapshot) <= MAX_MIGRATION_PREVIEW_SUMMARY_BYTES);
        assert!(serde_json::to_vec(&previews).unwrap().len() <= MAX_PREVIEW_SNAPSHOT_BYTES);
    }

    #[test]
    fn reserves_actionable_summaries_for_migration_previews_under_shared_budget() {
        let records = (1..=MAX_PREVIEW_SNAPSHOT_RECORDS as u64)
            .map(|seq| {
                record(
                    seq,
                    "library_migration_preview",
                    InboxStatus::AwaitingConfirmation,
                    Some(migration_preview_result(seq, 3, 3, 2048, 32)),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let refs = records.iter().collect::<Vec<_>>();
        let previews = preview_snapshots(&refs, 0);
        let encoded = serde_json::to_vec(&previews).expect("previews should serialize");
        let mut full_count = 0usize;
        let mut summary_count = 0usize;

        assert_eq!(previews.len(), MAX_PREVIEW_SNAPSHOT_RECORDS);
        assert!(encoded.len() <= MAX_PREVIEW_SNAPSHOT_BYTES);
        for (index, preview) in previews.iter().enumerate() {
            let expected_id = format!("migration-preview-{}", index + 1);
            let expected_revision = format!("migration-revision-{}", index + 1);
            assert_eq!(preview["preview_id"], expected_id);
            assert_eq!(preview["revision"], expected_revision);
            assert_eq!(preview["status"], "awaiting_confirmation");
            assert!(preview.get("reload_required").is_none());
            if preview["migration_summary"] == true {
                summary_count += 1;
                assert_eq!(preview["result"]["id"], expected_id);
                assert_eq!(preview["result"]["revision"], expected_revision);
                assert!(preview["result"].get("items").is_none());
                assert!(preview["result"].get("item_ids").is_none());
            } else {
                full_count += 1;
                assert!(preview["result"]["items"].is_array());
            }
        }
        assert!(
            summary_count > 0,
            "some previews should use the compact form"
        );
        assert!(
            full_count > 0,
            "some previews should retain their full details"
        );
    }

    #[test]
    fn full_preview_array_budget_includes_placeholders_and_keeps_newest_details() {
        let records = (1..=2)
            .map(|seq| {
                record(
                    seq,
                    "file_preview",
                    InboxStatus::AwaitingConfirmation,
                    Some(json!({
                        "preview_id": format!("preview-{seq}"),
                        "revision": format!("revision-{seq}"),
                        "preview": {"items": "x".repeat(2 * 1024 * 1024)},
                    })),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let refs = records.iter().collect::<Vec<_>>();
        let previews = preview_snapshots(&refs, 0);
        let encoded = serde_json::to_vec(&previews).expect("previews should serialize");

        assert!(encoded.len() <= MAX_PREVIEW_SNAPSHOT_BYTES);
        assert_eq!(previews.len(), 2);
        assert_eq!(previews[0]["preview_incomplete"], true);
        assert!(previews[0].get("preview").is_none());
        assert_eq!(previews[1]["preview_id"], "preview-2");
        assert!(previews[1].get("preview").is_some());
    }

    #[test]
    fn oversized_active_preview_becomes_identity_preserving_placeholder() {
        let record = record(
            7,
            "file_preview",
            InboxStatus::AwaitingConfirmation,
            Some(json!({
                "preview_id": "preview-7",
                "revision": "revision-7",
                "preview": {"steps": "x".repeat(MAX_PREVIEW_SNAPSHOT_BYTES + 1)},
            })),
            None,
        );
        let previews = preview_snapshots(&[&record], 0);

        assert_eq!(previews.len(), 1);
        let placeholder = &previews[0];
        assert_eq!(placeholder["seq"], 7);
        assert_eq!(placeholder["preview_id"], "preview-7");
        assert_eq!(placeholder["revision"], "revision-7");
        assert_eq!(placeholder["status"], "awaiting_confirmation");
        assert_eq!(placeholder["preview_incomplete"], true);
        assert_eq!(placeholder["incomplete"], true);
        assert_eq!(placeholder["reload_required"], true);
        assert_eq!(placeholder["reason"], PREVIEW_DETAILS_OMITTED_REASON);
        assert!(placeholder.get("preview").is_none());
        assert!(placeholder.get("items").is_none());
        assert!(placeholder.get("steps").is_none());
    }

    #[test]
    fn aggregates_omitted_active_previews_without_dropping_the_count() {
        let records = (1..=105)
            .map(|seq| {
                record(
                    seq,
                    "file_preview",
                    InboxStatus::AwaitingConfirmation,
                    Some(json!({
                        "preview_id": format!("preview-{seq}"),
                        "revision": format!("revision-{seq}"),
                        "preview": {"id": format!("preview-{seq}"), "items": []},
                    })),
                    None,
                )
            })
            .collect::<Vec<_>>();
        let refs = records.iter().collect::<Vec<_>>();
        let previews = preview_snapshots(&refs, 2);

        assert_eq!(previews.len(), 101);
        assert_eq!(previews[0]["seq"], 6);
        assert_eq!(previews[99]["seq"], 105);
        assert_eq!(previews[100]["preview_incomplete"], true);
        assert_eq!(previews[100]["incomplete"], true);
        assert_eq!(previews[100]["reload_required"], true);
        assert_eq!(previews[100]["omitted_count"], 7);
        assert_eq!(previews[100]["reason"], PREVIEWS_OMITTED_REASON);
        assert!(previews[100].get("seq").is_none());
        assert!(previews[100].get("preview").is_none());
    }

    #[test]
    fn five_thousand_file_preview_receipt_and_snapshots_keep_an_actionable_summary() {
        let mut receipt = generated_file_preview_record(500, MAX_FILE_PREVIEW_SELECTION_ITEMS);
        let full_bytes = serde_json::to_vec(&receipt).expect("full receipt should serialize");
        assert!(full_bytes.len() > 1024 * 1024);

        assert!(
            receipt
                .compact_oversized_result()
                .expect("oversized preview should compact")
        );
        assert_eq!(receipt.status, InboxStatus::AwaitingConfirmation);
        assert!(is_valid_compact_file_preview_result(&receipt));
        let result = receipt
            .result
            .as_ref()
            .expect("compacted result should remain");
        let summary = &result["preview"];
        assert_eq!(result["details_incomplete"], true);
        assert_eq!(result["result_truncated"], true);
        assert_eq!(summary["file_preview_summary"], true);
        assert_eq!(summary["summary_version"], 1);
        assert_eq!(summary["selected_count"], MAX_FILE_PREVIEW_SELECTION_ITEMS);
        assert_eq!(summary["move_item_count"], MAX_FILE_PREVIEW_SELECTION_ITEMS);
        assert_eq!(summary["file_count"], MAX_FILE_PREVIEW_SELECTION_ITEMS * 4);
        assert_eq!(
            summary["outcome_counts"]["move"],
            MAX_FILE_PREVIEW_SELECTION_ITEMS
        );
        assert_eq!(
            summary["omitted_item_count"],
            MAX_FILE_PREVIEW_SELECTION_ITEMS - 4
        );
        assert_eq!(summary["sample_items"].as_array().unwrap().len(), 4);
        assert!(summary.get("items").is_none());
        assert!(json_size_within(result, MAX_FILE_PREVIEW_SUMMARY_BYTES).is_some());

        let operations = operation_snapshots(&[&receipt]).expect("operation should fit");
        assert_eq!(operations[0]["details_incomplete"], true);
        assert_eq!(operations[0]["result_truncated"], true);
        assert_eq!(
            operations[0]["result"]["preview"]["file_preview_summary"],
            true
        );
        let previews = preview_snapshots(&[&receipt], 0);
        assert_eq!(previews.len(), 1);
        assert_eq!(previews[0]["incomplete"], true);
        assert_eq!(previews[0]["preview_incomplete"], true);
        assert_eq!(previews[0]["reload_required"], true);
        assert_eq!(previews[0]["details_incomplete"], true);
        assert_eq!(previews[0]["result_truncated"], true);
        assert_eq!(previews[0]["preview"]["file_preview_summary"], true);
        assert!(previews[0]["preview"].get("items").is_none());
        assert!(serde_json::to_vec(&previews).unwrap().len() <= MAX_PREVIEW_SNAPSHOT_BYTES);
    }

    #[test]
    fn small_file_preview_remains_a_full_legacy_receipt() {
        let mut receipt = generated_file_preview_record(501, 1);
        assert!(serde_json::to_vec(&receipt).unwrap().len() < 1024 * 1024);
        assert!(
            !receipt
                .compact_oversized_result()
                .expect("small preview should remain unchanged")
        );
        assert!(receipt.result.as_ref().unwrap()["preview"]["items"].is_array());
        assert!(receipt.result.as_ref().unwrap()["details_incomplete"].is_null());
    }

    #[test]
    fn metadata_only_summary_reports_full_selection_and_actual_patch_changes() {
        let mut receipt = generated_metadata_only_preview_record(505, 5_000);
        let no_op_summary = compact_file_preview_receipt_result(&receipt)
            .expect("metadata-only no-op preview should summarize");
        assert!(is_valid_compact_file_preview_result(&InboxRecord {
            result: Some(no_op_summary.clone()),
            ..receipt.clone()
        }));
        assert_eq!(no_op_summary["preview"]["selected_count"], 5_000);
        assert_eq!(no_op_summary["preview"]["metadata_only"], true);
        assert_eq!(no_op_summary["preview"]["move_item_count"], 0);
        assert_eq!(no_op_summary["preview"]["metadata_patch_count"], 5_000);
        assert_eq!(no_op_summary["preview"]["changed_metadata_patch_count"], 0);
        assert_eq!(
            no_op_summary["preview"]["omitted_metadata_patch_count"],
            4_996
        );

        receipt.result.as_mut().unwrap()["preview"]["metadata_patches"][0]["changed"] = json!(true);
        let changed_summary = compact_file_preview_receipt_result(&receipt)
            .expect("changed metadata patch should remain summarized");
        assert_eq!(
            changed_summary["preview"]["changed_metadata_patch_count"],
            1
        );
    }

    #[test]
    fn malformed_compact_file_preview_summary_fails_closed_in_projection() {
        let mut receipt = generated_file_preview_record(502, 1);
        let summary = compact_file_preview_receipt_result(&receipt)
            .expect("full preview should produce a bounded summary");
        receipt.result = Some(summary);
        receipt.result.as_mut().unwrap()["preview"]["changed_metadata_patch_count"] = json!(1);

        assert!(!is_valid_compact_file_preview_result(&receipt));
        let previews = preview_snapshots(&[&receipt], 0);
        assert_eq!(previews[0]["preview_incomplete"], true);
        assert!(previews[0].get("preview").is_none());
        let operation = operation_snapshot(&receipt, true);
        assert_eq!(operation["details_incomplete"], true);
        assert_eq!(operation["result_truncated"], true);
    }

    #[test]
    fn oversized_file_confirm_receipt_keeps_completion_and_retryable_partial_state() {
        let mut complete = generated_file_confirm_record(503, 5_000, false);
        assert!(serde_json::to_vec(&complete).unwrap().len() > 1024 * 1024);
        assert!(
            complete
                .compact_oversized_result()
                .expect("completed batch result should compact")
        );
        assert_eq!(complete.status, InboxStatus::Completed);
        let complete_summary = &complete.result.as_ref().unwrap()["operation"];
        assert_eq!(complete_summary["file_confirm_summary"], true);
        assert_eq!(complete_summary["status"], "complete");
        assert_eq!(complete_summary["selected_count"], 5_000);
        assert_eq!(complete_summary["file_count"], 20_000);
        assert_eq!(complete_summary["moved_file_count"], 20_000);
        assert_eq!(complete_summary["metadata_patched_item_count"], 2_500);
        assert_eq!(complete_summary["error_count"], 0);
        assert_eq!(complete_summary["omitted_item_count"], 4_996);
        assert!(complete_summary.get("items").is_none());
        let complete_operation = operation_snapshot(&complete, true);
        assert_eq!(complete_operation["details_incomplete"], true);
        assert_eq!(complete_operation["result_truncated"], true);
        assert_eq!(
            complete_operation["result"]["operation"]["file_confirm_summary"],
            true
        );

        let mut retryable_partial = generated_file_confirm_record(504, 5_000, true);
        retryable_partial.status = InboxStatus::Failed;
        retryable_partial.result.as_mut().unwrap()["operation"]["status"] = json!("partial");
        retryable_partial.result.as_mut().unwrap()["operation"]["retryable_access_error"] =
            json!(true);
        assert!(
            retryable_partial
                .compact_oversized_result()
                .expect("retryable partial batch result should compact")
        );
        assert_eq!(retryable_partial.status, InboxStatus::Failed);
        assert_eq!(
            retryable_partial.result.as_ref().unwrap()["operation"]["status"],
            "partial"
        );
        assert_eq!(
            retryable_partial.result.as_ref().unwrap()["operation"]["retryable_access_error"],
            true
        );
    }
}
