use std::io::{self, Write};

use anyhow::{Result, anyhow};
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::cloud::{InboxRecord, InboxStatus};

const MAX_OPERATION_SNAPSHOT_RECORDS: usize = 100;
const MAX_OPERATION_SNAPSHOT_BYTES: usize = 2 * 1024 * 1024;
const MAX_COMPACT_OPERATION_RESULT_BYTES: usize = 2 * 1024;
const MAX_OPERATION_ERROR_JSON_BYTES: usize = 1024;
const MAX_OPERATION_KIND_JSON_BYTES: usize = 128;
const MAX_PREVIEW_SNAPSHOT_RECORDS: usize = 100;
const MAX_PREVIEW_SNAPSHOT_BYTES: usize = 3 * 1024 * 1024;
const MAX_PREVIEW_ID_JSON_BYTES: usize = 128;

const OPERATION_DETAILS_OMITTED_REASON: &str =
    "Older operation details were omitted to keep the cloud state within its size limit.";
const PREVIEW_DETAILS_OMITTED_REASON: &str =
    "Preview details exceeded the cloud snapshot budget. Reload the preview before confirming.";
const PREVIEW_DETAILS_UNAVAILABLE_REASON: &str =
    "Preview details are unavailable in this cloud snapshot. Reload the preview before confirming.";
const PREVIEWS_OMITTED_REASON: &str = "Some pending previews were omitted from this snapshot. Refresh pending previews before confirming.";
/// Build the bounded operation receipt list, reserving space for the newest usable legacy import.
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
        if Some(index) == prioritized_legacy_import {
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
    let has_hints = result
        .get("hints")
        .and_then(Value::as_array)
        .is_some_and(|hints| !hints.is_empty());
    let has_candidates = result.get("candidates").and_then(Value::as_array).is_some();
    let result_marked_incomplete = ["incomplete", "details_incomplete", "result_truncated"]
        .iter()
        .any(|field| result.get(field).and_then(Value::as_bool) == Some(true));
    has_hints && has_candidates && !result_marked_incomplete
}

/// Compact large historical receipts after an upload-size rejection.
///
/// The newest usable completed legacy-import result stays intact because its hints are the manual
/// mapping entry point in the Mini App. Failed or explicitly incomplete imports do not reserve the
/// budget. Other details are replaced with a small identity summary.
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

    let mut changed = false;
    for (index, operation) in operations.iter_mut().enumerate() {
        let Some(fields) = operation.as_object_mut() else {
            continue;
        };

        changed |= bound_operation_metadata(fields);
        if Some(index) == latest_legacy_import {
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
            record.request.kind == "file_preview"
                && record.status == InboxStatus::AwaitingConfirmation
        })
        .collect::<Vec<_>>();
    active.sort_by_key(|record| record.request.seq);

    let locally_omitted = active.len().saturating_sub(MAX_PREVIEW_SNAPSHOT_RECORDS);
    if locally_omitted > 0 {
        active.drain(..locally_omitted);
    }
    let omitted_count = omitted_preview_count.saturating_add(locally_omitted);

    let mut snapshots = active
        .iter()
        .map(|record| {
            let result = record.result.as_ref();
            let reason = if result.and_then(|value| value.get("preview")).is_some() {
                PREVIEW_DETAILS_OMITTED_REASON
            } else {
                PREVIEW_DETAILS_UNAVAILABLE_REASON
            };
            preview_placeholder(
                record,
                preview_identity_value(result, "preview_id"),
                preview_identity_value(result, "revision"),
                reason,
            )
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
        let result = record.result.as_ref();
        let Some(preview) = result.and_then(|value| value.get("preview")) else {
            continue;
        };
        let preview_id = preview_identity_value(result, "preview_id");
        let revision = preview_identity_value(result, "revision");
        let placeholder_bytes = json_size_within(&snapshots[index], MAX_PREVIEW_SNAPSHOT_BYTES)
            .expect("preview placeholder should fit the bounded snapshot budget");
        let remaining = MAX_PREVIEW_SNAPSHOT_BYTES
            .saturating_sub(total_bytes.saturating_sub(placeholder_bytes));
        if json_size_within(preview, remaining).is_none() {
            continue;
        }

        let candidate = json!({
            "seq": record.request.seq,
            "preview_id": preview_id,
            "revision": revision,
            "status": "awaiting_confirmation",
            "preview": preview,
        });
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

fn operation_snapshot(record: &InboxRecord, include_full_result: bool) -> Value {
    let result = record.result.as_ref().map(|result| {
        if record.request.kind == "library_scan" {
            library_scan_receipt(result)
        } else if include_full_result {
            result.clone()
        } else {
            compact_result_summary(result)
        }
    });
    let result_was_compacted =
        !include_full_result && record.request.kind != "library_scan" && record.result.is_some();
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
        })?;
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

    fn record(
        seq: u64,
        kind: &str,
        status: InboxStatus,
        result: Option<Value>,
        error: Option<String>,
    ) -> InboxRecord {
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
            "hints": [{"hint_index": 0, "title": "Star Wars", "source_ids": []}],
            "candidates": candidates,
            "warnings": warnings,
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
        assert_eq!(previews[2]["preview"]["items"][0]["path"], "clip-3.mp4");
        assert!(
            previews
                .iter()
                .all(|preview| preview["preview_incomplete"] != true)
        );
        assert!(previews.iter().map(object_size).sum::<usize>() <= MAX_PREVIEW_SNAPSHOT_BYTES);
        assert!(serde_json::to_vec(&previews).unwrap().len() <= MAX_PREVIEW_SNAPSHOT_BYTES);
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
}
