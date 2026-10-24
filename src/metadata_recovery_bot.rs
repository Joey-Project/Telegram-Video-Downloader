//! Bound cloud commands for local metadata recovery analysis.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
#[cfg(test)]
use crate::metadata_recovery::RecoveryQuery;
use crate::metadata_recovery::{RecoveryReport, ResolvedMetadataEntry, analyze_metadata_recovery};

const METADATA_RECOVERY_ANALYSIS_BUDGET: Duration = Duration::from_secs(120);
const MAX_RECOVERY_REASON_BYTES: usize = 512;

pub(super) type RecoveryQueryFuture =
    Pin<Box<dyn Future<Output = Result<Vec<ResolvedMetadataEntry>>> + Send + 'static>>;
#[cfg(test)]
pub(super) type TestMetadataRecoveryResolver =
    Arc<dyn Fn(RecoveryQuery) -> RecoveryQueryFuture + Send + Sync + 'static>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecoveryScope {
    Selected,
    Library,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataRecoveryPayload {
    library_id: String,
    root_binding: String,
    scope: RecoveryScope,
    #[serde(default)]
    item_ids: Option<Vec<String>>,
    #[serde(default)]
    after_item_id: Option<String>,
}

struct PreparedMetadataRecovery {
    library_id: String,
    root_binding: String,
    selected_item_ids: Option<Vec<String>>,
    after_item_id: Option<String>,
}

pub(super) async fn dispatch(
    runtime: &CloudBotRuntime,
    request: &CloudRequest,
    library_id: &str,
    root_binding: &str,
) -> Result<CloudDispatchOutcome> {
    let prepared = match prepare_payload(&request.payload, library_id, root_binding) {
        Ok(prepared) => prepared,
        Err(error) => return Ok(cloud_dispatch_failure(error)),
    };

    let library_id_for_scan = prepared.library_id.clone();
    let root_binding_for_scan = prepared.root_binding.clone();
    #[cfg(test)]
    let file_provider = runtime.test_file_provider.clone();
    #[cfg(not(test))]
    let file_provider = None;
    let snapshot = run_cloud_library_operation_preserving_availability_with_root_binding(
        runtime,
        library_id_for_scan,
        root_binding_for_scan,
        "metadata-recovery snapshot scan failed",
        move |context, inbox, root| {
            let manager = open_cloud_library_manager_with_bound_root(root, inbox, file_provider)?;
            let snapshot = manager.scan(&queue_source_hints(&context)?)?;
            Ok((snapshot.clone(), snapshot))
        },
    )
    .await?;

    // The snapshot publication and library I/O permits have been released before resolver
    // calls begin. Preview construction rechecks this exact revision against a fresh snapshot.
    if runtime.epoch_fatal.is_signaled() {
        bail!("cloud database epoch changed; metadata recovery is stopped");
    }
    let selected_item_ids = prepared.selected_item_ids.as_deref();
    let after_item_id = prepared.after_item_id.as_deref();
    let config = Arc::clone(&runtime.bot.config);
    #[cfg(test)]
    let test_resolver = runtime.test_metadata_recovery_resolver.clone();

    let report = tokio::time::timeout(
        METADATA_RECOVERY_ANALYSIS_BUDGET,
        analyze_metadata_recovery(
            &snapshot,
            selected_item_ids,
            after_item_id,
            move |query| -> RecoveryQueryFuture {
                #[cfg(test)]
                if let Some(resolver) = test_resolver.as_ref() {
                    return resolver(query);
                }

                let config = Arc::clone(&config);
                Box::pin(async move {
                    crate::downloader::resolve_bilibili_metadata_recovery_source(
                        config.as_ref(),
                        &query.source_id,
                    )
                    .await
                })
            },
        ),
    )
    .await
    .context("metadata recovery exceeded its 120-second analysis budget")??;

    if runtime.epoch_fatal.is_signaled() {
        bail!("cloud database epoch changed; metadata recovery result was not persisted");
    }

    let recovery = recovery_report_value(report)?;
    let scope = if prepared.selected_item_ids.is_some() {
        "selected"
    } else {
        "library"
    };
    let result = json!({
        "library_id": prepared.library_id,
        "root_binding": prepared.root_binding,
        "scope": scope,
        "recovery": recovery,
    });
    let result_size = serde_json::to_vec(&result)
        .context("metadata recovery report could not be encoded")?
        .len();
    if result_size > crate::cloud_snapshot_budget::MAX_METADATA_RECOVERY_RESULT_BYTES {
        bail!(
            "metadata recovery report exceeds the 512 KiB result limit; select fewer items and analyze again"
        );
    }

    Ok(CloudDispatchOutcome {
        status: InboxStatus::Completed,
        result: Some(result),
        error: None,
        related_preview: None,
    })
}

fn prepare_payload(
    payload: &Value,
    expected_library_id: &str,
    expected_root_binding: &str,
) -> std::result::Result<PreparedMetadataRecovery, String> {
    let payload: MetadataRecoveryPayload = serde_json::from_value(payload.clone())
        .map_err(|error| format!("invalid metadata_recover payload: {error}"))?;
    if payload.library_id != expected_library_id || payload.root_binding != expected_root_binding {
        return Err(
            "metadata recovery library binding changed; refresh the library and retry".to_string(),
        );
    }
    if payload.library_id.trim().is_empty() || payload.library_id.len() > 128 {
        return Err("metadata recovery library ID is invalid".to_string());
    }

    let (selected_item_ids, after_item_id) = match payload.scope {
        RecoveryScope::Selected => {
            if payload.after_item_id.is_some() {
                return Err("selected metadata recovery cannot use a library cursor".to_string());
            }
            let Some(item_ids) = payload.item_ids else {
                return Err("selected metadata recovery requires item_ids".to_string());
            };
            if item_ids.is_empty() || item_ids.len() > 100 {
                return Err(
                    "selected metadata recovery must include between 1 and 100 items".to_string(),
                );
            }
            let mut unique = std::collections::HashSet::new();
            if item_ids.iter().any(|item_id| {
                item_id.trim().is_empty() || item_id.len() > 256 || !unique.insert(item_id)
            }) {
                return Err(
                    "selected metadata recovery contains an invalid or duplicate item ID"
                        .to_string(),
                );
            }
            (Some(item_ids), None)
        }
        RecoveryScope::Library => {
            if payload.item_ids.is_some() {
                return Err("library metadata recovery cannot include item_ids".to_string());
            }
            if payload
                .after_item_id
                .as_ref()
                .is_some_and(|cursor| cursor.trim().is_empty() || cursor.len() > 256)
            {
                return Err("the metadata recovery cursor is invalid".to_string());
            }
            (None, payload.after_item_id)
        }
    };

    Ok(PreparedMetadataRecovery {
        library_id: payload.library_id,
        root_binding: payload.root_binding,
        selected_item_ids,
        after_item_id,
    })
}

fn recovery_report_value(mut report: RecoveryReport) -> Result<Value> {
    if report.items.len() > 100 {
        bail!("metadata recovery returned more than 100 items in one batch");
    }
    for item in &mut report.items {
        if let Some(reason) = item.reason.as_mut() {
            *reason = bounded_redacted_text(reason, MAX_RECOVERY_REASON_BYTES);
        }
    }
    let report =
        serde_json::to_value(report).context("metadata recovery report could not be encoded")?;
    if report["revision"].as_str().is_none_or(str::is_empty)
        || !report["items"].is_array()
        || report["items"]
            .as_array()
            .is_some_and(|items| items.len() > 100)
    {
        bail!("metadata recovery returned an invalid report");
    }
    Ok(report)
}

fn bounded_redacted_text(text: &str, limit: usize) -> String {
    let redacted = crate::redaction::redact_sensitive_text(text);
    if redacted.len() <= limit {
        return redacted;
    }
    let mut end = limit;
    while !redacted.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &redacted[..end])
}
