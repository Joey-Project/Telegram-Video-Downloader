use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::cloud::{CloudInbox, CloudRequest, InboxStatus};
use crate::staging_cleanup::CleanupPreview;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CleanupOperation {
    Scan,
    Preview,
    Confirm,
    Cancel,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StagingCleanupPayload {
    operation: CleanupOperation,
    library_id: String,
    root_binding: String,
    #[serde(default)]
    item_ids: Vec<String>,
    #[serde(default)]
    preview_seq: Option<u64>,
    #[serde(default)]
    preview_id: Option<String>,
    #[serde(default)]
    revision: Option<String>,
}

pub(super) async fn dispatch(
    runtime: &crate::CloudBotRuntime,
    request: &CloudRequest,
    expected_library_id: &str,
    expected_root_binding: &str,
) -> Result<crate::CloudDispatchOutcome> {
    let payload: StagingCleanupPayload =
        match serde_json::from_value::<StagingCleanupPayload>(request.payload.clone()) {
            Ok(payload) => payload,
            Err(error) => {
                return Ok(crate::cloud_dispatch_failure(format!(
                    "invalid staging_cleanup payload: {error}"
                )));
            }
        };
    if payload.library_id != expected_library_id || payload.root_binding != expected_root_binding {
        return Ok(crate::cloud_dispatch_failure(
            "The staging request no longer matches its registered library. Refresh the library and try again.",
        ));
    }

    if let Err(error) = runtime
        .bot
        .queue
        .require_video_library_root_binding(&payload.library_id, &payload.root_binding)
    {
        return Ok(crate::cloud_dispatch_failure(format!("{error:#}")));
    }

    let allowed_chat_ids = runtime.bot.config.telegram.allowed_chat_ids.clone();
    match payload.operation {
        CleanupOperation::Scan => {
            if !payload.item_ids.is_empty()
                || payload.preview_seq.is_some()
                || payload.preview_id.is_some()
                || payload.revision.is_some()
            {
                return Ok(crate::cloud_dispatch_failure(
                    "A staging scan accepts only the library identity.",
                ));
            }
            let queue = Arc::clone(&runtime.bot.queue);
            let library_id = payload.library_id.clone();
            let root_binding = payload.root_binding.clone();
            let scan_result = tokio::task::spawn_blocking(move || {
                queue.scan_staging_cleanup(&library_id, &root_binding, &allowed_chat_ids)
            })
            .await
            .context("staging scan worker failed")?;
            let scan = match scan_result {
                Ok(scan) => scan,
                Err(error) if is_retryable_queue_error(&error) => return Err(error),
                Err(error) => {
                    return Ok(crate::cloud_dispatch_failure(format!(
                        "Staging scan failed: {error:#}"
                    )));
                }
            };
            let result = json!({
                "operation": "scan",
                "library_id": payload.library_id,
                "root_binding": payload.root_binding,
                "scan": scan,
            });
            if !CloudInbox::dispatch_result_fits(request, InboxStatus::Completed, &result)? {
                return Ok(crate::cloud_dispatch_failure(
                    "The staging list exceeds the durable receipt limit. Refresh the list after narrowing the selection.",
                ));
            }
            Ok(crate::CloudDispatchOutcome {
                status: InboxStatus::Completed,
                result: Some(result),
                error: None,
                related_preview: None,
            })
        }
        CleanupOperation::Preview => {
            if payload.item_ids.is_empty()
                || payload.item_ids.len() > 5_000
                || payload.preview_seq.is_some()
                || payload.preview_id.is_some()
                || payload.revision.is_some()
            {
                return Ok(crate::cloud_dispatch_failure(
                    "Select between 1 and 5,000 staging directories for a cleanup preview.",
                ));
            }
            let unique = payload.item_ids.iter().collect::<HashSet<_>>();
            if unique.len() != payload.item_ids.len()
                || payload.item_ids.iter().any(|id| id.is_empty())
            {
                return Ok(crate::cloud_dispatch_failure(
                    "A staging cleanup selection contains an empty or duplicate item ID.",
                ));
            }
            let queue = Arc::clone(&runtime.bot.queue);
            let library_id = payload.library_id.clone();
            let root_binding = payload.root_binding.clone();
            let item_ids = payload.item_ids;
            let preview_result = tokio::task::spawn_blocking(move || {
                queue.preview_staging_cleanup(
                    &library_id,
                    &root_binding,
                    &allowed_chat_ids,
                    &item_ids,
                )
            })
            .await
            .context("staging preview worker failed")?;
            let preview = match preview_result {
                Ok(preview) => preview,
                Err(error) if is_retryable_queue_error(&error) => return Err(error),
                Err(error) => {
                    return Ok(crate::cloud_dispatch_failure(format!(
                        "Staging cleanup preview failed: {error:#}"
                    )));
                }
            };
            if preview.library_id != payload.library_id
                || preview.root_binding != payload.root_binding
            {
                return Ok(crate::cloud_dispatch_failure(
                    "The cleanup preview no longer matches the registered library. Run a fresh scan.",
                ));
            }
            let cleanup_preview = serde_json::to_value(&preview)
                .context("failed to encode staging cleanup preview")?;
            let public_preview = public_preview_summary(&preview)?;
            let result = json!({
                "operation": "preview",
                "preview_id": preview.id,
                "revision": preview.revision,
                "library_id": preview.library_id,
                "root_binding": preview.root_binding,
                "preview": public_preview,
                "cleanup_preview": cleanup_preview,
            });
            if !CloudInbox::dispatch_result_fits(
                request,
                InboxStatus::AwaitingConfirmation,
                &result,
            )? {
                return Ok(crate::cloud_dispatch_failure(
                    "This cleanup preview is too large to retain safely. Select fewer staging directories and preview again.",
                ));
            }
            Ok(crate::CloudDispatchOutcome {
                status: InboxStatus::AwaitingConfirmation,
                result: Some(result),
                error: None,
                related_preview: None,
            })
        }
        CleanupOperation::Confirm => {
            let Some(preview_seq) = payload.preview_seq else {
                return Ok(crate::cloud_dispatch_failure(
                    "The cleanup confirmation is missing its preview sequence.",
                ));
            };
            let (Some(preview_id), Some(revision)) = (payload.preview_id, payload.revision) else {
                return Ok(crate::cloud_dispatch_failure(
                    "The cleanup confirmation is missing its preview identity.",
                ));
            };
            if !payload.item_ids.is_empty()
                || !valid_preview_scalar(&preview_id, 96)
                || !valid_preview_scalar(&revision, 256)
            {
                return Ok(crate::cloud_dispatch_failure(
                    "The cleanup confirmation has an invalid preview identity.",
                ));
            }
            let inbox = Arc::clone(&runtime.inbox);
            let confirmation_seq = request.seq;
            let confirmation_id = preview_id.clone();
            let preview_record = tokio::task::spawn_blocking(move || {
                inbox.awaiting_confirmation_record(&confirmation_id, confirmation_seq)
            })
            .await
            .context("staging preview lookup worker failed")??;
            let Some(preview_record) =
                preview_record.filter(|record| record.request.seq == preview_seq)
            else {
                return Ok(crate::cloud_dispatch_failure(
                    "This cleanup preview is no longer awaiting confirmation. Run a fresh scan and preview.",
                ));
            };
            let Some(result) = preview_record.result.as_ref() else {
                return Ok(crate::cloud_dispatch_failure(
                    "The persisted cleanup preview is incomplete. Run a fresh scan and preview.",
                ));
            };
            if result.get("library_id").and_then(Value::as_str) != Some(payload.library_id.as_str())
                || result.get("root_binding").and_then(Value::as_str)
                    != Some(payload.root_binding.as_str())
                || result.get("preview_id").and_then(Value::as_str) != Some(preview_id.as_str())
                || result.get("revision").and_then(Value::as_str) != Some(revision.as_str())
            {
                return Ok(crate::cloud_dispatch_failure(
                    "The cleanup preview identity does not match this request. Run a fresh preview.",
                ));
            }
            let Some(preview_value) = result.get("cleanup_preview") else {
                return Ok(crate::cloud_dispatch_failure(
                    "The cleanup preview has no retained local plan. Run a fresh preview.",
                ));
            };
            let preview: CleanupPreview = match serde_json::from_value(preview_value.clone()) {
                Ok(preview) => preview,
                Err(error) => {
                    return Ok(crate::cloud_dispatch_failure(format!(
                        "The retained cleanup plan is invalid; run a fresh preview: {error}"
                    )));
                }
            };
            if preview.id != preview_id
                || preview.revision != revision
                || preview.library_id != payload.library_id
                || preview.root_binding != payload.root_binding
            {
                return Ok(crate::cloud_dispatch_failure(
                    "The retained cleanup plan does not match the registered library. Run a fresh preview.",
                ));
            }

            let queue = Arc::clone(&runtime.bot.queue);
            let allowed_chat_ids = runtime.bot.config.telegram.allowed_chat_ids.clone();
            let cleanup_result = tokio::task::spawn_blocking(move || {
                queue.confirm_staging_cleanup(&preview, &allowed_chat_ids)
            })
            .await
            .context("staging cleanup confirmation worker failed")?;
            let cleanup = match cleanup_result {
                Ok(cleanup) => cleanup,
                Err(error) if is_retryable_queue_error(&error) => return Err(error),
                Err(error) => {
                    let mut failure = crate::cloud_dispatch_failure(format!(
                        "The cleanup preview is stale or could not be applied: {error:#}"
                    ));
                    failure.related_preview = Some((preview_seq, InboxStatus::Stale));
                    return Ok(failure);
                }
            };
            let preview_status = if cleanup.failed.is_empty() {
                InboxStatus::Completed
            } else {
                InboxStatus::Stale
            };
            Ok(crate::CloudDispatchOutcome {
                status: InboxStatus::Completed,
                result: Some(json!({
                    "operation": "confirm",
                    "library_id": payload.library_id,
                    "root_binding": payload.root_binding,
                    "preview_id": preview_id,
                    "revision": revision,
                    "cleanup": cleanup,
                })),
                error: None,
                related_preview: Some((preview_seq, preview_status)),
            })
        }
        CleanupOperation::Cancel => cancel_saved_preview(runtime, payload).await,
    }
}

pub(super) async fn dispatch_cancel(
    runtime: &crate::CloudBotRuntime,
    request: &CloudRequest,
) -> Result<crate::CloudDispatchOutcome> {
    let payload: StagingCleanupPayload =
        match serde_json::from_value::<StagingCleanupPayload>(request.payload.clone()) {
            Ok(payload) if payload.operation == CleanupOperation::Cancel => payload,
            Ok(_) => {
                return Ok(crate::cloud_dispatch_failure(
                    "The staging cleanup request is not a cancellation.",
                ));
            }
            Err(error) => {
                return Ok(crate::cloud_dispatch_failure(format!(
                    "invalid staging_cleanup cancellation payload: {error}"
                )));
            }
        };
    cancel_saved_preview(runtime, payload).await
}

async fn cancel_saved_preview(
    runtime: &crate::CloudBotRuntime,
    payload: StagingCleanupPayload,
) -> Result<crate::CloudDispatchOutcome> {
    let (Some(preview_seq), Some(preview_id), Some(revision)) =
        (payload.preview_seq, payload.preview_id, payload.revision)
    else {
        return Ok(crate::cloud_dispatch_failure(
            "The cleanup cancellation is missing its preview identity.",
        ));
    };
    if !payload.item_ids.is_empty()
        || !valid_preview_scalar(&preview_id, 96)
        || !valid_preview_scalar(&revision, 256)
    {
        return Ok(crate::cloud_dispatch_failure(
            "The cleanup cancellation has an invalid preview identity.",
        ));
    }

    let inbox = Arc::clone(&runtime.inbox);
    let preview_record = tokio::task::spawn_blocking({
        let inbox = Arc::clone(&inbox);
        move || inbox.record(preview_seq)
    })
    .await
    .context("staging preview cancellation lookup worker failed")??;
    let Some(_preview_record) = preview_record.filter(|record| {
        record.request.is_staging_cleanup_preview()
            && record.request.seq == preview_seq
            && record
                .result
                .as_ref()
                .and_then(|result| result.get("preview_id"))
                .and_then(Value::as_str)
                == Some(preview_id.as_str())
            && record
                .result
                .as_ref()
                .and_then(|result| result.get("revision"))
                .and_then(Value::as_str)
                == Some(revision.as_str())
            && record
                .request
                .payload
                .get("library_id")
                .and_then(Value::as_str)
                == Some(payload.library_id.as_str())
            && record
                .request
                .payload
                .get("root_binding")
                .and_then(Value::as_str)
                == Some(payload.root_binding.as_str())
            && record
                .result
                .as_ref()
                .and_then(|result| result.get("library_id"))
                .and_then(Value::as_str)
                == Some(payload.library_id.as_str())
            && record
                .result
                .as_ref()
                .and_then(|result| result.get("root_binding"))
                .and_then(Value::as_str)
                == Some(payload.root_binding.as_str())
    }) else {
        return Ok(crate::cloud_dispatch_failure(
            "The cleanup preview does not match this library or its recorded identity.",
        ));
    };

    let cancel_preview_id = preview_id.clone();
    let cancel_revision = revision.clone();
    let cancelled = tokio::task::spawn_blocking(move || {
        inbox.cancel_awaiting_preview(
            "staging_cleanup",
            preview_seq,
            &cancel_preview_id,
            &cancel_revision,
        )
    })
    .await
    .context("staging preview cancellation worker failed")??;
    if cancelled.is_none() {
        return Ok(crate::cloud_dispatch_failure(
            "This cleanup preview is no longer cancellable or its identity does not match.",
        ));
    }
    Ok(crate::CloudDispatchOutcome {
        status: InboxStatus::Completed,
        result: Some(json!({
            "operation": "cancel",
            "cancelled": true,
            "preview_seq": preview_seq,
            "preview_id": preview_id,
            "revision": revision,
        })),
        error: None,
        related_preview: None,
    })
}

pub(super) async fn unconfirmable_previews(
    runtime: &crate::CloudBotRuntime,
    records: &[crate::cloud::InboxRecord],
) -> (HashSet<u64>, bool) {
    let mut bindings = Vec::new();
    let mut unconfirmable = HashSet::new();
    for record in records {
        if record.status != InboxStatus::AwaitingConfirmation
            || !record.request.is_staging_cleanup_preview()
        {
            continue;
        }
        let request_library_id = record
            .request
            .payload
            .get("library_id")
            .and_then(Value::as_str);
        let request_root_binding = record
            .request
            .payload
            .get("root_binding")
            .and_then(Value::as_str);
        let result_library_id = record
            .result
            .as_ref()
            .and_then(|result| result.get("library_id"))
            .and_then(Value::as_str);
        let result_root_binding = record
            .result
            .as_ref()
            .and_then(|result| result.get("root_binding"))
            .and_then(Value::as_str);
        let Some((library_id, root_binding)) = request_library_id
            .zip(request_root_binding)
            .filter(|(request_id, _)| !request_id.trim().is_empty())
            .zip(result_library_id.zip(result_root_binding))
            .filter(
                |((request_id, request_binding), (result_id, result_binding))| {
                    request_id == result_id && request_binding == result_binding
                },
            )
            .map(|((library_id, root_binding), _)| {
                (library_id.to_string(), root_binding.to_string())
            })
        else {
            unconfirmable.insert(record.request.seq);
            continue;
        };
        bindings.push((record.request.seq, library_id, root_binding));
    }
    if bindings.is_empty() {
        return (unconfirmable, false);
    }

    let queue = Arc::clone(&runtime.bot.queue);
    let checked = tokio::task::spawn_blocking(move || -> Result<HashSet<u64>> {
        let registry = queue.registry_snapshot()?;
        let mut stale = HashSet::new();
        for (sequence, library_id, root_binding) in bindings {
            let registered_binding = registry
                .libraries
                .iter()
                .find(|library| library.id == library_id)
                .and_then(|library| library.root_binding_token());
            if registered_binding.as_deref() != Some(root_binding.as_str()) {
                stale.insert(sequence);
            }
        }
        Ok(stale)
    })
    .await;
    match checked {
        Ok(Ok(stale)) => {
            unconfirmable.extend(stale);
            (unconfirmable, false)
        }
        Ok(Err(error)) => {
            tracing::warn!(
                error = %crate::redaction::redact_sensitive_text(&format!("{error:#}")),
                "could not read the library registry while verifying staging cleanup previews"
            );
            (unconfirmable, true)
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "staging cleanup preview binding worker failed"
            );
            (unconfirmable, true)
        }
    }
}

fn valid_preview_scalar(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= max_bytes
        && !value.chars().any(char::is_control)
}

fn is_retryable_queue_error(error: &anyhow::Error) -> bool {
    crate::file_provider::is_file_provider_access_error(error)
        || error.chain().any(|cause| {
            cause
                .downcast_ref::<crate::staging_cleanup::StagingCleanupBusy>()
                .is_some()
        })
}

fn public_preview_summary(preview: &CleanupPreview) -> Result<Value> {
    const MAX_SAMPLE_ITEMS: usize = 4;
    let sample_items = preview
        .items
        .iter()
        .take(MAX_SAMPLE_ITEMS)
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(json!({
        "id": preview.id,
        "revision": preview.revision,
        "library_id": preview.library_id,
        "root_binding": preview.root_binding,
        "created_at": preview.created_at,
        "item_count": preview.items.len(),
        "omitted_item_count": preview.items.len().saturating_sub(sample_items.len()),
        "total_bytes": preview.total_bytes,
        "sample_items": sample_items,
    }))
}
