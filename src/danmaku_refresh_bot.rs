//! Durable, explicitly confirmed append-only updates of existing danmaku sidecars.

use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
use crate::danmaku_refresh::DanmakuRefreshPreview;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Preview,
    Confirm,
    Cancel,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    operation: Operation,
    library_id: String,
    root_binding: String,
    #[serde(default)]
    expected_revision: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    item_ids: Option<Vec<String>>,
    #[serde(default)]
    preview_seq: Option<u64>,
    #[serde(default)]
    preview_id: Option<String>,
    #[serde(default)]
    revision: Option<String>,
}

fn decode(request: &CloudRequest) -> Result<Payload> {
    ensure!(
        request.kind == "danmaku_refresh",
        "invalid danmaku request kind"
    );
    let payload: Payload = serde_json::from_value(request.payload.clone())
        .context("invalid danmaku_refresh payload")?;
    ensure!(valid_scalar(&payload.library_id, 128), "invalid library ID");
    ensure!(
        valid_scalar(&payload.root_binding, 256),
        "invalid root binding"
    );
    match payload.operation {
        Operation::Preview => {
            ensure!(
                payload.preview_seq.is_none()
                    && payload.preview_id.is_none()
                    && payload.revision.is_none(),
                "a danmaku preview cannot contain confirmation fields"
            );
            ensure!(
                payload
                    .expected_revision
                    .as_deref()
                    .is_some_and(|v| valid_scalar(v, 256)),
                "a danmaku preview requires the current library revision"
            );
            match payload.scope.as_deref() {
                Some("selected") => {
                    let ids = payload
                        .item_ids
                        .as_deref()
                        .context("selected danmaku refresh requires item IDs")?;
                    let unique = ids.iter().collect::<HashSet<_>>();
                    ensure!(
                        !ids.is_empty()
                            && ids.len() <= 5_000
                            && unique.len() == ids.len()
                            && ids.iter().all(|id| valid_scalar(id, 256)),
                        "select between 1 and 5,000 unique items"
                    );
                }
                Some("library") => ensure!(
                    payload.item_ids.is_none(),
                    "a library refresh cannot include item IDs"
                ),
                _ => bail!("invalid danmaku refresh scope"),
            }
        }
        Operation::Confirm | Operation::Cancel => {
            ensure!(
                payload.expected_revision.is_none()
                    && payload.scope.is_none()
                    && payload.item_ids.is_none(),
                "confirmation and cancellation cannot change the reviewed selection"
            );
            ensure!(
                payload
                    .preview_seq
                    .is_some_and(|seq| seq > 0 && seq <= 9_007_199_254_740_991),
                "invalid preview sequence"
            );
            ensure!(
                payload
                    .preview_id
                    .as_deref()
                    .is_some_and(|v| valid_scalar(v, 96))
                    && payload
                        .revision
                        .as_deref()
                        .is_some_and(|v| valid_scalar(v, 256)),
                "invalid preview identity"
            );
        }
    }
    Ok(payload)
}

fn valid_scalar(value: &str, limit: usize) -> bool {
    !value.is_empty()
        && value.len() <= limit
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

pub(super) async fn dispatch(
    runtime: &CloudBotRuntime,
    request: &CloudRequest,
    library_id: &str,
    root_binding: &str,
) -> Result<CloudDispatchOutcome> {
    let payload = match decode(request) {
        Ok(payload) => payload,
        Err(error) => return Ok(cloud_dispatch_failure(format!("{error:#}"))),
    };
    if payload.library_id != library_id || payload.root_binding != root_binding {
        return Ok(cloud_dispatch_failure(
            "The danmaku request no longer matches its library. Refresh and try again.",
        ));
    }
    match payload.operation {
        Operation::Preview => preview(runtime, request, payload).await,
        Operation::Confirm => confirm(runtime, request, payload).await,
        Operation::Cancel => cancel(runtime, payload).await,
    }
}

async fn preview(
    runtime: &CloudBotRuntime,
    request: &CloudRequest,
    payload: Payload,
) -> Result<CloudDispatchOutcome> {
    let library_id = payload.library_id.clone();
    let root_binding = payload.root_binding.clone();
    #[cfg(test)]
    let provider = runtime.test_file_provider.clone();
    #[cfg(not(test))]
    let provider = None;
    let plan = run_cloud_library_operation_preserving_availability_with_root_binding(
        runtime,
        library_id.clone(),
        root_binding.clone(),
        "danmaku preview worker failed",
        move |context, inbox, root| {
            let manager =
                open_cloud_library_manager_with_bound_root(root.clone(), inbox, provider)?;
            let snapshot = manager.scan(&queue_source_hints(&context)?)?;
            let plan = crate::danmaku_refresh::preview(
                &root,
                &snapshot,
                &payload.library_id,
                &payload.root_binding,
                payload
                    .expected_revision
                    .as_deref()
                    .expect("validated preview revision"),
                payload.scope.as_deref().expect("validated scope"),
                payload.item_ids.as_deref(),
            )?;
            Ok((plan, snapshot))
        },
    )
    .await?;
    let public = serde_json::to_value(plan.public_value())?;
    let result = json!({
        "operation": "preview", "library_id": library_id, "root_binding": root_binding,
        "preview_id": public["id"], "revision": public["revision"],
        "preview": public, "danmaku_preview": plan,
    });
    if !CloudInbox::dispatch_result_fits(request, InboxStatus::AwaitingConfirmation, &result)? {
        return Ok(cloud_dispatch_failure(
            "This danmaku preview exceeds the saved receipt limit. Select fewer videos and preview again.",
        ));
    }
    Ok(CloudDispatchOutcome {
        status: InboxStatus::AwaitingConfirmation,
        result: Some(result),
        error: None,
        related_preview: None,
    })
}

fn matches_identity(record: &InboxRecord, payload: &Payload) -> bool {
    record.request.kind == "danmaku_refresh"
        && record.request.is_bound_preview()
        && Some(record.request.seq) == payload.preview_seq
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
        && record.result.as_ref().is_some_and(|result| {
            result.get("library_id").and_then(Value::as_str) == Some(payload.library_id.as_str())
                && result.get("root_binding").and_then(Value::as_str)
                    == Some(payload.root_binding.as_str())
                && result.get("preview_id").and_then(Value::as_str) == payload.preview_id.as_deref()
                && result.get("revision").and_then(Value::as_str) == payload.revision.as_deref()
        })
}

async fn confirm(
    runtime: &CloudBotRuntime,
    request: &CloudRequest,
    payload: Payload,
) -> Result<CloudDispatchOutcome> {
    let inbox = Arc::clone(&runtime.inbox);
    let confirmation_seq = request.seq;
    let preview_id = payload.preview_id.clone().expect("validated preview ID");
    let lookup_id = preview_id.clone();
    let record = tokio::task::spawn_blocking(move || {
        inbox.awaiting_confirmation_record(&lookup_id, confirmation_seq)
    })
    .await
    .context("danmaku preview lookup worker failed")??;
    let Some(record) = record.filter(|record| matches_identity(record, &payload)) else {
        return Ok(cloud_dispatch_failure(
            "This danmaku preview is no longer awaiting confirmation or its library identity changed. Create a fresh preview.",
        ));
    };
    let preview_seq = record.request.seq;
    let plan: DanmakuRefreshPreview = serde_json::from_value(
        record
            .result
            .as_ref()
            .and_then(|result| result.get("danmaku_preview"))
            .cloned()
            .context("the retained local danmaku plan is missing")?,
    )
    .context("the retained local danmaku plan is invalid")?;
    let public = serde_json::to_value(plan.public_value())?;
    if public["id"].as_str() != payload.preview_id.as_deref()
        || public["revision"].as_str() != payload.revision.as_deref()
        || public["library_id"].as_str() != Some(payload.library_id.as_str())
        || public["root_binding"].as_str() != Some(payload.root_binding.as_str())
    {
        return Ok(cloud_dispatch_failure(
            "The retained danmaku plan does not match the confirmed preview.",
        ));
    }
    let lease = library_migration_bot::try_acquire_library_io(
        runtime,
        std::slice::from_ref(&payload.library_id),
    )
    .await?;
    let queue = Arc::clone(&runtime.bot.queue);
    let context = runtime.bot.clone();
    let inbox = Arc::clone(&runtime.inbox);
    let config = Arc::clone(&runtime.bot.config);
    let fatal = Arc::clone(&runtime.epoch_fatal);
    let library_id = payload.library_id.clone();
    let root_binding = payload.root_binding.clone();
    #[cfg(test)]
    let fetcher = runtime.test_danmaku_fetcher.clone();
    #[cfg(not(test))]
    let fetcher = None;
    #[cfg(test)]
    let provider = runtime.test_file_provider.clone();
    #[cfg(not(test))]
    let provider = None;
    let handle = tokio::runtime::Handle::current();
    let completed = tokio::task::spawn_blocking(move || {
        // Keep the I/O lease with the actual worker even if its async caller is cancelled.
        let _lease = lease;
        let root = queue.verified_video_library_root_with_binding(&library_id, &root_binding)?;
        let manager = open_cloud_library_manager_with_bound_root(root.clone(), inbox, provider)?;
        let current = manager.scan(&queue_source_hints(&context)?)?;
        ensure!(
            current.revision == plan.revision,
            "The library metadata changed after review. Refresh the library and create a new danmaku preview."
        );
        let guard: Arc<dyn Fn() -> Result<()> + Send + Sync> = Arc::new(move || {
            ensure!(
                !fatal.is_signaled(),
                "cloud database epoch changed; danmaku writes stopped"
            );
            queue.require_video_library_root_binding(&library_id, &root_binding)
        });
        handle.block_on(crate::danmaku_refresh::execute_with_guard(
            config, root, plan, fetcher, guard,
        ))
    })
    .await
    .context("danmaku update worker failed")?;
    let completed = match completed {
        Ok(completed) => completed,
        Err(error) => {
            let mut outcome = cloud_dispatch_failure(redact_sensitive_text(&format!(
                "Danmaku refresh stopped: {error:#}"
            )));
            outcome.related_preview = Some((preview_seq, InboxStatus::Stale));
            return Ok(outcome);
        }
    };
    let refresh_warning = run_cloud_library_operation_preserving_availability_with_root_binding(
        runtime,
        payload.library_id.clone(),
        payload.root_binding.clone(),
        "danmaku post-update scan failed",
        |context, inbox, root| {
            let manager = open_cloud_library_manager_with_bound_root(root, inbox, None)?;
            let snapshot = manager.scan(&queue_source_hints(&context)?)?;
            Ok(((), snapshot))
        },
    )
    .await
    .err()
    .map(|error| redact_sensitive_text(&format!("{error:#}")));
    let mut result = serde_json::to_value(completed)?;
    let fields = result
        .as_object_mut()
        .context("danmaku result must be an object")?;
    fields.extend([
        ("operation".into(), json!("confirm")),
        ("library_id".into(), json!(payload.library_id)),
        ("root_binding".into(), json!(payload.root_binding)),
        ("preview_id".into(), json!(preview_id)),
        ("revision".into(), json!(payload.revision)),
        ("library_refresh_warning".into(), json!(refresh_warning)),
    ]);
    Ok(CloudDispatchOutcome {
        status: InboxStatus::Completed,
        result: Some(result),
        error: None,
        related_preview: Some((preview_seq, InboxStatus::Completed)),
    })
}

pub(super) async fn dispatch_cancel(
    runtime: &CloudBotRuntime,
    request: &CloudRequest,
) -> Result<CloudDispatchOutcome> {
    let payload = match decode(request) {
        Ok(payload) if payload.operation == Operation::Cancel => payload,
        Ok(_) => {
            return Ok(cloud_dispatch_failure(
                "The danmaku request is not a cancellation.",
            ));
        }
        Err(error) => return Ok(cloud_dispatch_failure(format!("{error:#}"))),
    };
    cancel(runtime, payload).await
}

async fn cancel(runtime: &CloudBotRuntime, payload: Payload) -> Result<CloudDispatchOutcome> {
    let inbox = Arc::clone(&runtime.inbox);
    let preview_seq = payload.preview_seq.expect("validated preview sequence");
    let record = tokio::task::spawn_blocking(move || inbox.record(preview_seq))
        .await
        .context("danmaku cancellation lookup worker failed")??;
    if !record
        .as_ref()
        .is_some_and(|record| matches_identity(record, &payload))
    {
        return Ok(cloud_dispatch_failure(
            "This danmaku preview does not match the recorded library and preview identity.",
        ));
    }
    let preview_id = payload.preview_id.clone().expect("validated preview ID");
    let revision = payload.revision.clone().expect("validated revision");
    let inbox = Arc::clone(&runtime.inbox);
    let cancelled = tokio::task::spawn_blocking(move || {
        inbox.cancel_awaiting_preview("danmaku_refresh", preview_seq, &preview_id, &revision)
    })
    .await
    .context("danmaku cancellation worker failed")??;
    if cancelled.is_none() {
        return Ok(cloud_dispatch_failure(
            "This danmaku preview is no longer cancellable.",
        ));
    }
    Ok(CloudDispatchOutcome {
        status: InboxStatus::Completed,
        result: Some(
            json!({"operation":"cancel", "library_id":payload.library_id,
            "root_binding":payload.root_binding,"preview_id":payload.preview_id,"revision":payload.revision}),
        ),
        error: None,
        related_preview: None,
    })
}
