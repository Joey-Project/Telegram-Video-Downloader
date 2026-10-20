use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

const CLOUD_MIGRATION_HISTORY_LIMIT: usize = 32;

fn cloud_migration_history(
    migrations: &BTreeMap<String, crate::library_migration::MigrationStatus>,
) -> Result<(Value, usize)> {
    use crate::library_migration::MigrationRunStatus;

    let priority = |status: &crate::library_migration::MigrationStatus| match status.status {
        MigrationRunStatus::Queued | MigrationRunStatus::Running => 0,
        MigrationRunStatus::Paused | MigrationRunStatus::Failed | MigrationRunStatus::Partial => 1,
        MigrationRunStatus::Completed | MigrationRunStatus::Stopped => 2,
    };
    let mut selected = migrations.values().collect::<Vec<_>>();
    selected.sort_by(|left, right| {
        priority(left)
            .cmp(&priority(right))
            .then_with(|| right.created_at.cmp(&left.created_at))
            .then_with(|| left.id.cmp(&right.id))
    });
    let omitted = selected.len().saturating_sub(CLOUD_MIGRATION_HISTORY_LIMIT);
    selected.truncate(CLOUD_MIGRATION_HISTORY_LIMIT);
    Ok((serde_json::to_value(selected)?, omitted))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterLibrary {
    name: String,
    path: PathBuf,
    #[serde(default)]
    default_category: Option<String>,
    #[serde(default)]
    set_default: bool,
    #[serde(default)]
    create_directory: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LibraryId {
    library_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LibraryCategory {
    library_id: String,
    #[serde(default)]
    default_category: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LibraryName {
    library_id: String,
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DownloadSubmission {
    url: String,
    library_id: String,
    #[serde(default)]
    category: Option<String>,
}

struct CreatedDownload {
    created: bool,
    save_path: PathBuf,
    generation: u64,
    library_id: String,
    category: String,
}

fn category_path(value: Option<&str>) -> Result<Option<PathBuf>> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let path = Path::new(value);
    if value.len() > 1_024
        || value.chars().any(char::is_control)
        || path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        bail!("category must be a bounded root-relative folder");
    }
    Ok(Some(path.to_path_buf()))
}

pub(super) fn applied_settings(queue: &QueueManager) -> Result<Value> {
    let snapshot = queue.registry_snapshot()?;
    let default_category = snapshot
        .default_category
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_default();
    let libraries = snapshot
        .libraries
        .iter()
        .map(|library| {
            json!({
                "id": library.id,
                "name": library.name,
                "root": library.root.to_string_lossy(),
                "default_category": library.default_category.as_ref()
                    .map(|path| path.to_string_lossy().into_owned()).unwrap_or_default(),
                "is_default": library.id == snapshot.default_library_id,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "revision": snapshot.revision.to_string(),
        "default_library_id": snapshot.default_library_id,
        "default_category": default_category,
        "libraries": libraries,
    }))
}

pub(super) fn scoped_runtime(
    runtime: &CloudBotRuntime,
    library_id: Option<&str>,
) -> Result<(CloudBotRuntime, String)> {
    let settings = runtime.bot.queue.registry_snapshot()?;
    let library_id = library_id.unwrap_or(&settings.default_library_id);
    let library = settings
        .libraries
        .iter()
        .find(|library| library.id == library_id)
        .context("the media library is no longer registered")?;
    let mut scoped = runtime.clone();
    let mut config = (*runtime.bot.config).clone();
    config.downloads.video_dir = library.root.clone();
    scoped.bot.config = Arc::new(config);
    Ok((scoped, library.id.clone()))
}

pub(super) fn original_library_id(runtime: &CloudBotRuntime) -> Result<String> {
    let original = runtime.inbox.download_root();
    runtime
        .bot
        .queue
        .registry_snapshot()?
        .libraries
        .into_iter()
        .find(|library| {
            library.root == original
                || std::fs::canonicalize(original).ok().as_ref() == Some(&library.root)
        })
        .map(|library| library.id)
        .context("the original preview library is no longer registered")
}

pub(super) fn original_library_id_from_bot(bot: &BotContext) -> Result<String> {
    bot.queue
        .registry_snapshot()?
        .libraries
        .into_iter()
        .find(|library| same_root(&library.root, &bot.config.downloads.video_dir))
        .map(|library| library.id)
        .context("the original preview library is no longer registered")
}

pub(super) fn same_root(left: &Path, right: &Path) -> bool {
    left == right
        || std::fs::canonicalize(left)
            .ok()
            .zip(std::fs::canonicalize(right).ok())
            .is_some_and(|(left, right)| left == right)
}

pub(super) async fn refresh_libraries(runtime: &CloudBotRuntime) -> Result<()> {
    refresh_library_bindings(runtime).await?;
    let settings = runtime.bot.queue.registry_snapshot()?;
    let default_id = settings.default_library_id.clone();
    for library in settings.libraries {
        let (scoped, _) = scoped_runtime(runtime, Some(&library.id))?;
        let result = run_cloud_library_operation(
            &scoped,
            "media-library refresh worker failed",
            |context, inbox| Ok(((), scan_cloud_library(&context, inbox)?)),
        )
        .await;
        if let Err(error) = result {
            warn!(library_id = %library.id, error = %redact_sensitive_text(&format!("{error:#}")), "media-library refresh failed");
        }
    }
    *runtime.library_scan_warning.lock().await = runtime
        .library_unavailable
        .lock()
        .await
        .get(&default_id)
        .cloned();
    runtime.state_notify.notify_one();
    Ok(())
}

pub(super) async fn refresh_library_bindings(runtime: &CloudBotRuntime) -> Result<()> {
    let queue = Arc::clone(&runtime.bot.queue);
    tokio::task::spawn_blocking(move || queue.refresh_video_libraries())
        .await
        .context("media-library availability worker failed")?
}

pub(super) async fn unconfirmable_previews(
    runtime: &CloudBotRuntime,
    records: &[InboxRecord],
) -> (HashSet<String>, bool) {
    let mut grouped = BTreeMap::<Option<String>, Vec<(String, String)>>::new();
    for record in records {
        if record.request.kind != "file_preview"
            || record.status != InboxStatus::AwaitingConfirmation
        {
            continue;
        }
        if let Some(identity) = cloud_preview_identity(record) {
            let library_id = record
                .result
                .as_ref()
                .and_then(|result| result["library_id"].as_str())
                .or_else(|| record.request.payload["library_id"].as_str())
                .map(str::to_string);
            grouped.entry(library_id).or_default().push(identity);
        }
    }
    let mut unavailable = HashSet::new();
    let mut failed = false;
    for (library_id, keys) in grouped {
        let library_id = library_id
            .map(Ok)
            .unwrap_or_else(|| original_library_id(runtime));
        let scoped = library_id
            .and_then(|id| scoped_runtime(runtime, Some(&id)).map(|(runtime, _)| runtime));
        let result = match scoped {
            Ok(scoped) => cloud_unconfirmable_preview_ids(&scoped, &keys).await,
            Err(error) => Err(error),
        };
        match result {
            Ok(ids) => unavailable.extend(ids),
            Err(error) => {
                failed = true;
                warn!(error = %redact_sensitive_text(&format!("{error:#}")), "could not verify library preview anchors");
            }
        }
    }
    (unavailable, failed)
}

pub(super) async fn attach_library_views(
    runtime: &CloudBotRuntime,
    applied: &Value,
    legacy: Value,
) -> Result<Value> {
    let snapshots = runtime.library_snapshots.lock().await.clone();
    let unavailable = runtime.library_unavailable.lock().await.clone();
    let availability = runtime.bot.queue.video_library_statuses()?;
    let default_id = applied["default_library_id"].as_str().unwrap_or_default();
    let original_id = original_library_id(runtime).ok();
    let task_summaries = if runtime.bot.config.telegram.allow_all_chats {
        BTreeMap::new()
    } else {
        let library_ids = applied["libraries"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|descriptor| descriptor["id"].as_str().map(str::to_string))
            .collect::<Vec<_>>();
        let allowed_chat_ids = runtime.bot.config.telegram.allowed_chat_ids.clone();
        let queue = Arc::clone(&runtime.bot.queue);
        match tokio::task::spawn_blocking(move || {
            let mut summaries = BTreeMap::new();
            for library_id in library_ids {
                let summary = queue.incomplete_task_transfer_summary(
                    &library_id,
                    &allowed_chat_ids,
                );
                match summary {
                    Ok(summary) => {
                        summaries.insert(library_id, Some(summary));
                    }
                    Err(error) => {
                        warn!(library_id = %library_id, error = %redact_sensitive_text(&format!("{error:#}")), "could not summarize incomplete media-library tasks");
                        summaries.insert(library_id, None);
                    }
                }
            }
            summaries
        })
        .await
        {
            Ok(summaries) => summaries,
            Err(error) => {
                warn!(error = %error, "incomplete task summary worker failed");
                BTreeMap::new()
            }
        }
    };
    let mut views = Vec::new();
    let mut selected = None;
    for descriptor in applied["libraries"].as_array().into_iter().flatten() {
        let id = descriptor["id"].as_str().unwrap_or_default();
        let cached = snapshots.get(id);
        let mut snapshot = if let Some(snapshot) = cached {
            serde_json::to_value(snapshot)?
        } else if original_id.as_deref() == Some(id) && id == default_id {
            legacy.clone()
        } else {
            json!({
                "revision": "not-scanned", "scanned_at": 0,
                "root_label": descriptor["name"], "items": [], "categories": [],
                "warnings": ["The local media-library scan has not completed."], "ready": false,
            })
        };
        let warning = unavailable.get(id);
        if let Some(warning) = warning {
            merge_cloud_library_scan_warning(&mut snapshot, warning);
            for item in snapshot["items"].as_array_mut().into_iter().flatten() {
                item["presence"] = json!("unavailable");
            }
        }
        let status = availability.get(id);
        let availability_label =
            if warning.is_some() || status.is_some_and(|status| !status.available) {
                "unavailable"
            } else if snapshot["ready"].as_bool() == Some(false) {
                "unknown"
            } else {
                "available"
            };
        let mut view = descriptor.clone();
        view["availability"] = json!(availability_label);
        view["media_count"] = if snapshot["ready"].as_bool() == Some(false) {
            Value::Null
        } else {
            json!(snapshot["items"].as_array().map(Vec::len))
        };
        view["scanned_at"] = snapshot["scanned_at"].clone();
        view["free_bytes"] = json!(status.and_then(|status| status.available_space_bytes));
        view["total_bytes"] = Value::Null;
        let task_summary = task_summaries.get(id).and_then(Option::as_ref);
        view["incomplete_task_count"] = task_summary
            .map(|summary| json!(summary.task_count))
            .unwrap_or(Value::Null);
        view["staging_bytes"] = task_summary
            .and_then(|summary| summary.staging_bytes)
            .map(|bytes| json!(bytes))
            .unwrap_or(Value::Null);
        view["snapshot"] = snapshot.clone();
        if id == default_id {
            selected = Some(snapshot);
        }
        views.push(view);
    }
    let mut library = selected.unwrap_or(legacy);
    library["libraries"] = json!(views);
    library["default_library_id"] = json!(default_id);
    let (history, omitted) = cloud_migration_history(&*runtime.migrations.lock().await)?;
    library["migrations"] = history;
    library["migration_omitted_count"] = json!(omitted);
    Ok(library)
}

pub(super) async fn dispatch(
    runtime: &CloudBotRuntime,
    request: &CloudRequest,
) -> Option<Result<CloudDispatchOutcome>> {
    if request.kind == "download_submit" {
        return Some(submit_download(runtime, request).await);
    }
    if !matches!(
        request.kind.as_str(),
        "library_register"
            | "library_set_default"
            | "library_set_category"
            | "library_rename"
            | "library_remove"
    ) {
        return None;
    }
    let queue = Arc::clone(&runtime.bot.queue);
    let kind = request.kind.clone();
    let payload = request.payload.clone();
    let migration_runtime = runtime.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<Value> {
        match kind.as_str() {
            "library_register" => {
                let payload: RegisterLibrary = serde_json::from_value(payload)?;
                let category = category_path(payload.default_category.as_deref())?;
                queue.register_video_library(
                    &payload.name,
                    &payload.path,
                    category.as_deref(),
                    payload.set_default,
                    payload.create_directory,
                )?;
            }
            "library_set_default" => {
                let payload: LibraryId = serde_json::from_value(payload)?;
                queue.set_default_video_library(&payload.library_id)?;
            }
            "library_set_category" => {
                let payload: LibraryCategory = serde_json::from_value(payload)?;
                let category = category_path(payload.default_category.as_deref())?;
                queue.set_video_library_category(&payload.library_id, category.as_deref())?;
            }
            "library_rename" => {
                let payload: LibraryName = serde_json::from_value(payload)?;
                queue.rename_video_library(&payload.library_id, &payload.name)?;
            }
            "library_remove" => {
                let payload: LibraryId = serde_json::from_value(payload)?;
                library_migration_bot::ensure_library_removable(
                    &migration_runtime,
                    &payload.library_id,
                )?;
                queue.remove_video_library(&payload.library_id)?;
            }
            _ => unreachable!("only supported settings actions reach this worker"),
        }
        Ok(json!({"applied": applied_settings(&queue)?}))
    })
    .await
    .context("media-library settings worker failed")
    .and_then(|result| result);
    Some(match result {
        Ok(result) => {
            runtime.state_notify.notify_one();
            runtime.bot.queue.publication_notify().notify_one();
            Ok(CloudDispatchOutcome {
                status: InboxStatus::Completed,
                result: Some(result),
                error: None,
                related_preview: None,
            })
        }
        Err(error) => Ok(cloud_dispatch_failure(format!("{error:#}"))),
    })
}

async fn submit_download(
    runtime: &CloudBotRuntime,
    request: &CloudRequest,
) -> Result<CloudDispatchOutcome> {
    let payload: DownloadSubmission = match serde_json::from_value(request.payload.clone()) {
        Ok(payload) => payload,
        Err(error) => return Ok(cloud_dispatch_failure(format!("invalid download: {error}"))),
    };
    let Some(chat_id) = configured_cloud_management_chat_id(&runtime.bot.config) else {
        return Ok(cloud_dispatch_failure(
            "a single private bot owner is required",
        ));
    };
    let RouteResult::Jobs(mut jobs) = route_message(&payload.url, &[]) else {
        return Ok(cloud_dispatch_failure(
            "enter a supported video or collection link",
        ));
    };
    if jobs.len() != 1 || matches!(jobs[0], JobRequest::Pdf { .. }) {
        return Ok(cloud_dispatch_failure("enter one video or collection link"));
    }
    let job = jobs.remove(0);
    let queue = Arc::clone(&runtime.bot.queue);
    let epoch = runtime.client.database_epoch()?.to_string();
    let task_id = format!("c{epoch}-{}", request.seq);
    let create_task_id = task_id.clone();
    let source_update = i64::try_from(request.seq)?;
    let original_job = job.clone();
    let created = tokio::task::spawn_blocking(move || -> Result<CreatedDownload> {
        if let Some(existing) = queue.get(&create_task_id)? {
            let destination = queue
                .task_destination(&create_task_id)?
                .context("the existing download has no saved media destination")?;
            let save_path = destination.category.as_ref().map_or_else(
                || destination.root.clone(),
                |category| destination.root.join(category),
            );
            return Ok(CreatedDownload {
                created: false,
                save_path,
                generation: existing.generation,
                library_id: destination.library_id,
                category: destination
                    .category
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned())
                    .unwrap_or_default(),
            });
        }
        let destination = queue.video_destination(
            &payload.library_id,
            payload.category.as_deref().map(Path::new),
        )?;
        let save_path = destination.category.as_ref().map_or_else(
            || destination.root.clone(),
            |category| destination.root.join(category),
        );
        let category = destination
            .category
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default();
        let library_id = destination.library_id.clone();
        let (stored_job, sanitized) = sanitize_job_for_storage(original_job);
        let mut task = TaskRecord::new(
            create_task_id,
            source_update,
            0,
            chat_id,
            Some(chat_id),
            0,
            stored_job,
        );
        task.destination = Some(destination);
        task.url_was_sanitized = sanitized;
        let generation = task.generation;
        Ok(CreatedDownload {
            created: queue.create(task)?,
            save_path,
            generation,
            library_id,
            category,
        })
    })
    .await
    .context("download submission worker failed")?;
    let created = match created {
        Ok(result) => result,
        Err(error) => return Ok(cloud_dispatch_failure(format!("{error:#}"))),
    };
    if created.created {
        let job_id = runtime.bot.next_job_id.fetch_add(1, Ordering::Relaxed);
        queue_or_prompt_job(
            runtime.bot.clone(),
            chat_id,
            job_id,
            task_id.clone(),
            created.generation,
            job,
        );
    }
    runtime.state_notify.notify_one();
    Ok(CloudDispatchOutcome {
        status: InboxStatus::Completed,
        result: Some(json!({
            "task_ids": [task_id],
            "library_id": created.library_id,
            "category": created.category,
            "save_path": created.save_path.to_string_lossy(),
        })),
        error: None,
        related_preview: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_migration_history_is_bounded_and_prioritizes_recovery() {
        let make_status = |id: &str, status: &str, created_at: u64| {
            serde_json::from_value::<crate::library_migration::MigrationStatus>(json!({
                "id":id, "preview_id":id, "revision":"1", "status":status,
                "created_at":created_at,
                "source_library_id":"source", "target_library_id":"target",
                "scope":"library", "conflict":"skip", "item_conflicts":{},
                "include_incomplete_tasks":false, "set_target_default_on_success":false,
                "default_update_applied":false, "total_items":0, "completed_items":0,
                "success_count":0, "skipped_count":0, "failed_count":0,
                "total_files":0, "completed_files":0, "copied_bytes":0, "total_bytes":0,
                "required_space_bytes":0, "available_space_bytes":0,
                "incomplete_task_count":0, "staging_bytes":0,
                "awaiting_task_plans":false, "awaiting_task_commit":false,
                "warnings":[], "items":[], "task_transfers":[],
            }))
            .unwrap()
        };
        let mut migrations = BTreeMap::new();
        for index in 0..40 {
            let id = format!("completed-{index:02}");
            migrations.insert(id.clone(), make_status(&id, "completed", index));
        }
        for (id, status) in [
            ("running", "running"),
            ("paused", "paused"),
            ("failed", "failed"),
        ] {
            migrations.insert(id.into(), make_status(id, status, 0));
        }
        let (view, omitted) = cloud_migration_history(&migrations).unwrap();
        let rows = view.as_array().unwrap();
        assert_eq!(rows.len(), CLOUD_MIGRATION_HISTORY_LIMIT);
        assert_eq!(omitted, 11);
        assert_eq!(rows[0]["id"], "running");
        assert!(rows.iter().any(|row| row["id"] == "paused"));
        assert!(rows.iter().any(|row| row["id"] == "failed"));
        assert!(rows.iter().any(|row| row["id"] == "completed-39"));
        assert!(!rows.iter().any(|row| row["id"] == "completed-00"));
        assert_eq!(
            migrations.len(),
            43,
            "projection must retain durable/runtime history"
        );
    }

    #[test]
    fn nested_library_snapshots_shrink_without_hiding_omission() {
        let items = (0..16)
            .map(|index| {
                json!({
                    "id": format!("item-{index}"), "title": "x".repeat(2_048),
                })
            })
            .collect::<Vec<_>>();
        let source = json!({
            "revision": "source", "scanned_at": 123, "items": items,
            "categories": [], "warnings": [], "ready": true,
        });
        let mut library = source.clone();
        library["default_library_id"] = json!("primary");
        library["libraries"] = json!([
            {"id":"primary", "media_count":16, "snapshot":source},
            {"id":"archive", "media_count":16, "snapshot":source},
        ]);
        let mut snapshot = CloudStateSnapshot {
            revision: "source".to_string(),
            library,
            previews: Vec::new(),
            operations: Vec::new(),
            tasks: Vec::new(),
            download_dir: "/tmp/media".to_string(),
            settings_applied: json!({}),
        };
        let budget = snapshot.encoded_size(1).unwrap() / 4;
        assert!(snapshot.trim_library_for_upload(1, budget).unwrap());
        assert!(snapshot.encoded_size(1).unwrap() <= budget);
        for view in snapshot.library["libraries"].as_array().unwrap() {
            assert_eq!(view["media_count"], 16);
            assert_eq!(view["snapshot"]["scanned_at"], 123);
            let omitted = cloud_library_omitted_item_count(&view["snapshot"]);
            assert!(omitted > 0);
            assert_eq!(
                view["snapshot"]["items"].as_array().unwrap().len() + omitted,
                16
            );
        }
        assert_eq!(
            snapshot.library["items"],
            snapshot.library["libraries"][0]["snapshot"]["items"]
        );
    }

    #[test]
    fn nested_metadata_compaction_preserves_media_and_marks_both_views_incomplete() {
        let items = json!([{"id":"one"}, {"id":"two"}, {"id":"three"}]);
        let source = json!({
            "revision":"metadata", "scanned_at":123, "items":items,
            "categories":(0..96).map(|i| format!("{i}-{}", "c".repeat(499))).collect::<Vec<_>>(),
            "warnings":(0..96).map(|i| format!("{i}-{}", "w".repeat(16 * 1024))).collect::<Vec<_>>(),
            "ready":true,
        });
        let mut library = source.clone();
        library["default_library_id"] = json!("primary");
        library["libraries"] = json!([{ "id":"primary", "media_count":3, "snapshot":source }]);
        let mut snapshot = CloudStateSnapshot {
            revision: "metadata".to_string(),
            library,
            previews: Vec::new(),
            operations: Vec::new(),
            tasks: Vec::new(),
            download_dir: "/tmp/media".to_string(),
            settings_applied: json!({}),
        };
        let budget = 256 * 1024;
        assert!(snapshot.trim_library_for_upload(1, budget).unwrap());
        assert!(snapshot.encoded_size(1).unwrap() <= budget);
        for view in [
            &snapshot.library,
            &snapshot.library["libraries"][0]["snapshot"],
        ] {
            assert_eq!(view["items"], items);
            assert_eq!(view["incomplete"], true);
            assert_eq!(cloud_library_omitted_item_count(view), 0);
            assert!(
                serde_json::to_vec(&view["warnings"]).unwrap().len()
                    <= CLOUD_LIBRARY_WARNING_JSON_LIMIT_BYTES
            );
        }
    }
}
