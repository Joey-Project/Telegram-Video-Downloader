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

    let priority = |status: &crate::library_migration::MigrationStatus| {
        if status.terminal_finalization_pending {
            return 0;
        }
        match status.status {
            MigrationRunStatus::Queued | MigrationRunStatus::Running => 0,
            MigrationRunStatus::Paused
            | MigrationRunStatus::Failed
            | MigrationRunStatus::Partial => 1,
            MigrationRunStatus::Completed | MigrationRunStatus::Stopped => 2,
        }
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
    root_binding: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LibraryCategory {
    library_id: String,
    root_binding: String,
    #[serde(default)]
    default_category: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LibraryName {
    library_id: String,
    root_binding: String,
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DownloadSubmission {
    url: String,
    library_id: String,
    #[serde(default)]
    root_binding: Option<String>,
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
            let mut descriptor = json!({
                "id": library.id,
                "name": library.name,
                "root": library.root.to_string_lossy(),
                "default_category": library.default_category.as_ref()
                    .map(|path| path.to_string_lossy().into_owned()).unwrap_or_default(),
                "is_default": library.id == snapshot.default_library_id,
            });
            if let Some(root_binding) = library.root_binding_token() {
                descriptor["root_binding"] = json!(root_binding);
            }
            descriptor
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
) -> (HashSet<u64>, bool) {
    let mut file_previews =
        BTreeMap::<(Option<String>, Option<String>), Vec<(String, String, u64)>>::new();
    let mut migration_previews = BTreeMap::<String, Vec<(String, String, u64)>>::new();
    let mut unconfirmable_sequences = HashSet::new();
    let mut failed = false;
    for record in records {
        if record.status != InboxStatus::AwaitingConfirmation {
            continue;
        }
        match record.request.kind.as_str() {
            "file_preview" => {
                if let Some((preview_id, revision)) = cloud_preview_identity(record) {
                    let request_library_id = record
                        .request
                        .payload
                        .get("library_id")
                        .and_then(Value::as_str);
                    let result_library_id = record
                        .result
                        .as_ref()
                        .and_then(|result| result.get("library_id"))
                        .and_then(Value::as_str);
                    let request_root_binding = record
                        .request
                        .payload
                        .get("root_binding")
                        .and_then(Value::as_str);
                    let result_root_binding = record
                        .result
                        .as_ref()
                        .and_then(|result| result.get("root_binding"))
                        .and_then(Value::as_str);
                    let library_id = request_library_id
                        .zip(result_library_id)
                        .filter(|(request, result)| request == result)
                        .map(|(library_id, _)| library_id.to_string());
                    let root_binding = request_root_binding
                        .zip(result_root_binding)
                        .filter(|(request, result)| request == result)
                        .map(|(binding, _)| binding.to_string());
                    file_previews
                        .entry((library_id, root_binding))
                        .or_default()
                        .push((preview_id, revision, record.request.seq));
                } else {
                    unconfirmable_sequences.insert(record.request.seq);
                }
            }
            "library_migration_preview" => {
                let result = record.result.as_ref();
                let preview_id = result
                    .and_then(|result| result["id"].as_str())
                    .map(str::to_string);
                let revision = result
                    .and_then(|result| result["revision"].as_str())
                    .map(str::to_string);
                let library_id = result
                    .and_then(|result| result["source_library_id"].as_str())
                    .or_else(|| record.request.payload["source_library_id"].as_str())
                    .map(str::to_string);
                if let (Some(preview_id), Some(revision), Some(library_id)) =
                    (preview_id, revision, library_id)
                {
                    migration_previews.entry(library_id).or_default().push((
                        preview_id,
                        revision,
                        record.request.seq,
                    ));
                } else {
                    failed = true;
                    warn!(
                        seq = record.request.seq,
                        "could not read the source identity of an awaiting migration preview"
                    );
                }
            }
            _ => {}
        }
    }

    for ((library_id, root_binding), entries) in file_previews {
        let (Some(library_id), Some(root_binding)) = (library_id, root_binding) else {
            unconfirmable_sequences.extend(entries.iter().map(|(_, _, seq)| *seq));
            continue;
        };
        match runtime.bot.queue.registry_snapshot() {
            Ok(registry) => {
                let matches_registered_binding = registry
                    .libraries
                    .iter()
                    .find(|library| library.id == library_id)
                    .and_then(|library| library.root_binding_token())
                    .is_some_and(|registered| registered == root_binding);
                if !matches_registered_binding {
                    unconfirmable_sequences.extend(entries.iter().map(|(_, _, seq)| *seq));
                    continue;
                }
            }
            Err(error) => {
                failed = true;
                warn!(error = %redact_sensitive_text(&format!("{error:#}")), "could not read the registry while verifying file preview bindings");
                continue;
            }
        }
        let keys = entries
            .iter()
            .map(|(preview_id, revision, _)| (preview_id.clone(), revision.clone()))
            .collect::<Vec<_>>();
        match cloud_unconfirmable_file_preview_ids(runtime, &library_id, &root_binding, &keys).await
        {
            Ok(ids) => {
                for (preview_id, _, seq) in entries {
                    if ids.contains(&preview_id) {
                        unconfirmable_sequences.insert(seq);
                    }
                }
            }
            Err(error) => {
                failed = true;
                warn!(error = %redact_sensitive_text(&format!("{error:#}")), "could not verify library preview anchors");
            }
        }
    }

    for (library_id, entries) in migration_previews {
        let keys = entries
            .iter()
            .map(|(preview_id, revision, _)| (preview_id.clone(), revision.clone()))
            .collect::<Vec<_>>();
        match cloud_migration_preview_confirmability(runtime, &library_id, &keys).await {
            Ok(states) => {
                for ((_, _, seq), state) in entries.into_iter().zip(states) {
                    match state {
                        crate::library_migration::MigrationPreviewConfirmability::Confirmable => {}
                        crate::library_migration::MigrationPreviewConfirmability::DefinitelyUnconfirmable => {
                            unconfirmable_sequences.insert(seq);
                        }
                        crate::library_migration::MigrationPreviewConfirmability::Unknown => {
                            failed = true;
                            warn!(
                                library_id = %library_id,
                                seq,
                                "could not verify whether an awaiting migration preview is still confirmable"
                            );
                        }
                    }
                }
            }
            Err(error) => {
                failed = true;
                warn!(
                    library_id = %library_id,
                    error = %redact_sensitive_text(&format!("{error:#}")),
                    "could not verify migration preview state"
                );
            }
        }
    }

    (unconfirmable_sequences, failed)
}

async fn cloud_migration_preview_confirmability(
    runtime: &CloudBotRuntime,
    library_id: &str,
    preview_keys: &[(String, String)],
) -> Result<Vec<crate::library_migration::MigrationPreviewConfirmability>> {
    let (scoped, _) = scoped_runtime(runtime, Some(library_id))?;
    let bot = scoped.bot;
    let inbox = Arc::clone(&runtime.inbox);
    let preview_keys = preview_keys.to_vec();
    #[cfg(test)]
    let file_provider = runtime.test_file_provider.clone();
    #[cfg(not(test))]
    let file_provider = None;
    tokio::task::spawn_blocking(move || {
        let manager = open_cloud_library_manager(&bot, inbox, file_provider)?;
        crate::library_migration::MigrationEngine::preview_confirmability_for_inbox(
            &manager,
            &preview_keys,
        )
    })
    .await
    .context("cloud migration preview confirmability worker failed")?
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
    let io_lease = if kind == "library_remove" {
        if let Some(library_id) = payload.get("library_id").and_then(Value::as_str) {
            match crate::library_migration_bot::try_acquire_library_io(
                runtime,
                &[library_id.to_string()],
            )
            .await
            {
                Ok(lease) => Some(lease),
                Err(error) => return Some(Err(error)),
            }
        } else {
            None
        }
    } else {
        None
    };
    // Serialize removal against every scan from manager open through cache publication. The
    // per-library admission lease also excludes migration discovery and runners. The
    // management-dispatch semaphore does not cover startup and publication-watcher scans.
    let publication_guard = if kind == "library_remove" {
        Some(
            Arc::clone(&runtime.library_scan_publication_gate)
                .lock_owned()
                .await,
        )
    } else {
        None
    };
    let blocking_io_lease = io_lease.as_ref().map(Arc::clone);
    let worker = tokio::task::spawn_blocking(move || {
        let _io_lease = blocking_io_lease;
        let mut removed_library_id = None;
        let mut removed_was_original_library = false;
        let mut default_library_after_remove = None;
        let result = (|| -> Result<Value> {
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
                    queue.set_default_video_library_with_root_binding(
                        &payload.library_id,
                        &payload.root_binding,
                    )?;
                }
                "library_set_category" => {
                    let payload: LibraryCategory = serde_json::from_value(payload)?;
                    let category = category_path(payload.default_category.as_deref())?;
                    queue.set_video_library_category_with_root_binding(
                        &payload.library_id,
                        category.as_deref(),
                        &payload.root_binding,
                    )?;
                }
                "library_rename" => {
                    let payload: LibraryName = serde_json::from_value(payload)?;
                    queue.rename_video_library_with_root_binding(
                        &payload.library_id,
                        &payload.name,
                        &payload.root_binding,
                    )?;
                }
                "library_remove" => {
                    let payload: LibraryId = serde_json::from_value(payload)?;
                    queue.require_video_library_root_binding(
                        &payload.library_id,
                        &payload.root_binding,
                    )?;
                    removed_was_original_library =
                        original_library_id_from_bot(&migration_runtime.bot)
                            .ok()
                            .as_deref()
                            == Some(payload.library_id.as_str());
                    library_migration_bot::ensure_library_removable(
                        &migration_runtime,
                        &payload.library_id,
                    )?;
                    queue.remove_video_library_with_root_binding(
                        &payload.library_id,
                        &payload.root_binding,
                    )?;
                    removed_library_id = Some(payload.library_id);
                    default_library_after_remove = queue
                        .registry_snapshot()
                        .ok()
                        .map(|snapshot| snapshot.default_library_id);
                }
                _ => unreachable!("only supported settings actions reach this worker"),
            }
            Ok(json!({"applied": applied_settings(&queue)?}))
        })();
        (
            (
                result,
                removed_library_id,
                removed_was_original_library,
                default_library_after_remove,
            ),
            publication_guard,
        )
    })
    .await
    .context("media-library settings worker failed");
    let (worker, _publication_guard) = match worker {
        Ok((worker, publication_guard)) => (Ok(worker), publication_guard),
        Err(error) => (Err(error), None),
    };
    let (result, removed_library_id, removed_was_original_library, default_library_after_remove) =
        match worker {
            Ok(worker_result) => worker_result,
            Err(error) => (Err(error), None, false, None),
        };
    let _io_lease = io_lease;
    if let Some(removed_library_id) = removed_library_id {
        let default_snapshot = {
            let mut snapshots = runtime.library_snapshots.lock().await;
            snapshots.remove(&removed_library_id);
            default_library_after_remove
                .as_deref()
                .and_then(|default_id| snapshots.get(default_id).cloned())
        };
        let default_warning = {
            let mut unavailable = runtime.library_unavailable.lock().await;
            unavailable.remove(&removed_library_id);
            default_library_after_remove
                .as_deref()
                .and_then(|default_id| unavailable.get(default_id).cloned())
        };
        // The unscoped legacy cache has no identity of its own. Do not mirror another root's
        // snapshot into the original-root fallback after that original registration is removed.
        *runtime.library_snapshot.lock().await = if removed_was_original_library {
            None
        } else {
            default_snapshot
        };
        *runtime.library_scan_warning.lock().await = default_warning;
    }
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
    let Some(root_binding) = payload.root_binding.as_deref() else {
        return Ok(cloud_dispatch_failure(
            "root_binding must be an opaque root identity token",
        ));
    };
    if let Err(error) = runtime
        .bot
        .queue
        .require_video_library_root_binding(&payload.library_id, root_binding)
    {
        return Ok(cloud_dispatch_failure(format!("{error:#}")));
    }
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
            create_task_id.clone(),
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
        let created = queue.create_video_task_with_root_binding(
            task,
            &payload.library_id,
            payload
                .root_binding
                .as_deref()
                .expect("root binding was checked before spawning the worker"),
        )?;
        if created {
            return Ok(CreatedDownload {
                created: true,
                save_path,
                generation,
                library_id,
                category,
            });
        }
        let existing = queue
            .get(&create_task_id)?
            .context("the existing download disappeared after duplicate detection")?;
        let destination = queue
            .task_destination(&create_task_id)?
            .context("the existing download has no saved media destination")?;
        let save_path = destination.category.as_ref().map_or_else(
            || destination.root.clone(),
            |category| destination.root.join(category),
        );
        Ok(CreatedDownload {
            created: false,
            save_path,
            generation: existing.generation,
            library_id: destination.library_id,
            category: destination
                .category
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned())
                .unwrap_or_default(),
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
    fn cloud_migration_history_keeps_pending_terminal_controls_ahead_of_inert_history() {
        let make_status = |id: &str, status: &str, created_at: u64, pending: bool| {
            serde_json::from_value::<crate::library_migration::MigrationStatus>(json!({
                "id":id, "preview_id":id, "revision":"1", "status":status,
                "created_at":created_at,
                "source_library_id":"source", "target_library_id":"target",
                "scope":"library", "conflict":"skip", "item_conflicts":{},
                "include_incomplete_tasks":false, "set_target_default_on_success":false,
                "default_update_applied":false, "terminal_finalization_pending":pending,
                "total_items":0, "completed_items":0,
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
            let id = format!("inert-{:02}", index);
            let status = if index % 2 == 0 {
                "completed"
            } else {
                "stopped"
            };
            migrations.insert(id.clone(), make_status(&id, status, 1_000 + index, false));
        }
        migrations.insert(
            "pending-completed".to_string(),
            make_status("pending-completed", "completed", 1, true),
        );
        migrations.insert(
            "pending-stopped".to_string(),
            make_status("pending-stopped", "stopped", 2, true),
        );

        let (projection, omitted) = cloud_migration_history(&migrations).unwrap();
        let rows = projection.as_array().unwrap();
        assert_eq!(rows.len(), CLOUD_MIGRATION_HISTORY_LIMIT);
        assert_eq!(omitted, 10);
        for id in ["pending-completed", "pending-stopped"] {
            let row = rows
                .iter()
                .find(|row| row["id"] == id)
                .expect("pending terminal migration should remain in the UI projection");
            assert_eq!(row["terminal_finalization_pending"], true);
        }
        assert!(
            rows.iter().any(|row| row["id"] == "inert-39"),
            "recent terminal history should remain available after recoverable rows"
        );
        assert!(
            rows.iter().all(|row| row["id"] != "inert-00"),
            "the projection should still enforce its 32-row cap"
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
