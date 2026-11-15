//! Bounded, owner-scoped JSON bridge for the torrent Mini App.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::torrent_manager::TorrentMiniAppConfirm;
use crate::torrent_runtime::TorrentService;
use crate::torrent_types::{TorrentFileSnapshot, TorrentFolder, TorrentSnapshot};

const PAGE_SIZE: usize = 50;
const MAX_PAGE: u32 = 20_000;
const MAX_FILES: usize = 5_000;
const MAX_RESULT_BYTES: usize = 192 * 1024;
const MAX_DISPLAY_BYTES: usize = 512;
const MAX_ERROR_BYTES: usize = 1_024;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum MiniAppAction {
    Catalog {
        #[serde(default)]
        page: u32,
        #[serde(default)]
        query: String,
    },
    Detail {
        task_id: Option<String>,
        hash: Option<String>,
        #[serde(default)]
        page: u32,
        #[serde(default)]
        folder_id: u32,
        #[serde(default)]
        query: String,
        #[serde(default = "default_filter")]
        filter: String,
    },
    Review {
        task_id: String,
        inventory_revision: u64,
        selection_revision: u64,
        selected_indexes: Vec<u32>,
    },
    Confirm {
        task_id: String,
        review_id: String,
        inventory_revision: u64,
        selection_revision: u64,
        selected_indexes: Vec<u32>,
    },
    Control {
        hash: String,
        control: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FileFilter {
    Music,
    All,
    Ready,
    Waiting,
    Imported,
}

impl FileFilter {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "music" => Ok(Self::Music),
            "all" => Ok(Self::All),
            "ready" => Ok(Self::Ready),
            "waiting" => Ok(Self::Waiting),
            "imported" => Ok(Self::Imported),
            _ => bail!("torrent detail filter is invalid"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Music => "music",
            Self::All => "all",
            Self::Ready => "ready",
            Self::Waiting => "waiting",
            Self::Imported => "imported",
        }
    }

    fn matches(self, file: &TorrentFileSnapshot) -> bool {
        match self {
            Self::Music => file.supported_music,
            Self::All => true,
            Self::Ready => file.supported_music && file.complete,
            Self::Waiting => file.supported_music && !file.complete,
            Self::Imported => {
                file.supported_music
                    && file.music_stage.as_ref().is_some_and(|stage| {
                        matches!(stage, crate::torrent_types::TorrentMusicStage::Complete)
                    })
            }
        }
    }
}

fn default_filter() -> String {
    "music".to_string()
}

pub(crate) async fn handle_action(
    service: &TorrentService,
    owner_chat: i64,
    request_identity: &str,
    payload: &Value,
) -> Result<Value> {
    ensure!(owner_chat > 0, "Mini App owner is invalid");
    validate_request_identity(request_identity)?;
    let action: MiniAppAction =
        serde_json::from_value(payload.clone()).context("torrent action payload is invalid")?;
    match action {
        MiniAppAction::Catalog { page, query } => {
            validate_page(page)?;
            validate_query(&query)?;
            catalog(service, page, &query).await
        }
        MiniAppAction::Detail {
            task_id,
            hash,
            page,
            folder_id,
            query,
            filter,
        } => {
            validate_page(page)?;
            ensure!(folder_id <= 10_000, "torrent folder is invalid");
            validate_query(&query)?;
            let filter = FileFilter::parse(&filter)?;
            ensure!(
                task_id.is_some() ^ hash.is_some(),
                "provide exactly one torrent task_id or hash"
            );
            let snapshot = if let Some(task_id) = task_id {
                validate_opaque_id(&task_id, "torrent task identifier")?;
                service.miniapp_detail(owner_chat, task_id).await?
            } else {
                let hash = hash.expect("validated hash selector");
                validate_hash(&hash)?;
                service.miniapp_adopt(owner_chat, hash).await?
            };
            detail(snapshot, page, folder_id, &query, filter)
        }
        MiniAppAction::Review {
            task_id,
            inventory_revision,
            selection_revision,
            selected_indexes,
        } => {
            validate_opaque_id(&task_id, "torrent task identifier")?;
            validate_revision(inventory_revision)?;
            validate_revision(selection_revision)?;
            validate_indexes(&selected_indexes)?;
            let context = service
                .miniapp_review_context(
                    owner_chat,
                    task_id.clone(),
                    inventory_revision,
                    selection_revision,
                    selected_indexes.clone(),
                )
                .await?;
            review(
                context,
                &task_id,
                request_identity,
                inventory_revision,
                selection_revision,
                selected_indexes,
            )
        }
        MiniAppAction::Confirm {
            task_id,
            review_id,
            inventory_revision,
            selection_revision,
            selected_indexes,
        } => {
            validate_opaque_id(&task_id, "torrent task identifier")?;
            validate_opaque_id(&review_id, "torrent review identifier")?;
            validate_revision(inventory_revision)?;
            validate_revision(selection_revision)?;
            validate_indexes(&selected_indexes)?;
            let result = service
                .miniapp_confirm(
                    owner_chat,
                    TorrentMiniAppConfirm {
                        task_id: task_id.clone(),
                        inventory_revision,
                        selection_revision,
                        command_key: request_identity.to_string(),
                        review_id,
                        selected_indexes: selected_indexes.clone(),
                    },
                )
                .await?;
            let response = json!({
                "operation": "confirm",
                "task_id": task_id,
                "confirmed": true,
                "replayed": result.replayed,
                "selected_count": selected_indexes.len(),
            });
            check_result_budget(response)
        }
        MiniAppAction::Control { hash, control } => {
            validate_hash(&hash)?;
            let active = match control.as_str() {
                "pause" => false,
                "resume" => true,
                _ => bail!("torrent control is invalid"),
            };
            service.miniapp_control(hash.clone(), active).await?;
            check_result_budget(json!({
                "operation": "control",
                "hash": hash,
                "control": control,
                "applied": true,
            }))
        }
    }
}

async fn catalog(service: &TorrentService, page: u32, query: &str) -> Result<Value> {
    let mut all = Vec::new();
    let mut offset = 0u32;
    loop {
        let rows = service.list_torrents(offset, PAGE_SIZE as u32).await?;
        let count = rows.len();
        all.extend(rows);
        if count < PAGE_SIZE || offset as usize + count >= 20_000 {
            break;
        }
        offset = offset
            .checked_add(count as u32)
            .context("qBittorrent catalog offset overflowed")?;
    }
    let needle = query.to_lowercase();
    let rows = all
        .iter()
        .filter(|task| {
            needle.is_empty()
                || task.name.to_lowercase().contains(&needle)
                || task.state.to_lowercase().contains(&needle)
                || task.hash.to_lowercase().contains(&needle)
        })
        .collect::<Vec<_>>();
    let total = rows.len();
    let start = (page as usize)
        .checked_mul(PAGE_SIZE)
        .context("catalog page is out of range")?;
    let end = start.saturating_add(PAGE_SIZE).min(total);
    let display_rows = rows
        .get(start..end)
        .unwrap_or(&[])
        .iter()
        .map(|task| {
            json!({
                "hash": task.hash.to_ascii_lowercase(),
                "name": safe_display_name(&task.name),
                "state": display_text(&task.state, MAX_DISPLAY_BYTES),
                "progress": finite_progress(task.progress),
                "size": task.total_size,
            })
        })
        .collect::<Vec<_>>();
    check_result_budget(json!({
        "operation": "catalog",
        "page": page,
        "page_size": PAGE_SIZE,
        "total": total,
        "has_more": end < total,
        "query": query,
        "rows": display_rows,
    }))
}

pub(super) fn detail(
    snapshot: TorrentSnapshot,
    page: u32,
    folder_id: u32,
    query: &str,
    filter: FileFilter,
) -> Result<Value> {
    ensure!(
        !snapshot.files.is_empty(),
        "torrent inventory is not ready; retry detail explicitly"
    );
    let folders_by_id = snapshot
        .folders
        .iter()
        .map(|folder| (folder.id, folder))
        .collect::<HashMap<_, _>>();
    let current_path = if folder_id == 0 {
        PathBuf::new()
    } else {
        folders_by_id
            .get(&folder_id)
            .map(|folder| folder.relative_path.clone())
            .ok_or_else(|| anyhow::anyhow!("torrent folder is invalid"))?
    };
    let needle = query.to_lowercase();
    let matched_files = snapshot
        .files
        .iter()
        .filter(|file| filter.matches(file))
        .filter(|file| path_is_within(&file.relative_path, &current_path))
        .filter(|file| {
            needle.is_empty()
                || file
                    .relative_path
                    .to_string_lossy()
                    .to_lowercase()
                    .contains(&needle)
        })
        .collect::<Vec<_>>();
    let scope_indexes = matched_files
        .iter()
        .filter(|file| file.supported_music)
        .map(|file| file.index)
        .collect::<Vec<_>>();
    let mut entries = if needle.is_empty() {
        direct_entries(&snapshot.folders, &matched_files, &current_path)?
    } else {
        search_entries(&snapshot.folders, &matched_files, &current_path)?
    };
    entries.sort_by(|left, right| {
        left["path"]
            .as_str()
            .cmp(&right["path"].as_str())
            .then_with(|| left["kind"].as_str().cmp(&right["kind"].as_str()))
    });
    let total = entries.len();
    let start = (page as usize)
        .checked_mul(PAGE_SIZE)
        .context("torrent detail page is out of range")?;
    let end = start.saturating_add(PAGE_SIZE).min(total);
    let page_entries = entries.get(start..end).unwrap_or(&[]).to_vec();
    let task = json!({
        "task_id": snapshot.task_id,
        "name": safe_display_name(&snapshot.name),
        "phase": snapshot.phase,
        "state": snapshot.state.as_deref().map(|state| display_text(state, MAX_DISPLAY_BYTES)),
        "progress": finite_progress(snapshot.progress),
        "inventory_revision": snapshot.inventory_revision,
        "selection_revision": snapshot.selection_revision,
        "selected_indexes": snapshot.files.iter().filter(|file| file.selected && file.supported_music).map(|file| file.index).collect::<Vec<_>>(),
        "selected_count": snapshot.selected_count,
        "import_count": snapshot.import_count,
        "error": safe_task_error(snapshot.error.as_deref()),
    });
    let response = json!({
        "operation": "detail",
        "task": task,
        "page": page,
        "page_size": PAGE_SIZE,
        "total": total,
        "has_more": end < total,
        "folder_id": folder_id,
        "query": query,
        "filter": filter.name(),
        "scope_indexes": scope_indexes,
        "entries": page_entries,
    });
    check_result_budget(response)
}

fn direct_entries(
    folders: &[TorrentFolder],
    files: &[&TorrentFileSnapshot],
    current_path: &Path,
) -> Result<Vec<Value>> {
    let mut entries = Vec::new();
    for folder in folders {
        if !folder.relative_path.as_os_str().is_empty()
            && folder.relative_path.parent() == Some(current_path)
        {
            let descendants = files
                .iter()
                .filter(|file| path_is_within(&file.relative_path, &folder.relative_path))
                .collect::<Vec<_>>();
            if descendants.is_empty() {
                continue;
            }
            let indexes = descendants
                .iter()
                .filter(|file| file.supported_music)
                .map(|file| file.index)
                .collect::<Vec<_>>();
            entries.push(json!({
                "kind": "folder",
                "id": folder.id,
                "path": safe_relative_path(&folder.relative_path)?,
                "indexes": indexes,
            }));
        }
    }
    for file in files {
        if file.relative_path.parent().unwrap_or(Path::new("")) == current_path {
            entries.push(file_entry(file)?);
        }
    }
    Ok(entries)
}

fn search_entries(
    folders: &[TorrentFolder],
    files: &[&TorrentFileSnapshot],
    current_path: &Path,
) -> Result<Vec<Value>> {
    let folders_by_path = folders
        .iter()
        .map(|folder| (folder.relative_path.clone(), folder))
        .collect::<HashMap<_, _>>();
    let mut matching_paths = BTreeSet::<PathBuf>::new();
    let mut indexes_by_folder = HashMap::<PathBuf, Vec<u32>>::new();
    for file in files {
        let mut ancestor = file.relative_path.parent();
        while let Some(path) = ancestor {
            if path.as_os_str().is_empty()
                || path == current_path
                || !path_is_within(path, current_path)
            {
                break;
            }
            if folders_by_path.contains_key(path) {
                matching_paths.insert(path.to_path_buf());
                if file.supported_music && path.parent().unwrap_or(Path::new("")) == current_path {
                    indexes_by_folder
                        .entry(path.to_path_buf())
                        .or_default()
                        .push(file.index);
                }
            }
            ancestor = path.parent();
        }
    }
    let mut entries = Vec::new();
    for folder in folders {
        if folder.relative_path.parent().unwrap_or(Path::new("")) != current_path
            || !matching_paths.contains(&folder.relative_path)
        {
            continue;
        }
        let indexes = indexes_by_folder
            .get(&folder.relative_path)
            .cloned()
            .unwrap_or_default();
        entries.push(json!({
            "kind": "folder",
            "id": folder.id,
            "path": safe_relative_path(&folder.relative_path)?,
            "indexes": indexes,
        }));
    }
    for file in files {
        entries.push(file_entry(file)?);
    }
    Ok(entries)
}

fn file_entry(file: &TorrentFileSnapshot) -> Result<Value> {
    Ok(json!({
        "kind": "file",
        "index": file.index,
        "path": safe_relative_path(&file.relative_path)?,
        "size": file.size,
        "progress": finite_progress(file.progress),
        "complete": file.complete,
        "selected": file.selected,
        "supported_music": file.supported_music,
        "needs_reconfirm": file.needs_reconfirm,
        "music_stage": file.music_stage,
    }))
}

fn review(
    context: crate::torrent_manager::TorrentMiniAppReviewContext,
    task_id: &str,
    request_identity: &str,
    inventory_revision: u64,
    selection_revision: u64,
    mut selected_indexes: Vec<u32>,
) -> Result<Value> {
    selected_indexes.sort_unstable();
    let selected = selected_indexes.iter().copied().collect::<BTreeSet<_>>();
    let files = context
        .snapshot
        .files
        .iter()
        .filter(|file| selected.contains(&file.index))
        .collect::<Vec<_>>();
    let total_bytes = files.iter().try_fold(0u64, |sum, file| {
        sum.checked_add(file.size)
            .context("selected torrent byte total overflowed")
    })?;
    let ready_count = files.iter().filter(|file| file.complete).count();
    let waiting_count = files.len() - ready_count;
    let review_id = make_review_id(
        request_identity,
        task_id,
        inventory_revision,
        selection_revision,
        &selected_indexes,
    );
    let entries = files
        .iter()
        .take(PAGE_SIZE)
        .map(|file| file_entry(file))
        .collect::<Result<Vec<_>>>()?;
    check_result_budget(json!({
        "operation": "review",
        "review_id": review_id,
        "task_id": task_id,
        "inventory_revision": inventory_revision,
        "selection_revision": selection_revision,
        "selected_indexes": selected_indexes,
        "selected_count": files.len(),
        "total_bytes": total_bytes,
        "ready_count": ready_count,
        "waiting_count": waiting_count,
        "existing_import_count": context.existing_import_count,
        "removed_pending_count": context.removed_pending_count,
        "entries": entries,
        "more_count": files.len().saturating_sub(PAGE_SIZE),
    }))
}

fn make_review_id(
    request_identity: &str,
    task_id: &str,
    inventory_revision: u64,
    selection_revision: u64,
    indexes: &[u32],
) -> String {
    let mut digest = Sha256::new();
    digest.update(request_identity.as_bytes());
    digest.update([0]);
    digest.update(task_id.as_bytes());
    digest.update([0]);
    digest.update(inventory_revision.to_be_bytes());
    digest.update(selection_revision.to_be_bytes());
    for index in indexes {
        digest.update(index.to_be_bytes());
    }
    let mut output = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

fn path_is_within(path: &Path, parent: &Path) -> bool {
    parent.as_os_str().is_empty() || path == parent || path.starts_with(parent)
}

fn safe_relative_path(path: &Path) -> Result<String> {
    ensure!(
        !path.is_absolute()
            && path
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "torrent display path is invalid"
    );
    let value = path.to_string_lossy();
    ensure!(
        !value.chars().any(char::is_control),
        "torrent display path contains control characters"
    );
    Ok(display_text(&value, MAX_DISPLAY_BYTES))
}

fn display_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

fn safe_task_error(error: Option<&str>) -> Option<String> {
    error.map(|value| {
        let value = if value.contains('/')
            || value.contains('\\')
            || value.contains("save_path")
            || value.contains("content_path")
        {
            "Torrent task needs attention."
        } else {
            value
        };
        display_text(value, MAX_ERROR_BYTES)
    })
}

fn safe_display_name(value: &str) -> String {
    if value.chars().any(char::is_control) || value.contains('/') || value.contains('\\') {
        "Torrent task".to_string()
    } else {
        display_text(value, MAX_DISPLAY_BYTES)
    }
}

fn finite_progress(progress: f64) -> f64 {
    if progress.is_finite() {
        progress.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn validate_page(page: u32) -> Result<()> {
    ensure!(page <= MAX_PAGE, "torrent page is out of range");
    Ok(())
}

fn validate_query(query: &str) -> Result<()> {
    ensure!(query.len() <= 256, "torrent query is too long");
    ensure!(
        !query.chars().any(char::is_control),
        "torrent query contains control characters"
    );
    Ok(())
}

fn validate_revision(revision: u64) -> Result<()> {
    ensure!(
        revision > 0 && revision <= MAX_SAFE_INTEGER,
        "torrent revision is invalid"
    );
    Ok(())
}

fn validate_indexes(indexes: &[u32]) -> Result<()> {
    ensure!(indexes.len() <= MAX_FILES, "too many torrent file indexes");
    let mut unique = BTreeSet::new();
    for index in indexes {
        ensure!(
            *index < MAX_FILES as u32,
            "torrent file index is out of range"
        );
        ensure!(
            unique.insert(*index),
            "torrent selection contains duplicate indexes"
        );
    }
    Ok(())
}

fn validate_opaque_id(value: &str, label: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "{label} is invalid"
    );
    Ok(())
}

fn validate_hash(value: &str) -> Result<()> {
    ensure!(
        matches!(value.len(), 40 | 64)
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "qBittorrent hash must be lowercase hexadecimal"
    );
    Ok(())
}

fn validate_request_identity(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| { byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':') }),
        "torrent action request identity is invalid"
    );
    Ok(())
}

fn serialized_size(value: &Value) -> Result<usize> {
    Ok(serde_json::to_vec(value)?.len())
}

fn check_result_budget(value: Value) -> Result<Value> {
    ensure!(
        serialized_size(&value)? <= MAX_RESULT_BYTES,
        "torrent action result exceeds its size limit"
    );
    Ok(value)
}
