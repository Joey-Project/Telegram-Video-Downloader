//! Native inline-keyboard presentation for torrent file selection and Music import.

use std::path::{Component, Path};

use anyhow::{Context, Result, ensure};

use crate::telegram::{InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo};
use crate::torrent_types::{
    TorrentFileSnapshot, TorrentFolder, TorrentMusicStage, TorrentPhase, TorrentSnapshot,
};

const PAGE_SIZE: usize = 4;
const TELEGRAM_MESSAGE_LIMIT_UTF16: usize = 4_096;
const CALLBACK_DATA_LIMIT_BYTES: usize = 64;
const BUTTON_TEXT_LIMIT_UTF16: usize = 48;
const BODY_TITLE_LIMIT_UTF16: usize = 220;
const BODY_PATH_LIMIT_UTF16: usize = 700;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TorrentInlinePresentation {
    pub text: String,
    pub reply_markup: InlineKeyboardMarkup,
}

enum VisibleEntry<'a> {
    Folder(&'a TorrentFolder),
    File(&'a TorrentFileSnapshot),
}

#[derive(Default)]
struct ImportCounts {
    pending: usize,
    complete: usize,
    failed: usize,
    cancelled: usize,
    unknown: usize,
}

impl ImportCounts {
    fn from_snapshot(snapshot: &TorrentSnapshot) -> Self {
        let mut counts = Self::default();
        for file in &snapshot.files {
            match file.music_stage.as_ref() {
                Some(TorrentMusicStage::Pending) => counts.pending += 1,
                Some(TorrentMusicStage::Complete) => counts.complete += 1,
                Some(TorrentMusicStage::Failed) => counts.failed += 1,
                Some(TorrentMusicStage::Cancelled) => counts.cancelled += 1,
                Some(TorrentMusicStage::OutcomeUnknown) => counts.unknown += 1,
                None => {}
            }
        }
        counts
    }

    fn retryable(&self) -> usize {
        self.failed + self.cancelled
    }
}

#[cfg(test)]
pub fn render_torrent_inline(snapshot: &TorrentSnapshot) -> Result<TorrentInlinePresentation> {
    render_torrent_inline_with_miniapp(snapshot, None)
}

#[cfg(test)]
pub(crate) fn render_torrent_inline_with_miniapp(
    snapshot: &TorrentSnapshot,
    miniapp_worker_url: Option<&str>,
) -> Result<TorrentInlinePresentation> {
    render_torrent_inline_with_options(snapshot, miniapp_worker_url, true)
}

pub(crate) fn render_torrent_inline_with_options(
    snapshot: &TorrentSnapshot,
    miniapp_worker_url: Option<&str>,
    music_enabled: bool,
) -> Result<TorrentInlinePresentation> {
    ensure!(
        valid_task_id(&snapshot.task_id),
        "torrent task identifier is invalid"
    );

    if let Some(worker_url) = miniapp_worker_url {
        let app_url = miniapp_url(worker_url, Some(&snapshot.task_id))?;
        let downloaded = snapshot.files.iter().fold(0_u64, |total, file| {
            total.saturating_add((file.size as f64 * file.progress.clamp(0.0, 1.0)) as u64)
        });
        let total = snapshot
            .files
            .iter()
            .fold(0_u64, |total, file| total.saturating_add(file.size));
        let text = format!(
            "Torrent: {}\nStatus: {} · {} · {} / {}\n{}",
            bounded_text(&snapshot.name, BODY_TITLE_LIMIT_UTF16),
            phase_label(&snapshot.phase),
            progress_text(snapshot.progress),
            crate::human_bytes(downloaded),
            crate::human_bytes(total),
            music_status_line(snapshot, music_enabled),
        );
        ensure!(
            text.encode_utf16().count() <= TELEGRAM_MESSAGE_LIMIT_UTF16,
            "torrent summary exceeds Telegram's message limit"
        );
        let mut inline_keyboard = vec![vec![InlineKeyboardButton {
            text: "Manage torrent".to_string(),
            callback_data: String::new(),
            web_app: Some(WebAppInfo { url: app_url }),
        }]];
        if can_add_all_music(snapshot, music_enabled) {
            inline_keyboard.push(vec![button("添加所有音乐", callback(snapshot, "a", true)?)]);
        }
        return Ok(TorrentInlinePresentation {
            text,
            reply_markup: InlineKeyboardMarkup { inline_keyboard },
        });
    }

    let mut keyboard = Vec::new();
    let text = if snapshot.review_selection {
        render_review(snapshot, music_enabled, &mut keyboard)?
    } else {
        render_browser(snapshot, music_enabled, &mut keyboard)?
    };
    if can_add_all_music(snapshot, music_enabled) {
        keyboard.push(vec![button("添加所有音乐", callback(snapshot, "a", true)?)]);
    }
    ensure!(
        text.encode_utf16().count() <= TELEGRAM_MESSAGE_LIMIT_UTF16,
        "torrent card text exceeds Telegram's message limit"
    );

    Ok(TorrentInlinePresentation {
        text,
        reply_markup: InlineKeyboardMarkup {
            inline_keyboard: keyboard,
        },
    })
}

pub(crate) fn miniapp_url(worker_url: &str, task_id: Option<&str>) -> Result<String> {
    let mut worker = url::Url::parse(worker_url).context("cloud worker URL is invalid")?;
    if !worker.path().ends_with('/') {
        worker.set_path(&format!("{}/", worker.path()));
    }
    let mut app = worker
        .join("file/")
        .context("could not build the torrent Mini App URL")?;
    {
        let mut query = app.query_pairs_mut();
        query.append_pair("view", "torrents");
        if let Some(task_id) = task_id {
            query.append_pair("task_id", task_id);
        }
    }
    Ok(app.to_string())
}

fn render_browser(
    snapshot: &TorrentSnapshot,
    music_enabled: bool,
    keyboard: &mut Vec<Vec<InlineKeyboardButton>>,
) -> Result<String> {
    let current = folder_path(snapshot, snapshot.cursor_folder_id)?;
    let entries = visible_entries(snapshot, current);
    let page_count = entries.len().max(1).div_ceil(PAGE_SIZE);
    let page = usize::try_from(snapshot.cursor_page)
        .unwrap_or(usize::MAX)
        .min(page_count - 1);
    let start = page * PAGE_SIZE;
    let counts = ImportCounts::from_snapshot(snapshot);
    let selected_count = supported_selected_count(snapshot);

    let mut lines = vec![format!(
        "Torrent: {}",
        bounded_text(&snapshot.name, BODY_TITLE_LIMIT_UTF16)
    )];
    lines.push(format!(
        "Status: {} · {} · {} / {}",
        phase_label(&snapshot.phase),
        progress_text(snapshot.progress),
        snapshot.files.iter().fold(0_u64, |total, file| total
            .saturating_add((file.size as f64 * file.progress.clamp(0.0, 1.0)) as u64)),
        snapshot
            .files
            .iter()
            .fold(0_u64, |total, file| total.saturating_add(file.size)),
    ));
    lines.push(music_status_line(snapshot, music_enabled));
    if music_enabled && snapshot.inventory_verified {
        lines.push(format!(
            "{} selected · {} imported · {} waiting · {} failed/cancelled · {} unknown",
            selected_count,
            counts.complete,
            counts.pending,
            counts.retryable(),
            counts.unknown
        ));
    }
    let folder_label = if current.as_os_str().is_empty() {
        "All files".to_string()
    } else {
        bounded_text(
            &safe_relative_text(current).unwrap_or_else(|| "Folder".to_string()),
            160,
        )
    };
    lines.push(format!(
        "Folder: {folder_label} · page {}/{}",
        page + 1,
        page_count
    ));

    if entries.is_empty() {
        lines.push("No files are available yet. Use Refresh to check again.".to_string());
    } else {
        for entry in entries.iter().skip(start).take(PAGE_SIZE) {
            match entry {
                VisibleEntry::Folder(folder) => {
                    let (total, supported, selected) =
                        folder_counts(snapshot, &folder.relative_path);
                    let name = folder
                        .relative_path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy();
                    lines.push(format!(
                        "📁 {} · {total} files · {supported} music · {selected} selected",
                        bounded_text(&name, BODY_TITLE_LIMIT_UTF16)
                    ));

                    let folder_selected = supported > 0 && selected == supported;
                    let open = callback(snapshot, &format!("b:{}:0", folder.id), false)?;
                    let select = (supported > 0)
                        .then(|| {
                            callback(
                                snapshot,
                                &format!("d:{}:{}", folder.id, u8::from(!folder_selected)),
                                false,
                            )
                        })
                        .transpose()?;
                    let mut row = vec![button(
                        &format!("📂 Open {}", bounded_text(&name, 30)),
                        open,
                    )];
                    if let Some(select) = select {
                        row.push(button(
                            if folder_selected {
                                "☑ Selected"
                            } else {
                                "☐ Select"
                            },
                            select,
                        ));
                    }
                    keyboard.push(row);
                }
                VisibleEntry::File(file) => {
                    let name = file
                        .relative_path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy();
                    let suffix = if file.supported_music {
                        format!(
                            "{} · {}",
                            stage_label(file.music_stage.as_ref()),
                            format_size(file.size)
                        )
                    } else {
                        format!("Not music · {}", format_size(file.size))
                    };
                    lines.push(format!(
                        "{} {} · {} · {}",
                        if file.selected { "☑" } else { "☐" },
                        bounded_text(&name, BODY_TITLE_LIMIT_UTF16),
                        bounded_text(&suffix, 70),
                        progress_text(file.progress)
                    ));
                    if file.supported_music {
                        let action = format!("f:{}:{}", file.index, u8::from(!file.selected));
                        let data = callback(snapshot, &action, false)?;
                        keyboard.push(vec![button(
                            &format!(
                                "{} {}",
                                if file.selected { "☑" } else { "☐" },
                                bounded_text(&name, 38)
                            ),
                            data,
                        )]);
                    }
                }
            }
        }
    }

    add_browser_controls(
        snapshot,
        current,
        page,
        page_count,
        selected_count,
        music_enabled,
        keyboard,
    )?;
    Ok(lines.join("\n"))
}

fn render_review(
    snapshot: &TorrentSnapshot,
    music_enabled: bool,
    keyboard: &mut Vec<Vec<InlineKeyboardButton>>,
) -> Result<String> {
    let mut files = snapshot
        .files
        .iter()
        .filter(|file| file.supported_music && file.selected)
        .collect::<Vec<_>>();
    files.sort_by_key(|file| file.index);

    let page_count = files.len().max(1).div_ceil(PAGE_SIZE);
    let page = usize::try_from(snapshot.cursor_page)
        .unwrap_or(usize::MAX)
        .min(page_count - 1);
    let mut lines = vec![
        format!("Review {} selected music files", files.len()),
        format!(
            "Torrent: {}",
            bounded_text(&snapshot.name, BODY_TITLE_LIMIT_UTF16)
        ),
        format!(
            "Page {}/{} · import starts after download completes",
            page + 1,
            page_count
        ),
        music_status_line(snapshot, music_enabled),
    ];

    for file in files.iter().skip(page * PAGE_SIZE).take(PAGE_SIZE) {
        let relative = safe_relative_text(&file.relative_path)
            .unwrap_or_else(|| "[file name unavailable]".to_string());
        lines.push(format!(
            "• {} · {} · {} · {}",
            bounded_text(&relative, BODY_PATH_LIMIT_UTF16),
            format_size(file.size),
            progress_text(file.progress),
            stage_label(file.music_stage.as_ref())
        ));
    }
    if files.is_empty() {
        lines.push("No supported music files are selected.".to_string());
    }

    if page > 0 {
        keyboard.push(vec![button(
            "‹ Previous",
            callback(snapshot, &format!("v:{}", page - 1), true)?,
        )]);
    }
    if page + 1 < page_count {
        keyboard.push(vec![button(
            "Next ›",
            callback(snapshot, &format!("v:{}", page + 1), true)?,
        )]);
    }
    if music_enabled && !files.is_empty() {
        keyboard.push(vec![button(
            "Confirm import",
            callback(snapshot, "i", true)?,
        )]);
    }
    keyboard.push(vec![button(
        "‹ Back to files",
        callback(snapshot, "b:0:0", false)?,
    )]);
    Ok(lines.join("\n"))
}

fn add_browser_controls(
    snapshot: &TorrentSnapshot,
    current: &Path,
    page: usize,
    page_count: usize,
    selected_count: usize,
    music_enabled: bool,
    keyboard: &mut Vec<Vec<InlineKeyboardButton>>,
) -> Result<()> {
    let retryable_count = ImportCounts::from_snapshot(snapshot).retryable();
    let parent_id = if current.as_os_str().is_empty() {
        None
    } else {
        let parent = current.parent().unwrap_or_else(|| Path::new(""));
        snapshot
            .folders
            .iter()
            .find(|folder| folder.relative_path == parent)
            .map(|folder| folder.id)
            .or(Some(0))
    };
    let mut navigation = Vec::new();
    if let Some(parent_id) = parent_id {
        navigation.push(button(
            "‹ Back",
            callback(snapshot, &format!("b:{parent_id}:0"), false)?,
        ));
    }
    if page > 0 {
        navigation.push(button(
            "‹ Prev",
            callback(
                snapshot,
                &format!("b:{}:{}", snapshot.cursor_folder_id, page - 1),
                false,
            )?,
        ));
    }
    if page + 1 < page_count {
        navigation.push(button(
            "Next ›",
            callback(
                snapshot,
                &format!("b:{}:{}", snapshot.cursor_folder_id, page + 1),
                false,
            )?,
        ));
    }
    if !navigation.is_empty() {
        keyboard.push(navigation);
    }

    keyboard.push(vec![button("Refresh", callback(snapshot, "r", false)?)]);

    let (_, supported_here, selected_here) = folder_counts(snapshot, current);
    if supported_here > 0 {
        let action = format!(
            "d:{}:{}",
            snapshot.cursor_folder_id,
            u8::from(selected_here != supported_here)
        );
        keyboard.push(vec![button(
            if selected_here == supported_here {
                "☑ Clear this folder"
            } else if snapshot.cursor_folder_id == 0 {
                "☐ Select all music"
            } else {
                "☐ Select this folder"
            },
            callback(snapshot, &action, false)?,
        )]);
    }

    if selected_count > 0 {
        keyboard.push(vec![button(
            &format!("Review {selected_count} selected"),
            callback(snapshot, "v:0", true)?,
        )]);
    }
    if selected_count > 0 || snapshot.selected_count > 0 {
        keyboard.push(vec![button(
            "Clear selection",
            callback(snapshot, "c", false)?,
        )]);
    }
    if music_enabled && retryable_count > 0 {
        keyboard.push(vec![button(
            &format!("Retry {retryable_count} failed/cancelled"),
            callback(snapshot, "y", false)?,
        )]);
    }
    Ok(())
}

fn visible_entries<'a>(snapshot: &'a TorrentSnapshot, current: &Path) -> Vec<VisibleEntry<'a>> {
    let mut entries = snapshot
        .folders
        .iter()
        .filter(|folder| {
            folder.id != snapshot.cursor_folder_id
                && folder
                    .relative_path
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    == current
        })
        .map(VisibleEntry::Folder)
        .collect::<Vec<_>>();
    entries.extend(
        snapshot
            .files
            .iter()
            .filter(|file| file.relative_path.parent().unwrap_or_else(|| Path::new("")) == current)
            .map(VisibleEntry::File),
    );
    entries
}

fn folder_path(snapshot: &TorrentSnapshot, folder_id: u32) -> Result<&Path> {
    snapshot
        .folders
        .iter()
        .find(|folder| folder.id == folder_id)
        .map(|folder| folder.relative_path.as_path())
        .ok_or_else(|| anyhow::anyhow!("torrent folder cursor is invalid"))
}

fn folder_counts(snapshot: &TorrentSnapshot, folder_path: &Path) -> (usize, usize, usize) {
    let mut total = 0;
    let mut supported = 0;
    let mut selected = 0;
    for file in &snapshot.files {
        let inside = if folder_path.as_os_str().is_empty() {
            true
        } else {
            file.relative_path.starts_with(folder_path)
        };
        if inside {
            total += 1;
            if file.supported_music {
                supported += 1;
                if file.selected {
                    selected += 1;
                }
            }
        }
    }
    (total, supported, selected)
}

fn supported_selected_count(snapshot: &TorrentSnapshot) -> usize {
    snapshot
        .files
        .iter()
        .filter(|file| file.supported_music && file.selected)
        .count()
}

fn supported_music_count(snapshot: &TorrentSnapshot) -> usize {
    snapshot
        .files
        .iter()
        .filter(|file| file.supported_music)
        .count()
}

fn scheduled_music_count(snapshot: &TorrentSnapshot) -> usize {
    snapshot
        .files
        .iter()
        .filter(|file| file.supported_music && file.music_stage.is_some())
        .count()
}

fn can_add_all_music(snapshot: &TorrentSnapshot, music_enabled: bool) -> bool {
    let supported = supported_music_count(snapshot);
    music_enabled
        && snapshot.inventory_verified
        && supported > 0
        && scheduled_music_count(snapshot) < supported
}

fn music_status_line(snapshot: &TorrentSnapshot, music_enabled: bool) -> String {
    if !snapshot.inventory_verified {
        return if music_enabled {
            "Music: waiting for a verified file inventory.".to_string()
        } else {
            "Music imports are disabled on this bot; file inventory is not verified.".to_string()
        };
    }
    let supported = supported_music_count(snapshot);
    if supported == 0 {
        return if music_enabled {
            "Music: no supported music files.".to_string()
        } else {
            "Music imports are disabled on this bot; no supported music files.".to_string()
        };
    }
    if !music_enabled {
        return format!(
            "Music imports are disabled on this bot · {supported} supported music files."
        );
    }

    let scheduled = scheduled_music_count(snapshot);
    let counts = ImportCounts::from_snapshot(snapshot);
    if scheduled == supported {
        format!(
            "已安排添加 {scheduled} 个音乐文件 · {} imported · {} waiting · {} failed/cancelled · {} unknown",
            counts.complete,
            counts.pending,
            counts.retryable(),
            counts.unknown
        )
    } else if scheduled > 0 {
        format!(
            "Music: {scheduled}/{supported} supported files arranged · {} imported · {} waiting · {} failed/cancelled · {} unknown",
            counts.complete,
            counts.pending,
            counts.retryable(),
            counts.unknown
        )
    } else {
        format!(
            "Music: {supported} supported files · {} selected · {} imported · {} waiting · {} failed/cancelled · {} unknown",
            supported_selected_count(snapshot),
            counts.complete,
            counts.pending,
            counts.retryable(),
            counts.unknown
        )
    }
}

fn callback(snapshot: &TorrentSnapshot, action: &str, selection_revision: bool) -> Result<String> {
    let revision = if selection_revision {
        snapshot.selection_revision
    } else {
        snapshot.inventory_revision
    };
    let data = format!("t1:{}:{revision:016x}:{action}", snapshot.task_id);
    ensure!(
        data.is_ascii() && data.len() <= CALLBACK_DATA_LIMIT_BYTES,
        "torrent callback data exceeds Telegram's limit"
    );
    Ok(data)
}

fn button(label: &str, callback_data: String) -> InlineKeyboardButton {
    InlineKeyboardButton {
        text: bounded_text(label, BUTTON_TEXT_LIMIT_UTF16),
        callback_data,
        web_app: None,
    }
}

fn valid_task_id(task_id: &str) -> bool {
    task_id.len() == 32
        && task_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn phase_label(phase: &TorrentPhase) -> &'static str {
    match phase {
        TorrentPhase::Queued => "Queued",
        TorrentPhase::DownloadingMetainfo => "Reading torrent metadata",
        TorrentPhase::CheckingExisting => "Checking existing torrent",
        TorrentPhase::Submitting => "Submitting to qBittorrent",
        TorrentPhase::ReconciliationPending => "Waiting for qBittorrent",
        TorrentPhase::Downloading => "Downloading",
        TorrentPhase::Completed => "Download complete",
        TorrentPhase::SelectingFiles => "Choose music files",
        TorrentPhase::ImportRequested => "Music import requested",
        TorrentPhase::Importing => "Importing to Music",
        TorrentPhase::Imported => "Music import complete",
        TorrentPhase::Failed => "Failed",
        TorrentPhase::Ambiguous => "Needs attention",
        TorrentPhase::Invalid => "Invalid torrent",
    }
}

fn progress_text(progress: f64) -> String {
    if progress.is_finite() {
        format!("{:.0}%", progress.clamp(0.0, 1.0) * 100.0)
    } else {
        "progress unavailable".to_string()
    }
}

fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit + 1 < UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

fn stage_label(stage: Option<&TorrentMusicStage>) -> &'static str {
    match stage {
        Some(TorrentMusicStage::Pending) => "Import waiting",
        Some(TorrentMusicStage::Complete) => "Imported",
        Some(TorrentMusicStage::Failed) => "Import failed",
        Some(TorrentMusicStage::Cancelled) => "Import cancelled",
        Some(TorrentMusicStage::OutcomeUnknown) => "Import status unknown",
        None => "Not imported",
    }
}

fn safe_relative_text(path: &Path) -> Option<String> {
    if path.is_absolute() {
        return None;
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_str()?.to_string()),
            _ => return None,
        }
    }
    (!parts.is_empty()).then(|| parts.join("/"))
}

fn bounded_text(text: &str, max_utf16: usize) -> String {
    let cleaned = text
        .chars()
        .map(|character| {
            if character.is_control()
                || matches!(character as u32, 0x061c | 0x200e..=0x200f | 0x202a..=0x202e | 0x2066..=0x2069)
            {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    if cleaned.encode_utf16().count() <= max_utf16 {
        return cleaned;
    }
    if max_utf16 == 0 {
        return String::new();
    }
    let ellipsis = '…';
    let limit = max_utf16.saturating_sub(1);
    let mut output = String::new();
    let mut units = 0;
    for character in cleaned.chars() {
        let next = character.len_utf16();
        if units + next > limit {
            break;
        }
        output.push(character);
        units += next;
    }
    output.push(ellipsis);
    output
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn snapshot() -> TorrentSnapshot {
        TorrentSnapshot {
            task_id: "0123456789abcdef0123456789abcdef".into(),
            chat_id: 1,
            user_id: 2,
            upload_message_id: 3,
            card_message_id: Some(4),
            inventory_revision: 7,
            selection_revision: 9,
            render_revision: 11,
            name: "Seed pack 🎧".into(),
            identity: None,
            phase: TorrentPhase::Downloading,
            state: None,
            progress: 0.5,
            inventory_verified: false,
            cursor_folder_id: 0,
            cursor_page: 0,
            review_selection: false,
            folders: vec![
                TorrentFolder {
                    id: 0,
                    relative_path: PathBuf::new(),
                    file_count: 3,
                },
                TorrentFolder {
                    id: 1,
                    relative_path: PathBuf::from("Album"),
                    file_count: 2,
                },
            ],
            files: vec![
                TorrentFileSnapshot {
                    index: 0,
                    relative_path: PathBuf::from("Album/01.flac"),
                    size: 1024,
                    progress: 0.25,
                    complete: false,
                    selected: true,
                    supported_music: true,
                    needs_reconfirm: false,
                    music_stage: None,
                },
                TorrentFileSnapshot {
                    index: 1,
                    relative_path: PathBuf::from("Album/02.mp3"),
                    size: 2_048,
                    progress: 0.0,
                    complete: false,
                    selected: false,
                    supported_music: true,
                    needs_reconfirm: false,
                    music_stage: Some(TorrentMusicStage::Failed),
                },
                TorrentFileSnapshot {
                    index: 2,
                    relative_path: PathBuf::from("sample.txt"),
                    size: 5,
                    progress: 1.0,
                    complete: true,
                    selected: false,
                    supported_music: false,
                    needs_reconfirm: false,
                    music_stage: None,
                },
            ],
            selected_count: 1,
            import_count: 1,
            error: None,
        }
    }

    #[test]
    fn renders_native_callbacks_with_separate_inventory_and_selection_revisions() {
        let rendered = render_torrent_inline(&snapshot()).unwrap();
        let callbacks = rendered
            .reply_markup
            .inline_keyboard
            .iter()
            .flatten()
            .map(|button| button.callback_data.as_str())
            .collect::<Vec<_>>();
        assert!(callbacks.iter().any(|value| value == &"t1:0123456789abcdef0123456789abcdef:0000000000000007:b:1:0"));
        assert!(
            callbacks
                .iter()
                .any(|value| value == &"t1:0123456789abcdef0123456789abcdef:0000000000000009:v:0")
        );
        assert!(
            callbacks
                .iter()
                .any(|value| value == &"t1:0123456789abcdef0123456789abcdef:0000000000000007:d:1:1")
        );
        assert!(rendered.text.contains("sample.txt"));
        assert!(rendered.text.contains("Not music"));
        assert!(
            callbacks
                .iter()
                .all(|value| value.len() <= CALLBACK_DATA_LIMIT_BYTES)
        );
    }

    #[test]
    fn cloud_cards_show_a_bounded_summary_and_task_specific_miniapp_button() {
        let source = snapshot();
        let rendered =
            render_torrent_inline_with_miniapp(&source, Some("https://worker.example.test/bot"))
                .unwrap();
        assert!(rendered.text.contains("Torrent:"));
        assert!(!rendered.text.contains("sample.txt"));
        assert_eq!(rendered.reply_markup.inline_keyboard.len(), 1);
        let button = &rendered.reply_markup.inline_keyboard[0][0];
        assert_eq!(button.text, "Manage torrent");
        assert!(button.callback_data.is_empty());
        assert_eq!(
            button.web_app.as_ref().map(|web_app| web_app.url.as_str()),
            Some(
                "https://worker.example.test/bot/file/?view=torrents&task_id=0123456789abcdef0123456789abcdef"
            )
        );
    }

    #[test]
    fn review_pages_selected_supported_files_and_never_emits_a_local_path() {
        let mut source = snapshot();
        source.review_selection = true;
        source.files[0].relative_path = PathBuf::from("Album/音楽🎧.flac");
        source.files[1].selected = true;
        for index in 3..=5 {
            let mut file = source.files[0].clone();
            file.index = index;
            file.relative_path = PathBuf::from(format!("Disc/{index:02}.flac"));
            source.files.push(file);
        }
        source.selected_count = 5;
        source.cursor_page = 1;
        let rendered = render_torrent_inline(&source).unwrap();
        assert!(rendered.text.contains("Page 2/2"));
        assert!(rendered.text.contains("Disc/05.flac"));
        assert!(!rendered.text.contains("/Users/"));
        assert!(!rendered.text.contains("file://"));
        assert!(
            rendered
                .reply_markup
                .inline_keyboard
                .iter()
                .flatten()
                .any(|button| button.callback_data.ends_with(":v:0"))
        );
        assert!(
            rendered
                .reply_markup
                .inline_keyboard
                .iter()
                .flatten()
                .any(|button| button.callback_data.ends_with(":i"))
        );
    }

    #[test]
    fn caps_utf16_body_and_button_text_on_unicode_boundaries() {
        let mut source = snapshot();
        source.name = "音楽🎧".repeat(2_000);
        source.files[0].relative_path = PathBuf::from(format!("{}.flac", "曲🎵".repeat(200)));
        let rendered = render_torrent_inline(&source).unwrap();
        assert!(rendered.text.encode_utf16().count() <= TELEGRAM_MESSAGE_LIMIT_UTF16);
        assert!(
            rendered
                .reply_markup
                .inline_keyboard
                .iter()
                .flatten()
                .all(|button| button.text.encode_utf16().count() <= BUTTON_TEXT_LIMIT_UTF16)
        );
    }
}
