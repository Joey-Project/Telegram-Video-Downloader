//! Static Telegram HTML widget contract for owner torrent selection.

use serde_json::{Value, json};
use std::fmt;

#[cfg(test)]
pub const TORRENT_WIDGET_HTML: &str = include_str!("../assets/torrent-widget.html");
pub const TORRENT_WIDGET_MAX_ENTRIES: usize = 4;
pub const TORRENT_WIDGET_MAX_CHAT_DATA_BYTES: usize = 1_024;

const MAX_ACTION_PARAMETER: u32 = 5_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TorrentWidgetView {
    pub task_id: String,
    pub inventory_revision: u64,
    pub selection_revision: u64,
    pub render_revision: u64,
    pub title: String,
    pub status: String,
    pub progress_percent: Option<u8>,
    pub completed_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub folder_id: u32,
    pub folder_name: String,
    pub parent_folder_id: Option<u32>,
    /// Zero-based current page in the current folder.
    pub page: u32,
    pub page_count: u32,
    /// Entries from this page only. The caller owns full inventory pagination.
    pub entries: Vec<WidgetEntry>,
    pub selected_count: u32,
    pub import_pending_count: u32,
    pub import_completed_count: u32,
    pub import_failed_count: u32,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WidgetEntry {
    Folder {
        id: u32,
        title: String,
        file_count: u32,
        supported_music_count: u32,
        selected_count: u32,
    },
    File {
        index: u32,
        title: String,
        size_bytes: u64,
        download_percent: u8,
        complete: bool,
        supported_music: bool,
        selected: bool,
        music_stage: Option<WidgetMusicStage>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WidgetMusicStage {
    Pending,
    Complete,
    Failed,
    Cancelled,
    OutcomeUnknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TorrentWidgetPresentation {
    pub chat_data: String,
    pub caption: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TorrentWidgetActionRequest {
    pub task_id: String,
    /// The revision encoded in the request frame. Review and import requests use the selection
    /// revision; other actions use the inventory revision.
    pub expected_revision: u64,
    pub action: TorrentWidgetAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TorrentWidgetAction {
    Refresh,
    Browse { folder_id: u32, page: u32 },
    SetFile { index: u32, selected: bool },
    SetFolder { folder_id: u32, selected: bool },
    ClearSelection,
    ReviewSelection { page: u32 },
    ImportSelected,
    AddAllMusic,
    RetryImports,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WidgetProtocolError {
    ByteLength,
    Format,
    TaskId,
    Revision,
    Parameter,
}

impl fmt::Display for WidgetProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::ByteLength => "widget request must be 1 to 64 UTF-8 bytes",
            Self::Format => "widget request format is unsupported",
            Self::TaskId => "widget task identifier is invalid",
            Self::Revision => "widget expected revision is invalid",
            Self::Parameter => "widget request parameter is out of range",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for WidgetProtocolError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WidgetRenderError {
    InvalidTaskId,
    InvalidPage,
    InvalidProgress,
    TooManyEntries,
    ChatDataTooLarge,
}

impl fmt::Display for WidgetRenderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidTaskId => {
                "torrent widget task identifier must be 32 lowercase hex characters"
            }
            Self::InvalidPage => "torrent widget page must be within a non-empty page range",
            Self::InvalidProgress => "torrent widget progress must be between 0 and 100",
            Self::TooManyEntries => "torrent widget page may contain at most four entries",
            Self::ChatDataTooLarge => "torrent widget chat_data exceeds 1024 UTF-8 bytes",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for WidgetRenderError {}

/// Parses the compact, idempotent request protocol sent by the static widget.
///
/// Format: `t1:<32-lowerhex-task>:<16-lowerhex-expected-revision>:<op>[:args]`.
/// Review and import requests use the selection revision; all other actions use the inventory
/// revision.
/// Folder, browser-page, review-page, and file indexes use the repository's 5000-entry bound.
pub fn parse_widget_request(data: &str) -> Result<TorrentWidgetActionRequest, WidgetProtocolError> {
    if data.is_empty() || data.len() > 64 || !data.is_ascii() {
        return Err(WidgetProtocolError::ByteLength);
    }

    let fields = data.split(':').collect::<Vec<_>>();
    if fields.len() < 4 || fields[0] != "t1" {
        return Err(WidgetProtocolError::Format);
    }
    let task_id = fields[1];
    if task_id.len() != 32
        || !task_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(WidgetProtocolError::TaskId);
    }
    let revision = fields[2];
    if revision.len() != 16
        || !revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(WidgetProtocolError::Revision);
    }
    let expected_revision =
        u64::from_str_radix(revision, 16).map_err(|_| WidgetProtocolError::Revision)?;
    let action = match (fields[3], fields.len()) {
        ("r", 4) => TorrentWidgetAction::Refresh,
        ("c", 4) => TorrentWidgetAction::ClearSelection,
        ("v", 5) => TorrentWidgetAction::ReviewSelection {
            page: parse_bounded_u32(fields[4])?,
        },
        ("i", 4) => TorrentWidgetAction::ImportSelected,
        ("a", 4) => TorrentWidgetAction::AddAllMusic,
        ("y", 4) => TorrentWidgetAction::RetryImports,
        ("b", 6) => TorrentWidgetAction::Browse {
            folder_id: parse_bounded_u32(fields[4])?,
            page: parse_bounded_u32(fields[5])?,
        },
        ("f", 6) => TorrentWidgetAction::SetFile {
            index: parse_bounded_u32(fields[4])?,
            selected: parse_bool(fields[5])?,
        },
        ("d", 6) => TorrentWidgetAction::SetFolder {
            folder_id: parse_bounded_u32(fields[4])?,
            selected: parse_bool(fields[5])?,
        },
        _ => return Err(WidgetProtocolError::Format),
    };

    Ok(TorrentWidgetActionRequest {
        task_id: task_id.to_string(),
        expected_revision,
        action,
    })
}

/// Encodes the current page for `chat_data`, keeping filenames as JSON text only.
///
/// The UI uses DOM `textContent`; filenames are never interpreted as HTML. Titles
/// are shortened at UTF-8 boundaries and get an ellipsis when the view is compact.
pub fn render_torrent_widget(
    view: &TorrentWidgetView,
) -> Result<TorrentWidgetPresentation, WidgetRenderError> {
    if view.task_id.len() != 32
        || !view
            .task_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(WidgetRenderError::InvalidTaskId);
    }
    if view.page_count == 0 || view.page >= view.page_count || view.page > MAX_ACTION_PARAMETER {
        return Err(WidgetRenderError::InvalidPage);
    }
    if view.entries.len() > TORRENT_WIDGET_MAX_ENTRIES {
        return Err(WidgetRenderError::TooManyEntries);
    }
    if view.progress_percent.is_some_and(|progress| progress > 100)
        || view.entries.iter().any(|entry| {
            matches!(entry, WidgetEntry::File { download_percent, .. } if *download_percent > 100)
        })
    {
        return Err(WidgetRenderError::InvalidProgress);
    }

    let entries = view.entries.iter().map(encode_entry).collect::<Vec<_>>();
    let chat_data = json!({
        "v": 1,
        "t": &view.task_id,
        "i": format!("{:016x}", view.inventory_revision),
        "s": format!("{:016x}", view.selection_revision),
        "r": format!("{:016x}", view.render_revision),
        "x": display_text(&view.title, 40),
        "q": display_text(&view.status, 24),
        "p": view.progress_percent,
        "b": view.completed_bytes,
        "B": view.total_bytes,
        "f": view.folder_id,
        "n": display_text(&view.folder_name, 28),
        "d": view.parent_folder_id,
        "g": view.page,
        "G": view.page_count,
        "e": entries,
        "c": view.selected_count,
        "u": view.import_pending_count,
        "o": view.import_completed_count,
        "a": view.import_failed_count,
        "z": view.error.as_deref().map(|value| display_text(value, 32)),
    })
    .to_string();
    if chat_data.len() > TORRENT_WIDGET_MAX_CHAT_DATA_BYTES {
        return Err(WidgetRenderError::ChatDataTooLarge);
    }

    let title = display_text(&view.title, 64);
    let status = display_text(&view.status, 48);
    let caption = format!(
        "{title} — {status}. Select supported music to import; the downloaded torrent remains available for seeding. Reopen: /torrents {}",
        view.task_id
    );
    Ok(TorrentWidgetPresentation { chat_data, caption })
}

fn encode_entry(entry: &WidgetEntry) -> Value {
    match entry {
        WidgetEntry::Folder {
            id,
            title,
            file_count,
            supported_music_count,
            selected_count,
        } => json!({
            "k": "d",
            "i": id,
            "n": display_text(title, 36),
            "f": file_count,
            "m": supported_music_count,
            "c": selected_count,
        }),
        WidgetEntry::File {
            index,
            title,
            size_bytes,
            download_percent,
            complete,
            supported_music,
            selected,
            music_stage,
        } => json!({
            "k": "f",
            "i": index,
            "n": display_text(title, 36),
            "z": size_bytes,
            "p": download_percent,
            "d": complete,
            "m": supported_music,
            "s": selected,
            "u": music_stage.map(music_stage_code),
        }),
    }
}

fn music_stage_code(stage: WidgetMusicStage) -> u8 {
    match stage {
        WidgetMusicStage::Pending => 1,
        WidgetMusicStage::Complete => 2,
        WidgetMusicStage::Failed => 3,
        WidgetMusicStage::Cancelled => 4,
        WidgetMusicStage::OutcomeUnknown => 5,
    }
}

fn display_text(value: &str, max_bytes: usize) -> String {
    let cleaned = value
        .chars()
        .filter(|character| !character.is_control())
        .collect::<String>();
    if cleaned.len() <= max_bytes {
        return cleaned;
    }
    if max_bytes < '…'.len_utf8() {
        return String::new();
    }
    let prefix_limit = max_bytes - '…'.len_utf8();
    let mut prefix = String::new();
    for character in cleaned.chars() {
        if prefix.len() + character.len_utf8() > prefix_limit {
            break;
        }
        prefix.push(character);
    }
    prefix.push('…');
    prefix
}

fn parse_bounded_u32(value: &str) -> Result<u32, WidgetProtocolError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(WidgetProtocolError::Parameter);
    }
    let parsed = value
        .parse::<u32>()
        .map_err(|_| WidgetProtocolError::Parameter)?;
    if parsed > MAX_ACTION_PARAMETER {
        return Err(WidgetProtocolError::Parameter);
    }
    Ok(parsed)
}

fn parse_bool(value: &str) -> Result<bool, WidgetProtocolError> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(WidgetProtocolError::Parameter),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::ZlibEncoder;
    use std::io::Write;

    const TASK: &str = "0123456789abcdef0123456789abcdef";
    const REVISION: &str = "0000000000000012";

    #[test]
    fn compact_request_parser_accepts_each_supported_idempotent_action() {
        let parse = |suffix: &str| parse_widget_request(&format!("t1:{TASK}:{REVISION}:{suffix}"));
        assert_eq!(parse("i").unwrap().expected_revision, 0x12);
        assert_eq!(
            parse("v:0").unwrap().action,
            TorrentWidgetAction::ReviewSelection { page: 0 }
        );
        assert_eq!(
            parse("v:5000").unwrap().action,
            TorrentWidgetAction::ReviewSelection { page: 5000 }
        );
        assert_eq!(parse("r").unwrap().action, TorrentWidgetAction::Refresh);
        assert_eq!(
            parse("b:5:2").unwrap().action,
            TorrentWidgetAction::Browse {
                folder_id: 5,
                page: 2
            }
        );
        assert_eq!(
            parse("f:5000:1").unwrap().action,
            TorrentWidgetAction::SetFile {
                index: 5000,
                selected: true
            }
        );
        assert_eq!(
            parse("d:4:0").unwrap().action,
            TorrentWidgetAction::SetFolder {
                folder_id: 4,
                selected: false
            }
        );
        assert_eq!(
            parse("c").unwrap().action,
            TorrentWidgetAction::ClearSelection
        );
        assert_eq!(
            parse("i").unwrap().action,
            TorrentWidgetAction::ImportSelected
        );
        assert_eq!(
            parse("y").unwrap().action,
            TorrentWidgetAction::RetryImports
        );
    }

    #[test]
    fn compact_request_parser_rejects_malformed_bytes_and_out_of_range_values() {
        let prefix = format!("t1:{TASK}:{REVISION}:");
        for input in [
            "", "r:extra", "b:1", "b:5001:0", "f:0:2", "d:+1:1", "v:+1", "i:1",
        ] {
            assert!(
                parse_widget_request(&format!("{prefix}{input}")).is_err(),
                "{input}"
            );
        }
        assert!(parse_widget_request(&format!("t1:{}:{REVISION}:r", "A".repeat(32))).is_err());
        assert!(parse_widget_request(&format!("t1:{TASK}:10000000000000000:r")).is_err());
        assert!(
            parse_widget_request(&format!("t1:{TASK}:{REVISION}:r:{}", "x".repeat(64))).is_err()
        );
        assert!(parse_widget_request(&format!("t1:{TASK}:{REVISION}:ré")).is_err());
    }

    #[test]
    fn maximum_supported_browse_request_fits_the_64_byte_protocol_limit() {
        let request = format!("t1:{TASK}:ffffffffffffffff:b:5000:5000");
        assert_eq!(request.len(), 64);
        assert_eq!(
            parse_widget_request(&request).unwrap().action,
            TorrentWidgetAction::Browse {
                folder_id: 5000,
                page: 5000
            }
        );
    }

    #[test]
    fn page_state_stays_within_utf8_byte_budget_for_cjk_and_untrusted_titles() {
        let view = TorrentWidgetView {
            task_id: TASK.to_string(),
            inventory_revision: u64::MAX,
            selection_revision: u64::MAX,
            render_revision: u64::MAX,
            title: "音楽🎧\"\\".repeat(30),
            status: "Downloading".to_string(),
            progress_percent: Some(100),
            completed_bytes: Some(u64::MAX),
            total_bytes: Some(u64::MAX),
            folder_id: 5_000,
            folder_name: "文件夹".repeat(30),
            parent_folder_id: Some(4_999),
            page: 5_000,
            page_count: 5_001,
            entries: (0..4)
                .map(|index| WidgetEntry::File {
                    index,
                    title: "🎼\"音".repeat(50),
                    size_bytes: u64::MAX,
                    download_percent: 100,
                    complete: true,
                    supported_music: true,
                    selected: true,
                    music_stage: Some(WidgetMusicStage::OutcomeUnknown),
                })
                .collect(),
            selected_count: 5_000,
            import_pending_count: 5_000,
            import_completed_count: 5_000,
            import_failed_count: 5_000,
            error: Some("错误\"\\".repeat(30)),
        };
        let rendered = render_torrent_widget(&view).unwrap();
        assert!(rendered.chat_data.len() <= TORRENT_WIDGET_MAX_CHAT_DATA_BYTES);
        assert!(rendered.caption.chars().count() <= 1_024);
        let value: Value = serde_json::from_str(&rendered.chat_data).unwrap();
        assert_eq!(value["e"].as_array().unwrap().len(), 4);
        assert_eq!(value["s"], "ffffffffffffffff");
        assert_eq!(value["q"], "Downloading");
        assert!(value["e"][0]["n"].as_str().unwrap().ends_with('…'));
        assert!(!rendered.chat_data.contains("<script"));
        assert!(
            rendered
                .caption
                .contains("/torrents 0123456789abcdef0123456789abcdef")
        );
    }

    #[test]
    fn static_html_is_valid_and_under_telegram_zlib_level_nine_limits() {
        assert!(!TORRENT_WIDGET_HTML.is_empty());
        assert!(!TORRENT_WIDGET_HTML.contains('\0'));
        assert!(TORRENT_WIDGET_HTML.len() <= 32_768);
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(TORRENT_WIDGET_HTML.as_bytes()).unwrap();
        assert!(encoder.finish().unwrap().len() <= 4_096);
        assert!(TORRENT_WIDGET_HTML.contains("textContent"));
        assert!(!TORRENT_WIDGET_HTML.contains("innerHTML"));
    }
}
