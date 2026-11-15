//! Shared contracts for the durable torrent queue and its application adapters.

use std::fmt;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::music_types::{JobStage as MusicJobStage, LocalMusicJobRef, LocalMusicJobStatus};

/// Boxed async result used by the Telegram, qBittorrent, and Music adapter ports.
pub type TorrentFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Torrent queue settings. The password is intentionally redacted from `Debug` output.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct TorrentSettings {
    pub enabled: bool,
    pub base_url: String,
    pub username: Option<String>,
    #[serde(skip_serializing)]
    pub password: Option<String>,
    pub state_dir: PathBuf,
    pub metainfo_dir: Option<PathBuf>,
    pub max_torrent_bytes: usize,
    pub max_files: usize,
    pub poll_seconds: u64,
    pub request_timeout_seconds: u64,
    pub max_api_response_bytes: usize,
}

impl Default for TorrentSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: "http://127.0.0.1:8080".to_string(),
            username: None,
            password: None,
            state_dir: PathBuf::new(),
            metainfo_dir: None,
            max_torrent_bytes: DEFAULT_MAX_TORRENT_BYTES,
            max_files: DEFAULT_MAX_FILES,
            poll_seconds: 10,
            request_timeout_seconds: 10,
            max_api_response_bytes: DEFAULT_MAX_API_RESPONSE_BYTES,
        }
    }
}

impl fmt::Debug for TorrentSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TorrentSettings")
            .field("enabled", &self.enabled)
            .field("base_url", &self.base_url)
            .field("username", &self.username.as_ref().map(|_| "[redacted]"))
            .field("password", &self.password.as_ref().map(|_| "[redacted]"))
            .field("state_dir", &self.state_dir)
            .field("max_torrent_bytes", &self.max_torrent_bytes)
            .field("max_files", &self.max_files)
            .field("poll_seconds", &self.poll_seconds)
            .field("request_timeout_seconds", &self.request_timeout_seconds)
            .field("max_api_response_bytes", &self.max_api_response_bytes)
            .finish()
    }
}

impl TorrentSettings {
    /// Validate bounds and the qBittorrent URL before any network or filesystem work.
    pub fn validate(&self) -> Result<()> {
        crate::qbittorrent::validate_settings(self)
    }
}

pub const DEFAULT_MAX_TORRENT_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_MAX_FILES: usize = 5_000;
pub const DEFAULT_MAX_API_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_TORRENT_NESTING: usize = 64;
pub const MAX_TORRENT_ENTRIES: usize = 50_000;
pub const MAX_TORRENT_PATH_BYTES: usize = 4_096;
pub const MAX_TORRENT_COMPONENT_BYTES: usize = 255;
pub const MAX_TORRENT_NAME_BYTES: usize = 512;
pub const MAX_API_TORRENTS: usize = 20_000;
pub const WIDGET_PAGE_SIZE: usize = 4;

/// Telegram source metadata for an uploaded `.torrent` document.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TorrentUpload {
    pub update_id: i64,
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: i64,
    pub file_id: String,
    pub file_unique_id: String,
    pub file_name: String,
    pub file_size: u64,
}

/// Telegram message containing a magnet link. The URI can contain private tracker passkeys,
/// so its Debug representation is intentionally redacted.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TorrentMagnet {
    pub update_id: i64,
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: i64,
    pub uri: String,
}

/// Telegram catalog message referring to an already existing qBittorrent task.
/// This records the real origin of the catalog entry without pretending that the
/// task was created from an upload or a magnet URI.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TorrentExisting {
    pub chat_id: i64,
    pub user_id: i64,
    pub message_id: i64,
    pub hash: String,
}

/// Provenance for an existing qBittorrent task first associated through the private Mini App.
/// Unlike `TorrentExisting`, this source has no Telegram message and must never be rendered as
/// one or used to manufacture a card callback identity.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TorrentMiniAppSource {
    pub chat_id: i64,
    pub user_id: i64,
    pub hash: String,
}

impl fmt::Debug for TorrentMagnet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TorrentMagnet")
            .field("update_id", &self.update_id)
            .field("chat_id", &self.chat_id)
            .field("message_id", &self.message_id)
            .field("user_id", &self.user_id)
            .field("uri", &"[redacted]")
            .finish()
    }
}

/// The protocol identifiers encoded by a metainfo `info` dictionary.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TorrentIdentity {
    pub v1: Option<String>,
    pub v2: Option<String>,
}

impl TorrentIdentity {
    pub fn storage_key(&self) -> String {
        match (&self.v1, &self.v2) {
            (Some(v1), Some(v2)) => format!("v1-{v1}-v2-{v2}"),
            (Some(v1), None) => format!("v1-{v1}"),
            (None, Some(v2)) => format!("v2-{v2}"),
            (None, None) => "invalid".to_string(),
        }
    }

    pub fn matches_hash(&self, hash: &str) -> bool {
        self.v1
            .as_deref()
            .is_some_and(|v1| v1.eq_ignore_ascii_case(hash))
            || self
                .v2
                .as_deref()
                .is_some_and(|v2| v2.eq_ignore_ascii_case(hash))
    }
}

/// A validated file from a torrent metainfo dictionary.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetainfoFile {
    pub index: u32,
    pub relative_path: PathBuf,
    pub size: u64,
    pub is_padding: bool,
}

/// Validated metadata derived from an uploaded `.torrent` file.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TorrentMetainfo {
    pub identity: TorrentIdentity,
    pub raw_sha256: String,
    pub name: String,
    pub files: Vec<MetainfoFile>,
    pub total_size: u64,
    pub private: bool,
}

/// qBittorrent's read-only file record. The API path remains untrusted until matched against
/// the parsed metainfo and checked by the local Music source capture boundary.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RemoteTorrentFile {
    pub index: u32,
    pub relative_path: PathBuf,
    pub size: u64,
    pub progress: f64,
    pub priority: i64,
}

/// Read-only summary of a qBittorrent torrent.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TorrentRemoteSnapshot {
    pub hash: String,
    pub info_hash_v1: Option<String>,
    pub info_hash_v2: Option<String>,
    pub name: String,
    pub state: String,
    pub progress: f64,
    pub save_path: PathBuf,
    pub content_path: Option<PathBuf>,
    pub total_size: u64,
    pub amount_left: u64,
    pub files: Vec<RemoteTorrentFile>,
}

impl TorrentRemoteSnapshot {
    pub fn matches_identity(&self, identity: &TorrentIdentity) -> bool {
        identity.matches_hash(&self.hash)
            || (self.hash.len() == 40
                && identity.v1.is_none()
                && identity.v2.as_deref().is_some_and(|v2| {
                    v2.len() == 64
                        && v2
                            .get(..40)
                            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(&self.hash))
                }))
            || self.info_hash_v1.as_deref().is_some_and(|hash| {
                identity
                    .v1
                    .as_deref()
                    .is_some_and(|v1| v1.eq_ignore_ascii_case(hash))
            })
            || self.info_hash_v2.as_deref().is_some_and(|hash| {
                identity
                    .v2
                    .as_deref()
                    .is_some_and(|v2| v2.eq_ignore_ascii_case(hash))
            })
    }

    pub fn is_complete(&self) -> bool {
        self.amount_left == 0 && self.progress >= 1.0
    }
}

pub fn supports_music_import(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "m4a" | "mp3" | "aac" | "flac" | "wav"
            )
        })
}

/// Status of the mutating qBittorrent add request. A response never proves the torrent exists;
/// the manager must reconcile with a fresh, read-only lookup before recording attachment.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubmitDisposition {
    Accepted,
    InvalidMetainfo,
    Conflict,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SubmitResult {
    pub http_status: u16,
    pub disposition: SubmitDisposition,
}

/// File submitted to the local Music service after qBittorrent reports full completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedTorrentFile {
    pub source_key: String,
    pub source_root: PathBuf,
    pub relative_path: PathBuf,
    pub expected_size: u64,
}

/// Typed summary shown in a torrent file's Music row.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TorrentMusicStage {
    Pending,
    Complete,
    Failed,
    Cancelled,
    OutcomeUnknown,
}

impl From<&MusicJobStage> for TorrentMusicStage {
    fn from(stage: &MusicJobStage) -> Self {
        match stage {
            MusicJobStage::Complete => Self::Complete,
            MusicJobStage::Failed => Self::Failed,
            MusicJobStage::Cancelled => Self::Cancelled,
            MusicJobStage::OutcomeUnknown => Self::OutcomeUnknown,
            _ => Self::Pending,
        }
    }
}

impl From<LocalMusicJobRef> for TorrentMusicJobRef {
    fn from(value: LocalMusicJobRef) -> Self {
        Self {
            source_key: value.source_key,
            job_id: value.job_id,
        }
    }
}

impl From<LocalMusicJobStatus> for TorrentMusicJobStatus {
    fn from(value: LocalMusicJobStatus) -> Self {
        Self {
            job_id: value.job_id,
            source_key: value.source_key,
            stage: TorrentMusicStage::from(&value.stage),
            error: value.error,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TorrentMusicJobRef {
    pub source_key: String,
    pub job_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TorrentMusicJobStatus {
    pub job_id: String,
    pub source_key: String,
    pub stage: TorrentMusicStage,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TorrentPhase {
    Queued,
    DownloadingMetainfo,
    CheckingExisting,
    Submitting,
    ReconciliationPending,
    Downloading,
    Completed,
    SelectingFiles,
    ImportRequested,
    Importing,
    Imported,
    Failed,
    Ambiguous,
    Invalid,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TorrentFolder {
    pub id: u32,
    pub relative_path: PathBuf,
    pub file_count: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TorrentFileSnapshot {
    pub index: u32,
    pub relative_path: PathBuf,
    pub size: u64,
    pub progress: f64,
    pub complete: bool,
    pub selected: bool,
    pub supported_music: bool,
    pub needs_reconfirm: bool,
    pub music_stage: Option<TorrentMusicStage>,
}

/// Self-contained domain snapshot for a Telegram card renderer.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TorrentSnapshot {
    pub task_id: String,
    pub chat_id: i64,
    pub user_id: i64,
    pub upload_message_id: i64,
    pub card_message_id: Option<i64>,
    pub inventory_revision: u64,
    pub selection_revision: u64,
    pub render_revision: u64,
    pub name: String,
    pub identity: Option<TorrentIdentity>,
    pub phase: TorrentPhase,
    pub state: Option<String>,
    pub progress: f64,
    pub cursor_folder_id: u32,
    pub cursor_page: u32,
    pub review_selection: bool,
    pub folders: Vec<TorrentFolder>,
    pub files: Vec<TorrentFileSnapshot>,
    pub selected_count: usize,
    pub import_count: usize,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TorrentAction {
    Refresh,
    Browse { folder_id: u32, page: u32 },
    ReviewSelection { page: u32 },
    SetFile { index: u32, selected: bool },
    SetFolder { folder_id: u32, selected: bool },
    ClearSelection,
    ImportSelected,
    RetryImports,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TorrentEnqueueResult {
    pub task_id: String,
    pub created: bool,
    pub snapshot: TorrentSnapshot,
}

/// Application adapter for Telegram document downloads, qBittorrent API calls, card delivery,
/// and the durable local Music service. Methods are invoked only by the background manager
/// except that their results are recorded through fast local state transactions.
pub trait TorrentBackend: Send + Sync + 'static {
    fn download_torrent(&self, upload: TorrentUpload) -> TorrentFuture<'_, Vec<u8>>;

    fn lookup(&self, identity: TorrentIdentity) -> TorrentFuture<'_, Vec<TorrentRemoteSnapshot>>;

    fn list_torrents(
        &self,
        _offset: u32,
        _limit: u32,
    ) -> TorrentFuture<'_, Vec<TorrentRemoteSnapshot>> {
        Box::pin(async { anyhow::bail!("qBittorrent catalog listing is not configured") })
    }

    fn set_active(&self, _hash: String, _active: bool) -> TorrentFuture<'_, ()> {
        Box::pin(async { anyhow::bail!("qBittorrent task control is not configured") })
    }

    fn submit(&self, raw_torrent: Vec<u8>, file_name: String) -> TorrentFuture<'_, SubmitResult>;

    fn default_save_path(&self) -> TorrentFuture<'_, PathBuf> {
        Box::pin(async { anyhow::bail!("qBittorrent default save path is not configured") })
    }

    fn submit_magnet(&self, _uri: String, _save_path: PathBuf) -> TorrentFuture<'_, SubmitResult> {
        Box::pin(async { anyhow::bail!("qBittorrent magnet submission is not configured") })
    }

    fn export_metainfo(&self, _hash: String) -> TorrentFuture<'_, Vec<u8>> {
        Box::pin(async { anyhow::bail!("qBittorrent metainfo export is not configured") })
    }

    fn refresh(&self, hash: String) -> TorrentFuture<'_, TorrentRemoteSnapshot>;

    fn task_summary(&self, hash: String) -> TorrentFuture<'_, TorrentRemoteSnapshot> {
        self.refresh(hash)
    }

    fn send_card(&self, snapshot: TorrentSnapshot) -> TorrentFuture<'_, i64>;

    fn update_card(&self, snapshot: TorrentSnapshot) -> TorrentFuture<'_, ()>;

    fn enqueue_music(
        &self,
        chat_id: i64,
        user_id: i64,
        batch_id: String,
        files: Vec<CompletedTorrentFile>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobRef>>;

    /// Read existing local Music jobs by their durable source keys without admitting work.
    fn lookup_music_jobs(
        &self,
        _chat_id: i64,
        _user_id: i64,
        _source_keys: Vec<String>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        Box::pin(async { anyhow::bail!("local Music source lookup is not configured") })
    }

    fn music_status(
        &self,
        chat_id: i64,
        job_ids: Vec<String>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobStatus>>;

    fn retry_music(&self, _chat_id: i64, _job_ids: Vec<String>) -> TorrentFuture<'_, ()> {
        Box::pin(async { anyhow::bail!("Music retry is not configured") })
    }
}
