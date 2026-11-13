//! Shared boundaries for durable music jobs and their isolated platform adapters.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{error::Error, fmt};

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Recognized Telegram download outcomes safe to show to the upload owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MusicDownloadFailure {
    RefreshRequired,
    RateLimited,
    TimedOut,
    Busy,
}

impl MusicDownloadFailure {
    pub fn safe_message(self) -> &'static str {
        match self {
            Self::RefreshRequired => {
                "Telegram file reference expired; ask the sender to resend the audio."
            }
            Self::RateLimited => {
                "Telegram temporarily rate-limited the audio transfer; retry later."
            }
            Self::TimedOut => {
                "Telegram audio transfer exceeded its time limit; retry is available."
            }
            Self::Busy => "another Music transfer still owns this staging directory; retry later.",
        }
    }
}

impl fmt::Display for MusicDownloadFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_message())
    }
}

impl Error for MusicDownloadFailure {}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MusicUpload {
    pub update_id: i64,
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: i64,
    pub file_id: String,
    pub file_unique_id: String,
    pub file_name: String,
    pub file_size: u64,
    pub mime_type: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct MusicSettings {
    pub enabled: bool,
    pub state_dir: PathBuf,
    pub staging_dir: PathBuf,
    pub archive_dir: PathBuf,
    pub credentials_path: PathBuf,
    pub helper_python: PathBuf,
    pub transfer_helper: PathBuf,
    pub converter: PathBuf,
    pub conversion_timeout_seconds: u64,
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
    pub max_file_bytes: u64,
    pub cloud_poll_seconds: u64,
}

impl Default for MusicSettings {
    fn default() -> Self {
        Self {
            enabled: cfg!(target_os = "macos"),
            state_dir: PathBuf::new(),
            staging_dir: PathBuf::new(),
            archive_dir: PathBuf::new(),
            credentials_path: PathBuf::new(),
            helper_python: PathBuf::new(),
            transfer_helper: PathBuf::new(),
            converter: PathBuf::new(),
            conversion_timeout_seconds: 1800,
            ffmpeg: PathBuf::new(),
            ffprobe: PathBuf::new(),
            max_file_bytes: 512 * 1024 * 1024,
            cloud_poll_seconds: 60,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AudioBytes {
    pub path: PathBuf,
    pub sha256: String,
    pub size: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PreparedAudio {
    pub original: AudioBytes,
    pub import: AudioBytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversion: Option<flac_alac_converter::ConversionReport>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ArchiveState {
    pub path: PathBuf,
    pub uploaded: bool,
    pub uploading: bool,
    pub downloading: bool,
    pub conflicts: bool,
    pub local_bytes: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MusicTrack {
    pub persistent_id: String,
    pub location: Option<PathBuf>,
    pub cloud_status: String,
}

impl MusicTrack {
    pub fn cloud_complete(&self) -> bool {
        matches!(self.cloud_status.as_str(), "uploaded" | "matched")
    }
}

#[derive(Clone, Default)]
pub struct MusicCancellation(Arc<AtomicBool>);

impl MusicCancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub(crate) fn atomic(&self) -> &AtomicBool {
        &self.0
    }
}

/// All slow methods run on blocking workers, never the Telegram update loop.
/// `add_track` is called only after the caller has durably recorded an add intent.
pub trait MusicBackend: Send + Sync + 'static {
    fn download(
        &self,
        upload: &MusicUpload,
        destination: &Path,
        cancel: &MusicCancellation,
    ) -> Result<()>;
    fn prepare(
        &self,
        original: &Path,
        working_dir: &Path,
        cancel: &MusicCancellation,
    ) -> Result<PreparedAudio>;
    fn archive(&self, audio: &PreparedAudio, original_name: &str) -> Result<ArchiveState>;
    /// Run before persisting an add intent so missing consent remains safely retryable.
    fn check_music_ready(&self) -> Result<()> {
        Ok(())
    }
    fn check_import_source(&self, _audio: &PreparedAudio) -> Result<()> {
        Ok(())
    }
    fn add_track(&self, audio: &PreparedAudio) -> Result<String>;
    fn query_track(&self, persistent_id: &str) -> Result<MusicTrack>;
    fn verify_copy(&self, audio: &PreparedAudio, track: &MusicTrack) -> Result<()>;
    fn archive_status(&self, path: &Path) -> Result<ArchiveState>;
    fn evict_archive(&self, archive: &ArchiveState, track: &MusicTrack) -> Result<ArchiveState>;
}
