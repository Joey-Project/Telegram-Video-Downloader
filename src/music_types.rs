//! Shared boundaries for durable music jobs and their isolated platform adapters.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{error::Error, fmt};

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[path = "music_local.rs"]
mod local_snapshot;

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

/// A batch of complete files selected from a local torrent client for Music import.
#[derive(Clone, Debug)]
pub struct LocalMusicBatch {
    pub chat_id: i64,
    pub user_id: i64,
    pub batch_id: String,
    pub files: Vec<LocalMusicFile>,
}

/// A selected local file and the source-object proof captured by the server.
///
/// The proof field is private so callers cannot turn client-provided metadata into an
/// authorization to import an arbitrary path. Use [`LocalMusicFile::capture`] on a blocking
/// worker after the torrent client reports the selected file complete.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LocalMusicFile {
    pub source_key: String,
    pub source_root: PathBuf,
    pub relative_path: PathBuf,
    pub file_name: String,
    pub file_size: u64,
    proof: local_snapshot::SourceProof,
}

impl LocalMusicFile {
    /// Capture a bounded proof for a regular file reached below `source_root` without symlinks.
    ///
    /// This reads the selected file once to bind its SHA-256, size, and object identity. Call it
    /// off the async update loop and only after the torrent client reports the file complete.
    pub fn capture(
        source_key: String,
        source_root: PathBuf,
        relative_path: PathBuf,
        max_file_bytes: u64,
        cancel: &MusicCancellation,
    ) -> Result<Self> {
        let file_name = relative_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow::anyhow!("selected local file name is not valid UTF-8"))?
            .to_string();
        let proof =
            local_snapshot::capture_source(&source_root, &relative_path, max_file_bytes, cancel)?;
        let file = Self {
            source_key,
            source_root,
            relative_path,
            file_name,
            file_size: proof.file_size,
            proof,
        };
        file.validate_shape(max_file_bytes)?;
        Ok(file)
    }

    pub(crate) fn validate_shape(&self, max_file_bytes: u64) -> Result<()> {
        local_snapshot::validate_source_shape(self, max_file_bytes)
    }

    pub(crate) fn copy_verified_snapshot(
        &self,
        destination: &Path,
        max_file_bytes: u64,
        cancel: &MusicCancellation,
    ) -> Result<()> {
        local_snapshot::copy_verified_snapshot(self, destination, max_file_bytes, cancel)
    }

    pub(crate) fn verify_existing_snapshot(
        &self,
        destination: &Path,
        max_file_bytes: u64,
        cancel: &MusicCancellation,
    ) -> Result<()> {
        local_snapshot::verify_existing_snapshot(self, destination, max_file_bytes, cancel)
    }

    pub(crate) fn matches_content(&self, sha256: &str, size: u64) -> bool {
        self.file_size == size && self.proof.sha256.eq_ignore_ascii_case(sha256)
    }

    pub(crate) fn resolved_path(&self) -> PathBuf {
        self.proof.resolved_root.join(&self.relative_path)
    }
}

/// Durable reference to a local Music job. The source key is stable across batch replays.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LocalMusicJobRef {
    pub source_key: String,
    pub job_id: String,
}

/// Read-only local Music job state returned to the aggregate torrent widget.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct LocalMusicJobStatus {
    pub job_id: String,
    pub source_key: String,
    pub stage: JobStage,
    pub error: Option<String>,
}

/// Durable stage shared by Telegram and local Music jobs.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStage {
    Queued,
    Downloading,
    Preparing,
    Archiving,
    CatalogDedupe,
    Importing,
    Querying,
    WaitingMusicCloud,
    VerifyingCopy,
    ArchiveUploading,
    WaitingArchiveCloud,
    Evicting,
    Complete,
    Failed,
    Cancelled,
    OutcomeUnknown,
}

impl JobStage {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Failed | Self::Cancelled | Self::OutcomeUnknown
        )
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Preparing => "preparing audio",
            Self::Archiving => "saving archive copy",
            Self::CatalogDedupe => "checking existing music",
            Self::Importing => "importing into Music",
            Self::Querying => "checking Music",
            Self::WaitingMusicCloud => "waiting for Music cloud sync",
            Self::VerifyingCopy => "verifying the Music copy",
            Self::ArchiveUploading => "waiting for archive upload",
            Self::WaitingArchiveCloud => "waiting for archive upload",
            Self::Evicting => "evicting verified archive bytes",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::OutcomeUnknown => "import outcome unknown; no automatic retry",
        }
    }
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
    /// Remove only this job's private staging files after completed-copy proofs pass.
    fn cleanup_staging(
        &self,
        job_id: &str,
        audio: &PreparedAudio,
        archive: &ArchiveState,
        track: &MusicTrack,
    ) -> Result<()>;
}
