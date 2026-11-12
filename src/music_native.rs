//! Native, bounded audio preparation and Apple Music / iCloud operations.
//!
//! Every method is synchronous by design. The manager calls these methods on
//! blocking workers; none of the code here starts playback or changes volume.

use anyhow::{Context, bail};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use crate::music_types::{
    ArchiveState, AudioBytes, MusicCancellation, MusicSettings, MusicTrack, PreparedAudio,
};

const MAX_AUDIO_BYTES: u64 = 512 * 1024 * 1024;
const TOOL_STDOUT_LIMIT: usize = 64 * 1024;
const TOOL_STDERR_LIMIT: usize = 16 * 1024;
const AUDIO_TOOL_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const MUSIC_SCRIPT_TIMEOUT_SECS: u64 = 25;
const VERIFIED_COPY_TTL: Duration = Duration::from_secs(5 * 60);
const MUSIC_CLOUD_DIRECTORY: &str = "Library/Mobile Documents/com~apple~CloudDocs/Collection/Music";

#[cfg(target_os = "macos")]
const SF_DATALESS: u32 = 0x4000_0000;

static REMUX_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[cfg(target_os = "macos")]
static FILE_PROVIDER_COORDINATION_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug)]
struct FileIdentity {
    device: u64,
    inode: u64,
    size: u64,
}

#[derive(Clone, Debug)]
struct ToolOutput {
    stdout: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AudioKind {
    M4a,
    Mp3,
    RawAac,
}

#[derive(Clone, Debug)]
struct ProbeInfo {
    kind: AudioKind,
    attached_picture_indices: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DownloadStatus {
    NotDownloaded,
    Downloaded,
    Current,
    Unknown,
}

impl DownloadStatus {
    fn parse(value: &str) -> Self {
        match value {
            "NSURLUbiquitousItemDownloadingStatusNotDownloaded" => Self::NotDownloaded,
            "not downloaded" => Self::NotDownloaded,
            "NSURLUbiquitousItemDownloadingStatusDownloaded" => Self::Downloaded,
            "downloaded" => Self::Downloaded,
            "NSURLUbiquitousItemDownloadingStatusCurrent" => Self::Current,
            "current" => Self::Current,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct CloudMetadata {
    uploaded: bool,
    uploading: bool,
    downloading: bool,
    conflicts: bool,
    download_status: DownloadStatus,
}

#[derive(Clone, Debug)]
struct VerifiedCopy {
    persistent_id: String,
    path: PathBuf,
    identity: FileIdentity,
    expires_at: Instant,
}

struct PendingImportLease {
    audio: AudioBytes,
    file: File,
    identity: FileIdentity,
    expires_at: Instant,
}

/// Production backend for audio validation, Music automation, and iCloud Drive.
pub struct NativeMusicBackend {
    settings: MusicSettings,
    pending_import: Mutex<Option<PendingImportLease>>,
    verified_copy: Mutex<Option<VerifiedCopy>>,
}

impl NativeMusicBackend {
    pub fn new(settings: MusicSettings) -> Self {
        Self {
            settings,
            pending_import: Mutex::new(None),
            verified_copy: Mutex::new(None),
        }
    }

    /// Perform a non-prompting fixed-identity check before the manager records
    /// an add intent. This deliberately does not require muted system audio.
    pub fn check_music_ready(&self) -> anyhow::Result<()> {
        crate::music_automation::require_fixed_music_permission()
    }

    /// Materialize, validate, and keep the import staging file open before the
    /// manager persists an add intent. The single pending lease is short lived.
    pub fn check_import_source(&self, audio: &PreparedAudio) -> anyhow::Result<()> {
        materialize_if_needed(&audio.import.path, &self.settings, None)
            .context("Music import needs bounded materialization before its add intent")?;
        let file = open_verified_audio(&audio.import, self.settings.max_file_bytes)
            .context("open verified Music import bytes before its add intent")?;
        let identity = file_identity(&file, &audio.import.path)?;
        let lease = PendingImportLease {
            audio: audio.import.clone(),
            file,
            identity,
            expires_at: Instant::now() + VERIFIED_COPY_TTL,
        };
        *self
            .pending_import
            .lock()
            .map_err(|_| anyhow::anyhow!("pending import lease lock is poisoned"))? = Some(lease);
        Ok(())
    }

    pub fn prepare(
        &self,
        original: &Path,
        working_dir: &Path,
        cancel: &MusicCancellation,
    ) -> anyhow::Result<PreparedAudio> {
        check_cancelled(cancel)?;
        materialize_if_needed(original, &self.settings, Some(cancel))?;
        // Hold the provider item open across both FFmpeg reads to discourage
        // File Provider from evicting its bytes between the probe and decode.
        let source_lease = open_regular_file(original, true)?;
        let original_audio =
            inspect_open_audio(&source_lease, original, self.settings.max_file_bytes)?;
        let probe = probe_audio(&self.settings.ffprobe, original, cancel)?;
        decode_audio(&self.settings.ffmpeg, original, cancel)?;
        verify_open_audio(&source_lease, &original_audio, self.settings.max_file_bytes)
            .context("source audio changed while it was being decoded")?;

        let import = match probe.kind {
            AudioKind::M4a | AudioKind::Mp3 => original_audio.clone(),
            AudioKind::RawAac => {
                ensure_directory(working_dir, true)?;
                let output = unique_remux_path(working_dir, &original_audio.sha256)?;
                remux_aac(
                    &self.settings.ffmpeg,
                    original,
                    &output,
                    &probe.attached_picture_indices,
                    cancel,
                )?;
                materialize_if_needed(&output, &self.settings, Some(cancel))
                    .context("remuxed import needs bounded materialization before verification")?;
                let import_lease = open_regular_file(&output, true)?;
                let output_audio =
                    inspect_open_audio(&import_lease, &output, self.settings.max_file_bytes)?;
                let remux_probe = probe_audio(&self.settings.ffprobe, &output, cancel)?;
                if !matches!(remux_probe.kind, AudioKind::M4a) {
                    bail!("AAC remux did not produce an AAC/ALAC M4A import file");
                }
                decode_audio(&self.settings.ffmpeg, &output, cancel)?;
                verify_open_audio(&import_lease, &output_audio, self.settings.max_file_bytes)
                    .context("remuxed import changed while it was being decoded")?;
                output_audio
            }
        };

        Ok(PreparedAudio {
            original: original_audio,
            import,
        })
    }

    pub fn archive(
        &self,
        audio: &PreparedAudio,
        original_name: &str,
    ) -> anyhow::Result<ArchiveState> {
        let archive_root = self.archive_root()?;
        ensure_archive_directory(&archive_root)?;
        let filename = archive_filename(original_name, &audio.original)?;
        let destination = archive_root.join(filename);
        ensure_no_symlink_components(&destination, true)?;

        match fs::symlink_metadata(&destination) {
            Ok(metadata) => {
                identity_from_metadata(&destination, &metadata, true)?;
                materialize_if_needed(&destination, &self.settings, None).context(
                    "existing archive needs bounded materialization before verification",
                )?;
                let existing_file = open_regular_file(&destination, true)?;
                let existing =
                    inspect_open_audio(&existing_file, &destination, self.settings.max_file_bytes)?;
                if !same_sha256(&existing.sha256, &audio.original.sha256)
                    || existing.size != audio.original.size
                {
                    bail!(
                        "existing iCloud archive path contains different bytes; preserving it at {}",
                        destination.display()
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                materialize_if_needed(&audio.original.path, &self.settings, None)
                    .context("original audio needs bounded materialization before archiving")?;
                let source = open_verified_audio(&audio.original, self.settings.max_file_bytes)
                    .context("open verified original audio for iCloud archive")?;
                copy_create_only(&source, &destination, &audio.original, &self.settings)?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("inspect iCloud archive path {}", destination.display())
                });
            }
        }

        self.archive_status(&destination)
    }

    pub fn add_track(&self, audio: &PreparedAudio) -> anyhow::Result<String> {
        self.check_music_ready()?;
        // Keep a verified file descriptor open through Music's synchronous add
        // operation so File Provider cannot discard the import bytes mid-call.
        let lease = self.take_or_open_import_lease(&audio.import)?;
        let _source_lease = lease.file;
        let script = build_add_script(&audio.import.path)?;
        let response = execute_music_script(script, "Music.add")?;
        let persistent_id = response.trim().to_ascii_uppercase();
        if !is_persistent_id(&persistent_id) {
            bail!("Music add returned an invalid persistent ID; outcome is unknown");
        }
        Ok(persistent_id)
    }

    fn take_or_open_import_lease(&self, audio: &AudioBytes) -> anyhow::Result<PendingImportLease> {
        let candidate = self
            .pending_import
            .lock()
            .map_err(|_| anyhow::anyhow!("pending import lease lock is poisoned"))?
            .take();
        if let Some(lease) = candidate {
            let same_content = lease.audio.path == audio.path
                && lease.audio.sha256 == audio.sha256
                && lease.audio.size == audio.size;
            let same_identity = file_identity(&lease.file, &audio.path).is_ok_and(|identity| {
                identity.device == lease.identity.device
                    && identity.inode == lease.identity.inode
                    && identity.size == lease.identity.size
            });
            if same_content && same_identity && lease.expires_at > Instant::now() {
                return Ok(lease);
            }
        }
        materialize_if_needed(&audio.path, &self.settings, None)
            .context("Music import needs bounded materialization before add")?;
        let file = open_verified_audio(audio, self.settings.max_file_bytes)
            .context("open verified import bytes for Apple Music")?;
        let identity = file_identity(&file, &audio.path)?;
        Ok(PendingImportLease {
            audio: audio.clone(),
            file,
            identity,
            expires_at: Instant::now() + VERIFIED_COPY_TTL,
        })
    }

    pub fn query_track(&self, persistent_id: &str) -> anyhow::Result<MusicTrack> {
        self.check_music_ready()?;
        let script = build_status_script(persistent_id)?;
        let response = execute_music_script(script, "Music.status")?;
        parse_track_status(persistent_id, &response)
    }

    pub fn verify_copy(&self, audio: &PreparedAudio, track: &MusicTrack) -> anyhow::Result<()> {
        if !is_persistent_id(&track.persistent_id) {
            bail!("Music persistent ID is invalid");
        }
        if audio.import.size == 0
            || audio.import.size > MAX_AUDIO_BYTES
            || !is_sha256(&audio.import.sha256)
        {
            bail!("saved import content identity is invalid");
        }

        let current = self.query_track(&track.persistent_id)?;
        if current.location != track.location {
            bail!("Music track location changed since its persistent ID was queried");
        }
        let music_path = current
            .location
            .as_deref()
            .context("Music track has no readable file location")?;
        ensure_no_symlink_components(music_path, false)?;
        if is_dataless(
            &fs::symlink_metadata(music_path)
                .with_context(|| format!("inspect Music copy {}", music_path.display()))?,
        ) {
            bail!(
                "Music-owned copy is dataless and cannot be verified as a local independent copy"
            );
        }
        let music_file = open_regular_file(music_path, true)?;
        let music_identity = file_identity(&music_file, music_path)?;
        if music_identity.size != audio.import.size {
            bail!("Music-owned copy size does not match the saved import size");
        }

        require_known_path_is_distinct(&audio.original.path, music_path, music_identity)?;
        require_known_path_is_distinct(&audio.import.path, music_path, music_identity)?;
        let archive_root = self.archive_root()?;
        if music_path == archive_root.as_path() || music_path.starts_with(&archive_root) {
            bail!("Music-owned copy resolves inside the iCloud archive folder");
        }
        let digest = hash_open_file(&music_file)?;
        if !same_sha256(&digest, &audio.import.sha256) {
            bail!("Music-owned copy SHA-256 does not match the saved import bytes");
        }
        let after = file_identity(&music_file, music_path)?;
        if after.device != music_identity.device
            || after.inode != music_identity.inode
            || after.size != music_identity.size
        {
            bail!("Music-owned copy identity or size changed while it was verified");
        }
        require_not_ubiquitous(music_path)?;

        let record = VerifiedCopy {
            persistent_id: current.persistent_id,
            path: music_path.to_path_buf(),
            identity: after,
            expires_at: Instant::now() + VERIFIED_COPY_TTL,
        };
        *self
            .verified_copy
            .lock()
            .map_err(|_| anyhow::anyhow!("verified-copy receipt lock is poisoned"))? = Some(record);
        Ok(())
    }

    pub fn archive_status(&self, path: &Path) -> anyhow::Result<ArchiveState> {
        let archive_root = self.archive_root()?;
        validate_archive_child(&archive_root, path)?;
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("inspect archive metadata at {}", path.display()))?;
        let identity = identity_from_metadata(path, &metadata, true)?;
        if identity.size > MAX_AUDIO_BYTES {
            bail!("iCloud archive exceeds the 512 MiB audio limit");
        }
        let dataless = is_dataless(&metadata);
        let cloud = cloud_metadata(path)?;
        let cloud = cloud.context("iCloud archive resource metadata is unavailable")?;
        Ok(ArchiveState {
            path: path.to_path_buf(),
            uploaded: cloud.uploaded,
            uploading: cloud.uploading,
            downloading: cloud.downloading,
            conflicts: cloud.conflicts,
            local_bytes: !dataless && cloud.download_status != DownloadStatus::NotDownloaded,
        })
    }

    pub fn evict_archive(
        &self,
        archive: &ArchiveState,
        track: &MusicTrack,
    ) -> anyhow::Result<ArchiveState> {
        let archive_root = self.archive_root()?;
        validate_archive_child(&archive_root, &archive.path)?;
        let current_state = self.archive_status(&archive.path)?;
        if !current_state.uploaded
            || current_state.uploading
            || current_state.downloading
            || current_state.conflicts
            || !current_state.local_bytes
        {
            return Ok(current_state);
        }

        let metadata = fs::symlink_metadata(&archive.path).with_context(|| {
            format!(
                "inspect archive before eviction at {}",
                archive.path.display()
            )
        })?;
        let identity = identity_from_metadata(&archive.path, &metadata, true)?;
        if is_dataless(&metadata) {
            return Ok(current_state);
        }
        let cloud = cloud_metadata(&archive.path)?
            .context("iCloud archive resource metadata is unavailable before eviction")?;
        if cloud.download_status != DownloadStatus::Current {
            return Ok(current_state);
        }

        let actual_track = self.query_track(&track.persistent_id)?;
        if actual_track.location != track.location || actual_track.location.is_none() {
            bail!("Music track no longer resolves to the previously queried copy");
        }
        if !actual_track.cloud_complete() {
            return Ok(current_state);
        }
        let music_path = actual_track
            .location
            .as_deref()
            .context("Music track has no local copy path")?;
        let receipt = self
            .verified_copy
            .lock()
            .map_err(|_| anyhow::anyhow!("verified-copy receipt lock is poisoned"))?
            .take()
            .filter(|receipt| {
                receipt.expires_at > Instant::now()
                    && receipt.persistent_id == actual_track.persistent_id
                    && receipt.path == music_path
            })
            .context("a recent Music copy verification is required immediately before eviction")?;
        let music_metadata = fs::symlink_metadata(music_path)
            .with_context(|| format!("inspect verified Music copy at {}", music_path.display()))?;
        let music_identity = identity_from_metadata(music_path, &music_metadata, true)?;
        if music_identity.device != receipt.identity.device
            || music_identity.inode != receipt.identity.inode
            || music_identity.size != receipt.identity.size
        {
            bail!("verified Music copy changed before archive eviction");
        }
        if same_object(identity, music_identity) || music_path == archive.path {
            bail!("verified Music copy is not independent of the iCloud archive");
        }
        require_not_ubiquitous(music_path)?;

        let rechecked = self.archive_status(&archive.path)?;
        if !rechecked.uploaded
            || rechecked.uploading
            || rechecked.downloading
            || rechecked.conflicts
            || !rechecked.local_bytes
        {
            return Ok(rechecked);
        }
        let rechecked_cloud = cloud_metadata(&archive.path)?
            .context("iCloud archive resource metadata is unavailable at eviction gate")?;
        if rechecked_cloud.download_status != DownloadStatus::Current {
            return Ok(rechecked);
        }
        let final_metadata = fs::symlink_metadata(&archive.path).with_context(|| {
            format!(
                "recheck archive inode before eviction at {}",
                archive.path.display()
            )
        })?;
        let final_identity = identity_from_metadata(&archive.path, &final_metadata, true)?;
        if is_dataless(&final_metadata)
            || final_identity.device != identity.device
            || final_identity.inode != identity.inode
            || final_identity.size != identity.size
        {
            return Ok(rechecked);
        }

        evict_ubiquitous_item(&archive.path)?;
        // Read only filesystem and iCloud metadata after eviction. Never open,
        // hash, or request a download for the archived content here.
        self.archive_status(&archive.path)
    }

    fn archive_root(&self) -> anyhow::Result<PathBuf> {
        #[cfg(target_os = "macos")]
        {
            let expected =
                crate::music_automation::authenticated_home()?.join(MUSIC_CLOUD_DIRECTORY);
            let configured = &self.settings.archive_dir;
            if configured != &expected {
                bail!(
                    "configured Music archive must be {}; found {}",
                    expected.display(),
                    configured.display()
                );
            }
            Ok(expected)
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = &self.settings.archive_dir;
            bail!("iCloud Music archive operations are supported only on macOS")
        }
    }
}

fn check_cancelled(cancel: &MusicCancellation) -> anyhow::Result<()> {
    if cancel.is_cancelled() {
        bail!("Music audio preparation was cancelled");
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn same_sha256(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn is_persistent_id(value: &str) -> bool {
    value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn inspect_audio_bytes(path: &Path, configured_limit: u64) -> anyhow::Result<AudioBytes> {
    ensure_no_symlink_components(path, false)?;
    let file = open_regular_file(path, true)?;
    inspect_open_audio(&file, path, configured_limit)
}

fn inspect_open_audio(
    file: &File,
    path: &Path,
    configured_limit: u64,
) -> anyhow::Result<AudioBytes> {
    let limit = configured_limit.min(MAX_AUDIO_BYTES);
    let before = file_identity(file, path)?;
    if before.size == 0 || before.size > limit {
        bail!("audio file must contain between 1 byte and {limit} bytes");
    }
    let sha256 = hash_open_file(file)?;
    let after = file_identity(file, path)?;
    if before.device != after.device || before.inode != after.inode || before.size != after.size {
        bail!("audio file object or size changed while hashing");
    }
    Ok(AudioBytes {
        path: path.to_path_buf(),
        sha256,
        size: after.size,
    })
}

fn verify_open_audio(file: &File, audio: &AudioBytes, configured_limit: u64) -> anyhow::Result<()> {
    if !is_sha256(&audio.sha256)
        || audio.size == 0
        || audio.size > configured_limit.min(MAX_AUDIO_BYTES)
    {
        bail!("saved audio content identity is invalid");
    }
    let current = inspect_open_audio(file, &audio.path, configured_limit)?;
    if !same_sha256(&current.sha256, &audio.sha256) || current.size != audio.size {
        bail!("audio bytes differ from their saved SHA-256 and size");
    }
    Ok(())
}

fn open_verified_audio(audio: &AudioBytes, configured_limit: u64) -> anyhow::Result<File> {
    if !is_sha256(&audio.sha256)
        || audio.size == 0
        || audio.size > configured_limit.min(MAX_AUDIO_BYTES)
    {
        bail!("saved audio content identity is invalid");
    }
    let mut file = open_regular_file(&audio.path, true)?;
    let identity = file_identity(&file, &audio.path)?;
    if identity.size != audio.size || !same_sha256(&hash_open_file(&file)?, &audio.sha256) {
        bail!("audio path does not match its saved SHA-256 and size");
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

fn open_regular_file(path: &Path, require_owned: bool) -> anyhow::Result<File> {
    ensure_no_symlink_components(path, false)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path).with_context(|| {
        format!(
            "open regular file {} without following symlinks",
            path.display()
        )
    })?;
    file_identity_owned(&file, path, require_owned)?;
    Ok(file)
}

fn file_identity(file: &File, path: &Path) -> anyhow::Result<FileIdentity> {
    let open_metadata = file
        .metadata()
        .with_context(|| format!("read open file metadata for {}", path.display()))?;
    let path_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("recheck file path metadata for {}", path.display()))?;
    let opened = identity_from_metadata(path, &open_metadata, true)?;
    let current = identity_from_metadata(path, &path_metadata, true)?;
    if opened.device != current.device
        || opened.inode != current.inode
        || opened.size != current.size
    {
        bail!(
            "file path identity or size changed while it was open: {}",
            path.display()
        );
    }
    Ok(opened)
}

fn file_identity_owned(
    file: &File,
    path: &Path,
    require_owned: bool,
) -> anyhow::Result<FileIdentity> {
    let metadata = file
        .metadata()
        .with_context(|| format!("read file metadata for {}", path.display()))?;
    identity_from_metadata(path, &metadata, require_owned)
}

fn identity_from_metadata(
    path: &Path,
    metadata: &fs::Metadata,
    require_owned: bool,
) -> anyhow::Result<FileIdentity> {
    if !metadata.file_type().is_file() {
        bail!(
            "expected a regular file without symlinks: {}",
            path.display()
        );
    }
    if require_owned {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            // User-owned Music library files and Telegram staging files are
            // the only accepted inputs/copies.
            if metadata.uid() != unsafe { libc::getuid() } {
                bail!("file is not owned by the current user: {}", path.display());
            }
        }
    }
    let size = metadata.len();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
            size,
        })
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(FileIdentity {
            device: 0,
            inode: 0,
            size,
        })
    }
}

fn hash_open_file(file: &File) -> anyhow::Result<String> {
    let mut reader = file
        .try_clone()
        .context("clone open file descriptor for hashing")?;
    reader.seek(SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = reader
            .read(&mut buffer)
            .context("read audio bytes for SHA-256")?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn ensure_no_symlink_components(path: &Path, allow_missing_leaf: bool) -> anyhow::Result<()> {
    if !path.is_absolute() {
        bail!("path must be absolute: {}", path.display());
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        bail!(
            "path must not contain dot or parent components: {}",
            path.display()
        );
    }
    let mut current = PathBuf::from("/");
    let components: Vec<_> = path.components().skip(1).collect();
    for (index, component) in components.iter().enumerate() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!(
                    "symlink path component is not allowed: {}",
                    current.display()
                );
            }
            Ok(_) => {}
            Err(error)
                if allow_missing_leaf
                    && error.kind() == std::io::ErrorKind::NotFound
                    && index + 1 == components.len() =>
            {
                return Ok(());
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspect path component {}", current.display()));
            }
        }
    }
    Ok(())
}

fn ensure_directory(path: &Path, create: bool) -> anyhow::Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        bail!("working directory must be an absolute normalized path");
    }
    let mut current = PathBuf::from("/");
    for component in path.components().skip(1) {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!(
                    "working directory contains a symlink component: {}",
                    current.display()
                );
            }
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => bail!(
                "working directory component is not a directory: {}",
                current.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && create => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    let mut builder = fs::DirBuilder::new();
                    builder.mode(0o700);
                    builder.create(&current).with_context(|| {
                        format!("create working directory {}", current.display())
                    })?;
                }
                #[cfg(not(unix))]
                fs::create_dir(&current)
                    .with_context(|| format!("create working directory {}", current.display()))?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("inspect working directory component {}", current.display())
                });
            }
        }
    }
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect directory {}", path.display()))?;
    if !metadata.is_dir() {
        bail!("expected a real directory: {}", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.uid() != unsafe { libc::getuid() } || metadata.mode() & 0o077 != 0 {
            bail!(
                "working directory must be user-owned and private: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn probe_audio(
    ffprobe: &Path,
    path: &Path,
    cancel: &MusicCancellation,
) -> anyhow::Result<ProbeInfo> {
    let output = run_bounded_tool(
        ffprobe,
        &[
            "-v".into(),
            "error".into(),
            "-protocol_whitelist".into(),
            "file,pipe".into(),
            "-show_entries".into(),
            "format=format_name:stream=index,codec_type,codec_name:stream_disposition=attached_pic"
                .into(),
            "-of".into(),
            "json".into(),
            path.as_os_str().to_owned(),
        ],
        AUDIO_TOOL_TIMEOUT,
        cancel,
        TOOL_STDOUT_LIMIT,
        TOOL_STDERR_LIMIT,
    )?;
    let parsed: Value =
        serde_json::from_slice(&output.stdout).context("ffprobe returned invalid bounded JSON")?;
    let streams = parsed
        .get("streams")
        .and_then(Value::as_array)
        .context("ffprobe omitted the stream list")?;
    let mut audio_streams = Vec::new();
    let mut attached_picture_indices = Vec::new();
    for stream in streams {
        match stream.get("codec_type").and_then(Value::as_str) {
            Some("audio") => audio_streams.push(stream),
            Some("video")
                if stream
                    .get("disposition")
                    .and_then(|value| value.get("attached_pic"))
                    .and_then(Value::as_u64)
                    == Some(1) =>
            {
                let index = stream
                    .get("index")
                    .and_then(Value::as_u64)
                    .context("ffprobe omitted attached artwork stream index")?;
                attached_picture_indices.push(index);
            }
            _ => bail!("audio input contains an unsupported non-audio stream"),
        }
    }
    if audio_streams.len() != 1 {
        bail!("audio input must contain exactly one audio stream");
    }
    let format_names = parsed
        .get("format")
        .and_then(|format| format.get("format_name"))
        .and_then(Value::as_str)
        .context("ffprobe omitted the container format")?;
    let format_names: Vec<_> = format_names.split(',').collect();
    let codec = audio_streams[0]
        .get("codec_name")
        .and_then(Value::as_str)
        .context("ffprobe omitted the audio codec")?;
    let extension = path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let kind = match (extension.as_str(), codec) {
        ("m4a", "aac" | "alac") if format_names.contains(&"m4a") => AudioKind::M4a,
        ("mp3", "mp3") if format_names.len() == 1 && format_names[0] == "mp3" => AudioKind::Mp3,
        ("aac", "aac") if format_names.len() == 1 && format_names[0] == "aac" => AudioKind::RawAac,
        ("m4a", _) => bail!("M4A input must contain AAC or ALAC audio"),
        ("mp3", _) => bail!("MP3 input must contain MPEG Layer III audio"),
        ("aac", _) => bail!("raw AAC input must contain AAC audio"),
        _ => bail!("supported audio extensions are .m4a, .mp3, and .aac"),
    };
    Ok(ProbeInfo {
        kind,
        attached_picture_indices,
    })
}

fn decode_audio(ffmpeg: &Path, path: &Path, cancel: &MusicCancellation) -> anyhow::Result<()> {
    run_bounded_tool(
        ffmpeg,
        &[
            "-nostdin".into(),
            "-hide_banner".into(),
            "-loglevel".into(),
            "error".into(),
            "-protocol_whitelist".into(),
            "file,pipe".into(),
            "-i".into(),
            path.as_os_str().to_owned(),
            "-map".into(),
            "0:a:0".into(),
            "-f".into(),
            "null".into(),
            "-".into(),
        ],
        AUDIO_TOOL_TIMEOUT,
        cancel,
        1024,
        TOOL_STDERR_LIMIT,
    )?;
    Ok(())
}

fn remux_aac(
    ffmpeg: &Path,
    source: &Path,
    output: &Path,
    attached_picture_indices: &[u64],
    cancel: &MusicCancellation,
) -> anyhow::Result<()> {
    let mut args: Vec<std::ffi::OsString> = vec![
        "-nostdin".into(),
        "-hide_banner".into(),
        "-loglevel".into(),
        "error".into(),
        "-protocol_whitelist".into(),
        "file,pipe".into(),
        "-i".into(),
        source.as_os_str().to_owned(),
        "-map".into(),
        "0:a:0".into(),
    ];
    for index in attached_picture_indices {
        args.push("-map".into());
        args.push(format!("0:{index}").into());
    }
    args.extend([
        "-map_metadata".into(),
        "0".into(),
        "-map_chapters".into(),
        "0".into(),
        "-c:a".into(),
        "copy".into(),
    ]);
    if !attached_picture_indices.is_empty() {
        args.extend([
            "-c:v".into(),
            "copy".into(),
            "-disposition:v:0".into(),
            "attached_pic".into(),
        ]);
    }
    args.extend([
        "-movflags".into(),
        "+faststart".into(),
        "-f".into(),
        "ipod".into(),
        "-n".into(),
        output.as_os_str().to_owned(),
    ]);
    run_bounded_tool(
        ffmpeg,
        &args,
        AUDIO_TOOL_TIMEOUT,
        cancel,
        1024,
        TOOL_STDERR_LIMIT,
    )?;
    Ok(())
}

fn unique_remux_path(working_dir: &Path, sha256: &str) -> anyhow::Result<PathBuf> {
    for _ in 0..128 {
        let sequence = REMUX_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let stamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let candidate = working_dir.join(format!(
            "aac-remux-{}-{}-{}-{}.m4a",
            &sha256[..16],
            std::process::id(),
            sequence,
            stamp
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => {
                drop(file);
                fs::remove_file(&candidate).with_context(|| {
                    format!("prepare exclusive remux path {}", candidate.display())
                })?;
                return Ok(candidate);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("reserve remux path {}", candidate.display()));
            }
        }
    }
    bail!("could not reserve a unique AAC remux path")
}

fn run_bounded_tool(
    program: &Path,
    args: &[std::ffi::OsString],
    timeout: Duration,
    cancel: &MusicCancellation,
    stdout_limit: usize,
    stderr_limit: usize,
) -> anyhow::Result<ToolOutput> {
    if !program.is_absolute() {
        bail!("media tool path must be absolute: {}", program.display());
    }
    let metadata = fs::metadata(program)
        .with_context(|| format!("inspect media tool {}", program.display()))?;
    if !metadata.is_file() {
        bail!("media tool is not a regular file: {}", program.display());
    }
    check_cancelled(cancel)?;

    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("start media tool {}", program.display()))?;
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        bail!("media-tool stdout pipe unavailable");
    };
    let Some(stderr) = child.stderr.take() else {
        let _ = child.kill();
        let _ = child.wait();
        bail!("media-tool stderr pipe unavailable");
    };
    let stdout_reader = std::thread::spawn(move || read_capped_stream(stdout, stdout_limit));
    let stderr_reader = std::thread::spawn(move || read_capped_stream(stderr, stderr_limit));
    let deadline = Instant::now() + timeout;
    let status = loop {
        if cancel.is_cancelled() {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            bail!("media tool operation was cancelled");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            bail!(
                "media tool {} exceeded its bounded deadline",
                program.display()
            );
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => std::thread::sleep(Duration::from_millis(40)),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(error).context("poll media tool process");
            }
        }
    };
    let stdout = stdout_reader
        .join()
        .map_err(|_| anyhow::anyhow!("media stdout reader panicked"))??;
    let stderr = stderr_reader
        .join()
        .map_err(|_| anyhow::anyhow!("media stderr reader panicked"))??;
    if !status.success() {
        let diagnostic = String::from_utf8_lossy(&stderr).trim().to_owned();
        bail!(
            "media tool {} exited with {status}: {diagnostic}",
            program.display()
        );
    }
    Ok(ToolOutput { stdout })
}

fn read_capped_stream<R: Read>(mut reader: R, limit: usize) -> anyhow::Result<Vec<u8>> {
    let mut retained = Vec::with_capacity(limit.min(4096));
    let mut buffer = [0_u8; 4096];
    let mut overflow = false;
    loop {
        let count = reader
            .read(&mut buffer)
            .context("read bounded media-tool output")?;
        if count == 0 {
            break;
        }
        let remaining = limit.saturating_sub(retained.len());
        let keep = count.min(remaining);
        retained.extend_from_slice(&buffer[..keep]);
        overflow |= keep != count;
    }
    if overflow {
        bail!("media-tool output exceeded its {limit}-byte bound");
    }
    Ok(retained)
}

fn archive_filename(original_name: &str, audio: &AudioBytes) -> anyhow::Result<String> {
    let extension = audio
        .path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .map(str::to_ascii_lowercase)
        .context("original audio filename has no extension")?;
    if !matches!(extension.as_str(), "m4a" | "mp3" | "aac") {
        bail!("original audio extension is unsupported for archiving");
    }
    let stem = Path::new(original_name)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("audio");
    let mut safe = String::with_capacity(stem.len().min(96));
    let mut separator = false;
    for character in stem.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
            if safe.len() < 88 {
                safe.push(character);
            }
            separator = false;
        } else if character.is_ascii_whitespace() && !separator && safe.len() < 88 {
            safe.push('-');
            separator = true;
        }
    }
    let safe = safe.trim_matches(['.', '-', '_']);
    let stem = if safe.is_empty() { "audio" } else { safe };
    if !is_sha256(&audio.sha256) {
        bail!("original audio SHA-256 is invalid");
    }
    let sha256 = audio.sha256.to_ascii_lowercase();
    Ok(format!("{stem}-{sha256}.{extension}"))
}

fn require_known_path_is_distinct(
    source_path: &Path,
    music_path: &Path,
    music_identity: FileIdentity,
) -> anyhow::Result<()> {
    if source_path.as_os_str().is_empty() {
        return Ok(());
    }
    if !source_path.is_absolute() {
        bail!(
            "saved source path must be absolute: {}",
            source_path.display()
        );
    }
    if source_path == music_path {
        bail!("Music copy path is the same as a saved original/import path");
    }
    match fs::symlink_metadata(source_path) {
        Ok(metadata) => {
            ensure_no_symlink_components(source_path, false)?;
            let source_identity = identity_from_metadata(source_path, &metadata, true)?;
            if same_object(source_identity, music_identity) {
                bail!("Music copy is the same filesystem object as a saved original/import path");
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "inspect known source path metadata {}",
                    source_path.display()
                )
            });
        }
    }
    Ok(())
}

fn same_object(left: FileIdentity, right: FileIdentity) -> bool {
    left.device == right.device && left.inode == right.inode
}

fn apple_script_literal(value: &str) -> anyhow::Result<String> {
    if value.contains('\0') {
        bail!("AppleScript path contains a NUL byte");
    }
    let mut literal = String::with_capacity(value.len() + 2);
    literal.push('"');
    for character in value.chars() {
        match character {
            '"' => literal.push_str("\\\""),
            '\\' => literal.push_str("\\\\"),
            character if character.is_control() => {
                bail!("AppleScript path contains a control character");
            }
            character => literal.push(character),
        }
    }
    literal.push('"');
    Ok(literal)
}

fn build_add_script(path: &Path) -> anyhow::Result<String> {
    let path = path
        .to_str()
        .context("Music import path must be valid UTF-8")?;
    let path = apple_script_literal(path)?;
    Ok(format!(
        r#"on run
with timeout of {MUSIC_SCRIPT_TIMEOUT_SECS} seconds
  tell application id "com.apple.Music"
    set addedItem to add (POSIX file {path})
    if class of addedItem is list then
      if (count of addedItem) is not 1 then error "unexpected added track count"
      set addedTrack to item 1 of addedItem
    else if class of addedItem is track or class of addedItem is file track then
      set addedTrack to addedItem
    else
      error "unexpected add result class"
    end if
    set trackPid to persistent ID of addedTrack as text
  end tell
end timeout
return trackPid
end run"#
    ))
}

fn build_status_script(persistent_id: &str) -> anyhow::Result<String> {
    if !is_persistent_id(persistent_id) {
        bail!("invalid Music persistent ID");
    }
    let persistent_id = apple_script_literal(&persistent_id.to_ascii_uppercase())?;
    Ok(format!(
        r#"on run
set trackLocation to ""
set locationErrorNumber to ""
set locationErrorMessage to ""
with timeout of {MUSIC_SCRIPT_TIMEOUT_SECS} seconds
  tell application id "com.apple.Music"
    set fileMatches to (file tracks of library playlist 1 whose persistent ID is {persistent_id})
    if (count of fileMatches) > 1 then error "unexpected file track match count"
    if (count of fileMatches) is 1 then
      set trackItem to item 1 of fileMatches
    else
      set genericMatches to (tracks of library playlist 1 whose persistent ID is {persistent_id})
      if (count of genericMatches) is 0 then return "missing"
      if (count of genericMatches) is not 1 then error "unexpected generic track match count"
      set trackItem to item 1 of genericMatches
    end if
    set trackCloudStatus to cloud status of trackItem as text
    try
      set trackFile to (get location of trackItem)
      set trackLocation to POSIX path of (trackFile as alias)
    on error caughtMessage number caughtNumber
      set trackLocation to ""
      set locationErrorNumber to caughtNumber as text
      set locationErrorMessage to caughtMessage as text
    end try
  end tell
end timeout
return "found" & tab & my safeField(trackCloudStatus) & tab & my safeField(trackLocation) & tab & my safeField(locationErrorNumber) & tab & my safeField(locationErrorMessage)
end run

on safeField(theValue)
  set sourceText to theValue as text
  set cleanText to ""
  repeat with charIndex from 1 to (count characters of sourceText)
    set theCharacter to character charIndex of sourceText
    if theCharacter is tab or theCharacter is return or theCharacter is linefeed then
      set cleanText to cleanText & " "
    else
      set cleanText to cleanText & theCharacter
    end if
  end repeat
  return cleanText
end safeField"#
    ))
}

fn parse_track_status(persistent_id: &str, response: &str) -> anyhow::Result<MusicTrack> {
    let response = response.trim_end_matches(['\r', '\n']);
    let mut fields = response.splitn(5, '\t');
    if fields.next() != Some("found") {
        bail!("Music track was not found by persistent ID");
    }
    let cloud_status = fields
        .next()
        .context("Music.status omitted cloud status")?
        .to_owned();
    let location = fields.next().context("Music.status omitted location")?;
    let location_error_number = fields
        .next()
        .context("Music.status omitted location error number")?;
    let location_error_message = fields
        .next()
        .context("Music.status omitted location error message")?;
    if !location_error_number.is_empty() || !location_error_message.is_empty() {
        bail!("Music did not return a usable track location");
    }
    let location = if location.is_empty() {
        None
    } else {
        Some(PathBuf::from(location))
    };
    Ok(MusicTrack {
        persistent_id: persistent_id.to_ascii_uppercase(),
        location,
        cloud_status,
    })
}

#[cfg(target_os = "macos")]
fn execute_music_script(source: String, stage: &'static str) -> anyhow::Result<String> {
    crate::music_automation::execute_owned_music_script(
        source,
        stage,
        MUSIC_SCRIPT_TIMEOUT_SECS + 5,
    )
}

#[cfg(not(target_os = "macos"))]
fn execute_music_script(_source: String, _stage: &'static str) -> anyhow::Result<String> {
    bail!("Apple Music automation is supported only on macOS")
}

fn copy_create_only(
    source: &File,
    destination: &Path,
    expected: &AudioBytes,
    settings: &MusicSettings,
) -> anyhow::Result<()> {
    ensure_no_symlink_components(destination, true)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut target = options.open(destination).with_context(|| {
        format!(
            "create iCloud archive without overwrite at {}",
            destination.display()
        )
    })?;
    let source_identity = file_identity(source, &expected.path)?;
    if source_identity.size != expected.size {
        bail!("original audio size changed before iCloud archive copy");
    }
    let mut reader = source
        .try_clone()
        .context("clone original audio for archive copy")?;
    reader.seek(SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = reader
            .read(&mut buffer)
            .context("read original for iCloud archive")?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        target
            .write_all(&buffer[..count])
            .context("write create-only iCloud archive copy")?;
        total += count as u64;
        if total > MAX_AUDIO_BYTES {
            bail!("original audio exceeded the 512 MiB archive limit");
        }
    }
    target.sync_all().context("sync iCloud archive copy")?;
    let digest = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if total != expected.size || !same_sha256(&digest, &expected.sha256) {
        bail!("original audio changed while it was copied to the iCloud archive");
    }
    let after_source = file_identity(source, &expected.path)?;
    if after_source.device != source_identity.device
        || after_source.inode != source_identity.inode
        || after_source.size != source_identity.size
    {
        bail!("original audio identity or size changed during archive copy");
    }
    drop(target);
    materialize_if_needed(destination, settings, None)
        .context("new iCloud archive needs bounded materialization before readback")?;
    let archived = inspect_audio_bytes(destination, MAX_AUDIO_BYTES)
        .context("verify completed iCloud archive bytes")?;
    if !same_sha256(&archived.sha256, &expected.sha256) || archived.size != expected.size {
        bail!(
            "iCloud archive verification failed; preserving {}",
            destination.display()
        );
    }
    Ok(())
}

fn validate_archive_child(root: &Path, path: &Path) -> anyhow::Result<()> {
    ensure_no_symlink_components(root, false)?;
    ensure_no_symlink_components(path, false)?;
    if path.parent() != Some(root) {
        bail!(
            "iCloud archive item must be a direct child of {}",
            root.display()
        );
    }
    if !matches!(
        path.extension()
            .and_then(std::ffi::OsStr::to_str)
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("m4a" | "mp3" | "aac")
    ) {
        bail!("iCloud archive item must be .m4a, .mp3, or .aac");
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn ensure_archive_directory(root: &Path) -> anyhow::Result<()> {
    let collection = root
        .parent()
        .context("Music archive path has no Collection parent")?;
    ensure_no_symlink_components(collection, false)?;
    let parent_metadata = fs::symlink_metadata(collection).with_context(|| {
        format!(
            "inspect iCloud Collection directory {}",
            collection.display()
        )
    })?;
    if !parent_metadata.is_dir() {
        bail!("iCloud Collection parent is not a real directory");
    }
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => bail!("iCloud Music archive root is not a real directory"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o755);
                builder
                    .create(root)
                    .with_context(|| format!("create iCloud Music archive {}", root.display()))?;
            }
            #[cfg(not(unix))]
            fs::create_dir(root)
                .with_context(|| format!("create iCloud Music archive {}", root.display()))?;
            Ok(())
        }
        Err(error) => {
            Err(error).with_context(|| format!("inspect iCloud Music archive {}", root.display()))
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn ensure_archive_directory(_root: &Path) -> anyhow::Result<()> {
    bail!("iCloud Music archive operations are supported only on macOS")
}

#[cfg(target_os = "macos")]
fn cloud_metadata(path: &Path) -> anyhow::Result<Option<CloudMetadata>> {
    use objc2::rc::autoreleasepool;
    use objc2_foundation::{
        NSString, NSURL, NSURLIsUbiquitousItemKey, NSURLUbiquitousItemDownloadingStatusKey,
        NSURLUbiquitousItemHasUnresolvedConflictsKey, NSURLUbiquitousItemIsDownloadingKey,
        NSURLUbiquitousItemIsUploadedKey, NSURLUbiquitousItemIsUploadingKey,
    };

    autoreleasepool(|_| {
        let url = NSURL::from_file_path(path)
            .ok_or_else(|| anyhow::anyhow!("path is not a Foundation file URL"))?;
        // SAFETY: These are immutable Foundation URL resource-key constants,
        // defined for the lifetime of the Foundation framework.
        let is_ubiquitous_key = unsafe { NSURLIsUbiquitousItemKey };
        // SAFETY: Same Foundation constant guarantee as above.
        let uploaded_key = unsafe { NSURLUbiquitousItemIsUploadedKey };
        // SAFETY: Same Foundation constant guarantee as above.
        let uploading_key = unsafe { NSURLUbiquitousItemIsUploadingKey };
        // SAFETY: Same Foundation constant guarantee as above.
        let downloading_key = unsafe { NSURLUbiquitousItemIsDownloadingKey };
        // SAFETY: Same Foundation constant guarantee as above.
        let conflicts_key = unsafe { NSURLUbiquitousItemHasUnresolvedConflictsKey };
        // SAFETY: Same Foundation constant guarantee as above.
        let download_status_key = unsafe { NSURLUbiquitousItemDownloadingStatusKey };
        if resource_bool(&url, is_ubiquitous_key)? != Some(true) {
            return Ok(None);
        }
        let uploaded = required_resource_bool(&url, uploaded_key)?;
        let uploading = required_resource_bool(&url, uploading_key)?;
        let downloading = required_resource_bool(&url, downloading_key)?;
        let conflicts = required_resource_bool(&url, conflicts_key)?;
        let status_object = resource_object(&url, download_status_key)?
            .context("iCloud download status resource value is unavailable")?;
        let status = status_object
            .downcast_ref::<NSString>()
            .map(|value| value.to_string())
            .context("iCloud download status resource value is not a string")?;
        Ok(Some(CloudMetadata {
            uploaded,
            uploading,
            downloading,
            conflicts,
            download_status: DownloadStatus::parse(&status),
        }))
    })
}

#[cfg(target_os = "macos")]
fn resource_object(
    url: &objc2_foundation::NSURL,
    key: &objc2_foundation::NSURLResourceKey,
) -> anyhow::Result<Option<objc2::rc::Retained<objc2::runtime::AnyObject>>> {
    let mut value = None;
    // `AnyObject` accepts each Foundation resource-value class; the caller
    // checks the concrete class before reading it.
    unsafe { url.getResourceValue_forKey_error(&mut value, key) }
        .map_err(|error| anyhow::anyhow!("read Foundation resource value: {error:?}"))?;
    Ok(value)
}

#[cfg(target_os = "macos")]
fn resource_bool(
    url: &objc2_foundation::NSURL,
    key: &objc2_foundation::NSURLResourceKey,
) -> anyhow::Result<Option<bool>> {
    let Some(value) = resource_object(url, key)? else {
        return Ok(None);
    };
    value
        .downcast_ref::<objc2_foundation::NSNumber>()
        .map(|number| number.boolValue())
        .map(Some)
        .context("Foundation boolean resource value is not an NSNumber")
}

#[cfg(target_os = "macos")]
fn required_resource_bool(
    url: &objc2_foundation::NSURL,
    key: &objc2_foundation::NSURLResourceKey,
) -> anyhow::Result<bool> {
    resource_bool(url, key)?.context("required iCloud boolean resource value is unavailable")
}

#[cfg(target_os = "macos")]
fn materialize_if_needed(
    path: &Path,
    settings: &MusicSettings,
    cancel: Option<&MusicCancellation>,
) -> anyhow::Result<()> {
    use objc2::rc::autoreleasepool;
    use objc2_foundation::{NSFileManager, NSURL};

    ensure_no_symlink_components(path, false)?;
    let timeout = Duration::from_secs(settings.cloud_poll_seconds.clamp(1, 300));
    let deadline = Instant::now() + timeout;
    let metadata = fs::symlink_metadata(path).with_context(|| {
        format!(
            "inspect File Provider item {} before opening",
            path.display()
        )
    })?;
    let mut cloud = cloud_metadata(path)?;
    if !is_dataless(&metadata)
        && cloud.is_none_or(|value| value.download_status == DownloadStatus::Current)
    {
        return Ok(());
    }

    let url = NSURL::from_file_path(path)
        .ok_or_else(|| anyhow::anyhow!("path is not a Foundation file URL"))?;
    let file_manager = NSFileManager::defaultManager();
    if !autoreleasepool(|_| file_manager.isUbiquitousItemAtURL(&url)) {
        if is_dataless(&metadata) {
            request_fileprovider_materialization(path, timeout, cancel)?;
            let hydrated = fs::symlink_metadata(path)
                .with_context(|| format!("recheck File Provider item {}", path.display()))?;
            if !is_dataless(&hydrated) {
                return Ok(());
            }
        }
        bail!(
            "File Provider item is dataless or stale, but Foundation reports it is not an iCloud ubiquitous item; refusing to open {}",
            path.display()
        );
    }
    autoreleasepool(|_| {
        file_manager
            .startDownloadingUbiquitousItemAtURL_error(&url)
            .map_err(|error| anyhow::anyhow!("request iCloud materialization: {error:?}"))
    })?;

    loop {
        if cancel.is_some_and(MusicCancellation::is_cancelled) {
            bail!("iCloud materialization was cancelled");
        }
        if Instant::now() >= deadline {
            bail!(
                "iCloud item did not become current within {} seconds: {}",
                timeout.as_secs(),
                path.display()
            );
        }
        std::thread::sleep(Duration::from_millis(250));
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("recheck File Provider item {}", path.display()))?;
        cloud = cloud_metadata(path)?;
        if !is_dataless(&metadata)
            && cloud.is_some_and(|value| value.download_status == DownloadStatus::Current)
        {
            return Ok(());
        }
    }
}

#[cfg(target_os = "macos")]
fn request_fileprovider_materialization(
    path: &Path,
    timeout: Duration,
    cancel: Option<&MusicCancellation>,
) -> anyhow::Result<()> {
    use std::sync::mpsc;

    if FILE_PROVIDER_COORDINATION_ACTIVE
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        bail!("a previous bounded File Provider materialization request is still pending");
    }
    let path = path.to_path_buf();
    let (sender, receiver) = mpsc::channel();
    let spawn = std::thread::Builder::new()
        .name("music-fileprovider-materialize".into())
        .spawn(move || {
            struct ActiveReset;
            impl Drop for ActiveReset {
                fn drop(&mut self) {
                    FILE_PROVIDER_COORDINATION_ACTIVE.store(false, Ordering::Release);
                }
            }
            let _active_reset = ActiveReset;
            let provider = crate::file_provider::platform_queue_file_provider();
            let mut accessor = |coordinated_path: &Path| {
                if coordinated_path != path {
                    bail!("File Provider rewrote the coordinated music path");
                }
                let metadata = fs::symlink_metadata(coordinated_path).with_context(|| {
                    format!(
                        "inspect coordinated File Provider item {}",
                        coordinated_path.display()
                    )
                })?;
                if is_dataless(&metadata) {
                    bail!("File Provider coordination returned while the item remained dataless");
                }
                Ok(())
            };
            let result = provider.coordinate_read(&path, &mut accessor);
            let _ = sender.send(result);
        })
        .context("start bounded File Provider materialization request");
    if let Err(error) = spawn {
        FILE_PROVIDER_COORDINATION_ACTIVE.store(false, Ordering::Release);
        return Err(error);
    }

    let deadline = Instant::now() + timeout;
    loop {
        if cancel.is_some_and(MusicCancellation::is_cancelled) {
            bail!("File Provider materialization was cancelled");
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("File Provider materialization exceeded its bounded deadline");
        }
        match receiver.recv_timeout(remaining.min(Duration::from_millis(200))) {
            Ok(result) => return result.context("coordinate File Provider materialization"),
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                bail!("File Provider materialization worker stopped without a result");
            }
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn materialize_if_needed(
    path: &Path,
    _settings: &MusicSettings,
    cancel: Option<&MusicCancellation>,
) -> anyhow::Result<()> {
    if cancel.is_some_and(MusicCancellation::is_cancelled) {
        bail!("Music audio preparation was cancelled");
    }
    ensure_no_symlink_components(path, false)
}

#[cfg(target_os = "macos")]
fn require_not_ubiquitous(path: &Path) -> anyhow::Result<()> {
    use objc2::rc::autoreleasepool;
    use objc2_foundation::{NSFileManager, NSURL, NSURLIsUbiquitousItemKey};

    let url = NSURL::from_file_path(path)
        .ok_or_else(|| anyhow::anyhow!("Music copy path is not a Foundation file URL"))?;
    // SAFETY: Foundation defines this immutable URL resource-key constant for
    // the lifetime of the Foundation framework.
    let ubiquitous_key = unsafe { NSURLIsUbiquitousItemKey };
    let is_ubiquitous = autoreleasepool(|_| -> anyhow::Result<bool> {
        match resource_bool(&url, ubiquitous_key)? {
            Some(is_ubiquitous) => Ok(is_ubiquitous),
            None => Ok(NSFileManager::defaultManager().isUbiquitousItemAtURL(&url)),
        }
    })?;
    if is_ubiquitous {
        bail!("Music-owned copy is reported as ubiquitous");
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn require_not_ubiquitous(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn evict_ubiquitous_item(path: &Path) -> anyhow::Result<()> {
    use objc2::rc::autoreleasepool;
    use objc2_foundation::{NSFileManager, NSURL};

    let url = NSURL::from_file_path(path)
        .ok_or_else(|| anyhow::anyhow!("archive path is not a Foundation file URL"))?;
    let manager = NSFileManager::defaultManager();
    autoreleasepool(|_| {
        manager
            .evictUbiquitousItemAtURL_error(&url)
            .map_err(|error| anyhow::anyhow!("evict uploaded iCloud archive bytes: {error:?}"))
    })
}

#[cfg(not(target_os = "macos"))]
fn evict_ubiquitous_item(_path: &Path) -> anyhow::Result<()> {
    bail!("iCloud archive eviction is supported only on macOS")
}

#[cfg(target_os = "macos")]
fn is_dataless(metadata: &fs::Metadata) -> bool {
    use std::os::macos::fs::MetadataExt;
    metadata.st_flags() & SF_DATALESS != 0
}

#[cfg(not(target_os = "macos"))]
fn is_dataless(_metadata: &fs::Metadata) -> bool {
    false
}

#[cfg(not(target_os = "macos"))]
fn cloud_metadata(_path: &Path) -> anyhow::Result<Option<CloudMetadata>> {
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::atomic::Ordering;

    fn audio(path: &str) -> AudioBytes {
        AudioBytes {
            path: PathBuf::from(path),
            sha256: "0123456789abcdef".repeat(4),
            size: 42,
        }
    }

    #[test]
    fn archive_names_are_sanitized_and_use_full_content_hash() {
        let result =
            archive_filename("../../Artist Track?.mp3", &audio("/tmp/source.mp3")).unwrap();
        assert_eq!(
            result,
            "Artist-Track-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.mp3"
        );
        assert!(!result.contains('/'));
    }

    #[test]
    fn archive_name_is_byte_bounded_for_long_unicode_source_names() {
        let original_name = format!("🎵{}🎵.mp3", "A".repeat(200));
        let result = archive_filename(&original_name, &audio("/tmp/source.mp3")).unwrap();
        assert!(result.is_ascii());
        assert!(
            result.len() <= 157,
            "archive filename is {} bytes",
            result.len()
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn local_regular_file_is_accepted_as_nonubiquitous() {
        let directory = TestDirectory::new();
        let path = directory.path().join("local-music-copy.m4a");
        fs::write(&path, b"controlled local regular-file fixture")
            .expect("write local non-ubiquitous fixture");
        let metadata = fs::symlink_metadata(&path).expect("inspect local fixture");
        assert!(metadata.file_type().is_file());
        assert!(!is_dataless(&metadata));
        require_not_ubiquitous(&path).expect("accept regular non-ubiquitous local file");
    }

    #[test]
    fn fixed_music_scripts_use_posix_file_and_alias_based_location_readback() {
        let add = build_add_script(Path::new("/tmp/artist\\\"track.m4a")).unwrap();
        assert!(add.contains("tell application id \"com.apple.Music\""));
        assert!(add.contains("add (POSIX file \"/tmp/artist\\\\\\\"track.m4a\")"));

        let status = build_status_script("0123456789abcdef").unwrap();
        let get_location = status
            .find("set trackFile to (get location of trackItem)")
            .unwrap();
        let alias_conversion = status
            .find("set trackLocation to POSIX path of (trackFile as alias)")
            .unwrap();
        assert!(get_location < alias_conversion);
        assert!(status.contains("whose persistent ID is \"0123456789ABCDEF\""));
        assert!(!status.contains("POSIX path of (location of trackItem)"));
    }

    #[test]
    fn track_status_parser_keeps_the_pid_and_reports_missing_tracks() {
        let track = parse_track_status(
            "0123456789abcdef",
            "found\tuploaded\t/Users/test/Music/track.m4a\t\t",
        )
        .unwrap();
        assert_eq!(track.persistent_id, "0123456789ABCDEF");
        assert_eq!(track.cloud_status, "uploaded");
        assert_eq!(
            track.location,
            Some(PathBuf::from("/Users/test/Music/track.m4a"))
        );
        assert!(parse_track_status("0123456789abcdef", "missing").is_err());
    }

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let parent = fs::canonicalize(std::env::temp_dir())
                .expect("resolve temporary directory without symlink components");
            for _ in 0..128 {
                let sequence = REMUX_SEQUENCE.fetch_add(1, Ordering::Relaxed);
                let candidate = parent.join(format!(
                    "music-native-test-{}-{sequence}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("system clock is after Unix epoch")
                        .as_nanos()
                ));
                match fs::create_dir(&candidate) {
                    Ok(()) => {
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            fs::set_permissions(&candidate, fs::Permissions::from_mode(0o700))
                                .expect("make test directory private");
                        }
                        return Self(candidate);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(error) => panic!("create test directory: {error}"),
                }
            }
            panic!("could not reserve a unique test directory")
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn find_test_tool(name: &str) -> Option<PathBuf> {
        let search_path = std::env::var_os("PATH")?;
        std::env::split_paths(&search_path)
            .filter(|directory| directory.is_absolute())
            .map(|directory| directory.join(name))
            .find(|candidate| fs::metadata(candidate).is_ok_and(|metadata| metadata.is_file()))
    }

    fn fixture_args(output: &Path, codec_args: &[&str]) -> Vec<OsString> {
        let mut args: Vec<OsString> = [
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=44100:cl=stereo",
            "-t",
            "0.25",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        args.extend(codec_args.iter().map(OsString::from));
        args.push("-n".into());
        args.push(output.as_os_str().to_owned());
        args
    }

    fn generate_fixture(ffmpeg: &Path, output: &Path, codec_args: &[&str]) -> anyhow::Result<()> {
        run_bounded_tool(
            ffmpeg,
            &fixture_args(output, codec_args),
            Duration::from_secs(10),
            &MusicCancellation::default(),
            1024,
            4096,
        )?;
        Ok(())
    }

    fn fixture_settings(ffmpeg: &Path, ffprobe: &Path) -> MusicSettings {
        MusicSettings {
            enabled: true,
            state_dir: PathBuf::new(),
            staging_dir: PathBuf::new(),
            archive_dir: PathBuf::new(),
            credentials_path: PathBuf::new(),
            helper_python: PathBuf::new(),
            transfer_helper: PathBuf::new(),
            ffmpeg: ffmpeg.to_path_buf(),
            ffprobe: ffprobe.to_path_buf(),
            max_file_bytes: MAX_AUDIO_BYTES,
            cloud_poll_seconds: 1,
        }
    }

    fn independently_hash(path: &Path) -> String {
        let mut file = File::open(path).expect("open generated fixture for independent hash");
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            let count = file.read(&mut buffer).expect("read generated fixture");
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    #[test]
    fn prepare_validates_m4a_mp3_and_raw_aac_without_music_or_playback() {
        let Some(ffmpeg) = find_test_tool("ffmpeg") else {
            eprintln!(
                "SKIP: ffmpeg is unavailable; native audio integration fixtures were not generated"
            );
            return;
        };
        let Some(ffprobe) = find_test_tool("ffprobe") else {
            eprintln!(
                "SKIP: ffprobe is unavailable; native audio integration fixtures were not generated"
            );
            return;
        };

        let directory = TestDirectory::new();
        let m4a = directory.path().join("silence.m4a");
        let mp3 = directory.path().join("silence.mp3");
        let aac = directory.path().join("silence.aac");
        match generate_fixture(
            &ffmpeg,
            &m4a,
            &["-c:a", "aac", "-movflags", "+faststart", "-f", "ipod"],
        ) {
            Ok(()) => {}
            Err(error)
                if error.to_string().contains("Unknown encoder")
                    || error.to_string().contains("Encoder not found") =>
            {
                eprintln!(
                    "SKIP: ffmpeg has no AAC encoder; native audio integration cases skipped"
                );
                return;
            }
            Err(error) => panic!("generate AAC M4A silence fixture: {error:#}"),
        }
        generate_fixture(&ffmpeg, &aac, &["-c:a", "aac", "-f", "adts"])
            .expect("generate raw AAC silence fixture");

        let mp3_available = match generate_fixture(
            &ffmpeg,
            &mp3,
            &["-c:a", "libmp3lame", "-q:a", "9", "-f", "mp3"],
        ) {
            Ok(()) => true,
            Err(error)
                if error.to_string().contains("Unknown encoder")
                    || error.to_string().contains("Encoder not found") =>
            {
                eprintln!("SKIP: ffmpeg has no libmp3lame encoder; MP3 prepare case skipped");
                false
            }
            Err(error) => panic!("generate MP3 silence fixture: {error:#}"),
        };

        let settings = fixture_settings(&ffmpeg, &ffprobe);
        let backend = NativeMusicBackend::new(settings);
        let cancellation = MusicCancellation::default();
        let work_dir = directory.path().join("work");

        let prepared_m4a = backend
            .prepare(&m4a, &work_dir, &cancellation)
            .expect("prepare AAC M4A without transcoding");
        assert_eq!(prepared_m4a.original.sha256, independently_hash(&m4a));
        assert_eq!(
            prepared_m4a.original.size,
            fs::metadata(&m4a).unwrap().len()
        );
        assert_eq!(prepared_m4a.import.sha256, prepared_m4a.original.sha256);
        assert_eq!(prepared_m4a.import.size, prepared_m4a.original.size);
        assert_eq!(prepared_m4a.import.path.as_path(), m4a.as_path());

        if mp3_available {
            let prepared = backend
                .prepare(&mp3, &work_dir, &cancellation)
                .expect("prepare MP3 without transcoding");
            assert_eq!(prepared.original.sha256, independently_hash(&mp3));
            assert_eq!(prepared.import.sha256, prepared.original.sha256);
            assert_eq!(prepared.import.size, prepared.original.size);
            assert_eq!(prepared.import.path.as_path(), mp3.as_path());
        }

        let original_aac = fs::read(&aac).expect("read raw AAC before prepare");
        let prepared_aac = backend
            .prepare(&aac, &work_dir, &cancellation)
            .expect("remux raw AAC into M4A for Music");
        let original_aac_after = fs::read(&aac).expect("read raw AAC after prepare");
        assert_eq!(
            original_aac_after, original_aac,
            "original AAC bytes changed"
        );
        assert_eq!(prepared_aac.original.sha256, independently_hash(&aac));
        assert_eq!(
            prepared_aac
                .import
                .path
                .extension()
                .and_then(std::ffi::OsStr::to_str),
            Some("m4a")
        );
        assert_ne!(prepared_aac.import.path, prepared_aac.original.path);
        assert_ne!(prepared_aac.import.sha256, prepared_aac.original.sha256);
        assert_ne!(prepared_aac.import.size, prepared_aac.original.size);

        let corrupt = directory.path().join("corrupt.m4a");
        fs::write(&corrupt, b"not an audio container")
            .expect("write bounded corrupt-audio fixture");
        assert!(
            backend.prepare(&corrupt, &work_dir, &cancellation).is_err(),
            "corrupt audio must fail preparation before any Music or archive method is called"
        );
    }
}
