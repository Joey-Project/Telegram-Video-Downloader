use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[cfg(target_os = "macos")]
use std::ffi::CStr;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;

use crate::downloader::video_output_lock_file;
#[cfg(test)]
use crate::file_provider::platform_queue_file_provider;
use crate::file_provider::{
    QueueFileProvider, classify_deadlock_error, is_file_provider_access_error,
};
use crate::library::{
    BatchMoveResult, BatchStatus, CollectionMetadata, Confidence, ConflictPolicy, FileMovePreview,
    HintOrigin, IntegrityStatus, LegacyCandidate, LegacyImportReport, LibraryAttachment,
    LibraryItem, LibrarySnapshot, MAX_LEGACY_HINTS, MetadataFieldChange, MetadataPatch,
    MetadataPatchPreview, MetadataStatus, MoveOutcome, MovePreview, MovePreviewItem,
    MoveResultItem, OrganizationStatus, PhysicalItemIdentity, Presence, SourceHint,
};
use crate::media_libraries::{TaskTransferControl, TaskTransferEntryKind, TaskTransferFile};
use crate::safe_fs::{
    BoundDirectoryCursor, BoundDirectoryCursorEntry, BoundFile, BoundFileAccessPolicy,
    EntryIdentity, MigrationCopyChunk, RootedFs,
};

const LIBRARY_DIRECTORY: &str = ".telegram-video-downloader-library";
const LIBRARY_INDEX: &str = "index.json";
const LIBRARY_LOCK: &str = "library.lock";
const PREVIEW_PREFIX: &str = "preview-";
const BATCH_PREFIX: &str = "batch-";
const INDEX_QUARANTINE_PREFIX: &str = ".index.corrupt.";
const INDEX_VERSION: u32 = 1;
const INDEX_LIMIT: usize = 16 * 1024 * 1024;
const MAX_INDEX_QUARANTINES: usize = 3;
const MAX_MISSING_INDEX_RECORDS: usize = 1024;
const PREVIEW_LIMIT: usize = 4 * 1024 * 1024;
const MANIFEST_LIMIT: usize = 16 * 1024 * 1024;
const ABANDONED_PREVIEW_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const TERMINAL_BATCH_RECOVERY_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const MAX_OPERATION_STATE_DIRECTORY_NAMES_PER_PASS: usize = 32;
const MAX_OPERATION_STATE_FILES_PER_PASS: usize = 32;
const MAX_OPERATION_STATE_BYTES_PER_PASS: usize = 32 * 1024 * 1024;
const MAX_OPERATION_STATE_CLEANUP_ROOTS: usize = 8;
const SIDE_CAR_LIMIT: usize = 1024 * 1024;
const LEGACY_INPUT_LIMIT: usize = 1024 * 1024;
pub(crate) const MAX_METADATA_TITLE_CHARS: usize = 500;
const MAX_METADATA_TITLE_BYTES: usize = 4096;
const MAX_DEPTH: usize = 48;
const MAX_FILENAME_COMPONENT_BYTES: usize = 255;
const MAX_PATH_COMPONENT_BYTES: usize = MAX_FILENAME_COMPONENT_BYTES;
const MAX_ENTRIES: usize = 200_000;
const MAX_MEDIA_ITEMS: usize = 50_000;
const MAX_PINNED_PREVIEWS: usize = 64;
const MAX_PINNED_FILES_PER_PREVIEW: usize = 512;
const MAX_PINNED_FILES_TOTAL: usize = 4096;
const PREVIEW_ANCHOR_TTL: Duration = Duration::from_secs(60 * 60);
const PREVIEW_ANCHOR_REAPER_INTERVAL: Duration = Duration::from_secs(60);
const FFPROBE_TIMEOUT: Duration = Duration::from_secs(3);
const SCAN_TIMEOUT: Duration = Duration::from_secs(120);
const FFPROBE_OUTPUT_LIMIT: usize = 64 * 1024;
static PRIVATE_TEMP_SERIAL: AtomicU64 = AtomicU64::new(0);
static PROCESS_NONCE: OnceLock<String> = OnceLock::new();
static LIVE_PREVIEW_ANCHORS: OnceLock<Mutex<PreviewAnchorStore>> = OnceLock::new();
static PREVIEW_ANCHOR_REAPER: OnceLock<std::result::Result<(), String>> = OnceLock::new();
static OPERATION_STATE_CLEANUP_CURSORS: OnceLock<Mutex<OperationStateCleanupCursorRegistry>> =
    OnceLock::new();

#[derive(Clone, Debug, PartialEq, Eq)]
struct OperationStateCleanupCursorKey {
    canonical_root: PathBuf,
    library_device: u64,
    library_inode: u64,
}

struct OperationStateCleanupCursorEntry {
    key: OperationStateCleanupCursorKey,
    cursor: Arc<Mutex<Option<BoundDirectoryCursor>>>,
}

#[derive(Default)]
struct OperationStateCleanupCursorRegistry {
    entries: VecDeque<OperationStateCleanupCursorEntry>,
}

impl OperationStateCleanupCursorRegistry {
    fn cursor_for(
        &mut self,
        key: OperationStateCleanupCursorKey,
    ) -> Arc<Mutex<Option<BoundDirectoryCursor>>> {
        if let Some(index) = self.entries.iter().position(|entry| entry.key == key) {
            let entry = self
                .entries
                .remove(index)
                .expect("cursor registry position was found");
            let cursor = Arc::clone(&entry.cursor);
            self.entries.push_back(entry);
            return cursor;
        }
        if self.entries.len() >= MAX_OPERATION_STATE_CLEANUP_ROOTS {
            self.entries.pop_front();
        }
        let cursor = Arc::new(Mutex::new(None));
        self.entries.push_back(OperationStateCleanupCursorEntry {
            key,
            cursor: Arc::clone(&cursor),
        });
        cursor
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OperationStateFileKind {
    Preview,
    Batch,
}

struct OperationStateCleanupCandidate {
    entry: crate::safe_fs::BoundEntry,
    identity: EntryIdentity,
    file: BoundFile,
    digest: String,
    bytes: Vec<u8>,
    byte_len: usize,
    modified_seconds: i64,
}

enum OperationStateCandidateRead {
    Missing,
    Deferred,
    Ready(Box<OperationStateCleanupCandidate>),
}

#[derive(Default)]
struct PreviewAnchorStore {
    by_preview: HashMap<String, PreviewAnchorEntry>,
    order: VecDeque<String>,
    file_count: usize,
}

struct PreviewAnchorEntry {
    revision: String,
    anchors: HashMap<String, BoundFile>,
    expires_at: Instant,
}

#[derive(Default)]
struct TargetReservationSet {
    keys: HashSet<TargetReservationKey>,
    semantics_by_directory: HashMap<(u64, u64), TargetNameSemantics>,
    private_library_identity: Option<EntryIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TargetReservationKey {
    directory_device: u64,
    directory_inode: u64,
    components: Vec<String>,
}

struct ResolvedTargetParent {
    relative_path: PathBuf,
    identity: EntryIdentity,
    unresolved_parent_components: Vec<String>,
    target_name: String,
}

// These keys only prevent preview-time collisions; they are not persisted as filesystem identity.
// Unknown or unsupported name-comparison rules fail closed instead of being guessed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TargetNameSemantics {
    normalization_insensitive: bool,
    case_insensitive: bool,
    hfs_ignorable: bool,
}

impl TargetReservationSet {
    fn contains(&mut self, root: &RootedFs, target: &str) -> Result<bool> {
        let key = self.key_for(root, target)?;
        Ok(self.keys.contains(&key))
    }

    fn insert(&mut self, root: &RootedFs, target: &str) -> Result<()> {
        let key = self.key_for(root, target)?;
        self.keys.insert(key);
        Ok(())
    }

    fn key_for(&mut self, root: &RootedFs, target: &str) -> Result<TargetReservationKey> {
        let resolved = resolve_target_parent(root, target, self.private_library_identity)?;
        let directory_key = (resolved.identity.device(), resolved.identity.inode());
        let semantics = if let Some(semantics) = self.semantics_by_directory.get(&directory_key) {
            *semantics
        } else {
            let directory =
                open_validated_target_directory(root, &resolved.relative_path, resolved.identity)?;
            let semantics = target_name_semantics(&directory)?;
            self.semantics_by_directory.insert(directory_key, semantics);
            semantics
        };

        let mut names = resolved.unresolved_parent_components;
        names.push(resolved.target_name);
        build_target_reservation_key(resolved.identity, &names, semantics)
    }
}

fn resolve_target_parent(
    root: &RootedFs,
    target: &str,
    private_library_identity: Option<EntryIdentity>,
) -> Result<ResolvedTargetParent> {
    let relative = safe_relative_path(target, false)?;
    validate_not_private_downloader_path_with_identity(root, &relative, private_library_identity)?;
    let components = relative
        .components()
        .map(|component| match component {
            Component::Normal(name) => name
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("target filename is not valid UTF-8")),
            _ => bail!("target path contains an invalid component"),
        })
        .collect::<Result<Vec<_>>>()?;
    let (target_name, parent_components) = components
        .split_last()
        .ok_or_else(|| anyhow!("target path does not name a file"))?;

    let mut relative_parent = PathBuf::new();
    let mut parent_identity = root.root_identity();
    let mut unresolved_at = parent_components.len();
    for (index, component) in parent_components.iter().enumerate() {
        let candidate = relative_parent.join(component);
        let absolute = root.logical_root_path().join(&candidate);
        match root.entry_identity(&absolute)? {
            Some(identity) if identity.is_dir() => {
                relative_parent = candidate;
                parent_identity = identity;
            }
            Some(_) => bail!("target path parent is not a directory"),
            None => {
                unresolved_at = index;
                break;
            }
        }
    }

    if relative_parent.as_os_str().is_empty() {
        if root.root_identity() != parent_identity {
            bail!("target root directory identity changed during preview");
        }
    } else {
        let parent_path = root.logical_root_path().join(&relative_parent);
        let entry = root.bind_entry(&parent_path, false)?;
        root.open_bound_directory(&entry, parent_identity)?
            .validate_identity()?;
    }

    Ok(ResolvedTargetParent {
        relative_path: relative_parent,
        identity: parent_identity,
        unresolved_parent_components: parent_components[unresolved_at..].to_vec(),
        target_name: target_name.clone(),
    })
}

fn ensure_same_device_for_rename(
    source_path: &str,
    target_path: &str,
    source_device: u64,
    target_device: u64,
) -> Result<()> {
    if source_device != target_device {
        bail!(
            "cross-device moves are unsupported: source {source_path} and target {target_path} are on different devices"
        );
    }
    Ok(())
}

fn build_target_reservation_key(
    directory: EntryIdentity,
    names: &[String],
    semantics: TargetNameSemantics,
) -> Result<TargetReservationKey> {
    build_target_reservation_key_with(directory, names, semantics, platform_case_fold)
}

fn build_target_reservation_key_with<F>(
    directory: EntryIdentity,
    names: &[String],
    semantics: TargetNameSemantics,
    case_fold: F,
) -> Result<TargetReservationKey>
where
    F: Fn(&str) -> Result<String>,
{
    Ok(TargetReservationKey {
        directory_device: directory.device(),
        directory_inode: directory.inode(),
        components: names
            .iter()
            .map(|name| target_component_key_with(name, semantics, |value| case_fold(value)))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn open_validated_target_directory(
    root: &RootedFs,
    relative: &Path,
    expected: EntryIdentity,
) -> Result<File> {
    let path = root.logical_root_path().join(relative);
    let before = if relative.as_os_str().is_empty() {
        root.root_identity()
    } else {
        root.entry_identity(&path)?
            .ok_or_else(|| anyhow!("target parent directory disappeared"))?
    };
    if before != expected || !expected.is_dir() {
        bail!("target parent directory identity changed");
    }
    let directory = File::open(&path).with_context(|| {
        format!(
            "failed to inspect target parent directory {}",
            path.display()
        )
    })?;
    let metadata = directory
        .metadata()
        .context("failed to inspect opened target parent directory")?;
    if !metadata.is_dir()
        || metadata.dev() != expected.device()
        || metadata.ino() != expected.inode()
    {
        bail!("target parent directory changed while reading name semantics");
    }
    let after = if relative.as_os_str().is_empty() {
        root.root_identity()
    } else {
        root.entry_identity(&path)?
            .ok_or_else(|| anyhow!("target parent directory disappeared"))?
    };
    if after != expected {
        bail!("target parent directory changed while reading name semantics");
    }
    Ok(directory)
}

fn target_component_key_with<F>(
    name: &str,
    semantics: TargetNameSemantics,
    case_fold: F,
) -> Result<String>
where
    F: FnOnce(&str) -> Result<String>,
{
    let normalized = if semantics.normalization_insensitive {
        name.nfd().collect::<String>()
    } else {
        name.to_owned()
    };
    let folded = if semantics.case_insensitive {
        case_fold(&normalized)?
    } else {
        normalized
    };
    let mut key = if semantics.normalization_insensitive {
        folded.nfd().collect::<String>()
    } else {
        folded
    };
    if semantics.hfs_ignorable {
        key.retain(|character| !is_hfs_ignorable(character));
    }
    Ok(key)
}

fn is_private_downloader_component_with<F>(
    name: &str,
    semantics: TargetNameSemantics,
    case_fold: &F,
) -> Result<bool>
where
    F: Fn(&str) -> Result<String>,
{
    let key = target_component_key_with(name, semantics, |value| case_fold(value))?;
    let prefix = target_component_key_with(".telegram-video-downloader-", semantics, |value| {
        case_fold(value)
    })?;
    Ok(key.starts_with(&prefix))
}

fn is_private_downloader_component(name: &str, semantics: TargetNameSemantics) -> Result<bool> {
    is_private_downloader_component_with(name, semantics, &platform_case_fold)
}

pub(crate) fn validate_not_private_downloader_path(root: &RootedFs, relative: &Path) -> Result<()> {
    validate_not_private_downloader_path_with_identity(root, relative, None)
}

fn validate_not_private_downloader_path_with_identity(
    root: &RootedFs,
    relative: &Path,
    private_library_identity: Option<EntryIdentity>,
) -> Result<()> {
    let mut parent = PathBuf::new();
    let mut parent_identity = root.root_identity();
    let mut components = relative.components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            bail!("relative path contains an invalid component");
        };
        let name_text = name
            .to_str()
            .ok_or_else(|| anyhow!("relative path is not UTF-8"))?;
        let directory = open_validated_target_directory(root, &parent, parent_identity)?;
        let semantics = target_name_semantics(&directory).with_context(|| {
            format!("cannot determine name semantics under {}", parent.display())
        })?;
        if is_private_downloader_component(name_text, semantics)? {
            bail!("relative path targets a private downloader entry");
        }

        let candidate = parent.join(name);
        let candidate_absolute = root.logical_root_path().join(&candidate);
        let identity = root.entry_identity(&candidate_absolute)?;
        if identity.is_some_and(|identity| Some(identity) == private_library_identity) {
            bail!("relative path resolves to the private media-library directory");
        }
        match identity {
            Some(identity) if components.peek().is_some() => {
                if !identity.is_dir() {
                    bail!("relative path parent is not a directory");
                }
                parent = candidate;
                parent_identity = identity;
            }
            Some(_) => return Ok(()),
            None => {
                for remaining in components {
                    let Component::Normal(name) = remaining else {
                        bail!("relative path contains an invalid component");
                    };
                    let name = name
                        .to_str()
                        .ok_or_else(|| anyhow!("relative path is not UTF-8"))?;
                    if is_private_downloader_component(name, semantics)? {
                        bail!("relative path targets a private downloader entry");
                    }
                }
                return Ok(());
            }
        }
    }
    Ok(())
}

fn is_hfs_ignorable(character: char) -> bool {
    matches!(
        character as u32,
        0x200c..=0x200f | 0x202a..=0x202e | 0x206a..=0x206f | 0xfeff
    )
}

#[cfg(target_os = "macos")]
fn target_name_semantics(directory: &File) -> Result<TargetNameSemantics> {
    use std::ffi::c_void;
    use std::mem::MaybeUninit;

    #[repr(C)]
    struct VolumeCapabilitiesOutput {
        length: u32,
        capabilities: libc::vol_capabilities_attr_t,
    }

    let mut filesystem = MaybeUninit::<libc::statfs>::zeroed();
    // SAFETY: `filesystem` points to writable storage for `libc::statfs`.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), filesystem.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to identify target filesystem");
    }
    // SAFETY: fstatfs initialized the full structure on success.
    let filesystem = unsafe { filesystem.assume_init() };
    // SAFETY: f_fstypename is a fixed-size nul-terminated field from fstatfs.
    let filesystem_name = unsafe { CStr::from_ptr(filesystem.f_fstypename.as_ptr()) }
        .to_str()
        .context("target filesystem name is not valid UTF-8")?;
    if !matches!(filesystem_name, "apfs" | "hfs") {
        bail!("target filesystem name semantics are unsupported: {filesystem_name}");
    }

    let mut attributes = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: 0,
        volattr: libc::ATTR_VOL_CAPABILITIES,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    let mut output = MaybeUninit::<VolumeCapabilitiesOutput>::zeroed();
    // SAFETY: both pointers reference writable/initialized ABI-compatible structures, and the
    // output buffer has exactly the size described by `VolumeCapabilitiesOutput`.
    if unsafe {
        libc::fgetattrlist(
            directory.as_raw_fd(),
            (&mut attributes as *mut libc::attrlist).cast::<c_void>(),
            output.as_mut_ptr().cast::<c_void>(),
            std::mem::size_of::<VolumeCapabilitiesOutput>(),
            0,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error())
            .context("failed to read target filesystem name capabilities");
    }
    // SAFETY: fgetattrlist initialized the result structure on success.
    let output = unsafe { output.assume_init() };
    if (output.length as usize) < std::mem::size_of::<VolumeCapabilitiesOutput>() {
        bail!("target filesystem returned incomplete name capabilities");
    }
    let sensitive_bit = libc::VOL_CAP_FMT_CASE_SENSITIVE;
    if output.capabilities.valid[0] & sensitive_bit == 0 {
        bail!("target filesystem did not report whether names are case-sensitive");
    }
    let case_insensitive = output.capabilities.capabilities[0] & sensitive_bit == 0;
    Ok(TargetNameSemantics {
        normalization_insensitive: true,
        case_insensitive,
        // HFS uses additional ignorable characters; this conservative public-range subset does
        // not claim to reproduce the complete historical HFS collation table.
        hfs_ignorable: filesystem_name == "hfs" && case_insensitive,
    })
}

#[cfg(target_os = "linux")]
fn target_name_semantics(directory: &File) -> Result<TargetNameSemantics> {
    let mut filesystem = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    // SAFETY: `filesystem` points to writable storage for `libc::statfs`.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), filesystem.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to identify target filesystem");
    }
    // SAFETY: fstatfs initialized the full structure on success.
    let filesystem = unsafe { filesystem.assume_init() };
    let filesystem_type = filesystem.f_type as u64;
    if !matches!(filesystem_type, 0xef53 | 0x0102_1994) {
        bail!("target filesystem name semantics are unsupported (type {filesystem_type:#x})");
    }
    let flags = rustix::fs::ioctl_getflags(directory)
        .context("failed to inspect Linux directory name flags")?;
    linux_target_name_semantics(filesystem_type, flags.bits())
}

#[cfg(any(target_os = "linux", test))]
fn linux_target_name_semantics(
    filesystem_type: u64,
    directory_flags: u32,
) -> Result<TargetNameSemantics> {
    const EXT4_SUPER_MAGIC: u64 = 0xef53;
    const TMPFS_MAGIC: u64 = 0x0102_1994;
    const FS_CASEFOLD_FL: u32 = 0x4000_0000;

    if !matches!(filesystem_type, EXT4_SUPER_MAGIC | TMPFS_MAGIC) {
        bail!("target filesystem name semantics are unsupported (type {filesystem_type:#x})");
    }
    if directory_flags & FS_CASEFOLD_FL != 0 {
        bail!("case-folded Linux target directories are not supported safely");
    }
    Ok(TargetNameSemantics {
        normalization_insensitive: false,
        case_insensitive: false,
        hfs_ignorable: false,
    })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn target_name_semantics(_directory: &File) -> Result<TargetNameSemantics> {
    bail!("target filesystem name semantics are unsupported on this platform");
}

#[cfg(target_os = "macos")]
fn platform_case_fold(value: &str) -> Result<String> {
    use std::ffi::c_void;
    use std::ptr;

    #[repr(C)]
    struct CfRange {
        location: i64,
        length: i64,
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFStringCreateWithCharacters(
            allocator: *const c_void,
            characters: *const u16,
            count: i64,
        ) -> *const c_void;
        fn CFStringCreateMutableCopy(
            allocator: *const c_void,
            max_length: i64,
            string: *const c_void,
        ) -> *mut c_void;
        fn CFStringFold(string: *mut c_void, flags: u32, locale: *const c_void);
        fn CFStringGetLength(string: *const c_void) -> i64;
        fn CFStringGetCharacters(string: *const c_void, range: CfRange, buffer: *mut u16);
        fn CFRelease(value: *const c_void);
    }

    const CF_COMPARE_CASE_INSENSITIVE: u32 = 1;
    let characters = value.encode_utf16().collect::<Vec<_>>();
    // SAFETY: the UTF-16 slice remains alive for the duration of CFStringCreateWithCharacters.
    let source = unsafe {
        CFStringCreateWithCharacters(ptr::null(), characters.as_ptr(), characters.len() as i64)
    };
    if source.is_null() {
        bail!("CoreFoundation could not allocate a target name");
    }
    // SAFETY: `source` is a valid CFString retained by this function.
    let folded = unsafe { CFStringCreateMutableCopy(ptr::null(), 0, source) };
    // SAFETY: release the +1 source reference after making the mutable copy.
    unsafe { CFRelease(source) };
    if folded.is_null() {
        bail!("CoreFoundation could not allocate a folded target name");
    }
    let result = (|| {
        // SAFETY: `folded` is a valid mutable CFString; NULL selects the canonical locale.
        unsafe { CFStringFold(folded, CF_COMPARE_CASE_INSENSITIVE, ptr::null()) };
        // SAFETY: `folded` remains valid until the release below.
        let length = unsafe { CFStringGetLength(folded) };
        let length =
            usize::try_from(length).context("CoreFoundation returned an invalid length")?;
        let mut output = vec![0u16; length];
        // SAFETY: output has exactly the UTF-16 code-unit capacity returned by CFStringGetLength.
        unsafe {
            CFStringGetCharacters(
                folded,
                CfRange {
                    location: 0,
                    length: length as i64,
                },
                output.as_mut_ptr(),
            )
        };
        String::from_utf16(&output).context("CoreFoundation returned invalid UTF-16")
    })();
    // SAFETY: release the +1 mutable copy on both success and error paths.
    unsafe { CFRelease(folded) };
    result
}

#[cfg(not(target_os = "macos"))]
fn platform_case_fold(_value: &str) -> Result<String> {
    bail!("case-insensitive target names are unsupported on this platform");
}

impl PreviewAnchorStore {
    fn remove(&mut self, preview_id: &str) {
        if let Some(entry) = self.by_preview.remove(preview_id) {
            self.file_count = self.file_count.saturating_sub(entry.anchors.len());
        }
        self.order.retain(|id| id != preview_id);
    }

    fn prune_expired(&mut self, now: Instant) {
        let expired = self
            .by_preview
            .iter()
            .filter_map(|(preview_id, entry)| {
                (entry.expires_at <= now).then_some(preview_id.clone())
            })
            .collect::<Vec<_>>();
        for preview_id in expired {
            self.remove(&preview_id);
        }
    }

    fn touch(&mut self, preview_id: &str, now: Instant) -> Option<&HashMap<String, BoundFile>> {
        let entry = self.by_preview.get_mut(preview_id)?;
        entry.expires_at = now + PREVIEW_ANCHOR_TTL;
        self.order.retain(|id| id != preview_id);
        self.order.push_back(preview_id.to_string());
        self.by_preview.get(preview_id).map(|entry| &entry.anchors)
    }

    fn touch_for_revision(
        &mut self,
        preview_id: &str,
        revision: &str,
        now: Instant,
    ) -> Option<&HashMap<String, BoundFile>> {
        if self
            .by_preview
            .get(preview_id)
            .is_none_or(|entry| entry.revision != revision)
        {
            return None;
        }
        self.touch(preview_id, now)
    }

    fn remove_if_revision(&mut self, preview_id: &str, revision: &str) -> bool {
        if self
            .by_preview
            .get(preview_id)
            .is_none_or(|entry| entry.revision != revision)
        {
            return false;
        }
        self.remove(preview_id);
        true
    }

    fn matches_required_anchors(
        &self,
        preview_id: &str,
        revision: &str,
        required: &HashSet<String>,
    ) -> Result<bool> {
        if required.is_empty() {
            return Ok(true);
        }
        let Some(entry) = self.by_preview.get(preview_id) else {
            return Ok(false);
        };
        if entry.revision != revision {
            return Ok(false);
        }
        for token in required {
            let Some(anchor) = entry.anchors.get(token) else {
                return Ok(false);
            };
            let expected = decode_identity_token(token)?;
            let actual = anchor.identity();
            if actual.device() != expected.device
                || actual.inode() != expected.inode
                || !actual.is_file()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn make_room_for(&mut self, preview_id: &str, file_count: usize) -> Result<()> {
        self.remove(preview_id);
        if file_count > MAX_PINNED_FILES_PER_PREVIEW || file_count > MAX_PINNED_FILES_TOTAL {
            bail!("preview exceeds the in-process identity-anchor capacity");
        }
        while self.by_preview.len() >= MAX_PINNED_PREVIEWS
            || self.file_count.saturating_add(file_count) > MAX_PINNED_FILES_TOTAL
        {
            let Some(oldest) = self.order.front().cloned() else {
                bail!("preview identity-anchor capacity could not be reclaimed");
            };
            self.remove(&oldest);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct PreviewBuildRequest<'a> {
    pub(crate) item_ids: &'a [String],
    pub(crate) target_relative_dir: Option<&'a str>,
    pub(crate) rename: bool,
    pub(crate) conflict: ConflictPolicy,
    pub(crate) patches: &'a [MetadataPatch],
}

pub struct LibraryManager {
    root: RootedFs,
    provider: Arc<dyn QueueFileProvider>,
    library_dir: PathBuf,
    library_dir_identity: EntryIdentity,
    preview_cleanup_gate: Option<Arc<dyn PreviewCleanupGate>>,
    in_process_lock: Arc<Mutex<()>>,
    last_snapshot: Arc<Mutex<Option<LibrarySnapshot>>>,
    process_nonce: String,
    #[cfg(test)]
    scan_test_hooks: Arc<Mutex<ScanTestHooks>>,
    #[cfg(test)]
    preview_creation_time_unavailable: AtomicBool,
    #[cfg(test)]
    scan_attempts: AtomicUsize,
}

/// Exact proof for a private durable migration state file. The access policy is always
/// revalidated as owner-private mode 0600 with one link when applying this proof.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct MigrationStateFileProof {
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) size_bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug)]
pub(crate) struct MigrationStateSnapshot {
    pub(crate) proof: MigrationStateFileProof,
    pub(crate) contents: Vec<u8>,
}

pub(crate) enum PreviewCleanupOutcome {
    Removed,
    Retained,
    Deferred,
    NotRemoved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LegacyTargetVerification {
    Missing,
    Mismatch(PhysicalItemIdentity),
    Match(PhysicalItemIdentity),
}

/// Coordinates preview cleanup with durable Cloud confirmation receipt updates.
pub(crate) trait PreviewCleanupGate: Send + Sync {
    fn remove_preview_if_unconfirmed(
        &self,
        preview_id: &str,
        remove: &mut dyn FnMut() -> Result<bool>,
    ) -> Result<PreviewCleanupOutcome>;
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct LibraryIndex {
    version: u32,
    #[serde(default)]
    records: BTreeMap<String, IndexRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexRecord {
    item: LibraryItem,
    device: u64,
    inode: u64,
    file_type: String,
    #[serde(default)]
    birth_seconds: Option<i64>,
    #[serde(default)]
    birth_nanoseconds: Option<i64>,
    #[serde(default)]
    missing_since: Option<u64>,
    #[serde(default)]
    queue_metadata: QueueMetadataProvenance,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct QueueMetadataProvenance {
    title: Option<String>,
    source_ids: Vec<String>,
    collection: Option<CollectionMetadata>,
    requires_confirmation: bool,
}

struct BuiltLibraryItem {
    item: LibraryItem,
    sidecar_title_present: bool,
    authoritative_nfo_unreadable: bool,
    fingerprint: Option<FileFingerprint>,
}

struct ScanBuildState<'a> {
    started_at: Instant,
    timed_out: &'a mut bool,
    migration_incomplete_paths: &'a mut BTreeSet<String>,
    warnings: &'a mut Vec<String>,
    expire_after_attachment_count: &'a mut Option<usize>,
}

#[cfg(test)]
#[derive(Default)]
struct ScanTestHooks {
    incomplete_directories: BTreeSet<String>,
    max_entries: Option<usize>,
    max_media_items: Option<usize>,
    scope_unknown: bool,
    expire_after_built_item: bool,
    expire_after_attachment_count: Option<usize>,
    index_budget: Option<usize>,
    identity_reconciliation_steps: Option<Arc<AtomicUsize>>,
}

#[cfg(test)]
fn count_identity_reconciliation_step(steps: Option<&Arc<AtomicUsize>>) {
    if let Some(steps) = steps {
        steps.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HintPathResolution {
    Item(usize),
    PresentUnmatched,
    ConfirmedAbsent,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HintPathLookup {
    Entry(EntryIdentity),
    Missing,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPreview {
    version: u32,
    process_nonce: String,
    preview: MovePreview,
    patches: Vec<PreparedNfoPatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchManifest {
    version: u32,
    preview: MovePreview,
    status: BatchStatus,
    /// Becomes true only after the final result was durably written. Older manifests default to
    /// false so cleanup retains records whose execution outcome cannot be proven terminal.
    #[serde(default)]
    attempt_finished: bool,
    #[serde(default)]
    retryable_access_error: bool,
    files: Vec<ManifestFile>,
    patches: Vec<PreparedNfoPatch>,
    items: Vec<MoveResultItem>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PreparedNfoPatch {
    patch: MetadataPatch,
    target_path: String,
    media_target_path: String,
    media_identity: String,
    expected_identity: Option<String>,
    expected_sha256: Option<String>,
    expected_mode: Option<u16>,
    after_sha256: String,
    replacement: Vec<u8>,
    #[serde(default)]
    replacement_checkpoint: Option<NfoReplacementCheckpoint>,
    state: ManifestPatchState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NfoReplacementCheckpoint {
    staged_path: String,
    after_identity: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ManifestPatchState {
    Pending,
    Applied,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestFile {
    item_id: String,
    source_path: String,
    target_path: String,
    identity: String,
    state: ManifestFileState,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ManifestFileState {
    Pending,
    Moved,
    Skipped,
}

#[derive(Debug, Clone)]
struct ScannedFile {
    relative_path: String,
    identity: EntryIdentity,
}

#[derive(Debug, Default)]
struct SidecarIndex {
    by_media: HashMap<String, Vec<String>>,
    authoritative_nfo_by_media: HashMap<String, String>,
    ambiguous_count: usize,
}

#[derive(Debug)]
struct PrimaryStemOwner {
    relative_path: String,
    count: usize,
}

struct IndexQuarantineCandidate {
    path: PathBuf,
    identity: EntryIdentity,
    timestamp: u128,
    serial: u64,
}

struct IndexFileRead {
    digest: String,
    contents: Vec<u8>,
    file: BoundFile,
}

struct BoundFileRead {
    token: String,
    digest: String,
    contents: Vec<u8>,
    file: BoundFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UnobservedRecordStatus {
    presence: Presence,
    metadata_status: Option<MetadataStatus>,
}

#[derive(Debug, Clone)]
struct ScanWalk {
    files: Vec<ScannedFile>,
    complete_directories: BTreeSet<String>,
    incomplete_directories: BTreeSet<String>,
    root_categories: BTreeSet<String>,
    directory_name_semantics: HashMap<String, TargetNameSemantics>,
    warnings: Vec<String>,
    started_at: Instant,
    timed_out: bool,
    incomplete_scan: bool,
    migration_incomplete_paths: BTreeSet<String>,
    scope_unknown: bool,
    max_entries: usize,
    max_media_items: usize,
}

impl Default for ScanWalk {
    fn default() -> Self {
        Self {
            files: Vec::new(),
            complete_directories: BTreeSet::new(),
            incomplete_directories: BTreeSet::new(),
            root_categories: BTreeSet::new(),
            directory_name_semantics: HashMap::new(),
            warnings: Vec::new(),
            started_at: Instant::now(),
            timed_out: false,
            incomplete_scan: false,
            migration_incomplete_paths: BTreeSet::new(),
            scope_unknown: false,
            max_entries: MAX_ENTRIES,
            max_media_items: MAX_MEDIA_ITEMS,
        }
    }
}

#[derive(Debug, Clone)]
struct FileFingerprint {
    identity: EntryIdentity,
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    birth_seconds: Option<i64>,
    birth_nanoseconds: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MigrationFileProof {
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) size: u64,
    pub(crate) mode: u32,
    pub(crate) owner: u32,
    pub(crate) group: u32,
    pub(crate) link_count: u64,
    pub(crate) birth_seconds: Option<i64>,
    pub(crate) birth_nanoseconds: Option<i64>,
    pub(crate) sha256: String,
}

impl MigrationFileProof {
    pub(crate) fn entry_identity(&self) -> EntryIdentity {
        EntryIdentity::regular_file(self.device, self.inode)
    }

    pub(crate) fn access_policy(&self) -> BoundFileAccessPolicy {
        BoundFileAccessPolicy {
            owner: self.owner,
            group: self.group,
            mode: self.mode,
            link_count: self.link_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MigrationPlannedFile {
    pub(crate) source_path: String,
    pub(crate) target_path: String,
    pub(crate) proof: MigrationFileProof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MigrationPlanOutcome {
    Move,
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MigrationPlannedGroup {
    pub(crate) item_id: String,
    pub(crate) source_path: String,
    pub(crate) target_path: String,
    pub(crate) outcome: MigrationPlanOutcome,
    pub(crate) conflict: bool,
    pub(crate) files: Vec<MigrationPlannedFile>,
}

/// Completeness proof for a migration scan. This stays internal so public snapshot JSON is stable.
#[derive(Debug, Clone)]
pub(crate) struct MigrationScanResult {
    pub(crate) snapshot: LibrarySnapshot,
    pub(crate) complete: bool,
    /// Relative directory boundaries plus individual unreadable paths used only for migrations.
    pub(crate) incomplete_directories: BTreeSet<String>,
    pub(crate) scope_unknown: bool,
}

struct StoreGuards {
    _output: BoundFile,
    _library: BoundFile,
}

impl LibraryManager {
    #[cfg(test)]
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_provider(root, platform_queue_file_provider())
    }

    #[cfg(test)]
    pub(crate) fn open_with_provider(
        root_path: impl AsRef<Path>,
        provider: Arc<dyn QueueFileProvider>,
    ) -> Result<Self> {
        Self::open_with_provider_and_optional_preview_cleanup_gate(root_path, provider, None)
    }

    #[cfg(test)]
    pub(crate) fn open_with_provider_and_preview_cleanup_gate(
        root_path: impl AsRef<Path>,
        provider: Arc<dyn QueueFileProvider>,
        preview_cleanup_gate: Arc<dyn PreviewCleanupGate>,
    ) -> Result<Self> {
        let mut manager = Self::open_with_provider(root_path, provider)?;
        manager.preview_cleanup_gate = Some(preview_cleanup_gate);
        Ok(manager)
    }

    #[cfg(test)]
    pub(crate) fn open_with_preview_cleanup_gate(
        root_path: impl AsRef<Path>,
        preview_cleanup_gate: Arc<dyn PreviewCleanupGate>,
    ) -> Result<Self> {
        let mut manager = Self::open(root_path)?;
        manager.preview_cleanup_gate = Some(preview_cleanup_gate);
        Ok(manager)
    }

    #[cfg(test)]
    fn open_with_provider_and_optional_preview_cleanup_gate(
        root_path: impl AsRef<Path>,
        provider: Arc<dyn QueueFileProvider>,
        preview_cleanup_gate: Option<Arc<dyn PreviewCleanupGate>>,
    ) -> Result<Self> {
        let root_path = root_path.as_ref().to_path_buf();
        let mut rooted = None;
        let mut bind_root = |accessor_path: &Path| -> Result<()> {
            let candidate = RootedFs::new(&root_path)?;
            let coordinated = coordinated_path_under_root(&candidate, accessor_path)?;
            if coordinated != candidate.logical_root_path() {
                bail!("coordinated media-library root changed during access");
            }
            candidate.list_root_directory()?;
            rooted = Some(candidate);
            Ok(())
        };
        provider
            .coordinate_read(&root_path, &mut bind_root)
            .map_err(|error| classify_deadlock_error(&root_path, "read", error))?;
        let root =
            rooted.ok_or_else(|| anyhow!("File Provider supplied no download-root accessor"))?;
        Self::initialize_with_bound_root(root, provider, preview_cleanup_gate)
    }

    /// Open a library manager on a queue-validated root descriptor without reopening its path.
    /// The descriptor is revalidated before any library metadata is initialized.
    pub(crate) fn open_with_bound_root_and_provider_and_preview_cleanup_gate(
        root: RootedFs,
        provider: Arc<dyn QueueFileProvider>,
        preview_cleanup_gate: Arc<dyn PreviewCleanupGate>,
    ) -> Result<Self> {
        let root_path = root.logical_root_path().to_path_buf();
        let mut root_seen = false;
        let mut bind_root = |accessor_path: &Path| -> Result<()> {
            let coordinated = coordinated_path_under_root(&root, accessor_path)?;
            if coordinated != root_path {
                bail!("coordinated media-library root changed during access");
            }
            root.list_root_directory()?;
            root_seen = true;
            Ok(())
        };
        provider
            .coordinate_read(&root_path, &mut bind_root)
            .map_err(|error| classify_deadlock_error(&root_path, "read", error))?;
        if !root_seen {
            bail!("File Provider supplied no bound download-root accessor");
        }
        Self::initialize_with_bound_root(root, provider, Some(preview_cleanup_gate))
    }

    fn initialize_with_bound_root(
        root: RootedFs,
        provider: Arc<dyn QueueFileProvider>,
        preview_cleanup_gate: Option<Arc<dyn PreviewCleanupGate>>,
    ) -> Result<Self> {
        root.validate_configured_root()
            .context("registered media-library root changed before metadata initialization")?;
        let library_dir = root.logical_root_path().join(LIBRARY_DIRECTORY);
        let library_dir_identity =
            ensure_private_directory(&root, provider.as_ref(), &library_dir)?;
        let manager = Self {
            root,
            provider,
            library_dir,
            library_dir_identity,
            preview_cleanup_gate,
            in_process_lock: Arc::new(Mutex::new(())),
            last_snapshot: Arc::new(Mutex::new(None)),
            process_nonce: PROCESS_NONCE
                .get_or_init(|| new_operation_id("process"))
                .clone(),
            #[cfg(test)]
            scan_test_hooks: Arc::new(Mutex::new(ScanTestHooks::default())),
            #[cfg(test)]
            preview_creation_time_unavailable: AtomicBool::new(false),
            #[cfg(test)]
            scan_attempts: AtomicUsize::new(0),
        };
        manager.ensure_lock_file()?;
        manager.ensure_output_lock_file()?;
        Ok(manager)
    }

    pub fn scan(&self, hints: &[SourceHint]) -> Result<LibrarySnapshot> {
        let _process_guard = self
            .in_process_lock
            .lock()
            .map_err(|_| anyhow!("media-library lock is poisoned"))?;
        let _disk_guard = self.lock_store()?;
        self.scan_locked(hints)
    }

    pub(crate) fn migration_root_path(&self) -> &Path {
        self.root.root_path()
    }

    pub(crate) fn migration_root_identity(&self) -> EntryIdentity {
        self.root.root_identity()
    }

    #[cfg(test)]
    pub(crate) fn migration_scan(&self) -> Result<LibrarySnapshot> {
        self.scan(&[])
    }

    pub(crate) fn migration_scan_with_completeness(&self) -> Result<MigrationScanResult> {
        let _process_guard = self
            .in_process_lock
            .lock()
            .map_err(|_| anyhow!("media-library lock is poisoned"))?;
        let _disk_guard = self.lock_store()?;
        self.scan_locked_with_completeness(&[])
    }

    pub(crate) fn migration_scan_with_output_lease_and_completeness(
        &self,
        lease: &crate::downloader::VideoMigrationGuard,
    ) -> Result<MigrationScanResult> {
        lease.validate_for_root(&self.root)?;
        let _process_guard = self
            .in_process_lock
            .lock()
            .map_err(|_| anyhow!("media-library lock is poisoned"))?;
        let _library_guard = self.lock_library()?;
        self.scan_locked_with_completeness(&[])
    }

    pub(crate) fn migration_validate_output_lease(
        &self,
        lease: &crate::downloader::VideoMigrationGuard,
    ) -> Result<()> {
        lease.validate_for_root(&self.root)
    }

    pub(crate) fn migration_available_space_bytes(&self) -> Result<u64> {
        self.root.available_space_bytes()
    }

    pub(crate) fn migration_path_identity(
        &self,
        relative_path: &str,
    ) -> Result<Option<EntryIdentity>> {
        let absolute = self.absolute_path(relative_path)?;
        self.root.entry_identity_if_parent_exists(&absolute)
    }

    pub(crate) fn migration_target_matches_exact(
        &self,
        relative_path: &str,
        expected_identity: EntryIdentity,
        expected_size: u64,
        expected_access: BoundFileAccessPolicy,
        expected_sha256: &str,
    ) -> Result<bool> {
        let absolute = self.absolute_path(relative_path)?;
        let expected_digest = parse_sha256(expected_sha256)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration target path changed during exact verification");
            }
            let Some(file) = self.root.open_bound_file(coordinated)? else {
                return Ok(false);
            };
            if file.identity() != expected_identity
                || !expected_identity.is_file()
                || file.byte_len()? != expected_size
                || file.access_policy()? != expected_access
            {
                return Ok(false);
            }
            Ok(file.sha256()? == expected_digest)
        })
    }

    /// Captures the current physical identity of an exact rooted target without reading its
    /// contents. Birth time may be unavailable; callers that need durable identity proof must
    /// reject identities without the complete birth pair.
    pub(crate) fn migration_physical_identity(
        &self,
        relative_path: &str,
        expected_device: u64,
        expected_inode: u64,
    ) -> Result<Option<PhysicalItemIdentity>> {
        let absolute = self.absolute_path(relative_path)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration target path changed during identity capture");
            }
            let Some(file) = self.root.open_bound_file(coordinated)? else {
                return Ok(None);
            };
            let before = fingerprint(&file)?;
            let before_identity = physical_item_identity(&before);
            let after = fingerprint(&file)?;
            let after_identity = physical_item_identity(&after);
            if !same_physical_item_descriptor(&before, &after) {
                bail!("migration target identity changed during metadata capture");
            }
            Ok((before_identity.device == expected_device
                && before_identity.inode == expected_inode)
                .then_some(after_identity))
        })
    }

    /// Verifies legacy target evidence against one rooted descriptor and one content pass.
    /// Missing is distinct from a present mismatch; filesystem and provider errors remain errors.
    pub(crate) fn migration_verify_legacy_target(
        &self,
        relative_path: &str,
        expected_identity: EntryIdentity,
        expected_size: u64,
        expected_access: BoundFileAccessPolicy,
        expected_sha256: &str,
    ) -> Result<LegacyTargetVerification> {
        let absolute = self.absolute_path(relative_path)?;
        let expected_digest = parse_sha256(expected_sha256)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration target path changed during exact verification");
            }
            let Some(file) = self.root.open_bound_file(coordinated)? else {
                return Ok(LegacyTargetVerification::Missing);
            };

            let before = fingerprint(&file)?;
            let before_access = file.access_policy()?;
            let before_identity = physical_item_identity(&before);
            if !before_identity.has_complete_birth() {
                bail!(
                    "legacy migration target birth identity is unavailable; exact verification cannot be completed"
                );
            }

            let proof_mismatch = !expected_identity.is_file()
                || before.identity != expected_identity
                || before.size != expected_size
                || before_access != expected_access;
            let actual_sha256 = if proof_mismatch {
                None
            } else {
                Some(file.sha256()?)
            };
            let after = fingerprint(&file)?;
            let after_access = file.access_policy()?;
            let after_identity = physical_item_identity(&after);
            if !same_physical_item_descriptor(&before, &after)
                || before.size != after.size
                || before_access != after_access
                || !after_identity.has_complete_birth()
            {
                bail!(
                    "legacy migration target identity, birth, size, or access policy changed during exact verification"
                );
            }

            if proof_mismatch
                || actual_sha256.is_some_and(|digest| digest != expected_digest)
            {
                Ok(LegacyTargetVerification::Mismatch(after_identity))
            } else {
                Ok(LegacyTargetVerification::Match(after_identity))
            }
        })
    }

    pub(crate) fn migration_file_proof(&self, relative_path: &str) -> Result<MigrationFileProof> {
        let absolute = self.absolute_path(relative_path)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration source path changed during verification");
            }
            let file = self
                .root
                .open_bound_file(coordinated)?
                .ok_or_else(|| anyhow!("migration source file is missing: {relative_path}"))?;
            migration_proof_for_bound_file(&file)
        })
    }

    pub(crate) fn migration_file_identity_matches(
        &self,
        relative_path: &str,
        expected: &MigrationFileProof,
    ) -> Result<bool> {
        let absolute = self.absolute_path(relative_path)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration source path changed during revalidation");
            }
            let Some(file) = self.root.open_bound_file(coordinated)? else {
                return Ok(false);
            };
            let actual = fingerprint(&file)?;
            let policy = file.access_policy()?;
            Ok(actual.identity == expected.entry_identity()
                && actual.size == expected.size
                && actual.birth_seconds == expected.birth_seconds
                && actual.birth_nanoseconds == expected.birth_nanoseconds
                && policy == expected.access_policy())
        })
    }

    pub(crate) fn migration_file_chunk_sha256(
        &self,
        relative_path: &str,
        expected: &MigrationFileProof,
        offset: u64,
        length: u64,
    ) -> Result<String> {
        if length > 8 * 1024 * 1024 {
            bail!("migration source verification chunk exceeds its byte budget");
        }
        let absolute = self.absolute_path(relative_path)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration source path changed during chunk verification");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            let file = self
                .root
                .open_bound_file_if_identity(&entry, expected.entry_identity())?;
            file.validate_identity()?;
            if file.byte_len()? != expected.size
                || file.access_policy()? != expected.access_policy()
            {
                bail!("migration source size or access policy changed during chunk verification");
            }
            let end = offset
                .checked_add(length)
                .ok_or_else(|| anyhow!("migration verification offset overflowed"))?;
            if end > expected.size {
                bail!("migration source verification chunk exceeds its expected file size");
            }
            let byte_count = usize::try_from(length)
                .context("migration source verification chunk does not fit in memory")?;
            let mut bytes = vec![0_u8; byte_count];
            if byte_count > 0 {
                let mut reader = file.duplicate_std_file()?;
                reader.seek(SeekFrom::Start(offset))?;
                reader.read_exact(&mut bytes)?;
            }
            file.validate_identity()?;
            if file.byte_len()? != expected.size
                || file.access_policy()? != expected.access_policy()
            {
                bail!("migration source changed while verifying a content chunk");
            }
            Ok(hex_digest(Sha256::digest(bytes).as_slice()))
        })
    }

    pub(crate) fn plan_migration_groups(
        &self,
        target: &LibraryManager,
        snapshot: &LibrarySnapshot,
        item_ids: &[String],
        conflict: ConflictPolicy,
        item_conflicts: &BTreeMap<String, ConflictPolicy>,
    ) -> Result<Vec<MigrationPlannedGroup>> {
        if item_ids.is_empty() {
            bail!("select at least one media item to migrate");
        }
        let mut unique_ids = HashSet::new();
        if item_ids
            .iter()
            .any(|item_id| !unique_ids.insert(item_id.as_str()))
        {
            bail!("a media item was selected more than once for migration");
        }
        let items_by_id = snapshot
            .items
            .iter()
            .map(|item| (item.id.as_str(), item))
            .collect::<HashMap<_, _>>();
        if item_ids
            .iter()
            .any(|item_id| !items_by_id.contains_key(item_id.as_str()))
        {
            bail!("selected media item is not present in the current source scan");
        }

        let mut reserved = TargetReservationSet {
            private_library_identity: Some(target.library_dir_identity),
            ..TargetReservationSet::default()
        };
        let mut seen_sources = HashSet::new();
        let mut groups = Vec::with_capacity(item_ids.len());
        for item_id in item_ids {
            let conflict = item_conflicts.get(item_id).copied().unwrap_or(conflict);
            let item = items_by_id[item_id.as_str()];
            if item.presence != Presence::Present {
                bail!(
                    "selected media is missing or unavailable: {}",
                    item.relative_path
                );
            }
            let mut source_paths = vec![item.relative_path.clone()];
            source_paths.extend(
                item.attachments
                    .iter()
                    .map(|attachment| attachment.relative_path.clone()),
            );
            source_paths.sort_by(|left, right| {
                let left_primary = left == &item.relative_path;
                let right_primary = right == &item.relative_path;
                right_primary
                    .cmp(&left_primary)
                    .then_with(|| left.cmp(right))
            });
            if source_paths
                .iter()
                .any(|path| !seen_sources.insert(path.clone()))
            {
                bail!("a sidecar is associated with more than one selected media item");
            }

            let primary = self.absolute_path(&item.relative_path)?;
            let primary_stem = primary
                .file_stem()
                .and_then(OsStr::to_str)
                .ok_or_else(|| anyhow!("selected media has a non-UTF-8 filename"))?;
            let target_parent = Path::new(&item.relative_path)
                .parent()
                .unwrap_or_else(|| Path::new(""));
            let source_parent_absolute = self.root.logical_root_path().join(target_parent);
            let source_name_semantics =
                self.coordinate_read(&source_parent_absolute, |coordinated| {
                    if self.path_in_root(coordinated)? != source_parent_absolute {
                        bail!("coordinated source directory changed during migration preview");
                    }
                    let identity = if target_parent.as_os_str().is_empty() {
                        self.root.root_identity()
                    } else {
                        self.root
                            .entry_identity(&source_parent_absolute)?
                            .ok_or_else(|| anyhow!("source media directory disappeared"))?
                    };
                    if !identity.is_dir() {
                        bail!("source media parent is no longer a directory");
                    }
                    let directory =
                        open_validated_target_directory(&self.root, target_parent, identity)?;
                    target_name_semantics(&directory)
                        .context("filesystem name semantics unavailable for migration sidecars")
                })?;

            let mut planned_sources = Vec::with_capacity(source_paths.len());
            let mut longest_filename_tail = 0;
            for source_relative in &source_paths {
                let source_absolute = self.absolute_path(source_relative)?;
                let source_stem = source_absolute
                    .file_stem()
                    .and_then(OsStr::to_str)
                    .ok_or_else(|| anyhow!("migration attachment has a non-UTF-8 filename"))?;
                let suffix = if source_relative == &item.relative_path {
                    String::new()
                } else {
                    matching_sidecar_suffix_with(
                        source_stem,
                        primary_stem,
                        source_name_semantics,
                        platform_case_fold,
                    )?
                    .ok_or_else(|| {
                        anyhow!("sidecar association changed during migration planning")
                    })?
                };
                longest_filename_tail =
                    longest_filename_tail.max(filename_tail_bytes(&source_absolute, &suffix)?);
                planned_sources.push((source_relative.clone(), source_absolute, suffix));
            }

            let candidate_for_stem = |stem_suffix: &str| -> Result<(String, Vec<String>)> {
                let candidate_stem =
                    budgeted_group_stem(primary_stem, longest_filename_tail, stem_suffix, true)?;
                let candidate_primary =
                    target_parent.join(file_name_with_stem(&primary, &candidate_stem, "")?);
                let primary_target = path_to_relative_string(&candidate_primary)?;
                let target_primary_stem = candidate_primary
                    .file_stem()
                    .and_then(OsStr::to_str)
                    .ok_or_else(|| anyhow!("migration target filename is invalid"))?;
                let targets = planned_sources
                    .iter()
                    .map(|(_, source_absolute, suffix)| {
                        let name =
                            file_name_with_stem(source_absolute, target_primary_stem, suffix)?;
                        path_to_relative_string(&target_parent.join(name))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((primary_target, targets))
            };

            let (mut target_path, mut target_paths) = candidate_for_stem("")?;
            let has_conflict = migration_targets_conflict(target, &target_paths, &mut reserved)?;
            let mut skip_group = has_conflict && conflict == ConflictPolicy::Skip;
            if has_conflict && conflict == ConflictPolicy::KeepBoth {
                let mut found = false;
                for number in 2..=10_000 {
                    let suffix = format!(" ({number})");
                    let (candidate_primary, candidate_targets) = candidate_for_stem(&suffix)?;
                    if !migration_targets_conflict(target, &candidate_targets, &mut reserved)? {
                        target_path = candidate_primary;
                        target_paths = candidate_targets;
                        found = true;
                        break;
                    }
                }
                if !found {
                    bail!("could not find a free keep-both migration filename");
                }
                skip_group = false;
            }

            let mut files = Vec::with_capacity(planned_sources.len());
            for ((source_path, _, _), target_path) in planned_sources.iter().zip(&target_paths) {
                let proof = self.migration_file_proof(source_path)?;
                files.push(MigrationPlannedFile {
                    source_path: source_path.clone(),
                    target_path: target_path.clone(),
                    proof,
                });
            }
            if !skip_group {
                for path in &target_paths {
                    reserved.insert(&target.root, path)?;
                }
            }
            groups.push(MigrationPlannedGroup {
                item_id: item_id.clone(),
                source_path: item.relative_path.clone(),
                target_path,
                outcome: if skip_group {
                    MigrationPlanOutcome::Skip
                } else {
                    MigrationPlanOutcome::Move
                },
                conflict: has_conflict,
                files,
            });
        }
        Ok(groups)
    }

    pub(crate) fn migration_create_staging_file(&self, name: &str) -> Result<EntryIdentity> {
        let absolute = self.migration_staging_path(name)?;
        self.coordinate_write(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated staging path changed during migration");
            }
            self.validate_library_directory()?;
            self.root.create_migration_staging_file(&absolute)
        })
    }

    pub(crate) fn migration_staging_identity(
        &self,
        name: &str,
        expected_final_access: BoundFileAccessPolicy,
    ) -> Result<Option<EntryIdentity>> {
        let absolute = self.migration_staging_path(name)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration staging path changed during recovery");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            let Some(identity) = self.root.bound_entry_identity(&entry)? else {
                return Ok(None);
            };
            if !identity.is_file() {
                bail!("migration staging entry is not a regular file");
            }
            let file = self.root.open_bound_file_if_identity(&entry, identity)?;
            validate_migration_publication_staging_access(&file, expected_final_access)?;
            Ok(Some(identity))
        })
    }

    pub(crate) fn migration_staging_entry_identity(
        &self,
        name: &str,
    ) -> Result<Option<EntryIdentity>> {
        let absolute = self.migration_staging_path(name)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration staging path changed during orphan inspection");
            }
            self.root.entry_identity_if_parent_exists(coordinated)
        })
    }

    pub(crate) fn migration_staging_access_policy(
        &self,
        name: &str,
        expected_identity: EntryIdentity,
    ) -> Result<BoundFileAccessPolicy> {
        let absolute = self.migration_staging_path(name)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration staging path changed during access-policy capture");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            let file = self
                .root
                .open_bound_file_if_identity(&entry, expected_identity)?;
            file.validate_private_single_link(0o600)?;
            file.access_policy()
        })
    }

    pub(crate) fn migration_staging_chunk_sha256(
        &self,
        name: &str,
        expected_identity: EntryIdentity,
        expected_size: u64,
        expected_final_access: Option<BoundFileAccessPolicy>,
        offset: u64,
        length: u64,
    ) -> Result<String> {
        if length > 8 * 1024 * 1024 {
            bail!("migration verification chunk exceeds its byte budget");
        }
        let absolute = self.migration_staging_path(name)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration staging path changed during chunk verification");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            let file = self
                .root
                .open_bound_file_if_identity(&entry, expected_identity)?;
            if let Some(expected_access) = expected_final_access {
                validate_migration_publication_staging_access(&file, expected_access)?;
            } else {
                file.validate_private_single_link(0o600)?;
            }
            if file.byte_len()? != expected_size {
                bail!("migration staging size changed during chunk verification");
            }
            let end = offset
                .checked_add(length)
                .ok_or_else(|| anyhow!("migration verification offset overflowed"))?;
            if end > expected_size {
                bail!("migration verification chunk exceeds its expected file size");
            }
            let byte_count = usize::try_from(length)
                .context("migration verification chunk does not fit in memory")?;
            let mut bytes = vec![0_u8; byte_count];
            if byte_count > 0 {
                let mut reader = file.duplicate_std_file()?;
                reader.seek(SeekFrom::Start(offset))?;
                reader.read_exact(&mut bytes)?;
            }
            if let Some(expected_access) = expected_final_access {
                validate_migration_publication_staging_access(&file, expected_access)?;
            } else {
                file.validate_private_single_link(0o600)?;
            }
            if file.byte_len()? != expected_size {
                bail!("migration staging changed while verifying its content chunk");
            }
            Ok(hex_digest(Sha256::digest(bytes).as_slice()))
        })
    }

    pub(crate) fn migration_task_path_identity(
        &self,
        relative: &Path,
    ) -> Result<Option<EntryIdentity>> {
        let absolute = self.migration_task_absolute(relative)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated task staging path changed during inspection");
            }
            self.root.entry_identity_if_parent_exists(coordinated)
        })
    }

    pub(crate) fn migration_prepare_task_staging_job(
        &self,
        relative: &Path,
        migration_token: &str,
    ) -> Result<EntryIdentity> {
        let absolute = self.migration_task_absolute(relative)?;
        self.coordinate_write(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration task job path changed before initialization");
            }
            crate::downloader::prepare_video_staging_for_migration(
                &self.root,
                relative,
                migration_token,
            )
        })
    }

    pub(crate) fn migration_task_create_directory(
        &self,
        relative: &Path,
        mode: u32,
    ) -> Result<Option<EntryIdentity>> {
        let absolute = self.migration_task_absolute(relative)?;
        self.coordinate_write(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated task staging directory changed before creation");
            }
            self.ensure_task_staging_base()?;
            ensure_relative_parents(&self.root, &absolute)?;
            self.root.create_dir(&absolute, (mode & 0o7777) as u16)
        })
    }

    pub(crate) fn migration_task_source_directory_matches(
        &self,
        file: &TaskTransferFile,
    ) -> Result<bool> {
        if file.kind != TaskTransferEntryKind::Directory {
            bail!("task directory proof requested for a file entry");
        }
        let absolute = self.migration_task_absolute(&file.source_relative)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated task source directory changed during verification");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            let Some(identity) = self.root.bound_entry_identity(&entry)? else {
                return Ok(false);
            };
            if !identity.is_dir()
                || identity.device() != file.device
                || identity.inode() != file.inode
            {
                return Ok(false);
            }
            let (owner, group, mode) = self
                .root
                .open_bound_directory(&entry, identity)?
                .access_policy()?;
            Ok(owner == file.owner && group == file.group && mode == file.mode)
        })
    }

    /// Removes one exact source control file after the engine has persisted a cleanup intent.
    /// The digest, inode, owner, group, mode, and hard-link count all come from the frozen queue
    /// transfer plan; unknown or replaced support files are retained.
    pub(crate) fn migration_task_remove_source_control(
        &self,
        control: &TaskTransferControl,
    ) -> Result<()> {
        const MAX_CONTROL_BYTES: u64 = 1024 * 1024;
        if control.device == 0
            || control.inode == 0
            || control.size_bytes > MAX_CONTROL_BYTES
            || control.owner != unsafe { libc::geteuid() } as u32
            || control.mode != 0o600
            || control.link_count != Some(1)
            || !is_sha256_hex(&control.sha256)
        {
            bail!("task staging control proof is incomplete or violates its private access policy");
        }
        let absolute = self.migration_task_absolute(&control.source_relative)?;
        let expected_identity = EntryIdentity::regular_file(control.device, control.inode);
        let expected_access = BoundFileAccessPolicy {
            owner: control.owner,
            group: control.group,
            mode: control.mode,
            link_count: 1,
        };
        let expected_digest = parse_sha256(&control.sha256)?;
        self.coordinate_write(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated task staging control changed before cleanup");
            }
            let entry = self.root.bind_entry(&absolute, false)?;
            if self.root.bound_entry_identity(&entry)? != Some(expected_identity) {
                bail!("task staging control identity changed before cleanup");
            }
            let source = self
                .root
                .open_bound_file_if_identity(&entry, expected_identity)?;
            validate_task_control_source(
                &source,
                control.size_bytes,
                expected_access,
                expected_digest,
            )?;
            self.root.remove_bound_file_if_identity_with_validation(
                &entry,
                expected_identity,
                || {
                    validate_task_control_source(
                        &source,
                        control.size_bytes,
                        expected_access,
                        expected_digest,
                    )
                },
            )
        })
    }

    /// Removes only an empty planned source directory with the recorded identity and access
    /// policy. Directory timestamps and link count are intentionally ignored because child
    /// cleanup changes both without replacing the directory or changing its permissions.
    pub(crate) fn migration_task_remove_source_directory(
        &self,
        directory: &TaskTransferFile,
    ) -> Result<()> {
        if directory.kind != TaskTransferEntryKind::Directory
            || directory.device == 0
            || directory.inode == 0
            || directory.mode & !0o7777 != 0
            || directory.mode & 0o6000 != 0
        {
            bail!("task source directory proof is incomplete or invalid");
        }
        let absolute = self.migration_task_absolute(&directory.source_relative)?;
        let expected_identity = EntryIdentity::directory(directory.device, directory.inode);
        self.coordinate_write(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated task source directory changed before cleanup");
            }
            let entry = self.root.bind_entry(&absolute, false)?;
            if self.root.bound_entry_identity(&entry)? != Some(expected_identity) {
                bail!("task source directory identity changed before cleanup");
            }
            let bound = self.root.open_bound_directory(&entry, expected_identity)?;
            let (owner, group, mode) = bound.access_policy()?;
            if owner != directory.owner || group != directory.group || mode != directory.mode {
                bail!("task source directory access policy changed before cleanup");
            }
            if !self
                .root
                .list_bound_directory(&entry, expected_identity)?
                .is_empty()
            {
                bail!("task source directory contains unplanned entries and was preserved");
            }
            self.root
                .remove_bound_dir_if_identity(&entry, expected_identity)
        })
    }

    pub(crate) fn migration_task_target_directory_matches(
        &self,
        relative: &Path,
        expected_identity: EntryIdentity,
        file: &TaskTransferFile,
    ) -> Result<bool> {
        if file.kind != TaskTransferEntryKind::Directory {
            bail!("task directory proof requested for a file entry");
        }
        let absolute = self.migration_task_absolute(relative)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated task target directory changed during verification");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            let Some(identity) = self.root.bound_entry_identity(&entry)? else {
                return Ok(false);
            };
            if identity != expected_identity || !identity.is_dir() {
                return Ok(false);
            }
            let (owner, group, mode) = self
                .root
                .open_bound_directory(&entry, identity)?
                .access_policy()?;
            Ok(owner == file.owner && group == file.group && mode == file.mode)
        })
    }

    pub(crate) fn migration_task_directory_is_empty(
        &self,
        relative: &Path,
        expected_identity: EntryIdentity,
    ) -> Result<bool> {
        let absolute = self.migration_task_absolute(relative)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated task directory changed during emptiness check");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            if self.root.bound_entry_identity(&entry)? != Some(expected_identity) {
                bail!("task directory identity changed during emptiness check");
            }
            Ok(self
                .root
                .list_bound_directory(&entry, expected_identity)?
                .is_empty())
        })
    }

    pub(crate) fn migration_copy_task_chunk(
        &self,
        target: &LibraryManager,
        file: &TaskTransferFile,
        staging_name: &str,
        staging_identity: EntryIdentity,
        offset: u64,
        max_bytes: u64,
    ) -> Result<MigrationCopyChunk> {
        if file.kind != TaskTransferEntryKind::File {
            bail!("task transfer copy requires a regular file entry");
        }
        let source_absolute = self.migration_task_absolute(&file.source_relative)?;
        let staging_absolute = target.migration_staging_path(staging_name)?;
        let expected_policy = task_transfer_access_policy(file)?;
        self.coordinate_read(&source_absolute, |source_coordinated| {
            if source_coordinated != source_absolute {
                bail!("coordinated task source path changed during copy");
            }
            target.coordinate_write(&staging_absolute, |target_coordinated| {
                if target_coordinated != staging_absolute {
                    bail!("coordinated task staging file changed during copy");
                }
                self.root.copy_migration_chunk(
                    &source_absolute,
                    EntryIdentity::regular_file(file.device, file.inode),
                    file.size_bytes,
                    expected_policy,
                    &target.root,
                    &staging_absolute,
                    staging_identity,
                    offset,
                    max_bytes,
                )
            })
        })
    }

    pub(crate) fn migration_task_chunk_sha256(
        &self,
        path: &Path,
        expected_identity: EntryIdentity,
        expected_size: u64,
        expected_policy: BoundFileAccessPolicy,
        offset: u64,
        length: u64,
    ) -> Result<String> {
        if length > 8 * 1024 * 1024 {
            bail!("task staging verification chunk exceeds its byte budget");
        }
        let absolute = self.migration_task_absolute(path)?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated task file changed during chunk verification");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            let file = self
                .root
                .open_bound_file_if_identity(&entry, expected_identity)?;
            file.validate_identity()?;
            if file.byte_len()? != expected_size || file.access_policy()? != expected_policy {
                bail!("task file size or access policy changed during verification");
            }
            let end = offset
                .checked_add(length)
                .ok_or_else(|| anyhow!("task verification offset overflowed"))?;
            if end > expected_size {
                bail!("task verification chunk exceeds its expected file size");
            }
            let byte_count =
                usize::try_from(length).context("task chunk does not fit in memory")?;
            let mut bytes = vec![0_u8; byte_count];
            if byte_count > 0 {
                let mut reader = file.duplicate_std_file()?;
                reader.seek(SeekFrom::Start(offset))?;
                reader.read_exact(&mut bytes)?;
            }
            file.validate_identity()?;
            if file.byte_len()? != expected_size || file.access_policy()? != expected_policy {
                bail!("task file changed while verifying its content chunk");
            }
            Ok(hex_digest(Sha256::digest(bytes).as_slice()))
        })
    }

    pub(crate) fn migration_task_publish_file(
        &self,
        staging_name: &str,
        staging_identity: EntryIdentity,
        target_relative: &Path,
        file: &TaskTransferFile,
    ) -> Result<EntryIdentity> {
        let staging_absolute = self.migration_staging_path(staging_name)?;
        let target_absolute = self.migration_task_absolute(target_relative)?;
        let expected_access = task_transfer_access_policy(file)?;
        self.coordinate_write(&target_absolute, |coordinated| {
            if coordinated != target_absolute {
                bail!("coordinated task target changed before publication");
            }
            ensure_relative_parents(&self.root, &target_absolute)?;
            let staging_entry = self.root.bind_entry(&staging_absolute, false)?;
            let staging = self
                .root
                .open_bound_file_if_identity(&staging_entry, staging_identity)?;
            let staging_policy = staging.access_policy()?;
            if staging_policy.owner != file.owner || staging_policy.group != file.group {
                bail!("cannot preserve task file ownership or group at the destination");
            }
            self.root
                .publish_migration_staging_file_after_chunk_verification(
                    &staging_absolute,
                    staging_identity,
                    &target_absolute,
                    file.size_bytes,
                    expected_access,
                )
        })
    }

    pub(crate) fn migration_task_target_matches(
        &self,
        relative: &Path,
        expected_identity: EntryIdentity,
        file: &TaskTransferFile,
    ) -> Result<bool> {
        let absolute = self.migration_task_absolute(relative)?;
        let digest = file
            .sha256
            .as_deref()
            .ok_or_else(|| anyhow!("task transfer file is missing its full SHA-256 proof"))?;
        self.coordinate_read(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated task target changed during verification");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            let Some(identity) = self.root.bound_entry_identity(&entry)? else {
                return Ok(false);
            };
            if identity != expected_identity {
                bail!("task target identity changed after publication");
            }
            let actual = self.root.open_bound_file_if_identity(&entry, identity)?;
            let policy = task_transfer_access_policy(file)?;
            if actual.byte_len()? != file.size_bytes || actual.access_policy()? != policy {
                return Ok(false);
            }
            Ok(actual.sha256()? == parse_sha256(digest)?)
        })
    }

    /// Removes one exact task payload file after the migration journal has recorded its cleanup
    /// intent. The queue record and inode-bound control files are handled by the queue protocol;
    /// this helper is limited to a regular payload file in the recorded staging tree.
    pub(crate) fn migration_task_remove_source_file(&self, file: &TaskTransferFile) -> Result<()> {
        if file.kind != TaskTransferEntryKind::File {
            bail!("task source cleanup requires a regular payload file");
        }
        let absolute = self.migration_task_absolute(&file.source_relative)?;
        let digest = file
            .sha256
            .as_deref()
            .ok_or_else(|| anyhow!("task transfer file is missing its full SHA-256 proof"))?;
        let expected_digest = parse_sha256(digest)?;
        let expected_identity = EntryIdentity::regular_file(file.device, file.inode);
        let expected_access = task_transfer_access_policy(file)?;
        self.coordinate_write(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated task source changed before payload cleanup");
            }
            let entry = self.root.bind_entry(&absolute, false)?;
            if self.root.bound_entry_identity(&entry)? != Some(expected_identity) {
                bail!("task source payload identity changed before cleanup");
            }
            let source = self
                .root
                .open_bound_file_if_identity(&entry, expected_identity)?;
            source.validate_identity()?;
            if source.byte_len()? != file.size_bytes
                || source.access_policy()? != expected_access
                || source.sha256()? != expected_digest
            {
                bail!("task source payload content or access policy changed before cleanup");
            }
            self.root.remove_bound_file_if_identity_with_validation(
                &entry,
                expected_identity,
                || {
                    source.validate_identity()?;
                    if source.byte_len()? != file.size_bytes
                        || source.access_policy()? != expected_access
                        || source.sha256()? != expected_digest
                    {
                        bail!("task source payload changed while cleanup was quarantining it");
                    }
                    Ok(())
                },
            )
        })
    }

    // The source proof, destination root/path, captured staging identity, and byte window are
    // deliberately explicit so each cross-root copy step remains independently auditable.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn migration_copy_chunk(
        &self,
        target: &LibraryManager,
        source_path: &str,
        target_staging_path: &str,
        proof: &MigrationFileProof,
        staging_identity: EntryIdentity,
        offset: u64,
        max_bytes: u64,
    ) -> Result<MigrationCopyChunk> {
        let source_absolute = self.absolute_path(source_path)?;
        let staging_absolute = target.migration_staging_path(target_staging_path)?;
        self.coordinate_read(&source_absolute, |source_coordinated| {
            if source_coordinated != source_absolute {
                bail!("coordinated migration source path changed during copy");
            }
            target.coordinate_write(&staging_absolute, |target_coordinated| {
                if target_coordinated != staging_absolute {
                    bail!("coordinated migration staging path changed during copy");
                }
                self.root.copy_migration_chunk(
                    &source_absolute,
                    proof.entry_identity(),
                    proof.size,
                    proof.access_policy(),
                    &target.root,
                    &staging_absolute,
                    staging_identity,
                    offset,
                    max_bytes,
                )
            })
        })
    }

    pub(crate) fn migration_publish_staging_file(
        &self,
        staging_path: &str,
        staging_identity: EntryIdentity,
        target_path: &str,
        proof: &MigrationFileProof,
        expected_access: BoundFileAccessPolicy,
    ) -> Result<EntryIdentity> {
        let staging_absolute = self.migration_staging_path(staging_path)?;
        let target_absolute = self.absolute_path(target_path)?;
        self.coordinate_write(&target_absolute, |target_coordinated| {
            if target_coordinated != target_absolute {
                bail!("coordinated migration target changed during publication");
            }
            ensure_relative_parents(&self.root, &target_absolute)?;
            self.root
                .publish_migration_staging_file_after_chunk_verification(
                    &staging_absolute,
                    staging_identity,
                    &target_absolute,
                    proof.size,
                    expected_access,
                )
        })
    }

    pub(crate) fn migration_remove_source(
        &self,
        relative_path: &str,
        expected: &MigrationFileProof,
    ) -> Result<()> {
        let absolute = self.absolute_path(relative_path)?;
        self.coordinate_write(&absolute, |coordinated| {
            if coordinated != absolute {
                bail!("coordinated migration source path changed before cleanup");
            }
            let entry = self.root.bind_entry(&absolute, false)?;
            let identity = expected.entry_identity();
            if self.root.bound_entry_identity(&entry)? != Some(identity) {
                bail!("migration source object was replaced before cleanup");
            }
            let file = self.root.open_bound_file_if_identity(&entry, identity)?;
            if migration_proof_for_bound_file(&file)? != *expected {
                bail!("migration source content or access policy changed before cleanup");
            }
            self.root
                .remove_bound_file_if_identity_with_validation(&entry, identity, || {
                    if migration_proof_for_bound_file(&file)? != *expected {
                        bail!("migration source changed while cleanup was quarantining it");
                    }
                    Ok(())
                })
        })
    }

    pub(crate) fn migration_read_state(&self, name: &str, limit: usize) -> Result<Option<Vec<u8>>> {
        let path = self.migration_state_path(name)?;
        self.read_private_file(&path, limit)
    }

    pub(crate) fn migration_read_state_snapshot(
        &self,
        name: &str,
        limit: usize,
    ) -> Result<Option<MigrationStateSnapshot>> {
        let path = self.migration_state_path(name)?;
        self.coordinate_read(&path, |coordinated| {
            self.validate_library_directory()?;
            let Some(file) = self.root.open_bound_file(coordinated)? else {
                return Ok(None);
            };
            validate_private_migration_state_file(&file)?;
            let identity = file.identity();
            let contents = file.read_limited(limit)?;
            if file.byte_len()? != contents.len() as u64 {
                bail!("media-library state file changed while reading its snapshot");
            }
            let repeated = file.read_limited(limit)?;
            validate_private_migration_state_file(&file)?;
            if repeated != contents {
                bail!("media-library state content changed while reading its snapshot");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            if self.root.bound_entry_identity(&entry)? != Some(identity) {
                bail!("media-library state identity changed while reading its snapshot");
            }
            Ok(Some(MigrationStateSnapshot {
                proof: MigrationStateFileProof {
                    device: identity.device(),
                    inode: identity.inode(),
                    size_bytes: contents.len() as u64,
                    sha256: sha256_bytes(&contents),
                },
                contents,
            }))
        })
    }

    pub(crate) fn migration_replace_state_if_unchanged(
        &self,
        name: &str,
        snapshot: &MigrationStateSnapshot,
        contents: &[u8],
    ) -> Result<()> {
        let path = self.migration_state_path(name)?;
        if contents.len() > 64 * 1024 * 1024 {
            bail!("migration state file exceeds its size limit");
        }
        if snapshot.proof.size_bytes != snapshot.contents.len() as u64
            || sha256_bytes(&snapshot.contents) != snapshot.proof.sha256
        {
            bail!("migration state snapshot has an invalid content proof");
        }
        self.coordinate_write(&path, |coordinated| {
            self.validate_library_directory()?;
            let file = self
                .root
                .open_bound_file(coordinated)?
                .ok_or_else(|| anyhow!("migration state file disappeared before replacement"))?;
            validate_private_migration_state_file(&file)?;
            let expected = EntryIdentity::regular_file(snapshot.proof.device, snapshot.proof.inode);
            if file.identity() != expected
                || file.byte_len()? != snapshot.proof.size_bytes
                || sha256_bytes(&file.read_limited(64 * 1024 * 1024)?) != snapshot.proof.sha256
            {
                bail!("migration state file identity or content changed before replacement");
            }
            let entry = self.root.bind_entry(coordinated, false)?;
            if self.root.bound_entry_identity(&entry)? != Some(expected) {
                bail!("migration state file was replaced before atomic update");
            }
            // SafeFs uses one verification bound for both the original and staged files. Keep
            // the bound large enough for either exact document; the checkpoint callback below
            // still verifies the staged file against `contents.len()` and the requested bytes.
            let verify_bytes = usize::try_from(snapshot.proof.size_bytes)
                .context("migration state snapshot size does not fit in memory")?
                .max(contents.len());
            let temporary = private_temp_sibling(coordinated);
            let expected_replacement_hash = sha256_bytes(contents);
            let mut verify_original = |staged_entry: &crate::safe_fs::BoundEntry,
                                       staged_file: &BoundFile,
                                       staged_identity: EntryIdentity|
             -> Result<()> {
                // SafeFs invokes this callback with the newly staged replacement. Revalidate
                // the captured original through its already-open handle, and separately bind
                // the staged inode/content to the requested replacement bytes.
                validate_private_migration_state_file(&file)?;
                if file.identity() != expected
                    || file.byte_len()? != snapshot.proof.size_bytes
                    || sha256_bytes(&file.read_limited(64 * 1024 * 1024)?) != snapshot.proof.sha256
                {
                    bail!("migration state file changed during atomic update");
                }
                validate_private_migration_state_file(staged_file)?;
                let staged_contents = staged_file.read_limited(contents.len())?;
                if staged_entry.path() != temporary
                    || staged_identity != staged_file.identity()
                    || !staged_identity.is_file()
                    || self.root.bound_entry_identity(staged_entry)? != Some(staged_identity)
                    || staged_file.byte_len()? != contents.len() as u64
                    || staged_contents != contents
                    || sha256_bytes(&staged_contents) != expected_replacement_hash
                {
                    bail!("staged migration state replacement changed before atomic update");
                }
                Ok(())
            };
            let (_, new_identity) = self
                .root
                .replace_bound_file_atomically_if_identity_with_checkpoint(
                    &entry,
                    expected,
                    &temporary,
                    contents,
                    0o600,
                    verify_bytes,
                    &mut verify_original,
                )?;
            let current = self
                .root
                .open_bound_file(coordinated)?
                .ok_or_else(|| anyhow!("updated migration state file disappeared"))?;
            if current.identity() != new_identity {
                bail!("updated migration state file identity changed after replacement");
            }
            validate_private_migration_state_file(&current)?;
            if current.read_limited(contents.len())? != contents {
                bail!("updated migration state content changed after replacement");
            }
            Ok(())
        })
    }

    /// Removes a state file only when it is still the exact private file described by a
    /// previously persisted proof. Missing files are an idempotent success (`false`).
    pub(crate) fn migration_remove_state_if_proof(
        &self,
        name: &str,
        proof: &MigrationStateFileProof,
    ) -> Result<bool> {
        let path = self.migration_state_path(name)?;
        if proof.size_bytes > 64 * 1024 * 1024
            || proof.device == 0
            || proof.inode == 0
            || proof.sha256.len() != 64
            || !proof.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("migration state removal proof exceeds its size limit");
        }
        self.coordinate_write(&path, |coordinated| {
            self.validate_library_directory()?;
            let Some(file) = self.root.open_bound_file(coordinated)? else {
                return Ok(false);
            };
            validate_private_migration_state_file(&file)?;
            let expected = EntryIdentity::regular_file(proof.device, proof.inode);
            let verify = || -> Result<()> {
                validate_private_migration_state_file(&file)?;
                if file.identity() != expected
                    || file.byte_len()? != proof.size_bytes
                    || sha256_bytes(&file.read_limited(64 * 1024 * 1024)?) != proof.sha256
                {
                    bail!("migration state file no longer matches its retirement proof");
                }
                Ok(())
            };
            verify()?;
            let entry = self.root.bind_entry(coordinated, false)?;
            if self.root.bound_entry_identity(&entry)? != Some(expected) {
                bail!("migration state file was replaced before retirement");
            }
            self.root
                .remove_bound_file_if_identity_with_validation(&entry, expected, verify)?;
            Ok(true)
        })
    }

    pub(crate) fn migration_write_state(&self, name: &str, contents: &[u8]) -> Result<()> {
        let path = self.migration_state_path(name)?;
        if contents.len() > 64 * 1024 * 1024 {
            bail!("migration state file exceeds its size limit");
        }
        self.write_private_file(&path, contents)
    }

    pub(crate) fn migration_state_lock(&self) -> Result<BoundFile> {
        let path = self.library_dir.join("migration.lock");
        let file = self.coordinate_write(&path, |coordinated| {
            self.validate_library_directory()?;
            if self.root.open_bound_file(coordinated)?.is_none() {
                match self.root.create_new_bound_file(coordinated, &[], 0o600) {
                    Ok(_) => {}
                    Err(_) if self.root.open_bound_file(coordinated)?.is_some() => {}
                    Err(error) => return Err(error),
                }
            }
            let file = self
                .root
                .open_bound_file(coordinated)?
                .ok_or_else(|| anyhow!("migration state lock disappeared"))?;
            file.validate_private_single_link(0o600)?;
            Ok(file)
        })?;
        file.lock_exclusive()?;
        Ok(file)
    }

    pub(crate) fn migration_state_names(&self) -> Result<Vec<String>> {
        self.coordinate_read(&self.library_dir, |coordinated| {
            if coordinated != self.library_dir {
                bail!("coordinated migration state directory changed during listing");
            }
            self.validate_library_directory()?;
            let entry = self.root.bind_entry(&self.library_dir, false)?;
            let identity = self
                .root
                .bound_entry_identity(&entry)?
                .ok_or_else(|| anyhow!("private media-library directory disappeared"))?;
            self.root
                .validate_private_bound_directory(&entry, identity, 0o700)?;
            let names = self
                .root
                .list_bound_directory(&entry, identity)?
                .into_iter()
                .filter_map(|(name, entry_identity)| {
                    if !entry_identity.is_file() {
                        return None;
                    }
                    let name = name.to_str()?;
                    name.starts_with("migration-").then(|| name.to_owned())
                })
                .collect::<Vec<_>>();
            Ok(names)
        })
    }

    fn migration_state_path(&self, name: &str) -> Result<PathBuf> {
        if name.is_empty()
            || name.len() > 160
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || !name.starts_with("migration-")
            || !name.ends_with(".json")
        {
            bail!("migration state filename is invalid");
        }
        Ok(self.library_dir.join(name))
    }

    fn migration_staging_path(&self, name: &str) -> Result<PathBuf> {
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || !name.starts_with("migration-stage-")
        {
            bail!("migration staging filename is invalid");
        }
        Ok(self.library_dir.join(format!("{name}.partial")))
    }

    fn migration_task_absolute(&self, relative: &Path) -> Result<PathBuf> {
        if !is_task_staging_relative(relative) {
            bail!("task transfer path is outside its private staging namespace");
        }
        Ok(self.root.logical_root_path().join(relative))
    }

    fn ensure_task_staging_base(&self) -> Result<()> {
        let relative = Path::new(".telegram-video-downloader-staging");
        let absolute = self.root.logical_root_path().join(relative);
        let identity = match self.root.entry_identity(&absolute)? {
            Some(identity) if identity.is_dir() => identity,
            Some(_) => bail!("task staging root is not a directory"),
            None => self
                .root
                .create_dir(&absolute, 0o700)?
                .ok_or_else(|| anyhow!("task staging root appeared during creation"))?,
        };
        let entry = self.root.bind_entry(&absolute, false)?;
        let (owner, _, mode) = self
            .root
            .open_bound_directory(&entry, identity)?
            .access_policy()?;
        if owner != unsafe { libc::geteuid() } || mode != 0o700 {
            bail!("task staging root has an unexpected owner or access policy");
        }
        Ok(())
    }

    /// Returns active previews whose persisted identity requires a live descriptor that is no
    /// longer available. Callers should pass only their bounded active-confirmation page.
    /// The check does not extend anchor TTLs or change persisted preview state.
    pub(crate) fn unconfirmable_preview_ids(
        &self,
        preview_keys: &[(String, String)],
    ) -> Result<HashSet<String>> {
        let mut unconfirmable = HashSet::new();
        if preview_keys.is_empty() {
            return Ok(unconfirmable);
        }

        let _process_guard = self
            .in_process_lock
            .lock()
            .map_err(|_| anyhow!("media-library lock is poisoned"))?;
        let _disk_guard = self.lock_store()?;
        let mut required_previews = Vec::new();
        let mut seen_ids = HashSet::new();
        for (preview_id, revision) in preview_keys {
            validate_identifier(preview_id)?;
            if !seen_ids.insert(preview_id.as_str()) {
                bail!("preview confirmability request contains a duplicate ID");
            }
            let Some(bytes) =
                self.read_private_file(&self.preview_path(preview_id), PREVIEW_LIMIT)?
            else {
                unconfirmable.insert(preview_id.clone());
                continue;
            };
            let persisted: PersistedPreview =
                serde_json::from_slice(&bytes).context("failed to parse persisted move preview")?;
            if persisted.version != 1 || persisted.preview.id != *preview_id {
                bail!("persisted preview identity is invalid");
            }
            if persisted.preview.revision != *revision {
                unconfirmable.insert(preview_id.clone());
                continue;
            }
            let required = required_preview_anchor_tokens(&persisted)?;
            if required.is_empty() {
                continue;
            }
            if persisted.process_nonce != self.process_nonce {
                unconfirmable.insert(preview_id.clone());
                continue;
            }
            required_previews.push((preview_id.clone(), revision.clone(), required));
        }

        if required_previews.is_empty() {
            return Ok(unconfirmable);
        }
        let Some(cache) = LIVE_PREVIEW_ANCHORS.get() else {
            unconfirmable.extend(required_previews.into_iter().map(|(id, _, _)| id));
            return Ok(unconfirmable);
        };
        let mut cache = cache
            .lock()
            .map_err(|_| anyhow!("preview identity-anchor cache is poisoned"))?;
        cache.prune_expired(Instant::now());
        for (preview_id, revision, required) in required_previews {
            if !cache.matches_required_anchors(&preview_id, &revision, &required)? {
                unconfirmable.insert(preview_id);
            }
        }
        Ok(unconfirmable)
    }

    #[cfg(test)]
    pub(crate) fn mark_next_scan_directory_incomplete(&self, directory: &str) {
        self.scan_test_hooks
            .lock()
            .expect("scan test-hook lock should remain healthy")
            .incomplete_directories
            .insert(directory.to_string());
    }

    #[cfg(test)]
    pub(crate) fn limit_next_scan_entries_to(&self, limit: usize) {
        self.scan_test_hooks
            .lock()
            .expect("scan test-hook lock should remain healthy")
            .max_entries = Some(limit);
    }

    #[cfg(test)]
    pub(crate) fn limit_next_scan_media_items_to(&self, limit: usize) {
        self.scan_test_hooks
            .lock()
            .expect("scan test-hook lock should remain healthy")
            .max_media_items = Some(limit);
    }

    #[cfg(test)]
    pub(crate) fn mark_next_scan_scope_unknown(&self) {
        self.scan_test_hooks
            .lock()
            .expect("scan test-hook lock should remain healthy")
            .scope_unknown = true;
    }

    #[cfg(test)]
    pub(crate) fn expire_next_scan_after_built_item(&self) {
        self.scan_test_hooks
            .lock()
            .expect("scan test-hook lock should remain healthy")
            .expire_after_built_item = true;
    }

    #[cfg(test)]
    fn expire_next_scan_after_attachments(&self, count: usize) {
        self.scan_test_hooks
            .lock()
            .expect("scan test-hook lock should remain healthy")
            .expire_after_attachment_count = Some(count.max(1));
    }

    #[cfg(test)]
    fn limit_next_scan_index_to(&self, bytes: usize) {
        self.scan_test_hooks
            .lock()
            .expect("scan test-hook lock should remain healthy")
            .index_budget = Some(bytes);
    }

    #[cfg(test)]
    fn track_next_scan_identity_reconciliation(&self) -> Arc<AtomicUsize> {
        let steps = Arc::new(AtomicUsize::new(0));
        self.scan_test_hooks
            .lock()
            .expect("scan test-hook lock should remain healthy")
            .identity_reconciliation_steps = Some(Arc::clone(&steps));
        steps
    }

    #[cfg(test)]
    pub(crate) fn make_preview_creation_time_unavailable_for_test(&self) {
        self.preview_creation_time_unavailable
            .store(true, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub fn import_legacy(&self, input: &str) -> Result<LegacyImportReport> {
        self.import_legacy_with_hints(input, &[])
    }

    pub fn import_legacy_with_hints(
        &self,
        input: &str,
        hints: &[SourceHint],
    ) -> Result<LegacyImportReport> {
        if input.len() > LEGACY_INPUT_LIMIT {
            bail!("legacy import text exceeds the 1 MiB limit");
        }
        let mut report = parse_legacy_import(input);
        let snapshot = self.scan(hints)?;
        report.revision = Some(snapshot.revision.clone());
        populate_legacy_candidates(
            &mut report,
            &snapshot.items,
            DEFAULT_LEGACY_CANDIDATE_LIMITS,
        )?;
        Ok(report)
    }

    #[cfg(test)]
    pub fn preview(
        &self,
        item_ids: &[String],
        target_relative_dir: &str,
        rename: bool,
        conflict: ConflictPolicy,
    ) -> Result<MovePreview> {
        self.preview_with_patches(item_ids, Some(target_relative_dir), rename, conflict, &[])
    }

    #[cfg(test)]
    pub fn preview_with_patches(
        &self,
        item_ids: &[String],
        target_relative_dir: Option<&str>,
        rename: bool,
        conflict: ConflictPolicy,
        patches: &[MetadataPatch],
    ) -> Result<MovePreview> {
        self.preview_with_hints_and_patches(
            item_ids,
            target_relative_dir,
            rename,
            conflict,
            &[],
            patches,
        )
    }

    #[cfg(test)]
    pub fn preview_with_hints_and_patches(
        &self,
        item_ids: &[String],
        target_relative_dir: Option<&str>,
        rename: bool,
        conflict: ConflictPolicy,
        hints: &[SourceHint],
        patches: &[MetadataPatch],
    ) -> Result<MovePreview> {
        self.preview_with_hints_and_patches_and_target_device_resolver(
            PreviewBuildRequest {
                item_ids,
                target_relative_dir,
                rename,
                conflict,
                patches,
            },
            hints,
            |target_path| self.target_parent_device(target_path),
        )
    }

    /// Builds and persists a preview from one locked scan, returning that same snapshot.
    /// Validators run under the scan/store locks and must only inspect their supplied values.
    pub(crate) fn preview_with_hints_and_patches_and_snapshot_validator_and_preview_guard<V, G>(
        &self,
        request: PreviewBuildRequest<'_>,
        hints: &[SourceHint],
        validate_snapshot: V,
        validate_preview: G,
    ) -> Result<(MovePreview, LibrarySnapshot)>
    where
        V: FnOnce(&LibrarySnapshot) -> Result<()>,
        G: FnOnce(&MovePreview) -> Result<()>,
    {
        self.preview_with_hints_and_patches_and_snapshot_validator_and_target_device_resolver_and_preview_guard(
            request,
            hints,
            validate_snapshot,
            |target_path| self.target_parent_device(target_path),
            validate_preview,
        )
    }

    /// Test-compatible preview API for callers that do not need a snapshot validator.
    #[cfg(test)]
    pub(crate) fn preview_with_hints_and_patches_and_snapshot_validator<V>(
        &self,
        request: PreviewBuildRequest<'_>,
        hints: &[SourceHint],
        validate_snapshot: V,
    ) -> Result<(MovePreview, LibrarySnapshot)>
    where
        V: FnOnce(&LibrarySnapshot) -> Result<()>,
    {
        self.preview_with_hints_and_patches_and_snapshot_validator_and_preview_guard(
            request,
            hints,
            validate_snapshot,
            |_| Ok(()),
        )
    }

    fn target_parent_device(&self, target_path: &str) -> Result<u64> {
        Ok(
            resolve_target_parent(&self.root, target_path, Some(self.library_dir_identity))?
                .identity
                .device(),
        )
    }

    #[cfg(test)]
    fn preview_with_hints_and_patches_and_target_device_resolver<F>(
        &self,
        request: PreviewBuildRequest<'_>,
        hints: &[SourceHint],
        target_device_for_path: F,
    ) -> Result<MovePreview>
    where
        F: FnMut(&str) -> Result<u64>,
    {
        self.preview_with_hints_and_patches_and_snapshot_validator_and_target_device_resolver(
            request,
            hints,
            |_| Ok(()),
            target_device_for_path,
        )
        .map(|(preview, _)| preview)
    }

    #[cfg(test)]
    fn preview_with_hints_and_patches_and_snapshot_validator_and_target_device_resolver<F, V>(
        &self,
        request: PreviewBuildRequest<'_>,
        hints: &[SourceHint],
        validate_snapshot: V,
        target_device_for_path: F,
    ) -> Result<(MovePreview, LibrarySnapshot)>
    where
        F: FnMut(&str) -> Result<u64>,
        V: FnOnce(&LibrarySnapshot) -> Result<()>,
    {
        self.preview_with_hints_and_patches_and_snapshot_validator_and_target_device_resolver_and_preview_guard(
            request,
            hints,
            validate_snapshot,
            target_device_for_path,
            |_| Ok(()),
        )
    }

    fn preview_with_hints_and_patches_and_snapshot_validator_and_target_device_resolver_and_preview_guard<
        F,
        V,
        G,
    >(
        &self,
        request: PreviewBuildRequest<'_>,
        hints: &[SourceHint],
        validate_snapshot: V,
        mut target_device_for_path: F,
        validate_preview: G,
    ) -> Result<(MovePreview, LibrarySnapshot)>
    where
        F: FnMut(&str) -> Result<u64>,
        V: FnOnce(&LibrarySnapshot) -> Result<()>,
        G: FnOnce(&MovePreview) -> Result<()>,
    {
        let _process_guard = self
            .in_process_lock
            .lock()
            .map_err(|_| anyhow!("media-library lock is poisoned"))?;
        let _disk_guard = self.lock_store()?;
        let snapshot = self.scan_locked(hints)?;
        validate_snapshot(&snapshot)?;
        let preview = self.build_and_persist_preview_with_guard(
            &snapshot,
            request,
            &mut target_device_for_path,
            validate_preview,
        )?;
        Ok((preview, snapshot))
    }

    #[cfg(test)]
    fn scan_attempt_count_for_test(&self) -> usize {
        self.scan_attempts.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn preview_with_target_device_resolver(
        &self,
        item_ids: &[String],
        target_relative_dir: &str,
        rename: bool,
        conflict: ConflictPolicy,
        target_device_for_path: impl FnMut(&str) -> Result<u64>,
    ) -> Result<MovePreview> {
        self.preview_with_hints_and_patches_and_target_device_resolver(
            PreviewBuildRequest {
                item_ids,
                target_relative_dir: Some(target_relative_dir),
                rename,
                conflict,
                patches: &[],
            },
            &[],
            target_device_for_path,
        )
    }

    /// Executes a confirmation using its persisted private preview and exact revision.
    pub fn execute_preview(&self, preview_id: &str, revision: &str) -> Result<BatchMoveResult> {
        validate_identifier(preview_id)?;
        let _process_guard = self
            .in_process_lock
            .lock()
            .map_err(|_| anyhow!("media-library lock is poisoned"))?;
        let _disk_guard = self.lock_store()?;
        for warning in self.prune_persisted_operation_states() {
            eprintln!("persisted operation-state cleanup: {warning}");
        }
        let persisted = self.read_persisted_preview(preview_id)?;
        if persisted.preview.revision != revision {
            release_preview_anchors(preview_id);
            bail!("move preview revision changed; scan and preview the selected items again");
        }
        if let Some(contents) =
            self.read_private_file(&self.batch_path(preview_id), MANIFEST_LIMIT)?
        {
            let manifest: BatchManifest = serde_json::from_slice(&contents)
                .context("failed to parse persistent move batch manifest")?;
            if manifest.preview.id != preview_id || manifest.preview.revision != revision {
                bail!("persistent move batch does not match the submitted preview");
            }
            if manifest.status == BatchStatus::Complete
                && !manifest
                    .items
                    .iter()
                    .any(|item| item.outcome == MoveOutcome::Skip)
            {
                release_preview_anchors(preview_id);
                return Ok(batch_result(&manifest));
            }
        }
        require_preview_anchors(&persisted, &self.process_nonce)?;
        let result = self.execute_persisted(persisted)?;
        if result.status == BatchStatus::Complete {
            release_preview_anchors(preview_id);
        }
        Ok(result)
    }

    #[cfg(test)]
    fn build_and_persist_preview(
        &self,
        snapshot: &LibrarySnapshot,
        request: PreviewBuildRequest<'_>,
        target_device_for_path: &mut impl FnMut(&str) -> Result<u64>,
    ) -> Result<MovePreview> {
        self.build_and_persist_preview_with_guard(snapshot, request, target_device_for_path, |_| {
            Ok(())
        })
    }

    fn build_and_persist_preview_with_guard<G>(
        &self,
        snapshot: &LibrarySnapshot,
        request: PreviewBuildRequest<'_>,
        target_device_for_path: &mut impl FnMut(&str) -> Result<u64>,
        validate_preview: G,
    ) -> Result<MovePreview>
    where
        G: FnOnce(&MovePreview) -> Result<()>,
    {
        let PreviewBuildRequest {
            item_ids,
            target_relative_dir,
            rename,
            conflict,
            patches,
        } = request;
        if item_ids.is_empty() && patches.is_empty() {
            bail!("select at least one media item or metadata patch");
        }
        let target_dir = target_relative_dir
            .map(|value| self.safe_relative_path(value, true))
            .transpose()?;
        let target_relative_dir = target_dir
            .as_deref()
            .map(path_to_relative_string)
            .transpose()?;
        let mut unique_ids = HashSet::new();
        if item_ids.iter().any(|id| !unique_ids.insert(id.as_str())) {
            bail!("a media item was selected more than once");
        }
        let items_by_id = snapshot
            .items
            .iter()
            .map(|item| (item.id.as_str(), item))
            .collect::<HashMap<_, _>>();
        let mut move_items = Vec::new();
        let mut reserved_targets = TargetReservationSet {
            private_library_identity: Some(self.library_dir_identity),
            ..TargetReservationSet::default()
        };
        let organized_target_dirs = match target_dir.as_deref() {
            Some(target_dir) => organized_target_directories(
                &snapshot.items,
                target_dir,
                &self.root,
                &mut reserved_targets,
            )?,
            None => HashMap::new(),
        };
        let mut preview_anchors = HashMap::new();
        for item_id in item_ids {
            let item = items_by_id
                .get(item_id.as_str())
                .ok_or_else(|| anyhow!("selected media item is not in the current library"))?;
            if item.presence != Presence::Present {
                bail!(
                    "selected media item is missing or unavailable: {}",
                    item.relative_path
                );
            }
            if target_dir.is_none() {
                bail!("a target directory is required for media moves");
            }
            let item_target_dir = organized_target_dirs
                .get(&item.id)
                .cloned()
                .ok_or_else(|| anyhow!("selected media item has no organized target directory"))?;
            let target_depth = item_target_dir.components().count();
            if target_depth > MAX_DEPTH {
                bail!(
                    "move target directory exceeds the scan depth limit of {MAX_DEPTH} components"
                );
            }
            let (files, anchors) = self.preview_files_for_item(
                item,
                &item_target_dir,
                rename,
                conflict,
                &mut reserved_targets,
                target_device_for_path,
            )?;
            preview_anchors.extend(anchors);
            let main_target = files
                .iter()
                .find(|file| file.source_path == item.relative_path)
                .ok_or_else(|| anyhow!("move preview lost the selected primary media"))?
                .target_path
                .clone();
            let outcome = if files
                .iter()
                .all(|file| file.source_path == file.target_path)
            {
                MoveOutcome::NoOp
            } else if files.iter().any(|file| file.identity.is_empty()) {
                MoveOutcome::Skip
            } else {
                MoveOutcome::Move
            };
            let reason = (outcome == MoveOutcome::Skip)
                .then(|| "one or more target names already exist".to_string());
            if outcome == MoveOutcome::Move {
                for file in &files {
                    reserved_targets.insert(&self.root, &file.target_path)?;
                }
            }
            move_items.push(MovePreviewItem {
                item_id: item.id.clone(),
                source_path: item.relative_path.clone(),
                target_path: main_target,
                outcome,
                files,
                reason,
            });
        }

        let mut patch_ids = HashSet::new();
        let mut prepared_patches = Vec::new();
        let move_targets = move_items
            .iter()
            .map(|item| (item.item_id.as_str(), item))
            .collect::<HashMap<_, _>>();
        let mut move_file_owners = HashMap::new();
        for item in move_items
            .iter()
            .filter(|item| item.outcome == MoveOutcome::Move)
        {
            for file in &item.files {
                let key = reserved_targets.key_for(&self.root, &file.target_path)?;
                if let Some(previous_owner) = move_file_owners.insert(key, item.item_id.as_str())
                    && previous_owner != item.item_id
                {
                    bail!("move preview contains an ambiguous destination name");
                }
            }
        }
        let mut source_nfo_owners: HashMap<TargetReservationKey, Vec<&str>> = HashMap::new();
        if !patches.is_empty() {
            for source_item in &snapshot.items {
                let source_media = self.absolute_path(&source_item.relative_path)?;
                let canonical_nfo = with_extension(&source_media, "nfo");
                let mut source_nfo_paths = vec![path_to_relative_string(
                    canonical_nfo.strip_prefix(self.root.logical_root_path())?,
                )?];
                source_nfo_paths.extend(
                    source_item
                        .attachments
                        .iter()
                        .filter(|attachment| attachment.kind == "nfo")
                        .map(|attachment| attachment.relative_path.clone()),
                );
                if let Some(authoritative) = &source_item.authoritative_nfo_path {
                    source_nfo_paths.push(authoritative.clone());
                }
                source_nfo_paths.sort();
                source_nfo_paths.dedup();
                for source_nfo_relative in source_nfo_paths {
                    let key = reserved_targets.key_for(&self.root, &source_nfo_relative)?;
                    source_nfo_owners
                        .entry(key)
                        .or_default()
                        .push(source_item.id.as_str());
                }
            }
        }
        let mut patch_target_owners: HashMap<TargetReservationKey, &str> = HashMap::new();
        let mut patch_previews = Vec::new();
        for patch in patches {
            if !patch_ids.insert(patch.item_id.as_str()) {
                bail!("a media item has more than one metadata patch");
            }
            let item = items_by_id
                .get(patch.item_id.as_str())
                .ok_or_else(|| anyhow!("metadata patch item is not in the current library"))?;
            if item.presence != Presence::Present {
                bail!(
                    "metadata patch item is missing or unavailable: {}",
                    item.relative_path
                );
            }
            validate_metadata_patch(patch)?;
            if patch.fill_missing_only
                && item.collection.as_ref().is_some_and(|stored| {
                    patch.collection.as_ref().is_some_and(|requested| {
                        !fill_only_collection_patch_preserves_existing(stored, requested)
                    })
                })
            {
                bail!("fill-only metadata patch cannot replace a stored collection");
            }
            let source_media = self.absolute_path(&item.relative_path)?;
            let BoundFileRead {
                token: media_identity,
                file: media_file,
                ..
            } = self
                .read_file_with_token_and_handle(&source_media, None)?
                .ok_or_else(|| anyhow!("selected media disappeared before metadata preview"))?;
            preview_anchors.insert(media_identity.clone(), media_file);
            let move_target = move_targets
                .get(patch.item_id.as_str())
                .filter(|entry| entry.outcome == MoveOutcome::Move);
            if let Some(authoritative) = &item.authoritative_nfo_path
                && !item.attachments.iter().any(|attachment| {
                    attachment.kind == "nfo" && attachment.relative_path == *authoritative
                })
            {
                bail!("selected authoritative NFO is not attached to the scanned media item");
            }
            let target_media = move_target
                .map(|entry| entry.target_path.as_str())
                .unwrap_or(&item.relative_path);
            let source_nfo_relative = match &item.authoritative_nfo_path {
                Some(path) => path.clone(),
                None => {
                    let source_nfo = with_extension(&source_media, "nfo");
                    path_to_relative_string(
                        source_nfo.strip_prefix(self.root.logical_root_path())?,
                    )?
                }
            };
            let target_nfo_relative = match (&item.authoritative_nfo_path, move_target) {
                (Some(source_nfo), Some(move_item)) => move_item
                    .files
                    .iter()
                    .find(|file| file.source_path == *source_nfo)
                    .map(|file| file.target_path.clone())
                    .ok_or_else(|| anyhow!("authoritative NFO was omitted from the move plan"))?,
                (Some(source_nfo), None) => source_nfo.clone(),
                (None, _) => {
                    let target_nfo = with_extension(&self.absolute_path(target_media)?, "nfo");
                    path_to_relative_string(
                        target_nfo.strip_prefix(self.root.logical_root_path())?,
                    )?
                }
            };
            let source_nfo = self.absolute_path(&source_nfo_relative)?;
            let target_nfo_key = reserved_targets.key_for(&self.root, &target_nfo_relative)?;
            if source_nfo_owners
                .get(&target_nfo_key)
                .is_some_and(|owners| owners.iter().any(|owner| *owner != item.id.as_str()))
            {
                bail!(
                    "metadata patch destination is also a source sidecar candidate for another media item: {target_nfo_relative}"
                );
            }
            if move_file_owners
                .get(&target_nfo_key)
                .is_some_and(|owner| *owner != item.id.as_str())
            {
                bail!(
                    "metadata patch destination conflicts with another media item's move target: {target_nfo_relative}"
                );
            }
            if patch_target_owners
                .insert(target_nfo_key, item.id.as_str())
                .is_some()
            {
                bail!(
                    "multiple metadata patches resolve to the same NFO target: {target_nfo_relative}"
                );
            }
            let existing =
                self.read_file_with_token_and_handle(&source_nfo, Some(SIDE_CAR_LIMIT))?;
            let (expected_identity, expected_sha256, expected_mode, original) = match existing {
                Some(BoundFileRead {
                    token,
                    digest: sha,
                    contents: bytes,
                    file,
                }) => {
                    let decoded = decode_identity_token(&token)?;
                    preview_anchors.insert(token.clone(), file);
                    (
                        Some(token),
                        Some(sha),
                        Some((decoded.mode & 0o777) as u16),
                        Some(bytes),
                    )
                }
                None if item.authoritative_nfo_path.is_some() => {
                    bail!("authoritative NFO disappeared before metadata preview")
                }
                None => (None, None, None, None),
            };
            let new_contents = match original.as_deref() {
                Some(original) => {
                    let source = std::str::from_utf8(original)
                        .context("selected NFO is not UTF-8; no metadata patch was prepared")?;
                    if patch.organization_status == Some(OrganizationStatus::Standalone)
                        && (item.collection.is_some() || nfo_has_collection_metadata(source))
                    {
                        bail!("a media item with collection metadata cannot be marked standalone");
                    }
                    append_metadata_patch(source, patch)?.into_bytes()
                }
                None => {
                    if patch.organization_status == Some(OrganizationStatus::Standalone)
                        && item.collection.is_some()
                    {
                        bail!("a media item with collection metadata cannot be marked standalone");
                    }
                    new_nfo(patch.title.as_deref().unwrap_or(&item.title), patch).into_bytes()
                }
            };
            let after_sha256 = sha256_bytes(&new_contents);
            let after_text =
                std::str::from_utf8(&new_contents).context("prepared NFO patch is not UTF-8")?;
            let before_text = original
                .as_deref()
                .map(|bytes| std::str::from_utf8(bytes).context("selected NFO is not UTF-8"))
                .transpose()?;
            let changes = metadata_field_changes(before_text, after_text);
            let changed = expected_sha256.as_deref() != Some(after_sha256.as_str());
            if changed && let Some(expected_identity) = expected_identity.as_deref() {
                let token = decode_identity_token(expected_identity)?;
                if token.uid != unsafe { libc::geteuid() as u32 }
                    || token.gid != unsafe { libc::getegid() as u32 }
                    || token.mode & 0o7000 != 0
                {
                    bail!(
                        "existing NFO ownership or special permissions cannot be preserved safely"
                    );
                }
            }
            let existing_ids = original
                .as_deref()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .map(nfo_source_ids)
                .unwrap_or_default();
            patch_previews.push(MetadataPatchPreview {
                item_id: item.id.clone(),
                nfo_path: target_nfo_relative.clone(),
                changed,
                added_source_ids: missing_patch_source_ids(existing_ids, &patch.source_ids),
                changes,
            });
            prepared_patches.push(PreparedNfoPatch {
                patch: patch.clone(),
                target_path: target_nfo_relative,
                media_target_path: target_media.to_string(),
                media_identity,
                expected_identity,
                expected_sha256,
                expected_mode,
                after_sha256,
                replacement: new_contents,
                replacement_checkpoint: None,
                state: ManifestPatchState::Pending,
            });
        }

        let id = new_operation_id("preview");
        let preview = MovePreview {
            id: id.clone(),
            revision: snapshot.revision.clone(),
            target_relative_dir,
            rename,
            conflict,
            items: move_items,
            metadata_patches: patch_previews,
        };
        let persisted = PersistedPreview {
            version: 1,
            process_nonce: self.process_nonce.clone(),
            preview: preview.clone(),
            patches: prepared_patches,
        };
        let contents = serde_json::to_vec(&persisted).context("failed to encode move preview")?;
        if contents.len() > PREVIEW_LIMIT {
            bail!("move preview exceeds its private storage limit");
        }
        validate_preview(&preview)?;
        store_preview_anchors(&id, &preview.revision, preview_anchors)?;
        if let Err(error) = self.write_private_file(&self.preview_path(&id), &contents) {
            release_preview_anchors(&id);
            return Err(error);
        }
        Ok(preview)
    }

    fn preview_files_for_item(
        &self,
        item: &LibraryItem,
        target_dir: &Path,
        rename: bool,
        conflict: ConflictPolicy,
        reserved_targets: &mut TargetReservationSet,
        target_device_for_path: &mut impl FnMut(&str) -> Result<u64>,
    ) -> Result<(Vec<FileMovePreview>, HashMap<String, BoundFile>)> {
        let primary = self.absolute_path(&item.relative_path)?;
        let mut source_paths = vec![item.relative_path.clone()];
        source_paths.extend(
            item.attachments
                .iter()
                .map(|attachment| attachment.relative_path.clone()),
        );
        source_paths.sort_by(|left, right| {
            let left_primary = left == &item.relative_path;
            let right_primary = right == &item.relative_path;
            left_primary
                .cmp(&right_primary)
                .then_with(|| left.cmp(right))
        });
        let primary_stem = primary
            .file_stem()
            .and_then(OsStr::to_str)
            .ok_or_else(|| anyhow!("selected media has a non-UTF-8 filename"))?;
        let primary_parent = Path::new(&item.relative_path)
            .parent()
            .unwrap_or_else(|| Path::new(""));
        let primary_parent_absolute = self.root.logical_root_path().join(primary_parent);
        let name_semantics = self.coordinate_read(&primary_parent_absolute, |coordinated| {
            if self.path_in_root(coordinated)? != primary_parent_absolute {
                bail!("coordinated attachment directory changed during preview");
            }
            let identity = if primary_parent.as_os_str().is_empty() {
                self.root.root_identity()
            } else {
                self.root
                    .entry_identity(&primary_parent_absolute)?
                    .ok_or_else(|| anyhow!("attachment directory disappeared during preview"))?
            };
            if !identity.is_dir() {
                bail!("attachment path is no longer a directory");
            }
            let directory = open_validated_target_directory(&self.root, primary_parent, identity)?;
            target_name_semantics(&directory)
                .context("filesystem name semantics unavailable for attachment matching")
        })?;
        let requested_stem = if rename {
            standard_media_stem(item)
        } else {
            primary_stem.to_string()
        };
        let mut planned_sources = Vec::with_capacity(source_paths.len());
        let mut longest_filename_tail = 0;
        for source_rel in &source_paths {
            let source_abs = self.absolute_path(source_rel)?;
            let source_stem = source_abs
                .file_stem()
                .and_then(OsStr::to_str)
                .ok_or_else(|| anyhow!("attached media has a non-UTF-8 filename"))?;
            let suffix = if source_rel == &item.relative_path {
                String::new()
            } else {
                matching_sidecar_suffix_with(
                    source_stem,
                    primary_stem,
                    name_semantics,
                    platform_case_fold,
                )?
                .unwrap_or_default()
            };
            let tail_bytes = filename_tail_bytes(&source_abs, &suffix)?;
            longest_filename_tail = longest_filename_tail.max(tail_bytes);
            planned_sources.push((source_abs, suffix));
        }
        let target_stem = budgeted_group_stem(&requested_stem, longest_filename_tail, "", rename)?;
        let base_target = target_dir.join(file_name_with_stem(&primary, &target_stem, "")?);
        let targets_for = |primary_target: &Path| -> Result<Vec<String>> {
            let target_primary_stem = primary_target
                .file_stem()
                .and_then(OsStr::to_str)
                .ok_or_else(|| anyhow!("target media filename is invalid"))?;
            planned_sources
                .iter()
                .map(|(source_abs, suffix)| {
                    let target_name = file_name_with_stem(source_abs, target_primary_stem, suffix)?;
                    path_to_relative_string(&target_dir.join(target_name))
                })
                .collect()
        };
        let mut candidate = base_target.clone();
        let mut targets = targets_for(&candidate)?;
        let conflicts = self.has_move_conflicts(&source_paths, &targets, reserved_targets)?;
        if conflicts && conflict == ConflictPolicy::KeepBoth {
            let parent = base_target.parent().unwrap_or_else(|| Path::new(""));
            let mut found = false;
            for suffix in 2..=10_000 {
                let keep_both_suffix = format!(" ({suffix})");
                let candidate_stem = budgeted_group_stem(
                    &requested_stem,
                    longest_filename_tail,
                    &keep_both_suffix,
                    true,
                )?;
                let name = file_name_with_stem(&primary, &candidate_stem, "")?;
                candidate = parent.join(name);
                targets = targets_for(&candidate)?;
                if !self.has_move_conflicts(&source_paths, &targets, reserved_targets)? {
                    found = true;
                    break;
                }
            }
            if !found {
                bail!("could not find a free keep-both filename");
            }
        }
        let final_conflicts = self.has_move_conflicts(&source_paths, &targets, reserved_targets)?;
        let mut files = Vec::new();
        let mut source_devices = Vec::new();
        let mut anchors = HashMap::new();
        for (source_rel, target_rel) in source_paths.iter().zip(targets) {
            let source_abs = self.absolute_path(source_rel)?;
            let skip = final_conflicts && conflict == ConflictPolicy::Skip;
            let (identity, size, source_device) = if skip {
                let fingerprint = self.file_fingerprint(&source_abs)?.ok_or_else(|| {
                    anyhow!("selected media or attachment disappeared: {source_rel}")
                })?;
                (
                    String::new(),
                    fingerprint.size,
                    fingerprint.identity.device(),
                )
            } else {
                let BoundFileRead { token, file, .. } = self
                    .read_file_with_token_and_handle(&source_abs, None)?
                    .ok_or_else(|| {
                        anyhow!("selected media or attachment disappeared: {source_rel}")
                    })?;
                let size = token_size(&token)?;
                let source_device = file.identity().device();
                anchors.insert(token.clone(), file);
                (token, size, source_device)
            };
            source_devices.push(source_device);
            files.push(FileMovePreview {
                source_path: source_rel.clone(),
                target_path: target_rel,
                bytes: size,
                identity,
            });
        }
        if final_conflicts && conflict == ConflictPolicy::Skip {
            return Ok((files, anchors));
        }
        for ((source_path, file), source_device) in
            source_paths.iter().zip(&files).zip(source_devices)
        {
            if source_path == &file.target_path {
                continue;
            }
            let target_device = target_device_for_path(&file.target_path)?;
            ensure_same_device_for_rename(
                source_path,
                &file.target_path,
                source_device,
                target_device,
            )?;
        }
        files.sort_by(|left, right| {
            let left_primary = left.source_path == item.relative_path;
            let right_primary = right.source_path == item.relative_path;
            left_primary
                .cmp(&right_primary)
                .then_with(|| left.source_path.cmp(&right.source_path))
        });
        Ok((files, anchors))
    }

    fn has_move_conflicts(
        &self,
        source_paths: &[String],
        target_paths: &[String],
        reserved_targets: &mut TargetReservationSet,
    ) -> Result<bool> {
        for (source, target) in source_paths.iter().zip(target_paths) {
            if reserved_targets.contains(&self.root, target)? {
                return Ok(true);
            }
            if source == target {
                continue;
            }
            if safe_entry_identity(&self.root, &self.absolute_path(target)?)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn preview_path(&self, id: &str) -> PathBuf {
        self.library_dir.join(format!("{PREVIEW_PREFIX}{id}.json"))
    }

    fn preview_anchor(&self, preview_id: &str, identity: &str) -> Result<Option<BoundFile>> {
        let Some(cache) = LIVE_PREVIEW_ANCHORS.get() else {
            return Ok(None);
        };
        let mut cache = cache
            .lock()
            .map_err(|_| anyhow!("preview identity-anchor cache is poisoned"))?;
        let now = Instant::now();
        cache.prune_expired(now);
        Ok(cache
            .touch(preview_id, now)
            .and_then(|anchors| anchors.get(identity))
            .cloned())
    }

    fn batch_path(&self, id: &str) -> PathBuf {
        self.library_dir.join(format!("{BATCH_PREFIX}{id}.json"))
    }

    fn read_persisted_preview(&self, id: &str) -> Result<PersistedPreview> {
        let bytes = self
            .read_private_file(&self.preview_path(id), PREVIEW_LIMIT)?
            .ok_or_else(|| {
                anyhow!(
                    "move preview expired or is no longer available; scan and preview the selected items again"
                )
            })?;
        let persisted: PersistedPreview =
            serde_json::from_slice(&bytes).context("failed to parse persisted move preview")?;
        if persisted.version != 1 || persisted.preview.id != id {
            bail!("persisted preview identity is invalid");
        }
        Ok(persisted)
    }

    fn execute_persisted(&self, persisted: PersistedPreview) -> Result<BatchMoveResult> {
        let preview = persisted.preview;
        let batch_path = self.batch_path(&preview.id);
        let mut manifest = match self.read_private_file(&batch_path, MANIFEST_LIMIT)? {
            Some(bytes) => {
                let manifest: BatchManifest = serde_json::from_slice(&bytes)
                    .context("failed to parse persistent move batch manifest")?;
                if manifest.version != 1
                    || manifest.preview.id != preview.id
                    || manifest.preview.revision != preview.revision
                {
                    bail!("persistent move batch does not match the confirmed preview");
                }
                if manifest.status == BatchStatus::Complete
                    && !manifest
                        .items
                        .iter()
                        .any(|item| item.outcome == MoveOutcome::Skip)
                {
                    return Ok(batch_result(&manifest));
                }
                manifest
            }
            None => {
                let files = preview
                    .items
                    .iter()
                    .filter(|item| item.outcome == MoveOutcome::Move)
                    .flat_map(|item| {
                        item.files.iter().map(|file| ManifestFile {
                            item_id: item.item_id.clone(),
                            source_path: file.source_path.clone(),
                            target_path: file.target_path.clone(),
                            identity: file.identity.clone(),
                            state: ManifestFileState::Pending,
                            error: None,
                        })
                    })
                    .collect();
                let items = preview
                    .items
                    .iter()
                    .map(|item| MoveResultItem {
                        item_id: item.item_id.clone(),
                        outcome: item.outcome,
                        moved_files: 0,
                        total_files: item.files.len(),
                        metadata_patched: false,
                        error: None,
                    })
                    .collect();
                BatchManifest {
                    version: 1,
                    preview: preview.clone(),
                    status: BatchStatus::Partial,
                    attempt_finished: false,
                    retryable_access_error: false,
                    files,
                    patches: persisted.patches.clone(),
                    items,
                    warnings: Vec::new(),
                }
            }
        };
        manifest.attempt_finished = false;
        manifest.retryable_access_error = false;
        for item in &mut manifest.items {
            item.moved_files = 0;
            item.error = None;
        }
        for file in &mut manifest.files {
            if file.state == ManifestFileState::Pending {
                file.error = None;
            }
        }
        self.write_batch_manifest(&batch_path, &manifest)?;

        for item_index in 0..manifest.items.len() {
            let item_id = manifest.items[item_index].item_id.clone();
            let item_files = manifest
                .files
                .iter()
                .enumerate()
                .filter(|(_, file)| file.item_id == item_id)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let mut failed = false;
            for file_index in item_files {
                if manifest.files[file_index].state == ManifestFileState::Moved {
                    match self
                        .validate_moved_file(&manifest.preview.id, &manifest.files[file_index])
                    {
                        Ok(()) => {
                            manifest.files[file_index].error = None;
                            manifest.items[item_index].moved_files += 1;
                        }
                        Err(error) => {
                            if is_file_provider_access_error(&error) {
                                manifest.retryable_access_error = true;
                            }
                            let message = format!(
                                "previously moved target no longer matches its approved object: {error:#}"
                            );
                            manifest.files[file_index].error = Some(message.clone());
                            manifest.items[item_index].error = Some(message);
                            failed = true;
                        }
                    }
                    self.write_batch_manifest(&batch_path, &manifest)?;
                    continue;
                }
                if failed {
                    continue;
                }
                match self.execute_file_move(&manifest.preview.id, &manifest.files[file_index]) {
                    Ok(MoveFileAction::Moved | MoveFileAction::AlreadyMoved) => {
                        manifest.files[file_index].state = ManifestFileState::Moved;
                        manifest.files[file_index].error = None;
                        manifest.items[item_index].moved_files += 1;
                    }
                    Err(error) => {
                        if is_file_provider_access_error(&error) {
                            manifest.retryable_access_error = true;
                        }
                        let message = format!("{error:#}");
                        manifest.files[file_index].error = Some(message.clone());
                        manifest.items[item_index].error = Some(message);
                        failed = true;
                    }
                }
                self.write_batch_manifest(&batch_path, &manifest)?;
            }
        }

        for patch_index in 0..manifest.patches.len() {
            if manifest.patches[patch_index].state == ManifestPatchState::Applied {
                continue;
            }
            let item_id = manifest.patches[patch_index].patch.item_id.clone();
            if !item_group_ready(&manifest, &item_id) {
                continue;
            }
            let preview_id = manifest.preview.id.clone();
            let patch = manifest.patches[patch_index].clone();
            let patch_result = self.execute_nfo_patch_with_checkpoint(
                &preview_id,
                &patch,
                &mut |replacement_checkpoint| {
                    manifest.patches[patch_index].replacement_checkpoint = replacement_checkpoint;
                    self.write_batch_manifest(&batch_path, &manifest)
                },
            );
            match patch_result {
                Ok(()) => {
                    manifest.patches[patch_index].state = ManifestPatchState::Applied;
                    if let Some(result) = manifest
                        .items
                        .iter_mut()
                        .find(|item| item.item_id == item_id)
                    {
                        result.metadata_patched = true;
                        result.error = None;
                    } else {
                        manifest.items.push(MoveResultItem {
                            item_id,
                            outcome: MoveOutcome::NoOp,
                            moved_files: 0,
                            total_files: 0,
                            metadata_patched: true,
                            error: None,
                        });
                    }
                }
                Err(error) => {
                    if is_file_provider_access_error(&error) {
                        manifest.retryable_access_error = true;
                    }
                    let message = format!("{error:#}");
                    manifest.warnings.push(message.clone());
                    if let Some(result) = manifest
                        .items
                        .iter_mut()
                        .find(|item| item.item_id == item_id)
                    {
                        result.error = Some(message);
                    } else {
                        manifest.items.push(MoveResultItem {
                            item_id,
                            outcome: MoveOutcome::NoOp,
                            moved_files: 0,
                            total_files: 0,
                            metadata_patched: false,
                            error: Some(message),
                        });
                    }
                }
            }
            self.write_batch_manifest(&batch_path, &manifest)?;
        }

        for item in &mut manifest.items {
            if item.outcome == MoveOutcome::Skip && item.error.is_none() {
                item.error = Some(skipped_move_error(&manifest.preview, &item.item_id));
            }
        }

        let has_errors = manifest.items.iter().any(|item| item.error.is_some())
            || manifest
                .items
                .iter()
                .any(|item| item.outcome == MoveOutcome::Skip)
            || manifest
                .files
                .iter()
                .any(|file| file.state == ManifestFileState::Pending)
            || manifest
                .patches
                .iter()
                .any(|patch| patch.state == ManifestPatchState::Pending);
        let has_success = manifest
            .files
            .iter()
            .any(|file| file.state == ManifestFileState::Moved)
            || manifest.items.iter().any(|item| item.metadata_patched);
        manifest.status = if !has_errors {
            BatchStatus::Complete
        } else if has_success {
            BatchStatus::Partial
        } else {
            BatchStatus::Failed
        };
        manifest.attempt_finished = true;
        self.write_batch_manifest(&batch_path, &manifest)?;
        Ok(batch_result(&manifest))
    }

    fn execute_file_move(&self, preview_id: &str, file: &ManifestFile) -> Result<MoveFileAction> {
        if file.source_path == file.target_path {
            return Ok(MoveFileAction::AlreadyMoved);
        }
        let source = self.absolute_path(&file.source_path)?;
        let target = self.absolute_path(&file.target_path)?;
        let expected = decode_identity_token(&file.identity)?;
        let source_identity = self.root.entry_identity(&source)?;
        let target_identity = safe_entry_identity(&self.root, &target)?;
        match (source_identity, target_identity) {
            (Some(_), Some(_)) => bail!(
                "move target appeared after preview; source and destination were left untouched"
            ),
            (None, Some(target_identity)) => {
                let Some(read) = self.read_file_with_token_and_handle(&target, None)? else {
                    bail!(
                        "move source is missing and destination does not match the approved object"
                    );
                };
                let anchor = self.preview_anchor(preview_id, &file.identity)?;
                if identity_tokens_match_with_pinned_file(
                    &file.identity,
                    &read.token,
                    &read.file,
                    Some(target_identity),
                    anchor.as_ref(),
                )? {
                    return Ok(MoveFileAction::AlreadyMoved);
                }
                bail!("move source is missing and destination does not match the approved object")
            }
            (None, None) => bail!("move source disappeared before it could be recovered"),
            (Some(current), None) => {
                if current.device() != expected.device
                    || current.inode() != expected.inode
                    || !current.is_file()
                {
                    bail!("move source object was replaced after preview");
                }
                let read = self
                    .read_file_with_token_and_handle(&source, None)?
                    .ok_or_else(|| anyhow!("move source disappeared during validation"))?;
                if current != read.file.identity() {
                    bail!("move source object was replaced during validation");
                }
                let anchor = self.preview_anchor(preview_id, &file.identity)?;
                if !identity_tokens_match_with_pinned_file(
                    &file.identity,
                    &read.token,
                    &read.file,
                    Some(current),
                    anchor.as_ref(),
                )? {
                    bail!("move source content or access policy changed after preview");
                }
                self.coordinate_move(&source, &target, |source_accessor, target_accessor| {
                    if source_accessor != source || target_accessor != target {
                        bail!("coordinated move paths changed after preview");
                    }
                    ensure_relative_parents(&self.root, &target)?;
                    let source_entry = self.root.bind_entry(&source, false)?;
                    let current = self.root.bound_entry_identity(&source_entry)?
                        .ok_or_else(|| anyhow!("move source disappeared before rename"))?;
                    if current.device() != expected.device || current.inode() != expected.inode || !current.is_file() {
                        bail!("move source object was replaced before rename");
                    }
                    let source_file = self.root.open_bound_file_if_identity(&source_entry, current)?;
                    let (token, _) = token_for_bound_file(&source_file)?;
                    if !identity_tokens_match_with_pinned_file(
                        &file.identity,
                        &token,
                        &source_file,
                        Some(current),
                        anchor.as_ref(),
                    )? {
                        bail!("move source content or access policy changed before rename");
                    }
                    let target_entry = self.root.bind_entry(&target, false)?;
                    self.root.rename_via_bound_parents_noreplace_if_identity(
                        &source_entry,
                        &target_entry,
                        current,
                    )?;
                    let moved = self.root.open_bound_file_if_identity(&target_entry, current)?;
                    let (after, _) = token_for_bound_file(&moved)?;
                    if !identity_tokens_match_with_pinned_file(
                        &file.identity,
                        &after,
                        &moved,
                        Some(current),
                        anchor.as_ref(),
                    )? {
                        bail!("moved file content or access policy changed during rename; destination retained for recovery");
                    }
                    Ok(())
                })?;
                Ok(MoveFileAction::Moved)
            }
        }
    }

    fn validate_moved_file(&self, preview_id: &str, file: &ManifestFile) -> Result<()> {
        let target = self.absolute_path(&file.target_path)?;
        let read = self
            .read_file_with_token_and_handle(&target, None)?
            .ok_or_else(|| anyhow!("previously moved target is missing"))?;
        let path_identity = self.root.entry_identity(&target)?;
        let anchor = self.preview_anchor(preview_id, &file.identity)?;
        if !identity_tokens_match_with_pinned_file(
            &file.identity,
            &read.token,
            &read.file,
            path_identity,
            anchor.as_ref(),
        )? {
            bail!("previously moved target was replaced or its content/access policy changed");
        }
        Ok(())
    }

    #[cfg(test)]
    fn execute_nfo_patch(&self, preview_id: &str, patch: &PreparedNfoPatch) -> Result<()> {
        self.execute_nfo_patch_with_checkpoint(preview_id, patch, &mut |_| Ok(()))
    }

    fn execute_nfo_patch_with_checkpoint(
        &self,
        preview_id: &str,
        patch: &PreparedNfoPatch,
        persist_checkpoint: &mut dyn FnMut(Option<NfoReplacementCheckpoint>) -> Result<()>,
    ) -> Result<()> {
        let media = self.absolute_path(&patch.media_target_path)?;
        let target = self.absolute_path(&patch.target_path)?;
        let media_anchor = self.preview_anchor(preview_id, &patch.media_identity)?;
        let content_limit = SIDE_CAR_LIMIT.max(patch.replacement.len());
        let mut checkpoint_for_execution = patch.replacement_checkpoint.clone();
        let checkpoint_stage = patch
            .replacement_checkpoint
            .as_ref()
            .map(|checkpoint| self.nfo_checkpoint_stage_path(patch, checkpoint))
            .transpose()?;
        self.coordinate_read(&media, |coordinated_media| {
            if coordinated_media != media {
                bail!("coordinated media path changed during confirmed metadata patch");
            }
            let media_file = self.root.open_bound_file(coordinated_media)?
                .ok_or_else(|| anyhow!("selected media disappeared before its NFO patch"))?;
            let (media_token, _) = token_for_bound_file(&media_file)?;
            let media_path_identity = self.root.entry_identity(&media)?;
            if !identity_tokens_match_with_pinned_file(
                &patch.media_identity,
                &media_token,
                &media_file,
                media_path_identity,
                media_anchor.as_ref(),
            )? {
                bail!("selected media object, content, or access policy changed after metadata preview");
            }

            if let Some(file) = self.root.open_bound_file(&target)? {
                if file.byte_len()? > content_limit as u64 {
                    bail!("NFO exceeds its bounded patch verification limit");
                }
                let (token, digest) = token_for_bound_file(&file)?;
                let path_identity = self.root.entry_identity(&target)?;
                if let (Some(checkpoint), Some(stage_path)) =
                    (patch.replacement_checkpoint.as_ref(), checkpoint_stage.as_deref())
                    && digest == patch.after_sha256
                {
                    if self.nfo_checkpoint_matches_file(
                        patch,
                        checkpoint,
                        &target,
                        &file,
                        &token,
                        &digest,
                    )? && path_identity == Some(file.identity())
                    {
                        self.remove_checkpointed_displaced_nfo(
                            preview_id,
                            patch,
                            checkpoint,
                            stage_path,
                        )?;
                        return Ok(());
                    }
                    bail!("NFO has the prepared bytes but does not match its journaled replacement object");
                }
                if digest == patch.after_sha256 {
                    if let Some(expected) = patch.expected_identity.as_deref() {
                        let nfo_anchor = self.preview_anchor(preview_id, expected)?;
                        if identity_tokens_match_with_pinned_file(
                            expected,
                            &token,
                            &file,
                            path_identity,
                            nfo_anchor.as_ref(),
                        )? && digest == patch.expected_sha256.as_deref().unwrap_or_default()
                        {
                            return Ok(());
                        }
                        bail!("NFO already has the prepared content but its approved object identity cannot be verified");
                    }
                    if self.created_nfo_matches_prepared(
                        &target, &file, &token, &digest, patch,
                    )? {
                        return Ok(());
                    }
                    bail!("NFO already has the prepared content but its create identity or access policy cannot be verified");
                }
                let contents = file.read_limited(SIDE_CAR_LIMIT)?;
                if sha256_bytes(&contents) != digest {
                    bail!("NFO content changed while preparing its confirmed patch");
                }
                let expected = patch.expected_identity.as_deref()
                    .ok_or_else(|| anyhow!("NFO destination appeared after preview; leaving it untouched"))?;
                let nfo_path_identity = path_identity;
                let nfo_anchor = self.preview_anchor(preview_id, expected)?;
                if !identity_tokens_match_with_pinned_file(
                    expected,
                    &token,
                    &file,
                    nfo_path_identity,
                    nfo_anchor.as_ref(),
                )? || digest != patch.expected_sha256.as_deref().unwrap_or_default() {
                    bail!("NFO changed after preview; leaving the current NFO untouched");
                }
                if let (Some(checkpoint), Some(stage_path)) =
                    (patch.replacement_checkpoint.as_ref(), checkpoint_stage.as_deref())
                {
                    self.remove_checkpointed_staged_nfo(patch, checkpoint, stage_path)?;
                    persist_checkpoint(None)?;
                    checkpoint_for_execution = None;
                }
                let entry = self.root.bind_entry(&target, false)?;
                let temporary = private_temp_sibling(&target);
                let mut replacement_checkpoint = None;
                let (_, replacement_identity) = self
                    .root
                    .replace_bound_file_atomically_if_identity_with_checkpoint(
                    &entry,
                    file.identity(),
                    &temporary,
                    &patch.replacement,
                    patch.expected_mode.unwrap_or(0o644),
                    content_limit,
                    &mut |staged_entry, staged_file, staged_identity| {
                        if staged_entry.path() != temporary
                            || staged_identity != staged_file.identity()
                            || !staged_identity.is_file()
                            || self.root.entry_identity(staged_entry.path())?
                                != Some(staged_identity)
                        {
                            bail!("staged NFO replacement path or identity changed before checkpoint");
                        }

                        let current_before = self.root.entry_identity(&target)?;
                        let (before_token, before_digest) = token_for_bound_file(&file)?;
                        let before_bytes = file.read_limited(content_limit)?;
                        if current_before != Some(file.identity())
                            || before_digest != patch.expected_sha256.as_deref().unwrap_or_default()
                            || sha256_bytes(&before_bytes) != before_digest
                            || !identity_tokens_match_with_pinned_file(
                                expected,
                                &before_token,
                                &file,
                                current_before,
                                nfo_anchor.as_ref(),
                            )?
                        {
                            bail!("NFO changed while its replacement was being staged");
                        }

                        let staged_bytes = staged_file.read_limited(content_limit)?;
                        let (after_identity, staged_digest) = token_for_bound_file(staged_file)?;
                        let after_token = decode_identity_token(&after_identity)?;
                        let expected_token = decode_identity_token(expected)?;
                        let expected_mode = patch.expected_mode.unwrap_or(0o644);
                        if staged_bytes != patch.replacement
                            || staged_digest != patch.after_sha256
                            || after_token.size != patch.replacement.len() as u64
                            || after_token.uid != expected_token.uid
                            || after_token.gid != expected_token.gid
                            || after_token.mode & 0o777 != u32::from(expected_mode)
                            || after_token.mode & 0o7000 != 0
                        {
                            bail!("staged NFO replacement content or access policy changed before checkpoint");
                        }
                        let staged_path = path_to_relative_string(
                            staged_entry
                                .path()
                                .strip_prefix(self.root.logical_root_path())?,
                        )?;
                        let checkpoint = NfoReplacementCheckpoint {
                            staged_path,
                            after_identity,
                        };
                        persist_checkpoint(Some(checkpoint.clone()))?;
                        replacement_checkpoint = Some(checkpoint);
                        Ok(())
                    },
                )?;
                let checkpoint = replacement_checkpoint.as_ref().ok_or_else(|| {
                    anyhow!("atomic NFO replacement completed without a persisted identity checkpoint")
                })?;
                if replacement_identity.device()
                    != decode_identity_token(&checkpoint.after_identity)?.device
                    || replacement_identity.inode()
                        != decode_identity_token(&checkpoint.after_identity)?.inode
                {
                    bail!("atomic NFO replacement returned an unexpected object identity");
                }
                checkpoint_for_execution = Some(checkpoint.clone());
                self.remove_checkpointed_displaced_nfo(
                    preview_id,
                    patch,
                    checkpoint,
                    &temporary,
                )?;
            } else {
                if patch.expected_sha256.is_some() {
                    bail!("original NFO disappeared before patch; leaving it untouched");
                }
                self.root.create_new_bound_file(&target, &patch.replacement, 0o644)?;
            }
            let updated = self.root.open_bound_file(&target)?
                .ok_or_else(|| anyhow!("confirmed NFO patch disappeared"))?;
            if updated.byte_len()? > content_limit as u64 {
                bail!("confirmed NFO exceeds its bounded patch verification limit");
            }
            let (token, digest) = token_for_bound_file(&updated)?;
            if digest != patch.after_sha256 {
                bail!("confirmed NFO patch did not match its prepared content");
            }
            if patch.expected_identity.is_some() {
                let checkpoint = checkpoint_for_execution.as_ref().ok_or_else(|| {
                    anyhow!("original NFO replacement has no persisted identity checkpoint")
                })?;
                if !self.nfo_checkpoint_matches_file(
                    patch,
                    checkpoint,
                    &target,
                    &updated,
                    &token,
                    &digest,
                )? || self.root.entry_identity(&target)? != Some(updated.identity())
                {
                    bail!("confirmed NFO replacement does not match its journaled object identity");
                }
            } else if !self.created_nfo_matches_prepared(
                &target,
                &updated,
                &token,
                &digest,
                patch,
            )? {
                bail!("created NFO does not match its expected access policy");
            }
            let (media_after, _) = token_for_bound_file(&media_file)?;
            let media_path_identity = self.root.entry_identity(&media)?;
            if !identity_tokens_match_with_pinned_file(
                &patch.media_identity,
                &media_after,
                &media_file,
                media_path_identity,
                media_anchor.as_ref(),
            )? || media_path_identity != Some(media_file.identity()) {
                bail!("selected media changed while its NFO was being patched");
            }
            Ok(())
        })
    }

    fn nfo_checkpoint_stage_path(
        &self,
        patch: &PreparedNfoPatch,
        checkpoint: &NfoReplacementCheckpoint,
    ) -> Result<PathBuf> {
        validate_nfo_checkpoint_stage_path(&patch.target_path, &checkpoint.staged_path)?;
        let token = decode_identity_token(&checkpoint.after_identity)?;
        let expected = patch
            .expected_identity
            .as_deref()
            .ok_or_else(|| anyhow!("NFO replacement checkpoint has no approved before object"))?;
        let before = decode_identity_token(expected)?;
        let digest = checkpoint
            .after_identity
            .rsplit(':')
            .next()
            .ok_or_else(|| anyhow!("NFO replacement checkpoint digest is missing"))?;
        let requested_mode = patch.expected_mode.unwrap_or(0o644);
        if digest != patch.after_sha256
            || token.size != patch.replacement.len() as u64
            || token.uid != before.uid
            || token.gid != before.gid
            || token.mode & 0o777 != u32::from(requested_mode)
            || token.mode & 0o7000 != 0
            || before.uid != unsafe { libc::geteuid() as u32 }
            || before.gid != unsafe { libc::getegid() as u32 }
        {
            bail!(
                "NFO replacement checkpoint does not match its prepared content or access policy"
            );
        }
        self.absolute_path(&checkpoint.staged_path)
    }

    fn nfo_checkpoint_matches_file(
        &self,
        patch: &PreparedNfoPatch,
        checkpoint: &NfoReplacementCheckpoint,
        path: &Path,
        file: &BoundFile,
        token: &str,
        digest: &str,
    ) -> Result<bool> {
        let _ = self.nfo_checkpoint_stage_path(patch, checkpoint)?;
        let expected = decode_identity_token(&checkpoint.after_identity)?;
        let current = decode_identity_token(token)?;
        let path_identity = self.root.entry_identity(path)?;
        Ok(token == checkpoint.after_identity
            && digest == patch.after_sha256
            && current.device == expected.device
            && current.inode == expected.inode
            && current.size == expected.size
            && current.uid == expected.uid
            && current.gid == expected.gid
            && current.mode == expected.mode
            && path_identity == Some(file.identity())
            && file.identity().is_file())
    }

    fn remove_checkpointed_staged_nfo(
        &self,
        patch: &PreparedNfoPatch,
        checkpoint: &NfoReplacementCheckpoint,
        stage_path: &Path,
    ) -> Result<()> {
        let Some(file) = self.root.open_bound_file(stage_path)? else {
            return Ok(());
        };
        if file.byte_len()? > SIDE_CAR_LIMIT.max(patch.replacement.len()) as u64 {
            bail!("staged NFO replacement exceeds its bounded recovery limit");
        }
        let (token, digest) = token_for_bound_file(&file)?;
        if !self
            .nfo_checkpoint_matches_file(patch, checkpoint, stage_path, &file, &token, &digest)?
        {
            bail!("staged NFO replacement no longer matches its journaled identity");
        }
        let entry = self.root.bind_entry(stage_path, false)?;
        self.root
            .remove_bound_file_if_identity(&entry, file.identity())
    }

    fn remove_checkpointed_displaced_nfo(
        &self,
        preview_id: &str,
        patch: &PreparedNfoPatch,
        checkpoint: &NfoReplacementCheckpoint,
        stage_path: &Path,
    ) -> Result<()> {
        let expected_stage = self.nfo_checkpoint_stage_path(patch, checkpoint)?;
        if expected_stage != stage_path {
            bail!("journaled NFO replacement stage path changed");
        }
        let Some(file) = self.root.open_bound_file(stage_path)? else {
            return Ok(());
        };
        if file.byte_len()? > SIDE_CAR_LIMIT as u64 {
            bail!("displaced NFO exceeds its bounded recovery limit");
        }
        let expected = patch
            .expected_identity
            .as_deref()
            .ok_or_else(|| anyhow!("NFO replacement checkpoint has no approved before object"))?;
        let (token, digest) = token_for_bound_file(&file)?;
        let path_identity = self.root.entry_identity(stage_path)?;
        let anchor = self.preview_anchor(preview_id, expected)?;
        if digest != patch.expected_sha256.as_deref().unwrap_or_default()
            || !identity_tokens_match_with_pinned_file(
                expected,
                &token,
                &file,
                path_identity,
                anchor.as_ref(),
            )?
        {
            bail!("displaced NFO at the journaled stage path is not the approved before object");
        }
        let entry = self.root.bind_entry(stage_path, false)?;
        self.root
            .remove_bound_file_if_identity(&entry, file.identity())
    }

    fn created_nfo_matches_prepared(
        &self,
        target: &Path,
        file: &BoundFile,
        token: &str,
        digest: &str,
        patch: &PreparedNfoPatch,
    ) -> Result<bool> {
        if digest != patch.after_sha256 {
            return Ok(false);
        }
        let identity = file.identity();
        let path_identity = self.root.entry_identity(target)?;
        let fingerprint = fingerprint(file)?;
        let token_identity = decode_identity_token(token)?;
        let permissions = (fingerprint.mode & 0o777) as u16;
        let requested_permissions = patch.expected_mode.unwrap_or(0o644);

        // The preview had no NFO object to pin. Recovery therefore binds to the current rooted
        // path, exact prepared bytes, the creating user, and a non-executable mode no broader
        // than the requested 0644 policy. Timestamps and link count are not identity or content
        // signals here and may change independently.
        Ok(identity.is_file()
            && path_identity == Some(identity)
            && token_identity.device == identity.device()
            && token_identity.inode == identity.inode()
            && token_identity.size == patch.replacement.len() as u64
            && token_identity.uid == unsafe { libc::geteuid() as u32 }
            && fingerprint.uid == token_identity.uid
            && fingerprint.gid == token_identity.gid
            && permissions & 0o600 == 0o600
            && permissions & !requested_permissions == 0
            && fingerprint.mode & 0o7000 == 0)
    }

    fn write_batch_manifest(&self, path: &Path, manifest: &BatchManifest) -> Result<()> {
        let contents =
            serde_json::to_vec(manifest).context("failed to encode move batch manifest")?;
        if contents.len() > MANIFEST_LIMIT {
            bail!("move batch manifest exceeds its private storage limit");
        }
        self.write_private_file(path, &contents)
    }

    fn prune_persisted_operation_states(&self) -> Vec<String> {
        let registry = OPERATION_STATE_CLEANUP_CURSORS
            .get_or_init(|| Mutex::new(OperationStateCleanupCursorRegistry::default()));
        self.prune_persisted_operation_states_with_registry(registry)
    }

    fn prune_persisted_operation_states_with_registry(
        &self,
        registry: &Mutex<OperationStateCleanupCursorRegistry>,
    ) -> Vec<String> {
        let key = OperationStateCleanupCursorKey {
            canonical_root: self.root.root_path().to_path_buf(),
            library_device: self.library_dir_identity.device(),
            library_inode: self.library_dir_identity.inode(),
        };
        let cursor_handle = match registry.lock() {
            Ok(mut registry) => registry.cursor_for(key),
            Err(_) => {
                return vec!["persisted operation-state cleanup registry is unavailable".into()];
            }
        };
        let mut cursor = match cursor_handle.lock() {
            Ok(cursor) => cursor,
            Err(_) => {
                return vec!["persisted operation-state cleanup cursor is unavailable".into()];
            }
        };
        let mut warnings = Vec::new();
        let mut names_read = 0usize;
        let mut files_examined = 0usize;
        let mut content_bytes = 0usize;
        let now = SystemTime::now();

        while files_examined < MAX_OPERATION_STATE_FILES_PER_PASS {
            let names_left =
                MAX_OPERATION_STATE_DIRECTORY_NAMES_PER_PASS.saturating_sub(names_read);
            let page = match self.peek_operation_state_cursor(&mut cursor, names_left) {
                Ok(page) => page,
                Err(error) => {
                    *cursor = None;
                    warnings.push(format!(
                        "persisted operation-state cleanup cursor was reset after a read or directory validation error: {error:#}"
                    ));
                    break;
                }
            };
            names_read = names_read.saturating_add(page.names_read);
            let Some(entry) = page.entry else {
                if page.exhausted {
                    *cursor = None;
                }
                if page.names_read == 0
                    || names_read >= MAX_OPERATION_STATE_DIRECTORY_NAMES_PER_PASS
                {
                    break;
                }
                continue;
            };
            files_examined += 1;

            let (name, identity) = match entry {
                BoundDirectoryCursorEntry::Present { name, identity } => (name, identity),
                BoundDirectoryCursorEntry::Unreadable { name, error } => {
                    warnings.push(format!(
                        "retained unreadable persisted operation-state directory entry {name:?}: {error}"
                    ));
                    if let Some(cursor) = cursor.as_mut() {
                        self.root.consume_bound_directory_cursor_entry(cursor);
                    }
                    continue;
                }
            };
            if !identity.is_file() {
                if let Some(cursor) = cursor.as_mut() {
                    self.root.consume_bound_directory_cursor_entry(cursor);
                }
                continue;
            }
            let Some((kind, id)) = parse_operation_state_file_name(&name) else {
                if let Some(cursor) = cursor.as_mut() {
                    self.root.consume_bound_directory_cursor_entry(cursor);
                }
                continue;
            };
            let path = self.library_dir.join(&name);
            let limit = match kind {
                OperationStateFileKind::Preview => PREVIEW_LIMIT,
                OperationStateFileKind::Batch => MANIFEST_LIMIT,
            };
            let candidate = match self.read_operation_state_candidate(
                &path,
                identity,
                limit,
                MAX_OPERATION_STATE_BYTES_PER_PASS.saturating_sub(content_bytes),
            ) {
                Ok(OperationStateCandidateRead::Missing) => {
                    if let Some(cursor) = cursor.as_mut() {
                        self.root.consume_bound_directory_cursor_entry(cursor);
                    }
                    continue;
                }
                Ok(OperationStateCandidateRead::Deferred) => break,
                Ok(OperationStateCandidateRead::Ready(candidate)) => *candidate,
                Err(error) => {
                    warnings.push(format!(
                        "retained unreadable persisted operation-state file {}: {error:#}",
                        path.file_name()
                            .unwrap_or_else(|| OsStr::new("unknown"))
                            .to_string_lossy()
                    ));
                    if let Some(cursor) = cursor.as_mut() {
                        self.root.consume_bound_directory_cursor_entry(cursor);
                    }
                    continue;
                }
            };
            content_bytes = content_bytes.saturating_add(candidate.byte_len);

            let expired = match kind {
                OperationStateFileKind::Preview => {
                    let persisted =
                        match serde_json::from_slice::<PersistedPreview>(&candidate.bytes) {
                            Ok(persisted)
                                if persisted.version == 1 && persisted.preview.id == id =>
                            {
                                persisted
                            }
                            Ok(_) => {
                                warnings.push(format!(
                                    "retained persisted preview with mismatched identity: {}",
                                    path.display()
                                ));
                                if let Some(cursor) = cursor.as_mut() {
                                    self.root.consume_bound_directory_cursor_entry(cursor);
                                }
                                continue;
                            }
                            Err(error) => {
                                warnings.push(format!(
                                    "retained unreadable persisted preview {}: {error}",
                                    path.display()
                                ));
                                if let Some(cursor) = cursor.as_mut() {
                                    self.root.consume_bound_directory_cursor_entry(cursor);
                                }
                                continue;
                            }
                        };
                    let _ = persisted;
                    if operation_state_is_expired(
                        candidate.modified_seconds,
                        now,
                        ABANDONED_PREVIEW_TTL,
                    ) {
                        match self.operation_state_path_exists(&self.batch_path(&id)) {
                            Ok(false) => true,
                            Ok(true) => false,
                            Err(error) => {
                                warnings.push(format!(
                                    "retained persisted preview because its batch state could not be checked: {error:#}"
                                ));
                                false
                            }
                        }
                    } else {
                        false
                    }
                }
                OperationStateFileKind::Batch => {
                    let manifest = match serde_json::from_slice::<BatchManifest>(&candidate.bytes) {
                        Ok(manifest) if manifest.version == 1 && manifest.preview.id == id => {
                            manifest
                        }
                        Ok(_) => {
                            warnings.push(format!(
                                "retained persisted batch with mismatched identity: {}",
                                path.display()
                            ));
                            if let Some(cursor) = cursor.as_mut() {
                                self.root.consume_bound_directory_cursor_entry(cursor);
                            }
                            continue;
                        }
                        Err(error) => {
                            warnings.push(format!(
                                "retained unreadable persisted batch {}: {error}",
                                path.display()
                            ));
                            if let Some(cursor) = cursor.as_mut() {
                                self.root.consume_bound_directory_cursor_entry(cursor);
                            }
                            continue;
                        }
                    };
                    let recoverable = !manifest.attempt_finished || manifest.retryable_access_error;
                    !recoverable
                        && operation_state_is_expired(
                            candidate.modified_seconds,
                            now,
                            TERMINAL_BATCH_RECOVERY_RETENTION,
                        )
                }
            };

            if expired {
                if candidate.byte_len
                    > MAX_OPERATION_STATE_BYTES_PER_PASS.saturating_sub(content_bytes)
                {
                    break;
                }
                content_bytes = content_bytes.saturating_add(candidate.byte_len);
                let retention = match kind {
                    OperationStateFileKind::Preview => ABANDONED_PREVIEW_TTL,
                    OperationStateFileKind::Batch => TERMINAL_BATCH_RECOVERY_RETENTION,
                };
                match self.remove_operation_state_candidate(
                    &candidate,
                    limit,
                    kind,
                    &id,
                    retention,
                ) {
                    Ok(PreviewCleanupOutcome::Removed) => {
                        if kind == OperationStateFileKind::Preview {
                            release_preview_anchors(&id);
                        }
                    }
                    Ok(PreviewCleanupOutcome::Deferred) => warnings.push(format!(
                        "retained expired preview {id} while cloud confirmation state cannot be verified"
                    )),
                    Ok(PreviewCleanupOutcome::Retained | PreviewCleanupOutcome::NotRemoved) => {}
                    Err(error) => warnings.push(format!(
                        "retained persisted operation-state file after safe cleanup could not be verified: {error:#}"
                    )),
                }
            }
            if let Some(cursor) = cursor.as_mut() {
                self.root.consume_bound_directory_cursor_entry(cursor);
            }
        }
        warnings
    }

    fn peek_operation_state_cursor(
        &self,
        cursor: &mut Option<BoundDirectoryCursor>,
        max_names: usize,
    ) -> Result<crate::safe_fs::BoundDirectoryCursorPage> {
        self.coordinate_read(&self.library_dir, |coordinated| {
            if coordinated != self.library_dir {
                bail!("coordinated operation-state directory changed during cleanup");
            }
            self.validate_library_directory()?;
            if cursor.is_none() && max_names == 0 {
                return Ok(crate::safe_fs::BoundDirectoryCursorPage {
                    entry: None,
                    names_read: 0,
                    exhausted: false,
                });
            }
            let directory = self.root.bind_entry(&self.library_dir, false)?;
            if self.root.bound_entry_identity(&directory)? != Some(self.library_dir_identity) {
                bail!("private media-library directory changed during cleanup");
            }
            if cursor.is_none() {
                *cursor = Some(self.root.start_bound_directory_cursor(
                    &directory,
                    self.library_dir_identity,
                    0o700,
                )?);
            }
            let cursor = cursor
                .as_mut()
                .ok_or_else(|| anyhow!("operation-state cleanup cursor was not initialized"))?;
            self.root
                .peek_bound_directory_cursor(cursor, max_names, 0o700)
        })
    }

    fn read_operation_state_candidate(
        &self,
        path: &Path,
        expected: EntryIdentity,
        limit: usize,
        remaining_bytes: usize,
    ) -> Result<OperationStateCandidateRead> {
        self.coordinate_read(path, |coordinated| {
            if coordinated != path {
                bail!("coordinated operation-state path changed during cleanup");
            }
            self.validate_library_directory()?;
            let entry = self.root.bind_entry(coordinated, false)?;
            match self.root.bound_entry_identity(&entry)? {
                None => return Ok(OperationStateCandidateRead::Missing),
                Some(identity) if identity != expected => {
                    bail!("persisted operation-state path was replaced during cleanup")
                }
                Some(_) => {}
            }
            let file = self.root.open_bound_file_if_identity(&entry, expected)?;
            file.validate_private_single_link(0o600)
                .context("persisted operation-state file is not owner-private")?;
            let byte_len = usize::try_from(file.byte_len()?)
                .context("persisted operation-state file length is not representable")?;
            if byte_len > limit {
                bail!("persisted operation-state file exceeds its read limit");
            }
            if byte_len > remaining_bytes {
                return Ok(OperationStateCandidateRead::Deferred);
            }
            let bytes = file.read_limited(limit)?;
            if bytes.len() != byte_len || usize::try_from(file.byte_len()?)? != byte_len {
                bail!("persisted operation-state file changed length while being read");
            }
            file.validate_private_single_link(0o600)
                .context("persisted operation-state file access policy changed while being read")?;
            let digest = sha256_bytes(&bytes);
            let modified_seconds = file.content_stamp()?.1;
            if self.root.bound_entry_identity(&entry)? != Some(expected) {
                bail!("persisted operation-state file path was replaced while being read");
            }
            self.validate_library_directory()?;
            Ok(OperationStateCandidateRead::Ready(Box::new(
                OperationStateCleanupCandidate {
                    entry,
                    identity: expected,
                    file,
                    digest,
                    bytes,
                    byte_len,
                    modified_seconds,
                },
            )))
        })
    }

    fn operation_state_path_exists(&self, path: &Path) -> Result<bool> {
        self.coordinate_read(path, |coordinated| {
            if coordinated != path {
                bail!("coordinated operation-state path changed during lookup");
            }
            self.validate_library_directory()?;
            let entry = self.root.bind_entry(coordinated, false)?;
            self.root
                .bound_entry_identity(&entry)
                .map(|identity| identity.is_some())
        })
    }

    fn remove_operation_state_candidate(
        &self,
        candidate: &OperationStateCleanupCandidate,
        limit: usize,
        kind: OperationStateFileKind,
        id: &str,
        retention: Duration,
    ) -> Result<PreviewCleanupOutcome> {
        let mut remove = || {
            self.remove_operation_state_candidate_with_hook(
                candidate,
                limit,
                kind,
                id,
                retention,
                || {},
            )
        };
        if kind == OperationStateFileKind::Preview
            && let Some(gate) = self.preview_cleanup_gate.as_ref()
        {
            return gate.remove_preview_if_unconfirmed(id, &mut remove);
        }
        Ok(if remove()? {
            PreviewCleanupOutcome::Removed
        } else {
            PreviewCleanupOutcome::NotRemoved
        })
    }

    fn remove_operation_state_candidate_with_hook(
        &self,
        candidate: &OperationStateCleanupCandidate,
        limit: usize,
        kind: OperationStateFileKind,
        id: &str,
        retention: Duration,
        before_quarantine: impl FnOnce(),
    ) -> Result<bool> {
        self.validate_library_directory()?;
        candidate
            .file
            .validate_private_single_link(0o600)
            .context("persisted operation-state file access policy changed before cleanup")?;
        let expected = candidate.identity;
        let mut no_longer_eligible = false;
        before_quarantine();
        let result = self.root.remove_bound_file_if_identity_with_validation(
            &candidate.entry,
            expected,
            || {
                self.validate_library_directory()?;
                candidate.file.validate_private_single_link(0o600)?;
                let contents = candidate.file.read_limited(limit)?;
                if contents.len() != candidate.byte_len
                    || sha256_bytes(&contents) != candidate.digest
                {
                    bail!("persisted operation-state file content changed before cleanup");
                }
                let modified_seconds = candidate.file.content_stamp()?.1;
                if !operation_state_is_expired(modified_seconds, SystemTime::now(), retention) {
                    no_longer_eligible = true;
                    bail!("persisted operation-state file was refreshed before cleanup");
                }
                if kind == OperationStateFileKind::Preview {
                    match self.operation_state_path_exists(&self.batch_path(id)) {
                        Ok(false) => {}
                        Ok(true) => {
                            no_longer_eligible = true;
                            bail!("a batch state appeared before preview cleanup");
                        }
                        Err(error) => {
                            return Err(error.context(
                                "could not recheck associated batch state before preview cleanup",
                            ));
                        }
                    }
                }
                candidate.file.validate_private_single_link(0o600)?;
                self.validate_library_directory()
            },
        );
        match result {
            Ok(()) => Ok(true),
            Err(_) if no_longer_eligible => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn coordinate_move<T>(
        &self,
        source: &Path,
        target: &Path,
        mut action: impl FnMut(&Path, &Path) -> Result<T>,
    ) -> Result<T> {
        let mut result = None;
        let mut accessor = |source_accessor: &Path, target_accessor: &Path| -> Result<()> {
            let source_rooted = coordinated_path_under_root(&self.root, source_accessor)?;
            let target_rooted = coordinated_path_under_root(&self.root, target_accessor)?;
            result = Some(action(&source_rooted, &target_rooted)?);
            Ok(())
        };
        self.provider
            .coordinate_move(source, target, &mut accessor)
            .map_err(|error| classify_deadlock_error(source, "move", error))?;
        result.ok_or_else(|| anyhow!("File Provider supplied no move accessor"))
    }

    fn scan_locked(&self, hints: &[SourceHint]) -> Result<LibrarySnapshot> {
        self.scan_locked_with_completeness(hints)
            .map(|result| result.snapshot)
    }

    fn scan_locked_with_completeness(&self, hints: &[SourceHint]) -> Result<MigrationScanResult> {
        #[cfg(test)]
        self.scan_attempts.fetch_add(1, Ordering::Relaxed);
        let operation_state_warnings = self.prune_persisted_operation_states();
        let (mut index, index_warning) = self.read_index_with_recovery()?;
        let mut walk = ScanWalk::default();
        #[cfg(test)]
        let test_hooks = {
            let mut hooks = self
                .scan_test_hooks
                .lock()
                .map_err(|_| anyhow!("scan test-hook lock is poisoned"))?;
            std::mem::take(&mut *hooks)
        };
        #[cfg(test)]
        {
            walk.max_entries = test_hooks.max_entries.unwrap_or(MAX_ENTRIES);
            walk.max_media_items = test_hooks.max_media_items.unwrap_or(MAX_MEDIA_ITEMS);
            walk.scope_unknown = test_hooks.scope_unknown;
        }
        #[cfg(test)]
        let identity_reconciliation_steps = test_hooks.identity_reconciliation_steps.as_ref();
        #[cfg(test)]
        let index_budget = test_hooks.index_budget.unwrap_or(INDEX_LIMIT);
        #[cfg(test)]
        let mut expire_after_attachment_count = test_hooks.expire_after_attachment_count;
        #[cfg(not(test))]
        let index_budget = INDEX_LIMIT;
        #[cfg(not(test))]
        let mut expire_after_attachment_count = None;
        walk.warnings.extend(operation_state_warnings);
        if let Some(warning) = index_warning {
            walk.warnings.push(warning);
        }
        self.walk_directory(Path::new(""), 0, &mut walk)?;
        #[cfg(test)]
        for directory in &test_hooks.incomplete_directories {
            if walk.complete_directories.contains(directory) {
                walk.incomplete_directories.insert(directory.clone());
                walk.warnings.push(format!(
                    "scan was interrupted while enumerating directory {directory}"
                ));
            }
        }
        walk.files
            .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        if walk.files.len() > walk.max_entries {
            for file in walk.files.iter().skip(walk.max_entries) {
                let parent = Path::new(&file.relative_path)
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    .to_string_lossy()
                    .into_owned();
                walk.incomplete_directories.insert(parent);
            }
            walk.files.truncate(walk.max_entries);
            walk.incomplete_scan = true;
            walk.warnings.push(format!(
                "scan stopped at the {}-entry limit",
                walk.max_entries
            ));
        }

        let sidecars = SidecarIndex::build(
            &walk.files,
            &walk.incomplete_directories,
            &walk.directory_name_semantics,
        )?;
        if sidecars.ambiguous_count > 0 {
            walk.warnings.push(format!(
                "{} sidecar file(s) were left unattached because their longest matching media stem is ambiguous",
                sidecars.ambiguous_count
            ));
        }

        let mut videos = Vec::new();
        let mut sidecar_title_paths = HashSet::new();
        let mut unreadable_nfo_paths = HashSet::new();
        let mut unreadable_media = HashMap::new();
        let mut fingerprints = BTreeMap::new();
        let mut incomplete_attachment_media = 0usize;
        #[cfg(test)]
        let mut expire_after_built_item = test_hooks.expire_after_built_item;
        let primary_files = walk
            .files
            .iter()
            .filter(|file| is_primary_media(&file.relative_path))
            .collect::<Vec<_>>();
        for (primary_index, file) in primary_files.iter().enumerate() {
            if walk.timed_out || walk.started_at.elapsed() >= SCAN_TIMEOUT {
                walk.timed_out = true;
                if !walk
                    .warnings
                    .iter()
                    .any(|warning| warning.contains("120-second scan budget"))
                {
                    walk.warnings.push("scan stopped at the 120-second scan budget; unvisited paths were not marked missing".to_string());
                }
                for unvisited in &primary_files[primary_index..] {
                    walk.migration_incomplete_paths
                        .insert(unvisited.relative_path.clone());
                }
                break;
            }
            if videos.len() >= walk.max_media_items {
                walk.incomplete_scan = true;
                for unvisited in &primary_files[primary_index..] {
                    walk.migration_incomplete_paths
                        .insert(unvisited.relative_path.clone());
                }
                walk.warnings.push(format!(
                    "scan stopped at the {}-media-item limit",
                    walk.max_media_items
                ));
                break;
            }
            let attachments_complete = !walk
                .incomplete_directories
                .contains(parent_relative(&file.relative_path));
            let built_result = self.build_item(
                file,
                &sidecars,
                attachments_complete,
                &mut ScanBuildState {
                    started_at: walk.started_at,
                    timed_out: &mut walk.timed_out,
                    migration_incomplete_paths: &mut walk.migration_incomplete_paths,
                    warnings: &mut walk.warnings,
                    expire_after_attachment_count: &mut expire_after_attachment_count,
                },
            );
            match built_result {
                Ok(Some(built)) => {
                    if built.item.presence == Presence::Unavailable {
                        walk.migration_incomplete_paths
                            .insert(file.relative_path.clone());
                    }
                    if !attachments_complete {
                        incomplete_attachment_media = incomplete_attachment_media.saturating_add(1);
                    }
                    if built.sidecar_title_present {
                        sidecar_title_paths.insert(built.item.relative_path.clone());
                    }
                    if built.authoritative_nfo_unreadable {
                        unreadable_nfo_paths.insert(built.item.relative_path.clone());
                    }
                    if let Some(fingerprint) = &built.fingerprint {
                        fingerprints.insert(built.item.relative_path.clone(), fingerprint.clone());
                    }
                    videos.push(built.item);
                    #[cfg(test)]
                    if expire_after_built_item {
                        expire_after_built_item = false;
                        walk.timed_out = true;
                        if !walk
                            .warnings
                            .iter()
                            .any(|warning| warning.contains("120-second scan budget"))
                        {
                            walk.warnings.push("scan stopped at the 120-second scan budget; unvisited paths were not marked missing".to_string());
                        }
                    }
                }
                Ok(None) => {
                    if walk.timed_out {
                        walk.migration_incomplete_paths
                            .insert(file.relative_path.clone());
                    }
                    continue;
                }
                Err(error) => {
                    let metadata_status = if is_file_provider_access_error(&error) {
                        MetadataStatus::ProviderUnavailable
                    } else {
                        MetadataStatus::Unreadable
                    };
                    unreadable_media.insert(file.relative_path.clone(), metadata_status);
                    walk.migration_incomplete_paths
                        .insert(file.relative_path.clone());
                    walk.warnings.push(format!(
                        "skipped unreadable media entry {}: {error:#}",
                        file.relative_path
                    ));
                }
            }
        }
        if incomplete_attachment_media > 0 {
            walk.warnings.push(format!(
                "{incomplete_attachment_media} media item(s) are unavailable because their containing directory was not fully enumerated; attachments were not asserted"
            ));
        }

        let (hint_path_resolutions, unresolved_hint_paths) =
            self.resolve_hint_paths_for_scan(hints, &walk.files, &videos, &walk);
        if unresolved_hint_paths > 0 {
            walk.warnings.push(format!(
                "{} queue hint path(s) could not be safely resolved; source-ID fallback was skipped",
                unresolved_hint_paths
            ));
        }
        let hint_matches =
            matching_hint_indices_for_items(&videos, hints, &hint_path_resolutions, &walk);
        let mut queue_metadata =
            attach_matching_hints(&mut videos, hints, &hint_matches, &sidecar_title_paths);
        let mut scanned_primary_identity_counts: HashMap<(u64, u64), usize> = HashMap::new();
        for file in walk
            .files
            .iter()
            .filter(|file| is_primary_media(&file.relative_path))
        {
            *scanned_primary_identity_counts
                .entry((file.identity.device(), file.identity.inode()))
                .or_default() += 1;
        }
        let scanned_primary_identities = walk
            .files
            .iter()
            .filter(|file| is_primary_media(&file.relative_path))
            .map(|file| (file.relative_path.clone(), file.identity))
            .collect::<HashMap<_, _>>();
        let old_records = index.records.clone();
        let mut old_by_identity: HashMap<IndexObjectKey, Vec<String>> = HashMap::new();
        let mut old_by_path_and_identity: HashMap<(String, IndexObjectKey), Vec<String>> =
            HashMap::new();
        let mut old_by_walk_identity: HashMap<(u64, u64), Vec<String>> = HashMap::new();
        let mut old_by_path_and_walk_identity: HashMap<(String, u64, u64), Vec<String>> =
            HashMap::new();
        for (id, record) in &old_records {
            if record.file_type != "regular_file" {
                continue;
            }
            old_by_walk_identity
                .entry((record.device, record.inode))
                .or_default()
                .push(id.clone());
            old_by_path_and_walk_identity
                .entry((
                    record.item.relative_path.clone(),
                    record.device,
                    record.inode,
                ))
                .or_default()
                .push(id.clone());
            let key = (
                record.device,
                record.inode,
                record.birth_seconds,
                record.birth_nanoseconds,
            );
            old_by_identity.entry(key).or_default().push(id.clone());
            old_by_path_and_identity
                .entry((record.item.relative_path.clone(), key))
                .or_default()
                .push(id.clone());
        }
        // Unknown entries in an incompletely enumerated directory could be hard links to any
        // scanned item, so they prevent a unique-rename inference for this scan.
        let can_match_unique_rename = walk.incomplete_directories.is_empty();
        let mut used_ids = HashSet::new();
        let mut deferred_identity_ids = HashSet::new();
        let mut deferred_walk_identity_keys = HashSet::new();
        let mut deferred_identity_keys = HashSet::new();
        let mut deferred_identity_candidates: HashMap<IndexObjectKey, HashSet<String>> =
            HashMap::new();
        let mut deferred_identity_items = 0usize;
        let mut unreadable_identity_items = 0usize;
        let mut unavailable_records_to_publish = HashMap::new();
        let mut suppressed_unavailable_paths = HashSet::new();
        for (item_index, item) in videos.iter_mut().enumerate() {
            let Some(fingerprint) = fingerprints.get(&item.relative_path) else {
                if let Some(identity) = scanned_primary_identities
                    .get(&item.relative_path)
                    .filter(|identity| identity.is_file())
                {
                    let matching_records = old_by_path_and_walk_identity
                        .get(&(
                            item.relative_path.clone(),
                            identity.device(),
                            identity.inode(),
                        ))
                        .into_iter()
                        .flatten()
                        .filter_map(|id| old_records.get(id))
                        .filter(|record| record.file_type == "regular_file")
                        .collect::<Vec<_>>();
                    if matching_records.len() == 1 {
                        let record = matching_records[0];
                        item.physical_identity = Some(PhysicalItemIdentity::from_parts(
                            record.device,
                            record.inode,
                            record.birth_seconds,
                            record.birth_nanoseconds,
                        ));
                    }
                }
                if item.presence == Presence::Unavailable
                    && matches!(
                        item.metadata_status,
                        MetadataStatus::ProviderUnavailable | MetadataStatus::Unreadable
                    )
                    && let Some(identity) = scanned_primary_identities.get(&item.relative_path)
                    && identity.is_file()
                {
                    let walk_identity = (identity.device(), identity.inode());
                    let exact_ids = old_by_path_and_walk_identity
                        .get(&(item.relative_path.clone(), walk_identity.0, walk_identity.1))
                        .into_iter()
                        .flatten()
                        .filter(|id| !used_ids.contains(*id))
                        .cloned()
                        .collect::<Vec<_>>();
                    if exact_ids.len() == 1 {
                        let stable_id = exact_ids[0].clone();
                        suppressed_unavailable_paths.insert(item.relative_path.clone());
                        unavailable_records_to_publish
                            .insert(stable_id.clone(), item.metadata_status);
                        deferred_identity_ids.insert(stable_id);
                    } else if !exact_ids.is_empty() {
                        // Duplicate prior rows at the same path cannot be selected safely while
                        // the current media descriptor is unavailable. Keep all history in the
                        // index and suppress the path-derived row until a later readable scan.
                        suppressed_unavailable_paths.insert(item.relative_path.clone());
                        if deferred_walk_identity_keys.insert(walk_identity)
                            && let Some(candidates) = old_by_walk_identity.get(&walk_identity)
                        {
                            for id in candidates {
                                #[cfg(test)]
                                count_identity_reconciliation_step(identity_reconciliation_steps);
                                deferred_identity_ids.insert(id.clone());
                            }
                        }
                        unreadable_identity_items = unreadable_identity_items.saturating_add(1);
                    } else if let Some(candidates) = old_by_walk_identity.get(&walk_identity) {
                        // A walk identity without the opened descriptor cannot prove a rename.
                        // Defer prior history without transferring its ID or queue metadata.
                        if !candidates.is_empty() {
                            if deferred_walk_identity_keys.insert(walk_identity) {
                                for id in candidates {
                                    #[cfg(test)]
                                    count_identity_reconciliation_step(
                                        identity_reconciliation_steps,
                                    );
                                    deferred_identity_ids.insert(id.clone());
                                }
                            }
                            unreadable_identity_items = unreadable_identity_items.saturating_add(1);
                        }
                    }
                }
                continue;
            };
            item.physical_identity = Some(physical_item_identity(fingerprint));
            let mut item_queue_metadata = queue_metadata[item_index].clone();
            let key = index_object_key(fingerprint);
            let exact = old_by_path_and_identity
                .get(&(item.relative_path.clone(), key))
                .and_then(|candidate_ids| {
                    candidate_ids.iter().find_map(|id| {
                        let record = old_records.get(id)?;
                        (!used_ids.contains(id) && index_record_matches(record, fingerprint))
                            .then_some((id, record))
                    })
                });
            let current_identity_count =
                (fingerprint.identity.device(), fingerprint.identity.inode());
            if exact.is_none() && !can_match_unique_rename {
                // Partial scans must preserve every still-unused prior ID for this identity,
                // but matching and deferred registration are performed only once per key.
                let identity_candidates =
                    deferred_identity_candidates.entry(key).or_insert_with(|| {
                        old_by_identity
                            .get(&key)
                            .into_iter()
                            .flatten()
                            .filter_map(|id| {
                                #[cfg(test)]
                                count_identity_reconciliation_step(identity_reconciliation_steps);
                                let record = old_records.get(id)?;
                                (!used_ids.contains(id)
                                    && index_record_matches(record, fingerprint))
                                .then(|| id.clone())
                            })
                            .collect()
                    });
                if !identity_candidates.is_empty() {
                    if deferred_identity_keys.insert(key) {
                        for id in identity_candidates.iter() {
                            #[cfg(test)]
                            count_identity_reconciliation_step(identity_reconciliation_steps);
                            deferred_identity_ids.insert(id.clone());
                        }
                    }
                    deferred_identity_items = deferred_identity_items.saturating_add(1);
                    item.presence = Presence::Unavailable;
                    item.id = physical_item_id(fingerprint, &item.relative_path);
                    queue_metadata[item_index] = item_queue_metadata;
                    continue;
                }
            }
            let unique_rename = if exact.is_none()
                && can_match_unique_rename
                && scanned_primary_identity_counts.get(&current_identity_count) == Some(&1)
            {
                let identity_candidates = old_by_identity
                    .get(&key)
                    .into_iter()
                    .flatten()
                    .filter_map(|id| {
                        #[cfg(test)]
                        count_identity_reconciliation_step(identity_reconciliation_steps);
                        let record = old_records.get(id)?;
                        (!used_ids.contains(id) && index_record_matches(record, fingerprint))
                            .then(|| id.clone())
                    })
                    .collect::<HashSet<_>>();
                if identity_candidates.len() == 1 {
                    identity_candidates
                        .iter()
                        .next()
                        .and_then(|id| old_records.get_key_value(id))
                } else {
                    None
                }
            } else {
                None
            };
            let prior = exact.or(unique_rename);
            let stable_id = prior
                .map(|(id, _)| id.clone())
                .unwrap_or_else(|| physical_item_id(fingerprint, &item.relative_path));
            if let Some((prior_id, _)) = prior
                && let Some(candidates) = deferred_identity_candidates.get_mut(&key)
            {
                candidates.remove(prior_id);
            }
            if let Some((_, record)) = prior {
                // Queue metadata follows a physical item only after the index's object identity
                // signals match. The current path alone never carries an old association.
                if item.organization_status.is_none()
                    && unreadable_nfo_paths.contains(&item.relative_path)
                {
                    item.organization_status = record.item.organization_status;
                }
                merge_queue_metadata(&mut item_queue_metadata, &record.queue_metadata);
                preserve_queue_metadata(
                    item,
                    &item_queue_metadata,
                    sidecar_title_paths.contains(&item.relative_path),
                );
            }
            queue_metadata[item_index] = item_queue_metadata.clone();
            used_ids.insert(stable_id.clone());
            item.id = stable_id.clone();
            index.records.insert(
                stable_id,
                IndexRecord {
                    item: item.clone(),
                    device: fingerprint.identity.device(),
                    inode: fingerprint.identity.inode(),
                    file_type: "regular_file".to_string(),
                    birth_seconds: fingerprint.birth_seconds,
                    birth_nanoseconds: fingerprint.birth_nanoseconds,
                    missing_since: None,
                    queue_metadata: item_queue_metadata,
                },
            );
        }
        if deferred_identity_items > 0 {
            walk.warnings.push(format!(
                "{deferred_identity_items} media item(s) with an existing object identity are unavailable until a complete scan can confirm a unique move; previous identity and queue metadata were preserved"
            ));
        }
        if unreadable_identity_items > 0 {
            walk.warnings.push(format!(
                "{unreadable_identity_items} unreadable media item(s) have prior identity history retained as unavailable until a readable descriptor can confirm it"
            ));
        }
        if !suppressed_unavailable_paths.is_empty() {
            videos.retain(|item| !suppressed_unavailable_paths.contains(&item.relative_path));
        }

        let observed = videos
            .iter()
            .map(|item| item.id.clone())
            .collect::<HashSet<_>>();
        for (id, record) in &mut index.records {
            if observed.contains(id) {
                continue;
            }
            record.item.physical_identity = (record.file_type == "regular_file").then(|| {
                PhysicalItemIdentity::from_parts(
                    record.device,
                    record.inode,
                    record.birth_seconds,
                    record.birth_nanoseconds,
                )
            });
            if let Some(metadata_status) = unavailable_records_to_publish.get(id) {
                record.item.presence = Presence::Unavailable;
                record.item.metadata_status = *metadata_status;
                record.missing_since = None;
                videos.push(record.item.clone());
                continue;
            }
            if deferred_identity_ids.contains(id) {
                record.item.presence = Presence::Unavailable;
                record.missing_since = None;
                continue;
            }
            let unreadable_status = unreadable_media.get(&record.item.relative_path).copied();
            if let Some(status) =
                unobserved_record_status(&record.item.relative_path, &walk, unreadable_status)
            {
                record.item.presence = status.presence;
                if status.presence == Presence::Missing {
                    record.missing_since.get_or_insert_with(unix_now);
                }
                if let Some(metadata_status) = status.metadata_status {
                    record.item.metadata_status = metadata_status;
                }
                if unreadable_status.is_none()
                    && directory_under_incomplete(
                        parent_relative(&record.item.relative_path),
                        &walk.incomplete_directories,
                    )
                {
                    walk.warnings.push(format!(
                        "media status is unavailable because its directory could not be read: {}",
                        record.item.relative_path
                    ));
                }
                videos.push(record.item.clone());
            }
        }

        index.version = INDEX_VERSION;
        let mut persisted_index = compact_index_for_persistence(&index);
        let pruned_missing = prune_missing_index_records(
            &mut persisted_index,
            MAX_MISSING_INDEX_RECORDS,
            index_budget,
        )?;
        if !pruned_missing.is_empty() {
            let pruned_count = pruned_missing.len();
            let pruned_ids = pruned_missing.into_iter().collect::<HashSet<_>>();
            videos.retain(|item| !pruned_ids.contains(&item.id));
            walk.warnings.push(format!(
                "pruned {} oldest missing media records from the library index",
                pruned_count
            ));
        }

        videos.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        for item in &videos {
            if item.presence == Presence::Unavailable {
                walk.migration_incomplete_paths
                    .insert(item.relative_path.clone());
            }
        }
        let mut incomplete_directories = walk.incomplete_directories.clone();
        incomplete_directories.extend(walk.migration_incomplete_paths.iter().cloned());
        let scope_unknown = walk.scope_unknown
            || ((walk.timed_out || walk.incomplete_scan) && incomplete_directories.is_empty());
        let complete = !walk.timed_out
            && !walk.incomplete_scan
            && incomplete_directories.is_empty()
            && !scope_unknown;
        let categories = walk.root_categories.clone();
        let revision = snapshot_revision(&videos)?;
        if !self.write_index(&persisted_index, index_budget)? {
            walk.warnings.push(
                "media-library index exceeded its configured size limit; any previous valid index was kept, this snapshot is usable, but identity and queue association changes from this scan are not durable"
                    .to_string(),
            );
        }
        let snapshot = LibrarySnapshot {
            revision,
            scanned_at: unix_now(),
            root_label: self
                .root
                .logical_root_path()
                .file_name()
                .and_then(OsStr::to_str)
                .unwrap_or("Media")
                .to_string(),
            items: videos,
            categories: categories.into_iter().collect(),
            warnings: deduplicate(walk.warnings),
        };
        *self
            .last_snapshot
            .lock()
            .map_err(|_| anyhow!("media-library snapshot lock is poisoned"))? =
            Some(snapshot.clone());
        Ok(MigrationScanResult {
            snapshot,
            complete,
            incomplete_directories,
            scope_unknown,
        })
    }

    fn resolve_hint_paths_for_scan(
        &self,
        hints: &[SourceHint],
        scanned_files: &[ScannedFile],
        items: &[LibraryItem],
        walk: &ScanWalk,
    ) -> (HashMap<String, HintPathResolution>, usize) {
        let mut item_by_path = HashMap::new();
        for (item_index, item) in items.iter().enumerate() {
            item_by_path.insert(item.relative_path.as_str(), item_index);
        }

        let mut media_by_path = HashMap::new();
        let mut media_by_identity: HashMap<(u64, u64), Vec<&ScannedFile>> = HashMap::new();
        for file in scanned_files
            .iter()
            .filter(|file| is_primary_media(&file.relative_path))
        {
            media_by_path.insert(file.relative_path.as_str(), file.identity);
            media_by_identity
                .entry((file.identity.device(), file.identity.inode()))
                .or_default()
                .push(file);
        }

        let mut resolutions = HashMap::new();
        let mut unresolved_paths = 0usize;
        for path in hints
            .iter()
            .filter_map(|hint| hint.relative_path.as_deref())
        {
            if resolutions.contains_key(path) {
                continue;
            }
            let resolution = match self.resolve_hint_path_for_scan(
                path,
                &media_by_path,
                &media_by_identity,
                &item_by_path,
                walk,
            ) {
                Ok(resolution) => resolution,
                Err(_) => {
                    unresolved_paths = unresolved_paths.saturating_add(1);
                    HintPathResolution::Unavailable
                }
            };
            resolutions.insert(path.to_string(), resolution);
        }
        (resolutions, unresolved_paths)
    }

    fn resolve_hint_path_for_scan(
        &self,
        path: &str,
        media_by_path: &HashMap<&str, EntryIdentity>,
        media_by_identity: &HashMap<(u64, u64), Vec<&ScannedFile>>,
        item_by_path: &HashMap<&str, usize>,
        walk: &ScanWalk,
    ) -> Result<HintPathResolution> {
        let lookup = self.lookup_hint_path(path)?;
        if let Some(expected_identity) = media_by_path.get(path) {
            return Ok(match lookup {
                HintPathLookup::Entry(current_identity)
                    if current_identity == *expected_identity =>
                {
                    item_by_path
                        .get(path)
                        .copied()
                        .map(HintPathResolution::Item)
                        .unwrap_or(HintPathResolution::PresentUnmatched)
                }
                HintPathLookup::Entry(_) | HintPathLookup::Missing | HintPathLookup::Blocked => {
                    // A path that participated in this scan remains exclusive even if it changed
                    // or disappeared before hint resolution. It must never fall through by ID.
                    HintPathResolution::PresentUnmatched
                }
            });
        }

        let HintPathLookup::Entry(identity) = lookup else {
            return Ok(match lookup {
                HintPathLookup::Missing if scan_confirms_hint_path_absent(path, walk) => {
                    HintPathResolution::ConfirmedAbsent
                }
                HintPathLookup::Missing | HintPathLookup::Blocked => {
                    HintPathResolution::PresentUnmatched
                }
                HintPathLookup::Entry(_) => unreachable!("entry lookup matched above"),
            });
        };
        if !identity.is_file() {
            return Ok(HintPathResolution::PresentUnmatched);
        }

        let candidates = media_by_identity
            .get(&(identity.device(), identity.inode()))
            .into_iter()
            .flatten()
            .filter_map(|file| {
                if file.identity != identity
                    || !filesystem_equivalent_path_alias(
                        path,
                        &file.relative_path,
                        &walk.directory_name_semantics,
                    )
                {
                    return None;
                }
                item_by_path.get(file.relative_path.as_str()).copied()
            })
            .collect::<Vec<_>>();
        Ok(match candidates.as_slice() {
            [item_index] => HintPathResolution::Item(*item_index),
            _ => HintPathResolution::PresentUnmatched,
        })
    }

    fn lookup_hint_path(&self, path: &str) -> Result<HintPathLookup> {
        let relative = safe_relative_path(path, false)?;
        let mut parent = PathBuf::new();
        let mut parent_identity = self.root.root_identity();
        let mut components = relative.components().peekable();
        while let Some(component) = components.next() {
            let Component::Normal(name) = component else {
                bail!("hint path contains an invalid component");
            };
            let name_text = name
                .to_str()
                .ok_or_else(|| anyhow!("hint path is not UTF-8"))?;
            let directory = open_validated_target_directory(&self.root, &parent, parent_identity)?;
            let semantics = target_name_semantics(&directory)
                .context("filesystem name semantics unavailable while resolving queue hint")?;
            if is_private_downloader_component(name_text, semantics)? {
                return Ok(HintPathLookup::Blocked);
            }

            let candidate = parent.join(name);
            let absolute = self.root.logical_root_path().join(&candidate);
            let Some(identity) = self.root.entry_identity(&absolute)? else {
                return Ok(HintPathLookup::Missing);
            };
            if components.peek().is_some() {
                if !identity.is_dir() {
                    return Ok(HintPathLookup::Blocked);
                }
                parent = candidate;
                parent_identity = identity;
            } else {
                return Ok(HintPathLookup::Entry(identity));
            }
        }
        Ok(HintPathLookup::Blocked)
    }

    fn walk_directory(&self, relative: &Path, depth: usize, walk: &mut ScanWalk) -> Result<()> {
        let relative_string = path_to_relative_string(relative)?;
        if walk.started_at.elapsed() >= SCAN_TIMEOUT {
            walk.timed_out = true;
            walk.incomplete_directories.insert(relative_string);
            if !walk
                .warnings
                .iter()
                .any(|warning| warning.contains("120-second scan budget"))
            {
                walk.warnings.push("scan stopped at the 120-second scan budget; unvisited paths were not marked missing".to_string());
            }
            return Ok(());
        }
        if depth > MAX_DEPTH {
            walk.incomplete_directories.insert(relative_string.clone());
            walk.warnings
                .push(format!("scan depth limit reached at {relative_string}"));
            return Ok(());
        }
        let absolute = if relative.as_os_str().is_empty() {
            self.root.logical_root_path().to_path_buf()
        } else {
            self.root.logical_root_path().join(relative)
        };
        let result = self.coordinate_read(&absolute, |coordinated| {
            let expected = self.path_in_root(coordinated)?;
            if expected != absolute {
                bail!("coordinated directory path changed during scan");
            }
            let (entries, identity) = if relative.as_os_str().is_empty() {
                (self.root.list_root_directory()?, self.root.root_identity())
            } else {
                let entry = self.root.bind_entry(&absolute, false)?;
                let Some(identity) = self.root.bound_entry_identity(&entry)? else {
                    bail!("directory disappeared during scan");
                };
                if !identity.is_dir() {
                    bail!("scan path is no longer a directory");
                }
                (self.root.list_bound_directory(&entry, identity)?, identity)
            };
            let directory = open_validated_target_directory(&self.root, relative, identity)?;
            let semantics = target_name_semantics(&directory)
                .context("filesystem name semantics unavailable")?;
            Ok((entries, semantics))
        });

        let (entries, semantics) = match result {
            Ok(result) => result,
            Err(error) => {
                walk.incomplete_directories.insert(relative_string.clone());
                let error_text = format!("{error:#}");
                let class = if is_file_provider_access_error(&error) {
                    "File Provider access unavailable"
                } else if error_text.contains("filesystem name semantics unavailable") {
                    "filesystem name semantics unavailable"
                } else {
                    "directory unreadable"
                };
                walk.warnings
                    .push(format!("{class} at {relative_string}: {error:#}"));
                return Ok(());
            }
        };
        walk.directory_name_semantics
            .insert(relative_string.clone(), semantics);
        walk.complete_directories.insert(relative_string.clone());
        for (name, identity) in entries {
            if walk.started_at.elapsed() >= SCAN_TIMEOUT {
                walk.timed_out = true;
                walk.incomplete_directories.insert(relative_string.clone());
                if !walk
                    .warnings
                    .iter()
                    .any(|warning| warning.contains("120-second scan budget"))
                {
                    walk.warnings.push("scan stopped at the 120-second scan budget; unvisited paths were not marked missing".to_string());
                }
                return Ok(());
            }
            if excluded_entry(&name, identity, semantics, self.library_dir_identity)? {
                continue;
            }
            let Some(name_text) = name.to_str() else {
                walk.warnings.push(format!(
                    "ignored a non-UTF-8 media path under {relative_string}"
                ));
                continue;
            };
            let child_relative = relative.join(name_text);
            if identity.is_dir() {
                if relative.as_os_str().is_empty() {
                    walk.root_categories.insert(name_text.to_string());
                }
                self.walk_directory(&child_relative, depth + 1, walk)?;
            } else if identity.is_file() {
                if walk.files.len() >= walk.max_entries {
                    walk.incomplete_scan = true;
                    walk.incomplete_directories.insert(relative_string.clone());
                    walk.warnings.push(format!(
                        "scan stopped at the {}-entry limit",
                        walk.max_entries
                    ));
                    return Ok(());
                }
                walk.files.push(ScannedFile {
                    relative_path: path_to_relative_string(&child_relative)?,
                    identity,
                });
            }
            // Symlinks and special files are intentionally not followed or opened.
        }
        Ok(())
    }

    fn build_item(
        &self,
        media: &ScannedFile,
        sidecars: &SidecarIndex,
        attachments_complete: bool,
        scan: &mut ScanBuildState<'_>,
    ) -> Result<Option<BuiltLibraryItem>> {
        let absolute = self.absolute_path(&media.relative_path)?;
        let opened = self.read_media_file(&absolute);
        let opened_file = match opened {
            Ok(file) => file,
            Err(error) => {
                let status = if is_file_provider_access_error(&error) {
                    MetadataStatus::ProviderUnavailable
                } else {
                    MetadataStatus::Unreadable
                };
                scan.migration_incomplete_paths
                    .insert(media.relative_path.clone());
                scan.warnings.push(format!(
                    "media unavailable at {}: {error:#}",
                    media.relative_path
                ));
                let id = physical_item_id_from_identity(media.identity, &media.relative_path);
                return Ok(Some(BuiltLibraryItem {
                    item: LibraryItem {
                        id,
                        title: title_from_path(&media.relative_path),
                        relative_path: media.relative_path.clone(),
                        bytes: 0,
                        width: None,
                        height: None,
                        codec: None,
                        source_ids: Vec::new(),
                        collection: None,
                        organization_status: None,
                        attachments: Vec::new(),
                        authoritative_nfo_path: None,
                        confidence: Confidence::Low,
                        metadata_status: status,
                        integrity_status: IntegrityStatus::Unverified,
                        physical_identity: None,
                        presence: Presence::Unavailable,
                    },
                    sidecar_title_present: false,
                    authoritative_nfo_unreadable: false,
                    fingerprint: None,
                }));
            }
        };
        let (fingerprint, opened_presence) = match opened_file.as_ref() {
            Some(file) => (Some(fingerprint(file)?), Presence::Present),
            None => (None, Presence::Missing),
        };
        let presence = if attachments_complete {
            opened_presence
        } else {
            Presence::Unavailable
        };
        let item_identity = fingerprint
            .as_ref()
            .map(|value| value.identity)
            .unwrap_or(media.identity);
        let id = fingerprint
            .as_ref()
            .map(|value| physical_item_id(value, &media.relative_path))
            .unwrap_or_else(|| physical_item_id_from_identity(item_identity, &media.relative_path));
        let title_from_name = title_from_path(&media.relative_path);
        let mut title = title_from_name;
        let mut source_ids = ids_from_path(&media.relative_path);
        let mut collection = None;
        let mut organization_status = None;
        let mut nfo_collection_metadata_present = false;
        let authoritative_nfo_path = sidecars
            .authoritative_nfo(&media.relative_path)
            .map(str::to_owned);
        let mut nfo_title_present = false;
        let mut nfo_source_id_present = false;
        let mut sidecar_title_present = false;
        let mut metadata_status = if opened_presence == Presence::Missing {
            MetadataStatus::ProviderUnavailable
        } else {
            MetadataStatus::Unknown
        };
        let mut sidecar_unreadable = false;
        let mut authoritative_nfo_unreadable = false;
        let attachments = sidecars.attached_files(media);
        for attachment in &attachments {
            if scan.started_at.elapsed() >= SCAN_TIMEOUT {
                *scan.timed_out = true;
                push_scan_timeout_warning(scan.warnings);
                return Ok(None);
            }
            let lower = attachment.to_ascii_lowercase();
            if lower.ends_with(".nfo") {
                if authoritative_nfo_path.as_deref() == Some(attachment.as_str()) {
                    let path = self.absolute_path(attachment)?;
                    match self.read_file(&path, SIDE_CAR_LIMIT) {
                        Ok(Some(contents)) => match String::from_utf8(contents) {
                            Ok(text) => {
                                let nfo_title = xml_tag_text(&text, "title")
                                    .filter(|value| !value.trim().is_empty());
                                let nfo_ids = nfo_source_ids(&text);
                                organization_status = nfo_organization_status(&text);
                                nfo_collection_metadata_present |=
                                    nfo_has_collection_metadata(&text);
                                if let Some(value) = &nfo_title {
                                    title = value.trim().to_string();
                                }
                                nfo_title_present |= nfo_title.is_some();
                                sidecar_title_present |= nfo_title.is_some();
                                nfo_source_id_present |= !nfo_ids.is_empty();
                                source_ids.extend(nfo_ids);
                                if let Some(value) = nfo_collection_title(&text)
                                    && !value.trim().is_empty()
                                {
                                    collection = Some(CollectionMetadata {
                                        id: xml_tag_text(&text, "setid")
                                            .filter(|value| !value.trim().is_empty()),
                                        title: value.trim().to_string(),
                                        kind: xml_tag_text(&text, "settype")
                                            .filter(|value| !value.trim().is_empty())
                                            .unwrap_or_else(|| "nfo_set".to_string()),
                                        order: xml_tag_text(&text, "episode")
                                            .and_then(|value| value.parse().ok()),
                                        part_id: xml_tag_text(&text, "partid"),
                                    });
                                }
                                metadata_status = MetadataStatus::Partial;
                            }
                            Err(_) => {
                                sidecar_unreadable = true;
                                authoritative_nfo_unreadable = true;
                                scan.warnings
                                    .push(format!("NFO is not UTF-8: {attachment}"));
                            }
                        },
                        Ok(None) => {
                            sidecar_unreadable = true;
                            authoritative_nfo_unreadable = true;
                            scan.warnings.push(format!(
                                "authoritative NFO disappeared during scan: {attachment}"
                            ));
                        }
                        Err(error) => {
                            sidecar_unreadable = true;
                            authoritative_nfo_unreadable = true;
                            if is_file_provider_access_error(&error) {
                                scan.migration_incomplete_paths
                                    .insert(attachment.to_string());
                            }
                            scan.warnings
                                .push(format!("NFO unavailable at {attachment}: {error:#}"));
                        }
                    }
                }
            } else if lower.ends_with(".json") {
                let path = self.absolute_path(attachment)?;
                match self.read_file(&path, SIDE_CAR_LIMIT) {
                    Ok(Some(contents)) => {
                        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&contents) {
                            let (json_title, json_ids, json_collection) =
                                metadata_from_json(&value);
                            if let Some(json_title) = json_title {
                                if !sidecar_title_present {
                                    title = json_title;
                                }
                                sidecar_title_present = true;
                            }
                            source_ids.extend(json_ids);
                            collection = collection.or(json_collection);
                            metadata_status = MetadataStatus::Partial;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        sidecar_unreadable = true;
                        if is_file_provider_access_error(&error) {
                            scan.migration_incomplete_paths
                                .insert(attachment.to_string());
                        }
                        scan.warnings.push(format!(
                            "metadata sidecar unavailable at {attachment}: {error:#}"
                        ));
                    }
                }
            }

            let forced_timeout =
                if (*scan.expire_after_attachment_count).is_some_and(|remaining| remaining <= 1) {
                    *scan.expire_after_attachment_count = None;
                    true
                } else if let Some(remaining) = scan.expire_after_attachment_count.as_mut() {
                    *remaining -= 1;
                    false
                } else {
                    false
                };
            if forced_timeout || scan.started_at.elapsed() >= SCAN_TIMEOUT {
                *scan.timed_out = true;
                push_scan_timeout_warning(scan.warnings);
                return Ok(None);
            }
        }
        if scan.started_at.elapsed() >= SCAN_TIMEOUT {
            *scan.timed_out = true;
            push_scan_timeout_warning(scan.warnings);
            return Ok(None);
        }
        source_ids = normalize_source_ids(source_ids);
        if organization_status == Some(OrganizationStatus::Standalone)
            && (collection.is_some() || nfo_collection_metadata_present)
        {
            organization_status = None;
            scan.warnings.push(format!(
                "standalone marker ignored because the NFO also declares a collection: {media_relative_path}",
                media_relative_path = media.relative_path
            ));
        }
        let confidence = if !source_ids.is_empty() && metadata_status == MetadataStatus::Partial {
            Confidence::High
        } else if !source_ids.is_empty() {
            Confidence::Medium
        } else {
            Confidence::Low
        };
        if sidecar_unreadable {
            metadata_status = MetadataStatus::Unreadable;
        } else if nfo_title_present && nfo_source_id_present {
            metadata_status = MetadataStatus::Complete;
        } else if metadata_status == MetadataStatus::Unknown && !source_ids.is_empty() {
            metadata_status = MetadataStatus::Partial;
        }

        let media_specs = if opened_presence == Presence::Present {
            opened_file
                .as_ref()
                .ok_or_else(|| anyhow!("media descriptor disappeared during scan"))
                .and_then(probe_media)
                .unwrap_or_else(|error| {
                    scan.warnings.push(format!(
                        "ffprobe unavailable for {}: {error:#}",
                        media.relative_path
                    ));
                    MediaSpecs::default()
                })
        } else {
            MediaSpecs::default()
        };
        if scan.started_at.elapsed() >= SCAN_TIMEOUT {
            *scan.timed_out = true;
            push_scan_timeout_warning(scan.warnings);
            return Ok(None);
        }
        let bytes = fingerprint.as_ref().map(|value| value.size).unwrap_or(0);
        let library_attachments = attachments
            .iter()
            .map(|path| LibraryAttachment {
                relative_path: path.clone(),
                kind: attachment_kind(path).to_string(),
            })
            .collect();
        Ok(Some(BuiltLibraryItem {
            item: LibraryItem {
                id,
                title,
                relative_path: media.relative_path.clone(),
                bytes,
                width: media_specs.width,
                height: media_specs.height,
                codec: media_specs.codec,
                source_ids,
                collection,
                organization_status,
                attachments: library_attachments,
                authoritative_nfo_path,
                confidence,
                metadata_status,
                integrity_status: IntegrityStatus::Unverified,
                physical_identity: fingerprint.as_ref().map(physical_item_identity),
                presence,
            },
            sidecar_title_present,
            authoritative_nfo_unreadable,
            fingerprint,
        }))
    }

    fn absolute_path(&self, relative: &str) -> Result<PathBuf> {
        let relative = self.safe_relative_path(relative, false)?;
        Ok(self.root.logical_root_path().join(relative))
    }

    fn safe_relative_path(&self, value: &str, allow_empty: bool) -> Result<PathBuf> {
        let relative = safe_relative_path(value, allow_empty)?;
        validate_not_private_downloader_path_with_identity(
            &self.root,
            &relative,
            Some(self.library_dir_identity),
        )?;
        Ok(relative)
    }

    fn path_in_root(&self, coordinated: &Path) -> Result<PathBuf> {
        coordinated_path_under_root(&self.root, coordinated)
    }

    fn coordinate_read<T>(
        &self,
        path: &Path,
        mut action: impl FnMut(&Path) -> Result<T>,
    ) -> Result<T> {
        let mut result = None;
        let mut accessor = |coordinated_path: &Path| -> Result<()> {
            let rooted_path = coordinated_path_under_root(&self.root, coordinated_path)?;
            result = Some(action(&rooted_path)?);
            Ok(())
        };
        self.provider
            .coordinate_read(path, &mut accessor)
            .map_err(|error| classify_deadlock_error(path, "read", error))?;
        result.ok_or_else(|| {
            anyhow!(
                "File Provider supplied no read accessor for {}",
                path.display()
            )
        })
    }

    fn read_media_file(&self, path: &Path) -> Result<Option<BoundFile>> {
        self.coordinate_read(path, |coordinated| {
            if coordinated != path {
                bail!("coordinated media path changed during read");
            }
            self.root.open_bound_file(coordinated)
        })
    }

    fn read_file(&self, path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
        self.coordinate_read(path, |coordinated| {
            if coordinated != path {
                bail!("coordinated sidecar path changed during read");
            }
            let Some(file) = self.root.open_bound_file(coordinated)? else {
                return Ok(None);
            };
            file.read_limited(limit).map(Some)
        })
    }

    fn file_fingerprint(&self, path: &Path) -> Result<Option<FileFingerprint>> {
        self.coordinate_read(path, |coordinated| {
            if coordinated != path {
                bail!("coordinated file path changed while reading its identity");
            }
            let Some(file) = self.root.open_bound_file(coordinated)? else {
                return Ok(None);
            };
            Ok(Some(fingerprint(&file)?))
        })
    }

    #[cfg(test)]
    fn read_file_with_token(
        &self,
        path: &Path,
        content_limit: Option<usize>,
    ) -> Result<Option<(String, String, Vec<u8>)>> {
        self.read_file_with_token_and_handle(path, content_limit)
            .map(|result| result.map(|read| (read.token, read.digest, read.contents)))
    }

    fn read_file_with_token_and_handle(
        &self,
        path: &Path,
        content_limit: Option<usize>,
    ) -> Result<Option<BoundFileRead>> {
        self.coordinate_read(path, |coordinated| {
            if coordinated != path {
                bail!("coordinated file path changed during identity verification");
            }
            let Some(file) = self.root.open_bound_file(coordinated)? else {
                return Ok(None);
            };
            let (token, digest) = token_for_bound_file(&file)?;
            #[cfg(test)]
            let token = if self
                .preview_creation_time_unavailable
                .load(Ordering::Relaxed)
            {
                identity_token_without_creation_time_for_test(&token)?
            } else {
                token
            };
            let contents = match content_limit {
                Some(limit) => {
                    if file.byte_len()? > limit as u64 {
                        bail!("sidecar exceeds its {limit}-byte read limit");
                    }
                    let contents = file.read_limited(limit)?;
                    if sha256_bytes(&contents) != digest {
                        bail!("selected sidecar content changed while it was read");
                    }
                    contents
                }
                None => Vec::new(),
            };
            if self.root.entry_identity(coordinated)? != Some(file.identity()) {
                bail!("selected file path was replaced during identity verification");
            }
            Ok(Some(BoundFileRead {
                token,
                digest,
                contents,
                file,
            }))
        })
    }

    #[cfg(test)]
    fn read_index(&self) -> Result<LibraryIndex> {
        self.read_index_with_recovery().map(|(index, _)| index)
    }

    fn read_index_with_recovery(&self) -> Result<(LibraryIndex, Option<String>)> {
        let path = self.library_dir.join(LIBRARY_INDEX);
        let Some(read) = self.read_index_file(&path)? else {
            return Ok((empty_library_index(), None));
        };
        match serde_json::from_slice::<LibraryIndex>(&read.contents) {
            Ok(index) if index.version == INDEX_VERSION => Ok((index, None)),
            Ok(index) => {
                let reason = format!("unsupported schema version {}", index.version);
                let quarantine_name = self.quarantine_index_file(&path, &read)?;
                self.prune_index_quarantines()?;
                Ok((
                    empty_library_index(),
                    Some(index_recovery_warning(&reason, &quarantine_name)),
                ))
            }
            Err(_) => {
                let reason = "malformed or truncated JSON";
                let quarantine_name = self.quarantine_index_file(&path, &read)?;
                self.prune_index_quarantines()?;
                Ok((
                    empty_library_index(),
                    Some(index_recovery_warning(reason, &quarantine_name)),
                ))
            }
        }
    }

    fn read_index_file(&self, path: &Path) -> Result<Option<IndexFileRead>> {
        self.coordinate_read(path, |coordinated| {
            if coordinated != path {
                bail!("coordinated media-library index path changed during read");
            }
            self.validate_library_directory()?;
            let entry = self.root.bind_entry(coordinated, false)?;
            let Some(identity) = self.root.bound_entry_identity(&entry)? else {
                return Ok(None);
            };
            let file = self.root.open_bound_file_if_identity(&entry, identity)?;
            file.validate_private_single_link(0o600)
                .context("media-library index must be an owner-private regular file")?;
            let initial_len = file.byte_len()?;
            if initial_len > INDEX_LIMIT as u64 {
                bail!(
                    "media-library index exceeds the {INDEX_LIMIT}-byte recovery limit; it was not quarantined because its contents could not be safely verified"
                );
            }
            let contents = file.read_limited(INDEX_LIMIT)?;
            let digest = sha256_bytes(&contents);
            let repeated_contents = file.read_limited(INDEX_LIMIT)?;
            if repeated_contents != contents || file.byte_len()? != initial_len {
                bail!("media-library index content changed while it was being read");
            }
            file.validate_private_single_link(0o600)
                .context("media-library index access policy changed while it was being read")?;
            self.validate_library_directory()?;
            if self.root.bound_entry_identity(&entry)? != Some(identity) {
                bail!("media-library index path was replaced while it was being read");
            }
            Ok(Some(IndexFileRead {
                digest,
                contents,
                file,
            }))
        })
    }

    fn quarantine_index_file(&self, path: &Path, read: &IndexFileRead) -> Result<String> {
        let identity = read.file.identity();
        validate_index_contents(&read.file, &read.digest)?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_nanos();
        let serial = PRIVATE_TEMP_SERIAL.fetch_add(1, Ordering::Relaxed);
        let quarantine_name = format!("{INDEX_QUARANTINE_PREFIX}{timestamp}.{serial}.json");
        let quarantine_path = path.with_file_name(&quarantine_name);

        self.coordinate_move(
            path,
            &quarantine_path,
            |coordinated_source, coordinated_target| {
                if coordinated_source != path || coordinated_target != quarantine_path.as_path() {
                    bail!("coordinated media-library index quarantine path changed");
                }
                self.validate_library_directory()?;
                let source = self.root.bind_entry(coordinated_source, false)?;
                let destination = self.root.bind_entry(coordinated_target, false)?;
                let current = self.root.open_bound_file_if_identity(&source, identity)?;
                validate_index_contents(&current, &read.digest)?;
                self.root.rename_via_bound_parents_noreplace_if_identity(
                    &source,
                    &destination,
                    identity,
                )?;
                let quarantined = self
                    .root
                    .open_bound_file_if_identity(&destination, identity)?;
                validate_index_contents(&quarantined, &read.digest)?;
                if self.root.bound_entry_identity(&source)?.is_some() {
                    bail!("media-library index remained at its original path after quarantine");
                }
                self.validate_library_directory()?;
                Ok(())
            },
        )?;
        Ok(quarantine_name)
    }

    fn prune_index_quarantines(&self) -> Result<()> {
        self.validate_library_directory()?;
        let directory = self.root.bind_entry(&self.library_dir, false)?;
        let identity = self
            .root
            .bound_entry_identity(&directory)?
            .ok_or_else(|| anyhow!("private media-library directory disappeared"))?;
        self.root
            .validate_private_bound_directory(&directory, identity, 0o700)?;
        let entries = self.root.list_bound_directory(&directory, identity)?;
        let mut retained = Vec::with_capacity(MAX_INDEX_QUARANTINES + 1);
        for (name, entry_identity) in entries {
            if !entry_identity.is_file() {
                continue;
            }
            let Some((timestamp, serial)) = parse_index_quarantine_name(&name) else {
                continue;
            };
            retained.push(IndexQuarantineCandidate {
                path: self.library_dir.join(&name),
                identity: entry_identity,
                timestamp,
                serial,
            });
            if retained.len() > MAX_INDEX_QUARANTINES {
                let oldest = retained
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, candidate)| (candidate.timestamp, candidate.serial))
                    .map(|(index, _)| index)
                    .expect("retained index quarantine list is nonempty");
                let candidate = retained.remove(oldest);
                self.remove_index_quarantine(candidate)?;
            }
        }
        Ok(())
    }

    fn remove_index_quarantine(&self, candidate: IndexQuarantineCandidate) -> Result<()> {
        self.validate_library_directory()?;
        let entry = self.root.bind_entry(&candidate.path, false)?;
        if self.root.bound_entry_identity(&entry)? != Some(candidate.identity) {
            bail!("media-library index quarantine identity changed before pruning");
        }
        let file = self
            .root
            .open_bound_file_if_identity(&entry, candidate.identity)?;
        file.validate_private_single_link(0o600)
            .context("media-library index quarantine is not an owner-private regular file")?;
        let digest = bounded_index_file_digest(&file)?;
        let expected_identity = candidate.identity;
        self.root.remove_bound_file_if_identity_with_validation(
            &entry,
            expected_identity,
            || {
                self.validate_library_directory()?;
                validate_index_contents(&file, &digest)
            },
        )?;
        Ok(())
    }

    fn write_index(&self, index: &LibraryIndex, max_serialized_bytes: usize) -> Result<bool> {
        let limit = max_serialized_bytes.min(INDEX_LIMIT);
        let Some(writer) = serialize_index(index, IndexSerializationWriter::new(limit, true))?
        else {
            return Ok(false);
        };
        let contents = writer
            .capture
            .expect("index serialization should capture bytes for persistence");
        self.write_private_file(&self.library_dir.join(LIBRARY_INDEX), &contents)?;
        Ok(true)
    }

    fn read_private_file(&self, path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
        self.coordinate_read(path, |coordinated| {
            self.validate_library_directory()?;
            let Some(file) = self.root.open_bound_file(coordinated)? else {
                return Ok(None);
            };
            file.validate_private_single_link(0o600)
                .context("media-library state file must be owner-private")?;
            if file.byte_len()? > limit as u64 {
                bail!("media-library state file exceeds its size limit");
            }
            file.read_limited(limit).map(Some)
        })
    }

    fn write_private_file(&self, path: &Path, contents: &[u8]) -> Result<()> {
        self.coordinate_write(path, |coordinated| {
            self.validate_library_directory()?;
            if let Some(file) = self.root.open_bound_file(coordinated)? {
                let identity = file.identity();
                file.validate_private_single_link(0o600)?;
                let entry = self.root.bind_entry(coordinated, false)?;
                if self.root.bound_entry_identity(&entry)? != Some(identity) {
                    bail!("media-library state file changed before update");
                }
                let temporary = private_temp_sibling(coordinated);
                let (_, new_identity) = self.root.replace_bound_file_atomically_if_identity(
                    &entry, identity, &temporary, contents, 0o600,
                )?;
                let current = self
                    .root
                    .open_bound_file(coordinated)?
                    .ok_or_else(|| anyhow!("updated media-library state file disappeared"))?;
                if current.identity() != new_identity {
                    bail!("media-library state file changed after update");
                }
                current.validate_private_single_link(0o600)?;
            } else {
                self.root
                    .create_new_bound_file(coordinated, contents, 0o600)?;
            }
            Ok(())
        })
    }

    fn coordinate_write<T>(
        &self,
        path: &Path,
        mut action: impl FnMut(&Path) -> Result<T>,
    ) -> Result<T> {
        let mut result = None;
        let mut accessor = |coordinated_path: &Path| -> Result<()> {
            let rooted_path = coordinated_path_under_root(&self.root, coordinated_path)?;
            result = Some(action(&rooted_path)?);
            Ok(())
        };
        self.provider
            .coordinate_write(path, &mut accessor)
            .map_err(|error| classify_deadlock_error(path, "write", error))?;
        result.ok_or_else(|| {
            anyhow!(
                "File Provider supplied no write accessor for {}",
                path.display()
            )
        })
    }

    fn validate_library_directory(&self) -> Result<()> {
        let entry = self.root.bind_entry(&self.library_dir, false)?;
        let identity = self
            .root
            .bound_entry_identity(&entry)?
            .ok_or_else(|| anyhow!("private media-library directory disappeared"))?;
        if identity != self.library_dir_identity {
            bail!("private media-library directory identity changed");
        }
        self.root
            .validate_private_bound_directory(&entry, identity, 0o700)
    }

    fn ensure_lock_file(&self) -> Result<()> {
        let path = self.library_dir.join(LIBRARY_LOCK);
        self.coordinate_write(&path, |coordinated| {
            self.validate_library_directory()?;
            if self.root.open_bound_file(coordinated)?.is_none() {
                match self.root.create_new_bound_file(coordinated, &[], 0o600) {
                    Ok(_) => {}
                    Err(_) if self.root.open_bound_file(coordinated)?.is_some() => {}
                    Err(error) => return Err(error),
                }
            }
            let file = self
                .root
                .open_bound_file(coordinated)?
                .ok_or_else(|| anyhow!("media-library lock file disappeared"))?;
            file.validate_private_single_link(0o600)
        })
    }

    fn ensure_output_lock_file(&self) -> Result<()> {
        let path = self.root.logical_root_path().to_path_buf();
        self.coordinate_write(&path, |coordinated| {
            if coordinated != path {
                bail!("coordinated download root changed while opening its output lock");
            }
            video_output_lock_file(&self.root).map(|_| ())
        })
    }

    fn lock_store(&self) -> Result<StoreGuards> {
        let output = self.lock_output()?;
        let library = self.lock_library()?;
        Ok(StoreGuards {
            _output: output,
            _library: library,
        })
    }

    fn lock_library(&self) -> Result<BoundFile> {
        let path = self.library_dir.join(LIBRARY_LOCK);
        let file = self.coordinate_read(&path, |coordinated| {
            self.validate_library_directory()?;
            let file = self
                .root
                .open_bound_file(coordinated)?
                .ok_or_else(|| anyhow!("media-library lock file disappeared"))?;
            file.validate_private_single_link(0o600)?;
            Ok(file)
        })?;
        file.lock_exclusive()?;
        Ok(file)
    }

    fn lock_output(&self) -> Result<BoundFile> {
        let path = self.root.logical_root_path().to_path_buf();
        let file = self.coordinate_write(&path, |coordinated| {
            if coordinated != path {
                bail!("coordinated download root changed while acquiring its output lock");
            }
            video_output_lock_file(&self.root)
        })?;
        loop {
            if file.try_lock_exclusive()? {
                return Ok(file);
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}

#[derive(Debug)]
struct IdentityToken {
    device: u64,
    inode: u64,
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    birth_seconds: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
enum MoveFileAction {
    Moved,
    AlreadyMoved,
}

fn ensure_private_directory(
    root: &RootedFs,
    provider: &dyn QueueFileProvider,
    path: &Path,
) -> Result<EntryIdentity> {
    let mut directory_identity = None;
    let mut accessor = |coordinated_path: &Path| -> Result<()> {
        let rooted_path = coordinated_path_under_root(root, coordinated_path)?;
        if rooted_path != path {
            bail!("coordinated media-library path changed during creation");
        }
        let _ = root.create_dir(&rooted_path, 0o700)?;
        let entry = root.bind_entry(&rooted_path, false)?;
        let identity = root
            .bound_entry_identity(&entry)?
            .ok_or_else(|| anyhow!("private media-library directory disappeared"))?;
        root.validate_private_bound_directory(&entry, identity, 0o700)
            .context("media-library state directory must be owner-private")?;
        directory_identity = Some(identity);
        Ok(())
    };
    provider
        .coordinate_write(path, &mut accessor)
        .map_err(|error| classify_deadlock_error(path, "write", error))?;
    directory_identity.ok_or_else(|| {
        anyhow!("File Provider supplied no private media-library directory accessor")
    })
}

fn coordinated_path_under_root(root: &RootedFs, path: &Path) -> Result<PathBuf> {
    // File Provider can return canonical /private/... URLs for roots configured through the
    // verified /var, /tmp, or /etc aliases. Normalize only those fixed system aliases; arbitrary
    // path components remain subject to the rooted-prefix and normal-component checks below.
    let path = crate::media_libraries::normalize_trusted_macos_alias(path)?;
    let logical = root.logical_root_path();
    let normalized_logical = crate::media_libraries::normalize_trusted_macos_alias(logical)?;
    let canonical = root.root_path();
    let normalized_canonical = crate::media_libraries::normalize_trusted_macos_alias(canonical)?;
    if path == normalized_logical || path == normalized_canonical {
        return Ok(logical.to_path_buf());
    }
    let relative = path
        .strip_prefix(&normalized_logical)
        .or_else(|_| path.strip_prefix(&normalized_canonical))
        .map_err(|_| anyhow!("coordinated path is outside the configured media root"))?;
    for component in relative.components() {
        if !matches!(component, Component::Normal(_)) {
            bail!("coordinated path contains an invalid component");
        }
    }
    Ok(logical.join(relative))
}

fn safe_relative_path(value: &str, allow_empty: bool) -> Result<PathBuf> {
    if value.is_empty() {
        if allow_empty {
            return Ok(PathBuf::new());
        }
        bail!("relative file path must not be empty");
    }
    if value.len() > 2048 || value.contains('\0') || value.contains('\\') {
        bail!("relative path is invalid or too long");
    }
    let path = Path::new(value);
    let mut normalized = PathBuf::new();
    for component in path.components() {
        let Component::Normal(name) = component else {
            bail!("relative path must not contain root, current, or parent components");
        };
        let name = name
            .to_str()
            .ok_or_else(|| anyhow!("relative path is not UTF-8"))?;
        if name.len() > MAX_PATH_COMPONENT_BYTES {
            bail!(
                "relative path component exceeds the {MAX_PATH_COMPONENT_BYTES}-byte filesystem limit"
            );
        }
        normalized.push(name);
    }
    if normalized.as_os_str().is_empty() && !allow_empty {
        bail!("relative file path must not be empty");
    }
    Ok(normalized)
}

fn path_to_relative_string(path: &Path) -> Result<String> {
    let mut pieces = Vec::new();
    for component in path.components() {
        let Component::Normal(name) = component else {
            bail!("path is not root-relative");
        };
        pieces.push(
            name.to_str()
                .ok_or_else(|| anyhow!("path contains a non-UTF-8 component"))?,
        );
    }
    Ok(pieces.join("/"))
}

fn safe_file_stem(title: &str) -> String {
    let mut value = String::new();
    for character in title.chars() {
        let safe = if character.is_control() || matches!(character, '/' | '\\' | ':' | '\0') {
            '_'
        } else {
            character
        };
        if value.len() + safe.len_utf8() > 150 {
            break;
        }
        value.push(safe);
    }
    let mut value = value.trim().trim_matches('.').to_string();
    while value.contains("..") {
        value = value.replace("..", ".");
    }
    if value.is_empty() {
        "Untitled".to_string()
    } else {
        value
    }
}

fn standard_media_stem(item: &LibraryItem) -> String {
    let mut pieces = Vec::new();
    if let Some(collection) = &item.collection {
        if !collection.title.trim().is_empty() {
            pieces.push(safe_file_stem(collection.title.trim()));
        }
        if let Some(order) = collection
            .order
            .filter(|order| *order > 0)
            .or_else(|| collection.part_id.as_deref().and_then(part_ordinal))
        {
            pieces.push(format!("P{order:02}"));
        }
    }
    pieces.push(safe_file_stem(&item.title));
    if let Some(source_id) = item.source_ids.first() {
        let id = safe_file_stem(source_id);
        if !id.is_empty() {
            pieces.push(format!("[{id}]"));
        }
    }
    let mut specs = Vec::new();
    if let Some(height) = item.height.filter(|height| *height > 0) {
        specs.push(format!("{height}p"));
    }
    if let Some(codec) = item
        .codec
        .as_deref()
        .filter(|codec| !codec.trim().is_empty())
    {
        specs.push(safe_file_stem(&codec.to_ascii_lowercase()));
    }
    if !specs.is_empty() {
        pieces.push(format!("[{}]", specs.join(" ")));
    }
    safe_file_stem(&pieces.join(" - "))
}

fn part_ordinal(part_id: &str) -> Option<u32> {
    let value = part_id.trim();
    let digits = value
        .strip_prefix('P')
        .or_else(|| value.strip_prefix('p'))
        .unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u32>().ok().filter(|ordinal| *ordinal > 0)
}

fn organized_target_directories(
    items: &[LibraryItem],
    base: &Path,
    root: &RootedFs,
    reservations: &mut TargetReservationSet,
) -> Result<HashMap<String, PathBuf>> {
    let mut key_for = |path: &Path| {
        let relative_path = path_to_relative_string(path)?;
        reservations.key_for(root, &relative_path)
    };
    organized_target_directories_with_key(items, base, &mut key_for)
}

fn organized_target_directories_with_key<F>(
    items: &[LibraryItem],
    base: &Path,
    key_for: &mut F,
) -> Result<HashMap<String, PathBuf>>
where
    F: FnMut(&Path) -> Result<TargetReservationKey>,
{
    let mut collection_titles = BTreeMap::<Vec<u8>, String>::new();
    let mut collection_identities_by_item = HashMap::new();
    for item in items {
        let Some(collection) = item
            .collection
            .as_ref()
            .filter(|collection| !collection.kind.trim().is_empty())
        else {
            continue;
        };
        let title = collection.title.trim();
        if title.is_empty() {
            continue;
        }
        let identity = collection_directory_identity(collection)?;
        collection_titles
            .entry(identity.clone())
            .and_modify(|existing| {
                if title < existing.as_str() {
                    *existing = title.to_string();
                }
            })
            .or_insert_with(|| title.to_string());
        collection_identities_by_item.insert(item.id.as_str(), identity);
    }

    let identities = collection_titles.keys().cloned().collect::<Vec<_>>();
    let mut hashed_identities = HashSet::<Vec<u8>>::new();
    let mut base_name_owners = HashMap::<TargetReservationKey, Vec<Vec<u8>>>::new();
    for identity in &identities {
        let title = &collection_titles[identity];
        let candidate = base.join(safe_file_stem(title));
        base_name_owners
            .entry(key_for(&candidate)?)
            .or_default()
            .push(identity.clone());
    }
    for owners in base_name_owners.values().filter(|owners| owners.len() > 1) {
        hashed_identities.extend(owners.iter().cloned());
    }

    const MAX_COLLISION_PASSES: usize = 8;
    let mut collision_passes = 0usize;
    let mut hash_round = 0u32;
    let folder_names = loop {
        let mut folders = BTreeMap::<Vec<u8>, String>::new();
        let mut final_name_owners = HashMap::<TargetReservationKey, Vec<Vec<u8>>>::new();
        for identity in &identities {
            let title = &collection_titles[identity];
            let stem = safe_file_stem(title);
            let folder = if hashed_identities.contains(identity) {
                let discriminator = collection_directory_discriminator(identity, hash_round)?;
                format!("{stem} [{discriminator}]")
            } else {
                stem
            };
            let target_path = base.join(&folder);
            final_name_owners
                .entry(key_for(&target_path)?)
                .or_default()
                .push(identity.clone());
            folders.insert(identity.clone(), folder);
        }

        let collisions = final_name_owners
            .into_values()
            .filter(|owners| owners.len() > 1)
            .collect::<Vec<_>>();
        if collisions.is_empty() {
            break folders;
        }

        collision_passes += 1;
        if collision_passes > MAX_COLLISION_PASSES {
            bail!("collection folder names cannot be disambiguated within the retry limit");
        }

        let mut newly_hashed = false;
        for owners in collisions {
            for identity in owners {
                newly_hashed |= hashed_identities.insert(identity);
            }
        }
        if !newly_hashed {
            hash_round = hash_round
                .checked_add(1)
                .filter(|round| *round < 8)
                .ok_or_else(|| anyhow!("collection folder names cannot be disambiguated safely"))?;
        }
    };

    let base_name = base.file_name().and_then(OsStr::to_str);
    let base_parent = base.parent().unwrap_or_else(|| Path::new(""));
    let base_key = base_name
        .map(|name| key_for(&base_parent.join(name)))
        .transpose()?;

    let mut targets = HashMap::with_capacity(items.len());
    for item in items {
        let Some(identity) = collection_identities_by_item.get(item.id.as_str()) else {
            targets.insert(item.id.clone(), base.to_path_buf());
            continue;
        };
        let folder = folder_names
            .get(identity)
            .ok_or_else(|| anyhow!("collection folder identity was not indexed"))?;
        let folder_key = key_for(&base_parent.join(folder))?;
        let target_dir = if base_key.as_ref().is_some_and(|key| *key == folder_key) {
            base.to_path_buf()
        } else {
            base.join(folder)
        };
        targets.insert(item.id.clone(), target_dir);
    }
    Ok(targets)
}

fn collection_directory_identity(collection: &CollectionMetadata) -> Result<Vec<u8>> {
    let (identity_source, collection_key) =
        match collection.id.as_deref().filter(|id| !id.trim().is_empty()) {
            Some(id) => ("id", id.trim()),
            None => ("title", collection.title.trim()),
        };
    serde_json::to_vec(&(collection.kind.trim(), identity_source, collection_key))
        .context("serialize collection directory identity")
}

fn collection_directory_discriminator(identity: &[u8], round: u32) -> Result<String> {
    let encoded_identity = serde_json::to_vec(&(identity, round))
        .context("serialize collection directory discriminator")?;
    Ok(format!("{:x}", Sha256::digest(encoded_identity)))
}

fn filename_tail_bytes(source: &Path, suffix: &str) -> Result<usize> {
    let extension = source
        .extension()
        .and_then(OsStr::to_str)
        .filter(|extension| !extension.is_empty());
    let extension_bytes = extension.map_or(0, |extension| extension.len() + 1);
    suffix
        .len()
        .checked_add(extension_bytes)
        .ok_or_else(|| anyhow!("filename suffix length overflowed"))
}

fn budgeted_group_stem(
    requested_stem: &str,
    longest_filename_tail: usize,
    keep_both_suffix: &str,
    allow_truncation: bool,
) -> Result<String> {
    let reserved_bytes = longest_filename_tail
        .checked_add(keep_both_suffix.len())
        .ok_or_else(|| anyhow!("filename component length overflowed"))?;
    let stem_budget = MAX_FILENAME_COMPONENT_BYTES
        .checked_sub(reserved_bytes)
        .ok_or_else(|| {
            anyhow!(
                "attachment suffix and extension exceed the {MAX_FILENAME_COMPONENT_BYTES}-byte filename component limit"
            )
        })?;

    let mut fitted = String::new();
    for character in requested_stem.chars() {
        if fitted.len() + character.len_utf8() > stem_budget {
            break;
        }
        fitted.push(character);
    }
    if fitted.len() < requested_stem.len() && !allow_truncation {
        bail!(
            "the existing media stem cannot fit its attached filenames within the {MAX_FILENAME_COMPONENT_BYTES}-byte component limit"
        );
    }
    let fitted = if allow_truncation {
        fitted.trim_end().trim_end_matches('.')
    } else {
        fitted.as_str()
    };
    if fitted.is_empty() {
        bail!(
            "attachment suffixes leave no complete media-stem character within the {MAX_FILENAME_COMPONENT_BYTES}-byte component limit"
        );
    }
    Ok(format!("{fitted}{keep_both_suffix}"))
}

fn file_name_with_stem(source: &Path, stem: &str, suffix: &str) -> Result<String> {
    let extension = source.extension().and_then(OsStr::to_str);
    let mut file_name = format!("{stem}{suffix}");
    if let Some(extension) = extension.filter(|extension| !extension.is_empty()) {
        file_name.push('.');
        file_name.push_str(extension);
    }
    if file_name.len() > MAX_FILENAME_COMPONENT_BYTES {
        bail!(
            "generated filename component exceeds the {MAX_FILENAME_COMPONENT_BYTES}-byte filesystem limit"
        );
    }
    Ok(file_name)
}

fn with_extension(path: &Path, extension: &str) -> PathBuf {
    path.with_extension(extension)
}

fn safe_entry_identity(root: &RootedFs, path: &Path) -> Result<Option<EntryIdentity>> {
    let relative = path
        .strip_prefix(root.logical_root_path())
        .map_err(|_| anyhow!("path is outside the configured media root"))?;
    let mut components = relative.components().peekable();
    let mut current = root.logical_root_path().to_path_buf();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            bail!("path contains an invalid component");
        };
        current.push(name);
        if components.peek().is_some() {
            match root.entry_identity(&current)? {
                Some(identity) if identity.is_dir() => {}
                Some(_) => bail!("path parent is not a directory"),
                None => return Ok(None),
            }
        }
    }
    root.entry_identity(path)
}

fn migration_proof_for_bound_file(file: &BoundFile) -> Result<MigrationFileProof> {
    let before = fingerprint(file)?;
    let access_before = file.access_policy()?;
    let (_, sha256) = token_for_bound_file(file)?;
    let after = fingerprint(file)?;
    let access_after = file.access_policy()?;
    if !same_protected_fingerprint(&before, &after) || access_before != access_after {
        bail!("selected file identity, content length, or access policy changed while hashing");
    }
    Ok(MigrationFileProof {
        device: before.identity.device(),
        inode: before.identity.inode(),
        size: before.size,
        mode: access_before.mode,
        owner: access_before.owner,
        group: access_before.group,
        link_count: access_before.link_count,
        birth_seconds: before.birth_seconds,
        birth_nanoseconds: before.birth_nanoseconds,
        sha256,
    })
}

fn validate_migration_publication_staging_access(
    file: &BoundFile,
    expected_final_access: BoundFileAccessPolicy,
) -> Result<()> {
    file.validate_identity()?;
    let actual = file.access_policy()?;
    let expected_mode = expected_final_access.mode & 0o777;
    if expected_final_access.link_count != 1
        || expected_final_access.mode & !0o777 != 0
        || actual.owner != expected_final_access.owner
        || actual.group != expected_final_access.group
        || actual.link_count != expected_final_access.link_count
        || actual.mode & 0o7000 != 0
        || (actual.mode & 0o777 != 0o600 && actual.mode & 0o777 != expected_mode)
    {
        bail!("migration staging ownership, permissions, or link count changed");
    }
    Ok(())
}

fn validate_task_control_source(
    file: &BoundFile,
    expected_size: u64,
    expected_access: BoundFileAccessPolicy,
    expected_digest: [u8; 32],
) -> Result<()> {
    file.validate_identity()?;
    if file.byte_len()? != expected_size
        || file.access_policy()? != expected_access
        || file.sha256()? != expected_digest
    {
        bail!("task staging control content or access policy changed before cleanup");
    }
    file.validate_identity()
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn task_transfer_access_policy(file: &TaskTransferFile) -> Result<BoundFileAccessPolicy> {
    if file.kind != TaskTransferEntryKind::File {
        bail!("task file access policy requested for a directory entry");
    }
    let link_count = file
        .link_count
        .ok_or_else(|| anyhow!("task transfer file is missing its hard-link count proof"))?;
    if link_count != 1 {
        bail!("task transfer file has an unexpected hard-link count");
    }
    Ok(BoundFileAccessPolicy {
        owner: file.owner,
        group: file.group,
        mode: file.mode,
        link_count,
    })
}

fn is_task_staging_relative(path: &Path) -> bool {
    if path.as_os_str().len() > 2048 {
        return false;
    }
    let mut components = path.components();
    let Some(Component::Normal(root)) = components.next() else {
        return false;
    };
    if root != ".telegram-video-downloader-staging" {
        return false;
    }
    let Some(Component::Normal(job)) = components.next() else {
        return false;
    };
    if !job.to_string_lossy().starts_with("job-") {
        return false;
    }
    components.all(|component| matches!(component, Component::Normal(_)))
}

fn migration_targets_conflict(
    target: &LibraryManager,
    paths: &[String],
    reserved: &mut TargetReservationSet,
) -> Result<bool> {
    let mut local = TargetReservationSet {
        private_library_identity: Some(target.library_dir_identity),
        ..TargetReservationSet::default()
    };
    for path in paths {
        let absolute = target.absolute_path(path)?;
        if reserved.contains(&target.root, path)?
            || local.contains(&target.root, path)?
            || target
                .root
                .entry_identity_if_parent_exists(&absolute)?
                .is_some()
        {
            return Ok(true);
        }
        local.insert(&target.root, path)?;
    }
    Ok(false)
}

fn parse_sha256(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("migration SHA-256 value is invalid");
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let start = index * 2;
        *byte = u8::from_str_radix(&value[start..start + 2], 16)
            .context("migration SHA-256 value is invalid")?;
    }
    Ok(digest)
}

fn ensure_relative_parents(root: &RootedFs, path: &Path) -> Result<()> {
    let relative = path
        .strip_prefix(root.logical_root_path())
        .map_err(|_| anyhow!("move destination is outside the configured media root"))?;
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let mut accumulated = PathBuf::new();
    for component in parent.components() {
        let Component::Normal(name) = component else {
            bail!("move destination parent path is invalid");
        };
        accumulated.push(name);
        let absolute = root.logical_root_path().join(&accumulated);
        if root.create_dir(&absolute, 0o755)?.is_none() {
            let identity = root
                .entry_identity(&absolute)?
                .ok_or_else(|| anyhow!("move destination directory disappeared"))?;
            if !identity.is_dir() {
                bail!("move destination path contains a non-directory entry");
            }
        }
    }
    Ok(())
}

fn fingerprint(file: &BoundFile) -> Result<FileFingerprint> {
    file.validate_identity()?;
    let std_file = file.duplicate_std_file()?;
    let metadata = std_file
        .metadata()
        .context("failed to inspect selected file metadata")?;
    let identity = file.identity();
    if metadata.dev() != identity.device() || metadata.ino() != identity.inode() {
        bail!("selected descriptor does not match its rooted file identity");
    }
    let birth = metadata
        .created()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok());
    Ok(FileFingerprint {
        identity,
        size: metadata.len(),
        mode: metadata.mode(),
        uid: metadata.uid(),
        gid: metadata.gid(),
        birth_seconds: birth.as_ref().map(|time| time.as_secs() as i64),
        birth_nanoseconds: birth.map(|time| time.subsec_nanos() as i64),
    })
}

fn physical_item_identity(fingerprint: &FileFingerprint) -> PhysicalItemIdentity {
    PhysicalItemIdentity::from_parts(
        fingerprint.identity.device(),
        fingerprint.identity.inode(),
        fingerprint.birth_seconds,
        fingerprint.birth_nanoseconds,
    )
}

fn same_physical_item_descriptor(left: &FileFingerprint, right: &FileFingerprint) -> bool {
    left.identity.is_file()
        && right.identity.is_file()
        && left.identity == right.identity
        && physical_item_identity(left) == physical_item_identity(right)
}

fn token_for_bound_file(file: &BoundFile) -> Result<(String, String)> {
    // Protected signals: dev/inode/type select the exact object; the full digest protects content
    // stability without forcing routine scans to hash media; size corroborates the byte count;
    // mode/owner protect ordinary POSIX access policy. mtime/ctime and nlink are deliberately not
    // used as mutation verdicts because harmless metadata changes and File Provider transitions
    // can change them without changing the selected object, content, or mode/owner policy.
    let before = fingerprint(file)?;
    let digest = sha256_bound_file(file)?;
    let repeated_digest = sha256_bound_file(file)?;
    if digest != repeated_digest {
        bail!("selected file content changed while its preview identity was being captured");
    }
    let after = fingerprint(file)?;
    if !same_protected_fingerprint(&before, &after) {
        bail!("selected file identity, content length, or access policy changed while hashing");
    }
    let token = format!(
        "{}:{}:f:{}:{}:{}:{}:{}:{}:{}",
        before.identity.device(),
        before.identity.inode(),
        before.size,
        before.mode,
        before.uid,
        before.gid,
        before
            .birth_seconds
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        before
            .birth_nanoseconds
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        digest,
    );
    Ok((token, digest))
}

fn decode_identity_token(token: &str) -> Result<IdentityToken> {
    let fields = token.split(':').collect::<Vec<_>>();
    if fields.len() != 10 || fields[2] != "f" || fields[9].len() != 64 {
        bail!("move preview file identity token is invalid");
    }
    let birth_seconds = parse_optional_i64(fields[7])?;
    let birth_nanoseconds = parse_optional_i64(fields[8])?;
    if birth_seconds.is_some() != birth_nanoseconds.is_some()
        || birth_nanoseconds.is_some_and(|value| !(0..1_000_000_000).contains(&value))
    {
        bail!("move preview birth time is invalid");
    }
    Ok(IdentityToken {
        device: fields[0]
            .parse()
            .context("invalid preview device identity")?,
        inode: fields[1]
            .parse()
            .context("invalid preview inode identity")?,
        size: fields[3].parse().context("invalid preview file size")?,
        mode: fields[4].parse().context("invalid preview file mode")?,
        uid: fields[5].parse().context("invalid preview file owner")?,
        gid: fields[6].parse().context("invalid preview file group")?,
        birth_seconds,
    })
}

#[cfg(test)]
fn identity_token_without_creation_time_for_test(token: &str) -> Result<String> {
    let mut fields = token.split(':').map(str::to_owned).collect::<Vec<_>>();
    if fields.len() != 10 || fields[2] != "f" {
        bail!("move preview file identity token is invalid");
    }
    fields[7] = "-".to_string();
    fields[8] = "-".to_string();
    Ok(fields.join(":"))
}

#[cfg(test)]
fn identity_token_with_creation_time_for_test(
    token: &str,
    seconds: i64,
    nanoseconds: i64,
) -> Result<String> {
    let mut fields = token.split(':').map(str::to_owned).collect::<Vec<_>>();
    if fields.len() != 10 || fields[2] != "f" {
        bail!("move preview file identity token is invalid");
    }
    fields[7] = seconds.to_string();
    fields[8] = nanoseconds.to_string();
    decode_identity_token(&fields.join(":"))?;
    Ok(fields.join(":"))
}

fn identity_tokens_equal_except_birth_time(left: &str, right: &str) -> Result<bool> {
    if left == right {
        return Ok(true);
    }
    let left_identity = decode_identity_token(left)?;
    let right_identity = decode_identity_token(right)?;
    let left_fields = left.split(':').collect::<Vec<_>>();
    let right_fields = right.split(':').collect::<Vec<_>>();
    Ok(left_identity.device == right_identity.device
        && left_identity.inode == right_identity.inode
        && left_identity.size == right_identity.size
        && left_identity.mode == right_identity.mode
        && left_identity.uid == right_identity.uid
        && left_identity.gid == right_identity.gid
        && left_fields[9] == right_fields[9])
}

fn identity_tokens_match_with_pinned_file(
    expected: &str,
    current: &str,
    current_file: &BoundFile,
    path_identity: Option<EntryIdentity>,
    anchor: Option<&BoundFile>,
) -> Result<bool> {
    if expected == current {
        return Ok(true);
    }
    let Some(anchor) = anchor else {
        return Ok(false);
    };
    if !identity_tokens_equal_except_birth_time(expected, current)? {
        return Ok(false);
    }

    let expected_identity = decode_identity_token(expected)?;
    let current_identity = decode_identity_token(current)?;
    let pinned_identity = anchor.identity();
    if !pinned_identity.is_file()
        || !current_file.identity().is_file()
        || pinned_identity != current_file.identity()
        || path_identity != Some(pinned_identity)
        || pinned_identity.device() != expected_identity.device
        || pinned_identity.inode() != expected_identity.inode
        || pinned_identity.device() != current_identity.device
        || pinned_identity.inode() != current_identity.inode
    {
        return Ok(false);
    }

    anchor.validate_identity()?;
    current_file.validate_identity()?;
    let (anchor_current, _) = token_for_bound_file(anchor)?;
    identity_tokens_equal_except_birth_time(current, &anchor_current)
}

fn token_size(token: &str) -> Result<u64> {
    Ok(decode_identity_token(token)?.size)
}

fn parse_optional_i64(value: &str) -> Result<Option<i64>> {
    if value == "-" {
        Ok(None)
    } else {
        value
            .parse()
            .map(Some)
            .context("invalid preview birth time")
    }
}

fn same_protected_fingerprint(left: &FileFingerprint, right: &FileFingerprint) -> bool {
    left.identity == right.identity
        && left.size == right.size
        && left.mode == right.mode
        && left.uid == right.uid
        && left.gid == right.gid
        && left.birth_seconds == right.birth_seconds
        && left.birth_nanoseconds == right.birth_nanoseconds
}

fn item_group_ready(manifest: &BatchManifest, item_id: &str) -> bool {
    manifest
        .files
        .iter()
        .filter(|file| file.item_id == item_id)
        .all(|file| {
            file.state == ManifestFileState::Moved || file.state == ManifestFileState::Skipped
        })
}

fn skipped_move_error(preview: &MovePreview, item_id: &str) -> String {
    preview
        .items
        .iter()
        .find(|item| item.item_id == item_id && item.outcome == MoveOutcome::Skip)
        .and_then(|item| item.reason.clone())
        .unwrap_or_else(|| "selected media item was skipped because of a move conflict".to_string())
}

fn batch_result(manifest: &BatchManifest) -> BatchMoveResult {
    BatchMoveResult {
        id: manifest.preview.id.clone(),
        revision: manifest.preview.revision.clone(),
        status: manifest.status,
        retryable_access_error: manifest.retryable_access_error,
        items: manifest.items.clone(),
        warnings: manifest.warnings.clone(),
    }
}

fn store_preview_anchors(
    preview_id: &str,
    revision: &str,
    anchors: HashMap<String, BoundFile>,
) -> Result<()> {
    if anchors.is_empty() {
        return Ok(());
    }
    if anchors.len() > MAX_PINNED_FILES_PER_PREVIEW {
        bail!(
            "preview selects too many files without a persistent creation-time signal; split the selection"
        );
    }
    start_preview_anchor_reaper()?;
    let cache = LIVE_PREVIEW_ANCHORS.get_or_init(|| Mutex::new(PreviewAnchorStore::default()));
    let mut cache = cache
        .lock()
        .map_err(|_| anyhow!("preview identity-anchor cache is poisoned"))?;
    let now = Instant::now();
    cache.prune_expired(now);
    cache.make_room_for(preview_id, anchors.len())?;
    cache.file_count = cache.file_count.saturating_add(anchors.len());
    cache.order.push_back(preview_id.to_string());
    cache.by_preview.insert(
        preview_id.to_string(),
        PreviewAnchorEntry {
            revision: revision.to_string(),
            anchors,
            expires_at: now + PREVIEW_ANCHOR_TTL,
        },
    );
    Ok(())
}

fn start_preview_anchor_reaper() -> Result<()> {
    let started = PREVIEW_ANCHOR_REAPER.get_or_init(|| {
        thread::Builder::new()
            .name("preview-anchor-reaper".to_string())
            .spawn(|| {
                loop {
                    thread::sleep(PREVIEW_ANCHOR_REAPER_INTERVAL);
                    let Some(cache) = LIVE_PREVIEW_ANCHORS.get() else {
                        continue;
                    };
                    if let Ok(mut cache) = cache.lock() {
                        cache.prune_expired(Instant::now());
                    }
                }
            })
            .map(|_| ())
            .map_err(|error| error.to_string())
    });
    started
        .as_ref()
        .map(|_| ())
        .map_err(|error| anyhow!("failed to start preview identity-anchor reaper: {error}"))
}

fn required_preview_anchor_tokens(persisted: &PersistedPreview) -> Result<HashSet<String>> {
    let mut required = HashSet::new();
    for file in persisted.preview.items.iter().flat_map(|item| &item.files) {
        if !file.identity.is_empty()
            && decode_identity_token(&file.identity)?
                .birth_seconds
                .is_none()
        {
            required.insert(file.identity.clone());
        }
    }
    for patch in &persisted.patches {
        if decode_identity_token(&patch.media_identity)?
            .birth_seconds
            .is_none()
        {
            required.insert(patch.media_identity.clone());
        }
        if let Some(identity) = patch.expected_identity.as_deref()
            && decode_identity_token(identity)?.birth_seconds.is_none()
        {
            required.insert(identity.to_string());
        }
    }
    Ok(required)
}

#[cfg(test)]
fn preview_has_required_anchors(
    persisted: &PersistedPreview,
    process_nonce: &str,
    cache: Option<&PreviewAnchorStore>,
) -> Result<bool> {
    let required = required_preview_anchor_tokens(persisted)?;
    if required.is_empty() {
        return Ok(true);
    }
    if persisted.process_nonce != process_nonce {
        return Ok(false);
    }
    let Some(cache) = cache else {
        return Ok(false);
    };
    cache.matches_required_anchors(
        &persisted.preview.id,
        &persisted.preview.revision,
        &required,
    )
}

fn require_preview_anchors(persisted: &PersistedPreview, process_nonce: &str) -> Result<()> {
    let required = required_preview_anchor_tokens(persisted)?;
    if required.is_empty() {
        return Ok(());
    }
    if persisted.process_nonce != process_nonce {
        bail!(
            "selected object has no persistent creation-time signal or live descriptor anchor; create a new preview after restart"
        );
    }
    let cache = LIVE_PREVIEW_ANCHORS.get_or_init(|| Mutex::new(PreviewAnchorStore::default()));
    let mut cache = cache
        .lock()
        .map_err(|_| anyhow!("preview identity-anchor cache is poisoned"))?;
    let now = Instant::now();
    cache.prune_expired(now);
    if cache
        .touch_for_revision(&persisted.preview.id, &persisted.preview.revision, now)
        .is_none()
    {
        bail!("selected object lost its in-process descriptor anchor; create a new preview");
    }
    if !cache.matches_required_anchors(
        &persisted.preview.id,
        &persisted.preview.revision,
        &required,
    )? {
        bail!(
            "in-process descriptor anchor no longer matches the approved object; create a new preview"
        );
    }
    Ok(())
}

pub(crate) fn release_preview_anchors(preview_id: &str) {
    let Some(cache) = LIVE_PREVIEW_ANCHORS.get() else {
        return;
    };
    let Ok(mut cache) = cache.lock() else {
        return;
    };
    cache.remove(preview_id);
}

pub(crate) fn release_preview_anchors_for_revision(preview_id: &str, revision: &str) -> bool {
    let Some(cache) = LIVE_PREVIEW_ANCHORS.get() else {
        return false;
    };
    let Ok(mut cache) = cache.lock() else {
        return false;
    };
    cache.remove_if_revision(preview_id, revision)
}

fn sha256_bound_file(file: &BoundFile) -> Result<String> {
    let mut reader = file.duplicate_std_file()?;
    reader
        .seek(SeekFrom::Start(0))
        .context("failed to seek selected file")?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .context("failed to hash selected file")?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    file.validate_identity()?;
    Ok(hex_digest(digest.finalize().as_slice()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes).as_slice())
}

fn validate_private_migration_state_file(file: &BoundFile) -> Result<()> {
    file.validate_private_single_link(0o600)
        .context("media-library state file must be owner-private")?;
    let policy = file.access_policy()?;
    if policy.mode != 0o600 || policy.link_count != 1 {
        bail!("media-library state file access policy is not exactly private mode 0600");
    }
    Ok(())
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

type IndexObjectKey = (u64, u64, Option<i64>, Option<i64>);

fn index_object_key(fingerprint: &FileFingerprint) -> IndexObjectKey {
    (
        fingerprint.identity.device(),
        fingerprint.identity.inode(),
        fingerprint.birth_seconds,
        fingerprint.birth_nanoseconds,
    )
}

fn index_record_matches(record: &IndexRecord, fingerprint: &FileFingerprint) -> bool {
    record.device == fingerprint.identity.device()
        && record.inode == fingerprint.identity.inode()
        && record.file_type == "regular_file"
        && record.birth_seconds == fingerprint.birth_seconds
        && record.birth_nanoseconds == fingerprint.birth_nanoseconds
}

fn merge_queue_metadata(current: &mut QueueMetadataProvenance, previous: &QueueMetadataProvenance) {
    if current.title.is_none() {
        current.title = previous.title.clone();
    }
    current
        .source_ids
        .extend(previous.source_ids.iter().cloned());
    current.source_ids = normalize_source_ids(std::mem::take(&mut current.source_ids));
    if let Some(previous_collection) = &previous.collection {
        merge_collection_metadata(current.collection.as_mut(), previous_collection);
        if current.collection.is_none() {
            current.collection = Some(previous_collection.clone());
        }
    }
    current.requires_confirmation |= previous.requires_confirmation;
}

fn merge_collection_metadata(
    current: Option<&mut CollectionMetadata>,
    previous: &CollectionMetadata,
) {
    let Some(current) = current else {
        return;
    };
    if current.id.is_none() {
        current.id = previous.id.clone();
    }
    if current.kind.trim().is_empty() || current.kind == "nfo_set" {
        current.kind = previous.kind.clone();
    }
    if current.title.trim().is_empty() {
        current.title = previous.title.clone();
    }
    if current.order.is_none() {
        current.order = previous.order;
    }
    if current.part_id.is_none() {
        current.part_id = previous.part_id.clone();
    }
}

fn preserve_queue_metadata(
    item: &mut LibraryItem,
    provenance: &QueueMetadataProvenance,
    sidecar_title_present: bool,
) {
    if !sidecar_title_present
        && let Some(title) = provenance
            .title
            .as_deref()
            .filter(|title| !title.trim().is_empty())
    {
        item.title = title.to_string();
    }

    item.source_ids
        .extend(provenance.source_ids.iter().cloned());
    item.source_ids = normalize_source_ids(std::mem::take(&mut item.source_ids));

    if item.organization_status != Some(OrganizationStatus::Standalone)
        && let Some(collection) = provenance.collection.as_ref()
    {
        if let Some(current) = item.collection.as_mut() {
            merge_collection_metadata(Some(current), collection);
        } else {
            item.collection = Some(collection.clone());
        }
    }

    if provenance.requires_confirmation {
        item.confidence = Confidence::NeedsConfirmation;
    } else if item.confidence == Confidence::Low
        && (provenance.title.is_some()
            || !provenance.source_ids.is_empty()
            || provenance.collection.is_some())
    {
        item.confidence = Confidence::Medium;
    }
    if item.metadata_status == MetadataStatus::Unknown
        && (provenance.title.is_some()
            || !provenance.source_ids.is_empty()
            || provenance.collection.is_some())
    {
        // Queue associations do not prove that the current sidecar is complete.
        item.metadata_status = MetadataStatus::Partial;
    }
}

fn prune_missing_index_records(
    index: &mut LibraryIndex,
    max_missing_records: usize,
    max_serialized_bytes: usize,
) -> Result<Vec<String>> {
    let mut removed = BTreeSet::new();
    let mut missing = index
        .records
        .iter()
        .filter(|(_, record)| record.item.presence == Presence::Missing)
        .map(|(id, record)| (id.clone(), record.missing_since.unwrap_or_default()))
        .collect::<Vec<_>>();
    missing.sort_by(|(left_id, left_since), (right_id, right_since)| {
        left_since
            .cmp(right_since)
            .then_with(|| left_id.cmp(right_id))
    });

    let excess = missing.len().saturating_sub(max_missing_records);
    for (id, _) in missing.iter().take(excess) {
        index.records.remove(id);
        removed.insert(id.clone());
    }

    if index_fits_budget(index, max_serialized_bytes)? {
        return Ok(removed.into_iter().collect());
    }

    let mut remaining = index
        .records
        .iter()
        .filter(|(_, record)| record.item.presence == Presence::Missing)
        .map(|(id, record)| (id.clone(), record.missing_since.unwrap_or_default()))
        .collect::<Vec<_>>();
    remaining.sort_by(|(left_id, left_since), (right_id, right_since)| {
        right_since
            .cmp(left_since)
            .then_with(|| left_id.cmp(right_id))
    });

    let mut without_missing = index.clone();
    without_missing
        .records
        .retain(|_, record| record.item.presence != Presence::Missing);
    if !index_fits_budget(&without_missing, max_serialized_bytes)? {
        // The live identity cache alone exceeds the available budget. Keep the bounded
        // missing history in memory and let the caller preserve the previous on-disk index.
        return Ok(removed.into_iter().collect());
    }

    let mut low = 0;
    let mut high = remaining.len() + 1;
    while low + 1 < high {
        let keep_count = low + (high - low) / 2;
        let mut candidate = without_missing.clone();
        for (id, _) in remaining.iter().take(keep_count) {
            if let Some(record) = index.records.get(id) {
                candidate.records.insert(id.clone(), record.clone());
            }
        }
        if index_fits_budget(&candidate, max_serialized_bytes)? {
            low = keep_count;
        } else {
            high = keep_count;
        }
    }

    for (id, _) in remaining.iter().skip(low) {
        index.records.remove(id);
        removed.insert(id.clone());
    }
    Ok(removed.into_iter().collect())
}

fn compact_index_for_persistence(index: &LibraryIndex) -> LibraryIndex {
    let mut compact = index.clone();
    for record in compact.records.values_mut() {
        if record.item.presence == Presence::Present {
            // Present media directories were completely enumerated during this scan, so these
            // attachment paths are a rebuildable view rather than durable identity metadata.
            record.item.attachments.clear();
        }
    }
    compact
}

fn index_fits_budget(index: &LibraryIndex, max_bytes: usize) -> Result<bool> {
    let limit = max_bytes.min(INDEX_LIMIT);
    let writer = IndexSerializationWriter::new(limit, false);
    Ok(serialize_index(index, writer)?.is_some())
}

struct IndexSerializationWriter {
    limit: usize,
    written: usize,
    capture: Option<Vec<u8>>,
    exceeded: bool,
}

impl IndexSerializationWriter {
    fn new(limit: usize, capture: bool) -> Self {
        Self {
            limit,
            written: 0,
            capture: capture.then(Vec::new),
            exceeded: false,
        }
    }
}

impl Write for IndexSerializationWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.written) {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "media-library index exceeds its configured size limit",
            ));
        }
        self.written += bytes.len();
        if let Some(capture) = self.capture.as_mut() {
            capture.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialize_index(
    index: &LibraryIndex,
    mut writer: IndexSerializationWriter,
) -> Result<Option<IndexSerializationWriter>> {
    match serde_json::to_writer(&mut writer, index) {
        Ok(()) => Ok(Some(writer)),
        Err(_) if writer.exceeded => Ok(None),
        Err(error) => Err(error).context("failed to serialize media-library index"),
    }
}

fn physical_item_id(fingerprint: &FileFingerprint, relative_path: &str) -> String {
    physical_item_id_from_identity(fingerprint.identity, relative_path)
}

fn physical_item_id_from_identity(identity: EntryIdentity, relative_path: &str) -> String {
    // Path participates only to keep two hard-link names distinct. The persisted index carries
    // the same ID across a unique Finder move by matching object identity and creation time.
    physical_item_id_from_device_inode(identity.device(), identity.inode(), relative_path)
}

/// Rebuild the scanned item ID from a durable migration checkpoint's saved target identity.
/// Keep this formula shared with the scanner so status projection can match a target without
/// reopening or hashing media files.
pub(crate) fn physical_item_id_from_migration_checkpoint(
    device: u64,
    inode: u64,
    relative_path: &str,
) -> String {
    physical_item_id_from_device_inode(device, inode, relative_path)
}

fn physical_item_id_from_device_inode(device: u64, inode: u64, relative_path: &str) -> String {
    let raw = format!("{device}:{inode}:{relative_path}");
    format!("copy-{}", &sha256_bytes(raw.as_bytes())[..24])
}

fn new_operation_id(prefix: &str) -> String {
    let serial = PRIVATE_TEMP_SERIAL.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let raw = format!("{prefix}:{}:{now}:{serial}", std::process::id());
    format!("{prefix}-{}", &sha256_bytes(raw.as_bytes())[..24])
}

fn validate_identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 96
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("operation identifier is invalid");
    }
    Ok(())
}

fn parse_operation_state_file_name(name: &OsStr) -> Option<(OperationStateFileKind, String)> {
    let name = name.to_str()?;
    let (kind, id) = if let Some(suffix) = name.strip_prefix(PREVIEW_PREFIX) {
        let id = suffix.strip_suffix(".json")?;
        (OperationStateFileKind::Preview, id.to_string())
    } else {
        let suffix = name.strip_prefix(BATCH_PREFIX)?;
        let id = suffix.strip_suffix(".json")?;
        (OperationStateFileKind::Batch, id.to_string())
    };
    let hash = id.strip_prefix(PREVIEW_PREFIX)?;
    if hash.len() != 24
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    Some((kind, id))
}

fn operation_state_is_expired(modified_seconds: i64, now: SystemTime, retention: Duration) -> bool {
    let now_seconds = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let cutoff = now_seconds.saturating_sub(retention.as_secs());
    modified_seconds <= i64::try_from(cutoff).unwrap_or(i64::MAX)
}

fn private_temp_sibling(path: &Path) -> PathBuf {
    let serial = PRIVATE_TEMP_SERIAL.fetch_add(1, Ordering::Relaxed);
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    path.with_file_name(format!(".{name}.next-{}-{serial}", std::process::id()))
}

fn validate_nfo_checkpoint_stage_path(target: &str, staged: &str) -> Result<()> {
    let target_path = Path::new(target);
    let staged_path = Path::new(staged);
    if target_path.parent() != staged_path.parent() {
        bail!("NFO replacement checkpoint is not a sibling of its target");
    }
    let target_name = target_path
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| anyhow!("NFO target filename is invalid"))?;
    let staged_name = staged_path
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| anyhow!("NFO checkpoint filename is invalid"))?;
    let prefix = format!(".{target_name}.next-");
    let suffix = staged_name
        .strip_prefix(&prefix)
        .ok_or_else(|| anyhow!("NFO replacement checkpoint has an unexpected filename"))?;
    let (process_id, serial) = suffix
        .split_once('-')
        .ok_or_else(|| anyhow!("NFO replacement checkpoint filename is malformed"))?;
    if process_id.is_empty()
        || serial.is_empty()
        || !process_id.bytes().all(|byte| byte.is_ascii_digit())
        || !serial.bytes().all(|byte| byte.is_ascii_digit())
    {
        bail!("NFO replacement checkpoint filename is malformed");
    }
    Ok(())
}

#[derive(Debug, Default)]
struct MediaSpecs {
    width: Option<u32>,
    height: Option<u32>,
    codec: Option<String>,
}

fn probe_media(file: &BoundFile) -> Result<MediaSpecs> {
    // Keep ffprobe inside the already-opened, no-follow file object. Passing the public path
    // again would let a replacement symlink redirect the subprocess outside the library root.
    // The File Provider read accessor has ended before this descriptor is duplicated or the
    // subprocess is launched, so waiting for ffprobe never holds a coordination scope.
    let input = file.duplicate_std_file()?;
    let mut child = match Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type,codec_name,width,height:stream_disposition=attached_pic",
            "-of",
            "json",
            "-i",
            "/dev/fd/0",
        ])
        .stdin(Stdio::from(input))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MediaSpecs::default());
        }
        Err(error) => return Err(error).context("failed to start ffprobe"),
    };
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        bail!("ffprobe stdout pipe was not available");
    };
    let output_reader = thread::spawn(move || -> std::io::Result<(Vec<u8>, bool)> {
        let mut stdout = stdout;
        let mut captured = Vec::with_capacity(FFPROBE_OUTPUT_LIMIT.min(8 * 1024));
        let mut exceeded = false;
        let mut buffer = [0_u8; 4096];
        loop {
            let count = stdout.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            let remaining = FFPROBE_OUTPUT_LIMIT.saturating_sub(captured.len());
            let keep = remaining.min(count);
            captured.extend_from_slice(&buffer[..keep]);
            exceeded |= keep < count;
            // Continue draining with bounded memory. The child is still constrained by the
            // fixed deadline below, and otherwise could block forever on a full pipe.
        }
        Ok((captured, exceeded))
    });
    let deadline = std::time::Instant::now() + FFPROBE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = output_reader.join();
                bail!(
                    "ffprobe timed out after {} seconds",
                    FFPROBE_TIMEOUT.as_secs()
                );
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = output_reader.join();
                return Err(error).context("failed to check ffprobe status");
            }
        }
    };
    let (stdout, exceeded) = output_reader
        .join()
        .map_err(|_| anyhow!("ffprobe stdout reader panicked"))?
        .context("failed to collect ffprobe output")?;
    if exceeded {
        bail!("ffprobe output exceeded its size limit");
    }
    if !status.success() {
        return Ok(MediaSpecs::default());
    }
    let value: serde_json::Value =
        serde_json::from_slice(&stdout).context("ffprobe returned invalid JSON")?;
    Ok(media_specs_from_ffprobe_json(&value))
}

fn media_specs_from_ffprobe_json(value: &serde_json::Value) -> MediaSpecs {
    let Some(streams) = value.get("streams").and_then(serde_json::Value::as_array) else {
        return MediaSpecs::default();
    };

    let real_video = streams.iter().find(|stream| {
        stream.get("codec_type").and_then(serde_json::Value::as_str) == Some("video")
            && !stream_is_attached_picture(stream)
    });
    let (stream, is_video) = if let Some(video) = real_video {
        (video, true)
    } else if let Some(audio) = streams.iter().find(|stream| {
        stream.get("codec_type").and_then(serde_json::Value::as_str) == Some("audio")
    }) {
        (audio, false)
    } else {
        return MediaSpecs::default();
    };

    MediaSpecs {
        width: is_video
            .then(|| stream.get("width").and_then(serde_json::Value::as_u64))
            .flatten()
            .and_then(|value| u32::try_from(value).ok()),
        height: is_video
            .then(|| stream.get("height").and_then(serde_json::Value::as_u64))
            .flatten()
            .and_then(|value| u32::try_from(value).ok()),
        codec: stream
            .get("codec_name")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
    }
}

fn stream_is_attached_picture(stream: &serde_json::Value) -> bool {
    let Some(value) = stream
        .get("disposition")
        .and_then(|disposition| disposition.get("attached_pic"))
    else {
        return false;
    };
    value.as_bool() == Some(true) || value.as_u64() == Some(1)
}

fn excluded_entry(
    name: &OsStr,
    identity: EntryIdentity,
    semantics: TargetNameSemantics,
    private_library_identity: EntryIdentity,
) -> Result<bool> {
    let Some(name) = name.to_str() else {
        return Ok(true);
    };
    if identity == private_library_identity {
        return Ok(true);
    }
    is_private_downloader_component(name, semantics)
}

fn is_primary_media(path: &str) -> bool {
    let extension = Path::new(path)
        .extension()
        .and_then(OsStr::to_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(
        extension.as_str(),
        "mp4"
            | "mkv"
            | "webm"
            | "mov"
            | "avi"
            | "flv"
            | "m4v"
            | "ts"
            | "m2ts"
            | "wmv"
            | "mpg"
            | "mpeg"
            | "3gp"
            | "mp3"
            | "m4a"
            | "aac"
            | "flac"
            | "wav"
            | "ogg"
            | "opus"
            | "wma"
    )
}

fn is_sidecar(path: &str) -> bool {
    let extension = Path::new(path)
        .extension()
        .and_then(OsStr::to_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(
        extension.as_str(),
        "nfo"
            | "json"
            | "description"
            | "jpg"
            | "jpeg"
            | "png"
            | "webp"
            | "srt"
            | "vtt"
            | "ass"
            | "xml"
            | "txt"
    )
}

impl SidecarIndex {
    fn build(
        files: &[ScannedFile],
        incomplete_directories: &BTreeSet<String>,
        directory_name_semantics: &HashMap<String, TargetNameSemantics>,
    ) -> Result<Self> {
        Self::build_with_case_fold(
            files,
            incomplete_directories,
            directory_name_semantics,
            platform_case_fold,
        )
    }

    fn build_with_case_fold<F>(
        files: &[ScannedFile],
        incomplete_directories: &BTreeSet<String>,
        directory_name_semantics: &HashMap<String, TargetNameSemantics>,
        case_fold: F,
    ) -> Result<Self>
    where
        F: Fn(&str) -> Result<String> + Copy,
    {
        let mut owners: HashMap<String, HashMap<String, PrimaryStemOwner>> = HashMap::new();
        for file in files
            .iter()
            .filter(|file| is_primary_media(&file.relative_path))
        {
            let path = Path::new(&file.relative_path);
            let parent = path
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .to_string_lossy()
                .into_owned();
            if incomplete_directories.contains(&parent) {
                continue;
            }
            let semantics = directory_name_semantics
                .get(&parent)
                .copied()
                .ok_or_else(|| anyhow!("filesystem name semantics unavailable under {parent}"))?;
            let Some(stem) = path.file_stem().and_then(OsStr::to_str) else {
                continue;
            };
            let key = target_component_key_with(stem, semantics, |value| case_fold(value))?;
            let stems = owners.entry(parent).or_default();
            match stems.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let owner = entry.get_mut();
                    owner.count = owner.count.saturating_add(1);
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(PrimaryStemOwner {
                        relative_path: file.relative_path.clone(),
                        count: 1,
                    });
                }
            }
        }

        let mut index = Self::default();
        for file in files.iter().filter(|file| is_sidecar(&file.relative_path)) {
            let path = Path::new(&file.relative_path);
            let parent = path
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .to_string_lossy()
                .into_owned();
            if incomplete_directories.contains(&parent) {
                continue;
            }
            let semantics = directory_name_semantics
                .get(&parent)
                .copied()
                .ok_or_else(|| anyhow!("filesystem name semantics unavailable under {parent}"))?;
            let Some(stem) = path.file_stem().and_then(OsStr::to_str) else {
                continue;
            };
            let Some(stems) = owners.get(&parent) else {
                continue;
            };
            let Some(owner) =
                longest_matching_primary_stem_with(stem, stems, semantics, case_fold)?
            else {
                continue;
            };
            if owner.count > 1 {
                index.ambiguous_count = index.ambiguous_count.saturating_add(1);
                continue;
            }
            index
                .by_media
                .entry(owner.relative_path.clone())
                .or_default()
                .push(file.relative_path.clone());
        }
        for attachments in index.by_media.values_mut() {
            attachments.sort();
        }
        for (media_relative_path, attachments) in &index.by_media {
            let media = Path::new(media_relative_path);
            let parent = media
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .to_string_lossy()
                .into_owned();
            let semantics = directory_name_semantics
                .get(&parent)
                .copied()
                .ok_or_else(|| anyhow!("filesystem name semantics unavailable under {parent}"))?;
            let Some(media_stem) = media.file_stem().and_then(OsStr::to_str) else {
                continue;
            };
            let media_key =
                target_component_key_with(media_stem, semantics, |value| case_fold(value))?;
            let mut candidates = Vec::new();
            for attachment in attachments
                .iter()
                .filter(|attachment| attachment_kind(attachment) == "nfo")
            {
                let Some(nfo_stem) = Path::new(attachment).file_stem().and_then(OsStr::to_str)
                else {
                    continue;
                };
                let nfo_key =
                    target_component_key_with(nfo_stem, semantics, |value| case_fold(value))?;
                candidates.push((nfo_key == media_key, attachment.clone()));
            }
            candidates.sort_by(|(left_exact, left_path), (right_exact, right_path)| {
                right_exact
                    .cmp(left_exact)
                    .then_with(|| left_path.cmp(right_path))
            });
            if let Some((_, authoritative_nfo)) = candidates.into_iter().next() {
                index
                    .authoritative_nfo_by_media
                    .insert(media_relative_path.clone(), authoritative_nfo);
            }
        }
        Ok(index)
    }

    fn attached_files(&self, media: &ScannedFile) -> Vec<String> {
        self.by_media
            .get(&media.relative_path)
            .cloned()
            .unwrap_or_default()
    }

    fn authoritative_nfo(&self, media_relative_path: &str) -> Option<&str> {
        self.authoritative_nfo_by_media
            .get(media_relative_path)
            .map(String::as_str)
    }
}

fn longest_matching_primary_stem_with<'a, F>(
    sidecar_stem: &str,
    primary_stems: &'a HashMap<String, PrimaryStemOwner>,
    semantics: TargetNameSemantics,
    case_fold: F,
) -> Result<Option<&'a PrimaryStemOwner>>
where
    F: Fn(&str) -> Result<String> + Copy,
{
    let exact_key = target_component_key_with(sidecar_stem, semantics, |value| case_fold(value))?;
    if let Some(owner) = primary_stems.get(&exact_key) {
        return Ok(Some(owner));
    }
    for (boundary, _) in sidecar_stem.match_indices('.').rev() {
        if boundary == 0 {
            continue;
        }
        let prefix = &sidecar_stem[..boundary];
        let prefix_key = target_component_key_with(prefix, semantics, |value| case_fold(value))?;
        if let Some(owner) = primary_stems.get(&prefix_key) {
            return Ok(Some(owner));
        }
    }
    Ok(None)
}

fn matching_sidecar_suffix_with<F>(
    sidecar_stem: &str,
    primary_stem: &str,
    semantics: TargetNameSemantics,
    case_fold: F,
) -> Result<Option<String>>
where
    F: Fn(&str) -> Result<String> + Copy,
{
    let primary_key = target_component_key_with(primary_stem, semantics, |value| case_fold(value))?;
    let sidecar_key = target_component_key_with(sidecar_stem, semantics, |value| case_fold(value))?;
    if sidecar_key == primary_key {
        return Ok(Some(String::new()));
    }
    for (boundary, _) in sidecar_stem.match_indices('.').rev() {
        if boundary == 0 {
            continue;
        }
        let prefix = &sidecar_stem[..boundary];
        let prefix_key = target_component_key_with(prefix, semantics, |value| case_fold(value))?;
        if prefix_key == primary_key {
            return Ok(Some(sidecar_stem[boundary..].to_string()));
        }
    }
    Ok(None)
}

fn attachment_kind(path: &str) -> &'static str {
    match Path::new(path)
        .extension()
        .and_then(OsStr::to_str)
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "nfo" => "nfo",
        "srt" | "vtt" | "ass" => "subtitle",
        "xml" | "json" => "danmaku_or_metadata",
        "jpg" | "jpeg" | "png" | "webp" => "cover",
        _ => "sidecar",
    }
}

fn title_from_path(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(OsStr::to_str)
        .unwrap_or("Untitled")
        .replace(['_', '\u{00a0}'], " ")
        .trim()
        .to_string()
}

fn parent_relative(path: &str) -> &str {
    Path::new(path)
        .parent()
        .and_then(Path::to_str)
        .unwrap_or("")
}

fn directory_under_incomplete(path: &str, incomplete: &BTreeSet<String>) -> bool {
    incomplete.iter().any(|directory| {
        directory.is_empty()
            || path == directory
            || path
                .strip_prefix(directory)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn unobserved_record_status(
    path: &str,
    walk: &ScanWalk,
    unreadable_status: Option<MetadataStatus>,
) -> Option<UnobservedRecordStatus> {
    let unavailable = |metadata_status: Option<MetadataStatus>| {
        Some(UnobservedRecordStatus {
            presence: Presence::Unavailable,
            metadata_status,
        })
    };

    if let Some(metadata_status) = unreadable_status {
        return unavailable(Some(metadata_status));
    }
    let parent = parent_relative(path);
    if directory_under_incomplete(parent, &walk.incomplete_directories) {
        return unavailable(Some(MetadataStatus::ProviderUnavailable));
    }
    if walk.timed_out || walk.incomplete_scan {
        return unavailable(None);
    }
    if walk.complete_directories.contains(parent) {
        return Some(UnobservedRecordStatus {
            presence: Presence::Missing,
            metadata_status: None,
        });
    }
    None
}

fn push_scan_timeout_warning(warnings: &mut Vec<String>) {
    if !warnings
        .iter()
        .any(|warning| warning.contains("120-second scan budget"))
    {
        warnings.push(
            "scan stopped at the 120-second scan budget; unvisited paths were not marked missing"
                .to_string(),
        );
    }
}

fn snapshot_revision(items: &[LibraryItem]) -> Result<String> {
    let bytes = serde_json::to_vec(items).context("failed to encode library revision")?;
    Ok(sha256_bytes(&bytes))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn empty_library_index() -> LibraryIndex {
    LibraryIndex {
        version: INDEX_VERSION,
        ..LibraryIndex::default()
    }
}

fn index_recovery_warning(reason: &str, quarantine_name: &str) -> String {
    format!(
        "rebuilt media-library index after {reason}; previous index preserved as {quarantine_name} (automatic recovery is limited to owner-private files up to {INDEX_LIMIT} bytes)"
    )
}

fn parse_index_quarantine_name(name: &OsStr) -> Option<(u128, u64)> {
    let name = name.to_str()?;
    let body = name
        .strip_prefix(INDEX_QUARANTINE_PREFIX)?
        .strip_suffix(".json")?;
    let (timestamp, serial) = body.split_once('.')?;
    let timestamp = parse_canonical_decimal(timestamp)?;
    let serial = parse_canonical_decimal(serial)?;
    if timestamp == 0 || serial > u64::MAX as u128 {
        return None;
    }
    Some((timestamp, serial as u64))
}

fn parse_canonical_decimal(value: &str) -> Option<u128> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

fn bounded_index_file_digest(file: &BoundFile) -> Result<String> {
    file.validate_private_single_link(0o600)
        .context("media-library index quarantine access policy changed")?;
    let initial_len = file.byte_len()?;
    if initial_len > INDEX_LIMIT as u64 {
        bail!("media-library index quarantine exceeds the {INDEX_LIMIT}-byte verification limit");
    }
    let contents = file.read_limited(INDEX_LIMIT)?;
    let repeated_contents = file.read_limited(INDEX_LIMIT)?;
    if repeated_contents != contents || file.byte_len()? != initial_len {
        bail!("media-library index quarantine content changed while being verified");
    }
    file.validate_private_single_link(0o600)
        .context("media-library index quarantine access policy changed while being verified")?;
    Ok(sha256_bytes(&contents))
}

fn validate_index_contents(file: &BoundFile, expected_digest: &str) -> Result<()> {
    if bounded_index_file_digest(file)? != expected_digest {
        bail!("media-library index content changed during quarantine validation");
    }
    Ok(())
}

fn deduplicate(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

fn ids_from_path(path: &str) -> Vec<String> {
    extract_source_ids(path)
}

fn normalize_source_ids(ids: Vec<String>) -> Vec<String> {
    let mut normalized = BTreeSet::new();
    for raw in ids {
        let value = raw.trim().trim_matches(|character: char| {
            !character.is_ascii_alphanumeric() && character != '_' && character != '-'
        });
        if value.is_empty() || value.len() > 128 {
            continue;
        }
        let lower = value.to_ascii_lowercase();
        let canonical = if value.len() >= 8
            && value
                .get(..2)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("BV"))
            && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            format!("BV{}", &value[2..])
        } else if ["av", "cid", "ep"].iter().any(|prefix| {
            lower.strip_prefix(prefix).is_some_and(|digits| {
                !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
            })
        }) {
            lower
        } else if let Some((prefix, youtube_id)) = value.split_once(':')
            && (prefix.eq_ignore_ascii_case("youtube") || prefix.eq_ignore_ascii_case("yt"))
        {
            if is_youtube_video_id(youtube_id) {
                format!("youtube:{youtube_id}")
            } else {
                continue;
            }
        } else if is_youtube_video_id(value) {
            format!("youtube:{value}")
        } else {
            continue;
        };
        normalized.insert(canonical);
    }
    normalized.into_iter().collect()
}

fn is_youtube_video_id(value: &str) -> bool {
    value.len() == 11
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn nfo_source_ids(content: &str) -> Vec<String> {
    let mut cursor = 0;
    let mut ids = Vec::new();
    while let Some(element) = xml_element_span(content, "uniqueid", cursor) {
        let tag = &content[element.open_start..element.open_end];
        let kind = xml_attribute(tag, "type")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let raw_id = xml_text_content(&content[element.open_end..element.close_start])
            .trim()
            .to_string();
        let canonical = match kind.as_str() {
            "bilibili" | "bilibili-video" => raw_id,
            "bilibili-aid" | "bilibili_aid" => {
                if raw_id.to_ascii_lowercase().starts_with("av") {
                    raw_id
                } else {
                    format!("av{raw_id}")
                }
            }
            "bilibili-cid" | "bilibili_cid" => {
                if raw_id.to_ascii_lowercase().starts_with("cid") {
                    raw_id
                } else {
                    format!("cid{raw_id}")
                }
            }
            "bilibili-epid" | "bilibili_epid" => {
                if raw_id.to_ascii_lowercase().starts_with("ep") {
                    raw_id
                } else {
                    format!("ep{raw_id}")
                }
            }
            "youtube" | "youtube-video" | "youtube_video" => {
                format!("youtube:{raw_id}")
            }
            _ => {
                cursor = element.close_end;
                continue;
            }
        };
        ids.extend(normalize_source_ids(vec![canonical]));
        cursor = element.close_end;
    }
    deduplicate(ids)
}

fn nfo_organization_status(content: &str) -> Option<OrganizationStatus> {
    let element = xml_element_span(content, "tvd_organization", 0)?;
    if xml_element_span(content, "tvd_organization", element.close_end).is_some() {
        return None;
    }
    match xml_text_content(&content[element.open_end..element.close_start])
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "standalone" => Some(OrganizationStatus::Standalone),
        "unreviewed" => Some(OrganizationStatus::Unreviewed),
        _ => None,
    }
}

fn nfo_has_collection_metadata(content: &str) -> bool {
    ["set", "setid", "settype", "episode", "partid"]
        .iter()
        .any(|tag| xml_tag_text(content, tag).is_some_and(|value| !value.trim().is_empty()))
}

fn metadata_subfield_is_missing(content: &str, tag: &str, fill_missing_only: bool) -> bool {
    !fill_missing_only || xml_tag_text(content, tag).is_none_or(|value| value.trim().is_empty())
}

fn fill_only_collection_patch_preserves_existing(
    current: &CollectionMetadata,
    requested: &CollectionMetadata,
) -> bool {
    if !collection_identity_matches(current, requested) {
        return false;
    }
    let string_preserved = |current: Option<&str>, requested: Option<&str>| {
        current
            .is_none_or(|value| requested.is_some_and(|candidate| value.trim() == candidate.trim()))
    };
    string_preserved(
        current
            .id
            .as_deref()
            .filter(|value| !value.trim().is_empty()),
        requested
            .id
            .as_deref()
            .filter(|value| !value.trim().is_empty()),
    ) && (current.title.trim().is_empty() || current.title.trim() == requested.title.trim())
        && ((current.kind.trim().is_empty() || current.kind.eq_ignore_ascii_case("nfo_set"))
            || current.kind.eq_ignore_ascii_case(&requested.kind))
        && current
            .order
            .is_none_or(|order| requested.order == Some(order))
        && string_preserved(
            current
                .part_id
                .as_deref()
                .filter(|value| !value.trim().is_empty()),
            requested
                .part_id
                .as_deref()
                .filter(|value| !value.trim().is_empty()),
        )
}

fn collection_identity_matches(
    current: &CollectionMetadata,
    requested: &CollectionMetadata,
) -> bool {
    let current_id = current
        .id
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    let requested_id = requested
        .id
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    if let (Some(current_id), Some(requested_id)) = (current_id, requested_id) {
        return current_id.trim() == requested_id.trim();
    }
    canonical_collection_title(&current.title) == canonical_collection_title(&requested.title)
        && collection_kind_compatible(&current.kind, &requested.kind)
}

fn canonical_collection_title(title: &str) -> String {
    title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn collection_kind_compatible(current: &str, requested: &str) -> bool {
    fn specific(kind: &str) -> Option<&str> {
        let kind = kind.trim();
        (!kind.is_empty() && !kind.eq_ignore_ascii_case("nfo_set")).then_some(kind)
    }
    match (specific(current), specific(requested)) {
        (Some(current), Some(requested)) => current.eq_ignore_ascii_case(requested),
        _ => true,
    }
}

fn nfo_collection_matches_patch(content: &str, collection: &CollectionMetadata) -> bool {
    let existing_id = xml_tag_text(content, "setid").filter(|value| !value.trim().is_empty());
    let requested_id = collection
        .id
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    if let (Some(existing_id), Some(requested_id)) = (existing_id.as_deref(), requested_id) {
        if existing_id.trim() == requested_id.trim() {
            return true;
        }
        return false;
    }

    let existing_title = nfo_collection_title(content).filter(|value| !value.trim().is_empty());
    let requested_title =
        (!collection.title.trim().is_empty()).then_some(collection.title.as_str());
    let titles_match = matches!(
        (existing_title.as_deref(), requested_title),
        (Some(existing), Some(requested)) if existing.trim() == requested.trim()
    );
    if !titles_match {
        return false;
    }

    let existing_kind = xml_tag_text(content, "settype").filter(|value| !value.trim().is_empty());
    let requested_kind = (!collection.kind.trim().is_empty()
        && !collection.kind.eq_ignore_ascii_case("nfo_set"))
    .then_some(collection.kind.as_str());
    match (existing_kind.as_deref(), requested_kind) {
        (Some(existing), Some(requested)) => existing.eq_ignore_ascii_case(requested),
        _ => true,
    }
}

fn xml_tag_text(content: &str, name: &str) -> Option<String> {
    let element = xml_element_span(content, name, 0)?;
    Some(
        xml_text_content(&content[element.open_end..element.close_start])
            .trim()
            .to_string(),
    )
}

fn nfo_collection_title(content: &str) -> Option<String> {
    let set = xml_element_span(content, "set", 0)?;
    if set.self_closing {
        return None;
    }
    let children = direct_xml_child_summary(content, set).ok()?;
    if let Some(name) = children.matching {
        if children.duplicate_matching || direct_xml_child_summary(content, name).ok()?.count > 0 {
            return None;
        }
        return Some(
            xml_text_content(&content[name.open_end..name.close_start])
                .trim()
                .to_string(),
        );
    }
    if children.count != 0 {
        return None;
    }
    Some(
        xml_text_content(&content[set.open_end..set.close_start])
            .trim()
            .to_string(),
    )
}

struct DirectXmlChildSummary {
    count: usize,
    matching: Option<XmlElementSpan>,
    duplicate_matching: bool,
}

fn direct_xml_child_summary(
    content: &str,
    parent: XmlElementSpan,
) -> Result<DirectXmlChildSummary> {
    let mut summary = DirectXmlChildSummary {
        count: 0,
        matching: None,
        duplicate_matching: false,
    };
    if parent.self_closing {
        return Ok(summary);
    }

    let mut open_names = Vec::new();
    let mut cursor = parent.open_end;
    while let Some(markup) = xml_next_markup(content, cursor) {
        if markup.start >= parent.close_start {
            break;
        }
        if markup.end > parent.close_start {
            bail!("NFO child markup crosses its parent closing tag");
        }
        cursor = markup.end;
        match markup.kind {
            XmlMarkupKind::Start => {
                if open_names.is_empty() {
                    summary.count = summary.count.saturating_add(1);
                    if markup.name.eq_ignore_ascii_case("name") {
                        if summary.matching.is_some() {
                            summary.duplicate_matching = true;
                        } else {
                            let child = xml_element_span(content, "name", markup.start)
                                .filter(|span| span.open_start == markup.start)
                                .ok_or_else(|| {
                                    anyhow!("NFO collection name has no matching close")
                                })?;
                            if child.close_end > parent.close_start {
                                bail!("NFO collection name crosses its parent closing tag");
                            }
                            summary.matching = Some(child);
                        }
                    }
                }
                if !markup.self_closing {
                    open_names.push(markup.name);
                }
            }
            XmlMarkupKind::End => {
                let Some(open_name) = open_names.pop() else {
                    bail!("NFO collection contains an unmatched closing tag");
                };
                if !open_name.eq_ignore_ascii_case(markup.name) {
                    bail!("NFO collection contains mismatched nested tags");
                }
            }
            XmlMarkupKind::Opaque => {
                bail!("NFO collection contains unreadable markup");
            }
            _ => {}
        }
    }
    if !open_names.is_empty() {
        bail!("NFO collection contains an unclosed child tag");
    }
    Ok(summary)
}

fn xml_has_inner_markup(content: &str, element: XmlElementSpan) -> bool {
    xml_next_markup(content, element.open_end)
        .is_some_and(|markup| markup.start < element.close_start)
}

fn xml_attribute(tag: &str, attribute: &str) -> Option<String> {
    let markup = xml_next_markup(tag, 0)?;
    if markup.kind != XmlMarkupKind::Start {
        return None;
    }

    let bytes = tag.as_bytes();
    let mut cursor = markup.name_end;
    let tag_end = markup.end.checked_sub(1)?;
    while cursor < tag_end {
        while cursor < tag_end && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= tag_end || bytes[cursor] == b'/' {
            break;
        }

        let name_start = cursor;
        while cursor < tag_end
            && !bytes[cursor].is_ascii_whitespace()
            && !matches!(bytes[cursor], b'=' | b'/' | b'>')
        {
            cursor += 1;
        }
        if name_start == cursor {
            return None;
        }
        let name = &tag[name_start..cursor];

        while cursor < tag_end && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= tag_end || bytes[cursor] != b'=' {
            while cursor < tag_end && !bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            continue;
        }
        cursor += 1;
        while cursor < tag_end && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        let quote = *bytes.get(cursor)?;
        if quote != b'\'' && quote != b'"' {
            return None;
        }
        cursor += 1;
        let value_start = cursor;
        while cursor < tag_end && bytes[cursor] != quote {
            cursor += 1;
        }
        if cursor >= tag_end {
            return None;
        }
        if name.eq_ignore_ascii_case(attribute) {
            return Some(tag[value_start..cursor].to_string());
        }
        cursor += 1;
    }

    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum XmlMarkupKind {
    Start,
    End,
    Comment,
    CData,
    ProcessingInstruction,
    Declaration,
    Opaque,
}

#[derive(Clone, Copy, Debug)]
struct XmlMarkup<'a> {
    start: usize,
    end: usize,
    kind: XmlMarkupKind,
    name: &'a str,
    name_end: usize,
    self_closing: bool,
    content_start: usize,
    content_end: usize,
}

#[derive(Clone, Copy, Debug)]
struct XmlElementSpan {
    open_start: usize,
    open_end: usize,
    close_start: usize,
    close_end: usize,
    self_closing: bool,
}

fn xml_next_markup(content: &str, from: usize) -> Option<XmlMarkup<'_>> {
    let bytes = content.as_bytes();
    let mut cursor = from;

    while cursor < bytes.len() {
        let relative = content.get(cursor..)?.find('<')?;
        let start = cursor + relative;
        let remainder = &content[start..];

        if remainder.starts_with("<!--") {
            let body_start = start + 4;
            let Some(offset) = content[body_start..].find("-->") else {
                return Some(XmlMarkup {
                    start,
                    end: bytes.len(),
                    kind: XmlMarkupKind::Comment,
                    name: "",
                    name_end: bytes.len(),
                    self_closing: false,
                    content_start: body_start,
                    content_end: bytes.len(),
                });
            };
            let end = body_start + offset + 3;
            return Some(XmlMarkup {
                start,
                end,
                kind: XmlMarkupKind::Comment,
                name: "",
                name_end: end,
                self_closing: false,
                content_start: body_start,
                content_end: end - 3,
            });
        }

        if remainder.starts_with("<![CDATA[") {
            let body_start = start + 9;
            let Some(offset) = content[body_start..].find("]]>") else {
                return Some(XmlMarkup {
                    start,
                    end: bytes.len(),
                    kind: XmlMarkupKind::CData,
                    name: "",
                    name_end: bytes.len(),
                    self_closing: false,
                    content_start: body_start,
                    content_end: bytes.len(),
                });
            };
            let content_end = body_start + offset;
            let end = content_end + 3;
            return Some(XmlMarkup {
                start,
                end,
                kind: XmlMarkupKind::CData,
                name: "",
                name_end: end,
                self_closing: false,
                content_start: body_start,
                content_end,
            });
        }

        if remainder.starts_with("<?") {
            let body_start = start + 2;
            let end = content[body_start..]
                .find("?>")
                .map(|offset| body_start + offset + 2)
                .unwrap_or(bytes.len());
            return Some(XmlMarkup {
                start,
                end,
                kind: XmlMarkupKind::ProcessingInstruction,
                name: "",
                name_end: end,
                self_closing: false,
                content_start: body_start,
                content_end: end.saturating_sub(2),
            });
        }

        if remainder.starts_with("<!") {
            let body_start = start + 2;
            let end = xml_declaration_end(content, body_start).unwrap_or(bytes.len());
            return Some(XmlMarkup {
                start,
                end,
                kind: XmlMarkupKind::Declaration,
                name: "",
                name_end: end,
                self_closing: false,
                content_start: body_start,
                content_end: end.saturating_sub(1),
            });
        }

        let (kind, name_start) = if remainder.starts_with("</") {
            (XmlMarkupKind::End, start + 2)
        } else {
            (XmlMarkupKind::Start, start + 1)
        };
        let mut name_end = name_start;
        while name_end < bytes.len()
            && !bytes[name_end].is_ascii_whitespace()
            && !matches!(bytes[name_end], b'/' | b'>')
        {
            name_end += 1;
        }
        if name_end == name_start || name_end == bytes.len() {
            cursor = start + 1;
            continue;
        }

        let mut quote = None;
        let mut end = name_end;
        let mut terminated = false;
        while end < bytes.len() {
            let byte = bytes[end];
            match quote {
                Some(active) if byte == active => quote = None,
                Some(_) => {}
                None if byte == b'\'' || byte == b'"' => quote = Some(byte),
                None if byte == b'>' => {
                    end += 1;
                    terminated = true;
                    break;
                }
                None => {}
            }
            end += 1;
        }
        if !terminated || quote.is_some() {
            return Some(XmlMarkup {
                start,
                end: bytes.len(),
                kind: XmlMarkupKind::Opaque,
                name: "",
                name_end: bytes.len(),
                self_closing: false,
                content_start: start,
                content_end: bytes.len(),
            });
        }

        let self_closing = kind == XmlMarkupKind::Start
            && self_closing_xml_tag_slash(content, start, end - 1).is_some();
        return Some(XmlMarkup {
            start,
            end,
            kind,
            name: &content[name_start..name_end],
            name_end,
            self_closing,
            content_start: name_end,
            content_end: end - 1,
        });
    }

    None
}

fn xml_declaration_end(content: &str, from: usize) -> Option<usize> {
    let bytes = content.as_bytes();
    let mut cursor = from;
    let mut quote = None;
    let mut subset_depth = 0usize;

    while cursor < bytes.len() {
        if quote.is_none() && bytes[cursor..].starts_with(b"<!--") {
            cursor = bytes[cursor + 4..]
                .windows(3)
                .position(|window| window == b"-->")
                .map(|offset| cursor + 4 + offset + 3)?;
            continue;
        }
        let byte = bytes[cursor];
        match quote {
            Some(active) if byte == active => quote = None,
            Some(_) => {}
            None if byte == b'\'' || byte == b'"' => quote = Some(byte),
            None if byte == b'[' => subset_depth += 1,
            None if byte == b']' => subset_depth = subset_depth.saturating_sub(1),
            None if byte == b'>' && subset_depth == 0 => return Some(cursor + 1),
            None => {}
        }
        cursor += 1;
    }

    None
}

fn xml_element_span(content: &str, name: &str, from: usize) -> Option<XmlElementSpan> {
    let mut cursor = from;
    while let Some(open) = xml_next_markup(content, cursor) {
        cursor = open.end;
        if open.kind != XmlMarkupKind::Start || !open.name.eq_ignore_ascii_case(name) {
            continue;
        }
        if open.self_closing {
            return Some(XmlElementSpan {
                open_start: open.start,
                open_end: open.end,
                close_start: open.end,
                close_end: open.end,
                self_closing: true,
            });
        }

        let mut depth = 1usize;
        let mut nested_cursor = open.end;
        while let Some(markup) = xml_next_markup(content, nested_cursor) {
            nested_cursor = markup.end;
            if !markup.name.eq_ignore_ascii_case(name) {
                continue;
            }
            match markup.kind {
                XmlMarkupKind::Start if !markup.self_closing => depth += 1,
                XmlMarkupKind::End => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(XmlElementSpan {
                            open_start: open.start,
                            open_end: open.end,
                            close_start: markup.start,
                            close_end: markup.end,
                            self_closing: false,
                        });
                    }
                }
                _ => {}
            }
        }
        return None;
    }
    None
}

fn xml_text_content(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut cursor = 0;
    while let Some(markup) = xml_next_markup(value, cursor) {
        result.push_str(&xml_unescape(&value[cursor..markup.start]));
        if markup.kind == XmlMarkupKind::CData {
            result.push_str(&value[markup.content_start..markup.content_end]);
        }
        cursor = markup.end;
    }
    result.push_str(&xml_unescape(&value[cursor..]));
    result
}

fn xml_unescape(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut cursor = 0;

    while let Some(relative_ampersand) = value[cursor..].find('&') {
        let ampersand = cursor + relative_ampersand;
        result.push_str(&value[cursor..ampersand]);

        let mut end = ampersand + 1;
        let mut terminated = false;
        while end < value.len() {
            match value.as_bytes()[end] {
                b'&' => break,
                b';' => {
                    end += 1;
                    terminated = true;
                    break;
                }
                _ => {
                    let character = value[end..]
                        .chars()
                        .next()
                        .expect("end is a character boundary before the string end");
                    end += character.len_utf8();
                }
            }
        }

        let reference = &value[ampersand..end];
        if terminated {
            if let Some(character) = decode_xml_reference(reference) {
                result.push(character);
            } else {
                result.push_str(reference);
            }
        } else {
            result.push_str(reference);
        }
        cursor = end;
    }

    result.push_str(&value[cursor..]);
    result
}

fn decode_xml_reference(reference: &str) -> Option<char> {
    match reference {
        "&lt;" => return Some('<'),
        "&gt;" => return Some('>'),
        "&quot;" => return Some('"'),
        "&apos;" => return Some('\''),
        "&amp;" => return Some('&'),
        _ => {}
    }

    let body = reference.strip_prefix("&#")?.strip_suffix(';')?;
    let (digits, radix) = if let Some(hex) = body.strip_prefix('x') {
        (hex, 16)
    } else {
        (body, 10)
    };
    if digits.is_empty()
        || !digits.bytes().all(|byte| match radix {
            10 => byte.is_ascii_digit(),
            16 => byte.is_ascii_hexdigit(),
            _ => false,
        })
    {
        return None;
    }

    let codepoint = u32::from_str_radix(digits, radix).ok()?;
    if !is_xml_10_character(codepoint) {
        return None;
    }
    char::from_u32(codepoint)
}

fn is_xml_10_character(codepoint: u32) -> bool {
    matches!(
        codepoint,
        0x9 | 0xA | 0xD | 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x10000..=0x10FFFF
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn self_closing_xml_tag_slash(content: &str, start: usize, open_end: usize) -> Option<usize> {
    let bytes = content.as_bytes();
    let mut last_non_whitespace = open_end;
    while last_non_whitespace > start && bytes[last_non_whitespace - 1].is_ascii_whitespace() {
        last_non_whitespace -= 1;
    }
    (last_non_whitespace > start && bytes[last_non_whitespace - 1] == b'/')
        .then_some(last_non_whitespace - 1)
}

fn metadata_field_changes(before: Option<&str>, after: &str) -> Vec<MetadataFieldChange> {
    [
        ("title", "title"),
        ("collection", "set"),
        ("collection_id", "setid"),
        ("collection_kind", "settype"),
        ("order", "episode"),
        ("part_id", "partid"),
        ("organization_status", "tvd_organization"),
    ]
    .into_iter()
    .filter_map(|(field, tag)| {
        let read_value = |xml: &str| {
            if field == "collection" {
                nfo_collection_title(xml)
            } else {
                xml_tag_text(xml, tag)
            }
        };
        let after = read_value(after)?;
        let before = before.and_then(read_value);
        (before.as_deref() != Some(after.as_str())).then_some(MetadataFieldChange {
            field: field.to_string(),
            before,
            after,
        })
    })
    .collect()
}

fn append_metadata_patch(content: &str, patch: &MetadataPatch) -> Result<String> {
    let mut output = content.to_string();
    let mut nodes = Vec::new();
    if let Some(title) = patch
        .title
        .as_deref()
        .filter(|title| !title.trim().is_empty())
    {
        let existing_title = xml_tag_text(&output, "title");
        if patch.fill_missing_only
            && existing_title
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
        {
            // Keep a saved NFO title, including a user-edited title, byte-for-byte intact.
        } else if existing_title.is_some() {
            output = replace_xml_element(&output, "title", title.trim())?;
        } else {
            nodes.push(format!("  <title>{}</title>", xml_escape(title.trim())));
        }
    }
    let existing_ids = nfo_source_ids(&output).into_iter().collect::<HashSet<_>>();
    for id in normalize_source_ids(patch.source_ids.clone()) {
        if existing_ids.contains(&id) {
            continue;
        }
        if let Some((kind, value)) = source_id_nfo_value(&id) {
            nodes.push(format!(
                "  <uniqueid type=\"{kind}\">{}</uniqueid>",
                xml_escape(&value)
            ));
        }
    }
    if let Some(collection) = patch.collection.as_ref()
        && (!patch.fill_missing_only
            || !nfo_has_collection_metadata(&output)
            || nfo_collection_matches_patch(&output, collection))
    {
        if !collection.title.trim().is_empty()
            && (!patch.fill_missing_only
                || nfo_collection_title(&output).is_none_or(|value| value.trim().is_empty()))
        {
            if xml_element_span(&output, "set", 0).is_some() {
                if !patch.fill_missing_only || can_fill_nfo_collection_title(&output)? {
                    output = replace_nfo_collection_title(&output, collection.title.trim())?;
                }
            } else {
                nodes.push(format!(
                    "  <set>{}</set>",
                    xml_escape(collection.title.trim())
                ));
            }
        }
        if let Some(order) = collection.order
            && metadata_subfield_is_missing(&output, "episode", patch.fill_missing_only)
        {
            if xml_tag_text(&output, "episode").is_some() {
                output = replace_xml_element(&output, "episode", &order.to_string())?;
            } else {
                nodes.push(format!("  <episode>{order}</episode>"));
            }
        }
        for (tag, value) in [
            ("setid", collection.id.as_deref()),
            ("settype", Some(collection.kind.as_str())),
        ] {
            if let Some(value) = value.filter(|value| !value.trim().is_empty())
                && metadata_subfield_is_missing(&output, tag, patch.fill_missing_only)
            {
                if xml_tag_text(&output, tag).is_some() {
                    output = replace_xml_element(&output, tag, value.trim())?;
                } else {
                    nodes.push(format!("  <{tag}>{}</{tag}>", xml_escape(value.trim())));
                }
            }
        }
        if let Some(part_id) = collection
            .part_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            && metadata_subfield_is_missing(&output, "partid", patch.fill_missing_only)
        {
            if xml_tag_text(&output, "partid").is_some() {
                output = replace_xml_element(&output, "partid", part_id.trim())?;
            } else {
                nodes.push(format!("  <partid>{}</partid>", xml_escape(part_id.trim())));
            }
        }
    }
    if let Some(status) = patch.organization_status {
        let existing = xml_element_span(&output, "tvd_organization", 0);
        if let Some(element) = existing {
            if xml_element_span(&output, "tvd_organization", element.close_end).is_some() {
                bail!("NFO has multiple organization status fields; refusing an ambiguous patch");
            }
            let existing_text = xml_text_content(&output[element.open_end..element.close_start]);
            if !patch.fill_missing_only || existing_text.trim().is_empty() {
                let value = organization_status_nfo_value(status);
                output = replace_xml_element(&output, "tvd_organization", value)?;
            }
        } else {
            nodes.push(format!(
                "  <tvd_organization>{}</tvd_organization>",
                organization_status_nfo_value(status)
            ));
        }
    }
    if nodes.is_empty() {
        return Ok(output);
    }
    let close = root_close_start(&output).ok_or_else(|| {
        anyhow!("NFO root closing tag could not be identified; leaving it unchanged")
    })?;
    let mut inserted =
        String::with_capacity(output.len() + nodes.iter().map(String::len).sum::<usize>() + 2);
    inserted.push_str(&output[..close]);
    if !inserted.ends_with('\n') {
        inserted.push('\n');
    }
    for node in nodes {
        inserted.push_str(&node);
        inserted.push('\n');
    }
    inserted.push_str(&output[close..]);
    Ok(inserted)
}

fn replace_nfo_collection_title(content: &str, value: &str) -> Result<String> {
    let set = xml_element_span(content, "set", 0)
        .ok_or_else(|| anyhow!("NFO collection element disappeared"))?;
    if set.self_closing {
        return replace_xml_element_span(content, set, value);
    }

    let children = direct_xml_child_summary(content, set)?;
    if children.duplicate_matching {
        bail!("NFO collection has multiple direct <name> elements; refusing ambiguous patch");
    }
    if let Some(name) = children.matching {
        if direct_xml_child_summary(content, name)?.count > 0 || xml_has_inner_markup(content, name)
        {
            bail!("NFO collection name is complex; refusing to discard nested markup");
        }
        return replace_xml_element_span(content, name, value);
    }
    if children.count == 0 && !xml_has_inner_markup(content, set) {
        return replace_xml_element_span(content, set, value);
    }
    bail!("NFO collection has complex children but no unique <name>; refusing to discard them")
}

fn can_fill_nfo_collection_title(content: &str) -> Result<bool> {
    let Some(set) = xml_element_span(content, "set", 0) else {
        return Ok(true);
    };
    if set.self_closing {
        return Ok(true);
    }
    let children = direct_xml_child_summary(content, set)?;
    if children.duplicate_matching {
        bail!("NFO collection has multiple direct <name> elements; refusing ambiguous patch");
    }
    if let Some(name) = children.matching {
        return Ok(direct_xml_child_summary(content, name)?.count == 0
            && !xml_has_inner_markup(content, name));
    }
    Ok(children.count == 0 && !xml_has_inner_markup(content, set))
}

fn replace_xml_element(content: &str, name: &str, value: &str) -> Result<String> {
    let element = xml_element_span(content, name, 0)
        .ok_or_else(|| anyhow!("NFO element disappeared or has no matching close"))?;
    replace_xml_element_span(content, element, value)
}

fn replace_xml_element_span(content: &str, element: XmlElementSpan, value: &str) -> Result<String> {
    if element.self_closing {
        let slash = self_closing_xml_tag_slash(content, element.open_start, element.open_end - 1)
            .ok_or_else(|| anyhow!("NFO self-closing element has no closing slash"))?;
        let opening = xml_next_markup(content, element.open_start)
            .filter(|markup| {
                markup.start == element.open_start && markup.kind == XmlMarkupKind::Start
            })
            .ok_or_else(|| anyhow!("NFO self-closing element opening tag could not be reread"))?;
        let opening_name = opening.name;
        let mut output =
            String::with_capacity(content.len() + value.len() + opening_name.len() + 3);
        output.push_str(&content[..slash]);
        output.push_str(&content[slash + 1..element.open_end]);
        output.push_str(&xml_escape(value));
        output.push_str("</");
        output.push_str(opening_name);
        output.push('>');
        output.push_str(&content[element.open_end..]);
        return Ok(output);
    }
    let mut output = String::with_capacity(content.len() + value.len());
    output.push_str(&content[..element.open_end]);
    output.push_str(&xml_escape(value));
    output.push_str(&content[element.close_start..element.close_end]);
    output.push_str(&content[element.close_end..]);
    Ok(output)
}

fn new_nfo(fallback_title: &str, patch: &MetadataPatch) -> String {
    let mut content =
        String::from("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<movie>\n");
    let title = patch
        .title
        .as_deref()
        .or_else(|| (!patch.fill_missing_only).then_some(fallback_title))
        .unwrap_or_default();
    if !title.trim().is_empty() {
        content.push_str(&format!("  <title>{}</title>\n", xml_escape(title.trim())));
    }
    for id in normalize_source_ids(patch.source_ids.clone()) {
        if let Some((kind, value)) = source_id_nfo_value(&id) {
            content.push_str(&format!(
                "  <uniqueid type=\"{kind}\">{}</uniqueid>\n",
                xml_escape(&value)
            ));
        }
    }
    if let Some(status) = patch.organization_status {
        content.push_str(&format!(
            "  <tvd_organization>{}</tvd_organization>\n",
            organization_status_nfo_value(status)
        ));
    }
    if let Some(collection) = &patch.collection {
        if !collection.title.trim().is_empty() {
            content.push_str(&format!(
                "  <set>{}</set>\n",
                xml_escape(collection.title.trim())
            ));
        }
        if let Some(order) = collection.order {
            content.push_str(&format!("  <episode>{order}</episode>\n"));
        }
        if let Some(id) = collection
            .id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            content.push_str(&format!("  <setid>{}</setid>\n", xml_escape(id.trim())));
        }
        if !collection.kind.trim().is_empty() {
            content.push_str(&format!(
                "  <settype>{}</settype>\n",
                xml_escape(collection.kind.trim())
            ));
        }
        if let Some(part_id) = collection
            .part_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            content.push_str(&format!(
                "  <partid>{}</partid>\n",
                xml_escape(part_id.trim())
            ));
        }
    }
    content.push_str("</movie>\n");
    content
}

fn root_close_start(content: &str) -> Option<usize> {
    let mut cursor = 0;
    let root = loop {
        let markup = xml_next_markup(content, cursor)?;
        cursor = markup.end;
        if markup.kind == XmlMarkupKind::Start {
            break markup;
        }
    };
    if root.self_closing {
        return None;
    }

    let mut depth = 1usize;
    while let Some(markup) = xml_next_markup(content, cursor) {
        cursor = markup.end;
        if !markup.name.eq_ignore_ascii_case(root.name) {
            continue;
        }
        match markup.kind {
            XmlMarkupKind::Start if !markup.self_closing => depth += 1,
            XmlMarkupKind::End => {
                depth -= 1;
                if depth == 0 {
                    return xml_trailing_misc_only(content, markup.end).then_some(markup.start);
                }
            }
            _ => {}
        }
    }
    None
}

fn xml_trailing_misc_only(content: &str, mut cursor: usize) -> bool {
    while let Some(markup) = xml_next_markup(content, cursor) {
        if !content[cursor..markup.start].trim().is_empty() {
            return false;
        }
        if !matches!(
            markup.kind,
            XmlMarkupKind::Comment | XmlMarkupKind::ProcessingInstruction
        ) {
            return false;
        }
        cursor = markup.end;
    }
    content[cursor..].trim().is_empty()
}

fn source_id_nfo_value(id: &str) -> Option<(&'static str, String)> {
    if let Some(value) = id.strip_prefix("youtube:") {
        return is_youtube_video_id(value).then(|| ("youtube", value.to_string()));
    }
    if id.starts_with("BV") {
        return Some(("bilibili", id.to_string()));
    }
    if id.starts_with("av") {
        return Some(("bilibili-aid", id.to_string()));
    }
    if id.starts_with("cid") {
        return Some(("bilibili-cid", id.to_string()));
    }
    if id.starts_with("ep") {
        return Some(("bilibili-epid", id.to_string()));
    }
    None
}

fn organization_status_nfo_value(status: OrganizationStatus) -> &'static str {
    match status {
        OrganizationStatus::Standalone => "standalone",
        OrganizationStatus::Unreviewed => "unreviewed",
    }
}

fn missing_patch_source_ids(existing: Vec<String>, requested: &[String]) -> Vec<String> {
    let existing = existing.into_iter().collect::<HashSet<_>>();
    normalize_source_ids(requested.to_vec())
        .into_iter()
        .filter(|id| !existing.contains(id))
        .collect()
}

fn validate_metadata_patch(patch: &MetadataPatch) -> Result<()> {
    if patch.organization_status == Some(OrganizationStatus::Standalone)
        && patch.collection.is_some()
    {
        bail!("a standalone organization patch cannot include collection metadata");
    }
    if patch
        .title
        .as_deref()
        .is_some_and(|title| !metadata_title_within_limits(title))
    {
        bail!("metadata title exceeds its size limit");
    }
    if patch.source_ids.len() > 64
        || normalize_source_ids(patch.source_ids.clone()).len() != patch.source_ids.len()
    {
        bail!("metadata source IDs are invalid or duplicated");
    }
    if let Some(collection) = &patch.collection {
        if collection.title.trim().is_empty() || !metadata_title_within_limits(&collection.title) {
            bail!("collection title is empty or too long");
        }
        if collection.id.as_deref().is_some_and(|id| id.len() > 256)
            || collection
                .part_id
                .as_deref()
                .is_some_and(|id| id.len() > 256)
        {
            bail!("collection identifiers exceed their size limit");
        }
    }
    if patch
        .title
        .as_deref()
        .is_none_or(|title| title.trim().is_empty())
        && patch.source_ids.is_empty()
        && patch.collection.is_none()
        && patch.organization_status.is_none()
    {
        bail!("metadata patch has no fields to apply");
    }
    Ok(())
}

pub(crate) fn metadata_title_within_limits(title: &str) -> bool {
    title.len() <= MAX_METADATA_TITLE_BYTES
        && title.trim().chars().count() <= MAX_METADATA_TITLE_CHARS
        && !title.chars().any(char::is_control)
}

fn metadata_from_json(
    value: &serde_json::Value,
) -> (Option<String>, Vec<String>, Option<CollectionMetadata>) {
    let mut title = None;
    let mut ids = Vec::new();
    let mut collection = None;
    for object in json_media_objects(value) {
        collect_metadata_json_object(object, &mut title, &mut ids, &mut collection);
    }
    (title, normalize_source_ids(ids), collection)
}

fn json_media_objects(
    value: &serde_json::Value,
) -> Vec<&serde_json::Map<String, serde_json::Value>> {
    let Some(root) = value.as_object() else {
        return Vec::new();
    };
    if is_non_media_json_container(root) {
        return Vec::new();
    }
    if let Some(video_details) = root
        .get("videoDetails")
        .and_then(serde_json::Value::as_object)
        .filter(|object| !is_non_media_json_container(object))
    {
        return vec![video_details];
    }
    if let Some(view) = root
        .get("View")
        .and_then(serde_json::Value::as_object)
        .filter(|object| !is_non_media_json_container(object))
    {
        return vec![view];
    }
    if let Some(data) = root.get("data").and_then(serde_json::Value::as_object) {
        if is_non_media_json_container(data) {
            return Vec::new();
        }
        if let Some(view) = data
            .get("View")
            .and_then(serde_json::Value::as_object)
            .filter(|object| !is_non_media_json_container(object))
        {
            return vec![view];
        }
        if let Some(video_details) = data
            .get("videoDetails")
            .and_then(serde_json::Value::as_object)
            .filter(|object| !is_non_media_json_container(object))
        {
            return vec![video_details];
        }
    }

    if has_direct_media_fields(root) {
        return vec![root];
    }
    if root.contains_key("code")
        && let Some(data) = root.get("data").and_then(serde_json::Value::as_object)
        && !is_non_media_json_container(data)
        && has_direct_media_fields(data)
    {
        return vec![data];
    }
    Vec::new()
}

fn has_direct_media_fields(object: &serde_json::Map<String, serde_json::Value>) -> bool {
    [
        "title",
        "bvid",
        "aid",
        "avid",
        "cid",
        "epid",
        "episode_id",
        "videoId",
    ]
    .iter()
    .any(|key| object.contains_key(*key))
}

fn is_non_media_json_container(object: &serde_json::Map<String, serde_json::Value>) -> bool {
    object
        .get("_type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "playlist" | "multi_video" | "multi-video"
            )
        })
}

fn collect_metadata_json_object(
    object: &serde_json::Map<String, serde_json::Value>,
    title: &mut Option<String>,
    ids: &mut Vec<String>,
    collection: &mut Option<CollectionMetadata>,
) {
    for (key, value) in object {
        let key_lower = key.to_ascii_lowercase();
        match (key_lower.as_str(), value) {
            ("bvid", serde_json::Value::String(value)) => ids.push(value.clone()),
            ("aid" | "avid", serde_json::Value::Number(value)) => ids.push(format!("av{value}")),
            ("aid" | "avid", serde_json::Value::String(value)) => {
                ids.push(format!("av{}", value.trim_start_matches("av")))
            }
            ("cid", serde_json::Value::Number(value)) => ids.push(format!("cid{value}")),
            ("cid", serde_json::Value::String(value)) => {
                ids.push(format!("cid{}", value.trim_start_matches("cid")))
            }
            ("epid" | "episode_id", serde_json::Value::Number(value)) => {
                ids.push(format!("ep{value}"))
            }
            ("epid" | "episode_id", serde_json::Value::String(value)) => {
                ids.push(format!("ep{}", value.trim_start_matches("ep")))
            }
            ("title", serde_json::Value::String(value))
                if title.is_none() && !value.trim().is_empty() =>
            {
                *title = Some(value.trim().to_string());
            }
            ("ugc_season" | "season", serde_json::Value::Object(season))
                if collection.is_none() =>
            {
                let season_title = season
                    .get("title")
                    .or_else(|| season.get("name"))
                    .and_then(serde_json::Value::as_str);
                if let Some(season_title) = season_title.filter(|title| !title.trim().is_empty()) {
                    *collection = Some(CollectionMetadata {
                        id: season.get("id").and_then(json_scalar_string),
                        title: season_title.to_string(),
                        kind: "bilibili_season".to_string(),
                        order: None,
                        part_id: None,
                    });
                }
            }
            _ => {}
        }
    }
}

fn json_scalar_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn extract_source_ids(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut result = Vec::new();
    let mut index = 0;
    let mut next_url_scan_index = 0;
    while index < bytes.len() {
        let youtube_url_prefix_len = if index >= next_url_scan_index
            && bytes
                .get(index..index + 8)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"https://"))
        {
            Some(8)
        } else if index >= next_url_scan_index
            && bytes
                .get(index..index + 7)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"http://"))
        {
            Some(7)
        } else {
            None
        };
        if let Some(prefix_len) = youtube_url_prefix_len
            && (index == 0 || !bytes[index - 1].is_ascii_alphanumeric())
        {
            let mut end = index + prefix_len;
            while end < bytes.len() && !bytes[end].is_ascii_whitespace() {
                end += 1;
            }
            // Parse at most one apparent URL per whitespace-delimited token.
            next_url_scan_index = end;
            let candidate = text[index..end].trim_end_matches(is_legacy_url_terminal_punctuation);
            if let Some(video_id) =
                crate::youtube_video_id(candidate).filter(|video_id| is_youtube_video_id(video_id))
            {
                result.push(format!("youtube:{video_id}"));
            }
            // Continue scanning the URL body so Bilibili IDs, or other IDs after the URL,
            // retain the existing extraction behavior.
            index += prefix_len;
            continue;
        }

        let (prefix_len, is_bvid) = if bytes
            .get(index..index + 2)
            .is_some_and(|pair| pair.eq_ignore_ascii_case(b"BV"))
        {
            (2, true)
        } else if bytes
            .get(index..index + 3)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"cid"))
        {
            (3, false)
        } else if bytes
            .get(index..index + 2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"av"))
            || bytes
                .get(index..index + 2)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"ep"))
        {
            (2, false)
        } else {
            index += 1;
            continue;
        };
        if index > 0 && bytes[index - 1].is_ascii_alphanumeric() {
            index += prefix_len;
            continue;
        }
        let mut end = index + prefix_len;
        while end < bytes.len() && bytes[end].is_ascii_alphanumeric() {
            end += 1;
        }
        let token = &text[index..end];
        if (is_bvid && token.len() >= 8) || (!is_bvid && token.len() > prefix_len) {
            result.extend(normalize_source_ids(vec![token.to_string()]));
        }
        index = end.max(index + prefix_len);
    }
    deduplicate(result)
}

fn is_legacy_url_terminal_punctuation(character: char) -> bool {
    matches!(
        character,
        '.' | ','
            | '!'
            | '?'
            | ';'
            | ':'
            | ')'
            | ']'
            | '}'
            | '>'
            | '`'
            | '\''
            | '"'
            | '。'
            | '，'
            | '！'
            | '？'
            | '；'
            | '：'
            | '）'
            | '】'
            | '》'
            | '」'
            | '』'
            | '〉'
            | '〕'
            | '｝'
            | '”'
            | '’'
    )
}

fn filesystem_equivalent_path_alias(
    hint_path: &str,
    scanned_path: &str,
    directory_semantics: &HashMap<String, TargetNameSemantics>,
) -> bool {
    filesystem_equivalent_path_alias_with(
        hint_path,
        scanned_path,
        directory_semantics,
        &platform_case_fold,
    )
    .unwrap_or(false)
}

fn filesystem_equivalent_path_alias_with<F>(
    hint_path: &str,
    scanned_path: &str,
    directory_semantics: &HashMap<String, TargetNameSemantics>,
    case_fold: &F,
) -> Result<bool>
where
    F: Fn(&str) -> Result<String>,
{
    let hint_components = Path::new(hint_path)
        .components()
        .filter_map(|component| match component {
            Component::Normal(name) => name.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>();
    let scanned_components = Path::new(scanned_path)
        .components()
        .filter_map(|component| match component {
            Component::Normal(name) => name.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>();
    if hint_components.len() != scanned_components.len() {
        return Ok(false);
    }

    let mut parent = PathBuf::new();
    let mut differs_from_scanned_spelling = false;
    for (hint_component, scanned_component) in hint_components.iter().zip(scanned_components.iter())
    {
        if hint_component != scanned_component {
            let parent_key = parent
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            let Some(semantics) = directory_semantics.get(&parent_key).copied() else {
                return Ok(false);
            };
            let hint_key =
                target_component_key_with(hint_component, semantics, |value| case_fold(value))?;
            let scanned_key =
                target_component_key_with(scanned_component, semantics, |value| case_fold(value))?;
            if hint_key != scanned_key {
                return Ok(false);
            }
            differs_from_scanned_spelling = true;
        }
        parent.push(scanned_component);
    }
    Ok(differs_from_scanned_spelling)
}

fn matching_hint_indices_for_items(
    items: &[LibraryItem],
    hints: &[SourceHint],
    path_resolutions: &HashMap<String, HintPathResolution>,
    walk: &ScanWalk,
) -> Vec<Vec<usize>> {
    let mut items_without_cid_by_id: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut items_with_cid_by_id: HashMap<&str, Vec<usize>> = HashMap::new();
    for (item_index, item) in items.iter().enumerate() {
        let cids = item
            .source_ids
            .iter()
            .filter(|id| id.starts_with("cid"))
            .collect::<Vec<_>>();
        let index = if cids.is_empty() {
            &mut items_without_cid_by_id
        } else {
            &mut items_with_cid_by_id
        };
        let ids = if cids.is_empty() {
            item.source_ids.iter().collect::<Vec<_>>()
        } else {
            cids
        };
        for id in ids {
            index.entry(id.as_str()).or_default().push(item_index);
        }
    }

    let can_resolve_unique_ids =
        !walk.timed_out && !walk.incomplete_scan && walk.incomplete_directories.is_empty();
    let mut matches = vec![Vec::new(); items.len()];
    for (hint_index, hint) in hints.iter().enumerate() {
        if let Some(path) = hint.relative_path.as_deref() {
            match path_resolutions
                .get(path)
                .copied()
                .unwrap_or(HintPathResolution::Unavailable)
            {
                HintPathResolution::Item(item_index) if item_index < items.len() => {
                    matches[item_index].push(hint_index);
                    continue;
                }
                HintPathResolution::ConfirmedAbsent
                    if scan_confirms_hint_path_absent(path, walk) => {}
                HintPathResolution::Item(_)
                | HintPathResolution::PresentUnmatched
                | HintPathResolution::ConfirmedAbsent
                | HintPathResolution::Unavailable => continue,
            }
        }
        if !can_resolve_unique_ids {
            continue;
        }

        let hint_ids = normalize_source_ids(hint.source_ids.clone());
        if hint_ids.is_empty() {
            continue;
        }
        let hint_cids = hint_ids
            .iter()
            .filter(|id| id.starts_with("cid"))
            .collect::<Vec<_>>();
        let item_index = if hint_cids.is_empty() {
            unique_indexed_candidate(
                &items_without_cid_by_id,
                hint_ids.iter().map(String::as_str),
            )
        } else {
            unique_indexed_candidate(
                &items_with_cid_by_id,
                hint_cids.into_iter().map(String::as_str),
            )
        };
        if let Some(item_index) = item_index {
            matches[item_index].push(hint_index);
        }
    }
    matches
}

fn unique_indexed_candidate<'index, 'id>(
    index: &HashMap<&'index str, Vec<usize>>,
    ids: impl IntoIterator<Item = &'id str>,
) -> Option<usize> {
    let mut candidate = None;
    for id in ids {
        for item_index in index.get(id).into_iter().flatten() {
            if candidate.is_some_and(|existing| existing != *item_index) {
                return None;
            }
            candidate = Some(*item_index);
        }
    }
    candidate
}

fn scan_confirms_hint_path_absent(path: &str, walk: &ScanWalk) -> bool {
    if path.is_empty() || walk.timed_out || walk.incomplete_scan {
        return false;
    }
    let mut parent = parent_relative(path);
    loop {
        if directory_under_incomplete(parent, &walk.incomplete_directories) {
            return false;
        }
        if walk.complete_directories.contains(parent) {
            return true;
        }
        if parent.is_empty() {
            return false;
        }
        parent = parent_relative(parent);
    }
}

fn attach_matching_hints(
    items: &mut [LibraryItem],
    hints: &[SourceHint],
    matched_hint_indices: &[Vec<usize>],
    sidecar_title_paths: &HashSet<String>,
) -> Vec<QueueMetadataProvenance> {
    let mut queue_metadata = vec![QueueMetadataProvenance::default(); items.len()];
    for (item_index, item) in items.iter_mut().enumerate() {
        let provenance = &mut queue_metadata[item_index];
        for hint_index in &matched_hint_indices[item_index] {
            let hint = &hints[*hint_index];
            let hint_source_ids = normalize_source_ids(hint.source_ids.clone());
            item.source_ids.extend(hint_source_ids.iter().cloned());
            if let Some(title) = hint
                .title
                .as_deref()
                .filter(|title| !title.trim().is_empty())
            {
                if hint.origin == HintOrigin::Queue && provenance.title.is_none() {
                    provenance.title = Some(title.trim().to_string());
                }
                if !sidecar_title_paths.contains(&item.relative_path)
                    && item.title == title_from_path(&item.relative_path)
                {
                    item.title = title.trim().to_string();
                }
            }
            if hint.origin == HintOrigin::Queue {
                provenance.source_ids.extend(hint_source_ids);
                if provenance.collection.is_none() {
                    provenance.collection = hint.collection.clone();
                }
                provenance.requires_confirmation |= hint.requires_confirmation;
            }
            if item.organization_status != Some(OrganizationStatus::Standalone) {
                item.collection = item.collection.clone().or_else(|| hint.collection.clone());
            }
            if hint.requires_confirmation {
                item.confidence = Confidence::NeedsConfirmation;
            } else if item.confidence == Confidence::Low {
                item.confidence = Confidence::Medium;
            }
            if item.metadata_status != MetadataStatus::Complete {
                item.metadata_status = MetadataStatus::Partial;
            }
        }
        item.source_ids = normalize_source_ids(std::mem::take(&mut item.source_ids));
        provenance.source_ids = normalize_source_ids(std::mem::take(&mut provenance.source_ids));
    }
    queue_metadata
}

fn normalize_title(title: &str) -> String {
    title
        .chars()
        .filter(|character| {
            !character.is_whitespace()
                && !matches!(character, '-' | '_' | '.' | '[' | ']' | '(' | ')')
        })
        .flat_map(char::to_lowercase)
        .collect()
}

const DEFAULT_LEGACY_CANDIDATE_LIMITS: LegacyCandidateLimits = LegacyCandidateLimits {
    max_candidates: 10_000,
    max_serialized_array_bytes: 512 * 1024,
    max_match_visits: 500_000,
};

#[derive(Clone, Copy)]
struct LegacyCandidateLimits {
    max_candidates: usize,
    max_serialized_array_bytes: usize,
    max_match_visits: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LegacyCandidateTruncation {
    CandidateCount,
    SerializedBytes,
    MatchVisits,
}

impl LegacyCandidateTruncation {
    fn warning(self, limits: LegacyCandidateLimits) -> String {
        match self {
            Self::CandidateCount => format!(
                "Legacy candidate results were truncated at the {}-candidate limit; refine the import to see more matches.",
                limits.max_candidates
            ),
            Self::SerializedBytes => format!(
                "Legacy candidate results were truncated at the {}-byte serialized array limit; refine the import to see more matches.",
                limits.max_serialized_array_bytes
            ),
            Self::MatchVisits => format!(
                "Legacy candidate matching was truncated at the {} lookup/match-visit limit; refine the import to see more matches.",
                limits.max_match_visits
            ),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default, PartialEq, Eq)]
struct LegacyCandidateWork {
    present_items_indexed: usize,
    item_title_normalizations: usize,
    source_id_index_entries: usize,
    source_id_lookups: usize,
    title_index_lookups: usize,
    match_entries_visited: usize,
    hint_title_normalizations: usize,
    hint_source_id_values_normalized: usize,
}

struct LegacyCandidateMatches {
    candidates: Vec<LegacyCandidate>,
    truncation: Option<LegacyCandidateTruncation>,
    #[cfg(test)]
    work: LegacyCandidateWork,
}

fn populate_legacy_candidates(
    report: &mut LegacyImportReport,
    items: &[LibraryItem],
    limits: LegacyCandidateLimits,
) -> Result<()> {
    let matches = legacy_candidates(&report.hints, items, limits)?;
    report.candidates = matches.candidates;
    if let Some(truncation) = matches.truncation {
        report.warnings.push(truncation.warning(limits));
    }
    Ok(())
}

fn legacy_candidates(
    hints: &[SourceHint],
    items: &[LibraryItem],
    limits: LegacyCandidateLimits,
) -> Result<LegacyCandidateMatches> {
    let mut items_by_source_id: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut items_by_title: HashMap<String, Vec<usize>> = HashMap::new();
    #[cfg(test)]
    let mut work = LegacyCandidateWork::default();

    for (item_index, item) in items.iter().enumerate() {
        if item.presence != Presence::Present {
            continue;
        }
        #[cfg(test)]
        {
            work.present_items_indexed += 1;
            work.item_title_normalizations += 1;
        }

        let mut indexed_item_ids = HashSet::with_capacity(item.source_ids.len());
        for id in &item.source_ids {
            // Preserve the old exact comparison against item.source_ids; only hint IDs are
            // canonicalized by normalize_source_ids.
            if indexed_item_ids.insert(id.as_str()) {
                items_by_source_id
                    .entry(id.as_str())
                    .or_default()
                    .push(item_index);
                #[cfg(test)]
                {
                    work.source_id_index_entries += 1;
                }
            }
        }

        items_by_title
            .entry(normalize_title(&item.title))
            .or_default()
            .push(item_index);
    }

    let mut candidates = Vec::new();
    let mut serialized_array_bytes = 2usize;
    let mut match_visits_remaining = limits.max_match_visits;
    let mut truncation = None;

    'hints: for (hint_index, hint) in hints.iter().enumerate() {
        let mut item_indices = Vec::new();
        let mut matching_complete = true;

        #[cfg(test)]
        {
            work.hint_source_id_values_normalized += hint.source_ids.len();
        }
        let normalized_hint_ids = normalize_source_ids(hint.source_ids.clone());
        for id in normalized_hint_ids {
            if match_visits_remaining == 0 {
                matching_complete = false;
                truncation = Some(LegacyCandidateTruncation::MatchVisits);
                break;
            }
            match_visits_remaining -= 1;
            #[cfg(test)]
            {
                work.source_id_lookups += 1;
            }

            if let Some(indexed_items) = items_by_source_id.get(id.as_str()) {
                for item_index in indexed_items {
                    if match_visits_remaining == 0 {
                        matching_complete = false;
                        truncation = Some(LegacyCandidateTruncation::MatchVisits);
                        break;
                    }
                    match_visits_remaining -= 1;
                    #[cfg(test)]
                    {
                        work.match_entries_visited += 1;
                    }
                    item_indices.push(*item_index);
                }
            }
            if !matching_complete {
                break;
            }
        }

        item_indices.sort_unstable();
        item_indices.dedup();
        if !matching_complete {
            // Do not show partial matches or fall back to title when some source IDs were not
            // checked; either could give the user an incomplete or lower-priority choice set.
            break 'hints;
        }
        let (match_kind, matched_indices) = if !item_indices.is_empty() {
            ("source_id", item_indices)
        } else if let Some(title) = hint.title.as_deref() {
            if match_visits_remaining == 0 {
                truncation = Some(LegacyCandidateTruncation::MatchVisits);
                break 'hints;
            }
            match_visits_remaining -= 1;
            #[cfg(test)]
            {
                work.title_index_lookups += 1;
                work.hint_title_normalizations += 1;
            }

            let normalized_title = normalize_title(title);
            let mut title_indices = Vec::new();
            let mut title_match_complete = true;
            if let Some(indexed_items) = items_by_title.get(&normalized_title) {
                for item_index in indexed_items {
                    if match_visits_remaining == 0 {
                        title_match_complete = false;
                        truncation = Some(LegacyCandidateTruncation::MatchVisits);
                        break;
                    }
                    match_visits_remaining -= 1;
                    #[cfg(test)]
                    {
                        work.match_entries_visited += 1;
                    }
                    title_indices.push(*item_index);
                }
            }
            if !title_match_complete {
                // Keep this hint's candidate set whole and in the original library order.
                break 'hints;
            }
            ("title", title_indices)
        } else {
            ("unmatched", Vec::new())
        };

        if matched_indices.is_empty() {
            continue;
        }

        let ambiguous = matched_indices.len() > 1 || hint.source_ids.is_empty();
        let mut hint_candidates = Vec::with_capacity(
            matched_indices
                .len()
                .min(limits.max_candidates.saturating_sub(candidates.len())),
        );
        let mut hint_array_bytes = serialized_array_bytes;
        for item_index in matched_indices {
            if candidates.len() + hint_candidates.len() >= limits.max_candidates {
                truncation = Some(LegacyCandidateTruncation::CandidateCount);
                break;
            }
            let item = &items[item_index];
            let candidate = LegacyCandidate {
                hint_index,
                item_id: item.id.clone(),
                title: item.title.clone(),
                relative_path: item.relative_path.clone(),
                match_kind: match_kind.to_string(),
                ambiguous,
                requires_confirmation: true,
            };
            let candidate_bytes = serde_json::to_vec(&candidate)?.len();
            let separator_bytes = if candidates.is_empty() && hint_candidates.is_empty() {
                0
            } else {
                1
            };
            let next_array_bytes = hint_array_bytes
                .saturating_add(separator_bytes)
                .saturating_add(candidate_bytes);
            if next_array_bytes > limits.max_serialized_array_bytes {
                truncation = Some(LegacyCandidateTruncation::SerializedBytes);
                break;
            }
            hint_array_bytes = next_array_bytes;
            hint_candidates.push(candidate);
        }

        if truncation.is_some() {
            // Emit candidate groups atomically: never make a hint look uniquely matched because
            // the global count/byte budget cut off its remaining choices.
            break 'hints;
        }
        candidates.extend(hint_candidates);
        serialized_array_bytes = hint_array_bytes;
    }

    Ok(LegacyCandidateMatches {
        candidates,
        truncation,
        #[cfg(test)]
        work,
    })
}

fn parse_legacy_import(input: &str) -> LegacyImportReport {
    let parsed_json = serde_json::from_str::<serde_json::Value>(input).ok();
    let (text_blocks, origin) = match parsed_json.as_ref() {
        Some(value) => {
            let mut collector = LegacyTextCollector::default();
            collect_legacy_text(value, &mut collector, LEGACY_INPUT_LIMIT);
            (collector.blocks, HintOrigin::LegacyTelegramJson)
        }
        None => (vec![input.to_string()], HintOrigin::LegacyText),
    };
    let mut hints = Vec::new();
    let mut warnings = Vec::new();
    let mut seen = HashSet::new();
    let mut truncated = false;
    'blocks: for block in &text_blocks {
        // The import byte limit bounds total scanning, while repeated rows do not consume the
        // unique-hint allowance.
        for line in block.split(['\r', '\n']) {
            let ids = extract_source_ids(line);
            let title = title_hint_from_line(line);
            if ids.is_empty() && title.is_none() {
                continue;
            }
            if !seen.insert((ids.clone(), title.clone())) {
                continue;
            }
            if hints.len() == MAX_LEGACY_HINTS {
                truncated = true;
                break 'blocks;
            }
            hints.push(SourceHint {
                source_ids: ids,
                title,
                relative_path: None,
                collection: None,
                origin,
                requires_confirmation: true,
            });
        }
    }
    if truncated {
        warnings.push(
            "Legacy import hints were truncated at the 10,000 unique-hint limit.".to_string(),
        );
    }
    if hints.is_empty() {
        let ids = normalize_source_ids(
            text_blocks
                .iter()
                .flat_map(|block| extract_source_ids(block))
                .collect(),
        );
        if !ids.is_empty() {
            hints.push(SourceHint {
                source_ids: ids,
                title: None,
                relative_path: None,
                collection: None,
                origin,
                requires_confirmation: true,
            });
        } else {
            warnings.push(
                "no Bilibili BV/av/cid/ep identifiers or supported YouTube video URLs were found in the supplied text".to_string(),
            );
        }
    }
    LegacyImportReport {
        needs_confirmation: !hints.is_empty(),
        hints,
        warnings,
        revision: None,
        candidates: Vec::new(),
    }
}

#[derive(Default)]
struct LegacyTextCollector {
    blocks: Vec<String>,
    bytes_used: usize,
    #[cfg(test)]
    visited_values: usize,
}

fn collect_legacy_text(
    value: &serde_json::Value,
    collector: &mut LegacyTextCollector,
    byte_limit: usize,
) {
    if collector.bytes_used >= byte_limit {
        return;
    }
    #[cfg(test)]
    {
        collector.visited_values = collector.visited_values.saturating_add(1);
    }
    match value {
        serde_json::Value::String(text) => {
            let remaining_bytes = byte_limit.saturating_sub(collector.bytes_used);
            let mut end = 0;
            for (index, character) in text.char_indices() {
                let next = index + character.len_utf8();
                if next > remaining_bytes {
                    break;
                }
                end = next;
            }
            if end > 0 {
                collector.bytes_used = collector.bytes_used.saturating_add(end);
                collector.blocks.push(text[..end].to_string());
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                if collector.bytes_used >= byte_limit {
                    break;
                }
                collect_legacy_text(value, collector, byte_limit);
            }
        }
        serde_json::Value::Object(fields) => {
            for (key, value) in fields {
                if key.eq_ignore_ascii_case("text")
                    || key.eq_ignore_ascii_case("messages")
                    || key.eq_ignore_ascii_case("chat_history")
                    || value.is_object()
                    || value.is_array()
                {
                    if collector.bytes_used >= byte_limit {
                        break;
                    }
                    collect_legacy_text(value, collector, byte_limit);
                }
            }
        }
        _ => {}
    }
}

fn title_hint_from_line(line: &str) -> Option<String> {
    let trimmed = line.trim();
    let lower = trimmed.to_ascii_lowercase();
    for prefix in ["title:", "title：", "标题:", "标题：", "名称:", "名称："] {
        if (lower.starts_with(prefix) || trimmed.starts_with(prefix))
            && let Some(title) = normalize_legacy_title_hint(&trimmed[prefix.len()..])
        {
            return Some(title);
        }
    }
    if let Some((title, rest)) = trimmed.split_once(" - ")
        && !title.contains("http")
        && !extract_source_ids(rest).is_empty()
    {
        return normalize_legacy_title_hint(title);
    }
    None
}

fn normalize_legacy_title_hint(title: &str) -> Option<String> {
    if title.len() > MAX_METADATA_TITLE_BYTES {
        return None;
    }

    let mut normalized = String::with_capacity(title.len());
    let mut pending_space = false;
    for character in title.trim().chars() {
        if character.is_whitespace() {
            pending_space = !normalized.is_empty();
            continue;
        }
        if character.is_control() {
            return None;
        }
        if pending_space {
            normalized.push(' ');
        }
        normalized.push(character);
        pending_space = false;
    }

    (!normalized.is_empty() && metadata_title_within_limits(&normalized)).then_some(normalized)
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::file_provider::{FileProviderAccessError, MockQueueFileProvider, QueueFileProvider};

    use super::*;

    #[test]
    fn round9_media_probe_selects_real_video_or_audio_streams() {
        let cases = [
            (
                "audio-only file uses its audio codec without dimensions",
                serde_json::json!({
                    "streams": [{
                        "codec_type": "audio",
                        "codec_name": "flac",
                        "width": 640,
                        "height": 480
                    }]
                }),
                Some("flac"),
                None,
                None,
            ),
            (
                "attached cover art falls back to the audio stream",
                serde_json::json!({
                    "streams": [
                        {
                            "codec_type": "video",
                            "codec_name": "mjpeg",
                            "width": 1200,
                            "height": 1200,
                            "disposition": {"attached_pic": 1}
                        },
                        {"codec_type": "audio", "codec_name": "aac"}
                    ]
                }),
                Some("aac"),
                None,
                None,
            ),
            (
                "real video is preferred even when an audio stream comes first",
                serde_json::json!({
                    "streams": [
                        {"codec_type": "audio", "codec_name": "aac"},
                        {
                            "codec_type": "video",
                            "codec_name": "h264",
                            "width": 1920,
                            "height": 1080,
                            "disposition": {"attached_pic": 0}
                        }
                    ]
                }),
                Some("h264"),
                Some(1920),
                Some(1080),
            ),
            (
                "video without disposition remains compatible with older probe output",
                serde_json::json!({
                    "streams": [{
                        "codec_type": "video",
                        "codec_name": "hevc",
                        "width": 3840,
                        "height": 2160
                    }]
                }),
                Some("hevc"),
                Some(3840),
                Some(2160),
            ),
            (
                "cover-only input does not publish cover dimensions or codec",
                serde_json::json!({
                    "streams": [{
                        "codec_type": "video",
                        "codec_name": "png",
                        "width": 1200,
                        "height": 1200,
                        "disposition": {"attached_pic": true}
                    }]
                }),
                None,
                None,
                None,
            ),
        ];

        for (description, probe, codec, width, height) in cases {
            let actual = media_specs_from_ffprobe_json(&probe);
            assert_eq!(actual.codec.as_deref(), codec, "{description}");
            assert_eq!(actual.width, width, "{description}");
            assert_eq!(actual.height, height, "{description}");
        }
    }

    struct TemporaryRoot(PathBuf);

    impl TemporaryRoot {
        fn new(label: &str) -> Self {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "telegram-video-downloader-library-{label}-{}-{now}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("isolated media root should create");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TemporaryRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn manager(root: &TemporaryRoot) -> LibraryManager {
        LibraryManager::open_with_provider(root.path(), Arc::new(MockQueueFileProvider::default()))
            .expect("library manager should open against the isolated fixture")
    }

    #[test]
    fn migration_state_replacement_supports_growth_and_shrink_with_snapshot_cas() {
        const STATE_NAME: &str = "migration-checkpoint-growth.json";
        const ORIGINAL: &[u8] = br#"{"version":1}"#;
        const GROWN: &[u8] = br#"{"version":2,"birth_seconds":1792713600,"birth_nanoseconds":0}"#;
        const SHRUNK: &[u8] = br#"{"version":3}"#;

        let root = TemporaryRoot::new("migration-state-growth-shrink");
        let manager = manager(&root);
        manager
            .migration_write_state(STATE_NAME, ORIGINAL)
            .expect("initial migration state should be written");
        let original = manager
            .migration_read_state_snapshot(STATE_NAME, 64 * 1024 * 1024)
            .expect("initial migration state snapshot should be read")
            .expect("initial migration state should exist");

        manager
            .migration_replace_state_if_unchanged(STATE_NAME, &original, GROWN)
            .expect("migration state should grow when its checkpoint adds birth identity");
        assert_eq!(
            manager
                .migration_read_state(STATE_NAME, 64 * 1024 * 1024)
                .expect("grown migration state should be readable"),
            Some(GROWN.to_vec())
        );

        let stale_error = manager
            .migration_replace_state_if_unchanged(STATE_NAME, &original, b"stale")
            .expect_err("the original snapshot proof must not replace a newer checkpoint");
        assert!(format!("{stale_error:#}").contains("identity or content changed"));
        assert_eq!(
            manager
                .migration_read_state(STATE_NAME, 64 * 1024 * 1024)
                .expect("grown migration state should remain readable after stale CAS"),
            Some(GROWN.to_vec())
        );

        let grown = manager
            .migration_read_state_snapshot(STATE_NAME, 64 * 1024 * 1024)
            .expect("grown migration state snapshot should be read")
            .expect("grown migration state should exist");
        manager
            .migration_replace_state_if_unchanged(STATE_NAME, &grown, SHRUNK)
            .expect("migration state should shrink when an update removes checkpoint data");
        assert_eq!(
            manager
                .migration_read_state(STATE_NAME, 64 * 1024 * 1024)
                .expect("shrunk migration state should be readable"),
            Some(SHRUNK.to_vec())
        );
    }

    #[test]
    fn legacy_target_verification_ignores_mtime_only_changes() {
        let root = TemporaryRoot::new("legacy-target-mtime-change");
        let path = root.path().join("clip.mp4");
        fs::write(&path, b"original-data").expect("media fixture should write");
        let manager = manager(&root);
        let file = manager
            .read_media_file(&path)
            .expect("media fixture should open")
            .expect("media fixture should be present");
        let before = fingerprint(&file).expect("media fingerprint should be available");
        let expected_identity = physical_item_identity(&before);
        let expected_access = file
            .access_policy()
            .expect("media access policy should be available");
        if !expected_identity.has_complete_birth() {
            let error = manager
                .migration_verify_legacy_target(
                    "clip.mp4",
                    before.identity,
                    before.size,
                    expected_access,
                    &sha256_bytes(b"original-data"),
                )
                .expect_err("a missing birth pair should be unverifiable");
            assert!(format!("{error:#}").contains("birth identity is unavailable"));
            return;
        }
        drop(file);

        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("media should open for timestamp-only update")
            .set_modified(SystemTime::now() + Duration::from_secs(3_600))
            .expect("media mtime should update without changing content");

        let after_file = manager
            .read_media_file(&path)
            .expect("media should reopen after mtime-only update")
            .expect("media should remain present after mtime-only update");
        let after =
            fingerprint(&after_file).expect("updated media fingerprint should be available");
        assert_eq!(
            (after.birth_seconds, after.birth_nanoseconds),
            (before.birth_seconds, before.birth_nanoseconds),
            "setting a future mtime should preserve the protected birth identity"
        );
        drop(after_file);

        let result = manager
            .migration_verify_legacy_target(
                "clip.mp4",
                before.identity,
                before.size,
                expected_access,
                &sha256_bytes(b"original-data"),
            )
            .expect("mtime-only changes should remain verifiable");
        assert_eq!(result, LegacyTargetVerification::Match(expected_identity));
    }

    #[test]
    fn legacy_target_verification_reports_a_stable_replacement_as_mismatch() {
        let root = TemporaryRoot::new("legacy-target-replacement");
        let path = root.path().join("clip.mp4");
        let archived = root.path().join("old-clip.mp4");
        fs::write(&path, b"original-data").expect("media fixture should write");
        let manager = manager(&root);
        let original = manager
            .read_media_file(&path)
            .expect("media fixture should open")
            .expect("media fixture should be present");
        let before = fingerprint(&original).expect("media fingerprint should be available");
        let expected_access = original
            .access_policy()
            .expect("media access policy should be available");
        if !physical_item_identity(&before).has_complete_birth() {
            let error = manager
                .migration_verify_legacy_target(
                    "clip.mp4",
                    before.identity,
                    before.size,
                    expected_access,
                    &sha256_bytes(b"original-data"),
                )
                .expect_err("a missing birth pair should be unverifiable");
            assert!(format!("{error:#}").contains("birth identity is unavailable"));
            return;
        }
        drop(original);

        fs::rename(&path, &archived).expect("original media should remain available as evidence");
        fs::write(&path, b"replaced-data").expect("replacement media should write");

        let result = manager
            .migration_verify_legacy_target(
                "clip.mp4",
                before.identity,
                before.size,
                expected_access,
                &sha256_bytes(b"original-data"),
            )
            .expect("stable replacement should be classified");
        let LegacyTargetVerification::Mismatch(actual) = result else {
            panic!("replacement should not match legacy target evidence: {result:?}");
        };
        assert_ne!(
            (actual.device, actual.inode),
            (before.identity.device(), before.identity.inode())
        );
    }

    struct FailOneMediaReadProvider {
        target: PathBuf,
        armed: std::sync::Mutex<HashSet<PathBuf>>,
    }

    impl FailOneMediaReadProvider {
        fn new(target: PathBuf) -> Self {
            Self {
                target,
                armed: std::sync::Mutex::new(HashSet::new()),
            }
        }

        fn fail_next_media_read(&self) {
            self.fail_next_media_read_at(&self.target);
        }

        fn fail_next_media_read_at(&self, path: &Path) {
            self.armed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(path.to_path_buf());
        }
    }

    impl QueueFileProvider for FailOneMediaReadProvider {
        fn coordinate_read(
            &self,
            path: &Path,
            accessor: &mut dyn FnMut(&Path) -> Result<()>,
        ) -> Result<()> {
            let fail = self
                .armed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(path);
            if fail {
                return Err(FileProviderAccessError::new(
                    path,
                    "read",
                    "injected transient media access error",
                )
                .into());
            }
            accessor(path)
        }

        fn coordinate_write(
            &self,
            path: &Path,
            accessor: &mut dyn FnMut(&Path) -> Result<()>,
        ) -> Result<()> {
            accessor(path)
        }

        fn coordinate_move(
            &self,
            source: &Path,
            destination: &Path,
            accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
        ) -> Result<()> {
            accessor(source, destination)
        }
    }

    fn cleanup_preview(manager: &LibraryManager, item_id: &str) -> MovePreview {
        manager
            .preview(
                &[item_id.to_string()],
                "Cleanup archive",
                false,
                ConflictPolicy::Skip,
            )
            .expect("cleanup preview fixture should persist")
    }

    fn cleanup_batch(
        preview: &MovePreview,
        attempt_finished: bool,
        retryable_access_error: bool,
        pending_file: bool,
    ) -> BatchManifest {
        BatchManifest {
            version: 1,
            preview: preview.clone(),
            status: BatchStatus::Failed,
            attempt_finished,
            retryable_access_error,
            files: if pending_file {
                vec![ManifestFile {
                    item_id: "fixture-item".to_string(),
                    source_path: "clip.mp4".to_string(),
                    target_path: "Cleanup archive/clip.mp4".to_string(),
                    identity: "fixture-identity".to_string(),
                    state: ManifestFileState::Pending,
                    error: Some("permanent fixture failure".to_string()),
                }]
            } else {
                Vec::new()
            },
            patches: Vec::new(),
            items: Vec::new(),
            warnings: Vec::new(),
        }
    }

    fn set_operation_state_age(path: &Path, age: Duration) {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("operation-state fixture should open for mtime adjustment");
        file.set_modified(SystemTime::now() - age)
            .expect("operation-state fixture mtime should update");
    }

    fn run_operation_state_cleanup(
        manager: &LibraryManager,
        registry: &Mutex<OperationStateCleanupCursorRegistry>,
    ) -> Vec<String> {
        let _process_guard = manager
            .in_process_lock
            .lock()
            .expect("library manager lock should remain healthy");
        let _disk_guard = manager
            .lock_store()
            .expect("library store lock should remain healthy");
        manager.prune_persisted_operation_states_with_registry(registry)
    }

    fn cleanup_preview_candidate(
        manager: &LibraryManager,
        preview: &MovePreview,
    ) -> OperationStateCleanupCandidate {
        let path = manager.preview_path(&preview.id);
        let entry = manager
            .root
            .bind_entry(&path, false)
            .expect("persisted preview path should bind");
        let identity = manager
            .root
            .bound_entry_identity(&entry)
            .expect("persisted preview identity should read")
            .expect("persisted preview should exist");
        match manager
            .read_operation_state_candidate(&path, identity, PREVIEW_LIMIT, PREVIEW_LIMIT)
            .expect("persisted preview should be safely readable")
        {
            OperationStateCandidateRead::Ready(candidate) => *candidate,
            OperationStateCandidateRead::Missing | OperationStateCandidateRead::Deferred => {
                panic!("persisted preview should produce a cleanup candidate")
            }
        }
    }

    fn item_at<'a>(snapshot: &'a LibrarySnapshot, relative_path: &str) -> &'a LibraryItem {
        snapshot
            .items
            .iter()
            .find(|item| item.relative_path == relative_path)
            .expect("fixture media item should be present in the scan")
    }

    fn persisted_preview_count(manager: &LibraryManager) -> usize {
        fs::read_dir(&manager.library_dir)
            .expect("private library directory should list")
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(PREVIEW_PREFIX) && name.ends_with(".json"))
            })
            .count()
    }

    fn queue_hint(source_ids: &[&str], collection: CollectionMetadata) -> SourceHint {
        SourceHint {
            source_ids: source_ids.iter().map(|id| (*id).to_string()).collect(),
            title: None,
            relative_path: None,
            collection: Some(collection),
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        }
    }

    fn unique_legacy_hint_lines(start: usize, count: usize) -> String {
        (start..start + count)
            .map(|index| format!("BV1{index:010}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn index_fixture_item(id: &str, relative_path: &str, presence: Presence) -> LibraryItem {
        LibraryItem {
            id: id.to_string(),
            title: relative_path.to_string(),
            relative_path: relative_path.to_string(),
            bytes: 123,
            width: None,
            height: None,
            codec: None,
            source_ids: Vec::new(),
            collection: None,
            organization_status: None,
            attachments: Vec::new(),
            authoritative_nfo_path: None,
            confidence: Confidence::Low,
            metadata_status: MetadataStatus::Unknown,
            integrity_status: IntegrityStatus::Unverified,
            physical_identity: None,
            presence,
        }
    }

    fn index_fixture_record(
        id: &str,
        relative_path: &str,
        presence: Presence,
        device: u64,
        inode: u64,
        missing_since: Option<u64>,
    ) -> IndexRecord {
        IndexRecord {
            item: index_fixture_item(id, relative_path, presence),
            device,
            inode,
            file_type: "regular_file".to_string(),
            birth_seconds: Some(inode as i64),
            birth_nanoseconds: Some(0),
            missing_since,
            queue_metadata: QueueMetadataProvenance::default(),
        }
    }

    #[test]
    fn source_id_normalization_keeps_numeric_bilibili_prefixes_ahead_of_bare_youtube_ids() {
        assert_eq!(
            normalize_source_ids(vec!["cid12345678".to_string()]),
            vec!["cid12345678".to_string()]
        );
        assert_eq!(
            normalize_source_ids(vec!["av123456789".to_string()]),
            vec!["av123456789".to_string()]
        );
        assert_eq!(
            normalize_source_ids(vec!["ep123456789".to_string()]),
            vec!["ep123456789".to_string()]
        );
        assert_eq!(
            normalize_source_ids(vec!["youtube:cid12345678".to_string()]),
            vec!["youtube:cid12345678".to_string()]
        );
        assert_eq!(
            normalize_source_ids(vec!["bv1AbCdEfG".to_string()]),
            vec!["BV1AbCdEfG".to_string()]
        );
    }

    #[test]
    fn extracts_youtube_ids_from_supported_legacy_urls() {
        let ids = extract_source_ids(
            "Watch https://www.youtube.com/watch?v=dQw4w9WgXcQ&feature=share#t=30). \
             Rewatch https://youtu.be/PHH1wTDF-1M?si=share#t=10! \
             Again `https://youtu.be/PHH1wTDF-1M?si=share#t=10`。）",
        );
        assert_eq!(
            ids,
            vec![
                "youtube:dQw4w9WgXcQ".to_string(),
                "youtube:PHH1wTDF-1M".to_string(),
            ]
        );
    }

    #[test]
    fn youtube_url_extraction_preserves_bilibili_ids_and_later_tokens() {
        let ids = extract_source_ids(
            "https://www.bilibili.com/video/BV1AbCdEfG https://youtu.be/PHH1wTDF-1M). cid12345678 av123456789",
        );
        assert_eq!(
            ids,
            vec![
                "BV1AbCdEfG".to_string(),
                "youtube:PHH1wTDF-1M".to_string(),
                "cid12345678".to_string(),
                "av123456789".to_string(),
            ]
        );
    }

    #[test]
    fn youtube_url_extraction_rejects_invalid_hosts_ids_and_bare_text() {
        let text = concat!(
            "https://youtube.com.evil/watch?v=dQw4w9WgXcQ ",
            "https://www.youtube.com.evil/watch?v=dQw4w9WgXcQ ",
            "https://youtu.be.evil/PHH1wTDF-1M ",
            "https://www.youtube.com/watch?v=short ",
            "https://www.youtube.com/watch?v=dQw4w9WgXcQx ",
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ%2F ",
            "prefixhttps://youtu.be/PHH1wTDF-1M ",
            "PHH1wTDF-1M",
        );
        assert!(extract_source_ids(text).is_empty());
    }

    #[test]
    fn dense_unspaced_url_text_is_scanned_once_per_token() {
        let mut text = String::new();
        for _ in 0..256 {
            text.push_str("!https://invalid.example/path");
        }
        text.push_str(" cid12345678");

        assert_eq!(extract_source_ids(&text), vec!["cid12345678"]);
    }

    #[test]
    fn legacy_import_creates_a_confirmation_hint_from_a_youtube_url() {
        let report = parse_legacy_import("https://youtu.be/PHH1wTDF-1M");
        assert!(report.needs_confirmation);
        assert_eq!(report.hints.len(), 1);
        assert_eq!(report.hints[0].source_ids, vec!["youtube:PHH1wTDF-1M"]);
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn legacy_import_retains_title_only_hints_and_ignores_noise() {
        let plain = parse_legacy_import(
            "TITLE: Inception\nTitle： Inception\nhttps://youtube.com.evil/watch?v=dQw4w9WgXcQ",
        );
        assert!(plain.needs_confirmation);
        assert_eq!(
            plain.hints.len(),
            1,
            "duplicate title hints should deduplicate"
        );
        assert!(plain.hints[0].source_ids.is_empty());
        assert_eq!(plain.hints[0].title.as_deref(), Some("Inception"));
        assert_eq!(plain.hints[0].origin, HintOrigin::LegacyText);
        assert!(plain.hints[0].requires_confirmation);
        assert!(plain.warnings.is_empty());

        let telegram_json = serde_json::json!({
            "messages": [
                { "text": "Title: Inception" },
                { "text": "Title: Interstellar" },
                { "text": "https://youtube.com.evil/watch?v=dQw4w9WgXcQ" },
            ]
        })
        .to_string();
        let json_report = parse_legacy_import(&telegram_json);
        assert_eq!(
            json_report
                .hints
                .iter()
                .map(|hint| (hint.source_ids.clone(), hint.title.as_deref()))
                .collect::<Vec<_>>(),
            vec![
                (Vec::new(), Some("Inception")),
                (Vec::new(), Some("Interstellar")),
            ]
        );
        assert_eq!(json_report.hints[0].origin, HintOrigin::LegacyTelegramJson);

        let noise = parse_legacy_import(
            "\nTitle: \nhttps://youtube.com.evil/watch?v=dQw4w9WgXcQ\nhttps://example.invalid/video",
        );
        assert!(noise.hints.is_empty());
        assert!(!noise.needs_confirmation);
        assert_eq!(
            noise.warnings,
            vec![
                "no Bilibili BV/av/cid/ep identifiers or supported YouTube video URLs were found in the supplied text"
                    .to_string()
            ]
        );

        let id_only = parse_legacy_import("BV1abcdefgh");
        assert_eq!(id_only.hints.len(), 1);
        assert_eq!(id_only.hints[0].source_ids, vec!["BV1abcdefgh".to_string()]);
        assert_eq!(id_only.hints[0].title, None);
    }

    #[test]
    fn legacy_import_applies_the_global_hint_limit_to_title_only_rows() {
        let mut input = unique_legacy_hint_lines(0, MAX_LEGACY_HINTS - 1);
        input.push_str(&format!(
            "\nTitle: Inception\nBV1{:010}",
            MAX_LEGACY_HINTS + 1
        ));

        let report = parse_legacy_import(&input);

        assert_eq!(report.hints.len(), MAX_LEGACY_HINTS);
        assert_eq!(
            report.hints[MAX_LEGACY_HINTS - 1].source_ids,
            Vec::<String>::new()
        );
        assert_eq!(
            report.hints[MAX_LEGACY_HINTS - 1].title.as_deref(),
            Some("Inception")
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| { warning.contains("10,000 unique-hint limit") })
        );
    }

    #[test]
    fn legacy_import_keeps_the_exact_global_unique_hint_limit_without_warning() {
        let report = parse_legacy_import(&unique_legacy_hint_lines(0, MAX_LEGACY_HINTS));

        assert_eq!(report.hints.len(), MAX_LEGACY_HINTS);
        assert_eq!(
            report.hints.first().unwrap().source_ids,
            vec![format!("BV1{:010}", 0)]
        );
        assert_eq!(
            report.hints.last().unwrap().source_ids,
            vec![format!("BV1{:010}", MAX_LEGACY_HINTS - 1)]
        );
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn legacy_import_caps_unique_hints_across_json_text_blocks_and_warns() {
        let first_block = unique_legacy_hint_lines(0, 6_000);
        let second_block = unique_legacy_hint_lines(6_000, 6_000);
        let input = serde_json::json!({ "text": [first_block, second_block] }).to_string();
        let report = parse_legacy_import(&input);

        assert_eq!(report.hints.len(), MAX_LEGACY_HINTS);
        assert_eq!(
            report.hints.last().unwrap().source_ids,
            vec![format!("BV1{:010}", MAX_LEGACY_HINTS - 1)]
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("10,000 unique-hint limit"))
        );
    }

    #[test]
    fn legacy_import_duplicates_do_not_consume_the_unique_hint_limit() {
        let duplicate_block = vec!["BV1abcdefgh"; MAX_LEGACY_HINTS].join("\n");
        let unique_block = unique_legacy_hint_lines(0, MAX_LEGACY_HINTS - 1);
        let input = serde_json::json!({ "text": [duplicate_block, unique_block] }).to_string();
        let report = parse_legacy_import(&input);

        assert_eq!(report.hints.len(), MAX_LEGACY_HINTS);
        assert_eq!(report.hints[0].source_ids, vec!["BV1abcdefgh".to_string()]);
        assert_eq!(
            report.hints.last().unwrap().source_ids,
            vec![format!("BV1{:010}", MAX_LEGACY_HINTS - 2)]
        );
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn legacy_candidate_mapping_accepts_the_last_supported_hint_index() {
        let mut text = unique_legacy_hint_lines(0, MAX_LEGACY_HINTS - 1);
        text.push_str("\nBV1abcdefgh");
        let report = parse_legacy_import(&text);
        let mut item = index_fixture_item("mapped-item", "mapped.mp4", Presence::Present);
        item.source_ids = vec!["BV1abcdefgh".to_string()];

        let matches = legacy_candidates(&report.hints, &[item], DEFAULT_LEGACY_CANDIDATE_LIMITS)
            .expect("last in-range hint should remain available for matching");

        assert_eq!(report.hints.len(), MAX_LEGACY_HINTS);
        assert!(report.warnings.is_empty());
        assert_eq!(matches.candidates.len(), 1);
        assert_eq!(matches.candidates[0].hint_index, MAX_LEGACY_HINTS - 1);
    }

    #[test]
    fn round5_json_metadata_ignores_nested_chapters_and_non_media_wrappers() {
        let direct = serde_json::json!({
            "title": "Video title",
            "cid": 123,
            "chapters": [{ "title": "Opening chapter", "cid": 987 }],
            "playlist": [{ "title": "Other video", "aid": 654 }]
        });
        let (title, ids, collection) = metadata_from_json(&direct);
        assert_eq!(title.as_deref(), Some("Video title"));
        assert_eq!(ids, vec!["cid123"]);
        assert!(collection.is_none());

        let wrapped = serde_json::json!({
            "title": "Playlist title",
            "cid": 999,
            "chapters": [{ "title": "Wrong chapter", "cid": 998 }],
            "data": {
                "title": "Season wrapper",
                "cid": 997,
                "View": {
                    "title": "Episode title",
                    "cid": 321,
                    "bvid": "BV1AbCdEfG",
                    "ugc_season": { "id": 55, "title": "Season title" },
                    "episodes": [{ "cid": 996, "title": "Other episode" }]
                }
            }
        });
        let (title, ids, collection) = metadata_from_json(&wrapped);
        assert_eq!(title.as_deref(), Some("Episode title"));
        assert_eq!(ids, vec!["BV1AbCdEfG", "cid321"]);
        assert_eq!(
            collection.as_ref().map(|value| value.title.as_str()),
            Some("Season title")
        );

        let direct_with_data = serde_json::json!({
            "title": "Root media title",
            "cid": 456,
            "data": { "title": "Unrelated nested payload", "cid": 789 }
        });
        let (title, ids, _) = metadata_from_json(&direct_with_data);
        assert_eq!(title.as_deref(), Some("Root media title"));
        assert_eq!(ids, vec!["cid456"]);

        for payload_type in ["playlist", "multi_video"] {
            let collection_payload = serde_json::json!({
                "_type": payload_type,
                "title": "Collection title",
                "cid": 555,
                "entries": [{ "title": "Playlist entry", "cid": 556 }]
            });
            let (title, ids, collection) = metadata_from_json(&collection_payload);
            assert!(title.is_none());
            assert!(ids.is_empty());
            assert!(collection.is_none());
        }
    }

    #[test]
    fn round5_nfo_text_nodes_preserve_cdata_for_titles_and_source_ids() {
        let nfo = "<movie><title><![CDATA[Rock & Roll]]></title><uniqueid type=\"bilibili-cid\"><![CDATA[123456789]]></uniqueid></movie>";
        assert_eq!(xml_tag_text(nfo, "title").as_deref(), Some("Rock & Roll"));
        assert_eq!(nfo_source_ids(nfo), vec!["cid123456789"]);
    }

    #[test]
    fn nfo_collection_title_reads_scalar_and_nested_cdata_names() {
        assert_eq!(
            nfo_collection_title("<movie><set><![CDATA[Scalar Series]]></set></movie>").as_deref(),
            Some("Scalar Series")
        );
        assert_eq!(
            nfo_collection_title(
                "<movie><set><name><![CDATA[Nested Series]]></name><overview>Keep me</overview></set></movie>"
            )
            .as_deref(),
            Some("Nested Series")
        );
    }

    #[test]
    fn round6_xml_unescape_decodes_valid_decimal_and_hex_character_references() {
        assert_eq!(xml_unescape("&#65;&#xDF;&#x1F600; &lt;&amp;"), "Aß😀 <&");
    }

    #[test]
    fn round6_xml_unescape_preserves_invalid_character_references() {
        let references = "&#;|&#x;|&#xZZ;|&#4294967296;|&#x110000;|&#xD800;|&#1;|&#65|&unknown;";
        assert_eq!(xml_unescape(references), references);
    }

    #[test]
    fn round6_xml_unescape_decodes_once_and_keeps_cdata_literal() {
        assert_eq!(xml_unescape("&amp;#38; &amp;amp;"), "&#38; &amp;");
        assert_eq!(
            xml_text_content("<title><![CDATA[&#38; &amp;]]></title>"),
            "&#38; &amp;"
        );
    }

    #[test]
    fn round7_xml_comment_decoys_do_not_affect_nfo_read_or_patch() {
        let original = "<movie><!-- fake </movie> <title>Comment title</title><uniqueid type=\"bilibili-cid\">999</uniqueid> --><titleExtra>not the title</titleExtra><title>Real title</title><uniqueid type='bilibili-cid'>123</uniqueid><future>keep</future></movie>";
        assert_eq!(
            xml_tag_text(original, "title").as_deref(),
            Some("Real title")
        );
        assert_eq!(nfo_source_ids(original), vec!["cid123"]);
        assert_eq!(
            root_close_start(original),
            Some(original.rfind("</movie>").expect("real root close exists"))
        );

        let patched = append_metadata_patch(
            original,
            &MetadataPatch {
                item_id: "item".to_string(),
                title: Some("Patched title".to_string()),
                source_ids: vec!["cid456".to_string()],
                collection: None,
                fill_missing_only: false,
                organization_status: None,
            },
        )
        .expect("metadata patch should target real XML elements");

        assert!(patched.contains(
            "<!-- fake </movie> <title>Comment title</title><uniqueid type=\"bilibili-cid\">999</uniqueid> -->"
        ));
        assert!(patched.contains("<title>Patched title</title>"));
        assert!(patched.contains("<uniqueid type='bilibili-cid'>123</uniqueid>"));
        assert!(patched.contains("<uniqueid type=\"bilibili-cid\">cid456</uniqueid>"));
        assert_eq!(nfo_source_ids(&patched), vec!["cid123", "cid456"]);
        assert!(patched.contains("<future>keep</future>"));
    }

    #[test]
    fn round7_xml_scanner_skips_pi_declarations_and_quoted_attribute_decoys() {
        let nfo = r#"<?xml version="1.0"?>
<!DOCTYPE movie [ <!ENTITY fake "<title>Declaration title</title></movie>"> ]>
<?probe <title>PI title</title><uniqueid type="bilibili">888</uniqueid> ?>
<movie>
  <note decoy="<title>Attribute title</title>">
    <title><![CDATA[Literal <title>CDATA title</title> &amp;]]></title>
  </note>
  <uniqueid note="type='youtube' <uniqueid type='bilibili'>777</uniqueid>" type='bilibili-cid'>321</uniqueid>
  <!-- </movie> -->
</movie>
"#;

        assert_eq!(
            xml_tag_text(nfo, "title").as_deref(),
            Some("Literal <title>CDATA title</title> &amp;")
        );
        assert_eq!(nfo_source_ids(nfo), vec!["cid321"]);
        assert_eq!(
            root_close_start(nfo),
            Some(nfo.rfind("</movie>").expect("real root close exists"))
        );

        let patched = replace_xml_element(nfo, "title", "Patched & title")
            .expect("patch should replace only the actual title element");
        assert!(patched.contains(r#"<note decoy="<title>Attribute title</title>">"#));
        assert!(patched.contains("<title>Patched &amp; title</title>"));
        assert!(patched.contains("<uniqueid note=\"type='youtube' <uniqueid type='bilibili'>777</uniqueid>\" type='bilibili-cid'>321</uniqueid>"));
        assert!(patched.contains(
            "<?probe <title>PI title</title><uniqueid type=\"bilibili\">888</uniqueid> ?>"
        ));
    }

    #[test]
    fn round8_xml_self_closing_expansion_preserves_opening_name_spelling() {
        let cases = [
            ("<title/>", "title", "<title>", "</title>"),
            (
                "<TITLE attr='keep' />",
                "title",
                "<TITLE attr='keep' >",
                "</TITLE>",
            ),
            (
                "<TiTlE data=\"x\"/>",
                "title",
                "<TiTlE data=\"x\">",
                "</TiTlE>",
            ),
            ("<SeT />", "set", "<SeT >", "</SeT>"),
        ];
        let comment = "<!-- <title>comment title</title><set>comment set</set> -->";

        for (opening_tag, lookup, expanded_open, expected_close) in cases {
            let original = format!("<movie>{comment}{opening_tag}<future>opaque</future></movie>");
            let patch = MetadataPatch {
                item_id: "item".to_string(),
                title: (lookup == "title").then(|| "Updated & value".to_string()),
                source_ids: Vec::new(),
                collection: (lookup == "set").then(|| CollectionMetadata {
                    id: None,
                    title: "Updated & value".to_string(),
                    kind: "series".to_string(),
                    order: None,
                    part_id: None,
                }),
                fill_missing_only: false,
                organization_status: None,
            };

            let patched = append_metadata_patch(&original, &patch)
                .expect("metadata patch should expand a real self-closing element");
            let expected_expansion = format!("{expanded_open}Updated &amp; value{expected_close}");
            assert!(patched.contains(expected_expansion.as_str()));
            assert_eq!(
                xml_tag_text(&patched, lookup).as_deref(),
                Some("Updated & value")
            );
            assert!(patched.contains(comment));
            assert!(patched.contains("<future>opaque</future>"));

            let span = xml_element_span(&patched, lookup, 0)
                .expect("expanded element should remain readable");
            assert!(!span.self_closing);
            let opening = xml_next_markup(&patched, span.open_start)
                .expect("expanded opening tag should parse");
            let closing = xml_next_markup(&patched, span.close_start)
                .expect("expanded closing tag should parse");
            assert_eq!(opening.kind, XmlMarkupKind::Start);
            assert_eq!(closing.kind, XmlMarkupKind::End);
            assert_eq!(opening.name, closing.name);

            let repeated = append_metadata_patch(&patched, &patch)
                .expect("reapplying the same patch should remain compatible");
            assert_eq!(repeated, patched);
        }
    }

    #[test]
    fn round5_legacy_index_records_default_queue_provenance_to_unknown() {
        let value = serde_json::json!({
            "item": index_fixture_item("legacy", "clip.mp4", Presence::Present),
            "device": 1,
            "inode": 2,
            "file_type": "regular_file",
            "birth_seconds": 2,
            "birth_nanoseconds": 0,
            "missing_since": null
        });
        let record: IndexRecord = serde_json::from_value(value)
            .expect("an index record from the prior schema should remain readable");
        assert!(record.queue_metadata.title.is_none());
        assert!(record.queue_metadata.source_ids.is_empty());
        assert!(record.queue_metadata.collection.is_none());
        assert!(!record.queue_metadata.requires_confirmation);
    }

    #[test]
    fn round5_hint_paths_are_exclusive_and_fallback_requires_absence_and_uniqueness() {
        let mut first = index_fixture_item("first", "first-av123456789.mp4", Presence::Present);
        first.source_ids = vec!["av123456789".to_string()];
        let mut second = index_fixture_item("second", "second-av123456789.mp4", Presence::Present);
        second.source_ids = vec!["av123456789".to_string()];
        let items = vec![first, second];
        let hint = SourceHint {
            source_ids: vec!["av123456789".to_string()],
            title: Some("Queue title".to_string()),
            relative_path: Some("first-av123456789.mp4".to_string()),
            collection: None,
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let mut walk = ScanWalk::default();
        walk.complete_directories.insert(String::new());

        let exact = HashMap::from([(
            "first-av123456789.mp4".to_string(),
            HintPathResolution::Item(0),
        )]);
        assert_eq!(
            matching_hint_indices_for_items(&items, std::slice::from_ref(&hint), &exact, &walk),
            vec![vec![0], vec![]]
        );

        let absent = HashMap::from([(
            "missing.mp4".to_string(),
            HintPathResolution::ConfirmedAbsent,
        )]);
        let missing_hint = SourceHint {
            relative_path: Some("missing.mp4".to_string()),
            ..hint.clone()
        };
        assert_eq!(
            matching_hint_indices_for_items(
                &items,
                std::slice::from_ref(&missing_hint),
                &absent,
                &walk
            ),
            vec![Vec::<usize>::new(), Vec::<usize>::new()],
            "a duplicate source ID must stay ambiguous after an absent path"
        );

        let unique = vec![items[0].clone()];
        assert_eq!(
            matching_hint_indices_for_items(
                &unique,
                std::slice::from_ref(&missing_hint),
                &absent,
                &walk
            ),
            vec![vec![0]],
            "a confirmed absent path may use one unique source-ID candidate"
        );

        let mut incomplete_walk = walk.clone();
        incomplete_walk
            .incomplete_directories
            .insert("unreadable".to_string());
        let unavailable_parent_hint = SourceHint {
            relative_path: Some("unreadable/clip.mp4".to_string()),
            ..missing_hint.clone()
        };
        let absent_under_incomplete_parent = HashMap::from([(
            "unreadable/clip.mp4".to_string(),
            HintPathResolution::ConfirmedAbsent,
        )]);
        assert_eq!(
            matching_hint_indices_for_items(
                &unique,
                std::slice::from_ref(&unavailable_parent_hint),
                &absent_under_incomplete_parent,
                &incomplete_walk,
            ),
            vec![Vec::<usize>::new()],
            "an unreadable directory must not be treated as proof that the hint path is absent"
        );

        let pathless_hint = SourceHint {
            relative_path: None,
            ..missing_hint.clone()
        };
        assert_eq!(
            matching_hint_indices_for_items(
                &unique,
                std::slice::from_ref(&pathless_hint),
                &HashMap::new(),
                &walk
            ),
            vec![vec![0]],
            "a pathless hint may use one unique source-ID candidate"
        );
        assert_eq!(
            matching_hint_indices_for_items(&items, &[pathless_hint], &HashMap::new(), &walk),
            vec![Vec::<usize>::new(), Vec::<usize>::new()],
            "a pathless hint must not attach to multiple copies"
        );

        let present = HashMap::from([(
            "directory/clip.mp4".to_string(),
            HintPathResolution::PresentUnmatched,
        )]);
        let existing_hint = SourceHint {
            relative_path: Some("directory/clip.mp4".to_string()),
            ..missing_hint.clone()
        };
        assert_eq!(
            matching_hint_indices_for_items(&unique, &[existing_hint], &present, &walk),
            vec![Vec::<usize>::new()],
            "an existing non-media path must not fall back by source ID"
        );

        let unavailable = HashMap::from([(
            "unreadable/clip.mp4".to_string(),
            HintPathResolution::Unavailable,
        )]);
        let unreadable_hint = SourceHint {
            relative_path: Some("unreadable/clip.mp4".to_string()),
            ..missing_hint
        };
        assert_eq!(
            matching_hint_indices_for_items(&unique, &[unreadable_hint], &unavailable, &walk),
            vec![Vec::<usize>::new()],
            "a failed rooted lookup must remain distinct from a missing path"
        );
    }

    #[test]
    fn round10_hint_path_alias_uses_each_scanned_parent_filesystem_policy() {
        let normalization_insensitive =
            HashMap::from([(String::new(), round5_semantics(false, true))]);
        let normalization_sensitive =
            HashMap::from([(String::new(), round5_semantics(false, false))]);
        let case_insensitive = HashMap::from([(String::new(), round5_semantics(true, false))]);
        let case_sensitive = HashMap::from([(String::new(), round5_semantics(false, false))]);
        let hfs_ignorable = HashMap::from([(
            String::new(),
            TargetNameSemantics {
                hfs_ignorable: true,
                ..round5_semantics(false, true)
            },
        )]);

        assert!(
            filesystem_equivalent_path_alias_with(
                "e\u{301}.mp4",
                "é.mp4",
                &normalization_insensitive,
                &round5_full_fold_for_test,
            )
            .unwrap()
        );
        assert!(
            !filesystem_equivalent_path_alias_with(
                "e\u{301}.mp4",
                "é.mp4",
                &normalization_sensitive,
                &round5_full_fold_for_test,
            )
            .unwrap()
        );
        assert!(
            filesystem_equivalent_path_alias_with(
                "clip.mp4",
                "Clip.mp4",
                &case_insensitive,
                &round5_full_fold_for_test,
            )
            .unwrap()
        );
        assert!(
            !filesystem_equivalent_path_alias_with(
                "clip.mp4",
                "Clip.mp4",
                &case_sensitive,
                &round5_full_fold_for_test,
            )
            .unwrap()
        );
        assert!(
            filesystem_equivalent_path_alias_with(
                "clip\u{200d}.mp4",
                "clip.mp4",
                &hfs_ignorable,
                &round5_full_fold_for_test,
            )
            .unwrap()
        );
        assert!(
            !filesystem_equivalent_path_alias_with(
                "alias.mp4",
                "Clip.mp4",
                &case_insensitive,
                &round5_full_fold_for_test,
            )
            .unwrap()
        );

        let nested_policies = HashMap::from([
            (
                String::new(),
                TargetNameSemantics {
                    normalization_insensitive: true,
                    case_insensitive: false,
                    hfs_ignorable: false,
                },
            ),
            (
                "Épisode".to_string(),
                TargetNameSemantics {
                    normalization_insensitive: true,
                    case_insensitive: true,
                    hfs_ignorable: true,
                },
            ),
        ]);
        assert!(
            filesystem_equivalent_path_alias_with(
                "E\u{301}pisode/CLIP-e\u{301}.mp4\u{200d}",
                "Épisode/clip-é.mp4",
                &nested_policies,
                &round5_full_fold_for_test,
            )
            .unwrap()
        );
        assert!(
            !filesystem_equivalent_path_alias_with(
                "e\u{301}pisode/CLIP-e\u{301}.mp4\u{200d}",
                "Épisode/clip-é.mp4",
                &nested_policies,
                &round5_full_fold_for_test,
            )
            .unwrap(),
            "the root's case-sensitive policy must apply to its child directory"
        );
        let policies_keyed_by_hint_spelling = HashMap::from([
            (
                String::new(),
                TargetNameSemantics {
                    normalization_insensitive: true,
                    case_insensitive: false,
                    hfs_ignorable: false,
                },
            ),
            (
                "E\u{301}pisode".to_string(),
                TargetNameSemantics {
                    normalization_insensitive: true,
                    case_insensitive: true,
                    hfs_ignorable: true,
                },
            ),
        ]);
        assert!(
            !filesystem_equivalent_path_alias_with(
                "E\u{301}pisode/CLIP-e\u{301}.mp4\u{200d}",
                "Épisode/clip-é.mp4",
                &policies_keyed_by_hint_spelling,
                &round5_full_fold_for_test,
            )
            .unwrap(),
            "nested policy lookup must use the actual scanned parent spelling"
        );
    }

    #[test]
    fn round10_queue_hint_metadata_restores_through_normalized_path_alias() {
        let root = TemporaryRoot::new("round10-normalized-queue-hint");
        let scanned_relative = "Épisode/clip-é.mp4";
        let hint_relative = "E\u{301}pisode/clip-e\u{301}.mp4\u{200d}";
        let scanned_path = root.path().join(scanned_relative);
        let hint_path = root.path().join(hint_relative);
        fs::create_dir_all(scanned_path.parent().expect("scanned path has parent"))
            .expect("scanned parent should be created");
        fs::create_dir_all(hint_path.parent().expect("hint path has parent"))
            .expect("hint parent should be created");
        fs::write(&scanned_path, b"fixture media").expect("scanned media should be written");
        if let Err(error) = fs::hard_link(&scanned_path, &hint_path) {
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::AlreadyExists,
                "distinct portable aliases should be hard-linked"
            );
        }

        let manager = manager(&root);
        let identity = manager
            .root
            .entry_identity(&scanned_path)
            .expect("scanned path identity should be readable")
            .expect("scanned media should exist");
        let scanned_files = vec![ScannedFile {
            relative_path: scanned_relative.to_string(),
            identity,
        }];
        let mut item = index_fixture_item("physical-item", scanned_relative, Presence::Present);
        item.title = title_from_path(scanned_relative);
        let items = vec![item];
        let collection = CollectionMetadata {
            id: Some("queue-season".to_string()),
            title: "Queue Season".to_string(),
            kind: "queue".to_string(),
            order: Some(2),
            part_id: None,
        };
        let hint = SourceHint {
            source_ids: vec!["av123456789".to_string()],
            title: Some("Queue title".to_string()),
            relative_path: Some(hint_relative.to_string()),
            collection: Some(collection.clone()),
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let mut walk = ScanWalk::default();
        walk.directory_name_semantics.insert(
            String::new(),
            TargetNameSemantics {
                normalization_insensitive: true,
                case_insensitive: false,
                hfs_ignorable: false,
            },
        );
        walk.directory_name_semantics.insert(
            "Épisode".to_string(),
            TargetNameSemantics {
                normalization_insensitive: true,
                case_insensitive: true,
                hfs_ignorable: true,
            },
        );

        let (resolutions, unresolved) = manager.resolve_hint_paths_for_scan(
            std::slice::from_ref(&hint),
            &scanned_files,
            &items,
            &walk,
        );
        assert_eq!(unresolved, 0);
        assert_eq!(
            resolutions.get(hint_relative),
            Some(&HintPathResolution::Item(0)),
            "the rooted hint lookup and scan entry should resolve to one physical object"
        );

        let matches = matching_hint_indices_for_items(
            &items,
            std::slice::from_ref(&hint),
            &resolutions,
            &walk,
        );
        assert_eq!(matches, vec![vec![0]]);
        let mut attached_items = items;
        let provenance = attach_matching_hints(
            &mut attached_items,
            std::slice::from_ref(&hint),
            &matches,
            &HashSet::new(),
        );
        assert_eq!(attached_items[0].title, "Queue title");
        assert!(
            attached_items[0]
                .source_ids
                .contains(&"av123456789".to_string())
        );
        assert_eq!(attached_items[0].collection.as_ref(), Some(&collection));
        assert_eq!(provenance[0].title.as_deref(), Some("Queue title"));
        assert_eq!(provenance[0].source_ids, vec!["av123456789".to_string()]);
        assert_eq!(provenance[0].collection.as_ref(), Some(&collection));
    }

    #[test]
    fn round5_queue_provenance_survives_sidecar_edits_without_resurrecting_old_fields() {
        let root = TemporaryRoot::new("round5-queue-provenance");
        fs::write(root.path().join("clip.mp4"), b"fixture media")
            .expect("media fixture should write");
        let nfo_path = root.path().join("clip.nfo");
        fs::write(
            &nfo_path,
            "<movie><title>Sidecar One</title><uniqueid type=\"bilibili-cid\">111111111</uniqueid><set>Set One</set></movie>",
        )
        .expect("NFO fixture should write");
        let manager = manager(&root);
        let queue_collection = CollectionMetadata {
            id: Some("queue-season".to_string()),
            title: "Queue Season".to_string(),
            kind: "queue".to_string(),
            order: Some(3),
            part_id: None,
        };
        let queue_hint = SourceHint {
            source_ids: vec!["av123456789".to_string()],
            title: Some("Queue title".to_string()),
            relative_path: Some("clip.mp4".to_string()),
            collection: Some(queue_collection.clone()),
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };

        let first = manager
            .scan(&[queue_hint])
            .expect("initial scan should combine current sidecar and queue metadata");
        let first_item = item_at(&first, "clip.mp4");
        assert_eq!(first_item.title, "Sidecar One");
        assert!(first_item.source_ids.contains(&"cid111111111".to_string()));
        assert!(first_item.source_ids.contains(&"av123456789".to_string()));
        assert_eq!(first_item.collection.as_ref().unwrap().title, "Set One");

        fs::write(
            &nfo_path,
            "<movie><title>Sidecar Two</title><uniqueid type=\"bilibili-cid\">222222222</uniqueid><set>Set Two</set></movie>",
        )
        .expect("edited NFO fixture should write");
        let edited = manager
            .scan(&[])
            .expect("edited sidecar should replace prior sidecar-derived metadata");
        let edited_item = item_at(&edited, "clip.mp4");
        assert_eq!(edited_item.title, "Sidecar Two");
        assert!(edited_item.source_ids.contains(&"cid222222222".to_string()));
        assert!(!edited_item.source_ids.contains(&"cid111111111".to_string()));
        assert!(edited_item.source_ids.contains(&"av123456789".to_string()));
        assert_eq!(edited_item.collection.as_ref().unwrap().title, "Set Two");

        fs::remove_file(&nfo_path).expect("NFO fixture should be removable");
        let removed = manager
            .scan(&[])
            .expect("queue-origin metadata should survive removal of the NFO");
        let removed_item = item_at(&removed, "clip.mp4");
        assert_eq!(removed_item.title, "Queue title");
        assert_eq!(removed_item.source_ids, vec!["av123456789"]);
        assert_eq!(removed_item.collection.as_ref().unwrap(), &queue_collection);
    }

    #[test]
    fn round5_existing_directory_and_symlink_hint_paths_block_id_fallback() {
        let root = TemporaryRoot::new("round5-existing-hint-paths");
        fs::write(
            root.path().join("candidate-av123456789.mp4"),
            b"fixture media",
        )
        .expect("candidate media should write");
        fs::create_dir(root.path().join("existing-directory.mp4"))
            .expect("existing hint directory should create");
        symlink(
            "candidate-av123456789.mp4",
            root.path().join("existing-link.mp4"),
        )
        .expect("existing hint symlink should create");
        fs::write(
            root.path().join("existing-non-media.txt"),
            b"not the candidate",
        )
        .expect("existing non-media entry should write");

        let hints = [
            "existing-directory.mp4",
            "existing-link.mp4",
            "existing-non-media.txt",
        ]
        .into_iter()
        .map(|path| SourceHint {
            source_ids: vec!["av123456789".to_string()],
            title: Some("Must not attach".to_string()),
            relative_path: Some(path.to_string()),
            collection: None,
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        })
        .collect::<Vec<_>>();
        let snapshot = manager(&root)
            .scan(&hints)
            .expect("existing non-media paths should not fail the library scan");
        assert_eq!(
            item_at(&snapshot, "candidate-av123456789.mp4").title,
            "candidate-av123456789"
        );
    }

    #[test]
    fn round5_scanned_hint_path_binds_one_copy_and_missing_path_falls_back_uniquely() {
        let root = TemporaryRoot::new("round5-exclusive-hint-path");
        fs::write(root.path().join("selected-av123456789.mp4"), b"first copy")
            .expect("selected copy should write");
        fs::write(root.path().join("other-av123456789.mp4"), b"second copy")
            .expect("second copy should write");
        let selected_hint = SourceHint {
            source_ids: vec!["av123456789".to_string()],
            title: Some("Selected queue title".to_string()),
            relative_path: Some("selected-av123456789.mp4".to_string()),
            collection: None,
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let selected = manager(&root)
            .scan(&[selected_hint])
            .expect("exact durable path should resolve to one library copy");
        assert_eq!(
            item_at(&selected, "selected-av123456789.mp4").title,
            "Selected queue title"
        );
        assert_ne!(
            item_at(&selected, "other-av123456789.mp4").title,
            "Selected queue title",
            "a shared source ID must not spread an exact-path queue hint to another copy"
        );

        let unique_root = TemporaryRoot::new("round5-missing-hint-fallback");
        fs::write(
            unique_root.path().join("unique-av987654321.mp4"),
            b"unique copy",
        )
        .expect("unique copy should write");
        let missing_path_hint = SourceHint {
            source_ids: vec!["av987654321".to_string()],
            title: Some("Recovered queue title".to_string()),
            relative_path: Some("previous-location.mp4".to_string()),
            collection: None,
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let recovered = manager(&unique_root)
            .scan(&[missing_path_hint])
            .expect("confirmed absent path with a unique ID should recover the hint");
        assert_eq!(
            item_at(&recovered, "unique-av987654321.mp4").title,
            "Recovered queue title"
        );
    }

    #[test]
    fn round5_case_insensitive_existing_hint_alias_binds_only_when_identity_resolves() {
        let root = TemporaryRoot::new("round5-case-only-hint-alias");
        fs::write(root.path().join("Clip.mp4"), b"fixture media")
            .expect("media fixture should write");
        let directory = File::open(root.path()).expect("fixture root should open");
        let semantics = target_name_semantics(&directory)
            .expect("fixture filesystem name semantics should be available");
        let hint = SourceHint {
            source_ids: Vec::new(),
            title: Some("Queue alias title".to_string()),
            relative_path: Some("clip.mp4".to_string()),
            collection: None,
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let snapshot = manager(&root)
            .scan(&[hint])
            .expect("case-only alias scan should complete");
        if semantics.case_insensitive {
            assert_eq!(item_at(&snapshot, "Clip.mp4").title, "Queue alias title");
        } else {
            assert_eq!(item_at(&snapshot, "Clip.mp4").title, "Clip");
        }
    }

    #[test]
    fn first_scan_assigns_ids_to_media_without_prior_identity_candidates() {
        let root = TemporaryRoot::new("first-media-scan");
        fs::write(root.path().join("new-video.mp4"), b"fixture-video")
            .expect("media fixture should write");

        let snapshot = manager(&root)
            .scan(&[])
            .expect("first scan should not require prior identity candidates");

        assert_eq!(snapshot.items.len(), 1);
        assert_eq!(
            item_at(&snapshot, "new-video.mp4").presence,
            Presence::Present
        );
    }

    #[test]
    fn round10_unavailable_exact_path_preserves_stable_record_until_reopened_scan() {
        let root = TemporaryRoot::new("round10-unavailable-exact-path");
        let before = root.path().join("Before");
        let after = root.path().join("After");
        fs::create_dir(&before).expect("source directory should create");
        fs::create_dir(&after).expect("destination directory should create");
        fs::write(before.join("clip.mp4"), b"stable-media-object")
            .expect("media fixture should write");
        let media_path = after.join("clip.mp4");
        let provider_impl = Arc::new(FailOneMediaReadProvider::new(media_path.clone()));
        let provider: Arc<dyn QueueFileProvider> = provider_impl.clone();
        let manager = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should open against the injected provider");
        let hint = SourceHint {
            source_ids: vec!["youtube:abcdefghijk".to_string()],
            title: Some("Queue title".to_string()),
            relative_path: Some("Before/clip.mp4".to_string()),
            collection: None,
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let initial = manager
            .scan(&[hint])
            .expect("initial queue-aware scan should complete");
        let stable_id = item_at(&initial, "Before/clip.mp4").id.clone();

        fs::rename(before.join("clip.mp4"), &media_path)
            .expect("manual media rename should complete");
        let renamed = manager
            .scan(&[])
            .expect("renamed media should reconcile before the injected error");
        let renamed_item = item_at(&renamed, "After/clip.mp4");
        assert_eq!(renamed_item.id, stable_id);
        assert_eq!(renamed_item.title, "Queue title");

        provider_impl.fail_next_media_read();
        let unavailable = manager
            .scan(&[])
            .expect("a transient provider read error should keep the scan usable");
        assert_eq!(unavailable.items.len(), 1);
        let retained = item_at(&unavailable, "After/clip.mp4");
        assert_eq!(retained.id, stable_id);
        assert_eq!(retained.presence, Presence::Unavailable);
        assert_eq!(
            retained.metadata_status,
            MetadataStatus::ProviderUnavailable
        );
        assert_eq!(retained.title, "Queue title");
        assert_eq!(retained.source_ids, vec!["youtube:abcdefghijk"]);
        let unavailable_index = manager
            .read_index()
            .expect("unavailable stable record should remain indexed");
        assert_eq!(unavailable_index.records.len(), 1);
        let record = &unavailable_index.records[&stable_id];
        assert_eq!(record.item.relative_path, "After/clip.mp4");
        assert_eq!(record.item.presence, Presence::Unavailable);
        assert_eq!(record.missing_since, None);
        assert_eq!(record.queue_metadata.title.as_deref(), Some("Queue title"));
        drop(manager);

        let reopened_provider: Arc<dyn QueueFileProvider> = provider_impl;
        let reopened = LibraryManager::open_with_provider(root.path(), reopened_provider)
            .expect("library manager should reopen against the same provider");
        let recovered = reopened
            .scan(&[])
            .expect("a readable retry should restore the stable item");
        assert_eq!(recovered.items.len(), 1);
        let recovered_item = item_at(&recovered, "After/clip.mp4");
        assert_eq!(recovered_item.id, stable_id);
        assert_eq!(recovered_item.presence, Presence::Present);
        assert_eq!(recovered_item.title, "Queue title");
        assert_eq!(recovered_item.source_ids, vec!["youtube:abcdefghijk"]);
    }

    #[test]
    fn round10_unavailable_renamed_path_defers_history_without_binding_metadata() {
        let root = TemporaryRoot::new("round10-unavailable-renamed-path");
        let before = root.path().join("Before");
        let after = root.path().join("After");
        fs::create_dir(&before).expect("source directory should create");
        fs::create_dir(&after).expect("destination directory should create");
        fs::write(before.join("clip.mp4"), b"stable-media-object")
            .expect("media fixture should write");
        let media_path = after.join("clip.mp4");
        let provider_impl = Arc::new(FailOneMediaReadProvider::new(media_path.clone()));
        let provider: Arc<dyn QueueFileProvider> = provider_impl.clone();
        let manager = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should open against the injected provider");
        let hint = SourceHint {
            source_ids: vec!["youtube:abcdefghijk".to_string()],
            title: Some("Queue title".to_string()),
            relative_path: Some("Before/clip.mp4".to_string()),
            collection: None,
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let initial = manager
            .scan(&[hint])
            .expect("initial queue-aware scan should complete");
        let stable_id = item_at(&initial, "Before/clip.mp4").id.clone();
        fs::rename(before.join("clip.mp4"), &media_path)
            .expect("manual media rename should complete");

        provider_impl.fail_next_media_read();
        let unavailable = manager
            .scan(&[])
            .expect("an unreadable renamed item should defer old identity history");
        assert_eq!(unavailable.items.len(), 1);
        let provisional = item_at(&unavailable, "After/clip.mp4");
        assert_ne!(provisional.id, stable_id);
        assert_eq!(provisional.presence, Presence::Unavailable);
        assert_ne!(provisional.title, "Queue title");
        assert!(provisional.source_ids.is_empty());
        assert!(
            unavailable.warnings.iter().any(|warning| {
                warning.contains("prior identity history retained as unavailable")
            })
        );

        let deferred_index = manager
            .read_index()
            .expect("deferred prior identity should remain indexed");
        assert_eq!(deferred_index.records.len(), 1);
        let prior = &deferred_index.records[&stable_id];
        assert_eq!(prior.item.relative_path, "Before/clip.mp4");
        assert_eq!(prior.item.presence, Presence::Unavailable);
        assert_eq!(prior.missing_since, None);
        assert_eq!(prior.queue_metadata.title.as_deref(), Some("Queue title"));
        assert!(!deferred_index.records.contains_key(&provisional.id));
        drop(manager);

        let reopened_provider: Arc<dyn QueueFileProvider> = provider_impl;
        let reopened = LibraryManager::open_with_provider(root.path(), reopened_provider)
            .expect("library manager should reopen against the same provider");
        let recovered = reopened
            .scan(&[])
            .expect("a readable retry should reconcile the deferred identity");
        assert_eq!(recovered.items.len(), 1);
        let recovered_item = item_at(&recovered, "After/clip.mp4");
        assert_eq!(recovered_item.id, stable_id);
        assert_eq!(recovered_item.presence, Presence::Present);
        assert_eq!(recovered_item.title, "Queue title");
        assert_eq!(recovered_item.source_ids, vec!["youtube:abcdefghijk"]);
    }

    #[test]
    fn round10_unavailable_hardlink_aliases_defer_old_candidates_once_per_identity() {
        let root = TemporaryRoot::new("round10-unavailable-hardlink-aliases");
        let original_path = root.path().join("alias-a.mp4");
        fs::write(&original_path, b"stable-hardlink-object")
            .expect("primary hard-link fixture should write");
        for name in ["alias-b.mp4", "alias-c.mp4"] {
            fs::hard_link(&original_path, root.path().join(name))
                .expect("hard-link alias should create");
        }
        let provider_impl = Arc::new(FailOneMediaReadProvider::new(
            root.path().join("no-default-failure.mp4"),
        ));
        let provider: Arc<dyn QueueFileProvider> = provider_impl.clone();
        let manager = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should open against the injected provider");
        let initial = manager
            .scan(&[])
            .expect("initial hard-link scan should complete");
        let old_ids = ["alias-a.mp4", "alias-b.mp4", "alias-c.mp4"]
            .map(|path| item_at(&initial, path).id.clone());

        let old_names = ["alias-a.mp4", "alias-b.mp4", "alias-c.mp4"];
        let new_names = ["renamed-a.mp4", "renamed-b.mp4", "renamed-c.mp4"];
        for (old_name, new_name) in old_names.into_iter().zip(new_names) {
            fs::rename(root.path().join(old_name), root.path().join(new_name))
                .expect("hard-link alias rename should complete");
            provider_impl.fail_next_media_read_at(&root.path().join(new_name));
        }

        let steps = manager.track_next_scan_identity_reconciliation();
        let unavailable = manager
            .scan(&[])
            .expect("unreadable hard-link aliases should defer identity history");
        assert_eq!(unavailable.items.len(), 3);
        for name in new_names {
            let item = item_at(&unavailable, name);
            assert_eq!(item.presence, Presence::Unavailable);
            assert!(!old_ids.contains(&item.id));
        }
        assert_eq!(
            steps.load(Ordering::Relaxed),
            3,
            "the old candidate list should be traversed once for the shared inode, not once per alias"
        );

        let index = manager
            .read_index()
            .expect("prior hard-link identity history should remain indexed");
        assert_eq!(index.records.len(), 3);
        for old_id in old_ids {
            assert_eq!(index.records[&old_id].item.presence, Presence::Unavailable);
            assert_eq!(index.records[&old_id].missing_since, None);
        }
    }

    #[test]
    fn round10_unavailable_replacement_does_not_inherit_old_identity_or_queue_metadata() {
        let root = TemporaryRoot::new("round10-unavailable-replacement");
        let media_path = root.path().join("clip.mp4");
        let retained_old_object = root.path().join("old-object.bin");
        fs::write(&media_path, b"old-media-object").expect("media fixture should write");
        let provider_impl = Arc::new(FailOneMediaReadProvider::new(media_path.clone()));
        let provider: Arc<dyn QueueFileProvider> = provider_impl.clone();
        let manager = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should open against the injected provider");
        let hint = SourceHint {
            source_ids: vec!["youtube:abcdefghijk".to_string()],
            title: Some("Old queue title".to_string()),
            relative_path: Some("clip.mp4".to_string()),
            collection: None,
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let initial = manager
            .scan(&[hint])
            .expect("initial queue-aware scan should complete");
        let old_id = item_at(&initial, "clip.mp4").id.clone();

        fs::rename(&media_path, &retained_old_object)
            .expect("old object should remain allocated outside the media scan");
        fs::write(
            root.path().join("replacement.tmp"),
            b"replacement-media-object",
        )
        .expect("replacement object should write");
        fs::rename(root.path().join("replacement.tmp"), &media_path)
            .expect("replacement object should take the old path");
        provider_impl.fail_next_media_read();

        let snapshot = manager
            .scan(&[])
            .expect("an unreadable replacement should remain distinguishable");
        let old_record = snapshot
            .items
            .iter()
            .find(|item| item.id == old_id)
            .expect("old object history should remain visible");
        assert_eq!(old_record.presence, Presence::Missing);
        assert_eq!(old_record.title, "Old queue title");
        let replacement = snapshot
            .items
            .iter()
            .find(|item| item.relative_path == "clip.mp4" && item.id != old_id)
            .expect("replacement object should remain a separate unavailable item");
        assert_eq!(replacement.presence, Presence::Unavailable);
        assert_eq!(replacement.title, "clip");
        assert!(replacement.source_ids.is_empty());
        assert_ne!(replacement.id, old_id);
        let index = manager
            .read_index()
            .expect("old object identity should remain indexed");
        assert_eq!(index.records[&old_id].item.presence, Presence::Missing);
        assert_eq!(
            index.records[&old_id].queue_metadata.title.as_deref(),
            Some("Old queue title")
        );

        provider_impl.fail_next_media_read();
        let migration_scan = manager
            .migration_scan_with_completeness()
            .expect("migration scan should report the replacement read failure");
        assert!(!migration_scan.complete);
        assert!(migration_scan.incomplete_directories.contains("clip.mp4"));
        assert_eq!(
            item_at(&migration_scan.snapshot, "clip.mp4").presence,
            Presence::Unavailable
        );
        assert_eq!(
            migration_scan
                .snapshot
                .items
                .iter()
                .find(|item| item.id == old_id)
                .expect("old history should remain in the migration snapshot")
                .presence,
            Presence::Missing,
            "migration completeness must not rewrite the ordinary missing-history result"
        );
        assert!(!index.records.contains_key(&replacement.id));
    }

    #[test]
    fn round8_incomplete_directory_keeps_media_unavailable_and_complete_sibling_moves_sidecars() {
        let root = TemporaryRoot::new("round8-incomplete-attachments");
        let incomplete = root.path().join("Incomplete");
        let complete = root.path().join("Complete");
        fs::create_dir(&incomplete).expect("incomplete fixture directory should create");
        fs::create_dir(&complete).expect("complete fixture directory should create");
        fs::write(incomplete.join("old.mp4"), b"old-video")
            .expect("old media fixture should write");
        fs::write(incomplete.join("clip.mp4"), b"incomplete-video")
            .expect("incomplete media fixture should write");
        fs::write(
            incomplete.join("clip.nfo"),
            "<movie><title>Incomplete</title></movie>",
        )
        .expect("incomplete NFO fixture should write");
        fs::write(
            incomplete.join("clip.srt"),
            "1\n00:00:00,000 --> 00:00:01,000\ntext\n",
        )
        .expect("incomplete subtitle fixture should write");
        fs::write(complete.join("clip.mp4"), b"complete-video")
            .expect("complete media fixture should write");
        fs::write(
            complete.join("clip.nfo"),
            "<movie><title>Complete</title></movie>",
        )
        .expect("complete NFO fixture should write");
        fs::write(
            complete.join("clip.srt"),
            "1\n00:00:00,000 --> 00:00:01,000\ntext\n",
        )
        .expect("complete subtitle fixture should write");

        let manager = manager(&root);
        let initial = manager
            .scan(&[])
            .expect("initial complete scan should record both media items");
        let old_id = item_at(&initial, "Incomplete/old.mp4").id.clone();
        fs::remove_file(incomplete.join("old.mp4"))
            .expect("old media fixture should be absent from the next scan");
        manager.mark_next_scan_directory_incomplete("Incomplete");

        let snapshot = manager
            .scan(&[])
            .expect("partial directory scan should retain its known entries");
        let incomplete_item = item_at(&snapshot, "Incomplete/clip.mp4");
        assert_eq!(incomplete_item.presence, Presence::Unavailable);
        assert!(incomplete_item.attachments.is_empty());
        assert_eq!(item_at(&snapshot, "Incomplete/old.mp4").id, old_id);
        assert_eq!(
            item_at(&snapshot, "Incomplete/old.mp4").presence,
            Presence::Unavailable
        );
        assert!(
            snapshot
                .warnings
                .iter()
                .any(|warning| warning.contains("attachments were not asserted")),
            "the scan should explain why media in a partial directory is unavailable"
        );

        let unavailable_preview = manager.build_and_persist_preview(
            &snapshot,
            PreviewBuildRequest {
                item_ids: std::slice::from_ref(&incomplete_item.id),
                target_relative_dir: Some("Moved"),
                rename: false,
                conflict: ConflictPolicy::Skip,
                patches: &[],
            },
            &mut |target_path| manager.target_parent_device(target_path),
        );
        assert!(
            unavailable_preview.is_err(),
            "media from an incompletely enumerated attachment directory cannot be previewed"
        );

        let complete_item = item_at(&snapshot, "Complete/clip.mp4");
        assert_eq!(complete_item.presence, Presence::Present);
        assert_eq!(complete_item.attachments.len(), 2);
        let complete_preview = manager
            .build_and_persist_preview(
                &snapshot,
                PreviewBuildRequest {
                    item_ids: std::slice::from_ref(&complete_item.id),
                    target_relative_dir: Some("Moved"),
                    rename: false,
                    conflict: ConflictPolicy::Skip,
                    patches: &[],
                },
                &mut |target_path| manager.target_parent_device(target_path),
            )
            .expect("complete sibling directory should remain movable");
        assert_eq!(complete_preview.items[0].files.len(), 3);
        let moved_paths = complete_preview.items[0]
            .files
            .iter()
            .map(|file| file.target_path.clone())
            .collect::<Vec<_>>();
        let result = manager
            .execute_preview(&complete_preview.id, &complete_preview.revision)
            .expect("complete sibling move should execute");
        assert_eq!(result.status, BatchStatus::Complete);
        assert!(
            moved_paths
                .iter()
                .all(|path| root.path().join(path).is_file()),
            "the complete media, NFO, and subtitle should all move together"
        );
    }

    #[test]
    fn migration_scan_completeness_localizes_incomplete_directory() {
        let root = TemporaryRoot::new("migration-scan-completeness-directory");
        let incomplete = root.path().join("Incomplete");
        let complete = root.path().join("Complete");
        fs::create_dir(&incomplete).expect("incomplete fixture directory should create");
        fs::create_dir(&complete).expect("complete fixture directory should create");
        fs::write(incomplete.join("clip.mp4"), b"incomplete media")
            .expect("incomplete media fixture should write");
        fs::write(complete.join("clip.mp4"), b"complete media")
            .expect("complete media fixture should write");

        let manager = manager(&root);
        manager
            .scan(&[])
            .expect("baseline scan should index both media rows");
        manager.mark_next_scan_directory_incomplete("Incomplete");

        let result = manager
            .migration_scan_with_completeness()
            .expect("partial scan should return a bounded completeness proof");

        assert!(!result.complete);
        assert!(!result.scope_unknown);
        assert!(result.incomplete_directories.contains("Incomplete"));
        assert_eq!(
            item_at(&result.snapshot, "Incomplete/clip.mp4").presence,
            Presence::Unavailable
        );
        assert_eq!(
            item_at(&result.snapshot, "Complete/clip.mp4").presence,
            Presence::Present,
            "an unrelated fully enumerated subtree remains usable"
        );
    }

    #[test]
    fn migration_scan_completeness_localizes_entry_and_media_limits() {
        let root = TemporaryRoot::new("migration-scan-completeness-limits");
        let first = root.path().join("A");
        let second = root.path().join("B");
        fs::create_dir(&first).expect("first fixture directory should create");
        fs::create_dir(&second).expect("second fixture directory should create");
        fs::write(first.join("clip.mp4"), b"first media")
            .expect("first media fixture should write");
        fs::write(second.join("clip.mp4"), b"second media")
            .expect("second media fixture should write");

        let manager = manager(&root);
        manager
            .scan(&[])
            .expect("baseline scan should index both media rows");

        manager.limit_next_scan_entries_to(1);
        let entry_limited = manager
            .migration_scan_with_completeness()
            .expect("entry-limited scan should return its path proof");
        assert!(!entry_limited.complete);
        assert!(!entry_limited.scope_unknown);
        assert!(
            !entry_limited.incomplete_directories.is_empty(),
            "the directory containing entries past the lowered limit must be named"
        );

        manager
            .scan(&[])
            .expect("a full scan should restore the media rows before the next limit");
        manager.limit_next_scan_media_items_to(1);
        let media_limited = manager
            .migration_scan_with_completeness()
            .expect("media-limited scan should return its path proof");
        assert!(!media_limited.complete);
        assert!(!media_limited.scope_unknown);
        assert!(media_limited.incomplete_directories.contains("B/clip.mp4"));
        assert_eq!(
            item_at(&media_limited.snapshot, "A/clip.mp4").presence,
            Presence::Present
        );
        assert_eq!(
            item_at(&media_limited.snapshot, "B/clip.mp4").presence,
            Presence::Unavailable
        );
    }

    #[test]
    fn migration_scan_completeness_localizes_timeout_and_reports_unknown_scope() {
        let root = TemporaryRoot::new("migration-scan-completeness-timeout");
        let first = root.path().join("A");
        let second = root.path().join("B");
        fs::create_dir(&first).expect("first fixture directory should create");
        fs::create_dir(&second).expect("second fixture directory should create");
        fs::write(first.join("clip.mp4"), b"first media")
            .expect("first media fixture should write");
        fs::write(second.join("clip.mp4"), b"second media")
            .expect("second media fixture should write");

        let manager = manager(&root);
        manager
            .scan(&[])
            .expect("baseline scan should index both media rows");
        manager.expire_next_scan_after_built_item();
        let timed_out = manager
            .migration_scan_with_completeness()
            .expect("timed-out scan should return its path proof");
        assert!(!timed_out.complete);
        assert!(!timed_out.scope_unknown);
        assert!(timed_out.incomplete_directories.contains("B/clip.mp4"));
        assert_eq!(
            item_at(&timed_out.snapshot, "A/clip.mp4").presence,
            Presence::Present
        );
        assert_eq!(
            item_at(&timed_out.snapshot, "B/clip.mp4").presence,
            Presence::Unavailable
        );

        manager.mark_next_scan_scope_unknown();
        let unknown = manager
            .migration_scan_with_completeness()
            .expect("injected unlocalized interruption should return a proof");
        assert!(!unknown.complete);
        assert!(unknown.scope_unknown);
        assert!(unknown.incomplete_directories.is_empty());
    }

    #[test]
    fn migration_scan_completeness_blocks_provider_unavailable_paths() {
        let root = TemporaryRoot::new("migration-scan-completeness-provider");
        let category = root.path().join("Category");
        fs::create_dir(&category).expect("fixture category should create");
        let media_path = category.join("clip.mp4");
        let nfo_path = category.join("clip.nfo");
        fs::write(&media_path, b"fixture media").expect("media fixture should write");
        fs::write(&nfo_path, "<movie><title>Fixture</title></movie>")
            .expect("NFO fixture should write");
        let provider_impl = Arc::new(FailOneMediaReadProvider::new(media_path.clone()));
        let provider: Arc<dyn QueueFileProvider> = provider_impl.clone();
        let manager = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should open against the injected provider");
        manager
            .scan(&[])
            .expect("baseline scan should index the media and sidecar");

        provider_impl.fail_next_media_read();
        let unreadable_media = manager
            .migration_scan_with_completeness()
            .expect("provider media failure should return a partial scan");
        assert!(!unreadable_media.complete);
        assert!(!unreadable_media.scope_unknown);
        assert!(
            unreadable_media
                .incomplete_directories
                .contains("Category/clip.mp4")
        );
        assert_eq!(
            item_at(&unreadable_media.snapshot, "Category/clip.mp4").presence,
            Presence::Unavailable
        );

        manager
            .scan(&[])
            .expect("a readable scan should restore the metadata fixture");
        provider_impl.fail_next_media_read_at(&nfo_path);
        let unreadable_sidecar = manager
            .migration_scan_with_completeness()
            .expect("provider sidecar failure should return a partial scan");
        assert!(!unreadable_sidecar.complete);
        assert!(
            unreadable_sidecar
                .incomplete_directories
                .contains("Category/clip.nfo")
        );
        assert_eq!(
            item_at(&unreadable_sidecar.snapshot, "Category/clip.mp4").presence,
            Presence::Present,
            "metadata access failure must not be confused with the media itself disappearing"
        );

        manager
            .scan(&[])
            .expect("a readable scan should restore the sidecar state");
        provider_impl.fail_next_media_read_at(&category);
        let unreadable_directory = manager
            .migration_scan_with_completeness()
            .expect("provider directory failure should return a partial scan");
        assert!(!unreadable_directory.complete);
        assert!(!unreadable_directory.scope_unknown);
        assert!(
            unreadable_directory
                .incomplete_directories
                .contains("Category")
        );
        assert_eq!(
            item_at(&unreadable_directory.snapshot, "Category/clip.mp4").presence,
            Presence::Unavailable
        );
    }

    #[test]
    fn migration_scan_completeness_does_not_require_optional_metadata() {
        let root = TemporaryRoot::new("migration-scan-completeness-optional-metadata");
        fs::write(root.path().join("clip.mp4"), b"fixture media")
            .expect("media fixture should write");
        let manager = manager(&root);

        let result = manager
            .migration_scan_with_completeness()
            .expect("scan without optional metadata should complete");

        assert!(result.complete);
        assert!(!result.scope_unknown);
        assert!(result.incomplete_directories.is_empty());
        let item = item_at(&result.snapshot, "clip.mp4");
        assert_eq!(item.presence, Presence::Present);
        assert_eq!(item.confidence, Confidence::Low);
    }

    #[test]
    fn timeout_during_sidecar_loop_preserves_prior_metadata_until_full_rescan() {
        let root = TemporaryRoot::new("timeout-sidecar-preserves-prior-metadata");
        fs::write(root.path().join("clip.mp4"), b"stable clip media")
            .expect("clip media fixture should write");
        fs::write(root.path().join("z-later.mp4"), b"later media")
            .expect("later media fixture should write");
        let manager = manager(&root);
        let prior_collection = CollectionMetadata {
            id: Some("queue-season".to_string()),
            title: "Queue collection".to_string(),
            kind: "queue".to_string(),
            order: Some(7),
            part_id: Some("queue-part".to_string()),
        };
        let hints = vec![
            SourceHint {
                source_ids: vec!["youtube:abcdefghijk".to_string()],
                title: Some("Queue clip title".to_string()),
                relative_path: Some("clip.mp4".to_string()),
                collection: Some(prior_collection.clone()),
                origin: HintOrigin::Queue,
                requires_confirmation: false,
            },
            SourceHint {
                source_ids: vec!["av123456789".to_string()],
                title: Some("Queue later title".to_string()),
                relative_path: Some("z-later.mp4".to_string()),
                collection: None,
                origin: HintOrigin::Queue,
                requires_confirmation: false,
            },
        ];
        let initial = manager
            .scan(&hints)
            .expect("initial queue-aware scan should complete");
        let prior_clip = item_at(&initial, "clip.mp4").clone();
        let prior_later = item_at(&initial, "z-later.mp4").clone();
        assert_eq!(prior_clip.title, "Queue clip title");
        assert_eq!(prior_clip.collection, Some(prior_collection.clone()));
        assert_eq!(prior_clip.source_ids, vec!["youtube:abcdefghijk"]);

        fs::write(
            root.path().join("clip.json"),
            r#"{"title":"Partial JSON title","bvid":"BV1xx411c7mD","ugc_season":{"id":99,"title":"Partial collection"}}"#,
        )
        .expect("earlier-sorting JSON sidecar should write");
        fs::write(
            root.path().join("clip.nfo"),
            "<movie><title>Fresh NFO title</title><uniqueid type=\"bilibili-cid\">222222222</uniqueid><setid>fresh-season</setid><set>Fresh collection</set><settype>season</settype><episode>2</episode></movie>",
        )
        .expect("later-sorting NFO sidecar should write");
        manager.expire_next_scan_after_attachments(1);

        let partial = manager
            .scan(&[])
            .expect("an attachment-loop timeout should publish a usable partial snapshot");
        assert!(partial.warnings.iter().any(|warning| {
            warning.contains("120-second scan budget")
                && warning.contains("unvisited paths were not marked missing")
        }));
        let timed_out_clip = item_at(&partial, "clip.mp4");
        assert_eq!(timed_out_clip.id, prior_clip.id);
        assert_eq!(timed_out_clip.presence, Presence::Unavailable);
        assert_eq!(timed_out_clip.title, prior_clip.title);
        assert_eq!(timed_out_clip.source_ids, prior_clip.source_ids);
        assert_eq!(timed_out_clip.collection, prior_clip.collection);
        assert_eq!(timed_out_clip.attachments, prior_clip.attachments);

        let unvisited = item_at(&partial, "z-later.mp4");
        assert_eq!(unvisited.id, prior_later.id);
        assert_eq!(unvisited.presence, Presence::Unavailable);
        assert_eq!(unvisited.title, prior_later.title);
        assert_eq!(unvisited.source_ids, prior_later.source_ids);
        assert_eq!(unvisited.collection, prior_later.collection);

        let index = manager
            .read_index()
            .expect("timed-out scan should leave a readable index");
        let clip_record = &index.records[&prior_clip.id];
        assert_eq!(clip_record.item.title, prior_clip.title);
        assert_eq!(clip_record.item.source_ids, prior_clip.source_ids);
        assert_eq!(clip_record.item.collection, prior_clip.collection);
        assert_eq!(clip_record.item.presence, Presence::Unavailable);
        assert_eq!(
            clip_record.queue_metadata.title.as_deref(),
            Some("Queue clip title")
        );
        assert_eq!(clip_record.queue_metadata.source_ids, prior_clip.source_ids);
        assert_eq!(
            clip_record.queue_metadata.collection,
            Some(prior_collection)
        );

        let preview = manager.build_and_persist_preview(
            &partial,
            PreviewBuildRequest {
                item_ids: std::slice::from_ref(&timed_out_clip.id),
                target_relative_dir: Some("Moved"),
                rename: false,
                conflict: ConflictPolicy::Skip,
                patches: &[],
            },
            &mut |target_path| manager.target_parent_device(target_path),
        );
        assert!(
            preview.is_err(),
            "a timed-out item must not be editable through a move preview"
        );
        assert_eq!(persisted_preview_count(&manager), 0);

        let recovered = manager
            .scan(&[])
            .expect("a later complete scan should process all sidecars");
        let recovered_clip = item_at(&recovered, "clip.mp4");
        assert_eq!(recovered_clip.id, prior_clip.id);
        assert_eq!(recovered_clip.presence, Presence::Present);
        assert_eq!(recovered_clip.title, "Fresh NFO title");
        assert!(
            recovered_clip
                .source_ids
                .contains(&"cid222222222".to_string())
        );
        assert_eq!(
            recovered_clip
                .collection
                .as_ref()
                .map(|value| value.title.as_str()),
            Some("Fresh collection")
        );
    }

    #[test]
    fn timeout_after_final_attachment_does_not_publish_new_media() {
        let root = TemporaryRoot::new("timeout-final-attachment-new-media");
        fs::write(root.path().join("new.mp4"), b"new media")
            .expect("new media fixture should write");
        fs::write(
            root.path().join("new.nfo"),
            "<movie><title>New complete title</title><uniqueid type=\"bilibili-cid\">333333333</uniqueid><set>New collection</set></movie>",
        )
        .expect("sole NFO attachment should write");
        let manager = manager(&root);
        manager.expire_next_scan_after_attachments(1);

        let timed_out = manager
            .scan(&[])
            .expect("a timeout after the final attachment should leave a usable snapshot");
        assert!(timed_out.warnings.iter().any(|warning| {
            warning.contains("120-second scan budget")
                && warning.contains("unvisited paths were not marked missing")
        }));
        assert!(
            timed_out.items.is_empty(),
            "a first-seen item that hit the scan deadline must not be published"
        );
        assert!(
            manager
                .read_index()
                .expect("timed-out scan should leave a readable index")
                .records
                .is_empty(),
            "a partial first-seen item must not become a durable index record"
        );
        assert_eq!(persisted_preview_count(&manager), 0);

        let complete = manager
            .scan(&[])
            .expect("the next complete scan should discover the media normally");
        let item = item_at(&complete, "new.mp4");
        assert_eq!(item.presence, Presence::Present);
        assert_eq!(item.title, "New complete title");
        assert!(item.source_ids.contains(&"cid333333333".to_string()));
        assert_eq!(
            item.collection.as_ref().map(|value| value.title.as_str()),
            Some("New collection")
        );
    }

    #[test]
    fn round8_timeout_after_build_reuses_rename_identity_without_duplicate_history() {
        let root = TemporaryRoot::new("round8-timeout-after-build");
        let original_path = root.path().join("old-name.mp4");
        let renamed_path = root.path().join("new-name.mp4");
        fs::write(&original_path, b"same-media-object").expect("media fixture should write");
        let manager = manager(&root);
        let initial = manager.scan(&[]).expect("initial scan should complete");
        let stable_id = item_at(&initial, "old-name.mp4").id.clone();
        fs::rename(&original_path, &renamed_path).expect("manual rename should complete");
        manager.expire_next_scan_after_built_item();

        let after_timeout = manager
            .scan(&[])
            .expect("a timeout after building the descriptor should still reconcile it");
        assert_eq!(after_timeout.items.len(), 1);
        let renamed = item_at(&after_timeout, "new-name.mp4");
        assert_eq!(renamed.id, stable_id);
        assert_eq!(renamed.presence, Presence::Present);

        let rescanned = manager
            .scan(&[])
            .expect("subsequent complete scan should keep the reconciled identity");
        assert_eq!(rescanned.items.len(), 1);
        assert_eq!(item_at(&rescanned, "new-name.mp4").id, stable_id);
    }

    #[test]
    fn round8_timeout_hardlink_alias_in_unbuilt_tail_prevents_unique_rename_match() {
        let root = TemporaryRoot::new("round8-timeout-hardlink-tail");
        let old_path = root.path().join("old-name.mp4");
        let renamed_path = root.path().join("a-renamed.mp4");
        let alias_path = root.path().join("z-alias.mp4");
        fs::write(&old_path, b"same-media-object").expect("media fixture should write");
        let manager = manager(&root);
        let initial = manager.scan(&[]).expect("initial scan should complete");
        let old_id = item_at(&initial, "old-name.mp4").id.clone();
        fs::rename(&old_path, &renamed_path).expect("manual rename should complete");
        fs::hard_link(&renamed_path, &alias_path).expect("hardlink alias should create");
        manager.expire_next_scan_after_built_item();

        let partial = manager
            .scan(&[])
            .expect("partial scan should reconcile only its built item");
        let renamed = item_at(&partial, "a-renamed.mp4");
        assert_ne!(renamed.id, old_id);
        assert_eq!(renamed.presence, Presence::Present);
        let old_record = partial
            .items
            .iter()
            .find(|item| item.id == old_id)
            .expect("unreconciled old identity should remain in history");
        assert_eq!(old_record.presence, Presence::Unavailable);
        assert!(
            !partial
                .items
                .iter()
                .any(|item| item.relative_path == "z-alias.mp4"),
            "the unbuilt tail must not be published as a confirmed media item"
        );

        manager.expire_next_scan_after_built_item();
        let migration_scan = manager
            .migration_scan_with_completeness()
            .expect("migration scan should preserve timeout scope without changing presence");
        assert!(!migration_scan.complete);
        assert!(!migration_scan.scope_unknown);
        assert!(
            migration_scan
                .incomplete_directories
                .contains("z-alias.mp4")
        );
        let migration_renamed = item_at(&migration_scan.snapshot, "a-renamed.mp4");
        assert_ne!(
            migration_renamed.id, old_id,
            "the unbuilt hard-link alias must still prevent old identity reuse"
        );
        assert_eq!(
            migration_renamed.presence,
            Presence::Present,
            "the built item remains present while only the unbuilt tail is migration-incomplete"
        );
        assert_eq!(
            migration_scan
                .snapshot
                .items
                .iter()
                .find(|item| item.id == old_id)
                .expect("old identity history should remain visible")
                .presence,
            Presence::Unavailable
        );
    }

    #[test]
    fn round9_partial_rename_preserves_old_identity_until_complete_reconciliation() {
        let root = TemporaryRoot::new("round9-partial-rename-identity");
        let before = root.path().join("Before");
        let after = root.path().join("After");
        fs::create_dir(&before).expect("source directory should create");
        fs::create_dir(&after).expect("destination directory should create");
        fs::create_dir(root.path().join("Interrupted"))
            .expect("incomplete directory fixture should create");
        fs::write(before.join("clip.mp4"), b"stable-media-object")
            .expect("media fixture should write");
        let hint = SourceHint {
            source_ids: vec!["abcdefghijk".to_string()],
            title: Some("Queue title".to_string()),
            relative_path: Some("Before/clip.mp4".to_string()),
            collection: Some(CollectionMetadata {
                id: Some("playlist-9".to_string()),
                title: "Queue collection".to_string(),
                kind: "youtube_playlist".to_string(),
                order: Some(9),
                part_id: None,
            }),
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let initial_manager = manager(&root);
        let initial = initial_manager
            .scan(std::slice::from_ref(&hint))
            .expect("initial queue-aware scan should complete");
        let stable_id = item_at(&initial, "Before/clip.mp4").id.clone();
        fs::rename(before.join("clip.mp4"), after.join("clip.mp4"))
            .expect("manual media move should complete");
        initial_manager.mark_next_scan_directory_incomplete("Interrupted");

        let partial = initial_manager
            .scan(&[])
            .expect("partial scan should remain useful while deferring identity");
        let provisional = item_at(&partial, "After/clip.mp4");
        assert_eq!(provisional.presence, Presence::Unavailable);
        assert_ne!(provisional.id, stable_id);
        assert!(
            partial
                .warnings
                .iter()
                .any(|warning| { warning.contains("complete scan can confirm a unique move") })
        );
        assert_eq!(partial.items.len(), 1);

        let partial_index = initial_manager
            .read_index()
            .expect("deferred scan should preserve its prior index");
        assert_eq!(partial_index.records.len(), 1);
        let prior = &partial_index.records[&stable_id];
        assert_eq!(prior.item.relative_path, "Before/clip.mp4");
        assert_eq!(prior.item.presence, Presence::Unavailable);
        assert_eq!(prior.missing_since, None);
        assert_eq!(prior.queue_metadata.title.as_deref(), Some("Queue title"));
        assert_eq!(prior.queue_metadata.source_ids, vec!["youtube:abcdefghijk"]);
        assert_eq!(
            prior
                .queue_metadata
                .collection
                .as_ref()
                .map(|value| value.title.as_str()),
            Some("Queue collection")
        );
        assert!(
            !partial_index.records.contains_key(&provisional.id),
            "a provisional path-derived ID must not become authoritative"
        );
        drop(initial_manager);

        let reopened = manager(&root);
        let complete = reopened
            .scan(&[])
            .expect("complete scan should reconcile the retained identity");
        assert_eq!(complete.items.len(), 1);
        let moved = item_at(&complete, "After/clip.mp4");
        assert_eq!(moved.id, stable_id);
        assert_eq!(moved.presence, Presence::Present);
        assert_eq!(moved.title, "Queue title");
        assert_eq!(moved.source_ids, vec!["youtube:abcdefghijk"]);
        assert_eq!(
            moved.collection.as_ref().map(|value| value.title.as_str()),
            Some("Queue collection")
        );
        let complete_index = reopened
            .read_index()
            .expect("reconciled index should remain readable");
        assert_eq!(complete_index.records.len(), 1);
        assert_eq!(
            complete_index.records[&stable_id].item.relative_path,
            "After/clip.mp4"
        );
    }

    #[test]
    fn round9_partial_rename_does_not_demote_a_later_exact_hardlink_alias() {
        let root = TemporaryRoot::new("round9-partial-exact-alias");
        let exact_path = root.path().join("clip.mp4");
        let renamed_path = root.path().join("a-renamed.mp4");
        fs::create_dir(root.path().join("Interrupted"))
            .expect("incomplete directory fixture should create");
        fs::write(&exact_path, b"stable-hardlink-object").expect("media fixture should write");
        let manager = manager(&root);
        let initial = manager.scan(&[]).expect("initial scan should complete");
        let stable_id = item_at(&initial, "clip.mp4").id.clone();
        fs::rename(&exact_path, &renamed_path).expect("manual rename should complete");
        fs::hard_link(&renamed_path, &exact_path).expect("exact-path alias should create");
        manager.mark_next_scan_directory_incomplete("Interrupted");

        let partial = manager
            .scan(&[])
            .expect("partial scan should conservatively retain both hard-link names");
        let exact = item_at(&partial, "clip.mp4");
        let renamed = item_at(&partial, "a-renamed.mp4");
        assert_eq!(exact.id, stable_id);
        assert_eq!(exact.presence, Presence::Present);
        assert_eq!(renamed.presence, Presence::Unavailable);
        let index = manager
            .read_index()
            .expect("partial alias scan should persist readable index state");
        assert_eq!(index.records[&stable_id].item.relative_path, "clip.mp4");
        assert_eq!(index.records[&stable_id].item.presence, Presence::Present);
    }

    #[test]
    fn round9_complete_exact_hardlink_aliases_skip_identity_candidate_walk() {
        let root = TemporaryRoot::new("round9-exact-alias-candidate-walk");
        let primary_path = root.path().join("alias-a.mp4");
        fs::write(&primary_path, b"stable-hardlink-object")
            .expect("primary hard-link fixture should write");
        for name in ["alias-b.mp4", "alias-c.mp4"] {
            fs::hard_link(&primary_path, root.path().join(name))
                .expect("hard-link alias should create");
        }

        let manager = manager(&root);
        let initial = manager.scan(&[]).expect("initial scan should complete");
        let initial_ids = ["alias-a.mp4", "alias-b.mp4", "alias-c.mp4"]
            .map(|path| item_at(&initial, path).id.clone());

        let steps = manager.track_next_scan_identity_reconciliation();
        let rescanned = manager
            .scan(&[])
            .expect("complete exact-alias scan should succeed");

        assert_eq!(
            steps.load(Ordering::Relaxed),
            0,
            "exact path-and-identity matches must not traverse the identity candidate list"
        );
        for (index, path) in ["alias-a.mp4", "alias-b.mp4", "alias-c.mp4"]
            .into_iter()
            .enumerate()
        {
            assert_eq!(item_at(&rescanned, path).id, initial_ids[index]);
            assert_eq!(item_at(&rescanned, path).presence, Presence::Present);
        }
    }

    #[test]
    fn round9_partial_hardlink_alias_candidates_are_walked_and_deferred_once() {
        const ALIAS_COUNT: usize = 3;

        let root = TemporaryRoot::new("round9-partial-alias-candidate-cache");
        fs::create_dir(root.path().join("Interrupted"))
            .expect("incomplete directory fixture should create");
        let primary_path = root.path().join("old-alias-a.mp4");
        fs::write(&primary_path, b"stable-hardlink-object")
            .expect("primary hard-link fixture should write");
        for name in ["old-alias-b.mp4", "old-alias-c.mp4"] {
            fs::hard_link(&primary_path, root.path().join(name))
                .expect("hard-link alias should create");
        }

        let hint = SourceHint {
            source_ids: vec!["youtube:abcdefghijk".to_string()],
            title: Some("Queue-only title".to_string()),
            relative_path: Some("old-alias-a.mp4".to_string()),
            collection: None,
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let manager = manager(&root);
        let initial = manager
            .scan(&[hint])
            .expect("initial queue-aware hard-link scan should complete");
        let original_ids = ["old-alias-a.mp4", "old-alias-b.mp4", "old-alias-c.mp4"]
            .map(|path| item_at(&initial, path).id.clone());

        for (old_name, new_name) in [
            ("old-alias-a.mp4", "new-alias-a.mp4"),
            ("old-alias-b.mp4", "new-alias-b.mp4"),
            ("old-alias-c.mp4", "new-alias-c.mp4"),
        ] {
            fs::rename(root.path().join(old_name), root.path().join(new_name))
                .expect("hard-link alias rename should complete");
        }
        manager.mark_next_scan_directory_incomplete("Interrupted");
        let steps = manager.track_next_scan_identity_reconciliation();

        let partial = manager
            .scan(&[])
            .expect("partial hard-link scan should remain usable");

        assert_eq!(partial.items.len(), ALIAS_COUNT);
        for path in ["new-alias-a.mp4", "new-alias-b.mp4", "new-alias-c.mp4"] {
            assert_eq!(item_at(&partial, path).presence, Presence::Unavailable);
        }
        assert_eq!(
            steps.load(Ordering::Relaxed),
            2 * ALIAS_COUNT,
            "the old candidate list and deferred IDs should each be processed once per identity"
        );

        let index = manager
            .read_index()
            .expect("partial alias index should remain readable");
        assert_eq!(index.records.len(), ALIAS_COUNT);
        assert_eq!(
            index.records[&original_ids[0]]
                .queue_metadata
                .title
                .as_deref(),
            Some("Queue-only title"),
            "deferred identity reconciliation must preserve queue provenance"
        );
        for id in &original_ids {
            assert_eq!(index.records[id].item.presence, Presence::Unavailable);
        }
    }

    #[test]
    fn unobserved_records_are_unavailable_for_incomplete_or_unreadable_scans() {
        let mut walk = ScanWalk::default();
        walk.complete_directories.insert(String::new());
        walk.incomplete_scan = true;

        assert_eq!(
            unobserved_record_status("old-video.mp4", &walk, None),
            Some(UnobservedRecordStatus {
                presence: Presence::Unavailable,
                metadata_status: None,
            }),
            "a media-count limit is incomplete, not a timeout or proof of missing files"
        );
        assert!(!walk.timed_out);

        walk.incomplete_scan = false;
        assert_eq!(
            unobserved_record_status("old-video.mp4", &walk, Some(MetadataStatus::Unreadable)),
            Some(UnobservedRecordStatus {
                presence: Presence::Unavailable,
                metadata_status: Some(MetadataStatus::Unreadable),
            }),
            "an enumerated media item whose build failed is unavailable, not missing"
        );
        assert_eq!(
            unobserved_record_status("old-video.mp4", &walk, None),
            Some(UnobservedRecordStatus {
                presence: Presence::Missing,
                metadata_status: None,
            }),
            "only a complete scan may mark an unobserved item missing"
        );
    }

    #[test]
    fn missing_index_records_are_count_bounded_and_keep_recent_identity() {
        let root = TemporaryRoot::new("missing-index-bound");
        let manager = manager(&root);
        let mut index = LibraryIndex {
            version: INDEX_VERSION,
            records: BTreeMap::new(),
        };
        for number in 0..=MAX_MISSING_INDEX_RECORDS {
            let id = format!("missing-{number:04}");
            index.records.insert(
                id.clone(),
                index_fixture_record(
                    &id,
                    &format!("deleted-{number:04}.mp4"),
                    Presence::Missing,
                    number as u64 + 10,
                    number as u64 + 20,
                    Some(number as u64 + 1),
                ),
            );
        }
        assert!(
            manager
                .write_index(&index, INDEX_LIMIT)
                .expect("large but bounded fixture index should persist")
        );

        let snapshot = manager
            .scan(&[])
            .expect("scan should prune tombstones before the index size limit");
        let updated = manager.read_index().expect("pruned index should read");
        assert_eq!(updated.records.len(), MAX_MISSING_INDEX_RECORDS);
        assert_eq!(snapshot.items.len(), MAX_MISSING_INDEX_RECORDS);
        assert!(!updated.records.contains_key("missing-0000"));
        assert!(!snapshot.items.iter().any(|item| item.id == "missing-0000"));
        let newest = updated
            .records
            .get(&format!("missing-{:04}", MAX_MISSING_INDEX_RECORDS))
            .expect("newest missing identity should remain recoverable");
        assert_eq!(newest.device, MAX_MISSING_INDEX_RECORDS as u64 + 10);
        assert_eq!(newest.inode, MAX_MISSING_INDEX_RECORDS as u64 + 20);
        assert_eq!(
            newest.missing_since,
            Some(MAX_MISSING_INDEX_RECORDS as u64 + 1)
        );
    }

    #[test]
    fn missing_index_byte_budget_prunes_oldest_records_and_preserves_old_schema() {
        let mut index = LibraryIndex {
            version: INDEX_VERSION,
            records: BTreeMap::new(),
        };
        index.records.insert(
            "old".to_string(),
            index_fixture_record("old", "old-deleted.mp4", Presence::Missing, 2, 20, Some(1)),
        );
        index.records.insert(
            "new".to_string(),
            index_fixture_record("new", "new-deleted.mp4", Presence::Missing, 3, 30, Some(2)),
        );
        index.records.insert(
            "unavailable".to_string(),
            index_fixture_record(
                "unavailable",
                "unavailable.mp4",
                Presence::Unavailable,
                4,
                40,
                None,
            ),
        );
        let mut fits_with_newest = index.clone();
        fits_with_newest.records.remove("old");
        let byte_budget = serde_json::to_vec(&fits_with_newest).unwrap().len();

        let pruned = prune_missing_index_records(&mut index, 10, byte_budget)
            .expect("index should fit by pruning missing records");
        assert_eq!(pruned, vec!["old".to_string()]);
        assert!(index.records.contains_key("new"));
        assert!(index.records.contains_key("unavailable"));
        assert!(serde_json::to_vec(&index).unwrap().len() <= byte_budget);
        assert_eq!(index.records["new"].device, 3);
        assert_eq!(index.records["new"].inode, 30);

        let mut old_schema = serde_json::to_value(index).unwrap();
        old_schema["records"]["new"]
            .as_object_mut()
            .unwrap()
            .remove("missing_since");
        let decoded: LibraryIndex = serde_json::from_value(old_schema)
            .expect("index records without missing_since must remain readable");
        assert_eq!(decoded.records["new"].missing_since, None);
    }

    #[test]
    fn round9_present_index_compaction_drops_only_rebuildable_attachments() {
        let mut record =
            index_fixture_record("present", "clip.mp4", Presence::Present, 7, 70, None);
        record.item.attachments.push(LibraryAttachment {
            relative_path: "clip.en.srt".to_string(),
            kind: "subtitle".to_string(),
        });
        record.queue_metadata = QueueMetadataProvenance {
            title: Some("Queue title".to_string()),
            source_ids: vec!["youtube:abcdefghijk".to_string()],
            collection: Some(CollectionMetadata {
                id: Some("playlist-7".to_string()),
                title: "Queue collection".to_string(),
                kind: "youtube_playlist".to_string(),
                order: Some(7),
                part_id: None,
            }),
            requires_confirmation: false,
        };
        let mut index = LibraryIndex {
            version: INDEX_VERSION,
            records: BTreeMap::from([("present".to_string(), record)]),
        };

        let compact = compact_index_for_persistence(&index);
        assert_eq!(index.records["present"].item.attachments.len(), 1);
        assert!(compact.records["present"].item.attachments.is_empty());
        assert_eq!(compact.records["present"].item.id, "present");
        assert_eq!(compact.records["present"].item.relative_path, "clip.mp4");
        assert_eq!(compact.records["present"].device, 7);
        assert_eq!(compact.records["present"].inode, 70);
        assert_eq!(
            compact.records["present"].queue_metadata.title.as_deref(),
            Some("Queue title")
        );
        assert_eq!(
            compact.records["present"].queue_metadata.source_ids,
            vec!["youtube:abcdefghijk"]
        );
        assert!(
            serde_json::to_vec(&compact).unwrap().len() < serde_json::to_vec(&index).unwrap().len()
        );

        index.records.get_mut("present").unwrap().item.presence = Presence::Unavailable;
        let unavailable = compact_index_for_persistence(&index);
        assert_eq!(unavailable.records["present"].item.attachments.len(), 1);
    }

    #[test]
    fn round9_oversized_index_keeps_previous_bytes_and_publishes_full_snapshot() {
        let root = TemporaryRoot::new("round9-index-budget-fallback");
        fs::write(root.path().join("clip.mp4"), b"fixture media")
            .expect("media fixture should write");
        fs::write(
            root.path().join("clip.srt"),
            "1\n00:00:00,000 --> 00:00:01,000\nsubtitle\n",
        )
        .expect("subtitle fixture should write");
        let hint = SourceHint {
            source_ids: vec!["abcdefghijk".to_string()],
            title: Some("Queue title".to_string()),
            relative_path: Some("clip.mp4".to_string()),
            collection: Some(CollectionMetadata {
                id: Some("playlist-7".to_string()),
                title: "Queue collection".to_string(),
                kind: "youtube_playlist".to_string(),
                order: Some(7),
                part_id: None,
            }),
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let manager = manager(&root);
        let initial = manager
            .scan(std::slice::from_ref(&hint))
            .expect("initial scan should persist a valid index");
        let initial_item = item_at(&initial, "clip.mp4");
        assert!(
            initial_item
                .attachments
                .iter()
                .any(|file| file.relative_path == "clip.srt")
        );
        let index_path = root.path().join(LIBRARY_DIRECTORY).join(LIBRARY_INDEX);
        let previous_bytes = fs::read(&index_path).expect("persisted index should be readable");
        let small_budget = previous_bytes.len() - 1;

        manager.limit_next_scan_index_to(small_budget);
        let partial = manager
            .scan(&[])
            .expect("an oversized rebuildable cache should not fail the scan");
        let item = item_at(&partial, "clip.mp4");
        assert!(
            item.attachments
                .iter()
                .any(|file| file.relative_path == "clip.srt")
        );
        assert_eq!(item.title, "Queue title");
        assert_eq!(item.source_ids, vec!["youtube:abcdefghijk"]);
        assert_eq!(
            item.collection.as_ref().map(|value| value.title.as_str()),
            Some("Queue collection")
        );
        assert!(partial.warnings.iter().any(|warning| {
            warning.contains("previous valid index was kept")
                && warning.contains(
                    "identity and queue association changes from this scan are not durable",
                )
        }));
        assert_eq!(fs::read(&index_path).unwrap(), previous_bytes);
        let preserved = manager
            .read_index()
            .expect("previous index should remain valid");
        assert_eq!(preserved.records.len(), 1);
        assert_eq!(
            preserved
                .records
                .values()
                .next()
                .unwrap()
                .queue_metadata
                .title
                .as_deref(),
            Some("Queue title")
        );

        manager.limit_next_scan_index_to(small_budget);
        let repeated = manager
            .scan(&[])
            .expect("repeated over-budget scans should remain usable");
        assert!(
            repeated
                .warnings
                .iter()
                .any(|warning| { warning.contains("previous valid index was kept") })
        );
        assert_eq!(fs::read(&index_path).unwrap(), previous_bytes);
    }

    #[test]
    fn scan_uses_real_root_directories_and_distinguishes_hardlink_copies() {
        let root = TemporaryRoot::new("categories-hardlinks");
        fs::create_dir(root.path().join("Empty collection"))
            .expect("empty public category should create");
        fs::create_dir(root.path().join(".telegram-video-downloader-staging"))
            .expect("private staging fixture should create");
        fs::write(root.path().join("standalone.mp4"), b"fixture-video")
            .expect("root media fixture should write");
        fs::hard_link(
            root.path().join("standalone.mp4"),
            root.path().join("hardlink-copy.mp4"),
        )
        .expect("hard-link copy should create");
        fs::write(
            root.path()
                .join(".telegram-video-downloader-staging/hidden.mp4"),
            b"private",
        )
        .expect("private media fixture should write");
        let outside = TemporaryRoot::new("symlink-target");
        fs::write(outside.path().join("outside.mp4"), b"outside")
            .expect("outside target fixture should write");
        symlink(
            outside.path().join("outside.mp4"),
            root.path().join("escape.mp4"),
        )
        .expect("root symlink fixture should create");

        let snapshot = manager(&root)
            .scan(&[])
            .expect("library scan should complete");
        assert_eq!(snapshot.items.len(), 2);
        assert_ne!(
            item_at(&snapshot, "standalone.mp4").id,
            item_at(&snapshot, "hardlink-copy.mp4").id,
            "two hard-link paths must be separate physical library copies"
        );
        assert_eq!(snapshot.categories, vec!["Empty collection"]);
        assert!(
            snapshot
                .items
                .iter()
                .all(|item| !item.relative_path.contains("escape"))
        );
        assert!(
            snapshot
                .items
                .iter()
                .all(|item| !item.relative_path.contains("staging"))
        );
    }

    #[test]
    fn scan_rediscovers_a_finder_moved_media_and_preserves_its_copy_id() {
        let root = TemporaryRoot::new("finder-move");
        fs::create_dir(root.path().join("Before")).expect("source folder should create");
        fs::create_dir(root.path().join("After")).expect("destination folder should create");
        fs::write(
            root.path().join("Before/episode_BV1abcdefgh.mp4"),
            b"fixture-video",
        )
        .expect("media fixture should write");
        fs::write(
            root.path().join("Before/episode_BV1abcdefgh.nfo"),
            "<movie><title>Episode</title><uniqueid type=\"bilibili\">BV1abcdefgh</uniqueid><future>opaque</future></movie>",
        )
        .expect("NFO fixture should write");
        let manager = manager(&root);
        let before = manager.scan(&[]).expect("initial scan should complete");
        let old_id = item_at(&before, "Before/episode_BV1abcdefgh.mp4")
            .id
            .clone();
        fs::rename(
            root.path().join("Before/episode_BV1abcdefgh.mp4"),
            root.path().join("After/episode_BV1abcdefgh.mp4"),
        )
        .expect("Finder-style media move should complete");
        fs::rename(
            root.path().join("Before/episode_BV1abcdefgh.nfo"),
            root.path().join("After/episode_BV1abcdefgh.nfo"),
        )
        .expect("Finder-style NFO move should complete");

        let after = manager.scan(&[]).expect("rescan should complete");
        assert_eq!(item_at(&after, "After/episode_BV1abcdefgh.mp4").id, old_id);
        assert!(
            item_at(&after, "After/episode_BV1abcdefgh.mp4")
                .source_ids
                .contains(&"BV1abcdefgh".to_string())
        );
    }

    #[test]
    fn queue_only_metadata_survives_move_reopen_and_rescan_but_not_path_replacement() {
        let root = TemporaryRoot::new("queue-metadata-move");
        fs::create_dir(root.path().join("Before")).expect("source directory should create");
        fs::write(root.path().join("Before/clip.mp4"), b"fixture-video")
            .expect("media fixture should write");
        let hint = SourceHint {
            source_ids: vec!["abcdefghijk".to_string()],
            title: Some("Queue title".to_string()),
            relative_path: Some("Before/clip.mp4".to_string()),
            collection: Some(CollectionMetadata {
                id: Some("playlist-7".to_string()),
                title: "Queue collection".to_string(),
                kind: "youtube_playlist".to_string(),
                order: Some(7),
                part_id: None,
            }),
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };
        let initial_manager = manager(&root);
        let before = initial_manager
            .scan(std::slice::from_ref(&hint))
            .expect("queue-aware initial scan should complete");
        let original = item_at(&before, "Before/clip.mp4").clone();
        assert_eq!(original.source_ids, vec!["youtube:abcdefghijk".to_string()]);

        let preview = initial_manager
            .preview(
                std::slice::from_ref(&original.id),
                "Moved",
                false,
                ConflictPolicy::KeepBoth,
            )
            .expect("move should preview");
        let moved_relative_path = preview.items[0].target_path.clone();
        assert_eq!(
            initial_manager
                .execute_preview(&preview.id, &preview.revision)
                .expect("media move should complete")
                .status,
            BatchStatus::Complete
        );
        drop(initial_manager);

        let reopened = manager(&root);
        let moved = reopened
            .scan(std::slice::from_ref(&hint))
            .expect("reopened manager should recover metadata by object identity");
        let moved_item = item_at(&moved, &moved_relative_path);
        assert_eq!(moved_item.id, original.id);
        assert_eq!(moved_item.title, "Queue title");
        assert_eq!(moved_item.source_ids, original.source_ids);
        assert_eq!(moved_item.collection, original.collection);

        let rescanned = reopened
            .scan(&[])
            .expect("subsequent scan should retain persisted queue metadata");
        let rescanned_item = item_at(&rescanned, &moved_relative_path);
        assert_eq!(rescanned_item.id, original.id);
        assert_eq!(rescanned_item.title, "Queue title");
        assert_eq!(rescanned_item.source_ids, original.source_ids);
        assert_eq!(rescanned_item.collection, original.collection);

        let moved_absolute_path = root.path().join(&moved_relative_path);
        let replacement = moved_absolute_path
            .parent()
            .expect("target should have a parent folder")
            .join(".replacement");
        fs::write(&replacement, b"replacement-video")
            .expect("replacement media should write while original still exists");
        fs::rename(&replacement, &moved_absolute_path)
            .expect("replacement media should atomically reuse the old path");
        let after_replacement = reopened
            .scan(std::slice::from_ref(&hint))
            .expect("replacement path should scan");
        let replacement_item = after_replacement
            .items
            .iter()
            .find(|item| {
                item.relative_path == moved_relative_path && item.presence == Presence::Present
            })
            .expect("replacement media should be present");
        assert_ne!(replacement_item.id, original.id);
        assert_eq!(replacement_item.title, "clip");
        assert!(replacement_item.source_ids.is_empty());
        assert_eq!(replacement_item.collection, None);
    }

    #[test]
    fn legacy_import_candidates_include_queue_hinted_snapshot_metadata() {
        let root = TemporaryRoot::new("legacy-import-queue-snapshot");
        fs::write(root.path().join("episode.mp4"), b"fixture-video")
            .expect("media fixture should write");
        let manager = manager(&root);
        let hint = SourceHint {
            source_ids: vec!["BV1abcdefgh".to_string()],
            title: Some("Queued episode".to_string()),
            relative_path: Some("episode.mp4".to_string()),
            collection: None,
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };

        let report = manager
            .import_legacy_with_hints(
                "Episode One - https://www.bilibili.com/video/BV1abcdefgh",
                &[hint],
            )
            .expect("queue-aware legacy import should use the same scan snapshot");
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.candidates[0].match_kind, "source_id");
        assert_eq!(
            report.candidates[0].item_id,
            item_at(
                &manager.scan(&[]).expect("follow-up scan should complete"),
                "episode.mp4"
            )
            .id
        );
    }

    #[test]
    fn legacy_import_title_only_hint_can_be_manually_matched_to_a_local_item() {
        let root = TemporaryRoot::new("legacy-title-only-candidate");
        fs::write(root.path().join("Inception.mp4"), b"fixture-video")
            .expect("media fixture should write");
        let manager = manager(&root);

        let report = manager
            .import_legacy("Title: Inception")
            .expect("title-only legacy text should parse and match against the scan snapshot");

        assert_eq!(report.hints.len(), 1);
        assert!(report.hints[0].source_ids.is_empty());
        assert_eq!(report.hints[0].title.as_deref(), Some("Inception"));
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.candidates[0].hint_index, 0);
        assert_eq!(report.candidates[0].match_kind, "title");
        assert!(report.candidates[0].requires_confirmation);
        assert_eq!(
            report.candidates[0].item_id,
            item_at(
                &manager.scan(&[]).expect("follow-up scan should complete"),
                "Inception.mp4"
            )
            .id
        );
    }

    #[test]
    fn legacy_import_normalizes_title_whitespace_before_candidate_matching() {
        let root = TemporaryRoot::new("legacy-title-whitespace-candidate");
        fs::write(root.path().join("Star Wars.mp4"), b"fixture-video")
            .expect("media fixture should write");
        let manager = manager(&root);

        let report = manager
            .import_legacy("Title: Star\tWars")
            .expect("tab-separated title hint should be normalized before matching");

        assert_eq!(report.hints.len(), 1);
        assert_eq!(report.hints[0].title.as_deref(), Some("Star Wars"));
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.candidates[0].match_kind, "title");
        assert!(report.candidates[0].requires_confirmation);
        assert_eq!(report.candidates[0].title, "Star Wars");
    }

    #[test]
    fn legacy_candidates_only_match_present_items_by_source_id_or_title() {
        let mut present_direct =
            index_fixture_item("present-direct", "direct.mp4", Presence::Present);
        present_direct.title = "Direct title".to_string();
        present_direct.source_ids = vec!["BV1abcdefgh".to_string(), "BV1abcdefgh".to_string()];

        let mut missing_direct =
            index_fixture_item("missing-direct", "missing-direct.mp4", Presence::Missing);
        missing_direct.source_ids = vec!["BV1abcdefgh".to_string()];
        let mut unavailable_direct = index_fixture_item(
            "unavailable-direct",
            "unavailable-direct.mp4",
            Presence::Unavailable,
        );
        unavailable_direct.source_ids = vec!["BV1abcdefgh".to_string()];

        let mut missing_title =
            index_fixture_item("missing-title", "missing-title.mp4", Presence::Missing);
        missing_title.title = "Fallback title".to_string();
        missing_title.source_ids = vec!["cid1234".to_string()];
        let mut unavailable_title = index_fixture_item(
            "unavailable-title",
            "unavailable-title.mp4",
            Presence::Unavailable,
        );
        unavailable_title.title = "Fallback title".to_string();
        unavailable_title.source_ids = vec!["cid1234".to_string()];
        let mut present_title =
            index_fixture_item("present-title", "present-title.mp4", Presence::Present);
        present_title.title = "Fallback title".to_string();

        let mut missing_only_title =
            index_fixture_item("missing-only-title", "missing-only.mp4", Presence::Missing);
        missing_only_title.title = "Stale title".to_string();
        let mut unavailable_only_title = index_fixture_item(
            "unavailable-only-title",
            "unavailable-only.mp4",
            Presence::Unavailable,
        );
        unavailable_only_title.title = "Stale title".to_string();

        let items = vec![
            present_direct,
            missing_direct,
            unavailable_direct,
            missing_title,
            unavailable_title,
            present_title,
            missing_only_title,
            unavailable_only_title,
        ];
        let hints = vec![
            SourceHint {
                source_ids: vec!["BV1abcdefgh".to_string()],
                title: Some("Does not match the direct item".to_string()),
                relative_path: None,
                collection: None,
                origin: HintOrigin::LegacyText,
                requires_confirmation: true,
            },
            SourceHint {
                source_ids: vec!["cid1234".to_string()],
                title: Some("Fallback title".to_string()),
                relative_path: None,
                collection: None,
                origin: HintOrigin::LegacyText,
                requires_confirmation: true,
            },
            SourceHint {
                source_ids: Vec::new(),
                title: Some("Stale title".to_string()),
                relative_path: None,
                collection: None,
                origin: HintOrigin::LegacyText,
                requires_confirmation: true,
            },
        ];

        let candidates = legacy_candidates(&hints, &items, DEFAULT_LEGACY_CANDIDATE_LIMITS)
            .expect("legacy candidates should serialize")
            .candidates;
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| (
                    candidate.hint_index,
                    candidate.item_id.as_str(),
                    candidate.match_kind.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![
                (0, "present-direct", "source_id"),
                (1, "present-title", "title"),
            ],
            "non-present records must neither become candidates nor shadow a present title fallback"
        );
    }

    #[test]
    fn legacy_candidates_preserve_multi_id_order_priority_and_title_ambiguity() {
        let mut unavailable =
            index_fixture_item("unavailable", "unavailable.mp4", Presence::Unavailable);
        unavailable.title = "Shared title".to_string();
        unavailable.source_ids = vec!["cid1234".to_string()];

        let mut first = index_fixture_item("first", "first.mp4", Presence::Present);
        first.title = "Shared title".to_string();
        first.source_ids = vec![
            "cid1234".to_string(),
            "cid2345".to_string(),
            "cid1234".to_string(),
        ];

        let mut second = index_fixture_item("second", "second.mp4", Presence::Present);
        second.title = "Shared title".to_string();
        second.source_ids = vec!["cid2345".to_string()];

        let mut title_only = index_fixture_item("title-only", "title-only.mp4", Presence::Present);
        title_only.title = "Shared title".to_string();

        let items = vec![unavailable, first, second, title_only];
        let hints = vec![
            SourceHint {
                source_ids: vec!["cid2345".to_string(), "cid1234".to_string()],
                title: Some("Shared title".to_string()),
                relative_path: None,
                collection: None,
                origin: HintOrigin::LegacyText,
                requires_confirmation: true,
            },
            SourceHint {
                source_ids: Vec::new(),
                title: Some("Shared title".to_string()),
                relative_path: None,
                collection: None,
                origin: HintOrigin::LegacyText,
                requires_confirmation: true,
            },
        ];

        let matches = legacy_candidates(&hints, &items, DEFAULT_LEGACY_CANDIDATE_LIMITS)
            .expect("legacy candidates should serialize");
        assert_eq!(
            matches
                .candidates
                .iter()
                .map(|candidate| (
                    candidate.hint_index,
                    candidate.item_id.as_str(),
                    candidate.match_kind.as_str(),
                    candidate.ambiguous,
                ))
                .collect::<Vec<_>>(),
            vec![
                (0, "first", "source_id", true),
                (0, "second", "source_id", true),
                (1, "first", "title", true),
                (1, "second", "title", true),
                (1, "title-only", "title", true),
            ],
            "source-ID unions should deduplicate and retain library order, while direct matches outrank title matches"
        );
        assert_eq!(matches.truncation, None);
    }

    #[test]
    fn legacy_candidate_matching_work_scales_with_index_and_matches() {
        const ITEM_COUNT: usize = 1_000;
        let items = (0..ITEM_COUNT)
            .map(|index| {
                let mut item = index_fixture_item(
                    &format!("item-{index}"),
                    &format!("item-{index}.mp4"),
                    Presence::Present,
                );
                item.title = format!("Title {index}");
                item.source_ids = vec![format!("cid{}", 10_000 + index)];
                item
            })
            .collect::<Vec<_>>();
        let hints = (0..ITEM_COUNT)
            .map(|index| SourceHint {
                source_ids: if index % 2 == 0 {
                    vec![format!("cid{}", 10_000 + index)]
                } else {
                    Vec::new()
                },
                title: Some(format!("Title {index}")),
                relative_path: None,
                collection: None,
                origin: HintOrigin::LegacyText,
                requires_confirmation: true,
            })
            .collect::<Vec<_>>();

        let matches = legacy_candidates(&hints, &items, DEFAULT_LEGACY_CANDIDATE_LIMITS)
            .expect("legacy candidates should serialize");
        assert_eq!(matches.candidates.len(), ITEM_COUNT);
        assert_eq!(matches.truncation, None);
        assert_eq!(
            matches.work,
            LegacyCandidateWork {
                present_items_indexed: ITEM_COUNT,
                item_title_normalizations: ITEM_COUNT,
                source_id_index_entries: ITEM_COUNT,
                source_id_lookups: ITEM_COUNT / 2,
                title_index_lookups: ITEM_COUNT / 2,
                match_entries_visited: ITEM_COUNT,
                hint_title_normalizations: ITEM_COUNT / 2,
                hint_source_id_values_normalized: ITEM_COUNT / 2,
            }
        );
        let lookup_and_match_visits = matches.work.source_id_lookups
            + matches.work.title_index_lookups
            + matches.work.match_entries_visited;
        assert!(
            lookup_and_match_visits < ITEM_COUNT * ITEM_COUNT,
            "matching work should follow the index and actual matches, not hints multiplied by items"
        );
    }

    #[test]
    fn legacy_candidate_count_budget_keeps_prior_hints_and_warns() {
        let mut first_item = index_fixture_item("first", "first.mp4", Presence::Present);
        first_item.source_ids = vec!["cid1234".to_string()];
        let mut second_item = index_fixture_item("second", "second.mp4", Presence::Present);
        second_item.source_ids = vec!["cid2345".to_string()];
        let items = vec![first_item, second_item];
        let hints = vec![
            SourceHint {
                source_ids: vec!["cid1234".to_string()],
                title: None,
                relative_path: None,
                collection: None,
                origin: HintOrigin::LegacyText,
                requires_confirmation: true,
            },
            SourceHint {
                source_ids: vec!["cid2345".to_string()],
                title: None,
                relative_path: None,
                collection: None,
                origin: HintOrigin::LegacyText,
                requires_confirmation: true,
            },
        ];
        let mut report = LegacyImportReport {
            hints,
            warnings: Vec::new(),
            needs_confirmation: true,
            revision: None,
            candidates: Vec::new(),
        };

        populate_legacy_candidates(
            &mut report,
            &items,
            LegacyCandidateLimits {
                max_candidates: 1,
                max_serialized_array_bytes: 512 * 1024,
                max_match_visits: 100,
            },
        )
        .expect("bounded candidates should serialize");

        assert_eq!(
            report.hints.len(),
            2,
            "all imported hints must remain available"
        );
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.candidates[0].hint_index, 0);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("1-candidate limit"))
        );
        assert_eq!(DEFAULT_LEGACY_CANDIDATE_LIMITS.max_candidates, 10_000);
    }

    #[test]
    fn legacy_candidate_byte_budget_counts_serialized_array_and_keeps_hint_whole() {
        let mut first_item = index_fixture_item("first", "first.mp4", Presence::Present);
        first_item.source_ids = vec!["cid1234".to_string()];
        let mut second_item = index_fixture_item("second", "second.mp4", Presence::Present);
        second_item.source_ids = vec!["cid1234".to_string()];
        let items = vec![first_item, second_item];
        let hints = vec![SourceHint {
            source_ids: vec!["cid1234".to_string()],
            title: Some("not the matching title".to_string()),
            relative_path: None,
            collection: None,
            origin: HintOrigin::LegacyText,
            requires_confirmation: true,
        }];
        let first_match = legacy_candidates(
            &hints,
            &items,
            LegacyCandidateLimits {
                max_candidates: 10,
                max_serialized_array_bytes: 512 * 1024,
                max_match_visits: 100,
            },
        )
        .expect("unbounded-by-test-limit match should serialize")
        .candidates
        .remove(0);
        let one_candidate_array_bytes = serde_json::to_vec(&vec![first_match])
            .expect("candidate array should serialize")
            .len();
        assert_eq!(
            DEFAULT_LEGACY_CANDIDATE_LIMITS.max_serialized_array_bytes,
            512 * 1024
        );

        let mut report = LegacyImportReport {
            hints,
            warnings: Vec::new(),
            needs_confirmation: true,
            revision: None,
            candidates: Vec::new(),
        };
        populate_legacy_candidates(
            &mut report,
            &items,
            LegacyCandidateLimits {
                max_candidates: 10,
                max_serialized_array_bytes: one_candidate_array_bytes,
                max_match_visits: 100,
            },
        )
        .expect("bounded candidates should serialize");

        assert!(
            report.candidates.is_empty(),
            "an incomplete hint must not expose a partial choice set"
        );
        assert_eq!(report.hints.len(), 1);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("serialized array limit"))
        );
    }

    #[test]
    fn legacy_candidate_match_visit_budget_skips_incomplete_hint() {
        let ids = vec![
            "cid1000".to_string(),
            "cid2000".to_string(),
            "cid3000".to_string(),
            "cid4000".to_string(),
        ];
        let items = (0..4)
            .map(|index| {
                let mut item = index_fixture_item(
                    &format!("item-{index}"),
                    &format!("item-{index}.mp4"),
                    Presence::Present,
                );
                item.source_ids = ids.clone();
                item
            })
            .collect::<Vec<_>>();
        let hints = vec![SourceHint {
            source_ids: ids,
            title: Some("Must not fall back".to_string()),
            relative_path: None,
            collection: None,
            origin: HintOrigin::LegacyText,
            requires_confirmation: true,
        }];
        let mut report = LegacyImportReport {
            hints,
            warnings: Vec::new(),
            needs_confirmation: true,
            revision: None,
            candidates: Vec::new(),
        };

        populate_legacy_candidates(
            &mut report,
            &items,
            LegacyCandidateLimits {
                max_candidates: 10,
                max_serialized_array_bytes: 512 * 1024,
                max_match_visits: 10,
            },
        )
        .expect("bounded candidates should serialize");

        assert_eq!(report.hints.len(), 1);
        assert!(
            report.candidates.is_empty(),
            "an incomplete source-ID union must not become a partial or title fallback result"
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("10 lookup/match-visit limit"))
        );

        let matches = legacy_candidates(
            &report.hints,
            &items,
            LegacyCandidateLimits {
                max_candidates: 10,
                max_serialized_array_bytes: 512 * 1024,
                max_match_visits: 10,
            },
        )
        .expect("bounded matching should complete with a warning");
        assert_eq!(matches.work.match_entries_visited, 8);
        assert_eq!(matches.work.source_id_lookups, 2);
        assert_eq!(
            matches.truncation,
            Some(LegacyCandidateTruncation::MatchVisits)
        );
    }

    #[test]
    fn round7_legacy_text_collector_is_linear_bounded_and_utf8_safe() {
        let mut values = Vec::with_capacity(50_000);
        values.extend((0..25_000).map(|_| serde_json::Value::String(String::new())));
        values.extend((0..25_000).map(|_| serde_json::Value::String("x".to_string())));
        let input = serde_json::Value::Array(values);
        let mut collector = LegacyTextCollector::default();
        collect_legacy_text(&input, &mut collector, 10);

        assert_eq!(collector.bytes_used, 10);
        assert_eq!(collector.blocks.len(), 10);
        assert_eq!(collector.blocks.concat(), "x".repeat(10));
        assert_eq!(
            collector.visited_values,
            1 + 25_000 + 10,
            "the collector should stop visiting entries as soon as its byte budget is full"
        );

        let input = serde_json::json!("a界🙂z");
        let mut collector = LegacyTextCollector::default();
        collect_legacy_text(&input, &mut collector, 5);
        assert_eq!(collector.bytes_used, 4);
        assert_eq!(collector.blocks, ["a界"]);
    }

    #[test]
    fn round7_legacy_title_hints_use_the_shared_unicode_scalar_limit() {
        for scalar in ["a", "界", "🙂"] {
            let accepted = scalar.repeat(MAX_METADATA_TITLE_CHARS);
            let trimmed_line = format!("Title:  {accepted}  ");
            assert_eq!(
                title_hint_from_line(&trimmed_line).as_deref(),
                Some(accepted.as_str()),
                "500 trimmed Unicode scalar values should be accepted"
            );

            let rejected = scalar.repeat(MAX_METADATA_TITLE_CHARS + 1);
            assert_eq!(
                title_hint_from_line(&format!("Title: {rejected}")),
                None,
                "501 Unicode scalar values should be rejected"
            );
        }

        let cjk_title = "界".repeat(167);
        assert_eq!(cjk_title.len(), 501);
        assert!(metadata_title_within_limits(&cjk_title));
        assert_eq!(
            title_hint_from_line(&format!("Title: {cjk_title}")),
            Some(cjk_title)
        );
    }

    #[test]
    fn legacy_title_hints_normalize_whitespace_and_reject_controls() {
        assert_eq!(
            title_hint_from_line("Title:  Star\t\t Wars  ").as_deref(),
            Some("Star Wars")
        );
        assert_eq!(
            title_hint_from_line("Star\tWars - cid12345678").as_deref(),
            Some("Star Wars")
        );

        let json = serde_json::json!({
            "messages": [{ "text": "Title: 星河\t战士🙂" }]
        })
        .to_string();
        let json_report = parse_legacy_import(&json);
        assert_eq!(json_report.hints.len(), 1);
        assert_eq!(json_report.hints[0].title.as_deref(), Some("星河 战士🙂"));
        assert_eq!(json_report.hints[0].origin, HintOrigin::LegacyTelegramJson);
        assert!(json_report.warnings.is_empty());

        let exactly_500 = format!("{}\tb", "a".repeat(498));
        let expected_500 = format!("{} b", "a".repeat(498));
        assert_eq!(
            title_hint_from_line(&format!("Title: {exactly_500}")).as_deref(),
            Some(expected_500.as_str()),
            "the normalized title should be measured against the shared scalar limit"
        );
        let normalized_501 = format!("{}\tb", "a".repeat(499));
        assert_eq!(
            title_hint_from_line(&format!("Title: {normalized_501}")),
            None
        );

        assert_eq!(title_hint_from_line("Title: Bad\u{0001}Name"), None);
        assert!(!metadata_title_within_limits("Star\tWars"));
        assert!(!metadata_title_within_limits("Bad\u{0001}Name"));
        assert!(metadata_title_within_limits("星河 战士🙂"));
    }

    #[test]
    fn legacy_import_splits_lf_crlf_and_bare_cr_without_merging_hints() {
        let report = parse_legacy_import(
            "Title: First\rBV1abcdefgh\r\nTitle: 星河🙂\ncid12345678\rThird - ep123456789",
        );

        assert_eq!(report.hints.len(), 5);
        assert_eq!(report.hints[0].title.as_deref(), Some("First"));
        assert!(report.hints[0].source_ids.is_empty());
        assert_eq!(report.hints[1].title, None);
        assert_eq!(report.hints[1].source_ids, ["BV1abcdefgh"]);
        assert_eq!(report.hints[2].title.as_deref(), Some("星河🙂"));
        assert!(report.hints[2].source_ids.is_empty());
        assert_eq!(report.hints[3].title, None);
        assert_eq!(report.hints[3].source_ids, ["cid12345678"]);
        assert_eq!(report.hints[4].title.as_deref(), Some("Third"));
        assert_eq!(report.hints[4].source_ids, ["ep123456789"]);
        assert!(report.warnings.is_empty());
    }

    #[test]
    fn round7_metadata_patch_validation_uses_scalar_limits_for_titles() {
        for scalar in ["a", "界", "🙂"] {
            let accepted_title = scalar.repeat(MAX_METADATA_TITLE_CHARS);
            let patch = MetadataPatch {
                item_id: "fixture-item".to_string(),
                title: Some(accepted_title),
                source_ids: Vec::new(),
                collection: None,
                fill_missing_only: false,
                organization_status: None,
            };
            assert!(validate_metadata_patch(&patch).is_ok());

            let rejected_title = scalar.repeat(MAX_METADATA_TITLE_CHARS + 1);
            let patch = MetadataPatch {
                item_id: "fixture-item".to_string(),
                title: Some(rejected_title),
                source_ids: Vec::new(),
                collection: None,
                fill_missing_only: false,
                organization_status: None,
            };
            assert!(validate_metadata_patch(&patch).is_err());

            let accepted_collection = MetadataPatch {
                item_id: "fixture-item".to_string(),
                title: None,
                source_ids: Vec::new(),
                collection: Some(CollectionMetadata {
                    id: None,
                    title: scalar.repeat(MAX_METADATA_TITLE_CHARS),
                    kind: "season".to_string(),
                    order: None,
                    part_id: None,
                }),
                fill_missing_only: false,
                organization_status: None,
            };
            assert!(validate_metadata_patch(&accepted_collection).is_ok());

            let rejected_collection = MetadataPatch {
                item_id: "fixture-item".to_string(),
                title: None,
                source_ids: Vec::new(),
                collection: Some(CollectionMetadata {
                    id: None,
                    title: scalar.repeat(MAX_METADATA_TITLE_CHARS + 1),
                    kind: "season".to_string(),
                    order: None,
                    part_id: None,
                }),
                fill_missing_only: false,
                organization_status: None,
            };
            assert!(validate_metadata_patch(&rejected_collection).is_err());
        }

        let control_character_title = MetadataPatch {
            item_id: "fixture-item".to_string(),
            title: Some("Star\tWars".to_string()),
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        assert!(validate_metadata_patch(&control_character_title).is_err());
    }

    #[test]
    fn round11_preview_checks_attachment_devices_before_persisting() {
        let root = TemporaryRoot::new("round11-attachment-device");
        fs::write(root.path().join("clip.mp4"), b"fixture-video")
            .expect("source media should write");
        fs::write(root.path().join("clip.srt"), b"subtitle")
            .expect("subtitle attachment should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("library scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let device = fs::metadata(root.path())
            .expect("fixture root metadata should read")
            .dev();
        let rejected = manager.preview_with_target_device_resolver(
            std::slice::from_ref(&item_id),
            "New/Nested",
            false,
            ConflictPolicy::KeepBoth,
            |target| {
                if target.ends_with("clip.srt") {
                    Ok(device ^ 1)
                } else {
                    Ok(device)
                }
            },
        );
        let error = rejected.expect_err("cross-device attachment should reject the preview");
        assert!(
            error
                .to_string()
                .contains("cross-device moves are unsupported")
        );
        assert_eq!(persisted_preview_count(&manager), 0);
        assert!(!root.path().join("New").exists());
        assert_eq!(
            fs::read(root.path().join("clip.mp4")).unwrap(),
            b"fixture-video"
        );
        assert_eq!(fs::read(root.path().join("clip.srt")).unwrap(), b"subtitle");

        let preview = manager
            .preview(
                std::slice::from_ref(&item_id),
                "New/Nested",
                false,
                ConflictPolicy::KeepBoth,
            )
            .expect("same-device files under not-yet-created parents should preview");
        assert_eq!(preview.items[0].outcome, MoveOutcome::Move);
        assert_eq!(preview.items[0].files.len(), 2);
        assert!(
            preview.items[0]
                .files
                .iter()
                .any(|file| file.target_path == "New/Nested/clip.mp4")
        );
        assert!(
            preview.items[0]
                .files
                .iter()
                .any(|file| file.target_path == "New/Nested/clip.srt")
        );
        assert!(!root.path().join("New").exists());
        release_preview_anchors(&preview.id);
    }

    #[test]
    fn round11_mixed_device_batch_is_rejected_before_preview_persistence() {
        let root = TemporaryRoot::new("round11-mixed-device-batch");
        fs::write(root.path().join("first.mp4"), b"first source")
            .expect("first source should write");
        fs::write(root.path().join("second.mp4"), b"second source")
            .expect("second source should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("library scan should complete");
        let ids = [
            item_at(&snapshot, "first.mp4").id.clone(),
            item_at(&snapshot, "second.mp4").id.clone(),
        ];
        let device = fs::metadata(root.path())
            .expect("fixture root metadata should read")
            .dev();
        let error = manager
            .preview_with_target_device_resolver(
                &ids,
                "Archive/Nested",
                false,
                ConflictPolicy::KeepBoth,
                |target| {
                    if target.ends_with("second.mp4") {
                        Ok(device ^ 1)
                    } else {
                        Ok(device)
                    }
                },
            )
            .expect_err("one cross-device item should reject the whole batch preview");
        assert!(error.to_string().contains("second.mp4"));
        assert_eq!(persisted_preview_count(&manager), 0);
        assert!(!root.path().join("Archive").exists());
        assert_eq!(
            fs::read(root.path().join("first.mp4")).unwrap(),
            b"first source"
        );
        assert_eq!(
            fs::read(root.path().join("second.mp4")).unwrap(),
            b"second source"
        );
    }

    #[test]
    fn round11_skip_and_noop_previews_do_not_resolve_a_rename_device() {
        let root = TemporaryRoot::new("round11-skip-noop-device");
        fs::write(root.path().join("clip.mp4"), b"source").expect("source media should write");
        fs::create_dir(root.path().join("Existing")).expect("target directory should create");
        fs::write(root.path().join("Existing/clip.mp4"), b"existing target")
            .expect("conflicting target should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("library scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();

        let skipped = manager
            .preview_with_target_device_resolver(
                std::slice::from_ref(&item_id),
                "Existing",
                false,
                ConflictPolicy::Skip,
                |_| panic!("skipped item should not resolve a rename target device"),
            )
            .expect("skip policy should remain a valid preview");
        assert_eq!(skipped.items[0].outcome, MoveOutcome::Skip);

        let noop = manager
            .preview_with_target_device_resolver(
                std::slice::from_ref(&item_id),
                "",
                false,
                ConflictPolicy::KeepBoth,
                |_| panic!("no-op item should not resolve a rename target device"),
            )
            .expect("same-path no-op should remain a valid preview");
        assert_eq!(noop.items[0].outcome, MoveOutcome::NoOp);
        assert_eq!(noop.items[0].target_path, "clip.mp4");
        release_preview_anchors(&skipped.id);
        release_preview_anchors(&noop.id);
    }

    #[test]
    fn preview_has_no_media_side_effects_and_conflicts_are_explicit() {
        let root = TemporaryRoot::new("preview-conflicts");
        fs::write(root.path().join("clip.mp4"), b"fixture-video")
            .expect("source media should write");
        fs::create_dir(root.path().join("Existing")).expect("conflict directory should create");
        fs::write(root.path().join("Existing/clip.mp4"), b"keep this target")
            .expect("conflict target should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let source_before = fs::read(root.path().join("clip.mp4")).expect("source should read");
        let target_before = fs::read(root.path().join("Existing/clip.mp4"))
            .expect("existing destination should read");

        let skip = manager
            .preview(
                std::slice::from_ref(&item_id),
                "Existing",
                false,
                ConflictPolicy::Skip,
            )
            .expect("skip preview should be available");
        assert_eq!(skip.items[0].outcome, MoveOutcome::Skip);
        let skipped = manager
            .execute_preview(&skip.id, &skip.revision)
            .expect("a skipped batch should persist its conflict");
        assert_eq!(skipped.status, BatchStatus::Failed);
        assert!(skipped.items[0].error.is_some());

        let batch_path = manager.batch_path(&skip.id);
        let mut old_manifest: BatchManifest = serde_json::from_slice(
            &fs::read(&batch_path).expect("persisted skip manifest should read"),
        )
        .expect("persisted skip manifest should parse");
        old_manifest.status = BatchStatus::Complete;
        manager
            .write_batch_manifest(&batch_path, &old_manifest)
            .expect("legacy false-complete manifest should be restorable");
        let retried = manager
            .execute_preview(&skip.id, &skip.revision)
            .expect("a legacy false-complete skip must be re-evaluated");
        assert_eq!(retried.status, BatchStatus::Failed);
        assert!(retried.items[0].error.is_some());
        assert_eq!(
            fs::read(root.path().join("clip.mp4")).unwrap(),
            source_before
        );
        assert_eq!(
            fs::read(root.path().join("Existing/clip.mp4")).unwrap(),
            target_before
        );
        assert!(!root.path().join("Not created by preview").exists());

        let keep_both = manager
            .preview(&[item_id], "Existing", false, ConflictPolicy::KeepBoth)
            .expect("keep-both preview should be available");
        assert_eq!(keep_both.items[0].outcome, MoveOutcome::Move);
        assert!(keep_both.items[0].target_path.ends_with("clip (2).mp4"));
        assert!(!root.path().join("Existing/clip (2).mp4").exists());
        release_preview_anchors(&skip.id);
        release_preview_anchors(&keep_both.id);
    }

    #[test]
    fn move_target_at_scan_depth_limit_remains_visible_and_deeper_target_is_rejected() {
        let root = TemporaryRoot::new("move-target-scan-depth");
        let source_path = root.path().join("clip.mp4");
        fs::write(&source_path, b"fixture-video").expect("source media should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("initial scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let source_before = fs::read(&source_path).expect("source media should read");
        let source_identity_before = fs::metadata(&source_path).expect("source should stat");
        let previews_before = persisted_preview_count(&manager);
        let directory_at_limit = (0..MAX_DEPTH).map(|_| "d").collect::<Vec<_>>().join("/");
        let too_deep_directory = (0..=MAX_DEPTH).map(|_| "d").collect::<Vec<_>>().join("/");

        let rejected = manager.preview(
            std::slice::from_ref(&item_id),
            &too_deep_directory,
            false,
            ConflictPolicy::KeepBoth,
        );
        let error = rejected.expect_err("a target beyond scanner depth must be rejected");
        assert!(error.to_string().contains("scan depth limit"));
        assert_eq!(fs::read(&source_path).unwrap(), source_before);
        let source_identity_after = fs::metadata(&source_path).expect("source should still stat");
        assert_eq!(source_identity_before.dev(), source_identity_after.dev());
        assert_eq!(source_identity_before.ino(), source_identity_after.ino());
        assert!(!root.path().join(&too_deep_directory).exists());
        assert!(!root.path().join("d").exists());
        let previews_after = persisted_preview_count(&manager);
        assert_eq!(previews_after, previews_before);

        let preview = manager
            .preview(
                &[item_id],
                &directory_at_limit,
                false,
                ConflictPolicy::KeepBoth,
            )
            .expect("a target directory at the scan limit should be allowed");
        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("the boundary move should execute");
        assert_eq!(result.status, BatchStatus::Complete);
        let moved_path = format!("{directory_at_limit}/clip.mp4");
        let rescanned = manager
            .scan(&[])
            .expect("the boundary target should remain reachable by the scanner");
        let moved_item = item_at(&rescanned, &moved_path);
        assert_eq!(moved_item.presence, Presence::Present);
        release_preview_anchors(&preview.id);
    }

    #[test]
    fn organized_move_target_depth_includes_the_collection_folder() {
        let root = TemporaryRoot::new("organized-move-target-scan-depth");
        let source_path = root.path().join("clip.mp4");
        fs::write(&source_path, b"fixture-video").expect("source media should write");
        let manager = manager(&root);
        let mut snapshot = manager.scan(&[]).expect("initial scan should complete");
        let item = snapshot
            .items
            .iter_mut()
            .find(|item| item.relative_path == "clip.mp4")
            .expect("source media should be indexed");
        item.collection = Some(CollectionMetadata {
            id: Some("series-1".to_string()),
            title: "Collection".to_string(),
            kind: "series".to_string(),
            order: None,
            part_id: None,
        });
        let item_id = item.id.clone();
        let source_before = fs::read(&source_path).expect("source media should read");
        let source_identity_before = fs::metadata(&source_path).expect("source should stat");
        let previews_before = persisted_preview_count(&manager);
        let base_at_limit = (0..MAX_DEPTH).map(|_| "d").collect::<Vec<_>>().join("/");
        let base_before_limit = (0..MAX_DEPTH - 1)
            .map(|_| "d")
            .collect::<Vec<_>>()
            .join("/");

        let rejected = manager.build_and_persist_preview(
            &snapshot,
            PreviewBuildRequest {
                item_ids: std::slice::from_ref(&item_id),
                target_relative_dir: Some(&base_at_limit),
                rename: false,
                conflict: ConflictPolicy::KeepBoth,
                patches: &[],
            },
            &mut |target_path| manager.target_parent_device(target_path),
        );
        let error = rejected.expect_err("the collection folder must count toward target depth");
        assert!(error.to_string().contains("scan depth limit"));
        assert_eq!(fs::read(&source_path).unwrap(), source_before);
        assert!(!root.path().join(&base_at_limit).exists());
        assert!(!root.path().join("d").exists());
        let source_identity_after = fs::metadata(&source_path).expect("source should still stat");
        assert_eq!(source_identity_before.dev(), source_identity_after.dev());
        assert_eq!(source_identity_before.ino(), source_identity_after.ino());
        let previews_after = persisted_preview_count(&manager);
        assert_eq!(previews_after, previews_before);

        let preview = manager
            .build_and_persist_preview(
                &snapshot,
                PreviewBuildRequest {
                    item_ids: std::slice::from_ref(&item_id),
                    target_relative_dir: Some(&base_before_limit),
                    rename: false,
                    conflict: ConflictPolicy::KeepBoth,
                    patches: &[],
                },
                &mut |target_path| manager.target_parent_device(target_path),
            )
            .expect("a collection folder bringing the final target to depth 48 is allowed");
        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("the organized boundary move should execute");
        assert_eq!(result.status, BatchStatus::Complete);
        let moved_path = format!("{base_before_limit}/Collection/clip.mp4");
        let rescanned = manager
            .scan(&[])
            .expect("the organized boundary target should remain reachable");
        let moved_item = item_at(&rescanned, &moved_path);
        assert_eq!(moved_item.presence, Presence::Present);
        release_preview_anchors(&preview.id);
    }

    #[test]
    fn final_destination_path_components_obey_filesystem_byte_limit() {
        let ascii_at_limit = "a".repeat(MAX_PATH_COMPONENT_BYTES);
        let ascii_over_limit = "a".repeat(MAX_PATH_COMPONENT_BYTES + 1);
        assert!(safe_relative_path(&ascii_at_limit, false).is_ok());
        assert!(safe_relative_path(&ascii_over_limit, false).is_err());

        let cjk_at_limit = "界".repeat(MAX_PATH_COMPONENT_BYTES / "界".len());
        let emoji_at_limit = "😀".repeat(MAX_PATH_COMPONENT_BYTES / "😀".len());
        let emoji_over_limit = format!("{emoji_at_limit}😀");
        assert_eq!(cjk_at_limit.len(), MAX_PATH_COMPONENT_BYTES);
        assert_eq!(emoji_at_limit.len(), 252);
        assert_eq!(emoji_over_limit.len(), 256);
        assert!(safe_relative_path(&cjk_at_limit, false).is_ok());
        assert!(safe_relative_path(&emoji_at_limit, false).is_ok());
        assert!(safe_relative_path(&emoji_over_limit, false).is_err());

        let root = TemporaryRoot::new("overlong-final-target-component");
        let source_path = root.path().join("clip.mp4");
        fs::write(&source_path, b"fixture-video").expect("source media should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("initial scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let source_before = fs::read(&source_path).expect("source media should read");
        let source_identity_before = fs::metadata(&source_path).expect("source should stat");
        let previews_before = persisted_preview_count(&manager);
        let target_relative_dir = format!("d/{ascii_over_limit}");

        let error = manager
            .build_and_persist_preview(
                &snapshot,
                PreviewBuildRequest {
                    item_ids: std::slice::from_ref(&item_id),
                    target_relative_dir: Some(&target_relative_dir),
                    rename: false,
                    conflict: ConflictPolicy::KeepBoth,
                    patches: &[],
                },
                &mut |target_path| manager.target_parent_device(target_path),
            )
            .expect_err("an overlong target component must fail before preview persistence");
        assert!(error.to_string().contains("255-byte filesystem limit"));
        assert_eq!(fs::read(&source_path).unwrap(), source_before);
        let source_identity_after = fs::metadata(&source_path).expect("source should still stat");
        assert_eq!(source_identity_before.dev(), source_identity_after.dev());
        assert_eq!(source_identity_before.ino(), source_identity_after.ino());
        assert!(!root.path().join("d").exists());
        assert_eq!(persisted_preview_count(&manager), previews_before);
    }

    #[test]
    fn preview_validator_and_result_share_a_single_locked_scan() {
        let root = TemporaryRoot::new("preview-single-authoritative-scan");
        fs::write(root.path().join("clip.mp4"), b"fixture-video")
            .expect("source media should write");
        let manager = manager(&root);
        let initial = manager.scan(&[]).expect("initial scan should complete");
        let item_id = item_at(&initial, "clip.mp4").id.clone();
        let before = manager.scan_attempt_count_for_test();
        let mut validated_revision = None;

        let (preview, snapshot) = manager
            .preview_with_hints_and_patches_and_snapshot_validator(
                PreviewBuildRequest {
                    item_ids: std::slice::from_ref(&item_id),
                    target_relative_dir: Some("Organized"),
                    rename: false,
                    conflict: ConflictPolicy::KeepBoth,
                    patches: &[],
                },
                &[],
                |fresh_snapshot| {
                    assert!(fresh_snapshot.items.iter().any(|item| item.id == item_id));
                    validated_revision = Some(fresh_snapshot.revision.clone());
                    Ok(())
                },
            )
            .expect("validated preview should be built from the fresh scan");

        assert_eq!(manager.scan_attempt_count_for_test(), before + 1);
        assert_eq!(
            validated_revision.as_deref(),
            Some(snapshot.revision.as_str())
        );
        assert_eq!(preview.items.len(), 1);
        assert_eq!(preview.items[0].source_path, "clip.mp4");
    }

    #[test]
    fn preview_guard_rejection_does_not_persist_manifest_or_anchors() {
        let root = TemporaryRoot::new("preview-guard-rejects-before-persistence");
        let source_path = root.path().join("clip.mp4");
        fs::write(&source_path, b"fixture-video").expect("source media should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("initial scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let preview_count_before = persisted_preview_count(&manager);
        let source_identity_before = fs::metadata(&source_path).expect("source should stat");
        let mut rejected_preview = None;

        let error = manager
            .build_and_persist_preview_with_guard(
                &snapshot,
                PreviewBuildRequest {
                    item_ids: std::slice::from_ref(&item_id),
                    target_relative_dir: Some("Organized"),
                    rename: false,
                    conflict: ConflictPolicy::KeepBoth,
                    patches: &[],
                },
                &mut |target_path| manager.target_parent_device(target_path),
                |preview| {
                    rejected_preview = Some((preview.id.clone(), preview.revision.clone()));
                    Err(anyhow::anyhow!(
                        "preview receipt exceeds its persistence budget"
                    ))
                },
            )
            .expect_err("the preview guard should reject before persisting");
        assert!(error.to_string().contains("persistence budget"));
        let (preview_id, revision) =
            rejected_preview.expect("the guard should inspect the complete preview");
        assert_eq!(persisted_preview_count(&manager), preview_count_before);
        assert!(!root.path().join("Organized").exists());
        assert_eq!(fs::read(&source_path).unwrap(), b"fixture-video");
        let source_identity_after = fs::metadata(&source_path).expect("source should still stat");
        assert_eq!(source_identity_before.dev(), source_identity_after.dev());
        assert_eq!(source_identity_before.ino(), source_identity_after.ino());
        if let Some(cache) = LIVE_PREVIEW_ANCHORS.get() {
            assert!(
                !cache
                    .lock()
                    .expect("preview anchor lock should remain healthy")
                    .by_preview
                    .contains_key(&preview_id),
                "a rejected preview must not pin live file descriptors"
            );
        }
        assert!(
            manager
                .unconfirmable_preview_ids(&[(preview_id, revision)])
                .expect("missing rejected previews should remain distinguishable")
                .len()
                == 1,
            "a rejected preview must have no persisted confirmation record"
        );
    }

    #[test]
    fn skipped_conflict_is_partial_when_another_selected_item_moves() {
        let root = TemporaryRoot::new("skip-partial-batch");
        fs::write(root.path().join("clip.mp4"), b"conflicted source")
            .expect("conflicted media should write");
        fs::write(root.path().join("move.mp4"), b"movable source")
            .expect("movable media should write");
        fs::create_dir(root.path().join("Existing")).expect("target directory should create");
        fs::write(root.path().join("Existing/clip.mp4"), b"keep target")
            .expect("conflict target should write");

        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let ids = [
            item_at(&snapshot, "clip.mp4").id.clone(),
            item_at(&snapshot, "move.mp4").id.clone(),
        ];
        let preview = manager
            .preview(&ids, "Existing", false, ConflictPolicy::Skip)
            .expect("mixed move and conflict should preview");
        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("mixed batch should complete with an explicit partial result");

        assert_eq!(result.status, BatchStatus::Partial);
        assert_eq!(
            result
                .items
                .iter()
                .find(|item| item.item_id == ids[0])
                .unwrap()
                .outcome,
            MoveOutcome::Skip
        );
        assert!(
            result
                .items
                .iter()
                .find(|item| item.item_id == ids[0])
                .unwrap()
                .error
                .is_some()
        );
        assert!(root.path().join("Existing/move.mp4").exists());
    }

    #[test]
    fn confirmed_metadata_patch_preserves_unknown_nfo_fields_and_is_idempotent() {
        let root = TemporaryRoot::new("metadata-patch");
        fs::write(root.path().join("clip.mp4"), b"fixture-video")
            .expect("media fixture should write");
        let original_nfo = "<movie><note>İ keep</note><title>Old title</title><futureField key=\"x\">keep &amp; opaque</futureField></movie>\n";
        fs::write(root.path().join("clip.nfo"), original_nfo).expect("NFO fixture should write");
        fs::set_permissions(
            root.path().join("clip.nfo"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("NFO fixture should become private");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let patch = MetadataPatch {
            item_id,
            title: Some("Confirmed title".to_string()),
            source_ids: vec!["BV1abcdefgh".to_string()],
            collection: Some(CollectionMetadata {
                id: Some("collection-123".to_string()),
                title: "Series & More".to_string(),
                kind: "bilibili_collection".to_string(),
                order: Some(2),
                part_id: Some("Part-A".to_string()),
            }),
            fill_missing_only: false,
            organization_status: None,
        };
        let preview = manager
            .preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[patch])
            .expect("metadata-only preview should prepare");
        assert!(preview.metadata_patches[0].changed);
        assert_eq!(
            preview.metadata_patches[0].changes,
            vec![
                MetadataFieldChange {
                    field: "title".to_string(),
                    before: Some("Old title".to_string()),
                    after: "Confirmed title".to_string(),
                },
                MetadataFieldChange {
                    field: "collection".to_string(),
                    before: None,
                    after: "Series & More".to_string(),
                },
                MetadataFieldChange {
                    field: "collection_id".to_string(),
                    before: None,
                    after: "collection-123".to_string(),
                },
                MetadataFieldChange {
                    field: "collection_kind".to_string(),
                    before: None,
                    after: "bilibili_collection".to_string(),
                },
                MetadataFieldChange {
                    field: "order".to_string(),
                    before: None,
                    after: "2".to_string(),
                },
                MetadataFieldChange {
                    field: "part_id".to_string(),
                    before: None,
                    after: "Part-A".to_string(),
                },
            ]
        );
        assert_eq!(
            preview.metadata_patches[0].added_source_ids,
            vec!["BV1abcdefgh".to_string()]
        );
        assert_eq!(
            fs::read_to_string(root.path().join("clip.nfo")).unwrap(),
            original_nfo
        );

        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("confirmed NFO patch should complete");
        assert_eq!(result.status, BatchStatus::Complete);
        let patched =
            fs::read_to_string(root.path().join("clip.nfo")).expect("patched NFO should read");
        assert!(patched.contains("<title>Confirmed title</title>"));
        assert!(patched.contains("<note>İ keep</note>"));
        assert!(patched.contains("<futureField key=\"x\">keep &amp; opaque</futureField>"));
        assert!(patched.contains("<uniqueid type=\"bilibili\">BV1abcdefgh</uniqueid>"));
        assert!(patched.contains("<set>Series &amp; More</set>"));
        assert!(patched.contains("<setid>collection-123</setid>"));
        assert!(patched.contains("<settype>bilibili_collection</settype>"));
        assert!(patched.contains("<episode>2</episode>"));
        assert!(patched.contains("<partid>Part-A</partid>"));
        assert_eq!(
            fs::metadata(root.path().join("clip.nfo")).unwrap().mode() & 0o777,
            0o600,
            "confirmed NFO patch must preserve private file permissions"
        );
        let rescanned = manager
            .scan(&[])
            .expect("patched NFO should parse on rescan");
        assert_eq!(
            item_at(&rescanned, "clip.mp4").collection,
            Some(CollectionMetadata {
                id: Some("collection-123".to_string()),
                title: "Series & More".to_string(),
                kind: "bilibili_collection".to_string(),
                order: Some(2),
                part_id: Some("Part-A".to_string()),
            })
        );
        assert_eq!(
            manager
                .execute_preview(&preview.id, &preview.revision)
                .expect("completed batch retry should be idempotent")
                .status,
            BatchStatus::Complete
        );
    }

    #[tokio::test]
    async fn recovery_batch_enriches_missing_collection_fields_from_a_scanned_nfo() {
        let root = TemporaryRoot::new("metadata-recovery-incomplete-collection");
        fs::write(root.path().join("clip.mp4"), b"fixture-video")
            .expect("media fixture should write");
        let bvid = "BV1abcdefgh1";
        let original_nfo = format!(
            "<movie><title>Episode</title><uniqueid type=\"bilibili\">{bvid}</uniqueid><uniqueid type=\"bilibili-cid\">20</uniqueid><set>My collection</set><setid>shared-set</setid></movie>"
        );
        let nfo_path = root.path().join("clip.nfo");
        fs::write(&nfo_path, &original_nfo).expect("NFO fixture should write");
        fs::set_permissions(&nfo_path, fs::Permissions::from_mode(0o600))
            .expect("NFO fixture should become private");

        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("initial scan should complete");
        let local = item_at(&snapshot, "clip.mp4");
        assert_eq!(local.metadata_status, MetadataStatus::Complete);
        assert_eq!(local.collection.as_ref().unwrap().order, None);
        assert_eq!(local.collection.as_ref().unwrap().part_id, None);

        let report = crate::metadata_recovery::analyze_metadata_recovery(
            &snapshot,
            None,
            None,
            move |query| {
                assert_eq!(query.source_id, bvid);
                let entry = crate::metadata_recovery::ResolvedMetadataEntry {
                    bvid: Some(bvid.to_string()),
                    aid: None,
                    cid: Some(20),
                    epid: None,
                    title: "Remote episode title".to_string(),
                    collection: Some(CollectionMetadata {
                        id: Some("shared-set".to_string()),
                        title: "Remote collection title".to_string(),
                        kind: "bilibili_collection".to_string(),
                        order: Some(2),
                        part_id: Some("20".to_string()),
                    }),
                };
                async move { Ok(vec![entry]) }
            },
        )
        .await
        .expect("incomplete scanned collection should be analyzed");

        assert_eq!(report.items.len(), 1);
        assert_eq!(
            report.items[0].status,
            crate::metadata_recovery::RecoveryStatus::Ready
        );
        let patch = report.items[0].patch.as_ref().unwrap();
        let preview = manager
            .preview_with_patches(
                &[],
                None,
                false,
                ConflictPolicy::Skip,
                std::slice::from_ref(patch),
            )
            .expect("fill-only collection preview should include missing subfields");
        assert!(preview.metadata_patches[0].changed);
        assert!(
            preview.metadata_patches[0]
                .changes
                .iter()
                .any(|change| change.field == "order" && change.after == "2")
        );
        assert!(
            preview.metadata_patches[0]
                .changes
                .iter()
                .any(|change| change.field == "part_id" && change.after == "20")
        );
        assert_eq!(fs::read_to_string(&nfo_path).unwrap(), original_nfo);

        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("confirmed recovery patch should complete");
        assert_eq!(result.status, BatchStatus::Complete);
        let rescanned = manager.scan(&[]).expect("patched NFO should rescan");
        assert_eq!(
            item_at(&rescanned, "clip.mp4").collection,
            Some(CollectionMetadata {
                id: Some("shared-set".to_string()),
                title: "My collection".to_string(),
                kind: "bilibili_collection".to_string(),
                order: Some(2),
                part_id: Some("20".to_string()),
            })
        );
    }

    #[test]
    fn confirmed_collection_patch_preserves_kodi_set_children_and_rescans_name() {
        let root = TemporaryRoot::new("kodi-structured-set-patch");
        fs::write(root.path().join("clip.mp4"), b"fixture-video")
            .expect("media fixture should write");
        let original_nfo = "<movie><title>Episode</title><uniqueid type=\"bilibili-cid\">12345678</uniqueid><set source=\"library\"><name lang=\"en\">Old Series</name><overview xml:lang=\"en\">Keep this overview.</overview><future key=\"opaque\">Keep this node.</future></set><setid>old-series</setid></movie>";
        let nfo_path = root.path().join("clip.nfo");
        fs::write(&nfo_path, original_nfo).expect("NFO fixture should write");
        fs::set_permissions(&nfo_path, fs::Permissions::from_mode(0o600))
            .expect("NFO fixture should become private");

        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("initial scan should complete");
        let item = item_at(&snapshot, "clip.mp4");
        assert_eq!(
            item.collection
                .as_ref()
                .map(|collection| collection.title.as_str()),
            Some("Old Series"),
            "the collection title should come from Kodi's direct set/name child"
        );
        let patch = MetadataPatch {
            item_id: item.id.clone(),
            title: None,
            source_ids: Vec::new(),
            collection: Some(CollectionMetadata {
                id: Some("new-series".to_string()),
                title: "New Series".to_string(),
                kind: "bilibili_collection".to_string(),
                order: None,
                part_id: None,
            }),
            fill_missing_only: false,
            organization_status: None,
        };

        let preview = manager
            .preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[patch])
            .expect("structured collection metadata preview should succeed");
        assert!(preview.metadata_patches[0].changed);
        assert!(
            preview.metadata_patches[0]
                .changes
                .contains(&MetadataFieldChange {
                    field: "collection".to_string(),
                    before: Some("Old Series".to_string()),
                    after: "New Series".to_string(),
                })
        );
        assert_eq!(fs::read_to_string(&nfo_path).unwrap(), original_nfo);

        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("confirmed collection patch should complete");
        assert_eq!(result.status, BatchStatus::Complete);

        let patched = fs::read_to_string(&nfo_path).expect("patched NFO should read");
        assert!(patched.contains("<set source=\"library\">"));
        assert!(patched.contains("<name lang=\"en\">New Series</name>"));
        assert!(patched.contains("<overview xml:lang=\"en\">Keep this overview.</overview>"));
        assert!(patched.contains("<future key=\"opaque\">Keep this node.</future>"));
        assert!(!patched.contains("<set>New Series</set>"));

        let rescanned = manager.scan(&[]).expect("updated collection should rescan");
        assert_eq!(
            item_at(&rescanned, "clip.mp4").collection,
            Some(CollectionMetadata {
                id: Some("new-series".to_string()),
                title: "New Series".to_string(),
                kind: "bilibili_collection".to_string(),
                order: None,
                part_id: None,
            })
        );
    }

    #[test]
    fn collection_patch_rejects_ambiguous_or_complex_kodi_names() {
        let patch = MetadataPatch {
            item_id: "item".to_string(),
            title: None,
            source_ids: Vec::new(),
            collection: Some(CollectionMetadata {
                id: None,
                title: "New Series".to_string(),
                kind: "bilibili_collection".to_string(),
                order: None,
                part_id: None,
            }),
            fill_missing_only: false,
            organization_status: None,
        };
        for original in [
            "<movie><set><name>First</name><name>Second</name></set></movie>",
            "<movie><set><name>Old <em>Series</em></name><overview>Keep me</overview></set></movie>",
            "<movie><set><overview>Keep me</overview><future>Opaque</future></set></movie>",
        ] {
            let original = original.to_string();
            let before = original.clone();
            assert!(
                append_metadata_patch(&original, &patch).is_err(),
                "ambiguous or nested collection structure must fail closed: {original}"
            );
            assert_eq!(original, before);
        }
    }

    #[test]
    fn metadata_patch_uses_the_scanned_uppercase_nfo_path() {
        let root = TemporaryRoot::new("uppercase-nfo-patch");
        fs::write(root.path().join("Movie.mp4"), b"fixture-video")
            .expect("media fixture should write");
        fs::write(
            root.path().join("Movie.NFO"),
            "<movie><title>Original title</title><future>keep me</future></movie>",
        )
        .expect("uppercase NFO fixture should write");

        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let item = item_at(&snapshot, "Movie.mp4");
        assert_eq!(item.authoritative_nfo_path.as_deref(), Some("Movie.NFO"));
        assert_eq!(item.title, "Original title");

        let serialized = serde_json::to_value(item).expect("library item should serialize");
        assert!(
            serialized.get("authoritative_nfo_path").is_none(),
            "the scan-local NFO selection must not persist into an old index"
        );
        let restored: LibraryItem =
            serde_json::from_value(serialized).expect("legacy library item should deserialize");
        assert!(restored.authoritative_nfo_path.is_none());

        let patch = MetadataPatch {
            item_id: item.id.clone(),
            title: Some("Updated title".to_string()),
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        let preview = manager
            .preview_with_hints_and_patches(
                std::slice::from_ref(&item.id),
                Some("Moved"),
                false,
                ConflictPolicy::KeepBoth,
                &[],
                &[patch],
            )
            .expect("the selected existing NFO should move and patch together");
        assert!(preview.items[0].files.iter().any(|file| {
            file.source_path == "Movie.NFO" && file.target_path == "Moved/Movie.NFO"
        }));
        assert_eq!(preview.metadata_patches[0].nfo_path, "Moved/Movie.NFO");

        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("confirmed NFO patch should complete");
        assert_eq!(result.status, BatchStatus::Complete);
        let patched = fs::read_to_string(root.path().join("Moved/Movie.NFO"))
            .expect("the mapped uppercase target should remain readable");
        assert!(patched.contains("<title>Updated title</title>"));
        assert!(patched.contains("<future>keep me</future>"));
        let moved_dir = root.path().join("Moved");
        let nfo_names = fs::read_dir(&moved_dir)
            .expect("moved fixture directory should list")
            .map(|entry| {
                entry
                    .expect("directory entry should read")
                    .file_name()
                    .into_string()
                    .expect("fixture names should be UTF-8")
            })
            .filter(|name| name.to_ascii_lowercase().ends_with(".nfo"))
            .collect::<Vec<_>>();
        assert_eq!(
            nfo_names,
            vec!["Movie.NFO".to_string()],
            "the patch must follow the move mapping instead of creating a lowercase sibling"
        );
        assert!(!root.path().join("Movie.nfo").exists());

        let rescanned = manager.scan(&[]).expect("patched NFO should rescan");
        let rescanned_item = item_at(&rescanned, "Moved/Movie.mp4");
        assert_eq!(
            rescanned_item.authoritative_nfo_path.as_deref(),
            Some("Moved/Movie.NFO")
        );
        assert_eq!(rescanned_item.title, "Updated title");
    }

    #[test]
    fn exact_nfo_wins_over_a_dotted_suffix_nfo() {
        let root = TemporaryRoot::new("exact-nfo-priority");
        fs::write(root.path().join("Movie.mp4"), b"fixture-video")
            .expect("media fixture should write");
        fs::write(
            root.path().join("Movie.nfo"),
            "<movie><title>Exact title</title></movie>",
        )
        .expect("exact NFO should write");
        let suffix_nfo = "<movie><title>Suffix title</title><future>suffix stays</future></movie>";
        fs::write(root.path().join("Movie.zz.nfo"), suffix_nfo).expect("suffix NFO should write");

        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let item = item_at(&snapshot, "Movie.mp4");
        assert_eq!(item.authoritative_nfo_path.as_deref(), Some("Movie.nfo"));
        assert_eq!(item.title, "Exact title");

        let patch = MetadataPatch {
            item_id: item.id.clone(),
            title: Some("Updated exact title".to_string()),
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        let preview = manager
            .preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[patch])
            .expect("the exact-stem NFO should be selected");
        assert_eq!(preview.metadata_patches[0].nfo_path, "Movie.nfo");
        manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("confirmed exact NFO patch should complete");

        assert!(
            fs::read_to_string(root.path().join("Movie.nfo"))
                .unwrap()
                .contains("<title>Updated exact title</title>")
        );
        assert_eq!(
            fs::read_to_string(root.path().join("Movie.zz.nfo")).unwrap(),
            suffix_nfo,
            "the non-authoritative suffix NFO must remain unchanged"
        );
        let rescanned = manager.scan(&[]).expect("patched exact NFO should rescan");
        assert_eq!(
            item_at(&rescanned, "Movie.mp4").title,
            "Updated exact title"
        );
    }

    #[test]
    fn move_and_patch_preserve_the_selected_suffixed_nfo_path() {
        let root = TemporaryRoot::new("move-suffixed-nfo");
        fs::write(root.path().join("Movie.mp4"), b"fixture-video")
            .expect("media fixture should write");
        fs::write(
            root.path().join("Movie.en.nfo"),
            "<movie><title>Original title</title><future>keep me</future></movie>",
        )
        .expect("language-suffixed NFO should write");

        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let item = item_at(&snapshot, "Movie.mp4");
        assert_eq!(item.authoritative_nfo_path.as_deref(), Some("Movie.en.nfo"));
        let patch = MetadataPatch {
            item_id: item.id.clone(),
            title: Some("Moved title".to_string()),
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        let preview = manager
            .preview_with_hints_and_patches(
                std::slice::from_ref(&item.id),
                Some("Moved"),
                false,
                ConflictPolicy::KeepBoth,
                &[],
                &[patch],
            )
            .expect("move and metadata patch should preview together");
        assert!(preview.items[0].files.iter().any(|file| {
            file.source_path == "Movie.en.nfo" && file.target_path == "Moved/Movie.en.nfo"
        }));
        assert_eq!(
            preview.metadata_patches[0].nfo_path, "Moved/Movie.en.nfo",
            "the patch target must follow the selected attachment's actual move mapping"
        );

        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("move and patch should complete");
        assert_eq!(result.status, BatchStatus::Complete);
        let patched = fs::read_to_string(root.path().join("Moved/Movie.en.nfo"))
            .expect("moved NFO should be updated at its mapped path");
        assert!(patched.contains("<title>Moved title</title>"));
        assert!(patched.contains("<future>keep me</future>"));

        let rescanned = manager.scan(&[]).expect("moved item should rescan");
        let moved_item = item_at(&rescanned, "Moved/Movie.mp4");
        assert_eq!(
            moved_item.authoritative_nfo_path.as_deref(),
            Some("Moved/Movie.en.nfo")
        );
        assert_eq!(moved_item.title, "Moved title");
    }

    #[test]
    fn missing_authoritative_nfo_does_not_fall_back_to_create_canonical_path() {
        let root = TemporaryRoot::new("missing-authoritative-nfo");
        fs::write(root.path().join("Movie.mp4"), b"fixture-video")
            .expect("media fixture should write");
        fs::write(
            root.path().join("Movie.NFO"),
            "<movie><title>Original title</title></movie>",
        )
        .expect("uppercase NFO should write");

        let manager = manager(&root);
        let _process_guard = manager
            .in_process_lock
            .lock()
            .expect("manager process lock should be available");
        let _disk_guard = manager
            .lock_store()
            .expect("manager store lock should be available");
        let snapshot = manager
            .scan_locked(&[])
            .expect("scan should complete under the normal manager locks");
        let item = item_at(&snapshot, "Movie.mp4");
        assert_eq!(item.authoritative_nfo_path.as_deref(), Some("Movie.NFO"));
        fs::remove_file(root.path().join("Movie.NFO"))
            .expect("selected NFO should disappear before preview");
        let patch = MetadataPatch {
            item_id: item.id.clone(),
            title: Some("Replacement title".to_string()),
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };

        let mut no_target_device = |_target_path: &str| Ok(0_u64);
        let error = manager
            .build_and_persist_preview(
                &snapshot,
                PreviewBuildRequest {
                    item_ids: &[],
                    target_relative_dir: None,
                    rename: false,
                    conflict: ConflictPolicy::Skip,
                    patches: std::slice::from_ref(&patch),
                },
                &mut no_target_device,
            )
            .expect_err("a disappeared selected NFO must reject the metadata preview");
        assert!(
            error.to_string().contains("authoritative NFO disappeared"),
            "unexpected preview error: {error:#}"
        );
        assert_eq!(persisted_preview_count(&manager), 0);
        assert!(!root.path().join("Movie.nfo").exists());
    }

    #[test]
    fn repeated_metadata_patch_expands_self_closing_tag_without_losing_siblings() {
        let original = "<movie><title/><futureField key=\"x\">keep &amp; opaque</futureField><actor>keep actor</actor></movie>";
        let first_patch = MetadataPatch {
            item_id: "fixture-item".to_string(),
            title: Some("First title".to_string()),
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        let once = append_metadata_patch(original, &first_patch)
            .expect("first patch should expand the self-closing title");
        assert!(once.contains("<title>First title</title>"));
        assert!(once.contains("<futureField key=\"x\">keep &amp; opaque</futureField>"));
        assert!(once.contains("<actor>keep actor</actor>"));

        let second_patch = MetadataPatch {
            title: Some("Second title".to_string()),
            ..first_patch
        };
        let twice = append_metadata_patch(&once, &second_patch)
            .expect("second patch should replace only the title value");

        assert_eq!(twice.matches("<title").count(), 1);
        assert!(twice.contains("<title>Second title</title>"));
        assert!(twice.contains("<futureField key=\"x\">keep &amp; opaque</futureField>"));
        assert!(twice.contains("<actor>keep actor</actor>"));
    }

    #[test]
    fn fill_only_metadata_patch_preserves_custom_fields_and_fills_only_missing_values() {
        let existing = "<movie><title>My title</title><set>My collection</set><setid>custom-id</setid><futureField key=\"x\">keep &amp; opaque</futureField></movie>";
        let patch = MetadataPatch {
            item_id: "fixture-item".to_string(),
            title: Some("Remote title".to_string()),
            source_ids: vec!["BV1abcdefgh".to_string(), "cid12345".to_string()],
            collection: Some(CollectionMetadata {
                id: Some("remote-id".to_string()),
                title: "Remote collection".to_string(),
                kind: "bilibili_collection".to_string(),
                order: Some(4),
                part_id: Some("part-4".to_string()),
            }),
            fill_missing_only: true,
            organization_status: None,
        };

        let patched = append_metadata_patch(existing, &patch)
            .expect("fill-only metadata patch should preserve an existing NFO");

        assert!(patched.contains("<title>My title</title>"));
        assert!(patched.contains("<set>My collection</set>"));
        assert!(patched.contains("<setid>custom-id</setid>"));
        assert!(!patched.contains("Remote title"));
        assert!(!patched.contains("Remote collection"));
        assert!(!patched.contains("<episode>4</episode>"));
        assert!(!patched.contains("<partid>part-4</partid>"));
        assert!(patched.contains("<uniqueid type=\"bilibili\">BV1abcdefgh</uniqueid>"));
        assert!(patched.contains("<uniqueid type=\"bilibili-cid\">cid12345</uniqueid>"));
        assert!(patched.contains("<futureField key=\"x\">keep &amp; opaque</futureField>"));

        let missing = "<movie><title/><set></set><futureField>untouched</futureField></movie>";
        let filled = append_metadata_patch(missing, &patch)
            .expect("fill-only patch should populate missing fields");
        assert!(filled.contains("<title>Remote title</title>"));
        assert!(filled.contains("<set>Remote collection</set>"));
        assert_eq!(filled.matches("<set>").count(), 1);
        assert!(filled.contains("<setid>remote-id</setid>"));
        assert!(filled.contains("<episode>4</episode>"));
        assert!(filled.contains("<partid>part-4</partid>"));
        assert!(filled.contains("<futureField>untouched</futureField>"));

        let no_collection = "<movie><title>Local title</title></movie>";
        let inserted = append_metadata_patch(no_collection, &patch)
            .expect("fill-only patch should append a missing collection");
        assert_eq!(inserted.matches("<set>").count(), 1);
    }

    #[test]
    fn fill_only_collection_patch_adds_missing_subfields_only_for_the_same_collection() {
        let patch = MetadataPatch {
            item_id: "fixture-item".to_string(),
            title: None,
            source_ids: Vec::new(),
            collection: Some(CollectionMetadata {
                id: Some("shared-id".to_string()),
                title: "Remote title".to_string(),
                kind: "bilibili_collection".to_string(),
                order: Some(8),
                part_id: Some("20".to_string()),
            }),
            fill_missing_only: true,
            organization_status: None,
        };
        let existing = "<movie><set source=\"library\"><name lang=\"en\">My edited collection</name><overview>Keep this overview.</overview><future key=\"opaque\">Keep this node.</future></set><setid>shared-id</setid><episode>3</episode><futureField>keep</futureField></movie>";
        let patched = append_metadata_patch(existing, &patch)
            .expect("matching collection should receive missing metadata");
        assert!(patched.contains("<name lang=\"en\">My edited collection</name>"));
        assert!(patched.contains("<overview>Keep this overview.</overview>"));
        assert!(patched.contains("<future key=\"opaque\">Keep this node.</future>"));
        assert!(patched.contains("<setid>shared-id</setid>"));
        assert!(patched.contains("<episode>3</episode>"));
        assert!(patched.contains("<settype>bilibili_collection</settype>"));
        assert!(patched.contains("<partid>20</partid>"));
        assert!(patched.contains("<futureField>keep</futureField>"));
        assert!(!patched.contains("<episode>8</episode>"));

        let complex_without_name = "<movie><set><overview>Preserve this child.</overview></set><setid>shared-id</setid></movie>";
        let filled_complex = append_metadata_patch(complex_without_name, &patch)
            .expect("missing subfields should fill without discarding structured children");
        assert!(filled_complex.contains("<set><overview>Preserve this child.</overview></set>"));
        assert!(filled_complex.contains("<episode>8</episode>"));
        assert!(filled_complex.contains("<partid>20</partid>"));

        let unrelated = "<movie><set>My private collection</set><setid>private-id</setid></movie>";
        let preserved = append_metadata_patch(unrelated, &patch)
            .expect("a different collection should remain untouched");
        assert_eq!(preserved, unrelated);
    }

    #[test]
    fn fill_only_new_nfo_does_not_promote_the_filename_fallback_to_title() {
        let patch = MetadataPatch {
            item_id: "fixture-item".to_string(),
            title: None,
            source_ids: vec!["BV1abcdefgh1".to_string()],
            collection: None,
            fill_missing_only: true,
            organization_status: Some(OrganizationStatus::Unreviewed),
        };
        let created = new_nfo("legacy filename", &patch);
        assert!(!created.contains("<title>"));
        assert!(created.contains("<uniqueid type=\"bilibili\">BV1abcdefgh1</uniqueid>"));
        assert_eq!(nfo_source_ids(&created), vec!["BV1abcdefgh1"]);

        let legacy_patch = MetadataPatch {
            fill_missing_only: false,
            organization_status: None,
            ..patch
        };
        let legacy = new_nfo("legacy filename", &legacy_patch);
        assert!(legacy.contains("<title>legacy filename</title>"));
    }

    #[test]
    fn fill_only_preview_does_not_write_filename_title_for_source_id_only_patch() {
        let root = TemporaryRoot::new("fill-only-no-filename-title");
        fs::write(root.path().join("legacy.mp4"), b"fixture-video")
            .expect("media fixture should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let item = item_at(&snapshot, "legacy.mp4");
        assert_eq!(item.title, "legacy");

        let patch = MetadataPatch {
            item_id: item.id.clone(),
            title: None,
            source_ids: vec!["BV1abcdefgh1".to_string()],
            collection: None,
            fill_missing_only: true,
            organization_status: Some(OrganizationStatus::Unreviewed),
        };
        let preview = manager
            .preview_with_patches(
                &[],
                None,
                false,
                ConflictPolicy::Skip,
                std::slice::from_ref(&patch),
            )
            .expect("fill-only NFO preview should succeed");
        assert!(preview.metadata_patches[0].changed);
        assert!(
            !preview.metadata_patches[0]
                .changes
                .iter()
                .any(|change| change.field == "title")
        );

        manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("confirmed fill-only patch should write its explicit fields");
        let created =
            fs::read_to_string(root.path().join("legacy.nfo")).expect("new NFO should exist");
        assert!(!created.contains("<title>"));
        assert!(created.contains("<uniqueid type=\"bilibili\">BV1abcdefgh1</uniqueid>"));
        assert!(created.contains("<tvd_organization>unreviewed</tvd_organization>"));
    }

    #[test]
    fn organization_status_round_trips_and_duplicate_fields_are_ambiguous() {
        let patch = MetadataPatch {
            item_id: "fixture-item".to_string(),
            title: None,
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: Some(OrganizationStatus::Standalone),
        };
        let created = new_nfo("Local title", &patch);
        assert_eq!(
            nfo_organization_status(&created),
            Some(OrganizationStatus::Standalone)
        );

        let existing = "<movie><title>Local title</title><tvd_organization>standalone</tvd_organization><futureField>keep</futureField></movie>";
        let unreviewed = append_metadata_patch(
            existing,
            &MetadataPatch {
                organization_status: Some(OrganizationStatus::Unreviewed),
                ..patch.clone()
            },
        )
        .expect("explicit unreview action should replace the portable marker");
        assert_eq!(
            nfo_organization_status(&unreviewed),
            Some(OrganizationStatus::Unreviewed)
        );
        assert!(unreviewed.contains("<futureField>keep</futureField>"));

        let duplicate = "<movie><tvd_organization>standalone</tvd_organization><tvd_organization>unreviewed</tvd_organization></movie>";
        assert_eq!(nfo_organization_status(duplicate), None);
        assert!(append_metadata_patch(duplicate, &patch).is_err());
    }

    #[test]
    fn standalone_nfo_status_is_not_overridden_by_later_queue_collection_hints() {
        let root = TemporaryRoot::new("standalone-preserves-queue-hint");
        fs::write(root.path().join("clip.mp4"), b"fixture-video")
            .expect("media fixture should write");
        fs::write(
            root.path().join("clip.nfo"),
            "<movie><title>Clip title</title><tvd_organization>standalone</tvd_organization></movie>",
        )
        .expect("standalone NFO should write");
        let manager = manager(&root);
        let collection = CollectionMetadata {
            id: Some("queue-collection".to_string()),
            title: "Queue collection".to_string(),
            kind: "bilibili_collection".to_string(),
            order: Some(2),
            part_id: None,
        };
        let hint = SourceHint {
            source_ids: vec!["BV1abcdefgh".to_string()],
            title: Some("Queue title".to_string()),
            relative_path: Some("clip.mp4".to_string()),
            collection: Some(collection),
            origin: HintOrigin::Queue,
            requires_confirmation: false,
        };

        let first = manager
            .scan(std::slice::from_ref(&hint))
            .expect("queue metadata should scan");
        let first_item = item_at(&first, "clip.mp4");
        assert_eq!(
            first_item.organization_status,
            Some(OrganizationStatus::Standalone)
        );
        assert_eq!(first_item.collection, None);
        assert_eq!(first_item.title, "Clip title");

        let rescanned = manager
            .scan(&[])
            .expect("persisted queue metadata should remain suppressed by standalone intent");
        let rescanned_item = item_at(&rescanned, "clip.mp4");
        assert_eq!(
            rescanned_item.organization_status,
            Some(OrganizationStatus::Standalone)
        );
        assert_eq!(rescanned_item.collection, None);
    }

    #[test]
    fn standalone_declaration_is_rejected_when_nfo_contains_collection_metadata() {
        let root = TemporaryRoot::new("standalone-rejects-collection");
        fs::write(root.path().join("clip.mp4"), b"fixture-video")
            .expect("media fixture should write");
        fs::write(
            root.path().join("clip.nfo"),
            "<movie><title>Clip title</title><set>Custom collection</set></movie>",
        )
        .expect("collection NFO should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let patch = MetadataPatch {
            item_id: item_at(&snapshot, "clip.mp4").id.clone(),
            title: None,
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: Some(OrganizationStatus::Standalone),
        };

        let error = manager
            .preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[patch])
            .expect_err("an item with a stored collection cannot be marked standalone");
        assert!(error.to_string().contains("cannot be marked standalone"));
        assert_eq!(persisted_preview_count(&manager), 0);
    }

    #[test]
    fn complete_nfo_metadata_is_recognized_and_collection_identity_round_trips() {
        let root = TemporaryRoot::new("complete-nfo");
        fs::write(root.path().join("complete.mp4"), b"fixture-video")
            .expect("complete media should write");
        fs::write(
            root.path().join("complete.nfo"),
            "<movie><title>Complete episode</title><uniqueid type=\"youtube\">abcdefghijk</uniqueid><set>Collection</set><setid>collection-42</setid><settype>youtube_playlist</settype></movie>",
        )
        .expect("complete NFO should write");
        fs::write(root.path().join("partial.mp4"), b"fixture-video")
            .expect("partial media should write");
        fs::write(
            root.path().join("partial.nfo"),
            "<movie><title>Partial episode</title></movie>",
        )
        .expect("partial NFO should write");

        let snapshot = manager(&root)
            .scan(&[SourceHint {
                source_ids: vec!["abcdefghijk".to_string()],
                title: None,
                relative_path: None,
                collection: None,
                origin: HintOrigin::Queue,
                requires_confirmation: false,
            }])
            .expect("metadata scan should complete");
        let complete = item_at(&snapshot, "complete.mp4");
        assert_eq!(complete.metadata_status, MetadataStatus::Complete);
        assert_eq!(complete.source_ids, vec!["youtube:abcdefghijk".to_string()]);
        assert_eq!(
            complete.collection,
            Some(CollectionMetadata {
                id: Some("collection-42".to_string()),
                title: "Collection".to_string(),
                kind: "youtube_playlist".to_string(),
                order: None,
                part_id: None,
            })
        );
        assert_eq!(
            item_at(&snapshot, "partial.mp4").metadata_status,
            MetadataStatus::Partial
        );
    }

    #[test]
    fn newly_created_nfo_retries_by_digest_and_rejects_broader_access() {
        let root = TemporaryRoot::new("nfo-create-restart");
        fs::write(root.path().join("clip.mp4"), b"fixture-video")
            .expect("media fixture should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let patch = MetadataPatch {
            item_id: item_at(&snapshot, "clip.mp4").id.clone(),
            title: Some("Created title".to_string()),
            source_ids: vec!["youtube:abcdefghijk".to_string()],
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        let preview = manager
            .preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[patch])
            .expect("new NFO preview should prepare");
        let persisted = manager
            .read_persisted_preview(&preview.id)
            .expect("prepared NFO should be persisted");
        let prepared_patch = &persisted.patches[0];

        manager
            .execute_nfo_patch(&preview.id, prepared_patch)
            .expect("first attempt should create the NFO before its manifest update");
        let created_content =
            fs::read(root.path().join("clip.nfo")).expect("created NFO should read");
        let retried = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("digest-matched creation should resume after interruption");
        assert_eq!(retried.status, BatchStatus::Complete);
        assert_eq!(
            fs::read(root.path().join("clip.nfo")).unwrap(),
            created_content
        );

        fs::set_permissions(
            root.path().join("clip.nfo"),
            fs::Permissions::from_mode(0o666),
        )
        .expect("broader access policy should be applied for the regression case");
        assert!(
            manager
                .execute_nfo_patch(&preview.id, prepared_patch)
                .is_err()
        );
        assert_eq!(
            fs::read(root.path().join("clip.nfo")).unwrap(),
            created_content,
            "retry must not rewrite a digest-matched NFO with a changed access policy"
        );
    }

    #[derive(Default)]
    struct FailAppliedBatchManifestWriteProvider {
        batch_manifest_writes: AtomicUsize,
    }

    impl QueueFileProvider for FailAppliedBatchManifestWriteProvider {
        fn coordinate_read(
            &self,
            path: &Path,
            accessor: &mut dyn FnMut(&Path) -> Result<()>,
        ) -> Result<()> {
            accessor(path)
        }

        fn coordinate_write(
            &self,
            path: &Path,
            accessor: &mut dyn FnMut(&Path) -> Result<()>,
        ) -> Result<()> {
            let is_batch_manifest = path
                .file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.starts_with(BATCH_PREFIX));
            if is_batch_manifest && self.batch_manifest_writes.fetch_add(1, Ordering::SeqCst) == 2 {
                bail!("injected manifest write failure after NFO replacement");
            }
            accessor(path)
        }

        fn coordinate_move(
            &self,
            source: &Path,
            destination: &Path,
            accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
        ) -> Result<()> {
            accessor(source, destination)
        }
    }

    fn existing_nfo_manifest_failure_fixture(
        label: &str,
    ) -> (TemporaryRoot, LibraryManager, MovePreview) {
        let root = TemporaryRoot::new(label);
        fs::write(root.path().join("clip.mp4"), b"fixture-video")
            .expect("media fixture should write");
        fs::write(
            root.path().join("clip.nfo"),
            "<movie><title>Before</title><future>keep</future></movie>",
        )
        .expect("NFO fixture should write");
        fs::set_permissions(
            root.path().join("clip.nfo"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("NFO fixture should become private");
        let provider_impl = Arc::new(FailAppliedBatchManifestWriteProvider::default());
        let provider: Arc<dyn QueueFileProvider> = provider_impl;
        let manager = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should open");
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let patch = MetadataPatch {
            item_id: item_at(&snapshot, "clip.mp4").id.clone(),
            title: Some("After".to_string()),
            source_ids: vec!["youtube:abcdefghijk".to_string()],
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        let preview = manager
            .preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[patch])
            .expect("existing NFO patch should preview");
        assert!(
            manager
                .execute_preview(&preview.id, &preview.revision)
                .is_err(),
            "the injected third manifest write should fail after replacement"
        );
        let bytes = manager
            .read_private_file(&manager.batch_path(&preview.id), MANIFEST_LIMIT)
            .expect("interrupted batch manifest should read")
            .expect("interrupted batch manifest should exist");
        let manifest: BatchManifest =
            serde_json::from_slice(&bytes).expect("interrupted manifest should decode");
        assert_eq!(manifest.patches[0].state, ManifestPatchState::Pending);
        assert!(manifest.patches[0].replacement_checkpoint.is_some());
        assert!(
            fs::read_to_string(root.path().join("clip.nfo"))
                .unwrap()
                .contains("<title>After</title>"),
            "the NFO replacement should have happened before the injected manifest failure"
        );
        (root, manager, preview)
    }

    fn release_nfo_preview_anchors_if_restart_proof_is_persistent(
        manager: &LibraryManager,
        preview: &MovePreview,
    ) {
        let persisted = manager
            .read_persisted_preview(&preview.id)
            .expect("prepared NFO preview should read");
        let persistent_birth_time = persisted
            .preview
            .items
            .iter()
            .flat_map(|item| &item.files)
            .filter(|file| !file.identity.is_empty())
            .all(|file| {
                decode_identity_token(&file.identity)
                    .expect("move identity should decode")
                    .birth_seconds
                    .is_some()
            })
            && persisted.patches.iter().all(|patch| {
                decode_identity_token(&patch.media_identity)
                    .expect("media identity should decode")
                    .birth_seconds
                    .is_some()
                    && patch.expected_identity.as_deref().is_none_or(|identity| {
                        decode_identity_token(identity)
                            .expect("NFO identity should decode")
                            .birth_seconds
                            .is_some()
                    })
            });
        if persistent_birth_time {
            release_preview_anchors(&preview.id);
        }
    }

    #[test]
    fn existing_nfo_replacement_checkpoint_recovers_after_restart() {
        let (root, current_manager, preview) =
            existing_nfo_manifest_failure_fixture("nfo-checkpoint-restart");
        release_nfo_preview_anchors_if_restart_proof_is_persistent(&current_manager, &preview);
        drop(current_manager);

        let reopened = manager(&root);
        let result = reopened
            .execute_preview(&preview.id, &preview.revision)
            .expect("journaled replacement should complete after reopening");
        assert_eq!(result.status, BatchStatus::Complete);
        let updated_nfo = fs::read_to_string(root.path().join("clip.nfo")).unwrap();
        assert!(updated_nfo.contains("<title>After</title>"));
        assert!(updated_nfo.contains("<future>keep</future>"));
        assert!(updated_nfo.contains("<uniqueid type=\"youtube\">abcdefghijk</uniqueid>"));
        assert_eq!(
            updated_nfo.matches("<uniqueid type=\"youtube\">").count(),
            1,
            "restart recovery must not duplicate the NFO source ID"
        );
        let batch = reopened
            .read_private_file(&reopened.batch_path(&preview.id), MANIFEST_LIMIT)
            .expect("completed batch should read")
            .expect("completed batch should remain recoverable");
        let manifest: BatchManifest =
            serde_json::from_slice(&batch).expect("completed manifest should decode");
        assert_eq!(manifest.patches[0].state, ManifestPatchState::Applied);
        assert!(manifest.patches[0].replacement_checkpoint.is_some());
    }

    #[test]
    fn nfo_replacement_checkpoint_rejects_same_content_object_replacement() {
        let (root, current_manager, preview) =
            existing_nfo_manifest_failure_fixture("nfo-checkpoint-replaced");
        let current = root.path().join("clip.nfo");
        let replacement = root.path().join("external-copy.nfo");
        let bytes = fs::read(&current).expect("patched NFO should read");
        fs::write(&replacement, &bytes).expect("same-content external NFO should write");
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600))
            .expect("external NFO should use the approved access mode");
        fs::rename(&replacement, &current)
            .expect("same-content external NFO should replace the checkpointed object");
        release_nfo_preview_anchors_if_restart_proof_is_persistent(&current_manager, &preview);
        drop(current_manager);

        let reopened = manager(&root);
        let result = reopened
            .execute_preview(&preview.id, &preview.revision)
            .expect("mismatched checkpoint should become a failed batch result");
        assert_eq!(result.status, BatchStatus::Failed);
        assert_eq!(fs::read(current).unwrap(), bytes);
    }

    #[test]
    fn nfo_replacement_checkpoint_rejects_access_policy_change() {
        let (root, current_manager, preview) =
            existing_nfo_manifest_failure_fixture("nfo-checkpoint-permissions");
        let current = root.path().join("clip.nfo");
        let bytes = fs::read(&current).expect("patched NFO should read");
        fs::set_permissions(&current, fs::Permissions::from_mode(0o644))
            .expect("NFO access policy should change for the regression case");
        release_nfo_preview_anchors_if_restart_proof_is_persistent(&current_manager, &preview);
        drop(current_manager);

        let reopened = manager(&root);
        let result = reopened
            .execute_preview(&preview.id, &preview.revision)
            .expect("mismatched checkpoint should become a failed batch result");
        assert_eq!(result.status, BatchStatus::Failed);
        assert_eq!(fs::read(&current).unwrap(), bytes);
        assert_eq!(fs::metadata(current).unwrap().mode() & 0o777, 0o644);
    }

    #[test]
    fn legacy_nfo_manifest_without_replacement_checkpoint_fails_closed() {
        let (root, current_manager, preview) =
            existing_nfo_manifest_failure_fixture("nfo-checkpoint-legacy");
        for path in [
            current_manager.batch_path(&preview.id),
            current_manager.preview_path(&preview.id),
        ] {
            let bytes = current_manager
                .read_private_file(&path, MANIFEST_LIMIT.max(PREVIEW_LIMIT))
                .expect("legacy operation state should read")
                .expect("legacy operation state should exist");
            let mut value: serde_json::Value =
                serde_json::from_slice(&bytes).expect("operation state should decode");
            if let Some(patches) = value
                .get_mut("patches")
                .and_then(serde_json::Value::as_array_mut)
                && let Some(patch) = patches
                    .first_mut()
                    .and_then(serde_json::Value::as_object_mut)
            {
                patch.remove("replacement_checkpoint");
            }
            current_manager
                .write_private_file(&path, &serde_json::to_vec(&value).unwrap())
                .expect("checkpoint field should be removed to simulate an old manifest");
        }
        release_nfo_preview_anchors_if_restart_proof_is_persistent(&current_manager, &preview);
        drop(current_manager);

        let reopened = manager(&root);
        let result = reopened
            .execute_preview(&preview.id, &preview.revision)
            .expect("legacy after-state should be reported as a failed batch");
        assert_eq!(result.status, BatchStatus::Failed);
        assert!(
            fs::read_to_string(root.path().join("clip.nfo"))
                .unwrap()
                .contains("<title>After</title>")
        );
    }

    #[test]
    fn metadata_patch_rejects_shared_nfo_targets_with_or_without_existing_sidecar() {
        let root = TemporaryRoot::new("shared-nfo-target");
        fs::write(root.path().join("clip.mp4"), b"video-mp4").unwrap();
        fs::write(root.path().join("clip.mkv"), b"video-mkv").unwrap();
        let manager = manager(&root);
        let snapshot = manager
            .scan(&[])
            .expect("scan should find both media items");
        let mp4 = item_at(&snapshot, "clip.mp4");
        let patch_for = |item_id: &str, title: &str| MetadataPatch {
            item_id: item_id.to_string(),
            title: Some(title.to_string()),
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };

        assert!(
            manager
                .preview_with_patches(
                    &[],
                    None,
                    false,
                    ConflictPolicy::Skip,
                    &[patch_for(&mp4.id, "MP4 title")],
                )
                .is_err(),
            "a single patch must not claim an NFO candidate shared with another media item"
        );

        fs::write(
            root.path().join("clip.nfo"),
            "<movie><title>Shared title</title></movie>",
        )
        .expect("shared NFO should write");
        let snapshot = manager
            .scan(&[])
            .expect("scan with shared NFO should complete");
        let mp4 = item_at(&snapshot, "clip.mp4");
        let mkv = item_at(&snapshot, "clip.mkv");
        assert!(
            manager
                .preview_with_patches(
                    &[],
                    None,
                    false,
                    ConflictPolicy::Skip,
                    &[patch_for(&mp4.id, "MP4 title")],
                )
                .is_err(),
            "an existing shared NFO must not silently attach to one of its media candidates"
        );
        assert!(
            manager
                .preview_with_patches(
                    &[],
                    None,
                    false,
                    ConflictPolicy::Skip,
                    &[
                        patch_for(&mp4.id, "MP4 title"),
                        patch_for(&mkv.id, "MKV title"),
                    ],
                )
                .is_err(),
            "two patches must be rejected when their filesystem target keys are shared"
        );
    }

    #[test]
    fn metadata_patch_target_reservation_uses_case_and_normalization_aliases() {
        fn check_alias_pair(label: &str, first: &str, second: &str) {
            let root = TemporaryRoot::new(label);
            fs::write(root.path().join(first), b"first-video").unwrap();
            fs::write(root.path().join(second), b"second-video").unwrap();
            let manager = manager(&root);
            let snapshot = manager.scan(&[]).expect("alias scan should complete");
            assert_eq!(
                snapshot.items.len(),
                2,
                "both extension-distinct media should scan"
            );
            let mut keys = TargetReservationSet {
                private_library_identity: Some(manager.library_dir_identity),
                ..TargetReservationSet::default()
            };
            let first_nfo = with_extension(Path::new(first), "nfo");
            let second_nfo = with_extension(Path::new(second), "nfo");
            let first_key = keys
                .key_for(&manager.root, &path_to_relative_string(&first_nfo).unwrap())
                .unwrap();
            let second_key = keys
                .key_for(
                    &manager.root,
                    &path_to_relative_string(&second_nfo).unwrap(),
                )
                .unwrap();
            let aliased = first_key == second_key;
            let patch = MetadataPatch {
                item_id: snapshot.items[0].id.clone(),
                title: Some("Alias title".to_string()),
                source_ids: Vec::new(),
                collection: None,
                fill_missing_only: false,
                organization_status: None,
            };
            let result =
                manager.preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[patch]);
            assert_eq!(
                result.is_err(),
                aliased,
                "preview should follow the rooted filesystem's NFO target-name equivalence"
            );
        }

        check_alias_pair("nfo-case-alias", "Clip.mp4", "clip.mkv");
        check_alias_pair("nfo-normalization-alias", "Cafe\u{301}.mp4", "Café.mkv");
    }

    #[test]
    fn metadata_patch_reservation_allows_distinct_targets_and_same_item_move() {
        let root = TemporaryRoot::new("nfo-distinct-and-move");
        fs::write(root.path().join("one.mp4"), b"first-video").unwrap();
        fs::write(
            root.path().join("one.nfo"),
            "<movie><title>One</title></movie>",
        )
        .unwrap();
        fs::write(root.path().join("two.mkv"), b"second-video").unwrap();
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let one = item_at(&snapshot, "one.mp4");
        let two = item_at(&snapshot, "two.mkv");
        let patch_for = |item_id: &str, title: &str| MetadataPatch {
            item_id: item_id.to_string(),
            title: Some(title.to_string()),
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };

        let distinct = manager
            .preview_with_patches(
                &[],
                None,
                false,
                ConflictPolicy::KeepBoth,
                &[
                    patch_for(&one.id, "One updated"),
                    patch_for(&two.id, "Two updated"),
                ],
            )
            .expect("distinct NFO targets should remain legal");
        assert_eq!(distinct.metadata_patches.len(), 2);

        let same_item_move = manager
            .preview_with_hints_and_patches(
                std::slice::from_ref(&one.id),
                Some("Moved"),
                false,
                ConflictPolicy::KeepBoth,
                &[],
                &[patch_for(&one.id, "One moved")],
            )
            .expect("a patch may share its own item's planned NFO move target");
        assert_eq!(same_item_move.items[0].files.len(), 2);
        assert_eq!(same_item_move.metadata_patches[0].nfo_path, "Moved/one.nfo");
    }

    #[test]
    fn metadata_patch_target_conflicts_with_another_items_planned_nfo_move() {
        let root = TemporaryRoot::new("nfo-cross-item-move");
        fs::create_dir_all(root.path().join("A")).unwrap();
        fs::create_dir_all(root.path().join("B")).unwrap();
        fs::write(root.path().join("A/clip.mp4"), b"first-video").unwrap();
        fs::write(
            root.path().join("A/clip.nfo"),
            "<movie><title>First</title></movie>",
        )
        .unwrap();
        fs::write(root.path().join("B/clip.mkv"), b"second-video").unwrap();
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let first = item_at(&snapshot, "A/clip.mp4");
        let second = item_at(&snapshot, "B/clip.mkv");
        let patch = MetadataPatch {
            item_id: second.id.clone(),
            title: Some("Second updated".to_string()),
            source_ids: Vec::new(),
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        assert!(
            manager
                .preview_with_hints_and_patches(
                    &[first.id.clone(), second.id.clone()],
                    Some("Moved"),
                    false,
                    ConflictPolicy::KeepBoth,
                    &[],
                    &[patch],
                )
                .is_err(),
            "another item's planned NFO attachment move must reserve the shared target"
        );
    }

    #[test]
    fn confirmed_legacy_import_selects_a_local_candidate_before_writing_nfo() {
        let root = TemporaryRoot::new("legacy-import");
        fs::write(root.path().join("episode.mp4"), b"fixture-video")
            .expect("media fixture should write");
        let original_nfo = "<movie><title>Old title</title><future>opaque</future><uniqueid type=\"bilibili\">BV1abcdefgh</uniqueid></movie>";
        fs::write(root.path().join("episode.nfo"), original_nfo).expect("NFO fixture should write");
        let manager = manager(&root);
        let report = manager
            .import_legacy("Episode One - https://www.bilibili.com/video/BV1abcdefgh?cid=12345")
            .expect("legacy text should parse into weak hints");
        assert!(report.needs_confirmation);
        assert_eq!(report.hints[0].origin, HintOrigin::LegacyText);
        assert!(report.hints[0].requires_confirmation);
        assert_eq!(report.hints[0].title.as_deref(), Some("Episode One"));
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.candidates[0].match_kind, "source_id");
        assert!(report.candidates[0].requires_confirmation);
        assert_eq!(
            fs::read_to_string(root.path().join("episode.nfo")).unwrap(),
            original_nfo
        );

        let candidate = &report.candidates[0];
        let hint = &report.hints[candidate.hint_index];
        let patch = MetadataPatch {
            item_id: candidate.item_id.clone(),
            title: hint.title.clone(),
            source_ids: hint.source_ids.clone(),
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        let preview = manager
            .preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[patch])
            .expect("explicitly selected legacy metadata should preview");
        assert!(preview.metadata_patches[0].changed);
        assert_eq!(
            fs::read_to_string(root.path().join("episode.nfo")).unwrap(),
            original_nfo
        );
        manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("user-confirmed legacy patch should apply");
        let updated = fs::read_to_string(root.path().join("episode.nfo")).unwrap();
        assert!(updated.contains("<title>Episode One</title>"));
        assert!(updated.contains("<future>opaque</future>"));
    }

    #[test]
    fn metadata_only_patch_is_bound_to_selected_media_and_allows_touch_only_change() {
        let root = TemporaryRoot::new("metadata-only-media-identity");
        fs::write(root.path().join("clip.mp4"), b"approved-media")
            .expect("media fixture should write");
        let original_nfo = "<movie><title>Original</title><future>untouched</future></movie>";
        fs::write(root.path().join("clip.nfo"), original_nfo).expect("NFO fixture should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("scan should complete");
        let patch = MetadataPatch {
            item_id: item_at(&snapshot, "clip.mp4").id.clone(),
            title: Some("Confirmed".to_string()),
            source_ids: vec!["BV1abcdefgh".to_string()],
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        let preview = manager
            .preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[patch])
            .expect("metadata-only preview should bind media and NFO");
        let file = fs::OpenOptions::new()
            .write(true)
            .open(root.path().join("clip.mp4"))
            .expect("media should open for timestamp-only transition");
        file.set_modified(SystemTime::now() - Duration::from_secs(7_200))
            .expect("media mtime should update without changing content");
        let persisted = manager
            .read_persisted_preview(&preview.id)
            .expect("metadata preview should remain persisted");
        let expected_media = &persisted.patches[0].media_identity;
        let current_media = manager
            .read_file_with_token(&root.path().join("clip.mp4"), None)
            .expect("touched media identity should be readable")
            .expect("touched media should remain present")
            .0;
        assert!(
            identity_tokens_equal_except_birth_time(expected_media, &current_media)
                .expect("both media tokens should be valid"),
            "mtime-only transition changed more than birth time: expected={expected_media}; current={current_media}"
        );
        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("touch-only metadata confirmation should succeed");
        let updated = fs::read_to_string(root.path().join("clip.nfo")).unwrap();
        assert!(
            updated.contains("<title>Confirmed</title>"),
            "batch result: {result:#?}; updated NFO: {updated}"
        );
        assert!(updated.contains("<future>untouched</future>"));

        let replaced = TemporaryRoot::new("metadata-only-replacement");
        fs::write(replaced.path().join("clip.mp4"), b"approved-media")
            .expect("replacement fixture should write");
        fs::write(replaced.path().join("clip.nfo"), original_nfo)
            .expect("NFO fixture should write");
        let replaced_manager = self::manager(&replaced);
        let replaced_snapshot = replaced_manager.scan(&[]).unwrap();
        let replacement_patch = MetadataPatch {
            item_id: item_at(&replaced_snapshot, "clip.mp4").id.clone(),
            title: Some("Must not attach to replacement".to_string()),
            source_ids: vec!["BV1abcdefgh".to_string()],
            collection: None,
            fill_missing_only: false,
            organization_status: None,
        };
        let replacement_preview = replaced_manager
            .preview_with_patches(&[], None, false, ConflictPolicy::Skip, &[replacement_patch])
            .expect("replacement fixture should preview");
        fs::write(replaced.path().join("clip.mp4"), b"changed-media!")
            .expect("same-size media content should change");
        let result = replaced_manager
            .execute_preview(&replacement_preview.id, &replacement_preview.revision)
            .expect("stale metadata patch should be reported as a failed batch");
        assert_eq!(result.status, BatchStatus::Failed);
        assert_eq!(
            fs::read_to_string(replaced.path().join("clip.nfo")).unwrap(),
            original_nfo
        );
        release_preview_anchors(&replacement_preview.id);
    }

    #[test]
    fn multi_part_and_collection_layouts_are_flat_inside_their_named_folder() {
        let root = TemporaryRoot::new("organized-layout");
        fs::write(root.path().join("one.mp4"), b"video-one").unwrap();
        fs::write(
            root.path().join("one.nfo"),
            "<movie><title>Episode One</title><set>Series</set><episode>1</episode></movie>",
        )
        .unwrap();
        fs::write(root.path().join("two.mp4"), b"video-two").unwrap();
        fs::write(
            root.path().join("two.nfo"),
            "<movie><title>Episode Two</title><set>Series</set><episode>2</episode></movie>",
        )
        .unwrap();
        fs::write(root.path().join("solo_cid11.mp4"), b"part-one").unwrap();
        fs::write(root.path().join("solo_cid22.mp4"), b"part-two").unwrap();
        let manager = manager(&root);
        let hints = [
            queue_hint(
                &["BV1abcdefgh", "cid11"],
                CollectionMetadata {
                    id: Some("BV1abcdefgh".to_string()),
                    title: "Independent Series".to_string(),
                    kind: "multi_part".to_string(),
                    order: Some(1),
                    part_id: Some("P1".to_string()),
                },
            ),
            queue_hint(
                &["BV1abcdefgh", "cid22"],
                CollectionMetadata {
                    id: Some("BV1abcdefgh".to_string()),
                    title: "Independent Series".to_string(),
                    kind: "multi_part".to_string(),
                    order: Some(2),
                    part_id: Some("P2".to_string()),
                },
            ),
        ];
        let snapshot = manager
            .scan(&hints)
            .expect("scan should merge exact CID hints");
        let series_ids = snapshot
            .items
            .iter()
            .filter(|item| {
                item.collection
                    .as_ref()
                    .is_some_and(|value| value.title == "Series")
            })
            .map(|item| item.id.clone())
            .collect::<Vec<_>>();
        let multi_ids = snapshot
            .items
            .iter()
            .filter(|item| {
                item.collection
                    .as_ref()
                    .is_some_and(|value| value.title == "Independent Series")
            })
            .map(|item| item.id.clone())
            .collect::<Vec<_>>();
        let series_preview = manager
            .preview(&series_ids, "Organized", true, ConflictPolicy::KeepBoth)
            .expect("collection preview should build");
        assert!(
            series_preview
                .items
                .iter()
                .all(|item| item.target_path.starts_with("Organized/Series/"))
        );
        assert!(
            series_preview
                .items
                .iter()
                .any(|item| item.target_path.contains("P01 - Episode One"))
        );
        assert!(
            series_preview
                .items
                .iter()
                .any(|item| item.target_path.contains("P02 - Episode Two"))
        );

        let already_inside = manager
            .preview(&series_ids[..1], "Series", false, ConflictPolicy::KeepBoth)
            .expect("existing collection folder should not nest twice");
        assert_eq!(already_inside.items[0].target_path, "Series/one.mp4");

        let multi_preview = manager
            .preview_with_hints_and_patches(
                &multi_ids,
                Some("Organized"),
                true,
                ConflictPolicy::KeepBoth,
                &hints,
                &[],
            )
            .expect("independent multi-part preview should build");
        assert!(multi_preview.items.iter().all(|item| {
            item.target_path
                .starts_with("Organized/Independent Series/")
        }));
        assert!(
            multi_preview
                .items
                .iter()
                .any(|item| item.target_path.contains("P01 - solo cid11")),
            "multi-part preview targets: {:#?}",
            multi_preview
                .items
                .iter()
                .map(|item| &item.target_path)
                .collect::<Vec<_>>()
        );
        assert!(
            multi_preview
                .items
                .iter()
                .any(|item| item.target_path.contains("P02 - solo cid22")),
            "multi-part preview targets: {:#?}",
            multi_preview
                .items
                .iter()
                .map(|item| &item.target_path)
                .collect::<Vec<_>>()
        );
    }

    fn organized_collection_fixture(
        item_id: &str,
        title: &str,
        collection_kind: &str,
        collection_id: Option<&str>,
    ) -> LibraryItem {
        let mut item = index_fixture_item(item_id, "clip.mp4", Presence::Present);
        item.collection = Some(CollectionMetadata {
            id: collection_id.map(str::to_string),
            title: title.to_string(),
            kind: collection_kind.to_string(),
            order: None,
            part_id: None,
        });
        item
    }

    fn organized_test_case_fold(value: &str) -> Result<String> {
        let mut folded = String::new();
        for character in value.chars() {
            match character {
                'ß' | 'ẞ' => folded.push_str("ss"),
                'ς' => folded.push('σ'),
                _ => folded.extend(character.to_lowercase()),
            }
        }
        Ok(folded)
    }

    fn organized_test_target_key(
        path: &Path,
        semantics: TargetNameSemantics,
    ) -> Result<TargetReservationKey> {
        let components = path
            .components()
            .map(|component| match component {
                Component::Normal(name) => {
                    let name = name
                        .to_str()
                        .ok_or_else(|| anyhow!("test target component is not UTF-8"))?;
                    target_component_key_with(name, semantics, organized_test_case_fold)
                }
                _ => bail!("test target path has an unexpected component"),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(TargetReservationKey {
            directory_device: 7,
            directory_inode: 9,
            components,
        })
    }

    #[test]
    fn organized_collection_directories_disambiguate_sanitized_ids_and_truncated_titles() {
        let mut long_title_a = "x".repeat(150);
        long_title_a.push_str("-first");
        let mut long_title_b = "x".repeat(150);
        long_title_b.push_str("-second");
        let items = vec![
            organized_collection_fixture("one", "AC/DC", "series", Some("set/a")),
            organized_collection_fixture("two", "AC:DC", "series", Some("set:a")),
            organized_collection_fixture("three", "No/ID", "series", None),
            organized_collection_fixture("four", "No:ID", "series", None),
            organized_collection_fixture("five", &long_title_a, "series", None),
            organized_collection_fixture("six", &long_title_b, "series", None),
        ];
        let case_insensitive = TargetNameSemantics {
            normalization_insensitive: true,
            case_insensitive: true,
            hfs_ignorable: false,
        };
        let mut key_for = |path: &Path| organized_test_target_key(path, case_insensitive);
        let targets =
            organized_target_directories_with_key(&items, Path::new("Organized"), &mut key_for)
                .expect("colliding collection names should receive deterministic folders");

        for (first, second) in [("one", "two"), ("three", "four"), ("five", "six")] {
            let first_target = targets.get(first).unwrap();
            let second_target = targets.get(second).unwrap();
            assert_ne!(first_target, second_target);
            let first_name = first_target.file_name().unwrap().to_str().unwrap();
            let second_name = second_target.file_name().unwrap().to_str().unwrap();
            assert!(first_name.len() <= 217, "component was {first_name:?}");
            assert!(second_name.len() <= 217, "component was {second_name:?}");
            assert!(first_name.ends_with(']'));
            assert!(second_name.ends_with(']'));
        }
        assert_eq!(
            safe_file_stem(&"x".repeat(150)).len() + 2 + 64 + 1,
            217,
            "the largest disambiguated component must fit the 217-byte budget"
        );
    }

    #[test]
    fn organized_collection_directories_follow_case_and_unicode_target_semantics() {
        let items = [
            organized_collection_fixture("upper", "Series", "series", Some("upper-id")),
            organized_collection_fixture("lower", "series", "series", Some("lower-id")),
            organized_collection_fixture("composed", "Café", "series", Some("composed-id")),
            organized_collection_fixture(
                "decomposed",
                "Cafe\u{301}",
                "series",
                Some("decomposed-id"),
            ),
        ];
        let case_sensitive = TargetNameSemantics {
            normalization_insensitive: true,
            case_insensitive: false,
            hfs_ignorable: false,
        };
        let mut sensitive_key = |path: &Path| organized_test_target_key(path, case_sensitive);
        let sensitive_targets = organized_target_directories_with_key(
            &items[..2],
            Path::new("Organized"),
            &mut sensitive_key,
        )
        .expect("case-sensitive target names should preserve case distinctions");
        assert_eq!(sensitive_targets["upper"], Path::new("Organized/Series"));
        assert_eq!(sensitive_targets["lower"], Path::new("Organized/series"));

        let case_insensitive = TargetNameSemantics {
            case_insensitive: true,
            ..case_sensitive
        };
        let mut insensitive_key = |path: &Path| organized_test_target_key(path, case_insensitive);
        let insensitive_targets = organized_target_directories_with_key(
            &items[..2],
            Path::new("Organized"),
            &mut insensitive_key,
        )
        .expect("case-insensitive target names should disambiguate case aliases");
        assert_ne!(insensitive_targets["upper"], insensitive_targets["lower"]);

        let mut normalization_key = |path: &Path| organized_test_target_key(path, case_sensitive);
        let normalized_targets = organized_target_directories_with_key(
            &items[2..],
            Path::new("Organized"),
            &mut normalization_key,
        )
        .expect("normalization aliases should receive distinct stable names");
        assert_ne!(
            normalized_targets["composed"],
            normalized_targets["decomposed"]
        );
    }

    #[test]
    fn organized_collection_base_comparison_uses_target_name_semantics() {
        let item = organized_collection_fixture("one", "series", "series", Some("series-id"));
        let sensitive = TargetNameSemantics {
            normalization_insensitive: true,
            case_insensitive: false,
            hfs_ignorable: false,
        };
        let mut sensitive_key = |path: &Path| organized_test_target_key(path, sensitive);
        let sensitive_target = organized_target_directories_with_key(
            std::slice::from_ref(&item),
            Path::new("Series"),
            &mut sensitive_key,
        )
        .expect("case-sensitive base should preserve a distinct lowercase folder");
        assert_eq!(sensitive_target["one"], Path::new("Series/series"));

        let insensitive = TargetNameSemantics {
            case_insensitive: true,
            ..sensitive
        };
        let mut insensitive_key = |path: &Path| organized_test_target_key(path, insensitive);
        let insensitive_target = organized_target_directories_with_key(
            std::slice::from_ref(&item),
            Path::new("Series"),
            &mut insensitive_key,
        )
        .expect("case-insensitive base alias should avoid an unnecessary nested folder");
        assert_eq!(insensitive_target["one"], Path::new("Series"));
    }

    #[test]
    fn items_with_the_same_collection_identity_share_one_organized_folder() {
        let items = vec![
            organized_collection_fixture("one", "Series A", "series", Some("same-id")),
            organized_collection_fixture("two", "Series B", "series", Some("same-id")),
        ];
        let case_sensitive = TargetNameSemantics {
            normalization_insensitive: true,
            case_insensitive: false,
            hfs_ignorable: false,
        };
        let mut key_for = |path: &Path| organized_test_target_key(path, case_sensitive);
        let targets =
            organized_target_directories_with_key(&items, Path::new("Organized"), &mut key_for)
                .expect("same collection identity should map to one folder");
        assert_eq!(targets["one"], targets["two"]);
    }

    #[test]
    fn collection_identity_keeps_id_and_title_namespaces_distinct() {
        let items = vec![
            organized_collection_fixture("with-id", "One", "series", Some("Topic")),
            organized_collection_fixture("without-id", "Topic", "series", None),
        ];
        let semantics = TargetNameSemantics {
            normalization_insensitive: true,
            case_insensitive: false,
            hfs_ignorable: false,
        };
        let mut key_for = |path: &Path| organized_test_target_key(path, semantics);
        let targets =
            organized_target_directories_with_key(&items, Path::new("Organized"), &mut key_for)
                .expect("ID and title fallback values should remain distinct identities");
        assert_eq!(targets["with-id"], Path::new("Organized/One"));
        assert_eq!(targets["without-id"], Path::new("Organized/Topic"));
    }

    #[test]
    fn organized_collection_collision_retry_limit_bounds_new_owner_chains() {
        let mut items = vec![
            organized_collection_fixture("first", "A", "series", Some("first-id")),
            organized_collection_fixture("second", "B", "series", Some("second-id")),
        ];
        for index in 0..9 {
            items.push(organized_collection_fixture(
                &format!("item-{index}"),
                &format!("Title{index}"),
                "series",
                Some(&format!("id-{index}")),
            ));
        }

        let mut assigned_keys = HashMap::<String, usize>::new();
        assigned_keys.insert("A".to_string(), 0);
        assigned_keys.insert("B".to_string(), 0);
        for index in 0..9 {
            assigned_keys.insert(format!("Title{index}"), index + 1);
        }
        for item in &items {
            let collection = item.collection.as_ref().unwrap();
            let identity = collection_directory_identity(collection).unwrap();
            let discriminator = collection_directory_discriminator(&identity, 0).unwrap();
            let stem = safe_file_stem(&collection.title);
            let next_key = match collection.title.as_str() {
                "A" => 1,
                "B" => 100,
                title => {
                    let index = title
                        .strip_prefix("Title")
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    index + 2
                }
            };
            assigned_keys.insert(format!("{stem} [{discriminator}]"), next_key);
        }

        let calls = AtomicUsize::new(0);
        let mut key_for = |path: &Path| {
            calls.fetch_add(1, Ordering::SeqCst);
            let name = path.file_name().and_then(OsStr::to_str).unwrap_or_default();
            let key = assigned_keys.get(name).copied().unwrap_or(1_000);
            Ok(TargetReservationKey {
                directory_device: 7,
                directory_inode: 9,
                components: vec![format!("target-{key}")],
            })
        };
        let result =
            organized_target_directories_with_key(&items, Path::new("Organized"), &mut key_for);
        assert!(
            result.is_err(),
            "a multi-round collision chain should fail closed at the fixed pass limit"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            items.len() * 10,
            "the fixture adds one newly hashed owner per pass and must stop after eight retries"
        );
    }

    #[test]
    fn touch_only_preview_change_is_allowed_but_same_size_content_change_is_rejected() {
        let root = TemporaryRoot::new("touch-content");
        fs::write(root.path().join("touch.mp4"), b"same-content")
            .expect("touch fixture should write");
        let initial = manager(&root);
        let snapshot = initial.scan(&[]).expect("touch fixture should scan");
        let item_id = item_at(&snapshot, "touch.mp4").id.clone();
        let preview = initial
            .preview(&[item_id], "Moved", false, ConflictPolicy::KeepBoth)
            .expect("touch preview should build");
        let file = fs::OpenOptions::new()
            .write(true)
            .open(root.path().join("touch.mp4"))
            .expect("source should open for benign timestamp update");
        file.set_modified(SystemTime::now() - Duration::from_secs(24 * 60 * 60))
            .expect("source mtime should update");
        let expected_token = preview.items[0].files[0].identity.as_str();
        let current_token = initial
            .read_file_with_token(&root.path().join("touch.mp4"), None)
            .expect("touched source identity should be readable")
            .expect("touched source should remain present")
            .0;
        assert!(
            identity_tokens_equal_except_birth_time(expected_token, &current_token)
                .expect("both source tokens should be valid"),
            "mtime-only transition changed more than birth time: expected={expected_token}; current={current_token}"
        );
        drop(initial);
        let reopened = manager(&root);
        let result = reopened
            .execute_preview(&preview.id, &preview.revision)
            .expect("touch-only transition should preserve the approved content");
        assert_eq!(
            result.status,
            BatchStatus::Complete,
            "touch-only move batch result: {result:#?}"
        );
        assert_eq!(
            fs::read(root.path().join("Moved/touch.mp4")).unwrap(),
            b"same-content"
        );

        let changed = TemporaryRoot::new("content-changed");
        fs::write(changed.path().join("clip.mp4"), b"same-length")
            .expect("mutation fixture should write");
        let changed_manager = manager(&changed);
        let changed_snapshot = changed_manager.scan(&[]).unwrap();
        let changed_id = item_at(&changed_snapshot, "clip.mp4").id.clone();
        let changed_preview = changed_manager
            .preview(&[changed_id], "Moved", false, ConflictPolicy::KeepBoth)
            .unwrap();
        fs::write(changed.path().join("clip.mp4"), b"changedbyte")
            .expect("same-size content replacement should write");
        let rejected = changed_manager
            .execute_preview(&changed_preview.id, &changed_preview.revision)
            .expect("stale selection should return a failed batch result");
        assert_eq!(rejected.status, BatchStatus::Failed);
        assert_eq!(
            fs::read(changed.path().join("clip.mp4")).unwrap(),
            b"changedbyte"
        );
        assert!(!changed.path().join("Moved/clip.mp4").exists());
        release_preview_anchors(&changed_preview.id);
    }

    #[test]
    fn pinned_birth_time_exception_rejects_access_policy_and_object_replacement() {
        let permission_root = TemporaryRoot::new("birth-time-permission-change");
        fs::write(permission_root.path().join("clip.mp4"), b"same-content")
            .expect("permission fixture should write");
        fs::set_permissions(
            permission_root.path().join("clip.mp4"),
            fs::Permissions::from_mode(0o644),
        )
        .expect("permission fixture should have a stable initial mode");
        let permission_manager = manager(&permission_root);
        let permission_snapshot = permission_manager.scan(&[]).unwrap();
        let permission_item = item_at(&permission_snapshot, "clip.mp4").id.clone();
        let permission_preview = permission_manager
            .preview(&[permission_item], "Moved", false, ConflictPolicy::KeepBoth)
            .expect("permission fixture should preview");
        fs::set_permissions(
            permission_root.path().join("clip.mp4"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("permission fixture should change its mode");
        let permission_result = permission_manager
            .execute_preview(&permission_preview.id, &permission_preview.revision)
            .expect("permission change should be reported as a failed batch");
        assert_eq!(permission_result.status, BatchStatus::Failed);
        assert!(permission_root.path().join("clip.mp4").exists());
        assert!(!permission_root.path().join("Moved/clip.mp4").exists());
        release_preview_anchors(&permission_preview.id);

        let replacement_root = TemporaryRoot::new("birth-time-object-replacement");
        let source = replacement_root.path().join("clip.mp4");
        fs::write(&source, b"approved-data").expect("replacement fixture should write");
        let replacement_manager = manager(&replacement_root);
        let replacement_snapshot = replacement_manager.scan(&[]).unwrap();
        let replacement_item = item_at(&replacement_snapshot, "clip.mp4").id.clone();
        let replacement_preview = replacement_manager
            .preview(
                &[replacement_item],
                "Moved",
                false,
                ConflictPolicy::KeepBoth,
            )
            .expect("replacement fixture should preview");
        let replacement = replacement_root.path().join("replacement.mp4");
        fs::write(&replacement, b"replacement-data").expect("replacement object should write");
        fs::rename(&replacement, &source).expect("replacement object should replace the path");
        let replacement_result = replacement_manager
            .execute_preview(&replacement_preview.id, &replacement_preview.revision)
            .expect("object replacement should be reported as a failed batch");
        assert_eq!(replacement_result.status, BatchStatus::Failed);
        assert_eq!(fs::read(&source).unwrap(), b"replacement-data");
        assert!(!replacement_root.path().join("Moved/clip.mp4").exists());
        release_preview_anchors(&replacement_preview.id);
    }

    #[test]
    fn pinned_birth_time_exception_requires_exact_content_and_access_signals() {
        let root = TemporaryRoot::new("birth-time-token-proof");
        fs::write(root.path().join("clip.mp4"), b"same-content")
            .expect("identity fixture should write");
        let manager = manager(&root);
        let file = manager
            .read_file_with_token_and_handle(&root.path().join("clip.mp4"), None)
            .expect("identity file should open")
            .expect("identity file should exist");
        let identity = file.file.identity();
        let fields = file.token.split(':').collect::<Vec<_>>();
        let mut changed_birth = fields
            .iter()
            .map(|field| (*field).to_string())
            .collect::<Vec<_>>();
        changed_birth[7] = changed_birth[7]
            .parse::<i64>()
            .map(|value| (value + 1).to_string())
            .unwrap_or_else(|_| "0".to_string());
        changed_birth[8] = if changed_birth[8] == "-" {
            "1".to_string()
        } else {
            changed_birth[8]
                .parse::<i64>()
                .map(|value| ((value + 1) % 1_000_000_000).to_string())
                .unwrap_or_else(|_| "1".to_string())
        };
        let changed_birth = changed_birth.join(":");
        assert!(
            identity_tokens_match_with_pinned_file(
                &file.token,
                &changed_birth,
                &file.file,
                Some(identity),
                Some(&file.file),
            )
            .expect("birth-only comparison should be valid")
        );
        assert!(
            !identity_tokens_match_with_pinned_file(
                &file.token,
                &changed_birth,
                &file.file,
                Some(identity),
                None,
            )
            .expect("missing anchor should be a normal mismatch")
        );
        assert!(
            !identity_tokens_match_with_pinned_file(
                &file.token,
                &changed_birth,
                &file.file,
                None,
                Some(&file.file),
            )
            .expect("missing rooted path identity should be a normal mismatch")
        );

        for (field, replacement) in [(4, "384".to_string()), (9, "0".repeat(64))] {
            let mut changed = fields
                .iter()
                .map(|value| (*value).to_string())
                .collect::<Vec<_>>();
            changed[field] = replacement;
            let changed = changed.join(":");
            assert!(
                !identity_tokens_match_with_pinned_file(
                    &file.token,
                    &changed,
                    &file.file,
                    Some(identity),
                    Some(&file.file),
                )
                .expect("protected-field mismatch should be a normal rejection")
            );
        }
    }

    #[derive(Default)]
    struct FailSecondMoveProvider {
        move_count: AtomicUsize,
    }

    impl QueueFileProvider for FailSecondMoveProvider {
        fn coordinate_read(
            &self,
            path: &Path,
            accessor: &mut dyn FnMut(&Path) -> Result<()>,
        ) -> Result<()> {
            accessor(path)
        }

        fn coordinate_write(
            &self,
            path: &Path,
            accessor: &mut dyn FnMut(&Path) -> Result<()>,
        ) -> Result<()> {
            accessor(path)
        }

        fn coordinate_move(
            &self,
            source: &Path,
            destination: &Path,
            accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
        ) -> Result<()> {
            if self.move_count.fetch_add(1, Ordering::SeqCst) == 1 {
                bail!("injected one-time failure after the first attachment move");
            }
            accessor(source, destination)
        }
    }

    #[test]
    fn partial_attachment_move_retries_without_deleting_remaining_source() {
        let root = TemporaryRoot::new("partial-recovery");
        fs::write(root.path().join("clip.mp4"), b"primary-video")
            .expect("primary media should write");
        fs::write(root.path().join("clip.srt"), b"subtitle")
            .expect("subtitle attachment should write");
        let manager = LibraryManager::open_with_provider(
            root.path(),
            Arc::new(FailSecondMoveProvider::default()),
        )
        .expect("library manager should open");
        let snapshot = manager
            .scan(&[])
            .expect("scan should find primary and subtitle");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let preview = manager
            .preview(&[item_id], "Moved", false, ConflictPolicy::KeepBoth)
            .expect("group move should preview");
        let partial = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("first attempt should persist a partial batch result");
        assert_eq!(partial.status, BatchStatus::Partial);
        assert!(root.path().join("Moved/clip.srt").exists());
        assert!(root.path().join("clip.mp4").exists());
        assert!(!root.path().join("Moved/clip.mp4").exists());

        let complete = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("retry should resume remaining files");
        assert_eq!(complete.status, BatchStatus::Complete);
        assert!(root.path().join("Moved/clip.srt").exists());
        assert!(root.path().join("Moved/clip.mp4").exists());
        assert!(!root.path().join("clip.mp4").exists());
        assert!(!root.path().join("clip.srt").exists());
    }

    #[derive(Default)]
    struct FailSecondMoveWithAccessErrorProvider {
        move_count: AtomicUsize,
    }

    impl QueueFileProvider for FailSecondMoveWithAccessErrorProvider {
        fn coordinate_read(
            &self,
            path: &Path,
            accessor: &mut dyn FnMut(&Path) -> Result<()>,
        ) -> Result<()> {
            accessor(path)
        }

        fn coordinate_write(
            &self,
            path: &Path,
            accessor: &mut dyn FnMut(&Path) -> Result<()>,
        ) -> Result<()> {
            accessor(path)
        }

        fn coordinate_move(
            &self,
            source: &Path,
            destination: &Path,
            accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
        ) -> Result<()> {
            if self.move_count.fetch_add(1, Ordering::SeqCst) == 1 {
                return Err(crate::file_provider::FileProviderAccessError::new(
                    source,
                    "move",
                    "injected transient access failure",
                )
                .into());
            }
            accessor(source, destination)
        }
    }

    #[test]
    fn partial_file_provider_access_error_is_durable_and_retryable_after_reopen() {
        let root = TemporaryRoot::new("partial-file-provider-retryable");
        fs::write(root.path().join("clip.mp4"), b"primary-video")
            .expect("primary media should write");
        fs::write(root.path().join("clip.srt"), b"subtitle")
            .expect("subtitle attachment should write");
        let provider_impl = Arc::new(FailSecondMoveWithAccessErrorProvider::default());
        let provider: Arc<dyn QueueFileProvider> = provider_impl.clone();
        let manager = LibraryManager::open_with_provider(root.path(), Arc::clone(&provider))
            .expect("library manager should open");
        let snapshot = manager
            .scan(&[])
            .expect("scan should find primary and subtitle");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let preview = manager
            .preview(&[item_id], "Moved", false, ConflictPolicy::KeepBoth)
            .expect("group move should preview");

        let partial = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("transient File Provider error should persist a partial result");
        assert_eq!(partial.status, BatchStatus::Partial);
        assert!(partial.retryable_access_error);
        let manifest_path = manager.batch_path(&preview.id);
        let persisted = manager
            .read_private_file(&manifest_path, MANIFEST_LIMIT)
            .expect("partial manifest should be readable")
            .expect("partial manifest should be persisted");
        let manifest: BatchManifest =
            serde_json::from_slice(&persisted).expect("partial manifest should parse");
        assert_eq!(manifest.status, BatchStatus::Partial);
        assert!(manifest.retryable_access_error);
        assert!(root.path().join("Moved/clip.srt").exists());
        assert!(root.path().join("clip.mp4").exists());

        drop(manager);
        let reopened = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should reopen with the same provider");
        let complete = reopened
            .execute_preview(&preview.id, &preview.revision)
            .expect("the persisted partial batch should resume");
        assert_eq!(complete.status, BatchStatus::Complete);
        assert!(!complete.retryable_access_error);
        assert!(root.path().join("Moved/clip.srt").exists());
        assert!(root.path().join("Moved/clip.mp4").exists());
        assert!(!root.path().join("clip.mp4").exists());
        let persisted = reopened
            .read_private_file(&manifest_path, MANIFEST_LIMIT)
            .expect("completed manifest should be readable")
            .expect("completed manifest should remain persisted");
        let manifest: BatchManifest =
            serde_json::from_slice(&persisted).expect("completed manifest should parse");
        assert_eq!(manifest.status, BatchStatus::Complete);
        assert!(!manifest.retryable_access_error);
        assert_eq!(provider_impl.move_count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn partial_retry_refuses_to_attach_remaining_files_to_a_replaced_moved_target() {
        let root = TemporaryRoot::new("partial-target-replaced");
        fs::write(root.path().join("clip.mp4"), b"primary-video")
            .expect("primary media should write");
        fs::write(root.path().join("clip.srt"), b"approved subtitle")
            .expect("subtitle attachment should write");
        let manager = LibraryManager::open_with_provider(
            root.path(),
            Arc::new(FailSecondMoveProvider::default()),
        )
        .expect("library manager should open");
        let snapshot = manager
            .scan(&[])
            .expect("scan should find primary and subtitle");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let preview = manager
            .preview(&[item_id], "Moved", false, ConflictPolicy::KeepBoth)
            .expect("group move should preview");
        assert_eq!(
            manager
                .execute_preview(&preview.id, &preview.revision)
                .unwrap()
                .status,
            BatchStatus::Partial
        );
        let moved_subtitle = root.path().join("Moved/clip.srt");
        fs::rename(&moved_subtitle, root.path().join("Moved/clip.srt.saved"))
            .expect("preserve the previously moved approved subtitle");
        fs::write(&moved_subtitle, b"replacement subtitle")
            .expect("unapproved replacement should be present");

        let retry = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("unsafe partial retry should return a failed step");
        assert_eq!(retry.status, BatchStatus::Partial);
        assert!(root.path().join("clip.mp4").exists());
        assert!(!root.path().join("Moved/clip.mp4").exists());
        assert_eq!(fs::read(&moved_subtitle).unwrap(), b"replacement subtitle");
        assert_eq!(
            fs::read(root.path().join("Moved/clip.srt.saved")).unwrap(),
            b"approved subtitle"
        );
    }

    fn round4_scanned_file(relative_path: &str, inode: u64) -> ScannedFile {
        ScannedFile {
            relative_path: relative_path.to_string(),
            identity: EntryIdentity::regular_file(1, inode),
        }
    }

    fn round4_semantics_by_parent(
        files: &[ScannedFile],
        semantics: TargetNameSemantics,
    ) -> HashMap<String, TargetNameSemantics> {
        files
            .iter()
            .map(|file| {
                let parent = Path::new(&file.relative_path)
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    .to_string_lossy()
                    .into_owned();
                (parent, semantics)
            })
            .collect()
    }

    fn round4_sensitive_semantics() -> TargetNameSemantics {
        TargetNameSemantics {
            normalization_insensitive: false,
            case_insensitive: false,
            hfs_ignorable: false,
        }
    }

    #[derive(Clone, Copy)]
    enum Round4IndexMutation {
        ChildEntryChurn,
        SameSizeContentChange,
        ObjectReplacement,
        AccessPolicyChange,
    }

    struct Round4IndexMutationProvider {
        mutation: Round4IndexMutation,
        applied: AtomicUsize,
    }

    impl Round4IndexMutationProvider {
        fn new(mutation: Round4IndexMutation) -> Self {
            Self {
                mutation,
                applied: AtomicUsize::new(0),
            }
        }

        fn mutate_index_before_quarantine(&self, source: &Path) -> Result<()> {
            match self.mutation {
                Round4IndexMutation::ChildEntryChurn => {
                    let marker = source
                        .parent()
                        .ok_or_else(|| anyhow!("index source should have a parent"))?
                        .join("unrelated-child-entry");
                    fs::write(marker, b"child churn")?;
                }
                Round4IndexMutation::SameSizeContentChange => {
                    use std::io::{Seek, SeekFrom, Write};

                    let mut contents = fs::read(source)?;
                    contents[0] = b'[';
                    let mut file = fs::OpenOptions::new().write(true).open(source)?;
                    file.seek(SeekFrom::Start(0))?;
                    file.write_all(&contents)?;
                    file.sync_all()?;
                }
                Round4IndexMutation::ObjectReplacement => {
                    let displaced = source.with_file_name("index.replaced");
                    fs::rename(source, &displaced)?;
                    fs::write(source, b"{\"version\":1,\"records\":{}}")?;
                    fs::set_permissions(source, fs::Permissions::from_mode(0o600))?;
                }
                Round4IndexMutation::AccessPolicyChange => {
                    fs::set_permissions(source, fs::Permissions::from_mode(0o644))?;
                }
            }
            Ok(())
        }
    }

    impl QueueFileProvider for Round4IndexMutationProvider {
        fn coordinate_read(
            &self,
            path: &Path,
            accessor: &mut dyn FnMut(&Path) -> Result<()>,
        ) -> Result<()> {
            accessor(path)
        }

        fn coordinate_write(
            &self,
            path: &Path,
            accessor: &mut dyn FnMut(&Path) -> Result<()>,
        ) -> Result<()> {
            accessor(path)
        }

        fn coordinate_move(
            &self,
            source: &Path,
            destination: &Path,
            accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
        ) -> Result<()> {
            if source.file_name() == Some(OsStr::new(LIBRARY_INDEX))
                && self.applied.fetch_add(1, Ordering::SeqCst) == 0
            {
                self.mutate_index_before_quarantine(source)?;
            }
            accessor(source, destination)
        }
    }

    #[test]
    fn round4_sidecar_index_prefers_exact_stem_and_never_falls_back_from_ambiguity() {
        let files = vec![
            round4_scanned_file("episode.mp4", 1),
            round4_scanned_file("episode.en.mp4", 2),
            round4_scanned_file("episode.en.srt", 3),
            round4_scanned_file("duplicate.mp4", 4),
            round4_scanned_file("duplicate.mkv", 5),
            round4_scanned_file("duplicate.srt", 6),
            round4_scanned_file("episode.multi.mp4", 7),
            round4_scanned_file("episode.multi.mkv", 8),
            round4_scanned_file("episode.multi.en.srt", 9),
        ];
        let semantics = round4_semantics_by_parent(&files, round4_sensitive_semantics());
        let index = SidecarIndex::build(&files, &BTreeSet::new(), &semantics)
            .expect("filesystem name semantics should be available for every fixture parent");

        assert!(index.attached_files(&files[0]).is_empty());
        assert_eq!(
            index.attached_files(&files[1]),
            vec!["episode.en.srt".to_string()]
        );
        assert!(index.attached_files(&files[3]).is_empty());
        assert!(index.attached_files(&files[4]).is_empty());
        assert!(index.attached_files(&files[6]).is_empty());
        assert!(index.attached_files(&files[7]).is_empty());
        assert_eq!(index.ambiguous_count, 2);
    }

    #[test]
    fn round4_sidecar_index_scales_by_parent_and_stem_without_timing_assertions() {
        const MEDIA_COUNT: usize = 2048;
        let mut files = Vec::with_capacity(MEDIA_COUNT * 2);
        for number in 0..MEDIA_COUNT {
            let stem = format!("group-{number}/episode-{number}");
            files.push(round4_scanned_file(
                &format!("{stem}.mp4"),
                number as u64 + 1,
            ));
            files.push(round4_scanned_file(
                &format!("{stem}.srt"),
                number as u64 + MEDIA_COUNT as u64 + 1,
            ));
        }

        let semantics = round4_semantics_by_parent(&files, round4_sensitive_semantics());
        let index = SidecarIndex::build(&files, &BTreeSet::new(), &semantics)
            .expect("filesystem name semantics should be available for every fixture parent");

        assert_eq!(index.by_media.len(), MEDIA_COUNT);
        assert_eq!(index.ambiguous_count, 0);
        for number in [0, 1, 17, 1024, MEDIA_COUNT - 1] {
            let media = &files[number * 2];
            assert_eq!(
                index.attached_files(media),
                vec![format!("group-{number}/episode-{number}.srt")]
            );
        }
    }

    fn round5_full_fold_for_test(value: &str) -> Result<String> {
        let mut folded = String::new();
        for character in value.chars() {
            match character {
                'ß' | 'ẞ' => folded.push_str("ss"),
                'ς' => folded.push('σ'),
                _ => folded.extend(character.to_lowercase()),
            }
        }
        Ok(folded)
    }

    fn round5_semantics(
        case_insensitive: bool,
        normalization_insensitive: bool,
    ) -> TargetNameSemantics {
        TargetNameSemantics {
            normalization_insensitive,
            case_insensitive,
            hfs_ignorable: false,
        }
    }

    #[test]
    fn round5_private_downloader_names_follow_directory_case_and_hfs_rules() {
        let sensitive = round5_semantics(false, false);
        let insensitive = round5_semantics(true, true);
        let hfs_insensitive = TargetNameSemantics {
            hfs_ignorable: true,
            ..insensitive
        };

        assert!(
            is_private_downloader_component_with(
                ".TELEGRAM-VIDEO-DOWNLOADER-LIBRARY",
                insensitive,
                &round5_full_fold_for_test,
            )
            .expect("case-insensitive alias should normalize")
        );
        assert!(
            !is_private_downloader_component_with(
                ".TELEGRAM-VIDEO-DOWNLOADER-LIBRARY",
                sensitive,
                &round5_full_fold_for_test,
            )
            .expect("case-sensitive spelling should stay distinct")
        );
        assert!(
            is_private_downloader_component_with(
                ".telegram-video-downloader-\u{200c}library",
                hfs_insensitive,
                &round5_full_fold_for_test,
            )
            .expect("HFS comparison should remove the documented ignorable")
        );
        assert!(
            is_private_downloader_component_with(
                ".telegram-video-downloader-\u{200c}library",
                insensitive,
                &round5_full_fold_for_test,
            )
            .expect("an ignorable after the reserved prefix must remain private on APFS")
        );
        assert!(
            is_private_downloader_component_with(
                ".telegram-video-down\u{200c}loader-library",
                hfs_insensitive,
                &round5_full_fold_for_test,
            )
            .expect("HFS comparison should remove an ignorable inside the reserved prefix")
        );
        assert!(
            !is_private_downloader_component_with(
                ".telegram-video-down\u{200c}loader-library",
                insensitive,
                &round5_full_fold_for_test,
            )
            .expect("APFS comparison should preserve an HFS-only ignorable in the prefix")
        );

        let root = TemporaryRoot::new("round5-private-entry-identity");
        let manager = manager(&root);
        assert!(
            excluded_entry(
                OsStr::new("renamed-entry"),
                manager.library_dir_identity,
                sensitive,
                manager.library_dir_identity,
            )
            .expect("the saved private-directory identity should exclude aliases")
        );
    }

    #[test]
    fn round5_sidecar_index_uses_case_and_normalization_semantics_and_keeps_ambiguity() {
        let insensitive = round5_semantics(true, true);
        let insensitive_files = vec![
            round4_scanned_file("Movie.mp4", 1),
            round4_scanned_file("movie.en.srt", 2),
            round4_scanned_file("movie.zh.srt", 3),
        ];
        let insensitive_semantics = round4_semantics_by_parent(&insensitive_files, insensitive);
        let insensitive_index = SidecarIndex::build_with_case_fold(
            &insensitive_files,
            &BTreeSet::new(),
            &insensitive_semantics,
            round5_full_fold_for_test,
        )
        .expect("simulated insensitive directory should index attachments");
        assert_eq!(
            insensitive_index.attached_files(&insensitive_files[0]),
            vec!["movie.en.srt".to_string(), "movie.zh.srt".to_string()]
        );

        let sensitive_files = vec![
            round4_scanned_file("Movie.mp4", 4),
            round4_scanned_file("movie.en.srt", 5),
        ];
        let sensitive_semantics =
            round4_semantics_by_parent(&sensitive_files, round4_sensitive_semantics());
        let sensitive_index = SidecarIndex::build_with_case_fold(
            &sensitive_files,
            &BTreeSet::new(),
            &sensitive_semantics,
            round5_full_fold_for_test,
        )
        .expect("simulated sensitive directory should index attachments");
        assert!(
            sensitive_index
                .attached_files(&sensitive_files[0])
                .is_empty()
        );

        let normalized_files = vec![
            round4_scanned_file("café.mp4", 6),
            round4_scanned_file("cafe\u{301}.en.srt", 7),
        ];
        let normalized_semantics =
            round4_semantics_by_parent(&normalized_files, round5_semantics(false, true));
        let normalized_index = SidecarIndex::build_with_case_fold(
            &normalized_files,
            &BTreeSet::new(),
            &normalized_semantics,
            round5_full_fold_for_test,
        )
        .expect("normalization-insensitive directory should associate equivalent stems");
        assert_eq!(
            normalized_index.attached_files(&normalized_files[0]),
            vec!["cafe\u{301}.en.srt".to_string()]
        );

        let ambiguous_files = vec![
            round4_scanned_file("Movie.mp4", 8),
            round4_scanned_file("movie.mkv", 9),
            round4_scanned_file("movie.en.srt", 10),
        ];
        let ambiguous_semantics = round4_semantics_by_parent(&ambiguous_files, insensitive);
        let ambiguous_index = SidecarIndex::build_with_case_fold(
            &ambiguous_files,
            &BTreeSet::new(),
            &ambiguous_semantics,
            round5_full_fold_for_test,
        )
        .expect("filesystem-equivalent primary stems should remain safely ambiguous");
        assert!(
            ambiguous_index
                .attached_files(&ambiguous_files[0])
                .is_empty()
        );
        assert!(
            ambiguous_index
                .attached_files(&ambiguous_files[1])
                .is_empty()
        );
        assert_eq!(ambiguous_index.ambiguous_count, 1);

        let missing_semantics = HashMap::new();
        let error = SidecarIndex::build_with_case_fold(
            &sensitive_files,
            &BTreeSet::new(),
            &missing_semantics,
            round5_full_fold_for_test,
        )
        .expect_err("missing filesystem semantics must fail closed");
        assert!(format!("{error:#}").contains("filesystem name semantics unavailable"));
    }

    #[test]
    fn round5_attachment_suffix_preserves_language_qualifiers_after_folded_match() {
        let insensitive = round5_semantics(true, true);
        assert_eq!(
            matching_sidecar_suffix_with(
                "movie.en",
                "Movie",
                insensitive,
                round5_full_fold_for_test,
            )
            .expect("case alias should match"),
            Some(".en".to_string())
        );
        assert_eq!(
            matching_sidecar_suffix_with(
                "movie.zh",
                "Movie",
                insensitive,
                round5_full_fold_for_test,
            )
            .expect("case alias should match"),
            Some(".zh".to_string())
        );
        assert_eq!(
            matching_sidecar_suffix_with(
                "MASSE.en",
                "Maße",
                insensitive,
                round5_full_fold_for_test,
            )
            .expect("fold expansion should preserve the original suffix boundary"),
            Some(".en".to_string())
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn round5_private_path_guard_uses_detected_macos_name_semantics() {
        let root = TemporaryRoot::new("round5-macos-private-alias");
        let manager = manager(&root);
        let directory = File::open(root.path()).expect("temporary root should open");
        let semantics = target_name_semantics(&directory)
            .expect("temporary root should report supported macOS name semantics");
        let result =
            manager.safe_relative_path(".TELEGRAM-VIDEO-DOWNLOADER-LIBRARY/clip.mp4", false);
        assert_eq!(
            result.is_err(),
            semantics.case_insensitive,
            "the guard should reject the private-directory alias exactly when the volume folds case"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn round5_casefold_sidecars_keep_language_suffixes_during_move() {
        let root = TemporaryRoot::new("round5-macos-sidecar-suffix");
        fs::write(root.path().join("Movie.mp4"), b"video").expect("primary media should write");
        fs::write(root.path().join("movie.en.srt"), b"english")
            .expect("English sidecar should write");
        fs::write(root.path().join("movie.zh.srt"), b"Chinese")
            .expect("Chinese sidecar should write");
        let manager = manager(&root);
        let directory = File::open(root.path()).expect("temporary root should open");
        let semantics = target_name_semantics(&directory)
            .expect("temporary root should report supported macOS name semantics");
        let snapshot = manager.scan(&[]).expect("fixture scan should complete");
        let item = item_at(&snapshot, "Movie.mp4");

        if !semantics.case_insensitive {
            assert!(
                item.attachments.is_empty(),
                "case-sensitive volumes must keep case-distinct sidecars separate"
            );
            return;
        }
        assert_eq!(item.attachments.len(), 2);
        let preview = manager
            .preview(
                std::slice::from_ref(&item.id),
                "Moved",
                true,
                ConflictPolicy::KeepBoth,
            )
            .expect("case-folded sidecars should produce a move preview");
        let files = &preview.items[0].files;
        assert!(
            files
                .iter()
                .any(|file| file.target_path.ends_with(".en.srt"))
        );
        assert!(
            files
                .iter()
                .any(|file| file.target_path.ends_with(".zh.srt"))
        );

        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("move with language sidecars should complete");
        assert_eq!(result.status, BatchStatus::Complete);
        assert!(
            files
                .iter()
                .all(|file| root.path().join(&file.target_path).exists())
        );
    }

    #[test]
    fn long_attachment_qualifiers_fit_renamed_groups_and_keep_both_targets() {
        let root = TemporaryRoot::new("long-attachment-qualifier-budget");
        let clip_qualifier = "q".repeat(245);
        let episode_qualifier = "r".repeat(242);
        let unicode_title = "界😀".repeat(50);
        let ascii_title = "b".repeat(150);

        fs::write(root.path().join("clip.mp4"), b"clip media").expect("clip media should write");
        fs::write(
            root.path().join("clip.nfo"),
            format!("<movie><title>{unicode_title}</title></movie>\n"),
        )
        .expect("clip NFO should write");
        let clip_subtitle = format!("clip.{clip_qualifier}.srt");
        fs::write(root.path().join(&clip_subtitle), b"clip subtitle")
            .expect("long clip subtitle should write");

        fs::write(root.path().join("episode.mp4"), b"episode media")
            .expect("episode media should write");
        fs::write(
            root.path().join("episode.nfo"),
            format!("<movie><title>{ascii_title}</title></movie>\n"),
        )
        .expect("episode NFO should write");
        let episode_subtitle = format!("episode.{episode_qualifier}.srt");
        fs::write(root.path().join(&episode_subtitle), b"episode subtitle")
            .expect("long episode subtitle should write");

        fs::create_dir(root.path().join("Moved")).expect("target directory should create");
        fs::write(root.path().join("Moved/bbbbbbbb.mp4"), b"existing target")
            .expect("keep-both conflict target should write");

        let source_paths = [
            "clip.mp4",
            "clip.nfo",
            clip_subtitle.as_str(),
            "episode.mp4",
            "episode.nfo",
            episode_subtitle.as_str(),
        ];
        let original_files = source_paths
            .iter()
            .map(|relative| {
                let path = root.path().join(relative);
                let metadata = fs::metadata(&path).expect("source fixture should stat");
                (
                    (*relative).to_string(),
                    (
                        fs::read(&path).expect("source fixture should read"),
                        metadata.dev(),
                        metadata.ino(),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();

        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("source library should scan");
        let clip_id = item_at(&snapshot, "clip.mp4").id.clone();
        let episode_id = item_at(&snapshot, "episode.mp4").id.clone();
        let preview = manager
            .preview(
                &[clip_id.clone(), episode_id.clone()],
                "Moved",
                true,
                ConflictPolicy::KeepBoth,
            )
            .expect("long attachment groups should fit their destination components");

        let clip_preview = preview
            .items
            .iter()
            .find(|item| item.item_id == clip_id)
            .expect("clip should have a preview");
        let episode_preview = preview
            .items
            .iter()
            .find(|item| item.item_id == episode_id)
            .expect("episode should have a preview");
        for (preview_item, expected_targets) in [
            (
                clip_preview,
                [
                    "Moved/界.mp4".to_string(),
                    "Moved/界.nfo".to_string(),
                    format!("Moved/界.{clip_qualifier}.srt"),
                ],
            ),
            (
                episode_preview,
                [
                    "Moved/bbbb (2).mp4".to_string(),
                    "Moved/bbbb (2).nfo".to_string(),
                    format!("Moved/bbbb (2).{episode_qualifier}.srt"),
                ],
            ),
        ] {
            assert_eq!(preview_item.files.len(), expected_targets.len());
            let mut actual_targets = preview_item
                .files
                .iter()
                .map(|file| file.target_path.clone())
                .collect::<Vec<_>>();
            actual_targets.sort();
            let mut expected_targets = expected_targets.to_vec();
            expected_targets.sort();
            assert_eq!(actual_targets, expected_targets);
            assert!(preview_item.files.iter().all(|file| {
                Path::new(&file.target_path)
                    .file_name()
                    .and_then(OsStr::to_str)
                    .is_some_and(|name| name.len() <= MAX_FILENAME_COMPONENT_BYTES)
            }));
        }
        assert_eq!(
            Path::new(&format!("Moved/bbbb (2).{episode_qualifier}.srt"))
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .len(),
            MAX_FILENAME_COMPONENT_BYTES,
            "KeepBoth should reserve its suffix and use the full valid component budget"
        );

        let result = manager
            .execute_preview(&preview.id, &preview.revision)
            .expect("the complete attachment groups should move");
        assert_eq!(result.status, BatchStatus::Complete);
        for preview_item in &preview.items {
            for file in &preview_item.files {
                let (contents, device, inode) = &original_files[&file.source_path];
                let target = root.path().join(&file.target_path);
                let metadata = fs::metadata(&target).expect("moved group member should stat");
                assert_eq!(fs::read(&target).unwrap().as_slice(), contents.as_slice());
                assert_eq!(
                    metadata.dev(),
                    *device,
                    "rename should keep the source device"
                );
                assert_eq!(
                    metadata.ino(),
                    *inode,
                    "rename should keep the source object"
                );
                assert!(!root.path().join(&file.source_path).exists());
            }
        }
        assert_eq!(
            fs::read(root.path().join("Moved/bbbbbbbb.mp4")).unwrap(),
            b"existing target",
            "KeepBoth must leave the existing destination untouched"
        );

        let rescanned = manager
            .scan(&[])
            .expect("renamed media and long sidecars should remain scannable");
        for (media_path, expected_attachments) in [
            (
                "Moved/界.mp4",
                [
                    "Moved/界.nfo".to_string(),
                    format!("Moved/界.{clip_qualifier}.srt"),
                ],
            ),
            (
                "Moved/bbbb (2).mp4",
                [
                    "Moved/bbbb (2).nfo".to_string(),
                    format!("Moved/bbbb (2).{episode_qualifier}.srt"),
                ],
            ),
        ] {
            let item = item_at(&rescanned, media_path);
            assert_eq!(item.presence, Presence::Present);
            let actual_attachments = item
                .attachments
                .iter()
                .map(|attachment| attachment.relative_path.clone())
                .collect::<HashSet<_>>();
            assert_eq!(
                actual_attachments,
                expected_attachments.into_iter().collect::<HashSet<_>>()
            );
        }
    }

    #[test]
    fn group_filename_budget_rejects_suffixes_that_leave_no_room_for_a_stem() {
        let no_complete_stem = budgeted_group_stem("title", MAX_FILENAME_COMPONENT_BYTES, "", true)
            .expect_err("a zero-byte stem budget should reject the candidate");
        assert!(
            no_complete_stem
                .to_string()
                .contains("no complete media-stem character")
        );

        let no_keep_both_room =
            budgeted_group_stem("title", MAX_FILENAME_COMPONENT_BYTES - 3, " (2)", true)
                .expect_err("KeepBoth suffix must fit alongside at least one stem character");
        assert!(
            no_keep_both_room
                .to_string()
                .contains("filename component limit")
        );
    }

    #[test]
    fn round4_corrupt_index_is_quarantined_with_warning_and_rebuilt() {
        for (label, contents, warning_fragment) in [
            (
                "malformed",
                b"{\"version\":".as_slice(),
                "malformed or truncated JSON",
            ),
            (
                "unsupported",
                b"{\"version\":77,\"records\":{}}".as_slice(),
                "unsupported schema version 77",
            ),
        ] {
            let root = TemporaryRoot::new(&format!("round4-index-{label}"));
            let manager = manager(&root);
            let index_path = manager.library_dir.join(LIBRARY_INDEX);
            manager
                .write_private_file(&index_path, contents)
                .expect("corrupt index fixture should be written privately");

            let snapshot = manager
                .scan(&[])
                .expect("rebuildable index should not block a scan");

            let warning = snapshot
                .warnings
                .iter()
                .find(|warning| warning.contains(warning_fragment))
                .expect("scan should explain index recovery");
            assert!(warning.contains("preserved as .index.corrupt."));
            assert!(warning.contains(&format!("up to {INDEX_LIMIT} bytes")));
            let quarantined = fs::read_dir(&manager.library_dir)
                .expect("private library directory should list")
                .filter_map(Result::ok)
                .filter(|entry| parse_index_quarantine_name(&entry.file_name()).is_some())
                .collect::<Vec<_>>();
            assert_eq!(quarantined.len(), 1);
            assert_eq!(
                fs::read(quarantined[0].path())
                    .expect("quarantined diagnostic should remain")
                    .as_slice(),
                contents
            );
            let rebuilt = manager
                .read_index()
                .expect("replacement index should use the supported schema");
            assert_eq!(rebuilt.version, INDEX_VERSION);
            assert!(rebuilt.records.is_empty());
        }
    }

    #[test]
    fn round4_nonprivate_corrupt_index_remains_an_error_and_is_not_quarantined() {
        let root = TemporaryRoot::new("round4-index-access-policy");
        let manager = manager(&root);
        let index_path = manager.library_dir.join(LIBRARY_INDEX);
        let contents = b"{\"version\":";
        manager
            .write_private_file(&index_path, contents)
            .expect("corrupt index fixture should be written privately");
        fs::set_permissions(&index_path, fs::Permissions::from_mode(0o644))
            .expect("fixture should lose its private access policy");

        let error = manager
            .scan(&[])
            .expect_err("index access-policy failure must not look like a missing cache");

        assert!(format!("{error:#}").contains("owner-private"));
        assert_eq!(fs::read(&index_path).unwrap().as_slice(), contents);
        assert_eq!(
            fs::read_dir(&manager.library_dir)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| parse_index_quarantine_name(&entry.file_name()).is_some())
                .count(),
            0
        );
    }

    #[test]
    fn round4_index_quarantine_allows_private_directory_child_entry_churn() {
        let root = TemporaryRoot::new("round4-index-child-churn");
        let provider: Arc<dyn QueueFileProvider> = Arc::new(Round4IndexMutationProvider::new(
            Round4IndexMutation::ChildEntryChurn,
        ));
        let manager = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should open against the isolated fixture");
        let index_path = manager.library_dir.join(LIBRARY_INDEX);
        manager
            .write_private_file(&index_path, b"{\"version\":")
            .expect("corrupt index fixture should be written privately");

        let snapshot = manager
            .scan(&[])
            .expect("benign sibling creation must not block index recovery");

        assert!(snapshot.warnings.iter().any(|warning| {
            warning.contains("malformed or truncated JSON")
                && warning.contains("preserved as .index.corrupt.")
        }));
        assert!(manager.library_dir.join("unrelated-child-entry").exists());
    }

    #[test]
    fn round4_index_quarantine_rejects_same_size_content_change() {
        let root = TemporaryRoot::new("round4-index-content-race");
        let provider: Arc<dyn QueueFileProvider> = Arc::new(Round4IndexMutationProvider::new(
            Round4IndexMutation::SameSizeContentChange,
        ));
        let manager = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should open against the isolated fixture");
        let index_path = manager.library_dir.join(LIBRARY_INDEX);
        manager
            .write_private_file(&index_path, b"{\"version\":")
            .expect("corrupt index fixture should be written privately");

        let error = manager
            .scan(&[])
            .expect_err("same-size content mutation must reject quarantine");

        assert!(format!("{error:#}").contains("content changed"));
        assert_eq!(fs::read(&index_path).unwrap().as_slice(), b"[\"version\":");
        assert_eq!(
            fs::read_dir(&manager.library_dir)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| parse_index_quarantine_name(&entry.file_name()).is_some())
                .count(),
            0
        );
    }

    #[test]
    fn round4_index_quarantine_rejects_object_replacement() {
        let root = TemporaryRoot::new("round4-index-replacement-race");
        let provider: Arc<dyn QueueFileProvider> = Arc::new(Round4IndexMutationProvider::new(
            Round4IndexMutation::ObjectReplacement,
        ));
        let manager = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should open against the isolated fixture");
        let index_path = manager.library_dir.join(LIBRARY_INDEX);
        manager
            .write_private_file(&index_path, b"{\"version\":")
            .expect("corrupt index fixture should be written privately");

        let error = manager
            .scan(&[])
            .expect_err("a replacement object must reject quarantine");

        assert!(format!("{error:#}").contains("identity changed"));
        assert!(index_path.exists());
        assert_eq!(
            fs::read(&index_path).unwrap().as_slice(),
            b"{\"version\":1,\"records\":{}}"
        );
        assert_eq!(
            fs::read_to_string(manager.library_dir.join("index.replaced")).unwrap(),
            "{\"version\":"
        );
        assert_eq!(
            fs::read_dir(&manager.library_dir)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| parse_index_quarantine_name(&entry.file_name()).is_some())
                .count(),
            0
        );
    }

    #[test]
    fn round4_index_quarantine_rejects_access_policy_change() {
        let root = TemporaryRoot::new("round4-index-access-race");
        let provider: Arc<dyn QueueFileProvider> = Arc::new(Round4IndexMutationProvider::new(
            Round4IndexMutation::AccessPolicyChange,
        ));
        let manager = LibraryManager::open_with_provider(root.path(), provider)
            .expect("library manager should open against the isolated fixture");
        let index_path = manager.library_dir.join(LIBRARY_INDEX);
        manager
            .write_private_file(&index_path, b"{\"version\":")
            .expect("corrupt index fixture should be written privately");

        let error = manager
            .scan(&[])
            .expect_err("access-policy change must reject quarantine");

        assert!(format!("{error:#}").contains("access policy changed"));
        assert!(index_path.exists());
        assert_eq!(fs::read(&index_path).unwrap().as_slice(), b"{\"version\":");
        assert_eq!(fs::metadata(&index_path).unwrap().mode() & 0o777, 0o644);
        assert_eq!(
            fs::read_dir(&manager.library_dir)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| parse_index_quarantine_name(&entry.file_name()).is_some())
                .count(),
            0
        );
    }

    #[test]
    fn round4_index_quarantine_retention_is_bounded_and_name_strict() {
        let root = TemporaryRoot::new("round4-index-quarantine-bound");
        let manager = manager(&root);
        for (name, contents) in [
            (".index.corrupt.100.0.json", b"old-0".as_slice()),
            (".index.corrupt.101.1.json", b"old-1".as_slice()),
            (".index.corrupt.102.2.json", b"recent-2".as_slice()),
            (".index.corrupt.103.3.json", b"recent-3".as_slice()),
            (".index.corrupt.104.4.json", b"recent-4".as_slice()),
            (".index.corrupt.000.5.json", b"noncanonical".as_slice()),
            (".index.corrupt.105.5.json.backup", b"unrelated".as_slice()),
        ] {
            manager
                .write_private_file(&manager.library_dir.join(name), contents)
                .expect("quarantine fixture should be written privately");
        }

        manager
            .prune_index_quarantines()
            .expect("old generated quarantines should be safely pruned");

        let mut retained = fs::read_dir(&manager.library_dir)
            .expect("private library directory should list")
            .filter_map(Result::ok)
            .filter_map(|entry| {
                parse_index_quarantine_name(&entry.file_name()).map(|key| (key, entry.file_name()))
            })
            .collect::<Vec<_>>();
        retained.sort_by_key(|(key, _)| *key);
        assert_eq!(retained.len(), MAX_INDEX_QUARANTINES);
        assert_eq!(
            retained
                .iter()
                .map(|(_, name)| name.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            vec![
                ".index.corrupt.102.2.json".to_string(),
                ".index.corrupt.103.3.json".to_string(),
                ".index.corrupt.104.4.json".to_string(),
            ]
        );
        assert_eq!(
            fs::read(manager.library_dir.join(".index.corrupt.000.5.json"))
                .expect("noncanonical name should not be pruned"),
            b"noncanonical".to_vec()
        );
        assert_eq!(
            fs::read(manager.library_dir.join(".index.corrupt.105.5.json.backup"))
                .expect("backup name should not be pruned"),
            b"unrelated".to_vec()
        );
    }

    #[test]
    fn standard_stem_uses_only_real_order_and_keeps_specs_and_source_id() {
        let item = LibraryItem {
            id: "copy-test".to_string(),
            title: "Episode: One".to_string(),
            relative_path: "episode.mp4".to_string(),
            bytes: 1,
            width: None,
            height: Some(1080),
            codec: Some("h264".to_string()),
            source_ids: vec!["BV1abcdefgh".to_string()],
            collection: Some(CollectionMetadata {
                id: None,
                title: "Series".to_string(),
                kind: "multi_part".to_string(),
                order: None,
                part_id: Some("P03".to_string()),
            }),
            organization_status: None,
            attachments: Vec::new(),
            authoritative_nfo_path: None,
            confidence: Confidence::High,
            metadata_status: MetadataStatus::Complete,
            integrity_status: IntegrityStatus::Unverified,
            physical_identity: None,
            presence: Presence::Present,
        };
        let stem = standard_media_stem(&item);
        assert_eq!(
            stem,
            "Series - P03 - Episode_ One - [BV1abcdefgh] - [1080p h264]"
        );
        assert_eq!(part_ordinal("P3"), Some(3));
        assert_eq!(part_ordinal("Part-3"), None);

        let independent = LibraryItem {
            collection: None,
            ..item
        };
        assert!(!standard_media_stem(&independent).contains("P03"));

        let youtube = LibraryItem {
            source_ids: vec!["youtube:abcdefghijk".to_string()],
            ..independent
        };
        assert!(standard_media_stem(&youtube).contains("[youtube_abcdefghijk]"));
    }

    #[test]
    fn operation_state_file_parser_matches_generated_preview_and_batch_paths() {
        let root = TemporaryRoot::new("operation-state-file-name-parser");
        fs::write(root.path().join("clip.mp4"), b"media fixture").expect("media should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("initial scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let preview = cleanup_preview(&manager, &item_id);

        let preview_path = manager.preview_path(&preview.id);
        let preview_name = preview_path
            .file_name()
            .expect("generated preview path should have a filename");
        assert_eq!(
            parse_operation_state_file_name(preview_name),
            Some((OperationStateFileKind::Preview, preview.id.clone()))
        );

        let batch_path = manager.batch_path(&preview.id);
        let batch_name = batch_path
            .file_name()
            .expect("generated batch path should have a filename");
        assert_eq!(
            parse_operation_state_file_name(batch_name),
            Some((OperationStateFileKind::Batch, preview.id.clone()))
        );

        let hash = preview
            .id
            .strip_prefix(PREVIEW_PREFIX)
            .expect("generated preview ID should carry its prefix");
        for malformed in [
            format!("{PREVIEW_PREFIX}{hash}.json"),
            format!("batch-{hash}.json"),
            format!("{PREVIEW_PREFIX}{PREVIEW_PREFIX}{}.json", preview.id),
            "batch-preview-not-a-24-character-hex-id.json".to_string(),
        ] {
            assert_eq!(
                parse_operation_state_file_name(OsStr::new(&malformed)),
                None,
                "malformed generated state filename should be ignored: {malformed}"
            );
        }
        release_preview_anchors(&preview.id);
    }

    #[test]
    fn operation_state_cleanup_prunes_old_terminal_state_but_preserves_recovery() {
        let root = TemporaryRoot::new("operation-state-retention");
        let media_path = root.path().join("clip.mp4");
        let partial_download_path = root.path().join("clip.mp4.part");
        fs::write(&media_path, b"permanent media fixture").expect("media should write");
        fs::write(&partial_download_path, b"user download fragment")
            .expect("download fragment should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("initial scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();

        let abandoned = cleanup_preview(&manager, &item_id);
        let fresh = cleanup_preview(&manager, &item_id);
        let interrupted = cleanup_preview(&manager, &item_id);
        let retryable = cleanup_preview(&manager, &item_id);
        let finalized_pending = cleanup_preview(&manager, &item_id);
        let terminal = cleanup_preview(&manager, &item_id);

        let interrupted_path = manager.batch_path(&interrupted.id);
        let interrupted_manifest = cleanup_batch(&interrupted, false, false, true);
        let mut legacy = serde_json::to_value(&interrupted_manifest)
            .expect("legacy batch fixture should encode");
        legacy
            .as_object_mut()
            .expect("batch fixture should be an object")
            .remove("attempt_finished");
        let interrupted_manifest: BatchManifest =
            serde_json::from_value(legacy).expect("old batch schema should remain readable");
        assert!(!interrupted_manifest.attempt_finished);
        manager
            .write_batch_manifest(&interrupted_path, &interrupted_manifest)
            .expect("interrupted batch should persist");

        let retryable_path = manager.batch_path(&retryable.id);
        manager
            .write_batch_manifest(
                &retryable_path,
                &cleanup_batch(&retryable, true, true, true),
            )
            .expect("retryable batch should persist");

        let finalized_pending_path = manager.batch_path(&finalized_pending.id);
        manager
            .write_batch_manifest(
                &finalized_pending_path,
                &cleanup_batch(&finalized_pending, true, false, true),
            )
            .expect("finalized permanent-failure batch should persist");

        let terminal_path = manager.batch_path(&terminal.id);
        manager
            .write_batch_manifest(
                &terminal_path,
                &cleanup_batch(&terminal, true, false, false),
            )
            .expect("terminal batch should persist");

        let old_age = Duration::from_secs(9 * 24 * 60 * 60);
        for preview in [
            &abandoned,
            &interrupted,
            &retryable,
            &finalized_pending,
            &terminal,
        ] {
            set_operation_state_age(&manager.preview_path(&preview.id), old_age);
        }
        for path in [
            &interrupted_path,
            &retryable_path,
            &finalized_pending_path,
            &terminal_path,
        ] {
            set_operation_state_age(path, old_age);
        }

        let registry = Mutex::new(OperationStateCleanupCursorRegistry::default());
        for _ in 0..4 {
            let warnings = run_operation_state_cleanup(&manager, &registry);
            assert!(warnings.is_empty(), "cleanup warnings: {warnings:?}");
        }

        assert!(!manager.preview_path(&abandoned.id).exists());
        assert!(manager.preview_path(&fresh.id).exists());
        assert!(manager.preview_path(&interrupted.id).exists());
        assert!(interrupted_path.exists());
        assert!(manager.preview_path(&retryable.id).exists());
        assert!(retryable_path.exists());
        assert!(!manager.preview_path(&finalized_pending.id).exists());
        assert!(
            !finalized_pending_path.exists(),
            "a finished permanent failure with pending work should expire"
        );
        assert!(!manager.preview_path(&terminal.id).exists());
        assert!(!terminal_path.exists());
        assert_eq!(fs::read(&media_path).unwrap(), b"permanent media fixture");
        assert_eq!(
            fs::read(&partial_download_path).unwrap(),
            b"user download fragment",
            "operation-state cleanup must not remove download fragments"
        );

        for preview in [
            &abandoned,
            &fresh,
            &interrupted,
            &retryable,
            &finalized_pending,
            &terminal,
        ] {
            release_preview_anchors(&preview.id);
        }
    }

    #[test]
    fn operation_state_cleanup_rechecks_age_and_associated_batch_before_removal() {
        let root = TemporaryRoot::new("operation-state-recheck");
        fs::write(root.path().join("clip.mp4"), b"media fixture").expect("media should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("initial scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let old_age = Duration::from_secs(9 * 24 * 60 * 60);

        let refreshed = cleanup_preview(&manager, &item_id);
        let refreshed_path = manager.preview_path(&refreshed.id);
        set_operation_state_age(&refreshed_path, old_age);
        let refreshed_candidate = cleanup_preview_candidate(&manager, &refreshed);
        assert!(operation_state_is_expired(
            refreshed_candidate.modified_seconds,
            SystemTime::now(),
            ABANDONED_PREVIEW_TTL
        ));
        let refreshed_removed = manager
            .remove_operation_state_candidate_with_hook(
                &refreshed_candidate,
                PREVIEW_LIMIT,
                OperationStateFileKind::Preview,
                &refreshed.id,
                ABANDONED_PREVIEW_TTL,
                || set_operation_state_age(&refreshed_path, Duration::from_secs(1)),
            )
            .expect("a refreshed preview should be retained safely");
        assert!(!refreshed_removed);
        assert!(refreshed_path.exists());

        let batch_appeared = cleanup_preview(&manager, &item_id);
        let batch_appeared_path = manager.preview_path(&batch_appeared.id);
        set_operation_state_age(&batch_appeared_path, old_age);
        let batch_candidate = cleanup_preview_candidate(&manager, &batch_appeared);
        let batch_path = manager.batch_path(&batch_appeared.id);
        let batch_removed = manager
            .remove_operation_state_candidate_with_hook(
                &batch_candidate,
                PREVIEW_LIMIT,
                OperationStateFileKind::Preview,
                &batch_appeared.id,
                ABANDONED_PREVIEW_TTL,
                || {
                    manager
                        .write_batch_manifest(
                            &batch_path,
                            &cleanup_batch(&batch_appeared, false, false, true),
                        )
                        .expect("concurrent recovery state should persist");
                },
            )
            .expect("a preview with a newly appeared batch should be retained safely");
        assert!(!batch_removed);
        assert!(batch_appeared_path.exists());
        assert!(batch_path.exists());

        release_preview_anchors(&refreshed.id);
        release_preview_anchors(&batch_appeared.id);
    }

    #[test]
    fn operation_state_cleanup_cursor_progress_survives_manager_reopen() {
        const PREVIEW_COUNT: usize = 80;
        let root = TemporaryRoot::new("operation-state-cursor-reopen");
        fs::write(root.path().join("clip.mp4"), b"media fixture").expect("media should write");
        let first_manager = manager(&root);
        let snapshot = first_manager
            .scan(&[])
            .expect("initial scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        let base_preview = cleanup_preview(&first_manager, &item_id);
        let base_persisted: PersistedPreview = serde_json::from_slice(
            &first_manager
                .read_private_file(&first_manager.preview_path(&base_preview.id), PREVIEW_LIMIT)
                .unwrap()
                .unwrap(),
        )
        .expect("base preview should parse");

        let mut preview_paths = vec![first_manager.preview_path(&base_preview.id)];
        for index in 0..PREVIEW_COUNT {
            let id = format!("preview-{index:024x}");
            let mut persisted = base_persisted.clone();
            persisted.preview.id = id.clone();
            let path = first_manager.preview_path(&id);
            first_manager
                .write_private_file(
                    &path,
                    &serde_json::to_vec(&persisted).expect("preview fixture should encode"),
                )
                .expect("preview fixture should persist");
            preview_paths.push(path);
        }

        let directory_names = fs::read_dir(&first_manager.library_dir)
            .expect("private state directory should enumerate")
            .map(|entry| {
                entry
                    .expect("state directory entry should read")
                    .file_name()
                    .into_string()
                    .expect("state directory names should be UTF-8")
            })
            .collect::<Vec<_>>();
        let first_page_names = directory_names
            .iter()
            .take(MAX_OPERATION_STATE_DIRECTORY_NAMES_PER_PASS)
            .cloned()
            .collect::<HashSet<_>>();
        let old_age = Duration::from_secs(9 * 24 * 60 * 60);
        let mut protected_paths = Vec::new();
        let mut later_expired_count = 0usize;
        for path in &preview_paths {
            let name = path
                .file_name()
                .and_then(OsStr::to_str)
                .expect("preview name should be UTF-8");
            if first_page_names.contains(name) {
                set_operation_state_age(path, Duration::from_secs(1));
                protected_paths.push(path.clone());
            } else {
                set_operation_state_age(path, old_age);
                later_expired_count += 1;
            }
        }
        assert!(
            protected_paths.len() >= 20,
            "first directory page should contain preview states"
        );
        assert!(
            later_expired_count > 32,
            "expired previews should remain beyond the first page"
        );

        let registry = Mutex::new(OperationStateCleanupCursorRegistry::default());
        let first_warnings = run_operation_state_cleanup(&first_manager, &registry);
        assert!(
            first_warnings.is_empty(),
            "cleanup warnings: {first_warnings:?}"
        );
        assert!(protected_paths.iter().all(|path| path.exists()));
        let remaining_after_first = preview_paths.iter().filter(|path| path.exists()).count();
        assert!(remaining_after_first > 0);

        let key_before_reopen = OperationStateCleanupCursorKey {
            canonical_root: first_manager.root.root_path().to_path_buf(),
            library_device: first_manager.library_dir_identity.device(),
            library_inode: first_manager.library_dir_identity.inode(),
        };
        let cursor_before_reopen = registry
            .lock()
            .unwrap()
            .cursor_for(key_before_reopen.clone());
        assert!(cursor_before_reopen.lock().unwrap().is_some());
        drop(first_manager);

        let reopened = manager(&root);
        let key_after_reopen = OperationStateCleanupCursorKey {
            canonical_root: reopened.root.root_path().to_path_buf(),
            library_device: reopened.library_dir_identity.device(),
            library_inode: reopened.library_dir_identity.inode(),
        };
        let cursor_after_reopen = registry.lock().unwrap().cursor_for(key_after_reopen);
        assert!(Arc::ptr_eq(&cursor_before_reopen, &cursor_after_reopen));

        let second_warnings = run_operation_state_cleanup(&reopened, &registry);
        assert!(
            second_warnings.is_empty(),
            "cleanup warnings: {second_warnings:?}"
        );
        let remaining_after_second = preview_paths.iter().filter(|path| path.exists()).count();
        assert!(
            remaining_after_second < remaining_after_first,
            "a reopened manager should continue beyond protected entries"
        );
        assert!(protected_paths.iter().all(|path| path.exists()));

        release_preview_anchors(&base_preview.id);
    }

    #[test]
    fn operation_state_cleanup_cursor_registry_is_bounded_lru() {
        let mut registry = OperationStateCleanupCursorRegistry::default();
        let make_key = |index: usize| OperationStateCleanupCursorKey {
            canonical_root: PathBuf::from(format!("/isolated/cleanup-root-{index}")),
            library_device: index as u64,
            library_inode: (index + 100) as u64,
        };
        let mut original_cursors = Vec::new();
        for index in 0..MAX_OPERATION_STATE_CLEANUP_ROOTS {
            original_cursors.push(registry.cursor_for(make_key(index)));
        }
        let first = registry.cursor_for(make_key(0));
        assert!(Arc::ptr_eq(&first, &original_cursors[0]));
        registry.cursor_for(make_key(MAX_OPERATION_STATE_CLEANUP_ROOTS));
        assert_eq!(registry.entries.len(), MAX_OPERATION_STATE_CLEANUP_ROOTS);

        let retained_first = registry.cursor_for(make_key(0));
        assert!(Arc::ptr_eq(&retained_first, &original_cursors[0]));
        let reinserted_second = registry.cursor_for(make_key(1));
        assert!(!Arc::ptr_eq(&reinserted_second, &original_cursors[1]));
    }

    #[test]
    fn preview_anchor_store_touches_lru_and_drops_expired_handles() {
        let root = TemporaryRoot::new("preview-anchor-expiration");
        let media_path = root.path().join("clip.mp4");
        fs::write(&media_path, b"preview anchor fixture").expect("media fixture should write");
        let manager = manager(&root);
        let read = manager
            .read_file_with_token_and_handle(&media_path, None)
            .expect("media fixture should bind")
            .expect("media fixture should exist");
        let now = Instant::now();
        let mut anchors = HashMap::new();
        anchors.insert(read.token.clone(), read.file);
        let mut store = PreviewAnchorStore {
            file_count: anchors.len(),
            ..PreviewAnchorStore::default()
        };
        store
            .order
            .push_back("preview-anchor-expiration".to_string());
        store.by_preview.insert(
            "preview-anchor-expiration".to_string(),
            PreviewAnchorEntry {
                revision: "revision-expired".to_string(),
                anchors,
                expires_at: now - Duration::from_secs(1),
            },
        );
        store.order.push_back("preview-anchor-recent".to_string());
        store.by_preview.insert(
            "preview-anchor-recent".to_string(),
            PreviewAnchorEntry {
                revision: "revision-recent".to_string(),
                anchors: HashMap::new(),
                expires_at: now + Duration::from_secs(10),
            },
        );

        assert!(
            store
                .touch("preview-anchor-expiration", now)
                .is_some_and(|entry| entry.contains_key(&read.token))
        );
        assert_eq!(
            store.order.back().map(String::as_str),
            Some("preview-anchor-expiration")
        );
        assert_eq!(
            store.by_preview["preview-anchor-expiration"].expires_at,
            now + PREVIEW_ANCHOR_TTL
        );

        store.prune_expired(now + PREVIEW_ANCHOR_TTL + Duration::from_secs(1));
        assert!(store.by_preview.is_empty());
        assert!(store.order.is_empty());
        assert_eq!(store.file_count, 0);
    }

    #[test]
    fn active_preview_confirmability_requires_creation_time_or_live_anchor() {
        let root = TemporaryRoot::new("preview-confirmability-missing-created");
        fs::write(root.path().join("clip.mp4"), b"confirmation fixture")
            .expect("media fixture should write");
        let library = manager(&root);
        let snapshot = library.scan(&[]).expect("initial scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        library.make_preview_creation_time_unavailable_for_test();
        let preview = library
            .preview(&[item_id], "Moved", true, ConflictPolicy::KeepBoth)
            .expect("preview with unavailable creation metadata should persist");
        let key = (preview.id.clone(), preview.revision.clone());
        let persisted = library
            .read_persisted_preview(&preview.id)
            .expect("persisted preview should read");
        let required = required_preview_anchor_tokens(&persisted)
            .expect("preview identity tokens should be valid");
        assert_eq!(required.len(), 1);
        assert!(required.iter().all(|token| {
            decode_identity_token(token)
                .expect("fixture identity should decode")
                .birth_seconds
                .is_none()
        }));

        let expires_at_before = {
            let cache = LIVE_PREVIEW_ANCHORS
                .get()
                .expect("preview should store live anchors")
                .lock()
                .expect("anchor cache should remain healthy");
            cache.by_preview[&preview.id].expires_at
        };
        assert!(
            library
                .unconfirmable_preview_ids(std::slice::from_ref(&key))
                .expect("live preview anchors should be inspectable")
                .is_empty(),
            "an active descriptor anchor should keep this preview confirmable"
        );
        let expires_at_after = {
            let cache = LIVE_PREVIEW_ANCHORS
                .get()
                .expect("anchor cache should remain initialized")
                .lock()
                .expect("anchor cache should remain healthy");
            cache.by_preview[&preview.id].expires_at
        };
        assert_eq!(
            expires_at_before, expires_at_after,
            "publishing state must not refresh the one-hour anchor TTL"
        );

        {
            let mut cache = LIVE_PREVIEW_ANCHORS
                .get()
                .expect("preview anchor cache should remain initialized")
                .lock()
                .expect("anchor cache should remain healthy");
            cache.by_preview.get_mut(&preview.id).unwrap().expires_at =
                Instant::now() - Duration::from_secs(1);
        }
        assert!(
            library
                .unconfirmable_preview_ids(std::slice::from_ref(&key))
                .expect("expired anchor should be a normal stale-preview state")
                .contains(&preview.id),
            "the state check should enforce anchor expiry without waiting in real time"
        );

        let mut reopened = manager(&root);
        reopened.process_nonce = "simulated-new-process".to_string();
        assert!(
            reopened
                .unconfirmable_preview_ids(std::slice::from_ref(&key))
                .expect("restart should be distinguishable from a read failure")
                .contains(&preview.id),
            "a token without persistent creation time cannot survive a process restart"
        );

        release_preview_anchors(&preview.id);
        assert!(
            library
                .unconfirmable_preview_ids(std::slice::from_ref(&key))
                .expect("an evicted anchor should be reported as unavailable")
                .contains(&preview.id),
            "a persisted preview remains present but is no longer confirmable without its anchor"
        );

        let mut persistent = persisted;
        persistent.process_nonce = "simulated-new-process".to_string();
        for file in persistent
            .preview
            .items
            .iter_mut()
            .flat_map(|item| &mut item.files)
        {
            if !file.identity.is_empty() {
                file.identity = identity_token_with_creation_time_for_test(&file.identity, 1, 0)
                    .expect("test token should encode persistent creation time");
            }
        }
        assert!(
            preview_has_required_anchors(&persistent, "simulated-new-process", None)
                .expect("persistent identity tokens should be valid"),
            "a preview carrying persistent creation-time evidence should survive reopen without descriptor anchors"
        );

        let wrong_revision = (preview.id.clone(), "stale-revision".to_string());
        assert!(
            library
                .unconfirmable_preview_ids(&[wrong_revision])
                .expect("revision mismatch should be a normal stale-preview state")
                .contains(&preview.id)
        );
        let missing_id = new_operation_id("missing-confirmability-preview");
        assert!(
            library
                .unconfirmable_preview_ids(&[(missing_id.clone(), "revision".to_string())])
                .expect("missing preview should be a normal stale-preview state")
                .contains(&missing_id)
        );
    }

    #[test]
    fn preview_confirmability_preserves_transient_file_provider_errors() {
        let root = TemporaryRoot::new("preview-confirmability-provider-error");
        let media_path = root.path().join("clip.mp4");
        fs::write(&media_path, b"provider error fixture").expect("media fixture should write");
        let provider = Arc::new(FailOneMediaReadProvider::new(media_path.clone()));
        let provider_for_manager: Arc<dyn QueueFileProvider> = provider.clone();
        let library = LibraryManager::open_with_provider(root.path(), provider_for_manager)
            .expect("library manager should open against the injected provider");
        let snapshot = library.scan(&[]).expect("initial scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        library.make_preview_creation_time_unavailable_for_test();
        let preview = library
            .preview(&[item_id], "Moved", true, ConflictPolicy::KeepBoth)
            .expect("preview with unavailable creation metadata should persist");
        provider.fail_next_media_read_at(&library.preview_path(&preview.id));

        let error = library
            .unconfirmable_preview_ids(&[(preview.id.clone(), preview.revision.clone())])
            .expect_err("a transient private-preview read failure must remain an error");
        assert!(
            is_file_provider_access_error(&error),
            "transient provider failure must not be misreported as an unavailable anchor: {error:#}"
        );
        release_preview_anchors(&preview.id);
    }

    #[test]
    fn missing_creation_time_confirmability_expires_and_obeys_lru_eviction() {
        let root = TemporaryRoot::new("preview-confirmability-anchor-retention");
        let media_path = root.path().join("clip.mp4");
        fs::write(&media_path, b"anchor retention fixture").expect("media fixture should write");
        let manager = manager(&root);
        let snapshot = manager.scan(&[]).expect("initial scan should complete");
        let item_id = item_at(&snapshot, "clip.mp4").id.clone();
        manager.make_preview_creation_time_unavailable_for_test();
        let preview = manager
            .preview(&[item_id], "Moved", true, ConflictPolicy::KeepBoth)
            .expect("preview with unavailable creation metadata should persist");
        let persisted = manager
            .read_persisted_preview(&preview.id)
            .expect("persisted preview should read");
        let read = manager
            .read_file_with_token_and_handle(&media_path, None)
            .expect("same media object should remain readable")
            .expect("media fixture should remain present");
        let now = Instant::now();
        let mut anchors = HashMap::new();
        anchors.insert(read.token.clone(), read.file.clone());
        let mut cache = PreviewAnchorStore {
            file_count: anchors.len(),
            ..PreviewAnchorStore::default()
        };
        cache.order.push_back(preview.id.clone());
        cache.by_preview.insert(
            preview.id.clone(),
            PreviewAnchorEntry {
                revision: persisted.preview.revision.clone(),
                anchors,
                expires_at: now + PREVIEW_ANCHOR_TTL,
            },
        );
        assert!(
            preview_has_required_anchors(&persisted, &manager.process_nonce, Some(&cache))
                .expect("live anchor should match the persisted object identity")
        );

        cache.prune_expired(now + PREVIEW_ANCHOR_TTL);
        assert!(
            !preview_has_required_anchors(&persisted, &manager.process_nonce, Some(&cache))
                .expect("expired anchor should be a normal unavailable state")
        );

        let mut cache = PreviewAnchorStore::default();
        let mut anchors = HashMap::new();
        anchors.insert(read.token.clone(), read.file.clone());
        cache.file_count = anchors.len();
        cache.order.push_back(preview.id.clone());
        cache.by_preview.insert(
            preview.id.clone(),
            PreviewAnchorEntry {
                revision: persisted.preview.revision.clone(),
                anchors,
                expires_at: now + PREVIEW_ANCHOR_TTL,
            },
        );
        for index in 0..MAX_PINNED_PREVIEWS {
            let preview_id = format!("preview-confirmability-lru-{index}");
            cache
                .make_room_for(&preview_id, 0)
                .expect("LRU store should admit bounded empty preview entries");
            cache.order.push_back(preview_id.clone());
            cache.by_preview.insert(
                preview_id,
                PreviewAnchorEntry {
                    revision: "revision-lru".to_string(),
                    anchors: HashMap::new(),
                    expires_at: now + PREVIEW_ANCHOR_TTL,
                },
            );
        }
        assert!(!cache.by_preview.contains_key(&preview.id));
        assert!(
            !preview_has_required_anchors(&persisted, &manager.process_nonce, Some(&cache))
                .expect("LRU eviction should be a normal unavailable state")
        );
        release_preview_anchors(&preview.id);
    }

    #[test]
    fn preview_anchor_terminal_release_drops_global_entry() {
        let root = TemporaryRoot::new("preview-anchor-terminal-release");
        let media_path = root.path().join("clip.mp4");
        fs::write(&media_path, b"terminal release fixture").expect("media fixture should write");
        let manager = manager(&root);
        let read = manager
            .read_file_with_token_and_handle(&media_path, None)
            .expect("media fixture should bind")
            .expect("media fixture should exist");
        let preview_id = new_operation_id("test-preview-anchor-release");
        let mut anchors = HashMap::new();
        anchors.insert(read.token.clone(), read.file);
        store_preview_anchors(&preview_id, "revision-test", anchors)
            .expect("live preview anchors should be stored");
        {
            let cache = LIVE_PREVIEW_ANCHORS
                .get()
                .expect("anchor cache should initialize")
                .lock()
                .expect("anchor cache should remain healthy");
            assert!(cache.by_preview.contains_key(&preview_id));
            assert_eq!(
                cache.by_preview[&preview_id].anchors.len(),
                1,
                "terminal release fixture should retain one descriptor"
            );
        }

        release_preview_anchors(&preview_id);

        let cache = LIVE_PREVIEW_ANCHORS
            .get()
            .expect("anchor cache should remain initialized")
            .lock()
            .expect("anchor cache should remain healthy");
        assert!(!cache.by_preview.contains_key(&preview_id));
    }

    #[test]
    fn preview_anchor_revision_release_preserves_reused_id_for_new_revision() {
        let root = TemporaryRoot::new("preview-anchor-reused-id-revision-release");
        let media_path = root.path().join("clip.mp4");
        fs::write(&media_path, b"preview anchor reused-id fixture")
            .expect("media fixture should write");
        let manager = manager(&root);
        manager.make_preview_creation_time_unavailable_for_test();
        let snapshot = manager
            .scan(&[])
            .expect("library scan should find the preview fixture");
        let item_id = snapshot
            .items
            .iter()
            .find(|item| item.relative_path == "clip.mp4")
            .expect("preview fixture should appear in the library")
            .id
            .clone();
        let preview = manager
            .preview(
                std::slice::from_ref(&item_id),
                "organized",
                false,
                ConflictPolicy::KeepBoth,
            )
            .expect("fixture preview should persist with live descriptor anchors");
        let old_revision = preview.revision.clone();
        let new_revision = format!("{old_revision}-reused");
        let initially_confirmable = manager
            .unconfirmable_preview_ids(&[(preview.id.clone(), old_revision.clone())])
            .expect("current preview should be confirmable while its anchor is live");
        assert!(!initially_confirmable.contains(&preview.id));

        let newer_anchors = {
            let cache = LIVE_PREVIEW_ANCHORS
                .get()
                .expect("preview creation should initialize the anchor cache")
                .lock()
                .expect("anchor cache should remain healthy");
            cache
                .by_preview
                .get(&preview.id)
                .expect("preview should retain its registered anchors")
                .anchors
                .clone()
        };
        store_preview_anchors(&preview.id, &new_revision, newer_anchors)
            .expect("a reused ID should replace its anchor ownership revision");
        let mut persisted = manager
            .read_persisted_preview(&preview.id)
            .expect("persisted preview should remain readable");
        persisted.preview.revision = new_revision.clone();
        let bytes = serde_json::to_vec(&persisted).expect("replacement preview should encode");
        manager
            .write_private_file(&manager.preview_path(&preview.id), &bytes)
            .expect("reused preview ID should persist the new revision");

        let newer_preview = (preview.id.clone(), new_revision.clone());
        assert!(
            manager
                .unconfirmable_preview_ids(std::slice::from_ref(&newer_preview))
                .expect("new revision should match its replacement anchor entry")
                .is_empty(),
            "newer revision should be confirmable before stale cancellation"
        );
        assert!(
            !release_preview_anchors_for_revision(&preview.id, &old_revision),
            "stale cancellation must not release anchors now owned by the newer revision"
        );
        assert!(
            manager
                .unconfirmable_preview_ids(std::slice::from_ref(&newer_preview))
                .expect("newer revision should remain checkable after stale release")
                .is_empty(),
            "newer preview should remain confirmable after cancellation of the old revision"
        );

        assert!(
            release_preview_anchors_for_revision(&preview.id, &new_revision),
            "cancellation of the exact current revision should release its anchor entry"
        );
        assert!(
            manager
                .unconfirmable_preview_ids(std::slice::from_ref(&newer_preview))
                .expect("released current revision should remain a normal unavailable state")
                .contains(&preview.id),
            "the exact current revision should no longer be confirmable after release"
        );
    }

    #[test]
    fn preview_anchor_lru_capacity_evicts_oldest_and_keeps_all_limits() {
        let now = Instant::now();
        let mut store = PreviewAnchorStore::default();
        for index in 0..MAX_PINNED_PREVIEWS {
            let preview_id = format!("preview-capacity-{index}");
            store.order.push_back(preview_id.clone());
            store.by_preview.insert(
                preview_id,
                PreviewAnchorEntry {
                    revision: "revision-capacity".to_string(),
                    anchors: HashMap::new(),
                    expires_at: now + PREVIEW_ANCHOR_TTL,
                },
            );
        }
        store
            .make_room_for("preview-capacity-incoming", 1)
            .expect("oldest preview should be evicted to stay within the preview cap");

        assert!(!store.by_preview.contains_key("preview-capacity-0"));
        assert_eq!(store.by_preview.len(), MAX_PINNED_PREVIEWS - 1);
        assert_eq!(store.file_count, 0);
        assert!(store.order.contains(&"preview-capacity-1".to_string()));
    }

    #[test]
    fn target_component_keys_respect_case_and_normalization_policy() {
        fn full_fold_for_test(value: &str) -> Result<String> {
            let mut folded = String::new();
            for character in value.chars() {
                match character {
                    'ß' | 'ẞ' => folded.push_str("ss"),
                    'ς' => folded.push('σ'),
                    _ => folded.extend(character.to_lowercase()),
                }
            }
            Ok(folded)
        }

        let sensitive_normalized = TargetNameSemantics {
            normalization_insensitive: true,
            case_insensitive: false,
            hfs_ignorable: false,
        };
        let insensitive = TargetNameSemantics {
            normalization_insensitive: true,
            case_insensitive: true,
            hfs_ignorable: false,
        };
        let hfs_insensitive = TargetNameSemantics {
            hfs_ignorable: true,
            ..insensitive
        };
        let identity_fold = |value: &str| Ok(value.to_owned());

        assert_ne!(
            target_component_key_with("Episode", sensitive_normalized, identity_fold)
                .expect("sensitive name should normalize"),
            target_component_key_with("episode", sensitive_normalized, identity_fold)
                .expect("sensitive name should normalize")
        );
        assert_eq!(
            target_component_key_with("épisode", sensitive_normalized, identity_fold)
                .expect("composed name should normalize"),
            target_component_key_with("e\u{301}pisode", sensitive_normalized, identity_fold)
                .expect("decomposed name should normalize")
        );
        assert_eq!(
            target_component_key_with("Maße", insensitive, full_fold_for_test)
                .expect("case-insensitive name should fold"),
            target_component_key_with("MASSE", insensitive, full_fold_for_test)
                .expect("case-insensitive name should fold")
        );
        assert_eq!(
            target_component_key_with("Σ", insensitive, full_fold_for_test)
                .expect("capital sigma should fold"),
            target_component_key_with("ς", insensitive, full_fold_for_test)
                .expect("final sigma should fold")
        );
        assert_eq!(
            target_component_key_with("episode\u{200d}", hfs_insensitive, full_fold_for_test)
                .expect("HFS ignorable should be removed conservatively"),
            target_component_key_with("EPISODE", hfs_insensitive, full_fold_for_test)
                .expect("HFS name should fold")
        );
        assert_ne!(
            target_component_key_with("episode\u{200d}", insensitive, full_fold_for_test)
                .expect("APFS should retain HFS-only ignorable behavior"),
            target_component_key_with("EPISODE", insensitive, full_fold_for_test)
                .expect("APFS should retain HFS-only ignorable behavior")
        );
        let reserved_key = TargetReservationKey {
            directory_device: 7,
            directory_inode: 9,
            components: vec![
                target_component_key_with("Maße.mp4", insensitive, full_fold_for_test)
                    .expect("reserved case-insensitive name should fold"),
            ],
        };
        let alias_key = TargetReservationKey {
            directory_device: 7,
            directory_inode: 9,
            components: vec![
                target_component_key_with("MASSE.MP4", insensitive, full_fold_for_test)
                    .expect("case alias should fold"),
            ],
        };
        assert!(HashSet::from([reserved_key]).contains(&alias_key));
    }

    #[test]
    fn linux_target_name_semantics_support_known_case_sensitive_filesystems_only() {
        let exact = TargetNameSemantics {
            normalization_insensitive: false,
            case_insensitive: false,
            hfs_ignorable: false,
        };

        assert_eq!(
            linux_target_name_semantics(0xef53, 0).expect("ext4 should be supported"),
            exact
        );
        assert_eq!(
            linux_target_name_semantics(0x0102_1994, 0).expect("tmpfs should be supported"),
            exact
        );
        assert!(
            linux_target_name_semantics(0xef53, 0x4000_0000)
                .expect_err("case-folded ext4 directories must fail closed")
                .to_string()
                .contains("case-folded")
        );
        assert!(
            linux_target_name_semantics(0x1234, 0)
                .expect_err("unknown filesystems must fail closed")
                .to_string()
                .contains("unsupported")
        );
    }

    #[test]
    fn target_reservation_key_scopes_names_to_the_resolved_parent_identity() {
        let root = TemporaryRoot::new("target-reservation-parent-identity");
        fs::create_dir(root.path().join("nested")).expect("nested parent should create");
        let manager = manager(&root);
        let nested_identity = manager
            .root
            .entry_identity(&root.path().join("nested"))
            .expect("nested parent identity should read")
            .expect("nested parent should exist");
        let exact = TargetNameSemantics {
            normalization_insensitive: false,
            case_insensitive: false,
            hfs_ignorable: false,
        };
        let root_key = build_target_reservation_key(
            manager.root.root_identity(),
            &["Clip.mp4".to_string()],
            exact,
        )
        .expect("root reservation key should build");
        let nested_key =
            build_target_reservation_key(nested_identity, &["Clip.mp4".to_string()], exact)
                .expect("nested reservation key should build");

        assert_ne!(root_key, nested_key);
        assert_eq!(root_key.components, vec!["Clip.mp4".to_string()]);
        assert_eq!(nested_key.components, vec!["Clip.mp4".to_string()]);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_platform_fold_uses_full_locale_independent_casefold() {
        assert_eq!(
            platform_case_fold("Maße").expect("CFStringFold should support sharp s"),
            platform_case_fold("MASSE").expect("CFStringFold should support uppercase")
        );
        assert_eq!(
            platform_case_fold("Σ").expect("CFStringFold should support capital sigma"),
            platform_case_fold("ς").expect("CFStringFold should support final sigma")
        );
    }
}
