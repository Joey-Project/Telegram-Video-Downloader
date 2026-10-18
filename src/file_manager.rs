use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[cfg(target_os = "macos")]
use std::ffi::CStr;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
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
use crate::file_provider::{
    QueueFileProvider, classify_deadlock_error, is_file_provider_access_error,
    platform_queue_file_provider,
};
use crate::library::{
    BatchMoveResult, BatchStatus, CollectionMetadata, Confidence, ConflictPolicy, FileMovePreview,
    HintOrigin, IntegrityStatus, LegacyCandidate, LegacyImportReport, LibraryAttachment,
    LibraryItem, LibrarySnapshot, MetadataFieldChange, MetadataPatch, MetadataPatchPreview,
    MetadataStatus, MoveOutcome, MovePreview, MovePreviewItem, MoveResultItem, Presence,
    SourceHint,
};
use crate::safe_fs::{
    BoundDirectoryCursor, BoundDirectoryCursorEntry, BoundFile, EntryIdentity, RootedFs,
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
const MAX_DEPTH: usize = 48;
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
        let relative = safe_relative_path(target, false)?;
        validate_not_private_downloader_path(root, &relative, self.private_library_identity)?;
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

        let mut existing_parent = PathBuf::new();
        let mut parent_identity = root.root_identity();
        let mut unresolved_at = parent_components.len();
        for (index, component) in parent_components.iter().enumerate() {
            let candidate = existing_parent.join(component);
            let absolute = root.logical_root_path().join(&candidate);
            match root.entry_identity(&absolute)? {
                Some(identity) if identity.is_dir() => {
                    existing_parent = candidate;
                    parent_identity = identity;
                }
                Some(_) => bail!("target path parent is not a directory"),
                None => {
                    unresolved_at = index;
                    break;
                }
            }
        }

        let directory_key = (parent_identity.device(), parent_identity.inode());
        let semantics = if let Some(semantics) = self.semantics_by_directory.get(&directory_key) {
            *semantics
        } else {
            let directory =
                open_validated_target_directory(root, &existing_parent, parent_identity)?;
            let semantics = target_name_semantics(&directory)?;
            self.semantics_by_directory.insert(directory_key, semantics);
            semantics
        };

        let mut names = parent_components[unresolved_at..].to_vec();
        names.push(target_name.clone());
        build_target_reservation_key(parent_identity, &names, semantics)
    }
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

fn validate_not_private_downloader_path(
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
pub struct LibraryManager {
    root: RootedFs,
    provider: Arc<dyn QueueFileProvider>,
    library_dir: PathBuf,
    library_dir_identity: EntryIdentity,
    in_process_lock: Arc<Mutex<()>>,
    last_snapshot: Arc<Mutex<Option<LibrarySnapshot>>>,
    process_nonce: String,
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
    state: ManifestPatchState,
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

struct StoreGuards {
    _output: BoundFile,
    _library: BoundFile,
}

impl LibraryManager {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_provider(root, platform_queue_file_provider())
    }

    pub(crate) fn open_with_provider(
        root_path: impl AsRef<Path>,
        provider: Arc<dyn QueueFileProvider>,
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
        let library_dir = root.logical_root_path().join(LIBRARY_DIRECTORY);
        let library_dir_identity =
            ensure_private_directory(&root, provider.as_ref(), &library_dir)?;
        let manager = Self {
            root,
            provider,
            library_dir,
            library_dir_identity,
            in_process_lock: Arc::new(Mutex::new(())),
            last_snapshot: Arc::new(Mutex::new(None)),
            process_nonce: PROCESS_NONCE
                .get_or_init(|| new_operation_id("process"))
                .clone(),
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
        report.candidates = legacy_candidates(&report.hints, &snapshot.items);
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

    pub fn preview_with_hints_and_patches(
        &self,
        item_ids: &[String],
        target_relative_dir: Option<&str>,
        rename: bool,
        conflict: ConflictPolicy,
        hints: &[SourceHint],
        patches: &[MetadataPatch],
    ) -> Result<MovePreview> {
        let _process_guard = self
            .in_process_lock
            .lock()
            .map_err(|_| anyhow!("media-library lock is poisoned"))?;
        let _disk_guard = self.lock_store()?;
        let snapshot = self.scan_locked(hints)?;
        self.build_and_persist_preview(
            &snapshot,
            item_ids,
            target_relative_dir,
            rename,
            conflict,
            patches,
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

    fn build_and_persist_preview(
        &self,
        snapshot: &LibrarySnapshot,
        item_ids: &[String],
        target_relative_dir: Option<&str>,
        rename: bool,
        conflict: ConflictPolicy,
        patches: &[MetadataPatch],
    ) -> Result<MovePreview> {
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
            let target_dir = target_dir
                .as_deref()
                .ok_or_else(|| anyhow!("a target directory is required for media moves"))?;
            let item_target_dir = organized_target_dir(item, &snapshot.items, target_dir)?;
            let (files, anchors) = self.preview_files_for_item(
                item,
                &item_target_dir,
                rename,
                conflict,
                &mut reserved_targets,
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
            let source_media = self.absolute_path(&item.relative_path)?;
            let BoundFileRead {
                token: media_identity,
                file: media_file,
                ..
            } = self
                .read_file_with_token_and_handle(&source_media, None)?
                .ok_or_else(|| anyhow!("selected media disappeared before metadata preview"))?;
            preview_anchors.insert(media_identity.clone(), media_file);
            let target_media = move_targets
                .get(patch.item_id.as_str())
                .filter(|entry| entry.outcome == MoveOutcome::Move)
                .map(|entry| entry.target_path.as_str())
                .unwrap_or(&item.relative_path);
            let source_nfo = with_extension(&source_media, "nfo");
            let target_nfo = with_extension(&self.absolute_path(target_media)?, "nfo");
            let target_nfo_relative =
                path_to_relative_string(target_nfo.strip_prefix(self.root.logical_root_path())?)?;
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
                None => (None, None, None, None),
            };
            let new_contents = match original.as_deref() {
                Some(original) => {
                    let source = std::str::from_utf8(original)
                        .context("selected NFO is not UTF-8; no metadata patch was prepared")?;
                    append_metadata_patch(source, patch)?.into_bytes()
                }
                None => new_nfo(patch.title.as_deref().unwrap_or(&item.title), patch).into_bytes(),
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
        store_preview_anchors(&id, preview_anchors)?;
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
        let target_stem = if rename {
            standard_media_stem(item)
        } else {
            primary_stem.to_string()
        };
        let base_target = target_dir.join(file_name_with_stem(&primary, &target_stem)?);
        let target_for = |source_rel: &str, primary_target: &Path| -> Result<String> {
            let source_abs = self.absolute_path(source_rel)?;
            let source_stem = source_abs
                .file_stem()
                .and_then(OsStr::to_str)
                .ok_or_else(|| anyhow!("attached media has a non-UTF-8 filename"))?;
            let suffix = matching_sidecar_suffix_with(
                source_stem,
                primary_stem,
                name_semantics,
                platform_case_fold,
            )?
            .unwrap_or_default();
            let target_primary_stem = primary_target
                .file_stem()
                .and_then(OsStr::to_str)
                .ok_or_else(|| anyhow!("target media filename is invalid"))?;
            let target_name =
                file_name_with_stem(&source_abs, &format!("{target_primary_stem}{suffix}"))?;
            path_to_relative_string(&target_dir.join(target_name))
        };
        let mut candidate = base_target.clone();
        let mut targets = source_paths
            .iter()
            .map(|source| target_for(source, &candidate))
            .collect::<Result<Vec<_>>>()?;
        let conflicts = self.has_move_conflicts(&source_paths, &targets, reserved_targets)?;
        if conflicts && conflict == ConflictPolicy::KeepBoth {
            let parent = base_target.parent().unwrap_or_else(|| Path::new(""));
            let base_stem = base_target
                .file_stem()
                .and_then(OsStr::to_str)
                .ok_or_else(|| anyhow!("target media filename is invalid"))?;
            let extension = base_target
                .extension()
                .and_then(OsStr::to_str)
                .unwrap_or("");
            let mut found = false;
            for suffix in 2..=10_000 {
                let name = if extension.is_empty() {
                    format!("{base_stem} ({suffix})")
                } else {
                    format!("{base_stem} ({suffix}).{extension}")
                };
                candidate = parent.join(name);
                targets = source_paths
                    .iter()
                    .map(|source| target_for(source, &candidate))
                    .collect::<Result<Vec<_>>>()?;
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
        let mut anchors = HashMap::new();
        for (source_rel, target_rel) in source_paths.iter().zip(targets) {
            let source_abs = self.absolute_path(source_rel)?;
            let skip = final_conflicts && conflict == ConflictPolicy::Skip;
            let (identity, size) = if skip {
                let fingerprint = self.file_fingerprint(&source_abs)?.ok_or_else(|| {
                    anyhow!("selected media or attachment disappeared: {source_rel}")
                })?;
                (String::new(), fingerprint.size)
            } else {
                let BoundFileRead { token, file, .. } = self
                    .read_file_with_token_and_handle(&source_abs, None)?
                    .ok_or_else(|| {
                        anyhow!("selected media or attachment disappeared: {source_rel}")
                    })?;
                let size = token_size(&token)?;
                anchors.insert(token.clone(), file);
                (token, size)
            };
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
            match self.execute_nfo_patch(&manifest.preview.id, &manifest.patches[patch_index]) {
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

    fn execute_nfo_patch(&self, preview_id: &str, patch: &PreparedNfoPatch) -> Result<()> {
        let media = self.absolute_path(&patch.media_target_path)?;
        let target = self.absolute_path(&patch.target_path)?;
        let media_anchor = self.preview_anchor(preview_id, &patch.media_identity)?;
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
                let (token, digest) = token_for_bound_file(&file)?;
                if digest == patch.after_sha256 {
                    let path_identity = self.root.entry_identity(&target)?;
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
                let nfo_path_identity = self.root.entry_identity(&target)?;
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
                let entry = self.root.bind_entry(&target, false)?;
                let temporary = private_temp_sibling(&target);
                self.root.replace_bound_file_atomically_if_identity(
                    &entry,
                    file.identity(),
                    &temporary,
                    &patch.replacement,
                    patch.expected_mode.unwrap_or(0o644),
                )?;
            } else {
                if patch.expected_sha256.is_some() {
                    bail!("original NFO disappeared before patch; leaving it untouched");
                }
                self.root.create_new_bound_file(&target, &patch.replacement, 0o644)?;
            }
            let updated = self.root.open_bound_file(&target)?
                .ok_or_else(|| anyhow!("confirmed NFO patch disappeared"))?;
            let (token, digest) = token_for_bound_file(&updated)?;
            if digest != patch.after_sha256 {
                bail!("confirmed NFO patch did not match its prepared content");
            }
            if patch.expected_identity.is_none()
                && !self.created_nfo_matches_prepared(
                    &target, &updated, &token, &digest, patch,
                )?
            {
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
                    Ok(true) => {
                        if kind == OperationStateFileKind::Preview {
                            release_preview_anchors(&id);
                        }
                    }
                    Ok(false) => {}
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
    ) -> Result<bool> {
        self.remove_operation_state_candidate_with_hook(
            candidate,
            limit,
            kind,
            id,
            retention,
            || {},
        )
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
        let operation_state_warnings = self.prune_persisted_operation_states();
        let (mut index, index_warning) = self.read_index_with_recovery()?;
        let mut walk = ScanWalk::default();
        walk.warnings.extend(operation_state_warnings);
        if let Some(warning) = index_warning {
            walk.warnings.push(warning);
        }
        self.walk_directory(Path::new(""), 0, &mut walk)?;
        walk.files
            .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        if walk.files.len() > MAX_ENTRIES {
            for file in walk.files.iter().skip(MAX_ENTRIES) {
                let parent = Path::new(&file.relative_path)
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    .to_string_lossy()
                    .into_owned();
                walk.incomplete_directories.insert(parent);
            }
            walk.files.truncate(MAX_ENTRIES);
            walk.incomplete_scan = true;
            walk.warnings
                .push(format!("scan stopped at the {MAX_ENTRIES}-entry limit"));
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
        let mut unreadable_media = HashMap::new();
        for file in walk
            .files
            .iter()
            .filter(|file| is_primary_media(&file.relative_path))
        {
            if walk.timed_out || walk.started_at.elapsed() >= SCAN_TIMEOUT {
                walk.timed_out = true;
                if !walk
                    .warnings
                    .iter()
                    .any(|warning| warning.contains("120-second scan budget"))
                {
                    walk.warnings.push("scan stopped at the 120-second scan budget; unvisited paths were not marked missing".to_string());
                }
                break;
            }
            if videos.len() >= MAX_MEDIA_ITEMS {
                walk.incomplete_scan = true;
                walk.warnings.push(format!(
                    "scan stopped at the {MAX_MEDIA_ITEMS}-media-item limit"
                ));
                break;
            }
            match self.build_item(
                file,
                &sidecars,
                walk.started_at,
                &mut walk.timed_out,
                &mut walk.warnings,
            ) {
                Ok(built) => {
                    if built.sidecar_title_present {
                        sidecar_title_paths.insert(built.item.relative_path.clone());
                    }
                    videos.push(built.item);
                }
                Err(error) => {
                    let metadata_status = if is_file_provider_access_error(&error) {
                        MetadataStatus::ProviderUnavailable
                    } else {
                        MetadataStatus::Unreadable
                    };
                    unreadable_media.insert(file.relative_path.clone(), metadata_status);
                    walk.warnings.push(format!(
                        "skipped unreadable media entry {}: {error:#}",
                        file.relative_path
                    ));
                }
            }
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
        let mut fingerprints = BTreeMap::new();
        let mut current_object_counts: HashMap<(u64, u64, Option<i64>, Option<i64>), usize> =
            HashMap::new();
        for item in &videos {
            if walk.started_at.elapsed() >= SCAN_TIMEOUT {
                walk.timed_out = true;
                walk.warnings.push("scan stopped at the 120-second scan budget; unvisited paths were not marked missing".to_string());
                break;
            }
            if item.presence != Presence::Present {
                continue;
            }
            if let Some(fingerprint) =
                self.file_fingerprint(&self.absolute_path(&item.relative_path)?)?
            {
                *current_object_counts
                    .entry(index_object_key(&fingerprint))
                    .or_default() += 1;
                fingerprints.insert(item.relative_path.clone(), fingerprint);
            }
        }
        let old_records = index.records.clone();
        let mut used_ids = HashSet::new();
        for (item_index, item) in videos.iter_mut().enumerate() {
            let Some(fingerprint) = fingerprints.get(&item.relative_path) else {
                continue;
            };
            let mut item_queue_metadata = queue_metadata[item_index].clone();
            let key = index_object_key(fingerprint);
            let exact = old_records.iter().find(|(id, record)| {
                !used_ids.contains(*id)
                    && record.item.relative_path == item.relative_path
                    && index_record_matches(record, fingerprint)
            });
            let unique_rename = if exact.is_none() && current_object_counts.get(&key) == Some(&1) {
                let candidates = old_records
                    .iter()
                    .filter(|(id, record)| {
                        !used_ids.contains(*id) && index_record_matches(record, fingerprint)
                    })
                    .collect::<Vec<_>>();
                if candidates.len() == 1 {
                    candidates.first().copied()
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
            if let Some((_, record)) = prior {
                // Queue metadata follows a physical item only after the index's object identity
                // signals match. The current path alone never carries an old association.
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

        let observed = videos
            .iter()
            .map(|item| item.id.clone())
            .collect::<HashSet<_>>();
        for (id, record) in &mut index.records {
            if observed.contains(id) {
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

        let pruned_missing =
            prune_missing_index_records(&mut index, MAX_MISSING_INDEX_RECORDS, INDEX_LIMIT)?;
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
        let categories = walk.root_categories.clone();
        let revision = snapshot_revision(&videos)?;
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
        index.version = INDEX_VERSION;
        self.write_index(&index)?;
        *self
            .last_snapshot
            .lock()
            .map_err(|_| anyhow!("media-library snapshot lock is poisoned"))? =
            Some(snapshot.clone());
        Ok(snapshot)
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
                    || !case_only_path_alias(
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
                if walk.files.len() >= MAX_ENTRIES {
                    walk.incomplete_scan = true;
                    walk.incomplete_directories.insert(relative_string.clone());
                    walk.warnings
                        .push(format!("scan stopped at the {MAX_ENTRIES}-entry limit"));
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
        scan_started_at: Instant,
        scan_timed_out: &mut bool,
        warnings: &mut Vec<String>,
    ) -> Result<BuiltLibraryItem> {
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
                warnings.push(format!(
                    "media unavailable at {}: {error:#}",
                    media.relative_path
                ));
                let id = physical_item_id_from_identity(media.identity, &media.relative_path);
                return Ok(BuiltLibraryItem {
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
                        attachments: Vec::new(),
                        confidence: Confidence::Low,
                        metadata_status: status,
                        integrity_status: IntegrityStatus::Unverified,
                        presence: Presence::Unavailable,
                    },
                    sidecar_title_present: false,
                });
            }
        };
        let (fingerprint, presence) = match opened_file.as_ref() {
            Some(file) => (Some(fingerprint(file)?), Presence::Present),
            None => (None, Presence::Missing),
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
        let mut nfo_title_present = false;
        let mut nfo_source_id_present = false;
        let mut sidecar_title_present = false;
        let mut metadata_status = if presence == Presence::Missing {
            MetadataStatus::ProviderUnavailable
        } else {
            MetadataStatus::Unknown
        };
        let mut sidecar_unreadable = false;
        let attachments = sidecars.attached_files(media);
        for attachment in &attachments {
            if scan_started_at.elapsed() >= SCAN_TIMEOUT {
                *scan_timed_out = true;
                warnings.push("scan stopped at the 120-second scan budget; unvisited paths were not marked missing".to_string());
                break;
            }
            let lower = attachment.to_ascii_lowercase();
            if lower.ends_with(".nfo") {
                let path = self.absolute_path(attachment)?;
                match self.read_file(&path, SIDE_CAR_LIMIT) {
                    Ok(Some(contents)) => match String::from_utf8(contents) {
                        Ok(text) => {
                            let nfo_title = xml_tag_text(&text, "title")
                                .filter(|value| !value.trim().is_empty());
                            let nfo_ids = nfo_source_ids(&text);
                            if let Some(value) = &nfo_title {
                                title = value.trim().to_string();
                            }
                            nfo_title_present |= nfo_title.is_some();
                            sidecar_title_present |= nfo_title.is_some();
                            nfo_source_id_present |= !nfo_ids.is_empty();
                            source_ids.extend(nfo_ids);
                            if let Some(value) = xml_tag_text(&text, "set")
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
                            warnings.push(format!("NFO is not UTF-8: {attachment}"));
                        }
                    },
                    Ok(None) => {}
                    Err(error) => {
                        sidecar_unreadable = true;
                        warnings.push(format!("NFO unavailable at {attachment}: {error:#}"));
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
                        warnings.push(format!(
                            "metadata sidecar unavailable at {attachment}: {error:#}"
                        ));
                    }
                }
            }
        }
        source_ids = normalize_source_ids(source_ids);
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

        let media_specs =
            if presence == Presence::Present && scan_started_at.elapsed() < SCAN_TIMEOUT {
                opened_file
                    .as_ref()
                    .ok_or_else(|| anyhow!("media descriptor disappeared during scan"))
                    .and_then(probe_media)
                    .unwrap_or_else(|error| {
                        warnings.push(format!(
                            "ffprobe unavailable for {}: {error:#}",
                            media.relative_path
                        ));
                        MediaSpecs::default()
                    })
            } else {
                if scan_started_at.elapsed() >= SCAN_TIMEOUT {
                    *scan_timed_out = true;
                }
                MediaSpecs::default()
            };
        let bytes = fingerprint.as_ref().map(|value| value.size).unwrap_or(0);
        let library_attachments = attachments
            .iter()
            .map(|path| LibraryAttachment {
                relative_path: path.clone(),
                kind: attachment_kind(path).to_string(),
            })
            .collect();
        Ok(BuiltLibraryItem {
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
                attachments: library_attachments,
                confidence,
                metadata_status,
                integrity_status: IntegrityStatus::Unverified,
                presence,
            },
            sidecar_title_present,
        })
    }

    fn absolute_path(&self, relative: &str) -> Result<PathBuf> {
        let relative = self.safe_relative_path(relative, false)?;
        Ok(self.root.logical_root_path().join(relative))
    }

    fn safe_relative_path(&self, value: &str, allow_empty: bool) -> Result<PathBuf> {
        let relative = safe_relative_path(value, allow_empty)?;
        validate_not_private_downloader_path(
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

    fn write_index(&self, index: &LibraryIndex) -> Result<()> {
        let contents = serde_json::to_vec(index).context("failed to encode media-library index")?;
        if contents.len() > INDEX_LIMIT {
            bail!("media-library index exceeds its size limit");
        }
        self.write_private_file(&self.library_dir.join(LIBRARY_INDEX), &contents)
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
        Ok(StoreGuards {
            _output: output,
            _library: file,
        })
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
    let logical = root.logical_root_path();
    let canonical = root.root_path();
    if path == logical || path == canonical {
        return Ok(logical.to_path_buf());
    }
    let relative = path
        .strip_prefix(logical)
        .or_else(|_| path.strip_prefix(canonical))
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
        name.to_str()
            .ok_or_else(|| anyhow!("relative path is not UTF-8"))?;
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

fn organized_target_dir(
    item: &LibraryItem,
    all_items: &[LibraryItem],
    base: &Path,
) -> Result<PathBuf> {
    let Some(collection) = item
        .collection
        .as_ref()
        .filter(|collection| !collection.kind.trim().is_empty())
    else {
        return Ok(base.to_path_buf());
    };
    let title = collection.title.trim();
    if title.is_empty() {
        return Ok(base.to_path_buf());
    }
    let mut folder = safe_file_stem(title);
    let has_title_collision = all_items.iter().any(|other| {
        if other.id == item.id {
            return false;
        }
        let Some(other_collection) = other
            .collection
            .as_ref()
            .filter(|value| !value.kind.trim().is_empty())
        else {
            return false;
        };
        normalize_title(&other_collection.title) == normalize_title(title)
            && (other_collection.kind != collection.kind || other_collection.id != collection.id)
    });
    if has_title_collision
        && let Some(id) = collection.id.as_deref().filter(|id| !id.trim().is_empty())
    {
        folder = format!("{} [{}]", folder, safe_file_stem(id));
    }
    let base_name = base.file_name().and_then(OsStr::to_str).unwrap_or_default();
    if normalize_title(base_name) == normalize_title(&folder) {
        Ok(base.to_path_buf())
    } else {
        Ok(base.join(folder))
    }
}

fn file_name_with_stem(source: &Path, stem: &str) -> Result<String> {
    let extension = source.extension().and_then(OsStr::to_str);
    let file_name = match extension {
        Some(extension) if !extension.is_empty() => format!("{stem}.{extension}"),
        _ => stem.to_string(),
    };
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

fn store_preview_anchors(preview_id: &str, anchors: HashMap<String, BoundFile>) -> Result<()> {
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

fn require_preview_anchors(persisted: &PersistedPreview, process_nonce: &str) -> Result<()> {
    let mut required = HashSet::new();
    for file in persisted.preview.items.iter().flat_map(|item| &item.files) {
        if !file.identity.is_empty()
            && decode_identity_token(&file.identity)?
                .birth_seconds
                .is_none()
        {
            required.insert(file.identity.as_str());
        }
    }
    for patch in &persisted.patches {
        if decode_identity_token(&patch.media_identity)?
            .birth_seconds
            .is_none()
        {
            required.insert(patch.media_identity.as_str());
        }
        if let Some(identity) = patch.expected_identity.as_deref()
            && decode_identity_token(identity)?.birth_seconds.is_none()
        {
            required.insert(identity);
        }
    }
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
    let anchors = cache.touch(&persisted.preview.id, now).ok_or_else(|| {
        anyhow!("selected object lost its in-process descriptor anchor; create a new preview")
    })?;
    for token in required {
        let anchor = anchors.get(token).ok_or_else(|| {
            anyhow!("selected object lost its in-process descriptor anchor; create a new preview")
        })?;
        let expected = decode_identity_token(token)?;
        let actual = anchor.identity();
        if actual.device() != expected.device
            || actual.inode() != expected.inode
            || !actual.is_file()
        {
            bail!(
                "in-process descriptor anchor no longer matches the approved object; create a new preview"
            );
        }
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

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn index_object_key(fingerprint: &FileFingerprint) -> (u64, u64, Option<i64>, Option<i64>) {
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

    if let Some(collection) = provenance.collection.as_ref() {
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

    let encoded = serde_json::to_vec(index).context("failed to size the media-library index")?;
    if encoded.len() <= max_serialized_bytes {
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
    let base_size = serde_json::to_vec(&without_missing)
        .context("failed to size the media-library index without missing records")?
        .len();
    if base_size > max_serialized_bytes {
        for (id, _) in remaining {
            index.records.remove(&id);
            removed.insert(id);
        }
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
        let size = serde_json::to_vec(&candidate)
            .context("failed to size the bounded media-library index")?
            .len();
        if size <= max_serialized_bytes {
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

fn physical_item_id(fingerprint: &FileFingerprint, relative_path: &str) -> String {
    physical_item_id_from_identity(fingerprint.identity, relative_path)
}

fn physical_item_id_from_identity(identity: EntryIdentity, relative_path: &str) -> String {
    // Path participates only to keep two hard-link names distinct. The persisted index carries
    // the same ID across a unique Finder move by matching object identity and creation time.
    let raw = format!("{}:{}:{relative_path}", identity.device(), identity.inode());
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
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name,width,height",
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
    let Some(stream) = value
        .get("streams")
        .and_then(serde_json::Value::as_array)
        .and_then(|streams| streams.first())
    else {
        return Ok(MediaSpecs::default());
    };
    Ok(MediaSpecs {
        width: stream
            .get("width")
            .and_then(serde_json::Value::as_u64)
            .and_then(|v| u32::try_from(v).ok()),
        height: stream
            .get("height")
            .and_then(serde_json::Value::as_u64)
            .and_then(|v| u32::try_from(v).ok()),
        codec: stream
            .get("codec_name")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
    })
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
        Ok(index)
    }

    fn attached_files(&self, media: &ScannedFile) -> Vec<String> {
        self.by_media
            .get(&media.relative_path)
            .cloned()
            .unwrap_or_default()
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
    while let Some(start) = find_ascii_case_insensitive(content, "<uniqueid", cursor) {
        let Some(end_offset) = content[start..].find('>') else {
            break;
        };
        let end = start + end_offset;
        let Some(close) = find_ascii_case_insensitive(content, "</uniqueid>", end + 1) else {
            break;
        };
        let tag = &content[start..=end];
        let kind = xml_attribute(tag, "type")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let raw_id = xml_text_content(&content[end + 1..close])
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
                cursor = close + "</uniqueid>".len();
                continue;
            }
        };
        ids.extend(normalize_source_ids(vec![canonical]));
        cursor = close + "</uniqueid>".len();
    }
    deduplicate(ids)
}

fn xml_tag_text(content: &str, name: &str) -> Option<String> {
    let opening = format!("<{name}");
    let mut search_from = 0;
    while let Some(start) = find_ascii_case_insensitive(content, &opening, search_from) {
        let after_name = start + opening.len();
        if content
            .as_bytes()
            .get(after_name)
            .is_some_and(|byte| !byte.is_ascii_whitespace() && *byte != b'>' && *byte != b'/')
        {
            search_from = after_name;
            continue;
        }
        let open_end = content[after_name..].find('>')? + after_name;
        if self_closing_xml_tag_slash(content, start, open_end).is_some() {
            return Some(String::new());
        }
        let close_marker = format!("</{name}>");
        let close = find_ascii_case_insensitive(content, &close_marker, open_end + 1)?;
        let value = xml_text_content(&content[open_end + 1..close]);
        return Some(value.trim().to_string());
    }
    None
}

fn xml_attribute(tag: &str, attribute: &str) -> Option<String> {
    let needle = format!("{attribute}=");
    let offset = find_ascii_case_insensitive(tag, &needle, 0)? + needle.len();
    let quote = tag.as_bytes().get(offset).copied()?;
    if quote != b'\'' && quote != b'"' {
        return None;
    }
    let end = tag.as_bytes()[offset + 1..]
        .iter()
        .position(|byte| *byte == quote)?
        + offset
        + 1;
    Some(tag[offset + 1..end].to_string())
}

fn strip_xml_markup(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut inside = false;
    for character in value.chars() {
        match character {
            '<' => inside = true,
            '>' => inside = false,
            _ if !inside => result.push(character),
            _ => {}
        }
    }
    result
}

fn xml_text_content(value: &str) -> String {
    const CDATA_OPEN: &str = "<![CDATA[";
    const CDATA_CLOSE: &str = "]]>";

    let mut result = String::with_capacity(value.len());
    let mut cursor = 0;
    while cursor < value.len() {
        let Some(open_offset) = value[cursor..].find(CDATA_OPEN) else {
            result.push_str(&xml_unescape(&strip_xml_markup(&value[cursor..])));
            break;
        };
        let open = cursor + open_offset;
        result.push_str(&xml_unescape(&strip_xml_markup(&value[cursor..open])));
        let content_start = open + CDATA_OPEN.len();
        let Some(close_offset) = value[content_start..].find(CDATA_CLOSE) else {
            result.push_str(&value[content_start..]);
            break;
        };
        let close = content_start + close_offset;
        result.push_str(&value[content_start..close]);
        cursor = close + CDATA_CLOSE.len();
    }
    result
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
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
    ]
    .into_iter()
    .filter_map(|(field, tag)| {
        let after = xml_tag_text(after, tag)?;
        let before = before.and_then(|xml| xml_tag_text(xml, tag));
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
        if xml_tag_text(&output, "title").is_some() {
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
    if let Some(collection) = &patch.collection {
        if !collection.title.trim().is_empty() {
            if xml_tag_text(&output, "set").is_some() {
                output = replace_xml_element(&output, "set", collection.title.trim())?;
            } else {
                nodes.push(format!(
                    "  <set>{}</set>",
                    xml_escape(collection.title.trim())
                ));
            }
        }
        if let Some(order) = collection.order {
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
            if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
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
        {
            if xml_tag_text(&output, "partid").is_some() {
                output = replace_xml_element(&output, "partid", part_id.trim())?;
            } else {
                nodes.push(format!("  <partid>{}</partid>", xml_escape(part_id.trim())));
            }
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

fn replace_xml_element(content: &str, name: &str, value: &str) -> Result<String> {
    let opening = format!("<{name}");
    let mut search_from = 0;
    let (start, open_end) = loop {
        let Some(start) = find_ascii_case_insensitive(content, &opening, search_from) else {
            bail!("NFO element disappeared while preparing its confirmed update");
        };
        let boundary = content.as_bytes().get(start + opening.len()).copied();
        if !boundary.is_some_and(|byte| byte.is_ascii_whitespace() || byte == b'>' || byte == b'/')
        {
            search_from = start + opening.len();
            continue;
        }
        let end = content[start + opening.len()..]
            .find('>')
            .map(|offset| start + opening.len() + offset)
            .ok_or_else(|| anyhow!("NFO element has an incomplete opening tag"))?;
        break (start, end);
    };
    if let Some(slash) = self_closing_xml_tag_slash(content, start, open_end) {
        let mut output = String::with_capacity(content.len() + value.len() + name.len() + 3);
        output.push_str(&content[..slash]);
        output.push_str(&content[slash + 1..open_end + 1]);
        output.push_str(&xml_escape(value));
        output.push_str("</");
        output.push_str(name);
        output.push('>');
        output.push_str(&content[open_end + 1..]);
        return Ok(output);
    }
    let close_marker = format!("</{name}>");
    let close_start = find_ascii_case_insensitive(content, &close_marker, open_end + 1)
        .ok_or_else(|| anyhow!("NFO element has no closing tag"))?;
    let close_end = close_start + close_marker.len();
    let mut output = String::with_capacity(content.len() + value.len());
    output.push_str(&content[..open_end + 1]);
    output.push_str(&xml_escape(value));
    output.push_str(&content[close_start..close_end]);
    output.push_str(&content[close_end..]);
    Ok(output)
}

fn new_nfo(title: &str, patch: &MetadataPatch) -> String {
    let mut content =
        String::from("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<movie>\n");
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
    let root_name = loop {
        let start = find_ascii_case_insensitive(content, "<", cursor)?;
        let next = *content.as_bytes().get(start + 1)?;
        if next == b'?' || next == b'!' || next == b'/' {
            cursor = start + 2;
            continue;
        }
        let name_start = start + 1;
        let name_end = content[name_start..].find(|character: char| {
            character.is_ascii_whitespace() || character == '>' || character == '/'
        })? + name_start;
        break &content[name_start..name_end];
    };
    let closing = format!("</{root_name}");
    let mut close_start = None;
    let mut search_from = 0;
    while let Some(found) = find_ascii_case_insensitive(content, &closing, search_from) {
        close_start = Some(found);
        search_from = found + closing.len();
    }
    let close_start = close_start?;
    let close_end = content[close_start..].find('>')? + close_start + 1;
    if !content[close_end..].trim().is_empty() {
        return None;
    }
    Some(close_start)
}

fn find_ascii_case_insensitive(haystack: &str, needle: &str, from: usize) -> Option<usize> {
    if needle.is_empty() {
        return Some(from);
    }
    haystack
        .as_bytes()
        .get(from..)?
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
        .map(|offset| from + offset)
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

fn missing_patch_source_ids(existing: Vec<String>, requested: &[String]) -> Vec<String> {
    let existing = existing.into_iter().collect::<HashSet<_>>();
    normalize_source_ids(requested.to_vec())
        .into_iter()
        .filter(|id| !existing.contains(id))
        .collect()
}

fn validate_metadata_patch(patch: &MetadataPatch) -> Result<()> {
    if patch
        .title
        .as_deref()
        .is_some_and(|title| title.len() > 4096)
    {
        bail!("metadata title exceeds its size limit");
    }
    if patch.source_ids.len() > 64
        || normalize_source_ids(patch.source_ids.clone()).len() != patch.source_ids.len()
    {
        bail!("metadata source IDs are invalid or duplicated");
    }
    if let Some(collection) = &patch.collection {
        if collection.title.trim().is_empty() || collection.title.len() > 1024 {
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
    {
        bail!("metadata patch has no fields to apply");
    }
    Ok(())
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
    while index < bytes.len() {
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

fn case_only_path_alias(
    hint_path: &str,
    scanned_path: &str,
    directory_semantics: &HashMap<String, TargetNameSemantics>,
) -> bool {
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
        return false;
    }

    let mut parent = PathBuf::new();
    let mut differs_only_by_case = false;
    for (hint_component, scanned_component) in hint_components.iter().zip(scanned_components.iter())
    {
        if hint_component != scanned_component {
            let parent_key = parent
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            if !directory_semantics
                .get(&parent_key)
                .is_some_and(|semantics| semantics.case_insensitive)
                || !same_unicode_text_ignoring_case(hint_component, scanned_component)
            {
                return false;
            }
            differs_only_by_case = true;
        }
        parent.push(scanned_component);
    }
    differs_only_by_case
}

fn same_unicode_text_ignoring_case(left: &str, right: &str) -> bool {
    left != right
        && left
            .chars()
            .flat_map(char::to_lowercase)
            .eq(right.chars().flat_map(char::to_lowercase))
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
            item.collection = item.collection.clone().or_else(|| hint.collection.clone());
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

fn legacy_candidates(hints: &[SourceHint], items: &[LibraryItem]) -> Vec<LegacyCandidate> {
    let mut result = Vec::new();
    for (hint_index, hint) in hints.iter().enumerate() {
        let hint_ids = normalize_source_ids(hint.source_ids.clone())
            .into_iter()
            .collect::<HashSet<_>>();
        let direct = items
            .iter()
            .filter(|item| item.source_ids.iter().any(|id| hint_ids.contains(id)))
            .collect::<Vec<_>>();
        let (matches, match_kind) = if !direct.is_empty() {
            (direct, "source_id")
        } else if let Some(title) = hint.title.as_deref() {
            let normalized = normalize_title(title);
            let title_matches = items
                .iter()
                .filter(|item| normalize_title(&item.title) == normalized)
                .collect::<Vec<_>>();
            (title_matches, "title")
        } else {
            (Vec::new(), "unmatched")
        };
        let ambiguous = matches.len() > 1 || hint.source_ids.is_empty();
        for item in matches {
            result.push(LegacyCandidate {
                hint_index,
                item_id: item.id.clone(),
                title: item.title.clone(),
                relative_path: item.relative_path.clone(),
                match_kind: match_kind.to_string(),
                ambiguous,
                requires_confirmation: true,
            });
        }
    }
    result
}

fn parse_legacy_import(input: &str) -> LegacyImportReport {
    let parsed_json = serde_json::from_str::<serde_json::Value>(input).ok();
    let (text_blocks, origin) = match parsed_json.as_ref() {
        Some(value) => {
            let mut blocks = Vec::new();
            collect_legacy_text(value, &mut blocks, LEGACY_INPUT_LIMIT);
            (blocks, HintOrigin::LegacyTelegramJson)
        }
        None => (vec![input.to_string()], HintOrigin::LegacyText),
    };
    let mut hints = Vec::new();
    let mut warnings = Vec::new();
    for block in &text_blocks {
        for line in block.lines().take(10_000) {
            let ids = extract_source_ids(line);
            if ids.is_empty() {
                continue;
            }
            let title = title_hint_from_line(line);
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
                "no Bilibili BV/av/cid/ep identifiers were found in the supplied text".to_string(),
            );
        }
    }
    let mut seen = HashSet::new();
    hints.retain(|hint| seen.insert((hint.source_ids.clone(), hint.title.clone())));
    LegacyImportReport {
        needs_confirmation: !hints.is_empty(),
        hints,
        warnings,
        revision: None,
        candidates: Vec::new(),
    }
}

fn collect_legacy_text(value: &serde_json::Value, output: &mut Vec<String>, remaining: usize) {
    if output.iter().map(String::len).sum::<usize>() >= remaining {
        return;
    }
    match value {
        serde_json::Value::String(text) => {
            let room = remaining.saturating_sub(output.iter().map(String::len).sum::<usize>());
            if room > 0 {
                output.push(text.chars().take(room).collect());
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_legacy_text(value, output, remaining);
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
                    collect_legacy_text(value, output, remaining);
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
        if lower.starts_with(prefix) || trimmed.starts_with(prefix) {
            let value = trimmed[prefix.len()..].trim();
            if !value.is_empty() && value.len() <= 512 {
                return Some(value.to_string());
            }
        }
    }
    if let Some((title, rest)) = trimmed.split_once(" - ")
        && !title.trim().is_empty()
        && !title.contains("http")
        && !extract_source_ids(rest).is_empty()
        && title.len() <= 512
    {
        return Some(title.trim().to_string());
    }
    None
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::file_provider::{MockQueueFileProvider, QueueFileProvider};

    use super::*;

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
            attachments: Vec::new(),
            confidence: Confidence::Low,
            metadata_status: MetadataStatus::Unknown,
            integrity_status: IntegrityStatus::Unverified,
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
    fn round5_case_only_hint_alias_requires_case_insensitive_directory_semantics() {
        let insensitive = HashMap::from([(String::new(), round5_semantics(true, false))]);
        let sensitive = HashMap::from([(String::new(), round5_semantics(false, false))]);
        assert!(case_only_path_alias("clip.mp4", "Clip.mp4", &insensitive));
        assert!(!case_only_path_alias("clip.mp4", "Clip.mp4", &sensitive));
        assert!(!case_only_path_alias("alias.mp4", "Clip.mp4", &insensitive));
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
        manager
            .write_index(&index)
            .expect("large but bounded fixture index should persist");

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

    #[test]
    fn repeated_metadata_patch_expands_self_closing_tag_without_losing_siblings() {
        let original = "<movie><title/><futureField key=\"x\">keep &amp; opaque</futureField><actor>keep actor</actor></movie>";
        let first_patch = MetadataPatch {
            item_id: "fixture-item".to_string(),
            title: Some("First title".to_string()),
            source_ids: Vec::new(),
            collection: None,
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
            attachments: Vec::new(),
            confidence: Confidence::High,
            metadata_status: MetadataStatus::Complete,
            integrity_status: IntegrityStatus::Unverified,
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
                anchors,
                expires_at: now - Duration::from_secs(1),
            },
        );
        store.order.push_back("preview-anchor-recent".to_string());
        store.by_preview.insert(
            "preview-anchor-recent".to_string(),
            PreviewAnchorEntry {
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
        store_preview_anchors(&preview_id, anchors).expect("live preview anchors should be stored");
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
    fn preview_anchor_lru_capacity_evicts_oldest_and_keeps_all_limits() {
        let now = Instant::now();
        let mut store = PreviewAnchorStore::default();
        for index in 0..MAX_PINNED_PREVIEWS {
            let preview_id = format!("preview-capacity-{index}");
            store.order.push_back(preview_id.clone());
            store.by_preview.insert(
                preview_id,
                PreviewAnchorEntry {
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
