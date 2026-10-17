use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::io::{Read, Seek, SeekFrom};
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
use crate::safe_fs::{BoundFile, EntryIdentity, RootedFs};

const LIBRARY_DIRECTORY: &str = ".telegram-video-downloader-library";
const LIBRARY_INDEX: &str = "index.json";
const LIBRARY_LOCK: &str = "library.lock";
const PREVIEW_PREFIX: &str = "preview-";
const BATCH_PREFIX: &str = "batch-";
const INDEX_VERSION: u32 = 1;
const INDEX_LIMIT: usize = 16 * 1024 * 1024;
const PREVIEW_LIMIT: usize = 4 * 1024 * 1024;
const MANIFEST_LIMIT: usize = 16 * 1024 * 1024;
const SIDE_CAR_LIMIT: usize = 1024 * 1024;
const LEGACY_INPUT_LIMIT: usize = 1024 * 1024;
const MAX_DEPTH: usize = 48;
const MAX_ENTRIES: usize = 200_000;
const MAX_MEDIA_ITEMS: usize = 50_000;
const MAX_PINNED_PREVIEWS: usize = 64;
const MAX_PINNED_FILES_PER_PREVIEW: usize = 512;
const MAX_PINNED_FILES_TOTAL: usize = 4096;
const FFPROBE_TIMEOUT: Duration = Duration::from_secs(3);
const SCAN_TIMEOUT: Duration = Duration::from_secs(120);
const FFPROBE_OUTPUT_LIMIT: usize = 64 * 1024;
static PRIVATE_TEMP_SERIAL: AtomicU64 = AtomicU64::new(0);
static PROCESS_NONCE: OnceLock<String> = OnceLock::new();
static LIVE_PREVIEW_ANCHORS: OnceLock<Mutex<PreviewAnchorStore>> = OnceLock::new();

#[derive(Default)]
struct PreviewAnchorStore {
    by_preview: HashMap<String, HashMap<String, BoundFile>>,
    order: VecDeque<String>,
    file_count: usize,
}

#[derive(Clone)]
pub struct LibraryManager {
    root: RootedFs,
    provider: Arc<dyn QueueFileProvider>,
    library_dir: PathBuf,
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
        ensure_private_directory(&root, provider.as_ref(), &library_dir)?;
        let manager = Self {
            root,
            provider,
            library_dir,
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

    pub fn import_legacy(&self, input: &str) -> Result<LegacyImportReport> {
        if input.len() > LEGACY_INPUT_LIMIT {
            bail!("legacy import text exceeds the 1 MiB limit");
        }
        let mut report = parse_legacy_import(input);
        let snapshot = self.scan(&[])?;
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
            if manifest.status == BatchStatus::Complete {
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
            .map(|value| safe_relative_path(value, true))
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
        let mut reserved_targets = HashSet::new();
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
                &reserved_targets,
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
                reserved_targets.extend(files.iter().map(|file| file.target_path.clone()));
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
        reserved_targets: &HashSet<String>,
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
            let suffix = source_stem
                .strip_prefix(primary_stem)
                .filter(|suffix| suffix.is_empty() || suffix.starts_with('.'))
                .unwrap_or("");
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
        reserved_targets: &HashSet<String>,
    ) -> Result<bool> {
        for (source, target) in source_paths.iter().zip(target_paths) {
            if reserved_targets.contains(target) {
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
        let cache = cache
            .lock()
            .map_err(|_| anyhow!("preview identity-anchor cache is poisoned"))?;
        Ok(cache
            .by_preview
            .get(preview_id)
            .and_then(|anchors| anchors.get(identity))
            .cloned())
    }

    fn batch_path(&self, id: &str) -> PathBuf {
        self.library_dir.join(format!("{BATCH_PREFIX}{id}.json"))
    }

    fn read_persisted_preview(&self, id: &str) -> Result<PersistedPreview> {
        let bytes = self
            .read_private_file(&self.preview_path(id), PREVIEW_LIMIT)?
            .ok_or_else(|| anyhow!("move preview was not found"))?;
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
                if manifest.status == BatchStatus::Complete {
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
                    files,
                    patches: persisted.patches.clone(),
                    items,
                    warnings: Vec::new(),
                }
            }
        };
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

        let has_errors = manifest.items.iter().any(|item| item.error.is_some())
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
                    let expected = patch.expected_identity.as_deref().ok_or_else(|| {
                        anyhow!("NFO destination appeared after preview; leaving it untouched")
                    })?;
                    let path_identity = self.root.entry_identity(&target)?;
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
            let (_, digest) = token_for_bound_file(&updated)?;
            if digest != patch.after_sha256 {
                bail!("confirmed NFO patch did not match its prepared content");
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

    fn write_batch_manifest(&self, path: &Path, manifest: &BatchManifest) -> Result<()> {
        let contents =
            serde_json::to_vec(manifest).context("failed to encode move batch manifest")?;
        if contents.len() > MANIFEST_LIMIT {
            bail!("move batch manifest exceeds its private storage limit");
        }
        self.write_private_file(path, &contents)
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
        let mut index = self.read_index()?;
        let mut walk = ScanWalk::default();
        self.walk_directory(Path::new(""), 0, &mut walk)?;
        walk.files
            .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        if walk.files.len() > MAX_ENTRIES {
            walk.files.truncate(MAX_ENTRIES);
            walk.incomplete_scan = true;
            walk.warnings
                .push(format!("scan stopped at the {MAX_ENTRIES}-entry limit"));
        }

        let mut videos = Vec::new();
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
                &walk.files,
                hints,
                walk.started_at,
                &mut walk.timed_out,
                &mut walk.warnings,
            ) {
                Ok(item) => videos.push(item),
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

        attach_matching_hints(&mut videos, hints);
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
        for item in &mut videos {
            let Some(fingerprint) = fingerprints.get(&item.relative_path) else {
                continue;
            };
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
            let stable_id = exact
                .or(unique_rename)
                .map(|(id, _)| id.clone())
                .unwrap_or_else(|| physical_item_id(fingerprint, &item.relative_path));
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
            let entries = if relative.as_os_str().is_empty() {
                self.root.list_root_directory()?
            } else {
                let entry = self.root.bind_entry(&absolute, false)?;
                let Some(identity) = self.root.bound_entry_identity(&entry)? else {
                    bail!("directory disappeared during scan");
                };
                if !identity.is_dir() {
                    bail!("scan path is no longer a directory");
                }
                self.root.list_bound_directory(&entry, identity)?
            };
            Ok(entries)
        });

        let entries = match result {
            Ok(entries) => entries,
            Err(error) => {
                walk.incomplete_directories.insert(relative_string.clone());
                let class = if is_file_provider_access_error(&error) {
                    "File Provider access unavailable"
                } else {
                    "directory unreadable"
                };
                walk.warnings
                    .push(format!("{class} at {relative_string}: {error:#}"));
                return Ok(());
            }
        };
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
            if excluded_entry(&name) {
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
        all_files: &[ScannedFile],
        hints: &[SourceHint],
        scan_started_at: Instant,
        scan_timed_out: &mut bool,
        warnings: &mut Vec<String>,
    ) -> Result<LibraryItem> {
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
                return Ok(LibraryItem {
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
        let mut metadata_status = if presence == Presence::Missing {
            MetadataStatus::ProviderUnavailable
        } else {
            MetadataStatus::Unknown
        };
        let mut sidecar_unreadable = false;
        let attachments = attached_files(media, all_files, warnings)?;
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
                            if let Some(value) = xml_tag_text(&text, "title")
                                && !value.trim().is_empty()
                            {
                                title = value.trim().to_string();
                            }
                            source_ids.extend(nfo_source_ids(&text));
                            if let Some(value) = xml_tag_text(&text, "set")
                                && !value.trim().is_empty()
                            {
                                collection = Some(CollectionMetadata {
                                    id: None,
                                    title: value.trim().to_string(),
                                    kind: "nfo_set".to_string(),
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
                            if title == title_from_path(&media.relative_path)
                                && let Some(json_title) = json_title
                            {
                                title = json_title;
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
        let relevant_hints = matching_hints_for_item(&media.relative_path, &source_ids, hints);
        let mut confidence = if !source_ids.is_empty() && metadata_status == MetadataStatus::Partial
        {
            Confidence::High
        } else if !source_ids.is_empty() {
            Confidence::Medium
        } else {
            Confidence::Low
        };
        for hint in relevant_hints {
            source_ids.extend(hint.source_ids.iter().cloned());
            if title == title_from_path(&media.relative_path)
                && let Some(hint_title) = hint.title.as_deref()
                && !hint_title.trim().is_empty()
            {
                title = hint_title.trim().to_string();
            }
            collection = collection.or_else(|| hint.collection.clone());
            if hint.requires_confirmation {
                confidence = Confidence::NeedsConfirmation;
            } else if confidence == Confidence::Low {
                confidence = Confidence::Medium;
            }
            metadata_status = MetadataStatus::Partial;
        }
        source_ids = normalize_source_ids(source_ids);
        if sidecar_unreadable {
            metadata_status = MetadataStatus::Unreadable;
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
        Ok(LibraryItem {
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
        })
    }

    fn absolute_path(&self, relative: &str) -> Result<PathBuf> {
        let relative = safe_relative_path(relative, false)?;
        Ok(self.root.logical_root_path().join(relative))
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

    fn read_index(&self) -> Result<LibraryIndex> {
        let path = self.library_dir.join(LIBRARY_INDEX);
        let Some(contents) = self.read_private_file(&path, INDEX_LIMIT)? else {
            return Ok(LibraryIndex {
                version: INDEX_VERSION,
                ..LibraryIndex::default()
            });
        };
        let index: LibraryIndex = serde_json::from_slice(&contents)
            .context("failed to parse the rebuildable media-library index")?;
        if index.version != INDEX_VERSION {
            bail!("unsupported media-library index version {}", index.version);
        }
        Ok(index)
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
) -> Result<()> {
    let mut created = false;
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
        created = true;
        Ok(())
    };
    provider
        .coordinate_write(path, &mut accessor)
        .map_err(|error| classify_deadlock_error(path, "write", error))?;
    if !created {
        bail!("File Provider supplied no private media-library directory accessor");
    }
    Ok(())
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
        let text = name
            .to_str()
            .ok_or_else(|| anyhow!("relative path is not UTF-8"))?;
        if text.starts_with(LIBRARY_DIRECTORY) || text.starts_with(".telegram-video-downloader-") {
            bail!("relative path targets a private downloader entry");
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

fn batch_result(manifest: &BatchManifest) -> BatchMoveResult {
    BatchMoveResult {
        id: manifest.preview.id.clone(),
        revision: manifest.preview.revision.clone(),
        status: manifest.status,
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
    let cache = LIVE_PREVIEW_ANCHORS.get_or_init(|| Mutex::new(PreviewAnchorStore::default()));
    let mut cache = cache
        .lock()
        .map_err(|_| anyhow!("preview identity-anchor cache is poisoned"))?;
    let old_size = cache
        .by_preview
        .get(preview_id)
        .map(HashMap::len)
        .unwrap_or(0);
    let new_total = cache
        .file_count
        .saturating_sub(old_size)
        .saturating_add(anchors.len());
    if (!cache.by_preview.contains_key(preview_id) && cache.by_preview.len() >= MAX_PINNED_PREVIEWS)
        || new_total > MAX_PINNED_FILES_TOTAL
    {
        bail!(
            "too many outstanding previews need in-process identity anchors; confirm or restart before creating another preview"
        );
    }
    cache.file_count = new_total;
    if !cache.by_preview.contains_key(preview_id) {
        cache.order.push_back(preview_id.to_string());
    }
    cache.by_preview.insert(preview_id.to_string(), anchors);
    Ok(())
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
    let cache = cache
        .lock()
        .map_err(|_| anyhow!("preview identity-anchor cache is poisoned"))?;
    let anchors = cache.by_preview.get(&persisted.preview.id).ok_or_else(|| {
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

fn release_preview_anchors(preview_id: &str) {
    let Some(cache) = LIVE_PREVIEW_ANCHORS.get() else {
        return;
    };
    let Ok(mut cache) = cache.lock() else {
        return;
    };
    if let Some(anchors) = cache.by_preview.remove(preview_id) {
        cache.file_count = cache.file_count.saturating_sub(anchors.len());
    }
    cache.order.retain(|id| id != preview_id);
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

fn excluded_entry(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return true;
    };
    name == LIBRARY_DIRECTORY
        || name == ".telegram-video-downloader-queue"
        || name == ".telegram-video-downloader-staging"
        || name.starts_with(".telegram-video-downloader-")
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

fn attached_files(
    media: &ScannedFile,
    all_files: &[ScannedFile],
    warnings: &mut Vec<String>,
) -> Result<Vec<String>> {
    let media_path = Path::new(&media.relative_path);
    let parent = media_path.parent().unwrap_or_else(|| Path::new(""));
    let stem = media_path
        .file_stem()
        .and_then(OsStr::to_str)
        .ok_or_else(|| anyhow!("media path has a non-UTF-8 stem"))?;
    let same_stem_media = all_files
        .iter()
        .filter(|candidate| {
            if !is_primary_media(&candidate.relative_path) {
                return false;
            }
            let candidate_path = Path::new(&candidate.relative_path);
            candidate_path.parent().unwrap_or_else(|| Path::new("")) == parent
                && candidate_path.file_stem().and_then(OsStr::to_str) == Some(stem)
        })
        .map(|candidate| candidate.relative_path.as_str())
        .collect::<Vec<_>>();
    if same_stem_media.len() > 1
        && same_stem_media.first().copied() != Some(media.relative_path.as_str())
    {
        warnings.push(format!(
            "sidecars for same-stem media are assigned to the first entry only: {stem}"
        ));
        return Ok(Vec::new());
    }
    let mut attachments = all_files
        .iter()
        .filter(|candidate| {
            candidate.relative_path != media.relative_path && is_sidecar(&candidate.relative_path)
        })
        .filter(|candidate| {
            let candidate_path = Path::new(&candidate.relative_path);
            if candidate_path.parent().unwrap_or_else(|| Path::new("")) != parent {
                return false;
            }
            let Some(candidate_stem) = candidate_path.file_stem().and_then(OsStr::to_str) else {
                return false;
            };
            candidate_stem == stem
                || candidate_stem
                    .strip_prefix(stem)
                    .is_some_and(|suffix| suffix.starts_with('.'))
        })
        .map(|candidate| candidate.relative_path.clone())
        .collect::<Vec<_>>();
    attachments.sort();
    Ok(attachments)
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
        let canonical = if value.len() >= 8
            && value.starts_with("BV")
            && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            value.to_string()
        } else {
            let lower = value.to_ascii_lowercase();
            if ["av", "cid", "ep"].iter().any(|prefix| {
                lower.strip_prefix(prefix).is_some_and(|digits| {
                    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
                })
            }) {
                lower
            } else {
                continue;
            }
        };
        normalized.insert(canonical);
    }
    normalized.into_iter().collect()
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
        let raw_id = xml_unescape(content[end + 1..close].trim());
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
            .is_some_and(|byte| !byte.is_ascii_whitespace() && *byte != b'>')
        {
            search_from = after_name;
            continue;
        }
        let open_end = content[after_name..].find('>')? + after_name;
        let close_marker = format!("</{name}>");
        let close = find_ascii_case_insensitive(content, &close_marker, open_end + 1)?;
        let value = strip_xml_markup(&content[open_end + 1..close]);
        return Some(xml_unescape(value.trim()));
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

fn metadata_field_changes(before: Option<&str>, after: &str) -> Vec<MetadataFieldChange> {
    [
        ("title", "title"),
        ("collection", "set"),
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
    let open_end = loop {
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
        break end;
    };
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
    collect_metadata_json(value, &mut title, &mut ids, &mut collection);
    (title, normalize_source_ids(ids), collection)
}

fn collect_metadata_json(
    value: &serde_json::Value,
    title: &mut Option<String>,
    ids: &mut Vec<String>,
    collection: &mut Option<CollectionMetadata>,
) {
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                let key_lower = key.to_ascii_lowercase();
                match (key_lower.as_str(), value) {
                    ("bvid", serde_json::Value::String(value)) => ids.push(value.clone()),
                    ("aid" | "avid", serde_json::Value::Number(value)) => {
                        ids.push(format!("av{value}"))
                    }
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
                        if let Some(season_title) =
                            season_title.filter(|title| !title.trim().is_empty())
                        {
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
                collect_metadata_json(value, title, ids, collection);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_metadata_json(value, title, ids, collection);
            }
        }
        _ => {}
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

fn matching_hints_for_item<'a>(
    relative_path: &str,
    source_ids: &[String],
    hints: &'a [SourceHint],
) -> Vec<&'a SourceHint> {
    let item_ids = source_ids.iter().cloned().collect::<HashSet<_>>();
    hints
        .iter()
        .filter(|hint| {
            if hint.relative_path.as_deref() == Some(relative_path) {
                return true;
            }
            let hint_ids = normalize_source_ids(hint.source_ids.clone())
                .into_iter()
                .collect::<HashSet<_>>();
            if hint_ids.is_empty() || item_ids.is_empty() {
                return false;
            }
            let hint_cids = hint_ids
                .iter()
                .filter(|id| id.starts_with("cid"))
                .collect::<HashSet<_>>();
            let item_cids = item_ids
                .iter()
                .filter(|id| id.starts_with("cid"))
                .collect::<HashSet<_>>();
            if !hint_cids.is_empty() || !item_cids.is_empty() {
                return hint_cids.iter().any(|cid| item_cids.contains(cid));
            }
            hint_ids.iter().any(|id| item_ids.contains(id))
        })
        .collect()
}

fn attach_matching_hints(items: &mut [LibraryItem], hints: &[SourceHint]) {
    for item in items {
        for hint in matching_hints_for_item(&item.relative_path, &item.source_ids, hints) {
            item.source_ids.extend(hint.source_ids.iter().cloned());
            if item.title == title_from_path(&item.relative_path)
                && let Some(title) = hint
                    .title
                    .as_deref()
                    .filter(|title| !title.trim().is_empty())
            {
                item.title = title.trim().to_string();
            }
            item.collection = item.collection.clone().or_else(|| hint.collection.clone());
            if hint.requires_confirmation {
                item.confidence = Confidence::NeedsConfirmation;
            } else if item.confidence == Confidence::Low {
                item.confidence = Confidence::Medium;
            }
            item.metadata_status = MetadataStatus::Partial;
        }
        item.source_ids = normalize_source_ids(std::mem::take(&mut item.source_ids));
    }
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
                id: None,
                title: "Series & More".to_string(),
                kind: "user".to_string(),
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
        assert!(patched.contains("<episode>2</episode>"));
        assert!(patched.contains("<partid>Part-A</partid>"));
        assert_eq!(
            fs::metadata(root.path().join("clip.nfo")).unwrap().mode() & 0o777,
            0o600,
            "confirmed NFO patch must preserve private file permissions"
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
    }
}
