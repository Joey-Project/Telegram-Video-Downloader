use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;

use crate::safe_fs::{EntryIdentity, RootedFs};

pub const MAX_MEDIA_LIBRARIES: usize = 16;
const MAX_REGISTRY_BYTES: usize = 64 * 1024;
const MAX_RESOLVED_MIGRATION_DEFAULTS: usize = 10_000;
const MAX_MIGRATION_DEFAULT_ARCHIVE_BYTES: usize = 4 * 1024 * 1024;
const REGISTRY_FILE: &str = ".telegram-video-downloader-media-libraries.json";
const REGISTRY_TEMP_PREFIX: &str = ".telegram-video-downloader-media-libraries.tmp";
const DEFAULT_ARCHIVE_FILE: &str = ".telegram-video-downloader-media-library-defaults.json";
const DEFAULT_ARCHIVE_TEMP_PREFIX: &str = ".telegram-video-downloader-media-defaults.tmp";
static REGISTRY_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaLibrary {
    pub id: String,
    pub name: String,
    pub root: PathBuf,
    #[serde(default)]
    pub default_category: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    root_device: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    root_inode: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaLibrarySnapshot {
    pub revision: u64,
    pub default_library_id: String,
    #[serde(default)]
    pub default_category: Option<PathBuf>,
    pub libraries: Vec<MediaLibrary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationDefaultUpdateOutcome {
    Applied { snapshot: MediaLibrarySnapshot },
    RevisionChanged { snapshot: MediaLibrarySnapshot },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskDestination {
    pub library_id: String,
    pub root: PathBuf,
    #[serde(default)]
    pub category: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskDestinationTransfer {
    pub transfer_id: String,
    #[serde(default)]
    pub migration_id: String,
    pub task_id: String,
    pub started_generation: u64,
    pub generation: u64,
    pub revision: u64,
    pub source: TaskDestination,
    pub target: TaskDestination,
    pub previous_status: crate::queue::TaskStatus,
    #[serde(default)]
    pub previous_error: Option<String>,
    #[serde(default)]
    pub previous_cancel_requested: bool,
    #[serde(default)]
    pub pause_generation: Option<u64>,
    #[serde(default)]
    pub plan_fingerprint: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskDestinationTransferPhase {
    Copying,
    TargetInstalled,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TaskTransferSnapshot {
    pub record: crate::queue::TaskRecord,
    pub destination: TaskDestination,
    pub staging_bytes: u64,
    pub staging_file_count: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskTransferEntryKind {
    Directory,
    File,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskTransferFile {
    pub source_relative: PathBuf,
    pub target_relative: PathBuf,
    pub kind: TaskTransferEntryKind,
    pub device: u64,
    pub inode: u64,
    pub size_bytes: u64,
    pub owner: u32,
    pub group: u32,
    pub mode: u32,
    #[serde(default)]
    pub link_count: Option<u64>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub chunk_sha256: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskTransferControlKind {
    Owner,
    Retention,
    PublicationManifest,
    WorkerRequest,
    WorkerLifecycle,
    WorkerLifecycleTransition,
    BbdownConfig,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskTransferControl {
    pub source_relative: PathBuf,
    pub target_relative: PathBuf,
    pub kind: TaskTransferControlKind,
    pub device: u64,
    pub inode: u64,
    pub size_bytes: u64,
    pub owner: u32,
    pub group: u32,
    pub mode: u32,
    #[serde(default)]
    pub link_count: Option<u64>,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StagingMigrationControlReceipt {
    pub kind: TaskTransferControlKind,
    pub relative_path: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub size_bytes: u64,
    pub owner: u32,
    pub group: u32,
    pub mode: u32,
    pub link_count: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskTransferPlan {
    pub transfer: TaskDestinationTransfer,
    pub source_root: PathBuf,
    pub target_root: PathBuf,
    pub queue_record_source: PathBuf,
    pub queue_record_target: PathBuf,
    pub files: Vec<TaskTransferFile>,
    #[serde(default)]
    pub controls: Vec<TaskTransferControl>,
    #[serde(default)]
    pub published_path_rewrites: std::collections::BTreeMap<PathBuf, PathBuf>,
    pub total_bytes: u64,
    pub fingerprint: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaLibraryStatus {
    pub available: bool,
    pub available_space_bytes: Option<u64>,
    #[serde(default)]
    pub diagnostic: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RegistryDocument {
    version: u32,
    revision: u64,
    default_library_id: String,
    libraries: Vec<MediaLibrary>,
    #[serde(default)]
    applied_migration_defaults: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    skipped_migration_defaults: Vec<String>,
    #[serde(default)]
    archived_migration_default_count: usize,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    archived_migration_default_sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MigrationDefaultResolution {
    Applied,
    RevisionChanged,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct MigrationDefaultArchiveEntry {
    migration_id: String,
    outcome: MigrationDefaultResolution,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct MigrationDefaultArchiveDocument {
    version: u32,
    entries: Vec<MigrationDefaultArchiveEntry>,
}

struct LoadedMigrationDefaultArchive {
    entries: Vec<MigrationDefaultArchiveEntry>,
    bytes: Option<Vec<u8>>,
    identity: Option<EntryIdentity>,
}

pub(crate) struct MediaLibraryRegistryStore {
    fs: RootedFs,
    path: PathBuf,
    snapshot: MediaLibrarySnapshot,
    applied_migration_defaults: Vec<String>,
    skipped_migration_defaults: Vec<String>,
    archived_migration_default_count: usize,
    archived_migration_default_sha256: String,
    archived_migration_defaults: Vec<MigrationDefaultArchiveEntry>,
    archived_migration_default_bytes: Option<Vec<u8>>,
    archived_migration_default_identity: Option<EntryIdentity>,
}

impl MediaLibraryRegistryStore {
    pub(crate) fn open(config_path: Option<&Path>, configured_video_root: &Path) -> Result<Self> {
        let registry_path = registry_path(config_path, configured_video_root)?;
        let parent = registry_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or_else(|| anyhow!("media library settings path has no parent"))?;
        let rooted = RootedFs::new(parent).with_context(|| {
            format!(
                "failed to bind media library settings directory {}",
                parent.display()
            )
        })?;
        let path = rooted.logical_root_path().join(
            registry_path
                .file_name()
                .ok_or_else(|| anyhow!("media library settings path has no filename"))?,
        );

        let (
            snapshot,
            applied_migration_defaults,
            skipped_migration_defaults,
            archived_migration_default_count,
            archived_migration_default_sha256,
            archived_migration_defaults,
            archived_migration_default_bytes,
            archived_migration_default_identity,
        ) = if let Some(bytes) = read_private_registry(&rooted, &path)? {
            let document: RegistryDocument = serde_json::from_slice(&bytes)
                .context("invalid media library settings document")?;
            if document.version != 1 {
                bail!(
                    "unsupported media library settings version {}",
                    document.version
                );
            }
            let snapshot = snapshot_from_document(&document)?;
            validate_migration_id_history(
                &document.applied_migration_defaults,
                &document.skipped_migration_defaults,
            )?;
            let archive = load_migration_default_archive(&rooted, &path, &document)?;
            (
                snapshot,
                document.applied_migration_defaults,
                document.skipped_migration_defaults,
                document.archived_migration_default_count,
                document.archived_migration_default_sha256,
                archive.entries,
                archive.bytes,
                archive.identity,
            )
        } else {
            let archive_path = migration_default_archive_path(&path)?;
            if rooted.open_bound_file(&archive_path)?.is_some() {
                bail!("media library migration archive exists without its registry");
            }
            let library = library_from_existing_root("Default", configured_video_root, None)?;
            let snapshot = MediaLibrarySnapshot {
                revision: 1,
                default_library_id: library.id.clone(),
                default_category: None,
                libraries: vec![library],
            };
            validate_registry_shape(&snapshot)?;
            write_private_registry(&rooted, &path, &snapshot, &[], &[], 0, "")?;
            (
                snapshot,
                Vec::new(),
                Vec::new(),
                0,
                String::new(),
                Vec::new(),
                None,
                None,
            )
        };

        Ok(Self {
            fs: rooted,
            path,
            snapshot,
            applied_migration_defaults,
            skipped_migration_defaults,
            archived_migration_default_count,
            archived_migration_default_sha256,
            archived_migration_defaults,
            archived_migration_default_bytes,
            archived_migration_default_identity,
        })
    }

    pub(crate) fn snapshot(&self) -> MediaLibrarySnapshot {
        self.snapshot.clone()
    }

    #[cfg(test)]
    pub(crate) fn registry_path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn default_destination(&self) -> Result<TaskDestination> {
        destination_for_library(
            &self.snapshot,
            &self.snapshot.default_library_id,
            self.snapshot.default_category.as_deref(),
        )
    }

    pub(crate) fn library(&self, id: &str) -> Option<&MediaLibrary> {
        self.snapshot
            .libraries
            .iter()
            .find(|library| library.id == id)
    }

    pub(crate) fn root_binding(&self, id: &str) -> Option<(u64, u64)> {
        let library = self.library(id)?;
        Some((library.root_device?, library.root_inode?))
    }

    /// Adopt a previously unbound legacy root after QueueManager has opened and validated its
    /// coordinated directory and queue state. A recorded binding is never silently replaced.
    pub(crate) fn bind_root_identity(&mut self, id: &str, device: u64, inode: u64) -> Result<()> {
        let library = self
            .library(id)
            .ok_or_else(|| anyhow!("unknown media library {id}"))?;
        match (library.root_device, library.root_inode) {
            (Some(existing_device), Some(existing_inode))
                if (existing_device, existing_inode) == (device, inode) =>
            {
                return Ok(());
            }
            (Some(_), Some(_)) => {
                bail!("media library root object identity changed; it remains unavailable")
            }
            (Some(_), None) | (None, Some(_)) => {
                bail!("media library root identity binding is incomplete")
            }
            (None, None) => {}
        }

        let mut next = self.snapshot.clone();
        let library = next
            .libraries
            .iter_mut()
            .find(|library| library.id == id)
            .expect("library was found above");
        library.root_device = Some(device);
        library.root_inode = Some(inode);
        validate_registry_shape(&next)?;
        self.write_registry_snapshot(&next, None)?;
        self.snapshot = next;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn add(
        &mut self,
        name: &str,
        root: &Path,
        default_category: Option<&Path>,
        set_default: bool,
        create_directory: bool,
    ) -> Result<MediaLibrary> {
        let name = validate_name(name)?;
        let category = validate_category(default_category)?;
        self.preflight_add(&name, root, category.as_deref(), create_directory)?;
        let canonical_root = prepare_library_root(root, create_directory)?;
        let root_id = RootedFs::new(&canonical_root)?.root_identity();

        self.add_prepared_root(&name, canonical_root, category, set_default, root_id)
    }

    /// Register a root whose directory descriptor and queue ownership were already prepared by
    /// `QueueManager`. The persisted binding comes from that descriptor; a final pathname check
    /// rejects replacement before the single registry commit that may also change the default.
    pub(crate) fn add_with_root_identity(
        &mut self,
        name: &str,
        root: &Path,
        default_category: Option<&Path>,
        set_default: bool,
        opened_identity: EntryIdentity,
    ) -> Result<MediaLibrary> {
        let name = validate_name(name)?;
        let category = validate_category(default_category)?;
        self.preflight_add(&name, root, category.as_deref(), false)?;
        let canonical_root = prepare_library_root(root, false)?;
        self.add_prepared_root(
            &name,
            canonical_root,
            category,
            set_default,
            opened_identity,
        )
    }

    fn add_prepared_root(
        &mut self,
        name: &str,
        canonical_root: PathBuf,
        category: Option<PathBuf>,
        set_default: bool,
        root_id: EntryIdentity,
    ) -> Result<MediaLibrary> {
        let name = name.to_string();

        if let Some(existing) = self
            .snapshot
            .libraries
            .iter()
            .find(|library| {
                canonicalize_if_available(&library.root)
                    .is_ok_and(|existing_root| existing_root == canonical_root)
            })
            .cloned()
        {
            match (existing.root_device, existing.root_inode) {
                (Some(device), Some(inode))
                    if (device, inode) == (root_id.device(), root_id.inode()) => {}
                (Some(_), Some(_)) => {
                    bail!("registered media library path now names a different root object");
                }
                (None, None) => {}
                (Some(_), None) | (None, Some(_)) => {
                    bail!("registered media library root identity binding is incomplete");
                }
            }
            if existing.name != name || existing.default_category != category {
                bail!(
                    "media library root is already registered as {}",
                    existing.name
                );
            }

            self.ensure_root_path_matches_identity(&canonical_root, root_id)?;
            let mut next = self.snapshot.clone();
            let registered = next
                .libraries
                .iter_mut()
                .find(|library| library.id == existing.id)
                .expect("existing library remains registered");
            registered.root_device = Some(root_id.device());
            registered.root_inode = Some(root_id.inode());
            let default_changed = set_default
                && (next.default_library_id != existing.id
                    || next.default_category != existing.default_category);
            let binding_changed = (existing.root_device, existing.root_inode)
                != (Some(root_id.device()), Some(root_id.inode()));
            if !binding_changed && !default_changed {
                return Ok(existing);
            }
            if set_default {
                next.default_library_id = existing.id.clone();
                next.default_category = existing.default_category.clone();
            }
            if default_changed {
                next.revision = self
                    .snapshot
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("media library settings revision overflow"))?;
            }
            validate_registry_shape(&next)?;
            self.write_registry_snapshot(&next, Some((&canonical_root, root_id)))?;
            self.snapshot = next;
            return Ok(self
                .library(&existing.id)
                .expect("existing library remains registered")
                .clone());
        }

        if self.snapshot.libraries.len() >= MAX_MEDIA_LIBRARIES {
            bail!("at most {MAX_MEDIA_LIBRARIES} media libraries may be registered");
        }
        if self
            .snapshot
            .libraries
            .iter()
            .any(|library| library.name == name)
        {
            bail!("a media library named {name:?} is already registered");
        }
        for library in &self.snapshot.libraries {
            let existing_path = registered_root_overlap_path(&library.root)?;
            if canonical_root != existing_path && roots_overlap(&canonical_root, &existing_path) {
                bail!(
                    "media library roots may not be equal, nested, or overlapping: {} and {}",
                    canonical_root.display(),
                    existing_path.display()
                );
            }
            if library
                .root_device
                .zip(library.root_inode)
                .is_some_and(|binding| binding == (root_id.device(), root_id.inode()))
                || RootedFs::new(&existing_path)
                    .is_ok_and(|existing| existing.root_identity() == root_id)
            {
                bail!("media library path aliases an already registered root");
            }
        }

        validate_category_under_root(&canonical_root, category.as_deref())?;
        let library = MediaLibrary {
            id: library_id_for_root(&canonical_root),
            name,
            root: canonical_root,
            default_category: category,
            root_device: Some(root_id.device()),
            root_inode: Some(root_id.inode()),
        };
        if self.library(&library.id).is_some() {
            bail!("media library identifier collision");
        }
        let mut next = self.snapshot.clone();
        next.libraries.push(library.clone());
        if set_default {
            next.default_library_id = library.id.clone();
            next.default_category = library.default_category.clone();
        }
        self.commit_with_root_identity(next, &library.root, root_id)?;
        Ok(library)
    }

    fn ensure_root_path_matches_identity(
        &self,
        canonical_root: &Path,
        expected: EntryIdentity,
    ) -> Result<()> {
        let current = RootedFs::new(canonical_root)?.root_identity();
        if current != expected {
            bail!("media library root identity changed during registration");
        }
        Ok(())
    }

    /// Validate a registration request before a caller creates the requested directory or
    /// initializes any queue state beneath it. Offline libraries remain overlap boundaries by
    /// their stored absolute path, and their durable device/inode binding prevents re-registering
    /// the same directory object at a relocated path.
    pub(crate) fn preflight_add(
        &self,
        name: &str,
        root: &Path,
        default_category: Option<&Path>,
        create_directory: bool,
    ) -> Result<PathBuf> {
        let _ = validate_name(name)?;
        let _ = validate_category(default_category)?;
        if !root.is_absolute() {
            bail!("media library path must be absolute");
        }
        let requested = normalize_trusted_macos_alias(root)?;
        reject_symlink_components(&requested, !create_directory)?;
        let requested_for_overlap =
            fs::canonicalize(&requested).unwrap_or_else(|_| requested.clone());
        for library in &self.snapshot.libraries {
            let existing = registered_root_overlap_path(&library.root)?;
            if requested_for_overlap != existing && roots_overlap(&requested_for_overlap, &existing)
            {
                bail!(
                    "media library roots may not be equal, nested, or overlapping: {} and {}",
                    requested_for_overlap.display(),
                    existing.display()
                );
            }
        }
        Ok(requested_for_overlap)
    }

    pub(crate) fn set_default(&mut self, id: &str) -> Result<MediaLibrarySnapshot> {
        let library = self
            .library(id)
            .ok_or_else(|| anyhow!("unknown media library {id}"))?;
        validate_registered_root_for_use(&library.root)?;
        if self.snapshot.default_library_id == id {
            return Ok(self.snapshot());
        }
        let mut next = self.snapshot.clone();
        next.default_library_id = id.to_string();
        next.default_category = library.default_category.clone();
        self.commit(next)
    }

    /// Resolve a previously completed migration default update without applying it again.
    /// A replay must preserve the outcome even if a user has since changed the default.
    pub(crate) fn resolved_migration_default(
        &self,
        migration_id: &str,
    ) -> Result<Option<MigrationDefaultUpdateOutcome>> {
        validate_migration_id(migration_id)?;
        self.validate_migration_default_archive_on_disk()?;
        let applied_inline = self
            .applied_migration_defaults
            .iter()
            .any(|applied| applied == migration_id);
        let skipped_inline = self
            .skipped_migration_defaults
            .iter()
            .any(|skipped| skipped == migration_id);
        let inline = match (applied_inline, skipped_inline) {
            (true, true) => bail!("media library migration id has conflicting resolutions"),
            (true, false) => Some(MigrationDefaultResolution::Applied),
            (false, true) => Some(MigrationDefaultResolution::RevisionChanged),
            (false, false) => None,
        };
        let archived = self
            .archived_migration_defaults
            .binary_search_by(|entry| entry.migration_id.as_str().cmp(migration_id))
            .ok()
            .map(|index| self.archived_migration_defaults[index].outcome);
        let outcome = match (inline, archived) {
            (Some(inline), Some(archived)) if inline != archived => {
                bail!("media library migration archive conflicts with inline history");
            }
            (Some(outcome), _) | (None, Some(outcome)) => Some(outcome),
            (None, None) => None,
        };
        Ok(outcome.map(|outcome| match outcome {
            MigrationDefaultResolution::Applied => MigrationDefaultUpdateOutcome::Applied {
                snapshot: self.snapshot(),
            },
            MigrationDefaultResolution::RevisionChanged => {
                MigrationDefaultUpdateOutcome::RevisionChanged {
                    snapshot: self.snapshot(),
                }
            }
        }))
    }

    pub(crate) fn set_default_for_migration_if_revision(
        &mut self,
        id: &str,
        migration_id: &str,
        expected_revision: u64,
    ) -> Result<MigrationDefaultUpdateOutcome> {
        if let Some(outcome) = self.resolved_migration_default(migration_id)? {
            return Ok(outcome);
        }
        if self.migration_default_history_len().saturating_add(1) > MAX_RESOLVED_MIGRATION_DEFAULTS
        {
            bail!("media library migration idempotency history is full");
        }

        let mut next = self.snapshot.clone();
        let resolution = if self.snapshot.revision != expected_revision {
            MigrationDefaultResolution::RevisionChanged
        } else {
            let library = self
                .library(id)
                .ok_or_else(|| anyhow!("unknown media library {id}"))?;
            validate_registered_root_for_use(&library.root)?;
            next.default_library_id = library.id.clone();
            next.default_category = library.default_category.clone();
            if next.default_library_id != self.snapshot.default_library_id
                || next.default_category != self.snapshot.default_category
            {
                next.revision = self
                    .snapshot
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("media library settings revision overflow"))?;
            }
            MigrationDefaultResolution::Applied
        };

        let mut prior_inline = self
            .applied_migration_defaults
            .iter()
            .map(|migration_id| MigrationDefaultArchiveEntry {
                migration_id: migration_id.clone(),
                outcome: MigrationDefaultResolution::Applied,
            })
            .chain(self.skipped_migration_defaults.iter().map(|migration_id| {
                MigrationDefaultArchiveEntry {
                    migration_id: migration_id.clone(),
                    outcome: MigrationDefaultResolution::RevisionChanged,
                }
            }))
            .collect::<Vec<_>>();
        prior_inline.sort_by(|left, right| left.migration_id.cmp(&right.migration_id));
        let mut archived = self.archived_migration_defaults.clone();
        for entry in prior_inline {
            match archived
                .binary_search_by(|existing| existing.migration_id.cmp(&entry.migration_id))
            {
                Ok(index) if archived[index].outcome == entry.outcome => {}
                Ok(_) => bail!("media library migration archive conflicts with inline history"),
                Err(index) => archived.insert(index, entry),
            }
        }
        let archived_count = archived.len();
        validate_migration_default_count(archived_count, 1)?;
        let archive_bytes = if archived.is_empty() {
            None
        } else {
            Some(encode_migration_default_archive(&archived)?)
        };
        let archive_sha256 = archive_bytes.as_deref().map(sha256_hex).unwrap_or_default();
        let applied = if resolution == MigrationDefaultResolution::Applied {
            vec![migration_id.to_string()]
        } else {
            Vec::new()
        };
        let skipped = if resolution == MigrationDefaultResolution::RevisionChanged {
            vec![migration_id.to_string()]
        } else {
            Vec::new()
        };
        validate_registry_shape(&next)?;
        let registry_bytes =
            encode_private_registry(&next, &applied, &skipped, archived_count, &archive_sha256)?;

        // Archive prior exact outcomes first. If the main settings replacement is interrupted,
        // the still-inline entries let open() verify and recognize this duplicate archive tail.
        if let Some(archive_bytes) = archive_bytes {
            self.write_migration_default_archive(archived, archive_bytes)?;
        } else {
            self.archived_migration_defaults = archived;
        }
        self.validate_migration_default_archive_on_disk()?;
        write_private_registry_bytes(&self.fs, &self.path, &registry_bytes)?;
        self.snapshot = next;
        self.applied_migration_defaults = applied;
        self.skipped_migration_defaults = skipped;
        self.archived_migration_default_count = archived_count;
        self.archived_migration_default_sha256 = archive_sha256;
        Ok(match resolution {
            MigrationDefaultResolution::Applied => MigrationDefaultUpdateOutcome::Applied {
                snapshot: self.snapshot(),
            },
            MigrationDefaultResolution::RevisionChanged => {
                MigrationDefaultUpdateOutcome::RevisionChanged {
                    snapshot: self.snapshot(),
                }
            }
        })
    }

    fn migration_default_history_len(&self) -> usize {
        self.archived_migration_default_count
            .saturating_add(self.applied_migration_defaults.len())
            .saturating_add(self.skipped_migration_defaults.len())
    }

    fn write_migration_default_archive(
        &mut self,
        entries: Vec<MigrationDefaultArchiveEntry>,
        bytes: Vec<u8>,
    ) -> Result<()> {
        self.validate_migration_default_archive_on_disk()?;
        if self.archived_migration_default_bytes.as_deref() == Some(bytes.as_slice()) {
            self.archived_migration_defaults = entries;
            return Ok(());
        }
        let archive_path = migration_default_archive_path(&self.path)?;
        write_private_migration_archive(
            &self.fs,
            &archive_path,
            self.archived_migration_default_identity,
            &bytes,
        )?;
        let file = self
            .fs
            .open_bound_file(&archive_path)?
            .ok_or_else(|| anyhow!("migration default archive disappeared after writing"))?;
        file.validate_private_single_link(0o600)
            .context("media library migration archive must be private and single-linked")?;
        if file.read_limited(MAX_MIGRATION_DEFAULT_ARCHIVE_BYTES)? != bytes {
            bail!("media library migration archive changed after writing");
        }
        self.archived_migration_default_identity = Some(file.identity());
        self.archived_migration_default_bytes = Some(bytes);
        self.archived_migration_defaults = entries;
        Ok(())
    }

    fn validate_migration_default_archive_on_disk(&self) -> Result<()> {
        let path = migration_default_archive_path(&self.path)?;
        match (
            self.fs.open_bound_file(&path)?,
            self.archived_migration_default_identity,
            self.archived_migration_default_bytes.as_deref(),
        ) {
            (None, None, None) => Ok(()),
            (Some(file), Some(expected_identity), Some(expected_bytes)) => {
                file.validate_private_single_link(0o600)
                    .context("media library migration archive must be private and single-linked")?;
                if file.identity() != expected_identity {
                    bail!("media library migration archive identity changed");
                }
                if file.read_limited(MAX_MIGRATION_DEFAULT_ARCHIVE_BYTES)? != expected_bytes {
                    bail!("media library migration archive content changed");
                }
                Ok(())
            }
            _ => bail!("media library migration archive no longer matches its verified state"),
        }
    }

    pub(crate) fn set_category(
        &mut self,
        id: &str,
        category: Option<&Path>,
    ) -> Result<MediaLibrarySnapshot> {
        let library = self
            .library(id)
            .ok_or_else(|| anyhow!("unknown media library {id}"))?;
        let root = library.root.clone();
        let category = validate_category(category)?;
        validate_registered_root_for_use(&root)?;
        validate_category_under_root(&root, category.as_deref())?;
        if library.default_category == category {
            return Ok(self.snapshot());
        }
        let mut next = self.snapshot.clone();
        let target = next
            .libraries
            .iter_mut()
            .find(|library| library.id == id)
            .expect("validated media library remains registered");
        target.default_category = category.clone();
        if next.default_library_id == id {
            next.default_category = category;
        }
        self.commit(next)
    }

    pub(crate) fn rename(&mut self, id: &str, name: &str) -> Result<MediaLibrarySnapshot> {
        let name = validate_name(name)?;
        if self.library(id).is_none() {
            bail!("unknown media library {id}");
        }
        if self
            .snapshot
            .libraries
            .iter()
            .any(|library| library.id != id && library.name == name)
        {
            bail!("a media library named {name:?} is already registered");
        }
        if self.library(id).is_some_and(|library| library.name == name) {
            return Ok(self.snapshot());
        }
        let mut next = self.snapshot.clone();
        next.libraries
            .iter_mut()
            .find(|library| library.id == id)
            .expect("validated media library remains registered")
            .name = name;
        self.commit(next)
    }

    pub(crate) fn remove(&mut self, id: &str) -> Result<MediaLibrarySnapshot> {
        if self.library(id).is_none() {
            return Ok(self.snapshot());
        }
        if self.snapshot.default_library_id == id {
            bail!("set another media library as default before removing this one");
        }
        let mut next = self.snapshot.clone();
        next.libraries.retain(|library| library.id != id);
        self.commit(next)
    }

    fn commit(&mut self, mut next: MediaLibrarySnapshot) -> Result<MediaLibrarySnapshot> {
        self.commit_inner(&mut next, None)?;
        self.snapshot = next;
        Ok(self.snapshot())
    }

    fn commit_with_root_identity(
        &mut self,
        mut next: MediaLibrarySnapshot,
        root_path: &Path,
        root_identity: EntryIdentity,
    ) -> Result<MediaLibrarySnapshot> {
        self.commit_inner(&mut next, Some((root_path, root_identity)))?;
        self.snapshot = next;
        Ok(self.snapshot())
    }

    fn commit_inner(
        &self,
        next: &mut MediaLibrarySnapshot,
        root_identity: Option<(&Path, EntryIdentity)>,
    ) -> Result<()> {
        next.revision = self
            .snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow!("media library settings revision overflow"))?;
        validate_registry_shape(next)?;
        self.write_registry_snapshot(next, root_identity)
    }

    fn write_registry_snapshot(
        &self,
        snapshot: &MediaLibrarySnapshot,
        root_identity: Option<(&Path, EntryIdentity)>,
    ) -> Result<()> {
        self.validate_migration_default_archive_on_disk()?;
        let bytes = encode_private_registry(
            snapshot,
            &self.applied_migration_defaults,
            &self.skipped_migration_defaults,
            self.archived_migration_default_count,
            &self.archived_migration_default_sha256,
        )?;
        if let Some((root_path, identity)) = root_identity {
            // This is deliberately the last pathname-based operation before the registry's
            // atomic replacement. Persisted identity always comes from the prepared directory,
            // never from a fresh identity read here.
            self.ensure_root_path_matches_identity(root_path, identity)?;
        }
        write_private_registry_bytes(&self.fs, &self.path, &bytes)
    }
}

pub(crate) fn validate_task_destination(
    snapshot: &MediaLibrarySnapshot,
    destination: &TaskDestination,
) -> Result<()> {
    let library = snapshot
        .libraries
        .iter()
        .find(|library| library.id == destination.library_id)
        .ok_or_else(|| anyhow!("task destination refers to an unregistered media library"))?;
    if library.root != destination.root {
        bail!("task destination root does not match its registered media library");
    }
    let category = validate_category(destination.category.as_deref())?;
    if category != destination.category {
        bail!("task destination category is not normalized");
    }
    validate_registered_root_for_use(&library.root)?;
    validate_category_under_root(&library.root, category.as_deref())
}

pub(crate) fn destination_for_library(
    snapshot: &MediaLibrarySnapshot,
    library_id: &str,
    category: Option<&Path>,
) -> Result<TaskDestination> {
    let library = snapshot
        .libraries
        .iter()
        .find(|library| library.id == library_id)
        .ok_or_else(|| anyhow!("unknown media library {library_id}"))?;
    validate_registered_root_for_use(&library.root)?;
    let category = match category {
        Some(category) => validate_category(Some(category))?,
        None => library.default_category.clone(),
    };
    validate_category_under_root(&library.root, category.as_deref())?;
    Ok(TaskDestination {
        library_id: library.id.clone(),
        root: library.root.clone(),
        category,
    })
}

pub(crate) fn registry_path(
    config_path: Option<&Path>,
    configured_video_root: &Path,
) -> Result<PathBuf> {
    let parent = if let Some(config_path) = config_path {
        config_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or_else(|| anyhow!("loaded config path has no parent"))?
    } else {
        configured_video_root
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(configured_video_root)
    };
    Ok(normalize_trusted_macos_alias(parent)?.join(REGISTRY_FILE))
}

fn snapshot_from_document(document: &RegistryDocument) -> Result<MediaLibrarySnapshot> {
    let default_category = document
        .libraries
        .iter()
        .find(|library| library.id == document.default_library_id)
        .ok_or_else(|| anyhow!("media library settings has no valid default library"))?
        .default_category
        .clone();
    let snapshot = MediaLibrarySnapshot {
        revision: document.revision,
        default_library_id: document.default_library_id.clone(),
        default_category,
        libraries: document.libraries.clone(),
    };
    validate_registry_shape(&snapshot)?;
    Ok(snapshot)
}

fn validate_registry_shape(snapshot: &MediaLibrarySnapshot) -> Result<()> {
    if snapshot.libraries.is_empty() || snapshot.libraries.len() > MAX_MEDIA_LIBRARIES {
        bail!("media library settings must contain between 1 and {MAX_MEDIA_LIBRARIES} libraries");
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut names = std::collections::BTreeSet::new();
    for library in &snapshot.libraries {
        if library.id.trim().is_empty() || !ids.insert(library.id.as_str()) {
            bail!("media library settings contains an empty or duplicate identifier");
        }
        if library.root_device.is_some() != library.root_inode.is_some() {
            bail!("media library settings contains an incomplete root identity binding");
        }
        validate_name(&library.name)?;
        if !names.insert(library.name.as_str()) {
            bail!("media library names must be unique");
        }
        if !library.root.is_absolute() {
            bail!("registered media library root must be absolute");
        }
        let normalized = validate_category(library.default_category.as_deref())?;
        if normalized != library.default_category {
            bail!("registered media library category is not normalized");
        }
    }
    let default = snapshot
        .libraries
        .iter()
        .find(|library| library.id == snapshot.default_library_id)
        .ok_or_else(|| anyhow!("media library settings default is not registered"))?;
    if default.default_category != snapshot.default_category {
        bail!("media library settings default category does not match its default library");
    }
    Ok(())
}

fn validate_migration_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("migration id must contain 1 to 128 ASCII letters, digits, '-' or '_'");
    }
    Ok(())
}

fn validate_migration_ids(ids: &[String]) -> Result<()> {
    if ids.len() > MAX_RESOLVED_MIGRATION_DEFAULTS {
        bail!("media library migration idempotency history exceeds its limit");
    }
    let mut unique = std::collections::BTreeSet::new();
    for id in ids {
        validate_migration_id(id)?;
        if !unique.insert(id) {
            bail!("media library settings has duplicate migration ids");
        }
    }
    Ok(())
}

fn validate_migration_id_history(applied: &[String], skipped: &[String]) -> Result<()> {
    validate_migration_ids(applied)?;
    validate_migration_ids(skipped)?;
    if applied.len() + skipped.len() > MAX_RESOLVED_MIGRATION_DEFAULTS {
        bail!("media library migration idempotency history exceeds its limit");
    }
    let applied_ids = applied.iter().collect::<std::collections::BTreeSet<_>>();
    if skipped.iter().any(|id| applied_ids.contains(id)) {
        bail!("media library settings has conflicting migration resolutions");
    }
    Ok(())
}

fn library_from_existing_root(
    name: &str,
    root: &Path,
    default_category: Option<&Path>,
) -> Result<MediaLibrary> {
    let name = validate_name(name)?;
    if !root.is_absolute() {
        bail!("media library path must be absolute");
    }
    let rooted = RootedFs::new(root)?;
    rooted.validate_configured_root()?;
    // Preserve the configured spelling for verified system aliases such as /var, while
    // grandfathering an existing user-symlink root by adopting its already-resolved object.
    // New registrations still pass through strict component-by-component symlink rejection.
    let normalized_root = normalize_trusted_macos_alias(root)?;
    let trusted_alias =
        normalized_root != root && reject_symlink_components(&normalized_root, true).is_ok();
    let stored_root = if trusted_alias {
        root.to_path_buf()
    } else {
        rooted.root_path().to_path_buf()
    };
    let category = validate_category(default_category)?;
    validate_category_under_root(&stored_root, category.as_deref())?;
    Ok(MediaLibrary {
        id: library_id_for_root(&stored_root),
        name,
        root: stored_root,
        default_category: category,
        // The coordinated queue open adopts the legacy root's inode binding after the
        // File Provider has supplied its authoritative directory descriptor.
        root_device: None,
        root_inode: None,
    })
}

pub(crate) fn prepare_library_root(path: &Path, create_directory: bool) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("media library path must be absolute");
    }
    let path = normalize_trusted_macos_alias(path)?;
    reject_symlink_components(&path, !create_directory)?;
    if create_directory {
        create_directory_tree_safely(&path)?;
    }
    reject_symlink_components(&path, true)?;
    let metadata = fs::metadata(&path)
        .with_context(|| format!("media library directory is unavailable: {}", path.display()))?;
    if !metadata.is_dir() {
        bail!("media library path is not a directory: {}", path.display());
    }
    let canonical = fs::canonicalize(&path)
        .with_context(|| format!("failed to resolve media library path {}", path.display()))?;
    RootedFs::new(&canonical)?.validate_configured_root()?;
    Ok(canonical)
}

fn create_directory_tree_safely(path: &Path) -> Result<()> {
    let mut anchor = path.to_path_buf();
    let mut missing_components = Vec::new();
    loop {
        match fs::symlink_metadata(&anchor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!(
                    "media library path contains a symlink: {}",
                    anchor.display()
                );
            }
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => bail!(
                "media library path component is not a directory: {}",
                anchor.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = anchor.file_name().ok_or_else(|| {
                    anyhow!(
                        "cannot find an existing parent for media library {}",
                        path.display()
                    )
                })?;
                missing_components.push(name.to_os_string());
                anchor = anchor
                    .parent()
                    .ok_or_else(|| {
                        anyhow!(
                            "cannot find an existing parent for media library {}",
                            path.display()
                        )
                    })?
                    .to_path_buf();
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to inspect media library path {}", anchor.display())
                });
            }
        }
    }

    let canonical_anchor = fs::canonicalize(&anchor).with_context(|| {
        format!(
            "failed to resolve media library parent {}",
            anchor.display()
        )
    })?;
    let rooted = RootedFs::new(&canonical_anchor)?;
    let mut relative = PathBuf::new();
    for component in missing_components.into_iter().rev() {
        relative.push(component);
        let absolute = rooted.logical_root_path().join(&relative);
        if rooted.create_dir(&absolute, 0o755)?.is_none() {
            let identity = rooted
                .entry_identity_if_parent_exists(&absolute)?
                .ok_or_else(|| {
                    anyhow!(
                        "media library directory disappeared: {}",
                        absolute.display()
                    )
                })?;
            if !identity.is_dir() {
                bail!(
                    "media library path component is not a directory: {}",
                    absolute.display()
                );
            }
        }
    }
    Ok(())
}

fn validate_registered_root_for_use(root: &Path) -> Result<()> {
    if !root.is_absolute() {
        bail!("registered media library path is not absolute");
    }
    let root = normalize_trusted_macos_alias(root)?;
    reject_symlink_components(&root, true)?;
    let metadata = fs::metadata(&root).with_context(|| {
        format!(
            "registered media library is unavailable: {}",
            root.display()
        )
    })?;
    if !metadata.is_dir() {
        bail!(
            "registered media library is not a directory: {}",
            root.display()
        );
    }
    let canonical = fs::canonicalize(&root)?;
    if canonical != root {
        bail!("registered media library path no longer resolves to its recorded root");
    }
    RootedFs::new(&root)?.validate_configured_root()
}

fn validate_category_under_root(root: &Path, category: Option<&Path>) -> Result<()> {
    let Some(category) = category else {
        return Ok(());
    };
    let rooted = RootedFs::new(root)?;
    crate::file_manager::validate_not_private_downloader_path(&rooted, category)?;
    let mut current = root.to_path_buf();
    for component in category.components() {
        let Component::Normal(name) = component else {
            bail!("media library category must be a normalized root-relative path");
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!(
                    "media library category contains a symlink: {}",
                    current.display()
                );
            }
            Ok(metadata) if !metadata.is_dir() => {
                bail!(
                    "media library category component is not a directory: {}",
                    current.display()
                );
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to inspect media category {}", current.display())
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_category(category: Option<&Path>) -> Result<Option<PathBuf>> {
    let Some(category) = category else {
        return Ok(None);
    };
    if category.as_os_str().is_empty() {
        return Ok(None);
    }
    if category.is_absolute() {
        bail!("media library category must be relative to its root");
    }
    let mut normalized = PathBuf::new();
    let mut count = 0usize;
    for component in category.components() {
        let Component::Normal(name) = component else {
            bail!("media library category may not contain '.', '..', or root components");
        };
        let text = name
            .to_str()
            .ok_or_else(|| anyhow!("media library category must be valid UTF-8"))?;
        if text.is_empty() || text.chars().any(char::is_control) {
            bail!("media library category contains an invalid path component");
        }
        if is_private_downloader_component_conservative(text) {
            bail!("media library category may not use a private downloader directory");
        }
        normalized.push(name);
        count += 1;
    }
    if count == 0 {
        return Ok(None);
    }
    Ok(Some(normalized))
}

fn is_private_downloader_component_conservative(component: &str) -> bool {
    let normalized = component
        .nfd()
        .flat_map(char::to_lowercase)
        .filter(|character| {
            !matches!(
                *character as u32,
                0x200c..=0x200f | 0x202a..=0x202e | 0x206a..=0x206f | 0xfeff
            )
        })
        .collect::<String>();
    normalized.starts_with(".telegram-video-downloader-")
}

pub(crate) fn validate_name(name: &str) -> Result<String> {
    let name = name.trim();
    if name.is_empty() || name.len() > 100 || name.chars().any(char::is_control) {
        bail!("media library name must be 1 to 100 bytes and contain no control characters");
    }
    Ok(name.to_string())
}

fn reject_symlink_components(path: &Path, require_exists: bool) -> Result<()> {
    let normalized_path = normalize_trusted_macos_alias(path)?;
    let path = normalized_path.as_path();
    if !path.is_absolute() {
        bail!("media library path must be absolute");
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => current.push(Path::new("/")),
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::Normal(name) => {
                current.push(name);
                match fs::symlink_metadata(&current) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        bail!(
                            "media library path contains a symlink: {}",
                            current.display()
                        );
                    }
                    Ok(_) => {}
                    Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound && !require_exists => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        bail!("media library path does not exist: {}", current.display());
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("failed to inspect media library path {}", current.display())
                        });
                    }
                }
            }
            Component::CurDir | Component::ParentDir => {
                bail!("media library path must not contain '.' or '..' components");
            }
        }
    }
    Ok(())
}

/// macOS exposes a few system paths through stable symlink aliases. Normalize only those
/// aliases, and only after verifying that the alias still resolves to its documented target.
/// This keeps ordinary user supplied symlink components subject to the strict rejection below.
pub(crate) fn normalize_trusted_macos_alias(path: &Path) -> Result<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        for (alias, expected_target) in [
            (Path::new("/var"), Path::new("/private/var")),
            (Path::new("/tmp"), Path::new("/private/tmp")),
            (Path::new("/etc"), Path::new("/private/etc")),
        ] {
            let Ok(suffix) = path.strip_prefix(alias) else {
                continue;
            };
            let metadata = match fs::symlink_metadata(alias) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to inspect trusted macOS path alias {}",
                            alias.display()
                        )
                    });
                }
            };
            if !metadata.file_type().is_symlink() {
                continue;
            }
            let resolved_alias = fs::canonicalize(alias).with_context(|| {
                format!(
                    "failed to resolve trusted macOS path alias {}",
                    alias.display()
                )
            })?;
            let resolved_target = fs::canonicalize(expected_target).with_context(|| {
                format!(
                    "failed to resolve expected target for macOS path alias {}",
                    alias.display()
                )
            })?;
            if resolved_alias != resolved_target || resolved_target != expected_target {
                bail!(
                    "macOS system path alias {} does not resolve to its trusted target {}",
                    alias.display(),
                    expected_target.display()
                );
            }
            return Ok(resolved_target.join(suffix));
        }
    }
    Ok(path.to_path_buf())
}

fn canonicalize_if_available(path: &Path) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path).with_context(|| {
        format!(
            "failed to resolve registered media library {}",
            path.display()
        )
    })?;
    Ok(canonical)
}

fn registered_root_overlap_path(path: &Path) -> Result<PathBuf> {
    let normalized = normalize_trusted_macos_alias(path)?;
    Ok(canonicalize_if_available(&normalized).unwrap_or(normalized))
}

fn roots_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn library_id_for_root(root: &Path) -> String {
    let mut digest = Sha256::new();
    digest.update(b"media-library-id-v1\0");
    digest.update(root.as_os_str().as_encoded_bytes());
    let digest = digest.finalize();
    format!("ml-{}", hex_prefix(&digest, 16))
}

fn hex_prefix(bytes: &[u8], count: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(count);
    for byte in bytes.iter().take(count.div_ceil(2)) {
        output.push(HEX[(byte >> 4) as usize] as char);
        if output.len() < count {
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }
    output
}

fn document_from_snapshot(snapshot: &MediaLibrarySnapshot) -> RegistryDocument {
    RegistryDocument {
        version: 1,
        revision: snapshot.revision,
        default_library_id: snapshot.default_library_id.clone(),
        libraries: snapshot.libraries.clone(),
        applied_migration_defaults: Vec::new(),
        skipped_migration_defaults: Vec::new(),
        archived_migration_default_count: 0,
        archived_migration_default_sha256: String::new(),
    }
}

fn migration_default_archive_path(registry_path: &Path) -> Result<PathBuf> {
    let parent = registry_path
        .parent()
        .ok_or_else(|| anyhow!("media library settings path has no parent"))?;
    Ok(parent.join(DEFAULT_ARCHIVE_FILE))
}

fn load_migration_default_archive(
    root: &RootedFs,
    registry_path: &Path,
    registry: &RegistryDocument,
) -> Result<LoadedMigrationDefaultArchive> {
    let inline = inline_migration_default_entries(
        &registry.applied_migration_defaults,
        &registry.skipped_migration_defaults,
    );
    validate_migration_default_count(registry.archived_migration_default_count, inline.len())?;
    if registry.archived_migration_default_count == 0 {
        if !registry.archived_migration_default_sha256.is_empty() {
            bail!("media library migration archive has a digest without archived entries");
        }
    } else {
        validate_sha256_hex(&registry.archived_migration_default_sha256)?;
    }

    let archive_path = migration_default_archive_path(registry_path)?;
    let Some(file) = root.open_bound_file(&archive_path)? else {
        if registry.archived_migration_default_count == 0
            && registry.archived_migration_default_sha256.is_empty()
        {
            return Ok(LoadedMigrationDefaultArchive {
                entries: Vec::new(),
                bytes: None,
                identity: None,
            });
        }
        bail!("media library migration archive is missing despite its registry binding");
    };
    file.validate_private_single_link(0o600)
        .context("media library migration archive must be private and single-linked")?;
    if file.byte_len()? > MAX_MIGRATION_DEFAULT_ARCHIVE_BYTES as u64 {
        bail!("media library migration archive exceeds its size limit");
    }
    let bytes = file.read_limited(MAX_MIGRATION_DEFAULT_ARCHIVE_BYTES)?;
    let archive_document: MigrationDefaultArchiveDocument =
        serde_json::from_slice(&bytes).context("invalid media library migration archive")?;
    if archive_document.version != 1 {
        bail!("unsupported media library migration archive version");
    }
    validate_migration_archive_entries(&archive_document.entries)?;
    if serde_json::to_vec(&archive_document)? != bytes {
        bail!("media library migration archive is not canonically encoded");
    }
    if archive_document.entries.len() > MAX_RESOLVED_MIGRATION_DEFAULTS {
        bail!("media library migration archive exceeds its entry limit");
    }
    if archive_document.entries.is_empty()
        && registry.archived_migration_default_count == 0
        && registry.archived_migration_default_sha256.is_empty()
    {
        bail!("media library migration archive exists without a committed or pending outcome");
    }

    let archive_sha = sha256_hex(&bytes);
    let mut previous_archive = archive_document.entries.clone();
    let mut inline_matches = 0;
    for inline_entry in &inline {
        match previous_archive
            .binary_search_by(|entry| entry.migration_id.cmp(&inline_entry.migration_id))
        {
            Ok(index) if previous_archive[index].outcome == inline_entry.outcome => {
                inline_matches += 1;
            }
            Ok(_) => bail!("media library migration archive conflicts with inline history"),
            Err(_) => {}
        }
    }

    let normal_archive = archive_sha == registry.archived_migration_default_sha256
        && archive_document.entries.len() == registry.archived_migration_default_count
        && inline_matches == 0;
    if !normal_archive {
        // The archive is written before the registry is compacted. In that crash window, remove
        // the exact duplicate inline tail and verify that the prior archive still matches.
        for inline_entry in &inline {
            if let Ok(index) = previous_archive
                .binary_search_by(|entry| entry.migration_id.cmp(&inline_entry.migration_id))
            {
                previous_archive.remove(index);
            }
        }
        let prior_bytes = encode_migration_default_archive(&previous_archive)?;
        let prior_matches = if registry.archived_migration_default_count == 0 {
            registry.archived_migration_default_sha256.is_empty() && previous_archive.is_empty()
        } else {
            previous_archive.len() == registry.archived_migration_default_count
                && sha256_hex(&prior_bytes) == registry.archived_migration_default_sha256
        };
        if !prior_matches || inline_matches != inline.len() {
            bail!("media library migration archive does not match its registry binding");
        }
    }

    Ok(LoadedMigrationDefaultArchive {
        entries: archive_document.entries,
        bytes: Some(bytes),
        identity: Some(file.identity()),
    })
}

fn inline_migration_default_entries(
    applied: &[String],
    skipped: &[String],
) -> Vec<MigrationDefaultArchiveEntry> {
    let mut entries = applied
        .iter()
        .map(|migration_id| MigrationDefaultArchiveEntry {
            migration_id: migration_id.clone(),
            outcome: MigrationDefaultResolution::Applied,
        })
        .chain(
            skipped
                .iter()
                .map(|migration_id| MigrationDefaultArchiveEntry {
                    migration_id: migration_id.clone(),
                    outcome: MigrationDefaultResolution::RevisionChanged,
                }),
        )
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.migration_id.cmp(&right.migration_id));
    entries
}

fn validate_migration_archive_entries(entries: &[MigrationDefaultArchiveEntry]) -> Result<()> {
    let mut previous = None;
    for entry in entries {
        validate_migration_id(&entry.migration_id)?;
        if previous.is_some_and(|previous: &str| previous >= entry.migration_id.as_str()) {
            bail!("media library migration archive has duplicate or unsorted IDs");
        }
        previous = Some(entry.migration_id.as_str());
    }
    Ok(())
}

fn validate_migration_default_count(archived: usize, inline: usize) -> Result<()> {
    if archived.saturating_add(inline) > MAX_RESOLVED_MIGRATION_DEFAULTS {
        bail!("media library migration idempotency history exceeds its limit");
    }
    Ok(())
}

fn encode_migration_default_archive(entries: &[MigrationDefaultArchiveEntry]) -> Result<Vec<u8>> {
    validate_migration_archive_entries(entries)?;
    let document = MigrationDefaultArchiveDocument {
        version: 1,
        entries: entries.to_vec(),
    };
    let bytes = serde_json::to_vec(&document)
        .context("failed to encode media library migration archive")?;
    if bytes.len() > MAX_MIGRATION_DEFAULT_ARCHIVE_BYTES {
        bail!("media library migration archive exceeds its size limit");
    }
    Ok(bytes)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_prefix(&digest, 64)
}

fn validate_sha256_hex(value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("media library migration archive digest is malformed");
    }
    Ok(())
}

fn read_private_registry(root: &RootedFs, path: &Path) -> Result<Option<Vec<u8>>> {
    let Some(file) = root.open_bound_file(path)? else {
        return Ok(None);
    };
    file.validate_private_single_link(0o600)
        .context("media library settings file must be private and single-linked")?;
    if file.byte_len()? > MAX_REGISTRY_BYTES as u64 {
        bail!("media library settings file exceeds its size limit");
    }
    Ok(Some(file.read_limited(MAX_REGISTRY_BYTES)?))
}

fn write_private_registry(
    root: &RootedFs,
    path: &Path,
    snapshot: &MediaLibrarySnapshot,
    applied_migration_defaults: &[String],
    skipped_migration_defaults: &[String],
    archived_migration_default_count: usize,
    archived_migration_default_sha256: &str,
) -> Result<()> {
    let bytes = encode_private_registry(
        snapshot,
        applied_migration_defaults,
        skipped_migration_defaults,
        archived_migration_default_count,
        archived_migration_default_sha256,
    )?;
    write_private_registry_bytes(root, path, &bytes)
}

fn encode_private_registry(
    snapshot: &MediaLibrarySnapshot,
    applied_migration_defaults: &[String],
    skipped_migration_defaults: &[String],
    archived_migration_default_count: usize,
    archived_migration_default_sha256: &str,
) -> Result<Vec<u8>> {
    validate_migration_id_history(applied_migration_defaults, skipped_migration_defaults)?;
    validate_migration_default_count(
        archived_migration_default_count,
        applied_migration_defaults.len() + skipped_migration_defaults.len(),
    )?;
    if archived_migration_default_count == 0 {
        if !archived_migration_default_sha256.is_empty() {
            bail!("media library migration archive has a digest without archived entries");
        }
    } else {
        validate_sha256_hex(archived_migration_default_sha256)?;
    }
    let mut document = document_from_snapshot(snapshot);
    document.applied_migration_defaults = applied_migration_defaults.to_vec();
    document.skipped_migration_defaults = skipped_migration_defaults.to_vec();
    document.archived_migration_default_count = archived_migration_default_count;
    document.archived_migration_default_sha256 = archived_migration_default_sha256.to_string();
    let bytes = serde_json::to_vec(&document).context("failed to encode media library settings")?;
    if bytes.len() > MAX_REGISTRY_BYTES {
        bail!("media library settings exceeds its size limit");
    }
    Ok(bytes)
}

fn write_private_registry_bytes(root: &RootedFs, path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = temporary_sibling(path);
    if let Some(file) = root.open_bound_file(path)? {
        file.validate_private_single_link(0o600)
            .context("media library settings file must be private and single-linked")?;
        let entry = root.bind_entry(path, false)?;
        root.replace_bound_file_atomically_if_identity(
            &entry,
            file.identity(),
            &temp,
            bytes,
            0o600,
        )?;
    } else {
        let (source, identity) = root.create_new_bound_file(&temp, bytes, 0o600)?;
        let destination = root.bind_entry(path, false)?;
        root.rename_via_bound_parents_noreplace_if_identity(&source, &destination, identity)?;
    }
    Ok(())
}

fn write_private_migration_archive(
    root: &RootedFs,
    path: &Path,
    expected_identity: Option<EntryIdentity>,
    bytes: &[u8],
) -> Result<()> {
    if bytes.len() > MAX_MIGRATION_DEFAULT_ARCHIVE_BYTES {
        bail!("media library migration archive exceeds its size limit");
    }
    let temp = temporary_archive_sibling(path);
    match root.open_bound_file(path)? {
        Some(file) => {
            file.validate_private_single_link(0o600)
                .context("media library migration archive must be private and single-linked")?;
            if expected_identity != Some(file.identity()) {
                bail!("media library migration archive identity changed before replacement");
            }
            let entry = root.bind_entry(path, false)?;
            root.replace_bound_file_atomically_if_identity(
                &entry,
                file.identity(),
                &temp,
                bytes,
                0o600,
            )?;
        }
        None => {
            if expected_identity.is_some() {
                bail!("media library migration archive disappeared before replacement");
            }
            let (source, identity) = root.create_new_bound_file(&temp, bytes, 0o600)?;
            let destination = root.bind_entry(path, false)?;
            root.rename_via_bound_parents_noreplace_if_identity(&source, &destination, identity)?;
        }
    }
    Ok(())
}

fn temporary_sibling(path: &Path) -> PathBuf {
    let sequence = REGISTRY_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!(
        "{REGISTRY_TEMP_PREFIX}-{}-{sequence}",
        std::process::id()
    ))
}

fn temporary_archive_sibling(path: &Path) -> PathBuf {
    let sequence = REGISTRY_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!(
        "{DEFAULT_ARCHIVE_TEMP_PREFIX}-{}-{sequence}",
        std::process::id()
    ))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "media-library-registry-{name}-{}-{}",
            std::process::id(),
            REGISTRY_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn trusted_system_temp_alias_resolves_to_the_same_directory() {
        let temp = std::env::temp_dir();
        let normalized = normalize_trusted_macos_alias(&temp).unwrap();
        assert_eq!(
            fs::canonicalize(&normalized).unwrap(),
            fs::canonicalize(&temp).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn explicit_root_creation_rejects_symlink_ancestors_before_mkdir() {
        let directory = test_dir("create-symlink");
        let physical = directory.join("physical");
        fs::create_dir_all(&physical).unwrap();
        let alias = directory.join("alias");
        std::os::unix::fs::symlink(&physical, &alias).unwrap();
        let requested = alias.join("new-library");

        assert!(prepare_library_root(&requested, true).is_err());
        assert!(!physical.join("new-library").exists());
    }

    #[test]
    fn explicit_root_creation_creates_nested_missing_components() {
        let directory = test_dir("create-nested-root");
        let requested = directory.join("nested").join("archive").join("videos");

        let created = prepare_library_root(&requested, true)
            .expect("explicit directory creation should create missing components");

        assert_eq!(created, fs::canonicalize(&requested).unwrap());
        assert!(created.is_dir());
        assert!(created.parent().unwrap().is_dir());
        assert!(created.parent().unwrap().parent().unwrap().is_dir());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn imports_existing_root_and_persists_default_registry() {
        let directory = test_dir("import");
        let root = directory.join("videos");
        fs::create_dir_all(&root).unwrap();
        let registry = MediaLibraryRegistryStore::open(None, &root).unwrap();
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.libraries.len(), 1);
        assert_eq!(
            normalize_trusted_macos_alias(&snapshot.libraries[0].root).unwrap(),
            fs::canonicalize(&root).unwrap()
        );
        assert_eq!(snapshot.default_library_id, snapshot.libraries[0].id);
        let reopened = MediaLibraryRegistryStore::open(None, &root).unwrap();
        assert_eq!(reopened.snapshot(), snapshot);
    }

    #[test]
    fn root_identity_binding_persists_and_rejects_replacement() {
        let directory = test_dir("root-binding");
        let root = directory.join("videos");
        fs::create_dir_all(&root).unwrap();
        let mut registry = MediaLibraryRegistryStore::open(None, &root).unwrap();
        let library_id = registry.snapshot().default_library_id;
        let identity = RootedFs::new(&root).unwrap().root_identity();

        registry
            .bind_root_identity(&library_id, identity.device(), identity.inode())
            .unwrap();
        assert_eq!(
            registry.root_binding(&library_id),
            Some((identity.device(), identity.inode()))
        );
        assert!(
            registry
                .bind_root_identity(&library_id, identity.device(), identity.inode() + 1)
                .is_err()
        );

        let reopened = MediaLibraryRegistryStore::open(None, &root).unwrap();
        assert_eq!(
            reopened.root_binding(&library_id),
            Some((identity.device(), identity.inode()))
        );
    }

    #[test]
    fn migration_default_revision_check_records_applied_and_stale_outcomes() {
        let directory = test_dir("migration-default-cas");
        let source_root = directory.join("videos");
        let archive_root = directory.join("archive");
        fs::create_dir_all(&source_root).unwrap();
        fs::create_dir_all(&archive_root).unwrap();
        let mut registry = MediaLibraryRegistryStore::open(None, &source_root).unwrap();
        let source_id = registry.snapshot().default_library_id;
        let archive = registry
            .add("Archive", &archive_root, None, false, false)
            .unwrap();

        let stale_revision = registry.snapshot().revision;
        registry.set_default(&archive.id).unwrap();
        let changed_revision = registry.snapshot().revision;
        let stale = registry
            .set_default_for_migration_if_revision(
                &source_id,
                "migration-stale-default",
                stale_revision,
            )
            .unwrap();
        assert_eq!(
            stale,
            MigrationDefaultUpdateOutcome::RevisionChanged {
                snapshot: registry.snapshot()
            }
        );
        assert_eq!(registry.snapshot().default_library_id, archive.id);
        assert_eq!(registry.snapshot().revision, changed_revision);

        registry.set_default(&source_id).unwrap();
        let retry = registry
            .set_default_for_migration_if_revision(
                &archive.id,
                "migration-stale-default",
                stale_revision,
            )
            .unwrap();
        assert!(matches!(
            retry,
            MigrationDefaultUpdateOutcome::RevisionChanged { .. }
        ));
        assert_eq!(registry.snapshot().default_library_id, source_id);

        let current_revision = registry.snapshot().revision;
        let applied = registry
            .set_default_for_migration_if_revision(
                &archive.id,
                "migration-applied-default",
                current_revision,
            )
            .unwrap();
        assert!(matches!(
            applied,
            MigrationDefaultUpdateOutcome::Applied { .. }
        ));
        assert_eq!(registry.snapshot().default_library_id, archive.id);
        let applied_revision = registry.snapshot().revision;
        registry.set_default(&source_id).unwrap();
        let replay = registry
            .set_default_for_migration_if_revision(
                &archive.id,
                "migration-applied-default",
                current_revision,
            )
            .unwrap();
        assert!(matches!(
            replay,
            MigrationDefaultUpdateOutcome::Applied { .. }
        ));
        assert_eq!(registry.snapshot().default_library_id, source_id);
        assert_eq!(registry.snapshot().revision, applied_revision + 1);

        let mut reopened = MediaLibraryRegistryStore::open(None, &source_root).unwrap();
        assert_eq!(reopened.snapshot(), registry.snapshot());
        let durable_stale = reopened
            .set_default_for_migration_if_revision(
                &archive.id,
                "migration-stale-default",
                stale_revision,
            )
            .unwrap();
        assert!(matches!(
            durable_stale,
            MigrationDefaultUpdateOutcome::RevisionChanged { .. }
        ));
        assert_eq!(reopened.snapshot().default_library_id, source_id);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn migration_default_history_rotates_to_exact_archive_and_replays_after_user_change() {
        let directory = test_dir("migration-default-archive-many");
        let source_root = directory.join("videos");
        let archive_root = directory.join("archive");
        fs::create_dir_all(&source_root).unwrap();
        fs::create_dir_all(&archive_root).unwrap();
        let mut registry = MediaLibraryRegistryStore::open(None, &source_root).unwrap();
        let source_id = registry.snapshot().default_library_id;
        let archive = registry
            .add("Archive", &archive_root, None, false, false)
            .unwrap();

        for index in 0..360 {
            let migration_id = format!("migration-{index:04}-{}", "x".repeat(100));
            let revision = registry.snapshot().revision;
            if index % 2 == 0 {
                let outcome = registry
                    .set_default_for_migration_if_revision(
                        &archive.id,
                        &migration_id,
                        revision.saturating_add(1),
                    )
                    .unwrap();
                assert!(matches!(
                    outcome,
                    MigrationDefaultUpdateOutcome::RevisionChanged { .. }
                ));
            } else {
                let target = if registry.snapshot().default_library_id == source_id {
                    &archive.id
                } else {
                    &source_id
                };
                let outcome = registry
                    .set_default_for_migration_if_revision(target, &migration_id, revision)
                    .unwrap();
                assert!(matches!(
                    outcome,
                    MigrationDefaultUpdateOutcome::Applied { .. }
                ));
            }
        }

        let registry_bytes = read_private_registry(&registry.fs, &registry.path)
            .unwrap()
            .unwrap();
        assert!(registry_bytes.len() < MAX_REGISTRY_BYTES);
        assert_eq!(registry.applied_migration_defaults.len(), 1);
        assert!(registry.skipped_migration_defaults.is_empty());
        assert_eq!(registry.archived_migration_default_count, 359);
        let archive_counts = registry.archived_migration_defaults.iter().fold(
            (0, 0),
            |(applied, skipped), entry| match entry.outcome {
                MigrationDefaultResolution::Applied => (applied + 1, skipped),
                MigrationDefaultResolution::RevisionChanged => (applied, skipped + 1),
            },
        );
        assert_eq!(archive_counts, (179, 180));

        registry.set_default(&source_id).unwrap();
        let selected_default = registry.snapshot().default_library_id.clone();
        let mut reopened = MediaLibraryRegistryStore::open(None, &source_root).unwrap();
        let replay = reopened
            .set_default_for_migration_if_revision(
                &archive.id,
                &format!("migration-0000-{}", "x".repeat(100)),
                0,
            )
            .unwrap();
        assert!(matches!(
            replay,
            MigrationDefaultUpdateOutcome::RevisionChanged { .. }
        ));
        assert_eq!(reopened.snapshot().default_library_id, selected_default);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn archive_before_registry_crash_survives_user_commit_and_later_compaction() {
        let directory = test_dir("migration-default-archive-crash");
        let source_root = directory.join("videos");
        let archive_root = directory.join("archive");
        fs::create_dir_all(&source_root).unwrap();
        fs::create_dir_all(&archive_root).unwrap();
        let mut registry = MediaLibraryRegistryStore::open(None, &source_root).unwrap();
        let source_id = registry.snapshot().default_library_id;
        let archive = registry
            .add("Archive", &archive_root, None, false, false)
            .unwrap();

        let old_revision = registry.snapshot().revision;
        registry
            .set_default_for_migration_if_revision(
                &archive.id,
                "migration-inline-old",
                old_revision,
            )
            .unwrap();
        let archive_entries = inline_migration_default_entries(
            &registry.applied_migration_defaults,
            &registry.skipped_migration_defaults,
        );
        let archive_bytes = encode_migration_default_archive(&archive_entries).unwrap();
        let archive_path = migration_default_archive_path(&registry.path).unwrap();
        write_private_migration_archive(&registry.fs, &archive_path, None, &archive_bytes)
            .expect("simulate durable archive write before registry compaction");
        drop(registry);

        let mut reopened = MediaLibraryRegistryStore::open(None, &source_root)
            .expect("reopen must recognize the exact duplicated inline/archive tail");
        assert!(matches!(
            reopened
                .resolved_migration_default("migration-inline-old")
                .unwrap(),
            Some(MigrationDefaultUpdateOutcome::Applied { .. })
        ));
        reopened.set_default(&source_id).unwrap();
        reopened
            .set_category(&archive.id, Some(Path::new("Kids")))
            .unwrap();
        let expected_revision = reopened.snapshot().revision;
        reopened
            .set_default_for_migration_if_revision(
                &archive.id,
                "migration-after-crash",
                expected_revision,
            )
            .unwrap();
        reopened.set_default(&source_id).unwrap();
        drop(reopened);

        let mut final_registry = MediaLibraryRegistryStore::open(None, &source_root).unwrap();
        assert!(matches!(
            final_registry
                .resolved_migration_default("migration-inline-old")
                .unwrap(),
            Some(MigrationDefaultUpdateOutcome::Applied { .. })
        ));
        let default_before_replay = final_registry.snapshot().default_library_id.clone();
        final_registry
            .set_default_for_migration_if_revision(&archive.id, "migration-inline-old", 0)
            .unwrap();
        assert_eq!(
            final_registry.snapshot().default_library_id,
            default_before_replay
        );
        assert_eq!(final_registry.archived_migration_default_count, 1);
        let _ = fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn missing_unreadable_or_conflicting_archive_fails_closed_without_rewriting_registry() {
        use std::os::unix::fs::PermissionsExt;

        let directory = test_dir("migration-default-archive-integrity");
        let source_root = directory.join("videos");
        let archive_root = directory.join("archive");
        fs::create_dir_all(&source_root).unwrap();
        fs::create_dir_all(&archive_root).unwrap();
        let mut registry = MediaLibraryRegistryStore::open(None, &source_root).unwrap();
        let archive = registry
            .add("Archive", &archive_root, None, false, false)
            .unwrap();
        let revision = registry.snapshot().revision;
        registry
            .set_default_for_migration_if_revision(&archive.id, "migration-one", revision)
            .unwrap();
        registry
            .set_default_for_migration_if_revision(
                &registry.snapshot().default_library_id,
                "migration-two",
                revision.saturating_add(1),
            )
            .unwrap();
        let registry_path = registry.path.clone();
        let archive_path = migration_default_archive_path(&registry.path).unwrap();
        let main_bytes = fs::read(&registry_path).unwrap();
        drop(registry);

        let original_archive = fs::read(&archive_path).unwrap();
        let mut permissions = fs::metadata(&archive_path).unwrap().permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&archive_path, permissions).unwrap();
        assert!(MediaLibraryRegistryStore::open(None, &source_root).is_err());
        assert_eq!(fs::read(&archive_path).unwrap(), original_archive);
        assert_eq!(fs::read(&registry_path).unwrap(), main_bytes);

        let mut permissions = fs::metadata(&archive_path).unwrap().permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(&archive_path, permissions).unwrap();
        let mut changed: MigrationDefaultArchiveDocument =
            serde_json::from_slice(&original_archive).unwrap();
        changed.entries[0].outcome = match changed.entries[0].outcome {
            MigrationDefaultResolution::Applied => MigrationDefaultResolution::RevisionChanged,
            MigrationDefaultResolution::RevisionChanged => MigrationDefaultResolution::Applied,
        };
        let conflicting_bytes = serde_json::to_vec(&changed).unwrap();
        fs::write(&archive_path, &conflicting_bytes).unwrap();
        assert!(MediaLibraryRegistryStore::open(None, &source_root).is_err());
        assert_eq!(fs::read(&archive_path).unwrap(), conflicting_bytes);
        assert_eq!(fs::read(&registry_path).unwrap(), main_bytes);

        fs::remove_file(&archive_path).unwrap();
        assert!(MediaLibraryRegistryStore::open(None, &source_root).is_err());
        assert_eq!(fs::read(&registry_path).unwrap(), main_bytes);
        let _ = fs::remove_dir_all(directory);
    }

    #[cfg(unix)]
    #[test]
    fn prepared_root_identity_rejects_replacement_and_allows_child_churn() {
        let directory = test_dir("prepared-root-binding");
        let videos = directory.join("videos");
        let archive_root = directory.join("archive");
        let candidate_root = directory.join("candidate");
        let original_candidate = directory.join("candidate-original");
        fs::create_dir_all(&videos).unwrap();
        fs::create_dir_all(&archive_root).unwrap();
        fs::create_dir_all(&candidate_root).unwrap();
        let mut registry = MediaLibraryRegistryStore::open(None, &videos).unwrap();

        let archive_identity = RootedFs::new(&archive_root).unwrap().root_identity();
        let child = archive_root.join("ordinary-child");
        fs::create_dir(&child).unwrap();
        fs::remove_dir(&child).unwrap();
        let library = registry
            .add_with_root_identity("Archive", &archive_root, None, false, archive_identity)
            .unwrap();
        assert_eq!(
            registry.root_binding(&library.id),
            Some((archive_identity.device(), archive_identity.inode()))
        );
        assert_eq!(
            registry.snapshot().default_library_id,
            registry.snapshot().libraries[0].id
        );

        let old_identity = RootedFs::new(&candidate_root).unwrap().root_identity();
        fs::rename(&candidate_root, &original_candidate).unwrap();
        fs::create_dir(&candidate_root).unwrap();
        fs::write(
            candidate_root.join("keep.txt"),
            b"replacement stays untouched",
        )
        .unwrap();
        let before_rejected_default = registry.snapshot();
        let replacement_identity = RootedFs::new(&candidate_root).unwrap().root_identity();
        assert_ne!(replacement_identity, old_identity);
        let error = registry
            .add_with_root_identity("Replacement", &candidate_root, None, true, old_identity)
            .expect_err("registration must reject a replaced pathname before committing default");
        assert!(format!("{error:#}").contains("identity changed during registration"));
        assert_eq!(registry.snapshot(), before_rejected_default);
        assert_eq!(
            registry.snapshot().default_library_id,
            before_rejected_default.libraries[0].id
        );
        assert_eq!(
            fs::read(candidate_root.join("keep.txt")).unwrap(),
            b"replacement stays untouched"
        );

        let reopened = MediaLibraryRegistryStore::open(None, &videos).unwrap();
        assert_eq!(reopened.snapshot(), before_rejected_default);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn registration_is_idempotent_and_root_paths_must_not_overlap() {
        let directory = test_dir("register");
        let first_root = directory.join("videos");
        let second_root = directory.join("archive");
        fs::create_dir_all(&first_root).unwrap();
        fs::create_dir_all(&second_root).unwrap();
        let mut registry = MediaLibraryRegistryStore::open(None, &first_root).unwrap();
        let added = registry
            .add(
                "Archive",
                &second_root,
                Some(Path::new("Movies")),
                false,
                false,
            )
            .unwrap();
        let revision = registry.snapshot().revision;
        let replay = registry
            .add(
                "Archive",
                &second_root,
                Some(Path::new("Movies")),
                false,
                false,
            )
            .unwrap();
        assert_eq!(added, replay);
        assert_eq!(registry.snapshot().revision, revision);
        assert!(
            registry
                .add("Nested", &second_root.join("nested"), None, false, true)
                .is_err()
        );
    }

    #[test]
    fn offline_registered_root_still_blocks_nested_creation_before_side_effects() {
        let directory = test_dir("offline-overlap");
        let default_root = directory.join("videos");
        let archive_root = directory.join("archive");
        let offline_root = directory.join("archive-offline");
        fs::create_dir_all(&default_root).unwrap();
        fs::create_dir_all(&archive_root).unwrap();
        let mut registry = MediaLibraryRegistryStore::open(None, &default_root).unwrap();
        registry
            .add("Archive", &archive_root, None, false, false)
            .unwrap();
        fs::rename(&archive_root, &offline_root).unwrap();

        let nested = archive_root.join("created-through-offline-root");
        let error = registry
            .add("Nested", &nested, None, false, true)
            .expect_err("an offline library remains an overlap boundary");
        assert!(format!("{error:#}").contains("may not be equal, nested, or overlapping"));
        assert!(
            !archive_root.exists(),
            "overlap rejection must happen before create_directory creates the root"
        );
        assert!(!nested.exists());
    }

    #[test]
    fn relocated_registered_root_cannot_be_re_registered_by_inode_alias() {
        let directory = test_dir("relocated-root-alias");
        let default_root = directory.join("videos");
        let archive_root = directory.join("archive");
        let relocated_root = directory.join("archive-relocated");
        fs::create_dir_all(&default_root).unwrap();
        fs::create_dir_all(&archive_root).unwrap();
        let mut registry = MediaLibraryRegistryStore::open(None, &default_root).unwrap();
        let original = registry
            .add("Archive", &archive_root, None, false, false)
            .unwrap();
        let registered_path = original.root.clone();
        let revision = registry.snapshot().revision;
        fs::rename(&archive_root, &relocated_root).unwrap();

        let error = registry
            .add("Relocated", &relocated_root, None, false, false)
            .expect_err("the same bound root object cannot acquire a second path identity");
        assert!(format!("{error:#}").contains("aliases an already registered root"));
        assert_eq!(registry.snapshot().revision, revision);
        assert_eq!(
            registry.library(&original.id).unwrap().root,
            registered_path
        );
    }

    #[test]
    fn default_category_rejects_escape_and_symlink_components() {
        assert!(validate_category(Some(Path::new("../outside"))).is_err());
        assert!(validate_category(Some(Path::new("/absolute"))).is_err());

        let directory = test_dir("category");
        let root = directory.join("videos");
        fs::create_dir_all(&root).unwrap();
        #[cfg(unix)]
        {
            let target = directory.join("target");
            fs::create_dir_all(&target).unwrap();
            std::os::unix::fs::symlink(&target, root.join("linked")).unwrap();
            assert!(validate_category_under_root(&root, Some(Path::new("linked"))).is_err());
        }
        assert_eq!(validate_category(Some(Path::new(""))).unwrap(), None);
    }

    #[test]
    fn removing_default_is_blocked_and_unknown_remove_is_idempotent() {
        let directory = test_dir("remove");
        let root = directory.join("videos");
        fs::create_dir_all(&root).unwrap();
        let mut registry = MediaLibraryRegistryStore::open(None, &root).unwrap();
        let default_id = registry.snapshot().default_library_id;
        assert!(registry.remove(&default_id).is_err());
        let revision = registry.snapshot().revision;
        assert_eq!(registry.remove("missing").unwrap().revision, revision);
    }

    #[test]
    fn private_registry_rejects_non_private_replacement() {
        let directory = test_dir("permissions");
        let root = directory.join("videos");
        fs::create_dir_all(&root).unwrap();
        let registry = MediaLibraryRegistryStore::open(None, &root).unwrap();
        let metadata = fs::metadata(registry.registry_path()).unwrap();
        assert!(!metadata.permissions().readonly());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        }
        assert!(RootedFs::new(&directory).unwrap().root_identity().is_dir());
    }
}
