//! Bounded cleanup for one completed Music staging job.
//!
//! The manager's durable completion record selects the job ID. This module binds that exact
//! direct child of the configured staging root, inventories it without following links, then
//! removes only the identities captured in that inventory. Directory timestamps are ignored:
//! child creation/removal changes them during ordinary cleanup, while descriptor identity and
//! owner/mode policy continue to identify the selected objects.

use anyhow::{Context, Result, bail};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

use crate::safe_fs::{BoundEntry, BoundMetadataFile, EntryIdentity, RootedFs};

const MAX_CLEANUP_ENTRIES: usize = 4_096;
const MAX_CLEANUP_FILES: usize = 2_048;
const MAX_CLEANUP_DIRECTORIES: usize = 512;
const MAX_CLEANUP_DEPTH: usize = 16;
const MAX_CLEANUP_FILE_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_CLEANUP_TOTAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(45);

/// A no-follow snapshot of a regular Music or archive file used as an external deletion guard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VerifiedFileIdentity {
    pub(crate) path: PathBuf,
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) size: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DirectoryPolicy {
    owner: u32,
    group: u32,
    mode: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FilePolicy {
    owner: u32,
    group: u32,
    mode: u32,
    link_count: u64,
    size: u64,
}

#[derive(Clone, Debug)]
struct FileNode {
    entry: BoundEntry,
    identity: EntryIdentity,
    policy: FilePolicy,
    open_file: BoundMetadataFile,
}

#[derive(Clone, Debug)]
struct DirectoryNode {
    entry: BoundEntry,
    identity: EntryIdentity,
    policy: DirectoryPolicy,
}

#[derive(Default)]
struct ScanInventory {
    entries: usize,
    file_count: usize,
    directory_count: usize,
    bytes: u64,
    files: Vec<FileNode>,
    directories: Vec<DirectoryNode>,
}

/// Capture a normalized, non-symlinked regular-file identity for a fresh Music/archive proof.
pub(crate) fn identify_regular_file(path: &Path) -> Result<VerifiedFileIdentity> {
    validate_absolute_normalized_path(path, "guard file")?;
    if !path_components_are_real(path, false)? {
        bail!("guard file path is missing: {}", path.display());
    }
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect guard file {}", path.display()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        bail!(
            "guard path is not an owned regular file: {}",
            path.display()
        );
    }
    Ok(VerifiedFileIdentity {
        path: path.to_path_buf(),
        device: metadata.dev(),
        inode: metadata.ino(),
        size: metadata.len(),
    })
}

/// Return whether the exact manager-owned job directory exists, rejecting malformed paths and
/// replaced/symlinked staging roots. A missing configured root or job is an idempotent no-op.
pub(crate) fn staging_job_exists(staging_root: &Path, job_id: &str) -> Result<bool> {
    validate_job_id(job_id)?;
    let Some(root) = open_staging_root(staging_root)? else {
        return Ok(false);
    };
    let job_path = root.root_path().join(job_id);
    let entry = root.bind_entry(&job_path, false)?;
    match root.bound_entry_identity(&entry)? {
        None => Ok(false),
        Some(identity) if identity.is_dir() => Ok(true),
        Some(_) => bail!("Music staging job path is not a real directory"),
    }
}

/// Remove only `staging_root/job_id`, after proving that the currently verified Music copy and
/// uploaded archive are distinct from every selected file. The supplied external identities
/// come from the current Music verification receipt and a fresh archive metadata check.
#[cfg(test)]
pub(crate) fn cleanup_staging_tree(
    staging_root: &Path,
    job_id: &str,
    music: &VerifiedFileIdentity,
    archive: &VerifiedFileIdentity,
) -> Result<()> {
    cleanup_staging_tree_with_refresh(staging_root, job_id, || {
        Ok((music.clone(), archive.clone()))
    })
}

/// Production variant: refresh both external proofs after inventory and just before removal.
/// Music may relocate its own copy, so the caller resolves the persistent ID at this point.
pub(crate) fn cleanup_staging_tree_with_refresh<F>(
    staging_root: &Path,
    job_id: &str,
    refresh_guards: F,
) -> Result<()>
where
    F: FnMut() -> Result<(VerifiedFileIdentity, VerifiedFileIdentity)>,
{
    cleanup_staging_tree_with_hook(staging_root, job_id, refresh_guards, |_| Ok(()))
}

fn cleanup_staging_tree_with_hook<F, H>(
    staging_root: &Path,
    job_id: &str,
    mut refresh_guards: F,
    mut before_remove: H,
) -> Result<()>
where
    F: FnMut() -> Result<(VerifiedFileIdentity, VerifiedFileIdentity)>,
    H: FnMut(usize) -> Result<()>,
{
    validate_job_id(job_id)?;
    let Some(root) = open_staging_root(staging_root)? else {
        return Ok(());
    };
    let job_path = root.root_path().join(job_id);
    let job_entry = root.bind_entry(&job_path, false)?;
    let Some(job_identity) = root.bound_entry_identity(&job_entry)? else {
        return Ok(());
    };
    if !job_identity.is_dir() {
        bail!("Music staging job path is not a real directory");
    }
    ensure_same_device(job_identity, root.root_identity(), &job_path)?;

    let scan_deadline = Instant::now() + CLEANUP_TIMEOUT;
    let mut inventory = ScanInventory::default();
    scan_directory(
        &root,
        &job_entry,
        job_identity,
        0,
        &job_path,
        scan_deadline,
        &mut inventory,
    )?;
    let ScanInventory {
        files, directories, ..
    } = inventory;

    // Resolve current external objects after inventory. Music may move its own file during
    // cloud-library management, so the caller refreshes by persistent ID at this point.
    let (music, archive) = refresh_guards()?;
    validate_absolute_normalized_path(&music.path, "Music copy")?;
    validate_absolute_normalized_path(&archive.path, "archive")?;
    if same_file(&music, &archive) || music.path == archive.path {
        bail!("Music copy aliases the iCloud archive");
    }
    if music.path.starts_with(root.root_path()) || archive.path.starts_with(root.root_path()) {
        bail!("Music copy or archive resolves inside Music staging");
    }
    if !path_components_are_real(&music.path, false)?
        || !path_components_are_real(&archive.path, false)?
    {
        bail!("Music copy or archive disappeared during staging cleanup");
    }
    validate_external_identity(&music).context("revalidate the current Music copy")?;
    validate_external_identity(&archive).context("revalidate the uploaded archive")?;
    for file in &files {
        if same_file_identity(file.identity, music.device, music.inode)
            || same_file_identity(file.identity, archive.device, archive.inode)
        {
            bail!("a staging file aliases the Music copy or iCloud archive");
        }
    }

    // Revalidate every selected object before the first unlink. This checks the properties that
    // authorize removal (object identity, type, size bound, owner, mode, and link count). We do
    // not compare parent mtime/ctime because child churn is expected and does not prove mutation.
    root.validate_configured_root()?;
    validate_private_root(&root)?;
    for directory in &directories {
        validate_directory_node(&root, directory)?;
    }
    for file in &files {
        validate_file_node(&root, file)?;
    }
    validate_external_identity(&music)
        .context("revalidate the current Music copy before removal")?;
    validate_external_identity(&archive)
        .context("revalidate the uploaded archive before removal")?;

    let removal_deadline = Instant::now() + CLEANUP_TIMEOUT;
    let mut removed_files = 0usize;
    for file in &files {
        check_deadline(removal_deadline)?;
        before_remove(removed_files)?;
        validate_parent_policy(&root, file.entry.path().parent(), &directories)?;
        let Some(current) = root.bound_entry_identity(&file.entry)? else {
            continue;
        };
        if current != file.identity {
            bail!("Music staging file was replaced before removal");
        }
        root.remove_bound_file_if_identity_with_validation(&file.entry, file.identity, || {
            validate_file_descriptor(&file.open_file, file.identity, file.policy)
                .context("Music staging file changed after quarantine")?;
            validate_parent_policy(&root, file.entry.path().parent(), &directories)
                .context("Music staging parent policy changed after quarantine")?;
            validate_external_identity(&music)
                .context("current Music copy changed during staging cleanup")?;
            validate_external_identity(&archive)
                .context("uploaded archive changed during staging cleanup")?;
            Ok(())
        })?;
        removed_files += 1;
    }

    // Directories are stored post-order, so each removal is empty and affects only its captured
    // name/object. A new child makes rmdir fail and leaves the remaining job safely retryable.
    for directory in &directories {
        check_deadline(removal_deadline)?;
        validate_parent_policy(&root, directory.entry.path().parent(), &directories)?;
        validate_directory_at(
            &root,
            directory.entry.path(),
            directory.identity,
            directory.policy,
        )?;
        validate_external_identity(&music)
            .context("current Music copy changed during staging cleanup")?;
        validate_external_identity(&archive)
            .context("uploaded archive changed during staging cleanup")?;
        match root.bound_entry_identity(&directory.entry)? {
            None => continue,
            Some(current) if current == directory.identity => {
                root.remove_bound_dir_if_identity(&directory.entry, directory.identity)?;
            }
            Some(_) => bail!("Music staging directory was replaced before removal"),
        }
    }
    Ok(())
}

fn scan_directory(
    root: &RootedFs,
    entry: &BoundEntry,
    expected: EntryIdentity,
    depth: usize,
    job_path: &Path,
    deadline: Instant,
    inventory: &mut ScanInventory,
) -> Result<()> {
    check_deadline(deadline)?;
    if depth > MAX_CLEANUP_DEPTH {
        bail!("Music staging job exceeds the directory depth limit");
    }
    if !entry.path().starts_with(job_path) {
        bail!("Music staging scan escaped the selected job directory");
    }
    let Some(current) = root.bound_entry_identity(entry)? else {
        // A child that disappears between enumeration and opening is ordinary directory churn.
        return Ok(());
    };
    if current != expected {
        bail!("Music staging entry changed during cleanup scan");
    }
    ensure_same_device(current, root.root_identity(), entry.path())?;
    inventory.entries = inventory.entries.saturating_add(1);
    if inventory.entries > MAX_CLEANUP_ENTRIES {
        bail!("Music staging job exceeds the entry-count limit");
    }

    if expected.is_dir() {
        inventory.directory_count = inventory.directory_count.saturating_add(1);
        if inventory.directory_count > MAX_CLEANUP_DIRECTORIES {
            bail!("Music staging job exceeds the directory-count limit");
        }
        let directory = root.open_bound_directory(entry, expected)?;
        let (owner, group, mode) = directory.access_policy()?;
        let policy = DirectoryPolicy { owner, group, mode };
        validate_directory_policy(policy, entry.path())?;
        let children =
            root.list_bound_directory_until(entry, expected, MAX_CLEANUP_ENTRIES, deadline)?;
        for (name, child_identity) in children {
            check_deadline(deadline)?;
            if is_reserved_control_name(&name) {
                bail!("reserved Music state or cleanup-control entry found in staging job");
            }
            let child = root.bind_direct_child_of_bound_directory(entry, expected, &name)?;
            match root.bound_entry_identity(&child)? {
                None => continue,
                Some(current) if current == child_identity => {}
                Some(_) => bail!("Music staging child changed during cleanup scan"),
            }
            scan_directory(
                root,
                &child,
                child_identity,
                depth + 1,
                job_path,
                deadline,
                inventory,
            )?;
        }
        inventory.directories.push(DirectoryNode {
            entry: entry.clone(),
            identity: expected,
            policy,
        });
        return Ok(());
    }

    if !expected.is_file() {
        bail!("Music staging job contains a symlink or unsupported filesystem object");
    }
    inventory.file_count = inventory.file_count.saturating_add(1);
    if inventory.file_count > MAX_CLEANUP_FILES {
        bail!("Music staging job exceeds the regular-file count limit");
    }
    let file = root.open_bound_metadata_file_if_identity(entry, expected)?;
    let policy = file.access_policy()?;
    let size = file.byte_len()?;
    let snapshot = FilePolicy {
        owner: policy.owner,
        group: policy.group,
        mode: policy.mode,
        link_count: policy.link_count,
        size,
    };
    validate_file_policy(snapshot, entry.path())?;
    inventory.bytes = inventory
        .bytes
        .checked_add(size)
        .ok_or_else(|| anyhow::anyhow!("Music staging byte count overflow"))?;
    if inventory.bytes > MAX_CLEANUP_TOTAL_BYTES {
        bail!("Music staging job exceeds the total byte limit");
    }
    inventory.files.push(FileNode {
        entry: entry.clone(),
        identity: expected,
        policy: snapshot,
        open_file: file,
    });
    Ok(())
}

fn open_staging_root(staging_root: &Path) -> Result<Option<RootedFs>> {
    validate_absolute_normalized_path(staging_root, "Music staging root")?;
    if !path_components_are_real(staging_root, true)? {
        return Ok(None);
    }
    let root = RootedFs::new(staging_root).with_context(|| {
        format!(
            "open configured Music staging root {}",
            staging_root.display()
        )
    })?;
    if root.root_path() != staging_root {
        bail!("configured Music staging root resolves through an alias");
    }
    validate_private_root(&root)?;
    Ok(Some(root))
}

fn validate_private_root(root: &RootedFs) -> Result<()> {
    root.validate_configured_root()?;
    let identity = root.root_identity();
    let metadata = fs::symlink_metadata(root.root_path())
        .with_context(|| format!("inspect Music staging root {}", root.root_path().display()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.dev() != identity.device()
        || metadata.ino() != identity.inode()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o7777 != 0o700
    {
        bail!("Music staging root must be an owner-private directory");
    }
    Ok(())
}

fn validate_directory_node(root: &RootedFs, node: &DirectoryNode) -> Result<()> {
    validate_directory_at(root, node.entry.path(), node.identity, node.policy)
}

fn validate_directory_at(
    root: &RootedFs,
    path: &Path,
    expected: EntryIdentity,
    expected_policy: DirectoryPolicy,
) -> Result<()> {
    let entry = root.bind_entry(path, false)?;
    match root.bound_entry_identity(&entry)? {
        None => return Ok(()),
        Some(current) if current == expected && current.is_dir() => {}
        Some(_) => bail!("Music staging directory identity changed"),
    }
    let directory = root.open_bound_directory(&entry, expected)?;
    let (owner, group, mode) = directory.access_policy()?;
    let current_policy = DirectoryPolicy { owner, group, mode };
    if current_policy != expected_policy {
        bail!("Music staging directory access policy changed");
    }
    validate_directory_policy(current_policy, path)
}

fn validate_file_node(root: &RootedFs, node: &FileNode) -> Result<()> {
    validate_file_at(root, node.entry.path(), node.identity, node.policy)
}

fn validate_file_at(
    root: &RootedFs,
    path: &Path,
    expected: EntryIdentity,
    expected_policy: FilePolicy,
) -> Result<()> {
    let entry = root.bind_entry(path, false)?;
    match root.bound_entry_identity(&entry)? {
        None => return Ok(()),
        Some(current) if current == expected && current.is_file() => {}
        Some(_) => bail!("Music staging file identity changed"),
    }
    let file = root.open_bound_metadata_file_if_identity(&entry, expected)?;
    let policy = file.access_policy()?;
    let size = file.byte_len()?;
    let current_policy = FilePolicy {
        owner: policy.owner,
        group: policy.group,
        mode: policy.mode,
        link_count: policy.link_count,
        size,
    };
    if current_policy != expected_policy {
        bail!("Music staging file access policy or size changed");
    }
    validate_file_policy(current_policy, path)
}

fn validate_file_descriptor(
    file: &BoundMetadataFile,
    expected_identity: EntryIdentity,
    expected_policy: FilePolicy,
) -> Result<()> {
    file.validate_identity()?;
    if file.identity() != expected_identity {
        bail!("Music staging file descriptor identity changed");
    }
    let policy = file.access_policy()?;
    let current_policy = FilePolicy {
        owner: policy.owner,
        group: policy.group,
        mode: policy.mode,
        link_count: policy.link_count,
        size: file.byte_len()?,
    };
    if current_policy != expected_policy {
        bail!("Music staging file access policy or size changed");
    }
    Ok(())
}

fn validate_parent_policy(
    root: &RootedFs,
    parent: Option<&Path>,
    directories: &[DirectoryNode],
) -> Result<()> {
    let parent = parent.context("Music staging entry has no parent directory")?;
    if parent == root.root_path() {
        return validate_private_root(root);
    }
    let expected = directories
        .iter()
        .find(|directory| directory.entry.path() == parent)
        .context("Music staging entry parent is outside the selected job")?;
    validate_directory_node(root, expected)
}

fn validate_directory_policy(policy: DirectoryPolicy, path: &Path) -> Result<()> {
    if policy.owner != unsafe { libc::geteuid() } || policy.mode & 0o7777 != 0o700 {
        bail!(
            "Music staging directory is not owner-private: {}",
            path.display()
        );
    }
    Ok(())
}

fn validate_file_policy(policy: FilePolicy, path: &Path) -> Result<()> {
    if policy.owner != unsafe { libc::geteuid() }
        || policy.link_count != 1
        || policy.size > MAX_CLEANUP_FILE_BYTES
        || policy.mode & 0o7000 != 0
    {
        bail!(
            "Music staging file is unowned, hard-linked, oversized, or has special permissions: {}",
            path.display()
        );
    }
    Ok(())
}

fn validate_external_identity(expected: &VerifiedFileIdentity) -> Result<()> {
    let current = identify_regular_file(&expected.path)?;
    if current.device != expected.device
        || current.inode != expected.inode
        || current.size != expected.size
    {
        bail!(
            "guard file object identity or size changed: {}",
            expected.path.display()
        );
    }
    Ok(())
}

fn same_file(left: &VerifiedFileIdentity, right: &VerifiedFileIdentity) -> bool {
    same_file_identity_values(left.device, left.inode, right.device, right.inode)
}

fn same_file_identity(identity: EntryIdentity, device: u64, inode: u64) -> bool {
    identity.device() == device && identity.inode() == inode
}

fn same_file_identity_values(
    left_device: u64,
    left_inode: u64,
    right_device: u64,
    right_inode: u64,
) -> bool {
    left_device == right_device && left_inode == right_inode
}

fn ensure_same_device(identity: EntryIdentity, root: EntryIdentity, path: &Path) -> Result<()> {
    if identity.device() != root.device() {
        bail!(
            "cross-device entry found in Music staging job: {}",
            path.display()
        );
    }
    Ok(())
}

fn validate_job_id(job_id: &str) -> Result<()> {
    let Some(suffix) = job_id.strip_prefix("mj1-") else {
        bail!("invalid Music staging job ID");
    };
    if suffix.len() != 24
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("invalid Music staging job ID");
    }
    Ok(())
}

fn validate_absolute_normalized_path(path: &Path, label: &str) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        bail!(
            "{label} path must be absolute and normalized: {}",
            path.display()
        );
    }
    Ok(())
}

/// Walk existing path components without following links; stop cleanly at the first missing one
/// only when the caller permits an idempotent missing staging target.
fn path_components_are_real(path: &Path, allow_missing: bool) -> Result<bool> {
    let mut current = PathBuf::new();
    let components = path.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        match component {
            Component::RootDir => current.push(Path::new("/")),
            Component::Normal(name) => current.push(name),
            Component::Prefix(_) | Component::CurDir | Component::ParentDir => {
                bail!("path contains a non-normal component: {}", path.display());
            }
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    bail!("path contains a symlink component: {}", current.display());
                }
                if index + 1 < components.len() && !metadata.is_dir() {
                    bail!("path component is not a directory: {}", current.display());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && allow_missing => {
                return Ok(false);
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("inspect no-follow path component {}", current.display())
                });
            }
        }
    }
    Ok(true)
}

fn is_reserved_control_name(name: &std::ffi::OsStr) -> bool {
    matches!(
        name.to_str(),
        Some("owner.lock" | "jobs.json" | "catalog.json")
    ) || name
        .to_str()
        .is_some_and(|value| value.starts_with(".telegram-video-downloader-remove"))
}

fn check_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        bail!("Music staging cleanup exceeded its time limit");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn job_id() -> &'static str {
        "mj1-0123456789abcdef01234567"
    }

    fn create_guards(root: &Path) -> (VerifiedFileIdentity, VerifiedFileIdentity) {
        let music_path = root.join("music-copy.m4a");
        let archive_path = root.join("archive.m4a");
        fs::write(&music_path, b"independent Music copy").unwrap();
        fs::write(&archive_path, b"uploaded archive").unwrap();
        (
            identify_regular_file(&music_path).unwrap(),
            identify_regular_file(&archive_path).unwrap(),
        )
    }

    #[test]
    fn removes_only_the_selected_job_after_multiple_prepare_attempts() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let staging = root.join(".telegram-video-downloader-staging/music");
        let current = staging.join(job_id());
        let old_job = staging.join("mj1-fedcba9876543210fedcba98");
        fs::create_dir_all(current.join("prepare-attempt-2")).unwrap();
        fs::create_dir_all(&old_job).unwrap();
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&current, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(
            current.join("prepare-attempt-2"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::set_permissions(&old_job, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(current.join("original.flac"), b"staged original").unwrap();
        fs::write(
            current.join("prepare-attempt-2/import.m4a"),
            b"prepared import",
        )
        .unwrap();
        fs::write(old_job.join("original.m4a"), b"older cached upload").unwrap();
        let (music, archive) = create_guards(&root);

        cleanup_staging_tree(&staging, job_id(), &music, &archive).unwrap();

        assert!(!current.exists());
        assert_eq!(
            fs::read(old_job.join("original.m4a")).unwrap(),
            b"older cached upload"
        );
        assert_eq!(fs::read(&music.path).unwrap(), b"independent Music copy");
        assert_eq!(fs::read(&archive.path).unwrap(), b"uploaded archive");
    }

    #[test]
    fn missing_job_is_idempotent_and_a_second_cleanup_succeeds() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let staging = root.join("staging/music");
        fs::create_dir_all(&staging).unwrap();
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
        let (music, archive) = create_guards(&root);
        cleanup_staging_tree(&staging, job_id(), &music, &archive).unwrap();

        let job = staging.join(job_id());
        fs::create_dir(&job).unwrap();
        fs::set_permissions(&job, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(job.join("original.m4a"), b"one attempt").unwrap();
        cleanup_staging_tree(&staging, job_id(), &music, &archive).unwrap();
        cleanup_staging_tree(&staging, job_id(), &music, &archive).unwrap();
        assert!(!job.exists());
    }

    #[test]
    fn a_partial_removal_failure_can_be_retried_safely() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let staging = root.join("staging/music");
        let job = staging.join(job_id());
        fs::create_dir_all(&job).unwrap();
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&job, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(job.join("first.m4a"), b"first file").unwrap();
        fs::write(job.join("second.m4a"), b"second file").unwrap();
        let (music, archive) = create_guards(&root);

        let failed = cleanup_staging_tree_with_hook(
            &staging,
            job_id(),
            || Ok((music.clone(), archive.clone())),
            |removed_files| {
                if removed_files == 1 {
                    bail!("injected retryable failure after one selected file removal");
                }
                Ok(())
            },
        );
        assert!(failed.is_err());
        assert!(job.is_dir());
        assert_eq!(fs::read_dir(&job).unwrap().count(), 1);

        cleanup_staging_tree(&staging, job_id(), &music, &archive).unwrap();
        assert!(!job.exists());
    }

    #[test]
    fn refuses_symlinks_hardlinks_and_path_traversal_without_following_them() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let staging = root.join("staging/music");
        let job = staging.join(job_id());
        fs::create_dir_all(&job).unwrap();
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&job, fs::Permissions::from_mode(0o700)).unwrap();
        let outside = root.join("outside.txt");
        fs::write(&outside, b"preserve me").unwrap();
        std::os::unix::fs::symlink(&outside, job.join("link.txt")).unwrap();
        let (music, archive) = create_guards(&root);
        assert!(cleanup_staging_tree(&staging, job_id(), &music, &archive).is_err());
        assert_eq!(fs::read(&outside).unwrap(), b"preserve me");
        assert!(cleanup_staging_tree(&staging, "../outside", &music, &archive).is_err());

        fs::remove_file(job.join("link.txt")).unwrap();
        std::fs::hard_link(&outside, job.join("linked.txt")).unwrap();
        assert!(cleanup_staging_tree(&staging, job_id(), &music, &archive).is_err());
        assert_eq!(fs::read(&outside).unwrap(), b"preserve me");
    }

    #[test]
    fn refuses_music_inside_the_job_and_preserves_archive_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let staging = root.join("staging/music");
        let job = staging.join(job_id());
        fs::create_dir_all(&job).unwrap();
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&job, fs::Permissions::from_mode(0o700)).unwrap();
        let staged_music = job.join("music-copy.m4a");
        fs::write(&staged_music, b"Music copy").unwrap();
        let archive_path = root.join("archive.m4a");
        fs::write(&archive_path, b"archive").unwrap();
        let archive = identify_regular_file(&archive_path).unwrap();
        let music = identify_regular_file(&staged_music).unwrap();
        assert!(cleanup_staging_tree(&staging, job_id(), &music, &archive).is_err());
        assert!(staged_music.exists());

        let (music, archive) = create_guards(&root);
        let replacement = root.join("replacement.m4a");
        fs::write(&replacement, b"replacement archive").unwrap();
        fs::rename(&replacement, &archive.path).unwrap();
        fs::write(job.join("original.m4a"), b"staging bytes").unwrap();
        assert!(cleanup_staging_tree(&staging, job_id(), &music, &archive).is_err());
        assert!(job.exists());
        assert_eq!(fs::read(&archive.path).unwrap(), b"replacement archive");
    }

    #[test]
    fn refuses_a_replaced_staging_file_and_allows_a_later_retry() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let staging = root.join("staging/music");
        let job = staging.join(job_id());
        fs::create_dir_all(&job).unwrap();
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&job, fs::Permissions::from_mode(0o700)).unwrap();
        let selected = job.join("original.m4a");
        fs::write(&selected, b"captured staging object").unwrap();
        let (music, archive) = create_guards(&root);

        let replaced = cleanup_staging_tree_with_refresh(&staging, job_id(), || {
            fs::remove_file(&selected).unwrap();
            fs::write(&selected, b"replacement staging object").unwrap();
            Ok((music.clone(), archive.clone()))
        });
        assert!(replaced.is_err());
        assert_eq!(fs::read(&selected).unwrap(), b"replacement staging object");

        cleanup_staging_tree(&staging, job_id(), &music, &archive).unwrap();
        assert!(!job.exists());
    }
}
