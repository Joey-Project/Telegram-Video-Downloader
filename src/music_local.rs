//! Rooted, no-symlink snapshots for local Music sources.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use rustix::fs::{AtFlags, FileType, Mode, OFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[cfg(target_os = "macos")]
use std::ffi::CString;
#[cfg(target_os = "macos")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStrExt;

use super::{LocalMusicFile, MusicCancellation};

const COPY_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_CAPTURE_HASH_ATTEMPTS: usize = 2;
const MAX_SOURCE_KEY_BYTES: usize = 512;
const MAX_FILE_NAME_BYTES: usize = 512;
#[cfg(target_os = "macos")]
const SF_DATALESS: u32 = 0x4000_0000;

/// Immutable source proof captured from the selected file and its rooted directory.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct SourceProof {
    requested_root: PathBuf,
    pub(super) resolved_root: PathBuf,
    root_device: u64,
    root_inode: u64,
    file_device: u64,
    file_inode: u64,
    pub(super) file_size: u64,
    pub(super) sha256: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ObjectIdentity {
    device: u64,
    inode: u64,
    size: u64,
}

pub(super) fn capture_source(
    source_root: &Path,
    relative_path: &Path,
    max_file_bytes: u64,
    cancel: &MusicCancellation,
) -> Result<SourceProof> {
    capture_source_with_hash_hook(source_root, relative_path, max_file_bytes, cancel, |_| {
        Ok(())
    })
}

fn capture_source_with_hash_hook(
    source_root: &Path,
    relative_path: &Path,
    max_file_bytes: u64,
    cancel: &MusicCancellation,
    mut after_hash: impl FnMut(usize) -> Result<()>,
) -> Result<SourceProof> {
    validate_root_path(source_root)?;
    validate_relative_path(relative_path)?;
    let resolved_root = resolve_source_root(source_root)?;
    let root = open_root_directory(&resolved_root)?;
    let root_identity = directory_identity(&root)?;
    let mut source = open_relative_file(&root, relative_path)?;
    let captured_identity = regular_file_identity(&source, max_file_bytes)?;
    let mut hash_index = 0;
    let mut divergences = Vec::with_capacity(MAX_CAPTURE_HASH_ATTEMPTS);

    for attempt in 1..=MAX_CAPTURE_HASH_ATTEMPTS {
        let before = regular_file_identity(&source, max_file_bytes)?;
        ensure_capture_identity("before hashing", captured_identity, before)?;

        let first_sha = hash_file(&mut source, before.size, max_file_bytes, cancel)
            .context("hash local source during stable capture (first pass)")?;
        after_hash(hash_index)?;
        hash_index += 1;
        let after_first = regular_file_identity(&source, max_file_bytes)?;
        ensure_capture_identity("after first hash pass", captured_identity, after_first)?;

        let second_sha = hash_file(&mut source, after_first.size, max_file_bytes, cancel)
            .context("hash local source during stable capture (verification pass)")?;
        after_hash(hash_index)?;
        hash_index += 1;
        let after_second = regular_file_identity(&source, max_file_bytes)?;
        ensure_capture_identity(
            "after verification hash pass",
            captured_identity,
            after_second,
        )?;

        // Validate the same rooted path on every attempt. A digest mismatch may be retried once,
        // but replacement, access, materialization, cancellation, and read failures remain errors.
        verify_bound_path(
            source_root,
            &resolved_root,
            relative_path,
            root_identity,
            captured_identity,
        )
        .with_context(|| format!("revalidate local source path after capture attempt {attempt}"))?;

        if same_sha256(&first_sha, &second_sha) {
            return Ok(SourceProof {
                requested_root: source_root.to_path_buf(),
                resolved_root,
                root_device: root_identity.device,
                root_inode: root_identity.inode,
                file_device: captured_identity.device,
                file_inode: captured_identity.inode,
                file_size: captured_identity.size,
                sha256: second_sha,
            });
        }

        divergences.push((first_sha, second_sha));
    }

    let detail = divergences
        .iter()
        .enumerate()
        .map(|(index, (first, second))| {
            format!(
                "attempt {} first SHA-256 {first}, verification SHA-256 {second}",
                index + 1
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    bail!(
        "local source content remained unstable during bounded capture after {MAX_CAPTURE_HASH_ATTEMPTS} attempts: {detail}"
    )
}

fn ensure_capture_identity(
    phase: &str,
    expected: ObjectIdentity,
    observed: ObjectIdentity,
) -> Result<()> {
    if expected != observed {
        bail!(
            "local source identity changed {phase}: expected device/inode/size {}/{}/{}, observed {}/{}/{}",
            expected.device,
            expected.inode,
            expected.size,
            observed.device,
            observed.inode,
            observed.size,
        );
    }
    Ok(())
}

/// Revalidate the captured filesystem object binding without requiring its old content digest.
/// Explicit recovery may recapture changed bytes from this same bound object, but never from a
/// replacement root or file.
pub(super) fn revalidate_source_binding(file: &LocalMusicFile, max_file_bytes: u64) -> Result<()> {
    validate_source_shape(file, max_file_bytes)?;
    let _ = open_bound_source(file, max_file_bytes, &MusicCancellation::default())?;
    Ok(())
}

/// Compare captured source bindings while allowing an explicitly authorized recapture to change
/// only the content digest. The caller separately checks the logical `source_key`.
pub(super) fn same_source_binding(left: &LocalMusicFile, right: &LocalMusicFile) -> bool {
    left.source_root == right.source_root
        && left.relative_path == right.relative_path
        && left.file_name == right.file_name
        && left.file_size == right.file_size
        && left.proof.requested_root == right.proof.requested_root
        && left.proof.resolved_root == right.proof.resolved_root
        && left.proof.root_device == right.proof.root_device
        && left.proof.root_inode == right.proof.root_inode
        && left.proof.file_device == right.proof.file_device
        && left.proof.file_inode == right.proof.file_inode
        && left.proof.file_size == right.proof.file_size
}

pub(super) fn validate_source_shape(file: &LocalMusicFile, max_file_bytes: u64) -> Result<()> {
    validate_source_key(&file.source_key)?;
    validate_root_path(&file.source_root)?;
    validate_root_path(&file.proof.resolved_root)?;
    validate_relative_path(&file.relative_path)?;
    if file.source_root != file.proof.requested_root {
        bail!("selected local source root no longer matches its captured proof");
    }
    let path_name = file
        .relative_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("selected local file name is not valid UTF-8"))?;
    if file.file_name != path_name
        || file.file_name.is_empty()
        || file.file_name.len() > MAX_FILE_NAME_BYTES
        || file.file_name.chars().any(char::is_control)
    {
        bail!("selected local file name does not match its relative path");
    }
    if file.file_size == 0
        || file.file_size > max_file_bytes
        || file.proof.file_size != file.file_size
        || !is_sha256(&file.proof.sha256)
    {
        bail!("selected local file size or content proof is invalid");
    }
    Ok(())
}

pub(crate) fn validate_source_key(source_key: &str) -> Result<()> {
    if source_key.is_empty()
        || source_key.len() > MAX_SOURCE_KEY_BYTES
        || source_key.chars().any(char::is_control)
    {
        bail!("selected local source key is invalid");
    }
    Ok(())
}

pub(super) fn copy_verified_snapshot(
    selected: &LocalMusicFile,
    destination: &Path,
    max_file_bytes: u64,
    cancel: &MusicCancellation,
) -> Result<()> {
    validate_source_shape(selected, max_file_bytes)?;
    let (root, mut source, source_identity) = open_bound_source(selected, max_file_bytes, cancel)?;
    let mut target = OpenOptions::new()
        .write(true)
        .read(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(destination)
        .with_context(|| format!("create private Music snapshot {}", destination.display()))?;

    let copied_sha = copy_and_hash(&mut source, &mut target, source_identity.size, cancel)?;
    target
        .sync_all()
        .context("sync the private Music source snapshot")?;
    let target_identity = private_snapshot_identity(&target, max_file_bytes)?;
    if target_identity.size != source_identity.size
        || (target_identity.device == source_identity.device
            && target_identity.inode == source_identity.inode)
    {
        bail!("Music staging did not create an independent file snapshot");
    }
    if !same_sha256(&copied_sha, &selected.proof.sha256) {
        bail!(
            "private Music snapshot digest mismatch after copy: expected proof SHA-256 {}, observed copied SHA-256 {}",
            selected.proof.sha256,
            copied_sha,
        );
    }

    // Re-read the held source and staged destination. This binds the copied bytes to the proof
    // and catches in-place mutation during the copy, including same-size writes.
    let source_sha = hash_file(&mut source, source_identity.size, max_file_bytes, cancel)
        .context("re-hash selected source after snapshot copy")?;
    let destination_sha = hash_file(&mut target, target_identity.size, max_file_bytes, cancel)
        .context("re-hash private snapshot after copy")?;
    if !same_sha256(&source_sha, &selected.proof.sha256)
        || !same_sha256(&destination_sha, &selected.proof.sha256)
    {
        bail!(
            "post-copy digest mismatch: expected proof SHA-256 {}, observed source SHA-256 {}, observed snapshot SHA-256 {}",
            selected.proof.sha256,
            source_sha,
            destination_sha,
        );
    }
    if regular_file_identity(&source, max_file_bytes)? != source_identity
        || regular_file_identity(&target, max_file_bytes)? != target_identity
    {
        bail!("selected local file or its private snapshot identity changed during copying");
    }
    verify_bound_path(
        &selected.source_root,
        &selected.proof.resolved_root,
        &selected.relative_path,
        directory_identity(&root)?,
        source_identity,
    )?;
    Ok(())
}

pub(super) fn verify_existing_snapshot(
    selected: &LocalMusicFile,
    destination: &Path,
    max_file_bytes: u64,
    cancel: &MusicCancellation,
) -> Result<()> {
    validate_source_shape(selected, max_file_bytes)?;
    let (root, mut source, source_identity) =
        open_verified_source(selected, max_file_bytes, cancel)?;
    let mut target = open_regular_file(destination)?;
    let target_identity = private_snapshot_identity(&target, max_file_bytes)?;
    if target_identity.size != selected.file_size
        || (target_identity.device == source_identity.device
            && target_identity.inode == source_identity.inode)
    {
        bail!("retained Music snapshot has an invalid size or source identity");
    }
    let source_sha = hash_file(&mut source, source_identity.size, max_file_bytes, cancel)?;
    let target_sha = hash_file(&mut target, target_identity.size, max_file_bytes, cancel)?;
    if !same_sha256(&source_sha, &selected.proof.sha256)
        || !same_sha256(&target_sha, &selected.proof.sha256)
    {
        bail!(
            "retained Music snapshot digest mismatch: expected proof SHA-256 {}, observed source SHA-256 {}, observed snapshot SHA-256 {}",
            selected.proof.sha256,
            source_sha,
            target_sha,
        );
    }
    if regular_file_identity(&source, max_file_bytes)? != source_identity
        || regular_file_identity(&target, max_file_bytes)? != target_identity
    {
        bail!("retained Music snapshot or source changed during verification");
    }
    verify_bound_path(
        &selected.source_root,
        &selected.proof.resolved_root,
        &selected.relative_path,
        directory_identity(&root)?,
        source_identity,
    )?;
    Ok(())
}

fn open_verified_source(
    selected: &LocalMusicFile,
    max_file_bytes: u64,
    cancel: &MusicCancellation,
) -> Result<(File, File, ObjectIdentity)> {
    let (root, mut source, source_identity) = open_bound_source(selected, max_file_bytes, cancel)?;
    let sha256 = hash_file(&mut source, source_identity.size, max_file_bytes, cancel)?;
    if !same_sha256(&sha256, &selected.proof.sha256) {
        bail!("selected local file content changed after selection");
    }
    verify_bound_path(
        &selected.source_root,
        &selected.proof.resolved_root,
        &selected.relative_path,
        directory_identity(&root)?,
        source_identity,
    )?;
    Ok((root, source, source_identity))
}

fn open_bound_source(
    selected: &LocalMusicFile,
    max_file_bytes: u64,
    cancel: &MusicCancellation,
) -> Result<(File, File, ObjectIdentity)> {
    check_cancelled(cancel)?;
    let resolved_root = resolve_source_root(&selected.source_root)?;
    if resolved_root != selected.proof.resolved_root {
        bail!("selected local source root now resolves to a different directory");
    }
    let root = open_root_directory(&resolved_root)?;
    let root_identity = directory_identity(&root)?;
    if root_identity.device != selected.proof.root_device
        || root_identity.inode != selected.proof.root_inode
    {
        bail!("selected local source root was replaced");
    }
    let source = open_relative_file(&root, &selected.relative_path)?;
    let source_identity = regular_file_identity(&source, max_file_bytes)?;
    if source_identity.device != selected.proof.file_device
        || source_identity.inode != selected.proof.file_inode
        || source_identity.size != selected.proof.file_size
    {
        bail!("selected local file object was replaced or resized");
    }
    verify_bound_path(
        &selected.source_root,
        &selected.proof.resolved_root,
        &selected.relative_path,
        root_identity,
        source_identity,
    )?;
    Ok((root, source, source_identity))
}

fn verify_bound_path(
    source_root: &Path,
    resolved_root: &Path,
    relative_path: &Path,
    expected_root: ObjectIdentity,
    expected_file: ObjectIdentity,
) -> Result<()> {
    if resolve_source_root(source_root)? != resolved_root {
        bail!("selected local source root now resolves to a different directory");
    }
    let root = open_root_directory(resolved_root)?;
    let root_identity = directory_identity(&root)?;
    if root_identity.device != expected_root.device || root_identity.inode != expected_root.inode {
        bail!("selected local source root was replaced during verification");
    }
    let file = open_relative_file(&root, relative_path)?;
    if regular_file_identity(&file, expected_file.size)? != expected_file {
        bail!("selected local file path no longer resolves to its captured object");
    }
    Ok(())
}

fn open_root_directory(path: &Path) -> Result<File> {
    validate_root_path(path)?;
    let mut directory = file_from_fd(
        rustix::fs::open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_to_io)
        .context("open filesystem root for local Music source")?,
    );
    for component in path.components().skip(1) {
        let Component::Normal(name) = component else {
            bail!("selected local source root is not normalized");
        };
        let child = rustix::fs::openat(
            &directory,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_to_io)
        .with_context(|| {
            format!(
                "open source root component {} without following links",
                name.to_string_lossy()
            )
        })?;
        directory = file_from_fd(child);
    }
    if !directory
        .metadata()
        .context("inspect selected local source root")?
        .is_dir()
    {
        bail!("selected local source root is not a directory");
    }
    Ok(directory)
}

fn resolve_source_root(path: &Path) -> Result<PathBuf> {
    validate_root_path(path)?;
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect selected local source root {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("selected local source root must be a real directory, not a link");
    }
    let resolved = fs::canonicalize(path)
        .with_context(|| format!("resolve selected local source root {}", path.display()))?;
    validate_root_path(&resolved)?;
    Ok(resolved)
}

fn open_relative_file(root: &File, relative_path: &Path) -> Result<File> {
    validate_relative_path(relative_path)?;
    let components: Vec<_> = relative_path
        .components()
        .map(|component| match component {
            Component::Normal(name) => Ok(name),
            _ => bail!("selected local file path is not normalized"),
        })
        .collect::<Result<_>>()?;
    let (leaf, parents) = components
        .split_last()
        .ok_or_else(|| anyhow!("selected local file path is empty"))?;
    let mut parent = root
        .try_clone()
        .context("duplicate selected source root handle")?;
    for component in parents {
        let child = rustix::fs::openat(
            &parent,
            *component,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_to_io)
        .with_context(|| {
            format!(
                "open selected source directory {} without following links",
                component.to_string_lossy()
            )
        })?;
        parent = file_from_fd(child);
    }
    let metadata = rustix::fs::statat(&parent, *leaf, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(errno_to_io)
        .context("inspect selected local file without following links")?;
    if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile {
        bail!("selected local source is not a regular file");
    }
    reject_dataless_entry(&parent, leaf)?;
    let source = rustix::fs::openat(
        &parent,
        *leaf,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(errno_to_io)
    .context("open selected local file without following links")?;
    let source = file_from_fd(source);
    if !source
        .metadata()
        .context("inspect opened selected local file")?
        .is_file()
    {
        bail!("selected local source changed type while it was opened");
    }
    reject_dataless_file(&source)?;
    Ok(source)
}

fn regular_file_identity(file: &File, max_file_bytes: u64) -> Result<ObjectIdentity> {
    reject_dataless_file(file)?;
    let metadata = file
        .metadata()
        .context("inspect selected local file object")?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > max_file_bytes {
        bail!("selected local file must be a non-empty regular file within the configured limit");
    }
    Ok(ObjectIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        size: metadata.len(),
    })
}

fn private_snapshot_identity(file: &File, max_file_bytes: u64) -> Result<ObjectIdentity> {
    let identity = regular_file_identity(file, max_file_bytes)?;
    let metadata = file
        .metadata()
        .context("inspect private Music snapshot access metadata")?;
    if metadata.uid() != unsafe { libc::geteuid() }
        || metadata.nlink() != 1
        || metadata.mode() & 0o777 != 0o600
    {
        bail!("private Music snapshot ownership, mode, or link count changed");
    }
    Ok(identity)
}

fn directory_identity(directory: &File) -> Result<ObjectIdentity> {
    let metadata = directory
        .metadata()
        .context("inspect selected local source directory object")?;
    if !metadata.is_dir() {
        bail!("selected local source root was replaced by a non-directory");
    }
    Ok(ObjectIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        size: metadata.len(),
    })
}

fn hash_file(
    file: &mut File,
    expected_size: u64,
    max_file_bytes: u64,
    cancel: &MusicCancellation,
) -> Result<String> {
    if expected_size == 0 || expected_size > max_file_bytes {
        bail!("selected local file size is outside the configured limit");
    }
    check_cancelled(cancel)?;
    file.seek(SeekFrom::Start(0))
        .context("seek selected local file for verification")?;
    let mut digest = Sha256::new();
    let mut remaining = expected_size;
    let mut buffer = vec![0; COPY_BUFFER_BYTES];
    while remaining > 0 {
        check_cancelled(cancel)?;
        let limit = usize::try_from(remaining.min(buffer.len() as u64))
            .context("selected local file read size is invalid")?;
        let count = file
            .read(&mut buffer[..limit])
            .context("read selected local file for verification")?;
        if count == 0 {
            bail!("selected local file became shorter while being verified");
        }
        digest.update(&buffer[..count]);
        remaining -= count as u64;
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn copy_and_hash(
    source: &mut File,
    destination: &mut File,
    expected_size: u64,
    cancel: &MusicCancellation,
) -> Result<String> {
    source
        .seek(SeekFrom::Start(0))
        .context("seek selected local file before copying")?;
    let mut digest = Sha256::new();
    let mut remaining = expected_size;
    let mut buffer = vec![0; COPY_BUFFER_BYTES];
    while remaining > 0 {
        check_cancelled(cancel)?;
        let limit = usize::try_from(remaining.min(buffer.len() as u64))
            .context("selected local file copy size is invalid")?;
        let count = source
            .read(&mut buffer[..limit])
            .context("read selected local file while copying")?;
        if count == 0 {
            bail!("selected local file became shorter while being copied");
        }
        destination
            .write_all(&buffer[..count])
            .context("write private Music source snapshot")?;
        digest.update(&buffer[..count]);
        remaining -= count as u64;
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn open_regular_file(path: &Path) -> Result<File> {
    reject_dataless_path(path)?;
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect retained Music snapshot {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("retained Music snapshot is not a regular file");
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path).with_context(|| {
        format!(
            "open retained Music snapshot {} without following links",
            path.display()
        )
    })?;
    private_snapshot_identity(&file, u64::MAX)?;
    reject_dataless_file(&file)?;
    Ok(file)
}

fn validate_root_path(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
    {
        bail!("selected local source root must be an absolute normalized path");
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("selected local file path must stay below its source root");
    }
    Ok(())
}

fn check_cancelled(cancel: &MusicCancellation) -> Result<()> {
    if cancel.is_cancelled() {
        bail!("local Music source snapshot was cancelled");
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn same_sha256(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn file_from_fd(fd: OwnedFd) -> File {
    File::from(fd)
}

fn errno_to_io(error: rustix::io::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(error.raw_os_error())
}

#[cfg(target_os = "macos")]
fn reject_dataless_entry(parent: &File, leaf: &std::ffi::OsStr) -> Result<()> {
    use std::mem::MaybeUninit;

    let leaf = CString::new(leaf.as_bytes()).context("selected local file name contains NUL")?;
    let mut stat = MaybeUninit::<libc::stat>::zeroed();
    // SAFETY: `stat` is valid writable storage and `leaf` is NUL-terminated. The descriptor is
    // held open and AT_SYMLINK_NOFOLLOW prevents this metadata check from following a link.
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            leaf.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("inspect selected local file flags");
    }
    // SAFETY: fstatat initialized the structure after returning success.
    let stat = unsafe { stat.assume_init() };
    if stat.st_flags & SF_DATALESS != 0 {
        bail!("selected local source is not materialized; download it in the torrent client first");
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn reject_dataless_entry(_parent: &File, _leaf: &std::ffi::OsStr) -> Result<()> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn reject_dataless_file(file: &File) -> Result<()> {
    use std::os::macos::fs::MetadataExt as MacMetadataExt;

    let metadata = file
        .metadata()
        .context("inspect selected local file flags")?;
    if metadata.st_flags() & SF_DATALESS != 0 {
        bail!("selected local source is not materialized; download it in the torrent client first");
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn reject_dataless_file(_file: &File) -> Result<()> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn reject_dataless_path(path: &Path) -> Result<()> {
    use std::os::macos::fs::MetadataExt;

    let metadata = fs::symlink_metadata(path).context("inspect retained Music snapshot flags")?;
    if (metadata.st_flags() as u32) & SF_DATALESS != 0 {
        bail!("retained Music snapshot is not materialized; staging was preserved");
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn reject_dataless_path(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_CAPTURE_HASH_ATTEMPTS, MusicCancellation, capture_source_with_hash_hook,
        revalidate_source_binding, same_source_binding,
    };
    use sha2::{Digest, Sha256};
    use std::fs::{self, FileTimes, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    fn write_in_place(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        let mut file = OpenOptions::new().write(true).truncate(true).open(path)?;
        file.write_all(bytes)?;
        file.sync_all()
    }

    fn expected_sha256(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    #[test]
    fn capture_retries_same_identity_same_size_content_divergence_once() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let path = root.join("selected.flac");
        let original = vec![b'a'; 256 * 1024];
        let replacement = vec![b'b'; original.len()];
        fs::write(&path, &original).unwrap();
        let before = fs::metadata(&path).unwrap();
        let cancellation = MusicCancellation::default();
        let mut changed = false;

        let proof = capture_source_with_hash_hook(
            root,
            Path::new("selected.flac"),
            1024 * 1024,
            &cancellation,
            |hash_index| {
                if hash_index == 0 && !changed {
                    write_in_place(&path, &replacement)?;
                    changed = true;
                }
                Ok(())
            },
        )
        .expect("a single same-size mutation should be retried and recaptured stably");

        let after = fs::metadata(&path).unwrap();
        assert!(changed);
        assert_eq!(proof.sha256, expected_sha256(&replacement));
        assert_eq!(proof.file_size, replacement.len() as u64);
        assert_eq!(
            (before.dev(), before.ino(), before.len()),
            (after.dev(), after.ino(), after.len()),
            "the mutation must preserve object identity and size",
        );
    }

    #[test]
    fn capture_accepts_metadata_transition_when_content_stays_equal() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let path = root.join("selected.flac");
        let bytes = b"same audio bytes across provider metadata transition";
        fs::write(&path, bytes).unwrap();
        let original_modified = fs::metadata(&path).unwrap().modified().unwrap();
        let cancellation = MusicCancellation::default();
        let mut changed_time = false;

        let proof = capture_source_with_hash_hook(
            root,
            Path::new("selected.flac"),
            1024 * 1024,
            &cancellation,
            |hash_index| {
                if hash_index == 0 && !changed_time {
                    let file = OpenOptions::new().read(true).write(true).open(&path)?;
                    file.set_times(FileTimes::new().set_modified(
                        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
                    ))?;
                    changed_time = true;
                }
                Ok(())
            },
        )
        .expect("mtime changes alone must not invalidate stable source content");

        let modified_after = fs::metadata(&path).unwrap().modified().unwrap();
        assert!(changed_time);
        assert_ne!(original_modified, modified_after);
        assert_eq!(proof.sha256, expected_sha256(bytes));
    }

    #[test]
    fn capture_bounds_unstable_content_and_reports_both_observed_digests() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let path = root.join("selected.flac");
        let first_bytes = vec![b'a'; 64 * 1024];
        let second_bytes = vec![b'b'; first_bytes.len()];
        fs::write(&path, &first_bytes).unwrap();
        let cancellation = MusicCancellation::default();
        let mut writes = 0;

        let error = capture_source_with_hash_hook(
            root,
            Path::new("selected.flac"),
            1024 * 1024,
            &cancellation,
            |hash_index| {
                let bytes = if hash_index % 2 == 0 {
                    &second_bytes
                } else {
                    &first_bytes
                };
                write_in_place(&path, bytes)?;
                writes += 1;
                Ok(())
            },
        )
        .expect_err("repeated same-size writes must exhaust the bounded capture");

        let chain = format!("{error:#}");
        assert_eq!(writes, MAX_CAPTURE_HASH_ATTEMPTS * 2);
        assert!(chain.contains("unstable during bounded capture"), "{chain}");
        assert!(chain.contains(&expected_sha256(&first_bytes)), "{chain}");
        assert!(chain.contains(&expected_sha256(&second_bytes)), "{chain}");
    }

    #[test]
    fn capture_does_not_retry_cancellation_as_content_divergence() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let path = root.join("selected.flac");
        fs::write(&path, b"bounded cancellation fixture").unwrap();
        let cancellation = MusicCancellation::default();
        let mut completed_hashes = 0;

        let error = capture_source_with_hash_hook(
            root,
            Path::new("selected.flac"),
            1024 * 1024,
            &cancellation,
            |_| {
                completed_hashes += 1;
                cancellation.cancel();
                Ok(())
            },
        )
        .expect_err("cancellation must propagate without a stability retry");

        assert_eq!(completed_hashes, 1);
        assert!(format!("{error:#}").contains("cancelled"));
    }

    #[test]
    fn copy_digest_failure_reports_expected_and_observed_hashes() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("source");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("selected.flac");
        let original = vec![b'a'; 64 * 1024];
        let replacement = vec![b'b'; original.len()];
        fs::write(&path, &original).unwrap();
        let proof = super::capture_source(
            &root,
            Path::new("selected.flac"),
            1024 * 1024,
            &MusicCancellation::default(),
        )
        .unwrap();
        let selected = source_file(proof, &root);
        write_in_place(&path, &replacement).unwrap();

        let error = super::copy_verified_snapshot(
            &selected,
            &directory.path().join("original.flac"),
            1024 * 1024,
            &MusicCancellation::default(),
        )
        .expect_err("copying bytes that differ from the captured proof must fail");

        let chain = format!("{error:#}");
        assert!(chain.contains("private Music snapshot digest mismatch after copy"));
        assert!(chain.contains(&expected_sha256(&original)));
        assert!(chain.contains(&expected_sha256(&replacement)));
    }

    #[test]
    fn binding_revalidation_allows_changed_content_on_the_same_object() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let path = root.join("selected.flac");
        let original = b"captured source bytes";
        let replacement = b"replaced source bytes";
        assert_eq!(original.len(), replacement.len());
        fs::write(&path, original).unwrap();
        let before = fs::metadata(&path).unwrap();
        let proof = super::capture_source(
            root,
            Path::new("selected.flac"),
            1024 * 1024,
            &MusicCancellation::default(),
        )
        .unwrap();
        let source = source_file(proof, root);
        write_in_place(&path, replacement).unwrap();
        let fresh_proof = super::capture_source(
            root,
            Path::new("selected.flac"),
            1024 * 1024,
            &MusicCancellation::default(),
        )
        .unwrap();
        let fresh_source = source_file(fresh_proof, root);
        let after = fs::metadata(&path).unwrap();

        revalidate_source_binding(&source, 1024 * 1024)
            .expect("explicit recovery may recapture changed bytes from the same object");
        assert_ne!(source.proof.sha256, fresh_source.proof.sha256);
        assert!(same_source_binding(&source, &fresh_source));
        assert_eq!(
            (before.dev(), before.ino(), before.len()),
            (after.dev(), after.ino(), after.len())
        );
    }

    #[test]
    fn binding_revalidation_rejects_replaced_file_object() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let path = root.join("selected.flac");
        let original = b"captured source bytes";
        let replacement = b"replaced source bytes";
        assert_eq!(original.len(), replacement.len());
        fs::write(&path, original).unwrap();
        let proof = super::capture_source(
            root,
            Path::new("selected.flac"),
            1024 * 1024,
            &MusicCancellation::default(),
        )
        .unwrap();
        let selected = source_file(proof, root);
        let old_path = root.join("old-selected.flac");
        fs::rename(&path, &old_path).unwrap();
        fs::write(&path, replacement).unwrap();
        let replacement_proof = super::capture_source(
            root,
            Path::new("selected.flac"),
            1024 * 1024,
            &MusicCancellation::default(),
        )
        .unwrap();
        let replacement_source = source_file(replacement_proof, root);

        let error = revalidate_source_binding(&selected, 1024 * 1024)
            .expect_err("recovery must reject a replacement object even at the same size");
        assert!(format!("{error:#}").contains("replaced or resized"));
        assert!(!same_source_binding(&selected, &replacement_source));
    }

    fn source_file(source: super::SourceProof, root: &Path) -> super::LocalMusicFile {
        super::LocalMusicFile {
            source_key: "test-source".to_string(),
            source_root: root.to_path_buf(),
            relative_path: Path::new("selected.flac").to_path_buf(),
            file_name: "selected.flac".to_string(),
            file_size: source.file_size,
            proof: source,
        }
    }
}
