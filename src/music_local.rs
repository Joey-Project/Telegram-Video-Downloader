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
    validate_root_path(source_root)?;
    validate_relative_path(relative_path)?;
    let resolved_root = resolve_source_root(source_root)?;
    let root = open_root_directory(&resolved_root)?;
    let root_identity = directory_identity(&root)?;
    let mut source = open_relative_file(&root, relative_path)?;
    let before = regular_file_identity(&source, max_file_bytes)?;
    let sha256 = hash_file(&mut source, before.size, max_file_bytes, cancel)?;
    let after = regular_file_identity(&source, max_file_bytes)?;
    if before != after {
        bail!("selected local file identity or size changed while it was captured");
    }
    verify_bound_path(
        source_root,
        &resolved_root,
        relative_path,
        root_identity,
        before,
    )?;
    Ok(SourceProof {
        requested_root: source_root.to_path_buf(),
        resolved_root,
        root_device: root_identity.device,
        root_inode: root_identity.inode,
        file_device: before.device,
        file_inode: before.inode,
        file_size: before.size,
        sha256,
    })
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
        bail!("selected local file content changed before it could be snapshotted");
    }

    // Re-read the held source and staged destination. This binds the copied bytes to the proof
    // and catches in-place mutation during the copy, including same-size writes.
    let source_sha = hash_file(&mut source, source_identity.size, max_file_bytes, cancel)?;
    let destination_sha = hash_file(&mut target, target_identity.size, max_file_bytes, cancel)?;
    if !same_sha256(&source_sha, &selected.proof.sha256)
        || !same_sha256(&destination_sha, &selected.proof.sha256)
    {
        bail!("selected local file or its private snapshot changed during copying");
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
        bail!("retained Music snapshot does not match the selected local source proof");
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
