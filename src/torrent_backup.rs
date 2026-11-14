//! Read-only access to qBittorrent's saved `BT_backup` metainfo files.

#[cfg(test)]
use std::fs;
use std::path::{Component, Path};

use anyhow::{Context, Result, bail, ensure};

use crate::safe_fs::RootedFs;

/// Reads `<hash>.torrent` from an existing, absolute qBittorrent backup directory.
///
/// The returned bytes are later parsed and checked against the requested info hash by the
/// torrent manager. This function protects the opened file object and its size while reading;
/// mutable timestamps and directory-entry churn are not treated as file replacement.
pub(crate) fn read_metainfo(dir: &Path, hash: &str, max_bytes: usize) -> Result<Vec<u8>> {
    validate_absolute_directory_path(dir)?;
    ensure!(
        valid_hash(hash),
        "torrent backup hash must be 40 or 64 hexadecimal characters"
    );
    ensure!(max_bytes > 0, "torrent backup read limit must be positive");
    let max_bytes_u64 =
        u64::try_from(max_bytes).context("torrent backup read limit is too large")?;

    let normalized_hash = hash.to_ascii_lowercase();
    let file_name = format!("{normalized_hash}.torrent");
    let file_path = dir.join(file_name);

    // Root at `/` so every configured path component is opened without following links.
    let root = RootedFs::new(Path::new("/"))
        .context("open filesystem root for qBittorrent backup access")?;
    root.validate_configured_root()
        .context("validate qBittorrent backup directory")?;

    let entry_before = root
        .entry_identity(&file_path)
        .context("inspect qBittorrent backup metainfo entry")?
        .ok_or_else(|| anyhow::anyhow!("qBittorrent backup metainfo file is missing"))?;
    ensure!(
        entry_before.is_file(),
        "qBittorrent backup metainfo is not a regular file"
    );

    let file = root
        .open_bound_file(&file_path)
        .context("open qBittorrent backup metainfo without following links")?
        .ok_or_else(|| anyhow::anyhow!("qBittorrent backup metainfo file is missing"))?;
    let identity = file.identity();
    ensure!(
        identity == entry_before,
        "qBittorrent backup metainfo identity changed while opening"
    );

    let size_before = file
        .byte_len()
        .context("inspect qBittorrent backup metainfo size")?;
    ensure!(
        size_before <= max_bytes_u64,
        "qBittorrent backup metainfo exceeds the configured size limit"
    );
    let bytes = file
        .read_limited(max_bytes)
        .context("read qBittorrent backup metainfo within the configured size limit")?;
    let size_after = file
        .byte_len()
        .context("recheck qBittorrent backup metainfo size")?;
    let bytes_len =
        u64::try_from(bytes.len()).context("torrent backup read length is too large")?;
    ensure!(
        file.identity() == identity && size_before == size_after && bytes_len == size_after,
        "qBittorrent backup metainfo changed while reading"
    );
    ensure!(
        root.entry_identity(&file_path)? == Some(identity),
        "qBittorrent backup metainfo entry was replaced while reading"
    );
    root.validate_configured_root()
        .context("revalidate qBittorrent backup directory")?;
    validate_absolute_directory_path(dir)?;

    Ok(bytes)
}

fn valid_hash(hash: &str) -> bool {
    matches!(hash.len(), 40 | 64) && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Rejects relative and non-normalized inputs; `RootedFs` performs the component-by-component
/// no-follow directory opens for the actual read.
fn validate_absolute_directory_path(path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute(),
        "qBittorrent backup directory must be absolute"
    );

    let mut saw_root = false;
    for component in path.components() {
        match component {
            Component::RootDir if !saw_root => saw_root = true,
            Component::Normal(_) if saw_root => {}
            Component::RootDir
            | Component::Normal(_)
            | Component::CurDir
            | Component::ParentDir
            | Component::Prefix(_) => {
                bail!("qBittorrent backup directory path is not normalized")
            }
        }
    }

    ensure!(
        saw_root,
        "qBittorrent backup directory path is not normalized"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;

    use tempfile::tempdir_in;

    use super::*;

    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";

    fn canonical_tempdir() -> (tempfile::TempDir, PathBuf) {
        let temp_root = std::env::temp_dir()
            .canonicalize()
            .expect("system temporary root should canonicalize");
        let directory = tempdir_in(temp_root).expect("test directory should create");
        let canonical = directory
            .path()
            .canonicalize()
            .expect("test directory path should canonicalize");
        assert_eq!(canonical, directory.path());
        (directory, canonical)
    }

    #[test]
    fn reads_the_exact_hash_named_torrent_within_limit() {
        let (_directory, backup) = canonical_tempdir();
        let bytes = b"torrent metainfo bytes";
        fs::write(backup.join(format!("{HASH}.torrent")), bytes)
            .expect("test metainfo should write");

        assert_eq!(
            read_metainfo(&backup, &HASH.to_ascii_uppercase(), 128)
                .expect("uppercase hash should normalize for the backup filename"),
            bytes
        );

        let v2_hash = "abcdef0123456789".repeat(4);
        let v2_bytes = b"v2 torrent metainfo";
        fs::write(backup.join(format!("{v2_hash}.torrent")), v2_bytes)
            .expect("v2 metainfo should write");
        assert_eq!(
            read_metainfo(&backup, &v2_hash, 128).expect("64-digit metainfo should read"),
            v2_bytes
        );
    }

    #[test]
    fn rejects_oversized_and_missing_metainfo() {
        let (_directory, backup) = canonical_tempdir();
        fs::write(backup.join(format!("{HASH}.torrent")), b"12345")
            .expect("test metainfo should write");
        assert!(read_metainfo(&backup, HASH, 4).is_err());
        assert!(read_metainfo(&backup, &"a".repeat(40), 64).is_err());
    }

    #[test]
    fn rejects_non_absolute_or_symlink_directory_paths() {
        let (_directory, backup) = canonical_tempdir();
        assert!(read_metainfo(Path::new("relative/BT_backup"), HASH, 64).is_err());
        assert!(read_metainfo(&backup.join("../escape"), HASH, 64).is_err());

        let alias = backup
            .parent()
            .expect("temporary directory should have a parent")
            .join(format!(
                "{}-alias",
                backup.file_name().unwrap().to_string_lossy()
            ));
        symlink(&backup, &alias).expect("backup alias should create");
        assert!(read_metainfo(&alias, HASH, 64).is_err());
    }

    #[test]
    fn rejects_invalid_hashes_and_symlink_leaf() {
        let (_directory, backup) = canonical_tempdir();
        assert!(read_metainfo(&backup, "../escape", 64).is_err());
        assert!(read_metainfo(&backup, &"g".repeat(40), 64).is_err());

        let target = backup.join("target.torrent");
        fs::write(&target, b"linked bytes").expect("symlink target should write");
        symlink(&target, backup.join(format!("{HASH}.torrent")))
            .expect("metainfo symlink should create");
        assert!(read_metainfo(&backup, HASH, 64).is_err());
    }
}
