use std::fmt;

use anyhow::{Context, Result};

use crate::safe_fs::RootedFs;

const STATE_AND_SIDECAR_RESERVE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskSpaceError {
    pub(crate) available_bytes: u64,
    pub(crate) required_bytes: u64,
    pub(crate) phase: String,
}

impl fmt::Display for DiskSpaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Insufficient disk space during {}: {} available, {} required (including a 256 MiB reserve for state and sidecars). Clear space, then Resume the download.",
            self.phase,
            human_bytes(self.available_bytes),
            human_bytes(self.required_bytes),
        )
    }
}

impl std::error::Error for DiskSpaceError {}

pub(crate) fn ensure_disk_space(root: &RootedFs, additional_bytes: u64, phase: &str) -> Result<()> {
    let available_bytes = root
        .available_space_bytes()
        .with_context(|| format!("failed to query available disk space during {phase}"))?;
    let required_bytes = required_space_bytes(additional_bytes, STATE_AND_SIDECAR_RESERVE_BYTES);
    if !has_sufficient_space(
        available_bytes,
        additional_bytes,
        STATE_AND_SIDECAR_RESERVE_BYTES,
    ) {
        return Err(DiskSpaceError {
            available_bytes,
            required_bytes,
            phase: phase.to_owned(),
        }
        .into());
    }
    Ok(())
}

pub(crate) fn is_disk_space_failure(error: &anyhow::Error) -> bool {
    if error.downcast_ref::<DiskSpaceError>().is_some() {
        return true;
    }

    for cause in error.chain() {
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            #[cfg(unix)]
            if matches!(
                io_error.raw_os_error(),
                Some(libc::ENOSPC) | Some(libc::EDQUOT)
            ) {
                return true;
            }
        }
    }

    let message = format!("{error:#}").to_ascii_lowercase();
    [
        "no space left on device",
        "insufficient disk space",
        "disk is full",
        "disk full",
        "disk quota exceeded",
        "os error 28",
        "os error: 28",
        "os error 122",
        "os error: 122",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn required_space_bytes(additional_bytes: u64, reserve_bytes: u64) -> u64 {
    additional_bytes.saturating_add(reserve_bytes)
}

fn has_sufficient_space(available_bytes: u64, additional_bytes: u64, reserve_bytes: u64) -> bool {
    additional_bytes
        .checked_add(reserve_bytes)
        .is_some_and(|required| available_bytes >= required)
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if bytes < 1024 {
        return format!("{bytes} bytes");
    }

    let mut value = bytes as f64;
    let mut unit_index = 0;
    while value >= 1024.0 && unit_index + 1 < UNITS.len() {
        value /= 1024.0;
        unit_index += 1;
    }
    format!("{value:.1} {}", UNITS[unit_index])
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use anyhow::anyhow;

    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "telegram-video-downloader-disk-space-{label}-{}-{stamp}",
            std::process::id()
        ))
    }

    #[test]
    fn threshold_accepts_equality_and_rejects_an_undershoot() {
        assert!(has_sufficient_space(10, 7, 3));
        assert!(!has_sufficient_space(9, 7, 3));
    }

    #[test]
    fn threshold_saturates_required_bytes_on_overflow() {
        assert_eq!(required_space_bytes(u64::MAX, 1), u64::MAX);
        assert!(!has_sufficient_space(u64::MAX - 1, u64::MAX, 1));
        assert!(!has_sufficient_space(u64::MAX, u64::MAX, 1));
    }

    #[test]
    fn recognizes_typed_io_and_ipc_disk_space_failures() {
        let typed: anyhow::Error = DiskSpaceError {
            available_bytes: 0,
            required_bytes: 1,
            phase: "test".to_owned(),
        }
        .into();
        assert!(is_disk_space_failure(&typed));

        #[cfg(unix)]
        for errno in [libc::ENOSPC, libc::EDQUOT] {
            let error = anyhow!(std::io::Error::from_raw_os_error(errno));
            assert!(is_disk_space_failure(&error));
        }

        for message in [
            "worker failed: No space left on device",
            "remote writer: Disk quota exceeded",
            "ipc error: os error 28",
            "ipc error: os error 122",
        ] {
            assert!(is_disk_space_failure(&anyhow!("{message}")));
        }
        assert!(!is_disk_space_failure(&anyhow!("network timeout")));
    }

    #[test]
    fn failed_capacity_query_is_not_reported_as_insufficient_space() {
        let root = temp_dir("query-failure");
        let moved_root = root.with_extension("moved");
        fs::create_dir_all(&root).expect("root should create");
        let rooted = RootedFs::new(&root).expect("root should bind");
        fs::rename(&root, &moved_root).expect("root should move");

        let error = ensure_disk_space(&rooted, 0, "test phase")
            .expect_err("query should fail after the configured root is moved");

        assert!(error.downcast_ref::<DiskSpaceError>().is_none());
        drop(rooted);
        fs::remove_dir_all(moved_root).expect("moved test root should be removed");
    }

    #[test]
    fn insufficient_space_error_includes_phase_sizes_and_resume_hint() {
        let error = DiskSpaceError {
            available_bytes: 512,
            required_bytes: 1024,
            phase: "video transfer".to_owned(),
        };
        let message = error.to_string();

        assert!(message.contains("Insufficient disk space"));
        assert!(message.contains("video transfer"));
        assert!(message.contains("512 bytes available"));
        assert!(message.contains("1.0 KiB required"));
        assert!(message.contains("Clear space, then Resume"));
    }
}
