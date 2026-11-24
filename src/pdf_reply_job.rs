//! Reply once with the exact verified PDF after its queue record is durable.

use crate::config::AppConfig;
use crate::queue::{TaskRecord, TaskStatus};
use crate::router::JobRequest;
use crate::safe_fs::{BoundEntry, EntryIdentity, RootedFs};
use crate::{media_libraries, social_media_job};
use anyhow::{Context, Result, anyhow, bail, ensure};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(1);
const MAX_REPLY_FILE_BYTES: u64 = 512 * 1024 * 4000;

pub(crate) fn admission_bot_id(
    config: &AppConfig,
    is_private_chat: bool,
    chat_id: i64,
    submitter_user_id: Option<i64>,
    message_id: i64,
) -> Option<i64> {
    if !is_private_chat
        || !social_media_job::owner_matches(config, chat_id, submitter_user_id, message_id)
    {
        return None;
    }
    social_media_job::bot_id(config)
}

pub(crate) async fn send_completed_pdf_reply(
    config: &AppConfig,
    admitted_task: &TaskRecord,
    completed_task: &TaskRecord,
    saved_location: &str,
    primary_media_paths: &[PathBuf],
) -> Result<()> {
    let bot_id = social_media_job::bot_id(config)
        .context("configured Telegram bot identity is unavailable")?;
    ensure!(
        admitted_task.pdf_reply_bot_id == Some(bot_id)
            && completed_task.pdf_reply_bot_id == Some(bot_id),
        "PDF reply is not bound to the admitting Telegram bot"
    );
    ensure!(
        social_media_job::owner_matches(
            config,
            admitted_task.chat_id,
            admitted_task.submitter_user_id,
            admitted_task.message_id,
        ),
        "PDF replies require the configured private owner and original Telegram message"
    );
    ensure!(
        admitted_task.id == completed_task.id
            && admitted_task.generation == completed_task.generation
            && admitted_task.chat_id == completed_task.chat_id
            && admitted_task.message_id == completed_task.message_id
            && completed_task.status == TaskStatus::Completed
            && matches!(&admitted_task.job, JobRequest::Pdf { .. })
            && matches!(&completed_task.job, JobRequest::Pdf { .. }),
        "PDF reply does not match the durably completed task"
    );
    ensure!(
        completed_task.saved_location.as_deref() == Some(saved_location),
        "completed PDF path does not match the saved job report"
    );
    ensure!(
        primary_media_paths.len() == 1,
        "completed PDF job must contain exactly one saved document"
    );

    let source_path = &primary_media_paths[0];
    ensure!(
        source_path.display().to_string() == saved_location,
        "completed PDF path does not match the saved job report"
    );
    let file_name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("completed PDF has an invalid filename")?;
    ensure!(
        file_name.to_ascii_lowercase().ends_with(".pdf"),
        "completed document is not named as a PDF"
    );
    let hash_key = source_path.display().to_string();
    let expected_sha256 = completed_task
        .primary_media_hashes
        .get(&hash_key)
        .context("completed PDF has no persisted per-file digest")?;
    ensure!(
        expected_sha256.len() == 64 && expected_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "completed PDF digest is invalid"
    );

    let pdf_root_path = config.downloads.pdf_dir.clone();
    let source_path = source_path.clone();
    let file_name = file_name.to_owned();
    let expected_sha256 = expected_sha256.to_owned();
    let copy_file_name = file_name.clone();
    let copy_expected_sha256 = expected_sha256.clone();
    let (scratch, scratch_path) = tokio::task::spawn_blocking(move || {
        prepare_verified_pdf_copy(
            &pdf_root_path,
            &source_path,
            &copy_file_name,
            &copy_expected_sha256,
        )
    })
    .await
    .context("PDF reply copy worker failed to join")??;
    let random_id = reply_random_id(admitted_task, &expected_sha256);
    let result = social_media_job::send_verified_file_reply(
        config,
        admitted_task.chat_id,
        admitted_task.message_id,
        random_id,
        &scratch_path,
        &file_name,
        &expected_sha256,
    )
    .await;
    drop(scratch);
    result
}

fn prepare_verified_pdf_copy(
    pdf_root_path: &Path,
    source_path: &Path,
    file_name: &str,
    expected_sha256: &str,
) -> Result<(PdfReplyScratch, PathBuf)> {
    let pdf_root = RootedFs::new(pdf_root_path)?;
    let bound_source_path = path_bound_under_root(&pdf_root, source_path)?;
    let source_entry = pdf_root.bind_entry(&bound_source_path, false)?;
    let source = pdf_root
        .open_bound_file(&bound_source_path)?
        .context("completed PDF is missing")?;
    let source_identity = source.identity();
    ensure!(
        pdf_root.bound_entry_identity(&source_entry)? == Some(source_identity),
        "completed PDF path changed while opening"
    );
    let source_size = source.byte_len()?;
    ensure!(
        (1..=MAX_REPLY_FILE_BYTES).contains(&source_size),
        "completed PDF is empty or exceeds the Telegram upload limit"
    );

    let mut scratch = PdfReplyScratch::create()?;
    let scratch_path = scratch.copy_verified_pdf(
        &source,
        &source_entry,
        source_size,
        &pdf_root,
        file_name,
        expected_sha256,
    )?;
    Ok((scratch, scratch_path))
}

fn path_bound_under_root(root: &RootedFs, path: &Path) -> Result<PathBuf> {
    let logical_root = root.logical_root_path();
    let canonical_root = root.root_path();
    let normalized_logical_root = media_libraries::normalize_trusted_macos_alias(logical_root)?;
    let normalized_canonical_root = media_libraries::normalize_trusted_macos_alias(canonical_root)?;
    let normalized_path = media_libraries::normalize_trusted_macos_alias(path)?;
    let relative = normalized_path
        .strip_prefix(&normalized_logical_root)
        .or_else(|_| normalized_path.strip_prefix(&normalized_canonical_root))
        .map_err(|_| anyhow!("completed PDF is outside the configured PDF directory"))?;
    ensure!(
        relative
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_))),
        "completed PDF path is not a normal path beneath the configured PDF directory"
    );
    Ok(logical_root.join(relative))
}

fn reply_random_id(task: &TaskRecord, sha256: &str) -> i64 {
    let mut hash = Sha256::new();
    hash.update(b"pdf-original-v1\0");
    hash.update(task.chat_id.to_le_bytes());
    hash.update(task.message_id.to_le_bytes());
    hash.update(task.id.as_bytes());
    hash.update(sha256.as_bytes());
    let mut prefix = [0; 8];
    prefix.copy_from_slice(&hash.finalize()[..8]);
    let random_id = i64::from_le_bytes(prefix);
    if random_id == 0 { 1 } else { random_id }
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct PdfReplyScratch {
    parent_root: RootedFs,
    directory_path: PathBuf,
    directory_entry: BoundEntry,
    directory_identity: EntryIdentity,
    file_entry: Option<BoundEntry>,
    file_identity: Option<EntryIdentity>,
    cleaned: bool,
}

impl PdfReplyScratch {
    fn create() -> Result<Self> {
        // The MTProto helper opens each component without following symlinks. Bind the
        // scratch to the same canonical root we pass to that helper, including on macOS.
        let temp_root = std::env::temp_dir()
            .canonicalize()
            .context("failed to resolve the PDF reply temporary directory")?;
        let parent_root = RootedFs::new(&temp_root)?;
        let mut attempt = 0;
        while attempt < 32 {
            attempt += 1;
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let counter = SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
            let directory_path = parent_root.root_path().join(format!(
                "telegram-video-downloader-pdf-reply-{}-{timestamp:x}-{counter:x}",
                std::process::id()
            ));
            let Some(directory_identity) = parent_root.create_dir(&directory_path, 0o700)? else {
                continue;
            };
            let directory_entry = parent_root.bind_entry(&directory_path, false)?;
            let scratch = Self {
                parent_root,
                directory_path,
                directory_entry,
                directory_identity,
                file_entry: None,
                file_identity: None,
                cleaned: false,
            };
            scratch.parent_root.validate_private_bound_directory(
                &scratch.directory_entry,
                scratch.directory_identity,
                0o700,
            )?;
            return Ok(scratch);
        }
        bail!("could not create a unique private PDF reply directory")
    }

    fn copy_verified_pdf(
        &mut self,
        source: &crate::safe_fs::BoundFile,
        source_entry: &BoundEntry,
        source_size: u64,
        source_root: &RootedFs,
        file_name: &str,
        expected_sha256: &str,
    ) -> Result<PathBuf> {
        let source_identity = source.identity();
        self.parent_root.validate_private_bound_directory(
            &self.directory_entry,
            self.directory_identity,
            0o700,
        )?;
        let path = self.directory_path.join(file_name);
        let file_identity = self.parent_root.create_migration_staging_file(&path)?;
        let file_entry = self.parent_root.bind_entry(&path, false)?;
        self.file_entry = Some(file_entry.clone());
        self.file_identity = Some(file_identity);
        let destination = self
            .parent_root
            .open_bound_file_read_write_if_identity(&file_entry, file_identity)?;
        destination.validate_private_single_link(0o600)?;

        let mut source_copy = source.duplicate_std_file()?;
        source_copy
            .seek(SeekFrom::Start(0))
            .context("failed to seek the bound completed PDF")?;
        let mut bounded_source_copy = source_copy.take(source_size);
        let mut destination_copy = destination.duplicate_std_file()?;
        let copied_bytes = std::io::copy(&mut bounded_source_copy, &mut destination_copy)
            .context("failed to copy the completed PDF into private reply staging")?;
        drop(destination_copy);
        ensure!(
            copied_bytes == source_size && destination.byte_len()? == source_size,
            "completed PDF size changed while making its private reply copy"
        );
        ensure!(
            self.parent_root.bound_entry_identity(&file_entry)? == Some(file_identity),
            "private PDF reply copy changed while writing"
        );
        ensure!(
            hex_digest(&destination.sha256()?) == expected_sha256,
            "private PDF reply copy does not match the completed digest"
        );
        ensure!(
            source_root.bound_entry_identity(source_entry)? == Some(source_identity)
                && source.byte_len()? == source_size,
            "completed PDF changed while making its private reply copy"
        );
        self.parent_root.validate_private_bound_directory(
            &self.directory_entry,
            self.directory_identity,
            0o700,
        )?;
        Ok(path)
    }

    fn cleanup_after_send(&mut self) {
        if let Err(error) = self.cleanup() {
            tracing::warn!(
                path = %self.directory_path.display(),
                error = %error,
                "could not remove private PDF reply staging"
            );
        }
    }

    fn cleanup(&mut self) -> Result<()> {
        if self.cleaned {
            return Ok(());
        }
        if let (Some(entry), Some(identity)) = (&self.file_entry, self.file_identity) {
            self.parent_root
                .remove_bound_file_if_identity(entry, identity)?;
        }
        self.parent_root
            .remove_bound_dir_if_identity(&self.directory_entry, self.directory_identity)?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for PdfReplyScratch {
    fn drop(&mut self) {
        self.cleanup_after_send();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_binds_only_the_configured_private_owner() {
        let mut config = AppConfig::for_test();
        config.telegram.token = "987654:test-token".into();
        config.telegram.allowed_chat_ids = vec![123_456];
        config.telegram.allow_all_chats = false;

        assert_eq!(
            admission_bot_id(&config, true, 123_456, Some(123_456), 77),
            Some(987_654)
        );
        assert_eq!(
            admission_bot_id(&config, false, 123_456, Some(123_456), 77),
            None
        );
        assert_eq!(
            admission_bot_id(&config, true, 123_456, Some(123_457), 77),
            None
        );
        assert_eq!(
            admission_bot_id(&config, true, 123_457, Some(123_457), 77),
            None
        );
        assert_eq!(
            admission_bot_id(&config, true, 123_456, Some(123_456), 0),
            None
        );

        config.telegram.allow_all_chats = true;
        assert_eq!(
            admission_bot_id(&config, true, 123_456, Some(123_456), 77),
            None
        );
    }
}
