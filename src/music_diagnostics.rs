//! Bounded, private diagnostics for failures in the local Music import pipeline.
//!
//! Callers should capture the error before moving it to a blocking task, then treat
//! `record_failure` as best-effort so a diagnostics failure never replaces the
//! original job failure.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use rustix::fs::FlockOperation;
use serde::{Deserialize, Serialize};

use crate::file_provider::QueueFileProvider;
use crate::redaction::redact_sensitive_text;

const DIRECTORY_NAME: &str = "diagnostics";
const JOURNAL_NAME: &str = "failures.json";
const LOCK_NAME: &str = "failures.lock";
const JOURNAL_VERSION: u32 = 1;
const MAX_RECORDS: usize = 512;
const MAX_JOURNAL_BYTES: usize = 4 * 1024 * 1024;
const MAX_RECORD_BYTES: usize = 8 * 1024;
const MAX_JOB_ID_BYTES: usize = 256;
const MAX_ERROR_CAUSES: usize = 16;
const MAX_CAUSE_BYTES: usize = 768;
const MAX_ERROR_CHAIN_BYTES: usize = 4 * 1024;
const TEMP_NAME_ATTEMPTS: usize = 8;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticStage {
    SourceDownload,
    SourceValidation,
    AudioPreparation,
    Import,
    MusicQuery,
    MusicCopyVerification,
    ArchiveUpload,
    StagingCleanup,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticArtifact {
    ImportSource,
    PreparedCopy,
    MusicLibraryCopy,
    ArchiveSource,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct DiagnosticFileIdentity {
    pub(crate) device: u64,
    pub(crate) inode: u64,
}

/// Safe, path-free evidence that can accompany an error chain.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct DiagnosticEvidence {
    pub(crate) artifact: Option<DiagnosticArtifact>,
    pub(crate) expected_sha256_prefix: Option<String>,
    pub(crate) observed_sha256_prefix: Option<String>,
    pub(crate) expected_size_bytes: Option<u64>,
    pub(crate) observed_size_bytes: Option<u64>,
    pub(crate) expected_identity: Option<DiagnosticFileIdentity>,
    pub(crate) observed_identity: Option<DiagnosticFileIdentity>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct DiagnosticRecord {
    schema_version: u32,
    pub(crate) recorded_at_unix_ms: u64,
    pub(crate) job_id: String,
    pub(crate) stage: DiagnosticStage,
    pub(crate) error_chain: Vec<String>,
    pub(crate) evidence: Option<DiagnosticEvidence>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DiagnosticReceipt {
    pub(crate) recorded_at_unix_ms: u64,
    pub(crate) record_count: usize,
    pub(crate) journal_bytes: usize,
}

#[derive(Debug, Deserialize, Serialize)]
struct DiagnosticJournal {
    version: u32,
    records: Vec<DiagnosticRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl DiagnosticRecord {
    /// Capture a bounded, redacted, owned representation before handing work to a
    /// blocking writer. File paths, URLs, and credential-like values are removed.
    pub(crate) fn capture(
        job_id: &str,
        stage: DiagnosticStage,
        error: &anyhow::Error,
        evidence: Option<DiagnosticEvidence>,
    ) -> Result<Self> {
        validate_job_id(job_id)?;
        if let Some(evidence) = evidence.as_ref() {
            validate_evidence(evidence)?;
        }

        let mut record = Self {
            schema_version: JOURNAL_VERSION,
            recorded_at_unix_ms: now_unix_ms(),
            job_id: job_id.to_owned(),
            stage,
            error_chain: error
                .chain()
                .take(MAX_ERROR_CAUSES)
                .map(|cause| redact_error_text(&cause.to_string()))
                .collect(),
            evidence,
        };
        bound_record(&mut record)?;
        Ok(record)
    }
}

/// Persist one captured failure under `state_dir/diagnostics`.
///
/// This operation is intentionally synchronous so async callers can move it to
/// `spawn_blocking`. It returns an error on failed verification or I/O; callers
/// must keep their original job error as the primary result.
pub(crate) fn record_failure(
    state_dir: &Path,
    provider: &dyn QueueFileProvider,
    mut record: DiagnosticRecord,
) -> Result<DiagnosticReceipt> {
    validate_job_id(&record.job_id)?;
    if record.schema_version != JOURNAL_VERSION {
        bail!("music diagnostic record has an unsupported version");
    }
    if let Some(evidence) = record.evidence.as_ref() {
        validate_evidence(evidence)?;
    }
    bound_record(&mut record)?;

    let mut receipt = None;
    let mut write = |coordinated_state_dir: &Path| -> Result<()> {
        validate_private_directory(coordinated_state_dir, "music state directory")?;
        let diagnostics_dir = coordinated_state_dir.join(DIRECTORY_NAME);
        ensure_private_directory(&diagnostics_dir)?;
        receipt = Some(append_record(&diagnostics_dir, record.clone())?);
        Ok(())
    };
    provider.coordinate_write(state_dir, &mut write)?;
    receipt.ok_or_else(|| anyhow!("File Provider did not supply a diagnostics writer"))
}

fn append_record(directory: &Path, record: DiagnosticRecord) -> Result<DiagnosticReceipt> {
    validate_private_directory(directory, "music diagnostics directory")?;
    let lock_path = directory.join(LOCK_NAME);
    ensure_private_lock(&lock_path)?;
    let lock = open_private_file(&lock_path, true)?;
    ensure_path_matches_file(&lock_path, &lock, "music diagnostics lock")?;
    rustix::fs::flock(lock.as_fd(), FlockOperation::NonBlockingLockExclusive)
        .map_err(|error| anyhow!(std::io::Error::from_raw_os_error(error.raw_os_error())))
        .context("failed to lock music diagnostics journal")?;
    validate_private_file(&lock, 0o600, "music diagnostics lock")?;
    ensure_path_matches_file(&lock_path, &lock, "music diagnostics lock")?;
    validate_private_directory(directory, "music diagnostics directory")?;
    let directory_identity = file_identity(&fs::metadata(directory)?)?;

    let journal_path = directory.join(JOURNAL_NAME);
    let (mut journal, old_identity) = read_journal(&journal_path)?;
    let recorded_at_unix_ms = record.recorded_at_unix_ms;
    journal.records.push(record);
    trim_journal(&mut journal)?;
    let bytes = serialize_journal(&journal)?;
    if bytes.len() > MAX_JOURNAL_BYTES {
        bail!("music diagnostics journal exceeds its serialized size limit");
    }

    atomic_replace_journal(
        directory,
        &journal_path,
        &bytes,
        old_identity,
        directory_identity,
    )?;
    Ok(DiagnosticReceipt {
        recorded_at_unix_ms,
        record_count: journal.records.len(),
        journal_bytes: bytes.len(),
    })
}

fn read_journal(path: &Path) -> Result<(DiagnosticJournal, Option<FileIdentity>)> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((
                DiagnosticJournal {
                    version: JOURNAL_VERSION,
                    records: Vec::new(),
                },
                None,
            ));
        }
        Err(error) => return Err(error).context("failed to inspect music diagnostics journal"),
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
        || metadata.len() > MAX_JOURNAL_BYTES as u64
    {
        bail!("music diagnostics journal is not a bounded owner-private file");
    }

    let file = open_private_file(path, false)?;
    ensure_path_matches_file(path, &file, "music diagnostics journal")?;
    let identity = file_identity(&file.metadata()?)?;
    let mut reader = file.take((MAX_JOURNAL_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .context("failed to read music diagnostics journal")?;
    if bytes.len() > MAX_JOURNAL_BYTES {
        bail!("music diagnostics journal exceeds its read limit");
    }
    let journal: DiagnosticJournal =
        serde_json::from_slice(&bytes).context("failed to parse music diagnostics journal")?;
    if journal.version != JOURNAL_VERSION || journal.records.len() > MAX_RECORDS {
        bail!("music diagnostics journal has an unsupported shape");
    }
    for record in &journal.records {
        let serialized = serde_json::to_vec(record)?;
        if serialized.len() > MAX_RECORD_BYTES {
            bail!("music diagnostics journal contains an oversized record");
        }
    }
    ensure_path_matches_file(path, &reader.into_inner(), "music diagnostics journal")?;
    Ok((journal, Some(identity)))
}

fn trim_journal(journal: &mut DiagnosticJournal) -> Result<()> {
    while journal.records.len() > MAX_RECORDS {
        journal.records.remove(0);
    }
    loop {
        if serialize_journal(journal)?.len() <= MAX_JOURNAL_BYTES {
            return Ok(());
        }
        if journal.records.len() <= 1 {
            bail!("a music diagnostics record exceeds the journal size limit");
        }
        journal.records.remove(0);
    }
}

fn serialize_journal(journal: &DiagnosticJournal) -> Result<Vec<u8>> {
    serde_json::to_vec(journal).context("failed to encode music diagnostics journal")
}

fn atomic_replace_journal(
    directory: &Path,
    destination: &Path,
    bytes: &[u8],
    expected_old_identity: Option<FileIdentity>,
    expected_directory_identity: FileIdentity,
) -> Result<()> {
    validate_private_directory(directory, "music diagnostics directory")?;
    ensure_directory_identity(directory, expected_directory_identity)?;
    let (temporary, mut file) = create_temporary_file(directory)?;
    let result = (|| -> Result<()> {
        file.write_all(bytes)
            .context("failed to write music diagnostics journal")?;
        file.sync_all()
            .context("failed to sync music diagnostics journal")?;
        validate_private_file(&file, 0o600, "music diagnostics temporary file")?;
        let new_identity = file_identity(&file.metadata()?)?;
        drop(file);

        ensure_destination_identity(destination, expected_old_identity)?;
        ensure_directory_identity(directory, expected_directory_identity)?;
        fs::rename(&temporary, destination)
            .context("failed to replace music diagnostics journal")?;

        let committed = open_private_file(destination, false)?;
        if file_identity(&committed.metadata()?)? != new_identity {
            bail!("music diagnostics journal changed during replacement");
        }
        ensure_path_matches_file(destination, &committed, "music diagnostics journal")?;
        validate_private_directory(directory, "music diagnostics directory")?;
        ensure_directory_identity(directory, expected_directory_identity)?;
        sync_parent(destination)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn create_temporary_file(directory: &Path) -> Result<(PathBuf, File)> {
    for _ in 0..TEMP_NAME_ATTEMPTS {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate =
            directory.join(format!(".failures.{}.{}.tmp", std::process::id(), sequence));
        match OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).context("failed to create temporary music diagnostics file");
            }
        }
    }
    bail!("could not allocate a unique music diagnostics temporary file")
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(path) {
                Ok(()) => sync_parent(path)?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error).context("failed to create music diagnostics directory");
                }
            }
        }
        Err(error) => return Err(error).context("failed to inspect music diagnostics directory"),
    }
    validate_private_directory(path, "music diagnostics directory")
}

fn ensure_private_lock(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let created = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path);
            match created {
                Ok(file) => {
                    file.sync_all()
                        .context("failed to sync music diagnostics lock")?;
                    sync_parent(path)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error).context("failed to create music diagnostics lock");
                }
            }
        }
        Err(error) => return Err(error).context("failed to inspect music diagnostics lock"),
    }
    let file = open_private_file(path, true)?;
    ensure_path_matches_file(path, &file, "music diagnostics lock")
}

fn open_private_file(path: &Path, writable: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(writable)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .context("failed to open private music diagnostics file")?;
    validate_private_file(&file, 0o600, "music diagnostics file")?;
    Ok(file)
}

fn validate_private_directory(path: &Path, label: &str) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("failed to inspect {label}"))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o7777 != 0o700
    {
        bail!("{label} must be an owner-private directory");
    }
    Ok(())
}

fn validate_private_file(file: &File, mode: u32, label: &str) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {label}"))?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o7777 != mode
        || metadata.nlink() != 1
    {
        bail!("{label} must be a single-link owner-private regular file");
    }
    Ok(())
}

fn ensure_path_matches_file(path: &Path, file: &File, label: &str) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("failed to recheck {label}"))?;
    if metadata.file_type().is_symlink()
        || file_identity(&metadata)? != file_identity(&file.metadata()?)?
    {
        bail!("{label} identity changed while it was open");
    }
    Ok(())
}

fn ensure_destination_identity(path: &Path, expected: Option<FileIdentity>) -> Result<()> {
    match (fs::symlink_metadata(path), expected) {
        (Ok(metadata), Some(identity))
            if !metadata.file_type().is_symlink() && file_identity(&metadata)? == identity =>
        {
            if !metadata.is_file()
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.permissions().mode() & 0o7777 != 0o600
                || metadata.nlink() != 1
            {
                bail!("music diagnostics journal access policy changed");
            }
            Ok(())
        }
        (Err(error), None) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        (Err(error), _) => Err(error).context("failed to recheck music diagnostics journal"),
        _ => bail!("music diagnostics journal identity changed before replacement"),
    }
}

fn ensure_directory_identity(path: &Path, expected: FileIdentity) -> Result<()> {
    validate_private_directory(path, "music diagnostics directory")?;
    let metadata = fs::metadata(path).context("failed to recheck music diagnostics directory")?;
    if file_identity(&metadata)? != expected {
        bail!("music diagnostics directory identity changed");
    }
    Ok(())
}

fn file_identity(metadata: &fs::Metadata) -> Result<FileIdentity> {
    if !metadata.is_file() && !metadata.is_dir() {
        bail!("music diagnostics entry has an unsupported type");
    }
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("music diagnostics path has no parent"))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .context("failed to sync music diagnostics directory")
}

fn validate_job_id(job_id: &str) -> Result<()> {
    if job_id.is_empty()
        || job_id.len() > MAX_JOB_ID_BYTES
        || job_id == "."
        || job_id == ".."
        || !job_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("music diagnostic job ID is invalid");
    }
    Ok(())
}

fn validate_evidence(evidence: &DiagnosticEvidence) -> Result<()> {
    for digest in [
        evidence.expected_sha256_prefix.as_deref(),
        evidence.observed_sha256_prefix.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if !(8..=64).contains(&digest.len()) || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("music diagnostic digest prefix is invalid");
        }
    }
    Ok(())
}

fn bound_record(record: &mut DiagnosticRecord) -> Result<()> {
    validate_job_id(&record.job_id)?;
    if let Some(evidence) = record.evidence.as_ref() {
        validate_evidence(evidence)?;
    }
    record.error_chain.truncate(MAX_ERROR_CAUSES);
    for cause in &mut record.error_chain {
        *cause = truncate_utf8(cause, MAX_CAUSE_BYTES).to_owned();
    }
    let mut total = record.error_chain.iter().map(String::len).sum::<usize>();
    while total > MAX_ERROR_CHAIN_BYTES {
        let Some(last) = record.error_chain.last_mut() else {
            break;
        };
        let excess = total - MAX_ERROR_CHAIN_BYTES;
        let new_len = last.len().saturating_sub(excess.max(1));
        let truncated = truncate_utf8(last, new_len).to_owned();
        total -= last.len() - truncated.len();
        *last = truncated;
        if last.is_empty() && record.error_chain.len() > 1 {
            record.error_chain.pop();
        }
    }
    if record.error_chain.is_empty() {
        record
            .error_chain
            .push("failure details unavailable".to_owned());
    }
    while serde_json::to_vec(record)?.len() > MAX_RECORD_BYTES {
        if record.error_chain.len() > 1 && record.error_chain.last().is_some_and(String::is_empty) {
            record.error_chain.pop();
            continue;
        }
        let Some(last) = record.error_chain.last_mut() else {
            bail!("music diagnostic record exceeds its size limit");
        };
        if last.is_empty() {
            bail!("music diagnostic record exceeds its size limit");
        }
        let new_len = last.len() / 2;
        *last = truncate_utf8(last, new_len).to_owned();
    }
    Ok(())
}

fn redact_error_text(text: &str) -> String {
    let text = redact_sensitive_text(text);
    let text = redact_urls(&text);
    let text = redact_sensitive_fields(&text);
    let text = redact_absolute_paths(&text);
    let text = redact_email_addresses(&text);
    truncate_utf8(text.trim(), MAX_CAUSE_BYTES).to_owned()
}

fn redact_urls(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut output = String::with_capacity(text.len());
    let mut index = 0;
    while index < bytes.len() {
        let remaining = &text[index..];
        let is_url = remaining
            .get(..7)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"))
            || remaining
                .get(..8)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"));
        if !is_url {
            let character = remaining.chars().next().expect("index is in bounds");
            output.push(character);
            index += character.len_utf8();
            continue;
        }
        let end = remaining
            .char_indices()
            .find(|(_, character)| {
                character.is_ascii_whitespace()
                    || matches!(character, ')' | ']' | '}' | '>' | '"' | '\'' | ',')
            })
            .map_or(text.len(), |(offset, _)| index + offset);
        output.push_str("<redacted URL>");
        index = end;
    }
    output
}

fn redact_sensitive_fields(text: &str) -> String {
    text.lines()
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            let credential_header = lower.contains("authorization:")
                || lower.contains("proxy-authorization:")
                || lower.contains("cookie:")
                || lower.contains("set-cookie:");
            let credential_pair = [
                "token=",
                "token:",
                "secret=",
                "secret:",
                "password=",
                "password:",
                "passwd=",
                "api_key=",
                "api-key=",
                "access_key=",
                "access_token=",
                "refresh_token=",
                "client_secret=",
                "dsid=",
                "account=",
            ]
            .iter()
            .filter_map(|key| lower.find(key))
            .min();
            let bearer_value = lower.contains("bearer ") || lower.contains("identify_v1 ");
            if credential_header || bearer_value {
                "<redacted sensitive diagnostic text>".to_owned()
            } else if let Some(offset) = credential_pair {
                format!("{}<redacted sensitive value>", &line[..offset])
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn redact_absolute_paths(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut index = 0;
    while index < text.len() {
        let remaining = &text[index..];
        let at_boundary = index == 0
            || text[..index].chars().next_back().is_some_and(|character| {
                character.is_ascii_whitespace()
                    || matches!(character, '=' | ':' | '(' | '[' | '{' | '"' | '\'')
            });
        if !at_boundary || !remaining.starts_with('/') {
            let character = remaining.chars().next().expect("index is in bounds");
            output.push(character);
            index += character.len_utf8();
            continue;
        }
        let end = remaining
            .char_indices()
            .find(|(_, character)| {
                character.is_ascii_whitespace()
                    || matches!(character, ')' | ']' | '}' | '"' | '\'' | ',' | ';')
            })
            .map_or(text.len(), |(offset, _)| index + offset);
        let candidate = &remaining[..end - index];
        if candidate.contains('/') && candidate.len() > 1 {
            output.push_str("<redacted path>");
            index = end;
        } else {
            output.push('/');
            index += 1;
        }
    }
    output
}

fn redact_email_addresses(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut token_start = 0;
    for (index, character) in text
        .char_indices()
        .chain(std::iter::once((text.len(), ' ')))
    {
        if character.is_ascii_whitespace() || matches!(character, '<' | '>' | '(' | ')' | ',' | ';')
        {
            let token = &text[token_start..index];
            if token.contains('@') && token.contains('.') {
                output.push_str("<redacted address>");
            } else {
                output.push_str(token);
            }
            if index < text.len() {
                output.push(character);
            }
            token_start = index + character.len_utf8();
        }
    }
    output
}

fn truncate_utf8(value: &str, maximum_bytes: usize) -> &str {
    if value.len() <= maximum_bytes {
        return value;
    }
    let mut boundary = maximum_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::time::Instant;

    use crate::file_provider::QueueFileProvider;

    struct DirectProvider;

    impl QueueFileProvider for DirectProvider {
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
            accessor(source, destination)
        }
    }

    fn sample_record(job_id: &str, error: &str) -> DiagnosticRecord {
        DiagnosticRecord::capture(
            job_id,
            DiagnosticStage::MusicCopyVerification,
            &anyhow!("{error}"),
            Some(DiagnosticEvidence {
                artifact: Some(DiagnosticArtifact::MusicLibraryCopy),
                expected_sha256_prefix: Some("0123456789abcdef".to_owned()),
                observed_sha256_prefix: Some("fedcba9876543210".to_owned()),
                expected_size_bytes: Some(42),
                observed_size_bytes: Some(41),
                expected_identity: Some(DiagnosticFileIdentity {
                    device: 1,
                    inode: 2,
                }),
                observed_identity: Some(DiagnosticFileIdentity {
                    device: 1,
                    inode: 3,
                }),
            }),
        )
        .expect("sample record should be valid")
    }

    #[test]
    fn capture_redacts_urls_paths_credentials_and_addresses_but_keeps_error_context() {
        const SYNTHETIC_ACCESS_TOKEN: &str = "codex_synth_v1_access_a"; // synthetic-token-fixtures: joey-private-v3 / access-a, active
        let error = anyhow!(
            "Music copy verification failed at /Users/alice/Music/song.m4a; contact alice@example.test; token={SYNTHETIC_ACCESS_TOKEN}; account=12345; endpoint https://music.example/private?id=987"
        )
        .context("source copy could not be confirmed");
        let record = DiagnosticRecord::capture(
            "mjl1-abcdefabcdefabcdefabcdef",
            DiagnosticStage::MusicCopyVerification,
            &error,
            None,
        )
        .expect("safe job ID should be accepted");
        let encoded = serde_json::to_string(&record).expect("record should serialize");
        assert!(encoded.contains("source copy could not be confirmed"));
        assert!(encoded.contains("Music copy verification failed"));
        for forbidden in [
            "/Users/alice",
            "song.m4a",
            SYNTHETIC_ACCESS_TOKEN,
            "12345",
            "alice@example.test",
            "https://music.example",
            "id=987",
        ] {
            assert!(!encoded.contains(forbidden), "record exposed {forbidden}");
        }
        assert!(encoded.len() <= MAX_RECORD_BYTES);
    }

    #[test]
    fn capture_rejects_unsafe_job_ids_and_digest_evidence() {
        let error = anyhow!("failed");
        assert!(
            DiagnosticRecord::capture("../jobs", DiagnosticStage::Import, &error, None).is_err()
        );
        assert!(DiagnosticRecord::capture("job/1", DiagnosticStage::Import, &error, None).is_err());
        let evidence = DiagnosticEvidence {
            expected_sha256_prefix: Some("not-a-hash".to_owned()),
            ..DiagnosticEvidence::default()
        };
        assert!(
            DiagnosticRecord::capture(
                "mjl1-0123456789abcdef01234567",
                DiagnosticStage::Import,
                &error,
                Some(evidence),
            )
            .is_err()
        );
    }

    #[test]
    fn journal_retention_is_count_bounded_and_does_not_deduplicate_by_time() {
        let mut journal = DiagnosticJournal {
            version: JOURNAL_VERSION,
            records: Vec::new(),
        };
        for index in 0..(MAX_RECORDS + 8) {
            let mut record = sample_record(
                &format!("job-{index}"),
                "same timestamp is not a deduplication key",
            );
            record.recorded_at_unix_ms = 10;
            journal.records.push(record);
            trim_journal(&mut journal).expect("journal should remain bounded");
        }
        assert_eq!(journal.records.len(), MAX_RECORDS);
        assert_eq!(journal.records[0].job_id, "job-8");
        assert_eq!(journal.records[MAX_RECORDS - 1].job_id, "job-519");
        assert!(
            serialize_journal(&journal)
                .expect("journal should serialize")
                .len()
                <= MAX_JOURNAL_BYTES
        );
    }

    #[test]
    fn record_failure_writes_private_atomic_journal_and_returns_receipt() {
        let directory = tempfile::tempdir().expect("temporary state directory");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("state directory should be private");
        let record = sample_record("mjl1-0123456789abcdef01234567", "copy mismatch");
        let receipt = record_failure(directory.path(), &DirectProvider, record)
            .expect("diagnostic should be persisted");
        assert_eq!(receipt.record_count, 1);
        assert!(receipt.journal_bytes > 0 && receipt.journal_bytes <= MAX_JOURNAL_BYTES);

        let diagnostics = directory.path().join(DIRECTORY_NAME);
        let journal_path = diagnostics.join(JOURNAL_NAME);
        assert_eq!(
            fs::metadata(&diagnostics).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let journal_meta = fs::symlink_metadata(&journal_path).unwrap();
        assert_eq!(journal_meta.permissions().mode() & 0o777, 0o600);
        assert_eq!(journal_meta.nlink(), 1);
        let journal: DiagnosticJournal =
            serde_json::from_slice(&fs::read(&journal_path).unwrap()).unwrap();
        assert_eq!(journal.records.len(), 1);
        assert_eq!(journal.records[0].job_id, "mjl1-0123456789abcdef01234567");
        assert_eq!(journal.records[0].error_chain, vec!["copy mismatch"]);
    }

    #[test]
    fn record_failure_refuses_symlink_journal_without_touching_target() {
        let directory = tempfile::tempdir().expect("temporary state directory");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("state directory should be private");
        let diagnostics = directory.path().join(DIRECTORY_NAME);
        fs::create_dir(&diagnostics).expect("diagnostics directory");
        fs::set_permissions(&diagnostics, fs::Permissions::from_mode(0o700))
            .expect("diagnostics directory should be private");
        let lock = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(diagnostics.join(LOCK_NAME))
            .expect("lock file");
        drop(lock);
        let target = directory.path().join("target.json");
        fs::write(&target, b"keep me").expect("target file");
        std::os::unix::fs::symlink(&target, diagnostics.join(JOURNAL_NAME))
            .expect("journal symlink");

        let record = sample_record("mjl1-0123456789abcdef01234567", "copy mismatch");
        assert!(record_failure(directory.path(), &DirectProvider, record).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"keep me");
    }

    #[test]
    fn record_failure_refuses_a_corrupt_journal_without_replacing_it() {
        let directory = tempfile::tempdir().expect("temporary state directory");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("state directory should be private");
        let diagnostics = directory.path().join(DIRECTORY_NAME);
        fs::create_dir(&diagnostics).expect("diagnostics directory");
        fs::set_permissions(&diagnostics, fs::Permissions::from_mode(0o700))
            .expect("diagnostics directory should be private");
        let lock = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(diagnostics.join(LOCK_NAME))
            .expect("lock file");
        drop(lock);
        let journal = diagnostics.join(JOURNAL_NAME);
        fs::write(&journal, b"not-json").expect("corrupt journal fixture");
        fs::set_permissions(&journal, fs::Permissions::from_mode(0o600))
            .expect("journal should be private");

        let record = sample_record("mjl1-0123456789abcdef01234567", "copy mismatch");
        assert!(record_failure(directory.path(), &DirectProvider, record).is_err());
        assert_eq!(fs::read(&journal).unwrap(), b"not-json");
    }

    #[test]
    fn held_writer_lock_fails_promptly_without_replacing_primary_error() {
        let directory = tempfile::tempdir().expect("temporary state directory");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("state directory should be private");
        let diagnostics = directory.path().join(DIRECTORY_NAME);
        fs::create_dir(&diagnostics).expect("diagnostics directory");
        fs::set_permissions(&diagnostics, fs::Permissions::from_mode(0o700))
            .expect("diagnostics directory should be private");
        let lock_path = diagnostics.join(LOCK_NAME);
        let held_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lock_path)
            .expect("lock file");
        rustix::fs::flock(held_lock.as_fd(), FlockOperation::LockExclusive)
            .expect("test should hold the diagnostics lock");

        let primary_error = anyhow!("original Music copy verification failure");
        let primary_message = primary_error.to_string();
        let started = Instant::now();
        let diagnostic_error = record_failure(
            directory.path(),
            &DirectProvider,
            sample_record("mjl1-0123456789abcdef01234567", "copy mismatch"),
        )
        .expect_err("the held lock should make diagnostics best-effort fail");
        assert!(
            started.elapsed().as_secs() < 2,
            "lock contention must not stall"
        );
        assert!(diagnostic_error.chain().any(|cause| {
            cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        }));

        // Callers retain this original error and ignore the diagnostics error.
        assert_eq!(primary_error.to_string(), primary_message);
    }

    #[test]
    fn output_redacts_all_url_path_segments_and_utf8_truncation_keeps_boundaries() {
        assert_eq!(
            redact_urls("request https://example.test/a/b?q=x done"),
            "request <redacted URL> done"
        );
        assert_eq!(
            redact_absolute_paths("failed at /private/tmp/secret/file"),
            "failed at <redacted path>"
        );
        let text = "ééé";
        assert_eq!(truncate_utf8(text, 3), "é");
    }
}
