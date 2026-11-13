use crate::metadata;
use crate::native::{self, Controls};
use crate::types::{ConversionReport, ConvertRequest, Event, PROTOCOL_VERSION};
use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const MAX_INPUT_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Debug)]
pub struct ConversionFailure {
    pub message: String,
    pub partial_output: Option<PathBuf>,
    pub staging_directory: Option<PathBuf>,
    pub cancelled: bool,
}

impl std::fmt::Display for ConversionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(formatter)
    }
}
impl std::error::Error for ConversionFailure {}

/// Convert one file. Callers needing a hard deadline should use the process client.
pub fn convert(
    request: &ConvertRequest,
    cancelled: &AtomicBool,
    observe: &mut dyn FnMut(Event) -> Result<()>,
) -> std::result::Result<ConversionReport, ConversionFailure> {
    let mut partial_output = None;
    let result = convert_inner(request, cancelled, observe, &mut partial_output);
    result.map_err(|error| {
        let staging_directory = partial_output
            .as_ref()
            .and_then(|path| path.parent())
            .map(Path::to_owned);
        let partial_output = partial_output
            .filter(|path| fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file()));
        ConversionFailure {
            message: format!("{error:#}"),
            partial_output,
            staging_directory,
            cancelled: cancelled.load(Ordering::Acquire),
        }
    })
}

fn convert_inner(
    request: &ConvertRequest,
    cancelled: &AtomicBool,
    observe: &mut dyn FnMut(Event) -> Result<()>,
    partial_output: &mut Option<PathBuf>,
) -> Result<ConversionReport> {
    ensure!(
        request.version == PROTOCOL_VERSION,
        "unsupported protocol version"
    );
    ensure!(
        (1..=86_400).contains(&request.timeout_seconds),
        "invalid timeout_seconds"
    );
    check_cancel(cancelled)?;
    let input = absolute(&request.input)?;
    let output = absolute(&request.output)?;
    ensure!(input != output, "input and output must be different files");
    ensure!(
        !output.try_exists()? && fs::symlink_metadata(&output).is_err(),
        "output already exists"
    );
    let parent = output.parent().context("output has no parent directory")?;
    let parent_canonical = parent
        .canonicalize()
        .context("output parent is unavailable")?;
    ensure!(
        parent_canonical.is_dir(),
        "output parent is not a directory"
    );
    let parent_identity = identity(&fs::metadata(&parent_canonical)?);
    let final_path = parent_canonical.join(output.file_name().context("invalid output filename")?);

    let mut staging = tempfile::Builder::new();
    staging.prefix(".flac-alac-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        staging.permissions(fs::Permissions::from_mode(0o700));
    }
    let directory = staging
        .tempdir_in(&parent_canonical)
        .context("create private same-filesystem staging")?
        .keep();
    let staged_source = directory.join("source.flac");
    let staged_output = directory.join("output.partial.m4a");
    *partial_output = Some(staged_output.clone());

    // A private input snapshot pins bytes across metadata parsing and native callbacks.
    // File Provider flag/time changes are immaterial; content and object identity matter.
    let input_snapshot = snapshot_copy(&input, &staged_source, cancelled)?;
    check_cancel(cancelled)?;
    let source = metadata::read(&staged_source).context("read FLAC metadata")?;
    observe(Event::Started {
        version: PROTOCOL_VERSION,
        source: source.audio.clone(),
    })?;
    check_cancel(cancelled)?;
    let started = Instant::now();
    let mut last_progress = Instant::now();
    let mut last_frames = 0_u64;
    let mut progress = |frames: u64| -> Result<()> {
        check_cancel(cancelled)?;
        ensure!(
            started.elapsed() < Duration::from_secs(request.timeout_seconds),
            "conversion deadline exceeded"
        );
        if let Ok(metadata) = fs::metadata(&staged_output) {
            ensure!(
                metadata.len() <= MAX_INPUT_BYTES,
                "encoded output exceeds the 8 GiB limit"
            );
        }
        if frames == source.audio.frames
            || frames.saturating_sub(last_frames) >= 131_072
            || last_progress.elapsed() >= Duration::from_millis(250)
        {
            observe(Event::Progress {
                version: PROTOCOL_VERSION,
                frames,
                total_frames: source.audio.frames,
            })?;
            last_frames = frames;
            last_progress = Instant::now();
        }
        Ok(())
    };
    let mut controls = Controls {
        cancelled,
        progress: &mut progress,
        allow_compatibility: request.allow_compatibility,
    };
    let encoding = native::encode(&staged_source, &staged_output, &source.audio, &mut controls)
        .context("native ALAC encoding")?;
    let metadata = metadata::write_and_verify(&source, &staged_output)
        .context("migrate and verify metadata")?;
    let verification = native::verify(&staged_output, &encoding, &mut controls)
        .context("verify decoded ALAC audio")?;
    check_cancel(cancelled)?;
    ensure!(
        verification.output == encoding.output,
        "output audio parameters changed during verification"
    );
    ensure!(
        verification.frames == encoding.output.frames,
        "decoded output frame count differs"
    );
    ensure!(
        verification.bit_exact,
        "audio difference; output was not published"
    );
    let staged_hash = hash_file(&staged_source, cancelled)?;
    ensure!(
        staged_hash == input_snapshot.sha256,
        "private source snapshot changed"
    );
    let after = snapshot(&input, cancelled)?;
    ensure!(
        after.sha256 == input_snapshot.sha256 && after.object == input_snapshot.object,
        "original input changed during conversion"
    );
    let output_sha256 = hash_file(&staged_output, cancelled)?;
    let mut warnings = encoding.warnings;
    warnings.extend(metadata.warnings.iter().cloned());
    for warning in &warnings {
        observe(Event::Warning {
            version: PROTOCOL_VERSION,
            warning: warning.clone(),
        })?;
    }
    let mut source_spec = source.audio;
    if source_spec.frames == 0 {
        source_spec.frames = encoding.output.frames;
    }
    let mut report = ConversionReport {
        version: PROTOCOL_VERSION,
        source: source_spec,
        output: encoding.output,
        output_path: output.clone(),
        input_sha256: input_snapshot.sha256,
        output_sha256,
        pcm_sha256: verification.output_pcm_sha256,
        audio_bit_exact: verification.bit_exact,
        metadata,
        warnings,
    };
    ensure!(
        serde_json::to_vec(&report)?.len() <= crate::protocol::MAX_EVENT_BYTES / 2,
        "conversion report exceeds protocol limit"
    );
    check_cancel(cancelled)?;
    ensure!(
        identity(&fs::metadata(parent)?) == parent_identity,
        "output parent changed"
    );
    File::open(&staged_output)?.sync_all()?;
    publish_noclobber(&staged_output, &final_path)
        .context("publish verified output without replacement")?;
    *partial_output = None;
    if let Err(error) = File::open(&parent_canonical).and_then(|directory| directory.sync_all()) {
        report.warnings.push(crate::types::Warning::new(
            "directory_sync",
            format!("output is published but directory sync failed: {error}"),
        ));
    }
    // Only successful disposable copies are cleaned; failed/cancelled stages are retained.
    let _ = fs::remove_file(&staged_source);
    let _ = fs::remove_dir(&directory);
    *partial_output = None;
    Ok(report)
}

fn absolute(path: &Path) -> Result<PathBuf> {
    ensure!(!path.as_os_str().is_empty(), "empty path");
    Ok(if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    })
}

fn check_cancel(cancelled: &AtomicBool) -> Result<()> {
    ensure!(!cancelled.load(Ordering::Acquire), "conversion cancelled");
    Ok(())
}

#[derive(Debug)]
struct Snapshot {
    object: (u64, u64),
    sha256: String,
}

fn open_source(path: &Path) -> Result<File> {
    let before = fs::symlink_metadata(path)?;
    ensure!(
        before.is_file() && !before.file_type().is_symlink(),
        "input must be a regular file"
    );
    ensure!(
        before.len() <= MAX_INPUT_BYTES,
        "input exceeds the 8 GiB limit"
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let opened = file.metadata()?;
    ensure!(
        opened.is_file() && identity(&before) == identity(&opened),
        "input replaced while opening"
    );
    Ok(file)
}

fn snapshot_copy(input: &Path, destination: &Path, cancelled: &AtomicBool) -> Result<Snapshot> {
    let mut source = open_source(input)?;
    let original = source.metadata()?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut target = options.open(destination)?;
    let sha256 = copy_hash(&mut source, Some(&mut target), cancelled)?;
    target.sync_all()?;
    ensure!(
        identity(&source.metadata()?) == identity(&original),
        "input object changed"
    );
    Ok(Snapshot {
        object: identity(&original),
        sha256,
    })
}

fn snapshot(input: &Path, cancelled: &AtomicBool) -> Result<Snapshot> {
    let mut source = open_source(input)?;
    let object = identity(&source.metadata()?);
    let sha256 = copy_hash(&mut source, None, cancelled)?;
    Ok(Snapshot { object, sha256 })
}

fn hash_file(path: &Path, cancelled: &AtomicBool) -> Result<String> {
    Ok(snapshot(path, cancelled)?.sha256)
}

fn copy_hash(
    source: &mut File,
    mut target: Option<&mut File>,
    cancelled: &AtomicBool,
) -> Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        check_cancel(cancelled)?;
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total += read as u64;
        ensure!(
            total <= MAX_INPUT_BYTES,
            "file exceeded size limit while reading"
        );
        hash.update(&buffer[..read]);
        if let Some(output) = target.as_deref_mut() {
            output.write_all(&buffer[..read])?;
        }
    }
    Ok(format!("{:x}", hash.finalize()))
}

#[cfg(unix)]
fn identity(metadata: &fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (metadata.dev(), metadata.ino())
}
#[cfg(not(unix))]
fn identity(metadata: &fs::Metadata) -> (u64, u64) {
    (metadata.len(), 0)
}

#[cfg(target_os = "macos")]
fn publish_noclobber(source: &Path, target: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let source = CString::new(source.as_os_str().as_bytes())?;
    let target = CString::new(target.as_os_str().as_bytes())?;
    // SAFETY: both C strings remain live; RENAME_EXCL forbids replacing any destination.
    let result = unsafe { libc::renamex_np(source.as_ptr(), target.as_ptr(), libc::RENAME_EXCL) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}
#[cfg(not(target_os = "macos"))]
fn publish_noclobber(source: &Path, target: &Path) -> Result<()> {
    fs::hard_link(source, target)?;
    fs::remove_file(source)?;
    Ok(())
}
