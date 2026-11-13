//! Bounded parent process for the private raw-MTProto Music transfer helper.

use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::music_types::{MusicCancellation, MusicDownloadFailure, MusicSettings, MusicUpload};

const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_FILE_ID_BYTES: usize = 8 * 1024;
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_STREAM_BYTES: usize = 16 * 1024;
const TRANSFER_DEADLINE: Duration = Duration::from_secs(30 * 60);
const PROCESS_GRACE: Duration = Duration::from_secs(30);
const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(25);

pub struct MusicTransfer {
    settings: MusicSettings,
    bot_token: String,
}

impl MusicTransfer {
    pub fn new(settings: MusicSettings, bot_token: String) -> Self {
        Self {
            settings,
            bot_token,
        }
    }

    pub fn download(
        &self,
        upload: &MusicUpload,
        destination: &Path,
        cancel: &MusicCancellation,
    ) -> Result<()> {
        validate_transfer_paths(&self.settings, destination)?;
        if cancel.is_cancelled() {
            bail!("Telegram music transfer was cancelled");
        }
        if upload.file_size == 0
            || upload.file_size > MAX_FILE_BYTES
            || upload.file_size > self.settings.max_file_bytes.min(MAX_FILE_BYTES)
            || upload.file_id.is_empty()
            || upload.file_id.len() > MAX_FILE_ID_BYTES
            || !upload
                .file_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
            || upload.file_unique_id.is_empty()
            || upload.file_unique_id.len() > 1024
        {
            bail!("Telegram audio upload metadata is invalid or exceeds the 512 MiB limit");
        }
        if self.bot_token.is_empty()
            || self.bot_token.len() > 256
            || self.bot_token.chars().any(char::is_whitespace)
        {
            bail!("Telegram music transfer credentials are unavailable");
        }
        let python = expected_python_path()?;
        let helper = expected_helper_path()?;
        if self.settings.helper_python != python || self.settings.transfer_helper != helper {
            bail!("Telegram music helper is not installed in the active application profile");
        }
        verify_regular_helper(&helper)?;
        verify_absolute_interpreter(&python)?;
        let credentials_path = expected_credentials_path()?;
        if self.settings.credentials_path != credentials_path {
            bail!("Telegram application credentials must use the configured private path");
        }

        let unique_hash = Sha256::digest(upload.file_unique_id.as_bytes());
        let identity_sha256 = format!("{unique_hash:x}");
        let staging_root = path_text(&self.settings.staging_dir)?;
        let destination = path_text(destination)?;
        let credentials_path = path_text(&credentials_path)?;
        let request = HelperRequest {
            schema: 1,
            bot_token: &self.bot_token,
            credentials_path: &credentials_path,
            file_id: &upload.file_id,
            identity_sha256,
            expected_size: upload.file_size,
            staging_root: &staging_root,
            destination: &destination,
            deadline_seconds: TRANSFER_DEADLINE.as_secs(),
        };
        let request_bytes = serde_json::to_vec(&request)?;
        if request_bytes.len() > MAX_REQUEST_BYTES {
            bail!("Telegram music transfer request exceeds its size limit");
        }

        let output = run_helper_process(
            &python,
            &helper,
            request_bytes,
            cancel,
            TRANSFER_DEADLINE + PROCESS_GRACE,
        )?;
        let response: HelperResponse = serde_json::from_slice(&output)
            .map_err(|_| anyhow::anyhow!("Telegram music helper returned an invalid response"))?;
        if response.schema != 1 {
            bail!("Telegram music helper returned an unsupported response");
        }
        if !response.ok {
            // Only the bounded protocol enum reaches logs; never helper stderr,
            // credentials, file identifiers, or an arbitrary remote error body.
            tracing::warn!(failure = ?response.error, "Telegram Music transfer helper failed");
            match response.error {
                Some(HelperError::RefreshRequired) => {
                    return Err(MusicDownloadFailure::RefreshRequired.into());
                }
                Some(HelperError::RateLimited) => {
                    return Err(MusicDownloadFailure::RateLimited.into());
                }
                Some(HelperError::DeadlineExceeded | HelperError::RequestTimeout) => {
                    return Err(MusicDownloadFailure::TimedOut.into());
                }
                Some(HelperError::FileExists) => {
                    bail!("Telegram music destination already exists; refusing overwrite")
                }
                Some(HelperError::MaterializationPending) => {
                    bail!("Telegram music output awaits local materialization; retry is pending")
                }
                Some(HelperError::Busy) => {
                    return Err(MusicDownloadFailure::Busy.into());
                }
                Some(HelperError::Cancelled) => bail!("Telegram music transfer was cancelled"),
                Some(_) => bail!("Telegram audio transfer failed; retry is available"),
                None => bail!("Telegram music helper returned an invalid failure response"),
            }
        }
        let actual_size = response
            .size
            .ok_or_else(|| anyhow::anyhow!("Telegram music helper omitted the output size"))?;
        let actual_hash = response
            .sha256
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Telegram music helper omitted the output digest"))?;
        if actual_size != upload.file_size || !is_sha256(actual_hash) {
            bail!("Telegram music helper returned invalid output integrity data");
        }
        verify_published_output(Path::new(&destination), upload.file_size)?;
        Ok(())
    }
}

#[derive(Serialize)]
struct HelperRequest<'a> {
    schema: u8,
    bot_token: &'a str,
    credentials_path: &'a str,
    file_id: &'a str,
    identity_sha256: String,
    expected_size: u64,
    staging_root: &'a str,
    destination: &'a str,
    deadline_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HelperResponse {
    schema: u8,
    ok: bool,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    error: Option<HelperError>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HelperError {
    InvalidRequest,
    CredentialsUnavailable,
    UnsupportedFile,
    RefreshRequired,
    RateLimited,
    DeadlineExceeded,
    RequestTimeout,
    TransportError,
    RpcError,
    RpcDenied,
    UnexpectedResponse,
    IntegrityError,
    FileExists,
    MaterializationPending,
    Busy,
    StorageError,
    InternalError,
    Cancelled,
}

#[derive(Default)]
struct BoundedCapture {
    bytes: Vec<u8>,
    over_limit: bool,
    read_failed: bool,
}

fn capture_stream<R: Read>(
    mut stream: R,
    capture: Arc<Mutex<BoundedCapture>>,
    process_output_exceeded: Arc<AtomicBool>,
) {
    let mut chunk = [0_u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(count) => {
                let Ok(mut state) = capture.lock() else {
                    process_output_exceeded.store(true, Ordering::Release);
                    return;
                };
                let remaining = MAX_STREAM_BYTES.saturating_sub(state.bytes.len());
                if count > remaining {
                    state.over_limit = true;
                    process_output_exceeded.store(true, Ordering::Release);
                }
                let retained = count.min(remaining);
                state.bytes.extend_from_slice(&chunk[..retained]);
            }
            Err(_) => {
                if let Ok(mut state) = capture.lock() {
                    state.read_failed = true;
                }
                return;
            }
        }
    }
}

fn stop_and_reap(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.kill();
    }
    let _ = child.wait();
}

fn run_helper_process(
    python: &Path,
    helper: &Path,
    request: Vec<u8>,
    cancel: &MusicCancellation,
    timeout: Duration,
) -> Result<Vec<u8>> {
    if request.len() > MAX_REQUEST_BYTES {
        bail!("Telegram music transfer request exceeds its size limit");
    }
    let mut child = Command::new(python)
        .arg(helper)
        .env_clear()
        .env("PYTHONNOUSERSITE", "1")
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env("PYTHONUNBUFFERED", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| anyhow::anyhow!("could not start the Telegram music helper"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("Telegram music helper output is unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("Telegram music helper output is unavailable"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("Telegram music helper input is unavailable"))?;

    let stdout_capture = Arc::new(Mutex::new(BoundedCapture::default()));
    let stderr_capture = Arc::new(Mutex::new(BoundedCapture::default()));
    let output_exceeded = Arc::new(AtomicBool::new(false));

    let out_capture = Arc::clone(&stdout_capture);
    let out_exceeded = Arc::clone(&output_exceeded);
    let stdout_thread = thread::spawn(move || capture_stream(stdout, out_capture, out_exceeded));
    let err_capture = Arc::clone(&stderr_capture);
    let err_exceeded = Arc::clone(&output_exceeded);
    let stderr_thread = thread::spawn(move || capture_stream(stderr, err_capture, err_exceeded));
    let stdin_thread = thread::spawn(move || {
        let mut stdin = stdin;
        stdin.write_all(&request).is_ok()
    });

    let started = Instant::now();
    let status = loop {
        if cancel.is_cancelled() {
            stop_and_reap(&mut child);
            let _ = stdin_thread.join();
            let _ = stdout_thread.join();
            let _ = stderr_thread.join();
            bail!("Telegram music transfer was cancelled");
        }
        if output_exceeded.load(Ordering::Acquire) {
            stop_and_reap(&mut child);
            let _ = stdin_thread.join();
            let _ = stdout_thread.join();
            let _ = stderr_thread.join();
            bail!("Telegram music helper exceeded its output limit");
        }
        if started.elapsed() >= timeout {
            stop_and_reap(&mut child);
            let _ = stdin_thread.join();
            let _ = stdout_thread.join();
            let _ = stderr_thread.join();
            bail!("Telegram music helper exceeded its time limit");
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(CANCEL_POLL_INTERVAL),
            Err(_) => {
                stop_and_reap(&mut child);
                let _ = stdin_thread.join();
                let _ = stdout_thread.join();
                let _ = stderr_thread.join();
                bail!("Telegram music helper could not be monitored");
            }
        }
    };

    let input_written = stdin_thread
        .join()
        .map_err(|_| anyhow::anyhow!("Telegram music helper input failed"))?;
    stdout_thread
        .join()
        .map_err(|_| anyhow::anyhow!("Telegram music helper output failed"))?;
    stderr_thread
        .join()
        .map_err(|_| anyhow::anyhow!("Telegram music helper output failed"))?;
    if output_exceeded.load(Ordering::Acquire) {
        bail!("Telegram music helper exceeded its output limit");
    }
    let stdout = stdout_capture
        .lock()
        .map_err(|_| anyhow::anyhow!("Telegram music helper output failed"))?;
    let stderr = stderr_capture
        .lock()
        .map_err(|_| anyhow::anyhow!("Telegram music helper output failed"))?;
    if stdout.read_failed || stderr.read_failed || !input_written || !status.success() {
        bail!("Telegram music helper failed");
    }
    Ok(stdout.bytes.clone())
}

fn validate_transfer_paths(settings: &MusicSettings, destination: &Path) -> Result<()> {
    if !settings.enabled {
        bail!("automatic Music transfers are disabled");
    }
    if !settings.staging_dir.is_absolute()
        || !destination.is_absolute()
        || destination.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        bail!("Telegram music transfer paths are invalid");
    }
    let Some(job_dir) = destination.parent() else {
        bail!("Telegram music destination has no staging directory");
    };
    if job_dir.parent() != Some(settings.staging_dir.as_path())
        || !job_dir
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(valid_job_id)
        || !matches!(
            destination.file_name().and_then(|name| name.to_str()),
            Some(
                "original.m4a" | "original.mp3" | "original.aac" | "original.flac" | "original.wav"
            )
        )
        || settings
            .staging_dir
            .file_name()
            .and_then(|name| name.to_str())
            != Some("music")
        || settings
            .staging_dir
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            != Some(".telegram-video-downloader-staging")
    {
        bail!("Telegram music destination is outside its private job staging directory");
    }
    verify_private_directory(&settings.staging_dir)?;
    verify_private_directory(job_dir)?;
    Ok(())
}

fn valid_job_id(value: &str) -> bool {
    value.strip_prefix("mj1-").is_some_and(|suffix| {
        suffix.len() == 24
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

fn verify_private_directory(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| anyhow::anyhow!("Telegram music private staging directory is unavailable"))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        bail!("Telegram music staging directories must be owner-private and non-symlinked");
    }
    Ok(())
}

fn expected_profile_root() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| anyhow::anyhow!("account home directory is unavailable"))?;
    let profile = if cfg!(feature = "test-runtime") {
        "telegram-video-downloader-test"
    } else {
        "telegram-video-downloader"
    };
    Ok(home.join(".local/lib").join(profile))
}

fn expected_python_path() -> Result<PathBuf> {
    Ok(expected_profile_root()?.join("music-runtime/bin/python3"))
}

fn expected_helper_path() -> Result<PathBuf> {
    Ok(expected_profile_root()?.join("libexec/music_transfer.py"))
}

fn expected_credentials_path() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| anyhow::anyhow!("account home directory is unavailable"))?;
    Ok(home.join(".config/telegram-video-downloader/mtproto.json"))
}

fn verify_absolute_interpreter(path: &Path) -> Result<()> {
    if !path.is_absolute() || !path.is_file() {
        bail!("Telegram music Python runtime is unavailable");
    }
    Ok(())
}

fn verify_regular_helper(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| anyhow::anyhow!("Telegram music helper is unavailable"))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        bail!("Telegram music helper must be an owner-private regular file");
    }
    Ok(())
}

fn path_text(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("Telegram music transfer path is not UTF-8"))
}

fn verify_published_output(path: &Path, expected_size: u64) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| anyhow::anyhow!("Telegram music output is unavailable after transfer"))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.len() != expected_size
        || metadata.nlink() != 1
    {
        bail!("Telegram music output did not pass its file integrity checks");
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    fn fake_interpreter(root: &Path, body: &str) -> (PathBuf, PathBuf) {
        let interpreter = root.join("fake-python");
        let helper = root.join("music_transfer.py");
        std::fs::write(&interpreter, format!("#!/bin/sh\n{body}\n"))
            .expect("fake interpreter should be written");
        std::fs::set_permissions(&interpreter, std::fs::Permissions::from_mode(0o700))
            .expect("fake interpreter should be executable");
        std::fs::write(&helper, "placeholder\n").expect("helper placeholder should be written");
        (interpreter, helper)
    }

    fn request_json() -> Vec<u8> {
        br#"{"schema":1}"#.to_vec()
    }

    #[test]
    fn helper_output_is_capped_and_never_returned_in_errors() {
        let temp = TempDir::new().expect("temporary directory should be available");
        let (python, helper) = fake_interpreter(
            temp.path(),
            "i=0; while [ \"$i\" -lt 128 ]; do printf '%2048s' ''; i=$((i + 1)); done",
        );
        let cancel = MusicCancellation::default();
        let error = run_helper_process(
            &python,
            &helper,
            request_json(),
            &cancel,
            Duration::from_secs(5),
        )
        .expect_err("oversized output should fail");
        assert!(error.to_string().contains("output limit"));
        assert!(!error.to_string().contains("schema"));
    }

    #[test]
    fn helper_walltime_is_bounded_and_the_child_is_reaped() {
        let temp = TempDir::new().expect("temporary directory should be available");
        let (python, helper) = fake_interpreter(temp.path(), "while :; do :; done");
        let cancel = MusicCancellation::default();
        let started = Instant::now();
        let error = run_helper_process(
            &python,
            &helper,
            request_json(),
            &cancel,
            Duration::from_millis(100),
        )
        .expect_err("helper should time out");
        assert!(error.to_string().contains("time limit"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn cancellation_kills_the_helper_process() {
        let temp = TempDir::new().expect("temporary directory should be available");
        let (python, helper) = fake_interpreter(temp.path(), "while :; do :; done");
        let cancel = MusicCancellation::default();
        let worker_cancel = cancel.clone();
        let worker_python = python.clone();
        let worker_helper = helper.clone();
        let worker = thread::spawn(move || {
            run_helper_process(
                &worker_python,
                &worker_helper,
                request_json(),
                &worker_cancel,
                Duration::from_secs(10),
            )
        });
        thread::sleep(Duration::from_millis(100));
        cancel.cancel();
        let error = worker
            .join()
            .expect("worker should join")
            .expect_err("cancelled helper should fail");
        assert!(error.to_string().contains("cancelled"));
    }

    #[test]
    fn job_paths_are_limited_to_manager_generated_original_names() {
        let temp = TempDir::new().expect("temporary directory should be available");
        let staging = temp.path().join(".telegram-video-downloader-staging/music");
        let job = staging.join("mj1-0123456789abcdef01234567");
        std::fs::create_dir_all(&job).expect("staging directories should be created");
        std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o700))
            .expect("staging root should be private");
        std::fs::set_permissions(&job, std::fs::Permissions::from_mode(0o700))
            .expect("job directory should be private");
        let settings = MusicSettings {
            enabled: true,
            staging_dir: staging,
            ..MusicSettings::default()
        };
        assert!(validate_transfer_paths(&settings, &job.join("original.m4a")).is_ok());
        assert!(validate_transfer_paths(&settings, &job.join("original.flac")).is_ok());
        assert!(validate_transfer_paths(&settings, &job.join("original.wav")).is_ok());
        assert!(validate_transfer_paths(&settings, &job.join("input.mp3")).is_err());
        assert!(validate_transfer_paths(&settings, &job.join("../original.mp3")).is_err());
    }

    #[test]
    fn cancellation_state_can_be_shared_with_the_worker() {
        let cancel = MusicCancellation::default();
        let worker_cancel = Arc::new(cancel.clone());
        let worker = Arc::clone(&worker_cancel);
        cancel.cancel();
        assert!(worker.is_cancelled());
    }
}
