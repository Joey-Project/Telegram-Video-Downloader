//! Fixed-identity Apple Music permission, state preflight, and test-only quit.
//!
//! This module does not launch Music, play audio, or change playback settings.
//! The test-runtime `quit` mode only requests a normal Music quit.

use anyhow::{Context, bail};
use serde_json::{Value, json};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};

const MUSIC_BUNDLE_ID: &str = "com.apple.Music";
const MUSIC_QUERY_TIMEOUT_SECS: u64 = 7;

/// A safe, non-prompting result for production Music permission checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MusicPermissionError {
    NeedsAuthorization,
    Denied,
    MusicNotRunning,
}

impl Display for MusicPermissionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NeedsAuthorization => formatter
                .write_str("Apple Music permission needs an explicit operator authorization"),
            Self::Denied => formatter.write_str("Apple Music permission is denied"),
            Self::MusicNotRunning => {
                formatter.write_str("Apple Music did not start for the fixed bundle")
            }
        }
    }
}

impl Error for MusicPermissionError {}

#[cfg(feature = "test-runtime")]
const EXPECTED_BINARY_RELATIVE_PATH: &str =
    ".local/lib/telegram-video-downloader-test/bin/telegram-video-downloader-test";
#[cfg(not(feature = "test-runtime"))]
const EXPECTED_BINARY_RELATIVE_PATH: &str =
    ".local/lib/telegram-video-downloader/bin/telegram-video-downloader";

#[cfg(feature = "test-runtime")]
const EXPECTED_BUNDLE_ID: &str = "io.github.telegram-local-downloader.bot.test";
#[cfg(not(feature = "test-runtime"))]
const EXPECTED_BUNDLE_ID: &str = "io.github.telegram-local-downloader.bot";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Check,
    Authorize,
    Quit,
}

impl Mode {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "check" => Ok(Self::Check),
            "authorize" => Ok(Self::Authorize),
            "quit" => Ok(Self::Quit),
            _ => bail!("music preflight mode must be 'check', 'authorize', or 'quit'"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Check => "check",
            Self::Authorize => "authorize",
            Self::Quit => "quit",
        }
    }
}

fn ensure_mode_available(mode: Mode) -> anyhow::Result<()> {
    #[cfg(not(feature = "test-runtime"))]
    if mode == Mode::Quit {
        bail!("Music quit is available only in the test-runtime binary");
    }
    let _ = mode;
    Ok(())
}

/// Run a fixed-identity Music operation. Only `authorize` may prompt for
/// AppleEvents consent, and it does so only after verifying the executable.
pub fn run_cli(mode: &str) -> anyhow::Result<Value> {
    let mode = Mode::parse(mode)?;
    ensure_mode_available(mode)?;

    #[cfg(not(target_os = "macos"))]
    {
        let _ = mode;
        bail!("music automation preflight is supported only on macOS");
    }

    #[cfg(target_os = "macos")]
    {
        LAST_MUSIC_SCRIPT_PID.store(0, std::sync::atomic::Ordering::Relaxed);
        let expected_binary = expected_binary_path(&authenticated_home()?);
        verify_fixed_binary(
            std::env::current_exe()
                .context("resolve current executable")?
                .as_path(),
            &expected_binary,
        )?;
        let audio_muted = system_output_is_muted()?;
        if !audio_muted {
            bail!("system output is not muted; mute system audio before Music preflight");
        }

        let initial_permission = determine_permission(false)?;
        let permission = match initial_permission {
            Permission::WouldRequireConsent if mode == Mode::Authorize => {
                determine_permission(true)?
            }
            other => other,
        };

        let mut result = json!({
            "mode": mode.as_str(),
            "binary": expected_binary.display().to_string(),
            "bundleIdentifier": EXPECTED_BUNDLE_ID,
            "systemOutputMuted": true,
            "musicPermission": permission.as_str(),
            "musicQuery": null,
            "ready": false,
        });

        #[cfg(feature = "test-runtime")]
        if mode == Mode::Quit {
            let quit_requested = if permission == Permission::Granted {
                request_music_quit()?;
                true
            } else {
                false
            };
            result["quit_requested"] = json!(quit_requested);
            if let Some(pid) = last_music_script_pid() {
                result["music_script_pid"] = json!(pid);
            }
            return Ok(result);
        }

        if permission == Permission::Granted {
            match query_music_read_only() {
                Ok(query) => {
                    result["musicQuery"] = query;
                    result["ready"] = json!(true);
                }
                Err(error) => {
                    result["musicQueryError"] = json!(error.to_string());
                }
            }
        }
        if let Some(pid) = last_music_script_pid() {
            result["music_script_pid"] = json!(pid);
        }
        Ok(result)
    }
}

fn expected_binary_path(home: &Path) -> PathBuf {
    home.join(EXPECTED_BINARY_RELATIVE_PATH)
}

fn verify_fixed_binary(current_exe: &Path, expected: &Path) -> anyhow::Result<()> {
    let actual_real = std::fs::canonicalize(current_exe).with_context(|| {
        format!(
            "resolve executable path for fixed identity check: {}",
            current_exe.display()
        )
    })?;
    let expected_real = std::fs::canonicalize(expected).with_context(|| {
        format!(
            "fixed music automation executable is not installed at {}",
            expected.display()
        )
    })?;

    if actual_real != expected_real || actual_real.file_name() != expected.file_name() {
        bail!(
            "music automation must run from the fixed executable {}; current executable is {}",
            expected.display(),
            actual_real.display()
        );
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn verify_current_fixed_binary() -> anyhow::Result<PathBuf> {
    let expected = expected_binary_path(&authenticated_home()?);
    verify_fixed_binary(
        std::env::current_exe()
            .context("resolve current executable")?
            .as_path(),
        &expected,
    )?;
    Ok(expected)
}

/// Check the production executable's Music grant without prompting or checking
/// system volume state. If Music is stopped, only its fixed bundle is opened.
pub(crate) fn require_fixed_music_permission() -> anyhow::Result<()> {
    #[cfg(not(target_os = "macos"))]
    bail!("Apple Music permission checks are supported only on macOS");

    #[cfg(target_os = "macos")]
    {
        let _expected_binary = verify_current_fixed_binary()?;
        let mut permission = determine_permission(false)?;
        if permission == Permission::MusicNotRunning {
            open_music_bundle_bounded()?;
            permission = determine_permission(false)?;
        }
        match permission {
            Permission::Granted => Ok(()),
            Permission::WouldRequireConsent => {
                Err(anyhow::Error::new(MusicPermissionError::NeedsAuthorization))
            }
            Permission::Denied => Err(anyhow::Error::new(MusicPermissionError::Denied)),
            Permission::MusicNotRunning => {
                Err(anyhow::Error::new(MusicPermissionError::MusicNotRunning))
            }
        }
    }
}

/// Request AppleEvents consent only when an operator explicitly selects the
/// production authorization operation. This does not inspect or alter audio.
#[cfg(not(feature = "test-runtime"))]
pub fn request_runtime_permission() -> anyhow::Result<Value> {
    #[cfg(not(target_os = "macos"))]
    bail!("Apple Music authorization is supported only on macOS");

    #[cfg(target_os = "macos")]
    {
        let expected_binary = verify_current_fixed_binary()?;
        let mut permission = determine_permission(false)?;
        if permission == Permission::MusicNotRunning {
            open_music_bundle_bounded()?;
            permission = determine_permission(false)?;
        }
        if permission == Permission::WouldRequireConsent {
            permission = determine_permission(true)?;
        }

        let mut result = json!({
            "mode": "authorize",
            "binary": expected_binary.display().to_string(),
            "bundleIdentifier": EXPECTED_BUNDLE_ID,
            "musicPermission": permission.as_str(),
            "musicQuery": null,
            "ready": false,
        });
        if permission == Permission::Granted {
            match query_music_read_only() {
                Ok(query) => {
                    result["musicQuery"] = query;
                    result["ready"] = json!(true);
                }
                Err(error) => {
                    result["musicQueryError"] = json!(error.to_string());
                }
            }
        }
        Ok(result)
    }
}

#[cfg(target_os = "macos")]
fn open_music_bundle_bounded() -> anyhow::Result<()> {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    let mut child = Command::new("/usr/bin/open")
        .args(["-b", MUSIC_BUNDLE_ID])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("open the fixed Apple Music bundle")?;
    let deadline = Instant::now() + Duration::from_secs(MUSIC_QUERY_TIMEOUT_SECS);
    loop {
        if let Some(status) = child
            .try_wait()
            .context("wait for fixed Music open request")?
        {
            if status.success() {
                return Ok(());
            }
            bail!("opening the fixed Apple Music bundle exited with {status}");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("opening the fixed Apple Music bundle exceeded 7 seconds");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn authenticated_home() -> anyhow::Result<PathBuf> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt;

    let uid = unsafe { libc::getuid() };
    // Avoid trusting HOME, which a caller can override before launching this
    // binary. The passwd record comes from the current OS account identity.
    let mut passwd: libc::passwd = unsafe { std::mem::zeroed() };
    let mut buffer = vec![0_u8; 16 * 1024];
    let mut result = std::ptr::null_mut();
    let status = unsafe {
        libc::getpwuid_r(
            uid,
            &mut passwd,
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if status != 0 {
        bail!(
            "resolve current account home directory failed: {}",
            std::io::Error::from_raw_os_error(status)
        );
    }
    if result.is_null() || passwd.pw_dir.is_null() {
        bail!("current account has no home directory in the system account database");
    }
    let home = unsafe { CStr::from_ptr(passwd.pw_dir) };
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(home.to_bytes())))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Permission {
    Granted,
    WouldRequireConsent,
    Denied,
    MusicNotRunning,
}

impl Permission {
    fn as_str(self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::WouldRequireConsent => "needs_authorization",
            Self::Denied => "denied",
            Self::MusicNotRunning => "music_not_running",
        }
    }
}

#[cfg(target_os = "macos")]
fn determine_permission(ask_user_if_needed: bool) -> anyhow::Result<Permission> {
    use std::sync::mpsc;
    use std::time::Duration;

    let (sender, receiver) = mpsc::channel();
    std::thread::Builder::new()
        .name("music-automation-permission".into())
        .spawn(move || {
            let result = determine_permission_native(ask_user_if_needed);
            let _ = sender.send(result);
        })
        .context("start AppleEvents permission check")?;

    if ask_user_if_needed {
        // Apple documents that prompting can wait indefinitely for a user
        // decision. Keep this call off the main thread and wait for the user's
        // explicit choice without a timeout or automatic default.
        receiver
            .recv()
            .context("AppleEvents permission worker stopped")?
    } else {
        match receiver.recv_timeout(Duration::from_secs(MUSIC_QUERY_TIMEOUT_SECS)) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                bail!("AppleEvents permission check exceeded 7 seconds")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                bail!("AppleEvents permission worker stopped")
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn determine_permission_native(ask_user_if_needed: bool) -> anyhow::Result<Permission> {
    use std::ffi::c_void;

    #[repr(C)]
    struct AEDesc {
        descriptor_type: u32,
        data_handle: *mut *mut c_void,
    }

    #[link(name = "ApplicationServices", kind = "framework")]
    unsafe extern "C" {
        fn AECreateDesc(
            descriptor_type: u32,
            data: *const c_void,
            data_size: libc::c_long,
            result: *mut AEDesc,
        ) -> i16;
        fn AEDisposeDesc(desc: *mut AEDesc) -> i16;
        fn AEDeterminePermissionToAutomateTarget(
            target: *const AEDesc,
            event_class: u32,
            event_id: u32,
            ask_user_if_needed: u8,
        ) -> i32;
    }

    // AppleEvents FourCC values from AEDataModel.h / AppleEvents.h.
    const TYPE_APPLICATION_BUNDLE_ID: u32 = u32::from_be_bytes(*b"bund");
    const CORE_EVENT_CLASS: u32 = u32::from_be_bytes(*b"core");
    const GET_DATA_EVENT_ID: u32 = u32::from_be_bytes(*b"getd");
    const ERR_EVENT_NOT_PERMITTED: i32 = -1743;
    const ERR_WOULD_REQUIRE_CONSENT: i32 = -1744;
    const ERR_PROCESS_NOT_FOUND: i32 = -600;

    let mut target = AEDesc {
        descriptor_type: 0,
        data_handle: std::ptr::null_mut(),
    };
    let status = unsafe {
        AECreateDesc(
            TYPE_APPLICATION_BUNDLE_ID,
            MUSIC_BUNDLE_ID.as_ptr().cast(),
            libc::c_long::try_from(MUSIC_BUNDLE_ID.len())
                .expect("bundle identifier length fits macOS Size"),
            &mut target,
        )
    };
    if status != 0 {
        bail!("AECreateDesc failed with OSStatus {status}");
    }

    let status = unsafe {
        AEDeterminePermissionToAutomateTarget(
            &target,
            CORE_EVENT_CLASS,
            GET_DATA_EVENT_ID,
            u8::from(ask_user_if_needed),
        )
    };
    let dispose_status = unsafe { AEDisposeDesc(&mut target) };
    if dispose_status != 0 {
        bail!("AEDisposeDesc failed with OSStatus {dispose_status}");
    }

    match status {
        0 => Ok(Permission::Granted),
        ERR_WOULD_REQUIRE_CONSENT => Ok(Permission::WouldRequireConsent),
        ERR_EVENT_NOT_PERMITTED => Ok(Permission::Denied),
        ERR_PROCESS_NOT_FOUND => Ok(Permission::MusicNotRunning),
        other => bail!("AEDeterminePermissionToAutomateTarget failed with OSStatus {other}"),
    }
}

#[cfg(any(target_os = "macos", test))]
fn bounded_probe<T, F>(
    stage: &'static str,
    timeout: std::time::Duration,
    probe: F,
) -> anyhow::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
    use std::sync::mpsc;

    let (sender, receiver) = mpsc::channel();
    std::thread::Builder::new()
        .name("music-preflight-bounded-probe".into())
        .spawn(move || {
            let _ = sender.send(probe());
        })
        .context("start bounded preflight probe")?;

    match receiver.recv_timeout(timeout) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.context(format!("{stage} failed"))),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let milliseconds = timeout.as_millis();
            if milliseconds.is_multiple_of(1_000) {
                bail!("{stage} exceeded {} seconds", milliseconds / 1_000);
            }
            bail!("{stage} exceeded {milliseconds} ms");
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            bail!("{stage} worker stopped without returning a result")
        }
    }
}

#[cfg(target_os = "macos")]
fn system_output_is_muted() -> anyhow::Result<bool> {
    bounded_probe(
        "system output mute check",
        std::time::Duration::from_secs(7),
        system_output_is_muted_native,
    )
}

#[cfg(target_os = "macos")]
fn system_output_is_muted_native() -> anyhow::Result<bool> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create bounded system mute utility runtime")?
        .block_on(run_fixed_system_mute_query())
}

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
const NORMAL_MUSIC_QUIT_SCRIPT: &str = r#"with timeout of 20 seconds
  tell application id "com.apple.Music" to quit
end timeout
return "quit_requested""#;

#[cfg(all(feature = "test-runtime", target_os = "macos"))]
fn request_music_quit() -> anyhow::Result<()> {
    use std::time::Duration;

    let response = bounded_probe("Music normal quit", Duration::from_secs(27), || {
        execute_owned_music_script(NORMAL_MUSIC_QUIT_SCRIPT.into(), "Music.quit", 25)
    })?;

    if response.trim() != "quit_requested" {
        bail!("Music quit returned unexpected response {response:?}");
    }
    Ok(())
}

#[cfg(any(test, target_os = "macos"))]
fn parse_mute_text(text: &str) -> anyhow::Result<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        other => bail!("system output mute query returned unexpected value {other:?}"),
    }
}

#[cfg(target_os = "macos")]
const SYSTEM_MUTE_OSASCRIPT: &str = "output muted of (get volume settings)";
#[cfg(target_os = "macos")]
const OSASCRIPT_PATH: &str = "/usr/bin/osascript";
#[cfg(target_os = "macos")]
const SYSTEM_MUTE_STDOUT_LIMIT: usize = 128;
#[cfg(target_os = "macos")]
const SYSTEM_MUTE_STDERR_LIMIT: usize = 2048;
#[cfg(target_os = "macos")]
const SYSTEM_MUTE_UTILITY_TIMEOUT_SECS: u64 = 5;
#[cfg(target_os = "macos")]
const MUSIC_SCRIPT_SOURCE_LIMIT: usize = 16 * 1024;
#[cfg(target_os = "macos")]
const MUSIC_SCRIPT_STDOUT_LIMIT: usize = 64 * 1024;
#[cfg(target_os = "macos")]
const MUSIC_SCRIPT_STDERR_LIMIT: usize = 8 * 1024;

#[cfg(target_os = "macos")]
static LAST_MUSIC_SCRIPT_PID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
enum ScriptDelivery {
    EvalArgument,
    Stdin,
}

#[cfg(any(test, target_os = "macos"))]
async fn read_capped<R>(mut reader: R, limit: usize, stream: &str) -> anyhow::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut output = Vec::with_capacity(limit.min(2048));
    let mut buffer = [0_u8; 512];
    loop {
        let remaining_with_sentinel = limit.saturating_sub(output.len()).saturating_add(1);
        let read_size = remaining_with_sentinel.min(buffer.len());
        let count = reader.read(&mut buffer[..read_size]).await?;
        if count == 0 {
            return Ok(output);
        }
        output.extend_from_slice(&buffer[..count]);
        if output.len() > limit {
            bail!("osascript {stream} exceeded its {limit}-byte limit");
        }
    }
}

#[cfg(target_os = "macos")]
async fn run_osascript_child(
    script: &str,
    delivery: ScriptDelivery,
    stage: &'static str,
    timeout_secs: u64,
    stdout_limit: usize,
    stderr_limit: usize,
    audit_music_child: bool,
) -> anyhow::Result<(Vec<u8>, Vec<u8>, std::process::ExitStatus)> {
    use std::process::{ExitStatus, Stdio};
    use std::time::Duration;
    use tokio::process::{Child, Command};

    async fn kill_and_reap(child: &mut Child) {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }

    if matches!(delivery, ScriptDelivery::Stdin) && script.len() > MUSIC_SCRIPT_SOURCE_LIMIT {
        bail!("{stage} script exceeds its {MUSIC_SCRIPT_SOURCE_LIMIT}-byte limit");
    }

    let mut command = Command::new(OSASCRIPT_PATH);
    match delivery {
        ScriptDelivery::EvalArgument => {
            command.arg("-e").arg(script).stdin(Stdio::null());
        }
        ScriptDelivery::Stdin => {
            command
                .arg("-l")
                .arg("AppleScript")
                .arg("-")
                .stdin(Stdio::piped());
        }
    }
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("start fixed osascript child for {stage}"))?;
    let child_pid = match child.id() {
        Some(pid) => pid,
        None => {
            kill_and_reap(&mut child).await;
            bail!("{stage} osascript child did not expose a PID");
        }
    };
    if audit_music_child {
        LAST_MUSIC_SCRIPT_PID.store(child_pid, std::sync::atomic::Ordering::Relaxed);
        eprintln!("Music AppleScript child pid={child_pid} stage={stage}");
    }
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            kill_and_reap(&mut child).await;
            bail!("{stage} osascript stdout pipe was unavailable");
        }
    };
    let stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            kill_and_reap(&mut child).await;
            bail!("{stage} osascript stderr pipe was unavailable");
        }
    };
    let mut stdin = match delivery {
        ScriptDelivery::EvalArgument => None,
        ScriptDelivery::Stdin => match child.stdin.take() {
            Some(stdin) => Some(stdin),
            None => {
                kill_and_reap(&mut child).await;
                bail!("{stage} osascript stdin pipe was unavailable");
            }
        },
    };

    let write_script = async {
        if let Some(mut input) = stdin.take() {
            use tokio::io::AsyncWriteExt;
            input
                .write_all(script.as_bytes())
                .await
                .with_context(|| format!("write fixed script to {stage} osascript stdin"))?;
            input
                .shutdown()
                .await
                .with_context(|| format!("close {stage} osascript stdin"))?;
        }
        Ok::<(), anyhow::Error>(())
    };

    let operation = async {
        let (stdout, stderr, status, ()): (Vec<u8>, Vec<u8>, ExitStatus, ()) = tokio::try_join!(
            read_capped(stdout, stdout_limit, "stdout"),
            read_capped(stderr, stderr_limit, "stderr"),
            async {
                child
                    .wait()
                    .await
                    .with_context(|| format!("wait for {stage} osascript child"))
            },
            write_script,
        )?;
        Ok::<_, anyhow::Error>((stdout, stderr, status))
    };

    let completed = tokio::time::timeout(Duration::from_secs(timeout_secs), operation).await;
    let (stdout, stderr, status) = match completed {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            kill_and_reap(&mut child).await;
            return Err(error.context(format!("{stage} osascript failed")));
        }
        Err(_) => {
            kill_and_reap(&mut child).await;
            bail!("{stage} osascript exceeded {timeout_secs} seconds");
        }
    };

    Ok((stdout, stderr, status))
}

#[cfg(target_os = "macos")]
fn osascript_text_result(
    stage: &'static str,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    status: std::process::ExitStatus,
) -> anyhow::Result<String> {
    if !status.success() {
        let diagnostic = String::from_utf8_lossy(&stderr).trim().to_owned();
        bail!("{stage} osascript exited with {status}: {diagnostic}");
    }
    String::from_utf8(stdout).with_context(|| format!("{stage} osascript stdout was not UTF-8"))
}

#[cfg(target_os = "macos")]
async fn run_fixed_system_mute_query() -> anyhow::Result<bool> {
    let (stdout, stderr, status) = run_osascript_child(
        SYSTEM_MUTE_OSASCRIPT,
        ScriptDelivery::EvalArgument,
        "system-mute",
        SYSTEM_MUTE_UTILITY_TIMEOUT_SECS,
        SYSTEM_MUTE_STDOUT_LIMIT,
        SYSTEM_MUTE_STDERR_LIMIT,
        false,
    )
    .await?;
    parse_mute_text(&osascript_text_result(
        "system-mute",
        stdout,
        stderr,
        status,
    )?)
}

#[cfg(target_os = "macos")]
pub(crate) fn execute_owned_music_script(
    source: String,
    stage: &'static str,
    timeout_secs: u64,
) -> anyhow::Result<String> {
    if source.len() > MUSIC_SCRIPT_SOURCE_LIMIT {
        bail!("{stage} script exceeds its {MUSIC_SCRIPT_SOURCE_LIMIT}-byte limit");
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .with_context(|| format!("create {stage} osascript runtime"))?;
    let (stdout, stderr, status) = runtime.block_on(run_osascript_child(
        &source,
        ScriptDelivery::Stdin,
        stage,
        timeout_secs,
        MUSIC_SCRIPT_STDOUT_LIMIT,
        MUSIC_SCRIPT_STDERR_LIMIT,
        true,
    ))?;
    osascript_text_result(stage, stdout, stderr, status)
}

#[cfg(target_os = "macos")]
pub(crate) fn last_music_script_pid() -> Option<u32> {
    match LAST_MUSIC_SCRIPT_PID.load(std::sync::atomic::Ordering::Relaxed) {
        0 => None,
        pid => Some(pid),
    }
}

#[cfg(target_os = "macos")]
fn query_music_read_only() -> anyhow::Result<Value> {
    use std::sync::mpsc;
    use std::time::Duration;

    let (sender, receiver) = mpsc::channel();
    std::thread::Builder::new()
        .name("music-read-only-preflight".into())
        .spawn(move || {
            let source = "set text item delimiters to {tab}\n\
                          tell application id \"com.apple.Music\"\n\
                          set v to version as string\n\
                          set s to player state as string\n\
                          set m to mute as string\n\
                          set e to name of every encoder\n\
                          set e to e as text\n\
                          return v & tab & s & tab & m & tab & e\n\
                          end tell"
                .to_owned();
            let result =
                execute_owned_music_script(source, "Music.preflight", MUSIC_QUERY_TIMEOUT_SECS);
            let _ = sender.send(result);
        })
        .context("start read-only Music query")?;

    let text = match receiver.recv_timeout(Duration::from_secs(MUSIC_QUERY_TIMEOUT_SECS + 2)) {
        Ok(result) => result?,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            bail!("Music preflight worker exceeded 9 seconds")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => bail!("Music query worker stopped"),
    };
    let mut fields = text.splitn(4, '\t');
    let version = fields.next().unwrap_or_default().trim();
    let player_state = fields.next().unwrap_or_default().trim();
    let muted = fields.next().unwrap_or_default().trim();
    let encoders = fields.next().unwrap_or_default().trim();
    if version.is_empty() || player_state.is_empty() || muted.is_empty() {
        bail!("Music read-only query returned incomplete state");
    }
    Ok(json!({
        "version": version,
        "playerState": player_state,
        "musicMuted": muted.eq_ignore_ascii_case("true"),
        "encoders": encoders.split('\t').filter(|name| !name.is_empty()).collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_fixed_modes() {
        assert_eq!(Mode::parse("check").unwrap(), Mode::Check);
        assert_eq!(Mode::parse("authorize").unwrap(), Mode::Authorize);
        assert_eq!(Mode::parse("quit").unwrap(), Mode::Quit);
        assert!(Mode::parse("prompt").is_err());
    }

    #[test]
    fn quit_mode_is_gated_to_test_runtime() {
        let result = ensure_mode_available(Mode::Quit);
        #[cfg(feature = "test-runtime")]
        assert!(result.is_ok());
        #[cfg(not(feature = "test-runtime"))]
        assert!(result.is_err());
    }

    #[cfg(all(feature = "test-runtime", target_os = "macos"))]
    #[test]
    fn normal_quit_script_only_requests_a_bounded_quit() {
        assert!(NORMAL_MUSIC_QUIT_SCRIPT.contains("with timeout of 20 seconds"));
        assert!(
            NORMAL_MUSIC_QUIT_SCRIPT.contains("tell application id \"com.apple.Music\" to quit")
        );
        assert!(NORMAL_MUSIC_QUIT_SCRIPT.contains("return \"quit_requested\""));
        assert!(!NORMAL_MUSIC_QUIT_SCRIPT.contains("launch"));
        assert!(!NORMAL_MUSIC_QUIT_SCRIPT.contains("play"));
        assert!(!NORMAL_MUSIC_QUIT_SCRIPT.contains("set volume"));
    }

    #[test]
    fn permission_statuses_are_stable() {
        assert_eq!(Permission::Granted.as_str(), "granted");
        assert_eq!(
            Permission::WouldRequireConsent.as_str(),
            "needs_authorization"
        );
        assert_eq!(Permission::Denied.as_str(), "denied");
        assert_eq!(Permission::MusicNotRunning.as_str(), "music_not_running");
    }

    #[test]
    fn system_mute_parser_accepts_only_boolean_text() {
        assert!(parse_mute_text("true").unwrap());
        assert!(!parse_mute_text(" false \n").unwrap());
        assert!(parse_mute_text("unknown").is_err());
        assert!(parse_mute_text("").is_err());
    }

    #[test]
    fn mute_utility_output_reader_rejects_bytes_over_the_limit() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use tokio::io::AsyncWriteExt;

            let (mut writer, reader) = tokio::io::duplex(16);
            writer.write_all(b"12345").await.unwrap();
            drop(writer);
            let error = read_capped(reader, 4, "stdout").await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("stdout exceeded its 4-byte limit")
            );
        });
    }

    #[test]
    fn runtime_metadata_uses_the_build_selected_identity() {
        let home = Path::new("/Users/example");
        #[cfg(feature = "test-runtime")]
        {
            assert_eq!(
                EXPECTED_BUNDLE_ID,
                "io.github.telegram-local-downloader.bot.test"
            );
            assert_eq!(
                expected_binary_path(home),
                Path::new(
                    "/Users/example/.local/lib/telegram-video-downloader-test/bin/telegram-video-downloader-test"
                )
            );
        }
        #[cfg(not(feature = "test-runtime"))]
        {
            assert_eq!(
                EXPECTED_BUNDLE_ID,
                "io.github.telegram-local-downloader.bot"
            );
            assert_eq!(
                expected_binary_path(home),
                Path::new(
                    "/Users/example/.local/lib/telegram-video-downloader/bin/telegram-video-downloader"
                )
            );
        }
    }

    #[test]
    fn bounded_probe_returns_success_and_preserves_probe_errors() {
        assert!(
            bounded_probe("test probe", std::time::Duration::from_secs(1), || {
                Ok(true)
            })
            .unwrap()
        );

        let error =
            bounded_probe::<bool, _>("test probe", std::time::Duration::from_secs(1), || {
                anyhow::bail!("raw probe status 17")
            })
            .unwrap_err();
        assert!(error.to_string().contains("test probe failed"));
        assert!(format!("{error:#}").contains("raw probe status 17"));
    }

    #[test]
    fn bounded_probe_timeout_fails_closed_and_releases_worker() {
        use std::sync::mpsc;

        let (release_sender, release_receiver) = mpsc::channel();
        let (finished_sender, finished_receiver) = mpsc::channel();
        let error = bounded_probe(
            "system output mute check",
            std::time::Duration::from_millis(20),
            move || {
                release_receiver
                    .recv()
                    .expect("test releases blocked probe");
                let _ = finished_sender.send(());
                Ok(true)
            },
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("system output mute check exceeded 20 ms")
        );
        release_sender.send(()).unwrap();
        finished_receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("timed out test worker should exit after release");
    }
}
