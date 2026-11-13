use crate::protocol::{MAX_EVENT_BYTES, MAX_REQUEST_BYTES};
use crate::types::{Command, ConversionReport, ConvertRequest, Event, PROTOCOL_VERSION};
use anyhow::{Context, Result, bail, ensure};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

const MAX_STDOUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_STDERR_BYTES: usize = 256 * 1024;

/// Typed client with hard process deadlines. It needs no native audio libraries.
#[derive(Debug, Clone)]
pub struct ConverterClient {
    executable: PathBuf,
}

impl ConverterClient {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
        }
    }
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    pub fn convert(
        &self,
        request: &ConvertRequest,
        cancelled: &AtomicBool,
        observe: &mut dyn FnMut(&Event) -> Result<()>,
    ) -> Result<ConversionReport> {
        ensure!(
            (1..=86_400).contains(&request.timeout_seconds),
            "invalid timeout_seconds"
        );
        ensure!(
            request.version == PROTOCOL_VERSION,
            "unsupported protocol version"
        );
        ensure!(!cancelled.load(Ordering::Acquire), "converter cancelled");
        let mut command = std::process::Command::new(&self.executable);
        command
            .arg("--json")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let child = command.spawn().context("start converter worker")?;
        let mut child = OwnedChild::new(child);
        let mut stdin = child.child.stdin.take().context("worker stdin missing")?;
        set_nonblocking_fd(stdin.as_raw_fd())?;
        let stdout = child.child.stdout.take().context("worker stdout missing")?;
        let stderr = child.child.stderr.take().context("worker stderr missing")?;
        set_nonblocking_fd(stdout.as_raw_fd())?;
        set_nonblocking_fd(stderr.as_raw_fd())?;
        let (sender, events) = mpsc::sync_channel::<Result<Option<Event>, String>>(16);
        let reader_shutdown = Arc::new(AtomicBool::new(false));
        let stdout_shutdown = Arc::clone(&reader_shutdown);
        let stdout_reader = std::thread::spawn(move || {
            read_stdout_events(stdout, sender, stdout_shutdown);
        });
        let (diagnostic_sender, diagnostics) = mpsc::sync_channel(1);
        let stderr_overflow = Arc::new(AtomicBool::new(false));
        let reader_overflow = Arc::clone(&stderr_overflow);
        let stderr_shutdown = Arc::clone(&reader_shutdown);
        let stderr_reader = std::thread::spawn(move || {
            let mut reader = stderr;
            let mut captured = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                if stderr_shutdown.load(Ordering::Acquire) {
                    break;
                }
                let count = match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => count,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                };
                let room = MAX_STDERR_BYTES.saturating_sub(captured.len());
                captured.extend_from_slice(&buffer[..count.min(room)]);
                if count > room {
                    reader_overflow.store(true, Ordering::Release);
                }
            }
            let _ = diagnostic_sender.send(captured);
        });
        let mut diagnostic_capture = None;
        let outcome = (|| -> Result<ConversionReport> {
            let deadline = Instant::now() + Duration::from_secs(request.timeout_seconds);
            send_command(
                &mut stdin,
                &Command::Convert(request.clone()),
                deadline,
                Some(cancelled),
            )?;
            let mut cancellation_sent = None;
            let mut complete = None;
            let mut eof = false;
            let mut stderr_eof = false;
            loop {
                if Instant::now() >= deadline {
                    bail!("converter deadline exceeded");
                }
                ensure!(
                    !stderr_overflow.load(Ordering::Acquire),
                    "worker stderr exceeded limit"
                );
                if cancelled.load(Ordering::Acquire) && cancellation_sent.is_none() {
                    send_command(
                        &mut stdin,
                        &Command::Cancel,
                        deadline.min(Instant::now() + Duration::from_secs(2)),
                        None,
                    )?;
                    cancellation_sent = Some(Instant::now());
                }
                if cancellation_sent
                    .is_some_and(|sent: Instant| sent.elapsed() >= Duration::from_secs(2))
                {
                    bail!("converter cancellation grace exceeded");
                }
                match events.recv_timeout(Duration::from_millis(20)) {
                    Ok(Ok(Some(event))) => {
                        ensure!(
                            event.version() == PROTOCOL_VERSION,
                            "unsupported worker event version"
                        );
                        observe(&event)?;
                        match event {
                            Event::Completed { report, .. } => {
                                ensure!(complete.is_none(), "duplicate completion event");
                                ensure!(
                                    report.version == PROTOCOL_VERSION,
                                    "unsupported worker report version"
                                );
                                complete = Some(report);
                            }
                            Event::Failed { code, message, .. } => {
                                bail!("converter {code}: {message}")
                            }
                            Event::Cancelled { .. } => bail!("converter cancelled"),
                            _ => {}
                        }
                    }
                    Ok(Ok(None)) => eof = true,
                    Ok(Err(error)) => bail!("{error}"),
                    Err(RecvTimeoutError::Disconnected) => eof = true,
                    Err(RecvTimeoutError::Timeout) => {}
                }
                if !stderr_eof && let Ok(captured) = diagnostics.try_recv() {
                    stderr_eof = true;
                    diagnostic_capture = Some(captured);
                }
                if child.exited_without_reaping()? && eof && stderr_eof {
                    ensure!(
                        !stderr_overflow.load(Ordering::Acquire),
                        "worker stderr exceeded limit"
                    );
                    return complete.context("converter exited without a completed event");
                }
            }
        })();
        drop(stdin);
        // A successful terminal protocol event is accepted only after stdout EOF
        // and stderr EOF plus the leader's exit, so no descendant holds either pipe.
        // Do not signal a zombie-only group there (Darwin may report EPERM for that
        // group). On every failure/timeout path, clean the group before reaping.
        let child_status = if outcome.is_err() {
            child.stop_and_wait()
        } else {
            child.wait_exited()
        };
        reader_shutdown.store(true, Ordering::Release);
        // Stop queued sends before joining, including an event-reader failure path.
        drop(events);
        let _ = stdout_reader.join();
        let _ = stderr_reader.join();
        let diagnostic = diagnostic_capture
            .or_else(|| diagnostics.try_recv().ok())
            .unwrap_or_default();
        let outcome = outcome.and_then(|report| {
            let status = child_status.context("wait for converter worker")?;
            ensure!(status.success(), "converter exited with {status}");
            Ok(report)
        });
        outcome.with_context(|| {
            if diagnostic.is_empty() {
                "converter worker failed".to_owned()
            } else {
                format!(
                    "converter worker failed: {}",
                    String::from_utf8_lossy(&diagnostic)
                )
            }
        })
    }
}

#[cfg(unix)]
fn set_nonblocking_fd(fd: std::os::fd::RawFd) -> Result<()> {
    // SAFETY: the caller owns this live pipe descriptor; only its status flags change.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    ensure!(
        flags >= 0,
        "read worker pipe flags: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: the descriptor remains live and F_SETFL takes an integer bitmask.
    let result = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    ensure!(
        result == 0,
        "set worker pipe flags: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(not(unix))]
fn set_nonblocking_fd(_: std::os::fd::RawFd) -> Result<()> {
    bail!("the supervised converter client requires Unix process and pipe support")
}

#[cfg(unix)]
fn read_stdout_events(
    mut stdout: std::process::ChildStdout,
    sender: mpsc::SyncSender<Result<Option<crate::types::Event>, String>>,
    shutdown: Arc<AtomicBool>,
) {
    let mut line = Vec::new();
    let mut total = 0usize;
    let mut buffer = [0_u8; 8192];
    loop {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        match stdout.read(&mut buffer) {
            Ok(0) => {
                if !line.is_empty() && !send_event_line(&sender, &mut line) {
                    break;
                }
                let _ = sender.send(Ok(None));
                break;
            }
            Ok(count) => {
                total = total.saturating_add(count);
                if total > MAX_STDOUT_BYTES {
                    let _ = sender.send(Err("worker stdout exceeded limit".to_owned()));
                    break;
                }
                let mut failed = false;
                for byte in &buffer[..count] {
                    line.push(*byte);
                    if line.len() > MAX_EVENT_BYTES {
                        let _ = sender.send(Err("protocol line exceeds limit".to_owned()));
                        failed = true;
                        break;
                    }
                    if *byte == b'\n' && !send_event_line(&sender, &mut line) {
                        failed = true;
                        break;
                    }
                }
                if failed {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => {
                let _ = sender.send(Err(error.to_string()));
                break;
            }
        }
    }
}

#[cfg(unix)]
fn send_event_line(
    sender: &mpsc::SyncSender<Result<Option<crate::types::Event>, String>>,
    line: &mut Vec<u8>,
) -> bool {
    let result = serde_json::from_slice(line)
        .map(Some)
        .map_err(|error| format!("invalid worker event: {error}"));
    line.clear();
    sender.send(result).is_ok()
}

fn send_command(
    stdin: &mut std::process::ChildStdin,
    command: &Command,
    deadline: Instant,
    cancelled: Option<&AtomicBool>,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(command)?;
    ensure!(
        bytes.len() < MAX_REQUEST_BYTES,
        "converter request exceeds limit"
    );
    bytes.push(b'\n');
    let mut remaining = bytes.as_slice();
    while !remaining.is_empty() {
        ensure!(
            Instant::now() < deadline,
            "converter deadline exceeded while sending command"
        );
        ensure!(
            !cancelled.is_some_and(|value| value.load(Ordering::Acquire)),
            "converter cancelled"
        );
        match stdin.write(remaining) {
            Ok(0) => bail!("worker stdin closed"),
            Ok(written) => remaining = &remaining[written..],
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5))
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error).context("write converter command"),
        }
    }
    Ok(())
}

struct OwnedChild {
    child: Child,
    reaped: bool,
}

impl OwnedChild {
    fn new(child: Child) -> Self {
        Self {
            child,
            reaped: false,
        }
    }

    fn exited_without_reaping(&self) -> Result<bool> {
        #[cfg(unix)]
        {
            loop {
                // WNOWAIT reports exit without releasing the PID. Keeping the
                // leader unreaped pins its PID/PGID until all group signals finish.
                // SAFETY: waitid initializes siginfo on success; the child is our
                // direct child and the options request a nonblocking exit query.
                let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
                let result = unsafe {
                    libc::waitid(
                        libc::P_PID,
                        self.child.id() as libc::id_t,
                        &mut info,
                        libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                    )
                };
                if result == 0 {
                    // SAFETY: waitid succeeded and initialized the siginfo value.
                    return Ok(unsafe { info.si_pid() } != 0);
                }
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error).context("observe converter worker exit");
            }
        }
        #[cfg(not(unix))]
        {
            bail!("the supervised converter client requires Unix process support")
        }
    }

    fn stop_and_wait(&mut self) -> Result<std::process::ExitStatus> {
        if self.reaped {
            return Ok(self.child.wait()?);
        }
        let mut signal_error = None;
        #[cfg(unix)]
        {
            let group = -(self.child.id() as libc::pid_t);
            // The leader has not been reaped, so this PGID cannot be recycled
            // between observing its exit and signaling descendants that inherited
            // the pipes. ESRCH simply means the group has already disappeared.
            if let Err(error) = signal_group(group, libc::SIGTERM) {
                signal_error = Some(anyhow::anyhow!(
                    "send SIGTERM to converter process group: {error}"
                ));
            }
            std::thread::sleep(Duration::from_millis(50));
            if let Err(error) = signal_group(group, libc::SIGKILL)
                && signal_error.is_none()
            {
                signal_error = Some(anyhow::anyhow!(
                    "send SIGKILL to converter process group: {error}"
                ));
            }
        }
        let _ = self.child.kill();
        let status = self.child.wait().context("reap converter worker")?;
        self.reaped = true;
        if let Some(error) = signal_error {
            return Err(error);
        }
        Ok(status)
    }

    fn wait_exited(&mut self) -> Result<std::process::ExitStatus> {
        ensure!(
            self.exited_without_reaping()?,
            "converter worker did not exit after terminal protocol event"
        );
        let status = self.child.wait().context("reap converter worker")?;
        self.reaped = true;
        Ok(status)
    }
}

#[cfg(unix)]
fn signal_group(group: libc::pid_t, signal: libc::c_int) -> std::io::Result<()> {
    // SAFETY: group is the negative ID of the process group created for this
    // worker; OwnedChild retains the unreaped leader until cleanup completes.
    if unsafe { libc::kill(group, signal) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.stop_and_wait();
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn worker(script: &str) -> (tempfile::TempDir, ConverterClient) {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("protocol-fixture.sh");
        std::fs::write(&executable, format!("#!/bin/sh\n{script}\n")).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        (directory, ConverterClient::new(executable))
    }

    #[test]
    fn deadline_kills_and_reaps_a_stalled_worker() {
        let (_directory, client) = worker("exec /bin/sleep 30");
        let mut request = ConvertRequest::new("unused.flac", "unused.m4a");
        request.timeout_seconds = 1;
        let started = Instant::now();
        let error = client
            .convert(&request, &AtomicBool::new(false), &mut |_| Ok(()))
            .unwrap_err();
        assert!(format!("{error:#}").contains("deadline exceeded"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn immediate_worker_exit_cleans_pipe_holding_descendant_within_deadline() {
        let directory = tempfile::tempdir().unwrap();
        let pid_file = directory.path().join("descendant.pid");
        let quoted_pid_file = format!("'{}'", pid_file.to_string_lossy().replace('\'', "'\\''"));
        let (_fixture, client) = worker(&format!(
            "IFS= read -r request\n/bin/sleep 30 &\necho $! > {quoted_pid_file}\nexit 0"
        ));
        let mut request = ConvertRequest::new("unused.flac", "unused.m4a");
        request.timeout_seconds = 5;
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        let started = Instant::now();
        let conversion = std::thread::spawn(move || {
            client.convert(&request, &worker_cancelled, &mut |_| Ok(()))
        });

        // The pipe deadline includes process startup. Observe the marker from the
        // wrapper before awaiting timeout, so the test proves a pipe-holding child
        // existed and was not merely testing a worker that never got scheduled.
        let startup_deadline = started + Duration::from_secs(4);
        let mut descendant_pid = None;
        while descendant_pid.is_none()
            && !conversion.is_finished()
            && Instant::now() < startup_deadline
        {
            descendant_pid = std::fs::read_to_string(&pid_file)
                .ok()
                .and_then(|contents| contents.trim().parse::<libc::pid_t>().ok());
            if descendant_pid.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let descendant_started = descendant_pid.is_some();
        let descendant_was_running = descendant_pid.is_some_and(process_is_running);
        if !descendant_started {
            // Ensure a delayed or wedged fixture is still cleaned up before the
            // assertion reports that its startup handshake failed.
            cancelled.store(true, Ordering::Release);
        }
        let result = conversion.join().unwrap();
        assert!(
            descendant_started,
            "fixture did not start its descendant in time"
        );
        assert!(
            descendant_was_running,
            "fixture descendant was not running at startup"
        );
        let error = result.unwrap_err();
        assert!(format!("{error:#}").contains("deadline exceeded"));
        assert!(started.elapsed() < Duration::from_secs(7));

        let pid = descendant_pid.unwrap();
        assert!(
            !process_is_running(pid),
            "descendant {pid} is still running"
        );
    }

    #[test]
    fn normal_worker_success_still_returns_its_report() {
        let report = ConversionReport {
            version: PROTOCOL_VERSION,
            source: crate::types::AudioSpec {
                sample_rate: 48_000,
                channels: 2,
                bits_per_sample: 24,
                frames: 1,
            },
            output: crate::types::AudioSpec {
                sample_rate: 48_000,
                channels: 2,
                bits_per_sample: 24,
                frames: 1,
            },
            output_path: PathBuf::from("unused.m4a"),
            input_sha256: "a".repeat(64),
            output_sha256: "b".repeat(64),
            pcm_sha256: "c".repeat(64),
            audio_bit_exact: true,
            metadata: Default::default(),
            warnings: Vec::new(),
        };
        let event = Event::Completed {
            version: PROTOCOL_VERSION,
            report: report.clone(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let (_directory, client) =
            worker(&format!("IFS= read -r request\nprintf '%s\\n' '{json}'"));
        let result = client
            .convert(
                &ConvertRequest::new("unused.flac", "unused.m4a"),
                &AtomicBool::new(false),
                &mut |_| Ok(()),
            )
            .unwrap();
        assert_eq!(result.output_path, report.output_path);
        assert!(result.audio_bit_exact);
    }

    #[test]
    fn active_cancellation_still_reaches_worker_and_returns_cancelled() {
        let (_directory, client) = worker(
            "IFS= read -r request\nIFS= read -r cancel\nprintf '%s\\n' '{\"event\":\"cancelled\",\"version\":1,\"partial_output\":null}'",
        );
        let cancelled = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&cancelled);
        let trigger = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            signal.store(true, Ordering::Release);
        });
        let error = client
            .convert(
                &ConvertRequest::new("unused.flac", "unused.m4a"),
                &cancelled,
                &mut |_| Ok(()),
            )
            .unwrap_err();
        trigger.join().unwrap();
        assert!(format!("{error:#}").contains("converter cancelled"));
    }

    #[test]
    fn blocked_request_pipe_obeys_the_same_deadline() {
        let (_directory, client) = worker("exec /bin/sleep 30");
        let mut request = ConvertRequest::new("a".repeat(512 * 1024), "unused.m4a");
        request.timeout_seconds = 1;
        let started = Instant::now();
        let error = client
            .convert(&request, &AtomicBool::new(false), &mut |_| Ok(()))
            .unwrap_err();
        assert!(format!("{error:#}").contains("deadline exceeded"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn rejects_unknown_event_version() {
        let (_directory, client) = worker(
            "printf '%s\\n' '{\"event\":\"cancelled\",\"version\":99,\"partial_output\":null}'",
        );
        let error = client
            .convert(
                &ConvertRequest::new("unused.flac", "unused.m4a"),
                &AtomicBool::new(false),
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert!(format!("{error:#}").contains("unsupported worker event version"));
    }

    #[test]
    fn stderr_overflow_is_an_error() {
        let (_directory, client) =
            worker("/bin/dd if=/dev/zero bs=4096 count=80 1>&2 2>/dev/null\nexec /bin/sleep 30");
        let error = client
            .convert(
                &ConvertRequest::new("unused.flac", "unused.m4a"),
                &AtomicBool::new(false),
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert!(format!("{error:#}").contains("worker stderr exceeded limit"));
    }

    #[test]
    fn already_cancelled_request_does_not_spawn() {
        let client = ConverterClient::new("/does/not/exist");
        let error = client
            .convert(
                &ConvertRequest::new("unused.flac", "unused.m4a"),
                &AtomicBool::new(true),
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert!(format!("{error:#}").contains("converter cancelled"));
    }

    fn process_is_running(pid: libc::pid_t) -> bool {
        // SAFETY: kill(pid, 0) performs an existence/permission check only.
        if unsafe { libc::kill(pid, 0) } != 0 {
            return false;
        }
        let output = std::process::Command::new("/bin/ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        let state = String::from_utf8_lossy(&output.stdout);
        !state.trim().is_empty() && !state.trim_start().starts_with('Z')
    }
}
