use crate::protocol::{MAX_EVENT_BYTES, MAX_REQUEST_BYTES, read_line};
use crate::types::{Command, ConversionReport, ConvertRequest, Event, PROTOCOL_VERSION};
use anyhow::{Context, Result, bail, ensure};
use std::io::{BufReader, Read, Write};
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
        let mut child = OwnedChild(child);
        let mut stdin = child.0.stdin.take().context("worker stdin missing")?;
        set_nonblocking(&stdin)?;
        let stdout = child.0.stdout.take().context("worker stdout missing")?;
        let stderr = child.0.stderr.take().context("worker stderr missing")?;
        let (sender, events) = mpsc::sync_channel::<Result<Option<Event>, String>>(16);
        let stdout_reader = std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut total = 0;
            loop {
                let item = match read_line(&mut reader, MAX_EVENT_BYTES) {
                    Ok(Some(line)) => {
                        total += line.len();
                        if total > MAX_STDOUT_BYTES {
                            Err("worker stdout exceeded limit".to_owned())
                        } else {
                            serde_json::from_slice(&line)
                                .map(Some)
                                .map_err(|error| format!("invalid worker event: {error}"))
                        }
                    }
                    Ok(None) => {
                        let _ = sender.send(Ok(None));
                        break;
                    }
                    Err(error) => Err(error.to_string()),
                };
                let failed = item.is_err();
                if sender.send(item).is_err() || failed {
                    break;
                }
            }
        });
        let (diagnostic_sender, diagnostics) = mpsc::sync_channel(1);
        let stderr_overflow = Arc::new(AtomicBool::new(false));
        let reader_overflow = Arc::clone(&stderr_overflow);
        let stderr_reader = std::thread::spawn(move || {
            let mut reader = stderr;
            let mut captured = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let count = match reader.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(count) => count,
                };
                let room = MAX_STDERR_BYTES.saturating_sub(captured.len());
                captured.extend_from_slice(&buffer[..count.min(room)]);
                if count > room {
                    reader_overflow.store(true, Ordering::Release);
                }
            }
            let _ = diagnostic_sender.send(captured);
        });
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
                if let Some(status) = child.0.try_wait()?
                    && eof
                {
                    ensure!(status.success(), "converter exited with {status}");
                    return complete.context("converter exited without a completed event");
                }
            }
        })();
        drop(stdin);
        if outcome.is_err() {
            child.stop();
        }
        let _ = child.0.wait();
        // Stop queued sends before joining, including an event-reader failure path.
        drop(events);
        let _ = stdout_reader.join();
        let _ = stderr_reader.join();
        let diagnostic = diagnostics.try_recv().unwrap_or_default();
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
fn set_nonblocking(stdin: &std::process::ChildStdin) -> Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: stdin owns this live descriptor; only its status flags change.
    let flags = unsafe { libc::fcntl(stdin.as_raw_fd(), libc::F_GETFL) };
    ensure!(
        flags >= 0,
        "read worker stdin flags: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: the descriptor remains live and F_SETFL takes an integer bitmask.
    let result = unsafe { libc::fcntl(stdin.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) };
    ensure!(
        result == 0,
        "set worker stdin flags: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(not(unix))]
fn set_nonblocking(_: &std::process::ChildStdin) -> Result<()> {
    bail!("the supervised converter client requires Unix process and pipe support")
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

struct OwnedChild(Child);
impl OwnedChild {
    fn stop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) {
            return;
        }
        #[cfg(unix)]
        {
            // SAFETY: this PID identifies the fresh process group created above.
            unsafe {
                libc::kill(-(self.0.id() as i32), libc::SIGKILL);
            }
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            self.stop();
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
}
