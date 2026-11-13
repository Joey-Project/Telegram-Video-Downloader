use flac_alac_converter::protocol::{MAX_REQUEST_BYTES, read_line, write_line};
use flac_alac_converter::types::{Command, Event, PROTOCOL_VERSION};
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

fn emit(event: &Event) -> anyhow::Result<()> {
    write_line(&mut std::io::stdout().lock(), event)
}

fn fail(code: &str, message: String) -> i32 {
    let _ = emit(&Event::Failed {
        version: PROTOCOL_VERSION,
        code: code.into(),
        message,
        partial_output: None,
        staging_directory: None,
    });
    2
}

fn run() -> i32 {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    if arguments.len() == 1 && arguments[0] == "--version" {
        println!("flac-alac-converter {}", env!("CARGO_PKG_VERSION"));
        return 0;
    }
    if arguments.len() == 1 && arguments[0] == "--help" {
        println!(
            "Usage: flac-alac-converter [--json]\nRead one convert request as JSON Lines from stdin; an optional cancel command may follow.\nThis file-only tool never controls Music or plays audio."
        );
        return 0;
    }
    if !(arguments.is_empty() || arguments.len() == 1 && arguments[0] == "--json") {
        return fail("arguments", "expected --json, --help or --version".into());
    }
    #[cfg(unix)]
    {
        // SAFETY: restrictive umask applies only to this standalone worker process.
        unsafe {
            libc::umask(0o077);
        }
    }
    let mut reader = BufReader::new(std::io::stdin());
    let first = match read_line(&mut reader, MAX_REQUEST_BYTES) {
        Ok(Some(line)) => line,
        Ok(None) => return fail("protocol", "missing convert request".into()),
        Err(error) => return fail("protocol", error.to_string()),
    };
    let request = match serde_json::from_slice::<Command>(&first) {
        Ok(Command::Convert(request)) => request,
        Ok(Command::Cancel) => {
            let _ = emit(&Event::Cancelled {
                version: PROTOCOL_VERSION,
                partial_output: None,
                staging_directory: None,
            });
            return 130;
        }
        Err(error) => return fail("protocol", error.to_string()),
    };
    if request.version != PROTOCOL_VERSION {
        return fail("version", "unsupported protocol version".into());
    }
    if !(1..=86_400).contains(&request.timeout_seconds) {
        return fail(
            "options",
            "timeout_seconds must be between 1 and 86400".into(),
        );
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    let reason = Arc::new(AtomicU8::new(0));
    // An already-queued cancellation is deterministic, including very short files.
    while let Some(end) = reader.buffer().iter().position(|byte| *byte == b'\n') {
        let line = reader.buffer()[..=end].to_vec();
        reader.consume(end + 1);
        match serde_json::from_slice::<Command>(&line) {
            Ok(Command::Cancel) => {
                reason.store(1, Ordering::Release);
                cancelled.store(true, Ordering::Release);
            }
            _ => {
                return fail(
                    "protocol",
                    "only cancel may follow a convert request".into(),
                );
            }
        }
    }
    {
        let cancelled = Arc::clone(&cancelled);
        let reason = Arc::clone(&reason);
        std::thread::spawn(move || {
            loop {
                match read_line(&mut reader, MAX_REQUEST_BYTES) {
                    Ok(Some(line)) => {
                        let value = if matches!(
                            serde_json::from_slice::<Command>(&line),
                            Ok(Command::Cancel)
                        ) {
                            1
                        } else {
                            2
                        };
                        reason.store(value, Ordering::Release);
                        cancelled.store(true, Ordering::Release);
                    }
                    Ok(None) => return,
                    Err(_) => {
                        reason.store(2, Ordering::Release);
                        cancelled.store(true, Ordering::Release);
                        return;
                    }
                }
            }
        });
    }
    let (finished, completion) = std::sync::mpsc::channel::<()>();
    {
        let cancelled = Arc::clone(&cancelled);
        let reason = Arc::clone(&reason);
        let timeout = Duration::from_secs(request.timeout_seconds);
        std::thread::spawn(move || {
            if matches!(
                completion.recv_timeout(timeout),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ) {
                reason.store(3, Ordering::Release);
                cancelled.store(true, Ordering::Release);
                if completion.recv_timeout(Duration::from_secs(5)).is_err() {
                    // A wedged native call cannot keep this standalone worker alive forever.
                    // The private staging directory intentionally survives a forced exit.
                    std::process::exit(124);
                }
            }
        });
    }
    let result =
        flac_alac_converter::engine::convert(&request, &cancelled, &mut |event| emit(&event));
    let _ = finished.send(());
    match result {
        Ok(report) => {
            if emit(&Event::Completed {
                version: PROTOCOL_VERSION,
                report,
            })
            .is_ok()
            {
                0
            } else {
                1
            }
        }
        Err(error) => {
            let current_reason = reason.load(Ordering::Acquire);
            if error.cancelled && current_reason == 1 {
                let _ = emit(&Event::Cancelled {
                    version: PROTOCOL_VERSION,
                    partial_output: error.partial_output,
                    staging_directory: error.staging_directory,
                });
                130
            } else {
                let code = match current_reason {
                    2 => "protocol",
                    3 => "timeout",
                    _ => "conversion",
                };
                let _ = emit(&Event::Failed {
                    version: PROTOCOL_VERSION,
                    code: code.into(),
                    message: error.message,
                    partial_output: error.partial_output,
                    staging_directory: error.staging_directory,
                });
                1
            }
        }
    }
}

fn main() {
    std::process::exit(run());
}
