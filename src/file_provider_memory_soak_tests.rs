#![cfg(target_os = "macos")]

use std::fs;
use std::mem::MaybeUninit;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use objc2::rc::autoreleasepool;
use serde_json::{Value, json};

use crate::cloud::{CloudInbox, CloudRequest};

const SOAK_CHILD_ENV: &str = "TVD_FILE_PROVIDER_MEMORY_SOAK_CHILD";
const SOAK_OPERATIONS_ENV: &str = "TVD_FILE_PROVIDER_MEMORY_SOAK_OPERATIONS";
const DEFAULT_OPERATIONS: usize = 10_000;
const MAX_OPERATIONS: usize = 10_000;
const WARMUP_OPERATIONS: usize = 256;
const SAMPLE_BATCH_SIZE: usize = 1_000;
const REPORT_PREFIX: &str = "FILE_PROVIDER_MEMORY_SOAK_JSON=";

static TEMP_DIRECTORY_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after the Unix epoch")
            .as_nanos();
        let counter = TEMP_DIRECTORY_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "telegram-video-downloader-file-provider-soak-{}-{stamp:x}-{counter:x}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("temporary soak root should be created");
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
#[ignore = "manual isolated allocator soak; run with `cargo test cloud_inbox_pending_memory_soak -- --ignored --nocapture`; set TVD_FILE_PROVIDER_MEMORY_SOAK_OPERATIONS to 1..10000"]
fn cloud_inbox_pending_memory_soak() {
    if std::env::var_os(SOAK_CHILD_ENV).as_deref() == Some(std::ffi::OsStr::new("1")) {
        run_soak_child();
        return;
    }

    // Keep the measurement process isolated from parallel unit tests and the
    // cargo test harness. The child runs just this ignored test on its own
    // long-lived Rust thread, so before/after allocator deltas are attributable
    // to real CloudInbox::pending/File Provider calls.
    let output = Command::new(std::env::current_exe().expect("test executable path should exist"))
        .args([
            "cloud_inbox_pending_memory_soak",
            "--ignored",
            "--nocapture",
        ])
        .env(SOAK_CHILD_ENV, "1")
        .output()
        .expect("isolated soak subprocess should start");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "isolated soak child failed with {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );

    let report_line = stdout
        .lines()
        .find(|line| line.starts_with(REPORT_PREFIX))
        .expect("isolated soak child should print its JSON report");
    let report: Value = serde_json::from_str(&report_line[REPORT_PREFIX.len()..])
        .expect("isolated soak report should be valid JSON");
    assert!(
        report["samples"]
            .as_array()
            .is_some_and(|samples| samples.len() >= 2),
        "soak should report allocator samples before and after operations"
    );
    println!("{report_line}");
}

fn run_soak_child() {
    let operations = configured_operations();
    let root = TempRoot::new();
    let worker = thread::Builder::new()
        .name("cloud-inbox-memory-soak".to_string())
        .spawn(move || {
            autoreleasepool(|_| {
                let inbox = CloudInbox::open(root.path()).expect("local test inbox should open");
                inbox
                    .persist(soak_request())
                    .expect("local pending request should persist");

                let before_warmup = allocator_sample(0);
                let warmup_operations = operations.min(WARMUP_OPERATIONS);
                run_pending_operations(&inbox, warmup_operations);
                let after_warmup = allocator_sample(warmup_operations);

                let started = Instant::now();
                let mut completed = warmup_operations;
                let mut samples = vec![before_warmup, after_warmup];
                while completed < operations {
                    let batch_end = completed
                        .saturating_add(SAMPLE_BATCH_SIZE)
                        .min(operations);
                    run_pending_operations(&inbox, batch_end - completed);
                    completed = batch_end;
                    samples.push(allocator_sample(completed));
                }

                let report = json!({
                    "operations": operations,
                    "warmup_operations": warmup_operations,
                    "sample_batch_size": SAMPLE_BATCH_SIZE,
                    "elapsed_millis": started.elapsed().as_millis(),
                    "measurement": "malloc_zone_statistics across all zones plus proc_pid_rusage physical footprint",
                    "samples": samples,
                });
                println!("{REPORT_PREFIX}{report}");
            });
        })
        .expect("soak worker thread should start");
    worker.join().expect("soak worker should finish");
}

fn configured_operations() -> usize {
    std::env::var(SOAK_OPERATIONS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_OPERATIONS)
        .clamp(1, MAX_OPERATIONS)
}

fn run_pending_operations(inbox: &CloudInbox, count: usize) {
    for _ in 0..count {
        let pending = inbox
            .pending()
            .expect("CloudInbox::pending should read the local request");
        assert_eq!(pending.len(), 1, "the fixture request should stay pending");
        assert_eq!(pending[0].request.seq, 1);
        std::hint::black_box(pending);
    }
}

fn soak_request() -> CloudRequest {
    CloudRequest {
        seq: 1,
        kind: "telegram".to_string(),
        payload: json!({"update_id": 1, "message": {"text": "/queue"}}),
        created_at: 1,
        extra: serde_json::Map::new(),
    }
}

fn allocator_sample(completed_operations: usize) -> Value {
    // A null zone asks libmalloc to aggregate statistics for all zones. The
    // process is isolated, so the in-use slope is the primary leak signal;
    // physical footprint is recorded as context rather than used alone.
    let mut malloc_stats = MaybeUninit::<libc::malloc_statistics_t>::zeroed();
    unsafe {
        libc::malloc_zone_statistics(std::ptr::null_mut(), malloc_stats.as_mut_ptr());
    }
    let malloc_stats = unsafe { malloc_stats.assume_init() };

    json!({
        "completed_operations": completed_operations,
        "malloc_blocks_in_use": malloc_stats.blocks_in_use,
        "malloc_bytes_in_use": malloc_stats.size_in_use,
        "malloc_bytes_allocated": malloc_stats.size_allocated,
        "physical_footprint_bytes": physical_footprint_bytes(),
    })
}

fn physical_footprint_bytes() -> Option<u64> {
    let mut usage = MaybeUninit::<libc::rusage_info_v4>::uninit();
    // `proc_pid_rusage` fills the caller-provided versioned record. The cast
    // matches Apple's `rusage_info_t *` out-parameter.
    let status = unsafe {
        libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V4,
            usage.as_mut_ptr() as *mut libc::rusage_info_t,
        )
    };
    if status != 0 {
        return None;
    }
    Some(unsafe { usage.assume_init() }.ri_phys_footprint)
}
