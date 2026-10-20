use std::fs;
use std::future::Future;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier, Mutex as StdMutex};
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cloud::TEST_CLOUD_SECRET;
use crate::file_provider::{MockQueueFileProvider, QueueFileProvider};
use anyhow::{Context, Result};
use futures_util::SinkExt;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::library::{
    Confidence, HintOrigin, IntegrityStatus, LegacyCandidate, LibraryItem, LibrarySnapshot,
    MetadataStatus, Presence, SourceHint,
};

use super::*;

// Synthetic catalog joey-private-v3: api-key-a (active API key).
const TEST_TELEGRAM_TOKEN: &str = "codex_synth_v1_api_key_a";

// Chunked migration fixtures hash and sync 8–16 MiB while the full suite shares the output slot.
// Keep completion waits bounded without treating parallel-suite contention as a deadlock.
const MIGRATION_TEST_COMPLETION_TIMEOUT: Duration = Duration::from_secs(60);

struct FakeTelegramRequest {
    method: String,
    body: serde_json::Value,
    authorization: Option<String>,
    database_epoch: Option<String>,
}

fn take_cloud_test_telegram_requests(
    requests: &mut mpsc::UnboundedReceiver<FakeTelegramRequest>,
) -> Vec<FakeTelegramRequest> {
    let mut recorded = Vec::new();
    while let Ok(request) = requests.try_recv() {
        recorded.push(request);
    }
    recorded
}

struct FakeCloudApiState {
    pending: StdMutex<Vec<CloudRequest>>,
    database_epoch: StdMutex<String>,
    rotate_epoch_to: StdMutex<Option<String>>,
    epoch_recovery_requests: StdMutex<Vec<serde_json::Value>>,
    fetch_count: AtomicUsize,
    ack_count: AtomicUsize,
    ack_request_bodies: StdMutex<Vec<serde_json::Value>>,
    replace_pending_before_ack: StdMutex<Option<CloudRequest>>,
    fail_first_ack: AtomicBool,
    durable_before_ack: AtomicBool,
    inbox_root: PathBuf,
    state_posts: StdMutex<Vec<serde_json::Value>>,
    state_post_statuses: StdMutex<Vec<u16>>,
    state_post_response_headers: StdMutex<Vec<Vec<(String, String)>>>,
}

struct PauseOnDirectoryReadProvider {
    inner: MockQueueFileProvider,
    directory: PathBuf,
    armed: AtomicBool,
    read_started: std::sync::mpsc::SyncSender<()>,
    release_read: StdMutex<std::sync::mpsc::Receiver<()>>,
}

#[derive(Default)]
struct FailOnceInboxFileProvider {
    inner: MockQueueFileProvider,
    fail_write_paths: StdMutex<Vec<PathBuf>>,
    write_attempts: StdMutex<Vec<PathBuf>>,
}

impl FailOnceInboxFileProvider {
    fn fail_next_read(&self, detail: impl Into<String>) {
        self.inner.fail_next_read(detail);
    }

    fn fail_next_write_to(&self, path: PathBuf) {
        self.fail_write_paths
            .lock()
            .expect("inbox write failure lock should remain healthy")
            .push(path);
    }

    fn write_attempts(&self) -> Vec<PathBuf> {
        self.write_attempts
            .lock()
            .expect("inbox write attempts lock should remain healthy")
            .clone()
    }
}

impl QueueFileProvider for FailOnceInboxFileProvider {
    fn coordinate_read(
        &self,
        path: &std::path::Path,
        accessor: &mut dyn FnMut(&std::path::Path) -> Result<()>,
    ) -> Result<()> {
        self.inner.coordinate_read(path, accessor)
    }

    fn coordinate_write(
        &self,
        path: &std::path::Path,
        accessor: &mut dyn FnMut(&std::path::Path) -> Result<()>,
    ) -> Result<()> {
        self.write_attempts
            .lock()
            .expect("inbox write attempts lock should remain healthy")
            .push(path.to_path_buf());
        let failure = {
            let mut paths = self
                .fail_write_paths
                .lock()
                .expect("inbox write failure lock should remain healthy");
            paths
                .iter()
                .position(|candidate| candidate == path)
                .map(|index| paths.remove(index))
        };
        if failure.is_some() {
            return Err(crate::file_provider::FileProviderAccessError::new(
                path,
                "write",
                "simulated one-time inbox storage failure",
            )
            .into());
        }
        self.inner.coordinate_write(path, accessor)
    }

    fn coordinate_move(
        &self,
        source: &std::path::Path,
        destination: &std::path::Path,
        accessor: &mut dyn FnMut(&std::path::Path, &std::path::Path) -> Result<()>,
    ) -> Result<()> {
        self.inner.coordinate_move(source, destination, accessor)
    }
}

impl PauseOnDirectoryReadProvider {
    fn new(
        directory: PathBuf,
    ) -> (
        Arc<Self>,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        (
            Arc::new(Self {
                inner: MockQueueFileProvider::default(),
                directory,
                armed: AtomicBool::new(false),
                read_started: started_tx,
                release_read: StdMutex::new(release_rx),
            }),
            started_rx,
            release_tx,
        )
    }

    fn arm(&self) {
        self.armed.store(true, AtomicOrdering::SeqCst);
    }
}

impl QueueFileProvider for PauseOnDirectoryReadProvider {
    fn coordinate_read(
        &self,
        path: &std::path::Path,
        accessor: &mut dyn FnMut(&std::path::Path) -> Result<()>,
    ) -> Result<()> {
        if path == self.directory && self.armed.swap(false, AtomicOrdering::SeqCst) {
            let _ = self.read_started.send(());
            self.release_read
                .lock()
                .expect("read release lock should remain healthy")
                .recv_timeout(Duration::from_secs(5))
                .map_err(|error| {
                    anyhow::anyhow!("timed out waiting to resume recovery: {error}")
                })?;
        }
        self.inner.coordinate_read(path, accessor)
    }

    fn coordinate_write(
        &self,
        path: &std::path::Path,
        accessor: &mut dyn FnMut(&std::path::Path) -> Result<()>,
    ) -> Result<()> {
        self.inner.coordinate_write(path, accessor)
    }

    fn coordinate_move(
        &self,
        source: &std::path::Path,
        destination: &std::path::Path,
        accessor: &mut dyn FnMut(&std::path::Path, &std::path::Path) -> Result<()>,
    ) -> Result<()> {
        self.inner.coordinate_move(source, destination, accessor)
    }
}

async fn spawn_fake_telegram_api() -> (
    TelegramClient,
    mpsc::UnboundedReceiver<FakeTelegramRequest>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake Telegram API should bind localhost");
    let address = listener
        .local_addr()
        .expect("fake Telegram API should expose its address");
    let (requests_tx, requests_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut next_message_id = 1_000_i64;
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => return Ok(()),
                accepted = listener.accept() => {
                    let (mut stream, _) = accepted.context("fake Telegram API accept failed")?;
                    let request = read_fake_telegram_request(&mut stream).await?;
                    let response = fake_telegram_response(&request, &mut next_message_id);
                    let _ = requests_tx.send(request);
                    stream.write_all(response.as_bytes()).await
                        .context("fake Telegram API response write failed")?;
                }
            }
        }
    });
    (
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            format!("http://{address}"),
        ),
        requests_rx,
        shutdown_tx,
        server,
    )
}

async fn read_fake_http_request(stream: &mut TcpStream) -> Result<Option<FakeTelegramRequest>> {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 4_096];
        let read = stream
            .read(&mut chunk)
            .await
            .context("fake HTTP request read failed")?;
        if read == 0 {
            if bytes.is_empty() {
                return Ok(None);
            }
            anyhow::bail!("fake HTTP request ended before headers");
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() > 64 * 1024 {
            anyhow::bail!("fake HTTP request headers were too large");
        }
    };
    let headers = std::str::from_utf8(&bytes[..header_end])
        .context("fake HTTP request headers were not UTF-8")?;
    let request_line = headers
        .lines()
        .next()
        .context("fake HTTP request line was missing")?;
    let request_path = request_line
        .split_whitespace()
        .nth(1)
        .context("fake HTTP request path was missing")?;
    let method = request_path
        .split('?')
        .next()
        .context("fake HTTP request target was missing")?
        .rsplit('/')
        .next()
        .filter(|method| !method.is_empty())
        .context("fake HTTP request method was missing")?
        .to_string();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then_some(value.trim())
        })
        .unwrap_or("0")
        .parse::<usize>()
        .context("fake HTTP content length was invalid")?;
    let authorization = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("authorization")
            .then_some(value.trim().to_string())
    });
    let database_epoch = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("x-cloud-database-epoch")
            .then_some(value.trim().to_string())
    });
    let body_end = header_end.saturating_add(content_length);
    while bytes.len() < body_end {
        let mut chunk = [0_u8; 4_096];
        let read = stream
            .read(&mut chunk)
            .await
            .context("fake HTTP request body read failed")?;
        if read == 0 {
            anyhow::bail!("fake HTTP request ended before body");
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    let body = if content_length == 0 {
        serde_json::json!({})
    } else {
        serde_json::from_slice(&bytes[header_end..body_end])
            .context("fake HTTP request body was not JSON")?
    };
    Ok(Some(FakeTelegramRequest {
        method,
        body,
        authorization,
        database_epoch,
    }))
}

async fn read_fake_telegram_request(stream: &mut TcpStream) -> Result<FakeTelegramRequest> {
    read_fake_http_request(stream)
        .await?
        .ok_or_else(|| anyhow::anyhow!("fake HTTP request ended before headers"))
}

async fn parse_fake_http_fragment(fragment: &[u8]) -> Result<Option<FakeTelegramRequest>> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("fake HTTP fragment listener should bind")?;
    let address = listener
        .local_addr()
        .context("fake HTTP fragment listener should expose its address")?;
    let parser = tokio::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .context("fake HTTP fragment should connect")?;
        read_fake_http_request(&mut stream).await
    });
    let mut client = TcpStream::connect(address)
        .await
        .context("fake HTTP fragment client should connect")?;
    client
        .write_all(fragment)
        .await
        .context("fake HTTP fragment should be sent")?;
    drop(client);
    parser.await.context("fake HTTP parser task should join")?
}

fn fake_telegram_response(request: &FakeTelegramRequest, next_message_id: &mut i64) -> String {
    let result = match request.method.as_str() {
        "sendMessage" => {
            let message_id = *next_message_id;
            *next_message_id += 1;
            serde_json::json!({
                "message_id": message_id,
                "chat": {"id": request.body["chat_id"].as_i64().unwrap_or_default(), "type": "private"}
            })
        }
        _ => serde_json::json!(true),
    };
    let payload = serde_json::json!({"ok": true, "result": result}).to_string();
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
        payload.len()
    )
}

async fn stop_fake_telegram_api(
    shutdown: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<Result<()>>,
) {
    shutdown
        .send(())
        .expect("fake Telegram API shutdown should be accepted");
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("fake Telegram API should stop within timeout")
        .expect("fake Telegram API task should not panic")
        .expect("fake Telegram API should not fail");
}

async fn spawn_fake_cloud_api(
    inbox_root: PathBuf,
    pending: Vec<CloudRequest>,
) -> (
    String,
    Arc<FakeCloudApiState>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
) {
    spawn_fake_cloud_api_with_epoch(
        inbox_root,
        pending,
        crate::cloud::TEST_DATABASE_EPOCH.to_string(),
    )
    .await
}

async fn spawn_fake_cloud_api_with_epoch(
    inbox_root: PathBuf,
    pending: Vec<CloudRequest>,
    database_epoch: String,
) -> (
    String,
    Arc<FakeCloudApiState>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake cloud API should bind localhost");
    let address = listener
        .local_addr()
        .expect("fake cloud API should expose its address");
    let state = Arc::new(FakeCloudApiState {
        pending: StdMutex::new(pending),
        database_epoch: StdMutex::new(database_epoch),
        rotate_epoch_to: StdMutex::new(None),
        epoch_recovery_requests: StdMutex::new(Vec::new()),
        fetch_count: AtomicUsize::new(0),
        ack_count: AtomicUsize::new(0),
        ack_request_bodies: StdMutex::new(Vec::new()),
        replace_pending_before_ack: StdMutex::new(None),
        fail_first_ack: AtomicBool::new(true),
        durable_before_ack: AtomicBool::new(false),
        inbox_root,
        state_posts: StdMutex::new(Vec::new()),
        state_post_statuses: StdMutex::new(Vec::new()),
        state_post_response_headers: StdMutex::new(Vec::new()),
    });
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let task_state = Arc::clone(&state);
    let server = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => return Ok(()),
                accepted = listener.accept() => {
                    let (mut stream, _) = accepted.context("fake cloud API accept failed")?;
                    let Some(request) = read_fake_http_request(&mut stream).await? else {
                        continue;
                    };
                    let expected_authorization = format!("Bearer {TEST_CLOUD_SECRET}");
                    if request.authorization.as_deref() != Some(expected_authorization.as_str()) {
                        stream.write_all(fake_http_response(401, &serde_json::json!({"error":"unauthorized"})).as_bytes()).await?;
                        continue;
                    }
                    let mut response_headers = Vec::new();
                    let (status, payload) = match request.method.as_str() {
                        "requests" => {
                            task_state.fetch_count.fetch_add(1, AtomicOrdering::SeqCst);
                            let pending = task_state.pending.lock().expect("fake cloud state lock should not poison").clone();
                            let database_epoch = task_state.database_epoch
                                .lock()
                                .expect("fake database epoch lock should not poison")
                                .clone();
                            (200, serde_json::json!({"database_epoch": database_epoch, "requests": pending}))
                        }
                        "ack" => {
                            task_state.ack_count.fetch_add(1, AtomicOrdering::SeqCst);
                            task_state
                                .ack_request_bodies
                                .lock()
                                .expect("fake ACK request lock should not poison")
                                .push(request.body.clone());
                            let receipts = request.body["receipts"]
                                .as_array()
                                .context("ACK body should contain receipt identities")?
                                .clone();
                            let replacement = task_state
                                .replace_pending_before_ack
                                .lock()
                                .expect("fake ACK replacement lock should not poison")
                                .take();
                            if let Some(replacement) = replacement {
                                let mut pending = task_state
                                    .pending
                                    .lock()
                                    .expect("fake cloud state lock should not poison");
                                if let Some(current) = pending
                                    .iter_mut()
                                    .find(|current| current.seq == replacement.seq)
                                {
                                    *current = replacement;
                                }
                            }
                            let current_epoch = task_state.database_epoch
                                .lock()
                                .expect("fake database epoch lock should not poison")
                                .clone();
                            if request.database_epoch.as_deref() != Some(current_epoch.as_str()) {
                                response_headers.push(("X-Cloud-Database-Epoch".to_string(), current_epoch));
                                (409, serde_json::json!({"error":"database epoch changed"}))
                            } else {
                                let pending = task_state
                                    .pending
                                    .lock()
                                    .expect("fake cloud state lock should not poison")
                                    .clone();
                                let conflicting_seq = receipts.iter().find_map(|receipt| {
                                    let seq = receipt["seq"].as_u64()?;
                                    pending
                                        .iter()
                                        .find(|current| current.seq == seq)
                                        .filter(|current| !cloud_ack_receipt_matches_request(receipt, current))
                                        .map(|_| seq)
                                });
                                if let Some(seq) = conflicting_seq {
                                    response_headers.push((
                                        "X-Cloud-Inbox-Sequence-Conflict".to_string(),
                                        seq.to_string(),
                                    ));
                                    (409, serde_json::json!({"error":"ACK receipt identity changed"}))
                                } else {
                                    let durable = receipts.iter().all(|receipt| {
                                        let Some(seq) = receipt["seq"].as_u64() else {
                                            return false;
                                        };
                                        let Some(expected) = pending.iter().find(|item| item.seq == seq) else {
                                            return true;
                                        };
                                        let path = task_state.inbox_root
                                            .join(".telegram-video-downloader-cloud")
                                            .join("epochs")
                                            .join(&current_epoch)
                                            .join(format!("request-{seq:020}.json"));
                                        std::fs::read(path)
                                            .ok()
                                            .and_then(|bytes| serde_json::from_slice::<InboxRecord>(&bytes).ok())
                                            .is_some_and(|record| {
                                                record.request == *expected
                                                    && cloud_ack_receipt_matches_request(receipt, &record.request)
                                            })
                                    });
                                    task_state.durable_before_ack.store(durable, AtomicOrdering::SeqCst);
                                    if !durable {
                                        (409, serde_json::json!({"error":"request was not durable before ACK"}))
                                    } else if task_state.fail_first_ack.swap(false, AtomicOrdering::SeqCst) {
                                        // Simulate a lost/failed ACK response while leaving the backlog
                                        // available for at-least-once redelivery on the next GET.
                                        (503, serde_json::json!({"error":"simulated lost ACK"}))
                                    } else {
                                        let mut pending = task_state.pending.lock().expect("fake cloud state lock should not poison");
                                        pending.retain(|current| !receipts.iter().any(|receipt| {
                                            cloud_ack_receipt_matches_request(receipt, current)
                                        }));
                                        (200, serde_json::json!({"ok":true}))
                                    }
                                }
                            }
                        }
                        "state" => {
                            let current_epoch = task_state.database_epoch
                                .lock()
                                .expect("fake database epoch lock should not poison")
                                .clone();
                            if request.database_epoch.as_deref() != Some(current_epoch.as_str()) {
                                response_headers.push(("X-Cloud-Database-Epoch".to_string(), current_epoch));
                                (409, serde_json::json!({"error":"database epoch changed"}))
                            } else {
                            let library = &request.body["library"];
                            let mut content = request.body.clone();
                            if let Some(content) = content.as_object_mut() {
                                content.remove("reported_at");
                                content.remove("state_version");
                            }
                            let content_hash = format!(
                                "{:x}",
                                Sha256::digest(
                                    serde_json::to_vec(&content)
                                        .context("fake state content should serialize")?
                                )
                            );
                            let library_hash = format!("{:x}", Sha256::digest(
                                serde_json::to_vec(library).context("fake state library should serialize")?
                            ));
                            let operations = &request.body["operations"];
                            let operations_bytes = serde_json::to_vec(operations)
                                .context("fake state operations should serialize")?;
                            let operation_hash = format!("{:x}", Sha256::digest(&operations_bytes));
                            let operation_count = operations.as_array().map_or(0, Vec::len);
                            let library_items = library["items"].as_array().cloned().unwrap_or_default();
                            let library_categories = library["categories"].as_array().cloned().unwrap_or_default();
                            let library_warnings = library["warnings"]
                                .as_array()
                                .map(|warnings| warnings.as_slice())
                                .unwrap_or(&[]);
                            let library_warning_sample = library_warnings.iter().take(2).cloned().collect::<Vec<_>>();
                            let library_items_hash = format!("{:x}", Sha256::digest(
                                serde_json::to_vec(&library_items).context("fake library items should serialize")?
                            ));
                            let library_category_bytes = serde_json::to_vec(&library["categories"])
                                .context("fake library categories should serialize")?
                                .len();
                            let library_warning_bytes = serde_json::to_vec(&library["warnings"])
                                .context("fake library warnings should serialize")?
                                .len();
                            let tasks = request.body["tasks"].clone();
                            let status = {
                                let mut statuses = task_state.state_post_statuses
                                    .lock()
                                    .expect("fake state status lock should not poison");
                                if statuses.is_empty() { 200 } else { statuses.remove(0) }
                            };
                            let summary = serde_json::json!({
                                "status": status,
                                "state_version": request.body["state_version"],
                                "reported_at": request.body["reported_at"],
                                "revision": request.body["revision"],
                                "content_hash": content_hash,
                                "library_hash": library_hash,
                                "library_ready": library["ready"],
                                "library_incomplete": library["incomplete"],
                                "library_warnings": library_warning_sample,
                                "library_scan_warning": library["scan_warning"],
                                "library_scanned_at": library["scanned_at"],
                                "library_item_count": library_items.len(),
                                "library_items_hash": library_items_hash,
                                "library_category_count": library_categories.len(),
                                "library_category_bytes": library_category_bytes,
                                "library_categories_are_fixture_paths": library_categories.iter().all(|category| {
                                    category.as_str().is_some_and(|path| {
                                        path.starts_with("category-")
                                            && path.len() == 512
                                            && path.as_bytes().get(13..).is_some_and(|suffix| {
                                                suffix.iter().all(|byte| *byte == b'x')
                                            })
                                    })
                                }),
                                "library_warning_count": library_warnings.len(),
                                "library_warning_bytes": library_warning_bytes,
                                "library_category_omission_summary": library_warnings.iter().any(|warning| {
                                    warning.as_str().is_some_and(|text| text.contains("category path(s)"))
                                }),
                                "library_warning_compaction_summary": library_warnings.iter().any(|warning| {
                                    warning.as_str().is_some_and(|text| text.contains("warning(s)"))
                                }),
                                "operation_count": operation_count,
                                "operation_hash": operation_hash,
                                "operation_bytes": operations_bytes.len(),
                                "tasks": tasks,
                            });
                            task_state.state_posts
                                .lock()
                                .expect("fake state post lock should not poison")
                                .push(summary);
                            let payload = if (200..300).contains(&status) {
                                serde_json::json!({"ok":true})
                            } else {
                                serde_json::json!({"error":"simulated state post failure"})
                            };
                            (status, payload)
                            }
                        }
                        "rotate-on-conflict" => {
                            task_state
                                .epoch_recovery_requests
                                .lock()
                                .expect("fake epoch recovery lock should not poison")
                                .push(request.body.clone());
                            let current_epoch = task_state
                                .database_epoch
                                .lock()
                                .expect("fake database epoch lock should not poison")
                                .clone();
                            let seq = request.body["seq"].as_u64();
                            let pending_matches = seq.is_some_and(|seq| {
                                task_state
                                    .pending
                                    .lock()
                                    .expect("fake cloud state lock should not poison")
                                    .iter()
                                    .any(|pending| {
                                        pending.seq == seq
                                            && Some(pending.kind.as_str())
                                                == request.body["kind"].as_str()
                                            && Some(pending.created_at)
                                                == request.body["created_at"].as_u64()
                                            && pending
                                                .extra
                                                .get("request_id")
                                                .and_then(serde_json::Value::as_str)
                                                == request.body["request_id"].as_str()
                                            && pending
                                                .extra
                                                .get("payload_hash")
                                                .and_then(serde_json::Value::as_str)
                                                == request.body["payload_hash"].as_str()
                                    })
                            });
                            let next_epoch = task_state
                                .rotate_epoch_to
                                .lock()
                                .expect("fake epoch rotation lock should not poison")
                                .clone();
                            if request.database_epoch.as_deref() != Some(current_epoch.as_str())
                                || !pending_matches
                            {
                                response_headers.push((
                                    "X-Cloud-Database-Epoch".to_string(),
                                    current_epoch,
                                ));
                                (409, serde_json::json!({"error":"epoch rotation CAS did not match"}))
                            } else if let Some(next_epoch) = next_epoch {
                                *task_state
                                    .database_epoch
                                    .lock()
                                    .expect("fake database epoch lock should not poison") =
                                    next_epoch.clone();
                                (200, serde_json::json!({"rotated":true,"database_epoch":next_epoch}))
                            } else {
                                (409, serde_json::json!({"error":"epoch rotation was not configured"}))
                            }
                        }
                        _ => (404, serde_json::json!({"error":"not found"})),
                    };
                    if request.method == "state" && response_headers.is_empty() {
                        let mut headers = task_state.state_post_response_headers
                            .lock()
                            .expect("fake state response header lock should not poison");
                        if !headers.is_empty() { response_headers = headers.remove(0); }
                    }
                    stream.write_all(fake_http_response_with_headers(status, &payload, &response_headers).as_bytes()).await
                        .context("fake cloud API response write failed")?;
                }
            }
        }
    });
    (format!("http://{address}/"), state, shutdown_tx, server)
}

fn fake_http_response(status: u16, payload: &serde_json::Value) -> String {
    fake_http_response_with_headers(status, payload, &[])
}

fn fake_http_response_with_headers(
    status: u16,
    payload: &serde_json::Value,
    headers: &[(String, String)],
) -> String {
    let payload = payload.to_string();
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let extra_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n{extra_headers}\r\n{payload}",
        payload.len()
    )
}

async fn stop_fake_cloud_api(
    shutdown: oneshot::Sender<()>,
    server: tokio::task::JoinHandle<Result<()>>,
) {
    shutdown
        .send(())
        .expect("fake cloud API shutdown should be accepted");
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("fake cloud API should stop within timeout")
        .expect("fake cloud API task should not panic")
        .expect("fake cloud API should not fail");
}

#[tokio::test]
async fn fake_cloud_api_continues_after_empty_cancelled_connection() {
    let root = temp_cloud_bot_dir("fake-cloud-empty-cancel");
    let (cloud_base_url, _cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(root.clone(), Vec::new()).await;
    let address = cloud_base_url
        .strip_prefix("http://")
        .and_then(|url| url.strip_suffix('/'))
        .expect("fake cloud URL should contain a local socket address")
        .parse::<std::net::SocketAddr>()
        .expect("fake cloud URL should contain a valid socket address");
    let cancelled = TcpStream::connect(address)
        .await
        .expect("empty cancellation connection should be accepted");
    drop(cancelled);

    let client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("fake cloud client should use localhost");
    let requests = client
        .fetch_requests()
        .await
        .expect("fake cloud server should keep serving after empty cancellation");
    assert!(requests.is_empty());

    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fake_http_parser_rejects_nonempty_truncated_headers_and_bodies() {
    let header_error = match parse_fake_http_fragment(b"GET /requests HTTP/1.1\r\nHost:").await {
        Ok(_) => panic!("a nonempty truncated header must remain an error"),
        Err(error) => error,
    };
    assert!(format!("{header_error:#}").contains("ended before headers"));

    let body_error = match parse_fake_http_fragment(
        b"POST /state HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\n{}",
    )
    .await
    {
        Ok(_) => panic!("a truncated request body must remain an error"),
        Err(error) => error,
    };
    assert!(format!("{body_error:#}").contains("ended before body"));
}

async fn spawn_fake_cloud_websocket_api(
    inbox_root: PathBuf,
    websocket_request: CloudRequest,
    backlog: Vec<CloudRequest>,
    fetch_status: u16,
    block_first_ack: bool,
) -> (
    String,
    Arc<StdMutex<Vec<String>>>,
    oneshot::Receiver<(bool, Vec<u64>)>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake cloud WebSocket API should bind localhost");
    let address = listener
        .local_addr()
        .expect("fake cloud WebSocket API should expose its address");
    let events = Arc::new(StdMutex::new(Vec::new()));
    let server_events = Arc::clone(&events);
    let (ack_seen_tx, ack_seen_rx) = oneshot::channel();
    let (release_ack_tx, release_ack_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (websocket_stream, _) = listener
            .accept()
            .await
            .context("fake cloud WebSocket accept failed")?;
        let mut socket = accept_async(websocket_stream)
            .await
            .context("fake cloud WebSocket handshake failed")?;
        server_events
            .lock()
            .expect("fake cloud event lock should not poison")
            .push(format!("ws_frame:{}", websocket_request.seq));
        socket
            .send(WsMessage::Text(
                serde_json::to_string(&websocket_request)
                    .context("WebSocket request should serialize")?
                    .into(),
            ))
            .await
            .context("fake cloud WebSocket request send failed")?;

        let (mut request_stream, _) = listener
            .accept()
            .await
            .context("fake cloud REST request accept failed")?;
        let request = read_fake_telegram_request(&mut request_stream).await?;
        anyhow::ensure!(
            request.method == "requests",
            "receiver should fetch REST backlog before ACK"
        );
        let expected_authorization = format!("Bearer {TEST_CLOUD_SECRET}");
        anyhow::ensure!(
            request.authorization.as_deref() == Some(expected_authorization.as_str()),
            "REST backlog fetch should use local bearer authorization"
        );
        server_events
            .lock()
            .expect("fake cloud event lock should not poison")
            .push(format!("fetch:{fetch_status}"));
        let payload = if (200..300).contains(&fetch_status) {
            serde_json::json!({
                "database_epoch": crate::cloud::TEST_DATABASE_EPOCH,
                "requests": &backlog
            })
        } else {
            serde_json::json!({"error":"simulated backlog fetch failure"})
        };
        request_stream
            .write_all(fake_http_response(fetch_status, &payload).as_bytes())
            .await
            .context("fake cloud REST response write failed")?;
        drop(request_stream);
        if !(200..300).contains(&fetch_status) {
            let _ = socket.send(WsMessage::Close(None)).await;
            return Ok(());
        }

        let mut expected_ack_seqs = backlog
            .iter()
            .map(|request| request.seq)
            .collect::<Vec<_>>();
        expected_ack_seqs.sort_unstable();
        let mut ack_seen_tx = Some(ack_seen_tx);
        if !expected_ack_seqs.is_empty() {
            let (mut ack_stream, _) = listener
                .accept()
                .await
                .context("fake cloud ACK accept failed")?;
            let ack = read_fake_telegram_request(&mut ack_stream).await?;
            anyhow::ensure!(
                ack.method == "ack",
                "reconciliation should ACK the durable REST page"
            );
            anyhow::ensure!(
                ack.authorization.as_deref() == Some(expected_authorization.as_str()),
                "ACK should use local bearer authorization"
            );
            anyhow::ensure!(
                ack.database_epoch.as_deref() == Some(crate::cloud::TEST_DATABASE_EPOCH),
                "ACK must be scoped to the bound Cloud database epoch"
            );
            let receipts = ack.body["receipts"]
                .as_array()
                .context("ACK body should contain receipt identities")?;
            let mut acknowledged_requests = backlog.clone();
            acknowledged_requests.sort_by_key(|request| request.seq);
            anyhow::ensure!(
                receipts.len() == acknowledged_requests.len()
                    && receipts
                        .iter()
                        .zip(&acknowledged_requests)
                        .all(|(receipt, request)| cloud_ack_receipt_matches_request(
                            receipt, request
                        ),),
                "ACK must cover the authoritative REST page before accepting the WebSocket frame"
            );
            let durable = acknowledged_requests.iter().all(|request| {
                let path = runtime_test_request_path(
                    &inbox_root,
                    crate::cloud::TEST_DATABASE_EPOCH,
                    request.seq,
                );
                std::fs::read(path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<InboxRecord>(&bytes).ok())
                    .is_some_and(|record| record.request == *request)
            });
            anyhow::ensure!(durable, "every request must be durable before its page ACK");
            let seqs = acknowledged_requests
                .iter()
                .map(|request| request.seq)
                .collect::<Vec<_>>();
            server_events
                .lock()
                .expect("fake cloud event lock should not poison")
                .push(format!(
                    "ack:{}",
                    seqs.iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            if let Some(ack_seen_tx) = ack_seen_tx.take() {
                let _ = ack_seen_tx.send((durable, seqs));
            }
            if block_first_ack {
                release_ack_rx
                    .await
                    .context("test should release the first REST backlog ACK")?;
            }
            ack_stream
                .write_all(fake_http_response(200, &serde_json::json!({"ok":true})).as_bytes())
                .await
                .context("fake cloud ACK response write failed")?;
        }
        socket
            .send(WsMessage::Close(None))
            .await
            .context("fake cloud WebSocket close failed")?;
        Ok(())
    });
    (
        format!("http://{address}/"),
        events,
        ack_seen_rx,
        release_ack_tx,
        server,
    )
}

async fn wait_for_state_posts(
    state: &Arc<FakeCloudApiState>,
    count: usize,
) -> Vec<serde_json::Value> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let posts = state
                .state_posts
                .lock()
                .expect("fake state post lock should not poison")
                .clone();
            if posts.len() >= count {
                return posts;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("fake cloud API should receive the expected state posts")
}

fn empty_cloud_test_library_snapshot(revision: &str) -> LibrarySnapshot {
    LibrarySnapshot {
        revision: revision.to_string(),
        scanned_at: 1_790_000_000,
        root_label: "videos".to_string(),
        items: Vec::new(),
        categories: Vec::new(),
        warnings: Vec::new(),
    }
}

fn temp_cloud_bot_dir(label: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be available")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "telegram-video-downloader-cloud-bot-{label}-{unique}"
    ))
}

fn open_runtime_test_inbox(root: &Path) -> Result<CloudInbox> {
    CloudInbox::open_for_epoch(root, crate::cloud::TEST_DATABASE_EPOCH)
}

fn open_runtime_test_inbox_with_file_provider(
    root: &Path,
    file_provider: Arc<dyn QueueFileProvider>,
) -> Result<CloudInbox> {
    CloudInbox::open_for_epoch_with_file_provider(
        root,
        crate::cloud::TEST_DATABASE_EPOCH,
        file_provider,
    )
}

fn runtime_test_inbox_directory(root: &Path, epoch: &str) -> PathBuf {
    root.join(".telegram-video-downloader-cloud")
        .join("epochs")
        .join(epoch)
}

fn runtime_test_request_path(root: &Path, epoch: &str, seq: u64) -> PathBuf {
    runtime_test_inbox_directory(root, epoch).join(format!("request-{seq:020}.json"))
}

fn persisted_cloud_preview_count(video_root: &Path) -> usize {
    fs::read_dir(video_root.join(".telegram-video-downloader-library"))
        .expect("private library directory should list")
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("preview-") && name.ends_with(".json"))
        })
        .count()
}

fn make_runtime(
    telegram: TelegramClient,
    config: AppConfig,
    queue: Arc<QueueManager>,
    inbox: Arc<CloudInbox>,
) -> CloudBotRuntime {
    CloudBotRuntime {
        bot: BotContext {
            telegram,
            config: Arc::new(config),
            job_dispatch: JobDispatch {
                download_semaphore: Arc::new(Semaphore::new(1)),
                duplicate_scan_semaphore: Arc::new(Semaphore::new(1)),
            },
            next_job_id: Arc::new(AtomicU64::new(1)),
            queue,
            queue_start_retry_delay: QUEUE_START_FILE_PROVIDER_RETRY_DELAY,
        },
        client: CloudClient::with_test_base_url("http://127.0.0.1:1")
            .expect("test cloud client should construct"),
        inbox,
        request_notify: Arc::new(Notify::new()),
        state_notify: Arc::new(Notify::new()),
        library_snapshot: Arc::new(Mutex::new(None)),
        library_snapshots: Arc::new(Mutex::new(BTreeMap::new())),
        library_unavailable: Arc::new(Mutex::new(BTreeMap::new())),
        migrations: Arc::new(Mutex::new(BTreeMap::new())),
        migration_runners: Arc::new(Mutex::new(HashSet::new())),
        library_scan_warning: Arc::new(Mutex::new(None)),
        library_scan_publication_gate: Arc::new(Mutex::new(())),
        telegram_dispatch: Arc::new(Semaphore::new(1)),
        management_dispatch: Arc::new(Semaphore::new(1)),
        backlog_sync_gate: Arc::new(Mutex::new(())),
        reconciliation_complete: Arc::new(AtomicBool::new(true)),
        epoch_fatal: Arc::new(CloudEpochFatal::default()),
        cloud_ready_marker_emitted: Arc::new(AtomicBool::new(false)),
        test_file_provider: None,
        test_library_refresh_failure: Arc::new(StdMutex::new(None)),
        test_cloud_ready_marker_emissions: Arc::new(AtomicUsize::new(0)),
    }
}

#[test]
fn web_app_button_uses_web_app_field_without_callback_data() {
    let value = serde_json::to_value(InlineKeyboardButton {
        text: "Open File Manager".to_string(),
        callback_data: String::new(),
        web_app: Some(WebAppInfo {
            url: "https://worker.example.test/file/".to_string(),
        }),
    })
    .expect("button should serialize");
    assert!(value.get("callback_data").is_none());
    assert_eq!(
        value["web_app"]["url"].as_str(),
        Some("https://worker.example.test/file/")
    );
}

#[tokio::test]
async fn settings_command_opens_the_separate_private_app_with_mock_telegram() {
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.cloud = Some(CloudConfig {
        worker_url: "https://worker.example.test/bot".to_string(),
        shared_secret: None,
        fallback_poll_seconds: 30,
    });
    handle_settings_command(&telegram, &config, 123_456_789, true)
        .await
        .expect("settings command should send the app button");
    let request = requests
        .recv()
        .await
        .expect("mock Telegram should receive the button");
    assert_eq!(request.method, "sendMessage");
    let button = &request.body["reply_markup"]["inline_keyboard"][0][0];
    assert_eq!(
        button["web_app"]["url"],
        "https://worker.example.test/bot/settings/"
    );
    assert!(button.get("callback_data").is_none());
    assert!(requests.try_recv().is_err());

    handle_settings_command(&telegram, &config, -123_456_789, false)
        .await
        .expect("group command should explain the private-chat entry");
    let group_reply = requests
        .recv()
        .await
        .expect("mock Telegram should receive the group reply");
    assert!(group_reply.body.get("reply_markup").is_none());
    assert!(
        group_reply.body["text"]
            .as_str()
            .unwrap()
            .contains("private chat")
    );
    stop_fake_telegram_api(shutdown, server).await;
}

#[test]
fn settings_command_recognizes_bot_mentions_without_matching_longer_commands() {
    assert!(is_settings_command("/settings"));
    assert!(is_settings_command("/SETTINGS@ExampleBot"));
    assert!(!is_settings_command("/settings_extra"));
    assert!(!is_settings_command("settings"));
}

async fn run_library_management_test_request(
    runtime: &CloudBotRuntime,
    seq: u64,
    kind: &str,
    payload: serde_json::Value,
) -> InboxRecord {
    runtime
        .inbox
        .persist(CloudRequest {
            seq,
            kind: kind.to_string(),
            payload,
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("management ingress should persist before dispatch");
    runtime.request_notify.notify_one();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let record = runtime
                .inbox
                .record(seq)
                .expect("management receipt should load")
                .expect("management receipt should remain present");
            if matches!(
                record.status,
                InboxStatus::Completed | InboxStatus::Failed | InboxStatus::AwaitingConfirmation
            ) {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("management operation should reach a durable result")
}

#[tokio::test]
async fn media_library_settings_and_scoped_file_management_mock_e2e() {
    let root = temp_cloud_bot_dir("media-libraries-e2e");
    let archive = root.join("archive");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test roots should create");
    fs::create_dir_all(archive.join("Documentaries")).expect("archive should create");
    fs::write(
        config.downloads.video_dir.join("clip.mp4"),
        b"source library media",
    )
    .expect("source media should write");
    fs::write(
        config.downloads.video_dir.join("clip.nfo"),
        b"<movie><title>Source clip</title></movie>",
    )
    .expect("source metadata should write");
    fs::write(archive.join("clip.mp4"), b"different archive media")
        .expect("archive media should write");
    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    queue
        .create(TaskRecord::new(
            "original-target".to_string(),
            1,
            1,
            123_456_789,
            Some(123_456_789),
            0,
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=abcdefghijk".to_string(),
            },
        ))
        .expect("initial task should persist");
    let original_id = queue
        .default_video_destination()
        .expect("default should resolve")
        .library_id;
    let inbox =
        Arc::new(open_runtime_test_inbox(&config.downloads.video_dir).expect("inbox should open"));
    let runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));

    let registered = run_library_management_test_request(
        &runtime,
        701,
        "library_register",
        serde_json::json!({
            "name": "Archive", "path": archive, "default_category": "Documentaries",
            "set_default": false, "create_directory": false,
        }),
    )
    .await;
    assert_eq!(
        registered.status,
        InboxStatus::Completed,
        "{:?}",
        registered.error
    );
    let archive_id = queue
        .registry_snapshot()
        .unwrap()
        .libraries
        .into_iter()
        .find(|library| library.name == "Archive")
        .expect("registered library should be listed")
        .id;
    let changed = run_library_management_test_request(
        &runtime,
        702,
        "library_set_default",
        serde_json::json!({
            "library_id": archive_id,
        }),
    )
    .await;
    assert_eq!(
        changed.status,
        InboxStatus::Completed,
        "{:?}",
        changed.error
    );
    assert_eq!(
        queue
            .task_config(&config, "original-target")
            .unwrap()
            .unwrap()
            .downloads
            .video_dir,
        config.downloads.video_dir
    );
    assert_eq!(
        queue
            .default_video_config(&config)
            .unwrap()
            .downloads
            .video_dir,
        archive.join("Documentaries"),
        "the task config keeps the registered logical root path; canonical identity is checked separately"
    );

    queue
        .create(TaskRecord::new(
            "new-default-target".to_string(),
            2,
            2,
            123_456_789,
            Some(123_456_789),
            0,
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=lmnopqrstuv".to_string(),
            },
        ))
        .expect("new task should persist with new default");
    assert_eq!(
        queue
            .task_config(&config, "new-default-target")
            .unwrap()
            .unwrap()
            .downloads
            .video_dir,
        archive.join("Documentaries")
    );
    let scanned = run_library_management_test_request(
        &runtime,
        703,
        "library_scan",
        serde_json::json!({
            "library_id": original_id,
        }),
    )
    .await;
    assert_eq!(
        scanned.status,
        InboxStatus::Completed,
        "{:?}",
        scanned.error
    );
    let item_id = scanned.result.unwrap()["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["relative_path"] == "clip.mp4")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let preview = run_library_management_test_request(
        &runtime,
        704,
        "file_preview",
        serde_json::json!({
            "library_id": original_id, "item_ids": [item_id],
            "target_relative_dir": "organized", "conflict": "skip",
        }),
    )
    .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let preview = preview.result.unwrap();
    assert_eq!(preview["library_id"], original_id);
    let wrong = run_library_management_test_request(&runtime, 705, "file_confirm", serde_json::json!({
        "library_id": archive_id, "preview_id": preview["preview_id"], "revision": preview["revision"],
    })).await;
    assert_eq!(wrong.status, InboxStatus::Failed);
    assert!(config.downloads.video_dir.join("clip.mp4").exists());
    let confirmed = run_library_management_test_request(&runtime, 706, "file_confirm", serde_json::json!({
        "library_id": original_id, "preview_id": preview["preview_id"], "revision": preview["revision"],
    })).await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    assert_eq!(
        fs::read(config.downloads.video_dir.join("organized/clip.mp4")).unwrap(),
        b"source library media"
    );
    assert!(
        config
            .downloads
            .video_dir
            .join("organized/clip.nfo")
            .exists()
    );
    assert_eq!(
        fs::read(archive.join("clip.mp4")).unwrap(),
        b"different archive media"
    );
    let removed = run_library_management_test_request(
        &runtime,
        707,
        "library_remove",
        serde_json::json!({
            "library_id": archive_id,
        }),
    )
    .await;
    assert_eq!(
        removed.status,
        InboxStatus::Failed,
        "dependent tasks should keep their library registered"
    );
    assert!(archive.join("clip.mp4").exists());
    let recorded = take_cloud_test_telegram_requests(&mut requests);
    assert_eq!(
        recorded
            .iter()
            .filter(|request| request.method == "sendMessage")
            .count(),
        1
    );
    assert!(
        recorded
            .iter()
            .any(|request| request.method == "editMessageText")
    );

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let reopened =
        QueueManager::open(&config).expect("registry and both queue roots should reopen");
    assert_eq!(
        reopened
            .task_config(&config, "original-target")
            .unwrap()
            .unwrap()
            .downloads
            .video_dir,
        config.downloads.video_dir
    );
    assert_eq!(
        reopened
            .task_config(&config, "new-default-target")
            .unwrap()
            .unwrap()
            .downloads
            .video_dir,
        archive.join("Documentaries")
    );
    drop(reopened);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
fn install_mock_media_library_video_tools(root: &Path, config: &mut AppConfig) {
    use std::os::unix::fs::PermissionsExt;
    let scripts = [
        (
            "yt-dlp",
            r#"#!/bin/sh
case " $* " in
  *" --dump-json "*)
    printf '%s\n' '{"id":"abcdefghijk","title":"Library fixture","width":1280,"height":720,"vcodec":"avc1","acodec":"aac","format_id":"mock","filesize":64}'
    ;;
  *)
    printf 'synthetic media fixture\n' > 'Library fixture [abcdefghijk].mkv'
    printf '%s/Library fixture [abcdefghijk].mkv\n' "$(pwd -P)"
    ;;
esac
"#,
        ),
        (
            "ffprobe",
            r#"#!/bin/sh
printf '%s\n' '{"streams":[{"codec_type":"video","codec_name":"h264","width":1280,"height":720},{"codec_type":"audio","codec_name":"aac"}],"format":{"duration":"10","size":"24"}}'
"#,
        ),
        (
            "ffmpeg",
            r#"#!/bin/sh
for arg do
  if [ "$arg" = "-progress" ]; then
    printf '%s\n' 'frame=1' 'out_time_us=1000000' 'progress=end'
    exit 0
  fi
done
exit 0
"#,
        ),
    ];
    for (name, script) in scripts {
        let path = root.join(name);
        fs::write(&path, script).expect("mock tool should write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .expect("mock tool should be executable");
    }
    config.tools.yt_dlp = root.join("yt-dlp");
    config.tools.ffmpeg = root.join("ffmpeg");
}

#[tokio::test]
async fn media_library_migration_bundles_and_unfinished_choice_mock_telegram_e2e() {
    for include_tasks in [false, true] {
        let root = temp_cloud_bot_dir(if include_tasks {
            "migration-with-task"
        } else {
            "migration-without-task"
        });
        let archive = root.join("archive");
        let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = root.join("videos");
        config.downloads.pdf_dir = root.join("pdfs");
        config.ensure_runtime_dirs().unwrap();
        fs::create_dir_all(&archive).unwrap();
        for (name, content) in [
            ("Season/01 clip.mp4", b"complete media one".as_slice()),
            (
                "Season/01 clip.nfo",
                b"<movie><title>First clip</title></movie>".as_slice(),
            ),
            ("Season/01 clip.ass", b"retained danmaku".as_slice()),
            ("Season/02 clip.mp4", b"complete media two".as_slice()),
            (
                "Season/02 clip.nfo",
                b"<movie><title>Second clip</title></movie>".as_slice(),
            ),
        ] {
            let path = config.downloads.video_dir.join(name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, content).unwrap();
        }
        let queue = Arc::new(QueueManager::open(&config).unwrap());
        let source = queue.default_video_destination().unwrap();
        let target = queue
            .register_video_library("Archive", &archive, None, false, false)
            .unwrap();
        let retained =
            downloader::create_test_retained_video_staging(&config.downloads.video_dir).unwrap();
        fs::write(retained.join("segment.part"), b"retained partial payload").unwrap();
        let mut failed = TaskRecord::new(
            "failed-remains-recoverable".into(),
            1,
            1,
            123_456_789,
            Some(123_456_789),
            0,
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=abcdefghijk".into(),
            },
        );
        failed.status = TaskStatus::Failed;
        failed.error = Some("synthetic interrupted transfer".into());
        failed.staging_attempts.push(retained.clone());
        queue.create(failed).unwrap();
        let inbox = Arc::new(open_runtime_test_inbox(&config.downloads.video_dir).unwrap());
        let runtime = make_runtime(
            telegram,
            config.clone(),
            Arc::clone(&queue),
            Arc::clone(&inbox),
        );
        let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
        let preview=run_library_management_test_request(&runtime,901,"library_migration_preview",serde_json::json!({
            "source_library_id":source.library_id,"target_library_id":target.id,"scope":"library",
            "conflict":"skip","include_incomplete_tasks":include_tasks,"set_target_default_on_success":true,
        })).await;
        assert_eq!(
            preview.status,
            InboxStatus::AwaitingConfirmation,
            "{:?}",
            preview.error
        );
        let preview = preview.result.unwrap();
        assert_eq!(preview["incomplete_task_count"], 1);
        assert_eq!(preview["item_count"], 2);
        if include_tasks {
            assert!(preview["file_count"].as_u64().unwrap() >= 6);
        } else {
            assert_eq!(preview["file_count"], 5);
        }
        let payload =
            serde_json::json!({"preview_id":preview["id"],"revision":preview["revision"]});
        let confirm = run_library_management_test_request(
            &runtime,
            902,
            "library_migration_confirm",
            payload.clone(),
        )
        .await;
        assert_eq!(
            confirm.status,
            InboxStatus::Completed,
            "{:?}",
            confirm.error
        );
        let id = preview["id"].as_str().unwrap().to_string();
        let status = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let status = runtime.migrations.lock().await.get(&id).cloned();
                if let Some(status) = status
                    && !runtime.migration_runners.lock().await.contains(&id)
                {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("migration should reach a recoverable terminal checkpoint");
        assert_eq!(
            status.status,
            crate::library_migration::MigrationRunStatus::Completed,
            "{:?}",
            status
        );
        assert_eq!(status.success_count, 2);
        assert_eq!(status.failed_count, 0);
        assert_eq!(
            fs::read(archive.join("Season/01 clip.mp4")).unwrap(),
            b"complete media one"
        );
        assert_eq!(
            fs::read(archive.join("Season/01 clip.ass")).unwrap(),
            b"retained danmaku"
        );
        assert!(archive.join("Season/01 clip.nfo").exists());
        assert!(archive.join("Season/02 clip.nfo").exists());
        assert!(
            !config
                .downloads
                .video_dir
                .join("Season/01 clip.mp4")
                .exists()
        );
        let task = queue.get("failed-remains-recoverable").unwrap().unwrap();
        assert_eq!(
            task.status,
            TaskStatus::Failed,
            "a failed task must require explicit retry after relocation"
        );
        assert!(task.destination_transfer.is_none());
        let actual_attempt = task
            .staging_attempts
            .first()
            .expect("retained attempt remains associated");
        assert_eq!(
            fs::read(actual_attempt.join("segment.part")).unwrap(),
            b"retained partial payload"
        );
        if include_tasks {
            assert!(actual_attempt.starts_with(&target.root));
            assert!(!retained.join("segment.part").exists());
        } else {
            assert_eq!(actual_attempt, &retained);
        }
        assert_eq!(
            task.destination.unwrap().library_id,
            if include_tasks {
                target.id.clone()
            } else {
                source.library_id.clone()
            }
        );
        assert_eq!(
            queue.default_video_destination().unwrap().library_id,
            target.id
        );
        let sent = take_cloud_test_telegram_requests(&mut requests);
        assert_eq!(
            sent.iter()
                .filter(|request| request.method == "sendMessage")
                .count(),
            1,
            "one migration progress message"
        );
        assert!(
            sent.iter()
                .any(|request| request.method == "editMessageText")
        );
        let replay = runtime.inbox.record(902).unwrap().unwrap();
        dispatch_cloud_record(runtime.clone(), replay).await;
        assert!(
            requests.try_recv().is_err(),
            "a repeated confirmation must not send another message"
        );
        dispatcher.abort();
        let _ = dispatcher.await;
        runtime.signal_epoch_fatal();
        drop(runtime);
        drop(inbox);
        drop(queue);
        let reopened = QueueManager::open(&config).unwrap();
        assert_eq!(
            reopened
                .get("failed-remains-recoverable")
                .unwrap()
                .unwrap()
                .status,
            TaskStatus::Failed
        );
        assert_eq!(
            reopened.registry_snapshot().unwrap().libraries.len(),
            2,
            "source registration is retained"
        );
        drop(reopened);
        stop_fake_telegram_api(shutdown, server).await;
        let _ = fs::remove_dir_all(root);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_library_download_survives_default_change_mock_telegram_e2e() {
    let root = temp_cloud_bot_dir("explicit-library-download");
    let archive = root.join("archive");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test roots should create");
    fs::create_dir_all(archive.join("Documentaries")).expect("archive should create");
    install_mock_media_library_video_tools(&root, &mut config);
    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let original_id = queue.default_video_destination().unwrap().library_id;
    let library = queue
        .register_video_library(
            "Archive",
            &archive,
            Some(Path::new("Documentaries")),
            false,
            false,
        )
        .expect("archive should register");
    let inbox =
        Arc::new(open_runtime_test_inbox(&config.downloads.video_dir).expect("inbox should open"));
    let mut runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.bot.job_dispatch.download_semaphore = Arc::new(Semaphore::new(0));
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let payload = serde_json::json!({
        "url": "https://www.youtube.com/watch?v=abcdefghijk",
        "library_id": library.id, "category": "Documentaries",
    });
    let submitted =
        run_library_management_test_request(&runtime, 801, "download_submit", payload.clone())
            .await;
    assert_eq!(
        submitted.status,
        InboxStatus::Completed,
        "{:?}",
        submitted.error
    );
    let task_id = submitted.result.unwrap()["task_ids"][0]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if queue.get(&task_id).unwrap().unwrap().status == TaskStatus::Queued {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("task should queue without starting the mocked download");
    let changed = run_library_management_test_request(
        &runtime,
        802,
        "library_set_default",
        serde_json::json!({
            "library_id": original_id,
        }),
    )
    .await;
    assert_eq!(changed.status, InboxStatus::Completed);
    runtime.bot.job_dispatch.download_semaphore.add_permits(1);
    let completed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let task = queue.get(&task_id).unwrap().unwrap();
            if matches!(
                task.status,
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Interrupted
            ) {
                break task;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("mock download should finish");
    assert_eq!(
        completed.status,
        TaskStatus::Completed,
        "{:?}",
        completed.error
    );
    let destination = archive.join("Documentaries/Library fixture [abcdefghijk].mkv");
    assert_eq!(
        fs::read(&destination).unwrap(),
        b"synthetic media fixture\n"
    );
    assert!(destination.with_extension("nfo").exists());
    assert!(archive.join(".telegram-video-downloader-staging").exists());
    assert!(
        !archive
            .join("Documentaries/.telegram-video-downloader-staging")
            .exists()
    );
    assert!(
        !config
            .downloads
            .video_dir
            .join("Library fixture [abcdefghijk].mkv")
            .exists()
    );

    let mut first_requests = take_cloud_test_telegram_requests(&mut requests);
    tokio::time::timeout(Duration::from_secs(30), async {
        while !first_requests.iter().any(|request| {
            request.body["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("Finished job #"))
        }) {
            first_requests.push(
                requests
                    .recv()
                    .await
                    .expect("completion reply should arrive"),
            );
        }
    })
    .await
    .expect("completion Telegram edit should finish before checking replay");
    let replay = dispatch_cloud_management(
        &runtime,
        &CloudRequest {
            seq: 801,
            kind: "download_submit".to_string(),
            payload,
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        },
    )
    .await
    .expect("replayed submission should be idempotent");
    assert_eq!(replay.status, InboxStatus::Completed);
    tokio::task::yield_now().await;
    assert!(take_cloud_test_telegram_requests(&mut requests).is_empty());
    assert!(
        first_requests
            .iter()
            .any(|request| request.method == "sendMessage")
    );
    assert!(
        first_requests
            .iter()
            .any(|request| request.method == "editMessageText")
    );

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn queued_download_relocates_and_resumes_without_cancel_reply_mock_e2e() {
    let root = temp_cloud_bot_dir("queued-task-library-migration");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.ensure_runtime_dirs().unwrap();
    let archive = root.join("archive");
    fs::create_dir_all(&archive).unwrap();
    install_mock_media_library_video_tools(&root, &mut config);
    let queue = Arc::new(QueueManager::open(&config).unwrap());
    let source = queue.default_video_destination().unwrap();
    let target = queue
        .register_video_library("Archive", &archive, None, false, false)
        .unwrap();
    let inbox = Arc::new(open_runtime_test_inbox(&config.downloads.video_dir).unwrap());
    let mut runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.bot.job_dispatch.download_semaphore = Arc::new(Semaphore::new(0));
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let submitted=run_library_management_test_request(&runtime,951,"download_submit",serde_json::json!({
        "url":"https://www.youtube.com/watch?v=abcdefghijk","library_id":source.library_id,"category":"",
    })).await;
    assert_eq!(
        submitted.status,
        InboxStatus::Completed,
        "{:?}",
        submitted.error
    );
    let task_id = submitted.result.unwrap()["task_ids"][0]
        .as_str()
        .unwrap()
        .to_string();
    let generation = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let task = queue.get(&task_id).unwrap().unwrap();
            if task.status == TaskStatus::Queued && task.status_message_id.is_some() {
                break task.generation;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let preview = run_library_management_test_request(
        &runtime,
        952,
        "library_migration_preview",
        serde_json::json!({
            "source_library_id":source.library_id,"target_library_id":target.id,"scope":"library",
            "conflict":"skip","include_incomplete_tasks":true,
        }),
    )
    .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let preview = preview.result.unwrap();
    assert_eq!(preview["item_count"], 0);
    assert_eq!(preview["incomplete_task_count"], 1);
    let confirmed = run_library_management_test_request(
        &runtime,
        953,
        "library_migration_confirm",
        serde_json::json!({
            "preview_id":preview["id"],"revision":preview["revision"],
        }),
    )
    .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    let migration_id = preview["id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if !runtime
                .migration_runners
                .lock()
                .await
                .contains(migration_id)
            {
                let status = runtime
                    .migrations
                    .lock()
                    .await
                    .get(migration_id)
                    .unwrap()
                    .clone();
                assert_eq!(
                    status.status,
                    crate::library_migration::MigrationRunStatus::Completed,
                    "{:?}",
                    status
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("queued task ownership should relocate without starting media transfer");
    let task = queue.get(&task_id).unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::Queued);
    assert!(task.generation > generation);
    assert_eq!(task.destination.unwrap().library_id, target.id);
    assert!(task.destination_transfer.is_none());
    let mut recorded = take_cloud_test_telegram_requests(&mut requests);
    assert!(
        !recorded.iter().any(|request| request.body["text"]
            .as_str()
            .is_some_and(|text| text.starts_with("Canceled job"))),
        "migration pause must not report user cancellation"
    );
    runtime.bot.job_dispatch.download_semaphore.add_permits(1);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            recorded.extend(take_cloud_test_telegram_requests(&mut requests));
            if queue.get(&task_id).unwrap().unwrap().status == TaskStatus::Completed
                && recorded.iter().any(|request| {
                    request.body["text"]
                        .as_str()
                        .is_some_and(|text| text.starts_with("Finished job #"))
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("relocated task should download to its new frozen root");
    assert_eq!(
        fs::read(archive.join("Library fixture [abcdefghijk].mkv")).unwrap(),
        b"synthetic media fixture\n"
    );
    assert!(
        !config
            .downloads
            .video_dir
            .join("Library fixture [abcdefghijk].mkv")
            .exists()
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|request| request.method == "sendMessage"
                && request.body["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("Library migration")))
            .count(),
        1
    );
    dispatcher.abort();
    let _ = dispatcher.await;
    runtime.signal_epoch_fatal();
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn partial_task_migration_keep_both_rewrites_published_paths_mock_e2e() {
    let root = temp_cloud_bot_dir("partial-task-migration-paths");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.ensure_runtime_dirs().unwrap();
    let archive = root.join("archive");
    fs::create_dir_all(&archive).unwrap();
    let media = config.downloads.video_dir.join("clip.mp4");
    fs::write(&media, b"published source media").unwrap();
    fs::write(
        config.downloads.video_dir.join("clip.nfo"),
        b"<movie><title>Published clip</title></movie>",
    )
    .unwrap();
    fs::write(archive.join("clip.mp4"), b"unrelated destination media").unwrap();
    fs::write(
        archive.join("clip.nfo"),
        b"<movie><title>Unrelated clip</title></movie>",
    )
    .unwrap();
    let queue = Arc::new(QueueManager::open(&config).unwrap());
    let source = queue.default_video_destination().unwrap();
    let target = queue
        .register_video_library("Archive", &archive, None, false, false)
        .unwrap();
    let mut task = TaskRecord::new(
        "partial-published-task".into(),
        1,
        1,
        123_456_789,
        Some(123_456_789),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=abcdefghijk".into(),
        },
    );
    task.status = TaskStatus::PartiallyCompleted;
    task.saved_location = Some(media.to_string_lossy().into_owned());
    task.primary_media_hashes =
        hash_primary_media(&config.downloads.video_dir, std::slice::from_ref(&media)).unwrap();
    task.media_entries_completed = 1;
    task.media_entries_total = 2;
    queue.create(task).unwrap();
    let inbox = Arc::new(open_runtime_test_inbox(&config.downloads.video_dir).unwrap());
    let runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let base = serde_json::json!({"source_library_id":source.library_id,"target_library_id":target.id,
        "scope":"library","include_incomplete_tasks":true,"conflict":"skip"});
    let rejected = run_library_management_test_request(
        &runtime,
        971,
        "library_migration_preview",
        base.clone(),
    )
    .await;
    assert_eq!(
        rejected.status,
        InboxStatus::Failed,
        "a task's skipped published output cannot be silently left behind"
    );
    let mut payload = base;
    payload["conflict"] = serde_json::json!("keep_both");
    let preview =
        run_library_management_test_request(&runtime, 972, "library_migration_preview", payload)
            .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let preview = preview.result.unwrap();
    let expected_path = archive.join(preview["items"][0]["target_path"].as_str().unwrap());
    assert_ne!(expected_path, archive.join("clip.mp4"));
    let confirmed = run_library_management_test_request(
        &runtime,
        973,
        "library_migration_confirm",
        serde_json::json!({
            "preview_id":preview["id"],"revision":preview["revision"],
        }),
    )
    .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    let id = preview["id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if !runtime.migration_runners.lock().await.contains(id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let status = runtime.migrations.lock().await.get(id).unwrap().clone();
    assert_eq!(
        status.status,
        crate::library_migration::MigrationRunStatus::Completed,
        "{:?}",
        status
    );
    let task = queue.get("partial-published-task").unwrap().unwrap();
    assert_eq!(task.status, TaskStatus::PartiallyCompleted);
    let rewritten = task.primary_media_hashes.keys().next().unwrap();
    assert_eq!(
        fs::canonicalize(rewritten).unwrap(),
        fs::canonicalize(&expected_path).unwrap()
    );
    assert_eq!(
        fs::canonicalize(task.saved_location.unwrap()).unwrap(),
        fs::canonicalize(&expected_path).unwrap()
    );
    assert_eq!(fs::read(&expected_path).unwrap(), b"published source media");
    assert_eq!(
        fs::read(archive.join("clip.mp4")).unwrap(),
        b"unrelated destination media"
    );
    assert!(expected_path.with_extension("nfo").exists());
    assert!(!media.exists());
    assert_eq!(
        take_cloud_test_telegram_requests(&mut requests)
            .iter()
            .filter(|request| request.method == "sendMessage")
            .count(),
        1
    );
    dispatcher.abort();
    let _ = dispatcher.await;
    runtime.signal_epoch_fatal();
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(root);
}

#[test]
fn cloud_batch_messages_require_one_explicit_private_chat() {
    let mut config = AppConfig::for_test();
    assert_eq!(
        configured_cloud_management_chat_id(&config),
        Some(123_456_789)
    );
    config.telegram.allowed_chat_ids.push(987_654_321);
    assert_eq!(configured_cloud_management_chat_id(&config), None);
    config.telegram.allowed_chat_ids = vec![123_456_789];
    config.telegram.allow_all_chats = true;
    assert_eq!(configured_cloud_management_chat_id(&config), None);
    config.telegram.allow_all_chats = false;
    config.telegram.allowed_chat_ids = vec![-123_456_789];
    assert_eq!(configured_cloud_management_chat_id(&config), None);
}

#[test]
fn metadata_only_preview_accepts_selected_item_ids_without_move_target() {
    let payload = serde_json::from_value::<FilePreviewPayload>(serde_json::json!({
        "item_ids": ["library-item-1"],
        "metadata_patches": [{
            "item_id": "library-item-1",
            "title": "Imported title",
            "source_ids": ["BV1234567890"],
            "collection": {
                "id": null,
                "title": "Collection",
                "kind": "user",
                "order": null,
                "part_id": null
            },
            "hint_index": 4
        }]
    }))
    .expect("metadata-only payload should parse");
    let prepared = validate_file_preview_payload(payload)
        .expect("metadata-only payload should not require a move target");
    assert_eq!(prepared.item_ids, vec!["library-item-1"]);
    assert!(prepared.target_relative_dir.is_none());
    assert_eq!(prepared.patches.len(), 1);
    assert_eq!(
        prepared.hint_indices,
        vec![("library-item-1".to_string(), Some(4))]
    );
}

#[test]
fn legacy_checkbox_subset_can_select_only_one_imported_source_id() {
    let payload = serde_json::from_value::<FilePreviewPayload>(serde_json::json!({
        "item_ids": ["library-item-1"],
        "metadata_patches": [{
            "item_id": "library-item-1",
            "source_ids": ["cid123456"],
            "hint_index": 4
        }]
    }))
    .expect("subset patch payload should parse");
    let prepared = validate_file_preview_payload(payload)
        .expect("subset patch should pass request validation");
    let hint = SourceHint {
        source_ids: vec!["BV1234567890".to_string(), "cid123456".to_string()],
        title: Some("Imported title".to_string()),
        relative_path: None,
        collection: None,
        origin: HintOrigin::LegacyText,
        requires_confirmation: true,
    };
    assert!(metadata_patch_matches_legacy_hint(
        &prepared.patches[0],
        &hint
    ));
    let changed = MetadataPatch {
        title: Some("Different title".to_string()),
        ..prepared.patches[0].clone()
    };
    assert!(!metadata_patch_matches_legacy_hint(&changed, &hint));
}

#[test]
fn parses_stable_bilibili_ids_into_per_entry_source_hints() {
    let parsed = parse_bilibili_stable_media_id("bvid:BV1xx411c7mD:cid:123456:epid:789")
        .expect("stable media identity should parse");
    assert_eq!(parsed.bvid.as_deref(), Some("BV1xx411c7mD"));
    assert_eq!(parsed.cid.as_deref(), Some("123456"));
    assert_eq!(parsed.epid.as_deref(), Some("789"));
    assert_eq!(
        parsed.source_ids,
        vec!["BV1xx411c7mD", "cid123456", "ep789"]
    );
}

#[tokio::test]
async fn cloud_queue_hints_use_only_persisted_verified_episode_receipts() {
    let root = temp_cloud_bot_dir("verified-episode-hints");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));

    let stable_ids = vec![
        "bvid:BV1xx411c7mD:cid:101:epid:1001".to_string(),
        "bvid:BV1xx411c7mD:cid:102:epid:1002".to_string(),
        "bvid:BV1xx411c7mD:cid:103:epid:1003".to_string(),
    ];
    let mut partial = TaskRecord::new(
        "partial-episode-hints".to_string(),
        1,
        1,
        123_456_789,
        Some(123_456_789),
        0,
        JobRequest::Bilibili {
            url: "https://www.bilibili.com/bangumi/play/ss123".to_string(),
            selection: None,
        },
    );
    partial.status = TaskStatus::PartiallyCompleted;
    partial.plan = Some(PlanValidationSnapshot {
        stable_media_ids: stable_ids.clone(),
        title: Some("Season title".to_string()),
        ..PlanValidationSnapshot::default()
    });
    let mut attempt = BilibiliAttemptSnapshot::new_independent(
        "Season title".to_string(),
        [
            (1, "Episode 1".to_string(), Some(stable_ids[0].clone())),
            (2, "Episode 2".to_string(), Some(stable_ids[1].clone())),
            (3, "Episode 3".to_string(), Some(stable_ids[2].clone())),
        ],
        0,
    );
    attempt.entry_failed(1);
    partial.bilibili_attempt = Some(attempt);
    queue.create(partial).expect("partial task should persist");

    let legacy_path = config.downloads.video_dir.join("legacy.mp4");
    let mut legacy = TaskRecord::new(
        "legacy-completed-hints".to_string(),
        2,
        2,
        123_456_789,
        Some(123_456_789),
        1,
        JobRequest::Bilibili {
            url: "https://www.bilibili.com/video/BV1legacyabc".to_string(),
            selection: None,
        },
    );
    legacy.status = TaskStatus::Completed;
    legacy.saved_location = Some(legacy_path.display().to_string());
    legacy.plan = Some(PlanValidationSnapshot {
        stable_media_ids: vec!["bvid:BV1legacyabc:cid:401:epid:4001".to_string()],
        title: Some("Legacy title".to_string()),
        ..PlanValidationSnapshot::default()
    });
    legacy.primary_media_hashes =
        BTreeMap::from([(legacy_path.display().to_string(), "b".repeat(64))]);
    queue.create(legacy).expect("legacy task should persist");

    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let publication_notify = queue.publication_notify();
    let progress_notification = publication_notify.notified();
    queue
        .update_bilibili_attempt_if_generation(
            "partial-episode-hints",
            0,
            3,
            &BilibiliAttemptUpdate::Started,
        )
        .expect("ordinary progress should persist");
    assert!(
        tokio::time::timeout(Duration::from_millis(25), progress_notification)
            .await
            .is_err(),
        "ordinary progress must not request a library scan"
    );

    let episode_path = config.downloads.video_dir.join("episode-2.mp4");
    let episode_hash = "a".repeat(64);
    let verified_notification = publication_notify.notified();
    queue
        .update_bilibili_attempt_if_generation(
            "partial-episode-hints",
            0,
            2,
            &BilibiliAttemptUpdate::Verified {
                media_paths: vec![episode_path.clone()],
                hashes: BTreeMap::from([(episode_path.display().to_string(), episode_hash)]),
            },
        )
        .expect("verified output receipt should persist");
    tokio::time::timeout(Duration::from_secs(1), verified_notification)
        .await
        .expect("new receipt should request a debounced library refresh");

    let hints = queue_source_hints(&runtime.bot).expect("queue hints should load");
    assert_eq!(
        hints.len(),
        2,
        "only episode 2 and the legacy task should hint"
    );
    let partial_hint = hints
        .iter()
        .find(|hint| hint.relative_path.as_deref() == Some("episode-2.mp4"))
        .expect("verified episode should bind to its published path");
    assert_eq!(partial_hint.source_ids, vec!["cid102", "ep1002"]);
    assert_eq!(
        partial_hint
            .collection
            .as_ref()
            .and_then(|value| value.order),
        Some(2)
    );
    assert_eq!(
        partial_hint.title, None,
        "group title must not replace episode NFO title"
    );
    assert!(!hints.iter().any(|hint| {
        hint.source_ids.iter().any(|source_id| {
            source_id == "cid101"
                || source_id == "ep1001"
                || source_id == "cid103"
                || source_id == "ep1003"
        })
    }));
    assert!(hints.iter().any(|hint| {
        hint.relative_path.as_deref() == Some("legacy.mp4")
            && hint.source_ids.contains(&"BV1legacyabc".to_string())
    }));

    drop(runtime);
    drop(inbox);
    drop(queue);
    std::fs::remove_dir_all(root).expect("test download roots should clean up");
}

#[test]
fn local_transient_retries_back_off_to_one_minute() {
    assert_eq!(cloud_retry_delay_seconds(1), 5);
    assert_eq!(cloud_retry_delay_seconds(2), 10);
    assert_eq!(cloud_retry_delay_seconds(3), 20);
    assert_eq!(cloud_retry_delay_seconds(4), 40);
    assert_eq!(cloud_retry_delay_seconds(5), 60);
    assert_eq!(cloud_retry_delay_seconds(u32::MAX), 60);
}

#[tokio::test]
async fn cloud_request_dispatcher_leaves_management_request_pending_while_busy() {
    let root = temp_cloud_bot_dir("dispatcher-semaphore");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let request = CloudRequest {
        seq: 81,
        kind: "unsupported-management-operation".to_string(),
        payload: serde_json::json!({}),
        created_at: now_millis(),
        extra: serde_json::Map::new(),
    };
    inbox
        .persist(request)
        .expect("management request should persist");
    let occupied = runtime
        .management_dispatch
        .clone()
        .acquire_owned()
        .await
        .expect("management semaphore should remain available");
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    runtime.request_notify.notify_one();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let waiting = inbox
        .record(81)
        .expect("request record should load")
        .expect("request should remain present");
    assert_eq!(waiting.status, InboxStatus::Pending);
    assert_eq!(waiting.attempts, 0);

    drop(occupied);
    runtime.request_notify.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let record = inbox
                .record(81)
                .expect("request record should load")
                .expect("request should remain present");
            if record.status == InboxStatus::Failed {
                assert_eq!(record.attempts, 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("released semaphore should allow the pending request to dispatch");

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn oversized_dispatch_result_stays_terminal_and_is_not_redispatched() {
    let root = temp_cloud_bot_dir("oversized-dispatch-result");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let request = cloud_telegram_update(82, 820);
    inbox
        .persist(request.clone())
        .expect("Telegram request should persist");
    let terminal = inbox
        .persist_dispatch_result(
            82,
            InboxStatus::Completed,
            Some(serde_json::json!({
                "large": "x".repeat(1024 * 1024 + 128 * 1024)
            })),
            None,
        )
        .expect("oversized generated result should become a compact receipt");
    assert_eq!(terminal.status, InboxStatus::Failed);
    assert!(
        terminal
            .error
            .as_deref()
            .is_some_and(|error| error.contains("too large"))
    );
    assert!(
        inbox
            .pending()
            .expect("pending requests should load")
            .is_empty()
    );

    drop(inbox);
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("terminal inbox should reopen"),
    );
    assert_eq!(
        inbox
            .record(82)
            .expect("terminal receipt should load")
            .expect("terminal request should remain durable")
            .status,
        InboxStatus::Failed
    );
    assert!(
        !inbox
            .persist(request)
            .expect("duplicate sequence should deduplicate after restart")
    );
    assert!(
        inbox
            .pending()
            .expect("terminal request is not pending")
            .is_empty()
    );

    let runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    runtime.request_notify.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        requests.try_recv().is_err(),
        "a compact terminal receipt must not trigger another Telegram request"
    );

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn retryable_file_confirmation_receipt_keeps_preview_available() {
    let root = temp_cloud_bot_dir("retryable-file-confirmation");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    for offset in 0..128_u64 {
        let seq = 1_000 + offset;
        inbox
            .persist(CloudRequest {
                seq,
                kind: "legacy_import".to_string(),
                payload: serde_json::json!({"text":"historical"}),
                created_at: 100 + seq,
                extra: serde_json::Map::new(),
            })
            .expect("historical management request should persist");
        inbox
            .update(seq, |record| {
                record.status = InboxStatus::Completed;
                record.result = Some(serde_json::json!({
                    "receipt": "x".repeat(8 * 1024),
                }));
            })
            .expect("historical terminal receipt should persist");
    }
    let preview = MovePreview {
        id: "retryable-preview".to_string(),
        revision: "revision-retryable".to_string(),
        target_relative_dir: Some("organized".to_string()),
        rename: false,
        conflict: ConflictPolicy::Skip,
        items: Vec::new(),
        metadata_patches: Vec::new(),
    };
    inbox
        .persist(CloudRequest {
            seq: 83,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({"item_ids": ["item-a"]}),
            created_at: 100,
            extra: serde_json::Map::new(),
        })
        .expect("preview request should persist");
    inbox
        .update(83, |record| {
            record.status = InboxStatus::AwaitingConfirmation;
            record.result = Some(serde_json::json!({
                "preview_id": "retryable-preview",
                "revision": "revision-retryable",
                "preview": preview,
            }));
        })
        .expect("preview should await confirmation");
    let confirmation_created_at = now_millis();
    inbox
        .persist(CloudRequest {
            seq: 84,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": "retryable-preview",
                "revision": "revision-retryable"
            }),
            created_at: confirmation_created_at,
            extra: serde_json::Map::new(),
        })
        .expect("confirmation request should persist");
    inbox
        .update(84, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("confirmation should enter processing");
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let indexed_preview =
        cloud_awaiting_confirmation_record(&runtime, "retryable-preview".to_string(), 84)
            .await
            .expect("indexed preview lookup should succeed after terminal history")
            .expect("the active preview should remain indexed");
    assert_eq!(indexed_preview.request.seq, 83);

    persist_cloud_retry_receipt(
        &runtime,
        84,
        "retryable-preview",
        "revision-retryable",
        None,
    )
    .await
    .expect("transient confirmation should persist a retry receipt");
    let retry_at = now_seconds().saturating_add(cloud_retry_delay_seconds(1));
    inbox
        .update(84, |record| {
            record.status = InboxStatus::Retryable;
            record.error = Some("temporary file-provider access failure".to_string());
            record.next_attempt_at = retry_at;
        })
        .expect("transient confirmation should remain retryable");

    let confirmation = inbox
        .record(84)
        .expect("confirmation receipt should load")
        .expect("confirmation receipt should remain present");
    assert_eq!(confirmation.status, InboxStatus::Retryable);
    assert_eq!(confirmation.next_attempt_at, retry_at);
    assert_eq!(
        confirmation
            .result
            .as_ref()
            .and_then(|value| value["status"].as_str()),
        Some("retrying")
    );
    let preview_record = inbox
        .record(83)
        .expect("preview receipt should load")
        .expect("preview receipt should remain present");
    assert_eq!(preview_record.status, InboxStatus::AwaitingConfirmation);
    assert!(
        inbox
            .pending()
            .expect("pending inbox should load")
            .iter()
            .all(|record| record.request.seq != 84),
        "retry backoff should defer the transient confirmation"
    );
    inbox
        .update(84, |record| record.next_attempt_at = 0)
        .expect("due retry should persist");
    assert!(
        inbox
            .pending()
            .expect("due retries should load")
            .iter()
            .any(|record| record.request.seq == 84),
        "a due transient confirmation should be dispatched again"
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn preexpiry_confirmation_survives_scan_and_restart_after_preview_expiry() {
    const PREVIEW_RETENTION_SECONDS: u64 = 7 * 24 * 60 * 60;

    let root = temp_cloud_bot_dir("preexpiry-confirmation-retention");
    let (telegram, _telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let source_path = config.downloads.video_dir.join("clip.mp4");
    fs::write(&source_path, b"fixture-video").expect("media fixture should write");

    let file_provider = Arc::new(MockQueueFileProvider::default());
    let provider: Arc<dyn QueueFileProvider> = file_provider.clone();
    let library =
        LibraryManager::open_with_provider(&config.downloads.video_dir, Arc::clone(&provider))
            .expect("library manager should open with mock File Provider access");
    let snapshot = library
        .scan(&[])
        .expect("library scan should find the fixture");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("media fixture should appear in the snapshot")
        .id
        .clone();
    let preview = library
        .preview(
            std::slice::from_ref(&item_id),
            "organized",
            false,
            ConflictPolicy::KeepBoth,
        )
        .expect("file move should produce a persistent preview");
    let preview_id = preview.id.clone();
    let preview_revision = preview.revision.clone();
    let preview_manifest = config
        .downloads
        .video_dir
        .join(".telegram-video-downloader-library")
        .join(format!("preview-{preview_id}.json"));
    let manifest_file = fs::OpenOptions::new()
        .write(true)
        .open(&preview_manifest)
        .expect("preview manifest should open for expiry fixture");
    manifest_file
        .set_modified(SystemTime::now() - Duration::from_secs(8 * 24 * 60 * 60))
        .expect("preview manifest should be old enough to expire");

    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    inbox
        .persist(CloudRequest {
            seq: 83,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({
                "item_ids": [item_id],
                "target_relative_dir": "organized"
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("preview request should persist");
    inbox
        .update(83, |record| {
            record.status = InboxStatus::AwaitingConfirmation;
            record.result = Some(serde_json::json!({
                "preview_id": preview_id,
                "revision": preview_revision,
                "preview": preview,
            }));
        })
        .expect("preview should await confirmation");

    let preview_updated_at = now_seconds().saturating_sub(PREVIEW_RETENTION_SECONDS + 1);
    let preview_expires_at_seconds = preview_updated_at.saturating_add(PREVIEW_RETENTION_SECONDS);
    let preview_expires_at_millis = preview_expires_at_seconds.saturating_mul(1_000);
    let preview_receipt_path = runtime_test_request_path(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
        83,
    );
    let mut preview_receipt: InboxRecord = serde_json::from_slice(
        &fs::read(&preview_receipt_path).expect("preview receipt should read"),
    )
    .expect("preview receipt should parse");
    preview_receipt.updated_at = preview_updated_at;
    fs::write(
        &preview_receipt_path,
        serde_json::to_vec(&preview_receipt).expect("expired preview receipt should encode"),
    )
    .expect("expired preview receipt should persist");
    drop(inbox);

    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should reopen"),
    );
    inbox
        .persist(CloudRequest {
            seq: 84,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": preview_id,
                "revision": preview_revision,
            }),
            created_at: preview_expires_at_millis.saturating_sub(1),
            extra: serde_json::Map::new(),
        })
        .expect("pre-expiry confirmation should persist");
    inbox
        .update(84, |record| record.status = InboxStatus::Processing)
        .expect("confirmation should enter processing before restart");
    drop(inbox);

    // Persist a local receive time just before expiry, then reopen so the recovered index is
    // built from the same durable fixture. Production sets this timestamp once when persisting.
    let confirmation_receipt_path = runtime_test_request_path(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
        84,
    );
    let mut confirmation_receipt: InboxRecord = serde_json::from_slice(
        &fs::read(&confirmation_receipt_path).expect("confirmation receipt should read"),
    )
    .expect("confirmation receipt should parse");
    confirmation_receipt.received_at = Some(preview_expires_at_seconds.saturating_sub(1));
    fs::write(
        &confirmation_receipt_path,
        serde_json::to_vec(&confirmation_receipt).expect("pre-expiry receipt should encode"),
    )
    .expect("pre-expiry local receive time should persist");

    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should recover"),
    );
    let mut pending_confirmation = None;
    for _ in 0..4 {
        if let Some(record) = inbox
            .pending()
            .expect("bounded startup recovery should progress")
            .into_iter()
            .find(|record| record.request.seq == 84)
        {
            pending_confirmation = Some(record);
            break;
        }
    }
    let pending_confirmation =
        pending_confirmation.expect("restarted confirmation should return to Pending");
    assert_eq!(pending_confirmation.status, InboxStatus::Pending);
    assert!(pending_confirmation.request.created_at < preview_expires_at_millis);
    assert_eq!(
        pending_confirmation.received_at,
        Some(preview_expires_at_seconds.saturating_sub(1)),
        "recovery should preserve the pre-expiry local receive timestamp"
    );

    let scan_manager = LibraryManager::open_with_provider_and_preview_cleanup_gate(
        &config.downloads.video_dir,
        Arc::clone(&provider),
        inbox.clone(),
    )
    .expect("cloud library manager should open with confirmation retention");
    scan_manager
        .scan(&[])
        .expect("an unrelated library scan should complete after restart");
    assert!(
        preview_manifest.exists(),
        "a pre-expiry confirmation must protect its expired preview through another scan"
    );

    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let runtime = make_runtime(telegram, config.clone(), queue, Arc::clone(&inbox));
    let mut runtime = runtime;
    runtime.test_file_provider = Some(Arc::clone(&provider));
    let processing_confirmation = inbox
        .update(84, |record| record.status = InboxStatus::Processing)
        .expect("dispatcher should mark the recovered confirmation processing");
    dispatch_cloud_record(runtime.clone(), processing_confirmation).await;

    assert!(
        !source_path.exists(),
        "the restored confirmation should move the source after the preview retention boundary"
    );
    assert!(
        config
            .downloads
            .video_dir
            .join("organized/clip.mp4")
            .exists(),
        "the restored confirmation should complete its planned move"
    );
    assert_eq!(
        inbox
            .record(84)
            .expect("confirmation receipt should read")
            .expect("confirmation receipt should remain")
            .status,
        InboxStatus::Completed
    );

    drop(runtime);
    drop(inbox);
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn expired_preview_manifests_follow_terminal_and_unknown_confirmation_state() {
    let root = temp_cloud_bot_dir("expired-preview-confirmation-index");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    fs::write(
        config.downloads.video_dir.join("clip.mp4"),
        b"fixture-video",
    )
    .expect("media fixture should write");

    let provider = Arc::new(MockQueueFileProvider::default());
    let provider_trait: Arc<dyn QueueFileProvider> = provider.clone();
    let library = LibraryManager::open_with_provider(
        &config.downloads.video_dir,
        Arc::clone(&provider_trait),
    )
    .expect("library manager should open with mock File Provider access");
    let snapshot = library.scan(&[]).expect("library scan should find fixture");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("media fixture should appear in the snapshot")
        .id
        .clone();
    let abandoned_preview = library
        .preview(
            std::slice::from_ref(&item_id),
            "abandoned",
            false,
            ConflictPolicy::KeepBoth,
        )
        .expect("abandoned preview should persist");
    let unresolved_preview = library
        .preview(
            std::slice::from_ref(&item_id),
            "unresolved",
            false,
            ConflictPolicy::KeepBoth,
        )
        .expect("second preview should persist");
    let library_dir = config
        .downloads
        .video_dir
        .join(".telegram-video-downloader-library");
    let abandoned_manifest = library_dir.join(format!("preview-{}.json", abandoned_preview.id));
    let unresolved_manifest = library_dir.join(format!("preview-{}.json", unresolved_preview.id));
    let expired_at = SystemTime::now() - Duration::from_secs(8 * 24 * 60 * 60);
    for path in [&abandoned_manifest, &unresolved_manifest] {
        fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("preview manifest should open for expiry fixture")
            .set_modified(expired_at)
            .expect("preview manifest should be old enough to expire");
    }

    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    for (seq, preview) in [(1, &abandoned_preview), (2, &unresolved_preview)] {
        inbox
            .persist(CloudRequest {
                seq,
                kind: "file_preview".to_string(),
                payload: serde_json::json!({
                    "item_ids": [item_id.clone()],
                    "target_relative_dir": "organized"
                }),
                created_at: now_millis(),
                extra: serde_json::Map::new(),
            })
            .expect("preview request should persist");
        inbox
            .update(seq, |record| {
                record.status = InboxStatus::AwaitingConfirmation;
                record.result = Some(serde_json::json!({
                    "preview_id": preview.id.clone(),
                    "revision": preview.revision.clone(),
                    "preview": preview,
                }));
            })
            .expect("preview request should become confirmable");
    }
    inbox
        .update(1, |record| record.status = InboxStatus::Stale)
        .expect("abandoned preview receipt should become terminal");
    inbox
        .persist(CloudRequest {
            seq: 3,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": unresolved_preview.id.clone(),
                "revision": unresolved_preview.revision.clone(),
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("active confirmation should persist");
    inbox
        .update(2, |record| record.status = InboxStatus::Stale)
        .expect("missing preview receipt should leave no expiry association");
    drop(inbox);

    let missing_preview_receipt = runtime_test_request_path(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
        2,
    );
    fs::remove_file(&missing_preview_receipt)
        .expect("expired preview receipt should be absent for restart recovery");
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should recover"),
    );

    let manager = LibraryManager::open_with_provider_and_preview_cleanup_gate(
        &config.downloads.video_dir,
        provider_trait,
        inbox.clone(),
    )
    .expect("cloud library manager should open with inbox cleanup gate");
    manager
        .scan(&[])
        .expect("scan should perform bounded expired preview cleanup");
    assert!(
        !abandoned_manifest.exists(),
        "terminal receipt with no active confirmation must allow expired manifest cleanup"
    );
    assert!(
        unresolved_manifest.exists(),
        "active confirmation with no recoverable preview expiry must retain its manifest"
    );

    drop(manager);
    drop(inbox);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn retryable_file_provider_confirmation_can_resume_the_same_preview() {
    let root = temp_cloud_bot_dir("retryable-provider-confirmation");
    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let source_path = config.downloads.video_dir.join("clip.mp4");
    fs::write(&source_path, b"fixture-video").expect("media fixture should write");

    let file_provider = Arc::new(MockQueueFileProvider::default());
    let provider: Arc<dyn QueueFileProvider> = file_provider.clone();
    let library =
        LibraryManager::open_with_provider(&config.downloads.video_dir, Arc::clone(&provider))
            .expect("library manager should open with mock File Provider access");
    let snapshot = library
        .scan(&[])
        .expect("library scan should find the fixture");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("media fixture should appear in the snapshot")
        .id
        .clone();
    let preview = library
        .preview(
            std::slice::from_ref(&item_id),
            "organized",
            false,
            ConflictPolicy::KeepBoth,
        )
        .expect("file move should produce a persistent preview");
    let preview_id = preview.id.clone();
    let preview_revision = preview.revision.clone();

    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    inbox
        .persist(CloudRequest {
            seq: 83,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({"item_ids": [item_id], "target_relative_dir": "organized"}),
            created_at: 100,
            extra: serde_json::Map::new(),
        })
        .expect("preview request should persist");
    inbox
        .update(83, |record| {
            record.status = InboxStatus::AwaitingConfirmation;
            record.result = Some(serde_json::json!({
                "preview_id": preview_id.clone(),
                "revision": preview_revision.clone(),
                "preview": preview,
            }));
        })
        .expect("preview should await confirmation");

    let runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let mut runtime = runtime;
    runtime.test_file_provider = Some(Arc::clone(&provider));
    file_provider.fail_next_move("simulated temporary File Provider access failure");

    for seq in [84, 85] {
        let confirmation_created_at = now_millis();
        inbox
            .persist(CloudRequest {
                seq,
                kind: "file_confirm".to_string(),
                payload: serde_json::json!({
                    "preview_id": preview_id.clone(),
                    "revision": preview_revision.clone(),
                }),
                created_at: confirmation_created_at,
                extra: serde_json::Map::new(),
            })
            .expect("confirmation request should persist");
        let record = inbox
            .update(seq, |record| {
                record.status = InboxStatus::Processing;
                record.attempts = 1;
            })
            .expect("confirmation should enter processing");
        assert_eq!(record.request.created_at, confirmation_created_at);
        assert!(
            record.request.created_at >= 1_700_000_000_000,
            "confirmation created_at must use epoch milliseconds"
        );
        dispatch_cloud_record(runtime.clone(), record).await;

        let recorded = take_cloud_test_telegram_requests(&mut telegram_requests);
        let final_message = recorded
            .iter()
            .rev()
            .find(|request| request.method == "editMessageText")
            .and_then(|request| request.body["text"].as_str())
            .expect("the batch summary should update its Telegram progress message");

        let preview_record = inbox
            .record(83)
            .expect("preview receipt should load")
            .expect("preview receipt should remain present");
        let confirmation = inbox
            .record(seq)
            .expect("confirmation receipt should load")
            .expect("confirmation receipt should remain present");
        if seq == 84 {
            assert_eq!(confirmation.status, InboxStatus::Failed);
            assert_eq!(preview_record.status, InboxStatus::AwaitingConfirmation);
            assert!(
                final_message
                    .contains("Retry the same confirmation after local file access recovers.")
            );
            assert!(!final_message.contains("fresh preview"));
            assert!(confirmation.error.as_deref().is_some_and(|error| {
                error.contains("preview remains available")
                    && error.contains("after local file access recovers")
            }));
            assert_eq!(
                confirmation
                    .result
                    .as_ref()
                    .and_then(|value| value["operation"]["retryable_access_error"].as_bool()),
                Some(true)
            );
            assert!(
                source_path.exists(),
                "the failed access attempt must leave the source file in place"
            );
            assert!(
                !config
                    .downloads
                    .video_dir
                    .join("organized/clip.mp4")
                    .exists()
            );
            assert_eq!(file_provider.move_paths().len(), 1);
        } else {
            assert_eq!(confirmation.status, InboxStatus::Completed);
            assert_eq!(preview_record.status, InboxStatus::Completed);
            assert!(!final_message.contains("confirmation"));
            assert!(!final_message.contains("fresh preview"));
            assert_eq!(
                confirmation
                    .result
                    .as_ref()
                    .and_then(|value| value["operation"]["retryable_access_error"].as_bool()),
                Some(false)
            );
            assert!(!source_path.exists());
            assert!(
                config
                    .downloads
                    .video_dir
                    .join("organized/clip.mp4")
                    .exists()
            );
            assert_eq!(file_provider.move_paths().len(), 2);
        }
    }

    drop(runtime);
    drop(library);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn nonretryable_skipped_confirmation_requires_fresh_preview() {
    let root = temp_cloud_bot_dir("nonretryable-skipped-confirmation");
    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    fs::write(
        config.downloads.video_dir.join("clip.mp4"),
        b"fixture-video",
    )
    .expect("source media should write");
    fs::create_dir(config.downloads.video_dir.join("Existing"))
        .expect("existing target directory should create");
    fs::write(
        config.downloads.video_dir.join("Existing/clip.mp4"),
        b"existing-target",
    )
    .expect("conflicting target should write");

    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let file_provider = Arc::new(MockQueueFileProvider::default());
    let provider: Arc<dyn QueueFileProvider> = file_provider.clone();
    let library = LibraryManager::open_with_provider_and_preview_cleanup_gate(
        &config.downloads.video_dir,
        Arc::clone(&provider),
        inbox.clone(),
    )
    .expect("cloud library manager should open with the mock provider");
    let snapshot = library.scan(&[]).expect("library scan should complete");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("source media should appear in the library")
        .id
        .clone();
    let preview = library
        .preview(
            std::slice::from_ref(&item_id),
            "Existing",
            false,
            ConflictPolicy::Skip,
        )
        .expect("skip policy should create a conflict preview");
    assert_eq!(preview.items[0].outcome, crate::library::MoveOutcome::Skip);
    let preview_id = preview.id.clone();
    let preview_revision = preview.revision.clone();
    inbox
        .persist(CloudRequest {
            seq: 83,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({"item_ids": [item_id], "target_relative_dir": "Existing"}),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("preview request should persist");
    inbox
        .update(83, |record| {
            record.status = InboxStatus::AwaitingConfirmation;
            record.result = Some(serde_json::json!({
                "preview_id": preview_id.clone(),
                "revision": preview_revision.clone(),
                "preview": preview,
            }));
        })
        .expect("preview should await confirmation");

    let mut runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.test_file_provider = Some(Arc::clone(&provider));
    let confirmation_created_at = now_millis();
    inbox
        .persist(CloudRequest {
            seq: 84,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": preview_id.clone(),
                "revision": preview_revision.clone(),
            }),
            created_at: confirmation_created_at,
            extra: serde_json::Map::new(),
        })
        .expect("confirmation request should persist");
    let confirmation = inbox
        .update(84, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("confirmation should enter processing");
    dispatch_cloud_record(runtime.clone(), confirmation).await;

    let first_confirmation = inbox
        .record(84)
        .expect("confirmation receipt should load")
        .expect("confirmation receipt should remain present");
    let stale_preview = inbox
        .record(83)
        .expect("preview receipt should load")
        .expect("preview receipt should remain present");
    assert_eq!(first_confirmation.status, InboxStatus::Failed);
    assert_eq!(stale_preview.status, InboxStatus::Stale);
    assert!(first_confirmation.error.as_deref().is_some_and(|error| {
        error.contains("cannot be retried") && error.contains("fresh preview")
    }));
    assert_eq!(
        first_confirmation
            .result
            .as_ref()
            .and_then(|value| value["operation"]["retryable_access_error"].as_bool()),
        Some(false)
    );
    let recorded = take_cloud_test_telegram_requests(&mut telegram_requests);
    let final_message = recorded
        .iter()
        .rev()
        .find(|request| request.method == "editMessageText")
        .and_then(|request| request.body["text"].as_str())
        .expect("the failed batch summary should update its Telegram progress message");
    assert!(final_message.contains("run a fresh preview before confirming again"));
    assert!(!final_message.contains("retry the same confirmation"));
    assert!(file_provider.move_paths().is_empty());

    inbox
        .persist(CloudRequest {
            seq: 85,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": preview_id,
                "revision": preview_revision,
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("duplicate confirmation request should persist");
    let stale_retry = inbox
        .update(85, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("duplicate confirmation should enter processing");
    dispatch_cloud_record(runtime.clone(), stale_retry).await;
    let rejected_confirmation = inbox
        .record(85)
        .expect("stale retry receipt should load")
        .expect("stale retry receipt should remain present");
    assert_eq!(rejected_confirmation.status, InboxStatus::Failed);
    assert!(rejected_confirmation.error.as_deref().is_some_and(|error| {
        error.contains("no longer awaiting confirmation") && error.contains("fresh preview")
    }));
    assert!(
        take_cloud_test_telegram_requests(&mut telegram_requests).is_empty(),
        "a stale confirmation must be rejected before sending another batch progress message"
    );
    assert!(file_provider.move_paths().is_empty());

    drop(runtime);
    drop(library);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn completed_file_confirmation_is_preserved_when_library_refresh_fails() {
    let root = temp_cloud_bot_dir("completed-confirmation-refresh-failure");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let source_path = config.downloads.video_dir.join("clip.mp4");
    let source_nfo = config.downloads.video_dir.join("clip.nfo");
    fs::write(&source_path, b"fixture-video").expect("media fixture should write");
    fs::write(
        &source_nfo,
        "<movie><title>Original title</title><future>keep this</future></movie>",
    )
    .expect("NFO fixture should write");

    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let file_provider = Arc::new(MockQueueFileProvider::default());
    let provider: Arc<dyn QueueFileProvider> = file_provider.clone();
    let library = LibraryManager::open_with_provider_and_preview_cleanup_gate(
        &config.downloads.video_dir,
        Arc::clone(&provider),
        inbox.clone(),
    )
    .expect("library manager should open with mock File Provider access");
    let snapshot = library
        .scan(&[])
        .expect("library scan should find the fixture");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("media fixture should appear in the snapshot")
        .id
        .clone();
    let preview = library
        .preview_with_hints_and_patches(
            std::slice::from_ref(&item_id),
            Some("organized"),
            false,
            ConflictPolicy::KeepBoth,
            &[],
            &[MetadataPatch {
                item_id: item_id.clone(),
                title: Some("Updated title".to_string()),
                source_ids: Vec::new(),
                collection: None,
            }],
        )
        .expect("move and NFO patch should produce a preview");
    let mut expected_cached_snapshot = snapshot.clone();
    expected_cached_snapshot.revision = "last-successful-snapshot".to_string();
    expected_cached_snapshot.scanned_at = 1_792_407_600;
    let preview_id = preview.id.clone();
    let preview_revision = preview.revision.clone();
    inbox
        .persist(CloudRequest {
            seq: 86,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({
                "item_ids": [item_id],
                "target_relative_dir": "organized"
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("preview request should persist");
    inbox
        .update(86, |record| {
            record.status = InboxStatus::AwaitingConfirmation;
            record.result = Some(serde_json::json!({
                "preview_id": preview_id,
                "revision": preview_revision,
                "preview": preview,
            }));
        })
        .expect("preview should await confirmation");
    inbox
        .persist(CloudRequest {
            seq: 87,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": preview_id,
                "revision": preview_revision,
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("confirmation request should persist");
    let confirmation = inbox
        .update(87, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("confirmation should enter processing");

    let mut runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.test_file_provider = Some(Arc::clone(&provider));
    *runtime.library_snapshot.lock().await = Some(expected_cached_snapshot.clone());
    *runtime
        .test_library_refresh_failure
        .lock()
        .expect("refresh-failure test hook should remain healthy") =
        Some("simulated ordinary library refresh error".to_string());

    dispatch_cloud_record(runtime.clone(), confirmation).await;

    let completed = inbox
        .record(87)
        .expect("confirmation receipt should load")
        .expect("confirmation receipt should remain present");
    assert_eq!(completed.status, InboxStatus::Completed);
    assert!(completed.error.is_none());
    let stored_result = completed
        .result
        .as_ref()
        .expect("completed confirmation should store its result");
    let operation: crate::library::BatchMoveResult =
        serde_json::from_value(stored_result["operation"].clone())
            .expect("completed batch result should be retained");
    assert_eq!(operation.status, crate::library::BatchStatus::Complete);
    assert_eq!(operation.items.len(), 1);
    assert!(operation.items[0].metadata_patched);
    assert_eq!(
        operation.items[0].outcome,
        crate::library::MoveOutcome::Move
    );
    assert!(
        stored_result["library_refresh_warning"]
            .as_str()
            .is_some_and(|warning| warning.contains("simulated ordinary library refresh error"))
    );

    let completed_preview = inbox
        .record(86)
        .expect("preview receipt should load")
        .expect("preview receipt should remain present");
    assert_eq!(completed_preview.status, InboxStatus::Completed);
    assert!(!source_path.exists(), "the source media should have moved");
    assert!(!source_nfo.exists(), "the source NFO should have moved");
    assert!(
        config
            .downloads
            .video_dir
            .join("organized/clip.mp4")
            .exists()
    );
    let target_nfo = fs::read_to_string(config.downloads.video_dir.join("organized/clip.nfo"))
        .expect("patched NFO should be at the move destination");
    assert!(target_nfo.contains("<title>Updated title</title>"));
    assert!(target_nfo.contains("<future>keep this</future>"));

    let retained_snapshot = runtime
        .library_snapshot
        .lock()
        .await
        .as_ref()
        .expect("previous library snapshot should remain cached")
        .clone();
    assert_eq!(retained_snapshot, expected_cached_snapshot);
    assert!(
        runtime
            .library_scan_warning
            .lock()
            .await
            .as_deref()
            .is_some_and(|warning| warning.contains("simulated ordinary library refresh error"))
    );

    let mut recorded = Vec::new();
    while let Ok(request) = requests.try_recv() {
        recorded.push(request);
    }
    assert_eq!(
        recorded
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        vec!["sendMessage", "editMessageText"]
    );
    let final_text = recorded[1].body["text"]
        .as_str()
        .expect("final Telegram status should contain text");
    assert!(final_text.contains("completed"));
    assert!(final_text.contains("Library refresh warning"));
    assert!(!final_text.contains("failed before it could complete"));
    assert!(!final_text.contains("the file batch completed"));

    drop(runtime);
    drop(library);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn manual_metadata_patch_without_legacy_import_can_preview_and_confirm() {
    let root = temp_cloud_bot_dir("manual-metadata-patch-without-import");
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let source_path = config.downloads.video_dir.join("clip.mp4");
    let source_nfo = config.downloads.video_dir.join("clip.nfo");
    fs::write(&source_path, b"fixture-video").expect("media fixture should write");
    let original_nfo = "<movie><title>Original title</title><future>keep this</future></movie>";
    fs::write(&source_nfo, original_nfo).expect("NFO fixture should write");

    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let library =
        LibraryManager::open(&config.downloads.video_dir).expect("library manager should open");
    let snapshot = library
        .scan(&[])
        .expect("library scan should find the fixture");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("media fixture should appear in the snapshot")
        .id
        .clone();
    drop(library);

    let runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));

    inbox
        .persist(CloudRequest {
            seq: 1,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({
                "metadata_patches": [{
                    "item_id": item_id,
                    "title": "Must require a completed import",
                    "hint_index": 0,
                }]
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("explicit-hint preview should persist");
    let explicit_hint = inbox
        .update(1, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("explicit-hint preview should enter processing");
    dispatch_cloud_record(runtime.clone(), explicit_hint).await;
    let rejected_hint = inbox
        .record(1)
        .expect("explicit-hint receipt should load")
        .expect("explicit-hint receipt should remain present");
    assert_eq!(rejected_hint.status, InboxStatus::Failed);
    assert!(rejected_hint.error.as_deref().is_some_and(|error| {
        error.contains("metadata patches must come from a completed legacy import")
    }));
    assert_eq!(
        fs::read_to_string(&source_nfo).expect("original NFO should remain readable"),
        original_nfo,
        "an unvalidated explicit hint must not change the NFO"
    );

    inbox
        .persist(CloudRequest {
            seq: 2,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({
                "metadata_patches": [{
                    "item_id": item_id,
                    "title": "Manual title",
                }]
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("manual preview should persist");
    let manual_preview = inbox
        .update(2, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("manual preview should enter processing");
    dispatch_cloud_record(runtime.clone(), manual_preview).await;
    let preview_receipt = inbox
        .record(2)
        .expect("manual preview receipt should load")
        .expect("manual preview receipt should remain present");
    assert_eq!(preview_receipt.status, InboxStatus::AwaitingConfirmation);
    assert!(preview_receipt.error.is_none());
    let preview_result = preview_receipt
        .result
        .as_ref()
        .expect("manual preview should store its result");
    let preview_id = preview_result["preview_id"]
        .as_str()
        .expect("manual preview should include an id")
        .to_string();
    let revision = preview_result["revision"]
        .as_str()
        .expect("manual preview should include a revision")
        .to_string();

    inbox
        .persist(CloudRequest {
            seq: 3,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({"preview_id": preview_id, "revision": revision}),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("manual confirmation should persist");
    let confirmation = inbox
        .update(3, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("manual confirmation should enter processing");
    dispatch_cloud_record(runtime.clone(), confirmation).await;

    let completed = inbox
        .record(3)
        .expect("manual confirmation receipt should load")
        .expect("manual confirmation receipt should remain present");
    assert_eq!(completed.status, InboxStatus::Completed);
    assert!(completed.error.is_none());
    let operation: crate::library::BatchMoveResult = serde_json::from_value(
        completed
            .result
            .as_ref()
            .expect("completed confirmation should store its result")["operation"]
            .clone(),
    )
    .expect("manual metadata operation should deserialize");
    assert_eq!(operation.status, crate::library::BatchStatus::Complete);
    assert_eq!(operation.items.len(), 1);
    assert!(operation.items[0].metadata_patched);
    assert!(operation.items[0].error.is_none());

    let completed_preview = inbox
        .record(2)
        .expect("manual preview receipt should reload")
        .expect("manual preview receipt should remain present");
    assert_eq!(completed_preview.status, InboxStatus::Completed);
    let updated_nfo = fs::read_to_string(&source_nfo).expect("patched NFO should remain readable");
    assert!(updated_nfo.contains("<title>Manual title</title>"));
    assert!(updated_nfo.contains("<future>keep this</future>"));

    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn oversized_cloud_file_preview_is_rejected_before_manifest_persistence() {
    const ITEM_COUNT: usize = 100;
    const IDS_PER_PATCH: usize = 48;

    let root = temp_cloud_bot_dir("oversized-cloud-file-preview");
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let video_root = config.downloads.video_dir.clone();

    let mut item_ids = Vec::with_capacity(ITEM_COUNT);
    let mut source_files = Vec::with_capacity(ITEM_COUNT);
    for item_index in 0..ITEM_COUNT {
        let stem = format!("c{item_index:02}");
        let media = video_root.join(format!("{stem}.mp4"));
        fs::write(&media, b"fixture-video").expect("media fixture should write");
        source_files.push(media);
    }

    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(open_runtime_test_inbox(&video_root).expect("cloud inbox should open"));
    let library = LibraryManager::open(&video_root).expect("library manager should open");
    let snapshot = library
        .scan(&[])
        .expect("initial library scan should index the sidecar groups");
    for item_index in 0..ITEM_COUNT {
        let stem = format!("c{item_index:02}.mp4");
        let item = snapshot
            .items
            .iter()
            .find(|item| item.relative_path == stem)
            .expect("media fixture should be indexed");
        item_ids.push(item.id.clone());
    }
    drop(library);

    let runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));
    let patches = item_ids
        .iter()
        .enumerate()
        .map(|(item_index, item_id)| {
            let source_ids = (0..IDS_PER_PATCH)
                .map(|source_id_index| {
                    format!("cid{:0125}", item_index * IDS_PER_PATCH + source_id_index)
                })
                .collect::<Vec<_>>();
            assert!(source_ids.iter().all(|source_id| source_id.len() == 128));
            serde_json::json!({"item_id": item_id, "source_ids": source_ids})
        })
        .collect::<Vec<_>>();
    let large_request = CloudRequest {
        seq: 1,
        kind: "file_preview".to_string(),
        payload: serde_json::json!({"metadata_patches": patches}),
        created_at: now_millis(),
        extra: serde_json::Map::new(),
    };
    assert!(
        serde_json::to_vec(&large_request)
            .expect("large preview request should serialize")
            .len()
            < 1024 * 1024,
        "fixture request must fit the existing inbox request limit"
    );
    inbox
        .persist(large_request)
        .expect("large preview request should persist");
    let large_request = inbox
        .update(1, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("large preview request should enter processing");
    dispatch_cloud_record(runtime.clone(), large_request).await;

    let rejected = inbox
        .record(1)
        .expect("large preview receipt should load")
        .expect("large preview receipt should remain present");
    assert_eq!(rejected.status, InboxStatus::Failed);
    assert!(
        rejected.error.as_deref().is_some_and(|error| {
            error.contains("too large to save and confirm")
                && error.contains("split the changes into smaller previews")
        }),
        "oversized cloud preview should fail at the receipt-budget guard, got: {:?}",
        rejected.error
    );
    assert_eq!(persisted_cloud_preview_count(&video_root), 0);
    assert!(
        (0..ITEM_COUNT)
            .all(|item_index| !video_root.join(format!("c{item_index:02}.nfo")).exists()),
        "rejecting an oversized preview must not create any NFO target"
    );
    for source in &source_files {
        assert!(
            source.is_file(),
            "preview rejection must leave {source:?} in place"
        );
    }
    assert_eq!(
        fs::read(source_files[0].as_path()).expect("primary source should remain readable"),
        b"fixture-video"
    );

    inbox
        .persist(CloudRequest {
            seq: 2,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({
                "metadata_patches": [{
                    "item_id": item_ids[0],
                    "title": "Small preview",
                }]
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("small preview request should persist");
    let small_request = inbox
        .update(2, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("small preview request should enter processing");
    dispatch_cloud_record(runtime.clone(), small_request).await;

    let preview_receipt = inbox
        .record(2)
        .expect("small preview receipt should load")
        .expect("small preview receipt should remain present");
    assert_eq!(preview_receipt.status, InboxStatus::AwaitingConfirmation);
    let result = preview_receipt
        .result
        .as_ref()
        .expect("confirmable preview should keep its receipt result");
    let preview_id = result["preview_id"]
        .as_str()
        .expect("preview receipt should include its ID")
        .to_string();
    let revision = result["revision"]
        .as_str()
        .expect("preview receipt should include its revision")
        .to_string();
    assert_eq!(persisted_cloud_preview_count(&video_root), 1);

    inbox
        .persist(CloudRequest {
            seq: 3,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({"preview_id": preview_id, "revision": revision}),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("small preview confirmation should persist");
    let confirmation = inbox
        .update(3, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("small preview confirmation should enter processing");
    dispatch_cloud_record(runtime.clone(), confirmation).await;

    let completed = inbox
        .record(3)
        .expect("confirmation receipt should load")
        .expect("confirmation receipt should remain present");
    assert_eq!(completed.status, InboxStatus::Completed);
    let operation: crate::library::BatchMoveResult = serde_json::from_value(
        completed
            .result
            .as_ref()
            .expect("confirmation should retain its operation result")["operation"]
            .clone(),
    )
    .expect("operation result should deserialize");
    assert_eq!(operation.status, crate::library::BatchStatus::Complete);
    assert_eq!(
        fs::read_to_string(video_root.join("c00.nfo"))
            .expect("confirmed metadata patch should create its NFO"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<movie>\n  <title>Small preview</title>\n</movie>\n"
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn missing_related_preview_does_not_retry_a_durable_primary_outcome() {
    const PRIMARY_SEQ: u64 = 861;
    const MISSING_PREVIEW_SEQ: u64 = 862;

    let root = temp_cloud_bot_dir("missing-related-preview-outcome");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    inbox
        .persist(CloudRequest {
            seq: PRIMARY_SEQ,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": "completed-preview",
                "revision": "revision-a"
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("primary request should persist before dispatch");
    let processing = inbox
        .update(PRIMARY_SEQ, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("primary request should be durably processing");
    assert_eq!(processing.status, InboxStatus::Processing);

    let runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));
    let expected_result = serde_json::json!({
        "preview_id": "completed-preview",
        "revision": "revision-a",
        "operation": {"status": "complete"}
    });
    let completed = tokio::time::timeout(
        Duration::from_secs(5),
        persist_cloud_dispatch_outcome(
            &runtime,
            PRIMARY_SEQ,
            CloudDispatchOutcome {
                status: InboxStatus::Completed,
                result: Some(expected_result.clone()),
                error: None,
                related_preview: Some((MISSING_PREVIEW_SEQ, InboxStatus::Completed)),
            },
        ),
    )
    .await
    .is_ok();
    let primary = inbox
        .record(PRIMARY_SEQ)
        .expect("primary receipt should remain readable")
        .expect("primary receipt should remain durable");
    let missing_preview = inbox
        .record(MISSING_PREVIEW_SEQ)
        .expect("missing preview lookup should succeed");
    let no_telegram_calls = requests.try_recv().is_err();

    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(root);

    assert!(
        completed,
        "a missing related preview should not keep retrying after the primary result is durable"
    );
    assert_eq!(primary.status, InboxStatus::Completed);
    assert_eq!(primary.result, Some(expected_result));
    assert!(primary.error.is_none());
    assert!(missing_preview.is_none());
    assert!(
        no_telegram_calls,
        "outcome persistence must not repeat Telegram side effects"
    );
}

#[test]
fn oversized_legacy_import_receipt_preserves_a_bounded_mapping_prefix() {
    const SEQ: u64 = 1;
    const RECEIPT_LIMIT: usize = 1024 * 1024 + 64 * 1024;
    let hint_count = crate::library::MAX_LEGACY_HINTS;
    assert_eq!(hint_count, 10_000);
    let root = temp_cloud_bot_dir("oversized-legacy-import-receipt");
    fs::create_dir_all(&root).expect("inbox root should create");
    let inbox = CloudInbox::open(&root).expect("cloud inbox should open");

    let text = (0..hint_count)
        .map(|index| format!("BV1{index:010}"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(text.len(), 139_999);
    let request = CloudRequest {
        seq: SEQ,
        kind: "legacy_import".to_string(),
        payload: serde_json::json!({"text": text}),
        created_at: now_millis(),
        extra: serde_json::Map::new(),
    };
    inbox
        .persist(request)
        .expect("valid maximum-size legacy input should persist");

    let hints = (0..hint_count)
        .map(|index| SourceHint {
            source_ids: vec![format!("BV1{index:010}")],
            title: Some(format!("Imported title {index}")),
            relative_path: None,
            collection: None,
            origin: HintOrigin::LegacyText,
            requires_confirmation: true,
        })
        .collect::<Vec<_>>();
    let candidates = (0..hint_count)
        .map(|index| LegacyCandidate {
            hint_index: index,
            item_id: format!("item-{index:05}"),
            title: format!("Media title {index}"),
            relative_path: format!("media/{index:05}.mp4"),
            match_kind: "source_id".to_string(),
            ambiguous: false,
            requires_confirmation: true,
        })
        .collect::<Vec<_>>();
    let result = serde_json::json!({
        "revision": "library-revision",
        "library_revision": "library-revision",
        "hints": hints,
        "warnings": [],
        "needs_confirmation": true,
        "candidates": candidates,
    });

    let receipt = inbox
        .persist_dispatch_result(SEQ, InboxStatus::Completed, Some(result), None)
        .expect("oversized legacy import should preserve a useful compact receipt");
    assert_eq!(receipt.status, InboxStatus::Completed);
    assert!(receipt.error.is_none());
    let encoded_len = serde_json::to_vec(&receipt)
        .expect("complete receipt should serialize")
        .len();
    assert!(
        encoded_len <= RECEIPT_LIMIT,
        "the full persisted receipt including its request and truncation warning must fit: {encoded_len} bytes"
    );

    let result = receipt
        .result
        .as_ref()
        .expect("completed import should retain its result");
    let retained_hints = result["hints"]
        .as_array()
        .expect("hints should remain an array");
    let retained_candidates = result["candidates"]
        .as_array()
        .expect("candidates should remain an array");
    assert!(!retained_hints.is_empty());
    assert!(retained_hints.len() < hint_count);
    assert_eq!(retained_candidates.len(), retained_hints.len());
    let warning = result["warnings"]
        .as_array()
        .and_then(|warnings| warnings.iter().find_map(serde_json::Value::as_str))
        .expect("truncation warning should remain visible");
    assert!(warning.contains("truncated"));
    assert!(warning.contains("re-import a smaller selection"));

    for (index, (hint, candidate)) in retained_hints.iter().zip(retained_candidates).enumerate() {
        assert_eq!(candidate["hint_index"].as_u64(), Some(index as u64));
        assert_eq!(
            hint["source_ids"][0].as_str(),
            Some(format!("BV1{index:010}").as_str())
        );
        assert_eq!(
            candidate["item_id"].as_str(),
            Some(format!("item-{index:05}").as_str())
        );
    }

    drop(inbox);
    let reopened = CloudInbox::open(&root).expect("cloud inbox should reopen");
    let reopened_receipt = reopened
        .record(SEQ)
        .expect("persisted import receipt should reload")
        .expect("completed import receipt should remain durable");
    assert_eq!(reopened_receipt.status, InboxStatus::Completed);
    assert_eq!(reopened_receipt.result, receipt.result);
    assert!(
        serde_json::to_vec(&reopened_receipt)
            .expect("reopened receipt should serialize")
            .len()
            <= RECEIPT_LIMIT
    );

    drop(reopened);
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn dispatched_legacy_mapping_survives_management_history_eviction_and_reopen() {
    const LEGACY_IMPORT_SEQ: u64 = 1;
    const PREVIEW_SEQ: u64 = 101;
    const SELECTED_HINT_INDEX: usize = 1_000;

    let root = temp_cloud_bot_dir("legacy-mapping-management-eviction");
    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    fs::write(
        config.downloads.video_dir.join("clip.mp4"),
        b"fixture-video",
    )
    .expect("library media fixture should write");

    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let library =
        LibraryManager::open(&config.downloads.video_dir).expect("library manager should open");
    let snapshot = library
        .scan(&[])
        .expect("library scan should find the media fixture");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("fixture media should appear in the library")
        .id
        .clone();
    drop(library);

    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let mut hints = (0..=SELECTED_HINT_INDEX)
        .map(|index| SourceHint {
            source_ids: Vec::new(),
            title: Some(format!("unselected hint {index}")),
            relative_path: None,
            collection: None,
            origin: HintOrigin::LegacyText,
            requires_confirmation: false,
        })
        .collect::<Vec<_>>();
    hints[SELECTED_HINT_INDEX] = SourceHint {
        source_ids: vec!["BV1xx411c7mD".to_string()],
        title: Some("Imported title".to_string()),
        relative_path: None,
        collection: None,
        origin: HintOrigin::LegacyText,
        requires_confirmation: true,
    };
    let candidate = LegacyCandidate {
        hint_index: SELECTED_HINT_INDEX,
        item_id: item_id.clone(),
        title: "clip".to_string(),
        relative_path: "clip.mp4".to_string(),
        match_kind: "source_id".to_string(),
        ambiguous: false,
        requires_confirmation: true,
    };
    inbox
        .persist(CloudRequest {
            seq: LEGACY_IMPORT_SEQ,
            kind: "legacy_import".to_string(),
            payload: serde_json::json!({"text":"Imported title https://www.bilibili.com/video/BV1xx411c7mD"}),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("legacy source receipt should persist");
    inbox
        .update(LEGACY_IMPORT_SEQ, |record| {
            record.status = InboxStatus::Completed;
            record.result = Some(serde_json::json!({
                "hints": hints,
                "candidates": [candidate],
            }));
        })
        .expect("legacy source receipt should complete");

    for seq in 2..=100_u64 {
        inbox
            .persist(CloudRequest {
                seq,
                kind: "library_scan".to_string(),
                payload: serde_json::json!({}),
                created_at: now_millis(),
                extra: serde_json::Map::new(),
            })
            .expect("management receipt should persist");
        inbox
            .update(seq, |record| record.status = InboxStatus::Completed)
            .expect("management receipt should complete");
    }
    let (before_preview, _) = inbox
        .state_records_bounded(0)
        .expect("management state should remain readable");
    assert_eq!(before_preview.len(), 100);
    assert!(
        before_preview
            .iter()
            .any(|record| record.request.seq == LEGACY_IMPORT_SEQ)
    );

    drop(inbox);
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should reopen"),
    );
    let mut recovered_state = None;
    for _ in 0..16 {
        if let Ok((records, _)) = inbox.state_records_bounded(0) {
            recovered_state = Some(records);
            break;
        }
    }
    let recovered_state = recovered_state.expect("bounded inbox recovery should finish");
    assert!(
        recovered_state
            .iter()
            .any(|record| record.request.seq == LEGACY_IMPORT_SEQ)
    );
    assert_eq!(
        inbox
            .completed_legacy_import_records_bounded()
            .expect("reopened import index should resolve its source")
            .len(),
        1
    );

    let preview_request = CloudRequest {
        seq: PREVIEW_SEQ,
        kind: "file_preview".to_string(),
        payload: serde_json::json!({
            "metadata_patches": [{
                "item_id": item_id,
                "title": "Imported title",
                "source_ids": ["BV1xx411c7mD"],
                "hint_index": SELECTED_HINT_INDEX,
            }]
        }),
        created_at: now_millis(),
        extra: serde_json::Map::new(),
    };
    inbox
        .persist(preview_request)
        .expect("new file preview should persist");
    let (after_preview_persist, _) = inbox
        .state_records_bounded(0)
        .expect("management history should remain readable after eviction");
    assert_eq!(after_preview_persist.len(), 100);
    assert!(
        !after_preview_persist
            .iter()
            .any(|record| record.request.seq == LEGACY_IMPORT_SEQ)
    );
    assert_eq!(
        inbox
            .completed_legacy_import_records_bounded()
            .expect("the evicted UI receipt should remain available to validation")
            .first()
            .map(|record| record.request.seq),
        Some(LEGACY_IMPORT_SEQ)
    );
    let processing_preview = inbox
        .update(PREVIEW_SEQ, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("file preview should enter processing");

    let runtime = make_runtime(telegram, config, queue, Arc::clone(&inbox));
    dispatch_cloud_record(runtime.clone(), processing_preview).await;
    let preview_receipt = inbox
        .record(PREVIEW_SEQ)
        .expect("file preview receipt should load")
        .expect("file preview receipt should remain present");
    assert_eq!(preview_receipt.status, InboxStatus::AwaitingConfirmation);
    let stored_preview = preview_receipt
        .result
        .as_ref()
        .and_then(|result| result.get("preview"))
        .expect("valid legacy mapping should produce a preview");
    assert_eq!(
        stored_preview["metadata_patches"][0]["item_id"].as_str(),
        Some(item_id.as_str())
    );
    assert!(
        telegram_requests.try_recv().is_err(),
        "local preview dispatch should not call Telegram APIs"
    );

    drop(runtime);
    drop(inbox);
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}

#[test]
fn refresh_warning_does_not_change_a_partial_batch_summary() {
    let result = crate::library::BatchMoveResult {
        id: "partial-batch".to_string(),
        revision: "revision-a".to_string(),
        status: crate::library::BatchStatus::Partial,
        retryable_access_error: false,
        items: Vec::new(),
        warnings: Vec::new(),
    };

    let message = render_cloud_batch_result_with_refresh_warning(
        &result,
        Some("Library refresh failed after file confirmation: simulated error"),
    );

    assert!(message.contains("File batch partial-batch partially completed"));
    assert!(message.contains("Library refresh warning"));
    assert!(message.contains("run a fresh preview before confirming again"));
    assert!(!message.contains("retry the same confirmation"));
    assert!(!message.contains("the file batch completed"));
}

#[test]
fn cloud_batch_result_retry_advice_matches_confirmation_availability() {
    let cases = [
        (
            crate::library::BatchStatus::Complete,
            false,
            None,
            Some("fresh preview"),
        ),
        (
            crate::library::BatchStatus::Complete,
            true,
            None,
            Some("fresh preview"),
        ),
        (
            crate::library::BatchStatus::Partial,
            true,
            Some("Retry the same confirmation after local file access recovers."),
            Some("fresh preview"),
        ),
        (
            crate::library::BatchStatus::Partial,
            false,
            Some("run a fresh preview before confirming again"),
            Some("retry the same confirmation"),
        ),
        (
            crate::library::BatchStatus::Failed,
            true,
            Some("Retry the same confirmation after local file access recovers."),
            Some("fresh preview"),
        ),
        (
            crate::library::BatchStatus::Failed,
            false,
            Some("run a fresh preview before confirming again"),
            Some("retry the same confirmation"),
        ),
    ];

    for (status, retryable_access_error, expected, unexpected) in cases {
        let result = crate::library::BatchMoveResult {
            id: "advice-matrix".to_string(),
            revision: "revision-a".to_string(),
            status,
            retryable_access_error,
            items: Vec::new(),
            warnings: Vec::new(),
        };
        let message = render_cloud_batch_result(&result);
        if let Some(expected) = expected {
            assert!(
                message.contains(expected),
                "{status:?} with retryable_access_error={retryable_access_error} should contain {expected:?}: {message}"
            );
        } else {
            let lowercase_message = message.to_ascii_lowercase();
            assert!(
                !lowercase_message.contains("confirmation")
                    && !lowercase_message.contains("fresh preview"),
                "completed batches should not include retry advice: {message}"
            );
        }
        if let Some(unexpected) = unexpected {
            let lowercase_message = message.to_ascii_lowercase();
            assert!(
                !lowercase_message.contains(unexpected),
                "{status:?} with retryable_access_error={retryable_access_error} should not contain {unexpected:?}: {message}"
            );
        }
    }
}

fn cloud_telegram_update(seq: u64, update_id: i64) -> CloudRequest {
    cloud_telegram_update_with_text(seq, update_id, "/queue")
}

fn cloud_telegram_update_with_text(seq: u64, update_id: i64, text: &str) -> CloudRequest {
    let request = CloudRequest {
        seq,
        kind: "telegram".to_string(),
        payload: serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": update_id,
                "chat": {"id": 123_456_789, "type": "private"},
                "from": {"id": 123_456_789},
                "text": text
            }
        }),
        created_at: 1_790_000_000_000,
        extra: serde_json::Map::new(),
    };
    cloud_request_with_envelope_identity(request, &format!("telegram-update-{update_id}"))
}

fn cloud_request_with_envelope_identity(
    mut request: CloudRequest,
    request_id: &str,
) -> CloudRequest {
    let payload = serde_json::to_vec(&request.payload).expect("payload should serialize");
    request.extra.insert(
        "request_id".to_string(),
        serde_json::Value::String(request_id.to_string()),
    );
    request.extra.insert(
        "payload_hash".to_string(),
        serde_json::Value::String(format!("{:x}", Sha256::digest(payload))),
    );
    request
}

fn cloud_ack_receipt_matches_request(receipt: &serde_json::Value, request: &CloudRequest) -> bool {
    receipt["seq"].as_u64() == Some(request.seq)
        && receipt["kind"].as_str() == Some(request.kind.as_str())
        && receipt["created_at"].as_u64() == Some(request.created_at)
        && receipt["request_id"].as_str()
            == request
                .extra
                .get("request_id")
                .and_then(serde_json::Value::as_str)
        && receipt["payload_hash"].as_str()
            == request
                .extra
                .get("payload_hash")
                .and_then(serde_json::Value::as_str)
}

#[cfg(unix)]
#[test]
fn cloud_database_epoch_scopes_reused_sequence_without_replacing_old_receipt() {
    const SEQ: u64 = 77;
    const OLD_EPOCH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const NEW_EPOCH: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let root = temp_cloud_bot_dir("database-epoch-sequence-scope");
    fs::create_dir_all(&root).expect("cloud root should create");
    let old_inbox =
        CloudInbox::open_for_epoch(&root, OLD_EPOCH).expect("old epoch inbox should open");
    let old_request = cloud_telegram_update_with_text(SEQ, 7_701, "/queue");
    old_inbox
        .persist(old_request.clone())
        .expect("old epoch request should persist");
    let old_result = serde_json::json!({"preview_id":"old-preview","completed":true});
    old_inbox
        .persist_dispatch_result(SEQ, InboxStatus::Completed, Some(old_result.clone()), None)
        .expect("old epoch receipt should become completed");
    let old_record_path = old_inbox.record_path_for_test(SEQ);
    let old_record_bytes = fs::read(&old_record_path).expect("old receipt bytes should read");
    let (old_device, old_inode) = {
        let old_identity = fs::metadata(&old_record_path).expect("old receipt should stat");
        (old_identity.dev(), old_identity.ino())
    };
    drop(old_inbox);

    let new_inbox = CloudInbox::open_for_epoch(&root, NEW_EPOCH)
        .expect("new epoch inbox should open independently");
    assert!(
        new_inbox
            .records()
            .expect("new epoch records should list")
            .is_empty(),
        "a reused sequence must not inherit the prior epoch's completed confirmation"
    );
    let new_request = cloud_telegram_update_with_text(SEQ, 7_702, "/help");
    new_inbox
        .persist(new_request.clone())
        .expect("the new epoch may safely use the same sequence");
    let new_record = new_inbox
        .record(SEQ)
        .expect("new epoch record should load")
        .expect("new epoch record should exist");
    assert_eq!(new_record.request, new_request);
    assert_eq!(new_record.status, InboxStatus::Pending);
    assert_ne!(new_inbox.record_path_for_test(SEQ), old_record_path);

    let old_identity_after = fs::metadata(&old_record_path).expect("old receipt should remain");
    assert_eq!(old_identity_after.dev(), old_device);
    assert_eq!(old_identity_after.ino(), old_inode);
    assert_eq!(
        fs::read(&old_record_path).expect("old receipt bytes should remain readable"),
        old_record_bytes,
        "writing the new namespace must not replace or mutate the old receipt"
    );
    drop(new_inbox);

    let reopened_old =
        CloudInbox::open_for_epoch(&root, OLD_EPOCH).expect("old epoch should remain reopenable");
    let reopened_record = reopened_old
        .record(SEQ)
        .expect("old epoch receipt should load after reopen")
        .expect("old completed receipt should remain present");
    assert_eq!(reopened_record.request, old_request);
    assert_eq!(reopened_record.status, InboxStatus::Completed);
    assert_eq!(reopened_record.result, Some(old_result));

    drop(reopened_old);
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_rest_epoch_mismatch_keeps_reconciliation_closed_without_ack_or_dispatch() {
    const ACTUAL_EPOCH: &str = "cccccccccccccccccccccccccccccccc";

    let root = temp_cloud_bot_dir("database-epoch-rest-mismatch");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir)
            .expect("epoch-scoped cloud inbox should open"),
    );
    let existing_local = cloud_telegram_update_with_text(702, 7_020, "/help");
    inbox
        .persist(existing_local.clone())
        .expect("local pending work should be durable before reconciliation");
    let remote = cloud_telegram_update_with_text(703, 7_030, "/queue");
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api_with_epoch(
            config.downloads.video_dir.clone(),
            vec![remote.clone()],
            ACTUAL_EPOCH.to_string(),
        )
        .await;
    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");

    let error = sync_cloud_backlog(&runtime)
        .await
        .expect_err("a different authoritative epoch must stop reconciliation");
    let epoch_error = error
        .downcast_ref::<CloudDatabaseEpochChanged>()
        .expect("REST epoch mismatch should remain a typed fatal condition");
    assert_eq!(epoch_error.expected, crate::cloud::TEST_DATABASE_EPOCH);
    assert_eq!(epoch_error.actual, ACTUAL_EPOCH);
    assert!(runtime.epoch_fatal.is_signaled());
    assert!(
        !runtime
            .reconciliation_complete
            .load(AtomicOrdering::Acquire)
    );
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        0,
        "an epoch mismatch must not publish Cloud readiness"
    );
    assert_eq!(cloud_state.fetch_count.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(cloud_state.ack_count.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(
        inbox
            .record(existing_local.seq)
            .expect("existing local receipt should remain readable")
            .expect("existing local receipt should remain present")
            .status,
        InboxStatus::Pending
    );
    assert!(
        inbox
            .record(remote.seq)
            .expect("mismatched remote sequence should be checked")
            .is_none(),
        "a request from another epoch must not enter the local inbox"
    );

    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    tokio::time::timeout(Duration::from_secs(2), dispatcher)
        .await
        .expect("fatal epoch latch should stop dispatch promptly")
        .expect("dispatcher should not panic");
    assert!(
        telegram_requests.try_recv().is_err(),
        "no Telegram dispatch should start while the database epoch is unknown"
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn cloud_sequence_conflict_rotates_only_matching_remote_envelope_and_stops() {
    const NEXT_EPOCH: &str = "dddddddddddddddddddddddddddddddd";
    const SEQ: u64 = 811;

    let root = temp_cloud_bot_dir("database-epoch-sequence-conflict");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir)
            .expect("epoch-scoped cloud inbox should open"),
    );
    let existing_local = cloud_telegram_update_with_text(SEQ, 8_110, "/help");
    inbox
        .persist(existing_local.clone())
        .expect("the prior local envelope should be durable");
    let record_path = inbox.record_path_for_test(SEQ);
    let original_bytes = fs::read(&record_path).expect("old receipt bytes should read");
    let (original_device, original_inode) = {
        let original_identity = fs::metadata(&record_path).expect("old receipt should stat");
        (original_identity.dev(), original_identity.ino())
    };

    let remote = cloud_request_with_envelope_identity(
        cloud_telegram_update_with_text(SEQ, 8_111, "/queue"),
        "telegram-update-811",
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api_with_epoch(
            config.downloads.video_dir.clone(),
            vec![remote.clone()],
            crate::cloud::TEST_DATABASE_EPOCH.to_string(),
        )
        .await;
    *cloud_state
        .rotate_epoch_to
        .lock()
        .expect("fake rotation lock should remain healthy") = Some(NEXT_EPOCH.to_string());
    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");

    let error = sync_cloud_backlog(&runtime)
        .await
        .expect_err("a reused sequence with another envelope must enter fatal CAS recovery");
    let epoch_error = error
        .downcast_ref::<CloudDatabaseEpochChanged>()
        .expect("successful CAS rotation should request a process restart");
    assert_eq!(epoch_error.expected, crate::cloud::TEST_DATABASE_EPOCH);
    assert_eq!(epoch_error.actual, NEXT_EPOCH);
    assert!(runtime.epoch_fatal.is_signaled());
    assert!(
        !runtime
            .reconciliation_complete
            .load(AtomicOrdering::Acquire)
    );
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        0,
        "a sequence collision must not publish Cloud readiness"
    );
    assert_eq!(cloud_state.fetch_count.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(cloud_state.ack_count.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(
        cloud_state
            .epoch_recovery_requests
            .lock()
            .expect("fake recovery list should remain healthy")
            .as_slice(),
        &[serde_json::json!({
            "seq": SEQ,
            "request_id": "telegram-update-811",
            "payload_hash": remote.extra["payload_hash"],
            "kind": remote.kind,
            "created_at": remote.created_at
        })],
        "the recovery CAS must bind the original sequence and exact remote envelope identity"
    );
    assert_eq!(
        *cloud_state
            .database_epoch
            .lock()
            .expect("fake epoch lock should remain healthy"),
        NEXT_EPOCH,
        "the fake server should rotate only after a matching CAS"
    );
    let local_after = inbox
        .record(SEQ)
        .expect("prior local record should remain readable")
        .expect("prior local record should remain present");
    assert_eq!(local_after.request, existing_local);
    assert_eq!(local_after.status, InboxStatus::Pending);
    let identity_after = fs::metadata(&record_path).expect("old receipt should remain on disk");
    assert_eq!(identity_after.dev(), original_device);
    assert_eq!(identity_after.ino(), original_inode);
    assert_eq!(
        fs::read(&record_path).expect("old receipt bytes should remain readable"),
        original_bytes,
        "sequence conflict recovery must not overwrite the old epoch receipt"
    );

    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    tokio::time::timeout(Duration::from_secs(2), dispatcher)
        .await
        .expect("fatal epoch latch should stop dispatch promptly")
        .expect("dispatcher should not panic");
    assert!(
        telegram_requests.try_recv().is_err(),
        "no Telegram dispatch should start after sequence-conflict recovery"
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_ack_does_not_clear_a_replacement_envelope_with_the_same_sequence() {
    const SEQ: u64 = 812;

    let root = temp_cloud_bot_dir("ack-envelope-replacement");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );

    let fetched = cloud_telegram_update_with_text(SEQ, 8_120, "/queue");
    let replacement = cloud_telegram_update_with_text(SEQ, 8_121, "/help");
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), vec![fetched.clone()]).await;
    *cloud_state
        .replace_pending_before_ack
        .lock()
        .expect("fake ACK replacement lock should remain healthy") = Some(replacement.clone());
    let (telegram, _telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");

    let error = sync_cloud_backlog(&runtime)
        .await
        .expect_err("an ACK for the fetched envelope must reject a same-sequence replacement");
    let conflict = error
        .downcast_ref::<crate::cloud::CloudInboxSequenceConflict>()
        .expect("an ACK identity collision should remain a typed sequence conflict");
    assert_eq!(conflict.seq, SEQ);
    assert!(
        !runtime
            .reconciliation_complete
            .load(AtomicOrdering::Acquire),
        "the authoritative backlog is not reconciled after an identity conflict"
    );
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        0,
        "a rejected old ACK must not publish readiness"
    );
    assert_eq!(cloud_state.ack_count.load(AtomicOrdering::SeqCst), 1);
    let pending_after_ack = cloud_state
        .pending
        .lock()
        .expect("fake pending list should remain healthy")
        .clone();
    assert_eq!(
        pending_after_ack.as_slice(),
        std::slice::from_ref(&replacement),
        "the replacement envelope must remain pending after the old identity is rejected"
    );

    let receipts = cloud_state
        .ack_request_bodies
        .lock()
        .expect("fake ACK request list should remain healthy")[0]["receipts"]
        .as_array()
        .expect("ACK should carry exact receipt identities")
        .clone();
    assert_eq!(receipts.len(), 1);
    assert!(cloud_ack_receipt_matches_request(&receipts[0], &fetched));
    assert!(
        !cloud_ack_receipt_matches_request(&receipts[0], &replacement),
        "the sent ACK must identify the fetched envelope, not the replacement"
    );

    let local = inbox
        .record(SEQ)
        .expect("durable fetched request should remain readable")
        .expect("durable fetched request should remain on disk");
    assert_eq!(local.request, fetched);
    assert_eq!(local.status, InboxStatus::Pending);

    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_rest_persists_before_ack_and_recovers_duplicate_backlog_after_restart() {
    let root = temp_cloud_bot_dir("rest-ack-restart");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let newer = cloud_telegram_update(9, 90);
    let older = cloud_telegram_update(2, 20);
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) = spawn_fake_cloud_api(
        config.downloads.video_dir.clone(),
        vec![newer, older.clone(), older],
    )
    .await;
    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;

    let mut first_runtime = make_runtime(
        telegram.clone(),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    first_runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    assert!(sync_cloud_backlog(&first_runtime).await.is_err());
    assert_eq!(
        first_runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        0,
        "a failed authoritative ACK must not emit Cloud readiness"
    );
    assert!(
        cloud_state.durable_before_ack.load(AtomicOrdering::SeqCst),
        "mock Worker must observe the complete private inbox record before ACK"
    );
    let first_records = inbox.records().expect("first request should persist");
    assert_eq!(
        first_records
            .iter()
            .map(|record| record.request.seq)
            .collect::<Vec<_>>(),
        vec![2, 9],
        "every unique request in the REST page must be durable before its batch ACK"
    );
    assert_eq!(first_records[0].status, InboxStatus::Pending);
    assert_eq!(first_records[1].status, InboxStatus::Pending);
    drop(first_runtime);
    drop(inbox);

    // Reopening models process restart after durable persistence but before a successful ACK.
    let reopened_inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir)
            .expect("pending cloud inbox should recover after restart"),
    );
    let mut runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&reopened_inbox),
    );
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    sync_cloud_backlog(&runtime)
        .await
        .expect("redelivered requests should persist and ACK");
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        1,
        "readiness should be emitted after the full authoritative backlog is durable and ACKed"
    );
    let records = reopened_inbox
        .records()
        .expect("recovered inbox records should list");
    assert_eq!(
        records
            .iter()
            .map(|record| record.request.seq)
            .collect::<Vec<_>>(),
        vec![2, 9],
        "out-of-order and duplicate delivery should retain each exact sequence once"
    );
    assert!(
        records
            .iter()
            .all(|record| record.status == InboxStatus::Pending)
    );

    // The real dispatcher consumes the recovered durable inbox and sends a reply for each
    // distinct cloud sequence. Repeated seq=2 in the REST response must not execute twice.
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    for _ in 0..2 {
        let request = tokio::time::timeout(Duration::from_secs(5), telegram_requests.recv())
            .await
            .expect("dispatcher should respond to recovered /queue updates")
            .expect("mock Telegram should receive a response");
        assert_eq!(request.method, "sendMessage");
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let records = reopened_inbox
                .records()
                .expect("request results should load");
            if records
                .iter()
                .all(|record| record.status == InboxStatus::Completed)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all distinct requests should complete");
    assert!(
        telegram_requests.try_recv().is_err(),
        "duplicate seq must not send a second reply"
    );
    sync_cloud_backlog(&runtime)
        .await
        .expect("empty backlog should remain safe after duplicate delivery");
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        1,
        "later successful reconciliations must not repeat the Cloud readiness marker"
    );
    assert!(
        telegram_requests.try_recv().is_err(),
        "empty backlog must not replay a completed request"
    );

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(reopened_inbox);
    drop(queue);
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_readiness_requires_durable_persistence_and_ack_then_emits_once() {
    let root = temp_cloud_bot_dir("readiness-durable-ack");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox_provider = Arc::new(FailOnceInboxFileProvider::default());
    let provider: Arc<dyn QueueFileProvider> = inbox_provider.clone();
    let inbox = Arc::new(
        open_runtime_test_inbox_with_file_provider(&config.downloads.video_dir, provider)
            .expect("cloud inbox should open with retryable test storage"),
    );
    let request = cloud_telegram_update_with_text(401, 401, "/help");
    let request_path = runtime_test_request_path(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
        request.seq,
    );
    inbox_provider.fail_next_write_to(request_path);
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), vec![request]).await;
    let telegram = TelegramClient::with_test_api_base_url(
        TEST_TELEGRAM_TOKEN.to_string(),
        "http://127.0.0.1:1".to_string(),
    );
    let mut runtime = make_runtime(telegram, config, queue.clone(), inbox.clone());
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");

    let persistence_error = sync_cloud_backlog(&runtime)
        .await
        .expect_err("an inbox write failure must stop reconciliation before ACK");
    assert!(
        format!("{persistence_error:#}").contains("simulated one-time inbox storage failure"),
        "the reconciliation failure should retain its inbox persistence cause"
    );
    assert_eq!(cloud_state.ack_count.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        0,
        "an inbox persistence failure must not emit Cloud readiness"
    );

    assert!(
        sync_cloud_backlog(&runtime).await.is_err(),
        "the mock's failed ACK response must also keep Cloud unready"
    );
    assert_eq!(cloud_state.ack_count.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        0,
        "durable persistence without a successful ACK is not full reconciliation"
    );

    sync_cloud_backlog(&runtime)
        .await
        .expect("the redelivered durable request should eventually reconcile");
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        1,
        "the first complete durable-and-ACKed reconciliation should emit readiness"
    );
    sync_cloud_backlog(&runtime)
        .await
        .expect("later empty reconciliation should succeed");
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        1,
        "periodic successful reconciliations must not repeat readiness"
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn startup_recovery_defers_ack_and_keeps_the_authoritative_page_in_memory() {
    let root = temp_cloud_bot_dir("startup-recovery-no-ack");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = open_runtime_test_inbox(&config.downloads.video_dir)
        .expect("cloud inbox should open for history fixture");
    for seq in 1..=120 {
        inbox
            .persist(cloud_telegram_update(seq, seq as i64))
            .expect("history request should persist");
        inbox
            .persist_dispatch_result(seq, InboxStatus::Completed, None, None)
            .expect("history request should be terminal");
    }
    drop(inbox);

    let pending = cloud_telegram_update(200, 200);
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), vec![pending]).await;
    cloud_state
        .fail_first_ack
        .store(false, AtomicOrdering::SeqCst);
    let inbox_directory = runtime_test_inbox_directory(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let (provider, read_started, release_read) = PauseOnDirectoryReadProvider::new(inbox_directory);
    let provider_trait: Arc<dyn QueueFileProvider> = provider.clone();
    let inbox = Arc::new(
        open_runtime_test_inbox_with_file_provider(&config.downloads.video_dir, provider_trait)
            .expect("bounded inbox recovery should start"),
    );
    provider.arm();
    let telegram = TelegramClient::with_test_api_base_url(
        TEST_TELEGRAM_TOKEN.to_string(),
        "http://127.0.0.1:1".to_string(),
    );
    let mut runtime = make_runtime(telegram, config.clone(), queue.clone(), inbox.clone());
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");

    let sync_runtime = runtime.clone();
    let sync_task = tokio::spawn(async move { sync_cloud_backlog(&sync_runtime).await });
    tokio::task::spawn_blocking(move || read_started.recv_timeout(Duration::from_secs(5)))
        .await
        .expect("recovery pause waiter should join")
        .expect("initial recovery should reach its controlled read");
    assert_eq!(cloud_state.fetch_count.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(cloud_state.ack_count.load(AtomicOrdering::SeqCst), 0);
    let incoming_path = runtime_test_request_path(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
        200,
    );
    assert!(
        !incoming_path.exists(),
        "the incoming envelope must remain unwritten while the startup index is incomplete"
    );
    assert!(
        !sync_task.is_finished(),
        "backlog reconciliation must wait rather than ACK an incomplete recovery"
    );
    release_read
        .send(())
        .expect("test should release the bounded recovery read");
    tokio::time::timeout(Duration::from_secs(10), sync_task)
        .await
        .expect("backlog recovery should finish within its bounded history fixture")
        .expect("backlog sync task should not panic")
        .expect("recovered backlog request should persist and ACK");

    assert_eq!(cloud_state.fetch_count.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(cloud_state.ack_count.load(AtomicOrdering::SeqCst), 1);
    assert!(cloud_state.durable_before_ack.load(AtomicOrdering::SeqCst));
    assert!(
        cloud_state
            .pending
            .lock()
            .expect("fake cloud pending lock should remain healthy")
            .is_empty()
    );
    assert_eq!(
        inbox
            .record(200)
            .expect("persisted request should load")
            .expect("request should be durable after ACK")
            .status,
        InboxStatus::Pending
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn completed_queue_publication_refreshes_the_cloud_library_snapshot() {
    let root = temp_cloud_bot_dir("publication-refresh");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let telegram = TelegramClient::with_test_api_base_url(
        TEST_TELEGRAM_TOKEN.to_string(),
        "http://127.0.0.1:1".to_string(),
    );
    let runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let watcher = tokio::spawn(cloud_library_publication_watcher(runtime.clone()));

    let task_id = "cloud-publication-refresh-task";
    queue
        .create(TaskRecord::new(
            task_id.to_string(),
            1,
            1,
            123_456_789,
            Some(123_456_789),
            0,
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
            },
        ))
        .expect("queue task should persist");
    queue
        .set_status(task_id, TaskStatus::Running, None)
        .expect("queue task should start");
    let running = queue
        .get(task_id)
        .expect("running queue task should load")
        .expect("running queue task should exist");
    queue
        .begin_verification_if_generation(task_id, running.generation)
        .expect("verification should begin")
        .expect("queue task should remain current");
    let media_path = config.downloads.video_dir.join("published.mp4");
    std::fs::write(&media_path, b"published fixture")
        .expect("published media fixture should write");
    queue
        .complete_if_generation(
            task_id,
            running.generation,
            media_path.display().to_string(),
            std::slice::from_ref(&media_path),
            std::collections::BTreeMap::new(),
        )
        .expect("published output should complete")
        .expect("queue record should remain current");

    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if runtime.library_snapshot.lock().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("queue publication should trigger a cached library rescan");

    watcher.abort();
    let _ = watcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_queue_command_progresses_while_management_job_waits_at_barrier() {
    let root = temp_cloud_bot_dir("slow-management");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));

    let management_runtime = runtime.clone();
    let (started_sender, started_receiver) = oneshot::channel();
    let release_management = Arc::new(Notify::new());
    let management_release = Arc::clone(&release_management);
    let management_task = tokio::spawn(async move {
        let _permit = management_runtime
            .management_dispatch
            .acquire_owned()
            .await
            .expect("management semaphore should remain open");
        let _ = started_sender.send(());
        management_release.notified().await;
    });
    started_receiver
        .await
        .expect("slow management job should reach barrier");

    let request = CloudRequest {
        seq: 1,
        kind: "telegram".to_string(),
        payload: serde_json::json!({
            "update_id": 77,
            "message": {
                "message_id": 9,
                "chat": {"id": 123_456_789, "type": "private"},
                "from": {"id": 123_456_789},
                "text": "/queue"
            }
        }),
        created_at: 100,
        extra: serde_json::Map::new(),
    };
    inbox
        .persist(request)
        .expect("Telegram envelope should persist");
    let record = inbox
        .pending()
        .expect("pending update should list")
        .remove(0);
    let telegram_runtime = runtime.clone();
    let telegram_task = tokio::spawn(async move {
        let _permit = telegram_runtime
            .telegram_dispatch
            .clone()
            .acquire_owned()
            .await
            .expect("Telegram semaphore should remain open");
        dispatch_cloud_record(telegram_runtime, record).await;
    });

    let request = tokio::time::timeout(Duration::from_secs(5), requests.recv())
        .await
        .expect("queue response should not wait for management barrier")
        .expect("fake Telegram should observe queue response");
    assert_eq!(request.method, "sendMessage");
    telegram_task
        .await
        .expect("Telegram dispatch should finish while management is blocked");
    assert!(!management_task.is_finished());
    assert_eq!(
        inbox.records().expect("request result should persist")[0].status,
        InboxStatus::Completed
    );

    release_management.notify_one();
    management_task
        .await
        .expect("management barrier task should join");
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn file_batch_reuses_one_telegram_message_and_edits_it() {
    let root = temp_cloud_bot_dir("batch-message");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let request = CloudRequest {
        seq: 31,
        kind: "file_preview".to_string(),
        payload: serde_json::json!({"item_ids": ["item-a"], "target_relative_dir": ""}),
        created_at: 100,
        extra: serde_json::Map::new(),
    };
    inbox
        .persist(request)
        .expect("preview request should persist");
    inbox
        .update(31, |record| {
            record.status = InboxStatus::AwaitingConfirmation;
            record.result = Some(serde_json::json!({
                "preview_id": "preview-a",
                "revision": "revision-a",
                "preview": {}
            }));
        })
        .expect("preview result should persist");
    let runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));

    assert_eq!(
        begin_cloud_batch_message(&runtime, 31, "preview-a")
            .await
            .expect("first batch message should send"),
        Some(1000)
    );
    assert_eq!(
        begin_cloud_batch_message(&runtime, 31, "preview-a")
            .await
            .expect("retry should reuse the message"),
        Some(1000)
    );
    let mut recorded = Vec::new();
    while let Ok(request) = requests.try_recv() {
        recorded.push(request);
    }
    assert_eq!(
        recorded
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        vec!["sendMessage", "editMessageText"]
    );
    assert_eq!(recorded[1].body["message_id"].as_i64(), Some(1000));
    assert_eq!(recorded[1].body["chat_id"].as_i64(), Some(123_456_789));
    let stored = inbox.records().expect("batch receipt should load");
    assert_eq!(
        stored[0]
            .result
            .as_ref()
            .and_then(|result| result["telegram_status_message_id"].as_i64()),
        Some(1000)
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn failed_file_confirmation_edits_the_started_batch_message() {
    let root = temp_cloud_bot_dir("batch-message-failure");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let source_path = config.downloads.video_dir.join("clip.mp4");
    fs::write(&source_path, b"original media contents").expect("media fixture should write");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let library =
        LibraryManager::open_with_preview_cleanup_gate(&config.downloads.video_dir, inbox.clone())
            .expect("library manager should open with inbox cleanup coordination");
    let snapshot = library
        .scan(&[])
        .expect("library scan should find the media fixture");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("media fixture should appear in the library")
        .id
        .clone();
    let preview = library
        .preview(
            std::slice::from_ref(&item_id),
            "organized",
            false,
            ConflictPolicy::KeepBoth,
        )
        .expect("valid media should produce a persistent move preview");
    let preview_id = preview.id.clone();
    let preview_revision = preview.revision.clone();
    inbox
        .persist(CloudRequest {
            seq: 41,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({"item_ids": [item_id], "target_relative_dir": "organized"}),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("preview request should persist");
    inbox
        .update(41, |record| {
            record.status = InboxStatus::AwaitingConfirmation;
            record.result = Some(serde_json::json!({
                "preview_id": preview_id,
                "revision": preview_revision,
                "preview": preview,
            }));
        })
        .expect("preview confirmation receipt should persist");
    let changed_contents = b"media contents changed after preview";
    fs::write(&source_path, changed_contents).expect("source should change after preview creation");
    inbox
        .persist(CloudRequest {
            seq: 42,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": preview_id,
                "revision": preview_revision
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("confirmation request should persist");
    let runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    runtime.request_notify.notify_one();
    let finished_confirmation = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let confirmation = inbox
                .record(42)
                .expect("confirmation receipt should load")
                .expect("confirmation receipt should remain present");
            let preview = inbox
                .record(41)
                .expect("preview receipt should load")
                .expect("preview receipt should remain present");
            if confirmation.status == InboxStatus::Failed && preview.status == InboxStatus::Stale {
                break confirmation;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("permanently rejected confirmation should finish and stale its preview");
    assert_eq!(finished_confirmation.status, InboxStatus::Failed);
    assert!(
        finished_confirmation
            .error
            .as_deref()
            .is_some_and(|error| error.contains("preview")),
        "failed operation receipt should explain why confirmation could not execute"
    );
    let preview_record = inbox
        .record(41)
        .expect("preview receipt should load")
        .expect("preview receipt should remain present");
    assert_eq!(preview_record.status, InboxStatus::Stale);

    let mut recorded = Vec::new();
    while let Ok(request) = requests.try_recv() {
        recorded.push(request);
    }
    assert_eq!(
        recorded
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        vec!["sendMessage", "editMessageText"]
    );
    assert_eq!(recorded[1].body["message_id"].as_i64(), Some(1_000));
    let failure_message = recorded[1].body["text"]
        .as_str()
        .expect("failed batch edit should include rendered status text");
    assert!(failure_message.contains(" failed: 0 file(s) moved"));
    assert!(failure_message.contains("1 item(s) need review"));
    assert!(failure_message.contains("run a fresh preview before confirming again"));

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(library);
    assert_eq!(
        fs::read(&source_path).expect("changed source media should remain readable"),
        changed_contents,
        "revalidation must preserve the changed source object without moving it"
    );
    assert!(
        !root.join("videos/organized/clip.mp4").exists(),
        "revalidation must not publish the changed media at the preview target"
    );
    stop_fake_telegram_api(shutdown, server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn coalesced_queue_progress_publishes_final_state_without_library_rescan() {
    let root = temp_cloud_bot_dir("coalesced-progress");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let mut runtime = runtime;
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    let task_id = "coalesced-cloud-progress";
    queue
        .create(TaskRecord::new(
            task_id.to_string(),
            1,
            1,
            123_456_789,
            Some(123_456_789),
            0,
            JobRequest::Youtube {
                url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ".to_string(),
            },
        ))
        .expect("progress task should persist");
    let queue_changes = queue.state_change_notify();
    queue_changes.notified().await;
    *runtime.library_snapshot.lock().await =
        Some(empty_cloud_test_library_snapshot("stable-library-revision"));

    let publication_watcher = tokio::spawn(cloud_library_publication_watcher(runtime.clone()));
    let queue_state_watcher = tokio::spawn(cloud_queue_state_watcher_with_debounce(
        runtime.clone(),
        Duration::from_millis(75),
    ));
    let state_reporter = tokio::spawn(cloud_state_reporter(runtime.clone()));
    for completed in 1..=8 {
        queue
            .set_collection_progress(task_id, 10, completed, 2)
            .expect("progress update should persist");
    }

    let posts = wait_for_state_posts(&cloud_state, 1).await;
    let tasks = posts[0]["tasks"]
        .as_array()
        .expect("reported task summaries should be an array");
    let task = tasks
        .iter()
        .find(|task| task["id"].as_str() == Some(task_id))
        .expect("final coalesced state should include the progress task");
    assert_eq!(task["media_entries_total"].as_u64(), Some(10));
    assert_eq!(task["media_entries_completed"].as_u64(), Some(8));
    assert_eq!(task["media_entries_failed"].as_u64(), Some(2));

    tokio::time::sleep(Duration::from_millis(2_200)).await;
    let posts = cloud_state
        .state_posts
        .lock()
        .expect("fake state post lock should not poison")
        .clone();
    assert_eq!(
        posts.len(),
        1,
        "progress burst should publish one coalesced state"
    );
    assert_eq!(
        posts[0]["revision"].as_str(),
        Some("stable-library-revision")
    );
    assert_eq!(
        runtime
            .library_snapshot
            .lock()
            .await
            .as_ref()
            .map(|snapshot| snapshot.revision.as_str()),
        Some("stable-library-revision"),
        "queue progress must not request a media-library rescan"
    );

    state_reporter.abort();
    queue_state_watcher.abort();
    publication_watcher.abort();
    let _ = state_reporter.await;
    let _ = queue_state_watcher.await;
    let _ = publication_watcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_websocket_stays_connected_through_long_idle_and_acks_durable_request() {
    let root = temp_cloud_bot_dir("websocket-idle");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (telegram, _telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut runtime = make_runtime(telegram, config.clone(), queue, Arc::clone(&inbox));

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake cloud WebSocket should bind localhost");
    let address = listener
        .local_addr()
        .expect("fake cloud WebSocket should expose its address");
    runtime.client = CloudClient::with_test_base_url(&format!("http://{address}/"))
        .expect("test cloud client should use localhost");

    let (connected_tx, connected_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (ack_tx, ack_rx) = oneshot::channel();
    let server_root = config.downloads.video_dir.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .context("fake cloud WebSocket accept failed")?;
        let mut socket = accept_async(stream)
            .await
            .context("fake cloud WebSocket handshake failed")?;
        let _ = connected_tx.send(());
        release_rx
            .await
            .context("test should release the idle WebSocket")?;
        let request = cloud_telegram_update(73, 73);
        socket
            .send(WsMessage::Text(
                serde_json::to_string(&request)
                    .context("cloud request should serialize")?
                    .into(),
            ))
            .await
            .context("fake cloud WebSocket request send failed")?;

        let (mut requests_stream, _) = listener
            .accept()
            .await
            .context("fake cloud REST request accept failed")?;
        let backlog_fetch = read_fake_telegram_request(&mut requests_stream).await?;
        let expected_authorization = format!("Bearer {TEST_CLOUD_SECRET}");
        anyhow::ensure!(
            backlog_fetch.method == "requests"
                && backlog_fetch.authorization.as_deref() == Some(expected_authorization.as_str()),
            "receiver should fetch the authoritative REST request before ACK"
        );
        let backlog_payload = serde_json::json!({
            "database_epoch": crate::cloud::TEST_DATABASE_EPOCH,
            "requests": [&request]
        });
        requests_stream
            .write_all(fake_http_response(200, &backlog_payload).as_bytes())
            .await
            .context("fake cloud REST response write failed")?;
        drop(requests_stream);

        let (mut ack_stream, _) = listener
            .accept()
            .await
            .context("fake cloud ACK accept failed")?;
        let ack = read_fake_telegram_request(&mut ack_stream).await?;
        let record_path =
            runtime_test_request_path(&server_root, crate::cloud::TEST_DATABASE_EPOCH, request.seq);
        let durable = std::fs::read(record_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<InboxRecord>(&bytes).ok())
            .is_some_and(|record| record.request == request);
        let receipts = ack.body["receipts"].as_array();
        let valid_ack = ack.method == "ack"
            && ack.authorization.as_deref() == Some(expected_authorization.as_str())
            && ack.database_epoch.as_deref() == Some(crate::cloud::TEST_DATABASE_EPOCH)
            && receipts.is_some_and(|receipts| {
                receipts.len() == 1 && cloud_ack_receipt_matches_request(&receipts[0], &request)
            });
        ack_stream
            .write_all(fake_http_response(200, &serde_json::json!({"ok":true})).as_bytes())
            .await
            .context("fake cloud ACK response write failed")?;
        socket
            .send(WsMessage::Close(None))
            .await
            .context("fake cloud WebSocket close failed")?;
        let _ = ack_tx.send((durable, valid_ack));
        Ok::<(), anyhow::Error>(())
    });

    let mut socket = runtime
        .client
        .connect_websocket()
        .await
        .expect("test client should connect to local WebSocket");
    connected_rx
        .await
        .expect("fake server should finish WebSocket handshake");
    let waiting = Arc::new(Notify::new());
    let session_waiting = Arc::clone(&waiting);
    let session_runtime = runtime.clone();
    let session = tokio::spawn(async move {
        receive_cloud_websocket_session(&session_runtime, &mut socket, Some(session_waiting)).await
    });

    waiting.notified().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(76)).await;
    tokio::task::yield_now().await;
    assert!(
        !session.is_finished(),
        "an idle WebSocket session must stay connected beyond 75 seconds"
    );
    tokio::time::resume();
    release_tx
        .send(())
        .expect("test should release the WebSocket after idle time");

    let (durable_before_ack, ack_valid) = ack_rx
        .await
        .expect("fake server should observe an ACK after the idle period");
    assert!(
        durable_before_ack,
        "full request must be durable before ACK"
    );
    assert!(
        ack_valid,
        "ACK should use local bearer auth and the exact sequence"
    );
    session
        .await
        .expect("WebSocket receiver task should not panic")
        .expect("WebSocket close should end the receiver session cleanly");
    server
        .await
        .expect("fake cloud server should not panic")
        .expect("fake cloud server should complete");
    let records = inbox.records().expect("inbox records should load");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].request.seq, 73);
    assert_eq!(records[0].status, InboxStatus::Pending);

    drop(runtime);
    drop(inbox);
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn websocket_reconciles_and_acks_earlier_rest_requests_before_dispatching_later_frames() {
    let root = temp_cloud_bot_dir("websocket-rest-ordering");
    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let earlier = cloud_telegram_update_with_text(1, 1, "/help");
    let later = cloud_telegram_update_with_text(2, 2, "/help");
    let (cloud_base_url, events, first_ack, release_first_ack, cloud_server) =
        spawn_fake_cloud_websocket_api(
            config.downloads.video_dir.clone(),
            later,
            vec![earlier, cloud_telegram_update_with_text(2, 2, "/help")],
            200,
            true,
        )
        .await;
    let mut runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use the local WebSocket API");
    runtime
        .reconciliation_complete
        .store(false, AtomicOrdering::SeqCst);
    inbox
        .persist(cloud_telegram_update_with_text(90, 90, "/queue"))
        .expect("later local request should already be pending");

    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    runtime.request_notify.notify_one();
    let mut socket = runtime
        .client
        .connect_websocket()
        .await
        .expect("test client should connect to local WebSocket");
    let session_runtime = runtime.clone();
    let session = tokio::spawn(async move {
        receive_cloud_websocket_session(&session_runtime, &mut socket, None).await
    });

    let first_ack_observed = tokio::time::timeout(Duration::from_secs(5), first_ack).await;
    if first_ack_observed.is_err() {
        let _ = release_first_ack.send(());
        panic!("receiver should ACK the REST backlog before the WebSocket frame");
    }
    let (earlier_durable, earlier_ack_seqs) = first_ack_observed
        .expect("first REST ACK should arrive")
        .expect("fake server should observe the first REST ACK");
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let later_frame_is_durable_but_pending = inbox
        .record(2)
        .expect("later WebSocket receipt should load")
        .is_some_and(|record| record.status == InboxStatus::Pending);
    let local_dispatch_waited = telegram_requests.try_recv().is_err();
    let still_unreconciled = !runtime.reconciliation_complete.load(AtomicOrdering::SeqCst);
    release_first_ack
        .send(())
        .expect("test should release the REST ACK response");

    session
        .await
        .expect("WebSocket receive task should not panic")
        .expect("successful backlog reconciliation should close cleanly");
    tokio::time::timeout(Duration::from_secs(5), cloud_server)
        .await
        .expect("fake WebSocket API should finish")
        .expect("fake WebSocket API task should not panic")
        .expect("fake WebSocket API should complete");

    let events = events
        .lock()
        .expect("fake event log should not poison")
        .clone();
    assert!(
        earlier_durable,
        "the earlier REST request must be durable before ACK"
    );
    assert_eq!(earlier_ack_seqs, vec![1, 2]);
    assert!(
        later_frame_is_durable_but_pending,
        "the authoritative REST page must be durable before ACK and remain undispatched until ACK succeeds"
    );
    assert!(
        local_dispatch_waited,
        "pre-existing pending work must wait for reconciliation"
    );
    assert!(
        still_unreconciled,
        "the client must remain gated while the REST ACK is blocked"
    );
    assert_eq!(events, vec!["ws_frame:2", "fetch:200", "ack:1,2"]);
    assert!(
        runtime.reconciliation_complete.load(AtomicOrdering::SeqCst),
        "successful REST and WebSocket reconciliation should enable dispatch"
    );

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let complete = [1, 2, 90].into_iter().all(|seq| {
                inbox
                    .record(seq)
                    .expect("cloud receipt should load")
                    .is_some_and(|record| record.status == InboxStatus::Completed)
            });
            if complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("reconciled requests should dispatch after the REST backlog is acknowledged");
    let mut sent_messages = Vec::new();
    while let Ok(request) = telegram_requests.try_recv() {
        if request.method == "sendMessage" {
            sent_messages.push(request);
        }
    }
    assert_eq!(sent_messages.len(), 3);

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn websocket_backlog_fetch_failure_does_not_persist_or_dispatch_later_frames() {
    let root = temp_cloud_bot_dir("websocket-backlog-fetch-failure");
    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let later = cloud_telegram_update_with_text(2, 2, "/start");
    let (cloud_base_url, events, _first_ack, _release_first_ack, cloud_server) =
        spawn_fake_cloud_websocket_api(
            config.downloads.video_dir.clone(),
            later,
            Vec::new(),
            503,
            false,
        )
        .await;
    let mut runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use the local WebSocket API");
    runtime
        .reconciliation_complete
        .store(false, AtomicOrdering::SeqCst);
    inbox
        .persist(cloud_telegram_update_with_text(90, 90, "/queue"))
        .expect("later local request should already be pending");

    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    runtime.request_notify.notify_one();
    let mut socket = runtime
        .client
        .connect_websocket()
        .await
        .expect("test client should connect to local WebSocket");
    let session_runtime = runtime.clone();
    let session = tokio::spawn(async move {
        receive_cloud_websocket_session(&session_runtime, &mut socket, None).await
    });
    let session_result = tokio::time::timeout(Duration::from_secs(5), session)
        .await
        .expect("failed REST reconciliation should end the WebSocket session")
        .expect("WebSocket receive task should not panic");
    let server_result = tokio::time::timeout(Duration::from_secs(5), cloud_server)
        .await
        .expect("fake WebSocket API should finish")
        .expect("fake WebSocket API task should not panic");
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    assert!(
        session_result.is_err(),
        "REST fetch failure should propagate from the receiver"
    );
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        0,
        "a failed authoritative fetch must not emit Cloud readiness"
    );
    server_result.expect("fake WebSocket API should complete");
    assert!(
        inbox
            .record(2)
            .expect("later receipt should load")
            .is_none()
    );
    assert_eq!(
        inbox
            .record(90)
            .expect("pending local receipt should load")
            .expect("pending request should remain")
            .status,
        InboxStatus::Pending
    );
    assert!(!runtime.reconciliation_complete.load(AtomicOrdering::SeqCst));
    assert!(
        telegram_requests.try_recv().is_err(),
        "unreconciled local work must not dispatch"
    );
    assert_eq!(
        events
            .lock()
            .expect("fake event log should not poison")
            .as_slice(),
        ["ws_frame:2", "fetch:503"]
    );

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn local_dispatch_waits_for_initial_authoritative_reconciliation() {
    let root = temp_cloud_bot_dir("initial-reconciliation-gate");
    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let (cloud_base_url, _cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api(config.downloads.video_dir.clone(), Vec::new()).await;
    let mut runtime = make_runtime(telegram, config, Arc::clone(&queue), Arc::clone(&inbox));
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("test cloud client should use localhost");
    runtime
        .reconciliation_complete
        .store(false, AtomicOrdering::SeqCst);
    inbox
        .persist(cloud_telegram_update_with_text(90, 90, "/help"))
        .expect("local pending request should persist before startup reconciliation");

    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    runtime.request_notify.notify_one();
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    assert!(
        telegram_requests.try_recv().is_err(),
        "dispatcher notification and ticker must not bypass initial reconciliation"
    );
    assert!(
        !runtime.reconciliation_complete.load(AtomicOrdering::SeqCst),
        "test runtime should remain gated before authoritative fetch"
    );
    assert_eq!(
        inbox
            .record(90)
            .expect("pending request should load")
            .expect("pending request should remain durable")
            .status,
        InboxStatus::Pending
    );

    sync_cloud_backlog(&runtime)
        .await
        .expect("initial empty backlog reconciliation should succeed");
    assert!(
        runtime.reconciliation_complete.load(AtomicOrdering::SeqCst),
        "successful authoritative fetch should enable dispatcher"
    );
    assert_eq!(
        runtime
            .test_cloud_ready_marker_emissions
            .load(AtomicOrdering::SeqCst),
        1,
        "a successful REST-only reconciliation should establish Cloud readiness without WebSocket"
    );
    let request = tokio::time::timeout(Duration::from_secs(5), telegram_requests.recv())
        .await
        .expect("dispatcher should proceed after initial reconciliation")
        .expect("mock Telegram should receive the local request response");
    assert_eq!(request.method, "sendMessage");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if inbox
                .record(90)
                .expect("cloud receipt should load")
                .is_some_and(|record| record.status == InboxStatus::Completed)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("local request should complete after reconciliation");

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}

fn scan_order_test_snapshot(revision: &str) -> LibrarySnapshot {
    LibrarySnapshot {
        revision: revision.to_string(),
        scanned_at: 0,
        root_label: "scan-order-test".to_string(),
        items: Vec::new(),
        categories: Vec::new(),
        warnings: Vec::new(),
    }
}

#[tokio::test]
async fn cloud_library_publication_gate_keeps_snapshot_and_warning_ordered() {
    let root = temp_cloud_bot_dir("library-publication-order");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            TEST_TELEGRAM_TOKEN.to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );

    let older_started = Arc::new(Barrier::new(2));
    let release_older = Arc::new(Barrier::new(2));
    let older_started_worker = Arc::clone(&older_started);
    let release_older_worker = Arc::clone(&release_older);
    let older_runtime = runtime.clone();
    let older = tokio::spawn(async move {
        run_cloud_library_operation(&older_runtime, "older fixture scan failed", move |_, _| {
            let _ = older_started_worker.wait();
            let _ = release_older_worker.wait();
            Ok(((), scan_order_test_snapshot("older")))
        })
        .await
    });
    let older_started_waiter = Arc::clone(&older_started);
    tokio::task::spawn_blocking(move || {
        let _ = older_started_waiter.wait();
    })
    .await
    .expect("older scan start barrier should complete");

    let older_holds_publication_gate = runtime.library_scan_publication_gate.try_lock().is_err();
    let newer_runtime = runtime.clone();
    let mut newer_future = Box::pin(async move {
        run_cloud_library_operation(&newer_runtime, "newer fixture scan failed", move |_, _| {
            Ok(((), scan_order_test_snapshot("newer")))
        })
        .await
    });
    let newer_waits_on_gate = std::future::poll_fn(|context| {
        Poll::Ready(Future::poll(newer_future.as_mut(), context).is_pending())
    })
    .await;
    let newer = tokio::spawn(newer_future);
    let release_older_waiter = Arc::clone(&release_older);
    tokio::task::spawn_blocking(move || {
        let _ = release_older_waiter.wait();
    })
    .await
    .expect("older scan release barrier should complete");
    older
        .await
        .expect("older scan task should join")
        .expect("older scan should publish");
    newer
        .await
        .expect("newer scan task should join")
        .expect("newer scan should publish");
    assert!(
        older_holds_publication_gate && newer_waits_on_gate,
        "the older scan should hold the gate and a newer operation should poll to Pending"
    );
    assert_eq!(
        runtime
            .library_snapshot
            .lock()
            .await
            .as_ref()
            .map(|snapshot| snapshot.revision.as_str()),
        Some("newer"),
        "the later serialized scan should be the published snapshot"
    );

    let failing_started = Arc::new(Barrier::new(2));
    let release_failing = Arc::new(Barrier::new(2));
    let failing_started_worker = Arc::clone(&failing_started);
    let release_failing_worker = Arc::clone(&release_failing);
    let failing_runtime = runtime.clone();
    let failing = tokio::spawn(async move {
        run_cloud_library_operation::<()>(&failing_runtime, "older failure fixture", move |_, _| {
            let _ = failing_started_worker.wait();
            let _ = release_failing_worker.wait();
            Err(anyhow::anyhow!("older scan failed"))
        })
        .await
    });
    let failing_started_waiter = Arc::clone(&failing_started);
    tokio::task::spawn_blocking(move || {
        let _ = failing_started_waiter.wait();
    })
    .await
    .expect("failing scan start barrier should complete");

    let failing_holds_publication_gate = runtime.library_scan_publication_gate.try_lock().is_err();
    let recovery_runtime = runtime.clone();
    let mut recovery_future = Box::pin(async move {
        run_cloud_library_operation(
            &recovery_runtime,
            "recovery fixture scan failed",
            move |_, _| Ok(((), scan_order_test_snapshot("recovered"))),
        )
        .await
    });
    let recovery_waits_on_gate = std::future::poll_fn(|context| {
        Poll::Ready(Future::poll(recovery_future.as_mut(), context).is_pending())
    })
    .await;
    let recovery = tokio::spawn(recovery_future);
    let release_failing_waiter = Arc::clone(&release_failing);
    tokio::task::spawn_blocking(move || {
        let _ = release_failing_waiter.wait();
    })
    .await
    .expect("failing scan release barrier should complete");
    assert!(
        failing
            .await
            .expect("failing scan task should join")
            .is_err(),
        "the older scan should report its fixture failure"
    );
    recovery
        .await
        .expect("recovery task should join")
        .expect("recovery scan should publish");
    assert!(
        failing_holds_publication_gate && recovery_waits_on_gate,
        "the failing scan should hold the gate and recovery should poll to Pending"
    );
    assert_eq!(
        runtime
            .library_snapshot
            .lock()
            .await
            .as_ref()
            .map(|snapshot| snapshot.revision.as_str()),
        Some("recovered"),
        "a later successful scan should publish after the older failure"
    );
    assert!(
        runtime.library_scan_warning.lock().await.is_none(),
        "a stale failure warning must not overwrite a newer successful scan"
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = fs::remove_dir_all(root);
}

include!("cloud_state_http_tests.rs");

#[tokio::test]
async fn completed_telegram_action_retries_receipt_without_sending_twice() {
    let root = temp_cloud_bot_dir("telegram-result-persist-retry");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox_provider = Arc::new(FailOnceInboxFileProvider::default());
    let provider: Arc<dyn QueueFileProvider> = inbox_provider.clone();
    let inbox = Arc::new(
        open_runtime_test_inbox_with_file_provider(&config.downloads.video_dir, provider)
            .expect("cloud inbox should open with retryable test storage"),
    );
    let request = cloud_telegram_update_with_text(301, 301, "/help");
    inbox
        .persist(request.clone())
        .expect("Telegram request should persist");
    let processing = inbox
        .update(request.seq, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("Telegram request should enter processing");
    let receipt_path = runtime_test_request_path(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
        request.seq,
    );
    inbox_provider.fail_next_write_to(receipt_path.clone());

    let (telegram, mut telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let runtime = make_runtime(telegram, config.clone(), queue.clone(), inbox.clone());
    dispatch_cloud_record(runtime.clone(), processing).await;

    let completed = inbox
        .record(request.seq)
        .expect("Telegram receipt should load")
        .expect("Telegram receipt should remain durable");
    assert_eq!(completed.status, InboxStatus::Completed);
    let sends = std::iter::from_fn(|| telegram_requests.try_recv().ok())
        .filter(|request| request.method == "sendMessage")
        .count();
    assert_eq!(
        sends, 1,
        "the completed Telegram action must not be replayed"
    );
    assert_eq!(
        inbox_provider
            .write_attempts()
            .iter()
            .filter(|path| *path == &receipt_path)
            .count(),
        4,
        "the terminal receipt should retry the same write after one injected failure"
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn retryable_status_write_failure_preserves_error_and_retry_deadline() {
    let root = temp_cloud_bot_dir("retryable-status-persist-retry");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox_provider = Arc::new(FailOnceInboxFileProvider::default());
    let provider: Arc<dyn QueueFileProvider> = inbox_provider.clone();
    let inbox = Arc::new(
        open_runtime_test_inbox_with_file_provider(&config.downloads.video_dir, provider)
            .expect("cloud inbox should open with retryable test storage"),
    );
    let request = CloudRequest {
        seq: 302,
        kind: "file_confirm".to_string(),
        payload: serde_json::json!({"preview_id": "missing-preview", "revision": "revision-1"}),
        created_at: now_millis(),
        extra: serde_json::Map::new(),
    };
    inbox
        .persist(request.clone())
        .expect("confirmation request should persist");
    let processing = inbox
        .update(request.seq, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("confirmation should enter processing");
    let receipt_path = runtime_test_request_path(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
        request.seq,
    );
    inbox_provider.fail_next_read("simulated temporary inbox read failure");
    inbox_provider.fail_next_write_to(receipt_path.clone());

    let telegram = TelegramClient::with_test_api_base_url(
        TEST_TELEGRAM_TOKEN.to_string(),
        "http://127.0.0.1:1".to_string(),
    );
    let mut runtime = make_runtime(telegram, config.clone(), queue.clone(), inbox.clone());
    let library_provider = Arc::new(MockQueueFileProvider::default());
    let library_provider_trait: Arc<dyn QueueFileProvider> = library_provider.clone();
    runtime.test_file_provider = Some(library_provider_trait);
    let before = now_seconds();
    dispatch_cloud_record(runtime.clone(), processing).await;

    let retryable = inbox
        .record(request.seq)
        .expect("retryable confirmation should load")
        .expect("retryable confirmation should remain durable");
    assert_eq!(retryable.status, InboxStatus::Retryable);
    assert!(
        retryable
            .error
            .as_deref()
            .is_some_and(|error| error.contains("simulated temporary inbox read failure"))
    );
    assert!(retryable.next_attempt_at >= before.saturating_add(5));
    assert_eq!(
        inbox_provider
            .write_attempts()
            .iter()
            .filter(|path| *path == &receipt_path)
            .count(),
        4,
        "the Retryable state write should retry after one injected failure"
    );
    assert!(
        library_provider.read_paths().is_empty() && library_provider.move_paths().is_empty(),
        "the rejected inbox lookup must not reach local library execution"
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn completed_file_move_retries_primary_and_preview_receipts_without_repeating_move() {
    let root = temp_cloud_bot_dir("file-result-persist-retry");
    let (telegram, mut telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let source_path = config.downloads.video_dir.join("clip.mp4");
    fs::write(&source_path, b"fixture-video").expect("media fixture should write");

    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox_provider = Arc::new(FailOnceInboxFileProvider::default());
    let inbox_provider_trait: Arc<dyn QueueFileProvider> = inbox_provider.clone();
    let inbox = Arc::new(
        open_runtime_test_inbox_with_file_provider(
            &config.downloads.video_dir,
            inbox_provider_trait,
        )
        .expect("cloud inbox should open with retryable test storage"),
    );
    let library_provider = Arc::new(MockQueueFileProvider::default());
    let library_provider_trait: Arc<dyn QueueFileProvider> = library_provider.clone();
    let library = LibraryManager::open_with_provider_and_preview_cleanup_gate(
        &config.downloads.video_dir,
        Arc::clone(&library_provider_trait),
        inbox.clone(),
    )
    .expect("cloud library manager should open with mock file access");
    let snapshot = library
        .scan(&[])
        .expect("library scan should find the fixture");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("media fixture should appear in the snapshot")
        .id
        .clone();
    let preview = library
        .preview(
            std::slice::from_ref(&item_id),
            "organized",
            false,
            ConflictPolicy::KeepBoth,
        )
        .expect("file move should produce a persistent preview");
    let preview_id = preview.id.clone();
    let preview_revision = preview.revision.clone();
    let preview_seq = 303;
    let confirmation_seq = 304;
    inbox
        .persist(CloudRequest {
            seq: preview_seq,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({
                "item_ids": [item_id],
                "target_relative_dir": "organized"
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("preview receipt should persist");
    inbox
        .update(preview_seq, |record| {
            record.status = InboxStatus::AwaitingConfirmation;
            record.result = Some(serde_json::json!({
                "preview_id": preview_id,
                "revision": preview_revision,
                "telegram_status_delivery": "sent",
                "telegram_status_message_id": 9_999,
                "telegram_status_chat_id": configured_cloud_management_chat_id(&config)
                    .expect("test config should have one private owner chat"),
                "preview": preview,
            }));
        })
        .expect("preview should await confirmation");
    let confirm_request = CloudRequest {
        seq: confirmation_seq,
        kind: "file_confirm".to_string(),
        payload: serde_json::json!({
            "preview_id": preview_id,
            "revision": preview_revision,
        }),
        created_at: now_millis(),
        extra: serde_json::Map::new(),
    };
    inbox
        .persist(confirm_request.clone())
        .expect("confirmation request should persist");
    let processing = inbox
        .update(confirmation_seq, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("confirmation should enter processing");

    let primary_path = runtime_test_request_path(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
        confirmation_seq,
    );
    let preview_path = runtime_test_request_path(
        &config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
        preview_seq,
    );
    inbox_provider.fail_next_write_to(primary_path.clone());
    inbox_provider.fail_next_write_to(preview_path.clone());

    let mut runtime = make_runtime(telegram, config.clone(), queue.clone(), inbox.clone());
    runtime.test_file_provider = Some(library_provider_trait);
    dispatch_cloud_record(runtime.clone(), processing).await;

    let confirmation = inbox
        .record(confirmation_seq)
        .expect("confirmation receipt should load")
        .expect("confirmation receipt should remain durable");
    assert_eq!(
        confirmation.status,
        InboxStatus::Completed,
        "stored error: {:?}; moves: {:?}",
        confirmation.error,
        library_provider.move_paths()
    );
    assert_eq!(
        inbox
            .record(preview_seq)
            .expect("related preview receipt should load")
            .expect("related preview receipt should remain durable")
            .status,
        InboxStatus::Completed
    );
    assert_eq!(library_provider.move_paths().len(), 1);
    let telegram_methods = std::iter::from_fn(|| telegram_requests.try_recv().ok())
        .map(|request| request.method)
        .collect::<Vec<_>>();
    assert_eq!(
        telegram_methods
            .iter()
            .filter(|method| method.as_str() == "sendMessage")
            .count(),
        0,
        "an existing status message should prevent a new progress message"
    );
    assert_eq!(
        telegram_methods
            .iter()
            .filter(|method| method.as_str() == "editMessageText")
            .count(),
        2,
        "the existing status message should be edited before and after execution"
    );
    let writes = inbox_provider.write_attempts();
    for path in [&primary_path, &preview_path] {
        assert_eq!(
            writes.iter().filter(|written| *written == path).count(),
            4,
            "each result receipt should retry the same write once without repeating the file move"
        );
    }
    assert!(!source_path.exists());
    assert!(
        config
            .downloads
            .video_dir
            .join("organized/clip.mp4")
            .exists()
    );

    drop(runtime);
    drop(library);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cloud_preview_projection_and_confirmation_recheck_live_anchors() {
    let root = temp_cloud_bot_dir("cloud-preview-anchor-projection");
    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let source_path = config.downloads.video_dir.join("clip.mp4");
    fs::write(&source_path, b"fixture-video").expect("media fixture should write");

    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let provider = Arc::new(MockQueueFileProvider::default());
    let provider_trait: Arc<dyn QueueFileProvider> = provider.clone();
    let library = LibraryManager::open_with_provider_and_preview_cleanup_gate(
        &config.downloads.video_dir,
        Arc::clone(&provider_trait),
        inbox.clone(),
    )
    .expect("cloud library manager should open with mock file access");
    let snapshot = library
        .scan(&[])
        .expect("library scan should find the fixture");
    let item_id = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "clip.mp4")
        .expect("media fixture should appear in the snapshot")
        .id
        .clone();
    library.make_preview_creation_time_unavailable_for_test();
    let preview = library
        .preview(
            std::slice::from_ref(&item_id),
            "organized",
            false,
            ConflictPolicy::KeepBoth,
        )
        .expect("file move should produce a persistent preview");
    let preview_id = preview.id.clone();
    let preview_revision = preview.revision.clone();
    let preview_seq = 981;
    let confirmation_seq = 982;
    inbox
        .persist(CloudRequest {
            seq: preview_seq,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({
                "item_ids": [item_id],
                "target_relative_dir": "organized"
            }),
            created_at: now_millis(),
            extra: serde_json::Map::new(),
        })
        .expect("preview request should persist");
    inbox
        .update(preview_seq, |record| {
            record.status = InboxStatus::AwaitingConfirmation;
            record.result = Some(serde_json::json!({
                "preview_id": preview_id.clone(),
                "revision": preview_revision.clone(),
                "preview": preview,
            }));
        })
        .expect("preview should await confirmation");

    let mut runtime = make_runtime(telegram, config.clone(), queue.clone(), inbox.clone());
    *runtime.library_snapshot.lock().await = Some(snapshot);
    runtime.test_file_provider = Some(Arc::clone(&provider_trait));

    provider.fail_next_read("simulated transient preview receipt read failure");
    let transient_state = build_cloud_state_snapshot(&runtime)
        .await
        .expect("transient preview read failure should not fail state publication")
        .expect("state should be available");
    let transient_operation = transient_state
        .operations
        .iter()
        .find(|operation| operation["seq"] == preview_seq)
        .expect("preview operation should remain visible during a transient check failure");
    assert_eq!(transient_operation["status"], "awaiting_confirmation");
    assert_eq!(transient_state.library["incomplete"], true);
    assert!(
        transient_state.library["warnings"]
            .as_array()
            .is_some_and(|warnings| {
                warnings.iter().any(|warning| {
                    warning.as_str().is_some_and(|message| {
                        message.contains("could not verify whether a pending preview")
                    })
                })
            })
    );

    let confirmable_state = build_cloud_state_snapshot(&runtime)
        .await
        .expect("a later anchor check should recover")
        .expect("state should remain available");
    let confirmable_operation = confirmable_state
        .operations
        .iter()
        .find(|operation| operation["seq"] == preview_seq)
        .expect("preview operation should remain visible while its live anchor exists");
    assert_eq!(confirmable_operation["status"], "awaiting_confirmation");
    assert!(confirmable_state.previews.iter().any(|entry| {
        entry["preview_id"] == preview_id && entry["revision"] == preview_revision
    }));

    release_preview_anchors(&preview_id);
    let stale_state = build_cloud_state_snapshot(&runtime)
        .await
        .expect("a missing live anchor should be projected as stale")
        .expect("state should remain available");
    let stale_operation = stale_state
        .operations
        .iter()
        .find(|operation| operation["seq"] == preview_seq)
        .expect("stale preview operation should remain in history");
    assert_eq!(stale_operation["status"], "stale");
    assert!(
        stale_state
            .previews
            .iter()
            .all(|entry| entry["preview_id"] != preview_id)
    );

    let confirm_request = CloudRequest {
        seq: confirmation_seq,
        kind: "file_confirm".to_string(),
        payload: serde_json::json!({
            "preview_id": preview_id,
            "revision": preview_revision,
        }),
        created_at: now_millis(),
        extra: serde_json::Map::new(),
    };
    inbox
        .persist(confirm_request.clone())
        .expect("confirmation request should persist");
    let processing = inbox
        .update(confirmation_seq, |record| {
            record.status = InboxStatus::Processing;
            record.attempts = 1;
        })
        .expect("confirmation should enter processing");
    dispatch_cloud_record(runtime.clone(), processing).await;

    assert_eq!(
        inbox
            .record(confirmation_seq)
            .expect("confirmation receipt should load")
            .expect("confirmation receipt should remain durable")
            .status,
        InboxStatus::Failed,
        "stale anchor should reject confirmation before any operation starts"
    );
    assert_eq!(
        inbox
            .record(preview_seq)
            .expect("preview receipt should load")
            .expect("preview receipt should remain durable")
            .status,
        InboxStatus::Stale
    );
    assert!(provider.move_paths().is_empty());
    assert!(telegram_requests.try_recv().is_err());
    assert!(source_path.exists());

    drop(library);
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = std::fs::remove_dir_all(root);
}
#[path = "media_library_e2e_tests.rs"]
mod media_library_e2e_tests;

#[tokio::test]
async fn media_library_migration_uses_per_bundle_conflict_choices_mock_e2e() {
    let root = temp_cloud_bot_dir("migration-mixed-conflicts");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.ensure_runtime_dirs().unwrap();
    let archive = root.join("archive");
    fs::create_dir(&archive).unwrap();
    for name in ["one", "two"] {
        fs::write(
            config.downloads.video_dir.join(format!("{name}.mp4")),
            name.as_bytes(),
        )
        .unwrap();
        fs::write(
            config.downloads.video_dir.join(format!("{name}.nfo")),
            b"<movie/>",
        )
        .unwrap();
        fs::write(
            archive.join(format!("{name}.mp4")),
            b"existing target media",
        )
        .unwrap();
    }
    let queue = Arc::new(QueueManager::open(&config).unwrap());
    let source = queue.default_video_destination().unwrap();
    let target = queue
        .register_video_library("Archive", &archive, None, false, false)
        .unwrap();
    let inbox = Arc::new(open_runtime_test_inbox(&config.downloads.video_dir).unwrap());
    let runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let source_manager = LibraryManager::open(&config.downloads.video_dir).unwrap();
    let snapshot = source_manager.scan(&[]).unwrap();
    let one = snapshot
        .items
        .iter()
        .find(|item| item.relative_path == "one.mp4")
        .unwrap();
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let preview = run_library_management_test_request(
        &runtime,
        951,
        "library_migration_preview",
        serde_json::json!({
            "source_library_id": source.library_id, "target_library_id": target.id,
            "scope": "library", "conflict": "skip", "item_conflicts": { (one.id.clone()): "keep_both" },
        }),
    )
    .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let preview = preview.result.unwrap();
    let id = preview["id"].as_str().unwrap().to_string();
    let one_target = preview["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["source_path"] == "one.mp4")
        .unwrap()["target_path"]
        .as_str()
        .unwrap()
        .to_string();
    let confirmed = run_library_management_test_request(
        &runtime,
        952,
        "library_migration_confirm",
        serde_json::json!({"preview_id":id, "revision":preview["revision"]}),
    )
    .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    let status_result = tokio::time::timeout(MIGRATION_TEST_COMPLETION_TIMEOUT, async {
        loop {
            let status = runtime.migrations.lock().await.get(&id).cloned();
            let runner_active = runtime.migration_runners.lock().await.contains(&id);
            if let Some(status) = status
                && !runner_active
                && matches!(
                    status.status,
                    crate::library_migration::MigrationRunStatus::Completed
                        | crate::library_migration::MigrationRunStatus::Partial
                        | crate::library_migration::MigrationRunStatus::Failed
                        | crate::library_migration::MigrationRunStatus::Paused
                        | crate::library_migration::MigrationRunStatus::Stopped
                )
            {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let status = match status_result {
        Ok(status) => status,
        Err(_) => {
            let diagnostic = tokio::time::timeout(Duration::from_secs(1), async {
                let in_memory = runtime.migrations.lock().await.get(&id).cloned();
                let runner_active = runtime.migration_runners.lock().await.contains(&id);
                (in_memory, runner_active)
            })
            .await;
            match diagnostic {
                Ok((in_memory, runner_active)) => panic!(
                    "mixed conflict choices did not reach a terminal status: id={id}, runner_active={runner_active}, in_memory={in_memory:?}"
                ),
                Err(_) => panic!(
                    "mixed conflict choices timed out and migration-state locks remained unavailable: id={id}"
                ),
            }
        }
    };
    assert_eq!(
        status.status,
        crate::library_migration::MigrationRunStatus::Completed,
        "mixed conflict migration should complete: {status:?}"
    );
    assert_eq!((status.success_count, status.skipped_count), (1, 1));
    assert!(!config.downloads.video_dir.join("one.mp4").exists());
    assert!(!config.downloads.video_dir.join("one.nfo").exists());
    assert_eq!(fs::read(archive.join(&one_target)).unwrap(), b"one");
    assert!(archive.join(&one_target).with_extension("nfo").exists());
    assert_eq!(
        fs::read(archive.join("one.mp4")).unwrap(),
        b"existing target media"
    );
    assert!(config.downloads.video_dir.join("two.mp4").exists());
    assert!(config.downloads.video_dir.join("two.nfo").exists());
    let mut sends = 0;
    while let Ok(request) = requests.try_recv() {
        sends += usize::from(request.method == "sendMessage");
    }
    assert_eq!(sends, 1);
    runtime.signal_epoch_fatal();
    dispatcher.await.unwrap();
    stop_fake_telegram_api(shutdown, server).await;
    drop(source_manager);
    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn unavailable_saved_category_fails_without_losing_fragments_mock_e2e() {
    let root = temp_cloud_bot_dir("saved-category-replaced");
    let (telegram, mut requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.ensure_runtime_dirs().unwrap();
    let category = config.downloads.video_dir.join("Category");
    fs::create_dir(&category).unwrap();
    let outside = root.join("outside");
    fs::create_dir(&outside).unwrap();
    let queue = Arc::new(QueueManager::open(&config).unwrap());
    let library_id = queue.default_video_destination().unwrap().library_id;
    queue
        .set_video_library_category(&library_id, Some(Path::new("Category")))
        .unwrap();
    let retained =
        downloader::create_test_retained_video_staging(&config.downloads.video_dir).unwrap();
    fs::write(retained.join("segment.part"), b"retained before failure").unwrap();
    let job = JobRequest::Youtube {
        url: "https://www.youtube.com/watch?v=abcdefghijk".to_string(),
    };
    let mut task = TaskRecord::new(
        "saved-category-replaced".to_string(),
        1,
        1,
        123_456_789,
        Some(123_456_789),
        0,
        job.clone(),
    );
    task.staging_attempts.push(retained.clone());
    queue.create(task).unwrap();
    fs::remove_dir(&category).unwrap();
    std::os::unix::fs::symlink(&outside, &category).unwrap();
    let inbox = Arc::new(open_runtime_test_inbox(&config.downloads.video_dir).unwrap());
    let runtime = make_runtime(telegram, config, Arc::clone(&queue), inbox);
    queue_or_prompt_job(
        runtime.bot.clone(),
        123_456_789,
        1,
        "saved-category-replaced".to_string(),
        0,
        job,
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if queue
                .get("saved-category-replaced")
                .unwrap()
                .unwrap()
                .status
                == TaskStatus::Failed
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("destination failure should leave a retryable failed task");
    let request = tokio::time::timeout(Duration::from_secs(5), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        request.body["text"]
            .as_str()
            .is_some_and(|text| text.contains("could not open its saved destination"))
    );
    assert_eq!(
        fs::read(retained.join("segment.part")).unwrap(),
        b"retained before failure"
    );
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
    stop_fake_telegram_api(shutdown, server).await;
    drop(runtime);
    drop(queue);
    let _ = fs::remove_dir_all(root);
}
