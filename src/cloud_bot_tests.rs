use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
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
    Confidence, IntegrityStatus, LibraryItem, LibrarySnapshot, MetadataStatus, Presence,
};

use super::*;

// Synthetic catalog joey-private-v3: api-key-a (active API key).
const TEST_TELEGRAM_TOKEN: &str = "codex_synth_v1_api_key_a";

struct FakeTelegramRequest {
    method: String,
    body: serde_json::Value,
    authorization: Option<String>,
}

struct FakeCloudApiState {
    pending: StdMutex<Vec<CloudRequest>>,
    fail_first_ack: AtomicBool,
    durable_before_ack: AtomicBool,
    inbox_root: PathBuf,
    state_posts: StdMutex<Vec<serde_json::Value>>,
    state_post_statuses: StdMutex<Vec<u16>>,
    state_post_response_headers: StdMutex<Vec<Vec<(String, String)>>>,
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

async fn read_fake_telegram_request(stream: &mut TcpStream) -> Result<FakeTelegramRequest> {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 4_096];
        let read = stream
            .read(&mut chunk)
            .await
            .context("fake Telegram API request read failed")?;
        if read == 0 {
            anyhow::bail!("fake Telegram API request ended before headers");
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() > 64 * 1024 {
            anyhow::bail!("fake Telegram API request headers were too large");
        }
    };
    let headers = std::str::from_utf8(&bytes[..header_end])
        .context("fake Telegram API request headers were not UTF-8")?;
    let request_line = headers
        .lines()
        .next()
        .context("fake Telegram API request line was missing")?;
    let request_path = request_line
        .split_whitespace()
        .nth(1)
        .context("fake Telegram API request path was missing")?;
    let method = request_path
        .split('?')
        .next()
        .context("fake API request target was missing")?
        .rsplit('/')
        .next()
        .filter(|method| !method.is_empty())
        .context("fake Telegram API method was missing")?
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
        .context("fake Telegram API content length was invalid")?;
    let authorization = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("authorization")
            .then_some(value.trim().to_string())
    });
    let body_end = header_end.saturating_add(content_length);
    while bytes.len() < body_end {
        let mut chunk = [0_u8; 4_096];
        let read = stream
            .read(&mut chunk)
            .await
            .context("fake Telegram API request body read failed")?;
        if read == 0 {
            anyhow::bail!("fake Telegram API request ended before body");
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    let body = if content_length == 0 {
        serde_json::json!({})
    } else {
        serde_json::from_slice(&bytes[header_end..body_end])
            .context("fake Telegram API request body was not JSON")?
    };
    Ok(FakeTelegramRequest {
        method,
        body,
        authorization,
    })
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
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake cloud API should bind localhost");
    let address = listener
        .local_addr()
        .expect("fake cloud API should expose its address");
    let state = Arc::new(FakeCloudApiState {
        pending: StdMutex::new(pending),
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
                    let request = read_fake_telegram_request(&mut stream).await?;
                    let expected_authorization = format!("Bearer {TEST_CLOUD_SECRET}");
                    if request.authorization.as_deref() != Some(expected_authorization.as_str()) {
                        stream.write_all(fake_http_response(401, &serde_json::json!({"error":"unauthorized"})).as_bytes()).await?;
                        continue;
                    }
                    let (status, payload) = match request.method.as_str() {
                        "requests" => {
                            let pending = task_state.pending.lock().expect("fake cloud state lock should not poison").clone();
                            (200, serde_json::json!({"requests": pending}))
                        }
                        "ack" => {
                            let seqs = request.body["seqs"]
                                .as_array()
                                .context("ACK body should contain sequence list")?
                                .iter()
                                .map(|value| value.as_u64().context("ACK sequence should be unsigned"))
                                .collect::<Result<Vec<_>>>()?;
                            let durable = seqs.iter().all(|seq| {
                                let path = task_state.inbox_root
                                    .join(".telegram-video-downloader-cloud")
                                    .join(format!("request-{seq:020}.json"));
                                std::fs::read(path)
                                    .ok()
                                    .and_then(|bytes| serde_json::from_slice::<InboxRecord>(&bytes).ok())
                                    .is_some_and(|record| record.request.seq == *seq)
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
                                pending.retain(|request| !seqs.contains(&request.seq));
                                (200, serde_json::json!({"ok":true}))
                            }
                        }
                        "state" => {
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
                                "library_warnings": library["warnings"],
                                "library_item_count": library["items"].as_array().map_or(0, Vec::len),
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
                        _ => (404, serde_json::json!({"error":"not found"})),
                    };
                    let response_headers = if request.method == "state" {
                        let mut headers = task_state.state_post_response_headers
                            .lock()
                            .expect("fake state response header lock should not poison");
                        if headers.is_empty() { Vec::new() } else { headers.remove(0) }
                    } else {
                        Vec::new()
                    };
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
            serde_json::json!({"requests": &backlog})
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
            let seqs = ack.body["seqs"]
                .as_array()
                .context("ACK body should contain sequence list")?
                .iter()
                .map(|value| value.as_u64().context("ACK sequence should be unsigned"))
                .collect::<Result<Vec<_>>>()?;
            anyhow::ensure!(
                seqs == expected_ack_seqs,
                "ACK must cover the authoritative REST page before accepting the WebSocket frame"
            );
            let durable = seqs.iter().all(|seq| {
                let path = inbox_root
                    .join(".telegram-video-downloader-cloud")
                    .join(format!("request-{seq:020}.json"));
                std::fs::read(path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<InboxRecord>(&bytes).ok())
                    .is_some_and(|record| record.request.seq == *seq)
            });
            anyhow::ensure!(durable, "every request must be durable before its page ACK");
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
        library_scan_warning: Arc::new(Mutex::new(None)),
        library_scan_publication_gate: Arc::new(Mutex::new(())),
        telegram_dispatch: Arc::new(Semaphore::new(1)),
        management_dispatch: Arc::new(Semaphore::new(1)),
        backlog_sync_gate: Arc::new(Mutex::new(())),
        reconciliation_complete: Arc::new(AtomicBool::new(true)),
        test_file_provider: None,
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

    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
        CloudInbox::open(&config.downloads.video_dir).expect("terminal inbox should reopen"),
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
    inbox
        .persist(CloudRequest {
            seq: 84,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": "retryable-preview",
                "revision": "revision-retryable"
            }),
            created_at: 101,
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
async fn retryable_file_provider_confirmation_can_resume_the_same_preview() {
    let root = temp_cloud_bot_dir("retryable-provider-confirmation");
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

    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
        inbox
            .persist(CloudRequest {
                seq,
                kind: "file_confirm".to_string(),
                payload: serde_json::json!({
                    "preview_id": preview_id.clone(),
                    "revision": preview_revision.clone(),
                }),
                created_at: 100 + seq,
                extra: serde_json::Map::new(),
            })
            .expect("confirmation request should persist");
        let record = inbox
            .update(seq, |record| {
                record.status = InboxStatus::Processing;
                record.attempts = 1;
            })
            .expect("confirmation should enter processing");
        dispatch_cloud_record(runtime.clone(), record).await;

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

fn cloud_telegram_update(seq: u64, update_id: i64) -> CloudRequest {
    CloudRequest {
        seq,
        kind: "telegram".to_string(),
        payload: serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": update_id,
                "chat": {"id": 123_456_789, "type": "private"},
                "from": {"id": 123_456_789},
                "text": "/queue"
            }
        }),
        created_at: 1_790_000_000_000,
        extra: serde_json::Map::new(),
    }
}

fn cloud_telegram_update_with_text(seq: u64, update_id: i64, text: &str) -> CloudRequest {
    let mut request = cloud_telegram_update(seq, update_id);
    request.payload["message"]["text"] = serde_json::Value::String(text.to_string());
    request
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
        CloudInbox::open(&config.downloads.video_dir)
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
async fn completed_queue_publication_refreshes_the_cloud_library_snapshot() {
    let root = temp_cloud_bot_dir("publication-refresh");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test download roots should create");
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
    let queue = Arc::new(QueueManager::open(&config).expect("task queue should open"));
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
    let preview = MovePreview {
        id: "missing-persisted-preview".to_string(),
        revision: "revision-a".to_string(),
        target_relative_dir: Some(String::new()),
        rename: false,
        conflict: ConflictPolicy::Skip,
        items: Vec::new(),
        metadata_patches: Vec::new(),
    };
    let preview_id = preview.id.clone();
    let preview_revision = preview.revision.clone();
    inbox
        .persist(CloudRequest {
            seq: 41,
            kind: "file_preview".to_string(),
            payload: serde_json::json!({"item_ids": ["item-a"], "target_relative_dir": ""}),
            created_at: 100,
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
    inbox
        .persist(CloudRequest {
            seq: 42,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": "missing-persisted-preview",
                "revision": "revision-a"
            }),
            created_at: 101,
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
    assert!(
        recorded[1].body["text"]
            .as_str()
            .is_some_and(|text| text.contains("failed before it could complete"))
    );

    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
        let backlog_payload = serde_json::json!({"requests": [&request]});
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
        let record_path = server_root
            .join(".telegram-video-downloader-cloud")
            .join(format!("request-{:020}.json", request.seq));
        let durable = std::fs::read(record_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<InboxRecord>(&bytes).ok())
            .is_some_and(|record| record.request == request);
        let valid_ack = ack.method == "ack"
            && ack.authorization.as_deref() == Some(expected_authorization.as_str())
            && ack.body["seqs"] == serde_json::json!([request.seq]);
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
    let inbox =
        Arc::new(CloudInbox::open(&config.downloads.video_dir).expect("cloud inbox should open"));
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
        run_cloud_library_operation(&older_runtime, "older fixture scan failed", move |_| {
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
        run_cloud_library_operation(&newer_runtime, "newer fixture scan failed", move |_| {
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
        run_cloud_library_operation::<()>(&failing_runtime, "older failure fixture", move |_| {
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
            move |_| Ok(((), scan_order_test_snapshot("recovered"))),
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
