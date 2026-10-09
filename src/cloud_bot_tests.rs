use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cloud::TEST_CLOUD_SECRET;
use anyhow::{Context, Result};
use futures_util::SinkExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;

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
                        _ => (404, serde_json::json!({"error":"not found"})),
                    };
                    stream.write_all(fake_http_response(status, &payload).as_bytes()).await
                        .context("fake cloud API response write failed")?;
                }
            }
        }
    });
    (format!("http://{address}/"), state, shutdown_tx, server)
}

fn fake_http_response(status: u16, payload: &serde_json::Value) -> String {
    let payload = payload.to_string();
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        503 => "Service Unavailable",
        _ => "Error",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
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
        telegram_dispatch: Arc::new(Semaphore::new(1)),
        management_dispatch: Arc::new(Semaphore::new(1)),
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

#[test]
fn local_transient_retries_back_off_to_one_minute() {
    assert_eq!(cloud_retry_delay_seconds(1), 5);
    assert_eq!(cloud_retry_delay_seconds(2), 10);
    assert_eq!(cloud_retry_delay_seconds(3), 20);
    assert_eq!(cloud_retry_delay_seconds(4), 40);
    assert_eq!(cloud_retry_delay_seconds(5), 60);
    assert_eq!(cloud_retry_delay_seconds(u32::MAX), 60);
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
    assert_eq!(first_records.len(), 1);
    assert_eq!(first_records[0].request.seq, 2);
    assert_eq!(first_records[0].status, InboxStatus::Pending);
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
    let runtime = make_runtime(telegram, config, queue, Arc::clone(&inbox));
    let outcome = dispatch_cloud_management(
        &runtime,
        &CloudRequest {
            seq: 42,
            kind: "file_confirm".to_string(),
            payload: serde_json::json!({
                "preview_id": "missing-persisted-preview",
                "revision": "revision-a"
            }),
            created_at: 101,
            extra: serde_json::Map::new(),
        },
    )
    .await
    .expect("permanent confirmation errors should become a visible failed outcome");
    assert_eq!(outcome.status, InboxStatus::Failed);
    assert!(
        outcome
            .error
            .as_deref()
            .is_some_and(|error| error.contains("preview")),
        "failed operation result should explain that confirmation could not execute"
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
    assert_eq!(recorded[1].body["message_id"].as_i64(), Some(1_000));
    assert!(
        recorded[1].body["text"]
            .as_str()
            .is_some_and(|text| text.contains("failed before it could complete"))
    );

    drop(runtime);
    drop(inbox);
    stop_fake_telegram_api(shutdown, server).await;
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

        let (mut ack_stream, _) = listener
            .accept()
            .await
            .context("fake cloud ACK accept failed")?;
        let ack = read_fake_telegram_request(&mut ack_stream).await?;
        let expected_authorization = format!("Bearer {TEST_CLOUD_SECRET}");
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
