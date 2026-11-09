use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{WebSocketStream, accept_async};

use super::*;

const TEST_LIVENESS_POLICY: CloudWebSocketLivenessPolicy = CloudWebSocketLivenessPolicy {
    idle_ping_interval: Duration::from_millis(20),
    pong_timeout: Duration::from_millis(40),
};
const TEST_E2E_LIVENESS_POLICY: CloudWebSocketLivenessPolicy = CloudWebSocketLivenessPolicy {
    idle_ping_interval: Duration::from_millis(20),
    pong_timeout: Duration::from_millis(500),
};

async fn test_runtime(
    label: &str,
) -> (
    PathBuf,
    CloudBotRuntime,
    mpsc::UnboundedReceiver<FakeTelegramRequest>,
    oneshot::Sender<()>,
    JoinHandle<Result<()>>,
) {
    let root = temp_cloud_bot_dir(label);
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
    let (telegram, telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let runtime = make_runtime(telegram, config, queue, inbox);
    (
        root,
        runtime,
        telegram_requests,
        telegram_shutdown,
        telegram_server,
    )
}

async fn accept_local_websocket(listener: &TcpListener) -> Result<WebSocketStream<TcpStream>> {
    let (stream, _) = listener
        .accept()
        .await
        .context("local WebSocket client should connect")?;
    accept_async(stream)
        .await
        .context("local WebSocket handshake should complete")
}

async fn wait_for_client_ping(socket: &mut WebSocketStream<TcpStream>) -> Result<Vec<u8>> {
    loop {
        match socket.next().await {
            Some(Ok(WsMessage::Ping(payload))) => return Ok(payload.to_vec()),
            Some(Ok(WsMessage::Close(_))) | None => {
                anyhow::bail!("client disconnected before sending its liveness ping");
            }
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error).context("WebSocket read failed"),
        }
    }
}

async fn wait_for_client_disconnect(socket: &mut WebSocketStream<TcpStream>) -> Result<()> {
    loop {
        match socket.next().await {
            Some(Ok(WsMessage::Close(_))) | None => return Ok(()),
            Some(Err(error)) if is_expected_websocket_reset(&error) => return Ok(()),
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error).context("fake WebSocket read failed"),
        }
    }
}

fn is_expected_websocket_reset(error: &tokio_tungstenite::tungstenite::Error) -> bool {
    match error {
        tokio_tungstenite::tungstenite::Error::Protocol(
            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => true,
        tokio_tungstenite::tungstenite::Error::Io(error) => {
            error.kind() == ErrorKind::ConnectionReset
        }
        _ => false,
    }
}

fn set_test_cloud_client(runtime: &mut CloudBotRuntime, listener: &TcpListener) {
    let address = listener
        .local_addr()
        .expect("fake Cloudflare listener should expose its address");
    runtime.client = CloudClient::with_test_base_url(&format!("http://{address}/"))
        .expect("test cloud client should use the local WebSocket API");
}

fn viewer_interest_control(watch: bool) -> String {
    json!({
        "type": "viewer_interest",
        "database_epoch": crate::cloud::TEST_DATABASE_EPOCH,
        "viewers": [{
            "viewer_id": "22222222-2222-4222-8222-222222222222",
            "surface": "file",
            "watch": watch,
            "expires_at": crate::cloud::now_millis() + 60 * 60 * 1000
        }]
    })
    .to_string()
}

#[tokio::test]
async fn missing_websocket_pong_ends_the_idle_session() {
    let listener = Arc::new(
        TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake WebSocket should bind localhost"),
    );
    let (connected_tx, connected_rx) = oneshot::channel();
    let (release_server_tx, release_server_rx) = oneshot::channel();
    let server_listener = Arc::clone(&listener);
    let server = tokio::spawn(async move {
        let socket = accept_local_websocket(&server_listener).await?;
        let _ = connected_tx.send(());
        release_server_rx
            .await
            .context("test should release the silent WebSocket peer")?;
        drop(socket);
        Ok::<_, anyhow::Error>(())
    });
    let (root, mut runtime, _, telegram_shutdown, telegram_server) =
        test_runtime("liveness-missing-pong").await;
    set_test_cloud_client(&mut runtime, &listener);
    let mut socket = runtime
        .client
        .connect_websocket()
        .await
        .expect("test client should connect to fake WebSocket");
    let session_runtime = runtime.clone();
    let session = tokio::spawn(async move {
        receive_cloud_websocket_session_with_liveness_policy(
            &session_runtime,
            &mut socket,
            None,
            TEST_LIVENESS_POLICY,
        )
        .await
    });

    timeout(Duration::from_secs(5), connected_rx)
        .await
        .expect("silent peer should accept the WebSocket")
        .expect("silent peer should report its accepted connection");
    let result = timeout(Duration::from_secs(5), session)
        .await
        .expect("session should fail within its bounded liveness deadline")
        .expect("session task should not panic");
    let error = result.expect_err("a missing Pong should expire the WebSocket session");
    assert!(
        format!("{error:#}").contains("without a matching Pong"),
        "unexpected liveness error: {error:#}"
    );
    let _ = release_server_tx.send(());
    server
        .await
        .expect("fake WebSocket task should not panic")
        .expect("fake WebSocket task should complete");
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    drop(runtime);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn unrelated_pong_does_not_satisfy_the_websocket_probe() {
    let listener = Arc::new(
        TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake WebSocket should bind localhost"),
    );
    let server_listener = Arc::clone(&listener);
    let server = tokio::spawn(async move {
        let mut socket = accept_local_websocket(&server_listener).await?;
        let nonce = wait_for_client_ping(&mut socket).await?;
        socket
            .send(WsMessage::Pong(b"wrong-nonce".to_vec().into()))
            .await
            .context("fake server should send an unrelated Pong")?;
        wait_for_client_disconnect(&mut socket).await?;
        Ok::<_, anyhow::Error>(nonce)
    });
    let (root, mut runtime, _, telegram_shutdown, telegram_server) =
        test_runtime("liveness-mismatched-pong").await;
    set_test_cloud_client(&mut runtime, &listener);
    let mut socket = runtime
        .client
        .connect_websocket()
        .await
        .expect("test client should connect to fake WebSocket");
    let session_runtime = runtime.clone();
    let session = tokio::spawn(async move {
        receive_cloud_websocket_session_with_liveness_policy(
            &session_runtime,
            &mut socket,
            None,
            TEST_LIVENESS_POLICY,
        )
        .await
    });

    let result = timeout(Duration::from_secs(5), session)
        .await
        .expect("session should fail after an unrelated Pong")
        .expect("session task should not panic");
    let error = result.expect_err("an unrelated Pong must not keep the session alive");
    assert!(
        format!("{error:#}").contains("without a matching Pong"),
        "unexpected liveness error: {error:#}"
    );
    let nonce = server
        .await
        .expect("fake WebSocket task should not panic")
        .expect("fake WebSocket should observe the client disconnect");
    assert_ne!(nonce.as_slice(), b"wrong-nonce");
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    drop(runtime);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn healthy_idle_pongs_preserve_controls_and_do_not_poll_rest() {
    let listener = Arc::new(
        TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake WebSocket should bind localhost"),
    );
    let (ping_tx, mut ping_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (stop_server_tx, mut stop_server_rx) = oneshot::channel();
    let server_listener = Arc::clone(&listener);
    let server = tokio::spawn(async move {
        let mut socket = accept_local_websocket(&server_listener).await?;
        let viewer_id = "22222222-2222-4222-8222-222222222222";
        let mut watch = false;
        socket
            .send(WsMessage::Text(
                json!({
                    "type": "viewer_interest",
                    "database_epoch": crate::cloud::TEST_DATABASE_EPOCH,
                    "viewers": [{
                        "viewer_id": viewer_id,
                        "surface": "file",
                        "watch": watch,
                        "expires_at": crate::cloud::now_millis() + 60 * 60 * 1000
                    }]
                })
                .to_string()
                .into(),
            ))
            .await
            .context("viewer-interest control should send")?;
        socket
            .send(WsMessage::Text(
                json!({
                    "type": "snapshot_refresh",
                    "viewer_id": viewer_id,
                    "request_id": "33333333-3333-4333-8333-333333333333",
                    "database_epoch": crate::cloud::TEST_DATABASE_EPOCH,
                    "reason": "manual",
                    "scope": "operations",
                    "expires_at": crate::cloud::now_millis()
                        + crate::cloud::MAX_SNAPSHOT_REFRESH_LIFETIME_MILLIS
                })
                .to_string()
                .into(),
            ))
            .await
            .context("snapshot-refresh control should send")?;

        loop {
            tokio::select! {
                _ = &mut stop_server_rx => {
                    let _ = socket.close(None).await;
                    return Ok(());
                }
                message = socket.next() => {
                    match message {
                        Some(Ok(WsMessage::Ping(payload))) => {
                            let nonce = payload.to_vec();
                            socket.send(WsMessage::Pong(payload)).await
                                .context("fake Cloudflare runtime should answer Ping")?;
                            ping_tx.send(nonce).expect("test should receive ping nonce");
                            watch = !watch;
                            socket.send(WsMessage::Text(json!({
                                "type": "viewer_interest",
                                "database_epoch": crate::cloud::TEST_DATABASE_EPOCH,
                                "viewers": [{
                                    "viewer_id": viewer_id,
                                    "surface": "file",
                                    "watch": watch,
                                    "expires_at": crate::cloud::now_millis() + 60 * 60 * 1000
                                }]
                            }).to_string().into())).await
                                .context("updated viewer-interest control should send")?;
                        }
                        Some(Ok(WsMessage::Close(_))) | None => return Ok(()),
                        Some(Err(error)) if is_expected_websocket_reset(&error) => return Ok(()),
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(error).context("fake WebSocket read failed"),
                    }
                }
            }
        }
    });

    let (root, mut runtime, _, telegram_shutdown, telegram_server) =
        test_runtime("liveness-healthy-idle").await;
    set_test_cloud_client(&mut runtime, &listener);
    *runtime.last_backlog_reconciled_at.lock().await = Some(Instant::now());
    let fallback = tokio::spawn(cloud_rest_fallback(runtime.clone(), 5));
    let mut socket = runtime
        .client
        .connect_websocket()
        .await
        .expect("test client should connect to fake WebSocket");
    let waiting = Arc::new(Notify::new());
    let session_waiting = Arc::clone(&waiting);
    let session_runtime = runtime.clone();
    let session = tokio::spawn(async move {
        receive_cloud_websocket_session_with_liveness_policy(
            &session_runtime,
            &mut socket,
            Some(session_waiting),
            CloudWebSocketLivenessPolicy {
                idle_ping_interval: Duration::from_secs(1),
                pong_timeout: Duration::from_secs(20),
            },
        )
        .await
    });
    waiting.notified().await;
    timeout(Duration::from_secs(5), async {
        loop {
            let visible = runtime
                .viewer_interests
                .lock()
                .await
                .contains_key("22222222-2222-4222-8222-222222222222");
            let refresh_pending = runtime.snapshot_refreshes.lock().await.pending.len() == 1;
            if visible && refresh_pending {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("existing viewer and refresh controls should still be processed");
    timeout(Duration::from_secs(5), waiting.notified())
        .await
        .expect("session should enter its idle wait after handling both controls");

    tokio::time::pause();
    for expected_watch in [true, false, true] {
        tokio::time::advance(Duration::from_secs(1)).await;
        let nonce = timeout(Duration::from_secs(5), ping_rx.recv())
            .await
            .expect("healthy server should receive each idle probe")
            .expect("fake server should remain connected");
        assert_eq!(nonce.len(), 8, "probe should contain its exact nonce");
        timeout(Duration::from_secs(5), async {
            loop {
                let watch = runtime
                    .viewer_interests
                    .lock()
                    .await
                    .get("22222222-2222-4222-8222-222222222222")
                    .is_some_and(|interest| interest.watch);
                if watch == expected_watch {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("viewer-interest control should be processed after Pong");
    }

    assert_eq!(runtime.snapshot_refreshes.lock().await.pending.len(), 1);
    assert_eq!(
        runtime.snapshot_refreshes.lock().await.pending[0].request_id,
        "33333333-3333-4333-8333-333333333333"
    );
    tokio::select! {
        biased;
        incoming = listener.accept() => {
            let (stream, _) = incoming.expect("listener should remain available");
            drop(stream);
            panic!("healthy idle WebSocket should not trigger REST polling");
        }
        _ = tokio::task::yield_now() => {}
    }

    runtime.signal_epoch_fatal();
    let session_result = timeout(Duration::from_secs(5), session)
        .await
        .expect("epoch change should stop the idle WebSocket session")
        .expect("WebSocket session task should not panic");
    assert!(session_result.is_ok());
    assert!(runtime.snapshot_refreshes.lock().await.pending.is_empty());
    fallback.abort();
    let _ = fallback.await;
    let _ = stop_server_tx.send(());
    server
        .await
        .expect("fake WebSocket task should not panic")
        .expect("fake WebSocket should stop cleanly");
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    drop(runtime);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn slow_inline_control_handler_does_not_consume_pong_deadline() {
    let listener = Arc::new(
        TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake WebSocket should bind localhost"),
    );
    let (ping_tx, mut ping_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (send_control_tx, send_control_rx) = oneshot::channel();
    let (control_sent_tx, control_sent_rx) = oneshot::channel();
    let (release_pong_tx, release_pong_rx) = oneshot::channel();
    let (pong_sent_tx, pong_sent_rx) = oneshot::channel();
    let (stop_server_tx, mut stop_server_rx) = oneshot::channel();
    let server_listener = Arc::clone(&listener);
    let server = tokio::spawn(async move {
        let mut socket = accept_local_websocket(&server_listener).await?;
        let nonce = wait_for_client_ping(&mut socket).await?;
        ping_tx
            .send(nonce.clone())
            .expect("test should observe the first client Ping");
        send_control_rx
            .await
            .context("test should release viewer-interest control")?;
        socket
            .send(WsMessage::Text(viewer_interest_control(false).into()))
            .await
            .context("slow viewer-interest control should send")?;
        let _ = control_sent_tx.send(());
        release_pong_rx
            .await
            .context("test should release the matching Pong")?;
        socket
            .send(WsMessage::Pong(nonce.into()))
            .await
            .context("fake peer should return the matching Pong")?;
        let _ = pong_sent_tx.send(());

        loop {
            tokio::select! {
                _ = &mut stop_server_rx => {
                    let _ = socket.close(None).await;
                    return Ok(());
                }
                message = socket.next() => {
                    match message {
                        Some(Ok(WsMessage::Ping(payload))) => {
                            ping_tx.send(payload.to_vec()).expect("test should observe later Ping");
                            socket.send(WsMessage::Pong(payload)).await
                                .context("fake peer should answer later Ping")?;
                        }
                        Some(Ok(WsMessage::Close(_))) | None => return Ok(()),
                        Some(Err(error)) if is_expected_websocket_reset(&error) => return Ok(()),
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(error).context("fake WebSocket read failed"),
                    }
                }
            }
        }
    });

    let (root, mut runtime, _, telegram_shutdown, telegram_server) =
        test_runtime("liveness-slow-handler").await;
    set_test_cloud_client(&mut runtime, &listener);
    let mut socket = runtime
        .client
        .connect_websocket()
        .await
        .expect("test client should connect to fake WebSocket");
    let waiting = Arc::new(Notify::new());
    let handler_started = Arc::new(Notify::new());
    let session_waiting = Arc::clone(&waiting);
    let session_handler_started = Arc::clone(&handler_started);
    let session_runtime = runtime.clone();
    let session = tokio::spawn(async move {
        receive_cloud_websocket_session_observing_handler(
            &session_runtime,
            &mut socket,
            Some(session_waiting),
            CloudWebSocketLivenessPolicy {
                idle_ping_interval: Duration::from_secs(1),
                pong_timeout: Duration::from_secs(20),
            },
            Some(session_handler_started),
        )
        .await
    });
    waiting.notified().await;

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();
    let first_nonce = timeout(Duration::from_secs(5), ping_rx.recv())
        .await
        .expect("first idle probe should be sent")
        .expect("fake peer should observe first probe");
    assert_eq!(first_nonce.len(), 8);
    waiting.notified().await;

    let viewer_guard = runtime.viewer_interests.lock().await;
    send_control_tx
        .send(())
        .expect("fake peer should send the inline control frame");
    control_sent_rx
        .await
        .expect("fake peer should write the control frame");
    handler_started.notified().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(25)).await;
    drop(viewer_guard);
    tokio::time::resume();

    waiting.notified().await;
    release_pong_tx
        .send(())
        .expect("fake peer should return the outstanding Pong");
    pong_sent_rx
        .await
        .expect("fake peer should write the matching Pong");
    // The next loop notification proves the reader consumed the Pong before the deadline check.
    waiting.notified().await;

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();
    let later_nonce = timeout(Duration::from_secs(5), ping_rx.recv())
        .await
        .expect("session should remain connected after slow inline work")
        .expect("fake peer should observe a later probe");
    assert_eq!(later_nonce.len(), 8);

    runtime.signal_epoch_fatal();
    let result = timeout(Duration::from_secs(5), session)
        .await
        .expect("epoch signal should stop the session")
        .expect("session task should not panic");
    assert!(result.is_ok());
    let _ = stop_server_tx.send(());
    server
        .await
        .expect("fake WebSocket task should not panic")
        .expect("fake WebSocket task should complete");
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    drop(runtime);
    let _ = std::fs::remove_dir_all(root);
}

async fn receive_rest_request(listener: &TcpListener) -> Result<(TcpStream, FakeTelegramRequest)> {
    let (mut stream, _) = listener
        .accept()
        .await
        .context("local cloud REST request should connect")?;
    let request = read_fake_telegram_request(&mut stream).await?;
    Ok((stream, request))
}

async fn serve_request_page(
    listener: &TcpListener,
    requests: &[CloudRequest],
) -> Result<FakeTelegramRequest> {
    let (mut stream, request) = receive_rest_request(listener).await?;
    anyhow::ensure!(
        request.method == "requests",
        "receiver should fetch the authoritative REST request page"
    );
    let payload = json!({
        "database_epoch": crate::cloud::TEST_DATABASE_EPOCH,
        "requests": requests
    });
    stream
        .write_all(fake_http_response(200, &payload).as_bytes())
        .await
        .context("fake request-page response should write")?;
    Ok(request)
}

#[tokio::test]
async fn expired_idle_websocket_reconnects_and_acks_before_telegram_dispatch() {
    let (root, mut runtime, mut telegram_requests, telegram_shutdown, telegram_server) =
        test_runtime("liveness-reconnect-e2e").await;
    let server_root = runtime.bot.config.downloads.video_dir.clone();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake cloud API should bind localhost");
    let address = listener
        .local_addr()
        .expect("fake cloud API should expose its address");
    let (first_connection_tx, first_connection_rx) = oneshot::channel();
    let (ack_verified_tx, ack_verified_rx) = oneshot::channel::<(bool, bool)>();
    let (second_connection_tx, second_connection_rx) = oneshot::channel::<()>();
    let (stop_server_tx, mut stop_server_rx) = oneshot::channel();
    let request = cloud_telegram_update_with_text(25, 25, "/help");
    let request_seq = request.seq;
    let server = tokio::spawn(async move {
        let (mut initial_fetch, initial_request) = receive_rest_request(&listener).await?;
        anyhow::ensure!(initial_request.method == "requests");
        initial_fetch
            .write_all(
                fake_http_response(
                    200,
                    &json!({
                        "database_epoch": crate::cloud::TEST_DATABASE_EPOCH,
                        "requests": []
                    }),
                )
                .as_bytes(),
            )
            .await
            .context("initial empty request-page response should write")?;
        drop(initial_fetch);

        let first_socket = accept_local_websocket(&listener).await?;
        let _ = first_connection_tx.send(());

        let retry_request = serve_request_page(&listener, std::slice::from_ref(&request)).await?;
        drop(first_socket);
        anyhow::ensure!(retry_request.method == "requests");
        let (mut ack_stream, ack_request) = receive_rest_request(&listener).await?;
        let record_path =
            runtime_test_request_path(&server_root, crate::cloud::TEST_DATABASE_EPOCH, request.seq);
        let durable = std::fs::read(record_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<InboxRecord>(&bytes).ok())
            .is_some_and(|record| {
                record.request == request && record.status == InboxStatus::Pending
            });
        let valid_ack = ack_request.method == "ack"
            && ack_request.body["receipts"]
                .as_array()
                .is_some_and(|receipts| {
                    receipts.len() == 1 && cloud_ack_receipt_matches_request(&receipts[0], &request)
                });
        ack_stream
            .write_all(fake_http_response(200, &json!({"ok": true})).as_bytes())
            .await
            .context("fake ACK response should write")?;
        drop(ack_stream);
        let _ = ack_verified_tx.send((durable, valid_ack));

        let mut second_socket = accept_local_websocket(&listener).await?;
        let _ = second_connection_tx.send(());
        loop {
            tokio::select! {
                _ = &mut stop_server_rx => {
                    let _ = second_socket.close(None).await;
                    return Ok(());
                }
                message = second_socket.next() => {
                    match message {
                        Some(Ok(WsMessage::Ping(payload))) => {
                            second_socket.send(WsMessage::Pong(payload)).await
                                .context("reconnected peer should answer liveness Ping")?;
                        }
                        Some(Ok(WsMessage::Close(_))) | None => return Ok(()),
                        Some(Err(error)) if is_expected_websocket_reset(&error) => return Ok(()),
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(error).context("reconnected WebSocket read failed"),
                    }
                }
            }
        }
    });

    let expected_request_path = runtime_test_request_path(
        &runtime.bot.config.downloads.video_dir,
        crate::cloud::TEST_DATABASE_EPOCH,
        request_seq,
    );
    runtime.client = CloudClient::with_test_base_url(&format!("http://{address}/"))
        .expect("test cloud client should use local HTTP and WebSocket");
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let receiver = tokio::spawn(cloud_websocket_receiver_with_liveness_policy(
        runtime.clone(),
        TEST_E2E_LIVENESS_POLICY,
    ));

    timeout(Duration::from_secs(5), first_connection_rx)
        .await
        .expect("receiver should establish the initial WebSocket")
        .expect("fake server should observe the initial WebSocket");
    let (durable_before_ack, valid_ack) = timeout(Duration::from_secs(5), ack_verified_rx)
        .await
        .expect("reconnect should fetch and ACK the authoritative request")
        .expect("fake server should verify the durable ACK");
    assert!(
        durable_before_ack,
        "local receipt must be durable before ACK"
    );
    assert!(
        valid_ack,
        "ACK should bind the authoritative request envelope"
    );
    timeout(Duration::from_secs(5), second_connection_rx)
        .await
        .expect("receiver should reconnect after REST reconciliation")
        .expect("fake server should accept the reconnected WebSocket");

    let telegram_request = timeout(Duration::from_secs(5), telegram_requests.recv())
        .await
        .expect("durably ACKed request should reach the mock Telegram API")
        .expect("mock Telegram API should remain available");
    assert_eq!(telegram_request.method, "sendMessage");
    assert_eq!(telegram_request.body["chat_id"], 123_456_789_i64);
    assert!(expected_request_path.is_file());

    runtime.signal_epoch_fatal();
    timeout(Duration::from_secs(5), receiver)
        .await
        .expect("epoch signal should stop the receiver")
        .expect("receiver task should not panic");
    timeout(Duration::from_secs(5), dispatcher)
        .await
        .expect("epoch signal should stop the dispatcher")
        .expect("dispatcher task should not panic");
    let _ = stop_server_tx.send(());
    server
        .await
        .expect("fake Cloudflare task should not panic")
        .expect("fake Cloudflare task should complete");
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    drop(runtime);
    let _ = std::fs::remove_dir_all(root);
}
