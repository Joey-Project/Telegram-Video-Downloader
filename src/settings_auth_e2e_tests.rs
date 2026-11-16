use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::cloud::{
    CloudRequest, CloudViewerInterest, CloudViewerSurface, InboxRecord, InboxStatus,
    SnapshotRefreshReason, SnapshotRefreshRequest, SnapshotRefreshScope, TEST_DATABASE_EPOCH,
    now_millis,
};
use crate::cloud_bot_tests::{
    FakeCloudApiState, FakeTelegramRequest, TEST_BILIBILI_COOKIE_SENTINEL,
    TEST_BILIBILI_ERROR_TOKEN, TEST_BILIBILI_QR_SENTINEL, TEST_BILIBILI_TV_ACCESS_KEY,
    make_runtime, open_runtime_test_inbox, spawn_fake_bilibili_auth_api,
    spawn_fake_cloud_api_with_epoch, spawn_fake_telegram_api, stop_fake_bilibili_api,
    stop_fake_cloud_api, stop_fake_telegram_api, take_cloud_test_telegram_requests,
    temp_cloud_bot_dir,
};
use serde_json::{Value, json};

use super::*;

const OWNER_CHAT_ID: i64 = 123_456_789;
const SETTINGS_VIEWER_ID: &str = "00000000-0000-4000-8000-000000000001";
const E2E_TIMEOUT: Duration = Duration::from_secs(8);

struct AuthGlobalsRestore {
    generation: u64,
    credential_revision: u64,
    snapshot: settings_auth::Snapshot,
    auth_epoch: Option<bilibili_auth::AuthEpoch>,
}

impl Drop for AuthGlobalsRestore {
    fn drop(&mut self) {
        BILIBILI_AUTH_GENERATION.store(self.generation, Ordering::SeqCst);
        BILIBILI_CREDENTIAL_REVISION.store(self.credential_revision, Ordering::SeqCst);
        set_bilibili_settings_auth_epoch(self.auth_epoch);
        if let Some(state) = BILIBILI_SETTINGS_AUTH.get()
            && let Ok(mut state) = state.try_lock()
        {
            *state = self.snapshot.clone();
        }
    }
}

async fn dispatch_auth_request(runtime: &CloudBotRuntime, seq: u64, payload: Value) -> InboxRecord {
    let mut extra = serde_json::Map::new();
    extra.insert(
        "request_id".to_string(),
        json!(format!("settings-auth-e2e-{seq}")),
    );
    runtime
        .inbox
        .persist(CloudRequest {
            seq,
            kind: "bilibili_auth".to_string(),
            payload,
            created_at: now_millis(),
            extra,
        })
        .expect("auth action should be durably accepted");
    runtime.request_notify.notify_one();
    tokio::time::timeout(E2E_TIMEOUT, async {
        loop {
            if let Some(record) = runtime
                .inbox
                .record(seq)
                .expect("durable auth receipt should load")
                && matches!(record.status, InboxStatus::Completed | InboxStatus::Failed)
            {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("durable auth action should reach a terminal receipt")
}

async fn wait_for_auth_phase(phase: &str, generation: u64) -> Value {
    tokio::time::timeout(E2E_TIMEOUT, async {
        loop {
            let snapshot = bilibili_settings_auth_snapshot().await;
            if snapshot["phase"] == phase && snapshot["generation"].as_u64() == Some(generation) {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Bilibili auth state should reach the requested phase")
}

async fn wait_for_bilibili_poll(
    polls: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    expected: &str,
    case: &str,
) {
    let mut observed = Vec::new();
    let event = match tokio::time::timeout(E2E_TIMEOUT, async {
        loop {
            match polls.recv().await {
                Some(event) if event.ends_with(":ticket") => observed.push(event),
                Some(event) => break event,
                None => break String::new(),
            }
        }
    })
    .await
    {
        Ok(event) if !event.is_empty() => event,
        Ok(_) => {
            let snapshot = bilibili_settings_auth_snapshot().await;
            let phase = snapshot["phase"].as_str().unwrap_or("unknown");
            let reason = snapshot["safe_reason"].as_str().unwrap_or("unknown");
            panic!(
                "mock Bilibili API event channel closed before {expected} in {case}; observed safe request kinds: {observed:?}; safe auth phase={phase} reason={reason}"
            );
        }
        Err(_) => {
            let snapshot = bilibili_settings_auth_snapshot().await;
            let phase = snapshot["phase"].as_str().unwrap_or("unknown");
            let reason = snapshot["safe_reason"].as_str().unwrap_or("unknown");
            panic!(
                "mock Bilibili API did not receive expected QR poll {expected} in {case}; observed safe request kinds: {observed:?}; safe auth phase={phase} reason={reason}"
            );
        }
    };
    let snapshot = bilibili_settings_auth_snapshot().await;
    let phase = snapshot["phase"].as_str().unwrap_or("unknown");
    let reason = snapshot["safe_reason"].as_str().unwrap_or("unknown");
    assert_eq!(
        event, expected,
        "mock Bilibili poll order should be stable for {case}; observed safe request kinds: {observed:?}; safe auth phase={phase} reason={reason}"
    );
}

async fn wait_for_login_lock_idle() {
    tokio::time::timeout(E2E_TIMEOUT, async {
        loop {
            if let Some(lock) = BILIBILI_LOGIN_LOCK.get()
                && let Ok(guard) = lock.try_lock()
            {
                drop(guard);
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("detached Bilibili login should release its in-flight lock");
}

fn fake_cloud_auth_phase(body: &Value) -> Option<&str> {
    body.pointer("/settings/applied/bilibili_auth/phase")
        .and_then(Value::as_str)
}

async fn wait_for_cloud_auth_publication(
    state: &Arc<FakeCloudApiState>,
    phase: &str,
    minimum_count: usize,
) -> (Value, Value) {
    tokio::time::timeout(E2E_TIMEOUT, async {
        loop {
            let bodies = state
                .state_bodies
                .lock()
                .expect("fake cloud body lock should not poison")
                .clone();
            let posts = state
                .state_posts
                .lock()
                .expect("fake cloud post lock should not poison")
                .clone();
            if bodies.len() == posts.len()
                && bodies.len() >= minimum_count
                && let Some(index) = bodies
                    .iter()
                    .rposition(|body| fake_cloud_auth_phase(body) == Some(phase))
            {
                return (bodies[index].clone(), posts[index].clone());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("existing cloud reporter should publish the Bilibili auth state")
}

async fn request_user_settings_snapshot_refresh(
    runtime: &CloudBotRuntime,
    results: &mut tokio::sync::mpsc::Receiver<Value>,
    request_number: u64,
) -> Value {
    let expires_at = now_millis().saturating_add(60_000);
    runtime.viewer_interests.lock().await.insert(
        SETTINGS_VIEWER_ID.to_string(),
        CloudViewerInterest {
            viewer_id: SETTINGS_VIEWER_ID.to_string(),
            surface: CloudViewerSurface::Settings,
            watch: false,
            expires_at,
        },
    );
    let request = SnapshotRefreshRequest {
        viewer_id: SETTINGS_VIEWER_ID.to_string(),
        request_id: format!("a11a0000-0000-4000-8000-{request_number:012x}"),
        database_epoch: TEST_DATABASE_EPOCH.to_string(),
        reason: SnapshotRefreshReason::Manual,
        scope: SnapshotRefreshScope::Settings,
        library_id: None,
        expires_at,
    };
    enqueue_snapshot_refresh(runtime, request.clone())
        .await
        .expect("user Settings refresh request should queue");
    tokio::time::timeout(E2E_TIMEOUT, async {
        loop {
            if let Some(result) = results.recv().await
                && result["request_id"] == request.request_id
            {
                return result;
            }
        }
    })
    .await
    .expect("user Settings refresh should receive a result")
}

async fn externally_save_test_tv_credential(config: &AppConfig) -> bilibili_auth::AuthEpoch {
    let state_path = config.bilibili.auth.state_path.clone();
    let credential_file = config.bilibili.auth.credential_file.clone();
    let credential_profile = config.bilibili.auth.credential_profile.clone();
    let ((), auth_epoch) = tokio::task::spawn_blocking(move || {
        bilibili_auth::with_auth_mutation_transaction(
            &state_path,
            &credential_file,
            |transaction| {
                bilibili_core::CredentialRuntime::from_credential_file(
                    transaction.credential_file()?,
                    credential_profile.as_deref(),
                )?
                .save_merged(
                    bbdown_core::Credentials::default()
                        .with_tv_access_key(TEST_BILIBILI_TV_ACCESS_KEY),
                )?;
                Ok(())
            },
        )
    })
    .await
    .expect("external synthetic save worker should complete")
    .expect("external synthetic save should use the credential transaction");
    auth_epoch
}

async fn externally_delete_test_credentials(config: &AppConfig) -> bilibili_auth::AuthEpoch {
    let state_path = config.bilibili.auth.state_path.clone();
    let credential_file = config.bilibili.auth.credential_file.clone();
    let credential_profile = config.bilibili.auth.credential_profile.clone();
    let ((legacy_state, credential_state), auth_epoch) = tokio::task::spawn_blocking(move || {
        bilibili_auth::logout_bbdown_auth_with_epoch(
            &state_path,
            &credential_file,
            credential_profile.as_deref(),
        )
    })
    .await
    .expect("external synthetic delete worker should complete")
    .expect("external synthetic delete should use the shared auth transaction");
    legacy_state.expect("external delete should clean legacy state");
    credential_state.expect("external delete should clean selected credentials");
    auth_epoch
}

async fn seed_auth_save_race_state(
    config: &AppConfig,
    generation: u64,
) -> bilibili_auth::AuthEpoch {
    let auth_epoch = externally_save_test_tv_credential(config).await;
    BILIBILI_AUTH_GENERATION.store(generation, Ordering::SeqCst);
    BILIBILI_CREDENTIAL_REVISION.store(auth_epoch.value(), Ordering::SeqCst);
    set_bilibili_settings_auth_epoch(Some(auth_epoch));
    let mut snapshot = settings_auth::Snapshot::default();
    snapshot.transition(
        generation,
        settings_auth::Phase::WaitingForScan,
        "none",
        now_millis(),
    );
    snapshot.mode = Some(settings_auth::Mode::Tv);
    snapshot.credential_revision = auth_epoch.value();
    snapshot.credential_summary.has_tv_access_key = true;
    *bilibili_settings_auth_state().lock().await = snapshot;
    auth_epoch
}

fn telegram_login_command(config: &AppConfig) -> BilibiliAuthCommand {
    match crate::router::route_message("/bbdown login tv", &config.pdf.auto_domains) {
        RouteResult::BilibiliAuth(command) => command,
        route => panic!("Telegram login text should route to BBDown auth, got {route:?}"),
    }
}

async fn handle_test_telegram_login(runtime: &CloudBotRuntime) {
    handle_bilibili_auth_command(
        runtime.bot.telegram.clone(),
        Arc::clone(&runtime.bot.config),
        OWNER_CHAT_ID,
        true,
        telegram_login_command(&runtime.bot.config),
    )
    .await;
}

async fn public_auth_projection(runtime: &CloudBotRuntime) -> Value {
    let snapshot = build_cloud_state_snapshot(runtime)
        .await
        .expect("local cloud snapshot should build")
        .expect("local cloud snapshot should be available");
    let projection = json!({
        "revision": snapshot.revision,
        "library": snapshot.library,
        "previews": snapshot.previews,
        "operations": snapshot.operations,
        "tasks": snapshot.tasks,
        "download_dir": snapshot.download_dir,
        "settings_applied": snapshot.settings_applied,
    });
    assert_no_bilibili_secrets(&projection.to_string());
    projection
}

fn assert_no_bilibili_secrets(serialized: &str) {
    for secret in [
        TEST_BILIBILI_QR_SENTINEL,
        TEST_BILIBILI_COOKIE_SENTINEL,
        TEST_BILIBILI_TV_ACCESS_KEY,
        TEST_BILIBILI_ERROR_TOKEN,
    ] {
        assert!(
            !serialized.contains(secret),
            "public auth data must not contain credential fixtures"
        );
    }
}

fn web_wait_for_confirmation() -> Value {
    json!({
        "code": 0,
        "data": {
            "code": 86090,
            "message": "waiting for confirmation",
            "cookie": TEST_BILIBILI_COOKIE_SENTINEL,
        }
    })
}

fn web_expired() -> Value {
    json!({
        "code": 0,
        "data": {
            "code": 86038,
            "message": "expired",
            "cookie": TEST_BILIBILI_COOKIE_SENTINEL,
        }
    })
}

fn web_failed() -> Value {
    json!({
        "code": 0,
        "data": {
            "code": -1,
            "message": "authorization request failed",
            "access_token": TEST_BILIBILI_ERROR_TOKEN,
            "cookie": TEST_BILIBILI_COOKIE_SENTINEL,
        }
    })
}

fn tv_succeeded() -> Value {
    json!({
        "code": 0,
        "data": {
            "access_token": TEST_BILIBILI_TV_ACCESS_KEY,
        }
    })
}

#[tokio::test]
async fn bbdown_cancel_and_credential_save_share_one_auth_state_order() {
    let _auth_generation_guard = TEST_AUTH_GENERATION_LOCK.lock().await;
    let original_snapshot = bilibili_settings_auth_state().lock().await.clone();
    let _restore = AuthGlobalsRestore {
        generation: BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst),
        credential_revision: BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst),
        snapshot: original_snapshot,
        auth_epoch: get_bilibili_settings_auth_epoch(),
    };
    BILIBILI_AUTH_GENERATION.store(0, Ordering::SeqCst);
    BILIBILI_CREDENTIAL_REVISION.store(0, Ordering::SeqCst);
    set_bilibili_settings_auth_epoch(None);
    *bilibili_settings_auth_state().lock().await = settings_auth::Snapshot::default();

    let save_first_root = temp_cloud_bot_dir("settings-auth-cancel-save-first");
    let save_first_auth_dir = save_first_root.join("private-auth");
    fs::create_dir_all(&save_first_auth_dir).expect("private auth fixture should create");
    #[cfg(unix)]
    fs::set_permissions(&save_first_auth_dir, fs::Permissions::from_mode(0o700))
        .expect("private auth fixture should remain private");
    let mut save_first_config = AppConfig::for_test();
    save_first_config.bilibili.auth.credential_file = save_first_auth_dir.join("credentials.json");
    save_first_config.bilibili.auth.state_path = save_first_auth_dir.join("legacy-state.json");
    save_first_config
        .ensure_runtime_dirs()
        .expect("save-first test directories should create");
    let save_first_epoch = seed_auth_save_race_state(&save_first_config, 2).await;

    let (save_entered_tx, save_entered_rx) = tokio::sync::oneshot::channel();
    let (resume_save_tx, resume_save_rx) = tokio::sync::oneshot::channel();
    let save_config = save_first_config.clone();
    let save_task = tokio::spawn(async move {
        let saved = save_bbdown_login_credentials_with_sync_and_lock_hook(
            &save_config,
            2,
            save_first_epoch,
            bbdown_core::Credentials::default().with_tv_access_key(TEST_BILIBILI_COOKIE_SENTINEL),
            |_transaction| Ok(()),
            move || async move {
                let _ = save_entered_tx.send(());
                let _ = resume_save_rx.await;
            },
        )
        .await?;
        set_bilibili_settings_auth_credentials(&saved).await;
        Ok::<_, anyhow::Error>(saved)
    });
    save_entered_rx
        .await
        .expect("credential save should reach its lock barrier");
    let (cancel_started_tx, cancel_started_rx) = tokio::sync::oneshot::channel();
    let cancel_config = save_first_config.clone();
    let cancel_task = tokio::spawn(async move {
        let _ = cancel_started_tx.send(());
        cancel_bilibili_auth_generation(2, save_first_epoch.value(), &cancel_config).await
    });
    cancel_started_rx
        .await
        .expect("cancel task should start before the save barrier releases");
    tokio::task::yield_now().await;
    assert!(
        !cancel_task.is_finished(),
        "cancel must wait while a credential save owns the shared auth-state lock"
    );
    resume_save_tx
        .send(())
        .expect("save barrier should release after cancel waits");
    let saved = save_task
        .await
        .expect("save task should not panic")
        .expect("credential save should complete before cancel checks the new revision");
    assert!(
        !cancel_task.await.expect("cancel task should not panic"),
        "cancel must reject the generation after the credential save advances its revision"
    );
    assert_eq!(
        bilibili_core::CredentialRuntime::from_credential_file(
            save_first_config.bilibili.auth.credential_file.clone(),
            save_first_config
                .bilibili
                .auth
                .credential_profile
                .as_deref(),
        )
        .expect("saved fixture should remain readable")
        .load()
        .expect("saved fixture should parse")
        .tv_access_key
        .as_deref(),
        Some(TEST_BILIBILI_COOKIE_SENTINEL),
        "cancel after save must preserve the successfully written credential"
    );
    assert_eq!(
        BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst),
        saved.credential_revision
    );
    assert_eq!(
        bilibili_settings_auth_snapshot().await["phase"],
        "connected"
    );

    let cancel_first_root = temp_cloud_bot_dir("settings-auth-cancel-first");
    let cancel_first_auth_dir = cancel_first_root.join("private-auth");
    fs::create_dir_all(&cancel_first_auth_dir).expect("private auth fixture should create");
    #[cfg(unix)]
    fs::set_permissions(&cancel_first_auth_dir, fs::Permissions::from_mode(0o700))
        .expect("private auth fixture should remain private");
    let mut cancel_first_config = AppConfig::for_test();
    cancel_first_config.bilibili.auth.credential_file =
        cancel_first_auth_dir.join("credentials.json");
    cancel_first_config.bilibili.auth.state_path = cancel_first_auth_dir.join("legacy-state.json");
    cancel_first_config
        .ensure_runtime_dirs()
        .expect("cancel-first test directories should create");
    let cancel_first_epoch = seed_auth_save_race_state(&cancel_first_config, 2).await;

    let (cancel_lock_tx, cancel_lock_rx) = tokio::sync::oneshot::channel();
    let (resume_cancel_tx, resume_cancel_rx) = tokio::sync::oneshot::channel();
    let cancel_config = cancel_first_config.clone();
    let cancel_task = tokio::spawn(async move {
        cancel_bilibili_auth_generation_with_lock_hook(
            2,
            cancel_first_epoch.value(),
            &cancel_config,
            move || async move {
                let _ = cancel_lock_tx.send(());
                let _ = resume_cancel_rx.await;
            },
        )
        .await
    });
    cancel_lock_rx
        .await
        .expect("cancel should pause after taking the auth-state lock");
    let (save_started_tx, save_started_rx) = tokio::sync::oneshot::channel();
    let save_config = cancel_first_config.clone();
    let save_task = tokio::spawn(async move {
        let _ = save_started_tx.send(());
        save_bbdown_login_credentials_with_sync(
            &save_config,
            2,
            cancel_first_epoch,
            bbdown_core::Credentials::default().with_tv_access_key(TEST_BILIBILI_COOKIE_SENTINEL),
            |_transaction| Ok(()),
        )
        .await
    });
    save_started_rx
        .await
        .expect("stale credential save should start behind the cancel lock");
    tokio::task::yield_now().await;
    assert!(
        !save_task.is_finished(),
        "credential save must wait while cancel owns the shared auth-state lock"
    );
    resume_cancel_tx
        .send(())
        .expect("cancel barrier should release after the save waits");
    assert!(
        cancel_task.await.expect("cancel task should not panic"),
        "cancel that obtains the shared lock first should complete"
    );
    assert!(
        save_task
            .await
            .expect("save task should not panic")
            .is_err(),
        "save for the cancelled generation should fail before mutation"
    );
    assert_eq!(
        bilibili_core::CredentialRuntime::from_credential_file(
            cancel_first_config.bilibili.auth.credential_file.clone(),
            cancel_first_config
                .bilibili
                .auth
                .credential_profile
                .as_deref(),
        )
        .expect("cancel-first fixture should remain readable")
        .load()
        .expect("cancel-first fixture should parse")
        .tv_access_key
        .as_deref(),
        Some(TEST_BILIBILI_TV_ACCESS_KEY),
        "stale save must not replace the credential that existed before cancellation"
    );
    assert_eq!(BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst), 4);
    assert_eq!(
        bilibili_settings_auth_snapshot().await["phase"],
        "cancelled"
    );

    let _ = fs::remove_dir_all(save_first_root);
    let _ = fs::remove_dir_all(cancel_first_root);
}

#[tokio::test]
async fn telegram_bbdown_login_generations_are_fresh_and_settings_cancel_is_scoped() {
    let _auth_generation_guard = TEST_AUTH_GENERATION_LOCK.lock().await;
    let original_snapshot = bilibili_settings_auth_state().lock().await.clone();
    let _restore = AuthGlobalsRestore {
        generation: BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst),
        credential_revision: BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst),
        snapshot: original_snapshot,
        auth_epoch: get_bilibili_settings_auth_epoch(),
    };
    BILIBILI_AUTH_GENERATION.store(0, Ordering::SeqCst);
    BILIBILI_CREDENTIAL_REVISION.store(0, Ordering::SeqCst);
    set_bilibili_settings_auth_epoch(None);
    *bilibili_settings_auth_state().lock().await = settings_auth::Snapshot::default();

    let root = temp_cloud_bot_dir("telegram-settings-auth-generation-e2e");
    let auth_dir = root.join("private-auth");
    fs::create_dir_all(&auth_dir).expect("private auth fixture should create");
    #[cfg(unix)]
    fs::set_permissions(&auth_dir, fs::Permissions::from_mode(0o700))
        .expect("private auth fixture should remain private");
    let (bilibili_base_url, mut polls, release_poll, bilibili_shutdown, bilibili_server) =
        spawn_fake_bilibili_auth_api(Vec::new(), vec![tv_succeeded(), tv_succeeded()], &[1, 2])
            .await;

    let mut config = AppConfig::for_test();
    config.telegram.allowed_chat_ids = vec![OWNER_CHAT_ID];
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.bilibili.auth.credential_file = auth_dir.join("credentials.json");
    config.bilibili.auth.state_path = auth_dir.join("legacy-state.json");
    config.bilibili.auth.login_timeout_seconds = 20;
    config.bilibili.auth.poll_interval_seconds = 1;
    config.bilibili.global_args = vec![
        "--passport-base".to_string(),
        bilibili_base_url.clone(),
        "--tv-passport-base".to_string(),
        bilibili_base_url.clone(),
        "--tv-passport-poll-base".to_string(),
        bilibili_base_url.clone(),
    ];
    config
        .ensure_runtime_dirs()
        .expect("private runtime fixture roots should create");

    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let queue = Arc::new(QueueManager::open(&config).expect("fixture queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir)
            .expect("fixture cloud inbox should open"),
    );
    let runtime = make_runtime(telegram, config, queue, Arc::clone(&inbox));
    let (cloud_base_url, _cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api_with_epoch(
            runtime.bot.config.downloads.video_dir.clone(),
            Vec::new(),
            TEST_DATABASE_EPOCH.to_string(),
        )
        .await;
    let mut runtime = runtime;
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("auth dispatcher should use the loopback Cloud API");
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));

    handle_test_telegram_login(&runtime).await;
    wait_for_bilibili_poll(&mut polls, "tv:1", "first Telegram login attempt").await;
    let first = bilibili_settings_auth_snapshot().await;
    let first_generation = first["generation"]
        .as_u64()
        .expect("first Telegram QR snapshot should carry a generation");
    let first_revision = first["credential_revision"]
        .as_u64()
        .expect("first Telegram QR snapshot should carry a credential revision");
    assert!(first_generation > 0);
    assert_eq!(first_generation % 2, 0);
    assert_eq!(first["phase"], "waiting_for_scan");
    let first_cancel = dispatch_auth_request(
        &runtime,
        1,
        json!({
            "operation": "cancel",
            "generation": first_generation,
            "credential_revision": first_revision,
        }),
    )
    .await;
    assert_eq!(first_cancel.result.as_ref().unwrap()["status"], "completed");
    let first_cancelled_generation = first_cancel.result.as_ref().unwrap()["generation"]
        .as_u64()
        .expect("first MiniApp cancellation should carry the settled generation");
    release_poll
        .send("tv:1".to_string())
        .expect("first Telegram QR poll should be released after cancellation");
    wait_for_login_lock_idle().await;
    assert_eq!(
        wait_for_auth_phase("cancelled", first_cancelled_generation).await["phase"],
        "cancelled"
    );

    handle_test_telegram_login(&runtime).await;
    wait_for_bilibili_poll(&mut polls, "tv:2", "second Telegram login attempt").await;
    let second = bilibili_settings_auth_snapshot().await;
    let second_generation = second["generation"]
        .as_u64()
        .expect("second Telegram QR snapshot should carry a generation");
    let second_revision = second["credential_revision"]
        .as_u64()
        .expect("second Telegram QR snapshot should carry a credential revision");
    assert!(second_generation > first_generation);
    assert_eq!(second["phase"], "waiting_for_scan");

    let stale_cancel = dispatch_auth_request(
        &runtime,
        2,
        json!({
            "operation": "cancel",
            "generation": first_generation,
            "credential_revision": first_revision,
        }),
    )
    .await;
    assert_eq!(stale_cancel.status, InboxStatus::Failed);
    assert_eq!(stale_cancel.result.as_ref().unwrap()["status"], "conflict");
    let after_stale_cancel = bilibili_settings_auth_snapshot().await;
    assert_eq!(after_stale_cancel["generation"], second_generation);
    assert_eq!(after_stale_cancel["phase"], "waiting_for_scan");

    let second_cancel = dispatch_auth_request(
        &runtime,
        3,
        json!({
            "operation": "cancel",
            "generation": second_generation,
            "credential_revision": second_revision,
        }),
    )
    .await;
    assert_eq!(
        second_cancel.result.as_ref().unwrap()["status"],
        "completed"
    );
    let second_cancelled_generation = second_cancel.result.as_ref().unwrap()["generation"]
        .as_u64()
        .expect("current MiniApp cancellation should settle the second generation");
    release_poll
        .send("tv:2".to_string())
        .expect("second Telegram QR poll should be released after cancellation");
    wait_for_login_lock_idle().await;
    assert_eq!(
        wait_for_auth_phase("cancelled", second_cancelled_generation).await["phase"],
        "cancelled"
    );

    let telegram_requests = take_cloud_test_telegram_requests(&mut telegram_requests);
    let photos = telegram_requests
        .iter()
        .filter(|request| request.method == "sendPhoto")
        .collect::<Vec<_>>();
    assert_eq!(
        photos.len(),
        2,
        "each Telegram login should send one QR photo"
    );
    for request in &telegram_requests {
        if request.method == "sendPhoto" || request.method == "sendMessage" {
            let chat_id = request.body["chat_id"].as_i64().or_else(|| {
                request.body["chat_id"]
                    .as_str()
                    .and_then(|value| value.parse::<i64>().ok())
            });
            assert_eq!(chat_id, Some(OWNER_CHAT_ID));
        }
        assert_no_bilibili_secrets(&request.body.to_string());
    }

    dispatcher.abort();
    let _ = dispatcher.await;
    stop_fake_bilibili_api(bilibili_shutdown, bilibili_server).await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn durable_settings_auth_dispatcher_covers_qr_lifecycle_privacy_and_snapshot_push() {
    let _auth_generation_guard = TEST_AUTH_GENERATION_LOCK.lock().await;
    let original_snapshot = bilibili_settings_auth_state().lock().await.clone();
    let _restore = AuthGlobalsRestore {
        generation: BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst),
        credential_revision: BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst),
        snapshot: original_snapshot,
        auth_epoch: get_bilibili_settings_auth_epoch(),
    };
    BILIBILI_AUTH_GENERATION.store(0, Ordering::SeqCst);
    BILIBILI_CREDENTIAL_REVISION.store(0, Ordering::SeqCst);
    set_bilibili_settings_auth_epoch(None);
    *bilibili_settings_auth_state().lock().await = settings_auth::Snapshot::default();

    let root = temp_cloud_bot_dir("settings-auth-native-e2e");
    let auth_dir = root.join("private-auth");
    fs::create_dir_all(&auth_dir).expect("private fixture directory should create");
    #[cfg(unix)]
    fs::set_permissions(&auth_dir, fs::Permissions::from_mode(0o700))
        .expect("private fixture directory should remain private");

    let (bilibili_base_url, mut polls, release_poll, bilibili_shutdown, bilibili_server) =
        spawn_fake_bilibili_auth_api(
            vec![
                web_wait_for_confirmation(),
                web_wait_for_confirmation(),
                web_expired(),
                web_failed(),
            ],
            vec![
                tv_succeeded(),
                tv_succeeded(),
                tv_succeeded(),
                tv_succeeded(),
                tv_succeeded(),
            ],
            &[1, 2, 3, 4, 5],
        )
        .await;

    let mut config = AppConfig::for_test();
    config.telegram.allowed_chat_ids = vec![OWNER_CHAT_ID];
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.bilibili.auth.credential_file = auth_dir.join("credentials.json");
    config.bilibili.auth.state_path = auth_dir.join("legacy-state.json");
    config.bilibili.auth.login_timeout_seconds = 20;
    config.bilibili.auth.poll_interval_seconds = 1;
    config.bilibili.global_args = vec![
        "--passport-base".to_string(),
        bilibili_base_url.clone(),
        "--tv-passport-base".to_string(),
        bilibili_base_url.clone(),
        "--tv-passport-poll-base".to_string(),
        bilibili_base_url.clone(),
    ];
    config
        .ensure_runtime_dirs()
        .expect("private runtime fixture roots should create");

    let (telegram, mut telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let queue = Arc::new(QueueManager::open(&config).expect("fixture queue should open"));
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir)
            .expect("fixture cloud inbox should open"),
    );
    let mut runtime = make_runtime(telegram, config.clone(), queue, Arc::clone(&inbox));
    let (cloud_base_url, cloud_state, cloud_shutdown, cloud_server) =
        spawn_fake_cloud_api_with_epoch(
            config.downloads.video_dir.clone(),
            Vec::new(),
            TEST_DATABASE_EPOCH.to_string(),
        )
        .await;
    runtime.client = CloudClient::with_test_base_url(&cloud_base_url)
        .expect("snapshot client should use only the loopback fake cloud API");

    let reporter = tokio::spawn(cloud_state_reporter_with_retry_policy(
        runtime.clone(),
        Duration::from_secs(3_600),
        Duration::from_millis(25),
        Duration::from_secs(3_600),
    ));
    let (baseline, baseline_post) =
        wait_for_cloud_auth_publication(&cloud_state, "signed_out", 1).await;
    assert_eq!(
        baseline_post["snapshot_notify_viewers"].as_str(),
        Some("0"),
        "closed Settings should use the ordinary bootstrap publication"
    );
    assert_no_bilibili_secrets(&baseline.to_string());
    let startup_revision = baseline["settings"]["applied"]["bilibili_auth"]["credential_revision"]
        .as_u64()
        .expect("bootstrap snapshot should expose a safe durable credential epoch");
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));

    let missing_revision_cancel = dispatch_auth_request(
        &runtime,
        79,
        json!({"operation": "cancel", "generation": 0}),
    )
    .await;
    assert_eq!(missing_revision_cancel.status, InboxStatus::Failed);
    assert_eq!(
        BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst),
        0,
        "the durable dispatcher must reject cancel without its credential revision"
    );
    assert_eq!(
        bilibili_settings_auth_snapshot().await["phase"],
        "signed_out",
        "a malformed cancel must not change the auth phase"
    );
    assert_no_bilibili_secrets(&serde_json::to_string(&missing_revision_cancel).unwrap());

    let prior_process_start =
        dispatch_auth_request(&runtime, 80, json!({"operation": "login", "mode": "tv"})).await;
    let prior_process_generation = prior_process_start.result.as_ref().unwrap()["generation"]
        .as_u64()
        .expect("prior process login should include a safe generation");
    let prior_process_revision =
        prior_process_start.result.as_ref().unwrap()["credential_revision"]
            .as_u64()
            .expect("prior process login should include a durable credential epoch");
    assert_eq!(prior_process_revision, startup_revision);
    wait_for_bilibili_poll(&mut polls, "tv:1", "pre-restart cancellation fence").await;
    let prior_process_cancel = dispatch_auth_request(
        &runtime,
        81,
        json!({
            "operation": "cancel",
            "generation": prior_process_generation,
            "credential_revision": prior_process_revision,
        }),
    )
    .await;
    assert_eq!(
        prior_process_cancel.result.as_ref().unwrap()["status"],
        "completed"
    );
    let prior_process_cancelled_generation =
        prior_process_cancel.result.as_ref().unwrap()["generation"]
            .as_u64()
            .expect("prior process cancel should include a safe generation");
    release_poll
        .send("tv:1".to_string())
        .expect("prior process stale poll should be released after cancel");
    wait_for_login_lock_idle().await;
    assert_eq!(
        wait_for_auth_phase("cancelled", prior_process_cancelled_generation).await["phase"],
        "cancelled"
    );

    BILIBILI_AUTH_GENERATION.store(0, Ordering::SeqCst);
    BILIBILI_CREDENTIAL_REVISION.store(0, Ordering::SeqCst);
    set_bilibili_settings_auth_epoch(None);
    *bilibili_settings_auth_state().lock().await = settings_auth::Snapshot::default();
    initialize_bilibili_settings_auth(&config).await;
    let restart_revision = BILIBILI_CREDENTIAL_REVISION.load(Ordering::SeqCst);
    assert!(
        restart_revision > startup_revision,
        "reinitializing Settings auth after a process restart should advance the durable epoch"
    );
    let restarted_process_start =
        dispatch_auth_request(&runtime, 82, json!({"operation": "login", "mode": "tv"})).await;
    let restarted_process_generation =
        restarted_process_start.result.as_ref().unwrap()["generation"]
            .as_u64()
            .expect("restarted process login should include a safe generation");
    assert_eq!(
        restarted_process_generation, prior_process_generation,
        "process-local generations are expected to repeat so the credential epoch must fence them"
    );
    wait_for_bilibili_poll(&mut polls, "tv:2", "same-generation post-restart fence").await;
    let stale_cancel_after_restart = dispatch_auth_request(
        &runtime,
        83,
        json!({
            "operation": "cancel",
            "generation": prior_process_generation,
            "credential_revision": prior_process_revision,
        }),
    )
    .await;
    assert_eq!(stale_cancel_after_restart.status, InboxStatus::Failed);
    assert_eq!(
        stale_cancel_after_restart.result.as_ref().unwrap()["status"],
        "conflict"
    );
    assert_eq!(
        BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst),
        restarted_process_generation,
        "a stale cancel must not cancel the same-numbered generation after restart"
    );
    assert_eq!(
        bilibili_settings_auth_snapshot().await["phase"],
        "waiting_for_scan"
    );
    let current_process_cancel = dispatch_auth_request(
        &runtime,
        84,
        json!({
            "operation": "cancel",
            "generation": restarted_process_generation,
            "credential_revision": restart_revision,
        }),
    )
    .await;
    assert_eq!(
        current_process_cancel.result.as_ref().unwrap()["status"],
        "completed"
    );
    let post_restart_cancelled_generation =
        current_process_cancel.result.as_ref().unwrap()["generation"]
            .as_u64()
            .expect("current process cancel should include a safe generation");
    release_poll
        .send("tv:2".to_string())
        .expect("restarted stale poll should be released after valid cancel");
    wait_for_login_lock_idle().await;
    assert_eq!(
        wait_for_auth_phase("cancelled", post_restart_cancelled_generation).await["phase"],
        "cancelled"
    );
    let stale_after_restart = dispatch_auth_request(
        &runtime,
        90,
        json!({
            "operation": "logout",
            "generation": post_restart_cancelled_generation,
            "credential_revision": startup_revision,
        }),
    )
    .await;
    assert_eq!(stale_after_restart.status, InboxStatus::Failed);
    assert_eq!(
        stale_after_restart.result.as_ref().unwrap()["status"],
        "conflict"
    );
    assert_no_bilibili_secrets(&serde_json::to_string(&stale_after_restart).unwrap());
    let empty_after_stale_restart_logout = bilibili_core::CredentialRuntime::from_credential_file(
        config.bilibili.auth.credential_file.clone(),
        config.bilibili.auth.credential_profile.as_deref(),
    )
    .expect("restart fixture should keep the explicit private credential path")
    .load()
    .expect("stale restart logout should leave credentials readable");
    assert!(empty_after_stale_restart_logout.tv_access_key.is_none());

    let external_epoch = externally_save_test_tv_credential(&config).await;
    assert!(
        external_epoch.value() > restart_revision,
        "an external credential save should advance the persistent epoch"
    );
    let cached_after_external_save = bilibili_settings_auth_snapshot().await;
    assert_eq!(
        cached_after_external_save["credential_revision"].as_u64(),
        Some(restart_revision),
        "ordinary auth snapshots must not silently rescan an external credential write"
    );
    assert_ne!(
        cached_after_external_save["phase"], "connected",
        "external credential state becomes current through an explicit Settings refresh"
    );
    let (refresh_result_sender, mut refresh_results) = tokio::sync::mpsc::channel(16);
    *runtime.websocket_writer.lock().await = Some(refresh_result_sender);
    let external_save_refresh =
        request_user_settings_snapshot_refresh(&runtime, &mut refresh_results, 1).await;
    assert_eq!(external_save_refresh["status"], "published");
    let refreshed_external_snapshot = bilibili_settings_auth_snapshot().await;
    assert_eq!(refreshed_external_snapshot["phase"], "connected");
    assert_eq!(
        refreshed_external_snapshot["credential_revision"].as_u64(),
        Some(external_epoch.value())
    );
    assert_eq!(
        refreshed_external_snapshot["credential_summary"]["has_tv_access_key"],
        true
    );
    let stale_after_external_save = dispatch_auth_request(
        &runtime,
        91,
        json!({
            "operation": "logout",
            "generation": post_restart_cancelled_generation,
            "credential_revision": restart_revision,
        }),
    )
    .await;
    assert_eq!(stale_after_external_save.status, InboxStatus::Failed);
    assert_eq!(
        stale_after_external_save.result.as_ref().unwrap()["status"],
        "conflict"
    );
    assert_no_bilibili_secrets(&serde_json::to_string(&stale_after_external_save).unwrap());
    let external_credentials = bilibili_core::CredentialRuntime::from_credential_file(
        config.bilibili.auth.credential_file.clone(),
        config.bilibili.auth.credential_profile.as_deref(),
    )
    .expect("external save fixture should keep the explicit private credential path")
    .load()
    .expect("stale external-save logout should leave credentials readable");
    assert_eq!(
        external_credentials.tv_access_key.as_deref(),
        Some(TEST_BILIBILI_TV_ACCESS_KEY),
        "a stale snapshot must not delete credentials saved by another writer"
    );
    let fresh_external_revision = refreshed_external_snapshot["credential_revision"]
        .as_u64()
        .expect("refreshed snapshot should carry a safe current credential epoch");
    assert_eq!(fresh_external_revision, external_epoch.value());

    let refreshed_logout = dispatch_auth_request(
        &runtime,
        92,
        json!({
            "operation": "logout",
            "generation": post_restart_cancelled_generation,
            "credential_revision": fresh_external_revision,
        }),
    )
    .await;
    assert_eq!(
        refreshed_logout.result.as_ref().unwrap()["status"],
        "completed"
    );
    assert_eq!(
        bilibili_settings_auth_snapshot().await["credential_summary"]["has_tv_access_key"],
        false
    );
    assert!(
        bilibili_core::CredentialRuntime::from_credential_file(
            config.bilibili.auth.credential_file.clone(),
            config.bilibili.auth.credential_profile.as_deref(),
        )
        .expect("refreshed logout should retain the explicit private fixture path")
        .load()
        .expect("refreshed logout should leave credentials readable")
        .tv_access_key
        .is_none(),
        "a fresh snapshot revision should allow the user to remove the newly saved credential"
    );

    let reconnected_epoch = externally_save_test_tv_credential(&config).await;
    let external_reconnect_refresh =
        request_user_settings_snapshot_refresh(&runtime, &mut refresh_results, 2).await;
    assert_eq!(external_reconnect_refresh["status"], "published");
    let reconnected_snapshot = bilibili_settings_auth_snapshot().await;
    assert_eq!(reconnected_snapshot["phase"], "connected");
    assert_eq!(
        reconnected_snapshot["credential_revision"].as_u64(),
        Some(reconnected_epoch.value())
    );

    let deleted_epoch = externally_delete_test_credentials(&config).await;
    assert!(deleted_epoch.value() > reconnected_epoch.value());
    assert_eq!(
        bilibili_settings_auth_snapshot().await["phase"],
        "connected",
        "a shared-protocol delete remains last-known state until Settings explicitly refreshes"
    );
    let external_delete_refresh =
        request_user_settings_snapshot_refresh(&runtime, &mut refresh_results, 3).await;
    assert_eq!(external_delete_refresh["status"], "published");
    let externally_deleted_snapshot = bilibili_settings_auth_snapshot().await;
    assert_eq!(externally_deleted_snapshot["phase"], "signed_out");
    assert_eq!(
        externally_deleted_snapshot["credential_revision"].as_u64(),
        Some(deleted_epoch.value())
    );
    assert_eq!(
        externally_deleted_snapshot["credential_summary"]["has_tv_access_key"],
        false
    );

    fs::write(
        &config.bilibili.auth.credential_file,
        b"{invalid credentials",
    )
    .expect("credential fixture should become unreadable");
    let unavailable_refresh =
        request_user_settings_snapshot_refresh(&runtime, &mut refresh_results, 4).await;
    assert_eq!(unavailable_refresh["status"], "failed");
    assert_eq!(
        unavailable_refresh["error_code"],
        "reference_refresh_failed"
    );
    let unavailable_snapshot = bilibili_settings_auth_snapshot().await;
    assert_eq!(unavailable_snapshot["phase"], "failed");
    assert_eq!(unavailable_snapshot["safe_reason"], "login_failed");
    assert_eq!(unavailable_snapshot["credential_revision"], 0);
    assert_eq!(
        unavailable_snapshot["credential_summary"],
        json!({"has_cookie": false, "has_access_key": false, "has_tv_access_key": false}),
        "unreadable credential state must not publish its cached summary as current"
    );
    let unavailable_logout = dispatch_auth_request(
        &runtime,
        96,
        json!({
            "operation": "logout",
            "generation": unavailable_snapshot["generation"],
            "credential_revision": deleted_epoch.value(),
        }),
    )
    .await;
    assert_eq!(unavailable_logout.status, InboxStatus::Failed);
    assert_eq!(
        unavailable_logout.error.as_deref(),
        Some("Bilibili credential state could not be verified locally")
    );
    assert_eq!(
        fs::read(&config.bilibili.auth.credential_file)
            .expect("unreadable fixture should remain present"),
        b"{invalid credentials",
        "unverified cached state must not authorize a credential mutation"
    );
    fs::remove_file(&config.bilibili.auth.credential_file)
        .expect("test should repair the unreadable fixture before later login cases");
    let repaired_epoch = externally_delete_test_credentials(&config).await;
    let repaired_refresh =
        request_user_settings_snapshot_refresh(&runtime, &mut refresh_results, 5).await;
    assert_eq!(repaired_refresh["status"], "published");
    let repaired_snapshot = bilibili_settings_auth_snapshot().await;
    assert_eq!(repaired_snapshot["phase"], "signed_out");
    assert_eq!(
        repaired_snapshot["credential_revision"].as_u64(),
        Some(repaired_epoch.value()),
        "a later successful refresh should reconcile the repaired credential store"
    );
    runtime.viewer_interests.lock().await.clear();
    let posts_before_closed_auth = cloud_state
        .state_posts
        .lock()
        .expect("fake cloud post lock should not poison")
        .len();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake cloud post lock should not poison")
            .len(),
        posts_before_closed_auth,
        "credential refreshes while Settings is closed should keep idle throttling"
    );

    runtime.viewer_interests.lock().await.insert(
        SETTINGS_VIEWER_ID.to_string(),
        CloudViewerInterest {
            viewer_id: SETTINGS_VIEWER_ID.to_string(),
            surface: CloudViewerSurface::Settings,
            watch: false,
            expires_at: now_millis().saturating_add(60_000),
        },
    );
    let start_tv =
        dispatch_auth_request(&runtime, 1, json!({"operation": "login", "mode": "tv"})).await;
    assert_eq!(start_tv.status, InboxStatus::Completed);
    assert_eq!(start_tv.result.as_ref().unwrap()["status"], "accepted");
    let first_generation = start_tv.result.as_ref().unwrap()["generation"]
        .as_u64()
        .expect("start receipt should include a safe generation");
    wait_for_bilibili_poll(
        &mut polls,
        "tv:3",
        "open Settings saved-credentials lifecycle",
    )
    .await;
    let waiting = wait_for_auth_phase("waiting_for_scan", first_generation).await;
    assert_eq!(waiting["credential_summary"]["has_tv_access_key"], false);
    let (waiting_publication, waiting_post) =
        wait_for_cloud_auth_publication(&cloud_state, "waiting_for_scan", 2).await;
    assert_eq!(
        waiting_post["snapshot_notify_viewers"].as_str(),
        Some("1"),
        "an open Settings viewer should receive auth state changes"
    );
    assert_no_bilibili_secrets(&waiting_publication.to_string());
    let public_waiting = public_auth_projection(&runtime).await;
    assert_eq!(
        public_waiting["settings_applied"]["bilibili_auth"]["phase"],
        "waiting_for_scan"
    );

    let duplicate =
        dispatch_auth_request(&runtime, 2, json!({"operation": "login", "mode": "web"})).await;
    assert_eq!(duplicate.status, InboxStatus::Failed);
    assert_eq!(duplicate.result.as_ref().unwrap()["status"], "conflict");
    assert_eq!(
        BILIBILI_AUTH_GENERATION.load(Ordering::SeqCst),
        first_generation,
        "duplicate start must not replace the active generation"
    );
    assert_no_bilibili_secrets(&serde_json::to_string(&duplicate).unwrap());

    release_poll
        .send("tv:3".to_string())
        .expect("first mocked login poll should be released");
    let connected = wait_for_auth_phase("connected", first_generation).await;
    assert_eq!(connected["credential_summary"]["has_tv_access_key"], true);
    wait_for_login_lock_idle().await;
    let (connected_publication, connected_post) =
        wait_for_cloud_auth_publication(&cloud_state, "connected", 3).await;
    assert_eq!(
        connected_post["snapshot_notify_viewers"].as_str(),
        Some("1"),
        "a saved credential summary should be pushed to an open Settings viewer"
    );
    assert_no_bilibili_secrets(&connected_publication.to_string());
    let saved = bilibili_core::CredentialRuntime::from_credential_file(
        config.bilibili.auth.credential_file.clone(),
        config.bilibili.auth.credential_profile.as_deref(),
    )
    .expect("credential runtime should use the explicit private fixture path")
    .load()
    .expect("mock TV credentials should be saved locally");
    assert!(
        saved.tv_access_key.as_deref() == Some(TEST_BILIBILI_TV_ACCESS_KEY),
        "local credential store should contain the mock TV access key"
    );

    let start_web =
        dispatch_auth_request(&runtime, 3, json!({"operation": "login", "mode": "web"})).await;
    assert_eq!(start_web.result.as_ref().unwrap()["status"], "accepted");
    let web_generation = start_web.result.as_ref().unwrap()["generation"]
        .as_u64()
        .expect("web start should include a safe generation");
    wait_for_bilibili_poll(&mut polls, "web:1", "web QR confirmation lifecycle").await;
    let confirming = wait_for_auth_phase("waiting_for_confirmation", web_generation).await;
    let confirming_updated_at = confirming["updated_at_ms"].as_u64();
    let confirming_min_posts = cloud_state
        .state_posts
        .lock()
        .expect("fake cloud post lock should not poison")
        .len()
        + 1;
    let (confirming_publication, confirming_post) = wait_for_cloud_auth_publication(
        &cloud_state,
        "waiting_for_confirmation",
        confirming_min_posts,
    )
    .await;
    assert_eq!(
        confirming_post["snapshot_notify_viewers"].as_str(),
        Some("1")
    );
    assert_no_bilibili_secrets(&confirming_publication.to_string());
    let confirming_post_count = cloud_state
        .state_posts
        .lock()
        .expect("fake cloud post lock should not poison")
        .len();

    wait_for_bilibili_poll(&mut polls, "web:2", "web QR repeated confirmation poll").await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let repeated_confirmation = bilibili_settings_auth_snapshot().await;
    assert_eq!(
        repeated_confirmation["updated_at_ms"].as_u64(),
        confirming_updated_at,
        "repeated scan polls must not refresh phase timestamps"
    );
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake cloud post lock should not poison")
            .len(),
        confirming_post_count,
        "repeated confirmation polls must not create extra cloud publications"
    );

    wait_for_bilibili_poll(&mut polls, "web:3", "web QR expiration lifecycle").await;
    let expired = wait_for_auth_phase("expired", web_generation).await;
    assert_eq!(expired["credential_summary"]["has_tv_access_key"], true);
    wait_for_login_lock_idle().await;
    let (expired_publication, expired_post) =
        wait_for_cloud_auth_publication(&cloud_state, "expired", confirming_post_count + 1).await;
    assert_eq!(expired_post["snapshot_notify_viewers"].as_str(), Some("1"));
    assert_no_bilibili_secrets(&expired_publication.to_string());

    let posts_before_failed = cloud_state
        .state_posts
        .lock()
        .expect("fake cloud post lock should not poison")
        .len();
    let start_failed_web =
        dispatch_auth_request(&runtime, 4, json!({"operation": "login", "mode": "web"})).await;
    assert_eq!(
        start_failed_web.result.as_ref().unwrap()["status"],
        "accepted"
    );
    let failed_generation = start_failed_web.result.as_ref().unwrap()["generation"]
        .as_u64()
        .expect("failed web start should include a safe generation");
    wait_for_bilibili_poll(&mut polls, "web:4", "web QR failed lifecycle").await;
    let failed = wait_for_auth_phase("failed", failed_generation).await;
    assert_eq!(failed["credential_summary"]["has_tv_access_key"], true);
    wait_for_login_lock_idle().await;
    let (failed_publication, failed_post) =
        wait_for_cloud_auth_publication(&cloud_state, "failed", posts_before_failed + 1).await;
    assert_eq!(failed_post["snapshot_notify_viewers"].as_str(), Some("1"));
    assert_no_bilibili_secrets(&failed_publication.to_string());
    assert_no_bilibili_secrets(&public_auth_projection(&runtime).await.to_string());

    runtime.viewer_interests.lock().await.clear();
    let posts_before_closed_cancel = cloud_state
        .state_posts
        .lock()
        .expect("fake cloud post lock should not poison")
        .len();
    let start_cancelled =
        dispatch_auth_request(&runtime, 5, json!({"operation": "login", "mode": "tv"})).await;
    let cancelled_generation = start_cancelled.result.as_ref().unwrap()["generation"]
        .as_u64()
        .expect("cancelled login should include a safe generation");
    wait_for_bilibili_poll(&mut polls, "tv:4", "closed Settings cancellation lifecycle").await;
    let cancelled_revision = start_cancelled.result.as_ref().unwrap()["credential_revision"]
        .as_u64()
        .expect("cancel target should include a durable credential epoch");
    let cancel = dispatch_auth_request(
        &runtime,
        6,
        json!({
            "operation": "cancel",
            "generation": cancelled_generation,
            "credential_revision": cancelled_revision,
        }),
    )
    .await;
    assert_eq!(cancel.result.as_ref().unwrap()["status"], "completed");
    let cancelled_state_generation = cancel.result.as_ref().unwrap()["generation"]
        .as_u64()
        .expect("cancel receipt should include a safe generation");
    let cancelled = wait_for_auth_phase("cancelled", cancelled_state_generation).await;
    assert_eq!(cancelled["credential_summary"]["has_tv_access_key"], true);
    release_poll
        .send("tv:4".to_string())
        .expect("cancelled stale poll should be released");
    wait_for_login_lock_idle().await;
    let after_cancelled_stale_completion = bilibili_settings_auth_snapshot().await;
    assert_eq!(after_cancelled_stale_completion["phase"], "cancelled");
    assert_eq!(
        after_cancelled_stale_completion["credential_summary"]["has_tv_access_key"], true,
        "a stale poll after cancel must preserve the prior credential summary"
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake cloud post lock should not poison")
            .len(),
        posts_before_closed_cancel,
        "auth phase changes while Settings is closed should retain snapshot throttling"
    );

    let start_logout_race =
        dispatch_auth_request(&runtime, 7, json!({"operation": "login", "mode": "tv"})).await;
    let logout_race_generation = start_logout_race.result.as_ref().unwrap()["generation"]
        .as_u64()
        .expect("logout-race login should include a safe generation");
    wait_for_bilibili_poll(&mut polls, "tv:5", "closed Settings logout lifecycle").await;
    let logout_revision = start_logout_race.result.as_ref().unwrap()["credential_revision"]
        .as_u64()
        .expect("logout-race login should include a durable credential epoch");
    let logout = dispatch_auth_request(
        &runtime,
        8,
        json!({
            "operation": "logout",
            "generation": logout_race_generation,
            "credential_revision": logout_revision,
        }),
    )
    .await;
    assert_eq!(logout.result.as_ref().unwrap()["status"], "completed");
    let logged_out_generation = logout.result.as_ref().unwrap()["generation"]
        .as_u64()
        .expect("logout receipt should include a safe generation");
    let logged_out_revision = logout.result.as_ref().unwrap()["credential_revision"]
        .as_u64()
        .expect("logout receipt should include a safe revision");
    let logged_out = wait_for_auth_phase("signed_out", logged_out_generation).await;
    assert_eq!(
        logged_out["credential_revision"].as_u64(),
        Some(logged_out_revision)
    );
    assert_eq!(logged_out["credential_summary"]["has_tv_access_key"], false);
    release_poll
        .send("tv:5".to_string())
        .expect("logout stale poll should be released");
    wait_for_login_lock_idle().await;
    let after_logout_stale_completion = bilibili_settings_auth_snapshot().await;
    assert_eq!(after_logout_stale_completion["phase"], "signed_out");
    assert_eq!(
        after_logout_stale_completion["credential_summary"]["has_tv_access_key"], false,
        "a stale poll after logout must not restore credentials"
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        cloud_state
            .state_posts
            .lock()
            .expect("fake cloud post lock should not poison")
            .len(),
        posts_before_closed_cancel,
        "logout and late auth updates while Settings is closed should not publish eagerly"
    );

    let credentials_after_logout = bilibili_core::CredentialRuntime::from_credential_file(
        config.bilibili.auth.credential_file.clone(),
        config.bilibili.auth.credential_profile.as_deref(),
    )
    .expect("credential runtime should retain the explicit private fixture path")
    .load()
    .expect("logout should leave a readable empty credential store");
    assert!(credentials_after_logout.tv_access_key.is_none());

    let history = public_auth_projection(&runtime).await;
    assert_no_bilibili_secrets(&history.to_string());
    for seq in [
        1, 2, 3, 4, 5, 6, 7, 8, 79, 80, 81, 82, 83, 84, 90, 91, 92, 96,
    ] {
        let record = inbox
            .record(seq)
            .expect("durable auth history should load")
            .expect("durable auth history should remain available");
        assert_no_bilibili_secrets(&serde_json::to_string(&record).unwrap());
        if record.request.kind == "bilibili_auth" {
            if let Some(result) = &record.result {
                assert!(result.get("request_id").is_none());
            }
            if seq == 79 {
                assert_eq!(
                    record.error.as_deref(),
                    Some("invalid Bilibili auth request fields")
                );
            } else if seq == 96 {
                assert_eq!(
                    record.error.as_deref(),
                    Some("Bilibili credential state could not be verified locally")
                );
            } else {
                assert!(record.error.is_none());
            }
        }
    }

    let telegram_requests: Vec<FakeTelegramRequest> =
        take_cloud_test_telegram_requests(&mut telegram_requests);
    let photos = telegram_requests
        .iter()
        .filter(|request| request.method == "sendPhoto")
        .collect::<Vec<_>>();
    assert_eq!(photos.len(), 7, "each login start should send one QR photo");
    for request in &telegram_requests {
        if request.method == "sendPhoto" || request.method == "sendMessage" {
            let chat_id = request.body["chat_id"].as_i64().or_else(|| {
                request.body["chat_id"]
                    .as_str()
                    .and_then(|value| value.parse::<i64>().ok())
            });
            assert_eq!(chat_id, Some(OWNER_CHAT_ID));
        }
        assert_no_bilibili_secrets(&request.body.to_string());
    }
    let redacted_json_error =
        redact_sensitive_text(&json!({"access_token": TEST_BILIBILI_ERROR_TOKEN}).to_string());
    assert!(
        !redacted_json_error.contains(TEST_BILIBILI_ERROR_TOKEN),
        "auth error log sanitizer should redact structured credentials"
    );
    let public_body_text = cloud_state
        .state_bodies
        .lock()
        .expect("fake cloud body lock should not poison")
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert_no_bilibili_secrets(&public_body_text);

    dispatcher.abort();
    let _ = dispatcher.await;
    reporter.abort();
    let _ = reporter.await;
    stop_fake_bilibili_api(bilibili_shutdown, bilibili_server).await;
    stop_fake_cloud_api(cloud_shutdown, cloud_server).await;
    stop_fake_telegram_api(telegram_shutdown, telegram_server).await;
    let _ = fs::remove_dir_all(root);
}
