use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use anyhow::{Result, bail};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::*;
use crate::cloud_bot_tests::media_library_e2e_tests::{MigrationFixture, create_migration_fixture};
use crate::cloud_bot_tests::{
    FakeTelegramRequest, cloud_test_root_binding, make_runtime, open_runtime_test_inbox,
    run_library_management_test_request, spawn_fake_telegram_api, stop_fake_telegram_api,
    take_cloud_test_telegram_requests,
};

struct DanmakuRefreshHarness {
    fixture: MigrationFixture,
    library_id: String,
    root_binding: String,
    runtime: Option<CloudBotRuntime>,
    dispatcher: Option<JoinHandle<()>>,
    telegram_requests: Option<tokio::sync::mpsc::UnboundedReceiver<FakeTelegramRequest>>,
    telegram_shutdown: Option<oneshot::Sender<()>>,
    telegram_server: Option<JoinHandle<Result<()>>>,
}

struct FetchCancellationProbe(Arc<AtomicBool>);

impl Drop for FetchCancellationProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl DanmakuRefreshHarness {
    async fn drain_dispatch(&self) {
        // Durable completion is visible just before the detached request child
        // releases its runtime clone and management permit.
        let runtime = self.runtime.as_ref().expect("runtime should be open");
        let permit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            runtime.management_dispatch.acquire(),
        )
        .await
        .expect("management child should finish before fixture shutdown")
        .expect("management dispatch semaphore should remain open");
        drop(permit);
    }

    async fn close(mut self) {
        if let Some(dispatcher) = self.dispatcher.take() {
            dispatcher.abort();
            let _ = dispatcher.await;
        }
        self.drain_dispatch().await;
        if let (Some(shutdown), Some(server)) =
            (self.telegram_shutdown.take(), self.telegram_server.take())
        {
            stop_fake_telegram_api(shutdown, server).await;
        }
        drop(self.runtime.take());
        let _ = fs::remove_dir_all(self.fixture.root);
    }

    async fn request(&self, seq: u64, payload: Value) -> InboxRecord {
        run_library_management_test_request(
            self.runtime.as_ref().expect("runtime should be open"),
            seq,
            "danmaku_refresh",
            payload,
        )
        .await
    }

    async fn scan(&self, seq: u64) -> Value {
        let record = run_library_management_test_request(
            self.runtime.as_ref().expect("runtime should be open"),
            seq,
            "library_scan",
            json!({"library_id": self.library_id}),
        )
        .await;
        assert_eq!(record.status, InboxStatus::Completed, "{:?}", record.error);
        record
            .result
            .expect("library scan should return its snapshot")
    }

    fn bound(&self, operation: &str) -> Value {
        json!({
            "operation": operation,
            "library_id": self.library_id,
            "root_binding": self.root_binding,
        })
    }

    fn preview_reference(&self, seq: u64, preview: &Value, operation: &str) -> Value {
        let mut payload = self.bound(operation);
        payload["preview_seq"] = json!(seq);
        payload["preview_id"] = preview["id"].clone();
        payload["revision"] = preview["revision"].clone();
        payload
    }

    async fn restart(&mut self, durable_seq: u64) -> InboxRecord {
        if let Some(dispatcher) = self.dispatcher.take() {
            dispatcher.abort();
            let _ = dispatcher.await;
        }
        self.drain_dispatch().await;
        let (config, telegram, queue, fetcher) = {
            let runtime = self.runtime.as_ref().expect("runtime should be open");
            (
                (*runtime.bot.config).clone(),
                runtime.bot.telegram.clone(),
                Arc::clone(&runtime.bot.queue),
                runtime.test_danmaku_fetcher.clone(),
            )
        };
        drop(self.runtime.take());

        let inbox = Arc::new(
            open_runtime_test_inbox(&self.fixture.video_root)
                .expect("same-epoch inbox should reopen after refresh result persisted"),
        );
        let persisted = inbox
            .record(durable_seq)
            .expect("durable refresh receipt should load")
            .expect("durable refresh receipt should remain present");
        let mut runtime = make_runtime(telegram, config, queue, inbox);
        runtime.test_danmaku_fetcher = fetcher;
        self.runtime = Some(runtime);
        self.dispatcher = Some(tokio::spawn(cloud_request_dispatcher(
            self.runtime.as_ref().unwrap().clone(),
        )));
        persisted
    }
}

async fn danmaku_refresh_harness(
    label: &str,
    videos: &[(&str, &str, &str)],
    fetcher: crate::danmaku_refresh::TestDanmakuFetcher,
) -> DanmakuRefreshHarness {
    let (fixture, queue, inbox) =
        create_migration_fixture(label, crate::cloud::TEST_DATABASE_EPOCH);
    for (relative_path, title, cid) in videos {
        let media = fixture.archive_root.join(relative_path);
        fs::create_dir_all(
            media
                .parent()
                .expect("fixture media path should have a parent"),
        )
        .expect("fixture media parent should create");
        fs::write(&media, b"synthetic small media fixture").expect("fixture media should write");
        fs::write(media.with_extension("nfo"), source_nfo(title, cid))
            .expect("fixture NFO should write");
    }

    let (telegram, telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut runtime = make_runtime(telegram, fixture.config.clone(), queue, inbox);
    runtime.test_danmaku_fetcher = Some(fetcher);
    let library_id = fixture.target_library_id.clone();
    let root_binding = cloud_test_root_binding(&runtime.bot.queue, &library_id);
    let dispatcher = Some(tokio::spawn(cloud_request_dispatcher(runtime.clone())));

    DanmakuRefreshHarness {
        fixture,
        library_id,
        root_binding,
        runtime: Some(runtime),
        dispatcher,
        telegram_requests: Some(telegram_requests),
        telegram_shutdown: Some(telegram_shutdown),
        telegram_server: Some(telegram_server),
    }
}

fn source_nfo(title: &str, cid: &str) -> String {
    let cid = if cid.is_empty() {
        String::new()
    } else {
        format!("<uniqueid type=\"bilibili-cid\">{cid}</uniqueid>")
    };
    format!(
        "<movie><title>{title}</title><uniqueid type=\"bilibili\">BV1ab411C7x1</uniqueid>{cid}<set><name>Series Alpha</name></set><setid>42</setid><settype>season</settype><episode>1</episode></movie>"
    )
}

fn danmaku_xml(comments: &[(&str, &str)]) -> Vec<u8> {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><i><chatserver>comment.bilibili.com</chatserver><chatid>123456789</chatid><mission>0</mission><maxlimit>1000</maxlimit><state>0</state><real_name>0</real_name><source>k-v</source>",
    );
    for (position, text) in comments {
        xml.push_str(&format!(
            "<d p=\"{position},1,25,16777215,1700000000,0,userhash,0\">{text}</d>"
        ));
    }
    xml.push_str("</i>");
    xml.into_bytes()
}

fn snapshot_item_id(snapshot: &Value, relative_path: &str) -> String {
    snapshot["items"]
        .as_array()
        .expect("library snapshot should contain items")
        .iter()
        .find(|item| item["relative_path"] == relative_path)
        .and_then(|item| item["id"].as_str())
        .expect("requested media should appear in library snapshot")
        .to_string()
}

fn public_preview(record: &InboxRecord) -> &Value {
    record
        .result
        .as_ref()
        .and_then(|result| result.get("preview"))
        .expect("refresh preview should be returned")
}

fn read_sidecar(root: &Path, relative_path: &str) -> Vec<u8> {
    fs::read(root.join(relative_path).with_extension("xml"))
        .expect("danmaku XML sidecar should remain readable")
}

fn confirmation_payload(
    harness: &DanmakuRefreshHarness,
    preview_seq: u64,
    preview: &Value,
) -> Value {
    harness.preview_reference(preview_seq, preview, "confirm")
}

fn test_fetcher(xml: Vec<u8>) -> crate::danmaku_refresh::TestDanmakuFetcher {
    crate::danmaku_refresh::TestDanmakuFetcher::new(Arc::new(move |_cid| {
        let xml = xml.clone();
        Box::pin(async move { Ok(xml) })
    }))
}

#[tokio::test]
async fn danmaku_refresh_selected_cancel_confirm_repeat_and_restart_preserve_comments_e2e() {
    let old_xml = danmaku_xml(&[
        ("1.0", "retained after provider deletion"),
        ("2.0", "already present"),
    ]);
    let remote_xml = danmaku_xml(&[("2.0", "already present"), ("3.0", "new comment")]);
    let fetcher = test_fetcher(remote_xml);
    let mut harness = danmaku_refresh_harness(
        "danmaku-refresh-selected",
        &[("Series Alpha/episode.mp4", "Episode one", "123456789")],
        fetcher,
    )
    .await;
    let media = harness
        .fixture
        .archive_root
        .join("Series Alpha/episode.mp4");
    let original_media = fs::read(&media).expect("synthetic media should be readable");
    fs::write(media.with_extension("xml"), &old_xml).expect("existing comments should write");

    let snapshot = harness.scan(1).await;
    let item_id = snapshot_item_id(&snapshot, "Series Alpha/episode.mp4");
    let preview_record = harness
        .request(
            2,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "selected", "item_ids": [item_id],
            }),
        )
        .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = public_preview(&preview_record).clone();
    assert_eq!(preview["scope"], "selected");
    assert_eq!(preview["ready"], 1);
    assert_eq!(preview["skipped"], 0);
    assert_eq!(
        read_sidecar(&harness.fixture.archive_root, "Series Alpha/episode.mp4"),
        old_xml
    );
    let cancelled = harness
        .request(3, harness.preview_reference(2, &preview, "cancel"))
        .await;
    assert_eq!(
        cancelled.status,
        InboxStatus::Completed,
        "{:?}",
        cancelled.error
    );
    assert_eq!(
        read_sidecar(&harness.fixture.archive_root, "Series Alpha/episode.mp4"),
        old_xml
    );

    let snapshot = harness.scan(4).await;
    let item_id = snapshot_item_id(&snapshot, "Series Alpha/episode.mp4");
    let preview_record = harness
        .request(
            5,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "selected", "item_ids": [item_id],
            }),
        )
        .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = public_preview(&preview_record).clone();
    let confirmed = harness
        .request(6, confirmation_payload(&harness, 5, &preview))
        .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    let result = confirmed
        .result
        .as_ref()
        .expect("confirmation should return a summary");
    assert_eq!(result["media_files_changed"], false);
    assert_eq!(result["summary"]["added_comments"], 1);
    let merged = String::from_utf8(read_sidecar(
        &harness.fixture.archive_root,
        "Series Alpha/episode.mp4",
    ))
    .expect("merged comments should be UTF-8");
    for comment in [
        "retained after provider deletion",
        "already present",
        "new comment",
    ] {
        assert!(
            merged.contains(comment),
            "merged XML should contain {comment:?}"
        );
    }
    assert_eq!(
        merged.matches("already present").count(),
        1,
        "remote duplicate should deduplicate"
    );
    assert_eq!(
        fs::read(&media).unwrap(),
        original_media,
        "refresh must not redownload or rewrite media"
    );

    let reopened = harness.restart(6).await;
    assert_eq!(reopened.status, InboxStatus::Completed);
    assert_eq!(
        reopened.result, confirmed.result,
        "durable refresh receipt should survive inbox reopen"
    );
    let snapshot = harness.scan(7).await;
    let item_id = snapshot_item_id(&snapshot, "Series Alpha/episode.mp4");
    let preview_record = harness
        .request(
            8,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "selected", "item_ids": [item_id],
            }),
        )
        .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let repeated_preview = public_preview(&preview_record).clone();
    let repeated = harness
        .request(9, confirmation_payload(&harness, 8, &repeated_preview))
        .await;
    assert_eq!(
        repeated.status,
        InboxStatus::Completed,
        "{:?}",
        repeated.error
    );
    assert_eq!(
        repeated.result.as_ref().unwrap()["summary"]["added_comments"],
        0
    );
    let repeated_xml = String::from_utf8(read_sidecar(
        &harness.fixture.archive_root,
        "Series Alpha/episode.mp4",
    ))
    .expect("repeated refresh XML should be UTF-8");
    assert_eq!(repeated_xml.matches("already present").count(), 1);
    assert_eq!(repeated_xml.matches("new comment").count(), 1);
    assert!(
        take_cloud_test_telegram_requests(
            harness
                .telegram_requests
                .as_mut()
                .expect("Telegram request capture should remain open")
        )
        .is_empty(),
        "danmaku refresh must not send Telegram messages"
    );
    harness.close().await;
}

#[tokio::test]
async fn danmaku_refresh_new_sidecars_are_owned_by_the_scanned_media_e2e() {
    let fetch_calls = Arc::new(AtomicUsize::new(0));
    let fetcher = {
        let fetch_calls = Arc::clone(&fetch_calls);
        let remote_xml = danmaku_xml(&[("1.0", "new comment")]);
        crate::danmaku_refresh::TestDanmakuFetcher::new(Arc::new(move |_cid| {
            let fetch_calls = Arc::clone(&fetch_calls);
            let remote_xml = remote_xml.clone();
            Box::pin(async move {
                fetch_calls.fetch_add(1, Ordering::SeqCst);
                Ok(remote_xml)
            })
        }))
    };
    let harness = danmaku_refresh_harness(
        "danmaku-refresh-new-sidecar-owner",
        &[("Series/episode.mp4", "Episode one", "123456789")],
        fetcher,
    )
    .await;
    let snapshot = harness.scan(1).await;
    let preview_record = harness
        .request(
            2,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "library",
            }),
        )
        .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = public_preview(&preview_record).clone();
    assert_eq!(preview["ready"], 1);
    let confirmed = harness
        .request(3, confirmation_payload(&harness, 2, &preview))
        .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    harness.drain_dispatch().await;
    assert_eq!(fetch_calls.load(Ordering::SeqCst), 1);

    let refreshed = harness.scan(4).await;
    let episode = refreshed["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["relative_path"] == "Series/episode.mp4")
        .expect("the episode should remain in the library");
    for attachment in ["Series/episode.danmaku.xml", "Series/episode.danmaku.ass"] {
        assert!(
            episode["attachments"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["relative_path"] == attachment),
            "a fresh scan should assign {attachment} to the media that created it"
        );
    }
    harness.close().await;
}

#[tokio::test]
async fn danmaku_refresh_rejects_prefix_owned_sidecar_without_fetching_e2e() {
    let fetch_calls = Arc::new(AtomicUsize::new(0));
    let fetcher = {
        let fetch_calls = Arc::clone(&fetch_calls);
        let remote_xml = danmaku_xml(&[("1.0", "must not be fetched")]);
        crate::danmaku_refresh::TestDanmakuFetcher::new(Arc::new(move |_cid| {
            let fetch_calls = Arc::clone(&fetch_calls);
            let remote_xml = remote_xml.clone();
            Box::pin(async move {
                fetch_calls.fetch_add(1, Ordering::SeqCst);
                Ok(remote_xml)
            })
        }))
    };
    let harness = danmaku_refresh_harness(
        "danmaku-refresh-prefix-owner",
        &[
            ("episode.mp4", "Episode", "123456789"),
            ("episode.danmaku.mp4", "Episode Danmaku", "987654321"),
        ],
        fetcher,
    )
    .await;
    let snapshot = harness.scan(1).await;
    let episode_id = snapshot_item_id(&snapshot, "episode.mp4");
    let preview_record = harness
        .request(
            2,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "selected", "item_ids": [episode_id],
            }),
        )
        .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = public_preview(&preview_record).clone();
    assert_eq!(preview["ready"], 0);
    assert_eq!(preview["skipped"], 1);
    assert!(
        preview["items"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("ownership")
    );

    let confirmed = harness
        .request(3, confirmation_payload(&harness, 2, &preview))
        .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    harness.drain_dispatch().await;
    assert_eq!(fetch_calls.load(Ordering::SeqCst), 0);
    for sidecar in ["episode.danmaku.xml", "episode.danmaku.ass"] {
        assert!(
            !harness.fixture.archive_root.join(sidecar).exists(),
            "a conflicting output {sidecar} must not be created"
        );
    }
    let refreshed = harness.scan(4).await;
    for path in ["episode.mp4", "episode.danmaku.mp4"] {
        let item = refreshed["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["relative_path"] == path)
            .expect("both videos should remain indexed");
        assert!(
            !item["attachments"]
                .as_array()
                .unwrap()
                .iter()
                .any(|attachment| {
                    matches!(
                        attachment["relative_path"].as_str(),
                        Some("episode.danmaku.xml" | "episode.danmaku.ass")
                    )
                })
        );
    }
    harness.close().await;
}

#[tokio::test]
async fn danmaku_refresh_library_preview_reports_unsupported_items_and_collection_rows_e2e() {
    let remote = danmaku_xml(&[("1.0", "collection comment")]);
    let harness = danmaku_refresh_harness(
        "danmaku-refresh-library-scope",
        &[
            ("Series Alpha/episode.mp4", "Episode one", "123456789"),
            ("loose.mp4", "Unsupported video", ""),
        ],
        test_fetcher(remote),
    )
    .await;
    let collection_media = harness
        .fixture
        .archive_root
        .join("Series Alpha/episode.mp4");
    let original_media = fs::read(&collection_media).expect("media should read");
    let snapshot = harness.scan(1).await;
    let preview_record = harness
        .request(
            2,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "library",
            }),
        )
        .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = public_preview(&preview_record);
    assert_eq!(preview["scope"], "library");
    assert_eq!(preview["ready"], 1);
    assert_eq!(preview["skipped"], 1);
    let rows = preview["items"]
        .as_array()
        .expect("preview should list items");
    assert!(rows.iter().any(
        |item| item["relative_path"] == "Series Alpha/episode.mp4" && item["status"] == "ready"
    ));
    assert!(
        rows.iter()
            .any(|item| item["relative_path"] == "loose.mp4" && item["status"] == "skipped")
    );
    assert_eq!(preview["truncated"], false);
    assert!(
        !collection_media.with_extension("xml").exists(),
        "preview must not write comments"
    );
    assert_eq!(fs::read(&collection_media).unwrap(), original_media);

    let cancelled = harness
        .request(3, harness.preview_reference(2, preview, "cancel"))
        .await;
    assert_eq!(
        cancelled.status,
        InboxStatus::Completed,
        "{:?}",
        cancelled.error
    );
    assert!(!collection_media.with_extension("xml").exists());
    harness.close().await;
}

#[tokio::test]
async fn danmaku_refresh_deadline_keeps_completed_items_and_defers_remaining_e2e() {
    let remote_xml = danmaku_xml(&[("1.0", "new comment")]);
    let fetch_calls = Arc::new(AtomicUsize::new(0));
    let cancelled_pending_fetch = Arc::new(AtomicBool::new(false));
    let fetcher = {
        let fetch_calls = Arc::clone(&fetch_calls);
        let cancelled_pending_fetch = Arc::clone(&cancelled_pending_fetch);
        let remote_xml = remote_xml.clone();
        crate::danmaku_refresh::TestDanmakuFetcher::with_refresh_duration(
            Arc::new(move |_cid| {
                let call = fetch_calls.fetch_add(1, Ordering::SeqCst);
                let cancelled_pending_fetch = Arc::clone(&cancelled_pending_fetch);
                let remote_xml = remote_xml.clone();
                Box::pin(async move {
                    if call == 1 {
                        let _probe = FetchCancellationProbe(cancelled_pending_fetch);
                        std::future::pending::<anyhow::Result<Vec<u8>>>().await
                    } else {
                        Ok(remote_xml)
                    }
                })
            }),
            std::time::Duration::from_secs(3),
        )
    };
    let mut harness = danmaku_refresh_harness(
        "danmaku-refresh-deadline",
        &[
            ("episode-1.mp4", "Episode one", "123456789"),
            ("episode-2.mp4", "Episode two", "123456789"),
            ("episode-3.mp4", "Episode three", "123456789"),
        ],
        fetcher,
    )
    .await;
    let media_paths = ["episode-1.mp4", "episode-2.mp4", "episode-3.mp4"];
    let old_xml = [
        danmaku_xml(&[("0.5", "first retained")]),
        danmaku_xml(&[("0.5", "second retained")]),
        danmaku_xml(&[("0.5", "third retained")]),
    ];
    for (path, xml) in media_paths.iter().zip(&old_xml) {
        fs::write(
            harness
                .fixture
                .archive_root
                .join(path)
                .with_extension("xml"),
            xml,
        )
        .expect("existing sidecar should write");
    }
    let original_media = media_paths
        .iter()
        .map(|path| fs::read(harness.fixture.archive_root.join(path)).unwrap())
        .collect::<Vec<_>>();

    let snapshot = harness.scan(1).await;
    let preview_record = harness
        .request(
            2,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "library",
            }),
        )
        .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = public_preview(&preview_record).clone();
    assert_eq!(preview["ready"], 3);
    let preview_rows = preview["items"].as_array().unwrap();
    let item_ids = media_paths
        .iter()
        .map(|path| {
            preview_rows
                .iter()
                .find(|item| item["relative_path"] == *path)
                .and_then(|item| item["item_id"].as_str())
                .expect("preview should contain each requested video")
                .to_owned()
        })
        .collect::<Vec<_>>();

    let confirmed = harness
        .request(3, confirmation_payload(&harness, 2, &preview))
        .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    harness.drain_dispatch().await;
    assert!(cancelled_pending_fetch.load(Ordering::SeqCst));
    assert_eq!(fetch_calls.load(Ordering::SeqCst), 2);
    let result = confirmed.result.as_ref().unwrap();
    assert_eq!(result["summary"]["updated"], 1);
    assert_eq!(result["summary"]["skipped"], 2);
    assert_eq!(result["summary"]["added_comments"], 1);
    let result_rows = result["items"].as_array().unwrap();
    let first_result = result_rows
        .iter()
        .find(|item| item["item_id"] == item_ids[0])
        .expect("first completed item should appear in the result");
    assert_eq!(first_result["status"], "updated");
    assert_eq!(first_result["deferred"], false);
    for item_id in &item_ids[1..] {
        let deferred = result_rows
            .iter()
            .find(|item| item["item_id"] == *item_id)
            .expect("remaining item should appear in the result");
        assert_eq!(deferred["status"], "skipped");
        assert_eq!(deferred["deferred"], true);
    }
    for (index, path) in media_paths.iter().enumerate().skip(1) {
        assert_eq!(
            read_sidecar(&harness.fixture.archive_root, path),
            old_xml[index],
            "a deferred item's existing sidecar must remain unchanged"
        );
    }
    for (path, original) in media_paths.iter().zip(&original_media) {
        assert_eq!(
            fs::read(harness.fixture.archive_root.join(path)).unwrap(),
            *original
        );
    }
    assert!(
        String::from_utf8(read_sidecar(&harness.fixture.archive_root, media_paths[0]))
            .unwrap()
            .contains("new comment")
    );

    harness.runtime.as_mut().unwrap().test_danmaku_fetcher = Some(test_fetcher(remote_xml));
    let replayed = harness.restart(3).await;
    assert_eq!(replayed.status, InboxStatus::Completed);
    assert_eq!(replayed.result, confirmed.result);
    let snapshot = harness.scan(4).await;
    let preview_record = harness
        .request(
            5,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "library",
            }),
        )
        .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = public_preview(&preview_record).clone();
    let confirmed = harness
        .request(6, confirmation_payload(&harness, 5, &preview))
        .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    let result = confirmed.result.as_ref().unwrap();
    assert_eq!(result["summary"]["updated"], 2);
    assert_eq!(result["summary"]["unchanged"], 1);
    assert_eq!(result["summary"]["added_comments"], 2);
    let first_xml =
        String::from_utf8(read_sidecar(&harness.fixture.archive_root, media_paths[0])).unwrap();
    assert_eq!(first_xml.matches("new comment").count(), 1);
    for (index, path) in media_paths.iter().enumerate().skip(1) {
        let updated_xml =
            String::from_utf8(read_sidecar(&harness.fixture.archive_root, path)).unwrap();
        assert!(updated_xml.contains(["second retained", "third retained"][index - 1]));
        assert_eq!(updated_xml.matches("new comment").count(), 1);
    }
    for (path, original) in media_paths.iter().zip(&original_media) {
        assert_eq!(
            fs::read(harness.fixture.archive_root.join(path)).unwrap(),
            *original
        );
    }
    harness.close().await;
}

#[tokio::test]
async fn danmaku_refresh_between_items_deadline_keeps_completed_items_and_defers_remaining_e2e() {
    let remote_xml = danmaku_xml(&[("1.0", "new comment")]);
    let fetch_calls = Arc::new(AtomicUsize::new(0));
    let fetcher = {
        let fetch_calls = Arc::clone(&fetch_calls);
        let remote_xml = remote_xml.clone();
        crate::danmaku_refresh::TestDanmakuFetcher::expiring_after_first_successful_publish(
            Arc::new(move |_cid| {
                let fetch_calls = Arc::clone(&fetch_calls);
                let remote_xml = remote_xml.clone();
                Box::pin(async move {
                    fetch_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(remote_xml)
                })
            }),
        )
    };
    let deadline_probe = fetcher.clone();
    let harness = danmaku_refresh_harness(
        "danmaku-refresh-between-items-deadline",
        &[
            ("episode-1.mp4", "Episode one", "123456789"),
            ("episode-2.mp4", "Episode two", "123456789"),
            ("episode-3.mp4", "Episode three", "123456789"),
        ],
        fetcher,
    )
    .await;
    let media_paths = ["episode-1.mp4", "episode-2.mp4", "episode-3.mp4"];
    let old_xml = [
        danmaku_xml(&[("0.5", "first retained")]),
        danmaku_xml(&[("0.5", "second retained")]),
        danmaku_xml(&[("0.5", "third retained")]),
    ];
    for (path, xml) in media_paths.iter().zip(&old_xml) {
        fs::write(
            harness
                .fixture
                .archive_root
                .join(path)
                .with_extension("xml"),
            xml,
        )
        .expect("existing sidecar should write");
    }
    let original_media = media_paths
        .iter()
        .map(|path| fs::read(harness.fixture.archive_root.join(path)).unwrap())
        .collect::<Vec<_>>();
    let original_ass = media_paths
        .iter()
        .map(|path| {
            fs::read(
                harness
                    .fixture
                    .archive_root
                    .join(path)
                    .with_extension("ass"),
            )
            .ok()
        })
        .collect::<Vec<_>>();

    let snapshot = harness.scan(1).await;
    let preview_record = harness
        .request(
            2,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "library",
            }),
        )
        .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = public_preview(&preview_record).clone();
    assert_eq!(preview["ready"], 3);
    let preview_rows = preview["items"].as_array().unwrap();
    let item_ids = media_paths
        .iter()
        .map(|path| {
            preview_rows
                .iter()
                .find(|item| item["relative_path"] == *path)
                .and_then(|item| item["item_id"].as_str())
                .expect("preview should contain each requested video")
                .to_owned()
        })
        .collect::<Vec<_>>();

    let confirmed = harness
        .request(3, confirmation_payload(&harness, 2, &preview))
        .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    harness.drain_dispatch().await;
    assert_eq!(fetch_calls.load(Ordering::SeqCst), 1);
    assert_eq!(deadline_probe.between_item_deadline_hit_count(), 1);
    let result = confirmed.result.as_ref().unwrap();
    assert_eq!(result["summary"]["updated"], 1);
    assert_eq!(result["summary"]["skipped"], 2);
    assert_eq!(result["summary"]["added_comments"], 1);
    let result_rows = result["items"].as_array().unwrap();
    let first_result = result_rows
        .iter()
        .find(|item| item["item_id"] == item_ids[0])
        .expect("first completed item should appear in the result");
    assert_eq!(first_result["status"], "updated");
    assert_eq!(first_result["deferred"], false);
    for item_id in &item_ids[1..] {
        let deferred = result_rows
            .iter()
            .find(|item| item["item_id"] == *item_id)
            .expect("remaining item should appear in the result");
        assert_eq!(deferred["status"], "skipped");
        assert_eq!(deferred["deferred"], true);
    }
    for (index, path) in media_paths.iter().enumerate().skip(1) {
        assert_eq!(
            read_sidecar(&harness.fixture.archive_root, path),
            old_xml[index],
            "a deferred item's existing XML sidecar must remain unchanged"
        );
        assert_eq!(
            fs::read(
                harness
                    .fixture
                    .archive_root
                    .join(path)
                    .with_extension("ass")
            )
            .ok(),
            original_ass[index],
            "a deferred item's ASS sidecar state must remain unchanged"
        );
    }
    for (path, original) in media_paths.iter().zip(&original_media) {
        assert_eq!(
            fs::read(harness.fixture.archive_root.join(path)).unwrap(),
            *original
        );
    }
    assert!(
        String::from_utf8(read_sidecar(&harness.fixture.archive_root, media_paths[0]))
            .unwrap()
            .contains("new comment")
    );

    let snapshot = harness.scan(4).await;
    let preview_record = harness
        .request(
            5,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "library",
            }),
        )
        .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview = public_preview(&preview_record).clone();
    let continued = harness
        .request(6, confirmation_payload(&harness, 5, &preview))
        .await;
    assert_eq!(
        continued.status,
        InboxStatus::Completed,
        "{:?}",
        continued.error
    );
    harness.drain_dispatch().await;
    let continued_result = continued.result.as_ref().unwrap();
    assert_eq!(continued_result["summary"]["updated"], 2);
    assert_eq!(continued_result["summary"]["unchanged"], 1);
    assert_eq!(continued_result["summary"]["added_comments"], 2);
    assert_eq!(fetch_calls.load(Ordering::SeqCst), 4);
    for (index, path) in media_paths.iter().enumerate() {
        let updated_xml = String::from_utf8(read_sidecar(&harness.fixture.archive_root, path))
            .expect("continued XML sidecar should remain readable");
        assert!(
            updated_xml.contains(["first retained", "second retained", "third retained"][index])
        );
        assert_eq!(updated_xml.matches("new comment").count(), 1);
    }
    for (path, original) in media_paths.iter().zip(&original_media) {
        assert_eq!(
            fs::read(harness.fixture.archive_root.join(path)).unwrap(),
            *original
        );
    }
    harness.close().await;
}

#[tokio::test]
async fn danmaku_refresh_stale_sidecar_missing_root_and_invalid_cid_do_not_change_media_e2e() {
    let fetch_calls = Arc::new(AtomicUsize::new(0));
    let fetcher = {
        let fetch_calls = Arc::clone(&fetch_calls);
        crate::danmaku_refresh::TestDanmakuFetcher::new(Arc::new(move |cid| {
            let fetch_calls = Arc::clone(&fetch_calls);
            Box::pin(async move {
                if cid != 123_456_789 {
                    bail!("unexpected test CID {cid}");
                }
                fetch_calls.fetch_add(1, Ordering::SeqCst);
                Ok(danmaku_xml(&[("1.0", "remote comment")]))
            })
        }))
    };
    let harness = danmaku_refresh_harness(
        "danmaku-refresh-stale",
        &[("episode.mp4", "Episode one", "123456789")],
        fetcher,
    )
    .await;
    let media = harness.fixture.archive_root.join("episode.mp4");
    let original_media = fs::read(&media).unwrap();
    let original_xml = danmaku_xml(&[("0.5", "manually retained")]);
    fs::write(media.with_extension("xml"), &original_xml).expect("existing sidecar should write");
    let snapshot = harness.scan(1).await;
    let item_id = snapshot_item_id(&snapshot, "episode.mp4");
    let preview = harness
        .request(
            2,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "selected", "item_ids": [item_id],
            }),
        )
        .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let public = public_preview(&preview).clone();
    let externally_changed = danmaku_xml(&[("0.5", "user edit after preview")]);
    fs::write(media.with_extension("xml"), &externally_changed).unwrap();
    let rejected = harness
        .request(3, confirmation_payload(&harness, 2, &public))
        .await;
    assert!(
        matches!(rejected.status, InboxStatus::Failed | InboxStatus::Stale),
        "{:?}",
        rejected.error
    );
    assert_eq!(
        read_sidecar(&harness.fixture.archive_root, "episode.mp4"),
        externally_changed
    );
    assert_eq!(fs::read(&media).unwrap(), original_media);

    let calls_after_stale = fetch_calls.load(Ordering::SeqCst);
    assert!(
        calls_after_stale <= 1,
        "at most the selected CID may be fetched"
    );

    let snapshot = harness.scan(4).await;
    let item_id = snapshot_item_id(&snapshot, "episode.mp4");
    let preview = harness
        .request(
            5,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "selected", "item_ids": [item_id],
            }),
        )
        .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let public = public_preview(&preview).clone();
    let offline_root = harness.fixture.root.join("archive-offline");
    fs::rename(&harness.fixture.archive_root, &offline_root)
        .expect("library root should go offline");
    let missing_root = harness
        .request(6, confirmation_payload(&harness, 5, &public))
        .await;
    assert!(
        matches!(
            missing_root.status,
            InboxStatus::Failed | InboxStatus::Stale
        ),
        "{:?}",
        missing_root.error
    );
    fs::rename(&offline_root, &harness.fixture.archive_root)
        .expect("library root should reconnect");
    assert_eq!(
        read_sidecar(&harness.fixture.archive_root, "episode.mp4"),
        externally_changed
    );
    assert_eq!(fs::read(&media).unwrap(), original_media);
    assert_eq!(
        fetch_calls.load(Ordering::SeqCst),
        calls_after_stale,
        "an offline root must fail before another source request"
    );
    harness.close().await;
}

#[tokio::test]
async fn danmaku_refresh_rejects_unsafe_cid_and_wrong_root_binding_without_writes_e2e() {
    let fetch_calls = Arc::new(AtomicUsize::new(0));
    let fetcher = {
        let fetch_calls = Arc::clone(&fetch_calls);
        crate::danmaku_refresh::TestDanmakuFetcher::new(Arc::new(move |_cid| {
            let fetch_calls = Arc::clone(&fetch_calls);
            Box::pin(async move {
                fetch_calls.fetch_add(1, Ordering::SeqCst);
                Ok(danmaku_xml(&[("1.0", "remote comment")]))
            })
        }))
    };
    let harness = danmaku_refresh_harness(
        "danmaku-refresh-unsafe-cid",
        &[
            ("valid.mp4", "Valid", "123456789"),
            ("unsafe.mp4", "Unsafe", "../123"),
        ],
        fetcher,
    )
    .await;
    let valid_xml = danmaku_xml(&[("0.5", "valid old comment")]);
    let unsafe_xml = danmaku_xml(&[("0.5", "unsafe old comment")]);
    fs::write(harness.fixture.archive_root.join("valid.xml"), &valid_xml).unwrap();
    fs::write(harness.fixture.archive_root.join("unsafe.xml"), &unsafe_xml).unwrap();
    let snapshot = harness.scan(1).await;
    let preview = harness
        .request(
            2,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "library",
            }),
        )
        .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let rows = public_preview(&preview)["items"].as_array().unwrap();
    assert!(
        rows.iter()
            .any(|item| item["relative_path"] == "valid.mp4" && item["status"] == "ready")
    );
    assert!(
        rows.iter()
            .any(|item| item["relative_path"] == "unsafe.mp4" && item["status"] == "skipped")
    );
    let wrong_binding = harness.preview_reference(2, public_preview(&preview), "confirm");
    let mut wrong_binding = wrong_binding;
    wrong_binding["root_binding"] = json!("wrong-root-binding");
    let rejected = harness.request(3, wrong_binding).await;
    assert_eq!(
        rejected.status,
        InboxStatus::Failed,
        "wrong binding must be rejected"
    );
    assert_eq!(
        read_sidecar(&harness.fixture.archive_root, "valid.mp4"),
        valid_xml
    );
    assert_eq!(
        read_sidecar(&harness.fixture.archive_root, "unsafe.mp4"),
        unsafe_xml
    );
    assert_eq!(
        fetch_calls.load(Ordering::SeqCst),
        0,
        "wrong roots must not fetch"
    );
    let snapshot = harness.scan(4).await;
    let fresh = harness
        .request(
            5,
            json!({
                "operation": "preview", "library_id": harness.library_id,
                "root_binding": harness.root_binding, "expected_revision": snapshot["revision"],
                "scope": "library",
            }),
        )
        .await;
    assert_eq!(
        fresh.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        fresh.error
    );
    let confirmed = harness
        .request(6, confirmation_payload(&harness, 5, public_preview(&fresh)))
        .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );
    let result = confirmed.result.as_ref().unwrap();
    assert_eq!(result["summary"]["updated"], 1);
    assert_eq!(result["summary"]["skipped"], 1);
    assert_eq!(
        fetch_calls.load(Ordering::SeqCst),
        1,
        "only the valid CID may fetch"
    );
    assert_eq!(
        read_sidecar(&harness.fixture.archive_root, "unsafe.mp4"),
        unsafe_xml
    );
    harness.close().await;
}
