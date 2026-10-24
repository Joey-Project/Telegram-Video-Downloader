use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::anyhow;
use serde_json::{Value, json};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use super::*;
use crate::cloud_bot_tests::{
    cloud_test_root_binding, make_runtime, open_runtime_test_inbox,
    run_library_management_test_request, spawn_fake_telegram_api, stop_fake_telegram_api,
    temp_cloud_bot_dir,
};
use crate::metadata_recovery::{RecoveryQuery, ResolvedMetadataEntry};
use crate::metadata_recovery_bot::{RecoveryQueryFuture, TestMetadataRecoveryResolver};

struct RecoveryHarness {
    root: PathBuf,
    video_root: PathBuf,
    library_id: String,
    root_binding: String,
    queue: Arc<QueueManager>,
    runtime: Option<CloudBotRuntime>,
    dispatcher: Option<JoinHandle<()>>,
    telegram_shutdown: Option<oneshot::Sender<()>>,
    telegram_server: Option<JoinHandle<Result<()>>>,
}

impl RecoveryHarness {
    async fn close(mut self) {
        if let Some(dispatcher) = self.dispatcher {
            dispatcher.abort();
            let _ = dispatcher.await;
        }
        if let (Some(shutdown), Some(server)) = (self.telegram_shutdown, self.telegram_server) {
            stop_fake_telegram_api(shutdown, server).await;
        }
        drop(self.runtime.take());
        let _ = fs::remove_dir_all(self.root);
    }

    fn binding_payload(&self) -> Value {
        json!({
            "library_id": self.library_id,
            "root_binding": self.root_binding,
        })
    }

    async fn request(&self, seq: u64, kind: &str, mut payload: Value) -> InboxRecord {
        let fields = payload
            .as_object_mut()
            .expect("test operation payload should be an object");
        let binding_payload = self.binding_payload();
        if matches!(kind, "metadata_recover" | "file_preview") {
            for (key, value) in binding_payload
                .as_object()
                .expect("binding payload should be an object")
            {
                fields.insert(key.clone(), value.clone());
            }
        }
        run_library_management_test_request(
            self.runtime.as_ref().expect("runtime should be open"),
            seq,
            kind,
            payload,
        )
        .await
    }

    async fn rescan(&self, seq: u64) -> InboxRecord {
        run_library_management_test_request(
            self.runtime.as_ref().expect("runtime should be open"),
            seq,
            "library_scan",
            json!({"library_id": self.library_id}),
        )
        .await
    }
}

async fn recovery_harness(
    label: &str,
    fixtures: &[(&str, &str)],
    resolver: TestMetadataRecoveryResolver,
) -> RecoveryHarness {
    let root = temp_cloud_bot_dir(label);
    let video_root = root.join("videos");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = video_root.clone();
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("mock media roots should create");
    for (relative_path, nfo) in fixtures {
        let media_path = video_root.join(relative_path);
        fs::create_dir_all(
            media_path
                .parent()
                .expect("fixture media path should have a parent"),
        )
        .expect("fixture media parent should create");
        fs::write(&media_path, b"deterministic media fixture").expect("fixture media should write");
        fs::write(media_path.with_extension("nfo"), *nfo).expect("fixture metadata should write");
    }

    let queue = Arc::new(QueueManager::open(&config).expect("mock queue should open"));
    let library_id = queue
        .default_video_destination()
        .expect("default library should resolve")
        .library_id;
    let root_binding = cloud_test_root_binding(&queue, &library_id);
    let inbox =
        Arc::new(open_runtime_test_inbox(&video_root).expect("same-epoch mock inbox should open"));
    let (telegram, _telegram_requests, telegram_shutdown, telegram_server) =
        spawn_fake_telegram_api().await;
    let mut runtime = make_runtime(telegram, config, Arc::clone(&queue), inbox);
    runtime.test_metadata_recovery_resolver = Some(resolver);
    let dispatcher = Some(tokio::spawn(cloud_request_dispatcher(runtime.clone())));

    RecoveryHarness {
        root,
        video_root,
        library_id,
        root_binding,
        queue,
        runtime: Some(runtime),
        dispatcher,
        telegram_shutdown: Some(telegram_shutdown),
        telegram_server: Some(telegram_server),
    }
}

fn source_metadata(path: &str, title: &str) -> String {
    format!(
        "<movie><title>{title}</title><uniqueid type=\"bilibili\">{path}</uniqueid><future>preserve</future></movie>"
    )
}

fn resolved_entry(source_id: &str, title: &str, collection_id: &str) -> ResolvedMetadataEntry {
    ResolvedMetadataEntry {
        bvid: Some(source_id.to_string()),
        aid: None,
        cid: Some(123_456_789),
        epid: None,
        title: title.to_string(),
        collection: Some(crate::library::CollectionMetadata {
            id: Some(collection_id.to_string()),
            title: "Recovered series".to_string(),
            kind: "season".to_string(),
            order: Some(1),
            part_id: Some("123456789".to_string()),
        }),
    }
}

fn successful_resolver() -> TestMetadataRecoveryResolver {
    Arc::new(|query: RecoveryQuery| -> RecoveryQueryFuture {
        Box::pin(async move {
            Ok(vec![resolved_entry(
                &query.source_id,
                "Resolved episode title",
                "recovered-season",
            )])
        })
    })
}

fn snapshot_item_id(record: &InboxRecord, relative_path: &str) -> String {
    record
        .result
        .as_ref()
        .and_then(|result| result["items"].as_array())
        .and_then(|items| {
            items
                .iter()
                .find(|item| item["relative_path"] == relative_path)
        })
        .and_then(|item| item["id"].as_str())
        .expect("library snapshot should contain the selected fixture")
        .to_string()
}

fn recovery_report(record: &InboxRecord) -> &Value {
    record
        .result
        .as_ref()
        .and_then(|result| result.get("recovery"))
        .expect("completed recovery should persist the result envelope")
}

fn find_recovery_item<'a>(report: &'a Value, item_id: &str) -> &'a Value {
    report["items"]
        .as_array()
        .expect("recovery items should be an array")
        .iter()
        .find(|item| item["item_id"] == item_id)
        .expect("selected item should have a recovery result")
}

fn result_from(record: &InboxRecord) -> &Value {
    record
        .result
        .as_ref()
        .expect("request should persist its result")
}

#[tokio::test]
async fn metadata_recovery_mock_telegram_cloud_e2e_preview_confirm_rescan_and_status_toggle() {
    let mut harness = recovery_harness(
        "metadata-recovery-e2e",
        &[
            (
                "episode.mp4",
                "<movie><title>Manually curated title</title><uniqueid type=\"bilibili\">BV12TRrBcEP8</uniqueid><future>preserve</future></movie>",
            ),
            (
                "loose.mp4",
                "<movie><title>Loose video</title><future>keep loose field</future></movie>",
            ),
        ],
        successful_resolver(),
    )
    .await;

    let scanned = harness.rescan(1).await;
    assert_eq!(
        scanned.status,
        InboxStatus::Completed,
        "{:?}",
        scanned.error
    );
    let episode_id = snapshot_item_id(&scanned, "episode.mp4");
    let loose_id = snapshot_item_id(&scanned, "loose.mp4");

    let analysis = harness
        .request(
            2,
            "metadata_recover",
            json!({"scope": "selected", "item_ids": [episode_id]}),
        )
        .await;
    assert_eq!(
        analysis.status,
        InboxStatus::Completed,
        "{:?}",
        analysis.error
    );
    let report = recovery_report(&analysis);
    assert_eq!(result_from(&analysis)["library_id"], harness.library_id);
    assert_eq!(result_from(&analysis)["root_binding"], harness.root_binding);
    assert_eq!(result_from(&analysis)["scope"], "selected");
    let recovered = find_recovery_item(report, &episode_id);
    assert!(recovered["patch"].is_object(), "{recovered}");
    assert_eq!(recovered["patch"]["fill_missing_only"], true);
    let expected_revision = report["revision"]
        .as_str()
        .expect("analysis should include its snapshot revision");

    // Reopen the same-epoch durable inbox before previewing to prove the completed report is
    // available from disk without a new cloud request or a local polling cycle.
    if let Some(dispatcher) = harness.dispatcher.take() {
        dispatcher.abort();
        let _ = dispatcher.await;
    }
    let (config, telegram, resolver) = {
        let runtime = harness.runtime.as_ref().expect("runtime should be open");
        (
            (*runtime.bot.config).clone(),
            runtime.bot.telegram.clone(),
            runtime.test_metadata_recovery_resolver.clone(),
        )
    };
    let queue = Arc::clone(&harness.queue);
    harness.runtime.take();
    let reopened_inbox = Arc::new(
        open_runtime_test_inbox(&harness.video_root)
            .expect("same-epoch inbox should reopen after its report persisted"),
    );
    let reopened_report = reopened_inbox
        .record(2)
        .expect("reopened report receipt should load")
        .expect("reopened report receipt should remain present");
    assert_eq!(reopened_report.status, InboxStatus::Completed);
    assert_eq!(result_from(&reopened_report), result_from(&analysis));
    assert_eq!(recovery_report(&reopened_report), report);
    let mut reopened_runtime = make_runtime(telegram, config, queue, reopened_inbox);
    reopened_runtime.test_metadata_recovery_resolver = resolver;
    harness.runtime = Some(reopened_runtime);
    harness.dispatcher = Some(tokio::spawn(cloud_request_dispatcher(
        harness.runtime.as_ref().unwrap().clone(),
    )));

    let preview = harness
        .request(
            3,
            "file_preview",
            json!({
                "expected_revision": expected_revision,
                "item_ids": [episode_id],
                "metadata_patches": [recovered["patch"]],
            }),
        )
        .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let preview_result = result_from(&preview);
    let confirmed = harness
        .request(
            4,
            "file_confirm",
            json!({
                "preview_id": preview_result["preview_id"],
                "revision": preview_result["revision"],
            }),
        )
        .await;
    assert_eq!(
        confirmed.status,
        InboxStatus::Completed,
        "{:?}",
        confirmed.error
    );

    let rescanned = harness.rescan(5).await;
    assert_eq!(
        rescanned.status,
        InboxStatus::Completed,
        "{:?}",
        rescanned.error
    );
    let episode = rescanned.result.as_ref().unwrap()["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == episode_id)
        .expect("rescanned episode should remain present");
    assert_eq!(episode["title"], "Manually curated title");
    assert_eq!(episode["collection"]["title"], "Recovered series");
    let episode_nfo = fs::read_to_string(harness.video_root.join("episode.nfo"))
        .expect("confirmed metadata should remain readable");
    assert!(episode_nfo.contains("<future>preserve</future>"));
    assert!(episode_nfo.contains("Manually curated title"));

    let loose_nfo = harness.video_root.join("loose.nfo");
    let status_preview = harness
        .request(
            6,
            "file_preview",
            json!({
                "expected_revision": rescanned.result.as_ref().unwrap()["revision"],
                "item_ids": [loose_id],
                "metadata_patches": [{
                    "item_id": loose_id,
                    "organization_status": "standalone"
                }]
            }),
        )
        .await;
    assert_eq!(
        status_preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        status_preview.error
    );
    let status_preview_result = result_from(&status_preview);
    let marked = harness
        .request(
            7,
            "file_confirm",
            json!({
                "preview_id": status_preview_result["preview_id"],
                "revision": status_preview_result["revision"],
            }),
        )
        .await;
    assert_eq!(marked.status, InboxStatus::Completed, "{:?}", marked.error);
    let marked_scan = harness.rescan(8).await;
    let marked_item = marked_scan.result.as_ref().unwrap()["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == loose_id)
        .unwrap();
    assert_eq!(marked_item["organization_status"], "standalone");

    let unmark_preview = harness
        .request(
            9,
            "file_preview",
            json!({
                "expected_revision": marked_scan.result.as_ref().unwrap()["revision"],
                "item_ids": [loose_id],
                "metadata_patches": [{
                    "item_id": loose_id,
                    "organization_status": "unreviewed"
                }]
            }),
        )
        .await;
    assert_eq!(
        unmark_preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        unmark_preview.error
    );
    let unmark_result = result_from(&unmark_preview);
    let unmarked = harness
        .request(
            10,
            "file_confirm",
            json!({
                "preview_id": unmark_result["preview_id"],
                "revision": unmark_result["revision"],
            }),
        )
        .await;
    assert_eq!(
        unmarked.status,
        InboxStatus::Completed,
        "{:?}",
        unmarked.error
    );
    let unmarked_scan = harness.rescan(11).await;
    let unmarked_item = unmarked_scan.result.as_ref().unwrap()["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == loose_id)
        .unwrap();
    assert_eq!(unmarked_item["organization_status"], "unreviewed");

    let stale_analysis = harness
        .request(
            12,
            "metadata_recover",
            json!({"scope": "selected", "item_ids": [loose_id]}),
        )
        .await;
    assert_eq!(stale_analysis.status, InboxStatus::Completed);
    let stale_revision = recovery_report(&stale_analysis)["revision"].clone();
    fs::write(
        &loose_nfo,
        "<movie><title>Changed after analysis</title><future>keep loose field</future></movie>",
    )
    .expect("sidecar change should write");
    let previews_before =
        crate::cloud_bot_tests::persisted_cloud_preview_count(&harness.video_root);
    let stale_preview = harness
        .request(
            13,
            "file_preview",
            json!({
                "expected_revision": stale_revision,
                "item_ids": [loose_id],
                "metadata_patches": [{
                    "item_id": loose_id,
                    "title": "Must not apply after stale analysis"
                }]
            }),
        )
        .await;
    assert_eq!(stale_preview.status, InboxStatus::Failed);
    assert!(
        stale_preview
            .error
            .as_deref()
            .is_some_and(|error| error.contains("changed after metadata analysis"))
    );
    assert_eq!(
        crate::cloud_bot_tests::persisted_cloud_preview_count(&harness.video_root),
        previews_before,
        "stale analysis should fail before a preview manifest is persisted"
    );
    assert!(
        fs::read_to_string(&loose_nfo)
            .unwrap()
            .contains("Changed after analysis")
    );

    harness.close().await;
}

#[tokio::test]
async fn metadata_recovery_ambiguity_and_provider_failure_report_without_writes() {
    let ambiguous_id = "BV1ab411C7x1";
    let offline_id = "BV1xx411c7mD";
    let resolver: TestMetadataRecoveryResolver = Arc::new(move |query: RecoveryQuery| {
        Box::pin(async move {
            if query.source_id == ambiguous_id {
                Ok(vec![
                    resolved_entry(&query.source_id, "Episode one", "collection-one"),
                    ResolvedMetadataEntry {
                        bvid: Some(query.source_id.clone()),
                        aid: None,
                        cid: Some(123_456_790),
                        epid: None,
                        title: "Episode two".to_string(),
                        collection: Some(crate::library::CollectionMetadata {
                            id: Some("collection-two".to_string()),
                            title: "Another series".to_string(),
                            kind: "season".to_string(),
                            order: Some(2),
                            part_id: Some("123456790".to_string()),
                        }),
                    },
                ])
            } else {
                Err(anyhow!("metadata provider unavailable"))
            }
        })
    });
    let ambiguous_nfo_fixture = source_metadata(ambiguous_id, "Ambiguous");
    let offline_nfo_fixture = source_metadata(offline_id, "Offline");
    let fixtures = [
        ("ambiguous.mp4", ambiguous_nfo_fixture.as_str()),
        ("offline.mp4", offline_nfo_fixture.as_str()),
    ];
    let harness = recovery_harness("metadata-recovery-no-write", &fixtures, resolver).await;
    let scanned = harness.rescan(20).await;
    let ambiguous_item_id = snapshot_item_id(&scanned, "ambiguous.mp4");
    let offline_item_id = snapshot_item_id(&scanned, "offline.mp4");
    let ambiguous_nfo = fs::read(harness.video_root.join("ambiguous.nfo")).unwrap();
    let offline_nfo = fs::read(harness.video_root.join("offline.nfo")).unwrap();

    let analysis = harness
        .request(
            21,
            "metadata_recover",
            json!({
                "scope": "selected",
                "item_ids": [ambiguous_item_id, offline_item_id],
            }),
        )
        .await;
    assert_eq!(
        analysis.status,
        InboxStatus::Completed,
        "{:?}",
        analysis.error
    );
    let report = recovery_report(&analysis);
    let ambiguous = find_recovery_item(report, &ambiguous_item_id);
    assert_eq!(ambiguous["status"], "needs_confirmation");
    assert!(ambiguous["patch"].is_null());
    assert!(ambiguous["candidates"].as_array().unwrap().len() >= 2);
    let offline = find_recovery_item(report, &offline_item_id);
    assert_eq!(offline["status"], "unavailable");
    assert!(!offline["reason"].as_str().unwrap_or_default().is_empty());
    assert_eq!(
        fs::read(harness.video_root.join("ambiguous.nfo")).unwrap(),
        ambiguous_nfo
    );
    assert_eq!(
        fs::read(harness.video_root.join("offline.nfo")).unwrap(),
        offline_nfo
    );
    assert_eq!(
        crate::cloud_bot_tests::persisted_cloud_preview_count(&harness.video_root),
        0,
        "analysis with ambiguity or provider failure must not create previews"
    );

    harness.close().await;
}

#[tokio::test]
async fn metadata_recovery_resolver_wait_does_not_block_library_or_telegram_dispatch() {
    let resolver_started = Arc::new(Notify::new());
    let release_resolver = Arc::new(Notify::new());
    let started_for_resolver = Arc::clone(&resolver_started);
    let release_for_resolver = Arc::clone(&release_resolver);
    let resolver: TestMetadataRecoveryResolver =
        Arc::new(move |query: RecoveryQuery| -> RecoveryQueryFuture {
            let started = Arc::clone(&started_for_resolver);
            let release = Arc::clone(&release_for_resolver);
            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                Ok(vec![resolved_entry(
                    &query.source_id,
                    "Resolver resumed",
                    "resume-season",
                )])
            })
        });
    let episode_nfo = source_metadata("BV12TRrBcEP8", "Episode");
    let fixtures = [("episode.mp4", episode_nfo.as_str())];
    let harness = recovery_harness("metadata-recovery-admission", &fixtures, resolver).await;
    let scanned = harness.rescan(40).await;
    let item_id = snapshot_item_id(&scanned, "episode.mp4");
    let mut payload = json!({"scope": "selected", "item_ids": [item_id]});
    let binding_payload = harness.binding_payload();
    for (key, value) in binding_payload
        .as_object()
        .expect("binding payload should be an object")
    {
        payload
            .as_object_mut()
            .unwrap()
            .insert(key.clone(), value.clone());
    }
    let analysis_runtime = harness.runtime.as_ref().unwrap().clone();
    let mut analysis_task = tokio::spawn(async move {
        run_library_management_test_request(&analysis_runtime, 41, "metadata_recover", payload)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), resolver_started.notified())
        .await
        .expect("mock resolver should enter its blocked state");

    let concurrent_scan = tokio::time::timeout(Duration::from_secs(3), harness.rescan(42)).await;
    let concurrent_telegram = tokio::time::timeout(
        Duration::from_secs(3),
        run_library_management_test_request(
            harness.runtime.as_ref().unwrap(),
            43,
            "telegram",
            json!({"update_id": 4300}),
        ),
    )
    .await;
    let analysis_still_waiting = !analysis_task.is_finished();
    release_resolver.notify_one();
    let analysis = tokio::time::timeout(Duration::from_secs(5), &mut analysis_task)
        .await
        .expect("mock analysis should finish after resolver release")
        .expect("mock analysis task should not panic");

    assert!(
        concurrent_scan.is_ok(),
        "library scan should not wait on metadata provider I/O"
    );
    assert_eq!(
        concurrent_scan.unwrap().status,
        InboxStatus::Completed,
        "concurrent library scan should complete while the provider is blocked"
    );
    assert!(
        concurrent_telegram.is_ok(),
        "Telegram dispatch should remain independent"
    );
    assert_eq!(concurrent_telegram.unwrap().status, InboxStatus::Completed);
    assert!(
        analysis_still_waiting,
        "resolver gate should keep the analysis request pending"
    );
    assert_eq!(
        analysis.status,
        InboxStatus::Completed,
        "{:?}",
        analysis.error
    );

    harness.close().await;
}

#[tokio::test]
async fn whole_library_recovery_cursor_reaches_items_after_first_hundred() {
    let mut fixtures = Vec::new();
    for index in 0..101 {
        let source_id = "BV12TRrBcEP8".to_string();
        let relative_path = format!("item-{index:03}.mp4");
        fixtures.push((
            relative_path,
            source_metadata(&source_id, "Needs a collection"),
        ));
    }
    let fixture_refs = fixtures
        .iter()
        .map(|(path, nfo)| (path.as_str(), nfo.as_str()))
        .collect::<Vec<_>>();
    let query_count = Arc::new(AtomicUsize::new(0));
    let query_count_for_resolver = Arc::clone(&query_count);
    let resolver: TestMetadataRecoveryResolver = Arc::new(move |query: RecoveryQuery| {
        query_count_for_resolver.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            Ok(vec![resolved_entry(
                &query.source_id,
                "Resolved title",
                "library-season",
            )])
        })
    });
    let harness = recovery_harness("metadata-recovery-pagination", &fixture_refs, resolver).await;
    let first = harness
        .request(30, "metadata_recover", json!({"scope": "library"}))
        .await;
    assert_eq!(first.status, InboxStatus::Completed, "{:?}", first.error);
    assert_eq!(result_from(&first)["scope"], "library");
    let first_report = recovery_report(&first);
    let first_items = first_report["items"].as_array().unwrap();
    assert_eq!(first_items.len(), 100);
    assert!(first_report["total"].as_u64().unwrap_or_default() >= 101);
    assert!(first_report["remaining"].as_u64().unwrap_or_default() > 0);
    let cursor = first_report["next_cursor"]
        .as_str()
        .expect("first library batch should expose a stable continuation cursor")
        .to_string();
    let first_ids = first_items
        .iter()
        .map(|item| item["item_id"].as_str().unwrap().to_string())
        .collect::<HashSet<_>>();

    let second = harness
        .request(
            31,
            "metadata_recover",
            json!({"scope": "library", "after_item_id": cursor}),
        )
        .await;
    assert_eq!(second.status, InboxStatus::Completed, "{:?}", second.error);
    assert_eq!(result_from(&second)["scope"], "library");
    let second_report = recovery_report(&second);
    let second_items = second_report["items"].as_array().unwrap();
    assert_eq!(second_items.len(), 1);
    assert_eq!(second_report["remaining"], 0);
    assert!(second_report["next_cursor"].is_null());
    let second_id = second_items[0]["item_id"].as_str().unwrap();
    assert!(
        !first_ids.contains(second_id),
        "cursor must not repeat the prior batch"
    );
    assert_eq!(first_ids.len() + 1, 101);
    assert_eq!(query_count.load(Ordering::Relaxed), 2);

    harness.close().await;
}
