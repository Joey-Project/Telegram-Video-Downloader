use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::*;
use crate::cloud_bot_tests::{
    FakeTelegramRequest, cloud_test_root_binding, make_runtime, open_runtime_test_inbox,
    run_library_management_test_request, spawn_fake_telegram_api, stop_fake_telegram_api,
    take_cloud_test_telegram_requests, temp_cloud_bot_dir,
};
use crate::file_provider::{FileProviderAccessError, QueueFileProvider};

struct CollectionLayoutHarness {
    root: PathBuf,
    video_root: PathBuf,
    config: AppConfig,
    library_id: String,
    root_binding: String,
    queue: Arc<QueueManager>,
    runtime: Option<CloudBotRuntime>,
    dispatcher: Option<JoinHandle<()>>,
    telegram_shutdown: Option<oneshot::Sender<()>>,
    telegram_server: Option<JoinHandle<Result<()>>>,
    telegram_requests: Option<mpsc::UnboundedReceiver<FakeTelegramRequest>>,
}

impl CollectionLayoutHarness {
    async fn open(label: &str, provider: Arc<dyn QueueFileProvider>) -> Self {
        let root = temp_cloud_bot_dir(label);
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = root.join("videos");
        config.downloads.pdf_dir = root.join("pdfs");
        config
            .ensure_runtime_dirs()
            .expect("isolated cloud media roots should create");

        let queue = Arc::new(QueueManager::open(&config).expect("isolated task queue should open"));
        let library_id = queue
            .default_video_destination()
            .expect("default video library should resolve")
            .library_id;
        let root_binding = cloud_test_root_binding(&queue, &library_id);
        let inbox = Arc::new(
            open_runtime_test_inbox(&config.downloads.video_dir)
                .expect("same-epoch test inbox should open"),
        );
        let (telegram, telegram_requests, telegram_shutdown, telegram_server) =
            spawn_fake_telegram_api().await;
        let mut runtime = make_runtime(telegram, config.clone(), Arc::clone(&queue), inbox);
        runtime.test_file_provider = Some(provider);
        let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));

        Self {
            root,
            video_root: config.downloads.video_dir.clone(),
            config,
            library_id,
            root_binding,
            queue,
            runtime: Some(runtime),
            dispatcher: Some(dispatcher),
            telegram_shutdown: Some(telegram_shutdown),
            telegram_server: Some(telegram_server),
            telegram_requests: Some(telegram_requests),
        }
    }

    async fn request(&self, seq: u64, kind: &str, mut payload: Value) -> InboxRecord {
        if kind == "file_preview" {
            let fields = payload
                .as_object_mut()
                .expect("test preview request should be an object");
            fields.insert("library_id".to_string(), json!(self.library_id.clone()));
            fields.insert("root_binding".to_string(), json!(self.root_binding.clone()));
        }
        run_library_management_test_request(
            self.runtime.as_ref().expect("test runtime should be open"),
            seq,
            kind,
            payload,
        )
        .await
    }

    async fn reopen(&mut self, provider: Arc<dyn QueueFileProvider>) {
        self.close_runtime().await;
        let inbox = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match open_runtime_test_inbox(&self.video_root) {
                    Ok(inbox) => break inbox,
                    Err(error)
                        if format!("{error:#}")
                            .contains("cloud inbox already has a live local owner") =>
                    {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Err(error) => panic!("same-epoch test inbox should reopen: {error:#}"),
                }
            }
        })
        .await
        .expect("the previous dispatcher should release its inbox owner");
        let inbox = Arc::new(inbox);
        let (telegram, telegram_requests, telegram_shutdown, telegram_server) =
            spawn_fake_telegram_api().await;
        let mut runtime = make_runtime(
            telegram,
            self.config.clone(),
            Arc::clone(&self.queue),
            inbox,
        );
        runtime.test_file_provider = Some(provider);
        let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
        self.runtime = Some(runtime);
        self.dispatcher = Some(dispatcher);
        self.telegram_shutdown = Some(telegram_shutdown);
        self.telegram_server = Some(telegram_server);
        self.telegram_requests = Some(telegram_requests);
    }

    async fn close_runtime(&mut self) {
        if let Some(dispatcher) = self.dispatcher.take() {
            dispatcher.abort();
            let _ = dispatcher.await;
        }
        drop(self.runtime.take());
        drop(self.telegram_requests.take());
        if let (Some(shutdown), Some(server)) =
            (self.telegram_shutdown.take(), self.telegram_server.take())
        {
            stop_fake_telegram_api(shutdown, server).await;
        }
    }

    fn take_telegram_request_count(&mut self) -> usize {
        take_cloud_test_telegram_requests(
            self.telegram_requests
                .as_mut()
                .expect("fake Telegram request receiver should remain open"),
        )
        .len()
    }
}

impl Drop for CollectionLayoutHarness {
    fn drop(&mut self) {
        if let Some(dispatcher) = self.dispatcher.take() {
            dispatcher.abort();
        }
        drop(self.runtime.take());
        drop(self.telegram_requests.take());
        if let Some(shutdown) = self.telegram_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(server) = self.telegram_server.take() {
            server.abort();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[derive(Default)]
struct FailSecondMoveOnce {
    move_attempts: AtomicUsize,
    successful_moves: std::sync::Mutex<Vec<(PathBuf, PathBuf)>>,
}

impl FailSecondMoveOnce {
    fn successful_moves(&self) -> Vec<(PathBuf, PathBuf)> {
        self.successful_moves
            .lock()
            .expect("move log should remain healthy")
            .clone()
    }
}

impl QueueFileProvider for FailSecondMoveOnce {
    fn coordinate_read(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        accessor(path)
    }

    fn coordinate_write(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        accessor(path)
    }

    fn coordinate_move(
        &self,
        source: &Path,
        destination: &Path,
        accessor: &mut dyn FnMut(&Path, &Path) -> Result<()>,
    ) -> Result<()> {
        if self.move_attempts.fetch_add(1, Ordering::SeqCst) == 1 {
            return Err(FileProviderAccessError::new(
                source,
                "move",
                "injected one-time access failure after the first attachment move",
            )
            .into());
        }
        accessor(source, destination)?;
        self.successful_moves
            .lock()
            .expect("move log should remain healthy")
            .push((source.to_path_buf(), destination.to_path_buf()));
        Ok(())
    }
}

struct LegacyEpisode {
    order: u32,
    title: &'static str,
    media_source: PathBuf,
    nfo_source: PathBuf,
    subtitle_source: PathBuf,
    media_bytes: Vec<u8>,
    nfo_bytes: Vec<u8>,
    subtitle_bytes: Vec<u8>,
}

fn write_legacy_nested_collection(video_root: &Path) -> Vec<LegacyEpisode> {
    [(1, "Episode One"), (2, "Episode Two")]
        .into_iter()
        .map(|(order, title)| {
            let source_dir = video_root
                .join("Legacy Collection")
                .join("Series")
                .join(title);
            fs::create_dir_all(&source_dir).expect("legacy episode directory should create");
            let stem = format!("old-{order}");
            let media_source = source_dir.join(format!("{stem}.mp4"));
            let nfo_source = source_dir.join(format!("{stem}.nfo"));
            let subtitle_source = source_dir.join(format!("{stem}.en.srt"));
            let media_bytes = format!("deterministic opaque video fixture {order}").into_bytes();
            let nfo_bytes = format!(
                "<movie><title>{title}</title><set>Series</set><episode>{order}</episode></movie>"
            )
            .into_bytes();
            let subtitle_bytes = format!("subtitle fixture for {title}\n").into_bytes();
            fs::write(&media_source, &media_bytes).expect("legacy media fixture should write");
            fs::write(&nfo_source, &nfo_bytes).expect("legacy NFO fixture should write");
            fs::write(&subtitle_source, &subtitle_bytes)
                .expect("legacy subtitle fixture should write");
            LegacyEpisode {
                order,
                title,
                media_source,
                nfo_source,
                subtitle_source,
                media_bytes,
                nfo_bytes,
                subtitle_bytes,
            }
        })
        .collect()
}

#[tokio::test]
async fn old_nested_collection_cloud_confirm_flattens_and_recovers_after_reopen() {
    let provider = Arc::new(FailSecondMoveOnce::default());
    let provider_trait: Arc<dyn QueueFileProvider> = provider.clone();
    let mut harness =
        CollectionLayoutHarness::open("old-nested-flat-collection", provider_trait.clone()).await;
    let episodes = write_legacy_nested_collection(&harness.video_root);

    let scan = harness
        .request(
            1,
            "library_scan",
            json!({"library_id": harness.library_id.clone()}),
        )
        .await;
    assert_eq!(scan.status, InboxStatus::Completed, "{:?}", scan.error);
    let scan_result = scan.result.as_ref().expect("scan result should be present");
    let items = scan_result["items"]
        .as_array()
        .expect("scan should return library items");
    assert_eq!(items.len(), episodes.len());
    let mut item_ids = items
        .iter()
        .map(|item| {
            assert_eq!(item["collection"]["title"], "Series");
            item["id"]
                .as_str()
                .expect("scanned media should have an item ID")
                .to_string()
        })
        .collect::<Vec<_>>();
    item_ids.sort();

    let preview = harness
        .request(
            2,
            "file_preview",
            json!({
                "expected_revision": scan_result["revision"],
                "item_ids": item_ids,
                "target_relative_dir": "Organized",
                "rename": true,
                "conflict": "keep_both"
            }),
        )
        .await;
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let preview_result = preview
        .result
        .as_ref()
        .expect("preview result should persist");
    let preview_id = preview_result["preview_id"]
        .as_str()
        .expect("preview ID should be returned")
        .to_string();
    let revision = preview_result["revision"]
        .as_str()
        .expect("preview revision should be returned")
        .to_string();
    let preview_items = preview_result["preview"]["items"]
        .as_array()
        .expect("preview should include each selected item");
    assert_eq!(preview_items.len(), episodes.len());

    let expected_targets = episodes
        .iter()
        .map(|episode| {
            let stem = format!("Series - P{:02} - {}", episode.order, episode.title);
            let media = format!("Organized/Series/{stem}.mp4");
            let nfo = format!("Organized/Series/{stem}.nfo");
            let subtitle = format!("Organized/Series/{stem}.en.srt");
            let preview_item = preview_items
                .iter()
                .find(|item| item["target_path"] == media)
                .unwrap_or_else(|| {
                    panic!("preview should plan the flat target {media}: {preview_items:#?}")
                });
            assert_eq!(
                preview_item["files"]
                    .as_array()
                    .expect("selected media should include its attachments")
                    .iter()
                    .filter_map(|file| file["target_path"].as_str())
                    .collect::<std::collections::BTreeSet<_>>(),
                std::collections::BTreeSet::from([media.as_str(), nfo.as_str(), subtitle.as_str()]),
                "all selected media and sidecars should share the flat collection folder"
            );
            assert_eq!(
                Path::new(preview_item["target_path"].as_str().unwrap()).parent(),
                Some(Path::new("Organized/Series")),
                "organized collection items should have no per-episode directory"
            );
            (media, nfo, subtitle)
        })
        .collect::<Vec<_>>();
    assert!(
        !harness.video_root.join("Organized").exists(),
        "preview should not create its destination"
    );

    let first_confirm = harness
        .request(
            3,
            "file_confirm",
            json!({"preview_id": preview_id, "revision": revision}),
        )
        .await;
    assert_eq!(first_confirm.status, InboxStatus::Failed);
    assert_eq!(
        first_confirm
            .result
            .as_ref()
            .and_then(|result| result["operation"]["retryable_access_error"].as_bool()),
        Some(true),
        "a transient move failure should leave the same preview retryable"
    );
    let partial_moves = provider.successful_moves();
    assert_eq!(
        partial_moves.len(),
        4,
        "the unaffected episode should still move while the failed episode pauses"
    );
    assert_eq!(
        partial_moves
            .iter()
            .map(|(source, _)| source)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        partial_moves.len(),
        "partial execution should not move any source more than once"
    );
    assert!(
        partial_moves
            .iter()
            .all(|(source, target)| { target.is_file() && !source.exists() })
    );
    assert!(
        !partial_moves[0]
            .1
            .extension()
            .is_some_and(|extension| extension == "mp4"),
        "the first move should be an attachment before primary media"
    );
    assert!(
        episodes
            .iter()
            .filter(|episode| episode.media_source.is_file())
            .count()
            == 1,
        "the interrupted episode should retain its primary media source"
    );

    let preview_receipt = harness
        .runtime
        .as_ref()
        .unwrap()
        .inbox
        .record(2)
        .expect("preview receipt should remain readable")
        .expect("preview receipt should remain present");
    assert_eq!(preview_receipt.status, InboxStatus::AwaitingConfirmation);
    harness.reopen(provider_trait).await;

    // file_confirm opens a fresh LibraryManager and resumes from the durable partial batch.
    let resumed_confirm = harness
        .request(
            4,
            "file_confirm",
            json!({"preview_id": preview_id, "revision": revision}),
        )
        .await;
    assert_eq!(
        resumed_confirm.status,
        InboxStatus::Completed,
        "{:?}",
        resumed_confirm.error
    );
    let final_moves = provider.successful_moves();
    assert_eq!(final_moves.len(), 6);
    assert_eq!(
        final_moves
            .iter()
            .map(|(source, _)| source)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        final_moves.len(),
        "retry should not publish any source more than once"
    );
    let actual_target_paths = final_moves
        .iter()
        .map(|(_, target)| {
            target
                .strip_prefix(&harness.video_root)
                .expect("published destination should remain under the library root")
                .to_string_lossy()
                .into_owned()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        actual_target_paths.len(),
        final_moves.len(),
        "retry should not publish any file path more than once"
    );
    let expected_target_paths = expected_targets
        .iter()
        .flat_map(|(media, nfo, subtitle)| [media.clone(), nfo.clone(), subtitle.clone()])
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual_target_paths, expected_target_paths);

    let collection_target_dir = harness.video_root.join("Organized/Series");
    for (episode, (media, nfo, subtitle)) in episodes.iter().zip(&expected_targets) {
        let media_target = harness.video_root.join(media);
        let nfo_target = harness.video_root.join(nfo);
        let subtitle_target = harness.video_root.join(subtitle);
        assert_eq!(fs::read(&media_target).unwrap(), episode.media_bytes);
        assert_eq!(fs::read(&nfo_target).unwrap(), episode.nfo_bytes);
        assert_eq!(fs::read(&subtitle_target).unwrap(), episode.subtitle_bytes);
        assert!(!episode.media_source.exists());
        assert!(!episode.nfo_source.exists());
        assert!(!episode.subtitle_source.exists());
        assert_eq!(media_target.parent(), Some(collection_target_dir.as_path()));
        assert_eq!(nfo_target.parent(), Some(collection_target_dir.as_path()));
        assert_eq!(
            subtitle_target.parent(),
            Some(collection_target_dir.as_path())
        );
    }

    let rescanned = harness
        .request(
            5,
            "library_scan",
            json!({"library_id": harness.library_id.clone()}),
        )
        .await;
    assert_eq!(
        rescanned.status,
        InboxStatus::Completed,
        "{:?}",
        rescanned.error
    );
    let rescanned_items = rescanned.result.as_ref().unwrap()["items"]
        .as_array()
        .expect("rescan should return library items");
    assert_eq!(rescanned_items.len(), episodes.len());
    assert!(rescanned_items.iter().all(|item| {
        item["relative_path"]
            .as_str()
            .is_some_and(|path| path.starts_with("Organized/Series/"))
    }));
    assert!(
        harness.take_telegram_request_count() > 0,
        "the cloud confirmation flow should use the local fake Telegram API"
    );

    harness.close_runtime().await;
}
