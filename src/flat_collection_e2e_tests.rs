use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use unicode_segmentation::UnicodeSegmentation;

use super::*;
use crate::cloud_bot_tests::{
    FakeTelegramRequest, cloud_test_root_binding, make_runtime, open_runtime_test_inbox,
    run_library_management_test_request, spawn_fake_telegram_api, stop_fake_telegram_api,
    take_cloud_test_telegram_requests, temp_cloud_bot_dir,
};
use crate::file_provider::{FileProviderAccessError, MockQueueFileProvider, QueueFileProvider};

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

fn expected_moved_nfo(original: &[u8], target_media_path: &str) -> String {
    let original = std::str::from_utf8(original).expect("fixture NFO should be UTF-8");
    let target_filename = Path::new(target_media_path)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("moved media target should have a filename");
    assert_eq!(original.matches("</movie>").count(), 1);
    original.replacen(
        "</movie>",
        &format!("\n  <localfilename>{target_filename}</localfilename>\n</movie>"),
        1,
    )
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
            let stem = format!("P{:03} - {}", episode.order, episode.title);
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
    let completed_index = expected_targets
        .iter()
        .position(|(media, _, _)| harness.video_root.join(media).is_file())
        .expect("one complete item should be published before the injected failure");
    let completed_target = &expected_targets[completed_index];
    assert_eq!(
        fs::read_to_string(harness.video_root.join(&completed_target.1))
            .expect("completed item's NFO should remain readable"),
        expected_moved_nfo(&episodes[completed_index].nfo_bytes, &completed_target.0),
        "the completed item should durably record its renamed primary filename"
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
        assert_eq!(
            fs::read_to_string(&nfo_target).unwrap(),
            expected_moved_nfo(&episode.nfo_bytes, media)
        );
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

#[tokio::test]
async fn cloud_confirm_shortens_only_long_title_and_keeps_identity_specs_and_sidecars() {
    let provider: Arc<dyn QueueFileProvider> = Arc::new(MockQueueFileProvider::default());
    let mut harness = CollectionLayoutHarness::open("long-title-flat-collection", provider).await;

    let title = "界😀e\u{301}👨‍👩‍👧‍👦-多言語-".repeat(45);
    let source_dir = harness
        .video_root
        .join("Legacy Collection/Series/long episode");
    fs::create_dir_all(&source_dir).expect("long-title source directory should create");
    let source_stem = "long-episode";
    let media_source = source_dir.join(format!("{source_stem}.mp4"));
    let ffmpeg = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=black:s=1920x1080:r=1",
            "-frames:v",
            "1",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-an",
            "-y",
        ])
        .arg(&media_source)
        .output()
        .expect("installed ffmpeg should generate the video fixture");
    assert!(
        ffmpeg.status.success(),
        "ffmpeg should generate a one-frame 1080p h264 fixture: {}",
        String::from_utf8_lossy(&ffmpeg.stderr)
    );
    let media_bytes = fs::read(&media_source).expect("generated media fixture should read");
    let source_id = "youtube:dQw4w9WgXcQ";
    let nfo_bytes = format!(
        "<movie><title>{title}</title><uniqueid type=\"youtube\">dQw4w9WgXcQ</uniqueid><set>Series</set><episode>203</episode></movie>"
    );
    let sidecars = [
        ("nfo", nfo_bytes.as_bytes()),
        ("en.srt", b"subtitle payload".as_slice()),
        ("ass", b"ASS payload".as_slice()),
        ("danmaku.xml", b"danmaku payload".as_slice()),
        ("cover.jpg", b"cover payload".as_slice()),
    ];
    let nfo_source = source_dir.join(format!("{source_stem}.nfo"));
    for (suffix, bytes) in sidecars {
        fs::write(source_dir.join(format!("{source_stem}.{suffix}")), bytes)
            .expect("long-title sidecar should write");
    }

    let scan = harness
        .request(
            1,
            "library_scan",
            json!({"library_id": harness.library_id.clone()}),
        )
        .await;
    assert_eq!(scan.status, InboxStatus::Completed, "{:?}", scan.error);
    let scan_result = scan.result.as_ref().expect("scan result should persist");
    let item = scan_result["items"]
        .as_array()
        .expect("scan should return items")
        .iter()
        .find(|item| {
            item["relative_path"]
                .as_str()
                .is_some_and(|path| path.ends_with("long-episode.mp4"))
        })
        .expect("scan should find the long-title fixture");
    let item_id = item["id"].as_str().expect("scanned item should have an ID");
    assert_eq!(item["title"], title);
    assert_eq!(item["source_ids"][0], source_id);
    assert_eq!(item["height"], 1080);
    assert_eq!(item["codec"], "h264");

    let preview = harness
        .request(
            2,
            "file_preview",
            json!({
                "expected_revision": scan_result["revision"],
                "item_ids": [item_id],
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
    let preview_item = preview_result["preview"]["items"]
        .as_array()
        .expect("preview should include planned files")
        .first()
        .expect("preview should plan the selected item");
    let media_relative = preview_item["target_path"]
        .as_str()
        .expect("preview media target should be present")
        .to_string();
    let media_name = Path::new(&media_relative)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("media target should be UTF-8");
    assert!(
        media_name.starts_with("P203 - "),
        "collection title is omitted and order is padded: {media_name}"
    );
    assert!(
        media_name.contains("[youtube_dQw4w9WgXcQ]"),
        "source ID should remain in the filename: {media_name}"
    );
    assert!(
        media_name.contains("[1080p h264]"),
        "media specs should remain in the filename: {media_name}"
    );
    let title_in_name = media_name
        .strip_prefix("P203 - ")
        .and_then(|name| name.strip_suffix(" - [youtube_dQw4w9WgXcQ] - [1080p h264].mp4"))
        .expect("filename should retain the title between fixed identity fields");
    assert!(
        title_in_name.ends_with('…'),
        "an overlong title should end with an ellipsis: {title_in_name}"
    );
    let shortened = title_in_name.trim_end_matches('…');
    assert!(
        title.starts_with(shortened),
        "shortened title should be a prefix of the full title"
    );
    assert!(
        title
            .graphemes(true)
            .scan(String::new(), |prefix, grapheme| {
                prefix.push_str(grapheme);
                Some(prefix == shortened)
            })
            .any(|is_boundary| is_boundary),
        "shortening should stop on a grapheme boundary"
    );

    let file_paths = preview_item["files"]
        .as_array()
        .expect("preview should include media and attachments")
        .iter()
        .filter_map(|file| file["target_path"].as_str())
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(file_paths.len(), sidecars.len() + 1);
    assert!(
        file_paths
            .iter()
            .all(|path| Path::new(path).parent() == Some(Path::new("Organized/Series")))
    );
    for suffix in [".nfo", ".en.srt", ".ass", ".danmaku.xml", ".cover.jpg"] {
        assert!(
            file_paths.iter().any(|path| path.ends_with(suffix)),
            "preview should preserve the longest attachment suffix {suffix}: {file_paths:#?}"
        );
    }
    assert!(
        file_paths.iter().all(|path| {
            let name = Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap();
            name.starts_with("P203 - ") && name.contains("[youtube_dQw4w9WgXcQ] - [1080p h264]")
        }),
        "all sidecars should share the media identity stem: {file_paths:#?}"
    );
    let nfo_target = file_paths
        .iter()
        .find(|path| path.ends_with(".nfo"))
        .expect("preview should retain the NFO attachment")
        .clone();
    assert!(
        !harness.video_root.join("Organized").exists(),
        "preview must not publish files"
    );

    // Publish a valid, byte-identical conflict set only after learning the initial planned paths.
    let mut preexisting_targets = Vec::new();
    for target in &file_paths {
        let source = if target == &media_relative {
            media_source.clone()
        } else {
            let suffix = [".nfo", ".en.srt", ".ass", ".danmaku.xml", ".cover.jpg"]
                .into_iter()
                .find(|suffix| target.ends_with(suffix))
                .expect("planned attachment should have a recognized suffix");
            source_dir.join(format!("{source_stem}{suffix}"))
        };
        let destination = harness.video_root.join(target);
        assert!(
            !destination.exists(),
            "KeepBoth setup must never overwrite an existing target: {target}"
        );
        fs::create_dir_all(destination.parent().unwrap())
            .expect("KeepBoth target directory should create");
        fs::copy(&source, &destination).expect("valid existing target copy should create");
        preexisting_targets.push((target.clone(), fs::read(destination).unwrap()));
    }

    let refreshed_scan = harness
        .request(
            3,
            "library_scan",
            json!({"library_id": harness.library_id.clone()}),
        )
        .await;
    assert_eq!(
        refreshed_scan.status,
        InboxStatus::Completed,
        "{:?}",
        refreshed_scan.error
    );
    let refreshed_result = refreshed_scan
        .result
        .as_ref()
        .expect("refreshed scan result should persist");
    let refreshed_item_id = refreshed_result["items"]
        .as_array()
        .expect("refreshed scan should return library items")
        .iter()
        .find(|item| {
            item["relative_path"]
                .as_str()
                .is_some_and(|path| path.ends_with("long-episode.mp4"))
        })
        .and_then(|item| item["id"].as_str())
        .expect("refreshed scan should preserve the original item's identity")
        .to_string();
    let keep_both_preview = harness
        .request(
            4,
            "file_preview",
            json!({
                "expected_revision": refreshed_result["revision"],
                "item_ids": [refreshed_item_id],
                "target_relative_dir": "Organized",
                "rename": true,
                "conflict": "keep_both"
            }),
        )
        .await;
    assert_eq!(
        keep_both_preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        keep_both_preview.error
    );
    let keep_both_result = keep_both_preview
        .result
        .as_ref()
        .expect("KeepBoth preview result should persist");
    let keep_both_id = keep_both_result["preview_id"]
        .as_str()
        .expect("KeepBoth preview ID should be returned")
        .to_string();
    let keep_both_revision = keep_both_result["revision"]
        .as_str()
        .expect("KeepBoth preview revision should be returned")
        .to_string();
    let keep_both_item = keep_both_result["preview"]["items"]
        .as_array()
        .expect("KeepBoth preview should include its file plan")
        .first()
        .expect("KeepBoth preview should plan the selected item");
    let keep_both_paths = keep_both_item["files"]
        .as_array()
        .expect("KeepBoth preview should include media and attachments")
        .iter()
        .filter_map(|file| file["target_path"].as_str())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let keep_both_media = keep_both_item["target_path"]
        .as_str()
        .expect("KeepBoth media target should be present")
        .to_string();
    assert!(
        keep_both_media.starts_with("Organized/Series/P203 - "),
        "KeepBoth should preserve the flattened episode order: {keep_both_media}"
    );
    let keep_both_name = Path::new(&keep_both_media)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("KeepBoth media path should have a UTF-8 name");
    let keep_both_title = keep_both_name
        .strip_prefix("P203 - ")
        .and_then(|name| name.strip_suffix(" - [youtube_dQw4w9WgXcQ] - [1080p h264] (2).mp4"))
        .expect("KeepBoth should preserve ID and specs before its (2) suffix");
    assert!(
        keep_both_title.ends_with('…'),
        "the re-budgeted title should remain shortened: {keep_both_title}"
    );
    let keep_both_shortened = keep_both_title.trim_end_matches('…');
    assert!(title.starts_with(keep_both_shortened));
    assert!(
        title
            .graphemes(true)
            .scan(String::new(), |prefix, grapheme| {
                prefix.push_str(grapheme);
                Some(prefix == keep_both_shortened)
            })
            .any(|is_boundary| is_boundary),
        "KeepBoth title re-budgeting should stop on a grapheme boundary"
    );
    let keep_both_stem = keep_both_name
        .strip_suffix(".mp4")
        .expect("KeepBoth media name should end in .mp4");
    for suffix in [".nfo", ".en.srt", ".ass", ".danmaku.xml", ".cover.jpg"] {
        assert!(
            keep_both_paths.contains(&format!("Organized/Series/{keep_both_stem}{suffix}")),
            "KeepBoth should use the common (2) stem and preserve {suffix}: {keep_both_paths:#?}"
        );
    }
    let keep_both_nfo = keep_both_paths
        .iter()
        .find(|path| path.ends_with(".nfo"))
        .expect("KeepBoth plan should include the NFO")
        .clone();

    let confirm = harness
        .request(
            5,
            "file_confirm",
            json!({"preview_id": keep_both_id, "revision": keep_both_revision}),
        )
        .await;
    assert_eq!(
        confirm.status,
        InboxStatus::Completed,
        "{:?}",
        confirm.error
    );
    assert_eq!(
        fs::read(harness.video_root.join(&keep_both_media)).unwrap(),
        media_bytes
    );
    let nfo =
        fs::read_to_string(harness.video_root.join(&keep_both_nfo)).expect("moved NFO should read");
    assert!(
        nfo.contains(&format!("<title>{title}</title>")),
        "full title must remain in the NFO"
    );
    assert!(nfo.contains("<uniqueid type=\"youtube\">dQw4w9WgXcQ</uniqueid>"));
    for path in &keep_both_paths {
        assert!(
            harness.video_root.join(path).is_file(),
            "confirmed sidecar should exist: {path}"
        );
    }
    for (path, expected_bytes) in &preexisting_targets {
        assert_eq!(
            fs::read(harness.video_root.join(path)).unwrap(),
            *expected_bytes,
            "KeepBoth must leave pre-existing target bytes unchanged: {path}"
        );
    }
    let old_target_nfo = fs::read_to_string(harness.video_root.join(nfo_target))
        .expect("pre-existing target NFO should remain readable");
    assert!(old_target_nfo.contains(&format!("<title>{title}</title>")));
    assert!(old_target_nfo.contains("<uniqueid type=\"youtube\">dQw4w9WgXcQ</uniqueid>"));
    assert!(
        !nfo_source.exists(),
        "source NFO should move with its media"
    );
    harness.close_runtime().await;
}
