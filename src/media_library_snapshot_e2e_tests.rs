use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::*;
use crate::file_provider::{FileProviderAccessError, QueueFileProvider};
use crate::library::Presence;
use anyhow::Result;

const SNAPSHOT_TEST_CHAT_ID: i64 = 123_456_789;

#[tokio::test]
async fn cloud_snapshot_projects_video_library_ids_and_authorized_incomplete_counts() {
    let root = temp_cloud_bot_dir("library-snapshot-task-projection");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.telegram.allowed_chat_ids = vec![SNAPSHOT_TEST_CHAT_ID];
    config
        .ensure_runtime_dirs()
        .expect("snapshot fixture roots should create");
    let archive_root = root.join("archive");
    fs::create_dir_all(&archive_root).expect("archive root should create");
    fs::write(archive_root.join("queued.mp4"), b"queued media")
        .expect("queued fixture media should write");

    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let source_id = queue
        .default_video_destination()
        .expect("default video library should resolve")
        .library_id;
    let archive = queue
        .register_video_library("Archive", &archive_root, None, false, false)
        .expect("non-default library should register");
    let archive_destination = queue
        .video_destination(&archive.id, None)
        .expect("archive destination should resolve");

    let mut queued = TaskRecord::new(
        "snapshot-queued-video".to_string(),
        1,
        1,
        SNAPSHOT_TEST_CHAT_ID,
        Some(SNAPSHOT_TEST_CHAT_ID),
        0,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=abcdefghijk".to_string(),
        },
    );
    queued.status = TaskStatus::Queued;
    queued.destination = Some(archive_destination);
    // A malformed recorded staging path must not hide the task count. The queue summary API
    // reports the count and marks the byte estimate unknown for this library.
    queued
        .staging_attempts
        .push(archive_root.join("queued.mp4"));
    queue
        .create(queued)
        .expect("queued video task should persist");

    let mut legacy = TaskRecord::new(
        "snapshot-legacy-failed-video".to_string(),
        2,
        2,
        SNAPSHOT_TEST_CHAT_ID,
        Some(SNAPSHOT_TEST_CHAT_ID),
        1,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=zyxwvutsrqp".to_string(),
        },
    );
    legacy.status = TaskStatus::Failed;
    legacy.error = Some("synthetic interrupted video".to_string());
    queue
        .create(legacy)
        .expect("legacy-shaped video task should persist before migration simulation");
    let legacy_path = config
        .downloads
        .video_dir
        .join(".telegram-video-downloader-queue")
        .join("task-snapshot-legacy-failed-video.json");
    let mut legacy_record: serde_json::Value =
        serde_json::from_slice(&fs::read(&legacy_path).expect("legacy task should read"))
            .expect("legacy task JSON should decode");
    legacy_record
        .as_object_mut()
        .expect("task record should be an object")
        .remove("destination");
    fs::write(
        &legacy_path,
        serde_json::to_vec(&legacy_record).expect("legacy record should encode"),
    )
    .expect("legacy task should be rewritten without a frozen destination");

    let mut pdf = TaskRecord::new(
        "snapshot-pdf-failed".to_string(),
        3,
        3,
        SNAPSHOT_TEST_CHAT_ID,
        Some(SNAPSHOT_TEST_CHAT_ID),
        2,
        JobRequest::Pdf {
            url: "https://example.invalid/document.pdf".to_string(),
        },
    );
    pdf.status = TaskStatus::Failed;
    queue.create(pdf).expect("PDF task should persist");

    let mut unauthorized = TaskRecord::new(
        "snapshot-unauthorized-video".to_string(),
        4,
        4,
        9_999,
        Some(9_999),
        3,
        JobRequest::Youtube {
            url: "https://www.youtube.com/watch?v=mnopqrstuvw".to_string(),
        },
    );
    unauthorized.status = TaskStatus::Failed;
    unauthorized.destination = Some(archive_destination_for_test(&queue, &archive.id));
    queue
        .create(unauthorized)
        .expect("unauthorized task should persist");

    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            "test-token".to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let source_cache = empty_cloud_test_library_snapshot("source-cached");
    let archive_cache = empty_cloud_test_library_snapshot("archive-cached");
    *runtime.library_snapshot.lock().await = Some(source_cache.clone());
    runtime
        .library_snapshots
        .lock()
        .await
        .insert(source_id.clone(), source_cache);
    runtime
        .library_snapshots
        .lock()
        .await
        .insert(archive.id.clone(), archive_cache);

    let snapshot = build_cloud_state_snapshot(&runtime)
        .await
        .expect("cloud snapshot should build")
        .expect("runtime should publish a snapshot");
    let tasks = snapshot.tasks.as_slice();
    assert_eq!(
        tasks.len(),
        3,
        "authorized active tasks should be projected"
    );
    let task_by_id = tasks
        .iter()
        .map(|task| (task["id"].as_str().unwrap(), task))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(task_by_id["snapshot-queued-video"]["kind"], "video");
    assert_eq!(
        task_by_id["snapshot-queued-video"]["library_id"],
        archive.id
    );
    assert_eq!(task_by_id["snapshot-legacy-failed-video"]["kind"], "video");
    assert_eq!(
        task_by_id["snapshot-legacy-failed-video"]["library_id"],
        source_id
    );
    assert_eq!(task_by_id["snapshot-pdf-failed"]["kind"], "pdf");
    assert!(task_by_id["snapshot-pdf-failed"]["library_id"].is_null());
    assert!(task_by_id["snapshot-queued-video"].get("root").is_none());
    assert!(!task_by_id.contains_key("snapshot-unauthorized-video"));

    let libraries = snapshot.library["libraries"]
        .as_array()
        .expect("library descriptors should be projected");
    let source_view = libraries
        .iter()
        .find(|view| view["id"] == source_id)
        .expect("source library view should exist");
    let archive_view = libraries
        .iter()
        .find(|view| view["id"] == archive.id)
        .expect("archive library view should exist");
    assert_eq!(source_view["incomplete_task_count"], 1);
    assert_eq!(archive_view["incomplete_task_count"], 1);
    assert!(
        source_view["staging_bytes"].as_u64().is_some(),
        "legacy task without fragments should have a known estimate"
    );
    assert!(
        archive_view["staging_bytes"].is_null(),
        "one malformed recorded staging path should preserve count but make bytes unknown"
    );

    let mut allow_all_runtime = runtime.clone();
    let mut allow_all_config = (*runtime.bot.config).clone();
    allow_all_config.telegram.allow_all_chats = true;
    allow_all_runtime.bot.config = Arc::new(allow_all_config);
    let private_snapshot = build_cloud_state_snapshot(&allow_all_runtime)
        .await
        .expect("allow-all snapshot should build")
        .expect("runtime should publish an allow-all snapshot");
    assert!(private_snapshot.tasks.is_empty());
    for view in private_snapshot.library["libraries"].as_array().unwrap() {
        assert!(view["incomplete_task_count"].is_null());
        assert!(view["staging_bytes"].is_null());
    }

    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = fs::remove_dir_all(root);
}

fn archive_destination_for_test(
    queue: &QueueManager,
    library_id: &str,
) -> crate::media_libraries::TaskDestination {
    queue
        .video_destination(library_id, None)
        .expect("archive destination should resolve")
}

struct FailOnceOnPathProvider {
    path: PathBuf,
    armed: AtomicBool,
    failures: AtomicUsize,
}

impl FailOnceOnPathProvider {
    fn new(path: PathBuf) -> Self {
        Self {
            path: fs::canonicalize(&path).unwrap_or(path),
            armed: AtomicBool::new(false),
            failures: AtomicUsize::new(0),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl QueueFileProvider for FailOnceOnPathProvider {
    fn coordinate_read(
        &self,
        path: &Path,
        accessor: &mut dyn FnMut(&Path) -> Result<()>,
    ) -> Result<()> {
        let normalized_path = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        if normalized_path == self.path && self.armed.swap(false, Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(FileProviderAccessError::new(
                path,
                "read",
                "synthetic media-library root open failure",
            )
            .into());
        }
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
        accessor(source, destination)
    }
}

#[tokio::test]
async fn failed_non_default_library_open_keeps_cache_unavailable_until_scoped_recovery() {
    let root = temp_cloud_bot_dir("library-snapshot-scoped-open-recovery");
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config.telegram.allowed_chat_ids = vec![SNAPSHOT_TEST_CHAT_ID];
    config
        .ensure_runtime_dirs()
        .expect("snapshot fixture roots should create");
    let archive_root = root.join("archive");
    fs::create_dir_all(&archive_root).expect("archive root should create");
    let child_media = archive_root.join("clip.mp4");
    fs::write(&child_media, b"synthetic media bytes").expect("child media should write");
    fs::write(
        archive_root.join("clip.nfo"),
        b"<movie><title>Cached archive item</title></movie>",
    )
    .expect("child metadata should write");
    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let default_id = queue
        .default_video_destination()
        .expect("default video library should resolve")
        .library_id;
    let archive = queue
        .register_video_library("Archive", &archive_root, None, false, false)
        .expect("archive library should register");
    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("cloud inbox should open"),
    );
    let runtime = make_runtime(
        TelegramClient::with_test_api_base_url(
            "test-token".to_string(),
            "http://127.0.0.1:1".to_string(),
        ),
        config,
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );

    let default_failure =
        run_cloud_library_operation::<()>(&runtime, "synthetic default scan failed", |_, _| {
            Err(anyhow::anyhow!("synthetic default scan failure"))
        })
        .await;
    assert!(default_failure.is_err());
    let default_warning = runtime
        .library_scan_warning
        .lock()
        .await
        .clone()
        .expect("default scan failure should set the global warning");

    let provider = Arc::new(FailOnceOnPathProvider::new(archive_root.clone()));
    let (archive_runtime, scoped_id) =
        media_library_bot::scoped_runtime(&runtime, Some(&archive.id))
            .expect("archive runtime should scope");
    assert_eq!(scoped_id, archive.id);
    let scan = |provider: Arc<FailOnceOnPathProvider>| {
        let runtime = archive_runtime.clone();
        async move {
            run_cloud_library_operation(
                &runtime,
                "archive library open failed",
                move |context, inbox| {
                    let manager =
                        open_cloud_library_manager(&context, inbox, Some(provider.clone()))?;
                    let snapshot = manager.scan(&[])?;
                    Ok((snapshot.clone(), snapshot))
                },
            )
            .await
        }
    };

    let first = scan(Arc::clone(&provider))
        .await
        .expect("non-default library scan should succeed");
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].presence, Presence::Present);
    let cached = runtime
        .library_snapshots
        .lock()
        .await
        .get(&archive.id)
        .cloned()
        .expect("successful archive snapshot should be cached");
    assert_eq!(cached.items[0].relative_path, "clip.mp4");
    assert_eq!(
        runtime.library_scan_warning.lock().await.as_deref(),
        Some(default_warning.as_str()),
        "a non-default library success must preserve the default warning"
    );

    provider.arm();
    let failed = scan(Arc::clone(&provider)).await;
    assert!(
        failed.is_err(),
        "the injected library-root read denial should fail manager open"
    );
    assert_eq!(
        provider.failures.load(Ordering::SeqCst),
        1,
        "the provider should inject exactly one library-root denial"
    );
    assert!(
        runtime
            .library_unavailable
            .lock()
            .await
            .contains_key(&archive.id),
        "the selected non-default library should be marked unavailable"
    );
    assert_eq!(
        runtime.library_snapshots.lock().await.get(&archive.id),
        Some(&cached),
        "the failed scan should retain the last successful cached snapshot"
    );
    assert_eq!(
        runtime.library_scan_warning.lock().await.as_deref(),
        Some(default_warning.as_str()),
        "a non-default failure must not replace the default library warning"
    );

    let applied =
        media_library_bot::applied_settings(&queue).expect("library settings should load");
    let projected = media_library_bot::attach_library_views(
        &runtime,
        &applied,
        serde_json::to_value(empty_cloud_test_library_snapshot("legacy"))
            .expect("legacy snapshot should encode"),
    )
    .await
    .expect("scoped library views should build after scan failure");
    let archive_view = projected["libraries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|view| view["id"] == archive.id)
        .unwrap();
    assert_eq!(archive_view["availability"], "unavailable");
    assert_eq!(archive_view["snapshot"]["revision"], cached.revision);
    assert_eq!(
        archive_view["snapshot"]["items"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        archive_view["snapshot"]["items"][0]["presence"],
        "unavailable"
    );

    let recovered = scan(Arc::clone(&provider))
        .await
        .expect("the next non-default scan should recover");
    assert_eq!(
        provider.failures.load(Ordering::SeqCst),
        1,
        "the healthy recovery scan should not trigger another denial"
    );
    assert_eq!(recovered.items[0].presence, Presence::Present);
    assert!(
        !runtime
            .library_unavailable
            .lock()
            .await
            .contains_key(&archive.id),
        "a successful scoped recovery should clear only the archive warning"
    );
    assert!(
        runtime
            .library_unavailable
            .lock()
            .await
            .contains_key(&default_id),
        "recovery in another library must preserve the default library warning"
    );
    assert_eq!(
        runtime.library_scan_warning.lock().await.as_deref(),
        Some(default_warning.as_str())
    );

    drop(runtime);
    drop(inbox);
    drop(queue);
    let _ = fs::remove_dir_all(root);
}
