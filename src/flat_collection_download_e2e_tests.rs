//! End-to-end tests for flattened collection staging and publication.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

const TEST_PUBLICATION_NAMESPACE: &str = "test-flat-collection-e2e";
const TEST_MEDIA_PROFILE: &str = "test-flat-collection-media-profile-v1";

struct TestDirectory(PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn temp_root(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "telegram-video-downloader-{label}-{}-{nanos}",
        std::process::id()
    ))
}

fn write_fixture(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes)
        .unwrap_or_else(|error| panic!("failed to write test fixture {}: {error}", path.display()));
}

#[test]
fn flat_collection_keep_both_publishes_one_coherent_sidecar_group() {
    let output_root = temp_root("flat-collection-keep-both");
    let _cleanup = TestDirectory(output_root.clone());
    let collection_folder = "Flat Collection";
    let collection_dir = output_root.join(collection_folder);
    fs::create_dir_all(&collection_dir).expect("collection output folder should be created");

    let stem = "E0002-P003-bvid-BV1-cid-456-epid-none-Another episode";
    let original_media = collection_dir.join(format!("{stem}.mkv"));
    let original_nfo = collection_dir.join(format!("{stem}.nfo"));
    let partial_second_group = collection_dir.join(format!("{stem} (2).zh-CN-01-oldhash.srt"));
    write_fixture(&original_media, b"pre-existing media");
    write_fixture(&original_nfo, b"pre-existing metadata");
    write_fixture(&partial_second_group, b"pre-existing partial subtitle");

    let root = RootedFs::new(&output_root).expect("library root should bind");
    let collection_identity = root
        .entry_identity(&collection_dir)
        .expect("collection identity should be readable")
        .expect("collection directory should exist");
    let staging = create_video_staging_dir(&root).expect("private staging should be created");
    let staging_dir = staging.path().to_path_buf();
    let staged_collection_dir = staging_dir.join(collection_folder);
    fs::create_dir(&staged_collection_dir).expect("staged collection folder should be created");

    let fixture_bytes = [
        ("mkv", b"new primary media".as_slice()),
        ("nfo", b"new metadata".as_slice()),
        ("danmaku.xml", b"new danmaku xml".as_slice()),
        ("danmaku.ass", b"new danmaku ass".as_slice()),
        ("zh-CN-01-newhash.srt", b"new subtitle".as_slice()),
        ("cover-deadbeef.jpg", b"new artwork".as_slice()),
    ];
    let staged_files = fixture_bytes
        .iter()
        .map(|(suffix, bytes)| {
            let path = staged_collection_dir.join(format!("{stem}.{suffix}"));
            write_fixture(&path, bytes);
            path
        })
        .collect::<Vec<_>>();

    let expected_entry = BilibiliWorkerExpectedEntry {
        index: 2,
        part_index: 3,
        title: "Another episode".to_string(),
        stable_media_id: bilibili_plan_stable_media_id(Some("BV1"), 999, 456, None),
        bilibili_publication_namespace: Some(TEST_PUBLICATION_NAMESPACE.to_string()),
        output_template: Some(collection_folder.to_string()),
        collection_output_folder: Some(collection_folder.to_string()),
        collection_output_layout_version: Some(1),
        collection_directory: BilibiliCollectionDirectorySnapshot::Existing {
            path: collection_folder.to_string(),
            device: collection_identity.device(),
            inode: collection_identity.inode(),
            is_directory: collection_identity.is_dir(),
        },
        confirmed_streams: None,
    };
    assert_eq!(expected_entry.stable_media_id, "bvid:BV1:cid:456:epid:none");
    let duplicate = VideoDuplicate {
        identity: VideoIdentity {
            provider: VideoProvider::Bilibili,
            id: expected_entry.stable_media_id.clone(),
        },
        existing_videos: Vec::new(),
        skip_summary: None,
        overwrite_confirmation: None,
    };

    let publication = move_staged_video_files_with_root_and_sources(
        MoveExecutionContext {
            root: &root,
            staging: Some(&staging),
            bilibili_group_receipt: Some(BilibiliGroupReceiptWriteContext {
                expected_entry: &expected_entry,
                media_profile: TEST_MEDIA_PROFILE,
                progress: None,
            }),
        },
        &staging_dir,
        &output_root,
        &staged_files,
        VideoDuplicateAction::KeepBoth,
        &duplicate,
        StagedPrimaryMediaKind::Video,
    )
    .expect("the production staged publication path should publish the complete fixture");

    let expected_stem = format!("{stem} (3)");
    let expected_targets = fixture_bytes
        .iter()
        .map(|(suffix, _)| collection_dir.join(format!("{expected_stem}.{suffix}")))
        .collect::<Vec<_>>();
    assert_eq!(
        publication.primary_destinations,
        vec![collection_dir.join(format!("{expected_stem}.mkv"))]
    );
    for (target, (_, expected_bytes)) in expected_targets.iter().zip(fixture_bytes) {
        assert_eq!(target.parent(), Some(collection_dir.as_path()));
        assert_eq!(
            fs::read(target).expect("published fixture should be readable"),
            expected_bytes
        );
    }
    for source in &staged_files {
        assert!(!source.exists(), "published source should leave staging");
    }
    assert_eq!(fs::read(&original_media).unwrap(), b"pre-existing media");
    assert_eq!(fs::read(&original_nfo).unwrap(), b"pre-existing metadata");
    assert_eq!(
        fs::read(&partial_second_group).unwrap(),
        b"pre-existing partial subtitle"
    );

    let receipt = publication
        .bilibili_group_receipt
        .as_ref()
        .expect("collection publication should persist its recovery receipt");
    verify_bilibili_group_publication_plan_receipt_blocking(
        &output_root,
        receipt,
        &publication.primary_destinations,
        Some(TEST_PUBLICATION_NAMESPACE),
        Some(collection_folder),
    )
    .expect("the durable publication receipt should verify the new media path");

    staging
        .finish()
        .expect("published staging tree should be removed");
    fs::remove_dir_all(&output_root).expect("isolated fixture root should be removed");
}

#[test]
fn sdk_entry_output_flattens_then_publishes_flat_collection_sidecars_together() {
    let output_root = temp_root("flat-collection-sdk-entry");
    let _cleanup = TestDirectory(output_root.clone());
    let collection_folder = "SDK Collection";
    let collection_dir = output_root.join(collection_folder);
    fs::create_dir_all(&collection_dir).expect("collection output folder should be created");
    let stem = "E0002-P003-bvid-BV1-cid-456-epid-none-Another episode";
    write_fixture(
        &collection_dir.join(format!("{stem}.mkv")),
        b"existing media to keep",
    );
    write_fixture(
        &collection_dir.join(format!("{stem}.nfo")),
        b"existing metadata to keep",
    );
    write_fixture(
        &collection_dir.join(format!("{stem} (2).zh-CN-01-oldhash.srt")),
        b"incomplete prior subtitle",
    );

    let root = RootedFs::new(&output_root).expect("library root should bind");
    let collection_identity = root
        .entry_identity(&collection_dir)
        .expect("collection identity should be readable")
        .expect("collection directory should exist");
    let staging = create_video_staging_dir(&root).expect("private staging should be created");
    let output_dir = staging.path().to_path_buf();
    let staged_collection_dir = output_dir.join(collection_folder);
    let sdk_entry_dir = staged_collection_dir.join("P003-BV1-Another episode");
    fs::create_dir(&staged_collection_dir)
        .expect("staged collection output directory should be created");
    fs::create_dir(&sdk_entry_dir).expect("SDK entry output directory should be created");

    let sdk_files = [
        ("video.mkv", "video", b"SDK primary media".as_slice()),
        ("metadata.nfo", "nfo", b"SDK NFO sidecar".as_slice()),
        ("danmaku.xml", "danmaku", b"SDK danmaku XML".as_slice()),
        ("danmaku.ass", "danmaku_ass", b"SDK danmaku ASS".as_slice()),
        (
            "subtitle-zh-CN-01-trackhash.srt",
            "subtitle",
            b"SDK language subtitle".as_slice(),
        ),
        (
            "cover-image-deadbeef.jpg",
            "cover",
            b"SDK artwork".as_slice(),
        ),
    ];
    let sdk_paths = sdk_files
        .iter()
        .map(|(name, _, bytes)| {
            let path = sdk_entry_dir.join(name);
            write_fixture(&path, bytes);
            path
        })
        .collect::<Vec<_>>();
    let stable_media_id = bilibili_plan_stable_media_id(Some("BV1"), 999, 456, None);
    let plan = BilibiliDownloadPlan {
        title: "SDK Collection".to_string(),
        entries: vec![BilibiliDownloadEntry {
            index: 2,
            aid: 999,
            bvid: Some("BV1".to_string()),
            cid: 456,
            epid: None,
            title: "Another episode".to_string(),
            ..BilibiliDownloadEntry::default()
        }],
    };
    let mut report = BilibiliDownloadReport {
        title: "SDK Collection".to_string(),
        output_dir: output_dir.clone(),
        entries: vec![BilibiliEntryDownloadReport {
            index: 2,
            title: "Another episode".to_string(),
            files: sdk_files
                .iter()
                .zip(&sdk_paths)
                .map(|((_, kind, _), path)| BilibiliDownloadedFile {
                    kind: (*kind).to_string(),
                    path: path.clone(),
                })
                .collect(),
            mux: None,
        }],
    };
    let expected_entry = BilibiliWorkerExpectedEntry {
        index: 2,
        part_index: 3,
        title: "Another episode".to_string(),
        stable_media_id,
        bilibili_publication_namespace: Some(TEST_PUBLICATION_NAMESPACE.to_string()),
        output_template: Some(collection_folder.to_string()),
        collection_output_folder: Some(collection_folder.to_string()),
        collection_output_layout_version: Some(1),
        collection_directory: BilibiliCollectionDirectorySnapshot::Existing {
            path: collection_folder.to_string(),
            device: collection_identity.device(),
            inode: collection_identity.inode(),
            is_directory: collection_identity.is_dir(),
        },
        confirmed_streams: None,
    };

    let remapped = flatten_bilibili_collection_staging_output(
        &root,
        &output_dir,
        &plan,
        &mut report,
        Some(&expected_entry),
    )
    .expect("the production SDK-entry mapper should flatten the report output");
    assert_eq!(remapped.len(), sdk_files.len());
    let remapped_paths = remapped.values().cloned().collect::<BTreeSet<_>>();
    let reported_paths = report.entries[0]
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(reported_paths, remapped_paths);
    for (source, (_, _, bytes)) in sdk_paths.iter().zip(sdk_files) {
        let destination = remapped
            .get(source)
            .expect("every SDK file should have a flat destination");
        assert_eq!(destination.parent(), Some(staged_collection_dir.as_path()));
        assert!(
            destination
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(stem)),
            "flat outputs should share the expected media identity stem"
        );
        assert_eq!(fs::read(destination).unwrap(), bytes);
        assert!(!source.exists(), "mapper should move each SDK output");
    }
    assert!(
        report.entries[0]
            .files
            .iter()
            .all(|file| file.path.parent() == Some(staged_collection_dir.as_path())),
        "the report should point at the flattened staging paths"
    );

    let flat_staged_files = remapped.values().cloned().collect::<Vec<_>>();
    let duplicate = VideoDuplicate {
        identity: VideoIdentity {
            provider: VideoProvider::Bilibili,
            id: expected_entry.stable_media_id.clone(),
        },
        existing_videos: Vec::new(),
        skip_summary: None,
        overwrite_confirmation: None,
    };
    let publication = move_staged_video_files_with_root_and_sources(
        MoveExecutionContext {
            root: &root,
            staging: Some(&staging),
            bilibili_group_receipt: Some(BilibiliGroupReceiptWriteContext {
                expected_entry: &expected_entry,
                media_profile: TEST_MEDIA_PROFILE,
                progress: None,
            }),
        },
        &output_dir,
        &output_root,
        &flat_staged_files,
        VideoDuplicateAction::KeepBoth,
        &duplicate,
        StagedPrimaryMediaKind::Video,
    )
    .expect("the real staged mover should publish the flattened SDK result");

    let published_stem = format!("{stem} (3)");
    let expected_published = sdk_files
        .iter()
        .map(|(name, _, _)| {
            let suffix = Path::new(name)
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap();
            match *name {
                "video.mkv" => collection_dir.join(format!("{published_stem}.mkv")),
                "metadata.nfo" => collection_dir.join(format!("{published_stem}.nfo")),
                "danmaku.xml" => collection_dir.join(format!("{published_stem}.danmaku.xml")),
                "danmaku.ass" => collection_dir.join(format!("{published_stem}.danmaku.ass")),
                "subtitle-zh-CN-01-trackhash.srt" => {
                    collection_dir.join(format!("{published_stem}.zh-CN-01-trackhash.{suffix}"))
                }
                "cover-image-deadbeef.jpg" => {
                    collection_dir.join(format!("{published_stem}.cover-deadbeef.{suffix}"))
                }
                _ => unreachable!("fixture sidecar name was listed above"),
            }
        })
        .collect::<Vec<_>>();
    for (target, (_, _, bytes)) in expected_published.iter().zip(sdk_files) {
        assert_eq!(target.parent(), Some(collection_dir.as_path()));
        assert_eq!(fs::read(target).unwrap(), bytes);
    }
    assert_eq!(
        publication.primary_destinations,
        vec![collection_dir.join(format!("{published_stem}.mkv"))]
    );
    assert_eq!(
        fs::read(collection_dir.join(format!("{stem}.mkv"))).unwrap(),
        b"existing media to keep"
    );
    assert_eq!(
        fs::read(collection_dir.join(format!("{stem}.nfo"))).unwrap(),
        b"existing metadata to keep"
    );
    assert_eq!(
        fs::read(collection_dir.join(format!("{stem} (2).zh-CN-01-oldhash.srt"))).unwrap(),
        b"incomplete prior subtitle"
    );

    let receipt = publication
        .bilibili_group_receipt
        .as_ref()
        .expect("flattened collection publication should persist a receipt");
    verify_bilibili_group_publication_plan_receipt_blocking(
        &output_root,
        receipt,
        &publication.primary_destinations,
        Some(TEST_PUBLICATION_NAMESPACE),
        Some(collection_folder),
    )
    .expect("the flattened collection receipt should verify after publication");

    staging
        .finish()
        .expect("published staging tree should be removed");
    fs::remove_dir_all(&output_root).expect("isolated fixture root should be removed");
}

#[test]
fn direct_collection_mapper_and_publication_keep_multiple_videos_flat() {
    let output_root = temp_root("flat-collection-multiple-videos");
    let _cleanup = TestDirectory(output_root.clone());
    let collection_folder = "Multi Video Collection";
    let collection_dir = output_root.join(collection_folder);
    fs::create_dir_all(&collection_dir).expect("collection output folder should be created");
    let root = RootedFs::new(&output_root).expect("library root should bind");
    let staging = create_video_staging_dir(&root).expect("private staging should be created");
    let output_dir = staging.path().to_path_buf();
    let staged_collection_dir = output_dir.join(collection_folder);
    fs::create_dir(&staged_collection_dir).expect("staged collection folder should be created");

    let specs = [
        (1, "BV1", 501, "First episode", b"first media".as_slice()),
        (2, "BV2", 502, "Second episode", b"second media".as_slice()),
    ];
    let mut plan_entries = Vec::new();
    let mut report_entries = Vec::new();
    let mut source_groups = Vec::new();
    for (index, bvid, cid, title, media_bytes) in specs {
        let sdk_entry_dir = staged_collection_dir.join(format!("P{index:03}-{bvid}-{title}"));
        fs::create_dir(&sdk_entry_dir).expect("SDK entry output directory should be created");
        let sidecars = [
            ("video.mkv", "video", media_bytes),
            ("metadata.nfo", "nfo", b"metadata".as_slice()),
            (
                "subtitle-en-US-01-trackhash.srt",
                "subtitle",
                b"language subtitle".as_slice(),
            ),
        ];
        let files = sidecars
            .iter()
            .map(|(name, _, bytes)| {
                let path = sdk_entry_dir.join(name);
                write_fixture(&path, bytes);
                path
            })
            .collect::<Vec<_>>();
        source_groups.push(files.clone());
        plan_entries.push(BilibiliDownloadEntry {
            index,
            aid: 1000 + u64::from(index),
            bvid: Some(bvid.to_string()),
            cid,
            epid: None,
            title: title.to_string(),
            ..BilibiliDownloadEntry::default()
        });
        report_entries.push(BilibiliEntryDownloadReport {
            index,
            title: title.to_string(),
            files: sidecars
                .iter()
                .zip(files)
                .map(|((_, kind, _), path)| BilibiliDownloadedFile {
                    kind: (*kind).to_string(),
                    path,
                })
                .collect(),
            mux: None,
        });
    }
    let plan = BilibiliDownloadPlan {
        title: "Multi Video Collection".to_string(),
        entries: plan_entries,
    };
    let mut report = BilibiliDownloadReport {
        title: "Multi Video Collection".to_string(),
        output_dir: output_dir.clone(),
        entries: report_entries,
    };

    let remapped =
        flatten_bilibili_collection_staging_output(&root, &output_dir, &plan, &mut report, None)
            .expect("the production direct-collection mapper should flatten every reported video");
    assert_eq!(remapped.len(), 6);
    assert_eq!(report.entries.len(), 2);
    assert!(
        report.entries.iter().all(|entry| entry
            .files
            .iter()
            .all(|file| file.path.parent() == Some(staged_collection_dir.as_path()))),
        "each collection report should point directly inside the collection folder"
    );

    let staged_files = remapped.values().cloned().collect::<Vec<_>>();
    let duplicate = VideoDuplicate {
        identity: VideoIdentity {
            provider: VideoProvider::Bilibili,
            id: "collection-fixture".to_string(),
        },
        existing_videos: Vec::new(),
        skip_summary: None,
        overwrite_confirmation: None,
    };
    let publication = move_staged_video_files_with_root_and_sources(
        MoveExecutionContext {
            root: &root,
            staging: Some(&staging),
            bilibili_group_receipt: None,
        },
        &output_dir,
        &output_root,
        &staged_files,
        VideoDuplicateAction::KeepBoth,
        &duplicate,
        StagedPrimaryMediaKind::Video,
    )
    .expect("the real staged mover should publish both collection videos");

    let expected_media = specs
        .iter()
        .map(|(index, bvid, cid, title, media_bytes)| {
            let stem = format!("E{index:04}-P{index:03}-bvid-{bvid}-cid-{cid}-epid-none-{title}");
            let media = collection_dir.join(format!("{stem}.mkv"));
            assert_eq!(media.parent(), Some(collection_dir.as_path()));
            assert_eq!(fs::read(&media).unwrap(), *media_bytes);
            assert_eq!(
                fs::read(collection_dir.join(format!("{stem}.nfo"))).unwrap(),
                b"metadata"
            );
            assert_eq!(
                fs::read(collection_dir.join(format!("{stem}.en-US-01-trackhash.srt"))).unwrap(),
                b"language subtitle"
            );
            media
        })
        .collect::<Vec<_>>();
    let actual_media = publication
        .primary_destinations
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected_media = expected_media.into_iter().collect::<BTreeSet<_>>();
    assert_eq!(actual_media, expected_media);
    for group in source_groups {
        for source in group {
            assert!(!source.exists(), "published source should leave staging");
        }
    }

    staging
        .finish()
        .expect("published staging tree should be removed");
    fs::remove_dir_all(&output_root).expect("isolated fixture root should be removed");
}

fn flat_test_group_entry(index: u32, bvid: &str, cid: u64, title: &str) -> BilibiliGroupEntry {
    let download = BilibiliDownloadEntry {
        index,
        aid: 1000 + u64::from(index),
        bvid: Some(bvid.to_string()),
        cid,
        epid: None,
        title: title.to_string(),
        ..BilibiliDownloadEntry::default()
    };
    let stable_media_id = bilibili_plan_stable_media_id(
        download.bvid.as_deref(),
        download.aid,
        download.cid,
        download.epid,
    );
    BilibiliGroupEntry {
        download,
        stable_media_id,
    }
}

#[tokio::test]
async fn collection_group_driver_publishes_two_mock_sdk_children_with_page_stems_and_receipts() {
    let output_root = temp_root("flat-collection-group-driver");
    let _cleanup = TestDirectory(output_root.clone());
    let collection_folder = "Grouped Collection".to_string();
    let collection_dir = output_root.join(&collection_folder);
    fs::create_dir_all(&collection_dir).expect("collection output folder should be created");
    let root = RootedFs::new(&output_root).expect("library root should bind");
    let collection_identity = root
        .entry_identity(&collection_dir)
        .expect("collection identity should be readable")
        .expect("collection directory should exist");

    let entries = vec![
        flat_test_group_entry(1, "BV1", 501, "First episode"),
        flat_test_group_entry(2, "BV2", 502, "Second episode"),
    ];
    let inventory = BilibiliEpisodeGroupInventory {
        title: "Grouped Collection".to_string(),
        source: BilibiliGroupSource::Collection,
        pgc_season_id: None,
        entries: entries.clone(),
        output_template: Some(collection_folder.clone()),
        output_folder: Some(collection_folder.clone()),
        storage_relative_output_folder: Some(collection_folder.clone()),
        output_folder_identity: Some(collection_identity),
        collection_output_layout_version: Some(1),
    };
    let mut config =
        AppConfig::load(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.toml"))
            .expect("example config should parse");
    config.downloads.video_dir = output_root.clone();
    config.set_video_storage_root(output_root.clone());
    config.bilibili.auth.state_path = output_root.join("test-auth-state.json");

    let executed = Arc::new(StdMutex::new(Vec::<u32>::new()));
    let executor_calls = Arc::clone(&executed);
    let executor_root = output_root.clone();
    let executor_collection = collection_folder.clone();
    let execute_entry: BilibiliGroupEntryExecutor = Arc::new(move |entry| {
        let calls = Arc::clone(&executor_calls);
        let output_root = executor_root.clone();
        let collection_folder = executor_collection.clone();
        Box::pin(async move {
            calls
                .lock()
                .expect("mock group executor call list should remain healthy")
                .push(entry.download.index);
            let root = RootedFs::new(&output_root)?;
            let staging = create_video_staging_dir(&root)?;
            let output_dir = staging.path().to_path_buf();
            let sdk_collection_dir = output_dir.join(&collection_folder);
            let part_index = entry.download.index + 2;
            let sdk_entry_dir = sdk_collection_dir.join(format!(
                "P{part_index:03}-{}-{}",
                entry.download.cid, entry.download.title
            ));
            fs::create_dir_all(&sdk_entry_dir)?;
            let media_bytes = format!("synthetic bytes for {}", entry.stable_media_id).into_bytes();
            let nfo_bytes = format!(
                "<movie><uniqueid type=\"bilibili\">{}</uniqueid><uniqueid type=\"bilibili-cid\">cid{}</uniqueid></movie>",
                entry.download.bvid.as_deref().unwrap_or_default(),
                entry.download.cid
            )
            .into_bytes();
            let sdk_files = [
                ("video.mkv", "video", media_bytes.as_slice()),
                ("metadata.nfo", "nfo", nfo_bytes.as_slice()),
                ("danmaku.xml", "danmaku", b"mock danmaku".as_slice()),
                (
                    "subtitle-zh-CN-01-trackhash.srt",
                    "subtitle",
                    b"mock language subtitle".as_slice(),
                ),
                (
                    "cover-image-deadbeef.jpg",
                    "cover",
                    b"mock artwork".as_slice(),
                ),
            ];
            let sdk_paths = sdk_files
                .iter()
                .map(|(name, _, bytes)| {
                    let path = sdk_entry_dir.join(name);
                    write_fixture(&path, bytes);
                    path
                })
                .collect::<Vec<_>>();
            let plan = BilibiliDownloadPlan {
                title: collection_folder.clone(),
                entries: vec![entry.download.clone()],
            };
            let mut report = BilibiliDownloadReport {
                title: collection_folder.clone(),
                output_dir: output_dir.clone(),
                entries: vec![BilibiliEntryDownloadReport {
                    index: entry.download.index,
                    title: entry.download.title.clone(),
                    files: sdk_files
                        .iter()
                        .zip(&sdk_paths)
                        .map(|((_, kind, _), path)| BilibiliDownloadedFile {
                            kind: (*kind).to_string(),
                            path: path.clone(),
                        })
                        .collect(),
                    mux: None,
                }],
            };
            let expected_entry = BilibiliWorkerExpectedEntry {
                index: entry.download.index,
                part_index,
                title: entry.download.title.clone(),
                stable_media_id: entry.stable_media_id.clone(),
                bilibili_publication_namespace: Some(TEST_PUBLICATION_NAMESPACE.to_string()),
                output_template: Some(collection_folder.clone()),
                collection_output_folder: Some(collection_folder.clone()),
                collection_output_layout_version: Some(1),
                collection_directory: BilibiliCollectionDirectorySnapshot::Existing {
                    path: collection_folder.clone(),
                    device: collection_identity.device(),
                    inode: collection_identity.inode(),
                    is_directory: true,
                },
                confirmed_streams: None,
            };
            let remapped = flatten_bilibili_collection_staging_output(
                &root,
                &output_dir,
                &plan,
                &mut report,
                Some(&expected_entry),
            )?;
            let staged_files = remapped.values().cloned().collect::<Vec<_>>();
            let duplicate = VideoDuplicate {
                identity: VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: entry.stable_media_id.clone(),
                },
                existing_videos: Vec::new(),
                skip_summary: None,
                overwrite_confirmation: None,
            };
            let publication = move_staged_video_files_with_root_and_sources(
                MoveExecutionContext {
                    root: &root,
                    staging: Some(&staging),
                    bilibili_group_receipt: Some(BilibiliGroupReceiptWriteContext {
                        expected_entry: &expected_entry,
                        media_profile: TEST_MEDIA_PROFILE,
                        progress: None,
                    }),
                },
                &output_dir,
                &output_root,
                &staged_files,
                VideoDuplicateAction::KeepBoth,
                &duplicate,
                StagedPrimaryMediaKind::Video,
            )?;
            let primary_paths = publication.primary_destinations;
            staging.finish()?;
            Ok(JobReport {
                saved_location: join_paths(&primary_paths),
                details: "mock SDK output published through collection staging".to_string(),
                primary_media_paths: primary_paths,
                existing_bilibili_evidence: None,
            })
        })
    });
    let scanner: BilibiliGroupExistingMediaScanner = Arc::new(|_| Box::pin(async { Ok(None) }));
    let hooks = BilibiliGroupExecutionHooks {
        execute_entry,
        find_existing_media: scanner,
        record_published_evidence: Arc::new(|_| Box::pin(async { Ok(()) })),
        wait_before_retry: Arc::new(|_, _, _, _, _| Box::pin(async {})),
    };
    let (progress, _receiver) = job_progress_channel();

    let report = run_bilibili_episode_group_with_inventory(
        &config,
        &JobRequest::Bilibili {
            url: "https://www.bilibili.com/medialist/play/ml-test".to_string(),
            selection: Some(BilibiliSelection::All),
        },
        inventory,
        None,
        None,
        Some(progress.clone()),
        hooks,
    )
    .await
    .expect("the real collection group driver should complete two mock SDK children");

    assert_eq!(*executed.lock().unwrap(), vec![1, 2]);
    assert_eq!(report.primary_media_paths.len(), 2);
    for entry in &entries {
        let part_index = entry.download.index + 2;
        let stem = format!(
            "E{:04}-P{part_index:03}-{}-{}",
            entry.download.index,
            entry.stable_media_id.replace(':', "-"),
            entry.download.title
        );
        let media = collection_dir.join(format!("{stem}.mkv"));
        assert_eq!(media.parent(), Some(collection_dir.as_path()));
        assert_eq!(
            fs::read(&media).unwrap(),
            format!("synthetic bytes for {}", entry.stable_media_id).as_bytes()
        );
        assert!(collection_dir.join(format!("{stem}.nfo")).is_file());
        assert!(collection_dir.join(format!("{stem}.danmaku.xml")).is_file());
        assert!(
            collection_dir
                .join(format!("{stem}.zh-CN-01-trackhash.srt"))
                .is_file()
        );
        assert!(
            collection_dir
                .join(format!("{stem}.cover-deadbeef.jpg"))
                .is_file()
        );
        let receipt = match load_bilibili_group_publication_receipt_blocking(
            &output_root,
            &entry.stable_media_id,
            TEST_MEDIA_PROFILE,
            Some(TEST_PUBLICATION_NAMESPACE),
            Some(&collection_folder),
        )
        .unwrap()
        {
            BilibiliGroupReceiptLookup::Found(receipt) => receipt,
            BilibiliGroupReceiptLookup::Missing => {
                panic!("expected a durable group publication receipt, but none was saved")
            }
            BilibiliGroupReceiptLookup::ProfileMismatch => {
                panic!("expected a durable group publication receipt with the test profile")
            }
        };
        verify_bilibili_group_publication_plan_receipt_blocking(
            &output_root,
            &receipt,
            std::slice::from_ref(&media),
            Some(TEST_PUBLICATION_NAMESPACE),
            Some(&collection_folder),
        )
        .expect("each group receipt should verify the direct collection media path");
    }
    let snapshot = progress
        .bilibili_attempt_snapshot()
        .expect("group attempt snapshot should persist through the progress sender");
    assert_eq!(snapshot.outcome, Some(BilibiliAttemptOutcome::Verified));
    assert!(
        snapshot
            .entries
            .iter()
            .all(|entry| entry.state == BilibiliAttemptEntryState::Verified)
    );

    fs::remove_dir_all(&output_root).expect("isolated fixture root should be removed");
}
