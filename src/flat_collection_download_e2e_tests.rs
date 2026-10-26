//! End-to-end tests for flattened collection staging and publication.

use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
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
            bilibili_collection_destination: None,
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
        None,
        false,
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
            bilibili_collection_destination: None,
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

    let remapped = flatten_bilibili_collection_staging_output(
        &root,
        &output_dir,
        &plan,
        &mut report,
        None,
        None,
        false,
    )
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
            bilibili_collection_destination: None,
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

#[cfg(unix)]
#[tokio::test]
async fn sdk_unmuxed_segments_keep_report_order_and_bound_sidecars_through_publication_and_missing_scan()
 {
    let output_root = temp_root("flat-collection-segment-order");
    let _cleanup = TestDirectory(output_root.clone());
    let collection_folder = "Segment Collection";
    let collection_dir = output_root.join(collection_folder);
    fs::create_dir_all(&collection_dir).expect("collection output folder should be created");
    let stable_media_id = bilibili_plan_stable_media_id(Some("BV1"), 999, 456, None);
    let flat_stem = bilibili_collection_flat_stem(2, 3, &stable_media_id, "Segmented episode")
        .expect("fixture identity should fit the collection filename bound");
    let preexisting_first_segment = collection_dir.join(format!("{flat_stem}.part-001.flv"));
    let preexisting_first_nfo = collection_dir.join(format!("{flat_stem}.part-001.nfo"));
    write_fixture(&preexisting_first_segment, b"old conflicting segment");
    write_fixture(
        &preexisting_first_nfo,
        b"<movie><uniqueid type=\"bilibili-cid\">cid999</uniqueid></movie>",
    );
    let root = RootedFs::new(&output_root).expect("library root should bind");
    let collection_identity = root
        .entry_identity(&collection_dir)
        .expect("collection identity should be readable")
        .expect("collection directory should exist");
    let staging = create_video_staging_dir(&root).expect("private staging should be created");
    let output_dir = staging.path().to_path_buf();
    let staged_collection_dir = output_dir.join(collection_folder);
    fs::create_dir(&staged_collection_dir).expect("staged collection folder should be created");
    let sdk_entry_dir = staged_collection_dir.join("P003-BV1-Segmented episode");
    fs::create_dir(&sdk_entry_dir).expect("SDK entry directory should be created");

    // These source names and creation order differ from the report order chosen below.
    let segment_specs = [
        ("Part 10.flv", b"playback segment three".as_slice()),
        ("Part 3.flv", b"playback segment two".as_slice()),
        ("Part 2.flv", b"playback segment one".as_slice()),
    ];
    let created_segment_paths = segment_specs
        .iter()
        .map(|(name, bytes)| {
            let path = sdk_entry_dir.join(name);
            write_fixture(&path, bytes);
            path
        })
        .collect::<Vec<_>>();
    let sidecar_specs = [
        ("danmaku.xml", "danmaku", b"ordered danmaku".as_slice()),
        (
            "subtitle-en-US-01-trackhash.srt",
            "subtitle",
            b"ordered subtitle".as_slice(),
        ),
        (
            "cover-image-deadbeef.jpg",
            "cover",
            b"entry artwork".as_slice(),
        ),
    ];
    let sidecar_paths = sidecar_specs
        .iter()
        .map(|(name, _, bytes)| {
            let path = sdk_entry_dir.join(name);
            write_fixture(&path, bytes);
            path
        })
        .collect::<Vec<_>>();

    let sdk_entry_identity = root
        .entry_identity(&sdk_entry_dir)
        .expect("SDK entry identity should be readable")
        .expect("SDK entry directory should exist");
    let sdk_entry = root
        .bind_entry(&sdk_entry_dir, false)
        .expect("SDK entry should bind");
    let directory_segment_order = root
        .list_bound_directory(&sdk_entry, sdk_entry_identity)
        .expect("SDK entry children should be listed")
        .into_iter()
        .map(|(name, _)| sdk_entry_dir.join(name))
        .filter(|path| created_segment_paths.contains(path))
        .collect::<Vec<_>>();
    let lexical_segment_order = {
        let mut paths = created_segment_paths.clone();
        paths.sort();
        paths
    };
    let (part_10, part_3, part_2) = (
        created_segment_paths[0].clone(),
        created_segment_paths[1].clone(),
        created_segment_paths[2].clone(),
    );
    let report_order_candidates = vec![
        vec![part_10.clone(), part_3.clone(), part_2.clone()],
        vec![part_10.clone(), part_2.clone(), part_3.clone()],
        vec![part_3.clone(), part_10.clone(), part_2.clone()],
        vec![part_3.clone(), part_2.clone(), part_10.clone()],
        vec![part_2.clone(), part_10.clone(), part_3.clone()],
        vec![part_2.clone(), part_3.clone(), part_10.clone()],
    ];
    let report_primary_order = report_order_candidates
        .into_iter()
        .find(|order| {
            order != &directory_segment_order
                && order != &created_segment_paths
                && order != &lexical_segment_order
        })
        .expect(
            "three media segments allow an order distinct from enumeration, creation, and names",
        );
    assert_ne!(report_primary_order, directory_segment_order);
    assert_ne!(report_primary_order, created_segment_paths);
    assert_ne!(report_primary_order, lexical_segment_order);

    let download_entry = BilibiliDownloadEntry {
        index: 2,
        aid: 999,
        bvid: Some("BV1".to_string()),
        cid: 456,
        epid: None,
        title: "Segmented episode".to_string(),
        ..BilibiliDownloadEntry::default()
    };
    let plan = BilibiliDownloadPlan {
        title: "Segment Collection".to_string(),
        entries: vec![download_entry],
    };
    let mut report_files = report_primary_order
        .iter()
        .map(|path| BilibiliDownloadedFile {
            kind: "flv_segment".to_string(),
            path: path.clone(),
        })
        .collect::<Vec<_>>();
    report_files.extend(
        sidecar_specs
            .iter()
            .zip(&sidecar_paths)
            .map(|((_, kind, _), path)| BilibiliDownloadedFile {
                kind: (*kind).to_string(),
                path: path.clone(),
            }),
    );
    let mut report = BilibiliDownloadReport {
        title: "Segment Collection".to_string(),
        output_dir: output_dir.clone(),
        entries: vec![BilibiliEntryDownloadReport {
            index: 2,
            title: "Segmented episode".to_string(),
            files: report_files,
            mux: None,
        }],
    };

    let nfos = write_bilibili_nfos(
        &output_dir,
        "https://www.bilibili.com/video/BV1",
        &plan,
        &report,
    )
    .expect("the real Bilibili NFO writer should handle the first reported segment");
    assert_eq!(nfos, vec![report_primary_order[0].with_extension("nfo")]);
    let nfo_bytes = fs::read(&nfos[0]).expect("generated NFO should be readable");
    let nfo_text = std::str::from_utf8(&nfo_bytes).expect("generated NFO should be UTF-8");
    assert!(nfo_text.contains("<uniqueid type=\"bilibili\" default=\"true\">BV1</uniqueid>"));
    assert!(nfo_text.contains("<uniqueid type=\"bilibili-cid\">cid456</uniqueid>"));
    let directory_segment_order_after_nfo = root
        .list_bound_directory(&sdk_entry, sdk_entry_identity)
        .expect("SDK entry children should remain listable after writing its NFO")
        .into_iter()
        .map(|(name, _)| sdk_entry_dir.join(name))
        .filter(|path| created_segment_paths.contains(path))
        .collect::<Vec<_>>();
    assert_ne!(
        report_primary_order, directory_segment_order_after_nfo,
        "the fixture must keep report playback order distinct from filesystem enumeration"
    );

    let expected_entry = BilibiliWorkerExpectedEntry {
        index: 2,
        part_index: 3,
        title: "Segmented episode".to_string(),
        stable_media_id: stable_media_id.clone(),
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
        None,
        false,
    )
    .expect("the production mapper should flatten all reported segments and sidecars");
    assert_eq!(remapped.len(), report.entries[0].files.len() + 1);

    let mut ordered_flat_segments = Vec::new();
    for (ordinal, source) in report_primary_order.iter().enumerate() {
        let destination = remapped
            .get(source)
            .expect("every report-listed segment should be remapped");
        let expected_file_name = format!("{flat_stem}.part-{:03}.flv", ordinal + 1);
        assert_eq!(
            destination.file_name().and_then(|name| name.to_str()),
            Some(expected_file_name.as_str()),
            "part suffixes must follow report playback order"
        );
        assert_eq!(destination.parent(), Some(staged_collection_dir.as_path()));
        let expected_bytes = segment_specs
            .iter()
            .find(|(name, _)| sdk_entry_dir.join(name) == *source)
            .map(|(_, bytes)| bytes.to_vec())
            .expect("reported segment should have a fixture byte payload");
        assert_eq!(fs::read(destination).unwrap(), expected_bytes);
        ordered_flat_segments.push(destination.clone());
    }
    let report_segment_paths = report.entries[0]
        .files
        .iter()
        .filter(|file| file.kind == "flv_segment")
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    assert_eq!(report_segment_paths, ordered_flat_segments);

    let flat_first_segment_stem = format!("{flat_stem}.part-001");
    let flat_nfo = staged_collection_dir.join(format!("{flat_first_segment_stem}.nfo"));
    assert_eq!(remapped.get(&nfos[0]), Some(&flat_nfo));
    assert_eq!(fs::read(&flat_nfo).unwrap(), nfo_bytes);
    let expected_sidecars = sidecar_specs
        .iter()
        .zip(&sidecar_paths)
        .map(|((name, _, bytes), source)| {
            let extension = Path::new(name)
                .extension()
                .and_then(|ext| ext.to_str())
                .unwrap();
            let target = match *name {
                "danmaku.xml" => staged_collection_dir
                    .join(format!("{flat_first_segment_stem}.danmaku.{extension}")),
                "subtitle-en-US-01-trackhash.srt" => staged_collection_dir.join(format!(
                    "{flat_first_segment_stem}.en-US-01-trackhash.{extension}"
                )),
                "cover-image-deadbeef.jpg" => staged_collection_dir.join(format!(
                    "{flat_first_segment_stem}.cover-deadbeef.{extension}"
                )),
                _ => unreachable!("fixture sidecar was listed above"),
            };
            assert_eq!(remapped.get(source), Some(&target));
            assert_eq!(target.parent(), Some(staged_collection_dir.as_path()));
            assert_eq!(fs::read(&target).unwrap(), bytes.to_vec());
            (target, bytes.to_vec())
        })
        .collect::<Vec<_>>();

    let staged_files = remapped.values().cloned().collect::<Vec<_>>();
    let duplicate = VideoDuplicate {
        identity: VideoIdentity {
            provider: VideoProvider::Bilibili,
            id: stable_media_id,
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
            bilibili_collection_destination: None,
        },
        &output_dir,
        &output_root,
        &staged_files,
        VideoDuplicateAction::KeepBoth,
        &duplicate,
        StagedPrimaryMediaKind::Video,
    )
    .expect("the real staged publication path should publish all ordered segments and sidecars");
    let expected_published_segments = vec![
        collection_dir.join(format!("{flat_stem}.part-001 (2).flv")),
        collection_dir.join(format!("{flat_stem}.part-002.flv")),
        collection_dir.join(format!("{flat_stem}.part-003.flv")),
    ];
    assert_eq!(
        publication
            .primary_destinations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_published_segments
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
    );
    for (segment_source, destination) in report_primary_order
        .iter()
        .zip(&expected_published_segments)
    {
        assert_eq!(destination.parent(), Some(collection_dir.as_path()));
        let expected_bytes = segment_specs
            .iter()
            .find(|(name, _)| sdk_entry_dir.join(name) == *segment_source)
            .map(|(_, bytes)| bytes.to_vec())
            .expect("published segment should have a fixture byte payload");
        assert_eq!(fs::read(destination).unwrap(), expected_bytes);
    }
    let published_first_segment_stem = format!("{flat_stem}.part-001 (2)");
    let expected_published_nfo = collection_dir.join(format!("{published_first_segment_stem}.nfo"));
    assert_eq!(fs::read(expected_published_nfo).unwrap(), nfo_bytes);
    for (sidecar, expected_bytes) in expected_sidecars {
        let sidecar_name = sidecar.file_name().and_then(|name| name.to_str()).unwrap();
        let sidecar_variant = sidecar_name
            .strip_prefix(&flat_first_segment_stem)
            .expect("mapper sidecar should use the complete first-segment stem");
        let destination =
            collection_dir.join(format!("{published_first_segment_stem}{sidecar_variant}"));
        assert_eq!(destination.parent(), Some(collection_dir.as_path()));
        assert_eq!(fs::read(destination).unwrap(), expected_bytes);
    }

    assert_eq!(
        fs::read(&preexisting_first_segment).unwrap(),
        b"old conflicting segment",
        "KeepBoth must preserve the pre-existing conflicting primary"
    );
    assert_eq!(
        fs::read(&preexisting_first_nfo).unwrap(),
        b"<movie><uniqueid type=\"bilibili-cid\">cid999</uniqueid></movie>",
        "KeepBoth must preserve the pre-existing unrelated identity NFO"
    );
    assert!(
        publication.bilibili_group_receipt.as_ref().is_some(),
        "multi-segment publication should persist its recovery receipt"
    );
    let receipt = match load_bilibili_group_publication_receipt_blocking(
        &output_root,
        &expected_entry.stable_media_id,
        TEST_MEDIA_PROFILE,
        Some(TEST_PUBLICATION_NAMESPACE),
        Some(collection_folder),
    )
    .expect("published group receipt should reload from durable storage")
    {
        BilibiliGroupReceiptLookup::Found(receipt) => receipt,
        BilibiliGroupReceiptLookup::Missing => {
            panic!("expected a durable multi-segment publication receipt, but none was saved")
        }
        BilibiliGroupReceiptLookup::ProfileMismatch => {
            panic!("expected a durable multi-segment receipt with the test profile")
        }
    };
    verify_bilibili_group_publication_plan_receipt_blocking(
        &output_root,
        &receipt,
        &expected_published_segments,
        Some(TEST_PUBLICATION_NAMESPACE),
        Some(collection_folder),
    )
    .expect("the receipt should verify every published primary segment");

    let tool_dir = output_root.join("test-tools");
    fs::create_dir(&tool_dir).expect("task-local tool directory should be created");
    let ffprobe = tool_dir.join("ffprobe");
    write_fixture(&ffprobe, b"#!/bin/sh\nprintf 'video\\naudio\\n'\n");
    let mut ffprobe_permissions = fs::metadata(&ffprobe)
        .expect("task-local ffprobe fixture should be readable")
        .permissions();
    ffprobe_permissions.set_mode(0o755);
    fs::set_permissions(&ffprobe, ffprobe_permissions)
        .expect("task-local ffprobe fixture should be executable");
    let mut config =
        AppConfig::load(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.toml"))
            .expect("example config should parse");
    config.downloads.video_dir = output_root.clone();
    config.set_video_storage_root(output_root.clone());
    config.tools.ffmpeg = tool_dir.join("ffmpeg");
    config.bilibili.auth.state_path = output_root.join("test-auth-state.json");

    let group_entry = flat_test_group_entry(2, "BV1", 456, "Segmented episode");
    let inventory = BilibiliEpisodeGroupInventory {
        title: "Segment Collection".to_string(),
        source: BilibiliGroupSource::Collection,
        pgc_season_id: None,
        entries: vec![group_entry.clone()],
        output_template: Some(collection_folder.to_string()),
        output_folder: Some(collection_folder.to_string()),
        storage_relative_output_folder: Some(collection_folder.to_string()),
        output_folder_identity: Some(collection_identity),
        collection_output_layout_version: Some(1),
    };
    // Missing follows the identity NFO attached to the first segment. The durable receipt above
    // independently verifies that all three segment files published under the same entry.
    let identity_index =
        build_bilibili_group_identity_index(&config, &inventory, DownloadMode::All)
            .await
            .expect("collection identity inventory should scan the published output");
    let canonical_published_first_segment = fs::canonicalize(&expected_published_segments[0])
        .expect("published first segment should have a canonical path");
    let canonical_preexisting_first_segment = fs::canonicalize(&preexisting_first_segment)
        .expect("pre-existing first segment should have a canonical path");
    {
        let index = identity_index.lock().await;
        let matching_paths = index.metadata_videos(&bilibili_collection_cid_identity(456));
        let canonical_matching_paths = matching_paths
            .iter()
            .map(|path| {
                fs::canonicalize(path).unwrap_or_else(|error| {
                    panic!(
                        "indexed media path {} should resolve: {error}",
                        path.display()
                    )
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            canonical_matching_paths,
            vec![canonical_published_first_segment.clone()],
            "only the new KeepBoth first segment should carry the requested CID identity"
        );
        assert!(!canonical_matching_paths.contains(&canonical_preexisting_first_segment));
        assert!(bilibili_plan_entry_has_exact_existing_metadata(
            &index,
            &group_entry.download
        ));
    }

    let scanner_index = Arc::clone(&identity_index);
    let scanner_config = config.clone();
    let scanner_ffprobe = ffprobe_program(&config.tools.ffmpeg);
    let scanner: BilibiliGroupExistingMediaScanner = Arc::new(move |entry| {
        let index = Arc::clone(&scanner_index);
        let config = scanner_config.clone();
        let ffprobe = scanner_ffprobe.clone();
        Box::pin(async move {
            bilibili_plan_entry_compatible_existing_media_from_shared_index(
                &config,
                index,
                &entry.download,
                DownloadMode::All,
                &ffprobe,
            )
            .await
        })
    });
    let executor: BilibiliGroupEntryExecutor = Arc::new(|_| {
        Box::pin(async {
            bail!("Missing should reuse the CID-matched first segment without executing")
        })
    });
    let hooks = BilibiliGroupExecutionHooks {
        execute_entry: executor,
        find_existing_media: scanner,
        record_published_evidence: Arc::new(|_| Box::pin(async { Ok(()) })),
        wait_before_retry: Arc::new(|_, _, _, _, _| Box::pin(async {})),
    };
    let missing_report = run_bilibili_episode_group_with_inventory(
        &config,
        &JobRequest::Bilibili {
            url: "https://www.bilibili.com/video/BV1".to_string(),
            selection: Some(BilibiliSelection::Missing),
        },
        inventory,
        None,
        None,
        None,
        hooks,
    )
    .await
    .expect("Missing should recognize the newly published CID NFO through its first segment");
    let canonical_missing_paths = missing_report
        .primary_media_paths
        .iter()
        .map(|path| {
            fs::canonicalize(path).unwrap_or_else(|error| {
                panic!(
                    "Missing media path {} should resolve: {error}",
                    path.display()
                )
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        canonical_missing_paths,
        vec![canonical_published_first_segment],
        "Missing inventory associates the entry identity with its designated first segment"
    );

    staging
        .finish()
        .expect("published staging tree should be removed");
    fs::remove_dir_all(&output_root).expect("isolated fixture root should be removed");
}

#[test]
fn generic_artifact_only_groups_flatten_and_publish_without_primary_media() {
    let output_root = temp_root("flat-collection-artifact-only");
    let _cleanup = TestDirectory(output_root.clone());
    fs::create_dir_all(&output_root).expect("isolated output root should be created");
    let root = RootedFs::new(&output_root).expect("output root should bind");
    let source_cases = [
        (
            BilibiliGroupSource::Collection,
            "UGC collection",
            "UP-4242 Saved collection",
            None,
        ),
        (
            BilibiliGroupSource::Season,
            "PGC season",
            "Test season [season-71]",
            Some(71),
        ),
    ];
    #[derive(Clone, Copy)]
    struct ArtifactFileFixture {
        name: &'static str,
        kind: &'static str,
        bytes: &'static [u8],
    }
    #[derive(Clone, Copy)]
    struct ArtifactModeFixture {
        mode: &'static str,
        files: &'static [ArtifactFileFixture],
    }
    let mode_cases = [
        ArtifactModeFixture {
            mode: "subtitle",
            files: &[
                ArtifactFileFixture {
                    name: "subtitle-en-US-01-trackhash.srt",
                    kind: "subtitle",
                    bytes: b"English subtitle bytes",
                },
                ArtifactFileFixture {
                    name: "subtitle-zh-CN-02-otherhash.ass",
                    kind: "subtitle",
                    bytes: b"Chinese subtitle bytes",
                },
            ],
        },
        ArtifactModeFixture {
            mode: "danmaku",
            files: &[
                ArtifactFileFixture {
                    name: "danmaku.xml",
                    kind: "danmaku",
                    bytes: b"XML danmaku bytes",
                },
                ArtifactFileFixture {
                    name: "danmaku.ass",
                    kind: "danmaku_ass",
                    bytes: b"ASS danmaku bytes",
                },
            ],
        },
        ArtifactModeFixture {
            mode: "cover",
            files: &[ArtifactFileFixture {
                name: "cover-image-deadbeef.jpg",
                kind: "cover",
                bytes: b"cover image bytes",
            }],
        },
    ];
    let mut case_index = 0_u32;
    let mut flat_stems = BTreeSet::new();

    for (source, source_label, folder, season_id) in source_cases {
        let collection_dir = output_root.join(folder);
        fs::create_dir_all(&collection_dir)
            .expect("selected collection or season folder should be created");
        let collection_identity = root
            .entry_identity(&collection_dir)
            .expect("selected output folder identity should be readable")
            .expect("selected output folder should exist");

        for mode_case in &mode_cases {
            let mode = mode_case.mode;
            case_index += 1;
            let index = case_index;
            let title = format!("{source_label} {mode} output");
            let (bvid, aid, cid, epid) = if source == BilibiliGroupSource::Collection {
                (
                    Some(format!("BV{index}")),
                    10_000 + u64::from(index),
                    20_000 + u64::from(index),
                    None,
                )
            } else {
                (
                    None,
                    30_000 + u64::from(index),
                    40_000 + u64::from(index),
                    Some(50_000 + u64::from(index)),
                )
            };
            let stable_media_id = bilibili_plan_stable_media_id(bvid.as_deref(), aid, cid, epid);
            let download_entry = BilibiliDownloadEntry {
                index,
                aid,
                bvid: bvid.clone(),
                cid,
                epid,
                title: title.clone(),
                ..BilibiliDownloadEntry::default()
            };
            let flat_stem = bilibili_collection_flat_stem(index, index, &stable_media_id, &title)
                .expect("synthetic artifact identity should fit the flat filename budget");
            assert!(
                flat_stems.insert(flat_stem.clone()),
                "entry stems should be unique"
            );

            let mut config = AppConfig::load(
                &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.toml"),
            )
            .expect("example config should parse");
            config.downloads.video_dir = output_root.clone();
            config.set_video_storage_root(output_root.clone());
            config.bilibili.download_args = vec!["--only".to_string(), mode.to_string()];
            let options = bilibili_core::download_options(&config)
                .expect("artifact-only download options should parse");
            let media_profile = bilibili_group_media_profile(&options, &config);
            let inventory = BilibiliEpisodeGroupInventory {
                title: title.clone(),
                source,
                pgc_season_id: season_id,
                entries: vec![BilibiliGroupEntry {
                    download: download_entry.clone(),
                    stable_media_id: stable_media_id.clone(),
                }],
                output_template: Some(folder.to_string()),
                output_folder: Some(folder.to_string()),
                storage_relative_output_folder: Some(folder.to_string()),
                output_folder_identity: Some(collection_identity),
                collection_output_layout_version: Some(1),
            };
            let reviewed_plan = inventory.validation_snapshot(media_profile.clone());
            let reviewed_folder = match &reviewed_plan.bilibili_collection_directory {
                BilibiliCollectionDirectorySnapshot::Existing {
                    path,
                    is_directory: true,
                    ..
                } => path.as_str(),
                _ => panic!("generic v1 plan should retain its selected folder witness"),
            };
            assert_eq!(
                reviewed_plan.bilibili_collection_output_layout_version,
                Some(1)
            );
            assert_eq!(reviewed_folder, folder);
            let job = JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV1".to_string(),
                selection: Some(BilibiliSelection::All),
            };
            let allow_artifact_only_without_primary =
                bilibili_downloads_artifacts_only(&config, &job)
                    .expect("configured artifact mode should be recognized")
                    && reviewed_plan.bilibili_collection_output_layout_version == Some(1)
                    && reviewed_plan.media_profile.as_deref() == Some(media_profile.as_str());
            assert!(allow_artifact_only_without_primary);
            assert!(
                !should_use_independent_bilibili_episode_group(&config, &job)
                    .expect("generic artifact-only route should be selected")
            );

            let staging =
                create_video_staging_dir(&root).expect("private staging folder should be created");
            let output_dir = staging.path().to_path_buf();
            let staged_collection_dir = output_dir.join(folder);
            fs::create_dir_all(&staged_collection_dir)
                .expect("staged collection folder should be created");
            let sdk_entry_dir = staged_collection_dir.join(format!("P{index:03}-{title}"));
            fs::create_dir(&sdk_entry_dir).expect("mock SDK entry folder should be created");

            let plan = BilibiliDownloadPlan {
                title: title.clone(),
                entries: vec![download_entry],
            };
            let mut artifacts = Vec::new();
            let mut source_bytes = BTreeMap::new();
            for artifact in mode_case.files {
                let path = sdk_entry_dir.join(artifact.name);
                write_fixture(&path, artifact.bytes);
                source_bytes.insert(path.clone(), artifact.bytes.to_vec());
                artifacts.push(BilibiliDownloadedFile {
                    kind: artifact.kind.to_string(),
                    path,
                });
            }
            let mut report = BilibiliDownloadReport {
                title: title.clone(),
                output_dir: output_dir.clone(),
                entries: vec![BilibiliEntryDownloadReport {
                    index,
                    title: title.clone(),
                    files: artifacts,
                    mux: None,
                }],
            };

            // Generic Selection::All has no per-entry expected_entry. The reviewed layout and
            // destination folder permit flat sidecars without inventing primary media or a receipt.
            let remapped = flatten_bilibili_collection_staging_output(
                &root,
                &output_dir,
                &plan,
                &mut report,
                None,
                Some(reviewed_folder),
                allow_artifact_only_without_primary,
            )
            .expect(
                "reviewed artifact-only collection output should flatten without primary media",
            );
            assert_eq!(remapped.len(), source_bytes.len());
            assert!(remapped.values().all(|path| {
                path.parent() == Some(staged_collection_dir.as_path())
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| {
                            name.starts_with(&flat_stem) && !name.contains(".part-")
                        })
            }));
            for (source, destination) in &remapped {
                assert_eq!(fs::read(destination).unwrap(), source_bytes[source]);
            }
            let report_primary_paths = bilibili_report_primary_media(&output_dir, &report);
            assert!(report_primary_paths.is_empty());
            assert!(
                report.entries[0]
                    .files
                    .iter()
                    .all(|file| file.path.parent() == Some(staged_collection_dir.as_path()))
            );

            let mut prior_targets = BTreeMap::new();
            let mut expected_published = BTreeMap::new();
            for (source, staged_target) in &remapped {
                let relative = staged_target
                    .strip_prefix(&output_dir)
                    .expect("flattened artifact should remain under staging");
                let prior_target = output_root.join(relative);
                let previous_bytes = format!("pre-existing {mode} artifact").into_bytes();
                write_fixture(&prior_target, &previous_bytes);
                prior_targets.insert(prior_target.clone(), previous_bytes);

                let file_name = prior_target
                    .file_name()
                    .and_then(|name| name.to_str())
                    .expect("artifact filename should be UTF-8");
                let (stem, extension) = file_name
                    .rsplit_once('.')
                    .expect("fixture artifact should include an extension");
                let keep_both_target =
                    prior_target.with_file_name(format!("{stem} (2).{extension}"));
                expected_published.insert(keep_both_target, source_bytes[source].clone());
            }

            let duplicate = VideoDuplicate {
                identity: VideoIdentity {
                    provider: VideoProvider::Bilibili,
                    id: stable_media_id,
                },
                existing_videos: Vec::new(),
                skip_summary: None,
                overwrite_confirmation: None,
            };
            let destination_context = BilibiliCollectionDestinationContext {
                output_folder: reviewed_folder,
                collection_directory: &reviewed_plan.bilibili_collection_directory,
            };
            let moved = move_staged_artifact_files_with_root(
                MoveExecutionContext {
                    root: &root,
                    staging: Some(&staging),
                    bilibili_group_receipt: None,
                    bilibili_collection_destination: Some(destination_context),
                },
                &output_dir,
                &output_root,
                &remapped.values().cloned().collect::<Vec<_>>(),
                VideoDuplicateAction::KeepBoth,
                &duplicate,
                StagedPrimaryMediaKind::Video,
            )
            .expect("real artifact publication should preserve flat collection paths");
            assert_eq!(
                moved.iter().cloned().collect::<BTreeSet<_>>(),
                expected_published.keys().cloned().collect::<BTreeSet<_>>()
            );
            assert!(
                moved
                    .iter()
                    .all(|path| !is_primary_media_file(path, StagedPrimaryMediaKind::Video))
            );
            let primary_media_paths = reconstruct_published_primary_media_paths(
                &root,
                &output_dir,
                &BTreeMap::new(),
                &report_primary_paths,
                StagedPrimaryMediaKind::Video,
            )
            .expect("artifact-only publication should reconstruct no primary media paths");
            assert!(primary_media_paths.is_empty());
            for (path, bytes) in expected_published {
                assert_eq!(path.parent(), Some(collection_dir.as_path()));
                let name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .expect("published artifact filename should be UTF-8");
                assert!(name.starts_with(&flat_stem));
                assert!(!name.contains(".part-"));
                assert!(name.contains(" (2)."));
                assert_eq!(fs::read(path).unwrap(), bytes);
            }
            for (path, bytes) in prior_targets {
                assert_eq!(
                    fs::read(path).unwrap(),
                    bytes,
                    "KeepBoth must preserve prior bytes"
                );
            }
            staging
                .finish()
                .expect("published artifact staging tree should be removed");
        }
    }
    assert_eq!(flat_stems.len(), 6);

    let negative_collection_folder = "UP-4242 Saved collection";
    let negative_collection_dir = output_root.join(negative_collection_folder);
    let negative_collection_identity = root
        .entry_identity(&negative_collection_dir)
        .expect("negative fixture output folder identity should be readable")
        .expect("negative fixture output folder should exist");
    let mut negative_reviewed_plan = PlanValidationSnapshot {
        bilibili_collection_output_layout_version: Some(1),
        bilibili_collection_directory: BilibiliCollectionDirectorySnapshot::Existing {
            path: negative_collection_folder.to_string(),
            device: negative_collection_identity.device(),
            inode: negative_collection_identity.inode(),
            is_directory: negative_collection_identity.is_dir(),
        },
        ..PlanValidationSnapshot::default()
    };
    let mut video_only_config = {
        let mut config =
            AppConfig::load(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.toml"))
                .expect("example config should parse");
        config.bilibili.download_args = vec!["--only".to_string(), "video".to_string()];
        config
    };
    let video_options = bilibili_core::download_options(&video_only_config)
        .expect("video-only download options should parse");
    let video_media_profile = bilibili_group_media_profile(&video_options, &video_only_config);
    negative_reviewed_plan.media_profile = Some(video_media_profile.clone());
    video_only_config.downloads.video_dir = output_root.clone();
    video_only_config.set_video_storage_root(output_root.clone());
    let video_job = JobRequest::Bilibili {
        url: "https://www.bilibili.com/video/BV1".to_string(),
        selection: Some(BilibiliSelection::All),
    };
    let allow_artifact_only_without_primary =
        bilibili_downloads_artifacts_only(&video_only_config, &video_job)
            .expect("video-only mode should require primary media")
            && negative_reviewed_plan.bilibili_collection_output_layout_version == Some(1)
            && negative_reviewed_plan.media_profile.as_deref()
                == Some(video_media_profile.as_str());
    assert!(!allow_artifact_only_without_primary);
    assert!(
        should_use_independent_bilibili_episode_group(&video_only_config, &video_job)
            .expect("video-only group should retain primary-media execution")
    );

    let negative_staging =
        create_video_staging_dir(&root).expect("negative fixture staging folder should be created");
    let negative_output_dir = negative_staging.path().to_path_buf();
    let negative_staged_collection = negative_output_dir.join(negative_collection_folder);
    fs::create_dir_all(&negative_staged_collection)
        .expect("negative staged collection folder should be created");
    let negative_entry_dir = negative_staged_collection.join("P099-Video without primary");
    fs::create_dir(&negative_entry_dir).expect("negative mock SDK entry should be created");
    let negative_artifact = negative_entry_dir.join("subtitle-en-US-01-trackhash.srt");
    write_fixture(&negative_artifact, b"not a primary video");
    let negative_entry = BilibiliDownloadEntry {
        index: 99,
        aid: 99,
        bvid: Some("BV99".to_string()),
        cid: 99,
        epid: None,
        title: "Video without primary".to_string(),
        ..BilibiliDownloadEntry::default()
    };
    let negative_plan = BilibiliDownloadPlan {
        title: "Video without primary".to_string(),
        entries: vec![negative_entry],
    };
    let mut negative_report = BilibiliDownloadReport {
        title: "Video without primary".to_string(),
        output_dir: negative_output_dir.clone(),
        entries: vec![BilibiliEntryDownloadReport {
            index: 99,
            title: "Video without primary".to_string(),
            files: vec![BilibiliDownloadedFile {
                kind: "subtitle".to_string(),
                path: negative_artifact.clone(),
            }],
            mux: None,
        }],
    };
    let stable_negative_id = bilibili_plan_stable_media_id(Some("BV99"), 99, 99, None);
    let negative_flat_stem =
        bilibili_collection_flat_stem(99, 99, &stable_negative_id, "Video without primary")
            .expect("negative fixture identity should fit the flat filename budget");
    let negative_reviewed_folder = match &negative_reviewed_plan.bilibili_collection_directory {
        BilibiliCollectionDirectorySnapshot::Existing { path, .. } => path.as_str(),
        _ => panic!("negative video fixture should retain its selected folder witness"),
    };
    let error = flatten_bilibili_collection_staging_output(
        &root,
        &negative_output_dir,
        &negative_plan,
        &mut negative_report,
        None,
        Some(negative_reviewed_folder),
        allow_artifact_only_without_primary,
    )
    .expect_err("a video plan without primary media must fail even with a sidecar");
    assert!(error.to_string().contains("primary"));
    assert!(
        negative_artifact.exists(),
        "rejected output should remain in staging"
    );
    assert!(
        fs::read_dir(&negative_collection_dir)
            .expect("negative output folder should remain readable")
            .all(|entry| {
                entry
                    .expect("negative output entry should be readable")
                    .file_name()
                    .to_str()
                    .is_none_or(|name| !name.starts_with(&negative_flat_stem))
            })
    );
    negative_staging
        .finish()
        .expect("rejected video fixture staging tree should be removed");
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
                None,
                false,
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
                    bilibili_collection_destination: None,
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
