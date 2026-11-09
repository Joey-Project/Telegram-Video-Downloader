use std::collections::BTreeSet;
use std::fs;

use super::media_library_e2e_tests::create_migration_fixture;
use super::*;

#[tokio::test]
async fn file_preview_confirms_all_101_selected_items_with_one_telegram_flow() {
    const ITEM_COUNT: usize = 101;

    let (telegram, mut telegram_requests, shutdown, server) = spawn_fake_telegram_api().await;
    let (fixture, queue, inbox) = create_migration_fixture(
        "bulk-selection-101-confirmation",
        crate::cloud::TEST_DATABASE_EPOCH,
    );
    let cleanup_root = fixture.root.clone();
    let source_root = fixture.video_root.clone();
    let source_directory = source_root.join("bulk-source");
    fs::create_dir_all(&source_directory).expect("bulk source directory should create");

    let mut fixtures = Vec::with_capacity(ITEM_COUNT);
    for index in 0..ITEM_COUNT {
        let relative_path = format!("bulk-source/video-{index:03}.mp4");
        let contents = format!("bulk selection fixture {index:03}\n").into_bytes();
        fs::write(source_root.join(&relative_path), &contents)
            .expect("temporary bulk media fixture should write");
        fixtures.push((relative_path, contents));
    }

    let runtime = make_runtime(
        telegram,
        fixture.config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let library_id = fixture.source_library_id.clone();
    let root_binding = cloud_test_root_binding(&queue, &library_id);

    let scan = run_library_management_test_request(
        &runtime,
        46_001,
        "library_scan",
        serde_json::json!({"library_id": library_id}),
    )
    .await;
    assert_eq!(scan.status, InboxStatus::Completed, "{:?}", scan.error);
    let scan_items = scan
        .result
        .as_ref()
        .expect("library scan should return a snapshot")["items"]
        .as_array()
        .expect("library snapshot should contain its media items");
    assert_eq!(
        scan_items.len(),
        ITEM_COUNT,
        "the fixture should expose all 101 local media items"
    );

    let expected_paths = fixtures
        .iter()
        .map(|(relative_path, _)| relative_path.clone())
        .collect::<BTreeSet<_>>();
    let mut scanned_paths = BTreeSet::new();
    let selected_item_ids = scan_items
        .iter()
        .map(|item| {
            let relative_path = item["relative_path"]
                .as_str()
                .expect("scanned media should have a relative path")
                .to_string();
            scanned_paths.insert(relative_path);
            item["id"]
                .as_str()
                .expect("scanned media should have an item ID")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(scanned_paths, expected_paths);
    assert_eq!(selected_item_ids.len(), ITEM_COUNT);
    assert_eq!(
        selected_item_ids.iter().collect::<BTreeSet<_>>().len(),
        ITEM_COUNT,
        "each selected file should have a unique item ID"
    );

    let preview_record = run_library_management_test_request(
        &runtime,
        46_002,
        "file_preview",
        serde_json::json!({
            "library_id": library_id,
            "root_binding": root_binding,
            "item_ids": selected_item_ids.clone(),
            "target_relative_dir": "bulk-selected",
            "conflict": "skip",
        }),
    )
    .await;
    assert_eq!(
        preview_record.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview_record.error
    );
    let preview_result = preview_record
        .result
        .expect("the 101-item preview should be durably persisted");
    let preview = &preview_result["preview"];
    let preview_items = preview["items"]
        .as_array()
        .expect("file preview should retain every selected item");
    assert_eq!(
        preview_items.len(),
        ITEM_COUNT,
        "preview should not silently drop selections above 100 items"
    );
    let preview_item_ids = preview_items
        .iter()
        .map(|item| {
            assert_eq!(item["outcome"], "move");
            item["item_id"]
                .as_str()
                .expect("preview item should retain its selected ID")
                .to_string()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        preview_item_ids,
        selected_item_ids.iter().cloned().collect::<BTreeSet<_>>(),
        "the preview should contain exactly the selected item IDs"
    );
    let preview_source_paths = preview_items
        .iter()
        .map(|item| {
            item["source_path"]
                .as_str()
                .expect("preview item should retain its source path")
                .to_string()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(preview_source_paths, expected_paths);

    let preview_id = preview_result["preview_id"]
        .as_str()
        .expect("preview should have a stable ID")
        .to_string();
    let revision = preview_result["revision"]
        .as_str()
        .expect("preview should have a revision")
        .to_string();
    let confirmation = run_library_management_test_request(
        &runtime,
        46_003,
        "file_confirm",
        serde_json::json!({"preview_id": preview_id, "revision": revision}),
    )
    .await;
    assert_eq!(
        confirmation.status,
        InboxStatus::Completed,
        "{:?}",
        confirmation.error
    );
    let operation = &confirmation
        .result
        .as_ref()
        .expect("confirmation should return per-item results")["operation"];
    let moved_items = operation["items"]
        .as_array()
        .expect("confirmation should report every selected item");
    assert_eq!(moved_items.len(), ITEM_COUNT);
    let moved_item_ids = moved_items
        .iter()
        .map(|item| {
            assert_eq!(item["outcome"], "move");
            assert_eq!(item["moved_files"], 1);
            assert_eq!(item["total_files"], 1);
            item["item_id"]
                .as_str()
                .expect("confirmation item should retain its selected ID")
                .to_string()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(moved_item_ids, preview_item_ids);

    for (relative_path, contents) in &fixtures {
        let source_path = source_root.join(relative_path);
        let file_name = std::path::Path::new(relative_path)
            .file_name()
            .expect("fixture path should have a filename");
        let target_path = source_root.join("bulk-selected").join(file_name);
        assert!(!source_path.exists(), "selected source should be moved");
        assert_eq!(
            fs::read(target_path).expect("confirmed destination should exist"),
            *contents
        );
    }

    let recorded = take_cloud_test_telegram_requests(&mut telegram_requests);
    assert_eq!(
        recorded
            .iter()
            .filter(|request| request.method == "sendMessage")
            .count(),
        1,
        "one file confirmation should create one Telegram progress/reply message"
    );
    let edits = recorded
        .iter()
        .filter(|request| request.method == "editMessageText")
        .collect::<Vec<_>>();
    assert!(
        !edits.is_empty(),
        "the Telegram progress message should be edited with the result"
    );
    assert!(
        edits
            .iter()
            .all(|request| request.body["message_id"] == 1_000),
        "all Telegram updates should use the one progress message created by the fake API"
    );

    runtime.signal_epoch_fatal();
    dispatcher.abort();
    let _ = dispatcher.await;
    drop(runtime);
    drop(inbox);
    drop(queue);
    stop_fake_telegram_api(shutdown, server).await;
    let _ = fs::remove_dir_all(cleanup_root);
}
