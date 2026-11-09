use super::*;

const SOURCE_BINDING: &str = "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TARGET_BINDING: &str = "v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SELECTED_ITEMS: usize = 5_000;
const REAL_ITEMS: usize = 101;

fn full_preview_result(seq: u64, item_count: usize) -> serde_json::Value {
    let item_ids = (0..item_count)
        .map(|index| format!("media-{index:05}"))
        .collect::<Vec<_>>();
    let items = item_ids
        .iter()
        .map(|item_id| {
            let path = format!("Media/{item_id}.mp4");
            serde_json::json!({
                "item_id": item_id,
                "status": "copying",
                "conflict": false,
                "source_path": path,
                "target_path": path,
                "bytes": 1,
                "files": [{
                    "source_path": path,
                    "target_path": path,
                    "size_bytes": 1,
                }],
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "id": format!("migration-preview-{seq}"),
        "revision": format!("migration-revision-{seq}"),
        "source_library_id": "library-source",
        "source_root_binding": SOURCE_BINDING,
        "target_library_id": "library-target",
        "target_root_binding": TARGET_BINDING,
        "scope": "selected",
        "item_ids": item_ids,
        "conflict": "skip",
        "item_conflicts": {},
        "include_incomplete_tasks": false,
        "set_target_default_on_success": false,
        "item_count": item_count,
        "file_count": item_count,
        "total_bytes": item_count as u64,
        "required_space_bytes": item_count as u64,
        "available_space_bytes": 8 * 1024 * 1024 * 1024_u64,
        "conflict_count": 0,
        "incomplete_task_count": 0,
        "staging_bytes": 0,
        "warnings": [],
        "items": items,
    })
}

fn preview_record(seq: u64, result: serde_json::Value) -> crate::cloud::InboxRecord {
    crate::cloud::InboxRecord {
        schema_version: 1,
        request: crate::cloud::CloudRequest {
            seq,
            kind: "library_migration_preview".to_string(),
            payload: serde_json::json!({
                "source_library_id": "library-source",
                "source_root_binding": SOURCE_BINDING,
                "target_library_id": "library-target",
                "target_root_binding": TARGET_BINDING,
                "scope": "selected",
                "item_ids": [],
                "conflict": "skip",
                "include_incomplete_tasks": false,
                "set_target_default_on_success": false,
            }),
            created_at: seq,
            extra: Default::default(),
        },
        status: InboxStatus::AwaitingConfirmation,
        attempts: 1,
        received_at: None,
        updated_at: seq + 1,
        next_attempt_at: 0,
        error: None,
        result: Some(result),
    }
}

fn large_migration_status(item_count: usize) -> serde_json::Value {
    let items = (0..item_count)
        .map(|index| {
            let item_id = format!("media-{index:05}");
            let path = format!("Media/{item_id}/{}clip.mp4", "nested/".repeat(8));
            serde_json::json!({
                "item_id": item_id,
                "status": "copying",
                "source_path": path,
                "target_path": path,
                "bytes": 1,
                "files": [{
                    "source_path": path,
                    "target_path": path,
                    "size_bytes": 1,
                    "status": "copying",
                }],
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "id": "migration-job-large",
        "preview_id": "migration-preview-large",
        "revision": "migration-revision-large",
        "created_at": 1,
        "status": "queued",
        "source_library_id": "library-source",
        "target_library_id": "library-target",
        "scope": "selected",
        "conflict": "skip",
        "item_conflicts": {},
        "include_incomplete_tasks": false,
        "set_target_default_on_success": false,
        "default_update_applied": false,
        "default_update_resolved": false,
        "terminal_finalization_pending": false,
        "total_items": item_count,
        "completed_items": 0,
        "success_count": 0,
        "skipped_count": 0,
        "failed_count": 0,
        "total_files": item_count,
        "completed_files": 0,
        "copied_bytes": 0,
        "total_bytes": item_count as u64,
        "required_space_bytes": item_count as u64,
        "available_space_bytes": 8 * 1024 * 1024 * 1024_u64,
        "incomplete_task_count": 0,
        "staging_bytes": 0,
        "awaiting_task_plans": false,
        "awaiting_task_commit": false,
        "warnings": [],
        "items": items,
        "task_transfers": [],
    })
}

#[test]
fn five_thousand_item_preview_receipt_compacts_to_the_existing_actionable_v1_summary() {
    let mut record = preview_record(700, full_preview_result(700, SELECTED_ITEMS));
    record.request.payload["item_ids"] = serde_json::to_value(
        record.result.as_ref().unwrap()["item_ids"]
            .as_array()
            .unwrap(),
    )
    .unwrap();
    let full_bytes = serde_json::to_vec(&record).unwrap().len();
    assert!(
        full_bytes > 1024 * 1024,
        "fixture should cross the inbox result limit"
    );

    assert!(record.compact_oversized_result().unwrap());
    assert_eq!(record.status, InboxStatus::AwaitingConfirmation);
    let compact = record.result.as_ref().unwrap();
    assert_eq!(compact["migration_summary"], true);
    assert_eq!(compact["preview_id"], "migration-preview-700");
    assert_eq!(compact["revision"], "migration-revision-700");
    assert_eq!(compact["result"]["item_count"], SELECTED_ITEMS);
    assert_eq!(compact["result"]["omitted_item_count"], SELECTED_ITEMS - 4);
    assert_eq!(compact["result"]["source_root_binding"], SOURCE_BINDING);
    assert!(serde_json::to_vec(&record).unwrap().len() < 1024 * 1024);
    assert!(
        crate::cloud_snapshot_budget::validated_migration_preview_summary(&record, compact)
            .is_some()
    );

    let projection = crate::cloud_snapshot_budget::preview_snapshots(&[&record], 0);
    assert_eq!(projection.len(), 1);
    assert_eq!(projection[0], *compact);
}

#[test]
fn oversized_migration_confirm_and_control_receipts_keep_their_success_status() {
    let migration = large_migration_status(SELECTED_ITEMS);
    for (seq, kind, result) in [
        (
            704,
            "library_migration_confirm",
            serde_json::json!({
                "preview_id": "migration-preview-large",
                "revision": "migration-revision-large",
                "migration": migration.clone(),
            }),
        ),
        (
            705,
            "library_migration_control",
            serde_json::json!({"migration": migration, "accepted": true}),
        ),
    ] {
        let mut record = crate::cloud::InboxRecord {
            schema_version: 1,
            request: crate::cloud::CloudRequest {
                seq,
                kind: kind.to_string(),
                payload: serde_json::json!({}),
                created_at: seq,
                extra: Default::default(),
            },
            status: InboxStatus::Completed,
            attempts: 1,
            received_at: None,
            updated_at: seq + 1,
            next_attempt_at: 0,
            error: None,
            result: Some(result),
        };
        assert!(serde_json::to_vec(&record).unwrap().len() > 1024 * 1024);
        assert!(record.compact_oversized_result().unwrap());
        assert_eq!(record.status, InboxStatus::Completed);
        let compact = record.result.unwrap();
        assert_eq!(compact["migration"]["migration_status_summary"], true);
        assert_eq!(compact["migration"]["summary_version"], 1);
        assert_eq!(compact["migration"]["total_items"], SELECTED_ITEMS);
        assert_eq!(compact["migration"]["omitted_item_count"], SELECTED_ITEMS);
        if kind == "library_migration_control" {
            assert_eq!(compact["accepted"], true);
        }
    }
}

#[test]
fn migration_confirmation_accepts_legacy_full_receipts_and_rejects_unbound_or_malformed_v1() {
    let full = full_preview_result(701, 1);
    let full_record = preview_record(701, full.clone());
    assert!(
        crate::library_migration_bot::validate_preview_receipt_for_test(
            &full_record,
            "migration-preview-701",
            "migration-revision-701",
        )
        .is_ok()
    );

    let malformed_header = preview_record(
        702,
        serde_json::json!({
            "id": "migration-preview-702",
            "revision": "migration-revision-702",
            "source_library_id": "library-source",
            "source_root_binding": SOURCE_BINDING,
            "target_library_id": "library-target",
            "target_root_binding": TARGET_BINDING,
        }),
    );
    assert!(
        crate::library_migration_bot::validate_preview_receipt_for_test(
            &malformed_header,
            "migration-preview-702",
            "migration-revision-702",
        )
        .is_err()
    );

    let mut large = preview_record(703, full_preview_result(703, SELECTED_ITEMS));
    large.request.payload["item_ids"] = large.result.as_ref().unwrap()["item_ids"].clone();
    assert!(large.compact_oversized_result().unwrap());
    let valid = large.result.clone().unwrap();
    assert!(
        crate::library_migration_bot::validate_preview_receipt_for_test(
            &large,
            "migration-preview-703",
            "migration-revision-703",
        )
        .is_ok()
    );

    let mut invalid_summaries = Vec::new();
    let mut marker = valid.clone();
    marker["migration_summary"] = serde_json::json!(false);
    invalid_summaries.push(marker);
    let mut sequence = valid.clone();
    sequence["seq"] = serde_json::json!(704);
    invalid_summaries.push(sequence);
    let mut id = valid.clone();
    id["result"]["id"] = serde_json::json!("another-preview");
    invalid_summaries.push(id);
    let mut revision = valid.clone();
    revision["result"]["revision"] = serde_json::json!("another-revision");
    invalid_summaries.push(revision);
    let mut source = valid.clone();
    source["result"]["source_library_id"] = serde_json::json!("another-source");
    invalid_summaries.push(source);
    let mut source_binding = valid.clone();
    source_binding["result"]["source_root_binding"] = serde_json::json!("v1:short");
    invalid_summaries.push(source_binding);
    let mut target = valid.clone();
    target["result"]["target_library_id"] = serde_json::json!("another-target");
    invalid_summaries.push(target);
    let mut target_binding = valid.clone();
    target_binding["result"]["target_root_binding"] = serde_json::json!(SOURCE_BINDING);
    invalid_summaries.push(target_binding);
    let mut counts = valid.clone();
    counts["result"]["item_count"] = serde_json::json!(SELECTED_ITEMS + 1);
    invalid_summaries.push(counts);
    let mut omitted = valid.clone();
    omitted["result"]["omitted_item_count"] = serde_json::json!(SELECTED_ITEMS - 3);
    invalid_summaries.push(omitted);
    let mut version = valid.clone();
    version["summary_version"] = serde_json::json!(2);
    invalid_summaries.push(version);

    for (index, result) in invalid_summaries.into_iter().enumerate() {
        let mut record = large.clone();
        record.result = Some(result);
        assert!(
            crate::library_migration_bot::validate_preview_receipt_for_test(
                &record,
                "migration-preview-703",
                "migration-revision-703",
            )
            .is_err(),
            "malformed summary case {index} must fail closed"
        );
    }

    let mut wrong_request_binding = large;
    wrong_request_binding.request.payload["target_root_binding"] =
        serde_json::json!(SOURCE_BINDING);
    assert!(
        crate::library_migration_bot::validate_preview_receipt_for_test(
            &wrong_request_binding,
            "migration-preview-703",
            "migration-revision-703",
        )
        .is_err()
    );
}

#[tokio::test]
async fn selected_101_migration_confirms_from_bounded_receipt_then_pauses_before_copy() {
    let root = temp_cloud_bot_dir("migration-101-receipt");
    let archive = root.join("archive");
    let (telegram, _requests, shutdown, server) = spawn_fake_telegram_api().await;
    let mut config = AppConfig::for_test();
    config.downloads.video_dir = root.join("videos");
    config.downloads.pdf_dir = root.join("pdfs");
    config
        .ensure_runtime_dirs()
        .expect("test roots should create");
    fs::create_dir_all(&archive).expect("archive root should create");
    for index in 0..REAL_ITEMS {
        fs::write(
            config
                .downloads
                .video_dir
                .join(format!("media-{index:05}.mp4")),
            b"x",
        )
        .expect("synthetic media fixture should write");
    }

    let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
    let source = queue
        .default_video_destination()
        .expect("source video library should resolve");
    let target = queue
        .register_video_library("Archive", &archive, None, false, false)
        .expect("target video library should register");
    let scan_started = std::time::Instant::now();
    let file_provider: Arc<dyn crate::file_provider::QueueFileProvider> =
        Arc::new(crate::file_provider::MockQueueFileProvider::default());
    let snapshot = crate::file_manager::LibraryManager::open_with_provider(
        &config.downloads.video_dir,
        Arc::clone(&file_provider),
    )
    .expect("source manager should open")
    .scan(&[])
    .expect("source media should scan");
    eprintln!(
        "migration_receipt_stage=source_scan item_count={} elapsed_ms={}",
        snapshot.items.len(),
        scan_started.elapsed().as_millis()
    );
    assert_eq!(snapshot.items.len(), REAL_ITEMS);
    let item_ids = snapshot
        .items
        .iter()
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();

    let inbox = Arc::new(
        open_runtime_test_inbox(&config.downloads.video_dir).expect("test cloud inbox should open"),
    );
    let mut runtime = make_runtime(
        telegram,
        config.clone(),
        Arc::clone(&queue),
        Arc::clone(&inbox),
    );
    runtime.test_file_provider = Some(file_provider);
    let dispatcher = tokio::spawn(cloud_request_dispatcher(runtime.clone()));
    let preview_started = std::time::Instant::now();
    let preview = run_library_management_test_request(
        &runtime,
        5_000,
        "library_migration_preview",
        bound_migration_preview_payload(
            &queue,
            &source.library_id,
            &target.id,
            serde_json::json!({
                "scope": "selected",
                "item_ids": item_ids,
                "conflict": "skip",
            }),
        ),
    )
    .await;
    eprintln!(
        "migration_receipt_stage=preview status={:?} elapsed_ms={}",
        preview.status,
        preview_started.elapsed().as_millis()
    );
    assert_eq!(
        preview.status,
        InboxStatus::AwaitingConfirmation,
        "{:?}",
        preview.error
    );
    let compacted_preview =
        crate::cloud_snapshot_budget::compact_library_migration_receipt_result(&preview)
            .expect("full preview should compact to the existing summary contract");
    runtime
        .inbox
        .update(5_000, |record| {
            assert_eq!(record.status, InboxStatus::AwaitingConfirmation);
            record.result = Some(compacted_preview.clone());
        })
        .expect("bounded preview summary should persist");
    let compacted_record = runtime
        .inbox
        .record(5_000)
        .expect("bounded preview receipt should load")
        .expect("bounded preview receipt should remain present");
    let preview_result = compacted_record
        .result
        .expect("bounded preview summary should persist");
    assert_eq!(preview_result["migration_summary"], true);
    assert_eq!(preview_result["result"]["item_count"], REAL_ITEMS);
    assert_eq!(preview_result["preview_id"], preview_result["result"]["id"]);

    let library_ids = vec![source.library_id.clone(), target.id.clone()];
    let held_read_lease = runtime
        .library_io_gates
        .try_acquire(&library_ids)
        .await
        .expect("test should hold a shared library lease until pause is recorded");
    let confirm_started = std::time::Instant::now();
    let confirm = run_library_management_test_request(
        &runtime,
        5_001,
        "library_migration_confirm",
        serde_json::json!({
            "preview_id": preview_result["preview_id"],
            "revision": preview_result["revision"],
        }),
    )
    .await;
    eprintln!(
        "migration_receipt_stage=confirm status={:?} elapsed_ms={}",
        confirm.status,
        confirm_started.elapsed().as_millis()
    );
    assert_eq!(
        confirm.status,
        InboxStatus::Completed,
        "{:?}",
        confirm.error
    );
    let confirm_result = confirm.result.expect("confirm result should persist");
    assert_eq!(
        confirm_result["migration"]["preview_id"],
        preview_result["preview_id"]
    );
    assert_eq!(confirm_result["migration"]["total_items"], REAL_ITEMS);
    assert_eq!(confirm_result["migration"]["status"], "queued");

    tokio::time::timeout(
        Duration::from_secs(20),
        runtime.test_migration_runner_gate_waiting.notified(),
    )
    .await
    .expect("confirmed runner should wait behind the test-held read lease");
    let migration_id = confirm_result["migration"]["id"]
        .as_str()
        .expect("migration ID should be retained")
        .to_string();
    let pause = run_library_management_test_request(
        &runtime,
        5_002,
        "library_migration_control",
        serde_json::json!({"migration_id":migration_id,"control":"pause"}),
    )
    .await;
    assert_eq!(pause.status, InboxStatus::Completed, "{:?}", pause.error);
    let pause_result = pause.result.expect("pause result should persist");
    assert_eq!(pause_result["migration"]["status"], "paused");

    drop(held_read_lease);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let active = runtime
                .migration_runners
                .lock()
                .await
                .contains(&migration_id);
            let status = runtime.migrations.lock().await.get(&migration_id).cloned();
            if !active
                && status.is_some_and(|status| {
                    status.status == crate::library_migration::MigrationRunStatus::Paused
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("paused runner should finish without applying the full selection");

    for index in 0..REAL_ITEMS {
        assert!(
            config
                .downloads
                .video_dir
                .join(format!("media-{index:05}.mp4"))
                .exists(),
            "source item {index} should remain untouched"
        );
        assert!(
            !archive.join(format!("media-{index:05}.mp4")).exists(),
            "target item {index} must not be copied by this receipt test"
        );
    }

    dispatcher.abort();
    stop_fake_telegram_api(shutdown, server).await;
    fs::remove_dir_all(root).expect("temporary migration fixture should clean up");
}
