use super::*;
use std::sync::Arc;
use std::time::Duration;

use crate::cloud::{CloudRequest, InboxStatus, TEST_DATABASE_EPOCH};
use crate::config::AppConfig;
use crate::queue::QueueManager;

#[test]
fn injected_torrent_actor_must_match_the_canonical_private_owner() {
    let payload = serde_json::json!({
        "operation": "catalog",
        "actor_user_id": "123456789",
    });
    let object = payload.as_object().unwrap();
    assert_eq!(torrent_action_actor(object, 123_456_789), Some("123456789"));
    assert_eq!(torrent_action_actor(object, 123), None);

    let leading_zero = serde_json::json!({"actor_user_id": "0123"});
    assert_eq!(
        torrent_action_actor(leading_zero.as_object().unwrap(), 123),
        None
    );
    let numeric = serde_json::json!({"actor_user_id": 123});
    assert_eq!(
        torrent_action_actor(numeric.as_object().unwrap(), 123),
        None
    );
    assert_eq!(torrent_action_actor(object, 0), None);
}

#[test]
fn torrent_action_identity_comes_from_the_cloud_envelope() {
    let mut request = CloudRequest {
        seq: 42,
        kind: "torrent_action".to_string(),
        payload: serde_json::json!({"operation": "detail"}),
        created_at: 1,
        extra: Default::default(),
    };
    request.extra.insert(
        "request_id".to_string(),
        serde_json::json!("action:00000000-0000-4000-8000-000000000001"),
    );
    assert_eq!(
        cloud_action_request_id(&request),
        Some("action:00000000-0000-4000-8000-000000000001")
    );
    assert_eq!(
        format!(
            "{}:{}",
            cloud_action_request_id(&request).unwrap(),
            request.seq
        ),
        "action:00000000-0000-4000-8000-000000000001:42"
    );

    request
        .extra
        .insert("request_id".to_string(), serde_json::json!("  "));
    assert_eq!(cloud_action_request_id(&request), None);
}

#[test]
fn saved_torrent_review_indexes_must_be_unique_and_in_range() {
    let valid = serde_json::json!({
        "selected_count": 3,
        "selected_indexes": [0, 17, 4999],
    });
    assert_eq!(
        validated_torrent_review_indexes(&valid),
        Some(vec![0, 17, 4999])
    );

    let duplicate = serde_json::json!({
        "selected_count": 2,
        "selected_indexes": [17, 17],
    });
    assert_eq!(validated_torrent_review_indexes(&duplicate), None);

    let out_of_range = serde_json::json!({
        "selected_count": 1,
        "selected_indexes": [5000],
    });
    assert_eq!(validated_torrent_review_indexes(&out_of_range), None);

    let mismatched_count = serde_json::json!({
        "selected_count": 2,
        "selected_indexes": [1],
    });
    assert_eq!(validated_torrent_review_indexes(&mismatched_count), None);
}

fn torrent_request(seq: u64, payload: serde_json::Value) -> CloudRequest {
    let mut request = CloudRequest {
        seq,
        kind: "torrent_action".to_string(),
        payload,
        created_at: seq,
        extra: Default::default(),
    };
    request.extra.insert(
        "request_id".to_string(),
        serde_json::json!(format!("action:00000000-0000-4000-8000-{seq:012x}")),
    );
    request
}

async fn dispatch_and_persist_torrent_request(
    runtime: &CloudBotRuntime,
    inbox: &Arc<crate::cloud::CloudInbox>,
    request: &CloudRequest,
) -> anyhow::Result<CloudDispatchOutcome> {
    inbox.persist(request.clone())?;
    let outcome = dispatch_cloud_management(runtime, request).await?;
    persist_cloud_dispatch_outcome(runtime, request.seq, outcome.clone()).await;
    Ok(outcome)
}

#[tokio::test]
async fn cloud_inbox_review_confirm_and_replay_use_only_the_saved_draft() {
    let fixture = crate::torrent_miniapp_tests::miniapp_test_fixture()
        .await
        .expect("Mini App qBittorrent fixture");
    let cloud_root = fixture.temp.path().join("cloud-bridge");
    let mut config = AppConfig::for_test();
    config.telegram.allow_all_chats = false;
    config.telegram.allowed_chat_ids = vec![fixture.owner_chat];
    config.downloads.video_dir = cloud_root.join("video");
    config.downloads.pdf_dir = cloud_root.join("pdf");
    config
        .ensure_runtime_dirs()
        .expect("cloud test roots should exist");
    let queue = Arc::new(QueueManager::open(&config).expect("cloud test queue should open"));
    let inbox = Arc::new(
        crate::cloud::CloudInbox::open_for_epoch(&config.downloads.video_dir, TEST_DATABASE_EPOCH)
            .expect("cloud inbox should open"),
    );
    let (telegram, _requests, _shutdown, _server) =
        crate::cloud_bot_tests::spawn_fake_telegram_api().await;
    let mut runtime =
        crate::cloud_bot_tests::make_runtime(telegram, config, queue, Arc::clone(&inbox));
    runtime.bot.torrent = Some(Arc::clone(&fixture.service));

    let detail = fixture
        .service
        .handle_miniapp_action(
            fixture.owner_chat,
            "action:detail-seed:1",
            &serde_json::json!({
                "operation": "detail",
                "task_id": fixture.task_id,
                "filter": "music",
            }),
        )
        .await
        .expect("detail should provide review revisions");
    let first_review = torrent_request(
        1,
        serde_json::json!({
            "operation": "review",
            "actor_user_id": fixture.owner_chat.to_string(),
            "task_id": fixture.task_id,
            "inventory_revision": detail["task"]["inventory_revision"],
            "selection_revision": detail["task"]["selection_revision"],
            "selected_indexes": [0],
        }),
    );
    let review_notify = runtime.state_notify.notified();
    tokio::pin!(review_notify);
    let first_review_outcome =
        dispatch_and_persist_torrent_request(&runtime, &inbox, &first_review)
            .await
            .expect("review should dispatch through the durable cloud inbox");
    tokio::time::timeout(Duration::from_secs(1), review_notify)
        .await
        .expect("durable review result should wake the state reporter");
    assert_eq!(first_review_outcome.status, InboxStatus::Completed);
    let saved_review = inbox
        .record(first_review.seq)
        .expect("review receipt should be readable")
        .expect("review receipt should be durable");
    assert_eq!(saved_review.status, InboxStatus::Completed);
    assert_eq!(saved_review.result.as_ref().unwrap()["operation"], "review");
    assert_eq!(
        crate::cloud_snapshot_budget::operation_snapshots(&[&saved_review])
            .expect("review operation should be snapshotable")[0]["request_id"],
        first_review.extra["request_id"]
    );
    let first_review_id = saved_review.result.as_ref().unwrap()["review_id"]
        .as_str()
        .expect("successful review should carry its id")
        .to_string();

    let first_confirm = torrent_request(
        2,
        serde_json::json!({
            "operation": "confirm",
            "actor_user_id": fixture.owner_chat.to_string(),
            "review_seq": first_review.seq,
            "review_id": first_review_id,
        }),
    );
    let first_confirm_outcome =
        dispatch_and_persist_torrent_request(&runtime, &inbox, &first_confirm)
            .await
            .expect("confirm should reconstruct and commit the saved draft");
    assert_eq!(first_confirm_outcome.status, InboxStatus::Completed);
    assert_eq!(
        first_confirm_outcome.result.as_ref().unwrap()["selected_count"],
        1
    );
    let saved_confirm = inbox
        .record(first_confirm.seq)
        .expect("confirm receipt should be readable")
        .expect("confirm receipt should be durable");
    assert_eq!(saved_confirm.status, InboxStatus::Completed);
    assert_eq!(
        saved_confirm.result.as_ref().unwrap()["operation"],
        "confirm"
    );

    let updated_detail = fixture
        .service
        .handle_miniapp_action(
            fixture.owner_chat,
            "action:detail-seed:2",
            &serde_json::json!({
                "operation": "detail",
                "task_id": fixture.task_id,
                "filter": "music",
            }),
        )
        .await
        .expect("detail after first confirm");
    let second_review = torrent_request(
        3,
        serde_json::json!({
            "operation": "review",
            "actor_user_id": fixture.owner_chat.to_string(),
            "task_id": fixture.task_id,
            "inventory_revision": updated_detail["task"]["inventory_revision"],
            "selection_revision": updated_detail["task"]["selection_revision"],
            "selected_indexes": [1],
        }),
    );
    let second_review_outcome =
        dispatch_and_persist_torrent_request(&runtime, &inbox, &second_review)
            .await
            .expect("second review should dispatch");
    let second_review_id = second_review_outcome.result.as_ref().unwrap()["review_id"]
        .as_str()
        .expect("second review id")
        .to_string();
    let second_confirm = torrent_request(
        4,
        serde_json::json!({
            "operation": "confirm",
            "actor_user_id": fixture.owner_chat.to_string(),
            "review_seq": second_review.seq,
            "review_id": second_review_id,
        }),
    );
    let second_confirm_outcome =
        dispatch_and_persist_torrent_request(&runtime, &inbox, &second_confirm)
            .await
            .expect("second saved draft should confirm");
    assert_eq!(second_confirm_outcome.status, InboxStatus::Completed);

    let replay = dispatch_cloud_management(&runtime, &first_confirm)
        .await
        .expect("replaying an old confirm should be idempotent");
    assert_eq!(replay.status, InboxStatus::Completed);
    assert_eq!(replay.result.as_ref().unwrap()["replayed"], true);
    let stale_new_confirm = torrent_request(
        12,
        serde_json::json!({
            "operation": "confirm",
            "actor_user_id": fixture.owner_chat.to_string(),
            "review_seq": first_review.seq,
            "review_id": first_review_id,
        }),
    );
    let stale_outcome = dispatch_and_persist_torrent_request(&runtime, &inbox, &stale_new_confirm)
        .await
        .expect("a new command using stale reviewed revisions should fail");
    assert_eq!(stale_outcome.status, InboxStatus::Failed);
    let final_detail = fixture
        .service
        .handle_miniapp_action(
            fixture.owner_chat,
            "action:detail-seed:3",
            &serde_json::json!({
                "operation": "detail",
                "task_id": fixture.task_id,
                "filter": "music",
            }),
        )
        .await
        .expect("final task detail");
    assert_eq!(
        final_detail["task"]["selected_indexes"],
        serde_json::json!([1])
    );

    let mut substituted = first_confirm.clone();
    substituted.seq = 5;
    substituted.extra.insert(
        "request_id".to_string(),
        serde_json::json!("action:00000000-0000-4000-8000-000000000005"),
    );
    substituted.payload["selected_indexes"] = serde_json::json!([0, 1]);
    let rejected_substitution =
        dispatch_and_persist_torrent_request(&runtime, &inbox, &substituted)
            .await
            .expect("substituted draft should produce a durable failure");
    assert_eq!(rejected_substitution.status, InboxStatus::Failed);

    let mut wrong_owner = first_confirm.clone();
    wrong_owner.seq = 6;
    wrong_owner.extra.insert(
        "request_id".to_string(),
        serde_json::json!("action:00000000-0000-4000-8000-000000000006"),
    );
    wrong_owner.payload["actor_user_id"] = serde_json::json!("18");
    let rejected_owner = dispatch_and_persist_torrent_request(&runtime, &inbox, &wrong_owner)
        .await
        .expect("wrong owner should produce a durable failure");
    assert_eq!(rejected_owner.status, InboxStatus::Failed);

    let mut wrong_review_id = first_confirm.clone();
    wrong_review_id.seq = 7;
    wrong_review_id.extra.insert(
        "request_id".to_string(),
        serde_json::json!("action:00000000-0000-4000-8000-000000000007"),
    );
    wrong_review_id.payload["review_id"] = serde_json::json!("forged-review-id");
    let rejected_review_id =
        dispatch_and_persist_torrent_request(&runtime, &inbox, &wrong_review_id)
            .await
            .expect("mismatched review result identity should fail");
    assert_eq!(rejected_review_id.status, InboxStatus::Failed);

    let mut wrong_kind_receipt = CloudRequest {
        seq: 8,
        kind: "library_scan".to_string(),
        payload: serde_json::json!({
            "actor_user_id": fixture.owner_chat.to_string(),
            "operation": "review",
        }),
        created_at: 8,
        extra: Default::default(),
    };
    wrong_kind_receipt.extra.insert(
        "request_id".to_string(),
        serde_json::json!("action:00000000-0000-4000-8000-000000000008"),
    );
    inbox
        .persist(wrong_kind_receipt.clone())
        .expect("synthetic wrong-kind receipt should persist");
    inbox
        .persist_dispatch_result(
            wrong_kind_receipt.seq,
            InboxStatus::Completed,
            Some(serde_json::json!({
                "operation": "review",
                "review_id": first_review_id,
            })),
            None,
        )
        .expect("synthetic wrong-kind result should persist");
    let wrong_kind_confirm = torrent_request(
        9,
        serde_json::json!({
            "operation": "confirm",
            "actor_user_id": fixture.owner_chat.to_string(),
            "review_seq": wrong_kind_receipt.seq,
            "review_id": first_review_id,
        }),
    );
    let rejected_kind = dispatch_and_persist_torrent_request(&runtime, &inbox, &wrong_kind_confirm)
        .await
        .expect("wrong-kind receipt should fail");
    assert_eq!(rejected_kind.status, InboxStatus::Failed);

    let wrong_operation_receipt = torrent_request(
        10,
        serde_json::json!({
            "operation": "detail",
            "actor_user_id": fixture.owner_chat.to_string(),
        }),
    );
    inbox
        .persist(wrong_operation_receipt.clone())
        .expect("synthetic wrong-operation receipt should persist");
    inbox
        .persist_dispatch_result(
            wrong_operation_receipt.seq,
            InboxStatus::Completed,
            Some(serde_json::json!({
                "operation": "detail",
                "review_id": first_review_id,
            })),
            None,
        )
        .expect("synthetic wrong-operation result should persist");
    let wrong_operation_confirm = torrent_request(
        11,
        serde_json::json!({
            "operation": "confirm",
            "actor_user_id": fixture.owner_chat.to_string(),
            "review_seq": wrong_operation_receipt.seq,
            "review_id": first_review_id,
        }),
    );
    let rejected_operation =
        dispatch_and_persist_torrent_request(&runtime, &inbox, &wrong_operation_confirm)
            .await
            .expect("wrong-operation receipt should fail");
    assert_eq!(rejected_operation.status, InboxStatus::Failed);

    let failed_review = torrent_request(
        13,
        serde_json::json!({
            "operation": "review",
            "actor_user_id": fixture.owner_chat.to_string(),
            "task_id": fixture.task_id,
            "inventory_revision": updated_detail["task"]["inventory_revision"],
            "selection_revision": updated_detail["task"]["selection_revision"],
            "selected_indexes": [0],
        }),
    );
    inbox
        .persist(failed_review.clone())
        .expect("failed review receipt should persist");
    inbox
        .persist_dispatch_result(
            failed_review.seq,
            InboxStatus::Failed,
            Some(serde_json::json!({
                "operation": "review",
                "review_id": "failed-review-id",
            })),
            None,
        )
        .expect("failed review status should persist");
    let failed_confirm = torrent_request(
        14,
        serde_json::json!({
            "operation": "confirm",
            "actor_user_id": fixture.owner_chat.to_string(),
            "review_seq": failed_review.seq,
            "review_id": "failed-review-id",
        }),
    );
    let rejected_failed_review =
        dispatch_and_persist_torrent_request(&runtime, &inbox, &failed_confirm)
            .await
            .expect("failed review must not confirm");
    assert_eq!(rejected_failed_review.status, InboxStatus::Failed);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while fixture.backend.music_payloads().len() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let payloads = fixture.backend.music_payloads();
    assert_eq!(payloads.len(), 2);
    assert_eq!(payloads[0].len(), 1);
    assert_eq!(
        payloads[0][0].relative_path,
        std::path::PathBuf::from("album/first.flac")
    );
    assert_eq!(payloads[1].len(), 1);
    assert_eq!(
        payloads[1][0].relative_path,
        std::path::PathBuf::from("album/second.flac")
    );
    fixture
        .shutdown
        .send(true)
        .expect("stop Mini App test fixture");
}
