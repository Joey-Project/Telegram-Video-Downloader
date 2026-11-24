use super::*;

#[tokio::test]
async fn completed_pdf_reply_preserves_artifact_when_attachment_fails_e2e() {
    use std::os::unix::fs::PermissionsExt;

    const PDF_BYTES: &[u8] = b"%PDF-1.7\nsynthetic PDF fixture\n";
    let expected_sha256 = format!("{:x}", sha2::Sha256::digest(PDF_BYTES));

    for (case_index, (case, sender_succeeds, final_suffix)) in [
        ("success", true, "Replied with the saved PDF."),
        (
            "attachment-failure",
            false,
            "Telegram did not confirm delivery of the PDF attachment.",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let temp = tempfile::tempdir().expect("test directory");
        let root = temp.path();
        let mut config = AppConfig::for_test();
        config.downloads.video_dir = root.join("video");
        config.downloads.pdf_dir = root.join("pdf");
        config
            .ensure_runtime_dirs()
            .expect("test roots should create");

        let chat_id = 123_456_789;
        let original_message_id = 51 + case_index as i64;
        let update_id = 4_001 + case_index as i64;
        let bot_id = 987_654;
        config.telegram.token = format!("{bot_id}:test-token");
        config.telegram.allowed_chat_ids = vec![chat_id];
        config.telegram.allow_all_chats = false;

        let fake_uv = root.join("fake-uv");
        fs::write(
            &fake_uv,
            r#"#!/bin/sh
set -eu
output_dir=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output-dir) output_dir=$2; shift 2 ;;
    *) shift ;;
  esac
done
test -n "$output_dir"
mkdir -p "$output_dir"
printf '%s\n' '%PDF-1.7' 'synthetic PDF fixture' > "$output_dir/report.pdf"
printf '%s\n' "$output_dir/report.pdf"
"#,
        )
        .expect("mock PDF worker should write");
        fs::set_permissions(&fake_uv, fs::Permissions::from_mode(0o700))
            .expect("mock PDF worker should be executable");
        config.tools.uv = fake_uv;

        let saved_pdf = config.downloads.pdf_dir.join("report.pdf");
        let sender = root.join("fake-mtproto-python");
        fs::write(
            &sender,
            r##"#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

request = json.load(sys.stdin)
expected = json.loads(Path(__file__).with_name("sender-expectations.json").read_text())
file = Path(request["file_path"])
file_info = file.stat()
parent_info = file.parent.stat()
assert request["bot_token"] == expected["bot_token"]
assert request["chat_id"] == expected["chat_id"]
assert request["reply_to_message_id"] == expected["reply_to_message_id"]
assert request["file_name"] == "report.pdf"
assert request["expected_sha256"] == expected["sha256"]
assert hashlib.sha256(file.read_bytes()).hexdigest() == expected["sha256"]
assert file.resolve() != Path(expected["source_path"]).resolve()
assert file.parent.name.startswith("telegram-video-downloader-pdf-reply-")
assert stat.S_IMODE(file_info.st_mode) == 0o600
assert stat.S_IMODE(parent_info.st_mode) == 0o700
assert file_info.st_uid == os.getuid() and file_info.st_nlink == 1
assert request["random_id"] != 0
Path(expected["request_log"]).write_text(json.dumps(request))
if expected["sender_succeeds"]:
    print(json.dumps({"ok": True}))
else:
    print(json.dumps({"ok": False, "error": "rate_limited"}))
    sys.exit(1)
"##,
        )
        .expect("mock sender should write");
        fs::set_permissions(&sender, fs::Permissions::from_mode(0o700))
            .expect("mock sender should be executable");
        config.music.helper_python = sender;

        let request_log = root.join("sender-request.json");
        fs::write(
            root.join("sender-expectations.json"),
            serde_json::to_vec(&serde_json::json!({
                "bot_token": config.telegram.token,
                "chat_id": chat_id,
                "reply_to_message_id": original_message_id,
                "sha256": expected_sha256,
                "source_path": saved_pdf.display().to_string(),
                "request_log": request_log.display().to_string(),
                "sender_succeeds": sender_succeeds,
            }))
            .expect("sender expectations should encode"),
        )
        .expect("sender expectations should write");

        let queue = Arc::new(QueueManager::open(&config).expect("queue should open"));
        let (telegram, mut requests, updates, shutdown, server) = spawn_fake_telegram_api().await;
        let context = BotContext {
            telegram: telegram.clone(),
            config: Arc::new(config),
            job_dispatch: JobDispatch {
                download_semaphore: Arc::new(Semaphore::new(1)),
                duplicate_scan_semaphore: Arc::new(Semaphore::new(1)),
            },
            next_job_id: Arc::new(AtomicU64::new(1)),
            queue: Arc::clone(&queue),
            queue_start_retry_delay: Duration::ZERO,
            music: None,
            torrent: None,
        };
        updates
            .send(vec![serde_json::json!({
                "update_id": update_id,
                "message": {
                    "message_id": original_message_id,
                    "chat": {"id": chat_id, "type": "private"},
                    "from": {"id": chat_id},
                    "text": "/pdf https://example.test/report.pdf"
                }
            })])
            .expect("fake Telegram should accept the PDF request");
        let updates = telegram
            .get_updates(None, 0)
            .await
            .expect("fake Telegram should return the PDF request");
        let mut offset = None;
        process_telegram_updates_with_context(&context, updates, &mut offset)
            .await
            .expect("PDF request should enter the queue");
        assert_eq!(offset, Some(update_id + 1));

        let mut observed_messages = Vec::new();
        let final_text = tokio_timeout(Duration::from_secs(20), async {
            loop {
                let request = requests
                    .recv()
                    .await
                    .expect("fake Telegram request stream should remain open");
                if let Some(text) = request.body["text"].as_str() {
                    if text.starts_with("Finished job #") || text.starts_with("Failed job #") {
                        break text.to_owned();
                    }
                    observed_messages.push(text.to_owned());
                }
            }
        })
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{case} PDF job did not report completion: {error}; messages: {observed_messages:?}"
            )
        });
        assert!(
            final_text.ends_with(final_suffix),
            "{case} PDF reply status was unexpected: {final_text}"
        );
        assert!(final_text.contains(&format!("Saved: {}", saved_pdf.display())));

        let task_id = format!("u{update_id}-0");
        let completed = queue
            .get(&task_id)
            .expect("completed task should load")
            .expect("completed task should remain in the queue");
        assert_eq!(completed.status, TaskStatus::Completed);
        assert_eq!(completed.message_id, original_message_id);
        assert_eq!(completed.pdf_reply_bot_id, Some(bot_id));
        assert_eq!(
            completed.saved_location.as_deref(),
            Some(saved_pdf.to_str().unwrap())
        );
        assert_eq!(
            fs::read(&saved_pdf).expect("local PDF should remain"),
            PDF_BYTES
        );

        let saved_key = saved_pdf.display().to_string();
        assert_eq!(
            completed.primary_media_hashes.get(&saved_key),
            Some(&expected_sha256),
            "the completed queue record should retain the exact PDF digest"
        );
        let request: serde_json::Value = serde_json::from_slice(
            &fs::read(&request_log).expect("fake sender should record its verified request"),
        )
        .expect("sender request should be JSON");
        assert_eq!(request["reply_to_message_id"], original_message_id);
        assert_eq!(request["expected_sha256"], expected_sha256);
        let scratch_file = PathBuf::from(
            request["file_path"]
                .as_str()
                .expect("sender request should include its private copy"),
        );
        assert!(!scratch_file.exists(), "private PDF copy should be removed");
        assert!(
            !scratch_file.parent().unwrap().exists(),
            "private PDF scratch directory should be removed"
        );

        stop_fake_telegram_api(shutdown, server).await;
    }
}
