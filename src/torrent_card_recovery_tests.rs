use super::*;
use crate::torrent_inline::render_torrent_inline;

#[tokio::test]
async fn changed_inventory_repairs_browse_and_review_cursor_for_card_recovery() {
    let fixture = Fixture::new();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend).expect("open torrent manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;
    let initial_remote =
        remote_snapshot(&fixture.metainfo, &fixture.source_root, &[0.5, 1.0], 0.7, 2);
    manager
        .attach_remote(&task_id, initial_remote.clone())
        .expect("attach initial inventory");

    let mut snapshot = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .expect("read initial snapshot");
    let album_folder_id = snapshot
        .folders
        .iter()
        .find(|folder| folder.relative_path == Path::new("album"))
        .expect("fixture album folder")
        .id;
    snapshot = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.expect("current card"),
            &task_id,
            snapshot.inventory_revision,
            TorrentAction::Browse {
                folder_id: album_folder_id,
                page: 0,
            },
        )
        .expect("browse into album");
    assert_eq!(snapshot.cursor_folder_id, album_folder_id);
    assert!(render_torrent_inline(&snapshot).is_ok());

    // Progress is intentionally excluded from the selected source inventory identity.
    let mut progress_only = initial_remote.clone();
    progress_only.progress = 0.8;
    progress_only.files[0].progress = 0.9;
    manager
        .attach_remote(&task_id, progress_only.clone())
        .expect("attach progress-only update");
    snapshot = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .expect("read progress-only snapshot");
    assert_eq!(snapshot.cursor_folder_id, album_folder_id);
    assert_eq!(snapshot.cursor_page, 0);
    assert!(render_torrent_inline(&snapshot).is_ok());

    snapshot = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.expect("current card"),
            &task_id,
            snapshot.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .expect("select an audio file");
    snapshot = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            snapshot.card_message_id.expect("current card"),
            &task_id,
            snapshot.selection_revision,
            TorrentAction::ReviewSelection { page: 0 },
        )
        .expect("open the review page");
    assert!(snapshot.review_selection);

    // qBittorrent legitimately changes the file layout so the old `album` folder no longer
    // exists. Keep the same task identity and sizes; only the selected source paths change.
    let mut replacement = progress_only;
    replacement.files[0].relative_path = PathBuf::from("new-song.mp3");
    replacement.files[1].relative_path = PathBuf::from("new-notes.txt");
    manager
        .attach_remote(&task_id, replacement)
        .expect("attach replacement file layout");

    let recovered = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .expect("read repaired snapshot");
    assert_eq!(
        recovered.cursor_folder_id, 0,
        "missing folder falls back to root"
    );
    assert_eq!(
        recovered.cursor_page, 0,
        "review/browser page resets safely"
    );
    assert!(!recovered.review_selection, "stale review mode is exited");
    assert!(render_torrent_inline(&recovered).is_ok());

    let navigated_root = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            recovered.card_message_id.expect("current card"),
            &task_id,
            recovered.inventory_revision,
            TorrentAction::Browse {
                folder_id: 0,
                page: 0,
            },
        )
        .expect("root navigation remains available");
    assert!(render_torrent_inline(&navigated_root).is_ok());

    let reopened = manager
        .reopen_card(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            &task_id,
            "cursor-recovery".to_string(),
        )
        .expect("request replacement card");
    assert_eq!(reopened.cursor_folder_id, 0);
    assert_eq!(reopened.cursor_page, 0);
    assert!(render_torrent_inline(&reopened).is_ok());
    manager.process_task_once_for_test(&task_id).await;
    let delivered = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .expect("read reopened card");
    assert!(delivered.card_message_id.is_some());
    assert!(render_torrent_inline(&delivered).is_ok());
}
