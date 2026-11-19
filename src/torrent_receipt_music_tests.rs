use super::*;

fn fixture_with_files(files: &[(&str, u64)]) -> Fixture {
    let mut fixture = Fixture::new();
    let raw_torrent = torrent_with_files(files);
    let metainfo = crate::torrent_metainfo::parse_metainfo(
        &raw_torrent,
        DEFAULT_MAX_TORRENT_BYTES,
        DEFAULT_MAX_FILES,
    )
    .expect("synthetic torrent should parse");
    fixture.upload.file_size = raw_torrent.len() as u64;
    fixture.raw_torrent = raw_torrent;
    fixture.metainfo = metainfo;
    fixture
}

fn torrent_with_files(files: &[(&str, u64)]) -> Vec<u8> {
    let metainfo_files = files
        .iter()
        .map(|(path, size)| {
            dict(vec![
                (b"length".to_vec(), integer(*size as i64)),
                (b"path".to_vec(), list(path.split('/').map(text).collect())),
            ])
        })
        .collect();
    let info = dict(vec![
        (b"files".to_vec(), list(metainfo_files)),
        (b"name".to_vec(), text("release")),
        (b"piece length".to_vec(), integer(16_384)),
        (b"pieces".to_vec(), bencode_bytes(&[7; 20])),
    ]);
    dict(vec![(b"info".to_vec(), info)])
}

fn attach_complete_inventory(
    manager: &TorrentManager,
    fixture: &Fixture,
    task_id: &str,
) -> TorrentSnapshot {
    let mut progress = Vec::with_capacity(fixture.metainfo.files.len());
    for file in &fixture.metainfo.files {
        let source = fixture.source_root.join(&file.relative_path);
        std::fs::create_dir_all(source.parent().expect("torrent file should have a parent"))
            .expect("create torrent payload directory");
        std::fs::write(&source, vec![b'x'; file.size as usize])
            .expect("create completed torrent payload");
        progress.push(1.0);
    }
    manager
        .attach_remote(
            task_id,
            remote_snapshot(&fixture.metainfo, &fixture.source_root, &progress, 1.0, 0),
        )
        .expect("attach complete qBittorrent inventory");
    manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, task_id)
        .expect("read verified task snapshot")
}

fn scheduled_music_count(snapshot: &TorrentSnapshot) -> usize {
    snapshot
        .files
        .iter()
        .filter(|file| file.supported_music && file.music_stage.is_some())
        .count()
}

#[tokio::test]
async fn add_all_music_schedules_every_supported_track_once_across_replay_and_restart() {
    let fixture = fixture_with_files(&[
        ("album/first.mp3", 4),
        ("album/second.flac", 5),
        ("album/cover.jpg", 6),
    ]);
    let settings = fixture.settings();
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(settings.clone(), backend.clone()).expect("open manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;
    let initial = attach_complete_inventory(&manager, &fixture, &task_id);
    assert!(initial.inventory_verified);

    let card_message_id = initial
        .card_message_id
        .expect("task card should be persisted");
    let scheduled = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            card_message_id,
            &task_id,
            initial.selection_revision,
            TorrentAction::AddAllMusic,
        )
        .expect("atomically select and schedule all supported tracks");
    assert_eq!(
        scheduled
            .files
            .iter()
            .filter(|file| file.supported_music && file.selected)
            .map(|file| file.index)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(
        !scheduled.files[2].selected,
        "unsupported files stay unselected"
    );
    assert_eq!(scheduled_music_count(&scheduled), 2);
    assert_eq!(
        scheduled.import_count, 0,
        "pending requests are not imported jobs"
    );
    assert!(
        scheduled.files[..2]
            .iter()
            .all(|file| { file.music_stage == Some(TorrentMusicStage::Pending) })
    );
    assert_eq!(scheduled.card_message_id, Some(card_message_id));

    let replayed = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            card_message_id,
            &task_id,
            scheduled.selection_revision,
            TorrentAction::AddAllMusic,
        )
        .expect("a repeated click should reuse the same import ledger requests");
    assert_eq!(scheduled_music_count(&replayed), 2);
    assert_eq!(replayed.import_count, 0);
    assert_eq!(replayed.card_message_id, Some(card_message_id));
    assert!(
        backend.music_batches().is_empty(),
        "selection persists before Music admission"
    );
    drop(manager);

    let restarted = TorrentManager::open(settings, backend.clone()).expect("reopen manager");
    let recovered = restarted
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .expect("read recovered Add All Music ledger");
    assert_eq!(scheduled_music_count(&recovered), 2);
    assert_eq!(recovered.import_count, 0);
    assert_eq!(recovered.card_message_id, Some(card_message_id));
    let restarted_replay = restarted
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            card_message_id,
            &task_id,
            recovered.selection_revision,
            TorrentAction::AddAllMusic,
        )
        .expect("replay after restart should reuse the durable ledger");
    assert_eq!(scheduled_music_count(&restarted_replay), 2);
    assert_eq!(restarted_replay.import_count, 0);

    restarted.process_task_once_for_test(&task_id).await;
    restarted.process_task_once_for_test(&task_id).await;
    let batches = backend.music_batches();
    assert_eq!(
        batches.len(),
        1,
        "one batch admits the supported tracks once"
    );
    assert_eq!(batches[0].1.len(), 2);
    assert_eq!(
        batches[0].1[0].relative_path,
        PathBuf::from("album/first.mp3")
    );
    assert_eq!(
        batches[0].1[1].relative_path,
        PathBuf::from("album/second.flac")
    );
    for file in &fixture.metainfo.files {
        assert!(
            fixture.source_root.join(&file.relative_path).is_file(),
            "Music admission must leave the qBittorrent payload in place"
        );
    }
    assert_eq!(
        backend.music_batches().len(),
        1,
        "worker replay must not duplicate admission"
    );
}

#[tokio::test]
async fn add_all_music_rejects_unverified_empty_and_disabled_imports_without_receipts() {
    let unverified_fixture = fixture_with_files(&[("album/song.mp3", 4)]);
    let unverified_backend = unverified_fixture.backend(Vec::new(), None);
    let unverified_manager =
        TorrentManager::open(unverified_fixture.settings(), unverified_backend.clone())
            .expect("open unverified manager");
    let unverified_id = parse_task(&unverified_manager, unverified_fixture.upload.clone()).await;
    let unverified = unverified_manager
        .snapshot(
            unverified_fixture.upload.chat_id,
            unverified_fixture.upload.user_id,
            &unverified_id,
        )
        .expect("read metainfo-only task");
    assert!(!unverified.inventory_verified);
    let error = unverified_manager
        .apply_action(
            unverified_fixture.upload.chat_id,
            unverified_fixture.upload.user_id,
            unverified.card_message_id.expect("pending task card"),
            &unverified_id,
            unverified.selection_revision,
            TorrentAction::AddAllMusic,
        )
        .expect_err("metainfo-only files are not sufficient to schedule imports");
    assert!(
        error
            .to_string()
            .contains("verified torrent files are not ready")
    );
    let still_unverified = unverified_manager
        .snapshot(
            unverified_fixture.upload.chat_id,
            unverified_fixture.upload.user_id,
            &unverified_id,
        )
        .expect("read unchanged unverified task");
    assert_eq!(still_unverified.selected_count, 0);
    assert_eq!(scheduled_music_count(&still_unverified), 0);

    let empty_fixture = fixture_with_files(&[("album/video.mp4", 4), ("album/notes.txt", 5)]);
    let empty_backend = empty_fixture.backend(Vec::new(), None);
    let empty_manager = TorrentManager::open(empty_fixture.settings(), empty_backend.clone())
        .expect("open manager");
    let empty_id = parse_task(&empty_manager, empty_fixture.upload.clone()).await;
    let empty = attach_complete_inventory(&empty_manager, &empty_fixture, &empty_id);
    assert!(empty.inventory_verified);
    let error = empty_manager
        .apply_action(
            empty_fixture.upload.chat_id,
            empty_fixture.upload.user_id,
            empty.card_message_id.expect("empty task card"),
            &empty_id,
            empty.selection_revision,
            TorrentAction::AddAllMusic,
        )
        .expect_err("an inventory without supported music must not be scheduled");
    assert!(
        error
            .to_string()
            .contains("torrent has no supported music files")
    );
    let unchanged = empty_manager
        .snapshot(
            empty_fixture.upload.chat_id,
            empty_fixture.upload.user_id,
            &empty_id,
        )
        .expect("read unchanged empty-music task");
    assert_eq!(unchanged.selected_count, 0);
    assert_eq!(scheduled_music_count(&unchanged), 0);

    let disabled_fixture = fixture_with_files(&[("album/song.mp3", 4)]);
    let disabled_backend = disabled_fixture.backend(Vec::new(), None);
    disabled_backend.set_music_imports_enabled(false);
    let disabled_manager =
        TorrentManager::open(disabled_fixture.settings(), disabled_backend.clone())
            .expect("open disabled Music manager");
    let disabled_id = parse_task(&disabled_manager, disabled_fixture.upload.clone()).await;
    let disabled = attach_complete_inventory(&disabled_manager, &disabled_fixture, &disabled_id);
    let error = disabled_manager
        .apply_action(
            disabled_fixture.upload.chat_id,
            disabled_fixture.upload.user_id,
            disabled.card_message_id.expect("disabled task card"),
            &disabled_id,
            disabled.selection_revision,
            TorrentAction::AddAllMusic,
        )
        .expect_err("disabled Music must not report scheduled imports");
    assert!(error.to_string().contains("Music imports are disabled"));
    let unchanged = disabled_manager
        .snapshot(
            disabled_fixture.upload.chat_id,
            disabled_fixture.upload.user_id,
            &disabled_id,
        )
        .expect("read unchanged disabled task");
    assert_eq!(unchanged.selected_count, 0);
    assert_eq!(scheduled_music_count(&unchanged), 0);
}

#[tokio::test]
async fn add_all_music_storage_failure_does_not_return_or_persist_a_success_receipt() {
    let fixture = fixture_with_files(&[("album/song.mp3", 4), ("album/second.flac", 5)]);
    let backend = fixture.backend(Vec::new(), None);
    let manager = TorrentManager::open(fixture.settings(), backend).expect("open manager");
    let task_id = parse_task(&manager, fixture.upload.clone()).await;
    let before = attach_complete_inventory(&manager, &fixture, &task_id);
    let card_message_id = before
        .card_message_id
        .expect("task card should be persisted");
    let state_path = manager.store.state_path.clone();
    let backup_path = fixture.state_dir.join("state-before-failed-add-all.json");
    std::fs::rename(&state_path, &backup_path).expect("temporarily move the test state file");
    std::fs::create_dir(&state_path).expect("block the atomic state-file replacement");

    let result = manager.apply_action(
        fixture.upload.chat_id,
        fixture.upload.user_id,
        card_message_id,
        &task_id,
        before.selection_revision,
        TorrentAction::AddAllMusic,
    );
    assert!(
        result.is_err(),
        "failed persistence must not return an Add All receipt"
    );
    let unchanged = manager
        .snapshot(fixture.upload.chat_id, fixture.upload.user_id, &task_id)
        .expect("read uncommitted in-memory task state");
    assert_eq!(unchanged.selected_count, 0);
    assert_eq!(scheduled_music_count(&unchanged), 0);
    assert!(
        unchanged
            .files
            .iter()
            .all(|file| { !file.selected && file.music_stage.is_none() })
    );

    std::fs::remove_dir(&state_path).expect("remove state-file blocker");
    std::fs::rename(&backup_path, &state_path).expect("restore original private state file");
}
