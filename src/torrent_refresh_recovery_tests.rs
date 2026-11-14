use super::*;

async fn create_confirmed_existing_task(
    fixture: &Fixture,
) -> (
    Arc<TestBackend>,
    Arc<TorrentManager>,
    String,
    TorrentSnapshot,
) {
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    let backend = fixture.backend(vec![remote.clone()], None);
    backend.configure_magnet(
        fixture.source_root.clone(),
        Some(fixture.raw_torrent.clone()),
        0,
    );
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let queued = manager
        .adopt_existing(
            existing_source(fixture, remote.hash, fixture.upload.user_id),
            "catalog:refresh-recovery".to_string(),
        )
        .await
        .expect("adopt existing qBittorrent task");
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    let verified = manager
        .snapshot(
            fixture.upload.user_id,
            fixture.upload.user_id,
            &queued.task_id,
        )
        .expect("read verified existing task");
    let card_id = verified.card_message_id.expect("card delivered");
    let selected = manager
        .apply_action(
            fixture.upload.user_id,
            fixture.upload.user_id,
            card_id,
            &queued.task_id,
            verified.inventory_revision,
            TorrentAction::SetFile {
                index: 0,
                selected: true,
            },
        )
        .expect("select file");
    let confirmed = manager
        .apply_action(
            fixture.upload.user_id,
            fixture.upload.user_id,
            card_id,
            &queued.task_id,
            selected.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("confirm selection");
    (backend, manager, queued.task_id, confirmed)
}

#[tokio::test]
async fn transient_refresh_failure_preserves_confirmation_and_explicit_retry_resumes() {
    let fixture = Fixture::new();
    let (backend, manager, task_id, confirmed) = create_confirmed_existing_task(&fixture).await;
    let original = manager.store.task(&task_id).unwrap().unwrap();
    let original_import = original.imports.get(&0).unwrap().clone();

    backend.state.lock().unwrap().visible_candidates.clear();
    manager
        .apply_action(
            fixture.upload.user_id,
            fixture.upload.user_id,
            confirmed.card_message_id.unwrap(),
            &task_id,
            confirmed.inventory_revision,
            TorrentAction::Refresh,
        )
        .expect("request a read-only refresh");
    manager.process_task_once_for_test(&task_id).await;

    let now = unix_seconds();
    let deferred = manager.store.task(&task_id).unwrap().unwrap();
    let deferred_import = deferred.imports.get(&0).unwrap();
    assert_eq!(deferred.phase, original.phase);
    assert!(deferred.selected_indexes.contains(&0));
    assert_eq!(deferred.inventory_revision, original.inventory_revision);
    assert_eq!(deferred.selection_revision, original.selection_revision);
    assert_eq!(
        deferred_import.confirmed_inventory_revision,
        original_import.confirmed_inventory_revision
    );
    assert_eq!(
        deferred_import.confirmed_selection_revision,
        original_import.confirmed_selection_revision
    );
    assert!(deferred.manual_refresh);
    assert!(
        deferred
            .next_refresh_at
            .is_some_and(|retry_at| retry_at > now)
    );
    assert!(deferred_import.next_retry_at > now);
    assert!(!task_has_action_due(&deferred, now));
    assert!(backend.music_batches().is_empty());

    let restored_remote =
        remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    backend.set_visible_candidate(restored_remote);
    manager
        .apply_action(
            fixture.upload.user_id,
            fixture.upload.user_id,
            confirmed.card_message_id.unwrap(),
            &task_id,
            confirmed.inventory_revision,
            TorrentAction::Refresh,
        )
        .expect("explicitly retry the read-only refresh");
    manager.process_task_once_for_test(&task_id).await;
    let revalidated = manager.store.task(&task_id).unwrap().unwrap();
    assert_eq!(revalidated.inventory_revision, original.inventory_revision);
    assert_eq!(revalidated.selection_revision, original.selection_revision);
    assert_eq!(
        revalidated
            .imports
            .get(&0)
            .unwrap()
            .confirmed_selection_revision,
        original_import.confirmed_selection_revision
    );

    manager.process_task_once_for_test(&task_id).await;
    assert_eq!(backend.music_batches().len(), 1);
}

#[tokio::test]
async fn existing_refresh_with_a_different_identity_stays_invalid() {
    let fixture = Fixture::new();
    let (backend, manager, task_id, confirmed) = create_confirmed_existing_task(&fixture).await;
    let other_bytes = alternate_single_file_torrent();
    let other_meta = parse_metainfo(&other_bytes, DEFAULT_MAX_TORRENT_BYTES, DEFAULT_MAX_FILES)
        .expect("parse another valid torrent");
    backend.set_visible_candidate(remote_snapshot(
        &other_meta,
        &fixture.source_root,
        &[1.0],
        1.0,
        0,
    ));
    manager
        .apply_action(
            fixture.upload.user_id,
            fixture.upload.user_id,
            confirmed.card_message_id.unwrap(),
            &task_id,
            confirmed.inventory_revision,
            TorrentAction::Refresh,
        )
        .expect("request identity revalidation");
    manager.process_task_once_for_test(&task_id).await;

    let rejected = manager.store.task(&task_id).unwrap().unwrap();
    assert_eq!(rejected.phase, TorrentPhase::Invalid);
    assert!(rejected.selected_indexes.is_empty());
    assert!(backend.music_batches().is_empty());
}
