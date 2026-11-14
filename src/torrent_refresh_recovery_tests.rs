use super::*;
use crate::torrent_types::{
    CompletedTorrentFile, SubmitResult, TorrentBackend, TorrentMusicJobRef, TorrentMusicJobStatus,
    TorrentRemoteSnapshot, TorrentSnapshot, TorrentUpload,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct RecordingBackend {
    inner: Arc<TestBackend>,
    fail_lookup_once: AtomicBool,
    lookup_call_count: AtomicUsize,
    status_call_count: AtomicUsize,
    fail_second_status_call: AtomicBool,
    complete_later_pending: AtomicBool,
    status_chunk_sizes: Mutex<Vec<usize>>,
}

impl RecordingBackend {
    fn new(inner: Arc<TestBackend>) -> Self {
        Self {
            inner,
            fail_lookup_once: AtomicBool::new(false),
            lookup_call_count: AtomicUsize::new(0),
            status_call_count: AtomicUsize::new(0),
            fail_second_status_call: AtomicBool::new(false),
            complete_later_pending: AtomicBool::new(false),
            status_chunk_sizes: Mutex::new(Vec::new()),
        }
    }

    fn fail_one_lookup(&self) {
        self.fail_lookup_once.store(true, Ordering::SeqCst);
    }

    fn lookup_call_count(&self) -> usize {
        self.lookup_call_count.load(Ordering::SeqCst)
    }

    fn fail_second_status_call(&self) {
        self.fail_second_status_call.store(true, Ordering::SeqCst);
    }

    fn complete_later_pending(&self) {
        self.complete_later_pending.store(true, Ordering::SeqCst);
    }
}

impl TorrentBackend for RecordingBackend {
    fn download_torrent(
        &self,
        upload: TorrentUpload,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<u8>> {
        self.inner.download_torrent(upload)
    }

    fn lookup(
        &self,
        identity: TorrentIdentity,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentRemoteSnapshot>> {
        self.lookup_call_count.fetch_add(1, Ordering::SeqCst);
        if self.fail_lookup_once.swap(false, Ordering::SeqCst) {
            return Box::pin(async { Err(anyhow::anyhow!("temporary lookup failure")) });
        }
        self.inner.lookup(identity)
    }

    fn submit(
        &self,
        raw_torrent: Vec<u8>,
        file_name: String,
    ) -> crate::torrent_types::TorrentFuture<'_, SubmitResult> {
        self.inner.submit(raw_torrent, file_name)
    }

    fn export_metainfo(&self, hash: String) -> crate::torrent_types::TorrentFuture<'_, Vec<u8>> {
        self.inner.export_metainfo(hash)
    }

    fn refresh(
        &self,
        hash: String,
    ) -> crate::torrent_types::TorrentFuture<'_, TorrentRemoteSnapshot> {
        self.inner.refresh(hash)
    }

    fn send_card(&self, snapshot: TorrentSnapshot) -> crate::torrent_types::TorrentFuture<'_, i64> {
        self.inner.send_card(snapshot)
    }

    fn update_card(
        &self,
        snapshot: TorrentSnapshot,
    ) -> crate::torrent_types::TorrentFuture<'_, ()> {
        self.inner.update_card(snapshot)
    }

    fn enqueue_music(
        &self,
        chat_id: i64,
        user_id: i64,
        batch_id: String,
        files: Vec<CompletedTorrentFile>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        self.inner.enqueue_music(chat_id, user_id, batch_id, files)
    }

    fn music_status(
        &self,
        chat_id: i64,
        job_ids: Vec<String>,
    ) -> crate::torrent_types::TorrentFuture<'_, Vec<TorrentMusicJobStatus>> {
        let call = self.status_call_count.fetch_add(1, Ordering::SeqCst) + 1;
        self.status_chunk_sizes.lock().unwrap().push(job_ids.len());
        if call == 2 && self.fail_second_status_call.swap(false, Ordering::SeqCst) {
            return Box::pin(async { Err(anyhow::anyhow!("temporary status failure")) });
        }
        if self.complete_later_pending.load(Ordering::SeqCst) {
            let statuses = job_ids
                .iter()
                .filter_map(|job_id| {
                    let index = job_id.strip_prefix("status-job-")?.parse::<u32>().ok()?;
                    (index >= 1_000).then(|| TorrentMusicJobStatus {
                        job_id: job_id.clone(),
                        source_key: format!("status-source-{index}"),
                        stage: TorrentMusicStage::Complete,
                        error: None,
                    })
                })
                .collect();
            return Box::pin(async move { Ok(statuses) });
        }
        self.inner.music_status(chat_id, job_ids)
    }
}

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

async fn create_confirmed_uploaded_task(
    fixture: &Fixture,
) -> (
    Arc<TestBackend>,
    Arc<TorrentManager>,
    String,
    TorrentSnapshot,
) {
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    let backend = fixture.backend(vec![remote], None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let queued = manager
        .enqueue(fixture.upload.clone())
        .expect("enqueue uploaded torrent");
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    let verified = manager
        .snapshot(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            &queued.task_id,
        )
        .expect("read verified uploaded task");
    let selected = manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            verified.card_message_id.expect("card delivered"),
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
            fixture.upload.chat_id,
            fixture.upload.user_id,
            selected.card_message_id.expect("card delivered"),
            &queued.task_id,
            selected.selection_revision,
            TorrentAction::ImportSelected,
        )
        .expect("confirm selection");
    (backend, manager, queued.task_id, confirmed)
}

#[tokio::test]
async fn matching_remote_with_incomplete_inventory_backs_off_without_submission() {
    let fixture = Fixture::new();
    let mut incomplete =
        remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    incomplete.files.clear();
    let backend = fixture.backend(vec![incomplete], None);
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let queued = manager
        .enqueue(fixture.upload.clone())
        .expect("enqueue uploaded torrent");
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;

    let now = unix_seconds();
    let deferred = manager.store.task(&queued.task_id).unwrap().unwrap();
    assert!(deferred.metainfo.is_some());
    assert!(deferred.remote.is_none());
    assert!(deferred.manual_refresh);
    assert!(deferred.next_action_at > now);
    assert!(
        deferred
            .next_refresh_at
            .is_some_and(|retry_at| retry_at > now)
    );
    assert!(
        deferred
            .error
            .as_deref()
            .is_some_and(|error| error.contains("verifiable file inventory"))
    );
    assert!(!task_has_action_due(&deferred, now));
    assert_eq!(backend.submitted_count(), 0);

    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    backend.set_visible_candidate(remote);
    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            deferred.card_message_id.expect("card delivered"),
            &queued.task_id,
            deferred.inventory_revision,
            TorrentAction::Refresh,
        )
        .expect("explicitly retry attachment");
    manager.process_task_once_for_test(&queued.task_id).await;
    let retried = manager.store.task(&queued.task_id).unwrap().unwrap();
    assert!(retried.remote.is_some());
    assert!(!retried.manual_refresh);
    assert_eq!(backend.submitted_count(), 0);
}

#[tokio::test]
async fn failed_inventory_refresh_blocks_import_until_explicit_revalidation() {
    let fixture = Fixture::new();
    let (backend, manager, task_id, confirmed) = create_confirmed_uploaded_task(&fixture).await;
    let original = manager.store.task(&task_id).unwrap().unwrap();
    let original_import = original.imports.get(&0).unwrap().clone();
    let mut incomplete =
        remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    incomplete.files.clear();
    backend.set_visible_candidate(incomplete);

    manager
        .apply_action(
            fixture.upload.chat_id,
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
    assert!(deferred.manual_refresh);
    assert!(deferred.next_action_at > now);
    assert!(
        deferred
            .next_refresh_at
            .is_some_and(|retry_at| retry_at > now)
    );
    assert_eq!(deferred.inventory_revision, original.inventory_revision);
    assert_eq!(deferred.selection_revision, original.selection_revision);
    assert_eq!(
        deferred.remote, original.remote,
        "unverified inventory must not replace the last verified snapshot"
    );
    assert_eq!(
        deferred
            .imports
            .get(&0)
            .unwrap()
            .confirmed_inventory_revision,
        original_import.confirmed_inventory_revision
    );
    assert_eq!(
        deferred
            .imports
            .get(&0)
            .unwrap()
            .confirmed_selection_revision,
        original_import.confirmed_selection_revision
    );
    assert!(!task_has_action_due(&deferred, now));
    manager.process_task_once_for_test(&task_id).await;
    assert!(
        backend.music_batches().is_empty(),
        "stale inventory cannot admit the confirmed import"
    );

    backend.state.lock().unwrap().visible_candidates.clear();
    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            confirmed.card_message_id.unwrap(),
            &task_id,
            confirmed.inventory_revision,
            TorrentAction::Refresh,
        )
        .expect("request another read-only refresh");
    manager.process_task_once_for_test(&task_id).await;
    let unavailable = manager.store.task(&task_id).unwrap().unwrap();
    assert!(unavailable.manual_refresh);
    assert!(unavailable.next_action_at > unix_seconds());
    assert_eq!(unavailable.remote, original.remote);
    assert_eq!(
        unavailable
            .imports
            .get(&0)
            .unwrap()
            .confirmed_inventory_revision,
        original_import.confirmed_inventory_revision
    );
    assert!(backend.music_batches().is_empty());

    let valid = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    backend.set_visible_candidate(valid);
    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            confirmed.card_message_id.unwrap(),
            &task_id,
            confirmed.inventory_revision,
            TorrentAction::Refresh,
        )
        .expect("explicitly retry the read-only refresh");
    manager.process_task_once_for_test(&task_id).await;
    manager.process_task_once_for_test(&task_id).await;
    assert_eq!(backend.music_batches().len(), 1);
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

#[tokio::test]
async fn music_status_requests_are_chunked_and_pruned_terminal_jobs_are_preserved() {
    let fixture = Fixture::new();
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[1.0, 1.0], 1.0, 0);
    let inner = fixture.backend(vec![remote.clone()], None);
    inner.configure_magnet(
        fixture.source_root.clone(),
        Some(fixture.raw_torrent.clone()),
        0,
    );
    let backend = Arc::new(RecordingBackend::new(inner.clone()));
    backend.fail_second_status_call();
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let queued = manager
        .adopt_existing(
            existing_source(&fixture, remote.hash, fixture.upload.user_id),
            "catalog:large-status-set".to_string(),
        )
        .await
        .expect("adopt existing task");
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;

    manager
        .store
        .transact(|state| {
            let task = state.tasks.get_mut(&queued.task_id).unwrap();
            for index in 0..1_001_u32 {
                let source_key = format!("status-source-{index}");
                let stage = match index {
                    0 => TorrentMusicStage::Complete,
                    1 => TorrentMusicStage::Failed,
                    _ => TorrentMusicStage::Pending,
                };
                task.imports.insert(
                    index,
                    StoredImport {
                        source_key: source_key.clone(),
                        confirmed_inventory_revision: task.inventory_revision,
                        confirmed_selection_revision: task.selection_revision,
                        enqueue_intent: false,
                        batch_id: None,
                        music_job: Some(TorrentMusicJobRef {
                            source_key,
                            job_id: format!("status-job-{index}"),
                        }),
                        music_stage: Some(stage),
                        error: None,
                        attempts: 0,
                        next_retry_at: unix_seconds(),
                    },
                );
            }
            task.next_music_status_at = Some(unix_seconds());
            Ok(((), true))
        })
        .expect("insert historical Music references");

    manager.refresh_music_status_for_test(&queued.task_id).await;

    assert_eq!(*backend.status_chunk_sizes.lock().unwrap(), vec![1_000, 1]);
    let refreshed = manager.store.task(&queued.task_id).unwrap().unwrap();
    assert_eq!(
        refreshed.imports.get(&0).unwrap().music_stage,
        Some(TorrentMusicStage::Complete),
        "a pruned terminal job must keep its cached terminal stage"
    );
    assert_eq!(
        refreshed.imports.get(&1).unwrap().music_stage,
        Some(TorrentMusicStage::Failed)
    );
    assert_eq!(
        refreshed.imports.get(&1_000).unwrap().music_stage,
        Some(TorrentMusicStage::Pending),
        "the final reference in a failed later chunk must remain pending"
    );
    assert!(refreshed.next_music_status_at.is_some());

    backend.complete_later_pending();
    manager.refresh_music_status_for_test(&queued.task_id).await;
    let completed_later = manager.store.task(&queued.task_id).unwrap().unwrap();
    assert_eq!(
        completed_later.imports.get(&0).unwrap().music_stage,
        Some(TorrentMusicStage::Complete),
        "a pruned terminal job keeps its cached state after later polling"
    );
    assert_eq!(
        completed_later.imports.get(&1).unwrap().music_stage,
        Some(TorrentMusicStage::Failed)
    );
    assert_eq!(
        completed_later.imports.get(&1_000).unwrap().music_stage,
        Some(TorrentMusicStage::Complete),
        "a later chunk can complete on a subsequent bounded poll"
    );
    assert_eq!(
        *backend.status_chunk_sizes.lock().unwrap(),
        vec![1_000, 1, 1_000, 1]
    );
}

#[tokio::test]
async fn detached_manual_refresh_reconciles_and_backs_off_after_lookup_failure() {
    let fixture = Fixture::new();
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[0.5, 1.0], 0.7, 2);
    let inner = fixture.backend(vec![remote], None);
    let backend = Arc::new(RecordingBackend::new(inner.clone()));
    backend.fail_one_lookup();
    let manager = TorrentManager::open(fixture.settings(), backend).expect("open manager");
    let queued = manager
        .enqueue(fixture.upload.clone())
        .expect("enqueue uploaded torrent");
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    manager
        .store
        .transact(|state| {
            let task = state.tasks.get_mut(&queued.task_id).unwrap();
            assert!(task.metainfo.is_some());
            assert!(task.remote.is_none());
            task.manual_refresh = true;
            task.next_refresh_at = Some(unix_seconds());
            task.next_action_at = unix_seconds();
            Ok(((), true))
        })
        .expect("request refresh on a detached task");

    manager.process_task_once_for_test(&queued.task_id).await;
    let now = unix_seconds();
    let deferred = manager.store.task(&queued.task_id).unwrap().unwrap();
    assert!(!deferred.manual_refresh);
    assert!(deferred.next_refresh_at.is_none());
    assert!(deferred.next_action_at > now);
    assert!(!task_has_action_due(&deferred, now));
    assert_eq!(inner.submitted_count(), 0);

    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            deferred.card_message_id.expect("card delivered"),
            &queued.task_id,
            deferred.inventory_revision,
            TorrentAction::Refresh,
        )
        .expect("explicitly retry after the scheduled lookup backoff");
    manager.process_task_once_for_test(&queued.task_id).await;
    let reconciled = manager.store.task(&queued.task_id).unwrap().unwrap();
    assert!(reconciled.remote.is_some());
    assert!(!reconciled.manual_refresh);
    assert_eq!(inner.submitted_count(), 0);
}

#[tokio::test]
async fn ambiguous_lookup_backs_off_and_explicit_refresh_recovers_without_submission() {
    let fixture = Fixture::new();
    let remote = remote_snapshot(&fixture.metainfo, &fixture.source_root, &[0.5, 1.0], 0.7, 2);
    let inner = fixture.backend(vec![remote.clone(), remote.clone()], None);
    let backend = Arc::new(RecordingBackend::new(inner.clone()));
    let manager = TorrentManager::open(fixture.settings(), backend.clone()).expect("open manager");
    let queued = manager
        .enqueue(fixture.upload.clone())
        .expect("enqueue uploaded torrent");
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;
    manager.process_task_once_for_test(&queued.task_id).await;

    let now = unix_seconds();
    let ambiguous = manager.store.task(&queued.task_id).unwrap().unwrap();
    assert_eq!(ambiguous.phase, TorrentPhase::Ambiguous);
    assert!(ambiguous.remote.is_none());
    assert!(ambiguous.manual_refresh);
    assert!(ambiguous.next_action_at > now);
    assert!(
        ambiguous
            .next_refresh_at
            .is_some_and(|retry_at| retry_at > now)
    );
    assert_eq!(backend.lookup_call_count(), 1);
    assert_eq!(inner.submitted_count(), 0);

    manager.process_task_once_for_test(&queued.task_id).await;
    assert_eq!(
        backend.lookup_call_count(),
        1,
        "a card redraw must not bypass the reconciliation retry deadline"
    );
    manager
        .store
        .transact(|state| {
            let task = state.tasks.get_mut(&queued.task_id).unwrap();
            task.card_render_pending = true;
            task.next_card_update_at = unix_seconds().saturating_sub(1);
            Ok(((), true))
        })
        .expect("force a due card redraw");
    manager.process_task_once_for_test(&queued.task_id).await;
    assert_eq!(
        backend.lookup_call_count(),
        1,
        "a due card redraw must not bypass the reconciliation retry deadline"
    );

    inner.state.lock().unwrap().visible_candidates = vec![remote];
    manager
        .apply_action(
            fixture.upload.chat_id,
            fixture.upload.user_id,
            ambiguous.card_message_id.expect("card delivered"),
            &queued.task_id,
            ambiguous.inventory_revision,
            TorrentAction::Refresh,
        )
        .expect("explicitly request a fresh reconciliation");
    manager.process_task_once_for_test(&queued.task_id).await;

    let recovered = manager.store.task(&queued.task_id).unwrap().unwrap();
    assert!(recovered.remote.is_some());
    assert_ne!(recovered.phase, TorrentPhase::Ambiguous);
    assert!(!recovered.manual_refresh);
    assert_eq!(backend.lookup_call_count(), 2);
    assert_eq!(inner.submitted_count(), 0);
}
