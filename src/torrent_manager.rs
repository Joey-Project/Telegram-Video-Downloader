//! Durable torrent task state, selection actions, qBittorrent reconciliation, and Music handoff.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use rustix::fs::FlockOperation;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, Notify, watch};
use tokio::task::JoinHandle;

use crate::file_provider::{QueueFileProvider, platform_queue_file_provider};
use crate::torrent_metainfo::parse_metainfo;
use crate::torrent_types::{
    CompletedTorrentFile, DEFAULT_MAX_FILES, DEFAULT_MAX_TORRENT_BYTES,
    MAX_TORRENT_COMPONENT_BYTES, MAX_TORRENT_PATH_BYTES, MetainfoFile, RemoteTorrentFile,
    SubmitDisposition, SubmitResult, TorrentAction, TorrentBackend, TorrentEnqueueResult,
    TorrentExisting, TorrentFileSnapshot, TorrentFolder, TorrentMagnet, TorrentMetainfo,
    TorrentMusicJobRef, TorrentMusicStage, TorrentPhase, TorrentRemoteSnapshot, TorrentSettings,
    TorrentSnapshot, TorrentUpload, WIDGET_PAGE_SIZE, supports_music_import,
};

const STATE_FILE_NAME: &str = "state.json";
const OWNER_LOCK_FILE_NAME: &str = "owner.lock";
const METAINFO_DIR_NAME: &str = "metainfo";
const STATE_VERSION: u32 = 1;
const MAX_STATE_BYTES: usize = 64 * 1024 * 1024;
const MAX_TASKS: usize = 1_000;
const MAX_TOTAL_FILE_RECORDS: usize = 100_000;
const MAX_ERROR_BYTES: usize = 256;
const MAX_REOPEN_REQUEST_KEYS: usize = 64;
const MAX_ACTIVE_WORKERS: usize = 16;
const MAX_MUSIC_BATCH_FILES: usize = 16;
const MUSIC_STATUS_POLL_SECONDS: u64 = 30;
const BASE_RETRY_SECONDS: u64 = 5;
const MAX_RETRY_SECONDS: u64 = 5 * 60;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct TorrentManager {
    settings: TorrentSettings,
    store: Arc<TorrentStore>,
    backend: Arc<dyn TorrentBackend>,
    notify: Arc<Notify>,
    active_workers: Arc<Mutex<HashSet<String>>>,
}

impl std::fmt::Debug for TorrentManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TorrentManager")
            .field("settings", &self.settings)
            .field("state_dir", &self.store.state_dir)
            .finish_non_exhaustive()
    }
}

impl TorrentManager {
    pub fn open(settings: TorrentSettings, backend: Arc<dyn TorrentBackend>) -> Result<Arc<Self>> {
        settings.validate()?;
        ensure!(settings.enabled, "torrent manager is disabled");
        let provider = platform_queue_file_provider();
        let store = TorrentStore::open(&settings, provider)?;
        Ok(Arc::new(Self {
            settings,
            store: Arc::new(store),
            backend,
            notify: Arc::new(Notify::new()),
            active_workers: Arc::new(Mutex::new(HashSet::new())),
        }))
    }

    /// Starts the background worker. Telegram handlers should call only durable state methods.
    pub fn start(self: &Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(manager.settings.poll_seconds));
            loop {
                manager.dispatch_ready().await;
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = manager.notify.notified() => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                }
                if *shutdown.borrow() {
                    break;
                }
            }
        })
    }

    pub fn enqueue(&self, upload: TorrentUpload) -> Result<TorrentEnqueueResult> {
        validate_upload(&upload, &self.settings)?;
        let origin_key = upload_origin_key(&upload);
        self.enqueue_source(origin_key, Some(upload), None)
    }

    pub fn enqueue_magnet(&self, mut magnet: TorrentMagnet) -> Result<TorrentEnqueueResult> {
        ensure!(
            magnet.update_id >= 0
                && magnet.chat_id > 0
                && magnet.user_id == magnet.chat_id
                && magnet.message_id > 0,
            "magnet message identifiers are invalid"
        );
        magnet.uri = crate::torrent_magnet::parse_magnet_uri(&magnet.uri)?.normalized_uri;
        let origin_key = format!(
            "{}:{}:{}:{}:magnet",
            magnet.chat_id, magnet.user_id, magnet.message_id, magnet.update_id
        );
        self.enqueue_source(origin_key, None, Some(magnet))
    }

    /// Adopt one exact qBittorrent hash for a Telegram catalog request.
    pub async fn adopt_existing(
        &self,
        mut source: TorrentExisting,
        request_key: String,
    ) -> Result<TorrentEnqueueResult> {
        validate_existing_source(&source)?;
        validate_request_key(&request_key)?;
        source.hash.make_ascii_lowercase();
        let requested_hash = source.hash.clone();
        let prepared_id = self.prepare_existing_revalidation(&source)?;
        let remote = match self.backend.refresh(source.hash.clone()).await {
            Ok(remote) => remote,
            Err(error) => {
                if let Some(task_id) = prepared_id.as_deref() {
                    self.defer_existing_revalidation(task_id);
                }
                return Err(error);
            }
        };
        if !remote.hash.eq_ignore_ascii_case(&source.hash) {
            if let Some(task_id) = prepared_id.as_deref() {
                self.defer_existing_revalidation(task_id);
            }
            bail!("qBittorrent returned a different torrent hash");
        }

        let (matched_id, created_result) = self.store.transact(|state| {
            let matching_id = state
                .tasks
                .values()
                .find(|task| task_matches_existing_hash(task, &requested_hash))
                .map(|task| task.task_id.clone());
            if let Some(task_id) = matching_id {
                let task = state.tasks.get_mut(&task_id).expect("matched task exists");
                validate_owner(task, source.chat_id, source.user_id)?;
                if prepared_id.as_deref() != Some(task_id.as_str()) {
                    prepare_task_for_existing_revalidation(task);
                }
                return Ok((Some(task_id), None));
            }

            ensure!(
                state.tasks.len() < MAX_TASKS,
                "torrent task capacity is full"
            );
            let origin_key = existing_origin_key(&source);
            let task_id = task_id_for_origin(&origin_key);
            let now = unix_seconds();
            let mut pending_remote = remote.clone();
            pending_remote.files.clear();
            let task = StoredTorrent {
                task_id: task_id.clone(),
                origin_key: origin_key.clone(),
                upload: None,
                magnet: None,
                existing: Some(source.clone()),
                magnet_save_path: None,
                raw_sha256: None,
                metainfo: None,
                remote: Some(pending_remote),
                phase: TorrentPhase::DownloadingMetainfo,
                submit_intent: false,
                submit_result: None,
                invalid_after_reconcile: false,
                selected_indexes: BTreeSet::new(),
                imports: BTreeMap::new(),
                retry_job_ids: Vec::new(),
                inventory_revision: 0,
                selection_revision: 0,
                render_revision: 1,
                cursor_folder_id: 0,
                cursor_page: 0,
                review_selection: false,
                card_generation: 0,
                card_request_keys: vec![request_key.clone()],
                card_send_state: CardSendState::Requested,
                card_message_id: None,
                card_render_pending: false,
                next_card_update_at: 0,
                manual_refresh: false,
                next_action_at: now,
                next_refresh_at: None,
                next_music_status_at: None,
                attempts: 0,
                created_at: now,
                updated_at: now,
                error: None,
            };
            state.origins.insert(origin_key, task_id.clone());
            state.tasks.insert(task_id.clone(), task);
            let task = state.tasks.get(&task_id).expect("task was inserted");
            Ok((
                None,
                Some(TorrentEnqueueResult {
                    task_id,
                    created: true,
                    snapshot: task_snapshot(task),
                }),
            ))
        })?;
        if let Some(task_id) = matched_id {
            let stored = self
                .store
                .task(&task_id)?
                .ok_or_else(|| anyhow!("matched torrent task disappeared"))?;
            if stored.metainfo.is_some() {
                if let Err(error) = self.attach_remote(&task_id, remote.clone()) {
                    self.defer_existing_revalidation(&task_id);
                    return Err(error);
                }
                // The fresh snapshot was validated and attached before opening a new card.
            } else if stored.existing.is_some() {
                let mut pending_remote = remote.clone();
                pending_remote.files.clear();
                self.store.transact(|state| {
                    let task = state
                        .tasks
                        .get_mut(&task_id)
                        .context("matched torrent task disappeared")?;
                    task.remote = Some(pending_remote.clone());
                    task.phase = TorrentPhase::DownloadingMetainfo;
                    task.manual_refresh = false;
                    task.next_action_at = unix_seconds();
                    task.error = None;
                    touch(task);
                    Ok(((), true))
                })?;
            } else {
                self.defer_existing_revalidation(&task_id);
                bail!("matched torrent task has no verified metadata to attach the refresh");
            }

            self.store.transact(|state| {
                let task = state
                    .tasks
                    .get_mut(&task_id)
                    .ok_or_else(|| anyhow!("matched torrent task disappeared"))?;
                if !task.card_request_keys.iter().any(|key| key == &request_key) {
                    task.card_request_keys.push(request_key.clone());
                    if task.card_request_keys.len() > MAX_REOPEN_REQUEST_KEYS {
                        let excess = task.card_request_keys.len() - MAX_REOPEN_REQUEST_KEYS;
                        task.card_request_keys.drain(0..excess);
                    }
                    task.card_generation = task.card_generation.saturating_add(1);
                    task.card_message_id = None;
                    task.card_send_state = CardSendState::Requested;
                    task.card_render_pending = false;
                    task.next_action_at = unix_seconds();
                    touch(task);
                }
                Ok(((), true))
            })?;
            self.notify.notify_one();
            let task = self
                .store
                .task(&task_id)?
                .ok_or_else(|| anyhow!("matched torrent task disappeared"))?;
            return Ok(TorrentEnqueueResult {
                task_id,
                created: false,
                snapshot: task_snapshot(&task),
            });
        }
        self.notify.notify_one();
        Ok(created_result.expect("new task result was created"))
    }

    fn prepare_existing_revalidation(&self, source: &TorrentExisting) -> Result<Option<String>> {
        let state = self.store.snapshot()?;
        let Some(task_id) = state
            .tasks
            .values()
            .find(|task| task_matches_existing_hash(task, &source.hash))
            .map(|task| task.task_id.clone())
        else {
            return Ok(None);
        };
        self.store.transact(|state| {
            let task = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("matched torrent task disappeared"))?;
            validate_owner(task, source.chat_id, source.user_id)?;
            prepare_task_for_existing_revalidation(task);
            Ok(((), true))
        })?;
        self.notify.notify_one();
        Ok(Some(task_id))
    }

    fn defer_existing_revalidation(&self, task_id: &str) {
        let _ = self.store.transact(|state| {
            let task = state
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            task.manual_refresh = false;
            let retry_at = unix_seconds().saturating_add(self.settings.poll_seconds);
            task.inventory_revision = task.inventory_revision.saturating_add(1).max(1);
            task.selection_revision = task.selection_revision.saturating_add(1).max(1);
            task.next_refresh_at = Some(retry_at);
            task.next_action_at = retry_at;
            task.error =
                Some("The fresh qBittorrent inventory could not be attached safely.".to_string());
            touch(task);
            Ok(((), true))
        });
    }

    fn enqueue_source(
        &self,
        origin_key: String,
        upload: Option<TorrentUpload>,
        magnet: Option<TorrentMagnet>,
    ) -> Result<TorrentEnqueueResult> {
        let task_id = task_id_for_origin(&origin_key);
        let snapshot = self.store.transact(|state| {
            if let Some(existing) = state.tasks.get(&task_id) {
                ensure!(
                    existing.origin_key == origin_key
                        && existing.upload == upload
                        && existing.magnet == magnet,
                    "conflicting metadata was supplied for a torrent upload"
                );
                return Ok((task_snapshot(existing), false));
            }
            ensure!(
                state.tasks.len() < MAX_TASKS,
                "torrent task capacity is full"
            );
            let now = unix_seconds();
            let task = StoredTorrent {
                task_id: task_id.clone(),
                origin_key: origin_key.clone(),
                upload: upload.clone(),
                magnet: magnet.clone(),
                existing: None,
                magnet_save_path: None,
                raw_sha256: None,
                metainfo: None,
                remote: None,
                phase: TorrentPhase::Queued,
                submit_intent: false,
                submit_result: None,
                invalid_after_reconcile: false,
                selected_indexes: BTreeSet::new(),
                imports: BTreeMap::new(),
                retry_job_ids: Vec::new(),
                inventory_revision: 0,
                selection_revision: 0,
                render_revision: 1,
                cursor_folder_id: 0,
                cursor_page: 0,
                review_selection: false,
                card_generation: 0,
                card_request_keys: Vec::new(),
                card_send_state: CardSendState::Requested,
                card_message_id: None,
                card_render_pending: false,
                next_card_update_at: 0,
                manual_refresh: false,
                next_action_at: now,
                next_refresh_at: None,
                next_music_status_at: None,
                attempts: 0,
                created_at: now,
                updated_at: now,
                error: None,
            };
            state.origins.insert(origin_key, task_id.clone());
            state.tasks.insert(task_id.clone(), task);
            let task = state.tasks.get(&task_id).expect("task was inserted");
            Ok((task_snapshot(task), true))
        })?;
        let (snapshot, created) = snapshot;
        if created {
            self.notify.notify_one();
        }
        Ok(TorrentEnqueueResult {
            task_id,
            created,
            snapshot,
        })
    }

    /// Reissue the task card after an explicit `/torrents` recovery command.
    /// Replayed Telegram updates with the same request key do not open another generation.
    pub fn reopen_card(
        &self,
        chat_id: i64,
        user_id: i64,
        task_id: &str,
        request_key: String,
    ) -> Result<TorrentSnapshot> {
        validate_request_key(&request_key)?;
        let snapshot = self.store.transact(|state| {
            let task = state
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| anyhow!("torrent task was not found"))?;
            validate_owner(task, chat_id, user_id)?;
            if task.card_request_keys.iter().any(|key| key == &request_key) {
                return Ok((task_snapshot(task), false));
            }
            task.card_request_keys.push(request_key);
            if task.card_request_keys.len() > MAX_REOPEN_REQUEST_KEYS {
                let excess = task.card_request_keys.len() - MAX_REOPEN_REQUEST_KEYS;
                task.card_request_keys.drain(0..excess);
            }
            task.card_generation = task.card_generation.saturating_add(1);
            task.card_message_id = None;
            task.card_send_state = CardSendState::Requested;
            task.card_render_pending = false;
            task.next_action_at = unix_seconds();
            touch(task);
            Ok((task_snapshot(task), true))
        })?;
        let (snapshot, changed) = snapshot;
        if changed {
            self.notify.notify_one();
        }
        Ok(snapshot)
    }

    pub fn apply_action(
        &self,
        chat_id: i64,
        user_id: i64,
        card_message_id: i64,
        task_id: &str,
        inventory_revision: u64,
        action: TorrentAction,
    ) -> Result<TorrentSnapshot> {
        let snapshot = self.store.transact(|state| {
            let task = state
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| anyhow!("torrent task was not found"))?;
            validate_owner(task, chat_id, user_id)?;
            ensure!(
                task.phase != TorrentPhase::Invalid,
                "torrent task is no longer available"
            );
            ensure!(!task.manual_refresh, "torrent inventory is being refreshed");
            ensure!(
                task.card_message_id == Some(card_message_id),
                "torrent card is no longer current"
            );
            if matches!(
                &action,
                TorrentAction::ImportSelected | TorrentAction::ReviewSelection { .. }
            ) {
                ensure!(
                    task.selection_revision == inventory_revision,
                    "torrent selection changed; confirm the selected files again"
                );
            } else if !matches!(&action, &TorrentAction::Refresh) {
                ensure!(
                    task.inventory_revision == inventory_revision,
                    "torrent file list changed; refresh and confirm again"
                );
            }
            match action {
                TorrentAction::Refresh => {
                    task.manual_refresh = true;
                    task.next_action_at = unix_seconds();
                }
                TorrentAction::Browse { folder_id, page } => {
                    validate_browse(task, folder_id, page)?;
                    task.cursor_folder_id = folder_id;
                    task.cursor_page = page;
                    task.review_selection = false;
                }
                TorrentAction::ReviewSelection { page } => {
                    ensure!(
                        !task.selected_indexes.is_empty(),
                        "select music before reviewing"
                    );
                    ensure!(
                        (page as usize) < task.selected_indexes.len().div_ceil(WIDGET_PAGE_SIZE),
                        "review page is invalid"
                    );
                    task.review_selection = true;
                    task.cursor_page = page;
                }
                TorrentAction::SetFile { index, selected } => {
                    exit_review(task);
                    let file = task_file(task, index)?;
                    ensure!(
                        file.supported_music,
                        "this torrent file type cannot be imported into Music"
                    );
                    let changed = if selected {
                        task.selected_indexes.insert(index)
                    } else {
                        task.selected_indexes.remove(&index)
                    };
                    if changed {
                        task.selection_revision = task.selection_revision.saturating_add(1);
                    }
                    if !selected {
                        remove_unqueued_import(task, index);
                    }
                }
                TorrentAction::SetFolder {
                    folder_id,
                    selected,
                } => {
                    exit_review(task);
                    let folder = task_folders(task)?
                        .into_iter()
                        .find(|folder| folder.id == folder_id)
                        .ok_or_else(|| anyhow!("torrent folder selection is invalid"))?;
                    let indexes = indexes_in_folder(task, &folder.relative_path)?;
                    let previous = task.selected_indexes.clone();
                    for index in indexes {
                        let file = task_file(task, index)?;
                        if !file.supported_music {
                            continue;
                        }
                        if selected {
                            task.selected_indexes.insert(index);
                        } else {
                            task.selected_indexes.remove(&index);
                            remove_unqueued_import(task, index);
                        }
                    }
                    if previous != task.selected_indexes {
                        task.selection_revision = task.selection_revision.saturating_add(1);
                    }
                }
                TorrentAction::ClearSelection => {
                    exit_review(task);
                    let changed = !task.selected_indexes.is_empty();
                    task.selected_indexes.clear();
                    task.imports
                        .retain(|_, request| request.music_job.is_some() || request.enqueue_intent);
                    if changed {
                        task.selection_revision = task.selection_revision.saturating_add(1);
                    }
                }
                TorrentAction::ImportSelected => {
                    exit_review(task);
                    ensure!(
                        task.metainfo.is_some() && task.remote.is_some(),
                        "torrent files are not ready for selection"
                    );
                    ensure!(
                        !task.selected_indexes.is_empty(),
                        "select at least one supported Music file"
                    );
                    let selection_revision = task.selection_revision;
                    let inventory_revision = task.inventory_revision;
                    let now = unix_seconds();
                    for index in task.selected_indexes.iter().copied().collect::<Vec<_>>() {
                        let file = task_file(task, index)?;
                        ensure!(
                            file.supported_music,
                            "this torrent file type cannot be imported into Music"
                        );
                        let source_key = torrent_music_source_key(task, index)?;
                        task.imports
                            .entry(index)
                            .and_modify(|request| {
                                if request.music_job.is_none() {
                                    request.confirmed_inventory_revision = inventory_revision;
                                    request.confirmed_selection_revision = selection_revision;
                                    request.error = None;
                                    request.next_retry_at = now;
                                }
                            })
                            .or_insert_with(|| StoredImport {
                                source_key,
                                confirmed_inventory_revision: inventory_revision,
                                confirmed_selection_revision: selection_revision,
                                enqueue_intent: false,
                                batch_id: None,
                                music_job: None,
                                music_stage: Some(TorrentMusicStage::Pending),
                                error: None,
                                attempts: 0,
                                next_retry_at: now,
                            });
                    }
                    task.next_action_at = unix_seconds();
                }
                TorrentAction::RetryImports => {
                    let retry = task
                        .imports
                        .values()
                        .filter(|request| {
                            request.music_job.is_some()
                                && matches!(
                                    request.music_stage,
                                    Some(TorrentMusicStage::Failed | TorrentMusicStage::Cancelled)
                                )
                        })
                        .filter_map(|request| {
                            request.music_job.as_ref().map(|job| job.job_id.clone())
                        })
                        .collect::<Vec<_>>();
                    ensure!(
                        !retry.is_empty(),
                        "there are no failed Music imports to retry"
                    );
                    task.retry_job_ids = retry;
                    task.next_action_at = unix_seconds();
                }
            }
            if task.remote.is_some() && task.metainfo.is_some() {
                task.phase = aggregate_phase(task);
            }
            touch(task);
            // Explicit navigation/selection should redraw promptly; background progress remains throttled.
            task.next_card_update_at = 0;
            Ok((task_snapshot(task), true))
        })?;
        let (snapshot, changed) = snapshot;
        if changed {
            self.notify.notify_one();
        }
        Ok(snapshot)
    }

    pub fn list(&self, chat_id: i64) -> Result<Vec<TorrentSnapshot>> {
        let state = self.store.snapshot()?;
        Ok(state
            .tasks
            .values()
            .filter(|task| task.source_ids().0 == chat_id)
            .map(task_snapshot)
            .collect())
    }

    #[cfg(test)]
    pub fn snapshot(&self, chat_id: i64, user_id: i64, task_id: &str) -> Result<TorrentSnapshot> {
        let task = self
            .store
            .task(task_id)?
            .ok_or_else(|| anyhow!("torrent task was not found"))?;
        validate_owner(&task, chat_id, user_id)?;
        Ok(task_snapshot(&task))
    }

    /// Deterministic worker step for integration tests; production work uses `start`.
    #[cfg(test)]
    pub(crate) async fn process_task_once_for_test(&self, task_id: &str) {
        self.process_task_once(task_id).await;
    }

    #[cfg(test)]
    pub(crate) async fn refresh_music_status_for_test(&self, task_id: &str) {
        if let Ok(Some(task)) = self.store.task(task_id) {
            self.refresh_music_status(task).await;
        }
    }

    async fn dispatch_ready(self: &Arc<Self>) {
        // File Provider coordination can block while materializing state files. Keep this
        // single discovery read off Tokio's executor threads as well as task processing.
        let store = Arc::clone(&self.store);
        let due =
            match tokio::task::spawn_blocking(move || store.ready_task_ids(unix_seconds())).await {
                Ok(Ok(task_ids)) => task_ids,
                Err(_) => return,
                Ok(Err(_)) => return,
            };
        for task_id in due.into_iter().take(MAX_ACTIVE_WORKERS) {
            let mut active = self.active_workers.lock().await;
            if !active.insert(task_id.clone()) {
                continue;
            }
            drop(active);
            let manager = self.clone();
            let worker_handle = Handle::current();
            tokio::spawn(async move {
                let worker = Arc::clone(&manager);
                let worker_task_id = task_id.clone();
                // Keep the active-worker slot until the blocking closure and its asynchronous
                // ports have both completed, even if a caller stops waiting for this dispatcher.
                let _ = tokio::task::spawn_blocking(move || {
                    worker_handle.block_on(worker.process_task_once(&worker_task_id));
                })
                .await;
                manager.active_workers.lock().await.remove(&task_id);
                manager.notify.notify_one();
            });
        }
    }

    async fn process_task_once(&self, task_id: &str) {
        let Ok(Some(task)) = self.store.task(task_id) else {
            return;
        };
        if task.card_send_state == CardSendState::Requested {
            self.send_task_card(task).await;
            return;
        }
        if task.card_render_pending && task.next_card_update_at <= unix_seconds() {
            self.update_task_card(task.clone()).await;
        }
        if task.phase == TorrentPhase::Invalid {
            return;
        }
        if task.existing.is_some() && task.metainfo.is_none() {
            self.process_existing(task).await;
            return;
        }
        if task.metainfo.is_none() {
            if task.magnet.is_some() {
                self.process_magnet(task).await;
            } else {
                self.download_and_parse(task).await;
            }
            return;
        }
        if task.manual_refresh || task.next_refresh_at.is_some_and(|at| at <= unix_seconds()) {
            if task.existing.is_some() {
                self.process_existing(task).await;
            } else {
                self.refresh_remote(task).await;
            }
            return;
        }
        if task.remote.is_none() {
            if task.existing.is_some() {
                self.process_existing(task).await;
            } else {
                self.reconcile_or_submit(task).await;
            }
            return;
        }
        if !task.retry_job_ids.is_empty() {
            self.retry_music_jobs(task).await;
            return;
        }
        if self.has_ready_imports(&task) {
            self.enqueue_ready_imports(task).await;
            return;
        }
        if task
            .next_music_status_at
            .is_some_and(|at| at <= unix_seconds())
            && has_pending_music_jobs(&task)
        {
            self.refresh_music_status(task).await;
        }
    }

    async fn send_task_card(&self, task: StoredTorrent) {
        let generation = task.card_generation;
        let snapshot = task_snapshot(&task);
        let started = self.store.transact(|state| {
            let task = state
                .tasks
                .get_mut(&task.task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            if task.card_generation != generation
                || task.card_send_state != CardSendState::Requested
            {
                return Ok((false, false));
            }
            task.card_send_state = CardSendState::InFlight;
            touch(task);
            Ok((true, true))
        });
        if !matches!(started, Ok((true, true))) {
            return;
        }
        let result = self.backend.send_card(snapshot.clone()).await;
        let _ = self.store.transact(|state| {
            let task = state
                .tasks
                .get_mut(&task.task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            if task.card_generation != generation || task.card_send_state != CardSendState::InFlight
            {
                return Ok(((), false));
            }
            let changed_during_send =
                task.render_revision > snapshot.render_revision.saturating_add(1);
            match result {
                Ok(message_id) if message_id > 0 => {
                    task.card_message_id = Some(message_id);
                    task.card_send_state = CardSendState::Delivered;
                    task.card_render_pending = changed_during_send;
                    task.next_card_update_at =
                        unix_seconds().saturating_add(self.settings.poll_seconds);
                }
                _ => {
                    task.card_send_state = CardSendState::Unknown;
                    task.card_message_id = None;
                    task.card_render_pending = false;
                }
            }
            touch(task);
            if task.card_send_state == CardSendState::Delivered {
                task.card_render_pending = changed_during_send;
            }
            Ok(((), true))
        });
    }

    async fn update_task_card(&self, task: StoredTorrent) {
        let Some(message_id) = task.card_message_id else {
            return;
        };
        if task.card_send_state != CardSendState::Delivered || !task.card_render_pending {
            return;
        }
        let snapshot = task_snapshot(&task);
        let generation = task.card_generation;
        let task_id = task.task_id.clone();
        let result = self.backend.update_card(snapshot.clone()).await;
        let _ = self.store.transact(|state| {
            let task = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            if task.card_message_id != Some(message_id) || task.card_generation != generation {
                return Ok(((), false));
            }
            task.card_render_pending = task.render_revision > snapshot.render_revision;
            task.next_card_update_at = unix_seconds().saturating_add(self.settings.poll_seconds);
            if result.is_err() {
                // Do not send replacement Telegram messages automatically. `/torrents` explicitly
                // opens a new card generation when the user needs recovery.
                task.card_send_state = CardSendState::Unknown;
                task.card_message_id = None;
            }
            task.updated_at = unix_seconds();
            Ok(((), true))
        });
    }

    async fn process_magnet(&self, task: StoredTorrent) {
        let Some(source) = &task.magnet else {
            return;
        };
        let Ok(parsed) = crate::torrent_magnet::parse_magnet_uri(&source.uri) else {
            self.mark_invalid(&task.task_id, "Saved magnet metadata is invalid.");
            return;
        };
        let identity = parsed.identity;
        let matches = match self.backend.lookup(identity.clone()).await {
            Ok(matches) => matches
                .into_iter()
                .filter(|remote| remote.matches_identity(&identity))
                .collect::<Vec<_>>(),
            Err(_) => {
                self.defer_magnet(
                    &task.task_id,
                    "Could not check qBittorrent; the existing submission intent is retained.",
                );
                return;
            }
        };
        if matches.len() > 1 {
            self.defer_magnet(
                &task.task_id,
                "More than one qBittorrent task matches this magnet identity.",
            );
            return;
        }
        if let Some(remote) = matches.into_iter().next() {
            let bytes = match self.backend.export_metainfo(remote.hash.clone()).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    // Pending metadata is never exposed as a Music-selectable inventory.
                    let mut pending = remote;
                    pending.files.clear();
                    let _ = self.store.transact(|state| {
                        let current = state
                            .tasks
                            .get_mut(&task.task_id)
                            .context("torrent task disappeared")?;
                        current.remote = Some(pending.clone());
                        Ok(((), true))
                    });
                    self.defer_magnet(&task.task_id, "Waiting for verified torrent metadata from qBittorrent. The download is retained.");
                    return;
                }
            };
            let meta = match parse_metainfo(
                &bytes,
                self.settings.max_torrent_bytes,
                self.settings.max_files,
            ) {
                Ok(meta)
                    if identity
                        .v1
                        .as_ref()
                        .is_none_or(|hash| meta.identity.v1.as_ref() == Some(hash))
                        && identity
                            .v2
                            .as_ref()
                            .is_none_or(|hash| meta.identity.v2.as_ref() == Some(hash)) =>
                {
                    meta
                }
                _ => {
                    self.mark_invalid(&task.task_id, "Downloaded magnet metadata failed identity or safety validation. qBittorrent data was left unchanged.");
                    return;
                }
            };
            if validate_remote_inventory(&remote, &meta).is_err() {
                self.defer_magnet(&task.task_id, "Torrent metadata is available; waiting for a matching complete file inventory.");
                return;
            }
            if self
                .store
                .write_metainfo_blob(&task.task_id, &bytes, &meta.raw_sha256)
                .is_err()
            {
                self.defer_magnet(
                    &task.task_id,
                    "Could not save verified magnet metadata; retry is available.",
                );
                return;
            }
            let saved = self.store.transact(|state| {
                let current = state
                    .tasks
                    .get_mut(&task.task_id)
                    .context("torrent task disappeared")?;
                if current.metainfo.is_some() {
                    return Ok(((), false));
                }
                current.raw_sha256 = Some(meta.raw_sha256.clone());
                current.metainfo = Some(meta.clone());
                current.remote = None;
                current.inventory_revision = current.inventory_revision.saturating_add(1).max(1);
                current.selection_revision = current.selection_revision.saturating_add(1).max(1);
                current.attempts = 0;
                current.error = None;
                current.next_action_at = unix_seconds();
                touch(current);
                Ok(((), true))
            });
            if saved.is_ok() {
                let _ = self.attach_remote(&task.task_id, remote);
            }
            return;
        }
        if task.submit_intent {
            if task.invalid_after_reconcile {
                self.mark_invalid(
                    &task.task_id,
                    "qBittorrent rejected this magnet. No second submission was made.",
                );
            } else {
                self.defer_magnet(&task.task_id, "Submission outcome is being checked by identity without resubmitting the magnet.");
            }
            return;
        }
        let save_path = match task.magnet_save_path.clone() {
            Some(path) => path,
            None => match self.backend.default_save_path().await {
                Ok(path) if path.is_absolute() => {
                    path.join("Telegram Downloads").join(&task.task_id)
                }
                _ => {
                    self.defer_magnet(&task.task_id, "Could not resolve qBittorrent's default save path; no submission was made.");
                    return;
                }
            },
        };
        let intent = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(&task.task_id)
                .context("torrent task disappeared")?;
            if current.submit_intent {
                return Ok((false, false));
            }
            current.magnet_save_path = Some(save_path.clone());
            current.submit_intent = true;
            current.phase = TorrentPhase::Submitting;
            current.error = None;
            touch(current);
            Ok((true, true))
        });
        if !matches!(intent, Ok((true, true))) {
            return;
        }
        let result = self
            .backend
            .submit_magnet(source.uri.clone(), save_path)
            .await
            .unwrap_or(SubmitResult {
                http_status: 0,
                disposition: SubmitDisposition::Unknown,
            });
        let _ = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(&task.task_id)
                .context("torrent task disappeared")?;
            current.submit_result = Some(result.clone());
            current.invalid_after_reconcile =
                result.disposition == SubmitDisposition::InvalidMetainfo;
            current.phase = TorrentPhase::ReconciliationPending;
            current.next_action_at = unix_seconds();
            touch(current);
            Ok(((), true))
        });
    }

    async fn process_existing(&self, task: StoredTorrent) {
        let Some(source) = task.existing.as_ref() else {
            return;
        };
        let task_id = task.task_id.clone();
        let requested_hash = source.hash.clone();
        let remote = match self.backend.refresh(requested_hash.clone()).await {
            Ok(remote) if remote.hash.eq_ignore_ascii_case(&requested_hash) => remote,
            Ok(_) => {
                self.mark_existing_invalid(
                    &task_id,
                    "qBittorrent returned a different hash for this catalog task.",
                );
                return;
            }
            Err(_) => {
                self.mark_existing_invalid(
                    &task_id,
                    "The selected qBittorrent task is no longer available.",
                );
                return;
            }
        };

        if let Some(metainfo) = task.metainfo.as_ref() {
            if !remote.matches_identity(&metainfo.identity) {
                self.mark_existing_invalid(
                    &task_id,
                    "qBittorrent task identity changed after adoption.",
                );
                return;
            }
            if validate_remote_inventory(&remote, metainfo).is_err() {
                self.defer_existing(
                    &task_id,
                    "Waiting for qBittorrent to report a file inventory matching verified metadata.",
                );
                return;
            }
            let _ = self.attach_remote(&task_id, remote);
            return;
        }

        let bytes = match self.backend.export_metainfo(requested_hash.clone()).await {
            Ok(bytes) => bytes,
            Err(_) => {
                self.defer_existing(
                    &task_id,
                    "Waiting for verified torrent metadata from qBittorrent.",
                );
                return;
            }
        };
        let metainfo = match parse_metainfo(
            &bytes,
            self.settings.max_torrent_bytes,
            self.settings.max_files,
        ) {
            Ok(metainfo) if remote.matches_identity(&metainfo.identity) => metainfo,
            _ => {
                self.mark_existing_invalid(
                    &task_id,
                    "Exported torrent metadata did not match the selected qBittorrent task.",
                );
                return;
            }
        };
        if validate_remote_inventory(&remote, &metainfo).is_err() {
            self.defer_existing(
                &task_id,
                "Torrent metadata is verified; waiting for a matching qBittorrent file inventory.",
            );
            return;
        }
        if self
            .store
            .write_metainfo_blob(&task_id, &bytes, &metainfo.raw_sha256)
            .is_err()
        {
            self.defer_existing(
                &task_id,
                "Could not save verified torrent metadata; retry is available.",
            );
            return;
        }
        let saved = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(&task_id)
                .context("torrent task disappeared")?;
            if current
                .existing
                .as_ref()
                .is_none_or(|existing| !existing.hash.eq_ignore_ascii_case(&requested_hash))
            {
                bail!("catalog source changed while metadata was being verified");
            }
            current.raw_sha256 = Some(metainfo.raw_sha256.clone());
            current.metainfo = Some(metainfo.clone());
            current.remote = None;
            current.inventory_revision = current.inventory_revision.saturating_add(1).max(1);
            current.selection_revision = current.selection_revision.saturating_add(1).max(1);
            current.attempts = 0;
            current.error = None;
            current.next_action_at = unix_seconds();
            touch(current);
            Ok(((), true))
        });
        if saved.is_ok() {
            let _ = self.attach_remote(&task_id, remote);
        }
    }

    fn defer_existing(&self, task_id: &str, message: &str) {
        let _ = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(task_id)
                .context("torrent task disappeared")?;
            current.phase = TorrentPhase::DownloadingMetainfo;
            current.attempts = current.attempts.saturating_add(1);
            current.next_action_at = unix_seconds().saturating_add(backoff(current.attempts));
            current.error = Some(message.to_owned());
            touch(current);
            Ok(((), true))
        });
    }

    fn mark_existing_invalid(&self, task_id: &str, message: &str) {
        let _ = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(task_id)
                .context("torrent task disappeared")?;
            current.phase = TorrentPhase::Invalid;
            current.selected_indexes.clear();
            if let Some(remote) = current.remote.as_mut() {
                remote.files.clear();
            }
            current.error = Some(message.to_owned());
            touch(current);
            Ok(((), true))
        });
    }

    fn defer_magnet(&self, task_id: &str, message: &str) {
        let _ = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(task_id)
                .context("torrent task disappeared")?;
            current.phase = TorrentPhase::DownloadingMetainfo;
            current.manual_refresh = false;
            current.next_refresh_at = None;
            current.attempts = current.attempts.saturating_add(1);
            current.next_action_at = unix_seconds().saturating_add(backoff(current.attempts));
            current.error = Some(message.to_owned());
            touch(current);
            Ok(((), true))
        });
    }

    async fn download_and_parse(&self, task: StoredTorrent) {
        let task_id = task.task_id.clone();
        let Some(upload) = task.upload.clone() else {
            return;
        };
        let bytes = match self.backend.download_torrent(upload.clone()).await {
            Ok(bytes) => bytes,
            Err(_) => {
                let _ = self.store.transact(|state| {
                    let current = state
                        .tasks
                        .get_mut(&task_id)
                        .ok_or_else(|| anyhow!("torrent task disappeared"))?;
                    current.attempts = current.attempts.saturating_add(1);
                    current.next_action_at =
                        unix_seconds().saturating_add(backoff(current.attempts));
                    current.error = Some(
                        "Could not retrieve the uploaded torrent metadata; retry is available."
                            .to_string(),
                    );
                    touch(current);
                    Ok(((), true))
                });
                return;
            }
        };
        if bytes.len() > self.settings.max_torrent_bytes
            || (upload.file_size != 0 && u64::try_from(bytes.len()).ok() != Some(upload.file_size))
        {
            self.mark_invalid(
                &task_id,
                "Torrent metadata exceeds its configured size or has a mismatched file length.",
            );
            return;
        }
        let metainfo = match parse_metainfo(
            &bytes,
            self.settings.max_torrent_bytes,
            self.settings.max_files,
        ) {
            Ok(metainfo) => metainfo,
            Err(_) => {
                self.mark_invalid(
                    &task_id,
                    "Torrent metadata is invalid or exceeds supported limits.",
                );
                return;
            }
        };
        if folder_catalog(
            &metainfo
                .files
                .iter()
                .map(|file| file.relative_path.clone())
                .collect::<Vec<_>>(),
        )
        .is_err()
        {
            self.mark_invalid(
                &task_id,
                "Torrent contains too many folders for the supported selection controls.",
            );
            return;
        }
        let raw_sha256 = metainfo.raw_sha256.clone();
        if self
            .store
            .write_metainfo_blob(&task_id, &bytes, &raw_sha256)
            .is_err()
        {
            let _ = self.store.transact(|state| {
                let current = state
                    .tasks
                    .get_mut(&task_id)
                    .ok_or_else(|| anyhow!("torrent task disappeared"))?;
                current.attempts = current.attempts.saturating_add(1);
                current.next_action_at = unix_seconds().saturating_add(backoff(current.attempts));
                current.error = Some(
                    "Could not save the private torrent metadata; retry is available.".to_string(),
                );
                touch(current);
                Ok(((), true))
            });
            return;
        }
        let _ = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            if current.metainfo.is_none() {
                current.raw_sha256 = Some(raw_sha256);
                current.metainfo = Some(metainfo.clone());
                current.phase = TorrentPhase::CheckingExisting;
                current.inventory_revision = 1;
                current.selection_revision = current.selection_revision.saturating_add(1).max(1);
                current.attempts = 0;
                current.next_action_at = unix_seconds();
                current.error = None;
                touch(current);
            }
            Ok(((), true))
        });
    }

    async fn reconcile_or_submit(&self, task: StoredTorrent) {
        let Some(metainfo) = task.metainfo.clone() else {
            return;
        };
        let task_id = task.task_id.clone();
        let matches = match self.backend.lookup(metainfo.identity.clone()).await {
            Ok(matches) => matches
                .into_iter()
                .filter(|candidate| candidate.matches_identity(&metainfo.identity))
                .collect::<Vec<_>>(),
            Err(_) => {
                let _ = self.store.transact(|state| {
                    let current = state
                        .tasks
                        .get_mut(&task_id)
                        .ok_or_else(|| anyhow!("torrent task disappeared"))?;
                    current.attempts = current.attempts.saturating_add(1);
                    current.next_action_at =
                        unix_seconds().saturating_add(backoff(current.attempts));
                    current.error = Some(
                        "Could not check the local torrent client; retry is available.".to_string(),
                    );
                    touch(current);
                    Ok(((), true))
                });
                return;
            }
        };
        if matches.len() > 1 {
            let _ = self.store.transact(|state| {
                let current = state
                    .tasks
                    .get_mut(&task_id)
                    .ok_or_else(|| anyhow!("torrent task disappeared"))?;
                current.phase = TorrentPhase::Ambiguous;
                current.error = Some(
                    "More than one qBittorrent task matches this torrent identity.".to_string(),
                );
                touch(current);
                Ok(((), true))
            });
            return;
        }
        if let Some(remote) = matches.into_iter().next() {
            let _ = self.attach_remote(&task_id, remote);
            return;
        }

        if task.submit_intent {
            if task.invalid_after_reconcile {
                self.mark_invalid(&task_id, "qBittorrent rejected this torrent metadata.");
            } else {
                let _ = self.store.transact(|state| {
                    let current = state.tasks.get_mut(&task_id).ok_or_else(|| anyhow!("torrent task disappeared"))?;
                    current.phase = TorrentPhase::ReconciliationPending;
                    current.attempts = current.attempts.saturating_add(1);
                    current.next_action_at = unix_seconds().saturating_add(backoff(current.attempts));
                    current.error = Some("Submission outcome is unknown; the task is being checked without resubmitting.".to_string());
                    touch(current);
                    Ok(((), true))
                });
            }
            return;
        }

        let raw =
            match self
                .store
                .read_metainfo_blob(&task_id, task.raw_sha256.as_deref(), &metainfo)
            {
                Ok(bytes) => bytes,
                Err(_) => {
                    self.mark_invalid(
                        &task_id,
                        "Saved torrent metadata could not be verified; no submission was made.",
                    );
                    return;
                }
            };
        let intent_saved = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            if current.submit_intent || current.remote.is_some() {
                return Ok((false, false));
            }
            current.submit_intent = true;
            current.phase = TorrentPhase::Submitting;
            current.submit_result = None;
            current.next_action_at = unix_seconds();
            current.error = None;
            touch(current);
            Ok((true, true))
        });
        if !matches!(intent_saved, Ok((true, true))) {
            return;
        }
        let file_name = format!("{}.torrent", metainfo.identity.storage_key());
        let result = self.backend.submit(raw, file_name).await;
        let submit_result = match result {
            Ok(result) => result,
            Err(_) => SubmitResult {
                http_status: 0,
                disposition: SubmitDisposition::Unknown,
            },
        };
        let _ = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            if current.submit_intent && current.remote.is_none() {
                current.submit_result = Some(submit_result.clone());
                current.invalid_after_reconcile =
                    submit_result.disposition == SubmitDisposition::InvalidMetainfo;
                current.phase = TorrentPhase::ReconciliationPending;
                current.attempts = 0;
                current.next_action_at = unix_seconds();
                current.error = Some(
                    "qBittorrent submission is being verified by torrent identity.".to_string(),
                );
                touch(current);
            }
            Ok(((), true))
        });
    }

    async fn refresh_remote(&self, task: StoredTorrent) {
        let Some(current_remote) = task.remote.as_ref() else {
            return;
        };
        let task_id = task.task_id.clone();
        let result = self.backend.refresh(current_remote.hash.clone()).await;
        let remote = match result {
            Ok(remote) => remote,
            Err(_) => {
                let _ = self.store.transact(|state| {
                    let current = state
                        .tasks
                        .get_mut(&task_id)
                        .ok_or_else(|| anyhow!("torrent task disappeared"))?;
                    current.manual_refresh = false;
                    current.attempts = current.attempts.saturating_add(1);
                    current.next_refresh_at =
                        Some(unix_seconds().saturating_add(backoff(current.attempts)));
                    current.error = Some(
                        "Could not refresh qBittorrent progress; retry is available.".to_string(),
                    );
                    touch(current);
                    Ok(((), true))
                });
                return;
            }
        };
        let _ = self.attach_remote(&task_id, remote);
    }

    fn attach_remote(&self, task_id: &str, remote: TorrentRemoteSnapshot) -> Result<()> {
        self.store
            .transact(|state| {
                let task = state
                    .tasks
                    .get_mut(task_id)
                    .ok_or_else(|| anyhow!("torrent task disappeared"))?;
                let metainfo = task
                    .metainfo
                    .as_ref()
                    .ok_or_else(|| anyhow!("torrent metainfo disappeared"))?;
                ensure!(
                    remote.matches_identity(&metainfo.identity),
                    "qBittorrent returned a different torrent identity"
                );
                validate_remote_inventory(&remote, metainfo)?;
                let inventory_changed = task
                    .remote
                    .as_ref()
                    .map(|old| !same_inventory(old, &remote))
                    .unwrap_or_else(|| !same_metainfo_inventory(metainfo, &remote));
                if inventory_changed {
                    task.inventory_revision = task.inventory_revision.saturating_add(1).max(1);
                    task.selection_revision = task.selection_revision.saturating_add(1).max(1);
                }
                task.phase = if remote.is_complete() {
                    TorrentPhase::Completed
                } else {
                    TorrentPhase::Downloading
                };
                task.remote = Some(remote.clone());
                task.manual_refresh = false;
                task.attempts = 0;
                task.next_refresh_at = (!remote.is_complete())
                    .then(|| unix_seconds().saturating_add(self.settings.poll_seconds));
                task.next_action_at = unix_seconds();
                task.error = None;
                task.next_music_status_at.get_or_insert_with(unix_seconds);
                touch(task);
                Ok(((), true))
            })
            .map(|_| ())
    }

    fn has_ready_imports(&self, task: &StoredTorrent) -> bool {
        let Some(remote) = task.remote.as_ref() else {
            return false;
        };
        task.imports.iter().any(|(index, request)| {
            request.music_job.is_none()
                && task.selected_indexes.contains(index)
                && request.confirmed_inventory_revision == task.inventory_revision
                && request.confirmed_selection_revision == task.selection_revision
                && request.next_retry_at <= unix_seconds()
                && remote
                    .files
                    .iter()
                    .find(|file| file.index == *index)
                    .is_some_and(|file| file.progress >= 1.0)
        })
    }

    async fn enqueue_ready_imports(&self, task: StoredTorrent) {
        let Some(remote) = task.remote.as_ref() else {
            return;
        };
        let now = unix_seconds();
        let mut eligible = task
            .imports
            .iter()
            .filter(|(index, request)| {
                request.music_job.is_none()
                    && task.selected_indexes.contains(index)
                    && request.confirmed_inventory_revision == task.inventory_revision
                    && request.confirmed_selection_revision == task.selection_revision
                    && request.next_retry_at <= now
            })
            .filter_map(|(index, request)| {
                let remote_file = remote.files.iter().find(|file| file.index == *index)?;
                if remote_file.progress < 1.0 {
                    return None;
                }
                Some((*index, request.clone(), remote_file.clone()))
            })
            .collect::<Vec<_>>();
        eligible.sort_by_key(|(index, _, _)| *index);
        eligible.truncate(MAX_MUSIC_BATCH_FILES);
        if eligible.is_empty() {
            return;
        }
        let mut files = Vec::with_capacity(eligible.len());
        let mut included_indexes = BTreeSet::new();
        for (index, request, remote_file) in &eligible {
            let Ok(relative_path) = validate_remote_relative_path(&remote_file.relative_path)
            else {
                self.mark_import_error(
                    &task.task_id,
                    *index,
                    "qBittorrent returned an unsafe local file path.",
                );
                continue;
            };
            let source_key = request.source_key.clone();
            files.push(CompletedTorrentFile {
                source_key,
                source_root: remote.save_path.clone(),
                relative_path,
                expected_size: remote_file.size,
            });
            included_indexes.insert(*index);
        }
        if files.is_empty() {
            return;
        }
        let batch_id = music_batch_id(&task.task_id, task.inventory_revision, &files);
        let source_keys = files
            .iter()
            .map(|file| file.source_key.clone())
            .collect::<BTreeSet<_>>();
        let task_id = task.task_id.clone();
        let intent = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            if current.inventory_revision != task.inventory_revision {
                return Ok((false, false));
            }
            let current_inventory_revision = current.inventory_revision;
            let current_selection_revision = current.selection_revision;
            let selected_indexes = current.selected_indexes.clone();
            for (index, _, _) in &eligible {
                if included_indexes.contains(index)
                    && let Some(current_request) = current.imports.get_mut(index)
                    && current_request.music_job.is_none()
                    && current_request.confirmed_inventory_revision == current_inventory_revision
                    && current_request.confirmed_selection_revision == current_selection_revision
                    && selected_indexes.contains(index)
                {
                    current_request.enqueue_intent = true;
                    current_request.batch_id = Some(batch_id.clone());
                    current_request.next_retry_at = unix_seconds()
                        .saturating_add(backoff(current_request.attempts.saturating_add(1)));
                }
            }
            current.phase = TorrentPhase::Importing;
            current.next_action_at = unix_seconds().saturating_add(self.settings.poll_seconds);
            touch(current);
            Ok((true, true))
        });
        if !matches!(intent, Ok((true, true))) {
            return;
        }
        let result = self
            .backend
            .enqueue_music(
                task.source_ids().0,
                task.source_ids().1,
                batch_id.clone(),
                files,
            )
            .await;
        match result {
            Ok(refs) => {
                let _ = self.store.transact(|state| {
                    let current = state
                        .tasks
                        .get_mut(&task_id)
                        .ok_or_else(|| anyhow!("torrent task disappeared"))?;
                    if current.inventory_revision != task.inventory_revision {
                        return Ok(((), false));
                    }
                    for job in refs {
                        if !source_keys.contains(&job.source_key) {
                            continue;
                        }
                        if let Some((index, request)) = current
                            .imports
                            .iter_mut()
                            .find(|(_, request)| request.source_key == job.source_key)
                        {
                            if request.music_job.is_none() {
                                request.music_job = Some(job.clone());
                                request.music_stage = Some(TorrentMusicStage::Pending);
                                request.enqueue_intent = false;
                                request.error = None;
                            }
                            let _ = index;
                        }
                    }
                    current.phase = aggregate_phase(current);
                    current.next_music_status_at =
                        Some(unix_seconds().saturating_add(MUSIC_STATUS_POLL_SECONDS));
                    current.next_action_at = unix_seconds();
                    touch(current);
                    Ok(((), true))
                });
            }
            Err(_) => {
                let _ = self.store.transact(|state| {
                    let current = state.tasks.get_mut(&task_id).ok_or_else(|| anyhow!("torrent task disappeared"))?;
                    for (index, _, _) in &eligible {
                        if let Some(request) = current.imports.get_mut(index) {
                            request.attempts = request.attempts.saturating_add(1);
                            request.enqueue_intent = false;
                            request.batch_id = None;
                            request.error = Some("The selected local files could not be verified or Music is not ready; retry is available.".to_string());
                            request.next_retry_at = unix_seconds().saturating_add(backoff(request.attempts));
                        }
                    }
                    current.phase = aggregate_phase(current);
                    current.error = Some("One or more selected Music imports are waiting for a safe retry.".to_string());
                    current.next_action_at = current.imports.values().map(|request| request.next_retry_at).min().unwrap_or_else(unix_seconds);
                    touch(current);
                    Ok(((), true))
                });
            }
        }
    }

    async fn refresh_music_status(&self, task: StoredTorrent) {
        let refs = task
            .imports
            .values()
            .filter_map(|request| request.music_job.as_ref())
            .map(|job| job.job_id.clone())
            .collect::<Vec<_>>();
        if refs.is_empty() {
            return;
        }
        let statuses = match self.backend.music_status(task.source_ids().0, refs).await {
            Ok(statuses) => statuses,
            Err(_) => {
                let _ = self.store.transact(|state| {
                    let current = state
                        .tasks
                        .get_mut(&task.task_id)
                        .ok_or_else(|| anyhow!("torrent task disappeared"))?;
                    current.next_music_status_at =
                        Some(unix_seconds().saturating_add(MUSIC_STATUS_POLL_SECONDS));
                    Ok(((), true))
                });
                return;
            }
        };
        let _ = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(&task.task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            for status in statuses {
                if let Some(request) = current
                    .imports
                    .values_mut()
                    .find(|request| request.source_key == status.source_key)
                    && request
                        .music_job
                        .as_ref()
                        .is_some_and(|job| job.job_id == status.job_id)
                {
                    request.music_stage = Some(status.stage);
                    request.error = status.error.map(bounded_error);
                }
            }
            current.phase = aggregate_phase(current);
            current.next_music_status_at = has_pending_music_jobs(current)
                .then(|| unix_seconds().saturating_add(MUSIC_STATUS_POLL_SECONDS));
            current.next_action_at = current
                .imports
                .values()
                .filter(|request| request.music_job.is_none())
                .map(|request| request.next_retry_at)
                .min()
                .unwrap_or(u64::MAX);
            touch(current);
            Ok(((), true))
        });
    }

    async fn retry_music_jobs(&self, task: StoredTorrent) {
        let retry_ids = task.retry_job_ids.clone();
        let result = self
            .backend
            .retry_music(task.source_ids().0, retry_ids.clone())
            .await;
        let _ = self.store.transact(|state| {
            let current = state
                .tasks
                .get_mut(&task.task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            if current.retry_job_ids != retry_ids {
                return Ok(((), false));
            }
            current.retry_job_ids.clear();
            if result.is_ok() {
                for request in current.imports.values_mut() {
                    if request
                        .music_job
                        .as_ref()
                        .is_some_and(|job| retry_ids.contains(&job.job_id))
                    {
                        request.music_stage = Some(TorrentMusicStage::Pending);
                        request.error = None;
                    }
                }
                current.error = None;
            } else {
                current.error = Some(
                    "Music retry could not be started; use the retry action again.".to_string(),
                );
            }
            current.next_music_status_at = Some(unix_seconds());
            current.phase = aggregate_phase(current);
            touch(current);
            Ok(((), true))
        });
    }

    fn mark_import_error(&self, task_id: &str, index: u32, message: &str) {
        let _ = self.store.transact(|state| {
            let task = state
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            if let Some(request) = task.imports.get_mut(&index) {
                request.error = Some(message.to_string());
                request.next_retry_at = u64::MAX;
            }
            task.error = Some(message.to_string());
            touch(task);
            Ok(((), true))
        });
    }

    fn mark_invalid(&self, task_id: &str, message: &str) {
        let _ = self.store.transact(|state| {
            let task = state
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| anyhow!("torrent task disappeared"))?;
            task.phase = TorrentPhase::Invalid;
            task.error = Some(message.to_string());
            touch(task);
            Ok(((), true))
        });
    }
}

struct TorrentStore {
    state_dir: PathBuf,
    metainfo_dir: PathBuf,
    state_path: PathBuf,
    provider: Arc<dyn QueueFileProvider>,
    state: StdMutex<StoredState>,
    _owner_lock: File,
}

impl TorrentStore {
    fn open(settings: &TorrentSettings, provider: Arc<dyn QueueFileProvider>) -> Result<Self> {
        let state_dir = create_private_directory_path(&settings.state_dir)?;
        let metainfo_dir = state_dir.join(METAINFO_DIR_NAME);
        ensure_private_directory_entry(&metainfo_dir, provider.as_ref())?;
        let owner_lock_path = state_dir.join(OWNER_LOCK_FILE_NAME);
        ensure_private_lock_file(&owner_lock_path, provider.as_ref())?;
        let owner_lock = acquire_owner_lock(&owner_lock_path, provider.as_ref())?;
        let state_path = state_dir.join(STATE_FILE_NAME);
        let state = read_state(&state_path, provider.as_ref())?.unwrap_or_default();
        validate_state(&state, settings, &metainfo_dir)?;
        let store = Self {
            state_dir,
            metainfo_dir,
            state_path,
            provider,
            state: StdMutex::new(state),
            _owner_lock: owner_lock,
        };
        store.recover_interrupted_state()?;
        Ok(store)
    }

    fn recover_interrupted_state(&self) -> Result<()> {
        self.transact(|state| {
            for task in state.tasks.values_mut() {
                if task.card_send_state == CardSendState::InFlight {
                    task.card_send_state = CardSendState::Unknown;
                    task.card_message_id = None;
                    task.card_render_pending = false;
                }
                if task.submit_intent && task.remote.is_none() {
                    task.phase = TorrentPhase::ReconciliationPending;
                    task.next_action_at = unix_seconds();
                    task.error = Some(
                        "Submission outcome is unknown; the task will only be reconciled."
                            .to_string(),
                    );
                }
                for import in task.imports.values_mut() {
                    if import.enqueue_intent && import.music_job.is_none() {
                        // Retain the same source key and batch ID. The local Music service dedupes
                        // replayed source keys if a previous enqueue reached it before a crash.
                        import.next_retry_at = unix_seconds();
                    }
                }
            }
            Ok(())
        })
    }

    fn snapshot(&self) -> Result<StoredState> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| anyhow!("torrent state lock was poisoned"))
    }

    fn task(&self, task_id: &str) -> Result<Option<StoredTorrent>> {
        Ok(self.snapshot()?.tasks.get(task_id).cloned())
    }

    fn ready_task_ids(&self, now: u64) -> Result<Vec<String>> {
        let state = self.snapshot()?;
        Ok(state
            .tasks
            .values()
            .filter(|task| task_has_action_due(task, now))
            .map(|task| task.task_id.clone())
            .collect())
    }

    fn transact<T>(&self, update: impl FnOnce(&mut StoredState) -> Result<T>) -> Result<T> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| anyhow!("torrent state lock was poisoned"))?;
        let mut next = guard.clone();
        let output = update(&mut next)?;
        state_within_hard_caps(&next, &self.metainfo_dir)?;
        write_state_atomic(&self.state_path, &next, self.provider.as_ref())?;
        *guard = next;
        Ok(output)
    }

    fn write_metainfo_blob(
        &self,
        task_id: &str,
        bytes: &[u8],
        expected_sha256: &str,
    ) -> Result<()> {
        let path = self.metainfo_path(task_id);
        let mut written = false;
        let mut write = |coordinated_path: &Path| -> Result<()> {
            match fs::symlink_metadata(coordinated_path) {
                Ok(_) => {
                    let existing = read_private_file(coordinated_path, DEFAULT_MAX_TORRENT_BYTES)?;
                    ensure!(
                        sha256_hex(&existing) == expected_sha256,
                        "private torrent metadata file conflicts with task state"
                    );
                    written = true;
                    return Ok(());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).context("failed to inspect private torrent metadata");
                }
            }
            write_file_atomic(coordinated_path, bytes, 0o600)?;
            written = true;
            Ok(())
        };
        self.provider.coordinate_write(&path, &mut write)?;
        ensure!(
            written,
            "File Provider did not supply a coordinated torrent metadata accessor"
        );
        Ok(())
    }

    fn read_metainfo_blob(
        &self,
        task_id: &str,
        expected_sha256: Option<&str>,
        metainfo: &TorrentMetainfo,
    ) -> Result<Vec<u8>> {
        let path = self.metainfo_path(task_id);
        let mut result = None;
        let mut read = |coordinated_path: &Path| -> Result<()> {
            let bytes = read_private_file(coordinated_path, DEFAULT_MAX_TORRENT_BYTES)?;
            let sha256 = sha256_hex(&bytes);
            ensure!(
                Some(sha256.as_str()) == expected_sha256,
                "private torrent metadata digest changed"
            );
            let parsed = parse_metainfo(&bytes, DEFAULT_MAX_TORRENT_BYTES, DEFAULT_MAX_FILES)?;
            ensure!(
                parsed.raw_sha256 == metainfo.raw_sha256 && parsed.identity == metainfo.identity,
                "private torrent metadata identity changed"
            );
            result = Some(bytes);
            Ok(())
        };
        self.provider.coordinate_read(&path, &mut read)?;
        result.ok_or_else(|| {
            anyhow!("File Provider did not supply a coordinated torrent metadata accessor")
        })
    }

    fn metainfo_path(&self, task_id: &str) -> PathBuf {
        self.metainfo_dir.join(format!("{task_id}.torrent"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredState {
    version: u32,
    tasks: BTreeMap<String, StoredTorrent>,
    origins: BTreeMap<String, String>,
}

impl Default for StoredState {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            tasks: BTreeMap::new(),
            origins: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredTorrent {
    task_id: String,
    origin_key: String,
    upload: Option<TorrentUpload>,
    #[serde(default)]
    magnet: Option<TorrentMagnet>,
    #[serde(default)]
    existing: Option<TorrentExisting>,
    #[serde(default)]
    magnet_save_path: Option<PathBuf>,
    raw_sha256: Option<String>,
    metainfo: Option<TorrentMetainfo>,
    remote: Option<TorrentRemoteSnapshot>,
    phase: TorrentPhase,
    submit_intent: bool,
    submit_result: Option<SubmitResult>,
    invalid_after_reconcile: bool,
    selected_indexes: BTreeSet<u32>,
    imports: BTreeMap<u32, StoredImport>,
    retry_job_ids: Vec<String>,
    inventory_revision: u64,
    selection_revision: u64,
    render_revision: u64,
    cursor_folder_id: u32,
    cursor_page: u32,
    #[serde(default)]
    review_selection: bool,
    card_generation: u64,
    card_request_keys: Vec<String>,
    card_send_state: CardSendState,
    card_message_id: Option<i64>,
    card_render_pending: bool,
    #[serde(default)]
    next_card_update_at: u64,
    manual_refresh: bool,
    next_action_at: u64,
    next_refresh_at: Option<u64>,
    next_music_status_at: Option<u64>,
    attempts: u32,
    created_at: u64,
    updated_at: u64,
    error: Option<String>,
}

impl StoredTorrent {
    fn source_ids(&self) -> (i64, i64, i64) {
        self.upload
            .as_ref()
            .map(|source| (source.chat_id, source.user_id, source.message_id))
            .or_else(|| {
                self.magnet
                    .as_ref()
                    .map(|source| (source.chat_id, source.user_id, source.message_id))
            })
            .or_else(|| {
                self.existing
                    .as_ref()
                    .map(|source| (source.chat_id, source.user_id, source.message_id))
            })
            .unwrap_or((0, 0, 0))
    }
}

fn exit_review(task: &mut StoredTorrent) {
    if task.review_selection {
        task.review_selection = false;
        task.cursor_page = 0;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredImport {
    source_key: String,
    confirmed_inventory_revision: u64,
    confirmed_selection_revision: u64,
    enqueue_intent: bool,
    batch_id: Option<String>,
    music_job: Option<TorrentMusicJobRef>,
    music_stage: Option<TorrentMusicStage>,
    error: Option<String>,
    attempts: u32,
    next_retry_at: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CardSendState {
    Requested,
    InFlight,
    Delivered,
    Unknown,
}

fn task_has_action_due(task: &StoredTorrent, now: u64) -> bool {
    if task.card_send_state == CardSendState::Requested
        || (task.card_render_pending && task.next_card_update_at <= now)
    {
        return true;
    }
    if task.phase == TorrentPhase::Invalid {
        return false;
    }
    if task.manual_refresh || task.next_refresh_at.is_some_and(|at| at <= now) {
        return true;
    }
    if task.next_action_at <= now
        && (task.metainfo.is_none() || task.remote.is_none() || !task.retry_job_ids.is_empty())
    {
        return true;
    }
    if task.imports.values().any(|request| {
        request.music_job.is_none() && request.enqueue_intent && request.next_retry_at <= now
    }) {
        return true;
    }
    if task.next_music_status_at.is_some_and(|at| at <= now) && has_pending_music_jobs(task) {
        return true;
    }
    task.remote.as_ref().is_some_and(|remote| {
        task.imports.iter().any(|(index, request)| {
            request.music_job.is_none()
                && task.selected_indexes.contains(index)
                && request.confirmed_inventory_revision == task.inventory_revision
                && request.confirmed_selection_revision == task.selection_revision
                && request.next_retry_at <= now
                && remote
                    .files
                    .iter()
                    .find(|file| file.index == *index)
                    .is_some_and(|file| file.progress >= 1.0)
        })
    })
}

fn validate_upload(upload: &TorrentUpload, settings: &TorrentSettings) -> Result<()> {
    ensure!(
        upload.update_id >= 0 && upload.chat_id != 0 && upload.message_id > 0 && upload.user_id > 0,
        "torrent upload identifiers are invalid"
    );
    ensure!(
        !upload.file_id.is_empty() && upload.file_id.len() <= 4_096,
        "torrent upload file identifier is invalid"
    );
    ensure!(
        !upload.file_unique_id.is_empty() && upload.file_unique_id.len() <= 512,
        "torrent upload unique identifier is invalid"
    );
    ensure!(
        !upload.file_name.is_empty() && upload.file_name.len() <= 512,
        "torrent upload file name is invalid"
    );
    ensure!(
        upload.file_name.to_ascii_lowercase().ends_with(".torrent"),
        "uploaded file must have a .torrent extension"
    );
    ensure!(
        upload.file_size <= settings.max_torrent_bytes as u64,
        "uploaded torrent exceeds the configured byte limit"
    );
    Ok(())
}

fn validate_existing_source(source: &TorrentExisting) -> Result<()> {
    ensure!(
        source.chat_id > 0 && source.user_id == source.chat_id && source.message_id > 0,
        "catalog message identifiers are invalid"
    );
    ensure!(
        matches!(source.hash.len(), 40 | 64)
            && source.hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "qBittorrent hash is invalid"
    );
    Ok(())
}

fn validate_request_key(request_key: &str) -> Result<()> {
    ensure!(
        !request_key.is_empty() && request_key.len() <= 128,
        "torrent recovery request key is invalid"
    );
    ensure!(
        request_key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':')),
        "torrent recovery request key is invalid"
    );
    Ok(())
}

fn validate_owner(task: &StoredTorrent, chat_id: i64, user_id: i64) -> Result<()> {
    ensure!(
        task.source_ids().0 == chat_id && task.source_ids().1 == user_id,
        "torrent task belongs to another owner"
    );
    Ok(())
}

fn remove_unqueued_import(task: &mut StoredTorrent, index: u32) {
    let removable = task
        .imports
        .get(&index)
        .is_some_and(|request| request.music_job.is_none() && !request.enqueue_intent);
    if removable {
        task.imports.remove(&index);
    }
}

fn upload_origin_key(upload: &TorrentUpload) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        upload.chat_id, upload.user_id, upload.message_id, upload.update_id, upload.file_unique_id
    )
}

fn task_matches_existing_hash(task: &StoredTorrent, hash: &str) -> bool {
    task.existing
        .as_ref()
        .is_some_and(|source| source.hash.eq_ignore_ascii_case(hash))
        || (task.metainfo.is_some()
            && (task
                .remote
                .as_ref()
                .is_some_and(|remote| remote.hash.eq_ignore_ascii_case(hash))
                || task
                    .metainfo
                    .as_ref()
                    .is_some_and(|meta| meta.identity.matches_hash(hash))))
}

fn prepare_task_for_existing_revalidation(task: &mut StoredTorrent) {
    task.next_action_at = unix_seconds();
    task.next_refresh_at = None;
    if task.metainfo.is_some() {
        task.manual_refresh = true;
        task.phase = TorrentPhase::CheckingExisting;
    } else if task.existing.is_some() {
        task.manual_refresh = false;
        task.phase = TorrentPhase::DownloadingMetainfo;
    }
    touch(task);
}

fn existing_origin_key(source: &TorrentExisting) -> String {
    format!(
        "{}:{}:{}:existing",
        source.chat_id,
        source.user_id,
        source.hash.to_ascii_lowercase()
    )
}

fn task_id_for_origin(origin_key: &str) -> String {
    hex(&Sha256::digest(origin_key.as_bytes())[..16])
}

fn torrent_music_source_key(task: &StoredTorrent, index: u32) -> Result<String> {
    let metainfo = task
        .metainfo
        .as_ref()
        .ok_or_else(|| anyhow!("torrent metainfo is unavailable"))?;
    Ok(format!(
        "torrent:{}:{}:{}:{index}",
        task.source_ids().0,
        task.source_ids().1,
        metainfo.identity.storage_key()
    ))
}

fn music_batch_id(task_id: &str, revision: u64, files: &[CompletedTorrentFile]) -> String {
    let source_keys = files
        .iter()
        .map(|file| file.source_key.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let digest = Sha256::digest(source_keys.as_bytes());
    format!("torrent-{task_id}-{revision}-{}", hex(&digest[..6]))
}

fn task_file(task: &StoredTorrent, index: u32) -> Result<TorrentFileSnapshot> {
    let position = usize::try_from(index).context("torrent file index is invalid")?;
    if let Some(remote) = task.remote.as_ref() {
        let file = remote
            .files
            .get(position)
            .filter(|file| file.index == index)
            .ok_or_else(|| anyhow!("torrent file index is invalid"))?;
        return Ok(snapshot_file(task, file));
    }
    let file = task
        .metainfo
        .as_ref()
        .and_then(|metainfo| metainfo.files.get(position))
        .filter(|file| file.index == index)
        .ok_or_else(|| anyhow!("torrent file index is invalid"))?;
    Ok(snapshot_metainfo_file(task, file))
}

fn task_snapshot(task: &StoredTorrent) -> TorrentSnapshot {
    let metainfo = task.metainfo.as_ref();
    let remote = task.remote.as_ref();
    let files: Vec<TorrentFileSnapshot> = if let Some(remote) = remote {
        remote
            .files
            .iter()
            .map(|file| snapshot_file(task, file))
            .collect()
    } else {
        metainfo
            .into_iter()
            .flat_map(|metainfo| metainfo.files.iter())
            .map(|file| snapshot_metainfo_file(task, file))
            .collect()
    };
    let folders = folder_catalog(
        &files
            .iter()
            .map(|file| file.relative_path.clone())
            .collect::<Vec<_>>(),
    )
    .unwrap_or_default();
    let selected_count = files
        .iter()
        .filter(|file| file.selected && file.supported_music)
        .count();
    let import_count = task
        .imports
        .values()
        .filter(|request| request.music_job.is_some())
        .count();
    TorrentSnapshot {
        task_id: task.task_id.clone(),
        chat_id: task.source_ids().0,
        user_id: task.source_ids().1,
        upload_message_id: task.source_ids().2,
        card_message_id: task.card_message_id,
        inventory_revision: task.inventory_revision,
        selection_revision: task.selection_revision,
        render_revision: task.render_revision,
        name: metainfo
            .map(|metainfo| metainfo.name.clone())
            .unwrap_or_else(|| {
                task.upload
                    .as_ref()
                    .map(|upload| upload.file_name.clone())
                    .or_else(|| {
                        task.magnet.as_ref().and_then(|magnet| {
                            crate::torrent_magnet::parse_magnet_uri(&magnet.uri)
                                .ok()
                                .and_then(|parsed| parsed.display_name)
                        })
                    })
                    .or_else(|| {
                        task.existing
                            .as_ref()
                            .and_then(|_| remote.map(|remote| remote.name.clone()))
                    })
                    .unwrap_or_else(|| "Torrent task".into())
            }),
        identity: metainfo
            .map(|metainfo| metainfo.identity.clone())
            .or_else(|| {
                task.magnet.as_ref().and_then(|magnet| {
                    crate::torrent_magnet::parse_magnet_uri(&magnet.uri)
                        .ok()
                        .map(|parsed| parsed.identity)
                })
            }),
        phase: task.phase.clone(),
        state: remote.map(|remote| remote.state.clone()),
        progress: remote.map(|remote| remote.progress).unwrap_or(0.0),
        cursor_folder_id: task.cursor_folder_id,
        cursor_page: task.cursor_page,
        review_selection: task.review_selection,
        folders,
        files,
        selected_count,
        import_count,
        error: task.error.clone(),
    }
}

fn snapshot_file(task: &StoredTorrent, remote: &RemoteTorrentFile) -> TorrentFileSnapshot {
    let metainfo = task
        .metainfo
        .as_ref()
        .and_then(|metainfo| metainfo.files.get(remote.index as usize));
    let import = task.imports.get(&remote.index);
    TorrentFileSnapshot {
        index: remote.index,
        relative_path: remote.relative_path.clone(),
        size: remote.size,
        progress: remote.progress,
        complete: remote.progress >= 1.0,
        selected: task.selected_indexes.contains(&remote.index),
        supported_music: metainfo
            .is_some_and(|file| !file.is_padding && supports_music_import(&file.relative_path)),
        needs_reconfirm: import_needs_reconfirm(task, import),
        music_stage: import.and_then(|request| request.music_stage.clone()),
    }
}

fn snapshot_metainfo_file(task: &StoredTorrent, file: &MetainfoFile) -> TorrentFileSnapshot {
    let import = task.imports.get(&file.index);
    TorrentFileSnapshot {
        index: file.index,
        relative_path: file.relative_path.clone(),
        size: file.size,
        progress: 0.0,
        complete: false,
        selected: task.selected_indexes.contains(&file.index),
        supported_music: !file.is_padding && supports_music_import(&file.relative_path),
        needs_reconfirm: import_needs_reconfirm(task, import),
        music_stage: import.and_then(|request| request.music_stage.clone()),
    }
}

fn import_needs_reconfirm(task: &StoredTorrent, import: Option<&StoredImport>) -> bool {
    import.is_some_and(|request| {
        request.music_job.is_none()
            && !request.enqueue_intent
            && (request.confirmed_inventory_revision != task.inventory_revision
                || request.confirmed_selection_revision != task.selection_revision)
    })
}

fn task_folders(task: &StoredTorrent) -> Result<Vec<TorrentFolder>> {
    folder_catalog(&task_file_paths(task))
}

fn folder_catalog(paths: &[PathBuf]) -> Result<Vec<TorrentFolder>> {
    let mut folders = BTreeMap::<PathBuf, u32>::new();
    let mut root_count = 0_u32;
    for path in paths {
        root_count = root_count.saturating_add(1);
        let mut current = PathBuf::new();
        let components = path.components().collect::<Vec<_>>();
        ensure!(
            !components.is_empty() && components.len() <= 64,
            "torrent file path depth exceeds the supported limit"
        );
        for component in components.iter().take(components.len().saturating_sub(1)) {
            match component {
                Component::Normal(value) => current.push(value),
                _ => bail!("torrent folder path is unsafe"),
            }
            let descendants = folders.entry(current.clone()).or_default();
            *descendants = descendants.saturating_add(1);
        }
        ensure!(
            folders.len() <= 5_000,
            "torrent contains too many folder identifiers"
        );
    }
    let mut output = Vec::with_capacity(folders.len() + 1);
    output.push(TorrentFolder {
        id: 0,
        relative_path: PathBuf::new(),
        file_count: root_count,
    });
    for (index, (path, file_count)) in folders.into_iter().enumerate() {
        output.push(TorrentFolder {
            id: u32::try_from(index + 1)
                .context("torrent folder identifier exceeds the supported range")?,
            relative_path: path,
            file_count,
        });
    }
    Ok(output)
}

fn indexes_in_folder(task: &StoredTorrent, folder: &Path) -> Result<Vec<u32>> {
    if let Some(remote) = task.remote.as_ref() {
        return Ok(remote
            .files
            .iter()
            .filter(|file| folder.as_os_str().is_empty() || file.relative_path.starts_with(folder))
            .map(|file| file.index)
            .collect());
    }
    Ok(task
        .metainfo
        .as_ref()
        .into_iter()
        .flat_map(|metainfo| metainfo.files.iter())
        .filter(|file| folder.as_os_str().is_empty() || file.relative_path.starts_with(folder))
        .map(|file| file.index)
        .collect())
}

fn task_file_paths(task: &StoredTorrent) -> Vec<PathBuf> {
    if let Some(remote) = task.remote.as_ref() {
        return remote
            .files
            .iter()
            .map(|file| file.relative_path.clone())
            .collect();
    }
    task.metainfo
        .as_ref()
        .into_iter()
        .flat_map(|metainfo| metainfo.files.iter())
        .map(|file| file.relative_path.clone())
        .collect()
}

fn validate_browse(task: &StoredTorrent, folder_id: u32, page: u32) -> Result<()> {
    let folders = task_folders(task)?;
    let folder = folders
        .iter()
        .find(|folder| folder.id == folder_id)
        .ok_or_else(|| anyhow!("torrent folder cursor is invalid"))?;
    let direct_files = task_snapshot(task)
        .files
        .iter()
        .filter(|file| {
            if folder.relative_path.as_os_str().is_empty() {
                file.relative_path
                    .parent()
                    .is_none_or(|parent| parent.as_os_str().is_empty())
            } else {
                file.relative_path.parent() == Some(folder.relative_path.as_path())
            }
        })
        .count();
    let direct_folders = folders
        .iter()
        .filter(|candidate| candidate.id != folder_id)
        .filter(|candidate| {
            candidate.relative_path.parent() == Some(folder.relative_path.as_path())
        })
        .count();
    let rows = direct_files.saturating_add(direct_folders);
    let pages = rows.max(1).div_ceil(WIDGET_PAGE_SIZE);
    ensure!(
        usize::try_from(page).unwrap_or(usize::MAX) < pages,
        "torrent page cursor is invalid"
    );
    Ok(())
}

fn validate_remote_inventory(
    remote: &TorrentRemoteSnapshot,
    metainfo: &TorrentMetainfo,
) -> Result<()> {
    ensure!(
        remote.save_path.is_absolute(),
        "qBittorrent returned an invalid save path"
    );
    ensure!(
        remote.files.len() == metainfo.files.len(),
        "qBittorrent file inventory has a different shape"
    );
    for (index, file) in remote.files.iter().enumerate() {
        ensure!(
            file.index as usize == index,
            "qBittorrent file indexes are not contiguous"
        );
        ensure!(
            metainfo
                .files
                .get(index)
                .is_some_and(|source| source.size == file.size),
            "qBittorrent file inventory size does not match metainfo"
        );
        let _ = validate_remote_relative_path(&file.relative_path)?;
    }
    let paths = remote
        .files
        .iter()
        .map(|file| file.relative_path.clone())
        .collect::<Vec<_>>();
    folder_catalog(&paths)?;
    Ok(())
}

fn validate_remote_relative_path(path: &Path) -> Result<PathBuf> {
    ensure!(
        path.is_relative(),
        "qBittorrent returned an absolute file path"
    );
    let mut output = PathBuf::new();
    let mut bytes = 0_usize;
    let mut count = 0_usize;
    for component in path.components() {
        let Component::Normal(value) = component else {
            bail!("qBittorrent returned a path traversal");
        };
        let value = value
            .to_str()
            .ok_or_else(|| anyhow!("qBittorrent file path is not UTF-8"))?;
        ensure!(
            !value.is_empty() && value != "." && value != "..",
            "qBittorrent returned a path traversal"
        );
        ensure!(
            value.len() <= MAX_TORRENT_COMPONENT_BYTES,
            "qBittorrent file path component is too long"
        );
        ensure!(
            !value
                .chars()
                .any(|character| character.is_control()
                    || matches!(character, '/' | '\\' | ':' | '\0')),
            "qBittorrent returned an unsafe file path"
        );
        bytes = bytes.saturating_add(value.len() + 1);
        count += 1;
        ensure!(
            bytes <= MAX_TORRENT_PATH_BYTES && count <= 64,
            "qBittorrent file path is too deep or long"
        );
        output.push(value);
    }
    ensure!(
        !output.as_os_str().is_empty(),
        "qBittorrent returned an empty file path"
    );
    Ok(output)
}

fn same_inventory(left: &TorrentRemoteSnapshot, right: &TorrentRemoteSnapshot) -> bool {
    left.save_path == right.save_path
        && left.content_path == right.content_path
        && left.files.len() == right.files.len()
        && left.files.iter().zip(&right.files).all(|(left, right)| {
            left.index == right.index
                && left.relative_path == right.relative_path
                && left.size == right.size
        })
}

fn same_metainfo_inventory(metainfo: &TorrentMetainfo, remote: &TorrentRemoteSnapshot) -> bool {
    metainfo.files.len() == remote.files.len()
        && metainfo
            .files
            .iter()
            .zip(&remote.files)
            .all(|(left, right)| {
                left.index == right.index
                    && left.relative_path == right.relative_path
                    && left.size == right.size
            })
}

fn aggregate_phase(task: &StoredTorrent) -> TorrentPhase {
    let requests = task.imports.values().collect::<Vec<_>>();
    if !requests.is_empty()
        && requests
            .iter()
            .all(|request| request.music_stage == Some(TorrentMusicStage::Complete))
    {
        TorrentPhase::Imported
    } else if requests
        .iter()
        .any(|request| request.music_job.is_none() && !request.enqueue_intent)
    {
        TorrentPhase::ImportRequested
    } else if requests
        .iter()
        .any(|request| request.music_job.is_some() || request.enqueue_intent)
    {
        TorrentPhase::Importing
    } else if task
        .remote
        .as_ref()
        .is_some_and(TorrentRemoteSnapshot::is_complete)
    {
        TorrentPhase::Completed
    } else {
        TorrentPhase::Downloading
    }
}

fn has_pending_music_jobs(task: &StoredTorrent) -> bool {
    task.imports.values().any(|request| {
        request.music_job.is_some() && request.music_stage == Some(TorrentMusicStage::Pending)
    })
}

fn touch(task: &mut StoredTorrent) {
    task.render_revision = task.render_revision.saturating_add(1);
    task.updated_at = unix_seconds();
    if task.card_send_state == CardSendState::Delivered && task.card_message_id.is_some() {
        task.card_render_pending = true;
    }
}

fn backoff(attempts: u32) -> u64 {
    let shift = attempts.saturating_sub(1).min(16);
    BASE_RETRY_SECONDS
        .saturating_mul(1_u64 << shift)
        .min(MAX_RETRY_SECONDS)
}

fn bounded_error(error: String) -> String {
    if error.len() <= MAX_ERROR_BYTES {
        error
    } else {
        let mut end = MAX_ERROR_BYTES;
        while !error.is_char_boundary(end) {
            end -= 1;
        }
        error[..end].to_string()
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn validate_state(
    state: &StoredState,
    settings: &TorrentSettings,
    metainfo_dir: &Path,
) -> Result<()> {
    ensure!(
        state.version == STATE_VERSION,
        "torrent state version is unsupported"
    );
    ensure!(
        state.tasks.len() <= MAX_TASKS,
        "torrent state contains too many tasks"
    );
    ensure!(
        state_within_hard_caps(state, metainfo_dir).is_ok(),
        "torrent state exceeds hard limits"
    );
    for (task_id, task) in &state.tasks {
        ensure!(
            valid_task_id(task_id) && task.task_id == *task_id,
            "torrent state task identifier is invalid"
        );
        let source_count = usize::from(task.upload.is_some())
            + usize::from(task.magnet.is_some())
            + usize::from(task.existing.is_some());
        ensure!(source_count == 1, "torrent must have exactly one source");
        if let Some(upload) = &task.upload {
            validate_upload(upload, settings)?;
        }
        if let Some(magnet) = &task.magnet {
            ensure!(
                magnet.chat_id > 0
                    && magnet.user_id == magnet.chat_id
                    && magnet.message_id > 0
                    && magnet.update_id >= 0,
                "invalid magnet owner"
            );
            crate::torrent_magnet::parse_magnet_uri(&magnet.uri)?;
        }
        if let Some(existing) = &task.existing {
            validate_existing_source(existing)?;
        }
        ensure!(
            task.imports.len() <= settings.max_files,
            "torrent state contains too many import requests"
        );
        if let Some(meta) = &task.metainfo {
            ensure!(
                meta.files.len() <= settings.max_files,
                "torrent state metainfo contains too many files"
            );
            ensure!(
                folder_catalog(
                    &meta
                        .files
                        .iter()
                        .map(|file| file.relative_path.clone())
                        .collect::<Vec<_>>()
                )
                .is_ok(),
                "torrent state contains too many folders"
            );
        }
    }
    Ok(())
}

fn state_within_hard_caps(state: &StoredState, metainfo_dir: &Path) -> Result<()> {
    ensure!(
        state.version == STATE_VERSION,
        "torrent state version is unsupported"
    );
    ensure!(
        state.tasks.len() <= MAX_TASKS,
        "torrent state contains too many tasks"
    );
    let total_files = state.tasks.values().try_fold(0_usize, |total, task| {
        let files = task
            .metainfo
            .as_ref()
            .map(|meta| meta.files.len())
            .unwrap_or(0);
        total
            .checked_add(files)
            .ok_or_else(|| anyhow!("torrent file count overflow"))
    })?;
    ensure!(
        total_files <= MAX_TOTAL_FILE_RECORDS,
        "torrent state contains too many file records"
    );
    for (id, task) in &state.tasks {
        ensure!(
            valid_task_id(id) && task.task_id == *id,
            "torrent state task identifier is invalid"
        );
        ensure!(
            task.imports.len() <= 5_000,
            "torrent state contains too many Music requests"
        );
        ensure!(
            task.card_request_keys.len() <= MAX_REOPEN_REQUEST_KEYS,
            "torrent card replay keys exceed their limit"
        );
        ensure!(
            task.error
                .as_ref()
                .is_none_or(|error| error.len() <= MAX_ERROR_BYTES),
            "torrent state error text is too long"
        );
        if task.raw_sha256.is_some() {
            let path = metainfo_dir.join(format!("{id}.torrent"));
            ensure!(
                path.parent() == Some(metainfo_dir),
                "torrent raw metadata path is invalid"
            );
        }
    }
    for (origin, id) in &state.origins {
        ensure!(
            origin.len() <= 1_024 && state.tasks.contains_key(id),
            "torrent origin index is invalid"
        );
    }
    Ok(())
}

fn valid_task_id(task_id: &str) -> bool {
    task_id.len() == 32
        && task_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn create_private_directory_path(path: &Path) -> Result<PathBuf> {
    ensure!(
        path.is_absolute(),
        "torrent state directory must be absolute"
    );
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => current.push("/"),
            Component::Normal(value) => {
                current.push(value);
                match fs::symlink_metadata(&current) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        let expected_target = match current.as_path() {
                            path if path == Path::new("/tmp") => Path::new("/private/tmp"),
                            path if path == Path::new("/var") => Path::new("/private/var"),
                            _ => bail!("torrent state path contains a symlink"),
                        };
                        let resolved = fs::canonicalize(&current)
                            .context("failed to resolve a supported system path alias")?;
                        ensure!(
                            resolved == expected_target,
                            "torrent state system path alias has an unexpected target"
                        );
                        current = resolved;
                    }
                    Ok(metadata) => ensure!(
                        metadata.is_dir(),
                        "torrent state path contains a non-directory"
                    ),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        let mut builder = fs::DirBuilder::new();
                        builder.mode(0o700);
                        match builder.create(&current) {
                            Ok(()) => sync_parent(&current)?,
                            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                            Err(error) => {
                                return Err(error)
                                    .context("failed to create torrent state directory");
                            }
                        }
                        let metadata = fs::symlink_metadata(&current)
                            .context("failed to validate torrent state directory")?;
                        ensure!(
                            metadata.is_dir() && !metadata.file_type().is_symlink(),
                            "torrent state path contains a symlink"
                        );
                    }
                    Err(error) => {
                        return Err(error).context("failed to inspect torrent state directory");
                    }
                }
            }
            _ => bail!("torrent state directory path is invalid"),
        }
    }
    let metadata =
        fs::symlink_metadata(&current).context("failed to inspect torrent state directory")?;
    ensure!(
        metadata.is_dir() && metadata.uid() == unsafe { libc::geteuid() },
        "torrent state directory must belong to the current owner"
    );
    fs::set_permissions(&current, fs::Permissions::from_mode(0o700))
        .context("failed to make torrent state directory private")?;
    Ok(current)
}

fn ensure_private_directory_entry(path: &Path, provider: &dyn QueueFileProvider) -> Result<()> {
    let mut accessed = false;
    let mut ensure = |coordinated_path: &Path| -> Result<()> {
        match fs::symlink_metadata(coordinated_path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                match builder.create(coordinated_path) {
                    Ok(()) => sync_parent(coordinated_path)?,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => {
                        return Err(error).context("failed to create private torrent directory");
                    }
                }
            }
            Err(error) => return Err(error).context("failed to inspect private torrent directory"),
        }
        validate_private_directory(coordinated_path)?;
        accessed = true;
        Ok(())
    };
    provider.coordinate_write(path, &mut ensure)?;
    ensure!(
        accessed,
        "File Provider did not supply a coordinated torrent directory accessor"
    );
    Ok(())
}

fn validate_private_directory(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).context("failed to inspect private torrent directory")?;
    ensure!(
        metadata.is_dir()
            && !metadata.file_type().is_symlink()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.permissions().mode() & 0o7777 == 0o700,
        "torrent directory must be owner-private"
    );
    Ok(())
}

fn ensure_private_lock_file(path: &Path, provider: &dyn QueueFileProvider) -> Result<()> {
    let mut accessed = false;
    let mut ensure = |coordinated_path: &Path| -> Result<()> {
        match fs::symlink_metadata(coordinated_path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(coordinated_path)
                    .context("failed to create private torrent owner lock")?;
                file.sync_all()
                    .context("failed to sync torrent owner lock")?;
                sync_parent(coordinated_path)?;
            }
            Err(error) => return Err(error).context("failed to inspect torrent owner lock"),
        }
        let file = open_private_file(coordinated_path, true)?;
        validate_private_file(&file, 0o600)?;
        accessed = true;
        Ok(())
    };
    provider.coordinate_write(path, &mut ensure)?;
    ensure!(
        accessed,
        "File Provider did not supply a coordinated torrent lock accessor"
    );
    Ok(())
}

fn acquire_owner_lock(path: &Path, provider: &dyn QueueFileProvider) -> Result<File> {
    let mut lock = None;
    let mut open_and_lock = |coordinated_path: &Path| -> Result<()> {
        let file = open_private_file(coordinated_path, true)?;
        rustix::fs::flock(file.as_fd(), FlockOperation::NonBlockingLockExclusive)
            .map_err(|error| anyhow!(std::io::Error::from_raw_os_error(error.raw_os_error())))
            .context("torrent job state already has a live owner")?;
        validate_private_file(&file, 0o600)?;
        lock = Some(file);
        Ok(())
    };
    provider.coordinate_read(path, &mut open_and_lock)?;
    lock.ok_or_else(|| anyhow!("File Provider did not supply a coordinated torrent owner lock"))
}

fn open_private_file(path: &Path, writable: bool) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(writable)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .context("failed to open private torrent state file")?;
    validate_private_file(&file, 0o600)?;
    Ok(file)
}

fn validate_private_file(file: &File, mode: u32) -> Result<()> {
    let metadata = file
        .metadata()
        .context("failed to inspect private torrent state file")?;
    ensure!(
        metadata.is_file()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.permissions().mode() & 0o7777 == mode
            && metadata.nlink() == 1,
        "torrent state file must be a single-link owner-private regular file"
    );
    Ok(())
}

fn read_private_file(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
    let file = open_private_file(path, false)?;
    ensure!(
        file.metadata()?.len() <= max_bytes as u64,
        "private torrent file exceeds its byte limit"
    );
    let mut reader = file.take((max_bytes + 1) as u64);
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .context("failed to read private torrent file")?;
    ensure!(
        bytes.len() <= max_bytes,
        "private torrent file exceeds its byte limit"
    );
    Ok(bytes)
}

fn read_state(path: &Path, provider: &dyn QueueFileProvider) -> Result<Option<StoredState>> {
    let mut result = None;
    let mut read = |coordinated_path: &Path| -> Result<()> {
        let file = match open_private_file(coordinated_path, false) {
            Ok(file) => file,
            Err(error)
                if error.chain().any(|cause| {
                    cause
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
                }) =>
            {
                result = Some(None);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        ensure!(
            file.metadata()?.len() <= MAX_STATE_BYTES as u64,
            "torrent state file exceeds its read limit"
        );
        let mut reader = file.take((MAX_STATE_BYTES + 1) as u64);
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .context("failed to read torrent state")?;
        ensure!(
            bytes.len() <= MAX_STATE_BYTES,
            "torrent state file exceeds its read limit"
        );
        result = Some(Some(
            serde_json::from_slice(&bytes).context("failed to parse torrent state")?,
        ));
        Ok(())
    };
    provider.coordinate_read(path, &mut read)?;
    result
        .ok_or_else(|| anyhow!("File Provider did not supply a coordinated torrent state accessor"))
}

fn write_state_atomic(
    path: &Path,
    state: &StoredState,
    provider: &dyn QueueFileProvider,
) -> Result<()> {
    let bytes = serde_json::to_vec(state).context("failed to encode torrent state")?;
    ensure!(
        bytes.len() <= MAX_STATE_BYTES,
        "torrent state exceeds its serialized size limit"
    );
    let mut written = false;
    let mut write = |coordinated_path: &Path| -> Result<()> {
        validate_existing_state_path(coordinated_path)?;
        write_file_atomic(coordinated_path, &bytes, 0o600)?;
        written = true;
        Ok(())
    };
    provider.coordinate_write(path, &mut write)?;
    ensure!(
        written,
        "File Provider did not supply a coordinated torrent state writer"
    );
    Ok(())
}

fn validate_existing_state_path(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                !metadata.file_type().is_symlink()
                    && metadata.is_file()
                    && metadata.uid() == unsafe { libc::geteuid() }
                    && metadata.permissions().mode() & 0o7777 == 0o600
                    && metadata.nlink() == 1,
                "existing torrent state file is not owner-private"
            );
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("failed to inspect existing torrent state"),
    }
}

fn write_file_atomic(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("private torrent file has no parent"))?;
    let mut opened = None;
    let mut temporary = None;
    for _ in 0..8 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let candidate = parent.join(format!(
            ".torrent-{}.{}.{}.tmp",
            std::process::id(),
            nonce,
            sequence
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&candidate)
        {
            Ok(file) => {
                opened = Some(file);
                temporary = Some(candidate);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).context("failed to create temporary torrent state file");
            }
        }
    }
    let temporary =
        temporary.ok_or_else(|| anyhow!("could not allocate a unique torrent state temp"))?;
    let mut file = opened.ok_or_else(|| anyhow!("torrent state temp file was not opened"))?;
    let result = (|| -> Result<()> {
        file.write_all(bytes)
            .context("failed to write torrent state file")?;
        file.sync_all()
            .context("failed to sync torrent state file")?;
        validate_private_file(&file, mode)?;
        drop(file);
        fs::rename(&temporary, path).context("failed to replace torrent state file")?;
        sync_parent(path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)
            .context("failed to open torrent state directory for sync")?
            .sync_all()
            .context("failed to sync torrent state directory")?;
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
#[path = "torrent_manager_tests.rs"]
mod tests;
