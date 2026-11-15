//! Application adapters for the durable torrent worker and its private Telegram card.

use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::sync::{Notify, OnceCell, OwnedSemaphorePermit, Semaphore, oneshot, watch};

use crate::music_runtime::MusicService;
use crate::music_types::{LocalMusicBatch, LocalMusicFile, MusicCancellation};
use crate::qbittorrent::QbittorrentClient;
use crate::telegram::TelegramClient;
use crate::torrent_manager::{TorrentManager, TorrentMiniAppConfirm};
use crate::torrent_types::{
    CompletedTorrentFile, SubmitResult, TorrentAction, TorrentBackend, TorrentEnqueueResult,
    TorrentExisting, TorrentFuture, TorrentIdentity, TorrentMagnet, TorrentMusicJobRef,
    TorrentMusicJobStatus, TorrentMusicStage, TorrentPhase, TorrentRemoteSnapshot, TorrentSettings,
    TorrentSnapshot, TorrentUpload,
};
use crate::torrent_widget::{
    TorrentWidgetPresentation, TorrentWidgetView, WidgetEntry, WidgetMusicStage,
    render_torrent_widget,
};

/// Torrent metadata startup and local state writes never hold the Telegram receiver thread.
pub struct TorrentService {
    manager: OnceCell<Arc<TorrentManager>>,
    ready: Notify,
    started: AtomicBool,
    settings: TorrentSettings,
    backend: Arc<dyn TorrentBackend>,
    state_operations: Arc<Semaphore>,
    shutdown: watch::Receiver<bool>,
}

impl TorrentService {
    #[cfg(test)]
    pub fn new(
        settings: TorrentSettings,
        telegram: TelegramClient,
        music: Option<Arc<MusicService>>,
        music_max_bytes: u64,
        shutdown: watch::Receiver<bool>,
    ) -> Result<Arc<Self>> {
        Self::new_with_miniapp_worker_url(
            settings,
            telegram,
            music,
            music_max_bytes,
            shutdown,
            None,
        )
    }

    pub(crate) fn new_with_miniapp_worker_url(
        settings: TorrentSettings,
        telegram: TelegramClient,
        music: Option<Arc<MusicService>>,
        music_max_bytes: u64,
        shutdown: watch::Receiver<bool>,
        miniapp_worker_url: Option<String>,
    ) -> Result<Arc<Self>> {
        let backend = Arc::new(RuntimeTorrentBackend {
            qbit: QbittorrentClient::new(&settings)?,
            telegram,
            music,
            music_max_bytes,
            max_torrent_bytes: settings.max_torrent_bytes,
            metainfo_dir: settings.metainfo_dir.clone(),
            metainfo_readers: Arc::new(Semaphore::new(1)),
            shutdown: shutdown.clone(),
            miniapp_worker_url,
        });
        Ok(Self::with_backend(settings, backend, shutdown))
    }

    pub(crate) fn with_backend(
        settings: TorrentSettings,
        backend: Arc<dyn TorrentBackend>,
        shutdown: watch::Receiver<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            manager: OnceCell::new(),
            ready: Notify::new(),
            started: AtomicBool::new(false),
            settings,
            backend,
            state_operations: Arc::new(Semaphore::new(4)),
            shutdown,
        })
    }

    pub fn start(self: &Arc<Self>) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let mut shutdown = service.shutdown.clone();
            loop {
                if *shutdown.borrow() {
                    return;
                }
                let settings = service.settings.clone();
                let backend = Arc::clone(&service.backend);
                let (sender, receiver) = oneshot::channel();
                if std::thread::Builder::new()
                    .name("torrent-state-startup".into())
                    .spawn(move || {
                        let _ = sender.send(TorrentManager::open(settings, backend));
                    })
                    .is_err()
                {
                    tracing::warn!("could not start torrent state worker");
                    return;
                }
                let opened = tokio::select! {
                    _ = shutdown.changed() => return,
                    opened = receiver => opened,
                };
                match opened {
                    Ok(Ok(manager)) => {
                        if service.manager.set(Arc::clone(&manager)).is_ok() {
                            manager.start(service.shutdown.clone());
                        }
                        service.ready.notify_waiters();
                        return;
                    }
                    _ => tracing::warn!(
                        "torrent state unavailable; retaining tasks for startup retry"
                    ),
                }
                tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {},
                }
            }
        });
    }

    async fn manager(&self) -> Result<Arc<TorrentManager>> {
        let ready = self.ready.notified();
        if let Some(manager) = self.manager.get() {
            return Ok(Arc::clone(manager));
        }
        tokio::time::timeout(Duration::from_secs(5), ready)
            .await
            .context("torrent queue is not ready; message remains pending")?;
        self.manager
            .get()
            .cloned()
            .context("torrent queue is not ready")
    }

    pub async fn enqueue(&self, upload: TorrentUpload) -> Result<TorrentEnqueueResult> {
        let manager = self.manager().await?;
        let permit = self.reserve_state_operation()?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            manager.enqueue(upload)
        })
        .await
        .context("torrent admission worker stopped")?
    }

    pub async fn enqueue_magnet(&self, magnet: TorrentMagnet) -> Result<TorrentEnqueueResult> {
        let manager = self.manager().await?;
        let permit = self.reserve_state_operation()?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            manager.enqueue_magnet(magnet)
        })
        .await
        .context("magnet admission worker stopped")?
    }

    pub async fn apply_action(
        &self,
        chat_id: i64,
        user_id: i64,
        card_message_id: i64,
        task_id: String,
        expected_revision: u64,
        action: TorrentAction,
    ) -> Result<TorrentSnapshot> {
        let manager = self.manager().await?;
        let permit = self.reserve_state_operation()?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            manager.apply_action(
                chat_id,
                user_id,
                card_message_id,
                &task_id,
                expected_revision,
                action,
            )
        })
        .await
        .context("torrent selection worker stopped")?
    }

    pub async fn list(&self, chat_id: i64) -> Result<Vec<TorrentSnapshot>> {
        let manager = self.manager().await?;
        let permit = self.reserve_state_operation()?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            manager.list(chat_id)
        })
        .await
        .context("torrent listing worker stopped")?
    }

    pub async fn reopen_card(
        &self,
        chat_id: i64,
        user_id: i64,
        task_id: String,
        request_key: String,
    ) -> Result<TorrentSnapshot> {
        let manager = self.manager().await?;
        let permit = self.reserve_state_operation()?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            manager.reopen_card(chat_id, user_id, &task_id, request_key)
        })
        .await
        .context("torrent card worker stopped")?
    }

    /// Read one bounded page of qBittorrent task summaries for the private catalog.
    pub async fn list_torrents(
        &self,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TorrentRemoteSnapshot>> {
        self.backend.list_torrents(offset, limit).await
    }

    /// Read back one exact task without exposing qBittorrent paths to the UI.
    pub async fn remote_snapshot(&self, hash: String) -> Result<TorrentRemoteSnapshot> {
        self.backend.task_summary(hash).await
    }

    /// Issue one pause or resume request. A caller must read back the result and must not retry
    /// when the transport outcome is uncertain.
    pub async fn set_active(&self, hash: String, active: bool) -> Result<()> {
        self.backend.set_active(hash, active).await
    }

    /// Dispatch one strictly validated private Mini App action.
    pub async fn handle_miniapp_action(
        &self,
        owner_chat: i64,
        request_identity: &str,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        crate::torrent_miniapp::handle_action(self, owner_chat, request_identity, payload).await
    }

    pub(crate) async fn miniapp_detail(
        &self,
        owner_chat: i64,
        task_id: String,
    ) -> Result<TorrentSnapshot> {
        let manager = self.manager().await?;
        let permit = self.reserve_state_operation()?;
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            runtime.block_on(manager.miniapp_refresh_task(owner_chat, owner_chat, &task_id))
        })
        .await
        .context("Mini App torrent detail worker stopped")?
    }

    pub(crate) async fn miniapp_review_context(
        &self,
        owner_chat: i64,
        task_id: String,
        inventory_revision: u64,
        selection_revision: u64,
        selected_indexes: Vec<u32>,
    ) -> Result<crate::torrent_manager::TorrentMiniAppReviewContext> {
        let manager = self.manager().await?;
        let permit = self.reserve_state_operation()?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            manager.miniapp_review_context(
                owner_chat,
                owner_chat,
                &task_id,
                inventory_revision,
                selection_revision,
                &selected_indexes,
            )
        })
        .await
        .context("Mini App torrent review worker stopped")?
    }

    pub(crate) async fn miniapp_adopt(
        &self,
        owner_chat: i64,
        hash: String,
    ) -> Result<TorrentSnapshot> {
        let manager = self.manager().await?;
        let permit = self.reserve_state_operation()?;
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            runtime.block_on(manager.miniapp_adopt_existing(owner_chat, owner_chat, hash))
        })
        .await
        .context("Mini App torrent adoption worker stopped")?
    }

    pub(crate) async fn miniapp_confirm(
        &self,
        owner_chat: i64,
        confirmation: TorrentMiniAppConfirm,
    ) -> Result<crate::torrent_manager::TorrentMiniAppConfirmResult> {
        let manager = self.manager().await?;
        let permit = self.reserve_state_operation()?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            manager.miniapp_confirm_selection(owner_chat, owner_chat, confirmation)
        })
        .await
        .context("Mini App torrent confirmation worker stopped")?
    }

    pub(crate) async fn miniapp_control(&self, hash: String, active: bool) -> Result<()> {
        ensure_miniapp_hash(&hash)?;
        let hash = hash.to_ascii_lowercase();
        let before = self.backend.task_summary(hash.clone()).await?;
        anyhow::ensure!(
            before.hash.eq_ignore_ascii_case(&hash),
            "qBittorrent returned a different torrent hash"
        );
        self.backend.set_active(hash.clone(), active).await?;
        let after = self.backend.task_summary(hash.clone()).await?;
        anyhow::ensure!(
            after.hash.eq_ignore_ascii_case(&hash),
            "qBittorrent returned a different torrent hash after control"
        );
        let paused = torrent_state_is_paused(&after.state);
        anyhow::ensure!(
            paused == Some(!active),
            "qBittorrent control outcome is uncertain; refresh the catalog before retrying"
        );
        Ok(())
    }

    /// Adopt an already-existing task. Manager filesystem work runs on a blocking thread while
    /// the state permit remains owned through completion.
    pub async fn adopt_existing(
        &self,
        source: TorrentExisting,
        request_key: String,
    ) -> Result<TorrentEnqueueResult> {
        let manager = self.manager().await?;
        let permit = self.reserve_state_operation()?;
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            runtime.block_on(manager.adopt_existing(source, request_key))
        })
        .await
        .context("torrent catalog adoption worker stopped")?
    }

    fn reserve_state_operation(&self) -> Result<OwnedSemaphorePermit> {
        // A timed-out Widget answer must not release this permit while its blocking
        // filesystem operation is still alive. The closure owns it until completion.
        Arc::clone(&self.state_operations)
            .try_acquire_owned()
            .context("torrent state is busy; existing operations are still pending")
    }
}

fn ensure_miniapp_hash(hash: &str) -> Result<()> {
    anyhow::ensure!(
        matches!(hash.len(), 40 | 64) && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "qBittorrent hash is invalid"
    );
    Ok(())
}

fn torrent_state_is_paused(state: &str) -> Option<bool> {
    let state = state.to_ascii_lowercase();
    if state.starts_with("paused") || state.starts_with("stopped") {
        Some(true)
    } else if state.starts_with("downloading")
        || state.starts_with("uploading")
        || state.starts_with("stalledup")
        || state.starts_with("stalleddl")
        || state.starts_with("queued")
        || state.starts_with("checking")
        || state.starts_with("allocating")
        || state.starts_with("moving")
    {
        Some(false)
    } else {
        None
    }
}

struct RuntimeTorrentBackend {
    qbit: QbittorrentClient,
    telegram: TelegramClient,
    music: Option<Arc<MusicService>>,
    music_max_bytes: u64,
    max_torrent_bytes: usize,
    metainfo_dir: Option<PathBuf>,
    metainfo_readers: Arc<Semaphore>,
    shutdown: watch::Receiver<bool>,
    miniapp_worker_url: Option<String>,
}

impl TorrentBackend for RuntimeTorrentBackend {
    fn download_torrent(&self, upload: TorrentUpload) -> TorrentFuture<'_, Vec<u8>> {
        Box::pin(async move {
            self.telegram
                .download_small_file(&upload.file_id, self.max_torrent_bytes)
                .await
        })
    }

    fn lookup(&self, identity: TorrentIdentity) -> TorrentFuture<'_, Vec<TorrentRemoteSnapshot>> {
        Box::pin(async move { self.qbit.lookup(&identity).await })
    }

    fn submit(&self, bytes: Vec<u8>, name: String) -> TorrentFuture<'_, SubmitResult> {
        Box::pin(async move { self.qbit.submit(bytes, name).await })
    }

    fn default_save_path(&self) -> TorrentFuture<'_, PathBuf> {
        Box::pin(async move { self.qbit.default_save_path().await })
    }

    fn submit_magnet(&self, uri: String, save_path: PathBuf) -> TorrentFuture<'_, SubmitResult> {
        Box::pin(async move { self.qbit.submit_magnet(uri, save_path).await })
    }

    fn export_metainfo(&self, hash: String) -> TorrentFuture<'_, Vec<u8>> {
        Box::pin(async move {
            match self.qbit.export_metainfo(&hash).await {
                Ok(bytes) => Ok(bytes),
                Err(error) => {
                    let Some(directory) = self.metainfo_dir.clone() else {
                        return Err(error);
                    };
                    let permit = Arc::clone(&self.metainfo_readers)
                        .try_acquire_owned()
                        .context("local metainfo reader is busy")?;
                    let max_bytes = self.max_torrent_bytes;
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        crate::torrent_backup::read_metainfo(&directory, &hash, max_bytes)
                    })
                    .await
                    .context("local metainfo reader stopped")?
                }
            }
        })
    }

    fn refresh(&self, hash: String) -> TorrentFuture<'_, TorrentRemoteSnapshot> {
        Box::pin(async move { self.qbit.refresh(&hash).await })
    }

    fn task_summary(&self, hash: String) -> TorrentFuture<'_, TorrentRemoteSnapshot> {
        Box::pin(async move { self.qbit.task_summary(&hash).await })
    }

    fn list_torrents(
        &self,
        offset: u32,
        limit: u32,
    ) -> TorrentFuture<'_, Vec<TorrentRemoteSnapshot>> {
        Box::pin(async move { self.qbit.list_torrents(offset, limit).await })
    }

    fn set_active(&self, hash: String, active: bool) -> TorrentFuture<'_, ()> {
        Box::pin(async move { self.qbit.set_active(&hash, active).await })
    }

    fn send_card(&self, snapshot: TorrentSnapshot) -> TorrentFuture<'_, i64> {
        Box::pin(async move {
            let card = crate::torrent_inline::render_torrent_inline_with_miniapp(
                &snapshot,
                self.miniapp_worker_url.as_deref(),
            )?;
            self.telegram
                .send_message_with_inline_keyboard(snapshot.chat_id, card.text, card.reply_markup)
                .await
        })
    }

    fn update_card(&self, snapshot: TorrentSnapshot) -> TorrentFuture<'_, ()> {
        Box::pin(async move {
            let message_id = snapshot
                .card_message_id
                .context("torrent card is not registered")?;
            let card = crate::torrent_inline::render_torrent_inline_with_miniapp(
                &snapshot,
                self.miniapp_worker_url.as_deref(),
            )?;
            self.telegram
                .edit_message_text_with_inline_keyboard(
                    snapshot.chat_id,
                    message_id,
                    card.text,
                    card.reply_markup,
                )
                .await
        })
    }

    fn enqueue_music(
        &self,
        chat_id: i64,
        user_id: i64,
        batch_id: String,
        files: Vec<CompletedTorrentFile>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        Box::pin(async move {
            let music = self.music.as_ref().context("Music imports are disabled")?;
            if *self.shutdown.borrow() {
                bail!("torrent worker is stopping");
            }
            let cancellation = MusicCancellation::default();
            let capture_cancel = cancellation.clone();
            let max_bytes = self.music_max_bytes;
            let captured = tokio::task::spawn_blocking(move || {
                files
                    .into_iter()
                    .map(|file| {
                        let captured = LocalMusicFile::capture(
                            file.source_key,
                            file.source_root,
                            file.relative_path,
                            max_bytes,
                            &capture_cancel,
                        )?;
                        if captured.file_size != file.expected_size {
                            bail!("completed torrent file size changed before Music admission");
                        }
                        Ok(captured)
                    })
                    .collect::<Result<Vec<_>>>()
            });
            let mut shutdown = self.shutdown.clone();
            let files = tokio::select! {
                result = captured => result.context("local audio capture worker stopped")??,
                _ = shutdown.changed() => {
                    cancellation.cancel();
                    bail!("torrent worker stopped during local audio capture");
                }
            };
            let admitted = music
                .enqueue_local_batch(LocalMusicBatch {
                    chat_id,
                    user_id,
                    batch_id,
                    files,
                })
                .await?;
            Ok(admitted.into_iter().map(Into::into).collect())
        })
    }

    fn lookup_music_jobs(
        &self,
        chat_id: i64,
        user_id: i64,
        source_keys: Vec<String>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobRef>> {
        Box::pin(async move {
            let music = self.music.as_ref().context("Music imports are disabled")?;
            Ok(music
                .lookup_local_jobs(chat_id, user_id, source_keys)
                .await?
                .into_iter()
                .map(Into::into)
                .collect())
        })
    }

    fn music_status(
        &self,
        chat_id: i64,
        job_ids: Vec<String>,
    ) -> TorrentFuture<'_, Vec<TorrentMusicJobStatus>> {
        Box::pin(async move {
            let music = self.music.as_ref().context("Music imports are disabled")?;
            Ok(music
                .local_job_status(chat_id, &job_ids)
                .await?
                .into_iter()
                .map(Into::into)
                .collect())
        })
    }

    fn retry_music(&self, chat_id: i64, job_ids: Vec<String>) -> TorrentFuture<'_, ()> {
        Box::pin(async move {
            let music = self.music.as_ref().context("Music imports are disabled")?;
            let statuses = music.local_job_status(chat_id, &job_ids).await?;
            for status in statuses {
                if matches!(
                    status.stage,
                    crate::music_types::JobStage::Failed | crate::music_types::JobStage::Cancelled
                ) {
                    music.retry(chat_id, &status.job_id).await?;
                }
            }
            Ok(())
        })
    }
}

pub(crate) fn card_presentation(snapshot: &TorrentSnapshot) -> Result<TorrentWidgetPresentation> {
    render_torrent_widget(&widget_view(snapshot)).map_err(Into::into)
}

pub(crate) fn widget_view(snapshot: &TorrentSnapshot) -> TorrentWidgetView {
    let folder = snapshot
        .folders
        .iter()
        .find(|folder| folder.id == snapshot.cursor_folder_id);
    let current = folder
        .map(|folder| folder.relative_path.as_path())
        .unwrap_or_else(|| Path::new(""));
    let parent = (!current.as_os_str().is_empty()).then(|| {
        let parent = current.parent().unwrap_or_else(|| Path::new(""));
        snapshot
            .folders
            .iter()
            .find(|folder| folder.relative_path == parent)
            .map(|folder| folder.id)
            .unwrap_or(0)
    });
    let folders = snapshot
        .folders
        .iter()
        .filter(|folder| {
            folder.relative_path != current
                && folder
                    .relative_path
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    == current
        })
        .collect::<Vec<_>>();
    let files = snapshot
        .files
        .iter()
        .filter(|file| file.relative_path.parent().unwrap_or_else(|| Path::new("")) == current)
        .collect::<Vec<_>>();
    let pages = (folders.len() + files.len()).max(1).div_ceil(4);
    let page = (snapshot.cursor_page as usize).min(pages - 1);
    let start = page * 4;
    let mut entries = Vec::with_capacity(4);
    // Aggregate folder details only for the four visible rows, even in a 5000-file task.
    for folder in folders.iter().skip(start).take(4) {
        let descendants = snapshot
            .files
            .iter()
            .filter(|file| file.relative_path.starts_with(&folder.relative_path))
            .collect::<Vec<_>>();
        entries.push(WidgetEntry::Folder {
            id: folder.id,
            title: folder
                .relative_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned(),
            file_count: u32::try_from(descendants.len()).unwrap_or(u32::MAX),
            supported_music_count: u32::try_from(
                descendants
                    .iter()
                    .filter(|file| file.supported_music)
                    .count(),
            )
            .unwrap_or(u32::MAX),
            selected_count: u32::try_from(descendants.iter().filter(|file| file.selected).count())
                .unwrap_or(u32::MAX),
        });
    }
    let remaining = 4 - entries.len();
    for file in files
        .iter()
        .skip(start.saturating_sub(folders.len()))
        .take(remaining)
    {
        entries.push(WidgetEntry::File {
            index: file.index,
            title: file
                .relative_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned(),
            size_bytes: file.size,
            download_percent: progress_percent(file.progress),
            complete: file.complete,
            supported_music: file.supported_music,
            selected: file.selected,
            music_stage: file.music_stage.as_ref().map(|stage| match stage {
                TorrentMusicStage::Pending => WidgetMusicStage::Pending,
                TorrentMusicStage::Complete => WidgetMusicStage::Complete,
                TorrentMusicStage::Failed => WidgetMusicStage::Failed,
                TorrentMusicStage::Cancelled => WidgetMusicStage::Cancelled,
                TorrentMusicStage::OutcomeUnknown => WidgetMusicStage::OutcomeUnknown,
            }),
        });
    }
    let total_bytes = snapshot
        .files
        .iter()
        .fold(0u64, |sum, file| sum.saturating_add(file.size));
    let imported = snapshot
        .files
        .iter()
        .filter(|file| matches!(file.music_stage, Some(TorrentMusicStage::Complete)))
        .count();
    let failed = snapshot
        .files
        .iter()
        .filter(|file| {
            matches!(
                file.music_stage,
                Some(
                    TorrentMusicStage::Failed
                        | TorrentMusicStage::Cancelled
                        | TorrentMusicStage::OutcomeUnknown
                )
            )
        })
        .count();
    TorrentWidgetView {
        task_id: snapshot.task_id.clone(),
        inventory_revision: snapshot.inventory_revision,
        selection_revision: snapshot.selection_revision,
        render_revision: snapshot.render_revision,
        title: snapshot.name.clone(),
        status: if snapshot.files.iter().any(|file| file.needs_reconfirm) {
            "选择或文件列表已变化，请重新确认导入".into()
        } else {
            phase_label(&snapshot.phase).into()
        },
        progress_percent: Some(progress_percent(snapshot.progress)),
        completed_bytes: Some(((total_bytes as f64) * snapshot.progress.clamp(0.0, 1.0)) as u64),
        total_bytes: Some(total_bytes),
        folder_id: snapshot.cursor_folder_id,
        folder_name: if current.as_os_str().is_empty() {
            "全部文件".into()
        } else {
            current.to_string_lossy().into_owned()
        },
        parent_folder_id: parent,
        page: u32::try_from(page).unwrap_or(0),
        page_count: u32::try_from(pages).unwrap_or(u32::MAX),
        entries,
        selected_count: u32::try_from(snapshot.selected_count).unwrap_or(u32::MAX),
        import_pending_count: u32::try_from(
            snapshot.import_count.saturating_sub(imported + failed),
        )
        .unwrap_or(u32::MAX),
        import_completed_count: u32::try_from(imported).unwrap_or(u32::MAX),
        import_failed_count: u32::try_from(failed).unwrap_or(u32::MAX),
        // Keep local paths and backend diagnostic details out of Telegram widget state.
        error: snapshot
            .error
            .as_ref()
            .map(|_| "操作尚未完成，请刷新任务或重试失败导入。".into()),
    }
}

fn progress_percent(progress: f64) -> u8 {
    if progress.is_finite() {
        (progress.clamp(0.0, 1.0) * 100.0).floor() as u8
    } else {
        0
    }
}

#[cfg(test)]
#[path = "torrent_e2e_tests.rs"]
mod e2e_tests;

pub(crate) fn phase_label(phase: &TorrentPhase) -> &'static str {
    match phase {
        TorrentPhase::Queued => "已加入队列",
        TorrentPhase::DownloadingMetainfo => "读取种子",
        TorrentPhase::CheckingExisting => "核对现有下载",
        TorrentPhase::Submitting => "提交到 qBittorrent",
        TorrentPhase::ReconciliationPending | TorrentPhase::Ambiguous => "正在核对提交结果",
        TorrentPhase::Downloading => "下载中",
        TorrentPhase::Completed => "下载完成，可选择音乐",
        TorrentPhase::SelectingFiles => "选择要导入的音乐",
        TorrentPhase::ImportRequested => "等待所选文件下载完整",
        TorrentPhase::Importing => "导入 Music 中",
        TorrentPhase::Imported => "所选音乐已导入",
        TorrentPhase::Failed => "任务暂未完成",
        TorrentPhase::Invalid => "种子无法处理",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent_types::{TorrentFileSnapshot, TorrentFolder};
    use std::path::PathBuf;

    fn snapshot() -> TorrentSnapshot {
        TorrentSnapshot {
            task_id: "a".repeat(32),
            chat_id: 123456789,
            user_id: 123456789,
            upload_message_id: 1,
            card_message_id: Some(2),
            inventory_revision: 4,
            selection_revision: 2,
            render_revision: 8,
            name: "Selected album".into(),
            identity: None,
            phase: TorrentPhase::SelectingFiles,
            state: Some("uploading".into()),
            progress: 1.0,
            cursor_folder_id: 0,
            cursor_page: 0,
            review_selection: false,
            folders: vec![TorrentFolder {
                id: 0,
                relative_path: PathBuf::new(),
                file_count: 0,
            }],
            files: Vec::new(),
            selected_count: 0,
            import_count: 0,
            error: None,
        }
    }

    fn file(index: u32, path: &str) -> TorrentFileSnapshot {
        TorrentFileSnapshot {
            index,
            relative_path: path.into(),
            size: 1024,
            progress: 1.0,
            complete: true,
            supported_music: true,
            selected: false,
            needs_reconfirm: false,
            music_stage: None,
        }
    }

    #[test]
    fn widget_pages_all_entries_and_folder_selection_counts_supported_tracks() {
        let mut source = snapshot();
        source.folders.push(TorrentFolder {
            id: 1,
            relative_path: "Album".into(),
            file_count: 2,
        });
        source.files = (0..5)
            .map(|index| file(index, &format!("root-{index}.flac")))
            .collect();
        let mut selected = file(6, "Album/音楽.flac");
        selected.selected = true;
        source.files.push(selected);
        let mut cover = file(7, "Album/cover.jpg");
        cover.supported_music = false;
        source.files.push(cover);
        let first = widget_view(&source);
        assert_eq!(first.page_count, 2);
        assert_eq!(first.entries.len(), 4);
        assert!(matches!(
            &first.entries[0],
            WidgetEntry::Folder {
                id: 1,
                file_count: 2,
                supported_music_count: 1,
                selected_count: 1,
                ..
            }
        ));
        source.cursor_page = 1;
        let second = widget_view(&source);
        assert_eq!(second.entries.len(), 2);
        assert!(matches!(
            &second.entries[0],
            WidgetEntry::File { index: 3, .. }
        ));
        source.cursor_folder_id = 1;
        source.cursor_page = 0;
        let folder = widget_view(&source);
        assert_eq!(folder.parent_folder_id, Some(0));
        assert_eq!(folder.entries.len(), 2);
        assert!(matches!(
            &folder.entries[1],
            WidgetEntry::File {
                supported_music: false,
                ..
            }
        ));
    }

    #[test]
    fn widget_mapping_keeps_local_diagnostics_out_of_the_public_card() {
        let mut source = snapshot();
        source.error = Some("cannot open /Users/private-owner/sensitive/download.flac".into());
        let mut completed = file(0, "日本語/曲目.flac");
        completed.music_stage = Some(TorrentMusicStage::Complete);
        source.files.push(completed);
        source.import_count = 1;
        let view = widget_view(&source);
        assert_eq!(view.import_completed_count, 1);
        assert_eq!(view.import_pending_count, 0);
        assert!(!view.error.as_deref().unwrap().contains("/Users/"));
        let card = card_presentation(&source).expect("bounded card should render");
        assert!(!card.chat_data.contains("sensitive"));
        assert!(card.chat_data.len() <= 1024);
    }
}
