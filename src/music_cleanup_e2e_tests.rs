use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::music_manager::tests::{TestDirectory, TestHttpServer, open_manager, upload};
use crate::music_staging_cleanup::{cleanup_staging_tree, identify_regular_file};
use crate::music_types::{
    ArchiveState, AudioBytes, MusicBackend, MusicCancellation, MusicSettings, MusicTrack,
    MusicUpload, PreparedAudio,
};

const ORIGINAL_BYTES: &[u8] = b"synthetic FLAC fixture; not playable audio";
const PREPARED_BYTES: &[u8] = b"synthetic converted cache; not playable audio";
const PARTIAL_BYTES: &[u8] = b"synthetic interrupted download";
const MUSIC_PID: &str = "0123456789ABCDEF";

struct FilesystemCleanupBackend {
    staging_root: PathBuf,
    archive_root: PathBuf,
    music_copy: PathBuf,
    downloads: AtomicUsize,
    preparations: AtomicUsize,
    archives: AtomicUsize,
    adds: AtomicUsize,
    cleanups: AtomicUsize,
    native_cleanups: AtomicUsize,
    archive_failure: AtomicBool,
    cancel_download: AtomicBool,
    download_started: AtomicBool,
    cleanup_failures_remaining: AtomicUsize,
    archive_local_bytes: AtomicBool,
}

impl FilesystemCleanupBackend {
    fn new(settings: &MusicSettings, cleanup_failures: usize) -> Arc<Self> {
        Arc::new(Self {
            staging_root: settings.staging_dir.clone(),
            archive_root: settings.archive_dir.clone(),
            music_copy: settings
                .state_dir
                .parent()
                .expect("state dir should have a parent")
                .join("mock-music-library")
                .join("accepted-copy.m4a"),
            downloads: AtomicUsize::new(0),
            preparations: AtomicUsize::new(0),
            archives: AtomicUsize::new(0),
            adds: AtomicUsize::new(0),
            cleanups: AtomicUsize::new(0),
            native_cleanups: AtomicUsize::new(0),
            archive_failure: AtomicBool::new(false),
            cancel_download: AtomicBool::new(false),
            download_started: AtomicBool::new(false),
            cleanup_failures_remaining: AtomicUsize::new(cleanup_failures),
            archive_local_bytes: AtomicBool::new(true),
        })
    }

    fn fail_cleanup_once(&self) -> bool {
        let mut remaining = self.cleanup_failures_remaining.load(Ordering::Acquire);
        loop {
            if remaining == 0 {
                return false;
            }
            match self.cleanup_failures_remaining.compare_exchange(
                remaining,
                remaining - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(updated) => remaining = updated,
            }
        }
    }
}

impl MusicBackend for FilesystemCleanupBackend {
    fn download(
        &self,
        _upload: &MusicUpload,
        destination: &Path,
        cancel: &MusicCancellation,
    ) -> Result<()> {
        self.downloads.fetch_add(1, Ordering::Relaxed);
        if self.cancel_download.load(Ordering::Acquire) {
            write_private(destination, PARTIAL_BYTES)?;
            self.download_started.store(true, Ordering::Release);
            while !cancel.is_cancelled() {
                thread::sleep(Duration::from_millis(5));
            }
            bail!("synthetic download cancelled after a partial write");
        }
        if cancel.is_cancelled() {
            bail!("synthetic download cancelled before writing");
        }
        write_private(destination, ORIGINAL_BYTES)
    }

    fn prepare(
        &self,
        original: &Path,
        working_dir: &Path,
        cancel: &MusicCancellation,
    ) -> Result<PreparedAudio> {
        self.preparations.fetch_add(1, Ordering::Relaxed);
        if cancel.is_cancelled() {
            bail!("synthetic preparation cancelled");
        }
        let original_bytes = fs::read(original)?;
        let import_path = working_dir.join("converted-cache.m4a");
        write_private(&import_path, PREPARED_BYTES)?;
        Ok(PreparedAudio {
            original: audio_bytes(original.to_path_buf(), &original_bytes),
            import: audio_bytes(import_path, PREPARED_BYTES),
            conversion: None,
        })
    }

    fn archive(&self, audio: &PreparedAudio, original_name: &str) -> Result<ArchiveState> {
        self.archives.fetch_add(1, Ordering::Relaxed);
        if self.archive_failure.load(Ordering::Acquire) {
            bail!("synthetic archive failure after preparation");
        }
        let path = self.archive_root.join(original_name);
        write_private(&path, &fs::read(&audio.original.path)?)?;
        self.archive_local_bytes.store(true, Ordering::Release);
        Ok(ArchiveState {
            path,
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: true,
        })
    }

    fn add_track(&self, audio: &PreparedAudio) -> Result<String> {
        self.adds.fetch_add(1, Ordering::Relaxed);
        fs::create_dir_all(
            self.music_copy
                .parent()
                .context("mock Music directory should exist in the path")?,
        )?;
        fs::set_permissions(
            self.music_copy
                .parent()
                .expect("Music path should have a parent"),
            fs::Permissions::from_mode(0o700),
        )?;
        write_private(&self.music_copy, &fs::read(&audio.import.path)?)?;
        Ok(MUSIC_PID.to_string())
    }

    fn query_track(&self, persistent_id: &str) -> Result<MusicTrack> {
        Ok(MusicTrack {
            persistent_id: persistent_id.to_string(),
            location: Some(self.music_copy.clone()),
            cloud_status: "uploaded".to_string(),
        })
    }

    fn verify_copy(&self, audio: &PreparedAudio, track: &MusicTrack) -> Result<()> {
        if track.persistent_id != MUSIC_PID {
            bail!("unexpected synthetic Music persistent ID");
        }
        let location = track
            .location
            .as_deref()
            .context("synthetic Music track has no file location")?;
        let bytes = fs::read(location)?;
        if bytes.len() as u64 != audio.import.size || sha256(&bytes) != audio.import.sha256 {
            bail!("synthetic Music copy does not match the catalog import hash");
        }
        Ok(())
    }

    fn archive_status(&self, path: &Path) -> Result<ArchiveState> {
        Ok(ArchiveState {
            path: path.to_path_buf(),
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: self.archive_local_bytes.load(Ordering::Acquire),
        })
    }

    fn evict_archive(&self, archive: &ArchiveState, _track: &MusicTrack) -> Result<ArchiveState> {
        self.archive_local_bytes.store(false, Ordering::Release);
        Ok(ArchiveState {
            local_bytes: false,
            ..archive.clone()
        })
    }

    fn cleanup_staging(
        &self,
        job_id: &str,
        _audio: &PreparedAudio,
        archive: &ArchiveState,
        track: &MusicTrack,
    ) -> Result<()> {
        self.cleanups.fetch_add(1, Ordering::Relaxed);
        if self.fail_cleanup_once() {
            bail!("synthetic one-shot cleanup failure");
        }
        let music_path = track
            .location
            .as_deref()
            .context("synthetic Music track has no protected path")?;
        let music_identity = identify_regular_file(music_path)?;
        let archive_identity = identify_regular_file(&archive.path)?;
        self.native_cleanups.fetch_add(1, Ordering::Relaxed);
        cleanup_staging_tree(
            &self.staging_root,
            job_id,
            &music_identity,
            &archive_identity,
        )
    }
}

fn flac_upload(message_id: i64, file_unique_id: &str) -> MusicUpload {
    let mut upload = upload(message_id, file_unique_id);
    upload.file_name = format!("synthetic-{message_id}.flac");
    upload.file_size = ORIGINAL_BYTES.len() as u64;
    upload.mime_type = None;
    upload
}

fn canonical_settings(fixture: &TestDirectory) -> MusicSettings {
    let mut settings = fixture.settings();
    let media_root = fs::canonicalize(
        settings
            .state_dir
            .parent()
            .expect("state directory should have a parent"),
    )
    .expect("fixture media root should canonicalize");
    settings.state_dir = media_root.join(".music-state");
    settings.staging_dir = media_root.join(".music-staging");
    settings.archive_dir = media_root.join("archive");
    settings
}

fn audio_bytes(path: PathBuf, bytes: &[u8]) -> AudioBytes {
    AudioBytes {
        path,
        sha256: sha256(bytes),
        size: bytes.len() as u64,
    }
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn read_job(settings: &MusicSettings, job_id: &str) -> Option<serde_json::Value> {
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(settings.state_dir.join("jobs.json")).ok()?).ok()?;
    state.get("jobs")?.get(job_id).cloned()
}

async fn wait_for_job(
    settings: &MusicSettings,
    job_id: &str,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(job) = read_job(settings, job_id)
                && predicate(&job)
            {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("music job did not reach the expected persisted state")
}

async fn wait_for_download_start(backend: &FilesystemCleanupBackend) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !backend.download_started.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("synthetic partial download did not start");
}

async fn stop_worker(shutdown: watch::Sender<bool>, worker: tokio::task::JoinHandle<()>) {
    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("music worker should stop promptly")
        .expect("music worker should join");
}

fn job_staging_dir(settings: &MusicSettings, job_id: &str) -> PathBuf {
    settings.staging_dir.join(job_id)
}

#[tokio::test]
async fn completed_import_cleans_only_staging_and_preserves_archive_and_music_copy() {
    let fixture = TestDirectory::new();
    let settings = canonical_settings(&fixture);
    let server = TestHttpServer::start().await;
    let backend = FilesystemCleanupBackend::new(&settings, 0);
    let manager = open_manager(
        settings.clone(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let upload = flac_upload(31_001, "cleanup-success");
    let admission = manager
        .enqueue(upload.clone())
        .await
        .expect("upload should enqueue");
    let (shutdown, receiver) = watch::channel(false);
    let worker = manager.start(receiver);

    let job = wait_for_job(&settings, &admission.job_id, |job| {
        job["stage"] == "complete" && job["staging_cleanup_completed"] == true
    })
    .await;
    let archive_path = settings.archive_dir.join(&upload.file_name);
    let music_before = fs::read(&backend.music_copy).expect("mock Music copy should exist");
    let archive_before = fs::read(&archive_path).expect("uploaded archive fixture should exist");

    assert_eq!(job["stage"], "complete");
    assert!(!job_staging_dir(&settings, &admission.job_id).exists());
    assert_eq!(backend.downloads.load(Ordering::Acquire), 1);
    assert_eq!(backend.preparations.load(Ordering::Acquire), 1);
    assert_eq!(backend.archives.load(Ordering::Acquire), 1);
    assert_eq!(backend.adds.load(Ordering::Acquire), 1);
    assert_eq!(backend.cleanups.load(Ordering::Acquire), 1);
    assert_eq!(backend.native_cleanups.load(Ordering::Acquire), 1);
    assert_eq!(archive_before.as_slice(), ORIGINAL_BYTES);
    assert_eq!(music_before.as_slice(), PREPARED_BYTES);
    assert_eq!(
        fs::read(&archive_path).expect("archive should remain"),
        archive_before
    );
    assert_eq!(
        fs::read(&backend.music_copy).expect("Music copy should remain"),
        music_before
    );

    stop_worker(shutdown, worker).await;
    drop(manager);
    server.stop();
}

#[tokio::test]
async fn failed_archive_and_cancelled_partial_download_retain_incomplete_staging() {
    let fixture = TestDirectory::new();
    let settings = canonical_settings(&fixture);
    let server = TestHttpServer::start().await;
    let backend = FilesystemCleanupBackend::new(&settings, 0);
    backend.archive_failure.store(true, Ordering::Release);
    let manager = open_manager(
        settings.clone(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let failed_upload = flac_upload(31_002, "cleanup-archive-failure");
    let failed = manager
        .enqueue(failed_upload)
        .await
        .expect("failure fixture should enqueue");
    let (shutdown, receiver) = watch::channel(false);
    let worker = manager.start(receiver);
    let failed_job = wait_for_job(&settings, &failed.job_id, |job| job["stage"] == "failed").await;
    let failed_dir = job_staging_dir(&settings, &failed.job_id);
    assert_eq!(failed_job["staging_cleanup_completed"], false);
    assert!(failed_dir.join("original.flac").is_file());
    assert!(
        failed_dir
            .join("prepare-attempt-1")
            .join("converted-cache.m4a")
            .is_file()
    );
    assert_eq!(backend.cleanups.load(Ordering::Acquire), 0);

    stop_worker(shutdown, worker).await;
    drop(manager);

    let cancel_fixture = TestDirectory::new();
    let cancel_settings = canonical_settings(&cancel_fixture);
    let cancel_server = TestHttpServer::start().await;
    let cancel_backend = FilesystemCleanupBackend::new(&cancel_settings, 0);
    cancel_backend
        .cancel_download
        .store(true, Ordering::Release);
    let cancel_manager = open_manager(
        cancel_settings.clone(),
        &cancel_server,
        cancel_backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let mut cancelled_upload = flac_upload(31_003, "cleanup-cancelled-partial");
    cancelled_upload.file_size = (ORIGINAL_BYTES.len() + 100) as u64;
    let cancelled = cancel_manager
        .enqueue(cancelled_upload.clone())
        .await
        .expect("cancel fixture should enqueue");
    let (cancel_shutdown, cancel_receiver) = watch::channel(false);
    let cancel_worker = cancel_manager.start(cancel_receiver);
    wait_for_download_start(&cancel_backend).await;
    cancel_manager
        .cancel(cancelled_upload.chat_id, &cancelled.job_id)
        .await
        .expect("partial upload should cancel");
    let cancelled_job = wait_for_job(&cancel_settings, &cancelled.job_id, |job| {
        job["stage"] == "cancelled"
    })
    .await;
    let cancelled_original =
        job_staging_dir(&cancel_settings, &cancelled.job_id).join("original.flac");
    assert_eq!(cancelled_job["staging_cleanup_completed"], false);
    assert_eq!(
        fs::read(&cancelled_original).expect("partial original should be retained"),
        PARTIAL_BYTES
    );
    assert_eq!(cancel_backend.preparations.load(Ordering::Acquire), 0);
    assert_eq!(cancel_backend.cleanups.load(Ordering::Acquire), 0);

    stop_worker(cancel_shutdown, cancel_worker).await;
    drop(cancel_manager);
    cancel_server.stop();
    server.stop();
}

#[tokio::test]
async fn cleanup_failure_retries_after_restart_without_reimport_and_dedup_ignores_missing_source() {
    let fixture = TestDirectory::new();
    let settings = canonical_settings(&fixture);
    let server = TestHttpServer::start().await;
    let backend = FilesystemCleanupBackend::new(&settings, 1);
    let original_upload = flac_upload(31_004, "cleanup-restart-original");
    let manager = open_manager(
        settings.clone(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let admission = manager
        .enqueue(original_upload.clone())
        .await
        .expect("restart fixture should enqueue");
    let (shutdown, receiver) = watch::channel(false);
    let worker = manager.start(receiver);
    let failed_cleanup = wait_for_job(&settings, &admission.job_id, |job| {
        job["stage"] == "complete"
            && job["staging_cleanup_completed"] == false
            && job["staging_cleanup_attempts"] == 1
    })
    .await;
    let original_staging = job_staging_dir(&settings, &admission.job_id);
    let original_archive = settings.archive_dir.join(&original_upload.file_name);
    let archive_before = fs::read(&original_archive).expect("archive should exist before retry");
    let music_before = fs::read(&backend.music_copy).expect("Music copy should exist before retry");
    assert_eq!(failed_cleanup["stage"], "complete");
    assert!(original_staging.join("original.flac").is_file());
    assert!(
        original_staging
            .join("prepare-attempt-1")
            .join("converted-cache.m4a")
            .is_file()
    );
    assert_eq!(backend.downloads.load(Ordering::Acquire), 1);
    assert_eq!(backend.preparations.load(Ordering::Acquire), 1);
    assert_eq!(backend.adds.load(Ordering::Acquire), 1);

    stop_worker(shutdown, worker).await;
    drop(manager);

    let restarted = open_manager(
        settings.clone(),
        &server,
        backend.clone() as Arc<dyn MusicBackend>,
    )
    .await;
    let replay = restarted
        .enqueue(original_upload.clone())
        .await
        .expect("same-origin replay should be accepted idempotently");
    assert!(!replay.created);
    assert_eq!(replay.job_id, admission.job_id);
    let (restart_shutdown, restart_receiver) = watch::channel(false);
    let restart_worker = restarted.start(restart_receiver);
    let cleaned = wait_for_job(&settings, &admission.job_id, |job| {
        job["stage"] == "complete" && job["staging_cleanup_completed"] == true
    })
    .await;
    assert_eq!(cleaned["stage"], "complete");
    assert!(!original_staging.exists());
    assert_eq!(backend.downloads.load(Ordering::Acquire), 1);
    assert_eq!(backend.preparations.load(Ordering::Acquire), 1);
    assert_eq!(backend.archives.load(Ordering::Acquire), 1);
    assert_eq!(backend.adds.load(Ordering::Acquire), 1);
    assert_eq!(backend.cleanups.load(Ordering::Acquire), 2);
    assert_eq!(backend.native_cleanups.load(Ordering::Acquire), 1);

    let duplicate_upload = flac_upload(31_005, "cleanup-distinct-duplicate");
    let duplicate = restarted
        .enqueue(duplicate_upload.clone())
        .await
        .expect("same-content new upload should enqueue");
    assert!(duplicate.created);
    let duplicate_job = wait_for_job(&settings, &duplicate.job_id, |job| {
        job["stage"] == "complete" && job["staging_cleanup_completed"] == true
    })
    .await;
    assert_eq!(duplicate_job["stage"], "complete");
    assert!(!job_staging_dir(&settings, &duplicate.job_id).exists());
    assert_eq!(backend.downloads.load(Ordering::Acquire), 2);
    assert_eq!(backend.preparations.load(Ordering::Acquire), 2);
    assert_eq!(backend.archives.load(Ordering::Acquire), 1);
    assert_eq!(backend.adds.load(Ordering::Acquire), 1);
    assert_eq!(backend.native_cleanups.load(Ordering::Acquire), 2);
    assert_eq!(
        fs::read(&original_archive).expect("archive should remain"),
        archive_before
    );
    assert_eq!(
        fs::read(&backend.music_copy).expect("Music copy should remain"),
        music_before
    );
    let deduplicated_music = fs::read(&backend.music_copy).expect("dedup reused Music copy");
    assert_eq!(deduplicated_music.as_slice(), PREPARED_BYTES);

    stop_worker(restart_shutdown, restart_worker).await;
    drop(restarted);
    server.stop();
}
