//! Explicit local-source proof recovery for failed Music jobs.

use std::fs::File;
use std::io::Read;
use std::os::fd::OwnedFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use rustix::fs::{AtFlags, FileType, Mode, OFlags};
use serde::Deserialize;

use super::{JobStage, MusicManager, MusicStore, StoredJob, StoredLocalMusicSource, unix_seconds};
use crate::config::AppConfig;
use crate::music_runtime::RuntimeMusicBackend;
use crate::music_types::{LocalMusicFile, MusicBackend, MusicCancellation};
use crate::telegram::TelegramClient;

const REQUEST_SCHEMA_VERSION: u8 = 1;
const MAX_REQUEST_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceRecoveryRequest {
    schema_version: u8,
    job_id: String,
    expected_old_sha256: String,
    expected_new_sha256: String,
    expected_size: u64,
    accept_source_change: bool,
}

/// Validate a fresh local-file proof and optionally queue exactly one eligible failed job.
///
/// `apply == false` performs a read-only preview. Applying never starts a worker or calls Music
/// or Telegram; the normal bot runtime processes the newly queued job after its next start.
pub async fn run_cli(
    config_path: &Path,
    request_path: &Path,
    apply: bool,
) -> Result<serde_json::Value> {
    let config_path = config_path.to_path_buf();
    let request_path = request_path.to_path_buf();
    tokio::task::spawn_blocking(move || run_cli_blocking(&config_path, &request_path, apply))
        .await
        .context("local Music source recovery worker stopped")?
}

fn run_cli_blocking(
    config_path: &Path,
    request_path: &Path,
    apply: bool,
) -> Result<serde_json::Value> {
    let request = read_recovery_request(request_path)?;
    let config = AppConfig::load(config_path)?;
    #[cfg(all(feature = "test-runtime", not(test)))]
    crate::music_runtime::validate_test_profile(&config)?;

    if !config.music.enabled {
        bail!("enable Music imports before recovering a local source");
    }
    if request.expected_size == 0 || request.expected_size > config.music.max_file_bytes {
        bail!("approved source size is outside the configured Music file limit");
    }

    let settings = config.music.clone();
    let telegram = TelegramClient::new(config.telegram.token.clone());
    let backend: Arc<dyn MusicBackend> = Arc::new(RuntimeMusicBackend::new(
        settings.clone(),
        config.telegram.token.clone(),
    ));

    // `open_for_seed` takes the durable owner lock but deliberately does not start the worker
    // or rewrite interrupted jobs. The backend and Telegram client are inert in this path.
    let manager = MusicManager::open_for_seed(settings.clone(), telegram, backend)?;
    recover_local_job(&manager.store, &settings, request, apply)
}

fn read_recovery_request(path: &Path) -> Result<SourceRecoveryRequest> {
    let file = open_private_request_file(path)?;
    let metadata = file
        .metadata()
        .context("inspect opened local Music recovery request")?;
    // SAFETY: `getuid` reads the caller's real UID and takes no pointers or other arguments.
    let owner_uid = unsafe { libc::getuid() };
    if !metadata.is_file() || metadata.uid() != owner_uid || (metadata.mode() & 0o7777) != 0o600 {
        bail!(
            "local Music recovery request must be a regular file owned by this user with mode 0600"
        );
    }
    if metadata.len() > MAX_REQUEST_BYTES as u64 {
        bail!("local Music recovery request exceeds its size limit");
    }

    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take((MAX_REQUEST_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .context("read local Music recovery request")?;
    if bytes.len() > MAX_REQUEST_BYTES {
        bail!("local Music recovery request exceeds its size limit");
    }
    let request: SourceRecoveryRequest =
        serde_json::from_slice(&bytes).context("parse local Music recovery request")?;
    validate_recovery_request(request)
}

fn open_private_request_file(path: &Path) -> Result<File> {
    if !path.is_absolute() {
        bail!("local Music recovery request path must be absolute");
    }
    let components = path.components().collect::<Vec<_>>();
    if components.first() != Some(&Component::RootDir) {
        bail!("local Music recovery request path must be absolute");
    }
    let names = components
        .iter()
        .skip(1)
        .map(|component| match component {
            Component::Normal(name) => Ok(*name),
            _ => bail!("local Music recovery request path must be normalized"),
        })
        .collect::<Result<Vec<_>>>()?;
    let (leaf, parents) = names
        .split_last()
        .ok_or_else(|| anyhow!("local Music recovery request path is empty"))?;

    let mut parent = file_from_fd(
        rustix::fs::open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_to_io)
        .context("open filesystem root for local Music recovery request")?,
    );
    for component in parents {
        let next = rustix::fs::openat(
            &parent,
            *component,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_to_io)
        .context("open local Music recovery request directory without following links")?;
        parent = file_from_fd(next);
    }

    let metadata = rustix::fs::statat(&parent, *leaf, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(errno_to_io)
        .context("inspect local Music recovery request without following links")?;
    if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile {
        bail!("local Music recovery request must not be a symlink or special file");
    }
    let request = rustix::fs::openat(
        &parent,
        *leaf,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(errno_to_io)
    .context("open local Music recovery request without following links")?;
    Ok(file_from_fd(request))
}

fn validate_recovery_request(request: SourceRecoveryRequest) -> Result<SourceRecoveryRequest> {
    if request.schema_version != REQUEST_SCHEMA_VERSION {
        bail!("unsupported local Music recovery request schema");
    }
    if request.job_id.is_empty()
        || request.job_id.len() > 256
        || request.job_id.chars().any(char::is_control)
    {
        bail!("local Music recovery request has an invalid job ID");
    }
    if !is_sha256(&request.expected_old_sha256) || !is_sha256(&request.expected_new_sha256) {
        bail!("local Music recovery request requires two SHA-256 digests");
    }
    if request
        .expected_old_sha256
        .eq_ignore_ascii_case(&request.expected_new_sha256)
    {
        bail!("local Music source recovery requires changed content");
    }
    if request.expected_size == 0 {
        bail!("local Music recovery request has an invalid approved size");
    }
    if !request.accept_source_change {
        bail!("local Music source change was not explicitly approved");
    }
    Ok(request)
}

fn recover_local_job(
    store: &MusicStore,
    settings: &crate::music_types::MusicSettings,
    request: SourceRecoveryRequest,
    apply: bool,
) -> Result<serde_json::Value> {
    let original_job = store
        .job(&request.job_id)?
        .ok_or_else(|| anyhow!("local Music recovery job was not found"))?;
    let original_source = eligible_local_source(&original_job)?;
    let original_file = original_source.file.clone();
    let old_sha256 = normalize_sha256(original_file.content_sha256())?;
    let expected_old_sha256 = normalize_sha256(&request.expected_old_sha256)?;
    let expected_new_sha256 = normalize_sha256(&request.expected_new_sha256)?;
    if old_sha256 != expected_old_sha256 {
        bail!("approved old source digest does not match the failed job proof");
    }
    if request.expected_size > settings.max_file_bytes {
        bail!("approved source size is outside the configured Music file limit");
    }

    original_file.revalidate_source_binding(settings.max_file_bytes)?;
    let recovered_file = LocalMusicFile::capture(
        original_file.source_key.clone(),
        original_file.source_root.clone(),
        original_file.relative_path.clone(),
        settings.max_file_bytes,
        &MusicCancellation::default(),
    )?;
    if !original_file.same_source_binding(&recovered_file) {
        bail!("local Music source object changed during explicit recovery");
    }
    if recovered_file.file_size != request.expected_size
        || normalize_sha256(recovered_file.content_sha256())? != expected_new_sha256
    {
        bail!("recaptured local source does not match the approved digest and size");
    }
    original_file.revalidate_source_binding(settings.max_file_bytes)?;
    recovered_file.revalidate_source_binding(settings.max_file_bytes)?;

    if apply {
        let original_job_bytes = serde_json::to_vec(&original_job)
            .context("snapshot failed Music job before source recovery")?;
        store.transact(|state| {
            let job = state
                .jobs
                .get_mut(&request.job_id)
                .ok_or_else(|| anyhow!("local Music recovery job disappeared"))?;
            if serde_json::to_vec(job)? != original_job_bytes {
                bail!("local Music job changed during source recovery");
            }
            let source = eligible_local_source(job)?;
            if source.file != original_file
                || !source.file.same_source_binding(&recovered_file)
                || normalize_sha256(source.file.content_sha256())? != expected_old_sha256
            {
                bail!("local Music source proof changed during recovery");
            }

            let source = job
                .local_source
                .as_mut()
                .ok_or_else(|| anyhow!("failed Music job is not a local source"))?;
            source.file = recovered_file.clone();
            job.stage = JobStage::Queued;
            job.error = None;
            job.cancel_requested = false;
            job.next_poll_at = None;
            job.updated_at = unix_seconds();
            Ok(())
        })?;
    }

    Ok(serde_json::json!({
        "job_id": request.job_id,
        "old_sha256": old_sha256,
        "new_sha256": expected_new_sha256,
        "size": request.expected_size,
        "queued": apply,
        "dry_run": !apply,
        "added": false
    }))
}

fn eligible_local_source(job: &StoredJob) -> Result<&StoredLocalMusicSource> {
    if job.stage != JobStage::Failed
        || job.upload.is_some()
        || job.add_intent
        || job.persistent_id.is_some()
        || job.prepared.is_some()
        || job.archive.is_some()
        || job.download_complete
        || job.track.is_some()
        || job.local_copy_verified
    {
        bail!("Music job is outside the safe local-source recovery boundary");
    }
    job.local_source
        .as_ref()
        .ok_or_else(|| anyhow!("Music job is not a local source"))
}

fn normalize_sha256(value: &str) -> Result<String> {
    if !is_sha256(value) {
        bail!("local Music recovery digest is invalid");
    }
    Ok(value.to_ascii_lowercase())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn file_from_fd(fd: OwnedFd) -> File {
    File::from(fd)
}

fn errno_to_io(error: rustix::io::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(error.raw_os_error())
}

#[cfg(test)]
mod tests {
    use super::super::{job_identity, local_job_identity};
    use super::*;

    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    use std::path::PathBuf;

    use crate::music_manager::tests::TestDirectory;
    use crate::music_types::{ArchiveState, AudioBytes, PreparedAudio};

    struct Fixture {
        store: super::super::MusicStore,
        settings: crate::music_types::MusicSettings,
        job_id: String,
        original_file: LocalMusicFile,
        source_path: PathBuf,
        download_path: PathBuf,
        _directory: TestDirectory,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = TestDirectory::new();
            let mut settings = directory.settings();
            for path in [&settings.state_dir, &settings.staging_dir] {
                fs::create_dir_all(path).expect("private Music state directory should exist");
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                    .expect("private Music state directory should be private");
            }

            let source_root = settings
                .archive_dir
                .parent()
                .expect("fixture media directory should exist")
                .join("selected-source");
            fs::create_dir(&source_root).expect("selected source root should exist");
            let source_path = source_root.join("song.m4a");
            fs::write(&source_path, b"old bytes").expect("source bytes should be written");
            settings.max_file_bytes = 1024 * 1024;

            let original_file = LocalMusicFile::capture(
                "fixture-source-key".to_string(),
                source_root,
                PathBuf::from("song.m4a"),
                settings.max_file_bytes,
                &MusicCancellation::default(),
            )
            .expect("original source proof should be captured");
            let (job_id, origin_key) = local_job_identity(42, 42, &original_file.source_key);
            let download_path = settings.staging_dir.join(&job_id).join("download.partial");
            fs::create_dir_all(
                download_path
                    .parent()
                    .expect("job staging path should exist"),
            )
            .expect("job staging directory should exist");
            fs::write(&download_path, b"preserve partial bytes")
                .expect("partial download should be retained");

            drop(
                MusicStore::open(
                    &settings,
                    crate::file_provider::platform_queue_file_provider(),
                )
                .expect("Music state should initialize before seed-only access"),
            );
            let store = MusicStore::open_for_seed(
                &settings,
                crate::file_provider::platform_queue_file_provider(),
            )
            .expect("Music store should open without recovery");
            let now = unix_seconds();
            let job = StoredJob {
                job_id: job_id.clone(),
                origin_key,
                upload: None,
                local_source: Some(StoredLocalMusicSource {
                    chat_id: 42,
                    user_id: 42,
                    batch_id: "fixture-batch".to_string(),
                    file: original_file.clone(),
                }),
                stage: JobStage::Failed,
                created_at: now.saturating_sub(1),
                updated_at: now,
                attempt: 7,
                prepare_attempt: 2,
                archive_attempt: 3,
                download_path: Some(download_path.clone()),
                download_complete: false,
                prepared: None,
                archive: None,
                persistent_id: None,
                track: None,
                local_copy_verified: false,
                add_intent: false,
                cancel_requested: true,
                next_poll_at: Some(now.saturating_add(60)),
                error: Some("source content changed".to_string()),
                status_message_id: None,
                status_send_attempted: false,
                status_text: None,
                status_pending_text: None,
                status_edit_attempts: 0,
                status_next_attempt_at: None,
                staging_cleanup_completed: false,
                staging_cleanup_attempts: 0,
                staging_cleanup_next_attempt_at: None,
            };
            store
                .transact(|state| {
                    state.jobs.insert(job_id.clone(), job);
                    Ok(())
                })
                .expect("failed local job should be seeded");

            Self {
                store,
                settings,
                job_id,
                original_file,
                source_path,
                download_path,
                _directory: directory,
            }
        }

        fn request(&self, expected_new_sha256: &str, expected_size: u64) -> SourceRecoveryRequest {
            SourceRecoveryRequest {
                schema_version: REQUEST_SCHEMA_VERSION,
                job_id: self.job_id.clone(),
                expected_old_sha256: self.original_file.content_sha256().to_string(),
                expected_new_sha256: expected_new_sha256.to_string(),
                expected_size,
                accept_source_change: true,
            }
        }

        fn state_bytes(&self) -> Vec<u8> {
            fs::read(&self.store.state_path).expect("Music state should be readable")
        }
    }

    #[test]
    fn explicit_recovery_recaptures_changed_content_and_preserves_attempt_artifacts() {
        let fixture = Fixture::new();
        fs::write(&fixture.source_path, b"new bytes")
            .expect("same-size source content should change in place");
        let original_state = fixture.state_bytes();
        let expected_new_sha256 = LocalMusicFile::capture(
            fixture.original_file.source_key.clone(),
            fixture.original_file.source_root.clone(),
            fixture.original_file.relative_path.clone(),
            fixture.settings.max_file_bytes,
            &MusicCancellation::default(),
        )
        .expect("new content should be capturable")
        .content_sha256()
        .to_string();
        let request = fixture.request(&expected_new_sha256, b"new bytes".len() as u64);
        let preview = recover_local_job(&fixture.store, &fixture.settings, request.clone(), false)
            .expect("approved recovery preview should succeed");
        assert!(preview["dry_run"].as_bool().unwrap_or(false));
        assert!(!preview["queued"].as_bool().unwrap_or(true));
        assert!(!preview["added"].as_bool().unwrap_or(true));
        assert_eq!(fixture.state_bytes(), original_state);

        let applied = recover_local_job(&fixture.store, &fixture.settings, request, true)
            .expect("approved recovery should queue the failed job");
        assert!(!applied["dry_run"].as_bool().unwrap_or(true));
        assert!(applied["queued"].as_bool().unwrap_or(false));
        assert!(!applied["added"].as_bool().unwrap_or(true));
        let recovered = fixture
            .store
            .job(&fixture.job_id)
            .expect("job should read")
            .expect("recovered job should remain present");
        let source = recovered.local_source.expect("local source should remain");
        assert_eq!(recovered.stage, JobStage::Queued);
        assert_eq!(source.file.content_sha256(), expected_new_sha256);
        assert_eq!(recovered.attempt, 7);
        assert_eq!(recovered.prepare_attempt, 2);
        assert_eq!(recovered.archive_attempt, 3);
        assert_eq!(
            recovered.download_path.as_deref(),
            Some(fixture.download_path.as_path())
        );
        assert_eq!(
            fs::read(&fixture.download_path).expect("partial download should remain"),
            b"preserve partial bytes"
        );
        assert!(!recovered.cancel_requested);
        assert_eq!(recovered.error, None);
        assert_eq!(recovered.next_poll_at, None);
    }

    #[test]
    fn incorrect_digest_size_or_replaced_source_never_changes_job_state() {
        let fixture = Fixture::new();
        fs::write(&fixture.source_path, b"new bytes").expect("source should change in place");
        let good_new_sha256 = LocalMusicFile::capture(
            fixture.original_file.source_key.clone(),
            fixture.original_file.source_root.clone(),
            fixture.original_file.relative_path.clone(),
            fixture.settings.max_file_bytes,
            &MusicCancellation::default(),
        )
        .expect("new content should be capturable")
        .content_sha256()
        .to_string();
        let original_state = fixture.state_bytes();

        let mut wrong_old = fixture.request(&good_new_sha256, b"new bytes".len() as u64);
        wrong_old.expected_old_sha256 = "a".repeat(64);
        assert!(recover_local_job(&fixture.store, &fixture.settings, wrong_old, true).is_err());
        assert_eq!(fixture.state_bytes(), original_state);

        let mut wrong_new = fixture.request(&"b".repeat(64), b"new bytes".len() as u64);
        assert!(recover_local_job(&fixture.store, &fixture.settings, wrong_new, true).is_err());
        assert_eq!(fixture.state_bytes(), original_state);

        wrong_new = fixture.request(&good_new_sha256, b"new bytes".len() as u64 + 1);
        assert!(recover_local_job(&fixture.store, &fixture.settings, wrong_new, true).is_err());
        assert_eq!(fixture.state_bytes(), original_state);

        let replacement = fixture.source_path.with_extension("replacement");
        fs::write(&replacement, b"new bytes").expect("replacement bytes should be written");
        fs::rename(&replacement, &fixture.source_path).expect("source object should be replaced");
        let valid = fixture.request(&good_new_sha256, b"new bytes".len() as u64);
        assert!(recover_local_job(&fixture.store, &fixture.settings, valid, true).is_err());
        assert_eq!(fixture.state_bytes(), original_state);
    }

    #[test]
    fn recovery_rejects_add_intent_and_telegram_jobs() {
        let fixture = Fixture::new();
        fs::write(&fixture.source_path, b"new bytes").expect("source should change in place");
        let new_file = LocalMusicFile::capture(
            fixture.original_file.source_key.clone(),
            fixture.original_file.source_root.clone(),
            fixture.original_file.relative_path.clone(),
            fixture.settings.max_file_bytes,
            &MusicCancellation::default(),
        )
        .expect("new content should be capturable");
        let mut added_job = fixture
            .store
            .job(&fixture.job_id)
            .expect("job should read")
            .expect("fixture job should exist");
        added_job.add_intent = true;
        added_job.persistent_id = Some("fixture-persistent-id".to_string());
        added_job.local_copy_verified = true;
        added_job.prepared = Some(PreparedAudio {
            original: AudioBytes {
                path: fixture
                    .settings
                    .staging_dir
                    .join(&fixture.job_id)
                    .join("original.m4a"),
                sha256: "c".repeat(64),
                size: 9,
            },
            import: AudioBytes {
                path: fixture
                    .settings
                    .staging_dir
                    .join(&fixture.job_id)
                    .join("prepared.m4a"),
                sha256: "d".repeat(64),
                size: 9,
            },
            conversion: None,
        });
        added_job.archive = Some(ArchiveState {
            path: fixture.settings.archive_dir.join("archive.m4a"),
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: true,
        });
        fixture
            .store
            .transact(|state| {
                state.jobs.insert(fixture.job_id.clone(), added_job);
                Ok(())
            })
            .expect("post-add job should be valid durable state");
        let before = fixture.state_bytes();
        let request = fixture.request(new_file.content_sha256(), new_file.file_size);
        assert!(recover_local_job(&fixture.store, &fixture.settings, request, true).is_err());
        assert_eq!(fixture.state_bytes(), before);

        let upload = crate::music_manager::tests::upload(1, "fixture-file-unique-id");
        let (telegram_job_id, origin_key) = job_identity(&upload);
        let telegram_job = StoredJob {
            job_id: telegram_job_id.clone(),
            origin_key,
            upload: Some(upload),
            local_source: None,
            stage: JobStage::Failed,
            created_at: unix_seconds(),
            updated_at: unix_seconds(),
            attempt: 1,
            prepare_attempt: 0,
            archive_attempt: 0,
            download_path: None,
            download_complete: false,
            prepared: None,
            archive: None,
            persistent_id: None,
            track: None,
            local_copy_verified: false,
            add_intent: false,
            cancel_requested: false,
            next_poll_at: None,
            error: Some("fixture failure".to_string()),
            status_message_id: None,
            status_send_attempted: false,
            status_text: None,
            status_pending_text: None,
            status_edit_attempts: 0,
            status_next_attempt_at: None,
            staging_cleanup_completed: false,
            staging_cleanup_attempts: 0,
            staging_cleanup_next_attempt_at: None,
        };
        fixture
            .store
            .transact(|state| {
                state.jobs.insert(telegram_job_id.clone(), telegram_job);
                Ok(())
            })
            .expect("failed Telegram job should be durable state");
        let request = SourceRecoveryRequest {
            schema_version: REQUEST_SCHEMA_VERSION,
            job_id: telegram_job_id,
            expected_old_sha256: "e".repeat(64),
            expected_new_sha256: "f".repeat(64),
            expected_size: 9,
            accept_source_change: true,
        };
        let before = fixture.state_bytes();
        assert!(recover_local_job(&fixture.store, &fixture.settings, request, true).is_err());
        assert_eq!(fixture.state_bytes(), before);
    }

    #[test]
    fn request_parser_requires_explicit_approval_and_rejects_unknown_fields() {
        let request = SourceRecoveryRequest {
            schema_version: REQUEST_SCHEMA_VERSION,
            job_id: "mjl1-test".to_string(),
            expected_old_sha256: "a".repeat(64),
            expected_new_sha256: "b".repeat(64),
            expected_size: 1,
            accept_source_change: false,
        };
        assert!(validate_recovery_request(request).is_err());

        let json = serde_json::json!({
            "schema_version": 1,
            "job_id": "mjl1-test",
            "expected_old_sha256": "a".repeat(64),
            "expected_new_sha256": "b".repeat(64),
            "expected_size": 1,
            "accept_source_change": true,
            "unexpected": true
        });
        assert!(serde_json::from_value::<SourceRecoveryRequest>(json).is_err());
    }

    #[test]
    fn request_reader_rejects_symlinks_and_non_private_modes() {
        let directory = TestDirectory::new();
        let root = fs::canonicalize(
            directory
                .settings()
                .archive_dir
                .parent()
                .expect("fixture media directory should exist"),
        )
        .expect("fixture path should resolve without aliases");
        let request_path = root.join("recovery.json");
        let request_bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "job_id": "mjl1-test",
            "expected_old_sha256": "a".repeat(64),
            "expected_new_sha256": "b".repeat(64),
            "expected_size": 1,
            "accept_source_change": true
        }))
        .expect("request JSON should serialize");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&request_path)
            .expect("private request should be created");
        file.write_all(&request_bytes)
            .expect("request bytes should be written");
        file.sync_all().expect("request file should sync");
        assert!(read_recovery_request(&request_path).is_ok());

        fs::set_permissions(&request_path, fs::Permissions::from_mode(0o644))
            .expect("request mode should change");
        assert!(read_recovery_request(&request_path).is_err());

        let link = root.join("recovery-link.json");
        std::os::unix::fs::symlink(&request_path, &link).expect("request symlink should exist");
        assert!(read_recovery_request(&link).is_err());
    }
}
