#![cfg(target_os = "macos")]

use super::tests::{TestHttpServer, open_manager, upload};
use super::*;
use anyhow::ensure;
use std::sync::atomic::AtomicUsize;

struct WavBackend {
    native: crate::music_native::NativeMusicBackend,
    source: PathBuf,
    archive: PathBuf,
    music_copy: PathBuf,
    preparations: AtomicUsize,
    readiness: AtomicUsize,
    adds: AtomicUsize,
    evictions: AtomicUsize,
}

impl MusicBackend for WavBackend {
    fn download(&self, _: &MusicUpload, destination: &Path, _: &MusicCancellation) -> Result<()> {
        fs::copy(&self.source, destination)?;
        Ok(())
    }

    fn prepare(
        &self,
        original: &Path,
        working: &Path,
        cancel: &MusicCancellation,
    ) -> Result<PreparedAudio> {
        self.preparations.fetch_add(1, Ordering::Relaxed);
        self.native.prepare(original, working, cancel)
    }

    fn archive(&self, audio: &PreparedAudio, original_name: &str) -> Result<ArchiveState> {
        ensure!(
            Path::new(original_name)
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("wav")),
            "archive must retain the original WAV extension"
        );
        let path = self.archive.join(original_name);
        fs::copy(&audio.original.path, &path)?;
        Ok(ArchiveState {
            path,
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: true,
        })
    }

    fn check_music_ready(&self) -> Result<()> {
        if self.readiness.fetch_add(1, Ordering::Relaxed) == 0 {
            bail!("synthetic pre-add interruption");
        }
        Ok(())
    }

    fn check_import_source(&self, audio: &PreparedAudio) -> Result<()> {
        ensure!(
            audio
                .import
                .path
                .extension()
                .is_some_and(|ext| ext == "m4a")
        );
        Ok(())
    }

    fn add_track(&self, audio: &PreparedAudio) -> Result<String> {
        self.adds.fetch_add(1, Ordering::Relaxed);
        fs::create_dir_all(self.music_copy.parent().unwrap())?;
        fs::copy(&audio.import.path, &self.music_copy)?;
        Ok("1234567890ABCDEF".into())
    }

    fn query_track(&self, persistent_id: &str) -> Result<MusicTrack> {
        Ok(MusicTrack {
            persistent_id: persistent_id.into(),
            location: Some(self.music_copy.clone()),
            cloud_status: "matched".into(),
        })
    }

    fn verify_copy(&self, audio: &PreparedAudio, track: &MusicTrack) -> Result<()> {
        ensure!(track.location.as_deref() == Some(self.music_copy.as_path()));
        ensure!(fs::read(&audio.import.path)? == fs::read(&self.music_copy)?);
        Ok(())
    }

    fn archive_status(&self, path: &Path) -> Result<ArchiveState> {
        Ok(ArchiveState {
            path: path.into(),
            uploaded: true,
            uploading: false,
            downloading: false,
            conflicts: false,
            local_bytes: true,
        })
    }

    fn evict_archive(&self, archive: &ArchiveState, _: &MusicTrack) -> Result<ArchiveState> {
        self.evictions.fetch_add(1, Ordering::Relaxed);
        Ok(ArchiveState {
            local_bytes: false,
            ..archive.clone()
        })
    }
}

fn test_tool(name: &str) -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|root| root.join(name))
        .find(|path| path.is_file())
        .unwrap_or_else(|| panic!("missing WAV E2E test tool {name}"))
        .canonicalize()
        .unwrap()
}

fn wave_chunk(bytes: &mut Vec<u8>, kind: &[u8; 4], value: &[u8]) {
    bytes.extend_from_slice(kind);
    bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
    bytes.extend_from_slice(value);
    if !value.len().is_multiple_of(2) {
        bytes.push(0);
    }
}

fn tagged_wave() -> Vec<u8> {
    let channels = 2_u16;
    let rate = 48_000_u32;
    let mut fmt = Vec::new();
    fmt.extend_from_slice(&1_u16.to_le_bytes());
    fmt.extend_from_slice(&channels.to_le_bytes());
    fmt.extend_from_slice(&rate.to_le_bytes());
    fmt.extend_from_slice(&(rate * 4).to_le_bytes());
    fmt.extend_from_slice(&4_u16.to_le_bytes());
    fmt.extend_from_slice(&16_u16.to_le_bytes());
    let mut body = b"WAVE".to_vec();
    wave_chunk(&mut body, b"fmt ", &fmt);
    let mut info = b"INFO".to_vec();
    wave_chunk(
        &mut info,
        b"INAM",
        "中文 日本語 한국어 Русский Français Italiano\0".as_bytes(),
    );
    wave_chunk(&mut info, b"IART", "演奏者 🎵\0".as_bytes());
    wave_chunk(&mut body, b"LIST", &info);
    let pcm = (0..2048_i32)
        .flat_map(|sample| ((sample * 251 - 32768) as i16).to_le_bytes())
        .collect::<Vec<_>>();
    wave_chunk(&mut body, b"data", &pcm);
    let mut output = b"RIFF".to_vec();
    output.extend_from_slice(&(body.len() as u32).to_le_bytes());
    output.extend_from_slice(&body);
    output
}

async fn await_stage(manager: &MusicManager, job_id: &str, stage: JobStage) -> StoredJob {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let job = manager.store.job(job_id).unwrap().unwrap();
            if job.stage == stage {
                return job;
            }
            if stage == JobStage::Complete && job.stage == JobStage::Failed {
                panic!("WAV E2E unexpectedly failed: {:?}", job.error);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("WAV manager did not reach the expected stage")
}

async fn stop_worker(shutdown: watch::Sender<bool>, worker: JoinHandle<()>) {
    let _ = shutdown.send(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn real_wav_conversion_keeps_original_and_reuses_saved_alac_after_restart() {
    let Some(converter) = std::env::var_os("FLAC_ALAC_TEST_CONVERTER").map(PathBuf::from) else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI requires the real converter"
        );
        eprintln!("SKIP: WAV manager E2E requires FLAC_ALAC_TEST_CONVERTER");
        return;
    };
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().canonicalize().unwrap();
    let source = root.join("音乐-日本語-한국어-Русский-é-Italiano.WAV");
    let original = tagged_wave();
    fs::write(&source, &original).unwrap();
    let archive = root.join("archive");
    fs::create_dir(&archive).unwrap();
    let settings = MusicSettings {
        enabled: true,
        state_dir: root.join("state"),
        staging_dir: root.join("staging"),
        archive_dir: archive.clone(),
        max_file_bytes: 1024 * 1024,
        cloud_poll_seconds: 1,
        converter: converter.canonicalize().unwrap(),
        ffmpeg: test_tool("ffmpeg"),
        ffprobe: test_tool("ffprobe"),
        ..MusicSettings::default()
    };
    let backend = Arc::new(WavBackend {
        native: crate::music_native::NativeMusicBackend::new(settings.clone()),
        source,
        archive,
        music_copy: root.join("Music/local-copy.m4a"),
        preparations: AtomicUsize::new(0),
        readiness: AtomicUsize::new(0),
        adds: AtomicUsize::new(0),
        evictions: AtomicUsize::new(0),
    });
    let server = TestHttpServer::start().await;
    let manager = open_manager(settings.clone(), &server, backend.clone()).await;
    let mut request = upload(501, "wav-manager-e2e");
    request.file_name = "无损录音.WAV".into();
    request.file_size = original.len() as u64;
    request.mime_type = Some("audio/x-wav".into());
    let admitted = manager.enqueue(request.clone()).await.unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let worker = manager.start(receiver);
    let failed = await_stage(&manager, &admitted.job_id, JobStage::Failed).await;
    let audio = failed
        .prepared
        .as_ref()
        .unwrap_or_else(|| panic!("conversion failed: {:?}", failed.error));
    let report = audio
        .conversion
        .as_ref()
        .expect("WAV conversion must retain its report");
    assert!(report.audio_bit_exact);
    assert_eq!(report.source, report.output);
    assert_eq!(report.source.frames, 1024);
    assert_eq!(report.source.bits_per_sample, 16);
    assert!(report.metadata.mapped.iter().any(|name| name == "title"));
    assert_ne!(audio.original.sha256, audio.import.sha256);
    assert_eq!(fs::read(&audio.original.path).unwrap(), original);
    assert_eq!(
        fs::read(&failed.archive.as_ref().unwrap().path).unwrap(),
        original
    );
    assert_eq!(backend.adds.load(Ordering::Relaxed), 0);
    let import_path = audio.import.path.clone();
    stop_worker(shutdown, worker).await;
    drop(manager);

    let reopened = open_manager(settings, &server, backend.clone()).await;
    reopened.retry(42, &admitted.job_id).await.unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let worker = reopened.start(receiver);
    let completed = await_stage(&reopened, &admitted.job_id, JobStage::Complete).await;
    let audio = completed.prepared.as_ref().unwrap();
    assert_eq!(audio.import.path, import_path);
    assert_eq!(backend.preparations.load(Ordering::Relaxed), 1);
    assert_eq!(backend.adds.load(Ordering::Relaxed), 1);
    assert_eq!(backend.evictions.load(Ordering::Relaxed), 1);
    assert_eq!(
        fs::read(&backend.music_copy).unwrap(),
        fs::read(&audio.import.path).unwrap()
    );
    assert_eq!(
        fs::read(&completed.archive.as_ref().unwrap().path).unwrap(),
        original
    );
    assert!(
        server.wait_for_text("ALAC").await,
        "Telegram status should describe the verified conversion"
    );
    request.update_id += 1;
    request.message_id += 1;
    request.file_unique_id = "wav-same-content-again".into();
    let duplicate = reopened.enqueue(request).await.unwrap();
    let duplicate = await_stage(&reopened, &duplicate.job_id, JobStage::Complete).await;
    assert_eq!(
        duplicate.prepared.as_ref().unwrap().import.path,
        import_path
    );
    // New Telegram uploads are prepared before the existing content-catalog lookup.
    assert_eq!(backend.preparations.load(Ordering::Relaxed), 2);
    assert_eq!(backend.adds.load(Ordering::Relaxed), 1);
    stop_worker(shutdown, worker).await;
    server.stop();
}
