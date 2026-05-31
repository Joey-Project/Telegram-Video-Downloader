use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::{Instant, sleep_until, timeout as tokio_timeout};
use tracing::info;

use crate::config::AppConfig;
use crate::router::JobRequest;

static VIDEO_OUTPUT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const OUTPUT_CLOSE_GRACE: Duration = Duration::from_secs(2);
const OUTPUT_ABORT_GRACE: Duration = Duration::from_secs(3);

#[cfg(unix)]
type CommandProcessGroup = Option<libc::pid_t>;
#[cfg(not(unix))]
type CommandProcessGroup = Option<()>;

#[derive(Debug, Clone)]
pub struct JobReport {
    pub saved_location: String,
    pub details: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobProgress {
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub activity_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubtitleSource {
    Manual,
    Automatic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubtitlePlan {
    pub source: Option<SubtitleSource>,
    pub languages: Vec<String>,
}

impl SubtitlePlan {
    fn none() -> Self {
        Self {
            source: None,
            languages: Vec::new(),
        }
    }

    fn describe(&self) -> String {
        match &self.source {
            Some(SubtitleSource::Manual) => {
                format!("Subtitles: manual {}", self.languages.join(","))
            }
            Some(SubtitleSource::Automatic) => {
                format!("Subtitles: automatic {}", self.languages.join(","))
            }
            None => "Subtitles: no preferred subtitles found".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct YoutubeMetadata {
    id: Option<String>,
    title: Option<String>,
    description: Option<String>,
    uploader: Option<String>,
    channel: Option<String>,
    upload_date: Option<String>,
    webpage_url: Option<String>,
    #[serde(default)]
    subtitles: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    automatic_captions: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default)]
struct BilibiliMetadata {
    title: Option<String>,
    source_url: String,
    uploader_url: Option<String>,
    publish_date: Option<String>,
    id: Option<String>,
    aid: Option<String>,
}

pub async fn run_job(
    config: &AppConfig,
    job: &JobRequest,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    match job {
        JobRequest::Bilibili { url } => run_bilibili_job(config, url, progress).await,
        JobRequest::Youtube { url } => run_youtube_job(config, url, progress).await,
        JobRequest::Pdf { .. } => run_simple_job(config, job, progress).await,
    }
}

async fn run_simple_job(
    config: &AppConfig,
    job: &JobRequest,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    let spec = command_spec(config, job);
    let output = run_command(config, &spec, progress.clone()).await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "{} exited with status {}\n{}",
            spec.program.display(),
            output.status,
            summarize_output(&stdout, &stderr)
        );
    }

    let saved_location = last_nonempty_line(&stdout)
        .ok_or_else(|| anyhow!("pdf helper finished without printing output path"))?
        .to_string();
    Ok(JobReport {
        saved_location,
        details: tail_lines(&stderr, 6),
    })
}

async fn run_bilibili_job(
    config: &AppConfig,
    url: &str,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    let _guard = video_output_lock().await;
    let mut nfo_warnings = Vec::new();
    let needs_mux = config
        .bilibili
        .extra_args
        .iter()
        .any(|arg| arg == "--skip-mux");
    let before = match list_video_files(&config.downloads.video_dir) {
        Ok(files) => Some(files),
        Err(err) if needs_mux => {
            bail!("Bilibili post-processing failed: failed to scan before download: {err}");
        }
        Err(err) => {
            nfo_warnings.push(format!(
                "Bilibili post-processing skipped: failed to scan before download: {err}"
            ));
            None
        }
    };
    let spec = bilibili_command_spec(config, url);
    let command_started_at = SystemTime::now();
    let output = run_command(config, &spec, progress.clone()).await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "{} exited with status {}\n{}",
            spec.program.display(),
            output.status,
            summarize_output(&stdout, &stderr)
        );
    }

    let metadata = parse_bilibili_metadata(url, &stdout);
    let mut details = vec![tail_lines(&stdout, 6)];
    details.extend(nfo_warnings);

    if let Some(before) = before {
        match list_video_files(&config.downloads.video_dir) {
            Ok(after) => {
                let created_videos: Vec<_> = after.difference(&before).cloned().collect();
                let videos_to_process = if needs_mux {
                    bilibili_mux_candidates(config, &metadata, created_videos, command_started_at)?
                } else {
                    created_videos
                };
                if needs_mux && videos_to_process.is_empty() {
                    bail!("Bilibili post-processing failed: no video/audio stream pairs found");
                }
                let final_videos = if needs_mux {
                    merge_bilibili_streams(config, &videos_to_process, &metadata, progress).await?
                } else {
                    videos_to_process
                };
                if config.video.write_nfo {
                    match write_nfos_for_videos(
                        &final_videos,
                        &MediaNfo {
                            title: metadata.title.as_deref(),
                            plot: None,
                            unique_id_type: "bilibili",
                            unique_id: metadata
                                .id
                                .as_deref()
                                .or(metadata.aid.as_deref())
                                .unwrap_or(url),
                            source_url: &metadata.source_url,
                            studio: metadata.uploader_url.as_deref(),
                            premiered: metadata.publish_date.as_deref(),
                        },
                    ) {
                        Ok(created_nfos) if !created_nfos.is_empty() => {
                            details.push(format!("NFO: {}", join_paths(&created_nfos)));
                        }
                        Ok(_) => {}
                        Err(err) => details.push(format!("NFO skipped: {err}")),
                    }
                }
            }
            Err(err) if needs_mux => {
                bail!("Bilibili post-processing failed: failed to scan after download: {err}");
            }
            Err(err) => details.push(format!("NFO skipped: failed to scan after download: {err}")),
        }
    }

    Ok(JobReport {
        saved_location: config.downloads.video_dir.display().to_string(),
        details: nonempty_join(details),
    })
}

fn bilibili_mux_candidates(
    config: &AppConfig,
    metadata: &BilibiliMetadata,
    created_videos: Vec<PathBuf>,
    since: SystemTime,
) -> Result<Vec<PathBuf>> {
    let mut candidates = created_videos;
    if let Some(aid) = metadata.aid.as_deref() {
        let aid_dir = config.downloads.video_dir.join(aid);
        if aid_dir.is_dir() {
            for video in list_video_files(&aid_dir)? {
                let audio = video.with_extension("m4a");
                if audio.is_file()
                    && (modified_since(&video, since) || modified_since(&audio, since))
                    && !candidates.contains(&video)
                {
                    candidates.push(video);
                }
            }
        }
    }
    Ok(candidates)
}

fn modified_since(path: &Path, since: SystemTime) -> bool {
    path.metadata()
        .and_then(|metadata| metadata.modified())
        .is_ok_and(|modified| modified >= since)
}

async fn merge_bilibili_streams(
    config: &AppConfig,
    videos: &[PathBuf],
    metadata: &BilibiliMetadata,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<Vec<PathBuf>> {
    let mut merged = Vec::new();
    let video_only = config
        .bilibili
        .extra_args
        .iter()
        .any(|arg| arg == "--video-only");
    for video in videos {
        let audio = video.with_extension("m4a");
        if !audio.is_file() {
            if !video_only {
                bail!(
                    "Bilibili post-processing failed: expected audio stream {}",
                    audio.display()
                );
            }
            merged.push(video.clone());
            continue;
        }

        let title = metadata
            .title
            .as_deref()
            .or_else(|| video.file_stem().and_then(|stem| stem.to_str()))
            .unwrap_or("bilibili");
        let output = unique_output_path(&config.downloads.video_dir, title, "mp4");
        let spec = ffmpeg_mux_command_spec(config, video, &audio, &output);
        let output_result = run_command(config, &spec, progress.clone()).await?;
        if !output_result.status.success() {
            bail!(
                "{} exited with status {}\n{}",
                spec.program.display(),
                output_result.status,
                summarize_output(
                    &String::from_utf8_lossy(&output_result.stdout),
                    &String::from_utf8_lossy(&output_result.stderr)
                )
            );
        }

        let _ = fs::remove_file(video);
        let _ = fs::remove_file(&audio);
        merged.push(output);
    }
    Ok(merged)
}

async fn run_youtube_job(
    config: &AppConfig,
    url: &str,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<JobReport> {
    let metadata = fetch_youtube_metadata(config, url, progress.clone()).await?;
    let subtitle_plan = select_subtitles(&metadata, &config.video.subtitle_languages);
    let _guard = video_output_lock().await;
    let spec = youtube_download_command_spec(config, url, &subtitle_plan);
    let output = run_command(config, &spec, progress).await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "{} exited with status {}\n{}",
            spec.program.display(),
            output.status,
            summarize_output(&stdout, &stderr)
        );
    }

    let saved_location = last_nonempty_line(&stdout)
        .filter(|line| Path::new(line).is_absolute())
        .map(str::to_string)
        .unwrap_or_else(|| config.downloads.video_dir.display().to_string());

    let mut details = vec![subtitle_plan.describe(), tail_lines(&stderr, 6)];
    if config.video.write_nfo {
        let video_path = Path::new(&saved_location);
        if video_path.is_absolute() && video_path.is_file() && is_video_file(video_path) {
            let title = metadata
                .title
                .as_deref()
                .or_else(|| video_path.file_stem()?.to_str());
            let source_url = metadata.webpage_url.as_deref().unwrap_or(url);
            let studio = metadata.uploader.as_deref().or(metadata.channel.as_deref());
            let premiered = metadata.upload_date.as_deref().and_then(format_yt_date);
            match write_nfo_for_video(
                video_path,
                &MediaNfo {
                    title,
                    plot: metadata.description.as_deref(),
                    unique_id_type: "youtube",
                    unique_id: metadata.id.as_deref().unwrap_or(url),
                    source_url,
                    studio,
                    premiered: premiered.as_deref(),
                },
            ) {
                Ok(nfo_path) => details.push(format!("NFO: {}", nfo_path.display())),
                Err(err) => details.push(format!("NFO skipped: {err}")),
            }
        }
    }

    Ok(JobReport {
        saved_location,
        details: nonempty_join(details),
    })
}

pub fn command_spec(config: &AppConfig, job: &JobRequest) -> CommandSpec {
    match job {
        JobRequest::Bilibili { url } => bilibili_command_spec(config, url),
        JobRequest::Youtube { url } => {
            youtube_download_command_spec(config, url, &SubtitlePlan::none())
        }
        JobRequest::Pdf { url } => pdf_command_spec(config, url),
    }
}

pub fn bilibili_command_spec(config: &AppConfig, url: &str) -> CommandSpec {
    let mut args = vec![url.to_string(), "--skip-ai".to_string()];
    args.extend(config.bilibili.extra_args.iter().cloned());

    CommandSpec {
        program: config.tools.bbdown.clone(),
        args,
        cwd: config.downloads.video_dir.clone(),
        activity_dir: Some(config.downloads.video_dir.clone()),
    }
}

pub fn youtube_metadata_command_spec(config: &AppConfig, url: &str) -> CommandSpec {
    CommandSpec {
        program: config.tools.yt_dlp.clone(),
        args: vec![
            "--dump-json".to_string(),
            "--skip-download".to_string(),
            "--no-playlist".to_string(),
            url.to_string(),
        ],
        cwd: config.downloads.video_dir.clone(),
        activity_dir: None,
    }
}

pub fn youtube_download_command_spec(
    config: &AppConfig,
    url: &str,
    subtitle_plan: &SubtitlePlan,
) -> CommandSpec {
    let mut args = vec![
        "--no-playlist".to_string(),
        "-P".to_string(),
        config.downloads.video_dir.display().to_string(),
        "--merge-output-format".to_string(),
        "mkv".to_string(),
        "--remux-video".to_string(),
        "mkv".to_string(),
        "--embed-thumbnail".to_string(),
        "--embed-metadata".to_string(),
        "--embed-chapters".to_string(),
        "--embed-info-json".to_string(),
        "--convert-thumbnails".to_string(),
        "jpg".to_string(),
        "--print".to_string(),
        "after_move:filepath".to_string(),
    ];

    if config.video.keep_sidecars {
        args.extend([
            "--write-info-json".to_string(),
            "--write-description".to_string(),
            "--write-thumbnail".to_string(),
        ]);
    }

    match &subtitle_plan.source {
        Some(SubtitleSource::Manual) => args.push("--write-subs".to_string()),
        Some(SubtitleSource::Automatic) => args.push("--write-auto-subs".to_string()),
        None => {}
    }

    if !subtitle_plan.languages.is_empty() {
        args.extend([
            "--sub-langs".to_string(),
            subtitle_plan.languages.join(","),
            "--sub-format".to_string(),
            "srt/vtt/best".to_string(),
            "--convert-subs".to_string(),
            "srt".to_string(),
            "--embed-subs".to_string(),
        ]);
    }

    args.push(url.to_string());

    CommandSpec {
        program: config.tools.yt_dlp.clone(),
        args,
        cwd: config.downloads.video_dir.clone(),
        activity_dir: Some(config.downloads.video_dir.clone()),
    }
}

pub fn pdf_command_spec(config: &AppConfig, url: &str) -> CommandSpec {
    CommandSpec {
        program: config.tools.uv.clone(),
        args: vec![
            "run".to_string(),
            "python".to_string(),
            config
                .resolve_project_path(&config.tools.pdf_helper)
                .display()
                .to_string(),
            "--url".to_string(),
            url.to_string(),
            "--output-dir".to_string(),
            config.downloads.pdf_dir.display().to_string(),
            "--chrome".to_string(),
            config.tools.chrome.display().to_string(),
        ],
        cwd: config.resolve_project_path(Path::new(".")),
        activity_dir: Some(config.downloads.pdf_dir.clone()),
    }
}

fn ffmpeg_mux_command_spec(
    config: &AppConfig,
    video: &Path,
    audio: &Path,
    output: &Path,
) -> CommandSpec {
    CommandSpec {
        program: config.tools.ffmpeg.clone(),
        args: vec![
            "-hide_banner".to_string(),
            "-y".to_string(),
            "-i".to_string(),
            command_path_arg(video),
            "-i".to_string(),
            command_path_arg(audio),
            "-map".to_string(),
            "0:v:0".to_string(),
            "-map".to_string(),
            "1:a:0".to_string(),
            "-c".to_string(),
            "copy".to_string(),
            "-movflags".to_string(),
            "+faststart".to_string(),
            command_path_arg(output),
        ],
        cwd: config.downloads.video_dir.clone(),
        activity_dir: Some(config.downloads.video_dir.clone()),
    }
}

fn command_path_arg(path: &Path) -> String {
    if path.is_absolute() {
        return path.display().to_string();
    }
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(path)
        .display()
        .to_string()
}

async fn fetch_youtube_metadata(
    config: &AppConfig,
    url: &str,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<YoutubeMetadata> {
    let spec = youtube_metadata_command_spec(config, url);
    let output = run_command(config, &spec, progress).await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        bail!(
            "{} exited with status {}\n{}",
            spec.program.display(),
            output.status,
            summarize_output(&stdout, &stderr)
        );
    }

    let json = last_nonempty_line(&stdout).ok_or_else(|| anyhow!("yt-dlp returned no metadata"))?;
    serde_json::from_str(json).context("failed to parse yt-dlp metadata JSON")
}

async fn video_output_lock() -> MutexGuard<'static, ()> {
    VIDEO_OUTPUT_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .await
}

#[derive(Debug)]
struct CommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandStream {
    Stdout,
    Stderr,
}

#[derive(Debug)]
struct CommandChunk {
    stream: CommandStream,
    bytes: Vec<u8>,
}

async fn run_command(
    config: &AppConfig,
    spec: &CommandSpec,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
) -> Result<CommandOutput> {
    let mut file_activity = match &spec.activity_dir {
        Some(activity_dir) => match FileActivityTracker::new(activity_dir).await {
            Ok(tracker) => Some(tracker),
            Err(err) => {
                info!(
                    command = %spec.program.display(),
                    activity_dir = %activity_dir.display(),
                    error = %err,
                    "file activity tracking disabled"
                );
                None
            }
        },
        None => {
            info!(command = %spec.program.display(), "file activity tracking disabled");
            None
        }
    };

    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to run {}", spec.program.display()))?;
    let process_group = command_process_group(&child);

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to capture {} stdout", spec.program.display()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("failed to capture {} stderr", spec.program.display()))?;

    let (chunk_tx, mut chunk_rx) = mpsc::unbounded_channel();
    let stdout_handle = tokio::spawn(read_command_stream(
        stdout,
        CommandStream::Stdout,
        chunk_tx.clone(),
    ));
    let stderr_handle = tokio::spawn(read_command_stream(stderr, CommandStream::Stderr, chunk_tx));

    let total_timeout = Duration::from_secs(config.bot.command_timeout_seconds);
    let idle_timeout = Duration::from_secs(config.bot.command_idle_timeout_seconds);
    let started_at = Instant::now();
    let total_deadline = started_at + total_timeout;
    let mut last_activity_at = started_at;
    let progress_interval = Duration::from_secs(config.bot.progress_update_seconds);
    let activity_poll_interval = file_activity_poll_interval(progress_interval, idle_timeout);
    let mut next_activity_poll = started_at + activity_poll_interval;
    let mut progress_tracker = ProgressTracker::new(
        spec.program
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .unwrap_or("command")
            .to_string(),
        progress_interval,
        progress,
    );

    let mut output_closed = false;
    let status = loop {
        let idle_deadline = last_activity_at + idle_timeout;
        tokio::select! {
            maybe_chunk = chunk_rx.recv(), if !output_closed => {
                match maybe_chunk {
                    Some(chunk) => {
                        last_activity_at = Instant::now();
                        progress_tracker.observe(chunk.stream, &chunk.bytes);
                    }
                    None => output_closed = true,
                }
            }
            wait_result = child.wait() => {
                break wait_result
                    .with_context(|| format!("failed to wait for {}", spec.program.display()))?;
            }
            _ = sleep_until(total_deadline) => {
                terminate_command_tree(&mut child, process_group).await;
                let (stdout, stderr) =
                    collect_stream_outputs(stdout_handle, stderr_handle, process_group).await;
                bail!(
                    "{} timed out after {}s\n{}",
                    spec.program.display(),
                    config.bot.command_timeout_seconds,
                    summarize_output(&String::from_utf8_lossy(&stdout), &String::from_utf8_lossy(&stderr))
                );
            }
            _ = sleep_until(idle_deadline) => {
                terminate_command_tree(&mut child, process_group).await;
                let (stdout, stderr) =
                    collect_stream_outputs(stdout_handle, stderr_handle, process_group).await;
                bail!(
                    "{} had no output or file activity for {}s\n{}",
                    spec.program.display(),
                    config.bot.command_idle_timeout_seconds,
                    summarize_output(&String::from_utf8_lossy(&stdout), &String::from_utf8_lossy(&stderr))
                );
            }
            _ = sleep_until(next_activity_poll), if file_activity.is_some() => {
                next_activity_poll = Instant::now() + activity_poll_interval;
                let tracker = file_activity.as_mut().expect("guarded by is_some");
                match tracker.poll().await {
                    Ok(Some(message)) => {
                        last_activity_at = Instant::now();
                        progress_tracker.emit(message);
                    }
                    Ok(None) => {}
                    Err(err) => {
                        info!(
                            command = %spec.program.display(),
                            activity_dir = %tracker.root.display(),
                            error = %err,
                            "file activity tracking stopped"
                        );
                        file_activity = None;
                    }
                }
            }
        }
    };

    let (stdout, stderr) =
        collect_stream_outputs(stdout_handle, stderr_handle, process_group).await;
    Ok(CommandOutput {
        status,
        stdout,
        stderr,
    })
}

fn file_activity_poll_interval(progress_interval: Duration, idle_timeout: Duration) -> Duration {
    let half_idle_timeout = idle_timeout / 2;
    progress_interval.min(if half_idle_timeout.is_zero() {
        idle_timeout
    } else {
        half_idle_timeout
    })
}

fn command_process_group(child: &tokio::process::Child) -> CommandProcessGroup {
    #[cfg(unix)]
    {
        child.id().map(|id| id as libc::pid_t)
    }

    #[cfg(not(unix))]
    {
        let _ = child;
        None
    }
}

async fn terminate_command_tree(
    child: &mut tokio::process::Child,
    process_group: CommandProcessGroup,
) {
    #[cfg(unix)]
    if let Some(process_group_id) = process_group {
        signal_process_group(process_group_id, libc::SIGTERM);
        let direct_child_exited = tokio_timeout(Duration::from_secs(5), child.wait())
            .await
            .is_ok();
        signal_process_group(process_group_id, libc::SIGKILL);
        if !direct_child_exited {
            let _ = child.wait().await;
        }
        return;
    }

    let _ = child.kill().await;
}

#[cfg(unix)]
fn signal_process_group(process_group_id: libc::pid_t, signal: libc::c_int) {
    unsafe {
        libc::kill(-process_group_id, signal);
    }
}

fn force_terminate_process_group(process_group: CommandProcessGroup) {
    #[cfg(unix)]
    if let Some(process_group_id) = process_group {
        signal_process_group(process_group_id, libc::SIGKILL);
    }

    #[cfg(not(unix))]
    {
        let _ = process_group;
    }
}

async fn read_command_stream<R>(
    mut reader: R,
    stream: CommandStream,
    progress: mpsc::UnboundedSender<CommandChunk>,
) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let size = reader.read(&mut buffer).await?;
        if size == 0 {
            break;
        }
        let bytes = buffer[..size].to_vec();
        output.extend_from_slice(&bytes);
        let _ = progress.send(CommandChunk { stream, bytes });
    }
    Ok(output)
}

async fn collect_stream_outputs(
    mut stdout_handle: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    mut stderr_handle: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    process_group: CommandProcessGroup,
) -> (Vec<u8>, Vec<u8>) {
    let close_deadline = Instant::now() + OUTPUT_CLOSE_GRACE;
    let mut abort_deadline = close_deadline + OUTPUT_ABORT_GRACE;
    let mut did_terminate_group = false;
    let mut stdout = None;
    let mut stderr = None;

    loop {
        if stdout.is_some() && stderr.is_some() {
            break;
        }

        tokio::select! {
            result = &mut stdout_handle, if stdout.is_none() => {
                stdout = Some(join_stream_output(result));
            }
            result = &mut stderr_handle, if stderr.is_none() => {
                stderr = Some(join_stream_output(result));
            }
            _ = sleep_until(close_deadline), if !did_terminate_group => {
                force_terminate_process_group(process_group);
                did_terminate_group = true;
                abort_deadline = Instant::now() + OUTPUT_ABORT_GRACE;
            }
            _ = sleep_until(abort_deadline), if did_terminate_group => {
                if stdout.is_none() {
                    stdout_handle.abort();
                    stdout = Some(b"stdout reader did not close after process termination".to_vec());
                }
                if stderr.is_none() {
                    stderr_handle.abort();
                    stderr = Some(b"stderr reader did not close after process termination".to_vec());
                }
            }
        }
    }

    (
        stdout.expect("stdout is set before loop exits"),
        stderr.expect("stderr is set before loop exits"),
    )
}

fn join_stream_output(result: Result<std::io::Result<Vec<u8>>, tokio::task::JoinError>) -> Vec<u8> {
    match result {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => format!("failed to read command output: {err}").into_bytes(),
        Err(err) => format!("failed to join command output reader: {err}").into_bytes(),
    }
}

struct FileActivityTracker {
    root: PathBuf,
    baseline: FileActivitySnapshot,
    last_changed_file_count: usize,
    last_changed_size: u64,
}

struct FileActivitySnapshot {
    files: BTreeMap<PathBuf, u64>,
    direct_dirs: BTreeSet<PathBuf>,
}

impl FileActivityTracker {
    async fn new(root: &Path) -> Result<Self> {
        let root = root.to_path_buf();
        let baseline = collect_file_activity(root.clone(), None).await?;
        Ok(Self {
            root,
            baseline,
            last_changed_file_count: 0,
            last_changed_size: 0,
        })
    }

    async fn poll(&mut self) -> Result<Option<String>> {
        let current =
            collect_file_activity(self.root.clone(), Some(self.baseline.direct_dirs.clone()))
                .await?;
        let changed = current
            .files
            .iter()
            .filter(|(path, size)| self.baseline.files.get(*path) != Some(*size));
        let mut changed_file_count = 0;
        let mut changed_size = 0;
        for (_, size) in changed {
            changed_file_count += 1;
            changed_size += size;
        }

        if changed_file_count == self.last_changed_file_count
            && changed_size == self.last_changed_size
        {
            return Ok(None);
        }

        self.last_changed_file_count = changed_file_count;
        self.last_changed_size = changed_size;
        if changed_file_count == 0 {
            return Ok(None);
        }

        Ok(Some(format!(
            "files: {changed_file_count} changed, {} written",
            human_bytes(changed_size)
        )))
    }
}

async fn collect_file_activity(
    root: PathBuf,
    baseline_direct_dirs: Option<BTreeSet<PathBuf>>,
) -> Result<FileActivitySnapshot> {
    tokio::task::spawn_blocking(move || collect_file_activity_blocking(&root, baseline_direct_dirs))
        .await
        .context("failed to join file activity scan")?
}

fn collect_file_activity_blocking(
    root: &Path,
    baseline_direct_dirs: Option<BTreeSet<PathBuf>>,
) -> Result<FileActivitySnapshot> {
    let mut files = BTreeMap::new();
    let mut direct_dirs = BTreeSet::new();
    if !root.exists() {
        return Ok(FileActivitySnapshot { files, direct_dirs });
    }

    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(FileActivitySnapshot { files, direct_dirs });
        }
        Err(err) => return Err(err).with_context(|| format!("failed to read {}", root.display())),
    };

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if file_type.is_file() {
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            files.insert(path, metadata.len());
        } else if file_type.is_dir() {
            direct_dirs.insert(path.clone());
            if baseline_direct_dirs
                .as_ref()
                .is_some_and(|baseline| !baseline.contains(&path))
            {
                collect_file_sizes_recursive(&path, &mut files)?;
            }
        }
    }

    Ok(FileActivitySnapshot { files, direct_dirs })
}

fn collect_file_sizes_recursive(root: &Path, files: &mut BTreeMap<PathBuf, u64>) -> Result<()> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).with_context(|| format!("failed to read {}", root.display())),
    };

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        };
        if file_type.is_file() {
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            files.insert(path, metadata.len());
        } else if file_type.is_dir() {
            collect_file_sizes_recursive(&path, files)?;
        }
    }
    Ok(())
}

fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else {
        format!("{} B", bytes as u64)
    }
}

struct ProgressTracker {
    command_name: String,
    min_interval: Duration,
    next_send_at: Instant,
    progress: Option<mpsc::UnboundedSender<JobProgress>>,
    last_message: Option<String>,
}

impl ProgressTracker {
    fn new(
        command_name: String,
        min_interval: Duration,
        progress: Option<mpsc::UnboundedSender<JobProgress>>,
    ) -> Self {
        Self {
            command_name,
            min_interval,
            next_send_at: Instant::now(),
            progress,
            last_message: None,
        }
    }

    fn observe(&mut self, stream: CommandStream, bytes: &[u8]) {
        let Some(progress) = &self.progress else {
            return;
        };

        let text = normalize_terminal_text(&String::from_utf8_lossy(bytes));
        let Some(message) = summarize_progress_chunk(&self.command_name, stream, &text) else {
            return;
        };

        let message_changed = self.last_message.as_ref() != Some(&message);
        let now = Instant::now();
        if now < self.next_send_at {
            return;
        }
        if !message_changed {
            return;
        }

        self.send(progress.clone(), message, now);
    }

    fn emit(&mut self, message: String) {
        let Some(progress) = &self.progress else {
            return;
        };

        let now = Instant::now();
        if now < self.next_send_at {
            return;
        }
        if self.last_message.as_ref() == Some(&message) {
            return;
        }
        self.send(progress.clone(), message, now);
    }

    fn send(
        &mut self,
        progress: mpsc::UnboundedSender<JobProgress>,
        message: String,
        now: Instant,
    ) {
        self.last_message = Some(message.clone());
        self.next_send_at = now + self.min_interval;
        info!(command = %self.command_name, message = %message, "command progress");
        let _ = progress.send(JobProgress { message });
    }
}

fn summarize_progress_chunk(
    command_name: &str,
    stream: CommandStream,
    text: &str,
) -> Option<String> {
    if let Some(percent) = extract_last_percent(text) {
        return Some(format!("{command_name}: {percent}%"));
    }

    let line = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter(|line| !line.starts_with("Response:"))
        .filter(|line| !line.starts_with('{'))
        .filter(|line| !line.contains("baseUrl"))
        .rfind(|line| line.chars().count() <= 180)?;

    let normalized = line
        .trim_start_matches(|ch: char| ch == '-' || ch.is_ascii_whitespace())
        .to_string();
    if normalized.is_empty() {
        return None;
    }

    let stream_name = match stream {
        CommandStream::Stdout => "stdout",
        CommandStream::Stderr => "stderr",
    };
    Some(format!("{command_name} {stream_name}: {normalized}"))
}

fn normalize_terminal_text(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch == '\r' || ch == '\n' {
            normalized.push('\n');
        } else if ch.is_control() {
            normalized.push(' ');
        } else {
            normalized.push(ch);
        }
    }
    normalized
}

fn extract_last_percent(text: &str) -> Option<u8> {
    let bytes = text.as_bytes();
    let mut index = 0;
    let mut last = None;
    while index < bytes.len() {
        if !bytes[index].is_ascii_digit() {
            index += 1;
            continue;
        }

        let start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }

        if index < bytes.len() && bytes[index] == b'.' {
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
        }

        if index < bytes.len()
            && bytes[index] == b'%'
            && let Ok(value) = text[start..index].parse::<f64>()
            && (0.0..=100.0).contains(&value)
        {
            last = Some(value.floor() as u8);
        }
    }
    last
}

fn select_subtitles(metadata: &YoutubeMetadata, preferred_languages: &[String]) -> SubtitlePlan {
    let manual = select_matching_languages(&metadata.subtitles, preferred_languages);
    if !manual.is_empty() {
        return SubtitlePlan {
            source: Some(SubtitleSource::Manual),
            languages: manual,
        };
    }

    let automatic = select_matching_languages(&metadata.automatic_captions, preferred_languages);
    if !automatic.is_empty() {
        return SubtitlePlan {
            source: Some(SubtitleSource::Automatic),
            languages: automatic,
        };
    }

    SubtitlePlan::none()
}

fn select_matching_languages(
    available: &BTreeMap<String, serde_json::Value>,
    preferred_languages: &[String],
) -> Vec<String> {
    let mut selected = Vec::new();
    for preferred in preferred_languages {
        for language in available.keys() {
            if language_matches(preferred, language) && !selected.contains(language) {
                selected.push(language.clone());
            }
        }
    }
    selected
}

fn language_matches(preferred: &str, available: &str) -> bool {
    let preferred = preferred.to_ascii_lowercase();
    let available = available.to_ascii_lowercase();
    available == preferred
        || available
            .strip_prefix(&preferred)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn parse_bilibili_metadata(url: &str, stdout: &str) -> BilibiliMetadata {
    let mut metadata = BilibiliMetadata {
        source_url: url.to_string(),
        id: bilibili_id_from_url(url),
        ..BilibiliMetadata::default()
    };

    for line in stdout.lines() {
        if let Some((_, title)) = line.split_once("视频标题:") {
            metadata.title = Some(title.trim().to_string());
        } else if let Some((_, aid)) = line.split_once("获取aid结束:") {
            metadata.aid = Some(aid.trim().to_string());
        } else if let Some((_, published)) = line.split_once("发布时间:") {
            let published = published.trim();
            metadata.publish_date = published.get(..10).map(str::to_string);
        } else if let Some((_, uploader_url)) = line.split_once("UP主页:") {
            metadata.uploader_url = Some(uploader_url.trim().to_string());
        }
    }

    metadata
}

fn bilibili_id_from_url(raw_url: &str) -> Option<String> {
    let url = url::Url::parse(raw_url).ok()?;
    url.path_segments()?
        .find(|segment| {
            segment.starts_with("BV") || segment.starts_with("bv") || segment.starts_with("av")
        })
        .map(str::to_string)
}

fn list_video_files(root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut files = BTreeSet::new();
    collect_video_files(root, &mut files)?;
    Ok(files)
}

fn collect_video_files(path: &Path, files: &mut BTreeSet<PathBuf>) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_video_files(&path, files)?;
        } else if file_type.is_file() && is_video_file(&path) {
            files.insert(path);
        }
    }

    Ok(())
}

fn is_video_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "mkv" | "mp4" | "m4v" | "webm" | "mov" | "avi"
            )
        })
        .unwrap_or(false)
}

fn unique_output_path(root: &Path, title: &str, extension: &str) -> PathBuf {
    let stem = safe_file_stem(title);
    let mut candidate = root.join(format!("{stem}.{extension}"));
    let mut index = 2;
    while candidate.exists() {
        candidate = root.join(format!("{stem} ({index}).{extension}"));
        index += 1;
    }
    candidate
}

fn safe_file_stem(title: &str) -> String {
    let sanitized = title
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            ch if ch.is_control() => '_',
            ch => ch,
        })
        .collect::<String>()
        .trim()
        .trim_matches('.')
        .to_string();
    if sanitized.is_empty() {
        "bilibili".to_string()
    } else {
        sanitized
    }
}

struct MediaNfo<'a> {
    title: Option<&'a str>,
    plot: Option<&'a str>,
    unique_id_type: &'a str,
    unique_id: &'a str,
    source_url: &'a str,
    studio: Option<&'a str>,
    premiered: Option<&'a str>,
}

fn write_nfos_for_videos(videos: &[PathBuf], nfo: &MediaNfo<'_>) -> Result<Vec<PathBuf>> {
    videos
        .iter()
        .map(|video| write_nfo_for_video(video, nfo))
        .collect()
}

fn write_nfo_for_video(video_path: &Path, nfo: &MediaNfo<'_>) -> Result<PathBuf> {
    let title = nfo
        .title
        .or_else(|| video_path.file_stem().and_then(|stem| stem.to_str()))
        .unwrap_or("Untitled");
    let nfo_path = video_path.with_extension("nfo");
    fs::write(&nfo_path, render_nfo(title, nfo))
        .with_context(|| format!("failed to write {}", nfo_path.display()))?;
    Ok(nfo_path)
}

fn render_nfo(title: &str, nfo: &MediaNfo<'_>) -> String {
    let mut content =
        String::from("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<movie>\n");
    content.push_str(&format!("  <title>{}</title>\n", xml_escape(title)));
    content.push_str(&format!(
        "  <uniqueid type=\"{}\" default=\"true\">{}</uniqueid>\n",
        xml_escape(nfo.unique_id_type),
        xml_escape(nfo.unique_id)
    ));
    content.push_str(&format!(
        "  <trailer>{}</trailer>\n",
        xml_escape(nfo.source_url)
    ));

    if let Some(plot) = nfo.plot.filter(|plot| !plot.trim().is_empty()) {
        content.push_str(&format!("  <plot>{}</plot>\n", xml_escape(plot.trim())));
    }
    if let Some(studio) = nfo.studio.filter(|studio| !studio.trim().is_empty()) {
        content.push_str(&format!(
            "  <studio>{}</studio>\n",
            xml_escape(studio.trim())
        ));
    }
    if let Some(premiered) = nfo
        .premiered
        .filter(|premiered| !premiered.trim().is_empty())
    {
        content.push_str(&format!(
            "  <premiered>{}</premiered>\n",
            xml_escape(premiered.trim())
        ));
        if let Some(year) = premiered.get(..4) {
            content.push_str(&format!("  <year>{}</year>\n", xml_escape(year)));
        }
    }

    content.push_str("</movie>\n");
    content
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn format_yt_date(upload_date: &str) -> Option<String> {
    if upload_date.len() == 8 {
        Some(format!(
            "{}-{}-{}",
            &upload_date[0..4],
            &upload_date[4..6],
            &upload_date[6..8]
        ))
    } else {
        None
    }
}

fn summarize_output(stdout: &str, stderr: &str) -> String {
    let stderr_tail = tail_lines(stderr, 10);
    let stdout_tail = tail_lines(stdout, 10);
    match (stderr_tail.is_empty(), stdout_tail.is_empty()) {
        (true, true) => "no command output captured".to_string(),
        (false, true) => format!("stderr:\n{stderr_tail}"),
        (true, false) => format!("stdout:\n{stdout_tail}"),
        (false, false) => format!("stderr:\n{stderr_tail}\nstdout:\n{stdout_tail}"),
    }
}

fn last_nonempty_line(text: &str) -> Option<&str> {
    text.lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
}

fn tail_lines(text: &str, max_lines: usize) -> String {
    let lines: Vec<_> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    let start = lines.len().saturating_sub(max_lines);
    lines[start..].join("\n")
}

fn nonempty_join(lines: Vec<String>) -> String {
    lines
        .into_iter()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn join_paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::PathBuf;

    use crate::config::AppConfig;

    use super::*;

    fn test_config() -> AppConfig {
        AppConfig::load(Path::new("config.example.toml")).expect("example config should parse")
    }

    fn test_home() -> PathBuf {
        env::var_os("HOME")
            .map(PathBuf::from)
            .expect("HOME should be set during tests")
    }

    fn metadata_with_subtitles() -> YoutubeMetadata {
        YoutubeMetadata {
            subtitles: BTreeMap::from([
                ("en".to_string(), serde_json::json!([])),
                ("ja".to_string(), serde_json::json!([])),
                ("fr".to_string(), serde_json::json!([])),
            ]),
            automatic_captions: BTreeMap::from([
                ("zh-Hans".to_string(), serde_json::json!([])),
                ("en".to_string(), serde_json::json!([])),
            ]),
            ..YoutubeMetadata::default()
        }
    }

    #[test]
    fn builds_bilibili_command() {
        let config = test_config();
        let spec = command_spec(
            &config,
            &JobRequest::Bilibili {
                url: "https://www.bilibili.com/video/BV123".to_string(),
            },
        );

        assert_eq!(spec.program, PathBuf::from("BBDown"));
        assert!(spec.args.contains(&"--skip-ai".to_string()));
        assert!(spec.args.contains(&"--video-ascending".to_string()));
        assert!(spec.args.contains(&"--skip-mux".to_string()));
        assert_eq!(spec.cwd, test_home().join("Movies").join("Downloads"));
    }

    #[test]
    fn builds_ffmpeg_mux_command() {
        let config = test_config();
        let spec = ffmpeg_mux_command_spec(
            &config,
            Path::new("/tmp/video.mp4"),
            Path::new("/tmp/audio.m4a"),
            Path::new("/tmp/output.mp4"),
        );

        assert_eq!(spec.program, PathBuf::from("ffmpeg"));
        for expected in ["-i", "/tmp/video.mp4", "/tmp/audio.m4a", "-c", "copy"] {
            assert!(
                spec.args.contains(&expected.to_string()),
                "missing {expected}"
            );
        }
    }

    #[test]
    fn builds_youtube_metadata_command() {
        let config = test_config();
        let spec = youtube_metadata_command_spec(&config, "https://youtu.be/abc");

        assert_eq!(spec.program, PathBuf::from("yt-dlp"));
        assert!(spec.args.contains(&"--dump-json".to_string()));
        assert!(spec.args.contains(&"--skip-download".to_string()));
        assert!(spec.args.contains(&"--no-playlist".to_string()));
        assert_eq!(spec.activity_dir, None);
    }

    #[test]
    fn builds_youtube_download_command_with_metadata_sidecars() {
        let config = test_config();
        let subtitle_plan = SubtitlePlan {
            source: Some(SubtitleSource::Manual),
            languages: vec!["en".to_string(), "ja".to_string()],
        };
        let spec = youtube_download_command_spec(&config, "https://youtu.be/abc", &subtitle_plan);

        assert_eq!(spec.program, PathBuf::from("yt-dlp"));
        for expected in [
            "--merge-output-format",
            "mkv",
            "--remux-video",
            "--embed-thumbnail",
            "--embed-metadata",
            "--embed-chapters",
            "--embed-info-json",
            "--write-info-json",
            "--write-description",
            "--write-thumbnail",
            "--write-subs",
            "--sub-langs",
            "en,ja",
            "--embed-subs",
            "after_move:filepath",
        ] {
            assert!(
                spec.args.contains(&expected.to_string()),
                "missing {expected}"
            );
        }
        assert_eq!(spec.cwd, test_home().join("Movies").join("Downloads"));
    }

    #[test]
    fn builds_pdf_command_with_uv() {
        let config = test_config();
        let spec = command_spec(
            &config,
            &JobRequest::Pdf {
                url: "https://example.com".to_string(),
            },
        );

        assert_eq!(spec.program, PathBuf::from("uv"));
        assert_eq!(spec.args[0], "run");
        assert_eq!(spec.args[1], "python");
        assert!(
            spec.args
                .iter()
                .any(|arg| arg.ends_with("scripts/pdf_helper.py"))
        );
        assert!(spec.args.contains(&"--chrome".to_string()));
    }

    #[test]
    fn selects_manual_subtitles_before_automatic() {
        let plan = select_subtitles(
            &metadata_with_subtitles(),
            &["zh-Hans".to_string(), "en".to_string(), "ja".to_string()],
        );

        assert_eq!(
            plan,
            SubtitlePlan {
                source: Some(SubtitleSource::Manual),
                languages: vec!["en".to_string(), "ja".to_string()]
            }
        );
    }

    #[test]
    fn falls_back_to_automatic_subtitles() {
        let metadata = YoutubeMetadata {
            automatic_captions: BTreeMap::from([
                ("zh-Hans".to_string(), serde_json::json!([])),
                ("en".to_string(), serde_json::json!([])),
            ]),
            ..YoutubeMetadata::default()
        };

        let plan = select_subtitles(&metadata, &["zh".to_string(), "en".to_string()]);

        assert_eq!(
            plan,
            SubtitlePlan {
                source: Some(SubtitleSource::Automatic),
                languages: vec!["zh-Hans".to_string(), "en".to_string()]
            }
        );
    }

    #[test]
    fn parses_bilibili_metadata() {
        let metadata = parse_bilibili_metadata(
            "https://www.bilibili.com/video/BV12TRrBcEP8/",
            "[2026] - 获取aid结束: 1556453868\n[2026] - 视频标题: Workout\n[2026] - 发布时间: 2026-05-05 05:24:12 +01:00\n[2026] - UP主页: https://space.bilibili.com/604003146",
        );

        assert_eq!(metadata.title.as_deref(), Some("Workout"));
        assert_eq!(metadata.publish_date.as_deref(), Some("2026-05-05"));
        assert_eq!(
            metadata.uploader_url.as_deref(),
            Some("https://space.bilibili.com/604003146")
        );
        assert_eq!(metadata.id.as_deref(), Some("BV12TRrBcEP8"));
        assert_eq!(metadata.aid.as_deref(), Some("1556453868"));
    }

    #[cfg(unix)]
    #[test]
    fn finds_bilibili_mux_candidates_in_aid_directory() {
        let root = temp_test_dir("mux-candidates");
        let aid_dir = root.join("1556453868");
        fs::create_dir_all(&aid_dir).expect("aid dir should be created");
        let since = SystemTime::now();
        let video = aid_dir.join("1556453868.P1.1625322228.mp4");
        fs::write(&video, b"video").expect("video should be written");
        fs::write(aid_dir.join("1556453868.P1.1625322228.m4a"), b"audio")
            .expect("audio should be written");
        let mut config = test_config();
        config.downloads.video_dir = root.clone();
        let metadata = BilibiliMetadata {
            aid: Some("1556453868".to_string()),
            ..BilibiliMetadata::default()
        };

        let candidates = bilibili_mux_candidates(&config, &metadata, Vec::new(), since)
            .expect("candidates should scan");

        assert_eq!(candidates, vec![video]);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn renders_nfo_with_xml_escaping() {
        let nfo = render_nfo(
            "A & B",
            &MediaNfo {
                title: Some("A & B"),
                plot: Some("x < y"),
                unique_id_type: "youtube",
                unique_id: "id",
                source_url: "https://example.com/?a=1&b=2",
                studio: Some("Studio"),
                premiered: Some("2026-05-17"),
            },
        );

        assert!(nfo.contains("<title>A &amp; B</title>"));
        assert!(nfo.contains("<plot>x &lt; y</plot>"));
        assert!(nfo.contains("<year>2026</year>"));
    }

    #[test]
    fn extracts_latest_terminal_percent() {
        assert_eq!(
            extract_last_percent("[-----]  12% \u{0008}\u{0008}[###--]  87%"),
            Some(87)
        );
        assert_eq!(
            extract_last_percent("[download] 42.3% of 1.00MiB"),
            Some(42)
        );
        assert_eq!(
            extract_last_percent("[download] 100.0% of 1.00MiB"),
            Some(100)
        );
        assert_eq!(extract_last_percent("no progress"), None);
    }

    #[test]
    fn summarizes_command_progress_percent() {
        assert_eq!(
            summarize_progress_chunk("BBDown", CommandStream::Stdout, "  42% | - 1.2 MB/s"),
            Some("BBDown: 42%".to_string())
        );
    }

    #[test]
    fn summarizes_short_command_lines() {
        assert_eq!(
            summarize_progress_chunk("BBDown", CommandStream::Stdout, "开始合并音视频...\n"),
            Some("BBDown stdout: 开始合并音视频...".to_string())
        );
    }

    #[test]
    fn formats_file_activity_bytes() {
        assert_eq!(human_bytes(42), "42 B");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(2 * 1024 * 1024), "2.0 MiB");
    }

    #[test]
    fn keeps_file_activity_polling_ahead_of_idle_timeout() {
        assert_eq!(
            file_activity_poll_interval(Duration::from_secs(30), Duration::from_secs(300)),
            Duration::from_secs(30)
        );
        assert_eq!(
            file_activity_poll_interval(Duration::from_secs(600), Duration::from_secs(300)),
            Duration::from_secs(150)
        );
        assert_eq!(
            file_activity_poll_interval(Duration::from_secs(30), Duration::from_secs(1)),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn throttles_percent_progress_updates() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut tracker =
            ProgressTracker::new("yt-dlp".to_string(), Duration::from_secs(30), Some(tx));

        tracker.observe(CommandStream::Stdout, b"[download] 1.0%");
        assert_eq!(rx.try_recv().unwrap().message, "yt-dlp: 1%");

        tracker.observe(CommandStream::Stdout, b"[download] 2.0%");
        assert!(rx.try_recv().is_err());

        tracker.next_send_at = Instant::now() - Duration::from_secs(1);
        tracker.observe(CommandStream::Stdout, b"[download] 2.0%");
        assert_eq!(rx.try_recv().unwrap().message, "yt-dlp: 2%");
    }

    #[test]
    fn throttles_file_activity_progress_updates() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut tracker =
            ProgressTracker::new("BBDown".to_string(), Duration::from_secs(30), Some(tx));

        tracker.emit("files: 1 changed, 1.0 MiB written".to_string());
        assert_eq!(
            rx.try_recv().unwrap().message,
            "files: 1 changed, 1.0 MiB written"
        );

        tracker.emit("files: 1 changed, 2.0 MiB written".to_string());
        assert!(rx.try_recv().is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn tracks_files_in_new_direct_subdirectories() {
        let root = temp_test_dir("file-activity");
        let existing = root.join("existing");
        fs::create_dir_all(&existing).expect("existing dir should be created");
        fs::write(existing.join("old.part"), b"old").expect("existing file should be written");
        let mut tracker = FileActivityTracker::new(&root)
            .await
            .expect("tracker should initialize");

        fs::write(existing.join("old.part"), b"changed").expect("existing file should change");
        assert_eq!(tracker.poll().await.expect("poll should work"), None);

        let created = root.join("created");
        fs::create_dir_all(&created).expect("new dir should be created");
        fs::write(created.join("new.part"), b"new bytes").expect("new file should be written");
        let message = tracker.poll().await.expect("poll should work");

        assert_eq!(message, Some("files: 1 changed, 9 B written".to_string()));
        let _ = fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_terminates_descendant_processes() {
        let root = temp_test_dir("process-group");
        let pid_file = root.join("child.pid");
        let mut config = test_config();
        config.bot.command_timeout_seconds = 2;
        config.bot.command_idle_timeout_seconds = 30;
        config.bot.progress_update_seconds = 1;
        let spec = CommandSpec {
            program: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_string(),
                "sleep 30 & echo $! > \"$0\"; wait".to_string(),
                pid_file.display().to_string(),
            ],
            cwd: root.clone(),
            activity_dir: Some(root.clone()),
        };

        let result = run_command(&config, &spec, None).await;

        assert!(result.is_err());
        let pid = fs::read_to_string(&pid_file)
            .expect("child pid should be written")
            .trim()
            .parse::<libc::pid_t>()
            .expect("child pid should parse");
        for _ in 0..20 {
            if !process_exists(pid) {
                let _ = fs::remove_dir_all(&root);
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        let _ = fs::remove_dir_all(&root);
        panic!("descendant process {pid} survived command timeout");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn direct_child_exit_does_not_hang_on_background_pipe_holder() {
        let root = temp_test_dir("background-pipe");
        let pid_file = root.join("child.pid");
        let mut config = test_config();
        config.bot.command_timeout_seconds = 30;
        config.bot.command_idle_timeout_seconds = 30;
        config.bot.progress_update_seconds = 1;
        let spec = CommandSpec {
            program: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_string(),
                "sleep 30 & echo $! > \"$0\"; exit 0".to_string(),
                pid_file.display().to_string(),
            ],
            cwd: root.clone(),
            activity_dir: Some(root.clone()),
        };

        let result = tokio_timeout(Duration::from_secs(8), run_command(&config, &spec, None))
            .await
            .expect("run_command should not hang on inherited pipes");

        result.expect("direct child exit status should be successful");
        let pid = fs::read_to_string(&pid_file)
            .expect("child pid should be written")
            .trim()
            .parse::<libc::pid_t>()
            .expect("child pid should parse");
        for _ in 0..20 {
            if !process_exists(pid) {
                let _ = fs::remove_dir_all(&root);
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        let _ = fs::remove_dir_all(&root);
        panic!("background pipe holder {pid} survived command collection");
    }

    #[cfg(unix)]
    fn temp_test_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after UNIX_EPOCH")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "telegram-video-downloader-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("test temp dir should be created");
        root
    }

    #[cfg(unix)]
    fn process_exists(pid: libc::pid_t) -> bool {
        (unsafe { libc::kill(pid, 0) == 0 })
            || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }

    #[test]
    fn tails_nonempty_lines() {
        assert_eq!(tail_lines("a\n\nb\nc\n", 2), "b\nc");
    }
}
