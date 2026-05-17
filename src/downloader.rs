use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use tokio::process::Command;
use tokio::sync::{Mutex, MutexGuard};

use crate::config::AppConfig;
use crate::router::JobRequest;

static VIDEO_OUTPUT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct JobReport {
    pub saved_location: String,
    pub details: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
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
}

pub async fn run_job(config: &AppConfig, job: &JobRequest) -> Result<JobReport> {
    match job {
        JobRequest::Bilibili { url } => run_bilibili_job(config, url).await,
        JobRequest::Youtube { url } => run_youtube_job(config, url).await,
        JobRequest::Pdf { .. } => run_simple_job(config, job).await,
    }
}

async fn run_simple_job(config: &AppConfig, job: &JobRequest) -> Result<JobReport> {
    let spec = command_spec(config, job);
    let output = run_command(&spec).await?;
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

async fn run_bilibili_job(config: &AppConfig, url: &str) -> Result<JobReport> {
    let _guard = video_output_lock().await;
    let mut nfo_warnings = Vec::new();
    let before = if config.video.write_nfo {
        match list_video_files(&config.downloads.video_dir) {
            Ok(files) => Some(files),
            Err(err) => {
                nfo_warnings.push(format!(
                    "NFO skipped: failed to scan before download: {err}"
                ));
                None
            }
        }
    } else {
        None
    };
    let spec = bilibili_command_spec(config, url);
    let output = run_command(&spec).await?;
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
                match write_nfos_for_videos(
                    &created_videos,
                    &MediaNfo {
                        title: metadata.title.as_deref(),
                        plot: None,
                        unique_id_type: "bilibili",
                        unique_id: metadata.id.as_deref().unwrap_or(url),
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
            Err(err) => details.push(format!("NFO skipped: failed to scan after download: {err}")),
        }
    }

    Ok(JobReport {
        saved_location: config.downloads.video_dir.display().to_string(),
        details: nonempty_join(details),
    })
}

async fn run_youtube_job(config: &AppConfig, url: &str) -> Result<JobReport> {
    let metadata = fetch_youtube_metadata(config, url).await?;
    let subtitle_plan = select_subtitles(&metadata, &config.video.subtitle_languages);
    let _guard = video_output_lock().await;
    let spec = youtube_download_command_spec(config, url, &subtitle_plan);
    let output = run_command(&spec).await?;
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
    CommandSpec {
        program: config.tools.bbdown.clone(),
        args: vec![url.to_string(), "--skip-ai".to_string()],
        cwd: config.downloads.video_dir.clone(),
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
    }
}

async fn fetch_youtube_metadata(config: &AppConfig, url: &str) -> Result<YoutubeMetadata> {
    let spec = youtube_metadata_command_spec(config, url);
    let output = run_command(&spec).await?;
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

async fn run_command(spec: &CommandSpec) -> Result<std::process::Output> {
    Command::new(&spec.program)
        .args(&spec.args)
        .current_dir(&spec.cwd)
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("failed to run {}", spec.program.display()))
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
    use std::path::PathBuf;

    use crate::config::AppConfig;

    use super::*;

    fn test_config() -> AppConfig {
        AppConfig::load(Path::new("config.example.toml")).expect("example config should parse")
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

        assert_eq!(
            spec.program,
            PathBuf::from("/Users/joey/.dotnet/tools/BBDown")
        );
        assert!(spec.args.contains(&"--skip-ai".to_string()));
        assert_eq!(spec.cwd, PathBuf::from("/Users/joey/Movies/Downloads"));
    }

    #[test]
    fn builds_youtube_metadata_command() {
        let config = test_config();
        let spec = youtube_metadata_command_spec(&config, "https://youtu.be/abc");

        assert_eq!(spec.program, PathBuf::from("/Users/joey/.local/bin/yt-dlp"));
        assert!(spec.args.contains(&"--dump-json".to_string()));
        assert!(spec.args.contains(&"--skip-download".to_string()));
        assert!(spec.args.contains(&"--no-playlist".to_string()));
    }

    #[test]
    fn builds_youtube_download_command_with_metadata_sidecars() {
        let config = test_config();
        let subtitle_plan = SubtitlePlan {
            source: Some(SubtitleSource::Manual),
            languages: vec!["en".to_string(), "ja".to_string()],
        };
        let spec = youtube_download_command_spec(&config, "https://youtu.be/abc", &subtitle_plan);

        assert_eq!(spec.program, PathBuf::from("/Users/joey/.local/bin/yt-dlp"));
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
        assert_eq!(spec.cwd, PathBuf::from("/Users/joey/Movies/Downloads"));
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
            "[2026] - 视频标题: Workout\n[2026] - 发布时间: 2026-05-05 05:24:12 +01:00\n[2026] - UP主页: https://space.bilibili.com/604003146",
        );

        assert_eq!(metadata.title.as_deref(), Some("Workout"));
        assert_eq!(metadata.publish_date.as_deref(), Some("2026-05-05"));
        assert_eq!(
            metadata.uploader_url.as_deref(),
            Some("https://space.bilibili.com/604003146")
        );
        assert_eq!(metadata.id.as_deref(), Some("BV12TRrBcEP8"));
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
    fn tails_nonempty_lines() {
        assert_eq!(tail_lines("a\n\nb\nc\n", 2), "b\nc");
    }
}
