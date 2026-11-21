//! Download media from supported social URLs without exposing browser session data.

use crate::config::AppConfig;
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use url::Url;

pub(crate) const MAX_FILES: usize = 64;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const DIAGNOSTIC_LIMIT: u64 = 128 * 1024;

pub(crate) fn canonicalize_url(raw: &str) -> Option<String> {
    let url = Url::parse(raw).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
    {
        return None;
    }
    let path = url.path().trim_matches('/').split('/').collect::<Vec<_>>();
    match url.host_str()? {
        "x.com" | "www.x.com" | "twitter.com" | "www.twitter.com" | "mobile.twitter.com" => {
            let [owner, "status", id, suffix @ ..] = path.as_slice() else {
                return None;
            };
            if !valid_user(owner)
                || !numeric_id(id)
                || !matches!(suffix, [] | ["video", "1"] | ["photo", "1"])
            {
                return None;
            }
            Some(format!("https://x.com/{owner}/status/{id}"))
        }
        "instagram.com" | "www.instagram.com" | "m.instagram.com" => {
            let base = "https://www.instagram.com";
            match path.as_slice() {
                [kind @ ("p" | "reel" | "tv"), id] if media_code(id) => {
                    Some(format!("{base}/{kind}/{id}/"))
                }
                ["stories", owner, id] if valid_user(owner) && numeric_id(id) => {
                    Some(format!("{base}/stories/{owner}/{id}/"))
                }
                ["s", link] if media_code(link) && link.starts_with("aGlnaGxpZ2h0") => {
                    let mut ids = url.query_pairs().filter(|(key, _)| key == "story_media_id");
                    let (_, id) = ids.next()?;
                    (numeric_id(&id) && ids.next().is_none())
                        .then(|| format!("{base}/s/{link}?story_media_id={id}"))
                }
                [owner] | [owner, "avatar"]
                    if valid_user(owner)
                        && ![
                            "about", "accounts", "api", "direct", "explore", "p", "reel", "s",
                            "stories", "tv",
                        ]
                        .contains(owner) =>
                {
                    Some(format!("{base}/{owner}/avatar/"))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn numeric_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 24 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn media_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 90
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_user(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 30
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_'))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstagramProfiles {
    primary: BrowserProfile,
    fallback: Option<BrowserProfile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserProfile {
    cookies_from_browser: String,
}

impl BrowserProfile {
    fn cookie_scope(&self) -> Result<String> {
        let raw = self
            .cookies_from_browser
            .strip_prefix("chrome:")
            .context("Instagram profile must use Chrome")?;
        let profile = Path::new(raw);
        ensure!(
            profile.is_absolute()
                && !raw.chars().any(char::is_control)
                && !raw.contains("::")
                && !profile
                    .components()
                    .any(|part| matches!(part, std::path::Component::ParentDir)),
            "Instagram browser profile path is invalid"
        );
        Ok(format!("chrome/instagram.com:{raw}"))
    }
}

fn instagram_profiles() -> Result<InstagramProfiles> {
    let home =
        std::env::var_os("HOME").context("Instagram local browser profiles are unavailable")?;
    let directory = PathBuf::from(home).join(".config/telegram-video-downloader");
    let owner = unsafe { libc::geteuid() };
    let parent = std::fs::symlink_metadata(&directory)
        .context("Instagram profiles directory is unavailable")?;
    ensure!(
        parent.is_dir() && parent.uid() == owner && parent.mode() & 0o7777 == 0o700,
        "Instagram profiles directory must be private"
    );
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(directory.join("instagram-profiles.json"))
        .context("Instagram profiles are unavailable")?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file()
            && metadata.uid() == owner
            && metadata.nlink() == 1
            && metadata.mode() & 0o7777 == 0o600,
        "Instagram profiles must be an owner-private regular file"
    );
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= 4096,
        "Instagram profiles file exceeds its size limit"
    );
    serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("Instagram profiles file is invalid"))
}

pub(crate) async fn download(
    config: &AppConfig,
    raw: &str,
    staging: &Path,
) -> Result<Vec<PathBuf>> {
    let url = canonicalize_url(raw).context("Unsupported Instagram or X link")?;
    ensure!(
        std::fs::read_dir(staging)?.next().is_none(),
        "Original media download needs empty staging"
    );

    if url.starts_with("https://x.com/") {
        let mut command = tokio::process::Command::new(&config.tools.yt_dlp);
        command.args([
            "--ignore-config",
            "--no-plugin-dirs",
            "--no-playlist",
            "--no-progress",
            "--retries",
            "0",
            "--extractor-retries",
            "0",
            "--fragment-retries",
            "0",
            "--socket-timeout",
            "30",
            "--paths",
            ".",
            "--output",
            "%(id)s.%(ext)s",
        ]);
        if config.tools.ffmpeg != Path::new("ffmpeg") {
            command.arg("--ffmpeg-location").arg(&config.tools.ffmpeg);
        }
        command.arg("--").arg(&url);
        let (status, _) = run_worker(command, staging).await?;
        ensure!(status.success(), "Could not download this X video");
        return media_files(staging);
    }

    let profiles = tokio::task::spawn_blocking(instagram_profiles).await??;
    download_instagram(config, &url, staging, &profiles).await
}

async fn download_instagram(
    config: &AppConfig,
    url: &str,
    staging: &Path,
    profiles: &InstagramProfiles,
) -> Result<Vec<PathBuf>> {
    let (status, stderr) = run_instagram(config, url, staging, &profiles.primary).await?;
    if status.success()
        && !stderr.contains("[error]")
        && std::fs::read_dir(staging)?.next().is_some()
    {
        return media_files(staging);
    }
    let failure = instagram_failure(&stderr);
    if failure == InstagramFailure::Account
        && let Some(fallback) = &profiles.fallback
        && std::fs::read_dir(staging)?.next().is_none()
    {
        let (status, stderr) = run_instagram(config, url, staging, fallback).await?;
        if status.success()
            && !stderr.contains("[error]")
            && std::fs::read_dir(staging)?.next().is_some()
        {
            return media_files(staging);
        }
        bail!("{}", instagram_failure(&stderr).message());
    }
    bail!("{}", failure.message());
}

async fn run_instagram(
    config: &AppConfig,
    url: &str,
    staging: &Path,
    profile: &BrowserProfile,
) -> Result<(ExitStatus, String)> {
    let mut command = tokio::process::Command::new(&config.tools.gallery_dl);
    command.args([
        "--config-ignore",
        "--no-input",
        "--retries",
        "0",
        "--range",
        "1-65",
    ]);
    command.arg("--directory").arg(staging);
    command.args([
        "--filename",
        "{num:0>3}.{extension}",
        "--cookies-from-browser",
    ]);
    command.arg(profile.cookie_scope()?);
    command.arg("--").arg(url);
    run_worker(command, staging).await
}

#[derive(PartialEq, Eq)]
enum InstagramFailure {
    Account,
    Rate,
    Download,
}

impl InstagramFailure {
    fn message(self) -> &'static str {
        match self {
            Self::Account => "The saved Instagram account could not access this media",
            Self::Rate => "Instagram has temporarily limited downloads",
            Self::Download => "Could not download the Instagram media",
        }
    }
}

fn instagram_failure(stderr: &str) -> InstagramFailure {
    let text = stderr.to_ascii_lowercase();
    if [
        "http 429",
        "429 client error",
        "status code 429",
        "too many requests",
        "rate limit",
        "please wait a few minutes",
    ]
    .iter()
    .any(|needle| text.contains(needle))
    {
        return InstagramFailure::Rate;
    }
    // CDN/downloader errors may use the same status codes as Instagram's API.
    // Only the provider extractor can classify those as account access problems.
    if text.lines().any(|line| {
        (line.starts_with("[instagram]") || line.starts_with("[cookies]"))
            && [
                "login required",
                "login_required",
                "unauthorized",
                "authorizationerror",
                "authentication",
                "private account",
                "requested story could not be found",
                "notfounderror",
                "no results for",
                "challenge_required",
                "checkpoint_required",
                "could not load cookies",
                "failed to load cookies",
                "login with browser cookies",
                "403 forbidden",
            ]
            .iter()
            .any(|needle| line.contains(needle))
    }) {
        return InstagramFailure::Account;
    }
    InstagramFailure::Download
}

struct ProcessGroup(Option<i32>);

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if let Some(id) = self.0 {
            unsafe { libc::kill(-id, libc::SIGKILL) };
        }
    }
}

async fn run_worker(
    mut command: tokio::process::Command,
    staging: &Path,
) -> Result<(ExitStatus, String)> {
    command
        .current_dir(staging)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .context("Could not start the local social media downloader")?;
    let mut group = ProcessGroup(child.id().and_then(|id| i32::try_from(id).ok()));
    let stderr = child
        .stderr
        .take()
        .context("Downloader diagnostics are unavailable")?;
    let mut diagnostic = Vec::new();
    let mut limited = stderr.take(DIAGNOSTIC_LIMIT + 1);
    let status = tokio::time::timeout(DOWNLOAD_TIMEOUT, async {
        limited.read_to_end(&mut diagnostic).await?;
        ensure!(
            diagnostic.len() as u64 <= DIAGNOSTIC_LIMIT,
            "Downloader diagnostics exceeded their size limit"
        );
        child
            .wait()
            .await
            .context("Could not collect the downloader exit status")
    })
    .await
    .context("Social media download timed out")??;
    group.0 = None;
    Ok((status, String::from_utf8_lossy(&diagnostic).into_owned()))
}

fn media_files(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or_default();
        ensure!(
            entry.file_type()?.is_file()
                && !entry.file_name().to_string_lossy().starts_with('.')
                && matches!(
                    extension.to_ascii_lowercase().as_str(),
                    "mp4"
                        | "webm"
                        | "mov"
                        | "mkv"
                        | "jpg"
                        | "jpeg"
                        | "png"
                        | "webp"
                        | "avif"
                        | "gif"
                ),
            "A social media download left an unsupported or incomplete file"
        );
        files.push(path);
        ensure!(
            files.len() <= MAX_FILES,
            "Social media download exceeds the attachment count limit"
        );
    }
    ensure!(!files.is_empty(), "No original media files were downloaded");
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_exact_story_id_and_supported_posts_are_routed() {
        assert_eq!(
            canonicalize_url("https://instagram.com/creator/"),
            Some("https://www.instagram.com/creator/avatar/".into())
        );
        assert_eq!(
            canonicalize_url(
                "https://www.instagram.com/s/aGlnaGxpZ2h0OjEy?story_media_id=456&igsh=tracking"
            ),
            Some("https://www.instagram.com/s/aGlnaGxpZ2h0OjEy?story_media_id=456".into())
        );
        for unsupported in [
            "https://www.instagram.com/stories/creator/",
            "https://www.instagram.com/s/aGlnaGxpZ2h0OjEy",
            "https://www.instagram.com/s/aGlnaGxpZ2h0OjEy?story_media_id=1&story_media_id=2",
            "https://www.instagram.com.example.com/reel/ABC/",
            "https://twitter.com/creator/",
        ] {
            assert_eq!(canonicalize_url(unsupported), None, "{unsupported}");
        }
        assert_eq!(
            canonicalize_url("http://twitter.com/creator/status/12345?unused=1"),
            Some("https://x.com/creator/status/12345".into())
        );
    }

    #[tokio::test]
    async fn instagram_uses_fallback_only_for_account_access_with_empty_staging() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = tempfile::tempdir().unwrap();
        let temp = fixture.path();
        let worker = temp.join("gallery-fixture");
        std::fs::write(
            &worker,
            r#"#!/bin/sh
for arg do
    case "$arg" in
        'chrome/instagram.com:/test/primary-access') echo '[instagram][error] Login required' >&2; exit 1 ;;
        'chrome/instagram.com:/test/primary-empty') echo '[instagram][info] No results for this exact link' >&2; exit 0 ;;
        'chrome/instagram.com:/test/primary-rate') echo '[instagram][error] HTTP 429: Too Many Requests' >&2; exit 1 ;;
        'chrome/instagram.com:/test/primary-partial')
            printf 'partial' >002.mp4.part; echo '[instagram][error] Login required' >&2; exit 1 ;;
        'chrome/instagram.com:/test/primary-generic')
            echo '[cookies][info] Extracted cookies' >&2; echo '[download][error] 403 Forbidden (file not found)' >&2; exit 1 ;;
        'chrome/instagram.com:/test/fallback')
            printf 'second' >002.mp4; printf 'first' >001.jpg; exit 0 ;;
    esac
done
exit 2
"#,
        )
        .unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut config = AppConfig::for_test();
        config.tools.gallery_dl = worker;
        let url = "https://www.instagram.com/p/Example/";
        for case in ["access", "empty", "rate", "partial", "generic"] {
            let staging = temp.join(case);
            std::fs::create_dir(&staging).unwrap();
            let profiles = InstagramProfiles {
                primary: BrowserProfile {
                    cookies_from_browser: format!("chrome:/test/primary-{case}"),
                },
                fallback: Some(BrowserProfile {
                    cookies_from_browser: "chrome:/test/fallback".into(),
                }),
            };
            let result = download_instagram(&config, url, &staging, &profiles).await;
            if matches!(case, "access" | "empty") {
                assert_eq!(
                    result.unwrap(),
                    [staging.join("001.jpg"), staging.join("002.mp4")]
                );
            } else {
                assert!(result.is_err(), "{case}");
                assert!(
                    !staging.join("001.jpg").exists(),
                    "{case} must not use fallback"
                );
            }
        }
        assert_eq!(
            std::fs::read(temp.join("partial/002.mp4.part")).unwrap(),
            b"partial"
        );
    }
}
