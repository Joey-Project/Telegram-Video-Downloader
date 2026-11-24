//! Download media from supported social URLs without exposing browser session data.

use crate::config::AppConfig;
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use url::Url;

pub(crate) const MAX_FILES: usize = 64;
pub(crate) const MAX_STORY_FILES: usize = 100;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const SESSION_STATUS_TIMEOUT: Duration = Duration::from_secs(4);
const DIAGNOSTIC_LIMIT: u64 = 128 * 1024;
const INSTAGRAM_SESSION_HELPER: &str = include_str!("../scripts/instagram_session.py");

pub(crate) fn canonicalize_story_homepage(raw: &str) -> Option<String> {
    let url = Url::parse(raw).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || !matches!(
            url.host_str()?,
            "instagram.com" | "www.instagram.com" | "m.instagram.com"
        )
    {
        return None;
    }
    let path = url.path().strip_prefix('/')?;
    let owner = path.strip_suffix('/').unwrap_or(path);
    if !valid_instagram_homepage_owner(owner) {
        return None;
    }
    Some(format!("https://www.instagram.com/stories/{owner}/"))
}

fn canonical_story_collection_url(raw: &str) -> Option<&str> {
    let owner = raw
        .strip_prefix("https://www.instagram.com/stories/")?
        .strip_suffix('/')?;
    valid_instagram_homepage_owner(owner).then_some(raw)
}

fn valid_instagram_homepage_owner(owner: &str) -> bool {
    valid_user(owner)
        && ![
            "about", "accounts", "api", "direct", "explore", "p", "reel", "s", "stories", "tv",
        ]
        .contains(&owner)
}

pub(crate) fn max_files_for_url(url: &str) -> usize {
    if canonical_story_collection_url(url).is_some() {
        MAX_STORY_FILES
    } else {
        MAX_FILES
    }
}

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
                [owner] | [owner, "avatar"] if valid_instagram_homepage_owner(owner) => {
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
    fn chrome_path(&self) -> Result<&Path> {
        let raw = self
            .cookies_from_browser
            .strip_prefix("chrome:")
            .context("Instagram profile must use Chrome")?;
        let profile = Path::new(raw);
        ensure!(
            valid_chrome_profile_path(profile),
            "Instagram browser profile path is invalid"
        );
        Ok(profile)
    }
}

fn valid_chrome_profile_path(profile: &Path) -> bool {
    profile.is_absolute()
        && profile
            .to_str()
            .is_some_and(|raw| !raw.chars().any(char::is_control) && !raw.contains("::"))
        && !profile
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
}

fn selected_chrome_profile(path: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }
    // A removed profile can still have a backup under its original canonical path.
    path.ancestors()
        .skip(1)
        .find_map(|ancestor| {
            let canonical = std::fs::canonicalize(ancestor).ok()?;
            Some(canonical.join(path.strip_prefix(ancestor).ok()?))
        })
        .unwrap_or_else(|| path.to_path_buf())
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
    let (url, story_collection) = if let Some(story_url) = canonical_story_collection_url(raw) {
        (story_url.to_owned(), true)
    } else {
        (
            canonicalize_url(raw).context("Unsupported Instagram or X link")?,
            false,
        )
    };
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
        let outcome = run_worker(command, staging).await?;
        if !outcome.status.success() {
            bail!(
                "{} — Could not download this X video",
                worker_failure_detail("yt-dlp", &outcome, None)
            );
        }
        return media_files(staging, false);
    }

    let profiles = tokio::task::spawn_blocking(instagram_profiles).await??;
    download_instagram(config, &url, staging, &profiles, story_collection).await
}

async fn download_instagram(
    config: &AppConfig,
    url: &str,
    staging: &Path,
    profiles: &InstagramProfiles,
    story_collection: bool,
) -> Result<Vec<PathBuf>> {
    let mut recovery_attempted = false;
    let primary = run_instagram_with_recovery(
        config,
        url,
        staging,
        &profiles.primary,
        story_collection,
        InstagramProfileRole::Primary,
        &mut recovery_attempted,
    )
    .await?;
    let staging_is_empty = std::fs::read_dir(staging)?.next().is_none();
    if primary.status.success() && !staging_is_empty {
        return media_files(staging, story_collection);
    }
    if primary.diagnostics.failure == InstagramFailure::Account
        && let Some(fallback) = &profiles.fallback
        && staging_is_empty
    {
        let fallback_outcome = run_instagram_with_recovery(
            config,
            url,
            staging,
            fallback,
            story_collection,
            InstagramProfileRole::Fallback,
            &mut recovery_attempted,
        )
        .await?;
        let staging_is_empty = std::fs::read_dir(staging)?.next().is_none();
        if fallback_outcome.status.success() && !staging_is_empty {
            return media_files(staging, story_collection);
        }
        let primary_logout = primary.auth_summary.filter(|summary| {
            !matches!(summary.diagnosis, InstagramAuthDiagnosis::CheckUnavailable)
        });
        bail!(
            "{}",
            instagram_failure_message(
                fallback_outcome.diagnostics.failure,
                story_collection,
                &fallback_outcome,
                staging_is_empty,
                InstagramProfileRole::Fallback,
                primary_logout
                    .or(fallback_outcome.auth_summary)
                    .or(primary.auth_summary),
                primary_logout.is_some(),
            )
        );
    }
    bail!(
        "{}",
        instagram_failure_message(
            primary.diagnostics.failure,
            story_collection,
            &primary,
            staging_is_empty,
            InstagramProfileRole::Primary,
            primary.auth_summary,
            false,
        )
    );
}

async fn run_instagram_with_recovery(
    config: &AppConfig,
    url: &str,
    staging: &Path,
    profile: &BrowserProfile,
    story_collection: bool,
    role: InstagramProfileRole,
    recovery_attempted: &mut bool,
) -> Result<WorkerOutcome> {
    let configured_path = profile.chrome_path()?;
    // Use the same selected path for both the download and any later recovery.
    let selected_path = selected_chrome_profile(configured_path);
    ensure!(
        valid_chrome_profile_path(&selected_path),
        "Resolved Instagram browser profile path is invalid"
    );
    let mut outcome = run_instagram(config, url, staging, &selected_path, story_collection).await?;
    let staging_is_empty = std::fs::read_dir(staging)?.next().is_none();
    let provider_login_rejected = outcome.diagnostics.provider_login_rejected;

    if outcome.status.success() && (!staging_is_empty || !provider_login_rejected) {
        return Ok(outcome);
    }
    if outcome.diagnostics.failure == InstagramFailure::Rate {
        return Ok(outcome);
    }

    let session_status = match config.tools.instagram_session_python.as_deref() {
        Some(python) => Some(instagram_session_status(python, role, staging, &selected_path).await),
        None => None,
    };
    outcome.auth_summary = if session_status == Some(InstagramSessionStatus::MissingOrExpired) {
        Some(InstagramAuthSummary::local_logout())
    } else if provider_login_rejected {
        Some(InstagramAuthSummary::provider_rejected())
    } else if session_status == Some(InstagramSessionStatus::Unavailable) {
        Some(InstagramAuthSummary::check_unavailable())
    } else {
        None
    };

    let known_logout =
        provider_login_rejected || session_status == Some(InstagramSessionStatus::MissingOrExpired);
    if known_logout {
        // A local logout or tagged Instagram login rejection is account-specific.
        // This also preserves fallback behavior for a generic extractor KeyError
        // when the independent status check confirms that local cookies expired.
        outcome.diagnostics.failure = InstagramFailure::Account;
    }

    let recovery_is_safe = staging_is_empty
        && !*recovery_attempted
        && known_logout
        && outcome.diagnostics.failure != InstagramFailure::Rate;
    let Some(python) = config.tools.instagram_session_python.as_deref() else {
        return Ok(outcome);
    };
    if !recovery_is_safe {
        return Ok(outcome);
    }

    *recovery_attempted = true;
    let (range, filename) = instagram_download_limits(story_collection);
    let mut command = tokio::process::Command::new(python);
    command.args([
        "-I",
        "-c",
        INSTAGRAM_SESSION_HELPER,
        "restore",
        role.argument(),
    ]);
    command.arg(url).arg("--directory").arg(staging);
    command.args(["--range", &range, "--filename", filename]);
    command.arg("--profile-path").arg(&selected_path);

    // The bot owns this directory so cancellation also removes the cookie copy.
    let restored = match tempfile::tempdir() {
        Ok(scratch) => {
            command.env("TMPDIR", scratch.path());
            run_worker(command, staging).await
        }
        Err(error) => Err(error.into()),
    };
    match restored {
        Ok(mut recovered) => {
            recovered.auth_summary = outcome.auth_summary.map(|mut summary| {
                summary.recovery_issue = recovered.diagnostics.recovery_issue;
                summary
            });
            if outcome.diagnostics.failure == InstagramFailure::Account
                && recovered.diagnostics.failure != InstagramFailure::Rate
            {
                recovered.diagnostics.failure = InstagramFailure::Account;
            }
            if recovered.status.success() {
                tracing::info!(
                    profile = role.label(),
                    "Instagram session backup recovery attempt completed"
                );
            } else {
                tracing::warn!(
                    profile = role.label(),
                    "Instagram session backup recovery failed"
                );
            }
            Ok(recovered)
        }
        Err(_) => {
            tracing::warn!(
                profile = role.label(),
                "Instagram session backup recovery could not run"
            );
            outcome.diagnostics.recovery_issue = Some(InstagramRecoveryIssue::Unavailable);
            if let Some(summary) = &mut outcome.auth_summary {
                summary.recovery_issue = outcome.diagnostics.recovery_issue;
            }
            Ok(outcome)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InstagramSessionStatus {
    Readable,
    MissingOrExpired,
    Unavailable,
}

async fn instagram_session_status(
    python: &Path,
    role: InstagramProfileRole,
    staging: &Path,
    profile: &Path,
) -> InstagramSessionStatus {
    let Ok(scratch) = tempfile::tempdir() else {
        return InstagramSessionStatus::Unavailable;
    };
    let mut command = tokio::process::Command::new(python);
    command
        .args([
            "-I",
            "-c",
            INSTAGRAM_SESSION_HELPER,
            "status",
            role.argument(),
        ])
        .arg("--profile-path")
        .arg(profile)
        .env("TMPDIR", scratch.path())
        .current_dir(staging)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .kill_on_drop(true);
    let Ok(mut child) = command.spawn() else {
        return InstagramSessionStatus::Unavailable;
    };
    let mut group = ProcessGroup(child.id().and_then(|id| i32::try_from(id).ok()));
    let Ok(Ok(status)) = tokio::time::timeout(SESSION_STATUS_TIMEOUT, child.wait()).await else {
        return InstagramSessionStatus::Unavailable;
    };
    group.0 = None;
    // Exit 0 means a readable, unexpired local cookie exists; it does not prove
    // that Instagram will accept the session.
    match status.code() {
        Some(0) => InstagramSessionStatus::Readable,
        Some(20) => InstagramSessionStatus::MissingOrExpired,
        Some(21) => InstagramSessionStatus::Unavailable,
        _ => InstagramSessionStatus::Unavailable,
    }
}

async fn run_instagram(
    config: &AppConfig,
    url: &str,
    staging: &Path,
    profile: &Path,
    story_collection: bool,
) -> Result<WorkerOutcome> {
    let mut command = tokio::process::Command::new(&config.tools.gallery_dl);
    let (range, filename) = instagram_download_limits(story_collection);
    command.args([
        "--config-ignore",
        "--no-input",
        "--retries",
        "0",
        "--range",
        &range,
    ]);
    command.arg("--directory").arg(staging);
    command.args(["--filename", filename, "--cookies-from-browser"]);
    command.arg(format!("chrome/instagram.com:{}", profile.display()));
    command.arg("--").arg(url);
    run_worker(command, staging).await
}

fn instagram_download_limits(story_collection: bool) -> (String, &'static str) {
    let maximum = if story_collection {
        MAX_STORY_FILES
    } else {
        MAX_FILES
    };
    let filename = if story_collection {
        "{num:0>3}-{media_id}.{extension}"
    } else {
        "{num:0>3}.{extension}"
    };
    (format!("1-{}", maximum + 1), filename)
}

#[derive(Clone, Copy, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkerIssue {
    HttpStatus(u16),
    ExtractorKeyError,
    ExtractorError,
    CookieError,
    ConnectionError,
    RateLimit,
    AccountAccess,
    Unclassified,
}

impl WorkerIssue {
    fn label(self) -> String {
        match self {
            Self::HttpStatus(status) => format!("HTTP {status}"),
            Self::ExtractorKeyError => "extractor KeyError".into(),
            Self::ExtractorError => "extractor error".into(),
            Self::CookieError => "cookie error".into(),
            Self::ConnectionError => "connection error".into(),
            Self::RateLimit => "rate limit".into(),
            Self::AccountAccess => "account access error".into(),
            Self::Unclassified => "no recognized stderr category".into(),
        }
    }
}

#[derive(Clone, Copy)]
struct WorkerDiagnostics {
    failure: InstagramFailure,
    issue: WorkerIssue,
    provider_login_rejected: bool,
    recovery_issue: Option<InstagramRecoveryIssue>,
}

impl WorkerDiagnostics {
    fn parse(stderr: &str) -> Self {
        let failure = instagram_failure(stderr);
        let issue = worker_issue(stderr, failure);
        Self {
            failure,
            issue,
            provider_login_rejected: provider_login_rejection(stderr),
            recovery_issue: instagram_recovery_issue(stderr),
        }
    }
}

struct WorkerOutcome {
    status: ExitStatus,
    diagnostics: WorkerDiagnostics,
    auth_summary: Option<InstagramAuthSummary>,
}

#[derive(Clone, Copy)]
enum InstagramProfileRole {
    Primary,
    Fallback,
}

impl InstagramProfileRole {
    fn label(self) -> &'static str {
        match self {
            Self::Primary => "primary profile",
            Self::Fallback => "fallback profile",
        }
    }

    fn argument(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Fallback => "fallback",
        }
    }
}

#[derive(Clone, Copy)]
enum InstagramAuthDiagnosis {
    LocalLogout,
    ProviderRejected,
    CheckUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InstagramRecoveryIssue {
    NoBackup,
    BackupUnavailable,
    NoUsableLogin,
    Unavailable,
}

#[derive(Clone, Copy)]
struct InstagramAuthSummary {
    diagnosis: InstagramAuthDiagnosis,
    recovery_issue: Option<InstagramRecoveryIssue>,
}

impl InstagramAuthSummary {
    fn local_logout() -> Self {
        Self {
            diagnosis: InstagramAuthDiagnosis::LocalLogout,
            recovery_issue: None,
        }
    }

    fn provider_rejected() -> Self {
        Self {
            diagnosis: InstagramAuthDiagnosis::ProviderRejected,
            recovery_issue: None,
        }
    }

    fn check_unavailable() -> Self {
        Self {
            diagnosis: InstagramAuthDiagnosis::CheckUnavailable,
            recovery_issue: None,
        }
    }

    fn short_message(self) -> String {
        let diagnosis = match self.diagnosis {
            InstagramAuthDiagnosis::LocalLogout => "Instagram login is missing or expired",
            InstagramAuthDiagnosis::ProviderRejected => "Instagram rejected the saved login",
            InstagramAuthDiagnosis::CheckUnavailable => "Instagram login check unavailable",
        };
        let recovery = match self.recovery_issue {
            Some(InstagramRecoveryIssue::NoBackup) => Some("no backup"),
            Some(InstagramRecoveryIssue::BackupUnavailable) => Some("backup unavailable"),
            Some(InstagramRecoveryIssue::NoUsableLogin) => Some("backup has no usable login"),
            Some(InstagramRecoveryIssue::Unavailable) => Some("backup recovery unavailable"),
            None => None,
        };
        recovery.map_or_else(
            || diagnosis.to_owned(),
            |recovery| format!("{diagnosis}; {recovery}"),
        )
    }
}

impl InstagramRecoveryIssue {
    fn message(self) -> &'static str {
        match self {
            Self::NoBackup => "no saved Instagram session backup",
            Self::BackupUnavailable => "saved Instagram session backup is unavailable",
            Self::NoUsableLogin => "saved Instagram session backup has no usable login",
            Self::Unavailable => "Instagram session backup recovery is unavailable",
        }
    }
}

fn instagram_recovery_issue(stderr: &str) -> Option<InstagramRecoveryIssue> {
    for line in stderr.lines() {
        match line.trim() {
            "Instagram session recovery: no backup" => {
                return Some(InstagramRecoveryIssue::NoBackup);
            }
            "Instagram session recovery: backup unavailable" => {
                return Some(InstagramRecoveryIssue::BackupUnavailable);
            }
            "Instagram session recovery: backup has no usable login" => {
                return Some(InstagramRecoveryIssue::NoUsableLogin);
            }
            _ => {}
        }
    }
    None
}

fn provider_login_rejection(stderr: &str) -> bool {
    const REJECTIONS: &[&str] = &["login required", "login_required", "authenticationerror"];
    stderr.lines().any(|line| {
        let line = line.to_ascii_lowercase();
        let Some(payload) = line.strip_prefix("[instagram][error] ") else {
            return false;
        };
        let before_url = payload
            .split_once("https://")
            .or_else(|| payload.split_once("http://"))
            .map_or(payload, |(before, _)| before);
        let error = before_url.trim_start();
        REJECTIONS
            .iter()
            .any(|rejection| error.starts_with(rejection))
            || (error.starts_with("unauthorized") && error.contains("logged out"))
            || (error.starts_with("authrequired:") && error.contains("authenticated cookies"))
    })
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

fn worker_issue(stderr: &str, failure: InstagramFailure) -> WorkerIssue {
    let text = stderr.to_ascii_lowercase();
    if text.lines().any(|line| {
        line.starts_with("[instagram][error] an unexpected error occurred: keyerror - ")
            || line.starts_with("[instagram][error] keyerror:")
    }) {
        return WorkerIssue::ExtractorKeyError;
    }
    if let Some(status) = http_status(&text) {
        return WorkerIssue::HttpStatus(status);
    }
    for line in text.lines() {
        if line.starts_with("[cookies][error] ") {
            return WorkerIssue::CookieError;
        }
        let Some(payload) = downloader_error_payload(line) else {
            continue;
        };
        if ["extractorerror:", "extractor error:"]
            .iter()
            .any(|prefix| payload.starts_with(prefix))
        {
            return WorkerIssue::ExtractorError;
        }
        if [
            "could not load cookies",
            "failed to load cookies",
            "could not copy chrome cookie database",
            "failed to decrypt cookies",
            "could not find chrome cookies database",
        ]
        .iter()
        .any(|prefix| payload.starts_with(prefix))
        {
            return WorkerIssue::CookieError;
        }
        if [
            "connectionerror:",
            "connection error",
            "connection refused",
            "connection reset",
            "could not resolve",
            "network is unreachable",
            "temporary failure in name resolution",
            "timed out",
            "timeouterror:",
            "urlopen error",
            "sslerror:",
            "[ssl: certificate_verify_failed]",
            "certificate verify failed",
        ]
        .iter()
        .any(|prefix| payload.starts_with(prefix))
        {
            return WorkerIssue::ConnectionError;
        }
    }
    match failure {
        InstagramFailure::Rate => WorkerIssue::RateLimit,
        InstagramFailure::Account => WorkerIssue::AccountAccess,
        InstagramFailure::Download => WorkerIssue::Unclassified,
    }
}

fn http_status(stderr: &str) -> Option<u16> {
    for line in stderr.lines().rev() {
        let Some(payload) = downloader_error_payload(line) else {
            continue;
        };
        for marker in [
            "httperror:",
            "http error ",
            "http status code ",
            "http status ",
            "status code ",
            "http ",
        ] {
            if let Some(value) = payload.strip_prefix(marker) {
                if let Some(status) = parse_status_digits(trim_status_quotes(value)) {
                    return Some(status);
                }
                break;
            }
        }
        if line.starts_with('[')
            && let Some((code, _reason)) = payload.split_once(' ')
            && code.len() == 3
            && let Some(status) = parse_status_digits(code)
        {
            return Some(status);
        }
    }
    None
}

fn downloader_error_payload(line: &str) -> Option<&str> {
    let payload = if let Some(payload) = line.strip_prefix("error: ") {
        if let Some((extractor, message)) = payload
            .strip_prefix('[')
            .and_then(|tagged| tagged.split_once("] "))
        {
            message
                .split_once(": ")
                .filter(|(id, _)| {
                    matches!(extractor, "twitter" | "twitter:card")
                        && !id.is_empty()
                        && id.bytes().all(|byte| byte.is_ascii_digit())
                })
                .map_or(message, |(_, error)| error)
        } else {
            payload
        }
    } else {
        let (_, tagged_error) = line.strip_prefix('[')?.split_once(']')?;
        tagged_error.strip_prefix("[error] ")?
    };
    let payload = payload.trim_start();
    Some(
        [
            "unable to download video data: ",
            "unable to download webpage: ",
            "unable to download json metadata: ",
        ]
        .iter()
        .find_map(|prefix| payload.strip_prefix(prefix))
        .unwrap_or(payload),
    )
}

fn trim_status_quotes(value: &str) -> &str {
    value.trim_start_matches(|character: char| {
        character.is_ascii_whitespace() || matches!(character, '\'' | '"')
    })
}

fn parse_status_digits(value: &str) -> Option<u16> {
    let digits = value.bytes().take_while(u8::is_ascii_digit).take(4).count();
    if digits != 3
        || value
            .as_bytes()
            .get(digits)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_'))
    {
        return None;
    }
    let status = value[..digits].parse::<u16>().ok()?;
    (100..=599).contains(&status).then_some(status)
}

fn worker_failure_detail(
    tool: &str,
    outcome: &WorkerOutcome,
    profile: Option<InstagramProfileRole>,
) -> String {
    let status = outcome.status.code().map_or_else(
        || {
            outcome.status.signal().map_or_else(
                || "unknown process status".into(),
                |signal| format!("signal {signal}"),
            )
        },
        |code| format!("exit status {code}"),
    );
    let profile = profile.map_or_else(String::new, |role| format!(", {}", role.label()));
    let issue = outcome
        .diagnostics
        .recovery_issue
        .map(|issue| issue.message().to_owned())
        .unwrap_or_else(|| outcome.diagnostics.issue.label());
    format!("{tool} {status}{profile}, {issue}")
}

fn instagram_failure_message(
    failure: InstagramFailure,
    story_collection: bool,
    outcome: &WorkerOutcome,
    staging_is_empty: bool,
    profile: InstagramProfileRole,
    auth_summary: Option<InstagramAuthSummary>,
    primary_login_failed: bool,
) -> String {
    if story_collection
        && outcome.status.success()
        && staging_is_empty
        && !outcome.diagnostics.provider_login_rejected
    {
        return "No active Instagram Stories were found for this account".into();
    }
    let detail = worker_failure_detail("gallery-dl", outcome, Some(profile));
    let Some(auth_summary) = auth_summary else {
        return format!("{detail} — {}", failure.message());
    };
    let mut diagnosis = auth_summary.short_message();
    if primary_login_failed {
        diagnosis.push_str("; fallback also failed");
    }
    format!("{diagnosis} — {detail} — {}", failure.message())
}

struct ProcessGroup(Option<i32>);

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if let Some(id) = self.0 {
            unsafe { libc::kill(-id, libc::SIGKILL) };
        }
    }
}

async fn run_worker(mut command: tokio::process::Command, staging: &Path) -> Result<WorkerOutcome> {
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
    let stderr = String::from_utf8_lossy(&diagnostic);
    Ok(WorkerOutcome {
        status,
        diagnostics: WorkerDiagnostics::parse(&stderr),
        auth_summary: None,
    })
}

fn media_files(directory: &Path, story_collection: bool) -> Result<Vec<PathBuf>> {
    let maximum = if story_collection {
        MAX_STORY_FILES
    } else {
        MAX_FILES
    };
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
        if files.len() == maximum {
            bail!(
                "{}",
                if story_collection {
                    "Instagram Story collection exceeds the 100-item limit"
                } else {
                    "Social media download exceeds the attachment count limit"
                }
            );
        }
        files.push(path);
    }
    ensure!(
        !files.is_empty(),
        if story_collection {
            "No active Instagram Stories were found for this account"
        } else {
            "No original media files were downloaded"
        }
    );
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_instagram_session_python(
        directory: &Path,
        name: &str,
        status_code: u8,
        restore_body: &str,
    ) -> (PathBuf, PathBuf) {
        use std::os::unix::fs::PermissionsExt;

        fn shell_quote(value: &Path) -> String {
            format!("'{}'", value.to_string_lossy().replace('\'', "'\\''"))
        }

        let executable = directory.join(name);
        let log = directory.join(format!("{name}.log"));
        let script = format!(
            r#"#!/bin/sh
printf '%s:%s\n' "$4" "$5" >> {log}
case "$4" in
    status)
        [ "$6" = --profile-path ] && [ -d "$TMPDIR" ] || exit 38
        exit {status_code}
        ;;
    restore)
        [ "$7" = --directory ] || exit 32
        [ -d "$8" ] || exit 33
        [ "$9" = --range ] || exit 34
        [ "${{11}}" = --filename ] || exit 36
        [ "${{13}}" = --profile-path ] && [ -d "$TMPDIR" ] || exit 38
        case "$6" in
            'https://www.instagram.com/p/Example/')
                [ "${{10}}" = 1-65 ] && [ "${{12}}" = '{{num:0>3}}.{{extension}}' ] || exit 35 ;;
            'https://www.instagram.com/stories/creator/')
                [ "${{10}}" = 1-101 ] && [ "${{12}}" = '{{num:0>3}}-{{media_id}}.{{extension}}' ] || exit 35 ;;
            *) exit 31 ;;
        esac
        {restore_body}
        ;;
esac
exit 39
"#,
            log = shell_quote(&log),
        );
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        (executable, log)
    }

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

    #[test]
    fn instagram_profile_selection_resolves_parent_of_missing_profile() {
        let scratch = tempfile::tempdir().unwrap();
        let profiles = scratch.path().join("profiles");
        std::fs::create_dir(&profiles).unwrap();
        let alias = scratch.path().join("alias");
        std::os::unix::fs::symlink(&profiles, &alias).unwrap();
        let expected = profiles.canonicalize().unwrap();
        assert_eq!(
            selected_chrome_profile(&alias.join("Default")),
            expected.join("Default")
        );
        std::fs::create_dir(profiles.join("has::delimiter")).unwrap();
        assert!(!valid_chrome_profile_path(&selected_chrome_profile(
            &alias.join("has::delimiter")
        )));
    }

    #[test]
    fn story_command_canonicalizes_only_instagram_homepages() {
        let canonical = "https://www.instagram.com/stories/creator/";
        assert_eq!(
            canonicalize_story_homepage("https://instagram.com/creator/?igsh=tracking"),
            Some(canonical.into())
        );
        assert_eq!(
            canonicalize_story_homepage("http://m.instagram.com/creator"),
            Some(canonical.into())
        );
        assert_eq!(canonical_story_collection_url(canonical), Some(canonical));
        assert_eq!(max_files_for_url(canonical), MAX_STORY_FILES);
        assert_eq!(
            max_files_for_url("https://www.instagram.com/p/Example/"),
            MAX_FILES
        );
        assert_eq!(
            canonicalize_url("https://instagram.com/creator/"),
            Some("https://www.instagram.com/creator/avatar/".into()),
            "generic homepage routing must still request the avatar"
        );
        assert_eq!(
            canonicalize_url(canonical),
            None,
            "generic URL routing must continue rejecting a Story collection"
        );

        for unsupported in [
            "https://www.instagram.com/stories/creator/",
            "https://www.instagram.com/creator/reels/",
            "https://www.instagram.com/explore/",
            "https://www.instagram.com.evil.example/creator/",
            "https://user@www.instagram.com/creator/",
            "https://www.instagram.com:8443/creator/",
        ] {
            assert_eq!(
                canonicalize_story_homepage(unsupported),
                None,
                "{unsupported}"
            );
        }
        for unsupported_internal in [
            "http://www.instagram.com/stories/creator/",
            "https://instagram.com/stories/creator/",
            "https://www.instagram.com/stories/creator/?tracking=1",
            "https://www.instagram.com/stories/creator/1/",
            "https://www.instagram.com/stories/explore/",
        ] {
            assert_eq!(
                canonical_story_collection_url(unsupported_internal),
                None,
                "{unsupported_internal}"
            );
        }
    }

    #[test]
    fn media_file_order_uses_story_ordinal_and_story_limit_is_one_hundred() {
        let fixture = tempfile::tempdir().unwrap();
        for name in ["002-story-c.mp4", "001-story-b.mp4", "001-story-a.jpg"] {
            std::fs::write(fixture.path().join(name), b"story").unwrap();
        }
        let files = media_files(fixture.path(), true).unwrap();
        assert_eq!(
            files
                .iter()
                .map(|path| path.file_name().unwrap().to_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["001-story-a.jpg", "001-story-b.mp4", "002-story-c.mp4"]
        );

        let over_limit = tempfile::tempdir().unwrap();
        for index in 1..=MAX_STORY_FILES + 1 {
            std::fs::write(
                over_limit
                    .path()
                    .join(format!("{index:03}-story-{index:03}.jpg")),
                b"story",
            )
            .unwrap();
        }
        let error = media_files(over_limit.path(), true).unwrap_err();
        assert!(error.to_string().contains("100-item limit"));

        let generic_over_limit = tempfile::tempdir().unwrap();
        for index in 1..=MAX_FILES + 1 {
            std::fs::write(
                generic_over_limit.path().join(format!("{index:03}.jpg")),
                b"media",
            )
            .unwrap();
        }
        assert!(
            media_files(generic_over_limit.path(), false)
                .unwrap_err()
                .to_string()
                .contains("attachment count limit")
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
        'chrome/instagram.com:/test/primary-fallback-error') echo '[instagram][error] Login required' >&2; exit 1 ;;
        'chrome/instagram.com:/test/primary-empty') echo '[instagram][info] No results for this exact link' >&2; exit 0 ;;
        'chrome/instagram.com:/test/primary-rate') echo '[instagram][error] HTTP 429: Too Many Requests' >&2; exit 1 ;;
        'chrome/instagram.com:/test/primary-recovered')
            printf 'complete' >003.mp4; echo '[ytdl][error] Cannot import yt-dlp or youtube-dl' >&2; echo '[download][info] Trying fallback URL #1' >&2; exit 0 ;;
        'chrome/instagram.com:/test/primary-story')
            range_ok=0; filename_ok=0; url_ok=0
            for arg do
                case "$arg" in
                    '1-101') range_ok=1 ;;
                    '{num:0>3}-{media_id}.{extension}') filename_ok=1 ;;
                    'https://www.instagram.com/stories/creator/') url_ok=1 ;;
                esac
            done
            if [ "$range_ok" = 1 ] && [ "$filename_ok" = 1 ] && [ "$url_ok" = 1 ]; then
                printf 'third' >002-story-c.mp4; printf 'second' >001-story-b.mp4; printf 'first' >001-story-a.jpg; exit 0
            fi
            exit 3 ;;
        'chrome/instagram.com:/test/primary-no-active')
            echo '[instagram][info] No results for this exact link' >&2; exit 0 ;;
        'chrome/instagram.com:/test/primary-partial')
            printf 'partial' >002.mp4.part; echo '[instagram][error] Login required' >&2; exit 1 ;;
        'chrome/instagram.com:/test/primary-generic')
            echo '[cookies][info] Extracted cookies' >&2; echo '[download][error] 403 Forbidden (file not found)' >&2; exit 1 ;;
        'chrome/instagram.com:/test/primary-http-error')
            echo '[download][error] HttpError: '\''403 Forbidden'\'' https://private.example/private/path?token=secret_token_123 account=@private_account' >&2; exit 22 ;;
        'chrome/instagram.com:/test/primary-http-503')
            echo '[instagram][error] HttpError: '\''503 Service Unavailable'\''' >&2; exit 25 ;;
        'chrome/instagram.com:/test/primary-query-status')
            echo '[download][error] could not fetch https://private.example/archive/timeout/extractorerror?cookie=private_session&pin=427 not found' >&2; exit 24 ;;
        'chrome/instagram.com:/test/primary-key-error')
            echo '[cookies][info] Extracted cookies' >&2; echo '[instagram][error] An unexpected error occurred: KeyError - '\''query'\''. /private/worker/path https://private.example/private/path?token=secret_token_123 account=@private_account' >&2; exit 7 ;;
        'chrome/instagram.com:/test/primary-extractor-error')
            echo '[cookies][info] Extracted cookies' >&2; echo '[instagram][error] Unexpected parser failure for query /private/worker/path' >&2; exit 8 ;;
        'chrome/instagram.com:/test/fallback-error')
            echo '[download][error] HTTP Error 503: Service Unavailable' >&2; exit 17 ;;
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
        for case in [
            "access",
            "empty",
            "recovered",
            "rate",
            "partial",
            "generic",
            "http-error",
            "http-503",
            "query-status",
            "key-error",
            "extractor-error",
            "fallback-error",
        ] {
            let staging = temp.join(case);
            std::fs::create_dir(&staging).unwrap();
            let profiles = InstagramProfiles {
                primary: BrowserProfile {
                    cookies_from_browser: format!("chrome:/test/primary-{case}"),
                },
                fallback: Some(BrowserProfile {
                    cookies_from_browser: if case == "fallback-error" {
                        "chrome:/test/fallback-error".into()
                    } else {
                        "chrome:/test/fallback".into()
                    },
                }),
            };
            let result = download_instagram(&config, url, &staging, &profiles, false).await;
            if matches!(case, "access" | "empty") {
                assert_eq!(
                    result.unwrap(),
                    [staging.join("001.jpg"), staging.join("002.mp4")]
                );
            } else if case == "recovered" {
                assert_eq!(result.unwrap(), [staging.join("003.mp4")]);
            } else {
                let detail = result.unwrap_err().to_string();
                assert!(detail.contains("gallery-dl exit status"), "{detail}");
                let expected_role = if case == "fallback-error" {
                    "fallback profile"
                } else {
                    "primary profile"
                };
                assert!(detail.contains(expected_role), "{detail}");
                assert!(
                    !staging.join("001.jpg").exists(),
                    "{case} must not use fallback"
                );
                if case == "http-error" {
                    assert!(detail.contains("Could not download the Instagram media"));
                    assert!(
                        detail
                            .starts_with("gallery-dl exit status 22, primary profile, HTTP 403 — ")
                    );
                    for secret in [
                        "secret_token_123",
                        "private.example",
                        "/private/worker/path",
                        "@private_account",
                    ] {
                        assert!(!detail.contains(secret), "leaked {secret}: {detail}");
                    }
                } else if case == "key-error" {
                    assert!(detail.starts_with(
                        "gallery-dl exit status 7, primary profile, extractor KeyError — "
                    ));
                    assert!(detail.contains("extractor KeyError"), "{detail}");
                    assert!(!detail.contains("query"), "leaked key name: {detail}");
                    for secret in [
                        "secret_token_123",
                        "private.example",
                        "/private/worker/path",
                        "@private_account",
                    ] {
                        assert!(!detail.contains(secret), "leaked {secret}: {detail}");
                    }
                } else if case == "extractor-error" {
                    assert!(detail.contains("no recognized stderr category"), "{detail}");
                    assert!(!detail.contains("cookie error"), "{detail}");
                    assert!(!detail.contains("query"), "leaked query: {detail}");
                    assert!(!detail.contains("/private/worker/path"), "{detail}");
                } else if case == "rate" {
                    assert!(
                        detail
                            .starts_with("gallery-dl exit status 1, primary profile, HTTP 429 — ")
                    );
                } else if case == "fallback-error" {
                    assert!(
                        detail.starts_with(
                            "Instagram rejected the saved login; fallback also failed — "
                        ),
                        "{detail}"
                    );
                    assert!(
                        detail.contains("gallery-dl exit status 17, fallback profile, HTTP 503 — ")
                    );
                } else if case == "http-503" {
                    assert!(
                        detail
                            .starts_with("gallery-dl exit status 25, primary profile, HTTP 503 — ")
                    );
                } else if case == "query-status" {
                    assert!(detail.starts_with(
                        "gallery-dl exit status 24, primary profile, no recognized stderr category — "
                    ));
                    assert!(!detail.contains("HTTP 427"), "{detail}");
                    assert!(!detail.contains("private.example"), "{detail}");
                    assert!(!detail.contains("pin=427"), "{detail}");
                }
            }
        }
        assert_eq!(
            std::fs::read(temp.join("partial/002.mp4.part")).unwrap(),
            b"partial"
        );

        let story_url = "https://www.instagram.com/stories/creator/";
        let story_staging = temp.join("story");
        std::fs::create_dir(&story_staging).unwrap();
        let story_profile = InstagramProfiles {
            primary: BrowserProfile {
                cookies_from_browser: "chrome:/test/primary-story".into(),
            },
            fallback: None,
        };
        assert_eq!(
            download_instagram(&config, story_url, &story_staging, &story_profile, true)
                .await
                .unwrap(),
            vec![
                story_staging.join("001-story-a.jpg"),
                story_staging.join("001-story-b.mp4"),
                story_staging.join("002-story-c.mp4")
            ]
        );

        let no_active_staging = temp.join("no-active-story");
        std::fs::create_dir(&no_active_staging).unwrap();
        let no_active_profile = InstagramProfiles {
            primary: BrowserProfile {
                cookies_from_browser: "chrome:/test/primary-no-active".into(),
            },
            fallback: None,
        };
        let error = download_instagram(
            &config,
            story_url,
            &no_active_staging,
            &no_active_profile,
            true,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("No active Instagram Stories"));
    }

    #[tokio::test]
    async fn instagram_session_recovery_uses_mock_python_and_stops_after_one_attempt() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = tempfile::tempdir().unwrap();
        let temp = fixture.path();
        let worker = temp.join("gallery-session-fixture");
        std::fs::write(
            &worker,
            r#"#!/bin/sh
for arg do
    case "$arg" in
        'chrome/instagram.com:/test/keyerror') echo "[instagram][error] An unexpected error occurred: KeyError - 'query'" >&2; exit 7 ;;
        'chrome/instagram.com:/test/authorization') echo '[instagram][error] AuthorizationError: insufficient privileges for private content' >&2; exit 1 ;;
        'chrome/instagram.com:/test/rate') echo '[instagram][error] HTTP 429: Too Many Requests' >&2; exit 1 ;;
        'chrome/instagram.com:/test/empty') exit 0 ;;
        'chrome/instagram.com:/test/story-login') echo '[instagram][error] Login required' >&2; exit 0 ;;
        'chrome/instagram.com:/test/login-rejected') echo '[instagram][error] Login required' >&2; exit 1 ;;
        'chrome/instagram.com:/test/fallback-login') echo '[instagram][error] Login required' >&2; exit 1 ;;
        'chrome/instagram.com:/test/fallback-ok') printf 'fallback' >001.jpg; exit 0 ;;
    esac
done
exit 2
"#,
        )
        .unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();
        let url = "https://www.instagram.com/p/Example/";

        let recovered_python = mock_instagram_session_python(
            temp,
            "python-recovered",
            20,
            "printf 'recovered' >\"$8/001.jpg\"; exit 0",
        );
        let config_with = |python| {
            let mut config = AppConfig::for_test();
            config.tools.gallery_dl = worker.clone();
            config.tools.instagram_session_python = Some(python);
            config
        };
        let primary = |name: &str| BrowserProfile {
            cookies_from_browser: format!("chrome:/test/{name}"),
        };

        let recovered_staging = temp.join("recovered");
        std::fs::create_dir(&recovered_staging).unwrap();
        let recovered_profiles = InstagramProfiles {
            primary: primary("keyerror"),
            fallback: None,
        };
        assert_eq!(
            download_instagram(
                &config_with(recovered_python.0),
                url,
                &recovered_staging,
                &recovered_profiles,
                false,
            )
            .await
            .unwrap(),
            [recovered_staging.join("001.jpg")]
        );
        assert_eq!(
            std::fs::read_to_string(recovered_python.1).unwrap(),
            "status:primary\nrestore:primary\n"
        );

        let readable_python = mock_instagram_session_python(
            temp,
            "python-readable",
            0,
            "printf 'unexpected restore' >\"$8/001.jpg\"; exit 0",
        );
        let keyerror_staging = temp.join("keyerror-readable");
        std::fs::create_dir(&keyerror_staging).unwrap();
        let error = download_instagram(
            &config_with(readable_python.0.clone()),
            url,
            &keyerror_staging,
            &InstagramProfiles {
                primary: primary("keyerror"),
                fallback: None,
            },
            false,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("extractor KeyError"), "{error}");
        assert!(!error.contains("login check unavailable"), "{error}");
        assert_eq!(
            std::fs::read_to_string(&readable_python.1).unwrap(),
            "status:primary\n"
        );

        let authorization_staging = temp.join("authorization");
        std::fs::create_dir(&authorization_staging).unwrap();
        let error = download_instagram(
            &config_with(readable_python.0.clone()),
            url,
            &authorization_staging,
            &InstagramProfiles {
                primary: primary("authorization"),
                fallback: None,
            },
            false,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            !error.contains("Instagram rejected the saved login"),
            "{error}"
        );
        assert_eq!(
            std::fs::read_to_string(&readable_python.1).unwrap(),
            "status:primary\nstatus:primary\n"
        );

        let rate_python = mock_instagram_session_python(
            temp,
            "python-rate",
            20,
            "printf 'unexpected restore' >\"$8/001.jpg\"; exit 0",
        );
        let rate_staging = temp.join("rate");
        std::fs::create_dir(&rate_staging).unwrap();
        let error = download_instagram(
            &config_with(rate_python.0),
            url,
            &rate_staging,
            &InstagramProfiles {
                primary: primary("rate"),
                fallback: None,
            },
            false,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("temporarily limited"), "{error}");
        assert!(
            !rate_python.1.exists(),
            "rate limiting must not probe or restore"
        );

        let story_python = mock_instagram_session_python(
            temp,
            "python-story",
            20,
            "printf '%s\n' 'Instagram session recovery: no backup' >&2; exit 21",
        );
        let empty_staging = temp.join("empty-success");
        std::fs::create_dir(&empty_staging).unwrap();
        let error = download_instagram(
            &config_with(story_python.0.clone()),
            url,
            &empty_staging,
            &InstagramProfiles {
                primary: primary("empty"),
                fallback: None,
            },
            false,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("exit status 0"), "{error}");
        assert!(
            !story_python.1.exists(),
            "generic empty success must not probe"
        );

        let story_url = "https://www.instagram.com/stories/creator/";
        let no_story_staging = temp.join("story-empty");
        std::fs::create_dir(&no_story_staging).unwrap();
        let error = download_instagram(
            &config_with(story_python.0.clone()),
            story_url,
            &no_story_staging,
            &InstagramProfiles {
                primary: primary("empty"),
                fallback: Some(primary("fallback-ok")),
            },
            true,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("No active Instagram Stories"), "{error}");
        assert!(
            !story_python.1.exists(),
            "clean empty Stories must not probe or restore"
        );

        let story_staging = temp.join("story-empty-with-login-error");
        std::fs::create_dir(&story_staging).unwrap();
        assert_eq!(
            download_instagram(
                &config_with(story_python.0),
                story_url,
                &story_staging,
                &InstagramProfiles {
                    primary: primary("story-login"),
                    fallback: Some(primary("fallback-ok")),
                },
                true,
            )
            .await
            .unwrap(),
            [story_staging.join("001.jpg")]
        );
        assert_eq!(
            std::fs::read_to_string(story_python.1).unwrap(),
            "status:primary\nrestore:primary\n"
        );

        let failed_python = mock_instagram_session_python(
            temp,
            "python-overflow",
            20,
            "printf '%s' \"$TMPDIR\" >\"$8/../recovery-scratch-path\"; touch \"$TMPDIR/fixture-cookie-copy\"; head -c 131073 /dev/zero >&2; sleep 60",
        );
        let failed_staging = temp.join("failed-recovery");
        std::fs::create_dir(&failed_staging).unwrap();
        let error = download_instagram(
            &config_with(failed_python.0),
            url,
            &failed_staging,
            &recovered_profiles,
            false,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("backup recovery unavailable"), "{error}");
        let scratch = std::fs::read_to_string(temp.join("recovery-scratch-path")).unwrap();
        assert!(!Path::new(&scratch).exists());

        let once_python = mock_instagram_session_python(
            temp,
            "python-once",
            20,
            "if [ \"$5\" = primary ]; then printf '%s\\n' 'Instagram session recovery: no backup' >&2; exit 20; fi; printf 'unexpected fallback restore' >\"$8/001.jpg\"; exit 0",
        );
        let once_staging = temp.join("one-recovery");
        std::fs::create_dir(&once_staging).unwrap();
        let error = download_instagram(
            &config_with(once_python.0),
            url,
            &once_staging,
            &InstagramProfiles {
                primary: primary("keyerror"),
                fallback: Some(primary("fallback-login")),
            },
            false,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            error.starts_with(
                "Instagram login is missing or expired; no backup; fallback also failed — "
            ),
            "{error}"
        );
        assert!(!error.contains("Instagram session recovery:"), "{error}");
        assert_eq!(
            std::fs::read_to_string(once_python.1).unwrap(),
            "status:primary\nrestore:primary\nstatus:fallback\n"
        );
        assert!(std::fs::read_dir(&once_staging).unwrap().next().is_none());
    }

    #[test]
    fn only_tagged_explicit_login_rejections_trigger_session_recovery() {
        assert!(provider_login_rejection(
            "[instagram][error] Login required to access this Story"
        ));
        assert!(provider_login_rejection(
            "[instagram][error] AuthRequired: authenticated cookies needed for private content"
        ));
        assert!(provider_login_rejection(
            "[instagram][error] Unauthorized logged out query: user session is missing"
        ));
        assert!(!provider_login_rejection(
            "[instagram][error] AuthorizationError: insufficient privileges for private content"
        ));
        assert!(!provider_login_rejection(
            "[download][error] login required: https://private.example/login_required"
        ));
        assert!(!provider_login_rejection(
            "[instagram][error] Permission denied: https://private.example/login_required"
        ));
        assert_eq!(
            instagram_recovery_issue("Instagram session recovery: backup has no usable login"),
            Some(InstagramRecoveryIssue::NoUsableLogin)
        );
        assert!(
            InstagramAuthSummary {
                diagnosis: InstagramAuthDiagnosis::LocalLogout,
                recovery_issue: Some(InstagramRecoveryIssue::NoUsableLogin),
            }
            .short_message()
            .encode_utf16()
            .count()
                < 96
        );
    }

    #[test]
    fn x_worker_http_diagnostics_distinguish_status_from_tweet_id() {
        let metadata_error = "error: [twitter] 123456789: unable to download json metadata: http error 503: service unavailable";
        assert_eq!(http_status(metadata_error), Some(503));
        assert_eq!(
            http_status("error: [twitter] 503: no video could be found"),
            None
        );
        let bad_certificate = "ERROR: [twitter] 123456789: Unable to download JSON metadata: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed";
        assert_eq!(
            WorkerDiagnostics::parse(bad_certificate).issue,
            WorkerIssue::ConnectionError
        );
    }

    #[tokio::test]
    async fn x_worker_failure_includes_safe_http_context_only() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = tempfile::tempdir().unwrap();
        let worker = fixture.path().join("yt-dlp-fixture");
        std::fs::write(
            &worker,
            r#"#!/bin/sh
echo 'ERROR: unable to download video data: HTTP Error 403: Forbidden https://private.example/raw?token=secret_token_123 account=@private_account' >&2
exit 23
"#,
        )
        .unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();

        let mut config = AppConfig::for_test();
        config.tools.yt_dlp = worker;
        let staging = fixture.path().join("staging");
        std::fs::create_dir(&staging).unwrap();
        let error = download(&config, "https://x.com/creator/status/123", &staging)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("Could not download this X video"));
        assert!(
            error.starts_with("yt-dlp exit status 23, HTTP 403 — "),
            "{error}"
        );
        for secret in ["secret_token_123", "private.example", "@private_account"] {
            assert!(!error.contains(secret), "leaked {secret}: {error}");
        }
    }
}
