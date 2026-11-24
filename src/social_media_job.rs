//! Publish original social media before sending it back to the Telegram message.

use crate::config::AppConfig;
use crate::downloader::{JobProgressSender, JobReport, send_progress};
use crate::queue::TaskRecord;
use crate::safe_fs::{EntryIdentity, RootedFs};
use crate::social_media::max_files_for_url;
use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) const STAGING_DIR: &str = ".telegram-video-downloader-social-staging";
const ARCHIVE_DIR: &str = ".telegram-video-downloader-social";
const MANIFEST: &str = ".telegram-video-downloader-social-originals.json";
const MAX_MANIFEST: usize = 32 * 1024;
const MAX_HELPER_RESPONSE: usize = 1024;
const REPLY_TIMEOUT: Duration = Duration::from_secs(31 * 60);
const REPLY_HELPER: &str = include_str!("../scripts/social_media_reply.py");

#[derive(Debug, Deserialize, Serialize)]
struct Originals {
    version: u32,
    chat_id: i64,
    message_id: i64,
    url: String,
    files: Vec<Original>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Original {
    name: String,
    sha256: String,
}

#[derive(Serialize)]
struct ReplyRequest<'a> {
    bot_token: &'a str,
    credentials_path: &'a Path,
    chat_id: i64,
    reply_to_message_id: i64,
    random_id: i64,
    file_path: &'a Path,
    file_name: &'a str,
    expected_sha256: &'a str,
}

#[derive(Deserialize)]
struct ReplyResponse {
    ok: bool,
    error: Option<String>,
    phase: Option<serde_json::Value>,
    diagnostic: Option<serde_json::Value>,
}

pub(crate) fn owner_matches(
    config: &AppConfig,
    chat_id: i64,
    submitter_user_id: Option<i64>,
    message_id: i64,
) -> bool {
    !config.telegram.allow_all_chats
        && chat_id > 0
        && config.telegram.allowed_chat_ids.as_slice() == [chat_id]
        && submitter_user_id == Some(chat_id)
        && (1..=i64::from(i32::MAX)).contains(&message_id)
}

pub(crate) fn bot_id(config: &AppConfig) -> Option<i64> {
    let (prefix, secret) = config.telegram.token.split_once(':')?;
    if secret.is_empty() || !prefix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    prefix.parse::<i64>().ok().filter(|id| *id > 0)
}

pub(crate) async fn run(
    config: &AppConfig,
    task: &TaskRecord,
    url: &str,
    progress: Option<&JobProgressSender>,
) -> Result<JobReport> {
    ensure!(
        owner_matches(
            config,
            task.chat_id,
            task.submitter_user_id,
            task.message_id
        ),
        "Social media replies require the configured private owner and the original Telegram message"
    );
    ensure!(
        bot_id(config).is_some_and(|id| task.social_bot_id == Some(id)),
        "Use the Telegram Bot that received the original message to resume this download"
    );
    let root = RootedFs::new(config.video_storage_root())?;
    let archive = archive_path(config, task)?;
    let originals = if root.entry_identity_if_parent_exists(&archive)?.is_some() {
        load_originals(&root, &archive, task, url)?
    } else {
        send_progress(progress, "Downloading original media on the Mac".into());
        download_and_publish(config, &root, &archive, task, url).await?
    };

    let mut paths = Vec::with_capacity(originals.files.len());
    let mut sent = 0;
    let mut oversized = 0;
    let mut awaiting_confirmation = 0;
    let mut first_delivery_failure = None;
    for (index, original) in originals.files.iter().enumerate() {
        let path = archive.join(&original.name);
        let receipt = archive.join(format!(
            ".telegram-video-downloader-social-delivery-{}",
            index + 1
        ));
        let random_id = reply_random_id(task, index, original);
        let sent_receipt = format!("sent:{random_id}");
        let oversized_receipt = format!("too-large:{random_id}");

        let saved_receipt = root
            .open_bound_file(&receipt)?
            .map(|file| {
                file.validate_private_single_link(0o600)?;
                file.read_limited(32)
            })
            .transpose()?;
        if let Some(saved_receipt) = saved_receipt {
            if saved_receipt == sent_receipt.as_bytes() {
                sent += 1;
            } else if saved_receipt == oversized_receipt.as_bytes() {
                oversized += 1;
            } else {
                bail!("Saved Telegram reply receipt does not match the original media");
            }
        } else {
            let file = root
                .open_bound_file(&path)?
                .context("Saved original is missing")?;
            file.validate_private_single_link(0o600)?;
            ensure!(file.byte_len()? > 0, "Saved original is empty");
            let relative = path.strip_prefix(root.logical_root_path())?;
            let canonical_path = root.root_path().join(relative);
            send_progress(
                progress,
                format!(
                    "Replying with original file {} of {}",
                    index + 1,
                    originals.files.len()
                ),
            );
            let result = send_reply(config, task, &canonical_path, original, random_id).await;
            if let Err(error) = &result
                && let Some(rejection) = error.downcast_ref::<ReplyRejection>()
            {
                tracing::warn!(
                    task_id = %task.id,
                    generation = task.generation,
                    attachment_ordinal = index + 1,
                    category = rejection.category_label(),
                    phase = rejection.phase_label(),
                    diagnostic = rejection.diagnostic_label(),
                    rpc_code = ?rejection.rpc_code(),
                    "Telegram social attachment send failed"
                );
            }
            let proof = match result {
                Ok(()) => {
                    sent += 1;
                    sent_receipt
                }
                Err(error)
                    if error.downcast_ref::<ReplyRejection>()
                        == Some(&ReplyRejection::TooLarge) =>
                {
                    oversized += 1;
                    oversized_receipt
                }
                Err(error)
                    if error
                        .downcast_ref::<ReplyRejection>()
                        .is_some_and(ReplyRejection::is_delivery_unknown) =>
                {
                    if first_delivery_failure.is_none() {
                        first_delivery_failure = error
                            .downcast_ref::<ReplyRejection>()
                            .and_then(ReplyRejection::failure_info)
                            .cloned();
                    }
                    awaiting_confirmation += 1;
                    paths.push(path);
                    continue;
                }
                Err(error) => return Err(retryable_reply_error(error, &archive)),
            };
            persist_reply_receipt(&root, &archive, &receipt, task.generation, index, &proof)?;
        }
        paths.push(path);
    }
    if awaiting_confirmation > 0 {
        bail!(unconfirmed_delivery_message(
            awaiting_confirmation,
            first_delivery_failure.as_ref(),
            &archive
        ));
    }
    Ok(JobReport {
        saved_location: archive.display().to_string(),
        details: if oversized > 0 {
            format!(
                "{sent} uploaded to Telegram. {oversized} over the upload limit and saved locally."
            )
        } else if sent == 1 {
            "Replied with one original file.".into()
        } else {
            format!("Replied with {sent} original files.")
        },
        primary_media_paths: paths,
        existing_bilibili_evidence: None,
    })
}

fn retry_notice(archive: &Path) -> String {
    format!(
        "Originals saved in {}. Retry this task from /queue to finish sending them.",
        archive.display()
    )
}

fn retryable_reply_error(error: anyhow::Error, archive: &Path) -> anyhow::Error {
    if let Some(rejection) = error.downcast_ref::<ReplyRejection>() {
        anyhow!("{rejection}. {}", retry_notice(archive))
    } else {
        error.context(retry_notice(archive))
    }
}

fn persist_reply_receipt(
    root: &RootedFs,
    archive: &Path,
    receipt: &Path,
    generation: u64,
    index: usize,
    delivery: &str,
) -> Result<()> {
    let pending = archive.join(format!(
        ".telegram-video-downloader-social-receipt-{generation}-{index}-pending"
    ));
    let (source, identity) = root.create_new_bound_file(&pending, delivery.as_bytes(), 0o600)?;
    let target = root.bind_entry(receipt, false)?;
    root.rename_via_bound_parents_noreplace_if_identity(&source, &target, identity)
}

fn archive_path(config: &AppConfig, task: &TaskRecord) -> Result<PathBuf> {
    ensure!(
        !task.id.is_empty()
            && task
                .id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "Social task ID is invalid"
    );
    Ok(config.downloads.video_dir.join(ARCHIVE_DIR).join(&task.id))
}

async fn download_and_publish(
    config: &AppConfig,
    root: &RootedFs,
    archive: &Path,
    task: &TaskRecord,
    url: &str,
) -> Result<Originals> {
    let staging_root = root.logical_root_path().join(STAGING_DIR);
    root.create_dir(&staging_root, 0o700)?;
    private_directory_identity(root, &staging_root)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let staging = staging_root.join(format!("{}-{}-{now}", task.id, task.generation));
    let staging_identity = root
        .create_dir(&staging, 0o700)?
        .context("Social download staging directory already exists")?;
    let files = crate::social_media::download(config, url, &staging).await?;
    ensure!(private_directory_identity(root, &staging)? == staging_identity);
    let worker_root = root.clone();
    let worker_staging = staging.clone();
    let maximum_files = max_files_for_url(url);
    let mut originals = Originals {
        version: 1,
        chat_id: task.chat_id,
        message_id: task.message_id,
        url: url.to_owned(),
        files: Vec::new(),
    };
    // Hashes bind the file bytes to the retry and to the subsequent MTProto upload.
    let originals = tokio::task::spawn_blocking(move || {
        ensure!(
            (1..=maximum_files).contains(&files.len()),
            "Invalid number of social media files"
        );
        let root = &worker_root;
        let mut names = BTreeSet::new();
        for path in files {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| valid_name(name))
                .context("Social download returned an invalid file name")?;
            ensure!(
                path.parent() == Some(worker_staging.as_path()) && names.insert(name.to_owned()),
                "Social download returned an unexpected file path"
            );
            let file = root
                .open_bound_file(&path)?
                .context("Downloaded media is missing")?;
            let policy = file.access_policy()?;
            ensure!(
                policy.owner == unsafe { libc::geteuid() } && policy.link_count == 1,
                "Downloaded media is not exclusively owned by the bot user"
            );
            file.duplicate_std_file()?
                .set_permissions(std::fs::Permissions::from_mode(0o600))?;
            file.validate_private_single_link(0o600)?;
            let stamp = file.content_stamp()?;
            ensure!(stamp.0 > 0, "Downloaded media is empty");
            let sha256 = hex_digest(&file.sha256()?);
            ensure!(
                stamp == file.content_stamp()?,
                "Downloaded media changed while hashing"
            );
            file.sync_all()?;
            originals.files.push(Original {
                name: name.to_owned(),
                sha256,
            });
        }
        Ok::<_, anyhow::Error>(originals)
    })
    .await
    .context("Social media hashing worker failed")??;
    let body = serde_json::to_vec(&originals)?;
    ensure!(
        body.len() <= MAX_MANIFEST,
        "Social media manifest exceeds its size limit"
    );
    let source = root.bind_entry(&staging, false)?;
    root.validate_private_bound_directory(&source, staging_identity, 0o700)?;
    root.create_new_bound_file(&staging.join(MANIFEST), &body, 0o600)?;
    let parent = archive
        .parent()
        .context("Social archive parent is missing")?;
    root.create_dir(parent, 0o700)?;
    private_directory_identity(root, parent)?;
    let target = root.bind_entry(archive, false)?;
    root.rename_via_bound_parents_noreplace_if_identity(&source, &target, staging_identity)?;
    Ok(originals)
}

fn private_directory_identity(root: &RootedFs, path: &Path) -> Result<EntryIdentity> {
    let entry = root.bind_entry(path, false)?;
    let identity = root
        .bound_entry_identity(&entry)?
        .context("Social media directory is missing")?;
    root.validate_private_bound_directory(&entry, identity, 0o700)?;
    Ok(identity)
}

fn load_originals(
    root: &RootedFs,
    archive: &Path,
    task: &TaskRecord,
    url: &str,
) -> Result<Originals> {
    private_directory_identity(root, archive)?;
    let file = root
        .open_bound_file(&archive.join(MANIFEST))?
        .context("Existing social download has no verified original-media manifest")?;
    file.validate_private_single_link(0o600)?;
    let originals: Originals = serde_json::from_slice(&file.read_limited(MAX_MANIFEST)?)
        .context("Saved social media manifest is invalid")?;
    ensure!(
        originals.version == 1
            && originals.chat_id == task.chat_id
            && originals.message_id == task.message_id
            && originals.url == url
            && (1..=max_files_for_url(url)).contains(&originals.files.len()),
        "Saved original-media manifest belongs to another request"
    );
    let mut names = BTreeSet::new();
    for original in &originals.files {
        ensure!(
            valid_name(&original.name)
                && names.insert(&original.name)
                && original.sha256.len() == 64
                && original.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "Saved original-media manifest contains invalid file records"
        );
    }
    Ok(originals)
}

pub(crate) fn verify_completed_hashes(
    config: &AppConfig,
    task: &TaskRecord,
    url: &str,
    hashes: &BTreeMap<String, String>,
) -> Result<()> {
    let root = RootedFs::new(config.video_storage_root())?;
    let archive = archive_path(config, task)?;
    let originals = load_originals(&root, &archive, task, url)?;
    ensure!(
        originals.files.len() == hashes.len()
            && originals.files.iter().all(|original| {
                let path = archive.join(&original.name);
                hashes.get(path.to_string_lossy().as_ref()) == Some(&original.sha256)
            }),
        "Archived originals changed before completion verification"
    );
    Ok(())
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && !name.contains(['/', '\\'])
        && !name.chars().any(char::is_control)
        && name.len() <= 255
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn reply_random_id(task: &TaskRecord, index: usize, file: &Original) -> i64 {
    let mut hash = Sha256::new();
    hash.update(b"social-original-v1\0");
    hash.update(task.chat_id.to_le_bytes());
    hash.update(task.message_id.to_le_bytes());
    hash.update(task.id.as_bytes());
    hash.update((index as u64).to_le_bytes());
    hash.update(file.sha256.as_bytes());
    let mut prefix = [0; 8];
    prefix.copy_from_slice(&hash.finalize()[..8]);
    let id = i64::from_le_bytes(prefix);
    if id == 0 { 1 } else { id }
}

async fn send_reply(
    config: &AppConfig,
    task: &TaskRecord,
    path: &Path,
    original: &Original,
    random_id: i64,
) -> Result<()> {
    send_verified_file_reply(
        config,
        task.chat_id,
        task.message_id,
        random_id,
        path,
        &original.name,
        &original.sha256,
    )
    .await
}

/// Send an already-published file through the shared, identity-checked MTProto helper.
/// Callers bind the recipient to its admitting bot and provide the persisted digest; the helper
/// verifies the file bytes before Telegram delivery.
pub(crate) async fn send_verified_file_reply(
    config: &AppConfig,
    chat_id: i64,
    reply_to_message_id: i64,
    random_id: i64,
    path: &Path,
    file_name: &str,
    expected_sha256: &str,
) -> Result<()> {
    #[cfg(not(test))]
    let (python, credentials_path) = (
        crate::music_transfer::expected_python_path()?,
        crate::music_transfer::expected_credentials_path()?,
    );
    #[cfg(test)]
    let (python, credentials_path) = (
        config.music.helper_python.clone(),
        config.music.credentials_path.clone(),
    );
    ensure!(
        python.is_file(),
        "The private Telegram Python runtime is unavailable"
    );
    let request = ReplyRequest {
        bot_token: &config.telegram.token,
        credentials_path: &credentials_path,
        chat_id,
        reply_to_message_id,
        random_id,
        file_path: path,
        file_name,
        expected_sha256,
    };
    call_reply_helper(&python, &request).await
}

async fn call_reply_helper(python: &Path, request: &ReplyRequest<'_>) -> Result<()> {
    let mut child = tokio::process::Command::new(python)
        .args(["-I", "-c", REPLY_HELPER])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("Could not start the private Telegram file sender")?;
    let mut input = child
        .stdin
        .take()
        .context("Telegram sender stdin unavailable")?;
    let output = child
        .stdout
        .take()
        .context("Telegram sender stdout unavailable")?;
    let mut bytes = serde_json::to_vec(request)?;
    bytes.push(b'\n');
    let response = tokio::time::timeout(REPLY_TIMEOUT, async {
        input.write_all(&bytes).await?;
        drop(input);
        let mut response = Vec::new();
        output
            .take((MAX_HELPER_RESPONSE + 1) as u64)
            .read_to_end(&mut response)
            .await?;
        if response.len() > MAX_HELPER_RESPONSE {
            bail!("Private Telegram file sender exceeded its response limit");
        }
        let status = child.wait().await?;
        let response: ReplyResponse = serde_json::from_slice(&response)
            .map_err(|_| anyhow!("No verified result from the private Telegram file sender"))?;
        if !status.success() || !response.ok {
            return Err(reply_error(&response).into());
        }
        Ok(())
    })
    .await;
    response.context("Telegram original-file upload timed out")?
}

#[derive(Debug, PartialEq, Eq)]
enum ReplyRejection {
    TooLarge,
    DeliveryUnknown(ReplyFailureInfo),
    Other(&'static str, ReplyFailureInfo),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplyFailureInfo {
    category: &'static str,
    phase: Option<&'static str>,
    diagnostic: Option<&'static str>,
    rpc_code: Option<u32>,
}

impl ReplyFailureInfo {
    fn from_wire(
        category: Option<&str>,
        phase: Option<&str>,
        diagnostic: Option<&serde_json::Value>,
    ) -> Self {
        const CATEGORIES: &[&str] = &[
            "bot_identity_invalid",
            "credentials_unavailable",
            "deadline_exceeded",
            "dependency_unavailable",
            "delivery_unknown",
            "file_changed",
            "file_integrity_error",
            "file_too_large",
            "file_unavailable",
            "internal_error",
            "invalid_request",
            "rate_limited",
            "request_timeout",
            "storage_unavailable",
            "telegram_error",
        ];
        let category = CATEGORIES
            .iter()
            .copied()
            .find(|candidate| Some(*candidate) == category)
            .unwrap_or("unknown_error");
        let phase = match phase {
            Some("auth") => Some("authentication"),
            Some("upload_part") => Some("upload part"),
            Some("final_send") => Some("final send"),
            _ => None,
        };
        let diagnostic_kind = diagnostic
            .and_then(|value| value.get("kind"))
            .and_then(serde_json::Value::as_str)
            .and_then(|value| match value {
                "network_error" => Some("network error"),
                "rpc_error" => Some("RPC error"),
                "timeout" => Some("timeout"),
                "random_id_duplicate" => Some("duplicate random ID"),
                "rate_limited" => Some("rate limited"),
                _ => None,
            });
        let rpc_code = diagnostic
            .and_then(|value| value.get("rpc_code"))
            .and_then(serde_json::Value::as_u64)
            .and_then(|code| u32::try_from(code).ok())
            .filter(|code| (1..=9999).contains(code))
            .filter(|_| diagnostic_kind.is_some());
        Self {
            category,
            phase,
            diagnostic: diagnostic_kind,
            rpc_code,
        }
    }

    fn safe_summary(&self) -> String {
        let mut parts = vec![self.category.to_owned()];
        if let Some(phase) = self.phase {
            parts.push(phase.to_owned());
        }
        if let Some(diagnostic) = self.diagnostic {
            if diagnostic == "RPC error" {
                parts.push(match self.rpc_code {
                    Some(code) => format!("RPC {code}"),
                    None => diagnostic.to_owned(),
                });
            } else {
                parts.push(diagnostic.to_owned());
                if let Some(code) = self.rpc_code {
                    parts.push(format!("RPC {code}"));
                }
            }
        }
        parts.join(", ")
    }
}

impl ReplyRejection {
    fn is_delivery_unknown(&self) -> bool {
        matches!(self, Self::DeliveryUnknown(_))
    }

    fn failure_info(&self) -> Option<&ReplyFailureInfo> {
        match self {
            Self::DeliveryUnknown(info) | Self::Other(_, info) => Some(info),
            Self::TooLarge => None,
        }
    }

    fn category_label(&self) -> &'static str {
        self.failure_info()
            .map_or("file_too_large", |info| info.category)
    }

    fn phase_label(&self) -> &'static str {
        self.failure_info()
            .and_then(|info| info.phase)
            .unwrap_or("unknown")
    }

    fn diagnostic_label(&self) -> &'static str {
        self.failure_info()
            .and_then(|info| info.diagnostic)
            .unwrap_or("unknown")
    }

    fn rpc_code(&self) -> Option<u32> {
        self.failure_info().and_then(|info| info.rpc_code)
    }
}

impl std::fmt::Display for ReplyRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge => {
                formatter.write_str("The original file exceeds this Telegram Bot's upload limit")
            }
            Self::DeliveryUnknown(info) => write!(
                formatter,
                "Telegram did not confirm whether the original file was delivered ({})",
                info.safe_summary()
            ),
            Self::Other(message, info) => write!(formatter, "{message} ({})", info.safe_summary()),
        }
    }
}

impl std::error::Error for ReplyRejection {}

fn reply_error(response: &ReplyResponse) -> ReplyRejection {
    let phase = response.phase.as_ref().and_then(serde_json::Value::as_str);
    reply_error_fields(
        response.error.as_deref(),
        phase,
        response.diagnostic.as_ref(),
    )
}

fn reply_error_fields(
    category: Option<&str>,
    phase: Option<&str>,
    diagnostic: Option<&serde_json::Value>,
) -> ReplyRejection {
    let info = ReplyFailureInfo::from_wire(category, phase, diagnostic);
    match info.category {
        "file_too_large" => ReplyRejection::TooLarge,
        "delivery_unknown" => ReplyRejection::DeliveryUnknown(info),
        category => {
            let message = match category {
                "file_integrity_error"
                | "file_changed"
                | "file_unavailable"
                | "storage_unavailable" => "The original file no longer matches the saved download",
                "rate_limited" => "Telegram has temporarily limited uploads",
                "bot_identity_invalid" | "credentials_unavailable" => {
                    "The Telegram file sender could not authenticate"
                }
                "dependency_unavailable" => {
                    "The private Telegram runtime is missing its MTProto dependency"
                }
                "deadline_exceeded" | "request_timeout" => {
                    "Telegram original-file upload timed out"
                }
                _ => "Telegram did not verify delivery of the original file",
            };
            ReplyRejection::Other(message, info)
        }
    }
}

fn unconfirmed_delivery_message(
    count: usize,
    info: Option<&ReplyFailureInfo>,
    archive: &Path,
) -> String {
    let detail = info
        .map(|info| format!(" ({})", info.safe_summary()))
        .unwrap_or_default();
    format!(
        "Telegram did not confirm delivery of {count} file(s){detail}. {}",
        retry_notice(archive)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::JobRequest;
    use crate::social_media::MAX_FILES;

    #[test]
    fn reply_diagnostics_are_allowlisted_and_keep_legacy_categories() {
        let legacy: ReplyResponse =
            serde_json::from_str(r#"{"ok":false,"error":"delivery_unknown"}"#).unwrap();
        let rejection = reply_error(&legacy);
        assert!(rejection.is_delivery_unknown());
        assert_eq!(
            rejection.to_string(),
            "Telegram did not confirm whether the original file was delivered (delivery_unknown)"
        );

        let response = ReplyResponse {
            ok: false,
            error: Some("delivery_unknown".into()),
            phase: Some(serde_json::json!("final_send")),
            diagnostic: Some(serde_json::json!({"kind":"rpc_error", "rpc_code":500})),
        };
        let rejection = reply_error(&response);
        assert!(rejection.is_delivery_unknown());
        assert!(
            rejection
                .to_string()
                .contains("delivery_unknown, final send, RPC 500")
        );
        assert_eq!(rejection.rpc_code(), Some(500));

        let invalid = ReplyFailureInfo::from_wire(
            Some("token=secret"),
            Some("/private/path"),
            Some(&serde_json::json!({
                "kind":"raw exception text",
                "rpc_code":50_000
            })),
        );
        let summary = invalid.safe_summary();
        assert_eq!(summary, "unknown_error");
        assert!(!summary.contains("secret"));
        assert!(!summary.contains("private"));
        assert!(!summary.contains("exception"));
        assert!(!summary.contains("50000"));
    }

    #[test]
    fn malformed_optional_diagnostics_preserve_typed_categories() {
        let delivery_unknown: ReplyResponse = serde_json::from_str(
            r#"{"ok":false,"error":"delivery_unknown","phase":17,"diagnostic":{"kind":"rpc_error","rpc_code":-1}}"#,
        )
        .unwrap();
        let rejection = reply_error(&delivery_unknown);
        assert!(rejection.is_delivery_unknown());
        assert!(rejection.to_string().contains("delivery_unknown"));
        assert!(!rejection.to_string().contains("RPC -1"));

        let file_too_large: ReplyResponse = serde_json::from_str(
            r#"{"ok":false,"error":"file_too_large","phase":["final_send"],"diagnostic":"not-an-object"}"#,
        )
        .unwrap();
        assert_eq!(reply_error(&file_too_large), ReplyRejection::TooLarge);
    }

    #[test]
    fn unconfirmed_queue_error_shows_rpc_detail_before_retry_notice() {
        let info = ReplyFailureInfo::from_wire(
            Some("delivery_unknown"),
            Some("final_send"),
            Some(&serde_json::json!({"kind":"rpc_error", "rpc_code":500})),
        );
        let message = unconfirmed_delivery_message(1, Some(&info), Path::new("/archive"));
        let detail_position = message.find("RPC 500").unwrap();
        let retry_position = message.find("Originals saved in /archive").unwrap();
        assert!(detail_position < retry_position);
    }

    #[test]
    fn ordinary_reply_error_puts_safe_diagnostic_before_long_retry_notice() {
        let archive = Path::new("/archive/with/a/long/path/to/the/original/files");
        let rejection = reply_error_fields(
            Some("telegram_error"),
            Some("final_send"),
            Some(&serde_json::json!({"kind":"rpc_error", "rpc_code":500})),
        );
        let message = retryable_reply_error(anyhow::Error::new(rejection), archive).to_string();
        let detail_position = message.find("RPC 500").unwrap();
        let retry_position = message.find("Originals saved in").unwrap();
        assert!(detail_position < retry_position);
        assert!(message.contains("telegram_error, final send, RPC 500"));

        let legacy = reply_error_fields(Some("untrusted raw value"), None, None);
        let legacy_message = retryable_reply_error(anyhow::Error::new(legacy), archive).to_string();
        assert!(legacy_message.contains("unknown_error"));
        assert!(!legacy_message.contains("untrusted raw value"));
    }

    #[test]
    fn reply_error_preserves_too_large_classification() {
        assert_eq!(
            reply_error_fields(Some("file_too_large"), None, None),
            ReplyRejection::TooLarge
        );

        let too_large = anyhow::Error::new(ReplyRejection::TooLarge).context("retry notice");
        assert_eq!(
            too_large.downcast_ref::<ReplyRejection>(),
            Some(&ReplyRejection::TooLarge)
        );

        let delivery_unknown = anyhow::Error::new(reply_error_fields(
            Some("delivery_unknown"),
            Some("final_send"),
            None,
        ))
        .context("retry notice");
        assert!(
            delivery_unknown
                .downcast_ref::<ReplyRejection>()
                .is_some_and(ReplyRejection::is_delivery_unknown)
        );
    }

    #[test]
    fn story_archive_recovers_more_files_than_an_ordinary_social_job() {
        let directory = tempfile::tempdir().unwrap();
        let root = RootedFs::new(directory.path()).unwrap();
        let count = MAX_FILES + 1;
        for (name, url, expected) in [
            (
                "stories",
                "https://www.instagram.com/stories/example/",
                true,
            ),
            ("post", "https://www.instagram.com/p/example/", false),
        ] {
            let archive = root.logical_root_path().join(name);
            root.create_dir(&archive, 0o700).unwrap();
            let task = TaskRecord::new(
                name.into(),
                100,
                23,
                123,
                Some(123),
                0,
                JobRequest::Social { url: url.into() },
            );
            let originals = Originals {
                version: 1,
                chat_id: task.chat_id,
                message_id: task.message_id,
                url: url.into(),
                files: (1..=count)
                    .map(|index| Original {
                        name: format!("{index:03}.jpg"),
                        sha256: "a".repeat(64),
                    })
                    .collect(),
            };
            let body = serde_json::to_vec(&originals).unwrap();
            root.create_new_bound_file(&archive.join(MANIFEST), &body, 0o600)
                .unwrap();
            let result = load_originals(&root, &archive, &task, url);
            if expected {
                assert_eq!(result.unwrap().files.len(), count);
            } else {
                assert!(result.is_err());
            }
        }
    }
}
