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
                    if error.downcast_ref::<ReplyRejection>()
                        == Some(&ReplyRejection::DeliveryUnknown) =>
                {
                    awaiting_confirmation += 1;
                    paths.push(path);
                    continue;
                }
                Err(error) => return Err(error.context(retry_notice(&archive))),
            };
            persist_reply_receipt(&root, &archive, &receipt, task.generation, index, &proof)?;
        }
        paths.push(path);
    }
    ensure!(
        awaiting_confirmation == 0,
        "Telegram did not confirm delivery of {awaiting_confirmation} file(s). {}",
        retry_notice(&archive)
    );
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
            return Err(reply_error(response.error.as_deref()).into());
        }
        Ok(())
    })
    .await;
    response.context("Telegram original-file upload timed out")?
}

#[derive(Debug, PartialEq, Eq)]
enum ReplyRejection {
    TooLarge,
    DeliveryUnknown,
    Other(&'static str),
}

impl std::fmt::Display for ReplyRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::TooLarge => "The original file exceeds this Telegram Bot's upload limit",
            Self::DeliveryUnknown => {
                "Telegram did not confirm whether the original file was delivered"
            }
            Self::Other(message) => message,
        })
    }
}

impl std::error::Error for ReplyRejection {}

fn reply_error(category: Option<&str>) -> ReplyRejection {
    let message = match category {
        Some("file_too_large") => return ReplyRejection::TooLarge,
        Some("delivery_unknown") => return ReplyRejection::DeliveryUnknown,
        Some(
            "file_integrity_error" | "file_changed" | "file_unavailable" | "storage_unavailable",
        ) => "The original file no longer matches the saved download",
        Some("rate_limited") => "Telegram has temporarily limited uploads",
        Some("bot_identity_invalid" | "credentials_unavailable") => {
            "The Telegram file sender could not authenticate"
        }
        Some("dependency_unavailable") => {
            "The private Telegram runtime is missing its MTProto dependency"
        }
        Some("deadline_exceeded" | "request_timeout") => "Telegram original-file upload timed out",
        _ => "Telegram did not verify delivery of the original file",
    };
    ReplyRejection::Other(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::JobRequest;
    use crate::social_media::MAX_FILES;

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
