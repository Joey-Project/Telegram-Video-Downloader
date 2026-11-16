//! Explicitly confirmed, append-only refresh of library danmaku sidecars.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{AppConfig, BilibiliDanmakuConfig};
use crate::library::{LibraryItem, LibrarySnapshot, PhysicalItemIdentity, Presence};
use crate::safe_fs::{BoundEntry, BoundFile, BoundFileAccessPolicy, RootedFs};

const MAX_REFRESH_ROWS: usize = 5_000;
const MAX_DANMAKU_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_ASS_FILE_BYTES: usize = 64 * 1024 * 1024;
const MAX_NFO_BYTES: usize = 1024 * 1024;
const MAX_AGGREGATE_REFRESH_BYTES: usize = 256 * 1024 * 1024;
const MAX_REFRESH_RESULT_BYTES: usize = 900 * 1024;
const MAX_REFRESH_DURATION: Duration = Duration::from_secs(15 * 60);
const MAX_PREVIEW_DURATION: Duration = Duration::from_secs(120);
const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const ASS_MARKER_PREFIX: &str = "; TelegramVideoDownloader Danmaku v1: ";
const NO_CID_REASON: &str = "A single recorded Bilibili CID is required.";
const UNSUPPORTED_XML_REASON: &str = "The recorded danmaku XML is unreadable or unsupported.";
const UNSUPPORTED_ASS_REASON: &str =
    "The existing ASS sidecar is not a recognized rendering of this XML.";
const UNSUPPORTED_SIDECAR_REASON: &str = "The danmaku sidecar association is ambiguous or unsafe.";
const MEDIA_IDENTITY_REASON: &str = "The media file identity is unavailable.";
const NOT_PRESENT_REASON: &str = "The media item is not currently available.";
const DEFERRED_BATCH_REASON: &str =
    "This batch reached its local inspection limit; preview the remaining videos in another batch.";
const REVIEWED_FILES_CHANGED: &str =
    "A reviewed media or danmaku file changed; create a fresh preview.";
const GUARD_STOPPED: &str =
    "Library registration or runtime state changed; danmaku writes stopped.";
const REFRESH_FAILED_REASON: &str = "The danmaku source or sidecar update failed.";

static PREVIEW_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) type TestDanmakuFetcher =
    Arc<dyn Fn(u64) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>> + Send + Sync>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct DanmakuRefreshPreview {
    pub id: String,
    pub revision: String,
    pub library_id: String,
    pub root_binding: String,
    pub scope: String,
    pub items: Vec<DanmakuRefreshPreviewItem>,
    pub ready: usize,
    pub skipped: usize,
    pub total: usize,
    pub truncated: bool,
    captures: Vec<CapturedItem>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct DanmakuRefreshPreviewItem {
    pub item_id: String,
    pub title: String,
    pub relative_path: String,
    pub status: String,
    #[serde(default)]
    pub deferred: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct DanmakuRefreshPreviewPublic {
    pub id: String,
    pub revision: String,
    pub library_id: String,
    pub root_binding: String,
    pub scope: String,
    pub items: Vec<DanmakuRefreshPreviewItem>,
    pub ready: usize,
    pub skipped: usize,
    pub total: usize,
    pub truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct DanmakuRefreshResult {
    pub summary: DanmakuRefreshSummary,
    pub items: Vec<DanmakuRefreshResultItem>,
    pub media_files_changed: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct DanmakuRefreshSummary {
    pub updated: usize,
    pub unchanged: usize,
    pub skipped: usize,
    pub failed: usize,
    pub added_comments: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct DanmakuRefreshResultItem {
    pub item_id: String,
    pub title: String,
    pub status: String,
    #[serde(default)]
    pub deferred: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub added_comments: usize,
}

impl DanmakuRefreshPreview {
    pub(super) fn public_value(&self) -> DanmakuRefreshPreviewPublic {
        DanmakuRefreshPreviewPublic {
            id: self.id.clone(),
            revision: self.revision.clone(),
            library_id: self.library_id.clone(),
            root_binding: self.root_binding.clone(),
            scope: self.scope.clone(),
            items: self.items.clone(),
            ready: self.ready,
            skipped: self.skipped,
            total: self.total,
            truncated: self.truncated,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CapturedItem {
    item_id: String,
    title: String,
    relative_path: String,
    cid: u64,
    media_identity: PhysicalItemIdentity,
    xml: SidecarCapture,
    ass: SidecarCapture,
    nfo: Option<SidecarCapture>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SidecarCapture {
    relative_path: String,
    parent: ParentProof,
    state: CapturedFileState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CapturedFileState {
    Missing,
    Present(FileProof),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FileProof {
    identity: PhysicalItemIdentity,
    size_bytes: u64,
    sha256: [u8; 32],
    access: FileAccessProof,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FileAccessProof {
    owner: u32,
    group: u32,
    mode: u32,
    link_count: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ParentProof {
    relative_path: String,
    identity: PhysicalItemIdentity,
    access: Option<DirectoryAccessProof>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DirectoryAccessProof {
    owner: u32,
    group: u32,
    mode: u32,
}

struct ObservedSidecar {
    capture: SidecarCapture,
    bytes: Option<Vec<u8>>,
}

enum CaptureError {
    Skip(&'static str),
    Defer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishFailure {
    Stale,
    Guard,
    Io,
}

struct FetchBudget {
    consumed_bytes: usize,
    started: Instant,
}

pub(super) fn preview(
    root: &RootedFs,
    snapshot: &LibrarySnapshot,
    library_id: &str,
    root_binding: &str,
    expected_revision: &str,
    scope: &str,
    item_ids: Option<&[String]>,
) -> Result<DanmakuRefreshPreview> {
    ensure!(
        snapshot.revision == expected_revision,
        "the media library changed; refresh the library and preview again"
    );
    ensure!(
        !library_id.is_empty() && library_id.len() <= 128,
        "invalid media library identity"
    );
    ensure!(
        !root_binding.is_empty() && root_binding.len() <= 256,
        "invalid media library binding"
    );

    let selected = selected_items(snapshot, scope, item_ids)?;
    ensure!(
        !selected.is_empty(),
        "select at least one media item to refresh"
    );
    ensure!(
        selected.len() <= MAX_REFRESH_ROWS,
        "select no more than 5,000 media items per refresh"
    );

    let mut items = Vec::with_capacity(selected.len());
    let mut captures = Vec::new();
    let mut ready_paths = HashSet::new();
    let mut captured_bytes = 0_usize;
    let inspection_started = Instant::now();
    let mut inspection_deferred = false;

    for item in selected {
        if inspection_deferred {
            items.push(deferred_row(item));
            continue;
        }
        match capture_item(root, item, &mut captured_bytes, inspection_started) {
            Ok(capture) => {
                if !ready_paths.insert(capture.xml.relative_path.clone())
                    || !ready_paths.insert(capture.ass.relative_path.clone())
                {
                    items.push(skipped_row(item, UNSUPPORTED_SIDECAR_REASON));
                } else {
                    items.push(ready_row(item));
                    captures.push(capture);
                }
            }
            Err(CaptureError::Skip(reason)) => items.push(skipped_row(item, reason)),
            Err(CaptureError::Defer) => {
                inspection_deferred = true;
                items.push(deferred_row(item));
            }
        }
    }

    let ready = captures.len();
    let skipped = items.len().saturating_sub(ready);
    let id = preview_id(&snapshot.revision, library_id, root_binding);
    Ok(DanmakuRefreshPreview {
        id,
        revision: snapshot.revision.clone(),
        library_id: library_id.to_owned(),
        root_binding: root_binding.to_owned(),
        scope: scope.to_owned(),
        total: items.len(),
        ready,
        skipped,
        truncated: false,
        items,
        captures,
    })
}

fn selected_items<'a>(
    snapshot: &'a LibrarySnapshot,
    scope: &str,
    item_ids: Option<&[String]>,
) -> Result<Vec<&'a LibraryItem>> {
    match scope {
        "library" => {
            ensure!(
                item_ids.is_none(),
                "library refresh cannot include item IDs"
            );
            ensure!(
                snapshot.items.len() <= MAX_REFRESH_ROWS,
                "library contains more than 5,000 items; select a smaller group"
            );
            Ok(snapshot.items.iter().collect())
        }
        "selected" => {
            let ids = item_ids.context("selected refresh requires media item IDs")?;
            ensure!(
                !ids.is_empty() && ids.len() <= MAX_REFRESH_ROWS,
                "select between 1 and 5,000 media items"
            );
            let by_id = snapshot
                .items
                .iter()
                .map(|item| (item.id.as_str(), item))
                .collect::<HashMap<_, _>>();
            let mut seen = HashSet::with_capacity(ids.len());
            let mut selected = Vec::with_capacity(ids.len());
            for id in ids {
                ensure!(
                    seen.insert(id.as_str()),
                    "selected refresh contains a duplicate media item"
                );
                selected.push(
                    *by_id
                        .get(id.as_str())
                        .context("selected media item is not in the current library snapshot")?,
                );
            }
            Ok(selected)
        }
        _ => bail!("invalid danmaku refresh scope"),
    }
}

fn capture_item(
    root: &RootedFs,
    item: &LibraryItem,
    aggregate_bytes: &mut usize,
    inspection_started: Instant,
) -> std::result::Result<CapturedItem, CaptureError> {
    if item.presence != Presence::Present {
        return Err(CaptureError::Skip(NOT_PRESENT_REASON));
    }
    let cid = source_cid(item).ok_or(CaptureError::Skip(NO_CID_REASON))?;
    let media_path = safe_root_path(root, &item.relative_path)
        .map_err(|_| CaptureError::Skip(MEDIA_IDENTITY_REASON))?;
    let media = root
        .open_bound_file(&media_path)
        .map_err(|_| CaptureError::Skip(MEDIA_IDENTITY_REASON))?
        .ok_or(CaptureError::Skip(MEDIA_IDENTITY_REASON))?;
    let media_identity = media
        .physical_identity()
        .map_err(|_| CaptureError::Skip(MEDIA_IDENTITY_REASON))?;
    if item.physical_identity != Some(media_identity) {
        return Err(CaptureError::Skip(MEDIA_IDENTITY_REASON));
    }

    let (xml_relative, ass_relative) = sidecar_paths(root, item)?;
    let xml = observe_for_preview(
        root,
        &xml_relative,
        MAX_DANMAKU_BODY_BYTES,
        aggregate_bytes,
        inspection_started,
    )?;
    let ass = observe_for_preview(
        root,
        &ass_relative,
        MAX_ASS_FILE_BYTES,
        aggregate_bytes,
        inspection_started,
    )?;
    let nfo = item
        .authoritative_nfo_path
        .as_deref()
        .map(|relative| {
            let observed = observe_for_preview(
                root,
                relative,
                MAX_NFO_BYTES,
                aggregate_bytes,
                inspection_started,
            )?;
            if !matches!(observed.capture.state, CapturedFileState::Present(_)) {
                return Err(CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON));
            }
            Ok(observed.capture)
        })
        .transpose()?;

    let old_xml = match xml.bytes.as_deref() {
        Some(bytes) => {
            let text = std::str::from_utf8(bytes)
                .map_err(|_| CaptureError::Skip(UNSUPPORTED_XML_REASON))?;
            validate_xml(text).map_err(|_| CaptureError::Skip(UNSUPPORTED_XML_REASON))?;
            validate_chatid(text, cid).map_err(|_| CaptureError::Skip(UNSUPPORTED_XML_REASON))?;
            Some(text)
        }
        None => None,
    };
    if old_xml.is_none() && ass.bytes.is_some() {
        return Err(CaptureError::Skip(UNSUPPORTED_ASS_REASON));
    }
    if let (Some(xml_text), Some(ass_bytes)) = (old_xml, ass.bytes.as_deref())
        && !ass_is_generated_prefix(ass_bytes, xml_text)
            .map_err(|_| CaptureError::Skip(UNSUPPORTED_ASS_REASON))?
    {
        return Err(CaptureError::Skip(UNSUPPORTED_ASS_REASON));
    }

    Ok(CapturedItem {
        item_id: item.id.clone(),
        title: bounded_text(&item.title, 240),
        relative_path: item.relative_path.clone(),
        cid,
        media_identity,
        xml: xml.capture,
        ass: ass.capture,
        nfo,
    })
}

fn sidecar_paths(
    root: &RootedFs,
    item: &LibraryItem,
) -> std::result::Result<(String, String), CaptureError> {
    let media_path = Path::new(&item.relative_path);
    validate_relative_path(media_path)
        .map_err(|_| CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON))?;
    let parent = media_path.parent().unwrap_or_else(|| Path::new(""));
    let stem = media_path
        .file_stem()
        .and_then(|name| name.to_str())
        .filter(|stem| !stem.is_empty())
        .ok_or(CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON))?;

    let legacy_xml = parent.join(format!("{stem}.xml"));
    let danmaku_xml = parent.join(format!("{stem}.danmaku.xml"));
    let legacy_ass = parent.join(format!("{stem}.ass"));
    let danmaku_ass = parent.join(format!("{stem}.danmaku.ass"));

    let mut xml_candidates = Vec::new();
    let mut ass_candidates = Vec::new();
    for attachment in &item.attachments {
        let candidate = Path::new(&attachment.relative_path);
        if validate_relative_path(candidate).is_err()
            || candidate.parent().unwrap_or_else(|| Path::new("")) != parent
        {
            continue;
        }
        let Some(name) = candidate.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.eq_ignore_ascii_case(
            legacy_xml
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default(),
        ) || name.eq_ignore_ascii_case(
            danmaku_xml
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default(),
        ) {
            xml_candidates.push(attachment.relative_path.clone());
        }
        if name.eq_ignore_ascii_case(
            legacy_ass
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default(),
        ) || name.eq_ignore_ascii_case(
            danmaku_ass
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default(),
        ) {
            ass_candidates.push(attachment.relative_path.clone());
        }
    }
    xml_candidates.sort();
    xml_candidates.dedup();
    ass_candidates.sort();
    ass_candidates.dedup();

    let preferred_xml_name = danmaku_xml
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let xml_relative = if let Some(path) = preferred_candidate(&xml_candidates, preferred_xml_name)
    {
        path.to_owned()
    } else if xml_candidates.len() == 1 {
        xml_candidates[0].clone()
    } else if xml_candidates.len() > 1 {
        return Err(CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON));
    } else {
        let preferred = path_to_string(&danmaku_xml)
            .map_err(|_| CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON))?;
        let absolute = safe_root_path(root, &preferred)
            .map_err(|_| CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON))?;
        if root
            .entry_identity(&absolute)
            .map_err(|_| CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON))?
            .is_some()
        {
            return Err(CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON));
        }
        preferred
    };

    let xml_path = Path::new(&xml_relative);
    let xml_name = xml_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let expected_ass = if xml_name.to_ascii_lowercase().ends_with(".danmaku.xml") {
        xml_path.with_file_name(format!("{}.danmaku.ass", stem))
    } else {
        xml_path.with_extension("ass")
    };
    let expected_ass_name = expected_ass
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let associated_ass = ass_candidates
        .iter()
        .filter(|path| {
            Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case(expected_ass_name))
        })
        .collect::<Vec<_>>();
    if associated_ass.len() > 1 {
        return Err(CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON));
    }
    let ass_relative = if let Some(path) = associated_ass.first() {
        (**path).clone()
    } else {
        path_to_string(&expected_ass).map_err(|_| CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON))?
    };

    // An unindexed same-name sidecar can belong to another primary file. Only use it when the
    // library index associated it with this media item or when the destination is still absent.
    if associated_ass.is_empty() {
        let absolute = safe_root_path(root, &ass_relative)
            .map_err(|_| CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON))?;
        if root
            .entry_identity(&absolute)
            .map_err(|_| CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON))?
            .is_some()
        {
            return Err(CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON));
        }
    }
    Ok((xml_relative, ass_relative))
}

fn preferred_candidate<'a>(candidates: &'a [String], preferred_name: &str) -> Option<&'a str> {
    candidates
        .iter()
        .find(|path| {
            Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case(preferred_name))
        })
        .map(String::as_str)
}

fn source_cid(item: &LibraryItem) -> Option<u64> {
    let mut cids = HashSet::new();
    for digits in item
        .source_ids
        .iter()
        .filter_map(|value| value.strip_prefix("cid"))
    {
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        let cid = digits.parse::<u64>().ok()?;
        if cid == 0 {
            return None;
        }
        cids.insert(cid);
    }
    if cids.len() != 1 {
        return None;
    }
    cids.drain().next()
}

fn ready_row(item: &LibraryItem) -> DanmakuRefreshPreviewItem {
    DanmakuRefreshPreviewItem {
        item_id: item.id.clone(),
        title: bounded_text(&item.title, 240),
        relative_path: bounded_text(&item.relative_path, 512),
        status: "ready".to_owned(),
        deferred: false,
        reason: None,
    }
}

fn skipped_row(item: &LibraryItem, reason: &str) -> DanmakuRefreshPreviewItem {
    DanmakuRefreshPreviewItem {
        item_id: bounded_text(&item.id, 256),
        title: bounded_text(&item.title, 240),
        relative_path: bounded_text(&item.relative_path, 512),
        status: "skipped".to_owned(),
        deferred: false,
        reason: Some(reason.to_owned()),
    }
}

fn deferred_row(item: &LibraryItem) -> DanmakuRefreshPreviewItem {
    DanmakuRefreshPreviewItem {
        item_id: bounded_text(&item.id, 256),
        title: bounded_text(&item.title, 240),
        relative_path: bounded_text(&item.relative_path, 512),
        status: "skipped".to_owned(),
        deferred: true,
        reason: Some(DEFERRED_BATCH_REASON.to_owned()),
    }
}

fn preview_id(revision: &str, library_id: &str, root_binding: &str) -> String {
    let sequence = PREVIEW_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let mut hasher = Sha256::new();
    hasher.update(b"telegram-video-downloader/danmaku-refresh-preview/v1\0");
    hasher.update(revision.as_bytes());
    hasher.update(library_id.as_bytes());
    hasher.update(root_binding.as_bytes());
    hasher.update(nanos.to_be_bytes());
    hasher.update(sequence.to_be_bytes());
    format!("dmr1:{}", encode_hex(&hasher.finalize()[..16]))
}

pub(super) async fn execute_with_guard(
    config: Arc<AppConfig>,
    root: RootedFs,
    plan: DanmakuRefreshPreview,
    fetcher: Option<TestDanmakuFetcher>,
    guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
) -> Result<DanmakuRefreshResult> {
    tokio::time::timeout(
        MAX_REFRESH_DURATION,
        execute_inner(config, root, plan, fetcher, guard),
    )
    .await
    .map_err(|_| anyhow!("danmaku refresh exceeded its time limit"))?
}

async fn execute_inner(
    config: Arc<AppConfig>,
    root: RootedFs,
    plan: DanmakuRefreshPreview,
    fetcher: Option<TestDanmakuFetcher>,
    guard: Arc<dyn Fn() -> Result<()> + Send + Sync>,
) -> Result<DanmakuRefreshResult> {
    validate_plan_shape(&plan)?;
    validate_result_budget(&plan)?;
    guard().map_err(|_| anyhow!(GUARD_STOPPED))?;
    let request_timeout = crate::bilibili_core::request_timeout(&config)
        .unwrap_or(MAX_REQUEST_TIMEOUT)
        .min(MAX_REQUEST_TIMEOUT);
    let client = if fetcher.is_none() {
        Some(
            reqwest::Client::builder()
                .timeout(request_timeout)
                .user_agent("Mozilla/5.0")
                .build()
                .map_err(|_| anyhow!("danmaku request client could not be created"))?,
        )
    } else {
        None
    };

    let mut summary = DanmakuRefreshSummary::default();
    let mut results = Vec::with_capacity(plan.items.len());
    let mut captures = plan
        .captures
        .into_iter()
        .map(|capture| (capture.item_id.clone(), capture))
        .collect::<HashMap<_, _>>();
    let mut fetch_budget = FetchBudget {
        consumed_bytes: 0,
        started: Instant::now(),
    };

    for item in plan.items {
        if item.status == "skipped" {
            summary.skipped += 1;
            results.push(DanmakuRefreshResultItem {
                item_id: item.item_id,
                title: item.title,
                status: "skipped".to_owned(),
                deferred: item.deferred,
                reason: item.reason,
                added_comments: 0,
            });
            continue;
        }
        let capture = captures
            .remove(&item.item_id)
            .ok_or_else(|| anyhow!("the reviewed danmaku plan is incomplete"))?;
        if bounded_text(&capture.relative_path, 512) != item.relative_path
            || bounded_text(&capture.title, 240) != item.title
        {
            bail!("the reviewed danmaku plan does not match its public item list");
        }
        guard().map_err(|_| anyhow!(GUARD_STOPPED))?;
        let fetched = match fetch_xml(
            capture.cid,
            client.as_ref(),
            fetcher.as_ref(),
            &mut fetch_budget,
        )
        .await
        {
            Ok(fetched) => fetched,
            Err(_) => {
                summary.failed += 1;
                results.push(failed_row(&item));
                continue;
            }
        };

        guard().map_err(|_| anyhow!(GUARD_STOPPED))?;
        verify_media_and_sidecars(&root, &capture).map_err(|_| anyhow!(REVIEWED_FILES_CHANGED))?;
        let observed_xml =
            observe_sidecar(&root, &capture.xml.relative_path, MAX_DANMAKU_BODY_BYTES)
                .map_err(|_| anyhow!(REVIEWED_FILES_CHANGED))?;
        let observed_ass = observe_sidecar(&root, &capture.ass.relative_path, MAX_ASS_FILE_BYTES)
            .map_err(|_| anyhow!(REVIEWED_FILES_CHANGED))?;
        if observed_xml.capture != capture.xml || observed_ass.capture != capture.ass {
            bail!(REVIEWED_FILES_CHANGED);
        }
        let old_xml = observed_xml
            .bytes
            .as_deref()
            .map(std::str::from_utf8)
            .transpose()
            .map_err(|_| anyhow!(REVIEWED_FILES_CHANGED))?
            .unwrap_or("");
        let fetched_xml = fetched.xml;
        let prepared = (|| -> Result<(bbdown_core::DanmakuXmlMerge, String)> {
            validate_chatid(&fetched_xml, capture.cid)?;
            let merged = bbdown_core::merge_xml_preserving(old_xml, &fetched_xml)?;
            let new_ass = render_and_style_ass(&merged.xml, &config.bilibili.danmaku)?;
            if let Some(existing_ass) = observed_ass.bytes.as_deref()
                && !ass_is_generated_prefix(existing_ass, &merged.xml)?
            {
                bail!(UNSUPPORTED_ASS_REASON);
            }
            Ok((merged, new_ass))
        })();
        let (merged, new_ass) = match prepared {
            Ok(prepared) => prepared,
            Err(_) => {
                summary.failed += 1;
                results.push(failed_row(&item));
                continue;
            }
        };

        let xml_unchanged = observed_xml.bytes.as_deref() == Some(merged.xml.as_bytes());
        let ass_unchanged = observed_ass.bytes.as_deref() == Some(new_ass.as_bytes());
        if xml_unchanged && ass_unchanged {
            summary.unchanged += 1;
            results.push(DanmakuRefreshResultItem {
                item_id: item.item_id,
                title: item.title,
                status: "unchanged".to_owned(),
                deferred: false,
                reason: None,
                added_comments: 0,
            });
            continue;
        }

        match publish_pair(
            &root,
            &capture,
            merged.xml.as_bytes(),
            new_ass.as_bytes(),
            guard.as_ref(),
        ) {
            Ok(()) => {
                summary.updated += 1;
                summary.added_comments = summary
                    .added_comments
                    .saturating_add(merged.appended_comments);
                results.push(DanmakuRefreshResultItem {
                    item_id: item.item_id,
                    title: item.title,
                    status: "updated".to_owned(),
                    deferred: false,
                    reason: None,
                    added_comments: merged.appended_comments,
                });
            }
            Err(PublishFailure::Guard) => bail!(GUARD_STOPPED),
            Err(PublishFailure::Stale) => bail!(REVIEWED_FILES_CHANGED),
            Err(PublishFailure::Io) => {
                summary.failed += 1;
                results.push(failed_row(&item));
            }
        }
    }
    ensure!(
        captures.is_empty(),
        "the reviewed danmaku plan has extra private rows"
    );
    Ok(DanmakuRefreshResult {
        summary,
        items: results,
        media_files_changed: false,
    })
}

fn validate_plan_shape(plan: &DanmakuRefreshPreview) -> Result<()> {
    ensure!(
        plan.items.len() == plan.total
            && plan.items.len() <= MAX_REFRESH_ROWS
            && !plan.truncated
            && plan.ready + plan.skipped == plan.total,
        "the reviewed danmaku plan is invalid"
    );
    let ready = plan
        .items
        .iter()
        .filter(|item| item.status == "ready")
        .count();
    let skipped = plan
        .items
        .iter()
        .filter(|item| item.status == "skipped")
        .count();
    ensure!(
        ready == plan.ready && skipped == plan.skipped,
        "the reviewed danmaku plan counts are invalid"
    );
    ensure!(
        plan.items
            .iter()
            .all(|item| matches!(item.status.as_str(), "ready" | "skipped")),
        "the reviewed danmaku plan contains an unknown item status"
    );
    ensure!(
        plan.items.iter().all(|item| {
            !item.deferred
                || (item.status == "skipped"
                    && item.reason.as_deref() == Some(DEFERRED_BATCH_REASON))
        }),
        "the reviewed danmaku plan has an invalid deferred item"
    );
    ensure!(
        plan.items.iter().all(|item| {
            item.item_id.len() <= 256
                && item.title.len() <= 240
                && item.relative_path.len() <= 512
                && item
                    .reason
                    .as_ref()
                    .is_none_or(|reason| reason.len() <= 256)
        }),
        "the reviewed danmaku plan contains an oversized public row"
    );
    Ok(())
}

fn validate_result_budget(plan: &DanmakuRefreshPreview) -> Result<()> {
    // Build a worst-case-shaped receipt before the first network request or write. Failed rows
    // carry the longest fixed status/reason pair; per-item comment counts use the widest JSON
    // integer representation. Skipped rows retain their reviewed reason.
    let items = plan
        .items
        .iter()
        .map(|item| {
            if item.status == "skipped" {
                DanmakuRefreshResultItem {
                    item_id: item.item_id.clone(),
                    title: item.title.clone(),
                    status: "skipped".to_owned(),
                    deferred: item.deferred,
                    reason: item.reason.clone(),
                    added_comments: usize::MAX,
                }
            } else {
                DanmakuRefreshResultItem {
                    item_id: item.item_id.clone(),
                    title: item.title.clone(),
                    status: "failed".to_owned(),
                    deferred: false,
                    reason: Some(REFRESH_FAILED_REASON.to_owned()),
                    added_comments: usize::MAX,
                }
            }
        })
        .collect();
    let worst_case = DanmakuRefreshResult {
        summary: DanmakuRefreshSummary {
            updated: usize::MAX,
            unchanged: usize::MAX,
            skipped: usize::MAX,
            failed: usize::MAX,
            added_comments: usize::MAX,
        },
        items,
        media_files_changed: false,
    };
    ensure!(
        serde_json::to_vec(&worst_case)?.len() <= MAX_REFRESH_RESULT_BYTES,
        "the danmaku result exceeds the saved receipt limit; select fewer videos"
    );
    Ok(())
}

fn failed_row(item: &DanmakuRefreshPreviewItem) -> DanmakuRefreshResultItem {
    DanmakuRefreshResultItem {
        item_id: item.item_id.clone(),
        title: item.title.clone(),
        status: "failed".to_owned(),
        deferred: false,
        reason: Some(REFRESH_FAILED_REASON.to_owned()),
        added_comments: 0,
    }
}

struct FetchedXml {
    xml: String,
}

async fn fetch_xml(
    cid: u64,
    client: Option<&reqwest::Client>,
    fetcher: Option<&TestDanmakuFetcher>,
    budget: &mut FetchBudget,
) -> Result<FetchedXml> {
    ensure!(cid > 0, "invalid recorded danmaku CID");
    let remaining_budget = MAX_AGGREGATE_REFRESH_BYTES.saturating_sub(budget.consumed_bytes);
    ensure!(
        remaining_budget > MAX_DANMAKU_BODY_BYTES,
        "danmaku refresh exceeded its aggregate byte limit"
    );
    // Reserve room for the core decoder's full bounded output before starting another request.
    let wire_limit = MAX_DANMAKU_BODY_BYTES.min(remaining_budget - MAX_DANMAKU_BODY_BYTES);
    ensure!(
        budget.started.elapsed() < MAX_REFRESH_DURATION,
        "danmaku refresh exceeded its time limit"
    );
    let bytes = match fetcher {
        Some(fetcher) => tokio::time::timeout(MAX_REQUEST_TIMEOUT, (fetcher)(cid))
            .await
            .map_err(|_| anyhow!("danmaku source request timed out"))??,
        None => {
            let client = client.context("danmaku request client is unavailable")?;
            let url = comment_url(cid);
            let mut response = client
                .get(url)
                .header(reqwest::header::ACCEPT_ENCODING, "identity")
                .send()
                .await
                .map_err(|_| anyhow!("danmaku source request failed"))?;
            if !response.status().is_success() {
                bail!("danmaku source returned a non-success status");
            }
            if response
                .content_length()
                .is_some_and(|length| length > wire_limit as u64)
            {
                bail!("danmaku source exceeds its response size limit");
            }
            let mut body = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| anyhow!("danmaku source response could not be read"))?
            {
                if body.len().saturating_add(chunk.len()) > wire_limit {
                    bail!("danmaku source exceeds its response size limit");
                }
                body.extend_from_slice(&chunk);
            }
            body
        }
    };
    ensure!(
        bytes.len() <= wire_limit,
        "danmaku source exceeds its response size limit"
    );
    budget.consumed_bytes = budget.consumed_bytes.saturating_add(bytes.len());
    let decoded_limit = MAX_DANMAKU_BODY_BYTES
        .min(MAX_AGGREGATE_REFRESH_BYTES.saturating_sub(budget.consumed_bytes));
    ensure!(
        decoded_limit > 0,
        "danmaku refresh exceeded its aggregate byte limit"
    );
    // Reserve the bounded decompression allowance before decoding. If a malformed compressed
    // response consumes substantial output before failing validation, later rows cannot reuse
    // those already-spent bytes and trigger more source requests without bound.
    budget.consumed_bytes = budget.consumed_bytes.saturating_add(MAX_DANMAKU_BODY_BYTES);
    let xml = decode_danmaku_payload(&bytes, decoded_limit)?;
    budget.consumed_bytes = budget
        .consumed_bytes
        .saturating_sub(MAX_DANMAKU_BODY_BYTES.saturating_sub(xml.len()));
    ensure!(
        budget.consumed_bytes <= MAX_AGGREGATE_REFRESH_BYTES,
        "danmaku refresh exceeded its aggregate byte limit"
    );
    ensure!(
        budget.started.elapsed() < MAX_REFRESH_DURATION,
        "danmaku refresh exceeded its time limit"
    );
    Ok(FetchedXml { xml })
}

fn comment_url(cid: u64) -> String {
    format!("https://comment.bilibili.com/{cid}.xml")
}

fn decode_danmaku_payload(bytes: &[u8], decoded_limit: usize) -> Result<String> {
    ensure!(
        decoded_limit <= MAX_DANMAKU_BODY_BYTES,
        "invalid danmaku decoded-byte budget"
    );
    let xml = bbdown_core::decode_danmaku_xml(bytes)
        .map_err(|_| anyhow!("danmaku source is not a supported XML payload"))?;
    ensure!(
        xml.len() <= decoded_limit,
        "danmaku source exceeds its aggregate decoded-byte limit"
    );
    Ok(xml)
}

fn validate_xml(xml: &str) -> Result<()> {
    bbdown_core::merge_xml_preserving("", xml)
        .map(|_| ())
        .map_err(|_| anyhow!("danmaku XML is invalid"))
}

/// If a danmaku XML document records its source CID, require it to match the reviewed item.
/// The BBDown parser validates XML structure; this small scanner ignores comments, CDATA and
/// processing instructions so text that merely resembles a chatid tag cannot satisfy the check.
fn validate_chatid(xml: &str, expected_cid: u64) -> Result<()> {
    let bytes = xml.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let Some(relative) = bytes[cursor..].iter().position(|byte| *byte == b'<') else {
            break;
        };
        cursor += relative;
        if bytes[cursor..].starts_with(b"<!--") {
            cursor = after_delimiter(bytes, cursor + 4, b"-->")?;
            continue;
        }
        if bytes[cursor..].starts_with(b"<![CDATA[") {
            cursor = after_delimiter(bytes, cursor + 9, b"]]>")?;
            continue;
        }
        if bytes[cursor..].starts_with(b"<?") || bytes[cursor..].starts_with(b"<!") {
            cursor = after_delimiter(bytes, cursor + 2, b">")?;
            continue;
        }
        let start = cursor + 1;
        let closing = bytes.get(start) == Some(&b'/');
        let name_start = start + usize::from(closing);
        let name_end = bytes[name_start..]
            .iter()
            .position(|byte| !is_xml_name_byte(*byte))
            .map(|offset| name_start + offset)
            .unwrap_or(bytes.len());
        let name = &bytes[name_start..name_end];
        let tag_end = bytes[name_end..]
            .iter()
            .position(|byte| *byte == b'>')
            .map(|offset| name_end + offset)
            .context("danmaku XML contains an unterminated tag")?;
        if !closing && name == b"chatid" {
            let close = bytes[tag_end + 1..]
                .windows(b"</chatid>".len())
                .position(|window| window == b"</chatid>")
                .map(|offset| tag_end + 1 + offset)
                .context("danmaku XML chatid has no closing tag")?;
            let value = std::str::from_utf8(&bytes[tag_end + 1..close])?.trim();
            ensure!(
                !value.is_empty()
                    && value.bytes().all(|byte| byte.is_ascii_digit())
                    && value.parse::<u64>().ok() == Some(expected_cid),
                "danmaku XML chatid does not match its media CID"
            );
            cursor = close + b"</chatid>".len();
        } else {
            cursor = tag_end + 1;
        }
    }
    Ok(())
}

fn after_delimiter(bytes: &[u8], start: usize, delimiter: &[u8]) -> Result<usize> {
    bytes[start..]
        .windows(delimiter.len())
        .position(|window| window == delimiter)
        .map(|offset| start + offset + delimiter.len())
        .context("danmaku XML contains an unterminated declaration")
}

fn is_xml_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'-' | b'.')
}

fn render_and_style_ass(xml: &str, settings: &BilibiliDanmakuConfig) -> Result<String> {
    let rendered = bbdown_core::render_danmaku_ass(xml)
        .map_err(|_| anyhow!("danmaku XML could not be rendered as ASS"))?;
    if settings.enabled {
        Ok(crate::bilibili_danmaku::restyle_ass(&rendered, settings)
            .map_err(|_| anyhow!("generated ASS could not be styled"))?
            .unwrap_or(rendered))
    } else {
        Ok(rendered)
    }
}

fn ass_is_generated_prefix(ass: &[u8], xml: &str) -> Result<bool> {
    let ass = match std::str::from_utf8(ass) {
        Ok(ass) => ass,
        Err(_) => return Ok(false),
    };
    let rendered = bbdown_core::render_danmaku_ass(xml)
        .map_err(|_| anyhow!("existing XML cannot be rendered"))?;
    if let Some(settings) = style_settings_from_ass(ass) {
        let styled = crate::bilibili_danmaku::restyle_ass(&rendered, &settings)
            .map_err(|_| anyhow!("existing ASS styling is unsupported"))?
            .unwrap_or(rendered);
        Ok(ass_is_semantic_subset(ass, &styled))
    } else {
        Ok(ass_is_semantic_subset(ass, &rendered))
    }
}

fn ass_is_semantic_subset(existing: &str, expected: &str) -> bool {
    let existing_lines = existing.lines().collect::<Vec<_>>();
    let expected_lines = expected.lines().collect::<Vec<_>>();
    let existing_events = existing_lines
        .iter()
        .copied()
        .filter(|line| line.starts_with("Dialogue: "))
        .filter_map(semantic_event)
        .fold(HashMap::<String, usize>::new(), |mut counts, event| {
            *counts.entry(event).or_default() += 1;
            counts
        });
    let expected_events = expected_lines
        .iter()
        .copied()
        .filter(|line| line.starts_with("Dialogue: "))
        .filter_map(semantic_event)
        .fold(HashMap::<String, usize>::new(), |mut counts, event| {
            *counts.entry(event).or_default() += 1;
            counts
        });
    if !existing_lines
        .iter()
        .copied()
        .filter(|line| !line.starts_with("Dialogue: "))
        .eq(expected_lines
            .iter()
            .copied()
            .filter(|line| !line.starts_with("Dialogue: ")))
    {
        return false;
    }
    let existing_dialogue_lines = existing_lines
        .iter()
        .filter(|line| line.starts_with("Dialogue: "))
        .count();
    if existing_events.values().sum::<usize>() != existing_dialogue_lines {
        return false;
    }
    existing_events.iter().all(|(event, count)| {
        expected_events
            .get(event)
            .is_some_and(|expected| count <= expected)
    })
}

fn semantic_event(line: &str) -> Option<String> {
    let fields = line
        .strip_prefix("Dialogue: ")?
        .splitn(10, ',')
        .collect::<Vec<_>>();
    if fields.len() != 10 {
        return None;
    }
    let mut event = fields[..9].join(",");
    event.push(',');
    event.push_str(&normalize_generated_layout_tags(fields[9])?);
    Some(event)
}

fn normalize_generated_layout_tags(text: &str) -> Option<String> {
    let open = text.find('{')?;
    let close = text[open + 1..].find('}')? + open + 1;
    let tags = &text[open + 1..close];
    let mut normalized_tags = String::with_capacity(tags.len());
    let mut cursor = 0;
    while cursor < tags.len() {
        if tags.as_bytes()[cursor] != b'\\' {
            normalized_tags.push(tags[cursor..].chars().next()?);
            cursor += tags[cursor..].chars().next()?.len_utf8();
            continue;
        }
        let tag_start = cursor;
        cursor += 1;
        let name_start = cursor;
        while cursor < tags.len() && tags.as_bytes()[cursor].is_ascii_alphabetic() {
            cursor += 1;
        }
        if name_start == cursor {
            return None;
        }
        let name = &tags[name_start..cursor];
        let value_start = cursor;
        while cursor < tags.len() && tags.as_bytes()[cursor] != b'\\' {
            cursor += 1;
        }
        let value = &tags[value_start..cursor];
        if matches!(name, "an" | "pos" | "move" | "clip") {
            if !valid_generated_layout_tag(name, value) {
                return None;
            }
            normalized_tags.push('\\');
            normalized_tags.push_str(name);
            normalized_tags.push_str("(*)");
        } else {
            normalized_tags.push_str(&tags[tag_start..cursor]);
        }
    }
    Some(format!(
        "{}{{{normalized_tags}}}{}",
        &text[..open],
        &text[close + 1..]
    ))
}

fn valid_generated_layout_tag(name: &str, value: &str) -> bool {
    if name == "an" {
        return matches!(value, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9");
    }
    let Some(arguments) = value
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    else {
        return false;
    };
    let values = arguments.split(',').collect::<Vec<_>>();
    let expected_count = if name == "pos" { 2 } else { 4 };
    values.len() == expected_count
        && values
            .iter()
            .all(|value| value.parse::<f64>().is_ok_and(f64::is_finite))
}

fn style_settings_from_ass(ass: &str) -> Option<BilibiliDanmakuConfig> {
    let marker = ass
        .lines()
        .find(|line| line.starts_with(ASS_MARKER_PREFIX))?;
    let value = marker.strip_prefix(ASS_MARKER_PREFIX)?;
    let mut font_scale = None;
    let mut opacity = None;
    let mut display_area = None;
    for part in value.split(';') {
        let (key, value) = part.split_once('=')?;
        match key {
            "font_scale" => font_scale = value.parse::<f64>().ok(),
            "opacity" => opacity = value.parse::<f64>().ok(),
            "display_area" => display_area = value.parse::<f64>().ok(),
            _ => return None,
        }
    }
    let settings = BilibiliDanmakuConfig {
        enabled: true,
        font_scale: font_scale?,
        opacity: opacity?,
        display_area: display_area?,
    };
    (settings.font_scale.is_finite()
        && settings.font_scale > 0.0
        && settings.font_scale <= 10.0
        && settings.opacity.is_finite()
        && (0.0..=1.0).contains(&settings.opacity)
        && settings.display_area.is_finite()
        && settings.display_area > 0.0
        && settings.display_area <= 1.0)
        .then_some(settings)
}

fn verify_media_and_sidecars(root: &RootedFs, capture: &CapturedItem) -> Result<()> {
    let media_path = safe_root_path(root, &capture.relative_path)?;
    let media = root
        .open_bound_file(&media_path)?
        .ok_or_else(|| anyhow!(REVIEWED_FILES_CHANGED))?;
    ensure!(
        media.physical_identity()? == capture.media_identity,
        REVIEWED_FILES_CHANGED
    );
    verify_sidecar(root, &capture.xml)?;
    verify_sidecar(root, &capture.ass)?;
    verify_captured_nfo(root, capture)?;
    Ok(())
}

fn verify_captured_nfo(root: &RootedFs, capture: &CapturedItem) -> Result<()> {
    if let Some(nfo) = &capture.nfo {
        verify_sidecar(root, nfo)?;
    }
    Ok(())
}

fn observe_sidecar(root: &RootedFs, relative: &str, read_limit: usize) -> Result<ObservedSidecar> {
    observe_sidecar_with_budget(root, relative, read_limit, usize::MAX)?
        .context("sidecar unexpectedly exceeded an unlimited inspection budget")
}

fn observe_for_preview(
    root: &RootedFs,
    relative: &str,
    read_limit: usize,
    aggregate_bytes: &mut usize,
    inspection_started: Instant,
) -> std::result::Result<ObservedSidecar, CaptureError> {
    if inspection_started.elapsed() >= MAX_PREVIEW_DURATION {
        return Err(CaptureError::Defer);
    }
    let remaining = MAX_AGGREGATE_REFRESH_BYTES.saturating_sub(*aggregate_bytes);
    let observed = observe_sidecar_with_budget(root, relative, read_limit, remaining)
        .map_err(|_| CaptureError::Skip(UNSUPPORTED_SIDECAR_REASON))?
        .ok_or(CaptureError::Defer)?;
    *aggregate_bytes = aggregate_bytes.saturating_add(observed.bytes.as_ref().map_or(0, Vec::len));
    Ok(observed)
}

fn observe_sidecar_with_budget(
    root: &RootedFs,
    relative: &str,
    read_limit: usize,
    budget_bytes: usize,
) -> Result<Option<ObservedSidecar>> {
    let path = safe_root_path(root, relative)?;
    let parent_relative = Path::new(relative)
        .parent()
        .unwrap_or_else(|| Path::new(""));
    let parent = capture_parent(root, parent_relative)?;
    let entry = root.bind_entry(&path, false)?;
    let Some(identity) = root.bound_entry_identity(&entry)? else {
        return Ok(Some(ObservedSidecar {
            capture: SidecarCapture {
                relative_path: relative.to_owned(),
                parent,
                state: CapturedFileState::Missing,
            },
            bytes: None,
        }));
    };
    if !identity.is_file() {
        bail!("sidecar path is not a regular file");
    }
    let file = root.open_bound_file_if_identity(&entry, identity)?;
    let size = file.byte_len()?;
    ensure!(
        size <= read_limit as u64,
        "sidecar exceeds its individual size limit"
    );
    if size > budget_bytes as u64 {
        return Ok(None);
    }
    let bytes = file.read_limited(read_limit.min(budget_bytes))?;
    let access = access_proof(file.access_policy()?);
    ensure!(
        access.link_count == 1 && access.mode & !0o777 == 0,
        "sidecar access policy is not eligible for replacement"
    );
    let proof = FileProof {
        identity: file.physical_identity()?,
        size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        sha256: Sha256::digest(&bytes).into(),
        access,
    };
    Ok(Some(ObservedSidecar {
        capture: SidecarCapture {
            relative_path: relative.to_owned(),
            parent,
            state: CapturedFileState::Present(proof),
        },
        bytes: Some(bytes),
    }))
}

fn capture_parent(root: &RootedFs, relative: &Path) -> Result<ParentProof> {
    validate_relative_path(relative)?;
    if relative.as_os_str().is_empty() {
        return Ok(ParentProof {
            relative_path: String::new(),
            identity: root.root_physical_identity()?,
            access: None,
        });
    }
    let absolute = root.root_path().join(relative);
    let entry = root.bind_entry(&absolute, false)?;
    let identity = root
        .bound_entry_identity(&entry)?
        .ok_or_else(|| anyhow!("sidecar parent directory is missing"))?;
    ensure!(identity.is_dir(), "sidecar parent is not a directory");
    let directory = root.open_bound_directory(&entry, identity)?;
    let (owner, group, mode) = directory.access_policy()?;
    Ok(ParentProof {
        relative_path: path_to_string(relative)?,
        identity: directory.physical_identity()?,
        access: Some(DirectoryAccessProof { owner, group, mode }),
    })
}

fn verify_parent(root: &RootedFs, proof: &ParentProof) -> Result<()> {
    let current = capture_parent(root, Path::new(&proof.relative_path))?;
    ensure!(current == *proof, "sidecar parent directory changed");
    Ok(())
}

fn verify_sidecar(root: &RootedFs, capture: &SidecarCapture) -> Result<()> {
    let observed = observe_sidecar(
        root,
        &capture.relative_path,
        sidecar_limit(&capture.relative_path),
    )?;
    ensure!(
        observed.capture == *capture,
        "sidecar changed after preview"
    );
    Ok(())
}

fn sidecar_limit(relative: &str) -> usize {
    if relative.to_ascii_lowercase().ends_with(".nfo") {
        MAX_NFO_BYTES
    } else if relative.to_ascii_lowercase().ends_with(".ass") {
        MAX_ASS_FILE_BYTES
    } else {
        MAX_DANMAKU_BODY_BYTES
    }
}

fn publish_pair(
    root: &RootedFs,
    capture: &CapturedItem,
    xml_bytes: &[u8],
    ass_bytes: &[u8],
    guard: &(dyn Fn() -> Result<()> + Send + Sync),
) -> std::result::Result<(), PublishFailure> {
    verify_sidecar(root, &capture.xml).map_err(|_| PublishFailure::Stale)?;
    verify_sidecar(root, &capture.ass).map_err(|_| PublishFailure::Stale)?;
    verify_captured_nfo(root, capture).map_err(|_| PublishFailure::Stale)?;

    let xml_after = publish_sidecar(root, &capture.xml, xml_bytes, guard, || {
        verify_sidecar(root, &capture.ass).and_then(|()| verify_captured_nfo(root, capture))
    })?;

    verify_sidecar(root, &capture.ass).map_err(|_| PublishFailure::Stale)?;
    verify_captured_nfo(root, capture).map_err(|_| PublishFailure::Stale)?;
    verify_sidecar(root, &xml_after).map_err(|_| PublishFailure::Stale)?;
    publish_sidecar(root, &capture.ass, ass_bytes, guard, || {
        verify_sidecar(root, &xml_after).and_then(|()| verify_captured_nfo(root, capture))
    })?;
    Ok(())
}

fn publish_sidecar<F>(
    root: &RootedFs,
    capture: &SidecarCapture,
    output: &[u8],
    guard: &(dyn Fn() -> Result<()> + Send + Sync),
    check_other: F,
) -> std::result::Result<SidecarCapture, PublishFailure>
where
    F: Fn() -> Result<()>,
{
    verify_parent(root, &capture.parent).map_err(|_| PublishFailure::Stale)?;
    let absolute =
        safe_root_path(root, &capture.relative_path).map_err(|_| PublishFailure::Stale)?;
    let entry = root
        .bind_entry(&absolute, false)
        .map_err(|_| PublishFailure::Stale)?;
    match &capture.state {
        CapturedFileState::Present(expected) => {
            let identity = root
                .bound_entry_identity(&entry)
                .map_err(|_| PublishFailure::Stale)?
                .ok_or(PublishFailure::Stale)?;
            let file = root
                .open_bound_file_if_identity(&entry, identity)
                .map_err(|_| PublishFailure::Stale)?;
            verify_file_proof(root, &entry, &file, expected).map_err(|_| PublishFailure::Stale)?;
            let mode =
                u16::try_from(expected.access.mode & 0o777).map_err(|_| PublishFailure::Io)?;
            let temp = temporary_sibling(&capture.relative_path).map_err(|_| PublishFailure::Io)?;
            let mut checkpoint_failure = None;
            let replacement = root.replace_bound_file_atomically_if_identity_with_checkpoint(
                &entry,
                identity,
                &temp,
                output,
                mode,
                MAX_ASS_FILE_BYTES.max(MAX_DANMAKU_BODY_BYTES),
                &mut |_, _, _| {
                    if guard().is_err() {
                        checkpoint_failure = Some(PublishFailure::Guard);
                        bail!("publication guard rejected the write");
                    }
                    if verify_parent(root, &capture.parent)
                        .and_then(|()| verify_file_proof(root, &entry, &file, expected))
                        .and_then(|()| check_other())
                        .is_err()
                    {
                        checkpoint_failure = Some(PublishFailure::Stale);
                        bail!("reviewed sidecar changed before replacement");
                    }
                    Ok(())
                },
            );
            if let Some(failure) = checkpoint_failure {
                return Err(failure);
            }
            replacement.map_err(|_| PublishFailure::Io)?;
            observe_sidecar(
                root,
                &capture.relative_path,
                sidecar_limit(&capture.relative_path),
            )
            .map(|observed| observed.capture)
            .map_err(|_| PublishFailure::Io)
        }
        CapturedFileState::Missing => {
            if root
                .bound_entry_identity(&entry)
                .map_err(|_| PublishFailure::Stale)?
                .is_some()
            {
                return Err(PublishFailure::Stale);
            }
            check_other().map_err(|_| PublishFailure::Stale)?;
            if guard().is_err() {
                return Err(PublishFailure::Guard);
            }
            publish_new_sidecar(root, capture, &entry, output, guard, check_other)
        }
    }
}

fn publish_new_sidecar<F>(
    root: &RootedFs,
    capture: &SidecarCapture,
    target_entry: &BoundEntry,
    output: &[u8],
    guard: &(dyn Fn() -> Result<()> + Send + Sync),
    check_other: F,
) -> std::result::Result<SidecarCapture, PublishFailure>
where
    F: Fn() -> Result<()>,
{
    let temp_relative =
        temporary_sibling(&capture.relative_path).map_err(|_| PublishFailure::Io)?;
    let temp_relative = path_to_string(&temp_relative).map_err(|_| PublishFailure::Io)?;
    let temp_absolute = safe_root_path(root, &temp_relative).map_err(|_| PublishFailure::Io)?;
    let (temp_entry, temp_identity) = root
        .create_new_bound_file(&temp_absolute, output, 0o644)
        .map_err(|_| PublishFailure::Io)?;
    let result = (|| {
        verify_parent(root, &capture.parent).map_err(|_| PublishFailure::Stale)?;
        if root
            .bound_entry_identity(target_entry)
            .map_err(|_| PublishFailure::Stale)?
            .is_some()
        {
            return Err(PublishFailure::Stale);
        }
        check_other().map_err(|_| PublishFailure::Stale)?;
        if guard().is_err() {
            return Err(PublishFailure::Guard);
        }
        root.rename_via_bound_parents_noreplace_if_identity(
            &temp_entry,
            target_entry,
            temp_identity,
        )
        .map_err(|_| PublishFailure::Io)?;
        observe_sidecar(
            root,
            &capture.relative_path,
            sidecar_limit(&capture.relative_path),
        )
        .map(|observed| observed.capture)
        .map_err(|_| PublishFailure::Io)
    })();
    if result.is_err() {
        let _ = root.remove_bound_file_if_identity(&temp_entry, temp_identity);
    }
    result
}

fn verify_file_proof(
    root: &RootedFs,
    entry: &BoundEntry,
    file: &BoundFile,
    expected: &FileProof,
) -> Result<()> {
    ensure!(
        root.bound_entry_identity(entry)? == Some(file.identity()),
        "sidecar path identity changed"
    );
    ensure!(
        file.physical_identity()? == expected.identity,
        "sidecar identity changed"
    );
    let access = access_proof(file.access_policy()?);
    ensure!(access == expected.access, "sidecar access policy changed");
    let bytes = file.read_limited(sidecar_limit(entry.path().to_string_lossy().as_ref()))?;
    ensure!(
        u64::try_from(bytes.len()).unwrap_or(u64::MAX) == expected.size_bytes
            && Sha256::digest(&bytes).as_slice() == expected.sha256,
        "sidecar contents changed"
    );
    Ok(())
}

fn access_proof(access: BoundFileAccessPolicy) -> FileAccessProof {
    FileAccessProof {
        owner: access.owner,
        group: access.group,
        mode: access.mode,
        link_count: access.link_count,
    }
}

fn temporary_sibling(relative: &str) -> Result<PathBuf> {
    let parent = Path::new(relative)
        .parent()
        .unwrap_or_else(|| Path::new(""));
    let filename = format!(
        ".tvd-danmaku-refresh-{}-{}.tmp",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    Ok(parent.join(filename))
}

fn safe_root_path(root: &RootedFs, relative: &str) -> Result<PathBuf> {
    let relative = Path::new(relative);
    validate_relative_path(relative)?;
    Ok(root.root_path().join(relative))
}

fn validate_relative_path(path: &Path) -> Result<()> {
    ensure!(!path.is_absolute(), "path must be relative");
    for component in path.components() {
        ensure!(
            matches!(component, Component::Normal(_)),
            "path contains an unsafe component"
        );
    }
    Ok(())
}

fn path_to_string(path: &Path) -> Result<String> {
    validate_relative_path(path)?;
    path.to_str()
        .map(str::to_owned)
        .context("path is not valid UTF-8")
}

fn bounded_text(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_owned();
    }
    let mut end = limit;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value[..end].to_owned()
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn xml(comments: &[(&str, &str)]) -> String {
        let mut output = String::from("<i><chatserver>x</chatserver>");
        for (position, text) in comments {
            output.push_str(&format!(
                "<d p=\"{position},1,25,16777215,1700000000,0,user,0\">{text}</d>"
            ));
        }
        output.push_str("</i>");
        output
    }

    fn xml_with_chatid(cid: u64, text: &str) -> String {
        format!(
            "<i><chatid>{cid}</chatid><d p=\"1.0,1,25,16777215,1700000000,0,user,0\">{text}</d></i>"
        )
    }

    #[test]
    fn cid_requires_one_explicit_positive_source_id() {
        let mut item = test_item(vec!["BV1abcdefgh".to_owned(), "cid12345".to_owned()]);
        assert_eq!(source_cid(&item), Some(12_345));
        item.source_ids.push("cid67890".to_owned());
        assert_eq!(source_cid(&item), None);
        item.source_ids = vec!["../123".to_owned()];
        assert_eq!(source_cid(&item), None);
        item.source_ids = vec![" cid12345".to_owned()];
        assert_eq!(source_cid(&item), None);
        item.source_ids = vec!["cid999999999999999999999999999999".to_owned()];
        assert_eq!(source_cid(&item), None);
    }

    #[test]
    fn append_merge_is_idempotent_and_preserves_old_comments() -> Result<()> {
        let existing = xml(&[("1.0", "old"), ("2.0", "duplicate")]);
        let fetched = xml(&[("2.0", "duplicate"), ("3.0", "new")]);
        let merged = bbdown_core::merge_xml_preserving(&existing, &fetched)?;
        assert_eq!(merged.existing_comments, 2);
        assert_eq!(merged.appended_comments, 1);
        assert!(merged.xml.contains("old"));
        assert_eq!(merged.xml.matches("duplicate").count(), 1);
        assert_eq!(merged.xml.matches("new").count(), 1);
        let repeated = bbdown_core::merge_xml_preserving(&merged.xml, &fetched)?;
        assert_eq!(repeated.appended_comments, 0);
        assert_eq!(repeated.xml, merged.xml);
        Ok(())
    }

    #[test]
    fn chatid_is_optional_but_must_match_the_recorded_cid() -> Result<()> {
        validate_chatid("<i><chatid>12345</chatid></i>", 12_345)?;
        validate_chatid(
            "<i><!-- <chatid>9</chatid> --><d><![CDATA[<chatid>8</chatid>]]></d></i>",
            12_345,
        )?;
        assert!(validate_chatid("<i><chatid>54321</chatid></i>", 12_345).is_err());
        assert!(validate_chatid("<i><chatid>12x</chatid></i>", 12_345).is_err());
        Ok(())
    }

    #[test]
    fn previous_ass_is_a_safe_prefix_after_xml_first_interruption() -> Result<()> {
        let old_xml = xml(&[("1.0", "old comment")]);
        let fetched = xml(&[("1.0", "old comment"), ("2.0", "new comment")]);
        let old_ass = bbdown_core::render_danmaku_ass(&old_xml)?;
        let updated_xml = bbdown_core::merge_xml_preserving(&old_xml, &fetched)?.xml;
        assert!(ass_is_generated_prefix(old_ass.as_bytes(), &updated_xml)?);
        let updated_ass = bbdown_core::render_danmaku_ass(&updated_xml)?;
        assert!(updated_ass.contains("old comment"));
        assert!(updated_ass.contains("new comment"));
        Ok(())
    }

    #[test]
    fn old_styled_ass_matches_xml_superset_after_layout_changes() -> Result<()> {
        let old_xml = "<i><d p=\"10.0,1,25,16777215,1700000000,0,user,0\">old comment</d></i>";
        let updated_xml = "<i><d p=\"1.0,1,96,16777215,1600000000,0,user,0\">earlier large comment</d><d p=\"10.0,1,25,16777215,1700000000,0,user,0\">old comment</d></i>";
        let settings = BilibiliDanmakuConfig {
            enabled: true,
            font_scale: 1.0,
            opacity: 0.8,
            display_area: 0.75,
        };
        let old_ass = render_and_style_ass(old_xml, &settings)?;
        assert!(ass_is_generated_prefix(old_ass.as_bytes(), updated_xml)?);

        let with_custom_event = format!(
            "{old_ass}Dialogue: 0,0:00:01.00,0:00:02.00,Danmaku,,0,0,0,,{{\\fs25\\c&HFFFFFF&}}user edit\n"
        );
        assert!(!ass_is_generated_prefix(
            with_custom_event.as_bytes(),
            updated_xml
        )?);
        Ok(())
    }

    #[tokio::test]
    async fn xml_first_interruption_keeps_comments_and_fresh_preview_repairs_ass() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let media_path = directory.path().join("episode.mp4");
        let xml_path = directory.path().join("episode.xml");
        let ass_path = directory.path().join("episode.ass");
        let media_bytes = b"synthetic media bytes";
        let old_xml = "<i><chatserver>x</chatserver><d p=\"10.0,1,25,16777215,1700000000,0,user,0\">old comment</d></i>";
        let remote_xml = "<i><chatid>12345</chatid><d p=\"1.0,1,96,16777215,1600000000,0,user,0\">new large comment</d></i>";
        let style = BilibiliDanmakuConfig {
            enabled: true,
            font_scale: 1.0,
            opacity: 0.8,
            display_area: 0.75,
        };
        let old_ass = render_and_style_ass(old_xml, &style)?;
        std::fs::write(&media_path, media_bytes)?;
        std::fs::write(&xml_path, old_xml.as_bytes())?;
        std::fs::write(&ass_path, old_ass.as_bytes())?;

        let root = RootedFs::new(directory.path())?;
        let bound_media = root
            .open_bound_file(&media_path)?
            .context("test media file should be present")?;
        let mut item = test_item(vec!["cid12345".to_owned()]);
        item.physical_identity = Some(bound_media.physical_identity()?);
        item.attachments.extend([
            crate::library::LibraryAttachment {
                relative_path: "episode.xml".to_owned(),
                kind: "danmaku_or_metadata".to_owned(),
            },
            crate::library::LibraryAttachment {
                relative_path: "episode.ass".to_owned(),
                kind: "subtitle".to_owned(),
            },
        ]);
        let snapshot = LibrarySnapshot {
            revision: "revision".to_owned(),
            scanned_at: 1,
            root_label: "test".to_owned(),
            items: vec![item],
            categories: Vec::new(),
            warnings: Vec::new(),
        };
        let first_plan = preview(
            &root, &snapshot, "library", "binding", "revision", "library", None,
        )?;
        assert_eq!(first_plan.ready, 1);

        let interrupted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard_trigger = Arc::clone(&interrupted);
        let observed_xml_path = xml_path.clone();
        let observed_ass_path = ass_path.clone();
        let expected_old_ass = old_ass.clone();
        let guard: Arc<dyn Fn() -> Result<()> + Send + Sync> = Arc::new(move || {
            if !guard_trigger.load(Ordering::SeqCst)
                && std::fs::read_to_string(&observed_xml_path)
                    .is_ok_and(|xml| xml.contains("new large comment"))
                && std::fs::read(&observed_ass_path)
                    .is_ok_and(|ass| ass == expected_old_ass.as_bytes())
            {
                guard_trigger.store(true, Ordering::SeqCst);
                return Err(anyhow!("simulated stop between XML and ASS publication"));
            }
            Ok(())
        });
        let mut config = AppConfig::for_test();
        config.bilibili.danmaku = style;
        let fetcher: TestDanmakuFetcher = Arc::new(move |_cid| {
            let response = remote_xml.as_bytes().to_vec();
            Box::pin(async move { Ok(response) })
        });
        let interrupted_result = execute_with_guard(
            Arc::new(config.clone()),
            root.clone(),
            first_plan,
            Some(Arc::clone(&fetcher)),
            guard,
        )
        .await;
        assert!(interrupted_result.is_err());
        assert!(interrupted.load(Ordering::SeqCst));
        let interrupted_xml = std::fs::read_to_string(&xml_path)?;
        assert!(interrupted_xml.contains("old comment"));
        assert!(interrupted_xml.contains("new large comment"));
        assert_eq!(std::fs::read(&ass_path)?, old_ass.as_bytes());
        assert_eq!(std::fs::read(&media_path)?, media_bytes);

        let retry_plan = preview(
            &root, &snapshot, "library", "binding", "revision", "library", None,
        )?;
        assert_eq!(retry_plan.ready, 1);
        let always_ok: Arc<dyn Fn() -> Result<()> + Send + Sync> = Arc::new(|| Ok(()));
        let retry_result =
            execute_with_guard(Arc::new(config), root, retry_plan, Some(fetcher), always_ok)
                .await?;
        assert_eq!(retry_result.summary.updated, 1);
        assert_eq!(retry_result.summary.added_comments, 0);
        let repaired_xml = std::fs::read_to_string(&xml_path)?;
        let repaired_ass = std::fs::read_to_string(&ass_path)?;
        assert_eq!(repaired_xml.matches("old comment").count(), 1);
        assert_eq!(repaired_xml.matches("new large comment").count(), 1);
        assert!(repaired_ass.contains("old comment"));
        assert!(repaired_ass.contains("new large comment"));
        assert_ne!(repaired_ass, old_ass);
        assert_eq!(std::fs::read(&media_path)?, media_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn changed_authoritative_nfo_stops_before_xml_publication() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let media = directory.path().join("episode.mp4");
        let nfo = directory.path().join("episode.nfo");
        let xml_path = directory.path().join("episode.xml");
        std::fs::write(&media, b"small media")?;
        std::fs::write(
            &nfo,
            b"<movie><uniqueid type=\"bilibili-cid\">12345</uniqueid></movie>",
        )?;
        let old_xml = xml(&[("1.0", "old comment")]);
        std::fs::write(&xml_path, old_xml.as_bytes())?;

        let root = RootedFs::new(directory.path())?;
        let bound_media = root
            .open_bound_file(&media)?
            .context("test media file should be present")?;
        let mut item = test_item(vec!["cid12345".to_owned()]);
        item.physical_identity = Some(bound_media.physical_identity()?);
        item.authoritative_nfo_path = Some("episode.nfo".to_owned());
        item.attachments.push(crate::library::LibraryAttachment {
            relative_path: "episode.xml".to_owned(),
            kind: "danmaku_or_metadata".to_owned(),
        });
        let snapshot = LibrarySnapshot {
            revision: "revision".to_owned(),
            scanned_at: 1,
            root_label: "test".to_owned(),
            items: vec![item],
            categories: Vec::new(),
            warnings: Vec::new(),
        };
        let plan = preview(
            &root, &snapshot, "library", "binding", "revision", "library", None,
        )?;
        assert_eq!(plan.ready, 1);

        std::fs::write(
            &nfo,
            b"<movie><uniqueid type=\"bilibili-cid\">54321</uniqueid></movie>",
        )?;
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch_calls = Arc::clone(&calls);
        let fetcher: TestDanmakuFetcher = Arc::new(move |_cid| {
            fetch_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(xml_with_chatid(12_345, "new comment").into_bytes()) })
        });
        let guard: Arc<dyn Fn() -> Result<()> + Send + Sync> = Arc::new(|| Ok(()));
        let result = execute_with_guard(
            Arc::new(AppConfig::for_test()),
            root,
            plan,
            Some(fetcher),
            guard,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(std::fs::read(&xml_path)?, old_xml.as_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn bad_fetched_chatid_fails_only_that_video() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let root = RootedFs::new(directory.path())?;
        let mut items = Vec::new();
        for (id, stem, cid) in [("bad", "bad", 111_u64), ("good", "good", 222_u64)] {
            let media = directory.path().join(format!("{stem}.mp4"));
            std::fs::write(&media, b"small media")?;
            std::fs::write(
                directory.path().join(format!("{stem}.xml")),
                xml(&[("1.0", "existing")]),
            )?;
            let bound = root
                .open_bound_file(&media)?
                .context("test media file should be present")?;
            let mut item = test_item(vec![format!("cid{cid}")]);
            item.id = id.to_owned();
            item.title = stem.to_owned();
            item.relative_path = format!("{stem}.mp4");
            item.physical_identity = Some(bound.physical_identity()?);
            item.attachments.push(crate::library::LibraryAttachment {
                relative_path: format!("{stem}.xml"),
                kind: "danmaku_or_metadata".to_owned(),
            });
            items.push(item);
        }
        let bad_xml_path = directory.path().join("bad.xml");
        let old_bad_xml = std::fs::read(&bad_xml_path)?;
        let snapshot = LibrarySnapshot {
            revision: "revision".to_owned(),
            scanned_at: 1,
            root_label: "test".to_owned(),
            items,
            categories: Vec::new(),
            warnings: Vec::new(),
        };
        let plan = preview(
            &root, &snapshot, "library", "binding", "revision", "library", None,
        )?;
        assert_eq!(plan.ready, 2);
        let fetcher: TestDanmakuFetcher = Arc::new(|cid| {
            let response = if cid == 111 {
                xml_with_chatid(999, "wrong source").into_bytes()
            } else {
                xml_with_chatid(cid, "valid source").into_bytes()
            };
            Box::pin(async move { Ok(response) })
        });
        let guard: Arc<dyn Fn() -> Result<()> + Send + Sync> = Arc::new(|| Ok(()));
        let result = execute_with_guard(
            Arc::new(AppConfig::for_test()),
            root,
            plan,
            Some(fetcher),
            guard,
        )
        .await?;
        assert_eq!(result.summary.failed, 1);
        assert_eq!(result.summary.updated, 1);
        assert_eq!(std::fs::read(&bad_xml_path)?, old_bad_xml);
        assert!(
            std::fs::read(directory.path().join("good.xml"))?
                .windows(12)
                .any(|w| w == b"valid source")
        );
        Ok(())
    }

    #[tokio::test]
    async fn exhausted_fetch_budget_does_not_call_source() {
        let calls = Arc::new(AtomicUsize::new(0));
        let fetch_calls = Arc::clone(&calls);
        let fetcher: TestDanmakuFetcher = Arc::new(move |_cid| {
            fetch_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Vec::new()) })
        });
        let mut budget = FetchBudget {
            consumed_bytes: MAX_AGGREGATE_REFRESH_BYTES,
            started: Instant::now(),
        };
        assert!(
            fetch_xml(123, None, Some(&fetcher), &mut budget)
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn preview_sidecar_budget_defers_rows_instead_of_calling_them_oversized() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("episode.xml");
        std::fs::write(&path, b"small danmaku xml")?;
        let root = RootedFs::new(directory.path())?;
        let observed =
            observe_sidecar_with_budget(&root, "episode.xml", MAX_DANMAKU_BODY_BYTES, 1)?;
        assert!(observed.is_none());
        assert_eq!(std::fs::read(path)?, b"small danmaku xml");

        let row = deferred_row(&test_item(vec!["cid12345".to_owned()]));
        assert_eq!(row.status, "skipped");
        assert!(row.deferred);
        assert!(
            row.reason
                .as_deref()
                .is_some_and(|reason| reason.contains("another batch"))
        );
        Ok(())
    }

    fn test_item(source_ids: Vec<String>) -> LibraryItem {
        LibraryItem {
            id: "item".to_owned(),
            title: "Episode".to_owned(),
            relative_path: "episode.mp4".to_owned(),
            bytes: 1,
            width: None,
            height: None,
            codec: None,
            source_ids,
            collection: None,
            organization_status: None,
            attachments: Vec::new(),
            authoritative_nfo_path: None,
            confidence: crate::library::Confidence::High,
            metadata_status: crate::library::MetadataStatus::Complete,
            integrity_status: crate::library::IntegrityStatus::Verified,
            physical_identity: None,
            presence: Presence::Present,
        }
    }
}
