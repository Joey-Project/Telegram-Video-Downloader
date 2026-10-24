use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::library::{
    CollectionMetadata, Confidence, LibraryItem, LibrarySnapshot, MetadataPatch, MetadataStatus,
    OrganizationStatus, Presence,
};

pub const MAX_SELECTED_METADATA_RECOVERY_ITEMS: usize = 100;
pub const MAX_LIBRARY_METADATA_RECOVERY_ITEMS: usize = 100;

pub(crate) const MAX_RESOLVED_METADATA_ENTRIES: usize = 512;
const MAX_CHOICES_PER_ITEM: usize = 24;
const MAX_RESOLVER_QUERY_TIME: Duration = Duration::from_secs(12);
const MAX_ANALYSIS_TIME: Duration = Duration::from_secs(100);

/// One canonical Bilibili source identifier passed to the injected metadata resolver.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecoveryQuery {
    pub source_id: String,
}

/// The metadata-only identity and organization data returned for one remote video or episode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMetadataEntry {
    pub bvid: Option<String>,
    pub aid: Option<u64>,
    pub cid: Option<u64>,
    pub epid: Option<u64>,
    pub title: String,
    pub collection: Option<CollectionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReport {
    pub revision: String,
    pub items: Vec<RecoveryItem>,
    pub total: usize,
    pub remaining: usize,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryItem {
    pub item_id: String,
    pub title: String,
    pub relative_path: String,
    pub status: RecoveryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch: Option<MetadataPatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidates: Option<Vec<RecoveryCandidate>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCandidate {
    pub label: String,
    pub patch: MetadataPatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryStatus {
    Ready,
    NeedsConfirmation,
    Unchanged,
    Unavailable,
    Unsupported,
}

/// Analyze selected items, or one stable-ID-paginated batch of library candidates.
///
/// Passing `Some(ids)` analyzes those selected IDs (at most 100). Passing `None`
/// scans up to 100 present items that still need metadata or organization work.
/// Resolver errors are converted into bounded per-item unavailable results.
pub async fn analyze_metadata_recovery<F, Fut>(
    snapshot: &LibrarySnapshot,
    selected_item_ids: Option<&[String]>,
    after_item_id: Option<&str>,
    mut resolver: F,
) -> Result<RecoveryReport>
where
    F: FnMut(RecoveryQuery) -> Fut + Send,
    Fut: Future<Output = Result<Vec<ResolvedMetadataEntry>>> + Send,
{
    if let Some(item_ids) = selected_item_ids {
        if item_ids.len() > MAX_SELECTED_METADATA_RECOVERY_ITEMS {
            bail!(
                "selected metadata recovery is limited to {} items",
                MAX_SELECTED_METADATA_RECOVERY_ITEMS
            );
        }
        if after_item_id.is_some() {
            bail!("selected metadata recovery does not accept a library cursor");
        }
        analyze_selected_items(snapshot, item_ids, &mut resolver).await
    } else {
        analyze_library_batch(snapshot, after_item_id, &mut resolver).await
    }
}

async fn analyze_selected_items<F, Fut>(
    snapshot: &LibrarySnapshot,
    selected_item_ids: &[String],
    resolver: &mut F,
) -> Result<RecoveryReport>
where
    F: FnMut(RecoveryQuery) -> Fut + Send,
    Fut: Future<Output = Result<Vec<ResolvedMetadataEntry>>> + Send,
{
    let mut selected = selected_item_ids.iter().cloned().collect::<BTreeSet<_>>();
    let items_by_id = snapshot
        .items
        .iter()
        .map(|item| (item.id.as_str(), item))
        .collect::<HashMap<_, _>>();
    let requested_count = selected.len();
    let mut reports = Vec::with_capacity(requested_count);
    let mut resolved_by_source = HashMap::new();
    let deadline = tokio::time::Instant::now() + MAX_ANALYSIS_TIME;

    for item_id in std::mem::take(&mut selected) {
        let Some(item) = items_by_id.get(item_id.as_str()).copied() else {
            reports.push(RecoveryItem {
                item_id,
                title: String::new(),
                relative_path: String::new(),
                status: RecoveryStatus::Unsupported,
                reason: Some("Item is not in this library snapshot.".to_string()),
                patch: None,
                candidates: None,
            });
            continue;
        };
        reports.push(analyze_item(item, resolver, &mut resolved_by_source, deadline).await);
    }

    Ok(RecoveryReport {
        revision: snapshot.revision.clone(),
        items: reports,
        total: requested_count,
        remaining: 0,
        next_cursor: None,
    })
}

async fn analyze_library_batch<F, Fut>(
    snapshot: &LibrarySnapshot,
    after_item_id: Option<&str>,
    resolver: &mut F,
) -> Result<RecoveryReport>
where
    F: FnMut(RecoveryQuery) -> Fut + Send,
    Fut: Future<Output = Result<Vec<ResolvedMetadataEntry>>> + Send,
{
    let mut candidates = snapshot
        .items
        .iter()
        .filter(|item| is_library_candidate(item))
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.id.cmp(&right.id));
    let total = candidates.len();
    if let Some(cursor) = after_item_id {
        candidates.retain(|item| item.id.as_str() > cursor);
    }
    let page_len = candidates.len().min(MAX_LIBRARY_METADATA_RECOVERY_ITEMS);
    let remaining = candidates.len().saturating_sub(page_len);
    let page = candidates.into_iter().take(page_len).collect::<Vec<_>>();
    let next_cursor = if remaining > 0 {
        page.last().map(|item| item.id.clone())
    } else {
        None
    };

    let mut reports = Vec::with_capacity(page.len());
    let mut resolved_by_source = HashMap::new();
    let deadline = tokio::time::Instant::now() + MAX_ANALYSIS_TIME;
    for item in page {
        reports.push(analyze_item(item, resolver, &mut resolved_by_source, deadline).await);
    }

    Ok(RecoveryReport {
        revision: snapshot.revision.clone(),
        items: reports,
        total,
        remaining,
        next_cursor,
    })
}

fn is_library_candidate(item: &LibraryItem) -> bool {
    item.presence == Presence::Present
        && (item.metadata_status != MetadataStatus::Complete
            || item.confidence == Confidence::Low
            || item.confidence == Confidence::NeedsConfirmation
            || item.source_ids.is_empty()
            || item
                .collection
                .as_ref()
                .is_some_and(collection_has_missing_fields)
                && has_bilibili_identity_marker(item)
            || (item.collection.is_none()
                && item.organization_status != Some(OrganizationStatus::Standalone)))
}

async fn analyze_item<F, Fut>(
    item: &LibraryItem,
    resolver: &mut F,
    resolved_by_source: &mut HashMap<String, std::result::Result<Vec<ResolvedMetadataEntry>, ()>>,
    deadline: tokio::time::Instant,
) -> RecoveryItem
where
    F: FnMut(RecoveryQuery) -> Fut + Send,
    Fut: Future<Output = Result<Vec<ResolvedMetadataEntry>>> + Send,
{
    if item.presence != Presence::Present {
        return result_for(
            item,
            RecoveryStatus::Unavailable,
            "Media is missing or unavailable; metadata was not queried.",
        );
    }
    if matches!(
        item.metadata_status,
        MetadataStatus::Unreadable | MetadataStatus::ProviderUnavailable
    ) {
        return result_for(
            item,
            RecoveryStatus::Unavailable,
            "Local metadata is unreadable or unavailable.",
        );
    }

    let local_patch = trusted_local_patch(item);
    let identity = match source_identity(item) {
        Ok(identity) => identity,
        Err(reason) => {
            if !has_bilibili_identity_marker(item)
                && let Some(patch) = local_patch.as_ref().filter(|_| local_patch_can_fill(item))
            {
                return local_metadata_result(
                    item,
                    patch.clone(),
                    "Using reliable local metadata; no supported online source lookup is available.",
                );
            }
            return result_for(item, RecoveryStatus::Unsupported, reason);
        }
    };
    if let Some(patch) = local_patch
        .as_ref()
        .filter(|_| local_metadata_is_sufficient_without_lookup(item))
    {
        return local_metadata_result(
            item,
            patch.clone(),
            "Using reliable local title, source identity, and organization metadata.",
        );
    }
    let resolved = if let Some(cached) = resolved_by_source.get(&identity.query.source_id) {
        cached.as_ref().map_err(|_| ())
    } else if tokio::time::Instant::now() >= deadline {
        Err(())
    } else {
        let timeout = MAX_RESOLVER_QUERY_TIME
            .min(deadline.saturating_duration_since(tokio::time::Instant::now()));
        let outcome = tokio::time::timeout(timeout, resolver(identity.query.clone()))
            .await
            .ok()
            .and_then(std::result::Result::ok)
            .filter(|entries| entries.len() <= MAX_RESOLVED_METADATA_ENTRIES)
            .ok_or(());
        resolved_by_source.insert(identity.query.source_id.clone(), outcome);
        resolved_by_source
            .get(&identity.query.source_id)
            .expect("source resolution was just cached")
            .as_ref()
            .map_err(|_| ())
    };
    let Ok(resolved) = resolved else {
        if let Some(patch) = local_patch.as_ref().filter(|_| local_patch_can_fill(item)) {
            return local_metadata_result(
                item,
                patch.clone(),
                "Online metadata is unavailable; only reliable local metadata is offered.",
            );
        }
        return result_for(
            item,
            RecoveryStatus::Unavailable,
            "Metadata lookup is unavailable or exceeded its time/result limit.",
        );
    };

    let matches = matching_remote_entries(&identity, resolved);
    if matches.is_empty() {
        if let Some(patch) = local_patch.as_ref().filter(|_| local_patch_can_fill(item)) {
            return local_metadata_result(
                item,
                patch.clone(),
                "No exact remote identity matched; only reliable local metadata is offered.",
            );
        }
        return result_for(
            item,
            RecoveryStatus::Unsupported,
            "Remote metadata did not contain an exact source identity match.",
        );
    }

    if matches.len() == 1 {
        let patch = recovery_patch(item, matches[0]);
        if patch_has_changes(&patch, item) {
            return RecoveryItem {
                item_id: item.id.clone(),
                title: item.title.clone(),
                relative_path: item.relative_path.clone(),
                status: RecoveryStatus::Ready,
                reason: None,
                patch: Some(patch),
                candidates: None,
            };
        }
        return result_for(
            item,
            RecoveryStatus::Unchanged,
            "No missing metadata could be safely filled.",
        );
    }

    if matches.len() > MAX_CHOICES_PER_ITEM {
        return result_for(
            item,
            RecoveryStatus::Unsupported,
            "Too many remote parts to offer safely; add an exact CID or EPID and retry.",
        );
    }
    let candidates = matches
        .into_iter()
        .map(|entry| RecoveryCandidate {
            label: candidate_label(entry),
            patch: recovery_patch(item, entry),
        })
        .collect::<Vec<_>>();
    RecoveryItem {
        item_id: item.id.clone(),
        title: item.title.clone(),
        relative_path: item.relative_path.clone(),
        status: RecoveryStatus::NeedsConfirmation,
        reason: Some(
            "This BV resolves to multiple parts; choose the exact local part.".to_string(),
        ),
        patch: None,
        candidates: Some(candidates),
    }
}

#[derive(Debug, Clone)]
struct SourceIdentity {
    bvid: Option<String>,
    aid: Option<u64>,
    cid: Option<u64>,
    epid: Option<u64>,
    query: RecoveryQuery,
}

fn source_identity(item: &LibraryItem) -> std::result::Result<SourceIdentity, &'static str> {
    let mut bvids = BTreeSet::new();
    let mut aids = BTreeSet::new();
    let mut cids = BTreeSet::new();
    let mut epids = BTreeSet::new();
    let mut has_youtube = false;
    for raw in &item.source_ids {
        let value = raw.trim();
        if value.starts_with("BV")
            && value.len() == 12
            && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            bvids.insert(value.to_string());
        } else if let Some(digits) = value.strip_prefix("av") {
            if let Ok(id) = parse_positive_id(digits) {
                aids.insert(id);
            }
        } else if let Some(digits) = value.strip_prefix("cid") {
            if let Ok(id) = parse_positive_id(digits) {
                cids.insert(id);
            }
        } else if let Some(digits) = value.strip_prefix("ep") {
            if let Ok(id) = parse_positive_id(digits) {
                epids.insert(id);
            }
        } else if value.starts_with("youtube:") {
            has_youtube = true;
        }
    }
    if bvids.len() > 1 || aids.len() > 1 || cids.len() > 1 || epids.len() > 1 {
        return Err("Multiple Bilibili identities are associated with this media item.");
    }
    let bvid = bvids.into_iter().next();
    let aid = aids.into_iter().next();
    let cid = cids.into_iter().next();
    let epid = epids.into_iter().next();
    let source_id = if let Some(bvid) = &bvid {
        bvid.clone()
    } else if let Some(epid) = epid {
        format!("ep{epid}")
    } else if let Some(aid) = aid {
        format!("av{aid}")
    } else if cid.is_some() {
        return Err("A CID without a BV or EPID cannot be resolved safely.");
    } else if has_youtube {
        return Err("Metadata recovery currently supports Bilibili source IDs only.");
    } else {
        return Err("No supported Bilibili source ID is available for lookup.");
    };
    Ok(SourceIdentity {
        bvid,
        aid,
        cid,
        epid,
        query: RecoveryQuery { source_id },
    })
}

fn has_bilibili_identity_marker(item: &LibraryItem) -> bool {
    item.source_ids.iter().any(|raw| {
        let value = raw.trim();
        value.starts_with("BV")
            || value
                .strip_prefix("av")
                .is_some_and(|suffix| parse_positive_id(suffix).is_ok())
            || value
                .strip_prefix("cid")
                .is_some_and(|suffix| parse_positive_id(suffix).is_ok())
            || value
                .strip_prefix("ep")
                .is_some_and(|suffix| parse_positive_id(suffix).is_ok())
    })
}

fn parse_positive_id(value: &str) -> std::result::Result<u64, ()> {
    value.parse::<u64>().ok().filter(|id| *id > 0).ok_or(())
}

fn matching_remote_entries<'a>(
    identity: &SourceIdentity,
    entries: &'a [ResolvedMetadataEntry],
) -> Vec<&'a ResolvedMetadataEntry> {
    entries
        .iter()
        .filter(|entry| {
            if let Some(epid) = identity.epid {
                return entry.epid == Some(epid)
                    && !optional_identity_conflicts(
                        identity.bvid.as_deref(),
                        entry.bvid.as_deref(),
                    )
                    && !optional_identity_conflicts(identity.aid, entry.aid)
                    && !optional_identity_conflicts(identity.cid, entry.cid);
            }
            if let (Some(bvid), Some(cid)) = (&identity.bvid, identity.cid) {
                return entry.bvid.as_deref() == Some(bvid.as_str())
                    && entry.cid == Some(cid)
                    && !optional_identity_conflicts(identity.aid, entry.aid);
            }
            if let Some(bvid) = &identity.bvid {
                return entry.bvid.as_deref() == Some(bvid.as_str())
                    && !optional_identity_conflicts(identity.aid, entry.aid);
            }
            identity.aid.is_some_and(|aid| {
                entry.aid == Some(aid) && identity.cid.is_none_or(|cid| entry.cid == Some(cid))
            })
        })
        .collect()
}

fn optional_identity_conflicts<T: PartialEq>(local: Option<T>, remote: Option<T>) -> bool {
    matches!((local, remote), (Some(local), Some(remote)) if local != remote)
}

fn recovery_patch(item: &LibraryItem, entry: &ResolvedMetadataEntry) -> MetadataPatch {
    let mut source_ids = item.source_ids.clone();
    if let Some(bvid) = entry
        .bvid
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        source_ids.push(bvid.to_string());
    }
    if let Some(cid) = entry.cid.filter(|value| *value > 0) {
        source_ids.push(format!("cid{cid}"));
    }
    if let Some(epid) = entry.epid.filter(|value| *value > 0) {
        source_ids.push(format!("ep{epid}"));
    }
    if let Some(aid) = entry.aid.filter(|value| *value > 0) {
        source_ids.push(format!("av{aid}"));
    }
    source_ids.sort();
    source_ids.dedup();

    let collection = if item.organization_status == Some(OrganizationStatus::Standalone) {
        None
    } else {
        merge_recovery_collection(item.collection.as_ref(), entry.collection.as_ref())
    };
    MetadataPatch {
        item_id: item.id.clone(),
        // Keep the locally selected title. The NFO writer fills an empty title only.
        title: trusted_local_title(item)
            .map(str::to_string)
            .or_else(|| nonempty_remote_title(entry)),
        source_ids,
        collection,
        fill_missing_only: true,
        organization_status: None,
    }
}

fn merge_recovery_collection(
    local: Option<&CollectionMetadata>,
    remote: Option<&CollectionMetadata>,
) -> Option<CollectionMetadata> {
    let Some(local) = local else {
        return remote.cloned();
    };
    let Some(remote) = remote else {
        return Some(local.clone());
    };
    if !same_collection_identity(local, remote) {
        return Some(local.clone());
    }

    let mut merged = local.clone();
    fill_missing_optional_string(&mut merged.id, remote.id.as_deref());
    fill_missing_string(&mut merged.title, Some(remote.title.as_str()));
    if merged.order.is_none() {
        merged.order = remote.order;
    }
    fill_missing_optional_string(&mut merged.part_id, remote.part_id.as_deref());
    if (merged.kind.trim().is_empty() || merged.kind.eq_ignore_ascii_case("nfo_set"))
        && !remote.kind.trim().is_empty()
    {
        merged.kind = remote.kind.trim().to_string();
    }
    Some(merged)
}

fn same_collection_identity(local: &CollectionMetadata, remote: &CollectionMetadata) -> bool {
    let local_id = local.id.as_deref().filter(|value| !value.trim().is_empty());
    let remote_id = remote
        .id
        .as_deref()
        .filter(|value| !value.trim().is_empty());
    if let (Some(local_id), Some(remote_id)) = (local_id, remote_id) {
        return local_id.trim() == remote_id.trim();
    }

    let local_title = canonical_collection_title(&local.title);
    let remote_title = canonical_collection_title(&remote.title);
    if local_title.is_empty() || local_title != remote_title {
        return false;
    }
    collection_kinds_compatible(&local.kind, &remote.kind)
}

fn canonical_collection_title(title: &str) -> String {
    title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn collection_kinds_compatible(left: &str, right: &str) -> bool {
    fn specific_kind(kind: &str) -> Option<&str> {
        let kind = kind.trim();
        (!kind.is_empty() && !kind.eq_ignore_ascii_case("nfo_set")).then_some(kind)
    }
    match (specific_kind(left), specific_kind(right)) {
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
        _ => true,
    }
}

fn fill_missing_string(target: &mut String, remote: Option<&str>) {
    if target.trim().is_empty()
        && let Some(remote) = remote.map(str::trim).filter(|value| !value.is_empty())
    {
        *target = remote.to_string();
    }
}

fn fill_missing_optional_string(target: &mut Option<String>, remote: Option<&str>) {
    if target
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
    {
        *target = remote
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
    }
}

fn nonempty_remote_title(entry: &ResolvedMetadataEntry) -> Option<String> {
    let title = entry.title.trim();
    (!title.is_empty()).then(|| title.to_string())
}

fn trusted_local_title(item: &LibraryItem) -> Option<&str> {
    if matches!(
        item.confidence,
        Confidence::Low | Confidence::NeedsConfirmation
    ) {
        return None;
    }
    let title = item.title.trim();
    if title.is_empty() || title == filename_title(&item.relative_path) {
        return None;
    }
    Some(title)
}

fn filename_title(path: &str) -> String {
    Path::new(path)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("Untitled")
        .replace(['_', '\u{00a0}'], " ")
        .trim()
        .to_string()
}

fn trusted_local_patch(item: &LibraryItem) -> Option<MetadataPatch> {
    if item.confidence == Confidence::NeedsConfirmation {
        return None;
    }
    let title = trusted_local_title(item).map(str::to_string);
    let source_ids = if item.confidence == Confidence::Low {
        Vec::new()
    } else {
        item.source_ids.clone()
    };
    let collection = if item.confidence == Confidence::Low
        || item.organization_status == Some(OrganizationStatus::Standalone)
    {
        None
    } else {
        item.collection.clone()
    };
    if title.is_none() && source_ids.is_empty() && collection.is_none() {
        return None;
    }
    Some(MetadataPatch {
        item_id: item.id.clone(),
        title,
        source_ids,
        collection,
        fill_missing_only: true,
        organization_status: None,
    })
}

fn local_metadata_is_sufficient_without_lookup(item: &LibraryItem) -> bool {
    let has_trusted_title = trusted_local_title(item).is_some();
    let local_collection_complete = item
        .collection
        .as_ref()
        .is_some_and(|collection| !collection_has_missing_fields(collection));
    item.metadata_status != MetadataStatus::Complete
        && has_trusted_title
        && (item.authoritative_nfo_path.is_none() && item.collection.is_some()
            || item.authoritative_nfo_path.is_some() && local_collection_complete
            || item.organization_status == Some(OrganizationStatus::Standalone))
}

fn local_patch_can_fill(item: &LibraryItem) -> bool {
    item.metadata_status != MetadataStatus::Complete
        && (item.authoritative_nfo_path.is_none() || trusted_local_title(item).is_some())
}

fn local_metadata_result(item: &LibraryItem, patch: MetadataPatch, reason: &str) -> RecoveryItem {
    RecoveryItem {
        item_id: item.id.clone(),
        title: item.title.clone(),
        relative_path: item.relative_path.clone(),
        status: RecoveryStatus::Ready,
        reason: Some(reason.to_string()),
        patch: Some(patch),
        candidates: None,
    }
}

fn patch_has_changes(patch: &MetadataPatch, item: &LibraryItem) -> bool {
    item.metadata_status != MetadataStatus::Complete
        || patch
            .source_ids
            .iter()
            .any(|id| !item.source_ids.iter().any(|current| current == id))
        || (item.collection.is_none()
            && item.organization_status != Some(OrganizationStatus::Standalone)
            && patch.collection.is_some())
        || item.collection.as_ref().is_some_and(|current| {
            patch
                .collection
                .as_ref()
                .is_some_and(|proposed| collection_fills_missing_fields(current, proposed))
        })
}

fn collection_has_missing_fields(collection: &CollectionMetadata) -> bool {
    collection
        .id
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
        || collection.title.trim().is_empty()
        || collection.kind.trim().is_empty()
        || collection.kind.eq_ignore_ascii_case("nfo_set")
        || collection.order.is_none()
        || collection
            .part_id
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
}

fn collection_fills_missing_fields(
    current: &CollectionMetadata,
    proposed: &CollectionMetadata,
) -> bool {
    let current_kind_missing =
        current.kind.trim().is_empty() || current.kind.eq_ignore_ascii_case("nfo_set");
    current
        .id
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
        && proposed
            .id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        || current.title.trim().is_empty() && !proposed.title.trim().is_empty()
        || current_kind_missing && !proposed.kind.trim().is_empty()
        || current.order.is_none() && proposed.order.is_some()
        || current
            .part_id
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
            && proposed
                .part_id
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
}

fn candidate_label(entry: &ResolvedMetadataEntry) -> String {
    let title = if entry.title.trim().is_empty() {
        "Untitled Bilibili part"
    } else {
        entry.title.trim()
    };
    match (
        entry.collection.as_ref().and_then(|c| c.order),
        entry.cid,
        entry.epid,
    ) {
        (Some(order), _, _) => format!("Part {order}: {title}"),
        (None, Some(cid), _) => format!("{title} (CID {cid})"),
        (None, None, Some(epid)) => format!("{title} (EPID {epid})"),
        _ => title.to_string(),
    }
}

fn result_for(item: &LibraryItem, status: RecoveryStatus, reason: &str) -> RecoveryItem {
    RecoveryItem {
        item_id: item.id.clone(),
        title: item.title.clone(),
        relative_path: item.relative_path.clone(),
        status,
        reason: Some(reason.to_string()),
        patch: None,
        candidates: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::library::{IntegrityStatus, LibraryAttachment};

    fn item(id: &str, source_ids: &[&str]) -> LibraryItem {
        LibraryItem {
            id: id.to_string(),
            title: format!("Local title {id}"),
            relative_path: format!("{id}.mp4"),
            bytes: 12,
            width: None,
            height: None,
            codec: None,
            source_ids: source_ids
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            collection: None,
            organization_status: None,
            attachments: Vec::<LibraryAttachment>::new(),
            authoritative_nfo_path: None,
            confidence: Confidence::Medium,
            metadata_status: MetadataStatus::Partial,
            integrity_status: IntegrityStatus::Unverified,
            physical_identity: None,
            presence: Presence::Present,
        }
    }

    fn snapshot(items: Vec<LibraryItem>) -> LibrarySnapshot {
        LibrarySnapshot {
            revision: "revision-1".to_string(),
            scanned_at: 1,
            root_label: "Videos".to_string(),
            items,
            categories: Vec::new(),
            warnings: Vec::new(),
        }
    }

    fn collection(id: &str, title: &str, order: Option<u32>) -> CollectionMetadata {
        CollectionMetadata {
            id: Some(id.to_string()),
            title: title.to_string(),
            kind: "bilibili_collection".to_string(),
            order,
            part_id: None,
        }
    }

    fn entry(bvid: &str, cid: u64, title: &str, order: u32) -> ResolvedMetadataEntry {
        ResolvedMetadataEntry {
            bvid: Some(bvid.to_string()),
            aid: Some(123),
            cid: Some(cid),
            epid: None,
            title: title.to_string(),
            collection: Some(collection(bvid, "Remote series", Some(order))),
        }
    }

    #[tokio::test]
    async fn matches_multipart_by_exact_bvid_and_cid_and_deduplicates_by_bv() {
        let bvid = "BV1abcdefgh1";
        let first = item("first", &[bvid, "cid10"]);
        let second = item("second", &[bvid, "cid20"]);
        let input = snapshot(vec![first, second]);
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver_calls = Arc::clone(&calls);
        let entries = vec![
            entry(bvid, 10, "First part", 1),
            entry(bvid, 20, "Second part", 2),
        ];
        let ids = vec!["first".to_string(), "second".to_string()];

        let report = analyze_metadata_recovery(&input, Some(&ids), None, move |_| {
            resolver_calls.fetch_add(1, Ordering::SeqCst);
            let entries = entries.clone();
            async move { Ok(entries) }
        })
        .await
        .expect("local analyzer should complete");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(report.items.len(), 2);
        assert_eq!(report.items[0].status, RecoveryStatus::Ready);
        assert_eq!(report.items[1].status, RecoveryStatus::Ready);
        assert_eq!(
            report.items[0].patch.as_ref().unwrap().source_ids,
            vec!["BV1abcdefgh1", "av123", "cid10"]
        );
        assert_eq!(
            report.items[1].patch.as_ref().unwrap().source_ids,
            vec!["BV1abcdefgh1", "av123", "cid20"]
        );
    }

    #[tokio::test]
    async fn bvid_only_multiple_parts_require_explicit_choice_and_keep_local_title() {
        let bvid = "BV1abcdefgh1";
        let input = snapshot(vec![item("only", &[bvid])]);
        let ids = vec!["only".to_string()];
        let entries = vec![
            entry(bvid, 10, "Remote one", 1),
            entry(bvid, 20, "Remote two", 2),
        ];

        let report = analyze_metadata_recovery(&input, Some(&ids), None, move |_| {
            let entries = entries.clone();
            async move { Ok(entries) }
        })
        .await
        .expect("ambiguous metadata should be reported");

        let result = &report.items[0];
        assert_eq!(result.status, RecoveryStatus::NeedsConfirmation);
        assert!(result.patch.is_none());
        let choices = result.candidates.as_ref().unwrap();
        assert_eq!(choices.len(), 2);
        assert!(choices[0].label.contains("Part 1"));
        assert_eq!(choices[0].patch.title.as_deref(), Some("Local title only"));
        assert!(choices.iter().all(|choice| choice.patch.fill_missing_only));
        assert!(
            choices
                .iter()
                .all(|choice| choice.patch.organization_status.is_none())
        );
    }

    #[tokio::test]
    async fn exact_epid_match_preserves_a_custom_local_collection() {
        let mut local = item("episode", &["ep7", "BV1abcdefgh1"]);
        let custom = collection("personal-id", "My edited collection", None);
        local.collection = Some(custom.clone());
        // The NFO has IDs and a custom set but no title; the filename fallback
        // must not be promoted into the NFO or suppress metadata lookup.
        local.title = "episode".to_string();
        local.authoritative_nfo_path = Some("episode.nfo".to_string());
        let input = snapshot(vec![local]);
        let ids = vec!["episode".to_string()];
        let mut wrong = entry("BV1abcdefgh1", 10, "Wrong episode", 1);
        wrong.epid = Some(8);
        let mut exact = entry("BV1abcdefgh1", 20, "Remote episode", 2);
        exact.epid = Some(7);
        let entries = vec![wrong, exact];

        let report = analyze_metadata_recovery(&input, Some(&ids), None, move |_| {
            let entries = entries.clone();
            async move { Ok(entries) }
        })
        .await
        .expect("exact EPID should resolve");

        let patch = report.items[0].patch.as_ref().unwrap();
        assert_eq!(report.items[0].status, RecoveryStatus::Ready);
        assert_eq!(patch.title.as_deref(), Some("Remote episode"));
        assert_eq!(patch.collection.as_ref(), Some(&custom));
        assert_eq!(patch.collection.as_ref().unwrap().order, None);
        assert!(patch.source_ids.contains(&"ep7".to_string()));
        assert!(!patch.source_ids.contains(&"ep8".to_string()));
    }

    #[tokio::test]
    async fn absent_nfo_uses_trusted_local_hints_without_online_lookup() {
        let bvid = "BV1abcdefgh1";
        let mut local = item("queued", &[bvid, "cid10"]);
        local.title = "A verified queue title".to_string();
        local.collection = Some(collection("queue-set", "Queue collection", Some(3)));
        let input = snapshot(vec![local]);
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver_calls = Arc::clone(&calls);

        let report = analyze_metadata_recovery(&input, None, None, move |_| {
            resolver_calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(Vec::new()) }
        })
        .await
        .expect("trusted local metadata should be sufficient");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(report.items[0].status, RecoveryStatus::Ready);
        let patch = report.items[0].patch.as_ref().unwrap();
        assert_eq!(patch.title.as_deref(), Some("A verified queue title"));
        assert_eq!(
            patch.collection.as_ref().unwrap().id.as_deref(),
            Some("queue-set")
        );
        assert_eq!(patch.source_ids, vec!["BV1abcdefgh1", "cid10"]);
        assert!(patch.fill_missing_only);
    }

    #[tokio::test]
    async fn missing_nfo_title_uses_remote_title_and_preserves_custom_set_for_exact_cid() {
        let bvid = "BV1abcdefgh1";
        let mut local = item("legacy", &[bvid, "cid10"]);
        let custom = collection("personal-id", "My edited collection", Some(99));
        local.title = "legacy".to_string();
        local.collection = Some(custom.clone());
        local.authoritative_nfo_path = Some("legacy.nfo".to_string());
        let input = snapshot(vec![local]);
        let ids = vec!["legacy".to_string()];
        let entries = vec![
            entry(bvid, 10, "Remote title", 1),
            entry(bvid, 20, "Another part", 2),
        ];

        let report = analyze_metadata_recovery(&input, Some(&ids), None, move |_| {
            let entries = entries.clone();
            async move { Ok(entries) }
        })
        .await
        .expect("an exact CID should select its part");

        let patch = report.items[0].patch.as_ref().unwrap();
        assert_eq!(report.items[0].status, RecoveryStatus::Ready);
        assert_eq!(patch.title.as_deref(), Some("Remote title"));
        assert_eq!(patch.collection.as_ref(), Some(&custom));
        assert!(patch.source_ids.contains(&"cid10".to_string()));
        assert!(!patch.source_ids.contains(&"cid20".to_string()));
    }

    #[tokio::test]
    async fn av_and_cid_match_the_exact_remote_part_and_fill_only_missing_group_fields() {
        let mut local = item("legacy", &["av123", "cid20"]);
        let custom = CollectionMetadata {
            id: Some("shared-set".to_string()),
            title: "My edited collection".to_string(),
            kind: "user-kind".to_string(),
            order: Some(99),
            part_id: None,
        };
        local.title = "legacy".to_string();
        local.collection = Some(custom.clone());
        local.authoritative_nfo_path = Some("legacy.nfo".to_string());
        let mut wrong = entry("BV1abcdefgh1", 10, "Wrong part", 1);
        wrong.collection = Some(CollectionMetadata {
            id: Some("shared-set".to_string()),
            title: "Remote collection".to_string(),
            kind: "bilibili_collection".to_string(),
            order: Some(1),
            part_id: Some("10".to_string()),
        });
        let mut exact = entry("BV1abcdefgh1", 20, "Exact part", 2);
        exact.collection = Some(CollectionMetadata {
            id: Some("shared-set".to_string()),
            title: "Remote collection".to_string(),
            kind: "bilibili_collection".to_string(),
            order: Some(2),
            part_id: Some("20".to_string()),
        });
        let input = snapshot(vec![local]);
        let ids = vec!["legacy".to_string()];

        let report = analyze_metadata_recovery(&input, Some(&ids), None, move |query| {
            assert_eq!(query.source_id, "av123");
            let entries = vec![wrong.clone(), exact.clone()];
            async move { Ok(entries) }
        })
        .await
        .expect("exact AID and CID should resolve");

        let result = &report.items[0];
        let patch = result.patch.as_ref().unwrap();
        assert_eq!(result.status, RecoveryStatus::Ready);
        assert!(patch.source_ids.contains(&"cid20".to_string()));
        assert!(!patch.source_ids.contains(&"cid10".to_string()));
        let merged = patch.collection.as_ref().unwrap();
        assert_eq!(merged.id.as_deref(), Some("shared-set"));
        assert_eq!(merged.title, custom.title);
        assert_eq!(merged.kind, custom.kind);
        assert_eq!(merged.order, Some(99));
        assert_eq!(merged.part_id.as_deref(), Some("20"));
    }

    #[tokio::test]
    async fn av_and_cid_do_not_fall_back_to_a_different_aid_part() {
        let mut local = item("legacy", &["av123", "cid20"]);
        local.title = "legacy".to_string();
        local.authoritative_nfo_path = Some("legacy.nfo".to_string());
        let mut wrong = entry("BV1abcdefgh1", 10, "Wrong part", 1);
        wrong.collection = Some(CollectionMetadata {
            id: Some("remote-set".to_string()),
            title: "Remote collection".to_string(),
            kind: "bilibili_collection".to_string(),
            order: Some(1),
            part_id: Some("10".to_string()),
        });
        let input = snapshot(vec![local]);
        let ids = vec!["legacy".to_string()];

        let report = analyze_metadata_recovery(&input, Some(&ids), None, move |_| {
            let wrong = wrong.clone();
            async move { Ok(vec![wrong]) }
        })
        .await
        .expect("CID mismatches should produce a safe unavailable choice");

        assert_eq!(report.items[0].status, RecoveryStatus::Unsupported);
        assert!(
            report.items[0]
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("exact source identity"))
        );
        assert!(report.items[0].patch.is_none());
    }

    #[test]
    fn collection_without_id_merges_only_when_canonical_title_and_kind_match() {
        let local = CollectionMetadata {
            id: None,
            title: "My   collection".to_string(),
            kind: "nfo_set".to_string(),
            order: None,
            part_id: None,
        };
        let remote = CollectionMetadata {
            id: Some("remote-id".to_string()),
            title: "my collection".to_string(),
            kind: "bilibili_collection".to_string(),
            order: Some(4),
            part_id: Some("cid20".to_string()),
        };

        let merged = merge_recovery_collection(Some(&local), Some(&remote)).unwrap();
        assert_eq!(merged.id.as_deref(), Some("remote-id"));
        assert_eq!(merged.title, local.title);
        assert_eq!(merged.kind, "bilibili_collection");
        assert_eq!(merged.order, Some(4));
        assert_eq!(merged.part_id.as_deref(), Some("cid20"));

        let same_id_without_order = CollectionMetadata {
            id: Some("shared-set".to_string()),
            title: "Edited set name".to_string(),
            kind: "custom-kind".to_string(),
            order: None,
            part_id: None,
        };
        let matching_remote = CollectionMetadata {
            id: Some("shared-set".to_string()),
            title: "Remote title".to_string(),
            kind: "bilibili_collection".to_string(),
            order: Some(7),
            part_id: Some("cid7".to_string()),
        };
        let completed =
            merge_recovery_collection(Some(&same_id_without_order), Some(&matching_remote))
                .unwrap();
        assert_eq!(completed.title, same_id_without_order.title);
        assert_eq!(completed.kind, same_id_without_order.kind);
        assert_eq!(completed.order, Some(7));
        assert_eq!(completed.part_id.as_deref(), Some("cid7"));

        let different = CollectionMetadata {
            id: Some("different-id".to_string()),
            title: "An unrelated collection".to_string(),
            kind: "bilibili_collection".to_string(),
            order: Some(9),
            part_id: Some("cid99".to_string()),
        };
        assert_eq!(
            merge_recovery_collection(Some(&local), Some(&different)),
            Some(local)
        );
    }

    #[tokio::test]
    async fn non_bilibili_local_metadata_does_not_require_an_online_provider() {
        let mut local = item("youtube-item", &["youtube:abc123"]);
        local.title = "A trusted local title".to_string();
        let input = snapshot(vec![local]);
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver_calls = Arc::clone(&calls);

        let report = analyze_metadata_recovery(&input, None, None, move |_| {
            resolver_calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(Vec::new()) }
        })
        .await
        .expect("trusted non-Bilibili metadata should remain usable");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(report.items[0].status, RecoveryStatus::Ready);
        let patch = report.items[0].patch.as_ref().unwrap();
        assert_eq!(patch.title.as_deref(), Some("A trusted local title"));
        assert_eq!(patch.source_ids, vec!["youtube:abc123"]);
    }

    #[tokio::test]
    async fn standalone_keeps_partial_metadata_recoverable_but_suppresses_collection() {
        let bvid = "BV1abcdefgh1";
        let mut local = item("standalone", &[bvid]);
        local.organization_status = Some(OrganizationStatus::Standalone);
        let input = snapshot(vec![local]);
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver_calls = Arc::clone(&calls);
        let report = analyze_metadata_recovery(&input, None, None, move |_| {
            resolver_calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(Vec::new()) }
        })
        .await
        .expect("partial standalone metadata should remain eligible");

        assert_eq!(report.items.len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(report.items[0].status, RecoveryStatus::Ready);
        let patch = report.items[0].patch.as_ref().unwrap();
        assert!(patch.collection.is_none());
        assert!(patch.organization_status.is_none());
        assert_eq!(patch.source_ids, vec![bvid]);
    }

    #[tokio::test]
    async fn resolver_failure_is_deduplicated_and_reported_without_raw_error_text() {
        let bvid = "BV1abcdefgh1";
        let mut first = item("first", &[bvid]);
        let mut second = item("second", &[bvid]);
        first.confidence = Confidence::Low;
        second.confidence = Confidence::Low;
        let input = snapshot(vec![first, second]);
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver_calls = Arc::clone(&calls);
        let ids = vec!["first".to_string(), "second".to_string()];

        let report = analyze_metadata_recovery(&input, Some(&ids), None, move |_| {
            resolver_calls.fetch_add(1, Ordering::SeqCst);
            async { anyhow::bail!("SESSDATA=private-cookie") }
        })
        .await
        .expect("provider failures should stay local to their items");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            report
                .items
                .iter()
                .all(|item| item.status == RecoveryStatus::Unavailable)
        );
        assert!(report.items.iter().all(|item| {
            item.reason
                .as_deref()
                .is_some_and(|reason| !reason.contains("private-cookie") && reason.len() < 160)
        }));
    }

    #[tokio::test]
    async fn library_analysis_reports_stable_batches_and_reachable_cursors() {
        let items = (0..105)
            .map(|index| item(&format!("item-{index:03}"), &[]))
            .collect();
        let input = snapshot(items);

        let first = analyze_metadata_recovery(&input, None, None, |_| async { Ok(Vec::new()) })
            .await
            .expect("first library batch should complete");
        assert_eq!(first.total, 105);
        assert_eq!(first.items.len(), 100);
        assert_eq!(first.remaining, 5);
        assert_eq!(first.next_cursor.as_deref(), Some("item-099"));

        let second =
            analyze_metadata_recovery(&input, None, first.next_cursor.as_deref(), |_| async {
                Ok(Vec::new())
            })
            .await
            .expect("second library batch should complete");
        assert_eq!(second.total, 105);
        assert_eq!(second.items.len(), 5);
        assert_eq!(second.remaining, 0);
        assert!(second.next_cursor.is_none());
        assert_eq!(second.items[0].item_id, "item-100");
    }
}
