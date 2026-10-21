use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex;
use tokio::time::{Instant, timeout};
use tracing::debug;
use url::Url;

use crate::config::AppConfig;
use crate::downloader::{BilibiliPgcSeasonIdentity, resolve_bilibili_pgc_season_identity};
use crate::queue::{
    BilibiliGroupEntryIdentity, PlanValidationSnapshot, QueueManager, TaskRecord, TaskStatus,
};
use crate::router::JobRequest;

const MAX_INDEX_RECORD_READS: usize = 64;
const MAX_RESOLUTION_CALLS: usize = 8;
const MAX_RESOLUTION_WALL_TIME: Duration = Duration::from_secs(35);
const MAX_BACKFILL_STATES: usize = 128;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BackfillKey {
    queue_identity: usize,
    library_id: String,
    root_binding: String,
}

struct BackfillState {
    gate: Arc<Mutex<()>>,
    cursor: StdMutex<Option<String>>,
}

static BACKFILL_STATES: OnceLock<StdMutex<HashMap<BackfillKey, Arc<BackfillState>>>> =
    OnceLock::new();

fn state_for(key: BackfillKey) -> Arc<BackfillState> {
    let states = BACKFILL_STATES.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut states = states
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(state) = states.get(&key) {
        return Arc::clone(state);
    }

    if states.len() >= MAX_BACKFILL_STATES {
        let idle_keys = states
            .iter()
            .filter_map(|(key, state)| {
                (Arc::strong_count(state) == 1 && state.gate.try_lock().is_ok())
                    .then_some(key.clone())
            })
            .take(states.len().saturating_sub(MAX_BACKFILL_STATES - 1))
            .collect::<Vec<_>>();
        for idle_key in idle_keys {
            states.remove(&idle_key);
        }
    }

    let state = Arc::new(BackfillState {
        gate: Arc::new(Mutex::new(())),
        cursor: StdMutex::new(None),
    });
    states.insert(key, Arc::clone(&state));
    state
}

/// Backfills metadata on stable published Bilibili PGC records before a library scan.
/// Queue File Provider operations are isolated on blocking workers and network work never
/// runs while a queue, library I/O, or publication gate is held.
pub(crate) async fn backfill_legacy_pgc_metadata(
    runtime: &super::CloudBotRuntime,
    library_id: &str,
    root_binding: &str,
) {
    if runtime.epoch_fatal.is_signaled() || root_binding.is_empty() {
        return;
    }

    let queue = Arc::clone(&runtime.bot.queue);
    let key = BackfillKey {
        queue_identity: Arc::as_ptr(&queue) as usize,
        library_id: library_id.to_string(),
        root_binding: root_binding.to_string(),
    };
    let state = state_for(key);
    let Ok(_single_flight) = Arc::clone(&state.gate).try_lock_owned() else {
        return;
    };

    let cursor = state
        .cursor
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let epoch_fatal = Arc::clone(&runtime.epoch_fatal);
    let resolver_queue = Arc::clone(&queue);
    let config = Arc::clone(&runtime.bot.config);
    let result = run_backfill_pass(
        resolver_queue,
        config,
        library_id.to_string(),
        root_binding.to_string(),
        cursor,
        move || !epoch_fatal.is_signaled(),
        |config, url| async move { resolve_bilibili_pgc_season_identity(&config, &url).await },
    )
    .await;

    if let Some(cursor) = result {
        *state
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = cursor;
    }
}

pub(crate) async fn current_root_binding(
    queue: Arc<QueueManager>,
    library_id: String,
) -> Option<String> {
    tokio::task::spawn_blocking(move || {
        queue
            .registry_snapshot()
            .ok()?
            .libraries
            .into_iter()
            .find(|library| library.id == library_id)?
            .root_binding_token()
    })
    .await
    .ok()
    .flatten()
}

async fn run_backfill_pass<F, Fut>(
    queue: Arc<QueueManager>,
    config: Arc<AppConfig>,
    library_id: String,
    root_binding: String,
    cursor: Option<String>,
    mut epoch_is_current: F,
    resolve: impl Fn(Arc<AppConfig>, String) -> Fut,
) -> Option<Option<String>>
where
    F: FnMut() -> bool,
    Fut: std::future::Future<Output = Result<Option<BilibiliPgcSeasonIdentity>>>,
{
    if !epoch_is_current() {
        return None;
    }
    if !root_binding_is_valid(Arc::clone(&queue), library_id.clone(), root_binding.clone()).await
        || !epoch_is_current()
    {
        return None;
    }

    let candidates_queue = Arc::clone(&queue);
    let candidates_library_id = library_id.clone();
    let candidates_root_binding = root_binding.clone();
    let candidate_result = tokio::task::spawn_blocking(move || {
        candidates_queue.published_backfill_candidates(
            &candidates_library_id,
            &candidates_root_binding,
            cursor.as_deref(),
            MAX_INDEX_RECORD_READS,
        )
    })
    .await;
    if !epoch_is_current() {
        return None;
    }
    if !root_binding_is_valid(Arc::clone(&queue), library_id.clone(), root_binding.clone()).await
        || !epoch_is_current()
    {
        return None;
    }
    let Ok(Ok((candidates, index_cursor))) = candidate_result else {
        return None;
    };

    let deadline = Instant::now() + MAX_RESOLUTION_WALL_TIME;
    let mut resolution_calls = 0;
    let mut processed_candidates = 0;
    let mut last_processed_id = None;
    for record in &candidates {
        if !epoch_is_current() {
            return None;
        }
        if !is_stable_terminal_published_record(record) {
            last_processed_id = Some(record.id.clone());
            processed_candidates += 1;
            continue;
        }
        let Some(source_url) = explicit_matching_pgc_url(record) else {
            last_processed_id = Some(record.id.clone());
            processed_candidates += 1;
            continue;
        };
        let Some(plan) = record.plan.as_ref() else {
            last_processed_id = Some(record.id.clone());
            processed_candidates += 1;
            continue;
        };
        if plan.bilibili_pgc_season_id.is_some() {
            last_processed_id = Some(record.id.clone());
            processed_candidates += 1;
            continue;
        }
        let Some(plan_episode_ids) = plan_episode_ids(plan) else {
            last_processed_id = Some(record.id.clone());
            processed_candidates += 1;
            continue;
        };
        if !attempts_fit_plan(record, &plan_episode_ids) {
            last_processed_id = Some(record.id.clone());
            processed_candidates += 1;
            continue;
        }
        if resolution_calls >= MAX_RESOLUTION_CALLS || Instant::now() >= deadline {
            break;
        }
        if !epoch_is_current() {
            return None;
        }
        if !root_binding_is_valid(Arc::clone(&queue), library_id.clone(), root_binding.clone())
            .await
            || !epoch_is_current()
        {
            return None;
        }

        resolution_calls += 1;
        let resolution = timeout(
            deadline.saturating_duration_since(Instant::now()),
            resolve(Arc::clone(&config), source_url.clone()),
        )
        .await;
        if !epoch_is_current() {
            return None;
        }
        if !root_binding_is_valid(Arc::clone(&queue), library_id.clone(), root_binding.clone())
            .await
            || !epoch_is_current()
        {
            return None;
        }
        if let Ok(Ok(Some(identity))) = resolution
            && identity.season_id > 0
            && plan_episode_ids
                .values()
                .all(|episode_id| identity.episode_orders.contains_key(episode_id))
            && !identity.episode_orders.is_empty()
        {
            let episode_orders = build_episode_orders(plan, &identity, &plan_episode_ids, record);
            if !episode_orders.is_empty() {
                if !epoch_is_current() {
                    return None;
                }
                if !root_binding_is_valid(
                    Arc::clone(&queue),
                    library_id.clone(),
                    root_binding.clone(),
                )
                .await
                    || !epoch_is_current()
                {
                    return None;
                }
                let cas_queue = Arc::clone(&queue);
                let cas_library_id = library_id.clone();
                let cas_root_binding = root_binding.clone();
                let snapshot = record.clone();
                let cas_result = tokio::task::spawn_blocking(move || {
                    cas_queue.backfill_pgc_season_id_if_unchanged(
                        &cas_library_id,
                        &cas_root_binding,
                        &snapshot,
                        identity.season_id,
                        &episode_orders,
                    )
                })
                .await;
                if !epoch_is_current() {
                    return None;
                }
                if !root_binding_is_valid(
                    Arc::clone(&queue),
                    library_id.clone(),
                    root_binding.clone(),
                )
                .await
                    || !epoch_is_current()
                {
                    return None;
                }
                // A join failure can mean the blocking CAS completed after its caller stopped
                // waiting. Do not retry it in this pass; the operation itself is idempotent.
                if let Ok(Ok(true)) = cas_result {
                    debug!("backfilled legacy Bilibili PGC season metadata");
                }
            }
        }
        last_processed_id = Some(record.id.clone());
        processed_candidates += 1;
    }

    if processed_candidates == candidates.len() {
        Some(index_cursor)
    } else {
        Some(last_processed_id)
    }
}

#[cfg(test)]
pub(crate) async fn backfill_legacy_pgc_metadata_with_resolver_for_test<F, Fut>(
    queue: Arc<QueueManager>,
    config: Arc<AppConfig>,
    library_id: &str,
    root_binding: &str,
    cursor: Option<String>,
    resolve: F,
) -> Option<Option<String>>
where
    F: Fn(Arc<AppConfig>, String) -> Fut,
    Fut: std::future::Future<Output = Result<Option<BilibiliPgcSeasonIdentity>>>,
{
    run_backfill_pass(
        queue,
        config,
        library_id.to_string(),
        root_binding.to_string(),
        cursor,
        || true,
        resolve,
    )
    .await
}

async fn root_binding_is_valid(
    queue: Arc<QueueManager>,
    library_id: String,
    root_binding: String,
) -> bool {
    tokio::task::spawn_blocking(move || {
        queue.pgc_backfill_root_binding_valid(&library_id, &root_binding)
    })
    .await
    .is_ok_and(|result| result.unwrap_or(false))
}

fn is_stable_terminal_published_record(record: &TaskRecord) -> bool {
    matches!(
        record.status,
        TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
    ) && record.destination_transfer.is_none()
        && record.destination_transfer_phase.is_none()
        && record.destination_transfer_ack.is_none()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PgcSource {
    Md(u64),
    Ss(u64),
}

fn explicit_matching_pgc_url(record: &TaskRecord) -> Option<String> {
    let JobRequest::Bilibili { url, .. } = &record.job else {
        return None;
    };
    let (job_source, clean_url) = explicit_pgc_url(url)?;
    let (original_source, _) = explicit_pgc_url(&record.original_url)?;
    (job_source == original_source).then_some(clean_url)
}

fn explicit_pgc_url(raw_url: &str) -> Option<(PgcSource, String)> {
    let mut url = Url::parse(raw_url).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    let host = url.host_str()?.to_ascii_lowercase();
    if host != "bilibili.com" && !host.ends_with(".bilibili.com") {
        return None;
    }
    let last_segment = url.path_segments()?.rfind(|segment| !segment.is_empty())?;
    let source = parse_pgc_source_segment(last_segment)?;
    url.set_query(None);
    url.set_fragment(None);
    Some((source, url.to_string()))
}

fn parse_pgc_source_segment(segment: &str) -> Option<PgcSource> {
    let (kind, digits) = if let Some(digits) = segment.strip_prefix("md") {
        (b'm', digits)
    } else {
        (b's', segment.strip_prefix("ss")?)
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let id = digits.parse::<u64>().ok().filter(|id| *id > 0)?;
    Some(if kind == b'm' {
        PgcSource::Md(id)
    } else {
        PgcSource::Ss(id)
    })
}

fn episode_id_component(stable_media_id: &str) -> Option<Option<u64>> {
    let fields = stable_media_id.split(':').collect::<Vec<_>>();
    let mut episode_id = None;
    for pair in fields.windows(2) {
        if pair[0] != "epid" {
            continue;
        }
        if episode_id.is_some() {
            return Some(None);
        }
        episode_id = Some(
            pair[1]
                .parse::<u64>()
                .ok()
                .filter(|episode_id| *episode_id > 0),
        );
    }
    episode_id
}

fn plan_episode_ids(plan: &PlanValidationSnapshot) -> Option<BTreeMap<String, u64>> {
    if plan.stable_media_ids.is_empty() {
        return None;
    }
    let mut plan_episode_ids = BTreeMap::new();
    for stable_media_id in &plan.stable_media_ids {
        let Some(Some(episode_id)) = episode_id_component(stable_media_id) else {
            return None;
        };
        plan_episode_ids.insert(stable_media_id.clone(), episode_id);
    }
    if plan_episode_ids.len() != plan.stable_media_ids.iter().collect::<BTreeSet<_>>().len() {
        return None;
    }
    if plan_episode_ids.values().collect::<BTreeSet<_>>().len() != plan_episode_ids.len() {
        return None;
    }

    let stable_ids = plan_episode_ids
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    for identity in plan
        .bilibili_group_entries
        .iter()
        .chain(plan.bilibili_pgc_episode_orders.iter())
    {
        if !stable_ids.contains(identity.stable_media_id.as_str()) {
            return None;
        }
    }
    let mut references = plan
        .stable_media_ids
        .iter()
        .map(String::as_str)
        .chain(plan.bilibili_resolved_stream_ids.iter().map(String::as_str))
        .chain(plan.selected_format_ids.iter().map(String::as_str))
        .chain(plan.exact_sizes.iter().map(|size| size.subject.as_str()))
        .chain(
            plan.approximate_sizes
                .iter()
                .map(|size| size.subject.as_str()),
        )
        .chain(plan.resolution_codecs.iter().map(String::as_str));
    for reference in references.by_ref() {
        match episode_id_component(reference) {
            Some(Some(episode_id)) if plan_episode_ids.values().any(|id| *id == episode_id) => {}
            Some(_) => return None,
            None => {}
        }
    }
    Some(plan_episode_ids)
}

fn attempts_fit_plan(record: &TaskRecord, plan_episode_ids: &BTreeMap<String, u64>) -> bool {
    let Some(attempt) = record.bilibili_attempt.as_ref() else {
        return true;
    };
    attempt.entries.iter().all(|entry| {
        if entry.stable_media_id.trim().is_empty() {
            return entry.state != crate::bilibili_attempt::BilibiliAttemptEntryState::Verified;
        }
        plan_episode_ids.contains_key(&entry.stable_media_id)
            && episode_id_component(&entry.stable_media_id)
                .flatten()
                .is_some_and(|episode_id| plan_episode_ids.values().any(|id| *id == episode_id))
    })
}

fn build_episode_orders(
    plan: &PlanValidationSnapshot,
    identity: &BilibiliPgcSeasonIdentity,
    plan_episode_ids: &BTreeMap<String, u64>,
    record: &TaskRecord,
) -> Vec<BilibiliGroupEntryIdentity> {
    let mut titles = BTreeMap::new();
    for entry in plan
        .bilibili_pgc_episode_orders
        .iter()
        .chain(plan.bilibili_group_entries.iter())
    {
        if !entry.title.trim().is_empty() {
            titles
                .entry(entry.stable_media_id.as_str())
                .or_insert(entry.title.as_str());
        }
    }
    if let Some(attempt) = record.bilibili_attempt.as_ref() {
        for entry in &attempt.entries {
            if !entry.title.trim().is_empty() {
                titles
                    .entry(entry.stable_media_id.as_str())
                    .or_insert(entry.title.as_str());
            }
        }
    }
    plan_episode_ids
        .iter()
        .filter_map(|(stable_media_id, episode_id)| {
            Some(BilibiliGroupEntryIdentity {
                index: *identity.episode_orders.get(episode_id)?,
                title: titles
                    .get(stable_media_id.as_str())
                    .map(|title| (*title).to_string())
                    .unwrap_or_default(),
                stable_media_id: stable_media_id.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_plan() -> PlanValidationSnapshot {
        PlanValidationSnapshot {
            stable_media_ids: vec![
                "bvid:BV1fixture:aid:11:cid:101:epid:1001".to_string(),
                "bvid:BV1fixture:aid:12:cid:102:epid:1002".to_string(),
            ],
            selected_format_ids: vec![
                "bvid:BV1fixture:aid:11:cid:101:epid:1001:video:64".to_string(),
            ],
            title: Some("Fixture season".to_string()),
            ..PlanValidationSnapshot::default()
        }
    }

    #[test]
    fn only_explicit_matching_md_or_ss_source_is_eligible() {
        let mut record = fixture_record();
        assert_eq!(
            explicit_matching_pgc_url(&record).as_deref(),
            Some("https://www.bilibili.com/bangumi/media/md123")
        );

        record.original_url = "https://www.bilibili.com/bangumi/play/ss9876".to_string();
        record.job = JobRequest::Bilibili {
            url: "https://www.bilibili.com/bangumi/play/ss9876".to_string(),
            selection: Some(crate::router::BilibiliSelection::Current),
        };
        assert_eq!(
            explicit_matching_pgc_url(&record).as_deref(),
            Some("https://www.bilibili.com/bangumi/play/ss9876")
        );

        record.original_url = "https://www.bilibili.com/bangumi/media/md124".to_string();
        assert!(explicit_matching_pgc_url(&record).is_none());
        record.original_url = "https://www.bilibili.com/bangumi/play/ep123".to_string();
        assert!(explicit_matching_pgc_url(&record).is_none());
        record.original_url =
            "https://user:secret@www.bilibili.com/bangumi/media/md123".to_string();
        assert!(explicit_matching_pgc_url(&record).is_none());
    }

    #[test]
    fn requires_every_saved_episode_identity_to_fit_the_resolved_season() {
        let plan = fixture_plan();
        let plan_ids = plan_episode_ids(&plan).expect("legacy PGC plan should be valid");
        let identity = BilibiliPgcSeasonIdentity {
            season_id: 9876,
            episode_orders: BTreeMap::from([(1001, 10), (1002, 11)]),
        };
        assert!(
            plan_ids
                .values()
                .all(|episode_id| identity.episode_orders.contains_key(episode_id))
        );
        let orders = build_episode_orders(&plan, &identity, &plan_ids, &fixture_record());
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].index, 10);
        assert_eq!(orders[1].index, 11);

        let invalid = BilibiliPgcSeasonIdentity {
            season_id: 9876,
            episode_orders: BTreeMap::from([(1001, 10)]),
        };
        assert!(
            !plan_ids
                .values()
                .all(|episode_id| invalid.episode_orders.contains_key(episode_id))
        );
    }

    #[test]
    fn rejects_receipt_ids_outside_the_saved_plan_and_invalid_episode_numbers() {
        let mut record = fixture_record();
        let plan_ids = plan_episode_ids(record.plan.as_ref().unwrap()).unwrap();
        let mut attempt = crate::bilibili_attempt::BilibiliAttemptSnapshot::new_independent(
            "Fixture season".to_string(),
            [(
                10,
                "Episode 10".to_string(),
                Some("bvid:BV1fixture:cid:101:epid:1001".to_string()),
            )],
            0,
        );
        attempt.entries[0].state = crate::bilibili_attempt::BilibiliAttemptEntryState::Verified;
        attempt.entries[0].stable_media_id = "bvid:BV1fixture:cid:101:epid:9999".to_string();
        record.bilibili_attempt = Some(attempt);
        assert!(!attempts_fit_plan(&record, &plan_ids));

        let mut invalid_plan = fixture_plan();
        invalid_plan.stable_media_ids[0] = "bvid:BV1fixture:cid:101:epid:none".to_string();
        assert!(plan_episode_ids(&invalid_plan).is_none());

        let mut duplicate_episode_plan = fixture_plan();
        duplicate_episode_plan.stable_media_ids[1] =
            "bvid:BV1fixture:aid:12:cid:102:epid:1001".to_string();
        assert!(plan_episode_ids(&duplicate_episode_plan).is_none());
    }

    fn fixture_record() -> TaskRecord {
        TaskRecord {
            schema_version: 1,
            id: "fixture".to_string(),
            update_id: 1,
            message_id: 1,
            status_message_id: None,
            chat_id: 1,
            submitter_user_id: None,
            ordinal: 1,
            original_url: "https://www.bilibili.com/bangumi/media/md123".to_string(),
            url_was_sanitized: false,
            job: JobRequest::Bilibili {
                url: "https://www.bilibili.com/bangumi/media/md123?spm_id_from=fixture".to_string(),
                selection: Some(crate::router::BilibiliSelection::Latest),
            },
            destination: None,
            destination_transfer: None,
            destination_transfer_phase: None,
            destination_transfer_ack: None,
            status: TaskStatus::Completed,
            cancel_requested: false,
            plan: Some(fixture_plan()),
            proposed_plan: None,
            display_metadata: None,
            saved_location: None,
            primary_media_hashes: BTreeMap::new(),
            primary_media_hash_manifest: None,
            staging_attempts: Vec::new(),
            media_entries_total: 2,
            media_entries_completed: 2,
            media_entries_failed: 0,
            bilibili_attempt: None,
            error: None,
            created_at: 1,
            updated_at: 2,
            revision: 1,
            generation: 3,
            activity_revision: 4,
            user_actions: 0,
        }
    }
}
