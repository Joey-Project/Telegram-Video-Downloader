use std::future::Future;
use std::time::Duration;

use anyhow::{Result, anyhow};
use bbdown_core::{
    BiliClient, DownloadMode, DownloadPlan, EpisodeMetadata, Input, ResolvedContent, Selection,
};
use tokio::task::JoinSet;
use tokio::time::timeout;

const MAX_EPISODE_PLANNING_CONCURRENCY: usize = 2;

/// Resolves a Bilibili input and plans its selected download entries.
///
/// Season episodes are planned independently with a per-entry timeout and a
/// concurrency limit of two. Other inputs use the configured planning timeout,
/// which defaults to 60 seconds.
pub async fn plan_download(
    client: &BiliClient,
    url: &str,
    selection: Option<Selection>,
    mode: DownloadMode,
    per_entry_timeout: Duration,
    progress: impl Fn(usize, usize),
) -> Result<(DownloadPlan, ResolvedContent)> {
    let (input, resolved) = timeout(per_entry_timeout, async {
        let input = client.parse_input(url).await?;
        let resolved = client.resolve(input.clone(), selection.clone()).await?;
        Ok::<_, bbdown_core::Error>((input, resolved))
    })
    .await
    .map_err(|_| anyhow!("Bilibili input resolution timed out"))?
    .map_err(|error| {
        anyhow!(
            "failed to parse or resolve Bilibili input: {}",
            safe_planning_failure(&error)
        )
    })?;

    let ResolvedContent::Season(season) = &resolved else {
        let plan = timeout(
            per_entry_timeout,
            client.plan_with_download_mode(input, selection, mode),
        )
        .await
        .map_err(|_| anyhow!("Bilibili download planning timed out"))?
        .map_err(|error| {
            anyhow!(
                "failed to plan Bilibili download: {}",
                safe_planning_failure(&error)
            )
        })?;
        progress(plan.entries.len(), plan.entries.len());
        return Ok((plan, resolved));
    };

    let season_title = season.season.title.clone();
    let episodes = season.selected_episodes.clone();
    let total = episodes.len();
    progress(0, total);
    if episodes.is_empty() {
        return Err(anyhow!("season selection resolved to no episodes"));
    }
    let input_kind = episode_input_kind(&input);
    let work = episodes
        .into_iter()
        .map(|episode| {
            let index = episode.index;
            (index, (episode, input_kind))
        })
        .collect();

    let client = client.clone();
    let plans = schedule_bounded(
        work,
        per_entry_timeout,
        move |(episode, input_kind)| {
            let client = client.clone();
            async move {
                let input = input_kind.make_input(episode.epid);
                let mut plan = client
                    .plan_with_download_mode(input, Some(Selection::Episode(episode.epid)), mode)
                    .await
                    .map_err(|error| TaskFailure::Upstream(safe_planning_failure(&error)))?;
                validate_and_preserve_episode(&mut plan, &episode)?;
                Ok(plan.entries)
            }
        },
        progress,
    )
    .await?;

    let entries = plans.into_iter().flatten().collect();
    Ok((
        DownloadPlan {
            title: season_title,
            entries,
        },
        resolved,
    ))
}

#[derive(Clone, Copy)]
enum EpisodeInputKind {
    Pgc,
    Cheese,
    International,
}

impl EpisodeInputKind {
    const fn make_input(self, epid: u64) -> Input {
        match self {
            Self::Pgc => Input::Episode(epid),
            Self::Cheese => Input::CheeseEpisode(epid),
            Self::International => Input::IntlEpisode(epid),
        }
    }
}

fn episode_input_kind(input: &Input) -> EpisodeInputKind {
    match input {
        Input::CheeseEpisode(_) | Input::CheeseSeason(_) => EpisodeInputKind::Cheese,
        Input::IntlEpisode(_) => EpisodeInputKind::International,
        _ => EpisodeInputKind::Pgc,
    }
}

fn validate_and_preserve_episode(
    plan: &mut DownloadPlan,
    expected: &EpisodeMetadata,
) -> std::result::Result<(), TaskFailure> {
    if plan.entries.len() != 1 {
        return Err(TaskFailure::UnexpectedEntryCount);
    }

    let entry = &mut plan.entries[0];
    if entry.epid != Some(expected.epid) || entry.aid != expected.aid || entry.cid != expected.cid {
        return Err(TaskFailure::IdentityChanged);
    }

    // Keep the index from the initially resolved season inventory.
    entry.index = expected.index;
    Ok(())
}

async fn schedule_bounded<T, O, F, Fut>(
    tasks: Vec<(u32, T)>,
    per_task_timeout: Duration,
    make_future: F,
    progress: impl Fn(usize, usize),
) -> Result<Vec<O>>
where
    T: Send + 'static,
    O: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<O, TaskFailure>> + Send + 'static,
{
    let total = tasks.len();
    if total == 0 {
        return Ok(Vec::new());
    }

    let make_future = std::sync::Arc::new(make_future);
    let mut pending = tasks.into_iter().enumerate();
    let mut join_set = JoinSet::new();
    let mut ordered: Vec<Option<O>> = (0..total).map(|_| None).collect();
    let mut completed = 0;

    for _ in 0..MAX_EPISODE_PLANNING_CONCURRENCY.min(total) {
        spawn_episode_task(
            &mut join_set,
            pending.next().expect("bounded by task count"),
            per_task_timeout,
            std::sync::Arc::clone(&make_future),
        );
    }

    while let Some(joined) = join_set.join_next().await {
        let (ordinal, episode_index, task_result) = match joined {
            Ok(joined) => joined,
            Err(_) => {
                join_set.abort_all();
                while join_set.join_next().await.is_some() {}
                return Err(anyhow!(
                    "episode planning task was cancelled or failed unexpectedly"
                ));
            }
        };
        let output = match task_result {
            Ok(output) => output,
            Err(error) => {
                join_set.abort_all();
                while join_set.join_next().await.is_some() {}
                return Err(anyhow!("episode index {episode_index} {}", error.message()));
            }
        };
        ordered[ordinal] = Some(output);
        completed += 1;
        progress(completed, total);

        if let Some(next) = pending.next() {
            spawn_episode_task(
                &mut join_set,
                next,
                per_task_timeout,
                std::sync::Arc::clone(&make_future),
            );
        }
    }

    Ok(ordered.into_iter().flatten().collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TaskFailure {
    Timeout,
    Upstream(String),
    UnexpectedEntryCount,
    IdentityChanged,
}

impl TaskFailure {
    fn message(&self) -> String {
        match self {
            Self::Timeout => "planning timed out".to_string(),
            Self::Upstream(reason) => format!("planning failed: {reason}"),
            Self::UnexpectedEntryCount => "planner returned an unexpected entry count".to_string(),
            Self::IdentityChanged => "episode identity changed during planning".to_string(),
        }
    }
}

fn safe_planning_failure(error: &bbdown_core::Error) -> String {
    // API denial messages explain missing stream details. Keep HTTP/IO errors generic
    // because they can include credential-bearing URLs or local configuration paths.
    let reason = match error {
        bbdown_core::Error::Api { code, message } => format!("API returned code {code}: {message}"),
        bbdown_core::Error::AccessRestricted(message) => format!("access restricted: {message}"),
        _ => "upstream request failed".to_string(),
    };
    crate::redaction::redact_sensitive_text(&reason)
        .chars()
        .take(256)
        .collect()
}

fn spawn_episode_task<T, O, F, Fut>(
    join_set: &mut JoinSet<(usize, u32, std::result::Result<O, TaskFailure>)>,
    (ordinal, (episode_index, task)): (usize, (u32, T)),
    per_task_timeout: Duration,
    make_future: std::sync::Arc<F>,
) where
    T: Send + 'static,
    O: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<O, TaskFailure>> + Send + 'static,
{
    join_set.spawn(async move {
        let result = match timeout(per_task_timeout, make_future(task)).await {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(TaskFailure::Timeout),
        };
        (ordinal, episode_index, result)
    });
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use anyhow::Result;
    use tokio::sync::Notify;
    use tokio::time::{sleep, timeout};

    use super::{
        EpisodeInputKind, TaskFailure, episode_input_kind, safe_planning_failure, schedule_bounded,
        validate_and_preserve_episode,
    };
    use bbdown_core::{DownloadPlan, EpisodeMetadata};

    #[tokio::test]
    async fn episode_failure_preserves_api_denial_reason() {
        let error = schedule_bounded(
            vec![(203, ())],
            Duration::from_secs(1),
            |_| async {
                Err::<(), _>(TaskFailure::Upstream(safe_planning_failure(
                    &bbdown_core::Error::Api {
                        code: -10403,
                        message: "This video is not available in your region".to_string(),
                    },
                )))
            },
            |_, _| {},
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("episode index 203"));
        assert!(error.to_string().contains("API returned code -10403"));
        assert!(error.to_string().contains("not available in your region"));
    }

    #[tokio::test(start_paused = true)]
    async fn independent_timeouts_allow_total_duration_over_timeout() -> Result<()> {
        let progress = Arc::new(AtomicUsize::new(0));
        let progress_sink = Arc::clone(&progress);
        let plans = schedule_bounded(
            (1..=6).map(|index| (index, index)).collect(),
            Duration::from_secs(10),
            |value| async move {
                sleep(Duration::from_secs(6)).await;
                Ok(value)
            },
            move |done, total| {
                assert_eq!(done, progress_sink.fetch_add(1, Ordering::SeqCst) + 1);
                assert_eq!(total, 6);
            },
        )
        .await?;

        assert_eq!(plans, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(progress.load(Ordering::SeqCst), 6);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn scheduler_limits_concurrency_to_two() -> Result<()> {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let active_for_task = Arc::clone(&active);
        let peak_for_task = Arc::clone(&peak);
        schedule_bounded(
            (1..=7).map(|index| (index, index)).collect(),
            Duration::from_secs(10),
            move |value| {
                let active = Arc::clone(&active_for_task);
                let peak = Arc::clone(&peak_for_task);
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    sleep(Duration::from_secs(1)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(value)
                }
            },
            |_, _| {},
        )
        .await?;
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn completion_order_does_not_change_return_order() -> Result<()> {
        let results = schedule_bounded(
            vec![(11, (0, 9)), (22, (1, 2)), (33, (2, 1))],
            Duration::from_secs(20),
            |(value, delay)| async move {
                sleep(Duration::from_secs(delay)).await;
                Ok(value)
            },
            |_, _| {},
        )
        .await?;
        assert_eq!(results, vec![0, 1, 2]);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_error_contains_original_episode_index() {
        let error = schedule_bounded(
            vec![(17, ())],
            Duration::from_secs(5),
            |_| async move {
                std::future::pending::<()>().await;
                Ok(())
            },
            |_, _| {},
        )
        .await
        .expect_err("pending task should time out");
        assert!(error.to_string().contains("episode index 17"));
        assert!(error.to_string().contains("timed out"));
    }

    #[tokio::test(start_paused = true)]
    async fn failure_cancels_running_tasks_and_does_not_start_pending_tasks() {
        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let started = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));
        let started_for_task = Arc::clone(&started);
        let cancelled_for_task = Arc::clone(&cancelled);
        let work = tokio::spawn(async move {
            schedule_bounded(
                vec![(1, 1), (2, 2), (3, 3)],
                Duration::from_secs(5),
                move |value| {
                    let started = Arc::clone(&started_for_task);
                    let cancelled = Arc::clone(&cancelled_for_task);
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        if value == 1 {
                            while started.load(Ordering::SeqCst) < 2 {
                                tokio::task::yield_now().await;
                            }
                            Err(TaskFailure::Upstream(
                                "fixture upstream failure".to_string(),
                            ))
                        } else {
                            let _drop_flag = DropFlag(cancelled);
                            std::future::pending::<()>().await;
                            Ok(value)
                        }
                    }
                },
                |_, _| {},
            )
            .await
        });
        let result = work.await.expect("scheduler task should finish");
        assert!(result.is_err());
        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(started.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_scheduler_future_drops_started_children() {
        struct DropSignal {
            dropped: Arc<AtomicBool>,
            notify: Arc<Notify>,
        }
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::SeqCst);
                self.notify.notify_one();
            }
        }

        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let drop_notify = Arc::new(Notify::new());
        let started_for_task = Arc::clone(&started);
        let dropped_for_task = Arc::clone(&dropped);
        let notify_for_task = Arc::clone(&drop_notify);
        let scheduler = tokio::spawn(async move {
            schedule_bounded(
                vec![(1, ()), (2, ()), (3, ())],
                Duration::from_secs(10),
                move |_| {
                    let dropped = Arc::clone(&dropped_for_task);
                    let notify = Arc::clone(&notify_for_task);
                    let started = Arc::clone(&started_for_task);
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        let _signal = DropSignal { dropped, notify };
                        std::future::pending::<()>().await;
                        Ok(())
                    }
                },
                |_, _| {},
            )
            .await
        });

        while started.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        scheduler.abort();
        assert!(
            scheduler
                .await
                .expect_err("scheduler task should be aborted")
                .is_cancelled()
        );
        timeout(Duration::from_secs(1), drop_notify.notified())
            .await
            .expect("dropping the scheduler must drop its child tasks");
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn empty_selection_is_a_valid_empty_schedule() -> Result<()> {
        let progress_count = Arc::new(AtomicUsize::new(0));
        let progress_sink = Arc::clone(&progress_count);
        let result: Vec<usize> = schedule_bounded(
            Vec::<(u32, usize)>::new(),
            Duration::from_secs(1),
            |value| async move { Ok(value) },
            move |_, _| {
                progress_sink.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await?;
        assert!(result.is_empty());
        assert_eq!(progress_count.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn episode_input_kind_preserves_pgc_cheese_and_international_sources() {
        assert!(matches!(
            episode_input_kind(&bbdown_core::Input::Episode(42)),
            EpisodeInputKind::Pgc
        ));
        assert!(matches!(
            episode_input_kind(&bbdown_core::Input::CheeseEpisode(42)),
            EpisodeInputKind::Cheese
        ));
        assert!(matches!(
            episode_input_kind(&bbdown_core::Input::IntlEpisode(42)),
            EpisodeInputKind::International
        ));
        assert!(matches!(
            episode_input_kind(&bbdown_core::Input::CheeseSeason(42)),
            EpisodeInputKind::Cheese
        ));
    }

    #[test]
    fn refreshed_episode_identity_must_match_and_original_index_is_preserved() -> Result<()> {
        let expected = EpisodeMetadata {
            index: 9,
            aid: 100,
            bvid: Some("BV1xx411c7mD".to_owned()),
            cid: 200,
            epid: 300,
            title: "Episode title".to_owned(),
            long_title: None,
            pub_time: None,
        };
        let mut refreshed = test_plan(vec![test_entry(1, 100, 200, 300)])?;
        validate_and_preserve_episode(&mut refreshed, &expected)
            .expect("matching identity should be accepted");
        let entry = &refreshed.entries[0];
        assert_eq!(entry.index, 9);
        assert_eq!(entry.aid, expected.aid);
        assert_eq!(entry.cid, expected.cid);
        assert_eq!(entry.epid, Some(expected.epid));

        let mut changed_cid = test_plan(vec![test_entry(1, 100, 201, 300)])?;
        assert_eq!(
            validate_and_preserve_episode(&mut changed_cid, &expected),
            Err(TaskFailure::IdentityChanged)
        );
        let mut changed_epid = test_plan(vec![test_entry(1, 100, 200, 301)])?;
        assert_eq!(
            validate_and_preserve_episode(&mut changed_epid, &expected),
            Err(TaskFailure::IdentityChanged)
        );

        let entry = test_entry(1, 100, 200, 300);
        let mut multiple = test_plan(vec![entry.clone(), entry])?;
        assert_eq!(
            validate_and_preserve_episode(&mut multiple, &expected),
            Err(TaskFailure::UnexpectedEntryCount)
        );
        Ok(())
    }

    fn test_plan(entries: Vec<bbdown_core::DownloadEntry>) -> Result<DownloadPlan> {
        Ok(DownloadPlan {
            title: "Season title".to_owned(),
            entries,
        })
    }

    fn test_entry(index: u32, aid: u64, cid: u64, epid: u64) -> bbdown_core::DownloadEntry {
        serde_json::from_value(serde_json::json!({
            "index": index,
            "aid": aid,
            "bvid": "BV1xx411c7mD",
            "cid": cid,
            "epid": epid,
            "title": "Episode title",
            "source": "pgc_web",
            "streams": {
                "videos": [],
                "audios": [],
                "flv_segments": [],
                "accept_quality": [],
                "duration_seconds": null
            },
            "subtitles": [],
            "danmaku": {
                "cid": cid,
                "xml_url": "https://comment.example/1.xml"
            }
        }))
        .expect("test entry JSON should deserialize")
    }
}
