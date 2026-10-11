use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;

use crate::redaction::redact_sensitive_text;
use serde::{Deserialize, Serialize};

/// The terminal result of a Bilibili download attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BilibiliAttemptOutcome {
    Failed,
    PartiallyCompleted,
    Cancelled,
    Interrupted,
    Verified,
}

/// A bounded per-entry progress update applied to an existing snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BilibiliAttemptUpdate {
    Started,
    Stage {
        stage: String,
    },
    Completed,
    Failed,
    Retrying {
        attempt: usize,
        error: String,
    },
    Failure {
        error: String,
        attempts: usize,
    },
    AlreadyPresent {
        media_paths: Vec<PathBuf>,
        hashes: BTreeMap<String, String>,
    },
    Verified {
        media_paths: Vec<PathBuf>,
        hashes: BTreeMap<String, String>,
    },
}

impl BilibiliAttemptUpdate {
    pub fn apply(&self, snapshot: &mut BilibiliAttemptSnapshot, index: u32) {
        match self {
            Self::Started => snapshot.entry_started(index),
            Self::Stage { stage } => snapshot.stage_started(index, stage.clone()),
            Self::Completed => snapshot.entry_completed(index),
            Self::Failed => snapshot.entry_failed(index),
            Self::Retrying { attempt, error } => {
                snapshot.entry_retrying(index, *attempt, error.clone())
            }
            Self::Failure { error, attempts } => {
                snapshot.entry_failed_with_error(index, *attempts, error.clone())
            }
            Self::AlreadyPresent {
                media_paths,
                hashes,
            } => snapshot.entry_already_present_with_outputs(
                index,
                media_paths.clone(),
                hashes.clone(),
            ),
            Self::Verified {
                media_paths,
                hashes,
            } => snapshot.entry_verified_with_outputs(index, media_paths.clone(), hashes.clone()),
        }
    }
}

/// Progress for one explicitly selected episode or video entry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BilibiliAttemptEntry {
    pub index: u32,
    pub title: String,
    pub state: BilibiliAttemptEntryState,
    /// The most recently reported operation, such as `audio` or `danmaku`.
    pub stage: String,
    #[serde(default)]
    pub already_present: bool,
    #[serde(default)]
    pub requires_redownload: bool,
    #[serde(default)]
    pub attempts: usize,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub stable_media_id: String,
    #[serde(default)]
    pub published_media_paths: Vec<PathBuf>,
    #[serde(default)]
    pub published_media_hashes: BTreeMap<String, String>,
}

/// State of one entry in the current attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BilibiliAttemptEntryState {
    Pending,
    Downloading,
    Downloaded,
    Failed,
    Interrupted,
    Verified,
}

/// A user-facing, serializable snapshot of one Bilibili download attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BilibiliAttemptSnapshot {
    pub title: String,
    pub entries: Vec<BilibiliAttemptEntry>,
    /// Entries that were already present and therefore excluded from this attempt.
    pub skipped_entries: usize,
    pub outcome: Option<BilibiliAttemptOutcome>,
    #[serde(default)]
    pub independent_entries: bool,
    #[serde(default)]
    pub retry_indices: Option<Vec<u32>>,
    /// Normalized media requirements used to create the episode receipts.
    #[serde(default)]
    pub media_profile: Option<String>,
}

const SUMMARY_MAX_BYTES: usize = 1_800;
const MAX_DETAIL_RANGES: usize = 8;
const MAX_DETAIL_ENTRIES: usize = 8;

impl BilibiliAttemptSnapshot {
    pub fn new(
        title: String,
        entries: impl IntoIterator<Item = (u32, String)>,
        skipped_entries: usize,
    ) -> Self {
        Self {
            title,
            entries: entries
                .into_iter()
                .map(|(index, title)| BilibiliAttemptEntry {
                    index,
                    title,
                    state: BilibiliAttemptEntryState::Pending,
                    stage: String::new(),
                    already_present: false,
                    requires_redownload: false,
                    attempts: 0,
                    error: None,
                    stable_media_id: String::new(),
                    published_media_paths: Vec::new(),
                    published_media_hashes: BTreeMap::new(),
                })
                .collect(),
            skipped_entries,
            outcome: None,
            independent_entries: false,
            retry_indices: None,
            media_profile: None,
        }
    }

    /// Create a grouped attempt whose selected entries can be published independently.
    pub fn new_independent(
        title: String,
        entries: impl IntoIterator<Item = (u32, String, Option<String>)>,
        skipped_entries: usize,
    ) -> Self {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut snapshot = Self::new(
            title,
            entries
                .iter()
                .map(|(index, title, _)| (*index, title.clone())),
            skipped_entries,
        );
        snapshot.independent_entries = true;
        for (entry, (_, _, stable_media_id)) in snapshot.entries.iter_mut().zip(entries) {
            entry.stable_media_id = stable_media_id.unwrap_or_default();
        }
        snapshot
    }

    pub fn entry_started(&mut self, index: u32) {
        if self.outcome.is_some() {
            return;
        }
        for entry in self.entries.iter_mut().filter(|entry| entry.index == index) {
            if matches!(
                entry.state,
                BilibiliAttemptEntryState::Pending
                    | BilibiliAttemptEntryState::Failed
                    | BilibiliAttemptEntryState::Interrupted
                    | BilibiliAttemptEntryState::Downloading
            ) {
                entry.state = BilibiliAttemptEntryState::Downloading;
                entry.stage.clear();
                entry.already_present = false;
                entry.attempts = entry.attempts.max(1);
                entry.published_media_paths.clear();
                entry.published_media_hashes.clear();
            }
        }
        self.refresh_independent_skipped_entries();
    }

    pub fn stage_started(&mut self, index: u32, stage: String) {
        if self.outcome.is_some() {
            return;
        }
        for entry in self.entries.iter_mut().filter(|entry| entry.index == index) {
            match entry.state {
                BilibiliAttemptEntryState::Pending | BilibiliAttemptEntryState::Downloading => {
                    entry.state = BilibiliAttemptEntryState::Downloading;
                    entry.stage = stage.clone();
                }
                BilibiliAttemptEntryState::Downloaded if self.independent_entries => {
                    entry.stage = stage.clone();
                }
                _ => {}
            }
        }
    }

    pub fn entry_completed(&mut self, index: u32) {
        if self.outcome.is_some() {
            return;
        }
        for entry in self.entries.iter_mut().filter(|entry| entry.index == index) {
            if !matches!(
                entry.state,
                BilibiliAttemptEntryState::Downloaded | BilibiliAttemptEntryState::Verified
            ) {
                entry.state = BilibiliAttemptEntryState::Downloaded;
                entry.stage.clear();
                entry.already_present = false;
                entry.error = None;
            }
        }
        self.refresh_independent_skipped_entries();
    }

    pub fn entry_failed(&mut self, index: u32) {
        if self.outcome.is_some() {
            return;
        }
        for entry in self.entries.iter_mut().filter(|entry| entry.index == index) {
            if !matches!(
                entry.state,
                BilibiliAttemptEntryState::Downloaded | BilibiliAttemptEntryState::Verified
            ) {
                entry.state = BilibiliAttemptEntryState::Failed;
                entry.already_present = false;
                entry.attempts = entry.attempts.max(1);
            }
        }
        self.refresh_independent_skipped_entries();
    }

    pub fn entry_retrying(&mut self, index: u32, attempt: usize, error: String) {
        if self.outcome.is_some() {
            return;
        }
        let error = sanitize_entry_error(&error);
        for entry in self.entries.iter_mut().filter(|entry| entry.index == index) {
            if entry.state != BilibiliAttemptEntryState::Verified {
                entry.state = BilibiliAttemptEntryState::Downloading;
                entry.stage.clear();
                entry.already_present = false;
                entry.attempts = entry.attempts.max(attempt);
                entry.error = Some(error.clone());
                entry.published_media_paths.clear();
                entry.published_media_hashes.clear();
            }
        }
        self.refresh_independent_skipped_entries();
    }

    pub fn entry_failed_with_error(&mut self, index: u32, attempts: usize, error: String) {
        if self.outcome.is_some() {
            return;
        }
        let error = sanitize_entry_error(&error);
        for entry in self.entries.iter_mut().filter(|entry| entry.index == index) {
            if entry.state != BilibiliAttemptEntryState::Verified {
                entry.state = BilibiliAttemptEntryState::Failed;
                entry.already_present = false;
                if !self.independent_entries {
                    entry.stage.clear();
                }
                entry.attempts = entry.attempts.max(attempts);
                entry.error = Some(error.clone());
                entry.published_media_paths.clear();
                entry.published_media_hashes.clear();
            }
        }
        self.refresh_independent_skipped_entries();
    }

    pub fn entry_verified(&mut self, index: u32) {
        if self.outcome.is_some() {
            return;
        }
        for entry in self.entries.iter_mut().filter(|entry| entry.index == index) {
            entry.state = BilibiliAttemptEntryState::Verified;
            entry.stage.clear();
            if self.independent_entries {
                entry.already_present = false;
                entry.requires_redownload = false;
            }
            entry.error = None;
            entry.attempts = entry.attempts.max(1);
        }
        self.refresh_independent_skipped_entries();
    }

    pub fn entry_verified_with_outputs(
        &mut self,
        index: u32,
        media_paths: Vec<PathBuf>,
        hashes: BTreeMap<String, String>,
    ) {
        self.entry_verified(index);
        if self.outcome.is_some() {
            return;
        }
        for entry in self.entries.iter_mut().filter(|entry| entry.index == index) {
            entry.published_media_paths = media_paths.clone();
            entry.published_media_hashes = hashes.clone();
        }
    }

    pub fn entry_already_present_with_outputs(
        &mut self,
        index: u32,
        media_paths: Vec<PathBuf>,
        hashes: BTreeMap<String, String>,
    ) {
        if !self.independent_entries {
            return;
        }
        let previous_attempts: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| entry.index == index)
            .map(|entry| (entry.attempts, entry.requires_redownload))
            .collect();
        self.entry_verified_with_outputs(index, media_paths, hashes);
        if self.outcome.is_some() {
            return;
        }
        for (entry, attempts) in self
            .entries
            .iter_mut()
            .filter(|entry| entry.index == index)
            .zip(previous_attempts)
        {
            entry.already_present = true;
            entry.attempts = attempts.0;
            entry.requires_redownload = attempts.1;
        }
        self.refresh_independent_skipped_entries();
    }

    fn refresh_independent_skipped_entries(&mut self) {
        if self.independent_entries {
            self.skipped_entries = self
                .entries
                .iter()
                .filter(|entry| entry.already_present)
                .count();
        }
    }

    /// Prepare grouped entries for a resume or failed-entry retry.
    pub fn prepare_resume(&mut self, retry_failed: bool) {
        if !self.independent_entries {
            return;
        }
        self.outcome = None;
        self.retry_indices = if retry_failed {
            Some(
                self.entries
                    .iter()
                    .filter(|entry| entry.state == BilibiliAttemptEntryState::Failed)
                    .map(|entry| entry.index)
                    .collect(),
            )
        } else {
            None
        };
        for entry in &mut self.entries {
            let reset = if retry_failed {
                entry.state == BilibiliAttemptEntryState::Failed
            } else {
                matches!(
                    entry.state,
                    BilibiliAttemptEntryState::Pending
                        | BilibiliAttemptEntryState::Failed
                        | BilibiliAttemptEntryState::Interrupted
                        | BilibiliAttemptEntryState::Downloading
                )
            };
            if reset {
                entry.state = BilibiliAttemptEntryState::Pending;
                entry.stage.clear();
                entry.already_present = false;
                entry.published_media_paths.clear();
                entry.published_media_hashes.clear();
            }
        }
        self.refresh_independent_skipped_entries();
    }

    /// Prepare one eligible grouped entry for a scoped retry while retaining other outcomes.
    pub fn prepare_episode_retry(&mut self, index: u32) -> bool {
        if !self.independent_entries {
            return false;
        }
        let Some(entry) = self.entries.iter_mut().find(|entry| entry.index == index) else {
            return false;
        };
        if !matches!(
            entry.state,
            BilibiliAttemptEntryState::Pending
                | BilibiliAttemptEntryState::Failed
                | BilibiliAttemptEntryState::Interrupted
        ) {
            return false;
        }
        self.outcome = None;
        self.retry_indices = Some(vec![index]);
        entry.state = BilibiliAttemptEntryState::Pending;
        entry.stage.clear();
        entry.already_present = false;
        entry.error = None;
        entry.published_media_paths.clear();
        entry.published_media_hashes.clear();
        self.refresh_independent_skipped_entries();
        true
    }

    /// Reconcile this grouped attempt with a newly approved inventory.
    ///
    /// Only compatible verified receipts survive. Other matching entries retain their
    /// attempt/error history, while their state and published outputs are reset.
    pub fn reconcile_independent_inventory(
        &mut self,
        title: String,
        media_profile: Option<String>,
        entries: impl IntoIterator<Item = (u32, String, String)>,
        preserved_verified_ids: &std::collections::BTreeSet<String>,
        requires_redownload_ids: &std::collections::BTreeSet<String>,
    ) {
        let mut previous_by_id = BTreeMap::<String, VecDeque<_>>::new();
        for entry in std::mem::take(&mut self.entries) {
            if entry.stable_media_id.is_empty() {
                continue;
            }
            previous_by_id
                .entry(entry.stable_media_id.clone())
                .or_default()
                .push_back(entry);
        }

        self.entries = entries
            .into_iter()
            .map(|(index, title, stable_media_id)| {
                let previous = previous_by_id
                    .get_mut(&stable_media_id)
                    .and_then(VecDeque::pop_front);
                let preserve_verified = previous.as_ref().is_some_and(|entry| {
                    entry.state == BilibiliAttemptEntryState::Verified
                        && !entry.requires_redownload
                        && preserved_verified_ids.contains(&stable_media_id)
                });
                let mut entry = BilibiliAttemptEntry {
                    index,
                    title,
                    state: BilibiliAttemptEntryState::Pending,
                    stage: String::new(),
                    already_present: false,
                    requires_redownload: requires_redownload_ids.contains(&stable_media_id),
                    attempts: 0,
                    error: None,
                    stable_media_id,
                    published_media_paths: Vec::new(),
                    published_media_hashes: BTreeMap::new(),
                };
                if let Some(previous) = previous {
                    entry.attempts = previous.attempts;
                    entry.error = previous.error;
                    entry.requires_redownload |= previous.requires_redownload;
                    if preserve_verified {
                        entry.state = BilibiliAttemptEntryState::Verified;
                        entry.already_present = previous.already_present;
                        entry.published_media_paths = previous.published_media_paths;
                        entry.published_media_hashes = previous.published_media_hashes;
                    }
                }
                entry
            })
            .collect();
        self.title = title;
        self.skipped_entries = self
            .entries
            .iter()
            .filter(|entry| entry.already_present)
            .count();
        self.outcome = None;
        self.retry_indices = None;
        self.independent_entries = true;
        self.media_profile = media_profile;
    }

    pub fn verified_entries(&self) -> usize {
        self.count(BilibiliAttemptEntryState::Verified)
    }

    pub fn sanitize_errors(&mut self) {
        for entry in &mut self.entries {
            if let Some(error) = entry.error.as_deref() {
                let sanitized = sanitize_entry_error(error);
                entry.error = Some(sanitized);
            }
        }
    }

    pub fn finish(&mut self, mut outcome: BilibiliAttemptOutcome) {
        if self.outcome.is_some() {
            return;
        }

        if self.independent_entries
            && outcome == BilibiliAttemptOutcome::Verified
            && self
                .entries
                .iter()
                .any(|entry| entry.state != BilibiliAttemptEntryState::Verified)
        {
            outcome = if self.verified_entries() > 0 {
                BilibiliAttemptOutcome::PartiallyCompleted
            } else {
                BilibiliAttemptOutcome::Failed
            };
        }
        if self.independent_entries
            && outcome == BilibiliAttemptOutcome::Failed
            && self.verified_entries() > 0
        {
            outcome = BilibiliAttemptOutcome::PartiallyCompleted;
        }

        match outcome {
            BilibiliAttemptOutcome::Failed | BilibiliAttemptOutcome::PartiallyCompleted => {
                for entry in &mut self.entries {
                    if entry.state == BilibiliAttemptEntryState::Downloading {
                        entry.state = BilibiliAttemptEntryState::Failed;
                    }
                }
            }
            BilibiliAttemptOutcome::Cancelled | BilibiliAttemptOutcome::Interrupted => {
                for entry in &mut self.entries {
                    if entry.state == BilibiliAttemptEntryState::Downloading {
                        entry.state = BilibiliAttemptEntryState::Interrupted;
                    }
                }
            }
            BilibiliAttemptOutcome::Verified => {
                if !self.independent_entries {
                    for entry in &mut self.entries {
                        entry.state = BilibiliAttemptEntryState::Verified;
                        entry.stage.clear();
                    }
                }
            }
        }
        self.outcome = Some(outcome);
    }

    /// Number of selected entries in this attempt; skipped entries are separate.
    pub fn total_entries(&self) -> usize {
        self.entries.len()
    }

    /// Number of entries downloaded but still awaiting finalization or verification.
    pub fn downloaded_entries(&self) -> usize {
        self.count(BilibiliAttemptEntryState::Downloaded)
    }

    pub fn failed_entries(&self) -> usize {
        self.count(BilibiliAttemptEntryState::Failed)
    }

    pub fn summary(&self) -> String {
        let count = |state| self.count(state);
        let mut output = format!(
            "{}\nSelected: {}; skipped/already present: {}\nDownloaded pending mux/publication/verification: {}; verified: {}\nFailed: {}; not started: {}; interrupted: {}; running: {}",
            truncate_text(&single_line(&self.title), 240),
            self.total_entries(),
            self.skipped_entries,
            self.downloaded_entries(),
            count(BilibiliAttemptEntryState::Verified),
            count(BilibiliAttemptEntryState::Failed),
            count(BilibiliAttemptEntryState::Pending),
            count(BilibiliAttemptEntryState::Interrupted),
            count(BilibiliAttemptEntryState::Downloading),
        );
        if let Some(outcome) = self.outcome {
            let label = match outcome {
                BilibiliAttemptOutcome::Failed => "failed",
                BilibiliAttemptOutcome::PartiallyCompleted => "partially completed",
                BilibiliAttemptOutcome::Cancelled => "cancelled",
                BilibiliAttemptOutcome::Interrupted => "interrupted",
                BilibiliAttemptOutcome::Verified => "verified",
            };
            output.push_str("\nOutcome: ");
            output.push_str(label);
        }

        for state in [
            BilibiliAttemptEntryState::Failed,
            BilibiliAttemptEntryState::Interrupted,
        ] {
            let entries: Vec<_> = self
                .entries
                .iter()
                .filter(|entry| entry.state == state)
                .collect();
            if entries.is_empty() || output.len() >= SUMMARY_MAX_BYTES {
                continue;
            }

            let label = if state == BilibiliAttemptEntryState::Failed {
                "Failed"
            } else {
                "Interrupted"
            };
            let ranges = compact_ranges(entries.iter().map(|entry| entry.index));
            let shown_ranges = ranges
                .iter()
                .take(MAX_DETAIL_RANGES)
                .cloned()
                .collect::<Vec<_>>();
            let omitted_ranges = ranges.len().saturating_sub(shown_ranges.len());
            let mut section = format!("\n{label} episodes: {}", shown_ranges.join(", "));
            if omitted_ranges > 0 {
                section.push_str(&format!(" (+{omitted_ranges} ranges)"));
            }
            append_bounded(&mut output, &section);

            for entry in entries.into_iter().take(MAX_DETAIL_ENTRIES) {
                let title = single_line(&entry.title);
                let mut detail = if entry.stage.is_empty() {
                    format!("\nEpisode {}: {}", entry.index, title)
                } else {
                    format!(
                        "\nEpisode {}: {} (last reported stage: {})",
                        entry.index,
                        title,
                        single_line(&entry.stage)
                    )
                };
                if entry.attempts > 0 {
                    detail.push_str(&format!(" (attempts: {})", entry.attempts));
                }
                if let Some(error) = &entry.error {
                    detail.push_str(&format!(
                        " (last error: {})",
                        single_line(&sanitize_entry_error(error))
                    ));
                }
                append_bounded(&mut output, &detail);
            }
        }

        output
    }

    fn count(&self, state: BilibiliAttemptEntryState) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.state == state)
            .count()
    }
}

fn sanitize_entry_error(error: &str) -> String {
    let redacted = redact_sensitive_text(error);
    const MAX_ERROR_BYTES: usize = 1_000;
    if redacted.len() <= MAX_ERROR_BYTES {
        return redacted;
    }
    let mut end = MAX_ERROR_BYTES.saturating_sub(3);
    while !redacted.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &redacted[..end])
}

fn compact_ranges(indices: impl IntoIterator<Item = u32>) -> Vec<String> {
    let mut indices: Vec<_> = indices.into_iter().collect();
    indices.sort_unstable();
    indices.dedup();

    let mut ranges = Vec::new();
    let mut iter = indices.into_iter();
    let Some(mut start) = iter.next() else {
        return ranges;
    };
    let mut end = start;
    for index in iter {
        if end.checked_add(1) == Some(index) {
            end = index;
        } else {
            ranges.push(format_range(start, end));
            start = index;
            end = index;
        }
    }
    ranges.push(format_range(start, end));
    ranges
}

fn format_range(start: u32, end: u32) -> String {
    if start == end {
        start.to_string()
    } else {
        format!("{start}-{end}")
    }
}

fn single_line(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split_whitespace()
        .map(|word| {
            if word.contains("://") || word.starts_with("www.") {
                "[link]"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes.saturating_sub(3);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &value[..end])
}

fn append_bounded(output: &mut String, addition: &str) {
    if output.len().saturating_add(addition.len()) <= SUMMARY_MAX_BYTES {
        output.push_str(addition);
        return;
    }

    let remaining = SUMMARY_MAX_BYTES.saturating_sub(output.len());
    if remaining <= 3 {
        return;
    }
    let mut end = remaining - 3;
    while !addition.is_char_boundary(end) {
        end -= 1;
    }
    if end > 0 {
        output.push_str(&addition[..end]);
        output.push_str("...");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(indices: impl IntoIterator<Item = u32>) -> BilibiliAttemptSnapshot {
        BilibiliAttemptSnapshot::new(
            "Season".to_string(),
            indices
                .into_iter()
                .map(|index| (index, format!("Episode {index}"))),
            0,
        )
    }

    #[test]
    fn partial_attempt_reports_sixteen_successes_and_episode_seventeen_failure() {
        let mut attempt = snapshot(1..=17);
        for index in 1..=16 {
            attempt.entry_started(index);
            attempt.entry_completed(index);
        }
        attempt.entry_started(17);
        attempt.stage_started(17, "audio".to_string());
        attempt.entry_failed(17);

        assert_eq!(attempt.downloaded_entries(), 16);
        assert_eq!(attempt.failed_entries(), 1);
        let summary = attempt.summary();
        assert!(summary.contains("Downloaded pending mux/publication/verification: 16"));
        assert!(summary.contains("\nFailed episodes: 17"));
        assert!(summary.contains("Episode 17: Episode 17 (last reported stage: audio)"));
    }

    #[test]
    fn retry_after_entry_failure_clears_failed_count() {
        let mut attempt = snapshot([17]);
        attempt.entry_started(17);
        attempt.stage_started(17, "video".to_string());
        attempt.entry_failed(17);
        attempt.entry_started(17);
        attempt.stage_started(17, "audio".to_string());
        attempt.entry_completed(17);

        assert_eq!(attempt.failed_entries(), 0);
        assert_eq!(attempt.downloaded_entries(), 1);
        assert!(attempt.summary().contains("Failed: 0;"));
    }

    #[test]
    fn sparse_indices_are_not_filled_in_when_ranges_are_rendered() {
        let mut attempt = snapshot([1, 2, 5, 7, 8]);
        for index in [2, 5, 8] {
            attempt.entry_started(index);
            attempt.entry_failed(index);
        }

        assert!(attempt.summary().contains("\nFailed episodes: 2, 5, 8"));
        assert!(!attempt.summary().contains("2-8"));
        assert_eq!(attempt.total_entries(), 5);
    }

    #[test]
    fn duplicate_completion_is_idempotent() {
        let mut attempt = snapshot([1]);
        attempt.entry_started(1);
        attempt.entry_completed(1);
        attempt.entry_completed(1);

        assert_eq!(attempt.downloaded_entries(), 1);
        assert_eq!(attempt.failed_entries(), 0);
        assert!(
            attempt
                .summary()
                .contains("Downloaded pending mux/publication/verification: 1")
        );
    }

    #[test]
    fn duplicate_started_update_does_not_demote_downloaded_or_verified_entries() {
        let mut attempt = snapshot([1, 2]);
        attempt.entry_started(1);
        attempt.entry_completed(1);
        BilibiliAttemptUpdate::Started.apply(&mut attempt, 1);
        assert_eq!(attempt.downloaded_entries(), 1);

        attempt.finish(BilibiliAttemptOutcome::Verified);
        BilibiliAttemptUpdate::Started.apply(&mut attempt, 2);
        assert_eq!(attempt.count(BilibiliAttemptEntryState::Verified), 2);
    }

    #[test]
    fn update_events_delegate_to_snapshot_transitions() {
        let mut attempt = snapshot([17]);
        BilibiliAttemptUpdate::Started.apply(&mut attempt, 17);
        BilibiliAttemptUpdate::Stage {
            stage: "audio".to_string(),
        }
        .apply(&mut attempt, 17);
        assert_eq!(attempt.entries[0].stage, "audio");
        BilibiliAttemptUpdate::Completed.apply(&mut attempt, 17);

        assert_eq!(attempt.downloaded_entries(), 1);
    }

    #[test]
    fn independent_failure_keeps_the_last_reported_stage() {
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [(17, "Episode 17".into(), Some("ep-17".into()))],
            0,
        );
        BilibiliAttemptUpdate::Started.apply(&mut attempt, 17);
        BilibiliAttemptUpdate::Stage {
            stage: "audio".into(),
        }
        .apply(&mut attempt, 17);
        BilibiliAttemptUpdate::Failure {
            error: "audio request failed".into(),
            attempts: 1,
        }
        .apply(&mut attempt, 17);

        assert_eq!(attempt.entries[0].state, BilibiliAttemptEntryState::Failed);
        assert_eq!(attempt.entries[0].stage, "audio");
        assert!(
            attempt
                .summary()
                .contains("Episode 17: Episode 17 (last reported stage: audio)")
        );
    }

    #[test]
    fn independent_downloaded_entry_accepts_mux_stage_and_failure_keeps_it() {
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [(17, "Episode 17".into(), Some("ep-17".into()))],
            0,
        );
        BilibiliAttemptUpdate::Started.apply(&mut attempt, 17);
        BilibiliAttemptUpdate::Completed.apply(&mut attempt, 17);
        assert_eq!(
            attempt.entries[0].state,
            BilibiliAttemptEntryState::Downloaded
        );

        BilibiliAttemptUpdate::Stage {
            stage: "mux".into(),
        }
        .apply(&mut attempt, 17);
        assert_eq!(
            attempt.entries[0].state,
            BilibiliAttemptEntryState::Downloaded
        );
        assert_eq!(attempt.entries[0].stage, "mux");

        BilibiliAttemptUpdate::Failure {
            error: "mux failed".into(),
            attempts: 1,
        }
        .apply(&mut attempt, 17);
        assert_eq!(attempt.entries[0].state, BilibiliAttemptEntryState::Failed);
        assert_eq!(attempt.entries[0].stage, "mux");
    }

    #[test]
    fn already_present_event_tracks_skip_subset_and_verified_clears_it() {
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [(17, "Episode 17".into(), Some("ep-17".into()))],
            0,
        );
        let first_path = PathBuf::from("/downloads/episode-17.mp4");
        let first_hash = "a".repeat(64);
        let already_present = BilibiliAttemptUpdate::AlreadyPresent {
            media_paths: vec![first_path.clone()],
            hashes: BTreeMap::from([(first_path.display().to_string(), first_hash)]),
        };
        already_present.apply(&mut attempt, 17);
        already_present.apply(&mut attempt, 17);

        assert_eq!(
            attempt.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert!(attempt.entries[0].already_present);
        assert!(!attempt.entries[0].requires_redownload);
        assert_eq!(attempt.entries[0].attempts, 0);
        assert_eq!(attempt.skipped_entries, 1);

        let current_path = PathBuf::from("/downloads/episode-17-remuxed.mp4");
        BilibiliAttemptUpdate::Verified {
            media_paths: vec![current_path.clone()],
            hashes: BTreeMap::from([(current_path.display().to_string(), "b".repeat(64))]),
        }
        .apply(&mut attempt, 17);

        assert_eq!(
            attempt.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert!(!attempt.entries[0].already_present);
        assert_eq!(attempt.skipped_entries, 0);
        assert_eq!(attempt.entries[0].published_media_paths, vec![current_path]);
    }

    #[test]
    fn requires_redownload_survives_retry_failure_and_resume_until_verified() {
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".into(),
            [(17, "Episode 17".into(), Some("ep-17".into()))],
            0,
        );
        attempt.entries[0].requires_redownload = true;

        BilibiliAttemptUpdate::Started.apply(&mut attempt, 17);
        assert!(attempt.entries[0].requires_redownload);
        BilibiliAttemptUpdate::Retrying {
            attempt: 2,
            error: "retrying".into(),
        }
        .apply(&mut attempt, 17);
        assert!(attempt.entries[0].requires_redownload);
        BilibiliAttemptUpdate::Failure {
            error: "failed".into(),
            attempts: 2,
        }
        .apply(&mut attempt, 17);
        assert!(attempt.entries[0].requires_redownload);

        attempt.finish(BilibiliAttemptOutcome::Failed);
        attempt.prepare_resume(false);
        assert_eq!(attempt.entries[0].state, BilibiliAttemptEntryState::Pending);
        assert!(attempt.entries[0].requires_redownload);
        let serialized = serde_json::to_vec(&attempt).unwrap();
        attempt = serde_json::from_slice(&serialized).unwrap();
        assert!(attempt.entries[0].requires_redownload);

        BilibiliAttemptUpdate::AlreadyPresent {
            media_paths: vec![PathBuf::from("/downloads/episode-17.mp4")],
            hashes: BTreeMap::from([("/downloads/episode-17.mp4".into(), "a".repeat(64))]),
        }
        .apply(&mut attempt, 17);
        assert!(
            attempt.entries[0].requires_redownload,
            "an already-present event must not clear an invalid receipt marker"
        );

        BilibiliAttemptUpdate::Verified {
            media_paths: vec![PathBuf::from("/downloads/episode-17.mp4")],
            hashes: BTreeMap::from([("/downloads/episode-17.mp4".into(), "b".repeat(64))]),
        }
        .apply(&mut attempt, 17);
        assert!(!attempt.entries[0].requires_redownload);
    }

    #[test]
    fn legacy_downloaded_entry_keeps_ignoring_late_stage_updates() {
        let mut attempt = snapshot([17]);
        BilibiliAttemptUpdate::Started.apply(&mut attempt, 17);
        BilibiliAttemptUpdate::Completed.apply(&mut attempt, 17);

        BilibiliAttemptUpdate::Stage {
            stage: "mux".into(),
        }
        .apply(&mut attempt, 17);
        assert_eq!(
            attempt.entries[0].state,
            BilibiliAttemptEntryState::Downloaded
        );
        assert!(attempt.entries[0].stage.is_empty());

        BilibiliAttemptUpdate::Failure {
            error: "mux failed".into(),
            attempts: 1,
        }
        .apply(&mut attempt, 17);
        assert_eq!(attempt.entries[0].state, BilibiliAttemptEntryState::Failed);
        assert!(attempt.entries[0].stage.is_empty());
    }

    #[test]
    fn finalization_failure_keeps_downloaded_entries_unverified() {
        let mut attempt = snapshot([1, 2]);
        attempt.entry_started(1);
        attempt.entry_completed(1);
        attempt.entry_started(2);
        attempt.entry_completed(2);
        attempt.finish(BilibiliAttemptOutcome::Failed);

        assert_eq!(attempt.downloaded_entries(), 2);
        assert_eq!(attempt.failed_entries(), 0);
        assert!(
            attempt
                .summary()
                .contains("Downloaded pending mux/publication/verification: 2")
        );
        assert!(attempt.summary().contains("verified: 0"));
        assert!(attempt.summary().contains("Outcome: failed"));
    }

    #[test]
    fn cancellation_marks_active_entries_interrupted_without_false_failures() {
        let mut attempt = snapshot([1, 2]);
        attempt.entry_started(1);
        attempt.entry_completed(1);
        attempt.entry_started(2);
        attempt.stage_started(2, "danmaku".to_string());
        attempt.finish(BilibiliAttemptOutcome::Cancelled);

        assert_eq!(attempt.failed_entries(), 0);
        assert_eq!(attempt.downloaded_entries(), 1);
        assert!(attempt.summary().contains("interrupted: 1"));
        assert!(
            attempt
                .summary()
                .contains("Episode 2: Episode 2 (last reported stage: danmaku)")
        );
    }

    #[test]
    fn verified_terminal_marks_entries_verified_and_ignores_later_updates() {
        let mut attempt = snapshot([1, 2]);
        attempt.entry_started(1);
        attempt.entry_completed(1);
        attempt.finish(BilibiliAttemptOutcome::Verified);
        attempt.entry_failed(2);

        assert_eq!(attempt.downloaded_entries(), 0);
        assert_eq!(attempt.failed_entries(), 0);
        assert_eq!(attempt.count(BilibiliAttemptEntryState::Verified), 2);
        assert!(attempt.summary().contains("verified: 2"));
        assert!(attempt.summary().contains("Outcome: verified"));
    }

    #[test]
    fn grouped_resume_and_failed_retry_preserve_verified_entries_and_error_history() {
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".to_string(),
            [
                (1, "Episode 1".to_string(), Some("ep-1".to_string())),
                (2, "Episode 2".to_string(), Some("ep-2".to_string())),
                (3, "Episode 3".to_string(), Some("ep-3".to_string())),
            ],
            0,
        );
        attempt.entry_verified_with_outputs(
            1,
            vec![PathBuf::from("/downloads/episode-1.mp4")],
            BTreeMap::from([("/downloads/episode-1.mp4".to_string(), "a".repeat(64))]),
        );
        attempt.entry_failed_with_error(2, 2, "first failure".to_string());
        attempt.finish(BilibiliAttemptOutcome::Failed);

        assert_eq!(
            attempt.outcome,
            Some(BilibiliAttemptOutcome::PartiallyCompleted)
        );
        let summary = attempt.summary();
        assert!(summary.contains("Outcome: partially completed"));
        assert!(summary.contains("Failed episodes: 2"));
        assert!(summary.contains("attempts: 2"));
        attempt.prepare_resume(true);
        assert_eq!(
            attempt.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(attempt.entries[1].state, BilibiliAttemptEntryState::Pending);
        assert_eq!(attempt.entries[1].attempts, 2);
        assert_eq!(attempt.entries[1].error.as_deref(), Some("first failure"));
        assert_eq!(attempt.entries[2].state, BilibiliAttemptEntryState::Pending);
        assert_eq!(attempt.retry_indices, Some(vec![2]));
        assert_eq!(attempt.outcome, None);

        attempt.entries[2].state = BilibiliAttemptEntryState::Downloading;
        attempt.prepare_resume(false);
        assert_eq!(
            attempt.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(attempt.entries[1].state, BilibiliAttemptEntryState::Pending);
        assert_eq!(attempt.outcome, None);
        assert_eq!(attempt.retry_indices, None);
        assert_eq!(attempt.entries[2].state, BilibiliAttemptEntryState::Pending);
    }

    #[test]
    fn grouped_verified_terminal_does_not_promote_unverified_entries() {
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".to_string(),
            [
                (1, "Episode 1".to_string(), Some("ep-1".to_string())),
                (2, "Episode 2".to_string(), Some("ep-2".to_string())),
            ],
            0,
        );
        attempt.entry_verified(1);
        attempt.finish(BilibiliAttemptOutcome::Verified);

        assert_eq!(
            attempt.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(attempt.entries[1].state, BilibiliAttemptEntryState::Pending);
        assert_eq!(
            attempt.outcome,
            Some(BilibiliAttemptOutcome::PartiallyCompleted)
        );
    }

    #[test]
    fn grouped_failed_retry_scope_excludes_pending_and_interrupted_entries() {
        let mut attempt = BilibiliAttemptSnapshot::new_independent(
            "Season".to_string(),
            [
                (1, "Episode 1".to_string(), Some("ep-1".to_string())),
                (2, "Episode 2".to_string(), Some("ep-2".to_string())),
                (3, "Episode 3".to_string(), Some("ep-3".to_string())),
                (4, "Episode 4".to_string(), Some("ep-4".to_string())),
            ],
            0,
        );
        attempt.entry_verified(1);
        attempt.entry_failed_with_error(2, 2, "failed".into());
        attempt.entries[3].state = BilibiliAttemptEntryState::Interrupted;
        attempt.finish(BilibiliAttemptOutcome::PartiallyCompleted);

        attempt.prepare_resume(true);
        assert_eq!(attempt.retry_indices, Some(vec![2]));
        assert_eq!(
            attempt.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(attempt.entries[1].state, BilibiliAttemptEntryState::Pending);
        assert_eq!(attempt.entries[2].state, BilibiliAttemptEntryState::Pending);
        assert_eq!(
            attempt.entries[3].state,
            BilibiliAttemptEntryState::Interrupted
        );

        attempt.prepare_resume(false);
        assert_eq!(attempt.retry_indices, None);
        assert_eq!(
            attempt.entries[0].state,
            BilibiliAttemptEntryState::Verified
        );
        assert_eq!(attempt.entries[3].state, BilibiliAttemptEntryState::Pending);
    }

    #[test]
    fn legacy_snapshot_defaults_group_fields_and_entry_receipts() {
        let value = serde_json::json!({
            "title": "Season",
            "entries": [{
                "index": 1,
                "title": "Episode 1",
                "state": "Pending",
                "stage": ""
            }],
            "skipped_entries": 0,
            "outcome": null
        });
        let attempt: BilibiliAttemptSnapshot = serde_json::from_value(value).unwrap();

        assert!(!attempt.independent_entries);
        assert_eq!(attempt.retry_indices, None);
        assert!(!attempt.entries[0].already_present);
        assert!(!attempt.entries[0].requires_redownload);
        assert_eq!(attempt.entries[0].attempts, 0);
        assert_eq!(attempt.entries[0].error, None);
        assert!(attempt.entries[0].stable_media_id.is_empty());
        assert!(attempt.entries[0].published_media_paths.is_empty());
        assert!(attempt.entries[0].published_media_hashes.is_empty());
    }
}
