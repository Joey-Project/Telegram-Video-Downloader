use serde::{Deserialize, Serialize};

/// The terminal result of a Bilibili download attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BilibiliAttemptOutcome {
    Failed,
    Cancelled,
    Interrupted,
    Verified,
}

/// A bounded per-entry progress update applied to an existing snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BilibiliAttemptUpdate {
    Started,
    Stage { stage: String },
    Completed,
    Failed,
}

impl BilibiliAttemptUpdate {
    pub fn apply(&self, snapshot: &mut BilibiliAttemptSnapshot, index: u32) {
        match self {
            Self::Started => snapshot.entry_started(index),
            Self::Stage { stage } => snapshot.stage_started(index, stage.clone()),
            Self::Completed => snapshot.entry_completed(index),
            Self::Failed => snapshot.entry_failed(index),
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
                })
                .collect(),
            skipped_entries,
            outcome: None,
        }
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
            }
        }
    }

    pub fn stage_started(&mut self, index: u32, stage: String) {
        if self.outcome.is_some() {
            return;
        }
        for entry in self.entries.iter_mut().filter(|entry| entry.index == index) {
            if matches!(
                entry.state,
                BilibiliAttemptEntryState::Pending | BilibiliAttemptEntryState::Downloading
            ) {
                entry.state = BilibiliAttemptEntryState::Downloading;
                entry.stage = stage.clone();
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
            }
        }
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
            }
        }
    }

    pub fn finish(&mut self, outcome: BilibiliAttemptOutcome) {
        if self.outcome.is_some() {
            return;
        }

        match outcome {
            BilibiliAttemptOutcome::Failed => {
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
                for entry in &mut self.entries {
                    entry.state = BilibiliAttemptEntryState::Verified;
                    entry.stage.clear();
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
                let detail = if entry.stage.is_empty() {
                    format!("\nEpisode {}: {}", entry.index, title)
                } else {
                    format!(
                        "\nEpisode {}: {} (last reported stage: {})",
                        entry.index,
                        title,
                        single_line(&entry.stage)
                    )
                };
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
}
