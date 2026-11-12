---
id: 20261112-mat01
title: Bounded Music automation waits on busy Macs
status: active
created: 2026-11-12
updated: 2026-11-12
branch: codex/music-automation-timeouts
pr: ""
supersedes: []
superseded_by: ""
---

# Bounded Music automation waits on busy Macs

## Problem and agreed scope

Production Music consent was granted after the automatic-import deployment. The
immediate Music query exceeded its fixed seven-second deadline, and subsequent
preflight checks exceeded the five-second system-mute deadline. The user reported
high host load and requested longer waits. The production bot remained online;
accepted-track catalog handover had not started and no tracks were added again.

## Behavior

- Use bounded configurable waits for Music queries and system-mute inspection,
  with longer defaults suitable for a busy host: 180 and 60 seconds respectively.
- Reject unbounded or invalid overrides by falling back to safe defaults; keep a
  five-minute upper limit and align script, process and wrapper deadlines.
- Preserve the indefinite wait for the user's explicit consent decision, the
  single outstanding operation rule, child cleanup and bounded output.
- Keep Music work off the Telegram receiver and retain existing add-intent,
  persistent-ID and uncertain-outcome recovery rules.

## Validation and remaining acceptance

All 63 targeted Music/config/mock Telegram tests passed. Coverage includes valid,
out-of-range and overflowing integer timeout settings without mutating process-global
environment, plus aligned script and supervisor deadlines. After the signed update,
verify actual Music responses from the fixed production identity on the loaded
host, then complete query-only handover of the three previously accepted tracks.
Keep private native results and deployment/rollback evidence outside tracked docs.

Strict Clippy passed with and without `test-runtime`; formatting, whitespace and
project-journal validation passed. Local Cargo execution used one build job to
limit additional host load.
