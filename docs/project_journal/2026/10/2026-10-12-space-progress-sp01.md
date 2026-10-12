---
id: 20261012-sp01
title: Disk Space Recovery and Season Progress
status: completed
created: 2026-10-12
updated: 2026-10-12
branch: wip/bilibili-space-progress
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/39
supersedes: []
superseded_by:
---

# Disk Space Recovery and Season Progress

## Evidence

- The running season had 203 entries: three verified, episode four downloading,
  and 199 pending. Its live message instead displayed the child's one-entry
  summary and raw ANSI tracing output.
- Logs recorded ENOSPC on 2026-10-11 at 21:13 and 21:39 UTC. The worker exited,
  but queue reads required index writes and terminal-state persistence failed.
  The saved task therefore remained Running without a Resume action.

## Changes

- Keep the group summary in live progress, distinguish current-episode media
  details, include long episode titles, and remove internal logs and ANSI codes.
- Query unprivileged capacity through the bound output filesystem descriptor.
  Check download/mux peak space, actual mux input sizes, and publication state
  overhead. Monitor the 256 MiB margin while the worker runs.
- Keep capacity monitoring active independently of file-activity tracking.
  Cap safety polling separately from Telegram progress delivery intervals.
  Apply the publication reserve only to resumable Bilibili jobs.
- Treat ENOSPC and quota exhaustion as shared storage failures. Preserve existing
  files and retry saving the pause after space is freed, with an explicit notice
  while persistence remains incomplete.
  Recognize serialized Darwin quota errors (`Disc quota exceeded`, errno 69)
  as well as Linux quota errors after a child worker reports them through stderr.
- Avoid unchanged queue index writes during ordinary lookup and listing.
- Retry transient terminal-state lookup errors, but return permanently invalid
  task records instead of repeatedly polling them.
  Apply the same classification when saving a superseding cancellation.

## Validation

- Rust 1.95.0: all-target offline tests passed, 756 passed, zero failed, and
  15 existing opt-in tests ignored. Capacity thresholds and failure paths use
  injected values and mock writes; no disk-filling test was performed.
- Offline build, all-target Clippy with warnings denied, formatting, diff, and
  project journal checks passed. Builds used an isolated temporary target with
  debug information and incremental compilation disabled.
- The initial sandbox run could not bind mock localhost APIs or coordinate
  macOS queue access. The full run with host permissions passed after updating
  existing failure-injection tests to target mutations rather than queue reads.
- Ordinary queue reads avoid unchanged index writes. Guarded terminal updates
  honor cancellation atomically, while legacy helper semantics remain intact.
- A normal restart of the existing signed PR #38 binary recovered the affected
  task to Interrupted: three verified, one interrupted, and 199 pending. No new
  media download was started and no user media was deleted.
