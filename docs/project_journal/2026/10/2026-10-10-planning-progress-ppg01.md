---
id: 20261010-ppg01
title: Progress During Video Plan Inspection
status: completed
created: 2026-10-10
updated: 2026-10-10
branch: codex/planning-progress
pr:
supersedes: []
superseded_by:
---

# Progress During Video Plan Inspection

## Problem

The full-season skip test at 20:40 UTC spent approximately 170 seconds
planning 202 missing episodes after `Started job`, without progress updates.
Worker planning then reported counts before downloading. Users could not
distinguish the first planning pass from a stalled download.

## Implementation

- Cover metadata and existing-media inspection before queueing too. Checks
  that finish before the configured interval do not add a progress notice.
- Start the existing progress forwarder before queue plan inspection and keep
  the same channel, status message, and configured update timer through download.
- Report metadata resolution, existing-media checks, planned episode counts,
  and elapsed planning time. Keep independent episode deadlines and concurrency
  of two, snapshot validation, cancellation, and worker validation.
- Drain preflight progress before cancellation, failure, or plan confirmation
  so a pending edit cannot overwrite the subsequent terminal or paused message.
- Persist a newly created fallback progress message and edit it thereafter.
  Reuse it through queueing and startup. Defer a replacement after an edit
  failure to a later progress update. Restart the periodic update deadline
  after API completion so slow responses cannot cause adjacent catch-up edits.
- Carry replacement delivery state through ordinary-video plan confirmation,
  cancellation, and terminal results. If the last edit failed before a deferred
  replacement was sent, the subsequent result can create a new status message.

## Validation

- Rust 1.95.0: all-target tests passed (601 passed, 15 ignored), strict
  Clippy passed with warnings denied, and the isolated release build passed.
- Formatting and whitespace checks passed. Tests cover message reuse, throttling,
  deferred replacement, cancellation, and terminal status ordering.
- The release was built outside the live executable path, and the staged
  binary satisfies the original trusted signing requirement.
