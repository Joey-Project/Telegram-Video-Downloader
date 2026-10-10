---
id: 20261010-qpm01
title: Queue Metadata Before Download Planning
status: completed
created: 2026-10-10
updated: 2026-10-10
branch: wip/queue-metadata
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/35
supersedes: []
superseded_by:
---

# Queue Metadata Before Download Planning

## Problem

- `/queue` previously displayed only accepted or proposed download plans. Tasks
  awaiting duplicate choices had generic titles after restart, and stream-plan
  failures discarded otherwise available titles.
- Stream APIs can omit exact sizes, and the per-episode planner previously hid
  API denial codes behind a generic planning failure.

## Behavior

- Persist display metadata from duplicate resolution before prompting or queuing.
  A single resolved duplicate gets a best-effort stream preview with a five-second
  budget; multi-entry inventories remain metadata-only.
- Keep display metadata separate from accepted/proposed plans. Generation and
  status checks reject stale writes; changing the request clears its preview.
  Execution still resolves and validates the current plan.
- If planning fails without cached details, attempt a bounded title-only lookup.
  Its five-second budget includes credential synchronization and semaphore waits,
  so a blocked fallback cannot hide the original failure or keep the task running.
  Queue rows show the available title and a concise failure/interruption reason.
  API denial codes/messages remain redacted and bounded; HTTP/IO details stay
  generic.
- Prefer exact stream sizes, otherwise estimate from positive bitrate and duration
  using checked arithmetic. Estimates are marked `about`; missing or overflowing
  inputs remain unknown.
- Allocate detail-message space from the remaining Telegram text budget so errors
  cannot remove task action buttons. Old queue records remain readable; Resume or
  Retry refreshes records that never saved metadata.

## Validation

- Eight new regressions cover duplicate preview persistence through restart,
  denied preview and failed plan title retention, stale writes and request changes,
  legacy records, long error pages, bitrate estimate boundaries, and per-episode
  API denial reporting, and fallback timeout while credential synchronization waits.
- `cargo test --all-targets --locked --offline --quiet`: 568 passed, 13 ignored,
  zero failures. Localhost and macOS File Provider fixtures ran outside the sandbox.
- `cargo build --locked --offline`, strict Clippy, and format checks passed with
  Cargo/rustc 1.95.0. Build targets were isolated under a task-scoped temporary
  directory; the live executable's designated requirement was recorded first.
- README documents preview semantics, estimated sizes, and refresh behavior for
  old tasks. Project-journal and whitespace checks accompany the signed commit.
