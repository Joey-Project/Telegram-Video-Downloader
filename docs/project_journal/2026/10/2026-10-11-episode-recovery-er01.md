---
id: 20261011-er01
title: Independent Bilibili Episode Recovery
status: completed
created: 2026-10-11
updated: 2026-10-11
branch: wip/bilibili-episode-recovery
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/38
supersedes: []
superseded_by:
---

# Independent Bilibili Episode Recovery

## Problem

A terminal episode error stopped an entire season, and successful downloads
were not independently published. A later retry repeated work and did not
offer a way to select failed episodes.

## Scope

- Retain one group in the queue, with persistent per-episode execution state,
  sparse original indices, stable media identities, and verified output receipts.
- Resolve group inventory before execution; obtain a fresh play plan per episode
  rather than requiring every play URL before the first download.
- Publish verified episodes individually. Retry transient episode failures at
  most twice beyond the initial episode attempt, then continue later episodes.
  Shared authentication, storage, permission, and identity failures interrupt
  the group for explicit resume.
- Reuse the existing progress message and configured update interval. Report
  partial completion and offer group or single-episode retry through paginated
  episode and failure views.
- Revalidate successful output content and media identities before reuse.
  Persist invalid receipt markers across scoped retries until redownload is
  verified. Keep legacy attempts compatible and reject stale generation callbacks.
- Preserve legacy execution for artifact-only All selections.
- Reconcile approved inventory changes with saved episode state, invalidate
  receipts when media requirements change, and recover published outputs after
  interrupted completion bookkeeping. Reuse one existing-media index per run.

## Validation

- Rust 1.95.0: all-target offline tests passed with 673 passed and 15 existing
  opt-in tests ignored. Offline build, all-target Clippy with warnings denied,
  formatting, diff checks, and project journal validation passed.
- Regression tests exercise the actual episode driver through injected child
  executors: bounded 403 retries followed by continuation, publication before
  the next entry, failed-only and single-entry scopes, invalid receipt recovery
  across two resumes, and cancellation preserving verified outputs.
- Recovery regressions cover one shared index with new publication evidence,
  approved inventory growth and renumbering, media requirement changes,
  interrupted All publications, single-entry URLs, and zero-success failures.
- State and mock Telegram tests cover independent completion counts, restart,
  error retention, progress-channel closure, paginated episode views, message
  reuse, stale generation callbacks, and cross-chat action rejection.
- Builds use an isolated temporary Cargo target with debug information and
  incremental compilation disabled. This workstream does not deploy or restart
  the canonical macOS LaunchAgent.
