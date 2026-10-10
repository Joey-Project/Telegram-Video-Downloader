---
id: 20261010-bep01
title: Bounded Per-Episode Bangumi Planning
status: completed
created: 2026-10-10
updated: 2026-10-10
branch: codex/per-episode-planning
pr: null
supersedes: []
superseded_by:
---

# Bounded Per-Episode Bangumi Planning

## Problem

- The 203-episode `md1376` season could download its latest episode, but choosing
  all episodes timed out twice: duplicate probing and actual download planning.
- Both probes wrapped a serial SDK plan of every episode in a 60-second total
  deadline. Runtime logs show independent minute-long failures at 08:13:35 and
  08:14:36 UTC on 2026-10-10.

## Implementation

- Duplicate detection resolves selected metadata without requesting stream URLs.
  Exact single-entry overwrite checks still require a unique CID or episode ID;
  unknown zero IDs never become duplicate identities.
- Season planning uses at most two concurrent tasks with an independent timeout
  for each selected episode. Completed plans retain inventory order and original
  episode indices; refreshed episode identities must match the inventory.
- Each completion updates planning progress. Failure or cancellation aborts the
  other in-flight tasks. The existing download-plan consumer and media integrity
  verification remain in use.
- The pinned SDK does not expose planning from already resolved episode metadata,
  so each single-episode plan also refreshes its season metadata. NFO generation
  reuses the initial selected metadata instead of launching another parallel query.

## Validation

- Regression tests exercise virtual-time season planning beyond one timeout
  interval, the two-task ceiling, out-of-order completion, per-episode timeout
  reporting, failure/parent cancellation, selected metadata identity mapping,
  and refreshed episode identity checks.
- `cargo test --all-targets --locked --offline --quiet`: 560 passed, 13 ignored,
  zero failures. Localhost and File Provider tests require non-sandbox execution;
  the initial sandbox pass could not perform those operations.
- Strict Clippy, format checks, project-journal validation, and `git diff --check`
  passed with Cargo/rustc 1.95.0.
- Built the release into an isolated temporary target and verified its signature
  against the existing trusted certificate and executable identifier before
  installation. The canonical release was not used as a Cargo output path.
