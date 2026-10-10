---
id: 20261010-bsd01
title: Skip Existing Episodes in Full Seasons
status: completed
created: 2026-10-10
updated: 2026-10-10
branch: codex/skip-season-duplicates
pr: null
supersedes: []
superseded_by:
---

# Skip Existing Episodes in Full Seasons

## Problem

- At 17:01 UTC, selecting all episodes of `md1376` found the existing episode
  203. The duplicate prompt offered keep-both or cancellation; cancellation
  stopped the entire season rather than just its one duplicate.

## Implementation

- Full-season duplicate prompts count precise per-entry sidecar matches and
  offer `Skip duplicates`. The cancellation button reads `Cancel entire job`.
- The skip choice atomically updates the persisted job selection and queue
  status. Recovery retains the choice and rechecks current files.
- Under the existing output lock, execution resolves the season and filters
  exact existing entries before playback planning. Remaining episodes retain
  their original order and indices, with independent deadlines and concurrency
  of two. A coarse BV/AV match or filename alone cannot skip an episode.
- An entirely existing season returns existing media through the normal
  completed-output path. No existing media is overwritten or deleted.

## Validation

- Regression coverage includes partial matches, conflicting identities, all
  existing entries, callback labels, serialized skip choices, persisted queue
  recovery, and stale duplicate callbacks.
- `cargo test --all-targets --locked --offline --quiet`: 564 passed, 13 ignored,
  zero failures. The final run includes actual file removal/replacement and
  benign directory-entry churn.
- Strict Clippy and formatting passed with Cargo/rustc 1.95.0. Queue and HTTP
  tests ran outside the sandbox; compilation used an isolated temporary target.
- The isolated release build passed. The staged executable was signed with the
  existing trusted certificate and identifier, and verified against the recorded
  designated requirement before installation.
