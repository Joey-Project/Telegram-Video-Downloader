---
id: 20261010-bsd01
title: Skip Existing Episodes in Full Seasons
status: completed
created: 2026-10-10
updated: 2026-10-10
branch: codex/skip-season-duplicates
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/34
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
- Queue plan inspection and execution share the same filtering helper, including
  audio-only media. Conflicting CID/EPID evidence across associated sidecars
  prevents skipping the ambiguous file.
- Filtering rereads current sidecars and uses bounded stream probes for the
  requested audio/video mode. Artifact-only modes cannot skip from video evidence.
- The worker compares its lock-protected filtered plan with the queued snapshot
  before downloading. A changed plan stops with retry/requeue guidance so the
  queue can validate the new plan.
- The shared snapshot helper retains the current queue metadata preview and
  bandwidth-based size estimates after integrating the updated target branch.
- An entirely existing season returns existing media through the normal
  completed-output path. No existing media is overwritten or deleted.

## Validation

- Regression coverage includes partial matches, conflicting identities, all
  existing entries, callback labels, serialized skip choices, persisted queue
  recovery, and stale duplicate callbacks.
- A localhost-only mock makes an existing episode's play URL fail if requested:
  the skipped episode receives zero playback requests, the missing episode
  receives one, and an all-existing season produces a valid empty plan.
- `cargo test --all-targets --locked --offline --quiet -- --test-threads=4`:
  580 passed, 13 ignored, zero failures after target-branch integration.
  Coverage includes actual file
  removal/replacement, changed and newly conflicting sidecars, benign
  directory-entry churn, and real video/audio stream fixtures. Snapshot mismatch
  rejection and worker IPC propagation are tested directly; a full subprocess
  download race was not exercised.
- An initial parallel run hit an existing local Telegram progress timeout; the
  exact test passed alone, and the complete four-thread run passed.
- Strict Clippy and formatting passed with Cargo/rustc 1.95.0. Queue and HTTP
  tests ran outside the sandbox; compilation used an isolated temporary target.
- The isolated release build passed. The staged executable was signed with the
  existing trusted certificate and identifier, and verified against the recorded
  designated requirement before installation.
