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
- Identity and stream evidence must belong to the same current media object;
  only that verified path is returned as an existing output. Sidecars are checked
  again after probing. The guard protects device/inode/type identity and current
  episode metadata, without claiming content stability or rejecting harmless
  File Provider timestamp changes.
- Stream probing reads an inherited descriptor for the held media object rather
  than reopening its pathname, selecting its number within the process limit.
  Existing-entry checks use at most two concurrent probes, close the held files
  after probing, and retain verified identity metadata. Collecting skipped
  outputs revalidates identities and sidecars without retaining one open file
  per episode or repeating stream probes.
- The final report repeats skipped-file identity and sidecar validation after
  downloading the remaining episodes, preventing a changed skipped file from
  being reported complete using stale planning evidence.
- The worker carries typed media and sidecar identities through IPC. The parent
  validates them after publication and around descriptor-bound hashing; hashing
  compares the expected media identity on the same descriptor it reads. Sidecar
  validation binds the current pathname after reading, rejecting atomic
  replacements even if the original descriptor remains readable.
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
- A localhost-only mock with controlled stream-probe output makes an existing
  episode's play URL fail if requested:
  the skipped episode receives zero playback requests, the missing episode
  receives one, and an all-existing season produces a valid empty plan.
- `cargo test --all-targets --locked --offline --quiet -- --test-threads=4`:
  587 passed, 15 ignored, zero failures after target-branch integration.
  Coverage includes actual file
  removal/replacement, changed and newly conflicting sidecars, benign
  directory-entry churn. The separate real video/audio stream fixture test is
  explicitly run with `--ignored`, including mixed identity/completeness
  evidence from separate files. Snapshot mismatch
  rejection and worker IPC propagation are tested directly; a full subprocess
  download race was not exercised.
- An initial parallel run hit an existing local Telegram progress timeout; the
  exact test passed alone, and the complete four-thread run passed.
- Hosted macOS CI lacked FFmpeg. The planning mock now uses a controlled probe;
  real media validation follows the repository's opt-in external-tool convention.
- Descriptor replacement coverage proves ffprobe reads the held original file
  even when its pathname is replaced. A paused-time scheduler test checks two
  concurrent probes, preserved order, and conservative timeout handling. The
  localhost mock counts three probes across partial/all-existing planning and
  verifies that final output collection does not probe those files again.
- An isolated child process with a 64-FD limit probes 200 matching entries while
  retaining lightweight evidence. This covers both dynamic descriptor selection
  and release of each media descriptor rather than whole-season accumulation.
- Final-report regression coverage accepts unchanged evidence and directory
  churn while rejecting missing/replaced media and missing, unreadable, or
  conflicting sidecars with distinct errors.
- Publication and hashing regression coverage preserves typed IPC evidence and
  rejects replaced media, replaced sidecars, and evidence outside the reported
  outputs while accepting harmless directory churn.
- The explicit real-media test passed separately (one test); both controlled
  probe scripts passed `bash -n` and ShellCheck.
- Strict Clippy and formatting passed with Cargo/rustc 1.95.0. Queue and HTTP
  tests ran outside the sandbox; compilation used an isolated temporary target.
- The isolated release build passed. The staged executable was signed with the
  existing trusted certificate and identifier, and verified against the recorded
  designated requirement before installation.
