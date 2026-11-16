---
id: 20261116-drf01
title: Append-only danmaku refresh for existing library videos
status: completed
created: 2026-11-16
updated: 2026-11-16
branch: "codex/danmaku-refresh"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/76"
supersedes: []
superseded_by: ""
---

# Append-only danmaku refresh for existing library videos

## Agreed scope

- Add an explicit action to the `/file` Mini App for selected videos, selected
  collection members, or the current registered media library.
- Preview eligible videos and skipped items with reasons, then require explicit
  confirmation. A cancelled preview writes no media or sidecars.
- Fetch only danmaku; preserve comments already saved locally even if the current
  remote response omits them. Reuse the pinned BBDown-rust preservation and
  conversion APIs rather than downloading or transcoding media again.
- Derive the exact comment CID from recorded source identities. Do not interpret
  a collection part ordinal, title or ambiguous filename as a verified CID.
- Preserve incompatible or unreadable old sidecars and explain why they cannot
  be refreshed safely. Regenerated ASS retains the configured presentation.

## Implementation boundaries

- Cloud accepts owner-authenticated, strictly validated `danmaku_refresh`
  preview, confirm and cancel operations. All scanning, fetching, merging,
  validation and filesystem writes run on the Mac service.
- Reuse the durable inbox preview lifecycle, expiration, exact preview sequence,
  revision and library/root identity. Keep local content and identity proofs out
  of Cloud snapshots. No background polling or automatic library-wide refresh.
- Preserve media object identity and bytes. Revalidate captured sidecar objects
  and content before publication; a provider materialization or unrelated child
  directory change alone is not evidence of content mutation or replacement.
- Keep the actual worker's library I/O lease until all work ends, including when
  its async caller is cancelled. Recheck the cloud epoch and registered library
  binding after network waits before publishing sidecars.
- Reuse BBDown-rust's public bounded decoder and pure ASS renderer, introduced
  by [BBDown-rust #125](https://github.com/Joey-Project/BBDown-rust/pull/125).
- Preserve the configured ASS presentation and support repairing an interrupted
  XML-first publication. Unknown extra ASS comments remain protected.

## Validation

- Mock Telegram/inbox/Bilibili E2E covers preview, confirm, cancellation, repeated
  updates, restart and replay, local-only comments, mixed-library eligibility,
  stale proofs and unchanged media.
- Mini App app-shell E2E covers selection and library scope, review/confirm and
  cancel actions, durable reload, results and failure states.
- Additive D1 migration coverage preserves existing requests and high-water
  sequence values. Existing Rust and Cloud CI commands discover the new tests.
- The focused native suite passes all 42 cases, including mock Telegram replay,
  restart, cancellation, stale content and real XML-first interruption/retry.
  Native converter workspace coverage passes 27 unit and 8 E2E cases.
- Cloud validation passes 442 Worker/helper cases, 112 Mini App cases and 4 Widget
  regressions, plus typechecking and a Worker bundle dry run.
- Formatting, strict workspace Clippy, diff and project-journal checks pass.
  The existing Rust and Cloud CI workflows discover the added tests and retain
  their full workspace, service-helper and test-runtime validation.

## Delivery boundaries

- Root-bound local tests use temporary fixtures and mock Telegram/Bilibili data.
  No production media refresh or service deployment is needed for acceptance.
- Settings login, legacy attachment interoperability and core metadata-query
  limits are separate follow-up workstreams.
