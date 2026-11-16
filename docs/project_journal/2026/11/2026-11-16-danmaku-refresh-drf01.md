---
id: 20261116-drf01
title: Append-only danmaku refresh for existing library videos
status: active
created: 2026-11-16
updated: 2026-11-16
branch: "codex/danmaku-refresh"
pr: ""
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

## Validation plan

- Mock Telegram/inbox/Bilibili E2E covers preview, confirm, cancellation, repeated
  updates, restart and replay, local-only comments, mixed-library eligibility,
  stale proofs and unchanged media.
- Mini App app-shell E2E covers selection and library scope, review/confirm and
  cancel actions, durable reload, results and failure states.
- Additive D1 migration coverage preserves existing requests and high-water
  sequence values. Existing Rust and Cloud CI commands discover the new tests.
- Formatting, strict workspace Clippy and relevant Rust/Cloud tests are required
  before a signed landing commit and current-head remote review.

## Next steps

- Complete implementation and the validation described above.
- Deliver this work as the first PR, before settings login, legacy attachment
  interoperability, and core metadata-query resource limits in separate PRs.
