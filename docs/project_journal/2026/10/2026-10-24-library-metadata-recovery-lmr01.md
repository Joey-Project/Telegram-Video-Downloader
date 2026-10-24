---
id: 20261024-lmr01
title: User-triggered library metadata recovery
status: completed
created: 2026-10-24
updated: 2026-10-24
branch: "codex/library-metadata-recovery"
pr: ""
supersedes: []
superseded_by: ""
---

# User-triggered library metadata recovery

## Accepted scope

- Manually analyze selected media or the current library using saved NFO, publication/queue identity hints, and bounded metadata-only source queries.
- Match multi-page Bilibili entries with BVID and CID, or exact episode IDs. Offer explicit candidates when saved identity is insufficient; retain reasons for unsupported, unavailable, ambiguous, and unchanged items.
- Display proposed title, source identity, collection, and ordering changes before writing. Apply through the existing root-bound metadata-only preview and confirmation pipeline.
- Fill missing metadata by default, preserve custom titles/collections and unknown NFO fields, and keep media, attachments, actual quality, and file locations intact.
- Let users explicitly mark an uncollected video as standalone and undo that declaration. This suppresses only the absent-collection organizing reason; incomplete metadata, low confidence, and unavailable/missing files still need attention.
- Keep heavy analysis on the local Rust service. Reuse durable commands/results and hibernating snapshot updates without introducing polling.

## Data and operational contract

- Standalone/unreviewed organization status lives in application-specific NFO metadata and is rebuilt by scans; no additional per-video JSON sidecar is introduced.
- Recovery previews are tied to the analysis revision and current library root binding. Existing media/NFO identity/content validation and atomic writes remain required.
- Library-wide analysis has bounded batches and explicit continuation so later candidates remain reachable even when an earlier batch has no applicable changes.
- Local, mocked provider and Telegram/cloud E2E coverage must exercise analysis through confirmation and rescan, including ambiguity, provider failure, stale reports, custom metadata preservation, organization round trips, and continuation.

## Validation

- Rust all-target suite: 1254 passed, 16 existing ignored; all-target Clippy with warnings denied, formatting, and isolated release build passed. A final recovery-focused run passed 68 tests after lint-only fixture/import corrections.
- Cloud suite: 317 Worker/helper/migration tests and 18 Mini App UI tests passed. Type/syntax checks and both default and production-configuration deployment dry-runs passed.
- New mock Telegram/provider E2E covers analysis, durable results across restart, preview, confirmation, rescan, standalone toggles, ambiguity/provider failures without writes, stale revision refusal, independent dispatch, and library continuation beyond the first100 items.
- Mini App tests exercise candidate choices, revision invalidation, explicit standalone previews, newer processing/failed intents, and a result applied before its acceptance receipt arrives. They wait for observable snapshot application rather than request start.
- D1 migration tests preserve exact historical rows and acknowledgment state, verify atomic rollback on injected failure, and retain high-water sequence values even after all rows are removed.
- Stabilized the existing authorization-expiry WebSocket test by allowing the initial promise cleanup to finish before delivering the invalidation under test. No timeout or parallelism changes were needed.
- Mock macOS service helpers passed101 Python tests and the LaunchAgent integration suite. Runtime/build logs and transient delivery state remain in ignored local operational records. These checks make no real provider queries and do not write production NFO files.
