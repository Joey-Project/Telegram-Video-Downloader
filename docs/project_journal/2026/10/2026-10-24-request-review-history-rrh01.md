---
id: 20261024-rrh01
title: Actionable request review and recent local history
status: completed
created: 2026-10-24
updated: 2026-10-24
branch: "codex/request-review-history"
pr: ""
supersedes: []
superseded_by: ""
---

# Actionable request review and recent local history

## Scope

- Make Requests the actionable review entry point for file and library-migration previews.
- Keep operation forms and running work in Organizing, and move settled results to a separate searchable History view.
- Allow an unwanted awaiting-confirmation preview to be cancelled durably without changing media or requiring its library to be online.
- Reuse bounded recent management receipts and migration records saved by the Mac. History is best effort and does not introduce a permanent cloud archive or change file discovery.

## Interface and history contract

- Requests contains the actual file and migration preview cards and their separate confirmation/cancellation actions. Cross-library entries navigate to the original library and exact preview; orphaned-library previews can still be cancelled without a registered root.
- An accepted cancellation receipt alone does not hide its target. Wait for the exact completed cancellation result or the original preview's cancelled status. Failure keeps the preview visible, displays the reason, and restores its available actions.
- Reuse existing browser queue ownership and durable receipts across tabs. Reloads reconstruct queued cancellation tracking from the existing queue and never synthesize a new action.
- Completed/cancelled operation results and settled migrations leave active lists and counts. Expired/stale previews are terminal for both file and migration kinds. Running, paused, and control-bearing migration recovery/finalization work stays in Organizing.
- History searches retained records from all libraries in the browser. It reuses the Mac's bounded recent management history (up to 100 receipts, with seven-day terminal retention) and bounded migration projection (up to 32 jobs); omission and truncation remain visible. This is recent activity, not an all-time archive.
- Hide selection controls in Requests/History without clearing selections. Clear the new private view content and cancellation tracking on access expiry, using the existing owner-verification lifecycle.

## Cancellation contract

- `preview_cancel` carries the exact original preview kind, sequence, ID, and revision. The only permitted kinds are `file_preview` and `library_migration_preview`.
- The Worker validates the bounded identity and persists the action in the existing durable inbox with ordinary request-ID deduplication and replay rules.
- The local inbox checks identity and lifecycle under its existing operation lock. Only an unexecuted preview can become cancelled; a later confirmation cannot execute it.
- Cancelling an awaiting preview has no filesystem effects. Existing controls continue to govern already executing migrations.
- The new D1 migration preserves inbox rows, acknowledgments, deduplication keys, and the historical AUTOINCREMENT high-water mark, including when no rows remain.

## Validation

- The new mock Telegram E2E creates real temporary file and library-migration previews, rejects incorrect identity without consuming the original proposal, confirms an independent proposal, and refuses to cancel that completed work. It then takes a library offline, cancels both remaining previews, reopens the inbox/runtime, verifies cancelled history and the absence of active previews, restores the library, and rejects later confirmations while preserving file bytes and paths.
- Focused cancellation tests: four passed. The complete Rust suite passed 1,293 cases with zero failures and 16 pre-existing ignored cases; all-target Clippy with warnings denied, formatting checks, and an isolated optimized release build passed.
- Cloud transport tests cover exact cancellation identities, malformed values, stable request-ID retries, changed-payload rejection, and desktop acknowledgments. Migration tests cover populated and empty inboxes, row/ack preservation, sequence high-water marks, deduplication, and transactional failure rollback.
- Browser receipt regressions cover a helper tab sending cancellation, origin-tab sequence/error recovery, rejected receipts, and mismatched request/surface/kind/sequence rejection. All 30 action-queue tests passed.
- Full Cloud suite: 339 ordinary Worker/logic tests and 42 Mini App UI tests passed. The mounted-app E2E covers cross-library review, file/migration cancellation payloads, pending-state retention, backend failure recovery, terminal WebSocket updates, searchable History, and active migration recovery controls. The metadata recovery UI suite preserves all 29 prior tracking and reload cases with Requests as the approval destination.
- Binding generation, source/test TypeScript, JavaScript syntax, and default/production-configuration Wrangler dry-runs passed using the pinned toolchain. Both bundles contain 69 assets. Local checks use synthetic credentials and temporary media; production delivery evidence stays in ignored local records.
