---
id: 20261115-tma01
title: Torrent music selection in the Mini App
status: completed
created: 2026-11-15
updated: 2026-11-15
branch: "codex/torrent-miniapp"
pr: ""
supersedes: []
superseded_by: ""
---

# Torrent music selection in the Mini App

This extends the download/import backend recorded in `20261114-tmi01` and changes
its default file-selection interface.

## Agreed behavior

- Replace the default Telegram file-selection keyboard with a Torrents view in
  the existing `/file/` Mini App. Keep task summaries and a direct Mini App
  button in Telegram; retain the native controls for configurations without
  Cloud ingress.
- Browse qBittorrent tasks and their folders/files with names, sizes, download
  and Music import status. Support search, filters, selection across pages and
  whole-folder selection without changing download priorities.
- Keep the browser selection as a draft. Review is read-only; cancelling review
  must leave prior waiting imports untouched. Final confirmation atomically
  validates both inventory and selection revisions, then applies exactly the
  reviewed selection. Replayed confirmations cannot restore an older selection.
- Keep source files available for seeding and reuse the existing Music source
  validation, lossless conversion, original archive and content deduplication.

## Architecture boundaries

- Use the existing owner-authenticated action inbox and bounded operation
  results. The Worker injects the verified Telegram owner after strict payload
  validation; the Mac compares that identity with its configured private owner.
- Mac owns qBittorrent and media access. Cloud results contain bounded relative
  display paths, content identifiers and paginated metadata, with no tracker
  credentials, absolute paths or Telegram card authority.
- Opening, refreshing and explicit operations request current data. Existing
  snapshot coalescing and WebSocket invalidations deliver results; no new idle
  polling or repeated full torrent-inventory publishing is introduced.
- New database request-kind migration preserves existing inbox rows and sequence
  high-water marks. Existing native callbacks retain their own stale-card checks.

## Validation

- Worker runtime: strict payload limits, verified actor injection, owner/private
  chat rejection, request deduplication, local ACK and migration preservation.
- Rust: owner isolation, pure review, both independent revision races, atomic
  selection/import admission, replay and restart recovery, rejected-file
  rollback, payload preservation and bounded result projection.
- Mini App: task/folder navigation, search/filter/page selection retention,
  review cancellation, explicit confirmation, stale and unavailable states,
  deep links and no idle polling. New tests join the existing CI commands.
- Ordinary tests use local mock qBittorrent/Telegram/Music services and synthetic
  files. Production deployment and real Music imports are separate operations.

- The torrent-focused Rust suite passed 93 tests, including the mock service
  E2Es and durable inbox review/confirmation/replay tests. Workspace all-targets
  Clippy passed with warnings denied; formatting passed.
- Worker tests passed 437 cases, the Mini App UI suite passed 107 cases, and
  Widget regressions passed four cases. Type, syntax and bundle checks passed.
- The actual app-shell E2E covers queued action receipts, review cancellation,
  confirmation and refreshed detail results. Request IDs take precedence over
  sequence numbers when correlating retained operation results.
- Task navigation ignores late results from a previously opened task, including
  while its submission is still pending. Imported totals include only completed
  Music jobs. Search folders use disjoint child scopes, keeping 5,000-track
  nested inventories within the result budget without losing file pages.
- Inline metadata retrieval shares the background worker exclusion, reloads
  persisted state after claiming it, and retains cleanup ownership if the
  requesting action is cancelled. Tests cover retry and stale captured records.
- Confirmation receipts apply only to the reviewed task and preserve another
  task's draft and review. Folder listings exclude their own synthetic root;
  task-ID and hash deep links both reopen the corresponding task.
- Metadata filesystem work and worker-slot cleanup stay on blocking workers.
  The submitted task's selection is locked until confirmation settles, so later
  edits cannot be lost; other tasks remain browsable and editable.
- Synthetic mobile rendering at 320, 375 and 420 pixels showed no horizontal
  overflow or offscreen controls. Physical Telegram client acceptance is separate.

## Rollout

- Back up D1 privately and apply the new request-kind migration before enabling
  the interface. The migration retains inbox receipts and sequence high-water
  marks; reverting application code must not rewind the database.
- Deploy the matching signed desktop service before the Worker and Mini App
  assets. Existing torrent payloads, priorities and seeding remain unchanged.
