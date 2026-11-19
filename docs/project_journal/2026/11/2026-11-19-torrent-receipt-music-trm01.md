---
id: 20261119-trm01
title: Add all torrent music from the original Telegram receipt
status: completed
created: 2026-11-19
updated: 2026-11-19
branch: "codex/torrent-receipt-add-all-music"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/86"
supersedes: []
superseded_by: ""
---

# Add all torrent music from the original Telegram receipt

## Agreed behavior

- Edit the original acknowledgement for a newly submitted torrent document or magnet task as validated metadata becomes available.
- Once the complete file inventory is verified, show the task summary and supported music count, with an **添加所有音乐** action when music import is available.
- One click explicitly schedules all supported audio through the existing durable import pipeline. Incomplete files wait until downloaded before admission to Music.
- After saving the request, edit the same message to acknowledge how many music files were scheduled. Keep scheduled and completed import states distinct.
- Reuse owner/chat/message validation, stale-card protection, restart/replay behavior and Music deduplication. Preserve torrent payloads, download priorities and seeding.

## Implementation

- Extend the existing durable task-card lifecycle and direct Telegram edits, preserving the Mini App management entry and native selection controls.
- Bind the single AddAllMusic callback to the current selection revision. Validate inventory, select all supported files and persist import intents in one store transaction; later selection changes invalidate older buttons.
- Derive the scheduled-file receipt from the persisted import ledger, so progress updates and restart retain the confirmation without a second request lifecycle.
- If a pending request needs fresh confirmation after a save-path or selection revision change, show it separately from scheduled imports and offer the all-music action again.
- Offer the action only for a verified full inventory with supported audio and enabled Music imports. Keep scheduled and imported totals separate.

## Validation

- Mock Telegram/qBittorrent coverage for delayed complete metadata, same-message updates and the explicit all-music callback.
- Verify durable acknowledgement, repeated clicks, restart/replay, unsupported/no-music tasks and retryable errors without real Music imports or production task changes.
- Seven new regressions join the existing workspace/all-targets CI command without a workflow change. All 102 torrent-focused tests pass, including the full mocked Telegram/qBittorrent/Music path and preserved payloads.
- The pending-request checks use per-file ledger state; the existing imported count continues to include only completed Music jobs. Both failure cases from the initial test-oracle mismatch pass after correction, and completion-before-download is explicitly rejected by the E2E expectations.
- The offline workspace/all-targets suite passes with two test threads: 1,782 bot tests passed with 17 existing ignores; converter targets passed 27 and 8 tests. Validation uses mocks and temporary files.
- After an equivalent helper-argument cleanup for Clippy, all seven renderer tests, workspace/all-targets Clippy with warnings denied, formatting and diff checks pass. The complete suite result above precedes only that bounded refactoring.
- Cloud ingress already carries native callback data unchanged through its owner/private-chat checks and durable inbox. The new callback uses the existing namespace; no Worker, Mini App or database migration is required.
