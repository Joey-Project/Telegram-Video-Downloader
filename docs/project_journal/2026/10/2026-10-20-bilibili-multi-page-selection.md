---
id: 20261020-bilibili-multi-page-selection
title: Bilibili Multi-Page Download Choice
status: completed
created: 2026-10-20
updated: 2026-10-20
branch:
pr:
supersedes: []
superseded_by:
---

# Bilibili Multi-Page Download Choice

## Summary
- Ask for a page selection on normal Bilibili BV and AV video links, whether or not the link includes `?p=N`.
- Offer the linked page (or P1 when no page is specified) and all pages. Keep the entire UGC collection option when membership is detected.

## Current State
- Page-choice callbacks map to `Selection::Page` for one page and `Selection::All` for all pages; the original `?p=N` stays on the one-page choice.
- Direct collection links keep their existing whole-collection confirmation. Bangumi season selection remains unchanged.
- `--replay-message` reports video links that require this Telegram-only selection prompt instead of starting an ambiguous download.
- AV links resolve their BV ID before checking collection membership. Failed Bilibili short-link normalization stops the task rather than falling through to an unconfirmed download.
- Video-page selection persistence reports File Provider retries so the task does not appear to vanish while its keyboard is pending.
- Failed short-link tasks retry their terminal status write if File Provider temporarily blocks the queue.
- Short-link failure notification is sent before that retry loop, and README now documents that replay mode cannot handle Telegram-only Bilibili choices.
- README describes the new Telegram prompt. No live Telegram or Bilibili smoke test has been run.

## Follow-up: Single-Page Prompt and Existing-Part Skip
- Multi-page video links, including explicit `?p=N`, ask whether to download the linked P (or P1) or all P. A one-page standalone video starts directly; collection membership still offers the whole-collection action.
- When an all-P download finds compatible downloaded parts, the duplicate prompt offers `Skip downloaded`; the missing-entry plan downloads only remaining page indices and revalidates skipped media before reporting completion.
- The same missing-entry selection continues to support season entries. Unsupported media-only modes do not offer skip.
- Follow-up branch and PR/merge/deployment evidence will be recorded after the exact PR head passes review and CI.

## Evidence
- `cargo fmt --check` passed.
- `cargo test builds_bilibili_selection_keyboard_and_applies_selection` passed.
- `cargo test membership_probe_migrates_legacy_cookie_before_request` passed using a local mock HTTP response.
- `cargo test video_page_prompt_detection_includes_legacy_aid_and_replay_jobs` passed.
- `cargo test video_page_selection_keeps_file_provider_retry_notice_visible` passed.
- `cargo test failed_task_state_retries_file_provider_write_errors` passed.
