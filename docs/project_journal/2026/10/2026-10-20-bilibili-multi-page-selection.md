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
- README describes the new Telegram prompt. No live Telegram or Bilibili smoke test has been run.

## Evidence
- `cargo fmt --check` passed.
- `cargo test builds_bilibili_selection_keyboard_and_applies_selection` passed.
- `cargo test membership_probe_migrates_legacy_cookie_before_request` passed using a local mock HTTP response.
- `cargo test video_page_prompt_detection_includes_legacy_aid_and_replay_jobs` passed.
- `cargo test video_page_selection_keeps_file_provider_retry_notice_visible` passed.
