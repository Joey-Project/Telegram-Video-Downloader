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
- Ask for a page selection on normal Bilibili video links, whether or not the link includes `?p=N`.
- Offer the linked page (or P1 when no page is specified) and all pages. Keep the entire UGC collection option when membership is detected.

## Current State
- Page-choice callbacks map to `Selection::Page` for one page and `Selection::All` for all pages; the original `?p=N` stays on the one-page choice.
- Direct collection links keep their existing whole-collection confirmation. Bangumi season selection remains unchanged.
- README describes the new Telegram prompt. No live Telegram or Bilibili smoke test has been run.

## Evidence
- `cargo fmt --check` passed.
- `cargo test builds_bilibili_selection_keyboard_and_applies_selection` passed.
- `cargo test membership_probe_migrates_legacy_cookie_before_request` passed using a local mock HTTP response.
