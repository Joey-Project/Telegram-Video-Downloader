---
id: 20261116-sba01
title: Bilibili login controls in the settings Mini App
status: completed
created: 2026-11-16
updated: 2026-11-16
branch: "codex/settings-login"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/77"
supersedes: []
superseded_by: ""
---

# Bilibili login controls in the settings Mini App

## Scope

- Show the local login attempt and saved credential types in `/settings`.
  Credential presence is not a claim that Bilibili still accepts the session.
- Start Web, TV or access-key login through the existing local login flows.
  Send QR codes and authorization links only to the owner's Telegram chat.
- Bind cancellation and explicitly confirmed logout to the displayed attempt
  generation and persistent credential revision. Retain existing Telegram commands.
- Keep cancelled, expired or superseded attempts from saving stale credentials.
  A failed new attempt does not hide credentials already saved locally.

## Boundaries

- The optional authentication snapshot exposes bounded phase, mode, generation,
  credential revision, presence booleans, update time and fixed safe reason codes.
- QR tickets, links, cookies, access keys and raw provider responses stay off
  Cloudflare, Mini App storage, action results and public operation history.
- Reuse the existing credential lock, epoch, callback ownership and timeout
  rules. A durable action acknowledgement does not mean login has succeeded.
- The Mini App uses the existing user-driven snapshot refresh and live update
  connection; no background authentication polling is added to Cloudflare.
- D1 migration `0010` preserves all earlier kinds, retained rows and sequence
  high-water marks. Older snapshots without authentication data show unavailable.

## Acceptance

- Mock Telegram/provider E2E passes the actual durable dispatcher through QR
  delivery, success, expiry, failure, duplicate start, cancellation, logout,
  restart, external-save conflict/refresh/retry and viewer-aware snapshot push.
- Secret sentinels stay out of public snapshots, receipts, history and UI;
  authorization messages reach only the owner chat.
- Mini App app-shell tests cover current, older and stale settings snapshots,
  safe credential labels, cancellation and explicit logout. Existing CI commands
  discover the new native and UI cases.
- Integrated local Cloud validation passes 445 Worker/helper tests, 120 Mini App
  tests and 4 Widget regressions, plus Worker/test typechecking and a bundle dry
  run.
- Native validation passes the complete lifecycle E2E, 8 request/privacy/query
  cases, 3 snapshot-safety cases and 8 persistent-epoch cases; the epoch child
  helper remains opt-in and ignored.
- Strict workspace/all-target Clippy passes with warnings denied. Production
  credentials, real login sessions and services are outside automated acceptance.
