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
- Serialize cancellation and credential commits with the same auth-state lock.
  Telegram starts allocate fresh nonzero generations just like Mini App starts.
- Explicit settings refresh reconciles supported external credential writes and
  removals. Read failures expose unavailable state instead of cached presence.
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
- Integrated local Cloud validation passes 445 Worker/helper tests, 126 Mini App
  tests and 4 Widget regressions, plus Worker/test typechecking and a bundle dry
  run.
- Native validation passes the complete lifecycle E2E, 8 request/privacy/query
  cases, 3 snapshot-safety cases and 8 persistent-epoch cases; the epoch child
  helper remains opt-in and ignored.
- Review regressions pass save-first/cancel-first barriers, Telegram generation
  isolation, explicit credential refresh/error handling and the legacy lifecycle
  in the 6-case settings-auth filter. Contract/privacy cases (8), cross-process
  epoch recovery (3), cancellation regressions (4), and stale/persistent lock
  regressions pass after integrating the current danmaku base.
- The archive eviction mock holds pending, conflict and stable conditions until
  each expected gate is observed; its targeted regression passes without relying
  on a transient stage or changing production Music behavior.
- The authentication lifecycle mock separates request receipt from response
  completion and pauses each relevant poll. Publication acknowledgements precede
  the confirmation baseline; unchanged polls preserve both the auth timestamp
  and publication count, while expiration adds one state update. The revised
  lifecycle E2E passed three consecutive isolated runs, and the shared-helper
  generation E2E passed (`p2-auth-final-1-1794851490792430000.log`,
  `p2-auth-final-2-1794851551117820000.log`,
  `p2-auth-final-3-1794851566548857000.log`,
  `p2-auth-generation-helper-1794851584961278000.log`).
- Strict workspace/all-target Clippy passes with warnings denied. Production
  credentials, real login sessions and services are outside automated acceptance.
