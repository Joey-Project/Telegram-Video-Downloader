---
id: 20261009-akq01
title: Automatic Access-Key QR Authorization
status: completed
created: 2026-10-09
updated: 2026-10-09
branch: wip/access-key-qr-login
pr: null
supersedes: []
superseded_by:
---

# Automatic Access-Key QR Authorization

## Problem

- The previous `/bbdown login access-key` flow sent a BiliPlus landing-page QR and
  expected users to copy a callback URL or BALH credential message into Telegram.
- The live BiliPlus page uses `getauth` and `authpoll`, saves the authorization
  result internally, and redirects. It does not display the expected callback.

## Implementation

- The bot obtains the actual Bilibili authorization QR from BiliPlus and polls the
  provider until scan, confirmation, expiry, or success. A successful result saves
  the generic access key through the existing authentication transaction.
- Existing credentials are not sent during QR acquisition or polling. Polling
  posts the short-lived authorization code in the request body. Redirects are
  disabled, authorization QR origins are checked, and response bodies are capped
  at 64 KiB. Provider response bodies and credential values are excluded from errors.
- The private-chat login ownership marker blocks competing logins and manual
  callback processing. Success, timeout, expiry, failure, and logout clean up the
  matching ticket. Existing generation and cross-process epoch checks prevent
  stale login completion from restoring credentials after logout.
- Web-cookie and TV login behavior is preserved. The bot still depends on BiliPlus
  for generic access-key acquisition; automatic login does not establish that a
  public resolver can serve a particular episode.

## Validation

- Live protocol preflight on 2026-10-09: `getauth` returned code `0`, an
  authorization code, and a QR URL on `https://passport.bilibili.com`; one unscanned
  `authpoll` returned `86039`. No existing account credentials were sent and no
  account login was completed.
- Five provider mock tests cover automatic acquisition/polling, generic-key
  mapping, request method and body, cookie exclusion, scan/confirm/expiry states,
  untrusted QR origins, redirects, malformed/oversized responses, and error redaction.
- The pending-ticket regression covers callback exclusion and exact-ticket cleanup.
- The Telegram authorization-link regression verifies delivery preserves the QR
  link while ordinary logs contain only a fixed redacted placeholder.
- Synthetic credential fixture: `joey-private-v3`, `access-a`.
- `cargo clippy --all-targets --locked --offline -- -D warnings`: passed.
- `cargo test --all-targets --locked --offline --quiet`: 544 passed, 11 ignored,
  zero failures (555 total).
- `cargo build --release --locked --offline`: passed with Cargo 1.95.0.
- `cargo fmt --all -- --check`, `git diff --check`, and project-journal
  validation: passed.
- Local review was explicitly omitted for this task.
