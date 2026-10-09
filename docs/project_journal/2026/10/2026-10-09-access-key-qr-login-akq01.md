---
id: 20261009-akq01
title: Automatic Access-Key QR Authorization
status: blocked
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

## Runtime Deployment

- Built from the worktree into the existing canonical checkout's release target.
  The existing LaunchAgent continued to use the canonical binary, config, and
  working directory. Process inventory confirmed only one bot instance.
- The updated binary did not reach `telegram local downloader started`. A
  bounded process sample showed `QueueManager::open` waiting inside macOS
  `NSFileCoordinator` and `__open` while coordinating a download-root read.
  One restart reproduced the same startup block.
- Restored the pre-update binary and restarted the same LaunchAgent. A second
  bounded sample showed the identical directory-coordination block in that
  original binary as well. The restored process exists but is not verified ready.
- The access-key fix is committed and locally validated. Runtime deployment and
  real scan/confirmation remain blocked by the download-root startup issue.
  No logout, credential deletion, task cleanup, or resolver configuration change
  was performed.
- The fixed binary and bounded validation/sample artifacts are retained in
  `/private/tmp/telegram-access-key-flow` for recovery.

## Download-Root Pinning

- On 2026-10-09, Joey authorized pinning the configured download roots. The video
  root `/Users/joey/Movies/Downloads` has no managed File Provider item according
  to `fileproviderctl evaluate`; the PDF root is managed by File Provider.
- The PDF root was initially downloaded but not kept downloaded, and was not
  recursively downloaded. After checking the implementation in
  [icloud-tools Pinner.swift](https://github.com/icanhasjonas/icloud-tools/blob/main/Sources/icloud/Core/Pinner.swift),
  wrote the File Provider pin marker `com.apple.fileprovider.pinned#PX` with byte
  `0x31` to `/Users/joey/Documents/Downloads` only. No third-party tool was installed.
- System verification with `fileproviderctl evaluate` confirmed
  `isKeepDownloaded = 1`, followed by `isRecursivelyDownloaded = 1`. The hidden
  `.telegram-video-downloader-queue` directory also reports both flags as `1`
  through the inherited pin. File contents and access permissions were not edited.
- Read-only API research used GPT-6 Luna at high reasoning. Public File Provider
  download requests do not provide an equivalent client-facing pin setter; the
  pin marker is an implementation detail, and the actual system state was verified.
- Re-deployed the previously validated fixed binary to the canonical release
  path and restarted the existing LaunchAgent. A final restart after recursive
  downloading completed still did not reach the startup-ready log. A one-second
  process sample confirmed the same `QueueManager::open` -> `NSFileCoordinator`
  -> `__open` wait. Pinning and materialization are complete; the remaining
  startup-coordination block is unresolved. The canonical release path currently
  contains the fixed binary; the original binary and bounded diagnostic samples
  remain in the task-scoped temporary directory for recovery.
