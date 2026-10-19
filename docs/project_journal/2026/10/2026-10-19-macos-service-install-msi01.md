---
id: 20261019-msi01
title: Production Executable Outside Cargo Output
status: blocked
created: 2026-10-19
updated: 2026-10-19
branch: codex/macos-service-install
pr:
supersedes: []
superseded_by:
---

# Production Executable Outside Cargo Output

## Architectural Boundary

- The production LaunchAgent must execute
  `~/.local/lib/telegram-video-downloader/bin/telegram-video-downloader`,
  independently of Cargo target and intermediate build directories.
- Joey explicitly clarified that queue, completion history, publication locks,
  staging, and recovery metadata intentionally remain beside the media.
  Existing configuration, authentication state, working directory, and logs
  outside Cargo output remain in place. This supersedes the initial proposal
  to relocate every runtime path to Application Support.
- This boundary is recorded in `AGENTS.md` and the deployment documentation.
  Runtime credentials and configuration contents are not included in the diff.

## Implementation

- Explicit install/deploy commands stage and sign a copy, verify the existing
  designated requirement, stop managed services, back up state, and atomically
  install the executable and plist. Ordinary builds/tests do not deploy.
- Preserve the original certificate and fixed identifier; retain rollback
  artifacts outside Cargo output. Rollback preserves newer runtime writes.
- Default rollback uses the fixed path after Cargo cleanup; explicit
  `--original-entrypoint` verifies the original source before restoring its
  exact plist. A rollback reports service readiness as unconfirmed.
- Log the first two successful Telegram polls without token, URL, or message
  payload, so startup validation establishes continued polling.

## Validation

- Rust/Cargo 1.99.1: isolated, locked, offline release build; 771 all-target
  release tests passed, 16 opt-in tests ignored; release all-target Clippy
  with warnings denied and formatting passed.
- Before switching, both media indexes parse and all 19 indexed video records
  are readable: 14 completed and 5 cancelled. PDF has no indexed tasks.
- Installer: 39 fake-HOME regression tests, shell integration, Bash syntax,
  ShellCheck, Ruff 0.15.13 lint and formatting pass. Coverage includes a
  temporary build artifact inheriting group `wheel`, strict state backup
  metadata, startup PID visibility, partial-stop failures, and both rollback
  policies. Runtime builds and tests use isolated target/build directories.
- GitHub review corrections preserve configured media aliases, record media
  hardlink anchors without reading or copying payloads, and validate/capture the
  entire saved plist before rollback stops any service. A subprocess regression
  verifies that startup and exactly two poll markers survive `RUST_LOG=off`
  while ordinary tracing output remains filtered.
- Existing main-checkout changes and divergent local history remain untouched.

## Controlled Cutover and Current Runtime

- The new destination is
  `/Users/joey/.local/lib/telegram-video-downloader/bin/telegram-video-downloader`.
  Retain the rollback executable, exact plist, configuration, and state metadata
  under `~/.local/lib/telegram-video-downloader/backups/20261019T103210.929774Z`.
  Metadata backups preserve ownership/permissions/content; staging media payloads
  remain in place. Original lock identities are recorded, never recreated.
- The new path triggered a native Documents authorization request
  (`kTCCServiceSystemPolicyDocumentsFolder`, request `65688.38`). Joey declined
  granting new-path access and asked to restore the original repository entry.
- Restore the exact original plist and signed executable entrypoint at
  `~/Program/Codex-workspace/Telegram-Video-Downloader/target/release/telegram-video-downloader`.
  Configuration, working directory, logs, authentication, and media remain in
  their existing locations. No privacy grants or TCC state were changed.
- Recovery PID 74606 is a single original-path process but still waits inside
  `NSFileCoordinator` during queue startup. No new startup-ready marker or
  resumed polling has been verified. Do not treat a running PID as recovery.
- Compare 30 configuration, queue/completion, lock, and authentication files
  against the pre-switch snapshot: hashes, ownership, modes, device/inode,
  and link counts are unchanged. All 19 indexed records remain readable.

## Next Steps

- Resolve the outstanding native authorization/coordinator request without
  granting new-path access against Joey's decision, then verify the original
  service's startup-ready and Telegram activity.
- Activate the fixed path only after separate authorization and successful
  runtime checks. The original live Cargo release output must not be cleaned
  while this cutover remains deferred.
