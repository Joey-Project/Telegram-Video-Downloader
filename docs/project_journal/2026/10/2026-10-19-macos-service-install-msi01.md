---
id: 20261019-msi01
title: Production Executable Outside Cargo Output
status: completed
created: 2026-10-19
updated: 2026-10-19
branch: codex/macos-service-install
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/45
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
- Installer: 89 fake-HOME regression tests, shell integration, Bash syntax,
  ShellCheck, Ruff 0.15.13 lint and formatting pass. Coverage includes a
  temporary build artifact inheriting group `wheel`, strict state backup
  metadata, startup PID visibility, partial-stop failures, and both rollback
  policies. Runtime builds and tests use isolated target/build directories.
- GitHub review corrections preserve configured media aliases, record media
  hardlink anchors without reading or copying payloads, and validate/capture the
  entire saved plist before rollback stops any service. A subprocess regression
  verifies that startup and exactly two poll markers survive `RUST_LOG=off`
  while ordinary tracing output remains filtered.
- Further regressions cover macOS full-width executable-path inventory,
  download-worker ancestry and shutdown, stderr preflight, unrelated media
  symlinks, role-specific executable/cwd checks, and rollback transaction
  validation before any stop or replacement.
- Configuration path resolution follows the preserved service HOME, exact Rust
  expansion forms, physical configuration parent, and filesystem handling of
  aliases followed by `..`. Authentication paths are validated before stopping.
  Stderr must be an observable regular log, and readiness requires distinct
  `poll=1` and `poll=2` markers even when log paths share a hardlink.
- Additional compatibility checks preserve configuration and working-directory
  aliases while binding their physical targets, fail incomplete metadata walks,
  isolate process checks by user, scan verbose logs incrementally, and restore
  validated .NET-root discovery for first installs.
- Readiness records bind startup and both polls to the emitting service PID.
  Preserve log aliases, identify custom-named existing executables and workers,
  retain authentication cleanup recovery state, and reject missing rollback
  binaries even when the previous executable had no LaunchAgent plist.
- Existing main-checkout changes and divergent local history remain untouched.

## Controlled Cutover and Current Runtime

- Joey initially deferred the new-path Documents authorization and requested
  restoration of the original entrypoint, then resumed migration after unlocking
  the system. At `2026-10-19 18:33:37 UTC`, fixed-path PID `2221` passed startup-ready
  and two distinct successful Telegram polls, with all three markers bound to that PID.
- The GUI LaunchAgent is the only running downloader instance; the legacy user
  service is absent. `lsof` confirms
  `~/.local/lib/telegram-video-downloader/bin/telegram-video-downloader` and the
  preserved canonical repository working directory. Configuration, logs,
  authentication and media paths remain unchanged.
- An independent release build of the reviewed merged source took 2m32s in
  isolated target/build directories. The installed executable retains
  `Telegram Video Downloader Local Signing`, fingerprint
  `C24FE0C91539BD95BA4081CD944A8E17053FB304`, identifier
  `io.github.telegram-local-downloader.bot`, and the original designated
  requirement. Signature integrity and requirement verification pass.
- Both queue indexes parse and all 19 video records remain readable: 14 completed
  and 5 cancelled, with no missing indexed record. PDF has no indexed tasks.
  All 30 compared configuration/state files retain their hashes, owner, group,
  mode, device and link count. Twenty-eight retain their inodes; the two
  `index.json` files have new inodes from the expected atomic rewrite in
  `QueueManager::open`. Lock identities and media-associated state remain in place.
- Retain the new rollback executable, plist, configuration and state metadata in
  `~/.local/lib/telegram-video-downloader/backups/20261019T182920.731238Z`, alongside
  the earlier `20261019T103210.929774Z` backup. All 40 backed-up files pass hash,
  owner, group and mode checks; manifests record original device/inode identities.
  Media payloads are not copied or relocated.

## Next Steps

- No migration step remains. Future production updates use explicit signed,
  atomic installation; ordinary builds and tests do not deploy.
- Cargo cache cleanup is independent of the active executable. Keep rollback
  backups, configuration, repository PDF-helper resources, authentication data,
  logs and media-associated queue/completion/lock/recovery state. Cache cleanup
  is a separate operation and was not performed during this cutover.
