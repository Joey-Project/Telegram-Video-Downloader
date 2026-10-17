---
id: 20261017-adr01
title: ASS Bot Deployment and Reborn 203-Episode Refresh
status: completed
created: 2026-10-17
updated: 2026-10-17
branch: wip/ass-danmaku-defaults
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/43
supersedes: []
superseded_by:
---

# ASS Bot Deployment and Reborn 203-Episode Refresh

## Delivery

- The user explicitly requested pushing and deploying the three-times font,
  70%-opacity, upper-half ASS defaults and refreshing the locally downloaded
  203 episodes of HITMAN REBORN.
- Merge the already deployed main-branch SDK DEFLATE fix before deployment.
  The SDK remains pinned to `da70c4787b4b5f78bac2ce4f9b87fc035cffb74f`.
  Signed implementation commit `08de473` and signed compatibility merge
  `ef2bccf` preserve the existing SDK fix in the ASS delivery branch.
- The user subsequently authorized PR delivery into master and a refresh of
  the local deployment. PR #43 carries the implementation and these records.
- The main checkout's unrelated uncommitted changes remain untouched.
- The initial direct deployment preceded PR review. The PR delivery uses the
  repository's remote GitHub Codex review and Rust CI gates; the earlier local
  review adapter ACL failure is not counted as a completed review.

## Deployment

- Cargo/Rust 1.95.0: isolated offline release build, 770 passing all-target
  tests (16 opt-in tests ignored), formatting, and all-target Clippy with
  warnings denied pass after the SDK merge.
- Preserve a rollback binary before building. Sign the staged executable with
  the existing certificate `C24FE0C91539BD95BA4081CD944A8E17053FB304` and
  identifier `io.github.telegram-local-downloader.bot`. Strict signature and
  recorded designated-requirement checks pass before and after atomic install.
- Installed executable SHA-256:
  `74612766b73934d1c312c0b31ac05a587b3a36e23a9da2ea14ae495569e3ee05`.
- Graceful restart replaces PID 14548 with PID 72857. The startup-ready log is
  present at `2026-10-17T13:49:36.118592Z`; the process connects to Telegram's
  HTTPS endpoint. Follow-up network counters verify continued activity.
- Existing retained-removal and completed-staging recovery warnings remain
  preserved. They do not prevent startup. No TCC, LaunchAgent, credential,
  configuration, or unrelated cleanup changes were made.

## Library Refresh

- Select exactly one video and same-basename ASS/XML pair for each episode
  1 through 203. Exclude historical backups and duplicate directories without
  a corresponding video; the wider series tree contains 420 ASS files.
- Stage refreshed ASS through the production renderer, with input object
  identities and content digests retained for publication validation.
- Publish all 203 ASS files through the production atomic-replacement layer,
  after saving and hashing every original. Originals total 88,202,883 bytes;
  refreshed files total 112,266,728 bytes.
- All 720,229 dialogue rows retain their first nine fields, times, body text,
  and RGB colors. Dialogue/style font sizes are tripled, alpha is `4D`, and
  each dialogue clips to `(0,0,1920,540)`. Full staged semantic verification
  and live-file SHA-256 comparison pass.
- The publisher finished every replacement, then its final XML-size assertion
  misread fields absent from the older stage manifest and returned an error.
  Independent final verification uses the original inventory plus content
  hashes: every live ASS matches its validated output; all backups match the
  originals; XML hashes and identities are unchanged; video identities and
  sizes are unchanged. No replacement was rerun.
- Preserve the 203 originals, per-file receipts, stage manifest, semantic
  verification, final verification, and deployment receipt in the series'
  `.danmaku-style-backup-20261017-eCaxwC` directory. Stage manifest SHA-256:
  `4a8bb01e089e7068518c01beded55bf17f4b24f14b6322ea39d0bfe8e71d7071`.
- Existing XML and videos are not rewritten or downloaded again. Historical
  backup directories and duplicate directories without videos are untouched.
  Temporary build/stage copies are removed after preserving the receipts.
