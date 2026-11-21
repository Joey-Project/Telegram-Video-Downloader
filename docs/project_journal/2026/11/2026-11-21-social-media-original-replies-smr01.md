---
id: 20261121-smr01
title: Download Instagram and X media and reply with originals
status: completed
created: 2026-11-21
updated: 2026-11-21
branch: "codex/social-media-mtproto"
pr: ""
supersedes: []
superseded_by: ""
---

# Download Instagram and X media and reply with originals

- Route supported Instagram and X links through the persistent queue. Only the configured private owner can submit a social reply task.
- Run gallery-dl for Instagram with the Mac's preferred browser profile and account-specific fallback. Run yt-dlp for X status videos. Preserve downloaded bytes and keep interrupted staging for recovery.
- Atomically publish original files into the selected library and reply directly to the submitting Telegram message as the existing Bot over MTProto. Bind each task to its originating Bot ID, keep its originals outside ordinary file moves, record a receipt or size-limit disposition per attachment and reuse published files and stable MTProto random IDs on retry.
- Exercise the real queue and publication logic with fake Telegram and local downloader processes, plus protocol-level tests with a fake Telegram sender.

## Verification

- Mock Telegram end-to-end testing exercises partial delivery, stable replay IDs, durable preservation of originals, and resuming from the queue.
- Authenticated gallery-dl checks on the two supplied exact Highlight links produced a JPEG with the primary profile and an MP4 with the fallback profile after removal of tracking parameters.
- The independent live Bot returned `USER_MIGRATE` during initial MTProto authentication. The sender retries authentication once after the pinned Telethon client switches to Telegram's requested data center. Regression tests cover a completed switch, an unconfirmed switch, and repeated migrations.
- Production deployment and real large-file Telegram delivery remain release acceptance steps.
