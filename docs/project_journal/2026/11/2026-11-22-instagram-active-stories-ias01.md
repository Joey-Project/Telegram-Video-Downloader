---
id: 20261122-ias01
title: Download active Instagram Stories from a profile link
status: completed
created: 2026-11-22
updated: 2026-11-22
branch: "codex/instagram-active-stories"
pr: ""
supersedes: []
superseded_by: ""
---

# Download active Instagram Stories from a profile link

- Admit `/story <Instagram profile URL>` through the same owner-only queue as single Instagram media. Plain profile URLs continue to resolve to avatars.
- Download the active Stories visible to the configured Instagram session and reply with their original files to the command message. Reject batches above 100 items before publication. Retry reuses archived files and durable delivery receipts when available.
- Accept verified files when gallery-dl succeeds after a recovered downloader error. A failed exit or invalid staging still blocks publication.

## Verification

- Reproduced the production single-Story failure in isolated staging with gallery-dl 1.32.21. The primary profile returned no results. The fallback profile recovered from an optional downloader import error, exited successfully, and wrote the same MP4 bytes already preserved in production staging.
- Mock Telegram verified owner-only queue admission and preservation of the original reply target. Local gallery-dl fixtures verified collection arguments, mixed image and video ordering, the size of a batch, and the recovered downloader error.
