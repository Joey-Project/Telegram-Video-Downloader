---
id: 20261016-bdd01
title: Bilibili Danmaku DEFLATE Decoding
status: completed
created: 2026-10-16
updated: 2026-10-17
branch: wip/bilibili-danmaku-deflate-clean
pr: 42
supersedes: []
superseded_by:
---

# Bilibili Danmaku DEFLATE Decoding

## Evidence

- An existing episode had a 162,894-byte XML sidecar containing raw DEFLATE
  data and a 552-byte ASS containing no dialogue. Offline decompression
  recovered well-formed XML with 3,600 comments.
- The pinned SDK HTTP client enabled gzip but not deflate. Its permissive
  converter accepted the compressed response bytes and emitted only an ASS
  header, while the media download completed successfully.

## Changes

- Update the pinned SDK to include bounded raw DEFLATE, zlib, and gzip
  normalization before ASS conversion. Reqwest's optional deflate decoder
  accepts zlib wrapping only, so enabling that feature alone does not fix the
  observed raw stream. Retain the other dependency pins.
- Keep decoding, XML validation, and ASS conversion in the SDK rather than
  duplicating its private conversion rules in the bot. Invalid, truncated,
  and oversized payloads produce explicit SDK errors.

## Validation

- Rust 1.95.0 all-target tests passed: 762 passed and 15 existing opt-in tests
  ignored. Strict all-target Clippy, formatting, and the release build passed
  with the final immutable SDK revision.
- SDK regression coverage includes malformed and compressed XML, bounded
  decoding, multi-member gzip, retries, cancellation, and logical-file progress;
  its workspace suite passed 871 tests with three opt-in tests ignored.
- No production credentials or live Bilibili requests are needed for tests.

## Local Recovery

- Rebuilt the existing XML/ASS sidecars for episodes 90 through 203 offline:
  114 processed and verified, zero failures, and 400,797 matching XML comments
  and ASS dialogue events. Each episode retains exact original sidecars in
  its own new backup directory.
- The one-time recovery utility and operation receipts are retained outside
  the repository; no public repair command is introduced by this change.
- Episode 90 has 3,600 restored dialogue events. Its MP4 hash and object
  identity are unchanged. For the other episodes, verification compared
  media object identity and size; the repair tool never opens media for writing.
- A follow-up audit inspected 278 downloaded videos, including 267 with video
  creation or modification dates on or after October 9. It found another 154
  compressed-XML/header-only-ASS pairs: 96 Reborn copies covering episodes
  1 through 89 and seven extra copies of episodes 1 through 7, 37 piano
  collection entries, and 21 other videos. All 154 were backed up, rebuilt,
  and independently verified with 361,211 restored dialogue events and no
  failures. No video download or network request was needed.
- The post-repair audit found matching XML/ASS event counts for all 267 recent
  videos. All 243 unselected sidecars retained their SHA-256 content digests,
  and all 278 videos retained object identity, regular-file type, and size.
  Four older count mismatches and one older missing ASS were left unchanged;
  their original conversion options are unknown. Event-count verification
  does not establish actual player display behavior.
- Exact originals remain in each repaired entry's
  `.danmaku-backup-20261016` directory. Detailed batch receipts are retained
  under `/private/tmp/tvd-ass-repair-20261016`, with independent scan and
  scope-verification receipts under `/private/tmp/tvd-ass-audit-20261016`.
- The five older outliers date to June 6 or June 16. The four existing ASS
  files identify legacy C# BBDown as their generator; every event matches
  its XML by text and start time. Missing events cluster at high density,
  consistent with the legacy converter skipping comments when all 13
  lanes in their position class are occupied. This is distinct from the
  compressed-XML/header-only failure. The missing ASS belongs to the
  10-minute Pamela arm/abdomen workout: dry-run conversion produces 266
  events, and ffprobe found no embedded subtitle stream. Existing files
  cannot establish why its external ASS is absent. The user elected to
  retain all five older outliers unchanged. Investigation receipts
  are retained under `/private/tmp/tvd-legacy-ass-inspect-20261016`.

## Acceptance and Closure

- The SDK decoding fix and bot PR #42 were merged and deployed. On October 17,
  the owner confirmed that the rebuilt ASS files display correctly during
  playback. The playback acceptance check is complete.
- The media audit fully decoded 62 published MP4s, covering 13.65 hours of
  video and audio. Seven byte-identical old Reborn MP4 copies and ten completed
  task staging directories were subsequently permanently deleted with the
  owner's approval: 17 items and 43,616,230,014 logical bytes (40.62 GiB).
- A fresh preservation baseline and post-deletion check covered 271 retained
  videos and 551 XML/ASS sidecars, with zero mismatches. The retained unmatched
  staging directory was also checked without deleting or modifying it. The
  operation receipts remain outside this worktree under
  `/private/tmp/tvd-cleanup-execution-20261016`.
- The owner confirmed that concurrent downloads explained the apparent lack
  of available-space growth during cleanup. No storage-reclamation follow-up
  remains open for this workstream.
- The owner elected not to track the unmatched staging directory, alternate
  media-version choices, or the piano video's terminal timestamp regression.
  Keep the remaining files as they are; these are not pending workstream tasks.
- Refreshing already downloaded sidecars through a reusable bot feature is
  separate product backlog. This workstream completed a one-time offline
  recovery and does not expose a new public repair command.
