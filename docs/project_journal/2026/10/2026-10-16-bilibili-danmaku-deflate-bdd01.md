---
id: 20261016-bdd01
title: Bilibili Danmaku DEFLATE Decoding
status: completed
created: 2026-10-16
updated: 2026-10-16
branch: wip/bilibili-danmaku-deflate
pr: 41
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
