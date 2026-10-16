---
id: 20261016-bdd01
title: Bilibili Danmaku DEFLATE Decoding
status: completed
created: 2026-10-16
updated: 2026-10-16
branch: wip/bilibili-danmaku-deflate
pr:
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
- Validate downloaded XML and ASS before mux/publication. Reject malformed
  XML and missing ASS events when supported XML comments exist; allow valid
  empty pools and unsupported-only comments. Keep completed media available
  for recovery when this bot post-download check fails.
- Add an explicit-file offline repair tool with a dry-run default, bounded
  decompression, backup-before-write behavior, and object/content revalidation.
  It normalizes XML and regenerates ASS without touching media files.

## Validation

- Rust 1.95.0 all-target tests passed: 772 passed and 15 existing opt-in tests
  ignored. Strict all-target Clippy, formatting, and the release build passed
  with the final immutable SDK revision. All 12 Python repair tests passed.
- Regression tests cover malformed and compressed XML,
  legitimate empty pools, and missing ASS events.
- Repair-tool tests cover compression formats, rendering, limits, backups,
  and concurrent file replacement/content changes versus benign metadata
  changes. No production credentials or live Bilibili requests are needed.

## Local Recovery

- Rebuilt the existing XML/ASS sidecars for episodes 90 through 203 offline:
  114 processed and verified, zero failures, and 400,797 matching XML comments
  and ASS dialogue events. Each episode retains exact original sidecars in
  its own new backup directory.
- Episode 90 has 3,600 restored dialogue events. Its MP4 hash and object
  identity are unchanged. For the other episodes, verification compared
  media object identity and size; the repair tool never opens media for writing.
