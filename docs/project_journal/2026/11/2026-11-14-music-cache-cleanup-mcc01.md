---
id: 20261114-mcc01
title: Clean completed music download staging and conversion caches
status: completed
created: 2026-11-14
updated: 2026-11-14
branch: "codex/music-cache-cleanup"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/72"
supersedes: []
superseded_by: ""
---

# Clean completed music download staging and conversion caches

## Problem

Completed music imports retained both their downloaded originals and converted
import files, even after the original had reached iCloud Drive and Music owned a
verified independent copy. Archive eviction only released the archive's local
bytes; it did not remove these separate staging files.

## Design

- Clean a completed job's private music staging subtree after checking the
  uploaded original archive and independent Music-owned copy.
- Preserve iCloud original archives, Music library files, durable task records,
  content deduplication metadata, and all incomplete/failed/cancelled staging.
- Persist cleanup completion and retry state separately from import completion.
  Recover pending cleanup on restart without downloading, converting or adding
  the track again; cleanup failure must not turn a completed import into failure.
- Bound traversal and anchor deletion to validated directory descriptors. Reject
  links, object replacements and paths outside the selected job directory.
- Exercise the lifecycle through mock Telegram/backend E2E, including recovery,
  safe retry, deduplication with absent source files and preserved failures.

## Validation

- Three mock Telegram/backend E2E tests call the actual filesystem cleanup core
  and cover successful cleanup, failed/cancelled retention, interrupted cleanup
  retries and content deduplication after the previous source has been removed.
- Manager tests cover durable completion before removal, restart/retry scheduling,
  legacy records and preservation of incomplete work and deduplication metadata.
- Filesystem tests cover links, unsafe paths, aliases, object replacement,
  partial deletion/retry and metadata-only platform flags. A live metadata-only
  probe left an existing evicted iCloud original dataless with zero local blocks.
- Final full workspace run with `test-runtime`: 1,503 main tests and 35 converter
  tests passed, zero failures; 17 pre-existing tests remained ignored. Existing
  Rust CI includes the new tests through its workspace/all-targets commands.
- Strict workspace Clippy, formatting and journal validation passed. The large
  completed-history fixture alone has a 30-second completion wait for loaded
  hosts; its assertions are unchanged and timeout includes state diagnostics.
- Telegram, Music and iCloud behavior in automated tests is mocked; no new live
  Music import or production feature deployment is part of this validation.

## Local Maintenance

- The separately authorized one-time cleanup removed 17 verified redundant
  originals, conversion files and early test copies for eight accepted tracks.
- All eight original archives were confirmed uploaded without cloud conflicts;
  the independent Music copies were hash-verified before removing staging.
- The production bot resumed with startup readiness and Cloud ingress
  reconciliation. The deployed executable was not replaced by this maintenance.
- Private state backups and the exact deletion manifest remain outside Cargo;
  user filenames, local paths and track identities are not committed here.
