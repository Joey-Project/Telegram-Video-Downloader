---
id: 20261113-mas01
title: Bound stack use during Music archive copy and verification
status: completed
created: 2026-11-13
updated: 2026-11-13
branch: codex/music-archive-stack-safety
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/69"
supersedes: []
superseded_by: ""
---

# Bound stack use during Music archive copy and verification

## Problem

Real fixed-profile FLAC acceptance after PR #68 reached verified lossless
conversion, then aborted with a Tokio worker stack overflow while archiving the
original. No Music add had occurred. The copy function retained a 1 MiB stack
buffer while readback verification called a hashing function with another 1 MiB
stack buffer. Their nested frames exceeded the debug worker stack. This shared
archive path also applies to the existing audio formats.

## Fix

Keep the existing 1 MiB I/O chunks on the heap in both functions. Preserve
create-only publication, full SHA-256/size verification and source-object
revalidation. No global thread-stack increase is required.

## Validation

- A 512 KiB worker-stack regression copies more than two I/O chunks, verifies
  complete bytes, size and SHA-256 through the nested readback path, and refuses
  an alternate source at an existing destination without changing its bytes.
- Workspace: main program 1,437 passed and 17 existing ignored; converter unit
  tests 23 passed and native codec E2E tests 6 passed. Test-runtime Music tests
  passed 74/74. Clippy with warnings denied and formatting checks passed.
- The fixed signed test executable reused its Music permission. The original
  real Telegram FLAC task recovered from the interrupted archive phase, passed
  archive verification and saved its first Music persistent ID. It reused its
  retained download and verified ALAC; no file was resent or converted again.
- The real task subsequently completed: Music reported `matched`, its independent
  local ALAC matched the prepared file's SHA-256, and the unchanged original FLAC
  was uploaded to iCloud Drive before local eviction. A private read-only proof
  independently checked these states, retained source/import/Music bytes and
  the three pre-existing accepted task/catalog bindings. Production deployment
  remains a separate gate.

The explicit Python-runtime installation step is documented for upgrades too;
updating a bot binary or converter does not update the separately installed
transfer helper.
