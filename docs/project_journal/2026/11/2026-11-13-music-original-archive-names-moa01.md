---
id: 20261113-moa01
title: Preserve uploaded music filenames in original archives
status: completed
created: 2026-11-13
updated: 2026-11-14
branch: "codex/staging-cleanup"
pr: ""
supersedes: []
superseded_by: ""
---

# Preserve uploaded music filenames in original archives

## Problem

- The import manager passed an internal job/attempt filename into archiving
  instead of the original filename retained from the Telegram message.
- The archive name sanitizer discarded non-ASCII characters, losing titles in
  Chinese, Japanese, Korean, Russian and other supported languages.

## Scope

- Pass the original upload filename through the archive boundary.
- Retain Unicode titles, spaces and safe punctuation; sanitize filesystem
  separators and control characters and truncate on a UTF-8 boundary.
- Keep the full original-content SHA-256 suffix for collision separation and
  preserve the original format, including FLAC and WAV after ALAC conversion.
- Prefix archive retries with a short attempt discriminator so retained partial
  copies do not block a new archive attempt, including after filename truncation.
- Keep existing archive paths and catalog references valid. Do not rename old
  iCloud archives or add Music tracks merely to repair future archive names.

## Validation

- Manager tests verify the original Telegram filename reaches archiving and an
  archive retry retains both the original title and any previous partial copy.
- Native naming tests cover Chinese, Japanese, Korean, Russian, French and
  Italian, path separators, safe punctuation, UTF-8 byte limits and preserved
  retry discrimination after truncation.
- The real FLAC conversion/restart E2E verifies a multilingual original archive
  filename, unchanged original bytes and a saved ALAC import without a second
  conversion. Music, Telegram and iCloud upload are mocked in that test.
- The complete workspace suite and strict Clippy pass; see the companion
  [staging cleanup workstream](2026-11-13-retained-staging-cleanup-rsc01.md).
