---
id: 20261118-nfc01
title: Keep nested macOS file coordination within one operation purpose
status: completed
created: 2026-11-18
updated: 2026-11-18
branch: "codex/migration-followup-fixes"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/83"
supersedes: []
superseded_by: ""
---

# Keep nested macOS file coordination within one operation purpose

## Agreed behavior

- NFO replacement may persist its durable recovery checkpoint while holding
  coordinated access to the associated media. Nested access must not wait for
  its own enclosing operation to release that access.
- Preserve coordination between unrelated operations, including operations on
  other threads. Keep filesystem identity, content and access-policy checks,
  and persist the checkpoint before publishing the NFO replacement.

## Implementation

- Give each outer synchronous File Provider operation a unique purpose ID.
  Nested read, write and move accessors on that thread inherit the purpose.
  Each fresh NSFileCoordinator receives the purpose exactly once.
- A thread-local RAII scope releases the inherited purpose on completion,
  errors and unwinding. Coordinators and autoreleased objects retain their
  existing per-operation lifetimes.
- This follows Apple's documented use of
  [purposeIdentifier](https://developer.apple.com/documentation/foundation/nsfilecoordinator/purposeidentifier)
  for cooperating subsystems in one high-level operation. A global shared
  purpose would incorrectly exempt unrelated operations from coordination.

## Validation

- Seven native macOS tests pass: nested read/write/move, distinct later and
  cross-thread purposes, unwind cleanup, and existing autorelease-pool checks.
- Nineteen checkpoint tests pass, including restart recovery, target/stage
  replacement, content/access-policy changes, and uncheckpointed orphan stages.
- An interrupted native metadata batch resumed through its original persisted
  preview and completed the remaining updates after revalidating moved files
  and existing replacement checkpoints.

## Operational scope

- Migration receipts, filesystem inventories and incident stack samples remain
  private and uncommitted. Library registrations and media paths
  are not part of this source change.
