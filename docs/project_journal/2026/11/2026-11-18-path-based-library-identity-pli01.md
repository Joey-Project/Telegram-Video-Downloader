---
id: 20261118-pli01
title: Resolve media libraries by their registered paths after reboot
status: completed
created: 2026-11-18
updated: 2026-11-18
branch: "wip/path-based-library-identity"
pr: ""
supersedes: []
superseded_by: ""
---

# Resolve media libraries by their registered paths after reboot

## Agreed behavior

- The registered normalized absolute path is the library's cross-restart source
  of truth. Persisted device/inode values must not prevent reopening that path
  after a reboot or replace its logical registration identity.
- Device/inode comparisons protect the currently opened object during live queue
  and file operations. Replacement during active work must still fail without
  adopting or mutating the replacement tree. Child-entry churn and timestamps
  alone do not replace a root directory.
- Missing, unreadable and offline roots remain registered with an unavailable
  status. Reopening must not manufacture a missing library directory.
- Preserve existing library IDs, paths, defaults, categories, task metadata and
  existing registration nonces. Do not introduce a persistent volume-UUID gate
  or require users to re-register a library solely for a device-number change.

## Implementation

- Physical root bindings live only in the current registry-store session and
  are established before queue ownership and recovery. Legacy serialized
  physical fields are ignored and omitted from subsequent registry writes.
- Versioned path-based registration tokens retain explicit re-registration
  invalidation. The Worker supports old and new token formats during rollout;
  the new Mac compares the current token before accepting a scoped operation.
  Old v1 requests must be regenerated rather than applied under a new binding.

## Validation

- Five regression tests pass for stale legacy physical metadata across queue
  reopen, retained registrations/tasks, unavailable roots, old-token rejection
  through mock Telegram, and live directory/symlink replacement. Benign child
  churn remains accepted while replacement trees remain untouched.
- All 42 snapshot-budget tests pass, including v2 receipt replay and compaction;
  unsupported tokens retain only a nonconfirmable omission placeholder.
- Worker/Mini App validation passes: TypeScript typechecking, 457 Worker tests,
  126 UI tests, four torrent-widget tests, and Wrangler dry-run packaging.

## Operational scope

- These checks use isolated fixture roots and mock Telegram. They do not reboot
  the Mac, modify production registrations, apply pending metadata previews, or
  migrate media. Per-file transfer and publication witnesses remain intact;
  an interrupted operation can still require a fresh preview after reboot.
