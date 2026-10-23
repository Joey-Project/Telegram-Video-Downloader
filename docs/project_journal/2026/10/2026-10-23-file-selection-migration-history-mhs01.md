---
id: 20261023-mhs01
title: File selection explanations and migrated source history
status: completed
created: 2026-10-23
updated: 2026-10-23
branch: "codex/moved-history-selection"
pr: ""
supersedes: []
superseded_by: ""
---

# File selection explanations and migrated source history

## Scope

- Explain why a retained library row cannot be selected, including on mobile, and distinguish selected, selectable, and skipped rows in group selection.
- Distinguish successfully migrated source history from unexplained missing media, preserve source records, and offer the verified destination in the Mini App.
- Keep provenance computation on the local Rust service; retain the existing Cloudflare transport and hibernating update architecture.

## Data and behavior contract

- Source history keeps its original physical-copy ID and missing presence. A successful move does not delete historical records or reuse their IDs for the destination.
- Migration status can expose an optional destination item ID derived from the primary file's saved target identity and relative path after publication and source removal complete.
- A snapshot may annotate a missing source as `moved_to` only when durable migration provenance connects it to an available target snapshot containing the exact present target ID and path. A matching title or byte size is insufficient.
- Rebuild annotations for each projection. Missing or unavailable targets, replaced objects, reappeared sources, and legacy records without the required identity evidence must not inherit a stale moved annotation.
- Use complete local migration provenance for annotation, independently of the bounded migration-history list shown in the cloud UI.
- Verified migrated-out source rows remain available as history, with a destination action, and are excluded from current organizing work. Ordinary missing rows remain visible for investigation.
- Disabled selections explain missing, unavailable, unknown, and non-operable library state without relying on a hover tooltip or desktop-only metadata. Group labels distinguish retained rows from selectable rows.

## Implementation

- `MigrationStatusItem.target_item_id` is optional for backward compatibility and derives from the same device/inode/path ID calculation used by the scanner. It is available only for a complete group's published primary file with a persisted target identity and completed source removal.
- Rust builds exact migration edges from the complete runtime migration map and resolves a present destination with a 64-hop/cycle guard. The cloud history display remains independently bounded to 32 jobs.
- Each library projection clears previous annotations before rebuilding them. Default-library deduplication still retains one consistent snapshot representation.
- The Mini App labels annotated source rows as Moved, explains that history is not selectable, and can switch libraries, clear filters, locate the destination page, and open the target details.
- Group labels expose selected/selectable/not-present counts, identify present-but-read-only rows, and separately explain items omitted by the 100-item cap. The cap notice is bound to its scope, page, and group.

## Validation

- Rust mock Telegram E2E performs A-to-B-to-C media/sidecar migrations, durable reloads and rescans, exact destination linking, and negative cases for missing/replaced/unavailable targets and reappeared sources.
- Mini App UI E2E covers mixed availability, visible reasons and ARIA associations, group counts, selection caps, organizing filters, and cross-library destination navigation.
- Complete local Rust suite passed: 1,224 passed, zero failed, 16 pre-existing ignored, using four test threads. The first default-concurrency run had one existing partial-task-migration timeout; that case passed alone in 7.47 seconds before the bounded full rerun. No timeout was increased or failure suppressed.
- Complete Worker/logic and Mini App suite passed: 310 logic/runtime tests and 12 UI tests. Existing CI globs include the new cases.
- Type and syntax checks, Clippy across all Rust targets with warnings denied, default and production-configuration Wrangler dry-runs, Rust formatting, diff whitespace checks, and journal validation passed. Production deployment evidence remains in local ignored operational records.
