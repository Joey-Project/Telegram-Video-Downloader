---
id: 20261118-lsm01
title: Recover legacy media source metadata from local evidence
status: completed
created: 2026-11-18
updated: 2026-11-18
branch: "codex/migration-followup-fixes"
pr: ""
supersedes: []
superseded_by: ""
---

# Recover legacy media source metadata from local evidence

## Agreed behavior

- Read numeric Bilibili IDs from older NFOs as AV IDs without a network lookup.
  Preserve existing NFO fields, explicit source types and authoritative source
  metadata. A numeric AV ID must not become a bare YouTube ID.
- Recognize the explicit trailing bracketed video ID convention inside a
  YouTube directory as a fallback. Filename-only evidence remains partial
  metadata until a portable NFO with title and source ID is saved.
- Missing historical catalog objects do not own current sidecar destinations.
  Present and unavailable objects retain conflict protection; keep history.

## Implementation

- Legacy Bilibili unique IDs are type-aware, whitespace-tolerant and validated
  as positive u64 values. Existing and canonical duplicates normalize together.
- Filename fallback requires a supported video suffix, YouTube directory
  context and a final bracketed identifier. Numeric-only strings and known
  resolution/framerate labels do not become YouTube identities. Authoritative
  NFOs and existing source IDs take precedence.
- Source-NFO reservation excludes only confirmed Missing objects. Existing
  ownership, content, object and access-policy checks remain active.
- Existing metadata-only preview/execution can append canonical source IDs to
  old NFOs and create a missing NFO from observed local title/ID, preserving old
  fields and requiring no media relocation or download.

## Validation

- All 198 file-manager tests pass. Coverage includes eight legacy NFO variants,
  invalid and ambiguous IDs, multilingual filenames, NFO precedence, historical
  replacement records, unavailable owners and existing shared-sidecar rejection.
- Real metadata-only recovery was read back through the existing reader.
  Independent inventories verified original media and unrelated NFOs unchanged,
  original XML fields preserved, and new NFO content matched observed source
  evidence. Runtime receipts remain private and uncommitted. Temporary NFO
  before-images were removed after the accepted current state was revalidated.
