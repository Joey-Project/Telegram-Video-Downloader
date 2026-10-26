---
id: 20261025-fcl01
title: Keep collection media and sidecars in one directory
status: completed
created: 2026-10-25
updated: 2026-10-26
branch: "wip/flat-collection-layout"
pr: "62"
supersedes: []
superseded_by: ""
---

# Keep collection media and sidecars in one directory

## Agreed behavior

- Store collection videos and their associated NFO, subtitles, danmaku and
  artwork directly in one collection directory, with unique related names.
- A multi-part video inside a collection does not introduce another directory.
  Standalone multi-part videos keep their own grouping directory.
- Preserve collection order and stable source identities. Filename or folder
  collisions never silently overwrite another media item or its sidecars.
- Existing nested collections are reorganized through an explicit preview and
  confirmation, retaining the current safe transaction and recovery behavior.
  Media bytes are preserved without downloading or transcoding them again.
- Deploying this feature does not automatically reorganize existing user media.

## Existing organizer contract

- Existing organize previews already drop the selected media's nested source
  parent and place its files directly into the target collection directory.
  Reuse the existing selection, organize, preview and confirmation controls.
- Only selected media and reliably associated sidecars move. Ambiguous/shared
  assets remain at the source; incomplete metadata can first be recovered with
  the existing metadata recovery flow.
- Moves are durable per file and resumable. A partial failure may leave a partial
  batch; retry the same preview/revision to continue. There is no batch-wide
  rollback or implicit expansion to unselected members of the collection.

## Implementation

- Normalize completed SDK staging output after media verification, mux cleanup
  and NFO generation. No-replace rooted renames preserve media bytes and remap
  the report and NFO paths before the existing publication transaction runs.
- New UGC and season group plans use layout version 1. Season folders use a
  bounded title and stable season ID, with the same reviewed directory witness
  and execution-time object binding as UGC collections.
- New subtitle-only, danmaku-only and cover-only `All` plans use the same versioned
  layout and reviewed directory binding through generic artifact publication.
  Only explicit artifact-only plans may complete without primary media; their
  files never become video-recovery evidence or primary-media receipts.
- Preserve coherent KeepBoth names for media and sidecars when original or
  partial target groups already exist. NFO shares the exact media basename;
  other sidecars retain their distinct language or artwork variants.
- Approved legacy tasks retain their original layout. A v1 season replan can
  recover legacy exact-path/hash attempt evidence and task-namespaced receipts;
  it cannot adopt another task's receipt or a generic unscoped media match.
  Final group validation retains the accepted legacy scope only for entries
  recovered from that exact evidence.
- Unmuxed video segments and audio tracks take their ordinals from the ordered
  SDK report, preserving playback order independently of directory enumeration.
  Shared entry sidecars follow the first primary segment's complete stem so the
  existing publication and identity index can keep their association.
- Collision handling remains per primary and its sidecars; partially occupied
  segment sets may have different KeepBoth suffixes. Generic Missing discovery
  recognizes the NFO-attached first segment as an existing candidate. Same-task
  publication receipts, rather than that candidate scan, validate the full set
  of published primary files.
- Unsupported layout versions fail explicitly. Logical and canonical macOS root
  spellings are resolved against the captured root, retaining rooted containment
  and identity validation.

## Download layout contract

- The pinned SDK creates an entry subdirectory even when the bot supplies a
  collection output directory. Flatten completed staged output only after SDK
  processing, local muxing, danmaku styling and NFO generation have finished.
- UGC collections and PGC seasons use one bound collection directory. Preserve
  standalone multi-part and single-video routing.
- Media stems include group order, actual part order and stable source identity,
  followed by a readable title within filename limits. All related files share
  that media stem and retain their distinct language or variant suffixes.
- Keep-both publication reserves one consistent name for the media and its
  sidecar group, including when a partial target group already exists.
- Saved plans bind the layout version. A legacy pending task retains its approved
  output recipe; changing to a new recipe requires explicit replan/confirmation.
  Exact same-task publication receipts remain eligible for verified recovery
  after confirmation. Existing files are never silently moved or adopted as a
  different task's new copy.

## Validation

- Formatting, strict all-targets Clippy and isolated release build pass with Rust
  1.99.1. Default-parallel all-targets tests pass: 1,343 passed, zero failures,
  16 existing ignored tests. The 42 Rust/Cargo inputs remained byte-identical
  across the final gates.
- Focused collection coverage passes 93 tests; the dedicated legacy season
  exact-evidence/tamper/namespace regression also passes.
- Artifact coverage passes 10 tests, including six collection/season and
  subtitle/danmaku/cover combinations with no primary media, normal video
  rejection without primary output, SDK identity consistency, legacy and unknown
  layout handling, directory replacement refusal and category-prefix binding.
- The mock Telegram cloud flow scans nested media, previews and confirms moves,
  persists a partial failure, reopens the same-epoch inbox/runtime and a fresh
  file manager, then resumes without duplicate moves or changed media bytes.
- Download integration covers nested SDK output, multiple direct collection
  videos, a multi-part child, coherent sidecar KeepBoth names, partial conflicts,
  group sequencing and explicit durable receipt verification. Regression cases
  also exercise complete legacy-season recovery and ordered unmuxed segment
  publication with shared sidecars, unrelated-identity KeepBoth conflicts,
  full-set receipt verification and first-segment Missing discovery through the
  real identity index. These use mocked SDK outcomes and local files rather than
  a live upstream media download.
- Cloud validation passes typechecking, 362 backend tests, 82 mounted UI tests
  and the Wrangler dry-run. The unchanged-snapshot storage test compares stable
  stored rows instead of nondeterministic query timing metadata; its write-budget
  assertion remains in place. No production Worker source changes.
- Existing `cargo test --all-targets` CI discovery includes both new test modules.
  Transient PR/CI/deployment receipts stay in ignored task artifacts.
