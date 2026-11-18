---
id: 20261118-mnv02
title: Preserve media identity fields in compact collection filenames
status: completed
created: 2026-11-18
updated: 2026-11-18
branch: "codex/media-naming-v2"
pr: ""
supersedes: []
superseded_by: ""
---

# Preserve media identity fields in compact collection filenames

## Agreed behavior

- Keep one directory per collection and omit its repeated title from member filenames.
- Use `P001 - Episode title - [source-id] - [1080p h264]` for an ordered collection member. Pad order to at least three digits and never truncate larger values. Standalone names omit the order prefix.
- Reserve order, stable source identity, known media specifications, matching attachment suffixes, extensions and collision suffixes before fitting the readable title. Shorten only that title, with an ellipsis on a complete grapheme boundary.
- Preserve full metadata titles and CID in NFO. Media, NFO and matching attachments use the same filename stem.
- Produce a fresh unified migration preview for registered libraries after the format changes. Previously migrated collections are included, and explicitly approved placeholder titles may use verified upstream titles. Actual moves and duplicate handling require separate confirmation.

## Implementation

- Share the title-fitting and structured-stem formatter between organization, new V2 collection recipes, bot-owned FFmpeg mux outputs and eligible single-video YouTube staging outputs.
- Preserve explicit V1 recipes and provider-owned paths so existing recovery records remain usable. Verify YouTube report aliases against the staged object before remapping publication paths and NFO references.
- Keep existing publication/recovery, content-stability, object-identity and access-policy protections. New-download naming happens before publication.

## Validation

- Formatting and workspace/all-targets Clippy with warnings denied pass.
- Full workspace tests pass with controlled concurrency (`--test-threads=8`): 1,755 bot tests passed with 17 ignored, plus native converter targets with 27 and 8 passing tests. The converter client without native dependencies passes 9 tests.
- Shared formatter and mock Telegram E2E tests cover multilingual graphemes, three-digit order, KeepBoth sidecars, unchanged media, complete NFO titles, V1/V2 publication and explicit-library download/relocation through verified filesystem aliases. Existing all-targets CI includes the new tests.

## Next Steps

- Generate a new private, read-only unified migration preview after the reviewed implementation is installed. Confirm its concrete scope before moving existing media.
