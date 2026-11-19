---
id: 20261118-mnv02
title: Preserve media identity fields in compact collection filenames
status: completed
created: 2026-11-18
updated: 2026-11-19
branch: "codex/media-naming-v2"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/84"
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
- Reserve synthesized NFO extensions before fitting names. Skip only inferred canonical source candidates outside the filename-component policy; real attached and authoritative NFO paths retain their original ownership and read guards.
- Recognize structured YouTube, BV and AID filename identities without sidecars. Keep Bilibili filename-only evidence separate from CID/episode overwrite authority.
- Share structured source parsing with library discovery and source-ID-only hint matching. Explicit source fields take precedence over marker-like title text; real NFO aliases and legacy YouTube directory-context rules remain available.
- Size fresh local mux names from actual downloaded attachment tails before reserving output. Use one 10,000-candidate KeepBoth bound for publication, organization and parsing, with a derived maximum suffix budget. Exhaustion reports a recoverable failure.
- Keep existing publication/recovery, content-stability, object-identity and access-policy protections. New-download naming happens before publication.

## Validation

- Formatting and workspace/all-targets Clippy with warnings denied pass.
- Full workspace tests pass with controlled concurrency (`--test-threads=2`): 1,766 bot tests passed with 17 ignored, plus native converter targets with 27 and 8 passing tests. The converter client without native dependencies passes 9 tests.
- Shared formatter and mock Telegram E2E tests cover multilingual graphemes, three-digit order, KeepBoth sidecars, unchanged media, complete NFO titles, V1/V2 publication and explicit-library download/relocation through verified filesystem aliases. Existing all-targets CI includes the new tests.
- Additional regressions exercise actual NFO creation while renaming a 255-byte `.ts` source, and filename-only duplicate detection for both providers while preserving overwrite restrictions and legacy names.
- Real library scans cover unhinted discovery and source-ID-only recovery. Mock FFmpeg mux-to-publication tests cover long subtitle tails, KeepBoth, NFO references and metadata preservation; reserved targets exercise bounded exhaustion.
- An eight-thread local run encountered the existing staging-cleanup test's 30-second management wait timeout. Its isolated repeat passed in 1.05 seconds, and the complete two-thread run passed with the same assertion and deadline.

## Next Steps

- Generate a new private, read-only unified migration preview after the reviewed implementation is installed. Confirm its concrete scope before moving existing media.
