---
id: 20261016-adm01
title: ASS Danmaku Defaults
status: completed
created: 2026-10-16
updated: 2026-10-16
branch: wip/ass-danmaku-defaults
pr:
supersedes: []
superseded_by:
---

# ASS Danmaku Defaults

## Changes

- Generated ASS defaults to three times each original font size, 70% opacity,
  and the upper half of the 1920-by-1080 script canvas. Existing configuration
  files inherit these defaults without adding keys.
- Expose `bilibili.danmaku.font_scale`, `opacity`, and `display_area` with finite
  range validation. Include the settings in saved group download requirements
  so incompatible plans require confirmation again.
- The pinned `bbdown-core` API has no styling options. Adapt only its reported
  ASS sidecars after downloading and before marking the download complete or
  publishing files. Recompute lane spacing and off-screen movement endpoints;
  relocate bottom-fixed comments to upper lanes and clip every event to the
  configured region. Preserve times, colors, escaped text, XML, and subtitles.
- Use the existing filesystem layer's bound-file identity checks and atomic
  replacement. Matching profile markers make retries idempotent; conflicting
  markers require regenerating the sidecar instead of scaling it again.
- Preserve upstream round-robin lane assignment. Dense comments can still
  overlap, and comments taller than the configured region are clipped.

## Validation

- Rust/Cargo 1.95.0: offline all-target tests pass (770 passed, 16 opt-in tests
  ignored), including configuration, generated layout, repeated processing,
  report-based staging integration, XML/subtitle preservation, and saved-plan
  requirement invalidation. Local mock APIs require loopback permissions.
- The additional opt-in pixel test passes with installed libass 0.17.4 through
  a temporary native RGB24 rendering adapter. White at 70% opacity renders
  near 178/255; all pixels below row 539 remain black even for oversized
  multiline text. The same text with full-screen, opaque settings produces
  visible lower-half pixels and reaches 255/255.
- The local FFmpeg 8.1.1 lacks the ASS filter, so pixel validation uses libass
  directly. No Apple TV or Infuse device playback was performed.
- Formatting, all-target Clippy with warnings denied, offline build, diff, and
  project journal validation pass. Build artifacts use isolated temporary
  target/build directories. The recorded live executable requirement remains
  `io.github.telegram-local-downloader.bot` with certificate root
  `c24fe0c91539bd95ba4081cd944a8e17053fb304`; it is not replaced or restarted.

## Scope

- Only future ASS generation uses the new defaults. Previously downloaded
  sidecars are not rewritten. Live bot deployment is separate from this change.
