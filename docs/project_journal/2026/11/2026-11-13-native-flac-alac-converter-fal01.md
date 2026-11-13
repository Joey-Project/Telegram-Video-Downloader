---
id: 20261113-fal01
title: Isolated native FLAC to ALAC converter
status: completed
created: 2026-11-13
updated: 2026-11-13
branch: codex/native-flac-alac-converter
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/67"
supersedes: []
superseded_by: ""
---

# Isolated native FLAC to ALAC converter

## Agreed scope

Build a standalone Rust crate and executable that directly uses official libFLAC
and Apple's AudioToolbox/Core Audio APIs. Audio conversion, M4A metadata migration,
read-back verification and structured progress/results belong to one isolated
worker process. A typed Rust client handles process supervision and cancellation
without linking native audio libraries into its caller.

Prefer bit-exact audio. Any necessary supported compatibility transformation must
be explicit, reported as a warning and verified against its intended result;
unplanned sample differences or damaged input remain errors. Keep original files
unchanged. The bot's eventual iCloud archive must contain the original upload,
not its ALAC derivative. Failed/cancelled private staging is retained.

Read all source metadata; map known fields to standard Music tags, preserve
original values in namespaced freeform audit items, preserve picture bytes and
descriptors, and report data that cannot be represented. Music need not display
every preserved field. Original FLAC remains the complete archival source.

## Interface and validation plan

- Versioned JSON Lines request/event protocol, bounded input/output and runtime,
  explicit cancellation, private same-filesystem staging, no destination clobber.
- A default native tool plus a client-only library feature selection with no
  libFLAC or Apple framework linkage.
- Real macOS codec E2E using generated fixtures and an independent PCM verifier;
  test 16/24/32-bit samples including nonzero low bits, multiple rates/layouts,
  damaged input, existing targets, source preservation and cancellation.
- Chinese, Japanese, Korean, Russian, French and Italian in paths and tag values,
  mixed scripts, decomposed accents, emoji and repeated values. Preserve exact
  Unicode codepoints rather than normalizing or using lossy replacement.
- Keep private real-song acceptance outside tracked fixtures. Previous CLI-only
  experiments are reference evidence, not proof of the new native wrapper.

## Delivery boundary

This workstream implements the independent converter and its tests first. Telegram
ingress integration, Music library additions and production deployment follow only
after the standalone tool has passed its acceptance gates.

## Implementation

`crates/flac-alac-converter` contains the standalone executable, native backend,
bounded metadata migration and client-only Rust API. The worker accepts one
versioned JSON Lines conversion request and optional cancellation. It publishes
only after source revalidation, metadata read-back and full decoded PCM SHA-256
verification. Private staging is explicitly created with mode 0700 even for
in-process callers; retained failures expose staging and existing partial paths.

The first version supports mono/stereo and a known nonzero FLAC sample count.
It preserves rate and normalized sample values, including all 32-bit low bits.
ALAC uses 16/20/24/32-bit source precision; supported promotions require an
explicit compatibility option and warning. It does not implement resampling,
downmixing or a lossy fallback. Multichannel and unknown-length inputs are
explicitly rejected rather than silently changing audio or speaker roles.

Valid comments use exact original keys under `org.joey.flac-original`; raw
internal, invalid, picture and opaque data use `org.joey.flac-original.raw` to
avoid key collisions. Metadata writes first relocate a bounded MP4 movie index
to the tail while leaving media bytes and absolute chunk offsets in place.
This avoids mp4ameta's full-media buffering for the original front-index layout.
Fragmented or oversized unsupported layouts fail with their staging retained.

CI now installs libFLAC/pkg-config before native workspace builds, runs workspace
Clippy/tests and exercises the client-only feature selection. The crate README
documents protocol, limits, native dependencies and the separate integration step.

## Validation

- Native crate: 20 unit tests and six real-codec E2E tests passed on macOS with
  libFLAC 1.5.0. The E2E cases include 8-bit lossless promotion/strict rejection,
  16/24/32-bit independent PCM comparison, multilingual standard/audit fields,
  raw invalid text, PNG artwork, source changes, cancellation and no-clobber.
- Workspace Clippy passed with warnings denied. The full workspace test run
  passed, including 1,426 main-program tests (17 existing ignored tests) and
  the new native converter suites. Client-only tests cover stalled
  workers, blocked request pipes, bounded diagnostics and protocol validation.
- The approved private 24-bit/48 kHz/stereo song passed through the new native
  worker. Independent system decoding matched all 12,581,120 PCM frames; all
  24 original comment values and the 2,135,900-byte JPEG were preserved. The
  actual iCloud original's SHA-256 remained unchanged. Private evidence and the
  resulting M4A remain under ignored `.codex-tmp` paths, not tracked fixtures.
- macOS codec tests require normal system codec-service access: the restricted
  sandbox could not enumerate ALAC even through `afconvert`. Acceptance ran in
  the approved non-sandboxed test runner. System audio was verified muted.

## Next integration step

Wire the converter into the existing music job path, bundle/sign/install its
helper, and retain the original FLAC in the iCloud music archive. Verify Music
uses its own local ALAC copy before evicting the archived original's local data.
This standalone-tool change does not itself enable FLAC uploads in the live bot.
