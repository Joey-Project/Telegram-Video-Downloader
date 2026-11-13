---
id: 20261113-mwh01
title: WAV imports and converted Music record handoff
status: completed
created: 2026-11-13
updated: 2026-11-13
branch: codex/music-wav-record-handoff
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/70"
supersedes: []
superseded_by: ""
---

# WAV imports and converted Music record handoff

## Agreed scope

Support uploaded PCM WAV through the existing isolated lossless ALAC worker.
Retain the unchanged WAV in the original iCloud music archive and migrate metadata
best-effort. Preserve sample values/rate/channels; report reversible precision
promotion and reject unsupported input without silently changing the audio.

Extend the explicit accepted-record handoff to carry the complete prepared-audio
record, including different original/import hashes and conversion proof. Reuse
the saved Music persistent ID; do not convert again or add another track. Keep
legacy accepted M4A proof support and preserve all existing jobs/catalog records.

## Implementation plan

1. Native RIFF/WAVE decoding, bounded metadata preservation and real-codec tests.
2. WAV Telegram/transfer/native-preparation admission and original archive guards.
3. Converted accepted-record verification, read-only preview, atomic conflict-safe
   batch seeding and repeat-safe existing-record handling.
4. Mock Telegram/Music plus real converter E2E across interruption/restart,
   compatibility regressions, documentation and isolated validation.

## Validation boundary

Normal tests use generated audio and mock Music; they do not play audio, add
tracks, contact the production bot or install/restart services. Any real operator
handoff must stop the catalog owner, retain rollback/state backup, use the actual
fixed signed executable and verify its read-only Music authorization separately.
Conversion acceptance and iTunes Match cloud behavior remain independent.

## Implementation

The native worker identifies FLAC/WAVE from snapshot bytes under its unchanged
protocol. Its bounded RIFF parser supports mono/stereo integer PCM 8/16/24/32;
the existing AudioToolbox encoder and independent decoded-PCM verifier guard the
ALAC output. INFO values map to standard Music fields and exact values/raw
non-audio chunks use separate WAVE audit namespaces. Unsupported float/container/
precision/channel layouts fail explicitly. Oversized WAVE metadata is reported
instead of silently dropped, and the original remains the archival source.

Telegram admission, Rust/Python transfer allowlists, native preparation, archive
validation and task status support WAV without changing existing audio formats.
Preparation reports retain separate source/import identities and restart/retry
continues using saved prepared audio. A new separate same-content Telegram upload
still follows existing preparation-before-catalog-lookup behavior; catalog reuse
prevents another Music add.

Accepted schema-1 items can contain typed `prepared_audio`. The operator command
rechecks retained source/import hashes, complete report bindings, the saved Music
PID and independent local copy, and stable uploaded archive status. Legacy M4A
items retain their receipt-backed route. Seed-only opening takes the existing
owner lock without job recovery. A preview checks conflicts without rewriting
jobs/catalog; apply is one transaction. Exact replay preserves the old entry,
while conflicting source/import/report/PID or archive-path bindings fail. Fresh
hydration flags do not masquerade as an identity conflict. No add, conversion or
Telegram polling is part of the handoff.

## Validation

- Native unit tests: 27 passed. Real-codec E2E: eight passed, covering bit-exact
  WAVE widths, Chinese/Japanese/Korean/Russian/French/Italian metadata, opaque
  chunks, malformed/unsupported inputs, cancellation and unchanged originals.
- Bot suite: 1,448 passed with 17 existing ignored tests. The real-WAV/mock
  Telegram/Music case covers pre-add interruption, restart, one saved conversion,
  original archive bytes and duplicate-content reuse without a second Music add.
- Proof-verification tests use mock Music and real temporary-byte hashing;
  batch tests cover preview, atomic apply, replay and no partial mutation on
  later invalid/conflicting records. Native retained-file tests reject changed
  content and FIFO sources without following links or playing audio; the
  validator also refuses dataless inputs instead of materializing them implicitly.
- Client-only process library: nine tests passed without native dependencies.
- Pinned Python 3.14.5 transfer suite: 28 passed, including WAVE destinations.
- Test-runtime Music regression: 85 passed. Workspace/all-targets Clippy passed
  with warnings denied; formatting and project-journal validation passed.

## Operator boundary

Normal tests do not deploy, contact the production bot or add Music tracks.
The command supports either fixed signed profile with its matching config;
production application, state backup and startup/Cloud readiness remain explicit
operator steps. One-time private proofs, source identities and local acceptance
receipts stay outside tracked fixtures and PR content.
