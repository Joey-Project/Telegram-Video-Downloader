---
id: 20261009-fdm01
title: Bilibili descriptor mux corruption
status: completed
created: 2026-10-09
updated: 2026-10-10
branch: codex/fix-bilibili-fd-faststart
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/30
supersedes: []
superseded_by:
---

# Bilibili Descriptor Mux Corruption

## Cause and Change

- The bot disables BBDown-rust's mux and performs its own descriptor-bound FFmpeg mux. Combining `fd:` output with `-movflags +faststart` reopens a duplicate descriptor during the second-pass data move. Its shared file offset corrupts the media even when FFmpeg returns success.
- FFmpeg 8.1.1 reproduced the failure with synthetic H.264/AAC media. Path output with faststart and descriptor output without faststart both decoded successfully; descriptor output with faststart lost the `mdat` header and failed decoding.
- Remove faststart from the shared DASH/FLV descriptor-output command; the MP4 index remains at the end for local playback.
- Fully decode every new Bilibili and YouTube media output with FFmpeg before publication and task completion. Bilibili mux output stays private and raw streams remain available until verification succeeds. Decode errors, command failure, timeout, missing decoded output, or content changes reject the output.
- Bind verification to the opened file object and compare SHA-256 before and after decoding. Rewind duplicated descriptors before reading; timestamp changes and directory child-entry churn alone do not invalidate unchanged content. Revalidate the published pathname identity separately before accepting path-based inputs.
- Show decoded media time during verification. Artifact-only downloads do not run audio/video decoding.
- The supplied 17.65-second file's hash matched its completion record. Repairing chunk offsets and the `free`/`mdat` headers in a temporary copy still failed decoding: the media was overwritten, not merely misindexed. Complete recovery requires retained raw streams or a new download.

## Validation

- DASH and FLV command tests reject faststart on descriptor output.
- Real-FFmpeg opt-in tests decode standalone video/audio streams and the production mux output, compare both encoded stream hashes, and reject a corrupt output even when mux returned success while retaining its raw streams.
- Playback-gate tests reject failed or empty decoding, accept benign timestamp/child-entry churn, and reject same-size content mutation or object replacement.
- Rust/Cargo 1.95.0: offline locked build, Clippy for all targets with warnings denied, formatting, diff checks, and project-journal validation passed.
- The real-FFmpeg regressions passed with FFmpeg 8.1.1: 2 passed. The final serial full Rust suite passed: 549 passed, 13 ignored. Formatting, Clippy with warnings denied, and diff checks passed.
- The first full run was blocked by sandbox restrictions on localhost mock servers and macOS file coordination. The final suite ran with those operations permitted and no production-service access.
