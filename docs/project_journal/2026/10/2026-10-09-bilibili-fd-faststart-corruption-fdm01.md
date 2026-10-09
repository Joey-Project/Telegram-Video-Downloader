---
id: 20261009-fdm01
title: Bilibili descriptor mux corruption
status: completed
created: 2026-10-09
updated: 2026-10-09
branch: codex/fix-bilibili-fd-faststart
pr:
supersedes: []
superseded_by:
---

# Bilibili Descriptor Mux Corruption

## Cause and Change

- The bot disables BBDown-rust's mux and performs its own descriptor-bound FFmpeg mux. Combining `fd:` output with `-movflags +faststart` reopens a duplicate descriptor during the second-pass data move. Its shared file offset corrupts the media even when FFmpeg returns success.
- FFmpeg 8.1.1 reproduced the failure with synthetic H.264/AAC media. Path output with faststart and descriptor output without faststart both decoded successfully; descriptor output with faststart lost the `mdat` header and failed decoding.
- Keep descriptor-bound inputs and output, staging, publication, and cleanup unchanged. Remove faststart from the shared DASH/FLV descriptor-output command; the MP4 index remains at the end for local playback.
- The supplied 17.65-second file's hash matched its completion record. Repairing chunk offsets and the `free`/`mdat` headers in a temporary copy still failed decoding: the media was overwritten, not merely misindexed. Complete recovery requires retained raw streams or a new download.

## Validation

- DASH and FLV command tests reject faststart on descriptor output.
- A real-FFmpeg opt-in test generates separate H.264/AAC streams, runs the production mux and cleanup, compares both encoded stream hashes, and fully decodes the published MP4.
- Rust/Cargo 1.95.0: offline locked build, Clippy for all targets with warnings denied, formatting, diff checks, and project-journal validation passed.
- The real-FFmpeg regression passed with FFmpeg 8.1.1. The serial full Rust suite passed: 537 passed, 12 ignored; the real-FFmpeg test is explicitly opt-in.
- The first parallel full run timed out in the existing collection-progress Telegram mock test (536 passed, 1 failed, 12 ignored). That test passed alone in 1.49 seconds, and the serial full suite passed in 90.13 seconds. No unrelated progress or timeout logic was changed.
