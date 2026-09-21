---
id: 20260921-live-download-resolution-summary-lrs01
title: Live Download Resolution Summary
status: completed
created: 2026-09-21
updated: 2026-09-21
branch: wip/live-download-resolve-summary
pr: 16
supersedes: []
superseded_by:
---

# Live Download Resolution Summary

## Summary
- Telegram live download status now retains the resolved media plan alongside subsequent download, file-growth, mux, and move updates.
- Bilibili plan summaries report entry count, selected video/audio properties, and conservative expected media size from the selected `bbdown-core` streams.
- YouTube summaries report the selected yt-dlp formats when available and fall back to top-level metadata without inventing a file size.

## Current State
- Exact upstream sizes render directly; approximate sizes render as `about`, mixed known/unknown streams render as `at least`, and entirely unavailable sizes render as `unknown`.
- The Bilibili worker carries the summary to its parent through an internal JSON wire record, so the status survives the process boundary.

## Next Steps
- No follow-up is required for this workstream after the PR is merged and the LaunchAgent is restarted with the release build.

## Evidence
- Unit coverage exercises selected Bilibili DASH streams, multi-entry partial size data, yt-dlp split formats, unknown-size behavior, progress-context persistence, and Bilibili worker propagation.
- GitHub Codex review on PR #16 identified the fallback-delivery omission; the fallback now reuses the rendered progress text and has a dedicated regression test.
