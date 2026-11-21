---
id: 20261121-lsc01
title: Keep current media views accurate with fewer cloud writes
status: completed
created: 2026-11-21
updated: 2026-11-21
branch: "codex/file-manager-refresh"
pr: ""
supersedes: []
superseded_by: ""
---

# Keep current media views accurate with fewer cloud writes

## Agreed behavior

- The Mac checks library availability and reconciles the command inbox on the
  existing hourly cadence. A full media scan becomes due 24 hours after each
  registered library's last successful complete scan. Manual refresh and local
  operation scans can satisfy that deadline sooner.
- A background snapshot is published only when its meaningful state changes.
  The last acknowledged comparison point survives restart. A scan or report
  timestamp and routine free-space movement alone do not require a D1 write.
  Explicit refresh still returns a current result and the UI distinguishes
  scan time from publication time.
- Verified managed moves carry the existing item identity into the destination,
  including metadata provenance. A retained history entry can show the previous
  library and path. When a move stops with both primary files present, each
  file has its own identity. Historical reconciliation uses completed move
  receipts and verified source and destination identities. Source IDs and
  size identify possible matches.
- Library and Needs organizing show available media by default. Missing,
  Unavailable and Moved history remain explicit status filters.

## Current State

- Before this change, a read-only production comparison found 287 retained
  missing records in Default after a successful scan. Source IDs and byte sizes
  produced candidates for 278 rows. The 210 video IDs in the verified layout
  migration report already resolve to present files, and the bounded local
  download-history inspection found no completed path hashes for the missing
  rows. Confirming another pair requires evidence of the move or content.

## Validation

- The 24-hour mock transport exercise makes one startup and 24 hourly inbox
  reads, with zero periodic snapshot manifests or chunk uploads.
- Nineteen focused Rust tests cover scan timing and upload recovery,
  migration stops, replay and round trips, and verified historical metadata
  repair in temporary libraries.
- Worker type checking, 457 Worker integration tests, 128 Mini App UI tests,
  four torrent widget tests, and dry-run packaging pass.
