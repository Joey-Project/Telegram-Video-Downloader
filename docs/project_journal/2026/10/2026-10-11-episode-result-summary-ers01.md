---
id: 20261011-ers01
title: Bilibili Episode Result Summaries
status: completed
created: 2026-10-11
updated: 2026-10-11
branch: wip/episode-result-summary
pr:
supersedes: []
superseded_by:
---

# Bilibili Episode Result Summaries

## Problem

A season worker failure reported only an HTTP 403 and a truncated log tail.
Episode completion events were visible as text, but ordinary season jobs did
not persist episode results. Users could not distinguish downloaded episodes,
the episode that failed, and entries that had never started.

## Behavior

- Initialize results from the exact selected plan indices, preserving sparse
  selections and reporting already-present skips separately.
- Send one initial structured snapshot and small per-entry updates across the
  existing worker progress channel. Preserve the existing message reuse and
  configured Telegram update interval; episode updates do not send messages.
- Track entry start, download completion, terminal entry failure, and the last
  reported file stage. A file retry does not itself fail an episode.
- Put partial counts and bounded failed-episode details before verbose error
  logs. Download completion remains distinct from verified publication; only
  successful outer finalization marks entries verified.
- Persist attempt results in backward-compatible optional queue metadata.
  Restart marks active entries interrupted. Retry clears the new attempt
  results, and stale generation updates cannot modify a newer attempt.
- Keep the current stop-on-terminal-error behavior and existing staging
  recovery policy. Counts describe reported stages, not a guarantee that
  partial files remain available or are playable.

## Validation

- Rust 1.95.0 all-target tests passed: 615 passed, 15 ignored. The offline
  build, strict Clippy with warnings denied, formatting, and journal validation
  passed with an isolated Cargo target.
- Regression coverage includes fragmented season progress records, sparse
  indices, repeated events, file retries, terminal message truncation,
  persistent results, restart, and stale updates after retry.
- Live deployment remains a separate signed installation gate.
