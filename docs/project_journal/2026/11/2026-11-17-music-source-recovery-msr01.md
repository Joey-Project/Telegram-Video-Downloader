---
id: 20261117-msr01
title: Stable Music source proofs and retained failure diagnostics
status: completed
created: 2026-11-17
updated: 2026-11-17
branch: "codex/music-source-recovery"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/81"
supersedes: []
superseded_by: ""
---

# Stable Music source proofs and retained failure diagnostics

## Summary

- Detect source-content divergence during admission, preserve useful pipeline
  errors locally, and provide explicit one-job recovery for changed local bytes.

## Current State

- Source capture compares paired full-file hashes on the same bound handle,
  with one bounded retry for differing content. Object identity, size, rooted
  path, cancellation and read failures remain independent checks. Metadata-only
  changes remain acceptable when the selected object and bytes stay stable.
- Snapshot copying still verifies the admitted digest. A change after admission
  fails closed and retains its private copy, including on a same-size in-place
  write. Retrying does not silently approve different bytes.
- Music failures retain a private, bounded and redacted diagnostic journal with
  stage, error chain and optional digest/size evidence. The writer rejects
  invalid or replaced control files and uses a nonblocking writer lock.
  Diagnostic errors never replace the primary failed-job state or user message.
- `--music-recover-local-source` previews or explicitly applies one approved
  old/new digest and size for a failed local job without a prior add intent,
  Music ID, prepared audio or archive. It requires the same filesystem binding,
  takes the existing owner lock, and applies an atomic job-specific transaction.
  It does not start the worker or contact Music/Telegram. Existing attempts,
  retained staging and unrelated jobs remain intact.
- A production incident demonstrated a stale admitted digest despite matching
  current source/staging bytes. Historical same-size concurrent writing remains
  a plausible cause, not a proven event, because the original error was lost.
  The independent Apple authentication/keychain wait cycle is an OS incident;
  these source-recovery changes do not claim to resolve cloud synchronization.

## Next Steps

- Continue controlled OS-level cloud-sync diagnosis separately from source
  recovery; do not treat a successful local Music add as cloud confirmation.

## Evidence

- Focused regressions cover stable capture/retry bounds, metadata-only changes,
  real content/object changes, cancellation, diagnostic privacy/retention/lock
  refusal, generic user errors, and preview/apply recovery boundaries.
- Full workspace validation passed: 1,734 bot tests, 27 converter unit tests and
  8 native converter E2E cases; 17 opt-in bot tests remain ignored. The bot suite
  includes mock Telegram/qBittorrent admission, selected imports, restarts,
  deduplication, responsive commands and retained staging. Evidence:
  `music-recovery-workspace-fresh-e2e-1794910433135780000.log`.
- Strict `cargo clippy --locked --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check`, diff whitespace and project-journal validation
  passed. No real Music imports or torrent mutations are part of these tests.
