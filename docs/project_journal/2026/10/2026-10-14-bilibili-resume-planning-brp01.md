---
id: 20261014-brp01
title: Bilibili Missing Resume Planning
status: completed
created: 2026-10-14
updated: 2026-10-14
branch: wip/bilibili-resume-plan
pr:
supersedes: []
superseded_by:
---

# Bilibili Missing Resume Planning

## Evidence

- The saved 203-episode task had stream confirmations for only 195 episodes.
  Missing selection skipped compatible local media during planning, but the
  persisted attempt required episodes one through seven to redownload.
- Execution bypassed reuse for those episodes and sent an unconfirmed stream
  fingerprint to the worker. Comparing that absent fingerprint with a freshly
  resolved plan deterministically paused episode one on every resume.
- The log contained six identical first-episode requirement errors. This
  evidence does not establish an upstream format or codec change or identify
  which earlier operation first set the redownload marker.

## Changes

- Read the accepted plan and scoped durable redownload markers before planning.
  Force stream probes for redownload targets even when local media exists.
- Preserve prior stream confirmations for compatible skipped episodes only
  when media requirements and episode identity remain compatible. Fresh probes
  remain authoritative for episodes that are actually resolved again.
- Stop before starting an episode worker if its reviewed plan lacks a stream
  confirmation. Persist its redownload marker and report the episode number,
  title, and need to resume planning instead of claiming a format changed.
- Treat new stream information as added knowledge when the old plan had no
  per-episode stream evidence. Accepting that information retains compatible
  completed outputs. Losing a confirmation, changing known formats, sizes,
  codecs, or global requirements still invalidates compatibility.
- Retain explicit redownload markers until publication and verification succeed.
  Newly added plan information still goes through the normal confirmation flow.

## Validation

- Rust 1.95.0: offline all-target tests passed with 762 passed, zero failed,
  and 15 existing opt-in tests ignored. Probes use localhost mock APIs and
  synthetic files; no production access key or media download was used.
- Coverage includes forced probes despite existing media, carrying only
  compatible accepted confirmations, pausing before an unconfirmed worker,
  scoped retry markers, and retaining completed outputs after newly resolved
  plan information. Existing format, exact-size, codec, and identity guards
  remain covered.
- Offline build and all-target Clippy with warnings denied passed. Formatting,
  diff, and project journal validation passed. Builds use an isolated target
  without replacing the live signed LaunchAgent executable.
