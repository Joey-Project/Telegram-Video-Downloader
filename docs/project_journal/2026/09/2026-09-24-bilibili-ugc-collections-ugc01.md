---
id: 20260924-ugc01
title: Bilibili UGC Collection Downloads
status: completed
created: 2026-09-24
updated: 2026-09-24
branch: wip/bilibili-ugc-collections
pr:
supersedes: []
superseded_by:
---

# Bilibili UGC Collection Downloads

## Summary
- Upgrade the pinned `bbdown-core` dependency to `v0.6.0` (`0a94b071`), which exposes explicit UGC collection-membership resolution.
- Extend Bilibili jobs so a normal BV video that belongs to a UGC collection can be downloaded as the current video or the complete collection after explicit Telegram confirmation.
- Support direct Bilibili collection and series URLs, with all items published below a stable owner/title/kind/id directory.

## Current State
- The dependency upgrade is present locally and passed `cargo test`, `cargo fmt --check`, and `cargo clippy --all-targets -- -D warnings` before this workstream began.
- A positive membership probe was verified against `BV1kk4y1T7cd`: collection `167822`, title `东方钢琴单曲集`, owner `Satori旅人`, 37 items. `BV12TRrBcEP8` is a negative single-video control.
- Normal BV membership now presents `Current video`, `Entire collection`, and `Cancel`. Direct UGC collection and series URLs present `Entire collection` and `Cancel`.
- Confirmed collection jobs resolve all entries, recursively inventory NFO/info sidecar identities in the target collection directory, and download only missing items through the existing staging and atomic-publication flow.
- Collection output is named `<UP主> - <合集名> [collection-<id>]` or `[series-<id>]`; unsafe or oversized components are normalized, and an unavailable owner name falls back to `UP-<mid>`.
- The live progress message reports collection total, current item, already-present items, and completed downloads. Single-video, YouTube, and PDF behavior remains unchanged.

## Plan
- Add explicit collection job data and generalized confirmation callbacks for PGC episodes, BV membership, and direct UGC URLs.
- Resolve collection entries through `bbdown-core`, inventory existing sidecars inside the target collection directory, and request only missing entry indices.
- Publish into `<UP主> - <合集名> [collection-<id>]` or `[series-<id>]`, with a stable `UP-<mid>` fallback when an owner name is unavailable.
- Report collection total, current item, completed items, and skipped existing items through the existing live Telegram progress message.
- Preserve single-video behavior when membership is absent or cannot be resolved, and leave YouTube/PDF routing untouched.

## Next Steps
- Run a deployed Telegram smoke test with the positive and negative BV controls plus a direct collection URL when the local bot is updated.

## Evidence
- Upstream release: `bbdown-core v0.6.0` at `0a94b071bbc1897ec1d1fec9dfcf7883c5754a15`.
- Related migration workstream: `docs/project_journal/2026/06/2026-06-18-bbdown-rust-migration-bbd04f.md`.
- Rust validation after implementation: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` (`437 passed`, `10 ignored`).
- Python validation: `uv run ruff format --check`, `uv run ruff check`, and `uv run python -m unittest discover -s tests` (`20 passed`).
