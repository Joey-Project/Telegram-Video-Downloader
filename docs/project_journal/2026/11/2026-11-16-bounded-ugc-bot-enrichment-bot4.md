---
id: 20261116-ugc-bot4
title: Bounded UGC enrichment in metadata recovery
status: completed
created: 2026-11-16
updated: 2026-11-16
branch: codex/bounded-metadata-resolution
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/79"
supersedes: []
superseded_by: ""
---

# Bounded UGC enrichment in metadata recovery

## Summary
- Metadata recovery opts into the bounded BBDown-rust APIs only for optional UGC membership and collection-order enrichment.
- Membership and full collection calls retain separate resolver budgets; a refused optional lookup preserves the base metadata or the already verified membership.

## Current State
- Bot options cap each response at 1 MiB, each full collection resolution at 64 requests, raw archives and expanded pages at 512 each, and returned strings at 4 MiB.
- `Cargo.toml` and `Cargo.lock` pin `bbdown-core` to merged BBDown-rust commit `f0cb37ce9821f28a7fb8fb2a238553ed9c0265cf`.
- Membership lookup is used by video-page selection and metadata recovery. Full collection order is requested only by optional metadata recovery enrichment.
- Membership failure, limit refusal, or timeout keeps base video metadata. Collection-order failure, limit refusal, or timeout keeps verified membership without order.
- These limits cover the UGC membership and collection-resolution calls. The source-video probe and other metadata paths keep their existing behavior.

## Coverage
- `bilibili_core` covers the bot option values and rejects an oversized Content-Length through a raw TCP header-only fixture before body or JSON parsing.
- Metadata recovery tests cover optional membership and collection failures, timeouts, oversized groups, verified-membership fallback, and exact order mapping.

## Validation
- Integration source `f166c810a425d5e56b91bd5542eba9b53d1b4513` includes the parent's custom-name KeepBoth repair and pins both manifest and lockfile to `f0cb37ce9821f28a7fb8fb2a238553ed9c0265cf`. Compared with the earlier `afd39a25` pin, the merged SDK changes only its project journal; SDK Rust sources and manifests are identical.
- One fresh test binary, `rust-cache/bounded/build/debug/deps/telegram_video_downloader-80b2f68028bfa173` (SHA-256 `46fe027241b2af3455cf852c5871dcc790655f991b8770bee1d2769dafb356e1`), passed both new KeepBoth regressions, all 193 file-manager cases, 20 Bilibili-core contract tests, 25 metadata-recovery tests and 10 flat-collection publication E2Es. Each filter had zero failures and zero ignored tests; overlapping filters are not counted as unique tests.
- Strict workspace/all-target Clippy with `-D warnings` and `cargo fmt --all -- --check` passed on that combined source.
- The project journal validator and `git diff --check` passed after this journal update.
- Tests used local mocks and temporary fixtures; no live Bilibili, Telegram, or Music service was called.

## Evidence
- Integration points: `src/bilibili_core.rs` and `src/downloader.rs`.
- SDK pin-update log: `bot4-pin-update-1794867862320431000.log`.
- New KeepBoth and file-manager logs: `p4-review3f9-1794870073636520000.log`, `p4-file-manager193-1794870154639312000.log`.
- Bilibili-core log: `p4-bilibili20-1794870209765384000.log`.
- Metadata-recovery log: `p4-metadata25-1794870216762598000.log`.
- Collection/publication log: `p4-flat-collection10-1794870245288633000.log`.
- Strict Clippy log: `p4-strict-clippy-1794870254032890000.log`.
- Formatting log: `p4-fmt-check-1794870298415808000.log`.
