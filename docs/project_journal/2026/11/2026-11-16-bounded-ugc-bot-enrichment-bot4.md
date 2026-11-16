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
- The SDK repin was tested on top of integration source `1e5c7e51c4f3a100c1db01810d6e3702be60613e`: the patched manifest and lockfile selected `f0cb37ce9821f28a7fb8fb2a238553ed9c0265cf`, and the SDK dependency compiled from that exact revision. Compared with the earlier `afd39a25` pin, the merged SDK changes only its project journal; SDK Rust sources and manifests are identical.
- A fresh test binary, `rust-cache/bounded/build/debug/deps/telegram_video_downloader-80b2f68028bfa173` (SHA-256 `62f55584f9b3de19aed78dca05a7fce11af370396b896b2110991d17c7fefe86`), passed the 20 Bilibili-core contract tests, 25 metadata-recovery tests, and 10 flat-collection publication E2Es (55 tests total, 0 failed, 0 ignored). The broader `flat_collection` filter also passed 12 tests, including those 10 E2Es.
- Strict workspace/all-target Clippy with `-D warnings` and `cargo fmt --all -- --check` passed after the repin.
- The project journal validator and `git diff --check` passed after this journal update.
- Tests used local mocks and temporary fixtures; no live Bilibili, Telegram, or Music service was called.

## Evidence
- Integration points: `src/bilibili_core.rs` and `src/downloader.rs`.
- SDK pin-update log: `bot4-pin-update-1794867862320431000.log`.
- Bilibili-core log: `bot4-newcore20-1794867882728242000.log`.
- Metadata-recovery log: `bot4-new-meta25-1794868121236492000.log`.
- Broad and exact collection/publication logs: `bot4-new-collection10-1794868148512931000.log`, `bot4-new-flatpub10-1794868173871722000.log`.
- Strict Clippy log: `bot4-strict-clippy-1794868193231830000.log`.
- Formatting log: `bot4-fmt-check-1794868308423669000.log`.
