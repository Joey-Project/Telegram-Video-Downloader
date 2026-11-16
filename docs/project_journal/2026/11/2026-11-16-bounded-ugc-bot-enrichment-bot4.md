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
- Membership lookup is used by video-page selection and metadata recovery. Full collection order is requested only by optional metadata recovery enrichment.
- Membership failure, limit refusal, or timeout keeps base video metadata. Collection-order failure, limit refusal, or timeout keeps verified membership without order.
- These limits cover the UGC membership and collection-resolution calls. The source-video probe and other metadata paths keep their existing behavior.

## Coverage
- `bilibili_core` covers the bot option values and rejects an oversized Content-Length through a raw TCP header-only fixture before body or JSON parsing.
- Metadata recovery tests cover optional membership and collection failures, timeouts, oversized groups, verified-membership fallback, and exact order mapping.

## Validation
- At integration source HEAD `60d583a9433cbda2a229d97ae6f5782ca29262e5`, `Cargo.toml` and `Cargo.lock` pin `bbdown-core` to `afd39a25ed32421bfb6f5c51bb7733153e3db915`. `cargo test --locked --workspace bilibili_core -- --test-threads=2` passed 20 tests (0 failed, 0 ignored), including the oversized response-header rejection and bot quota contract.
- The same fresh test binary, `rust-cache/bounded/build/debug/deps/telegram_video_downloader-b88d3c8f19aac0a5` (SHA-256 `82cf675db133679fb52851df36fbbd7ee6301f213d90d0264dbdcfa39d7a70ae`), passed 25 metadata-recovery tests, 4 maximum-name/ownership regressions, 7 torrent Mini App cases, 3 projected-layout/explicit-ownership migrations, 4 secondary-NFO/danmaku-alias regressions and 10 flat collection/publication E2Es. With the core filter, all 73 affected tests passed with zero failures and zero ignored tests.
- Strict workspace/all-target Clippy with `-D warnings` and `cargo fmt --all -- --check` passed at this source HEAD.
- The project journal validator and `git diff --check` passed.
- Tests used local mocks and temporary fixtures; no live Bilibili API was called.

## Evidence
- Integration points: `src/bilibili_core.rs` and `src/downloader.rs`.
- Membership and bot-quota log: `b4-final-core-20-1794863242592531000.log`.
- Metadata-recovery log: `b4-final-meta25-1794863401009416000.log`.
- Maximum-name/ownership and Mini App logs: `b4-final-review4-1794863425936862000.log`, `b4-final-miniapp7-1794863440106463000.log`.
- Projected-layout migration logs: `b4-final-legacy-skip1-corrected-1794863484250798000.log`, `b4-final-legacy-leaf1-1794863492780663000.log`, `b4-final-explicit-cover1-1794863501072164000.log`.
- Secondary-NFO logs: `b4-final-secondary-nfo-skip1-1794863517625886000.log`, `b4-final-secondary-nfo-stable1-1794863525087108000.log`.
- Danmaku alias/ownership logs: `b4-final-danmaku-alias1-1794863539412289000.log`, `b4-final-danmaku-secondary1-1794863550692715000.log`.
- Collection/publication log: `b4-final-collection-publication10-1794863608576673000.log`.
- Strict Clippy log: `b4-final-strict-clippy-1794863626544181000.log`.
- Formatting log: `b4-final-fmt-1794863720400874000.log`.
