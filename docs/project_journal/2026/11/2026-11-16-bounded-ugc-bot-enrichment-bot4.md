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
- At source HEAD `ce00349aa1e5fc22536ee4532d7f7bf326c270ba` with the locked BBDown core revision `afd39a25`, `cargo test --workspace --locked bilibili_core -- --test-threads=1` passed 20 tests (0 failed, 0 ignored).
- The `metadata_recovery_` filter ran from the exact main test binary produced by that command, `rust-cache/bounded/build/debug/deps/telegram_video_downloader-b88d3c8f19aac0a5` (SHA-256 `54d03375b7d63119fa25c1a9b570f6db06391171e51a3ec9830279b53020c8d6`): 25 passed, 0 failed, 0 ignored. Coverage includes optional UGC fallback, timeout, order mapping, and mock Telegram/provider E2E cases.
- Strict workspace/all-target Clippy with `-D warnings` and `cargo fmt --all -- --check` passed.
- The project journal validator and `git diff --check` passed.
- Tests used local mocks and temporary fixtures; no live Bilibili API was called.

## Evidence
- Integration points: `src/bilibili_core.rs` and `src/downloader.rs`.
- Membership and bot-quota log: `bot4-final-core-filter-1794843189162998000.log`.
- Metadata-recovery log: `bot4-final-metadata-recovery-1794843386332084000.log`.
- Strict Clippy log: `bot4-final-strict-clippy-1794843488736093000.log`.
- Formatting log: `bot4-final-fmt-1794843572786266000.log`.
