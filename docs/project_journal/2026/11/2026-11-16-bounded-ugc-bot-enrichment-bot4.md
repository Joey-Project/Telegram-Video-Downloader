---
id: 20261116-ugc-bot4
title: Bounded UGC enrichment in metadata recovery
status: completed
created: 2026-11-16
updated: 2026-11-16
branch: codex/bounded-metadata-resolution
pr: ""
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
- `cargo test --locked --workspace bilibili_core -- --test-threads=2`: 20 passed, 0 failed, 0 ignored. This includes the bot quota configuration and raw TCP oversized membership-header tests.
- The `metadata_recovery_` filter ran from the bounded profile's test binary: 25 passed, 0 failed, 0 ignored, including optional UGC fallback, timeout, order mapping, and mock Telegram/provider E2E coverage.
- Strict workspace/all-target Clippy and `cargo fmt --all -- --check` passed. The project journal validator and `git diff --check` passed.
- Tests used local mocks and temporary fixtures; no live Bilibili API was called.

## Evidence
- Integration points: `src/bilibili_core.rs` and `src/downloader.rs`.
- Membership and bot-quota log: `bounded-ugc-core-membership-1794834570830804000.log`.
- Metadata-recovery log: `bounded-ugc-metadata-recovery-bin-1794835058988614000.log`.
- Strict Clippy log: `bounded-ugc-strict-clippy-1794835106540932000.log`.
- Formatting log: `bounded-ugc-fmt-1794835293302201000.log`.
