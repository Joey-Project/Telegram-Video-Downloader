---
id: 20261024-mrf01
title: Metadata recovery query budgets and preview visibility
status: completed
created: 2026-10-24
updated: 2026-10-24
branch: "codex/metadata-recovery-regressions"
pr: ""
supersedes: []
superseded_by: ""
---

# Metadata recovery query budgets and preview visibility

## Reported behavior and evidence

- A library analysis produced six ready items and many identical lookup-failure messages. The saved production receipt contains 67 results: six ready, six unchanged, six unsupported, and 49 unavailable.
- The analyzer used sequential source queries with a 12-second per-query ceiling and a shared 100-second analysis budget. It classified unattempted items after that budget exactly like actual provider errors, timeouts, and oversized results.
- Optional UGC collection enrichment could discard already-resolved exact video metadata when collection lookup failed or took too long.
- Both reported preview requests had completed on the local service and reached awaiting-confirmation state in the fresh cloud snapshot. The Mini App kept the user on Needs organizing instead of opening the Organizing tab that contained those previews.
- Two anonymous, read-only failed-source samples returned HTTP 412. This establishes anonymous endpoint refusal for those samples, not the outcome of the bot's authenticated metadata path.

## Repair contract

- Bound source concurrency, retain deterministic item ordering and source deduplication, and distinguish actual lookup failures from items deferred by the shared budget.
- Keep deferred items reachable through library continuation or an explicit selected-scope deferred-only analysis action.
- Represent an initial batch deferred before its first lookup with a null cursor and all candidates remaining. The parser permits that start-over case only when the first row is deferred and `remaining == total > 0`.
- Preserve validated base metadata and explicitly confirmed UGC membership when optional collection-order enrichment fails within a separate bounded window. Keep known collection ID/title/kind and exact CID, leave unknown order absent, and never overwrite custom metadata.
- Open Organizing when recovery updates are submitted and show the pending request until its confirmation preview arrives. Keep hibernating WebSocket snapshot updates; do not add a polling loop.
- Clearing analysis choices retains an already submitted preview's preparation/failure tracking. Root-binding changes and private-access expiry still clear it.
- Recover a preview submission from its original request ID in owner snapshots when another browser tab drains the shared queue. Publish the bounded ID and original library/root identity on file-preview receipts and active previews, including compact placeholders, so an omitted operation receipt cannot strand the submitting tab.
- Complete serialized credential synchronization before acquiring shared validation capacity, within the same source deadline, so waiting recovery queries do not reserve download-validation slots.

## Validation

- Two configured existing-profile base queries succeeded in 1.314s and 0.554s. A separate read-only probe through the actual optional-enrichment helper confirmed membership for both samples, started full collection resolution, and reached its 3s cap. This reproduces a concrete slow optional stage without proving every historical failure had that cause.
- After preserving explicit membership, the same two real samples returned one valid collection-bearing entry each at the 3s enrichment deadline, with zero invented order values. These probes load the existing profile without legacy synchronization, credential refresh, or writes; they do not submit production commands.
- Recovery-focused tests passed 95 cases, including mock Telegram analysis-to-confirmation-to-rescan for verified membership without order, unchanged video bytes, and preservation of custom NFO content.
- Full Rust suite: 1288 passed, zero failed, 16 existing ignored; recovery-focused suite: 95 passed. All-target Clippy with warnings denied, formatting, and isolated optimized release build passed. Snapshot regressions cover exact persisted request IDs, pending/failure identity, compaction/replay, placeholders, and independent preview/history retention.
- Cloud suite: 318 ordinary tests and 32 UI tests passed; generated binding types, TypeScript, JavaScript syntax, and deployment packaging passed. Tests wait for mock snapshot publication and completed refreshes, isolate delegated listeners between page mounts, and honor the one-second invalidation throttle instead of assuming immediate reads.
- UI regressions exercise automatic Organizing navigation, pending-to-confirmation handoff, same-root revision failure visibility, deferred-only retry after reload without browser selection, and cross-tab success/failure with foreign-request and root-binding rejection. The loading region exposes its refresh lifecycle through aria-busy.
- Read-only provider diagnostics do not constitute production metadata confirmation. Private runtime snapshots, logs, and deployment bookkeeping remain outside Git.
