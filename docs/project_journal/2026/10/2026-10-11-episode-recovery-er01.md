---
id: 20261011-er01
title: Independent Bilibili Episode Recovery
status: completed
created: 2026-10-11
updated: 2026-10-11
branch: wip/bilibili-episode-recovery
pr: https://github.com/Joey-Project/Telegram-Video-Downloader/pull/38
supersedes: []
superseded_by:
---

# Independent Bilibili Episode Recovery

## Problem

A terminal episode error stopped an entire season, and successful downloads
were not independently published. A later retry repeated work and did not
offer a way to select failed episodes.

## Scope

- Retain one group in the queue, with persistent per-episode execution state,
  sparse original indices, stable media identities, and verified output receipts.
- Resolve group inventory and inspect selected episode streams with at most two
  concurrent probes and independent timeouts. Individual unavailable streams do
  not abort inspection; fresh execution plans must match confirmed per-episode
  formats, resolutions, and codecs before downloading.
- Publish verified episodes individually. Retry transient episode failures at
  most twice beyond the initial episode attempt, then continue later episodes.
  Shared authentication, storage, permission, and identity failures interrupt
  the group for explicit resume.
- Reuse the existing progress message and configured update interval. Report
  partial completion and offer group or single-episode retry through paginated
  episode and failure views.
- Revalidate successful output content and media identities before reuse.
  Persist invalid receipt markers across scoped retries until redownload is
  verified. Keep legacy attempts compatible and reject stale generation callbacks.
- Preserve legacy execution for artifact-only All selections.
- Reconcile approved inventory changes with saved episode state, invalidate
  receipts when media requirements change, and recover published outputs after
  interrupted completion bookkeeping. Reuse one existing-media index per run.
- Preserve limited retry scopes by stable media identity across approved
  inventory changes. Distinguish settled partial results from restart
  interruptions, and offer finalization resume when all outputs are verified.
- Bind artifact requirements to output receipts and persist one latest private
  publication receipt per logical queue task, stable media identity, output root,
  and selected collection directory. Recovery does not depend on optional NFO or
  sidecars; content hashes and bound path checks distinguish invalid output from
  unreadable recovery state.
- Recover published outputs even when the episode-start event was not saved,
  preserve explicit fresh All keep-both behavior, and compare original receipt
  hashes at group completion and final queue hashing. Retain selected collection
  directory identities and release shared index locks before timed media probes.
- Offer a claimable normal resume after scoped retry preparation fails before
  execution. Keep explicit cancellation authoritative over interrupted cleanup
  snapshots, and pause collection groups when refreshed page identities change.
- Carry reviewed collection-directory object identities across plan confirmation
  and execution. Media verification remains provisional until the parent queue
  completes final hashing and persistence, with matching failure/restart outcomes.
  Legacy collection plans and Missing-to-Existing directory transitions use the
  existing plan confirmation flow while retaining valid episode receipts.
- Partition collection publication receipts by their selected relative directory
  and validate that durable and queue-snapshot media paths remain in that scope.
  Shared media identities in separate collections retain independent recovery;
  legacy unscoped receipts cannot silently supply collection recovery evidence.
- Persist a logical task namespace before child execution. All/keep-both recovery
  uses only that task's publication evidence and does not trust the shared media
  index as proof of a newly requested copy. Missing selections retain skip-existing.
- Let explicit parent cancellation replace failed or partial episode outcomes,
  and pause once when the shared configured output root disappears.
- Bind durable publication requirements to confirmed episode stream fingerprints
  so accepting new formats cannot reuse an old-format receipt after queue receipt
  persistence was interrupted.
- Revalidate copied and durable publication receipts before launching an automatic
  retry. Verified outputs survive retryable post-publication cleanup errors;
  changed outputs still require a fresh child execution.
- Carry the confirmed collection directory object into each child publication.
  Bind destination parents to that object before moving files. Missing directories
  use atomic owned creation, retaining the resulting identity for later episodes.

## Validation

- Rust 1.95.0: all-target offline tests passed with eight parallel threads,
  720 passed and 15 existing
  opt-in tests ignored. Offline build, all-target Clippy with warnings denied,
  formatting, diff checks, and project journal validation passed.
- Regression tests exercise the actual episode driver through injected child
  executors: bounded 403 retries followed by continuation, publication before
  the next entry, failed-only and single-entry scopes, invalid receipt recovery
  across two resumes, and cancellation preserving verified outputs.
- Recovery regressions cover one shared index with new publication evidence,
  approved inventory growth and renumbering, media requirement changes,
  interrupted All publications, single-entry URLs, and zero-success failures.
- Durable recovery regressions cover receipt persistence before the production
  mover, NFO-disabled outputs, unchanged-content rematerialization, content
  mutation, and unreadable receipt state. UI and state regressions preserve
  retry scopes across inventory changes and expose finalization resume while
  distinguishing settled partial results from restart interruptions.
- Completion regressions reject later content mutations and preserve original
  hashes through the actual parent queue finalization path; same-content file
  replacement and normal directory child-entry churn remain accepted. Controlled
  timeout tests prove background probes release the publication-index lock and
  publication-index updates fail within a bounded wait.
- Recovery action regressions exercise a failed-only retry followed by planning
  failure and execute the rendered resume callback. Cancellation tests cover
  interrupted cleanup snapshots and reload; refreshed collection CID changes
  stop the driver before a later episode starts.
- Finalization regressions preserve verified episode files while replacing the
  provisional group outcome after parent failure or restart. Collection plan
  regressions retain reviewed directory identity across delayed execution.
- Cross-collection recovery regressions retain independent receipts for the same
  media identity in two directories; foreign queue paths cannot count
  as successful outputs in the selected collection.
- Stream planning regressions cover bounded parallelism, continuation after one
  unavailable episode, resolved-empty versus unresolved streams, and execution
  checks against the confirmed per-entry fingerprint. Durable recovery rejects
  an old-format receipt without saved queue paths after new streams are accepted.
- Task isolation and restart regressions reject another All task's publication
  evidence, preserve task namespaces, retain cancellation over failed summaries,
  and pause on output-root disappearance while keeping single-file misses local.
- Publication regressions replace the collection directory in the actual mover
  and reject path rebinding, while allowing normal child churn and consecutive
  publications into an atomically created directory. Concurrent foreign creation
  is rejected. Automatic retry tests recover event-only and durable-only receipts
  after cleanup timeout, and redownload changed media instead of reusing it.
- State and mock Telegram tests cover independent completion counts, restart,
  error retention, progress-channel closure, paginated episode views, message
  reuse, stale generation callbacks, and cross-chat action rejection.
- Output-lock tests accept only the documented acquisition progress when another
  parallel test holds the shared lock, while retaining explicit contention and
  clean-marker recovery-scan assertions.
- Builds use an isolated temporary Cargo target with debug information and
  incremental compilation disabled. This workstream does not deploy or restart
  the canonical macOS LaunchAgent.
