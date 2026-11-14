---
id: 20261113-rsc01
title: Explicit retained video download staging cleanup
status: completed
created: 2026-11-13
updated: 2026-11-14
branch: "codex/staging-cleanup"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/71"
supersedes: []
superseded_by: ""
---

# Explicit retained video download staging cleanup

## Scope

- Add a `/file` view for recorded temporary video download directories, with explicit
  selection, a deletion preview, confirmation and per-directory results.
- Keep permanent retention as the default. Explain that cleanup discards partial
  download data and may require a fresh download when retrying the task.
- Keep inspection and filesystem work on the Mac. Reuse user-driven snapshot
  refresh and asynchronous requests without introducing idle cloud polling.

## Behavior and safety

- Protect active writers, completed downloads awaiting publication, published
  media, and all queue, lock, publication and recovery identities.
- Bind cleanup to configured library roots and exact owned staging objects;
  reject unknown ownership, unsafe paths, unavailable roots and stale previews.
- Revalidate task state and filesystem identity at confirmation. Ordinary
  timestamp or File Provider metadata changes alone do not prove replacement.
- Exercise cleanup only on synthetic temporary libraries during validation;
  never delete the user's retained download fragments as a test.
- Video staging uses an explicit Scan staging button, page selection, a saved
  cleanup preview and confirm/cancel actions in Requests. Completed results are
  compact history receipts. PDF and Music staging/originals are outside this view.
- Inert failed, interrupted, cancelled and partially completed tasks are eligible
  only when the exact private stage has its incomplete-retry retention marker.
  Completed download markers, publication/mux recovery, active cancellation
  registrations and transfer state stay protected. Unassociated stages produce
  a warning and remain untouched.
- A bounded private ownership pass includes indexed tasks from every chat while
  displaying only authorized rows. Shared directories, aliases added after
  preview and incomplete ownership inspection prevent deletion. Multiple distinct
  attempts of one task can be cleaned together without changing its queue record.
- Nonblocking output leases reuse the existing process and file locks without
  triggering startup publication recovery. Descriptor-bound tree inspection
  validates object birth identity, private access policy, typed child inventory
  and file lengths. It does not hash or read partial payload contents. Ordinary
  mtime/ctime changes and temporary child churn are not replacement evidence.
- Removal quarantines and revalidates the exact selected directory. A failed
  inventory check preserves it and restores it when the original path is free;
  interrupted cleanup retains and reports its
  quarantine for manual inspection. Missing paths only count as already removed
  when no replacement or matching cleanup quarantine is present.
- Cancel validates the saved preview without requiring the secondary library to
  remain online. Transient writer contention stays retryable; invalid selections
  and stale previews terminate with refresh guidance.
- Requests allow at most 5,000 selected IDs and retain the complete execution
  plan locally. Scan and plan documents each have a 512 KiB budget, and the full
  local inbox receipt must fit 1 MiB. Truncated scans are labelled; an oversized
  plan is rejected with a smaller-selection message. Cloud previews contain a
  bounded sample and exact bindings, never the private identity proofs.
- D1 migration 0007 adds the request kind while preserving pending/acknowledged
  messages, payload hashes, deduplication IDs, sequence high-water and epoch.
- A prepared preview stops waiting for computation while remaining available for
  confirmation or cancellation. Leaving Requests therefore releases its operation
  subscription. Confirmation receipts publish the library/root identity used to
  invalidate the previous scan without exposing the private request payload.
- Accepted confirmations and cancellations keep their exact preview identity
  visible in bounded pending receipts, including while the desktop is offline or
  retrying. Those controls stay disabled across Mini App sessions until a result
  arrives. Pending summaries expose only allowlisted scalar bindings and omit
  selections and local filesystem proofs.
- Ordinary retained failures with a DIRTY recovery marker can proceed after a
  bounded read-only audit of nested output transactions and every staging owner,
  retention/lifecycle marker and inventory. It runs no startup recovery. Cleanup
  revalidates the lease and requires a fresh full audit before declaring recovery
  CLEAN after deletion. Unreadable, unknown or over-budget recovery stays blocked.
- Global queue ownership failures protect every displayed row. Directory
  inventory truncation alone leaves earlier verified rows previewable. Ordinary
  media titles containing recovery or overwrite remain valid; only actual
  reserved recovery artifacts are protected. Directory listings enforce their
  entry and time budgets while reading, with object identity revalidation.

## Validation

- Twenty-five staging-specific tests pass, including real temporary files and mocked
  Telegram/cloud requests for offline cancellation, restart confirmation,
  resumed tasks, cross-chat aliases, multiple attempts, replay and proof privacy.
- Filesystem tests preserve new/replaced entries, symlink/hard-link targets and
  completed unpublished downloads. A benign timestamp test explicitly preserves
  birth identity: on this host a pre-birth mtime also changes creation time, so
  the fixture uses a future mtime and verifies unchanged creation identity.
- Required Rust CI covers the complete workspace, native converter unit tests,
  eight real codec E2Es, nine converter client tests without native features,
  service helpers and fixed Music runtime checks. Strict workspace Clippy and
  formatting pass.
- Worker/D1: 406 tests pass; mounted Mini App: 94 tests pass. Type checking,
  migration preservation checks and deployment dry-run pass. Existing CI
  automatically discovers the new Rust and Mini App tests.
- Mounted UI regressions cover releasing preview-result interest on navigation,
  retaining review controls and invalidating scans using the real public receipt
  shape. The restart-confirmation E2E verifies that shape from the Rust snapshot.
- Additional regressions cover accepted confirm/cancel across reload/offline/retry
  and terminal release, DIRTY ordinary failures, interrupted removal quarantines,
  nested transaction directories, ordinary recovery-word media names, bounded
  listings and truncated inventory with complete or incomplete queue ownership.
