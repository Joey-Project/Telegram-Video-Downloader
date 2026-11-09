---
id: 20261109-lux01
title: Current library visibility and larger bulk selection
status: completed
created: 2026-11-09
updated: 2026-11-09
branch: "wip/library-moved-bulk"
pr: "https://github.com/Joey-Project/Telegram-Video-Downloader/pull/64"
supersedes: []
superseded_by: ""
---

# Current library visibility and larger bulk selection

## Scope

- Hide verified migrated-out source records from the default Library view,
  while retaining their history and destination navigation.
- Support selections of up to 5,000 videos across pages for file previews and
  selected library migrations. Keep metadata lookups in bounded 100-item passes.

## Behavior

- The default Current items filter excludes only records with a valid moved
  destination annotation. Moved history explicitly reveals them. Ordinary
  missing, unavailable and unknown records remain visible for investigation.
- Library counts subtract verified retained moves. Empty-state and omitted-row
  notices distinguish a library with no current retained rows from a partial
  cloud view that cannot classify omitted records.
- Frontend, Worker and local file-preview validation agree on the 5,000-item
  ceiling. A 5,001-item action is rejected before Worker inbox insertion.
  Existing request-byte, filesystem identity and descriptor-anchor limits remain.
- Move and metadata previews share a bounded descriptor budget. Tokens without
  complete persistent creation-time evidence take priority; other descriptors
  are retained within the remaining budget to tolerate benign timestamp
  changes. Previously accepted selections retain all their identity anchors.
  Large selections use persistent identity for files without a cached handle.
  Exact object, content and access-policy validation still applies; more than
  512 files requiring live identity evidence are rejected before persistence.
- Private preview and batch storage budgets are 32 MiB and 64 MiB. Cleanup has
  a 128 MiB pass budget so even the largest accepted document can be read and
  revalidated before expiration. Oversized rewrites retain the previous record.
- Large cloud previews carry a bounded, explicitly labelled summary and sample,
  tied to the full saved plan. Confirmation executes that local plan, not the
  displayed sample. Older clients remain unable to confirm an incomplete view.
- Metadata recovery keeps its existing local resolver concurrency, deadlines
  and 100-item request/report limits. Large selections use explicit user-driven
  batches rather than automatically submitting many queries.
- No existing media is moved, renamed, deleted or rescanned merely by changing
  the default view or increasing the selection ceiling.

## Validation

- Worker/logic suite: 397 tests passed, including authenticated local HTTP/D1
  persistence of exactly 5,000 IDs for file-preview and migration actions and
  rejection of 5,001 without inserting a request.
- Mini App UI suite: 90 tests passed. Type checking, JavaScript syntax checks
  and the Worker deployment dry-run passed.
- Real-file/mock-Telegram coverage confirms a sample-only receipt and moves all
  101 selected temporary files using the complete saved plan. Separate library
  migration coverage confirms a compact preview for 101 real files and pauses
  before copying. The 5,000-item tests cover wire persistence, summary bounds
  and multibyte-path storage/expiration, not a 5,000-file copy run.
- Additional real-file regressions move and verify all 513 selected files,
  including a benign timestamp update to an anchored source after preview,
  preview 513 existing-NFO metadata patches within the descriptor budget, and
  reject 513 files lacking creation-time evidence without persisting a preview
  or modifying sources. Existing anchor and creation-time regressions pass.
- Two pre-existing liveness tests were stabilized by pausing Tokio time only for
  explicit advances and resuming it for real TCP/notification waits. The idle,
  Pong and slow-handler timings and all assertions remain unchanged. Each test
  passed three focused repetitions; the full liveness module also passed.
  No production liveness behavior changed.
- Native formatting and strict Clippy passed. The final default-parallel
  all-target suite passed 1,370 tests with 17 ignored and no failures, including
  both 101-item E2Es and the 5,000-item storage/expiration regression.
- Existing CI discovers every new test. Operational build, source-binding and
  deployment evidence remains in the private local task directory.
