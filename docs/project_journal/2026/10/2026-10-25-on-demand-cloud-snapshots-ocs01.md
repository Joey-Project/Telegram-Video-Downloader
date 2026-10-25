---
id: 20261025-ocs01
title: Publish cloud time snapshots on demand
status: completed
created: 2026-10-25
updated: 2026-10-25
branch: "wip/on-demand-cloud-snapshots"
pr: ""
supersedes: []
superseded_by: ""
---

# Publish cloud time snapshots on demand

## Agreed behavior

"Snapshot" means a view of the Mac's state at a particular time. The Mac remains
authoritative and durably records tasks, files, management operations and progress.
Cloudflare stores a published snapshot for the Mini Apps; a local change alone is
not a reason to publish it immediately.

- Preserve reliable Telegram/action ingress, durable local receipt acknowledgments,
  reconnect reconciliation, authorization, request deduplication, replay windows,
  preview identity checks and execution-time filesystem revalidation.
- Keep the existing hibernating notification connections. Remove the permanent
  30-second idle REST loop and five-minute snapshot publication cadence in favor
  of low-frequency reconciliation and on-demand publication.
- Local task progress, disk capacity changes and background scans remain local
  unless an active Mini App interaction needs the result. Telegram replies and
  download messages continue directly from the Mac independently of CF snapshots.
- Opening a Mini App shows the saved snapshot and its time, then requests the
  current page's needed data once. A refresh button requests actual Mac data,
  rather than merely rereading the existing CF snapshot. No passive whole-page
  polling while a visible page is idle.
- Searching, sorting, pagination, selection and expanding already loaded details
  reuse the snapshot. Preparing downloads, file previews, metadata recovery,
  library migration and settings changes refresh their necessary reference data.
- Combine relevant refreshed data and an operation's result in the existing
  business exchange where possible. Avoid an additional refresh-before-submit
  round trip. Publish only relevant changed content using the existing bounded
  chunked snapshot transport; do not rescan every media library for each click.
- Return watched operation results to an active Mini App. Closing or hiding the
  page ends that live interest; unobserved progress/results remain durable locally
  and are visible on a subsequent explicit/on-open refresh.
- Low-frequency refresh expires one hour after the last successful Mac-to-CF
  refresh. Successful user-triggered refresh moves expiry forward; cache reads
  and failed uploads do not. Preserve expiry across restart. A partial data
  refresh must not label unrelated old data as freshly scanned.
- Hourly background publication refreshes availability/capacity and uses the
  latest local file index. It does not start another full filesystem scan or
  metadata backfill. Opening/manually refreshing a library and existing explicit
  scan, download-publication and management triggers retain actual scans. If an
  explicit scoped refresh coincides with expiry, use that requested reference
  collection once, retaining per-request failure reporting.
- Keep stale/offline snapshots usable with accurate age/pending/failure messages.
  A refresh never substitutes for validation when a command actually executes.

## Trigger inventory

| Trigger | Communication / fresh reference data |
| --- | --- |
| New Telegram or Mini App command | Persist ingress, notify Mac, acknowledge durable local receipt |
| Mac starts or reconnects | Reconcile missing durable commands before trusting live hints |
| Mini App opens / explicit refresh | Refresh required current-page data and acknowledge the refresh outcome |
| Prepare download | Current destination/defaults, availability/capacity, relevant configuration |
| Prepare organize / move / migration | Source/target identity, files, conflicts, capacity, unfinished tasks |
| Metadata recovery / rescan | Selected media metadata or requested library index |
| Change settings / library configuration | Current setting, registration and validation results |
| Confirm / retry / cancel | Execute with fresh local validation; return related result/current data |
| Watched operation changes | Coalesced related result/progress while a user is actively interested |
| No user interaction / disk changes | No immediate CF state publication |
| Hourly expiry | One low-frequency refresh; successful publication rearms expiry |

## Implementation sequence

1. Freeze the cross-layer refresh, interest and publication contract after targeted
   Rust, Worker and Mini App investigation. Record concrete choices here before
   implementing them.
2. Add bounded/authenticated on-demand refresh and foreground interest transport,
   retaining existing ingress reliability and hibernation behavior.
3. Implement local publication admission, durable successful-refresh expiry,
   coalescing and hourly idle fallback. Prevent disk-capacity noise from causing
   repeated business-state publication.
4. Connect both Mini Apps' opening/refresh/actions to the contract. Reuse the
   browser action identity/receipt mechanisms where appropriate and preserve
   authorization, offline drafts and cancellation recovery.
5. Add mock Telegram / local HTTP E2E and Worker/DOM tests proving idle request
   bounds, operation piggybacking, foreground interest, expiry reset, restart,
   failure recovery and execution-time validation.
6. Run affected complete test/type/lint/build gates and record exact source and
   evidence. Ordinary builds/tests must not restart the production bot. Preserve
   the canonical checkout's unrelated edits and all media/control-state paths.

## D1 write budget follow-up

The user also reported an account-level D1 alert at 76% of the 100,000 daily
free-tier rows-written allowance. Include this in the same optimization: fewer
admitted snapshots must reduce their metadata, chunk-reference and cleanup writes,
not merely Worker invocation counts. Audit and measure the existing storage path
before adding any storage optimization. Cover unchanged/changed snapshots and a
24-hour idle schedule; count index-maintenance writes where the local D1 runtime
reports them. Preserve the existing atomic manifest, chunk integrity, request
deduplication and cleanup guarantees. Local test metrics are regression evidence,
not a claim that the entire account's production usage has been measured.

Cloudflare documents the allowance, index write accounting and UTC reset in its
[D1 pricing reference](https://developers.cloudflare.com/d1/platform/pricing/).

The audit found that the existing one-hour chunk-ACK cache can expire just as an
hourly publication starts, causing unchanged committed chunks to be uploaded
again. Keep a bounded in-memory set of hashes from the latest successfully
acknowledged manifest, scoped to epoch and monotonic state version. The server
already excludes current manifest references from orphan collection and validates
every manifest's hash/byte references before committing it. Reuse those confirmed
chunks while referenced; preserve the existing TTL for other staged hashes.
Invalidate both caches on a missing-chunk response or epoch change and retain the
existing retry/reupload path. Do not persist a new cache or change D1 schema/GC.
Test expiry, changed content, missing chunks, failed commits and epoch isolation;
paused-time request counts must also account for the separate chunk-cache clock.

## Current phase

The accepted plan is implemented and locally validated. The following contract
describes the resulting behavior. Production services, D1 data and media were not
changed by implementation, builds or tests.

## Validation evidence

The mock-HTTP regression
`cloud_bot_tests::idle_snapshot_and_inbox_http_counts_stay_hourly_for_24_hours`
starts with a recent persisted successful publication, suppressing a redundant
startup snapshot. It performs the startup authoritative inbox read, then advances
24 hours with real-I/O barriers: 25 inbox GETs, 24 manifest commits and 23 chunk
uploads. The chunk uploads occur only on the first publication; subsequent hourly
publications reuse the committed content. This is a controlled unchanged-index
fixture, not a measurement of the production account or every possible workload.

The D1 storage regression measures zero writes for repeated saved-snapshot reads,
preserves chunk contents/timestamps for an unchanged commit, and bounds the local
write cost of one changed chunk. Local Miniflare deletion metrics do not fully
represent production index accounting, so these tests do not assert a billing
guarantee. The client cache regressions cover expiry, changed content, failed
commits/uploads, missing-chunk recovery and epoch changes.

Final local gates on Node.js 25.8.2, Wrangler 4.149.0 and Rust 1.99.1:

- Rust default-parallel `cargo test --all-targets`: 1,325 passed, zero failed,
  16 ignored. Ignored tests were not executed as passing tests.
- `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`,
  `cargo check --all-targets` and the isolated release build passed.
- All 359 ordinary Worker/D1/shared-helper tests and all 74 mounted UI tests
  passed, including both apps' epoch-reset, held-read and dispatch-fencing cases.
- `npm run typecheck` and `npm run build` passed. The build is a Wrangler
  deployment dry-run and reads 71 static assets; it does not deploy them.
- All 101 macOS service-helper tests and the isolated LaunchAgent shell
  integration passed. They use temporary homes and mocked system commands.

The existing CI test commands discover the new regressions. These are local
mock HTTP/Telegram, Miniflare D1 and DOM checks, not live Cloudflare quota,
physical-device or production-deployment acceptance. Verification logs and
intermediate diagnostics remain in ignored local task directories.

## Frozen implementation contract

- Reuse the existing owner-authenticated, epoch-bound hibernating viewer/local
  WebSockets for ephemeral refresh/control messages. Add no durable inbox kind
  or D1 migration for read-only refresh requests. Durable business commands keep
  their existing envelopes, UUIDs, ACKs and recovery behavior.
- The Mac opts in to control frames with `X-Cloud-Snapshot-Control: v1` on its
  existing WebSocket handshake. Legacy clients still receive durable envelopes
  but never new control frames, avoiding parser-driven reconnect storms during
  mixed-version deployment or rollback. A refresh reports unavailable when only
  legacy local clients are connected. No credential or persistent identity changes.
- Viewer `ready` frames advertise `snapshot_control:"v1"`. New browsers send
  controls only after that capability is advertised; an older relay remains a
  usable cached view and reports real-refresh unavailability rather than entering
  a ready/send/unsupported-close reconnect loop.
- Viewer interest frame: `{type:"interest", database_epoch, visible, surface,
  watch}`, where surface is `file|settings` and watch indicates a visible
  operation/results view or a pending action whose result is awaited. Opening a
  page alone does not enable continuous publication of background changes.
- Viewer refresh frame: `{type:"refresh_request", database_epoch, request_id,
  reason, scope, library_id?}`. Reasons are `open|manual|reference`; scopes are
  `library|settings|operations|download`. Library refresh targets a registered
  library, using the current default when no ID is supplied. Read-only refresh
  IDs are UUIDs and are coalesced separately from durable command IDs.
- The DO forwards bounded `snapshot_refresh` frames with viewer ID, request ID,
  epoch, scope/reason, optional library ID and an authorization-bounded deadline.
  It reports a correlated unavailable result when no local client can receive
  the request. A refresh never waits indefinitely or becomes a queued business
  operation. Duplicate/pending IDs and frame sizes have explicit bounds.
- The DO sends the Mac a `viewer_interest` message containing the current bounded
  visible viewer set (`viewer_id`, `surface`, `watch`, `expires_at`) and epoch.
  Send on changes and local-client reconnect; hide/close/authorization expiry
  remove interest. Attachments preserve this state across hibernation; no
  periodic presence heartbeat or extra credential is introduced.
- After an admitted refresh has been collected and successfully committed through
  the existing chunked snapshot API, the Mac sends a correlated `refresh_result`
  over its existing socket with viewer/request IDs, epoch, status `published`,
  state version and report time. Failures use bounded generic status/codes.
  The DO forwards only to the matching authorized visible requester. Acceptance
  or a cached GET alone is never presented as a successful Mac refresh.
- Local dirty notifications do not admit cloud publication by themselves. Admit
  explicit refresh, relevant semantic changes while a visible viewer watches
  operations, or hourly expiry. Capacity-only changes do not trigger watched
  operation publication. Preserve actual capacity in requested snapshots.
- Existing business execution refreshes its required local references. Its result
  and those references share the admitted snapshot publication; submitting the
  action does not also request a separate pre-refresh. CF cache/chunk reads after
  a published-result notification remain allowed and coalesced.
- A snapshot publication may tell the authenticated Worker whether viewers need
  notification through `X-Snapshot-Notify-Viewers: 0|1`. Background hourly posts
  with no interested viewer skip the DO notification call. Legacy callers retain
  their previous notification behavior when the header is absent.
- Persist the last successful publication time alongside the existing local
  state-version metadata, preserving it on version advancement. Persist an optional
  fixed-size SHA-256 of the acknowledged semantic operations/tasks view there as
  well, so restarting and reactivating an unchanged watch does not force an extra
  publication. Update both only after upload ACK; legacy missing hashes remain
  compatible and must not hide completed results that were never published. Every admitted
  publication has a fresh version; rearm one-hour expiry only after upload ACK.
  Preserve original file-index `scanned_at` values unless actually rescanned:
  fresh capacity/settings must not be labelled a fresh file scan.
- Align the hour-level REST reconciliation schedule with successful refresh
  activity, overriding the old configured 30-second idle cadence. Startup and
  reconnect reconciliation remain immediate. Missing initial publication is
  bootstrapped; a recent persisted publication does not reset solely on restart.
- Inbox reconciliation retains a maximum one-hour gap since its own last
  successful authoritative read. A state-only progress publication cannot defer
  that read indefinitely and strand a missed command. Coalesce due reads with
  requested/hourly refresh work where possible; only actual backlog reads reset
  the inbox deadline. This is separate from the snapshot's sliding expiry.
  Failed background reads use bounded exponential retry backoff instead of a
  permanent five-second error loop; successful reconciliation resets the backoff.
  New command hints, startup/reconnect and explicit user reads retain their
  immediate authoritative reconciliation path.
- Both Mini Apps first read the CF snapshot and display its age, then request one
  actual refresh on a visible open. Explicit refresh requests Mac data; pure
  local interactions and idle timers do not. Visibility resume does not by itself
  start another refresh. Read/publish notifications may update CF cache only for
  a pending refresh or active operation interest. The existing local age clock
  may redraw labels without making network requests.
- Cloud snapshot age uses the one-hour policy. UI labels describe a saved time
  snapshot rather than claiming a recent snapshot proves the Mac is online.
  Offline/failed refresh leaves cached data and pending durable actions intact.
