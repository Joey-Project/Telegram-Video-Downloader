# Cloud ingress and file management

## Scope and delivery status

The isolated `wip/file-management-cloud` branch implements stages 1–4: Telegram ingress through a Worker inbox, the Rust local client, a persistent media library, the `/file` Mini App, and preview-confirm file operations including legacy metadata patches. `/settings` and new-download location selection are stage 5 and remain deferred.

The branch integrates master through `fefdfcb3cd03a5e153e9e02fd8f9bdf6c727e5b0`. Local validation is recorded below. No production Cloudflare resources, Worker secrets, Telegram webhook changes, bot deployment, or live quota/hibernation checks have occurred.

The local service initiates every cloud connection and sends Telegram replies directly. It exposes no public inbound endpoint. With `[cloud]` absent, the existing `getUpdates` bot path remains active.

## Delivery protocol

1. The Worker validates the Telegram webhook or Mini App request, writes the complete request to D1, and assigns a sequence number. Telegram submissions deduplicate by update ID; Mini App actions deduplicate by client request ID.
2. After the D1 write, the Worker sends the full request envelope over the hibernating WebSocket. A failed notification leaves the D1 request available for replay. Before enabling dispatch after a notification, Rust reconciles the authoritative REST backlog so earlier unacknowledged requests are durably present even when notifications arrive out of order.
3. Rust validates the envelope and JavaScript-safe sequence number, then durably writes the full request to its private inbox under the configured video download root. It sends `POST /api/local/ack` only after that write succeeds. WebSocket delivery uses this same HTTPS receipt; the WebSocket itself has no ACK frame.
4. The inbox deduplicates by exact sequence and envelope, not by a maximum-sequence watermark. An unseen lower sequence is accepted. Re-delivery of an identical sequence wakes recovery without creating another local record; a conflicting envelope for a used sequence is rejected.
5. Local receipt and operation completion are separate. Pending management work survives restart; a confirmed file batch uses its persisted preview and move journal to resume unfinished steps. An interrupted Telegram update whose external side effect is uncertain is surfaced as uncertain rather than blindly replayed.
6. Rust separately publishes operation results and snapshots through HTTPS. The client fetches the oldest pending batch at startup, on reconnect, and on a low-frequency fallback poll (30 seconds by default, configurable no lower than 5 seconds). WebSocket reconnect uses bounded backoff.

Startup dispatch remains paused until authoritative reconciliation succeeds. REST polls and WebSocket reconciliation share one gate, drain acknowledged pages in sequence order, and leave dispatch paused after a failed fetch or receipt. A notification absent from both the REST backlog and the exact local durable inbox is not dispatched.

The Rust client leaves an established WebSocket open while it is idle; it does not reconnect on an idle deadline. The independent REST fallback reads the D1 backlog and does not invoke the notification object. A real close or socket error triggers reconnect and backlog reconciliation. Production Durable Object hibernation behavior still needs live validation.

The durable local inbox stores full request envelopes and statuses such as pending, processing, awaiting confirmation, retryable, completed, failed, stale, and uncertain. The inbox is single-process owned and private to the configured local account. Download-task update-ID deduplication remains in place for downloads; the inbox also covers bot commands, callbacks, and management requests.

## Interfaces

Local service requests carry the fixed shared secret in their `Authorization: Bearer` header. The secret is never placed in a URL or sent to the Mini App. Mini App requests carry Telegram `initData`, verified by the Worker against the bot and configured owner.

| Route | Purpose |
| --- | --- |
| `POST /webhook` | Persist authorized Telegram updates |
| `GET /api/local/ws` | Deliver full request envelopes over WS |
| `GET /api/local/requests?limit=100` | Fetch the oldest unacknowledged requests |
| `POST /api/local/ack` | Confirm local durable receipt, not execution completion |
| `POST /api/local/state` | Publish local state and operation results |
| `GET /api/state` | Read the latest local snapshot and safely expose unacknowledged request summaries |
| `GET /api/library` | Read the media library snapshot |
| `POST /api/actions` | Submit a file-management action |

Example move preview:

```json
{
  "request_id": "10000000-0000-4000-8000-000000000001",
  "kind": "file_preview",
  "payload": {
    "item_ids": ["a-library-copy-id"],
    "target_relative_dir": "Learning",
    "rename": true,
    "conflict": "skip"
  }
}
```

An empty `target_relative_dir` selects the download root. A metadata-only preview omits `target_relative_dir` and supplies `item_ids` exactly matching its `metadata_patches`. Imported hints are not applied automatically: the user explicitly maps a hint to a library item, reviews the resulting preview, and confirms `{ "preview_id": "…", "revision": "…" }`. A manual match without `hint_index` is also explicit and still requires preview and confirmation. The `legacy_import` action only returns hints and candidates.

The state payload uses a locally persisted, increasing, JavaScript-safe `state_version`. A content retry reuses its version and content; D1 rejects lower versions and treats identical same-version content idempotently. `reported_at` is excluded from the content hash so retries and heartbeats can refresh contact time without changing that version. `created_at` and `reported_at` are Unix milliseconds; `library.scanned_at` is Unix seconds. Before the first successful scan, Rust reports a `ready: false` placeholder rather than an empty library.

A state-version conflict returns the current stored version in `X-Cloud-State-Version`. Rust validates that hint and durably allocates a version above both its local counter and the stored version before retrying the same content. This recovers after moving the installation or losing the private counter file. Conflicts without a valid hint remain eligible for backed-off retry; invalid or exhausted counters are reported rather than wrapping.

Operation history and active previews have separate encoded-byte budgets. Recent import details remain available for manual mapping; older oversized results carry explicit truncation warnings. An incomplete preview cannot be confirmed. A 413 response first compacts operation details, then reduces the library only if necessary, without silently describing a reduced snapshot as complete. Authentication failures remain eligible for backed-off retry after configuration is corrected.

The Mini App exposes private library data only after the current Telegram identity passes the Worker owner check. An authorization failure hides private content and stops action dispatch until authentication succeeds again. Network errors, rate limits, and server failures retain queued action IDs for retry; permanently rejected actions are surfaced and removed so later valid requests can proceed. Failed previews remain visible in operation history, and missing or unavailable media cannot be selected for a move.

Selection controls and preview submission share the 100-item request limit. Bulk selection preserves existing selections, adds only the available capacity, and reports when another batch is needed.

State and receipt writes do not call the notification object. A five-minute heartbeat republishes the cached library snapshot and does not scan the filesystem. A scan runs at startup, on an explicit library-scan request, after a newly published download completes, or after a local management operation changes the library. Scan failures preserve the prior snapshot and report a sanitized warning. The UI shows report and scan times; absence of the first scan means waiting for synchronization, not an empty library. The UI considers the local service stale after ten minutes without a report.

Task status and progress use a separate notification, coalesced over two seconds, to refresh cloud summaries without scanning the media library. Active-task summaries include up to 100 tasks across queue pages. The local inbox indexes pending sequences and recent management state so dispatch ticks and progress reports do not deserialize the full terminal history. Durable terminal receipts remain available for exact-sequence deduplication. A queued Telegram update becomes processing only after acquiring its execution permit.

## Local library and scan boundaries

- Scan only the configured video download root; never traverse the whole disk or follow paths outside it. The recursive scan has a depth limit of 48, a 200,000-entry limit, a 50,000-media-item limit, and a 120-second budget.
- Skip private queue, staging, cloud-inbox, and library-internal directories. Symlinks and special files are not followed or opened. Non-UTF-8 media paths are skipped with a warning.
- Only directories fully visited by the scan can establish that a prior item is missing. An unreadable or unavailable subtree remains unavailable; unvisited paths at a depth, entry, or time limit are not marked missing.
- Rebuild the index from local files, NFO metadata, and completed queue hints. The index is not authoritative content. Queue hints include per-entry stable Bilibili IDs so multi-part entries do not collapse to one collection-level identity.
- Bilibili tasks can publish verified episodes before the whole task completes. Their source hints use only each verified entry's own stable ID and durable output paths/hashes, retaining its original episode index. Failed or pending entries are not advertised as published. Legacy completed tasks without per-entry attempt records retain the original whole-task hint path. A receipt is a source hint, not proof that the file's current content is intact.
- A newly persisted verified episode receipt notifies the debounced library scanner; ordinary progress updates do not. A partial task keeps its existing retry/history semantics, independently of whether its published episodes appear in the library.
- Queue-derived titles, source IDs, and collection metadata survive identity-preserving moves and index reloads. A different file at the old path does not inherit that identity. YouTube source IDs use `youtube:<video-id>`; NFO collection IDs and kinds survive metadata updates. Fully populated recognized NFO metadata is marked complete.
- Confirmed missing history is capped at 1,024 records and pruned further when necessary to fit the 16 MiB index budget. Present or unavailable records are not discarded to make room for missing history. The index remains rebuildable.
- Probe media using an already opened, no-follow file descriptor duplicated to `ffprobe` stdin and read through `/dev/fd/0`; do not reopen a public pathname in the subprocess. File Provider coordination ends before waiting for the process. Probe time is limited to 3 seconds and captured output to 64 KiB. If `ffprobe` is absent or returns no usable stream, basic library management remains available without those media specifications.

## File operation and recovery guarantees

- Every selected source and target is resolved under the configured root. Moves use no-replace operations and persist a per-file batch journal; a multi-file batch is not an atomic filesystem transaction.
- Preview and execution protect three properties: object identity, content stability, and access policy. The file token records device/inode/type, size, mode, owner/group, creation time where available, and SHA-256 content. The digest detects same-size content edits; owner/mode detect ordinary POSIX access-policy changes. Modification time, change time, and link count are not mutation verdicts by themselves because harmless metadata and File Provider materialization can change them.
- Revalidation distinguishes absence, a readable mismatch, and an unreadable or failed check. Before a move, the source must still match the approved token and the target must remain absent. After a move, the destination is checked again. During restart recovery, a missing source plus a destination matching the complete approved token is recognized as an already completed step. A replacement, different target, changed content/policy, or failed revalidation is left untouched and reported; the operation does not continue from path names alone.
- Creation time can change during a benign macOS metadata update. A birth-time-only difference is accepted only while the process retains the original open descriptor and the current path still names that same file, with matching type, size, SHA-256, owner/group, and mode. Without that descriptor anchor, including after restart or preview-cache eviction, a token mismatch requires a fresh preview. Descriptor retention is bounded; it does not relax content or access-policy checks.
- NFO patches are prepared with the preview and applied only after confirmation. The UI shows each known field's old and new values and the exact source IDs being added. Existing NFO identity and SHA-256 are checked before atomic replacement; the media object is also revalidated. Unknown XML content is retained, ordinary POSIX permissions are preserved, and a retry recognizes the exact prepared after-content digest. This does not claim arbitrary extended-ACL preservation.
- Closing the Mini App does not cancel an accepted operation. Retryable File Provider access failures use persisted local backoff. A batch containing a retryable access failure retains its original confirmable preview and durable partial manifest; a later confirmation resumes unfinished steps. Permanent or stale preview failures require a fresh preview.
- Conflict-policy skips count as failures, producing a failed or partial result. A permanent confirmation failure closes its old preview; an interrupted NFO creation is recognized only when the prepared content digest and expected access policy still match.
- Each confirmed batch attempts one Telegram `sendMessage`, persists a `sending` receipt before the call, then persists the returned message ID and edits that message for the terminal summary. Telegram has no idempotency key for `sendMessage`: if Telegram accepts the message but the process stops before saving its ID, the local receipt cannot identify that message. The retry will not send a duplicate and will surface a warning; `/file` remains the source of the durable operation result. Current messages provide start and final status, not a guarantee of per-file live progress.

## Configuration and production handoff

The optional local `[cloud]` configuration contains `worker_url`, `shared_secret`, and `fallback_poll_seconds`. `worker_url` must be an HTTPS origin without a path prefix because the Worker and Mini App serve root-relative routes. `TELEGRAM_VIDEO_DOWNLOADER_CLOUD_SHARED_SECRET` can supply the shared secret without placing it in a tracked configuration. Secret values are omitted from serialization and redacted in `Debug` output. Cloud mode requires HTTPS, `allow_all_chats = false`, and exactly one positive private-chat ID; it must match the Worker's `OWNER_USER_ID`.

The Worker uses the `DB` D1 binding, a `NOTIFIER` Durable Object binding, and static assets. Its secrets are `LOCAL_SHARED_SECRET`, `WEBHOOK_SECRET`, and `BOT_TOKEN`. Cloudflare account credentials are used only by deployment tooling and are not given to the frontend or Rust runtime.

Resource creation, schema migration, secret configuration, and Telegram webhook switching remain explicit deployment steps. Existing `getUpdates` and webhook ingestion must not run simultaneously against the production bot. Preserve pending Telegram updates during handoff. No production bot, webhook, secret, or cloud resource was changed for this branch.

## Checks and remaining validation

- With Node.js 25.8.2 and Wrangler 4.149.0, `npm run typecheck`, `npm test` (40/40), and `npm run build` passed on 2026-10-18. The build is a deployment dry-run, not a deployment. The regressions include owner verification before displaying private content, invalid webhook-owner configuration, authorization failure handling, permanent action rejection, terminal and incomplete previews, import truncation warnings, available-item selection and the 100-item limit, metadata previews, state-version conflict hints, and YouTube links.
- With Rust 1.99.1 on 2026-10-18, `cargo fmt --all -- --check`, `cargo check --all-targets --locked --offline`, `cargo clippy --all-targets --locked --offline -- -D warnings`, `cargo test --all-targets --locked --offline --quiet`, and `cargo build --locked --offline` passed. The full test suite reports 853 passed, 0 failed, and 16 ignored; ignored tests were not executed as passing tests. Compilation commands explicitly used this worktree's target directory. The local debug build did not install or restart the bot.
- Rust local tests cover the inbox, management operations, queue-publication scan trigger, local mock Cloud REST persistence/ACK/restart/dispatch, and mock Telegram interactions, including a single batch-status message and editing it after failure. The episode-receipt regression covers partial results, excluding failed/pending entries, legacy whole-task compatibility, and notifying only for persisted publication changes. Library tests cover incomplete scans, non-overwriting moves, partial recovery, metadata-only confirmation, content/policy/replacement rejection, and benign metadata changes with an original descriptor anchor.
- Mock integration regressions cover Telegram permit ordering, startup and out-of-order WebSocket reconciliation, permanent confirmation failure, and injected File Provider failures through the dispatcher. A partial file batch keeps the same preview and resumes unfinished steps. Other regressions cover compact oversized-result receipts across restart, progress coalescing without a library rescan, recovered authorization, large operation history, repeated 413 responses below the soft limit, accepted-snapshot heartbeats, and stopping an irreducible upload without consuming versions indefinitely.
- CI includes Rust and Cloudflare checks, type checking, and local deployment-bundle validation without production credentials. Current-head CI and remote review are tracked on [PR #32](https://github.com/Joey-Project/Telegram-Video-Downloader/pull/32); results for older commits do not qualify for the updated head.
- Real Durable Object hibernation, production quota behavior, configured-owner alignment, and webhook switching require a separate live deployment check; local simulations do not establish those facts.
