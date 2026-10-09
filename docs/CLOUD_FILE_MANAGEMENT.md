# Cloud ingress and file management

## Scope and delivery status

The isolated `wip/file-management-cloud` branch implements stages 1–4: Telegram ingress through a Worker inbox, the Rust local client, a persistent media library, the `/file` Mini App, and preview-confirm file operations including legacy metadata patches. `/settings` and new-download location selection are stage 5 and remain deferred.

Cloudflare type checking, all 23 local tests, and Wrangler deployment dry-run passed. Rust's full local test suite passed with 572 tests passing and 11 ignored; formatting and strict Clippy checks passed. No production Cloudflare resources, Worker secrets, Telegram webhook changes, bot deployment, or live quota/hibernation checks have occurred.

The local service initiates every cloud connection and sends Telegram replies directly. It exposes no public inbound endpoint. With `[cloud]` absent, the existing `getUpdates` bot path remains active.

## Delivery protocol

1. The Worker validates the Telegram webhook or Mini App request, writes the complete request to D1, and assigns a sequence number. Telegram submissions deduplicate by update ID; Mini App actions deduplicate by client request ID.
2. After the D1 write, the Worker sends the full request envelope over the hibernating WebSocket. A failed notification leaves the D1 request available for replay. The REST backlog is authoritative after startup and every WebSocket reconnect.
3. Rust validates the envelope and JavaScript-safe sequence number, then durably writes the full request to its private inbox under the configured video download root. It sends `POST /api/local/ack` only after that write succeeds. WebSocket delivery uses this same HTTPS receipt; the WebSocket itself has no ACK frame.
4. The inbox deduplicates by exact sequence and envelope, not by a maximum-sequence watermark. An unseen lower sequence is accepted. Re-delivery of an identical sequence wakes recovery without creating another local record; a conflicting envelope for a used sequence is rejected.
5. Local receipt and operation completion are separate. Pending management work survives restart; a confirmed file batch uses its persisted preview and move journal to resume unfinished steps. An interrupted Telegram update whose external side effect is uncertain is surfaced as uncertain rather than blindly replayed.
6. Rust separately publishes operation results and snapshots through HTTPS. The client fetches the oldest pending batch at startup, on reconnect, and on a low-frequency fallback poll (30 seconds by default, configurable no lower than 5 seconds). WebSocket reconnect uses bounded backoff.

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

The state payload uses a locally persisted, increasing, JavaScript-safe `state_version`. A content retry reuses its version and content; D1 rejects lower versions and treats identical same-version content idempotently. `created_at` and `reported_at` are Unix milliseconds; `library.scanned_at` is Unix seconds. Before the first successful scan, Rust reports a `ready: false` placeholder rather than an empty library.

State and receipt writes do not call the notification object. A five-minute heartbeat republishes the cached library snapshot and does not scan the filesystem. A scan runs at startup, on an explicit library-scan request, after a newly published download completes, or after a local management operation changes the library. Scan failures preserve the prior snapshot and report a sanitized warning. The UI shows report and scan times; absence of the first scan means waiting for synchronization, not an empty library. The UI considers the local service stale after ten minutes without a report.

## Local library and scan boundaries

- Scan only the configured video download root; never traverse the whole disk or follow paths outside it. The recursive scan has a depth limit of 48, a 200,000-entry limit, a 50,000-media-item limit, and a 120-second budget.
- Skip private queue, staging, cloud-inbox, and library-internal directories. Symlinks and special files are not followed or opened. Non-UTF-8 media paths are skipped with a warning.
- Only directories fully visited by the scan can establish that a prior item is missing. An unreadable or unavailable subtree remains unavailable; unvisited paths at a depth, entry, or time limit are not marked missing.
- Rebuild the index from local files, NFO metadata, and completed queue hints. The index is not authoritative content. Queue hints include per-entry stable Bilibili IDs so multi-part entries do not collapse to one collection-level identity.
- Probe media using an already opened, no-follow file descriptor duplicated to `ffprobe` stdin and read through `/dev/fd/0`; do not reopen a public pathname in the subprocess. File Provider coordination ends before waiting for the process. Probe time is limited to 3 seconds and captured output to 64 KiB. If `ffprobe` is absent or returns no usable stream, basic library management remains available without those media specifications.

## File operation and recovery guarantees

- Every selected source and target is resolved under the configured root. Moves use no-replace operations and persist a per-file batch journal; a multi-file batch is not an atomic filesystem transaction.
- Preview and execution protect three properties: object identity, content stability, and access policy. The file token records device/inode/type, size, mode, owner/group, creation time where available, and SHA-256 content. The digest detects same-size content edits; owner/mode detect ordinary POSIX access-policy changes. Modification time, change time, and link count are not mutation verdicts by themselves because harmless metadata and File Provider materialization can change them.
- Revalidation distinguishes absence, a readable mismatch, and an unreadable or failed check. Before a move, the source must still match the approved token and the target must remain absent. After a move, the destination is checked again. During restart recovery, a missing source plus a destination matching the complete approved token is recognized as an already completed step. A replacement, different target, changed content/policy, or failed revalidation is left untouched and reported; the operation does not continue from path names alone.
- Creation time can change during a benign macOS metadata update. A birth-time-only difference is accepted only while the process retains the original open descriptor and the current path still names that same file, with matching type, size, SHA-256, owner/group, and mode. Without that descriptor anchor, including after restart or preview-cache eviction, a token mismatch requires a fresh preview. Descriptor retention is bounded; it does not relax content or access-policy checks.
- NFO patches are prepared with the preview and applied only after confirmation. The UI shows each known field's old and new values and the exact source IDs being added. Existing NFO identity and SHA-256 are checked before atomic replacement; the media object is also revalidated. Unknown XML content is retained, ordinary POSIX permissions are preserved, and a retry recognizes the exact prepared after-content digest. This does not claim arbitrary extended-ACL preservation.
- Closing the Mini App does not cancel an accepted operation. Retryable File Provider access failures use persisted local backoff; permanent or stale preview failures require a fresh preview.
- Each confirmed batch attempts one Telegram `sendMessage`, persists a `sending` receipt before the call, then persists the returned message ID and edits that message for the terminal summary. Telegram has no idempotency key for `sendMessage`: if Telegram accepts the message but the process stops before saving its ID, the local receipt cannot identify that message. The retry will not send a duplicate and will surface a warning; `/file` remains the source of the durable operation result. Current messages provide start and final status, not a guarantee of per-file live progress.

## Configuration and production handoff

The optional local `[cloud]` configuration contains `worker_url`, `shared_secret`, and `fallback_poll_seconds`. `TELEGRAM_VIDEO_DOWNLOADER_CLOUD_SHARED_SECRET` can supply the shared secret without placing it in a tracked configuration. Secret values are omitted from serialization and redacted in `Debug` output. Cloud mode requires HTTPS, `allow_all_chats = false`, and exactly one positive private-chat ID; it must match the Worker's `OWNER_USER_ID`.

The Worker uses the `DB` D1 binding, a `NOTIFIER` Durable Object binding, and static assets. Its secrets are `LOCAL_SHARED_SECRET`, `WEBHOOK_SECRET`, and `BOT_TOKEN`. Cloudflare account credentials are used only by deployment tooling and are not given to the frontend or Rust runtime.

Resource creation, schema migration, secret configuration, and Telegram webhook switching remain explicit deployment steps. Existing `getUpdates` and webhook ingestion must not run simultaneously against the production bot. Preserve pending Telegram updates during handoff. No production bot, webhook, secret, or cloud resource was changed for this branch.

## Checks and remaining validation

- With Node.js 25.8.2 and Wrangler 4.149.0, `npm run typecheck`, `npm test` (23/23), and `npm run build` passed. The build is a deployment dry-run, not a deployment.
- With Rust 1.95.0, `cargo fmt --all -- --check`, `cargo clippy --all-targets --locked --offline -- -D warnings`, `cargo test --all-targets --locked --offline --quiet`, and `cargo build --locked --offline` passed. The full test suite reports 572 passed, 0 failed, and 11 ignored; ignored tests were not executed as passing tests. The local build did not install or restart the bot.
- Rust local tests cover the inbox, management operations, queue-publication scan trigger, local mock Cloud REST persistence/ACK/restart/dispatch, and mock Telegram interactions, including a single batch-status message and editing it after failure. Library tests cover incomplete scans, non-overwriting moves, partial recovery, metadata-only confirmation, content/policy/replacement rejection, and benign metadata changes with an original descriptor anchor.
- CI includes Rust and Cloudflare checks, type checking, and local deployment-bundle validation without production credentials. These workflows have not yet run remotely for this branch.
- Real Durable Object hibernation, production quota behavior, configured-owner alignment, and webhook switching require a separate live deployment check; local simulations do not establish those facts.
