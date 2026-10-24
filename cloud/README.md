# Cloud file manager

This Cloudflare Worker hosts the private Telegram Mini Apps at `/file/` and `/settings/`, validates Telegram Mini App `initData`, stores owner requests in a durable D1 inbox, and uses a hibernating Durable Object for desktop ingress notifications and Mini App state-change notifications. D1 remains the reconnect source of truth. The desktop reports its registered-library snapshots, applied settings, and migration status over the authenticated local API; downloads and file operations continue to run on that desktop.

## Local checks

Run from this directory with Node.js 25:

```sh
npm ci
npm run typecheck
npm test
npm run build
```

`build` runs Wrangler's deploy dry-run and does not deploy. `npm run dev` starts a local Worker with local D1 storage. The Wrangler file intentionally uses a placeholder D1 ID and owner ID; configure real resource values only through the separately coordinated environment setup.

## Runtime configuration

The Worker needs these bindings:

- `DB`: D1 inbox and chunked desktop snapshot storage. Apply `migrations/` before serving requests.
- `NOTIFIER`: one Durable Object namespace for the single bot/device scope.
- `OWNER_USER_ID`: the Telegram owner user ID as a string.
- `LOCAL_SHARED_SECRET`: bearer credential for the desktop's `/api/local/*` endpoints.
- `WEBHOOK_SECRET`: Telegram webhook secret-token value.
- `BOT_TOKEN`: used server-side to verify Telegram Mini App `initData` signatures and sign domain-separated, short-lived viewer connection tickets.

The three secrets must be supplied through the runtime's secret store. They are never returned by API responses or embedded in the static app.

## API outline

- `POST /webhook`: verify Telegram's webhook secret, persist supported owner-private updates, then send a best-effort live notification.
- `GET /api/state` and `GET /api/library`: owner-only Mini App reads. The state endpoint includes safe summaries of up to 100 unacknowledged cloud inbox requests, without their payloads.
- `POST /api/updates/session` and `GET /api/updates/ws`: owner-authenticated Mini App subscription setup and hibernating WebSocket notifications. The browser obtains a short-lived connection ticket using Telegram authorization, then supplies it through WebSocket subprotocol headers. The ticket is never placed in the URL or persistent browser storage; no additional manually managed credentials are required.
- `POST /api/actions`: owner-only validated and idempotent file-management, settings, download-submission, metadata-recovery, and migration requests. Apply all pending D1 migrations before enabling their action kinds.
- `GET /api/local/requests`, `POST /api/local/ack`, `POST /api/local/state`, and `GET /api/local/ws`: shared-secret desktop integration. Inbox acknowledgment means durable local receipt, not completion.

## Metadata recovery upgrades

For metadata-recovery upgrades, export a private D1 backup, then apply migration
`0005_metadata_recovery_kind.sql` through `wrangler d1 migrations apply` against the
intended database. It expands the inbox kind constraint while preserving requests,
acknowledgments, and sequence history. Install and verify the matching desktop Rust
service before deploying the new Worker and Mini App assets. Existing action kinds
remain supported throughout this order; rolling back code does not roll back D1 data.

## Live Mini App updates

Both Mini Apps subscribe to lightweight state-change hints instead of polling every twelve
seconds. A successful snapshot commit or inbox change triggers notification only after its
D1 write. Uploading an individual staged content chunk does not publish a new snapshot.
The desktop and browser use separate subscription roles: browsers never receive the
desktop's inbox envelopes or its shared secret.

The browser subscribes before reconciling its current state, and reads the existing
`chunked-v1` HTTP manifest and verified content chunks after a notification. Notifications
are hints, not durable state or permission to act. Refreshes are coalesced without dropping
changes received during a read. Reconnection, returning to the foreground and low-frequency
fallback reads recover changes missed during disconnection or failed notification delivery.
Failed snapshot reads retry after one second, doubling the delay up to one minute until
a read succeeds. These retries are quiet; explicit refreshes retain user-visible errors.
Inactive pages pause unnecessary connection retries and refreshes. Connection authorization
expires with the original Telegram authorization; reconnecting does not extend that deadline.

The Durable Object accepts both client roles through the Hibernation WebSocket API. Idle
connections do not require a running JavaScript timer or a permanently active object. The
object uses an alarm for viewer authorization expiry. Mobile page suspension remains a
separate lifecycle concern and is handled by reconnection and reconciliation.

An action envelope requires `request_id`, `kind`, `payload`, `queued_at_ms` and
`replay_expires_at_ms`. Create the UUID and Unix-millisecond timestamps once for a new action;
retain them unchanged across retries, reloads and tabs. The positive retry interval is at most
six days, with at most five minutes of future creation-clock skew. Both timestamps participate
in same-ID replay validation. A retained exact request returns its original sequence, including
after its deadline. If that row is absent and the deadline has passed, the Worker returns
HTTP 410 with `code: "action_replay_window_expired"` before inserting a new sequence.
Acknowledged rows are retained for seven days. Browser rows with an expired or unverifiable
window require manual outcome checking; they must not be assigned a new timestamp or ID.
Migration-preview payloads additionally require the displayed `source_root_binding` and
`target_root_binding`; the Worker preserves them for validation by the desktop.
File-preview and legacy-import payloads require the displayed `library_id` and `root_binding`.
Metadata-edit drafts retain the import result's original binding. Retrying a saved action must
preserve that original binding; a fresh snapshot must not upgrade an older request to a
replacement registration.
File confirmation validates the binding in its durable preview receipt; a caller cannot replace
that saved binding with a newer registration token.

The first request-list read bootstraps the D1 `database_epoch`; the desktop must send it in `X-Cloud-Database-Epoch` on subsequent request-list, acknowledgment, state, and WebSocket calls. Writes and WebSocket handshakes without a valid epoch fail closed. Acknowledgments carry a `receipts` array containing each request's `seq`, `request_id`, `payload_hash`, `kind`, and `created_at`; the Worker checks every existing row's identity before acknowledging any row in the batch. If a pending sequence collides with a different durable local envelope, the desktop may request conditional epoch rotation only when the current D1 row still matches that request's stable ID, payload hash, kind, and creation time. Rotation clears the old library snapshot but preserves inbox rows; the desktop restarts against a separate local inbox namespace. Older local receipt directories are retained and their previews require a fresh preview before confirmation.

All timestamps ending in `created_at` or `reported_at` are Unix milliseconds. `library.scanned_at` is Unix seconds.

`settings.applied` contains the locally verified registry revision, default library/category, and
library descriptors. `library.libraries` contains each library's independent snapshot and
availability; `library.migrations` contains durable progress and recovery controls. Legacy flat
library fields remain an alias for the default library. A pending inbox request never becomes
an applied setting until the desktop has validated and saved it. See
[media-library behavior and recovery](../docs/MEDIA_LIBRARIES.md).
