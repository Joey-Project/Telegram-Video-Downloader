# Cloud file manager

This Cloudflare Worker hosts the private Telegram Mini Apps at `/file/` and `/settings/`, validates Telegram Mini App `initData`, stores owner requests in a durable D1 inbox, and uses a hibernating Durable Object only to notify the connected desktop client. D1 remains the reconnect source of truth. The desktop reports its registered-library snapshots, applied settings, and migration status over the authenticated local API; downloads and file operations continue to run on that desktop.

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
- `BOT_TOKEN`: used only to verify Telegram Mini App `initData` signatures.

The three secrets must be supplied through the runtime's secret store. They are never returned by API responses or embedded in the static app.

## API outline

- `POST /webhook`: verify Telegram's webhook secret, persist supported owner-private updates, then send a best-effort live notification.
- `GET /api/state` and `GET /api/library`: owner-only Mini App reads. The state endpoint includes safe summaries of up to 100 unacknowledged cloud inbox requests, without their payloads.
- `POST /api/actions`: owner-only validated and idempotent file-management, settings, download-submission, and migration requests. Apply `migrations/0003_media_library_request_kinds.sql` before enabling the new action kinds.
- `GET /api/local/requests`, `POST /api/local/ack`, `POST /api/local/state`, and `GET /api/local/ws`: shared-secret desktop integration. Inbox acknowledgment means durable local receipt, not completion.

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

The first request-list read bootstraps the D1 `database_epoch`; the desktop must send it in `X-Cloud-Database-Epoch` on subsequent request-list, acknowledgment, state, and WebSocket calls. Writes and WebSocket handshakes without a valid epoch fail closed. Acknowledgments carry a `receipts` array containing each request's `seq`, `request_id`, `payload_hash`, `kind`, and `created_at`; the Worker checks every existing row's identity before acknowledging any row in the batch. If a pending sequence collides with a different durable local envelope, the desktop may request conditional epoch rotation only when the current D1 row still matches that request's stable ID, payload hash, kind, and creation time. Rotation clears the old library snapshot but preserves inbox rows; the desktop restarts against a separate local inbox namespace. Older local receipt directories are retained and their previews require a fresh preview before confirmation.

All timestamps ending in `created_at` or `reported_at` are Unix milliseconds. `library.scanned_at` is Unix seconds.

`settings.applied` contains the locally verified registry revision, default library/category, and
library descriptors. `library.libraries` contains each library's independent snapshot and
availability; `library.migrations` contains durable progress and recovery controls. Legacy flat
library fields remain an alias for the default library. A pending inbox request never becomes
an applied setting until the desktop has validated and saved it. See
[media-library behavior and recovery](../docs/MEDIA_LIBRARIES.md).
