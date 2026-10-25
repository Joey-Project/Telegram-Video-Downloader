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

## Observability

The tracked Wrangler config enables Workers Logs and traces with independent head-based sampling: 10% of requests contribute invocation and custom logs, including error details emitted for sampled requests, and 1% of requests are traced. Logs and traces can still be limited by platform ingestion, so this does not guarantee that every error appears in retained logs. Workers request and error metrics are reported separately from sampled logs and traces. See [Workers Logs](https://developers.cloudflare.com/workers/observability/logs/workers-logs/), [Workers Traces](https://developers.cloudflare.com/workers/observability/traces/), and [Workers metrics and analytics](https://developers.cloudflare.com/workers/observability/metrics-and-analytics/).

Increase trace sampling only for a bounded debugging window, then restore the tracked rate. The private production config must mirror this policy at deploy time; validate the settings on the deployed Worker afterward.

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

## On-demand time snapshots

The Mac stores authoritative local state. Cloudflare stores a snapshot from a particular
time, rather than a continuously mirrored view. Local disk-capacity changes, task progress
and background scans do not by themselves publish another cloud snapshot. The idle
refresh expires one hour after the last successful publication; user-requested publication
moves that deadline forward, while cache reads and failed uploads do not. The deadline
survives restart alongside the existing local state-version metadata.
An hourly background refresh checks availability/capacity and publishes the latest
local index; it does not start another full file scan or metadata backfill.

Both Mini Apps display the saved snapshot and its time before requesting current Mac
data once on open. The refresh button requests an actual Mac refresh. Searching, sorting,
pagination and selection use already loaded data; idle pages do not poll for snapshots.
Reference-data requests refresh the required scope, and business operations return their
current references and results in the same publication instead of requiring a separate
pre-refresh exchange. File-index scan times stay distinct from snapshot publication times.

The existing owner-authenticated viewer WebSocket carries bounded ephemeral refresh
requests and foreground operation interest. The Mac returns a correlated refresh result
only after its snapshot upload succeeds. An unavailable or failed refresh leaves the saved
snapshot visible. These read-only control frames never enter the durable business inbox;
commands retain their existing request IDs, receipt acknowledgments and recovery rules.
Hidden/closed pages stop live interest. Active operation results may notify interested
viewers; unobserved progress stays local until the next requested or expired refresh.

Published-result hints allow coalesced reads of the existing `chunked-v1` manifest and
verified content chunks. They are not permission to act: the Mac revalidates every actual
file operation. Uploading a staged chunk alone does not publish a snapshot. The authenticated
local state API accepts `X-Snapshot-Notify-Viewers: 0` to avoid waking the notification
object for an unobserved background publication; an absent header preserves legacy behavior.
The Mac reuses hashes confirmed by its latest successfully committed manifest, so an
hourly refresh does not reupload unchanged active chunks merely because a staging cache
expired. This bounded cache stays in memory and is scoped to epoch/version. Every commit
still checks its chunk references; a missing-chunk response clears the cache and retries
the existing upload path.

The Durable Object uses the Hibernation WebSocket API for both roles, with bounded
attachments and authorization-expiry alarms. No periodic presence heartbeat or separate
credential is required. Browser authorization retains its original deadline across reconnects;
viewer sockets never receive the Mac's business envelopes or shared secret. See the
[implementation contract and validation journal](../docs/project_journal/2026/10/2026-10-25-on-demand-cloud-snapshots-ocs01.md).

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
