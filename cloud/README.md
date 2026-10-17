# Cloud file manager

This Cloudflare Worker hosts the private Telegram Mini App at `/file/`, validates Telegram Mini App `initData`, stores owner requests in a durable D1 inbox, and uses a hibernating Durable Object only to notify the connected desktop client. D1 remains the reconnect source of truth. The desktop reports its library snapshot over the authenticated local API; file operations continue to run on that desktop.

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
- `POST /api/actions`: owner-only validated and idempotent file-management requests.
- `GET /api/local/requests`, `POST /api/local/ack`, `POST /api/local/state`, and `GET /api/local/ws`: shared-secret desktop integration. Inbox acknowledgment means durable local receipt, not completion.

All timestamps ending in `created_at` or `reported_at` are Unix milliseconds. `library.scanned_at` is Unix seconds.
