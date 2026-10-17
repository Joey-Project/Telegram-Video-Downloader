import type { InboxEnvelope } from "./types";
import { HttpError } from "./types";
import { requireLocalAuthorization, requireOwner, requireWebhookAuthorization } from "./security";
import { acknowledge, emptySnapshot, enqueue, getPending, getPendingSummary, readSnapshot, storeSnapshot } from "./store";
import {
  MAX_ACTION_BODY_BYTES,
  MAX_STATE_BODY_BYTES,
  MAX_WEBHOOK_BODY_BYTES,
  parseAckSeqs,
  parseLimit,
  readJsonBody,
  isOwnerPrivateTelegramUpdate,
  validateActionRequest,
  validateSnapshot,
  validateTelegramUpdate,
} from "./validation";
export { NotificationHub } from "./notification_hub";

const NOTIFIER_ID = "telegram-single-owner";

function json(data: unknown, status = 200): Response {
  return Response.json(data, {
    status,
    headers: { "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff" },
  });
}

function persistedResponse(status: number, seq: number, duplicate: boolean): Response {
  return json({ seq, duplicate, stored: true }, status);
}

async function notifyAfterPersist(env: Env, envelope: InboxEnvelope): Promise<void> {
  try {
    await env.NOTIFIER.getByName(NOTIFIER_ID).notify(envelope);
  } catch {
    // D1 is the durable inbox; live notifications are only a low-latency hint.
    console.warn(JSON.stringify({ event: "inbox_notification_failed", seq: envelope.seq }));
  }
}

async function handleWebhook(request: Request, env: Env): Promise<Response> {
  requireWebhookAuthorization(request, env);
  const rawUpdate = validateTelegramUpdate(await readJsonBody(request, MAX_WEBHOOK_BODY_BYTES));
  if (!isOwnerPrivateTelegramUpdate(rawUpdate, env.OWNER_USER_ID)) {
    return json({ ok: true, stored: false, ignored: true });
  }
  const result = await enqueue(env.DB, `tg:${rawUpdate.update_id}`, "telegram", rawUpdate);
  await notifyAfterPersist(env, result.envelope);
  return persistedResponse(200, result.envelope.seq, result.duplicate);
}

async function handleAction(request: Request, env: Env): Promise<Response> {
  await requireOwner(request, env);
  const action = validateActionRequest(await readJsonBody(request, MAX_ACTION_BODY_BYTES));
  const result = await enqueue(env.DB, `action:${action.request_id}`, action.kind, action.payload);
  await notifyAfterPersist(env, result.envelope);
  return persistedResponse(202, result.envelope.seq, result.duplicate);
}

async function handleLocalRequests(request: Request, env: Env): Promise<Response> {
  requireLocalAuthorization(request, env);
  const limit = parseLimit(new URL(request.url).searchParams.get("limit"));
  return json({ requests: await getPending(env.DB, limit) });
}

async function handleLocalAck(request: Request, env: Env): Promise<Response> {
  requireLocalAuthorization(request, env);
  const seqs = parseAckSeqs(await readJsonBody(request, 32 * 1024));
  return json({ acked: await acknowledge(env.DB, seqs) });
}

async function handleLocalState(request: Request, env: Env): Promise<Response> {
  requireLocalAuthorization(request, env);
  const snapshot = validateSnapshot(await readJsonBody(request, MAX_STATE_BODY_BYTES));
  const now = Date.now();
  if (snapshot.reported_at > now + 60_000) throw new HttpError(400, "reported_at is too far in the future");
  await storeSnapshot(env.DB, snapshot);
  return json({ stored: true, last_seen: now });
}

async function handleLocalWebSocket(request: Request, env: Env): Promise<Response> {
  requireLocalAuthorization(request, env);
  if (request.method !== "GET" || request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    return json({ error: "WebSocket upgrade required" }, 426);
  }
  return env.NOTIFIER.getByName(NOTIFIER_ID).fetch(request);
}

async function handleState(request: Request, env: Env): Promise<Response> {
  await requireOwner(request, env);
  const [snapshot, pendingRequests] = await Promise.all([
    readSnapshot(env.DB),
    getPendingSummary(env.DB, 100),
  ]);
  return json({ ...(snapshot ?? emptySnapshot()), pending_requests: pendingRequests });
}

async function handleLibrary(request: Request, env: Env): Promise<Response> {
  await requireOwner(request, env);
  const snapshot = await readSnapshot(env.DB);
  if (snapshot === null) {
    return json({ synced: false, library: null, reported_at: null, last_seen: null, stale: true });
  }
  return json({
    synced: true,
    library: snapshot.library,
    reported_at: snapshot.reported_at,
    last_seen: snapshot.last_seen,
    stale: snapshot.stale,
  });
}

async function dispatch(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const path = url.pathname;

  if (path === "/webhook") {
    if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
    return handleWebhook(request, env);
  }
  if (path === "/api/local/requests") {
    if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
    return handleLocalRequests(request, env);
  }
  if (path === "/api/local/ack") {
    if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
    return handleLocalAck(request, env);
  }
  if (path === "/api/local/state") {
    if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
    return handleLocalState(request, env);
  }
  if (path === "/api/local/ws") return handleLocalWebSocket(request, env);
  if (path === "/api/state") {
    if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
    return handleState(request, env);
  }
  if (path === "/api/library") {
    if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
    return handleLibrary(request, env);
  }
  if (path === "/api/actions") {
    if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
    return handleAction(request, env);
  }
  if (path === "/file") {
    return new Response(null, { status: 308, headers: { Location: "/file/", "Cache-Control": "no-store" } });
  }
  if (path.startsWith("/api/")) return json({ error: "Not found" }, 404);
  return env.ASSETS.fetch(request);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      return await dispatch(request, env);
    } catch (error) {
      if (error instanceof HttpError) return json({ error: error.message }, error.status);
      const path = new URL(request.url).pathname;
      console.error(JSON.stringify({ event: "request_failed", path }));
      return json({ error: "Service unavailable" }, 503);
    }
  },
};
