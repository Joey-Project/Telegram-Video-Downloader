import type { InboxEnvelope, RequestKind } from "./types";
import { HttpError } from "./types";
import { requireConfiguredOwnerUserId, requireLocalAuthorization, requireOwner, requireWebhookAuthorization } from "./security";
import {
  acknowledge,
  assertDatabaseEpoch,
  type AcknowledgedReceipt,
  createDatabaseEpoch,
  emptySnapshot,
  enqueue,
  getDatabaseEpoch,
  getPendingPage,
  getPendingSummary,
  isDatabaseEpoch,
  readSnapshot,
  readSnapshotContentChunk,
  rotateDatabaseEpochOnSequenceConflict,
  storeChunkedSnapshot,
  storeSnapshotContentChunk,
  storeSnapshot,
} from "./store";
import {
  MAX_ACTION_BODY_BYTES,
  MAX_CHUNKED_SNAPSHOT_MANIFEST_BODY_BYTES,
  MAX_STATE_BODY_BYTES,
  MAX_WEBHOOK_BODY_BYTES,
  isRecord,
  parseLimit,
  readJsonBody,
  readJsonBodyWithByteLength,
  readUtf8ChunkBody,
  isOwnerPrivateTelegramUpdate,
  validateActionRequest,
  validateChunkedSnapshot,
  validatePreparedSnapshot,
  validateSnapshot,
  validateTelegramUpdate,
} from "./validation";
import type { MiniAppUpdateEvent } from "./notification_hub";
import {
  issueMiniAppUpdatesTicket,
  MINIAPP_UPDATES_PROTOCOL,
  MINIAPP_UPDATES_SESSION_PATH,
  MINIAPP_UPDATES_SOCKET_PATH,
  NOTIFIER_ROLE_HEADER,
  NOTIFIER_VIEWER_AUTH_EXPIRY_HEADER,
  NOTIFIER_VIEWER_EPOCH_HEADER,
  NOTIFIER_VIEWER_OWNER_HEADER,
  NOTIFIER_VIEWER_TICKET_EXPIRY_HEADER,
  readMiniAppUpdatesTicketProtocol,
  verifyMiniAppUpdatesTicket,
} from "./updates";
export { NotificationHub } from "./notification_hub";

const NOTIFIER_ID = "telegram-single-owner";
const CLOUD_REQUEST_PAGE_SIZE = 100;
const MAX_ACK_RECEIPTS = 500;
const MAX_ACK_BODY_BYTES = 128 * 1024;
const MAX_EPOCH_ROTATION_REQUEST_ID_LENGTH = 256;
const SHA256_HEX_PATTERN = /^[0-9a-f]{64}$/u;
const REQUEST_KINDS = new Set<RequestKind>([
  "telegram",
  "library_scan",
  "library_register",
  "library_set_default",
  "library_set_category",
  "library_rename",
  "library_remove",
  "metadata_recover",
  "file_preview",
  "file_confirm",
  "preview_cancel",
  "legacy_import",
  "download_submit",
  "library_migration_preview",
  "library_migration_confirm",
  "library_migration_control",
]);

function isRequestKind(value: unknown): value is RequestKind {
  return typeof value === "string" && REQUEST_KINDS.has(value as RequestKind);
}

function parseAckReceipts(value: unknown): AcknowledgedReceipt[] {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new HttpError(400, "Acknowledgment body must be an object");
  }
  const receiptsValue = (value as Record<string, unknown>).receipts;
  if (!Array.isArray(receiptsValue) || receiptsValue.length > MAX_ACK_RECEIPTS) {
    throw new HttpError(400, "Acknowledgment receipts must contain at most 500 entries");
  }
  const seen = new Set<number>();
  const receipts: AcknowledgedReceipt[] = [];
  for (const candidate of receiptsValue) {
    if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
      throw new HttpError(400, "Acknowledgment receipt must be an object");
    }
    const record = candidate as Record<string, unknown>;
    const { seq, request_id: requestId, payload_hash: payloadHash, kind, created_at: createdAt } = record;
    if (typeof seq !== "number" || !Number.isSafeInteger(seq) || seq <= 0 ||
      typeof requestId !== "string" || requestId.length === 0 ||
      new TextEncoder().encode(requestId).byteLength > MAX_EPOCH_ROTATION_REQUEST_ID_LENGTH ||
      typeof payloadHash !== "string" || !SHA256_HEX_PATTERN.test(payloadHash) ||
      !isRequestKind(kind) ||
      typeof createdAt !== "number" || !Number.isSafeInteger(createdAt) || createdAt <= 0 ||
      seen.has(seq)) {
      throw new HttpError(400, "Acknowledgment receipt is malformed or duplicated");
    }
    seen.add(seq);
    receipts.push({
      seq,
      request_id: requestId,
      payload_hash: payloadHash,
      kind,
      created_at: createdAt,
    });
  }
  return receipts;
}

function json(data: unknown, status = 200, extraHeaders: Record<string, string> = {}): Response {
  return Response.json(data, {
    status,
    headers: { "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", ...extraHeaders },
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

async function notifyViewersAfterPersist(
  env: Env,
  reason: MiniAppUpdateEvent["reason"],
  databaseEpoch?: string,
  stateVersion?: number,
): Promise<void> {
  try {
    const epoch = databaseEpoch ?? await getDatabaseEpoch(env.DB);
    const event: MiniAppUpdateEvent = {
      type: "invalidate",
      database_epoch: epoch,
      reason,
      ...(stateVersion === undefined ? {} : { state_version: stateVersion }),
    };
    await env.NOTIFIER.getByName(NOTIFIER_ID).notifyViewers(event);
  } catch {
    // D1 is authoritative; viewer updates are hints and cannot undo a durable write.
    console.warn(JSON.stringify({ event: "miniapp_update_notification_failed", reason }));
  }
}

async function handleWebhook(request: Request, env: Env): Promise<Response> {
  requireWebhookAuthorization(request, env);
  const ownerUserId = requireConfiguredOwnerUserId(env.OWNER_USER_ID);
  const rawUpdate = validateTelegramUpdate(await readJsonBody(request, MAX_WEBHOOK_BODY_BYTES));
  if (!isOwnerPrivateTelegramUpdate(rawUpdate, ownerUserId)) {
    return json({ ok: true, stored: false, ignored: true });
  }
  const result = await enqueue(env.DB, `tg:${rawUpdate.update_id}`, "telegram", rawUpdate);
  await notifyAfterPersist(env, result.envelope);
  await notifyViewersAfterPersist(env, "inbox");
  return persistedResponse(200, result.envelope.seq, result.duplicate);
}

async function handleAction(request: Request, env: Env): Promise<Response> {
  await requireOwner(request, env);
  const action = validateActionRequest(await readJsonBody(request, MAX_ACTION_BODY_BYTES));
  const result = await enqueue(env.DB, `action:${action.request_id}`, action.kind, action.payload, {
    queued_at_ms: action.queued_at_ms,
    replay_expires_at_ms: action.replay_expires_at_ms,
  });
  await notifyAfterPersist(env, result.envelope);
  await notifyViewersAfterPersist(env, "inbox");
  return persistedResponse(202, result.envelope.seq, result.duplicate);
}

async function handleLocalRequests(request: Request, env: Env): Promise<Response> {
  requireLocalAuthorization(request, env);
  const limit = parseLimit(new URL(request.url).searchParams.get("limit"), CLOUD_REQUEST_PAGE_SIZE);
  const page = await getPendingPage(env.DB, limit);
  const expectedEpoch = request.headers.get("X-Cloud-Database-Epoch");
  if (expectedEpoch !== null) {
    if (!isDatabaseEpoch(expectedEpoch)) throw new HttpError(400, "Cloud database epoch is invalid");
    if (expectedEpoch !== page.database_epoch) {
      throw new HttpError(409, "Cloud database epoch changed", undefined, page.database_epoch);
    }
  }
  return json(page);
}

async function handleLocalAck(request: Request, env: Env): Promise<Response> {
  requireLocalAuthorization(request, env);
  const databaseEpoch = await assertDatabaseEpoch(env.DB, request.headers.get("X-Cloud-Database-Epoch"));
  const receipts = parseAckReceipts(await readJsonBody(request, MAX_ACK_BODY_BYTES));
  const acked = await acknowledge(env.DB, receipts, databaseEpoch);
  if (receipts.length > 0) await notifyViewersAfterPersist(env, "inbox", databaseEpoch);
  return json({ acked });
}

async function handleLocalSnapshotChunk(request: Request, env: Env, sha256: string): Promise<Response> {
  requireLocalAuthorization(request, env);
  const databaseEpoch = await assertDatabaseEpoch(env.DB, request.headers.get("X-Cloud-Database-Epoch"));
  const { bytes, data } = await readUtf8ChunkBody(request);
  await storeSnapshotContentChunk(env.DB, databaseEpoch, sha256, bytes, data);
  return json({ stored: true, sha256 });
}

async function handleLocalState(request: Request, env: Env): Promise<Response> {
  requireLocalAuthorization(request, env);
  const databaseEpoch = await assertDatabaseEpoch(env.DB, request.headers.get("X-Cloud-Database-Epoch"));
  const { value: body, byteLength } = await readJsonBodyWithByteLength(request, MAX_STATE_BODY_BYTES);
  if (isRecord(body) && body.format === "chunked-v1") {
    if (byteLength > MAX_CHUNKED_SNAPSHOT_MANIFEST_BODY_BYTES) {
      throw new HttpError(413, "Chunked snapshot manifest request is too large");
    }
    const snapshot = validateChunkedSnapshot(body);
    const now = Date.now();
    if (snapshot.reported_at > now + 60_000) throw new HttpError(400, "reported_at is too far in the future");
    await storeChunkedSnapshot(env.DB, snapshot, databaseEpoch);
    await notifyViewersAfterPersist(env, "state", databaseEpoch, snapshot.state_version);
    return json({ stored: true, last_seen: now });
  }
  const prepared = isRecord(body) && Object.hasOwn(body, "format") ? validatePreparedSnapshot(body) : undefined;
  const snapshot = prepared?.snapshot ?? validateSnapshot(body);
  const now = Date.now();
  if (snapshot.reported_at > now + 60_000) throw new HttpError(400, "reported_at is too far in the future");
  await storeSnapshot(env.DB, snapshot, databaseEpoch, prepared);
  await notifyViewersAfterPersist(env, "state", databaseEpoch, snapshot.state_version);
  return json({ stored: true, last_seen: now });
}

async function handleLocalEpochRotation(request: Request, env: Env): Promise<Response> {
  requireLocalAuthorization(request, env);
  const expectedEpoch = request.headers.get("X-Cloud-Database-Epoch");
  await assertDatabaseEpoch(env.DB, expectedEpoch);
  const body = await readJsonBody(request, 8 * 1024);
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    throw new HttpError(400, "Epoch rotation request must be an object");
  }
  const record = body as Record<string, unknown>;
  const seq = record.seq;
  const requestId = record.request_id;
  const payloadHash = record.payload_hash;
  const kind = record.kind;
  const createdAt = record.created_at;
  if (typeof seq !== "number" || !Number.isSafeInteger(seq) || seq <= 0 ||
    typeof requestId !== "string" || requestId.length === 0 || requestId.length > MAX_EPOCH_ROTATION_REQUEST_ID_LENGTH ||
    typeof payloadHash !== "string" || !SHA256_HEX_PATTERN.test(payloadHash) ||
    !isRequestKind(kind) ||
    typeof createdAt !== "number" || !Number.isSafeInteger(createdAt) || createdAt <= 0) {
    throw new HttpError(400, "Epoch rotation request is malformed");
  }
  const nextEpoch = createDatabaseEpoch();
  const result = await rotateDatabaseEpochOnSequenceConflict(env.DB, expectedEpoch!, nextEpoch, {
    seq,
    request_id: requestId,
    payload_hash: payloadHash,
    kind,
    created_at: createdAt,
  });
  if (!result.rotated) {
    throw new HttpError(409, "Cloud sequence conflict no longer matches", undefined, result.database_epoch);
  }
  await notifyViewersAfterPersist(env, "epoch", result.database_epoch);
  return json({ rotated: true, database_epoch: result.database_epoch });
}

async function handleLocalWebSocket(request: Request, env: Env): Promise<Response> {
  requireLocalAuthorization(request, env);
  await assertDatabaseEpoch(env.DB, request.headers.get("X-Cloud-Database-Epoch"));
  if (request.method !== "GET" || request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    return json({ error: "WebSocket upgrade required" }, 426);
  }
  const headers = new Headers(request.headers);
  headers.set(NOTIFIER_ROLE_HEADER, "local-client");
  headers.delete(NOTIFIER_VIEWER_OWNER_HEADER);
  headers.delete(NOTIFIER_VIEWER_EPOCH_HEADER);
  headers.delete(NOTIFIER_VIEWER_TICKET_EXPIRY_HEADER);
  headers.delete(NOTIFIER_VIEWER_AUTH_EXPIRY_HEADER);
  return env.NOTIFIER.getByName(NOTIFIER_ID).fetch(new Request(request, { headers }));
}

async function handleMiniAppUpdatesSession(request: Request, env: Env): Promise<Response> {
  const owner = await requireOwner(request, env);
  const databaseEpoch = await getDatabaseEpoch(env.DB);
  const issued = await issueMiniAppUpdatesTicket(env.BOT_TOKEN, owner.userId, databaseEpoch, owner.expiresAt);
  return json({
    ticket: issued.ticket,
    websocket_path: MINIAPP_UPDATES_SOCKET_PATH,
    protocol: MINIAPP_UPDATES_PROTOCOL,
    expires_at: owner.expiresAt,
    database_epoch: databaseEpoch,
  });
}

async function handleMiniAppUpdatesWebSocket(request: Request, env: Env): Promise<Response> {
  if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
  if (new URL(request.url).search.length > 0) return json({ error: "WebSocket ticket must not be in the URL" }, 400);
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    return json({ error: "WebSocket upgrade required" }, 426);
  }
  const ticket = readMiniAppUpdatesTicketProtocol(request.headers.get("sec-websocket-protocol"));
  if (ticket === null) return json({ error: "Unauthorized" }, 401);
  const claims = await verifyMiniAppUpdatesTicket(ticket, env.BOT_TOKEN, env.OWNER_USER_ID);
  if (claims === null) return json({ error: "Unauthorized" }, 401);
  await assertDatabaseEpoch(env.DB, claims.database_epoch);

  const headers = new Headers(request.headers);
  headers.set("sec-websocket-protocol", MINIAPP_UPDATES_PROTOCOL);
  headers.set(NOTIFIER_ROLE_HEADER, "viewer");
  headers.set(NOTIFIER_VIEWER_OWNER_HEADER, claims.owner_user_id);
  headers.set(NOTIFIER_VIEWER_EPOCH_HEADER, claims.database_epoch);
  headers.set(NOTIFIER_VIEWER_TICKET_EXPIRY_HEADER, String(claims.ticket_expires_at));
  headers.set(NOTIFIER_VIEWER_AUTH_EXPIRY_HEADER, String(claims.auth_expires_at));
  headers.delete("x-telegram-init-data");
  headers.delete("authorization");
  return env.NOTIFIER.getByName(NOTIFIER_ID).fetch(new Request(request, { headers }));
}

function requireChunkedSnapshotCapability(request: Request): void {
  if (request.headers.get("X-Snapshot-Format") === "chunked-v1") return;
  throw new HttpError(
    426,
    "A new Mini App version is available. Close and reopen it from Telegram.",
    undefined,
    undefined,
    undefined,
    "snapshot_client_upgrade_required",
  );
}

async function handleState(request: Request, env: Env): Promise<Response> {
  await requireOwner(request, env);
  const [snapshot, pendingRequests] = await Promise.all([
    readSnapshot(env.DB),
    getPendingSummary(env.DB, CLOUD_REQUEST_PAGE_SIZE),
  ]);
  if (snapshot?.transport === "chunked-v1") {
    requireChunkedSnapshotCapability(request);
    return json({
      format: "chunked-v1",
      state_version: snapshot.state_version,
      reported_at: snapshot.reported_at,
      library_revision: snapshot.library_revision,
      library: snapshot.library,
      state: snapshot.state,
      synced: true,
      last_seen: snapshot.last_seen,
      stale: snapshot.stale,
      pending_requests: pendingRequests,
      database_epoch: snapshot.database_epoch,
    });
  }
  return json({ ...(snapshot ?? emptySnapshot()), pending_requests: pendingRequests });
}

async function handleLibrary(request: Request, env: Env): Promise<Response> {
  await requireOwner(request, env);
  const snapshot = await readSnapshot(env.DB);
  if (snapshot === null) {
    return json({ synced: false, library: null, reported_at: null, last_seen: null, stale: true });
  }
  if (snapshot.transport === "chunked-v1") {
    requireChunkedSnapshotCapability(request);
    return json({
      format: "chunked-v1",
      database_epoch: snapshot.database_epoch,
      state_version: snapshot.state_version,
      library_revision: snapshot.library_revision,
      synced: true,
      library: snapshot.library,
      reported_at: snapshot.reported_at,
      last_seen: snapshot.last_seen,
      stale: snapshot.stale,
    });
  }
  return json({
    synced: true,
    library: snapshot.library,
    reported_at: snapshot.reported_at,
    last_seen: snapshot.last_seen,
    stale: snapshot.stale,
  });
}

async function handleSnapshotContentChunk(request: Request, env: Env, sha256: string): Promise<Response> {
  await requireOwner(request, env);
  const databaseEpoch = await assertDatabaseEpoch(env.DB, request.headers.get("X-Cloud-Database-Epoch"));
  const chunk = await readSnapshotContentChunk(env.DB, databaseEpoch, sha256);
  if (chunk === null) return json({ error: "Snapshot content chunk not found" }, 404);
  return json(chunk);
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
  if (path === "/api/local/epoch/rotate-on-conflict") {
    if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
    return handleLocalEpochRotation(request, env);
  }
  if (path === "/api/local/state") {
    if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
    return handleLocalState(request, env);
  }
  const localChunkMatch = /^\/api\/local\/state\/chunks\/([0-9a-f]{64})$/u.exec(path);
  if (localChunkMatch !== null) {
    if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
    return handleLocalSnapshotChunk(request, env, localChunkMatch[1]!);
  }
  if (path === "/api/local/ws") return handleLocalWebSocket(request, env);
  if (path === MINIAPP_UPDATES_SESSION_PATH) {
    if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
    return handleMiniAppUpdatesSession(request, env);
  }
  if (path === MINIAPP_UPDATES_SOCKET_PATH) return handleMiniAppUpdatesWebSocket(request, env);
  if (path === "/api/state") {
    if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
    return handleState(request, env);
  }
  if (path === "/api/library") {
    if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
    return handleLibrary(request, env);
  }
  const ownerChunkMatch = /^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u.exec(path);
  if (ownerChunkMatch !== null) {
    if (request.method !== "GET") return json({ error: "Method not allowed" }, 405);
    return handleSnapshotContentChunk(request, env, ownerChunkMatch[1]!);
  }
  if (path === "/api/actions") {
    if (request.method !== "POST") return json({ error: "Method not allowed" }, 405);
    return handleAction(request, env);
  }
  if (path === "/file") {
    return new Response(null, { status: 308, headers: { Location: "/file/", "Cache-Control": "no-store" } });
  }
  if (path === "/settings") {
    return new Response(null, { status: 308, headers: { Location: "/settings/", "Cache-Control": "no-store" } });
  }
  if (path.startsWith("/api/")) return json({ error: "Not found" }, 404);
  return env.ASSETS.fetch(request);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      return await dispatch(request, env);
    } catch (error) {
      if (error instanceof HttpError) {
        const extraHeaders: Record<string, string> = {};
        const body: Record<string, unknown> = { error: error.message };
        if (error.code !== undefined) body.code = error.code;
        const stateVersion = error.stateVersion;
        if (error.status === 409 && Number.isSafeInteger(stateVersion) && stateVersion !== undefined && stateVersion > 0) {
          extraHeaders["X-Cloud-State-Version"] = String(stateVersion);
        }
        if (error.status === 409 && error.databaseEpoch !== undefined && isDatabaseEpoch(error.databaseEpoch)) {
          extraHeaders["X-Cloud-Database-Epoch"] = error.databaseEpoch;
        }
        if (error.status === 409 && error.sequenceConflict !== undefined &&
          Number.isSafeInteger(error.sequenceConflict) && error.sequenceConflict > 0) {
          extraHeaders["X-Cloud-Inbox-Sequence-Conflict"] = String(error.sequenceConflict);
        }
        return json(body, error.status, extraHeaders);
      }
      const path = new URL(request.url).pathname;
      console.error(JSON.stringify({ event: "request_failed", path }));
      return json({ error: "Service unavailable" }, 503);
    }
  },
};
