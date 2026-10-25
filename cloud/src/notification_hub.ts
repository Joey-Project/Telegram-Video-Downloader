import { DurableObject } from "cloudflare:workers";
import type { InboxEnvelope } from "./types";
import {
  MINIAPP_UPDATES_MAX_VIEWERS,
  MINIAPP_UPDATES_PROTOCOL,
  NOTIFIER_LOCAL_CONTROL_HEADER,
  NOTIFIER_LOCAL_EPOCH_HEADER,
  NOTIFIER_ROLE_HEADER,
  NOTIFIER_VIEWER_AUTH_EXPIRY_HEADER,
  NOTIFIER_VIEWER_EPOCH_HEADER,
  NOTIFIER_VIEWER_OWNER_HEADER,
  NOTIFIER_VIEWER_TICKET_EXPIRY_HEADER,
  SNAPSHOT_CONTROL_VERSION,
} from "./updates";
import { getDatabaseEpoch, isDatabaseEpoch } from "./store";

const LOCAL_CLIENT_TAG = "local-client";
const MINIAPP_VIEWER_TAG = "miniapp-viewer";
const VIEWER_AUTH_CLOSE_CODE = 4401;
const VIEWER_EPOCH_CLOSE_CODE = 4409;
const CONTROL_MESSAGE_MAX_BYTES = 4 * 1024;
const MAX_PENDING_REFRESHES_PER_VIEWER = 8;
const MAX_RECENT_REFRESH_IDS_PER_VIEWER = 32;
const REFRESH_TTL_MS = 120_000;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;
const REFRESH_SCOPES = new Set(["library", "settings", "operations", "download"]);
const REFRESH_REASONS = new Set(["open", "manual", "reference"]);
const ERROR_CODE_PATTERN = /^[a-z][a-z0-9_]{0,47}$/u;
const utf8 = new TextEncoder();

type ViewerSurface = "file" | "settings";
type RefreshScope = "library" | "settings" | "operations" | "download";
type RefreshReason = "open" | "manual" | "reference";

interface PendingRefresh {
  request_id: string;
  expires_at: number;
}

interface SeenRefresh {
  request_id: string;
  expires_at: number;
}

interface ViewerAttachment {
  role: "viewer";
  viewer_id: string;
  owner_user_id: string;
  database_epoch: string;
  auth_expires_at: number;
  visible: boolean;
  surface: ViewerSurface;
  watch: boolean;
  pending_refreshes: PendingRefresh[];
  seen_refresh_ids: SeenRefresh[];
  event_sequence: number;
}

interface LocalAttachment {
  role: "local-client";
  database_epoch: string;
  snapshot_control: "v1" | null;
  event_sequence: number;
}

interface ViewerInterest {
  viewer_id: string;
  surface: ViewerSurface;
  watch: boolean;
  expires_at: number;
}

interface ViewerInterestMessage {
  type: "interest";
  database_epoch: string;
  visible: boolean;
  surface: ViewerSurface;
  watch: boolean;
}

interface RefreshRequestMessage {
  type: "refresh_request";
  database_epoch: string;
  request_id: string;
  reason: RefreshReason;
  scope: RefreshScope;
  library_id?: string;
}

interface RefreshResultMessage {
  type: "refresh_result";
  viewer_id: string;
  request_id: string;
  database_epoch: string;
  status: "published" | "failed" | "unavailable";
  state_version?: number;
  reported_at?: number;
  error_code?: string;
}

export interface MiniAppUpdateEvent {
  type: "invalidate";
  database_epoch: string;
  state_version?: number;
  reason: "state" | "inbox" | "epoch";
}

function jsonError(status: number, message: string, headers: Record<string, string> = {}): Response {
  return Response.json({ error: message }, {
    status,
    headers: { "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", ...headers },
  });
}

function readExpiryHeader(request: Request, header: string): number | null {
  const value = request.headers.get(header) ?? "";
  if (!/^[1-9]\d{0,15}$/u.test(value)) return null;
  const expiry = Number(value);
  return Number.isSafeInteger(expiry) ? expiry : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, required: string[], optional: string[] = []): boolean {
  const allowed = new Set([...required, ...optional]);
  return required.every((key) => Object.hasOwn(value, key)) && Object.keys(value).every((key) => allowed.has(key));
}

function parseControlMessage(value: string | ArrayBuffer): unknown | null {
  if (typeof value !== "string" || utf8.encode(value).byteLength > CONTROL_MESSAGE_MAX_BYTES) return null;
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return null;
  }
}

function isViewerAttachment(value: unknown): value is ViewerAttachment {
  if (!isRecord(value)) return false;
  return value.role === "viewer" && typeof value.viewer_id === "string" && UUID_PATTERN.test(value.viewer_id) &&
    typeof value.owner_user_id === "string" && /^[1-9]\d{0,15}$/u.test(value.owner_user_id) &&
    typeof value.database_epoch === "string" && isDatabaseEpoch(value.database_epoch) &&
    typeof value.auth_expires_at === "number" && Number.isSafeInteger(value.auth_expires_at) && value.auth_expires_at > 0 &&
    typeof value.visible === "boolean" && (value.surface === "file" || value.surface === "settings") &&
    typeof value.watch === "boolean" && Array.isArray(value.pending_refreshes) &&
    value.pending_refreshes.length <= MAX_PENDING_REFRESHES_PER_VIEWER &&
    value.pending_refreshes.every((entry) => isRecord(entry) && typeof entry.request_id === "string" &&
      UUID_PATTERN.test(entry.request_id) && typeof entry.expires_at === "number" &&
      Number.isSafeInteger(entry.expires_at) && entry.expires_at > 0) &&
    Array.isArray(value.seen_refresh_ids) && value.seen_refresh_ids.length <= MAX_RECENT_REFRESH_IDS_PER_VIEWER &&
    value.seen_refresh_ids.every((entry) => isRecord(entry) && typeof entry.request_id === "string" &&
      UUID_PATTERN.test(entry.request_id) && typeof entry.expires_at === "number" &&
      Number.isSafeInteger(entry.expires_at) && entry.expires_at > 0) &&
    typeof value.event_sequence === "number" && Number.isSafeInteger(value.event_sequence) && value.event_sequence >= 0;
}

function isLocalAttachment(value: unknown): value is LocalAttachment {
  if (!isRecord(value)) return false;
  return value.role === "local-client" && typeof value.database_epoch === "string" &&
    isDatabaseEpoch(value.database_epoch) &&
    (value.snapshot_control === null || value.snapshot_control === SNAPSHOT_CONTROL_VERSION) &&
    typeof value.event_sequence === "number" && Number.isSafeInteger(value.event_sequence) && value.event_sequence >= 0;
}

function parseViewerInterest(value: unknown): ViewerInterestMessage | null {
  if (!isRecord(value) || !hasExactKeys(value, ["type", "database_epoch", "visible", "surface", "watch"]) ||
    value.type !== "interest" || typeof value.database_epoch !== "string" || !isDatabaseEpoch(value.database_epoch) ||
    typeof value.visible !== "boolean" || (value.surface !== "file" && value.surface !== "settings") ||
    typeof value.watch !== "boolean") {
    return null;
  }
  return value as unknown as ViewerInterestMessage;
}

function parseRefreshRequest(value: unknown): RefreshRequestMessage | null {
  if (!isRecord(value) || !hasExactKeys(
    value,
    ["type", "database_epoch", "request_id", "reason", "scope"],
    ["library_id"],
  ) || value.type !== "refresh_request" || typeof value.database_epoch !== "string" ||
    !isDatabaseEpoch(value.database_epoch) || typeof value.request_id !== "string" ||
    !UUID_PATTERN.test(value.request_id) || typeof value.reason !== "string" ||
    !REFRESH_REASONS.has(value.reason) || typeof value.scope !== "string" || !REFRESH_SCOPES.has(value.scope)) {
    return null;
  }
  if (Object.hasOwn(value, "library_id") && ((value.scope !== "library" && value.scope !== "download") ||
    typeof value.library_id !== "string" || utf8.encode(value.library_id).byteLength === 0 ||
    utf8.encode(value.library_id).byteLength > 256 ||
    /[\u0000-\u001f\u007f]/u.test(value.library_id))) {
    return null;
  }
  return value as unknown as RefreshRequestMessage;
}

function parseRefreshResult(value: unknown): RefreshResultMessage | null {
  if (!isRecord(value) || !hasExactKeys(
    value,
    ["type", "viewer_id", "request_id", "database_epoch", "status"],
    ["state_version", "reported_at", "error_code"],
  ) || value.type !== "refresh_result" || typeof value.viewer_id !== "string" ||
    !UUID_PATTERN.test(value.viewer_id) || typeof value.request_id !== "string" ||
    !UUID_PATTERN.test(value.request_id) || typeof value.database_epoch !== "string" ||
    !isDatabaseEpoch(value.database_epoch) ||
    (value.status !== "published" && value.status !== "failed" && value.status !== "unavailable")) {
    return null;
  }
  if (value.status === "published") {
    if (typeof value.state_version !== "number" || !Number.isSafeInteger(value.state_version) || value.state_version <= 0 ||
      typeof value.reported_at !== "number" || !Number.isSafeInteger(value.reported_at) || value.reported_at <= 0 ||
      Object.hasOwn(value, "error_code")) {
      return null;
    }
  } else if (Object.hasOwn(value, "state_version") || Object.hasOwn(value, "reported_at") ||
    (Object.hasOwn(value, "error_code") && (typeof value.error_code !== "string" || !ERROR_CODE_PATTERN.test(value.error_code)))) {
    return null;
  }
  return value as unknown as RefreshResultMessage;
}

function makeRefreshResult(
  attachment: ViewerAttachment,
  requestId: string,
  status: RefreshResultMessage["status"],
  errorCode?: string,
): RefreshResultMessage {
  return {
    type: "refresh_result",
    viewer_id: attachment.viewer_id,
    request_id: requestId,
    database_epoch: attachment.database_epoch,
    status,
    ...(errorCode === undefined ? {} : { error_code: errorCode }),
  };
}

function isControlInterest(value: unknown): value is ViewerInterest {
  if (!isRecord(value)) return false;
  return typeof value.viewer_id === "string" && UUID_PATTERN.test(value.viewer_id) &&
    (value.surface === "file" || value.surface === "settings") && typeof value.watch === "boolean" &&
    typeof value.expires_at === "number" && Number.isSafeInteger(value.expires_at) && value.expires_at > 0;
}

export class NotificationHub extends DurableObject<Env> {
  private readonly messageQueues = new WeakMap<WebSocket, Promise<void>>();
  private readonly viewerMutationQueues = new Map<string, Promise<void>>();

  async fetch(request: Request): Promise<Response> {
    if (request.method !== "GET" || request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return jsonError(426, "WebSocket upgrade required");
    }
    const role = request.headers.get(NOTIFIER_ROLE_HEADER);
    if (role === "viewer") return this.acceptViewer(request);
    if (role === "local-client") return this.acceptLocalClient(request);
    return jsonError(401, "Unauthorized");
  }

  private acceptLocalClient(request: Request): Response {
    const databaseEpoch = request.headers.get(NOTIFIER_LOCAL_EPOCH_HEADER) ?? "";
    const snapshotControl = request.headers.get(NOTIFIER_LOCAL_CONTROL_HEADER);
    if (!isDatabaseEpoch(databaseEpoch) || (snapshotControl !== null && snapshotControl !== SNAPSHOT_CONTROL_VERSION)) {
      return jsonError(401, "Unauthorized");
    }
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    if (!client || !server) return jsonError(500, "WebSocket setup failed");
    this.ctx.acceptWebSocket(server, [LOCAL_CLIENT_TAG]);
    server.serializeAttachment({
      role: "local-client",
      database_epoch: databaseEpoch,
      snapshot_control: snapshotControl === SNAPSHOT_CONTROL_VERSION ? SNAPSHOT_CONTROL_VERSION : null,
      event_sequence: 0,
    } satisfies LocalAttachment);
    if (snapshotControl === SNAPSHOT_CONTROL_VERSION) {
      try {
        server.send(JSON.stringify(this.viewerInterestMessage(databaseEpoch, Date.now())));
      } catch {
        try {
          server.close(1011, "Viewer interest replay failed");
        } catch {
          // The runtime removes closed sockets from the WebSocket set.
        }
        return jsonError(503, "WebSocket setup failed");
      }
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  private async acceptViewer(request: Request): Promise<Response> {
    const ownerUserId = request.headers.get(NOTIFIER_VIEWER_OWNER_HEADER) ?? "";
    const databaseEpoch = request.headers.get(NOTIFIER_VIEWER_EPOCH_HEADER) ?? "";
    const ticketExpiresAt = readExpiryHeader(request, NOTIFIER_VIEWER_TICKET_EXPIRY_HEADER);
    const authExpiresAt = readExpiryHeader(request, NOTIFIER_VIEWER_AUTH_EXPIRY_HEADER);
    if (ownerUserId !== this.env.OWNER_USER_ID || !/^[1-9]\d{0,15}$/u.test(ownerUserId) ||
      !isDatabaseEpoch(databaseEpoch) || ticketExpiresAt === null || authExpiresAt === null ||
      request.headers.get("sec-websocket-protocol") !== MINIAPP_UPDATES_PROTOCOL) {
      return jsonError(401, "Unauthorized");
    }
    const now = Date.now();
    if (ticketExpiresAt <= now || authExpiresAt <= now || ticketExpiresAt > authExpiresAt) {
      return jsonError(401, "Mini App authorization has expired");
    }

    let currentEpoch: string;
    try {
      currentEpoch = await getDatabaseEpoch(this.env.DB);
    } catch {
      return jsonError(503, "Service unavailable");
    }
    if (currentEpoch !== databaseEpoch) {
      return jsonError(409, "Cloud database epoch changed", { "X-Cloud-Database-Epoch": currentEpoch });
    }

    const activeViewerCount = await this.pruneExpiredOrStaleViewers(now, ownerUserId, currentEpoch);
    if (activeViewerCount >= MINIAPP_UPDATES_MAX_VIEWERS) {
      return jsonError(429, "Too many Mini App update connections");
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    if (!client || !server) return jsonError(500, "WebSocket setup failed");
    this.ctx.acceptWebSocket(server, [MINIAPP_VIEWER_TAG]);
    server.serializeAttachment({
      role: "viewer",
      viewer_id: crypto.randomUUID(),
      owner_user_id: ownerUserId,
      database_epoch: databaseEpoch,
      auth_expires_at: authExpiresAt,
      visible: false,
      surface: "file",
      watch: false,
      pending_refreshes: [],
      seen_refresh_ids: [],
      event_sequence: 0,
    } satisfies ViewerAttachment);

    try {
      await this.scheduleViewerExpiry();
      server.send(JSON.stringify({ type: "ready", database_epoch: databaseEpoch, snapshot_control: SNAPSHOT_CONTROL_VERSION }));
    } catch {
      try {
        server.close(1011, "WebSocket setup failed");
      } catch {
        // The runtime removes closed sockets from the WebSocket set.
      }
      return jsonError(503, "WebSocket setup failed");
    }

    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: { "Sec-WebSocket-Protocol": MINIAPP_UPDATES_PROTOCOL },
    });
  }

  notify(envelope: InboxEnvelope): { delivered: number; failed: number } {
    const message = JSON.stringify(envelope);
    let delivered = 0;
    let failed = 0;
    for (const socket of this.ctx.getWebSockets(LOCAL_CLIENT_TAG)) {
      try {
        socket.send(message);
        delivered += 1;
      } catch {
        failed += 1;
        try {
          socket.close(1011, "Notification delivery failed");
        } catch {
          // A closed socket is removed by the runtime.
        }
      }
    }
    return { delivered, failed };
  }

  notifyViewers(event: MiniAppUpdateEvent): { delivered: number; failed: number } {
    const message = JSON.stringify(event);
    const now = Date.now();
    let delivered = 0;
    let failed = 0;
    for (const socket of this.ctx.getWebSockets(MINIAPP_VIEWER_TAG)) {
      const attachment = this.readViewerAttachment(socket);
      if (attachment === null || attachment.owner_user_id !== this.env.OWNER_USER_ID ||
        attachment.auth_expires_at <= now ||
        (event.reason !== "epoch" && (attachment.database_epoch !== event.database_epoch || !attachment.visible ||
          (!attachment.watch && !attachment.pending_refreshes.some((pending) => pending.expires_at > now))))) {
        if (attachment === null || attachment.auth_expires_at <= now) this.closeExpiredViewer(socket);
        continue;
      }
      try {
        socket.send(message);
        delivered += 1;
      } catch {
        failed += 1;
        try {
          socket.close(1011, "Notification delivery failed");
        } catch {
          // A closed socket is removed by the runtime.
        }
      }
    }
    return { delivered, failed };
  }

  async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const previous = this.messageQueues.get(socket) ?? Promise.resolve();
    const next = previous.then(async () => {
      try {
        await this.processWebSocketMessage(socket, message);
      } catch {
        try {
          socket.close(1011, "Control message processing failed");
        } catch {
          // A closed socket is removed by the runtime.
        }
      }
    });
    this.messageQueues.set(socket, next);
    try {
      await next;
    } finally {
      if (this.messageQueues.get(socket) === next) this.messageQueues.delete(socket);
    }
  }

  async webSocketClose(socket: WebSocket, code: number, reason: string): Promise<void> {
    const initial = this.readViewerAttachment(socket);
    try {
      socket.close(code, reason);
    } catch {
      // The close handshake may already be complete.
    }
    if (initial !== null) {
      let hadInterest = false;
      let databaseEpoch = initial.database_epoch;
      await this.withViewerMutation(initial.viewer_id, () => {
        const latest = this.readViewerAttachment(socket);
        if (latest === null) return;
        hadInterest = latest.visible;
        databaseEpoch = latest.database_epoch;
        latest.visible = false;
        latest.watch = false;
        latest.pending_refreshes = [];
        latest.event_sequence += 1;
        try {
          socket.serializeAttachment(latest);
        } catch {
          // The connection is already closing; no later message can use this attachment.
        }
      });
      if (hadInterest) await this.notifyViewerInterest(databaseEpoch);
      await this.scheduleViewerExpiry();
    }
  }

  async alarm(): Promise<void> {
    const now = Date.now();
    let currentEpoch: string | null = null;
    try {
      currentEpoch = await getDatabaseEpoch(this.env.DB);
    } catch {
      // Fail closed for refresh/presence; auth expiry cleanup still proceeds.
    }
    let interestChanged = false;
    for (const socket of this.ctx.getWebSockets(MINIAPP_VIEWER_TAG)) {
      const initial = this.readViewerAttachment(socket);
      if (initial === null) continue;
      await this.withViewerMutation(initial.viewer_id, () => {
        const attachment = this.readViewerAttachment(socket);
        if (attachment === null) return;
        const staleEpoch = currentEpoch !== null && attachment.database_epoch !== currentEpoch;
        if (attachment.owner_user_id !== this.env.OWNER_USER_ID || attachment.auth_expires_at <= now || staleEpoch) {
          if (attachment.visible) interestChanged = true;
          attachment.visible = false;
          attachment.watch = false;
          attachment.pending_refreshes = [];
          attachment.event_sequence += 1;
          socket.serializeAttachment(attachment);
          if (staleEpoch && currentEpoch !== null) this.sendEpochInvalidation(socket, currentEpoch);
          this.closeExpiredViewer(socket);
          return;
        }

        const expired = attachment.pending_refreshes.filter((pending) => pending.expires_at <= now);
        const activePending = attachment.pending_refreshes.filter((pending) => pending.expires_at > now);
        const activeSeen = attachment.seen_refresh_ids.filter((seen) => seen.expires_at > now);
        const changed = expired.length > 0 || activePending.length !== attachment.pending_refreshes.length ||
          activeSeen.length !== attachment.seen_refresh_ids.length;
        attachment.pending_refreshes = activePending;
        attachment.seen_refresh_ids = activeSeen;
        if (changed) {
          attachment.event_sequence += 1;
          socket.serializeAttachment(attachment);
        }
        if (attachment.visible && socket.readyState === WebSocket.OPEN) {
          for (const pending of expired) {
            try {
              socket.send(JSON.stringify(makeRefreshResult(attachment, pending.request_id, "failed", "refresh_timeout")));
            } catch {
              try {
                socket.close(1011, "Refresh result delivery failed");
              } catch {
                // A closed socket is removed by the runtime.
              }
              break;
            }
          }
        }
      });
    }
    if (interestChanged && currentEpoch !== null) await this.notifyViewerInterest(currentEpoch);
    await this.scheduleViewerExpiry();
  }

  private async processWebSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (socket.readyState !== WebSocket.OPEN) return;
    const value = parseControlMessage(message);
    if (value === null) {
      this.closeForPolicy(socket, "Invalid control frame");
      return;
    }
    const tags = this.ctx.getTags(socket);
    if (tags.includes(MINIAPP_VIEWER_TAG)) {
      await this.processViewerMessage(socket, value);
      return;
    }
    if (tags.includes(LOCAL_CLIENT_TAG)) {
      await this.processLocalMessage(socket, value);
      return;
    }
    this.closeForPolicy(socket, "Unknown WebSocket role");
  }

  private async processViewerMessage(socket: WebSocket, value: unknown): Promise<void> {
    const initial = this.readViewerAttachment(socket);
    if (initial === null || initial.owner_user_id !== this.env.OWNER_USER_ID || initial.auth_expires_at <= Date.now()) {
      this.closeExpiredViewer(socket);
      return;
    }
    if (socket.readyState !== WebSocket.OPEN) return;
    const interest = parseViewerInterest(value);
    const refresh = parseRefreshRequest(value);
    if (interest === null && refresh === null) {
      this.closeForPolicy(socket, "Invalid viewer control frame");
      return;
    }

    await this.withViewerMutation(initial.viewer_id, async () => {
      const before = this.readViewerAttachment(socket);
      if (before === null || before.owner_user_id !== this.env.OWNER_USER_ID || before.auth_expires_at <= Date.now()) {
        this.closeExpiredViewer(socket);
        return;
      }
      if (socket.readyState !== WebSocket.OPEN) return;

      let currentEpoch: string;
      try {
        currentEpoch = await getDatabaseEpoch(this.env.DB);
      } catch {
        const latest = this.readViewerAttachment(socket);
        if (refresh !== null && latest !== null && latest.owner_user_id === this.env.OWNER_USER_ID &&
          latest.auth_expires_at > Date.now() && socket.readyState === WebSocket.OPEN) {
          this.sendViewerResult(socket, latest, refresh.request_id, "unavailable", "cloud_unavailable");
        }
        return;
      }

      const latest = this.readViewerAttachment(socket);
      if (latest === null || latest.owner_user_id !== this.env.OWNER_USER_ID ||
        latest.auth_expires_at <= Date.now() || socket.readyState !== WebSocket.OPEN) {
        if (latest === null || latest.auth_expires_at <= Date.now()) this.closeExpiredViewer(socket);
        return;
      }
      if (currentEpoch !== latest.database_epoch) {
        this.sendEpochInvalidation(socket, currentEpoch);
        try {
          socket.close(VIEWER_EPOCH_CLOSE_CODE, "Cloud database epoch changed");
        } catch {
          // A closed socket is removed by the runtime.
        }
        return;
      }

      if (interest !== null) {
        if (interest.database_epoch !== latest.database_epoch) {
          this.sendEpochInvalidation(socket, currentEpoch);
          return;
        }
        const visible = interest.visible;
        const watch = visible && interest.watch;
        const changed = latest.visible !== visible || latest.surface !== interest.surface || latest.watch !== watch ||
          (!visible && latest.pending_refreshes.length > 0);
        latest.visible = visible;
        latest.surface = interest.surface;
        latest.watch = watch;
        if (!visible) latest.pending_refreshes = [];
        if (changed) {
          latest.event_sequence += 1;
          socket.serializeAttachment(latest);
          await this.notifyViewerInterest(latest.database_epoch);
        }
        await this.scheduleViewerExpiry();
        return;
      }

      if (refresh === null) return;
      if (refresh.database_epoch !== latest.database_epoch) {
        this.sendEpochInvalidation(socket, currentEpoch);
        return;
      }
      if (!latest.visible) {
        this.sendViewerResult(socket, latest, refresh.request_id, "unavailable", "viewer_not_visible");
        return;
      }

      const now = Date.now();
      const pendingBefore = latest.pending_refreshes.length;
      const seenBefore = latest.seen_refresh_ids.length;
      latest.pending_refreshes = latest.pending_refreshes.filter((pending) => pending.expires_at > now);
      latest.seen_refresh_ids = latest.seen_refresh_ids.filter((seen) => seen.expires_at > now);
      const pruned = pendingBefore !== latest.pending_refreshes.length || seenBefore !== latest.seen_refresh_ids.length;
      if (latest.pending_refreshes.some((pending) => pending.request_id === refresh.request_id) ||
        latest.seen_refresh_ids.some((seen) => seen.request_id === refresh.request_id)) {
        if (pruned) {
          latest.event_sequence += 1;
          socket.serializeAttachment(latest);
          await this.scheduleViewerExpiry();
        }
        return;
      }
      if (latest.pending_refreshes.length >= MAX_PENDING_REFRESHES_PER_VIEWER) {
        this.sendViewerResult(socket, latest, refresh.request_id, "unavailable", "too_many_pending_refreshes");
        return;
      }

      const expiresAt = Math.min(latest.auth_expires_at, now + REFRESH_TTL_MS);
      latest.seen_refresh_ids.push({ request_id: refresh.request_id, expires_at: expiresAt });
      if (latest.seen_refresh_ids.length > MAX_RECENT_REFRESH_IDS_PER_VIEWER) {
        latest.seen_refresh_ids = latest.seen_refresh_ids.slice(-MAX_RECENT_REFRESH_IDS_PER_VIEWER);
      }
      const localSocket = this.findControlLocalClient(latest.database_epoch);
      if (localSocket === null) {
        latest.event_sequence += 1;
        socket.serializeAttachment(latest);
        this.sendViewerResult(socket, latest, refresh.request_id, "unavailable", "local_unavailable");
        await this.scheduleViewerExpiry();
        return;
      }

      const controlFrame = {
        type: "snapshot_refresh",
        viewer_id: latest.viewer_id,
        request_id: refresh.request_id,
        database_epoch: latest.database_epoch,
        reason: refresh.reason,
        scope: refresh.scope,
        ...(refresh.library_id === undefined ? {} : { library_id: refresh.library_id }),
        expires_at: expiresAt,
      };
      latest.pending_refreshes.push({ request_id: refresh.request_id, expires_at: expiresAt });
      latest.event_sequence += 1;
      socket.serializeAttachment(latest);
      try {
        localSocket.send(JSON.stringify(controlFrame));
      } catch {
        try {
          localSocket.close(1011, "Refresh request delivery failed");
        } catch {
          // A closed socket is removed by the runtime.
        }
        latest.pending_refreshes = latest.pending_refreshes.filter((pending) => pending.request_id !== refresh.request_id);
        latest.event_sequence += 1;
        socket.serializeAttachment(latest);
        this.sendViewerResult(socket, latest, refresh.request_id, "unavailable", "local_unavailable");
      }
      await this.scheduleViewerExpiry();
    });
  }

  private async processLocalMessage(socket: WebSocket, value: unknown): Promise<void> {
    let attachment = this.readLocalAttachment(socket);
    if (attachment === null || attachment.snapshot_control !== SNAPSHOT_CONTROL_VERSION) {
      this.closeForPolicy(socket, "Local control capability is not enabled");
      return;
    }
    if (socket.readyState !== WebSocket.OPEN) return;
    const result = parseRefreshResult(value);
    if (result === null) {
      this.closeForPolicy(socket, "Invalid local control frame");
      return;
    }
    attachment.event_sequence += 1;
    const eventSequence = attachment.event_sequence;
    socket.serializeAttachment(attachment);

    let currentEpoch: string;
    try {
      currentEpoch = await getDatabaseEpoch(this.env.DB);
    } catch {
      return;
    }
    const latest = this.readLocalAttachment(socket);
    if (latest === null || latest.event_sequence !== eventSequence || socket.readyState !== WebSocket.OPEN) return;
    if (currentEpoch !== latest.database_epoch || result.database_epoch !== latest.database_epoch) {
      try {
        socket.close(VIEWER_EPOCH_CLOSE_CODE, "Cloud database epoch changed");
      } catch {
        // A closed socket is removed by the runtime.
      }
      return;
    }

    const now = Date.now();
    for (const viewer of this.ctx.getWebSockets(MINIAPP_VIEWER_TAG)) {
      const initialViewer = this.readViewerAttachment(viewer);
      if (initialViewer === null || initialViewer.viewer_id !== result.viewer_id) {
        continue;
      }
      let delivered = false;
      await this.withViewerMutation(result.viewer_id, async () => {
        const before = this.readViewerAttachment(viewer);
        if (before === null || before.viewer_id !== result.viewer_id || before.owner_user_id !== this.env.OWNER_USER_ID ||
          !before.visible || before.auth_expires_at <= Date.now() || before.database_epoch !== result.database_epoch ||
          viewer.readyState !== WebSocket.OPEN) {
          return;
        }

        let activeEpoch: string;
        try {
          activeEpoch = await getDatabaseEpoch(this.env.DB);
        } catch {
          return;
        }
        const latest = this.readViewerAttachment(viewer);
        if (latest === null || latest.viewer_id !== result.viewer_id || latest.owner_user_id !== this.env.OWNER_USER_ID ||
          !latest.visible || latest.auth_expires_at <= Date.now() || latest.database_epoch !== result.database_epoch ||
          activeEpoch !== latest.database_epoch || viewer.readyState !== WebSocket.OPEN) {
          return;
        }
        const pending = latest.pending_refreshes.find((entry) => entry.request_id === result.request_id);
        if (pending === undefined || pending.expires_at <= Date.now()) return;

        const forwarded: RefreshResultMessage = {
          type: "refresh_result",
          viewer_id: result.viewer_id,
          request_id: result.request_id,
          database_epoch: result.database_epoch,
          status: result.status,
          ...(result.state_version === undefined ? {} : { state_version: result.state_version }),
          ...(result.reported_at === undefined ? {} : { reported_at: result.reported_at }),
          ...(result.error_code === undefined ? {} : { error_code: result.error_code }),
        };
        latest.pending_refreshes = latest.pending_refreshes.filter((entry) => entry.request_id !== result.request_id);
        latest.event_sequence += 1;
        viewer.serializeAttachment(latest);
        try {
          viewer.send(JSON.stringify(forwarded));
          delivered = true;
        } catch {
          try {
            viewer.close(1011, "Refresh result delivery failed");
          } catch {
            // A closed socket is removed by the runtime.
          }
        }
      });
      if (delivered) await this.scheduleViewerExpiry();
      return;
    }
  }

  private async pruneExpiredOrStaleViewers(now: number, ownerUserId: string, databaseEpoch: string): Promise<number> {
    let active = 0;
    let interestChanged = false;
    for (const socket of this.ctx.getWebSockets(MINIAPP_VIEWER_TAG)) {
      const initial = this.readViewerAttachment(socket);
      if (initial === null) {
        this.closeExpiredViewer(socket);
        continue;
      }
      await this.withViewerMutation(initial.viewer_id, () => {
        const attachment = this.readViewerAttachment(socket);
        if (attachment === null || attachment.owner_user_id !== ownerUserId ||
          attachment.auth_expires_at <= now || attachment.database_epoch !== databaseEpoch) {
          if (attachment?.visible) interestChanged = true;
          if (attachment !== null) {
            attachment.visible = false;
            attachment.watch = false;
            attachment.pending_refreshes = [];
            attachment.event_sequence += 1;
            socket.serializeAttachment(attachment);
          }
          this.closeExpiredViewer(socket);
        } else if (socket.readyState !== WebSocket.CLOSED) {
          active += 1;
        }
      });
    }
    if (interestChanged) await this.notifyViewerInterest(databaseEpoch);
    return active;
  }

  private async withViewerMutation(viewerId: string, operation: () => Promise<void> | void): Promise<void> {
    const previous = this.viewerMutationQueues.get(viewerId) ?? Promise.resolve();
    const next = previous.catch(() => undefined).then(operation);
    this.viewerMutationQueues.set(viewerId, next);
    try {
      await next;
    } finally {
      if (this.viewerMutationQueues.get(viewerId) === next) this.viewerMutationQueues.delete(viewerId);
    }
  }

  private viewerInterestMessage(databaseEpoch: string, now: number): {
    type: "viewer_interest";
    database_epoch: string;
    viewers: ViewerInterest[];
  } {
    const viewers: ViewerInterest[] = [];
    for (const socket of this.ctx.getWebSockets(MINIAPP_VIEWER_TAG)) {
      const attachment = this.readViewerAttachment(socket);
      if (attachment === null || attachment.owner_user_id !== this.env.OWNER_USER_ID ||
        attachment.database_epoch !== databaseEpoch || attachment.auth_expires_at <= now ||
        !attachment.visible || socket.readyState === WebSocket.CLOSED) {
        continue;
      }
      viewers.push({
        viewer_id: attachment.viewer_id,
        surface: attachment.surface,
        watch: attachment.watch,
        expires_at: attachment.auth_expires_at,
      });
    }
    viewers.sort((left, right) => left.viewer_id.localeCompare(right.viewer_id));
    return { type: "viewer_interest", database_epoch: databaseEpoch, viewers };
  }

  private async notifyViewerInterest(databaseEpoch: string): Promise<void> {
    const message = JSON.stringify(this.viewerInterestMessage(databaseEpoch, Date.now()));
    for (const socket of this.ctx.getWebSockets(LOCAL_CLIENT_TAG)) {
      const attachment = this.readLocalAttachment(socket);
      if (attachment === null || attachment.snapshot_control !== SNAPSHOT_CONTROL_VERSION ||
        attachment.database_epoch !== databaseEpoch || socket.readyState !== WebSocket.OPEN) {
        continue;
      }
      try {
        socket.send(message);
      } catch {
        try {
          socket.close(1011, "Viewer interest delivery failed");
        } catch {
          // A closed socket is removed by the runtime.
        }
      }
    }
  }

  private findControlLocalClient(databaseEpoch: string): WebSocket | null {
    for (const socket of this.ctx.getWebSockets(LOCAL_CLIENT_TAG)) {
      const attachment = this.readLocalAttachment(socket);
      if (attachment !== null && attachment.snapshot_control === SNAPSHOT_CONTROL_VERSION &&
        attachment.database_epoch === databaseEpoch && socket.readyState === WebSocket.OPEN) {
        return socket;
      }
    }
    return null;
  }

  private sendViewerResult(
    socket: WebSocket,
    attachment: ViewerAttachment,
    requestId: string,
    status: RefreshResultMessage["status"],
    errorCode?: string,
  ): void {
    try {
      socket.send(JSON.stringify(makeRefreshResult(attachment, requestId, status, errorCode)));
    } catch {
      try {
        socket.close(1011, "Refresh result delivery failed");
      } catch {
        // A closed socket is removed by the runtime.
      }
    }
  }

  private sendEpochInvalidation(socket: WebSocket, databaseEpoch: string): void {
    try {
      socket.send(JSON.stringify({ type: "invalidate", database_epoch: databaseEpoch, reason: "epoch" }));
    } catch {
      try {
        socket.close(1011, "Epoch notification delivery failed");
      } catch {
        // A closed socket is removed by the runtime.
      }
    }
  }

  private closeForPolicy(socket: WebSocket, reason: string): void {
    try {
      socket.close(1008, reason);
    } catch {
      // A closed socket is removed by the runtime.
    }
  }

  private closeExpiredViewer(socket: WebSocket): void {
    try {
      socket.close(VIEWER_AUTH_CLOSE_CODE, "Mini App authorization expired");
    } catch {
      // A closed socket is removed by the runtime.
    }
  }

  private readViewerAttachment(socket: WebSocket): ViewerAttachment | null {
    try {
      const value: unknown = socket.deserializeAttachment();
      return isViewerAttachment(value) ? value : null;
    } catch {
      return null;
    }
  }

  private readLocalAttachment(socket: WebSocket): LocalAttachment | null {
    try {
      const value: unknown = socket.deserializeAttachment();
      return isLocalAttachment(value) ? value : null;
    } catch {
      return null;
    }
  }

  private async scheduleViewerExpiry(): Promise<void> {
    const now = Date.now();
    let earliestExpiry: number | null = null;
    for (const socket of this.ctx.getWebSockets(MINIAPP_VIEWER_TAG)) {
      const attachment = this.readViewerAttachment(socket);
      if (attachment === null || attachment.owner_user_id !== this.env.OWNER_USER_ID ||
        attachment.auth_expires_at <= now || socket.readyState === WebSocket.CLOSED) {
        continue;
      }
      earliestExpiry = earliestExpiry === null
        ? attachment.auth_expires_at
        : Math.min(earliestExpiry, attachment.auth_expires_at);
      for (const pending of attachment.pending_refreshes) {
        if (pending.expires_at > now) earliestExpiry = Math.min(earliestExpiry, pending.expires_at);
      }
    }
    if (earliestExpiry === null) {
      await this.ctx.storage.deleteAlarm();
    } else {
      await this.ctx.storage.setAlarm(earliestExpiry);
    }
  }
}
