import { DurableObject } from "cloudflare:workers";
import type { InboxEnvelope } from "./types";
import {
  MINIAPP_UPDATES_MAX_VIEWERS,
  MINIAPP_UPDATES_PROTOCOL,
  NOTIFIER_ROLE_HEADER,
  NOTIFIER_VIEWER_AUTH_EXPIRY_HEADER,
  NOTIFIER_VIEWER_EPOCH_HEADER,
  NOTIFIER_VIEWER_OWNER_HEADER,
  NOTIFIER_VIEWER_TICKET_EXPIRY_HEADER,
} from "./updates";
import { getDatabaseEpoch, isDatabaseEpoch } from "./store";

const LOCAL_CLIENT_TAG = "local-client";
const MINIAPP_VIEWER_TAG = "miniapp-viewer";
const VIEWER_AUTH_CLOSE_CODE = 4401;

interface ViewerAttachment {
  role: "viewer";
  owner_user_id: string;
  database_epoch: string;
  auth_expires_at: number;
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

function isViewerAttachment(value: unknown): value is ViewerAttachment {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  return record.role === "viewer" &&
    typeof record.owner_user_id === "string" && /^[1-9]\d{0,15}$/u.test(record.owner_user_id) &&
    typeof record.database_epoch === "string" && isDatabaseEpoch(record.database_epoch) &&
    typeof record.auth_expires_at === "number" && Number.isSafeInteger(record.auth_expires_at) && record.auth_expires_at > 0;
}

export class NotificationHub extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    if (request.method !== "GET" || request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return jsonError(426, "WebSocket upgrade required");
    }
    const role = request.headers.get(NOTIFIER_ROLE_HEADER);
    if (role === "viewer") return this.acceptViewer(request);
    if (role === "local-client") return this.acceptLocalClient();
    return jsonError(401, "Unauthorized");
  }

  private acceptLocalClient(): Response {
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    if (!client || !server) return jsonError(500, "WebSocket setup failed");
    this.ctx.acceptWebSocket(server, [LOCAL_CLIENT_TAG]);
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

    const activeViewerCount = this.pruneExpiredOrStaleViewers(now, ownerUserId);
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
      owner_user_id: ownerUserId,
      database_epoch: databaseEpoch,
      auth_expires_at: authExpiresAt,
    } satisfies ViewerAttachment);

    try {
      await this.scheduleViewerExpiry();
      server.send(JSON.stringify({ type: "ready", database_epoch: databaseEpoch }));
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
      const attachment = socket.deserializeAttachment();
      if (!isViewerAttachment(attachment) || attachment.owner_user_id !== this.env.OWNER_USER_ID ||
        attachment.auth_expires_at <= now) {
        this.closeExpiredViewer(socket);
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

  async webSocketMessage(socket: WebSocket): Promise<void> {
    try {
      socket.close(1008, "Client messages are not supported");
    } catch {
      // A closed socket is removed by the runtime.
    }
  }

  async webSocketClose(socket: WebSocket, code: number, reason: string): Promise<void> {
    try {
      socket.close(code, reason);
    } catch {
      // The close handshake may already be complete.
    }
    if (this.ctx.getTags(socket).includes(MINIAPP_VIEWER_TAG)) {
      await this.scheduleViewerExpiry();
    }
  }

  async alarm(): Promise<void> {
    this.pruneExpiredOrStaleViewers(Date.now(), this.env.OWNER_USER_ID);
    await this.scheduleViewerExpiry();
  }

  private pruneExpiredOrStaleViewers(now: number, ownerUserId: string): number {
    let active = 0;
    for (const socket of this.ctx.getWebSockets(MINIAPP_VIEWER_TAG)) {
      const attachment = socket.deserializeAttachment();
      if (!isViewerAttachment(attachment) || attachment.owner_user_id !== ownerUserId ||
        attachment.auth_expires_at <= now) {
        this.closeExpiredViewer(socket);
      } else if (socket.readyState !== WebSocket.CLOSED) {
        active += 1;
      }
    }
    return active;
  }

  private closeExpiredViewer(socket: WebSocket): void {
    try {
      socket.close(VIEWER_AUTH_CLOSE_CODE, "Mini App authorization expired");
    } catch {
      // A closed socket is removed by the runtime.
    }
  }

  private async scheduleViewerExpiry(): Promise<void> {
    const now = Date.now();
    let earliestExpiry: number | null = null;
    for (const socket of this.ctx.getWebSockets(MINIAPP_VIEWER_TAG)) {
      const attachment = socket.deserializeAttachment();
      if (!isViewerAttachment(attachment) || attachment.owner_user_id !== this.env.OWNER_USER_ID ||
        attachment.auth_expires_at <= now || socket.readyState === WebSocket.CLOSED) {
        continue;
      }
      earliestExpiry = earliestExpiry === null
        ? attachment.auth_expires_at
        : Math.min(earliestExpiry, attachment.auth_expires_at);
    }
    if (earliestExpiry === null) {
      await this.ctx.storage.deleteAlarm();
    } else {
      await this.ctx.storage.setAlarm(earliestExpiry);
    }
  }
}
