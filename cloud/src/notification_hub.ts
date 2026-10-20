import { DurableObject } from "cloudflare:workers";
import type { InboxEnvelope } from "./types";

export class NotificationHub extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    if (request.method !== "GET" || request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket upgrade required", { status: 426 });
    }
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    if (!client || !server) return new Response("WebSocket setup failed", { status: 500 });
    this.ctx.acceptWebSocket(server, ["local-client"]);
    return new Response(null, { status: 101, webSocket: client });
  }

  notify(envelope: InboxEnvelope): { delivered: number; failed: number } {
    const message = JSON.stringify(envelope);
    let delivered = 0;
    let failed = 0;
    for (const socket of this.ctx.getWebSockets("local-client")) {
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

  webSocketMessage(socket: WebSocket): void {
    try {
      socket.close(1008, "Client messages are not supported");
    } catch {
      // A closed socket is removed by the runtime.
    }
  }
}
