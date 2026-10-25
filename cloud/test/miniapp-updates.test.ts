import { SELF } from "cloudflare:test";
import { env } from "cloudflare:workers";
import type { D1Migration } from "@cloudflare/vitest-plugin";
import { afterEach, describe, expect, it, vi } from "vitest";
import { syntheticCredentials } from "./fixtures/synthetic-credentials";
import snapshotControlFixtureJson from "./fixtures/snapshot-control.json";
import { getDatabaseEpoch } from "../src/store";
import { sha256Hex } from "../src/validation";
import {
  issueMiniAppUpdatesTicket,
  MINIAPP_UPDATES_PROTOCOL,
  MINIAPP_UPDATES_SESSION_PATH,
  MINIAPP_UPDATES_SOCKET_PATH,
  SNAPSHOT_CONTROL_VERSION,
  SNAPSHOT_NOTIFY_VIEWERS_HEADER,
} from "../src/updates";

const testEnv = env as typeof env & { TEST_MIGRATIONS: D1Migration[] };
const workerUrl = "https://worker.test";
const ownerId = 123456789;
const botToken = syntheticCredentials.botToken.value;
const localToken = syntheticCredentials.localSharedSecret.value;
const testDatabaseEpoch = "0123456789abcdef0123456789abcdef";
const encoder = new TextEncoder();
const testSockets = new Set<WebSocket>();

interface SessionResponse {
  ticket: string;
  websocket_path: string;
  protocol: string;
  expires_at: number;
  database_epoch: string;
}

interface UpdateEvent {
  type: string;
  viewer_id?: string;
  request_id?: string;
  database_epoch?: string;
  state_version?: number;
  reason?: string;
  status?: string;
  error_code?: string;
  expires_at?: number;
  viewers?: Array<{ viewer_id: string; surface: string; watch: boolean; expires_at: number }>;
}

interface SnapshotControlFixture {
  database_epoch: string;
  viewer_id: string;
  request_id: string;
  clock_ms: number;
  viewer_interest: { type: string; database_epoch: string; viewers: Array<Record<string, unknown>> };
  snapshot_refresh: Record<string, unknown>;
  refresh_result: Record<string, unknown>;
}

const snapshotControlFixture = snapshotControlFixtureJson as unknown as SnapshotControlFixture;

function parseMessage(data: unknown): UpdateEvent | null {
  if (typeof data !== "string") return null;
  try {
    const parsed = JSON.parse(data) as unknown;
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
      ? parsed as UpdateEvent
      : null;
  } catch {
    return null;
  }
}

function collectMessages(socket: WebSocket): {
  messages: UpdateEvent[];
  waitFor(predicate: (event: UpdateEvent) => boolean, afterIndex?: number, timeoutMs?: number): Promise<UpdateEvent>;
} {
  const messages: UpdateEvent[] = [];
  const waiters: Array<{
    predicate: (event: UpdateEvent) => boolean;
    afterIndex: number;
    resolve: (event: UpdateEvent) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof setTimeout>;
  }> = [];
  socket.addEventListener("message", (message) => {
    const parsed = parseMessage(message.data);
    if (parsed === null) return;
    messages.push(parsed);
    const messageIndex = messages.length - 1;
    for (let index = waiters.length - 1; index >= 0; index -= 1) {
      const waiter = waiters[index]!;
      if (messageIndex < waiter.afterIndex || !waiter.predicate(parsed)) continue;
      clearTimeout(waiter.timer);
      waiters.splice(index, 1);
      waiter.resolve(parsed);
    }
  });
  socket.accept();
  return {
    messages,
    waitFor(predicate, afterIndex = 0, timeoutMs = 3_000) {
      const alreadySeen = messages.slice(afterIndex).find(predicate);
      if (alreadySeen !== undefined) return Promise.resolve(alreadySeen);
      return new Promise<UpdateEvent>((resolve, reject) => {
        const waiter = {
          predicate,
          afterIndex,
          resolve,
          reject,
          timer: setTimeout(() => {
            const index = waiters.indexOf(waiter);
            if (index >= 0) waiters.splice(index, 1);
            reject(new Error("Timed out waiting for a Mini App update"));
          }, timeoutMs),
        };
        waiters.push(waiter);
      });
    },
  };
}

function localHeaders(): Record<string, string> {
  return {
    authorization: `Bearer ${localToken}`,
    "X-Cloud-Database-Epoch": testDatabaseEpoch,
  };
}

function trackSocketResponse(response: Response): Response {
  if (response.webSocket !== null) testSockets.add(response.webSocket);
  return response;
}

async function signInitData(options: { userId?: number; authDate?: number } = {}): Promise<string> {
  const params = new URLSearchParams();
  params.set("auth_date", String(options.authDate ?? Math.floor(Date.now() / 1000)));
  params.set("chat_type", "private");
  params.set("user", JSON.stringify({ id: options.userId ?? ownerId, is_bot: false, first_name: "Owner" }));
  const entries = [...params.entries()].sort(([left], [right]) => left.localeCompare(right));
  const checkString = entries.map(([key, value]) => `${key}=${value}`).join("\n");
  const derivationKey = await crypto.subtle.importKey(
    "raw",
    encoder.encode("WebAppData"),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const secret = await crypto.subtle.sign("HMAC", derivationKey, encoder.encode(botToken));
  const verificationKey = await crypto.subtle.importKey(
    "raw",
    secret,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const digest = new Uint8Array(await crypto.subtle.sign("HMAC", verificationKey, encoder.encode(checkString)));
  params.set("hash", [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join(""));
  return params.toString();
}

async function createSession(initData?: string): Promise<Response> {
  return SELF.fetch(new Request(`${workerUrl}${MINIAPP_UPDATES_SESSION_PATH}`, {
    method: "POST",
    headers: { "x-telegram-init-data": initData ?? await signInitData() },
  }));
}

async function openViewer(
  ticket: string,
  path = MINIAPP_UPDATES_SOCKET_PATH,
  extraHeaders: Record<string, string> = {},
): Promise<Response> {
  return trackSocketResponse(await SELF.fetch(new Request(`${workerUrl}${path}`, {
    headers: {
      upgrade: "websocket",
      "sec-websocket-protocol": `${MINIAPP_UPDATES_PROTOCOL}, ${ticket}`,
      ...extraHeaders,
    },
  })));
}

async function openLocalClient(snapshotControl?: "v1"): Promise<Response> {
  const headers: Record<string, string> = { ...localHeaders(), upgrade: "websocket" };
  if (snapshotControl !== undefined) headers["X-Cloud-Snapshot-Control"] = snapshotControl;
  return trackSocketResponse(await SELF.fetch(new Request(`${workerUrl}/api/local/ws`, {
    headers,
  })));
}

async function readJson<T>(response: Response): Promise<T> {
  return await response.json() as T;
}

async function webhook(updateId: number): Promise<Response> {
  return SELF.fetch(new Request(`${workerUrl}/webhook`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-telegram-bot-api-secret-token": syntheticCredentials.webhookSecret.value,
    },
    body: JSON.stringify({
      update_id: updateId,
      message: {
        message_id: updateId,
        date: Math.floor(Date.now() / 1000),
        from: { id: ownerId, is_bot: false, first_name: "Owner" },
        chat: { id: ownerId, type: "private", first_name: "Owner" },
        text: "/scan",
      },
    }),
  }));
}

function localSnapshot(stateVersion: number): Record<string, unknown> {
  const now = Date.now();
  return {
    state_version: stateVersion,
    reported_at: now,
    library: {
      revision: `library-${stateVersion}`,
      scanned_at: Math.floor(now / 1000),
      items: [],
      categories: [],
      warnings: [],
    },
    previews: [],
    operations: [],
    tasks: [],
    settings: { download_dir: "/Users/test/Videos" },
  };
}

async function postChunk(sha256: string, content: string): Promise<Response> {
  return SELF.fetch(new Request(`${workerUrl}/api/local/state/chunks/${sha256}`, {
    method: "POST",
    headers: { ...localHeaders(), "content-type": "application/octet-stream" },
    body: content,
  }));
}

async function waitForClose(socket: WebSocket, timeoutMs: number): Promise<CloseEvent> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Timed out waiting for WebSocket close")), timeoutMs);
    socket.addEventListener("close", (event) => {
      clearTimeout(timer);
      resolve(event);
    }, { once: true });
  });
}

afterEach(async () => {
  const closing: Array<Promise<CloseEvent | undefined>> = [];
  for (const socket of testSockets) {
    if (socket.readyState === WebSocket.CLOSED) continue;
    closing.push(waitForClose(socket, 1_000).catch(() => undefined));
    try {
      socket.close(1000, "Test cleanup");
    } catch {
      // The WebSocket may already be closing.
    }
  }
  await Promise.all(closing);
  testSockets.clear();
  vi.restoreAllMocks();
});

describe("Mini App update capabilities", () => {
  it("issues an owner-bound session ticket and rejects non-owner or expired Telegram auth", async () => {
    const response = await createSession();
    expect(response.status).toBe(200);
    const session = await readJson<SessionResponse>(response);
    expect(session).toMatchObject({
      websocket_path: MINIAPP_UPDATES_SOCKET_PATH,
      protocol: MINIAPP_UPDATES_PROTOCOL,
      database_epoch: testDatabaseEpoch,
    });
    expect(session.ticket).toMatch(/^v1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/u);
    expect(session.expires_at).toBeGreaterThan(Date.now());
    expect(session.expires_at).toBeLessThanOrEqual(Date.now() + 6 * 60 * 60 * 1000 + 1_000);

    const nonOwner = await createSession(await signInitData({ userId: ownerId + 1 }));
    const expired = await createSession(await signInitData({ authDate: Math.floor(Date.now() / 1000) - 7 * 60 * 60 }));
    expect(nonOwner.status).toBe(403);
    expect(expired.status).toBe(401);
  });

  it("rejects tampered tickets, URL tickets, and tickets for a stale database epoch", async () => {
    const session = await readJson<SessionResponse>(await createSession());
    const [version, payload, signature] = session.ticket.split(".");
    const replacement = signature?.[0] === "A" ? "B" : "A";
    const tampered = `${version}.${payload}.${replacement}${signature?.slice(1) ?? ""}`;
    expect((await openViewer(tampered)).status).toBe(401);
    expect((await SELF.fetch(new Request(`${workerUrl}${MINIAPP_UPDATES_SOCKET_PATH}?ticket=${session.ticket}`, {
      headers: { upgrade: "websocket", "sec-websocket-protocol": MINIAPP_UPDATES_PROTOCOL },
    }))).status).toBe(400);
    expect((await SELF.fetch(new Request(`${workerUrl}${MINIAPP_UPDATES_SOCKET_PATH}`, {
      headers: { upgrade: "websocket", "sec-websocket-protocol": "local-client" },
    }))).status).toBe(401);

    const staleEpoch = "ffffffffffffffffffffffffffffffff";
    await testEnv.DB.prepare("UPDATE cloud_database_meta SET epoch = ? WHERE id = 1").bind(staleEpoch).run();
    const stale = await openViewer(session.ticket);
    expect(stale.status).toBe(409);
    expect(stale.headers.get("X-Cloud-Database-Epoch")).toBe(staleEpoch);
  });

  it("keeps viewer notifications separate from local inbox payloads and only notifies after snapshot commit, enqueue, and ack", async () => {
    const session = await readJson<SessionResponse>(await createSession());
    const connected = await openViewer(session.ticket);
    expect(connected.status).toBe(101);
    expect(connected.headers.get("Sec-WebSocket-Protocol")).toBe(MINIAPP_UPDATES_PROTOCOL);
    const socket = connected.webSocket;
    expect(socket).not.toBeNull();
    const collector = collectMessages(socket!);
    expect(await collector.waitFor((event) => event.type === "ready")).toMatchObject({ snapshot_control: "v1" });
    const localConnection = await openLocalClient(SNAPSHOT_CONTROL_VERSION);
    expect(localConnection.status).toBe(101);
    const localSocket = localConnection.webSocket!;
    const localCollector = collectMessages(localSocket);
    await localCollector.waitFor((event) => event.type === "viewer_interest" && event.viewers?.length === 0);
    socket!.send(JSON.stringify({
      type: "interest",
      database_epoch: testDatabaseEpoch,
      visible: true,
      surface: "file",
      watch: true,
    }));
    await localCollector.waitFor((event) => event.type === "viewer_interest" && event.viewers?.length === 1);

    const libraryText = JSON.stringify({ revision: "library-chunked", items: [] });
    const stateText = JSON.stringify({ previews: [], operations: [], tasks: [], settings: { download_dir: "/Videos" } });
    const libraryHash = await sha256Hex(libraryText);
    const stateHash = await sha256Hex(stateText);
    const libraryBytes = encoder.encode(libraryText).byteLength;
    const stateBytes = encoder.encode(stateText).byteLength;
    expect((await postChunk(libraryHash, libraryText)).status).toBe(200);
    expect((await postChunk(stateHash, stateText)).status).toBe(200);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(collector.messages.filter((event) => event.type === "invalidate")).toEqual([]);

    const chunkedCommit = collector.waitFor((event) => event.type === "invalidate" && event.reason === "state", collector.messages.length);
    const committed = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({
        format: "chunked-v1",
        state_version: 1,
        reported_at: Date.now(),
        library_revision: "library-chunked",
        library: { bytes: libraryBytes, chunks: [{ sha256: libraryHash, bytes: libraryBytes }] },
        state: { bytes: stateBytes, chunks: [{ sha256: stateHash, bytes: stateBytes }] },
      }),
    }));
    expect(committed.status).toBe(200);
    expect(await chunkedCommit).toMatchObject({
      type: "invalidate",
      reason: "state",
      state_version: 1,
      database_epoch: testDatabaseEpoch,
    });

    const legacyCommit = collector.waitFor((event) => event.type === "invalidate" && event.reason === "state" && event.state_version === 2, collector.messages.length);
    const legacy = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(localSnapshot(2)),
    }));
    expect(legacy.status).toBe(200);
    expect(await legacyCommit).toMatchObject({ reason: "state", state_version: 2 });

    const preparedState = collector.waitFor((event) => event.type === "invalidate" && event.reason === "state" && event.state_version === 3, collector.messages.length);
    const preparedAt = Date.now();
    const prepared = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({
        format: "prepared-v1",
        state_version: 3,
        reported_at: preparedAt,
        library_json: JSON.stringify({
          revision: "library-prepared",
          scanned_at: Math.floor(preparedAt / 1000),
          items: [],
          categories: [],
          warnings: [],
        }),
        state_json: JSON.stringify({
          revision: "snapshot-prepared",
          previews: [],
          operations: [],
          tasks: [],
          settings: { download_dir: "/Users/test/Videos" },
        }),
      }),
    }));
    expect(prepared.status).toBe(200);
    expect(await preparedState).toMatchObject({ reason: "state", state_version: 3 });

    const firstEnqueue = collector.waitFor((event) => event.type === "invalidate" && event.reason === "inbox", collector.messages.length);
    const localEnqueue = localCollector.waitFor((event) => "payload" in event && "request_id" in event, localCollector.messages.length);
    expect((await webhook(701)).status).toBe(200);
    expect(await firstEnqueue).toMatchObject({ type: "invalidate", reason: "inbox", database_epoch: testDatabaseEpoch });
    expect(await localEnqueue).toMatchObject({ kind: "telegram", request_id: "tg:701" });

    const duplicateEnqueue = collector.waitFor((event) => event.type === "invalidate" && event.reason === "inbox", collector.messages.length);
    const localDuplicate = localCollector.waitFor((event) => "payload" in event && "request_id" in event, localCollector.messages.length);
    expect((await webhook(701)).status).toBe(200);
    expect(await duplicateEnqueue).toMatchObject({ type: "invalidate", reason: "inbox" });
    expect(await localDuplicate).toMatchObject({ kind: "telegram", request_id: "tg:701" });
    expect(collector.messages.some((event) => "payload" in event || "request_id" in event)).toBe(false);
    expect(localCollector.messages.some((event) => event.type === "invalidate")).toBe(false);

    const request = await testEnv.DB.prepare(
      "SELECT seq, dedup_key, payload_hash, kind, created_at FROM inbox WHERE dedup_key = ?",
    ).bind("tg:701").first<{ seq: number; dedup_key: string; payload_hash: string; kind: string; created_at: number }>();
    expect(request).not.toBeNull();
    const acked = collector.waitFor((event) => event.type === "invalidate" && event.reason === "inbox", collector.messages.length);
    const ack = await SELF.fetch(new Request(`${workerUrl}/api/local/ack`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({ receipts: [{
        seq: request!.seq,
        request_id: request!.dedup_key,
        payload_hash: request!.payload_hash,
        kind: request!.kind,
        created_at: request!.created_at,
      }] }),
    }));
    expect(ack.status).toBe(200);
    expect(await acked).toMatchObject({ reason: "inbox", database_epoch: testDatabaseEpoch });

    const secondEnqueue = collector.waitFor((event) => event.type === "invalidate" && event.reason === "inbox", collector.messages.length);
    expect((await webhook(702)).status).toBe(200);
    await secondEnqueue;
    const rotationRow = await testEnv.DB.prepare(
      "SELECT seq, dedup_key, payload_hash, kind, created_at FROM inbox WHERE dedup_key = ?",
    ).bind("tg:702").first<{ seq: number; dedup_key: string; payload_hash: string; kind: string; created_at: number }>();
    expect(rotationRow).not.toBeNull();
    const rotated = collector.waitFor((event) => event.type === "invalidate" && event.reason === "epoch", collector.messages.length);
    const rotation = await SELF.fetch(new Request(`${workerUrl}/api/local/epoch/rotate-on-conflict`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({
        seq: rotationRow!.seq,
        request_id: rotationRow!.dedup_key,
        payload_hash: rotationRow!.payload_hash,
        kind: rotationRow!.kind,
        created_at: rotationRow!.created_at,
      }),
    }));
    expect(rotation.status).toBe(200);
    const rotationResult = await readJson<{ database_epoch: string }>(rotation);
    expect(await rotated).toMatchObject({ reason: "epoch", database_epoch: rotationResult.database_epoch });

    socket!.close(1000, "Test complete");
    localSocket.close(1000, "Test complete");
  });

  it("negotiates local control frames without exposing them to legacy clients", async () => {
    const unsupported = await SELF.fetch(new Request(`${workerUrl}/api/local/ws`, {
      headers: { ...localHeaders(), upgrade: "websocket", "X-Cloud-Snapshot-Control": "v2" },
    }));
    expect(unsupported.status).toBe(400);

    const legacyConnection = await openLocalClient();
    expect(legacyConnection.status).toBe(101);
    const legacySocket = legacyConnection.webSocket!;
    const legacyCollector = collectMessages(legacySocket);

    const session = await readJson<SessionResponse>(await createSession());
    const viewerConnection = await openViewer(session.ticket, MINIAPP_UPDATES_SOCKET_PATH, {
      "X-Cloud-Snapshot-Control": SNAPSHOT_CONTROL_VERSION,
    });
    expect(viewerConnection.status).toBe(101);
    const viewerSocket = viewerConnection.webSocket!;
    const viewerCollector = collectMessages(viewerSocket);
    expect(await viewerCollector.waitFor((event) => event.type === "ready")).toMatchObject({
      snapshot_control: SNAPSHOT_CONTROL_VERSION,
    });

    viewerSocket.send(JSON.stringify({
      type: "interest",
      database_epoch: testDatabaseEpoch,
      visible: true,
      surface: "settings",
      watch: false,
    }));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(legacyCollector.messages.filter((event) => event.type === "viewer_interest" || event.type === "snapshot_refresh"))
      .toEqual([]);

    const requestId = crypto.randomUUID();
    const unavailable = viewerCollector.waitFor((event) => event.type === "refresh_result" && event.request_id === requestId);
    viewerSocket.send(JSON.stringify({
      type: "refresh_request",
      database_epoch: testDatabaseEpoch,
      request_id: requestId,
      reason: "open",
      scope: "settings",
    }));
    expect(await unavailable).toMatchObject({
      type: "refresh_result",
      request_id: requestId,
      database_epoch: testDatabaseEpoch,
      status: "unavailable",
      error_code: "local_unavailable",
    });

    const controlConnection = await openLocalClient(SNAPSHOT_CONTROL_VERSION);
    expect(controlConnection.status).toBe(101);
    const controlSocket = controlConnection.webSocket!;
    const controlCollector = collectMessages(controlSocket);
    const replay = await controlCollector.waitFor((event) => event.type === "viewer_interest");
    expect(replay.viewers).toHaveLength(1);
    expect(replay.viewers?.[0]).toMatchObject({
      surface: "settings",
      watch: false,
      expires_at: session.expires_at,
    });
    expect(legacyCollector.messages.filter((event) => event.type === "viewer_interest" || event.type === "snapshot_refresh"))
      .toEqual([]);

    viewerSocket.close(1000, "Test complete");
    legacySocket.close(1000, "Test complete");
    controlSocket.close(1000, "Test complete");
  });

  it("matches the shared snapshot-control wire fixture and returns only a committed correlated result", async () => {
    const session = await readJson<SessionResponse>(await createSession());
    const viewerConnection = await openViewer(session.ticket);
    expect(viewerConnection.status).toBe(101);
    const viewerSocket = viewerConnection.webSocket!;
    const viewerCollector = collectMessages(viewerSocket);
    expect(await viewerCollector.waitFor((event) => event.type === "ready")).toMatchObject({
      snapshot_control: SNAPSHOT_CONTROL_VERSION,
      database_epoch: snapshotControlFixture.database_epoch,
    });

    const localConnection = await openLocalClient(SNAPSHOT_CONTROL_VERSION);
    expect(localConnection.status).toBe(101);
    const localSocket = localConnection.webSocket!;
    const localCollector = collectMessages(localSocket);
    expect(await localCollector.waitFor((event) => event.type === "viewer_interest"))
      .toEqual({ ...snapshotControlFixture.viewer_interest, viewers: [] });

    viewerSocket.send(JSON.stringify({
      type: "interest",
      database_epoch: snapshotControlFixture.database_epoch,
      visible: true,
      surface: "file",
      watch: true,
    }));
    const interest = await localCollector.waitFor((event) => event.type === "viewer_interest" && event.viewers?.length === 1);
    const viewerId = interest.viewers?.[0]?.viewer_id;
    expect(viewerId).toMatch(/^[0-9a-f-]{36}$/iu);
    if (viewerId === undefined) throw new Error("Viewer interest omitted its viewer ID");
    expect(interest).toEqual({
      ...snapshotControlFixture.viewer_interest,
      viewers: [{
        ...snapshotControlFixture.viewer_interest.viewers[0]!,
        viewer_id: viewerId,
        expires_at: session.expires_at,
      }],
    });

    const requestStartedAt = Date.now();
    viewerSocket.send(JSON.stringify({
      type: "refresh_request",
      database_epoch: snapshotControlFixture.database_epoch,
      request_id: snapshotControlFixture.request_id,
      reason: "manual",
      scope: "library",
    }));
    const refresh = await localCollector.waitFor((event) => event.type === "snapshot_refresh");
    expect(refresh).toMatchObject({
      ...snapshotControlFixture.snapshot_refresh,
      viewer_id: viewerId,
      expires_at: refresh.expires_at,
    });
    expect(refresh.expires_at).toBeGreaterThanOrEqual(requestStartedAt + 119_000);
    expect(refresh.expires_at).toBeLessThanOrEqual(Date.now() + 120_000);
    expect(refresh.expires_at).toBeLessThanOrEqual(session.expires_at);

    const reportedAt = Date.now();
    const invalidation = viewerCollector.waitFor((event) =>
      event.type === "invalidate" && event.reason === "state" && event.state_version === 42,
    );
    const published = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: {
        ...localHeaders(),
        "content-type": "application/json",
        [SNAPSHOT_NOTIFY_VIEWERS_HEADER]: "1",
      },
      body: JSON.stringify({ ...localSnapshot(42), reported_at: reportedAt }),
    }));
    expect(published.status).toBe(200);
    expect(await invalidation).toMatchObject({ reason: "state", state_version: 42 });

    localSocket.send(JSON.stringify({
      ...snapshotControlFixture.refresh_result,
      viewer_id: viewerId,
      request_id: "33333333-3333-4333-8333-333333333333",
      database_epoch: snapshotControlFixture.database_epoch,
      reported_at: reportedAt,
    }));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(viewerCollector.messages.some((event) => event.type === "refresh_result")).toBe(false);

    const expectedResult = {
      ...snapshotControlFixture.refresh_result,
      viewer_id: viewerId,
      reported_at: reportedAt,
    };
    const correlated = viewerCollector.waitFor((event) =>
      event.type === "refresh_result" && event.request_id === snapshotControlFixture.request_id,
    );
    localSocket.send(JSON.stringify(expectedResult));
    expect(await correlated).toEqual(expectedResult);

    viewerSocket.send(JSON.stringify({
      type: "refresh_request",
      database_epoch: snapshotControlFixture.database_epoch,
      request_id: snapshotControlFixture.request_id,
      reason: "manual",
      scope: "library",
    }));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(localCollector.messages.filter((event) => event.type === "snapshot_refresh")).toHaveLength(1);

    const hiddenRequestId = "44444444-4444-4444-8444-444444444444";
    viewerSocket.send(JSON.stringify({
      type: "refresh_request",
      database_epoch: snapshotControlFixture.database_epoch,
      request_id: hiddenRequestId,
      reason: "reference",
      scope: "settings",
    }));
    await localCollector.waitFor((event) => event.type === "snapshot_refresh" && event.request_id === hiddenRequestId);

    viewerSocket.send(JSON.stringify({
      type: "interest",
      database_epoch: snapshotControlFixture.database_epoch,
      visible: false,
      surface: "file",
      watch: false,
    }));
    const hidden = await localCollector.waitFor((event) => event.type === "viewer_interest" && event.viewers?.length === 0);
    expect(hidden).toEqual({ ...snapshotControlFixture.viewer_interest, viewers: [] });

    const visibleResultCount = viewerCollector.messages.filter((event) => event.type === "refresh_result").length;
    localSocket.send(JSON.stringify({
      type: "refresh_result",
      viewer_id: viewerId,
      request_id: hiddenRequestId,
      database_epoch: snapshotControlFixture.database_epoch,
      status: "failed",
      error_code: "refresh_failed",
    }));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(viewerCollector.messages.filter((event) => event.type === "refresh_result")).toHaveLength(visibleResultCount);

    const hiddenIndex = viewerCollector.messages.length;
    const hiddenPublication = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: {
        ...localHeaders(),
        "content-type": "application/json",
        [SNAPSHOT_NOTIFY_VIEWERS_HEADER]: "1",
      },
      body: JSON.stringify({ ...localSnapshot(43), reported_at: Date.now() }),
    }));
    expect(hiddenPublication.status).toBe(200);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(viewerCollector.messages.slice(hiddenIndex).some((event) => event.type === "invalidate")).toBe(false);

    viewerSocket.close(1000, "Test complete");
    localSocket.close(1000, "Test complete");
  });

  it("closes the viewer control socket on malformed or oversized frames", async () => {
    const session = await readJson<SessionResponse>(await createSession());
    const malformed = await openViewer(session.ticket);
    const malformedCollector = collectMessages(malformed.webSocket!);
    await malformedCollector.waitFor((event) => event.type === "ready");
    const malformedClose = waitForClose(malformed.webSocket!, 2_000);
    malformed.webSocket!.send("{");
    expect((await malformedClose).code).toBe(1008);

    const oversized = await openViewer(session.ticket);
    const oversizedCollector = collectMessages(oversized.webSocket!);
    await oversizedCollector.waitFor((event) => event.type === "ready");
    const oversizedClose = waitForClose(oversized.webSocket!, 2_000);
    oversized.webSocket!.send("x".repeat(4 * 1024 + 1));
    expect((await oversizedClose).code).toBe(1008);
  });

  it("caps viewer sockets and closes a viewer when its attached auth expiry alarm runs", async () => {
    const session = await readJson<SessionResponse>(await createSession());
    const sockets: WebSocket[] = [];
    for (let index = 0; index < 32; index += 1) {
      const response = await openViewer(session.ticket);
      expect(response.status).toBe(101);
      const socket = response.webSocket!;
      socket.accept();
      sockets.push(socket);
    }
    expect((await openViewer(session.ticket)).status).toBe(429);
    const closeWaiters = sockets.map((socket) => waitForClose(socket, 2_000));
    for (const socket of sockets) socket.close(1000, "Test complete");
    await Promise.all(closeWaiters);

    const now = Date.now();
    const shortTicket = await issueMiniAppUpdatesTicket(
      botToken,
      String(ownerId),
      await getDatabaseEpoch(testEnv.DB),
      now + 3_000,
      now,
    );
    const expiring = await openViewer(shortTicket.ticket);
    expect(expiring.status).toBe(101);
    const expiringSocket = expiring.webSocket!;
    expiringSocket.accept();
    const closed = waitForClose(expiringSocket, 5_000);
    await closed.then((event) => expect(event.code).toBe(4401));
  });
});
