import { readFile } from "node:fs/promises";
import { webcrypto } from "node:crypto";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createSnapshotUpdates } from "../public/file/snapshot-updates.js";
import { makeChunkedSnapshot, TEST_DATABASE_EPOCH } from "./snapshot-loader-fixtures.js";
import { syntheticCredentials } from "./fixtures/synthetic-credentials.js";

const UPDATE_PROTOCOL = "miniapp-updates-v1";
const MOCK_INIT_DATA = new URLSearchParams({ query_id: "snapshot-updates-ui", auth_date: "0" }).toString();
const LIBRARY_ID = "main-library";
const ROOT_BINDING = `v1:${"a".repeat(64)}`;

type SnapshotBundle = Awaited<ReturnType<typeof makeChunkedSnapshot>>;

let originalWindowWebSocket: PropertyDescriptor | undefined;

function makeMemoryStorage() {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, String(value)),
    removeItem: (key: string) => values.delete(key),
    clear: () => values.clear(),
  };
}

function ownerSnapshot({
  title = "Alpha video",
  category = "Films",
  stateVersion = 1,
  reportedAt,
  pendingRequests = [],
  operations = [],
}: {
  title?: string;
  category?: string;
  stateVersion?: number;
  reportedAt?: number;
  pendingRequests?: unknown[];
  operations?: unknown[];
} = {}) {
  return {
    synced: true,
    stale: false,
    reported_at: reportedAt,
    state_version: stateVersion,
    last_seen: Math.floor(Date.now() / 1_000),
    settings: {
      download_dir: "/srv/media",
      applied: {
        default_library_id: LIBRARY_ID,
        libraries: [{
          id: LIBRARY_ID,
          name: "Main library",
          root: "/srv/media",
          root_binding: ROOT_BINDING,
          default_category: category,
          is_default: true,
        }],
      },
    },
    library: {
      default_library_id: LIBRARY_ID,
      revision: `revision-${stateVersion}`,
      scanned_at: 1_800_000_000 + stateVersion,
      root_label: "Main library",
      ready: true,
      items: [{ id: "item-alpha", title, relative_path: `Films/${title}.mkv`, presence: "present", bytes: 1_024 }],
      categories: ["Films", "Shows"],
      warnings: [],
      libraries: [{
        id: LIBRARY_ID,
        root: "/srv/media",
        root_binding: ROOT_BINDING,
        availability: "available",
        available: true,
        is_default: true,
      }],
    },
    pending_requests: pendingRequests,
    tasks: [],
    operations,
    previews: [],
  };
}

function jsonResponse(data: unknown, status = 200): Response {
  return { ok: status >= 200 && status < 300, status, json: async () => data } as Response;
}

function sessionResponse(databaseEpoch = TEST_DATABASE_EPOCH) {
  return {
    ticket: syntheticCredentials.localSharedSecret.value,
    websocket_path: "/api/updates/ws",
    protocol: UPDATE_PROTOCOL,
    expires_at: Date.now() + 6 * 60 * 60 * 1_000,
    database_epoch: databaseEpoch,
  };
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  let markSettled!: () => void;
  const settled = new Promise<void>((done) => { markSettled = done; });
  return { promise, resolve, settled, markSettled };
}

class MockWebSocket extends EventTarget {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  readonly url: string;
  readonly protocols: string[];
  readonly sent: string[] = [];
  protocol = "";
  readyState = MockWebSocket.CONNECTING;
  binaryType: BinaryType = "blob";
  bufferedAmount = 0;
  extensions = "";
  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  onclose: ((event: Event) => void) | null = null;

  constructor(url: string | URL, protocols?: string | string[]) {
    super();
    this.url = String(url);
    this.protocols = Array.isArray(protocols) ? [...protocols] : protocols ? [protocols] : [];
    this.protocol = this.protocols[0] ?? "";
  }

  open() {
    if (this.readyState === MockWebSocket.CLOSED) return;
    this.readyState = MockWebSocket.OPEN;
    this.dispatch("open");
  }

  receive(message: Record<string, unknown>) {
    if (this.readyState !== MockWebSocket.OPEN) return;
    this.dispatch("message", { data: JSON.stringify(message) });
  }

  serverClose(code: number, reason = "") {
    if (this.readyState === MockWebSocket.CLOSED) return;
    this.readyState = MockWebSocket.CLOSED;
    this.dispatch("close", { code, reason, wasClean: code === 1000 });
  }

  send(data: string) { this.sent.push(data); }

  close(code = 1000, reason = "") {
    this.readyState = MockWebSocket.CLOSING;
    this.serverClose(code, reason);
  }

  private dispatch(type: string, properties: Record<string, unknown> = {}) {
    const event = new Event(type);
    for (const [key, value] of Object.entries(properties)) {
      Object.defineProperty(event, key, { configurable: true, value });
    }
    this.dispatchEvent(event);
    const handler = (this as unknown as Record<string, ((value: Event) => void) | null>)[`on${type}`];
    handler?.call(this, event);
  }
}

function addMockWebSocket({ autoReady = true, snapshotControl = "v1" }: { autoReady?: boolean; snapshotControl?: "v1" | null } = {}) {
  const sockets: MockWebSocket[] = [];
  let failuresRemaining = 0;
  let shouldAutoReady = autoReady;

  class AutoReadyWebSocket extends MockWebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols);
      sockets.push(this);
      const fail = failuresRemaining > 0;
      if (fail) failuresRemaining -= 1;
      const sendReady = shouldAutoReady;
      Promise.resolve().then(() => {
        if (fail) {
          this.serverClose(1006, "Mock network interruption");
          return;
        }
        this.open();
        if (sendReady) this.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH, ...(snapshotControl ? { snapshot_control: snapshotControl } : {}) });
      });
    }
  }

  originalWindowWebSocket ??= Object.getOwnPropertyDescriptor(window, "WebSocket");
  vi.stubGlobal("WebSocket", AutoReadyWebSocket as unknown as typeof WebSocket);
  Object.defineProperty(window, "WebSocket", {
    configurable: true,
    writable: true,
    value: AutoReadyWebSocket,
  });
  return {
    sockets,
    WebSocket: AutoReadyWebSocket,
    failNextConnections(count: number) { failuresRemaining += count; },
    setAutoReady(value: boolean) { shouldAutoReady = value; },
  };
}

function createSnapshotServer(initial: SnapshotBundle) {
  let current = initial;
  const queued: SnapshotBundle[] = [];
  const bundles = new Map<string, string>();
  const counts = { sessions: 0, states: 0, chunks: 0, actions: 0, actionReplies: 0 };
  let sessionGate: ReturnType<typeof deferred> | null = null;
  let stateGate: ReturnType<typeof deferred> | null = null;
  let sessionStatus = 200;
  let nextStateStatus = 200;
  let actionHandler: ((action: Record<string, unknown>) => void | Promise<void>) | null = null;

  function register(bundle: SnapshotBundle) {
    for (const [sha256, data] of bundle.chunks) bundles.set(sha256, data);
  }
  register(initial);

  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    const headers = new Headers(init?.headers);
    expect(headers.get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
    expect(init?.cache).toBe("no-store");

    if (path === "/api/updates/session") {
      counts.sessions += 1;
      expect(init?.method).toBe("POST");
      if (sessionGate) {
        const gate = sessionGate;
        sessionGate = null;
        await gate.promise;
        gate.markSettled();
      }
      return jsonResponse(sessionResponse(), sessionStatus);
    }
    if (path === "/api/state") {
      counts.states += 1;
      expect(headers.get("X-Snapshot-Format")).toBe("chunked-v1");
      if (nextStateStatus !== 200) {
        const status = nextStateStatus;
        nextStateStatus = 200;
        return jsonResponse({ error: "Snapshot unavailable" }, status);
      }
      const bundle = queued.shift() ?? current;
      if (stateGate) {
        const gate = stateGate;
        stateGate = null;
        await gate.promise;
        gate.markSettled();
      }
      return jsonResponse(bundle.manifest);
    }
    const chunk = path.match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
    if (chunk) {
      counts.chunks += 1;
      expect(headers.get("X-Cloud-Database-Epoch")).toBe(TEST_DATABASE_EPOCH);
      const sha256 = chunk[1]!;
      return jsonResponse({ sha256, data: bundles.get(sha256) });
    }
    if (path === "/api/actions") {
      counts.actions += 1;
      const action = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
      await actionHandler?.(action);
      counts.actionReplies += 1;
      return jsonResponse({ seq: 7 });
    }
    throw new Error(`Unexpected request ${path}`);
  });

  return {
    counts,
    fetchMock,
    setCurrent(bundle: SnapshotBundle) { current = bundle; register(bundle); },
    queueSnapshot(bundle: SnapshotBundle) { queued.push(bundle); register(bundle); },
    holdNextSession() {
      const gate = deferred();
      sessionGate = gate;
      return gate;
    },
    holdNextState() {
      const gate = deferred();
      stateGate = gate;
      return gate;
    },
    setSessionStatus(status: number) { sessionStatus = status; },
    failNextState(status = 503) { nextStateStatus = status; },
    setActionHandler(handler: (action: Record<string, unknown>) => void | Promise<void>) { actionHandler = handler; },
  };
}

function element<T extends Element>(selector: string): T {
  const found = document.querySelector<T>(selector);
  if (!found) throw new Error(`Expected element ${selector}`);
  return found;
}

function sentFrames(socket: MockWebSocket): Record<string, unknown>[] {
  return socket.sent.map((frame) => JSON.parse(frame) as Record<string, unknown>);
}

function sentFramesOfType(socket: MockWebSocket, type: string): Record<string, unknown>[] {
  return sentFrames(socket).filter((frame) => frame.type === type);
}

function publishRefreshResult(socket: MockWebSocket, request: Record<string, unknown>, stateVersion: number) {
  socket.receive({
    type: "refresh_result",
    request_id: request.request_id,
    database_epoch: TEST_DATABASE_EPOCH,
    status: "published",
    state_version: stateVersion,
    reported_at: Math.floor(Date.now() / 1_000),
  });
}

async function loadPageBody(path: string) {
  const html = await readFile(resolve(process.cwd(), path), "utf8");
  const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
  expect(body).toBeDefined();
  document.body.innerHTML = body!;
}

function installBrowserMocks({ telegramEvents = false } = {}) {
  const eventHandlers = new Map<string, () => void>();
  const webApp = {
    initData: MOCK_INIT_DATA,
    ready: vi.fn(),
    expand: vi.fn(),
    ...(telegramEvents ? {
      onEvent: vi.fn((event: string, handler: () => void) => eventHandlers.set(event, handler)),
      offEvent: vi.fn((event: string, handler: () => void) => {
        if (eventHandlers.get(event) === handler) eventHandlers.delete(event);
      }),
    } : {}),
  };
  Object.defineProperty(window, "Telegram", { configurable: true, value: { WebApp: webApp } });
  Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
  vi.stubGlobal("crypto", webcrypto);
  const localStorage = makeMemoryStorage();
  vi.stubGlobal("localStorage", localStorage);
  return { webApp, eventHandlers, localStorage };
}

afterEach(async () => {
  window.dispatchEvent(new Event("pagehide"));
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  if (originalWindowWebSocket) {
    Object.defineProperty(window, "WebSocket", originalWindowWebSocket);
    originalWindowWebSocket = undefined;
  } else {
    delete (window as Window & { WebSocket?: typeof WebSocket }).WebSocket;
  }
  delete (window as Window & { Telegram?: unknown }).Telegram;
  const domWindow = window as Window & { happyDOM?: { cancelAsync(): void } };
  domWindow.happyDOM?.cancelAsync();
  document.body.innerHTML = "";
});

describe("snapshot updates browser flows", () => {
  it("loads the published snapshot before one correlated open refresh and keeps local/idle work request-free", async () => {
    vi.useFakeTimers();
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot());
    const server = createSnapshotServer({ manifest, chunks });
    const mockSockets = addMockWebSocket({ autoReady: false });
    const { localStorage } = installBrowserMocks();
    await loadPageBody("public/file/index.html");
    vi.stubGlobal("fetch", server.fetchMock);
    window.fetch = server.fetchMock as typeof window.fetch;

    vi.resetModules();
    // The browser entry intentionally has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is declaration-free.
    await import("../public/file/app.js");
    await vi.waitFor(() => expect(server.counts.states).toBe(1));
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    expect(element<HTMLElement>("#library-list").textContent).toContain("Alpha video");
    expect(sentFramesOfType(mockSockets.sockets[0]!, "refresh_request")).toHaveLength(0);

    await vi.waitFor(() => expect(server.counts.sessions).toBe(1));
    const socket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(socket.readyState).toBe(MockWebSocket.OPEN));
    expect(socket.protocols).toEqual([UPDATE_PROTOCOL, syntheticCredentials.localSharedSecret.value]);
    socket.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH, snapshot_control: "v1" });
    await vi.waitFor(() => expect(sentFramesOfType(socket, "refresh_request")).toHaveLength(1));
    const openRequest = sentFramesOfType(socket, "refresh_request")[0]!;
    expect(openRequest).toMatchObject({ reason: "open", scope: "library", library_id: LIBRARY_ID });
    expect(openRequest.request_id).toMatch(/^[0-9a-f-]{36}$/iu);
    expect(server.counts.states).toBe(1);

    const openedSnapshot = await makeChunkedSnapshot(ownerSnapshot({ title: "Beta from desktop", stateVersion: 2 }));
    server.setCurrent(openedSnapshot);
    publishRefreshResult(socket, openRequest, 2);
    await vi.waitFor(() => expect(server.counts.states).toBe(2));
    await vi.waitFor(() => expect(element<HTMLElement>("#library-list").textContent).toContain("Beta from desktop"));
    expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("Desktop snapshot published");

    const sentBeforeLocalViewChange = socket.sent.length;
    const search = element<HTMLInputElement>("#search-input");
    search.value = "Alpha";
    search.dispatchEvent(new Event("input", { bubbles: true }));
    search.value = "";
    search.dispatchEvent(new Event("input", { bubbles: true }));
    element<HTMLButtonElement>('[data-view="history"]').click();
    await vi.advanceTimersByTimeAsync(0);
    expect(socket.sent).toHaveLength(sentBeforeLocalViewChange);
    expect(server.counts.states).toBe(2);
    expect(element<HTMLElement>("#sync-detail").textContent).toContain("Snapshot published");

    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(sentFramesOfType(socket, "refresh_request")).toHaveLength(2));
    const manualRequest = sentFramesOfType(socket, "refresh_request")[1]!;
    expect(manualRequest).toMatchObject({ reason: "manual", scope: "operations" });
    socket.receive({
      type: "refresh_result",
      request_id: manualRequest.request_id,
      database_epoch: TEST_DATABASE_EPOCH,
      status: "failed",
      error_code: "desktop_busy",
    });
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("Desktop refresh failed"));
    expect(server.counts.states).toBe(2);
    expect(element<HTMLElement>("#library-list").textContent).toContain("Beta from desktop");

    const cacheReadFailureSnapshot = await makeChunkedSnapshot(ownerSnapshot({ title: "Unreloaded desktop snapshot", stateVersion: 3 }));
    server.setCurrent(cacheReadFailureSnapshot);
    server.failNextState();
    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(sentFramesOfType(socket, "refresh_request")).toHaveLength(3));
    const cacheReadFailureRequest = sentFramesOfType(socket, "refresh_request")[2]!;
    publishRefreshResult(socket, cacheReadFailureRequest, 3);
    await vi.waitFor(() => expect(server.counts.states).toBe(3));
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("displayed saved view could not be reloaded"));
    expect(element<HTMLElement>("#library-list").textContent).toContain("Beta from desktop");

    element<HTMLButtonElement>("#download-here").click();
    await vi.waitFor(() => expect(sentFramesOfType(socket, "refresh_request")).toHaveLength(4));
    const referenceRequest = sentFramesOfType(socket, "refresh_request")[3]!;
    expect(referenceRequest).toMatchObject({ reason: "reference", scope: "download", library_id: LIBRARY_ID });
    const referenceSnapshot = await makeChunkedSnapshot(ownerSnapshot({ title: "Download references refreshed", stateVersion: 4 }));
    server.setCurrent(referenceSnapshot);
    publishRefreshResult(socket, referenceRequest, 4);
    await vi.waitFor(() => expect(server.counts.states).toBe(4));
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("Desktop snapshot published"));

    const serverRequestsBeforeIdle = socket.sent.length;
    await vi.advanceTimersByTimeAsync(60_000);
    expect(server.counts.states).toBe(4);
    expect(server.counts.sessions).toBe(1);
    expect(socket.sent).toHaveLength(serverRequestsBeforeIdle);
    expect(element<HTMLElement>("#connection-label").textContent).toContain("Saved snapshot");

    localStorage.setItem("file-manager.snapshot.v1", "private cached snapshot");
    socket.serverClose(4401, "Owner authorization expired");
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(true));
    expect(element<HTMLElement>("#auth-gate").classList.contains("hidden")).toBe(false);
    expect(localStorage.getItem("file-manager.snapshot.v1")).toBeNull();
  });

  it("watches a pending settings result, reads only its invalidations, and resumes without an automatic refresh", async () => {
    vi.useFakeTimers();
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot());
    const server = createSnapshotServer({ manifest, chunks });
    const mockSockets = addMockWebSocket();
    const { eventHandlers } = installBrowserMocks({ telegramEvents: true });
    const stored = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => stored.get(key) ?? null,
      setItem: (key: string, value: string) => stored.set(key, String(value)),
      removeItem: (key: string) => stored.delete(key),
    });
    await loadPageBody("public/settings/index.html");
    vi.stubGlobal("fetch", server.fetchMock);
    window.fetch = server.fetchMock as typeof window.fetch;

    vi.resetModules();
    // The browser entry intentionally has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is declaration-free.
    await import("../public/settings/app.js");
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    await vi.waitFor(() => expect(sentFramesOfType(mockSockets.sockets[0]!, "refresh_request")).toHaveLength(1));
    const socket = mockSockets.sockets[0]!;
    const openRequest = sentFramesOfType(socket, "refresh_request")[0]!;
    expect(openRequest).toMatchObject({ reason: "open", scope: "settings" });
    const initial = await makeChunkedSnapshot(ownerSnapshot());
    server.setCurrent(initial);
    publishRefreshResult(socket, openRequest, 1);
    await vi.waitFor(() => expect(server.counts.states).toBe(2));
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("Desktop settings published"));

    server.setActionHandler(async () => {
      server.setCurrent(await makeChunkedSnapshot(ownerSnapshot({
        stateVersion: 2,
        pendingRequests: [{ seq: 7, kind: "library_scan", status: "pending", created_at: Math.floor(Date.now() / 1_000) }],
      })));
    });
    element<HTMLButtonElement>('[data-settings-action="library_scan"]').click();
    await vi.waitFor(() => expect(server.counts.actionReplies).toBe(1));
    await vi.waitFor(() => expect(sentFramesOfType(socket, "interest").some((frame) => frame.watch === true)).toBe(true));
    expect(sentFramesOfType(socket, "refresh_request")).toHaveLength(1);

    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "inbox" });
    await vi.waitFor(() => expect(server.counts.states).toBe(3));
    await vi.waitFor(() => expect(element<HTMLElement>("#waiting-list").textContent).toContain("Waiting · #7"));

    const statesBeforePause = server.counts.states;
    const sessionsBeforePause = server.counts.sessions;
    eventHandlers.get("deactivated")?.();
    expect(socket.readyState).toBe(MockWebSocket.CLOSED);
    const completed = await makeChunkedSnapshot(ownerSnapshot({
      stateVersion: 3,
      operations: [{ seq: 7, kind: "library_scan", status: "completed", updated_at: Math.floor(Date.now() / 1_000), result: { message: "Desktop scan finished." } }],
    }));
    server.setCurrent(completed);
    eventHandlers.get("activated")?.();
    await vi.waitFor(() => expect(server.counts.sessions).toBe(sessionsBeforePause + 1));
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(2));
    const resumedSocket = mockSockets.sockets[1]!;
    await vi.waitFor(() => expect(sentFramesOfType(resumedSocket, "interest").some((frame) => frame.watch === true)).toBe(true));
    expect(server.counts.states).toBe(statesBeforePause);
    expect(sentFramesOfType(resumedSocket, "refresh_request")).toHaveLength(0);

    resumedSocket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 3, reason: "inbox" });
    await vi.waitFor(() => expect(server.counts.states).toBe(statesBeforePause + 1));
    await vi.waitFor(() => expect(element<HTMLElement>("#operations-list").textContent).toContain("Desktop scan finished."));
    await vi.waitFor(() => expect(element<HTMLElement>("#toast").textContent).toContain("was applied by the desktop"));
    expect(sentFramesOfType(resumedSocket, "refresh_request")).toHaveLength(0);

    resumedSocket.serverClose(4401, "Owner authorization expired");
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(true));
    expect(element<HTMLElement>("#auth-gate").classList.contains("hidden")).toBe(false);
  });

  it("does not keep a watch for a completed restored recovery report while browsing locally", async () => {
    vi.useFakeTimers();
    const historicalOperation = {
      seq: 9,
      request_id: "completed-recovery-request",
      kind: "metadata_recover",
      status: "completed",
      library_id: LIBRARY_ID,
      root_binding: ROOT_BINDING,
      scope: "library",
      result: {
        library_id: LIBRARY_ID,
        root_binding: ROOT_BINDING,
        scope: "library",
        recovery: { revision: "revision-1", items: [], total: 0, remaining: 0, next_cursor: null },
      },
    };
    const initial = await makeChunkedSnapshot(ownerSnapshot({
      reportedAt: Math.floor(Date.now() / 1_000),
      operations: [historicalOperation],
    }));
    const server = createSnapshotServer(initial);
    const mockSockets = addMockWebSocket();
    installBrowserMocks();
    await loadPageBody("public/file/index.html");
    vi.stubGlobal("fetch", server.fetchMock);
    window.fetch = server.fetchMock as typeof window.fetch;

    vi.resetModules();
    // The browser entry intentionally has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is declaration-free.
    await import("../public/file/app.js");
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    const socket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(sentFramesOfType(socket, "refresh_request")).toHaveLength(1));
    const openRequest = sentFramesOfType(socket, "refresh_request")[0]!;
    expect(sentFramesOfType(socket, "interest").every((frame) => frame.watch === false)).toBe(true);
    server.setCurrent(initial);
    publishRefreshResult(socket, openRequest, 1);
    await vi.waitFor(() => expect(server.counts.states).toBe(2));
    await vi.waitFor(() => expect(element<HTMLElement>("#recovery-status").textContent).toContain("Analysis revision revision-1"));

    const sentBeforeLocalWork = socket.sent.length;
    const search = element<HTMLInputElement>("#search-input");
    search.value = "Alpha";
    search.dispatchEvent(new Event("input", { bubbles: true }));
    element<HTMLButtonElement>('[data-view="needs"]').click();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(socket.sent).toHaveLength(sentBeforeLocalWork);
    expect(server.counts.states).toBe(2);
    expect(sentFramesOfType(socket, "interest").every((frame) => frame.watch === false)).toBe(true);
  });

  it("deduplicates interest frames and coalesces invalidation plus publication cache reads", async () => {
    vi.useFakeTimers();
    const mockSockets = addMockWebSocket({ autoReady: false });
    const api = vi.fn(async () => sessionResponse());
    let resolveRead!: (value: { state_version: number; database_epoch: string }) => void;
    const readGate = new Promise<{ state_version: number; database_epoch: string }>((resolve) => { resolveRead = resolve; });
    const readSnapshot = vi.fn(async () => await readGate);
    const controller = createSnapshotUpdates({
      api: api as unknown as (path: string, options: { method: "POST"; signal?: AbortSignal }) => Promise<unknown>,
      readSnapshot,
      surface: "file",
      environment: { WebSocket: mockSockets.WebSocket as unknown as typeof WebSocket, randomUUID: () => "8c2fda7e-8198-4192-8c02-0e746a711491", random: () => 0.5 },
    });
    controller.start();
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    const socket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(socket.readyState).toBe(MockWebSocket.OPEN));
    socket.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH, snapshot_control: "v1" });
    expect(sentFramesOfType(socket, "interest")).toHaveLength(1);
    controller.setWatch(false);
    controller.setWatch(false);
    expect(sentFramesOfType(socket, "interest")).toHaveLength(1);
    controller.setWatch(true);
    controller.setWatch(true);
    expect(sentFramesOfType(socket, "interest")).toHaveLength(2);
    controller.setWatch(false);
    expect(sentFramesOfType(socket, "interest")).toHaveLength(3);

    const openRefresh = controller.requestMacRefresh({ reason: "open", scope: "library", libraryId: LIBRARY_ID });
    const manualRefresh = controller.requestMacRefresh({ reason: "manual", scope: "library", libraryId: LIBRARY_ID });
    expect(manualRefresh).toBe(openRefresh);
    await vi.waitFor(() => expect(sentFramesOfType(socket, "refresh_request")).toHaveLength(1));
    const request = sentFramesOfType(socket, "refresh_request")[0]!;
    expect(request).toMatchObject({ reason: "open", scope: "library", library_id: LIBRARY_ID });
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "state" });
    await vi.waitFor(() => expect(readSnapshot).toHaveBeenCalledTimes(1));
    publishRefreshResult(socket, request, 2);
    resolveRead({ state_version: 2, database_epoch: TEST_DATABASE_EPOCH });
    const result = await openRefresh;
    expect(result).toMatchObject({ status: "published", state_version: 2, snapshot: { state_version: 2 } });
    expect(readSnapshot).toHaveBeenCalledTimes(1);
    controller.stop();
  });

  it("settles a sent refresh when its viewer disconnects and does not replay it after reconnect", async () => {
    vi.useFakeTimers();
    const mockSockets = addMockWebSocket();
    const api = vi.fn(async () => sessionResponse());
    const controller = createSnapshotUpdates({
      api: api as unknown as (path: string, options: { method: "POST"; signal?: AbortSignal }) => Promise<unknown>,
      readSnapshot: vi.fn(async () => ({ state_version: 1, database_epoch: TEST_DATABASE_EPOCH })),
      surface: "settings",
      environment: { WebSocket: mockSockets.WebSocket as unknown as typeof WebSocket, random: () => 0.5 },
    });
    controller.start();
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    const firstSocket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(sentFramesOfType(firstSocket, "interest")).toHaveLength(1));

    const refresh = controller.requestMacRefresh({ reason: "manual", scope: "operations" });
    await vi.waitFor(() => expect(sentFramesOfType(firstSocket, "refresh_request")).toHaveLength(1));
    firstSocket.serverClose(1006, "Mock network interruption");

    await expect(refresh).resolves.toMatchObject({
      status: "unavailable",
      error_code: "transport_disconnected",
    });
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(2));
    const reconnectedSocket = mockSockets.sockets[1]!;
    await vi.waitFor(() => expect(sentFramesOfType(reconnectedSocket, "interest")).toHaveLength(1));
    expect(sentFramesOfType(firstSocket, "refresh_request")).toHaveLength(1);
    expect(sentFramesOfType(reconnectedSocket, "refresh_request")).toHaveLength(0);
    controller.stop();
  });

  it("starts the response timeout after send and accepts a result near the 120-second server window", async () => {
    vi.useFakeTimers();
    const mockSockets = addMockWebSocket({ autoReady: false });
    const sessionGate = deferred();
    const api = vi.fn(async () => {
      await sessionGate.promise;
      sessionGate.markSettled();
      return sessionResponse();
    });
    let resolveRead!: (value: { state_version: number; database_epoch: string }) => void;
    const readGate = new Promise<{ state_version: number; database_epoch: string }>((resolve) => { resolveRead = resolve; });
    const readSnapshot = vi.fn(async () => await readGate);
    const controller = createSnapshotUpdates({
      api: api as unknown as (path: string, options: { method: "POST"; signal?: AbortSignal }) => Promise<unknown>,
      readSnapshot,
      surface: "settings",
      environment: { WebSocket: mockSockets.WebSocket as unknown as typeof WebSocket, random: () => 0.5 },
    });
    controller.start();
    const refresh = controller.requestMacRefresh({ reason: "manual", scope: "operations" });
    let refreshSettled = false;
    void refresh.then(() => { refreshSettled = true; });

    await vi.advanceTimersByTimeAsync(9_000);
    sessionGate.resolve();
    await sessionGate.settled;
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    const socket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(socket.readyState).toBe(MockWebSocket.OPEN));
    await vi.advanceTimersByTimeAsync(9_000);
    socket.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH, snapshot_control: "v1" });
    await vi.waitFor(() => expect(sentFramesOfType(socket, "refresh_request")).toHaveLength(1));
    const request = sentFramesOfType(socket, "refresh_request")[0]!;

    await vi.advanceTimersByTimeAsync(119_900);
    expect(refreshSettled).toBe(false);
    publishRefreshResult(socket, request, 2);
    await vi.waitFor(() => expect(readSnapshot).toHaveBeenCalledTimes(1));
    await vi.advanceTimersByTimeAsync(10_000);
    expect(refreshSettled).toBe(false);
    resolveRead({ state_version: 2, database_epoch: TEST_DATABASE_EPOCH });
    await expect(refresh).resolves.toMatchObject({ status: "published", state_version: 2 });
    expect(readSnapshot).toHaveBeenCalledTimes(1);
    controller.stop();
  });

  it("keeps old viewers compatible and performs no idle reads for 24 hours", async () => {
    vi.useFakeTimers();
    const mockSockets = addMockWebSocket({ autoReady: false, snapshotControl: null });
    const api = vi.fn(async () => sessionResponse());
    const readSnapshot = vi.fn(async () => ({ state_version: 2, database_epoch: TEST_DATABASE_EPOCH }));
    const onAuthorizationExpired = vi.fn();
    const controller = createSnapshotUpdates({
      api: api as unknown as (path: string, options: { method: "POST"; signal?: AbortSignal }) => Promise<unknown>,
      readSnapshot,
      surface: "settings",
      onAuthorizationExpired,
      environment: { WebSocket: mockSockets.WebSocket as unknown as typeof WebSocket, random: () => 0.5 },
    });
    controller.start();
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    const socket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(socket.readyState).toBe(MockWebSocket.OPEN));
    socket.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH });
    expect(sentFramesOfType(socket, "interest")).toHaveLength(0);
    expect(await controller.requestMacRefresh({ reason: "open", scope: "settings" }))
      .toMatchObject({ status: "unavailable", error_code: "unsupported" });

    controller.setWatch(true);
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "state" });
    await vi.waitFor(() => expect(readSnapshot).toHaveBeenCalledTimes(1));
    controller.setWatch(false);
    const sentBeforeIdle = socket.sent.length;
    await vi.advanceTimersByTimeAsync(24 * 60 * 60 * 1_000);
    expect(readSnapshot).toHaveBeenCalledTimes(1);
    expect(api).toHaveBeenCalledTimes(1);
    expect(socket.sent).toHaveLength(sentBeforeIdle);
    expect(onAuthorizationExpired).toHaveBeenCalledTimes(1);
    controller.stop();
  });

  it("keeps a late watched cache read private after 4401 and ignores an old resume callback", async () => {
    vi.useFakeTimers();
    const mockSockets = addMockWebSocket({ autoReady: false });
    const handlers = new Map<string, () => void>();
    const telegram = {
      onEvent: (event: "activated" | "deactivated", handler: () => void) => { handlers.set(event, handler); },
      offEvent: (event: "activated" | "deactivated", handler: () => void) => {
        if (handlers.get(event) === handler) handlers.delete(event);
      },
    };
    const api = vi.fn(async () => sessionResponse());
    const heldRead = deferred();
    const readSnapshot = vi.fn(() => heldRead.promise.then(() => ({ state_version: 2, database_epoch: TEST_DATABASE_EPOCH, private_title: "Late private snapshot" })));
    const onAuthorizationExpired = vi.fn();
    const controller = createSnapshotUpdates({
      api: api as unknown as (path: string, options: { method: "POST"; signal?: AbortSignal }) => Promise<unknown>,
      readSnapshot,
      surface: "file",
      onAuthorizationExpired,
      environment: { WebSocket: mockSockets.WebSocket as unknown as typeof WebSocket, telegram, random: () => 0.5 },
    });
    controller.start();
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    const socket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(socket.readyState).toBe(MockWebSocket.OPEN));
    socket.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH, snapshot_control: "v1" });
    controller.setWatch(true);
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "state" });
    await vi.waitFor(() => expect(readSnapshot).toHaveBeenCalledTimes(1));

    const staleResume = handlers.get("activated");
    expect(staleResume).toBeDefined();
    socket.serverClose(4401, "Owner authorization expired");
    expect(onAuthorizationExpired).toHaveBeenCalledTimes(1);
    heldRead.resolve();
    await heldRead.promise;
    for (let index = 0; index < 12; index += 1) await Promise.resolve();
    staleResume?.();
    controller.start();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(readSnapshot).toHaveBeenCalledTimes(1);
    expect(api).toHaveBeenCalledTimes(1);
    expect(mockSockets.sockets).toHaveLength(1);
  });

  it("revalidates after an epoch hint without requesting a Mac refresh and stops after 4401", async () => {
    vi.useFakeTimers();
    const mockSockets = addMockWebSocket();
    const newEpoch = "1".repeat(32);
    let currentEpoch = TEST_DATABASE_EPOCH;
    const api = vi.fn(async () => sessionResponse(currentEpoch));
    const readSnapshot = vi.fn(async (options: { expectedDatabaseEpoch?: string } = {}) => ({ state_version: 1, database_epoch: options.expectedDatabaseEpoch ?? currentEpoch }));
    const onAuthorizationExpired = vi.fn();
    const onEpochChanged = vi.fn();
    const controller = createSnapshotUpdates({
      api: api as unknown as (path: string, options: { method: "POST"; signal?: AbortSignal }) => Promise<unknown>,
      readSnapshot,
      surface: "file",
      onAuthorizationExpired,
      onEpochChanged,
      environment: { WebSocket: mockSockets.WebSocket as unknown as typeof WebSocket, random: () => 0.5 },
    });
    controller.start();
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    const socket = mockSockets.sockets[0]!;
    socket.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH, snapshot_control: "v1" });
    currentEpoch = newEpoch;
    socket.receive({ type: "invalidate", database_epoch: newEpoch, state_version: 1, reason: "epoch" });
    await vi.waitFor(() => expect(readSnapshot).toHaveBeenCalledTimes(1));
    expect(readSnapshot).toHaveBeenCalledWith(expect.objectContaining({ expectedDatabaseEpoch: newEpoch, epochChanged: true }));
    expect(onEpochChanged).toHaveBeenCalledWith({ databaseEpoch: newEpoch });
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(2));
    expect(sentFramesOfType(mockSockets.sockets[1]!, "refresh_request")).toHaveLength(0);
    expect(api).toHaveBeenCalledTimes(2);

    mockSockets.sockets[1]!.serverClose(4401, "Owner authorization expired");
    expect(onAuthorizationExpired).toHaveBeenCalledTimes(1);
    controller.start();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(mockSockets.sockets).toHaveLength(2);
    expect(api).toHaveBeenCalledTimes(2);
  });
});
