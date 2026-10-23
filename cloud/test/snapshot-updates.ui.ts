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
  pendingRequests = [],
  operations = [],
}: {
  title?: string;
  category?: string;
  stateVersion?: number;
  pendingRequests?: unknown[];
  operations?: unknown[];
} = {}) {
  return {
    synced: true,
    stale: false,
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

function sessionResponse() {
  return {
    ticket: syntheticCredentials.localSharedSecret.value,
    websocket_path: "/api/updates/ws",
    protocol: UPDATE_PROTOCOL,
    expires_at: Date.now() + 6 * 60 * 60 * 1_000,
    database_epoch: TEST_DATABASE_EPOCH,
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

function addMockWebSocket({ autoReady = true } = {}) {
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
        if (sendReady) this.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH });
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
  const counts = { sessions: 0, states: 0, chunks: 0, actions: 0 };
  let sessionGate: ReturnType<typeof deferred> | null = null;
  let stateGate: ReturnType<typeof deferred> | null = null;
  let sessionStatus = 200;
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
    setActionHandler(handler: (action: Record<string, unknown>) => void | Promise<void>) { actionHandler = handler; },
  };
}

function element<T extends Element>(selector: string): T {
  const found = document.querySelector<T>(selector);
  if (!found) throw new Error(`Expected element ${selector}`);
  return found;
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
  it("uses ready and invalidation for file snapshots, skips the 12-second poll, coalesces hints, and keeps a late response private after 4401", async () => {
    vi.useFakeTimers();
    const { manifest: initialManifest, chunks: initialChunks } = await makeChunkedSnapshot(ownerSnapshot());
    const server = createSnapshotServer({ manifest: initialManifest, chunks: initialChunks });
    const mockSockets = addMockWebSocket({ autoReady: false });
    const { localStorage } = installBrowserMocks();
    await loadPageBody("public/file/index.html");
    vi.stubGlobal("fetch", server.fetchMock);
    window.fetch = server.fetchMock as typeof window.fetch;

    vi.resetModules();
    // The browser entry intentionally has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is declaration-free.
    await import("../public/file/app.js");
    await vi.waitFor(() => expect(server.counts.sessions).toBe(1));
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    const socket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(socket.readyState).toBe(MockWebSocket.OPEN));
    expect(socket.url).toBe("ws://localhost/api/updates/ws");
    expect(socket.protocols).toEqual([UPDATE_PROTOCOL, syntheticCredentials.localSharedSecret.value]);
    expect(server.counts.states).toBe(0);

    socket.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH });
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    await vi.waitFor(() => expect(server.counts.states).toBe(1));
    expect(element<HTMLElement>("#library-list").textContent).toContain("Alpha video");

    const healthyStateCount = server.counts.states;
    await vi.advanceTimersByTimeAsync(12_000);
    expect(server.counts.states).toBe(healthyStateCount);

    const pushed = await makeChunkedSnapshot(ownerSnapshot({ title: "Beta from WS", stateVersion: 2 }));
    server.setCurrent(pushed);
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "state" });
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(server.counts.states).toBe(healthyStateCount + 1));
    await vi.waitFor(() => expect(element<HTMLElement>("#library-list").textContent).toContain("Beta from WS"));

    const staleDuringRead = await makeChunkedSnapshot(ownerSnapshot({ title: "Stale in-flight response", stateVersion: 3 }));
    const latestAfterHints = await makeChunkedSnapshot(ownerSnapshot({ title: "Latest coalesced snapshot", stateVersion: 4 }));
    server.queueSnapshot(staleDuringRead);
    server.queueSnapshot(latestAfterHints);
    const readGate = server.holdNextState();
    const beforeCoalescedRead = server.counts.states;
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 3, reason: "state" });
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(server.counts.states).toBe(beforeCoalescedRead + 1));
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 4, reason: "inbox" });
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 5, reason: "state" });
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 5, reason: "state" });
    expect(server.counts.states).toBe(beforeCoalescedRead + 1);
    readGate.resolve();
    await vi.waitFor(() => expect(element<HTMLElement>("#library-list").textContent).toContain("Stale in-flight response"));
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.waitFor(() => expect(server.counts.states).toBe(beforeCoalescedRead + 2));
    await vi.waitFor(() => expect(element<HTMLElement>("#library-list").textContent).toContain("Latest coalesced snapshot"));

    localStorage.setItem("file-manager.snapshot.v1", "private cached snapshot");
    const latePrivateSnapshot = await makeChunkedSnapshot(ownerSnapshot({ title: "Late private response", stateVersion: 6 }));
    server.queueSnapshot(latePrivateSnapshot);
    const lateReadGate = server.holdNextState();
    const beforeLateRead = server.counts.states;
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 6, reason: "state" });
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(server.counts.states).toBe(beforeLateRead + 1));
    socket.serverClose(4401, "Owner authorization expired");
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(true));
    expect(element<HTMLElement>("#auth-gate").classList.contains("hidden")).toBe(false);
    expect(localStorage.getItem("file-manager.snapshot.v1")).toBeNull();
    lateReadGate.resolve();
    await lateReadGate.settled;
    for (let index = 0; index < 12; index += 1) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);
    expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(true);
    expect(element<HTMLElement>("#auth-gate").classList.contains("hidden")).toBe(false);
    expect(element<HTMLElement>("#library-list").textContent).not.toContain("Late private response");
  });

  it("refreshes a pending settings action from an invalidation, catches up on resume, and hides settings on 4401", async () => {
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
    await vi.waitFor(() => expect(mockSockets.sockets[0]?.readyState).toBe(MockWebSocket.OPEN));
    expect(server.counts.states).toBeGreaterThanOrEqual(1);

    server.setActionHandler(async () => {
      server.setCurrent(await makeChunkedSnapshot(ownerSnapshot({
        stateVersion: 2,
        pendingRequests: [{ seq: 7, kind: "library_scan", status: "pending", created_at: Math.floor(Date.now() / 1_000) }],
      })));
    });
    element<HTMLButtonElement>('[data-settings-action="library_scan"]').click();
    await vi.waitFor(() => expect(server.counts.actions).toBe(1));
    await vi.waitFor(() => expect(element<HTMLElement>("#waiting-list").textContent).toContain("Waiting · #7"));

    const completed = await makeChunkedSnapshot(ownerSnapshot({
      stateVersion: 3,
      operations: [{ seq: 7, kind: "library_scan", status: "completed", updated_at: Math.floor(Date.now() / 1_000), result: { message: "Desktop scan finished." } }],
    }));
    server.setCurrent(completed);
    const socket = mockSockets.sockets[0]!;
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 3, reason: "inbox" });
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.waitFor(() => expect(element<HTMLElement>("#operations-list").textContent).toContain("Desktop scan finished."));
    await vi.waitFor(() => expect(element<HTMLElement>("#toast").textContent).toContain("was applied by the desktop"));

    const statesBeforePause = server.counts.states;
    const sessionsBeforePause = server.counts.sessions;
    eventHandlers.get("deactivated")?.();
    expect(socket.readyState).toBe(MockWebSocket.CLOSED);
    const resumedSnapshot = await makeChunkedSnapshot(ownerSnapshot({ category: "Shows", stateVersion: 4 }));
    server.setCurrent(resumedSnapshot);
    eventHandlers.get("activated")?.();
    await vi.waitFor(() => expect(server.counts.sessions).toBe(sessionsBeforePause + 1));
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(2));
    await vi.waitFor(() => expect((element<HTMLInputElement>('[data-category-form="main-library"] input[name="default_category"]')).value).toBe("Shows"));
    expect(server.counts.states).toBeGreaterThan(statesBeforePause);

    mockSockets.sockets[1]!.serverClose(4401, "Owner authorization expired");
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(true));
    expect(element<HTMLElement>("#auth-gate").classList.contains("hidden")).toBe(false);
  });

  it("backs off reconnects, uses the 60-second HTTP fallback, and does not resume after controller expiry", async () => {
    vi.useFakeTimers();
    const mockSockets = addMockWebSocket({ autoReady: false });
    const api = vi.fn(async () => sessionResponse());
    const refresh = vi.fn(async () => {});
    const controller = createSnapshotUpdates({
      api: api as unknown as (path: string, options: { method: "POST"; signal?: AbortSignal }) => Promise<unknown>,
      refresh,
      environment: { WebSocket: mockSockets.WebSocket as unknown as typeof WebSocket, random: () => 0.5 },
    });
    controller.start();
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    const socket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(socket.readyState).toBe(MockWebSocket.OPEN));
    socket.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH });
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(1));

    mockSockets.failNextConnections(20);
    const baselineSessions = api.mock.calls.length;
    const refreshesBeforeDisconnect = refresh.mock.calls.length;
    socket.serverClose(1006, "Connection lost");
    await vi.advanceTimersByTimeAsync(1_000);
    expect(api).toHaveBeenCalledTimes(baselineSessions + 1);
    expect(refresh).toHaveBeenCalledTimes(refreshesBeforeDisconnect + 1);
    await vi.advanceTimersByTimeAsync(1_999);
    expect(api).toHaveBeenCalledTimes(baselineSessions + 1);
    await vi.advanceTimersByTimeAsync(1);
    expect(api).toHaveBeenCalledTimes(baselineSessions + 2);
    await vi.advanceTimersByTimeAsync(56_999);
    expect(refresh).toHaveBeenCalledTimes(refreshesBeforeDisconnect + 1);
    await vi.advanceTimersByTimeAsync(1_001);
    expect(refresh).toHaveBeenCalledTimes(refreshesBeforeDisconnect + 2);
    controller.stop();
  });

  it("does not restart or refresh after 4401 even when a held controller read completes and an old resume callback fires", async () => {
    vi.useFakeTimers();
    const mockSockets = addMockWebSocket({ autoReady: false });
    const handlers = new Map<string, () => void>();
    const telegram = {
      onEvent: (_event: "activated" | "deactivated", handler: () => void) => { handlers.set(_event, handler); },
      offEvent: (_event: "activated" | "deactivated", handler: () => void) => {
        if (handlers.get(_event) === handler) handlers.delete(_event);
      },
    };
    const api = vi.fn(async () => sessionResponse());
    const heldRead = deferred();
    let holdNextRefresh = false;
    const refresh = vi.fn(() => holdNextRefresh ? heldRead.promise : Promise.resolve());
    const onAuthorizationExpired = vi.fn();
    const controller = createSnapshotUpdates({
      api: api as unknown as (path: string, options: { method: "POST"; signal?: AbortSignal }) => Promise<unknown>,
      refresh,
      onAuthorizationExpired,
      environment: { WebSocket: mockSockets.WebSocket as unknown as typeof WebSocket, telegram, random: () => 0.5 },
    });
    controller.start();
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    const socket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(socket.readyState).toBe(MockWebSocket.OPEN));
    socket.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH });
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(1));

    const staleResume = handlers.get("activated");
    expect(staleResume).toBeDefined();
    holdNextRefresh = true;
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "state" });
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(2));
    socket.serverClose(4401, "Owner authorization expired");
    expect(onAuthorizationExpired).toHaveBeenCalledTimes(1);

    const sessionCallsAfterExpiry = api.mock.calls.length;
    heldRead.resolve();
    for (let index = 0; index < 12; index += 1) await Promise.resolve();
    staleResume?.();
    controller.start();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(refresh).toHaveBeenCalledTimes(2);
    expect(api).toHaveBeenCalledTimes(sessionCallsAfterExpiry);
    expect(mockSockets.sockets).toHaveLength(1);
  });

  it("backs off failed live refreshes and preserves a queued explicit refresh", async () => {
    vi.useFakeTimers();
    const mockSockets = addMockWebSocket({ autoReady: false });
    const heldRefresh = deferred();
    const quietValues: boolean[] = [];
    const refreshStartedAt: number[] = [];
    let calls = 0;
    const refresh = vi.fn(async ({ quiet }: { quiet: boolean }) => {
      calls += 1;
      quietValues.push(quiet);
      refreshStartedAt.push(Date.now());
      if (calls === 1) {
        await heldRefresh.promise;
        heldRefresh.markSettled();
      }
      if ([1, 2, 4].includes(calls)) throw new Error("temporary refresh failure");
    });
    const api = vi.fn(async () => sessionResponse());
    const controller = createSnapshotUpdates({
      api: api as unknown as (path: string, options: { method: "POST"; signal?: AbortSignal }) => Promise<unknown>,
      refresh,
      environment: { WebSocket: mockSockets.WebSocket as unknown as typeof WebSocket, random: () => 0.5 },
    });
    controller.start();
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    const socket = mockSockets.sockets[0]!;
    await vi.waitFor(() => expect(socket.readyState).toBe(MockWebSocket.OPEN));
    socket.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH });
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(1));

    const queuedManualRefresh = controller.refresh();
    const queuedManualOutcome = queuedManualRefresh.then(
      () => ({ error: null }),
      (error: unknown) => ({ error }),
    );
    let queuedManualSettled = false;
    void queuedManualRefresh.then(() => { queuedManualSettled = true; }, () => { queuedManualSettled = true; });
    heldRefresh.resolve();
    await heldRefresh.settled;
    const elapsedSinceFirstAttempt = Date.now() - refreshStartedAt[0]!;
    await vi.advanceTimersByTimeAsync(Math.max(0, 999 - elapsedSinceFirstAttempt));
    if (Date.now() - refreshStartedAt[0]! < 1_000) {
      expect(refresh).toHaveBeenCalledTimes(1);
      expect(queuedManualSettled).toBe(false);
    }
    await vi.advanceTimersByTimeAsync(10);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(2));
    expect(quietValues).toEqual([true, false]);
    expect(refreshStartedAt[1]! - refreshStartedAt[0]!).toBeGreaterThanOrEqual(1_000);
    const queuedManualResult = await queuedManualOutcome;
    expect(queuedManualResult.error).toBeInstanceOf(Error);
    expect((queuedManualResult.error as Error).message).toBe("temporary refresh failure");

    for (let index = 0; index < 12; index += 1) await Promise.resolve();
    const elapsedSinceSecondAttempt = Date.now() - refreshStartedAt[1]!;
    await vi.advanceTimersByTimeAsync(Math.max(0, 1_999 - elapsedSinceSecondAttempt));
    if (Date.now() - refreshStartedAt[1]! < 2_000) expect(refresh).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(10);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(3));
    expect(quietValues[2]).toBe(true);
    expect(refreshStartedAt[2]! - refreshStartedAt[1]!).toBeGreaterThanOrEqual(2_000);

    const explicitRefreshAfterSuccess = controller.refresh();
    const explicitRefreshOutcome = explicitRefreshAfterSuccess.then(
      () => ({ error: null }),
      (error: unknown) => ({ error }),
    );
    const elapsedSinceThirdAttempt = Date.now() - refreshStartedAt[2]!;
    await vi.advanceTimersByTimeAsync(Math.max(0, 999 - elapsedSinceThirdAttempt));
    if (Date.now() - refreshStartedAt[2]! < 1_000) expect(refresh).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(10);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(4));
    expect(quietValues[3]).toBe(false);
    expect(refreshStartedAt[3]! - refreshStartedAt[2]!).toBeGreaterThanOrEqual(1_000);
    const explicitRefreshResult = await explicitRefreshOutcome;
    expect(explicitRefreshResult.error).toBeInstanceOf(Error);
    expect((explicitRefreshResult.error as Error).message).toBe("temporary refresh failure");

    for (let index = 0; index < 12; index += 1) await Promise.resolve();
    const elapsedSinceFourthAttempt = Date.now() - refreshStartedAt[3]!;
    await vi.advanceTimersByTimeAsync(Math.max(0, 999 - elapsedSinceFourthAttempt));
    if (Date.now() - refreshStartedAt[3]! < 1_000) expect(refresh).toHaveBeenCalledTimes(4);
    await vi.advanceTimersByTimeAsync(10);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(5));
    expect(quietValues[4]).toBe(true);
    expect(refreshStartedAt[4]! - refreshStartedAt[3]!).toBeGreaterThanOrEqual(1_000);
    controller.stop();
  });

  it("clears failed-refresh retries on hidden lifecycle pauses and 4401 expiry", async () => {
    vi.useFakeTimers();
    const mockSockets = addMockWebSocket({ autoReady: true });
    const lifecycleHandlers = new Map<string, () => void>();
    const mutableDocumentMock = {
      visibilityState: "visible" as DocumentVisibilityState,
      hidden: false,
      addEventListener: (type: string, handler: () => void) => lifecycleHandlers.set(type, handler),
      removeEventListener: (type: string, handler: () => void) => {
        if (lifecycleHandlers.get(type) === handler) lifecycleHandlers.delete(type);
      },
    };
    const documentMock = mutableDocumentMock as unknown as Document;
    const api = vi.fn(async () => sessionResponse());
    let failuresRemaining = 1;
    const refresh = vi.fn(async () => {
      if (failuresRemaining > 0) {
        failuresRemaining -= 1;
        throw new Error("temporary refresh failure");
      }
    });
    const controller = createSnapshotUpdates({
      api: api as unknown as (path: string, options: { method: "POST"; signal?: AbortSignal }) => Promise<unknown>,
      refresh,
      environment: {
        WebSocket: mockSockets.WebSocket as unknown as typeof WebSocket,
        document: documentMock,
        navigator: { onLine: true } as Navigator,
        random: () => 0.5,
      },
    });
    controller.start();
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(1));
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(1));

    mutableDocumentMock.visibilityState = "hidden";
    mutableDocumentMock.hidden = true;
    lifecycleHandlers.get("visibilitychange")?.();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(refresh).toHaveBeenCalledTimes(1);

    mutableDocumentMock.visibilityState = "visible";
    mutableDocumentMock.hidden = false;
    lifecycleHandlers.get("visibilitychange")?.();
    await vi.waitFor(() => expect(mockSockets.sockets).toHaveLength(2));
    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(2));
    for (let index = 0; index < 12; index += 1) await Promise.resolve();

    failuresRemaining = 1;
    const resumedSocket = mockSockets.sockets[1]!;
    resumedSocket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "state" });
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(3));
    await vi.advanceTimersByTimeAsync(0);
    resumedSocket.serverClose(4401, "Owner authorization expired");
    await vi.advanceTimersByTimeAsync(60_000);
    expect(refresh).toHaveBeenCalledTimes(3);
    expect(api).toHaveBeenCalledTimes(2);
  });
});
