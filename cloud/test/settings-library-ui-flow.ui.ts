import { readFile } from "node:fs/promises";
import { webcrypto } from "node:crypto";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { makeChunkedSnapshot, TEST_DATABASE_EPOCH } from "./snapshot-loader-fixtures.js";
import { syntheticCredentials } from "./fixtures/synthetic-credentials.js";

const LIBRARY_ID = "main-library";
const ROOT_BINDING = `v1:${"a".repeat(64)}`;
const ACTION_QUEUE_STORAGE_KEY = "file-manager.actions.v1";
const MOCK_INIT_DATA = new URLSearchParams({ query_id: "settings-library-ui-flow", auth_date: "0" }).toString();
const UPDATE_PROTOCOL = "miniapp-updates-v1";
const MOCK_REPORTED_AT_MS = Date.now();

let originalWindowWebSocket: PropertyDescriptor | undefined;

class MockWebSocket extends EventTarget {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  readonly url: string;
  readonly protocols: string[];
  readonly protocol: string;
  readonly sent: Record<string, any>[] = [];
  readyState = MockWebSocket.CONNECTING;
  watch = false;
  currentStateVersion: () => number;
  currentDatabaseEpoch: () => string;

  constructor(
    url: string | URL,
    protocols: string[] | string | undefined,
    currentStateVersion: () => number,
    currentDatabaseEpoch: () => string,
  ) {
    super();
    this.url = String(url);
    this.protocols = Array.isArray(protocols) ? [...protocols] : protocols ? [protocols] : [];
    this.protocol = this.protocols[0] ?? "";
    this.currentStateVersion = currentStateVersion;
    this.currentDatabaseEpoch = currentDatabaseEpoch;
  }

  openAndReady() {
    this.readyState = MockWebSocket.OPEN;
    this.dispatchEvent(new Event("open"));
    this.receive({ type: "ready", database_epoch: this.currentDatabaseEpoch(), snapshot_control: "v1" });
  }

  send(data: string) {
    const message = JSON.parse(data) as Record<string, any>;
    this.sent.push(message);
    if (message.type === "interest") {
      this.watch = message.watch === true && message.visible === true;
      return;
    }
    if (message.type === "refresh_request") {
      queueMicrotask(() => this.receive({
        type: "refresh_result",
        request_id: message.request_id,
        database_epoch: this.currentDatabaseEpoch(),
        status: "published",
        state_version: this.currentStateVersion(),
        reported_at: MOCK_REPORTED_AT_MS,
      }));
    }
  }

  receive(message: Record<string, unknown>) {
    const event = new Event("message");
    Object.defineProperty(event, "data", { configurable: true, value: JSON.stringify(message) });
    this.dispatchEvent(event);
  }
}

function installMockWebSocket(
  currentStateVersion: () => number = () => 1,
  currentDatabaseEpoch: () => string = () => TEST_DATABASE_EPOCH,
) {
  const sockets: MockWebSocket[] = [];
  class AutoReadyWebSocket extends MockWebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols, currentStateVersion, currentDatabaseEpoch);
      sockets.push(this);
      queueMicrotask(() => this.openAndReady());
    }
  }
  originalWindowWebSocket ??= Object.getOwnPropertyDescriptor(window, "WebSocket");
  vi.stubGlobal("WebSocket", AutoReadyWebSocket as unknown as typeof WebSocket);
  Object.defineProperty(window, "WebSocket", { configurable: true, writable: true, value: AutoReadyWebSocket });
  return sockets;
}

function sentFramesOfType(socket: MockWebSocket, type: string) {
  return socket.sent.filter((frame) => frame.type === type);
}

function updateSession(databaseEpoch = TEST_DATABASE_EPOCH) {
  return {
    ticket: syntheticCredentials.localSharedSecret.value,
    websocket_path: "/api/updates/ws",
    protocol: UPDATE_PROTOCOL,
    expires_at: Date.now() + 60 * 60 * 1_000,
    database_epoch: databaseEpoch,
  };
}

const ownerSnapshot = {
  synced: true,
  stale: false,
  last_seen: Date.now(),
  settings: {
    download_dir: "/srv/media",
    applied: {
      default_library_id: LIBRARY_ID,
      libraries: [{
        id: LIBRARY_ID,
        name: "Main library",
        root: "/srv/media",
        root_binding: ROOT_BINDING,
        default_category: "Films",
        is_default: true,
      }],
    },
  },
  library: {
    default_library_id: LIBRARY_ID,
    revision: "compact-settings-revision",
    scanned_at: 1_800_000_000,
    root_label: "Main library",
    ready: true,
    items: [
      { id: "main-item-a", relative_path: "Films/A.mkv" },
      { id: "main-item-b", relative_path: "Shows/B.mkv" },
    ],
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
  pending_requests: [],
  tasks: [],
  operations: [],
  previews: [],
};

function element<T extends Element>(selector: string): T {
  const found = document.querySelector<T>(selector);
  if (!found) throw new Error(`Expected element ${selector}`);
  return found;
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
  const domWindow = window as Window & { happyDOM?: { cancelAsync(): void } };
  domWindow.happyDOM?.cancelAsync();
  document.body.innerHTML = "";
});

describe("settings library UI flow", () => {
  it("shows compact default snapshot counts and category, and still queues a settings action", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);
    let currentManifest = manifest;
    const sockets = installMockWebSocket(() => currentManifest.state_version ?? 0);
    let stateRequests = 0;
    let failFirstChunk = true;
    let failNextStateResponse = false;
    const responseChunks = new Map(chunks);

    const html = await readFile(resolve(process.cwd(), "public/settings/index.html"), "utf8");
    const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
    expect(body).toBeDefined();
    document.body.innerHTML = body!;
    expect(document.querySelector("script[src], link[rel='stylesheet']")).toBeNull();

    Object.defineProperty(window, "Telegram", {
      configurable: true,
      value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    const stored = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => stored.get(key) ?? null,
      setItem: (key: string, value: string) => stored.set(key, String(value)),
      removeItem: (key: string) => stored.delete(key),
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input) === "/api/updates/session") {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        expect(init?.method).toBe("POST");
        expect(init?.cache).toBe("no-store");
        return { ok: true, status: 200, json: async () => updateSession() } as Response;
      }
      if (String(input) === "/api/state") {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        expect(new Headers(init?.headers).get("X-Snapshot-Format")).toBe("chunked-v1");
        expect(init?.cache).toBe("no-store");
        stateRequests += 1;
        if (failNextStateResponse) {
          failNextStateResponse = false;
          return { ok: false, status: 503, json: async () => ({}) } as Response;
        }
        return { ok: true, status: 200, json: async () => currentManifest } as Response;
      }
      const chunkPath = String(input).match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
      if (chunkPath) {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        expect(new Headers(init?.headers).get("X-Cloud-Database-Epoch")).toBe(TEST_DATABASE_EPOCH);
        expect(init?.cache).toBe("no-store");
        const sha256 = chunkPath[1]!;
        if (failFirstChunk) {
          failFirstChunk = false;
          return { ok: false, status: 503, json: async () => ({}) } as Response;
        }
        return { ok: true, status: 200, json: async () => ({ sha256, data: responseChunks.get(sha256) }) } as Response;
      }
      if (String(input) === "/api/actions") {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        return { ok: true, status: 200, json: async () => ({ seq: 7 }) } as Response;
      }
      throw new Error(`Unexpected request ${String(input)}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    window.fetch = fetchMock as typeof window.fetch;

    vi.resetModules();
    // The browser entry has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
    await import("../public/settings/app.js");
    await vi.waitFor(() => expect(stateRequests).toBe(1));
    expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(true);
    expect(element<HTMLElement>("#auth-gate").classList.contains("hidden")).toBe(false);
    await vi.waitFor(() => expect(sentFramesOfType(sockets[0]!, "refresh_request")).toHaveLength(1));
    const openRequest = sentFramesOfType(sockets[0]!, "refresh_request")[0]!;
    expect(openRequest).toMatchObject({ reason: "open", scope: "settings" });
    expect(openRequest.request_id).toMatch(/^[0-9a-f-]{36}$/iu);
    await vi.waitFor(() => expect(stateRequests).toBe(2));
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    const formattedReportTime = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" })
      .format(new Date(MOCK_REPORTED_AT_MS));
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent)
      .toBe(`Desktop settings published at ${formattedReportTime}.`));
    expect(element<HTMLElement>("#sync-badge").textContent).toBe("Saved snapshot");
    expect(element<HTMLElement>("#sync-detail").textContent).toContain("Snapshot published Just now");
    expect(sockets).toHaveLength(1);
    expect(sockets[0]?.protocols).toEqual([UPDATE_PROTOCOL, syntheticCredentials.localSharedSecret.value]);

    await vi.advanceTimersByTimeAsync(5 * 60 * 1_000);
    expect(stateRequests).toBe(2);

    const card = element<HTMLElement>("#library-list .settings-library-card");
    expect(card.textContent).toContain("2 media items");
    expect(card.textContent).toContain("Default download category: Films");
    const formattedScanTime = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" })
      .format(new Date(Number(ownerSnapshot.library.scanned_at) * 1_000));
    expect(card.textContent).toContain(`Last scan: ${formattedScanTime}`);
    expect(element<HTMLInputElement>('[data-category-form="main-library"] input[name="default_category"]').value).toBe("Films");
    const scanButton = element<HTMLButtonElement>('[data-settings-action="library_scan"]');
    expect(scanButton.disabled).toBe(false);
    scanButton.click();

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/api/actions", expect.any(Object)));
    const actionRequest = fetchMock.mock.calls.find(([input]) => String(input) === "/api/actions");
    expect(actionRequest).toBeDefined();
    const action = JSON.parse(String(actionRequest?.[1]?.body));
    expect(action).toMatchObject({ kind: "library_scan", payload: { library_id: LIBRARY_ID } });
    await vi.waitFor(() => expect(JSON.parse(stored.get(ACTION_QUEUE_STORAGE_KEY) ?? "[]")).toEqual([]));
    await vi.waitFor(() => expect(sentFramesOfType(sockets[0]!, "interest").some((frame) => frame.watch === true)).toBe(true));
    expect(sentFramesOfType(sockets[0]!, "refresh_request")).toHaveLength(1);
    expect(fetchMock.mock.calls.filter(([input]) => String(input) === "/api/updates/session")).toHaveLength(1);
    expect(fetchMock.mock.calls.filter(([input]) => String(input) === "/api/state")).toHaveLength(2);
    expect(fetchMock.mock.calls.filter(([input]) => String(input).startsWith("/api/snapshot/chunks/"))).toHaveLength(
      new Set([...manifest.library.chunks, ...manifest.state.chunks].map((chunk: { sha256: string }) => chunk.sha256)).size * 2,
    );
    expect(fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(1);

    const pushedSnapshot = {
      ...ownerSnapshot,
      settings: {
        ...ownerSnapshot.settings,
        applied: {
          ...ownerSnapshot.settings.applied,
          libraries: ownerSnapshot.settings.applied.libraries.map((library) => ({ ...library, default_category: "Shows" })),
        },
      },
      state_version: 2,
    };
    const pushedBundle = await makeChunkedSnapshot(pushedSnapshot);
    currentManifest = pushedBundle.manifest;
    for (const [sha256, data] of pushedBundle.chunks) responseChunks.set(sha256, data);
    const statesBeforePush = stateRequests;
    sockets[0]?.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "inbox" });
    await vi.waitFor(() => expect(stateRequests).toBe(statesBeforePush + 1));
    await vi.waitFor(() => expect(element<HTMLInputElement>('[data-category-form="main-library"] input[name="default_category"]').value).toBe("Shows"));

    await vi.advanceTimersByTimeAsync(0);
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));
    const statesBeforeManualFailure = stateRequests;
    failNextStateResponse = true;
    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(sentFramesOfType(sockets[0]!, "refresh_request")).toHaveLength(2));
    expect(sentFramesOfType(sockets[0]!, "refresh_request")[1]).toMatchObject({ reason: "manual", scope: "settings" });
    await vi.waitFor(() => expect(stateRequests).toBe(statesBeforeManualFailure + 1));
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));
    await vi.waitFor(() => expect(element<HTMLElement>("#toast").textContent).toContain("The desktop published a snapshot, but settings could not reload it."));
    expect(element<HTMLInputElement>('[data-category-form="main-library"] input[name="default_category"]').value).toBe("Shows");
  });

  it("restores settings watch from cloud inbox and active operations after reload, then stops at terminal state", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const initialSnapshot = {
      ...ownerSnapshot,
      state_version: 1,
      pending_requests: [{ seq: 41, kind: "library_scan", status: "waiting_for_local", created_at: Math.floor(Date.now() / 1_000) }],
      operations: [{ seq: 40, kind: "library_rename", status: "processing", updated_at: Math.floor(Date.now() / 1_000) }],
    };
    const initialBundle = await makeChunkedSnapshot(initialSnapshot);
    let currentManifest = initialBundle.manifest;
    const responseChunks = new Map(initialBundle.chunks);
    const sockets = installMockWebSocket(() => currentManifest.state_version ?? 0);
    let stateRequests = 0;
    let actionRequests = 0;
    const stored = new Map<string, string>();

    const html = await readFile(resolve(process.cwd(), "public/settings/index.html"), "utf8");
    const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
    expect(body).toBeDefined();
    document.body.innerHTML = body!;
    Object.defineProperty(window, "Telegram", {
      configurable: true,
      value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => stored.get(key) ?? null,
      setItem: (key: string, value: string) => stored.set(key, String(value)),
      removeItem: (key: string) => stored.delete(key),
    });
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/api/updates/session") return { ok: true, status: 200, json: async () => updateSession() } as Response;
      if (path === "/api/state") {
        stateRequests += 1;
        return { ok: true, status: 200, json: async () => currentManifest } as Response;
      }
      const chunkPath = path.match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
      if (chunkPath) return { ok: true, status: 200, json: async () => ({ sha256: chunkPath[1], data: responseChunks.get(chunkPath[1]!) }) } as Response;
      if (path === "/api/actions") {
        actionRequests += 1;
        return { ok: true, status: 200, json: async () => ({ seq: 42 }) } as Response;
      }
      throw new Error(`Unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    window.fetch = fetchMock as typeof window.fetch;

    vi.resetModules();
    // @ts-expect-error The browser entry has no declarations because it is executed for its side effects.
    await import("../public/settings/app.js");
    await vi.waitFor(() => expect(stateRequests).toBe(2));
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("Desktop settings published"));
    const socket = sockets[0]!;
    await vi.waitFor(() => expect(sentFramesOfType(socket, "interest").some((frame) => frame.watch === true)).toBe(true));
    expect(stored.get(ACTION_QUEUE_STORAGE_KEY) ?? null).toBeNull();
    expect(actionRequests).toBe(0);
    expect(sentFramesOfType(socket, "refresh_request")).toHaveLength(1);

    const terminalBundle = await makeChunkedSnapshot({
      ...initialSnapshot,
      state_version: 2,
      pending_requests: [],
      operations: [
        { seq: 40, kind: "library_rename", status: "completed", updated_at: Math.floor(Date.now() / 1_000) },
        { seq: 41, kind: "library_scan", status: "completed", updated_at: Math.floor(Date.now() / 1_000) },
      ],
    });
    currentManifest = terminalBundle.manifest;
    for (const [sha256, data] of terminalBundle.chunks) responseChunks.set(sha256, data);
    socket.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "state" });
    await vi.waitFor(() => expect(stateRequests).toBe(3));
    await vi.waitFor(() => expect(sentFramesOfType(socket, "interest").at(-1)?.watch).toBe(false));
    expect(element<HTMLElement>("#operations-list").textContent).toContain("Completed");
    expect(sentFramesOfType(socket, "refresh_request")).toHaveLength(1);
    expect(actionRequests).toBe(0);
  });

  it("settles a hung published cache read and ignores its late private snapshot response", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const initialBundle = await makeChunkedSnapshot(ownerSnapshot);
    const lateSnapshot = {
      ...ownerSnapshot,
      state_version: 2,
      settings: {
        ...ownerSnapshot.settings,
        applied: {
          ...ownerSnapshot.settings.applied,
          libraries: ownerSnapshot.settings.applied.libraries.map((library) => ({ ...library, default_category: "Shows" })),
        },
      },
    };
    const lateBundle = await makeChunkedSnapshot(lateSnapshot);
    let currentManifest = initialBundle.manifest;
    const responseChunks = new Map([...initialBundle.chunks, ...lateBundle.chunks]);
    const sockets = installMockWebSocket(() => currentManifest.state_version ?? 0);
    let stateRequests = 0;
    let shouldHoldState = false;
    let stateGate: Promise<void> | null = null;
    let releaseStateGate: (() => void) | null = null;
    let heldReadSignal: AbortSignal | null = null;
    const currentHeldReadSignal = (): AbortSignal | null => heldReadSignal;
    const releaseHeldStateRead = () => releaseStateGate?.();

    const html = await readFile(resolve(process.cwd(), "public/settings/index.html"), "utf8");
    const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
    expect(body).toBeDefined();
    document.body.innerHTML = body!;
    Object.defineProperty(window, "Telegram", {
      configurable: true,
      value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    vi.stubGlobal("localStorage", { getItem: () => null, setItem: vi.fn(), removeItem: vi.fn() });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (path === "/api/updates/session") return { ok: true, status: 200, json: async () => updateSession() } as Response;
      if (path === "/api/state") {
        stateRequests += 1;
        if (shouldHoldState) {
          shouldHoldState = false;
          heldReadSignal = init?.signal ?? null;
          const gate = stateGate;
          if (gate) await gate;
        }
        return { ok: true, status: 200, json: async () => currentManifest } as Response;
      }
      const chunkPath = path.match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
      if (chunkPath) return { ok: true, status: 200, json: async () => ({ sha256: chunkPath[1], data: responseChunks.get(chunkPath[1]!) }) } as Response;
      throw new Error(`Unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    window.fetch = fetchMock as typeof window.fetch;

    vi.resetModules();
    // @ts-expect-error The browser entry has no declarations because it is executed for its side effects.
    await import("../public/settings/app.js");
    await vi.waitFor(() => expect(stateRequests).toBe(2));
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("Desktop settings published"));
    expect(element<HTMLInputElement>('[data-category-form="main-library"] input[name="default_category"]').value).toBe("Films");

    stateGate = new Promise<void>((resolve) => { releaseStateGate = resolve; });
    shouldHoldState = true;
    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(sentFramesOfType(sockets[0]!, "refresh_request")).toHaveLength(2));
    await vi.waitFor(() => expect(stateRequests).toBe(3));
    await vi.waitFor(() => expect(heldReadSignal).not.toBeNull());

    await vi.advanceTimersByTimeAsync(25_000);
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("could not be reloaded"));
    expect(element<HTMLButtonElement>("#refresh-button").disabled).toBe(false);
    expect(currentHeldReadSignal()?.aborted).toBe(true);
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));

    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(sentFramesOfType(sockets[0]!, "refresh_request")).toHaveLength(3));
    expect(sockets).toHaveLength(1);
    expect(sockets[0]!.readyState).toBe(MockWebSocket.OPEN);
    const secondManualRequest = sentFramesOfType(sockets[0]!, "refresh_request").at(-1)!;
    expect(secondManualRequest.request_id).toMatch(/^[0-9a-f-]{36}$/iu);
    sockets[0]!.receive({
      type: "refresh_result",
      request_id: secondManualRequest.request_id,
      database_epoch: TEST_DATABASE_EPOCH,
      status: "published",
      state_version: 1,
      reported_at: MOCK_REPORTED_AT_MS,
    });
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).not.toContain("Requesting current settings"));
    await vi.waitFor(() => expect(stateRequests).toBe(4));
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));

    currentManifest = lateBundle.manifest;
    releaseHeldStateRead();
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));
    expect(element<HTMLInputElement>('[data-category-form="main-library"] input[name="default_category"]').value).toBe("Films");
    expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("Desktop settings published at");
  });

  it("keeps settings private when a chunk does not match its manifest", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    installMockWebSocket();
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);

    const html = await readFile(resolve(process.cwd(), "public/settings/index.html"), "utf8");
    const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
    expect(body).toBeDefined();
    document.body.innerHTML = body!;
    Object.defineProperty(window, "Telegram", {
      configurable: true,
      value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    vi.stubGlobal("localStorage", {
      getItem: () => null,
      setItem: vi.fn(),
      removeItem: vi.fn(),
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      if (String(input) === "/api/updates/session") {
        return { ok: true, status: 200, json: async () => updateSession() } as Response;
      }
      if (String(input) === "/api/state") {
        return { ok: true, status: 200, json: async () => manifest } as Response;
      }
      const chunkPath = String(input).match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
      if (chunkPath) {
        const sha256 = chunkPath[1]!;
        return { ok: true, status: 200, json: async () => ({ sha256, data: `${chunks.get(sha256)} ` }) } as Response;
      }
      throw new Error(`Unexpected request ${String(input)}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    window.fetch = fetchMock as typeof window.fetch;

    vi.resetModules();
    // The browser entry has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
    await import("../public/settings/app.js");
    await vi.waitFor(() => expect(element<HTMLElement>("#auth-message").textContent).toContain("Private settings stay hidden"));

    expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(true);
    expect(element<HTMLElement>("#auth-gate").classList.contains("hidden")).toBe(false);
    expect(document.querySelector(".settings-library-card")).toBeNull();
    expect(fetchMock.mock.calls.some(([input]) => String(input).startsWith("/api/snapshot/chunks/"))).toBe(true);
  });

  it("keeps verified settings visible and stale after a snapshot-format upgrade response", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const sockets = installMockWebSocket();
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);

    const html = await readFile(resolve(process.cwd(), "public/settings/index.html"), "utf8");
    const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
    expect(body).toBeDefined();
    document.body.innerHTML = body!;
    Object.defineProperty(window, "Telegram", {
      configurable: true,
      value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    vi.stubGlobal("localStorage", {
      getItem: () => null,
      setItem: vi.fn(),
      removeItem: vi.fn(),
    });

    let stateRequests = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input) === "/api/updates/session") {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        expect(init?.method).toBe("POST");
        return { ok: true, status: 200, json: async () => updateSession() } as Response;
      }
      if (String(input) === "/api/state") {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        expect(new Headers(init?.headers).get("X-Snapshot-Format")).toBe("chunked-v1");
        expect(init?.cache).toBe("no-store");
        stateRequests += 1;
        if (stateRequests <= 2) return { ok: true, status: 200, json: async () => manifest } as Response;
        return {
          ok: false,
          status: 426,
          json: async () => ({
            error: "Mini App client upgrade required",
            required_format: "chunked-v1",
            library: { items: [], categories: [] },
            settings: {},
          }),
        } as Response;
      }
      const chunkPath = String(input).match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
      if (chunkPath) {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        expect(new Headers(init?.headers).get("X-Cloud-Database-Epoch")).toBe(TEST_DATABASE_EPOCH);
        expect(init?.cache).toBe("no-store");
        const sha256 = chunkPath[1]!;
        return { ok: true, status: 200, json: async () => ({ sha256, data: chunks.get(sha256) }) } as Response;
      }
      throw new Error(`Unexpected request ${String(input)}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    window.fetch = fetchMock as typeof window.fetch;

    vi.resetModules();
    // The browser entry has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
    await import("../public/settings/app.js");
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    await vi.waitFor(() => expect(stateRequests).toBe(2)); // Initial cache read, then the correlated open refresh.
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("Desktop settings published"));
    const card = element<HTMLElement>("#library-list .settings-library-card");
    expect(card.textContent).toContain("2 media items");
    expect(card.textContent).toContain("Default download category: Films");

    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(sentFramesOfType(sockets[0]!, "refresh_request")).toHaveLength(2));
    expect(sentFramesOfType(sockets[0]!, "refresh_request")[1]).toMatchObject({ reason: "manual", scope: "settings" });
    await vi.waitFor(() => expect(stateRequests).toBe(3));
    await vi.waitFor(() => expect(element<HTMLElement>("#sync-title").textContent).toBe("Showing the last saved settings snapshot"));

    expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false);
    expect(element<HTMLElement>("#sync-badge").textContent).toBe("Snapshot outdated");
    const staleCard = element<HTMLElement>("#library-list .settings-library-card");
    expect(staleCard.textContent).toContain("2 media items");
    expect(staleCard.textContent).toContain("Default download category: Films");
    expect(element<HTMLInputElement>('[data-category-form="main-library"] input[name="default_category"]').value).toBe("Films");
    expect(fetchMock.mock.calls.filter(([input]) => String(input) === "/api/state")).toHaveLength(3);
    expect(fetchMock.mock.calls.filter(([input]) => String(input).startsWith("/api/snapshot/chunks/"))).toHaveLength(
      new Set([...manifest.library.chunks, ...manifest.state.chunks].map((chunk: { sha256: string }) => chunk.sha256)).size,
    );
  });

  it("fences queued settings actions across database epoch changes and ignores a held old read", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const oldEpoch = TEST_DATABASE_EPOCH;
    const newEpoch = "c".repeat(32);
    const failedEpoch = "d".repeat(32);
    const snapshotWithName = (name: string, stateVersion: number) => ({
      ...ownerSnapshot,
      state_version: stateVersion,
      last_seen: Date.now(),
      settings: {
        ...ownerSnapshot.settings,
        applied: {
          ...ownerSnapshot.settings.applied,
          libraries: ownerSnapshot.settings.applied.libraries.map((library) => ({ ...library, name })),
        },
      },
      library: {
        ...ownerSnapshot.library,
        libraries: ownerSnapshot.library.libraries.map((library) => ({ ...library, name })),
      },
    });
    const oldBundle = await makeChunkedSnapshot(snapshotWithName("Old epoch library", 34_936), { databaseEpoch: oldEpoch });
    let currentManifest = oldBundle.manifest;
    let currentDatabaseEpoch = oldEpoch;
    let sessionStatus = 200;
    let sessionRequests = 0;
    let stateRequests = 0;
    const stateRequestEpochs: string[] = [];
    const stateRequestVersions: number[] = [];
    const responseChunks = new Map(oldBundle.chunks);
    const chunkEpochs = new Map<string, Set<string>>();
    const registerBundle = (bundle: Awaited<ReturnType<typeof makeChunkedSnapshot>>) => {
      for (const [sha256, data] of bundle.chunks) {
        responseChunks.set(sha256, data);
        const epochs = chunkEpochs.get(sha256) ?? new Set<string>();
        epochs.add(bundle.manifest.database_epoch);
        chunkEpochs.set(sha256, epochs);
      }
    };
    registerBundle(oldBundle);
    const stateReadGates: Array<{ promise: Promise<void>; release: () => void }> = [];
    const holdNextStateResponse = () => {
      let release!: () => void;
      const promise = new Promise<void>((resolve) => { release = resolve; });
      stateReadGates.push({ promise, release });
      return release;
    };
    const publishSnapshot = async (snapshot: Record<string, any>, databaseEpoch: string) => {
      const bundle = await makeChunkedSnapshot(snapshot, { databaseEpoch });
      currentManifest = bundle.manifest;
      currentDatabaseEpoch = databaseEpoch;
      registerBundle(bundle);
    };
    const sockets = installMockWebSocket(() => currentManifest.state_version ?? 0, () => currentDatabaseEpoch);
    const stored = new Map<string, string>();
    const actions: Array<Record<string, any>> = [];
    const actionSawVerifiedEpoch: boolean[] = [];
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
      expect(init?.cache).toBe("no-store");
      if (String(input) === "/api/updates/session") {
        expect(init?.method).toBe("POST");
        sessionRequests += 1;
        return {
          ok: sessionStatus >= 200 && sessionStatus < 300,
          status: sessionStatus,
          json: async () => sessionStatus >= 200 && sessionStatus < 300
            ? updateSession(currentDatabaseEpoch)
            : { error: "Update session unavailable" },
        } as Response;
      }
      if (String(input) === "/api/state") {
        expect(new Headers(init?.headers).get("X-Snapshot-Format")).toBe("chunked-v1");
        stateRequests += 1;
        const manifest = currentManifest;
        stateRequestEpochs.push(manifest.database_epoch);
        stateRequestVersions.push(manifest.state_version);
        const gate = stateReadGates.shift();
        if (gate) await gate.promise;
        return { ok: true, status: 200, json: async () => manifest } as Response;
      }
      const chunkPath = String(input).match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
      if (chunkPath) {
        const sha256 = chunkPath[1]!;
        const requestedEpoch = new Headers(init?.headers).get("X-Cloud-Database-Epoch");
        expect(requestedEpoch).toBeTruthy();
        expect(chunkEpochs.get(sha256)?.has(requestedEpoch!)).toBe(true);
        return { ok: true, status: 200, json: async () => ({ sha256, data: responseChunks.get(sha256) }) } as Response;
      }
      if (String(input) === "/api/actions") {
        const action = JSON.parse(String(init?.body)) as Record<string, any>;
        actions.push(action);
        const expectedName = actions.length === 1 ? "New epoch library" : "Latest epoch library";
        actionSawVerifiedEpoch.push(element<HTMLElement>("#library-list").textContent?.includes(expectedName) === true);
        return {
          ok: true,
          status: 200,
          json: async () => ({ seq: 7 + actions.length, request_id: action.request_id }),
        } as Response;
      }
      throw new Error(`Unexpected request ${String(input)}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    window.fetch = fetchMock as typeof window.fetch;
    Object.defineProperty(window, "Telegram", {
      configurable: true,
      value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => stored.get(key) ?? null,
      setItem: (key: string, value: string) => stored.set(key, String(value)),
      removeItem: (key: string) => stored.delete(key),
    });
    const html = await readFile(resolve(process.cwd(), "public/settings/index.html"), "utf8");
    const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
    expect(body).toBeDefined();
    document.body.innerHTML = body!;

    vi.resetModules();
    // The browser entry has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
    await import("../public/settings/app.js");
    await vi.waitFor(() => expect(stateRequests).toBe(2));
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("Desktop settings published"));
    expect(element<HTMLElement>("#library-list").textContent).toContain("Old epoch library");
    expect(stateRequestEpochs.slice(0, 2)).toEqual([oldEpoch, oldEpoch]);
    expect(stateRequestVersions.slice(0, 2)).toEqual([34_936, 34_936]);

    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: false });
    element<HTMLButtonElement>('[data-settings-action="library_scan"]').click();
    await vi.waitFor(() => expect(JSON.parse(stored.get(ACTION_QUEUE_STORAGE_KEY) ?? "[]")).toHaveLength(1));
    expect(actions).toHaveLength(0);
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });

    const oldSocket = sockets[0]!;
    const beforeHeldRead = stateRequests;
    const releaseOldRead = holdNextStateResponse();
    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(sentFramesOfType(oldSocket, "refresh_request")).toHaveLength(2));
    await vi.waitFor(() => expect(stateRequests).toBe(beforeHeldRead + 1));

    const releaseNewEpochRead = holdNextStateResponse();
    currentDatabaseEpoch = newEpoch;
    await publishSnapshot(snapshotWithName("New epoch library", 1), newEpoch);
    oldSocket.receive({ type: "invalidate", database_epoch: newEpoch, reason: "epoch" });
    releaseOldRead();
    await vi.waitFor(() => expect(stateRequests).toBe(beforeHeldRead + 2));
    expect(stateRequestEpochs.slice(-2)).toEqual([oldEpoch, newEpoch]);
    expect(stateRequestVersions.slice(-2)).toEqual([34_936, 1]);
    await vi.waitFor(() => expect(element<HTMLElement>("#sync-badge").textContent).toBe("Snapshot outdated"));
    expect(element<HTMLElement>("#library-list").textContent).toContain("Old epoch library");
    expect(JSON.parse(stored.get(ACTION_QUEUE_STORAGE_KEY) ?? "[]")).toHaveLength(1);
    expect(actions).toHaveLength(0);

    releaseNewEpochRead();
    await vi.waitFor(() => expect(element<HTMLElement>("#library-list").textContent).toContain("New epoch library"));
    await vi.waitFor(() => expect(actions).toHaveLength(1));
    await vi.waitFor(() => expect(JSON.parse(stored.get(ACTION_QUEUE_STORAGE_KEY) ?? "[]")).toEqual([]));
    const firstActionId = actions[0]!.request_id;
    expect(actionSawVerifiedEpoch).toEqual([true]);
    expect(stateRequestEpochs).toContain(newEpoch);
    expect(stateRequestVersions).toContain(1);
    await vi.waitFor(() => expect(sockets.length).toBeGreaterThan(1));
    await vi.waitFor(() => expect(sentFramesOfType(sockets.at(-1)!, "interest").length).toBeGreaterThan(0));

    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: false });
    element<HTMLButtonElement>('[data-settings-action="library_scan"]').click();
    await vi.waitFor(() => expect(JSON.parse(stored.get(ACTION_QUEUE_STORAGE_KEY) ?? "[]")).toHaveLength(1));
    const secondActionId = JSON.parse(stored.get(ACTION_QUEUE_STORAGE_KEY)!)[0].request_id;
    expect(secondActionId).not.toBe(firstActionId);
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    const newEpochSocket = sockets.at(-1)!;
    const beforeFailedRead = stateRequests;
    sessionStatus = 503;
    currentDatabaseEpoch = failedEpoch;
    newEpochSocket.receive({ type: "invalidate", database_epoch: failedEpoch, reason: "epoch" });
    await vi.waitFor(() => expect(stateRequests).toBe(beforeFailedRead + 1));
    await vi.waitFor(() => expect(element<HTMLElement>("#sync-badge").textContent).toBe("Snapshot outdated"));
    expect(element<HTMLElement>("#library-list").textContent).toContain("New epoch library");
    expect(JSON.parse(stored.get(ACTION_QUEUE_STORAGE_KEY) ?? "[]")).toHaveLength(1);
    expect(actions).toHaveLength(1);

    await publishSnapshot(snapshotWithName("Latest epoch library", 1), failedEpoch);
    sessionStatus = 200;
    window.dispatchEvent(new Event("online"));
    await vi.waitFor(() => expect(element<HTMLElement>("#library-list").textContent).toContain("Latest epoch library"));
    await vi.waitFor(() => expect(actions).toHaveLength(2));
    await vi.waitFor(() => expect(JSON.parse(stored.get(ACTION_QUEUE_STORAGE_KEY) ?? "[]")).toEqual([]));
    await vi.advanceTimersByTimeAsync(0);
    expect(actions.map((action) => action.request_id)).toEqual([firstActionId, secondActionId]);
    expect(actionSawVerifiedEpoch).toEqual([true, true]);
    expect(stateRequestEpochs.slice(-2)).toEqual([newEpoch, failedEpoch]);
    expect(stateRequestVersions.slice(-2)).toEqual([1, 1]);
    expect(sessionRequests).toBeGreaterThan(2);
  });
});
