import { readFile } from "node:fs/promises";
import { webcrypto } from "node:crypto";
import { resolve } from "node:path";
import { Event as HappyDOMEvent } from "happy-dom";
import type { HTMLSelectElement as HappyDOMSelectElement } from "happy-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { makeChunkedSnapshot, TEST_DATABASE_EPOCH } from "./snapshot-loader-fixtures.js";
import { syntheticCredentials } from "./fixtures/synthetic-credentials.js";

const LIBRARY_ID = "test-library";
const ROOT_BINDING = `v1:${"a".repeat(64)}`;
const MOCK_INIT_DATA = new URLSearchParams({ query_id: "file-manager-ui-flow", auth_date: "0" }).toString();
const UPDATE_PROTOCOL = "miniapp-updates-v1";

let originalWindowWebSocket: PropertyDescriptor | undefined;

class MockWebSocket extends EventTarget {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  readonly url: string;
  readonly protocols: string[];
  readonly protocol: string;
  readyState = MockWebSocket.CONNECTING;

  constructor(url: string | URL, protocols?: string | string[]) {
    super();
    this.url = String(url);
    this.protocols = Array.isArray(protocols) ? [...protocols] : protocols ? [protocols] : [];
    this.protocol = this.protocols[0] ?? "";
  }

  openAndReady() {
    this.readyState = MockWebSocket.OPEN;
    this.emit("open");
    this.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH });
  }

  receive(message: Record<string, unknown>) {
    const event = new Event("message");
    Object.defineProperty(event, "data", { configurable: true, value: JSON.stringify(message) });
    this.dispatchEvent(event);
  }

  private emit(type: string) {
    this.dispatchEvent(new Event(type));
  }
}

function installMockWebSocket() {
  const sockets: MockWebSocket[] = [];
  class AutoReadyWebSocket extends MockWebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols);
      sockets.push(this);
      queueMicrotask(() => this.openAndReady());
    }
  }
  originalWindowWebSocket ??= Object.getOwnPropertyDescriptor(window, "WebSocket");
  vi.stubGlobal("WebSocket", AutoReadyWebSocket as unknown as typeof WebSocket);
  Object.defineProperty(window, "WebSocket", { configurable: true, writable: true, value: AutoReadyWebSocket });
  return sockets;
}

function updateSession() {
  return {
    ticket: syntheticCredentials.localSharedSecret.value,
    websocket_path: "/api/updates/ws",
    protocol: UPDATE_PROTOCOL,
    expires_at: Date.now() + 60 * 60 * 1_000,
    database_epoch: TEST_DATABASE_EPOCH,
  };
}

const ownerSnapshot = {
  synced: true,
  stale: false,
  last_seen: Math.floor(Date.now() / 1000),
  settings: {
    download_dir: "/srv/media",
    applied: {
      default_library_id: LIBRARY_ID,
      libraries: [{
        id: LIBRARY_ID,
        name: "Test library",
        root: "/srv/media",
        root_binding: ROOT_BINDING,
        default_category: "Films",
        availability: "available",
        available: true,
        is_default: true,
      }],
    },
  },
  library: {
    revision: "compact-ui-revision",
    scanned_at: Math.floor(Date.now() / 1000),
    root_label: "Test library",
    ready: true,
    default_library_id: LIBRARY_ID,
    items: [
      { id: "item-alpha", title: "Alpha video", relative_path: "Films/Alpha.mkv", presence: "present", bytes: 1024 },
      { id: "item-beta", title: "Beta video", relative_path: "Films/Beta.mkv", presence: "present", bytes: 2048 },
      { id: "series-a-1", title: "Shared episode 1", relative_path: "Series/A1.mkv", presence: "present", collection: { id: "series-a", title: "Shared Series", kind: "series", order: 1 } },
      { id: "series-a-2", title: "Shared episode 2", relative_path: "Series/A2.mkv", presence: "present", collection: { id: "series-a", title: "Shared Series renamed", kind: "series", order: 2 } },
      { id: "series-b-1", title: "Shared episode 3", relative_path: "Series/B1.mkv", presence: "present", collection: { id: "series-b", title: "Shared Series", kind: "series", order: 1 } },
    ],
    categories: ["Films", "Series", "Shows"],
    warnings: [],
    libraries: [{ id: LIBRARY_ID, availability: "available", available: true, is_default: true }],
  },
  pending_requests: [],
  tasks: [],
  operations: [],
  previews: [],
};

function element<T>(selector: string): T {
  const found = document.querySelector(selector);
  if (!found) throw new Error(`Expected element ${selector}`);
  return found as unknown as T;
}

type MemoryStorageFixture = {
  getItem: (key: string) => string | null;
  setItem: (key: string, value: string) => void;
  removeItem: (key: string) => void;
  clear: () => void;
};

function makeMemoryStorage(): MemoryStorageFixture {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => { values.set(key, String(value)); },
    removeItem: (key: string) => { values.delete(key); },
    clear: () => { values.clear(); },
  };
}

function filePreviewFixture(seq: number, libraryId: string, rootBinding: string, id: string, revision: string) {
  return {
    seq,
    request_id: `file-preview-${id}`,
    kind: "file_preview",
    status: "awaiting_confirmation",
    library_id: libraryId,
    root_binding: rootBinding,
    recovery_identity: { library_id: libraryId, root_binding: rootBinding },
    result: {
      library_id: libraryId,
      root_binding: rootBinding,
      preview_id: id,
      revision,
      preview: {
        target_relative_dir: "Reviewed",
        conflict: "skip",
        items: [{ item_id: "item-alpha", source_path: "Source.mkv", target_path: "Reviewed/Source.mkv" }],
      },
    },
  };
}

async function mountFileManagerFixture(
  initialSnapshot: Record<string, any>,
  storage: MemoryStorageFixture,
  onAction?: (
    action: Record<string, any>,
    seq: number,
    publishSnapshot: (snapshot: Record<string, any>) => Promise<void>,
  ) => Promise<void> | void,
) {
  const sockets = installMockWebSocket();
  let currentManifest: Record<string, any>;
  let stateRequests = 0;
  let nextActionSeq = 901;
  const actions: Array<Record<string, any>> = [];
  let currentSnapshot = initialSnapshot;
  const initialBundle = await makeChunkedSnapshot(initialSnapshot);
  currentManifest = initialBundle.manifest;
  const responseChunks = new Map(initialBundle.chunks);

  async function publishSnapshot(snapshot: Record<string, any>) {
    currentSnapshot = snapshot;
    const bundle = await makeChunkedSnapshot(snapshot);
    currentManifest = bundle.manifest;
    for (const [sha256, data] of bundle.chunks) responseChunks.set(sha256, data);
  }

  const html = await readFile(resolve(process.cwd(), "public/file/index.html"), "utf8");
  const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
  expect(body).toBeDefined();
  document.body.replaceWith(document.createElement("body"));
  document.body.innerHTML = body!;
  expect(document.querySelector("script[src], link[rel='stylesheet']")).toBeNull();
  Object.defineProperty(window, "Telegram", {
    configurable: true,
    value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
  });
  Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
  vi.stubGlobal("localStorage", storage);
  Object.defineProperty(window, "matchMedia", { configurable: true, value: vi.fn(() => ({ matches: true })) });

  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
    expect(init?.cache).toBe("no-store");
    if (String(input) === "/api/updates/session") {
      expect(init?.method).toBe("POST");
      return { ok: true, status: 200, json: async () => updateSession() } as Response;
    }
    if (String(input) === "/api/state") {
      expect(new Headers(init?.headers).get("X-Snapshot-Format")).toBe("chunked-v1");
      stateRequests += 1;
      return { ok: true, status: 200, json: async () => currentManifest } as Response;
    }
    const chunkPath = String(input).match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
    if (chunkPath) {
      expect(new Headers(init?.headers).get("X-Cloud-Database-Epoch")).toBe(TEST_DATABASE_EPOCH);
      const sha256 = chunkPath[1]!;
      return { ok: true, status: 200, json: async () => ({ sha256, data: responseChunks.get(sha256) }) } as Response;
    }
    if (String(input) === "/api/actions") {
      const action = JSON.parse(String(init?.body)) as Record<string, any>;
      actions.push(action);
      const seq = nextActionSeq++;
      await onAction?.(action, seq, publishSnapshot);
      return { ok: true, status: 202, json: async () => ({ seq, request_id: action.request_id }) } as Response;
    }
    throw new Error(`Unexpected request ${String(input)}`);
  });
  vi.stubGlobal("fetch", fetchMock);
  window.fetch = fetchMock as typeof window.fetch;

  vi.resetModules();
  // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
  await import("../public/file/app.js");
  await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(0));
  await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));

  return {
    actions,
    sockets,
    fetchMock,
    publishSnapshot,
    get stateRequests() { return stateRequests; },
    get currentSnapshot() { return currentSnapshot; },
  };
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
  const domWindow = window as Window & { happyDOM?: { cancelAsync(): void } };
  domWindow.happyDOM?.cancelAsync();
    document.body.replaceWith(document.createElement("body"));
});

describe("file manager UI flow", () => {
  it("shows and clears selected items, navigates to the move card, and exposes mobile Details", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);
    const sockets = installMockWebSocket();
    let currentManifest = manifest;
    let stateRequests = 0;
    let failNextStateResponse = false;
    const responseChunks = new Map(chunks);

    const html = await readFile(resolve(process.cwd(), "public/file/index.html"), "utf8");
    const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
    expect(body).toBeDefined();
    document.body.replaceWith(document.createElement("body"));
    document.body.innerHTML = body!;
    expect(document.querySelector("script[src], link[rel='stylesheet']")).toBeNull();

    Object.defineProperty(window, "Telegram", {
      configurable: true,
      value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    vi.stubGlobal("localStorage", window.localStorage);
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: vi.fn(() => ({ matches: true })),
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
      expect(init?.cache).toBe("no-store");
      if (String(input) === "/api/updates/session") {
        expect(init?.method).toBe("POST");
        return { ok: true, status: 200, json: async () => updateSession() } as Response;
      }
      if (String(input) === "/api/state") {
        expect(new Headers(init?.headers).get("X-Snapshot-Format")).toBe("chunked-v1");
        stateRequests += 1;
        if (stateRequests === 1 || failNextStateResponse) {
          failNextStateResponse = false;
          return { ok: false, status: 503, json: async () => ({}) } as Response;
        }
        return { ok: true, status: 200, json: async () => currentManifest } as Response;
      }
      const chunkPath = String(input).match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
      if (chunkPath) {
        expect(new Headers(init?.headers).get("X-Cloud-Database-Epoch")).toBe(TEST_DATABASE_EPOCH);
        const sha256 = chunkPath[1]!;
        return { ok: true, status: 200, json: async () => ({ sha256, data: responseChunks.get(sha256) }) } as Response;
      }
      throw new Error(`Unexpected request ${String(input)}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    window.fetch = fetchMock as typeof window.fetch;

    const firstReadAt = Date.now();
    // The browser entry has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
    await import("../public/file/app.js");
    await vi.waitFor(() => expect(stateRequests).toBe(1));
    expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(true);
    expect(element<HTMLElement>("#auth-gate").classList.contains("hidden")).toBe(false);
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.waitFor(() => expect(stateRequests).toBe(2));
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    expect(Date.now() - firstReadAt).toBeLessThan(5 * 60 * 1_000);
    expect(sockets).toHaveLength(1);
    expect(sockets[0]?.protocols).toEqual([UPDATE_PROTOCOL, syntheticCredentials.localSharedSecret.value]);
    expect(fetchMock).toHaveBeenCalledTimes(3 + manifest.library.chunks.length + manifest.state.chunks.length);
    expect((element<HTMLElement>("#library-select") as unknown as { value: string }).value).toBe(LIBRARY_ID);
    expect(element<HTMLOptionElement>("#library-select option").disabled).toBe(false);
    expect([...document.querySelectorAll<HTMLOptionElement>("#category-folders option")].map((option) => option.value)).toEqual(expect.arrayContaining(["Films", "Series", "Shows"]));

    expect(element<HTMLButtonElement>("#select-visible").textContent?.trim()).toBe("Select this page");
    element<HTMLButtonElement>("#download-here").click();
    expect((element<HTMLElement>("#download-library-select") as unknown as { value: string }).value).toBe(LIBRARY_ID);
    expect(element<HTMLInputElement>("#download-category").value).toBe("Films");
    expect([...document.querySelectorAll<HTMLOptionElement>("#download-categories option")].map((option) => option.value)).toEqual(expect.arrayContaining(["Films", "Series", "Shows"]));
    expect(element<HTMLButtonElement>("#download-submit").disabled).toBe(false);
    element<HTMLButtonElement>("#download-cancel").click();
    const detailsButton = element<HTMLButtonElement>('[data-details="item-alpha"]');
    expect(detailsButton.classList.contains("media-details")).toBe(true);
    expect(detailsButton.closest(".media-actions")).not.toBeNull();
    expect(detailsButton.closest(".media-meta")).toBeNull();
    detailsButton.click();
    expect(element<HTMLElement>(".detail-sheet").getAttribute("aria-label")).toBe("File details");
    element<HTMLButtonElement>(".detail-panel .detail-close").click();
    expect(document.querySelector(".detail-panel")).toBeNull();

    const sameTitleGroupChecks = [...document.querySelectorAll<HTMLInputElement>("#library-list [data-select-group]")]
      .filter((checkbox) => checkbox.closest("summary")?.querySelector("span")?.textContent === "Shared Series");
    expect(sameTitleGroupChecks).toHaveLength(2);
    const firstSameTitleGroupCheck = sameTitleGroupChecks[0];
    if (!firstSameTitleGroupCheck) throw new Error("Expected a group checkbox for the first collection identity");
    firstSameTitleGroupCheck.click();
    expect([...document.querySelectorAll<HTMLInputElement>("#library-list [data-select-item]:checked")].map((checkbox) => checkbox.dataset.selectItem).sort()).toEqual(["series-a-1", "series-a-2"]);
    element<HTMLButtonElement>("#clear-selection").click();
    expect(document.querySelectorAll<HTMLInputElement>("[data-select-item]:checked")).toHaveLength(0);

    const tray = element<HTMLElement>("#selection-actions");
    expect(tray.classList.contains("hidden")).toBe(true);
    element<HTMLInputElement>('[data-select-item="item-alpha"]').click();
    await vi.waitFor(() => expect(tray.classList.contains("hidden")).toBe(false));
    expect(element<HTMLElement>("#selection-count").textContent).toBe("1 item selected");

    element<HTMLButtonElement>("#select-visible").click();
    expect(element<HTMLElement>("#selection-count").textContent).toBe("5 items selected");
    const search = element<HTMLInputElement>("#search-input");
    search.value = "Beta";
    search.dispatchEvent(new Event("input", { bubbles: true }));
    expect(element<HTMLElement>("#selection-count").textContent).toBe("5 items selected");

    const pushedSnapshot = {
      ...ownerSnapshot,
      state_version: 2,
      library: {
        ...ownerSnapshot.library,
        revision: "pushed-ui-revision",
        items: ownerSnapshot.library.items.map((item) => item.id === "item-beta"
          ? { ...item, title: "Beta from pushed update" }
          : item),
      },
    };
    const pushedBundle = await makeChunkedSnapshot(pushedSnapshot);
    currentManifest = pushedBundle.manifest;
    for (const [sha256, data] of pushedBundle.chunks) responseChunks.set(sha256, data);
    const stateRequestsBeforePush = stateRequests;
    await vi.advanceTimersByTimeAsync(1_000);
    sockets[0]?.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "state" });
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(stateRequestsBeforePush));
    await vi.waitFor(() => expect(element<HTMLElement>("#library-list").textContent).toContain("Beta from pushed update"));

    failNextStateResponse = true;
    const statesBeforeManualFailure = stateRequests;
    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(stateRequests).toBe(statesBeforeManualFailure + 1));
    await vi.waitFor(() => expect(element<HTMLElement>("#toast").textContent).toContain("Request failed (503)"));
    await vi.waitFor(() => expect(element<HTMLElement>("#selection-count").textContent).toBe("5 items selected"));

    element<HTMLButtonElement>("#clear-selection").click();
    expect(tray.classList.contains("hidden")).toBe(true);
    expect(document.querySelectorAll<HTMLInputElement>("[data-select-item]:checked")).toHaveLength(0);

    search.value = "";
    search.dispatchEvent(new Event("input", { bubbles: true }));
    element<HTMLInputElement>('[data-select-item="item-alpha"]').click();
    expect(element<HTMLElement>("#selection-count").textContent).toBe("1 item selected");

    const moveCard = element<HTMLElement>("#move-files-card");
    const scrollIntoView = vi.fn();
    moveCard.scrollIntoView = scrollIntoView;
    element<HTMLButtonElement>("#organize-selected").click();
    expect(element<HTMLElement>("#view-library").classList.contains("hidden")).toBe(true);
    expect(element<HTMLElement>("#view-organizing").classList.contains("hidden")).toBe(false);
    expect(document.activeElement).toBe(moveCard);
    expect(scrollIntoView).toHaveBeenCalledWith(expect.objectContaining({ block: "start" }));

    const css = await readFile(resolve(process.cwd(), "public/file/styles.css"), "utf8");
    expect(css).toMatch(/-webkit-text-size-adjust:\s*100%/u);
    expect(css).toMatch(/(?:^|[;{])\s*text-size-adjust:\s*100%/u);
    expect(css).toContain("env(safe-area-inset-bottom");
    expect(css).toMatch(/\.media-details\s*\{[^}]*min-height:\s*36px/su);
  });

  it("keeps the verified library visible and stale after a snapshot-format upgrade response", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    installMockWebSocket();
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);

    const html = await readFile(resolve(process.cwd(), "public/file/index.html"), "utf8");
    const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
    expect(body).toBeDefined();
    document.body.innerHTML = body!;
    Object.defineProperty(window, "Telegram", {
      configurable: true,
      value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    vi.stubGlobal("localStorage", window.localStorage);

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
        if (stateRequests === 1) return { ok: true, status: 200, json: async () => manifest } as Response;
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
        expect(new Headers(init?.headers).get("X-Cloud-Database-Epoch")).toBe(TEST_DATABASE_EPOCH);
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
    await import("../public/file/app.js");
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    expect(element<HTMLButtonElement>('[data-details="item-alpha"]')).toBeDefined();

    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(stateRequests).toBe(2));
    await vi.waitFor(() => expect(element<HTMLElement>("#sync-title").textContent).toBe("No recent computer contact"));

    expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false);
    expect(element<HTMLButtonElement>('[data-details="item-alpha"]')).toBeDefined();
    expect([...document.querySelectorAll<HTMLOptionElement>("#category-folders option")].map((option) => option.value)).toEqual(expect.arrayContaining(["Films", "Series", "Shows"]));
    expect(fetchMock.mock.calls.filter(([input]) => String(input) === "/api/state")).toHaveLength(2);
    expect(fetchMock.mock.calls.filter(([input]) => String(input).startsWith("/api/snapshot/chunks/"))).toHaveLength(
      new Set([...manifest.library.chunks, ...manifest.state.chunks].map((chunk: { sha256: string }) => chunk.sha256)).size,
    );
  });

  it("keeps review previews in Requests until terminal snapshots move them into searchable History", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const sockets = installMockWebSocket();
    const storage = makeMemoryStorage();

    const secondaryLibraryId = "secondary-library";
    const secondaryRootBinding = `v1:${"b".repeat(64)}`;
    const primaryDescriptor = {
      id: LIBRARY_ID,
      name: "Primary library",
      root: "/srv/media",
      root_binding: ROOT_BINDING,
      default_category: "Films",
      availability: "available",
      available: true,
      is_default: true,
    };
    const secondaryDescriptor = {
      id: secondaryLibraryId,
      name: "Secondary library",
      root: "/srv/secondary",
      root_binding: secondaryRootBinding,
      default_category: "Series",
      availability: "available",
      available: true,
    };
    const primaryScan = { ...ownerSnapshot.library, libraries: [], migrations: [] };
    const secondaryScan = {
      revision: "secondary-review-r1",
      scanned_at: ownerSnapshot.library.scanned_at,
      root_label: "Secondary library",
      ready: true,
      default_library_id: LIBRARY_ID,
      items: [{ id: "secondary-item", title: "Secondary video", relative_path: "Series/Secondary.mkv", presence: "present", bytes: 512 }],
      categories: ["Series"],
      warnings: [],
      libraries: [{ id: secondaryLibraryId, availability: "available", available: true }],
    };

    function filePreview(seq: number, libraryId: string, rootBinding: string, id: string, revision: string) {
      return {
        seq,
        request_id: `file-preview-${id}`,
        kind: "file_preview",
        status: "awaiting_confirmation",
        library_id: libraryId,
        root_binding: rootBinding,
        recovery_identity: { library_id: libraryId, root_binding: rootBinding },
        result: {
          library_id: libraryId,
          root_binding: rootBinding,
          preview_id: id,
          revision,
          preview: {
            target_relative_dir: "Reviewed",
            conflict: "skip",
            items: [{ item_id: libraryId === LIBRARY_ID ? "item-alpha" : "secondary-item", source_path: "Source.mkv", target_path: "Reviewed/Source.mkv" }],
          },
        },
      };
    }

    function migrationPreview(seq: number, id: string, revision: string) {
      return {
        seq,
        request_id: `migration-preview-${id}`,
        kind: "library_migration_preview",
        status: "awaiting_confirmation",
        result: {
          id,
          revision,
          source_library_id: LIBRARY_ID,
          target_library_id: secondaryLibraryId,
          source_root_binding: ROOT_BINDING,
          target_root_binding: secondaryRootBinding,
          scope: "library",
          conflict: "skip",
          include_incomplete_tasks: false,
          set_target_default_on_success: false,
          items: [{ item_id: "item-alpha", source_path: "Films/Alpha.mkv", target_path: "Films/Alpha.mkv", outcome: "move" }],
          item_count: 1,
          file_count: 1,
          total_bytes: 1024,
          required_space_bytes: 0,
          available_space_bytes: 4096,
          conflict_count: 0,
          incomplete_task_count: 0,
        },
      };
    }

    function libraryMigrations() {
      return [
        {
          id: "migration-history",
          source_library_id: LIBRARY_ID,
          target_library_id: secondaryLibraryId,
          status: "completed",
          total_items: 1,
          completed_items: 1,
          total_files: 1,
          completed_files: 1,
          copied_bytes: 1024,
          total_bytes: 1024,
          success_count: 1,
          skipped_count: 0,
          failed_count: 0,
        },
        {
          id: "migration-finalization",
          source_library_id: LIBRARY_ID,
          target_library_id: secondaryLibraryId,
          status: "completed",
          terminal_finalization_pending: true,
          warnings: ["Migration needs attention: terminal finalization is pending."],
          total_items: 1,
          completed_items: 1,
          total_files: 1,
          completed_files: 1,
          copied_bytes: 1024,
          total_bytes: 1024,
          success_count: 1,
          skipped_count: 0,
          failed_count: 0,
        },
        {
          id: "migration-handoff",
          source_library_id: LIBRARY_ID,
          target_library_id: secondaryLibraryId,
          status: "running",
          total_items: 1,
          completed_items: 0,
          total_files: 1,
          completed_files: 0,
          copied_bytes: 0,
          total_bytes: 1024,
          success_count: 0,
          skipped_count: 0,
          failed_count: 0,
          incomplete_task_count: 1,
          task_transfer_count: 1,
          task_transfers: [{ copy_complete: true, commit_acked: false, source_cleanup_complete: false, completed_files: 1, total_files: 1 }],
        },
      ];
    }

    function snapshot(operations: Record<string, unknown>[], pendingRequests: Record<string, unknown>[], stateVersion: number) {
      return {
        ...ownerSnapshot,
        state_version: stateVersion,
        settings: {
          ...ownerSnapshot.settings,
          applied: {
            ...ownerSnapshot.settings.applied,
            default_library_id: LIBRARY_ID,
            libraries: [primaryDescriptor, secondaryDescriptor],
          },
        },
        library: {
          ...ownerSnapshot.library,
          libraries: [
            { id: LIBRARY_ID, root_binding: ROOT_BINDING, availability: "available", available: true, is_default: true, snapshot: primaryScan },
            { id: secondaryLibraryId, root_binding: secondaryRootBinding, availability: "available", available: true, snapshot: secondaryScan },
          ],
          migrations: libraryMigrations(),
        },
        operations,
        pending_requests: pendingRequests,
        previews: [],
      };
    }

    const initialOperations = [
      filePreview(101, LIBRARY_ID, ROOT_BINDING, "file-confirm-primary", "file-confirm-r1"),
      filePreview(102, LIBRARY_ID, ROOT_BINDING, "file-cancel-primary", "file-cancel-r1"),
      migrationPreview(103, "migration-confirm-primary", "migration-confirm-r1"),
      migrationPreview(104, "migration-cancel-primary", "migration-cancel-r1"),
      filePreview(105, secondaryLibraryId, secondaryRootBinding, "file-cancel-secondary", "file-cancel-secondary-r1"),
      filePreview(106, LIBRARY_ID, ROOT_BINDING, "file-cancel-failure", "file-cancel-failure-r1"),
      filePreview(107, LIBRARY_ID, ROOT_BINDING, "file-cancel-confirmed", "file-cancel-confirmed-r1"),
    ];
    let currentSnapshot = snapshot(initialOperations, [], 1);
    const initialBundle = await makeChunkedSnapshot(currentSnapshot);
    let currentManifest = initialBundle.manifest;
    const responseChunks = new Map(initialBundle.chunks);
    const actions: Array<Record<string, any>> = [];
    const acceptedActions: Array<{ action: Record<string, any>; seq: number }> = [];
    let nextActionSeq = 901;
    let stateRequests = 0;

    async function publishSnapshot(nextSnapshot: ReturnType<typeof snapshot>) {
      currentSnapshot = nextSnapshot;
      const nextBundle = await makeChunkedSnapshot(nextSnapshot);
      currentManifest = nextBundle.manifest;
      for (const [sha256, data] of nextBundle.chunks) responseChunks.set(sha256, data);
    }

    const html = await readFile(resolve(process.cwd(), "public/file/index.html"), "utf8");
    const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
    expect(body).toBeDefined();
    document.body.innerHTML = body!;
    expect(document.querySelector("script[src], link[rel='stylesheet']")).toBeNull();
    Object.defineProperty(window, "Telegram", {
      configurable: true,
      value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    vi.stubGlobal("localStorage", storage);
    Object.defineProperty(window, "matchMedia", { configurable: true, value: vi.fn(() => ({ matches: true })) });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
      expect(init?.cache).toBe("no-store");
      if (String(input) === "/api/updates/session") {
        expect(init?.method).toBe("POST");
        return { ok: true, status: 200, json: async () => updateSession() } as Response;
      }
      if (String(input) === "/api/state") {
        expect(new Headers(init?.headers).get("X-Snapshot-Format")).toBe("chunked-v1");
        stateRequests += 1;
        return { ok: true, status: 200, json: async () => currentManifest } as Response;
      }
      const chunkPath = String(input).match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
      if (chunkPath) {
        expect(new Headers(init?.headers).get("X-Cloud-Database-Epoch")).toBe(TEST_DATABASE_EPOCH);
        const sha256 = chunkPath[1]!;
        return { ok: true, status: 200, json: async () => ({ sha256, data: responseChunks.get(sha256) }) } as Response;
      }
      if (String(input) === "/api/actions") {
        const action = JSON.parse(String(init?.body)) as Record<string, any>;
        actions.push(action);
        const seq = nextActionSeq++;
        acceptedActions.push({ action, seq });
        const pending = {
          seq,
          request_id: action.request_id,
          kind: action.kind,
          status: "pending",
          created_at: Math.floor(Date.now() / 1000),
          payload: action.payload,
        };
        await publishSnapshot(snapshot(currentSnapshot.operations, [...currentSnapshot.pending_requests, pending], currentSnapshot.state_version + 1));
        return { ok: true, status: 202, json: async () => ({ seq, request_id: action.request_id }) } as Response;
      }
      throw new Error(`Unexpected request ${String(input)}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    window.fetch = fetchMock as typeof window.fetch;

    vi.resetModules();
    // The browser entry has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
    await import("../public/file/app.js");
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(0));
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    expect(element<HTMLButtonElement>('[data-view="waiting"]').classList.contains("active")).toBe(true);
    expect(document.querySelector('[data-preview-cancel-id="file-cancel-primary"]')).not.toBeNull();
    expect(document.querySelector('[data-preview-cancel-id="migration-cancel-primary"]')).not.toBeNull();
    const fileConfirm = element<HTMLButtonElement>('[data-confirm="file-confirm-primary"]');
    const migrationConfirm = element<HTMLButtonElement>('[data-migration-confirm="migration-confirm-primary"]');
    expect(fileConfirm.disabled).toBe(false);
    expect(migrationConfirm.disabled).toBe(false);

    const fileConfirmRefreshBefore = stateRequests;
    fileConfirm.click();
    await vi.waitFor(() => expect(actions.some((action) => action.kind === "file_confirm"
      && action.payload?.preview_id === "file-confirm-primary")).toBe(true));
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(fileConfirmRefreshBefore));
    const migrationConfirmRefreshBefore = stateRequests;
    element<HTMLButtonElement>('[data-migration-confirm="migration-confirm-primary"]').click();
    await vi.waitFor(() => expect(actions.some((action) => action.kind === "library_migration_confirm"
      && action.payload?.preview_id === "migration-confirm-primary")).toBe(true));
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(migrationConfirmRefreshBefore));

    const primaryPreviewCounts = () => ({
      files: document.querySelectorAll("#preview-list article.stack-card").length,
      migrations: document.querySelectorAll("#migration-preview-list article.stack-card").length,
    });
    const firstPrimaryCounts = primaryPreviewCounts();
    expect(firstPrimaryCounts.files).toBe(4);
    expect(firstPrimaryCounts.migrations).toBe(2);

    const failedCancelButton = element<HTMLButtonElement>('[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="file-cancel-failure"]');
    const activeCountBeforeFailedCancel = element<HTMLElement>("#waiting-count").textContent;
    const failedCancelRefreshBefore = stateRequests;
    failedCancelButton.click();
    await vi.waitFor(() => expect(actions.some((action) => action.kind === "preview_cancel"
      && action.payload?.preview_id === "file-cancel-failure")).toBe(true));
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(failedCancelRefreshBefore));
    expect(document.querySelector('[data-preview-cancel-id="file-cancel-failure"]')).not.toBeNull();
    expect(element<HTMLButtonElement>('[data-preview-cancel-id="file-cancel-failure"]').disabled).toBe(true);
    expect(element<HTMLButtonElement>('[data-confirm="file-cancel-failure"]').disabled).toBe(true);

    const failedCancelAction = actions.find((action) => action.kind === "preview_cancel" && action.payload?.preview_id === "file-cancel-failure");
    expect(failedCancelAction?.payload).toEqual({
      preview_kind: "file_preview",
      preview_seq: 106,
      preview_id: "file-cancel-failure",
      revision: "file-cancel-failure-r1",
    });
    const failedCancelAccepted = acceptedActions.find(({ action }) => action.request_id === failedCancelAction?.request_id);
    if (!failedCancelAccepted) throw new Error("Expected the failed cancellation to have an accepted HTTP receipt");
    const failedCancelOperation = {
      seq: failedCancelAccepted.seq,
      request_id: failedCancelAction?.request_id,
      kind: "preview_cancel",
      status: "failed",
      error: "The computer could not cancel this preview.",
      result: {},
    };
    const failedSnapshot = snapshot(
      [...currentSnapshot.operations, failedCancelOperation],
      currentSnapshot.pending_requests.filter((request) => request.seq !== failedCancelAccepted.seq),
      currentSnapshot.state_version + 1,
    );
    await publishSnapshot(failedSnapshot);
    const requestsBeforeFailedSnapshot = stateRequests;
    sockets[0]?.receive({
      type: "invalidate",
      database_epoch: TEST_DATABASE_EPOCH,
      state_version: failedSnapshot.state_version,
      reason: "state",
    });
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(requestsBeforeFailedSnapshot));
    expect(document.querySelector('[data-preview-cancel-id="file-cancel-failure"]')).not.toBeNull();
    expect(element<HTMLElement>("#preview-list").textContent).toContain("The computer could not cancel this preview.");
    expect(element<HTMLButtonElement>('[data-preview-cancel-id="file-cancel-failure"]').disabled).toBe(false);
    expect(element<HTMLButtonElement>('[data-preview-cancel-id="file-cancel-failure"]').textContent).toContain("Retry cancellation");
    expect(element<HTMLButtonElement>('[data-confirm="file-cancel-failure"]').disabled).toBe(false);
    expect(element<HTMLElement>("#waiting-count").textContent).toBe(activeCountBeforeFailedCancel);

    const activeCountBeforeRetiredCancel = Number(element<HTMLElement>("#waiting-count").textContent);
    expect(Number.isSafeInteger(activeCountBeforeRetiredCancel)).toBe(true);
    const retiredCancelRefreshBefore = stateRequests;
    element<HTMLButtonElement>('[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="file-cancel-confirmed"]').click();
    await vi.waitFor(() => expect(actions.some((action) => action.kind === "preview_cancel"
      && action.payload?.preview_id === "file-cancel-confirmed")).toBe(true));
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(retiredCancelRefreshBefore));
    const retiredCancelAction = actions.find((action) => action.kind === "preview_cancel" && action.payload?.preview_id === "file-cancel-confirmed");
    expect(retiredCancelAction?.payload).toEqual({
      preview_kind: "file_preview",
      preview_seq: 107,
      preview_id: "file-cancel-confirmed",
      revision: "file-cancel-confirmed-r1",
    });
    const retiredCancelAccepted = acceptedActions.find(({ action }) => action.request_id === retiredCancelAction?.request_id);
    if (!retiredCancelAccepted) throw new Error("Expected the confirmed-preview cancellation to have an accepted HTTP receipt");
    const retiredCancelFailure = {
      seq: retiredCancelAccepted.seq,
      request_id: retiredCancelAction?.request_id,
      kind: "preview_cancel",
      status: "failed",
      error: "This preview was already confirmed before cancellation.",
      result: {},
    };
    const retiredSnapshot = snapshot(
      [...currentSnapshot.operations.map((operation) => operation.seq === 107 ? { ...operation, status: "completed" } : operation), retiredCancelFailure],
      currentSnapshot.pending_requests.filter((request) => request.seq !== retiredCancelAccepted.seq),
      currentSnapshot.state_version + 1,
    );
    await publishSnapshot(retiredSnapshot);
    const requestsBeforeRetiredSnapshot = stateRequests;
    sockets[0]?.receive({
      type: "invalidate",
      database_epoch: TEST_DATABASE_EPOCH,
      state_version: retiredSnapshot.state_version,
      reason: "state",
    });
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(requestsBeforeRetiredSnapshot));
    expect(document.querySelector('[data-preview-cancel-id="file-cancel-confirmed"]')).toBeNull();
    expect(element<HTMLElement>("#waiting-count").textContent).toBe(String(activeCountBeforeRetiredCancel - 1));
    const primaryPreviewCountsAfterRetirement = primaryPreviewCounts();
    expect(primaryPreviewCountsAfterRetirement.files).toBe(firstPrimaryCounts.files - 1);

    const cancelPrimaryFile = element<HTMLButtonElement>('[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="file-cancel-primary"]');
    const fileRequestsBeforeCancel = stateRequests;
    cancelPrimaryFile.click();
    await vi.waitFor(() => expect(actions.some((action) => action.kind === "preview_cancel"
      && action.payload?.preview_id === "file-cancel-primary")).toBe(true));
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(fileRequestsBeforeCancel));
    expect(primaryPreviewCounts().files).toBe(primaryPreviewCountsAfterRetirement.files);
    expect(element<HTMLElement>("#preview-list").textContent).toContain("Review needed");
    const primaryFileCancelAction = actions.find((action) => action.kind === "preview_cancel" && action.payload?.preview_id === "file-cancel-primary");
    expect(primaryFileCancelAction?.payload).toEqual({
      preview_kind: "file_preview",
      preview_seq: 102,
      preview_id: "file-cancel-primary",
      revision: "file-cancel-r1",
    });

    const cancelPrimaryMigration = element<HTMLButtonElement>('[data-preview-cancel-kind="library_migration_preview"][data-preview-cancel-id="migration-cancel-primary"]');
    const migrationRequestsBeforeCancel = stateRequests;
    cancelPrimaryMigration.click();
    await vi.waitFor(() => expect(actions.some((action) => action.kind === "preview_cancel"
      && action.payload?.preview_id === "migration-cancel-primary")).toBe(true));
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(migrationRequestsBeforeCancel));
    expect(primaryPreviewCounts().migrations).toBe(firstPrimaryCounts.migrations);
    const primaryMigrationCancelAction = actions.find((action) => action.kind === "preview_cancel" && action.payload?.preview_id === "migration-cancel-primary");
    expect(primaryMigrationCancelAction?.payload).toEqual({
      preview_kind: "library_migration_preview",
      preview_seq: 104,
      preview_id: "migration-cancel-primary",
      revision: "migration-cancel-r1",
    });

    element<HTMLButtonElement>('[data-review-library="secondary-library"]').click();
    await vi.waitFor(() => expect(element<HappyDOMSelectElement>("#library-select").value).toBe(secondaryLibraryId));
    expect(document.querySelector('[data-preview-cancel-id="file-cancel-secondary"]')).not.toBeNull();
    const secondaryFileCancel = element<HTMLButtonElement>('[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="file-cancel-secondary"]');
    const secondaryRequestsBeforeCancel = stateRequests;
    secondaryFileCancel.click();
    await vi.waitFor(() => expect(actions.some((action) => action.kind === "preview_cancel"
      && action.payload?.preview_id === "file-cancel-secondary")).toBe(true));
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(secondaryRequestsBeforeCancel));
    expect(document.querySelector('[data-preview-cancel-id="file-cancel-secondary"]')).not.toBeNull();
    const secondaryFileCancelAction = actions.find((action) => action.kind === "preview_cancel" && action.payload?.preview_id === "file-cancel-secondary");
    expect(secondaryFileCancelAction?.payload).toEqual({
      preview_kind: "file_preview",
      preview_seq: 105,
      preview_id: "file-cancel-secondary",
      revision: "file-cancel-secondary-r1",
    });
    const secondaryFileCancelAccepted = acceptedActions.find(({ action }) => action.request_id === secondaryFileCancelAction?.request_id);
    if (!secondaryFileCancelAccepted) throw new Error("Expected the final cancellation to have an accepted HTTP receipt");
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#waiting-list").textContent).toContain(`Waiting for computer · #${secondaryFileCancelAccepted.seq}`);
      expect(element<HTMLElement>("#waiting-list").textContent).toContain("Saved in the cloud inbox");
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
    });

    const librarySelect = element<HappyDOMSelectElement>("#library-select");
    librarySelect.value = LIBRARY_ID;
    librarySelect.dispatchEvent(new HappyDOMEvent("change", { bubbles: true }));
    await vi.waitFor(() => expect(librarySelect.value).toBe(LIBRARY_ID));

    const settledOperations: Record<string, unknown>[] = [
      filePreview(101, LIBRARY_ID, ROOT_BINDING, "file-confirm-primary", "file-confirm-r1"),
      { ...filePreview(102, LIBRARY_ID, ROOT_BINDING, "file-cancel-primary", "file-cancel-r1"), status: "cancelled" },
      migrationPreview(103, "migration-confirm-primary", "migration-confirm-r1"),
      { ...migrationPreview(104, "migration-cancel-primary", "migration-cancel-r1"), status: "cancelled" },
      { ...filePreview(105, secondaryLibraryId, secondaryRootBinding, "file-cancel-secondary", "file-cancel-secondary-r1"), status: "cancelled" },
      { ...filePreview(106, LIBRARY_ID, ROOT_BINDING, "file-cancel-failure", "file-cancel-failure-r1"), status: "cancelled" },
      { ...filePreview(107, LIBRARY_ID, ROOT_BINDING, "file-cancel-confirmed", "file-cancel-confirmed-r1"), status: "completed" },
      ...acceptedActions.map(({ action, seq }) => {
        const cancellationError = action.request_id === failedCancelAction?.request_id
          ? "The computer could not cancel this preview."
          : action.request_id === retiredCancelAction?.request_id
            ? "This preview was already confirmed before cancellation."
            : null;
        return {
        seq,
        request_id: action.request_id,
        kind: action.kind,
        status: cancellationError ? "failed" : "completed",
        ...(cancellationError ? { error: cancellationError } : {}),
        result: action.kind === "preview_cancel" && !cancellationError
          ? { cancelled: true, ...action.payload }
          : { accepted: true, preview_id: action.payload?.preview_id, revision: action.payload?.revision },
        };
      }),
    ];
    settledOperations[0] = { ...settledOperations[0]!, status: "confirmed" };
    settledOperations[2] = { ...settledOperations[2]!, status: "confirmed" };
    const terminalSnapshot = snapshot(settledOperations, [], currentSnapshot.state_version + 1);
    await publishSnapshot(terminalSnapshot);
    const requestsBeforeTerminalUpdate = stateRequests;
    sockets[0]?.receive({
      type: "invalidate",
      database_epoch: TEST_DATABASE_EPOCH,
      state_version: terminalSnapshot.state_version,
      reason: "state",
    });
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(requestsBeforeTerminalUpdate));
    await vi.waitFor(() => expect(primaryPreviewCounts()).toEqual({ files: 0, migrations: 0 }));
    expect(element<HTMLElement>("#waiting-count").textContent).toBe("0");

    element<HTMLButtonElement>('[data-view="organizing"]').click();
    expect(document.querySelector('[data-migration-control="finalize_terminal"][data-migration-id="migration-finalization"]')).not.toBeNull();
    expect(document.querySelector('[data-migration-control="pause"][data-migration-id="migration-handoff"]')).not.toBeNull();
    expect(document.querySelector("#migration-progress-list article.stack-card")).not.toBeNull();
    expect(document.querySelectorAll("#migration-progress-list article.stack-card")).toHaveLength(2);
    expect(document.querySelectorAll("#history-migration-list article.stack-card")).toHaveLength(1);

    element<HTMLButtonElement>('[data-view="history"]').click();
    const historySearch = element<HTMLInputElement>("#history-search");
    historySearch.value = "cancelled";
    historySearch.dispatchEvent(new Event("input", { bubbles: true }));
    expect(element<HTMLElement>("#operation-list").textContent).toContain("File preview");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Library migration preview");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Cancelled");

    historySearch.value = "completed";
    historySearch.dispatchEvent(new Event("input", { bubbles: true }));
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Preview cancellation");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Completed");

    historySearch.value = "already confirmed before cancellation";
    historySearch.dispatchEvent(new Event("input", { bubbles: true }));
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Preview cancellation");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("This preview was already confirmed before cancellation.");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Failed");

    historySearch.value = "";
    historySearch.dispatchEvent(new Event("input", { bubbles: true }));
    expect(element<HTMLElement>("#history-migration-list").textContent).toContain("Primary library → Secondary library");
    expect(element<HTMLElement>("#history-migration-list").textContent).not.toContain("Finish pending finalization");
    expect(element<HTMLElement>("#history-migration-list").textContent).not.toContain("Unfinished task handoff");
  });

  it("keeps unknown cancellations paused and preserves incomplete offline review identity", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const secondaryLibraryId = "offline-review-library";
    const secondaryRootBinding = `v1:${"c".repeat(64)}`;
    const unknownRequestId = "00000000-0000-4000-8000-000000000601";
    const unknownPayload = {
      preview_kind: "file_preview",
      preview_seq: 601,
      preview_id: "unknown-cancel-preview",
      revision: "unknown-cancel-r1",
    };
    const memoryStorage = makeMemoryStorage();
    let failCancellationStorageWrite = false;
    const storage = {
      getItem: memoryStorage.getItem,
      setItem(key: string, value: string) {
        if (failCancellationStorageWrite && key === "file-manager.preview-cancellations.v1") {
          throw new Error("Simulated cancellation-state storage failure");
        }
        memoryStorage.setItem(key, value);
      },
      removeItem: memoryStorage.removeItem,
      clear: memoryStorage.clear,
    };
    memoryStorage.setItem("file-manager.actions.v1", JSON.stringify([{
      request_id: unknownRequestId,
      kind: "preview_cancel",
      surface: "file",
      payload: unknownPayload,
      queued_at_ms: Date.now(),
      replay_expires_at_ms: Date.now() + 60 * 60 * 1_000,
      delivery_outcome: "outcome_unknown",
      delivery_error_code: "action_replay_window_unverifiable",
      delivery_error: "Check the computer before trying this cancellation again.",
    }]));

    const primaryDescriptor = {
      id: LIBRARY_ID,
      name: "Primary library",
      root: "/srv/media",
      root_binding: ROOT_BINDING,
      default_category: "Films",
      availability: "available",
      available: true,
      is_default: true,
    };
    const secondaryDescriptor = {
      id: secondaryLibraryId,
      name: "Offline library",
      root: "/srv/offline",
      root_binding: secondaryRootBinding,
      default_category: "Series",
      availability: "offline",
      available: false,
    };
    // Model an older nested library projection repeating a stale global count.
    // The top-level migration_omitted_count below is the current global value.
    const primaryScan = { ...ownerSnapshot.library, libraries: [], migrations: [], migration_omitted_count: 2 };
    const migrationPreview = {
      seq: 602,
      request_id: "migration-preview-incomplete-offline",
      kind: "library_migration_preview",
      status: "awaiting_confirmation",
      details_incomplete: true,
      preview_id: "migration-incomplete-offline",
      revision: "migration-incomplete-r1",
      reason: "The offline library preview omitted its path details.",
      result: {
        id: "migration-incomplete-offline",
        revision: "migration-incomplete-r1",
        source_library_id: secondaryLibraryId,
        target_library_id: LIBRARY_ID,
        source_root_binding: secondaryRootBinding,
        target_root_binding: ROOT_BINDING,
      },
    };
    const initialSnapshot = {
      ...ownerSnapshot,
      state_version: 1,
      settings: {
        ...ownerSnapshot.settings,
        applied: {
          ...ownerSnapshot.settings.applied,
          default_library_id: LIBRARY_ID,
          libraries: [primaryDescriptor, secondaryDescriptor],
        },
      },
      library: {
        ...ownerSnapshot.library,
        migration_omitted_count: 3,
        migrations: [],
        libraries: [
          { id: LIBRARY_ID, root_binding: ROOT_BINDING, availability: "available", available: true, is_default: true, snapshot: primaryScan },
          { id: secondaryLibraryId, root_binding: secondaryRootBinding, availability: "offline", available: false },
        ],
      },
      operations: [filePreviewFixture(601, LIBRARY_ID, ROOT_BINDING, "unknown-cancel-preview", "unknown-cancel-r1"), migrationPreview],
      pending_requests: [],
      previews: [{ preview_incomplete: true, omitted_count: 2, reason: "Two active preview identities were omitted from this fixture." }],
    };
    const fixture = await mountFileManagerFixture(initialSnapshot, storage);

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    expect(element<HTMLElement>("#waiting-count").textContent).toBe("3+");
    expect(element<HTMLElement>("#preview-list").textContent).toContain("2 active previews were omitted");
    const aggregateCard = [...document.querySelectorAll<HTMLElement>("#preview-list article.stack-card")]
      .find((card) => card.textContent?.includes("Two active preview identities were omitted"));
    expect(aggregateCard).toBeDefined();
    expect(aggregateCard?.querySelector("[data-preview-cancel], [data-confirm], [data-migration-confirm]")).toBeNull();
    expect(element<HTMLButtonElement>('[data-confirm="unknown-cancel-preview"]').disabled).toBe(true);
    expect(document.querySelector('[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="unknown-cancel-preview"]')).toBeNull();
    expect(element<HTMLButtonElement>('[data-preview-cancel-dismiss-kind="file_preview"][data-preview-cancel-dismiss-id="unknown-cancel-preview"]')).toBeDefined();
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);

    element<HTMLButtonElement>('[data-preview-cancel-dismiss-kind="file_preview"][data-preview-cancel-dismiss-id="unknown-cancel-preview"]').click();
    expect(JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null")).toEqual([]);
    expect(element<HTMLElement>("#preview-list").innerHTML).toContain('data-preview-cancel-kind="file_preview"');
    expect(element<HTMLButtonElement>('[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="unknown-cancel-preview"]').disabled).toBe(false);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);

    const offlineReview = element<HTMLButtonElement>('[data-review-library="offline-review-library"][data-review-preview-kind="library_migration_preview"][data-review-preview-id="migration-incomplete-offline"]');
    offlineReview.click();
    await vi.waitFor(() => expect(element<HappyDOMSelectElement>("#library-select").value).toBe(secondaryLibraryId));
    expect(element<HTMLElement>("#migration-preview-list").textContent).toContain("The offline library preview omitted its path details.");
    expect(document.querySelector('[data-migration-confirm="migration-incomplete-offline"]')).toBeNull();
    const offlineCancel = element<HTMLButtonElement>('[data-preview-cancel-kind="library_migration_preview"][data-preview-cancel-id="migration-incomplete-offline"]');
    expect(offlineCancel.disabled).toBe(false);

    failCancellationStorageWrite = true;
    offlineCancel.click();
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
    expect(element<HTMLElement>("#toast").textContent).toContain("could not safely save the cancellation request");

    failCancellationStorageWrite = false;
    element<HTMLButtonElement>('[data-preview-cancel-kind="library_migration_preview"][data-preview-cancel-id="migration-incomplete-offline"]').click();
    await vi.waitFor(() => expect(fixture.actions).toHaveLength(1));
    expect(fixture.actions[0]?.payload).toEqual({
      preview_kind: "library_migration_preview",
      preview_seq: 602,
      preview_id: "migration-incomplete-offline",
      revision: "migration-incomplete-r1",
    });

    element<HTMLButtonElement>('[data-view="history"]').click();
    expect(element<HTMLElement>("#history-migration-list").textContent).toContain("3 additional migrations are not included in this view");
  });

  it("restores an accepted cancellation after reload and settles it only from a terminal snapshot", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const preview = filePreviewFixture(701, LIBRARY_ID, ROOT_BINDING, "reload-cancel-preview", "reload-cancel-r1");
    const initialSnapshot = { ...ownerSnapshot, state_version: 1, operations: [preview], pending_requests: [], previews: [] };
    const storage = makeMemoryStorage();
    const acceptedActions: Array<Record<string, any>> = [];
    let acceptedPendingSnapshot: Record<string, any> | null = null;
    const onAction = async (
      action: Record<string, any>,
      seq: number,
      publishSnapshot: (snapshot: Record<string, any>) => Promise<void>,
    ) => {
      acceptedActions.push(action);
      acceptedPendingSnapshot = {
        ...initialSnapshot,
        state_version: 2,
        pending_requests: [{
          seq,
          request_id: action.request_id,
          kind: action.kind,
          status: "pending",
          created_at: Math.floor(Date.now() / 1000),
          payload: action.payload,
        }],
      };
      await publishSnapshot(acceptedPendingSnapshot);
    };
    const firstPage = await mountFileManagerFixture(initialSnapshot, storage, onAction);
    await vi.waitFor(() => expect(firstPage.sockets).toHaveLength(1));
    await vi.waitFor(() => expect(firstPage.sockets[0]?.readyState).toBe(MockWebSocket.OPEN));

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    const stateRequestsBeforeCancel = firstPage.stateRequests;
    element<HTMLButtonElement>('[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="reload-cancel-preview"]').click();
    await vi.waitFor(() => expect(acceptedActions).toHaveLength(1));
    await vi.waitFor(() => expect(firstPage.stateRequests).toBeGreaterThan(stateRequestsBeforeCancel));
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#waiting-list").textContent).toContain("Waiting for computer · #901");
      expect(element<HTMLElement>("#waiting-list").textContent).toContain("Saved in the cloud inbox");
    });
    const acceptedCancel = acceptedActions[0]!;
    expect(acceptedCancel.payload).toEqual({
      preview_kind: "file_preview",
      preview_seq: 701,
      preview_id: "reload-cancel-preview",
      revision: "reload-cancel-r1",
    });
    await vi.waitFor(() => expect(JSON.parse(storage.getItem("file-manager.actions.v1") ?? "null")).toEqual([]));
    const persisted = JSON.parse(storage.getItem("file-manager.preview-cancellations.v1") ?? "null") as { entries?: Array<{ key: string; tracker: Record<string, unknown> }> } | null;
    expect(persisted?.entries).toContainEqual({
      key: JSON.stringify(["file_preview", 701, "reload-cancel-preview", "reload-cancel-r1"]),
      tracker: { requestId: acceptedCancel.request_id, seq: 901, status: "submitted", error: null },
    });
    expect(acceptedPendingSnapshot).not.toBeNull();

    const reloadedPage = await mountFileManagerFixture(acceptedPendingSnapshot!, storage, onAction);
    await vi.waitFor(() => expect(reloadedPage.sockets).toHaveLength(1));
    await vi.waitFor(() => expect(reloadedPage.sockets[0]?.readyState).toBe(MockWebSocket.OPEN));
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => expect(element<HTMLButtonElement>('[data-confirm="reload-cancel-preview"]').disabled).toBe(true));
    const restoredCancelButton = element<HTMLButtonElement>('[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="reload-cancel-preview"]');
    expect(restoredCancelButton.disabled).toBe(true);
    expect(restoredCancelButton.textContent).toContain("Cancel request submitted");
    expect(acceptedActions).toHaveLength(1);
    expect(reloadedPage.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);

    const terminalSnapshot = {
      ...acceptedPendingSnapshot!,
      state_version: 3,
      operations: [
        { ...preview, status: "cancelled" },
        {
          seq: 901,
          request_id: acceptedCancel.request_id,
          kind: "preview_cancel",
          status: "completed",
          result: { cancelled: true, ...acceptedCancel.payload },
        },
      ],
      pending_requests: [],
    };
    await reloadedPage.publishSnapshot(terminalSnapshot);
    const stateRequestsBeforeTerminal = reloadedPage.stateRequests;
    reloadedPage.sockets[0]?.receive({
      type: "invalidate",
      database_epoch: TEST_DATABASE_EPOCH,
      state_version: 3,
      reason: "state",
    });
    await vi.waitFor(() => expect(reloadedPage.stateRequests).toBeGreaterThan(stateRequestsBeforeTerminal));
    await vi.waitFor(() => expect(document.querySelector('[data-preview-cancel-id="reload-cancel-preview"]')).toBeNull());
    await vi.waitFor(() => expect(element<HTMLElement>("#history-count").textContent).toBe("2"));
    expect(element<HTMLElement>("#waiting-count").textContent).toBe("0");

    element<HTMLButtonElement>('[data-view="history"]').click();
    expect(element<HTMLInputElement>("#history-search").value).toBe("");
    expect(element<HTMLElement>("#history-summary").textContent).toContain("2 recent records retained");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Preview cancellation");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Completed");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("File preview");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Cancelled");
  });
});
