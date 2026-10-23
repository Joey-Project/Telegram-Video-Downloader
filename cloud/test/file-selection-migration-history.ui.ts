import { readFile } from "node:fs/promises";
import { webcrypto } from "node:crypto";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { makeChunkedSnapshot, TEST_DATABASE_EPOCH } from "./snapshot-loader-fixtures.js";
import { syntheticCredentials } from "./fixtures/synthetic-credentials.js";

const SOURCE_LIBRARY_ID = "source-library";
const TARGET_LIBRARY_ID = "archive-library";
const SOURCE_BINDING = `v1:${"a".repeat(64)}`;
const TARGET_BINDING = `v1:${"c".repeat(64)}`;
const MOCK_INIT_DATA = new URLSearchParams({ query_id: "file-selection-migration-history", auth_date: "0" }).toString();
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
    this.dispatchEvent(new Event("open"));
    this.receive({ type: "ready", database_epoch: TEST_DATABASE_EPOCH });
  }

  receive(message: Record<string, unknown>) {
    const event = new Event("message");
    Object.defineProperty(event, "data", { configurable: true, value: JSON.stringify(message) });
    this.dispatchEvent(event);
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

const historyCollection = { id: "history", title: "History", kind: "series", order: 1 };
const batchCollection = { id: "batch", title: "Batch", kind: "series" };
const batchItems = Array.from({ length: 101 }, (_, index) => ({
  id: `batch-${String(index).padStart(3, "0")}`,
  title: `Batch ${String(index).padStart(3, "0")}`,
  relative_path: `Batch/${String(index).padStart(3, "0")}.mkv`,
  presence: "present",
  bytes: 1024,
  confidence: "high",
  metadata_status: "complete",
  collection: { ...batchCollection, order: index + 1 },
}));

const movedSource = {
  id: "source-moved-item",
  title: "History moved source",
  relative_path: "History/moved.mp4",
  presence: "missing",
  bytes: 1024,
  confidence: "high",
  metadata_status: "complete",
  collection: { ...historyCollection, order: 1 },
  moved_to: {
    library_id: TARGET_LIBRARY_ID,
    library_name: "Archive",
    item_id: "archive-moved-item",
    relative_path: "History/moved.mp4",
  },
};

const ordinaryMissing = {
  id: "ordinary-missing-item",
  title: "History ordinary missing",
  relative_path: "History/ordinary-missing.mp4",
  presence: "missing",
  bytes: 1024,
  confidence: "high",
  metadata_status: "complete",
  collection: { ...historyCollection, order: 2 },
};

const mixedItems = [
  movedSource,
  ordinaryMissing,
  {
    id: "history-unavailable-item",
    title: "History unavailable",
    relative_path: "History/unavailable.mp4",
    presence: "unavailable",
    bytes: 1024,
    confidence: "high",
    metadata_status: "complete",
    collection: { ...historyCollection, order: 3 },
  },
  {
    id: "history-unknown-item",
    title: "History unknown",
    relative_path: "History/unknown.mp4",
    presence: "unknown",
    bytes: 1024,
    confidence: "high",
    metadata_status: "complete",
    collection: { ...historyCollection, order: 4 },
  },
  {
    id: "history-present-item",
    title: "History present",
    relative_path: "History/present.mp4",
    presence: "present",
    bytes: 1024,
    confidence: "high",
    metadata_status: "complete",
    collection: { ...historyCollection, order: 5 },
  },
];

const sourceSnapshot = {
  revision: "source-selection-revision",
  scanned_at: 1_800_000_000,
  root_label: "Source library",
  ready: true,
  default_library_id: SOURCE_LIBRARY_ID,
  items: [...batchItems, ...mixedItems],
  categories: ["Batch", "History"],
  warnings: [],
};

const targetSnapshot = {
  revision: "target-selection-revision",
  scanned_at: 1_800_000_000,
  root_label: "Archive",
  ready: true,
  items: [{
    id: "archive-moved-item",
    title: "History moved in Archive",
    relative_path: "History/moved.mp4",
    presence: "present",
    bytes: 1024,
    confidence: "high",
    metadata_status: "complete",
  }],
  categories: ["History"],
  warnings: [],
};

const ownerSnapshot = {
  synced: true,
  stale: false,
  last_seen: Math.floor(Date.now() / 1000),
  settings: {
    download_dir: "/srv/source",
    applied: {
      default_library_id: SOURCE_LIBRARY_ID,
      libraries: [
        {
          id: SOURCE_LIBRARY_ID,
          name: "Source library",
          root: "/srv/source",
          root_binding: SOURCE_BINDING,
          default_category: "",
          availability: "available",
          available: true,
          is_default: true,
        },
        {
          id: TARGET_LIBRARY_ID,
          name: "Archive",
          root: "/srv/archive",
          root_binding: TARGET_BINDING,
          default_category: "",
          availability: "available",
          available: true,
          is_default: false,
        },
      ],
    },
  },
  library: {
    ...sourceSnapshot,
    libraries: [
      { id: SOURCE_LIBRARY_ID, name: "Source library", availability: "available", available: true, is_default: true },
      {
        id: TARGET_LIBRARY_ID,
        name: "Archive",
        root: "/srv/archive",
        root_binding: TARGET_BINDING,
        availability: "available",
        available: true,
        is_default: false,
        snapshot: targetSnapshot,
      },
    ],
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

function click(selector: string) {
  element<HTMLElement>(selector).click();
}

function setInputValue(selector: string, value: string) {
  const input = element<HTMLElement>(selector) as HTMLElement & { value: string };
  input.value = value;
  input.dispatchEvent(new Event(selector === "#search-input" ? "input" : "change", { bubbles: true }));
}

function expectReasonLinked(row: HTMLElement) {
  const checkbox = row.querySelector<HTMLInputElement>("[data-select-item]");
  expect(checkbox).not.toBeNull();
  expect(checkbox!.disabled).toBe(true);
  const reasonId = checkbox!.getAttribute("aria-describedby");
  expect(reasonId).toBeTruthy();
  const reason = reasonId ? document.getElementById(reasonId) : null;
  expect(reason).not.toBeNull();
  expect(row.contains(reason)).toBe(true);
  expect(reason!.classList.contains("media-selection-reason")).toBe(true);
  expect(reason!.textContent?.trim()).toBeTruthy();
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
  document.body.innerHTML = "";
});

describe("mobile library selection and moved history", () => {
  it("explains skipped rows, honors the selection limit, keeps ordinary missing items, and opens a moved target", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);
    const sockets = installMockWebSocket();

    const html = await readFile(resolve(process.cwd(), "public/file/index.html"), "utf8");
    const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
    expect(body).toBeDefined();
    document.body.innerHTML = body!;

    Object.defineProperty(window, "Telegram", {
      configurable: true,
      value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    Object.defineProperty(window, "innerWidth", { configurable: true, value: 390 });
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: vi.fn(() => ({ matches: true })),
    });
    vi.stubGlobal("localStorage", window.localStorage);

    const responseChunks = new Map(chunks);
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input) === "/api/updates/session") {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        expect(init?.method).toBe("POST");
        return { ok: true, status: 200, json: async () => updateSession() } as Response;
      }
      if (String(input) === "/api/state") {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        expect(new Headers(init?.headers).get("X-Snapshot-Format")).toBe("chunked-v1");
        return { ok: true, status: 200, json: async () => manifest } as Response;
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

    // The browser entry has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
    await import("../public/file/app.js");
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    expect(sockets).toHaveLength(1);
    expect((element<HTMLElement>("#library-select") as unknown as { value: string }).value).toBe(SOURCE_LIBRARY_ID);

    // The first two pages contain the 100 selectable batch rows. The last batch row and the
    // mixed-status present row remain visible on page three and are skipped at the cap.
    click("#select-visible");
    expect(element<HTMLElement>("#selection-count").textContent).toBe("50 items selected");
    click('button[data-library-page="library"][data-page-direction="1"]');
    click("#select-visible");
    expect(element<HTMLElement>("#selection-count").textContent).toBe("100 items selected");
    click('button[data-library-page="library"][data-page-direction="1"]');
    click("#select-visible");
    expect(element<HTMLElement>("#selection-count").textContent).toBe("100 items selected");
    expect(element<HTMLElement>("#toast").textContent).toContain("Only 100 items can be selected");
    expect(element<HTMLElement>("#toast").textContent).toContain("2 more were left unselected");

    click("#clear-selection");
    setInputValue("#search-input", "History");
    expect(document.querySelectorAll("#library-list [data-item-row]")).toHaveLength(5);
    const movedRow = element<HTMLElement>('#library-list [data-item-row="source-moved-item"]');
    const movedLink = element<HTMLElement>('[data-open-moved-target][data-source-item-id="source-moved-item"]');
    expect(movedRow.querySelector(".moved-target")?.textContent).toContain("Moved to Archive");
    expect(movedLink.dataset.targetLibraryId).toBe(TARGET_LIBRARY_ID);
    expect(movedLink.dataset.targetItemId).toBe("archive-moved-item");

    for (const itemId of [
      "source-moved-item",
      "ordinary-missing-item",
      "history-unavailable-item",
      "history-unknown-item",
    ]) {
      expectReasonLinked(element<HTMLElement>(`#library-list [data-item-row="${itemId}"]`));
    }
    const historyGroup = [...document.querySelectorAll<HTMLInputElement>("#library-list [data-select-group]")]
      .find((checkbox) => checkbox.closest("summary")?.textContent?.includes("History"));
    expect(historyGroup).toBeDefined();
    const groupCount = historyGroup!.closest("summary")?.querySelector<HTMLElement>(".group-selection-count");
    expect(groupCount?.textContent).toMatch(/0[^0-9]*selected/iu);
    expect(groupCount?.textContent).toMatch(/1[^0-9]*selectable/iu);
    expect(groupCount?.textContent).toMatch(/4[^0-9]*skipped/iu);
    historyGroup!.click();
    expect(element<HTMLElement>("#selection-count").textContent).toBe("1 item selected");
    const selectedHistoryIds = [...document.querySelectorAll<HTMLInputElement>("#library-list [data-select-item]:checked")]
      .map((checkbox) => checkbox.dataset.selectItem);
    expect(selectedHistoryIds).toEqual(["history-present-item"]);
    const updatedGroupCount = [...document.querySelectorAll<HTMLElement>("#library-list .group-selection-count")]
      .find((count) => count.closest("summary")?.textContent?.includes("History"));
    expect(updatedGroupCount?.textContent).toMatch(/1[^0-9]*selected/iu);
    expect(updatedGroupCount?.textContent).toMatch(/1[^0-9]*selectable/iu);
    expect(updatedGroupCount?.textContent).toMatch(/4[^0-9]*skipped/iu);

    click('[data-view="needs"]');
    expect(document.querySelector('#needs-list [data-item-row="source-moved-item"]')).toBeNull();
    expect(document.querySelector('#needs-list [data-item-row="ordinary-missing-item"]')).not.toBeNull();

    click('[data-view="library"]');
    setInputValue("#search-input", "History moved source");
    setInputValue("#category-filter", "History");
    setInputValue("#status-filter", "missing");
    click('[data-open-moved-target][data-source-item-id="source-moved-item"]');
    expect((element<HTMLElement>("#library-select") as unknown as { value: string }).value).toBe(TARGET_LIBRARY_ID);
    expect(element<HTMLInputElement>("#search-input").value).toBe("");
    expect(element<HTMLInputElement>("#category-filter").value).toBe("");
    expect((element<HTMLElement>("#status-filter") as unknown as { value: string }).value).toBe("");
    expect(element<HTMLElement>('#library-list [data-item-row="archive-moved-item"]')).not.toBeNull();
    expect(element<HTMLElement>(".detail-sheet").getAttribute("aria-label")).toBe("File details");
    expect(element<HTMLElement>(".detail-sheet h2").textContent).toBe("History moved in Archive");
  });
});
