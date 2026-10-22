import { readFile } from "node:fs/promises";
import { webcrypto } from "node:crypto";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { makeChunkedSnapshot, TEST_DATABASE_EPOCH } from "./snapshot-loader-fixtures.js";

const LIBRARY_ID = "main-library";
const ROOT_BINDING = `v1:${"a".repeat(64)}`;
const MOCK_INIT_DATA = new URLSearchParams({ query_id: "settings-library-ui-flow", auth_date: "0" }).toString();

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
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  const domWindow = window as Window & { happyDOM?: { cancelAsync(): void } };
  domWindow.happyDOM?.cancelAsync();
  document.body.innerHTML = "";
});

describe("settings library UI flow", () => {
  it("shows compact default snapshot counts and category, and still queues a settings action", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);

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
      if (String(input) === "/api/state") {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        expect(init?.cache).toBe("no-store");
        return { ok: true, status: 200, json: async () => manifest } as Response;
      }
      const chunkPath = String(input).match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
      if (chunkPath) {
        expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
        expect(new Headers(init?.headers).get("X-Cloud-Database-Epoch")).toBe(TEST_DATABASE_EPOCH);
        expect(init?.cache).toBe("no-store");
        const sha256 = chunkPath[1]!;
        return { ok: true, status: 200, json: async () => ({ sha256, data: chunks.get(sha256) }) } as Response;
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
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));

    const card = element<HTMLElement>("#library-list .settings-library-card");
    expect(card.textContent).toContain("2 media items");
    expect(card.textContent).toContain("Default download category: Films");
    expect(card.textContent).toMatch(/Last scan: (?!Not scanned)/u);
    expect(element<HTMLInputElement>('[data-category-form="main-library"] input[name="default_category"]').value).toBe("Films");
    const scanButton = element<HTMLButtonElement>('[data-settings-action="library_scan"]');
    expect(scanButton.disabled).toBe(false);
    scanButton.click();

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/api/actions", expect.any(Object)));
    const actionRequest = fetchMock.mock.calls.find(([input]) => String(input) === "/api/actions");
    expect(actionRequest).toBeDefined();
    const action = JSON.parse(String(actionRequest?.[1]?.body));
    expect(action).toMatchObject({ kind: "library_scan", payload: { library_id: LIBRARY_ID } });
    const snapshotRequestCount = 1 + manifest.library.chunks.length + manifest.state.chunks.length;
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(snapshotRequestCount * 2 + 1));
  });

  it("keeps settings private when a chunk does not match its manifest", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
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
});
