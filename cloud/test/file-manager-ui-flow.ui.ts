import { readFile } from "node:fs/promises";
import { webcrypto } from "node:crypto";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { makeChunkedSnapshot, TEST_DATABASE_EPOCH } from "./snapshot-loader-fixtures.js";

const LIBRARY_ID = "test-library";
const ROOT_BINDING = `v1:${"a".repeat(64)}`;
const MOCK_INIT_DATA = new URLSearchParams({ query_id: "file-manager-ui-flow", auth_date: "0" }).toString();

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

describe("file manager UI flow", () => {
  it("shows and clears selected items, navigates to the move card, and exposes mobile Details", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);

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
    vi.stubGlobal("localStorage", window.localStorage);
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: vi.fn(() => ({ matches: true })),
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
      expect(init?.cache).toBe("no-store");
      if (String(input) === "/api/state") {
        expect(new Headers(init?.headers).get("X-Snapshot-Format")).toBe("chunked-v1");
        return { ok: true, status: 200, json: async () => manifest } as Response;
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

    // The browser entry has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
    await import("../public/file/app.js");
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    expect(fetchMock).toHaveBeenCalledTimes(1 + manifest.library.chunks.length + manifest.state.chunks.length);
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

    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2 + manifest.library.chunks.length + manifest.state.chunks.length));
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
});
