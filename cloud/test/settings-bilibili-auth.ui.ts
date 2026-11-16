import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { makeChunkedSnapshot, TEST_DATABASE_EPOCH } from "./snapshot-loader-fixtures.js";
import { syntheticCredentials } from "./fixtures/synthetic-credentials.js";

const MOCK_INIT_DATA = new URLSearchParams({ query_id: "settings-bilibili-auth", auth_date: "0" }).toString();
let originalWebSocket: PropertyDescriptor | undefined;
let originalConfirm: PropertyDescriptor | undefined;

class MiniSocket extends EventTarget {
  static OPEN = 1;
  readyState = 0;
  protocol: string;
  constructor(_url: string | URL, protocols?: string | string[]) {
    super();
    this.protocol = Array.isArray(protocols) ? protocols[0] ?? "" : protocols ?? "";
    queueMicrotask(() => {
      this.readyState = 1;
      this.dispatchEvent(new Event("open"));
      const event = new Event("message");
      Object.defineProperty(event, "data", { configurable: true, value: JSON.stringify({ type: "ready", database_epoch: TEST_DATABASE_EPOCH, snapshot_control: "v1" }) });
      this.dispatchEvent(event);
    });
  }
  send() {}
  close() { this.readyState = 3; }
}

async function boot(auth?: Record<string, unknown>, operations: unknown[] = [], stale = false) {
  const snapshot = {
    state_version: 1,
    reported_at: stale ? Date.now() - 2 * 60 * 60 * 1000 : Date.now(),
    library: { revision: "auth-ui", scanned_at: 1, items: [], categories: [], warnings: [], libraries: [] },
    previews: [], operations, tasks: [],
    settings: { download_dir: "/srv/media", applied: {
      revision: "settings-auth", default_library_id: "main", default_category: "Films",
      libraries: [{ id: "main", name: "Main", root: "/srv/media", default_category: "Films", is_default: true }],
      ...(auth ? { bilibili_auth: auth } : {}),
    } },
  };
  const { manifest, chunks } = await makeChunkedSnapshot(snapshot);
  manifest.stale = stale;
  const responseChunks = new Map(chunks);
  const requests: Array<{ path: string; init?: RequestInit }> = [];
  const receipts: Record<string, unknown>[] = [];
  const html = await readFile(resolve(process.cwd(), "public/settings/index.html"), "utf8");
  document.body.innerHTML = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1] ?? "";
  originalConfirm = Object.getOwnPropertyDescriptor(window, "confirm");
  Object.defineProperty(window, "confirm", { configurable: true, writable: true, value: vi.fn(() => false) });
  Object.defineProperty(window, "Telegram", { configurable: true, value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } } });
  Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
  const stored = new Map<string, string>();
  vi.stubGlobal("localStorage", { getItem: (key: string) => stored.get(key) ?? null, setItem: (key: string, value: string) => stored.set(key, String(value)), removeItem: (key: string) => stored.delete(key) });
  originalWebSocket ??= Object.getOwnPropertyDescriptor(window, "WebSocket");
  vi.stubGlobal("WebSocket", MiniSocket as unknown as typeof WebSocket);
  Object.defineProperty(window, "WebSocket", { configurable: true, writable: true, value: MiniSocket });
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    requests.push(init === undefined ? { path } : { path, init });
    if (path === "/api/updates/session") return { ok: true, status: 200, json: async () => ({ ticket: "test-ticket", websocket_path: "/api/updates/ws", protocol: "miniapp-updates-v1", expires_at: Date.now() + 60_000, database_epoch: TEST_DATABASE_EPOCH }) } as Response;
    if (path === "/api/state") return { ok: true, status: 200, json: async () => manifest } as Response;
    const chunk = path.match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
    if (chunk) return { ok: true, status: 200, json: async () => ({ sha256: chunk[1], data: responseChunks.get(chunk[1]!) }) } as Response;
    if (path === "/api/actions") {
      const action = JSON.parse(String(init?.body)) as Record<string, unknown>;
      receipts.push(action);
      return { ok: true, status: 202, json: async () => ({ seq: 10 + receipts.length }) } as Response;
    }
    throw new Error("Unexpected API request");
  });
  vi.stubGlobal("fetch", fetchMock);
  window.fetch = fetchMock as typeof window.fetch;
  vi.resetModules();
  // @ts-expect-error JavaScript app entry is intentionally declaration-free.
  await import("../public/settings/app.js");
  await vi.waitFor(() => expect(document.querySelector("#app-content")?.classList.contains("hidden")).toBe(false));
  return { requests, receipts, stored };
}

afterEach(() => {
  window.dispatchEvent(new Event("pagehide"));
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  if (originalWebSocket) Object.defineProperty(window, "WebSocket", originalWebSocket);
  else delete (window as Window & { WebSocket?: typeof WebSocket }).WebSocket;
  originalWebSocket = undefined;
  if (originalConfirm) Object.defineProperty(window, "confirm", originalConfirm);
  else Reflect.deleteProperty(window, "confirm");
  originalConfirm = undefined;
  (window as Window & { happyDOM?: { cancelAsync(): void } }).happyDOM?.cancelAsync();
  document.body.innerHTML = "";
});

describe("Bilibili auth Settings flow", () => {
  it("keeps web, TV, access-key, QR, cookie, and access-key material out of Mini App requests and rendered state", async () => {
    const auth = {
      phase: "waiting_for_scan", mode: "web", generation: 7, credential_revision: 3,
      credential_summary: { has_cookie: false, has_access_key: false, has_tv_access_key: false },
      updated_at_ms: Date.now(), safe_reason: "none",
    };
    const localOnlyMaterials = [syntheticCredentials.bilibiliAuth.qrPayload.value, syntheticCredentials.bilibiliAuth.cookie.value, syntheticCredentials.bilibiliAuth.accessKey.value];
    const { requests, receipts, stored } = await boot(auth, [{ kind: "bilibili_auth", status: "failed", error: localOnlyMaterials[1], result: { error: localOnlyMaterials[2], qr_ticket: localOnlyMaterials[0] } }]);
    expect(document.querySelector("#bilibili-auth-phase")?.textContent).toBe("Waiting for QR scan");
    expect(document.querySelector("#bilibili-auth-detail")?.textContent).toContain("private Telegram chat");
    for (const mode of ["web", "tv", "access_key"]) {
      (document.querySelector('[data-bilibili-login="' + mode + '"]') as HTMLButtonElement).click();
      await vi.waitFor(() => expect(receipts.some((action) => (action.payload as Record<string, unknown>)?.mode === mode)).toBe(true));
    }
    (document.querySelector("#bilibili-auth-cancel") as HTMLButtonElement).click();
    await vi.waitFor(() => expect(receipts.some((action) => (action.payload as Record<string, unknown>)?.operation === "cancel")).toBe(true));
    expect(receipts.map((action) => action.kind)).toEqual(["bilibili_auth", "bilibili_auth", "bilibili_auth", "bilibili_auth"]);
    expect(receipts[0]).toMatchObject({ payload: { operation: "login", mode: "web" } });
    expect(receipts[1]).toMatchObject({ payload: { operation: "login", mode: "tv" } });
    expect(receipts[2]).toMatchObject({ payload: { operation: "login", mode: "access_key" } });
    expect(receipts[3]).toMatchObject({ payload: { operation: "cancel", generation: 7, credential_revision: 3 } });
    const externallyVisible = JSON.stringify({ requests: requests.filter(({ path }) => path === "/api/actions"), dom: document.body.textContent, html: document.body.innerHTML, storage: [...stored.values()] });
    for (const material of localOnlyMaterials) expect(externallyVisible).not.toContain(material);
  });

  it("requires a current saved-credential snapshot and explicit confirmation before generation-bound logout", async () => {
    const auth = {
      phase: "connected", mode: "tv", generation: 12, credential_revision: 19,
      credential_summary: { has_cookie: false, has_access_key: false, has_tv_access_key: true },
      updated_at_ms: Date.now(), safe_reason: "none",
    };
    const { receipts } = await boot(auth);
    const logout = document.querySelector("#bilibili-auth-logout") as HTMLButtonElement;
    expect(logout.disabled).toBe(false);
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(false);
    logout.click();
    expect(confirm).toHaveBeenCalledOnce();
    expect(receipts).toHaveLength(0);
    confirm.mockReturnValue(true);
    logout.click();
    await vi.waitFor(() => expect(receipts).toHaveLength(1));
    expect(receipts[0]).toMatchObject({ kind: "bilibili_auth", payload: { operation: "logout", generation: 12, credential_revision: 19 } });
    expect(document.querySelector("#bilibili-auth-account")?.textContent).toContain("TV credentials");
    expect(document.querySelector("#bilibili-auth-phase")?.textContent).toBe("Credentials saved");
    expect(document.querySelector("#bilibili-auth-detail")?.textContent).toContain("does not verify");
  });

  it.each(["expired", "cancelled", "failed"])("retains logout access to saved credentials after a %s login attempt", async (phase) => {
    const { receipts } = await boot({
      phase, mode: "web", generation: 14, credential_revision: 20,
      credential_summary: { has_cookie: true, has_access_key: false, has_tv_access_key: false },
      updated_at_ms: Date.now(), safe_reason: "none",
    });
    expect(document.querySelector("#bilibili-auth-account")?.textContent).toContain("web credentials");
    const logout = document.querySelector("#bilibili-auth-logout") as HTMLButtonElement;
    expect(logout.disabled).toBe(false);
    vi.spyOn(window, "confirm").mockReturnValue(true);
    logout.click();
    await vi.waitFor(() => expect(receipts).toHaveLength(1));
    expect(receipts[0]).toMatchObject({ kind: "bilibili_auth", payload: { operation: "logout", generation: 14, credential_revision: 20 } });
  });

  it("does not present an older connected snapshot as current or allow scoped actions from it", async () => {
    const auth = {
      phase: "connected", mode: "web", generation: 9, credential_revision: 2,
      credential_summary: { has_cookie: true, has_access_key: false, has_tv_access_key: false },
      updated_at_ms: Date.now() - 90_000, safe_reason: "none",
    };
    await boot(auth, [], true);
    expect(document.querySelector("#bilibili-auth-phase")?.textContent).toBe("Status unavailable");
    expect(document.querySelector("#bilibili-auth-detail")?.textContent).toContain("Refresh settings");
    expect((document.querySelector("#bilibili-auth-cancel") as HTMLButtonElement).disabled).toBe(true);
    expect((document.querySelector("#bilibili-auth-logout") as HTMLButtonElement).disabled).toBe(true);
  });

  it("shows unavailable for a pre-auth snapshot without inventing signed-out success", async () => {
    await boot();
    expect(document.querySelector("#bilibili-auth-phase")?.textContent).toBe("Status unavailable");
    expect(document.querySelector("#bilibili-auth-detail")?.textContent).toContain("unavailable");
    expect((document.querySelector("#bilibili-auth-logout") as HTMLButtonElement).disabled).toBe(true);
  });
});
