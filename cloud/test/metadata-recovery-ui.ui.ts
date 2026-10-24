import { readFile } from "node:fs/promises";
import { webcrypto } from "node:crypto";
import { resolve } from "node:path";
import { Event as HappyDOMEvent } from "happy-dom";
import type { HTMLSelectElement as HappyDOMSelectElement } from "happy-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { makeChunkedSnapshot, TEST_DATABASE_EPOCH } from "./snapshot-loader-fixtures.js";
import { syntheticCredentials } from "./fixtures/synthetic-credentials.js";

const LIBRARY_ID = "recovery-library";
const ROOT_BINDING = `v1:${"a".repeat(64)}`;
const MOCK_INIT_DATA = new URLSearchParams({ query_id: "metadata-recovery-ui", auth_date: "0" }).toString();
const UPDATE_PROTOCOL = "miniapp-updates-v1";
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

class MockWebSocket extends EventTarget {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  readonly protocols: string[];
  readonly protocol: string;
  readyState = MockWebSocket.CONNECTING;

  constructor(_url: string | URL, protocols?: string | string[]) {
    super();
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
  class AutoReadyWebSocket extends MockWebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols);
      queueMicrotask(() => this.openAndReady());
    }
  }
  originalWindowWebSocket ??= Object.getOwnPropertyDescriptor(window, "WebSocket");
  vi.stubGlobal("WebSocket", AutoReadyWebSocket as unknown as typeof WebSocket);
  Object.defineProperty(window, "WebSocket", { configurable: true, writable: true, value: AutoReadyWebSocket });
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
        name: "Recovery library",
        root: "/srv/media",
        root_binding: ROOT_BINDING,
        default_category: "",
        availability: "available",
        available: true,
        is_default: true,
      }],
    },
  },
  library: {
    revision: "recovery-ui-r1",
    scanned_at: Math.floor(Date.now() / 1000),
    root_label: "Recovery library",
    ready: true,
    default_library_id: LIBRARY_ID,
    items: [
      { id: "item-ready", title: "Ready video", relative_path: "Ready.mkv", presence: "present", bytes: 1024, metadata_status: "partial", confidence: "high" },
      { id: "item-choice", title: "Choice video", relative_path: "Choice.mkv", presence: "present", bytes: 2048, metadata_status: "partial", confidence: "low" },
      { id: "item-series", title: "Series video", relative_path: "Series/One.mkv", presence: "present", collection: { id: "series", title: "Series", kind: "series", order: 1, part_id: null } },
    ],
    categories: ["Series"],
    warnings: [],
    libraries: [{ id: LIBRARY_ID, availability: "available", available: true, is_default: true }],
  },
  pending_requests: [],
  tasks: [],
  operations: [],
  previews: [],
};

const firstReport = {
  revision: "recovery-ui-r1",
  total: 103,
  remaining: 3,
  next_cursor: "item-100",
  items: [
    { item_id: "item-ready", title: "Ready video", relative_path: "Ready.mkv", status: "ready", patch: { item_id: "item-ready", title: "Recovered title" } },
    { item_id: "item-choice", title: "Choice video", relative_path: "Choice.mkv", status: "needs_confirmation", candidates: [
      { label: "Series episode 1", patch: { item_id: "item-choice", collection: { id: "series", title: "Series", kind: "series", order: 1, part_id: null } } },
      { label: "Series episode 2", patch: { item_id: "item-choice", collection: { id: "series", title: "Series", kind: "series", order: 2, part_id: null } } },
    ] },
    { item_id: "item-unchanged", title: "Complete video", relative_path: "Complete.mkv", status: "unchanged", reason: "Metadata is already complete." },
    { item_id: "item-unavailable", title: "Offline video", relative_path: "Offline.mkv", status: "unavailable", reason: "The source could not be checked." },
    { item_id: "item-unsupported", title: "Unknown video", relative_path: "Unknown.mkv", status: "unsupported", reason: "No supported source ID was found." },
  ],
};

function element<T>(selector: string): T {
  const found = document.querySelector(selector);
  if (!found) throw new Error(`Expected element ${selector}`);
  return found as unknown as T;
}

async function waitForSubmittedAction(sentActions: Array<Record<string, any>>, kind: string) {
  await vi.waitFor(() => expect(sentActions.some((action) => action.kind === kind)).toBe(true));
}

async function waitForRecoveryResult(text: string) {
  await vi.waitFor(() => expect(element<HTMLElement>("#metadata-recovery-results").textContent).toContain(text));
}

async function waitForLocalActionQueueEmpty() {
  await vi.waitFor(() => {
    const queue = JSON.parse(localStorage.getItem("file-manager.actions.v1") ?? "[]");
    expect(queue).toHaveLength(0);
  });
}

async function mountApp(options: {
  initialSnapshot?: Record<string, any>;
  holdMetadataResponse?: boolean;
  blockThirdStateResponse?: boolean;
  deferRecoveryPreviewSnapshot?: boolean;
} = {}) {
  const {
    initialSnapshot = ownerSnapshot,
    holdMetadataResponse = false,
    blockThirdStateResponse = false,
    deferRecoveryPreviewSnapshot = false,
  } = options;
  vi.useFakeTimers();
  vi.stubGlobal("crypto", webcrypto);
  installMockWebSocket();
  const html = await readFile(resolve(process.cwd(), "public/file/index.html"), "utf8");
  const body = html.match(/<body>([\s\S]*?)<\/body>/iu)?.[1];
  if (!body) throw new Error("File manager body was not found");
  document.body.innerHTML = body;
  Object.defineProperty(window, "Telegram", {
    configurable: true,
    value: { WebApp: { initData: MOCK_INIT_DATA, ready: vi.fn(), expand: vi.fn() } },
  });
  Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
  vi.stubGlobal("navigator", window.navigator);
  Object.defineProperty(window, "matchMedia", { configurable: true, value: vi.fn(() => ({ matches: true })) });
  vi.stubGlobal("localStorage", makeMemoryStorage());

  let currentManifest: Record<string, any>;
  const responseChunks = new Map<string, string>();
  const initial = await makeChunkedSnapshot(initialSnapshot);
  currentManifest = initial.manifest;
  for (const [digest, data] of initial.chunks) responseChunks.set(digest, data);
  const sentActions: Array<Record<string, any>> = [];
  let actionSeq = 1;
  let previousRecoveryOperation: Record<string, any> | null = null;
  let stateRequests = 0;
  let releaseMetadataResponse: (() => void) | null = null;
  let releaseThirdStateResponse: (() => void) | null = null;
  const metadataResponseGate = holdMetadataResponse
    ? new Promise<void>((resolveGate) => { releaseMetadataResponse = resolveGate; })
    : Promise.resolve();
  const thirdStateResponseGate = blockThirdStateResponse
    ? new Promise<void>((resolveGate) => { releaseThirdStateResponse = resolveGate; })
    : Promise.resolve();

  async function replaceSnapshot(snapshot: Record<string, any>) {
    const next = await makeChunkedSnapshot(snapshot);
    currentManifest = next.manifest;
    for (const [digest, data] of next.chunks) responseChunks.set(digest, data);
  }

  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    if (String(input) === "/api/updates/session") {
      expect(new Headers(init?.headers).get("X-Telegram-Init-Data")).toBe(MOCK_INIT_DATA);
      return { ok: true, status: 200, json: async () => ({
        ticket: syntheticCredentials.localSharedSecret.value,
        websocket_path: "/api/updates/ws",
        protocol: UPDATE_PROTOCOL,
        expires_at: Date.now() + 60 * 60 * 1_000,
        database_epoch: TEST_DATABASE_EPOCH,
      }) } as Response;
    }
    if (String(input) === "/api/state") {
      expect(new Headers(init?.headers).get("X-Snapshot-Format")).toBe("chunked-v1");
      stateRequests += 1;
      if (blockThirdStateResponse && stateRequests === 3) await thirdStateResponseGate;
      return { ok: true, status: 200, json: async () => currentManifest } as Response;
    }
    const chunkPath = String(input).match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
    if (chunkPath) {
      expect(new Headers(init?.headers).get("X-Cloud-Database-Epoch")).toBe(TEST_DATABASE_EPOCH);
      const digest = chunkPath[1]!;
      return { ok: true, status: 200, json: async () => ({ sha256: digest, data: responseChunks.get(digest) }) } as Response;
    }
    if (String(input) === "/api/actions") {
      const action = JSON.parse(String(init?.body)) as Record<string, any>;
      sentActions.push(action);
      if (action.kind === "metadata_recover") {
        const recovery = action.payload.after_item_id === "item-100"
          ? { revision: "recovery-ui-r1", total: 103, remaining: 0, next_cursor: null, items: [
            { item_id: "item-next", title: "Next batch video", relative_path: "Next.mkv", status: "ready", patch: { item_id: "item-next", source_ids: ["BVnext"] } },
          ] }
          : firstReport;
        const operation = {
          seq: actionSeq,
          kind: "metadata_recover",
          status: "completed",
          error: null,
          recovery_identity: {
            library_id: LIBRARY_ID,
            root_binding: ROOT_BINDING,
            scope: action.payload.scope,
          },
          result: { scope: action.payload.scope, library_id: LIBRARY_ID, root_binding: ROOT_BINDING, recovery },
        };
        await replaceSnapshot({
          ...ownerSnapshot,
          state_version: actionSeq + 1,
          operations: [
            ...(previousRecoveryOperation ? [previousRecoveryOperation] : []),
            operation,
            { kind: "snapshot_applied_marker", status: "completed", result: {} },
          ],
        });
        previousRecoveryOperation = operation;
        await metadataResponseGate;
      }
      if (action.kind === "file_preview") {
        if (!deferRecoveryPreviewSnapshot || !action.payload.expected_revision) {
          await replaceSnapshot({
            ...ownerSnapshot,
            state_version: actionSeq + 1,
            operations: [
              ...(previousRecoveryOperation ? [previousRecoveryOperation] : []),
              {
                seq: actionSeq,
                kind: "file_preview",
                status: "awaiting_confirmation",
                result: {
                  library_id: LIBRARY_ID,
                  root_binding: ROOT_BINDING,
                  preview_id: "recovery-preview-2",
                  revision: "recovery-preview-r1",
                  preview: {
                    metadata_patches: [{
                      item_id: "item-ready",
                      changed: true,
                      changes: [{ field: "title", before: "Original title", after: "Recovered title" }],
                    }],
                  },
                },
              },
            ],
          });
        }
      }
      const seq = actionSeq++;
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
  await vi.waitFor(() => {
    expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false);
    expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
  });
  return {
    sentActions,
    replaceSnapshot,
    get stateRequests() { return stateRequests; },
    releaseMetadataActionResponse: () => releaseMetadataResponse?.(),
    releaseThirdStateResponse: () => releaseThirdStateResponse?.(),
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
  (window as Window & { happyDOM?: { cancelAsync(): void } }).happyDOM?.cancelAsync();
  document.body.innerHTML = "";
});

describe("metadata recovery Mini App flow", () => {
  it("analyzes grouped results, previews chosen candidates at the report revision, and follows the next cursor", async () => {
    const { sentActions } = await mountApp();
    element<HTMLButtonElement>('[data-view="needs"]').click();
    element<HTMLButtonElement>("#recover-library").click();
    await waitForRecoveryResult("Choose a match");
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).toContain("The source could not be checked.");
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).toContain("No supported source ID was found.");

    const candidate = element<HappyDOMSelectElement>('[data-recovery-candidate="item-choice"]');
    candidate.value = "1";
    candidate.dispatchEvent(new HappyDOMEvent("change", { bubbles: true }));
    element<HTMLInputElement>('[data-recovery-item="item-ready"]').click();
    element<HTMLInputElement>('[data-recovery-item="item-choice"]').click();
    element<HTMLButtonElement>("[data-recovery-preview]").click();
    await waitForSubmittedAction(sentActions, "file_preview");
    const preview = sentActions.find((action) => action.kind === "file_preview");
    expect(preview?.payload.expected_revision).toBe("recovery-ui-r1");
    expect(preview?.payload.metadata_patches).toEqual([
      { item_id: "item-ready", title: "Recovered title", fill_missing_only: true },
      { item_id: "item-choice", collection: { id: "series", title: "Series", kind: "series", order: 2, part_id: null }, fill_missing_only: true },
    ]);
    expect(element<HTMLButtonElement>('[data-view="organizing"]').classList.contains("active")).toBe(true);
    expect(element<HTMLElement>("#preview-list").textContent).toContain("Recovery preview is being prepared");
    await vi.waitFor(() => expect(document.querySelector('[data-confirm="recovery-preview-2"]')).not.toBeNull());
    expect(element<HTMLElement>("#preview-list").textContent).toContain("Recovered title");
    element<HTMLButtonElement>('[data-confirm="recovery-preview-2"]').click();
    await vi.waitFor(() => expect(sentActions.some((action) => action.kind === "file_confirm")).toBe(true));
    await waitForLocalActionQueueEmpty();
    expect(sentActions.find((action) => action.kind === "file_confirm")?.payload).toMatchObject({
      preview_id: "recovery-preview-2",
      revision: "recovery-preview-r1",
      library_id: LIBRARY_ID,
    });
    element<HTMLButtonElement>('[data-view="needs"]').click();

    element<HTMLButtonElement>("[data-recovery-next]").click();
    await vi.waitFor(() => expect(sentActions.filter((action) => action.kind === "metadata_recover")).toHaveLength(2));
    const analyses = sentActions.filter((action) => action.kind === "metadata_recover");
    expect(analyses[1]?.payload).toMatchObject({ scope: "library", after_item_id: "item-100" });
    await waitForRecoveryResult("Next batch video");
    await waitForLocalActionQueueEmpty();
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).not.toContain("Series episode 1");
    expect(firstReport.items[1]?.candidates?.[0]?.label).toBe("Series episode 1");

    element<HTMLButtonElement>("[data-recovery-clear]").click();
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).not.toContain("Next batch video");
  });

  it("clears recovery candidates when the library revision changes", async () => {
    const app = await mountApp({
      initialSnapshot: {
        ...ownerSnapshot,
        operations: [{
          seq: 1,
          kind: "metadata_recover",
          status: "completed",
          result: { scope: "library", library_id: LIBRARY_ID, root_binding: ROOT_BINDING, recovery: firstReport },
        }],
      },
    });
    const { replaceSnapshot } = app;
    element<HTMLButtonElement>('[data-view="needs"]').click();
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).toContain("Ready video");
    await replaceSnapshot({
      ...ownerSnapshot,
      state_version: 2,
      library: {
        ...ownerSnapshot.library,
        revision: "recovery-ui-r2",
        items: ownerSnapshot.library.items.map((item) => item.id === "item-ready"
          ? { ...item, title: "Revision updated marker" }
          : item),
      },
      operations: [{
        seq: 1,
        kind: "metadata_recover",
        status: "completed",
        result: { scope: "library", library_id: LIBRARY_ID, root_binding: ROOT_BINDING, recovery: firstReport },
      }],
    });
    const requestsBeforeRefresh = app.stateRequests;
    element<HTMLButtonElement>("#refresh-button").click();
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.waitFor(() => expect(app.stateRequests).toBeGreaterThan(requestsBeforeRefresh));
    await vi.waitFor(() => expect(element<HTMLElement>("#needs-list").textContent).toContain("Revision updated marker"));
    await vi.waitFor(() => expect(element<HTMLElement>("#recovery-status").textContent).toContain("library changed"));
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).not.toContain("Ready video");
  });

  it("retries only deferred rows from a current selected report without library selection state", async () => {
    const deferredReport = {
      revision: "recovery-ui-r1",
      total: 1,
      remaining: 0,
      next_cursor: null,
      items: [{
        item_id: "item-deferred",
        title: "Deferred video",
        relative_path: "Deferred.mkv",
        status: "deferred",
        reason: "The lookup budget ended before this item was analyzed.",
      }],
    };
    const { sentActions } = await mountApp({
      initialSnapshot: {
        ...ownerSnapshot,
        operations: [{
          seq: 50,
          kind: "metadata_recover",
          status: "completed",
          recovery_identity: { library_id: LIBRARY_ID, root_binding: ROOT_BINDING, scope: "selected" },
          result: {
            scope: "selected",
            library_id: LIBRARY_ID,
            root_binding: ROOT_BINDING,
            recovery: deferredReport,
          },
        }],
      },
    });
    element<HTMLButtonElement>('[data-view="needs"]').click();
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).toContain("Deferred · analyze again");
    element<HTMLButtonElement>("[data-recovery-retry-deferred]").click();
    await waitForSubmittedAction(sentActions, "metadata_recover");
    expect(sentActions.find((action) => action.kind === "metadata_recover")?.payload).toEqual({
      library_id: LIBRARY_ID,
      root_binding: ROOT_BINDING,
      scope: "selected",
      item_ids: ["item-deferred"],
    });
    await waitForLocalActionQueueEmpty();
    await waitForRecoveryResult("Ready video");
  });

  it("keeps a failed recovery preview visible after a same-root revision change", async () => {
    const { sentActions, replaceSnapshot } = await mountApp({ deferRecoveryPreviewSnapshot: true });
    element<HTMLButtonElement>('[data-view="needs"]').click();
    element<HTMLButtonElement>("#recover-library").click();
    await waitForRecoveryResult("Choose a match");
    element<HTMLInputElement>('[data-recovery-item="item-ready"]').click();
    element<HTMLButtonElement>("[data-recovery-preview]").click();
    expect(element<HTMLButtonElement>('[data-view="organizing"]').classList.contains("active")).toBe(true);
    await vi.waitFor(() => expect(element<HTMLElement>("#preview-list").textContent).toContain("was submitted. Waiting for your computer to prepare the preview."));
    const sequenceMatch = element<HTMLElement>("#preview-list").textContent?.match(/recovery preview request #(\d+) was submitted/iu);
    expect(sequenceMatch).not.toBeNull();
    const previewSequence = Number(sequenceMatch?.[1]);
    await waitForLocalActionQueueEmpty();
    await replaceSnapshot({
      ...ownerSnapshot,
      state_version: previewSequence + 1,
      library: { ...ownerSnapshot.library, revision: "recovery-ui-r2" },
      operations: [{
        seq: previewSequence,
        kind: "file_preview",
        status: "failed",
        error: "The library changed while the preview was being prepared.",
        result: { library_id: LIBRARY_ID, root_binding: ROOT_BINDING },
      }],
    });
    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(element<HTMLElement>("#preview-list").textContent).toContain(`Recovery preview failed · request #${previewSequence}`));
    expect(element<HTMLElement>("#preview-list").textContent).toContain("The library changed while the preview was being prepared.");
    expect(sentActions.some((action) => action.kind === "file_preview" && action.payload.expected_revision === "recovery-ui-r1")).toBe(true);
  });

  it("routes explicit standalone changes through the ordinary metadata preview request", async () => {
    const { sentActions } = await mountApp();
    element<HTMLButtonElement>('[data-details="item-ready"]').click();
    expect(element<HTMLElement>(".detail-panel").textContent).toContain("Standalone status only clears the missing-collection check");
    element<HTMLButtonElement>('[data-organization-status="standalone"]').click();
    await waitForSubmittedAction(sentActions, "file_preview");
    const preview = sentActions.find((action) => action.kind === "file_preview");
    expect(preview?.payload).toMatchObject({
      library_id: LIBRARY_ID,
      root_binding: ROOT_BINDING,
      item_ids: ["item-ready"],
      metadata_patches: [{ item_id: "item-ready", organization_status: "standalone" }],
    });
    expect(preview?.payload.expected_revision).toBeUndefined();
    expect(preview?.payload.metadata_patches[0].fill_missing_only).toBeUndefined();

    element<HTMLButtonElement>('[data-details="item-series"]').click();
    expect(element<HTMLElement>(".detail-panel").textContent).toContain("Existing collections cannot be declared standalone");
    expect(document.querySelector('[data-organization-status="standalone"][data-organization-item="item-series"]')).toBeNull();
    await waitForLocalActionQueueEmpty();
  });

  it.each([
    ["processing", "Metadata analysis is queued or running on your computer."],
    ["failed", "Analysis failed: Latest analysis failed."],
  ])("restores the newest %s intent after reload with an empty browser queue", async (latestStatus, expectedStatus) => {
    const latestOperation = {
      seq: 2,
      kind: "metadata_recover",
      status: latestStatus,
      created_at: 2,
      updated_at: 2,
      attempts: 0,
      next_attempt_at: null,
      error: latestStatus === "failed" ? "Latest analysis failed." : null,
      result: null,
      recovery_identity: { library_id: LIBRARY_ID, root_binding: ROOT_BINDING, scope: "library" },
    };
    expect(latestOperation.result).toBeNull();
    expect(latestOperation).not.toHaveProperty("payload");
    const initialSnapshot = {
      ...ownerSnapshot,
      operations: [
        {
          seq: 1,
          kind: "metadata_recover",
          status: "completed",
          recovery_identity: { library_id: LIBRARY_ID, root_binding: ROOT_BINDING, scope: "library" },
          result: { scope: "library", library_id: LIBRARY_ID, root_binding: ROOT_BINDING, recovery: firstReport },
        },
        latestOperation,
      ],
    };
    await mountApp({ initialSnapshot });
    expect(JSON.parse(localStorage.getItem("file-manager.actions.v1") ?? "[]")).toEqual([]);
    element<HTMLButtonElement>('[data-view="needs"]').click();
    expect(element<HTMLElement>("#recovery-status").textContent).toContain(expectedStatus);
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).not.toContain("Ready video");
  });

  it.each([
    ["a different library", { library_id: "other-library", root_binding: ROOT_BINDING, scope: "library" }],
    ["a different root binding", { library_id: LIBRARY_ID, root_binding: `v1:${"b".repeat(64)}`, scope: "library" }],
  ])("does not let a newer intent for %s block this library's completed report", async (_label, recoveryIdentity) => {
    const initialSnapshot = {
      ...ownerSnapshot,
      operations: [
        {
          seq: 1,
          kind: "metadata_recover",
          status: "completed",
          recovery_identity: { library_id: LIBRARY_ID, root_binding: ROOT_BINDING, scope: "library" },
          result: { scope: "library", library_id: LIBRARY_ID, root_binding: ROOT_BINDING, recovery: firstReport },
        },
        {
          seq: 2,
          kind: "metadata_recover",
          status: "processing",
          created_at: 3,
          updated_at: 3,
          attempts: 0,
          next_attempt_at: null,
          error: null,
          result: null,
          recovery_identity: recoveryIdentity,
        },
      ],
    };
    await mountApp({ initialSnapshot });
    expect(JSON.parse(localStorage.getItem("file-manager.actions.v1") ?? "[]")).toEqual([]);
    element<HTMLButtonElement>('[data-view="needs"]').click();
    expect(element<HTMLElement>("#recovery-status").textContent).toContain("Analysis revision recovery-ui-r1");
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).toContain("Ready video");
  });

  it("does not use a completed report whose identity conflicts with its recovery receipt", async () => {
    const initialSnapshot = {
      ...ownerSnapshot,
      operations: [{
        seq: 1,
        kind: "metadata_recover",
        status: "completed",
        recovery_identity: { library_id: LIBRARY_ID, root_binding: ROOT_BINDING, scope: "library" },
        result: {
          scope: "library",
          library_id: LIBRARY_ID,
          root_binding: `v1:${"b".repeat(64)}`,
          recovery: firstReport,
        },
      }],
    };
    await mountApp({ initialSnapshot });
    expect(JSON.parse(localStorage.getItem("file-manager.actions.v1") ?? "[]")).toEqual([]);
    element<HTMLButtonElement>('[data-view="needs"]').click();
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).not.toContain("Ready video");
    expect(element<HTMLElement>("#recovery-status").textContent).toContain("library changed");
  });

  it.each([
    ["foreign library", { library_id: "other-library", root_binding: ROOT_BINDING, scope: "library" }],
    ["foreign root", { library_id: LIBRARY_ID, root_binding: `v1:${"b".repeat(64)}`, scope: "library" }],
    ["invalid scope", { library_id: LIBRARY_ID, root_binding: ROOT_BINDING, scope: "other" }],
  ])("fails closed on a %s receipt identity even when its result looks local", async (_label, recoveryIdentity) => {
    const initialSnapshot = {
      ...ownerSnapshot,
      operations: [{
        seq: 1,
        kind: "metadata_recover",
        status: "completed",
        recovery_identity: recoveryIdentity,
        result: { scope: "library", library_id: LIBRARY_ID, root_binding: ROOT_BINDING, recovery: firstReport },
      }],
    };
    await mountApp({ initialSnapshot });
    expect(JSON.parse(localStorage.getItem("file-manager.actions.v1") ?? "[]")).toEqual([]);
    element<HTMLButtonElement>('[data-view="needs"]').click();
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).not.toContain("Ready video");
  });

  it("applies a cached result as soon as the accepted action receipt binds its sequence", async () => {
    const app = await mountApp({
      holdMetadataResponse: true,
      blockThirdStateResponse: true,
    });
    const { sentActions, releaseMetadataActionResponse, releaseThirdStateResponse } = app;
    element<HTMLButtonElement>('[data-view="needs"]').click();
    element<HTMLButtonElement>("#recover-library").click();
    await waitForSubmittedAction(sentActions, "metadata_recover");

    element<HTMLButtonElement>("#refresh-button").click();
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.waitFor(() => expect(app.stateRequests).toBe(2));
    // A started `/api/state` fetch is not enough: wait until the refreshed snapshot has
    // been verified, accepted, and rendered before returning the held action receipt.
    await vi.waitFor(() => expect(element<HTMLElement>("#operation-list").textContent).toContain("Snapshot applied marker"));
    await vi.waitFor(() => expect(element<HTMLElement>("#recovery-status").textContent).toContain("queued or running"));
    expect(element<HTMLElement>("#metadata-recovery-results").textContent).not.toContain("Recovered title");

    releaseMetadataActionResponse();
    await vi.waitFor(() => expect(element<HTMLElement>("#metadata-recovery-results").textContent).toContain("Recovered title"));
    await vi.waitFor(() => expect(app.stateRequests).toBe(3));
    releaseThirdStateResponse();
    await waitForLocalActionQueueEmpty();
  });
});
