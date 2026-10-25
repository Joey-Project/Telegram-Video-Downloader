import { readFile } from "node:fs/promises";
import { webcrypto } from "node:crypto";
import { resolve } from "node:path";
import { Event as HappyDOMEvent } from "happy-dom";
import type { HTMLButtonElement as HappyDOMButtonElement, HTMLSelectElement as HappyDOMSelectElement } from "happy-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { makeChunkedSnapshot, TEST_DATABASE_EPOCH } from "./snapshot-loader-fixtures.js";
import { syntheticCredentials } from "./fixtures/synthetic-credentials.js";
import {
  ACTION_REPLAY_WINDOW_MS,
  ACTION_RECEIPT_STORAGE_PREFIX,
  ACTION_SURFACE_FILE,
  createQueuedAction,
} from "../public/file/action-queue.js";
import {
  PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY,
  PREVIEW_CANCELLATION_STORAGE_KEY,
} from "../public/file/preview-cancellation-state.js";

const LIBRARY_ID = "test-library";
const ROOT_BINDING = `v1:${"a".repeat(64)}`;
const MIGRATION_TARGET_ID = "migration-target-library";
const MIGRATION_TARGET_BINDING = `v1:${"b".repeat(64)}`;
const MIGRATION_PREVIEW_ID = "migration-cancel-preview";
const MIGRATION_PREVIEW_REVISION = "migration-cancel-r1";
const MIGRATION_PREVIEW_SEQ = 801;
const MIGRATION_CONFLICT_ITEM_ID = "conflict-bundle";
const FILE_CANCEL_PREVIEW_ID = "file-history-cancel-preview";
const FILE_CANCEL_PREVIEW_REVISION = "file-history-cancel-r1";
const FILE_CANCEL_PREVIEW_SEQ = 802;
const FILE_CANCEL_REQUEST_ID = "00000000-0000-4000-8000-000000000711";
const MIGRATION_CANCEL_REQUEST_ID = "00000000-0000-4000-8000-000000000712";
const FILE_CANCEL_REQUEST_SEQ = 903;
const MIGRATION_CANCEL_REQUEST_SEQ = 902;
const FILE_BUSY_PREVIEW_ID = "file-busy-cancel-preview";
const FILE_BUSY_PREVIEW_REVISION = "file-busy-cancel-r1";
const FILE_BUSY_PREVIEW_SEQ = 803;
const FILE_BUSY_CANCEL_REQUEST_ID = "00000000-0000-4000-8000-000000000713";
const RECEIPT_FAILURE_PREVIEW_ID = "file-receipt-failure-preview";
const RECEIPT_FAILURE_PREVIEW_REVISION = "file-receipt-failure-r1";
const RECEIPT_FAILURE_PREVIEW_SEQ = 804;
const SNAPSHOT_REFRESH_START_BUDGET_MS = 1_000;
const MOCK_REPORTED_AT_MS = Date.now();
const MOCK_INIT_DATA = new URLSearchParams({ query_id: "file-manager-ui-flow", auth_date: "0" }).toString();
const UPDATE_PROTOCOL = "miniapp-updates-v1";
const activeMockSockets: MockWebSocket[] = [];

type MockActionResponse = { status: number; body: Record<string, unknown> };
type MigrationPendingSnapshot = Record<string, any> & {
  state_version: number;
  pending_requests: Record<string, any>[];
};

let originalWindowWebSocket: PropertyDescriptor | undefined;
const originalWindowAddEventListener = window.addEventListener;
const originalWindowAddEventListenerDescriptor = Object.getOwnPropertyDescriptor(window, "addEventListener");
const capturedWindowListeners: Array<{
  type: string;
  listener: EventListenerOrEventListenerObject;
  options: boolean | AddEventListenerOptions | undefined;
}> = [];
const mountedSnapshotUpdates: Array<{ stop(): void }> = [];
type FixtureWindowTimerApi = {
  setTimeout(handler: TimerHandler, timeout?: number, ...args: any[]): number;
  clearTimeout(handle?: number): void;
  setInterval(handler: TimerHandler, timeout?: number, ...args: any[]): number;
  clearInterval(handle?: number): void;
};
let fixtureWindowTimerTracking: {
  clear(): void;
  restore(): void;
} | null = null;
let settleCurrentFixtureActions: (() => Promise<void>) | null = null;

function stopMountedSnapshotUpdates() {
  for (const updates of mountedSnapshotUpdates.splice(0)) updates.stop();
}

function installSnapshotUpdatesFactoryCapture() {
  vi.doMock("../public/file/snapshot-updates.js", async (importOriginal) => {
    const original = await importOriginal<typeof import("../public/file/snapshot-updates.js")>();
    return {
      ...original,
      createSnapshotUpdates: (...args: Parameters<typeof original.createSnapshotUpdates>) => {
        const updates = original.createSnapshotUpdates(...args);
        mountedSnapshotUpdates.push(updates);
        return updates;
      },
    };
  });
}

function clearFixtureWindowTimers() {
  const tracking = fixtureWindowTimerTracking;
  fixtureWindowTimerTracking = null;
  if (!tracking) return;
  tracking.clear();
  tracking.restore();
}

async function fixtureTaskBoundary() {
  const task = new Promise<void>((resolve) => window.setTimeout(resolve, 0));
  if (vi.isFakeTimers()) await vi.advanceTimersByTimeAsync(0);
  await task;
}

function deferredSignal() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => { release = resolve; });
  return { promise, release };
}

async function releaseMountedFixture() {
  stopMountedSnapshotUpdates();
  const settleActions = settleCurrentFixtureActions;
  settleCurrentFixtureActions = null;
  if (settleActions) await settleActions();
  clearFixtureWindowTimers();
  clearCapturedWindowListeners();
}

function installFixtureWindowTimerTracking() {
  clearFixtureWindowTimers();
  const descriptors = {
    setTimeout: Object.getOwnPropertyDescriptor(window, "setTimeout"),
    clearTimeout: Object.getOwnPropertyDescriptor(window, "clearTimeout"),
    setInterval: Object.getOwnPropertyDescriptor(window, "setInterval"),
    clearInterval: Object.getOwnPropertyDescriptor(window, "clearInterval"),
  };
  const timerApi = window as unknown as FixtureWindowTimerApi;
  const originalSetTimeout = timerApi.setTimeout.bind(window);
  const originalClearTimeout = timerApi.clearTimeout.bind(window);
  const originalSetInterval = timerApi.setInterval.bind(window);
  const originalClearInterval = timerApi.clearInterval.bind(window);
  const timeouts = new Set<number>();
  const intervals = new Set<number>();
  const setTimeout = ((handler: TimerHandler, timeout?: number, ...args: any[]) => {
    let handle: number;
    if (typeof handler === "function") {
      handle = originalSetTimeout((...callbackArgs: any[]) => {
        timeouts.delete(handle);
        handler(...callbackArgs);
      }, timeout, ...args);
    } else {
      handle = originalSetTimeout(handler, timeout, ...args);
    }
    timeouts.add(handle);
    return handle;
  }) as unknown as typeof window.setTimeout;
  const clearTimeout = ((handle?: number) => {
    if (handle !== undefined) timeouts.delete(handle);
    originalClearTimeout(handle);
  }) as unknown as typeof window.clearTimeout;
  const setInterval = ((handler: TimerHandler, timeout?: number, ...args: any[]) => {
    const handle = originalSetInterval(handler, timeout, ...args);
    intervals.add(handle);
    return handle;
  }) as unknown as typeof window.setInterval;
  const clearInterval = ((handle?: number) => {
    if (handle !== undefined) intervals.delete(handle);
    originalClearInterval(handle);
  }) as unknown as typeof window.clearInterval;
  Object.defineProperties(window, {
    setTimeout: { configurable: true, writable: true, value: setTimeout },
    clearTimeout: { configurable: true, writable: true, value: clearTimeout },
    setInterval: { configurable: true, writable: true, value: setInterval },
    clearInterval: { configurable: true, writable: true, value: clearInterval },
  });
  fixtureWindowTimerTracking = {
    clear() {
      for (const handle of timeouts) originalClearTimeout(handle);
      for (const handle of intervals) originalClearInterval(handle);
      timeouts.clear();
      intervals.clear();
    },
    restore() {
      for (const [key, descriptor] of Object.entries(descriptors)) {
        if (descriptor) Object.defineProperty(window, key, descriptor);
        else Reflect.deleteProperty(window, key);
      }
    },
  };
}

function clearCapturedWindowListeners() {
  for (const { type, listener, options } of capturedWindowListeners.splice(0)) {
    (window as EventTarget).removeEventListener(type, listener, options);
  }
}

function captureMountedAppWindowListeners() {
  clearCapturedWindowListeners();
  Object.defineProperty(window, "addEventListener", {
    configurable: true,
    writable: true,
    value(type: string, listener: EventListenerOrEventListenerObject | null, options?: boolean | AddEventListenerOptions) {
      if (listener && ["storage", "online", "keydown"].includes(type)) {
        capturedWindowListeners.push({ type, listener, options });
      }
      const add = originalWindowAddEventListener as unknown as (
        type: string,
        callback: EventListenerOrEventListenerObject | null,
        settings?: boolean | AddEventListenerOptions,
      ) => void;
      return add.call(window, type, listener, options);
    },
  });
}

function restoreWindowAddEventListener() {
  clearCapturedWindowListeners();
  if (originalWindowAddEventListenerDescriptor) {
    Object.defineProperty(window, "addEventListener", originalWindowAddEventListenerDescriptor);
  } else {
    Reflect.deleteProperty(window, "addEventListener");
  }
}

class MockWebSocket extends EventTarget {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  readonly url: string;
  readonly protocols: string[];
  readonly protocol: string;
  readyState = MockWebSocket.CONNECTING;
  sentFrames: Record<string, any>[] = [];
  watch = false;
  pendingSnapshotVersion = 0;
  currentStateVersion: () => number = () => 1;
  currentDatabaseEpoch: () => string = () => TEST_DATABASE_EPOCH;

  constructor(url: string | URL, protocols?: string | string[]) {
    super();
    this.url = String(url);
    this.protocols = Array.isArray(protocols) ? [...protocols] : protocols ? [protocols] : [];
    this.protocol = this.protocols[0] ?? "";
  }

  openAndReady() {
    if (this.readyState !== MockWebSocket.CONNECTING) return;
    this.readyState = MockWebSocket.OPEN;
    this.emit("open");
    this.receive({ type: "ready", database_epoch: this.currentDatabaseEpoch(), snapshot_control: "v1" });
  }

  send(data: string) {
    const message = JSON.parse(data) as Record<string, any>;
    this.sentFrames.push(message);
    if (message.type === "interest") {
      this.watch = message.watch === true && message.visible === true;
      if (this.watch && this.pendingSnapshotVersion > 0) {
        const stateVersion = this.pendingSnapshotVersion;
        this.pendingSnapshotVersion = 0;
        this.receive({ type: "invalidate", database_epoch: this.currentDatabaseEpoch(), state_version: stateVersion, reason: "state" });
      }
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

  publishSnapshot(stateVersion: number) {
    if (this.watch) {
      this.receive({ type: "invalidate", database_epoch: this.currentDatabaseEpoch(), state_version: stateVersion, reason: "state" });
    } else {
      this.pendingSnapshotVersion = Math.max(this.pendingSnapshotVersion, stateVersion);
    }
  }

  receive(message: Record<string, unknown>) {
    if (this.readyState !== MockWebSocket.OPEN) return;
    const event = new Event("message");
    Object.defineProperty(event, "data", { configurable: true, value: JSON.stringify(message) });
    this.dispatchEvent(event);
  }

  close(code = 1000, reason = "") {
    if (this.readyState === MockWebSocket.CLOSING || this.readyState === MockWebSocket.CLOSED) return;
    this.readyState = MockWebSocket.CLOSING;
    this.readyState = MockWebSocket.CLOSED;
    const event = new Event("close");
    Object.defineProperties(event, {
      code: { configurable: true, value: code },
      reason: { configurable: true, value: reason },
      wasClean: { configurable: true, value: true },
    });
    this.dispatchEvent(event);
  }

  private emit(type: string) {
    this.dispatchEvent(new Event(type));
  }
}

function latestOpenSocket(sockets: MockWebSocket[]) {
  return [...sockets].reverse().find((socket) => socket.readyState === MockWebSocket.OPEN && socket.protocol === UPDATE_PROTOCOL);
}

function installMockWebSocket(
  currentDatabaseEpoch: () => string = () => TEST_DATABASE_EPOCH,
  currentStateVersion: () => number = () => 1,
) {
  const sockets: MockWebSocket[] = [];
  class AutoReadyWebSocket extends MockWebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols);
      this.currentDatabaseEpoch = currentDatabaseEpoch;
      this.currentStateVersion = currentStateVersion;
      sockets.push(this);
      activeMockSockets.push(this);
      queueMicrotask(() => this.openAndReady());
    }
  }
  originalWindowWebSocket ??= Object.getOwnPropertyDescriptor(window, "WebSocket");
  vi.stubGlobal("WebSocket", AutoReadyWebSocket as unknown as typeof WebSocket);
  Object.defineProperty(window, "WebSocket", { configurable: true, writable: true, value: AutoReadyWebSocket });
  return sockets;
}

function publishMockSnapshot(stateVersion: number) {
  for (const socket of activeMockSockets) socket.publishSnapshot(stateVersion);
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

function controlledSnapshotClock() {
  const realNow = Date.now.bind(Date);
  let offset = 0;
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + offset);
  return {
    advanceRefreshBudget() {
      // Match the client's one-second minimum refresh-start interval without waiting on wall time.
      offset += SNAPSHOT_REFRESH_START_BUDGET_MS;
    },
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

function migrationPreviewSnapshot() {
  const sourceLibrary = {
    id: LIBRARY_ID,
    name: "Test library",
    root: "/srv/media",
    root_binding: ROOT_BINDING,
    default_category: "Films",
    availability: "available",
    available: true,
    is_default: true,
  };
  const targetLibrary = {
    id: MIGRATION_TARGET_ID,
    name: "Migration target",
    root: "/srv/target",
    root_binding: MIGRATION_TARGET_BINDING,
    default_category: "Films",
    availability: "available",
    available: true,
  };
  const sourceScan = { ...ownerSnapshot.library, libraries: [], migrations: [] };
  const targetScan = {
    ...ownerSnapshot.library,
    revision: "migration-target-revision",
    root_label: "Migration target",
    libraries: [],
    migrations: [],
  };
  const preview = {
    seq: MIGRATION_PREVIEW_SEQ,
    request_id: "migration-preview-request",
    kind: "library_migration_preview",
    status: "awaiting_confirmation",
    preview_id: MIGRATION_PREVIEW_ID,
    revision: MIGRATION_PREVIEW_REVISION,
    result: {
      id: MIGRATION_PREVIEW_ID,
      revision: MIGRATION_PREVIEW_REVISION,
      source_library_id: LIBRARY_ID,
      target_library_id: MIGRATION_TARGET_ID,
      source_root_binding: ROOT_BINDING,
      target_root_binding: MIGRATION_TARGET_BINDING,
      scope: "library",
      conflict: "skip",
      include_incomplete_tasks: false,
      set_target_default_on_success: false,
      item_count: 1,
      file_count: 1,
      conflict_count: 1,
      incomplete_task_count: 0,
      total_bytes: 100,
      required_space_bytes: 100,
      available_space_bytes: 10_000,
      items: [{
        item_id: MIGRATION_CONFLICT_ITEM_ID,
        conflict: true,
        source_path: "Films/Conflict.mkv",
        target_path: "Films/Conflict.mkv",
        files: [],
      }],
    },
  };
  return {
    ...ownerSnapshot,
    state_version: 1,
    settings: {
      ...ownerSnapshot.settings,
      applied: {
        ...ownerSnapshot.settings.applied,
        default_library_id: LIBRARY_ID,
        libraries: [sourceLibrary, targetLibrary],
      },
    },
    library: {
      ...ownerSnapshot.library,
      libraries: [
        { ...sourceLibrary, snapshot: sourceScan },
        { ...targetLibrary, snapshot: targetScan },
      ],
      migrations: [],
    },
    operations: [preview],
    pending_requests: [],
    previews: [],
  };
}

function fileCancellationPreviewEntry() {
  const operation = filePreviewFixture(
    FILE_CANCEL_PREVIEW_SEQ,
    LIBRARY_ID,
    ROOT_BINDING,
    FILE_CANCEL_PREVIEW_ID,
    FILE_CANCEL_PREVIEW_REVISION,
  );
  return {
    ...operation,
    id: FILE_CANCEL_PREVIEW_ID,
    preview_id: FILE_CANCEL_PREVIEW_ID,
    revision: FILE_CANCEL_PREVIEW_REVISION,
    preview: operation.result.preview,
  };
}

function migrationCancellationPreviewSnapshot() {
  const snapshot = migrationPreviewSnapshot();
  return {
    ...snapshot,
    synced: true,
    stale: false,
    last_seen: Date.now(),
    previews: [snapshot.operations[0], fileCancellationPreviewEntry()],
  };
}

function snapshotWithLibraryLabel(snapshot: Record<string, any>, name: string) {
  return {
    ...snapshot,
    settings: {
      ...snapshot.settings,
      applied: {
        ...snapshot.settings.applied,
        libraries: snapshot.settings.applied.libraries.map((library: Record<string, any>) => (
          library.id === LIBRARY_ID ? { ...library, name } : library
        )),
      },
    },
    library: {
      ...snapshot.library,
      libraries: snapshot.library.libraries.map((library: Record<string, any>) => (
        library.id === LIBRARY_ID ? { ...library, name } : library
      )),
    },
  };
}

function saturatedCancellationSnapshot() {
  const snapshot = migrationCancellationPreviewSnapshot();
  return {
    ...snapshot,
    operations: newerHistoryOperations(1_000, 100),
    pending_requests: [],
  };
}

function orphanCancellationSnapshot(stateVersion = 1) {
  const snapshot = migrationPreviewSnapshot();
  return {
    ...snapshot,
    state_version: stateVersion,
    synced: true,
    stale: false,
    last_seen: Date.now(),
    operations: [
      {
        seq: FILE_CANCEL_REQUEST_SEQ,
        request_id: FILE_CANCEL_REQUEST_ID,
        kind: "preview_cancel",
        status: "completed",
        result_truncated: true,
        result: {},
      },
      ...newerHistoryOperations(1_000, 2),
    ],
    pending_requests: [],
    previews: [],
  };
}

function newerHistoryOperations(startSeq = 1_000, count = 100) {
  return Array.from({ length: count }, (_, index) => ({
    seq: startSeq + index,
    request_id: `retained-history-${startSeq + index}`,
    kind: "download_submit",
    status: "completed",
    result: {},
  }));
}

function previewCancellationStorageKey(kind: "file_preview" | "library_migration_preview", seq: number, id: string, revision: string) {
  return JSON.stringify([kind, seq, id, revision]);
}

function cancellationTrackerEnvelope(trackers: Array<{
  key: string;
  requestId: string;
  seq: number | null;
  status: "queued" | "submitted" | "unknown";
  error: string | null;
}>) {
  return JSON.stringify({
    version: 1,
    entries: trackers.map(({ key, ...tracker }) => ({
      key,
      tracker: {
        requestId: tracker.requestId,
        seq: tracker.seq,
        status: tracker.status,
        error: tracker.error,
      },
    })),
  });
}

function acceptedCancellationTrackers(seqOffset = 0) {
  return [
    {
      key: previewCancellationStorageKey("file_preview", FILE_CANCEL_PREVIEW_SEQ, FILE_CANCEL_PREVIEW_ID, FILE_CANCEL_PREVIEW_REVISION),
      requestId: FILE_CANCEL_REQUEST_ID,
      seq: FILE_CANCEL_REQUEST_SEQ + seqOffset,
      status: "submitted" as const,
      error: null,
    },
    {
      key: previewCancellationStorageKey("library_migration_preview", MIGRATION_PREVIEW_SEQ, MIGRATION_PREVIEW_ID, MIGRATION_PREVIEW_REVISION),
      requestId: MIGRATION_CANCEL_REQUEST_ID,
      seq: MIGRATION_CANCEL_REQUEST_SEQ + seqOffset,
      status: "submitted" as const,
      error: null,
    },
  ];
}

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
    publishSnapshot: (snapshot: Record<string, any>, databaseEpoch?: string) => Promise<void>,
  ) => Promise<MockActionResponse | void> | MockActionResponse | void,
) {
  await releaseMountedFixture();
  installFixtureWindowTimerTracking();
  let currentDatabaseEpoch = TEST_DATABASE_EPOCH;
  let currentManifest: Record<string, any>;
  const sockets = installMockWebSocket(
    () => currentDatabaseEpoch,
    () => currentManifest?.state_version ?? 1,
  );
  let stateRequests = 0;
  let sessionRequests = 0;
  let sessionStatus = 200;
  let nextStateResponseGate: ReturnType<typeof deferredSignal> | null = null;
  const stateRequestVersions: number[] = [];
  const stateRequestEpochs: string[] = [];
  let nextActionSeq = 901;
  const actions: Array<Record<string, any>> = [];
  let pendingActionResponseCount = 0;
  let currentSnapshot = initialSnapshot;
  const initialBundle = await makeChunkedSnapshot(initialSnapshot, { databaseEpoch: currentDatabaseEpoch });
  currentManifest = initialBundle.manifest;
  const responseChunks = new Map(initialBundle.chunks);
  const chunkEpochs = new Map<string, Set<string>>();

  function registerBundle(bundle: Awaited<ReturnType<typeof makeChunkedSnapshot>>) {
    for (const [sha256, data] of bundle.chunks) {
      responseChunks.set(sha256, data);
      const epochs = chunkEpochs.get(sha256) ?? new Set<string>();
      epochs.add(bundle.manifest.database_epoch);
      chunkEpochs.set(sha256, epochs);
    }
  }
  registerBundle(initialBundle);

  async function publishSnapshot(snapshot: Record<string, any>, databaseEpoch = currentDatabaseEpoch) {
    currentDatabaseEpoch = databaseEpoch;
    currentSnapshot = snapshot;
    const bundle = await makeChunkedSnapshot(snapshot, { databaseEpoch });
    currentManifest = bundle.manifest;
    registerBundle(bundle);
  }

  async function publishActionSnapshot(snapshot: Record<string, any>) {
    await publishSnapshot(snapshot);
    publishMockSnapshot(currentManifest.state_version ?? 0);
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
      stateRequestVersions.push(manifest.state_version);
      stateRequestEpochs.push(manifest.database_epoch);
      const gate = nextStateResponseGate;
      nextStateResponseGate = null;
      if (gate) await gate.promise;
      return { ok: true, status: 200, json: async () => manifest } as Response;
    }
    const chunkPath = String(input).match(/^\/api\/snapshot\/chunks\/([0-9a-f]{64})$/u);
    if (chunkPath) {
      const sha256 = chunkPath[1]!;
      const requestEpoch = new Headers(init?.headers).get("X-Cloud-Database-Epoch");
      expect(requestEpoch).toBeTruthy();
      expect(chunkEpochs.get(sha256)?.has(requestEpoch!)).toBe(true);
      return { ok: true, status: 200, json: async () => ({ sha256, data: responseChunks.get(sha256) }) } as Response;
    }
    if (String(input) === "/api/actions") {
      pendingActionResponseCount += 1;
      try {
        const action = JSON.parse(String(init?.body)) as Record<string, any>;
        actions.push(action);
        const seq = nextActionSeq++;
        let actionPublishedSnapshot = false;
        const publishAcceptedSnapshot = async (snapshot: Record<string, any>) => {
          actionPublishedSnapshot = true;
          await publishActionSnapshot(snapshot);
        };
        const customResponse = await onAction?.(action, seq, publishAcceptedSnapshot);
        if (customResponse) {
          if (customResponse.status >= 200 && customResponse.status < 300 && !actionPublishedSnapshot) {
            const currentVersion = Number.isSafeInteger(currentSnapshot.state_version)
              ? currentSnapshot.state_version
              : currentManifest.state_version ?? 0;
            const pending = {
              seq,
              request_id: action.request_id,
              kind: action.kind,
              status: "pending",
              created_at: Math.floor(Date.now() / 1000),
              payload: action.payload,
            };
            await publishAcceptedSnapshot({
              ...currentSnapshot,
              state_version: currentVersion + 1,
              pending_requests: [...(Array.isArray(currentSnapshot.pending_requests) ? currentSnapshot.pending_requests : []), pending],
            });
          }
          return {
            ok: customResponse.status >= 200 && customResponse.status < 300,
            status: customResponse.status,
            json: async () => customResponse.body,
          } as Response;
        }
        if (!actionPublishedSnapshot) {
          const currentVersion = Number.isSafeInteger(currentSnapshot.state_version)
            ? currentSnapshot.state_version
            : currentManifest.state_version ?? 0;
          const pending = {
            seq,
            request_id: action.request_id,
            kind: action.kind,
            status: "pending",
            created_at: Math.floor(Date.now() / 1000),
            payload: action.payload,
          };
          await publishAcceptedSnapshot({
            ...currentSnapshot,
            state_version: currentVersion + 1,
            pending_requests: [...(Array.isArray(currentSnapshot.pending_requests) ? currentSnapshot.pending_requests : []), pending],
          });
        }
        return { ok: true, status: 202, json: async () => ({ seq, request_id: action.request_id }) } as Response;
      } finally {
        pendingActionResponseCount -= 1;
      }
    }
    throw new Error(`Unexpected request ${String(input)}`);
  });
  vi.stubGlobal("fetch", fetchMock);
  window.fetch = fetchMock as typeof window.fetch;

  vi.resetModules();
  // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
  await import("../public/file/app.js");
  await vi.waitFor(() => expect(latestOpenSocket(sockets)).toBeDefined());
  await vi.waitFor(() => {
    expect(stateRequests).toBeGreaterThan(0);
    expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false);
    expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
  });

  settleCurrentFixtureActions = async () => {
    if (actions.length === 0) return;
    await vi.waitFor(() => expect(pendingActionResponseCount).toBe(0));
    await fixtureTaskBoundary();
    await vi.waitFor(() => {
      expect(pendingActionResponseCount).toBe(0);
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
    });
    await fixtureTaskBoundary();
  };

  return {
    actions,
    sockets,
    fetchMock,
    publishSnapshot,
    setDatabaseEpoch(databaseEpoch: string) { currentDatabaseEpoch = databaseEpoch; },
    setSessionStatus(status: number) { sessionStatus = status; },
    holdNextStateResponse() {
      if (nextStateResponseGate) throw new Error("A state response is already held.");
      const gate = deferredSignal();
      nextStateResponseGate = gate;
      return gate.release;
    },
    stateRequestVersions,
    stateRequestEpochs,
    get sessionRequests() { return sessionRequests; },
    get stateRequests() { return stateRequests; },
    get currentSnapshot() { return currentSnapshot; },
  };
}

beforeEach(() => {
  captureMountedAppWindowListeners();
  installSnapshotUpdatesFactoryCapture();
});

afterEach(async () => {
  window.dispatchEvent(new Event("pagehide"));
  await releaseMountedFixture();
  activeMockSockets.length = 0;
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  restoreWindowAddEventListener();
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
  it("watches cloud-pending scans and imports after reload, then stops at terminal state", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const initialSnapshot = {
      ...ownerSnapshot,
      state_version: 1,
      pending_requests: [
        { seq: 811, kind: "library_scan", status: "waiting_for_local", created_at: Math.floor(Date.now() / 1_000) },
        { seq: 812, kind: "legacy_import", status: "waiting_for_local", created_at: Math.floor(Date.now() / 1_000) },
      ],
      operations: [],
    };
    const fixture = await mountFileManagerFixture(initialSnapshot, makeMemoryStorage());
    const socket = latestOpenSocket(fixture.sockets);
    expect(socket).toBeDefined();
    await vi.waitFor(() => expect(socket?.sentFrames.some((frame) => frame.type === "interest" && frame.watch === true)).toBe(true));
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
    expect(socket?.sentFrames.filter((frame) => frame.type === "refresh_request")).toHaveLength(1);

    const requestsBeforeTerminal = fixture.stateRequests;
    await fixture.publishSnapshot({
      ...initialSnapshot,
      state_version: 2,
      pending_requests: [],
      operations: [
        { seq: 811, kind: "library_scan", status: "completed", updated_at: Math.floor(Date.now() / 1_000) },
        { seq: 812, kind: "legacy_import", status: "failed", updated_at: Math.floor(Date.now() / 1_000), error: "The local import could not be applied." },
      ],
    });
    socket?.receive({ type: "invalidate", database_epoch: TEST_DATABASE_EPOCH, state_version: 2, reason: "state" });
    await vi.waitFor(() => expect(fixture.stateRequests).toBe(requestsBeforeTerminal + 1));
    await vi.waitFor(() => expect(socket?.sentFrames.filter((frame) => frame.type === "interest").at(-1)?.watch).toBe(false));
    expect(socket?.sentFrames.filter((frame) => frame.type === "refresh_request")).toHaveLength(1);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
  });

  it("shows and clears selected items, navigates to the move card, and exposes mobile Details", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);
    const sockets = installMockWebSocket();
    let currentManifest = manifest;
    for (const socket of sockets) socket.currentStateVersion = () => currentManifest.state_version ?? 0;
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

    vi.resetModules();
    const firstReadAt = Date.now();
    // The browser entry has no declarations because it is executed for its side effects.
    // @ts-expect-error The JavaScript browser entry is intentionally declaration-free.
    await import("../public/file/app.js");
    await vi.waitFor(() => expect(stateRequests).toBe(1));
    expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(true);
    expect(element<HTMLElement>("#auth-gate").classList.contains("hidden")).toBe(false);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sockets[0]?.sentFrames).toEqual(expect.arrayContaining([expect.objectContaining({ type: "refresh_request", reason: "open" })]));
    await vi.waitFor(() => expect(stateRequests).toBe(2));
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false));
    const formattedReportTime = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" })
      .format(new Date(MOCK_REPORTED_AT_MS));
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent)
      .toBe(`Desktop snapshot published at ${formattedReportTime}.`));
    expect(Date.now() - firstReadAt).toBeLessThan(5 * 60 * 1_000);
    expect(element<HTMLElement>("#sync-badge").textContent).toBe("Saved snapshot");
    expect(element<HTMLElement>("#sync-detail").textContent).toContain("Snapshot published Just now");
    const formattedScanTime = new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" })
      .format(new Date(Number(ownerSnapshot.library.scanned_at) * 1_000));
    expect(element<HTMLElement>("#sync-detail").textContent).toContain(`Last scan ${formattedScanTime}`);
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
    element<HTMLButtonElement>("#refresh-button").click();
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(stateRequestsBeforePush));
    await vi.waitFor(() => expect(element<HTMLElement>("#library-list").textContent).toContain("Beta from pushed update"));

    failNextStateResponse = true;
    const statesBeforeManualFailure = stateRequests;
    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(stateRequests).toBe(statesBeforeManualFailure + 1));
    await vi.waitFor(() => expect(element<HTMLElement>("#toast").textContent).toContain("published a snapshot, but this view could not reload it"));
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

  it.each(["library_scan", "legacy_import"] as const)(
    "keeps the foreground update watch active for %s until its terminal snapshot",
    async (kind) => {
      const fixture = await mountFileManagerFixture(ownerSnapshot, makeMemoryStorage());
      const socket = latestOpenSocket(fixture.sockets);
      if (!socket) throw new Error("Expected the file manager update socket to be open.");

      if (kind === "library_scan") {
        element<HTMLButtonElement>("#scan-button").click();
      } else {
        element<HTMLTextAreaElement>("#legacy-text").value = "Legacy metadata entry";
        element<HTMLButtonElement>("#import-legacy").click();
      }

      await vi.waitFor(() => expect(fixture.actions).toHaveLength(1));
      const action = fixture.actions[0]!;
      expect(action.kind).toBe(kind);
      await vi.waitFor(() => expect(JSON.parse(localStorage.getItem("file-manager.actions.v1") ?? "[]")).toEqual([]));
      await vi.waitFor(() => expect(socket.watch).toBe(true));
      expect(socket.sentFrames.filter((frame) => frame.type === "refresh_request")).toHaveLength(1);

      const accepted = fixture.currentSnapshot.pending_requests.find((request: Record<string, any>) => request.request_id === action.request_id);
      expect(accepted?.seq).toEqual(expect.any(Number));
      const requestsBeforeTerminal = fixture.stateRequests;
      const terminalSnapshot = {
        ...fixture.currentSnapshot,
        state_version: Number(fixture.currentSnapshot.state_version ?? 1) + 1,
        operations: [{
          seq: accepted.seq,
          kind,
          status: "completed",
          result: kind === "legacy_import"
            ? { library_id: LIBRARY_ID, root_binding: ROOT_BINDING, revision: "legacy-import-r1" }
            : { revision: "library-scan-r2", scanned_at: Math.floor(Date.now() / 1_000) },
        }],
        pending_requests: [],
      };
      await fixture.publishSnapshot(terminalSnapshot);
      socket.publishSnapshot(terminalSnapshot.state_version);
      await vi.waitFor(() => expect(fixture.stateRequests).toBe(requestsBeforeTerminal + 1));
      await vi.waitFor(() => expect(socket.watch).toBe(false));
      expect(socket.sentFrames.filter((frame) => frame.type === "refresh_request")).toHaveLength(1);
    },
  );

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

    await vi.waitFor(() => expect(stateRequests).toBe(2)); // Initial cache read, then the one open refresh.
    const requestsBeforeManualRefresh = stateRequests;
    element<HTMLButtonElement>("#refresh-button").click();
    await vi.waitFor(() => expect(stateRequests).toBe(requestsBeforeManualRefresh + 1));
    await vi.waitFor(() => expect(element<HTMLElement>("#sync-title").textContent?.toLowerCase()).toContain("saved snapshot"));

    expect(element<HTMLElement>("#app-content").classList.contains("hidden")).toBe(false);
    expect(element<HTMLButtonElement>('[data-details="item-alpha"]')).toBeDefined();
    expect([...document.querySelectorAll<HTMLOptionElement>("#category-folders option")].map((option) => option.value)).toEqual(expect.arrayContaining(["Films", "Series", "Shows"]));
    expect(fetchMock.mock.calls.filter(([input]) => String(input) === "/api/state")).toHaveLength(requestsBeforeManualRefresh + 1);
    expect(fetchMock.mock.calls.filter(([input]) => String(input).startsWith("/api/snapshot/chunks/"))).toHaveLength(
      new Set([...manifest.library.chunks, ...manifest.state.chunks].map((chunk: { sha256: string }) => chunk.sha256)).size,
    );
  });

  it("keeps queued actions fenced until a lower-version snapshot from the new database epoch verifies", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const oldEpoch = TEST_DATABASE_EPOCH;
    const newEpoch = "c".repeat(32);
    const previewId = "epoch-fenced-cancellation";
    const revision = "epoch-fenced-cancellation-r1";
    const preview = filePreviewFixture(949, LIBRARY_ID, ROOT_BINDING, previewId, revision);
    const oldSnapshot = snapshotWithLibraryLabel({
      ...ownerSnapshot,
      state_version: 34_936,
      last_seen: Date.now(),
      operations: [preview],
      pending_requests: [],
      previews: [],
    }, "Old epoch library");
    const storage = makeMemoryStorage();
    let actionSawVerifiedNewEpoch = false;
    const fixture = await mountFileManagerFixture(oldSnapshot, storage, (action, seq) => {
      actionSawVerifiedNewEpoch = element<HTMLElement>("#library-select").textContent?.includes("New epoch library") === true;
      return { status: 202, body: { seq, request_id: action.request_id } };
    });
    await vi.waitFor(() => expect(element<HTMLElement>("#snapshot-refresh-status").textContent).toContain("Desktop snapshot published"));
    expect(fixture.currentSnapshot.state_version).toBe(34_936);
    expect(fixture.stateRequestEpochs.every((epoch) => epoch === oldEpoch)).toBe(true);

    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: false });
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    element<HTMLButtonElement>(`[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="${previewId}"]`).click();
    await vi.waitFor(() => expect(JSON.parse(storage.getItem("file-manager.actions.v1") ?? "[]")).toHaveLength(1));
    expect(fixture.actions).toHaveLength(0);
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });

    const oldSocket = latestOpenSocket(fixture.sockets);
    if (!oldSocket) throw new Error("Expected the old-epoch update socket to remain open.");
    const requestsBefore = fixture.stateRequests;
    const sessionsBefore = fixture.sessionRequests;
    const releaseHeldOldRead = fixture.holdNextStateResponse();
    try {
      element<HTMLButtonElement>("#refresh-button").click();
      await vi.waitFor(() => expect(oldSocket.sentFrames.some((frame) => frame.type === "refresh_request" && frame.reason === "manual")).toBe(true));
      await vi.waitFor(() => expect(fixture.stateRequests).toBe(requestsBefore + 1));

      fixture.setSessionStatus(503);
      fixture.setDatabaseEpoch(newEpoch);
      oldSocket.receive({ type: "invalidate", database_epoch: newEpoch, reason: "epoch" });
      releaseHeldOldRead();

      await vi.waitFor(() => expect(fixture.stateRequests).toBe(requestsBefore + 2));
      await vi.waitFor(() => expect(fixture.sessionRequests).toBeGreaterThan(sessionsBefore));
      expect(fixture.stateRequestVersions.slice(-2)).toEqual([34_936, 34_936]);
      expect(fixture.stateRequestEpochs.slice(-2)).toEqual([oldEpoch, oldEpoch]);
      await vi.waitFor(() => expect(element<HTMLElement>("#sync-badge").textContent).toBe("Snapshot outdated"));
      expect(element<HTMLElement>("#library-select").textContent).toContain("Old epoch library");
      expect(JSON.parse(storage.getItem("file-manager.actions.v1") ?? "[]")).toHaveLength(1);
      window.dispatchEvent(new Event("online"));
      expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
      expect(fixture.actions).toHaveLength(0);

      const newSnapshot = snapshotWithLibraryLabel({
        ...oldSnapshot,
        state_version: 1,
        last_seen: Date.now(),
      }, "New epoch library");
      await fixture.publishSnapshot(newSnapshot, newEpoch);
      fixture.setSessionStatus(200);
      await vi.advanceTimersByTimeAsync(2_000);
      await vi.waitFor(() => expect(element<HTMLElement>("#library-select").textContent).toContain("New epoch library"));
      await vi.waitFor(() => expect(fixture.actions).toHaveLength(1));
      expect(fixture.stateRequestEpochs).toContain(newEpoch);
      expect(fixture.stateRequestVersions).toContain(1);
      expect(actionSawVerifiedNewEpoch).toBe(true);
      expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(1);
      expect(element<HTMLElement>("#sync-badge").textContent).toBe("Saved snapshot");
    } finally {
      releaseHeldOldRead();
      fixture.setSessionStatus(200);
    }
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
    for (const socket of sockets) socket.currentStateVersion = () => currentManifest.state_version ?? 0;
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
      publishMockSnapshot(currentManifest.state_version ?? 0);
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
    const requestsBeforeFailedSnapshot = stateRequests;
    expect(sockets[0]?.watch).toBe(true);
    await publishSnapshot(failedSnapshot);
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(requestsBeforeFailedSnapshot));
    await vi.waitFor(() => expect(element<HTMLElement>("#preview-list").textContent).toContain("The computer could not cancel this preview."));
    expect(document.querySelector('[data-preview-cancel-id="file-cancel-failure"]')).not.toBeNull();
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
    const requestsBeforeRetiredSnapshot = stateRequests;
    expect(sockets[0]?.watch).toBe(true);
    await publishSnapshot(retiredSnapshot);
    await vi.waitFor(() => expect(stateRequests).toBeGreaterThan(requestsBeforeRetiredSnapshot));
    await vi.waitFor(() => expect(document.querySelector('[data-preview-cancel-id="file-cancel-confirmed"]')).toBeNull());
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
    const requestsBeforeTerminalUpdate = stateRequests;
    expect(sockets[0]?.watch).toBe(true);
    await publishSnapshot(terminalSnapshot);
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

  it("settles a rejected migration cancellation receipt into the retryable preview in the same render", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const snapshot = migrationPreviewSnapshot();
    const storage = makeMemoryStorage();
    const error = "The computer could not cancel this migration preview.";
    const fixture = await mountFileManagerFixture(snapshot, storage, () => ({
      status: 422,
      body: { error },
    }));

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    const stateRequestsBeforeCancel = fixture.stateRequests;
    element<HTMLButtonElement>('[data-preview-cancel-kind="library_migration_preview"][data-preview-cancel-id="migration-cancel-preview"]').click();

    await vi.waitFor(() => expect(fixture.actions).toHaveLength(1));
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#migration-preview-list").textContent).toContain(error);
      const retry = element<HTMLButtonElement>('[data-preview-cancel-kind="library_migration_preview"][data-preview-cancel-id="migration-cancel-preview"]');
      expect(retry.textContent).toContain("Retry cancellation");
      expect(retry.disabled).toBe(false);
      expect(element<HTMLButtonElement>('[data-migration-confirm="migration-cancel-preview"]').disabled).toBe(false);
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
    });

    expect(fixture.actions[0]?.kind).toBe("preview_cancel");
    expect(fixture.stateRequests).toBe(stateRequestsBeforeCancel);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(1);
  });

  it("locks a dirty migration conflict draft during queued and submitted cancellation, then restores it after a fresh failure snapshot", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const clock = controlledSnapshotClock();
    const initialSnapshot = migrationPreviewSnapshot();
    const storage = makeMemoryStorage();
    let acceptedSnapshot: MigrationPendingSnapshot = initialSnapshot;
    let acceptedSeq = 0;
    let signalActionStarted!: () => void;
    const actionStarted = new Promise<void>((resolve) => { signalActionStarted = resolve; });
    let releaseActionResponse!: (response: MockActionResponse) => void;
    const actionResponse = new Promise<MockActionResponse>((resolve) => { releaseActionResponse = resolve; });
    const fixture = await mountFileManagerFixture(initialSnapshot, storage, async (action, seq, publishSnapshot) => {
      if (action.kind !== "preview_cancel") return { status: 422, body: { error: "Unexpected superseding preview request." } };
      acceptedSeq = seq;
      const pendingRequest = {
        seq,
        request_id: action.request_id,
        kind: action.kind,
        status: "pending",
        created_at: Math.floor(Date.now() / 1000),
        payload: action.payload,
      };
      acceptedSnapshot = {
        ...acceptedSnapshot,
        state_version: acceptedSnapshot.state_version + 1,
        pending_requests: [...acceptedSnapshot.pending_requests, pendingRequest],
      };
      await publishSnapshot(acceptedSnapshot);
      signalActionStarted();
      return actionResponse;
    });

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    const conflictSelector = () => element<HappyDOMSelectElement>(`[data-migration-conflict-item="${MIGRATION_CONFLICT_ITEM_ID}"]`);
    conflictSelector().value = "keep_both";
    conflictSelector().dispatchEvent(new HappyDOMEvent("change", { bubbles: true }));
    expect(conflictSelector().value).toBe("keep_both");
    expect(element<HTMLButtonElement>('[data-migration-refresh-conflicts="migration-cancel-preview"]').disabled).toBe(false);
    expect(element<HTMLButtonElement>('[data-migration-confirm="migration-cancel-preview"]').disabled).toBe(true);

    element<HTMLButtonElement>('[data-preview-cancel-kind="library_migration_preview"][data-preview-cancel-id="migration-cancel-preview"]').click();
    await actionStarted;

    const attemptSupersedingRefresh = () => {
      const refresh = element<HappyDOMButtonElement>('[data-migration-refresh-conflicts="migration-cancel-preview"]');
      const selector = conflictSelector();
      expect(refresh.disabled).toBe(true);
      expect(selector.disabled).toBe(true);
      expect(element<HTMLButtonElement>('[data-migration-confirm="migration-cancel-preview"]').disabled).toBe(true);
      refresh.dispatchEvent(new HappyDOMEvent("click", { bubbles: true }));
      conflictSelector().value = "skip";
      conflictSelector().dispatchEvent(new HappyDOMEvent("change", { bubbles: true }));
    };
    attemptSupersedingRefresh();
    expect(element<HTMLElement>("#migration-preview-list").textContent).toContain("Cancellation is saved in this browser and will be sent when connected.");
    expect(JSON.parse(storage.getItem("file-manager.actions.v1") ?? "[]").map((action: Record<string, unknown>) => action.kind)).toEqual(["preview_cancel"]);
    expect(fixture.actions).toHaveLength(1);

    const cancellationAction = fixture.actions[0]!;
    releaseActionResponse({
      status: 202,
      body: { seq: acceptedSeq, request_id: cancellationAction.request_id },
    });
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#waiting-list").textContent).toContain(`Waiting for computer · #${acceptedSeq}`);
      expect(element<HTMLElement>("#waiting-list").textContent).toContain("Saved in the cloud inbox");
      expect(element<HTMLElement>("#migration-preview-list").textContent).toContain(`Cancellation request #${acceptedSeq} was submitted. Waiting for your computer.`);
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
    });

    attemptSupersedingRefresh();
    expect(fixture.actions).toHaveLength(1);
    expect(JSON.parse(storage.getItem("file-manager.actions.v1") ?? "[]")).toEqual([]);

    const cancellationError = "The computer could not cancel this migration preview.";
    const failureSnapshot = {
      ...fixture.currentSnapshot,
      state_version: fixture.currentSnapshot.state_version + 1,
      operations: [...fixture.currentSnapshot.operations, {
        seq: acceptedSeq,
        request_id: cancellationAction.request_id,
        kind: "preview_cancel",
        status: "failed",
        error: cancellationError,
        result: {
          preview_kind: "library_migration_preview",
          preview_seq: MIGRATION_PREVIEW_SEQ,
          preview_id: MIGRATION_PREVIEW_ID,
          revision: MIGRATION_PREVIEW_REVISION,
        },
      }],
      pending_requests: [],
    };
    clock.advanceRefreshBudget();
    await fixture.publishSnapshot(failureSnapshot);
    const stateRequestsBeforeFailure = fixture.stateRequests;
    const stateRequestVersionsBeforeFailure = fixture.stateRequestVersions.length;
    let liveSocket: MockWebSocket | undefined;
    await new Promise<void>((resolve, reject) => window.setTimeout(() => {
      try {
        liveSocket = latestOpenSocket(fixture.sockets);
        if (!liveSocket) throw new Error("Expected a live updates connection before failure snapshot invalidation.");
        liveSocket.receive({
          type: "invalidate",
          database_epoch: TEST_DATABASE_EPOCH,
          state_version: failureSnapshot.state_version,
          reason: "state",
        });
        resolve();
      } catch (error) {
        reject(error);
      }
    }, 0));
    await vi.waitFor(() => {
      expect(fixture.stateRequests, `failure snapshot v${failureSnapshot.state_version}; fetched manifest versions [${fixture.stateRequestVersions.join(", ")}]`).toBe(stateRequestsBeforeFailure + 1);
      expect(fixture.stateRequestVersions.slice(stateRequestVersionsBeforeFailure)).toContain(failureSnapshot.state_version);
      expect(element<HTMLElement>("#migration-preview-list").textContent).toContain(cancellationError);
      expect(conflictSelector().value).toBe("keep_both");
      expect(conflictSelector().disabled).toBe(false);
      expect(element<HTMLButtonElement>('[data-migration-refresh-conflicts="migration-cancel-preview"]').disabled).toBe(false);
      const retry = element<HTMLButtonElement>('[data-preview-cancel-kind="library_migration_preview"][data-preview-cancel-id="migration-cancel-preview"]');
      expect(retry.textContent).toContain("Retry cancellation");
      expect(retry.disabled).toBe(false);
      expect(element<HTMLButtonElement>('[data-migration-confirm="migration-cancel-preview"]').disabled).toBe(true);
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
    });
    expect(fixture.actions).toHaveLength(1);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(1);
  });

  it("keeps a dirty migration draft locked for an unknown cancellation and rejects synthetic refresh/change events", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const snapshot = { ...migrationPreviewSnapshot(), last_seen: Date.now() };
    const storage = makeMemoryStorage();
    const fixture = await mountFileManagerFixture(snapshot, storage, () => ({
      status: 410,
      body: { error: "The migration cancellation delivery is unknown." },
    }));

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    const conflictSelector = () => element<HappyDOMSelectElement>(`[data-migration-conflict-item="${MIGRATION_CONFLICT_ITEM_ID}"]`);
    conflictSelector().value = "keep_both";
    conflictSelector().dispatchEvent(new HappyDOMEvent("change", { bubbles: true }));
    expect(conflictSelector().value).toBe("keep_both");
    element<HTMLButtonElement>('[data-preview-cancel-kind="library_migration_preview"][data-preview-cancel-id="migration-cancel-preview"]').click();
    await vi.waitFor(() => expect(fixture.actions).toHaveLength(1));
    await vi.waitFor(() => expect(element<HTMLElement>("#migration-preview-list").textContent).toContain("The cloud no longer has a retained receipt for this request."));

    const refresh = element<HappyDOMButtonElement>('[data-migration-refresh-conflicts="migration-cancel-preview"]');
    expect(refresh.disabled).toBe(true);
    expect(conflictSelector().disabled).toBe(true);
    expect(element<HTMLButtonElement>('[data-migration-confirm="migration-cancel-preview"]').disabled).toBe(true);
    refresh.dispatchEvent(new HappyDOMEvent("click", { bubbles: true }));
    conflictSelector().value = "skip";
    conflictSelector().dispatchEvent(new HappyDOMEvent("change", { bubbles: true }));
    element<HTMLButtonElement>('[data-view="waiting"]').click();

    expect(conflictSelector().value).toBe("keep_both");
    expect(element<HTMLButtonElement>('[data-migration-refresh-conflicts="migration-cancel-preview"]').disabled).toBe(true);
    expect(element<HTMLButtonElement>('[data-migration-confirm="migration-cancel-preview"]').disabled).toBe(true);
    expect(fixture.actions).toHaveLength(1);
    expect(fixture.actions[0]?.kind).toBe("preview_cancel");
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(1);
    expect(JSON.parse(storage.getItem("file-manager.actions.v1") ?? "[]").map((action: Record<string, unknown>) => action.kind)).toEqual(["preview_cancel"]);
  });

  it("keeps evicted file and migration cancellations dismissible across stale storage events, reload, and counts", async () => {
    const storage = makeMemoryStorage();
    const submittedTrackers = acceptedCancellationTrackers();
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope(submittedTrackers));
    const snapshot = saturatedCancellationSnapshot();
    const fixture = await mountFileManagerFixture(snapshot, storage);
    const fileKey = submittedTrackers[0]!.key;
    const migrationKey = submittedTrackers[1]!.key;
    const fileDismiss = () => element<HTMLButtonElement>(`[data-preview-cancel-dismiss-kind="file_preview"][data-preview-cancel-dismiss-id="${FILE_CANCEL_PREVIEW_ID}"]`);
    const migrationDismiss = () => element<HTMLButtonElement>(`[data-preview-cancel-dismiss-kind="library_migration_preview"][data-preview-cancel-dismiss-id="${MIGRATION_PREVIEW_ID}"]`);

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => {
      expect(fileDismiss()).toBeDefined();
      expect(migrationDismiss()).toBeDefined();
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("4");
    });
    const reconciled = JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null") as {
      entries: Array<{ key: string; tracker: { status: string; seq: number | null } }>;
    };
    expect(reconciled.entries).toEqual(expect.arrayContaining([
      expect.objectContaining({ key: fileKey, tracker: expect.objectContaining({ status: "unknown", seq: FILE_CANCEL_REQUEST_SEQ }) }),
      expect.objectContaining({ key: migrationKey, tracker: expect.objectContaining({ status: "unknown", seq: MIGRATION_CANCEL_REQUEST_SEQ }) }),
    ]));

    const staleSubmittedEnvelope = cancellationTrackerEnvelope(submittedTrackers);
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, staleSubmittedEnvelope);
    window.dispatchEvent(new StorageEvent("storage", {
      key: PREVIEW_CANCELLATION_STORAGE_KEY,
      newValue: staleSubmittedEnvelope,
    }));
    expect(fileDismiss()).toBeDefined();
    expect(migrationDismiss()).toBeDefined();
    expect(element<HTMLElement>("#waiting-count").textContent).toBe("4");
    const storageAfterStaleSubmittedEvent = JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null") as {
      entries: Array<{ key: string; tracker: { status: string; seq: number | null } }>;
    };
    expect(storageAfterStaleSubmittedEvent.entries).toEqual(expect.arrayContaining([
      expect.objectContaining({ key: fileKey, tracker: expect.objectContaining({ status: "unknown", seq: FILE_CANCEL_REQUEST_SEQ }) }),
      expect.objectContaining({ key: migrationKey, tracker: expect.objectContaining({ status: "unknown", seq: MIGRATION_CANCEL_REQUEST_SEQ }) }),
    ]));

    fileDismiss().click();
    await vi.waitFor(() => expect(element<HTMLElement>("#waiting-count").textContent).toBe("3"));
    const afterFileDismissal = JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null") as { entries: Array<{ key: string }> };
    expect(afterFileDismissal.entries.map((entry) => entry.key)).toEqual([migrationKey]);
    migrationDismiss().click();
    await vi.waitFor(() => expect(element<HTMLElement>("#waiting-count").textContent).toBe("2"));
    expect(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY)).toBeNull();
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);

    const reloaded = await mountFileManagerFixture(snapshot, storage);
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => expect(element<HTMLElement>("#waiting-count").textContent).toBe("2"));
    expect(document.querySelector('[data-preview-cancel-dismiss-kind="file_preview"]')).toBeNull();
    expect(document.querySelector('[data-preview-cancel-dismiss-kind="library_migration_preview"]')).toBeNull();
    expect(element<HTMLButtonElement>(`[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="${FILE_CANCEL_PREVIEW_ID}"]`).disabled).toBe(false);
    expect(element<HTMLButtonElement>(`[data-preview-cancel-kind="library_migration_preview"][data-preview-cancel-id="${MIGRATION_PREVIEW_ID}"]`).disabled).toBe(false);
    expect(reloaded.actions).toHaveLength(0);
  });

  it("shows and safely dismisses short-history orphan cancellations including a compacted terminal receipt", async () => {
    const backingStorage = makeMemoryStorage();
    let failCancellationStorageWrite = false;
    const storage: MemoryStorageFixture = {
      getItem: backingStorage.getItem,
      setItem(key, value) {
        if (failCancellationStorageWrite && key === PREVIEW_CANCELLATION_STORAGE_KEY) {
          throw new Error("Simulated cancellation-state storage failure");
        }
        backingStorage.setItem(key, value);
      },
      removeItem(key) {
        if (failCancellationStorageWrite && key === PREVIEW_CANCELLATION_STORAGE_KEY) {
          throw new Error("Simulated cancellation-state storage failure");
        }
        backingStorage.removeItem(key);
      },
      clear: backingStorage.clear,
    };
    const submittedTrackers = acceptedCancellationTrackers();
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope(submittedTrackers));
    const snapshot = orphanCancellationSnapshot();
    let fixture = await mountFileManagerFixture(snapshot, storage);
    const fileKey = submittedTrackers[0]!.key;
    const migrationKey = submittedTrackers[1]!.key;
    const fileDismiss = () => element<HTMLButtonElement>(`#review-navigation-list [data-preview-cancel-dismiss-kind="file_preview"][data-preview-cancel-dismiss-id="${FILE_CANCEL_PREVIEW_ID}"]`);
    const migrationDismiss = () => element<HTMLButtonElement>(`#review-navigation-list [data-preview-cancel-dismiss-kind="library_migration_preview"][data-preview-cancel-dismiss-id="${MIGRATION_PREVIEW_ID}"]`);

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => {
      expect(fileDismiss()).toBeDefined();
      expect(migrationDismiss()).toBeDefined();
      expect(element<HTMLElement>("#review-navigation-list").textContent).toContain(`Cancellation request #${FILE_CANCEL_REQUEST_SEQ} is recorded, but its result details are unavailable.`);
      expect(element<HTMLElement>("#review-navigation-list").textContent).toContain("Its outcome is unknown; check your computer before dismissing this browser notice.");
      expect(element<HTMLElement>("#review-navigation-list").textContent).not.toContain("Cancelled");
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("2");
    });
    const persistedUnknown = JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null") as {
      entries: Array<{ key: string; tracker: { status: string } }>;
    };
    expect(persistedUnknown.entries).toEqual(expect.arrayContaining([
      expect.objectContaining({ key: fileKey, tracker: expect.objectContaining({ status: "unknown" }) }),
      expect.objectContaining({ key: migrationKey, tracker: expect.objectContaining({ status: "unknown" }) }),
    ]));

    const staleSnapshot = {
      ...snapshotWithLibraryLabel(snapshot, "Stale cancellation snapshot applied"),
      state_version: 2,
      stale: true,
      reported_at: Date.now() - 61 * 60 * 1_000,
      last_seen: Date.now() - 11 * 60 * 1_000,
    };
    const stateRequestsBeforeStale = fixture.stateRequests;
    await fixture.publishSnapshot(staleSnapshot);
    fixture.sockets[0]?.receive({
      type: "invalidate",
      database_epoch: TEST_DATABASE_EPOCH,
      state_version: staleSnapshot.state_version,
      reason: "state",
    });
    await vi.waitFor(() => {
      expect(fixture.stateRequests).toBe(stateRequestsBeforeStale + 1);
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
      expect(element<HTMLElement>("#library-select").textContent).toContain("Stale cancellation snapshot applied");
      expect(fileDismiss()).toBeDefined();
      expect(migrationDismiss()).toBeDefined();
    });
    const persistedAfterStaleSnapshot = JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null") as {
      entries: Array<{ key: string; tracker: { status: string; seq: number | null } }>;
    };
    expect(persistedAfterStaleSnapshot.entries).toEqual(expect.arrayContaining([
      expect.objectContaining({ key: fileKey, tracker: expect.objectContaining({ status: "unknown", seq: FILE_CANCEL_REQUEST_SEQ }) }),
      expect.objectContaining({ key: migrationKey, tracker: expect.objectContaining({ status: "unknown", seq: MIGRATION_CANCEL_REQUEST_SEQ }) }),
    ]));

    fixture = await mountFileManagerFixture(staleSnapshot, storage);
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("2");
      expect(fileDismiss()).toBeDefined();
      expect(migrationDismiss()).toBeDefined();
    });
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: false });

    failCancellationStorageWrite = true;
    fileDismiss().click();
    expect(element<HTMLElement>("#toast").textContent).toContain("could not be removed from browser storage");
    expect(fileDismiss()).toBeDefined();
    expect(migrationDismiss()).toBeDefined();
    expect(element<HTMLElement>("#waiting-count").textContent).toBe("2");
    expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries.map((entry: { key: string }) => entry.key)).toEqual([fileKey, migrationKey].sort());
    expect(fixture.actions).toHaveLength(0);

    failCancellationStorageWrite = false;
    fileDismiss().click();
    await vi.waitFor(() => expect(element<HTMLElement>("#waiting-count").textContent).toBe("1"));
    expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries.map((entry: { key: string }) => entry.key)).toEqual([migrationKey]);
    migrationDismiss().click();
    await vi.waitFor(() => expect(element<HTMLElement>("#waiting-count").textContent).toBe("0"));
    expect(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY)).toBeNull();
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);

    const reloaded = await mountFileManagerFixture(staleSnapshot, storage);
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => expect(element<HTMLElement>("#waiting-count").textContent).toBe("0"));
    expect(document.querySelector("#review-navigation-list [data-preview-cancel-dismiss-kind]")).toBeNull();
    expect(reloaded.actions).toHaveLength(0);
  });

  it("keeps exact pending, saturated pending, omission, stale, and incomplete histories blocked", async () => {
    const clock = controlledSnapshotClock();
    const storage = makeMemoryStorage();
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope(acceptedCancellationTrackers()));
    const saturated = saturatedCancellationSnapshot();
    const submitted = acceptedCancellationTrackers();
    const exactPendingRequests = submitted.map((tracker, index) => ({
      seq: tracker.seq,
      request_id: tracker.requestId,
      kind: "preview_cancel",
      status: "pending",
      created_at: Math.floor(Date.now() / 1000),
      payload: {
        preview_kind: index === 0 ? "file_preview" : "library_migration_preview",
        preview_seq: index === 0 ? FILE_CANCEL_PREVIEW_SEQ : MIGRATION_PREVIEW_SEQ,
        preview_id: index === 0 ? FILE_CANCEL_PREVIEW_ID : MIGRATION_PREVIEW_ID,
        revision: index === 0 ? FILE_CANCEL_PREVIEW_REVISION : MIGRATION_PREVIEW_REVISION,
      },
    }));
    const initialSnapshot: MigrationPendingSnapshot = {
      ...saturated,
      state_version: 1,
      pending_requests: exactPendingRequests,
    };
    const fixture = await mountFileManagerFixture(initialSnapshot, storage);
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    const expectNoDismissibleCancellation = () => {
      expect(document.querySelector('[data-preview-cancel-dismiss-kind="file_preview"]')).toBeNull();
      expect(document.querySelector('[data-preview-cancel-dismiss-kind="library_migration_preview"]')).toBeNull();
    };
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#waiting-list").textContent).toContain(`Waiting for computer · #${FILE_CANCEL_REQUEST_SEQ}`);
      expect(element<HTMLElement>("#waiting-list").textContent).toContain(`Waiting for computer · #${MIGRATION_CANCEL_REQUEST_SEQ}`);
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
      expectNoDismissibleCancellation();
    });

    const publishAndCheck = async (
      nextSnapshot: Record<string, any>,
      appliedMarker: () => void = () => {},
      pauseAfterRequest = false,
    ) => {
      clock.advanceRefreshBudget();
      const requestsBefore = fixture.stateRequests;
      const manifestVersionsBefore = fixture.stateRequestVersions.length;
      await fixture.publishSnapshot(nextSnapshot);
      let liveSocket: MockWebSocket | undefined;
      await vi.waitFor(() => {
        liveSocket = latestOpenSocket(fixture.sockets);
        expect(liveSocket).toBeDefined();
      });
      liveSocket!.receive({
        type: "invalidate",
        database_epoch: TEST_DATABASE_EPOCH,
        state_version: nextSnapshot.state_version,
        reason: "state",
      });
      await vi.waitFor(() => {
        expect(fixture.stateRequests, `state_version ${nextSnapshot.state_version}; fetched manifest versions [${fixture.stateRequestVersions.join(", ")}]`).toBe(requestsBefore + 1);
        expect(fixture.stateRequestVersions.slice(manifestVersionsBefore)).toContain(nextSnapshot.state_version);
      });
      if (pauseAfterRequest) Object.defineProperty(window.navigator, "onLine", { configurable: true, value: false });
      await vi.waitFor(() => {
        expect(fixture.stateRequests, `state_version ${nextSnapshot.state_version} must request the published snapshot`).toBe(requestsBefore + 1);
        expect(element<HTMLElement>("#app-content").getAttribute("aria-busy"), `state_version ${nextSnapshot.state_version} must finish applying the snapshot`).toBe("false");
        appliedMarker();
        expectNoDismissibleCancellation();
      });
    };
    const saturatedPending = {
      ...saturated,
      state_version: 2,
      pending_requests: Array.from({ length: 100 }, (_, index) => ({
        seq: 2_000 + index,
        request_id: `saturated-pending-${index}`,
        kind: "library_scan",
        status: "pending",
        created_at: Math.floor(Date.now() / 1000),
      })),
    };
    await publishAndCheck(saturatedPending, () => expect(element<HTMLElement>("#waiting-count").textContent).toBe("104"));

    const omissionPlaceholder = {
      preview_incomplete: true,
      omitted_count: 2,
      reason: "Two active preview identities were omitted from this fixture.",
    };
    const omittedSnapshot = {
      ...saturated,
      state_version: 3,
      operations: newerHistoryOperations(1_000, 4),
      pending_requests: [],
      previews: [omissionPlaceholder],
    };
    await publishAndCheck(omittedSnapshot, () => expect(element<HTMLElement>("#preview-list").textContent).toContain("2 active previews were omitted"));

    const staleSnapshot = {
      ...saturated,
      state_version: 4,
      synced: true,
      stale: true,
      reported_at: Date.now() - 61 * 60 * 1_000,
      last_seen: Date.now() - 11 * 60 * 1_000,
      operations: newerHistoryOperations(1_000, 4),
      pending_requests: [],
      previews: [],
      library: {
        ...saturated.library,
        libraries: saturated.library.libraries.map((library: Record<string, any>) => (
          library.id === LIBRARY_ID ? { ...library, name: "Stale history snapshot applied" } : library
        )),
      },
    };
    await publishAndCheck(staleSnapshot, () => {
      expect(element<HTMLElement>("#library-select").textContent).toContain("Stale history snapshot applied");
    });

    const incompleteSnapshot: Record<string, any> = {
      ...snapshotWithLibraryLabel(staleSnapshot, "Incomplete history snapshot applied"),
      state_version: 5,
      stale: false,
      synced: true,
      last_seen: Date.now(),
      previews: [],
    };
    delete incompleteSnapshot.operations;
    delete incompleteSnapshot.pending_requests;
    delete incompleteSnapshot.previews;
    await publishAndCheck(incompleteSnapshot, () => {
      expect(element<HTMLElement>("#library-select").textContent).toContain("Stale history snapshot applied");
    }, true);
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
  });

  it("keeps a known accepted sequence when its queue receipt replaces an unknown tracker without a sequence", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const storage = makeMemoryStorage();
    const requestId = "00000000-0000-4000-8000-000000000713";
    const cancellationSeq = 905;
    const payload = {
      preview_kind: "file_preview",
      preview_seq: FILE_CANCEL_PREVIEW_SEQ,
      preview_id: FILE_CANCEL_PREVIEW_ID,
      revision: FILE_CANCEL_PREVIEW_REVISION,
    };
    const queuedAction = createQueuedAction({
      request_id: requestId,
      kind: "preview_cancel",
      payload,
      surface: ACTION_SURFACE_FILE,
    });
    storage.setItem("file-manager.actions.v1", JSON.stringify([{
      ...queuedAction,
      delivery_outcome: "outcome_unknown",
      delivery_error_code: "action_replay_window_unverifiable",
      delivery_error: "The prior cancellation delivery is unknown.",
    }]));
    const fileTracker = {
      key: previewCancellationStorageKey("file_preview", FILE_CANCEL_PREVIEW_SEQ, FILE_CANCEL_PREVIEW_ID, FILE_CANCEL_PREVIEW_REVISION),
      requestId,
      seq: null,
      status: "unknown" as const,
      error: "The prior cancellation delivery is unknown.",
    };
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope([fileTracker]));
    const snapshot: MigrationPendingSnapshot = migrationCancellationPreviewSnapshot();
    snapshot.pending_requests = [{
      seq: cancellationSeq,
      request_id: requestId,
      kind: "preview_cancel",
      status: "pending",
      created_at: Math.floor(Date.now() / 1000),
      payload,
    }];
    const fixture = await mountFileManagerFixture(snapshot, storage);

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => expect(element<HTMLElement>("#preview-list").textContent).toContain("Cancellation status is pending or ambiguous on your computer. Waiting for a current result."));
    const receiptKey = `${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(requestId)}`;
    const acceptedReceipt = JSON.stringify({
      version: 1,
      request_id: requestId,
      surface: ACTION_SURFACE_FILE,
      kind: "preview_cancel",
      seq: cancellationSeq,
      stored_at: Date.now(),
    });
    storage.setItem(receiptKey, acceptedReceipt);
    window.dispatchEvent(new StorageEvent("storage", { key: receiptKey, newValue: acceptedReceipt }));

    await vi.waitFor(() => {
      expect(element<HTMLElement>("#waiting-list").textContent).toContain(`Waiting for computer · #${cancellationSeq}`);
      expect(element<HTMLElement>("#preview-list").textContent).toContain(`Cancellation request #${cancellationSeq} was submitted. Waiting for your computer.`);
      expect(document.querySelector(`[data-preview-cancel-dismiss-kind="file_preview"][data-preview-cancel-dismiss-id="${FILE_CANCEL_PREVIEW_ID}"]`)).toBeNull();
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
    });
    expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries).toContainEqual({
      key: fileTracker.key,
      tracker: { requestId, seq: cancellationSeq, status: "submitted", error: null },
    });
    expect(storage.getItem("file-manager.actions.v1")).toBe("[]");
    expect(storage.getItem(receiptKey)).toBeNull();
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
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
      last_seen: Date.now(),
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

  it.each([
    { status: "queued" as const, seq: null, error: null },
    { status: "submitted" as const, seq: FILE_CANCEL_REQUEST_SEQ, error: null },
    { status: "unknown" as const, seq: FILE_CANCEL_REQUEST_SEQ, error: "Check the computer before trying this cancellation again." },
  ])("keeps an online file preview's confirm disabled during a $status cancellation without showing the offline hint", async ({ status, seq, error }) => {
    const storage = makeMemoryStorage();
    const key = previewCancellationStorageKey("file_preview", FILE_BUSY_PREVIEW_SEQ, FILE_BUSY_PREVIEW_ID, FILE_BUSY_PREVIEW_REVISION);
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope([{
      key,
      requestId: FILE_BUSY_CANCEL_REQUEST_ID,
      seq,
      status,
      error,
    }]));
    const snapshot = {
      ...ownerSnapshot,
      state_version: 1,
      operations: [filePreviewFixture(FILE_BUSY_PREVIEW_SEQ, LIBRARY_ID, ROOT_BINDING, FILE_BUSY_PREVIEW_ID, FILE_BUSY_PREVIEW_REVISION)],
      pending_requests: [],
      previews: [],
    };
    const fixture = await mountFileManagerFixture(snapshot, storage);

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    const confirm = element<HTMLButtonElement>(`[data-confirm="${FILE_BUSY_PREVIEW_ID}"]`);
    expect(window.navigator.onLine).toBe(true);
    expect(confirm.disabled).toBe(true);
    expect(element<HTMLElement>("#preview-list").textContent).not.toContain("This library is offline or showing a saved scan.");
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
  });

  it("shows the offline review hint for a genuinely offline file library", async () => {
    const offlineLibraryId = "offline-file-preview-library";
    const offlineBinding = `v1:${"d".repeat(64)}`;
    const offlineLibrary = {
      id: offlineLibraryId,
      name: "Offline file library",
      root: "/srv/offline",
      root_binding: offlineBinding,
      default_category: "Films",
      availability: "offline",
      available: false,
    };
    const snapshot = {
      ...ownerSnapshot,
      state_version: 1,
      settings: {
        ...ownerSnapshot.settings,
        applied: {
          ...ownerSnapshot.settings.applied,
          libraries: [...ownerSnapshot.settings.applied.libraries, offlineLibrary],
        },
      },
      library: {
        ...ownerSnapshot.library,
        libraries: [...ownerSnapshot.library.libraries, offlineLibrary],
      },
      operations: [filePreviewFixture(805, offlineLibraryId, offlineBinding, "offline-file-preview", "offline-file-r1")],
      pending_requests: [],
      previews: [],
    };
    await mountFileManagerFixture(snapshot, makeMemoryStorage());

    const librarySelect = element<HappyDOMSelectElement>("#library-select");
    librarySelect.value = offlineLibraryId;
    librarySelect.dispatchEvent(new HappyDOMEvent("change", { bubbles: true }));
    await vi.waitFor(() => expect(librarySelect.value).toBe(offlineLibraryId));
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    expect(element<HTMLButtonElement>('[data-confirm="offline-file-preview"]').disabled).toBe(true);
    expect(element<HTMLElement>("#preview-list").textContent).toContain("This library is offline or showing a saved scan.");
  });

  it("persists dismissal of a seq-less unknown cancellation and keeps it dismissed after reload", async () => {
    const memoryStorage = makeMemoryStorage();
    let failCancellationTrackerWrite = false;
    let failActionQueueWrite = false;
    const storage: MemoryStorageFixture = {
      getItem: memoryStorage.getItem,
      setItem(key, value) {
        if (failActionQueueWrite && key === "file-manager.actions.v1") {
          throw new Error("Simulated action queue storage failure");
        }
        if (failCancellationTrackerWrite && key === PREVIEW_CANCELLATION_STORAGE_KEY) {
          throw new Error("Simulated cancellation tracker storage failure");
        }
        memoryStorage.setItem(key, value);
      },
      removeItem(key) {
        if (failCancellationTrackerWrite && key === PREVIEW_CANCELLATION_STORAGE_KEY) {
          throw new Error("Simulated cancellation tracker storage failure");
        }
        memoryStorage.removeItem(key);
      },
      clear: memoryStorage.clear,
    };
    const requestId = "00000000-0000-4000-8000-000000000721";
    const payload = {
      preview_kind: "file_preview",
      preview_seq: 821,
      preview_id: "seq-less-unknown-preview",
      revision: "seq-less-unknown-r1",
    };
    const cancellationKey = previewCancellationStorageKey("file_preview", payload.preview_seq, payload.preview_id, payload.revision);
    const unknownAction = createQueuedAction({
      request_id: requestId,
      kind: "preview_cancel",
      payload,
      surface: ACTION_SURFACE_FILE,
    });
    const unknownQueueRow = {
      ...unknownAction,
      delivery_outcome: "outcome_unknown",
      delivery_error_code: "action_replay_window_unverifiable",
      delivery_error: "The prior cancellation delivery is unknown.",
    };
    memoryStorage.setItem("file-manager.actions.v1", JSON.stringify([unknownQueueRow]));
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope([{
      key: cancellationKey,
      requestId,
      seq: null,
      status: "unknown",
      error: "The prior cancellation delivery is unknown.",
    }]));
    const snapshot = {
      ...ownerSnapshot,
      state_version: 1,
      synced: true,
      stale: false,
      last_seen: Date.now(),
      operations: [],
      pending_requests: [],
      previews: [],
    };
    const fixture = await mountFileManagerFixture(snapshot, storage);
    const notice = () => element<HTMLButtonElement>(`[data-dismiss-unknown-action="${requestId}"]`);

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => {
      expect(notice()).toBeDefined();
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("1");
    });
    expect(snapshot.operations).not.toContainEqual(expect.objectContaining({ seq: payload.preview_seq }));
    expect(JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null")).toEqual([unknownQueueRow]);
    expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries).toContainEqual({
      key: cancellationKey,
      tracker: { requestId, seq: null, status: "unknown", error: "The prior cancellation delivery is unknown." },
    });

    failCancellationTrackerWrite = true;
    notice().click();
    expect(notice()).toBeDefined();
    expect(element<HTMLElement>("#waiting-count").textContent).toBe("1");
    expect(JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null")).toEqual([unknownQueueRow]);
    expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries).toContainEqual({
      key: cancellationKey,
      tracker: expect.objectContaining({ requestId, seq: null, status: "unknown" }),
    });
    expect(storage.getItem(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY)).toBeNull();
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);

    failCancellationTrackerWrite = false;
    failActionQueueWrite = true;
    notice().click();
    expect(notice()).toBeDefined();
    expect(element<HTMLElement>("#waiting-count").textContent).toBe("1");
    expect(JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null")).toEqual([unknownQueueRow]);
    expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries).toContainEqual({
      key: cancellationKey,
      tracker: expect.objectContaining({ requestId, seq: null, status: "unknown" }),
    });
    expect(storage.getItem(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY)).toBeNull();

    failActionQueueWrite = false;
    notice().click();
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("0");
      expect(document.querySelector(`[data-dismiss-unknown-action="${requestId}"]`)).toBeNull();
      expect(JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null")).toEqual([]);
      const persisted = JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null");
      expect(persisted?.entries ?? []).not.toContainEqual(expect.objectContaining({ key: cancellationKey }));
      expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY) ?? "null").entries).toContainEqual({
        key: cancellationKey,
        request_ids: [requestId],
      });
    });
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);

    const reloaded = await mountFileManagerFixture(snapshot, storage);
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("0");
      expect(document.querySelector(`[data-dismiss-unknown-action="${requestId}"]`)).toBeNull();
    });
    expect(reloaded.actions).toHaveLength(0);
    expect(reloaded.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
  });

  it("keeps a newer cancellation tracker when dismissing an older unknown queue row for the same target", async () => {
    const storage = makeMemoryStorage();
    const oldRequestId = "00000000-0000-4000-8000-000000000729";
    const newRequestId = "00000000-0000-4000-8000-000000000730";
    const payload = {
      preview_kind: "file_preview",
      preview_seq: 829,
      preview_id: "same-target-cancellation-retry",
      revision: "same-target-cancellation-retry-r1",
    };
    const key = previewCancellationStorageKey("file_preview", payload.preview_seq, payload.preview_id, payload.revision);
    const oldQueueRow = {
      ...createQueuedAction({
        request_id: oldRequestId,
        kind: "preview_cancel",
        payload,
        surface: ACTION_SURFACE_FILE,
      }),
      delivery_outcome: "outcome_unknown",
      delivery_error_code: "action_replay_window_unverifiable",
      delivery_error: "The prior cancellation delivery is unknown.",
    };
    storage.setItem("file-manager.actions.v1", JSON.stringify([oldQueueRow]));
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope([{
      key,
      requestId: newRequestId,
      seq: null,
      status: "unknown",
      error: "The newer cancellation delivery is unknown.",
    }]));
    const snapshot = {
      ...ownerSnapshot,
      state_version: 1,
      synced: true,
      stale: false,
      last_seen: Date.now(),
      operations: [],
      pending_requests: [],
      previews: [],
    };
    const fixture = await mountFileManagerFixture(snapshot, storage);
    const oldNotice = () => element<HTMLButtonElement>(`[data-dismiss-unknown-action="${oldRequestId}"]`);
    const storedTrackers = () => JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null")?.entries;

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => {
      expect(oldNotice()).toBeDefined();
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("2");
    });
    expect(storedTrackers()).toContainEqual({
      key,
      tracker: expect.objectContaining({ requestId: newRequestId, seq: null, status: "unknown" }),
    });

    oldNotice().click();
    await vi.waitFor(() => {
      expect(document.querySelector(`[data-dismiss-unknown-action="${oldRequestId}"]`)).toBeNull();
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("1");
      expect(JSON.parse(storage.getItem("file-manager.actions.v1") ?? "null")).toEqual([]);
      expect(storedTrackers()).toContainEqual({
        key,
        tracker: expect.objectContaining({ requestId: newRequestId, seq: null, status: "unknown" }),
      });
    });
    expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY) ?? "null").entries).toContainEqual({
      key,
      request_ids: [oldRequestId],
    });
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);

    const reloaded = await mountFileManagerFixture(snapshot, storage);
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("1");
      expect(document.querySelector(`[data-dismiss-unknown-action="${oldRequestId}"]`)).toBeNull();
      expect(storedTrackers()).toContainEqual({
        key,
        tracker: expect.objectContaining({ requestId: newRequestId, seq: null, status: "unknown" }),
      });
    });
    expect(reloaded.actions).toHaveLength(0);
  });

  it("surfaces and dismisses a verified pre-queue orphan without replay or stale-tab resurrection", async () => {
    const storage = makeMemoryStorage();
    const requestId = "00000000-0000-4000-8000-000000000722";
    const payload = {
      preview_kind: "file_preview",
      preview_seq: 822,
      preview_id: "verified-prequeue-orphan",
      revision: "verified-prequeue-orphan-r1",
    };
    const key = previewCancellationStorageKey("file_preview", payload.preview_seq, payload.preview_id, payload.revision);
    const intent = [{ key, requestId, seq: null, status: "queued" as const, error: null }];
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope(intent));
    const snapshot = {
      ...ownerSnapshot,
      state_version: 1,
      synced: true,
      stale: false,
      last_seen: Date.now(),
      operations: [],
      pending_requests: [],
      previews: [],
    };
    const fixture = await mountFileManagerFixture(snapshot, storage);
    const dismiss = () => element<HTMLButtonElement>(`#review-navigation-list [data-preview-cancel-dismiss-kind="file_preview"][data-preview-cancel-dismiss-id="${payload.preview_id}"]`);

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => {
      expect(dismiss()).toBeDefined();
      expect(element<HTMLElement>("#review-navigation-list").textContent).toContain("No saved cancellation action or receipt is available.");
      expect(element<HTMLElement>("#review-navigation-list").textContent).toContain("This browser will not resend it");
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("1");
    });
    expect(storage.getItem("file-manager.actions.v1")).toBeNull();
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
    expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries).toContainEqual({
      key,
      tracker: expect.objectContaining({ requestId, seq: null, status: "unknown" }),
    });

    dismiss().click();
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("0");
      expect(document.querySelector(`[data-preview-cancel-dismiss-id="${payload.preview_id}"]`)).toBeNull();
      const persisted = JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null");
      expect(persisted?.entries ?? []).not.toContainEqual(expect.objectContaining({ key }));
      expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY) ?? "null").entries).toContainEqual({
        key,
        request_ids: [requestId],
      });
    });

    const staleEnvelope = cancellationTrackerEnvelope(intent);
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, staleEnvelope);
    window.dispatchEvent(new StorageEvent("storage", {
      key: PREVIEW_CANCELLATION_STORAGE_KEY,
      newValue: staleEnvelope,
    }));
    await vi.waitFor(() => {
      const persisted = JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null");
      expect(persisted?.entries ?? []).not.toContainEqual(expect.objectContaining({ key }));
      expect(document.querySelector(`[data-preview-cancel-dismiss-id="${payload.preview_id}"]`)).toBeNull();
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("0");
    });

    const reloaded = await mountFileManagerFixture(snapshot, storage);
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
      expect(document.querySelector(`[data-preview-cancel-dismiss-id="${payload.preview_id}"]`)).toBeNull();
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("0");
    });
    expect(reloaded.actions).toHaveLength(0);
    expect(reloaded.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);

    const replacementRequestId = "00000000-0000-4000-8000-000000000723";
    const replacementEnvelope = cancellationTrackerEnvelope([{ ...intent[0]!, requestId: replacementRequestId }]);
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, replacementEnvelope);
    const replacement = await mountFileManagerFixture(snapshot, storage);
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#review-navigation-list").textContent).toContain("No saved cancellation action or receipt is available.");
      expect(document.querySelector(`[data-preview-cancel-dismiss-id="${payload.preview_id}"]`)).not.toBeNull();
      expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries).toContainEqual({
        key,
        tracker: expect.objectContaining({ requestId: replacementRequestId, seq: null, status: "unknown" }),
      });
    });
    expect(replacement.actions).toHaveLength(0);
    expect(replacement.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
  });

  it.each([
    "stale owner snapshot",
    "exact pending cancellation",
    "uncorrelated pending cancellation",
  ])("does not treat a %s as proof that a queued cancellation was unaccepted", async (blocker) => {
    const storage = makeMemoryStorage();
    const requestId = "00000000-0000-4000-8000-000000000724";
    const payload = {
      preview_kind: "file_preview",
      preview_seq: 824,
      preview_id: "blocked-prequeue-orphan",
      revision: "blocked-prequeue-orphan-r1",
    };
    const key = previewCancellationStorageKey("file_preview", payload.preview_seq, payload.preview_id, payload.revision);
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope([{
      key,
      requestId,
      seq: null,
      status: "queued",
      error: null,
    }]));
    const snapshot: Record<string, any> = {
      ...ownerSnapshot,
      state_version: 1,
      synced: true,
      stale: false,
      last_seen: Date.now(),
      operations: [],
      pending_requests: [],
      previews: [],
    };
    if (blocker === "stale owner snapshot") {
      snapshot.stale = true;
      snapshot.reported_at = Date.now() - 61 * 60 * 1_000;
      snapshot.last_seen = Date.now() - 11 * 60 * 1_000;
    } else if (blocker === "exact pending cancellation") {
      snapshot.pending_requests = [{
        seq: 925,
        request_id: requestId,
        kind: "preview_cancel",
        status: "pending",
        payload,
      }];
    } else {
      snapshot.pending_requests = [{
        seq: 926,
        request_id: "00000000-0000-4000-8000-000000000725",
        kind: "preview_cancel",
        status: "pending",
      }];
    }
    const fixture = await mountFileManagerFixture(snapshot, storage);

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));
    const visibleRequestText = `${element<HTMLElement>("#review-navigation-list").textContent} ${element<HTMLElement>("#waiting-list").textContent}`.toLowerCase();
    expect(visibleRequestText).toMatch(/cancel/iu);
    expect(document.querySelector(`[data-preview-cancel-dismiss-kind="file_preview"][data-preview-cancel-dismiss-id="${payload.preview_id}"]`)).toBeNull();
    expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries).toContainEqual({
      key,
      tracker: expect.objectContaining({ requestId, seq: null }),
    });
    expect(storage.getItem("file-manager.actions.v1")).toBeNull();
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
  });

  it("does not dismiss an in-flight cancellation intent if its local queue row disappears during snapshot refresh", async () => {
    const clock = controlledSnapshotClock();
    const storage = makeMemoryStorage();
    const previewId = "inflight-prequeue-orphan";
    const previewSeq = 828;
    const revision = "inflight-prequeue-orphan-r1";
    const initialSnapshot = {
      ...ownerSnapshot,
      state_version: 1,
      synced: true,
      stale: false,
      last_seen: Date.now(),
      operations: [filePreviewFixture(previewSeq, LIBRARY_ID, ROOT_BINDING, previewId, revision)],
      pending_requests: [],
      previews: [],
    };
    const actionCompletion: { release?: () => void } = {};
    let acceptedSeq = 0;
    const fixture = await mountFileManagerFixture(initialSnapshot, storage, (action, seq) => new Promise<MockActionResponse>((resolveResponse) => {
      acceptedSeq = seq;
      actionCompletion.release = () => resolveResponse({ status: 202, body: { seq, request_id: action.request_id } });
    }));
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    element<HTMLButtonElement>(`[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="${previewId}"]`).click();
    await vi.waitFor(() => expect(fixture.actions).toHaveLength(1));
    const requestId = String(fixture.actions[0]?.request_id ?? "");
    const payload = fixture.actions[0]?.payload as Record<string, unknown>;
    const key = previewCancellationStorageKey("file_preview", previewSeq, previewId, revision);
    expect(requestId).toMatch(/^[0-9a-f-]{36}$/u);
    expect(payload).toEqual({ preview_kind: "file_preview", preview_seq: previewSeq, preview_id: previewId, revision });
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(1);

    // Model a concurrent stale-tab write that removes the queue row while this tab still awaits its POST.
    storage.setItem("file-manager.actions.v1", "[]");
    const nextSnapshot: Record<string, any> = snapshotWithLibraryLabel({
      ...initialSnapshot,
      state_version: 2,
      last_seen: Date.now(),
      operations: [],
      pending_requests: [],
      previews: [],
    }, "No review target after in-flight cancellation");
    clock.advanceRefreshBudget();
    const requestsBefore = fixture.stateRequests;
    const manifestVersionsBefore = fixture.stateRequestVersions.length;
    await fixture.publishSnapshot(nextSnapshot);
    let liveSocket: MockWebSocket | undefined;
    await vi.waitFor(() => {
      liveSocket = latestOpenSocket(fixture.sockets);
      expect(liveSocket).toBeDefined();
    });
    liveSocket!.receive({
      type: "invalidate",
      database_epoch: TEST_DATABASE_EPOCH,
      state_version: nextSnapshot.state_version,
      reason: "state",
    });
    const releaseAction = actionCompletion.release as (() => void) | undefined;
    if (!releaseAction) throw new Error("Expected the cancellation API request to remain in flight");
    let released = false;
    try {
      await vi.waitFor(() => {
        const fetchedVersions = fixture.stateRequestVersions.slice(manifestVersionsBefore);
        expect(fetchedVersions).toContain(nextSnapshot.state_version);
        // The cache read completes while this deliberately held POST remains unresolved.
        expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
        expect(element<HTMLElement>("#library-select").textContent).toContain("No review target after in-flight cancellation");
      });
      const fetchedVersions = fixture.stateRequestVersions.slice(manifestVersionsBefore);
      expect(fixture.stateRequests, `expected one eligible state refresh; before=${requestsBefore}; after=${fixture.stateRequests}; fetched=[${fetchedVersions.join(",")}]`).toBe(requestsBefore + 1);
      expect(document.querySelector(`[data-preview-cancel-dismiss-kind="file_preview"][data-preview-cancel-dismiss-id="${previewId}"]`)).toBeNull();
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("1");
      expect(JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries).toContainEqual({
        key,
        tracker: expect.objectContaining({ requestId, seq: null }),
      });
      expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(1);
      expect(storage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(requestId)}`)).toBeNull();

      releaseAction();
      released = true;
      await vi.waitFor(() => {
        expect(acceptedSeq).toBeGreaterThan(0);
        const tracker = JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null")
          ?.entries?.find((entry: { key: string }) => entry.key === key)?.tracker;
        const receipt = storage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(requestId)}`);
        expect(tracker?.seq === acceptedSeq || receipt !== null).toBe(true);
        expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
      });
    } finally {
      if (!released) releaseAction();
      await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));
    }
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(1);
  });

  it("keeps a durable accepted receipt from being reclassified as an unaccepted orphan", async () => {
    const storage = makeMemoryStorage();
    const requestId = "00000000-0000-4000-8000-000000000726";
    const acceptedSeq = 927;
    const payload = {
      preview_kind: "file_preview",
      preview_seq: 826,
      preview_id: "accepted-receipt-not-orphan",
      revision: "accepted-receipt-not-orphan-r1",
    };
    const key = previewCancellationStorageKey("file_preview", payload.preview_seq, payload.preview_id, payload.revision);
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope([{
      key,
      requestId,
      seq: null,
      status: "queued",
      error: null,
    }]));
    const receiptKey = `${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(requestId)}`;
    const receipt = JSON.stringify({
      version: 1,
      request_id: requestId,
      surface: ACTION_SURFACE_FILE,
      kind: "preview_cancel",
      outcome: "accepted",
      seq: acceptedSeq,
      stored_at: Date.now(),
    });
    storage.setItem(receiptKey, receipt);
    const snapshot = {
      ...ownerSnapshot,
      state_version: 1,
      synced: true,
      stale: false,
      last_seen: Date.now(),
      operations: [],
      pending_requests: [],
      previews: [],
    };
    const fixture = await mountFileManagerFixture(snapshot, storage);

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));
    const persisted = JSON.parse(storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null");
    const recoveredTracker = persisted?.entries?.find((entry: { key: string }) => entry.key === key)?.tracker;
    expect(recoveredTracker).toBeDefined();
    expect(recoveredTracker?.status).not.toBe("failed");
    if (recoveredTracker?.seq === acceptedSeq) {
      expect(storage.getItem(receiptKey)).toBeNull();
    } else {
      expect(recoveredTracker?.seq).toBeNull();
      expect(storage.getItem(receiptKey)).toBe(receipt);
    }
    expect(element<HTMLElement>("#review-navigation-list").textContent).not.toContain("No saved cancellation action or receipt is available.");
    expect(storage.getItem("file-manager.actions.v1")).toBeNull();
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
  });

  it.each(["action queue", "receipt"])("keeps a queued cancellation visible and blocked when the %s cannot be read", async (unreadable) => {
    const memoryStorage = makeMemoryStorage();
    let failEvidenceRead = true;
    const storage: MemoryStorageFixture = {
      getItem(key) {
        if (failEvidenceRead && (unreadable === "action queue" && key === "file-manager.actions.v1"
            || unreadable === "receipt" && key === `${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent("00000000-0000-4000-8000-000000000727")}`)) {
          throw new Error(`Simulated ${unreadable} storage read failure`);
        }
        return memoryStorage.getItem(key);
      },
      setItem: memoryStorage.setItem,
      removeItem: memoryStorage.removeItem,
      clear: memoryStorage.clear,
    };
    const requestId = "00000000-0000-4000-8000-000000000727";
    const payload = {
      preview_kind: "file_preview",
      preview_seq: 827,
      preview_id: `unreadable-${unreadable.replaceAll(" ", "-")}-orphan`,
      revision: "unreadable-orphan-r1",
    };
    const key = previewCancellationStorageKey("file_preview", payload.preview_seq, payload.preview_id, payload.revision);
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, cancellationTrackerEnvelope([{
      key,
      requestId,
      seq: null,
      status: "queued",
      error: null,
    }]));
    const snapshot = {
      ...ownerSnapshot,
      state_version: 1,
      synced: true,
      stale: false,
      last_seen: Date.now(),
      operations: [],
      pending_requests: [],
      previews: [],
    };
    const fixture = await mountFileManagerFixture(snapshot, storage);

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));
    const visibleText = `${element<HTMLElement>("#review-navigation-list").textContent} ${element<HTMLElement>("#waiting-list").textContent}`.toLowerCase();
    expect(visibleText).toMatch(/cancel/iu);
    expect(document.querySelector(`[data-preview-cancel-dismiss-kind="file_preview"][data-preview-cancel-dismiss-id="${payload.preview_id}"]`)).toBeNull();
    expect(JSON.parse(memoryStorage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null").entries).toContainEqual({
      key,
      tracker: expect.objectContaining({ requestId, seq: null }),
    });
    expect(fixture.actions).toHaveLength(0);
    expect(fixture.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);

    failEvidenceRead = false;
  });

  it("shows the operation-history cap warning with a zero-result search and active operations at the cap", async () => {
    const snapshot = {
      ...ownerSnapshot,
      state_version: 1,
      synced: true,
      stale: false,
      last_seen: Date.now(),
      operations: [
        ...newerHistoryOperations(1_000, 99),
        {
          seq: 1_099,
          request_id: "active-operation-at-history-cap",
          kind: "library_scan",
          status: "running",
          result: {},
        },
      ],
      pending_requests: [],
      previews: [],
    };
    await mountFileManagerFixture(snapshot, makeMemoryStorage());

    element<HTMLButtonElement>('[data-view="history"]').click();
    const search = element<HTMLInputElement>("#history-search");
    search.value = "no-history-result-matches-this-query";
    search.dispatchEvent(new Event("input", { bubbles: true }));

    expect(snapshot.operations).toHaveLength(100);
    expect(element<HTMLElement>("#active-operation-list").textContent).toContain("Library scan");
    expect(element<HTMLElement>("#history-count").textContent).toBe("99");
    expect(element<HTMLElement>("#history-summary").textContent).toContain("0 matching recent records");
    expect(element<HTMLElement>("#history-summary").textContent).toContain("Older operation records may be omitted from this snapshot.");
  });

  it("keeps an accepted cancellation receipt and same request queued until tracker storage recovers", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const memoryStorage = makeMemoryStorage();
    let failCancellationTrackerWrite = false;
    const storage = {
      getItem: memoryStorage.getItem,
      setItem(key: string, value: string) {
        if (failCancellationTrackerWrite && key === PREVIEW_CANCELLATION_STORAGE_KEY) {
          throw new Error("Simulated cancellation tracker storage failure");
        }
        memoryStorage.setItem(key, value);
      },
      removeItem: memoryStorage.removeItem,
      clear: memoryStorage.clear,
    };
    const preview = filePreviewFixture(
      RECEIPT_FAILURE_PREVIEW_SEQ,
      LIBRARY_ID,
      ROOT_BINDING,
      RECEIPT_FAILURE_PREVIEW_ID,
      RECEIPT_FAILURE_PREVIEW_REVISION,
    );
    const snapshot = { ...ownerSnapshot, state_version: 1, operations: [preview], pending_requests: [], previews: [] };
    let acceptedServerActions = 0;
    let acceptedSeq = 0;
    const firstPage = await mountFileManagerFixture(snapshot, storage, (action, seq) => {
      if (action.kind !== "preview_cancel") throw new Error(`Unexpected action ${action.kind}`);
      acceptedServerActions += 1;
      acceptedSeq = seq;
      const key = previewCancellationStorageKey("file_preview", RECEIPT_FAILURE_PREVIEW_SEQ, RECEIPT_FAILURE_PREVIEW_ID, RECEIPT_FAILURE_PREVIEW_REVISION);
      const intent = JSON.parse(memoryStorage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null") as { entries?: Array<{ key: string; tracker: Record<string, unknown> }> } | null;
      expect(intent?.entries).toContainEqual({
        key,
        tracker: { requestId: action.request_id, seq: null, status: "queued", error: "" },
      });
      // The intent and action are already durable. Fail only the tracker update after receipt processing starts.
      failCancellationTrackerWrite = true;
      return { status: 202, body: { seq, request_id: action.request_id } };
    });

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    element<HTMLButtonElement>(`[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="${RECEIPT_FAILURE_PREVIEW_ID}"]`).click();
    await vi.waitFor(() => {
      expect(firstPage.actions).toHaveLength(1);
      const action = firstPage.actions[0]!;
      const queued = JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null") as Array<Record<string, any>>;
      expect(queued).toHaveLength(1);
      expect(queued[0]).toMatchObject({ request_id: action.request_id, kind: "preview_cancel", payload: action.payload });
      const receipt = JSON.parse(memoryStorage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`) ?? "null");
      expect(receipt).toMatchObject({
        version: 1,
        request_id: action.request_id,
        surface: ACTION_SURFACE_FILE,
        kind: "preview_cancel",
        outcome: "accepted",
        seq: acceptedSeq,
      });
    });
    const acceptedAction = firstPage.actions[0]!;
    const receiptKey = `${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(acceptedAction.request_id)}`;
    expect(acceptedServerActions).toBe(1);
    expect(firstPage.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(1);

    // Model an aged queue record: its six-day replay window has expired, while the accepted receipt remains durable.
    const now = Date.now();
    const queued = JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null") as Array<Record<string, any>>;
    const agedAction = {
      ...queued[0],
      queued_at_ms: now - ACTION_REPLAY_WINDOW_MS - 1,
      replay_expires_at_ms: now - 1,
    };
    memoryStorage.setItem("file-manager.actions.v1", JSON.stringify([agedAction]));
    expect(agedAction.replay_expires_at_ms).toBeLessThanOrEqual(now);

    const reloadedPage = await mountFileManagerFixture(snapshot, storage, () => {
      throw new Error("An accepted cancellation must not be posted again during receipt recovery");
    });
    element<HTMLButtonElement>('[data-view="waiting"]').click();
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));
    expect(JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null")).toEqual([agedAction]);
    expect(JSON.parse(memoryStorage.getItem(receiptKey) ?? "null")).toMatchObject({ outcome: "accepted", seq: acceptedSeq });
    expect(reloadedPage.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
    expect(reloadedPage.actions).toHaveLength(0);

    // A cross-tab-style receipt recovery can now persist the tracker, consume the receipt, and remove the same UUID.
    failCancellationTrackerWrite = false;
    Object.defineProperty(window.navigator, "onLine", { configurable: true, value: true });
    const storageEvent = new Event("storage");
    Object.defineProperty(storageEvent, "key", { configurable: true, value: receiptKey });
    window.dispatchEvent(storageEvent);
    await vi.waitFor(() => {
      expect(JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null")).toEqual([]);
      expect(memoryStorage.getItem(receiptKey)).toBeNull();
      const persisted = JSON.parse(memoryStorage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null") as { entries?: Array<{ key: string; tracker: Record<string, unknown> }> } | null;
      expect(persisted?.entries).toContainEqual({
        key: previewCancellationStorageKey("file_preview", RECEIPT_FAILURE_PREVIEW_SEQ, RECEIPT_FAILURE_PREVIEW_ID, RECEIPT_FAILURE_PREVIEW_REVISION),
        tracker: { requestId: acceptedAction.request_id, seq: acceptedSeq, status: "submitted", error: null },
      });
    });
    expect(acceptedServerActions).toBe(1);
    expect(reloadedPage.actions).toHaveLength(0);
    expect(reloadedPage.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
  });

  it("retains receipt evidence when queue removal fails through terminal snapshot and reload", async () => {
    vi.useFakeTimers();
    vi.stubGlobal("crypto", webcrypto);
    const memoryStorage = makeMemoryStorage();
    let failActionQueueWrite = false;
    let actionQueueWriteAttempts = 0;
    const storage = {
      getItem: memoryStorage.getItem,
      setItem(key: string, value: string) {
        if (key === "file-manager.actions.v1") {
          actionQueueWriteAttempts += 1;
          if (failActionQueueWrite) throw new Error("Simulated action queue storage failure");
        }
        memoryStorage.setItem(key, value);
      },
      removeItem: memoryStorage.removeItem,
      clear: memoryStorage.clear,
    };
    const preview = filePreviewFixture(
      RECEIPT_FAILURE_PREVIEW_SEQ,
      LIBRARY_ID,
      ROOT_BINDING,
      RECEIPT_FAILURE_PREVIEW_ID,
      RECEIPT_FAILURE_PREVIEW_REVISION,
    );
    const initialSnapshot = { ...ownerSnapshot, state_version: 1, operations: [preview], pending_requests: [], previews: [] };
    let acceptedServerActions = 0;
    let acceptedSeq = 0;
    const firstPage = await mountFileManagerFixture(initialSnapshot, storage, (action, seq) => {
      if (action.kind !== "preview_cancel") throw new Error(`Unexpected action ${action.kind}`);
      acceptedServerActions += 1;
      acceptedSeq = seq;
      failActionQueueWrite = true;
      return { status: 202, body: { seq, request_id: action.request_id } };
    });

    element<HTMLButtonElement>('[data-view="waiting"]').click();
    element<HTMLButtonElement>(`[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="${RECEIPT_FAILURE_PREVIEW_ID}"]`).click();
    await vi.waitFor(() => {
      expect(firstPage.actions).toHaveLength(1);
      const action = firstPage.actions[0]!;
      const queued = JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null") as Array<Record<string, any>>;
      expect(queued).toHaveLength(1);
      expect(queued[0]).toMatchObject({ request_id: action.request_id, kind: "preview_cancel", payload: action.payload });
      const receiptKey = `${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`;
      expect(JSON.parse(memoryStorage.getItem(receiptKey) ?? "null")).toMatchObject({ outcome: "accepted", seq: acceptedSeq });
      const cancellationState = JSON.parse(memoryStorage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null") as { entries?: Array<{ key: string; tracker: Record<string, unknown> }> } | null;
      expect(cancellationState?.entries).toContainEqual({
        key: previewCancellationStorageKey("file_preview", RECEIPT_FAILURE_PREVIEW_SEQ, RECEIPT_FAILURE_PREVIEW_ID, RECEIPT_FAILURE_PREVIEW_REVISION),
        tracker: { requestId: action.request_id, seq: acceptedSeq, status: "submitted", error: null },
      });
      expect(actionQueueWriteAttempts).toBeGreaterThan(1);
    });
    await vi.waitFor(() => {
      expect(element<HTMLElement>("#toast").textContent.toLowerCase()).toContain("could not save the action queue");
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
    });
    const acceptedAction = firstPage.actions[0]!;
    const receiptKey = `${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(acceptedAction.request_id)}`;
    expect(acceptedServerActions).toBe(1);

    const terminalSnapshot = {
      ...initialSnapshot,
      state_version: 2,
      operations: [
        { ...preview, status: "cancelled" },
        {
          seq: acceptedSeq,
          request_id: acceptedAction.request_id,
          kind: "preview_cancel",
          status: "completed",
          result: { cancelled: true, ...acceptedAction.payload },
        },
      ],
      pending_requests: [],
      previews: [],
    };
    await firstPage.publishSnapshot(terminalSnapshot);
    // Advance only the client refresh budget; the WebSocket invalidation still arrives as its own timer task.
    vi.setSystemTime(new Date(Date.now() + SNAPSHOT_REFRESH_START_BUDGET_MS));
    const stateRequestsBeforeTerminal = firstPage.stateRequests;
    const versionsBeforeTerminal = firstPage.stateRequestVersions.length;
    const invalidationTask = new Promise<void>((resolve, reject) => window.setTimeout(() => {
      try {
        const liveSocket = latestOpenSocket(firstPage.sockets);
        if (!liveSocket) throw new Error("Expected a live updates connection before terminal cancellation snapshot invalidation.");
        liveSocket.receive({
          type: "invalidate",
          database_epoch: TEST_DATABASE_EPOCH,
          state_version: terminalSnapshot.state_version,
          reason: "state",
        });
        resolve();
      } catch (error) {
        reject(error);
      }
    }, 0));
    await vi.advanceTimersByTimeAsync(0);
    await invalidationTask;
    await vi.waitFor(() => {
      expect(firstPage.stateRequests).toBe(stateRequestsBeforeTerminal + 1);
      expect(firstPage.stateRequestVersions.slice(versionsBeforeTerminal)).toContain(terminalSnapshot.state_version);
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
    });
    const terminalQueue = JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null") as Array<Record<string, any>>;
    expect(terminalQueue).toHaveLength(1);
    expect(terminalQueue[0]?.request_id).toBe(acceptedAction.request_id);
    expect(JSON.parse(memoryStorage.getItem(receiptKey) ?? "null")).toMatchObject({ outcome: "accepted", seq: acceptedSeq });
    const cancellationKey = previewCancellationStorageKey("file_preview", RECEIPT_FAILURE_PREVIEW_SEQ, RECEIPT_FAILURE_PREVIEW_ID, RECEIPT_FAILURE_PREVIEW_REVISION);
    const terminalTrackers = JSON.parse(memoryStorage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null") as { entries?: Array<{ key: string; tracker: Record<string, unknown> }> } | null;
    expect(terminalTrackers?.entries).toContainEqual({
      key: cancellationKey,
      tracker: { requestId: acceptedAction.request_id, seq: acceptedSeq, status: "submitted", error: null },
    });

    // Also exercise recovery if another tab had already retired its copy of this terminal tracker.
    memoryStorage.removeItem(PREVIEW_CANCELLATION_STORAGE_KEY);

    // The action is now older than its safe replay window; the accepted receipt remains the only durable proof.
    const now = Date.now();
    const agedAction = {
      ...terminalQueue[0],
      queued_at_ms: now - ACTION_REPLAY_WINDOW_MS - 1,
      replay_expires_at_ms: now - 1,
    };
    memoryStorage.setItem("file-manager.actions.v1", JSON.stringify([agedAction]));
    const reloadedPage = await mountFileManagerFixture(terminalSnapshot, storage, () => {
      throw new Error("A terminal cancellation receipt must never be posted again");
    });
    await vi.waitFor(() => expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false"));
    expect(JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null")).toHaveLength(1);
    expect(JSON.parse(memoryStorage.getItem(receiptKey) ?? "null")).toMatchObject({ outcome: "accepted", seq: acceptedSeq });
    expect(reloadedPage.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
    expect(reloadedPage.actions).toHaveLength(0);

    failActionQueueWrite = false;
    const attemptsBeforeRecovery = actionQueueWriteAttempts;
    const onlineEvent = new Event("online");
    window.dispatchEvent(onlineEvent);
    await vi.waitFor(() => {
      expect(actionQueueWriteAttempts).toBeGreaterThan(attemptsBeforeRecovery);
      expect(JSON.parse(memoryStorage.getItem("file-manager.actions.v1") ?? "null")).toEqual([]);
      expect(memoryStorage.getItem(receiptKey)).toBeNull();
    });
    expect(acceptedServerActions).toBe(1);
    expect(reloadedPage.actions).toHaveLength(0);
    expect(reloadedPage.fetchMock.mock.calls.filter(([input]) => String(input) === "/api/actions")).toHaveLength(0);
  });

  it("restores an accepted cancellation after reload and settles it only from a terminal snapshot", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const clock = controlledSnapshotClock();
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
    clock.advanceRefreshBudget();
    const stateRequestsBeforeCancel = firstPage.stateRequests;
    const versionsBeforeCancel = firstPage.stateRequestVersions.length;
    element<HTMLButtonElement>('[data-preview-cancel-kind="file_preview"][data-preview-cancel-id="reload-cancel-preview"]').click();
    await vi.waitFor(() => expect(acceptedActions).toHaveLength(1));
    await vi.waitFor(() => {
      expect(firstPage.stateRequests).toBeGreaterThan(stateRequestsBeforeCancel);
      expect(firstPage.stateRequestVersions.slice(versionsBeforeCancel)).toContain(2);
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
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
    await vi.waitFor(() => expect(firstPage.sockets[0]?.readyState).toBe(MockWebSocket.CLOSED));
    await vi.waitFor(() => expect(reloadedPage.sockets).toHaveLength(1));
    await vi.waitFor(() => expect(reloadedPage.sockets[0]?.readyState).toBe(MockWebSocket.OPEN));
    expect(reloadedPage.stateRequestVersions).toContain(2);
    expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
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
    clock.advanceRefreshBudget();
    const stateRequestsBeforeTerminal = reloadedPage.stateRequests;
    const versionsBeforeTerminal = reloadedPage.stateRequestVersions.length;
    await new Promise<void>((resolve, reject) => window.setTimeout(() => {
      try {
        const liveSocket = latestOpenSocket(reloadedPage.sockets);
        if (!liveSocket) throw new Error("Expected a live updates connection before terminal cancellation snapshot invalidation.");
        liveSocket.receive({
          type: "invalidate",
          database_epoch: TEST_DATABASE_EPOCH,
          state_version: 3,
          reason: "state",
        });
        resolve();
      } catch (error) {
        reject(error);
      }
    }, 0));
    await vi.waitFor(() => {
      expect(reloadedPage.stateRequestVersions.slice(versionsBeforeTerminal)).toContain(3);
      expect(element<HTMLElement>("#app-content").getAttribute("aria-busy")).toBe("false");
      expect(element<HTMLElement>("#history-count").textContent).toBe("2");
      expect(element<HTMLElement>("#history-summary").textContent).toContain("2 recent records retained");
      expect(element<HTMLElement>("#waiting-count").textContent).toBe("0");
      expect(document.querySelector('[data-preview-cancel-id="reload-cancel-preview"]')).toBeNull();
    });
    expect(reloadedPage.stateRequests).toBe(stateRequestsBeforeTerminal + 1);

    element<HTMLButtonElement>('[data-view="history"]').click();
    expect(element<HTMLInputElement>("#history-search").value).toBe("");
    expect(element<HTMLElement>("#history-summary").textContent).toContain("2 recent records retained");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Preview cancellation");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Completed");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("File preview");
    expect(element<HTMLElement>("#operation-list").textContent).toContain("Cancelled");
  });
});
