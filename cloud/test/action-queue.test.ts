import { describe, expect, it, vi } from "vitest";
import {
  ACTION_RECEIPT_STORAGE_PREFIX,
  ACTION_SURFACE_FILE,
  ACTION_SURFACE_SETTINGS,
  actionBelongsToSurface,
  consumeRejectedActionReceipts,
  dispatchNextOwnedAction,
  recoverActionReceipts,
  recordActionReceipt,
  restorePendingActions,
  type DownloadSubmissionReceiptTracker,
  type LocalActionReceiptTracker,
  type MigrationConflictReceiptTracker,
  type QueuedAction,
} from "../public/file/action-queue.js";

type RejectionTrackerState = {
  localActions?: ReadonlyMap<string, LocalActionReceiptTracker>;
  downloadSubmission?: DownloadSubmissionReceiptTracker | null;
  migrationConflictDrafts?: ReadonlyMap<string, MigrationConflictReceiptTracker>;
};

function sharedQueue(initial: QueuedAction[]) {
  let value = JSON.stringify(initial);
  return {
    read() {
      return JSON.parse(value) as QueuedAction[];
    },
    write(queue: QueuedAction[]) {
      value = JSON.stringify(queue);
      return true;
    },
    contents() {
      return JSON.parse(value) as QueuedAction[];
    },
  };
}

class MemoryStorage {
  values = new Map<string, string>();
  failWrites = false;
  get length() { return this.values.size; }
  key(index: number) { return [...this.values.keys()][index] ?? null; }
  getItem(key: string) { return this.values.get(key) ?? null; }
  setItem(key: string, value: string) {
    if (this.failWrites) throw new Error("storage full");
    this.values.set(key, value);
  }
  removeItem(key: string) { this.values.delete(key); }
  clear() { this.values.clear(); }
}

describe("shared Mini App action queue ownership", () => {
  it("lets both pages drain their own entries and retain each page's receipt state", async () => {
    const actions: QueuedAction[] = [
      { request_id: "file-download", kind: "download_submit", payload: {}, surface: ACTION_SURFACE_FILE },
      { request_id: "settings-rename", kind: "library_rename", payload: {}, surface: ACTION_SURFACE_SETTINGS },
      { request_id: "file-refresh", kind: "library_migration_preview", payload: { supersedes_preview_seq: 79 }, surface: ACTION_SURFACE_FILE },
      { request_id: "file-scan", kind: "library_scan", payload: {}, surface: ACTION_SURFACE_FILE },
      { request_id: "settings-scan", kind: "library_scan", payload: {}, surface: ACTION_SURFACE_SETTINGS },
      { request_id: "legacy-file-preview", kind: "file_preview", payload: {} },
    ];
    const storage = sharedQueue(actions);
    const localActions = new Map([
      ["settings-rename", { kind: "library_rename", payload: {}, seq: null as number | null }],
      ["settings-scan", { kind: "library_scan", payload: {}, seq: null as number | null }],
    ]);
    const settingsState = { localActions };
    const fileState = {
      downloadSubmission: { request_id: "file-download", seq: null as number | null, payload: {} },
      migrationConflictDrafts: new Map([
        ["preview-old", { refreshRequestId: "file-refresh", refreshPayload: { supersedes_preview_seq: 79 }, refreshSeq: null as number | null }],
      ]),
    };
    const receiptStorage = new MemoryStorage();
    const sent: Array<{ surface: string; request_id: string }> = [];
    let nextSeq = 80;

    const drain = async (surface: "settings" | "file") => {
      while (true) {
        const outcome = await dispatchNextOwnedAction({
          surface,
          readQueue: storage.read,
          writeQueue: storage.write,
          dispatch: async (action) => {
            sent.push({ surface, request_id: action.request_id });
            await Promise.resolve();
            nextSeq += 1;
            return { seq: nextSeq };
          },
          onReceipt: (action, receipt, resolvedSurface) => recordActionReceipt(
            action,
            receipt,
            surface === ACTION_SURFACE_SETTINGS ? settingsState : fileState,
            { surface: resolvedSurface, storage: receiptStorage },
          ),
        });
        if (outcome.status === "empty") return;
        if (outcome.status !== "sent") throw new Error(`Unexpected queue result: ${outcome.status}`);
      }
    };

    await Promise.all([drain(ACTION_SURFACE_SETTINGS), drain(ACTION_SURFACE_FILE)]);

    expect(sent).toHaveLength(actions.length);
    expect(sent.filter((entry) => entry.surface === ACTION_SURFACE_SETTINGS).map((entry) => entry.request_id)).toEqual([
      "settings-rename", "settings-scan",
    ]);
    expect(sent.filter((entry) => entry.surface === ACTION_SURFACE_FILE).map((entry) => entry.request_id)).toEqual([
      "file-download", "file-refresh", "file-scan", "legacy-file-preview",
    ]);
    expect(fileState.downloadSubmission.seq).toBeGreaterThan(0);
    expect(fileState.migrationConflictDrafts.get("preview-old")?.refreshSeq).toBeGreaterThan(0);
    expect(localActions.get("settings-rename")?.seq).toBeGreaterThan(0);
    expect(localActions.get("settings-scan")?.seq).toBeGreaterThan(0);
    expect(storage.contents()).toEqual([]);
  });

  it("keeps foreign entries while finding later owned work and preserves malformed surface tags", () => {
    const settings = { request_id: "settings", kind: "library_remove", payload: {}, surface: ACTION_SURFACE_SETTINGS };
    const file = { request_id: "file", kind: "file_preview", payload: {}, surface: ACTION_SURFACE_FILE };
    const malformed = { request_id: "malformed", kind: "download_submit", payload: {}, surface: "other" } as unknown as QueuedAction;

    expect(actionBelongsToSurface(malformed, ACTION_SURFACE_FILE)).toBe(false);
    expect(actionBelongsToSurface(malformed, ACTION_SURFACE_SETTINGS)).toBe(false);
    expect(actionBelongsToSurface({ request_id: "legacy-settings", kind: "library_scan", payload: {} }, ACTION_SURFACE_SETTINGS)).toBe(true);
    expect(actionBelongsToSurface({ request_id: "legacy-file", kind: "file_preview", payload: {} }, ACTION_SURFACE_FILE)).toBe(true);
    expect(actionBelongsToSurface(file, ACTION_SURFACE_SETTINGS)).toBe(false);
    expect(actionBelongsToSurface(settings, ACTION_SURFACE_FILE)).toBe(false);
  });

  it("retains the same action on send failure and records a retry receipt before failed removal", async () => {
    const action: QueuedAction = { request_id: "file-download", kind: "download_submit", payload: {}, surface: ACTION_SURFACE_FILE };
    const storage = sharedQueue([action]);
    const state = { downloadSubmission: { request_id: action.request_id, seq: null as number | null } };
    const receiptStorage = new MemoryStorage();
    const onReceipt = vi.fn((entry: QueuedAction, receipt: unknown) =>
      recordActionReceipt(entry, receipt, state, { surface: ACTION_SURFACE_FILE, storage: receiptStorage }),
    );
    const failedSend = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: storage.read,
      writeQueue: storage.write,
      dispatch: async () => { throw new TypeError("temporary connection error"); },
      onReceipt,
    });

    expect(failedSend.status).toBe("error");
    expect(storage.contents()).toEqual([action]);
    expect(onReceipt).not.toHaveBeenCalled();

    const failedRemoval = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: storage.read,
      writeQueue: () => false,
      dispatch: async () => ({ seq: 81 }),
      onReceipt,
    });
    expect(failedRemoval.status).toBe("remove_failed");
    expect(state.downloadSubmission.seq).toBe(81);
    expect(storage.contents()).toEqual([action]);
  });

  it.each([
    ["download submission", "download_submit", { url: "https://example.test/video", library_id: "main", category: "" }],
    ["conflict refresh", "library_migration_preview", { supersedes_preview_seq: 4, supersedes_preview_id: "old", supersedes_revision: "rev-old" }],
  ] as const)("lets the originating tab recover a deduplicated %s receipt", async (_label, kind, payload) => {
    const action: QueuedAction = { request_id: `origin-${kind}`, kind, payload, surface: ACTION_SURFACE_FILE };
    const queue = sharedQueue([action]);
    const receiptStorage = new MemoryStorage();
    const originState = kind === "download_submit"
      ? { downloadSubmission: { request_id: action.request_id, seq: null as number | null, payload } }
      : { migrationConflictDrafts: new Map([[
        "old-preview",
        { refreshRequestId: action.request_id, refreshSeq: null as number | null, refreshPayload: payload },
      ]]) };
    const helperState = { migrationConflictDrafts: new Map() };
    let acceptedSeq: number | null = null;

    const lostOriginResponse = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: queue.read,
      writeQueue: queue.write,
      dispatch: async (entry) => {
        acceptedSeq = 93;
        throw new TypeError(`response lost after accepting ${entry.request_id}`);
      },
      onReceipt: (entry, receipt, surface) => recordActionReceipt(entry, receipt, originState, { surface, storage: receiptStorage }),
    });
    expect(lostOriginResponse.status).toBe("error");
    expect(queue.contents()).toEqual([action]);

    const result = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: queue.read,
      writeQueue: queue.write,
      dispatch: async (entry) => {
        expect(entry.request_id).toBe(action.request_id);
        return { seq: acceptedSeq };
      },
      onReceipt: (entry, receipt, surface) => recordActionReceipt(entry, receipt, helperState, { surface, storage: receiptStorage }),
    });

    expect(result.status).toBe("sent");
    expect(queue.contents()).toEqual([]);
    expect(receiptStorage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`)).not.toBeNull();
    if ("downloadSubmission" in originState) expect(originState.downloadSubmission.seq).toBeNull();
    else expect(originState.migrationConflictDrafts.get("old-preview")?.refreshSeq).toBeNull();

    const recovered = recoverActionReceipts(originState, { surface: ACTION_SURFACE_FILE, storage: receiptStorage });
    expect(recovered.matched).toBe(1);
    expect(recovered.requestIds).toEqual([action.request_id]);
    if ("downloadSubmission" in originState) expect(originState.downloadSubmission.seq).toBe(93);
    else expect(originState.migrationConflictDrafts.get("old-preview")?.refreshSeq).toBe(93);
    expect(receiptStorage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`)).toBeNull();
  });

  it("does not dequeue a delivered action when durable receipt storage fails", async () => {
    const action: QueuedAction = { request_id: "receipt-storage-failure", kind: "download_submit", payload: {}, surface: ACTION_SURFACE_FILE };
    const queue = sharedQueue([action]);
    const receiptStorage = new MemoryStorage();
    receiptStorage.failWrites = true;
    const outcome = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: queue.read,
      writeQueue: queue.write,
      dispatch: async () => ({ seq: 94 }),
      onReceipt: (entry, receipt, surface) => recordActionReceipt(entry, receipt, {}, { surface, storage: receiptStorage }),
    });

    expect(outcome.status).toBe("receipt_failed");
    expect(queue.contents()).toEqual([action]);
  });

  it("leaves an unmatched receipt for its sleeping origin tab to consume", () => {
    const action: QueuedAction = { request_id: "sleeping-origin", kind: "download_submit", payload: { url: "https://example.test/v" }, surface: ACTION_SURFACE_FILE };
    const receiptStorage = new MemoryStorage();
    const helperTab = { migrationConflictDrafts: new Map() };
    const recorded = recordActionReceipt(action, { seq: 95 }, helperTab, { surface: ACTION_SURFACE_FILE, storage: receiptStorage, now: 1_000 });
    expect(recorded.persisted).toBe(true);
    const key = `${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`;
    const helperRecovery = recoverActionReceipts(helperTab, { surface: ACTION_SURFACE_FILE, storage: receiptStorage });
    expect(helperRecovery.matched).toBe(0);
    expect(receiptStorage.getItem(key)).not.toBeNull();

    const sleepingTab = { downloadSubmission: { request_id: action.request_id, seq: null as number | null } };
    const recovered = recoverActionReceipts(sleepingTab, { surface: ACTION_SURFACE_FILE, storage: receiptStorage });

    expect(recovered.matched).toBe(1);
    expect(sleepingTab.downloadSubmission.seq).toBe(95);
    expect(receiptStorage.getItem(key)).toBeNull();
  });

  it("recovers a deduplicated settings receipt only in the tab that owns its local action", () => {
    const action: QueuedAction = {
      request_id: "settings-origin",
      kind: "library_rename",
      payload: { library_id: "archive", name: "Archive" },
      surface: ACTION_SURFACE_SETTINGS,
    };
    const receiptStorage = new MemoryStorage();
    const originState = {
      localActions: new Map([[action.request_id, {
        kind: action.kind,
        payload: action.payload,
        seq: null as number | null,
      }]]),
    };
    const helperState = { localActions: new Map() };

    const helperReceipt = recordActionReceipt(action, { seq: 96 }, helperState, {
      surface: ACTION_SURFACE_SETTINGS,
      storage: receiptStorage,
    });
    expect(helperReceipt).toMatchObject({ persisted: true, matched: false });
    expect(originState.localActions.get(action.request_id)?.seq).toBeNull();

    const recovered = recoverActionReceipts(originState, {
      surface: ACTION_SURFACE_SETTINGS,
      storage: receiptStorage,
    });
    expect(recovered).toMatchObject({ matched: 1, localActions: true });
    expect(originState.localActions.get(action.request_id)?.seq).toBe(96);
    expect(receiptStorage.length).toBe(0);
  });

  it("restores a tracker-backed action with the same request ID when no receipt is available", () => {
    const state = {
      downloadSubmission: {
        request_id: "restored-download",
        seq: null as number | null,
        payload: { url: "https://example.test/v", library_id: "archive", category: "" },
      },
    };
    const queue = sharedQueue([]);
    const restored = restorePendingActions(state, {
      surface: ACTION_SURFACE_FILE,
      readQueue: queue.read,
      writeQueue: queue.write,
    });

    expect(restored.requeued).toBe(1);
    expect(queue.contents()).toEqual([{
      request_id: "restored-download",
      kind: "download_submit",
      payload: state.downloadSubmission.payload,
      surface: ACTION_SURFACE_FILE,
    }]);
  });

  it("delivers permanent rejections only to the matching download, settings, and conflict origin", async () => {
    const cases = [
      {
        surface: ACTION_SURFACE_FILE,
        kind: "download_submit",
        payload: { url: "https://example.test/video", library_id: "main", category: "" },
        origin: (requestId: string): RejectionTrackerState => ({
          downloadSubmission: { request_id: requestId, seq: null as number | null, payload: { url: "https://example.test/video" } },
        }),
        helper: (): RejectionTrackerState => ({ downloadSubmission: null }),
      },
      {
        surface: ACTION_SURFACE_SETTINGS,
        kind: "library_rename",
        payload: { library_id: "archive", name: "Archive" },
        origin: (requestId: string): RejectionTrackerState => ({
          localActions: new Map([[requestId, { kind: "library_rename", payload: { library_id: "archive", name: "Archive" }, seq: null as number | null }]]),
        }),
        helper: (): RejectionTrackerState => ({ localActions: new Map() }),
      },
      {
        surface: ACTION_SURFACE_FILE,
        kind: "library_migration_preview",
        payload: { supersedes_preview_seq: 17, supersedes_preview_id: "old", supersedes_revision: "r1" },
        origin: (requestId: string): RejectionTrackerState => ({
          migrationConflictDrafts: new Map([["old", {
            refreshRequestId: requestId,
            refreshPayload: { supersedes_preview_seq: 17 },
            refreshSeq: null as number | null,
            refreshError: null as string | null,
          }]]),
        }),
        helper: (): RejectionTrackerState => ({ migrationConflictDrafts: new Map() }),
      },
    ] as const;

    for (const [index, scenario] of cases.entries()) {
      const requestId = `permanent-rejection-${index}`;
      const action: QueuedAction = {
        request_id: requestId,
        kind: scenario.kind,
        payload: scenario.payload,
        surface: scenario.surface,
      };
      const queue = sharedQueue([action]);
      const receiptStorage = new MemoryStorage();
      const originState = scenario.origin(requestId);
      const helperState = scenario.helper();
      const result = await dispatchNextOwnedAction({
        surface: scenario.surface,
        readQueue: queue.read,
        writeQueue: queue.write,
        dispatch: async () => { throw Object.assign(new Error(`Invalid input ${"é".repeat(200)}\nfor this action`), { status: 422 }); },
        onReceipt: (entry, receipt, surface) => recordActionReceipt(entry, receipt, helperState, { surface, storage: receiptStorage, now: 2_000 }),
      });

      expect(result.status).toBe("rejected");
      expect(queue.contents()).toEqual([]);
      const key = `${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(requestId)}`;
      const persisted = JSON.parse(receiptStorage.getItem(key) ?? "null");
      expect(persisted).toMatchObject({ version: 1, request_id: requestId, surface: scenario.surface, kind: scenario.kind, outcome: "rejected", status: 422 });
      expect(new TextEncoder().encode(persisted.error).byteLength).toBeLessThanOrEqual(256);
      expect(persisted.error).not.toMatch(/[\u0000-\u001f\u007f]/u);

      const helperRecovery = recoverActionReceipts(helperState, { surface: scenario.surface, storage: receiptStorage });
      expect(helperRecovery.matched).toBe(0);
      expect(receiptStorage.getItem(key)).not.toBeNull();

      const originRecovery = recoverActionReceipts(originState, { surface: scenario.surface, storage: receiptStorage });
      expect(originRecovery.matched).toBe(1);
      expect(originRecovery.rejectedRequestIds).toEqual([requestId]);
      if (scenario.surface === ACTION_SURFACE_SETTINGS) {
        const local = originState.localActions?.get(requestId);
        expect(local).toMatchObject({ rejected: true, error: persisted.error, seq: null });
        expect(restorePendingActions(originState, { surface: scenario.surface, readQueue: () => [], writeQueue: () => true }).requeued).toBe(0);
      } else if (scenario.kind === "download_submit") {
        expect(originState.downloadSubmission).toMatchObject({ rejected: true, error: persisted.error, seq: null });
        expect(restorePendingActions(originState, { surface: scenario.surface, readQueue: () => [], writeQueue: () => true }).requeued).toBe(0);
      } else {
        const draft = originState.migrationConflictDrafts?.get("old");
        expect(draft).toMatchObject({ refreshRequestId: null, rejectedRefreshRequestId: requestId, refreshPayload: null, refreshError: persisted.error });
        expect(restorePendingActions(originState, { surface: scenario.surface, readQueue: () => [], writeQueue: () => true }).requeued).toBe(0);
      }
      expect(consumeRejectedActionReceipts(originState, { surface: scenario.surface, requestIds: [requestId], storage: receiptStorage }).consumed).toBe(1);
      expect(receiptStorage.getItem(key)).toBeNull();
    }
  });

  it("retains permanently rejected actions if rejection receipt storage fails, then retries the same ID", async () => {
    const action: QueuedAction = { request_id: "rejection-storage-retry", kind: "download_submit", payload: { url: "https://example.test/video" }, surface: ACTION_SURFACE_FILE };
    const queue = sharedQueue([action]);
    const receiptStorage = new MemoryStorage();
    receiptStorage.failWrites = true;
    const onReceipt = (entry: QueuedAction, receipt: unknown, surface: "settings" | "file") =>
      recordActionReceipt(entry, receipt, {}, { surface, storage: receiptStorage, now: 3_000 });
    const failed = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: queue.read,
      writeQueue: queue.write,
      dispatch: async () => { throw Object.assign(new Error("Invalid request"), { status: 422 }); },
      onReceipt,
    });
    expect(failed.status).toBe("rejection_receipt_failed");
    expect(queue.contents()).toEqual([action]);
    expect(receiptStorage.length).toBe(0);

    receiptStorage.failWrites = false;
    const retried = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: queue.read,
      writeQueue: queue.write,
      dispatch: async (entry) => {
        expect(entry.request_id).toBe(action.request_id);
        throw Object.assign(new Error("Invalid request"), { status: 422 });
      },
      onReceipt,
    });
    expect(retried.status).toBe("rejected");
    expect(queue.contents()).toEqual([]);
  });

  it.each([401, 403, 408, 429, 500, 600])("does not persist a rejection for status %i", async (status) => {
    const action: QueuedAction = { request_id: `not-terminal-${status}`, kind: "download_submit", payload: {}, surface: ACTION_SURFACE_FILE };
    const queue = sharedQueue([action]);
    const receiptStorage = new MemoryStorage();
    const result = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: queue.read,
      writeQueue: queue.write,
      dispatch: async () => { throw Object.assign(new Error(`HTTP ${status}`), { status }); },
      onReceipt: (entry, receipt, surface) => recordActionReceipt(entry, receipt, {}, { surface, storage: receiptStorage }),
    });
    expect(result.status).toBe("error");
    expect(queue.contents()).toEqual([action]);
    expect(receiptStorage.length).toBe(0);
  });

  it("keeps a rejection receipt until the origin can remove its queue entry", async () => {
    const action: QueuedAction = { request_id: "rejection-removal-retry", kind: "download_submit", payload: {}, surface: ACTION_SURFACE_FILE };
    const queue = sharedQueue([action]);
    const originState = { downloadSubmission: { request_id: action.request_id, seq: null as number | null, payload: {} } };
    const receiptStorage = new MemoryStorage();
    const result = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: queue.read,
      writeQueue: () => false,
      dispatch: async () => { throw Object.assign(new Error("Invalid request"), { status: 422 }); },
      onReceipt: (entry, receipt, surface) => recordActionReceipt(entry, receipt, originState, { surface, storage: receiptStorage, now: 4_000 }),
    });
    expect(result.status).toBe("rejection_remove_failed");
    expect(queue.contents()).toEqual([action]);
    expect(receiptStorage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`)).not.toBeNull();

    const recovered = recoverActionReceipts(originState, { surface: ACTION_SURFACE_FILE, storage: receiptStorage });
    expect(recovered.rejectedRequestIds).toEqual([action.request_id]);
    expect(consumeRejectedActionReceipts(originState, { surface: ACTION_SURFACE_FILE, requestIds: recovered.rejectedRequestIds, storage: receiptStorage }).consumed).toBe(1);
    expect(receiptStorage.length).toBe(0);
  });
});
