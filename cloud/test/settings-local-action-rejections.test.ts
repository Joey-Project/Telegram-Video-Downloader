import { describe, expect, it } from "vitest";
import {
  ACTION_RECEIPT_STORAGE_PREFIX,
  ACTION_SURFACE_SETTINGS,
  consumeRejectedActionReceipts,
  createQueuedAction,
  dispatchNextOwnedAction,
  recoverActionReceipts,
  recordActionReceipt,
  restorePendingActions,
  type QueuedAction,
} from "../public/file/action-queue.js";
import { takeRejectedSettingsActions } from "../public/settings/local-action-rejections.js";

class MemoryStorage {
  values = new Map<string, string>();
  get length() { return this.values.size; }
  clear() { this.values.clear(); }
  key(index: number) { return [...this.values.keys()][index] ?? null; }
  getItem(key: string) { return this.values.get(key) ?? null; }
  setItem(key: string, value: string) { this.values.set(key, value); }
  removeItem(key: string) { this.values.delete(key); }
}

function sharedQueue(initial: QueuedAction[]) {
  let actions = [...initial];
  return {
    read: () => [...actions],
    write(next: QueuedAction[]) { actions = [...next]; return true; },
    contents: () => [...actions],
  };
}

type FreshQueuedAction = QueuedAction & { queued_at_ms: number; replay_expires_at_ms: number };

function localState(action: FreshQueuedAction) {
  return {
    localActions: new Map([[action.request_id, {
      kind: action.kind,
      payload: action.payload,
      label: "Rename Archive",
      seq: null as number | null,
      rejected: false,
      queued_at_ms: action.queued_at_ms,
      replay_expires_at_ms: action.replay_expires_at_ms,
    }]]),
  };
}

function rejected(status: number) {
  return Object.assign(new Error(`Rejected with status ${status}`), { status });
}

describe("Settings permanent action rejections", () => {
  it("settles a same-tab rejection only after the durable receipt and queue removal", async () => {
    const action = createQueuedAction({
      request_id: "settings-origin-422",
      kind: "library_rename",
      payload: { library_id: "archive", name: "Archive" },
      surface: ACTION_SURFACE_SETTINGS,
    });
    const queue = sharedQueue([action]);
    const receiptStorage = new MemoryStorage();
    const state = localState(action);
    const outcome = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_SETTINGS,
      readQueue: queue.read,
      writeQueue: queue.write,
      dispatch: async () => { throw rejected(422); },
      onReceipt: (entry, receipt, surface) => recordActionReceipt(entry, receipt, state, { surface, storage: receiptStorage }),
    });

    expect(outcome.status).toBe("rejected");
    expect(queue.contents()).toEqual([]);
    expect(receiptStorage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`)).toBeNull();
    if (outcome.status !== "rejected") throw new Error("Expected a durable rejection result");
    const receiptUpdate = outcome.receiptUpdate as { rejectedRequestIds?: string[] };
    const settled = takeRejectedSettingsActions(state.localActions, receiptUpdate.rejectedRequestIds);
    expect(settled).toEqual([{
      requestId: action.request_id,
      kind: "library_rename",
      label: "Rename Archive",
      error: "Rejected with status 422",
    }]);
    expect(state.localActions.has(action.request_id)).toBe(false);
  });

  it("leaves a helper-tab rejection for the origin to recover and consume", async () => {
    const action = createQueuedAction({
      request_id: "settings-sleeping-origin",
      kind: "library_rename",
      payload: { library_id: "archive", name: "Archive" },
      surface: ACTION_SURFACE_SETTINGS,
    });
    const queue = sharedQueue([action]);
    const receiptStorage = new MemoryStorage();
    const origin = localState(action);
    const helper = { localActions: new Map() };
    const outcome = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_SETTINGS,
      readQueue: queue.read,
      writeQueue: queue.write,
      dispatch: async () => { throw rejected(422); },
      onReceipt: (entry, receipt, surface) => recordActionReceipt(entry, receipt, helper, { surface, storage: receiptStorage }),
    });

    expect(outcome.status).toBe("rejected");
    expect(queue.contents()).toEqual([]);
    expect(receiptStorage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`)).not.toBeNull();
    if (outcome.status !== "rejected") throw new Error("Expected a durable rejection result");
    const receiptUpdate = outcome.receiptUpdate as { rejectedRequestIds?: string[] };
    expect(receiptUpdate.rejectedRequestIds).toEqual([]);
    expect(takeRejectedSettingsActions(helper.localActions, [action.request_id])).toEqual([]);

    const recovered = recoverActionReceipts(origin, { surface: ACTION_SURFACE_SETTINGS, storage: receiptStorage });
    expect(recovered.rejectedRequestIds).toEqual([action.request_id]);
    // The origin confirms the shared row is absent before consuming its receipt.
    expect(queue.read().some((entry) => entry.request_id === action.request_id)).toBe(false);
    const consumed = consumeRejectedActionReceipts(origin, {
      surface: ACTION_SURFACE_SETTINGS,
      requestIds: recovered.rejectedRequestIds,
      storage: receiptStorage,
    });
    expect(consumed.consumed).toBe(1);
    expect(takeRejectedSettingsActions(origin.localActions, recovered.rejectedRequestIds)).toHaveLength(1);
    expect(receiptStorage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`)).toBeNull();
  });

  it("keeps the queue and receipt when queue removal fails, then settles on recovery", async () => {
    const action = createQueuedAction({
      request_id: "settings-remove-failure",
      kind: "library_rename",
      payload: { library_id: "archive", name: "Archive" },
      surface: ACTION_SURFACE_SETTINGS,
    });
    const queue = sharedQueue([action]);
    const receiptStorage = new MemoryStorage();
    const state = localState(action);
    const failed = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_SETTINGS,
      readQueue: queue.read,
      writeQueue: () => false,
      dispatch: async () => { throw rejected(422); },
      onReceipt: (entry, receipt, surface) => recordActionReceipt(entry, receipt, state, { surface, storage: receiptStorage }),
    });

    expect(failed.status).toBe("rejection_remove_failed");
    expect(queue.contents()).toEqual([action]);
    expect(receiptStorage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`)).not.toBeNull();
    expect(restorePendingActions(state, {
      surface: ACTION_SURFACE_SETTINGS,
      readQueue: queue.read,
      writeQueue: queue.write,
    }).requeued).toBe(0);

    const recovered = recoverActionReceipts(state, { surface: ACTION_SURFACE_SETTINGS, storage: receiptStorage });
    const remaining = queue.read().filter((entry) => !new Set(recovered.requestIds).has(entry.request_id));
    expect(queue.write(remaining)).toBe(true);
    consumeRejectedActionReceipts(state, {
      surface: ACTION_SURFACE_SETTINGS,
      requestIds: recovered.rejectedRequestIds,
      storage: receiptStorage,
    });
    expect(takeRejectedSettingsActions(state.localActions, recovered.rejectedRequestIds)).toHaveLength(1);
    expect(queue.contents()).toEqual([]);
    expect(receiptStorage.getItem(`${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(action.request_id)}`)).toBeNull();
  });

  it.each([401, 403, 408, 429, 500])("does not make HTTP %s a terminal rejection", async (status) => {
    const action = createQueuedAction({
      request_id: `settings-http-${status}`,
      kind: "library_rename",
      payload: { library_id: "archive", name: "Archive" },
      surface: ACTION_SURFACE_SETTINGS,
    });
    const queue = sharedQueue([action]);
    const receiptStorage = new MemoryStorage();
    const state = localState(action);
    const outcome = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_SETTINGS,
      readQueue: queue.read,
      writeQueue: queue.write,
      dispatch: async () => { throw rejected(status); },
      onReceipt: (entry, receipt, surface) => recordActionReceipt(entry, receipt, state, { surface, storage: receiptStorage }),
    });

    expect(outcome.status).toBe("error");
    expect(queue.contents()).toEqual([action]);
    expect(state.localActions.get(action.request_id)?.rejected).not.toBe(true);
    expect(receiptStorage.values.size).toBe(0);
  });
});
