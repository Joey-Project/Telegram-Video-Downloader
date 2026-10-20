import { describe, expect, it, vi } from "vitest";
import {
  ACTION_SURFACE_FILE,
  ACTION_SURFACE_SETTINGS,
  actionBelongsToSurface,
  dispatchNextOwnedAction,
  recordActionReceipt,
  type QueuedAction,
} from "../public/file/action-queue.js";

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

describe("shared Mini App action queue ownership", () => {
  it("lets both pages drain their own entries and retain each page's receipt state", async () => {
    const actions: QueuedAction[] = [
      { request_id: "file-download", kind: "download_submit", payload: {}, surface: ACTION_SURFACE_FILE },
      { request_id: "settings-rename", kind: "library_rename", payload: {}, surface: ACTION_SURFACE_SETTINGS },
      { request_id: "file-refresh", kind: "library_migration_preview", payload: {}, surface: ACTION_SURFACE_FILE },
      { request_id: "file-scan", kind: "library_scan", payload: {}, surface: ACTION_SURFACE_FILE },
      { request_id: "settings-scan", kind: "library_scan", payload: {}, surface: ACTION_SURFACE_SETTINGS },
      { request_id: "legacy-file-preview", kind: "file_preview", payload: {} },
    ];
    const storage = sharedQueue(actions);
    const localActions = new Map([
      ["settings-rename", { seq: null as number | null }],
      ["settings-scan", { seq: null as number | null }],
    ]);
    const downloadSubmission = { request_id: "file-download", seq: null as number | null };
    const migrationConflictDrafts = new Map([
      ["preview-old", { refreshRequestId: "file-refresh", refreshSeq: null as number | null }],
    ]);
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
          onReceipt: (action, receipt) => surface === ACTION_SURFACE_SETTINGS
            ? recordActionReceipt(action, receipt, { localActions })
            : recordActionReceipt(action, receipt, { downloadSubmission, migrationConflictDrafts }),
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
    expect(downloadSubmission.seq).toBeGreaterThan(0);
    expect(migrationConflictDrafts.get("preview-old")?.refreshSeq).toBeGreaterThan(0);
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
    const downloadSubmission = { request_id: action.request_id, seq: null as number | null };
    const onReceipt = vi.fn((entry: QueuedAction, receipt: unknown) =>
      recordActionReceipt(entry, receipt, { downloadSubmission }),
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
    expect(downloadSubmission.seq).toBe(81);
    expect(storage.contents()).toEqual([action]);
  });
});
