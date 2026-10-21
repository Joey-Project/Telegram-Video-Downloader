import { describe, expect, it, vi } from "vitest";
import {
  ACTION_SURFACE_FILE,
  dispatchNextOwnedAction,
  recordActionReceipt,
  type QueuedAction,
} from "../public/file/action-queue.js";
import {
  inspectDownloadSubmissionHistory,
  RECENT_OPERATION_SNAPSHOT_LIMIT,
  RECENT_SNAPSHOT_MAX_AGE_MS,
  releaseDownloadSubmissionTracking,
} from "../public/file/download-submission-history.js";

function cappedSnapshot(firstSequence: number, overrides: Record<string, unknown> = {}) {
  return {
    synced: true,
    stale: false,
    last_seen: Date.now(),
    operations: Array.from({ length: RECENT_OPERATION_SNAPSHOT_LIMIT }, (_, index) => ({
      seq: firstSequence + index,
      kind: "library_scan",
      status: "completed",
    })),
    pending_requests: [],
    ...overrides,
  };
}

describe("File download submission history reconciliation", () => {
  it("offers explicit release after more than one capped window has advanced", () => {
    const submission = { request_id: "download-original", seq: 7 };
    const snapshot = cappedSnapshot(108);
    const history = inspectDownloadSubmissionHistory(submission, snapshot);

    expect(snapshot.operations).toHaveLength(100);
    expect(history.kind).toBe("outside_recent_history");
    const state = { downloadSubmission: submission, downloadMessage: null };
    expect(releaseDownloadSubmissionTracking(state, snapshot)).toBe(true);
    expect(state.downloadSubmission).toBeNull();
    expect(state.downloadMessage).toContain("result is unknown");
    expect(state.downloadMessage).toContain("does not cancel or resend");
  });

  it("does not offer release while the request is pending or its operation is visible", () => {
    const submission = { request_id: "download-original", seq: 9 };
    const pending = cappedSnapshot(10, { pending_requests: [{ seq: 9 }] });
    expect(inspectDownloadSubmissionHistory(submission, pending).kind).toBe("pending");

    const visible = cappedSnapshot(10, {
      operations: [{ seq: 9, kind: "download_submit", status: "running" }, ...cappedSnapshot(10).operations.slice(1)],
    });
    expect(inspectDownloadSubmissionHistory(submission, visible).kind).toBe("recorded");
    const state = { downloadSubmission: submission, downloadMessage: null };
    expect(releaseDownloadSubmissionTracking(state, visible)).toBe(false);
    expect(state.downloadSubmission).toBe(submission);
  });

  it("keeps visible completed and failed receipts on the existing settlement path", () => {
    for (const status of ["completed", "failed", "pending", "retrying"]) {
      const submission = { request_id: `download-${status}`, seq: 11 };
      const snapshot = cappedSnapshot(12, {
        operations: [{ seq: 11, kind: "download_submit", status }, ...cappedSnapshot(12).operations.slice(1)],
      });
      expect(inspectDownloadSubmissionHistory(submission, snapshot)).toMatchObject({
        kind: "recorded",
        operation: { status },
      });
      const state = { downloadSubmission: submission, downloadMessage: null };
      expect(releaseDownloadSubmissionTracking(state, snapshot)).toBe(false);
      expect(state.downloadSubmission).toBe(submission);
    }
  });

  it("keeps waiting through stale, unsynced, short, malformed, or ambiguous snapshots", () => {
    const submission = { request_id: "download-original", seq: 10 };
    const full = cappedSnapshot(11);
    const variants = [
      cappedSnapshot(11, { stale: true }),
      cappedSnapshot(11, { synced: false }),
      cappedSnapshot(11, { last_seen: Date.now() - RECENT_SNAPSHOT_MAX_AGE_MS - 1 }),
      cappedSnapshot(11, { last_seen: Date.now() + 60_000 }),
      cappedSnapshot(11, { operations: full.operations.slice(1) }),
      cappedSnapshot(11, { operations: [{ seq: "11" }, ...full.operations.slice(1)] }),
      cappedSnapshot(11, { operations: [{ seq: 11 }, { seq: 11 }, ...full.operations.slice(2)] }),
      cappedSnapshot(11, { operations: [{ seq: 10, kind: "library_scan" }, ...full.operations.slice(1)] }),
      cappedSnapshot(11, { operations: [{ seq: 11, kind: "library_scan" }, ...full.operations.slice(1)] }),
      { synced: true, stale: false, last_seen: Date.now(), operations: full.operations },
    ];

    for (const snapshot of variants) {
      expect(inspectDownloadSubmissionHistory(submission, snapshot).kind).toBe("unknown");
      const state = { downloadSubmission: submission, downloadMessage: null };
      expect(releaseDownloadSubmissionTracking(state, snapshot)).toBe(false);
      expect(state.downloadSubmission).toBe(submission);
    }
  });

  it("releases only the browser tracker after an accepted receipt and does not dispatch again", async () => {
    const payload = { url: "https://example.test/video", library_id: "main", category: "" };
    const action: QueuedAction = {
      request_id: "download-stable-id",
      kind: "download_submit",
      payload,
      surface: ACTION_SURFACE_FILE,
    };
    const state = {
      downloadSubmission: { request_id: action.request_id, seq: null as number | null, payload },
      downloadMessage: null as string | null,
    };
    const receiptStorage = new MemoryStorage();
    let queue = [action];
    const dispatch = vi.fn(async () => ({ seq: 12 }));
    const accepted = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: () => queue,
      writeQueue: (next) => { queue = next; return true; },
      dispatch,
      onReceipt: (queued, receipt, surface) => recordActionReceipt(queued, receipt, state, { surface, storage: receiptStorage }),
    });

    expect(accepted.status).toBe("sent");
    expect(state.downloadSubmission.seq).toBe(12);
    expect(queue).toEqual([]);
    expect(releaseDownloadSubmissionTracking(state, cappedSnapshot(112))).toBe(true);
    expect(state.downloadSubmission).toBeNull();
    expect(queue).toEqual([]);
    expect(dispatch).toHaveBeenCalledTimes(1);
    expect(state.downloadMessage).toContain("download-stable-id");
    expect(state.downloadMessage).toContain("sequence 12");
    expect(state.downloadMessage).toMatch(/unknown.*does not cancel or resend/u);
  });
});

class MemoryStorage implements Storage {
  readonly #values = new Map<string, string>();
  get length() { return this.#values.size; }
  clear() { this.#values.clear(); }
  getItem(key: string) { return this.#values.get(key) ?? null; }
  key(index: number) { return [...this.#values.keys()][index] ?? null; }
  removeItem(key: string) { this.#values.delete(key); }
  setItem(key: string, value: string) { this.#values.set(key, value); }
}
