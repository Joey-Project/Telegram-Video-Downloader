import { describe, expect, it, vi } from "vitest";
import {
  ACTION_REPLAY_WINDOW_MS,
  ACTION_SURFACE_FILE,
  dispatchNextOwnedAction,
  restorePendingActions,
  type QueuedAction,
} from "../public/file/action-queue.js";
import { createFileQueuedAction, fileActionRequestBody } from "../public/file/file-action-dispatch.js";

describe("File action replay envelope", () => {
  it("stamps a fresh action once and sends the same bounded replay window with the rendered payload", () => {
    const payload = {
      url: "https://example.test/video",
      library_id: "main",
      root_binding: `v1:${"a".repeat(64)}`,
      category: "",
    };
    const action = createFileQueuedAction("download_submit", payload, "fresh-request", 20_000);

    expect(action).toMatchObject({
      request_id: "fresh-request",
      kind: "download_submit",
      surface: ACTION_SURFACE_FILE,
      payload,
      queued_at_ms: 20_000,
      replay_expires_at_ms: 20_000 + ACTION_REPLAY_WINDOW_MS,
    });
    expect(fileActionRequestBody(action)).toEqual({
      request_id: "fresh-request",
      kind: "download_submit",
      payload,
      queued_at_ms: 20_000,
      replay_expires_at_ms: 20_000 + ACTION_REPLAY_WINDOW_MS,
    });
  });

  it("does not add a replay window to an old tracker and never POSTs that restored row", async () => {
    const state = {
      downloadSubmission: {
        request_id: "legacy-request",
        seq: null,
        payload: { url: "https://example.test/legacy", library_id: "main", category: "" },
      },
    };
    let queue: QueuedAction[] = [];
    const restored = restorePendingActions(state, {
      surface: ACTION_SURFACE_FILE,
      readQueue: () => queue,
      writeQueue: (nextQueue) => { queue = nextQueue; return true; },
    });
    const dispatch = vi.fn(async () => ({ seq: 3 }));

    expect(restored.requeued).toBe(1);
    expect(queue[0]).not.toHaveProperty("queued_at_ms");
    expect(queue[0]).not.toHaveProperty("replay_expires_at_ms");

    const result = await dispatchNextOwnedAction({
      surface: ACTION_SURFACE_FILE,
      readQueue: () => queue,
      writeQueue: (nextQueue) => { queue = nextQueue; return true; },
      dispatch,
    });

    expect(result).toMatchObject({ status: "outcome_unknown", code: "action_replay_window_unverifiable", persisted: true });
    expect(dispatch).not.toHaveBeenCalled();
    expect(queue[0]).toMatchObject({
      request_id: "legacy-request",
      delivery_outcome: "outcome_unknown",
      delivery_error_code: "action_replay_window_unverifiable",
    });
  });
});
