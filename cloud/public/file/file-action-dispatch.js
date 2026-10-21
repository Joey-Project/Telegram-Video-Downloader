import { ACTION_SURFACE_FILE, createQueuedAction } from "./action-queue.js";

export function createFileQueuedAction(kind, payload, requestId, now = Date.now()) {
  return createQueuedAction({
    request_id: requestId,
    kind,
    payload,
    surface: ACTION_SURFACE_FILE,
  }, now);
}

export function fileActionRequestBody(action) {
  return {
    request_id: action.request_id,
    kind: action.kind,
    payload: action.payload,
    queued_at_ms: action.queued_at_ms,
    replay_expires_at_ms: action.replay_expires_at_ms,
  };
}
