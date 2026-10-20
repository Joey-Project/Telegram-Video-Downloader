export const ACTION_QUEUE_LIMIT = 20;

export const ACTION_SURFACE_SETTINGS = "settings";
export const ACTION_SURFACE_FILE = "file";

const SETTINGS_ACTION_KINDS = new Set([
  "library_register", "library_set_default", "library_set_category", "library_rename", "library_remove", "library_scan",
]);

export function appendAction(queue, action) {
  if (queue.length >= ACTION_QUEUE_LIMIT) return { ok: false, queue };
  return { ok: true, queue: [...queue, action] };
}

export function removeQueuedAction(queue, requestId) {
  return queue.filter((action) => action.request_id !== requestId);
}

export function actionBelongsToSurface(action, surface) {
  if (surface !== ACTION_SURFACE_SETTINGS && surface !== ACTION_SURFACE_FILE) return false;
  if (!action || typeof action !== "object") return false;

  if (Object.prototype.hasOwnProperty.call(action, "surface")) {
    return action.surface === surface;
  }

  // Older queue entries have no surface. Settings-only kinds belong to Settings;
  // other legacy actions remain available to the File page. New library_scan
  // actions are tagged because both pages can create them.
  const settingsOwned = SETTINGS_ACTION_KINDS.has(action.kind);
  return surface === ACTION_SURFACE_SETTINGS ? settingsOwned : !settingsOwned;
}

export function nextQueuedActionForSurface(queue, surface, excludedIds = new Set()) {
  return (Array.isArray(queue) ? queue : []).find((action) =>
    action && typeof action.request_id === "string"
      && !excludedIds.has(action.request_id)
      && actionBelongsToSurface(action, surface)
  ) ?? null;
}

export async function dispatchNextOwnedAction({
  surface,
  readQueue,
  writeQueue,
  excludedIds = new Set(),
  dispatch,
  onReceipt,
}) {
  const action = nextQueuedActionForSurface(readQueue(), surface, excludedIds);
  if (!action) return { status: "empty" };

  let receipt;
  try {
    receipt = await dispatch(action);
  } catch (error) {
    return { status: "error", action, error };
  }

  // Record the receipt before removing the action, so a storage failure can
  // safely retry the same request ID without losing its local sequence.
  const receiptUpdate = onReceipt?.(action, receipt);
  const removed = writeQueue(removeQueuedAction(readQueue(), action.request_id));
  return { status: removed ? "sent" : "remove_failed", action, receipt, receiptUpdate };
}

export function recordActionReceipt(action, receipt, {
  localActions,
  downloadSubmission,
  migrationConflictDrafts,
} = {}) {
  const result = { localAction: false, downloadSubmission: false, migrationConflictDrafts: 0 };
  if (!Number.isSafeInteger(receipt?.seq)) return result;

  const local = localActions?.get(action?.request_id);
  if (local) {
    local.seq = receipt.seq;
    result.localAction = true;
  }

  if (downloadSubmission?.request_id === action?.request_id) {
    downloadSubmission.seq = receipt.seq;
    result.downloadSubmission = true;
  }

  if (migrationConflictDrafts instanceof Map) {
    for (const draft of migrationConflictDrafts.values()) {
      if (draft.refreshRequestId === action?.request_id) {
        draft.refreshSeq = receipt.seq;
        result.migrationConflictDrafts += 1;
      }
    }
  }

  return result;
}

export function shouldRetryActionFailure(error) {
  const status = error && typeof error === "object" ? error.status : undefined;
  if (!Number.isInteger(status)) return true;
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}
