export const ACTION_QUEUE_LIMIT = 20;

export const ACTION_SURFACE_SETTINGS = "settings";
export const ACTION_SURFACE_FILE = "file";
export const ACTION_RECEIPT_STORAGE_PREFIX = "file-manager.action-receipt.v1:";

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

  // Persist a sequence receipt before removing any action whose originating tab
  // may need that sequence to settle local UI state. If storage is unavailable,
  // retrying the same request ID is safe and keeps the originating tab recoverable.
  let receiptUpdate;
  try {
    receiptUpdate = onReceipt?.(action, receipt, surface);
  } catch (error) {
    return { status: "receipt_failed", action, receipt, error };
  }
  if (needsDurableReceipt(action, surface) && receiptUpdate?.persisted !== true) {
    return { status: "receipt_failed", action, receipt, receiptUpdate };
  }

  const removed = writeQueue(removeQueuedAction(readQueue(), action.request_id));
  return { status: removed ? "sent" : "remove_failed", action, receipt, receiptUpdate };
}

function needsDurableReceipt(action, surface) {
  if (!action || !actionBelongsToSurface(action, surface)) return false;
  if (surface === ACTION_SURFACE_SETTINGS) return SETTINGS_ACTION_KINDS.has(action.kind);
  return action.kind === "download_submit"
    || action.kind === "library_migration_preview" && hasSupersessionPayload(action.payload);
}

function hasSupersessionPayload(payload) {
  return payload && typeof payload === "object"
    && Object.prototype.hasOwnProperty.call(payload, "supersedes_preview_seq");
}

function validRequestId(value) {
  return typeof value === "string" && value.length > 0 && value.length <= 128
    && value.trim() === value && !/[\u0000-\u001f\u007f]/u.test(value);
}

function receiptStorageKey(requestId) {
  return `${ACTION_RECEIPT_STORAGE_PREFIX}${encodeURIComponent(requestId)}`;
}

function getStorage(storage) {
  if (storage) return storage;
  try { return globalThis.localStorage; } catch { return null; }
}

function validReceiptRecord(value, expectedRequestId) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  return value.version === 1
    && validRequestId(value.request_id)
    && value.request_id === expectedRequestId
    && (value.surface === ACTION_SURFACE_SETTINGS || value.surface === ACTION_SURFACE_FILE)
    && typeof value.kind === "string"
    && Number.isSafeInteger(value.seq) && value.seq > 0
    && Number.isSafeInteger(value.stored_at) && value.stored_at > 0;
}

function applyReceiptToTrackers(action, receipt, state, surface) {
  const result = { matched: false, localAction: false, downloadSubmission: false, migrationConflictDrafts: 0 };
  if (!validRequestId(action?.request_id) || !Number.isSafeInteger(receipt?.seq) || receipt.seq <= 0) return result;

  if (surface === ACTION_SURFACE_SETTINGS && SETTINGS_ACTION_KINDS.has(action.kind)) {
    const local = state?.localActions instanceof Map ? state.localActions.get(action.request_id) : null;
    if (local && local.kind === action.kind && (local.seq == null || local.seq === receipt.seq)) {
      local.seq = receipt.seq;
      result.localAction = true;
    }
  }

  if (surface === ACTION_SURFACE_FILE && action.kind === "download_submit"
      && state?.downloadSubmission?.request_id === action.request_id
      && (state.downloadSubmission.seq == null || state.downloadSubmission.seq === receipt.seq)) {
    state.downloadSubmission.seq = receipt.seq;
    result.downloadSubmission = true;
  }

  if (surface === ACTION_SURFACE_FILE && action.kind === "library_migration_preview"
      && state?.migrationConflictDrafts instanceof Map) {
    for (const draft of state.migrationConflictDrafts.values()) {
      if (draft.refreshRequestId === action.request_id
          && (draft.refreshSeq == null || draft.refreshSeq === receipt.seq)) {
        draft.refreshSeq = receipt.seq;
        result.migrationConflictDrafts += 1;
      }
    }
  }

  result.matched = result.localAction || result.downloadSubmission || result.migrationConflictDrafts > 0;
  return result;
}

function trackerKinds(state, requestId, surface) {
  const kinds = new Set();
  if (surface === ACTION_SURFACE_SETTINGS && state?.localActions instanceof Map) {
    const local = state.localActions.get(requestId);
    if (local && (local.seq == null || Number.isSafeInteger(local.seq)) && SETTINGS_ACTION_KINDS.has(local.kind)) kinds.add(local.kind);
  }
  if (surface === ACTION_SURFACE_FILE) {
    if (state?.downloadSubmission?.request_id === requestId
        && (state.downloadSubmission.seq == null || Number.isSafeInteger(state.downloadSubmission.seq))) {
      kinds.add("download_submit");
    }
    if (state?.migrationConflictDrafts instanceof Map
        && [...state.migrationConflictDrafts.values()].some((draft) => draft.refreshRequestId === requestId
          && (draft.refreshSeq == null || Number.isSafeInteger(draft.refreshSeq)))) {
      kinds.add("library_migration_preview");
    }
  }
  return kinds;
}

function trackerIds(state, surface) {
  const ids = new Set();
  if (surface === ACTION_SURFACE_SETTINGS && state?.localActions instanceof Map) {
    for (const [requestId, local] of state.localActions) {
      if ((local?.seq == null || Number.isSafeInteger(local?.seq))
          && SETTINGS_ACTION_KINDS.has(local?.kind) && validRequestId(requestId)) ids.add(requestId);
    }
  }
  if (surface === ACTION_SURFACE_FILE) {
    if ((state?.downloadSubmission?.seq == null || Number.isSafeInteger(state?.downloadSubmission?.seq))
        && validRequestId(state?.downloadSubmission?.request_id)) {
      ids.add(state.downloadSubmission.request_id);
    }
    if (state?.migrationConflictDrafts instanceof Map) {
      for (const draft of state.migrationConflictDrafts.values()) {
        if ((draft?.refreshSeq == null || Number.isSafeInteger(draft?.refreshSeq))
            && validRequestId(draft?.refreshRequestId)) ids.add(draft.refreshRequestId);
      }
    }
  }
  return ids;
}

export function recordActionReceipt(action, receipt, state = {}, { surface = action?.surface, storage, now = Date.now() } = {}) {
  const resolvedSurface = surface ?? (SETTINGS_ACTION_KINDS.has(action?.kind) ? ACTION_SURFACE_SETTINGS : ACTION_SURFACE_FILE);
  const result = { persisted: false, matched: false, localAction: false, downloadSubmission: false, migrationConflictDrafts: 0 };
  if (!needsDurableReceipt(action, resolvedSurface)) {
    result.persisted = true;
    return result;
  }
  if (!validRequestId(action?.request_id) || !Number.isSafeInteger(receipt?.seq) || receipt.seq <= 0
      || !Number.isSafeInteger(now) || now <= 0) return result;

  const targetStorage = getStorage(storage);
  if (!targetStorage || typeof targetStorage.setItem !== "function") return result;
  const record = {
    version: 1,
    request_id: action.request_id,
    surface: resolvedSurface,
    kind: action.kind,
    seq: receipt.seq,
    stored_at: now,
  };
  try {
    targetStorage.setItem(receiptStorageKey(action.request_id), JSON.stringify(record));
  } catch {
    return result;
  }

  result.persisted = true;
  const applied = applyReceiptToTrackers(action, receipt, state, resolvedSurface);
  Object.assign(result, applied);
  if (applied.matched) {
    try { targetStorage.removeItem(receiptStorageKey(action.request_id)); } catch { /* Re-reading this receipt is idempotent. */ }
  }
  return result;
}

export function recoverActionReceipts(state, { surface, storage } = {}) {
  const result = { matched: 0, localActions: false, downloadSubmission: false, migrationConflictDrafts: 0, requestIds: [] };
  if (surface !== ACTION_SURFACE_SETTINGS && surface !== ACTION_SURFACE_FILE) return result;
  const targetStorage = getStorage(storage);
  if (!targetStorage || typeof targetStorage.getItem !== "function") return result;

  for (const requestId of trackerIds(state, surface)) {
    const kinds = trackerKinds(state, requestId, surface);
    if (kinds.size === 0) continue;
    let record;
    try { record = JSON.parse(targetStorage.getItem(receiptStorageKey(requestId)) ?? "null"); } catch { continue; }
    if (!validReceiptRecord(record, requestId) || record.surface !== surface || !kinds.has(record.kind)) continue;
    const applied = applyReceiptToTrackers({ request_id: requestId, kind: record.kind }, record, state, surface);
    if (!applied.matched) continue;
    result.matched += 1;
    result.localActions ||= applied.localAction;
    result.downloadSubmission ||= applied.downloadSubmission;
    result.migrationConflictDrafts += applied.migrationConflictDrafts;
    result.requestIds.push(requestId);
    try { targetStorage.removeItem(receiptStorageKey(requestId)); } catch { /* Re-reading this receipt is idempotent. */ }
  }

  // An unmatched key may belong to a sleeping origin tab. Only that tab's
  // matching tracker may consume it; matched records are removed above.
  return result;
}

export function restorePendingActions(state, { surface, readQueue, writeQueue } = {}) {
  const result = { requeued: 0, skippedAtLimit: 0, writeFailed: false };
  if (surface !== ACTION_SURFACE_SETTINGS && surface !== ACTION_SURFACE_FILE
      || typeof readQueue !== "function" || typeof writeQueue !== "function") return result;

  const candidates = [];
  if (surface === ACTION_SURFACE_SETTINGS && state?.localActions instanceof Map) {
    for (const [requestId, local] of state.localActions) {
      if (local?.seq !== null || !SETTINGS_ACTION_KINDS.has(local.kind) || !validRequestId(requestId)
          || !local.payload || typeof local.payload !== "object") continue;
      candidates.push({ request_id: requestId, kind: local.kind, payload: local.payload, surface });
    }
  }
  if (surface === ACTION_SURFACE_FILE) {
    const submission = state?.downloadSubmission;
    if (submission?.seq === null && validRequestId(submission.request_id)
        && submission.payload && typeof submission.payload === "object") {
      candidates.push({ request_id: submission.request_id, kind: "download_submit", payload: submission.payload, surface });
    }
    if (state?.migrationConflictDrafts instanceof Map) {
      for (const draft of state.migrationConflictDrafts.values()) {
        if (draft?.refreshSeq !== null || !validRequestId(draft?.refreshRequestId)
            || !draft.refreshPayload || typeof draft.refreshPayload !== "object") continue;
        candidates.push({ request_id: draft.refreshRequestId, kind: "library_migration_preview", payload: draft.refreshPayload, surface });
      }
    }
  }
  if (candidates.length === 0) return result;

  const queue = readQueue();
  const knownIds = new Set(queue.map((action) => action?.request_id));
  const restored = [...queue];
  for (const action of candidates) {
    if (knownIds.has(action.request_id)) continue;
    if (restored.length >= ACTION_QUEUE_LIMIT) {
      result.skippedAtLimit += 1;
      continue;
    }
    restored.push(action);
    knownIds.add(action.request_id);
    result.requeued += 1;
  }
  if (result.requeued > 0 && !writeQueue(restored)) result.writeFailed = true;
  return result;
}

export function shouldRetryActionFailure(error) {
  const status = error && typeof error === "object" ? error.status : undefined;
  if (!Number.isInteger(status)) return true;
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}
