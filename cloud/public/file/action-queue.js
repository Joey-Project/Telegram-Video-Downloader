export const ACTION_QUEUE_LIMIT = 20;
export const ACTION_REPLAY_WINDOW_MS = 6 * 24 * 60 * 60 * 1000;

export const ACTION_SURFACE_SETTINGS = "settings";
export const ACTION_SURFACE_FILE = "file";
export const ACTION_RECEIPT_STORAGE_PREFIX = "file-manager.action-receipt.v1:";

const SETTINGS_ACTION_KINDS = new Set([
  "library_register", "library_set_default", "library_set_category", "library_rename", "library_remove", "library_scan",
]);

const MAX_ACTION_CLOCK_SKEW_MS = 5 * 60 * 1000;
const OUTCOME_UNKNOWN_CODES = new Set([
  "action_replay_window_expired",
  "action_replay_window_unverifiable",
]);

export function createQueuedAction(action, now = Date.now()) {
  if (!action || typeof action !== "object" || Array.isArray(action)) {
    throw new TypeError("Queued action must be an object");
  }
  if (!Number.isSafeInteger(now) || now <= 0 || now > Number.MAX_SAFE_INTEGER - ACTION_REPLAY_WINDOW_MS) {
    throw new TypeError("Queued action timestamp is invalid");
  }
  const {
    queued_at_ms: _queuedAt,
    replay_expires_at_ms: _expiresAt,
    delivery_outcome: _deliveryOutcome,
    delivery_error_code: _deliveryCode,
    delivery_error: _deliveryError,
    ...freshAction
  } = action;
  return {
    ...freshAction,
    queued_at_ms: now,
    replay_expires_at_ms: now + ACTION_REPLAY_WINDOW_MS,
  };
}

export function actionReplayStatus(action, now = Date.now()) {
  if (!action || typeof action !== "object" || Array.isArray(action)
      || !Number.isSafeInteger(now) || now <= 0
      || !Number.isSafeInteger(action.queued_at_ms) || action.queued_at_ms <= 0
      || !Number.isSafeInteger(action.replay_expires_at_ms) || action.replay_expires_at_ms <= 0
      || action.replay_expires_at_ms <= action.queued_at_ms
      || action.replay_expires_at_ms - action.queued_at_ms > ACTION_REPLAY_WINDOW_MS
      || action.queued_at_ms > now + MAX_ACTION_CLOCK_SKEW_MS) {
    return {
      status: "unverifiable",
      code: "action_replay_window_unverifiable",
    };
  }
  if (action.replay_expires_at_ms <= now) {
    return {
      status: "expired",
      code: "action_replay_window_expired",
    };
  }
  return { status: "valid", code: null };
}

export function appendAction(queue, action) {
  if (queue.length >= ACTION_QUEUE_LIMIT) return { ok: false, queue };
  return { ok: true, queue: [...queue, action] };
}

export async function persistQueuedActionAndDrain({
  action,
  readQueue,
  writeQueue,
  onQueued,
  afterPersist,
  isOnline,
  onOffline,
  onSending,
  drain,
}) {
  const queued = appendAction(readQueue(), action);
  if (!queued.ok) return { status: "queue_full", action };
  if (!writeQueue(queued.queue)) return { status: "storage_failed", action };

  onQueued?.(action);
  afterPersist?.(action);
  if (!isOnline()) {
    onOffline?.(action);
    return { status: "queued", action };
  }

  onSending?.(action);
  await drain();
  return { status: "drained", action };
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

function beforeReceiptAcknowledged(callback, action, receipt, applied, surface) {
  if (typeof callback !== "function") return true;
  try { return callback(action, receipt, applied, surface) !== false; } catch { return false; }
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

  if (action.delivery_outcome === "outcome_unknown") {
    excludedIds.add(action.request_id);
    return {
      status: "outcome_unknown",
      action,
      code: validOutcomeUnknownCode(action.delivery_error_code) ?? "action_replay_window_unverifiable",
      error: boundedOutcomeUnknownMessage(action.delivery_error),
      persisted: true,
    };
  }

  const replay = actionReplayStatus(action);
  if (replay.status !== "valid") {
    excludedIds.add(action.request_id);
    const error = replay.status === "expired"
      ? "This saved request is past its safe replay window. Check the desktop before dismissing it."
      : "This saved request has no verifiable replay window. Check the desktop before dismissing it.";
    const persisted = persistOutcomeUnknown(action, replay.code, error, readQueue, writeQueue);
    return { status: "outcome_unknown", action: { ...action, ...unknownFields(replay.code, error) }, code: replay.code, error, persisted };
  }

  let receipt;
  try {
    receipt = await dispatch(action);
  } catch (error) {
    if (isReplayWindowExpiry(error)) {
      const code = "action_replay_window_expired";
      const message = "The cloud no longer has a retained receipt for this request. Check the desktop before dismissing it.";
      excludedIds.add(action.request_id);
      const persisted = persistOutcomeUnknown(action, code, message, readQueue, writeQueue);
      return { status: "outcome_unknown", action: { ...action, ...unknownFields(code, message) }, code, error: message, persisted };
    }
    if (needsDurableReceipt(action, surface) && isPermanentReceiptRejection(error)) {
      const rejection = {
        outcome: "rejected",
        status: error.status,
        error: boundedReceiptError(error?.message),
      };
      let receiptUpdate;
      try {
        receiptUpdate = onReceipt?.(action, rejection, surface);
      } catch (receiptError) {
        return { status: "rejection_receipt_failed", action, error, receiptError };
      }
      if (receiptUpdate?.persisted !== true) {
        return { status: "rejection_receipt_failed", action, error, receiptUpdate };
      }
      const queue = readQueue();
      const removed = !queue.some((entry) => entry?.request_id === action.request_id)
        || writeQueue(removeQueuedAction(queue, action.request_id));
      if (removed) receiptUpdate?.consume?.();
      return removed
        ? { status: "rejected", action, rejection, receiptUpdate }
        : { status: "rejection_remove_failed", action, rejection, receiptUpdate };
    }
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

  const queue = readQueue();
  const removed = !queue.some((entry) => entry?.request_id === action.request_id)
    || writeQueue(removeQueuedAction(queue, action.request_id));
  return { status: removed ? "sent" : "remove_failed", action, receipt, receiptUpdate };
}

function isReplayWindowExpiry(error) {
  const status = error && typeof error === "object" ? error.status : undefined;
  const code = error && typeof error === "object" ? error.code : undefined;
  return code === "action_replay_window_expired" || status === 410;
}

function validOutcomeUnknownCode(value) {
  return typeof value === "string" && OUTCOME_UNKNOWN_CODES.has(value) ? value : null;
}

function boundedOutcomeUnknownMessage(value) {
  const text = String(value ?? "Delivery outcome is unknown. Check the desktop before dismissing this request.")
    .replace(/[\u0000-\u001f\u007f]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  let bounded = "";
  const encoder = new TextEncoder();
  for (const character of text || "Delivery outcome is unknown. Check the desktop before dismissing this request.") {
    if (encoder.encode(bounded + character).byteLength > 256) break;
    bounded += character;
  }
  return bounded || "Delivery outcome is unknown. Check the desktop before dismissing this request.";
}

function unknownFields(code, error) {
  return {
    delivery_outcome: "outcome_unknown",
    delivery_error_code: code,
    delivery_error: boundedOutcomeUnknownMessage(error),
  };
}

function persistOutcomeUnknown(action, code, error, readQueue, writeQueue) {
  try {
    const current = readQueue();
    let found = false;
    let changed = false;
    const fields = unknownFields(code, error);
    const updated = current.map((entry) => {
      if (entry?.request_id !== action.request_id) return entry;
      found = true;
      if (entry.delivery_outcome === fields.delivery_outcome
          && entry.delivery_error_code === fields.delivery_error_code
          && entry.delivery_error === fields.delivery_error) return entry;
      changed = true;
      return { ...entry, ...fields };
    });
    if (!found) return false;
    return !changed || writeQueue(updated) === true;
  } catch {
    return false;
  }
}

function needsDurableReceipt(action, surface) {
  if (!action || !actionBelongsToSurface(action, surface)) return false;
  if (surface === ACTION_SURFACE_SETTINGS) return SETTINGS_ACTION_KINDS.has(action.kind);
  return action.kind === "download_submit" || action.kind === "preview_cancel"
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

function isPermanentReceiptRejection(error) {
  const status = error && typeof error === "object" ? error.status : undefined;
  return Number.isInteger(status) && status >= 400 && status <= 499
    && status !== 401 && status !== 403 && status !== 408 && status !== 429;
}

function boundedReceiptError(value) {
  const text = String(value ?? "Request rejected")
    .replace(/[\u0000-\u001f\u007f]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  let bounded = "";
  const encoder = new TextEncoder();
  for (const character of text || "Request rejected") {
    if (encoder.encode(bounded + character).byteLength > 256) break;
    bounded += character;
  }
  return bounded || "Request rejected";
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
  const common = value.version === 1
    && validRequestId(value.request_id)
    && value.request_id === expectedRequestId
    && (value.surface === ACTION_SURFACE_SETTINGS || value.surface === ACTION_SURFACE_FILE)
    && typeof value.kind === "string"
    && Number.isSafeInteger(value.stored_at) && value.stored_at > 0;
  if (!common) return false;
  if (value.outcome === undefined || value.outcome === "accepted") {
    return Number.isSafeInteger(value.seq) && value.seq > 0;
  }
  return value.outcome === "rejected"
    && Number.isInteger(value.status) && value.status >= 400 && value.status <= 499
    && value.status !== 401 && value.status !== 403 && value.status !== 408 && value.status !== 429
    && typeof value.error === "string" && value.error.length > 0
    && new TextEncoder().encode(value.error).byteLength <= 256
    && !/[\u0000-\u001f\u007f]/u.test(value.error);
}

function applyReceiptToTrackers(action, receipt, state, surface) {
  const result = { matched: false, localAction: false, downloadSubmission: false, migrationConflictDrafts: 0, rejected: false };
  let previewCancellationMatched = false;
  if (!validRequestId(action?.request_id)) return result;
  const rejected = receipt?.outcome === "rejected";
  if (rejected ? !validReceiptRecord({ ...receipt, version: 1, request_id: action.request_id, surface, kind: action.kind, stored_at: 1 }, action.request_id)
    : !Number.isSafeInteger(receipt?.seq) || receipt.seq <= 0) return result;

  if (surface === ACTION_SURFACE_SETTINGS && SETTINGS_ACTION_KINDS.has(action.kind)) {
    const local = state?.localActions instanceof Map ? state.localActions.get(action.request_id) : null;
    if (local && local.kind === action.kind && (local.seq == null || local.seq === receipt.seq)) {
      if (rejected) {
        local.rejected = true;
        local.error = receipt.error;
      } else {
        local.seq = receipt.seq;
      }
      result.localAction = true;
    }
  }

  if (surface === ACTION_SURFACE_FILE && action.kind === "download_submit"
      && state?.downloadSubmission?.request_id === action.request_id
      && (state.downloadSubmission.seq == null || state.downloadSubmission.seq === receipt.seq)) {
    if (rejected) {
      state.downloadSubmission.rejected = true;
      state.downloadSubmission.error = receipt.error;
    } else {
      state.downloadSubmission.seq = receipt.seq;
    }
    result.downloadSubmission = true;
  }

  if (surface === ACTION_SURFACE_FILE && action.kind === "library_migration_preview"
      && state?.migrationConflictDrafts instanceof Map) {
    for (const draft of state.migrationConflictDrafts.values()) {
      if ((draft.refreshRequestId === action.request_id || draft.rejectedRefreshRequestId === action.request_id)
          && (draft.refreshSeq == null || draft.refreshSeq === receipt.seq)) {
        if (rejected) {
          draft.rejectedRefreshRequestId = action.request_id;
          draft.refreshRequestId = null;
          draft.refreshPayload = null;
          draft.refreshSeq = null;
          draft.refreshError = receipt.error;
        } else {
          draft.refreshSeq = receipt.seq;
        }
        result.migrationConflictDrafts += 1;
      }
    }
  }

  if (surface === ACTION_SURFACE_FILE && action.kind === "preview_cancel"
      && state?.previewCancellations instanceof Map) {
    for (const tracker of state.previewCancellations.values()) {
      if (tracker?.requestId !== action.request_id || tracker.seq != null && tracker.seq !== receipt.seq) continue;
      if (rejected) {
        tracker.status = "failed";
        tracker.error = receipt.error;
      } else {
        tracker.status = "submitted";
        tracker.seq = receipt.seq;
        tracker.error = null;
      }
      previewCancellationMatched = true;
    }
  }

  result.rejected = rejected && (result.localAction || result.downloadSubmission || result.migrationConflictDrafts > 0 || previewCancellationMatched);
  result.matched = result.localAction || result.downloadSubmission || result.migrationConflictDrafts > 0 || previewCancellationMatched;
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
        && [...state.migrationConflictDrafts.values()].some((draft) => (draft.refreshRequestId === requestId || draft.rejectedRefreshRequestId === requestId)
          && (draft.refreshSeq == null || Number.isSafeInteger(draft.refreshSeq)))) {
      kinds.add("library_migration_preview");
    }
    if (state?.previewCancellations instanceof Map
        && [...state.previewCancellations.values()].some((tracker) => tracker?.requestId === requestId
          && (tracker.seq == null || Number.isSafeInteger(tracker.seq)))) {
      kinds.add("preview_cancel");
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
        for (const requestId of [draft?.refreshRequestId, draft?.rejectedRefreshRequestId]) {
          if ((draft?.refreshSeq == null || Number.isSafeInteger(draft?.refreshSeq)) && validRequestId(requestId)) ids.add(requestId);
        }
      }
    }
    if (state?.previewCancellations instanceof Map) {
      for (const tracker of state.previewCancellations.values()) {
        if ((tracker?.seq == null || Number.isSafeInteger(tracker.seq)) && validRequestId(tracker?.requestId)) {
          ids.add(tracker.requestId);
        }
      }
    }
  }
  return ids;
}

export function recordActionReceipt(action, receipt, state = {}, {
  surface = action?.surface,
  storage,
  now = Date.now(),
  beforeAcknowledge,
} = {}) {
  const resolvedSurface = surface ?? (SETTINGS_ACTION_KINDS.has(action?.kind) ? ACTION_SURFACE_SETTINGS : ACTION_SURFACE_FILE);
  const result = { persisted: false, matched: false, localAction: false, downloadSubmission: false, migrationConflictDrafts: 0, rejected: false, rejectedRequestIds: [] };
  if (!needsDurableReceipt(action, resolvedSurface)) {
    result.persisted = true;
    return result;
  }
  const rejected = receipt?.outcome === "rejected";
  if (!validRequestId(action?.request_id) || !Number.isSafeInteger(now) || now <= 0) return result;
  if (rejected) {
    if (!isPermanentReceiptRejection(receipt) || typeof receipt.error !== "string"
        || receipt.error.length === 0 || new TextEncoder().encode(receipt.error).byteLength > 256
        || /[\u0000-\u001f\u007f]/u.test(receipt.error)) return result;
  } else if (!Number.isSafeInteger(receipt?.seq) || receipt.seq <= 0) return result;

  const targetStorage = getStorage(storage);
  if (!targetStorage || typeof targetStorage.setItem !== "function") return result;
  const record = {
    version: 1,
    request_id: action.request_id,
    surface: resolvedSurface,
    kind: action.kind,
    ...(rejected
      ? { outcome: "rejected", status: receipt.status, error: receipt.error }
      : { outcome: "accepted", seq: receipt.seq }),
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
  if (!beforeReceiptAcknowledged(beforeAcknowledge, action, receipt, applied, resolvedSurface)) {
    result.persisted = false;
    result.acknowledgementBlocked = true;
    return result;
  }
  if (applied.rejected) result.rejectedRequestIds.push(action.request_id);
  if (applied.matched && !rejected) {
    try { targetStorage.removeItem(receiptStorageKey(action.request_id)); } catch { /* Re-reading this receipt is idempotent. */ }
  }
  if (applied.matched && rejected) {
    result.consume = () => {
      try { targetStorage.removeItem(receiptStorageKey(action.request_id)); } catch { /* Re-reading this rejection is idempotent. */ }
    };
  }
  return result;
}

export function recoverActionReceipts(state, { surface, storage, beforeAcknowledge } = {}) {
  const result = { matched: 0, localActions: false, downloadSubmission: false, migrationConflictDrafts: 0, requestIds: [], blockedRequestIds: [], rejectedRequestIds: [], rejections: [] };
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
    if (!beforeReceiptAcknowledged(beforeAcknowledge, { request_id: requestId, kind: record.kind }, record, applied, surface)) {
      result.blockedRequestIds.push(requestId);
      continue;
    }
    if (!applied.matched) continue;
    result.matched += 1;
    result.localActions ||= applied.localAction;
    result.downloadSubmission ||= applied.downloadSubmission;
    result.migrationConflictDrafts += applied.migrationConflictDrafts;
    result.requestIds.push(requestId);
    if (applied.rejected) {
      result.rejectedRequestIds.push(requestId);
      result.rejections.push({ request_id: requestId, kind: record.kind, status: record.status, error: record.error });
    }
    if (!applied.rejected) {
      try { targetStorage.removeItem(receiptStorageKey(requestId)); } catch { /* Re-reading this receipt is idempotent. */ }
    }
  }

  // Unmatched keys may belong to a sleeping origin tab. Accepted receipts are
  // consumed here after any supplied acknowledgement gate succeeds; rejected
  // receipts stay until the origin removes its queue row.
  return result;
}

export function consumeRejectedActionReceipts(state, { surface, requestIds, storage } = {}) {
  const result = { consumed: 0 };
  if (surface !== ACTION_SURFACE_SETTINGS && surface !== ACTION_SURFACE_FILE) return result;
  const targetStorage = getStorage(storage);
  if (!targetStorage || typeof targetStorage.getItem !== "function" || typeof targetStorage.removeItem !== "function") return result;
  for (const requestId of Array.isArray(requestIds) ? requestIds : []) {
    if (!validRequestId(requestId) || !hasRejectedTracker(state, requestId, surface)) continue;
    let record;
    try { record = JSON.parse(targetStorage.getItem(receiptStorageKey(requestId)) ?? "null"); } catch { continue; }
    if (!validReceiptRecord(record, requestId) || record.outcome !== "rejected" || record.surface !== surface) continue;
    if (!trackerKinds(state, requestId, surface).has(record.kind)) continue;
    try {
      targetStorage.removeItem(receiptStorageKey(requestId));
      result.consumed += 1;
    } catch { /* Keep the rejection available for the origin's next recovery pass. */ }
  }
  return result;
}

function hasRejectedTracker(state, requestId, surface) {
  if (surface === ACTION_SURFACE_SETTINGS) return state?.localActions instanceof Map
    && state.localActions.get(requestId)?.rejected === true;
  if (surface === ACTION_SURFACE_FILE) {
    return state?.downloadSubmission?.request_id === requestId && state.downloadSubmission.rejected === true
      || state?.migrationConflictDrafts instanceof Map
        && [...state.migrationConflictDrafts.values()].some((draft) => draft.rejectedRefreshRequestId === requestId)
      || state?.previewCancellations instanceof Map
        && [...state.previewCancellations.values()].some((tracker) => tracker.requestId === requestId && tracker.status === "failed");
  }
  return false;
}

export function restorePendingActions(state, { surface, readQueue, writeQueue } = {}) {
  const result = { requeued: 0, skippedAtLimit: 0, writeFailed: false };
  if (surface !== ACTION_SURFACE_SETTINGS && surface !== ACTION_SURFACE_FILE
      || typeof readQueue !== "function" || typeof writeQueue !== "function") return result;

  const candidates = [];
  if (surface === ACTION_SURFACE_SETTINGS && state?.localActions instanceof Map) {
    for (const [requestId, local] of state.localActions) {
      if (local?.seq !== null || local?.rejected === true || local?.delivery_outcome === "outcome_unknown"
          || !SETTINGS_ACTION_KINDS.has(local.kind) || !validRequestId(requestId)
          || !local.payload || typeof local.payload !== "object") continue;
      candidates.push({
        request_id: requestId,
        kind: local.kind,
        payload: local.payload,
        surface,
        ...replayWindowFields(local),
        ...unknownTrackerFields(local),
      });
    }
  }
  if (surface === ACTION_SURFACE_FILE) {
    const submission = state?.downloadSubmission;
    if (submission?.seq === null && submission?.rejected !== true && submission?.delivery_outcome !== "outcome_unknown"
        && validRequestId(submission.request_id)
        && submission.payload && typeof submission.payload === "object") {
      candidates.push({
        request_id: submission.request_id,
        kind: "download_submit",
        payload: submission.payload,
        surface,
        ...replayWindowFields(submission),
        ...unknownTrackerFields(submission),
      });
    }
    if (state?.migrationConflictDrafts instanceof Map) {
      for (const draft of state.migrationConflictDrafts.values()) {
        if (draft?.refreshSeq !== null || draft?.refreshDeliveryOutcome === "outcome_unknown"
            || !validRequestId(draft?.refreshRequestId)
            || !draft.refreshPayload || typeof draft.refreshPayload !== "object") continue;
        candidates.push({
          request_id: draft.refreshRequestId,
          kind: "library_migration_preview",
          payload: draft.refreshPayload,
          surface,
          ...(draft.refreshQueuedAtMs === undefined ? {} : { queued_at_ms: draft.refreshQueuedAtMs }),
          ...(draft.refreshExpiresAtMs === undefined ? {} : { replay_expires_at_ms: draft.refreshExpiresAtMs }),
          ...(draft.refreshDeliveryOutcome === "outcome_unknown" ? { delivery_outcome: "outcome_unknown" } : {}),
        });
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

function replayWindowFields(tracker) {
  return {
    ...(Object.prototype.hasOwnProperty.call(tracker, "queued_at_ms") ? { queued_at_ms: tracker.queued_at_ms } : {}),
    ...(Object.prototype.hasOwnProperty.call(tracker, "replay_expires_at_ms") ? { replay_expires_at_ms: tracker.replay_expires_at_ms } : {}),
  };
}

function unknownTrackerFields(tracker) {
  return tracker.delivery_outcome === "outcome_unknown"
    ? {
      delivery_outcome: "outcome_unknown",
      ...(tracker.delivery_error_code ? { delivery_error_code: tracker.delivery_error_code } : {}),
      ...(tracker.delivery_error ? { delivery_error: tracker.delivery_error } : {}),
    }
    : {};
}

export function shouldRetryActionFailure(error) {
  const status = error && typeof error === "object" ? error.status : undefined;
  if (!Number.isInteger(status)) return true;
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}
