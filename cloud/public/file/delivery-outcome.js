const UNKNOWN_OUTCOME = "outcome_unknown";

function boundedMessage(action) {
  const value = action?.delivery_error;
  return typeof value === "string" && value.trim() && value.length <= 512
    ? value
    : "The request may or may not have reached your computer. Check the desktop before submitting it again.";
}

export function isUnknownDeliveryAction(action, remembered = null) {
  return action?.delivery_outcome === UNKNOWN_OUTCOME
    || typeof action?.request_id === "string" && remembered instanceof Map && remembered.has(action.request_id);
}

export function unknownFileActionForKind(queue, kind, remembered = null) {
  if (!Array.isArray(queue) || typeof kind !== "string") return null;
  return queue.find((action) => action?.kind === kind && isUnknownDeliveryAction(action, remembered)) ?? null;
}

export function markUnknownFileAction(state, action) {
  const requestId = action?.request_id;
  if (typeof requestId !== "string" || !requestId || action?.delivery_outcome !== UNKNOWN_OUTCOME) {
    return { downloadSubmission: false, migrationConflictDrafts: 0 };
  }
  state.unknownActionOutcomes ??= new Map();
  state.unknownActionOutcomes.set(requestId, {
    kind: action.kind,
    code: typeof action.delivery_error_code === "string" ? action.delivery_error_code : "action_replay_window_unverifiable",
    error: boundedMessage(action),
  });
  state.quarantinedActionIds?.add(requestId);

  let downloadSubmission = false;
  if (action.kind === "download_submit" && state.downloadSubmission?.request_id === requestId) {
    state.downloadSubmission.outcomeUnknown = true;
    state.downloadSubmission.outcomeError = boundedMessage(action);
    state.downloadSubmission.delivery_outcome = UNKNOWN_OUTCOME;
    state.downloadSubmission.delivery_error_code = typeof action.delivery_error_code === "string"
      ? action.delivery_error_code
      : "action_replay_window_unverifiable";
    state.downloadSubmission.delivery_error = boundedMessage(action);
    downloadSubmission = true;
  }

  let migrationConflictDrafts = 0;
  if (action.kind === "library_migration_preview" && state.migrationConflictDrafts instanceof Map) {
    for (const [draftKey, draft] of state.migrationConflictDrafts) {
      if (draft?.refreshRequestId !== requestId) continue;
      draft.refreshOutcomeUnknown = true;
      draft.refreshDeliveryOutcome = UNKNOWN_OUTCOME;
      draft.refreshDeliveryErrorCode = typeof action.delivery_error_code === "string"
        ? action.delivery_error_code
        : "action_replay_window_unverifiable";
      draft.refreshError = boundedMessage(action);
      state.migrationConflictInvalidated?.add(draftKey);
      migrationConflictDrafts += 1;
    }
  }
  return { downloadSubmission, migrationConflictDrafts };
}

export function dismissUnknownFileAction(state, requestId, queue, writeQueue) {
  if (typeof requestId !== "string" || !requestId || !Array.isArray(queue) || typeof writeQueue !== "function") {
    return { status: "invalid" };
  }
  const action = queue.find((entry) => entry?.request_id === requestId);
  const remembered = state?.unknownActionOutcomes instanceof Map ? state.unknownActionOutcomes.get(requestId) : null;
  if (!action || !isUnknownDeliveryAction(action, state?.unknownActionOutcomes)) return { status: "not_unknown" };

  const remaining = queue.filter((entry) => entry?.request_id !== requestId);
  if (!writeQueue(remaining)) return { status: "storage_failed", action };

  state.quarantinedActionIds?.delete(requestId);
  state.unknownActionOutcomes?.delete(requestId);
  let downloadSubmission = false;
  if (state.downloadSubmission?.request_id === requestId) {
    state.downloadSubmission = null;
    state.downloadMessage = "The delivery outcome is unknown. The browser notice was dismissed; check your computer before adding this request again.";
    downloadSubmission = true;
  }

  let migrationConflictDrafts = 0;
  if (state.migrationConflictDrafts instanceof Map) {
    for (const draft of state.migrationConflictDrafts.values()) {
      if (draft?.refreshRequestId !== requestId) continue;
      draft.refreshRequestId = null;
      draft.refreshSeq = null;
      draft.refreshPayload = null;
      draft.refreshHistory = null;
      draft.refreshOutcomeUnknown = false;
      draft.refreshDeliveryOutcome = null;
      draft.refreshDeliveryErrorCode = null;
      draft.refreshError = remembered?.error
        ?? "The delivery outcome is unknown. The browser notice was dismissed; check your computer before requesting another preview.";
      migrationConflictDrafts += 1;
    }
  }
  return { status: "dismissed", action, downloadSubmission, migrationConflictDrafts };
}
