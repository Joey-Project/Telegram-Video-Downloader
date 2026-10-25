import { ACTION_RECEIPT_STORAGE_PREFIX, ACTION_SURFACE_FILE, consumeRejectedActionReceipts, dispatchNextOwnedAction, persistQueuedActionAndDrain, recordActionReceipt, recoverActionReceipts, removeQueuedAction, restorePendingActions, shouldRetryActionFailure } from "./action-queue.js";
import { buildLegacyMappingChoices, buildLegacyMetadataPatches, getOrCreateLegacySelection, LEGACY_MAPPING_PAGE_SIZE, paginateLegacyMappingChoices, selectLegacyImportForMapping, selectLegacyMappingChoice, updateLegacySelection } from "./legacy-mapping-state.js";
import { hasMetadataPatchFields, hasMetadataPreviewChanges, metadataPreviewDetails } from "./metadata-preview.js";
import { buildMetadataRecoveryPreviewPayload, buildOrganizationStatusPatch, groupMetadataRecoveryItems, metadataRecoveryOperationId, parseMetadataRecoveryReport } from "./metadata-recovery.js";
import { readRecoveryPreviewRequest, writeRecoveryPreviewRequest } from "./metadata-recovery-preview-state.js";
import { HttpResponseError } from "./http-error.js";
import { createOwnerSnapshotLoader } from "./snapshot-loader.js";
import { createSnapshotUpdates } from "./snapshot-updates.js";
import { acceptOwnerSnapshot, beginOwnerVerification, drainActionsAfterSnapshotRefresh, failOwnerSnapshot, mayDispatchActions, mayUsePrivateSnapshot } from "./snapshot-session.js";
import { RECENT_OPERATION_SNAPSHOT_LIMIT, RECENT_SNAPSHOT_MAX_AGE_MS } from "./download-submission-history.js";
import { MAX_SELECTED_ITEMS, updateSelectionWithinLimit } from "./selection-limit.js";
import { MAX_MANUAL_TARGET_OPTIONS, searchPresentManualTargets } from "./manual-target-picker.js";
import { MAX_MIGRATION_ITEM_CONFLICTS, buildMigrationPreviewRefreshPayload, migrationConflictChoicesChanged, migrationConflictPolicy, migrationControlActions, migrationRefreshMatches, normalizeMigrationItemConflicts, setMigrationConflictChoice } from "./migration-conflicts.js";
import { submitLegacyFileImport } from "./legacy-file-import.js";
import { buildWaitingEntries, isIncompletePreview, isPresentMedia, isReviewablePreviewStatus, isWaitingEntryPending, operationDetailsWarning, operationHistoryEntries, presentMediaItems } from "./state-view.js";
import { sourceIdUrl } from "./source-links.js";
import { escapeHtml, renderMovePreviewItems } from "./preview-rendering.js";
import { registeredMediaLibrary } from "./library-view.js";
import { validateTargetRelativeDirectory } from "./target-directory.js";
import { boundedFolderSuggestions, buildMigrationPreviewPayloadFromRenderedOptions, canOperateOnRegisteredMediaLibrary, collectionIdentity, defaultMediaLibraryId, filterLibraryItems, groupLibraryItems, isValidLibraryRootBinding, itemFolder, librarySelectionCounts, LIBRARY_PAGE_SIZE, mediaLibraryAvailability, mediaLibraryEntries, mediaSnapshotInfo, mediaSnapshotOmissionNotice, migrationEndpointAvailability, migrationPreviewBindingsMatch, movedToTarget, needsOrganizing, operationLibraryId, operationMatchesLibrary, paginateLibraryItems, renderMigrationTargetOptions, resolveMediaLibrary, selectMigrationTarget, statusOf } from "./library-view.js";
import { incompleteMigrationTaskSummary } from "./migration-task-summary.js";
import { renderMigrationDiscoveryWarnings, renderMigrationHistory } from "./migration-rendering.js";
import { createMigrationRefreshQueuedCallback, inspectMigrationRefreshHistory, migrationPreviewDraftKey, migrationPreviewEntries as collectMigrationPreviewEntries, migrationPreviewPageWindow, reconcileMigrationRefreshDrafts, releaseMigrationRefreshTracking, renderMigrationPreview as renderMigrationPreviewCard } from "./migration-preview-rendering.js";
import { inspectDownloadSubmissionHistory, releaseDownloadSubmissionTracking } from "./download-submission-history.js";
import { dismissUnknownFileAction, isUnknownDeliveryAction, markUnknownFileAction, unknownFileActionForKind } from "./delivery-outcome.js";
import { buildDownloadSubmitPayload, isBoundDownloadLibraryOption } from "./download-submit.js";
import { createFileQueuedAction, fileActionRequestBody } from "./file-action-dispatch.js";
import { buildFilePreviewPayloadFromRenderedOption, buildLegacyMetadataPreviewPayloadFromRenderedOption, buildSelectedFilePreviewPayloadFromRenderedOption, clearFileIdentityScopedStateOnSnapshot, fileOperationIdentityMatches, fileOperationLibraryIdentity } from "./file-preview-identity.js";
import { PREVIEW_CANCELLATION_STORAGE_KEY, readPreviewCancellations, writePreviewCancellations } from "./preview-cancellation-state.js";

const API = "/api";
const loadOwnerSnapshot = createOwnerSnapshotLoader();
const CACHE_KEY = "file-manager.snapshot.v1";
const ACTION_QUEUE_KEY = "file-manager.actions.v1";
const MAX_LEGACY_BYTES = 256 * 1024;

const telegram = window.Telegram?.WebApp;
const initData = telegram?.initData ?? "";
const initialParams = new URLSearchParams(window.location.search);
const state = { ownerVerified: false, privateUiVisible: false, canDispatchActions: false, snapshot: null, libraryId: initialParams.get("library_id") ?? "", selected: new Set(), groupSelectionLimitNotice: null, quarantinedActionIds: new Set(), unknownActionOutcomes: new Map(), view: "library", query: "", historyQuery: "", libraryPage: 0, needsPage: 0, detailId: null, legacyOperation: null, legacyOperationKey: null, legacySelection: new Map(), legacyPage: 0, manualPickerKey: null, manualPickerQuery: "", toastTimer: 0, loading: false, draining: null, retryDelay: 1500, retryTimer: 0, refreshAfterDrain: false, downloadSubmission: null, downloadMessage: null, downloadFormOpened: false, metadataRecovery: null, metadataRecoveryPreviewRequest: null, previewCancellations: new Map(), previewCancellationAttempts: new Set(), dismissedRecoveryRequestIds: new Set(), recoveryHiddenThroughSeq: new Map(), migrationConflictDrafts: new Map(), migrationConflictInvalidated: new Set() };
let snapshotUpdates = null;
let snapshotUpdateStatus = "connecting";
let privateAccessExpired = false;
let metadataRecoveryPreviewRestoreAttempted = false;
let previewCancellationRestoreAttempted = false;

const byId = (id) => document.getElementById(id);
let cachedFolderLibrary = null;
let cachedFolders = [];
const formatBytes = (value) => {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes < 0) return "Size unavailable";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let size = bytes;
  let unit = -1;
  do { size /= 1024; unit += 1; } while (size >= 1024 && unit < units.length - 1);
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unit]}`;
};
const formatTime = (value, unit = "milliseconds") => {
  const number = Number(value);
  const date = new Date(unit === "seconds" ? number * 1000 : number);
  if (!Number.isFinite(date.getTime()) || date.getTime() <= 0) return "Not available";
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(date);
};
const timeAgo = (value, unit = "milliseconds") => {
  const raw = Number(value);
  const timestamp = unit === "seconds" ? raw * 1000 : raw;
  if (!Number.isFinite(timestamp) || timestamp <= 0) return "Not yet";
  const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
  if (seconds < 60) return "Just now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} hr ago`;
  return `${Math.floor(seconds / 86400)} days ago`;
};
const activeLibrary = () => resolveMediaLibrary(state.snapshot, state.libraryId);
const activeLibrarySnapshot = () => activeLibrary().snapshot;
const activeLibraryId = () => activeLibrary().id;
const libraryItems = () => Array.isArray(activeLibrarySnapshot()?.items) ? activeLibrarySnapshot().items : [];
const snapshotValue = () => state.snapshot ?? { synced: false, stale: true, library: null, previews: [], operations: [], tasks: [], settings: {} };

function libraryCanOperate(libraryId = activeLibraryId()) {
  return canOperateOnRegisteredMediaLibrary(state.snapshot, libraryId);
}

function selectionLibraryReadOnlyReason() {
  const descriptor = activeLibrary().descriptor;
  const offline = mediaLibraryAvailability(descriptor) === "offline";
  const savedScan = descriptor?.snapshot_stale === true;
  if (offline && savedScan) return "Library offline; its saved scan is read-only.";
  if (savedScan) return "Saved scan is read-only; refresh before selecting.";
  if (offline) return "Library offline; reconnect before selecting.";
  return "Library is read-only; refresh its current scan before selecting.";
}

function mediaSelectionReason(item, selectionEnabled, movedTarget = null) {
  const presence = statusOf(item);
  const reasons = [];
  if (presence === "missing" && movedTarget) reasons.push("This source history item cannot be selected; open its destination item.");
  else if (presence === "missing") reasons.push("Missing from this library; cannot select. Run a scan to refresh its status.");
  else if (presence === "unavailable") reasons.push("Unavailable in this library; cannot select.");
  else if (presence !== "present") reasons.push("File availability is unknown; scan before selecting.");
  if (!selectionEnabled) reasons.push(selectionLibraryReadOnlyReason());
  return reasons.join(" ");
}

function accessibleToken(value) {
  return Array.from(String(value ?? ""), (character) => character.codePointAt(0).toString(16)).join("-") || "empty";
}

function filePreviewPayload(fields, expectedIdentity = null) {
  const libraryOption = byId("library-select")?.selectedOptions?.[0];
  if (libraryOption?.value !== activeLibraryId()) return null;
  return buildFilePreviewPayloadFromRenderedOption({
    ownerSnapshot: state.snapshot,
    libraryOption,
    expectedIdentity,
    fields,
  });
}

function dismissMetadataRecoveryId(id) {
  if (!id) return;
  state.dismissedRecoveryRequestIds.add(String(id));
  while (state.dismissedRecoveryRequestIds.size > 128) {
    state.dismissedRecoveryRequestIds.delete(state.dismissedRecoveryRequestIds.values().next().value);
  }
}

function clearMetadataRecoveryReport({ dismiss = true } = {}) {
  const current = state.metadataRecovery;
  if (dismiss && current?.requestId) {
    const latestSeq = recoveryOperationsForLibrary(current.libraryId)
      .reduce((maximum, operation) => Number.isSafeInteger(operation?.seq) ? Math.max(maximum, operation.seq) : maximum, 0);
    if (latestSeq > 0) {
      const previous = state.recoveryHiddenThroughSeq.get(current.libraryId) ?? 0;
      state.recoveryHiddenThroughSeq.set(current.libraryId, Math.max(previous, latestSeq));
    }
    dismissMetadataRecoveryId(current.requestId);
    if (Number.isSafeInteger(current.seq)) dismissMetadataRecoveryId(String(current.seq));
  }
  state.metadataRecovery = null;
}

function setMetadataRecoveryPreviewRequest(request) {
  const next = request ? { ...request } : null;
  state.metadataRecoveryPreviewRequest = next;
  metadataRecoveryPreviewRestoreAttempted = true;
  try { writeRecoveryPreviewRequest(localStorage, next); } catch { /* The in-memory request remains available for this page. */ }
}

const PERSISTED_PREVIEW_CANCELLATION_STATUSES = new Set(["queued", "submitted", "unknown"]);

function safelyGetFileStorage() {
  try { return window.localStorage; } catch { return null; }
}

function readStoredPreviewCancellations(storage) {
  if (!storage || typeof storage.getItem !== "function") return null;
  let raw;
  try { raw = storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY); } catch { return null; }
  let stored;
  try { stored = readPreviewCancellations(storage); } catch { return null; }
  if (!(stored instanceof Map) || raw !== null && stored.size === 0) return null;
  return stored;
}

function mergePreviewCancellationTrackers(current, incoming) {
  if (current?.status === "failed" && current.requestId === incoming?.requestId) return current;
  if (current?.requestId !== incoming?.requestId) return incoming;
  const rank = { queued: 0, unknown: 1, submitted: 2, failed: 3 };
  const status = rank[String(current?.status ?? "").toLowerCase()] >= rank[String(incoming?.status ?? "").toLowerCase()]
    ? current.status
    : incoming.status;
  const preferred = status === current?.status ? current : incoming;
  return { ...preferred, requestId: current.requestId, seq: current.seq ?? incoming.seq, status };
}

function persistPreviewCancellations({ removeKeys = [] } = {}) {
  const storage = safelyGetFileStorage();
  if (!storage) return false;
  const removed = new Set(removeKeys);
  const stored = readStoredPreviewCancellations(storage);
  if (!stored) return false;
  for (const [key, tracker] of stored) {
    if (removed.has(key)) continue;
    const current = state.previewCancellations.get(key);
    if (!current) state.previewCancellations.set(key, { ...tracker });
    else if (current.status !== "failed") {
      state.previewCancellations.set(key, mergePreviewCancellationTrackers(current, tracker));
    }
  }
  const pending = new Map([...state.previewCancellations].filter(([key, tracker]) =>
    !removed.has(key) && PERSISTED_PREVIEW_CANCELLATION_STATUSES.has(String(tracker?.status ?? "").toLowerCase())));
  try { return writePreviewCancellations(storage, pending); } catch { return false; }
}

function mergePreviewCancellationsFromStorage() {
  if (!state.ownerVerified || !state.privateUiVisible) return false;
  const storage = safelyGetFileStorage();
  const stored = readStoredPreviewCancellations(storage);
  if (!stored) return false;
  let changed = false;
  for (const [key, current] of state.previewCancellations) {
    if (current?.status === "failed" || stored.has(key)) continue;
    state.previewCancellations.delete(key);
    state.previewCancellationAttempts.delete(key);
    changed = true;
  }
  for (const [key, incoming] of stored) {
    const current = state.previewCancellations.get(key);
    const merged = current
      ? current.status === "failed" && current.requestId !== incoming.requestId
        ? { ...incoming }
        : mergePreviewCancellationTrackers(current, incoming)
      : { ...incoming };
    if (!current || current.requestId !== merged.requestId || current.seq !== merged.seq
        || current.status !== merged.status || current.error !== merged.error) {
      state.previewCancellations.set(key, merged);
      changed = true;
    }
  }
  return changed;
}

function restorePreviewCancellations() {
  if (previewCancellationRestoreAttempted || !state.ownerVerified || !state.privateUiVisible) return false;
  const storage = safelyGetFileStorage();
  if (!storage) return false;
  const restored = readStoredPreviewCancellations(storage);
  if (!(restored instanceof Map)) return false;
  previewCancellationRestoreAttempted = true;
  for (const [key, tracker] of restored) {
    if (!PERSISTED_PREVIEW_CANCELLATION_STATUSES.has(String(tracker?.status ?? "").toLowerCase())) continue;
    state.previewCancellations.set(key, { ...tracker });
  }
  persistPreviewCancellations();
  return true;
}

function restoreMetadataRecoveryPreviewRequest(snapshot) {
  if (metadataRecoveryPreviewRestoreAttempted || !state.ownerVerified || !state.privateUiVisible) return false;
  metadataRecoveryPreviewRestoreAttempted = true;
  if (state.metadataRecoveryPreviewRequest) return false;

  let request = null;
  try { request = readRecoveryPreviewRequest(localStorage); } catch { return false; }
  if (!request) return false;
  if (registeredMediaLibrary(snapshot, request.libraryId)?.root_binding !== request.rootBinding) {
    setMetadataRecoveryPreviewRequest(null);
    return true;
  }
  setMetadataRecoveryPreviewRequest(request);
  return true;
}

function clearMetadataRecoveryState({ dismiss = true, preservePreviewRequest = false } = {}) {
  clearMetadataRecoveryReport({ dismiss });
  if (!preservePreviewRequest) setMetadataRecoveryPreviewRequest(null);
}

function recoveryContext() {
  const option = byId("library-select")?.selectedOptions?.[0];
  if (option?.value !== activeLibraryId()) return null;
  const libraryId = activeLibraryId();
  const rootBinding = option?.dataset?.rootBinding ?? "";
  const revision = activeLibrarySnapshot()?.revision;
  if (!libraryId || !isValidLibraryRootBinding(rootBinding) || typeof revision !== "string" || !revision) return null;
  return { libraryId, rootBinding, revision };
}

function metadataRecoveryReceiptIdentity(operation) {
  if (!operation || typeof operation !== "object" || Array.isArray(operation)
      || !Object.hasOwn(operation, "recovery_identity")) return { present: false, identity: null };
  const value = operation.recovery_identity;
  const fields = ["library_id", "root_binding", "scope"];
  if (!value || typeof value !== "object" || Array.isArray(value)
      || Object.keys(value).length !== fields.length
      || !fields.every((field) => Object.hasOwn(value, field))
      || Object.keys(value).some((field) => !fields.includes(field))
      || !["selected", "library"].includes(value.scope)) {
    return { present: true, identity: null };
  }
  const parsed = fileOperationLibraryIdentity({ recovery_identity: value });
  if (!parsed || parsed.library_id !== value.library_id || parsed.root_binding !== value.root_binding
      || !isValidLibraryRootBinding(value.root_binding)) {
    return { present: true, identity: null };
  }
  return {
    present: true,
    identity: Object.freeze({ library_id: parsed.library_id, root_binding: parsed.root_binding, scope: value.scope }),
  };
}

function recoveryOperationScope(operation) {
  const receipt = metadataRecoveryReceiptIdentity(operation);
  if (receipt.present) return receipt.identity?.scope ?? "unknown";
  const result = operation?.result && typeof operation.result === "object" ? operation.result : {};
  const candidates = [result.scope, operation?.scope, operation?.payload?.scope];
  return candidates.find((scope) => scope === "selected" || scope === "library") ?? "unknown";
}

function recoveryOperationMatchesLibrary(operation, libraryId) {
  const receipt = metadataRecoveryReceiptIdentity(operation);
  if (receipt.present) {
    if (!receipt.identity || receipt.identity.library_id !== libraryId) return false;
    // Check the receipt's identity independently so a malformed or mismatched
    // completed result cannot make a valid newer intent look foreign.
    return fileOperationIdentityMatches(state.snapshot, { recovery_identity: receipt.identity }, libraryId);
  }
  return operationMatchesLibrary(operation, libraryId)
    && fileOperationIdentityMatches(state.snapshot, operation, libraryId);
}

function recoveryOperationsForLibrary(libraryId) {
  return (Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [])
    .filter((operation) => operation?.kind === "metadata_recover"
      && recoveryOperationMatchesLibrary(operation, libraryId))
    .sort((left, right) => Number(right?.seq ?? 0) - Number(left?.seq ?? 0));
}

function recoveryOperationCompacted(operation) {
  const result = operation?.result && typeof operation.result === "object" ? operation.result : {};
  return operation?.details_incomplete === true || operation?.result_truncated === true
    || result.details_incomplete === true || result.result_truncated === true;
}

function recoveryOperationReport(operation) {
  if (!operation || recoveryOperationCompacted(operation)) return null;
  const result = operation.result && typeof operation.result === "object" ? operation.result : {};
  const receipt = metadataRecoveryReceiptIdentity(operation);
  const boundIdentity = receipt.present
    ? receipt.identity
    : fileOperationLibraryIdentity(operation);
  if (!boundIdentity || result.library_id !== boundIdentity.library_id
      || result.root_binding !== boundIdentity.root_binding
      || !["selected", "library"].includes(result.scope)
      || receipt.present && result.scope !== boundIdentity.scope) return null;
  return parseMetadataRecoveryReport(result.recovery);
}

function recoveryOperationMatches(operation, recovery) {
  if (recoveryOperationScope(operation) !== recovery.scope) return false;
  return metadataRecoveryOperationId(operation) === recovery.requestId
    || Number.isSafeInteger(recovery.seq) && Number.isSafeInteger(operation?.seq) && operation.seq === recovery.seq;
}

function currentMetadataRecovery() {
  const context = recoveryContext();
  if (!context) return null;
  let recovery = state.metadataRecovery;
  if (!recovery) {
    const queuedAction = [...readActionQueue()].reverse().find((action) => {
      const payload = action?.payload && typeof action.payload === "object" ? action.payload : {};
      return action?.kind === "metadata_recover" && payload.library_id === context.libraryId
        && payload.root_binding === context.rootBinding;
    });
    const requestId = metadataRecoveryOperationId(queuedAction);
    if (queuedAction && requestId && !state.dismissedRecoveryRequestIds.has(requestId)) {
      const payload = queuedAction.payload && typeof queuedAction.payload === "object" ? queuedAction.payload : {};
      recovery = {
        requestId,
        libraryId: context.libraryId,
        rootBinding: context.rootBinding,
        scope: payload.scope === "selected" ? "selected" : "library",
        report: null,
        selectedIds: new Set(),
        candidateChoices: new Map(),
        invalidated: false,
        compacted: false,
        deliveryUnknown: queuedAction.delivery_outcome === "outcome_unknown",
        error: null,
      };
      state.metadataRecovery = recovery;
    }
  }
  if (recovery && (recovery.libraryId !== context.libraryId || recovery.rootBinding !== context.rootBinding)) {
    const previewRequest = state.metadataRecoveryPreviewRequest;
    const recoveryRootChanged = recovery.libraryId === context.libraryId
      && recovery.rootBinding !== context.rootBinding;
    clearMetadataRecoveryState({ preservePreviewRequest: true });
    if (recoveryRootChanged && previewRequest?.libraryId === recovery.libraryId
        && previewRequest.rootBinding !== context.rootBinding) {
      setMetadataRecoveryPreviewRequest(null);
    }
    recovery = null;
  }
  if (recovery) {
    if (recovery.report && recovery.report.revision !== context.revision) {
      recovery.report = null;
      recovery.selectedIds.clear();
      recovery.candidateChoices.clear();
      recovery.invalidated = true;
    }
    const operation = recovery.requestId
      ? recoveryOperationsForLibrary(context.libraryId).find((entry) => recoveryOperationMatches(entry, recovery))
      : null;
    if (operation) {
      if (recoveryOperationCompacted(operation)) {
        recovery.compacted = true;
        recovery.invalidated = true;
        recovery.report = null;
        recovery.selectedIds.clear();
        recovery.candidateChoices.clear();
      }
      else {
        const report = recoveryOperationReport(operation);
        if (report) {
          if (report.revision === context.revision && !recovery.invalidated) {
            recovery.report = report;
            recovery.error = null;
          } else {
            recovery.report = null;
            recovery.selectedIds.clear();
            recovery.candidateChoices.clear();
            recovery.invalidated = true;
          }
        } else if (["failed", "stale", "rejected"].includes(String(operation.status))) {
          recovery.error = String(operation.error ?? operation.message ?? "Metadata analysis failed on your computer.");
        } else if (["completed", "complete", "succeeded"].includes(String(operation.status))) {
          recovery.invalidated = true;
          recovery.report = null;
          recovery.selectedIds.clear();
          recovery.candidateChoices.clear();
        }
      }
    }
    return recovery;
  }

  for (const operation of recoveryOperationsForLibrary(context.libraryId)) {
    const requestId = metadataRecoveryOperationId(operation);
    if (!requestId || state.dismissedRecoveryRequestIds.has(requestId)) continue;
    const hiddenThroughSeq = state.recoveryHiddenThroughSeq.get(context.libraryId) ?? 0;
    if (Number.isSafeInteger(operation?.seq) && operation.seq <= hiddenThroughSeq) continue;
    const recovery = {
      requestId,
      libraryId: context.libraryId,
      rootBinding: context.rootBinding,
      scope: recoveryOperationScope(operation),
      report: null,
      selectedIds: new Set(),
      candidateChoices: new Map(),
      invalidated: false,
      compacted: recoveryOperationCompacted(operation),
      error: null,
    };
    const report = recoveryOperationReport(operation);
    if (report && report.revision === context.revision) recovery.report = report;
    else if (report || recovery.compacted) recovery.invalidated = true;
    else if (["failed", "stale", "rejected"].includes(String(operation.status))) {
      recovery.error = String(operation.error ?? operation.message ?? "Metadata analysis failed on your computer.");
    } else if (["completed", "complete", "succeeded"].includes(String(operation.status))) {
      recovery.invalidated = true;
    }
    state.metadataRecovery = recovery;
    return recovery;
  }
  return null;
}

async function requestMetadataRecovery(scope, { deferredItemIds = null } = {}) {
  if (!libraryCanOperate()) return showToast("This library is offline or showing a saved scan. Reconnect it before analyzing metadata.");
  const context = recoveryContext();
  if (!context) return showToast("The displayed library identity or revision is unavailable. Refresh the library before analyzing metadata.");
  const current = deferredItemIds === null ? null : currentMetadataRecovery();
  if (deferredItemIds !== null && (scope !== "selected" || !Array.isArray(deferredItemIds)
      || deferredItemIds.length === 0 || deferredItemIds.length > MAX_SELECTED_ITEMS
      || !current || current.scope !== "selected" || !current.report
      || current.report.revision !== context.revision
      || deferredItemIds.some((id) => !current.report.items.some((item) => item.item_id === id && item.status === "deferred")))) {
    return showToast("The deferred results no longer match this library revision. Analyze the current selection again before retrying.");
  }
  const payload = {
    library_id: context.libraryId,
    root_binding: context.rootBinding,
    scope,
    ...(scope === "selected" ? { item_ids: deferredItemIds === null ? [...state.selected] : [...deferredItemIds] } : {}),
  };
  if (scope === "selected" && (payload.item_ids.length === 0 || payload.item_ids.length > MAX_SELECTED_ITEMS)) {
    return showToast(`Select between 1 and ${MAX_SELECTED_ITEMS} present items to analyze metadata.`);
  }
  if (scope === "library") {
    const current = currentMetadataRecovery();
    if (current?.scope === "library" && current.report?.remaining > 0 && current.report.next_cursor) payload.after_item_id = current.report.next_cursor;
  }
  await submitAction("metadata_recover", payload, {
    onQueued: (action) => {
      state.dismissedRecoveryRequestIds.delete(action.request_id);
      state.metadataRecovery = {
        requestId: action.request_id,
        libraryId: context.libraryId,
        rootBinding: context.rootBinding,
        scope,
        report: null,
        selectedIds: new Set(),
        candidateChoices: new Map(),
        invalidated: false,
        compacted: false,
        error: null,
      };
      renderMetadataRecovery();
    },
  });
}

function previewMetadataRecoverySelection() {
  const context = recoveryContext();
  const recovery = currentMetadataRecovery();
  if (!libraryCanOperate() || !context || !recovery?.report || recovery.report.revision !== context.revision) {
    return showToast("This analysis is out of date. Analyze the current library before previewing metadata changes.");
  }
  const payload = buildMetadataRecoveryPreviewPayload({
    report: recovery.report,
    libraryId: context.libraryId,
    rootBinding: context.rootBinding,
    itemIds: [...recovery.selectedIds],
    candidateChoices: recovery.candidateChoices,
  });
  if (!payload) return showToast("Select only ready items and choose a candidate for every item that needs confirmation.");
  submitAction("file_preview", payload, {
    onQueued: (action) => {
      state.view = "waiting";
      setMetadataRecoveryPreviewRequest({
        requestId: action.request_id,
        seq: null,
        libraryId: context.libraryId,
        rootBinding: context.rootBinding,
        status: "queued",
        error: "",
      });
      render();
    },
  });
}

function updateMetadataRecoveryPreviewRequest(action, { status, error = "", seq = undefined } = {}) {
  const request = state.metadataRecoveryPreviewRequest;
  if (action?.kind !== "file_preview" || !request || action.request_id !== request.requestId) return false;
  request.status = status;
  request.error = String(error ?? "");
  if (seq !== undefined) request.seq = Number.isSafeInteger(seq) && seq > 0 ? seq : null;
  setMetadataRecoveryPreviewRequest(request);
  renderPreviews();
  return true;
}

function metadataRecoveryPreviewSnapshotMatches(entry, request, snapshot) {
  if (entry?.kind !== "file_preview" || !Number.isSafeInteger(entry.seq) || entry.seq <= 0) return false;
  const identity = fileOperationLibraryIdentity(entry);
  if (identity?.library_id !== request.libraryId
      || identity.root_binding !== request.rootBinding
      || !fileOperationIdentityMatches(snapshot, entry, request.libraryId)) return false;
  return typeof entry.request_id === "string"
    && (entry.request_id === request.requestId || entry.request_id === `action:${request.requestId}`);
}

function reconcileMetadataRecoveryPreviewRequest(snapshot) {
  const request = state.metadataRecoveryPreviewRequest;
  if (!request || !snapshot) return false;
  let updated = { ...request };

  const operations = Array.isArray(snapshot.operations) ? snapshot.operations : [];
  const operation = operations.find((entry) => metadataRecoveryPreviewSnapshotMatches(entry, request, snapshot));
  if (operation) {
    if (Number.isSafeInteger(operation.seq) && operation.seq > 0) updated.seq = operation.seq;
    const status = String(operation.status ?? "").toLowerCase();
    updated.error = String(operation.error ?? "");
    if (["failed", "rejected", "stale", "cancelled", "expired"].includes(status)) {
      updated.status = status;
    } else if (status === "completed") {
      setMetadataRecoveryPreviewRequest(null);
      return true;
    } else {
      updated.status = "submitted";
    }
    setMetadataRecoveryPreviewRequest(updated);
  }

  const previews = Array.isArray(snapshot.previews) ? snapshot.previews : [];
  const preview = previews.find((entry) => metadataRecoveryPreviewSnapshotMatches(entry, request, snapshot));
  if (preview) {
    setMetadataRecoveryPreviewRequest(null);
    return true;
  }
  return Boolean(operation);
}

function previewOrganizationStatus(item, organizationStatus) {
  if (!libraryCanOperate()) return showToast("This library is offline or showing a saved scan. Reconnect it before changing metadata.");
  const patch = buildOrganizationStatusPatch(item, organizationStatus);
  if (!patch) return showToast("An item with an existing collection cannot be marked standalone. Its collection will be preserved.");
  const payload = filePreviewPayload({
    item_ids: [String(item.id)],
    metadata_patches: [patch],
    rename: false,
    conflict: "skip",
  });
  if (!payload) return showToast("The displayed library identity changed. Refresh the library before previewing this metadata update.");
  submitAction("file_preview", payload);
}

function migrationLibraryId(operation, result, field) {
  for (const candidate of [result, operation]) {
    if (!candidate || typeof candidate !== "object" || !Object.prototype.hasOwnProperty.call(candidate, field)) continue;
    const value = candidate[field];
    return typeof value === "string" && value.length > 0 && value.length <= 128
      && value.trim() === value && !/[\u0000-\u001f\u007f]/u.test(value)
      ? value
      : "";
  }
  return "";
}

function normalizeLibraryContext() {
  const entries = mediaLibraryEntries(state.snapshot);
  const valid = entries.some((entry) => entry.id === state.libraryId);
  if (!valid) state.libraryId = defaultMediaLibraryId(state.snapshot);
}

function libraryLabel(libraryId) {
  const descriptor = mediaLibraryEntries(state.snapshot).find((entry) => entry.id === libraryId);
  return descriptor ? `${descriptor.name} · ${descriptor.root}` : "Library location unavailable";
}

function syncLibraryUrl() {
  const url = new URL(window.location.href);
  if (state.libraryId) url.searchParams.set("library_id", state.libraryId);
  else url.searchParams.delete("library_id");
  window.history.replaceState(null, "", url.toString());
}

function renderLibrarySelector() {
  const selector = byId("library-select");
  const entries = mediaLibraryEntries(state.snapshot);
  normalizeLibraryContext();
  selector.innerHTML = entries.map((entry) => {
    const suffix = entry.is_default === true ? " · default" : "";
    const availability = mediaLibraryAvailability(entry);
    const status = availability === "offline"
      ? " · offline"
      : entry.snapshot_stale === true ? " · saved scan" : "";
    return `<option value="${escapeHtml(entry.id)}" data-root-binding="${escapeHtml(entry.root_binding ?? "")}">${escapeHtml(entry.name)}${suffix}${status}</option>`;
  }).join("");
  selector.value = state.libraryId;
  selector.disabled = entries.length < 2;
  const location = activeLibrary();
  const status = mediaLibraryAvailability(location.descriptor);
  byId("library-location").textContent = location.descriptor?.root || "Media root unavailable";
  byId("library-local-status").textContent = status === "offline" ? "Offline · last saved view" : status === "available" ? "Available on the desktop" : "Availability not reported";
  byId("library-local-status").className = `badge${status === "available" ? " good" : status === "offline" ? " warning" : ""}`;
}

function renderDownloadStatus() {
  const status = byId("download-status");
  const submit = byId("download-submit");
  const clearWait = byId("download-clear-wait");
  if (!status || !submit || !clearWait) return;
  const submission = state.downloadSubmission;
  const history = inspectDownloadSubmissionHistory(submission, state.snapshot);
  const queuedUnknown = unknownFileActionForKind(readActionQueue(), "download_submit", state.unknownActionOutcomes);
  const selectedOption = byId("download-library-select")?.selectedOptions?.[0];
  const hasBoundLibrary = isBoundDownloadLibraryOption(selectedOption);
  submit.disabled = Boolean(submission) || Boolean(queuedUnknown) || !libraryCanOperate(selectedOption?.value || activeLibraryId()) || !hasBoundLibrary;
  const deliveryUnknown = submission?.outcomeUnknown === true || Boolean(queuedUnknown);
  clearWait.classList.toggle("hidden", history.kind !== "outside_recent_history" && !deliveryUnknown);
  clearWait.textContent = deliveryUnknown ? "Dismiss after checking computer" : "Clear waiting status";
  if (deliveryUnknown) {
    const remembered = queuedUnknown && state.unknownActionOutcomes.get(queuedUnknown.request_id);
    const outcomeError = submission?.outcomeError ?? queuedUnknown?.delivery_error ?? remembered?.error ?? "The delivery outcome is unknown.";
    status.textContent = `${outcomeError} This request will not be sent again automatically. Check your computer before dismissing this notice.`;
    status.classList.add("warning-text");
    return;
  }
  if (submission?.seq !== null && submission?.seq !== undefined) {
    if (history.kind === "pending") {
      status.textContent = "Saved in the cloud inbox. Waiting for your computer; this request is not applied yet.";
      status.classList.remove("warning-text");
    } else if (history.kind === "outside_recent_history") {
      status.textContent = "This request is outside the recent status history. Its result is unknown and it has not been resent. You can clear this browser's waiting status after checking your computer.";
      status.classList.add("warning-text");
    } else if (history.kind === "recorded") {
      status.textContent = "Your computer has this request. Waiting for its local result.";
      status.classList.remove("warning-text");
    } else {
      status.textContent = "The current snapshot does not show this request's local result. It has not been resent; wait for a fresh status update.";
      status.classList.add("warning-text");
    }
  } else if (submission?.rejected === true) {
    status.textContent = submission.error ?? "This request was rejected. It remains paused until browser storage is available.";
    status.classList.add("warning-text");
  } else if (submission) {
    status.textContent = "Saved in this browser. It will be sent when the connection is available; no destination has been changed yet.";
    status.classList.add("warning-text");
  } else if (!hasBoundLibrary) {
    status.textContent = "This library registration identity is unavailable. Refresh the page before adding a download.";
    status.classList.add("warning-text");
  } else {
    status.textContent = state.downloadMessage ?? "Paste a video or collection URL. The selected library and category are saved with this task.";
    status.classList.toggle("warning-text", Boolean(state.downloadMessage));
  }
}

function settleDownloadSubmission() {
  const submission = state.downloadSubmission;
  if (!submission || !Number.isSafeInteger(submission.seq)) return;
  const history = inspectDownloadSubmissionHistory(submission, state.snapshot);
  const operation = history.kind === "recorded" ? history.operation : null;
  if (!operation) return;
  if (operation.status === "completed") {
    const result = operation.result && typeof operation.result === "object" ? operation.result : {};
    const sameForm = byId("download-url")?.value === submission.url
      && byId("download-library-select")?.value === submission.library_id
      && byId("download-category")?.value === submission.category;
    state.downloadSubmission = null;
    state.downloadMessage = `Saved to ${String(result.save_path ?? libraryLabel(submission.library_id))}.`;
    if (sameForm) {
      byId("download-url").value = "";
      byId("download-category").value = "";
      closeDownloadForm();
    }
    const url = new URL(window.location.href);
    url.searchParams.delete("download");
    url.searchParams.delete("category");
    window.history.replaceState(null, "", url.toString());
    showToast(state.downloadMessage);
  } else if (["failed", "stale"].includes(String(operation.status))) {
    state.downloadSubmission = null;
    state.downloadMessage = String(operation.error ?? "The request failed on your computer. Review the error and try again.");
    showToast(state.downloadMessage);
  }
  renderDownloadStatus();
}

function activeLibraryOptions(selectedId = activeLibraryId()) {
  return mediaLibraryEntries(state.snapshot).map((entry) =>
    `<option value="${escapeHtml(entry.id)}" data-root-binding="${escapeHtml(entry.root_binding ?? "")}" ${entry.id === selectedId ? "selected" : ""} ${libraryCanOperate(entry.id) ? "" : "disabled"}>${escapeHtml(entry.name)}${entry.is_default === true ? " · default" : ""}${libraryCanOperate(entry.id) ? "" : " · offline"}</option>`,
  ).join("");
}

function updateDownloadFormOptions() {
  const librarySelect = byId("download-library-select");
  if (!librarySelect) return;
  const selectedId = librarySelect.value || activeLibraryId();
  librarySelect.innerHTML = activeLibraryOptions(selectedId);
  const resolved = resolveMediaLibrary(state.snapshot, selectedId);
  const folders = Array.isArray(resolved.snapshot?.categories) ? resolved.snapshot.categories : [];
  byId("download-categories").innerHTML = [...new Set(folders)]
    .filter((folder) => typeof folder === "string" && folder)
    .slice(0, 100)
    .map((folder) => `<option value="${escapeHtml(folder)}"></option>`)
    .join("");
  renderDownloadStatus();
}

function openDownloadForm({ updateUrl = true } = {}) {
  if (!state.ownerVerified) return;
  const dialog = byId("download-dialog");
  updateDownloadFormOptions();
  const currentParams = new URLSearchParams(window.location.search);
  const fromDownloadRoute = currentParams.get("download") === "1";
  const selectedId = fromDownloadRoute ? currentParams.get("library_id") || activeLibraryId() : activeLibraryId();
  byId("download-library-select").value = selectedId;
  updateDownloadFormOptions();
  const libraryDefaultCategory = mediaLibraryEntries(state.snapshot).find((entry) => entry.id === selectedId)?.default_category ?? "";
  const currentCategory = selectedId === activeLibraryId()
    ? byId("category-filter").value || libraryDefaultCategory
    : libraryDefaultCategory;
  const category = fromDownloadRoute && currentParams.has("category")
    ? currentParams.get("category")
    : currentCategory;
  byId("download-category").value = category;
  byId("download-category").dataset.userEdited = "false";
  renderDownloadStatus();
  if (updateUrl) {
    const url = new URL(window.location.href);
    url.searchParams.set("download", "1");
    url.searchParams.set("library_id", selectedId);
    if (category) url.searchParams.set("category", category);
    else url.searchParams.delete("category");
    window.history.replaceState(null, "", url.toString());
  }
  if (!dialog.open) dialog.showModal();
}

function closeDownloadForm() {
  const dialog = byId("download-dialog");
  if (dialog?.open) dialog.close();
}

function showToast(message) {
  const toast = byId("toast");
  toast.textContent = message;
  toast.classList.remove("hidden");
  clearTimeout(state.toastTimer);
  state.toastTimer = window.setTimeout(() => toast.classList.add("hidden"), 3600);
}

function setConnection(mode, label) {
  const dot = byId("connection-dot");
  dot.className = `connection-dot${mode === "live" ? " live" : mode === "stale" ? " stale" : ""}`;
  byId("connection-label").textContent = label;
}

function updateSnapshotUpdateStatus(status) {
  snapshotUpdateStatus = status;
  if (status === "expired") return;
  if (status === "offline") {
    setConnection("stale", "Offline · showing saved library");
    return;
  }
  if (status === "paused") {
    setConnection("", "Updates paused");
    return;
  }
  if (status === "fallback") {
    setConnection("", "Periodic refresh");
    return;
  }
  if (status === "connecting") {
    setConnection("", state.snapshot ? "Reconnecting for updates" : "Connecting for updates");
    return;
  }
  const synced = state.snapshot?.synced === true;
  const stale = state.snapshot?.stale === true;
  setConnection(synced && !stale ? "live" : stale ? "stale" : "", !synced ? "Waiting for computer" : stale ? "Contact is overdue" : "Computer connected");
}

function refreshLocalSnapshotFreshness({ renderContent = true } = {}) {
  const snapshot = state.snapshot;
  if (!snapshot) return;
  const now = Date.now();
  const freshnessTime = Number.isSafeInteger(snapshot.last_seen) ? snapshot.last_seen : snapshot.reported_at;
  const stale = snapshot.synced !== true
    || !Number.isSafeInteger(freshnessTime)
    || freshnessTime <= 0
    || freshnessTime > now
    || now - freshnessTime > RECENT_SNAPSHOT_MAX_AGE_MS;
  const changed = snapshot.stale !== stale;
  if (changed) state.snapshot = { ...snapshot, stale };
  if (mayUsePrivateSnapshot(state)) {
    if (changed && renderContent) render();
    else renderSync();
  }
  updateSnapshotUpdateStatus(snapshotUpdateStatus);
}

function clearCachedSnapshot() {
  try { localStorage.removeItem(CACHE_KEY); } catch { /* Private content remains hidden if storage is unavailable. */ }
}

function showPrivateAccessGate(message, { preserveRecoveryPreviewRequest = false } = {}) {
  state.detailId = null;
  clearSelectedItems();
  clearMetadataRecoveryState({ preservePreviewRequest: preserveRecoveryPreviewRequest });
  state.historyQuery = "";
  if (!preserveRecoveryPreviewRequest) {
    state.previewCancellations.clear();
    state.previewCancellationAttempts.clear();
  }
  state.legacyOperation = null;
  state.legacyOperationKey = null;
  state.legacySelection.clear();
  document.querySelector(".detail-panel")?.remove();
  document.querySelectorAll("#app-content .stack-list, #app-content .library-list, #metadata-recovery-results")
    .forEach((list) => { list.innerHTML = ""; });
  byId("history-summary").textContent = "";
  byId("history-search").value = "";
  for (const id of ["waiting-count", "organizing-count", "history-count", "library-count", "needs-count"]) {
    byId(id).textContent = "0";
  }
  byId("app-content").classList.add("hidden");
  byId("app-content").setAttribute("aria-busy", "false");
  byId("auth-gate").classList.remove("hidden");
  byId("auth-message").textContent = message;
  setConnection("", "Owner verification required");
}

function invalidateMetadataRecoveryForSnapshot(previousSnapshot, nextSnapshot) {
  const recovery = state.metadataRecovery;
  const previewRequest = state.metadataRecoveryPreviewRequest;
  if (!previousSnapshot) return false;
  const entryFor = (snapshot, libraryId) => mediaLibraryEntries(snapshot).find((entry) => entry.id === libraryId);
  const bindingChanged = (libraryId) => {
    const previousEntry = entryFor(previousSnapshot, libraryId);
    const nextEntry = entryFor(nextSnapshot, libraryId);
    return previousEntry?.root_binding !== nextEntry?.root_binding;
  };
  const recoveryBindingChanged = Boolean(recovery && bindingChanged(recovery.libraryId));
  const recoveryRevisionChanged = Boolean(recovery
    && resolveMediaLibrary(previousSnapshot, recovery.libraryId).snapshot?.revision
      !== resolveMediaLibrary(nextSnapshot, recovery.libraryId).snapshot?.revision);
  const previewBindingChanged = Boolean(previewRequest
    && entryFor(nextSnapshot, previewRequest.libraryId)?.root_binding !== previewRequest.rootBinding);

  if (recoveryBindingChanged || recoveryRevisionChanged) {
    clearMetadataRecoveryReport({ dismiss: false });
  }
  if (previewBindingChanged && state.metadataRecoveryPreviewRequest === previewRequest) {
    setMetadataRecoveryPreviewRequest(null);
  }
  return recoveryBindingChanged || recoveryRevisionChanged || previewBindingChanged;
}

function acceptFreshSnapshot(snapshot) {
  const identityChanged = clearFileIdentityScopedStateOnSnapshot(state, state.snapshot, snapshot);
  const recoveryChanged = invalidateMetadataRecoveryForSnapshot(state.snapshot, snapshot);
  if (identityChanged || recoveryChanged) {
    state.groupSelectionLimitNotice = null;
  }
  Object.assign(state, acceptOwnerSnapshot(snapshot, state));
  restoreMetadataRecoveryPreviewRequest(state.snapshot);
  reconcileMetadataRecoveryPreviewRequest(state.snapshot);
  settleMigrationConflictRefreshes();
  normalizeLibraryContext();
  byId("auth-gate").classList.add("hidden");
  byId("app-content").classList.remove("hidden");
  restorePreviewCancellations();
  reconcilePreviewCancellations(state.snapshot);
  recoverFileActionReceipts();
  const params = new URLSearchParams(window.location.search);
  if (params.get("download") === "1" && !state.downloadFormOpened) {
    state.downloadFormOpened = true;
    openDownloadForm({ updateUrl: false });
  }
}

function applySnapshotFailure(status) {
  const transition = failOwnerSnapshot(state, status, Date.now());
  Object.assign(state, transition);
  if (transition.clearCache) clearCachedSnapshot();
  if (transition.authorizationRejected) {
    state.refreshAfterDrain = false;
    if (state.retryTimer) window.clearTimeout(state.retryTimer);
    state.retryTimer = 0;
  }
  return transition;
}

function handleAuthorizationFailure(error) {
  privateAccessExpired = true;
  snapshotUpdates?.stop();
  applySnapshotFailure(error?.status);
  showPrivateAccessGate("Owner access could not be verified. Reopen this Mini App from the owner's private chat.");
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers ?? {});
  headers.set("X-Telegram-Init-Data", initData);
  if (options.body !== undefined) headers.set("Content-Type", "application/json");
  const response = await fetch(`${API}${path}`, { ...options, headers, cache: "no-store" });
  let data;
  try { data = await response.json(); } catch { data = {}; }
  if (!response.ok) throw new HttpResponseError(response.status, typeof data?.error === "string" ? data.error : `Request failed (${response.status})`);
  return data;
}

async function performRefreshState({ quiet = false } = {}) {
  if (!initData || privateAccessExpired || state.loading) return;
  recoverFileActionReceipts();
  state.loading = true;
  byId("app-content").setAttribute("aria-busy", "true");
  try {
    const fresh = await loadOwnerSnapshot((path, headers) => api(path, { headers }));
    if (privateAccessExpired) return;
    acceptFreshSnapshot(fresh);
    refreshLocalSnapshotFreshness({ renderContent: false });
    render();
    updateSnapshotUpdateStatus(snapshotUpdateStatus);
    await drainActionsAfterSnapshotRefresh(
      state,
      navigator.onLine,
      () => drainActionQueue({ quiet }),
    );
  } catch (error) {
    const transition = applySnapshotFailure(error?.status);
    if (transition.authorizationRejected) {
      privateAccessExpired = true;
      snapshotUpdates?.stop();
    }
    if (mayUsePrivateSnapshot(state)) {
      setConnection("stale", "Showing saved library");
      render();
    } else {
      showPrivateAccessGate(transition.authorizationRejected
        ? "Owner access could not be verified. Reopen this Mini App from the owner's private chat."
        : "The private library stays hidden until its owner is verified. Check the connection and retry.", {
        preserveRecoveryPreviewRequest: !transition.authorizationRejected,
      });
    }
    if (!transition.authorizationRejected) updateSnapshotUpdateStatus(snapshotUpdateStatus);
    if (!quiet && navigator.onLine && mayUsePrivateSnapshot(state)) showToast(error.message);
    throw error;
  } finally {
    state.loading = false;
    byId("app-content").setAttribute("aria-busy", "false");
    if (state.refreshAfterDrain) {
      state.refreshAfterDrain = false;
      window.setTimeout(() => refreshState({ quiet: true }), 0);
    }
  }
}

function refreshState(options = {}) {
  const refresh = snapshotUpdates ? snapshotUpdates.refresh(options) : performRefreshState(options);
  // The transport observes callback failures for retry; UI callers rely on the toast above.
  return refresh.catch(() => {});
}

function readActionQueue() {
  try {
    const value = JSON.parse(localStorage.getItem(ACTION_QUEUE_KEY) ?? "[]");
    return Array.isArray(value) ? value.filter((entry) => entry && typeof entry.request_id === "string" && typeof entry.kind === "string" && entry.payload && typeof entry.payload === "object") : [];
  } catch { return []; }
}

function recoverFileActionReceipts() {
  const recovered = recoverActionReceipts(state, { surface: ACTION_SURFACE_FILE });
  if (recovered.matched > 0) {
    persistPreviewCancellations();
    const matchedIds = new Set(recovered.requestIds);
    const queue = readActionQueue();
    const remaining = queue.filter((action) => !matchedIds.has(action.request_id));
    const queueUpdated = remaining.length === queue.length || writeActionQueue(remaining);
    if (recovered.rejectedRequestIds.length > 0) {
      if (queueUpdated) {
        consumeRejectedActionReceipts(state, { surface: ACTION_SURFACE_FILE, requestIds: recovered.rejectedRequestIds });
      } else {
        for (const requestId of recovered.rejectedRequestIds) state.quarantinedActionIds.add(requestId);
      }
      for (const rejection of recovered.rejections) settleRejectedFileAction(rejection, queueUpdated);
    }
    if (recovered.downloadSubmission) renderDownloadStatus();
    if (recovered.migrationConflictDrafts > 0) renderMigrations();
    if (recovered.matched > 0) {
      renderPreviewReviewCards();
      renderTabCounts();
    }
  }
  return recovered;
}

function settleRejectedFileAction(rejection, queueRemoved) {
  if (!rejection || typeof rejection.request_id !== "string") return;
  if (queueRemoved) state.quarantinedActionIds.delete(rejection.request_id);
  const message = typeof rejection.error === "string" ? rejection.error : "The request was rejected.";
  if (rejection.kind === "download_submit" && state.downloadSubmission?.request_id === rejection.request_id) {
    if (queueRemoved) {
      state.downloadSubmission = null;
      state.downloadMessage = message;
    } else {
      state.downloadSubmission.rejected = true;
      state.downloadSubmission.error = message;
    }
    renderDownloadStatus();
  }
  if (rejection.kind === "library_migration_preview") {
    for (const draft of state.migrationConflictDrafts.values()) {
      if (draft.rejectedRefreshRequestId !== rejection.request_id) continue;
      draft.refreshError = message;
      if (queueRemoved) draft.rejectedRefreshRequestId = null;
    }
    renderMigrations();
  }
  if (rejection.kind === "preview_cancel") {
    for (const tracker of state.previewCancellations.values()) {
      if (tracker.requestId !== rejection.request_id) continue;
      tracker.status = "failed";
      tracker.error = message;
    }
    persistPreviewCancellations();
    renderPreviewReviewCards();
  }
}

function writeActionQueue(actions) {
  if (actions.length > 20) return false;
  try { localStorage.setItem(ACTION_QUEUE_KEY, JSON.stringify(actions)); } catch { showToast("This browser could not save the action queue."); return false; }
  renderQueueCount();
  return true;
}

function renderQueueCount() {
  const count = readActionQueue().filter((action) => !state.quarantinedActionIds.has(action.request_id)
    && !isUnknownDeliveryAction(action, state.unknownActionOutcomes)).length;
  byId("queued-actions").textContent = count ? `${count} action${count === 1 ? "" : "s"} waiting to send` : "";
}

async function drainActionQueue({ quiet = false } = {}) {
  if (!initData || !mayDispatchActions(state) || !navigator.onLine) return false;
  if (state.draining) return state.draining;
  recoverFileActionReceipts();
  for (const action of readActionQueue()) {
    if (action.delivery_outcome === "outcome_unknown") {
      markUnknownFileAction(state, action);
      if (action.kind === "preview_cancel") persistPreviewCancellations();
    }
  }
  const restored = restorePendingActions(state, {
    surface: ACTION_SURFACE_FILE,
    readQueue: readActionQueue,
    writeQueue: writeActionQueue,
  });
  if ((restored.writeFailed || restored.skippedAtLimit > 0) && !quiet) {
    showToast("A saved request could not be restored to the browser queue yet. It will retry when queue space is available.");
  }
  state.draining = (async () => {
    let sentAny = false;
    let connectionInterrupted = false;
    let removalFailed = false;
    const permanentRejections = [];
    while (navigator.onLine) {
      const outcome = await dispatchNextOwnedAction({
        surface: ACTION_SURFACE_FILE,
        readQueue: readActionQueue,
        writeQueue: writeActionQueue,
        excludedIds: state.quarantinedActionIds,
        dispatch: (action) => api("/actions", {
          method: "POST",
        body: JSON.stringify(fileActionRequestBody(action)),
        }),
        onReceipt: (action, receipt, resolvedSurface) => {
          const recorded = recordActionReceipt(action, receipt, state, { surface: resolvedSurface });
          if (action?.kind === "preview_cancel") persistPreviewCancellations();
          if (action?.kind === "file_preview" && action.request_id === state.metadataRecoveryPreviewRequest?.requestId) {
            updateMetadataRecoveryPreviewRequest(action, {
              status: receipt?.outcome === "rejected" ? "rejected" : "submitted",
              error: receipt?.error,
              seq: receipt?.seq,
            });
          }
          if (action?.kind === "metadata_recover" && action.request_id === state.metadataRecovery?.requestId
              && Number.isSafeInteger(receipt?.seq)) {
            state.metadataRecovery.seq = receipt.seq;
            renderMetadataRecovery();
          }
          if (action?.kind === "preview_cancel") renderPreviewReviewCards();
          return recorded;
        },
      });
      const receiptUpdate = outcome.receiptUpdate ?? {};
      if (receiptUpdate.downloadSubmission) renderDownloadStatus();
      if (receiptUpdate.migrationConflictDrafts > 0) renderMigrations();
      if (outcome.action?.kind === "preview_cancel") renderPreviewReviewCards();
      if (outcome.status === "empty") break;
      if (outcome.status === "remove_failed") {
        if (!quiet) showToast("Request reached the computer, but this browser could not update its retry queue.");
        break;
      }
      if (outcome.status === "receipt_failed") {
        connectionInterrupted = true;
        if (!quiet) showToast("The computer accepted the request, but this browser could not save its receipt. The same request ID will retry.");
        if (!state.retryTimer) {
          const delay = state.retryDelay;
          state.retryDelay = Math.min(state.retryDelay * 2, 30_000);
          state.retryTimer = window.setTimeout(() => {
            state.retryTimer = 0;
            drainActionQueue({ quiet });
          }, delay);
        }
        break;
      }
      if (outcome.status === "rejection_receipt_failed") {
        connectionInterrupted = true;
        if (!quiet) showToast("The rejection could not be saved in this browser. The same request remains queued and will retry.");
        if (!state.retryTimer) {
          const delay = state.retryDelay;
          state.retryDelay = Math.min(state.retryDelay * 2, 30_000);
          state.retryTimer = window.setTimeout(() => {
            state.retryTimer = 0;
            drainActionQueue({ quiet });
          }, delay);
        }
        break;
      }
      if (outcome.status === "rejection_remove_failed") {
        const { action, rejection } = outcome;
        state.quarantinedActionIds.add(action.request_id);
        settleRejectedFileAction({ request_id: action.request_id, kind: action.kind, ...rejection }, false);
        if (!quiet) showToast("The rejection is saved, but this browser could not remove the rejected action yet. It will stay paused here until storage is available.");
        removalFailed = true;
        break;
      }
      if (outcome.status === "outcome_unknown") {
        const { action, code, error } = outcome;
        const marked = action?.delivery_outcome === "outcome_unknown"
          ? action
          : { ...action, delivery_outcome: "outcome_unknown", delivery_error_code: code, delivery_error: error };
        const updates = markUnknownFileAction(state, marked);
        if (action?.kind === "preview_cancel") persistPreviewCancellations();
        if (action?.kind === "metadata_recover" && state.metadataRecovery?.requestId === action.request_id) {
          state.metadataRecovery.deliveryUnknown = true;
          renderMetadataRecovery();
        }
        updateMetadataRecoveryPreviewRequest(action, { status: "unknown", error });
        if (action?.kind === "preview_cancel") renderPreviewReviewCards();
        if (updates.downloadSubmission) renderDownloadStatus();
        if (updates.migrationConflictDrafts > 0) renderMigrations();
        renderWaiting();
        if (!quiet) showToast("Delivery outcome is unknown. The request will not be sent again automatically; check your computer before dismissing the notice.");
        continue;
      }
      if (outcome.status === "rejected") {
        const { action, rejection, receiptUpdate } = outcome;
        if (receiptUpdate?.rejectedRequestIds?.includes(action.request_id)) {
          settleRejectedFileAction({ request_id: action.request_id, kind: action.kind, ...rejection }, true);
        }
        if (action.kind === "metadata_recover" && state.metadataRecovery?.requestId === action.request_id) {
          state.metadataRecovery.error = rejection.error;
          renderMetadataRecovery();
        }
        updateMetadataRecoveryPreviewRequest(action, { status: "rejected", error: rejection.error });
        if (action?.kind === "preview_cancel") renderPreviewReviewCards();
        permanentRejections.push(rejection.error);
        continue;
      }
      if (outcome.status === "sent") {
        sentAny = true;
        continue;
      }

      const { action, error } = outcome;
      if (error?.status === 401 || error?.status === 403) {
        handleAuthorizationFailure(error);
        break;
      }
      if (!shouldRetryActionFailure(error)) {
        state.quarantinedActionIds.add(action.request_id);
        const latestQueue = readActionQueue();
        if (!writeActionQueue(removeQueuedAction(latestQueue, action.request_id))) removalFailed = true;
        if (state.downloadSubmission?.request_id === action.request_id) {
          state.downloadSubmission = null;
          state.downloadMessage = typeof error?.message === "string" ? error.message : "The request was rejected before it reached your computer.";
          renderDownloadStatus();
        }
        updateMetadataRecoveryPreviewRequest(action, {
          status: "rejected",
          error: typeof error?.message === "string" ? error.message : "The preview request was rejected.",
        });
        if (action?.kind === "preview_cancel") {
          for (const tracker of state.previewCancellations.values()) {
            if (tracker.requestId !== action.request_id) continue;
            tracker.status = "failed";
            tracker.error = typeof error?.message === "string" ? error.message : "The computer rejected the cancellation request.";
          }
          persistPreviewCancellations();
          renderPreviewReviewCards();
        }
        for (const [previewId, draft] of state.migrationConflictDrafts) {
          if (draft.refreshRequestId === action.request_id) {
            draft.refreshRequestId = null;
            draft.refreshSeq = null;
            draft.refreshError = typeof error?.message === "string" ? error.message : "The refreshed preview request was rejected.";
            state.migrationConflictDrafts.set(previewId, draft);
          }
        }
        renderMigrations();
        permanentRejections.push(typeof error?.message === "string" ? error.message : "Request rejected");
        continue;
      }
      if (!state.retryTimer) {
        const delay = state.retryDelay;
        state.retryDelay = Math.min(state.retryDelay * 2, 30_000);
        state.retryTimer = window.setTimeout(() => {
          state.retryTimer = 0;
          drainActionQueue({ quiet });
        }, delay);
      }
      connectionInterrupted = true;
      break;
    }
    if (sentAny) state.retryDelay = 1500;
    if (permanentRejections.length) {
      const count = permanentRejections.length;
      const details = [...new Set(permanentRejections)].slice(0, 2).join(" · ");
      const remainder = connectionInterrupted ? " Other saved requests remain queued while the connection recovers." : "";
      const storageNote = removalFailed ? " One rejected request could not be removed from browser storage and will stay quarantined for this session." : "";
      showToast(`${count} rejected request${count === 1 ? " was" : "s were"} removed from the retry queue: ${details}.${remainder}${storageNote}`);
    } else if (!quiet && connectionInterrupted) {
      showToast("Connection interrupted. The request remains queued with the same ID.");
    }
    return sentAny;
  })();
  let sentAny = false;
  try { sentAny = await state.draining; }
  finally { state.draining = null; }
  if (sentAny && state.ownerVerified) {
    if (state.loading) state.refreshAfterDrain = true;
    else await refreshState({ quiet: true });
  }
  return sentAny;
}

async function submitAction(kind, payload, { onQueued, requestId: suppliedRequestId } = {}) {
  if (!mayDispatchActions(state)) return false;
  if (kind === "download_submit" && unknownFileActionForKind(readActionQueue(), kind, state.unknownActionOutcomes)) {
    state.downloadMessage = "A previous download has an unknown delivery outcome. Check your computer and dismiss its notice before adding another request.";
    renderDownloadStatus();
    return false;
  }
  if (["file_preview", "legacy_import", "metadata_recover"].includes(kind)) {
    const libraryId = String(payload?.library_id ?? "");
    if (!fileOperationIdentityMatches(state.snapshot, { result: payload }, libraryId)) {
      showToast("This action does not match a verified library registration. Refresh the library and create a new preview or import.");
      return false;
    }
  }
  if (kind === "preview_cancel" && !reviewTargetForCancellation(payload)) {
    showToast("This preview is no longer awaiting review. Refresh the Requests view before cancelling it.");
    return false;
  }
  const unavailableLibraryAction = (() => {
    if (["file_preview", "legacy_import", "metadata_recover"].includes(kind)) {
      return !libraryCanOperate(String(payload?.library_id ?? ""));
    }
    if (kind === "download_submit") return !libraryCanOperate(String(payload?.library_id ?? ""));
    if (kind === "library_migration_preview") {
      return !libraryCanOperate(String(payload?.source_library_id ?? ""))
        || !libraryCanOperate(String(payload?.target_library_id ?? ""));
    }
    if (kind === "file_confirm") {
      const preview = previews().find((entry) => entry.id === payload?.preview_id);
      const libraryId = String(payload?.library_id ?? "");
      return !preview || !fileOperationIdentityMatches(state.snapshot, preview, libraryId)
        || !libraryCanOperate(libraryId);
    }
    if (kind === "library_migration_confirm") {
      const preview = migrationPreviewEntries().find((entry) => entry.id === payload?.preview_id);
      const result = preview?.result ?? {};
      const sourceId = migrationLibraryId(preview, result, "source_library_id");
      const targetId = migrationLibraryId(preview, result, "target_library_id");
      const endpoints = migrationEndpointAvailability(state.snapshot, sourceId, targetId);
      const bindingsMatch = migrationPreviewBindingsMatch(
        state.snapshot,
        sourceId,
        targetId,
        result.source_root_binding,
        result.target_root_binding,
      );
      return !endpoints.sourceAvailable || !endpoints.targetAvailable || !bindingsMatch;
    }
    return false;
  })();
  if (unavailableLibraryAction) {
    showToast("This library is offline or showing a saved scan. Reconnect it and refresh before changing files.");
    return false;
  }
  if (kind === "file_preview") {
    for (const field of ["item_ids", "metadata_patches"]) {
      if (Array.isArray(payload?.[field]) && payload[field].length > MAX_SELECTED_ITEMS) {
        showToast(`Choose at most ${MAX_SELECTED_ITEMS} ${field === "item_ids" ? "videos" : "metadata matches"} per request.`);
        return false;
      }
    }
  }
  const action = createFileQueuedAction(kind, payload, suppliedRequestId ?? crypto.randomUUID());
  const queued = await persistQueuedActionAndDrain({
    action,
    readQueue: readActionQueue,
    writeQueue: writeActionQueue,
    onQueued: (queuedAction) => {
      onQueued?.(queuedAction);
      if (["file_preview", "library_migration_preview"].includes(kind) && state.view !== "waiting") {
        state.view = "waiting";
        render();
      }
    },
    afterPersist: () => {
      if (kind !== "download_submit") return;
      state.downloadSubmission = {
        request_id: action.request_id,
        seq: null,
        url: String(payload.url ?? ""),
        library_id: String(payload.library_id ?? ""),
        category: String(payload.category ?? ""),
        payload: { ...payload },
        queued_at_ms: action.queued_at_ms,
        replay_expires_at_ms: action.replay_expires_at_ms,
      };
      renderDownloadStatus();
    },
    isOnline: () => navigator.onLine,
    onOffline: () => showToast("Saved offline. It will send when this device reconnects."),
    onSending: () => showToast("Sending the saved request"),
    drain: drainActionQueue,
  });
  if (queued.status === "queue_full") {
    showToast("The offline queue is full. Reconnect and send its saved requests first.");
    return false;
  }
  if (queued.status === "storage_failed") {
    showToast("This browser could not save the action, so it was not sent.");
    return false;
  }
  return true;
}

function updateSelectedItems(candidateIds, selected) {
  const result = updateSelectionWithinLimit(state.selected, candidateIds, selected);
  state.selected.clear();
  for (const id of result.ids) state.selected.add(id);
  state.groupSelectionLimitNotice = null;
  return result.omittedCount;
}

function clearSelectedItems() {
  state.selected.clear();
  state.groupSelectionLimitNotice = null;
}

function selectionLimitMessage(omittedCount = 0) {
  return omittedCount > 0
    ? `Only ${MAX_SELECTED_ITEMS} items can be selected. ${omittedCount} more were left unselected; deselect an item to add another.`
    : `Selection limit: ${MAX_SELECTED_ITEMS}. Deselect an item to select another.`;
}

function allFolders() {
  const library = activeLibrarySnapshot();
  if (library === cachedFolderLibrary) return cachedFolders;
  const folders = new Set(Array.isArray(library?.categories) ? library.categories : []);
  for (const item of libraryItems()) if (itemFolder(item)) folders.add(itemFolder(item));
  cachedFolderLibrary = library;
  cachedFolders = [...folders].filter((folder) => typeof folder === "string").sort((a, b) => a.localeCompare(b));
  return cachedFolders;
}

function filteredItems() {
  return filterLibraryItems(libraryItems(), {
    query: state.query,
    category: byId("category-filter").value,
    status: byId("status-filter").value,
  });
}

function renderFilters() {
  const folders = allFolders();
  const categorySuggestions = boundedFolderSuggestions(folders, byId("category-filter").value);
  const destinationSuggestions = boundedFolderSuggestions(folders, byId("target-dir").value);
  byId("category-folders").innerHTML = categorySuggestions.map((folder) => `<option value="${escapeHtml(folder)}"></option>`).join("");
  byId("known-folders").innerHTML = destinationSuggestions.map((folder) => `<option value="${escapeHtml(folder)}"></option>`).join("");
}

function itemRow(item, selectionEnabled = true, scope = "library") {
  const id = String(item.id ?? "");
  const selected = state.selected.has(id);
  const presence = statusOf(item);
  const movedTarget = movedToTarget(item);
  const displayStatus = movedTarget ? "Moved" : presence === "present" ? "Available" : presence === "missing" ? "Missing" : presence === "unavailable" ? "Unavailable" : "Status unknown";
  const rowSelectable = isPresentMedia(item) && selectionEnabled;
  const selectionReason = rowSelectable ? "" : mediaSelectionReason(item, selectionEnabled, movedTarget);
  const reasonId = `selection-reason-${scope}-${accessibleToken(id)}`;
  const movedTargetRegistered = movedTarget
    && mediaLibraryEntries(state.snapshot).some((entry) => entry.id === movedTarget.library_id);
  const movedTargetMarkup = movedTarget ? `<div class="moved-target"><span class="moved-target-label">Moved to ${escapeHtml(movedTarget.library_name)}</span><span class="moved-target-note">Source entry stays here as history.</span>${movedTargetRegistered
    ? `<button class="small-button moved-target-action" type="button" aria-label="Open ${escapeHtml(movedTarget.library_name)} item ${escapeHtml(movedTarget.item_id)}" data-open-moved-target data-source-library-id="${escapeHtml(activeLibraryId())}" data-source-item-id="${escapeHtml(id)}" data-target-library-id="${escapeHtml(movedTarget.library_id)}" data-target-item-id="${escapeHtml(movedTarget.item_id)}" data-target-relative-path="${escapeHtml(movedTarget.relative_path)}">Open target item</button>`
    : `<span class="moved-target-unavailable">Destination library is no longer registered.</span>`}</div>` : "";
  const dims = item.width && item.height ? `${escapeHtml(item.width)} × ${escapeHtml(item.height)}` : "";
  const codec = item.codec ? escapeHtml(item.codec) : "";
  return `<article class="library-row${selected ? " selected" : ""}" data-item-row="${escapeHtml(id)}">
    <input class="row-check" type="checkbox" aria-label="Select ${escapeHtml(item.title ?? item.relative_path)}" ${selectionReason ? `aria-describedby="${reasonId}"` : ""} data-select-item="${escapeHtml(id)}" ${selected ? "checked" : ""} ${rowSelectable ? "" : "disabled"}>
    <div class="media-icon" aria-hidden="true">▣</div>
    <div class="media-main"><div class="media-title"><span>${escapeHtml(item.title || item.relative_path || "Untitled video")}</span></div><div class="media-path">${escapeHtml(item.relative_path || "Path unavailable")}</div>${movedTargetMarkup}${selectionReason ? `<p class="media-selection-reason${!selectionEnabled && isPresentMedia(item) ? " readonly" : ""}" id="${reasonId}">${escapeHtml(selectionReason)}</p>` : ""}</div>
    <div class="media-actions"><div class="media-meta"><span><i class="status-dot ${escapeHtml(movedTarget ? "moved" : presence)}"></i>${displayStatus}</span><span>${formatBytes(item.bytes)}</span>${dims ? `<span>${dims}</span>` : ""}${codec ? `<span>${codec}</span>` : ""}</div><button class="details-button media-details" type="button" data-details="${escapeHtml(id)}">Details</button></div>
  </article>`;
}

function libraryPageMarkup(page, scope) {
  if (page.pageCount <= 1) return "";
  const previous = page.page > 0
    ? `<button class="quiet-button" type="button" data-library-page="${scope}" data-page-direction="-1">Previous</button>`
    : `<button class="quiet-button" type="button" disabled>Previous</button>`;
  const next = page.page + 1 < page.pageCount
    ? `<button class="quiet-button" type="button" data-library-page="${scope}" data-page-direction="1">Next</button>`
    : `<button class="quiet-button" type="button" disabled>Next</button>`;
  const label = scope === "needs" ? "Needs organizing pages" : "Library pages";
  return `<nav class="toolbar library-pagination" aria-label="${label}">${previous}<span class="field-hint">Page ${page.page + 1} of ${page.pageCount} · items ${page.start + 1}–${page.end} of ${page.total.toLocaleString()}</span>${next}</nav>`;
}

function renderGroupedItems(container, items, page, scope, selectionEnabled = true) {
  const groups = groupLibraryItems(items);
  const renderedGroups = groups.map(({ title, items: entries }, index) => {
    const selectableEntries = presentMediaItems(entries);
    const groupIds = selectableEntries.map((item) => String(item.id));
    const groupKey = collectionIdentity(entries[0]?.collection);
    const groupCounts = librarySelectionCounts(entries, state.selected, selectionEnabled);
    const groupCountId = `group-selection-count-${scope}-${accessibleToken(groupKey)}`;
    const allSelected = groupIds.length > 0 && groupIds.every((id) => state.selected.has(id));
    const someSelected = groupIds.some((id) => state.selected.has(id));
    const capNotice = state.groupSelectionLimitNotice?.scope === scope
      && state.groupSelectionLimitNotice?.page === page.page
      && state.groupSelectionLimitNotice?.groupKey === groupKey
      ? `<span class="group-selection-cap-note" aria-live="polite">${state.groupSelectionLimitNotice.omittedCount} additional present item${state.groupSelectionLimitNotice.omittedCount === 1 ? "" : "s"} skipped by the ${MAX_SELECTED_ITEMS}-item selection limit. Deselect selected items to add more.</span>`
      : "";
    return `<details class="collection-group" ${index < 3 ? "open" : ""}>
      <summary><input class="group-check" type="checkbox" aria-label="Select available ${escapeHtml(title)} items on this page" aria-describedby="${groupCountId}" data-select-group="${escapeHtml(groupKey)}" ${allSelected ? "checked" : ""} ${someSelected && !allSelected ? "data-partial=true" : ""} ${groupCounts.selectableCount ? "" : "disabled"}><span>${escapeHtml(title)}</span><span class="group-count group-selection-count" id="${groupCountId}" aria-live="polite">${groupCounts.selectedCount} selected · ${groupCounts.selectableCount} selectable · ${groupCounts.skippedUnavailableCount} skipped (not present)${groupCounts.readOnlyCount ? ` · ${groupCounts.readOnlyCount} present but read-only` : ""} on this page</span>${capNotice}</summary>
      <div class="collection-items">${entries.map((item) => itemRow(item, selectionEnabled, scope)).join("")}</div>
    </details>`;
  }).join("");
  container.innerHTML = `${renderedGroups}${libraryPageMarkup(page, scope)}`;
  container.querySelectorAll("[data-partial]").forEach((checkbox) => { checkbox.indeterminate = true; });
}

function pageItemsForScope(scope) {
  const filtered = filteredItems();
  const scopedItems = scope === "needs" ? filtered.filter(needsOrganizing) : filtered;
  const pageKey = scope === "needs" ? "needsPage" : "libraryPage";
  const page = paginateLibraryItems(scopedItems, state[pageKey]);
  state[pageKey] = page.page;
  return page.items;
}

function renderLibrary() {
  renderLibrarySelector();
  const library = activeLibrarySnapshot();
  const canOperate = libraryCanOperate();
  const availableIds = new Set(presentMediaItems(libraryItems()).map((item) => String(item.id)));
  let prunedSelection = false;
  for (const id of state.selected) {
    if (availableIds.has(id)) continue;
    state.selected.delete(id);
    prunedSelection = true;
  }
  if (prunedSelection) state.groupSelectionLimitNotice = null;
  renderFilters();
  const items = filteredItems();
  const needsItems = items.filter(needsOrganizing);
  const libraryPage = paginateLibraryItems(items, state.libraryPage);
  const needsPage = paginateLibraryItems(needsItems, state.needsPage);
  state.libraryPage = libraryPage.page;
  state.needsPage = needsPage.page;
  const list = byId("library-list");
  renderGroupedItems(list, libraryPage.items, libraryPage, "library", canOperate);
  renderGroupedItems(byId("needs-list"), needsPage.items, needsPage, "needs", canOperate);
  const synced = state.snapshot?.synced === true;
  const mediaInfo = mediaSnapshotInfo(activeLibrary().descriptor, library);
  const omissionNotice = mediaSnapshotOmissionNotice(activeLibrary().descriptor, library);
  const firstScanPending = synced && (library?.ready === false || Number(library?.scanned_at) === 0);
  const retainedRowsMissing = mediaInfo.omittedCount > 0 && mediaInfo.retainedCount === 0;
  const localOffline = mediaLibraryAvailability(activeLibrary().descriptor) === "offline";
  const savedLibraryView = activeLibrary().descriptor?.snapshot_stale === true;
  byId("library-empty").classList.toggle("hidden", !synced || libraryItems().length > 0 && !firstScanPending);
  list.classList.toggle("hidden", !synced || !library || firstScanPending || libraryItems().length === 0);
  const emptyHeading = byId("library-empty").querySelector("h2");
  const emptyCopy = byId("library-empty").querySelector("p");
  if (firstScanPending) {
    emptyHeading.textContent = "Waiting for the first scan";
    emptyCopy.textContent = "Your computer is connected. The library will appear when its first scan completes.";
  } else if (localOffline) {
    emptyHeading.textContent = "This media root is offline";
    emptyCopy.textContent = "The last saved view remains available when one was synced. Reconnect the desktop drive and refresh to scan it again.";
  } else if (!library) {
    emptyHeading.textContent = "No saved view for this library";
    emptyCopy.textContent = "This registered root has not reported a library snapshot yet.";
  } else if (retainedRowsMissing) {
    emptyHeading.textContent = "The cloud list has no retained media rows";
    emptyCopy.textContent = `The full scan reports ${mediaInfo.totalCount.toLocaleString()} media items, but no item details fit in this cloud view. Open the desktop app to browse the complete list.`;
  } else {
    emptyHeading.textContent = "No media found yet";
    emptyCopy.textContent = "Run a scan on the connected computer to discover videos and their sidecar files.";
  }
  if (!synced) {
    list.innerHTML = "";
    byId("library-empty").classList.add("hidden");
  }
  const total = mediaInfo.totalCount;
  byId("library-count").textContent = synced && !firstScanPending ? String(total) : "—";
  byId("library-summary").textContent = firstScanPending
    ? "Your computer is connected, but the first scan has not completed."
    : state.snapshot?.synced
    ? `${savedLibraryView ? "Showing the last saved scan; cached items are read-only · " : ""}${items.length.toLocaleString()} matching retained item${items.length === 1 ? "" : "s"} · ${items.length ? `showing ${libraryPage.start + 1}–${libraryPage.end} on page ${libraryPage.page + 1} of ${Math.max(1, libraryPage.pageCount)} filtered retained rows` : "no matches"} · ${mediaInfo.retainedCount.toLocaleString()} retained in cloud list · ${total.toLocaleString()} in library${omissionNotice ? ` · ${omissionNotice}` : ""}${state.selected.size ? ` · ${state.selected.size} selected${state.selected.size >= MAX_SELECTED_ITEMS ? ` · selection limit ${MAX_SELECTED_ITEMS}; deselect to add more` : ""}` : ""}`
    : "A first scan has not completed yet.";
  const needsTotal = libraryItems().filter(needsOrganizing).length;
  byId("needs-count").textContent = synced && !firstScanPending ? String(needsTotal) : "—";
  byId("needs-summary").textContent = firstScanPending
    ? "Needs organizing will appear after the first scan completes."
    : state.snapshot?.synced
    ? `${needsItems.length.toLocaleString()} matching retained item${needsItems.length === 1 ? "" : "s"} · ${needsItems.length ? `showing ${needsPage.start + 1}–${needsPage.end} on page ${needsPage.page + 1} of ${Math.max(1, needsPage.pageCount)} filtered retained rows` : "no matches"} · ${needsTotal.toLocaleString()} retained rows need attention${omissionNotice ? ` · ${omissionNotice}` : ""}`
    : "Needs organizing will appear after the first scan.";
  byId("needs-empty").classList.toggle("hidden", !synced || firstScanPending || needsItems.length > 0);
  byId("needs-list").classList.toggle("hidden", !synced || firstScanPending || needsItems.length === 0);
  byId("download-here").disabled = !canOperate;
  byId("select-visible").disabled = !canOperate;
  byId("target-dir").disabled = !canOperate;
  byId("root-target").disabled = !canOperate;
  byId("rename-toggle").disabled = !canOperate;
  byId("conflict-select").disabled = !canOperate;
  byId("preview-move").disabled = !canOperate || state.selected.size === 0;
  byId("import-legacy").disabled = !canOperate;
  const selectionActions = byId("selection-actions");
  selectionActions.classList.toggle("hidden", state.selected.size === 0 || ["waiting", "history"].includes(state.view));
  byId("selection-count").textContent = `${state.selected.size} ${state.selected.size === 1 ? "item" : "items"} selected`;
  byId("organize-selected").disabled = !canOperate || state.selected.size === 0;
  byId("recover-selected").disabled = !canOperate || state.selected.size === 0 || state.selected.size > MAX_SELECTED_ITEMS;
  renderMetadataRecovery();
}

const RECOVERY_GROUP_TITLES = {
  ready: "Ready to preview",
  needs_confirmation: "Choose a match",
  deferred: "Deferred · analyze again",
  unchanged: "Already complete",
  unavailable: "Unavailable",
  unsupported: "Unsupported",
};

function recoveryPatchSummary(patch) {
  const parts = [];
  if (typeof patch?.title === "string") parts.push(`Title: ${patch.title}`);
  if (patch?.collection && typeof patch.collection === "object") parts.push(`Collection: ${patch.collection.title}`);
  if (Array.isArray(patch?.source_ids) && patch.source_ids.length) parts.push(`Source IDs: ${patch.source_ids.join(", ")}`);
  if (patch?.organization_status) parts.push(`Organization status: ${patch.organization_status}`);
  return parts.join(" · ");
}

function recoveryItemMarkup(item, recovery) {
  const selectable = item.status === "ready" || item.status === "needs_confirmation";
  const checked = recovery.selectedIds.has(item.item_id);
  const candidateChoice = recovery.candidateChoices.get(item.item_id);
  const candidates = Array.isArray(item.candidates) ? item.candidates : [];
  const candidateControl = item.status === "needs_confirmation"
    ? candidates.length
      ? `<label class="recovery-candidate-label">Candidate<select class="select-control" data-recovery-candidate="${escapeHtml(item.item_id)}"><option value="">Choose a candidate</option>${candidates.map((candidate, index) => `<option value="${index}" ${candidateChoice === index ? "selected" : ""}>${escapeHtml(candidate.label)}</option>`).join("")}</select></label>`
      : `<p class="field-hint">No supported candidate is available for this item.</p>`
    : "";
  const summary = item.patch ? recoveryPatchSummary(item.patch) : "";
  return `<article class="recovery-item">
    <label class="recovery-item-select"><input type="checkbox" data-recovery-item="${escapeHtml(item.item_id)}" ${checked ? "checked" : ""} ${selectable ? "" : "disabled"}><span><strong>${escapeHtml(item.title || "Untitled video")}</strong><code>${escapeHtml(item.relative_path || "Path unavailable")}</code></span></label>
    <span class="state-pill ${selectable ? "pending" : ""}">${escapeHtml(RECOVERY_GROUP_TITLES[item.status])}</span>
    ${summary ? `<p class="field-hint">${escapeHtml(summary)}</p>` : ""}
    ${item.reason ? `<p class="recovery-reason">${escapeHtml(item.reason)}</p>` : item.status === "deferred" ? `<p class="recovery-reason">This item was not analyzed within the current work budget.</p>` : ""}
    ${candidateControl}
  </article>`;
}

function renderMetadataRecovery() {
  const current = recoveryContext();
  const canOperate = Boolean(current && libraryCanOperate(current.libraryId));
  const recovery = current ? currentMetadataRecovery() : null;
  const report = recovery?.report ?? null;
  const groups = report ? groupMetadataRecoveryItems(report.items) : null;
  const libraryButton = byId("recover-library");
  if (libraryButton) {
    libraryButton.disabled = !canOperate || Boolean(recovery?.requestId && !report && !recovery.error && !recovery.invalidated);
    libraryButton.textContent = recovery?.scope === "library" && report?.remaining > 0 ? "Analyze next batch" : "Analyze current library";
  }
  const selectedButton = byId("recover-selected");
  if (selectedButton) selectedButton.disabled = !canOperate || state.selected.size === 0 || state.selected.size > MAX_SELECTED_ITEMS;
  const status = byId("recovery-status");
  const results = byId("metadata-recovery-results");
  if (!status || !results) return;

  if (!current) {
    status.textContent = "Connect and refresh an available library before analyzing metadata.";
    results.innerHTML = "";
    return;
  }
  if (!recovery) {
    status.textContent = canOperate
      ? "Analyze selected items from the selection tray, or start a bounded pass over the current library."
      : "This library is offline or showing a saved scan. Reconnect it before analyzing metadata.";
    results.innerHTML = "";
    return;
  }
  if (recovery.invalidated) {
    status.textContent = recovery.compacted
      ? "This saved report was compacted and its item details are unavailable. Analyze again to get a fresh report."
      : "The library changed after this analysis. Its selections and candidates were cleared; analyze again before previewing updates.";
    results.innerHTML = "";
    return;
  }
  if (!report) {
    status.textContent = recovery.deliveryUnknown
      ? "The analysis request delivery outcome is unknown. Check your computer before starting another analysis; this request will not be resent automatically."
      : recovery.error
        ? `Analysis failed: ${recovery.error}`
        : "Metadata analysis is queued or running on your computer. Results arrive through the existing live update connection.";
    results.innerHTML = "";
    return;
  }

  status.textContent = recovery.scope === "selected"
    ? `Analysis revision ${report.revision} · ${report.items.length} selected result${report.items.length === 1 ? "" : "s"} · ${report.total} needs-organizing candidates in the library`
    : `Analysis revision ${report.revision} · ${report.items.length} items in this batch · ${report.total} needs-organizing candidates in the library · ${report.remaining} remaining after this batch`;
  const sections = Object.entries(groups).filter(([key, items]) => key !== "deferred" || items.length > 0).map(([key, items]) => {
    const content = items.length ? items.map((item) => recoveryItemMarkup(item, recovery)).join("")
      : `<p class="field-hint">No items in this group.</p>`;
    return `<section class="recovery-group" aria-labelledby="recovery-group-${key}"><h4 id="recovery-group-${key}">${escapeHtml(RECOVERY_GROUP_TITLES[key])} <span>${items.length}</span></h4>${content}</section>`;
  }).join("");
  const deferredItems = report.items.filter((item) => item.status === "deferred");
  const continuationHint = recovery.scope === "library" && report.remaining > 0
    ? deferredItems.length > 0
      ? "This batch reached its work budget. Analyze the next batch to retry deferred rows from the last completed item."
      : "More candidates remain. Analyze the next batch to continue through the library without silently skipping rows."
    : "";
  const deferredRetry = recovery.scope === "selected" && deferredItems.length > 0
    ? `<button class="secondary-button" type="button" data-recovery-retry-deferred>Analyze deferred items</button>`
    : "";
  const previewPayload = canOperate ? buildMetadataRecoveryPreviewPayload({
    report,
    libraryId: current.libraryId,
    rootBinding: current.rootBinding,
    itemIds: [...recovery.selectedIds],
    candidateChoices: recovery.candidateChoices,
  }) : null;
  results.innerHTML = `${continuationHint ? `<p class="field-hint">${escapeHtml(continuationHint)}</p>` : ""}<div class="recovery-group-list">${sections}</div><div class="recovery-footer"><button class="primary-button" type="button" data-recovery-preview ${previewPayload ? "" : "disabled"}>Preview selected updates</button>${deferredRetry}${recovery.scope === "library" && report.remaining > 0 ? `<button class="secondary-button" type="button" data-recovery-next>Analyze next batch</button>` : ""}<button class="quiet-button" type="button" data-recovery-clear>Clear report and choices</button></div>`;
}

function operationTitle(operation) {
  const kind = String(operation.kind ?? "operation").replaceAll("_", " ");
  return kind.replace(/^\w/u, (letter) => letter.toLocaleUpperCase());
}

function statusText(value) {
  const text = String(value ?? "pending").replaceAll("_", " ");
  return text.replace(/^\w/u, (letter) => letter.toLocaleUpperCase());
}

const SETTLED_OPERATION_STATUSES = new Set([
  "completed", "failed", "cancelled", "confirmed", "expired", "stale", "rejected",
  "uncertain", "partial_failure", "partial", "stopped", "superseded",
]);

function historySearchMatches(value) {
  const query = String(state.historyQuery ?? "").trim().toLocaleLowerCase();
  if (!query) return true;
  try { return JSON.stringify(value).toLocaleLowerCase().includes(query); } catch { return false; }
}

function legacyOperations() {
  return (Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : []).filter((operation) =>
    operation?.kind === "legacy_import" && operationMatchesLibrary(operation, activeLibraryId()));
}

function allReviewEntries() {
  return [
    ...allFilePreviews().filter((entry) => isReviewablePreviewStatus(entry.status)
      && !snapshotConfirmsPreviewCancellation("file_preview", entry))
      .map((entry) => ({ kind: "file_preview", entry, libraryId: entry.library_id ?? "" })),
    ...allMigrationPreviewEntries().map((entry) => ({
      kind: "library_migration_preview",
      entry,
      libraryId: String(entry.result?.source_library_id ?? entry.source_library_id ?? ""),
    })),
  ];
}

function queuedCancellationMatches(action, review) {
  const payload = previewCancellationPayload(review.kind, review.entry);
  return action?.kind === "preview_cancel" && payload
    && action.payload?.preview_kind === payload.preview_kind
    && action.payload?.preview_seq === payload.preview_seq
    && action.payload?.preview_id === payload.preview_id
    && action.payload?.revision === payload.revision;
}

function renderWaiting() {
  const tasks = Array.isArray(state.snapshot?.tasks) ? state.snapshot.tasks : [];
  const operations = Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [];
  const cloudQueue = Array.isArray(state.snapshot?.pending_requests) ? state.snapshot.pending_requests : [];
  const { cloud: pendingRequests, local } = buildWaitingEntries(cloudQueue, tasks, operations);
  const reviewEntries = allReviewEntries();
  const reviewSequences = new Set(reviewEntries.map(({ entry }) => entry.seq).filter((seq) => Number.isSafeInteger(seq) && seq > 0));
  const pendingCloud = pendingRequests.filter((entry) => isWaitingEntryPending(entry)
    && !(Number.isSafeInteger(entry.seq) && reviewSequences.has(entry.seq)));
  const pending = local.filter((entry) => isWaitingEntryPending(entry)
    && !(Number.isSafeInteger(entry.seq) && reviewSequences.has(entry.seq)));
  const localQueue = readActionQueue();
  for (const action of localQueue) {
    if (action.delivery_outcome === "outcome_unknown") {
      markUnknownFileAction(state, action);
      if (action.kind === "preview_cancel") persistPreviewCancellations();
    }
  }
  const cards = [];
  for (const request of pendingCloud) cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(operationTitle(request))}</h3><p>Saved in the cloud inbox. Your desktop app has not acknowledged local receipt yet.</p></div><span class="state-pill pending">Waiting for computer · #${escapeHtml(request.seq)}</span></div><p>Received ${escapeHtml(timeAgo(request.created_at))}</p></article>`);
  for (const action of localQueue) {
    if (action.kind === "preview_cancel" && reviewEntries.some((review) => queuedCancellationMatches(action, review))) continue;
    const remembered = state.unknownActionOutcomes.get(action.request_id);
    const displayAction = remembered && !isUnknownDeliveryAction(action)
      ? { ...action, delivery_outcome: "outcome_unknown", delivery_error: remembered.error }
      : action;
    if (isUnknownDeliveryAction(displayAction, state.unknownActionOutcomes)) {
      const title = action.kind === "download_submit" ? "Download delivery outcome unknown"
        : action.kind === "preview_cancel" ? "Cancellation delivery outcome unknown"
          : "Preview delivery outcome unknown";
      const detail = typeof displayAction.delivery_error === "string"
        ? displayAction.delivery_error
        : remembered?.error ?? "The request may or may not have reached your computer.";
      cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${title}</h3><p>${escapeHtml(detail)} It will not be retried automatically. Check your computer before dismissing this notice.</p></div><span class="state-pill warning">Outcome unknown</span></div><div class="stack-actions"><button class="quiet-button" type="button" data-dismiss-unknown-action="${escapeHtml(action.request_id)}">Dismiss after checking computer</button></div></article>`);
    } else {
      cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(action.kind.replaceAll("_", " "))}</h3><p>Saved in this browser. It will be sent when this device is connected.</p></div><span class="state-pill pending">Offline queue</span></div></article>`);
    }
  }
  for (const entry of pending) cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(operationTitle(entry))}</h3><p>${escapeHtml(entry.message ?? entry.error ?? "Your computer has received this request and is working on it.")}</p></div><span class="state-pill pending">${escapeHtml(statusText(entry.status))}</span></div>${entry.updated_at ? `<p>Updated ${escapeHtml(timeAgo(entry.updated_at, "seconds"))}</p>` : ""}</article>`);
  byId("waiting-list").innerHTML = cards.length ? cards.join("") : `<div class="empty-state"><div class="empty-art" aria-hidden="true">✓</div><h2>No other requests are waiting</h2><p>New requests and scans will appear here while your computer processes them.</p></div>`;
}

function operationPreview(operation) {
  if (operation?.kind !== "file_preview") return null;
  const result = operation.result && typeof operation.result === "object" ? operation.result : {};
  const preview = operation.preview && typeof operation.preview === "object"
    ? operation.preview
    : result.preview && typeof result.preview === "object" ? result.preview : null;
  const identity = fileOperationLibraryIdentity(operation);
  const id = operation.preview_id ?? result.preview_id ?? preview?.preview_id ?? preview?.id;
  const revision = operation.revision ?? result.revision ?? preview?.revision;
  if (typeof id !== "string" || typeof revision !== "string") return null;
  return {
    ...operation,
    kind: "file_preview",
    seq: operation.seq,
    id,
    revision,
    preview,
    status: operation.status,
    incomplete: operation.details_incomplete === true || operation.result_truncated === true || !preview,
    preview_incomplete: operation.preview_incomplete === true,
    reload_required: operation.reload_required === true,
    reason: operation.reason,
    ...(identity ?? {}),
  };
}

function allFilePreviews() {
  const collected = [];
  for (const preview of Array.isArray(state.snapshot?.previews) ? state.snapshot.previews : []) {
    if (preview?.kind !== "file_preview") continue;
    const incomplete = preview.preview_incomplete === true || preview.incomplete === true || preview.reload_required === true;
    const id = preview.preview_id ?? preview.id;
    const revision = preview.revision ?? preview.preview?.revision;
    if (typeof id !== "string" || typeof revision !== "string") continue;
    collected.push({
      ...preview,
      kind: "file_preview",
      seq: preview.seq,
      id,
      revision,
      preview: incomplete ? null : preview.preview && typeof preview.preview === "object" ? preview.preview : preview,
      incomplete: incomplete || !preview.preview,
      preview_incomplete: preview.preview_incomplete === true,
      reload_required: preview.reload_required === true,
      reason: preview.reason,
      status: preview.status,
      ...(fileOperationLibraryIdentity(preview) ?? {}),
    });
  }
  for (const operation of Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : []) {
    const found = operationPreview(operation);
    if (found) collected.push(found);
  }
  const unique = new Map();
  for (const value of collected) {
    if (Number.isSafeInteger(value.seq) && value.seq > 0) {
      const key = `seq:${value.seq}`;
      const existing = unique.get(key);
      if (!existing || existing.incomplete && !value.incomplete) unique.set(key, value);
    } else {
      unique.set(JSON.stringify([value.kind, value.id, value.revision]), value);
    }
  }
  return [...unique.values()].reverse();
}

function omittedPreviewAggregate(snapshot = state.snapshot) {
  const aggregates = (Array.isArray(snapshot?.previews) ? snapshot.previews : [])
    .filter((entry) => entry && typeof entry === "object" && typeof entry.kind !== "string"
      && entry.preview_incomplete === true && Number.isSafeInteger(entry.omitted_count) && entry.omitted_count > 0);
  if (!aggregates.length) return null;
  const aggregate = aggregates[0];
  const omittedCount = Math.max(...aggregates.map((entry) => entry.omitted_count));
  return { omittedCount, reason: typeof aggregate.reason === "string" ? aggregate.reason : "Some active previews were omitted from the saved snapshot." };
}

function previews() {
  return allFilePreviews().filter((entry) => !entry.library_id || entry.library_id === activeLibraryId());
}

function previewCancellationPayload(kind, entry) {
  if (!entry || !["file_preview", "library_migration_preview"].includes(kind)
      || !Number.isSafeInteger(entry.seq) || entry.seq <= 0) return null;
  const id = entry.preview_id ?? entry.id;
  const revision = entry.revision;
  const encoder = new TextEncoder();
  if (typeof id !== "string" || !id || id.trim() !== id || encoder.encode(id).byteLength > 128
      || /[\u0000-\u001f\u007f\u0080-\u009f]/u.test(id)
      || typeof revision !== "string" || !revision || revision.trim() !== revision || encoder.encode(revision).byteLength > 256
      || /[\u0000-\u001f\u007f\u0080-\u009f]/u.test(revision)) return null;
  return { preview_kind: kind, preview_seq: entry.seq, preview_id: id, revision };
}

function previewCancellationKey(payload) {
  return payload ? JSON.stringify([payload.preview_kind, payload.preview_seq, payload.preview_id, payload.revision]) : null;
}

function previewCancellationTracker(kind, entry) {
  const payload = previewCancellationPayload(kind, entry);
  const key = previewCancellationKey(payload);
  if (!payload || !key) return null;
  let tracker = state.previewCancellations.get(key);
  if (!tracker) {
    const queuedAction = readActionQueue().find((action) => action.kind === "preview_cancel"
      && action.payload?.preview_kind === payload.preview_kind
      && action.payload?.preview_seq === payload.preview_seq
      && action.payload?.preview_id === payload.preview_id
      && action.payload?.revision === payload.revision);
    if (queuedAction) {
      tracker = {
        requestId: queuedAction.request_id,
        seq: null,
        status: queuedAction.delivery_outcome === "outcome_unknown" ? "unknown" : "queued",
        error: queuedAction.delivery_error ?? "",
      };
      state.previewCancellations.set(key, tracker);
      persistPreviewCancellations();
    }
  }
  return { payload, key, tracker: tracker ?? null };
}

const ACTIVE_PREVIEW_CANCELLATION_STATUSES = new Set([
  "queued", "submitted", "unknown", "pending", "processing", "inflight", "in_flight",
]);

function isActivePreviewCancellation(tracker, key) {
  return Boolean(key && state.previewCancellationAttempts.has(key))
    || ACTIVE_PREVIEW_CANCELLATION_STATUSES.has(String(tracker?.status ?? "").toLowerCase());
}

function previewCancellationBusy(kind, entry) {
  const cancellation = previewCancellationTracker(kind, entry);
  if (!cancellation) return false;
  return isActivePreviewCancellation(cancellation.tracker, cancellation.key);
}

function migrationCancellationBusyForDraft(previewId, revision) {
  const operation = migrationPreviewEntries().find((entry) => entry.id === previewId && entry.revision === revision);
  if (operation && previewCancellationBusy("library_migration_preview", operation)) return true;
  const hasMatchingCancellation = (key) => {
    let identity;
    try { identity = JSON.parse(key); } catch { return false; }
    return Array.isArray(identity) && identity[0] === "library_migration_preview"
      && identity[2] === previewId && identity[3] === revision;
  };
  for (const [key, tracker] of state.previewCancellations) {
    if (hasMatchingCancellation(key) && isActivePreviewCancellation(tracker, key)) return true;
  }
  for (const key of state.previewCancellationAttempts) {
    if (hasMatchingCancellation(key)) return true;
  }
  return false;
}

function reviewTargetForCancellation(payload) {
  return allReviewEntries().find(({ kind, entry }) => {
    const target = previewCancellationPayload(kind, entry);
    return target && target.preview_kind === payload?.preview_kind
      && target.preview_seq === payload?.preview_seq
      && target.preview_id === payload?.preview_id
      && target.revision === payload?.revision;
  }) ?? null;
}

function cancellationOperationMatches(operation, payload) {
  const result = operation?.result && typeof operation.result === "object" ? operation.result : {};
  return operation?.kind === "preview_cancel"
    && String(operation.status ?? "").toLowerCase() === "completed"
    && result.cancelled === true
    && result.preview_kind === payload.preview_kind
    && result.preview_seq === payload.preview_seq
    && result.preview_id === payload.preview_id
    && result.revision === payload.revision;
}

function previewCancellationHistory(tracker, snapshot) {
  if (!Number.isSafeInteger(tracker?.seq) || tracker.seq <= 0) return { kind: "unaccepted" };
  const operations = Array.isArray(snapshot?.operations) ? snapshot.operations : null;
  const pendingRequests = Array.isArray(snapshot?.pending_requests) ? snapshot.pending_requests : null;
  if (!operations || !pendingRequests) return { kind: "unknown" };
  const operation = operations.find((entry) => entry?.kind === "preview_cancel" && entry.seq === tracker.seq);
  if (operation) return { kind: "recorded", operation };
  const pending = pendingRequests.find((entry) => entry?.seq === tracker.seq);
  if (pending) return { kind: "pending", pending };
  const lastSeen = snapshot.last_seen;
  if (snapshot.synced !== true || snapshot.stale !== false || !Number.isSafeInteger(lastSeen)
      || lastSeen > Date.now() || Date.now() - lastSeen > RECENT_SNAPSHOT_MAX_AGE_MS
      || operations.length !== RECENT_OPERATION_SNAPSHOT_LIMIT) return { kind: "unknown" };
  let previousSequence = tracker.seq;
  for (const entry of operations) {
    if (!Number.isSafeInteger(entry?.seq) || entry.seq <= previousSequence || typeof entry.kind !== "string" || typeof entry.status !== "string") {
      return { kind: "unknown" };
    }
    previousSequence = entry.seq;
  }
  return { kind: "outside_recent_history" };
}

function reconcilePreviewCancellations(snapshot) {
  if (!(state.previewCancellations instanceof Map) || !snapshot) return false;
  let changed = false;
  const removedKeys = new Set();
  const operations = Array.isArray(snapshot.operations) ? snapshot.operations : [];
  const snapshotPreviews = Array.isArray(snapshot.previews) ? snapshot.previews : [];
  for (const [key, tracker] of state.previewCancellations) {
    let identity;
    try { identity = JSON.parse(key); } catch { continue; }
    const [kind, targetSeq, targetId, targetRevision] = identity;
    const payload = { preview_kind: kind, preview_seq: targetSeq, preview_id: targetId, revision: targetRevision };
    const target = [...snapshotPreviews, ...operations].find((entry) => entry?.kind === kind && entry.seq === targetSeq
      && (entry.preview_id ?? entry.id ?? entry.result?.preview_id ?? entry.result?.id) === targetId
      && (entry.revision ?? entry.result?.revision) === targetRevision);
    const targetStatus = String(target?.status ?? "").toLowerCase();
    if (!Number.isSafeInteger(tracker.seq)) {
      const cancellation = operations.find((entry) => entry?.kind === "preview_cancel"
        && entry?.result?.preview_kind === kind
        && entry.result.preview_seq === targetSeq
        && entry.result.preview_id === targetId
        && entry.result.revision === targetRevision);
      if (cancellation && cancellationOperationMatches(cancellation, payload)) {
        state.previewCancellations.delete(key);
        removedKeys.add(key);
        changed = true;
        continue;
      }
      if (cancellation && ["failed", "rejected", "stale", "expired"].includes(String(cancellation.status).toLowerCase())) {
        tracker.status = "failed";
        tracker.error = String(cancellation.error ?? "The computer could not cancel this preview.");
        changed = true;
      }
    }
    if (target && SETTLED_OPERATION_STATUSES.has(targetStatus)) {
      state.previewCancellations.delete(key);
      removedKeys.add(key);
      changed = true;
      continue;
    }
    if (!Number.isSafeInteger(tracker.seq)) continue;
    const cancellation = operations.find((entry) => entry?.kind === "preview_cancel" && entry.seq === tracker.seq);
    if (cancellation) {
      if (cancellationOperationMatches(cancellation, payload)) {
        state.previewCancellations.delete(key);
        removedKeys.add(key);
        changed = true;
        continue;
      }
      if (["failed", "rejected", "stale", "expired"].includes(String(cancellation.status).toLowerCase())) {
        tracker.status = "failed";
        tracker.error = String(cancellation.error ?? "The computer could not cancel this preview.");
        changed = true;
      }
      continue;
    }
    if (tracker.status === "submitted" && previewCancellationHistory(tracker, snapshot).kind === "outside_recent_history") {
      tracker.status = "unknown";
      tracker.error = `Cancellation request #${tracker.seq} is outside recent history. Check your computer before trying again.`;
      changed = true;
    }
  }
  if (changed) persistPreviewCancellations({ removeKeys: removedKeys });
  return changed;
}

function describePreview(preview) {
  const moveItems = Array.isArray(preview.items) ? preview.items : [];
  const patchItems = Array.isArray(preview.metadata_patches) ? preview.metadata_patches : [];
  const lines = [];
  const moveRows = renderMovePreviewItems(moveItems);
  if (moveRows) lines.push(moveRows);
  for (const patch of patchItems) {
    const details = metadataPreviewDetails(patch).map((detail) => detail.kind === "status"
      ? escapeHtml(detail.text)
      : `${escapeHtml(detail.label)}: ${escapeHtml(detail.before ?? "Not set")} → ${escapeHtml(detail.after)}`);
    const changes = details.join("<br>");
    lines.push(`<div class="preview-line"><span>${escapeHtml(patch.nfo_path ?? patch.item_id ?? "Metadata file")}<br><span class="field-hint">${changes}</span></span><strong class="move">Metadata</strong></div>`);
  }
  return lines.length ? lines.join("") : `<div class="preview-line"><span>This preview contains no file or metadata changes.</span></div>`;
}

function incompleteTaskSummary(libraryId) {
  const descriptor = mediaLibraryEntries(state.snapshot).find((entry) => entry.id === libraryId);
  return incompleteMigrationTaskSummary(descriptor, state.snapshot?.tasks, libraryId);
}

function migrationTargetOptions() {
  const sourceId = activeLibraryId();
  const target = byId("migration-target");
  const selection = selectMigrationTarget(mediaLibraryEntries(state.snapshot), sourceId, target.value, libraryCanOperate);
  target.innerHTML = renderMigrationTargetOptions(selection, escapeHtml);
  target.value = selection.selectedId;
  const sourceAvailable = libraryCanOperate(sourceId);
  const targetAvailable = libraryCanOperate(target.value);
  const sourceOption = byId("library-select").selectedOptions[0];
  const targetOption = target.selectedOptions[0];
  const bindingsReady = isValidLibraryRootBinding(sourceOption?.dataset?.rootBinding)
    && isValidLibraryRootBinding(targetOption?.dataset?.rootBinding);
  target.disabled = selection.targets.length === 0 || !sourceAvailable;
  byId("migration-scope").disabled = !sourceAvailable;
  byId("migration-include-tasks").disabled = !sourceAvailable;
  byId("migration-set-default").disabled = !sourceAvailable;
  byId("migration-conflict").disabled = !sourceAvailable;
  byId("migration-preview").disabled = selection.availableTargets.length === 0 || !sourceAvailable || !targetAvailable;
  byId("migration-source").textContent = `Source: ${libraryLabel(sourceId)} · Choose the source in the library selector above.`;
  const selectionSize = state.selected.size;
  const sourceMediaInfo = mediaSnapshotInfo(activeLibrary().descriptor, activeLibrarySnapshot());
  const sourceOmissionNotice = mediaSnapshotOmissionNotice(activeLibrary().descriptor, activeLibrarySnapshot());
  const selectedScope = byId("migration-scope").value === "selected";
  byId("migration-selection-count").textContent = selectedScope
    ? `${selectionSize ? `${selectionSize} selected media item${selectionSize === 1 ? "" : "s"} from this library. Collection selections include their selected media rows.` : "Select media in the Library view first to migrate a selected subset."}${sourceOmissionNotice ? ` Selected-scope previews can use only the ${sourceMediaInfo.retainedCount.toLocaleString()} retained rows available in this cloud view.` : ""}`
    : `An entire-library preview uses a fresh full local scan.${sourceOmissionNotice ? ` The cloud view currently retains ${sourceMediaInfo.retainedCount.toLocaleString()} of ${sourceMediaInfo.totalCount.toLocaleString()} items; the preview will report the full scan count.` : ""}`;
  const taskSummary = incompleteTaskSummary(sourceId);
  byId("migration-task-option").classList.toggle("hidden", taskSummary.count === 0);
  const countLabel = taskSummary.countIsLowerBound ? `At least ${taskSummary.count}` : String(taskSummary.count);
  const byteLabel = taskSummary.bytes === null ? "staged data size unknown" : `${formatBytes(taskSummary.bytes)} staged data`;
  byId("migration-task-count").textContent = `${countLabel} unfinished task${taskSummary.count === 1 ? "" : "s"} · ${byteLabel}. This option is off by default.`;
  byId("migration-preview").disabled = !sourceAvailable || !selection.availableTargets.length || !targetAvailable || !bindingsReady || selectedScope && selectionSize === 0;
}

function migrationPreviewEntries() {
  return collectMigrationPreviewEntries(state.snapshot, activeLibraryId())
    .filter((entry) => !snapshotConfirmsPreviewCancellation("library_migration_preview", entry));
}

function allMigrationPreviewEntries() {
  return collectMigrationPreviewEntries(state.snapshot)
    .filter((entry) => !snapshotConfirmsPreviewCancellation("library_migration_preview", entry));
}

function retainedMigrationState() {
  const descriptors = mediaLibraryEntries(state.snapshot);
  const migrations = new Map();
  // The Rust snapshot publishes one global migration history and omitted count. Nested
  // library snapshots may carry a mirrored count, so combine reports by maximum, not sum.
  let omittedCount = Number.isSafeInteger(state.snapshot?.library?.migration_omitted_count)
    && state.snapshot.library.migration_omitted_count > 0
    ? state.snapshot.library.migration_omitted_count
    : 0;
  for (const descriptor of descriptors) {
    const snapshot = descriptor.snapshot && typeof descriptor.snapshot === "object"
      ? descriptor.snapshot
      : descriptor.id === activeLibraryId() ? state.snapshot?.library : null;
    if (!snapshot) continue;
    const omitted = Number.isSafeInteger(snapshot.migration_omitted_count) && snapshot.migration_omitted_count > 0
      ? snapshot.migration_omitted_count
      : 0;
    omittedCount = Math.max(omittedCount, omitted);
    for (const migration of Array.isArray(snapshot.migrations) ? snapshot.migrations : []) {
      if (!migration || typeof migration !== "object") continue;
      const key = typeof migration.id === "string" && migration.id
        ? migration.id
        : JSON.stringify([migration.source_library_id, migration.target_library_id, migration.started_at, migration.created_at]);
      migrations.set(key, migration);
    }
  }
  const active = mediaLibraryEntries(state.snapshot).some((entry) => entry.id === activeLibraryId());
  if (!migrations.size && active && Array.isArray(state.snapshot?.library?.migrations)) {
    for (const migration of state.snapshot.library.migrations) {
      if (migration && typeof migration === "object") migrations.set(String(migration.id ?? JSON.stringify(migration)), migration);
    }
  }
  return { migrations: [...migrations.values()], omittedCount };
}

function migrationConflictDraft(previewId, revision) {
  const key = migrationPreviewDraftKey(previewId, revision);
  return key ? state.migrationConflictDrafts.get(key) ?? null : null;
}

function saveMigrationConflictDraft(previewId, revision, draft) {
  const key = migrationPreviewDraftKey(previewId, revision);
  if (!key) return null;
  state.migrationConflictDrafts.set(key, { ...draft, previewId, revision });
  return key;
}

function deleteMigrationConflictDraft(previewId, revision) {
  const key = migrationPreviewDraftKey(previewId, revision);
  if (!key) return;
  state.migrationConflictDrafts.delete(key);
  state.migrationConflictInvalidated.delete(key);
}

function retireMigrationConflictDrafts(previews) {
  const liveKeys = new Set(previews.map((preview) => migrationPreviewDraftKey(preview.id, preview.revision)));
  const snapshots = [
    ...(Array.isArray(state.snapshot?.previews) ? state.snapshot.previews : []),
    ...(Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : []),
  ];
  const retainedRefreshSequences = new Set(snapshots
    .filter((entry) => entry?.kind === "library_migration_preview" && Number.isSafeInteger(entry.seq))
    .map((entry) => entry.seq));
  const retainedRequestIds = new Set(readActionQueue().map((action) => action.request_id));
  for (const [key, draft] of state.migrationConflictDrafts) {
    if (liveKeys.has(key)
      || typeof draft.refreshRequestId === "string" && retainedRequestIds.has(draft.refreshRequestId)
      || Number.isSafeInteger(draft.refreshSeq) && retainedRefreshSequences.has(draft.refreshSeq)) continue;
    state.migrationConflictDrafts.delete(key);
    state.migrationConflictInvalidated.delete(key);
  }
  for (const key of state.migrationConflictInvalidated) {
    if (!state.migrationConflictDrafts.has(key) && !liveKeys.has(key)) state.migrationConflictInvalidated.delete(key);
  }
}

function settleMigrationConflictRefreshes() {
  reconcileMigrationRefreshDrafts(state.migrationConflictDrafts, state.snapshot, Date.now(), state.migrationConflictInvalidated);
}

function renderMigrationPreview(operation) {
  const result = operation?.result && typeof operation.result === "object" ? operation.result : {};
  const id = String(operation?.preview_id ?? operation?.id ?? result.id ?? result.preview_id ?? "");
  const revision = String(operation?.revision ?? result.revision ?? "");
  const sourceLibraryId = migrationLibraryId(operation, result, "source_library_id");
  const targetLibraryId = migrationLibraryId(operation, result, "target_library_id");
  const endpoints = migrationEndpointAvailability(state.snapshot, sourceLibraryId, targetLibraryId);
  const rootBindingsMatch = migrationPreviewBindingsMatch(
    state.snapshot,
    sourceLibraryId,
    targetLibraryId,
    result.source_root_binding,
    result.target_root_binding,
  );
  const draft = migrationConflictDraft(id, revision);
  if (draft && Number.isSafeInteger(draft.refreshSeq)) {
    draft.refreshHistory = inspectMigrationRefreshHistory({ seq: draft.refreshSeq }, state.snapshot).kind;
  }
  const savedConflicts = normalizeMigrationItemConflicts(result.item_conflicts);
  const draftConflicts = draft ? normalizeMigrationItemConflicts(draft.itemConflicts) : savedConflicts;
  const choicesChanged = Boolean(draft && migrationConflictChoicesChanged(savedConflicts, draftConflicts));
  const cancellation = previewCancellationMarkup("library_migration_preview", operation);
  const cardId = previewCardId("library_migration_preview", operation);
  const card = renderMigrationPreviewCard(operation, {
    activeLibraryId: activeLibraryId(),
    sourceAvailable: endpoints.sourceAvailable,
    targetAvailable: endpoints.targetAvailable,
    rootBindingsMatch,
    draft,
    page: draft?.page,
    draftChanged: choicesChanged,
    refreshPending: Boolean(draft?.refreshRequestId),
    invalidated: state.migrationConflictInvalidated.has(migrationPreviewDraftKey(id, revision)),
    cancelMarkup: cancellation,
    cancelPending: previewCancellationBusy("library_migration_preview", operation),
    cardId,
  });
  return card.replace('<article class="stack-card">', `<article class="stack-card" id="${cardId}" tabindex="-1">`);
}

function renderMigrations() {
  migrationTargetOptions();
  const previews = migrationPreviewEntries();
  retireMigrationConflictDrafts(previews);
  byId("migration-preview-list").innerHTML = previews.map(renderMigrationPreview).join("");
  const library = state.snapshot?.library;
  const libraries = mediaLibraryEntries(state.snapshot);
  const retained = retainedMigrationState();
  const migrations = retained.migrations;
  const hasActiveControls = (migration) => migrationControlActions(migration).length > 0
    || ["queued", "running", "paused"].includes(String(migration?.status ?? "").toLowerCase());
  byId("migration-progress-list").innerHTML = renderMigrationDiscoveryWarnings({
    warnings: library?.migration_discovery_warnings,
    omittedCount: library?.migration_discovery_warning_omitted_count,
    libraries,
  }) + renderMigrationHistory({
    migrations: migrations.filter(hasActiveControls),
    libraries,
    libraryId: activeLibraryId(),
    omittedCount: 0,
  });
  byId("history-migration-list").innerHTML = renderMigrationHistory({
    migrations: migrations.filter((migration) => !hasActiveControls(migration) && historySearchMatches(migration)),
    libraries,
    omittedCount: retained.omittedCount,
  });
}

function previewCardId(kind, entry) {
  return `review-${accessibleToken(JSON.stringify([kind, entry?.seq, entry?.id, entry?.revision]))}`;
}

function snapshotConfirmsPreviewCancellation(kind, entry) {
  const payload = previewCancellationPayload(kind, entry);
  if (!payload) return false;
  return (Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [])
    .some((operation) => cancellationOperationMatches(operation, payload));
}

function previewCancellationMarkup(kind, entry) {
  const cancellation = previewCancellationTracker(kind, entry);
  if (!cancellation) return "";
  const { payload, key, tracker } = cancellation;
  const attempt = state.previewCancellationAttempts.has(key);
  const busy = attempt || tracker?.status === "queued" || tracker?.status === "submitted";
  const attrs = `data-preview-cancel-kind="${escapeHtml(kind)}" data-preview-cancel-seq="${escapeHtml(payload.preview_seq)}" data-preview-cancel-id="${escapeHtml(payload.preview_id)}" data-preview-cancel-revision="${escapeHtml(payload.revision)}"`;
  if (tracker?.status === "unknown") {
    return `<p class="field-hint" role="status">${escapeHtml(tracker.error || "The cancellation delivery outcome is unknown.")} Check your computer before trying again.</p><button class="quiet-button" type="button" data-preview-cancel-dismiss-kind="${escapeHtml(kind)}" data-preview-cancel-dismiss-seq="${escapeHtml(payload.preview_seq)}" data-preview-cancel-dismiss-id="${escapeHtml(payload.preview_id)}" data-preview-cancel-dismiss-revision="${escapeHtml(payload.revision)}">Dismiss after checking computer</button>`;
  }
  const status = busy
    ? `<p class="field-hint" role="status">${tracker?.status === "queued" ? "Cancellation is saved in this browser and will be sent when connected." : Number.isSafeInteger(tracker?.seq) ? `Cancellation request #${tracker.seq} was submitted. Waiting for your computer.` : "Submitting cancellation…"}</p>`
    : tracker?.status === "failed" ? `<p class="field-hint" role="alert">${escapeHtml(tracker.error || "The computer could not cancel this preview.")}</p>` : "";
  const label = busy ? "Cancel request submitted" : tracker?.status === "failed" ? "Retry cancellation" : "Cancel preview";
  return `${status}<button class="quiet-button" type="button" data-preview-cancel ${attrs} ${busy ? "disabled" : ""}>${label}</button>`;
}

function renderReviewNavigation() {
  const cards = [];
  const entries = allReviewEntries();
  for (const review of entries) {
    if (!review.libraryId || review.libraryId === activeLibraryId()) continue;
    const { kind, entry, libraryId } = review;
    const canReview = Boolean(registeredMediaLibrary(state.snapshot, libraryId)
      && [...byId("library-select").options].some((option) => option.value === libraryId));
    const id = previewCancellationPayload(kind, entry)?.preview_id ?? entry.id ?? "";
    const revision = entry.revision ?? "";
    const sequence = Number.isSafeInteger(entry.seq) ? entry.seq : "";
    const cancelState = previewCancellationTracker(kind, entry)?.tracker;
    const cancelMessage = cancelState?.status === "queued" ? "Cancellation is queued in this browser."
      : cancelState?.status === "submitted" ? `Cancellation request #${cancelState.seq} is waiting for your computer.`
        : cancelState?.status === "failed" ? `Cancellation failed: ${cancelState.error || "The computer could not cancel this preview."}`
          : "";
    const title = kind === "file_preview" ? "File changes need review" : "Library migration needs review";
    const reviewButton = canReview
      ? `<button class="secondary-button" type="button" data-review-library="${escapeHtml(libraryId)}" data-review-preview-kind="${escapeHtml(kind)}" data-review-preview-seq="${escapeHtml(sequence)}" data-review-preview-id="${escapeHtml(id)}" data-review-preview-revision="${escapeHtml(revision)}">Review</button>`
      : `<p class="field-hint">This library is no longer registered. You can still cancel this saved preview by its exact identity.</p>`;
    cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${title}</h3><p>${escapeHtml(libraryLabel(libraryId))} · request ${sequence ? `#${sequence}` : "details unavailable"}</p>${cancelMessage ? `<p class="field-hint">${escapeHtml(cancelMessage)}</p>` : ""}</div><span class="state-pill pending">Review needed</span></div><div class="stack-actions">${reviewButton}${previewCancellationMarkup(kind, entry)}</div></article>`);
  }
  const recovery = state.metadataRecoveryPreviewRequest;
  if (recovery && recovery.libraryId !== activeLibraryId()
      && registeredMediaLibrary(state.snapshot, recovery.libraryId)?.root_binding === recovery.rootBinding) {
    cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>Recovery preview is being prepared</h3><p>${escapeHtml(libraryLabel(recovery.libraryId))} · request ${Number.isSafeInteger(recovery.seq) ? `#${recovery.seq}` : escapeHtml(recovery.requestId)}</p></div><span class="state-pill pending">Waiting</span></div><button class="secondary-button" type="button" data-review-library="${escapeHtml(recovery.libraryId)}">Open library</button></article>`);
  }
  byId("review-navigation-list").innerHTML = cards.join("");
}

function renderPreviewReviewCards() {
  reconcilePreviewCancellations(state.snapshot);
  renderMigrations();
  renderPreviews();
  renderReviewNavigation();
}

function renderPreviews() {
  const allPreviews = previews();
  const omittedAggregate = omittedPreviewAggregate();
  const submission = state.metadataRecoveryPreviewRequest;
  if (submission && Number.isSafeInteger(submission.seq)
      && allPreviews.some((entry) => entry.seq === submission.seq && isReviewablePreviewStatus(entry.status)
        && !snapshotConfirmsPreviewCancellation("file_preview", entry))) {
    setMetadataRecoveryPreviewRequest(null);
  }
  const pending = allPreviews.filter((entry) => isReviewablePreviewStatus(entry.status)
    && !snapshotConfirmsPreviewCancellation("file_preview", entry));
  const currentSubmission = state.metadataRecoveryPreviewRequest;
  const submissionLibrary = currentSubmission
    ? registeredMediaLibrary(state.snapshot, currentSubmission.libraryId)
    : null;
  const showRequestCard = Boolean(currentSubmission && currentSubmission.libraryId === activeLibraryId()
    && submissionLibrary?.root_binding === currentSubmission.rootBinding);
  const requestCard = showRequestCard
    ? (() => {
      const queued = readActionQueue().some((action) => action.request_id === currentSubmission.requestId);
      const operation = Number.isSafeInteger(currentSubmission.seq)
        ? (Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : []).find((entry) => entry?.seq === currentSubmission.seq && entry?.kind === "file_preview")
        : null;
      const operationStatus = String(operation?.status ?? currentSubmission.status ?? "").toLowerCase();
      const failureStatuses = ["failed", "rejected", "stale", "cancelled", "expired"];
      if (failureStatuses.includes(operationStatus)) {
        const error = operation?.error ?? currentSubmission.error ?? "The computer could not create this preview.";
        return `<article class="stack-card" id="${previewCardId("file_preview", { seq: currentSubmission.seq, id: currentSubmission.requestId, revision: "recovery" })}"><div class="stack-card-head"><div><h3>Recovery preview failed · request #${escapeHtml(currentSubmission.seq ?? currentSubmission.requestId)}</h3><p>${escapeHtml(error)}</p></div><span class="state-pill warning">Needs attention</span></div><button type="button" class="secondary-button" data-recovery-preview-dismiss="${escapeHtml(currentSubmission.requestId)}">Dismiss notice</button></article>`;
      }
      const message = currentSubmission.status === "rejected"
        ? `The computer rejected recovery preview request ${Number.isSafeInteger(currentSubmission.seq) ? `#${currentSubmission.seq}` : currentSubmission.requestId}: ${currentSubmission.error || "Request rejected"}`
        : currentSubmission.status === "unknown"
          ? `Delivery outcome for recovery preview request ${currentSubmission.requestId} is unknown. Check your computer before retrying.`
          : queued
            ? `Recovery preview request ${currentSubmission.requestId} is queued in this browser and will be sent when the connection is available.`
            : Number.isSafeInteger(currentSubmission.seq)
              ? `Recovery preview request #${currentSubmission.seq} was submitted. Waiting for your computer to prepare the preview.`
              : `Submitting recovery preview request ${currentSubmission.requestId}…`;
      return `<article class="stack-card" id="${previewCardId("file_preview", { seq: currentSubmission.seq, id: currentSubmission.requestId, revision: "recovery" })}"><div class="stack-card-head"><div><h3>Recovery preview is being prepared</h3><p>${escapeHtml(message)}</p></div><span class="state-pill pending">Waiting</span></div></article>`;
    })()
    : "";
  const omittedNotice = omittedAggregate
    ? `<article class="stack-card" role="status"><div class="stack-card-head"><div><h3>${omittedAggregate.omittedCount} active preview${omittedAggregate.omittedCount === 1 ? " was" : "s were"} omitted</h3><p>${escapeHtml(omittedAggregate.reason)} This saved snapshot does not include the preview identities, so these entries cannot be reviewed, confirmed, or cancelled here. Refresh Requests for an updated view.</p></div><span class="state-pill warning">Details unavailable</span></div></article>`
    : "";
  byId("preview-list").innerHTML = requestCard + omittedNotice + pending.map((entry) => {
    const cancel = previewCancellationMarkup("file_preview", entry);
    const cardId = previewCardId("file_preview", entry);
    if (isIncompletePreview(entry)) {
      const reason = entry.reason || "Preview details were omitted from the saved snapshot. Request a fresh preview before confirming.";
      return `<article class="stack-card" id="${cardId}" tabindex="-1"><div class="stack-card-head"><div><h3>Preview needs to be regenerated</h3><p>${escapeHtml(reason)}</p></div><span class="state-pill pending">Refresh required</span></div><div class="stack-actions">${cancel}</div></article>`;
    }
    if (!fileOperationIdentityMatches(state.snapshot, entry, entry.library_id ?? "")) {
      return `<article class="stack-card" id="${cardId}" tabindex="-1"><div class="stack-card-head"><div><h3>Preview needs refreshing</h3><p>This saved preview does not have a verified identity for the current library registration. No changes were applied; request a fresh preview before confirming.</p></div><span class="state-pill pending">Refresh required</span></div><div class="stack-actions">${cancel}</div></article>`;
    }
    const preview = entry.preview;
    const hasMetadataChanges = hasMetadataPreviewChanges(preview);
    const hasMoveItems = Array.isArray(preview.items) && preview.items.length > 0;
    const title = hasMetadataChanges && hasMoveItems ? "Move and update metadata" : hasMetadataChanges ? "Update metadata" : "Move files";
    const libraryId = typeof entry.library_id === "string" ? entry.library_id : "";
    const canConfirm = libraryCanOperate(libraryId) && !previewCancellationBusy("file_preview", entry);
    const offlineMessage = canConfirm ? "" : `<p class="field-hint">This library is offline or showing a saved scan. Reconnect it and refresh before confirming.</p>`;
    return `<article class="stack-card" id="${cardId}" tabindex="-1"><div class="stack-card-head"><div><h3>${title}</h3><p>Destination: ${escapeHtml(preview.target_relative_dir ?? "Metadata only · file locations stay the same")} · Conflict: ${escapeHtml(preview.conflict ?? "skip")}</p>${offlineMessage}</div><span class="state-pill pending">Review needed</span></div><div class="preview-table">${describePreview(preview)}</div><div class="stack-actions"><button class="small-button confirm" type="button" data-confirm="${escapeHtml(entry.id)}" data-revision="${escapeHtml(entry.revision)}" data-library-id="${escapeHtml(libraryId)}" ${canConfirm ? "" : "disabled"}>Confirm changes</button>${cancel}</div></article>`;
  }).join("");
  if (!requestCard && !omittedNotice && !pending.length) byId("preview-list").innerHTML = "";
}

function renderLegacyMappings() {
  const importSelection = selectLegacyImportForMapping(legacyOperations());
  const operation = importSelection.usable ? importSelection.operation : null;
  const container = byId("legacy-mappings");
  state.legacyOperation = operation;
  if (!importSelection.latest) {
    container.classList.add("hidden");
    state.manualPickerKey = null;
    state.manualPickerQuery = "";
    return;
  }
  if (!operation) {
    const latest = importSelection.latest;
    const detailsWarning = operationDetailsWarning(latest);
    const failure = typeof latest.error === "string" && latest.error ? `${latest.error} ` : "";
    state.legacyOperationKey = null;
    state.legacyPage = 0;
    container.classList.remove("hidden");
    container.innerHTML = `<p class="warning-text">${escapeHtml(detailsWarning || `The latest legacy import is ${statusText(latest.status)}.${failure} A completed, complete import report is required before mapping hints.`)}</p>`;
    state.manualPickerKey = null;
    state.manualPickerQuery = "";
    return;
  }
  const importIdentity = fileOperationLibraryIdentity(operation);
  if (!importIdentity || importIdentity.library_id !== activeLibraryId()
      || !fileOperationIdentityMatches(state.snapshot, operation, activeLibraryId())) {
    state.legacyOperation = null;
    state.legacyOperationKey = null;
    state.legacyPage = 0;
    container.classList.remove("hidden");
    container.innerHTML = `<p class="warning-text">This legacy import has no verified identity for the current library registration. Submit a fresh import before mapping metadata.</p>`;
    state.manualPickerKey = null;
    state.manualPickerQuery = "";
    return;
  }
  const incompleteWarning = operationDetailsWarning(operation);
  if (incompleteWarning || !Array.isArray(operation.result?.hints) || !Array.isArray(operation.result?.candidates)) {
    state.legacyOperation = null;
    state.legacyOperationKey = null;
    state.legacyPage = 0;
    container.classList.remove("hidden");
    container.innerHTML = `<p class="warning-text">${escapeHtml(incompleteWarning || "The saved legacy import report is incomplete. Submit the import again to review the full report.")}</p>`;
    state.manualPickerKey = null;
    state.manualPickerQuery = "";
    return;
  }
  const operationKey = String(operation.seq ?? operation.created_at ?? "legacy-import");
  if (state.legacyOperationKey !== operationKey) {
    state.legacyOperationKey = operationKey;
    state.legacySelection = new Map();
    state.legacyPage = 0;
    state.manualPickerKey = null;
    state.manualPickerQuery = "";
  }
  const hints = operation.result.hints;
  const choices = buildLegacyMappingChoices(operationKey, hints, operation.result.candidates, libraryItems());
  const page = paginateLegacyMappingChoices(choices, state.legacyPage, LEGACY_MAPPING_PAGE_SIZE);
  state.legacyPage = page.page;
  const warnings = Array.isArray(operation.result.warnings) ? operation.result.warnings : [];
  const validKeys = new Set(choices.map((choice) => choice.key));
  const manualKeys = new Set(choices.filter((choice) => choice.kind === "manual").map((choice) => choice.key));
  if (state.manualPickerKey && !manualKeys.has(state.manualPickerKey)) {
    state.manualPickerKey = null;
    state.manualPickerQuery = "";
  }
  container.classList.remove("hidden");
  const rows = page.choices.map((choice) => {
    const { hint, hintIndex } = choice;
    const sourceIds = Array.isArray(hint.source_ids) ? hint.source_ids : [];
    const collection = hint.collection && typeof hint.collection === "object" ? hint.collection.title : "";
    const selection = getOrCreateLegacySelection(state.legacySelection, choice.key, {
      selected: false, hintIndex, manual: choice.kind === "manual", item_id: "",
      title: Boolean(hint.title), source_ids: sourceIds.length > 0, collection: Boolean(collection),
    });
    if (choice.kind === "candidate") {
      const candidate = choice.candidate;
      const candidateError = typeof candidate.error === "string" ? candidate.error : typeof candidate.error_message === "string" ? candidate.error_message : "";
      return `<div class="mapping-row"><div class="mapping-row-head"><input type="checkbox" aria-label="Apply legacy hint to ${escapeHtml(candidate.title ?? candidate.relative_path)}" data-map-select="${escapeHtml(choice.key)}" data-hint-index="${hintIndex}" data-item-id="${escapeHtml(candidate.item_id)}" ${selection.selected ? "checked" : ""}><div><div class="mapping-name">${escapeHtml(hint.title || candidate.title || "Legacy source links")} → ${escapeHtml(candidate.title || candidate.relative_path || candidate.item_id)}</div><div class="mapping-path">${escapeHtml(candidate.relative_path ?? "Path unavailable")} · ${escapeHtml(candidate.match_kind ?? "candidate match")}${candidate.ambiguous ? " · check this match" : ""}</div>${candidateError ? `<p class="mapping-help">${escapeHtml(candidateError)}</p>` : ""}</div></div><div class="mapping-fields"><label><input type="checkbox" data-map-field="title" data-map-key="${escapeHtml(choice.key)}" ${selection.title ? "checked" : ""} ${hint.title ? "" : "disabled"}> Title</label><label><input type="checkbox" data-map-field="source_ids" data-map-key="${escapeHtml(choice.key)}" ${selection.source_ids ? "checked" : ""} ${sourceIds.length ? "" : "disabled"}> ${sourceIds.length} source link${sourceIds.length === 1 ? "" : "s"}</label><label><input type="checkbox" data-map-field="collection" data-map-key="${escapeHtml(choice.key)}" ${selection.collection ? "checked" : ""} ${collection ? "" : "disabled"}> Collection</label></div>${candidate.requires_confirmation || hint.requires_confirmation ? `<p class="mapping-help">This match needs your confirmation. Review the item path and proposed fields before continuing.</p>` : ""}<div class="mapping-actions"><button class="small-button" type="button" data-open-manual-hint="${hintIndex}">Choose a different video</button></div></div>`;
    }
    const pickerOpen = state.manualPickerKey === choice.key;
    const picker = pickerOpen ? manualTargetPickerMarkup(choice.key, selection, hintIndex) : "";
    const selectedTarget = selection.item_id ? `<p class="mapping-path">Selected video ID: ${escapeHtml(selection.item_id)}</p>` : "";
    const description = choice.hasCandidate ? "Manual replacement for the automatic match" : "No reliable automatic match · choose a present video manually";
    const pickerLabel = selection.item_id ? "Change selected video" : choice.hasCandidate ? "Choose a different video" : "Choose a video";
    return `<div class="mapping-row manual-mapping"><div class="mapping-row-head"><input type="checkbox" aria-label="Apply this legacy hint after manually choosing a video" data-map-select="${escapeHtml(choice.key)}" data-manual="true" data-hint-index="${hintIndex}" ${selection.selected ? "checked" : ""}><div><div class="mapping-name">${escapeHtml(hint.title || "Legacy hint without a reliable match")}</div><div class="mapping-path">${description}</div></div></div><button class="small-button" type="button" data-manual-picker-toggle="${escapeHtml(choice.key)}" aria-expanded="${pickerOpen}">${pickerLabel}</button>${pickerOpen ? picker : selectedTarget}<div class="mapping-fields"><label><input type="checkbox" data-map-field="title" data-map-key="${escapeHtml(choice.key)}" ${selection.title ? "checked" : ""} ${hint.title ? "" : "disabled"}> Title</label><label><input type="checkbox" data-map-field="source_ids" data-map-key="${escapeHtml(choice.key)}" ${selection.source_ids ? "checked" : ""} ${sourceIds.length ? "" : "disabled"}> ${sourceIds.length} source link${sourceIds.length === 1 ? "" : "s"}</label><label><input type="checkbox" data-map-field="collection" data-map-key="${escapeHtml(choice.key)}" ${selection.collection ? "checked" : ""} ${collection ? "" : "disabled"}> Collection</label></div><p class="mapping-help">This is a manual association. Review the selected path and generated metadata preview before continuing.</p></div>`;
  });
  const displayedRange = page.total ? `Showing rows ${page.start + 1}–${page.end} of ${page.total.toLocaleString()} · ${hints.length.toLocaleString()} hints total` : `${hints.length.toLocaleString()} hints · no current mapping choices`;
  const previousPage = page.page > 0 ? `<button class="quiet-button" type="button" data-legacy-page="-1">Previous</button>` : `<button class="quiet-button" type="button" disabled>Previous</button>`;
  const nextPage = page.page + 1 < page.pageCount ? `<button class="quiet-button" type="button" data-legacy-page="1">Next</button>` : `<button class="quiet-button" type="button" disabled>Next</button>`;
  const pagination = page.pageCount > 1 ? `<div class="mapping-pagination" aria-label="Legacy mapping pages">${previousPage}<span>Page ${page.page + 1} of ${page.pageCount}</span>${nextPage}</div>` : "";
  const newerImportNotice = importSelection.latest !== operation ? `<p class="warning-text">A newer legacy import did not save a complete successful report. Showing the latest completed import; see request history for the newer result.</p>` : "";
  const emptyMessage = hints.length === 0 ? `<p class="field-hint">No hints or possible media matches were found.</p>` : "";
  container.innerHTML = `<p class="mapping-title">Choose a video for each legacy hint</p>${newerImportNotice}${warnings.length ? `<p class="warning-text">${warnings.map(escapeHtml).join(" · ")}</p>` : ""}<p class="field-hint">${displayedRange}</p>${rows.length ? rows.join("") : emptyMessage}${pagination}<div class="mapping-actions"><button id="preview-metadata" class="primary-button" type="button" ${choices.length && importIdentity && fileOperationIdentityMatches(state.snapshot, operation, activeLibraryId()) && libraryCanOperate() ? "" : "disabled"}>Preview selected updates <span aria-hidden="true">→</span></button></div>`;
  for (const key of state.legacySelection.keys()) if (!validKeys.has(key)) state.legacySelection.delete(key);
}

function manualTargetPickerContents(selection) {
  const result = searchPresentManualTargets(libraryItems(), state.manualPickerQuery, selection.item_id);
  const selectedId = String(selection.item_id ?? "");
  const selectedPresent = selectedId && result.items.some((item) => String(item.id) === selectedId);
  const options = result.items.map((item) => {
    const id = String(item.id);
    const title = item.title || item.relative_path || id;
    return `<option value="${escapeHtml(id)}" ${id === selectedId ? "selected" : ""}>${escapeHtml(title)} · ${escapeHtml(item.relative_path || "Path unavailable")}</option>`;
  }).join("");
  const status = selection.item_id && !selectedPresent
    ? "The previous video is no longer present. Choose a current video."
    : result.hasMore
      ? `Showing up to ${MAX_MANUAL_TARGET_OPTIONS} present matches. Refine the search to narrow the list.`
      : `${result.items.length} present video${result.items.length === 1 ? "" : "s"} shown.`;
  return {
    options: `<option value="" ${selectedPresent ? "" : "selected"}>Choose a present video…</option>${options}`,
    status,
  };
}

function manualTargetPickerMarkup(key, selection, hintIndex) {
  const contents = manualTargetPickerContents(selection);
  return `<div class="manual-target-picker"><label class="field-label" for="manual-target-search-${hintIndex}">Search present videos</label><input id="manual-target-search-${hintIndex}" class="text-control" type="search" autocomplete="off" data-manual-picker-search="${escapeHtml(key)}" value="${escapeHtml(state.manualPickerQuery)}" placeholder="Title, path, or ID"><p class="field-hint" data-manual-picker-status="${escapeHtml(key)}">${escapeHtml(contents.status)}</p><select id="manual-target-${hintIndex}" class="select-control manual-target" data-manual-target="${escapeHtml(key)}">${contents.options}</select></div>`;
}

function renderOperations() {
  const operations = Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [];
  const history = operationHistoryEntries(operations).filter((entry) =>
    SETTLED_OPERATION_STATUSES.has(String(entry?.status ?? "").toLowerCase()) && historySearchMatches(entry));
  const active = operations.filter((entry) => !["file_preview", "library_migration_preview", "preview_cancel"].includes(entry?.kind)
    && !SETTLED_OPERATION_STATUSES.has(String(entry?.status ?? "").toLowerCase()));
  const renderEntries = (entries) => entries.slice().reverse().map((entry) => {
    const result = entry.result && typeof entry.result === "object" ? entry.result : {};
    const outcome = result.operation && typeof result.operation === "object" ? result.operation : result;
    const preview = result.preview && typeof result.preview === "object"
      ? result.preview
      : entry.preview && typeof entry.preview === "object" ? entry.preview : null;
    const cancelTarget = entry.kind === "preview_cancel" && result.cancelled === true
      ? `${statusText(result.preview_kind)} #${Number.isSafeInteger(result.preview_seq) ? result.preview_seq : "?"} · ${String(result.preview_id ?? "preview identity unavailable")} · revision ${String(result.revision ?? "unavailable")}`
      : "";
    const items = Array.isArray(outcome.items) ? outcome.items : Array.isArray(result.items) ? result.items : [];
    const hintCount = Array.isArray(result.hints) ? result.hints.length : 0;
    const candidateCount = Array.isArray(result.candidates) ? result.candidates.length : 0;
    const detailsWarning = operationDetailsWarning(entry);
    const movedFiles = items.reduce((total, item) => total + (Number.isSafeInteger(item.moved_files) ? item.moved_files : 0), 0);
    const totalFiles = items.reduce((total, item) => total + (Number.isSafeInteger(item.total_files) ? item.total_files : 0), 0);
    const metadataUpdated = items.filter((item) => item.metadata_patched === true).length;
    const summary = cancelTarget
      ? `Cancelled ${cancelTarget}. No changes were applied by this request.`
      : entry.kind === "file_preview" && String(entry.status).toLowerCase() === "cancelled"
        ? "This preview was cancelled before confirmation. No changes were applied."
        : items.length
      ? `${items.length} item${items.length === 1 ? "" : "s"} · ${movedFiles}/${totalFiles} related file${totalFiles === 1 ? "" : "s"} moved · ${metadataUpdated} metadata update${metadataUpdated === 1 ? "" : "s"}`
      : entry.kind === "legacy_import"
        ? detailsWarning
          ? "Legacy import report is incomplete; see the warning below."
          : `${hintCount} hints read · ${candidateCount} possible matches. Choose the matches above to prepare updates.`
          : "Request received by your computer.";
    const warnings = [
      ...(Array.isArray(entry.warnings) ? entry.warnings : []),
      ...(Array.isArray(result.warnings) ? result.warnings : []),
      ...(Array.isArray(outcome.warnings) ? outcome.warnings : []),
    ];
    const telegramWarning = result.telegram_progress_warning ?? outcome.telegram_progress_warning;
    const itemResults = items.map((item) => {
      const libraryItem = libraryItems().find((candidate) => String(candidate.id) === String(item.item_id));
      const label = libraryItem?.title || libraryItem?.relative_path || item.source_path || item.item_id || "Selected item";
      const fileProgress = Number.isSafeInteger(item.total_files) ? `${Number(item.moved_files ?? 0)}/${item.total_files} files moved` : "No files moved";
      const metadata = item.metadata_patched === true ? " · metadata updated" : "";
      return `<div class="preview-line"><span>${escapeHtml(label)}<br><span class="field-hint">${escapeHtml(fileProgress + metadata)}</span>${item.error ? `<br><span class="preview-reason">${escapeHtml(item.error)}</span>` : ""}</span><strong class="${item.error ? "skip" : "move"}">${item.error ? "Needs review" : "Done"}</strong></div>`;
    }).join("");
    const warningDetails = [...warnings, ...(typeof telegramWarning === "string" ? [telegramWarning] : [])];
    if (detailsWarning) warningDetails.push(detailsWarning);
    const sourceLibraryId = String(result.source_library_id ?? "");
    const targetLibraryId = String(result.target_library_id ?? "");
    const libraryContext = sourceLibraryId && targetLibraryId
      ? `${libraryLabel(sourceLibraryId)} → ${libraryLabel(targetLibraryId)}`
      : operationLibraryId(entry) ? libraryLabel(operationLibraryId(entry)) : "";
    const contextLabel = libraryContext ? `<p class="field-hint">${escapeHtml(libraryContext)}</p>` : "";
    const previewDetails = preview ? `<div class="preview-table">${describePreview(preview)}</div>` : "";
    const title = entry.kind === "preview_cancel" ? "Preview cancellation" : operationTitle(entry);
    return `<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(title)}</h3><p>${escapeHtml(entry.error ?? summary)}</p>${contextLabel}</div><span class="state-pill">${escapeHtml(statusText(entry.status))}</span></div>${entry.created_at ? `<p>Received ${escapeHtml(timeAgo(entry.created_at))}</p>` : ""}${previewDetails}${itemResults ? `<div class="preview-table">${itemResults}</div>` : ""}${warningDetails.length ? `<div class="operation-warnings">${warningDetails.map((warning) => `<p>${escapeHtml(warning)}</p>`).join("")}</div>` : ""}${entry.kind === "legacy_import" && result.revision ? `<p>Library revision: ${escapeHtml(result.revision)}</p>` : ""}</article>`;
  }).join("");
  byId("active-operation-list").innerHTML = renderEntries(active);
  byId("operation-list").innerHTML = renderEntries(history);
}

function renderTabCounts() {
  const operations = Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [];
  const tasks = Array.isArray(state.snapshot?.tasks) ? state.snapshot.tasks : [];
  const cloudQueue = Array.isArray(state.snapshot?.pending_requests) ? state.snapshot.pending_requests : [];
  const reviewEntries = allReviewEntries();
  const reviewSequences = new Set(reviewEntries.map(({ entry }) => entry.seq).filter((seq) => Number.isSafeInteger(seq) && seq > 0));
  const { cloud: pendingCloud, local } = buildWaitingEntries(cloudQueue, tasks, operations);
  const requestKeys = new Set();
  let rowIndex = 0;
  for (const entry of [...pendingCloud, ...local]) {
    if (!isWaitingEntryPending(entry) || Number.isSafeInteger(entry.seq) && reviewSequences.has(entry.seq)) continue;
    requestKeys.add(Number.isSafeInteger(entry.seq) && entry.seq > 0 ? `seq:${entry.seq}` : `row:${rowIndex++}`);
  }
  for (const { kind, entry } of reviewEntries) {
    const key = previewCancellationKey(previewCancellationPayload(kind, entry));
    const tracker = key ? state.previewCancellations.get(key) : null;
    requestKeys.add(Number.isSafeInteger(entry.seq) && entry.seq > 0
      ? `seq:${entry.seq}`
      : `preview:${kind}:${entry.id}:${entry.revision}`);
    if (!isActivePreviewCancellation(tracker, key)) continue;
    if (Number.isSafeInteger(tracker?.seq) && tracker.seq > 0) requestKeys.add(`seq:${tracker.seq}`);
    else if (tracker?.requestId) requestKeys.add(`request:${tracker.requestId}`);
    else requestKeys.add(`cancel:${key}`);
  }
  for (const action of readActionQueue()) requestKeys.add(`request:${action.request_id}`);
  const recovery = state.metadataRecoveryPreviewRequest;
  if (recovery) {
    requestKeys.add(Number.isSafeInteger(recovery.seq) && recovery.seq > 0 ? `seq:${recovery.seq}` : `request:${recovery.requestId}`);
  }
  for (const [key, tracker] of state.previewCancellations) {
    if (!isActivePreviewCancellation(tracker, key)) continue;
    if (Number.isSafeInteger(tracker.seq) && tracker.seq > 0) requestKeys.add(`seq:${tracker.seq}`);
    else if (tracker.requestId) requestKeys.add(`request:${tracker.requestId}`);
    else requestKeys.add(`cancel:${key}`);
  }
  const omittedPreviews = omittedPreviewAggregate();
  byId("waiting-count").textContent = omittedPreviews ? `${requestKeys.size}+` : String(requestKeys.size);

  const activeOperations = operations.filter((entry) => !["file_preview", "library_migration_preview", "preview_cancel"].includes(entry?.kind)
    && !SETTLED_OPERATION_STATUSES.has(String(entry?.status ?? "").toLowerCase()));
  const retained = retainedMigrationState();
  const activeMigrations = retained.migrations.filter((migration) =>
    (migration.source_library_id === activeLibraryId() || migration.target_library_id === activeLibraryId())
    && (migrationControlActions(migration).length > 0 || ["queued", "running", "paused"].includes(String(migration.status ?? "").toLowerCase())));
  byId("organizing-count").textContent = String(activeOperations.length + activeMigrations.length);

  const historyOperations = operationHistoryEntries(operations)
    .filter((entry) => SETTLED_OPERATION_STATUSES.has(String(entry?.status ?? "").toLowerCase()));
  const historyMigrations = retained.migrations.filter((migration) =>
    migrationControlActions(migration).length === 0 && !["queued", "running", "paused"].includes(String(migration.status ?? "").toLowerCase()));
  const visibleHistoryCount = historyOperations.filter(historySearchMatches).length
    + historyMigrations.filter(historySearchMatches).length;
  const retainedHistoryCount = historyOperations.length + historyMigrations.length;
  byId("history-count").textContent = String(retainedHistoryCount);
  const query = String(state.historyQuery ?? "").trim();
  byId("history-summary").textContent = query
    ? `${visibleHistoryCount} matching recent record${visibleHistoryCount === 1 ? "" : "s"} · ${retainedHistoryCount} retained.`
    : retainedHistoryCount
      ? `${retainedHistoryCount} recent record${retainedHistoryCount === 1 ? "" : "s"} retained in this snapshot.`
      : "No recent activity is available in the current snapshot.";
}

function renderSync() {
  const snapshot = snapshotValue();
  const localLibrary = activeLibrary();
  const librarySnapshot = localLibrary.snapshot;
  const localAvailability = mediaLibraryAvailability(localLibrary.descriptor);
  const savedLibraryView = localLibrary.descriptor?.snapshot_stale === true;
  const synced = snapshot.synced === true;
  const title = byId("sync-title");
  const detail = byId("sync-detail");
  const badge = byId("sync-badge");
  badge.className = `badge${synced && !snapshot.stale ? " good" : synced ? " warning" : ""}`;
  if (!synced) {
    title.textContent = "Waiting for your computer";
    detail.textContent = state.snapshot?.cached_at ? "Showing your last saved view while we reconnect." : "Your library appears here after the desktop app connects.";
    badge.textContent = "Not synced";
    return;
  }
  const firstScanPending = !librarySnapshot || librarySnapshot.ready === false || Number(librarySnapshot.scanned_at) === 0;
  title.textContent = localAvailability === "offline"
    ? "This media root is offline"
    : savedLibraryView ? "Showing a saved scan while this root refreshes"
      : snapshot.stale ? "No recent computer contact" : firstScanPending ? "Connected · first scan pending" : "Your library is synced";
  detail.textContent = `Root: ${localLibrary.descriptor?.root || "not reported"} · Last contact ${timeAgo(snapshot.last_seen)} · Last scan ${firstScanPending ? "not completed" : formatTime(librarySnapshot?.scanned_at, "seconds")}${savedLibraryView ? " · Cached items are read-only until this root scans again." : ""}`;
  badge.textContent = localAvailability === "offline" ? "Drive offline" : savedLibraryView ? "Saved scan" : snapshot.stale ? "Contact overdue" : "Up to date";
}

function renderDetail() {
  const old = document.querySelector(".detail-panel");
  if (old) old.remove();
  if (!state.detailId) return;
  const item = libraryItems().find((entry) => String(entry.id) === state.detailId);
  if (!item) { state.detailId = null; return; }
  const panel = document.createElement("div");
  panel.className = "detail-panel";
  panel.setAttribute("role", "presentation");
  const attachments = Array.isArray(item.attachments) ? item.attachments : [];
  const sourceIds = Array.isArray(item.source_ids) ? item.source_ids : [];
  const collection = item.collection && typeof item.collection === "object" ? item.collection.title : item.collection;
  const root = String(activeLibrary().descriptor?.root ?? state.snapshot?.settings?.download_dir ?? "").replace(/[\\/]+$/u, "");
  const pathJoin = (relative) => root ? `${root}${root.includes("\\") && !root.includes("/") ? "\\" : "/"}${relative}` : "";
  const relativePath = String(item.relative_path ?? "");
  const fullPath = pathJoin(relativePath);
  const organizationStatus = String(item.organization_status ?? "unreviewed").toLowerCase();
  const hasCollection = Boolean(item.collection && (typeof item.collection === "string" ? item.collection.trim() : item.collection.title));
  const organizationControls = organizationStatus === "standalone"
    ? `<button class="small-button detail-organization-action" type="button" data-organization-status="unreviewed" data-organization-item="${escapeHtml(item.id)}" ${libraryCanOperate() ? "" : "disabled"}>Undo standalone declaration</button><p class="field-hint">Undoing the declaration restores the usual collection review check.</p>`
    : hasCollection
      ? `<p class="field-hint">This item already has a collection. Existing collections cannot be declared standalone.</p>`
      : `<button class="small-button detail-organization-action" type="button" data-organization-status="standalone" data-organization-item="${escapeHtml(item.id)}" ${libraryCanOperate() ? "" : "disabled"}>Mark as standalone</button><p class="field-hint">Standalone status only clears the missing-collection check. Missing, unavailable, incomplete, and low-confidence items still need review.</p>`;
  panel.innerHTML = `
    <aside class="detail-sheet" role="dialog" aria-modal="true" aria-label="File details">
      <div class="detail-top"><div><p class="eyebrow">FILE DETAILS</p><h2>${escapeHtml(item.title || "Untitled video")}</h2></div><button class="detail-close" type="button" aria-label="Close details">×</button></div>
      <section class="detail-section path-section">
        <h3>Relative path</h3><div class="copy-row"><code>${escapeHtml(relativePath || "Path unavailable")}</code><button class="small-button" type="button" data-copy="${escapeHtml(relativePath)}">Copy</button></div>
        ${fullPath ? `<h3>Full path</h3><div class="copy-row"><code>${escapeHtml(fullPath)}</code><button class="small-button" type="button" data-copy="${escapeHtml(fullPath)}">Copy</button></div>` : `<p class="field-hint">Full path is unavailable until the desktop reports its library root.</p>`}
      </section>
      <div class="detail-grid">
        <div class="detail-stat"><span>File size</span><strong>${formatBytes(item.bytes)}</strong></div>
        <div class="detail-stat"><span>File availability</span><strong>${escapeHtml(statusOf(item))}</strong></div>
        <div class="detail-stat"><span>Source availability</span><strong>Not checked</strong></div>
        <div class="detail-stat"><span>Resolution</span><strong>${item.width && item.height ? `${escapeHtml(item.width)} × ${escapeHtml(item.height)}` : "Unknown"}</strong></div>
        <div class="detail-stat"><span>Codec</span><strong>${escapeHtml(item.codec || "Unknown")}</strong></div>
        <div class="detail-stat"><span>Metadata</span><strong>${escapeHtml(item.metadata_status ?? "Unknown")}</strong></div>
        <div class="detail-stat"><span>Confidence</span><strong>${escapeHtml(item.confidence ?? "Unknown")}</strong></div>
        <div class="detail-stat"><span>Organization</span><strong>${escapeHtml(organizationStatus)}</strong></div>
        <div class="detail-stat"><span>Integrity</span><strong>${escapeHtml(item.integrity_status ?? "Unknown")}</strong></div>
        <div class="detail-stat"><span>Media + related files</span><strong>${1 + attachments.length} files</strong></div>
      </div>
      <section class="detail-section"><h3>Organization status</h3>${organizationControls}</section>
      ${collection ? `<section class="detail-section"><h3>Collection</h3><div class="attachment">${escapeHtml(collection)}</div></section>` : ""}
      <section class="detail-section"><h3>Source links (${sourceIds.length})</h3>${sourceIds.length ? sourceIds.map((source) => { const url = sourceIdUrl(source); return `<div class="attachment">${url ? `<a href="${url}" target="_blank" rel="noopener noreferrer">${escapeHtml(source)}</a>` : escapeHtml(source)}</div>`; }).join("") : `<div class="attachment">No source links recorded.</div>`}<p class="field-hint">Remote source availability has not been checked.</p></section>
      <section class="detail-section"><h3>Related files (${attachments.length})</h3>${attachments.length ? attachments.map((attachment) => { const rel = attachment.relative_path ?? attachment.path ?? "Related file"; const absolute = pathJoin(rel); return `<div class="attachment"><div class="copy-row"><code>${escapeHtml(rel)}</code><button class="small-button" type="button" data-copy="${escapeHtml(rel)}">Copy relative</button></div>${absolute ? `<div class="copy-row"><code>${escapeHtml(absolute)}</code><button class="small-button" type="button" data-copy="${escapeHtml(absolute)}">Copy full</button></div>` : ""}<span>${escapeHtml(attachment.kind ?? "attachment")}</span></div>`; }).join("") : `<div class="attachment">No attachments found.</div>`}</section>
    </aside>`;
  document.body.append(panel);
  panel.addEventListener("click", (event) => { if (event.target === panel || event.target.closest(".detail-close")) { state.detailId = null; renderDetail(); } });
}

function render() {
  if (!mayUsePrivateSnapshot(state)) return;
  reconcilePreviewCancellations(state.snapshot);
  renderSync();
  renderLibrary();
  settleDownloadSubmission();
  renderDownloadStatus();
  renderWaiting();
  renderMigrations();
  renderPreviews();
  renderReviewNavigation();
  renderLegacyMappings();
  renderOperations();
  renderTabCounts();
  renderQueueCount();
  for (const tab of document.querySelectorAll(".view-tab")) {
    const active = tab.dataset.view === state.view;
    tab.classList.toggle("active", active);
    if (active) tab.setAttribute("aria-current", "page"); else tab.removeAttribute("aria-current");
  }
  for (const panel of document.querySelectorAll(".view-panel")) panel.classList.toggle("hidden", panel.id !== `view-${state.view}`);
  renderDetail();
}

function previewSelectedLegacy() {
  const operation = state.legacyOperation;
  if (!operation) return;
  const importIdentity = fileOperationLibraryIdentity(operation);
  if (!importIdentity || importIdentity.library_id !== activeLibraryId()
      || !fileOperationIdentityMatches(state.snapshot, operation, activeLibraryId())) {
    return showToast("This legacy import no longer matches the displayed library registration. Refresh the library and submit a fresh import.");
  }
  const choices = buildLegacyMappingChoices(
    state.legacyOperationKey,
    operation.result.hints,
    operation.result.candidates,
    libraryItems(),
  );
  const result = buildLegacyMetadataPatches({
    hints: operation.result.hints,
    choices,
    selections: state.legacySelection,
    items: libraryItems(),
    limit: MAX_SELECTED_ITEMS,
  });
  const messages = {
    "empty-selection": "Select at least one explicit hint-to-video match.",
    "selection-limit": `Choose at most ${MAX_SELECTED_ITEMS} metadata matches per request.`,
    "duplicate-hint": "Choose at most one video for each legacy hint.",
    "manual-target-required": "Choose a present video for every manual match.",
    "target-not-present": "This video is no longer present. Choose a current video manually.",
    "duplicate-target": "Choose each video once; combine its changes in one mapping.",
    "invalid-hint": "This legacy hint is no longer available. Refresh the import report.",
    "empty-fields": "Choose at least one metadata field to apply.",
  };
  if (result.error) return showToast(messages[result.error] ?? "Review the selected legacy mappings.");
  const payload = buildLegacyMetadataPreviewPayloadFromRenderedOption({
    ownerSnapshot: state.snapshot,
    libraryOption: byId("library-select")?.selectedOptions?.[0],
    importOperation: operation,
    fields: { metadata_patches: result.patches, rename: false, conflict: "skip" },
  });
  if (!payload) return showToast("The displayed library registration changed. Refresh and submit a fresh import before previewing metadata.");
  submitAction("file_preview", payload);
}

function selectVisible() {
  if (!libraryCanOperate()) return showToast("This library is offline or showing a saved scan. Reconnect it before selecting files to change.");
  const scope = state.view === "needs" ? "needs" : "library";
  const visible = presentMediaItems(pageItemsForScope(scope));
  const allSelected = visible.length > 0 && visible.every((item) => state.selected.has(String(item.id)));
  const omittedCount = updateSelectedItems(visible.map((item) => String(item.id)), !allSelected);
  state.groupSelectionLimitNotice = null;
  renderLibrary();
  if (omittedCount > 0) showToast(selectionLimitMessage(omittedCount));
}

function openMovedTarget(button) {
  const sourceLibraryId = button.dataset.sourceLibraryId;
  const sourceItemId = button.dataset.sourceItemId;
  if (!sourceLibraryId || sourceLibraryId !== activeLibraryId()) {
    showToast("This migration link no longer matches the displayed source library. Refresh the view.");
    return;
  }
  const sourceItem = libraryItems().find((item) => String(item.id) === sourceItemId);
  const target = movedToTarget(sourceItem);
  if (!target
      || target.library_id !== button.dataset.targetLibraryId
      || target.item_id !== button.dataset.targetItemId
      || target.relative_path !== button.dataset.targetRelativePath) {
    showToast("This migration destination changed in the latest library snapshot. Refresh the view.");
    return;
  }
  const targetLibrary = mediaLibraryEntries(state.snapshot).find((entry) => entry.id === target.library_id);
  if (!targetLibrary) {
    showToast("The destination library is no longer registered, so this item cannot be opened from the current view.");
    return;
  }

  state.libraryId = target.library_id;
  state.view = "library";
  clearSelectedItems();
  state.detailId = null;
  state.query = "";
  state.libraryPage = 0;
  state.needsPage = 0;
  byId("search-input").value = "";
  byId("category-filter").value = "";
  byId("status-filter").value = "";

  const targetSnapshot = resolveMediaLibrary(state.snapshot, target.library_id).snapshot;
  const targetItems = Array.isArray(targetSnapshot?.items) ? targetSnapshot.items : [];
  const matchingItems = filterLibraryItems(targetItems);
  const targetIndex = matchingItems.findIndex((item) => String(item.id) === target.item_id);
  let destinationMessage = "The destination item is not in the retained cloud list yet. The search and filters were cleared; refresh the target library to locate it.";
  if (targetIndex >= 0) {
    state.libraryPage = Math.floor(targetIndex / LIBRARY_PAGE_SIZE);
    const currentTarget = matchingItems[targetIndex];
    if (isPresentMedia(currentTarget) && String(currentTarget.relative_path ?? "") === target.relative_path) {
      state.detailId = target.item_id;
      destinationMessage = `Located the target item in ${targetLibrary.name || target.library_name}.`;
    } else if (String(currentTarget.relative_path ?? "") !== target.relative_path) {
      destinationMessage = "The destination ID appears at a different path in this saved view. Refresh the target library before opening it.";
    } else {
      destinationMessage = "The destination row is not currently marked present in this saved view. Refresh the target library before opening it.";
    }
  }
  syncLibraryUrl();
  render();
  showToast(destinationMessage);
}

function bindEvents() {
  byId("refresh-button").addEventListener("click", () => refreshState());
  byId("scan-button").addEventListener("click", () => submitAction("library_scan", { library_id: activeLibraryId() }));
  document.querySelectorAll("[data-scan]").forEach((button) => button.addEventListener("click", () => submitAction("library_scan", { library_id: activeLibraryId() })));
  byId("library-select").addEventListener("change", () => {
    state.libraryId = byId("library-select").value;
    clearSelectedItems();
    clearMetadataRecoveryState({ preservePreviewRequest: true });
    state.detailId = null;
    state.legacyOperation = null;
    state.legacyOperationKey = null;
    state.legacySelection.clear();
    state.manualPickerKey = null;
    state.libraryPage = 0;
    state.needsPage = 0;
    cachedFolderLibrary = null;
    syncLibraryUrl();
    render();
  });
  byId("recover-library").addEventListener("click", () => requestMetadataRecovery("library"));
  byId("recover-selected").addEventListener("click", () => requestMetadataRecovery("selected"));
  byId("download-here").addEventListener("click", () => openDownloadForm());
  byId("download-close").addEventListener("click", closeDownloadForm);
  byId("download-cancel").addEventListener("click", closeDownloadForm);
  byId("download-clear-wait").addEventListener("click", () => {
    const queuedUnknown = unknownFileActionForKind(readActionQueue(), "download_submit", state.unknownActionOutcomes);
    if (state.downloadSubmission?.outcomeUnknown === true || queuedUnknown) {
      const requestId = state.downloadSubmission?.outcomeUnknown === true
        ? state.downloadSubmission.request_id
        : queuedUnknown?.request_id;
      if (!requestId) return;
      const dismissed = dismissUnknownFileAction(state, requestId, readActionQueue(), writeActionQueue);
      if (dismissed.status !== "dismissed") {
        showToast("The delivery notice could not be removed from browser storage. It remains paused here.");
        return;
      }
      renderDownloadStatus();
      renderWaiting();
      showToast(state.downloadMessage);
      return;
    }
    if (!releaseDownloadSubmissionTracking(state, state.snapshot)) {
      renderDownloadStatus();
      return;
    }
    renderDownloadStatus();
    showToast(state.downloadMessage);
  });
  byId("download-dialog").addEventListener("click", (event) => { if (event.target === byId("download-dialog")) closeDownloadForm(); });
  byId("download-library-select").addEventListener("change", () => {
    updateDownloadFormOptions();
    if (byId("download-category").dataset.userEdited !== "true") {
      const descriptor = mediaLibraryEntries(state.snapshot).find((entry) => entry.id === byId("download-library-select").value);
      byId("download-category").value = String(descriptor?.default_category ?? "");
    }
    const url = new URL(window.location.href);
    url.searchParams.set("download", "1");
    url.searchParams.set("library_id", byId("download-library-select").value);
    if (byId("download-category").value) url.searchParams.set("category", byId("download-category").value);
    else url.searchParams.delete("category");
    window.history.replaceState(null, "", url.toString());
    renderDownloadStatus();
  });
  byId("download-category").addEventListener("input", () => {
    byId("download-category").dataset.userEdited = "true";
    const url = new URL(window.location.href);
    url.searchParams.set("download", "1");
    url.searchParams.set("library_id", byId("download-library-select").value);
    if (byId("download-category").value) url.searchParams.set("category", byId("download-category").value);
    else url.searchParams.delete("category");
    window.history.replaceState(null, "", url.toString());
  });
  byId("download-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const url = byId("download-url").value.trim();
    const category = byId("download-category").value.trim();
    const { directory, error } = validateTargetRelativeDirectory(category);
    if (error) return showToast(error);
    if (!url) return showToast("Enter a video or collection URL first.");
    const selectedOption = byId("download-library-select").selectedOptions[0];
    const payload = buildDownloadSubmitPayload({ url, selectedOption, category: directory });
    if (!payload) {
      state.downloadMessage = "This library registration identity is unavailable. Refresh the page before adding a download.";
      renderDownloadStatus();
      return showToast(state.downloadMessage);
    }
    state.downloadMessage = null;
    submitAction("download_submit", payload);
  });
  byId("migration-scope").addEventListener("change", renderMigrations);
  byId("migration-target").addEventListener("change", renderMigrations);
  byId("migration-preview").addEventListener("click", () => {
    const sourceOption = byId("library-select").selectedOptions[0];
    const targetOption = byId("migration-target").selectedOptions[0];
    const sourceId = sourceOption?.value ?? "";
    const targetId = targetOption?.value ?? "";
    const scope = byId("migration-scope").value;
    if (!sourceId || !targetId || sourceId === targetId) return showToast("Choose a different registered target library.");
    if (scope === "selected" && state.selected.size === 0) return showToast("Select media or collections in the Library view first.");
    if (state.selected.size > MAX_SELECTED_ITEMS) return showToast(`Choose at most ${MAX_SELECTED_ITEMS} media items per migration preview.`);
    const payload = buildMigrationPreviewPayloadFromRenderedOptions({
      sourceOption,
      targetOption,
      scope,
      itemIds: [...state.selected],
      includeIncompleteTasks: !byId("migration-task-option").classList.contains("hidden") && byId("migration-include-tasks").checked,
      conflict: byId("migration-conflict").value,
      setTargetDefaultOnSuccess: byId("migration-set-default").checked,
    });
    if (!payload) return showToast("A displayed source or target library identity is unavailable. Refresh the page before requesting a preview.");
    submitAction("library_migration_preview", payload);
  });
  byId("search-input").addEventListener("input", (event) => { state.query = event.target.value; state.libraryPage = 0; state.needsPage = 0; state.groupSelectionLimitNotice = null; renderLibrary(); });
  byId("history-search").addEventListener("input", (event) => { state.historyQuery = event.target.value; render(); });
  byId("category-filter").addEventListener("input", () => { state.libraryPage = 0; state.needsPage = 0; state.groupSelectionLimitNotice = null; renderLibrary(); });
  byId("status-filter").addEventListener("change", () => { state.libraryPage = 0; state.needsPage = 0; state.groupSelectionLimitNotice = null; renderLibrary(); });
  byId("select-visible").addEventListener("click", selectVisible);
  byId("organize-selected").addEventListener("click", () => {
    if (!state.selected.size) return;
    if (!libraryCanOperate()) return showToast("This library is offline or showing a saved scan. Reconnect it before changing files.");
    state.view = "organizing";
    render();
    const moveCard = byId("move-files-card");
    moveCard?.scrollIntoView({ behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth", block: "start" });
    moveCard?.focus({ preventScroll: true });
  });
  byId("clear-selection").addEventListener("click", () => {
    clearSelectedItems();
    render();
  });
  byId("target-dir").addEventListener("input", renderFilters);
  byId("root-target").addEventListener("click", () => { byId("target-dir").value = ""; renderFilters(); byId("target-dir").focus(); });
  byId("preview-move").addEventListener("click", () => {
    if (!libraryCanOperate()) return showToast("This library is offline or showing a saved scan. Reconnect it before changing files.");
    if (state.selected.size > MAX_SELECTED_ITEMS) return showToast(`Choose at most ${MAX_SELECTED_ITEMS} videos per request.`);
    const { directory, error } = validateTargetRelativeDirectory(byId("target-dir").value);
    if (error) return showToast(error);
    const libraryOption = byId("library-select")?.selectedOptions?.[0];
    const payload = buildSelectedFilePreviewPayloadFromRenderedOption({
      state,
      ownerSnapshot: state.snapshot,
      libraryOption,
      fields: { target_relative_dir: directory, rename: byId("rename-toggle").checked, conflict: byId("conflict-select").value },
    });
    if (!payload) return showToast("The displayed library identity is unavailable or changed. Refresh the page before previewing file changes.");
    submitAction("file_preview", payload);
  });
  byId("legacy-text").addEventListener("input", (event) => { byId("legacy-size").textContent = `${new TextEncoder().encode(event.target.value).byteLength.toLocaleString()} / 256 KB`; });
  byId("import-legacy").addEventListener("click", async () => {
    if (!libraryCanOperate()) return showToast("This library is offline or showing a saved scan. Reconnect it before importing metadata.");
    const importIdentityPayload = filePreviewPayload({});
    if (!importIdentityPayload) return showToast("The displayed library identity is unavailable or changed. Refresh the page before importing metadata.");
    let text = byId("legacy-text").value;
    const fileInput = byId("legacy-file");
    if (!text && fileInput?.files?.[0]) {
      return submitLegacyFileImport(
        fileInput.files[0],
        (value) => submitAction("legacy_import", { text: value, library_id: importIdentityPayload.library_id, root_binding: importIdentityPayload.root_binding }),
        showToast,
        MAX_LEGACY_BYTES,
      );
    }
    const bytes = new TextEncoder().encode(text).byteLength;
    if (bytes === 0) return showToast("Paste legacy text or choose a text file first.");
    if (bytes > MAX_LEGACY_BYTES) return showToast("Legacy text exceeds the 256 KB limit.");
    submitAction("legacy_import", { text, library_id: importIdentityPayload.library_id, root_binding: importIdentityPayload.root_binding });
  });
  byId("legacy-text").addEventListener("paste", async (event) => {
    if (byId("legacy-text").value) return;
    const clipboard = event.clipboardData?.getData("text/plain");
    if (clipboard && new TextEncoder().encode(clipboard).byteLength <= MAX_LEGACY_BYTES) {
      window.setTimeout(() => { byId("legacy-size").textContent = `${new TextEncoder().encode(byId("legacy-text").value).byteLength.toLocaleString()} / 256 KB`; }, 0);
    }
  });
  document.body.addEventListener("input", (event) => {
    const search = event.target.closest("[data-manual-picker-search]");
    if (!search) return;
    state.manualPickerQuery = search.value;
    const key = search.dataset.manualPickerSearch;
    const selection = state.legacySelection.get(key) ?? { item_id: "" };
    const contents = manualTargetPickerContents(selection);
    const picker = search.closest(".manual-target-picker");
    const target = picker?.querySelector("[data-manual-target]");
    const status = picker?.querySelector("[data-manual-picker-status]");
    if (target) target.innerHTML = contents.options;
    if (status) status.textContent = contents.status;
  });
  document.body.addEventListener("change", (event) => {
    const recoveryCandidate = event.target.closest("[data-recovery-candidate]");
    if (recoveryCandidate) {
      const recovery = currentMetadataRecovery();
      const itemId = recoveryCandidate.dataset.recoveryCandidate;
      const index = recoveryCandidate.value === "" ? null : Number(recoveryCandidate.value);
      const item = recovery?.report?.items.find((entry) => entry.item_id === itemId);
      if (!recovery || !item || item.status !== "needs_confirmation"
          || index !== null && (!Number.isSafeInteger(index) || index < 0 || index >= (item.candidates?.length ?? 0))) {
        renderMetadataRecovery();
        return;
      }
      if (index === null) recovery.candidateChoices.delete(itemId);
      else recovery.candidateChoices.set(itemId, index);
      renderMetadataRecovery();
      return;
    }
    const recoveryItem = event.target.closest("[data-recovery-item]");
    if (recoveryItem) {
      const recovery = currentMetadataRecovery();
      const itemId = recoveryItem.dataset.recoveryItem;
      const item = recovery?.report?.items.find((entry) => entry.item_id === itemId);
      if (!recovery || !item || !["ready", "needs_confirmation"].includes(item.status)) {
        renderMetadataRecovery();
        return;
      }
      if (recoveryItem.checked) recovery.selectedIds.add(itemId);
      else recovery.selectedIds.delete(itemId);
      renderMetadataRecovery();
      return;
    }
    const migrationConflict = event.target.closest("[data-migration-conflict-item]");
    if (migrationConflict) {
      const previewId = migrationConflict.dataset.migrationPreviewId;
      const revision = migrationConflict.dataset.migrationPreviewRevision;
      const itemId = migrationConflict.dataset.migrationConflictItem;
      const operation = migrationPreviewEntries().find((entry) => entry.id === previewId && entry.revision === revision);
      if (!operation) return;
      if (previewCancellationBusy("library_migration_preview", operation)) {
        renderMigrations();
        return;
      }
      const result = operation?.result ?? {};
      const existing = migrationConflictDraft(previewId, revision);
      const defaultPolicy = result.conflict === "keep_both" ? "keep_both" : "skip";
      const current = normalizeMigrationItemConflicts(existing?.itemConflicts ?? result.item_conflicts);
      const next = setMigrationConflictChoice(current, itemId, migrationConflict.value, defaultPolicy);
      if (next[itemId] !== (migrationConflict.value === defaultPolicy ? undefined : migrationConflict.value)) {
        migrationConflict.value = migrationConflictPolicy(result, itemId, current);
        showToast(`Per-bundle conflict overrides are limited to ${MAX_MIGRATION_ITEM_CONFLICTS.toLocaleString()} bundles.`);
        return;
      }
      const choicesChanged = migrationConflictChoicesChanged(result.item_conflicts, next);
      const updatedDraft = { ...existing, previewId, revision, itemConflicts: next, page: existing?.page ?? 0, refreshError: null };
      const draftHasPendingWork = Boolean(updatedDraft.refreshRequestId || Number.isSafeInteger(updatedDraft.refreshSeq) || updatedDraft.refreshError);
      if (choicesChanged || updatedDraft.page > 0 || draftHasPendingWork) {
        const key = saveMigrationConflictDraft(previewId, revision, updatedDraft);
        if (choicesChanged) state.migrationConflictInvalidated.add(key);
        else state.migrationConflictInvalidated.delete(key);
      } else {
        deleteMigrationConflictDraft(previewId, revision);
      }
      renderMigrations();
      return;
    }
    const mapCheckbox = event.target.closest("[data-map-select]");
    const mapField = event.target.closest("[data-map-field]");
    const manualTarget = event.target.closest("[data-manual-target]");
    if (mapCheckbox || mapField || manualTarget) {
      const key = mapCheckbox?.dataset.mapSelect ?? mapField?.dataset.mapKey ?? manualTarget?.dataset.manualTarget;
      if (mapCheckbox) {
        const result = selectLegacyMappingChoice(
          state.legacySelection,
          key,
          Number(mapCheckbox.dataset.hintIndex),
          mapCheckbox.checked,
          MAX_SELECTED_ITEMS,
        );
        if (!result.ok) {
          mapCheckbox.checked = false;
          if (result.reason === "limit") showToast(selectionLimitMessage(1));
          else showToast("This legacy mapping is no longer available. Refresh the import report.");
        }
      }
      if (mapField) updateLegacySelection(state.legacySelection, key, { [mapField.dataset.mapField]: mapField.checked });
      if (manualTarget) updateLegacySelection(state.legacySelection, key, { item_id: manualTarget.value });
      return;
    }
    const groupCheckbox = event.target.closest("[data-select-group]");
    if (groupCheckbox) {
      if (!libraryCanOperate()) { renderLibrary(); return; }
      const groupIdentity = groupCheckbox.dataset.selectGroup;
      const scope = state.view === "needs" ? "needs" : "library";
      const visibleItems = pageItemsForScope(scope);
      const groupPage = scope === "needs" ? state.needsPage : state.libraryPage;
      const groupItems = presentMediaItems(visibleItems.filter((item) => collectionIdentity(item?.collection) === groupIdentity));
      const omittedCount = updateSelectedItems(groupItems.map((item) => String(item.id)), groupCheckbox.checked);
      state.groupSelectionLimitNotice = omittedCount > 0 ? { scope, groupKey: groupIdentity, page: groupPage, omittedCount } : null;
      renderLibrary();
      if (omittedCount > 0) showToast(`${selectionLimitMessage(omittedCount)} Group skipped counts are for files that are not present.`);
      return;
    }
    const checkbox = event.target.closest("[data-select-item]");
    if (!checkbox) return;
    if (!libraryCanOperate()) { renderLibrary(); return; }
    const itemId = checkbox.dataset.selectItem;
    const item = libraryItems().find((entry) => String(entry.id) === String(itemId));
    state.groupSelectionLimitNotice = null;
    if (!isPresentMedia(item)) {
      state.selected.delete(itemId);
      renderLibrary();
      return;
    }
    const omittedCount = updateSelectedItems([itemId], checkbox.checked);
    renderLibrary();
    if (omittedCount > 0) showToast(selectionLimitMessage(omittedCount));
  });
  document.body.addEventListener("click", (event) => {
    const reviewLibrary = event.target.closest("[data-review-library]");
    if (reviewLibrary) {
      const libraryId = reviewLibrary.dataset.reviewLibrary;
      const library = registeredMediaLibrary(state.snapshot, libraryId);
      const option = [...byId("library-select").options].find((entry) => entry.value === libraryId);
      if (!library || !option) {
        showToast("This preview's registered library is not available in the current snapshot.");
        return;
      }
      state.libraryId = libraryId;
      byId("library-select").value = libraryId;
      clearSelectedItems();
      clearMetadataRecoveryState({ preservePreviewRequest: true });
      state.detailId = null;
      state.legacyOperation = null;
      state.legacyOperationKey = null;
      state.legacySelection.clear();
      state.manualPickerKey = null;
      state.libraryPage = 0;
      state.needsPage = 0;
      cachedFolderLibrary = null;
      state.view = "waiting";
      syncLibraryUrl();
      render();
      const kind = reviewLibrary.dataset.reviewPreviewKind;
      const sequence = Number(reviewLibrary.dataset.reviewPreviewSeq);
      const previewId = reviewLibrary.dataset.reviewPreviewId ?? "";
      const revision = reviewLibrary.dataset.reviewPreviewRevision ?? "";
      if (kind && previewId && revision && Number.isSafeInteger(sequence) && sequence > 0) {
        const card = document.getElementById(previewCardId(kind, { seq: sequence, id: previewId, revision }));
        card?.scrollIntoView?.({ behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth", block: "start" });
        card?.focus?.({ preventScroll: true });
      }
      return;
    }
    const cancelDismiss = event.target.closest("[data-preview-cancel-dismiss-kind]");
    if (cancelDismiss) {
      const kind = cancelDismiss.dataset.previewCancelDismissKind;
      const payload = {
        preview_kind: kind,
        preview_seq: Number(cancelDismiss.dataset.previewCancelDismissSeq),
        preview_id: cancelDismiss.dataset.previewCancelDismissId,
        revision: cancelDismiss.dataset.previewCancelDismissRevision,
      };
      const key = previewCancellationKey(payload);
      const tracker = key ? state.previewCancellations.get(key) : null;
      if (tracker?.requestId) {
        const dismissed = dismissUnknownFileAction(state, tracker.requestId, readActionQueue(), writeActionQueue);
        if (dismissed.status !== "dismissed") {
          showToast("The cancellation notice could not be removed from browser storage. It remains paused here.");
          return;
        }
      } else if (tracker) {
        tracker.requestId = null;
        tracker.seq = null;
        tracker.status = "failed";
        tracker.error = "The unknown delivery notice was dismissed after checking the computer.";
      }
      persistPreviewCancellations();
      render();
      showToast("The cancellation result is still unknown. Check your computer before trying again.");
      return;
    }
    const cancelPreview = event.target.closest("[data-preview-cancel]");
    if (cancelPreview) {
      const kind = cancelPreview.dataset.previewCancelKind;
      const payload = {
        preview_kind: kind,
        preview_seq: Number(cancelPreview.dataset.previewCancelSeq),
        preview_id: cancelPreview.dataset.previewCancelId,
        revision: cancelPreview.dataset.previewCancelRevision,
      };
      const review = reviewTargetForCancellation(payload);
      if (!review) {
        showToast("This preview is no longer awaiting review. Refresh the Requests view before cancelling it.");
        return;
      }
      const cancellation = previewCancellationTracker(kind, review.entry);
      if (!cancellation || state.previewCancellationAttempts.has(cancellation.key)
          || ["queued", "submitted", "unknown"].includes(cancellation.tracker?.status)) return;
      const previousTracker = cancellation.tracker ? { ...cancellation.tracker } : null;
      const requestId = crypto.randomUUID();
      state.previewCancellations.set(cancellation.key, {
        requestId,
        seq: null,
        status: "queued",
        error: "",
      });
      if (!persistPreviewCancellations()) {
        if (previousTracker) state.previewCancellations.set(cancellation.key, previousTracker);
        else state.previewCancellations.delete(cancellation.key);
        render();
        showToast("This browser could not safely save the cancellation request, so it was not sent.");
        return;
      }
      state.previewCancellationAttempts.add(cancellation.key);
      cancelPreview.disabled = true;
      render();
      submitAction("preview_cancel", payload, {
        requestId,
        onQueued: (action) => {
          state.previewCancellations.set(cancellation.key, {
            requestId: action.request_id,
            seq: null,
            status: "queued",
            error: "",
          });
          persistPreviewCancellations();
          render();
        },
      }).then((queued) => {
        state.previewCancellationAttempts.delete(cancellation.key);
        if (!queued) {
          const current = state.previewCancellations.get(cancellation.key);
          if (current?.requestId === requestId) {
            if (previousTracker) state.previewCancellations.set(cancellation.key, previousTracker);
            else state.previewCancellations.delete(cancellation.key);
          }
          persistPreviewCancellations({ removeKeys: [cancellation.key] });
          showToast("The cancellation could not be queued. The preview is still available for review.");
        }
        render();
      }, () => {
        state.previewCancellationAttempts.delete(cancellation.key);
        const current = state.previewCancellations.get(cancellation.key);
        if (current?.requestId === requestId) {
          if (previousTracker) state.previewCancellations.set(cancellation.key, previousTracker);
          else state.previewCancellations.delete(cancellation.key);
        }
        persistPreviewCancellations({ removeKeys: [cancellation.key] });
        render();
        showToast("The cancellation could not be queued. The preview is still available for review.");
      });
      return;
    }
    const recoveryPreview = event.target.closest("[data-recovery-preview]");
    if (recoveryPreview) { previewMetadataRecoverySelection(); return; }
    const recoveryRetryDeferred = event.target.closest("[data-recovery-retry-deferred]");
    if (recoveryRetryDeferred) {
      const recovery = currentMetadataRecovery();
      const deferredItemIds = recovery?.report?.items
        .filter((item) => item.status === "deferred")
        .map((item) => item.item_id) ?? [];
      requestMetadataRecovery("selected", { deferredItemIds });
      return;
    }
    const recoveryNext = event.target.closest("[data-recovery-next]");
    if (recoveryNext) { requestMetadataRecovery("library"); return; }
    const previewNoticeDismiss = event.target.closest("[data-recovery-preview-dismiss]");
    if (previewNoticeDismiss) {
      if (state.metadataRecoveryPreviewRequest?.requestId === previewNoticeDismiss.dataset.recoveryPreviewDismiss) {
        setMetadataRecoveryPreviewRequest(null);
        render();
      }
      return;
    }
    const recoveryClear = event.target.closest("[data-recovery-clear]");
    if (recoveryClear) {
      clearMetadataRecoveryState({ preservePreviewRequest: true });
      renderMetadataRecovery();
      return;
    }
    const organizationAction = event.target.closest("[data-organization-status]");
    if (organizationAction) {
      const item = libraryItems().find((entry) => String(entry.id) === organizationAction.dataset.organizationItem);
      if (item) previewOrganizationStatus(item, organizationAction.dataset.organizationStatus);
      return;
    }
    const dismissUnknown = event.target.closest("[data-dismiss-unknown-action]");
    if (dismissUnknown) {
      const requestId = dismissUnknown.dataset.dismissUnknownAction;
      const dismissed = dismissUnknownFileAction(state, requestId, readActionQueue(), writeActionQueue);
      if (dismissed.status !== "dismissed") {
        showToast("The delivery notice could not be removed from browser storage. It remains paused here.");
        return;
      }
      if (state.metadataRecoveryPreviewRequest?.requestId === requestId) setMetadataRecoveryPreviewRequest(null);
      if (dismissed.downloadSubmission) renderDownloadStatus();
      if (dismissed.migrationConflictDrafts > 0) renderMigrations();
      renderWaiting();
      renderPreviewReviewCards();
      showToast("The delivery outcome is still unknown. The browser notice was dismissed; check your computer before resubmitting.");
      return;
    }
    const migrationPage = event.target.closest("[data-migration-preview-page]");
    if (migrationPage) {
      const previewId = migrationPage.dataset.migrationPreviewPage;
      const revision = migrationPage.dataset.migrationPreviewRevision;
      const operation = migrationPreviewEntries().find((entry) => entry.id === previewId && entry.revision === revision);
      const direction = Number(migrationPage.dataset.pageDirection);
      const files = Array.isArray(operation?.result?.items) ? operation.result.items : [];
      if (!operation || ![-1, 1].includes(direction)) return;
      const existing = migrationConflictDraft(previewId, revision);
      const page = migrationPreviewPageWindow(files.length, existing?.page);
      const nextPage = migrationPreviewPageWindow(files.length, page.page + direction).page;
      if (nextPage === page.page) return;
      const baseline = normalizeMigrationItemConflicts(operation.result?.item_conflicts);
      const draft = {
        ...existing,
        previewId,
        revision,
        itemConflicts: normalizeMigrationItemConflicts(existing?.itemConflicts ?? baseline),
        page: nextPage,
      };
      const key = saveMigrationConflictDraft(previewId, revision, draft);
      if (migrationConflictChoicesChanged(baseline, draft.itemConflicts)) state.migrationConflictInvalidated.add(key);
      renderMigrations();
      return;
    }
    const migrationRefresh = event.target.closest("[data-migration-refresh-conflicts]");
    if (migrationRefresh) {
      const previewId = migrationRefresh.dataset.migrationRefreshConflicts;
      const revision = migrationRefresh.dataset.migrationPreviewRevision;
      const operation = migrationPreviewEntries().find((entry) => entry.id === previewId && entry.revision === revision);
      if (!operation) return;
      if (previewCancellationBusy("library_migration_preview", operation)) {
        renderMigrations();
        return;
      }
      const sourceLibraryId = migrationLibraryId(operation, operation?.result ?? {}, "source_library_id");
      const targetLibraryId = migrationLibraryId(operation, operation?.result ?? {}, "target_library_id");
      const endpoints = migrationEndpointAvailability(state.snapshot, sourceLibraryId, targetLibraryId);
      const rootBindingsMatch = migrationPreviewBindingsMatch(
        state.snapshot,
        sourceLibraryId,
        targetLibraryId,
        operation?.result?.source_root_binding,
        operation?.result?.target_root_binding,
      );
      if (!endpoints.sourceAvailable || !endpoints.targetAvailable || !rootBindingsMatch) {
        showToast(!rootBindingsMatch
          ? "The source or target registration changed. Refresh the libraries before requesting a replacement preview."
          : "This source library is offline or showing a saved scan. Reconnect it before refreshing the preview.");
        return;
      }
      const draft = migrationConflictDraft(previewId, revision);
      const payload = buildMigrationPreviewRefreshPayload(operation?.result, draft?.itemConflicts, {
        seq: operation?.seq,
        id: operation?.id,
        revision: operation?.revision,
      });
      if (!payload) {
        showToast("This preview cannot be refreshed. Choose a new source and target, then request another preview.");
        return;
      }
      migrationRefresh.disabled = true;
      submitAction("library_migration_preview", payload, {
        onQueued: createMigrationRefreshQueuedCallback({
          operation,
          previewId,
          revision,
          draft,
          payload,
          saveDraft: saveMigrationConflictDraft,
          markInvalidated: (key) => state.migrationConflictInvalidated.add(key),
          render: renderMigrations,
        }),
      }).then((queued) => {
        if (!queued) {
          const current = migrationConflictDraft(previewId, revision);
          if (current && !current.refreshRequestId) {
            current.refreshError = "The refreshed preview could not be queued. Try again when the connection is ready.";
            renderMigrations();
          }
        }
      });
      return;
    }
    const clearMigrationRefresh = event.target.closest("[data-migration-refresh-clear]");
    if (clearMigrationRefresh) {
      const previewId = clearMigrationRefresh.dataset.migrationRefreshClear;
      const revision = clearMigrationRefresh.dataset.migrationPreviewRevision;
      if (migrationCancellationBusyForDraft(previewId, revision)) {
        renderMigrations();
        return;
      }
      const draft = migrationConflictDraft(previewId, revision);
      if (!draft || !releaseMigrationRefreshTracking(draft, state.snapshot)) {
        renderMigrations();
        showToast("Recent status history cannot prove that this refresh has left the pending inbox. Its outcome remains unknown.");
        return;
      }
      saveMigrationConflictDraft(previewId, revision, draft);
      renderMigrations();
      showToast(draft.refreshError);
      return;
    }
    const movedTargetButton = event.target.closest("[data-open-moved-target]");
    if (movedTargetButton) {
      openMovedTarget(movedTargetButton);
      return;
    }
    const libraryPageButton = event.target.closest("[data-library-page]");
    if (libraryPageButton) {
      const scope = libraryPageButton.dataset.libraryPage === "needs" ? "needs" : "library";
      const pageKey = scope === "needs" ? "needsPage" : "libraryPage";
      state[pageKey] += Number(libraryPageButton.dataset.pageDirection);
      renderLibrary();
      return;
    }
    const pageButton = event.target.closest("[data-legacy-page]");
    if (pageButton) {
      state.legacyPage += Number(pageButton.dataset.legacyPage);
      renderLegacyMappings();
      return;
    }
    const manualHintButton = event.target.closest("[data-open-manual-hint]");
    if (manualHintButton && state.legacyOperation) {
      const operationKey = state.legacyOperationKey;
      const hintIndex = Number(manualHintButton.dataset.openManualHint);
      const choices = buildLegacyMappingChoices(
        operationKey,
        state.legacyOperation.result.hints,
        state.legacyOperation.result.candidates,
        libraryItems(),
      );
      const manualKey = `${operationKey}:manual:${hintIndex}`;
      const manualIndex = choices.findIndex((choice) => choice.key === manualKey);
      if (manualIndex >= 0) {
        state.legacyPage = Math.floor(manualIndex / LEGACY_MAPPING_PAGE_SIZE);
        state.manualPickerKey = manualKey;
        state.manualPickerQuery = "";
        renderLegacyMappings();
        byId("legacy-mappings").querySelector("[data-manual-picker-search]")?.focus();
      }
      return;
    }
    const manualPickerToggle = event.target.closest("[data-manual-picker-toggle]");
    if (manualPickerToggle) {
      const key = manualPickerToggle.dataset.manualPickerToggle;
      state.manualPickerKey = state.manualPickerKey === key ? null : key;
      state.manualPickerQuery = "";
      renderLegacyMappings();
      if (state.manualPickerKey) byId("legacy-mappings").querySelector("[data-manual-picker-search]")?.focus();
      return;
    }
    const groupCheckbox = event.target.closest("[data-select-group]");
    if (groupCheckbox) { event.stopPropagation(); return; }
    const tab = event.target.closest("[data-view]");
    if (tab) { state.view = tab.dataset.view; render(); return; }
    const details = event.target.closest("[data-details]");
    if (details) { state.detailId = details.dataset.details; renderDetail(); return; }
    const confirm = event.target.closest("[data-confirm]");
    if (confirm) {
      if (!libraryCanOperate(confirm.dataset.libraryId || "")) {
        showToast("This library is offline or showing a saved scan. Reconnect it and refresh before confirming.");
        return;
      }
      const matchingPreview = previews().find((entry) => entry.id === confirm.dataset.confirm && entry.revision === confirm.dataset.revision);
      if (!matchingPreview || isIncompletePreview(matchingPreview)) {
        showToast("This preview is incomplete. Request a fresh preview before confirming.");
        return;
      }
      if (!fileOperationIdentityMatches(state.snapshot, matchingPreview, confirm.dataset.libraryId || "")) {
        showToast("This preview no longer matches the library registration. Request a fresh preview before confirming.");
        return;
      }
      confirm.disabled = true;
      submitAction("file_confirm", { preview_id: confirm.dataset.confirm, revision: confirm.dataset.revision, ...(confirm.dataset.libraryId ? { library_id: confirm.dataset.libraryId } : {}) });
      return;
    }
    const migrationConfirm = event.target.closest("[data-migration-confirm]");
    if (migrationConfirm) {
      const previewId = migrationConfirm.dataset.migrationConfirm;
      const matchingPreview = migrationPreviewEntries().find((entry) => entry.id === previewId && entry.revision === migrationConfirm.dataset.revision);
      if (matchingPreview && previewCancellationBusy("library_migration_preview", matchingPreview)) {
        renderMigrations();
        return;
      }
      const result = matchingPreview?.result ?? {};
      const sourceId = String(result.source_library_id ?? matchingPreview?.source_library_id ?? activeLibraryId());
      const targetId = String(result.target_library_id ?? matchingPreview?.target_library_id ?? "");
      const rootBindingsMatch = migrationPreviewBindingsMatch(
        state.snapshot,
        sourceId,
        targetId,
        result.source_root_binding,
        result.target_root_binding,
      );
      if (!libraryCanOperate(sourceId) || targetId && !libraryCanOperate(targetId) || !rootBindingsMatch) {
        showToast("The source or target library is offline or showing a saved scan. Reconnect it and refresh before starting the migration.");
        return;
      }
      const saved = normalizeMigrationItemConflicts(matchingPreview?.result?.item_conflicts);
      const revision = migrationConfirm.dataset.revision;
      const draft = migrationConflictDraft(previewId, revision);
      const draftKey = migrationPreviewDraftKey(previewId, revision);
      if (!matchingPreview || state.migrationConflictInvalidated.has(draftKey) || draft && migrationConflictChoicesChanged(saved, draft.itemConflicts)) {
        showToast("Conflict choices changed. Refresh the preview before confirming the migration.");
        return;
      }
      migrationConfirm.disabled = true;
      submitAction("library_migration_confirm", { preview_id: previewId, revision: migrationConfirm.dataset.revision });
      return;
    }
    const migrationControl = event.target.closest("[data-migration-control]");
    if (migrationControl) {
      migrationControl.disabled = true;
      submitAction("library_migration_control", { migration_id: migrationControl.dataset.migrationId, control: migrationControl.dataset.migrationControl });
      return;
    }
    const copyButton = event.target.closest("[data-copy]");
    if (copyButton) {
      const value = copyButton.dataset.copy ?? "";
      if (navigator.clipboard?.writeText) navigator.clipboard.writeText(value).then(() => showToast("Copied to clipboard")).catch(() => showToast("Could not copy this path"));
      else showToast("Clipboard access is unavailable in this browser.");
      return;
    }
    if (event.target.closest("#preview-metadata")) { previewSelectedLegacy(); return; }
  });
  window.addEventListener("storage", (event) => {
    if (event.key === PREVIEW_CANCELLATION_STORAGE_KEY || event.key === null) {
      if (state.ownerVerified && state.privateUiVisible && mergePreviewCancellationsFromStorage()) {
        recoverFileActionReceipts();
        render();
      }
      return;
    }
    if (!event.key?.startsWith(ACTION_RECEIPT_STORAGE_PREFIX)) return;
    const recovered = recoverFileActionReceipts();
    if (recovered.matched === 0) return;
    if (state.ownerVerified) {
      if (state.loading) state.refreshAfterDrain = true;
      else refreshState({ quiet: true });
    }
  });
  window.addEventListener("online", () => { showToast("Connection restored. Sending saved actions."); drainActionQueue(); refreshState({ quiet: true }); });
  window.addEventListener("keydown", (event) => { if (event.key === "Escape" && state.detailId) { state.detailId = null; renderDetail(); } });
}

function start() {
  telegram?.ready?.();
  telegram?.expand?.();
  bindEvents();
  byId("app-content").setAttribute("aria-busy", "true");
  Object.assign(state, beginOwnerVerification());
  clearCachedSnapshot();
  if (!initData) {
    byId("app-content").setAttribute("aria-busy", "false");
    byId("auth-gate").classList.remove("hidden");
    return;
  }
  byId("auth-message").textContent = "Verifying access to the private library…";
  byId("auth-gate").classList.remove("hidden");
  setConnection("", "Verifying owner access");
  snapshotUpdates = createSnapshotUpdates({
    api,
    refresh: performRefreshState,
    onAuthorizationExpired: () => handleAuthorizationFailure({ status: 401 }),
    onStatus: updateSnapshotUpdateStatus,
  });
  window.setInterval(refreshLocalSnapshotFreshness, 60_000);
  snapshotUpdates.start();
}

start();
