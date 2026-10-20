import { appendAction, removeQueuedAction, shouldRetryActionFailure } from "./action-queue.js";
import { buildLegacyMappingChoices, buildLegacyMetadataPatches, getOrCreateLegacySelection, LEGACY_MAPPING_PAGE_SIZE, paginateLegacyMappingChoices, selectLegacyImportForMapping, selectLegacyMappingChoice, updateLegacySelection } from "./legacy-mapping-state.js";
import { hasMetadataPatchFields, hasMetadataPreviewChanges, metadataPreviewDetails } from "./metadata-preview.js";
import { HttpResponseError } from "./http-error.js";
import { acceptOwnerSnapshot, beginOwnerVerification, drainActionsAfterSnapshotRefresh, failOwnerSnapshot, mayDispatchActions, mayUsePrivateSnapshot } from "./snapshot-session.js";
import { MAX_SELECTED_ITEMS, updateSelectionWithinLimit } from "./selection-limit.js";
import { MAX_MANUAL_TARGET_OPTIONS, searchPresentManualTargets } from "./manual-target-picker.js";
import { MAX_MIGRATION_ITEM_CONFLICTS, buildMigrationPreviewRefreshPayload, migrationConflictChoicesChanged, migrationConflictPolicy, migrationRefreshMatches, normalizeMigrationItemConflicts, setMigrationConflictChoice } from "./migration-conflicts.js";
import { submitLegacyFileImport } from "./legacy-file-import.js";
import { buildWaitingEntries, isIncompletePreview, isPresentMedia, isReviewablePreviewStatus, isWaitingEntryPending, operationDetailsWarning, operationHistoryEntries, presentMediaItems } from "./state-view.js";
import { sourceIdUrl } from "./source-links.js";
import { escapeHtml, renderMovePreviewItems } from "./preview-rendering.js";
import { validateTargetRelativeDirectory } from "./target-directory.js";
import { boundedFolderSuggestions, canOperateOnMediaLibrary, defaultMediaLibraryId, filterLibraryItems, groupLibraryItems, itemFolder, LIBRARY_PAGE_SIZE, mediaLibraryAvailability, mediaLibraryEntries, needsOrganizing, operationLibraryId, operationMatchesLibrary, paginateLibraryItems, resolveMediaLibrary, statusOf } from "./library-view.js";
import { incompleteMigrationTaskSummary } from "./migration-task-summary.js";
import { renderMigrationHistory } from "./migration-rendering.js";

const API = "/api";
const CACHE_KEY = "file-manager.snapshot.v1";
const ACTION_QUEUE_KEY = "file-manager.actions.v1";
const MAX_LEGACY_BYTES = 256 * 1024;
const POLL_MS = 12_000;

const telegram = window.Telegram?.WebApp;
const initData = telegram?.initData ?? "";
const initialParams = new URLSearchParams(window.location.search);
const state = { ownerVerified: false, privateUiVisible: false, canDispatchActions: false, snapshot: null, libraryId: initialParams.get("library_id") ?? "", selected: new Set(), quarantinedActionIds: new Set(), view: "library", query: "", libraryPage: 0, needsPage: 0, detailId: null, legacyOperation: null, legacyOperationKey: null, legacySelection: new Map(), legacyPage: 0, manualPickerKey: null, manualPickerQuery: "", toastTimer: 0, loading: false, draining: null, retryDelay: 1500, retryTimer: 0, refreshAfterDrain: false, downloadSubmission: null, downloadMessage: null, downloadFormOpened: false, migrationConflictDrafts: new Map(), migrationConflictInvalidated: new Set() };

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
  const descriptor = mediaLibraryEntries(state.snapshot).find((entry) => entry.id === libraryId);
  return canOperateOnMediaLibrary(descriptor);
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
    return `<option value="${escapeHtml(entry.id)}">${escapeHtml(entry.name)}${suffix}${status}</option>`;
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
  if (!status || !submit) return;
  const submission = state.downloadSubmission;
  submit.disabled = Boolean(submission) || !libraryCanOperate(byId("download-library-select")?.value || activeLibraryId());
  if (submission?.seq !== null && submission?.seq !== undefined) {
    const pending = (Array.isArray(state.snapshot?.pending_requests) ? state.snapshot.pending_requests : [])
      .find((request) => request.seq === submission.seq);
    status.textContent = pending
      ? "Saved in the cloud inbox. Waiting for your computer; this request is not applied yet."
      : "Your computer received this request. Waiting for its local result.";
    status.classList.remove("warning-text");
  } else if (submission) {
    status.textContent = "Saved in this browser. It will be sent when the connection is available; no destination has been changed yet.";
    status.classList.add("warning-text");
  } else {
    status.textContent = state.downloadMessage ?? "Paste a video or collection URL. The selected library and category are saved with this task.";
    status.classList.toggle("warning-text", Boolean(state.downloadMessage));
  }
}

function settleDownloadSubmission() {
  const submission = state.downloadSubmission;
  if (!submission || !Number.isSafeInteger(submission.seq)) return;
  const operation = (Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [])
    .find((entry) => entry?.kind === "download_submit" && entry.seq === submission.seq);
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
    `<option value="${escapeHtml(entry.id)}" ${entry.id === selectedId ? "selected" : ""} ${libraryCanOperate(entry.id) ? "" : "disabled"}>${escapeHtml(entry.name)}${entry.is_default === true ? " · default" : ""}${libraryCanOperate(entry.id) ? "" : " · offline"}</option>`,
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

function clearCachedSnapshot() {
  try { localStorage.removeItem(CACHE_KEY); } catch { /* Private content remains hidden if storage is unavailable. */ }
}

function showPrivateAccessGate(message) {
  state.detailId = null;
  state.selected.clear();
  state.legacyOperation = null;
  state.legacyOperationKey = null;
  state.legacySelection.clear();
  document.querySelector(".detail-panel")?.remove();
  byId("app-content").classList.add("hidden");
  byId("auth-gate").classList.remove("hidden");
  byId("auth-message").textContent = message;
  setConnection("", "Owner verification required");
}

function acceptFreshSnapshot(snapshot) {
  Object.assign(state, acceptOwnerSnapshot(snapshot, state));
  settleMigrationConflictRefreshes();
  normalizeLibraryContext();
  byId("auth-gate").classList.add("hidden");
  byId("app-content").classList.remove("hidden");
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

async function refreshState({ quiet = false } = {}) {
  if (!initData || state.loading) return;
  state.loading = true;
  try {
    const fresh = await api("/state");
    acceptFreshSnapshot(fresh);
    setConnection(fresh.synced ? (fresh.stale ? "stale" : "live") : "", fresh.synced ? (fresh.stale ? "Contact is overdue" : "Computer connected") : "Waiting for computer");
    render();
    await drainActionsAfterSnapshotRefresh(
      state,
      navigator.onLine,
      () => drainActionQueue({ quiet }),
    );
  } catch (error) {
    const transition = applySnapshotFailure(error?.status);
    if (mayUsePrivateSnapshot(state)) {
      setConnection("stale", "Showing saved library");
      render();
    } else {
      showPrivateAccessGate(transition.authorizationRejected
        ? "Owner access could not be verified. Reopen this Mini App from the owner's private chat."
        : "The private library stays hidden until its owner is verified. Check the connection and retry.");
    }
    if (!quiet && navigator.onLine && mayUsePrivateSnapshot(state)) showToast(error.message);
  } finally {
    state.loading = false;
    if (state.refreshAfterDrain) {
      state.refreshAfterDrain = false;
      window.setTimeout(() => refreshState({ quiet: true }), 0);
    }
  }
}

function readActionQueue() {
  try {
    const value = JSON.parse(localStorage.getItem(ACTION_QUEUE_KEY) ?? "[]");
    return Array.isArray(value) ? value.filter((entry) => entry && typeof entry.request_id === "string" && typeof entry.kind === "string" && entry.payload && typeof entry.payload === "object") : [];
  } catch { return []; }
}

function writeActionQueue(actions) {
  if (actions.length > 20) return false;
  try { localStorage.setItem(ACTION_QUEUE_KEY, JSON.stringify(actions)); } catch { showToast("This browser could not save the action queue."); return false; }
  renderQueueCount();
  return true;
}

function renderQueueCount() {
  const count = readActionQueue().filter((action) => !state.quarantinedActionIds.has(action.request_id)).length;
  byId("queued-actions").textContent = count ? `${count} action${count === 1 ? "" : "s"} waiting to send` : "";
}

async function drainActionQueue({ quiet = false } = {}) {
  if (!initData || !mayDispatchActions(state) || !navigator.onLine) return false;
  if (state.draining) return state.draining;
  state.draining = (async () => {
    let sentAny = false;
    let connectionInterrupted = false;
    let removalFailed = false;
    const permanentRejections = [];
    while (navigator.onLine) {
      const action = readActionQueue().find((entry) => !state.quarantinedActionIds.has(entry.request_id));
      if (!action) break;
      try {
        const receipt = await api("/actions", { method: "POST", body: JSON.stringify({ request_id: action.request_id, kind: action.kind, payload: action.payload }) });
        if (state.downloadSubmission?.request_id === action.request_id && Number.isSafeInteger(receipt?.seq)) {
          state.downloadSubmission.seq = receipt.seq;
          renderDownloadStatus();
        }
        for (const draft of state.migrationConflictDrafts.values()) {
          if (draft.refreshRequestId === action.request_id && Number.isSafeInteger(receipt?.seq)) {
            draft.refreshSeq = receipt.seq;
          }
        }
        const latestQueue = readActionQueue();
        if (!writeActionQueue(removeQueuedAction(latestQueue, action.request_id))) {
          if (!quiet) showToast("Request reached the computer, but this browser could not update its retry queue.");
          break;
        }
        sentAny = true;
      } catch (error) {
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

async function submitAction(kind, payload, { onQueued } = {}) {
  if (!mayDispatchActions(state)) return false;
  const unavailableLibraryAction = (() => {
    if (["file_preview", "legacy_import"].includes(kind)) {
      return !libraryCanOperate(String(payload?.library_id ?? activeLibraryId()));
    }
    if (kind === "download_submit") return !libraryCanOperate(String(payload?.library_id ?? ""));
    if (kind === "library_migration_preview") {
      return !libraryCanOperate(String(payload?.source_library_id ?? ""))
        || !libraryCanOperate(String(payload?.target_library_id ?? ""));
    }
    if (kind === "file_confirm") {
      const preview = previews().find((entry) => entry.id === payload?.preview_id);
      return preview ? !libraryCanOperate(String(preview.library_id || activeLibraryId())) : false;
    }
    if (kind === "library_migration_confirm") {
      const preview = migrationPreviewEntries().find((entry) => entry.id === payload?.preview_id);
      const result = preview?.result ?? {};
      const sourceId = String(result.source_library_id ?? preview?.source_library_id ?? activeLibraryId());
      const targetId = String(result.target_library_id ?? preview?.target_library_id ?? "");
      return !libraryCanOperate(sourceId) || Boolean(targetId && !libraryCanOperate(targetId));
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
  const action = { request_id: crypto.randomUUID(), kind, payload };
  const queued = appendAction(readActionQueue(), action);
  if (!queued.ok) {
    showToast("The offline queue is full. Reconnect and send its saved requests first.");
    return false;
  }
  if (!writeActionQueue(queued.queue)) {
    showToast("This browser could not save the action, so it was not sent.");
    return false;
  }
  if (typeof onQueued === "function") onQueued(action);
  if (kind === "download_submit") {
    state.downloadSubmission = {
      request_id: action.request_id,
      seq: null,
      url: String(payload.url ?? ""),
      library_id: String(payload.library_id ?? ""),
      category: String(payload.category ?? ""),
    };
    renderDownloadStatus();
  }
  if (!navigator.onLine) {
    showToast("Saved offline. It will send when this device reconnects.");
    return true;
  }
  showToast("Sending the saved request");
  await drainActionQueue();
  return true;
}

function updateSelectedItems(candidateIds, selected) {
  const result = updateSelectionWithinLimit(state.selected, candidateIds, selected);
  state.selected.clear();
  for (const id of result.ids) state.selected.add(id);
  return result.omittedCount;
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

function itemRow(item, selectionEnabled = true) {
  const id = String(item.id ?? "");
  const selected = state.selected.has(id);
  const presence = statusOf(item);
  const displayStatus = presence === "present" ? "Available" : presence === "missing" ? "Missing" : presence === "unavailable" ? "Unavailable" : "Status unknown";
  const dims = item.width && item.height ? `${escapeHtml(item.width)} × ${escapeHtml(item.height)}` : "";
  const codec = item.codec ? escapeHtml(item.codec) : "";
  return `<article class="library-row${selected ? " selected" : ""}" data-item-row="${escapeHtml(id)}">
    <input class="row-check" type="checkbox" aria-label="Select ${escapeHtml(item.title ?? item.relative_path)}" data-select-item="${escapeHtml(id)}" ${selected ? "checked" : ""} ${isPresentMedia(item) && selectionEnabled ? "" : "disabled"}>
    <div class="media-icon" aria-hidden="true">▣</div>
    <div class="media-main"><div class="media-title"><span>${escapeHtml(item.title || item.relative_path || "Untitled video")}</span></div><div class="media-path">${escapeHtml(item.relative_path || "Path unavailable")}</div></div>
    <div class="media-meta"><span><i class="status-dot ${escapeHtml(presence)}"></i>${displayStatus}</span><span>${formatBytes(item.bytes)}</span>${dims ? `<span>${dims}</span>` : ""}${codec ? `<span>${codec}</span>` : ""}<button class="details-button" type="button" data-details="${escapeHtml(id)}">Details</button></div>
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
    const allSelected = groupIds.length > 0 && groupIds.every((id) => state.selected.has(id));
    const someSelected = groupIds.some((id) => state.selected.has(id));
    return `<details class="collection-group" ${index < 3 ? "open" : ""}>
      <summary><input class="group-check" type="checkbox" aria-label="Select available ${escapeHtml(title)} items on this page" data-select-group="${escapeHtml(title)}" ${allSelected ? "checked" : ""} ${someSelected && !allSelected ? "data-partial=true" : ""} ${selectableEntries.length && selectionEnabled ? "" : "disabled"}><span>${escapeHtml(title)}</span><span class="group-count">${entries.length} file${entries.length === 1 ? "" : "s"} on this page</span></summary>
      <div class="collection-items">${entries.map((item) => itemRow(item, selectionEnabled)).join("")}</div>
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
  for (const id of state.selected) if (!availableIds.has(id)) state.selected.delete(id);
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
  const firstScanPending = synced && (library?.ready === false || Number(library?.scanned_at) === 0);
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
  } else {
    emptyHeading.textContent = "No media found yet";
    emptyCopy.textContent = "Run a scan on the connected computer to discover videos and their sidecar files.";
  }
  if (!synced) {
    list.innerHTML = "";
    byId("library-empty").classList.add("hidden");
  }
  const total = libraryItems().length || Number(activeLibrary().descriptor?.media_count ?? 0);
  byId("library-count").textContent = synced && !firstScanPending ? String(total) : "—";
  byId("library-summary").textContent = firstScanPending
    ? "Your computer is connected, but the first scan has not completed."
    : state.snapshot?.synced
    ? `${savedLibraryView ? "Showing the last saved scan; cached items are read-only · " : ""}${items.length.toLocaleString()} matching item${items.length === 1 ? "" : "s"} · ${items.length ? `showing ${libraryPage.start + 1}–${libraryPage.end} on page ${libraryPage.page + 1} of ${Math.max(1, libraryPage.pageCount)}` : "no matches"} · ${total.toLocaleString()} in library${state.selected.size ? ` · ${state.selected.size} selected${state.selected.size >= MAX_SELECTED_ITEMS ? ` · selection limit ${MAX_SELECTED_ITEMS}; deselect to add more` : ""}` : ""}`
    : "A first scan has not completed yet.";
  const needsTotal = libraryItems().filter(needsOrganizing).length;
  byId("needs-count").textContent = synced && !firstScanPending ? String(needsTotal) : "—";
  byId("needs-summary").textContent = firstScanPending
    ? "Needs organizing will appear after the first scan completes."
    : state.snapshot?.synced
    ? `${needsItems.length.toLocaleString()} matching item${needsItems.length === 1 ? "" : "s"} · ${needsItems.length ? `showing ${needsPage.start + 1}–${needsPage.end} on page ${needsPage.page + 1} of ${Math.max(1, needsPage.pageCount)}` : "no matches"} · ${needsTotal.toLocaleString()} need attention in total`
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
}

function operationTitle(operation) {
  const kind = String(operation.kind ?? "operation").replaceAll("_", " ");
  return kind.replace(/^\w/u, (letter) => letter.toLocaleUpperCase());
}

function statusText(value) {
  const text = String(value ?? "pending").replaceAll("_", " ");
  return text.replace(/^\w/u, (letter) => letter.toLocaleUpperCase());
}

function legacyOperations() {
  return (Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : []).filter((operation) =>
    operation?.kind === "legacy_import" && operationMatchesLibrary(operation, activeLibraryId()));
}

function renderWaiting() {
  const tasks = Array.isArray(state.snapshot?.tasks) ? state.snapshot.tasks : [];
  const operations = Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [];
  const cloudQueue = Array.isArray(state.snapshot?.pending_requests) ? state.snapshot.pending_requests : [];
  const { cloud: pendingRequests, local } = buildWaitingEntries(cloudQueue, tasks, operations);
  const pendingCloud = pendingRequests.filter(isWaitingEntryPending);
  const pending = local.filter(isWaitingEntryPending);
  const localQueue = readActionQueue();
  byId("waiting-count").textContent = String(pendingCloud.length + pending.length + localQueue.length);
  const cards = [];
  for (const request of pendingCloud) cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(operationTitle(request))}</h3><p>Saved in the cloud inbox. Your desktop app has not acknowledged local receipt yet.</p></div><span class="state-pill pending">Waiting for computer · #${escapeHtml(request.seq)}</span></div><p>Received ${escapeHtml(timeAgo(request.created_at))}</p></article>`);
  for (const action of localQueue) cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(action.kind.replaceAll("_", " "))}</h3><p>Saved in this browser. It will be sent when this device is connected.</p></div><span class="state-pill pending">Offline queue</span></div></article>`);
  for (const entry of pending) cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(operationTitle(entry))}</h3><p>${escapeHtml(entry.message ?? entry.error ?? "Your computer has received this request and is working on it.")}</p></div><span class="state-pill pending">${escapeHtml(statusText(entry.status))}</span></div>${entry.updated_at ? `<p>Updated ${escapeHtml(timeAgo(entry.updated_at, "seconds"))}</p>` : ""}</article>`);
  byId("waiting-list").innerHTML = cards.length ? cards.join("") : `<div class="empty-state"><div class="empty-art" aria-hidden="true">✓</div><h2>Nothing is waiting</h2><p>New requests and scans will appear here while your computer processes them.</p></div>`;
}

function operationPreview(operation) {
  if (operation?.preview && typeof operation.preview === "object") return { id: operation.preview_id ?? operation.preview.id, revision: operation.revision ?? operation.preview.revision, preview: operation.preview, status: operation.status, library_id: operationLibraryId(operation) };
  if (operation?.result?.preview && typeof operation.result.preview === "object") return { id: operation.preview_id ?? operation.result.preview_id ?? operation.result.preview.id, revision: operation.revision ?? operation.result.revision ?? operation.result.preview.revision, preview: operation.result.preview, status: operation.status, library_id: operationLibraryId(operation) };
  return null;
}

function previews() {
  const collected = [];
  for (const preview of Array.isArray(state.snapshot?.previews) ? state.snapshot.previews : []) {
    if (preview?.kind === "library_migration_preview") continue;
    const incomplete = preview.preview_incomplete === true || preview.incomplete === true || preview.reload_required === true;
    collected.push({
      id: preview.preview_id ?? preview.id ?? (incomplete ? "omitted-preview-details" : undefined),
      revision: preview.revision ?? preview.preview?.revision ?? (incomplete ? String(preview.omitted_count ?? "unknown") : undefined),
      preview: incomplete ? null : preview.preview ?? preview,
      incomplete: preview.incomplete === true,
      preview_incomplete: preview.preview_incomplete === true,
      reload_required: preview.reload_required === true,
      reason: preview.reason,
      status: preview.status,
      library_id: preview.library_id ?? preview.source_library_id ?? "",
    });
  }
  for (const operation of Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : []) {
    const found = operationPreview(operation);
    if (found) collected.push(found);
  }
  const unique = new Map();
  for (const value of collected) {
    if (value.id && value.revision) unique.set(`${value.id}:${value.revision}`, value);
  }
  return [...unique.values()].reverse().filter((entry) => operationMatchesLibrary(entry, activeLibraryId()));
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
  const entries = mediaLibraryEntries(state.snapshot).filter((entry) => entry.id !== sourceId);
  byId("migration-target").innerHTML = entries.map((entry) =>
    `<option value="${escapeHtml(entry.id)}" ${libraryCanOperate(entry.id) ? "" : "disabled"}>${escapeHtml(entry.name)} · ${escapeHtml(entry.root ?? "root unavailable")}${libraryCanOperate(entry.id) ? "" : " · offline"}</option>`,
  ).join("");
  const availableTargets = entries.filter((entry) => libraryCanOperate(entry.id));
  const sourceAvailable = libraryCanOperate(sourceId);
  if (!availableTargets.some((entry) => entry.id === byId("migration-target").value)) {
    byId("migration-target").value = availableTargets[0]?.id ?? "";
  }
  byId("migration-target").disabled = availableTargets.length === 0 || !sourceAvailable;
  byId("migration-scope").disabled = !sourceAvailable;
  byId("migration-include-tasks").disabled = !sourceAvailable;
  byId("migration-set-default").disabled = !sourceAvailable;
  byId("migration-conflict").disabled = !sourceAvailable;
  byId("migration-preview").disabled = availableTargets.length === 0 || !sourceAvailable;
  byId("migration-source").textContent = `Source: ${libraryLabel(sourceId)} · Choose the source in the library selector above.`;
  const selectionSize = state.selected.size;
  byId("migration-selection-count").textContent = selectionSize
    ? `${selectionSize} selected media item${selectionSize === 1 ? "" : "s"} from this library. Collection selections include their selected media rows.`
    : "Select media in the Library view first to migrate a selected subset.";
  const taskSummary = incompleteTaskSummary(sourceId);
  byId("migration-task-option").classList.toggle("hidden", taskSummary.count === 0);
  const countLabel = taskSummary.countIsLowerBound ? `At least ${taskSummary.count}` : String(taskSummary.count);
  const byteLabel = taskSummary.bytes === null ? "staged data size unknown" : `${formatBytes(taskSummary.bytes)} staged data`;
  byId("migration-task-count").textContent = `${countLabel} unfinished task${taskSummary.count === 1 ? "" : "s"} · ${byteLabel}. This option is off by default.`;
  const selectedScope = byId("migration-scope").value === "selected";
  byId("migration-preview").disabled = !sourceAvailable || availableTargets.length === 0 || selectedScope && selectionSize === 0;
}

function migrationPreviewEntries() {
  const active = [];
  for (const preview of Array.isArray(state.snapshot?.previews) ? state.snapshot.previews : []) {
    if (preview?.kind !== "library_migration_preview" || preview?.status !== "awaiting_confirmation") continue;
    active.push({
      ...preview,
      result: preview.result && typeof preview.result === "object" ? preview.result : null,
      id: preview.preview_id ?? preview.id,
      incomplete: preview.preview_incomplete === true || preview.incomplete === true || preview.reload_required === true,
    });
  }
  for (const operation of Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : []) {
    if (operation?.kind !== "library_migration_preview" || operation?.status !== "awaiting_confirmation") continue;
    const result = operation.result && typeof operation.result === "object" ? operation.result : {};
    active.push({
      ...operation,
      id: result.id ?? result.preview_id ?? operation.preview_id,
      result,
      incomplete: operation.details_incomplete === true || operation.result_truncated === true || result.incomplete === true,
    });
  }
  const unique = new Map();
  for (const operation of active) {
    const result = operation.result ?? {};
    const sourceLibraryId = result.source_library_id ?? operation.source_library_id;
    if (sourceLibraryId !== activeLibraryId()) continue;
    const id = operation.id ?? result.id ?? result.preview_id;
    const revision = operation.revision ?? result.revision;
    if (id && revision) {
      const key = `${id}:${revision}`;
      const existing = unique.get(key);
      if (!existing || existing.incomplete && !operation.incomplete) {
        unique.set(key, { ...operation, id, revision });
      }
    }
  }
  return [...unique.values()].slice(-8).reverse();
}

function settleMigrationConflictRefreshes() {
  const snapshots = [
    ...(Array.isArray(state.snapshot?.previews) ? state.snapshot.previews : []),
    ...(Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : []),
  ];
  for (const [previewId, draft] of state.migrationConflictDrafts) {
    if (!Number.isSafeInteger(draft.refreshSeq)) continue;
    const operation = snapshots.find((entry) => entry?.kind === "library_migration_preview" && entry.seq === draft.refreshSeq);
    if (!operation) continue;
    const result = operation.result && typeof operation.result === "object"
      ? operation.result
      : operation.preview && typeof operation.preview === "object" ? operation.preview : {};
    if (operation.status === "awaiting_confirmation") {
      if (migrationRefreshMatches(result, draft.refreshPayload) && (result.id ?? result.preview_id) !== previewId) {
        state.migrationConflictDrafts.delete(previewId);
        continue;
      }
      if (operation.details_incomplete !== true && operation.result_truncated !== true) {
        draft.refreshRequestId = null;
        draft.refreshSeq = null;
        draft.refreshError = "The computer returned a preview with different conflict choices. Review it before continuing.";
      }
    } else if (["failed", "stale"].includes(String(operation.status))) {
      draft.refreshRequestId = null;
      draft.refreshSeq = null;
      draft.refreshError = String(operation.error ?? "The computer could not create a refreshed preview.");
    }
  }
}

function renderMigrationPreview(operation) {
  const result = operation?.result && typeof operation.result === "object" ? operation.result : {};
  const id = String(result.id ?? result.preview_id ?? operation?.preview_id ?? operation?.id ?? "");
  const revision = String(result.revision ?? operation?.revision ?? "");
  const sourceLibraryId = String(result.source_library_id ?? operation?.source_library_id ?? activeLibraryId());
  const sourceAvailable = libraryCanOperate(sourceLibraryId);
  const targetLibraryId = String(result.target_library_id ?? operation?.target_library_id ?? "");
  const targetAvailable = !targetLibraryId || libraryCanOperate(targetLibraryId);
  if (operation?.incomplete || !Array.isArray(result.items)) {
    return `<article class="stack-card"><div class="stack-card-head"><div><h3>Migration preview needs to be regenerated</h3><p>${escapeHtml(operation?.reason ?? "Preview details are unavailable in this cloud snapshot. Request a fresh preview before confirming.")}</p></div><span class="state-pill pending">Refresh required</span></div></article>`;
  }
  const files = Array.isArray(result.items) ? result.items : [];
  const displayItems = [...files.slice(0, 30), ...files.slice(30).filter((item) => item?.conflict === true)];
  const draft = state.migrationConflictDrafts.get(id);
  const savedConflicts = normalizeMigrationItemConflicts(result.item_conflicts);
  const draftConflicts = draft ? normalizeMigrationItemConflicts(draft.itemConflicts) : savedConflicts;
  const choicesChanged = Boolean(draft && migrationConflictChoicesChanged(savedConflicts, draftConflicts));
  const invalidated = state.migrationConflictInvalidated.has(id) || choicesChanged;
  const refreshPending = Boolean(draft?.refreshRequestId);
  const paths = displayItems.map((item) => {
    const itemId = String(item.item_id ?? "");
    const conflict = item.conflict === true && itemId.length > 0;
    const policy = conflict
      ? draftConflicts[itemId] ?? (result.conflict === "keep_both" ? "keep_both" : "skip")
      : "skip";
    const outcome = item.outcome === "skip" || item.status === "skipped" ? "Will skip" : "Will move";
    const nested = Array.isArray(item.files) ? item.files : [];
    const labels = nested.slice(0, 12).map((file) => `${escapeHtml(file.source_path ?? "Source unavailable")} → ${escapeHtml(file.target_path ?? "Target unavailable")}`);
    const path = `${escapeHtml(item.source_path ?? "Source unavailable")} → ${escapeHtml(item.target_path ?? "Target unavailable")}`;
    const choice = conflict
      ? `<label class="migration-conflict-choice">${escapeHtml(itemId)} conflict<select class="select-control" data-migration-conflict-item="${escapeHtml(itemId)}" data-migration-preview-id="${escapeHtml(id)}" aria-label="Conflict handling for ${escapeHtml(itemId)}" ${refreshPending || !sourceAvailable || !targetAvailable ? "disabled" : ""}><option value="skip" ${policy === "skip" ? "selected" : ""}>Skip</option><option value="keep_both" ${policy === "keep_both" ? "selected" : ""}>Keep both</option></select></label>`
      : `<strong class="${item.outcome === "skip" || item.status === "skipped" ? "skip" : "move"}">${outcome}</strong>`;
    return `<div class="preview-line${conflict ? " migration-conflict-line" : ""}"><span>${path}${labels.length ? `<br><span class="field-hint">${labels.join("<br>")}</span>` : ""}</span>${choice}</div>`;
  }).join("");
  const moreCount = Math.max(0, files.length - displayItems.length);
  const more = moreCount ? `<p class="field-hint">${moreCount} non-conflicting item paths are retained in the preview.</p>` : "";
  const summary = `${Number(result.item_count ?? files.length).toLocaleString()} media items · ${Number(result.file_count ?? 0).toLocaleString()} files · ${formatBytes(result.total_bytes)} total`;
  const space = `Required space ${formatBytes(result.required_space_bytes)} · Available ${formatBytes(result.available_space_bytes)} · ${Number(result.conflict_count ?? 0)} conflict${Number(result.conflict_count ?? 0) === 1 ? "" : "s"}`;
  const taskBytes = result.incomplete_task_bytes ?? result.staging_bytes;
  const taskBytesLabel = taskBytes === null || taskBytes === undefined
    ? "staged data size unknown"
    : `${formatBytes(taskBytes)} staged data`;
  const tasks = Number(result.incomplete_task_count ?? 0) > 0
    ? `<p>${Number(result.incomplete_task_count).toLocaleString()} unfinished tasks · ${taskBytesLabel} ${result.include_incomplete_tasks === true ? "included" : "left in the source"}</p>`
    : "";
  const refreshMessage = refreshPending
    ? `<p class="field-hint" role="status">Waiting for the updated preview from your computer. This preview cannot be confirmed.</p>`
    : draft?.refreshError ? `<p class="field-hint" role="alert">${escapeHtml(draft.refreshError)}</p>` : "";
  const staleMessage = invalidated && !refreshPending
    ? `<p class="field-hint" role="alert">Conflict choices changed. Refresh this preview before starting the migration.</p>`
    : "";
  const supersededMessage = state.migrationConflictInvalidated.has(id) && !draft
    ? `<p class="field-hint" role="status">This preview was superseded by a refreshed preview and cannot be confirmed.</p>`
    : "";
  const canConfirm = Boolean(id && revision) && !invalidated && sourceAvailable && targetAvailable;
  const canRefresh = Boolean(id && revision && draft && choicesChanged && !refreshPending);
  const refreshButton = canRefresh
    ? `<button class="small-button" type="button" data-migration-refresh-conflicts="${escapeHtml(id)}" ${sourceAvailable && targetAvailable ? "" : "disabled"}>Refresh preview</button>`
    : "";
  const conflictLimit = Number(result.conflict_count ?? files.filter((item) => item?.conflict === true).length) > MAX_MIGRATION_ITEM_CONFLICTS
    ? `<p class="field-hint">This preview has more than ${MAX_MIGRATION_ITEM_CONFLICTS.toLocaleString()} conflicting bundles. Refresh with the global choice to review a supported subset.</p>`
    : "";
  const offlineMessage = sourceAvailable && targetAvailable ? "" : `<p class="field-hint">The source or target library is offline or showing a saved scan. Reconnect it and refresh before starting the migration.</p>`;
  return `<article class="stack-card"><div class="stack-card-head"><div><h3>Migration preview</h3><p>${escapeHtml(summary)}</p><p>${escapeHtml(space)}</p>${tasks}<p>Category and collection structure will be preserved. Default conflict choice: ${escapeHtml(result.conflict ?? "skip")}.</p>${offlineMessage}${refreshMessage}${staleMessage}${supersededMessage}${conflictLimit}</div><span class="state-pill pending">Review needed</span></div><div class="preview-table">${paths || `<div class="preview-line"><span>No media items were found in this selection.</span></div>`}${more}</div><div class="stack-actions">${refreshButton}<button class="small-button confirm" type="button" data-migration-confirm="${escapeHtml(id)}" data-revision="${escapeHtml(revision)}" ${canConfirm ? "" : "disabled"}>Start migration</button></div></article>`;
}

function renderMigrations() {
  migrationTargetOptions();
  const previews = migrationPreviewEntries();
  byId("migration-preview-list").innerHTML = previews.map(renderMigrationPreview).join("");
  byId("migration-progress-list").innerHTML = renderMigrationHistory({
    migrations: state.snapshot?.library?.migrations,
    libraries: mediaLibraryEntries(state.snapshot),
    libraryId: activeLibraryId(),
    omittedCount: state.snapshot?.library?.migration_omitted_count,
  });
}

function renderPreviews() {
  const pending = previews().filter((entry) => isReviewablePreviewStatus(entry.status));
  byId("organizing-count").textContent = String(pending.length);
  byId("preview-list").innerHTML = pending.map((entry) => {
    if (isIncompletePreview(entry)) {
      const reason = entry.reason || "Preview details were omitted from the saved snapshot. Request a fresh preview before confirming.";
      return `<article class="stack-card"><div class="stack-card-head"><div><h3>Preview needs to be regenerated</h3><p>${escapeHtml(reason)}</p></div><span class="state-pill pending">Refresh required</span></div></article>`;
    }
    const preview = entry.preview;
    const hasMetadataChanges = hasMetadataPreviewChanges(preview);
    const hasMoveItems = Array.isArray(preview.items) && preview.items.length > 0;
    const title = hasMetadataChanges && hasMoveItems ? "Move and update metadata" : hasMetadataChanges ? "Update metadata" : "Move files";
    const libraryId = entry.library_id || activeLibraryId();
    const canConfirm = libraryCanOperate(libraryId);
    const offlineMessage = canConfirm ? "" : `<p class="field-hint">This library is offline or showing a saved scan. Reconnect it and refresh before confirming.</p>`;
    return `<article class="stack-card"><div class="stack-card-head"><div><h3>${title}</h3><p>Destination: ${escapeHtml(preview.target_relative_dir ?? "Metadata only · file locations stay the same")} · Conflict: ${escapeHtml(preview.conflict ?? "skip")}</p>${offlineMessage}</div><span class="state-pill pending">Review needed</span></div><div class="preview-table">${describePreview(preview)}</div><div class="stack-actions"><button class="small-button confirm" type="button" data-confirm="${escapeHtml(entry.id)}" data-revision="${escapeHtml(entry.revision)}" data-library-id="${escapeHtml(libraryId)}" ${canConfirm ? "" : "disabled"}>Confirm changes</button></div></article>`;
  }).join("");
  if (!pending.length) byId("preview-list").innerHTML = "";
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
  container.innerHTML = `<p class="mapping-title">Choose a video for each legacy hint</p>${newerImportNotice}${warnings.length ? `<p class="warning-text">${warnings.map(escapeHtml).join(" · ")}</p>` : ""}<p class="field-hint">${displayedRange}</p>${rows.length ? rows.join("") : emptyMessage}${pagination}<div class="mapping-actions"><button id="preview-metadata" class="primary-button" type="button" ${choices.length && libraryCanOperate() ? "" : "disabled"}>Preview selected updates <span aria-hidden="true">→</span></button></div>`;
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
  const visible = operationHistoryEntries(operations);
  byId("operation-list").innerHTML = visible.slice(-12).reverse().map((entry) => {
    const result = entry.result && typeof entry.result === "object" ? entry.result : {};
    const outcome = result.operation && typeof result.operation === "object" ? result.operation : result;
    const items = Array.isArray(outcome.items) ? outcome.items : Array.isArray(result.items) ? result.items : [];
    const hintCount = Array.isArray(result.hints) ? result.hints.length : 0;
    const candidateCount = Array.isArray(result.candidates) ? result.candidates.length : 0;
    const detailsWarning = operationDetailsWarning(entry);
    const movedFiles = items.reduce((total, item) => total + (Number.isSafeInteger(item.moved_files) ? item.moved_files : 0), 0);
    const totalFiles = items.reduce((total, item) => total + (Number.isSafeInteger(item.total_files) ? item.total_files : 0), 0);
    const metadataUpdated = items.filter((item) => item.metadata_patched === true).length;
    const summary = items.length
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
    return `<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(operationTitle(entry))}</h3><p>${escapeHtml(entry.error ?? summary)}</p>${contextLabel}</div><span class="state-pill">${escapeHtml(statusText(entry.status))}</span></div>${entry.created_at ? `<p>Received ${escapeHtml(timeAgo(entry.created_at))}</p>` : ""}${itemResults ? `<div class="preview-table">${itemResults}</div>` : ""}${warningDetails.length ? `<div class="operation-warnings">${warningDetails.map((warning) => `<p>${escapeHtml(warning)}</p>`).join("")}</div>` : ""}${entry.kind === "legacy_import" && result.revision ? `<p>Library revision: ${escapeHtml(result.revision)}</p>` : ""}</article>`;
  }).join("");
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
        <div class="detail-stat"><span>Integrity</span><strong>${escapeHtml(item.integrity_status ?? "Unknown")}</strong></div>
        <div class="detail-stat"><span>Media + related files</span><strong>${1 + attachments.length} files</strong></div>
      </div>
      ${collection ? `<section class="detail-section"><h3>Collection</h3><div class="attachment">${escapeHtml(collection)}</div></section>` : ""}
      <section class="detail-section"><h3>Source links (${sourceIds.length})</h3>${sourceIds.length ? sourceIds.map((source) => { const url = sourceIdUrl(source); return `<div class="attachment">${url ? `<a href="${url}" target="_blank" rel="noopener noreferrer">${escapeHtml(source)}</a>` : escapeHtml(source)}</div>`; }).join("") : `<div class="attachment">No source links recorded.</div>`}<p class="field-hint">Remote source availability has not been checked.</p></section>
      <section class="detail-section"><h3>Related files (${attachments.length})</h3>${attachments.length ? attachments.map((attachment) => { const rel = attachment.relative_path ?? attachment.path ?? "Related file"; const absolute = pathJoin(rel); return `<div class="attachment"><div class="copy-row"><code>${escapeHtml(rel)}</code><button class="small-button" type="button" data-copy="${escapeHtml(rel)}">Copy relative</button></div>${absolute ? `<div class="copy-row"><code>${escapeHtml(absolute)}</code><button class="small-button" type="button" data-copy="${escapeHtml(absolute)}">Copy full</button></div>` : ""}<span>${escapeHtml(attachment.kind ?? "attachment")}</span></div>`; }).join("") : `<div class="attachment">No attachments found.</div>`}</section>
    </aside>`;
  document.body.append(panel);
  panel.addEventListener("click", (event) => { if (event.target === panel || event.target.closest(".detail-close")) { state.detailId = null; renderDetail(); } });
}

function render() {
  if (!mayUsePrivateSnapshot(state)) return;
  renderSync();
  renderLibrary();
  settleDownloadSubmission();
  renderDownloadStatus();
  renderWaiting();
  renderMigrations();
  renderPreviews();
  renderLegacyMappings();
  renderOperations();
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
  submitAction("file_preview", { library_id: activeLibraryId(), metadata_patches: result.patches, rename: false, conflict: "skip" });
}

function selectVisible() {
  if (!libraryCanOperate()) return showToast("This library is offline or showing a saved scan. Reconnect it before selecting files to change.");
  const scope = state.view === "needs" ? "needs" : "library";
  const visible = presentMediaItems(pageItemsForScope(scope));
  const allSelected = visible.length > 0 && visible.every((item) => state.selected.has(String(item.id)));
  const omittedCount = updateSelectedItems(visible.map((item) => String(item.id)), !allSelected);
  renderLibrary();
  if (omittedCount > 0) showToast(selectionLimitMessage(omittedCount));
}

function bindEvents() {
  byId("refresh-button").addEventListener("click", () => refreshState());
  byId("scan-button").addEventListener("click", () => submitAction("library_scan", { library_id: activeLibraryId() }));
  document.querySelectorAll("[data-scan]").forEach((button) => button.addEventListener("click", () => submitAction("library_scan", { library_id: activeLibraryId() })));
  byId("library-select").addEventListener("change", () => {
    state.libraryId = byId("library-select").value;
    state.selected.clear();
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
  byId("download-here").addEventListener("click", () => openDownloadForm());
  byId("download-close").addEventListener("click", closeDownloadForm);
  byId("download-cancel").addEventListener("click", closeDownloadForm);
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
    const libraryId = byId("download-library-select").value;
    const category = byId("download-category").value.trim();
    const { directory, error } = validateTargetRelativeDirectory(category);
    if (error) return showToast(error);
    if (!url) return showToast("Enter a video or collection URL first.");
    state.downloadMessage = null;
    submitAction("download_submit", { url, library_id: libraryId, category: directory });
  });
  byId("migration-scope").addEventListener("change", renderMigrations);
  byId("migration-preview").addEventListener("click", () => {
    const sourceId = activeLibraryId();
    const targetId = byId("migration-target").value;
    const scope = byId("migration-scope").value;
    if (!sourceId || !targetId || sourceId === targetId) return showToast("Choose a different registered target library.");
    if (scope === "selected" && state.selected.size === 0) return showToast("Select media or collections in the Library view first.");
    if (state.selected.size > MAX_SELECTED_ITEMS) return showToast(`Choose at most ${MAX_SELECTED_ITEMS} media items per migration preview.`);
    submitAction("library_migration_preview", {
      source_library_id: sourceId,
      target_library_id: targetId,
      scope,
      ...(scope === "selected" ? { item_ids: [...state.selected] } : {}),
      include_incomplete_tasks: !byId("migration-task-option").classList.contains("hidden") && byId("migration-include-tasks").checked,
      conflict: byId("migration-conflict").value,
      set_target_default_on_success: byId("migration-set-default").checked,
    });
  });
  byId("search-input").addEventListener("input", (event) => { state.query = event.target.value; state.libraryPage = 0; state.needsPage = 0; renderLibrary(); });
  byId("category-filter").addEventListener("input", () => { state.libraryPage = 0; state.needsPage = 0; renderLibrary(); });
  byId("status-filter").addEventListener("change", () => { state.libraryPage = 0; state.needsPage = 0; renderLibrary(); });
  byId("select-visible").addEventListener("click", selectVisible);
  byId("target-dir").addEventListener("input", renderFilters);
  byId("root-target").addEventListener("click", () => { byId("target-dir").value = ""; renderFilters(); byId("target-dir").focus(); });
  byId("preview-move").addEventListener("click", () => {
    if (!libraryCanOperate()) return showToast("This library is offline or showing a saved scan. Reconnect it before changing files.");
    if (state.selected.size > MAX_SELECTED_ITEMS) return showToast(`Choose at most ${MAX_SELECTED_ITEMS} videos per request.`);
    const { directory, error } = validateTargetRelativeDirectory(byId("target-dir").value);
    if (error) return showToast(error);
    submitAction("file_preview", { library_id: activeLibraryId(), item_ids: [...state.selected], target_relative_dir: directory, rename: byId("rename-toggle").checked, conflict: byId("conflict-select").value });
  });
  byId("legacy-text").addEventListener("input", (event) => { byId("legacy-size").textContent = `${new TextEncoder().encode(event.target.value).byteLength.toLocaleString()} / 256 KB`; });
  byId("import-legacy").addEventListener("click", async () => {
    if (!libraryCanOperate()) return showToast("This library is offline or showing a saved scan. Reconnect it before importing metadata.");
    let text = byId("legacy-text").value;
    const fileInput = byId("legacy-file");
    if (!text && fileInput?.files?.[0]) {
      return submitLegacyFileImport(
        fileInput.files[0],
        (value) => submitAction("legacy_import", { text: value, library_id: activeLibraryId() }),
        showToast,
        MAX_LEGACY_BYTES,
      );
    }
    const bytes = new TextEncoder().encode(text).byteLength;
    if (bytes === 0) return showToast("Paste legacy text or choose a text file first.");
    if (bytes > MAX_LEGACY_BYTES) return showToast("Legacy text exceeds the 256 KB limit.");
    submitAction("legacy_import", { text, library_id: activeLibraryId() });
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
    const migrationConflict = event.target.closest("[data-migration-conflict-item]");
    if (migrationConflict) {
      const previewId = migrationConflict.dataset.migrationPreviewId;
      const itemId = migrationConflict.dataset.migrationConflictItem;
      const operation = migrationPreviewEntries().find((entry) => entry.id === previewId);
      const result = operation?.result ?? {};
      const existing = state.migrationConflictDrafts.get(previewId);
      const defaultPolicy = result.conflict === "keep_both" ? "keep_both" : "skip";
      const current = normalizeMigrationItemConflicts(existing?.itemConflicts ?? result.item_conflicts);
      const next = setMigrationConflictChoice(current, itemId, migrationConflict.value, defaultPolicy);
      if (next[itemId] !== (migrationConflict.value === defaultPolicy ? undefined : migrationConflict.value)) {
        migrationConflict.value = migrationConflictPolicy(result, itemId, current);
        showToast(`Per-bundle conflict overrides are limited to ${MAX_MIGRATION_ITEM_CONFLICTS.toLocaleString()} bundles.`);
        return;
      }
      if (migrationConflictChoicesChanged(result.item_conflicts, next)) {
        state.migrationConflictDrafts.set(previewId, { ...existing, itemConflicts: next, refreshError: null });
        state.migrationConflictInvalidated.add(previewId);
      } else {
        state.migrationConflictDrafts.delete(previewId);
        state.migrationConflictInvalidated.delete(previewId);
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
      const groupTitle = groupCheckbox.dataset.selectGroup;
      const scope = state.view === "needs" ? "needs" : "library";
      const visibleItems = pageItemsForScope(scope);
      const groupItems = presentMediaItems(visibleItems.filter((item) => (item.collection?.title ?? item.collection ?? "Uncollected") === groupTitle));
      const omittedCount = updateSelectedItems(groupItems.map((item) => String(item.id)), groupCheckbox.checked);
      renderLibrary();
      if (omittedCount > 0) showToast(selectionLimitMessage(omittedCount));
      return;
    }
    const checkbox = event.target.closest("[data-select-item]");
    if (!checkbox) return;
    if (!libraryCanOperate()) { renderLibrary(); return; }
    const itemId = checkbox.dataset.selectItem;
    const item = libraryItems().find((entry) => String(entry.id) === String(itemId));
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
    const migrationRefresh = event.target.closest("[data-migration-refresh-conflicts]");
    if (migrationRefresh) {
      const previewId = migrationRefresh.dataset.migrationRefreshConflicts;
      const operation = migrationPreviewEntries().find((entry) => entry.id === previewId);
      const sourceLibraryId = String(operation?.result?.source_library_id ?? operation?.source_library_id ?? activeLibraryId());
      if (!libraryCanOperate(sourceLibraryId)) {
        showToast("This source library is offline or showing a saved scan. Reconnect it before refreshing the preview.");
        return;
      }
      const draft = state.migrationConflictDrafts.get(previewId);
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
        onQueued(action) {
          state.migrationConflictDrafts.set(previewId, {
            ...draft,
            refreshRequestId: action.request_id,
            refreshPayload: payload,
            refreshSeq: null,
            refreshError: null,
          });
          renderMigrations();
        },
      }).then((queued) => {
        if (!queued) {
          const current = state.migrationConflictDrafts.get(previewId);
          if (current && !current.refreshRequestId) {
            current.refreshError = "The refreshed preview could not be queued. Try again when the connection is ready.";
            renderMigrations();
          }
        }
      });
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
      if (!libraryCanOperate(confirm.dataset.libraryId || activeLibraryId())) {
        showToast("This library is offline or showing a saved scan. Reconnect it and refresh before confirming.");
        return;
      }
      const matchingPreview = previews().find((entry) => entry.id === confirm.dataset.confirm && entry.revision === confirm.dataset.revision);
      if (!matchingPreview || isIncompletePreview(matchingPreview)) {
        showToast("This preview is incomplete. Request a fresh preview before confirming.");
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
      const result = matchingPreview?.result ?? {};
      const sourceId = String(result.source_library_id ?? matchingPreview?.source_library_id ?? activeLibraryId());
      const targetId = String(result.target_library_id ?? matchingPreview?.target_library_id ?? "");
      if (!libraryCanOperate(sourceId) || targetId && !libraryCanOperate(targetId)) {
        showToast("The source or target library is offline or showing a saved scan. Reconnect it and refresh before starting the migration.");
        return;
      }
      const saved = normalizeMigrationItemConflicts(matchingPreview?.result?.item_conflicts);
      const draft = state.migrationConflictDrafts.get(previewId);
      if (!matchingPreview || state.migrationConflictInvalidated.has(previewId) || draft && migrationConflictChoicesChanged(saved, draft.itemConflicts)) {
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
  window.addEventListener("online", () => { showToast("Connection restored. Sending saved actions."); drainActionQueue(); refreshState({ quiet: true }); });
  window.addEventListener("keydown", (event) => { if (event.key === "Escape" && state.detailId) { state.detailId = null; renderDetail(); } });
}

function start() {
  telegram?.ready?.();
  telegram?.expand?.();
  bindEvents();
  Object.assign(state, beginOwnerVerification());
  clearCachedSnapshot();
  if (!initData) {
    byId("auth-gate").classList.remove("hidden");
    return;
  }
  byId("auth-message").textContent = "Verifying access to the private library…";
  byId("auth-gate").classList.remove("hidden");
  setConnection("", "Verifying owner access");
  refreshState();
  window.setInterval(() => refreshState({ quiet: true }), POLL_MS);
}

start();
