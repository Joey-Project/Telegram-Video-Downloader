import { ACTION_QUEUE_LIMIT, ACTION_RECEIPT_STORAGE_PREFIX, ACTION_SURFACE_SETTINGS, actionBelongsToSurface, appendAction, consumeRejectedActionReceipts, createQueuedAction, dispatchNextOwnedAction, recordActionReceipt, recoverActionReceipts, removeQueuedAction, restorePendingActions, shouldRetryActionFailure } from "/file/action-queue.js";
import { HttpResponseError } from "/file/http-error.js";
import { createOwnerSnapshotLoader } from "/file/snapshot-loader.js";
import { createSnapshotUpdates } from "/file/snapshot-updates.js";
import { acceptOwnerSnapshot, beginOwnerVerification, drainActionsAfterSnapshotRefresh, failOwnerSnapshot, mayDispatchActions, mayUsePrivateSnapshot } from "/file/snapshot-session.js";
import { escapeHtml } from "/file/preview-rendering.js";
import { validateTargetRelativeDirectory } from "/file/target-directory.js";
import { mediaLibraryAvailability, mediaLibraryEntries, resolveMediaLibrary } from "/file/library-view.js";
import { buildLibraryMutationPayload, isValidLibraryRootBinding } from "./library-actions.js";
import { captureLibraryEditorFocus, libraryDraftFieldValue, reconcileLibraryDrafts, restoreLibraryEditorFocus, setLibraryDraftField } from "./library-drafts.js";
import { takeRejectedSettingsActions } from "./local-action-rejections.js";
import { timeAgoMilliseconds, timeAgoSeconds } from "./time-format.js";

const API = "/api";
const loadOwnerSnapshot = createOwnerSnapshotLoader();
const ACTION_QUEUE_KEY = "file-manager.actions.v1";
const MAX_ACTION_QUEUE = ACTION_QUEUE_LIMIT;
const MAX_DISPLAY_SNAPSHOT_AGE_MS = 60 * 60 * 1_000;
const SETTINGS_KINDS = new Set([
  "library_register", "library_set_default", "library_set_category", "library_rename", "library_remove", "library_scan",
]);

const telegram = window.Telegram?.WebApp;
const initData = telegram?.initData ?? "";
const state = {
  ownerVerified: false,
  privateUiVisible: false,
  canDispatchActions: false,
  snapshot: null,
  loading: false,
  draining: null,
  retryDelay: 1_500,
  retryTimer: 0,
  quarantinedActionIds: new Set(),
  unknownActionNotices: new Map(),
  localActions: new Map(),
  libraryDrafts: new Map(),
  toastTimer: 0,
};
let snapshotUpdates = null;
let snapshotUpdateStatus = "connecting";
let snapshotRefreshPending = false;
let snapshotRefreshMessage = "";
const snapshotRefreshRequests = new Map();
let snapshotReadInFlight = null;
let snapshotReadInFlightGeneration = -1;
let snapshotReadInFlightEpoch = null;
let snapshotReadAbortController = null;
let snapshotReadSerial = 0;
let snapshotReadActiveToken = 0;
let epochVerificationPending = false;
let epochVerificationGeneration = 0;
let epochVerificationExpectedDatabaseEpoch = null;
let advertisedDatabaseEpoch = null;
let privateAccessExpired = false;
let applicationDisposed = false;
let applicationGeneration = 0;
let snapshotFreshnessTimer = 0;

const byId = (id) => document.getElementById(id);
const formatTime = (value, unit = "seconds") => {
  const number = Number(value);
  const date = new Date(unit === "seconds" ? number * 1000 : number);
  if (!Number.isFinite(date.getTime()) || date.getTime() <= 0) return "Not available";
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(date);
};
const formatBytes = (value) => {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes < 0) return "Size unavailable";
  if (bytes < 1_024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let size = bytes;
  let unit = -1;
  do { size /= 1_024; unit += 1; } while (size >= 1_024 && unit < units.length - 1);
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unit]}`;
};

function setConnection(mode, label) {
  const dot = byId("connection-dot");
  if (dot) dot.className = `connection-dot${mode === "live" ? " live" : mode === "stale" ? " stale" : ""}`;
  const connectionLabel = byId("connection-label");
  if (connectionLabel) connectionLabel.textContent = label;
}

function updateSnapshotUpdateStatus(status) {
  snapshotUpdateStatus = status;
  if (status === "expired") return;
  const age = snapshotAgeLabel(state.snapshot);
  if (snapshotRefreshPending) setConnection("", "Refreshing desktop snapshot…");
  else if (status === "offline") setConnection("stale", state.snapshot ? `Offline · saved ${age}` : "Offline · no saved snapshot");
  else if (status === "fallback" || status === "unavailable") setConnection("stale", state.snapshot ? `Saved snapshot · ${age}` : "Snapshot refresh unavailable");
  else setConnection(state.snapshot?.stale === true ? "stale" : "", state.snapshot ? `Saved snapshot · ${age}` : "Loading saved snapshot");
}

function refreshLocalSnapshotFreshness({ renderContent = true } = {}) {
  if (applicationDisposed) return;
  const snapshot = state.snapshot;
  if (!snapshot) return;
  const now = Date.now();
  const freshnessTime = Number.isSafeInteger(snapshot.reported_at) ? snapshot.reported_at : 0;
  const stale = epochVerificationPending
    || snapshot.synced !== true
    || freshnessTime <= 0
    || freshnessTime > now
    || now - freshnessTime > MAX_DISPLAY_SNAPSHOT_AGE_MS;
  const changed = snapshot.stale !== stale;
  if (changed) state.snapshot = { ...snapshot, stale };
  if (mayUsePrivateSnapshot(state)) {
    if (changed && renderContent) render();
    else renderSync();
  }
  updateSnapshotUpdateStatus(snapshotUpdateStatus);
}

function snapshotAgeLabel(snapshot) {
  const publishedAt = Number.isSafeInteger(snapshot?.reported_at) ? snapshot.reported_at : 0;
  return publishedAt > 0 ? timeAgoMilliseconds(publishedAt) : "age unavailable";
}

function setSnapshotRefreshStatus(message, mode = "") {
  snapshotRefreshMessage = message;
  const status = byId("snapshot-refresh-status");
  if (status) {
    status.textContent = snapshotRefreshMessage;
    status.className = `snapshot-refresh-status${mode ? ` ${mode}` : ""}`;
  }
  const button = byId("refresh-button");
  if (button) button.disabled = snapshotRefreshPending;
}

function updateSnapshotInterest() {
  if (applicationDisposed || !snapshotUpdates) return;
  const isAwaitingActionResult = (requestId, deliveryOutcome) => deliveryOutcome !== "outcome_unknown"
    && !state.quarantinedActionIds.has(requestId);
  const queued = readActionQueue().some((action) => actionBelongsToSurface(action, ACTION_SURFACE_SETTINGS)
    && isAwaitingActionResult(action.request_id, action.delivery_outcome));
  const localResultPending = [...state.localActions.entries()].some(([requestId, action]) =>
    isAwaitingActionResult(requestId, action.delivery_outcome));
  const pendingCloudRequest = (Array.isArray(state.snapshot?.pending_requests) ? state.snapshot.pending_requests : [])
    .some((request) => SETTINGS_KINDS.has(request?.kind) && request?.status === "waiting_for_local");
  const pendingDesktopOperation = (Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [])
    .some((operation) => SETTINGS_KINDS.has(operation?.kind)
      && ["pending", "processing", "retryable"].includes(String(operation?.status)));
  snapshotUpdates.setWatch(queued || state.draining !== null || localResultPending || pendingCloudRequest || pendingDesktopOperation);
}

function showToast(message) {
  if (applicationDisposed) return;
  const toast = byId("toast");
  if (!toast) return;
  toast.textContent = message;
  toast.classList.remove("hidden");
  clearTimeout(state.toastTimer);
  state.toastTimer = window.setTimeout(() => toast.classList.add("hidden"), 4_500);
}

function showPrivateAccessGate(message) {
  byId("app-content").classList.add("hidden");
  byId("auth-gate").classList.remove("hidden");
  byId("auth-message").textContent = message;
  setConnection("", "Owner verification required");
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers ?? {});
  headers.set("X-Telegram-Init-Data", initData);
  if (options.body !== undefined) headers.set("Content-Type", "application/json");
  const response = await fetch(`${API}${path}`, { ...options, headers, cache: "no-store" });
  let data;
  try { data = await response.json(); } catch { data = {}; }
  if (!response.ok) {
    throw new HttpResponseError(response.status, typeof data?.error === "string" ? data.error : `Request failed (${response.status})`);
  }
  return data;
}

function readActionQueue() {
  return readActionQueueState().actions;
}

function readActionQueueState() {
  try {
    const stored = localStorage.getItem(ACTION_QUEUE_KEY);
    if (stored === null) return { ok: true, actions: [] };
    const value = JSON.parse(stored);
    if (!Array.isArray(value)) return { ok: false, actions: [] };
    return {
      ok: true,
      actions: value.filter((entry) => entry && typeof entry.request_id === "string" && typeof entry.kind === "string" && entry.payload && typeof entry.payload === "object"),
    };
  } catch { return { ok: false, actions: [] }; }
}

function writeActionQueue(actions) {
  if (actions.length > MAX_ACTION_QUEUE) return false;
  try { localStorage.setItem(ACTION_QUEUE_KEY, JSON.stringify(actions)); } catch { return false; }
  if (!applicationDisposed) renderPending();
  return true;
}

function recoverSettingsActionReceipts() {
  const recovered = recoverActionReceipts(state, { surface: ACTION_SURFACE_SETTINGS });
  if (recovered.matched === 0) return recovered;

  const rejectedIds = recovered.rejectedRequestIds ?? [];
  const queueState = readActionQueueState();
  if (!queueState.ok) {
    quarantineRejectedActions(rejectedIds);
    return recovered;
  }

  const matchedIds = new Set(recovered.requestIds);
  const remaining = queueState.actions.filter((action) => !matchedIds.has(action.request_id));
  const queueRemovalSucceeded = remaining.length === queueState.actions.length || writeActionQueue(remaining);
  if (!queueRemovalSucceeded) {
    quarantineRejectedActions(rejectedIds);
    return recovered;
  }
  if (remaining.length === queueState.actions.length) renderPending();

  if (rejectedIds.length > 0) {
    // Rejection receipts stay durable until this origin has confirmed the
    // matching shared-queue rows are gone.
    consumeRejectedActionReceipts(state, { surface: ACTION_SURFACE_SETTINGS, requestIds: rejectedIds });
    const settled = settleRejectedSettingsActions(rejectedIds);
    if (settled.length > 0) showRejectedSettingsActions(settled);
  }
  return recovered;
}

function quarantineRejectedActions(requestIds) {
  let newlyQuarantined = false;
  for (const requestId of requestIds) {
    if (!state.quarantinedActionIds.has(requestId)) newlyQuarantined = true;
    state.quarantinedActionIds.add(requestId);
  }
  if (requestIds.length > 0) renderPending();
  if (newlyQuarantined) {
    showToast("A rejected settings request is still saved in this browser because its queue removal could not be confirmed. It is paused until browser storage works again.");
  }
  if (requestIds.length > 0) scheduleActionRetry(true);
}

function settleRejectedSettingsActions(requestIds) {
  const settled = takeRejectedSettingsActions(state.localActions, requestIds);
  for (const action of settled) {
    state.quarantinedActionIds.delete(action.requestId);
    if (action.kind === "library_register") {
      const form = byId("register-form");
      if (form?.dataset.pendingRequestId === action.requestId) delete form.dataset.pendingRequestId;
    }
  }
  if (settled.length > 0) renderPending();
  updateSnapshotInterest();
  return settled;
}

function rejectionNotices(actions) {
  return actions.map((action) => `${action.label} was rejected before reaching the desktop: ${action.error}`);
}

function showRejectedSettingsActions(actions) {
  const notices = rejectionNotices(actions).slice(0, 2);
  if (actions.length > notices.length) notices.push(`${actions.length - notices.length} more settings request(s) were rejected.`);
  if (notices.length > 0) showToast(notices.join(" · "));
}

function scheduleActionRetry(quiet) {
  if (state.retryTimer) return;
  const delay = state.retryDelay;
  state.retryDelay = Math.min(state.retryDelay * 2, 30_000);
  state.retryTimer = window.setTimeout(() => {
    state.retryTimer = 0;
    drainActionQueue({ quiet });
  }, delay);
}

function clearLocalSettingsAction(action, message) {
  state.localActions.delete(action.request_id);
  updateSnapshotInterest();
  showToast(message);
  renderPending();
}

function settleLocalActions() {
  const operations = Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [];
  for (const [requestId, local] of state.localActions) {
    if (!Number.isSafeInteger(local.seq)) continue;
    const operation = operations.find((entry) => entry?.seq === local.seq && entry?.kind === local.kind);
    if (!operation || !["completed", "failed", "stale", "uncertain"].includes(String(operation.status))) continue;
    const error = String(operation.error ?? operation.result?.error ?? "The local operation did not complete.");
    if (operation.status === "completed" && local.kind === "library_register") {
      const form = byId("register-form");
      if (form && form.dataset.pendingRequestId === requestId) {
        form.reset();
        delete form.dataset.pendingRequestId;
      }
    }
    clearLocalSettingsAction(
      { request_id: requestId },
      operation.status === "completed"
        ? `${local.label} was applied by the desktop.`
        : `${local.label} failed on the desktop: ${error}`,
    );
  }
}

async function drainActionQueue({ quiet = false } = {}) {
  if (!initData || applicationDisposed || !mayDispatchActions(state) || !navigator.onLine) return false;
  const pageGeneration = applicationGeneration;
  while (state.draining) {
    const activeDrain = state.draining;
    await activeDrain;
    if (!initData || applicationDisposed || pageGeneration !== applicationGeneration || !mayDispatchActions(state) || !navigator.onLine) return false;
  }
  recoverSettingsActionReceipts();
  const restored = restorePendingActions(state, {
    surface: ACTION_SURFACE_SETTINGS,
    readQueue: readActionQueue,
    writeQueue: writeActionQueue,
  });
  if (restored.writeFailed && !quiet) showToast("This browser could not restore a saved settings request. It will retry when storage is available.");
  else if (restored.skippedAtLimit > 0 && !quiet) showToast("The browser request queue is full. A saved settings request remains pending in this tab.");
  state.draining = (async () => {
    let sentAny = false;
    let interrupted = false;
    let receiptFailed = false;
    const rejected = [];
    while (navigator.onLine && !applicationDisposed && pageGeneration === applicationGeneration) {
      const outcome = await dispatchNextOwnedAction({
        surface: ACTION_SURFACE_SETTINGS,
        readQueue: readActionQueue,
        writeQueue: writeActionQueue,
        excludedIds: state.quarantinedActionIds,
        dispatch: (action) => api("/actions", {
          method: "POST",
          body: JSON.stringify({
            request_id: action.request_id,
            kind: action.kind,
            payload: action.payload,
            queued_at_ms: action.queued_at_ms,
            replay_expires_at_ms: action.replay_expires_at_ms,
          }),
        }),
        onReceipt: (action, receipt, surface) => recordActionReceipt(action, receipt, state, { surface }),
      });
      if (applicationDisposed || pageGeneration !== applicationGeneration) break;
      if (outcome.status === "empty") break;
      if (outcome.status === "outcome_unknown") {
        state.quarantinedActionIds.add(outcome.action.request_id);
        state.unknownActionNotices.set(outcome.action.request_id, outcome.action);
        const local = state.localActions.get(outcome.action.request_id);
        if (local) {
          local.delivery_outcome = "outcome_unknown";
          local.delivery_error_code = outcome.code;
          local.delivery_error = outcome.error;
        }
        renderPending();
        continue;
      }
      if (outcome.status === "remove_failed") {
        if (!quiet) showToast("The request reached the cloud inbox, but this browser could not update its retry queue.");
        break;
      }
      if (outcome.status === "sent") {
        sentAny = true;
        continue;
      }
      if (outcome.status === "rejected") {
        const settled = settleRejectedSettingsActions(outcome.receiptUpdate?.rejectedRequestIds ?? []);
        if (settled.length > 0) rejected.push(...rejectionNotices(settled));
        else rejected.push("A saved settings request was rejected before the desktop could apply it.");
        continue;
      }
      if (outcome.status === "rejection_receipt_failed") {
        receiptFailed = true;
        scheduleActionRetry(quiet);
        break;
      }
      if (outcome.status === "rejection_remove_failed") {
        state.quarantinedActionIds.add(outcome.action.request_id);
        renderPending();
        if (!quiet) showToast("The cloud rejected this settings request, but browser storage could not remove it from the retry queue. It is paused until cleanup succeeds.");
        scheduleActionRetry(quiet);
        break;
      }
      if (outcome.status === "receipt_failed") {
        receiptFailed = true;
        scheduleActionRetry(quiet);
        break;
      }

      const { action, error } = outcome;
      if (error?.status === 401 || error?.status === 403) {
        handleAuthorizationFailure();
        break;
      }
      if (!shouldRetryActionFailure(error)) {
        state.quarantinedActionIds.add(action.request_id);
        renderPending();
        const local = state.localActions.get(action.request_id);
        if (local) rejected.push(`${local.label} could not be safely finalized: ${error.message}. The request remains paused in this browser queue.`);
        else rejected.push("A settings request could not be safely finalized. It remains paused in this browser queue.");
        break;
      }
      interrupted = true;
      scheduleActionRetry(quiet);
      break;
    }
    if (applicationDisposed || pageGeneration !== applicationGeneration) return sentAny;
    if (sentAny) state.retryDelay = 1_500;
    if (rejected.length) showToast(`Request rejected before local execution: ${[...new Set(rejected)].slice(0, 2).join(" · ")}`);
    else if (!quiet && receiptFailed) showToast("The request reached the cloud inbox, but this browser could not save its receipt. It will retry with the same ID.");
    else if (!quiet && interrupted) showToast("Connection interrupted. Saved requests will retry with their same IDs.");
    return sentAny;
  })();
  let sentAny = false;
  try { sentAny = await state.draining; }
  finally { state.draining = null; }
  if (applicationDisposed || pageGeneration !== applicationGeneration) return sentAny;
  updateSnapshotInterest();
  return sentAny;
}

async function submitAction(kind, payload, label, onQueued = null) {
  if (applicationDisposed || !mayDispatchActions(state)) return false;
  const action = createQueuedAction({ request_id: crypto.randomUUID(), kind, payload, surface: ACTION_SURFACE_SETTINGS });
  const queued = appendAction(readActionQueue(), action);
  if (!queued.ok) return showToast("The browser retry queue is full. Send its saved requests first."), false;
  if (!writeActionQueue(queued.queue)) return showToast("This browser could not save the request, so it was not sent."), false;
  state.localActions.set(action.request_id, {
    kind,
    payload,
    label,
    seq: null,
    queued_at_ms: action.queued_at_ms,
    replay_expires_at_ms: action.replay_expires_at_ms,
  });
  updateSnapshotInterest();
  onQueued?.();
  if (kind === "library_register") byId("register-form").dataset.pendingRequestId = action.request_id;
  showToast(navigator.onLine ? `${label} saved. Waiting for the desktop to apply it.` : `${label} saved in this browser. It has not reached the desktop.`);
  if (navigator.onLine) await drainActionQueue();
  else renderPending();
  if (applicationDisposed) return false;
  return true;
}

function renderSync() {
  const snapshot = state.snapshot ?? {};
  const synced = snapshot.synced === true;
  byId("sync-badge").className = `badge${synced && !snapshot.stale ? " good" : synced ? " warning" : ""}`;
  byId("sync-badge").textContent = !synced ? "Not synced" : snapshot.stale ? "Snapshot outdated" : "Saved snapshot";
  byId("sync-title").textContent = !synced
    ? "No saved settings snapshot"
    : snapshot.stale ? "Showing the last saved settings snapshot" : "Saved settings snapshot";
  byId("sync-detail").textContent = !synced
    ? "Applied settings appear here after a snapshot is published."
    : `Snapshot published ${snapshotAgeLabel(snapshot)}. New requests stay pending until local execution finishes.`;
  setSnapshotRefreshStatus(snapshotRefreshMessage, snapshotRefreshPending ? "pending" : "");
}

function libraryDescriptor(entry) {
  // mediaLibraryEntries already joins applied settings and live status. Do not
  // re-merge a stale status descriptor here: it could restore the old root
  // binding after the registry reports a replacement as unavailable.
  return entry;
}

function renderLibraryCard(rawEntry) {
  const entry = libraryDescriptor(rawEntry);
  const id = String(entry.id ?? "");
  const name = String(entry.name ?? "Unnamed library");
  const root = String(entry.root ?? "Path unavailable");
  const rootBinding = typeof entry.root_binding === "string" ? entry.root_binding : "";
  const canMutateRegistration = isValidLibraryRootBinding(rootBinding);
  const category = String(entry.default_category ?? "");
  const nameDraft = libraryDraftFieldValue(state.libraryDrafts, entry, "name");
  const categoryDraft = libraryDraftFieldValue(state.libraryDrafts, entry, "default_category");
  const availability = mediaLibraryAvailability(entry);
  const status = entry.snapshot_stale === true
    ? availability === "offline" ? "Offline · last saved view only" : "Scan pending · saved view only"
    : availability === "offline"
      ? "Offline · last saved view only"
      : availability === "available" ? "Available on the desktop" : "Availability not reported";
  const resolvedSnapshot = resolveMediaLibrary(state.snapshot, id).snapshot;
  const snapshot = entry.snapshot && typeof entry.snapshot === "object" ? entry.snapshot : resolvedSnapshot;
  const mediaCount = Number.isSafeInteger(entry.media_count)
    ? Number(entry.media_count)
    : Array.isArray(snapshot?.items) ? snapshot.items.length : null;
  const scanTime = entry.scanned_at ?? snapshot?.scanned_at ?? null;
  const idAttr = escapeHtml(id);
  const rootBindingAttr = escapeHtml(rootBinding);
  const registrationMutationDisabled = canMutateRegistration ? "" : "disabled";
  const nameAttr = escapeHtml(name);
  const categoryAttr = escapeHtml(category);
  const badges = [
    entry.is_default === true ? '<span class="badge good">Default library</span>' : "",
    `<span class="badge${entry.snapshot_stale === true || availability === "offline" ? " warning" : availability === "available" ? " good" : ""}">${escapeHtml(status)}</span>`,
  ].filter(Boolean).join("");
  const dependentWarning = entry.is_default === true
    ? '<p class="settings-status-note">Choose another default library before removing this one.</p>'
    : '<p class="settings-status-note">Removing this registration keeps media files in place. The desktop will block removal while tasks depend on this library.</p>';
  return `<article class="settings-library-card">
    <div class="settings-library-heading"><div><h3>${nameAttr}</h3><p>Default download category: <strong>${categoryAttr || "Library root"}</strong></p></div><div class="settings-library-badges">${badges}</div></div>
    <code class="settings-library-path">${escapeHtml(root)}</code>
    <div class="settings-library-stats"><span class="badge">${mediaCount === null ? "Media count unavailable" : `${mediaCount.toLocaleString()} media items`}</span><span class="badge">Last scan: ${escapeHtml(scanTime ? formatTime(scanTime) : "Not scanned")}</span></div>
    ${entry.snapshot_stale === true ? '<p class="settings-status-note">This is the last saved scan. Cached items are unavailable for local actions until the root is scanned again.</p>' : ""}
    <div class="settings-card-actions">
      ${entry.is_default === true ? "" : `<button class="small-button" type="button" data-settings-action="library_set_default" data-library-id="${idAttr}" data-library-root-binding="${rootBindingAttr}" ${registrationMutationDisabled}>Set as default</button>`}
      <button class="small-button" type="button" data-settings-action="library_scan" data-library-id="${idAttr}">Scan this library</button>
      <button class="small-button danger" type="button" data-settings-action="library_remove" data-library-id="${idAttr}" data-library-root-binding="${rootBindingAttr}" data-library-is-default="${entry.is_default === true}" ${entry.is_default === true || !canMutateRegistration ? "disabled" : ""}>Remove registration</button>
    </div>
    ${canMutateRegistration ? "" : '<p class="settings-status-note">Refresh settings to verify this library before changing its registration.</p>'}
    ${dependentWarning}
    <div class="settings-edit-grid">
      <form class="settings-edit-form" data-rename-form="${idAttr}" data-library-root="${escapeHtml(root)}" data-library-root-binding="${rootBindingAttr}" data-library-name="${nameAttr}" data-library-default-category="${categoryAttr}">
        <label class="field-label" for="rename-${idAttr}">Rename library</label>
        <input id="rename-${idAttr}" class="text-input" name="name" type="text" maxlength="100" required value="${escapeHtml(nameDraft)}">
        <button class="small-button" type="submit" ${registrationMutationDisabled}>Save name</button>
      </form>
      <form class="settings-edit-form" data-category-form="${idAttr}" data-library-root="${escapeHtml(root)}" data-library-root-binding="${rootBindingAttr}" data-library-name="${nameAttr}" data-library-default-category="${categoryAttr}">
        <label class="field-label" for="category-${idAttr}">Default category</label>
        <input id="category-${idAttr}" class="text-input" name="default_category" type="text" maxlength="1024" value="${escapeHtml(categoryDraft)}" placeholder="Library root">
        <button class="small-button" type="submit" ${registrationMutationDisabled}>Save category</button>
      </form>
    </div>
  </article>`;
}

function renderLibraries() {
  const entries = mediaLibraryEntries(state.snapshot).map(libraryDescriptor);
  reconcileLibraryDrafts(state.libraryDrafts, entries);
  byId("library-empty").classList.toggle("hidden", entries.length > 0);
  const list = byId("library-list");
  const focus = captureLibraryEditorFocus(list, document.activeElement, entries);
  list.innerHTML = entries.map(renderLibraryCard).join("");
  restoreLibraryEditorFocus(list, focus, entries);
  const stale = state.snapshot?.stale === true || state.snapshot?.synced !== true;
  byId("library-list").classList.toggle("settings-applied-warning", stale);
}

function kindLabel(kind) {
  return String(kind ?? "request").replaceAll("_", " ").replace(/^\w/u, (letter) => letter.toLocaleUpperCase());
}

function statusLabel(status) {
  return String(status ?? "pending").replaceAll("_", " ").replace(/^\w/u, (letter) => letter.toLocaleUpperCase());
}

function renderPending() {
  const snapshot = state.snapshot ?? {};
  const queue = readActionQueue();
  const unknownById = new Map(queue
    .filter((action) => SETTINGS_KINDS.has(action.kind) && action.delivery_outcome === "outcome_unknown")
    .map((action) => [action.request_id, action]));
  for (const [requestId, action] of state.unknownActionNotices) {
    if (SETTINGS_KINDS.has(action?.kind) && !unknownById.has(requestId)) unknownById.set(requestId, action);
  }
  const unknown = [...unknownById.values()];
  const local = queue.filter((action) => SETTINGS_KINDS.has(action.kind)
    && action.delivery_outcome !== "outcome_unknown"
    && !state.quarantinedActionIds.has(action.request_id));
  const pending = (Array.isArray(snapshot.pending_requests) ? snapshot.pending_requests : [])
    .filter((entry) => SETTINGS_KINDS.has(entry?.kind));
  const inProgress = (Array.isArray(snapshot.operations) ? snapshot.operations : [])
    .filter((entry) => SETTINGS_KINDS.has(entry?.kind) && ["pending", "processing", "retryable"].includes(String(entry?.status)));
  const rows = [];
  for (const action of unknown) rows.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(kindLabel(action.kind))} · delivery outcome unknown</h3><p>${escapeHtml(action.delivery_error ?? "The replay window ended before a receipt could be confirmed. Check the desktop before dismissing this request.")}</p><p>This request will not be sent again automatically. Check the desktop for its result, then dismiss this notice.</p></div><span class="state-pill pending">Check desktop</span></div><button class="small-button" type="button" data-dismiss-unknown-action="${escapeHtml(action.request_id)}">I checked the desktop · dismiss</button></article>`);
  for (const action of local) rows.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(kindLabel(action.kind))}</h3><p>Saved in this browser retry queue. No local change has been applied.</p></div><span class="state-pill pending">Browser queue</span></div></article>`);
  for (const request of pending) rows.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(kindLabel(request.kind))}</h3><p>Saved in the cloud inbox. Waiting for the desktop; applied settings have not changed yet.</p></div><span class="state-pill pending">Waiting · #${escapeHtml(request.seq)}</span></div><p>Received ${escapeHtml(timeAgoMilliseconds(request.created_at))}</p></article>`);
  for (const operation of inProgress) rows.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(kindLabel(operation.kind))}</h3><p>${escapeHtml(operation.error ?? "The desktop received this request and is working on it.")}</p></div><span class="state-pill pending">${escapeHtml(statusLabel(operation.status))}</span></div>${operation.updated_at ? `<p>Updated ${escapeHtml(timeAgoSeconds(operation.updated_at))}</p>` : ""}</article>`);
  byId("waiting-list").innerHTML = rows.length
    ? rows.join("")
    : '<div class="empty-state"><div class="empty-art" aria-hidden="true">✓</div><h2>Nothing is waiting</h2><p>New library requests appear here until the desktop finishes them.</p></div>';
  const queuedCount = queue.filter((action) => action.delivery_outcome !== "outcome_unknown"
    && !state.quarantinedActionIds.has(action.request_id)).length;
  byId("queued-actions").textContent = queuedCount ? `${queuedCount} saved request${queuedCount === 1 ? "" : "s"}` : "";
}

function settingsOperations() {
  return (Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [])
    .filter((entry) => SETTINGS_KINDS.has(entry?.kind))
    .slice(-30)
    .reverse();
}

function renderOperations() {
  const operations = settingsOperations();
  byId("operations-list").innerHTML = operations.length
    ? operations.map((entry) => {
      const isCompleted = entry.status === "completed";
      const isFailure = ["failed", "stale", "uncertain"].includes(String(entry.status));
      const result = entry.result && typeof entry.result === "object" ? entry.result : {};
      const message = isFailure
        ? entry.error ?? result.error ?? "The desktop could not apply this request."
        : isCompleted ? result.message ?? "The desktop completed this request." : "Waiting for the local result.";
      const label = kindLabel(entry.kind);
      const root = result.root ?? result.path;
      const detail = [message, root, result.name, result.default_category].filter(Boolean).join(" · ");
      return `<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(label)}</h3><p>${escapeHtml(detail)}</p>${entry.error ? `<p class="warning-text">${escapeHtml(entry.error)}</p>` : ""}</div><span class="state-pill${isFailure ? " pending" : isCompleted ? "" : " pending"}">${escapeHtml(statusLabel(entry.status))}</span></div>${entry.updated_at ? `<p>Updated ${escapeHtml(timeAgoSeconds(entry.updated_at))}</p>` : ""}</article>`;
    }).join("")
    : '<div class="empty-state"><div class="empty-art" aria-hidden="true">◷</div><h2>No local settings receipts yet</h2><p>Completed and failed settings requests will appear here.</p></div>';
}

function render() {
  if (!mayUsePrivateSnapshot(state)) return;
  renderSync();
  renderLibraries();
  renderPending();
  renderOperations();
  settleLocalActions();
  updateSnapshotInterest();
}

function normalizeName(value) {
  const name = String(value ?? "").trim();
  if (!name || new TextEncoder().encode(name).byteLength > 100 || /[\u0000-\u001f\u007f]/u.test(name)) {
    return { name, error: "Enter a name of 1–100 UTF-8 bytes without control characters." };
  }
  return { name, error: null };
}

function normalizeRoot(value) {
  const path = String(value ?? "").trim();
  const validAbsolute = path.startsWith("/") || /^[A-Za-z]:[\\/]/u.test(path) || /^\\\\[^\\]/u.test(path);
  if (!validAbsolute || !path || new TextEncoder().encode(path).byteLength > 1_024 || /[\u0000-\u001f\u007f]/u.test(path)) {
    return { path, error: "Enter an absolute desktop path of at most 1,024 UTF-8 bytes." };
  }
  return { path, error: null };
}

function normalizeCategory(value) {
  return validateTargetRelativeDirectory(value);
}

function displayedLibraryFromForm(form) {
  return {
    id: form.dataset.renameForm ?? form.dataset.categoryForm,
    root: form.dataset.libraryRoot,
    root_binding: form.dataset.libraryRootBinding,
    name: form.dataset.libraryName,
    default_category: form.dataset.libraryDefaultCategory,
  };
}

function bindEvents() {
  byId("refresh-button").addEventListener("click", () => refreshState());
  byId("waiting-list").addEventListener("click", (event) => {
    const button = event.target.closest("[data-dismiss-unknown-action]");
    if (!button) return;
    const requestId = button.dataset.dismissUnknownAction;
    const action = readActionQueue().find((entry) => entry.request_id === requestId && entry.delivery_outcome === "outcome_unknown")
      ?? state.unknownActionNotices.get(requestId);
    if (!action) return renderPending();
    if (!window.confirm("Only dismiss this notice after checking the desktop for the request’s result. The browser will not send this request again.")) return;
    if (!writeActionQueue(removeQueuedAction(readActionQueue(), requestId))) {
      showToast("This browser could not dismiss the checked request. Its notice remains saved.");
      return;
    }
    state.localActions.delete(requestId);
    state.unknownActionNotices.delete(requestId);
    state.quarantinedActionIds.delete(requestId);
    renderPending();
  });
  byId("register-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const name = normalizeName(form.elements.name.value);
    const path = normalizeRoot(form.elements.path.value);
    const category = normalizeCategory(form.elements.default_category.value);
    const error = name.error ?? path.error ?? category.error;
    if (error) return showToast(error);
    submitAction("library_register", {
      name: name.name,
      path: path.path,
      default_category: category.directory,
      set_default: form.elements.set_default.checked,
      create_directory: form.elements.create_directory.checked,
    }, `Register “${name.name}”`);
  });
  byId("library-list").addEventListener("submit", (event) => {
    const form = event.target.closest("form[data-rename-form], form[data-category-form]");
    if (!form) return;
    event.preventDefault();
    const displayedEntry = displayedLibraryFromForm(form);
    if (form.dataset.renameForm) {
      const name = normalizeName(form.elements.name.value);
      if (name.error) return showToast(name.error);
      const payload = buildLibraryMutationPayload("library_rename", displayedEntry, { name: name.name });
      if (!payload) return showToast("Refresh settings to verify this library before changing its registration.");
      submitAction("library_rename", payload, `Rename library to “${name.name}”`, () => {
        setLibraryDraftField(state.libraryDrafts, displayedEntry, "name", name.name);
      });
    } else {
      const category = normalizeCategory(form.elements.default_category.value);
      if (category.error) return showToast(category.error);
      const payload = buildLibraryMutationPayload("library_set_category", displayedEntry, { default_category: category.directory });
      if (!payload) return showToast("Refresh settings to verify this library before changing its registration.");
      submitAction("library_set_category", payload, "Update default category", () => {
        setLibraryDraftField(state.libraryDrafts, displayedEntry, "default_category", category.directory);
      });
    }
  });
  byId("library-list").addEventListener("input", (event) => {
    const form = event.target.closest("form[data-rename-form], form[data-category-form]");
    if (!form) return;
    const displayedEntry = displayedLibraryFromForm(form);
    if (!isValidLibraryRootBinding(displayedEntry.root_binding)) return;
    const currentEntry = mediaLibraryEntries(state.snapshot).map(libraryDescriptor)
      .find((candidate) => candidate.id === displayedEntry.id);
    if (!currentEntry || currentEntry.root_binding !== displayedEntry.root_binding) return;
    const field = form.dataset.renameForm ? "name" : "default_category";
    setLibraryDraftField(state.libraryDrafts, displayedEntry, field, event.target.value);
  });
  window.addEventListener("storage", (event) => {
    if (!event.key?.startsWith(ACTION_RECEIPT_STORAGE_PREFIX)) return;
    const recovered = recoverSettingsActionReceipts();
    if (recovered.matched === 0) return;
    renderPending();
    if (state.ownerVerified) {
      if (!state.loading) void performRefreshState({ quiet: true }).catch(() => {});
    }
  });
  byId("library-list").addEventListener("click", (event) => {
    const button = event.target.closest("[data-settings-action]");
    if (!button) return;
    const kind = button.dataset.settingsAction;
    const libraryId = button.dataset.libraryId;
    if (kind === "library_scan") {
      submitAction(kind, { library_id: libraryId }, "Scan library");
      return;
    }
    const payload = buildLibraryMutationPayload(kind, {
      id: libraryId,
      root_binding: button.dataset.libraryRootBinding,
    });
    if (!payload) return showToast("Refresh settings to verify this library before changing its registration.");
    if (kind === "library_remove") {
      if (button.dataset.libraryIsDefault === "true") return showToast("Set another library as default before removing this one.");
      if (!window.confirm("Remove this library registration? Its media files stay in place. The desktop checks for dependent tasks before applying the change.")) return;
    }
    submitAction(kind, payload, kind === "library_remove" ? "Remove library registration" : "Change default library");
  });
  window.addEventListener("online", () => {
    showToast("Connection restored. Sending saved requests.");
    drainActionQueue();
  });
}

function applySnapshotFailure(status) {
  const transition = failOwnerSnapshot(state, status, Date.now());
  Object.assign(state, transition);
  if (transition.authorizationRejected) {
    if (state.retryTimer) window.clearTimeout(state.retryTimer);
    state.retryTimer = 0;
  }
  return transition;
}

function handleAuthorizationFailure() {
  privateAccessExpired = true;
  snapshotUpdates?.stop();
  applySnapshotFailure(401);
  showPrivateAccessGate("Owner access could not be verified. Reopen this Mini App from the owner’s private chat.");
}

function markEpochVerificationPending(databaseEpoch = null) {
  if (applicationDisposed) return;
  const expectedEpoch = typeof databaseEpoch === "string" ? databaseEpoch : null;
  if (epochVerificationPending && epochVerificationExpectedDatabaseEpoch === expectedEpoch) return;
  epochVerificationPending = true;
  epochVerificationExpectedDatabaseEpoch = expectedEpoch;
  epochVerificationGeneration += 1;
  state.canDispatchActions = false;
  if (state.snapshot) state.snapshot = { ...state.snapshot, stale: true };
  if (mayUsePrivateSnapshot(state)) render();
}

function linkSnapshotReadAbort(signal) {
  if (!signal || !snapshotReadAbortController) return () => {};
  const controller = snapshotReadAbortController;
  const abort = () => controller.abort();
  if (signal.aborted) abort();
  else signal.addEventListener("abort", abort, { once: true });
  return () => signal.removeEventListener("abort", abort);
}

function createSnapshotReadAbortController() {
  const Controller = window.AbortController ?? globalThis.AbortController;
  return typeof Controller === "function" ? new Controller() : null;
}

function settleSnapshotReadOnAbort(read, signal, onAbort = () => {}) {
  if (!signal) return read;
  if (signal.aborted) {
    onAbort();
    return Promise.resolve(state.snapshot);
  }
  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", abort);
      callback(value);
    };
    const abort = () => {
      if (settled) return;
      onAbort();
      finish(resolve, state.snapshot);
    };
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    Promise.resolve(read).then(
      (value) => finish(resolve, value),
      (error) => finish(reject, error),
    );
  });
}

function performRefreshState(options = {}) {
  if (options.epochChanged === true) markEpochVerificationPending(options.expectedDatabaseEpoch);
  const generation = epochVerificationGeneration;
  const minimumVersion = Number.isSafeInteger(options.minimumVersion) ? options.minimumVersion : 0;
  const expectedDatabaseEpoch = typeof options.expectedDatabaseEpoch === "string"
    ? options.expectedDatabaseEpoch
    : epochVerificationPending ? epochVerificationExpectedDatabaseEpoch : null;
  if (snapshotReadInFlight) {
    const inFlight = snapshotReadInFlight;
    const inFlightGeneration = snapshotReadInFlightGeneration;
    const inFlightEpoch = snapshotReadInFlightEpoch;
    const unlinkAbort = linkSnapshotReadAbort(options.signal);
    return inFlight.then((snapshot) => {
      if (applicationDisposed || options.signal?.aborted) return state.snapshot;
      const version = Number.isSafeInteger(snapshot?.state_version) ? snapshot.state_version : 0;
      const epochReadRequired = epochVerificationPending && (
        inFlightGeneration !== epochVerificationGeneration
        || expectedDatabaseEpoch !== null && (inFlightEpoch !== expectedDatabaseEpoch || snapshot?.database_epoch !== expectedDatabaseEpoch)
      );
      const sameEpoch = !expectedDatabaseEpoch || snapshot?.database_epoch === expectedDatabaseEpoch;
      return minimumVersion > version && sameEpoch || epochReadRequired
        ? performRefreshState({ ...options, epochChanged: false, ...(expectedDatabaseEpoch ? { expectedDatabaseEpoch } : {}) })
        : snapshot;
    }).finally(unlinkAbort);
  }
  const readController = createSnapshotReadAbortController();
  snapshotReadAbortController = readController;
  const unlinkAbort = linkSnapshotReadAbort(options.signal);
  const readSignal = readController?.signal ?? options.signal ?? null;
  const readApplicationGeneration = applicationGeneration;
  const read = performRefreshStateOnce({ ...options, signal: readSignal, epochGeneration: generation, expectedDatabaseEpoch });
  const readToken = snapshotReadActiveToken;
  const inFlight = settleSnapshotReadOnAbort(
    read,
    readSignal,
    () => {
      if (snapshotReadActiveToken !== readToken) return;
      snapshotReadActiveToken = 0;
      state.loading = false;
      if (!applicationDisposed && readApplicationGeneration === applicationGeneration) byId("app-content")?.setAttribute("aria-busy", "false");
    },
  );
  const trackedRead = inFlight.finally(() => {
    unlinkAbort();
    if (snapshotReadInFlight === trackedRead) {
      snapshotReadInFlight = null;
      snapshotReadInFlightGeneration = -1;
      snapshotReadInFlightEpoch = null;
      snapshotReadAbortController = null;
    }
  });
  snapshotReadInFlight = trackedRead;
  snapshotReadInFlightGeneration = generation;
  snapshotReadInFlightEpoch = expectedDatabaseEpoch;
  return trackedRead;
}

async function performRefreshStateOnce({ quiet = false, minimumVersion = 0, epochGeneration = -1, expectedDatabaseEpoch = null, signal = null } = {}) {
  if (!initData || privateAccessExpired || applicationDisposed || signal?.aborted) return state.snapshot;
  const pageGeneration = applicationGeneration;
  const readToken = ++snapshotReadSerial;
  snapshotReadActiveToken = readToken;
  recoverSettingsActionReceipts();
  state.loading = true;
  byId("app-content")?.setAttribute("aria-busy", "true");
  try {
    const fresh = await loadOwnerSnapshot((path, headers) => api(path, { headers, signal }), { signal });
    if (signal?.aborted) return state.snapshot;
    if (applicationDisposed || pageGeneration !== applicationGeneration || epochGeneration !== epochVerificationGeneration) return state.snapshot;
    if (privateAccessExpired) return;
    if (expectedDatabaseEpoch && fresh.database_epoch !== expectedDatabaseEpoch) {
      throw new Error("The owner snapshot belongs to a different database epoch.");
    }
    if (!epochVerificationPending && advertisedDatabaseEpoch && fresh.database_epoch !== advertisedDatabaseEpoch) {
      markEpochVerificationPending(advertisedDatabaseEpoch);
    }
    const sameEpoch = !state.snapshot || fresh.database_epoch === state.snapshot.database_epoch;
    const currentVersion = sameEpoch && Number.isSafeInteger(state.snapshot?.state_version) ? state.snapshot.state_version : 0;
    if (minimumVersion > 0 && fresh.state_version < minimumVersion) throw new Error("The published snapshot is behind the desktop refresh result.");
    if (sameEpoch && fresh.state_version < currentVersion) return state.snapshot;
    if (epochVerificationPending && epochGeneration === epochVerificationGeneration
        && (!epochVerificationExpectedDatabaseEpoch || fresh.database_epoch === epochVerificationExpectedDatabaseEpoch)) {
      epochVerificationPending = false;
      epochVerificationExpectedDatabaseEpoch = null;
    }
    Object.assign(state, acceptOwnerSnapshot(fresh, state));
    if (epochVerificationPending) {
      state.canDispatchActions = false;
      state.snapshot = { ...state.snapshot, stale: true };
    }
    byId("auth-gate").classList.add("hidden");
    byId("app-content").classList.remove("hidden");
    refreshLocalSnapshotFreshness({ renderContent: false });
    render();
    updateSnapshotUpdateStatus(snapshotUpdateStatus);
    updateSnapshotInterest();
    // Snapshot verification completes independently of queue delivery. A held POST receipt
    // must not make a later correlated publication reread wait behind that action.
    void drainActionsAfterSnapshotRefresh(state, navigator.onLine, () => drainActionQueue({ quiet })).catch(() => {});
    return fresh;
  } catch (error) {
    if (signal?.aborted) return state.snapshot;
    if (applicationDisposed || pageGeneration !== applicationGeneration || epochGeneration !== epochVerificationGeneration) return state.snapshot;
    const transition = applySnapshotFailure(error?.status);
    if (transition.authorizationRejected) {
      privateAccessExpired = true;
      snapshotUpdates?.stop();
    }
    if (epochVerificationPending && !transition.authorizationRejected) {
      state.canDispatchActions = false;
      if (state.snapshot) state.snapshot = { ...state.snapshot, stale: true };
    }
    if (mayUsePrivateSnapshot(state)) {
      setConnection("stale", "Showing last saved settings");
      render();
    } else {
      showPrivateAccessGate(transition.authorizationRejected
        ? "Owner access could not be verified. Reopen this Mini App from the owner’s private chat."
        : "Private settings stay hidden until the owner is verified. Check the connection and retry.");
    }
    if (!transition.authorizationRejected) updateSnapshotUpdateStatus(snapshotUpdateStatus);
    if (!quiet && navigator.onLine && mayUsePrivateSnapshot(state)) showToast(error.message);
    throw error;
  } finally {
    if (snapshotReadActiveToken === readToken) {
      snapshotReadActiveToken = 0;
      state.loading = false;
      if (!applicationDisposed && pageGeneration === applicationGeneration) byId("app-content")?.setAttribute("aria-busy", "false");
    }
  }
}

function refreshErrorMessage(errorCode) {
  if (errorCode === "offline") return "Offline · showing the saved settings snapshot. Reconnect to request a desktop refresh.";
  if (errorCode === "unsupported") return "Desktop refresh is unavailable on this connection. Showing the saved settings snapshot.";
  if (errorCode === "hidden" || errorCode === "not_visible" || errorCode === "viewer_not_visible") return "The page is hidden; the saved settings snapshot remains available.";
  if (errorCode === "timeout" || errorCode === "refresh_timeout") return "No desktop refresh result arrived within two minutes. Showing the saved settings snapshot.";
  if (errorCode === "local_unavailable") return "No desktop app is connected to refresh now. Showing the saved settings snapshot.";
  if (errorCode === "cloud_unavailable") return "The cloud could not check the desktop connection. Showing the saved settings snapshot.";
  if (errorCode === "unavailable" || errorCode === "transport_unavailable") return "The desktop is unavailable. Showing the saved settings snapshot.";
  return "Desktop refresh failed. Showing the saved settings snapshot.";
}

function refreshState({ reason = "manual", quiet = false } = {}) {
  const key = "settings";
  const existing = snapshotRefreshRequests.get(key);
  if (existing) {
    if (!quiet) existing.notifyUser = true;
    return existing.promise;
  }
  const entry = { notifyUser: !quiet, promise: null };
  snapshotRefreshPending = true;
  setSnapshotRefreshStatus("Requesting current settings from the desktop…", "pending");
  updateSnapshotUpdateStatus(snapshotUpdateStatus);
  entry.promise = (async () => {
    let mode = "warning";
    try {
      const result = await snapshotUpdates?.requestMacRefresh({ reason, scope: "settings" })
        ?? { status: "unavailable", error_code: "transport_unavailable" };
      if (applicationDisposed) return result;
      if (result.status === "published") {
        if (result.cache_read_failed) {
          setSnapshotRefreshStatus(`Desktop snapshot published at ${formatTime(result.reported_at, "milliseconds")}; the displayed saved settings could not be reloaded.`, "warning");
          if (entry.notifyUser) showToast("The desktop published a snapshot, but settings could not reload it. The saved settings remain visible.");
        } else {
          mode = "success";
          setSnapshotRefreshStatus(`Desktop settings published at ${formatTime(result.reported_at, "milliseconds")}.`, "success");
          if (entry.notifyUser) showToast(`Settings refreshed from the desktop at ${formatTime(result.reported_at, "milliseconds")}.`);
        }
      } else {
        const message = refreshErrorMessage(result.error_code);
        setSnapshotRefreshStatus(message, "warning");
        if (entry.notifyUser) showToast(message);
      }
      return result;
    } catch (error) {
      if (applicationDisposed) return { status: "failed", error_code: "request_failed" };
      const message = refreshErrorMessage(error?.error_code ?? error?.code);
      setSnapshotRefreshStatus(message, "warning");
      if (entry.notifyUser) showToast(message);
      return { status: "failed", error_code: "request_failed" };
    } finally {
      snapshotRefreshRequests.delete(key);
      snapshotRefreshPending = snapshotRefreshRequests.size > 0;
      updateSnapshotUpdateStatus(snapshotUpdateStatus);
      setSnapshotRefreshStatus(snapshotRefreshMessage, mode);
    }
  })();
  snapshotRefreshRequests.set(key, entry);
  return entry.promise;
}

function start() {
  telegram?.ready?.();
  telegram?.expand?.();
  bindEvents();
  Object.assign(state, beginOwnerVerification());
  if (!initData) {
    byId("auth-gate").classList.remove("hidden");
    return;
  }
  byId("auth-message").textContent = "Verifying owner access to local media settings…";
  byId("auth-gate").classList.remove("hidden");
  setConnection("", "Verifying owner access");
  snapshotUpdates = createSnapshotUpdates({
    api,
    readSnapshot: performRefreshState,
    surface: "settings",
    onAuthorizationExpired: handleAuthorizationFailure,
    onReady: ({ databaseEpoch }) => {
      advertisedDatabaseEpoch = databaseEpoch;
      if (state.snapshot && state.snapshot.database_epoch !== databaseEpoch) {
        markEpochVerificationPending(databaseEpoch);
        void performRefreshState({ quiet: true, expectedDatabaseEpoch: databaseEpoch }).catch(() => {});
      }
    },
    onEpochChanged: ({ databaseEpoch }) => {
      advertisedDatabaseEpoch = databaseEpoch;
      markEpochVerificationPending(databaseEpoch);
    },
    onStatus: updateSnapshotUpdateStatus,
  });
  snapshotFreshnessTimer = window.setInterval(refreshLocalSnapshotFreshness, 60_000);
  window.addEventListener("pagehide", disposeApplication, { once: true });
  snapshotUpdates.start();
  const requestOpenSnapshot = () => {
    if (privateAccessExpired || applicationDisposed) return;
    void refreshState({ reason: "open", quiet: true });
  };
  void performRefreshState({ quiet: true }).then(requestOpenSnapshot).catch(() => {
    // A transient cache read failure does not cancel the one explicit refresh for this open.
    // The correlated desktop result retries the owner-checked read; 401 stops this path.
    requestOpenSnapshot();
  });
}

function disposeApplication(event) {
  if (event?.persisted === true) return;
  if (applicationDisposed) return;
  applicationDisposed = true;
  applicationGeneration += 1;
  snapshotUpdates?.stop();
  if (snapshotFreshnessTimer) window.clearInterval(snapshotFreshnessTimer);
  snapshotFreshnessTimer = 0;
}

start();
