import { ACTION_QUEUE_LIMIT, ACTION_RECEIPT_STORAGE_PREFIX, ACTION_SURFACE_SETTINGS, appendAction, consumeRejectedActionReceipts, createQueuedAction, dispatchNextOwnedAction, recordActionReceipt, recoverActionReceipts, removeQueuedAction, restorePendingActions, shouldRetryActionFailure } from "/file/action-queue.js";
import { HttpResponseError } from "/file/http-error.js";
import { acceptOwnerSnapshot, beginOwnerVerification, drainActionsAfterSnapshotRefresh, failOwnerSnapshot, mayDispatchActions, mayUsePrivateSnapshot } from "/file/snapshot-session.js";
import { escapeHtml } from "/file/preview-rendering.js";
import { validateTargetRelativeDirectory } from "/file/target-directory.js";
import { mediaLibraryAvailability, mediaLibraryEntries } from "/file/library-view.js";
import { buildLibraryMutationPayload, isValidLibraryRootBinding } from "./library-actions.js";
import { captureLibraryEditorFocus, libraryDraftFieldValue, reconcileLibraryDrafts, restoreLibraryEditorFocus, setLibraryDraftField } from "./library-drafts.js";
import { takeRejectedSettingsActions } from "./local-action-rejections.js";
import { timeAgoMilliseconds, timeAgoSeconds } from "./time-format.js";

const API = "/api";
const ACTION_QUEUE_KEY = "file-manager.actions.v1";
const POLL_MS = 12_000;
const MAX_ACTION_QUEUE = ACTION_QUEUE_LIMIT;
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
  refreshAfterDrain: false,
  quarantinedActionIds: new Set(),
  unknownActionNotices: new Map(),
  localActions: new Map(),
  libraryDrafts: new Map(),
  toastTimer: 0,
};

const byId = (id) => document.getElementById(id);
const formatTime = (value) => {
  const date = new Date(Number(value) * 1000);
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
  dot.className = `connection-dot${mode === "live" ? " live" : mode === "stale" ? " stale" : ""}`;
  byId("connection-label").textContent = label;
}

function showToast(message) {
  const toast = byId("toast");
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
  renderPending();
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
  if (!initData || !mayDispatchActions(state) || !navigator.onLine) return false;
  if (state.draining) return state.draining;
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
    while (navigator.onLine) {
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
        state.snapshot = null;
        Object.assign(state, failOwnerSnapshot(state, error.status));
        showPrivateAccessGate("Owner access could not be verified. Reopen this Mini App from the owner’s private chat.");
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
    if (sentAny) state.retryDelay = 1_500;
    if (rejected.length) showToast(`Request rejected before local execution: ${[...new Set(rejected)].slice(0, 2).join(" · ")}`);
    else if (!quiet && receiptFailed) showToast("The request reached the cloud inbox, but this browser could not save its receipt. It will retry with the same ID.");
    else if (!quiet && interrupted) showToast("Connection interrupted. Saved requests will retry with their same IDs.");
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

async function submitAction(kind, payload, label, onQueued = null) {
  if (!mayDispatchActions(state)) return false;
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
  onQueued?.();
  if (kind === "library_register") byId("register-form").dataset.pendingRequestId = action.request_id;
  showToast(navigator.onLine ? `${label} saved. Waiting for the desktop to apply it.` : `${label} saved in this browser. It has not reached the desktop.`);
  if (navigator.onLine) await drainActionQueue();
  else renderPending();
  return true;
}

function renderSync() {
  const snapshot = state.snapshot ?? {};
  const synced = snapshot.synced === true;
  byId("sync-badge").className = `badge${synced && !snapshot.stale ? " good" : synced ? " warning" : ""}`;
  byId("sync-badge").textContent = !synced ? "Not synced" : snapshot.stale ? "Contact overdue" : "Connected";
  byId("sync-title").textContent = !synced
    ? "Waiting for your computer"
    : snapshot.stale ? "Showing the last saved settings" : "Settings are synced";
  byId("sync-detail").textContent = !synced
    ? "Applied settings appear here after the desktop app connects."
    : `Last desktop contact ${timeAgoMilliseconds(snapshot.last_seen)}. New requests stay pending until local execution finishes.`;
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
  const snapshot = entry.snapshot && typeof entry.snapshot === "object" ? entry.snapshot : null;
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
      if (state.loading) state.refreshAfterDrain = true;
      else refreshState({ quiet: true });
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
    refreshState({ quiet: true });
  });
}

function applySnapshotFailure(status) {
  const transition = failOwnerSnapshot(state, status, Date.now());
  Object.assign(state, transition);
  if (transition.authorizationRejected) {
    state.refreshAfterDrain = false;
    if (state.retryTimer) window.clearTimeout(state.retryTimer);
    state.retryTimer = 0;
  }
  return transition;
}

async function refreshState({ quiet = false } = {}) {
  if (!initData || state.loading) return;
  recoverSettingsActionReceipts();
  state.loading = true;
  try {
    const fresh = await api("/state");
    Object.assign(state, acceptOwnerSnapshot(fresh, state));
    byId("auth-gate").classList.add("hidden");
    byId("app-content").classList.remove("hidden");
    setConnection(fresh.synced ? fresh.stale ? "stale" : "live" : "", fresh.synced ? fresh.stale ? "Contact is overdue" : "Computer connected" : "Waiting for computer");
    render();
    await drainActionsAfterSnapshotRefresh(state, navigator.onLine, () => drainActionQueue({ quiet }));
  } catch (error) {
    const transition = applySnapshotFailure(error?.status);
    if (mayUsePrivateSnapshot(state)) {
      setConnection("stale", "Showing last saved settings");
      render();
    } else {
      showPrivateAccessGate(transition.authorizationRejected
        ? "Owner access could not be verified. Reopen this Mini App from the owner’s private chat."
        : "Private settings stay hidden until the owner is verified. Check the connection and retry.");
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
  refreshState();
  window.setInterval(() => refreshState({ quiet: true }), POLL_MS);
}

start();
