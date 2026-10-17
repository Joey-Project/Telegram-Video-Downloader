import { appendAction, removeQueuedAction } from "./action-queue.js";
import { getOrCreateLegacySelection, updateLegacySelection } from "./legacy-mapping-state.js";
import { metadataPreviewDetails } from "./metadata-preview.js";
import { buildWaitingEntries } from "./state-view.js";

const API = "/api";
const CACHE_KEY = "file-manager.snapshot.v1";
const ACTION_QUEUE_KEY = "file-manager.actions.v1";
const MAX_LEGACY_BYTES = 256 * 1024;
const POLL_MS = 12_000;

const telegram = window.Telegram?.WebApp;
const initData = telegram?.initData ?? "";
const state = { snapshot: null, selected: new Set(), view: "library", query: "", detailId: null, legacyOperation: null, legacyOperationKey: null, legacySelection: new Map(), toastTimer: 0, loading: false, draining: null, retryDelay: 1500, retryTimer: 0, refreshAfterDrain: false };

const byId = (id) => document.getElementById(id);
const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/gu, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character]);
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
const statusOf = (item) => String(item?.presence ?? "unknown").toLowerCase();
const needsOrganizing = (item) => {
  const confidence = String(item?.confidence ?? "low").toLowerCase();
  const metadata = String(item?.metadata_status ?? "unknown").toLowerCase();
  const hasCollection = Boolean(item?.collection && (typeof item.collection === "string" ? item.collection.trim() : item.collection.title));
  return confidence !== "high" || metadata !== "complete" || !hasCollection || ["missing", "unavailable"].includes(statusOf(item));
};
const itemFolder = (item) => {
  const path = String(item?.relative_path ?? "").replaceAll("\\", "/");
  const slash = path.lastIndexOf("/");
  return slash < 0 ? "" : path.slice(0, slash);
};
const libraryItems = () => Array.isArray(state.snapshot?.library?.items) ? state.snapshot.library.items : [];
const snapshotValue = () => state.snapshot ?? { synced: false, stale: true, library: null, previews: [], operations: [], tasks: [], settings: {} };

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

function cachedSnapshot() {
  try {
    const parsed = JSON.parse(localStorage.getItem(CACHE_KEY) ?? "null");
    if (parsed && typeof parsed === "object") return parsed;
  } catch { /* Ignore an unreadable cache and wait for a fresh state. */ }
  return null;
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers ?? {});
  headers.set("X-Telegram-Init-Data", initData);
  if (options.body !== undefined) headers.set("Content-Type", "application/json");
  const response = await fetch(`${API}${path}`, { ...options, headers, cache: "no-store" });
  let data;
  try { data = await response.json(); } catch { data = {}; }
  if (!response.ok) throw new Error(typeof data?.error === "string" ? data.error : `Request failed (${response.status})`);
  return data;
}

async function refreshState({ quiet = false } = {}) {
  if (!initData || state.loading) return;
  state.loading = true;
  try {
    const fresh = await api("/state");
    state.snapshot = fresh;
    try { localStorage.setItem(CACHE_KEY, JSON.stringify(fresh)); } catch { /* Browsing remains available without cache. */ }
    setConnection(fresh.synced ? (fresh.stale ? "stale" : "live") : "", fresh.synced ? (fresh.stale ? "Contact is overdue" : "Computer connected") : "Waiting for computer");
    render();
    if (!quiet) await drainActionQueue();
  } catch (error) {
    const cached = state.snapshot ?? cachedSnapshot();
    if (cached) {
      state.snapshot = { ...cached, stale: true, cached_at: Date.now() };
      setConnection("stale", "Showing saved library");
      render();
    } else {
      setConnection("", "Waiting for connection");
    }
    if (!quiet && navigator.onLine) showToast(error.message);
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
  const count = readActionQueue().length;
  byId("queued-actions").textContent = count ? `${count} action${count === 1 ? "" : "s"} waiting to send` : "";
}

async function drainActionQueue() {
  if (!initData || !navigator.onLine) return false;
  if (state.draining) return state.draining;
  state.draining = (async () => {
    let sentAny = false;
    while (navigator.onLine) {
      const action = readActionQueue()[0];
      if (!action) break;
      try {
        await api("/actions", { method: "POST", body: JSON.stringify({ request_id: action.request_id, kind: action.kind, payload: action.payload }) });
        const latestQueue = readActionQueue();
        if (!writeActionQueue(removeQueuedAction(latestQueue, action.request_id))) {
          showToast("Request reached the computer, but this browser could not update its retry queue.");
          break;
        }
        sentAny = true;
      } catch {
        if (!state.retryTimer) {
          const delay = state.retryDelay;
          state.retryDelay = Math.min(state.retryDelay * 2, 30_000);
          state.retryTimer = window.setTimeout(() => {
            state.retryTimer = 0;
            drainActionQueue();
          }, delay);
        }
        showToast("Connection interrupted. The request remains queued with the same ID.");
        break;
      }
    }
    if (sentAny) state.retryDelay = 1500;
    return sentAny;
  })();
  let sentAny = false;
  try { sentAny = await state.draining; }
  finally { state.draining = null; }
  if (sentAny) {
    if (state.loading) state.refreshAfterDrain = true;
    else await refreshState({ quiet: true });
  }
  return sentAny;
}

async function submitAction(kind, payload) {
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
  if (!navigator.onLine) {
    showToast("Saved offline. It will send when this device reconnects.");
    return true;
  }
  showToast("Sending the saved request");
  await drainActionQueue();
  return true;
}

function allFolders() {
  const folders = new Set(Array.isArray(state.snapshot?.library?.categories) ? state.snapshot.library.categories : []);
  for (const item of libraryItems()) if (itemFolder(item)) folders.add(itemFolder(item));
  return [...folders].filter((folder) => typeof folder === "string").sort((a, b) => a.localeCompare(b));
}

function filteredItems() {
  const query = state.query.trim().toLocaleLowerCase();
  const category = byId("category-filter").value;
  const status = byId("status-filter").value;
  return libraryItems().filter((item) => {
    const text = [item.title, item.relative_path, item.id, ...(Array.isArray(item.source_ids) ? item.source_ids : [])].join(" ").toLocaleLowerCase();
    return (!query || text.includes(query)) && (!category || itemFolder(item) === category) && (!status || statusOf(item) === status);
  }).sort((left, right) => String(left.title ?? left.relative_path ?? "").localeCompare(String(right.title ?? right.relative_path ?? "")));
}

function renderFilters() {
  const select = byId("category-filter");
  const current = select.value;
  const folders = allFolders();
  select.innerHTML = `<option value="">All folders</option>${folders.map((folder) => `<option value="${escapeHtml(folder)}">${escapeHtml(folder || "Library root")}</option>`).join("")}`;
  if (folders.includes(current)) select.value = current;
  byId("known-folders").innerHTML = folders.map((folder) => `<option value="${escapeHtml(folder)}"></option>`).join("");
}

function itemRow(item) {
  const id = String(item.id ?? "");
  const selected = state.selected.has(id);
  const presence = statusOf(item);
  const displayStatus = presence === "present" ? "Available" : presence === "missing" ? "Missing" : presence === "unavailable" ? "Unavailable" : "Status unknown";
  const dims = item.width && item.height ? `${escapeHtml(item.width)} × ${escapeHtml(item.height)}` : "";
  const codec = item.codec ? escapeHtml(item.codec) : "";
  return `<article class="library-row${selected ? " selected" : ""}" data-item-row="${escapeHtml(id)}">
    <input class="row-check" type="checkbox" aria-label="Select ${escapeHtml(item.title ?? item.relative_path)}" data-select-item="${escapeHtml(id)}" ${selected ? "checked" : ""}>
    <div class="media-icon" aria-hidden="true">▣</div>
    <div class="media-main"><div class="media-title"><span>${escapeHtml(item.title || item.relative_path || "Untitled video")}</span></div><div class="media-path">${escapeHtml(item.relative_path || "Path unavailable")}</div></div>
    <div class="media-meta"><span><i class="status-dot ${escapeHtml(presence)}"></i>${displayStatus}</span><span>${formatBytes(item.bytes)}</span>${dims ? `<span>${dims}</span>` : ""}${codec ? `<span>${codec}</span>` : ""}<button class="details-button" type="button" data-details="${escapeHtml(id)}">Details</button></div>
  </article>`;
}

function renderGroupedItems(container, items) {
  const groups = new Map();
  for (const item of items) {
    const collection = typeof item.collection === "string" ? item.collection : item.collection?.title;
    const key = collection || "Uncollected";
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(item);
  }
  container.innerHTML = [...groups.entries()].map(([title, entries], index) => {
    const groupIds = entries.map((item) => String(item.id));
    const allSelected = groupIds.length > 0 && groupIds.every((id) => state.selected.has(id));
    const someSelected = groupIds.some((id) => state.selected.has(id));
    return `<details class="collection-group" ${index < 3 ? "open" : ""}>
      <summary><input class="group-check" type="checkbox" aria-label="Select all ${escapeHtml(title)} items" data-select-group="${escapeHtml(title)}" ${allSelected ? "checked" : ""} ${someSelected && !allSelected ? "data-partial=true" : ""}><span>${escapeHtml(title)}</span><span class="group-count">${entries.length} file${entries.length === 1 ? "" : "s"}</span></summary>
      <div class="collection-items">${entries.map(itemRow).join("")}</div>
    </details>`;
  }).join("");
  container.querySelectorAll("[data-partial]").forEach((checkbox) => { checkbox.indeterminate = true; });
}

function renderLibrary() {
  renderFilters();
  const items = filteredItems();
  const list = byId("library-list");
  renderGroupedItems(list, items);
  const needsItems = items.filter(needsOrganizing);
  renderGroupedItems(byId("needs-list"), needsItems);
  const synced = state.snapshot?.synced === true;
  const firstScanPending = synced && (state.snapshot.library?.ready === false || Number(state.snapshot.library?.scanned_at) === 0);
  byId("library-empty").classList.toggle("hidden", !synced || libraryItems().length > 0 && !firstScanPending);
  list.classList.toggle("hidden", !synced || firstScanPending || libraryItems().length === 0);
  const emptyHeading = byId("library-empty").querySelector("h2");
  const emptyCopy = byId("library-empty").querySelector("p");
  if (firstScanPending) {
    emptyHeading.textContent = "Waiting for the first scan";
    emptyCopy.textContent = "Your computer is connected. The library will appear when its first scan completes.";
  } else {
    emptyHeading.textContent = "No media found yet";
    emptyCopy.textContent = "Run a scan on the connected computer to discover videos and their sidecar files.";
  }
  if (!synced) {
    list.innerHTML = "";
    byId("library-empty").classList.add("hidden");
  }
  const visible = items.length;
  const total = libraryItems().length;
  byId("library-count").textContent = synced && !firstScanPending ? String(total) : "—";
  byId("library-summary").textContent = firstScanPending
    ? "Your computer is connected, but the first scan has not completed."
    : state.snapshot?.synced
    ? `${visible.toLocaleString()} shown of ${total.toLocaleString()} item${total === 1 ? "" : "s"}${state.selected.size ? ` · ${state.selected.size} selected` : ""}`
    : "A first scan has not completed yet.";
  byId("needs-count").textContent = synced && !firstScanPending ? String(libraryItems().filter(needsOrganizing).length) : "—";
  byId("needs-summary").textContent = firstScanPending
    ? "Needs organizing will appear after the first scan completes."
    : state.snapshot?.synced
    ? `${needsItems.length.toLocaleString()} item${needsItems.length === 1 ? "" : "s"} shown for review · ${libraryItems().filter(needsOrganizing).length.toLocaleString()} need attention in total`
    : "Needs organizing will appear after the first scan.";
  byId("needs-empty").classList.toggle("hidden", !synced || firstScanPending || needsItems.length > 0);
  byId("needs-list").classList.toggle("hidden", !synced || firstScanPending || needsItems.length === 0);
  byId("preview-move").disabled = state.selected.size === 0;
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
  return (Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : []).filter((operation) => operation?.kind === "legacy_import");
}

function renderWaiting() {
  const tasks = Array.isArray(state.snapshot?.tasks) ? state.snapshot.tasks : [];
  const operations = Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [];
  const cloudQueue = Array.isArray(state.snapshot?.pending_requests) ? state.snapshot.pending_requests : [];
  const { cloud: pendingRequests, local } = buildWaitingEntries(cloudQueue, tasks, operations);
  const isPending = (entry) => !["completed", "failed", "cancelled", "confirmed"].includes(String(entry.status).toLowerCase());
  const pendingCloud = pendingRequests.filter(isPending);
  const pending = local.filter(isPending);
  const localQueue = readActionQueue();
  byId("waiting-count").textContent = String(pendingCloud.length + pending.length + localQueue.length);
  const cards = [];
  for (const request of pendingCloud) cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(operationTitle(request))}</h3><p>Saved in the cloud inbox. Your desktop app has not acknowledged local receipt yet.</p></div><span class="state-pill pending">Waiting for computer · #${escapeHtml(request.seq)}</span></div><p>Received ${escapeHtml(timeAgo(request.created_at))}</p></article>`);
  for (const action of localQueue) cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(action.kind.replaceAll("_", " "))}</h3><p>Saved in this browser. It will be sent when this device is connected.</p></div><span class="state-pill pending">Offline queue</span></div></article>`);
  for (const entry of pending) cards.push(`<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(operationTitle(entry))}</h3><p>${escapeHtml(entry.message ?? entry.error ?? "Your computer has received this request and is working on it.")}</p></div><span class="state-pill pending">${escapeHtml(statusText(entry.status))}</span></div>${entry.updated_at ? `<p>Updated ${escapeHtml(timeAgo(entry.updated_at, "seconds"))}</p>` : ""}</article>`);
  byId("waiting-list").innerHTML = cards.length ? cards.join("") : `<div class="empty-state"><div class="empty-art" aria-hidden="true">✓</div><h2>Nothing is waiting</h2><p>New requests and scans will appear here while your computer processes them.</p></div>`;
}

function operationPreview(operation) {
  if (operation?.preview && typeof operation.preview === "object") return { id: operation.preview_id ?? operation.preview.id, revision: operation.revision ?? operation.preview.revision, preview: operation.preview, status: operation.status };
  if (operation?.result?.preview && typeof operation.result.preview === "object") return { id: operation.preview_id ?? operation.result.preview_id ?? operation.result.preview.id, revision: operation.revision ?? operation.result.revision ?? operation.result.preview.revision, preview: operation.result.preview, status: operation.status };
  return null;
}

function previews() {
  const collected = [];
  for (const preview of Array.isArray(state.snapshot?.previews) ? state.snapshot.previews : []) collected.push({ id: preview.preview_id ?? preview.id, revision: preview.revision ?? preview.preview?.revision, preview: preview.preview ?? preview, status: preview.status });
  for (const operation of Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : []) {
    const found = operationPreview(operation);
    if (found) collected.push(found);
  }
  const unique = new Map();
  for (const value of collected) {
    if (value.id && value.revision) unique.set(`${value.id}:${value.revision}`, value);
  }
  return [...unique.values()].reverse();
}

function describePreview(preview) {
  const moveItems = Array.isArray(preview.items) ? preview.items : [];
  const patchItems = Array.isArray(preview.metadata_patches) ? preview.metadata_patches : [];
  const lines = [];
  for (const item of moveItems) {
    const outcome = String(item.outcome ?? "move");
    const label = outcome === "skip" ? "Skipped" : outcome === "no_op" ? "No change" : "Move";
    lines.push(`<div class="preview-line"><span>${escapeHtml(item.source_path ?? item.item_id ?? "Selected file")} → ${escapeHtml(item.target_path ?? "Library root")}${item.files?.length ? ` <span class="field-hint">(${item.files.length} files including attachments)</span>` : ""}${item.reason ? `<br><span class="preview-reason">${escapeHtml(item.reason)}</span>` : ""}</span><strong class="${outcome === "skip" ? "skip" : "move"}">${label}</strong></div>`);
  }
  for (const patch of patchItems) {
    const details = metadataPreviewDetails(patch).map((detail) => detail.kind === "status"
      ? escapeHtml(detail.text)
      : `${escapeHtml(detail.label)}: ${escapeHtml(detail.before ?? "Not set")} → ${escapeHtml(detail.after)}`);
    const changes = details.join("<br>");
    lines.push(`<div class="preview-line"><span>${escapeHtml(patch.nfo_path ?? patch.item_id ?? "Metadata file")}<br><span class="field-hint">${changes}</span></span><strong class="move">Metadata</strong></div>`);
  }
  return lines.length ? lines.join("") : `<div class="preview-line"><span>This preview contains no file or metadata changes.</span></div>`;
}

function renderPreviews() {
  const pending = previews().filter((entry) => !["confirmed", "completed", "cancelled", "expired"].includes(String(entry.status ?? "").toLowerCase()));
  byId("organizing-count").textContent = String(pending.length);
  byId("preview-list").innerHTML = pending.map((entry) => {
    const preview = entry.preview;
    const title = Array.isArray(preview.metadata_patches) && preview.items?.length ? "Move and update metadata" : Array.isArray(preview.metadata_patches) ? "Update metadata" : "Move files";
    return `<article class="stack-card"><div class="stack-card-head"><div><h3>${title}</h3><p>Destination: ${escapeHtml(preview.target_relative_dir ?? "Metadata only · file locations stay the same")} · Conflict: ${escapeHtml(preview.conflict ?? "skip")}</p></div><span class="state-pill pending">Review needed</span></div><div class="preview-table">${describePreview(preview)}</div><div class="stack-actions"><button class="small-button confirm" type="button" data-confirm="${escapeHtml(entry.id)}" data-revision="${escapeHtml(entry.revision)}">Confirm changes</button></div></article>`;
  }).join("");
  if (!pending.length) byId("preview-list").innerHTML = "";
}

function renderLegacyMappings() {
  const operations = legacyOperations();
  const operation = [...operations].reverse().find((entry) => Array.isArray(entry.result?.hints) && Array.isArray(entry.result?.candidates));
  state.legacyOperation = operation ?? null;
  const container = byId("legacy-mappings");
  if (!operation) {
    container.classList.add("hidden");
    return;
  }
  const operationKey = String(operation.seq ?? operation.created_at ?? "legacy-import");
  if (state.legacyOperationKey !== operationKey) {
    state.legacyOperationKey = operationKey;
    state.legacySelection = new Map();
  }
  const hints = operation.result.hints;
  const candidates = operation.result.candidates;
  const warnings = Array.isArray(operation.result.warnings) ? operation.result.warnings : [];
  const matchedHintIndexes = new Set(candidates.map((candidate) => Number(candidate.hint_index)));
  const manualHints = hints.map((hint, hintIndex) => ({ hint, hintIndex })).filter(({ hintIndex }) => !matchedHintIndexes.has(hintIndex));
  const manualItems = libraryItems().filter((item) => statusOf(item) === "present");
  container.classList.remove("hidden");
  const candidateKeys = new Set();
  const candidateRows = candidates.map((candidate, index) => {
    const hintIndex = Number(candidate.hint_index);
    const hint = hints[hintIndex] ?? {};
    const candidateKey = `${operationKey}:${hintIndex}:${candidate.item_id}:${index}`;
    candidateKeys.add(candidateKey);
    const sourceIds = Array.isArray(hint.source_ids) ? hint.source_ids : [];
    const collection = hint.collection && typeof hint.collection === "object" ? hint.collection.title : "";
    const selection = getOrCreateLegacySelection(state.legacySelection, candidateKey, { selected: false, title: Boolean(hint.title), source_ids: sourceIds.length > 0, collection: Boolean(collection) });
    return `<div class="mapping-row"><div class="mapping-row-head"><input type="checkbox" aria-label="Apply legacy hint to ${escapeHtml(candidate.title ?? candidate.relative_path)}" data-map-select="${escapeHtml(candidateKey)}" data-hint-index="${hintIndex}" data-item-id="${escapeHtml(candidate.item_id)}" ${selection.selected ? "checked" : ""}><div><div class="mapping-name">${escapeHtml(hint.title || candidate.title || "Legacy source links")} → ${escapeHtml(candidate.title || candidate.relative_path || candidate.item_id)}</div><div class="mapping-path">${escapeHtml(candidate.relative_path ?? "Path unavailable")} · ${escapeHtml(candidate.match_kind ?? "candidate match")}${candidate.ambiguous ? " · check this match" : ""}</div></div></div><div class="mapping-fields"><label><input type="checkbox" data-map-field="title" data-map-key="${escapeHtml(candidateKey)}" ${selection.title ? "checked" : ""} ${hint.title ? "" : "disabled"}> Title</label><label><input type="checkbox" data-map-field="source_ids" data-map-key="${escapeHtml(candidateKey)}" ${selection.source_ids ? "checked" : ""} ${sourceIds.length ? "" : "disabled"}> ${sourceIds.length} source link${sourceIds.length === 1 ? "" : "s"}</label><label><input type="checkbox" data-map-field="collection" data-map-key="${escapeHtml(candidateKey)}" ${selection.collection ? "checked" : ""} ${collection ? "" : "disabled"}> Collection</label></div>${candidate.requires_confirmation || hint.requires_confirmation ? `<p class="mapping-help">This match needs your confirmation. Review the item path and proposed fields before continuing.</p>` : ""}</div>`;
  });
  const manualRows = manualHints.map(({ hint, hintIndex }) => {
    const manualKey = `${operationKey}:manual:${hintIndex}`;
    candidateKeys.add(manualKey);
    const sourceIds = Array.isArray(hint.source_ids) ? hint.source_ids : [];
    const collection = hint.collection && typeof hint.collection === "object" ? hint.collection.title : "";
    const selection = getOrCreateLegacySelection(state.legacySelection, manualKey, { selected: false, item_id: "", title: Boolean(hint.title), source_ids: sourceIds.length > 0, collection: Boolean(collection) });
    const options = manualItems.map((item) => `<option value="${escapeHtml(item.id)}" ${String(item.id) === selection.item_id ? "selected" : ""}>${escapeHtml(item.title || item.relative_path || item.id)} · ${escapeHtml(item.relative_path || "Path unavailable")}</option>`).join("");
    return `<div class="mapping-row manual-mapping"><div class="mapping-row-head"><input type="checkbox" aria-label="Apply this legacy hint after manually choosing a video" data-map-select="${escapeHtml(manualKey)}" data-manual="true" data-hint-index="${hintIndex}" ${selection.selected ? "checked" : ""}><div><div class="mapping-name">${escapeHtml(hint.title || "Legacy hint without a reliable match")}</div><div class="mapping-path">No reliable automatic match · choose a present video manually</div></div></div><label class="field-label manual-target-label" for="manual-target-${hintIndex}">Match this hint to a video</label><select id="manual-target-${hintIndex}" class="select-control manual-target" data-manual-target="${escapeHtml(manualKey)}"><option value="">Choose a video…</option>${options}</select><div class="mapping-fields"><label><input type="checkbox" data-map-field="title" data-map-key="${escapeHtml(manualKey)}" ${selection.title ? "checked" : ""} ${hint.title ? "" : "disabled"}> Title</label><label><input type="checkbox" data-map-field="source_ids" data-map-key="${escapeHtml(manualKey)}" ${selection.source_ids ? "checked" : ""} ${sourceIds.length ? "" : "disabled"}> ${sourceIds.length} source link${sourceIds.length === 1 ? "" : "s"}</label><label><input type="checkbox" data-map-field="collection" data-map-key="${escapeHtml(manualKey)}" ${selection.collection ? "checked" : ""} ${collection ? "" : "disabled"}> Collection</label></div><p class="mapping-help">This is a manual association. Review the selected path and the generated metadata preview before confirming.</p></div>`;
  });
  container.innerHTML = `<p class="mapping-title">Choose a video for each legacy hint</p>${warnings.length ? `<p class="warning-text">${warnings.map(escapeHtml).join(" · ")}</p>` : ""}${candidateRows.length ? candidateRows.join("") : ""}${manualRows.length ? `<p class="mapping-title manual-heading">No reliable match</p>${manualRows.join("")}` : candidates.length ? "" : `<p class="field-hint">No hints or possible media matches were found.</p>`}<div class="mapping-actions"><button id="preview-metadata" class="primary-button" type="button" ${candidateRows.length || manualRows.length ? "" : "disabled"}>Preview selected updates <span aria-hidden="true">→</span></button></div>`;
  for (const key of state.legacySelection.keys()) if (!candidateKeys.has(key)) state.legacySelection.delete(key);
}

function renderOperations() {
  const operations = Array.isArray(state.snapshot?.operations) ? state.snapshot.operations : [];
  const visible = operations.filter((entry) => entry?.kind !== "file_preview");
  byId("operation-list").innerHTML = visible.slice(-12).reverse().map((entry) => {
    const result = entry.result && typeof entry.result === "object" ? entry.result : {};
    const outcome = result.operation && typeof result.operation === "object" ? result.operation : result;
    const items = Array.isArray(outcome.items) ? outcome.items : Array.isArray(result.items) ? result.items : [];
    const hintCount = Array.isArray(result.hints) ? result.hints.length : 0;
    const candidateCount = Array.isArray(result.candidates) ? result.candidates.length : 0;
    const movedFiles = items.reduce((total, item) => total + (Number.isSafeInteger(item.moved_files) ? item.moved_files : 0), 0);
    const totalFiles = items.reduce((total, item) => total + (Number.isSafeInteger(item.total_files) ? item.total_files : 0), 0);
    const metadataUpdated = items.filter((item) => item.metadata_patched === true).length;
    const summary = items.length
      ? `${items.length} item${items.length === 1 ? "" : "s"} · ${movedFiles}/${totalFiles} related file${totalFiles === 1 ? "" : "s"} moved · ${metadataUpdated} metadata update${metadataUpdated === 1 ? "" : "s"}`
      : entry.kind === "legacy_import"
        ? `${hintCount} hints read · ${candidateCount} possible matches. Choose the matches above to prepare updates.`
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
    return `<article class="stack-card"><div class="stack-card-head"><div><h3>${escapeHtml(operationTitle(entry))}</h3><p>${escapeHtml(entry.error ?? summary)}</p></div><span class="state-pill">${escapeHtml(statusText(entry.status))}</span></div>${entry.created_at ? `<p>Received ${escapeHtml(timeAgo(entry.created_at))}</p>` : ""}${itemResults ? `<div class="preview-table">${itemResults}</div>` : ""}${warningDetails.length ? `<div class="operation-warnings">${warningDetails.map((warning) => `<p>${escapeHtml(warning)}</p>`).join("")}</div>` : ""}${entry.kind === "legacy_import" && result.revision ? `<p>Library revision: ${escapeHtml(result.revision)}</p>` : ""}</article>`;
  }).join("");
}

function renderSync() {
  const snapshot = snapshotValue();
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
  const firstScanPending = snapshot.library?.ready === false || Number(snapshot.library?.scanned_at) === 0;
  title.textContent = snapshot.stale ? "No recent computer contact" : firstScanPending ? "Connected · first scan pending" : "Your library is synced";
  detail.textContent = `Last contact ${timeAgo(snapshot.last_seen)} · Last scan ${firstScanPending ? "not completed" : formatTime(snapshot.library?.scanned_at, "seconds")}`;
  badge.textContent = snapshot.stale ? "Contact overdue" : "Up to date";
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
  const root = String(state.snapshot?.settings?.download_dir ?? "").replace(/[\\/]+$/u, "");
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
      <section class="detail-section"><h3>Source links (${sourceIds.length})</h3>${sourceIds.length ? sourceIds.map((source) => `<div class="attachment">${escapeHtml(source)}</div>`).join("") : `<div class="attachment">No source links recorded.</div>`}<p class="field-hint">Remote source availability has not been checked.</p></section>
      <section class="detail-section"><h3>Related files (${attachments.length})</h3>${attachments.length ? attachments.map((attachment) => { const rel = attachment.relative_path ?? attachment.path ?? "Related file"; const absolute = pathJoin(rel); return `<div class="attachment"><div class="copy-row"><code>${escapeHtml(rel)}</code><button class="small-button" type="button" data-copy="${escapeHtml(rel)}">Copy relative</button></div>${absolute ? `<div class="copy-row"><code>${escapeHtml(absolute)}</code><button class="small-button" type="button" data-copy="${escapeHtml(absolute)}">Copy full</button></div>` : ""}<span>${escapeHtml(attachment.kind ?? "attachment")}</span></div>`; }).join("") : `<div class="attachment">No attachments found.</div>`}</section>
    </aside>`;
  document.body.append(panel);
  panel.addEventListener("click", (event) => { if (event.target === panel || event.target.closest(".detail-close")) { state.detailId = null; renderDetail(); } });
}

function render() {
  renderSync();
  renderLibrary();
  renderWaiting();
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
  const hints = operation.result.hints;
  const selections = [...document.querySelectorAll("[data-map-select]:checked")];
  if (!selections.length) return showToast("Select at least one explicit hint-to-video match.");
  const patches = [];
  const usedItems = new Set();
  for (const selected of selections) {
    const hintIndex = Number(selected.dataset.hintIndex);
    const key = selected.dataset.mapSelect;
    const selection = state.legacySelection.get(key);
    const itemId = selected.dataset.manual === "true" ? selection?.item_id : selected.dataset.itemId;
    const hint = hints[hintIndex];
    if (selected.dataset.manual === "true" && !itemId) return showToast("Choose a present video for every manual match.");
    if (!hint || !itemId) continue;
    if (usedItems.has(itemId)) return showToast("Choose each video once; combine its changes in one mapping.");
    usedItems.add(itemId);
    if (!selection) continue;
    const patch = { item_id: itemId };
    if (selected.dataset.manual !== "true") patch.hint_index = hintIndex;
    if (selection.title && hint.title) patch.title = hint.title;
    if (selection.source_ids && Array.isArray(hint.source_ids) && hint.source_ids.length) patch.source_ids = hint.source_ids;
    if (selection.collection && hint.collection) patch.collection = hint.collection;
    if (Object.keys(patch).length > 2) patches.push(patch);
  }
  if (!patches.length) return showToast("Choose at least one metadata field to apply.");
  submitAction("file_preview", { metadata_patches: patches, rename: false, conflict: "skip" });
}

function selectVisible() {
  const filtered = filteredItems();
  const visible = state.view === "needs" ? filtered.filter(needsOrganizing) : filtered;
  const allSelected = visible.length > 0 && visible.every((item) => state.selected.has(String(item.id)));
  for (const item of visible) {
    const id = String(item.id);
    if (allSelected) state.selected.delete(id); else state.selected.add(id);
  }
  renderLibrary();
}

function bindEvents() {
  byId("refresh-button").addEventListener("click", () => refreshState());
  byId("scan-button").addEventListener("click", () => submitAction("library_scan", {}));
  document.querySelectorAll("[data-scan]").forEach((button) => button.addEventListener("click", () => submitAction("library_scan", {})));
  byId("search-input").addEventListener("input", (event) => { state.query = event.target.value; renderLibrary(); });
  byId("category-filter").addEventListener("change", renderLibrary);
  byId("status-filter").addEventListener("change", renderLibrary);
  byId("select-visible").addEventListener("click", selectVisible);
  byId("root-target").addEventListener("click", () => { byId("target-dir").value = ""; byId("target-dir").focus(); });
  byId("preview-move").addEventListener("click", () => {
    const target = byId("target-dir").value.trim().replaceAll("\\", "/");
    if (target.startsWith("/") || target.split("/").some((part) => part === ".." || part === "." || part === "" && target !== "")) return showToast("Use a safe relative folder path without empty segments.");
    submitAction("file_preview", { item_ids: [...state.selected], target_relative_dir: target, rename: byId("rename-toggle").checked, conflict: byId("conflict-select").value });
  });
  byId("legacy-text").addEventListener("input", (event) => { byId("legacy-size").textContent = `${new TextEncoder().encode(event.target.value).byteLength.toLocaleString()} / 256 KB`; });
  byId("import-legacy").addEventListener("click", async () => {
    let text = byId("legacy-text").value;
    const fileInput = byId("legacy-file");
    if (!text && fileInput?.files?.[0]) text = await fileInput.files[0].text();
    const bytes = new TextEncoder().encode(text).byteLength;
    if (bytes === 0) return showToast("Paste legacy text or choose a text file first.");
    if (bytes > MAX_LEGACY_BYTES) return showToast("Legacy text exceeds the 256 KB limit.");
    submitAction("legacy_import", { text });
  });
  byId("legacy-text").addEventListener("paste", async (event) => {
    if (byId("legacy-text").value) return;
    const clipboard = event.clipboardData?.getData("text/plain");
    if (clipboard && new TextEncoder().encode(clipboard).byteLength <= MAX_LEGACY_BYTES) {
      window.setTimeout(() => { byId("legacy-size").textContent = `${new TextEncoder().encode(byId("legacy-text").value).byteLength.toLocaleString()} / 256 KB`; }, 0);
    }
  });
  document.body.addEventListener("change", (event) => {
    const mapCheckbox = event.target.closest("[data-map-select]");
    const mapField = event.target.closest("[data-map-field]");
    const manualTarget = event.target.closest("[data-manual-target]");
    if (mapCheckbox || mapField || manualTarget) {
      const key = mapCheckbox?.dataset.mapSelect ?? mapField?.dataset.mapKey ?? manualTarget?.dataset.manualTarget;
      if (mapCheckbox) updateLegacySelection(state.legacySelection, key, { selected: mapCheckbox.checked });
      if (mapField) updateLegacySelection(state.legacySelection, key, { [mapField.dataset.mapField]: mapField.checked });
      if (manualTarget) updateLegacySelection(state.legacySelection, key, { item_id: manualTarget.value });
      return;
    }
    const groupCheckbox = event.target.closest("[data-select-group]");
    if (groupCheckbox) {
      const groupTitle = groupCheckbox.dataset.selectGroup;
      const filtered = filteredItems();
      const visibleItems = state.view === "needs" ? filtered.filter(needsOrganizing) : filtered;
      const groupItems = visibleItems.filter((item) => (item.collection?.title ?? item.collection ?? "Uncollected") === groupTitle);
      for (const item of groupItems) {
        const id = String(item.id);
        if (groupCheckbox.checked) state.selected.add(id); else state.selected.delete(id);
      }
      renderLibrary();
      return;
    }
    const checkbox = event.target.closest("[data-select-item]");
    if (!checkbox) return;
    if (checkbox.checked) state.selected.add(checkbox.dataset.selectItem); else state.selected.delete(checkbox.dataset.selectItem);
    const row = checkbox.closest(".library-row");
    row?.classList.toggle("selected", checkbox.checked);
    byId("library-summary").textContent = `${filteredItems().length.toLocaleString()} shown of ${libraryItems().length.toLocaleString()} item${libraryItems().length === 1 ? "" : "s"}${state.selected.size ? ` · ${state.selected.size} selected` : ""}`;
    byId("preview-move").disabled = state.selected.size === 0;
  });
  document.body.addEventListener("click", (event) => {
    const groupCheckbox = event.target.closest("[data-select-group]");
    if (groupCheckbox) { event.stopPropagation(); return; }
    const tab = event.target.closest("[data-view]");
    if (tab) { state.view = tab.dataset.view; render(); return; }
    const details = event.target.closest("[data-details]");
    if (details) { state.detailId = details.dataset.details; renderDetail(); return; }
    const confirm = event.target.closest("[data-confirm]");
    if (confirm) {
      confirm.disabled = true;
      submitAction("file_confirm", { preview_id: confirm.dataset.confirm, revision: confirm.dataset.revision });
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
  if (!initData) {
    byId("auth-gate").classList.remove("hidden");
    return;
  }
  byId("app-content").classList.remove("hidden");
  const cached = cachedSnapshot();
  if (cached) { state.snapshot = { ...cached, stale: true, cached_at: Date.now() }; render(); setConnection("stale", "Showing saved library"); }
  else render();
  refreshState();
  window.setInterval(() => refreshState({ quiet: true }), POLL_MS);
}

start();
