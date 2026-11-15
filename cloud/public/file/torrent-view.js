export const TORRENT_PAGE_SIZE = 50;
export const TORRENT_DRAFT_STORAGE_KEY = "file-manager.torrent-selection.v1";
const MAX_DRAFT_TASKS = 32;
const MAX_SELECTION_INDEX = 4_999;
const FILTERS = new Set(["music", "all", "ready", "waiting", "imported"]);
const SAFE_ID = /^[\x21-\x7e]{1,128}$/u;

const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/gu, (character) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
}[character]));

function boundedText(value, maxBytes = 512) {
  const text = String(value ?? "");
  const encoder = new TextEncoder();
  if (encoder.encode(text).byteLength <= maxBytes) return text;
  let output = "";
  for (const character of text) {
    if (encoder.encode(output + character).byteLength > maxBytes - 3) break;
    output += character;
  }
  return `${output}…`;
}

function validIndex(value) {
  return Number.isSafeInteger(value) && value >= 0 && value <= MAX_SELECTION_INDEX;
}

function normalizeQuery(value) {
  const query = String(value ?? "");
  if (/[\u0000-\u001f\u007f-\u009f]/u.test(query)) return null;
  return boundedText(query, 256);
}

function normalizeIndexes(value) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.filter(validIndex))].slice(0, MAX_SELECTION_INDEX + 1).sort((a, b) => a - b);
}

function readDrafts(storage) {
  if (!storage) return new Map();
  try {
    const value = JSON.parse(storage.getItem(TORRENT_DRAFT_STORAGE_KEY) ?? "{}");
    if (!value || typeof value !== "object" || Array.isArray(value)) return new Map();
    return new Map(Object.entries(value).slice(-MAX_DRAFT_TASKS).flatMap(([taskId, draft]) => {
      if (!SAFE_ID.test(taskId) || !draft || typeof draft !== "object"
          || !Number.isSafeInteger(draft.inventoryRevision) || draft.inventoryRevision <= 0
          || !Number.isSafeInteger(draft.selectionRevision) || draft.selectionRevision <= 0) return [];
      return [[taskId, {
        inventoryRevision: draft.inventoryRevision,
        selectionRevision: draft.selectionRevision,
        indexes: new Set(normalizeIndexes(draft.indexes)),
      }]];
    }));
  } catch { return new Map(); }
}

function writeDrafts(storage, drafts) {
  if (!storage) return false;
  try {
    const value = Object.fromEntries([...drafts].slice(-MAX_DRAFT_TASKS).map(([taskId, draft]) => [taskId, {
      inventoryRevision: draft.inventoryRevision,
      selectionRevision: draft.selectionRevision,
      indexes: [...draft.indexes].filter(validIndex).slice(0, MAX_SELECTION_INDEX + 1),
    }]));
    storage.setItem(TORRENT_DRAFT_STORAGE_KEY, JSON.stringify(value));
    return true;
  } catch { return false; }
}

function operationForRequest(snapshot, requestId, receiptSeq = null) {
  if (!requestId) return null;
  return (Array.isArray(snapshot?.operations) ? snapshot.operations : []).find((entry) =>
    entry?.kind === "torrent_action" && (
      Object.prototype.hasOwnProperty.call(entry, "request_id")
        ? entry.request_id === requestId || entry.request_id === `action:${requestId}`
        : Number.isSafeInteger(receiptSeq) && entry?.seq === receiptSeq
    )
  ) ?? null;
}

function requestSequenceCollision(snapshot, requestId, receiptSeq) {
  if (!requestId || !Number.isSafeInteger(receiptSeq)) return false;
  return (Array.isArray(snapshot?.operations) ? snapshot.operations : []).some((entry) =>
    entry?.kind === "torrent_action" && entry?.seq === receiptSeq
      && Object.prototype.hasOwnProperty.call(entry, "request_id")
      && entry.request_id !== requestId && entry.request_id !== `action:${requestId}`,
  );
}

function operationPending(snapshot, requestId) {
  if (!requestId) return false;
  return (Array.isArray(snapshot?.pending_requests) ? snapshot.pending_requests : []).some((entry) =>
    entry?.kind === "torrent_action" && (entry?.request_id === requestId || entry?.request_id === `action:${requestId}`),
  );
}

function receiptAgedOut(snapshot, requestId, receiptSeq) {
  const operations = Array.isArray(snapshot?.operations) ? snapshot.operations : [];
  if (!Number.isSafeInteger(receiptSeq) || !operations.length || operationPending(snapshot, requestId)) return false;
  const firstSeq = Number(operations[0]?.seq);
  return Number.isSafeInteger(firstSeq) && firstSeq > receiptSeq;
}

function formatBytes(value) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes < 0) return "Size unavailable";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let size = bytes;
  let unit = -1;
  do { size /= 1024; unit += 1; } while (size >= 1024 && unit < units.length - 1);
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unit]}`;
}

function formatProgress(value) {
  const progress = Number(value);
  if (!Number.isFinite(progress)) return "Progress unavailable";
  const percent = progress >= 0 && progress <= 1 ? progress * 100 : progress;
  return `${Math.max(0, Math.min(100, percent)).toFixed(1)}%`;
}

function prettyState(value) {
  return String(value ?? "unknown").replaceAll("_", " ").replace(/([a-z])([A-Z])/gu, "$1 $2");
}

const TORRENT_STATE_LABELS = new Map([
  ["downloading", "Downloading"],
  ["stalleddl", "Waiting for peers"],
  ["queueddl", "Queued to download"],
  ["uploading", "Seeding"],
  ["stalledup", "Seeding · waiting for peers"],
  ["queuedup", "Queued to seed"],
  ["pauseddl", "Paused"],
  ["pausedup", "Paused"],
  ["checkingdl", "Checking files"],
  ["checkingup", "Checking files"],
  ["checkingresumedata", "Checking files"],
  ["error", "Needs attention"],
  ["missingfiles", "Files missing"],
  ["moving", "Moving files"],
  ["allocating", "Preparing files"],
]);

function humanTorrentState(value) {
  const raw = String(value ?? "");
  const label = TORRENT_STATE_LABELS.get(raw.toLowerCase()) ?? prettyState(raw);
  return label.replace(/^\p{L}/u, (letter) => letter.toLocaleUpperCase());
}

function taskStatusLabel(task) {
  const stateLabel = task?.state ? humanTorrentState(task.state) : "";
  const phaseLabel = task?.phase ? humanTorrentState(task.phase) : "";
  if (!stateLabel) return phaseLabel || "Status unavailable";
  if (!phaseLabel || stateLabel.toLocaleLowerCase() === phaseLabel.toLocaleLowerCase()) return stateLabel;
  return `${stateLabel} · ${phaseLabel}`;
}

function actionId() {
  return crypto.randomUUID();
}

export function createTorrentView({ root, submit, onMessage = () => {}, onTaskBack = () => {}, onTaskOpen = () => {}, storage } = {}) {
  if (!(root instanceof HTMLElement) || typeof submit !== "function") {
    throw new TypeError("Torrent view requires a root element and submit callback.");
  }
  let draftStorage = storage;
  if (draftStorage === undefined) {
    try { draftStorage = globalThis.localStorage; } catch { draftStorage = null; }
  }
  const drafts = readDrafts(draftStorage);
  const state = {
    snapshot: null,
    receipts: new Map(),
    mode: "catalog",
    catalogPage: 0,
    catalogQuery: "",
    catalogRequestId: null,
    catalogError: "",
    taskId: "",
    taskRef: "",
    taskRefType: "task_id",
    requestedTaskId: "",
    detailPage: 0,
    folderId: 0,
    folderTrail: [],
    detailQuery: "",
    filter: "music",
    detailRequestId: null,
    deferredDetailRequest: null,
    detailError: "",
    detailResult: null,
    draftStale: false,
    reviewRequestId: null,
    reviewSeq: null,
    reviewResult: null,
    reviewError: "",
    reviewCancelled: false,
    confirmRequestId: null,
    confirmTaskId: null,
    confirmError: "",
    controlRequestId: null,
    controlError: "",
    commandBusy: false,
    opened: false,
    disposed: false,
  };

  function draftFor(taskId = state.taskId) {
    return taskId ? drafts.get(taskId) ?? null : null;
  }

  function actionOperation(requestId) {
    return operationForRequest(state.snapshot, requestId, state.receipts.get(requestId));
  }

  function actionPending(requestId) {
    if (!requestId) return false;
    const operation = actionOperation(requestId);
    return !operation || !["completed", "complete", "succeeded", "failed", "stale", "rejected", "uncertain", "cancelled", "expired"]
      .includes(String(operation.status ?? "").toLowerCase());
  }

  function confirmationPendingFor(taskId) {
    return Boolean(taskId && state.confirmTaskId === taskId && actionPending(state.confirmRequestId));
  }

  function saveDraft() {
    const draft = draftFor();
    if (draft) drafts.set(state.taskId, draft);
    if (!writeDrafts(draftStorage, drafts)) onMessage("This browser could not save the task selection draft.", "error");
  }

  function dispatchDeferredDetail() {
    const deferred = state.deferredDetailRequest;
    if (!deferred || state.commandBusy) return false;
    state.deferredDetailRequest = null;
    if (!state.taskRef) return false;
    requestDetail(deferred);
    return true;
  }

  function send(payload, requestId, field) {
    state[field] = requestId;
    state.commandBusy = true;
    render();
    Promise.resolve(submit(payload, requestId)).then((queued) => {
      if (state.disposed) return;
      state.commandBusy = false;
      if (!queued && state[field] === requestId) {
        state[field] = null;
        state[`${field.replace("RequestId", "Error")}`] = "The request could not be saved or sent. Reconnect and try again.";
        if (field === "confirmRequestId") state.confirmTaskId = null;
      }
      if (!dispatchDeferredDetail()) render();
    }).catch((error) => {
      if (state.disposed) return;
      state.commandBusy = false;
      const errorField = field.replace("RequestId", "Error");
      if (state[field] === requestId) {
        state[field] = null;
        if (errorField in state) state[errorField] = String(error?.message ?? "The request could not be sent.");
        if (field === "confirmRequestId") state.confirmTaskId = null;
      }
      if (!dispatchDeferredDetail()) render();
    });
    return requestId;
  }

  function requestCatalog({ page = state.catalogPage, query = state.catalogQuery } = {}) {
    if (state.commandBusy || actionPending(state.catalogRequestId)) return null;
    const normalizedQuery = normalizeQuery(query);
    if (normalizedQuery === null) {
      state.catalogError = "Search text cannot contain control characters.";
      render();
      return null;
    }
    state.mode = "catalog";
    state.catalogPage = Math.max(0, Math.min(20_000, page));
    state.catalogQuery = normalizedQuery;
    state.catalogError = "";
    return send({ operation: "catalog", page: state.catalogPage, query: state.catalogQuery }, actionId(), "catalogRequestId");
  }

  function requestDetail({ page = 0, folderId = state.folderId, query = state.detailQuery, filter = state.filter } = {}) {
    if (!state.taskRef || actionPending(state.detailRequestId)) return null;
    const normalizedQuery = normalizeQuery(query);
    if (normalizedQuery === null) {
      state.detailError = "Search text cannot contain control characters.";
      render();
      return null;
    }
    state.mode = "detail";
    state.detailPage = Math.max(0, Math.min(20_000, page));
    state.folderId = Number.isSafeInteger(folderId) ? Math.max(0, Math.min(10_000, folderId)) : 0;
    state.detailQuery = normalizedQuery;
    state.filter = FILTERS.has(filter) ? filter : "music";
    state.detailError = "";
    state.reviewResult = null;
    state.reviewRequestId = null;
    state.reviewSeq = null;
    state.reviewError = "";
    if (state.commandBusy) {
      state.deferredDetailRequest = {
        page: state.detailPage,
        folderId: state.folderId,
        query: state.detailQuery,
        filter: state.filter,
      };
      render();
      return null;
    }
    state.deferredDetailRequest = null;
    return send({
      operation: "detail",
      [state.taskRefType]: state.taskRef,
      page: state.detailPage,
      folder_id: state.folderId,
      query: state.detailQuery,
      filter: state.filter,
    }, actionId(), "detailRequestId");
  }

  function enterTask(taskRef, refType = "task_id") {
    const safeId = String(taskRef ?? "");
    if (!SAFE_ID.test(safeId)) {
      state.taskId = "";
      state.mode = "catalog";
      state.detailError = "The task link is invalid. Return to the task list and choose a task.";
      render();
      return;
    }
    const safeType = refType === "hash" ? "hash" : "task_id";
    if (state.taskRef !== safeId || state.taskRefType !== safeType) {
      state.taskRef = safeId;
      state.taskRefType = safeType;
      state.taskId = "";
      state.folderId = 0;
      state.folderTrail = [];
      state.detailPage = 0;
      state.detailQuery = "";
      state.filter = "music";
      state.detailResult = null;
      state.detailRequestId = null;
      state.deferredDetailRequest = null;
      state.detailError = "";
      state.reviewResult = null;
      state.reviewRequestId = null;
      state.reviewSeq = null;
      state.draftStale = false;
      state.requestedTaskId = "";
    }
    state.mode = "detail";
    if (state.requestedTaskId !== safeId) {
      state.requestedTaskId = safeId;
      onTaskOpen(safeId, safeType);
      requestDetail({ page: 0, folderId: 0, query: "", filter: "music" });
    } else render();
  }

  function open(taskId, hash) {
    if (state.disposed) return;
    state.opened = true;
    if (taskId) return enterTask(taskId);
    if (hash) return enterTask(hash, "hash");
    if (state.mode === "detail" && state.taskRef) return render();
    state.mode = "catalog";
    if (!state.catalogRequestId && !catalogCurrent()) requestCatalog({ page: state.catalogPage, query: state.catalogQuery });
    else render();
  }

  function currentDetailOperation() {
    return actionOperation(state.detailRequestId);
  }

  function catalogOperation() {
    return actionOperation(state.catalogRequestId);
  }

  function catalogCurrent() {
    const operation = actionOperation(state.catalogRequestId);
    return Boolean(operation && ["completed", "complete", "succeeded"].includes(String(operation.status).toLowerCase()));
  }

  function currentReviewOperation() {
    return actionOperation(state.reviewRequestId);
  }

  function reconcile() {
    for (const [requestId, field, message] of [
      [state.catalogRequestId, "catalogError", "The task list result has expired. Refresh tasks to request a new list."],
      [state.detailRequestId, "detailError", "Task details have expired. Refresh files before continuing."],
      [state.reviewRequestId, "reviewError", "The review result expired. Request a fresh read-only review before confirming."],
      [state.confirmRequestId, "confirmError", "The confirmation result expired. Refresh the task to verify its saved selection before another change."],
      [state.controlRequestId, "controlError", "The task control result expired. Refresh tasks to read back the current state before another change."],
    ]) {
      const seq = state.receipts.get(requestId);
      if (!actionOperation(state.snapshot, requestId, seq)
          && requestSequenceCollision(state.snapshot, requestId, seq)) {
        state[field] = field === "catalogError"
          ? "The task list response belongs to a different request. Refresh tasks before continuing."
          : field === "detailError"
            ? "The task detail response belongs to a different request. Refresh files before selecting."
            : field === "reviewError"
              ? "The review response belongs to a different request. Request a fresh review before confirming."
              : field === "confirmError"
                ? "The save response belongs to a different request. Refresh the task to verify the saved selection."
                : "The task control response belongs to a different request. Refresh tasks to verify the current state.";
        if (field === "catalogError") state.catalogRequestId = null;
        else if (field === "detailError") state.detailRequestId = null;
        else if (field === "reviewError") state.reviewRequestId = null;
        else if (field === "confirmError") {
          state.confirmRequestId = null;
          state.confirmTaskId = null;
        }
        else state.controlRequestId = null;
        continue;
      }
      if (receiptAgedOut(state.snapshot, requestId, seq)) {
        state[field] = message;
        if (field === "catalogError") state.catalogRequestId = null;
        else if (field === "detailError") state.detailRequestId = null;
        else if (field === "reviewError") state.reviewRequestId = null;
        else if (field === "confirmError") {
          state.confirmRequestId = null;
          state.confirmTaskId = null;
        }
        else state.controlRequestId = null;
      }
    }
    const catalogOp = catalogOperation();
    if (catalogOp) {
      const status = String(catalogOp.status ?? "").toLowerCase();
      if (["failed", "stale", "rejected", "uncertain"].includes(status)) state.catalogError = String(catalogOp.error ?? catalogOp.result?.error ?? "The task list could not be loaded.");
    }
    const detailOp = currentDetailOperation();
    if (detailOp) {
      const status = String(detailOp.status ?? "").toLowerCase();
      if (["completed", "complete", "succeeded"].includes(status)) {
        const result = detailOp.result;
        const task = result?.task;
        if (result?.operation === "detail" && task && typeof task.task_id === "string"
            && Array.isArray(result.entries) && Array.isArray(result.scope_indexes)
            && Number.isSafeInteger(task.inventory_revision) && Number.isSafeInteger(task.selection_revision)
            && detailOp.details_incomplete !== true && detailOp.result_truncated !== true) {
          if (state.detailResult?.task?.task_id === task.task_id
              && (state.detailResult.task.inventory_revision !== task.inventory_revision
                || state.detailResult.task.selection_revision !== task.selection_revision)) {
            state.draftStale = true;
          }
          state.detailResult = result;
          state.taskId = task.task_id;
          const savedDraft = draftFor(task.task_id);
          if (!savedDraft) {
            drafts.set(task.task_id, {
              inventoryRevision: task.inventory_revision,
              selectionRevision: task.selection_revision,
              indexes: new Set(normalizeIndexes(task.selected_indexes)),
            });
            saveDraft();
          } else if (savedDraft.inventoryRevision !== task.inventory_revision
              || savedDraft.selectionRevision !== task.selection_revision) {
            state.draftStale = true;
          }
        } else {
          state.detailError = "The desktop returned an incomplete task detail. Refresh the task before selecting files.";
        }
      } else if (["failed", "stale", "rejected", "uncertain"].includes(status)) {
        state.detailError = String(detailOp.error ?? detailOp.result?.error ?? "Task details could not be loaded.");
      }
    }
    const reviewOp = currentReviewOperation();
    if (reviewOp) {
      const status = String(reviewOp.status ?? "").toLowerCase();
      if (["completed", "complete", "succeeded"].includes(status)) {
        const result = reviewOp.result;
        if (result?.operation === "review" && typeof result.review_id === "string" && SAFE_ID.test(result.review_id)
            && typeof result.task_id === "string" && Number.isSafeInteger(result.inventory_revision)
            && Number.isSafeInteger(result.selection_revision) && Array.isArray(result.selected_indexes)
            && Number.isSafeInteger(result.selected_count) && Array.isArray(result.entries)
            && Number.isSafeInteger(reviewOp.seq) && reviewOp.details_incomplete !== true && reviewOp.result_truncated !== true) {
          state.reviewResult = result;
          state.reviewSeq = reviewOp.seq;
          state.reviewError = "";
        } else {
          state.reviewError = "The review result is incomplete. Request a fresh review before confirming.";
        }
      } else if (["failed", "stale", "rejected", "uncertain"].includes(status)) {
        state.reviewError = String(reviewOp.error ?? reviewOp.result?.error ?? "The selection could not be reviewed.");
        state.reviewRequestId = null;
      }
    }
    const confirmOp = actionOperation(state.confirmRequestId);
    if (confirmOp) {
      const status = String(confirmOp.status ?? "").toLowerCase();
      if (["completed", "complete", "succeeded"].includes(status)) {
        const result = confirmOp.result;
        const confirmedTaskId = state.confirmTaskId;
        const isCurrentConfirmedTask = typeof confirmedTaskId === "string"
          && state.detailResult?.task?.task_id === confirmedTaskId;
        state.confirmRequestId = null;
        state.confirmTaskId = null;
        if (result?.operation === "confirm" && result.confirmed === true
            && typeof confirmedTaskId === "string" && result.task_id === confirmedTaskId
            && confirmOp.details_incomplete !== true && confirmOp.result_truncated !== true) {
          state.confirmError = isCurrentConfirmedTask
            ? "Selection saved. Music imports will follow the task as its files complete; downloads and seeding continue."
            : "";
          const folder = state.folderId;
          drafts.delete(confirmedTaskId);
          if (!writeDrafts(draftStorage, drafts)) {
            onMessage("The saved selection was confirmed, but this browser could not clear its local draft.", "error");
          }
          if (isCurrentConfirmedTask) {
            state.detailRequestId = null;
            state.deferredDetailRequest = null;
            state.reviewRequestId = null;
            state.reviewSeq = null;
            state.reviewResult = null;
            state.detailResult = null;
            state.draftStale = false;
            requestDetail({ page: state.detailPage, folderId: folder, query: state.detailQuery, filter: state.filter });
          }
        } else {
          const mismatch = "The confirmation response did not match the reviewed task. Refresh files and review the selection again.";
          if (isCurrentConfirmedTask && result?.task_id !== confirmedTaskId) {
            state.detailError = mismatch;
            state.reviewRequestId = null;
            state.reviewSeq = null;
            state.reviewResult = null;
            state.reviewError = "";
            state.confirmError = "";
          } else {
            state.confirmError = isCurrentConfirmedTask
              ? "The confirmation result was incomplete or expired. Refresh the task to verify its saved selection before trying again."
              : "";
          }
        }
      } else if (["failed", "stale", "rejected", "uncertain"].includes(status)) {
        state.confirmError = String(confirmOp.error ?? confirmOp.result?.error ?? "The selection was not confirmed.");
        state.confirmRequestId = null;
        state.confirmTaskId = null;
      }
    }
    const controlOp = actionOperation(state.controlRequestId);
    if (controlOp) {
      const status = String(controlOp.status ?? "").toLowerCase();
      if (["completed", "complete", "succeeded"].includes(status)) {
        state.controlRequestId = null;
        if (controlOp.result?.operation === "control" && controlOp.result.applied === true
            && controlOp.details_incomplete !== true && controlOp.result_truncated !== true) {
          state.controlError = "Task control completed. Showing the latest observed state.";
          requestCatalog({ page: state.catalogPage, query: state.catalogQuery });
        } else {
          state.controlError = "The control result was incomplete or expired. Refresh tasks to verify the current state before another change.";
        }
      } else if (["failed", "stale", "rejected", "uncertain"].includes(status)) {
        state.controlError = String(controlOp.error ?? controlOp.result?.error ?? "The task state could not be changed. Refresh to verify its current state.");
        state.controlRequestId = null;
      }
    }
  }

  function hasPendingResult() {
    const ids = [state.catalogRequestId, state.detailRequestId, state.reviewRequestId, state.confirmRequestId, state.controlRequestId];
    return ids.some((id) => {
      if (!id) return false;
      const operation = actionOperation(id);
      return !operation || !["completed", "complete", "succeeded", "failed", "stale", "rejected", "uncertain", "cancelled", "expired"]
        .includes(String(operation.status ?? "").toLowerCase()) || operationPending(state.snapshot, id);
    });
  }

  function setSnapshot(snapshot) {
    state.snapshot = snapshot;
    reconcile();
    render();
  }

  function recordReceipt(requestId, receipt) {
    if (!requestId || !Number.isSafeInteger(receipt?.seq) || receipt.seq <= 0) return;
    state.receipts.set(requestId, receipt.seq);
    while (state.receipts.size > 64) state.receipts.delete(state.receipts.keys().next().value);
    reconcile();
    render();
  }

  function fileRow(entry, draft, disabledForResult = false) {
    const selected = draft?.indexes.has(entry.index) ?? entry.selected === true;
    const disabled = !entry.supported_music || state.draftStale || state.commandBusy || disabledForResult;
    const reason = entry.needs_reconfirm ? "Review required after source changed"
      : !entry.supported_music ? "Unsupported Music file" : `Music: ${entry.music_stage ? prettyState(entry.music_stage) : "Ready for review"}`;
    return `<label class="torrent-file-row ${selected ? "is-selected" : ""}">
      <input type="checkbox" data-torrent-file="${entry.index}" ${selected ? "checked" : ""} ${disabled ? "disabled" : ""}>
      <span class="torrent-file-copy"><strong title="${escapeHtml(boundedText(entry.path))}">${escapeHtml(boundedText(entry.path))}</strong><small>${escapeHtml(formatBytes(entry.size))} · ${escapeHtml(formatProgress(entry.progress))} · ${escapeHtml(reason)}</small></span>
      <span class="torrent-file-status">${entry.complete ? "Complete" : "Downloading"}</span>
    </label>`;
  }

  function renderCatalog() {
    const operation = catalogOperation();
    const result = operation?.result?.operation === "catalog" ? operation.result : null;
    const status = String(operation?.status ?? "").toLowerCase();
    const rows = Array.isArray(result?.rows) ? result.rows : [];
    const pending = actionPending(state.catalogRequestId);
    const missingResult = operation && ["completed", "complete", "succeeded"].includes(status) && (!result || !Array.isArray(result.rows)
      || !Number.isSafeInteger(result.total) || typeof result.has_more !== "boolean" || operation.details_incomplete === true || operation.result_truncated === true);
    const error = state.catalogError || (missingResult ? "The task list result was incomplete or expired. Refresh tasks to request a new list." : operation?.result?.error ?? (status === "processing" ? "The desktop is loading the task list…" : ""));
    return `<div class="torrent-heading"><div><p class="eyebrow">ALL DOWNLOADS</p><h2>Choose a task for Music</h2><p>Downloads and seeding continue. Music import is a separate choice and only runs as selected files complete.</p></div><button class="secondary-button" type="button" data-torrent-catalog-refresh ${state.commandBusy ? "disabled" : ""}>Refresh tasks</button></div>
      <form class="torrent-search" data-torrent-catalog-search><label class="search-box"><span aria-hidden="true">⌕</span><input type="search" name="query" maxlength="256" value="${escapeHtml(state.catalogQuery)}" placeholder="Search task names" aria-label="Search torrent tasks"></label><button class="quiet-button" type="submit">Search</button></form>
      ${error ? `<p class="torrent-message ${status === "processing" ? "" : "error"}" role="status">${escapeHtml(error)}</p>` : pending ? `<p class="torrent-message" role="status">Waiting for your computer to return the task list…</p>` : ""}
      <p class="torrent-summary">${result ? `${Number(result.total ?? 0).toLocaleString()} tasks · page ${Number(result.page ?? 0) + 1}` : "Task list has not been loaded yet."}</p>
      <div class="torrent-task-list" aria-live="polite">${rows.map((row) => {
        const paused = /paused|stopped/iu.test(String(row.state ?? ""));
        const controlBusy = actionPending(state.controlRequestId);
        return `<article class="torrent-task-row"><button class="torrent-task-open" type="button" data-torrent-open="${escapeHtml(row.hash)}" ${pending ? "disabled" : ""}><strong>${escapeHtml(boundedText(row.name))}</strong><span>${escapeHtml(humanTorrentState(row.state))} · ${escapeHtml(formatProgress(row.progress))} · ${escapeHtml(formatBytes(row.size))}</span></button><button class="quiet-button torrent-control" type="button" data-torrent-control="${paused ? "resume" : "pause"}" data-torrent-hash="${escapeHtml(row.hash)}" ${state.commandBusy || controlBusy ? "disabled" : ""}>${paused ? "Resume" : "Pause"}</button></article>`;
      }).join("") || (result && !rows.length ? `<div class="empty-state"><h3>No matching tasks</h3><p>Change the search or refresh the list.</p></div>` : "")}</div>
      <nav class="torrent-pagination" aria-label="Task list pages"><button class="quiet-button" type="button" data-torrent-catalog-page="-1" ${!result || result.page <= 0 || state.commandBusy ? "disabled" : ""}>Previous</button><span>Page ${Number(result?.page ?? state.catalogPage) + 1}</span><button class="quiet-button" type="button" data-torrent-catalog-page="1" ${!result?.has_more || state.commandBusy ? "disabled" : ""}>Next</button></nav>
      ${state.controlError ? `<p class="torrent-message" role="status">${escapeHtml(state.controlError)}</p>` : ""}`;
  }

  function renderReview() {
    const result = state.reviewResult;
    const pending = actionPending(state.reviewRequestId);
    if (!result && !pending && !state.reviewError) return "";
    const entries = Array.isArray(result?.entries) ? result.entries : [];
    const detailTask = state.detailResult?.task;
    const revisionsMatch = detailTask && result
      && result.task_id === detailTask.task_id
      && result.inventory_revision === detailTask.inventory_revision
      && result.selection_revision === detailTask.selection_revision;
    const canConfirm = Boolean(result && revisionsMatch && Number.isSafeInteger(state.reviewSeq)
      && !state.commandBusy && !actionPending(state.confirmRequestId) && !state.draftStale);
    return `<section class="torrent-review" aria-labelledby="torrent-review-title"><div class="torrent-review-heading"><div><p class="eyebrow">READ-ONLY REVIEW</p><h3 id="torrent-review-title">Check this Music selection</h3></div><button class="quiet-button" type="button" data-torrent-review-cancel ${state.commandBusy || actionPending(state.confirmRequestId) ? "disabled" : ""}>Close review</button></div>
      ${pending ? `<p class="torrent-message" role="status">Waiting for a read-only review from your computer…</p>` : ""}
      ${state.reviewError ? `<p class="torrent-message error" role="alert">${escapeHtml(state.reviewError)}</p>` : ""}
      ${result ? `<div class="torrent-review-stats"><span><strong>${Number(result.selected_count ?? 0)}</strong> selected</span><span><strong>${escapeHtml(formatBytes(result.total_bytes))}</strong> total</span><span><strong>${Number(result.ready_count ?? 0)}</strong> ready</span><span><strong>${Number(result.waiting_count ?? 0)}</strong> still downloading</span><span><strong>${Number(result.existing_import_count ?? 0)}</strong> already imported</span></div><ul>${entries.map((entry) => `<li><strong>${escapeHtml(boundedText(entry.path))}</strong><span>${escapeHtml(formatBytes(entry.size))} · ${entry.complete ? "Complete" : "Waiting for download"} · ${escapeHtml(prettyState(entry.music_stage))}</span></li>`).join("")}</ul>${Number(result.more_count ?? 0) ? `<p class="field-hint">${Number(result.more_count).toLocaleString()} more selected files are included in the review.</p>` : ""}${!revisionsMatch ? `<p class="torrent-message error">The task changed after this review. Request a fresh review before saving.</p>` : ""}<button class="primary-button" type="button" data-torrent-confirm ${canConfirm ? "" : "disabled"}>${Number(result.selected_count ?? 0) === 0 ? "Save empty selection" : "Confirm Music selection"}</button>` : ""}</section>`;
  }

  function renderDetail() {
    const operation = actionOperation(state.detailRequestId);
    const result = state.detailResult;
    const task = result?.task;
    const entries = Array.isArray(result?.entries) ? result.entries : [];
    const draft = draftFor(task?.task_id);
    const selectedCount = draft?.indexes.size ?? Number(task?.selected_count ?? 0);
    const pending = actionPending(state.detailRequestId);
    const folders = entries.filter((entry) => entry?.kind === "folder");
    const files = entries.filter((entry) => entry?.kind === "file");
    const detailStatus = String(operation?.status ?? "").toLowerCase();
    const progress = task ? formatProgress(task.progress) : "Progress unavailable";
    const detailMissing = operation && ["completed", "complete", "succeeded"].includes(detailStatus)
      && (!result || !Array.isArray(result.entries) || !Array.isArray(result.scope_indexes) || !task
        || !Number.isSafeInteger(task.inventory_revision) || !Number.isSafeInteger(task.selection_revision)
        || operation.details_incomplete === true || operation.result_truncated === true);
    const detailError = state.detailError || (detailMissing ? "Task details were incomplete or expired. Refresh files before selecting." : operation?.result?.error ?? "");
    const detailBusy = Boolean(pending || detailStatus === "processing" || detailStatus === "pending" || detailStatus === "retryable");
    const selectionDisabled = Boolean(detailError || detailBusy || state.draftStale || confirmationPendingFor(task?.task_id));
    const folderLabel = state.folderTrail.at(-1)?.path ?? "Task root";
    return `<div class="torrent-heading"><div><button class="torrent-back" type="button" data-torrent-back>← All tasks</button><p class="eyebrow">MUSIC SELECTION</p><h2>${escapeHtml(boundedText(task?.name ?? "Torrent task"))}</h2><p>${escapeHtml(taskStatusLabel(task))} · ${escapeHtml(progress)}</p></div><button class="secondary-button" type="button" data-torrent-detail-refresh ${state.commandBusy ? "disabled" : ""}>Refresh files</button></div>
      ${detailError ? `<p class="torrent-message error" role="alert">${escapeHtml(detailError)}</p>` : state.deferredDetailRequest ? `<p class="torrent-message" role="status">Waiting for the previous request to finish before loading this task…</p>` : pending ? `<p class="torrent-message" role="status">Waiting for task files…</p>` : detailStatus === "processing" ? `<p class="torrent-message" role="status">Your computer is checking the task files…</p>` : ""}
      ${task ? `<div class="torrent-summary"><span>${selectedCount.toLocaleString()} selected</span><span>${Number(task.import_count ?? 0).toLocaleString()} imported</span></div>` : ""}
      ${state.draftStale ? `<aside class="torrent-stale" role="alert"><strong>Task files or saved selection changed.</strong><span>This draft is tied to older revisions and cannot be reviewed or confirmed.</span><button class="secondary-button" type="button" data-torrent-rebase ${state.commandBusy || confirmationPendingFor(task?.task_id) ? "disabled" : ""}>Discard draft and use current selection</button></aside>` : ""}
      <form class="torrent-search" data-torrent-detail-search><label class="search-box"><span aria-hidden="true">⌕</span><input type="search" name="query" maxlength="256" value="${escapeHtml(state.detailQuery)}" placeholder="Search folders and files" aria-label="Search task files"></label><select name="filter" class="select-control" aria-label="Filter files"><option value="music" ${state.filter === "music" ? "selected" : ""}>Music files</option><option value="all" ${state.filter === "all" ? "selected" : ""}>All files</option><option value="ready" ${state.filter === "ready" ? "selected" : ""}>Ready for Music</option><option value="waiting" ${state.filter === "waiting" ? "selected" : ""}>Waiting for download</option><option value="imported" ${state.filter === "imported" ? "selected" : ""}>Imported</option></select><button class="quiet-button" type="submit">Apply</button></form>
      <div class="torrent-folder-tools">${state.folderId ? `<button class="quiet-button" type="button" data-torrent-up ${detailBusy ? "disabled" : ""}>↑ Up</button>` : ""}<span class="field-hint">${escapeHtml(boundedText(folderLabel))}</span><button class="quiet-button" type="button" data-torrent-select-scope ${result?.scope_indexes?.length && !selectionDisabled ? "" : "disabled"}>Select matching (${Number(result?.scope_indexes?.length ?? 0).toLocaleString()})</button><button class="quiet-button" type="button" data-torrent-clear-scope ${selectedCount && !selectionDisabled ? "" : "disabled"}>Clear task selection</button></div>
      <div class="torrent-file-list" aria-live="polite">${folders.map((folder) => {
        const folderIndexes = normalizeIndexes(folder.indexes);
        const folderDraft = draftFor(task?.task_id);
        const allSelected = folderIndexes.length > 0 && folderIndexes.every((index) => folderDraft?.indexes.has(index));
        return `<div class="torrent-folder-row"><input type="checkbox" data-torrent-folder-select="${folder.id}" aria-label="Select files under ${escapeHtml(boundedText(folder.path))}" ${allSelected ? "checked" : ""} ${folderIndexes.length && !selectionDisabled ? "" : "disabled"}><button class="torrent-folder-open" type="button" data-torrent-folder="${folder.id}" data-torrent-folder-path="${escapeHtml(boundedText(folder.path))}" ${detailBusy ? "disabled" : ""}><span aria-hidden="true">▸</span><strong>${escapeHtml(boundedText(folder.path))}</strong><small>${Number(folderIndexes.length).toLocaleString()} matching files</small></button></div>`;
      }).join("")}${files.map((entry) => fileRow(entry, draft, selectionDisabled)).join("")}${!entries.length && result ? `<div class="empty-state"><h3>No files in this view</h3><p>Change the filter or search, or open a parent folder.</p></div>` : ""}</div>
      <nav class="torrent-pagination" aria-label="Task file pages"><button class="quiet-button" type="button" data-torrent-detail-page="-1" ${!result || result.page <= 0 || state.commandBusy ? "disabled" : ""}>Previous</button><span>Page ${Number(result?.page ?? state.detailPage) + 1} of ${Math.max(1, Math.ceil(Number(result?.total ?? 0) / TORRENT_PAGE_SIZE))}</span><button class="quiet-button" type="button" data-torrent-detail-page="1" ${!result?.has_more || state.commandBusy ? "disabled" : ""}>Next</button></nav>
      ${task?.error ? `<p class="torrent-message error" role="status">${escapeHtml(boundedText(task.error, 1024))}</p>` : ""}${state.confirmError ? `<p class="torrent-message" role="status">${escapeHtml(state.confirmError)}</p>` : ""}${state.reviewError && !state.reviewResult ? `<p class="torrent-message error" role="alert">${escapeHtml(state.reviewError)}</p>` : ""}
      ${renderReview()}
      <aside class="torrent-selection-bar" role="region" aria-label="Music selection" aria-live="polite"><span><strong>${selectedCount.toLocaleString()}</strong> selected</span><button class="primary-button" type="button" data-torrent-review ${task && !selectionDisabled && !state.commandBusy ? "" : "disabled"}>Review selection</button></aside>`;
  }

  function render() {
    if (state.disposed) return;
    root.innerHTML = state.mode === "detail" ? renderDetail() : renderCatalog();
    root.dataset.torrentMode = state.mode;
  }

  function selectionForIndexes(indexes, selected) {
    const task = state.detailResult?.task;
    const draft = draftFor(task?.task_id);
    if (!draft || state.draftStale || confirmationPendingFor(task?.task_id)) return;
    for (const index of indexes) {
      if (!validIndex(index)) continue;
      if (selected) draft.indexes.add(index);
      else draft.indexes.delete(index);
    }
    if (draft.indexes.size > MAX_SELECTION_INDEX + 1) {
      onMessage("The selection exceeds the supported 5,000-file limit.", "error");
      draft.indexes = new Set([...draft.indexes].slice(0, MAX_SELECTION_INDEX + 1));
    }
    saveDraft();
    state.reviewResult = null;
    state.reviewRequestId = null;
    state.reviewSeq = null;
    state.reviewError = "";
    render();
  }

  function requestReview() {
    const task = state.detailResult?.task;
    const draft = draftFor(task?.task_id);
    if (!task || !draft || state.draftStale || state.commandBusy || actionPending(state.reviewRequestId)) return;
    state.reviewResult = null;
    state.reviewSeq = null;
    state.reviewCancelled = false;
    state.reviewError = "";
    send({
      operation: "review",
      task_id: task.task_id,
      inventory_revision: draft.inventoryRevision,
      selection_revision: draft.selectionRevision,
      selected_indexes: [...draft.indexes].sort((a, b) => a - b),
    }, actionId(), "reviewRequestId");
  }

  function confirmReview() {
    if (!state.reviewResult || !Number.isSafeInteger(state.reviewSeq) || state.commandBusy || actionPending(state.confirmRequestId)) return;
    const task = state.detailResult?.task;
    if (!task || task.task_id !== state.reviewResult.task_id
        || task.inventory_revision !== state.reviewResult.inventory_revision
        || task.selection_revision !== state.reviewResult.selection_revision) {
      state.reviewError = "The task changed after review. Refresh it and request a new review.";
      render();
      return;
    }
    state.confirmError = "Saving the reviewed selection…";
    state.confirmTaskId = state.reviewResult.task_id;
    send({ operation: "confirm", review_seq: state.reviewSeq, review_id: state.reviewResult.review_id }, actionId(), "confirmRequestId");
  }

  root.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target.closest("[data-torrent-back], [data-torrent-catalog-refresh], [data-torrent-detail-refresh], [data-torrent-catalog-page], [data-torrent-detail-page], [data-torrent-open], [data-torrent-folder], [data-torrent-up], [data-torrent-select-scope], [data-torrent-clear-scope], [data-torrent-review], [data-torrent-review-cancel], [data-torrent-confirm], [data-torrent-rebase], [data-torrent-control]") : null;
    if (!target) return;
    if (target.hasAttribute("data-torrent-back")) {
      state.mode = "catalog";
      state.taskId = "";
      state.taskRef = "";
      state.requestedTaskId = "";
      state.detailRequestId = null;
      state.deferredDetailRequest = null;
      state.detailError = "";
      state.detailResult = null;
      state.reviewResult = null;
      state.reviewRequestId = null;
      state.reviewSeq = null;
      state.draftStale = false;
      onTaskBack();
      if (!catalogCurrent()) requestCatalog({ page: state.catalogPage, query: state.catalogQuery });
      else render();
      return;
    }
    if (target.hasAttribute("data-torrent-catalog-refresh")) { requestCatalog(); return; }
    if (target.hasAttribute("data-torrent-detail-refresh")) { requestDetail({ page: state.detailPage }); return; }
    if (target.hasAttribute("data-torrent-catalog-page")) { requestCatalog({ page: state.catalogPage + Number(target.dataset.torrentCatalogPage) }); return; }
    if (target.hasAttribute("data-torrent-detail-page")) { requestDetail({ page: state.detailPage + Number(target.dataset.torrentDetailPage) }); return; }
    if (target.hasAttribute("data-torrent-open")) { enterTask(target.dataset.torrentOpen, "hash"); return; }
    if (target.hasAttribute("data-torrent-folder")) {
      state.folderTrail.push({ id: state.folderId, path: target.dataset.torrentFolderPath ?? "" });
      requestDetail({ page: 0, folderId: Number(target.dataset.torrentFolder) });
      return;
    }
    if (target.hasAttribute("data-torrent-up")) {
      const parent = state.folderTrail.pop();
      requestDetail({ page: 0, folderId: parent?.id ?? 0 });
      return;
    }
    if (target.hasAttribute("data-torrent-select-scope")) {
      const scope = state.detailResult?.scope_indexes ?? [];
      const draft = draftFor(state.detailResult?.task?.task_id);
      const selected = scope.some((index) => !draft?.indexes.has(index));
      selectionForIndexes(scope, selected);
      return;
    }
    if (target.hasAttribute("data-torrent-clear-scope")) {
      selectionForIndexes([...(draftFor()?.indexes ?? [])], false);
      return;
    }
    if (target.hasAttribute("data-torrent-review")) { requestReview(); return; }
    if (target.hasAttribute("data-torrent-review-cancel")) {
      state.reviewResult = null;
      state.reviewRequestId = null;
      state.reviewSeq = null;
      state.reviewError = "";
      state.reviewCancelled = true;
      render();
      onMessage("Review closed. The saved Music selection and existing imports were not changed.");
      return;
    }
    if (target.hasAttribute("data-torrent-confirm")) { confirmReview(); return; }
    if (target.hasAttribute("data-torrent-rebase")) {
      const task = state.detailResult?.task;
      if (!task || confirmationPendingFor(task.task_id)) return;
      drafts.set(task.task_id, {
        inventoryRevision: task.inventory_revision,
        selectionRevision: task.selection_revision,
        indexes: new Set(normalizeIndexes(task.selected_indexes)),
      });
      state.draftStale = false;
      state.reviewResult = null;
      state.reviewRequestId = null;
      state.reviewSeq = null;
      saveDraft();
      render();
      return;
    }
    if (target.hasAttribute("data-torrent-control")) {
      const hash = target.dataset.torrentHash;
      const control = target.dataset.torrentControl;
      if (!hash || !["pause", "resume"].includes(control)) return;
      state.controlError = "Waiting for the desktop to apply and read back this task state…";
      send({ operation: "control", hash, control }, actionId(), "controlRequestId");
    }
  });

  root.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.matches("[data-torrent-file]")) {
      selectionForIndexes([Number(target.dataset.torrentFile)], target.checked);
      return;
    }
    if (target.matches("[data-torrent-folder-select]")) {
      const folderId = Number(target.dataset.torrentFolderSelect);
      const folder = state.detailResult?.entries?.find((entry) => entry.kind === "folder" && entry.id === folderId);
      selectionForIndexes(folder?.indexes ?? [], target.checked);
    }
  });

  root.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;
    event.preventDefault();
    if (form.matches("[data-torrent-catalog-search]")) {
      const query = new FormData(form).get("query");
      requestCatalog({ page: 0, query });
    } else if (form.matches("[data-torrent-detail-search]")) {
      const data = new FormData(form);
      requestDetail({ page: 0, folderId: state.folderId, query: data.get("query"), filter: data.get("filter") });
    }
  });

  return {
    open,
    currentTaskId() { return state.taskRef; },
    currentTaskRefType() { return state.taskRefType; },
    recordReceipt,
    setSnapshot,
    render,
    hasPendingResult,
    dispose() { state.disposed = true; root.replaceChildren(); },
  };
}
