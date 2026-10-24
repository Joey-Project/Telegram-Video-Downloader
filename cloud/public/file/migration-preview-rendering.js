import {
  MAX_MIGRATION_ITEM_CONFLICTS,
  migrationConflictChoicesChanged,
  migrationRefreshMatches,
  normalizeMigrationItemConflicts,
} from "./migration-conflicts.js";
import { escapeHtml } from "./preview-rendering.js";
import { RECENT_OPERATION_SNAPSHOT_LIMIT, RECENT_SNAPSHOT_MAX_AGE_MS } from "./download-submission-history.js";

const MAX_SUMMARY_SAMPLE_ITEMS = 4;
const MAX_SUMMARY_PATH_CHARS = 240;
const ROOT_BINDING_PATTERN = /^v1:[0-9a-f]{64}$/u;
export const MIGRATION_PREVIEW_PAGE_SIZE = 30;

export function migrationPreviewDraftKey(id, revision) {
  if (typeof id !== "string" || !id || typeof revision !== "string" || !revision) return null;
  return JSON.stringify([id, revision]);
}

export function migrationPreviewEntries(snapshot, selectedLibraryId) {
  const active = [];
  for (const preview of Array.isArray(snapshot?.previews) ? snapshot.previews : []) {
    if (preview?.kind !== "library_migration_preview" || preview?.status !== "awaiting_confirmation") continue;
    active.push({
      ...preview,
      kind: "library_migration_preview",
      result: preview.result && typeof preview.result === "object" ? preview.result : null,
      id: preview.preview_id ?? preview.id,
      seq: preview.seq,
      incomplete: preview.preview_incomplete === true || preview.incomplete === true || preview.reload_required === true,
    });
  }
  for (const operation of Array.isArray(snapshot?.operations) ? snapshot.operations : []) {
    if (operation?.kind !== "library_migration_preview" || operation?.status !== "awaiting_confirmation") continue;
    const result = operation.result && typeof operation.result === "object" ? operation.result : {};
    active.push({
      ...operation,
      kind: "library_migration_preview",
      id: operation.preview_id ?? result.id ?? result.preview_id ?? operation.id,
      seq: operation.seq,
      result,
      incomplete: operation.details_incomplete === true || operation.result_truncated === true || result.incomplete === true,
    });
  }
  const unique = new Map();
  for (const operation of active) {
    const result = operation.result ?? {};
    const sourceLibraryId = result.source_library_id ?? operation.source_library_id;
    if (typeof selectedLibraryId === "string" && selectedLibraryId && sourceLibraryId !== selectedLibraryId) continue;
    const id = operation.id ?? result.id ?? result.preview_id;
    const revision = operation.revision ?? result.revision;
    if (!id || !revision) continue;
    const key = Number.isSafeInteger(operation.seq) && operation.seq > 0
      ? `seq:${operation.seq}`
      : migrationPreviewDraftKey(id, revision);
    if (!key) continue;
    const existing = unique.get(key);
    if (!existing || existing.incomplete && !operation.incomplete) {
      unique.set(key, { ...operation, id, revision });
    }
  }
  return [...unique.values()].reverse();
}

export function inspectMigrationRefreshHistory(refresh, snapshot, now = Date.now()) {
  if (!Number.isSafeInteger(refresh?.seq) || refresh.seq <= 0) return { kind: "unaccepted" };
  const operations = Array.isArray(snapshot?.operations) ? snapshot.operations : null;
  const previews = Array.isArray(snapshot?.previews) ? snapshot.previews : null;
  if (!operations || !previews) return { kind: "unknown" };
  const operation = [...operations, ...previews].find((entry) =>
    entry?.kind === "library_migration_preview" && entry.seq === refresh.seq);
  if (operation) return { kind: "recorded", operation };

  const pendingRequests = Array.isArray(snapshot?.pending_requests) ? snapshot.pending_requests : null;
  if (!pendingRequests) return { kind: "unknown" };
  const pending = pendingRequests.find((entry) => entry?.seq === refresh.seq);
  if (pending) return { kind: "pending", pending };

  const lastSeen = snapshot.last_seen;
  if (snapshot.synced !== true || snapshot.stale !== false
      || !Number.isSafeInteger(lastSeen) || lastSeen > now
      || now - lastSeen > RECENT_SNAPSHOT_MAX_AGE_MS
      || operations.length !== RECENT_OPERATION_SNAPSHOT_LIMIT
      || pendingRequests.length >= RECENT_OPERATION_SNAPSHOT_LIMIT) {
    return { kind: "unknown" };
  }

  let previousSequence = refresh.seq;
  for (const entry of operations) {
    if (typeof entry?.kind !== "string" || typeof entry.status !== "string"
        || !Number.isSafeInteger(entry.seq) || entry.seq <= previousSequence) {
      return { kind: "unknown" };
    }
    previousSequence = entry.seq;
  }
  return { kind: "outside_recent_history" };
}

export function releaseMigrationRefreshTracking(draft, snapshot, now = Date.now()) {
  if (!draft || typeof draft !== "object") return false;
  const seq = draft.refreshSeq;
  const requestId = draft.refreshRequestId;
  const history = inspectMigrationRefreshHistory({ seq: draft?.refreshSeq }, snapshot, now);
  if (history.kind !== "outside_recent_history") return false;
  draft.refreshRequestId = null;
  draft.refreshSeq = null;
  draft.refreshPayload = null;
  draft.refreshHistory = null;
  draft.refreshOutcomeUnknown = false;
  const requestIdentity = typeof requestId === "string" && requestId.length > 0
    ? `Request ${requestId} (sequence ${seq})`
    : `Request sequence ${seq}`;
  draft.refreshError = `${requestIdentity} is outside recent status history. Its result is unknown. Clearing this browser's waiting status does not cancel or resend it. Check your computer before refreshing again.`;
  return true;
}

export function reconcileMigrationRefreshDrafts(drafts, snapshot, now = Date.now(), invalidated = null) {
  const previews = Array.isArray(snapshot?.previews) ? snapshot.previews : [];
  const operations = Array.isArray(snapshot?.operations) ? snapshot.operations : [];
  const visible = [...operations, ...previews];
  for (const [draftKey, draft] of drafts) {
    if (!Number.isSafeInteger(draft.refreshSeq)) continue;
    const operation = visible.find((entry) =>
      entry?.kind === "library_migration_preview" && entry.seq === draft.refreshSeq);
    if (!operation) {
      draft.refreshHistory = inspectMigrationRefreshHistory({ seq: draft.refreshSeq }, snapshot, now).kind;
      continue;
    }
    draft.refreshHistory = "recorded";
    const result = operation.result && typeof operation.result === "object"
      ? operation.result
      : operation.preview && typeof operation.preview === "object" ? operation.preview : {};
    if (operation.status === "awaiting_confirmation") {
      const replacementId = result.id ?? result.preview_id ?? operation.preview_id ?? operation.id;
      const replacementRevision = result.revision ?? operation.revision;
      const replacementKey = migrationPreviewDraftKey(replacementId, replacementRevision);
      if (migrationRefreshMatches(result, draft.refreshPayload) && replacementKey && replacementKey !== draftKey) {
        drafts.delete(draftKey);
        invalidated?.delete(draftKey);
        continue;
      }
      if (operation.details_incomplete !== true && operation.result_truncated !== true) {
        draft.refreshRequestId = null;
        draft.refreshSeq = null;
        draft.refreshPayload = null;
        draft.refreshOutcomeUnknown = false;
        draft.refreshError = "The computer returned a preview with different conflict choices. Review it before continuing.";
      }
    } else if (["failed", "stale"].includes(String(operation.status))) {
      draft.refreshRequestId = null;
      draft.refreshSeq = null;
      draft.refreshPayload = null;
      draft.refreshOutcomeUnknown = false;
      draft.refreshError = String(operation.error ?? "The computer could not create a refreshed preview.");
    }
  }
}

export function createMigrationRefreshQueuedCallback({
  operation,
  previewId,
  revision,
  draft,
  payload,
  saveDraft,
  markInvalidated,
  render,
}) {
  const result = operation?.result && typeof operation.result === "object" ? operation.result : {};
  const currentDraft = draft && typeof draft === "object" ? draft : {};
  return (action) => {
    const key = saveDraft(previewId, revision, {
      ...currentDraft,
      refreshRequestId: action.request_id,
      rejectedRefreshRequestId: null,
      refreshPayload: payload,
      refreshSeq: null,
      refreshHistory: null,
      refreshOutcomeUnknown: false,
      refreshDeliveryOutcome: null,
      refreshDeliveryErrorCode: null,
      refreshQueuedAtMs: action.queued_at_ms,
      refreshExpiresAtMs: action.replay_expires_at_ms,
      refreshError: null,
    });
    if (key && migrationConflictChoicesChanged(result.item_conflicts, currentDraft.itemConflicts)) {
      markInvalidated(key);
    }
    render();
  };
}

export function migrationPreviewPageWindow(itemCount, requestedPage) {
  const count = Number.isSafeInteger(itemCount) && itemCount >= 0 ? itemCount : 0;
  const pageCount = Math.max(1, Math.ceil(count / MIGRATION_PREVIEW_PAGE_SIZE));
  const page = Number.isSafeInteger(requestedPage)
    ? Math.min(Math.max(0, requestedPage), pageCount - 1)
    : 0;
  const start = page * MIGRATION_PREVIEW_PAGE_SIZE;
  return {
    page,
    pageCount,
    start,
    end: Math.min(count, start + MIGRATION_PREVIEW_PAGE_SIZE),
  };
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

function displayText(value, maxChars = MAX_SUMMARY_PATH_CHARS) {
  const text = String(value ?? "");
  const characters = [...text];
  return characters.length > maxChars ? `${characters.slice(0, maxChars - 1).join("")}…` : text;
}

function validIdentity(value, { migrationId = false } = {}) {
  if (typeof value !== "string" || !value || value.length > 128 || value.trim() !== value) return false;
  if (/[\u0000-\u001f\u007f]/u.test(value)) return false;
  return !migrationId || /^[A-Za-z0-9-]+$/u.test(value);
}

function safeCount(value) {
  return Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function compactPreview(operation, result, options) {
  const envelopeId = operation?.preview_id ?? operation?.id;
  const envelopeRevision = operation?.revision;
  const resultId = result.id ?? result.preview_id;
  const id = typeof envelopeId === "string" ? envelopeId : resultId;
  const revision = typeof envelopeRevision === "string" ? envelopeRevision : result.revision;
  const identitiesMatch = (envelopeId === undefined || envelopeId === resultId)
    && (envelopeRevision === undefined || envelopeRevision === result.revision);

  const itemCount = safeCount(result.item_count);
  const fileCount = safeCount(result.file_count);
  const conflictCount = safeCount(result.conflict_count);
  const incompleteTaskCount = safeCount(result.incomplete_task_count);
  const omittedItemCount = safeCount(result.omitted_item_count);
  const omittedFileCount = safeCount(result.omitted_file_count);
  const byteFields = [result.total_bytes, result.required_space_bytes, result.available_space_bytes];
  const byteFieldsValid = byteFields.every((value) => Number.isSafeInteger(value) && value >= 0)
    && (result.staging_bytes === null || result.staging_bytes === undefined
      || Number.isSafeInteger(result.staging_bytes) && result.staging_bytes >= 0);
  const sampleItems = Array.isArray(result.sample_items) ? result.sample_items : [];
  const samples = sampleItems.slice(0, MAX_SUMMARY_SAMPLE_ITEMS);
  const locallyOmittedItemCount = sampleItems.length - samples.length;
  const displayedOmittedItemCount = (omittedItemCount ?? 0) + locallyOmittedItemCount;
  const sourceLibraryId = typeof result.source_library_id === "string" ? result.source_library_id : "";
  const targetLibraryId = typeof result.target_library_id === "string" ? result.target_library_id : "";
  const validScope = result.scope === "library" || result.scope === "selected";
  const validConflict = result.conflict === "skip" || result.conflict === "keep_both";
  const summaryValid = identitiesMatch
    && validIdentity(id, { migrationId: true })
    && validIdentity(revision)
    && validIdentity(sourceLibraryId)
    && validIdentity(targetLibraryId)
    && typeof result.source_root_binding === "string" && ROOT_BINDING_PATTERN.test(result.source_root_binding)
    && typeof result.target_root_binding === "string" && ROOT_BINDING_PATTERN.test(result.target_root_binding)
    && validScope
    && validConflict
    && typeof result.include_incomplete_tasks === "boolean"
    && typeof result.set_target_default_on_success === "boolean"
    && [itemCount, fileCount, conflictCount, incompleteTaskCount, omittedItemCount, omittedFileCount]
      .every((value) => value !== null)
    && Array.isArray(result.sample_items)
    && sampleItems.length <= MAX_SUMMARY_SAMPLE_ITEMS
    && itemCount === sampleItems.length + omittedItemCount
    && omittedFileCount === fileCount
    && byteFieldsValid
    && sampleItems.every((item) => item && typeof item === "object"
      && typeof item.item_id === "string"
      && typeof item.source_path === "string" && typeof item.target_path === "string"
      && (item.outcome === "move" || item.outcome === "skip"));

  const status = String(operation?.status ?? "unknown");
  const invalidated = options.invalidated === true || options.draftChanged === true
    || options.refreshPending === true || operation?.invalidated === true;
  const awaitingConfirmation = status === "awaiting_confirmation";
  const librariesAvailable = options.sourceAvailable === true && options.targetAvailable === true;
  const rootBindingsMatch = options.rootBindingsMatch === true;
  const canConfirm = summaryValid && awaitingConfirmation && librariesAvailable && rootBindingsMatch && !invalidated && options.cancelPending !== true;

  const summary = `${itemCount?.toLocaleString() ?? "Unknown"} media items · ${fileCount?.toLocaleString() ?? "Unknown"} files · ${formatBytes(result.total_bytes)} total`;
  const space = `Required space ${formatBytes(result.required_space_bytes)} · Available ${formatBytes(result.available_space_bytes)} · ${conflictCount?.toLocaleString() ?? "Unknown"} conflict${conflictCount === 1 ? "" : "s"}`;
  const taskBytes = result.staging_bytes === null || result.staging_bytes === undefined
    ? "staged data size unknown"
    : `${formatBytes(result.staging_bytes)} staged data`;
  const tasks = incompleteTaskCount > 0
    ? `<p>${incompleteTaskCount.toLocaleString()} unfinished tasks · ${taskBytes} ${result.include_incomplete_tasks === true ? "included" : "left in the source"}</p>`
    : "";
  const conflictLabel = result.conflict === "keep_both" ? "Keep both" : result.conflict === "skip" ? "Skip" : "Unknown";
  const sampleRows = samples.map((item) => {
    const outcome = item.outcome === "skip" || item.status === "skipped" ? "Will skip" : "Will move";
    const source = escapeHtml(displayText(item.source_path));
    const target = escapeHtml(displayText(item.target_path));
    const label = escapeHtml(displayText(item.item_id ?? "Selected media", 160));
    const truncated = [...String(item.source_path)].length > MAX_SUMMARY_PATH_CHARS
      || [...String(item.target_path)].length > MAX_SUMMARY_PATH_CHARS;
    const pathHint = truncated ? `<br><span class="field-hint">Sample path abbreviated for display.</span>` : "";
    return `<div class="preview-line"><span>${label}<br><span class="field-hint">${source} → ${target}</span>${pathHint}</span><strong class="${outcome === "Will skip" ? "skip" : "move"}">${outcome}</strong></div>`;
  }).join("");
  const omittedItems = displayedOmittedItemCount > 0
    ? `<p class="field-hint">Showing ${samples.length.toLocaleString()} sample media paths; ${displayedOmittedItemCount.toLocaleString()} item details omitted from this view.</p>`
    : "";
  const omittedFiles = omittedFileCount > 0
    ? `<p class="field-hint">${omittedFileCount.toLocaleString()} attached-file paths omitted from this view.</p>`
    : "";
  const selectionNotice = `<p>The complete ${escapeHtml(result.scope === "selected" ? "selected set" : "library selection")} and its existing conflict choices will be used.</p>`;
  const unavailable = !librariesAvailable
    ? `<p class="field-hint">The source or target library is offline or showing a saved scan. Reconnect it before starting the migration.</p>`
    : "";
  const identityChanged = !rootBindingsMatch
    ? `<p class="field-hint" role="alert">The registered source or target identity is unavailable or has changed. Request a fresh preview before continuing.</p>`
    : "";
  const stale = !awaitingConfirmation
    ? `<p class="field-hint" role="status">This preview is ${escapeHtml(status.replaceAll("_", " "))} and cannot be started.</p>`
    : "";
  const invalidatedMessage = invalidated
    ? `<p class="field-hint" role="alert">This preview has pending or changed conflict choices. Request a fresh preview before starting.</p>`
    : "";
  const invalidSummary = !summaryValid
    ? `<p class="field-hint" role="alert">The compact preview summary is incomplete or inconsistent. Request a fresh preview before starting.</p>`
    : "";

  return `<article class="stack-card"><div class="stack-card-head"><div><h3>Migration preview</h3><p>${escapeHtml(summary)}</p><p>${escapeHtml(space)}</p>${tasks}<p>Source: ${escapeHtml(sourceLibraryId)} · Target: ${escapeHtml(targetLibraryId)}</p>${selectionNotice}<p>Default conflict choice: ${conflictLabel}.</p>${unavailable}${identityChanged}${stale}${invalidatedMessage}${invalidSummary}</div><span class="state-pill pending">Compact review</span></div><div class="preview-table">${sampleRows || `<div class="preview-line"><span>No sample media paths are available.</span></div>`}${omittedItems}${omittedFiles}</div><div class="stack-actions"><button class="small-button confirm" type="button" data-migration-confirm="${escapeHtml(id)}" data-revision="${escapeHtml(revision)}" ${canConfirm ? "" : "disabled"}>Start migration</button>${options.cancelMarkup ?? ""}</div></article>`;
}

function fullPreview(operation, result, options) {
  const resultId = result.id ?? result.preview_id;
  const resultRevision = result.revision;
  const id = String(operation?.preview_id ?? operation?.id ?? resultId ?? "");
  const revision = String(operation?.revision ?? resultRevision ?? "");
  const sourceAvailable = options.sourceAvailable === true;
  const targetAvailable = options.targetAvailable === true;
  const rootBindingsValid = typeof result.source_root_binding === "string" && ROOT_BINDING_PATTERN.test(result.source_root_binding)
    && typeof result.target_root_binding === "string" && ROOT_BINDING_PATTERN.test(result.target_root_binding);
  const rootBindingsMatch = rootBindingsValid && options.rootBindingsMatch === true;
  if (operation?.incomplete || !Array.isArray(result.items)) {
    return `<article class="stack-card"><div class="stack-card-head"><div><h3>Migration preview needs to be regenerated</h3><p>${escapeHtml(operation?.reason ?? "Preview details are unavailable in this cloud snapshot. Request a fresh preview before confirming.")}</p></div><span class="state-pill pending">Refresh required</span></div></article>`;
  }
  if ((resultId !== undefined && resultId !== id) || (resultRevision !== undefined && resultRevision !== revision)) {
    return `<article class="stack-card"><div class="stack-card-head"><div><h3>Migration preview needs to be regenerated</h3><p>The preview identity changed while its details were being loaded. Request a fresh preview before continuing.</p></div><span class="state-pill pending">Refresh required</span></div></article>`;
  }
  const files = result.items;
  const candidateDraft = options.draft && typeof options.draft === "object" ? options.draft : null;
  const draft = candidateDraft?.previewId === id && candidateDraft?.revision === revision ? candidateDraft : null;
  const page = migrationPreviewPageWindow(files.length, options.page ?? draft?.page);
  const displayItems = files.slice(page.start, page.end);
  const savedConflicts = normalizeMigrationItemConflicts(result.item_conflicts);
  const draftConflicts = draft ? normalizeMigrationItemConflicts(draft.itemConflicts) : savedConflicts;
  const choicesChanged = Boolean(draft && migrationConflictChoicesChanged(savedConflicts, draftConflicts));
  const refreshOutcomeUnknown = draft?.refreshOutcomeUnknown === true;
  const invalidated = options.invalidated === true || choicesChanged || refreshOutcomeUnknown;
  const refreshPending = Boolean(draft?.refreshRequestId) && !refreshOutcomeUnknown;
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
      ? `<label class="migration-conflict-choice">${escapeHtml(itemId)} conflict<select class="select-control" data-migration-conflict-item="${escapeHtml(itemId)}" data-migration-preview-id="${escapeHtml(id)}" data-migration-preview-revision="${escapeHtml(revision)}" aria-label="Conflict handling for ${escapeHtml(itemId)}" ${refreshPending || refreshOutcomeUnknown || !sourceAvailable || !targetAvailable || !rootBindingsMatch ? "disabled" : ""}><option value="skip" ${policy === "skip" ? "selected" : ""}>Skip</option><option value="keep_both" ${policy === "keep_both" ? "selected" : ""}>Keep both</option></select></label>`
      : `<strong class="${item.outcome === "skip" || item.status === "skipped" ? "skip" : "move"}">${outcome}</strong>`;
    return `<div class="preview-line${conflict ? " migration-conflict-line" : ""}"><span>${path}${labels.length ? `<br><span class="field-hint">${labels.join("<br>")}</span>` : ""}</span>${choice}</div>`;
  }).join("");
  const pageControls = page.pageCount > 1
    ? `<nav class="toolbar migration-preview-pagination" aria-label="Migration preview item pages"><button class="quiet-button" type="button" data-migration-preview-page="${escapeHtml(id)}" data-migration-preview-revision="${escapeHtml(revision)}" data-page-direction="-1" ${page.page === 0 ? "disabled" : ""}>Previous items</button><span class="field-hint">Page ${page.page + 1} of ${page.pageCount} · items ${page.start + 1}–${page.end} of ${files.length.toLocaleString()} · choices on other pages are retained</span><button class="quiet-button" type="button" data-migration-preview-page="${escapeHtml(id)}" data-migration-preview-revision="${escapeHtml(revision)}" data-page-direction="1" ${page.page + 1 >= page.pageCount ? "disabled" : ""}>Next items</button></nav>`
    : "";
  const summary = `${Number(result.item_count ?? files.length).toLocaleString()} media items · ${Number(result.file_count ?? 0).toLocaleString()} files · ${formatBytes(result.total_bytes)} total`;
  const space = `Required space ${formatBytes(result.required_space_bytes)} · Available ${formatBytes(result.available_space_bytes)} · ${Number(result.conflict_count ?? 0)} conflict${Number(result.conflict_count ?? 0) === 1 ? "" : "s"}`;
  const taskBytes = result.incomplete_task_bytes ?? result.staging_bytes;
  const taskBytesLabel = taskBytes === null || taskBytes === undefined
    ? "staged data size unknown"
    : `${formatBytes(taskBytes)} staged data`;
  const tasks = Number(result.incomplete_task_count ?? 0) > 0
    ? `<p>${Number(result.incomplete_task_count).toLocaleString()} unfinished tasks · ${taskBytesLabel} ${result.include_incomplete_tasks === true ? "included" : "left in the source"}</p>`
    : "";
  const refreshOutsideHistory = draft?.refreshHistory === "outside_recent_history";
  const clearRefreshButton = refreshOutsideHistory
    ? `<button class="quiet-button" type="button" data-migration-refresh-clear="${escapeHtml(id)}" data-migration-preview-revision="${escapeHtml(revision)}">Clear waiting status</button>`
    : "";
  const refreshMessage = refreshOutcomeUnknown
    ? `<p class="field-hint" role="status">${escapeHtml(draft?.refreshError ?? "The refresh delivery outcome is unknown.")} It will not be sent again automatically. Check your computer before dismissing the notice.</p>`
    : refreshPending
    ? refreshOutsideHistory
      ? `<p class="field-hint" role="status">This refresh is outside recent history. Its result is unknown. Check your computer before clearing this browser's waiting status.</p>`
      : draft?.refreshHistory === "unknown"
        ? `<p class="field-hint" role="status">The available history cannot confirm whether this refresh finished. Its result remains unknown; waiting is unchanged.</p>`
        : `<p class="field-hint" role="status">Waiting for the updated preview from your computer. This preview cannot be confirmed.</p>`
    : draft?.refreshError ? `<p class="field-hint" role="alert">${escapeHtml(draft.refreshError)}</p>` : "";
  const staleMessage = invalidated && !refreshPending
    ? `<p class="field-hint" role="alert">Conflict choices changed. Refresh this preview before starting the migration.</p>`
    : "";
  const supersededMessage = options.invalidated === true && !draft
    ? `<p class="field-hint" role="status">This preview was superseded by a refreshed preview and cannot be confirmed.</p>`
    : "";
  const canConfirm = Boolean(id && revision) && !invalidated && sourceAvailable && targetAvailable && rootBindingsMatch && options.cancelPending !== true;
  const canRefresh = Boolean(id && revision && draft && (choicesChanged || draft.refreshError) && !refreshPending && !refreshOutcomeUnknown && rootBindingsMatch);
  const refreshButton = canRefresh
    ? `<button class="small-button" type="button" data-migration-refresh-conflicts="${escapeHtml(id)}" data-migration-preview-revision="${escapeHtml(revision)}" ${sourceAvailable && targetAvailable ? "" : "disabled"}>Refresh preview</button>`
    : "";
  const conflictLimit = Number(result.conflict_count ?? files.filter((item) => item?.conflict === true).length) > MAX_MIGRATION_ITEM_CONFLICTS
    ? `<p class="field-hint">This preview has more than ${MAX_MIGRATION_ITEM_CONFLICTS.toLocaleString()} conflicting bundles. Refresh with the global choice to review a supported subset.</p>`
    : "";
  const offlineMessage = sourceAvailable && targetAvailable ? "" : `<p class="field-hint">The source or target library is offline or showing a saved scan. Reconnect it and refresh before starting the migration.</p>`;
  const identityMessage = rootBindingsMatch ? "" : `<p class="field-hint" role="alert">The registered source or target identity is unavailable or has changed. Request a fresh preview before continuing.</p>`;
  return `<article class="stack-card"><div class="stack-card-head"><div><h3>Migration preview</h3><p>${escapeHtml(summary)}</p><p>${escapeHtml(space)}</p>${tasks}<p>Category and collection structure will be preserved. Default conflict choice: ${escapeHtml(result.conflict ?? "skip")}.</p>${offlineMessage}${identityMessage}${refreshMessage}${staleMessage}${supersededMessage}${conflictLimit}</div><span class="state-pill pending">Review needed</span></div>${pageControls}<div class="preview-table">${paths || `<div class="preview-line"><span>No media items were found in this selection.</span></div>`}</div>${pageControls}<div class="stack-actions">${clearRefreshButton}${refreshButton}<button class="small-button confirm" type="button" data-migration-confirm="${escapeHtml(id)}" data-revision="${escapeHtml(revision)}" ${canConfirm ? "" : "disabled"}>Start migration</button>${options.cancelMarkup ?? ""}</div></article>`;
}

export function renderMigrationPreview(operation, options = {}) {
  const result = operation?.result && typeof operation.result === "object" ? operation.result : {};
  if (operation?.migration_summary === true) return compactPreview(operation, result, options);
  return fullPreview(operation, result, options);
}
