export const MAX_MIGRATION_ITEM_CONFLICTS = 10_000;

const POLICIES = new Set(["skip", "keep_both"]);

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function normalizeMigrationItemConflicts(value) {
  if (!isRecord(value)) return {};
  const entries = Object.entries(value).slice(0, MAX_MIGRATION_ITEM_CONFLICTS);
  const result = Object.create(null);
  for (const [itemId, policy] of entries) {
    if (!itemId || new TextEncoder().encode(itemId).byteLength > 256 || /[\u0000-\u001f\u007f]/u.test(itemId)) continue;
    if (POLICIES.has(policy)) result[itemId] = policy;
  }
  return result;
}

export function migrationConflictPolicy(result, itemId, overrides = {}) {
  const defaultPolicy = result?.conflict === "keep_both" ? "keep_both" : "skip";
  const normalized = normalizeMigrationItemConflicts(overrides);
  return normalized[itemId] ?? defaultPolicy;
}

export function setMigrationConflictChoice(current, itemId, policy, defaultPolicy = "skip") {
  if (typeof itemId !== "string" || !itemId || !POLICIES.has(policy)) return normalizeMigrationItemConflicts(current);
  const next = normalizeMigrationItemConflicts(current);
  if (policy === defaultPolicy) delete next[itemId];
  else {
    if (!Object.hasOwn(next, itemId) && Object.keys(next).length >= MAX_MIGRATION_ITEM_CONFLICTS) return next;
    next[itemId] = policy;
  }
  return next;
}

export function migrationConflictChoicesChanged(original, draft) {
  const left = normalizeMigrationItemConflicts(original);
  const right = normalizeMigrationItemConflicts(draft);
  const leftKeys = Object.keys(left).sort();
  const rightKeys = Object.keys(right).sort();
  return leftKeys.length !== rightKeys.length || leftKeys.some((key, index) => key !== rightKeys[index] || left[key] !== right[key]);
}

export function buildMigrationPreviewRefreshPayload(result, itemConflicts, supersedes) {
  if (!isRecord(result) || !["library", "selected"].includes(result.scope)) return null;
  if (typeof result.source_library_id !== "string" || typeof result.target_library_id !== "string") return null;
  if (!isRecord(supersedes)
    || !Number.isSafeInteger(supersedes.seq) || supersedes.seq <= 0
    || typeof supersedes.id !== "string" || !supersedes.id.trim()
    || typeof supersedes.revision !== "string" || !supersedes.revision.trim()) return null;
  const items = Array.isArray(result.items) ? result.items : [];
  const conflictIds = new Set(items.filter((item) => item?.conflict === true).map((item) => item.item_id).filter((id) => typeof id === "string"));
  const normalized = normalizeMigrationItemConflicts(itemConflicts);
  const overrides = Object.fromEntries(Object.entries(normalized).filter(([itemId]) => conflictIds.has(itemId)));
  const payload = {
    source_library_id: result.source_library_id,
    target_library_id: result.target_library_id,
    scope: result.scope,
    include_incomplete_tasks: result.include_incomplete_tasks === true,
    conflict: result.conflict === "keep_both" ? "keep_both" : "skip",
    set_target_default_on_success: result.set_target_default_on_success === true,
    supersedes_preview_seq: supersedes.seq,
    supersedes_preview_id: supersedes.id,
    supersedes_revision: supersedes.revision,
  };
  if (result.scope === "selected") {
    if (!Array.isArray(result.item_ids) || result.item_ids.length === 0) return null;
    payload.item_ids = [...result.item_ids];
  }
  if (Object.keys(overrides).length) payload.item_conflicts = overrides;
  return payload;
}

export function migrationRefreshMatches(result, expected) {
  if (!isRecord(result) || !isRecord(expected)) return false;
  const fieldsMatch = result.source_library_id === expected.source_library_id
    && result.target_library_id === expected.target_library_id
    && result.scope === expected.scope
    && (result.include_incomplete_tasks === true) === (expected.include_incomplete_tasks === true)
    && (result.conflict === "keep_both" ? "keep_both" : "skip") === expected.conflict
    && (result.set_target_default_on_success === true) === (expected.set_target_default_on_success === true);
  if (!fieldsMatch) return false;
  const resultIds = result.scope === "selected" && Array.isArray(result.item_ids) ? [...result.item_ids].sort() : [];
  const expectedIds = expected.scope === "selected" && Array.isArray(expected.item_ids) ? [...expected.item_ids].sort() : [];
  if (resultIds.length !== expectedIds.length || resultIds.some((id, index) => id !== expectedIds[index])) return false;
  return !migrationConflictChoicesChanged(result.item_conflicts, expected.item_conflicts);
}

export function migrationControlActions(migration) {
  if (!isRecord(migration)) return [];
  const status = migration.status;
  const active = status === "queued" || status === "running";
  const paused = status === "paused";
  const terminalWithTaskError = status === "partial" || status === "failed";
  const terminal = status === "completed" || status === "stopped";
  const hasReportedTaskTransferErrors = Number.isSafeInteger(migration.task_transfer_error_count)
    && migration.task_transfer_error_count > 0;
  const hasTaskTransferError = hasReportedTaskTransferErrors || (Array.isArray(migration.task_transfers)
    && migration.task_transfers.some((transfer) => isRecord(transfer)
      && typeof transfer.error === "string" && transfer.error.trim().length > 0));
  const hasFailedMedia = Number.isSafeInteger(migration.failed_count) && migration.failed_count > 0;
  const actions = [];
  if (active) actions.push("pause");
  if (paused) actions.push("resume");
  if (active || paused || terminalWithTaskError && (hasFailedMedia || hasTaskTransferError)) actions.push("stop_remaining");
  if (terminalWithTaskError && (hasFailedMedia || hasTaskTransferError)) actions.push("retry_failed");
  if (terminal && migration.terminal_finalization_pending === true) actions.push("finalize_terminal");
  return actions;
}

export function migrationHistoryNotice(omittedCount) {
  if (!Number.isSafeInteger(omittedCount) || omittedCount <= 0) return "";
  const noun = omittedCount === 1 ? "migration is" : "migrations are";
  return `${omittedCount} additional ${noun} not included in this view; local recovery records are retained.`;
}
