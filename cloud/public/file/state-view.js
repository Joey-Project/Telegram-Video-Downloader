export function mergeRequestEntries(pendingRequests, tasks, operations) {
  const entries = new Map();
  for (const [group, values] of [["cloud", pendingRequests], ["task", tasks], ["operation", operations]]) {
    if (!Array.isArray(values)) continue;
    values.forEach((entry, index) => {
      if (!entry || typeof entry !== "object") return;
      const seq = Number(entry.seq);
      const key = Number.isSafeInteger(seq) && seq > 0 ? `seq:${seq}` : `${group}:${index}`;
      entries.set(key, entry);
    });
  }
  return [...entries.values()];
}

export function buildWaitingEntries(pendingRequests, tasks, operations) {
  const cloud = mergeRequestEntries(pendingRequests, [], []);
  const local = mergeRequestEntries([], tasks, operations);
  const localSequences = new Set(local.map((entry) => Number(entry.seq)).filter(Number.isSafeInteger));
  return {
    cloud: cloud.filter((entry) => !localSequences.has(Number(entry.seq))),
    local,
  };
}

const TERMINAL_PREVIEW_STATUSES = new Set([
  "confirmed",
  "completed",
  "cancelled",
  "expired",
  "failed",
  "uncertain",
  "partial_failure",
  "partial",
  "stale",
  "rejected",
  "stopped",
  "superseded",
]);

const TERMINAL_WAITING_STATUSES = new Set([
  "completed",
  "failed",
  "cancelled",
  "confirmed",
  "expired",
  "stale",
  "rejected",
  "uncertain",
  "partial_failure",
  "partial",
  "stopped",
  "superseded",
]);

export function isPresentMedia(item) {
  return String(item?.presence ?? "unknown").toLowerCase() === "present";
}

export function presentMediaItems(items) {
  return Array.isArray(items) ? items.filter(isPresentMedia) : [];
}

export function isReviewablePreviewStatus(status) {
  return !TERMINAL_PREVIEW_STATUSES.has(String(status ?? "").toLowerCase());
}

export function isIncompletePreview(entry) {
  return !entry
    || entry.incomplete === true
    || entry.preview_incomplete === true
    || entry.reload_required === true
    || !entry.preview
    || typeof entry.preview !== "object";
}

export function isWaitingEntryPending(entry) {
  const status = String(entry?.status ?? "pending").toLowerCase();
  return ["file_preview", "library_migration_preview"].includes(entry?.kind)
    ? isReviewablePreviewStatus(status)
    : !TERMINAL_WAITING_STATUSES.has(status);
}

function hasPreviewPayload(entry) {
  return Boolean(
    (entry?.preview && typeof entry.preview === "object")
    || (entry?.result?.preview && typeof entry.result.preview === "object"),
  );
}

export function operationHistoryEntries(operations) {
  const entries = Array.isArray(operations) ? operations : [];
  return entries.filter((entry) => (
    entry?.kind !== "file_preview"
    || !hasPreviewPayload(entry)
    || !isReviewablePreviewStatus(entry.status)
  ));
}

export function operationDetailsWarning(entry) {
  if (!entry || typeof entry !== "object") return null;
  const result = entry.result && typeof entry.result === "object" ? entry.result : {};
  const nestedOperation = result.operation && typeof result.operation === "object"
    ? result.operation
    : {};
  const incomplete = [entry, result, nestedOperation].some((value) => (
    value.details_incomplete === true || value.result_truncated === true
  ));
  if (!incomplete) return null;
  return entry.kind === "legacy_import"
    ? "The saved legacy import report is incomplete. Some hints and matches were omitted; submit the import again to review the full report."
    : "Some operation details were omitted from the saved history.";
}
