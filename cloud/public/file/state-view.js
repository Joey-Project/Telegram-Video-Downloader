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
  "partial_failure",
  "stale",
  "rejected",
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
