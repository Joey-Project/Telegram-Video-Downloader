export const MAX_MIGRATION_TASK_ROWS = 100;

const INCOMPLETE_TASK_STATUSES = new Set([
  "received",
  "preparing",
  "awaiting_selection",
  "awaiting_confirmation",
  "awaiting_duplicate_choice",
  "queued",
  "running",
  "verifying",
  "partially_completed",
  "interrupted",
  "failed",
]);

function safeNonnegativeInteger(value) {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function taskLibraryId(task) {
  const id = task?.library_id ?? task?.destination?.library_id ?? task?.target?.library_id;
  return typeof id === "string" && id.length > 0 ? id : "";
}

function isIncompleteVideoTask(task, libraryId) {
  if (String(task?.kind ?? "").toLowerCase() !== "video" || taskLibraryId(task) !== libraryId) return false;
  return INCOMPLETE_TASK_STATUSES.has(String(task?.status ?? "").toLowerCase());
}

function taskStagingBytes(task) {
  for (const value of [task?.staging_bytes, task?.partial_bytes, task?.downloaded_bytes]) {
    if (safeNonnegativeInteger(value)) return value;
  }
  return null;
}

export function incompleteMigrationTaskSummary(descriptor, tasks, libraryId = descriptor?.id) {
  const taskRows = Array.isArray(tasks) ? tasks : [];
  const hasReportedCount = safeNonnegativeInteger(descriptor?.incomplete_task_count);
  const hasReportedBytes = safeNonnegativeInteger(descriptor?.staging_bytes);
  const rowCount = Math.min(taskRows.length, MAX_MIGRATION_TASK_ROWS);
  let fallbackCount = 0;
  let fallbackBytes = 0;
  let fallbackBytesKnown = true;

  for (let index = 0; index < rowCount; index += 1) {
    const task = taskRows[index];
    if (!isIncompleteVideoTask(task, libraryId)) continue;
    fallbackCount += 1;
    const bytes = taskStagingBytes(task);
    if (bytes === null) fallbackBytesKnown = false;
    else fallbackBytes = Math.min(Number.MAX_SAFE_INTEGER, fallbackBytes + bytes);
  }

  const count = hasReportedCount ? descriptor.incomplete_task_count : fallbackCount;
  const fallbackIsComplete = taskRows.length < MAX_MIGRATION_TASK_ROWS
    && (!hasReportedCount || count === fallbackCount);
  const bytes = hasReportedBytes
    ? descriptor.staging_bytes
    : fallbackBytesKnown && fallbackIsComplete ? fallbackBytes : null;

  return {
    count,
    bytes,
    countIsLowerBound: !hasReportedCount && taskRows.length >= MAX_MIGRATION_TASK_ROWS,
  };
}
