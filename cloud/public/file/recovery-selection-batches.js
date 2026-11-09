export const MAX_METADATA_RECOVERY_BATCH_ITEMS = 100;

const MAX_METADATA_RECOVERY_SELECTION_ITEMS = 5_000;
const MAX_ITEM_ID_LENGTH = 128;
const MAX_CONTEXT_VALUE_LENGTH = 128;

function validText(value, maxLength) {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength
    && value.trim() === value && !/[\u0000-\u001f\u007f]/u.test(value);
}

function validIds(itemIds) {
  if (!Array.isArray(itemIds) || itemIds.length === 0
      || itemIds.length > MAX_METADATA_RECOVERY_SELECTION_ITEMS) return false;
  const seen = new Set();
  for (const itemId of itemIds) {
    if (!validText(itemId, MAX_ITEM_ID_LENGTH) || seen.has(itemId)) return false;
    seen.add(itemId);
  }
  return true;
}

function createQueue(libraryId, rootBinding, itemIds, submittedCount) {
  return Object.freeze({
    libraryId,
    rootBinding,
    itemIds: Object.freeze([...itemIds]),
    submittedCount,
  });
}

function validQueue(queue) {
  return Boolean(queue && typeof queue === "object"
    && Object.isFrozen(queue)
    && Object.isFrozen(queue.itemIds)
    && validText(queue.libraryId, MAX_CONTEXT_VALUE_LENGTH)
    && validText(queue.rootBinding, MAX_CONTEXT_VALUE_LENGTH)
    && validIds(queue.itemIds)
    && Number.isSafeInteger(queue.submittedCount)
    && queue.submittedCount >= 0
    && queue.submittedCount <= queue.itemIds.length);
}

export function createRecoverySelectionQueue(input = {}) {
  if (!input || typeof input !== "object") return null;
  const { libraryId, rootBinding, itemIds } = input;
  if (!validText(libraryId, MAX_CONTEXT_VALUE_LENGTH)
      || !validText(rootBinding, MAX_CONTEXT_VALUE_LENGTH)
      || !validIds(itemIds)) return null;
  return createQueue(libraryId, rootBinding, itemIds, 0);
}

export function getCurrentRecoverySelectionBatch(queue) {
  if (!validQueue(queue) || queue.submittedCount >= queue.itemIds.length) return null;
  return Object.freeze(queue.itemIds.slice(
    queue.submittedCount,
    queue.submittedCount + MAX_METADATA_RECOVERY_BATCH_ITEMS,
  ));
}

export function getRecoverySelectionQueueStatus(queue) {
  if (!validQueue(queue)) return null;
  const totalCount = queue.itemIds.length;
  const remainingCount = totalCount - queue.submittedCount;
  return Object.freeze({
    totalCount,
    submittedCount: queue.submittedCount,
    remainingCount,
    currentCount: Math.min(MAX_METADATA_RECOVERY_BATCH_ITEMS, remainingCount),
    hasCurrentBatch: remainingCount > 0,
  });
}

export function matchesRecoverySelectionQueue(queue, context = {}) {
  if (!context || typeof context !== "object") return false;
  const { libraryId, rootBinding, selectedIds } = context;
  if (!validQueue(queue)
      || libraryId !== queue.libraryId
      || rootBinding !== queue.rootBinding
      || !validIds(selectedIds)
      || selectedIds.length !== queue.itemIds.length) return false;
  const queueIds = new Set(queue.itemIds);
  return selectedIds.every((itemId) => queueIds.has(itemId));
}

export function advanceRecoverySelectionQueue(
  queue,
  input = {},
) {
  if (!input || typeof input !== "object") return null;
  const { libraryId, rootBinding, selectedIds, outcome } = input;
  if (!validQueue(queue)) return null;
  if (!matchesRecoverySelectionQueue(queue, { libraryId, rootBinding, selectedIds })) return null;
  if (outcome !== "queued" || queue.submittedCount >= queue.itemIds.length) return queue;
  return createQueue(
    queue.libraryId,
    queue.rootBinding,
    queue.itemIds,
    Math.min(queue.itemIds.length, queue.submittedCount + MAX_METADATA_RECOVERY_BATCH_ITEMS),
  );
}
