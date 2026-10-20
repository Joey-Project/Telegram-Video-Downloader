export const ACTION_QUEUE_LIMIT = 20;

export function appendAction(queue, action) {
  if (queue.length >= ACTION_QUEUE_LIMIT) return { ok: false, queue };
  return { ok: true, queue: [...queue, action] };
}

export function removeQueuedAction(queue, requestId) {
  return queue.filter((action) => action.request_id !== requestId);
}

export function shouldRetryActionFailure(error) {
  const status = error && typeof error === "object" ? error.status : undefined;
  if (!Number.isInteger(status)) return true;
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}
