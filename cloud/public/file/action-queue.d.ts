export interface QueuedAction {
  request_id: string;
  kind: string;
  payload: Record<string, unknown>;
}

export const ACTION_QUEUE_LIMIT: number;
export function appendAction<T extends QueuedAction>(queue: T[], action: T): { ok: boolean; queue: T[] };
export function removeQueuedAction<T extends QueuedAction>(queue: T[], requestId: string): T[];
