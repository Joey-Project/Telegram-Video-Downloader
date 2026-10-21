import type { QueuedAction } from "./action-queue.js";

export function createFileQueuedAction<T extends Record<string, unknown>>(
  kind: string,
  payload: T,
  requestId: string,
  now?: number,
): QueuedAction & { payload: T; queued_at_ms: number; replay_expires_at_ms: number };

export function fileActionRequestBody(action: QueuedAction): {
  request_id: string;
  kind: string;
  payload: Record<string, unknown>;
  queued_at_ms?: number;
  replay_expires_at_ms?: number;
};
