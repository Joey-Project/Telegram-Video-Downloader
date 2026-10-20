export interface QueuedAction {
  request_id: string;
  kind: string;
  payload: Record<string, unknown>;
  surface?: "settings" | "file";
}

export const ACTION_QUEUE_LIMIT: number;
export const ACTION_SURFACE_SETTINGS: "settings";
export const ACTION_SURFACE_FILE: "file";
export function appendAction<T extends QueuedAction>(queue: T[], action: T): { ok: boolean; queue: T[] };
export function removeQueuedAction<T extends QueuedAction>(queue: T[], requestId: string): T[];
export function actionBelongsToSurface(action: QueuedAction, surface: "settings" | "file"): boolean;
export function nextQueuedActionForSurface(
  queue: readonly QueuedAction[] | null | undefined,
  surface: "settings" | "file",
  excludedIds?: ReadonlySet<string>,
): QueuedAction | null;
export function dispatchNextOwnedAction(options: {
  surface: "settings" | "file";
  readQueue: () => QueuedAction[];
  writeQueue: (queue: QueuedAction[]) => boolean;
  excludedIds?: ReadonlySet<string>;
  dispatch: (action: QueuedAction) => Promise<unknown>;
  onReceipt?: (action: QueuedAction, receipt: unknown) => unknown;
}): Promise<
  | { status: "empty" }
  | { status: "error"; action: QueuedAction; error: unknown }
  | { status: "sent" | "remove_failed"; action: QueuedAction; receipt: unknown; receiptUpdate: unknown }
>;
export function recordActionReceipt(
  action: QueuedAction,
  receipt: unknown,
  state?: {
    localActions?: Map<string, { seq?: number | null }>;
    downloadSubmission?: { request_id?: string; seq?: number | null } | null;
    migrationConflictDrafts?: Map<string, { refreshRequestId?: string | null; refreshSeq?: number | null }>;
  },
): { localAction: boolean; downloadSubmission: boolean; migrationConflictDrafts: number };
export function shouldRetryActionFailure(error: unknown): boolean;
