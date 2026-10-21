export interface QueuedAction {
  request_id: string;
  kind: string;
  payload: Record<string, unknown>;
  surface?: "settings" | "file";
  queued_at_ms?: number;
  replay_expires_at_ms?: number;
  delivery_outcome?: "outcome_unknown";
  delivery_error_code?: "action_replay_window_expired" | "action_replay_window_unverifiable";
  delivery_error?: string;
}

export interface LocalActionReceiptTracker {
  kind?: string;
  payload?: Record<string, unknown>;
  label?: string;
  seq?: number | null;
  rejected?: boolean;
  error?: string;
  queued_at_ms?: number;
  replay_expires_at_ms?: number;
  delivery_outcome?: "outcome_unknown";
  delivery_error_code?: "action_replay_window_expired" | "action_replay_window_unverifiable";
  delivery_error?: string;
}
export interface DownloadSubmissionReceiptTracker {
  request_id?: string;
  payload?: Record<string, unknown>;
  seq?: number | null;
  rejected?: boolean;
  error?: string;
  queued_at_ms?: number;
  replay_expires_at_ms?: number;
  delivery_outcome?: "outcome_unknown";
  delivery_error_code?: "action_replay_window_expired" | "action_replay_window_unverifiable";
  delivery_error?: string;
}
export interface MigrationConflictReceiptTracker {
  refreshRequestId?: string | null;
  rejectedRefreshRequestId?: string | null;
  refreshPayload?: Record<string, unknown> | null;
  refreshSeq?: number | null;
  refreshError?: string | null;
  refreshQueuedAtMs?: number | null;
  refreshExpiresAtMs?: number | null;
  refreshDeliveryOutcome?: "outcome_unknown" | null;
}

export const ACTION_QUEUE_LIMIT: number;
export const ACTION_REPLAY_WINDOW_MS: number;
export const ACTION_SURFACE_SETTINGS: "settings";
export const ACTION_SURFACE_FILE: "file";
export const ACTION_RECEIPT_STORAGE_PREFIX: string;
export function createQueuedAction<T extends QueuedAction>(action: T, now?: number): T & { queued_at_ms: number; replay_expires_at_ms: number };
export function actionReplayStatus(
  action: QueuedAction,
  now?: number,
): { status: "valid"; code: null } | { status: "expired" | "unverifiable"; code: "action_replay_window_expired" | "action_replay_window_unverifiable" };
export function appendAction<T extends QueuedAction>(queue: T[], action: T): { ok: boolean; queue: T[] };
export function persistQueuedActionAndDrain<T extends QueuedAction>(options: {
  action: T;
  readQueue: () => T[];
  writeQueue: (queue: T[]) => boolean;
  onQueued?: (action: T) => void;
  afterPersist?: (action: T) => void;
  isOnline: () => boolean;
  onOffline?: (action: T) => void;
  onSending?: (action: T) => void;
  drain: () => Promise<unknown>;
}): Promise<{ status: "queue_full" | "storage_failed" | "queued" | "drained"; action: T }>;
export function removeQueuedAction<T extends QueuedAction>(queue: T[], requestId: string): T[];
export function actionBelongsToSurface(action: QueuedAction, surface: "settings" | "file"): boolean;
export function nextQueuedActionForSurface(
  queue: readonly QueuedAction[] | null | undefined,
  surface: "settings" | "file",
  excludedIds?: Set<string>,
): QueuedAction | null;
export function dispatchNextOwnedAction(options: {
  surface: "settings" | "file";
  readQueue: () => QueuedAction[];
  writeQueue: (queue: QueuedAction[]) => boolean;
  excludedIds?: Set<string>;
  dispatch: (action: QueuedAction) => Promise<unknown>;
  onReceipt?: (action: QueuedAction, receipt: unknown, surface: "settings" | "file") => unknown;
}): Promise<
  | { status: "empty" }
  | { status: "error"; action: QueuedAction; error: unknown }
  | { status: "rejected"; action: QueuedAction; rejection: { outcome: "rejected"; status: number; error: string }; receiptUpdate: unknown }
  | { status: "rejection_receipt_failed"; action: QueuedAction; error: unknown; receiptUpdate?: unknown; receiptError?: unknown }
  | { status: "rejection_remove_failed"; action: QueuedAction; rejection: { outcome: "rejected"; status: number; error: string }; receiptUpdate: unknown }
  | { status: "outcome_unknown"; action: QueuedAction; code: "action_replay_window_expired" | "action_replay_window_unverifiable"; error: string; persisted: boolean }
  | { status: "receipt_failed"; action: QueuedAction; receipt: unknown; receiptUpdate?: unknown; error?: unknown }
  | { status: "sent" | "remove_failed"; action: QueuedAction; receipt: unknown; receiptUpdate: unknown }
>;
export function recordActionReceipt(
  action: QueuedAction,
  receipt: unknown,
  state?: {
    localActions?: ReadonlyMap<string, LocalActionReceiptTracker>;
    downloadSubmission?: DownloadSubmissionReceiptTracker | null;
    migrationConflictDrafts?: ReadonlyMap<string, MigrationConflictReceiptTracker>;
  },
  options?: { surface?: "settings" | "file"; storage?: Storage; now?: number },
): { persisted: boolean; matched: boolean; localAction: boolean; downloadSubmission: boolean; migrationConflictDrafts: number; rejected?: boolean; rejectedRequestIds?: string[]; consume?: () => void };
export function recoverActionReceipts(
  state: {
    localActions?: ReadonlyMap<string, LocalActionReceiptTracker>;
    downloadSubmission?: DownloadSubmissionReceiptTracker | null;
    migrationConflictDrafts?: ReadonlyMap<string, MigrationConflictReceiptTracker>;
  },
  options: { surface: "settings" | "file"; storage?: Storage },
): { matched: number; localActions: boolean; downloadSubmission: boolean; migrationConflictDrafts: number; requestIds: string[]; rejectedRequestIds: string[]; rejections: Array<{ request_id: string; kind: string; status: number; error: string }> };
export function consumeRejectedActionReceipts(
  state: {
    localActions?: ReadonlyMap<string, LocalActionReceiptTracker>;
    downloadSubmission?: DownloadSubmissionReceiptTracker | null;
    migrationConflictDrafts?: ReadonlyMap<string, MigrationConflictReceiptTracker>;
  },
  options: { surface: "settings" | "file"; requestIds: readonly string[]; storage?: Storage },
): { consumed: number };
export function restorePendingActions(
  state: {
    localActions?: ReadonlyMap<string, LocalActionReceiptTracker>;
    downloadSubmission?: DownloadSubmissionReceiptTracker | null;
    migrationConflictDrafts?: ReadonlyMap<string, MigrationConflictReceiptTracker>;
  },
  options: { surface: "settings" | "file"; readQueue: () => QueuedAction[]; writeQueue: (queue: QueuedAction[]) => boolean },
): { requeued: number; skippedAtLimit: number; writeFailed: boolean };
export function shouldRetryActionFailure(error: unknown): boolean;
