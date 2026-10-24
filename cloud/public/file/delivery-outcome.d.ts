export type UnknownDeliveryAction = {
  request_id: string;
  kind: string;
  payload?: Record<string, unknown>;
  delivery_outcome: "outcome_unknown";
  delivery_error_code?: string;
  delivery_error?: string;
};
export type FileActionQueueRow = {
  request_id?: unknown;
  kind?: unknown;
  delivery_outcome?: unknown;
};

export function isUnknownDeliveryAction(action: unknown, remembered?: Map<string, unknown> | null): boolean;
export function unknownFileActionForKind<T extends FileActionQueueRow>(
  queue: readonly T[] | null | undefined,
  kind: string,
  remembered?: Map<string, unknown> | null,
): T | null;
export function markUnknownFileAction(
  state: {
    downloadSubmission?: {
      request_id?: string;
      outcomeUnknown?: boolean;
      outcomeError?: string;
      delivery_outcome?: "outcome_unknown";
      delivery_error_code?: string;
      delivery_error?: string;
    } | null;
    migrationConflictDrafts?: Map<string, Record<string, unknown>>;
    migrationConflictInvalidated?: Set<string>;
    previewCancellations?: Map<string, { requestId?: string | null; seq?: number | null; status?: string; error?: string | null }>;
    unknownActionOutcomes?: Map<string, { kind?: string; code?: string; error?: string }>;
    quarantinedActionIds?: Set<string>;
  },
  action: UnknownDeliveryAction,
): { downloadSubmission: boolean; migrationConflictDrafts: number };
export function dismissUnknownFileAction(
  state: {
    downloadSubmission?: { request_id?: string } | null;
    downloadMessage?: string | null;
    migrationConflictDrafts?: Map<string, Record<string, unknown>>;
    migrationConflictInvalidated?: Set<string>;
    previewCancellations?: Map<string, { requestId?: string | null; seq?: number | null; status?: string; error?: string | null }>;
    unknownActionOutcomes?: Map<string, unknown>;
    quarantinedActionIds?: Set<string>;
  },
  requestId: string,
  queue: readonly unknown[],
  writeQueue: (queue: unknown[]) => boolean,
): { status: "invalid" | "not_unknown" | "storage_failed" } | {
  status: "dismissed";
  action: UnknownDeliveryAction;
  downloadSubmission: boolean;
  migrationConflictDrafts: number;
};
