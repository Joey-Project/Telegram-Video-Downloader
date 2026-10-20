export function mergeRequestEntries<T extends { seq?: number }>(
  pendingRequests: T[] | undefined,
  tasks: T[] | undefined,
  operations: T[] | undefined,
): T[];

export function buildWaitingEntries<T extends { seq?: number }>(
  pendingRequests: T[] | undefined,
  tasks: T[] | undefined,
  operations: T[] | undefined,
): { cloud: T[]; local: T[] };

export function isPresentMedia(item: unknown): boolean;
export function presentMediaItems<T>(items: T[] | undefined): T[];
export function isReviewablePreviewStatus(status: unknown): boolean;
export function isIncompletePreview(entry: {
  incomplete?: boolean;
  id?: string;
  omitted_count?: number;
  preview?: unknown;
  preview_incomplete?: boolean;
  preview_id?: string;
  reason?: string;
  reload_required?: boolean;
  revision?: string;
  status?: string;
} | undefined): boolean;
export function isWaitingEntryPending(entry: { kind?: string; status?: string } | undefined): boolean;
export function operationHistoryEntries<T extends { kind?: string; status?: string; preview?: unknown; result?: unknown }>(operations: T[] | undefined): T[];
export function operationDetailsWarning(entry: {
  details_incomplete?: boolean;
  kind?: string;
  result?: unknown;
  result_truncated?: boolean;
} | undefined): string | null;
