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
export function operationHistoryEntries<T extends { kind?: string; status?: string; preview?: unknown; result?: unknown }>(operations: T[] | undefined): T[];
