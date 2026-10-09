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
