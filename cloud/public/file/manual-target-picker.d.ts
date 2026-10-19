export const MAX_MANUAL_TARGET_OPTIONS: 100;

export type ManualTargetLike = {
  id?: unknown;
  presence?: unknown;
  title?: unknown;
  relative_path?: unknown;
};

export type ManualTargetSearchResult<T extends ManualTargetLike> = {
  items: T[];
  hasMore: boolean;
  scannedCount: number;
};

export function searchPresentManualTargets<T extends ManualTargetLike>(
  items: readonly T[] | null | undefined,
  query: unknown,
  selectedId?: unknown,
  limit?: number,
): ManualTargetSearchResult<T>;
