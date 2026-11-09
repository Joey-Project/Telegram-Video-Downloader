export const MAX_SELECTED_ITEMS: 5_000;
export function updateSelectionWithinLimit(
  currentIds: Iterable<string>,
  candidateIds: Iterable<string>,
  selected: boolean,
  maximum?: number,
): { ids: string[]; omittedCount: number };
