export const MAX_SELECTED_ITEMS = 100;

export function updateSelectionWithinLimit(currentIds, candidateIds, selected, maximum = MAX_SELECTED_ITEMS) {
  const next = new Set();
  for (const value of currentIds) {
    if (typeof value === "string" && value.length > 0) next.add(value);
  }

  let omittedCount = 0;
  for (const value of candidateIds) {
    if (typeof value !== "string" || value.length === 0) continue;
    if (!selected) {
      next.delete(value);
      continue;
    }
    if (next.has(value)) continue;
    if (next.size >= maximum) {
      omittedCount += 1;
      continue;
    }
    next.add(value);
  }

  return { ids: [...next], omittedCount };
}
