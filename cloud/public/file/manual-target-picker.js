export const MAX_MANUAL_TARGET_OPTIONS = 100;

export function searchPresentManualTargets(items, query, selectedId = "", limit = MAX_MANUAL_TARGET_OPTIONS) {
  const boundedLimit = Number.isSafeInteger(limit)
    ? Math.max(0, Math.min(MAX_MANUAL_TARGET_OPTIONS, limit))
    : MAX_MANUAL_TARGET_OPTIONS;
  const normalizedQuery = String(query ?? "").trim().toLowerCase();
  const normalizedSelectedId = selectedId == null ? "" : String(selectedId);
  const matches = [];
  const seenIds = new Set();
  let selectedItem = null;
  let scannedCount = 0;
  let hasMore = false;

  for (const item of Array.isArray(items) ? items : []) {
    if (String(item?.presence ?? "").toLowerCase() !== "present") continue;
    const id = typeof item?.id === "string" || typeof item?.id === "number" ? String(item.id) : "";
    if (!id || seenIds.has(id)) continue;
    seenIds.add(id);
    scannedCount += 1;

    if (id === normalizedSelectedId) {
      selectedItem = item;
      continue;
    }

    const searchableText = [item?.title, item?.relative_path, id]
      .filter((value) => typeof value === "string" || typeof value === "number")
      .join(" ")
      .toLowerCase();
    if (normalizedQuery && !searchableText.includes(normalizedQuery)) continue;

    if (matches.length < boundedLimit + 1) matches.push(item);
    else hasMore = true;

    if (matches.length > boundedLimit && (!normalizedSelectedId || selectedItem)) {
      hasMore = true;
      break;
    }
  }

  const availableMatchSlots = Math.max(0, boundedLimit - (selectedItem ? 1 : 0));
  if (matches.length > availableMatchSlots) hasMore = true;
  return {
    items: [
      ...(selectedItem && boundedLimit > 0 ? [selectedItem] : []),
      ...matches.slice(0, availableMatchSlots),
    ],
    hasMore,
    scannedCount,
  };
}
