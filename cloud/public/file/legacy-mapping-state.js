export function getOrCreateLegacySelection(selections, key, defaults) {
  const existing = selections.get(key);
  if (existing) return existing;
  const created = { ...defaults };
  selections.set(key, created);
  return created;
}

export function updateLegacySelection(selections, key, patch) {
  const selection = selections.get(key);
  if (!selection) return false;
  Object.assign(selection, patch);
  return true;
}

export function filterPresentLegacyCandidates(candidates, items) {
  const presentItemIds = new Set(
    (Array.isArray(items) ? items : [])
      .filter((item) =>
        typeof item?.id === "string"
        && item.id.length > 0
        && String(item?.presence ?? "").toLowerCase() === "present",
      )
      .map((item) => item.id),
  );
  return (Array.isArray(candidates) ? candidates : []).filter((candidate) =>
    typeof candidate?.item_id === "string" && presentItemIds.has(candidate.item_id),
  );
}
