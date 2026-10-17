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
