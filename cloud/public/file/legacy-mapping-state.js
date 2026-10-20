export const LEGACY_MAPPING_PAGE_SIZE = 50;

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

export function hasDuplicateLegacyHintSelection(selections) {
  const seen = new Set();
  for (const selection of Array.isArray(selections) ? selections : []) {
    const rawHintIndex = selection?.hintIndex ?? selection?.dataset?.hintIndex;
    const hintIndex = typeof rawHintIndex === "number" ? rawHintIndex : Number(rawHintIndex);
    if (!Number.isSafeInteger(hintIndex) || hintIndex < 0 || seen.has(hintIndex)) return true;
    seen.add(hintIndex);
  }
  return false;
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

function hasCompleteLegacyImportDetails(operation) {
  const result = operation?.result && typeof operation.result === "object" ? operation.result : {};
  const nestedOperation = result.operation && typeof result.operation === "object" ? result.operation : {};
  return [operation, result, nestedOperation].every((value) => (
    value?.details_incomplete !== true && value?.result_truncated !== true
  ));
}

export function isUsableLegacyImport(operation) {
  const result = operation?.result;
  return operation?.kind === "legacy_import"
    && String(operation.status ?? "").toLowerCase() === "completed"
    && Array.isArray(result?.hints)
    && Array.isArray(result?.candidates)
    && hasCompleteLegacyImportDetails(operation);
}

export function selectLegacyImportForMapping(operations) {
  const imports = (Array.isArray(operations) ? operations : [])
    .filter((operation) => operation?.kind === "legacy_import");
  const latest = imports.at(-1) ?? null;
  const usable = [...imports].reverse().find(isUsableLegacyImport) ?? null;
  return {
    operation: usable ?? latest,
    usable: Boolean(usable),
    latest,
  };
}

function validHintIndex(value, hintCount) {
  const index = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(index) && index >= 0 && index < hintCount ? index : null;
}

export function buildLegacyMappingChoices(operationKey, hints, candidates, items) {
  const hintRows = Array.isArray(hints) ? hints : [];
  const presentItemIds = new Set(
    (Array.isArray(items) ? items : [])
      .filter((item) => typeof item?.id === "string" && item.id.length > 0 && String(item.presence ?? "").toLowerCase() === "present")
      .map((item) => item.id),
  );
  const candidatesByHint = new Map();
  for (const [candidateIndex, candidate] of (Array.isArray(candidates) ? candidates : []).entries()) {
    if (typeof candidate?.item_id !== "string" || !presentItemIds.has(candidate.item_id)) continue;
    const hintIndex = validHintIndex(candidate.hint_index, hintRows.length);
    if (hintIndex === null) continue;
    const rows = candidatesByHint.get(hintIndex) ?? [];
    rows.push({ candidate, candidateIndex });
    candidatesByHint.set(hintIndex, rows);
  }

  const choices = [];
  for (let hintIndex = 0; hintIndex < hintRows.length; hintIndex += 1) {
    const hint = hintRows[hintIndex];
    for (const { candidate, candidateIndex } of candidatesByHint.get(hintIndex) ?? []) {
      choices.push({
        key: `${operationKey}:${hintIndex}:${candidate.item_id}:${candidateIndex}`,
        kind: "candidate",
        hintIndex,
        hint,
        candidate,
      });
    }
    choices.push({
      key: `${operationKey}:manual:${hintIndex}`,
      kind: "manual",
      hintIndex,
      hint,
      hasCandidate: (candidatesByHint.get(hintIndex)?.length ?? 0) > 0,
    });
  }
  return choices;
}

export function paginateLegacyMappingChoices(choices, page, pageSize = LEGACY_MAPPING_PAGE_SIZE) {
  const rows = Array.isArray(choices) ? choices : [];
  const boundedPageSize = Number.isSafeInteger(pageSize) ? Math.max(1, Math.min(100, pageSize)) : LEGACY_MAPPING_PAGE_SIZE;
  const pageCount = Math.ceil(rows.length / boundedPageSize);
  const requestedPage = Number.isSafeInteger(page) ? page : 0;
  const currentPage = pageCount ? Math.max(0, Math.min(pageCount - 1, requestedPage)) : 0;
  const start = currentPage * boundedPageSize;
  const end = Math.min(rows.length, start + boundedPageSize);
  return {
    choices: rows.slice(start, end),
    page: currentPage,
    pageCount,
    pageSize: boundedPageSize,
    start,
    end,
    total: rows.length,
  };
}

export function selectLegacyMappingChoice(selections, key, hintIndex, selected, limit = 100) {
  const selection = selections?.get(key);
  if (!selection || !Number.isSafeInteger(hintIndex) || hintIndex < 0) return { ok: false, reason: "missing" };
  if (!selected) {
    selection.selected = false;
    return { ok: true, replaced: false };
  }

  const selectedEntries = [...selections.entries()].filter(([otherKey, value]) => otherKey !== key && value?.selected === true);
  const sameHint = selectedEntries.filter(([, value]) => Number(value?.hintIndex) === hintIndex);
  const otherHints = selectedEntries.length - sameHint.length;
  if (otherHints >= limit) return { ok: false, reason: "limit" };
  for (const [, value] of sameHint) value.selected = false;
  selection.selected = true;
  return { ok: true, replaced: sameHint.length > 0 };
}

export function buildLegacyMetadataPatches({ hints, choices, selections, items, limit = 100 }) {
  const hintRows = Array.isArray(hints) ? hints : [];
  const choiceRows = Array.isArray(choices) ? choices : [];
  const selected = choiceRows.filter((choice) => selections?.get(choice.key)?.selected === true);
  if (selected.length === 0) return { patches: [], error: "empty-selection" };
  if (selected.length > limit) return { patches: [], error: "selection-limit" };
  if (hasDuplicateLegacyHintSelection(selected.map((choice) => ({ hintIndex: choice.hintIndex })))) {
    return { patches: [], error: "duplicate-hint" };
  }

  const presentItemIds = new Set(
    (Array.isArray(items) ? items : [])
      .filter((item) => typeof item?.id === "string" && String(item.presence ?? "").toLowerCase() === "present")
      .map((item) => item.id),
  );
  const usedItems = new Set();
  const patches = [];
  for (const choice of selected) {
    const hint = hintRows[choice.hintIndex];
    if (!hint) return { patches: [], error: "invalid-hint" };
    const selection = selections.get(choice.key);
    const itemId = choice.kind === "manual" ? selection.item_id : choice.candidate?.item_id;
    if (choice.kind === "manual" && !itemId) return { patches: [], error: "manual-target-required" };
    if (typeof itemId !== "string" || !presentItemIds.has(itemId)) return { patches: [], error: "target-not-present" };
    if (usedItems.has(itemId)) return { patches: [], error: "duplicate-target" };
    usedItems.add(itemId);

    const patch = { item_id: itemId };
    if (choice.kind !== "manual") patch.hint_index = choice.hintIndex;
    if (selection.title && hint.title) patch.title = hint.title;
    if (selection.source_ids && Array.isArray(hint.source_ids) && hint.source_ids.length) patch.source_ids = hint.source_ids;
    if (selection.collection && hint.collection) patch.collection = hint.collection;
    if (Object.keys(patch).every((key) => key === "item_id" || key === "hint_index")) continue;
    patches.push(patch);
  }
  if (patches.length === 0) return { patches: [], error: "empty-fields" };
  if (patches.length > limit) return { patches: [], error: "selection-limit" };
  return { patches, error: null };
}
