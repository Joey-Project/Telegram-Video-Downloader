const RECOVERY_STATUSES = new Set(["ready", "needs_confirmation", "unchanged", "unavailable", "unsupported", "deferred"]);
const ORGANIZATION_STATUSES = new Set(["standalone", "unreviewed"]);
const MAX_RECOVERY_ITEMS = 100;
const MAX_RECOVERY_CANDIDATES = 24;

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function onlyKeys(value, keys) {
  return Object.keys(value).every((key) => keys.includes(key));
}

function boundedText(value, maxLength) {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength
    && value.trim() === value && !/[\u0000-\u001f\u007f]/u.test(value);
}

function parseCollection(value) {
  if (!isRecord(value) || !onlyKeys(value, ["id", "title", "kind", "order", "part_id"])
      || !["id", "title", "kind", "order", "part_id"].every((key) => Object.hasOwn(value, key))
      || !boundedText(value.title, 1000) || !boundedText(value.kind, 64)) return false;
  if (!(value.id === null || boundedText(value.id, 128))) return false;
  if (!(value.part_id === null || boundedText(value.part_id, 128))) return false;
  return value.order === null || Number.isSafeInteger(value.order) && value.order >= 0 && value.order <= 0xffff_ffff;
}

function parsePatch(value, expectedItemId) {
  if (!isRecord(value) || !onlyKeys(value, ["item_id", "hint_index", "title", "source_ids", "collection", "organization_status", "fill_missing_only"])
      || value.item_id !== expectedItemId || !boundedText(value.item_id, 128)) return null;
  if (value.hint_index !== undefined && value.hint_index !== null
      && (!Number.isSafeInteger(value.hint_index) || value.hint_index < 0 || value.hint_index > 9999)) return null;
  if (value.title !== undefined && value.title !== null && !boundedText(value.title, 1000)) return null;
  if (value.source_ids !== undefined && value.source_ids !== null) {
    if (!Array.isArray(value.source_ids) || value.source_ids.length > 64
        || value.source_ids.some((entry) => !boundedText(entry, 128))
        || new Set(value.source_ids).size !== value.source_ids.length) return null;
  }
  if (value.collection !== undefined && value.collection !== null && !parseCollection(value.collection)) return null;
  if (value.organization_status !== undefined && value.organization_status !== null
      && !ORGANIZATION_STATUSES.has(value.organization_status)) return null;
  if (value.fill_missing_only !== undefined && typeof value.fill_missing_only !== "boolean") return null;
  if ((value.title === undefined || value.title === null)
      && (!Array.isArray(value.source_ids) || value.source_ids.length === 0)
      && (value.collection === undefined || value.collection === null)
      && (value.organization_status === undefined || value.organization_status === null)) return null;
  return Object.freeze({
    item_id: value.item_id,
    ...(value.title == null ? {} : { title: value.title }),
    ...(!Array.isArray(value.source_ids) || value.source_ids.length === 0 ? {} : { source_ids: Object.freeze([...value.source_ids]) }),
    ...(value.collection == null ? {} : { collection: Object.freeze({ ...value.collection }) }),
    ...(value.organization_status == null ? {} : { organization_status: value.organization_status }),
    ...(value.fill_missing_only === undefined ? {} : { fill_missing_only: value.fill_missing_only }),
  });
}

function parseCandidate(value, itemId) {
  if (!isRecord(value) || !onlyKeys(value, ["label", "patch"]) || !boundedText(value.label, 1000)) return null;
  const patch = parsePatch(value.patch, itemId);
  return patch ? Object.freeze({ label: value.label, patch }) : null;
}

function parseItem(value) {
  if (!isRecord(value) || !onlyKeys(value, ["item_id", "title", "relative_path", "status", "reason", "patch", "candidates"])
      || !boundedText(value.item_id, 128)
      || typeof value.title !== "string" || value.title.length > 1000
      || typeof value.relative_path !== "string" || value.relative_path.length > 4096
      || !RECOVERY_STATUSES.has(value.status)) return null;
  if (value.reason !== undefined && (typeof value.reason !== "string" || value.reason.length > 1000)) return null;
  const patch = value.patch === undefined ? undefined : parsePatch(value.patch, value.item_id);
  if (value.patch !== undefined && !patch) return null;
  let candidates;
  if (value.candidates !== undefined) {
    if (!Array.isArray(value.candidates) || value.candidates.length > MAX_RECOVERY_CANDIDATES) return null;
    const parsed = value.candidates.map((candidate) => parseCandidate(candidate, value.item_id));
    if (parsed.some((candidate) => !candidate)) return null;
    candidates = Object.freeze(parsed);
  }
  if (value.status === "ready" && !patch) return null;
  if (patch?.organization_status || candidates?.some((candidate) => candidate.patch.organization_status)) return null;
  return Object.freeze({
    item_id: value.item_id,
    title: value.title,
    relative_path: value.relative_path,
    status: value.status,
    ...(value.reason === undefined ? {} : { reason: value.reason }),
    ...(patch === undefined ? {} : { patch }),
    ...(candidates === undefined ? {} : { candidates }),
  });
}

export function parseMetadataRecoveryReport(value) {
  if (!isRecord(value) || !onlyKeys(value, ["revision", "items", "total", "remaining", "next_cursor"])
      || !["revision", "items", "total", "remaining", "next_cursor"].every((key) => Object.hasOwn(value, key))
      || !boundedText(value.revision, 256)
      || !Array.isArray(value.items) || value.items.length > MAX_RECOVERY_ITEMS
      || !Number.isSafeInteger(value.total) || value.total < 0
      || !Number.isSafeInteger(value.remaining) || value.remaining < 0
      || !(value.next_cursor === null || boundedText(value.next_cursor, 128))) return null;
  if (value.remaining > 0 && value.next_cursor === null) return null;
  if (value.remaining === 0 && value.next_cursor !== null) return null;
  const items = value.items.map(parseItem);
  if (items.some((item) => !item) || new Set(items.map((item) => item.item_id)).size !== items.length) return null;
  return Object.freeze({
    revision: value.revision,
    items: Object.freeze(items),
    total: value.total,
    remaining: value.remaining,
    next_cursor: value.next_cursor,
  });
}

export function groupMetadataRecoveryItems(items) {
  const groups = {
    ready: [],
    needs_confirmation: [],
    deferred: [],
    unchanged: [],
    unavailable: [],
    unsupported: [],
  };
  for (const item of Array.isArray(items) ? items : []) {
    if (Object.hasOwn(groups, item?.status)) groups[item.status].push(item);
  }
  return groups;
}

export function buildMetadataRecoveryPreviewPayload({ report, libraryId, rootBinding, itemIds, candidateChoices }) {
  if (!report || !boundedText(libraryId, 128) || !boundedText(rootBinding, 128)
      || !Array.isArray(itemIds) || itemIds.length === 0 || itemIds.length > MAX_RECOVERY_ITEMS) return null;
  if (new Set(itemIds).size !== itemIds.length) return null;
  const byId = new Map(report.items.map((item) => [item.item_id, item]));
  const patches = [];
  for (const itemId of itemIds) {
    const item = byId.get(itemId);
    if (!item) return null;
    let patch;
    if (item.status === "ready") patch = item.patch;
    else if (item.status === "needs_confirmation") {
      const choice = candidateChoices?.get(itemId);
      patch = Number.isSafeInteger(choice) ? item.candidates?.[choice]?.patch : undefined;
    }
    if (!patch) return null;
    patches.push({ ...patch, item_id: itemId, fill_missing_only: true });
  }
  return {
    library_id: libraryId,
    root_binding: rootBinding,
    item_ids: [...itemIds],
    metadata_patches: patches,
    expected_revision: report.revision,
  };
}

export function buildOrganizationStatusPatch(item, organizationStatus) {
  if (!item || typeof item.id !== "string" || !boundedText(item.id, 128)
      || !ORGANIZATION_STATUSES.has(organizationStatus)) return null;
  const hasCollection = Boolean(item.collection && (
    typeof item.collection === "string" ? item.collection.trim() : item.collection.title
  ));
  if (organizationStatus === "standalone" && hasCollection) return null;
  return { item_id: item.id, organization_status: organizationStatus };
}

export function metadataRecoveryOperationId(operation) {
  if (!isRecord(operation)) return "";
  if (typeof operation.request_id === "string" && operation.request_id) return operation.request_id;
  if (typeof operation.id === "string" && operation.id) return operation.id;
  return Number.isSafeInteger(operation.seq) ? String(operation.seq) : "";
}
