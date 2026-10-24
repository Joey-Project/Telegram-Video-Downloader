export const LIBRARY_PAGE_SIZE = 50;
export const MAX_FOLDER_SUGGESTIONS = 100;

function validRootBinding(value) {
  return typeof value === "string" && /^v1:[0-9a-f]{64}$/u.test(value);
}

export function isValidLibraryRootBinding(value) {
  return validRootBinding(value);
}

function hasInvalidRootBinding(entry) {
  return Boolean(entry && Object.hasOwn(entry, "root_binding") && entry.root_binding != null
    && !validRootBinding(entry.root_binding));
}

function mismatchedRootBinding(applied, live) {
  return hasInvalidRootBinding(applied) || hasInvalidRootBinding(live)
    || validRootBinding(applied?.root_binding) && validRootBinding(live?.root_binding)
      && applied.root_binding !== live.root_binding;
}

export function mediaLibraryEntries(ownerSnapshot) {
  const library = ownerSnapshot?.library;
  const applied = ownerSnapshot?.settings?.applied;
  const appliedEntries = Array.isArray(applied?.libraries) ? applied.libraries : [];
  const statusEntries = Array.isArray(library?.libraries) ? library.libraries : [];
  const merged = new Map();
  for (const entry of appliedEntries) {
    if (!entry || typeof entry.id !== "string") continue;
    merged.set(entry.id, { ...entry });
  }
  for (const entry of statusEntries) {
    if (!entry || typeof entry.id !== "string") continue;
    const appliedEntry = merged.get(entry.id);
    if (mismatchedRootBinding(appliedEntry, entry)) {
      merged.set(entry.id, {
        ...appliedEntry,
        id: entry.id,
        availability: "unavailable",
        available: false,
        snapshot: null,
        snapshot_stale: true,
      });
    } else {
      const combined = { ...appliedEntry, ...entry };
      if (validRootBinding(appliedEntry?.root_binding) && !validRootBinding(entry.root_binding)) {
        combined.root_binding = appliedEntry.root_binding;
      }
      merged.set(entry.id, combined);
    }
  }
  if (merged.size === 0 && library && typeof library === "object") {
    const id = String(applied?.default_library_id ?? library.default_library_id ?? "default");
    merged.set(id, {
      id,
      name: String(library.root_label ?? "Default library"),
      root: String(ownerSnapshot?.settings?.download_dir ?? ""),
      ...(Object.hasOwn(library, "root_binding") ? { root_binding: library.root_binding } : {}),
      default_category: String(applied?.default_category ?? ""),
      is_default: true,
      availability: library.availability,
      snapshot: library,
    });
  }
  return [...merged.values()];
}

export function defaultMediaLibraryId(ownerSnapshot) {
  const entries = mediaLibraryEntries(ownerSnapshot);
  const configured = ownerSnapshot?.settings?.applied?.default_library_id ?? ownerSnapshot?.library?.default_library_id;
  if (typeof configured === "string" && entries.some((entry) => entry.id === configured)) return configured;
  return String(entries.find((entry) => entry.is_default === true)?.id ?? entries[0]?.id ?? "");
}

export function resolveMediaLibrary(ownerSnapshot, requestedId = "") {
  const entries = mediaLibraryEntries(ownerSnapshot);
  const fallbackId = defaultMediaLibraryId(ownerSnapshot);
  const selected = entries.find((entry) => entry.id === requestedId)
    ?? entries.find((entry) => entry.id === fallbackId)
    ?? entries[0]
    ?? null;
  if (!selected) return { descriptor: null, snapshot: null, id: "" };
  const snapshot = selected.snapshot && typeof selected.snapshot === "object"
    ? selected.snapshot
    : selected.id === fallbackId && selected.snapshot_stale !== true
        && mediaLibraryAvailability(selected) !== "offline"
      ? ownerSnapshot?.library ?? null
      : null;
  return { descriptor: selected, snapshot, id: String(selected.id) };
}

export function mediaLibraryAvailability(descriptor) {
  const value = String(descriptor?.availability ?? "").toLowerCase();
  if (["offline", "unavailable"].includes(value) || descriptor?.available === false) return "offline";
  if (value === "available" || descriptor?.available === true) return "available";
  return "unknown";
}

export function registeredMediaLibrary(ownerSnapshot, libraryId) {
  if (!validLibraryIdentity(libraryId)) return null;
  return mediaLibraryEntries(ownerSnapshot).find((entry) => entry.id === libraryId) ?? null;
}

export function canOperateOnRegisteredMediaLibrary(ownerSnapshot, libraryId) {
  const descriptor = registeredMediaLibrary(ownerSnapshot, libraryId);
  return descriptor !== null && canOperateOnMediaLibrary(descriptor);
}

export function migrationEndpointAvailability(ownerSnapshot, sourceLibraryId, targetLibraryId) {
  const source = registeredMediaLibrary(ownerSnapshot, sourceLibraryId);
  const target = registeredMediaLibrary(ownerSnapshot, targetLibraryId);
  return {
    sourceRegistered: source !== null,
    targetRegistered: target !== null,
    sourceAvailable: source !== null && canOperateOnMediaLibrary(source),
    targetAvailable: target !== null && canOperateOnMediaLibrary(target),
  };
}

export function migrationPreviewBindingsMatch(ownerSnapshot, sourceLibraryId, targetLibraryId, sourceRootBinding, targetRootBinding) {
  if (!validRootBinding(sourceRootBinding) || !validRootBinding(targetRootBinding)) return false;
  const source = registeredMediaLibrary(ownerSnapshot, sourceLibraryId);
  const target = registeredMediaLibrary(ownerSnapshot, targetLibraryId);
  return source?.root_binding === sourceRootBinding && target?.root_binding === targetRootBinding;
}

export function selectMigrationTarget(entries, sourceLibraryId, currentTargetId, canOperate = () => true) {
  const targets = (Array.isArray(entries) ? entries : []).filter((entry) => entry?.id !== sourceLibraryId);
  const availableTargets = targets.filter((entry) => canOperate(entry.id));
  const selectedId = targets.some((entry) => entry.id === currentTargetId)
    ? currentTargetId
    : String(availableTargets[0]?.id ?? targets[0]?.id ?? "");
  return { targets, availableTargets, selectedId };
}

export function renderMigrationTargetOptions(selection, escapeHtml) {
  if (!selection || typeof escapeHtml !== "function") return "";
  return selection.targets.map((entry) => {
    const available = selection.availableTargets.some((candidate) => candidate.id === entry.id);
    const selected = entry.id === selection.selectedId;
    return `<option value="${escapeHtml(entry.id)}" data-root-binding="${escapeHtml(entry.root_binding ?? "")}"${selected ? " selected" : ""}${available ? "" : " disabled"}>${escapeHtml(entry.name)} · ${escapeHtml(entry.root ?? "root unavailable")}${available ? "" : " · offline"}</option>`;
  }).join("");
}

export function buildMigrationPreviewPayload({
  sourceLibraryId,
  targetLibraryId,
  sourceRootBinding,
  targetRootBinding,
  scope,
  itemIds = [],
  includeIncompleteTasks = false,
  conflict = "skip",
  setTargetDefaultOnSuccess = false,
} = {}) {
  if (typeof sourceLibraryId !== "string" || !sourceLibraryId
      || typeof targetLibraryId !== "string" || !targetLibraryId || sourceLibraryId === targetLibraryId) return null;
  if (!validRootBinding(sourceRootBinding) || !validRootBinding(targetRootBinding)) return null;
  if (scope !== "library" && scope !== "selected") return null;
  return {
    source_library_id: sourceLibraryId,
    target_library_id: targetLibraryId,
    source_root_binding: sourceRootBinding,
    target_root_binding: targetRootBinding,
    scope,
    ...(scope === "selected" ? { item_ids: Array.isArray(itemIds) ? [...itemIds] : [] } : {}),
    include_incomplete_tasks: includeIncompleteTasks === true,
    conflict: conflict === "keep_both" ? "keep_both" : "skip",
    set_target_default_on_success: setTargetDefaultOnSuccess === true,
  };
}

export function buildMigrationPreviewPayloadFromRenderedOptions({
  sourceOption,
  targetOption,
  ...options
} = {}) {
  return buildMigrationPreviewPayload({
    ...options,
    sourceLibraryId: sourceOption?.value,
    targetLibraryId: targetOption?.value,
    sourceRootBinding: sourceOption?.dataset?.rootBinding,
    targetRootBinding: targetOption?.dataset?.rootBinding,
  });
}

export function canOperateOnMediaLibrary(descriptor) {
  return validLibraryIdentity(descriptor?.id)
    && mediaLibraryAvailability(descriptor) !== "offline"
    && descriptor.snapshot_stale !== true;
}

function validLibraryIdentity(value) {
  return typeof value === "string" && value.length > 0 && value.length <= 128
    && value.trim() === value && !/[\u0000-\u001f\u007f]/u.test(value);
}

function safeNonnegativeCount(value) {
  return Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function libraryItemOmissionWarningCount(snapshot) {
  const warnings = Array.isArray(snapshot?.warnings) ? snapshot.warnings : [];
  const prefix = "Cloud snapshot omitted ";
  const suffix = " library item(s) from the complete source view to fit the upload budget.";
  for (const warning of warnings) {
    if (typeof warning !== "string" || !warning.startsWith(prefix) || !warning.endsWith(suffix)) continue;
    const digits = warning.slice(prefix.length, -suffix.length);
    if (!/^\d+$/u.test(digits)) continue;
    const count = Number(digits);
    if (Number.isSafeInteger(count) && count >= 0) return count;
  }
  return null;
}

export function mediaSnapshotInfo(descriptor, snapshot) {
  const items = Array.isArray(snapshot?.items) ? snapshot.items : [];
  const reportedCount = safeNonnegativeCount(descriptor?.media_count);
  const warningOmittedCount = libraryItemOmissionWarningCount(snapshot);
  const hasSnapshot = Boolean(snapshot && typeof snapshot === "object");
  const totalCount = Math.max(
    reportedCount ?? 0,
    items.length,
    hasSnapshot ? items.length + (warningOmittedCount ?? 0) : 0,
  );
  const omittedCount = hasSnapshot ? Math.max(0, totalCount - items.length) : 0;
  return {
    totalCount,
    retainedCount: items.length,
    omittedCount,
  };
}

export function mediaSnapshotOmissionNotice(descriptor, snapshot) {
  const info = mediaSnapshotInfo(descriptor, snapshot);
  if (info.omittedCount === 0) return "";
  const totalLabel = Number.isSafeInteger(info.totalCount) ? info.totalCount.toLocaleString() : "an unknown total";
  return `The cloud view retains ${info.retainedCount.toLocaleString()} of ${totalLabel} media items; ${info.omittedCount.toLocaleString()} item details are omitted. Search, filters, and pagination cover only the retained rows.`;
}

function operationLibraryBinding(operation) {
  const result = operation?.result && typeof operation.result === "object" ? operation.result : {};
  const recoveryIdentity = operation?.recovery_identity && typeof operation.recovery_identity === "object"
    ? operation.recovery_identity
    : {};
  const nested = result.operation && typeof result.operation === "object" ? result.operation : {};
  const preview = result.preview && typeof result.preview === "object" ? result.preview : {};
  const candidates = [
    [recoveryIdentity, "library_id"],
    [result, "library_id"], [result, "source_library_id"],
    [nested, "library_id"], [nested, "source_library_id"],
    [preview, "library_id"], [preview, "source_library_id"],
    [operation, "source_library_id"], [operation, "library_id"],
  ];
  for (const [container, key] of candidates) {
    if (Object.prototype.hasOwnProperty.call(container, key)) {
      const value = container[key];
      return { present: true, valid: validLibraryIdentity(value), id: validLibraryIdentity(value) ? value : "" };
    }
  }
  return { present: false, valid: false, id: "" };
}

export function operationLibraryId(operation) {
  const binding = operationLibraryBinding(operation);
  return binding.valid ? binding.id : "";
}

export function operationMatchesLibrary(operation, libraryId) {
  const binding = operationLibraryBinding(operation);
  return binding.present ? binding.valid && binding.id === libraryId : true;
}

export function itemFolder(item) {
  const path = String(item?.relative_path ?? "").replaceAll("\\", "/");
  const slash = path.lastIndexOf("/");
  return slash < 0 ? "" : path.slice(0, slash);
}

export function statusOf(item) {
  return String(item?.presence ?? "unknown").toLowerCase();
}

export function movedToTarget(item) {
  if (statusOf(item) !== "missing") return null;
  const target = item?.moved_to;
  if (!target || typeof target !== "object" || Array.isArray(target)) return null;
  const libraryId = target.library_id;
  const libraryName = target.library_name;
  const itemId = target.item_id;
  const relativePath = target.relative_path;
  if (!validLibraryIdentity(libraryId)
      || typeof libraryName !== "string" || !libraryName.trim()
      || typeof itemId !== "string" || !itemId.trim() || itemId.trim() !== itemId
      || /[\u0000-\u001f\u007f]/u.test(itemId)
      || typeof relativePath !== "string" || !relativePath.trim()) return null;
  return {
    library_id: libraryId,
    library_name: libraryName,
    item_id: itemId,
    relative_path: relativePath,
  };
}

export function librarySelectionCounts(items, selectedIds, canOperate) {
  const rows = Array.isArray(items) ? items : [];
  const selected = selectedIds && typeof selectedIds[Symbol.iterator] === "function"
    ? new Set(selectedIds)
    : new Set();
  const presentCount = rows.filter((item) => statusOf(item) === "present").length;
  return {
    totalCount: rows.length,
    selectedCount: rows.filter((item) => selected.has(String(item?.id ?? ""))).length,
    selectableCount: canOperate ? presentCount : 0,
    skippedUnavailableCount: rows.length - presentCount,
    readOnlyCount: canOperate ? 0 : presentCount,
  };
}

export function collectionIdentity(collection) {
  if (typeof collection === "string") {
    const title = collection.trim();
    return title ? JSON.stringify(["", "title", title]) : JSON.stringify(["uncollected"]);
  }
  if (!collection || typeof collection !== "object") return JSON.stringify(["uncollected"]);

  const kind = typeof collection.kind === "string" ? collection.kind.trim() : "";
  const id = typeof collection.id === "string" ? collection.id.trim() : "";
  const title = String(collection.title ?? "").trim();
  return id
    ? JSON.stringify([kind, "id", id])
    : JSON.stringify([kind, "title", title]);
}

function collectionOrder(item) {
  const order = item?.collection?.order;
  return Number.isSafeInteger(order) && order >= 0 ? order : null;
}

function libraryTitle(item) {
  return String(item?.title ?? item?.relative_path ?? "");
}

function sortLibraryItems(items) {
  const titleSorted = [...items].sort((left, right) => libraryTitle(left).localeCompare(libraryTitle(right)));
  const groups = new Map();
  for (const item of titleSorted) {
    const identity = collectionIdentity(item?.collection);
    if (!groups.has(identity)) groups.set(identity, []);
    groups.get(identity).push(item);
  }

  return [...groups.values()].flatMap((group) => group
    .map((item, index) => ({ item, index, order: collectionOrder(item) }))
    .sort((left, right) => {
      if (left.order !== null && right.order !== null && left.order !== right.order) return left.order - right.order;
      if (left.order !== null && right.order === null) return -1;
      if (left.order === null && right.order !== null) return 1;
      return libraryTitle(left.item).localeCompare(libraryTitle(right.item)) || left.index - right.index;
    })
    .map(({ item }) => item));
}

export function needsOrganizing(item) {
  if (movedToTarget(item)) return false;
  const confidence = String(item?.confidence ?? "low").toLowerCase();
  const metadata = String(item?.metadata_status ?? "unknown").toLowerCase();
  const hasCollection = Boolean(item?.collection && (typeof item.collection === "string" ? item.collection.trim() : item.collection.title));
  const standalone = String(item?.organization_status ?? "").toLowerCase() === "standalone";
  return confidence !== "high" || metadata !== "complete" || !hasCollection && !standalone || ["missing", "unavailable"].includes(statusOf(item));
}

export function filterLibraryItems(items, { query = "", category = "", status = "" } = {}) {
  const normalizedQuery = String(query).trim().toLocaleLowerCase();
  const filtered = (Array.isArray(items) ? items : []).filter((item) => {
    const text = [item?.title, item?.relative_path, item?.id, ...(Array.isArray(item?.source_ids) ? item.source_ids : [])].join(" ").toLocaleLowerCase();
    return (!normalizedQuery || text.includes(normalizedQuery))
      && (!category || itemFolder(item) === category)
      && (!status || statusOf(item) === status);
  });
  return sortLibraryItems(filtered);
}

export function paginateLibraryItems(items, page, pageSize = LIBRARY_PAGE_SIZE) {
  const rows = Array.isArray(items) ? items : [];
  const boundedPageSize = Number.isSafeInteger(pageSize) ? Math.max(1, Math.min(LIBRARY_PAGE_SIZE, pageSize)) : LIBRARY_PAGE_SIZE;
  const pageCount = Math.ceil(rows.length / boundedPageSize);
  const requestedPage = Number.isSafeInteger(page) ? page : 0;
  const currentPage = pageCount ? Math.max(0, Math.min(pageCount - 1, requestedPage)) : 0;
  const start = currentPage * boundedPageSize;
  const end = Math.min(rows.length, start + boundedPageSize);
  return {
    items: rows.slice(start, end),
    page: currentPage,
    pageCount,
    pageSize: boundedPageSize,
    start,
    end,
    total: rows.length,
  };
}

export function groupLibraryItems(items) {
  const groups = new Map();
  for (const item of Array.isArray(items) ? items : []) {
    const collection = typeof item?.collection === "string" ? item.collection : item?.collection?.title;
    const title = collection || "Uncollected";
    const identity = collectionIdentity(item?.collection);
    if (!groups.has(identity)) groups.set(identity, { title, items: [] });
    groups.get(identity).items.push(item);
  }
  return [...groups.values()];
}

export function boundedFolderSuggestions(folders, query = "", limit = MAX_FOLDER_SUGGESTIONS) {
  const maxSuggestions = Number.isSafeInteger(limit) ? Math.max(0, Math.min(MAX_FOLDER_SUGGESTIONS, limit)) : MAX_FOLDER_SUGGESTIONS;
  if (maxSuggestions === 0) return [];
  const normalizedQuery = String(query ?? "").trim().toLocaleLowerCase();
  const suggestions = [];
  const seen = new Set();
  for (const folder of Array.isArray(folders) ? folders : []) {
    if (typeof folder !== "string" || !folder || seen.has(folder)) continue;
    seen.add(folder);
    if (normalizedQuery && !folder.toLocaleLowerCase().startsWith(normalizedQuery)) continue;
    suggestions.push(folder);
    if (suggestions.length >= maxSuggestions) break;
  }
  return suggestions;
}
