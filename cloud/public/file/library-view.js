export const LIBRARY_PAGE_SIZE = 50;
export const MAX_FOLDER_SUGGESTIONS = 100;

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
    merged.set(entry.id, { ...merged.get(entry.id), ...entry });
  }
  if (merged.size === 0 && library && typeof library === "object") {
    const id = String(applied?.default_library_id ?? library.default_library_id ?? "default");
    merged.set(id, {
      id,
      name: String(library.root_label ?? "Default library"),
      root: String(ownerSnapshot?.settings?.download_dir ?? ""),
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
    : selected.id === fallbackId
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

export function operationLibraryId(operation) {
  const result = operation?.result && typeof operation.result === "object" ? operation.result : {};
  const nested = result.operation && typeof result.operation === "object" ? result.operation : {};
  const preview = result.preview && typeof result.preview === "object" ? result.preview : {};
  const id = result.library_id ?? result.source_library_id ?? nested.library_id ?? nested.source_library_id
    ?? preview.library_id ?? preview.source_library_id ?? operation?.source_library_id ?? operation?.library_id;
  return typeof id === "string" ? id : "";
}

export function operationMatchesLibrary(operation, libraryId) {
  const id = operationLibraryId(operation);
  return !id || id === libraryId;
}

export function itemFolder(item) {
  const path = String(item?.relative_path ?? "").replaceAll("\\", "/");
  const slash = path.lastIndexOf("/");
  return slash < 0 ? "" : path.slice(0, slash);
}

export function statusOf(item) {
  return String(item?.presence ?? "unknown").toLowerCase();
}

export function needsOrganizing(item) {
  const confidence = String(item?.confidence ?? "low").toLowerCase();
  const metadata = String(item?.metadata_status ?? "unknown").toLowerCase();
  const hasCollection = Boolean(item?.collection && (typeof item.collection === "string" ? item.collection.trim() : item.collection.title));
  return confidence !== "high" || metadata !== "complete" || !hasCollection || ["missing", "unavailable"].includes(statusOf(item));
}

export function filterLibraryItems(items, { query = "", category = "", status = "" } = {}) {
  const normalizedQuery = String(query).trim().toLocaleLowerCase();
  return (Array.isArray(items) ? items : []).filter((item) => {
    const text = [item?.title, item?.relative_path, item?.id, ...(Array.isArray(item?.source_ids) ? item.source_ids : [])].join(" ").toLocaleLowerCase();
    return (!normalizedQuery || text.includes(normalizedQuery))
      && (!category || itemFolder(item) === category)
      && (!status || statusOf(item) === status);
  }).sort((left, right) => String(left?.title ?? left?.relative_path ?? "").localeCompare(String(right?.title ?? right?.relative_path ?? "")));
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
    if (!groups.has(title)) groups.set(title, []);
    groups.get(title).push(item);
  }
  return [...groups.entries()].map(([title, entries]) => ({ title, items: entries }));
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
