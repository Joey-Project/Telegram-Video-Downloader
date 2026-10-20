export const LIBRARY_PAGE_SIZE = 50;
export const MAX_FOLDER_SUGGESTIONS = 100;

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
