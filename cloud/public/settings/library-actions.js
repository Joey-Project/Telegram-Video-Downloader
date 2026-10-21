const ROOT_BINDING_PATTERN = /^v1:[0-9a-f]{64}$/u;
const ROOT_BOUND_MUTATION_KINDS = new Set([
  "library_set_default",
  "library_set_category",
  "library_rename",
  "library_remove",
]);

export function isValidLibraryRootBinding(value) {
  return typeof value === "string" && ROOT_BINDING_PATTERN.test(value);
}

export function buildLibraryMutationPayload(kind, displayedEntry, changes = {}) {
  if (!ROOT_BOUND_MUTATION_KINDS.has(kind) || !displayedEntry || typeof displayedEntry !== "object") return null;
  const libraryId = displayedEntry.id;
  const rootBinding = displayedEntry.root_binding;
  if (typeof libraryId !== "string" || libraryId.length === 0 || !isValidLibraryRootBinding(rootBinding)) return null;

  const payload = { library_id: libraryId, root_binding: rootBinding };
  if (kind === "library_rename") {
    if (typeof changes.name !== "string") return null;
    payload.name = changes.name;
  } else if (kind === "library_set_category") {
    if (typeof changes.default_category !== "string") return null;
    payload.default_category = changes.default_category;
  }
  return payload;
}
