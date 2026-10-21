const ROOT_BINDING_PATTERN = /^v1:[0-9a-f]{64}$/u;
const MAX_LIBRARY_ID_BYTES = 128;
const UTF8_ENCODER = new TextEncoder();

export function isBoundDownloadLibraryOption(option) {
  if (!option || typeof option !== "object") return false;
  const libraryId = option.value;
  const rootBinding = option.dataset?.rootBinding;
  if (typeof libraryId !== "string" || libraryId.trim() !== libraryId || libraryId.length === 0
      || UTF8_ENCODER.encode(libraryId).byteLength > MAX_LIBRARY_ID_BYTES
      || /[\u0000-\u001f\u007f]/u.test(libraryId)) return false;
  return typeof rootBinding === "string" && ROOT_BINDING_PATTERN.test(rootBinding);
}

export function buildDownloadSubmitPayload(options = {}) {
  const { url, selectedOption, category } = options;
  if (typeof url !== "string" || url.trim().length === 0 || !isBoundDownloadLibraryOption(selectedOption)) return null;
  const payload = {
    url: url.trim(),
    library_id: selectedOption.value,
    root_binding: selectedOption.dataset.rootBinding,
  };
  if (Object.hasOwn(options, "category")) {
    if (typeof category !== "string") return null;
    payload.category = category;
  }
  return payload;
}
