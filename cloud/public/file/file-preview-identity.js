import { registeredMediaLibrary } from "./library-view.js";

const ROOT_BINDING_PATTERN = /^v1:[0-9a-f]{64}$/u;
const MAX_LIBRARY_ID_BYTES = 128;
const UTF8_ENCODER = new TextEncoder();

function validLibraryId(value) {
  return typeof value === "string" && value.length > 0 && value.trim() === value
    && UTF8_ENCODER.encode(value).byteLength <= MAX_LIBRARY_ID_BYTES
    && !/[\u0000-\u001f\u007f]/u.test(value);
}

function validRootBinding(value) {
  return typeof value === "string" && ROOT_BINDING_PATTERN.test(value);
}

export function buildFilePreviewPayloadFromRenderedOption({
  ownerSnapshot,
  libraryOption,
  expectedIdentity,
  fields = {},
} = {}) {
  const libraryId = libraryOption?.value;
  const rootBinding = libraryOption?.dataset?.rootBinding;
  if (!validLibraryId(libraryId) || !validRootBinding(rootBinding)
      || !fields || typeof fields !== "object" || Array.isArray(fields)) return null;

  const descriptor = registeredMediaLibrary(ownerSnapshot, libraryId);
  if (!descriptor || descriptor.root_binding !== rootBinding) return null;
  if (expectedIdentity && (expectedIdentity.library_id !== libraryId || expectedIdentity.root_binding !== rootBinding)) return null;

  return { ...fields, library_id: libraryId, root_binding: rootBinding };
}

export function buildSelectedFilePreviewPayloadFromRenderedOption({
  state,
  ownerSnapshot,
  libraryOption,
  fields = {},
} = {}) {
  if (!(state?.selected instanceof Set) || state.selected.size === 0) return null;
  return buildFilePreviewPayloadFromRenderedOption({
    ownerSnapshot,
    libraryOption,
    fields: { ...fields, item_ids: [...state.selected] },
  });
}

export function buildLegacyMetadataPreviewPayloadFromRenderedOption({
  ownerSnapshot,
  libraryOption,
  importOperation,
  fields = {},
} = {}) {
  const identity = fileOperationLibraryIdentity(importOperation);
  if (!identity || !fileOperationIdentityMatches(ownerSnapshot, importOperation, identity.library_id)) return null;
  return buildFilePreviewPayloadFromRenderedOption({ ownerSnapshot, libraryOption, expectedIdentity: identity, fields });
}

export function fileOperationLibraryIdentity(operation) {
  if (!operation || typeof operation !== "object") return null;
  const result = operation.result && typeof operation.result === "object" ? operation.result : null;
  const nestedPreview = result?.preview && typeof result.preview === "object"
    ? result.preview
    : operation.preview && typeof operation.preview === "object" ? operation.preview : null;
  const sources = [operation, operation.recovery_identity, result, nestedPreview]
    .filter((source) => source && typeof source === "object");
  const values = { library_id: [], root_binding: [] };
  for (const source of sources) {
    for (const field of Object.keys(values)) {
      if (!Object.hasOwn(source, field)) continue;
      const value = source[field];
      const valid = field === "library_id" ? validLibraryId(value) : validRootBinding(value);
      if (!valid) return null;
      values[field].push(value);
    }
  }
  if (values.library_id.length === 0 || values.root_binding.length === 0) return null;
  if (new Set(values.library_id).size !== 1 || new Set(values.root_binding).size !== 1) return null;
  return { library_id: values.library_id[0], root_binding: values.root_binding[0] };
}

export function fileOperationIdentityMatches(ownerSnapshot, operation, expectedLibraryId = "") {
  const identity = fileOperationLibraryIdentity(operation);
  if (!identity || expectedLibraryId && identity.library_id !== expectedLibraryId) return false;
  return registeredMediaLibrary(ownerSnapshot, identity.library_id)?.root_binding === identity.root_binding;
}

export function clearFileIdentityScopedStateOnSnapshot(state, previousSnapshot, nextSnapshot) {
  if (!state || !previousSnapshot || typeof state.libraryId !== "string" || !state.libraryId) return false;
  const previous = registeredMediaLibrary(previousSnapshot, state.libraryId);
  if (!previous) return false;
  const current = registeredMediaLibrary(nextSnapshot, state.libraryId);
  const previousBinding = previous.root_binding;
  const currentBinding = current?.root_binding;
  if (validRootBinding(previousBinding) && validRootBinding(currentBinding) && previousBinding === currentBinding) return false;

  state.selected?.clear();
  state.detailId = null;
  state.legacyOperation = null;
  state.legacyOperationKey = null;
  state.legacySelection?.clear();
  state.legacyPage = 0;
  state.manualPickerKey = null;
  state.manualPickerQuery = "";
  state.libraryPage = 0;
  state.needsPage = 0;
  return true;
}
