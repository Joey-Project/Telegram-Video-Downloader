const DRAFT_FIELDS = new Set(["name", "default_category"]);
const LIBRARY_EDITOR_FORMS = "form[data-rename-form], form[data-category-form]";
const ROOT_BINDING_PATTERN = /^v1:[0-9a-f]{64}$/u;

function libraryKey(entry) {
  if (!entry || typeof entry !== "object") return null;
  if (typeof entry.id !== "string" || !entry.id || typeof entry.root !== "string" || !entry.root) return null;
  if (typeof entry.root_binding !== "string" || !ROOT_BINDING_PATTERN.test(entry.root_binding)) return null;
  return { id: entry.id, root: entry.root, rootBinding: entry.root_binding };
}

function sameLibraryBinding(left, right) {
  return left?.root === right?.root && left?.rootBinding === right?.rootBinding;
}

function appliedValue(entry, field) {
  return typeof entry?.[field] === "string" ? entry[field] : "";
}

function hasDraftFields(draft) {
  return [...DRAFT_FIELDS].some((field) => Object.hasOwn(draft?.fields ?? {}, field));
}

export function setLibraryDraftField(drafts, entry, field, value) {
  if (!(drafts instanceof Map) || !DRAFT_FIELDS.has(field)) return false;
  const key = libraryKey(entry);
  if (!key) return false;

  let draft = drafts.get(key.id);
  if (draft && !sameLibraryBinding(draft, key)) {
    drafts.delete(key.id);
    draft = null;
  }

  const text = String(value ?? "");
  if (text === appliedValue(entry, field)) {
    if (draft) {
      delete draft.fields[field];
      if (!hasDraftFields(draft)) drafts.delete(key.id);
    }
    return true;
  }

  if (!draft) {
    draft = { root: key.root, rootBinding: key.rootBinding, fields: Object.create(null) };
    drafts.set(key.id, draft);
  }
  draft.fields[field] = text;
  return true;
}

export function libraryDraftFieldValue(drafts, entry, field) {
  const key = libraryKey(entry);
  if (!key || !DRAFT_FIELDS.has(field)) return appliedValue(entry, field);
  const draft = drafts instanceof Map ? drafts.get(key.id) : null;
  if (sameLibraryBinding(draft, key) && Object.hasOwn(draft.fields ?? {}, field)) return draft.fields[field];
  return appliedValue(entry, field);
}

export function reconcileLibraryDrafts(drafts, entries) {
  if (!(drafts instanceof Map)) return;
  const current = new Map();
  for (const entry of Array.isArray(entries) ? entries : []) {
    const key = libraryKey(entry);
    if (key) current.set(key.id, { ...key, entry });
  }

  for (const [id, draft] of drafts) {
    const live = current.get(id);
    if (!live || !sameLibraryBinding(draft, live)) {
      drafts.delete(id);
      continue;
    }
    for (const field of DRAFT_FIELDS) {
      if (Object.hasOwn(draft.fields ?? {}, field) && draft.fields[field] === appliedValue(live.entry, field)) {
        delete draft.fields[field];
      }
    }
    if (!hasDraftFields(draft)) drafts.delete(id);
  }
}

export function captureLibraryEditorFocus(list, activeElement, entries) {
  if (!list?.contains?.(activeElement) || activeElement?.disabled || activeElement?.readOnly) return null;
  const form = activeElement.closest?.(LIBRARY_EDITOR_FORMS);
  if (!form) return null;
  const id = form.dataset?.renameForm ?? form.dataset?.categoryForm;
  const field = form.dataset?.renameForm ? "name" : "default_category";
  const entry = (Array.isArray(entries) ? entries : []).find((candidate) => candidate?.id === id);
  const key = libraryKey(entry);
  if (!key || form.dataset?.libraryRoot !== key.root
    || form.dataset?.libraryRootBinding !== key.rootBinding
    || !Number.isSafeInteger(activeElement.selectionStart) || !Number.isSafeInteger(activeElement.selectionEnd)) return null;
  return {
    id: key.id,
    root: key.root,
    rootBinding: key.rootBinding,
    field,
    selectionStart: activeElement.selectionStart,
    selectionEnd: activeElement.selectionEnd,
    selectionDirection: activeElement.selectionDirection ?? "none",
  };
}

export function restoreLibraryEditorFocus(list, focus, entries) {
  if (!focus || !list?.querySelectorAll) return false;
  const entry = (Array.isArray(entries) ? entries : []).find((candidate) => candidate?.id === focus.id);
  const key = libraryKey(entry);
  if (!key || !sameLibraryBinding(key, focus) || !DRAFT_FIELDS.has(focus.field)) return false;
  const form = [...list.querySelectorAll(LIBRARY_EDITOR_FORMS)].find((candidate) =>
    (candidate.dataset?.renameForm ?? candidate.dataset?.categoryForm) === focus.id
      && (candidate.dataset?.renameForm ? "name" : "default_category") === focus.field
      && candidate.dataset?.libraryRoot === focus.root
      && candidate.dataset?.libraryRootBinding === focus.rootBinding
  );
  const input = form?.elements?.namedItem?.(focus.field);
  if (!input || input.disabled || input.readOnly || typeof input.focus !== "function") return false;
  input.focus({ preventScroll: true });
  if (typeof input.setSelectionRange === "function") {
    const length = String(input.value ?? "").length;
    const start = Math.max(0, Math.min(length, focus.selectionStart));
    const end = Math.max(start, Math.min(length, focus.selectionEnd));
    input.setSelectionRange(start, end, focus.selectionDirection);
  }
  return true;
}
