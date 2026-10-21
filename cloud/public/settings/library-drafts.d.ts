export type LibraryDraftField = "name" | "default_category";

export interface LibraryDraftEntry {
  id?: unknown;
  root?: unknown;
  name?: unknown;
  default_category?: unknown;
  [key: string]: unknown;
}

export interface LibraryDraftRecord {
  root: string;
  fields: Partial<Record<LibraryDraftField, string>>;
}

export interface LibraryEditorForm {
  dataset?: {
    renameForm?: string;
    categoryForm?: string;
    libraryRoot?: string;
  };
  elements?: {
    namedItem(name: string): unknown;
  };
}

export interface LibraryEditorInput {
  disabled?: boolean;
  readOnly?: boolean;
  selectionStart?: number | null;
  selectionEnd?: number | null;
  selectionDirection?: string | null;
  value?: unknown;
  closest(selector: string): LibraryEditorForm | null;
  focus?(options?: { preventScroll?: boolean }): void;
  setSelectionRange?(start: number, end: number, direction?: string): void;
}

export interface LibraryEditorList {
  contains(element: LibraryEditorInput): boolean;
  querySelectorAll?(selector: string): Iterable<LibraryEditorForm>;
}

export interface LibraryEditorFocus {
  id: string;
  root: string;
  field: LibraryDraftField;
  selectionStart: number;
  selectionEnd: number;
  selectionDirection: string;
}

export function setLibraryDraftField(
  drafts: Map<string, LibraryDraftRecord>,
  entry: LibraryDraftEntry | null | undefined,
  field: LibraryDraftField,
  value: unknown,
): boolean;

export function libraryDraftFieldValue(
  drafts: ReadonlyMap<string, LibraryDraftRecord>,
  entry: LibraryDraftEntry | null | undefined,
  field: LibraryDraftField,
): string;

export function reconcileLibraryDrafts(
  drafts: Map<string, LibraryDraftRecord>,
  entries: readonly LibraryDraftEntry[] | null | undefined,
): void;

export function captureLibraryEditorFocus(
  list: LibraryEditorList | null | undefined,
  activeElement: LibraryEditorInput | null | undefined,
  entries: readonly LibraryDraftEntry[] | null | undefined,
): LibraryEditorFocus | null;

export function restoreLibraryEditorFocus(
  list: LibraryEditorList | null | undefined,
  focus: LibraryEditorFocus | null | undefined,
  entries: readonly LibraryDraftEntry[] | null | undefined,
): boolean;
