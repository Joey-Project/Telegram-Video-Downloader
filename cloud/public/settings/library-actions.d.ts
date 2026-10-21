export type LibraryMutationKind =
  | "library_set_default"
  | "library_set_category"
  | "library_rename"
  | "library_remove";

export interface DisplayedLibraryIdentity {
  id?: unknown;
  root_binding?: unknown;
}

export interface LibraryMutationChanges {
  name?: unknown;
  default_category?: unknown;
}

export interface BoundLibraryMutationPayload {
  library_id: string;
  root_binding: string;
  name?: string;
  default_category?: string;
}

export function isValidLibraryRootBinding(value: unknown): value is string;

export function buildLibraryMutationPayload(
  kind: string,
  displayedEntry: DisplayedLibraryIdentity | null | undefined,
  changes?: LibraryMutationChanges,
): BoundLibraryMutationPayload | null;
