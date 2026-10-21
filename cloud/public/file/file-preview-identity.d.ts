export type FileLibraryOption = {
  value?: unknown;
  dataset?: { rootBinding?: unknown };
} | null | undefined;

export type FileLibraryIdentity = {
  library_id: string;
  root_binding: string;
};

export type FileIdentityScopedState = {
  libraryId?: unknown;
  selected?: Set<string>;
  detailId?: unknown;
  legacyOperation?: unknown;
  legacyOperationKey?: unknown;
  legacySelection?: Map<unknown, unknown>;
  legacyPage?: unknown;
  manualPickerKey?: unknown;
  manualPickerQuery?: unknown;
  libraryPage?: unknown;
  needsPage?: unknown;
} | null | undefined;

export function buildFilePreviewPayloadFromRenderedOption(options: {
  ownerSnapshot: unknown;
  libraryOption: FileLibraryOption;
  expectedIdentity?: FileLibraryIdentity | null;
  fields?: Record<string, unknown>;
}): (Record<string, unknown> & FileLibraryIdentity) | null;

export function buildSelectedFilePreviewPayloadFromRenderedOption(options: {
  state: { selected?: Set<string> } | null | undefined;
  ownerSnapshot: unknown;
  libraryOption: FileLibraryOption;
  fields?: Record<string, unknown>;
}): (Record<string, unknown> & FileLibraryIdentity & { item_ids: string[] }) | null;

export function buildLegacyMetadataPreviewPayloadFromRenderedOption(options: {
  ownerSnapshot: unknown;
  libraryOption: FileLibraryOption;
  importOperation: unknown;
  fields?: Record<string, unknown>;
}): (Record<string, unknown> & FileLibraryIdentity) | null;

export function fileOperationLibraryIdentity(operation: unknown): FileLibraryIdentity | null;
export function fileOperationIdentityMatches(ownerSnapshot: unknown, operation: unknown, expectedLibraryId?: string): boolean;
export function clearFileIdentityScopedStateOnSnapshot(state: FileIdentityScopedState, previousSnapshot: unknown, nextSnapshot: unknown): boolean;
