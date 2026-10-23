export const LIBRARY_PAGE_SIZE: number;
export const MAX_FOLDER_SUGGESTIONS: number;

export type MediaLibraryDescriptor = Record<string, unknown> & { id: string };
export type MediaLibrarySnapshot = {
  descriptor: MediaLibraryDescriptor | null;
  snapshot: (Record<string, unknown> & { items?: unknown[]; categories?: unknown[] }) | null;
  id: string;
};

export type LibraryViewItem = {
  id?: unknown;
  title?: unknown;
  relative_path?: unknown;
  source_ids?: unknown;
  presence?: unknown;
  confidence?: unknown;
  metadata_status?: unknown;
  collection?: unknown;
  moved_to?: unknown;
};
export type MovedToTarget = {
  library_id: string;
  library_name: string;
  item_id: string;
  relative_path: string;
};

export function itemFolder(item: LibraryViewItem): string;
export function statusOf(item: LibraryViewItem): string;
export function movedToTarget(item: LibraryViewItem): MovedToTarget | null;
export function librarySelectionCounts<T extends LibraryViewItem>(
  items: readonly T[] | null | undefined,
  selectedIds: Iterable<string> | null | undefined,
  canOperate: boolean,
): {
  totalCount: number;
  selectedCount: number;
  selectableCount: number;
  skippedUnavailableCount: number;
  readOnlyCount: number;
};
export function needsOrganizing(item: LibraryViewItem): boolean;
export function mediaLibraryEntries(ownerSnapshot: unknown): MediaLibraryDescriptor[];
export function defaultMediaLibraryId(ownerSnapshot: unknown): string;
export function resolveMediaLibrary(ownerSnapshot: unknown, requestedId?: string): MediaLibrarySnapshot;
export function mediaLibraryAvailability(descriptor: MediaLibraryDescriptor | null | undefined): "available" | "offline" | "unknown";
export function registeredMediaLibrary(ownerSnapshot: unknown, libraryId: string): MediaLibraryDescriptor | null;
export function canOperateOnRegisteredMediaLibrary(ownerSnapshot: unknown, libraryId: string): boolean;
export function isValidLibraryRootBinding(value: unknown): value is string;
export function migrationEndpointAvailability(ownerSnapshot: unknown, sourceLibraryId: string, targetLibraryId: string): {
  sourceRegistered: boolean;
  targetRegistered: boolean;
  sourceAvailable: boolean;
  targetAvailable: boolean;
};
export function migrationPreviewBindingsMatch(
  ownerSnapshot: unknown,
  sourceLibraryId: string,
  targetLibraryId: string,
  sourceRootBinding: unknown,
  targetRootBinding: unknown,
): boolean;
export function selectMigrationTarget(
  entries: readonly MediaLibraryDescriptor[] | null | undefined,
  sourceLibraryId: string,
  currentTargetId: string,
  canOperate?: (libraryId: string) => boolean,
): {
  targets: MediaLibraryDescriptor[];
  availableTargets: MediaLibraryDescriptor[];
  selectedId: string;
};
export function renderMigrationTargetOptions(
  selection: ReturnType<typeof selectMigrationTarget>,
  escapeHtml: (value: unknown) => string,
): string;
export function buildMigrationPreviewPayload(options: {
  sourceLibraryId: string;
  targetLibraryId: string;
  sourceRootBinding: string;
  targetRootBinding: string;
  scope: "library" | "selected";
  itemIds?: readonly string[];
  includeIncompleteTasks?: boolean;
  conflict?: "skip" | "keep_both";
  setTargetDefaultOnSuccess?: boolean;
}): Record<string, unknown> | null;
export function buildMigrationPreviewPayloadFromRenderedOptions(options: {
  sourceOption: { value?: unknown; dataset?: { rootBinding?: unknown } } | null | undefined;
  targetOption: { value?: unknown; dataset?: { rootBinding?: unknown } } | null | undefined;
  scope: "library" | "selected";
  itemIds?: readonly string[];
  includeIncompleteTasks?: boolean;
  conflict?: "skip" | "keep_both";
  setTargetDefaultOnSuccess?: boolean;
}): Record<string, unknown> | null;
export function canOperateOnMediaLibrary(descriptor: MediaLibraryDescriptor | null | undefined): boolean;
export function mediaSnapshotInfo(descriptor: MediaLibraryDescriptor | null | undefined, snapshot: unknown): {
  totalCount: number;
  retainedCount: number;
  omittedCount: number;
};
export function mediaSnapshotOmissionNotice(descriptor: MediaLibraryDescriptor | null | undefined, snapshot: unknown): string;
export function operationLibraryId(operation: Record<string, unknown> | null | undefined): string;
export function operationMatchesLibrary(operation: Record<string, unknown> | null | undefined, libraryId: string): boolean;
export function collectionIdentity(collection: unknown): string;
export function filterLibraryItems<T extends LibraryViewItem>(
  items: readonly T[] | null | undefined,
  filters?: { query?: string; category?: string; status?: string },
): T[];
export function paginateLibraryItems<T>(
  items: readonly T[] | null | undefined,
  page: number,
  pageSize?: number,
): {
  items: T[];
  page: number;
  pageCount: number;
  pageSize: number;
  start: number;
  end: number;
  total: number;
};
export function groupLibraryItems<T extends LibraryViewItem>(items: readonly T[] | null | undefined): Array<{
  title: string;
  items: T[];
}>;
export function boundedFolderSuggestions(
  folders: readonly string[] | null | undefined,
  query?: string,
  limit?: number,
): string[];
