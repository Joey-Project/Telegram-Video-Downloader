export const LIBRARY_PAGE_SIZE: number;
export const MAX_FOLDER_SUGGESTIONS: number;

export type LibraryViewItem = {
  id?: unknown;
  title?: unknown;
  relative_path?: unknown;
  source_ids?: unknown;
  presence?: unknown;
  confidence?: unknown;
  metadata_status?: unknown;
  collection?: unknown;
};

export function itemFolder(item: LibraryViewItem): string;
export function statusOf(item: LibraryViewItem): string;
export function needsOrganizing(item: LibraryViewItem): boolean;
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
