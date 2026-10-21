export const MIGRATION_PREVIEW_PAGE_SIZE: 30;
export function migrationPreviewDraftKey(id: string, revision: string): string | null;
export function migrationPreviewPageWindow(
  itemCount: number,
  requestedPage: number | undefined,
): { page: number; pageCount: number; start: number; end: number };
export function renderMigrationPreview(
  operation: unknown,
  options?: {
    activeLibraryId?: string;
    sourceAvailable?: boolean;
    targetAvailable?: boolean;
    invalidated?: boolean;
    draftChanged?: boolean;
    refreshPending?: boolean;
    draft?: unknown;
    page?: number;
  },
): string;
