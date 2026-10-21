export const MIGRATION_PREVIEW_PAGE_SIZE: 30;
export function migrationPreviewDraftKey(id: string, revision: string): string | null;
export function migrationPreviewEntries(snapshot: unknown, selectedLibraryId: string): Array<Record<string, unknown>>;
export function createMigrationRefreshQueuedCallback(options: {
  operation: unknown;
  previewId: string;
  revision: string;
  draft?: Record<string, unknown> | null;
  payload: Record<string, unknown>;
  saveDraft: (previewId: string, revision: string, draft: Record<string, unknown>) => string | null;
  markInvalidated: (key: string) => void;
  render: () => void;
}): (action: { request_id: string }) => void;
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
