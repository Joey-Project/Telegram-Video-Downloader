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
  },
): string;
