export const MIGRATION_PREVIEW_PAGE_SIZE: 30;
export function migrationPreviewDraftKey(id: string, revision: string): string | null;
export function migrationPreviewEntries(snapshot: unknown, selectedLibraryId?: string): Array<Record<string, unknown>>;
export type MigrationRefreshHistoryKind = "unaccepted" | "recorded" | "pending" | "outside_recent_history" | "unknown";
export function inspectMigrationRefreshHistory(
  refresh: { seq?: unknown } | null | undefined,
  snapshot: { synced?: unknown; stale?: unknown; last_seen?: unknown; operations?: unknown; previews?: unknown; pending_requests?: unknown } | null | undefined,
  now?: number,
): { kind: MigrationRefreshHistoryKind; operation?: unknown; pending?: unknown };
export function releaseMigrationRefreshTracking(
  draft: Record<string, unknown> | null | undefined,
  snapshot: unknown,
  now?: number,
): boolean;
export function reconcileMigrationRefreshDrafts(
  drafts: Iterable<[string, Record<string, unknown>]>,
  snapshot: unknown,
  now?: number,
  invalidated?: Set<string> | null,
): void;
export function createMigrationRefreshQueuedCallback(options: {
  operation: unknown;
  previewId: string;
  revision: string;
  draft?: Record<string, unknown> | null;
  payload: Record<string, unknown>;
  saveDraft: (previewId: string, revision: string, draft: Record<string, unknown>) => string | null;
  markInvalidated: (key: string) => void;
  render: () => void;
}): (action: { request_id: string; queued_at_ms?: number; replay_expires_at_ms?: number }) => void;
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
    rootBindingsMatch?: boolean;
    invalidated?: boolean;
    draftChanged?: boolean;
    refreshPending?: boolean;
    cancelPending?: boolean;
    cancelMarkup?: string;
    draft?: unknown;
    page?: number;
  },
): string;
