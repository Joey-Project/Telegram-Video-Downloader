export const PREVIEW_CANCELLATION_STORAGE_KEY: "file-manager.preview-cancellations.v1";
export const PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY: "file-manager.preview-cancellation-dismissals.v1";

export type PreviewCancellationKind = "file_preview" | "library_migration_preview";
export type PreviewCancellationStatus = "queued" | "submitted" | "unknown" | "failed";

export interface PreviewCancellationTracker {
  requestId: string | null;
  seq: number | null;
  status: PreviewCancellationStatus;
  error: string | null;
}

export interface PreviewCancellationStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export function readPreviewCancellations(
  storage: Pick<PreviewCancellationStorage, "getItem">,
): Map<string, PreviewCancellationTracker>;

export function writePreviewCancellations(
  storage: Pick<PreviewCancellationStorage, "setItem" | "removeItem">,
  cancellations: Map<string, PreviewCancellationTracker>,
): boolean;

export function readPreviewCancellationDismissals(
  storage: Pick<PreviewCancellationStorage, "getItem">,
): Map<string, Set<string>> | null;

export function writePreviewCancellationDismissals(
  storage: Pick<PreviewCancellationStorage, "setItem" | "removeItem">,
  dismissals: Map<string, Set<string>>,
): boolean;
