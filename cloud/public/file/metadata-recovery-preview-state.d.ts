export const RECOVERY_PREVIEW_STORAGE_KEY: "file-manager.metadata-recovery-preview.v1";

export type RecoveryPreviewRequestStatus =
  | "queued"
  | "submitted"
  | "rejected"
  | "unknown"
  | "failed"
  | "stale"
  | "cancelled"
  | "expired";

export interface RecoveryPreviewRequest {
  requestId: string;
  seq: number | null;
  libraryId: string;
  rootBinding: string;
  status: RecoveryPreviewRequestStatus;
  error: string;
}

export interface RecoveryPreviewStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export function readRecoveryPreviewRequest(
  storage: Pick<RecoveryPreviewStorage, "getItem">,
): Readonly<RecoveryPreviewRequest> | null;

export function writeRecoveryPreviewRequest(
  storage: Pick<RecoveryPreviewStorage, "setItem" | "removeItem">,
  trackerOrNull: RecoveryPreviewRequest | null,
): boolean;
