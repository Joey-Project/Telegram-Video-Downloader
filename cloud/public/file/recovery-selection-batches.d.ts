export const MAX_METADATA_RECOVERY_BATCH_ITEMS: 100;

export type RecoverySelectionQueue = Readonly<{
  libraryId: string;
  rootBinding: string;
  itemIds: readonly string[];
  submittedCount: number;
}>;

export type RecoverySelectionQueueContext = {
  libraryId: string;
  rootBinding: string;
  selectedIds: readonly string[];
};

export type RecoverySelectionQueueStatus = Readonly<{
  totalCount: number;
  submittedCount: number;
  remainingCount: number;
  currentCount: number;
  hasCurrentBatch: boolean;
}>;

export function createRecoverySelectionQueue(input: {
  libraryId: string;
  rootBinding: string;
  itemIds: readonly string[];
}): RecoverySelectionQueue | null;
export function getCurrentRecoverySelectionBatch(queue: RecoverySelectionQueue | null | undefined): readonly string[] | null;
export function getRecoverySelectionQueueStatus(queue: RecoverySelectionQueue | null | undefined): RecoverySelectionQueueStatus | null;
export function matchesRecoverySelectionQueue(
  queue: RecoverySelectionQueue | null | undefined,
  context: RecoverySelectionQueueContext,
): boolean;
export function advanceRecoverySelectionQueue(
  queue: RecoverySelectionQueue | null | undefined,
  input: RecoverySelectionQueueContext & { outcome: string },
): RecoverySelectionQueue | null;
