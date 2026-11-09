export type FileConfirmSummaryItem = {
  itemId: string;
  outcome: "move" | "skip" | "no_op";
  movedFiles: number;
  totalFiles: number;
  metadataPatched: boolean;
  error: string | null;
};

type FileConfirmSummaryBase = {
  id: string;
  revision: string;
  status: string;
  summaryMessage: string;
};

export type FileConfirmSummary = FileConfirmSummaryBase & (
  | { detailsAvailable: false; retryableAccessError: null }
  | {
    detailsAvailable: true;
    retryableAccessError: boolean;
    selectedCount: number;
    outcomeCounts: { move: number; skip: number; noOp: number };
    fileCount: number;
    movedFileCount: number;
    metadataPatchedItemCount: number;
    errorCount: number;
    warningCount: number;
    omittedItemCount: number;
    omittedWarningCount: number;
    sampleItems: FileConfirmSummaryItem[];
    sampleWarnings: string[];
  }
);

export function normalizeFileConfirmSummary(entry: unknown): FileConfirmSummary | null;
export function renderFileConfirmSummary(summary: FileConfirmSummary | null | undefined): string;
