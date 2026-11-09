export type FilePreviewSummaryFile = {
  sourcePath: string;
  targetPath: string;
  bytes: number | string;
};

export type FilePreviewSummaryItem = {
  itemId: string;
  sourcePath: string;
  targetPath: string;
  outcome: "move" | "skip" | "no_op";
  fileCount: number;
  omittedFileCount: number;
  files: FilePreviewSummaryFile[];
};

export type FilePreviewSummaryMetadataPatch = {
  itemId: string;
  nfoPath: string;
  changed: boolean;
};

export type FilePreviewSummary = {
  id: string;
  revision: string;
  libraryId: string;
  rootBinding: string;
  selectedCount: number;
  metadataOnly: boolean;
  targetRelativeDir: string | null;
  rename: boolean;
  conflict: "skip" | "keep_both";
  moveItemCount: number;
  fileCount: number;
  totalBytes: string;
  outcomeCounts: { move: number; skip: number; noOp: number };
  conflictCount: number;
  metadataPatchCount: number;
  changedMetadataPatchCount: number;
  omittedItemCount: number;
  omittedFileCount: number;
  omittedMetadataPatchCount: number;
  sampleItems: FilePreviewSummaryItem[];
  sampleMetadataPatches: FilePreviewSummaryMetadataPatch[];
};

export function normalizeFilePreviewSummary(entry: unknown): FilePreviewSummary | null;
export function renderFilePreviewSummary(summary: FilePreviewSummary | null | undefined): string;
export function filePreviewSummaryConfirmLabel(summary: FilePreviewSummary | null | undefined): string;
