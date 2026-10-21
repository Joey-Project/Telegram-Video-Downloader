export const MAX_MIGRATION_TASK_ROWS: number;

export interface MigrationTaskSummaryDescriptor {
  id?: string;
  incomplete_task_count?: unknown;
  staging_bytes?: unknown;
}

export interface MigrationTaskSummaryTask {
  kind?: unknown;
  library_id?: unknown;
  status?: unknown;
  staging_bytes?: unknown;
  partial_bytes?: unknown;
  downloaded_bytes?: unknown;
  destination?: { library_id?: unknown } | null;
  target?: { library_id?: unknown } | null;
}

export function incompleteMigrationTaskSummary(
  descriptor: MigrationTaskSummaryDescriptor | null | undefined,
  tasks: readonly MigrationTaskSummaryTask[] | null | undefined,
  libraryId?: string,
): { count: number; bytes: number | null; countIsLowerBound: boolean };
