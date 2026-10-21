export function renderMigrationHistory(input: {
  migrations: unknown;
  libraries?: Array<{ id: string; name?: string }>;
  libraryId: string;
  omittedCount?: unknown;
}): string;
