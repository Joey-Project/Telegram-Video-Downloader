export type MigrationConflictPolicy = "skip" | "keep_both";

export const MAX_MIGRATION_ITEM_CONFLICTS: number;
export function normalizeMigrationItemConflicts(value: unknown): Record<string, MigrationConflictPolicy>;
export function migrationConflictPolicy(
  result: { conflict?: MigrationConflictPolicy } | null | undefined,
  itemId: string,
  overrides?: unknown,
): MigrationConflictPolicy;
export function setMigrationConflictChoice(
  current: unknown,
  itemId: string,
  policy: MigrationConflictPolicy,
  defaultPolicy?: MigrationConflictPolicy,
): Record<string, MigrationConflictPolicy>;
export function migrationConflictChoicesChanged(original: unknown, draft: unknown): boolean;
export function buildMigrationPreviewRefreshPayload(result: unknown, itemConflicts: unknown): Record<string, unknown> | null;
export function migrationRefreshMatches(result: unknown, expected: unknown): boolean;
