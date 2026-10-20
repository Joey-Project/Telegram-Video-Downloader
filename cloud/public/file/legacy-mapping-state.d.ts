export type LegacyMappingSelection = {
  selected: boolean;
  item_id?: string;
  title: boolean;
  source_ids: boolean;
  collection: boolean;
};

export function getOrCreateLegacySelection<T extends LegacyMappingSelection>(
  selections: Map<string, T>,
  key: string,
  defaults: T,
): T;

export function updateLegacySelection<T extends LegacyMappingSelection>(
  selections: Map<string, T>,
  key: string,
  patch: Partial<T>,
): boolean;

export type LegacyHintSelectionLike = {
  hintIndex?: unknown;
  dataset?: { hintIndex?: unknown } | null;
} | null | undefined;

export function hasDuplicateLegacyHintSelection(
  selections: readonly LegacyHintSelectionLike[] | null | undefined,
): boolean;

export type LegacyCandidateLike = { item_id?: unknown };
export type PresentMediaLike = { id?: unknown; presence?: unknown };

export function filterPresentLegacyCandidates<T extends LegacyCandidateLike>(
  candidates: readonly T[] | null | undefined,
  items: readonly PresentMediaLike[] | null | undefined,
): T[];

export const LEGACY_MAPPING_PAGE_SIZE: number;

export type LegacyImportOperationLike = {
  kind?: unknown;
  status?: unknown;
  details_incomplete?: unknown;
  result_truncated?: unknown;
  error?: unknown;
  result?: {
    hints?: unknown;
    candidates?: unknown;
    details_incomplete?: unknown;
    result_truncated?: unknown;
    operation?: { details_incomplete?: unknown; result_truncated?: unknown } | null;
  } | null;
} | null | undefined;

export function isUsableLegacyImport(operation: LegacyImportOperationLike): boolean;

export function selectLegacyImportForMapping(
  operations: readonly LegacyImportOperationLike[] | null | undefined,
): {
  operation: LegacyImportOperationLike;
  usable: boolean;
  latest: LegacyImportOperationLike;
};

export type LegacyMappingHint = {
  title?: unknown;
  source_ids?: unknown;
  collection?: unknown;
  [key: string]: unknown;
};

export type LegacyMappingCandidate = LegacyCandidateLike & {
  hint_index?: unknown;
  title?: unknown;
  relative_path?: unknown;
  match_kind?: unknown;
  ambiguous?: unknown;
  requires_confirmation?: unknown;
  error?: unknown;
  error_message?: unknown;
};

export type LegacyMappingChoice = {
  key: string;
  kind: "candidate" | "manual";
  hintIndex: number;
  hint: LegacyMappingHint;
  candidate?: LegacyMappingCandidate;
  hasCandidate?: boolean;
};

export function buildLegacyMappingChoices(
  operationKey: string | number,
  hints: readonly LegacyMappingHint[] | null | undefined,
  candidates: readonly LegacyMappingCandidate[] | null | undefined,
  items: readonly PresentMediaLike[] | null | undefined,
): LegacyMappingChoice[];

export function paginateLegacyMappingChoices(
  choices: readonly LegacyMappingChoice[] | null | undefined,
  page: number,
  pageSize?: number,
): {
  choices: LegacyMappingChoice[];
  page: number;
  pageCount: number;
  pageSize: number;
  start: number;
  end: number;
  total: number;
};

export function selectLegacyMappingChoice(
  selections: Map<string, LegacyMappingSelection>,
  key: string,
  hintIndex: number,
  selected: boolean,
  limit?: number,
): { ok: boolean; reason?: "missing" | "limit"; replaced?: boolean };

export function buildLegacyMetadataPatches(args: {
  hints: readonly LegacyMappingHint[] | null | undefined;
  choices: readonly LegacyMappingChoice[] | null | undefined;
  selections: Map<string, LegacyMappingSelection & { hintIndex?: number }>;
  items: readonly PresentMediaLike[] | null | undefined;
  limit?: number;
}): {
  patches: Array<Record<string, unknown>>;
  error?: "empty-selection" | "selection-limit" | "duplicate-hint" | "manual-target-required" | "target-not-present" | "duplicate-target" | "invalid-hint" | "empty-fields" | null;
};
