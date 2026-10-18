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

export type LegacyCandidateLike = { item_id?: unknown };
export type PresentMediaLike = { id?: unknown; presence?: unknown };

export function filterPresentLegacyCandidates<T extends LegacyCandidateLike>(
  candidates: readonly T[] | null | undefined,
  items: readonly PresentMediaLike[] | null | undefined,
): T[];
