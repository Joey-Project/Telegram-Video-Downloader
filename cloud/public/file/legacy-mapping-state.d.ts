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
