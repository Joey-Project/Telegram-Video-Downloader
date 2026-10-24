import type { MetadataPatch, MetadataRecoveryReport, OrganizationStatus } from "../../src/types";

export function parseMetadataRecoveryReport(value: unknown): MetadataRecoveryReport | null;
export type MetadataRecoveryGroups = Record<MetadataRecoveryReport["items"][number]["status"], MetadataRecoveryReport["items"]>;
export function groupMetadataRecoveryItems(items: unknown): MetadataRecoveryGroups;
export function buildMetadataRecoveryPreviewPayload(input: {
  report: MetadataRecoveryReport;
  libraryId: string;
  rootBinding: string;
  itemIds: string[];
  candidateChoices: Map<string, number>;
}): {
  library_id: string;
  root_binding: string;
  item_ids: string[];
  metadata_patches: MetadataPatch[];
  expected_revision: string;
} | null;
export function buildOrganizationStatusPatch(
  item: { id?: unknown; collection?: unknown; organization_status?: unknown } | null | undefined,
  organizationStatus: OrganizationStatus,
): MetadataPatch | null;
export function metadataRecoveryOperationId(operation: unknown): string;
