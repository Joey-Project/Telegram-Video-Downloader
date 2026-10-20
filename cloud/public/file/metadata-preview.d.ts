export interface MetadataPreviewDetail {
  kind: "status" | "change";
  text?: string;
  label?: string;
  before?: string | null;
  after?: string;
}

export function metadataPreviewDetails(patch: unknown): MetadataPreviewDetail[];
export function hasMetadataPatchFields(patch: unknown): boolean;
export function hasMetadataPreviewChanges(preview: unknown): boolean;
