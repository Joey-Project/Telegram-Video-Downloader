export interface MetadataPreviewDetail {
  kind: "status" | "change";
  text?: string;
  label?: string;
  before?: string | null;
  after?: string;
}

export function metadataPreviewDetails(patch: unknown): MetadataPreviewDetail[];
