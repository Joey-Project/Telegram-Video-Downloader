export type RequestKind =
  | "telegram"
  | "library_scan"
  | "file_preview"
  | "file_confirm"
  | "legacy_import";

export interface InboxEnvelope {
  seq: number;
  kind: RequestKind;
  payload: Record<string, unknown>;
  created_at: number;
}

export interface CollectionMetadata {
  id: string | null;
  title: string;
  kind: string;
  order: number | null;
  part_id: string | null;
}

export interface MetadataPatch {
  item_id: string;
  hint_index?: number;
  title?: string;
  source_ids?: string[];
  collection?: CollectionMetadata;
}

export interface FilePreviewPayload {
  item_ids?: string[];
  target_relative_dir?: string;
  rename?: boolean;
  conflict?: "skip" | "keep_both";
  metadata_patches?: MetadataPatch[];
}

export interface ActionRequest {
  request_id: string;
  kind: Exclude<RequestKind, "telegram">;
  payload: Record<string, unknown>;
}

export interface LocalSnapshot {
  revision?: string;
  state_version: number;
  reported_at: number;
  library: Record<string, unknown> & {
    revision: string;
    scanned_at: number;
    items: unknown[];
    categories: string[];
    warnings: string[];
  };
  previews: unknown[];
  operations: unknown[];
  tasks: unknown[];
  settings: { download_dir: string };
}

export class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "HttpError";
  }
}
