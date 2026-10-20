export type RequestKind =
  | "telegram"
  | "library_scan"
  | "library_register"
  | "library_set_default"
  | "library_set_category"
  | "library_rename"
  | "library_remove"
  | "file_preview"
  | "file_confirm"
  | "legacy_import"
  | "download_submit"
  | "library_migration_preview"
  | "library_migration_confirm"
  | "library_migration_control";

export interface InboxEnvelope {
  seq: number;
  kind: RequestKind;
  payload: Record<string, unknown>;
  created_at: number;
  request_id: string;
  payload_hash: string;
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

export type ConflictPolicy = "skip" | "keep_both";

export interface LibraryMigrationPreviewPayload {
  source_library_id: string;
  target_library_id: string;
  scope: "library" | "selected";
  item_ids?: string[];
  include_incomplete_tasks?: boolean;
  conflict?: ConflictPolicy;
  item_conflicts?: Record<string, ConflictPolicy>;
  set_target_default_on_success?: boolean;
  supersedes_preview_seq?: number;
  supersedes_preview_id?: string;
  supersedes_revision?: string;
}

export interface FilePreviewPayload {
  library_id?: string;
  item_ids?: string[];
  target_relative_dir?: string;
  rename?: boolean;
  conflict?: ConflictPolicy;
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
  settings: {
    download_dir: string;
    applied?: {
      revision: string;
      default_library_id: string;
      default_category: string;
      libraries: Array<{
        id: string;
        name: string;
        root: string;
        default_category: string;
        is_default: boolean;
      }>;
    };
  };
}

export class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly stateVersion?: number,
    readonly databaseEpoch?: string,
    readonly sequenceConflict?: number,
  ) {
    super(message);
    this.name = "HttpError";
  }
}
