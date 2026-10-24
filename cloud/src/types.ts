export type RequestKind =
  | "telegram"
  | "library_scan"
  | "library_register"
  | "library_set_default"
  | "library_set_category"
  | "library_rename"
  | "library_remove"
  | "metadata_recover"
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

export type OrganizationStatus = "standalone" | "unreviewed";

export interface MetadataPatch {
  item_id: string;
  hint_index?: number;
  title?: string;
  source_ids?: string[];
  collection?: CollectionMetadata;
  organization_status?: OrganizationStatus;
  fill_missing_only?: boolean;
}

export interface MetadataRecoveryPayload extends BoundLibraryMutationIdentity {
  scope: "selected" | "library";
  item_ids?: string[];
  after_item_id?: string;
}

export type MetadataRecoveryStatus = "ready" | "needs_confirmation" | "unchanged" | "unavailable" | "unsupported";

export interface MetadataRecoveryCandidate {
  label: string;
  patch: MetadataPatch;
}

export interface MetadataRecoveryItem {
  item_id: string;
  title: string;
  relative_path: string;
  status: MetadataRecoveryStatus;
  reason?: string;
  patch?: MetadataPatch;
  candidates?: MetadataRecoveryCandidate[];
}

export interface MetadataRecoveryReport {
  revision: string;
  items: MetadataRecoveryItem[];
  total: number;
  remaining: number;
  next_cursor: string | null;
}

export type ConflictPolicy = "skip" | "keep_both";

export interface LibraryMigrationPreviewPayload {
  source_library_id: string;
  target_library_id: string;
  source_root_binding: string;
  target_root_binding: string;
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

export interface FilePreviewPayload extends BoundLibraryMutationIdentity {
  item_ids?: string[];
  expected_revision?: string;
  target_relative_dir?: string;
  rename?: boolean;
  conflict?: ConflictPolicy;
  metadata_patches?: MetadataPatch[];
}

export interface LegacyImportPayload extends BoundLibraryMutationIdentity {
  text: string;
}

export interface ActionRequest {
  request_id: string;
  kind: Exclude<RequestKind, "telegram">;
  payload: Record<string, unknown>;
  queued_at_ms: number;
  replay_expires_at_ms: number;
}

export interface BoundLibraryMutationIdentity extends Record<string, unknown> {
  library_id: string;
  root_binding: string;
}

export interface DownloadSubmitPayload extends BoundLibraryMutationIdentity {
  url: string;
  category?: string;
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
    migration_discovery_warnings?: Array<{
      source_library_id: string;
      record_name: string;
      kind: "disappeared" | "unreadable" | "oversized" | "malformed" | "source_binding_mismatch" | "source_unavailable" | "enumeration_failed" | "maintenance_failed";
      message: string;
    }>;
    migration_discovery_warning_omitted_count?: number;
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
        root_binding?: string | null;
      }>;
    };
  };
}

export interface PreparedSnapshotContent {
  snapshot: LocalSnapshot;
  libraryJson: string;
  stateJson: string;
}

export interface SnapshotChunkDescriptor {
  sha256: string;
  bytes: number;
}

export interface SnapshotChunkManifest {
  bytes: number;
  chunks: SnapshotChunkDescriptor[];
}

export interface ChunkedSnapshotContent {
  state_version: number;
  reported_at: number;
  library_revision: string;
  library: SnapshotChunkManifest;
  state: SnapshotChunkManifest;
}

export interface ChunkedStoredSnapshot {
  transport: "chunked-v1";
  format: "chunked-v1";
  database_epoch: string;
  synced: true;
  state_version: number;
  library_revision: string;
  reported_at: number;
  last_seen: number;
  stale: boolean;
  library: SnapshotChunkManifest;
  state: SnapshotChunkManifest;
}

export class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly stateVersion?: number,
    readonly databaseEpoch?: string,
    readonly sequenceConflict?: number,
    readonly code?: string,
  ) {
    super(message);
    this.name = "HttpError";
  }
}
