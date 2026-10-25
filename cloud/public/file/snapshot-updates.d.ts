export type SnapshotUpdatesStatus = "connecting" | "live" | "fallback" | "offline" | "paused" | "unavailable" | "expired";
export type SnapshotRefreshReason = "open" | "manual" | "reference";
export type SnapshotRefreshScope = "library" | "settings" | "operations" | "download";
export type SnapshotRefreshOutcome = "published" | "failed" | "unavailable";

export interface PublishedSnapshotReadOptions {
  quiet: true;
  /** Minimum state version required after a correlated Mac publication. */
  minimumVersion?: number;
  /** Aborted when the bounded post-publication cache read expires or this controller stops. */
  signal: AbortSignal;
  /** The owner snapshot is being reverified after the Cloud database epoch changed. */
  epochChanged?: boolean;
  /** Expected database epoch for this owner-verified cache read. */
  expectedDatabaseEpoch?: string;
}

export interface SnapshotRefreshRequest {
  reason: SnapshotRefreshReason;
  scope: SnapshotRefreshScope;
  libraryId?: string;
}

export interface SnapshotRefreshResult {
  status: SnapshotRefreshOutcome;
  request_id?: string;
  database_epoch?: string;
  state_version?: number;
  reported_at?: number;
  error_code?: string;
  cache_read_failed?: boolean;
}

export interface SnapshotUpdatesEnvironment {
  window?: Window & typeof globalThis;
  document?: Document;
  navigator?: Navigator;
  location?: Location;
  WebSocket?: typeof WebSocket;
  AbortController?: typeof AbortController;
  telegram?: {
    onEvent?: (event: "activated" | "deactivated", handler: () => void) => void;
    offEvent?: (event: "activated" | "deactivated", handler: () => void) => void;
  };
  now?: () => number;
  random?: () => number;
  randomUUID?: () => string;
  setTimeout?: typeof globalThis.setTimeout;
  clearTimeout?: typeof globalThis.clearTimeout;
}

export interface SnapshotUpdatesOptions<TSnapshot = unknown> {
  api(path: string, options: { method: "POST"; signal?: AbortSignal }): Promise<unknown>;
  readSnapshot(options: PublishedSnapshotReadOptions): Promise<TSnapshot> | TSnapshot;
  surface: "file" | "settings";
  onAuthorizationExpired?: () => void;
  onReady?: (event: { databaseEpoch: string; snapshotControl: "v1" | null }) => void;
  onEpochChanged?: (event: { databaseEpoch: string }) => void;
  onStatus?: (status: SnapshotUpdatesStatus) => void;
  environment?: SnapshotUpdatesEnvironment;
}

export interface SnapshotUpdatesController<TSnapshot = unknown> {
  start(): void;
  stop(): void;
  /** Set whether the visible Mini App is actively waiting for operation results. */
  setWatch(watch: boolean): void;
  /** Request a correlated publication from the Mac and then read its published snapshot. */
  requestMacRefresh(request: SnapshotRefreshRequest): Promise<SnapshotRefreshResult & { snapshot?: TSnapshot }>;
}

export function createSnapshotUpdates<TSnapshot = unknown>(options: SnapshotUpdatesOptions<TSnapshot>): SnapshotUpdatesController<TSnapshot>;
