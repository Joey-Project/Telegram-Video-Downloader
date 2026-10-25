export type SnapshotRequestHeaders = Readonly<Record<string, string>>;

export type SnapshotJsonFetcher = (
  path: string,
  headers?: SnapshotRequestHeaders,
) => Promise<unknown>;

export interface OwnerSnapshot {
  /** Present for verified chunked-v1 snapshots; omitted by legacy full responses. */
  database_epoch?: string;
  state_version?: number;
  reported_at?: number;
  last_seen?: number;
  synced?: boolean;
  stale?: boolean;
  [key: string]: unknown;
}

export interface OwnerSnapshotLoaderOptions {
  /** Aborting abandons this loader generation so late cache work cannot block or affect a later read. */
  signal?: AbortSignal;
}

/** Request chunked-v1 when loading an owner snapshot, while accepting legacy full responses. */
export function loadOwnerSnapshot(fetchJson: SnapshotJsonFetcher): Promise<OwnerSnapshot>;

/** Create a page-local loader with bounded, verified, in-memory chunk reuse. */
export function createOwnerSnapshotLoader(): (
  fetchJson: SnapshotJsonFetcher,
  options?: OwnerSnapshotLoaderOptions,
) => Promise<OwnerSnapshot>;
