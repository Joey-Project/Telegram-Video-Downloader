export type SnapshotRequestHeaders = Readonly<Record<string, string>>;

export type SnapshotJsonFetcher = (
  path: string,
  headers?: SnapshotRequestHeaders,
) => Promise<unknown>;

/** Request chunked-v1 when loading an owner snapshot, while accepting legacy full responses. */
export function loadOwnerSnapshot(fetchJson: SnapshotJsonFetcher): Promise<unknown>;

/** Create a page-local loader with bounded, verified, in-memory chunk reuse. */
export function createOwnerSnapshotLoader(): (fetchJson: SnapshotJsonFetcher) => Promise<unknown>;
