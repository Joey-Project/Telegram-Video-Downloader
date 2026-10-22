export type SnapshotRequestHeaders = Readonly<Record<string, string>>;

export type SnapshotJsonFetcher = (
  path: string,
  headers?: SnapshotRequestHeaders,
) => Promise<unknown>;

/** Load a legacy owner snapshot or fetch and validate all chunks in a chunked-v1 manifest. */
export function loadOwnerSnapshot(fetchJson: SnapshotJsonFetcher): Promise<unknown>;
