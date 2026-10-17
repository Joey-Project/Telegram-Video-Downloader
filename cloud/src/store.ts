import { HttpError } from "./types";
import type { InboxEnvelope, LocalSnapshot, RequestKind } from "./types";
import { canonicalJson, MAX_WEBHOOK_BODY_BYTES, sha256Hex } from "./validation";

interface InboxRow {
  seq: number;
  kind: RequestKind;
  payload_json: string;
  created_at: number;
  acked_at: number | null;
  payload_hash: string;
}

interface SnapshotMetaRow {
  state_version: number;
  reported_at: number;
  received_at: number;
  library_revision: string;
  library_hash: string;
  library_chunk_count: number;
  state_hash: string;
  state_chunk_count: number;
}

interface SnapshotChunkRow {
  kind: "library" | "state";
  part: number;
  data: string;
}

export interface EnqueueResult {
  envelope: InboxEnvelope;
  duplicate: boolean;
}

export interface PendingSummary {
  seq: number;
  kind: RequestKind;
  created_at: number;
  status: "waiting_for_local";
}

const SNAPSHOT_CHUNK_BYTES = 500_000;
const MAX_SNAPSHOT_BYTES = 12 * 1024 * 1024;
const MAX_SNAPSHOT_CHUNKS = 24;
const HEARTBEAT_WRITE_INTERVAL_MS = 30_000;
const SNAPSHOT_STALE_AFTER_MS = 10 * 60_000;

export async function enqueue(
  db: D1Database,
  dedupKey: string,
  kind: RequestKind,
  payload: Record<string, unknown>,
): Promise<EnqueueResult> {
  const payloadJson = canonicalJson(payload);
  if (new TextEncoder().encode(payloadJson).byteLength > MAX_WEBHOOK_BODY_BYTES) {
    throw new HttpError(413, "Stored request payload is too large");
  }
  const payloadHash = await sha256Hex(payloadJson);
  const createdAt = Date.now();
  const insert = await db.prepare(
    "INSERT INTO inbox (dedup_key, payload_hash, kind, payload_json, created_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(dedup_key) DO NOTHING",
  ).bind(dedupKey, payloadHash, kind, payloadJson, createdAt).run();
  if (!insert.success) throw new Error("D1 inbox insert failed");

  const row = await db.prepare(
    "SELECT seq, kind, payload_json, created_at, acked_at, payload_hash FROM inbox WHERE dedup_key = ?",
  ).bind(dedupKey).first<InboxRow>();
  if (row === null) throw new Error("D1 inbox row was not readable after insert");
  if (row.payload_hash !== payloadHash || row.kind !== kind) {
    throw new HttpError(409, "Request identifier was already used with a different request");
  }

  let payloadValue: unknown;
  try {
    payloadValue = JSON.parse(row.payload_json) as unknown;
  } catch {
    throw new Error("Stored inbox payload is invalid JSON");
  }
  if (typeof payloadValue !== "object" || payloadValue === null || Array.isArray(payloadValue)) {
    throw new Error("Stored inbox payload is not an object");
  }
  return {
    envelope: {
      seq: row.seq,
      kind: row.kind,
      payload: payloadValue as Record<string, unknown>,
      created_at: row.created_at,
    },
    duplicate: insert.meta.changes === 0,
  };
}

export async function getPending(db: D1Database, limit: number): Promise<InboxEnvelope[]> {
  const result = await db.prepare(
    "SELECT seq, kind, payload_json, created_at FROM inbox WHERE acked_at IS NULL ORDER BY seq ASC LIMIT ?",
  ).bind(limit).all<Omit<InboxRow, "acked_at" | "payload_hash">>();
  if (!result.success) throw new Error("D1 inbox read failed");
  return result.results.map((row) => ({
    seq: row.seq,
    kind: row.kind,
    payload: JSON.parse(row.payload_json) as Record<string, unknown>,
    created_at: row.created_at,
  }));
}

export async function getPendingSummary(db: D1Database, limit: number): Promise<PendingSummary[]> {
  const result = await db.prepare(
    "SELECT seq, kind, created_at FROM inbox WHERE acked_at IS NULL ORDER BY seq ASC LIMIT ?",
  ).bind(limit).all<Omit<PendingSummary, "status">>();
  if (!result.success) throw new Error("D1 inbox summary read failed");
  return result.results.map((row) => ({ ...row, status: "waiting_for_local" }));
}

export async function getPendingBySeq(db: D1Database, seq: number): Promise<InboxEnvelope | null> {
  const row = await db.prepare(
    "SELECT seq, kind, payload_json, created_at FROM inbox WHERE seq = ? AND acked_at IS NULL",
  ).bind(seq).first<Omit<InboxRow, "acked_at" | "payload_hash">>();
  if (row === null) return null;
  return {
    seq: row.seq,
    kind: row.kind,
    payload: JSON.parse(row.payload_json) as Record<string, unknown>,
    created_at: row.created_at,
  };
}

export async function acknowledge(db: D1Database, seqs: number[]): Promise<number> {
  const statements: D1PreparedStatement[] = [];
  for (let offset = 0; offset < seqs.length; offset += 99) {
    const chunk = seqs.slice(offset, offset + 99);
    const placeholders = chunk.map(() => "?").join(",");
    statements.push(db.prepare(
      `UPDATE inbox SET acked_at = ? WHERE acked_at IS NULL AND seq IN (${placeholders})`,
    ).bind(Date.now(), ...chunk));
  }
  const results = await db.batch(statements);
  if (results.some((result) => !result.success)) throw new Error("D1 inbox acknowledgement failed");
  return results.reduce((total, result) => total + result.meta.changes, 0);
}

function splitUtf8(value: string): string[] {
  const bytes = new TextEncoder().encode(value);
  if (bytes.byteLength > MAX_SNAPSHOT_BYTES) {
    throw new HttpError(413, "Snapshot exceeds the storage size limit");
  }
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const chunks: string[] = [];
  let start = 0;
  while (start < bytes.length) {
    let end = Math.min(start + SNAPSHOT_CHUNK_BYTES, bytes.length);
    while (end < bytes.length && (bytes[end]! & 0xc0) === 0x80) end -= 1;
    if (end === start) throw new Error("Unable to split snapshot at a UTF-8 boundary");
    chunks.push(decoder.decode(bytes.subarray(start, end)));
    start = end;
  }
  return chunks;
}

function stateOnly(snapshot: LocalSnapshot): Record<string, unknown> {
  const { library: _library, reported_at: _reportedAt, state_version: _stateVersion, ...state } = snapshot;
  return state;
}

export async function storeSnapshot(
  db: D1Database,
  snapshot: LocalSnapshot,
): Promise<{ ignoredOutOfOrder: boolean }> {
  const libraryJson = canonicalJson(snapshot.library);
  const stateJson = canonicalJson(stateOnly(snapshot));
  const [libraryHash, stateHash, prior] = await Promise.all([
    sha256Hex(libraryJson),
    sha256Hex(stateJson),
    db.prepare(
      `SELECT state_version, reported_at, received_at, library_revision, library_hash,
              library_chunk_count, state_hash, state_chunk_count
       FROM snapshot_meta WHERE id = 1`,
    ).first<SnapshotMetaRow>(),
  ]);
  const libraryChunks = splitUtf8(libraryJson);
  const stateChunks = splitUtf8(stateJson);
  if (libraryChunks.length + stateChunks.length > MAX_SNAPSHOT_CHUNKS) {
    throw new HttpError(413, "Snapshot exceeds the storage chunk limit");
  }
  const now = Date.now();
  if (prior !== null && snapshot.state_version < prior.state_version) {
    throw new HttpError(409, "Snapshot state_version is older than the stored state");
  }

  const contentMatches = prior !== null &&
    prior.library_revision === snapshot.library.revision &&
    prior.library_hash === libraryHash &&
    prior.state_hash === stateHash;
  if (prior !== null && snapshot.state_version === prior.state_version && !contentMatches) {
    throw new HttpError(409, "Snapshot state_version was reused with different content");
  }

  const heartbeatDue = prior === null || now - prior.received_at >= HEARTBEAT_WRITE_INTERVAL_MS;
  if (prior !== null && snapshot.state_version === prior.state_version && contentMatches) {
    if (heartbeatDue) {
      const heartbeat = await db.prepare(
        `UPDATE snapshot_meta
         SET reported_at = MAX(reported_at, ?), received_at = ?
         WHERE id = 1 AND state_version = ? AND library_hash = ? AND state_hash = ?`,
      ).bind(snapshot.reported_at, now, snapshot.state_version, libraryHash, stateHash).run();
      if (!heartbeat.success) throw new Error("D1 snapshot heartbeat update failed");
    }
    const latest = await db.prepare(
      `SELECT state_version, reported_at, received_at, library_revision, library_hash,
              library_chunk_count, state_hash, state_chunk_count
       FROM snapshot_meta WHERE id = 1`,
    ).first<SnapshotMetaRow>();
    if (latest === null || latest.state_version !== snapshot.state_version ||
      latest.library_hash !== libraryHash || latest.state_hash !== stateHash) {
      throw new HttpError(409, "Snapshot was superseded by a newer local state");
    }
    return { ignoredOutOfOrder: false };
  }

  const versionGate = "(NOT EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1) OR (SELECT state_version FROM snapshot_meta WHERE id = 1) < ?)";
  const statements: D1PreparedStatement[] = [];
  statements.push(db.prepare(
    `DELETE FROM snapshot_chunks
     WHERE kind = 'library' AND ${versionGate}
       AND (NOT EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1) OR
            (SELECT library_hash FROM snapshot_meta WHERE id = 1) <> ?)`,
  ).bind(snapshot.state_version, libraryHash));
  statements.push(...libraryChunks.map((data, part) => db.prepare(
    `INSERT INTO snapshot_chunks (kind, part, data)
     SELECT 'library', ?, ?
     WHERE ${versionGate}
       AND (NOT EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1) OR
            (SELECT library_hash FROM snapshot_meta WHERE id = 1) <> ?)`,
  ).bind(part, data, snapshot.state_version, libraryHash)));
  statements.push(db.prepare(
    `DELETE FROM snapshot_chunks
     WHERE kind = 'state' AND ${versionGate}
       AND (NOT EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1) OR
            (SELECT state_hash FROM snapshot_meta WHERE id = 1) <> ?)`,
  ).bind(snapshot.state_version, stateHash));
  statements.push(...stateChunks.map((data, part) => db.prepare(
    `INSERT INTO snapshot_chunks (kind, part, data)
     SELECT 'state', ?, ?
     WHERE ${versionGate}
       AND (NOT EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1) OR
            (SELECT state_hash FROM snapshot_meta WHERE id = 1) <> ?)`,
  ).bind(part, data, snapshot.state_version, stateHash)));
  statements.push(db.prepare(
    `INSERT INTO snapshot_meta (
       id, state_version, reported_at, received_at, library_revision, library_hash,
       library_chunk_count, state_hash, state_chunk_count
     ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET
       state_version = excluded.state_version,
       reported_at = MAX(snapshot_meta.reported_at, excluded.reported_at),
       received_at = excluded.received_at,
       library_revision = excluded.library_revision,
       library_hash = excluded.library_hash,
       library_chunk_count = excluded.library_chunk_count,
       state_hash = excluded.state_hash,
       state_chunk_count = excluded.state_chunk_count
     WHERE excluded.state_version > snapshot_meta.state_version`,
  ).bind(
    snapshot.state_version,
    snapshot.reported_at,
    now,
    snapshot.library.revision,
    libraryHash,
    libraryChunks.length,
    stateHash,
    stateChunks.length,
  ));
  const results = await db.batch(statements);
  if (results.some((result) => !result.success)) throw new Error("D1 snapshot batch failed");

  const latest = await db.prepare(
    `SELECT state_version, reported_at, received_at, library_revision, library_hash,
            library_chunk_count, state_hash, state_chunk_count
     FROM snapshot_meta WHERE id = 1`,
  ).first<SnapshotMetaRow>();
  if (latest === null) throw new Error("D1 snapshot metadata was not readable after write");
  if (latest.state_version > snapshot.state_version) {
    throw new HttpError(409, "Snapshot was superseded by a newer local state");
  }
  if (latest.state_version !== snapshot.state_version || latest.library_revision !== snapshot.library.revision ||
    latest.library_hash !== libraryHash || latest.state_hash !== stateHash) {
    throw new HttpError(409, "Snapshot state_version was reused with different content");
  }
  return { ignoredOutOfOrder: false };
}

export interface StoredSnapshot {
  synced: true;
  state_version: number;
  revision: string | null;
  reported_at: number;
  last_seen: number;
  stale: boolean;
  library: Record<string, unknown>;
  previews: unknown[];
  operations: unknown[];
  tasks: unknown[];
  settings: { download_dir: string };
}

export async function readSnapshot(db: D1Database): Promise<StoredSnapshot | null> {
  const meta = await db.prepare(
    `SELECT state_version, reported_at, received_at, library_revision, library_hash,
            library_chunk_count, state_hash, state_chunk_count
     FROM snapshot_meta WHERE id = 1`,
  ).first<SnapshotMetaRow>();
  if (meta === null) return null;

  const chunksResult = await db.prepare(
    "SELECT kind, part, data FROM snapshot_chunks ORDER BY kind ASC, part ASC",
  ).all<SnapshotChunkRow>();
  if (!chunksResult.success) throw new Error("D1 snapshot chunks read failed");
  const libraryChunks = chunksResult.results.filter((chunk) => chunk.kind === "library");
  const stateChunks = chunksResult.results.filter((chunk) => chunk.kind === "state");
  if (libraryChunks.length !== meta.library_chunk_count || stateChunks.length !== meta.state_chunk_count) {
    throw new Error("Stored snapshot chunk count mismatch");
  }

  const library = JSON.parse(libraryChunks.map((chunk) => chunk.data).join("")) as unknown;
  const state = JSON.parse(stateChunks.map((chunk) => chunk.data).join("")) as unknown;
  if (typeof library !== "object" || library === null || Array.isArray(library) ||
    typeof state !== "object" || state === null || Array.isArray(state)) {
    throw new Error("Stored snapshot is malformed");
  }
  const stateRecord = state as Record<string, unknown>;
  const libraryRecord = library as Record<string, unknown>;
  const revision = typeof stateRecord.revision === "string" ? stateRecord.revision : null;
  if (!Array.isArray(stateRecord.previews) || !Array.isArray(stateRecord.operations) || !Array.isArray(stateRecord.tasks) ||
    typeof stateRecord.settings !== "object" || stateRecord.settings === null || Array.isArray(stateRecord.settings)) {
    throw new Error("Stored snapshot state is malformed");
  }
  return {
    synced: true,
    state_version: meta.state_version,
    revision,
    reported_at: meta.reported_at,
    last_seen: meta.received_at,
    stale: Date.now() - meta.received_at > SNAPSHOT_STALE_AFTER_MS,
    library: libraryRecord,
    previews: stateRecord.previews,
    operations: stateRecord.operations,
    tasks: stateRecord.tasks,
    settings: stateRecord.settings as { download_dir: string },
  };
}

export function emptySnapshot(): {
  synced: false;
  revision: null;
  reported_at: null;
  last_seen: null;
  stale: true;
  library: null;
  previews: [];
  operations: [];
  tasks: [];
  settings: { download_dir: null };
  pending_requests: [];
} {
  return {
    synced: false,
    revision: null,
    reported_at: null,
    last_seen: null,
    stale: true,
    library: null,
    previews: [],
    operations: [],
    tasks: [],
    settings: { download_dir: null },
    pending_requests: [],
  };
}
