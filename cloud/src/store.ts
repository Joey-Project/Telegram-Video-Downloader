import { HttpError } from "./types";
import type {
  ChunkedSnapshotContent,
  ChunkedStoredSnapshot,
  InboxEnvelope,
  LocalSnapshot,
  PreparedSnapshotContent,
  RequestKind,
  SnapshotChunkManifest,
} from "./types";
import {
  canonicalJson,
  MAX_CHUNKED_SNAPSHOT_BYTES,
  MAX_SNAPSHOT_CONTENT_CHUNK_BYTES,
  MAX_JSON_COMPLEXITY_DEPTH,
  MAX_SNAPSHOT_JSON_NODES,
  MAX_WEBHOOK_BODY_BYTES,
  sha256Hex,
  validateChunkedSnapshot,
} from "./validation";

interface InboxRow {
  seq: number;
  dedup_key: string;
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
  transport: "legacy" | "chunked-v1";
  library_manifest_json: string | null;
  state_manifest_json: string | null;
}

interface SnapshotChunkRow {
  kind: "library" | "state";
  part: number;
  data: string;
}

interface SnapshotContentChunkRow {
  database_epoch: string;
  sha256: string;
  byte_length: number;
  data: string;
  created_at: number;
}

export interface EnqueueResult {
  envelope: InboxEnvelope;
  duplicate: boolean;
}

export interface ActionReplayWindow {
  queued_at_ms: number;
  replay_expires_at_ms: number;
}

export interface PendingSummary {
  seq: number;
  kind: RequestKind;
  created_at: number;
  status: "waiting_for_local";
}

export interface PendingPage {
  database_epoch: string;
  requests: InboxEnvelope[];
}

interface DatabaseEpochRow {
  epoch: string;
}

const SNAPSHOT_CHUNK_BYTES = 500_000;
const MAX_SNAPSHOT_BYTES = 12 * 1024 * 1024;
const MAX_SNAPSHOT_CHUNKS = 24;
const SNAPSHOT_CHUNKED_HASH_DOMAIN = "telegram-video-downloader/chunked-v1/";
const SNAPSHOT_CHUNK_PRUNE_BATCH_SIZE = 32;
const SNAPSHOT_CHUNK_RETENTION_MS = 60 * 60 * 1000;
const SNAPSHOT_META_SELECT = `SELECT state_version, reported_at, received_at, library_revision, library_hash,
       library_chunk_count, state_hash, state_chunk_count, transport, library_manifest_json, state_manifest_json
       FROM snapshot_meta WHERE id = 1`;
const HEARTBEAT_WRITE_INTERVAL_MS = 30_000;
const SNAPSHOT_STALE_AFTER_MS = 10 * 60_000;
// Keep acknowledged idempotency keys reserved for a bounded replay window.
export const ACKED_INBOX_REPLAY_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
export const ACKED_INBOX_PRUNE_BATCH_SIZE = 100;

export function isDatabaseEpoch(value: string): boolean {
  return /^[0-9a-f]{32}$/u.test(value);
}

async function readDatabaseEpoch(db: D1Database): Promise<string> {
  const row = await db.prepare("SELECT epoch FROM cloud_database_meta WHERE id = 1").first<DatabaseEpochRow>();
  if (row === null || !isDatabaseEpoch(row.epoch)) throw new Error("D1 database epoch is missing or invalid");
  return row.epoch;
}

export async function getDatabaseEpoch(db: D1Database): Promise<string> {
  return readDatabaseEpoch(db);
}

function epochChanged(expected: string, actual: string): HttpError {
  return new HttpError(409, "Cloud database epoch changed", undefined, actual);
}

export async function assertDatabaseEpoch(db: D1Database, expected: string | null): Promise<string> {
  if (expected === null || expected.length === 0) throw new HttpError(428, "Cloud database epoch is required");
  if (!isDatabaseEpoch(expected)) throw new HttpError(400, "Cloud database epoch is invalid");
  const actual = await readDatabaseEpoch(db);
  if (actual !== expected) throw epochChanged(expected, actual);
  return actual;
}

async function pruneAcknowledgedInbox(db: D1Database, now = Date.now()): Promise<number> {
  const cutoff = now - ACKED_INBOX_REPLAY_RETENTION_MS;
  const result = await db.prepare(
    `DELETE FROM inbox
     WHERE seq IN (
       SELECT seq FROM inbox
       WHERE acked_at IS NOT NULL AND acked_at < ?
       ORDER BY acked_at ASC, seq ASC
       LIMIT ?
     )`,
  ).bind(cutoff, ACKED_INBOX_PRUNE_BATCH_SIZE).run();
  if (!result.success) throw new Error("D1 acknowledged inbox retention failed");
  return result.meta.changes;
}

async function pruneAcknowledgedInboxBestEffort(db: D1Database): Promise<void> {
  try {
    await pruneAcknowledgedInbox(db);
  } catch {
    // Retention is maintenance; it must not change the result of a durable inbox operation.
    console.warn(JSON.stringify({ event: "inbox_retention_prune_failed" }));
  }
}

export async function enqueue(
  db: D1Database,
  dedupKey: string,
  kind: RequestKind,
  payload: Record<string, unknown>,
  replayWindow?: ActionReplayWindow,
): Promise<EnqueueResult> {
  const payloadJson = canonicalJson(payload);
  if (new TextEncoder().encode(payloadJson).byteLength > MAX_WEBHOOK_BODY_BYTES) {
    throw new HttpError(413, "Stored request payload is too large");
  }
  const hashInput = replayWindow === undefined
    ? payloadJson
    : canonicalJson({
      kind,
      payload,
      queued_at_ms: replayWindow.queued_at_ms,
      replay_expires_at_ms: replayWindow.replay_expires_at_ms,
    });
  const payloadHash = await sha256Hex(hashInput);
  const readRow = () => db.prepare(
    "SELECT seq, dedup_key, kind, payload_json, created_at, acked_at, payload_hash FROM inbox WHERE dedup_key = ?",
  ).bind(dedupKey).first<InboxRow>();

  // Check an extant row before expiry. Its exact hash is the durable proof that
  // this request ID already received a sequence, even when the retry window
  // elapsed while the D1 row was still retained.
  if (replayWindow !== undefined) {
    const existing = await readRow();
    if (existing !== null) return buildEnqueueResult(existing, payloadHash, kind, true);
    if (Date.now() >= replayWindow.replay_expires_at_ms) {
      throw new HttpError(
        410,
        "Action replay window expired; delivery outcome is unknown. Check the desktop before submitting again.",
        undefined,
        undefined,
        undefined,
        "action_replay_window_expired",
      );
    }
  }

  const createdAt = Date.now();
  const insert = await db.prepare(
    "INSERT INTO inbox (dedup_key, payload_hash, kind, payload_json, created_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(dedup_key) DO NOTHING",
  ).bind(dedupKey, payloadHash, kind, payloadJson, createdAt).run();
  if (!insert.success) throw new Error("D1 inbox insert failed");

  const row = await readRow();
  if (row === null) throw new Error("D1 inbox row was not readable after insert");
  return buildEnqueueResult(row, payloadHash, kind, insert.meta.changes === 0);
}

function buildEnqueueResult(row: InboxRow, payloadHash: string, kind: RequestKind, duplicate: boolean): EnqueueResult {
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
      request_id: row.dedup_key,
      payload_hash: row.payload_hash,
    },
    duplicate,
  };
}

export async function getPending(db: D1Database, limit: number): Promise<InboxEnvelope[]> {
  await pruneAcknowledgedInboxBestEffort(db);
  const result = await db.prepare(
    "SELECT seq, dedup_key, kind, payload_json, created_at, payload_hash FROM inbox WHERE acked_at IS NULL ORDER BY seq ASC LIMIT ?",
  ).bind(limit).all<Omit<InboxRow, "acked_at">>();
  if (!result.success) throw new Error("D1 inbox read failed");
  return result.results.map((row) => ({
    seq: row.seq,
    kind: row.kind,
    payload: JSON.parse(row.payload_json) as Record<string, unknown>,
    created_at: row.created_at,
    request_id: row.dedup_key,
    payload_hash: row.payload_hash,
  }));
}

export async function getPendingPage(db: D1Database, limit: number): Promise<PendingPage> {
  await pruneAcknowledgedInboxBestEffort(db);
  const [epochResult, inboxResult] = await db.batch<DatabaseEpochRow | InboxRow>([
    db.prepare("SELECT epoch FROM cloud_database_meta WHERE id = 1"),
    db.prepare(
      `SELECT seq, dedup_key, payload_hash, kind, payload_json, created_at
       FROM inbox WHERE acked_at IS NULL ORDER BY seq ASC LIMIT ?`,
    ).bind(limit),
  ]);
  if (!epochResult?.success) throw new Error("D1 database epoch read failed");
  const epoch = epochResult.results[0] as DatabaseEpochRow | undefined;
  if (epoch === undefined || !isDatabaseEpoch(epoch.epoch)) throw new Error("D1 database epoch is missing or invalid");
  if (!inboxResult?.success) throw new Error("D1 inbox read failed");
  return {
    database_epoch: epoch.epoch,
    requests: (inboxResult.results as InboxRow[]).map((row) => ({
      seq: row.seq,
      kind: row.kind,
      payload: JSON.parse(row.payload_json) as Record<string, unknown>,
      created_at: row.created_at,
      request_id: row.dedup_key,
      payload_hash: row.payload_hash,
    })),
  };
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
    "SELECT seq, dedup_key, payload_hash, kind, payload_json, created_at FROM inbox WHERE seq = ? AND acked_at IS NULL",
  ).bind(seq).first<Omit<InboxRow, "acked_at">>();
  if (row === null) return null;
  return {
    seq: row.seq,
    kind: row.kind,
    payload: JSON.parse(row.payload_json) as Record<string, unknown>,
    created_at: row.created_at,
    request_id: row.dedup_key,
    payload_hash: row.payload_hash,
  };
}

export interface AcknowledgedReceipt {
  seq: number;
  request_id: string;
  payload_hash: string;
  kind: RequestKind;
  created_at: number;
}

export async function acknowledge(
  db: D1Database,
  receipts: AcknowledgedReceipt[],
  expectedEpoch?: string,
): Promise<number> {
  const expected = expectedEpoch ?? await readDatabaseEpoch(db);
  if (!isDatabaseEpoch(expected)) throw new HttpError(428, "Cloud database epoch is required");
  const receiptsJson = JSON.stringify(receipts);
  const statements: D1PreparedStatement[] = [
    db.prepare(
      "UPDATE cloud_database_meta SET epoch = epoch WHERE id = 1 AND epoch = ?",
    ).bind(expected),
    db.prepare(
      "SELECT i.seq\n" +
      "FROM inbox AS i\n" +
      "JOIN json_each(?) AS receipt\n" +
      "  ON i.seq = CAST(json_extract(receipt.value, '$.seq') AS INTEGER)\n" +
      "WHERE i.dedup_key <> json_extract(receipt.value, '$.request_id')\n" +
      "   OR i.payload_hash <> json_extract(receipt.value, '$.payload_hash')\n" +
      "   OR i.kind <> json_extract(receipt.value, '$.kind')\n" +
      "   OR i.created_at <> json_extract(receipt.value, '$.created_at')\n" +
      "ORDER BY i.seq ASC LIMIT 1",
    ).bind(receiptsJson),
    db.prepare(
      "UPDATE inbox SET acked_at = ?\n" +
      "WHERE acked_at IS NULL\n" +
      "  AND EXISTS (\n" +
      "    SELECT 1 FROM json_each(?) AS receipt\n" +
      "    WHERE CAST(json_extract(receipt.value, '$.seq') AS INTEGER) = inbox.seq\n" +
      "      AND json_extract(receipt.value, '$.request_id') = inbox.dedup_key\n" +
      "      AND json_extract(receipt.value, '$.payload_hash') = inbox.payload_hash\n" +
      "      AND json_extract(receipt.value, '$.kind') = inbox.kind\n" +
      "      AND CAST(json_extract(receipt.value, '$.created_at') AS INTEGER) = inbox.created_at\n" +
      "  )\n" +
      "  AND NOT EXISTS (\n" +
      "    SELECT 1 FROM json_each(?) AS receipt\n" +
      "    JOIN inbox AS existing\n" +
      "      ON existing.seq = CAST(json_extract(receipt.value, '$.seq') AS INTEGER)\n" +
      "    WHERE existing.dedup_key <> json_extract(receipt.value, '$.request_id')\n" +
      "       OR existing.payload_hash <> json_extract(receipt.value, '$.payload_hash')\n" +
      "       OR existing.kind <> json_extract(receipt.value, '$.kind')\n" +
      "       OR existing.created_at <> json_extract(receipt.value, '$.created_at')\n" +
      "  )\n" +
      "  AND EXISTS (SELECT 1 FROM cloud_database_meta WHERE id = 1 AND epoch = ?)",
    ).bind(Date.now(), receiptsJson, receiptsJson, expected),
  ];
  const results = await db.batch(statements);
  if (results.some((result) => !result.success)) throw new Error("D1 inbox acknowledgement failed");
  await assertDatabaseEpoch(db, expected);
  const mismatch = results[1]?.results[0] as { seq: number } | undefined;
  if (mismatch !== undefined) {
    throw new HttpError(409, "Cloud inbox sequence conflicts with a different request", undefined, undefined, mismatch.seq);
  }
  // The ACK is committed before best-effort cleanup; failures cannot make it appear uncommitted.
  await pruneAcknowledgedInboxBestEffort(db);
  return results[2]?.meta.changes ?? 0;
}

export interface EpochRotationRequest {
  seq: number;
  request_id: string;
  payload_hash: string;
  kind: RequestKind;
  created_at: number;
}

export function createDatabaseEpoch(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function rotateDatabaseEpochOnSequenceConflict(
  db: D1Database,
  expectedEpoch: string,
  nextEpoch: string,
  request: EpochRotationRequest,
): Promise<{ rotated: boolean; database_epoch: string }> {
  if (!isDatabaseEpoch(expectedEpoch) || !isDatabaseEpoch(nextEpoch) || expectedEpoch === nextEpoch) {
    throw new Error("Invalid database epoch rotation request");
  }
  const results = await db.batch([
    db.prepare(
      `UPDATE cloud_database_meta SET epoch = ?
       WHERE id = 1 AND epoch = ?
         AND EXISTS (
           SELECT 1 FROM inbox
           WHERE seq = ? AND acked_at IS NULL AND dedup_key = ? AND payload_hash = ?
             AND kind = ? AND created_at = ?
         )`,
    ).bind(
      nextEpoch,
      expectedEpoch,
      request.seq,
      request.request_id,
      request.payload_hash,
      request.kind,
      request.created_at,
    ),
    db.prepare(
      `DELETE FROM snapshot_chunks
       WHERE EXISTS (SELECT 1 FROM cloud_database_meta WHERE id = 1 AND epoch = ?)`,
    ).bind(nextEpoch),
    db.prepare(
      `DELETE FROM snapshot_meta
       WHERE id = 1 AND EXISTS (SELECT 1 FROM cloud_database_meta WHERE id = 1 AND epoch = ?)`,
    ).bind(nextEpoch),
  ]);
  if (results.some((result) => !result.success)) throw new Error("D1 database epoch rotation failed");
  const actual = await readDatabaseEpoch(db);
  return { rotated: results[0]?.meta.changes === 1, database_epoch: actual };
}

async function sha256HexBytes(bytes: Uint8Array<ArrayBuffer>): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function normalizedManifestJson(manifest: SnapshotChunkManifest): string {
  return JSON.stringify({
    bytes: manifest.bytes,
    chunks: manifest.chunks.map(({ sha256, bytes }) => ({ sha256, bytes })),
  });
}

async function hashSnapshotManifest(kind: "library" | "state", manifestJson: string): Promise<string> {
  const bytes = new TextEncoder().encode(`${SNAPSHOT_CHUNKED_HASH_DOMAIN}${kind}\0${manifestJson}`);
  return sha256HexBytes(bytes);
}

export async function storeSnapshotContentChunk(
  db: D1Database,
  expectedEpoch: string,
  expectedHash: string,
  bytes: Uint8Array<ArrayBuffer>,
  data: string,
): Promise<void> {
  if (!isDatabaseEpoch(expectedEpoch)) throw new HttpError(400, "Cloud database epoch is invalid");
  if (!/^[0-9a-f]{64}$/u.test(expectedHash)) throw new HttpError(400, "Snapshot content chunk hash is invalid");
  if (bytes.byteLength === 0 || bytes.byteLength > MAX_SNAPSHOT_CONTENT_CHUNK_BYTES) {
    throw new HttpError(413, "Snapshot content chunk is too large");
  }
  const encodedData = new TextEncoder().encode(data);
  if (encodedData.byteLength !== bytes.byteLength || encodedData.some((byte, index) => byte !== bytes[index])) {
    throw new HttpError(400, "Snapshot content chunk bytes are inconsistent");
  }
  const actualHash = await sha256HexBytes(bytes);
  if (actualHash !== expectedHash) throw new HttpError(400, "Snapshot content chunk hash does not match its URL");

  const result = await db.prepare(
    `INSERT INTO snapshot_content_chunks (database_epoch, sha256, byte_length, data, created_at)
     SELECT ?, ?, ?, ?, ?
     WHERE EXISTS (SELECT 1 FROM cloud_database_meta WHERE id = 1 AND epoch = ?)
     ON CONFLICT(database_epoch, sha256) DO UPDATE SET created_at = excluded.created_at
     WHERE snapshot_content_chunks.byte_length = excluded.byte_length
       AND snapshot_content_chunks.data = excluded.data`,
  ).bind(expectedEpoch, expectedHash, bytes.byteLength, data, Date.now(), expectedEpoch).run();
  if (!result.success) throw new Error("D1 snapshot content chunk write failed");
  if (result.meta.changes === 1) return;
  await assertDatabaseEpoch(db, expectedEpoch);
  throw new Error("Stored snapshot content chunk conflicts with its content address");
}

export async function readSnapshotContentChunk(
  db: D1Database,
  expectedEpoch: string,
  expectedHash: string,
): Promise<{ sha256: string; data: string } | null> {
  if (!isDatabaseEpoch(expectedEpoch)) throw new HttpError(400, "Cloud database epoch is invalid");
  if (!/^[0-9a-f]{64}$/u.test(expectedHash)) throw new HttpError(400, "Snapshot content chunk hash is invalid");
  const row = await db.prepare(
    `SELECT database_epoch, sha256, byte_length, data, created_at
     FROM snapshot_content_chunks
     WHERE database_epoch = ? AND sha256 = ?
       AND EXISTS (SELECT 1 FROM cloud_database_meta WHERE id = 1 AND epoch = ?)`,
  ).bind(expectedEpoch, expectedHash, expectedEpoch).first<SnapshotContentChunkRow>();
  if (row === null) {
    await assertDatabaseEpoch(db, expectedEpoch);
    return null;
  }
  const bytes = new TextEncoder().encode(row.data);
  if (row.database_epoch !== expectedEpoch || row.sha256 !== expectedHash ||
    row.byte_length !== bytes.byteLength || bytes.byteLength > MAX_SNAPSHOT_CONTENT_CHUNK_BYTES ||
    await sha256HexBytes(bytes) !== expectedHash) {
    throw new Error("Stored snapshot content chunk integrity check failed");
  }
  return { sha256: expectedHash, data: row.data };
}

function utf8ChunkEnd(bytes: Uint8Array, start: number): number {
  let end = Math.min(start + SNAPSHOT_CHUNK_BYTES, bytes.length);
  while (end < bytes.length && (bytes[end]! & 0xc0) === 0x80) end -= 1;
  if (end === start) throw new Error("Unable to split snapshot at a UTF-8 boundary");
  return end;
}

function countUtf8Chunks(bytes: Uint8Array): number {
  if (bytes.byteLength > MAX_SNAPSHOT_BYTES) {
    throw new HttpError(413, "Snapshot exceeds the storage size limit");
  }
  let count = 0;
  for (let start = 0; start < bytes.length; count += 1) start = utf8ChunkEnd(bytes, start);
  return count;
}

function splitUtf8(bytes: Uint8Array): string[] {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const chunks: string[] = [];
  let start = 0;
  while (start < bytes.length) {
    const end = utf8ChunkEnd(bytes, start);
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
  expectedEpoch?: string,
  prepared?: Pick<PreparedSnapshotContent, "libraryJson" | "stateJson">,
): Promise<{ ignoredOutOfOrder: boolean }> {
  const epoch = expectedEpoch ?? await readDatabaseEpoch(db);
  if (!isDatabaseEpoch(epoch)) throw new HttpError(400, "Cloud database epoch is invalid");
  const snapshotCanonicalJsonOptions = {
    maxNodes: MAX_SNAPSHOT_JSON_NODES,
    maxDepth: MAX_JSON_COMPLEXITY_DEPTH,
    complexityStatus: 413,
  };
  const libraryJson = prepared?.libraryJson ?? canonicalJson(snapshot.library, snapshotCanonicalJsonOptions);
  const stateJson = prepared?.stateJson ?? canonicalJson(stateOnly(snapshot), snapshotCanonicalJsonOptions);
  const encoder = new TextEncoder();
  const libraryBytes = encoder.encode(libraryJson);
  const stateBytes = encoder.encode(stateJson);
  const [libraryHash, stateHash, readResults] = await Promise.all([
    sha256HexBytes(libraryBytes),
    sha256HexBytes(stateBytes),
    db.batch<DatabaseEpochRow | SnapshotMetaRow>([
      db.prepare("SELECT epoch FROM cloud_database_meta WHERE id = 1"),
      db.prepare(SNAPSHOT_META_SELECT),
    ]),
  ]);
  const epochResult = readResults[0];
  const snapshotResult = readResults[1];
  if (!epochResult?.success) throw new Error("D1 database epoch read failed");
  const currentEpoch = epochResult.results[0] as DatabaseEpochRow | undefined;
  if (currentEpoch === undefined || !isDatabaseEpoch(currentEpoch.epoch)) {
    throw new Error("D1 database epoch is missing or invalid");
  }
  if (currentEpoch.epoch !== epoch) throw epochChanged(epoch, currentEpoch.epoch);
  if (!snapshotResult?.success) throw new Error("D1 snapshot metadata read failed");
  const prior = snapshotResult.results[0] as SnapshotMetaRow | undefined ?? null;
  const libraryChunkCount = countUtf8Chunks(libraryBytes);
  const stateChunkCount = countUtf8Chunks(stateBytes);
  if (libraryChunkCount + stateChunkCount > MAX_SNAPSHOT_CHUNKS) {
    throw new HttpError(413, "Snapshot exceeds the storage chunk limit");
  }
  const now = Date.now();
  if (prior !== null && snapshot.state_version < prior.state_version) {
    throw new HttpError(409, "Snapshot state_version is older than the stored state", prior.state_version);
  }

  const contentMatches = prior !== null &&
    prior.transport === "legacy" &&
    prior.library_revision === snapshot.library.revision &&
    prior.library_hash === libraryHash &&
    prior.state_hash === stateHash;
  if (prior !== null && snapshot.state_version === prior.state_version && !contentMatches) {
    throw new HttpError(409, "Snapshot state_version was reused with different content", prior.state_version);
  }

  const heartbeatDue = prior === null || now - prior.received_at >= HEARTBEAT_WRITE_INTERVAL_MS;
  if (prior !== null && snapshot.state_version === prior.state_version && contentMatches) {
    if (heartbeatDue) {
      const heartbeat = await db.prepare(
        `UPDATE snapshot_meta
         SET reported_at = MAX(reported_at, ?), received_at = ?
         WHERE id = 1 AND state_version = ? AND library_hash = ? AND state_hash = ?
           AND EXISTS (SELECT 1 FROM cloud_database_meta WHERE id = 1 AND epoch = ?)`,
      ).bind(snapshot.reported_at, now, snapshot.state_version, libraryHash, stateHash, epoch).run();
      if (!heartbeat.success) throw new Error("D1 snapshot heartbeat update failed");
    }
    const [epochRead, latestRead] = await db.batch<DatabaseEpochRow | SnapshotMetaRow>([
      db.prepare("SELECT epoch FROM cloud_database_meta WHERE id = 1"),
      db.prepare(SNAPSHOT_META_SELECT),
    ]);
    if (!epochRead?.success) throw new Error("D1 database epoch read failed");
    const latestEpoch = epochRead.results[0] as DatabaseEpochRow | undefined;
    if (latestEpoch === undefined || !isDatabaseEpoch(latestEpoch.epoch)) {
      throw new Error("D1 database epoch is missing or invalid");
    }
    if (latestEpoch.epoch !== epoch) throw epochChanged(epoch, latestEpoch.epoch);
    if (!latestRead?.success) throw new Error("D1 snapshot metadata read failed");
    const latest = latestRead.results[0] as SnapshotMetaRow | undefined ?? null;
    if (latest === null || latest.state_version !== snapshot.state_version ||
      latest.library_hash !== libraryHash || latest.state_hash !== stateHash) {
      throw new HttpError(409, "Snapshot was superseded by a newer local state", latest?.state_version);
    }
    return { ignoredOutOfOrder: false };
  }

  const libraryChunks = splitUtf8(libraryBytes);
  const stateChunks = splitUtf8(stateBytes);
  const versionGate = "(NOT EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1) OR (SELECT state_version FROM snapshot_meta WHERE id = 1) < ?)";
  const epochGate = "EXISTS (SELECT 1 FROM cloud_database_meta WHERE id = 1 AND epoch = ?)";
  const statements: D1PreparedStatement[] = [
    db.prepare("UPDATE cloud_database_meta SET epoch = epoch WHERE id = 1 AND epoch = ?").bind(epoch),
  ];
  statements.push(db.prepare(
    `DELETE FROM snapshot_chunks
     WHERE kind = 'library' AND ${versionGate} AND ${epochGate}
       AND (NOT EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1) OR
            (SELECT library_hash FROM snapshot_meta WHERE id = 1) <> ?)`,
  ).bind(snapshot.state_version, epoch, libraryHash));
  statements.push(...libraryChunks.map((data, part) => db.prepare(
    `INSERT INTO snapshot_chunks (kind, part, data)
     SELECT 'library', ?, ?
     WHERE ${versionGate} AND ${epochGate}
       AND (NOT EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1) OR
            (SELECT library_hash FROM snapshot_meta WHERE id = 1) <> ?)`,
  ).bind(part, data, snapshot.state_version, epoch, libraryHash)));
  statements.push(db.prepare(
    `DELETE FROM snapshot_chunks
     WHERE kind = 'state' AND ${versionGate} AND ${epochGate}
       AND (NOT EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1) OR
            (SELECT state_hash FROM snapshot_meta WHERE id = 1) <> ?)`,
  ).bind(snapshot.state_version, epoch, stateHash));
  statements.push(...stateChunks.map((data, part) => db.prepare(
    `INSERT INTO snapshot_chunks (kind, part, data)
     SELECT 'state', ?, ?
     WHERE ${versionGate} AND ${epochGate}
       AND (NOT EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1) OR
            (SELECT state_hash FROM snapshot_meta WHERE id = 1) <> ?)`,
  ).bind(part, data, snapshot.state_version, epoch, stateHash)));
  statements.push(db.prepare(
    `INSERT INTO snapshot_meta (
       id, state_version, reported_at, received_at, library_revision, library_hash,
       library_chunk_count, state_hash, state_chunk_count, transport,
       library_manifest_json, state_manifest_json
     ) SELECT 1, ?, ?, ?, ?, ?, ?, ?, ?, 'legacy', NULL, NULL WHERE ${epochGate}
     ON CONFLICT(id) DO UPDATE SET
       state_version = excluded.state_version,
       reported_at = MAX(snapshot_meta.reported_at, excluded.reported_at),
       received_at = excluded.received_at,
       library_revision = excluded.library_revision,
       library_hash = excluded.library_hash,
       library_chunk_count = excluded.library_chunk_count,
       state_hash = excluded.state_hash,
       state_chunk_count = excluded.state_chunk_count,
       transport = 'legacy',
       library_manifest_json = NULL,
       state_manifest_json = NULL
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
    epoch,
  ));
  const results = await db.batch(statements);
  if (results.some((result) => !result.success)) throw new Error("D1 snapshot batch failed");

  const [epochRead, latestRead] = await db.batch<DatabaseEpochRow | SnapshotMetaRow>([
    db.prepare("SELECT epoch FROM cloud_database_meta WHERE id = 1"),
    db.prepare(SNAPSHOT_META_SELECT),
  ]);
  if (!epochRead?.success) throw new Error("D1 database epoch read failed");
  const latestEpoch = epochRead.results[0] as DatabaseEpochRow | undefined;
  if (latestEpoch === undefined || !isDatabaseEpoch(latestEpoch.epoch)) {
    throw new Error("D1 database epoch is missing or invalid");
  }
  if (latestEpoch.epoch !== epoch) throw epochChanged(epoch, latestEpoch.epoch);
  if (!latestRead?.success) throw new Error("D1 snapshot metadata read failed");
  const latest = latestRead.results[0] as SnapshotMetaRow | undefined ?? null;
  if (latest === null) throw new Error("D1 snapshot metadata was not readable after write");
  if (latest.state_version > snapshot.state_version) {
    throw new HttpError(409, "Snapshot was superseded by a newer local state", latest.state_version);
  }
  if (latest.state_version !== snapshot.state_version || latest.library_revision !== snapshot.library.revision ||
    latest.library_hash !== libraryHash || latest.state_hash !== stateHash) {
    throw new HttpError(409, "Snapshot state_version was reused with different content", latest.state_version);
  }
  return { ignoredOutOfOrder: false };
}

function pruneOrphanSnapshotChunks(
  db: D1Database,
  now: number,
  expectedEpoch: string,
  stateVersion: number,
  libraryHash: string,
  stateHash: string,
): D1PreparedStatement {
  return db.prepare(
    `DELETE FROM snapshot_content_chunks
     WHERE EXISTS (
       SELECT 1 FROM cloud_database_meta
       WHERE id = 1 AND epoch = ?
     )
       AND EXISTS (
         SELECT 1 FROM snapshot_meta
         WHERE id = 1 AND transport = 'chunked-v1' AND state_version = ?
           AND library_hash = ? AND state_hash = ?
       )
       AND rowid IN (
         SELECT orphan.rowid
         FROM snapshot_content_chunks AS orphan
         WHERE orphan.created_at < ?
           AND (
             orphan.database_epoch <> ?
             OR orphan.sha256 NOT IN (
               SELECT json_extract(ref.value, '$.sha256')
               FROM snapshot_meta AS meta,
                 json_each(COALESCE(meta.library_manifest_json, '{}'), '$.chunks') AS ref
               WHERE meta.id = 1 AND meta.transport = 'chunked-v1'
               UNION
               SELECT json_extract(ref.value, '$.sha256')
               FROM snapshot_meta AS meta,
                 json_each(COALESCE(meta.state_manifest_json, '{}'), '$.chunks') AS ref
               WHERE meta.id = 1 AND meta.transport = 'chunked-v1'
             )
           )
         ORDER BY orphan.created_at ASC, orphan.sha256 ASC
         LIMIT ?
       )`,
  ).bind(
    expectedEpoch,
    stateVersion,
    libraryHash,
    stateHash,
    now - SNAPSHOT_CHUNK_RETENTION_MS,
    expectedEpoch,
    SNAPSHOT_CHUNK_PRUNE_BATCH_SIZE,
  );
}

export async function storeChunkedSnapshot(
  db: D1Database,
  snapshot: ChunkedSnapshotContent,
  expectedEpoch: string,
): Promise<void> {
  if (!isDatabaseEpoch(expectedEpoch)) throw new HttpError(400, "Cloud database epoch is invalid");
  const libraryManifestJson = normalizedManifestJson(snapshot.library);
  const stateManifestJson = normalizedManifestJson(snapshot.state);
  const [libraryHash, stateHash] = await Promise.all([
    hashSnapshotManifest("library", libraryManifestJson),
    hashSnapshotManifest("state", stateManifestJson),
  ]);
  const referencedChunks = JSON.stringify([
    ...snapshot.library.chunks.map(({ sha256, bytes }) => ({ sha256, bytes })),
    ...snapshot.state.chunks.map(({ sha256, bytes }) => ({ sha256, bytes })),
  ]);
  const readLatest = () => db.batch<DatabaseEpochRow | SnapshotMetaRow>([
    db.prepare("SELECT epoch FROM cloud_database_meta WHERE id = 1"),
    db.prepare(SNAPSHOT_META_SELECT),
  ]);
  const [epochResult, snapshotResult] = await readLatest();
  if (!epochResult?.success) throw new Error("D1 database epoch read failed");
  const currentEpoch = epochResult.results[0] as DatabaseEpochRow | undefined;
  if (currentEpoch === undefined || !isDatabaseEpoch(currentEpoch.epoch)) {
    throw new Error("D1 database epoch is missing or invalid");
  }
  if (currentEpoch.epoch !== expectedEpoch) throw epochChanged(expectedEpoch, currentEpoch.epoch);
  if (!snapshotResult?.success) throw new Error("D1 snapshot metadata read failed");
  const prior = snapshotResult.results[0] as SnapshotMetaRow | undefined ?? null;

  if (prior !== null && snapshot.state_version < prior.state_version) {
    throw new HttpError(409, "Snapshot state_version is older than the stored state", prior.state_version);
  }
  const contentMatches = prior !== null && prior.transport === "chunked-v1" &&
    prior.library_revision === snapshot.library_revision &&
    prior.library_hash === libraryHash && prior.state_hash === stateHash;
  if (prior !== null && snapshot.state_version === prior.state_version && !contentMatches) {
    throw new HttpError(409, "Snapshot state_version was reused with different content", prior.state_version);
  }

  const now = Date.now();
  if (contentMatches && prior !== null && snapshot.state_version === prior.state_version) {
    const missingChunk = await db.prepare(
      `SELECT 1 AS missing
       FROM json_each(?) AS ref
       LEFT JOIN snapshot_content_chunks AS chunk
         ON chunk.database_epoch = ? AND chunk.sha256 = json_extract(ref.value, '$.sha256')
       WHERE chunk.sha256 IS NULL
          OR chunk.byte_length <> CAST(json_extract(ref.value, '$.bytes') AS INTEGER)
       LIMIT 1`,
    ).bind(referencedChunks, expectedEpoch).first<{ missing: number }>();
    if (missingChunk !== null) {
      await assertDatabaseEpoch(db, expectedEpoch);
      throw new HttpError(428, "Snapshot content chunks are missing or have invalid byte lengths");
    }
    const heartbeatStatements = now - prior.received_at >= HEARTBEAT_WRITE_INTERVAL_MS
      ? [db.prepare(
        `UPDATE snapshot_meta
         SET reported_at = MAX(reported_at, ?), received_at = ?
         WHERE id = 1 AND transport = 'chunked-v1' AND state_version = ?
           AND library_hash = ? AND state_hash = ?
           AND EXISTS (SELECT 1 FROM cloud_database_meta WHERE id = 1 AND epoch = ?)`,
      ).bind(snapshot.reported_at, now, snapshot.state_version, libraryHash, stateHash, expectedEpoch)]
      : [];
    heartbeatStatements.push(pruneOrphanSnapshotChunks(
      db,
      now,
      expectedEpoch,
      snapshot.state_version,
      libraryHash,
      stateHash,
    ));
    const heartbeatResults = await db.batch(heartbeatStatements);
    if (heartbeatResults.some((result) => !result.success)) {
      throw new Error("D1 snapshot heartbeat maintenance failed");
    }
    const [latestEpochResult, latestSnapshotResult] = await readLatest();
    if (!latestEpochResult?.success) throw new Error("D1 database epoch read failed");
    const latestEpoch = latestEpochResult.results[0] as DatabaseEpochRow | undefined;
    if (latestEpoch === undefined || !isDatabaseEpoch(latestEpoch.epoch)) {
      throw new Error("D1 database epoch is missing or invalid");
    }
    if (latestEpoch.epoch !== expectedEpoch) throw epochChanged(expectedEpoch, latestEpoch.epoch);
    if (!latestSnapshotResult?.success) throw new Error("D1 snapshot metadata read failed");
    const latest = latestSnapshotResult.results[0] as SnapshotMetaRow | undefined ?? null;
    if (latest === null || latest.state_version !== snapshot.state_version ||
      latest.transport !== "chunked-v1" || latest.library_hash !== libraryHash || latest.state_hash !== stateHash) {
      throw new HttpError(409, "Snapshot was superseded by a newer local state", latest?.state_version);
    }
    return;
  }

  const commit = db.prepare(
    `INSERT INTO snapshot_meta (
       id, state_version, reported_at, received_at, library_revision, library_hash,
       library_chunk_count, state_hash, state_chunk_count, transport,
       library_manifest_json, state_manifest_json
     )
     SELECT 1, ?, ?, ?, ?, ?, ?, ?, ?, 'chunked-v1', ?, ?
     WHERE EXISTS (SELECT 1 FROM cloud_database_meta WHERE id = 1 AND epoch = ?)
       AND NOT EXISTS (
         SELECT 1 FROM json_each(?) AS ref
         LEFT JOIN snapshot_content_chunks AS chunk
           ON chunk.database_epoch = ? AND chunk.sha256 = json_extract(ref.value, '$.sha256')
         WHERE chunk.sha256 IS NULL
            OR chunk.byte_length <> CAST(json_extract(ref.value, '$.bytes') AS INTEGER)
       )
     ON CONFLICT(id) DO UPDATE SET
       state_version = excluded.state_version,
       reported_at = MAX(snapshot_meta.reported_at, excluded.reported_at),
       received_at = excluded.received_at,
       library_revision = excluded.library_revision,
       library_hash = excluded.library_hash,
       library_chunk_count = excluded.library_chunk_count,
       state_hash = excluded.state_hash,
       state_chunk_count = excluded.state_chunk_count,
       transport = 'chunked-v1',
       library_manifest_json = excluded.library_manifest_json,
       state_manifest_json = excluded.state_manifest_json
     WHERE excluded.state_version > snapshot_meta.state_version`,
  ).bind(
    snapshot.state_version,
    snapshot.reported_at,
    now,
    snapshot.library_revision,
    libraryHash,
    snapshot.library.chunks.length,
    stateHash,
    snapshot.state.chunks.length,
    libraryManifestJson,
    stateManifestJson,
    expectedEpoch,
    referencedChunks,
    expectedEpoch,
  );
  const clearLegacyChunks = db.prepare(
    `DELETE FROM snapshot_chunks
     WHERE EXISTS (
       SELECT 1 FROM snapshot_meta
       WHERE id = 1 AND transport = 'chunked-v1' AND state_version = ?
         AND library_hash = ? AND state_hash = ?
         AND EXISTS (SELECT 1 FROM cloud_database_meta WHERE id = 1 AND epoch = ?)
     )`,
  ).bind(snapshot.state_version, libraryHash, stateHash, expectedEpoch);
  const pruneOrphanChunks = pruneOrphanSnapshotChunks(
    db,
    now,
    expectedEpoch,
    snapshot.state_version,
    libraryHash,
    stateHash,
  );
  const results = await db.batch([commit, clearLegacyChunks, pruneOrphanChunks]);
  if (results.some((result) => !result.success)) throw new Error("D1 chunked snapshot commit failed");

  const [latestEpochResult, latestSnapshotResult] = await readLatest();
  if (!latestEpochResult?.success) throw new Error("D1 database epoch read failed");
  const latestEpoch = latestEpochResult.results[0] as DatabaseEpochRow | undefined;
  if (latestEpoch === undefined || !isDatabaseEpoch(latestEpoch.epoch)) {
    throw new Error("D1 database epoch is missing or invalid");
  }
  if (latestEpoch.epoch !== expectedEpoch) throw epochChanged(expectedEpoch, latestEpoch.epoch);
  if (!latestSnapshotResult?.success) throw new Error("D1 snapshot metadata read failed");
  const latest = latestSnapshotResult.results[0] as SnapshotMetaRow | undefined ?? null;
  if (latest !== null && latest.state_version > snapshot.state_version) {
    throw new HttpError(409, "Snapshot was superseded by a newer local state", latest.state_version);
  }
  if (latest !== null && latest.state_version === snapshot.state_version) {
    if (latest.transport !== "chunked-v1" || latest.library_revision !== snapshot.library_revision ||
      latest.library_hash !== libraryHash || latest.state_hash !== stateHash) {
      throw new HttpError(409, "Snapshot state_version was reused with different content", latest.state_version);
    }
    return;
  }
  throw new HttpError(428, "Snapshot content chunks are missing or have invalid byte lengths");
}

export interface StoredSnapshot {
  transport?: "legacy";
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

export async function readSnapshot(db: D1Database): Promise<StoredSnapshot | ChunkedStoredSnapshot | null> {
  const [metaResult, epochResult, chunksResult] = await db.batch<SnapshotMetaRow | DatabaseEpochRow | SnapshotChunkRow>([
    db.prepare(SNAPSHOT_META_SELECT),
    db.prepare("SELECT epoch FROM cloud_database_meta WHERE id = 1"),
    db.prepare(
      `SELECT kind, part, data FROM snapshot_chunks
       WHERE EXISTS (SELECT 1 FROM snapshot_meta WHERE id = 1 AND transport = 'legacy')
       ORDER BY kind ASC, part ASC`,
    ),
  ]);
  if (!metaResult?.success) throw new Error("D1 snapshot metadata read failed");
  const meta = metaResult.results[0] as SnapshotMetaRow | undefined;
  if (meta === undefined) return null;

  if (meta.transport === "chunked-v1") {
    if (!epochResult?.success) throw new Error("D1 database epoch read failed");
    const epoch = epochResult.results[0] as DatabaseEpochRow | undefined;
    if (epoch === undefined || !isDatabaseEpoch(epoch.epoch)) throw new Error("D1 database epoch is missing or invalid");
    if (meta.library_manifest_json === null || meta.state_manifest_json === null) {
      throw new Error("Stored chunked snapshot manifest is missing");
    }
    let parsedLibrary: unknown;
    let parsedState: unknown;
    try {
      parsedLibrary = JSON.parse(meta.library_manifest_json) as unknown;
      parsedState = JSON.parse(meta.state_manifest_json) as unknown;
    } catch {
      throw new Error("Stored chunked snapshot manifest is invalid JSON");
    }
    let snapshot: ChunkedSnapshotContent;
    try {
      snapshot = validateChunkedSnapshot({
        format: "chunked-v1",
        state_version: meta.state_version,
        reported_at: meta.reported_at,
        library_revision: meta.library_revision,
        library: parsedLibrary,
        state: parsedState,
      });
    } catch {
      throw new Error("Stored chunked snapshot manifest is malformed");
    }
    const [libraryHash, stateHash] = await Promise.all([
      hashSnapshotManifest("library", normalizedManifestJson(snapshot.library)),
      hashSnapshotManifest("state", normalizedManifestJson(snapshot.state)),
    ]);
    if (libraryHash !== meta.library_hash || stateHash !== meta.state_hash ||
      snapshot.library.chunks.length !== meta.library_chunk_count ||
      snapshot.state.chunks.length !== meta.state_chunk_count) {
      throw new Error("Stored chunked snapshot manifest integrity check failed");
    }
    if (!chunksResult?.success) throw new Error("D1 snapshot chunks read failed");
    if (chunksResult.results.length !== 0) throw new Error("Legacy snapshot chunks remain beside a chunked snapshot");
    return {
      transport: "chunked-v1",
      format: "chunked-v1",
      database_epoch: epoch.epoch,
      synced: true,
      state_version: meta.state_version,
      library_revision: meta.library_revision,
      reported_at: meta.reported_at,
      last_seen: meta.received_at,
      stale: Date.now() - meta.received_at > SNAPSHOT_STALE_AFTER_MS,
      library: snapshot.library,
      state: snapshot.state,
    };
  }

  if (!chunksResult?.success) throw new Error("D1 snapshot chunks read failed");
  const chunks = chunksResult.results as SnapshotChunkRow[];
  const libraryChunks = chunks.filter((chunk) => chunk.kind === "library");
  const stateChunks = chunks.filter((chunk) => chunk.kind === "state");
  if (libraryChunks.length !== meta.library_chunk_count || stateChunks.length !== meta.state_chunk_count) {
    throw new Error("Stored snapshot chunk count mismatch");
  }

  const libraryJson = libraryChunks.map((chunk) => chunk.data).join("");
  const stateJson = stateChunks.map((chunk) => chunk.data).join("");
  const [libraryHash, stateHash] = await Promise.all([sha256Hex(libraryJson), sha256Hex(stateJson)]);
  if (libraryHash !== meta.library_hash || stateHash !== meta.state_hash) {
    throw new Error("Stored snapshot hash mismatch");
  }

  const library = JSON.parse(libraryJson) as unknown;
  const state = JSON.parse(stateJson) as unknown;
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
