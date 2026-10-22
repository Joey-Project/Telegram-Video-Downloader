import { SELF } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import { syntheticCredentials } from "./fixtures/synthetic-credentials";
import {
  enqueue,
  getDatabaseEpoch,
  readSnapshot,
  readSnapshotContentChunk,
  rotateDatabaseEpochOnSequenceConflict,
  storeSnapshot,
} from "../src/store";
import type { LocalSnapshot } from "../src/types";
import {
  canonicalJson,
  MAX_CHUNKED_SNAPSHOT_MANIFEST_BODY_BYTES,
  MAX_STATE_BODY_BYTES,
  sha256Hex,
} from "../src/validation";

const testEnv = env as typeof env & { DB: D1Database };
const localSecret = syntheticCredentials.localSharedSecret.value;
const workerUrl = "https://worker.test";
const encoder = new TextEncoder();

function localHeaders(databaseEpoch: string): HeadersInit {
  return {
    authorization: `Bearer ${localSecret}`,
    "X-Cloud-Database-Epoch": databaseEpoch,
    "content-type": "application/json",
  };
}

async function postState(body: string): Promise<Response> {
  const databaseEpoch = await getDatabaseEpoch(testEnv.DB);
  return SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
    method: "POST",
    headers: localHeaders(databaseEpoch),
    body,
  }));
}

function preparedEnvelope(
  stateVersion: number,
  libraryJson: string,
  stateJson: string,
  reportedAt = Date.now(),
): string {
  return JSON.stringify({
    format: "prepared-v1",
    state_version: stateVersion,
    reported_at: reportedAt,
    library_json: libraryJson,
    state_json: stateJson,
  });
}

function validPreparedParts(): { libraryJson: string; stateJson: string } {
  return {
    libraryJson: JSON.stringify({
      warnings: [],
      ready: true,
      revision: "prepared-library-1",
      items: [],
      categories: [],
      scanned_at: 1_800_000_000,
    }),
    stateJson: JSON.stringify({
      settings: { download_dir: "/Users/test/Videos" },
      tasks: [],
      operations: [],
      previews: [],
      revision: "prepared-state-1",
    }),
  };
}

async function postContentChunk(bytes: Uint8Array, sha256: string): Promise<Response> {
  const databaseEpoch = await getDatabaseEpoch(testEnv.DB);
  const body = new Uint8Array(bytes.byteLength);
  body.set(bytes);
  return SELF.fetch(new Request(`${workerUrl}/api/local/state/chunks/${sha256}`, {
    method: "POST",
    headers: { ...localHeaders(databaseEpoch), "content-type": "application/octet-stream" },
    body: body.buffer,
  }));
}

async function uploadChunk(data: string): Promise<{ sha256: string; bytes: number }> {
  const bytes = encoder.encode(data);
  const sha256 = await sha256Hex(data);
  const response = await postContentChunk(bytes, sha256);
  if (response.status !== 200) throw new Error(`Chunk upload failed with HTTP ${response.status}`);
  return { sha256, bytes: bytes.byteLength };
}

async function seedOldOrphanChunks(epoch: string, count: number, createdAt: number, label: string): Promise<void> {
  const chunks = await Promise.all(Array.from({ length: count }, async (_, index) => {
    const data = `${label}-${index}`;
    return { data, sha256: await sha256Hex(data), bytes: encoder.encode(data).byteLength };
  }));
  await testEnv.DB.batch(chunks.map(({ data, sha256, bytes }) => testEnv.DB.prepare(
    `INSERT INTO snapshot_content_chunks (database_epoch, sha256, byte_length, data, created_at)
     VALUES (?, ?, ?, ?, ?)`,
  ).bind(epoch, sha256, bytes, data, createdAt)));
}

async function countOldOrphanChunks(
  epoch: string,
  createdAt: number,
  activeHashes: [string, string],
): Promise<number> {
  const row = await testEnv.DB.prepare(
    `SELECT COUNT(*) AS count
     FROM snapshot_content_chunks
     WHERE database_epoch = ? AND created_at = ? AND sha256 NOT IN (?, ?)`,
  ).bind(epoch, createdAt, ...activeHashes).first<{ count: number }>();
  return row?.count ?? 0;
}

function chunkManifest(chunks: Array<{ sha256: string; bytes: number }>): {
  bytes: number;
  chunks: Array<{ sha256: string; bytes: number }>;
} {
  return { bytes: chunks.reduce((total, chunk) => total + chunk.bytes, 0), chunks };
}

function chunkedEnvelope(
  stateVersion: number,
  libraryChunks: Array<{ sha256: string; bytes: number }>,
  stateChunks: Array<{ sha256: string; bytes: number }>,
  reportedAt = Date.now(),
  libraryRevision = "chunked-library-1",
): string {
  return JSON.stringify({
    format: "chunked-v1",
    state_version: stateVersion,
    reported_at: reportedAt,
    library_revision: libraryRevision,
    library: chunkManifest(libraryChunks),
    state: chunkManifest(stateChunks),
  });
}

describe("snapshot storage", () => {
  it("hashes and stores the exact prepared JSON bytes, and preserves version conflicts", async () => {
    const { libraryJson, stateJson } = validPreparedParts();
    const firstBody = preparedEnvelope(1, libraryJson, stateJson);
    expect(encoder.encode(firstBody).byteLength).toBeLessThan(MAX_STATE_BODY_BYTES);

    const first = await postState(firstBody);
    expect(first.status).toBe(200);
    expect(await first.json()).toEqual({ stored: true, last_seen: expect.any(Number) });

    const storedChunks = await testEnv.DB.prepare(
      "SELECT kind, part, data FROM snapshot_chunks ORDER BY kind ASC, part ASC",
    ).all<{ kind: string; part: number; data: string }>();
    expect(storedChunks.results.filter((row) => row.kind === "library").map((row) => row.data).join("")).toBe(libraryJson);
    expect(storedChunks.results.filter((row) => row.kind === "state").map((row) => row.data).join("")).toBe(stateJson);

    const metadata = await testEnv.DB.prepare(
      "SELECT library_hash, state_hash FROM snapshot_meta WHERE id = 1",
    ).first<{ library_hash: string; state_hash: string }>();
    expect(metadata?.library_hash).toBe(await sha256Hex(libraryJson));
    expect(metadata?.state_hash).toBe(await sha256Hex(stateJson));
    const storedPrepared = await readSnapshot(testEnv.DB);
    expect(storedPrepared?.transport ?? "legacy").toBe("legacy");
    expect(storedPrepared?.library).toMatchObject({ revision: "prepared-library-1" });

    const identicalRetry = await postState(preparedEnvelope(1, libraryJson, stateJson, Date.now() + 1_000));
    expect(identicalRetry.status).toBe(200);

    const changedState = JSON.parse(stateJson) as Record<string, unknown>;
    changedState.operations = [{ seq: 1, status: "changed" }];
    const conflict = await postState(preparedEnvelope(1, libraryJson, JSON.stringify(changedState)));
    expect(conflict.status).toBe(409);
    expect(conflict.headers.get("X-Cloud-State-Version")).toBe("1");
  });

  it("does not decode storage chunks for identical, conflicting, or stale version paths", async () => {
    const base = {
      state_version: 2,
      reported_at: Date.now(),
      library: {
        revision: "legacy-library-2",
        scanned_at: 1_800_000_000,
        root_label: "Videos",
        items: [],
        categories: [],
        warnings: [],
        ready: true,
      },
      previews: [],
      operations: [],
      tasks: [],
      settings: { download_dir: "/Users/test/Videos" },
    } satisfies LocalSnapshot;
    await storeSnapshot(testEnv.DB, base);

    const decode = vi.spyOn(TextDecoder.prototype, "decode");
    try {
      await storeSnapshot(testEnv.DB, { ...base, reported_at: base.reported_at + 1 });
      await expect(storeSnapshot(testEnv.DB, {
        ...base,
        operations: [{ seq: 1, status: "changed" }],
      })).rejects.toMatchObject({ status: 409 });
      await expect(storeSnapshot(testEnv.DB, { ...base, state_version: 1 })).rejects.toMatchObject({ status: 409 });
      expect(decode).not.toHaveBeenCalled();
    } finally {
      decode.mockRestore();
    }
  });

  it("splits UTF-8 on scalar boundaries while hashing the original canonical bytes", async () => {
    const libraryWithoutPadding = {
      revision: "unicode-library",
      scanned_at: 1_800_000_000,
      root_label: "Videos",
      items: [],
      categories: [],
      warnings: [],
      ready: true,
      aa_padding: "",
      marker: "🙂",
    };
    const markerByteOffset = encoder.encode(canonicalJson(libraryWithoutPadding).split("🙂", 1)[0] ?? "").byteLength;
    const paddingLength = 499_999 - markerByteOffset;
    expect(paddingLength).toBeGreaterThan(0);
    const library = { ...libraryWithoutPadding, aa_padding: "x".repeat(paddingLength) };
    const libraryJson = canonicalJson(library);
    const markerByteStart = encoder.encode(libraryJson.split("🙂", 1)[0] ?? "").byteLength;
    expect(markerByteStart).toBe(499_999);

    const snapshot = {
      state_version: 1,
      reported_at: Date.now(),
      library,
      previews: [],
      operations: [],
      tasks: [],
      settings: { download_dir: "/Users/test/Videos" },
    } as unknown as LocalSnapshot;
    await storeSnapshot(testEnv.DB, snapshot);

    const chunks = await testEnv.DB.prepare(
      "SELECT data FROM snapshot_chunks WHERE kind = 'library' ORDER BY part ASC",
    ).all<{ data: string }>();
    expect(chunks.results).toHaveLength(2);
    expect(chunks.results.map((chunk) => chunk.data).join("")).toBe(libraryJson);
    expect(chunks.results[0]?.data.endsWith('"marker":"')).toBe(true);
    expect(chunks.results[1]?.data.startsWith("🙂")).toBe(true);
    const storedUnicode = await readSnapshot(testEnv.DB);
    expect(storedUnicode?.transport ?? "legacy").toBe("legacy");
    expect((storedUnicode?.library as Record<string, unknown> | undefined)?.marker).toBe("🙂");
  });

  it("retains the raw body limit for prepared snapshots", async () => {
    const oversized = JSON.stringify({
      format: "prepared-v1",
      state_version: 1,
      reported_at: Date.now(),
      library_json: `"${"x".repeat(MAX_STATE_BODY_BYTES)}"`,
      state_json: "{}",
    });
    expect(encoder.encode(oversized).byteLength).toBeGreaterThan(MAX_STATE_BODY_BYTES);

    const response = await postState(oversized);
    expect(response.status).toBe(413);
    expect(await response.json()).toEqual({ error: "Request body is too large" });
  });

  it("enforces the existing JSON depth and node bounds on prepared parts", async () => {
    const { libraryJson: _libraryJson, stateJson } = validPreparedParts();
    let nested: unknown = null;
    for (let depth = 0; depth < 49; depth += 1) nested = { nested };
    const deepLibrary = JSON.stringify({
      revision: "deep-library",
      scanned_at: 1_800_000_000,
      items: [nested],
      categories: [],
      warnings: [],
    });
    const response = await postState(preparedEnvelope(1, deepLibrary, stateJson));
    expect(response.status).toBe(413);
    expect(await response.json()).toEqual({ error: "JSON structure is too complex" });
  });

  it("rejects prepared parts beyond the existing node bound", async () => {
    const { stateJson } = validPreparedParts();
    const libraryJson = JSON.stringify({
      revision: "large-library",
      scanned_at: 1_800_000_000,
      items: Array.from({ length: 1_000_000 }, () => null),
      categories: [],
      warnings: [],
    });
    const body = preparedEnvelope(1, libraryJson, stateJson);
    expect(encoder.encode(body).byteLength).toBeLessThan(MAX_STATE_BODY_BYTES);

    const response = await postState(body);
    expect(response.status).toBe(413);
    expect(await response.json()).toEqual({ error: "JSON structure is too complex" });
  });

  it("stores only valid UTF-8 content at its verified address and preserves a leading BOM", async () => {
    const data = "\uFEFFchunk🙂";
    const bytes = encoder.encode(data);
    const hash = await sha256Hex(data);
    const stored = await postContentChunk(bytes, hash);
    expect(stored.status).toBe(200);
    expect(await stored.json()).toEqual({ stored: true, sha256: hash });
    expect((await postContentChunk(bytes, hash)).status).toBe(200);

    const row = await testEnv.DB.prepare(
      "SELECT byte_length, data FROM snapshot_content_chunks WHERE database_epoch = ? AND sha256 = ?",
    ).bind(await getDatabaseEpoch(testEnv.DB), hash).first<{ byte_length: number; data: string }>();
    expect(row).toEqual({ byte_length: bytes.byteLength, data });

    const wrongAddress = await postContentChunk(encoder.encode("different bytes"), "0".repeat(64));
    const invalidUtf8 = await postContentChunk(new Uint8Array([0xff, 0xfe]), "1".repeat(64));
    const oversized = await postContentChunk(encoder.encode("x".repeat(32 * 1024 + 1)), "2".repeat(64));
    expect(wrongAddress.status).toBe(400);
    expect(invalidUtf8.status).toBe(400);
    expect(oversized.status).toBe(413);
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM snapshot_content_chunks").first<{ count: number }>())?.count)
      .toBe(1);
  });

  it("commits UTF-8-safe manifests, advances identical content to a newer version, and checks heartbeat refs", async () => {
    const libraryText = '{"items":["🙂"],"tail":"end"}';
    const emojiOffset = libraryText.indexOf("🙂");
    const libraryParts = [libraryText.slice(0, emojiOffset), "🙂", libraryText.slice(emojiOffset + 2)];
    const libraryChunks = await Promise.all(libraryParts.map(uploadChunk));
    const stateChunks = await Promise.all(['{"settings":{},"tasks":[]}'].map(uploadChunk));
    const firstBody = chunkedEnvelope(1, libraryChunks, stateChunks);
    expect(encoder.encode(firstBody).byteLength).toBeLessThan(MAX_CHUNKED_SNAPSHOT_MANIFEST_BODY_BYTES);

    const first = await postState(firstBody);
    expect(first.status).toBe(200);
    const advancedVersion = await postState(chunkedEnvelope(2, libraryChunks, stateChunks));
    expect(advancedVersion.status).toBe(200);
    expect((await testEnv.DB.prepare("SELECT state_version, transport FROM snapshot_meta WHERE id = 1")
      .first<{ state_version: number; transport: string }>())).toEqual({ state_version: 2, transport: "chunked-v1" });

    const retry = await postState(chunkedEnvelope(2, libraryChunks, stateChunks, Date.now() + 1_000));
    expect(retry.status).toBe(200);
    const changedStateChunks = [await uploadChunk('{"settings":{"changed":true},"tasks":[]}')];
    const conflict = await postState(chunkedEnvelope(2, libraryChunks, changedStateChunks));
    expect(conflict.status).toBe(409);
    expect(conflict.headers.get("X-Cloud-State-Version")).toBe("2");

    const oldVersion = await postState(firstBody);
    expect(oldVersion.status).toBe(409);
    expect(oldVersion.headers.get("X-Cloud-State-Version")).toBe("2");

    await testEnv.DB.prepare(
      "DELETE FROM snapshot_content_chunks WHERE database_epoch = ? AND sha256 = ?",
    ).bind(await getDatabaseEpoch(testEnv.DB), stateChunks[0]!.sha256).run();
    const missingHeartbeatChunk = await postState(chunkedEnvelope(2, libraryChunks, stateChunks));
    expect(missingHeartbeatChunk.status).toBe(428);
  });

  it("drains bounded old orphans on identical heartbeats while preserving active chunks", async () => {
    const libraryText = '{"revision":"heartbeat-gc-library","items":[]}';
    const stateText = '{"settings":{"heartbeat_gc":true},"tasks":[]}';
    const libraryChunks = [await uploadChunk(libraryText)];
    const stateChunks = [await uploadChunk(stateText)];
    expect((await postState(chunkedEnvelope(1, libraryChunks, stateChunks))).status).toBe(200);

    const epoch = await getDatabaseEpoch(testEnv.DB);
    const staleAt = Date.now() - 2 * 60 * 60 * 1000;
    await seedOldOrphanChunks(epoch, 40, staleAt, `heartbeat-gc-${Date.now()}`);
    const activeHashes: [string, string] = [libraryChunks[0]!.sha256, stateChunks[0]!.sha256];
    await testEnv.DB.prepare(
      "UPDATE snapshot_content_chunks SET created_at = ? WHERE database_epoch = ? AND sha256 IN (?, ?)",
    ).bind(staleAt, epoch, ...activeHashes).run();
    expect(await countOldOrphanChunks(epoch, staleAt, activeHashes)).toBe(40);

    expect((await postState(chunkedEnvelope(1, libraryChunks, stateChunks))).status).toBe(200);
    expect(await countOldOrphanChunks(epoch, staleAt, activeHashes)).toBe(8);

    expect((await postState(chunkedEnvelope(1, libraryChunks, stateChunks))).status).toBe(200);
    expect(await countOldOrphanChunks(epoch, staleAt, activeHashes)).toBe(0);
    await expect(readSnapshotContentChunk(testEnv.DB, epoch, activeHashes[0]))
      .resolves.toEqual({ sha256: activeHashes[0], data: libraryText });
    await expect(readSnapshotContentChunk(testEnv.DB, epoch, activeHashes[1]))
      .resolves.toEqual({ sha256: activeHashes[1], data: stateText });
  });

  it("does not prune chunks if the database epoch changes during a commit or heartbeat", async () => {
    const libraryText = '{"revision":"stale-gc-library","items":[]}';
    const stateText = '{"settings":{"stale_gc":true},"tasks":[]}';
    const libraryChunks = [await uploadChunk(libraryText)];
    const stateChunks = [await uploadChunk(stateText)];
    expect((await postState(chunkedEnvelope(1, libraryChunks, stateChunks))).status).toBe(200);

    const epoch = await getDatabaseEpoch(testEnv.DB);
    const staleAt = Date.now() - 2 * 60 * 60 * 1000;
    await seedOldOrphanChunks(epoch, 40, staleAt, `stale-gc-${Date.now()}`);
    const activeHashes: [string, string] = [libraryChunks[0]!.sha256, stateChunks[0]!.sha256];
    await testEnv.DB.prepare(
      "UPDATE snapshot_content_chunks SET created_at = ? WHERE database_epoch = ? AND sha256 IN (?, ?)",
    ).bind(staleAt, epoch, ...activeHashes).run();
    expect(await countOldOrphanChunks(epoch, staleAt, activeHashes)).toBe(40);

    try {
      await testEnv.DB.prepare(
        `CREATE TRIGGER rotate_epoch_after_snapshot_commit
         AFTER UPDATE OF state_version ON snapshot_meta
         WHEN NEW.state_version <> OLD.state_version
         BEGIN
           UPDATE cloud_database_meta
           SET epoch = CASE
             WHEN epoch = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' THEN 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
             ELSE 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
           END
           WHERE id = 1;
         END`,
      ).run();
      const commitAfterEpochChange = await postState(chunkedEnvelope(2, libraryChunks, stateChunks));
      expect(commitAfterEpochChange.status).toBe(409);
      expect(await countOldOrphanChunks(epoch, staleAt, activeHashes)).toBe(40);

      await testEnv.DB.prepare("DROP TRIGGER rotate_epoch_after_snapshot_commit").run();
      await testEnv.DB.prepare("UPDATE cloud_database_meta SET epoch = ? WHERE id = 1").bind(epoch).run();
      await testEnv.DB.prepare("UPDATE snapshot_meta SET received_at = ? WHERE id = 1")
        .bind(Date.now() - 60_000).run();
      await testEnv.DB.prepare(
        `CREATE TRIGGER rotate_epoch_after_snapshot_heartbeat
         AFTER UPDATE OF received_at ON snapshot_meta
         WHEN NEW.received_at <> OLD.received_at
         BEGIN
           UPDATE cloud_database_meta
           SET epoch = CASE
             WHEN epoch = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' THEN 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
             ELSE 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
           END
           WHERE id = 1;
         END`,
      ).run();
      const heartbeatAfterEpochChange = await postState(chunkedEnvelope(2, libraryChunks, stateChunks));
      expect(heartbeatAfterEpochChange.status).toBe(409);
      expect(await countOldOrphanChunks(epoch, staleAt, activeHashes)).toBe(40);
    } finally {
      await testEnv.DB.prepare("DROP TRIGGER IF EXISTS rotate_epoch_after_snapshot_commit").run();
      await testEnv.DB.prepare("DROP TRIGGER IF EXISTS rotate_epoch_after_snapshot_heartbeat").run();
      await testEnv.DB.prepare("UPDATE cloud_database_meta SET epoch = ? WHERE id = 1").bind(epoch).run();
    }
  });

  it("keeps manifest commits bounded, rejects missing refs, and accepts the 386-reference ceiling", async () => {
    const absentHash = await sha256Hex("not uploaded");
    const absent = await postState(chunkedEnvelope(
      1,
      [{ sha256: absentHash, bytes: encoder.encode("not uploaded").byteLength }],
      [{ sha256: absentHash, bytes: encoder.encode("not uploaded").byteLength }],
    ));
    expect(absent.status).toBe(428);

    const oversizedManifestBody = JSON.stringify({ format: "chunked-v1", padding: "x".repeat(MAX_CHUNKED_SNAPSHOT_MANIFEST_BODY_BYTES) });
    expect(encoder.encode(oversizedManifestBody).byteLength).toBeGreaterThan(MAX_CHUNKED_SNAPSHOT_MANIFEST_BODY_BYTES);
    expect(encoder.encode(oversizedManifestBody).byteLength).toBeLessThan(MAX_STATE_BODY_BYTES);
    const oversizedManifest = await postState(oversizedManifestBody);
    expect(oversizedManifest.status).toBe(413);

    const descriptor = await uploadChunk("x");
    const maximumLibrary = Array.from({ length: 385 }, () => descriptor);
    const atLimitBody = chunkedEnvelope(1, maximumLibrary, [descriptor]);
    expect(encoder.encode(atLimitBody).byteLength).toBeLessThan(MAX_CHUNKED_SNAPSHOT_MANIFEST_BODY_BYTES);
    const atLimit = await postState(atLimitBody);
    expect(atLimit.status).toBe(200);

    const overLimit = await postState(chunkedEnvelope(2, [...maximumLibrary, descriptor], [descriptor]));
    expect(overLimit.status).toBe(413);
  });

  it("allows only one concurrent writer to claim a state version", async () => {
    const libraryA = [await uploadChunk('{"revision":"race-a","items":[]}')];
    const stateA = [await uploadChunk('{"settings":{"writer":"a"},"tasks":[]}')];
    const libraryB = [await uploadChunk('{"revision":"race-b","items":[]}')];
    const stateB = [await uploadChunk('{"settings":{"writer":"b"},"tasks":[]}')];
    const [responseA, responseB] = await Promise.all([
      postState(chunkedEnvelope(1, libraryA, stateA)),
      postState(chunkedEnvelope(1, libraryB, stateB)),
    ]);
    expect([responseA.status, responseB.status].sort()).toEqual([200, 409]);
    expect(responseA.status === 200 ? responseB.headers.get("X-Cloud-State-Version") : responseA.headers.get("X-Cloud-State-Version"))
      .toBe("1");
    const stored = await readSnapshot(testEnv.DB);
    expect(stored?.transport).toBe("chunked-v1");
    expect(stored?.state_version).toBe(1);
    expect(stored?.transport === "chunked-v1" ? stored.library_revision : null).toMatch(/^chunked-library-1$/u);
  });

  it("detects chunk and manifest corruption when reading a chunked snapshot", async () => {
    const libraryText = '{"revision":"integrity","items":[]}';
    const stateText = '{"settings":{},"tasks":[]}';
    const libraryChunks = [await uploadChunk(libraryText)];
    const stateChunks = [await uploadChunk(stateText)];
    expect((await postState(chunkedEnvelope(1, libraryChunks, stateChunks))).status).toBe(200);

    const snapshot = await readSnapshot(testEnv.DB);
    expect(snapshot?.transport).toBe("chunked-v1");
    if (snapshot?.transport !== "chunked-v1") throw new Error("Expected a chunked snapshot");
    expect(snapshot.library.chunks).toEqual(libraryChunks);
    expect(snapshot.state.chunks).toEqual(stateChunks);

    await testEnv.DB.prepare(
      "UPDATE snapshot_content_chunks SET data = ? WHERE database_epoch = ? AND sha256 = ?",
    ).bind('{"revision":"tampered","items":[]}', await getDatabaseEpoch(testEnv.DB), libraryChunks[0]!.sha256).run();
    await expect(readSnapshotContentChunk(testEnv.DB, await getDatabaseEpoch(testEnv.DB), libraryChunks[0]!.sha256))
      .rejects.toThrow("integrity check failed");

    const badManifest = { ...snapshot.library, chunks: [{ ...libraryChunks[0]!, sha256: "f".repeat(64) }] };
    await testEnv.DB.prepare("UPDATE snapshot_meta SET library_manifest_json = ? WHERE id = 1")
      .bind(JSON.stringify(badManifest)).run();
    await expect(readSnapshot(testEnv.DB)).rejects.toThrow("manifest integrity check failed");
  });

  it("preserves legacy state on a failed chunked transition and clears its rows on success", async () => {
    const legacy: LocalSnapshot = {
      state_version: 1,
      reported_at: Date.now(),
      library: { revision: "legacy-one", scanned_at: 1_800_000_000, items: [], categories: [], warnings: [] },
      previews: [],
      operations: [],
      tasks: [],
      settings: { download_dir: "/Users/test/Videos" },
    };
    await storeSnapshot(testEnv.DB, legacy);
    const libraryChunks = [await uploadChunk('{"revision":"v2-library","items":[]}')];
    const stateChunks = [await uploadChunk('{"settings":{},"tasks":[]}')];

    await testEnv.DB.prepare(
      `CREATE TRIGGER reject_chunked_snapshot BEFORE INSERT ON snapshot_meta
       WHEN NEW.transport = 'chunked-v1'
       BEGIN SELECT RAISE(ABORT, 'test chunked snapshot rollback'); END`,
    ).run();
    const failedTransition = await postState(chunkedEnvelope(2, libraryChunks, stateChunks));
    expect(failedTransition.status).toBe(503);
    expect((await testEnv.DB.prepare("SELECT state_version, transport FROM snapshot_meta WHERE id = 1")
      .first<{ state_version: number; transport: string }>())).toEqual({ state_version: 1, transport: "legacy" });
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM snapshot_chunks").first<{ count: number }>())?.count)
      .toBeGreaterThan(0);

    await testEnv.DB.prepare("DROP TRIGGER reject_chunked_snapshot").run();
    expect((await postState(chunkedEnvelope(2, libraryChunks, stateChunks))).status).toBe(200);
    expect((await testEnv.DB.prepare("SELECT transport, library_manifest_json FROM snapshot_meta WHERE id = 1")
      .first<{ transport: string; library_manifest_json: string | null }>())?.transport).toBe("chunked-v1");
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM snapshot_chunks").first<{ count: number }>())?.count)
      .toBe(0);

    await storeSnapshot(testEnv.DB, { ...legacy, state_version: 3, reported_at: Date.now() + 1_000 });
    const metadata = await testEnv.DB.prepare(
      "SELECT transport, library_manifest_json, state_manifest_json FROM snapshot_meta WHERE id = 1",
    ).first<{ transport: string; library_manifest_json: string | null; state_manifest_json: string | null }>();
    expect(metadata).toEqual({ transport: "legacy", library_manifest_json: null, state_manifest_json: null });
  });

  it("prunes bounded stale content from retired database epochs", async () => {
    const oldEpoch = await getDatabaseEpoch(testEnv.DB);
    const oldChunk = await uploadChunk("retired orphan");
    await testEnv.DB.prepare(
      "UPDATE snapshot_content_chunks SET created_at = ? WHERE database_epoch = ? AND sha256 = ?",
    ).bind(Date.now() - 2 * 60 * 60 * 1000, oldEpoch, oldChunk.sha256).run();

    const pending = await enqueue(testEnv.DB, "action:snapshot-gc-rotation", "library_scan", { library_id: "Videos" });
    const nextEpoch = "abcdefabcdefabcdefabcdefabcdefab";
    const rotated = await rotateDatabaseEpochOnSequenceConflict(testEnv.DB, oldEpoch, nextEpoch, {
      seq: pending.envelope.seq,
      request_id: pending.envelope.request_id,
      payload_hash: pending.envelope.payload_hash,
      kind: pending.envelope.kind,
      created_at: pending.envelope.created_at,
    });
    expect(rotated.rotated).toBe(true);
    expect(rotated.database_epoch).toBe(nextEpoch);

    const libraryChunks = [await uploadChunk('{"revision":"new-epoch","items":[]}')];
    const stateChunks = [await uploadChunk('{"settings":{},"tasks":[]}')];
    expect((await postState(chunkedEnvelope(1, libraryChunks, stateChunks))).status).toBe(200);
    expect((await testEnv.DB.prepare(
      "SELECT COUNT(*) AS count FROM snapshot_content_chunks WHERE database_epoch = ?",
    ).bind(oldEpoch).first<{ count: number }>())?.count).toBe(0);
  });
});
