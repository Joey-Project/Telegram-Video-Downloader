import { applyD1Migrations } from "cloudflare:test";
import { env } from "cloudflare:workers";
import type { D1Migration } from "@cloudflare/vitest-plugin";
import { describe, expect, it } from "vitest";

const testEnv = env as typeof env & { TEST_MIGRATIONS: D1Migration[] };
const probeMigrationsTable = "metadata_recovery_probe_migrations";

function metadataRecoveryProbeMigration(queries: string[]): D1Migration {
  const source = testEnv.TEST_MIGRATIONS.find(({ name }) => name.includes("0005_metadata_recovery_kind"));
  if (!source) throw new Error("Migration 0005 metadata_recovery_kind was not loaded for the test");
  return {
    name: "0005_metadata_recovery_probe",
    queries: [
      ...source.queries.map((query) => query
        .replaceAll("inbox_with_metadata_recovery_kind", "inbox_probe_with_metadata_recovery_kind")
        .replaceAll("inbox_sequence_backup", "inbox_probe_sequence_backup")
        .replaceAll("inbox_pending_seq", "inbox_probe_pending_seq")
        .replace(/\binbox\b/gu, "inbox_probe")),
      ...queries,
    ],
  };
}

async function createOldInboxProbe() {
  await testEnv.DB.prepare(`CREATE TABLE inbox_probe (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    dedup_key TEXT NOT NULL UNIQUE,
    payload_hash TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('library_scan', 'file_preview')),
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    acked_at INTEGER
  )`).run();
}

describe("metadata recovery inbox migration", () => {
  it("preserves populated inbox fields and rolls back the failed migration atomically", async () => {
    await createOldInboxProbe();
    const originalRows = [
      {
        seq: 7,
        dedup_key: "action:preview-request",
        payload_hash: "preview-payload-hash",
        kind: "file_preview",
        payload_json: '{"library_id":"main","item_ids":["movie-a"],"metadata_patches":[{"item_id":"movie-a","title":"Recovered title"}]}',
        created_at: 1_800_000_001,
        acked_at: null,
      },
      {
        seq: 23,
        dedup_key: "action:scan-request",
        payload_hash: "scan-payload-hash",
        kind: "library_scan",
        payload_json: '{"library_id":"main"}',
        created_at: 1_800_000_002,
        acked_at: 1_800_000_003,
      },
    ];
    for (const row of originalRows) {
      await testEnv.DB.prepare(`INSERT INTO inbox_probe
        (seq, dedup_key, payload_hash, kind, payload_json, created_at, acked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`)
        .bind(row.seq, row.dedup_key, row.payload_hash, row.kind, row.payload_json, row.created_at, row.acked_at)
        .run();
    }
    await testEnv.DB.prepare("UPDATE sqlite_sequence SET seq = 41 WHERE name = 'inbox_probe'").run();

    const failed = metadataRecoveryProbeMigration(["INSERT INTO metadata_recovery_probe_failure (id) VALUES (1)"]);
    await expect(applyD1Migrations(testEnv.DB, [failed], probeMigrationsTable)).rejects.toThrow();

    const afterRollback = await testEnv.DB.prepare(`SELECT seq, dedup_key, payload_hash, kind, payload_json, created_at, acked_at
      FROM inbox_probe ORDER BY seq`).all<Record<string, unknown>>();
    expect(afterRollback.results).toEqual(originalRows);
    const probeTable = await testEnv.DB.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'inbox_probe_with_metadata_recovery_kind'").first();
    expect(probeTable).toBeNull();

    await applyD1Migrations(testEnv.DB, [metadataRecoveryProbeMigration([])], probeMigrationsTable);
    const preservedRows = await testEnv.DB.prepare(`SELECT seq, dedup_key, payload_hash, kind, payload_json, created_at, acked_at
      FROM inbox_probe ORDER BY seq`).all<Record<string, unknown>>();
    expect(preservedRows.results).toEqual(originalRows);

    const next = await testEnv.DB.prepare(`INSERT INTO inbox_probe
      (dedup_key, payload_hash, kind, payload_json, created_at)
      VALUES ('action:recovery-request', 'recovery-payload-hash', 'metadata_recover',
        '{"library_id":"main","scope":"library"}', 1_800_000_005)
      RETURNING seq`).first<{ seq: number }>();
    expect(next?.seq).toBe(42);

    const duplicate = await testEnv.DB.prepare(`INSERT INTO inbox_probe
      (dedup_key, payload_hash, kind, payload_json, created_at)
      VALUES (?, ?, ?, ?, ?) ON CONFLICT(dedup_key) DO NOTHING`)
      .bind("action:preview-request", "ignored-hash", "file_preview", "{}", 1_800_000_004)
      .run();
    expect(duplicate.meta.changes).toBe(0);
  });

  it("preserves an empty inbox's higher historical AUTOINCREMENT value", async () => {
    await createOldInboxProbe();
    await testEnv.DB.prepare(`INSERT INTO inbox_probe
      (seq, dedup_key, payload_hash, kind, payload_json, created_at)
      VALUES (47, 'action:old-request', 'old-hash', 'library_scan', '{}', 1)`).run();
    await testEnv.DB.prepare("DELETE FROM inbox_probe").run();
    await testEnv.DB.prepare("UPDATE sqlite_sequence SET seq = 89 WHERE name = 'inbox_probe'").run();

    await applyD1Migrations(testEnv.DB, [metadataRecoveryProbeMigration([])], probeMigrationsTable);

    const count = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox_probe").first<{ count: number }>();
    const savedSequence = await testEnv.DB.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'inbox_probe'").first<{ seq: number }>();
    expect(count?.count).toBe(0);
    expect(savedSequence?.seq).toBe(89);
    const next = await testEnv.DB.prepare(`INSERT INTO inbox_probe
      (dedup_key, payload_hash, kind, payload_json, created_at)
      VALUES ('action:recovery-request', 'recovery-payload-hash', 'metadata_recover',
        '{"library_id":"main","scope":"library"}', 1_800_000_005)
      RETURNING seq`).first<{ seq: number }>();
    expect(next?.seq).toBe(90);
  });
});
