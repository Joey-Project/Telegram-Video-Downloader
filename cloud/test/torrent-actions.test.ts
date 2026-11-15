import { SELF } from "cloudflare:test";
import { env } from "cloudflare:workers";
import type { D1Migration } from "@cloudflare/vitest-plugin";
import { describe, expect, it } from "vitest";
import { validateTorrentActionPayload } from "../src/torrent_actions";
import { ACTION_REPLAY_WINDOW_MS } from "../src/validation";
import { syntheticCredentials } from "./fixtures/synthetic-credentials";

const testEnv = env as typeof env & { TEST_MIGRATIONS: D1Migration[] };
const ownerId = 123456789;
const hash = "a".repeat(40);
const review = { operation: "review", task_id: "task_1", inventory_revision: 1,
  selection_revision: 2, selected_indexes: [3, 1] };
const encoder = new TextEncoder();

async function initData(userId = ownerId, chatType = "private"): Promise<string> {
  const params = new URLSearchParams({ auth_date: String(Math.floor(Date.now() / 1000)),
    chat_type: chatType, user: JSON.stringify({ id: userId, is_bot: false }) });
  const check = [...params.entries()].sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${value}`).join("\n");
  const derivation = await crypto.subtle.importKey("raw", encoder.encode("WebAppData"),
    { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const secret = await crypto.subtle.sign("HMAC", derivation, encoder.encode(syntheticCredentials.botToken.value));
  const key = await crypto.subtle.importKey("raw", secret, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const digest = new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(check)));
  params.set("hash", [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join(""));
  return params.toString();
}

function action(payload: Record<string, unknown>) {
  const queuedAt = Date.now();
  return { request_id: crypto.randomUUID(), kind: "torrent_action", payload,
    queued_at_ms: queuedAt, replay_expires_at_ms: queuedAt + ACTION_REPLAY_WINDOW_MS };
}

async function submit(body: unknown, signed = initData()): Promise<Response> {
  return SELF.fetch("https://worker.test/api/actions", { method: "POST",
    headers: { "content-type": "application/json", "X-Telegram-Init-Data": await signed },
    body: JSON.stringify(body) });
}

describe("torrent Mini App payload boundary", () => {
  it("admits only documented operations and normalizes bounded defaults", () => {
    expect(validateTorrentActionPayload({ operation: "catalog" })).toEqual({ operation: "catalog", page: 0, query: "" });
    expect(validateTorrentActionPayload({ operation: "detail", hash })).toEqual({
      operation: "detail", hash, page: 0, folder_id: 0, query: "", filter: "music",
    });
    expect(validateTorrentActionPayload({ ...review, selected_indexes: [] }).selected_indexes).toEqual([]);
    expect(validateTorrentActionPayload(review).selected_indexes).toEqual([1, 3]);
    expect(validateTorrentActionPayload({ operation: "confirm", review_seq: 1, review_id: "review_1" }))
      .toEqual({ operation: "confirm", review_seq: 1, review_id: "review_1" });
    expect(validateTorrentActionPayload({ operation: "control", hash, control: "pause" }).control).toBe("pause");
  });

  it("supports a complete 5000-file selection and multilingual searches", () => {
    const indexes = Array.from({ length: 5000 }, (_, index) => index);
    expect(validateTorrentActionPayload({ ...review, selected_indexes: indexes }).selected_indexes).toEqual(indexes);
    const query = "音楽 音乐 음악 Музыка Français Italiano";
    expect(validateTorrentActionPayload({ operation: "catalog", query }).query).toBe(query);
  });

  it.each([
    { operation: "catalog", actor_user_id: String(ownerId) },
    { operation: "catalog", chat_id: ownerId },
    { operation: "catalog", page: -1 },
    { operation: "catalog", page: 20_001 },
    { operation: "catalog", query: "音".repeat(86) },
    { operation: "catalog", query: "album\nother" },
    { operation: "detail" },
    { operation: "detail", task_id: "task_1", hash },
    { operation: "detail", task_id: "../outside" },
    { operation: "detail", hash: hash.toUpperCase() },
    { operation: "detail", hash, folder_id: 10_001 },
    { operation: "detail", hash, filter: "arbitrary" },
    { operation: "detail", hash, save_path: "/private" },
    { ...review, inventory_revision: 0 },
    { ...review, selection_revision: Number.MAX_SAFE_INTEGER + 1 },
    { ...review, selected_indexes: [1, 1] },
    { ...review, selected_indexes: [5000] },
    { ...review, selected_indexes: [-1] },
    { ...review, selected_indexes: [1.5] },
    { ...review, selected_indexes: Array(5001).fill(1) },
    { operation: "confirm", review_seq: 0, review_id: "review_1" },
    { operation: "confirm", review_seq: 1, review_id: "review_1", selected_indexes: [1] },
    { operation: "control", hash, control: "delete" },
    { operation: "control", hash, control: "resume", password: 1 },
  ])("rejects malformed or client-owned authority before local execution: %j", (payload) => {
    expect(() => validateTorrentActionPayload(payload)).toThrow();
  });
});

describe("torrent Mini App Worker ingress", () => {
  it("injects only the authenticated actor, deduplicates replay, and permits native local ACK", async () => {
    const request = action(review);
    const first = await submit(request);
    expect(first.status).toBe(202);
    const firstBody = await first.json() as { seq: number };
    const duplicate = await submit(request);
    expect(duplicate.status).toBe(202);
    expect(await duplicate.json()).toMatchObject({ seq: firstBody.seq, duplicate: true });
    const headers = { authorization: `Bearer ${syntheticCredentials.localSharedSecret.value}`,
      "X-Cloud-Database-Epoch": "0123456789abcdef0123456789abcdef" };
    const page = await SELF.fetch("https://worker.test/api/local/requests", { headers });
    expect(page.status).toBe(200);
    const pending = await page.json() as { requests: Array<Record<string, unknown>> };
    expect(pending.requests).toHaveLength(1);
    expect(pending.requests[0]).toMatchObject({ kind: "torrent_action", payload: {
      actor_user_id: String(ownerId), selected_indexes: [1, 3], task_id: "task_1",
    } });
    const stored = await testEnv.DB.prepare("SELECT seq, dedup_key AS request_id, payload_hash, kind, created_at FROM inbox WHERE seq = ?")
      .bind(firstBody.seq).first();
    const acknowledged = await SELF.fetch("https://worker.test/api/local/ack", { method: "POST",
      headers: { ...headers, "content-type": "application/json" }, body: JSON.stringify({ receipts: [stored] }) });
    expect(acknowledged.status).toBe(200);
    expect(await acknowledged.json()).toEqual({ acked: 1 });
  });

  it("rejects client actor spoofing, non-owner and group context without creating inbox rows", async () => {
    expect((await submit(action({ operation: "catalog", actor_user_id: "987654321" }))).status).toBe(400);
    expect((await submit(action({ operation: "catalog" }), initData(ownerId + 1))).status).toBe(403);
    expect((await submit(action({ operation: "catalog" }), initData(ownerId, "group"))).status).toBe(403);
    expect(await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first()).toEqual({ count: 0 });
  });

  it("rejects request ID reuse for a different selection", async () => {
    const request = action(review);
    expect((await submit(request)).status).toBe(202);
    expect((await submit({ ...request, payload: { ...review, selected_indexes: [4] } })).status).toBe(409);
  });

  it("preserves old receipts and the deleted sequence high-water mark through the kind migration", async () => {
    await testEnv.DB.prepare(`CREATE TABLE torrent_migration_probe (
      seq INTEGER PRIMARY KEY AUTOINCREMENT, dedup_key TEXT NOT NULL UNIQUE, payload_hash TEXT NOT NULL,
      kind TEXT NOT NULL CHECK(kind IN ('telegram','library_scan')), payload_json TEXT NOT NULL,
      created_at INTEGER NOT NULL, acked_at INTEGER
    )`).run();
    await testEnv.DB.batch([
      testEnv.DB.prepare("INSERT INTO torrent_migration_probe VALUES (6,'pending','hash-a','telegram','{}',1,NULL)"),
      testEnv.DB.prepare("INSERT INTO torrent_migration_probe VALUES (31,'acked','hash-b','library_scan','{}',2,3)"),
      testEnv.DB.prepare("INSERT INTO torrent_migration_probe VALUES (47,'removed','hash-c','telegram','{}',2,3)"),
      testEnv.DB.prepare("DELETE FROM torrent_migration_probe WHERE seq = 47"),
    ]);
    const migration = testEnv.TEST_MIGRATIONS.find((candidate) => candidate.name.startsWith("0008_"));
    expect(migration).toBeDefined();
    for (const query of migration!.queries) {
      await testEnv.DB.prepare(query.replaceAll("inbox_with_torrent_action_kind", "torrent_migration_next")
        .replaceAll("inbox_sequence_backup", "torrent_migration_sequence_backup")
        .replaceAll("inbox_pending_seq", "torrent_migration_pending_seq")
        .replace(/\binbox\b/gu, "torrent_migration_probe")).run();
    }
    const rows = await testEnv.DB.prepare("SELECT seq, dedup_key, kind, acked_at FROM torrent_migration_probe ORDER BY seq").all();
    expect(rows.results).toEqual([{ seq: 6, dedup_key: "pending", kind: "telegram", acked_at: null },
      { seq: 31, dedup_key: "acked", kind: "library_scan", acked_at: 3 }]);
    expect(await testEnv.DB.prepare("INSERT INTO torrent_migration_probe (dedup_key,payload_hash,kind,payload_json,created_at) VALUES ('next','hash-d','torrent_action','{}',4) RETURNING seq").first())
      .toEqual({ seq: 48 });
  });
});
