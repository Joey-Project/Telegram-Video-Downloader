import { listDurableObjectIds, SELF } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import type { D1Migration } from "@cloudflare/vitest-plugin";
import { syntheticCredentials } from "./fixtures/synthetic-credentials";
import { ACTION_QUEUE_LIMIT, appendAction, removeQueuedAction } from "../public/file/action-queue.js";
import { getOrCreateLegacySelection, updateLegacySelection } from "../public/file/legacy-mapping-state.js";
import { metadataPreviewDetails } from "../public/file/metadata-preview.js";
import { buildWaitingEntries, mergeRequestEntries } from "../public/file/state-view.js";

const testEnv = env as typeof env & { TEST_MIGRATIONS: D1Migration[] };
const ownerId = 123456789;
const ownerToken = syntheticCredentials.localSharedSecret.value;
const webhookToken = syntheticCredentials.webhookSecret.value;
const botToken = syntheticCredentials.botToken.value;
const workerUrl = "https://worker.test";
const textEncoder = new TextEncoder();

describe("metadata preview details", () => {
  it("shows only concrete known-field changes and the source IDs to add", () => {
    expect(metadataPreviewDetails({
      changed: true,
      changes: [
        { field: "title", before: "Old title", after: "Confirmed title" },
        { field: "collection", before: null, after: "Series" },
        { field: "order", before: "1", after: "2" },
      ],
      added_source_ids: ["BV1abcdefgh", "cid11"],
    })).toEqual([
      { kind: "status", text: "NFO will be updated" },
      { kind: "change", label: "Title", before: "Old title", after: "Confirmed title" },
      { kind: "change", label: "Collection", before: null, after: "Series" },
      { kind: "change", label: "Order", before: "1", after: "2" },
      { kind: "change", label: "Source IDs to add", before: null, after: "BV1abcdefgh, cid11" },
    ]);
  });
});

function ownerMessage(updateId: number, text = "/scan", fromId = ownerId, chatId = ownerId, chatType = "private") {
  return {
    update_id: updateId,
    message: {
      message_id: updateId,
      date: Math.floor(Date.now() / 1000),
      from: { id: fromId, is_bot: false, first_name: "Owner" },
      chat: { id: chatId, type: chatType, first_name: "Owner" },
      text,
    },
  };
}

async function webhook(update: unknown, secret: string = webhookToken): Promise<Response> {
  return SELF.fetch(new Request(`${workerUrl}/webhook`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-telegram-bot-api-secret-token": secret },
    body: JSON.stringify(update),
  }));
}

function localHeaders(): HeadersInit {
  return { authorization: `Bearer ${ownerToken}` };
}

async function responseJson<T>(response: Response): Promise<T> {
  return await response.json() as T;
}

async function localRequests(limit = 100): Promise<Response> {
  return SELF.fetch(new Request(`${workerUrl}/api/local/requests?limit=${limit}`, { headers: localHeaders() }));
}

async function signInitData(options: { userId?: number; authDate?: number; chatType?: string; chat?: object } = {}): Promise<string> {
  const params = new URLSearchParams();
  params.set("auth_date", String(options.authDate ?? Math.floor(Date.now() / 1000)));
  params.set("chat_type", options.chatType ?? "private");
  params.set("user", JSON.stringify({ id: options.userId ?? ownerId, is_bot: false, first_name: "Owner" }));
  if (options.chat) params.set("chat", JSON.stringify(options.chat));
  const entries = [...params.entries()].sort(([left], [right]) => left.localeCompare(right));
  const checkString = entries.map(([key, value]) => `${key}=${value}`).join("\n");
  const derivationKey = await crypto.subtle.importKey("raw", textEncoder.encode("WebAppData"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const secret = await crypto.subtle.sign("HMAC", derivationKey, textEncoder.encode(botToken));
  const verificationKey = await crypto.subtle.importKey("raw", secret, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const digest = new Uint8Array(await crypto.subtle.sign("HMAC", verificationKey, textEncoder.encode(checkString)));
  params.set("hash", [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join(""));
  return params.toString();
}

async function ownerRequest(path: string, initData: string, options: RequestInit = {}): Promise<Response> {
  const headers = new Headers(options.headers ?? {});
  headers.set("x-telegram-init-data", initData);
  if (options.body !== undefined) headers.set("content-type", "application/json");
  return SELF.fetch(new Request(`${workerUrl}${path}`, { ...options, headers }));
}

function localSnapshot(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  const now = Date.now();
  return {
    state_version: 1,
    reported_at: now,
    library: {
      revision: "library-hash-a",
      scanned_at: Math.floor(now / 1000),
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
    ...overrides,
  };
}

describe("durable Telegram ingress", () => {
  it("requires the webhook secret and does not wake the notifier for unauthorized traffic", async () => {
    const response = await webhook(ownerMessage(4), "wrong-secret");
    expect(response.status).toBe(401);
    expect(await listDurableObjectIds(testEnv.NOTIFIER)).toHaveLength(0);
    expect((await localRequests()).status).toBe(200);
    expect(await responseJson<{ requests: unknown[] }>(await localRequests())).toEqual({ requests: [] });
  });

  it("ignores valid updates outside the configured owner's private message/callback path", async () => {
    const nonOwner = await webhook(ownerMessage(5, "/start", ownerId + 1, ownerId + 1));
    const group = await webhook(ownerMessage(6, "/start", ownerId, -100123, "group"));
    const unsupported = await webhook({ update_id: 7, inline_query: { id: "inline", from: { id: ownerId } } });
    const callback = await webhook({ update_id: 8, callback_query: { id: "cb", from: { id: ownerId, is_bot: false }, message: { from: { id: 999, is_bot: true }, chat: { id: ownerId, type: "private" } } } });

    expect(nonOwner.status).toBe(200);
    expect(group.status).toBe(200);
    expect(unsupported.status).toBe(200);
    expect(callback.status).toBe(200);
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toHaveLength(1);
    expect(await listDurableObjectIds(testEnv.NOTIFIER)).toHaveLength(1);
  });

  it("stores the raw owner update before acknowledging and assigns ordered cloud sequences", async () => {
    const first = await webhook(ownerMessage(9000, "/first"));
    const second = await webhook(ownerMessage(2, "/second"));
    expect(first.status).toBe(200);
    expect(second.status).toBe(200);

    const body = await (await localRequests()).json() as { requests: Array<{ seq: number; kind: string; payload: Record<string, unknown> }> };
    expect(body.requests.map((request) => request.seq)).toEqual([1, 2]);
    expect(body.requests.map((request) => request.kind)).toEqual(["telegram", "telegram"]);
    expect((body.requests[0]?.payload.message as { text: string }).text).toBe("/first");
    expect(body.requests[0]?.payload).not.toHaveProperty("update");
  });

  it("deduplicates identical Telegram updates and rejects changed payloads with the same update ID", async () => {
    expect((await webhook(ownerMessage(40, "/same"))).status).toBe(200);
    const duplicate = await webhook(ownerMessage(40, "/same"));
    const conflict = await webhook(ownerMessage(40, "/different"));
    expect(duplicate.status).toBe(200);
    expect((await responseJson<{ duplicate: boolean }>(duplicate)).duplicate).toBe(true);
    expect(conflict.status).toBe(409);
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toHaveLength(1);
  });

  it("returns success only after the D1 insert and never acknowledges a failed insert", async () => {
    const notifierIdsBefore = await listDurableObjectIds(testEnv.NOTIFIER);
    await testEnv.DB.exec("DROP TABLE inbox");
    const response = await webhook(ownerMessage(88));
    expect(response.status).toBe(503);
    expect(await listDurableObjectIds(testEnv.NOTIFIER)).toEqual(notifierIdsBefore);
  });

  it("provides an authenticated WebSocket hint while the D1 backlog remains the reconnect source of truth", async () => {
    const unauthorized = await SELF.fetch(new Request(`${workerUrl}/api/local/ws`, { headers: { upgrade: "websocket" } }));
    expect(unauthorized.status).toBe(401);

    const connected = await SELF.fetch(new Request(`${workerUrl}/api/local/ws`, { headers: { ...localHeaders(), upgrade: "websocket" } }));
    expect(connected.status).toBe(101);
    expect(connected.webSocket).not.toBeNull();

    await webhook(ownerMessage(99, "/backlog"));
    const firstRead = await (await localRequests()).json() as { requests: Array<{ seq: number }> };
    expect(firstRead.requests.map((request) => request.seq)).toEqual([1]);
    const ack = await SELF.fetch(new Request(`${workerUrl}/api/local/ack`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({ seqs: [1] }),
    }));
    expect(ack.status).toBe(200);
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toEqual([]);
  });
});

describe("Mini App identity and actions", () => {
  it("verifies signed Telegram initData, owner, private chat, freshness, and tamper resistance", async () => {
    const valid = await ownerRequest("/api/state", await signInitData());
    expect(valid.status).toBe(200);
    expect((await responseJson<{ synced: boolean }>(valid)).synced).toBe(false);

    const wrongOwner = await ownerRequest("/api/state", await signInitData({ userId: ownerId + 1 }));
    const group = await ownerRequest("/api/state", await signInitData({ chatType: "group" }));
    const stale = await ownerRequest("/api/state", await signInitData({ authDate: Math.floor(Date.now() / 1000) - 7 * 60 * 60 }));
    const signed = new URLSearchParams(await signInitData());
    signed.set("user", JSON.stringify({ id: ownerId + 1, is_bot: false }));
    const tampered = await ownerRequest("/api/state", signed.toString());
    expect(wrongOwner.status).toBe(403);
    expect(group.status).toBe(403);
    expect(stale.status).toBe(401);
    expect(tampered.status).toBe(401);
  });

  it("enqueues an idempotent validated action and detects request ID reuse across payload or kind", async () => {
    const initData = await signInitData();
    const action = {
      request_id: "00000000-0000-4000-8000-000000000001",
      kind: "file_preview",
      payload: { item_ids: ["item-a"], target_relative_dir: "", rename: false, conflict: "keep_both" },
    };
    const first = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify(action) });
    const duplicate = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify(action) });
    const changed = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...action, kind: "library_scan", payload: {} }) });
    expect(first.status).toBe(202);
    expect(duplicate.status).toBe(202);
    expect((await responseJson<{ duplicate: boolean }>(duplicate)).duplicate).toBe(true);
    expect(changed.status).toBe(409);
  });

  it("shows a cloud-accepted request while the desktop is offline without exposing its payload", async () => {
    const initData = await signInitData();
    const accepted = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000000010",
        kind: "file_preview",
        payload: { item_ids: ["private-item"], target_relative_dir: "Private/Target" },
      }),
    });
    expect(accepted.status).toBe(202);

    const state = await responseJson<{ synced: boolean; pending_requests: Array<Record<string, unknown>> }>(await ownerRequest("/api/state", initData));
    expect(state.synced).toBe(false);
    expect(state.pending_requests).toEqual([
      { seq: 1, kind: "file_preview", created_at: expect.any(Number), status: "waiting_for_local" },
    ]);
    expect(state.pending_requests[0]).not.toHaveProperty("payload");

    const desktopBacklog = await responseJson<{ requests: Array<{ payload: { item_ids: string[] } }> }>(await localRequests());
    expect(desktopBacklog.requests[0]?.payload.item_ids).toEqual(["private-item"]);
  });

  it("rejects unsafe destinations and malformed or oversized metadata patches", async () => {
    const initData = await signInitData();
    const base = { request_id: "00000000-0000-4000-8000-000000000002", kind: "file_preview", payload: {} };
    const unsafe = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, payload: { item_ids: ["item-a"], target_relative_dir: "../private" } }) });
    const invalidPatch = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, payload: { metadata_patches: [{ item_id: "item-a", hint_index: 1000, source_ids: ["source-a"] }] } }) });
    expect(unsafe.status).toBe(400);
    expect(invalidPatch.status).toBe(400);
  });

  it("accepts a manual metadata patch without a hint index and keeps metadata-only items exact", async () => {
    const initData = await signInitData();
    const accepted = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000000003",
        kind: "file_preview",
        payload: { metadata_patches: [{ item_id: "item-a", title: "Selected manually", source_ids: ["source-a"] }] },
      }),
    });
    const extraItem = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000000004",
        kind: "file_preview",
        payload: { item_ids: ["item-a", "item-b"], metadata_patches: [{ item_id: "item-a", title: "Selected manually" }] },
      }),
    });
    expect(accepted.status).toBe(202);
    expect(extraItem.status).toBe(400);
  });
});

describe("local client API and state snapshots", () => {
  it("requires the shared local authorization for every local endpoint", async () => {
    const response = await SELF.fetch(new Request(`${workerUrl}/api/local/requests`));
    expect(response.status).toBe(401);
  });

  it("stores the first incomplete scan health state without reporting an empty ready library", async () => {
    const owner = await signInitData();
    const initial = await ownerRequest("/api/state", owner);
    expect((await responseJson<{ synced: boolean }>(initial)).synced).toBe(false);

    const snapshot = localSnapshot({
      library: { revision: "not-scanned", scanned_at: 0, root_label: "Videos", items: [], categories: [], warnings: [], ready: false },
    });
    const stored = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(snapshot),
    }));
    expect(stored.status).toBe(200);
    const response = await ownerRequest("/api/state", owner);
    const state = await responseJson<{ synced: boolean; library: { ready: boolean; scanned_at: number }; last_seen: number }>(response);
    expect(state.synced).toBe(true);
    expect(state.library.ready).toBe(false);
    expect(state.library.scanned_at).toBe(0);
    expect(state.last_seen).toBeGreaterThan(0);
  });

  it("keeps state_version monotonic, accepts identical retries, and rejects same-version mutations", async () => {
    const sendState = (snapshot: Record<string, unknown>) => SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(snapshot),
    }));
    const firstBody = localSnapshot();
    const first = await sendState(firstBody);
    const retry = await sendState({ ...firstBody, reported_at: Number(firstBody.reported_at) + 5_000 });
    const changedSameVersion = await sendState({ ...firstBody, operations: [{ seq: 1, status: "done" }] });
    const oldVersion = await sendState({ ...firstBody, state_version: 0 });
    const newer = await sendState({ ...firstBody, state_version: 2, operations: [{ seq: 2, status: "done" }] });
    const lateOlder = await sendState(firstBody);
    expect(first.status).toBe(200);
    expect(retry.status).toBe(200);
    expect(changedSameVersion.status).toBe(409);
    expect(oldVersion.status).toBe(400);
    expect(newer.status).toBe(200);
    expect(lateOlder.status).toBe(409);
  });

  it.each([100, 500])("atomically acknowledges %i inbox sequences without exceeding D1's bind limit", async (count) => {
    for (let offset = 0; offset < count; offset += 90) {
      const rows = Array.from({ length: Math.min(90, count - offset) }, (_, index) => {
        const sequence = offset + index + 1;
        return testEnv.DB.prepare("INSERT INTO inbox (dedup_key, payload_hash, kind, payload_json, created_at) VALUES (?, ?, 'library_scan', '{}', ?)")
          .bind(`seed:${sequence}`, `hash-${sequence}`, Date.now());
      });
      await testEnv.DB.batch(rows);
    }
    const response = await SELF.fetch(new Request(`${workerUrl}/api/local/ack`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({ seqs: Array.from({ length: count }, (_, index) => index + 1) }),
    }));
    expect(response.status).toBe(200);
    expect((await responseJson<{ acked: number }>(response)).acked).toBe(count);
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox WHERE acked_at IS NOT NULL").first<{ count: number }>())?.count).toBe(count);
  });

  it("serves the static Mini App at the stable /file/ path", async () => {
    const response = await SELF.fetch(new Request(`${workerUrl}/file/`));
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("Video Library");
  });
});

describe("offline action queue helpers", () => {
  it("refuses a new action at the bound instead of silently discarding an older action", () => {
    const initial = Array.from({ length: ACTION_QUEUE_LIMIT }, (_, index) => ({ request_id: `request-${index}`, kind: "library_scan", payload: {} }));
    const result = appendAction(initial, { request_id: "request-new", kind: "file_preview", payload: {} });
    expect(result.ok).toBe(false);
    expect(result.queue).toEqual(initial);
    expect(result.queue).toHaveLength(ACTION_QUEUE_LIMIT);
  });

  it("removes only the acknowledged ID from a freshly read queue", () => {
    const first = { request_id: "request-first", kind: "library_scan", payload: {} };
    const concurrent = { request_id: "request-concurrent", kind: "file_preview", payload: { item_ids: ["item-a"] } };
    const queue = appendAction([first], concurrent).queue;
    expect(removeQueuedAction(queue, first.request_id)).toEqual([concurrent]);
  });

  it("keeps mapping choices across repeated legacy operation renders", () => {
    const selections = new Map();
    const initial = getOrCreateLegacySelection(selections, "legacy:4:manual:0", {
      selected: false,
      item_id: "",
      title: true,
      source_ids: true,
      collection: false,
    });
    updateLegacySelection(selections, "legacy:4:manual:0", { selected: true, item_id: "present-video" });
    updateLegacySelection(selections, "legacy:4:manual:0", { collection: true });

    const afterRender = getOrCreateLegacySelection(selections, "legacy:4:manual:0", {
      selected: false,
      item_id: "",
      title: true,
      source_ids: true,
      collection: false,
    });
    expect(afterRender).toBe(initial);
    expect(afterRender).toEqual({ selected: true, item_id: "present-video", title: true, source_ids: true, collection: true });
  });

  it("deduplicates waiting entries by sequence and keeps the richest local operation", () => {
    const merged = mergeRequestEntries(
      [{ seq: 1, kind: "file_confirm", status: "waiting_for_local" }, { seq: 2, kind: "legacy_import", status: "waiting_for_local" }],
      [{ seq: 1, kind: "file_confirm", status: "received" }],
      [{ seq: 1, kind: "file_confirm", status: "partial_failure", result: { items: [{ item_id: "video" }] } }],
    );
    expect(merged).toHaveLength(2);
    expect(merged[0]?.status).toBe("partial_failure");
    expect(merged[0]?.result).toEqual({ items: [{ item_id: "video" }] });
    expect(merged[1]?.seq).toBe(2);
  });

  it("renders a cloud-only request once and replaces a matching cloud summary with one local entry", () => {
    const cloudOnly = buildWaitingEntries(
      [{ seq: 7, kind: "file_preview", status: "waiting_for_local" }],
      [],
      [],
    );
    expect(cloudOnly.cloud).toHaveLength(1);
    expect(cloudOnly.local).toHaveLength(0);

    const alreadyReceived = buildWaitingEntries(
      [{ seq: 8, kind: "file_confirm", status: "waiting_for_local" }],
      [{ seq: 8, kind: "file_confirm", status: "received" }],
      [{ seq: 8, kind: "file_confirm", status: "partial_failure", result: { items: [{ item_id: "video" }] } }],
    );
    expect(alreadyReceived.cloud).toHaveLength(0);
    expect(alreadyReceived.local).toHaveLength(1);
    expect(alreadyReceived.local[0]?.status).toBe("partial_failure");
  });
});
