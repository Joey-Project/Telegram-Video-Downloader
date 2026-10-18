import { listDurableObjectIds, SELF } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import type { D1Migration } from "@cloudflare/vitest-plugin";
import { syntheticCredentials } from "./fixtures/synthetic-credentials";
import { ACTION_QUEUE_LIMIT, appendAction, removeQueuedAction, shouldRetryActionFailure } from "../public/file/action-queue.js";
import { HttpResponseError } from "../public/file/http-error.js";
import { getOrCreateLegacySelection, updateLegacySelection } from "../public/file/legacy-mapping-state.js";
import { hasMetadataPatchFields, hasMetadataPreviewChanges, metadataPreviewDetails } from "../public/file/metadata-preview.js";
import { MAX_SELECTED_ITEMS, updateSelectionWithinLimit } from "../public/file/selection-limit.js";
import { acceptOwnerSnapshot, beginOwnerVerification, failOwnerSnapshot, mayDispatchActions, mayUsePrivateSnapshot } from "../public/file/snapshot-session.js";
import { sourceIdUrl } from "../public/file/source-links.js";
import { buildWaitingEntries, isIncompletePreview, isPresentMedia, isReviewablePreviewStatus, isWaitingEntryPending, mergeRequestEntries, operationDetailsWarning, operationHistoryEntries, presentMediaItems } from "../public/file/state-view.js";
import { canonicalJson, MAX_SELECTED_ITEMS as WORKER_MAX_SELECTED_ITEMS, MAX_STATE_BODY_BYTES } from "../src/validation";
import worker from "../src/index";

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

  it("recognizes one actual metadata field without counting item identity fields", () => {
    expect(hasMetadataPatchFields({ item_id: "video", source_ids: ["youtube:dQw4w9WgXcQ"] })).toBe(true);
    expect(hasMetadataPatchFields({ item_id: "video", hint_index: 3 })).toBe(false);
  });

  it("detects actual metadata changes instead of treating an empty patch list as an update", () => {
    expect(hasMetadataPreviewChanges({ items: [{ item_id: "video" }], metadata_patches: [] })).toBe(false);
    expect(hasMetadataPreviewChanges({ metadata_patches: [{ changes: [{ field: "title", before: "Old", after: "New" }] }] })).toBe(true);
  });
});

describe("selection limit", () => {
  it("matches the Worker request limit and preserves order while capping additions", () => {
    expect(MAX_SELECTED_ITEMS).toBe(WORKER_MAX_SELECTED_ITEMS);
    const added = updateSelectionWithinLimit(
      ["already-selected"],
      Array.from({ length: MAX_SELECTED_ITEMS }, (_, index) => `item-${index}`),
      true,
    );
    expect(added.ids).toHaveLength(MAX_SELECTED_ITEMS);
    expect(added.ids[0]).toBe("already-selected");
    expect(added.ids.at(-1)).toBe(`item-${MAX_SELECTED_ITEMS - 2}`);
    expect(added.omittedCount).toBe(1);

    const removed = updateSelectionWithinLimit(added.ids, ["already-selected"], false);
    expect(removed.ids).toHaveLength(MAX_SELECTED_ITEMS - 1);
    expect(removed.ids[0]).toBe("item-0");
    expect(removed.omittedCount).toBe(0);
  });
});

describe("snapshot canonicalization bounds", () => {
  it("keeps the default canonical JSON node cap for non-snapshot payloads", () => {
    let error: unknown;
    try {
      canonicalJson(Array.from({ length: 100_000 }, () => null));
    } catch (caught) {
      error = caught;
    }
    expect(error).toMatchObject({ status: 400, message: "JSON structure is too complex" });
  });
});

describe("owner snapshot session", () => {
  it("keeps private UI and queued actions gated until a successful owner response", () => {
    const startup = beginOwnerVerification();
    expect(startup).toMatchObject({
      ownerVerified: false,
      snapshot: null,
      privateUiVisible: false,
      canDispatchActions: false,
      clearCache: true,
    });
    expect(mayUsePrivateSnapshot(startup)).toBe(false);
    expect(mayDispatchActions(startup)).toBe(false);

    expect(mayUsePrivateSnapshot({ ...startup, snapshot: { library: { items: ["private"] } } })).toBe(false);
    const accepted = acceptOwnerSnapshot({ synced: true, library: { items: ["private"] } });
    expect(mayUsePrivateSnapshot(accepted)).toBe(true);
    expect(mayDispatchActions(accepted)).toBe(true);
  });

  it.each([401, 403])("clears private state and pauses use after HTTP %i", (status) => {
    const accepted = acceptOwnerSnapshot({ synced: true, library: { items: ["private"] } });
    const failed = failOwnerSnapshot(accepted, status, 1234);
    expect(failed).toEqual({
      ownerVerified: false,
      snapshot: null,
      privateUiVisible: false,
      canDispatchActions: false,
      clearCache: true,
      authorizationRejected: true,
    });
    expect(mayUsePrivateSnapshot(failed)).toBe(false);
    expect(mayDispatchActions(failed)).toBe(false);
  });

  it("uses only the in-memory owner-verified snapshot after a temporary failure", () => {
    const accepted = acceptOwnerSnapshot({ synced: true, library: { items: ["private"] } });
    const stale = failOwnerSnapshot(accepted, 503, 1234);
    expect(stale.ownerVerified).toBe(true);
    expect(mayUsePrivateSnapshot(stale)).toBe(true);
    expect(mayDispatchActions(stale)).toBe(true);
    expect(stale.snapshot).toMatchObject({ stale: true, cached_at: 1234 });

    const firstLoadFailure = failOwnerSnapshot(beginOwnerVerification(), 503, 1234);
    expect(firstLoadFailure.snapshot).toBeNull();
    expect(mayUsePrivateSnapshot(firstLoadFailure)).toBe(false);
    expect(mayDispatchActions(firstLoadFailure)).toBe(false);
  });
});

describe("Mini App source links", () => {
  it("links canonical YouTube IDs and leaves other IDs unchanged", () => {
    expect(sourceIdUrl("youtube:dQw4w9WgXcQ")).toBe("https://www.youtube.com/watch?v=dQw4w9WgXcQ");
    expect(sourceIdUrl("BV17x411w7KC")).toBeNull();
    expect(sourceIdUrl("youtube:bad/id")).toBeNull();
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

async function webhookWithOwnerId(update: unknown, ownerUserId: string, secret: string = webhookToken): Promise<Response> {
  const overriddenEnv = Object.create(testEnv) as object;
  Object.defineProperty(overriddenEnv, "OWNER_USER_ID", { value: ownerUserId });
  return worker.fetch(
    new Request(`${workerUrl}/webhook`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-telegram-bot-api-secret-token": secret },
      body: JSON.stringify(update),
    }),
    overriddenEnv as Env,
  );
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

  it("rejects authorized webhooks until the owner ID is configured", async () => {
    const placeholder = await webhookWithOwnerId(ownerMessage(9), "0");
    const invalid = await webhookWithOwnerId(ownerMessage(10), "not-a-user-id");
    const unauthorized = await webhookWithOwnerId(ownerMessage(11), "0", "wrong-secret");

    expect(placeholder.status).toBe(503);
    expect(invalid.status).toBe(503);
    expect(unauthorized.status).toBe(401);
    expect((await responseJson<{ error: string }>(placeholder)).error).toContain("Owner authorization");
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toHaveLength(0);
    expect(await listDurableObjectIds(testEnv.NOTIFIER)).toHaveLength(0);

    const configured = await webhookWithOwnerId(ownerMessage(12), String(ownerId));
    expect(configured.status).toBe(200);
    expect(await responseJson<{ stored: boolean }>(configured)).toMatchObject({ stored: true });
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toHaveLength(1);
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
    expect(conflict.headers.get("X-Cloud-State-Version")).toBeNull();
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
    const maxItems = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, request_id: "00000000-0000-4000-8000-000000000022", payload: { item_ids: Array.from({ length: MAX_SELECTED_ITEMS }, (_, index) => `item-${index}`), target_relative_dir: "" } }) });
    const maxPatches = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, request_id: "00000000-0000-4000-8000-000000000023", payload: { metadata_patches: Array.from({ length: MAX_SELECTED_ITEMS }, (_, index) => ({ item_id: `item-${index}`, title: "Selected" })) } }) });
    const tooManyItems = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, request_id: "00000000-0000-4000-8000-000000000020", payload: { item_ids: Array.from({ length: MAX_SELECTED_ITEMS + 1 }, (_, index) => `item-${index}`) } }) });
    const tooManyPatches = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, request_id: "00000000-0000-4000-8000-000000000021", payload: { metadata_patches: Array.from({ length: MAX_SELECTED_ITEMS + 1 }, (_, index) => ({ item_id: `item-${index}`, title: "Selected" })) } }) });
    expect(unsafe.status).toBe(400);
    expect(invalidPatch.status).toBe(400);
    expect(maxItems.status).toBe(202);
    expect(maxPatches.status).toBe(202);
    expect(tooManyItems.status).toBe(400);
    expect(tooManyPatches.status).toBe(400);
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
    const state = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(localSnapshot({ state_version: 0 })),
    }));
    expect(response.status).toBe(401);
    expect(state.status).toBe(401);
    expect(state.headers.get("X-Cloud-State-Version")).toBeNull();
  });

  it("caps the local request page at the shared 100-entry batch size", async () => {
    const defaultPage = await SELF.fetch(new Request(`${workerUrl}/api/local/requests`, { headers: localHeaders() }));
    const oversizedPage = await localRequests(101);
    expect(defaultPage.status).toBe(200);
    expect(oversizedPage.status).toBe(400);
    expect((await responseJson<{ error: string }>(oversizedPage)).error).toContain("1 to 100");
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

  it("stores a valid library snapshot with more than 6,700 media items", async () => {
    const base = localSnapshot();
    const library = base.library as Record<string, unknown>;
    const items = Array.from({ length: 6_701 }, (_, index) => ({
      id: `media-${index}`,
      title: `Video ${index}`,
      relative_path: `Videos/${index}.mp4`,
      bytes: 1_024,
      width: null,
      height: null,
      codec: null,
      source_ids: [],
      collection: null,
      attachments: [],
      confidence: "high",
      metadata_status: "complete",
      integrity_status: "verified",
      presence: "present",
    }));
    const body = JSON.stringify({ ...base, library: { ...library, items } });
    expect(textEncoder.encode(body).byteLength).toBeLessThan(MAX_STATE_BODY_BYTES);

    const response = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body,
    }));
    expect(response.status).toBe(200);

    const owner = await signInitData();
    const stored = await responseJson<{ synced: boolean; library: { items: unknown[] } }>(await ownerRequest("/api/library", owner));
    expect(stored.synced).toBe(true);
    expect(stored.library.items).toHaveLength(6_701);
  });

  it("returns 413 for snapshot complexity while retaining bounded default and depth behavior", async () => {
    const base = localSnapshot();
    const library = base.library as Record<string, unknown>;
    const nodeHeavyBody = JSON.stringify({
      ...base,
      library: { ...library, items: Array.from({ length: 1_000_000 }, () => null) },
    });
    expect(textEncoder.encode(nodeHeavyBody).byteLength).toBeLessThan(MAX_STATE_BODY_BYTES);

    const nodeHeavy = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: nodeHeavyBody,
    }));
    expect(nodeHeavy.status).toBe(413);
    expect(await responseJson<{ error: string }>(nodeHeavy)).toEqual({ error: "JSON structure is too complex" });

    let deepValue: unknown = null;
    for (let depth = 0; depth < 50; depth += 1) deepValue = [deepValue];
    const tooDeep = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({ ...base, library: { ...library, items: [], unexpected: deepValue } }),
    }));
    expect(tooDeep.status).toBe(413);
    expect(await responseJson<{ error: string }>(tooDeep)).toEqual({ error: "JSON structure is too complex" });

    const oversizedBody = JSON.stringify({ ...base, padding: "x".repeat(MAX_STATE_BODY_BYTES) });
    expect(textEncoder.encode(oversizedBody).byteLength).toBeGreaterThan(MAX_STATE_BODY_BYTES);
    const oversized = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: oversizedBody,
    }));
    expect(oversized.status).toBe(413);
    expect(await responseJson<{ error: string }>(oversized)).toEqual({ error: "Request body is too large" });
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
    expect(changedSameVersion.headers.get("X-Cloud-State-Version")).toBe("1");
    expect(oldVersion.status).toBe(400);
    expect(oldVersion.headers.get("X-Cloud-State-Version")).toBeNull();
    expect(newer.status).toBe(200);
    expect(lateOlder.status).toBe(409);
    expect(lateOlder.headers.get("X-Cloud-State-Version")).toBe("2");
  });

  it("returns the actual newer version when a concurrent snapshot supersedes a write", async () => {
    const sendState = (snapshot: Record<string, unknown>) => SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(snapshot),
    }));
    const current = localSnapshot();
    expect((await sendState(current)).status).toBe(200);
    await testEnv.DB.prepare(
      "CREATE TRIGGER supersede_snapshot_during_write AFTER INSERT ON snapshot_chunks WHEN NEW.kind = 'library' BEGIN UPDATE snapshot_meta SET state_version = 51 WHERE id = 1; END",
    ).run();
    const library = current.library as Record<string, unknown>;
    const superseded = await sendState({
      ...current,
      state_version: 2,
      library: { ...library, revision: "library-hash-concurrent" },
    });
    expect(superseded.status).toBe(409);
    expect(superseded.headers.get("X-Cloud-State-Version")).toBe("51");
    expect(await responseJson<{ error: string }>(superseded)).toEqual({ error: "Snapshot was superseded by a newer local state" });
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

  it("retries network, timeout, throttling, and server failures but not permanent 4xx responses", () => {
    expect(shouldRetryActionFailure(new TypeError("Failed to fetch"))).toBe(true);
    for (const status of [408, 429, 500, 503]) {
      const error = new HttpResponseError(status, "Temporary failure");
      expect(error.status).toBe(status);
      expect(shouldRetryActionFailure(error)).toBe(true);
    }
    for (const status of [400, 401, 403, 404, 413, 422, 600]) {
      expect(shouldRetryActionFailure(new HttpResponseError(status, "Rejected"))).toBe(false);
    }
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

  it("limits move selections to present media", () => {
    const items = [
      { id: "present", presence: "present" },
      { id: "missing", presence: "missing" },
      { id: "unavailable", presence: "unavailable" },
      { id: "unknown" },
    ];
    expect(isPresentMedia(items[0])).toBe(true);
    expect(isPresentMedia(items[1])).toBe(false);
    expect(presentMediaItems(items).map((item) => item.id)).toEqual(["present"]);
  });

  it("keeps failed file previews in history while hiding active previews already shown for confirmation", () => {
    const active = { seq: 1, kind: "file_preview", status: "awaiting_confirmation", result: { preview: { revision: 1 } } };
    const failedWithoutPreview = { seq: 2, kind: "file_preview", status: "failed", error: "Selected item is missing" };
    const failedWithPreview = { seq: 3, kind: "file_preview", status: "failed", result: { preview: { revision: 2 } }, error: "Preview became stale" };
    const other = { seq: 4, kind: "library_scan", status: "completed" };
    expect(isReviewablePreviewStatus(active.status)).toBe(true);
    expect(isReviewablePreviewStatus(failedWithPreview.status)).toBe(false);
    expect(operationHistoryEntries([active, failedWithoutPreview, failedWithPreview, other])).toEqual([
      failedWithoutPreview,
      failedWithPreview,
      other,
    ]);
  });

  it("does not count terminal file previews as waiting", () => {
    for (const status of [
      "confirmed",
      "completed",
      "cancelled",
      "expired",
      "failed",
      "uncertain",
      "partial_failure",
      "stale",
      "rejected",
    ]) {
      expect(isWaitingEntryPending({ kind: "file_preview", status }), status).toBe(false);
    }
    for (const status of ["waiting_for_local", "awaiting_confirmation", "processing", "retryable"]) {
      expect(isWaitingEntryPending({ kind: "file_preview", status }), status).toBe(true);
    }
    expect(isWaitingEntryPending({ kind: "file_confirm", status: "received" })).toBe(true);
    expect(isWaitingEntryPending({ kind: "file_confirm", status: "completed" })).toBe(false);
  });

  it("requires a fresh preview when the saved details were omitted", () => {
    expect(isIncompletePreview({
      preview_id: "preview-a",
      revision: "revision-a",
      status: "awaiting_confirmation",
      preview_incomplete: true,
      incomplete: true,
      reload_required: true,
      reason: "Preview details were omitted from the cloud snapshot.",
      preview: null,
    })).toBe(true);
    expect(isIncompletePreview({ incomplete: true, omitted_count: 3, reload_required: true })).toBe(true);
    expect(isIncompletePreview({ preview: { items: [], metadata_patches: [] } })).toBe(false);
    expect(isIncompletePreview({ preview: null })).toBe(true);
  });

  it("shows when saved operation details were omitted from history", () => {
    expect(operationDetailsWarning({
      kind: "legacy_import",
      details_incomplete: true,
      result_truncated: true,
      result: { hints_summary: "omitted" },
    })).toContain("submit the import again");
    expect(operationDetailsWarning({
      kind: "file_confirm",
      result: { operation: { details_incomplete: true } },
    })).toContain("omitted from the saved history");
    expect(operationDetailsWarning({
      kind: "legacy_import",
      result: { hints: [], candidates: [] },
    })).toBeNull();
  });
});
