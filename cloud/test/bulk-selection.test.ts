import { SELF } from "cloudflare:test";
import { env } from "cloudflare:workers";
import type { D1Migration } from "@cloudflare/vitest-plugin";
import { describe, expect, it } from "vitest";
import { syntheticCredentials } from "./fixtures/synthetic-credentials";
import { MAX_SELECTED_ITEMS, updateSelectionWithinLimit } from "../public/file/selection-limit.js";
import {
  ACTION_REPLAY_WINDOW_MS,
  MAX_ACTION_BODY_BYTES,
  MAX_RECOVERY_BATCH_ITEMS,
  MAX_SELECTED_ITEMS as WORKER_MAX_SELECTED_ITEMS,
} from "../src/validation";

const testEnv = env as typeof env & { TEST_MIGRATIONS: D1Migration[] };
const ownerId = 123456789;
const botToken = syntheticCredentials.botToken.value;
const workerUrl = "https://worker.test";
const rootBinding = `v1:${"a".repeat(64)}`;
const targetRootBinding = `v1:${"b".repeat(64)}`;
const textEncoder = new TextEncoder();

async function signOwnerInitData(): Promise<string> {
  const params = new URLSearchParams();
  params.set("auth_date", String(Math.floor(Date.now() / 1000)));
  params.set("chat_type", "private");
  params.set("user", JSON.stringify({ id: ownerId, is_bot: false, first_name: "Owner" }));
  const entries = [...params.entries()].sort(([left], [right]) => left.localeCompare(right));
  const checkString = entries.map(([key, value]) => `${key}=${value}`).join("\n");
  const derivationKey = await crypto.subtle.importKey(
    "raw",
    textEncoder.encode("WebAppData"),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const secret = await crypto.subtle.sign("HMAC", derivationKey, textEncoder.encode(botToken));
  const verificationKey = await crypto.subtle.importKey(
    "raw",
    secret,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const digest = new Uint8Array(await crypto.subtle.sign("HMAC", verificationKey, textEncoder.encode(checkString)));
  params.set("hash", [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join(""));
  return params.toString();
}

function itemIds(count: number): string[] {
  return Array.from({ length: count }, (_, index) => `copy-${index.toString(16).padStart(24, "0")}`);
}

type BulkActionKind = "file_preview" | "library_migration_preview" | "staging_cleanup";

function payloadFor(kind: BulkActionKind, ids: string[]) {
  if (kind === "file_preview") {
    return {
      library_id: "library-test",
      root_binding: rootBinding,
      item_ids: ids,
      target_relative_dir: "",
      rename: false,
      conflict: "skip",
    };
  }
  if (kind === "staging_cleanup") {
    return {
      operation: "preview",
      library_id: "library-test",
      root_binding: rootBinding,
      item_ids: ids,
    };
  }
  return {
    source_library_id: "library-test",
    target_library_id: "library-archive",
    source_root_binding: rootBinding,
    target_root_binding: targetRootBinding,
    scope: "selected",
    item_ids: ids,
    include_incomplete_tasks: false,
    conflict: "skip",
    set_target_default_on_success: false,
  };
}

async function submitAction(
  initData: string,
  kind: BulkActionKind,
  ids: string[],
  requestId: string,
): Promise<{ response: Response; bodyBytes: number }> {
  return submitPayload(initData, kind, payloadFor(kind, ids), requestId);
}

async function submitPayload(
  initData: string,
  kind: BulkActionKind,
  payload: Record<string, unknown>,
  requestId: string,
): Promise<{ response: Response; bodyBytes: number }> {
  const queuedAt = Date.now();
  const body = JSON.stringify({
    request_id: requestId,
    kind,
    payload,
    queued_at_ms: queuedAt,
    replay_expires_at_ms: queuedAt + ACTION_REPLAY_WINDOW_MS,
  });
  const response = await SELF.fetch(new Request(`${workerUrl}/api/actions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-telegram-init-data": initData,
    },
    body,
  }));
  return { response, bodyBytes: textEncoder.encode(body).byteLength };
}

describe("5,000-item batch selection", () => {
  it("allows a 101-item selection while retaining the 5,000-item default ceiling", () => {
    expect(MAX_SELECTED_ITEMS).toBe(5_000);
    expect(MAX_SELECTED_ITEMS).toBe(WORKER_MAX_SELECTED_ITEMS);
    expect(MAX_RECOVERY_BATCH_ITEMS).toBe(100);

    const oneHundredAndOne = updateSelectionWithinLimit(
      [],
      Array.from({ length: 101 }, (_, index) => `item-${index}`),
      true,
    );
    expect(oneHundredAndOne.ids).toHaveLength(101);
    expect(oneHundredAndOne.omittedCount).toBe(0);

    const capped = updateSelectionWithinLimit([], itemIds(MAX_SELECTED_ITEMS + 1), true);
    expect(capped.ids).toHaveLength(MAX_SELECTED_ITEMS);
    expect(capped.omittedCount).toBe(1);
  });

  it.each(["file_preview", "library_migration_preview", "staging_cleanup"] as const)(
    "persists exactly 5,000 %s item IDs and rejects 5,001",
    async (kind) => {
      const initData = await signOwnerInitData();
      const acceptedId = kind === "file_preview"
        ? "00000000-0000-4000-8000-000000000501"
        : kind === "library_migration_preview"
          ? "00000000-0000-4000-8000-000000000502"
          : "00000000-0000-4000-8000-000000000505";
      const rejectedId = kind === "file_preview"
        ? "00000000-0000-4000-8000-000000000503"
        : kind === "library_migration_preview"
          ? "00000000-0000-4000-8000-000000000504"
          : "00000000-0000-4000-8000-000000000506";

      const accepted = await submitAction(initData, kind, itemIds(MAX_SELECTED_ITEMS), acceptedId);
      expect(accepted.response.status).toBe(202);
      expect(accepted.bodyBytes).toBeLessThan(MAX_ACTION_BODY_BYTES);

      const stored = await testEnv.DB.prepare(
        "SELECT kind, payload_json FROM inbox WHERE dedup_key = ?",
      ).bind(`action:${acceptedId}`).first<{ kind: string; payload_json: string }>();
      expect(stored?.kind).toBe(kind);
      expect(JSON.parse(stored?.payload_json ?? "{}").item_ids).toEqual(itemIds(MAX_SELECTED_ITEMS));

      const rejected = await submitAction(initData, kind, itemIds(MAX_SELECTED_ITEMS + 1), rejectedId);
      expect(rejected.response.status).toBe(400);
      const rejectedRow = await testEnv.DB.prepare(
        "SELECT seq FROM inbox WHERE dedup_key = ?",
      ).bind(`action:${rejectedId}`).first<{ seq: number }>();
      expect(rejectedRow).toBeNull();
    },
  );

  it("validates staging cleanup scan, confirm, and cancel payloads before durable inbox writes", async () => {
    const initData = await signOwnerInitData();
    const identity = { library_id: "library-test", root_binding: rootBinding };
    const validPayloads = [
      { operation: "scan", ...identity },
      { operation: "confirm", ...identity, preview_seq: 42, preview_id: "cleanup-preview-42", revision: "staging-r7" },
      { operation: "cancel", ...identity, preview_seq: 43, preview_id: "cleanup-preview-43", revision: "staging-r8" },
    ];
    for (const [index, payload] of validPayloads.entries()) {
      const requestId = `00000000-0000-4000-8000-00000000051${index}`;
      const accepted = await submitPayload(initData, "staging_cleanup", payload, requestId);
      expect(accepted.response.status).toBe(202);
      const stored = await testEnv.DB.prepare(
        "SELECT kind, payload_json FROM inbox WHERE dedup_key = ?",
      ).bind(`action:${requestId}`).first<{ kind: string; payload_json: string }>();
      expect(stored?.kind).toBe("staging_cleanup");
      expect(JSON.parse(stored?.payload_json ?? "{}")).toEqual(payload);
    }

    const invalidPayloads = [
      { operation: "delete_all", ...identity },
      { operation: "scan", ...identity, item_ids: ["unexpected"] },
      { operation: "preview", ...identity, item_ids: [] },
      { operation: "cancel", ...identity, preview_seq: 0, preview_id: "cleanup-preview", revision: "staging-r1" },
    ];
    for (const [index, payload] of invalidPayloads.entries()) {
      const requestId = `00000000-0000-4000-8000-00000000052${index}`;
      const rejected = await submitPayload(initData, "staging_cleanup", payload, requestId);
      expect(rejected.response.status).toBe(400);
      const row = await testEnv.DB.prepare(
        "SELECT seq FROM inbox WHERE dedup_key = ?",
      ).bind(`action:${requestId}`).first<{ seq: number }>();
      expect(row).toBeNull();
    }
  });
});
