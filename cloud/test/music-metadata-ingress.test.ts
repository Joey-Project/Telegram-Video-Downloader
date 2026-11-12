import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { syntheticCredentials } from "./fixtures/synthetic-credentials";

const ownerId = 123456789;
const workerUrl = "https://worker.test";
const testDatabaseEpoch = "0123456789abcdef0123456789abcdef";
const webhookSecret = syntheticCredentials.webhookSecret.value;
const localSecret = syntheticCredentials.localSharedSecret.value;

function privateMessageUpdate(updateId: number, fromId: number, media: Record<string, unknown>) {
  return {
    update_id: updateId,
    message: {
      message_id: updateId,
      date: 1794355200,
      from: { id: fromId, is_bot: false, first_name: "Owner" },
      chat: { id: fromId, type: "private", first_name: "Owner" },
      ...media,
    },
  };
}

function audioUpdate(updateId = 81001, fromId = ownerId) {
  return privateMessageUpdate(updateId, fromId, {
    audio: {
      file_id: "fixture-audio-file-id",
      file_unique_id: "fixture-audio-unique-id",
      duration: 241,
      performer: "Fixture Artist",
      title: "Fixture Track",
      file_name: "fixture-track.m4a",
      mime_type: "audio/mp4",
      file_size: 2_147_483_649,
    },
  });
}

function documentUpdate(updateId = 81002, fromId = ownerId) {
  return privateMessageUpdate(updateId, fromId, {
    document: {
      file_id: "fixture-document-file-id",
      file_unique_id: "fixture-document-unique-id",
      file_name: "fixture-track.mp3",
      mime_type: "audio/mpeg",
      file_size: 2_147_483_777,
    },
  });
}

async function postWebhook(update: unknown): Promise<Response> {
  return SELF.fetch(new Request(`${workerUrl}/webhook`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-telegram-bot-api-secret-token": webhookSecret,
    },
    body: JSON.stringify(update),
  }));
}

async function fetchDesktopInbox(): Promise<{ requests: Array<{
  seq: number;
  kind: string;
  request_id: string;
  payload: Record<string, unknown>;
}> }> {
  const response = await SELF.fetch(new Request(`${workerUrl}/api/local/requests?limit=100`, {
    headers: {
      authorization: `Bearer ${localSecret}`,
      "X-Cloud-Database-Epoch": testDatabaseEpoch,
    },
  }));
  expect(response.status).toBe(200);
  return await response.json() as { requests: Array<{
    seq: number;
    kind: string;
    request_id: string;
    payload: Record<string, unknown>;
  }> };
}

describe("large Telegram music metadata ingress", () => {
  it("durably exposes owner audio metadata with a 64-bit-safe file size to desktop polling", async () => {
    const update = audioUpdate();
    const response = await postWebhook(update);

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ stored: true, duplicate: false });

    const inbox = await fetchDesktopInbox();
    expect(inbox.requests).toHaveLength(1);
    expect(inbox.requests[0]).toMatchObject({ kind: "telegram", request_id: "tg:81001" });
    expect(inbox.requests[0]?.payload).toEqual(update);
    expect((await fetchDesktopInbox()).requests).toEqual(inbox.requests);
    const message = inbox.requests[0]?.payload.message as { audio: { file_size: number } };
    expect(message.audio.file_size).toBeGreaterThan(20 * 1024 * 1024);
    expect(message.audio.file_size).toBeGreaterThan(2 ** 31);
  });

  it("durably exposes owner audio documents and preserves metadata on duplicate delivery", async () => {
    const update = documentUpdate();
    const first = await postWebhook(update);
    const retry = await postWebhook(update);

    expect(first.status).toBe(200);
    expect(retry.status).toBe(200);
    expect(await first.json()).toMatchObject({ stored: true, duplicate: false });
    expect(await retry.json()).toMatchObject({ stored: true, duplicate: true });

    const inbox = await fetchDesktopInbox();
    expect(inbox.requests).toHaveLength(1);
    expect(inbox.requests[0]).toMatchObject({ kind: "telegram", request_id: "tg:81002" });
    expect(inbox.requests[0]?.payload).toEqual(update);
    const message = inbox.requests[0]?.payload.message as { document: { file_size: number } };
    expect(message.document.file_size).toBeGreaterThan(20 * 1024 * 1024);
    expect(message.document.file_size).toBeGreaterThan(2 ** 31);
  });

  it("does not persist metadata sent by a foreign Telegram account", async () => {
    const response = await postWebhook(audioUpdate(81003, ownerId + 1));

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ stored: false, ignored: true });
    expect((await fetchDesktopInbox()).requests).toEqual([]);
  });
});
