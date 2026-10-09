import { HttpError } from "./types";

const utf8 = new TextEncoder();
const MAX_INIT_DATA_BYTES = 16 * 1024;
const MAX_AUTH_AGE_SECONDS = 6 * 60 * 60;
const MAX_FUTURE_SKEW_SECONDS = 60;

function constantTimeEqual(left: string, right: string): boolean {
  const leftBytes = utf8.encode(left);
  const rightBytes = utf8.encode(right);
  let difference = leftBytes.length ^ rightBytes.length;
  const maxLength = Math.max(leftBytes.length, rightBytes.length);
  for (let index = 0; index < maxLength; index += 1) {
    difference |= (leftBytes[index] ?? 0) ^ (rightBytes[index] ?? 0);
  }
  return difference === 0;
}

function parseHex(value: string): Uint8Array<ArrayBuffer> | null {
  if (!/^[0-9a-f]{64}$/iu.test(value)) return null;
  const bytes = new Uint8Array(new ArrayBuffer(32));
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

async function validTelegramHash(params: URLSearchParams, botToken: string): Promise<boolean> {
  const hashes = params.getAll("hash");
  if (hashes.length !== 1) return false;
  const expectedHash = parseHex(hashes[0] ?? "");
  if (expectedHash === null) return false;

  const entries = [...params.entries()].filter(([key]) => key !== "hash");
  if (entries.length > 100) return false;
  entries.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
  const checkString = entries.map(([key, value]) => `${key}=${value}`).join("\n");

  const derivationKey = await crypto.subtle.importKey(
    "raw",
    utf8.encode("WebAppData"),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const secret = await crypto.subtle.sign("HMAC", derivationKey, utf8.encode(botToken));
  const verificationKey = await crypto.subtle.importKey(
    "raw",
    secret,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"],
  );
  return crypto.subtle.verify("HMAC", verificationKey, expectedHash, utf8.encode(checkString));
}

export function requireLocalAuthorization(request: Request, env: Env): void {
  if (!env.LOCAL_SHARED_SECRET) throw new HttpError(503, "Local client authorization is not configured");
  const authorization = request.headers.get("authorization") ?? "";
  if (authorization.length > 512) throw new HttpError(401, "Unauthorized");
  const match = /^Bearer ([^\s]+)$/iu.exec(authorization);
  if (!match || !constantTimeEqual(match[1] ?? "", env.LOCAL_SHARED_SECRET)) {
    throw new HttpError(401, "Unauthorized");
  }
}

export function requireWebhookAuthorization(request: Request, env: Env): void {
  if (!env.WEBHOOK_SECRET) throw new HttpError(503, "Webhook authorization is not configured");
  const secret = request.headers.get("x-telegram-bot-api-secret-token") ?? "";
  if (secret.length > 256 || !constantTimeEqual(secret, env.WEBHOOK_SECRET)) {
    throw new HttpError(401, "Unauthorized");
  }
}

export async function requireOwner(
  request: Request,
  env: Env,
): Promise<{ userId: string }> {
  if (!env.BOT_TOKEN || !/^[1-9]\d{0,15}$/u.test(env.OWNER_USER_ID)) {
    throw new HttpError(503, "Mini App authorization is not configured");
  }
  const initData = request.headers.get("x-telegram-init-data") ?? "";
  if (initData.length === 0 || utf8.encode(initData).byteLength > MAX_INIT_DATA_BYTES) {
    throw new HttpError(401, "Unauthorized");
  }

  let params: URLSearchParams;
  try {
    params = new URLSearchParams(initData);
  } catch {
    throw new HttpError(401, "Unauthorized");
  }
  const keys = new Set<string>();
  for (const [key] of params.entries()) {
    if (keys.has(key)) throw new HttpError(401, "Unauthorized");
    keys.add(key);
  }

  let valid: boolean;
  try {
    valid = await validTelegramHash(params, env.BOT_TOKEN);
  } catch {
    throw new HttpError(401, "Unauthorized");
  }
  if (!valid) throw new HttpError(401, "Unauthorized");

  const authDateText = params.get("auth_date");
  if (!authDateText || !/^\d{1,12}$/u.test(authDateText)) throw new HttpError(401, "Unauthorized");
  const authDate = Number(authDateText);
  const nowSeconds = Math.floor(Date.now() / 1000);
  if (!Number.isSafeInteger(authDate) || authDate > nowSeconds + MAX_FUTURE_SKEW_SECONDS || nowSeconds - authDate > MAX_AUTH_AGE_SECONDS) {
    throw new HttpError(401, "Mini App authorization has expired");
  }

  const userText = params.get("user");
  if (!userText || userText.length > 4096) throw new HttpError(401, "Unauthorized");
  let user: unknown;
  try {
    user = JSON.parse(userText) as unknown;
  } catch {
    throw new HttpError(401, "Unauthorized");
  }
  if (typeof user !== "object" || user === null || Array.isArray(user)) {
    throw new HttpError(401, "Unauthorized");
  }
  const userRecord = user as Record<string, unknown>;
  const userId = typeof userRecord.id === "number" && Number.isSafeInteger(userRecord.id)
    ? String(userRecord.id)
    : typeof userRecord.id === "string" && /^[1-9]\d{0,15}$/u.test(userRecord.id)
      ? userRecord.id
      : "";
  if (!userId || userRecord.is_bot === true) throw new HttpError(401, "Unauthorized");
  if (!constantTimeEqual(userId, env.OWNER_USER_ID)) throw new HttpError(403, "Forbidden");

  const chatType = params.get("chat_type");
  if ((chatType !== null && chatType !== "private" && chatType !== "sender") || params.has("chat")) {
    throw new HttpError(403, "Mini App must be opened in the owner's private chat");
  }
  return { userId };
}
