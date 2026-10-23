import { requireConfiguredOwnerUserId } from "./security";
import { isDatabaseEpoch } from "./store";

export const MINIAPP_UPDATES_PROTOCOL = "miniapp-updates-v1";
export const MINIAPP_UPDATES_SESSION_PATH = "/api/updates/session";
export const MINIAPP_UPDATES_SOCKET_PATH = "/api/updates/ws";
export const MINIAPP_UPDATES_TICKET_TTL_MS = 60_000;
export const MINIAPP_UPDATES_MAX_VIEWERS = 32;

export const NOTIFIER_ROLE_HEADER = "x-tvd-notifier-role";
export const NOTIFIER_VIEWER_OWNER_HEADER = "x-tvd-viewer-owner";
export const NOTIFIER_VIEWER_EPOCH_HEADER = "x-tvd-viewer-epoch";
export const NOTIFIER_VIEWER_TICKET_EXPIRY_HEADER = "x-tvd-viewer-ticket-expiry";
export const NOTIFIER_VIEWER_AUTH_EXPIRY_HEADER = "x-tvd-viewer-auth-expiry";

const utf8 = new TextEncoder();
const TICKET_VERSION = "v1";
const TICKET_DOMAIN = "telegram-video-downloader/miniapp-updates/ticket/v1\0";
const MAX_TICKET_LENGTH = 1024;
const MAX_AUTH_LIFETIME_FROM_ISSUE_MS = 6 * 60 * 60 * 1000 + 60_000;
const MAX_FUTURE_ISSUE_SKEW_MS = 30_000;

export interface MiniAppUpdatesTicketClaims {
  version: 1;
  role: "viewer";
  owner_user_id: string;
  database_epoch: string;
  issued_at: number;
  ticket_expires_at: number;
  auth_expires_at: number;
}

export interface IssuedMiniAppUpdatesTicket {
  ticket: string;
  ticketExpiresAt: number;
}

function base64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "");
}

function fromBase64Url(value: string): Uint8Array<ArrayBuffer> | null {
  if (!/^[A-Za-z0-9_-]+$/u.test(value) || value.length > MAX_TICKET_LENGTH) return null;
  try {
    const base64 = value.replaceAll("-", "+").replaceAll("_", "/");
    const binary = atob(base64 + "=".repeat((4 - base64.length % 4) % 4));
    const bytes = new Uint8Array(new ArrayBuffer(binary.length));
    for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
    return base64Url(bytes) === value ? bytes : null;
  } catch {
    return null;
  }
}

async function ticketKey(secret: string, usage: "sign" | "verify"): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    utf8.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    [usage],
  );
}

function isTicketClaims(value: unknown): value is MiniAppUpdatesTicketClaims {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  const expectedKeys = [
    "version",
    "role",
    "owner_user_id",
    "database_epoch",
    "issued_at",
    "ticket_expires_at",
    "auth_expires_at",
  ];
  if (Object.keys(record).length !== expectedKeys.length || expectedKeys.some((key) => !Object.hasOwn(record, key))) {
    return false;
  }
  return record.version === 1 && record.role === "viewer" &&
    typeof record.owner_user_id === "string" && /^[1-9]\d{0,15}$/u.test(record.owner_user_id) &&
    typeof record.database_epoch === "string" && isDatabaseEpoch(record.database_epoch) &&
    typeof record.issued_at === "number" && Number.isSafeInteger(record.issued_at) && record.issued_at > 0 &&
    typeof record.ticket_expires_at === "number" && Number.isSafeInteger(record.ticket_expires_at) &&
    typeof record.auth_expires_at === "number" && Number.isSafeInteger(record.auth_expires_at);
}

export async function issueMiniAppUpdatesTicket(
  secret: string,
  ownerUserId: string,
  databaseEpoch: string,
  authExpiresAt: number,
  now = Date.now(),
): Promise<IssuedMiniAppUpdatesTicket> {
  if (!secret || !Number.isSafeInteger(now) || !Number.isSafeInteger(authExpiresAt) || authExpiresAt <= now ||
    authExpiresAt > now + MAX_AUTH_LIFETIME_FROM_ISSUE_MS || !isDatabaseEpoch(databaseEpoch)) {
    throw new Error("Mini App update ticket scope is invalid");
  }
  const owner = requireConfiguredOwnerUserId(ownerUserId);
  const ticketExpiresAt = Math.min(now + MINIAPP_UPDATES_TICKET_TTL_MS, authExpiresAt);
  const claims: MiniAppUpdatesTicketClaims = {
    version: 1,
    role: "viewer",
    owner_user_id: owner,
    database_epoch: databaseEpoch,
    issued_at: now,
    ticket_expires_at: ticketExpiresAt,
    auth_expires_at: authExpiresAt,
  };
  const payload = base64Url(utf8.encode(JSON.stringify(claims)));
  const key = await ticketKey(secret, "sign");
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", key, utf8.encode(`${TICKET_DOMAIN}${payload}`)));
  return { ticket: `${TICKET_VERSION}.${payload}.${base64Url(signature)}`, ticketExpiresAt };
}

export async function verifyMiniAppUpdatesTicket(
  ticket: string,
  secret: string,
  expectedOwnerUserId: string,
  now = Date.now(),
): Promise<MiniAppUpdatesTicketClaims | null> {
  if (!secret || ticket.length > MAX_TICKET_LENGTH) return null;
  const parts = ticket.split(".");
  if (parts.length !== 3 || parts[0] !== TICKET_VERSION) return null;
  const payload = parts[1] ?? "";
  const signatureText = parts[2] ?? "";
  const payloadBytes = fromBase64Url(payload);
  const signature = fromBase64Url(signatureText);
  if (payloadBytes === null || signature === null || signature.byteLength !== 32) return null;

  let claimsValue: unknown;
  try {
    claimsValue = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(payloadBytes)) as unknown;
  } catch {
    return null;
  }
  if (!isTicketClaims(claimsValue)) return null;

  let validSignature: boolean;
  try {
    const key = await ticketKey(secret, "verify");
    validSignature = await crypto.subtle.verify("HMAC", key, signature, utf8.encode(`${TICKET_DOMAIN}${payload}`));
  } catch {
    return null;
  }
  if (!validSignature) return null;

  let expectedOwner: string;
  try {
    expectedOwner = requireConfiguredOwnerUserId(expectedOwnerUserId);
  } catch {
    return null;
  }
  if (claimsValue.owner_user_id !== expectedOwner || claimsValue.ticket_expires_at <= now ||
    claimsValue.auth_expires_at <= now || claimsValue.ticket_expires_at <= claimsValue.issued_at ||
    claimsValue.ticket_expires_at - claimsValue.issued_at > MINIAPP_UPDATES_TICKET_TTL_MS ||
    claimsValue.issued_at > now + MAX_FUTURE_ISSUE_SKEW_MS ||
    claimsValue.auth_expires_at <= claimsValue.issued_at ||
    claimsValue.auth_expires_at - claimsValue.issued_at > MAX_AUTH_LIFETIME_FROM_ISSUE_MS) {
    return null;
  }
  return claimsValue;
}

export function readMiniAppUpdatesTicketProtocol(header: string | null): string | null {
  if (header === null || header.length > MAX_TICKET_LENGTH + MINIAPP_UPDATES_PROTOCOL.length + 8) return null;
  const protocols = header.split(",").map((protocol) => protocol.trim());
  if (protocols.length !== 2 || protocols[0] !== MINIAPP_UPDATES_PROTOCOL) return null;
  const ticket = protocols[1] ?? "";
  if (ticket.length < 8 || ticket.length > MAX_TICKET_LENGTH || !/^[A-Za-z0-9._-]+$/u.test(ticket)) return null;
  return ticket;
}
