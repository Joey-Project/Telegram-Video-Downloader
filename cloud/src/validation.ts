import { HttpError } from "./types";
import type {
  ActionRequest,
  CollectionMetadata,
  FilePreviewPayload,
  LocalSnapshot,
  MetadataPatch,
  RequestKind,
} from "./types";

export const MAX_WEBHOOK_BODY_BYTES = 1_000_000;
export const MAX_LEGACY_TEXT_BYTES = 256 * 1024;
const MAX_ACTION_BODY_ENVELOPE_BYTES = 4 * 1024;
// JSON may represent each ASCII code unit as a six-byte \\uXXXX escape.
export const MAX_ACTION_BODY_BYTES = MAX_LEGACY_TEXT_BYTES * 6 + MAX_ACTION_BODY_ENVELOPE_BYTES;
export const MAX_STATE_BODY_BYTES = 12 * 1024 * 1024;
export const MAX_TARGET_RELATIVE_DIR_BYTES = 1_024;
export const MAX_SELECTED_ITEMS = 100;
const MAX_COLLECTION_ORDER = 0xffff_ffff;
const MAX_LEGACY_HINTS = 10_000;
const MAX_LEGACY_HINT_INDEX = MAX_LEGACY_HINTS - 1;
const MAX_TITLE_SCALAR_VALUES = 500;
const MAX_TITLE_CODE_UNITS = MAX_TITLE_SCALAR_VALUES * 2;

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export async function readJsonBody(
  request: Request,
  maxBytes: number,
): Promise<unknown> {
  const contentType = request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
  if (contentType !== "application/json") {
    throw new HttpError(415, "Content-Type must be application/json");
  }

  const declaredLength = request.headers.get("content-length");
  if (declaredLength !== null) {
    const length = Number(declaredLength);
    if (!Number.isSafeInteger(length) || length < 0) {
      throw new HttpError(400, "Invalid Content-Length");
    }
    if (length > maxBytes) {
      throw new HttpError(413, "Request body is too large");
    }
  }

  if (request.body === null) {
    throw new HttpError(400, "Request body is required");
  }

  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  while (true) {
    const part = await reader.read();
    if (part.done) break;
    totalBytes += part.value.byteLength;
    if (totalBytes > maxBytes) {
      await reader.cancel();
      throw new HttpError(413, "Request body is too large");
    }
    chunks.push(part.value);
  }

  const bytes = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new HttpError(400, "Request body must be UTF-8 JSON");
  }

  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new HttpError(400, "Request body is not valid JSON");
  }
}

export function canonicalJson(
  value: unknown,
  options: { maxNodes?: number; complexityStatus?: number } = {},
): string {
  const maxNodes = options.maxNodes ?? 100_000;
  const complexityStatus = options.complexityStatus ?? 400;
  let nodes = 0;
  const visit = (current: unknown, depth: number): string => {
    nodes += 1;
    if (nodes > maxNodes || depth > 48) {
      throw new HttpError(complexityStatus, "JSON structure is too complex");
    }
    if (current === null || typeof current !== "object") {
      const serialized = JSON.stringify(current);
      if (serialized === undefined) throw new HttpError(400, "Unsupported JSON value");
      return serialized;
    }
    if (Array.isArray(current)) {
      return `[${current.map((item) => visit(item, depth + 1)).join(",")}]`;
    }
    const object = current as Record<string, unknown>;
    const keys = Object.keys(object).sort();
    return `{${keys.map((key) => `${JSON.stringify(key)}:${visit(object[key], depth + 1)}`).join(",")}}`;
  };
  return visit(value, 0);
}

export async function sha256Hex(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function onlyKeys(record: Record<string, unknown>, allowed: readonly string[]): boolean {
  return Object.keys(record).every((key) => allowed.includes(key));
}

function boundedString(
  value: unknown,
  field: string,
  maxLength: number,
  options: { allowEmpty?: boolean } = {},
): string {
  if (typeof value !== "string") throw new HttpError(400, `${field} must be a string`);
  const normalized = value.trim();
  if ((!options.allowEmpty && normalized.length === 0) || normalized.length > maxLength) {
    throw new HttpError(400, `${field} is empty or too long`);
  }
  if (/[\u0000-\u001f\u007f]/u.test(normalized)) {
    throw new HttpError(400, `${field} contains control characters`);
  }
  return normalized;
}

function boundedUtf8String(value: unknown, field: string, maxBytes: number): string {
  if (typeof value !== "string") throw new HttpError(400, `${field} must be a string`);
  const normalized = value.trim();
  if (hasUnpairedSurrogate(normalized)) {
    throw new HttpError(400, `${field} contains an unpaired surrogate`);
  }
  if (normalized.length === 0 || new TextEncoder().encode(normalized).byteLength > maxBytes) {
    throw new HttpError(400, `${field} is empty or too long`);
  }
  if (/[\u0000-\u001f\u007f]/u.test(normalized)) {
    throw new HttpError(400, `${field} contains control characters`);
  }
  return normalized;
}

function boundedLegacyText(value: unknown): string {
  if (typeof value !== "string") throw new HttpError(400, "text must be a string");
  if (hasUnpairedSurrogate(value)) {
    throw new HttpError(400, "text contains an unpaired surrogate");
  }
  if (new TextEncoder().encode(value).byteLength > MAX_LEGACY_TEXT_BYTES) {
    throw new HttpError(413, "Legacy import text is too large");
  }
  if (/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]/u.test(value)) {
    throw new HttpError(400, "text contains control characters");
  }
  return value;
}

function hasUnpairedSurrogate(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const nextCodeUnit = value.charCodeAt(index + 1);
      if (!(nextCodeUnit >= 0xdc00 && nextCodeUnit <= 0xdfff)) return true;
      index += 1;
    } else if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      return true;
    }
  }
  return false;
}

function boundedTitle(value: unknown, field: string): string {
  // A valid 500-scalar string uses at most 1,000 UTF-16 code units.
  const normalized = boundedString(value, field, MAX_TITLE_CODE_UNITS);
  if (hasUnpairedSurrogate(normalized)) {
    throw new HttpError(400, `${field} contains an unpaired surrogate`);
  }
  if (Array.from(normalized).length > MAX_TITLE_SCALAR_VALUES) {
    throw new HttpError(400, `${field} is empty or too long`);
  }
  return normalized;
}

function stringList(value: unknown, field: string, maxItems: number, maxLength: number): string[] {
  if (!Array.isArray(value) || value.length === 0 || value.length > maxItems) {
    throw new HttpError(400, `${field} must contain between 1 and ${maxItems} values`);
  }
  const values = value.map((entry, index) => boundedString(entry, `${field}[${index}]`, maxLength));
  if (new Set(values).size !== values.length) throw new HttpError(400, `${field} contains duplicates`);
  return values;
}

function parseCollection(value: unknown): CollectionMetadata {
  if (!isRecord(value) || !onlyKeys(value, ["id", "title", "kind", "order", "part_id"]) ||
    !["id", "title", "kind", "order", "part_id"].every((key) => Object.hasOwn(value, key))) {
    throw new HttpError(400, "metadata_patches.collection has an invalid shape");
  }
  const title = boundedTitle(value.title, "collection.title");
  const kind = boundedUtf8String(value.kind, "collection.kind", 64);
  const id = value.id === undefined || value.id === null
    ? null
    : boundedUtf8String(value.id, "collection.id", 128);
  const partId = value.part_id === undefined || value.part_id === null
    ? null
    : boundedUtf8String(value.part_id, "collection.part_id", 128);
  const order = value.order === undefined || value.order === null
    ? null
    : value.order;
  if (order !== null &&
    (!Number.isSafeInteger(order) || (order as number) < 0 || (order as number) > MAX_COLLECTION_ORDER)) {
    throw new HttpError(400, `collection.order must be an integer from 0 to ${MAX_COLLECTION_ORDER} or null`);
  }
  return { id, title, kind, order: order as number | null, part_id: partId };
}

function parseMetadataPatches(value: unknown): MetadataPatch[] {
  if (!Array.isArray(value) || value.length === 0 || value.length > MAX_SELECTED_ITEMS) {
    throw new HttpError(400, `metadata_patches must contain between 1 and ${MAX_SELECTED_ITEMS} entries`);
  }
  const patches = value.map((entry, index) => {
    if (!isRecord(entry) || !onlyKeys(entry, ["item_id", "hint_index", "title", "source_ids", "collection"])) {
      throw new HttpError(400, `metadata_patches[${index}] has an invalid shape`);
    }
    const itemId = boundedString(entry.item_id, `metadata_patches[${index}].item_id`, 128);
    const patch: MetadataPatch = { item_id: itemId };
    if (entry.hint_index !== undefined) {
      if (!Number.isSafeInteger(entry.hint_index) || (entry.hint_index as number) < 0 ||
        (entry.hint_index as number) > MAX_LEGACY_HINT_INDEX) {
        throw new HttpError(400, `metadata_patches[${index}].hint_index must be an integer from 0 to ${MAX_LEGACY_HINT_INDEX}`);
      }
      patch.hint_index = entry.hint_index as number;
    }
    if (entry.title !== undefined) patch.title = boundedTitle(entry.title, `metadata_patches[${index}].title`);
    if (entry.source_ids !== undefined) patch.source_ids = stringList(entry.source_ids, `metadata_patches[${index}].source_ids`, 64, 128);
    if (entry.collection !== undefined) patch.collection = parseCollection(entry.collection);
    if (patch.title === undefined && patch.source_ids === undefined && patch.collection === undefined) {
      throw new HttpError(400, `metadata_patches[${index}] contains no changes`);
    }
    return patch;
  });
  if (new Set(patches.map((patch) => patch.item_id)).size !== patches.length) {
    throw new HttpError(400, "metadata_patches contains duplicate item IDs");
  }
  const hintIndexes = patches.flatMap((patch) => patch.hint_index === undefined ? [] : [patch.hint_index]);
  if (new Set(hintIndexes).size !== hintIndexes.length) {
    throw new HttpError(400, "metadata_patches contains duplicate hint indexes");
  }
  return patches;
}

function parseRelativeDirectory(value: unknown): string {
  const directory = boundedString(value, "target_relative_dir", MAX_TARGET_RELATIVE_DIR_BYTES, { allowEmpty: true });
  if (hasUnpairedSurrogate(directory)) {
    throw new HttpError(400, "target_relative_dir contains an unpaired surrogate");
  }
  if (new TextEncoder().encode(directory).byteLength > MAX_TARGET_RELATIVE_DIR_BYTES) {
    throw new HttpError(400, `target_relative_dir must not exceed ${MAX_TARGET_RELATIVE_DIR_BYTES} UTF-8 bytes`);
  }
  if (directory === "") return "";
  if (directory.startsWith("/") || directory.includes("\\") || directory.endsWith("/") || /^[A-Za-z]:/u.test(directory)) {
    throw new HttpError(400, "target_relative_dir must be a safe relative path");
  }
  const segments = directory.split("/");
  if (segments.some((segment) => {
    const lowercaseSegment = segment.toLowerCase();
    return segment.length === 0 || segment === "." || segment === ".." ||
      lowercaseSegment.startsWith(".telegram-video-downloader-library") ||
      lowercaseSegment.startsWith(".telegram-video-downloader-");
  })) {
    throw new HttpError(400, "target_relative_dir must be a safe relative path");
  }
  return directory;
}

function parsePreviewPayload(value: unknown): FilePreviewPayload {
  if (!isRecord(value) || !onlyKeys(value, ["item_ids", "target_relative_dir", "rename", "conflict", "metadata_patches"])) {
    throw new HttpError(400, "file_preview payload has an invalid shape");
  }

  const metadataPatches = value.metadata_patches === undefined
    ? undefined
    : parseMetadataPatches(value.metadata_patches);
  const itemIds = value.item_ids === undefined
    ? metadataPatches?.map((patch) => patch.item_id)
    : stringList(value.item_ids, "item_ids", MAX_SELECTED_ITEMS, 128);
  if (itemIds !== undefined && itemIds.length > MAX_SELECTED_ITEMS) {
    throw new HttpError(400, `item_ids cannot exceed ${MAX_SELECTED_ITEMS} items`);
  }
  if (metadataPatches !== undefined) {
    if (itemIds === undefined) throw new HttpError(400, "item_ids are required with metadata_patches");
    const selected = new Set(itemIds);
    if (metadataPatches.some((patch) => !selected.has(patch.item_id))) {
      throw new HttpError(400, "Every metadata patch must target a selected item");
    }
  }

  const targetRelativeDir = value.target_relative_dir === undefined
    ? undefined
    : parseRelativeDirectory(value.target_relative_dir);
  if (targetRelativeDir === undefined && metadataPatches === undefined) {
    throw new HttpError(400, "target_relative_dir is required unless metadata_patches are provided");
  }
  if (targetRelativeDir !== undefined && itemIds === undefined) {
    throw new HttpError(400, "item_ids are required when moving files");
  }
  if (targetRelativeDir === undefined && metadataPatches !== undefined && itemIds !== undefined) {
    const patchIds = metadataPatches.map((patch) => patch.item_id).sort();
    const selectedIds = [...itemIds].sort();
    if (patchIds.length !== selectedIds.length || patchIds.some((id, index) => id !== selectedIds[index])) {
      throw new HttpError(400, "Metadata-only item_ids must match metadata_patches exactly");
    }
  }

  const rename = value.rename === undefined ? false : value.rename;
  if (typeof rename !== "boolean") throw new HttpError(400, "rename must be a boolean");
  const conflict = value.conflict === undefined ? "skip" : value.conflict;
  if (conflict !== "skip" && conflict !== "keep_both") {
    throw new HttpError(400, "conflict must be skip or keep_both");
  }

  return {
    ...(itemIds === undefined ? {} : { item_ids: itemIds }),
    ...(targetRelativeDir === undefined ? {} : { target_relative_dir: targetRelativeDir }),
    rename,
    conflict,
    ...(metadataPatches === undefined ? {} : { metadata_patches: metadataPatches }),
  };
}

export function validateActionRequest(value: unknown): ActionRequest {
  if (!isRecord(value) || !onlyKeys(value, ["request_id", "kind", "payload"])) {
    throw new HttpError(400, "Action request has an invalid shape");
  }
  const requestId = boundedString(value.request_id, "request_id", 36);
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(requestId)) {
    throw new HttpError(400, "request_id must be a UUID");
  }
  if (typeof value.kind !== "string" || !["library_scan", "file_preview", "file_confirm", "legacy_import"].includes(value.kind)) {
    throw new HttpError(400, "kind is not supported");
  }
  if (!isRecord(value.payload)) throw new HttpError(400, "payload must be an object");

  const kind = value.kind as Exclude<RequestKind, "telegram">;
  let payload: Record<string, unknown>;
  switch (kind) {
    case "library_scan":
      if (Object.keys(value.payload).length !== 0) throw new HttpError(400, "library_scan payload must be empty");
      payload = {};
      break;
    case "file_preview":
      payload = parsePreviewPayload(value.payload) as unknown as Record<string, unknown>;
      break;
    case "file_confirm":
      if (!onlyKeys(value.payload, ["preview_id", "revision"])) {
        throw new HttpError(400, "file_confirm payload has an invalid shape");
      }
      payload = {
        preview_id: boundedString(value.payload.preview_id, "preview_id", 128),
        revision: boundedString(value.payload.revision, "revision", 256),
      };
      break;
    case "legacy_import": {
      if (!onlyKeys(value.payload, ["text"])) throw new HttpError(400, "legacy_import payload has an invalid shape");
      const text = boundedLegacyText(value.payload.text);
      payload = { text };
      break;
    }
  }
  return { request_id: requestId, kind, payload };
}

export function validateTelegramUpdate(value: unknown): Record<string, unknown> & { update_id: number } {
  if (!isRecord(value) || !Number.isSafeInteger(value.update_id) || (value.update_id as number) < 0) {
    throw new HttpError(400, "Telegram update must include a non-negative integer update_id");
  }
  return value as Record<string, unknown> & { update_id: number };
}

export function isOwnerPrivateTelegramUpdate(
  update: Record<string, unknown> & { update_id: number },
  ownerUserId: string,
): boolean {
  if (!/^[1-9]\d{0,15}$/u.test(ownerUserId)) return false;
  const ownerId = Number(ownerUserId);
  const isOwner = (value: unknown): boolean => isRecord(value) &&
    ((typeof value.id === "number" && Number.isSafeInteger(value.id) && value.id === ownerId) ||
      (typeof value.id === "string" && value.id === ownerUserId)) && value.is_bot !== true;
  const isOwnerPrivateChat = (value: unknown): boolean => {
    if (!isRecord(value) || !isRecord(value.chat) || value.chat.type !== "private") return false;
    const chatId = value.chat.id;
    const correctChat = (typeof chatId === "number" && Number.isSafeInteger(chatId) && chatId === ownerId) || chatId === ownerUserId;
    return correctChat;
  };
  if (isOwnerPrivateChat(update.message) && isOwner((update.message as Record<string, unknown>).from)) return true;
  if (!isRecord(update.callback_query) || !isOwner(update.callback_query.from)) return false;
  return isOwnerPrivateChat(update.callback_query.message);
}

export function validateSnapshot(value: unknown): LocalSnapshot {
  if (!isRecord(value)) throw new HttpError(400, "Snapshot must be an object");
  if (value.revision !== undefined && typeof value.revision !== "string") {
    throw new HttpError(400, "revision must be a string when provided");
  }
  if (!Number.isSafeInteger(value.reported_at) || (value.reported_at as number) <= 0) {
    throw new HttpError(400, "reported_at must be a positive Unix millisecond timestamp");
  }
  if (!Number.isSafeInteger(value.state_version) || (value.state_version as number) <= 0) {
    throw new HttpError(400, "state_version must be a positive safe integer");
  }
  if (!isRecord(value.library)) throw new HttpError(400, "library must be an object");
  const library = value.library;
  if (typeof library.revision !== "string" || !Number.isSafeInteger(library.scanned_at) || (library.scanned_at as number) < 0) {
    throw new HttpError(400, "library revision and scanned_at are required");
  }
  if (!Array.isArray(library.items) || !Array.isArray(library.categories) || !library.categories.every((item) => typeof item === "string") ||
    !Array.isArray(library.warnings) || !library.warnings.every((item) => typeof item === "string")) {
    throw new HttpError(400, "library items, categories, or warnings are invalid");
  }
  if (!Array.isArray(value.previews) || !Array.isArray(value.operations) || !Array.isArray(value.tasks)) {
    throw new HttpError(400, "previews, operations, and tasks must be arrays");
  }
  if (!isRecord(value.settings) || typeof value.settings.download_dir !== "string") {
    throw new HttpError(400, "settings.download_dir is required");
  }
  return value as unknown as LocalSnapshot;
}

export function parseLimit(value: string | null, maximum = 500): number {
  const fallback = Math.min(100, maximum);
  const range = `1 to ${maximum}`;
  if (value === null) return fallback;
  if (!/^\d{1,3}$/u.test(value)) throw new HttpError(400, `limit must be an integer from ${range}`);
  const limit = Number(value);
  if (limit < 1 || limit > maximum) throw new HttpError(400, `limit must be an integer from ${range}`);
  return limit;
}

export function parseAckSeqs(value: unknown): number[] {
  if (!isRecord(value) || !onlyKeys(value, ["seqs"]) || !Array.isArray(value.seqs) || value.seqs.length < 1 || value.seqs.length > 500) {
    throw new HttpError(400, "seqs must contain between 1 and 500 sequence values");
  }
  const seqs = value.seqs;
  if (!seqs.every((seq) => Number.isSafeInteger(seq) && (seq as number) > 0)) {
    throw new HttpError(400, "seqs must contain positive integers");
  }
  return [...new Set(seqs as number[])];
}
