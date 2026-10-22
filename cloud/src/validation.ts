import { HttpError } from "./types";
import type {
  ActionRequest,
  BoundLibraryMutationIdentity,
  CollectionMetadata,
  DownloadSubmitPayload,
  FilePreviewPayload,
  LocalSnapshot,
  MetadataPatch,
  ChunkedSnapshotContent,
  SnapshotChunkDescriptor,
  SnapshotChunkManifest,
  PreparedSnapshotContent,
  RequestKind,
} from "./types";

export const MAX_WEBHOOK_BODY_BYTES = 1_000_000;
export const MAX_LEGACY_TEXT_BYTES = 256 * 1024;
const MAX_ACTION_BODY_ENVELOPE_BYTES = 4 * 1024;
// JSON may represent each ASCII code unit as a six-byte \\uXXXX escape.
export const MAX_ACTION_BODY_BYTES = MAX_LEGACY_TEXT_BYTES * 6 + MAX_ACTION_BODY_ENVELOPE_BYTES;
export const ACTION_REPLAY_WINDOW_MS = 6 * 24 * 60 * 60 * 1_000;
const MAX_ACTION_CLOCK_SKEW_MS = 5 * 60 * 1_000;
export const MAX_STATE_BODY_BYTES = 12 * 1024 * 1024;
export const MAX_CHUNKED_SNAPSHOT_MANIFEST_BODY_BYTES = 64 * 1024;
export const MAX_SNAPSHOT_CONTENT_CHUNK_BYTES = 32 * 1024;
export const MAX_CHUNKED_SNAPSHOT_BYTES = MAX_STATE_BODY_BYTES;
export const MAX_CHUNKED_SNAPSHOT_CHUNKS = 386;
export const MAX_SNAPSHOT_JSON_NODES = 1_000_000;
export const MAX_JSON_COMPLEXITY_DEPTH = 48;
export const MAX_TARGET_RELATIVE_DIR_BYTES = 1_024;
export const MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES = 255;
export const MAX_SELECTED_ITEMS = 100;
export const MAX_MEDIA_LIBRARIES = 16;
export const MAX_MIGRATION_ITEM_CONFLICTS = 10_000;
export const MAX_MIGRATION_DISCOVERY_WARNINGS = 16;
export const MAX_LIBRARY_ROOT_BYTES = 1_024;
export const MAX_DOWNLOAD_URL_BYTES = 8_192;
const MAX_COLLECTION_ORDER = 0xffff_ffff;
const MAX_LEGACY_HINTS = 10_000;
const MAX_LEGACY_HINT_INDEX = MAX_LEGACY_HINTS - 1;
const MAX_TITLE_SCALAR_VALUES = 500;
const MAX_TITLE_CODE_UNITS = MAX_TITLE_SCALAR_VALUES * 2;
const UTF8_ENCODER = new TextEncoder();

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export async function readJsonBody(
  request: Request,
  maxBytes: number,
): Promise<unknown> {
  return (await readJsonBodyWithByteLength(request, maxBytes)).value;
}

export async function readJsonBodyWithByteLength(
  request: Request,
  maxBytes: number,
): Promise<{ value: unknown; byteLength: number }> {
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
    return { value: JSON.parse(text) as unknown, byteLength: totalBytes };
  } catch {
    throw new HttpError(400, "Request body is not valid JSON");
  }
}

export async function readUtf8ChunkBody(
  request: Request,
  maxBytes = MAX_SNAPSHOT_CONTENT_CHUNK_BYTES,
): Promise<{ bytes: Uint8Array<ArrayBuffer>; data: string }> {
  const contentType = request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
  if (contentType !== "application/octet-stream") {
    throw new HttpError(415, "Content-Type must be application/octet-stream");
  }

  const declaredLength = request.headers.get("content-length");
  if (declaredLength !== null) {
    const length = Number(declaredLength);
    if (!Number.isSafeInteger(length) || length < 0) throw new HttpError(400, "Invalid Content-Length");
    if (length > maxBytes) throw new HttpError(413, "Snapshot content chunk is too large");
  }
  if (request.body === null) throw new HttpError(400, "Request body is required");

  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  while (true) {
    const part = await reader.read();
    if (part.done) break;
    totalBytes += part.value.byteLength;
    if (totalBytes > maxBytes) {
      await reader.cancel();
      throw new HttpError(413, "Snapshot content chunk is too large");
    }
    chunks.push(part.value);
  }
  if (totalBytes === 0) throw new HttpError(400, "Snapshot content chunk must not be empty");

  const bytes = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  let data: string;
  try {
    data = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
  } catch {
    throw new HttpError(400, "Snapshot content chunk must be valid UTF-8");
  }
  return { bytes, data };
}

export function canonicalJson(
  value: unknown,
  options: { maxNodes?: number; maxDepth?: number; complexityStatus?: number } = {},
): string {
  const maxNodes = options.maxNodes ?? 100_000;
  const maxDepth = options.maxDepth ?? MAX_JSON_COMPLEXITY_DEPTH;
  const complexityStatus = options.complexityStatus ?? 400;
  let nodes = 0;
  const visit = (current: unknown, depth: number): string => {
    nodes += 1;
    if (nodes > maxNodes || depth > maxDepth) {
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

function assertSnapshotJsonComplexity(value: unknown): void {
  let nodes = 0;
  const visit = (current: unknown, depth: number): void => {
    nodes += 1;
    if (nodes > MAX_SNAPSHOT_JSON_NODES || depth > MAX_JSON_COMPLEXITY_DEPTH) {
      throw new HttpError(413, "JSON structure is too complex");
    }
    if (Array.isArray(current)) {
      for (const item of current) visit(item, depth + 1);
    } else if (isRecord(current)) {
      for (const key in current) {
        if (Object.hasOwn(current, key)) visit(current[key], depth + 1);
      }
    }
  };
  visit(value, 0);
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
  if (UTF8_ENCODER.encode(directory).byteLength > MAX_TARGET_RELATIVE_DIR_BYTES) {
    throw new HttpError(400, `target_relative_dir must not exceed ${MAX_TARGET_RELATIVE_DIR_BYTES} UTF-8 bytes`);
  }
  if (directory === "") return "";
  if (directory.startsWith("/") || directory.includes("\\") || directory.endsWith("/") || /^[A-Za-z]:/u.test(directory)) {
    throw new HttpError(400, "target_relative_dir must be a safe relative path");
  }
  const segments = directory.split("/");
  if (segments.some((segment) => UTF8_ENCODER.encode(segment).byteLength > MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES)) {
    throw new HttpError(
      400,
      `target_relative_dir components must not exceed ${MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES} UTF-8 bytes`,
    );
  }
  if (segments.some((segment) => {
    const lowercaseSegment = segment
      .replace(/[\u200c-\u200f\u202a-\u202e\u206a-\u206f\ufeff]/gu, "")
      .toLowerCase();
    return segment.length === 0 || segment === "." || segment === ".." ||
      lowercaseSegment.startsWith(".telegram-video-downloader-library") ||
      lowercaseSegment.startsWith(".telegram-video-downloader-");
  })) {
    throw new HttpError(400, "target_relative_dir must be a safe relative path");
  }
  return directory;
}

function parseLibraryId(value: unknown, field = "library_id"): string {
  return boundedUtf8String(value, field, 128);
}

function parseOptionalLibraryId(value: unknown): { library_id?: string } {
  return value === undefined ? {} : { library_id: parseLibraryId(value) };
}

function parseAbsoluteLibraryRoot(value: unknown): string {
  const root = boundedUtf8String(value, "path", MAX_LIBRARY_ROOT_BYTES);
  if (!root.startsWith("/") && !/^[A-Za-z]:[\\/]/u.test(root) && !/^\\\\[^\\]/u.test(root)) {
    throw new HttpError(400, "path must be an absolute path on the deployment machine");
  }
  return root;
}

function parseOptionalRootBinding(value: unknown, field: string): void {
  if (value === undefined || value === null) return;
  if (typeof value !== "string" || UTF8_ENCODER.encode(value).byteLength !== 67
      || !/^v1:[0-9a-f]{64}$/u.test(value)) {
    throw new HttpError(400, `${field} must be an opaque root binding token`);
  }
}

function parseRequiredRootBinding(value: unknown): string {
  if (typeof value !== "string" || UTF8_ENCODER.encode(value).byteLength !== 67
      || !/^v1:[0-9a-f]{64}$/u.test(value)) {
    throw new HttpError(400, "root_binding must be an opaque root identity token");
  }
  return value;
}

function parseBoundLibraryMutationIdentity(value: Record<string, unknown>): BoundLibraryMutationIdentity {
  return {
    library_id: parseLibraryId(value.library_id),
    root_binding: parseRequiredRootBinding(value.root_binding),
  };
}

function parseLibraryName(value: unknown, field = "name"): string {
  return boundedUtf8String(value, field, 100);
}

function parsePreviewPayload(value: unknown): FilePreviewPayload {
  if (!isRecord(value) || !onlyKeys(value, ["library_id", "root_binding", "item_ids", "target_relative_dir", "rename", "conflict", "metadata_patches"])) {
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
    ...parseBoundLibraryMutationIdentity(value),
    ...(itemIds === undefined ? {} : { item_ids: itemIds }),
    ...(targetRelativeDir === undefined ? {} : { target_relative_dir: targetRelativeDir }),
    rename,
    conflict,
    ...(metadataPatches === undefined ? {} : { metadata_patches: metadataPatches }),
  };
}

function parseSettingsAction(kind: string, value: Record<string, unknown>): Record<string, unknown> {
  switch (kind) {
    case "library_register": {
      if (!onlyKeys(value, ["name", "path", "default_category", "set_default", "create_directory"])) {
        throw new HttpError(400, "library_register payload has an invalid shape");
      }
      const setDefault = value.set_default === undefined ? false : value.set_default;
      const createDirectory = value.create_directory === undefined ? false : value.create_directory;
      if (typeof setDefault !== "boolean" || typeof createDirectory !== "boolean") {
        throw new HttpError(400, "set_default and create_directory must be booleans");
      }
      return {
        name: parseLibraryName(value.name),
        path: parseAbsoluteLibraryRoot(value.path),
        default_category: value.default_category === undefined ? "" : parseRelativeDirectory(value.default_category),
        set_default: setDefault,
        create_directory: createDirectory,
      };
    }
    case "library_set_default":
    case "library_remove": {
      if (!onlyKeys(value, ["library_id", "root_binding"])) throw new HttpError(400, `${kind} payload has an invalid shape`);
      return parseBoundLibraryMutationIdentity(value);
    }
    case "library_set_category": {
      if (!onlyKeys(value, ["library_id", "root_binding", "default_category"])) {
        throw new HttpError(400, "library_set_category payload has an invalid shape");
      }
      return {
        ...parseBoundLibraryMutationIdentity(value),
        default_category: parseRelativeDirectory(value.default_category),
      };
    }
    case "library_rename": {
      if (!onlyKeys(value, ["library_id", "root_binding", "name"])) throw new HttpError(400, "library_rename payload has an invalid shape");
      return {
        ...parseBoundLibraryMutationIdentity(value),
        name: parseLibraryName(value.name),
      };
    }
    default:
      throw new HttpError(400, "kind is not supported");
  }
}

function parseMigrationPreview(value: Record<string, unknown>): Record<string, unknown> {
  if (!onlyKeys(value, [
    "source_library_id", "target_library_id", "source_root_binding", "target_root_binding",
    "scope", "item_ids", "include_incomplete_tasks",
    "conflict", "item_conflicts", "set_target_default_on_success",
    "supersedes_preview_seq", "supersedes_preview_id", "supersedes_revision",
  ])) {
    throw new HttpError(400, "library_migration_preview payload has an invalid shape");
  }
  const scope = value.scope;
  if (scope !== "library" && scope !== "selected") {
    throw new HttpError(400, "scope must be library or selected");
  }
  const itemIds = value.item_ids === undefined
    ? undefined
    : stringList(value.item_ids, "item_ids", MAX_SELECTED_ITEMS, 128);
  if (scope === "selected" && itemIds === undefined) {
    throw new HttpError(400, "item_ids are required when scope is selected");
  }
  if (scope === "library" && itemIds !== undefined) {
    throw new HttpError(400, "item_ids must be omitted when scope is library");
  }
  const includeIncompleteTasks = value.include_incomplete_tasks === undefined ? false : value.include_incomplete_tasks;
  const setTargetDefault = value.set_target_default_on_success === undefined ? false : value.set_target_default_on_success;
  if (typeof includeIncompleteTasks !== "boolean" || typeof setTargetDefault !== "boolean") {
    throw new HttpError(400, "migration options must be booleans");
  }
  const conflict = value.conflict === undefined ? "skip" : value.conflict;
  if (conflict !== "skip" && conflict !== "keep_both") {
    throw new HttpError(400, "conflict must be skip or keep_both");
  }
  let itemConflicts: Record<string, "skip" | "keep_both"> | undefined;
  if (value.item_conflicts !== undefined) {
    if (!isRecord(value.item_conflicts)) {
      throw new HttpError(400, "item_conflicts must be an object keyed by media item ID");
    }
    const entries = Object.entries(value.item_conflicts);
    if (entries.length > MAX_MIGRATION_ITEM_CONFLICTS) {
      throw new HttpError(400, `item_conflicts cannot exceed ${MAX_MIGRATION_ITEM_CONFLICTS} entries`);
    }
    const selectedIds = new Set(itemIds ?? []);
    itemConflicts = Object.create(null) as Record<string, "skip" | "keep_both">;
    for (const [rawItemId, policy] of entries) {
      const itemId = boundedUtf8String(rawItemId, "item_conflicts item ID", 256);
      if (itemId !== rawItemId) throw new HttpError(400, "item_conflicts IDs must not have surrounding whitespace");
      if (scope === "selected" && !selectedIds.has(itemId)) {
        throw new HttpError(400, "item_conflicts may only target selected media item IDs");
      }
      if (policy !== "skip" && policy !== "keep_both") {
        throw new HttpError(400, "each item_conflicts value must be skip or keep_both");
      }
      itemConflicts[itemId] = policy;
    }
  }
  const sourceLibraryId = parseLibraryId(value.source_library_id, "source_library_id");
  const targetLibraryId = parseLibraryId(value.target_library_id, "target_library_id");
  const sourceRootBinding = parseRequiredRootBinding(value.source_root_binding);
  const targetRootBinding = parseRequiredRootBinding(value.target_root_binding);
  if (sourceLibraryId === targetLibraryId) throw new HttpError(400, "source and target libraries must differ");
  const supersessionFields = ["supersedes_preview_seq", "supersedes_preview_id", "supersedes_revision"] as const;
  const hasSupersessionField = supersessionFields.map((field) => Object.hasOwn(value, field));
  const hasSupersession = hasSupersessionField.some(Boolean);
  if (hasSupersession && !hasSupersessionField.every(Boolean)) {
    throw new HttpError(400, "all supersedes preview fields must be provided together");
  }
  let supersedesPreviewSeq: number | undefined;
  let supersedesPreviewId: string | undefined;
  let supersedesRevision: string | undefined;
  if (hasSupersession) {
    if (!Number.isSafeInteger(value.supersedes_preview_seq) || (value.supersedes_preview_seq as number) <= 0) {
      throw new HttpError(400, "supersedes_preview_seq must be a positive safe integer");
    }
    supersedesPreviewSeq = value.supersedes_preview_seq as number;
    supersedesPreviewId = boundedString(value.supersedes_preview_id, "supersedes_preview_id", 128);
    supersedesRevision = boundedString(value.supersedes_revision, "supersedes_revision", 256);
  }
  return {
    source_library_id: sourceLibraryId,
    target_library_id: targetLibraryId,
    source_root_binding: sourceRootBinding,
    target_root_binding: targetRootBinding,
    scope,
    ...(itemIds === undefined ? {} : { item_ids: itemIds }),
    include_incomplete_tasks: includeIncompleteTasks,
    conflict,
    ...(itemConflicts === undefined ? {} : { item_conflicts: itemConflicts }),
    set_target_default_on_success: setTargetDefault,
    ...(hasSupersession ? {
      supersedes_preview_seq: supersedesPreviewSeq,
      supersedes_preview_id: supersedesPreviewId,
      supersedes_revision: supersedesRevision,
    } : {}),
  };
}

function parseDownloadSubmit(value: Record<string, unknown>): DownloadSubmitPayload {
  if (!onlyKeys(value, ["url", "library_id", "root_binding", "category"])) {
    throw new HttpError(400, "download_submit payload has an invalid shape");
  }
  const url = boundedUtf8String(value.url, "url", MAX_DOWNLOAD_URL_BYTES);
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new HttpError(400, "url must be an absolute HTTP or HTTPS URL");
  }
  if ((parsed.protocol !== "https:" && parsed.protocol !== "http:") || !parsed.hostname || parsed.username || parsed.password) {
    throw new HttpError(400, "url must be an absolute HTTP or HTTPS URL without embedded credentials");
  }
  return {
    url,
    library_id: parseLibraryId(value.library_id),
    root_binding: parseRequiredRootBinding(value.root_binding),
    ...(value.category === undefined ? {} : { category: parseRelativeDirectory(value.category) }),
  };
}

const MIGRATION_DISCOVERY_WARNING_KINDS = [
  "disappeared",
  "unreadable",
  "oversized",
  "malformed",
  "source_binding_mismatch",
  "source_unavailable",
  "enumeration_failed",
  "maintenance_failed",
] as const;

function validateMigrationDiscoveryWarnings(library: Record<string, unknown>): void {
  const warnings = library.migration_discovery_warnings;
  if (warnings !== undefined) {
    if (!Array.isArray(warnings) || warnings.length > MAX_MIGRATION_DISCOVERY_WARNINGS) {
      throw new HttpError(400, `library.migration_discovery_warnings must contain at most ${MAX_MIGRATION_DISCOVERY_WARNINGS} entries`);
    }
    for (const [index, candidate] of warnings.entries()) {
      if (!isRecord(candidate)) throw new HttpError(400, `library.migration_discovery_warnings[${index}] must be an object`);
      const kind = candidate.kind;
      if (!MIGRATION_DISCOVERY_WARNING_KINDS.includes(kind as typeof MIGRATION_DISCOVERY_WARNING_KINDS[number])) {
        throw new HttpError(400, `library.migration_discovery_warnings[${index}].kind is invalid`);
      }
      const rootLevelEnumeration = kind === "enumeration_failed"
        && candidate.source_library_id === ""
        && candidate.record_name === "";
      if (rootLevelEnumeration) {
        // A registry-level enumeration failure has no single source library or manifest.
      } else {
        parseLibraryId(candidate.source_library_id, `library.migration_discovery_warnings[${index}].source_library_id`);
        const sourceLevelWithoutManifest = candidate.record_name === ""
          && (kind === "source_unavailable" || kind === "maintenance_failed" || kind === "enumeration_failed");
        if (!sourceLevelWithoutManifest) {
          const recordName = boundedUtf8String(candidate.record_name, `library.migration_discovery_warnings[${index}].record_name`, 160);
          if (recordName === "." || recordName === ".." || recordName.includes("/") || recordName.includes("\\")) {
            throw new HttpError(400, `library.migration_discovery_warnings[${index}].record_name must be a basename`);
          }
        }
      }
      boundedUtf8String(candidate.message, `library.migration_discovery_warnings[${index}].message`, 192);
    }
  }
  const omitted = library.migration_discovery_warning_omitted_count;
  if (omitted !== undefined && (!Number.isSafeInteger(omitted) || (omitted as number) < 0)) {
    throw new HttpError(400, "library.migration_discovery_warning_omitted_count must be a non-negative safe integer");
  }
}

function validateAppliedSettings(value: unknown): void {
  if (!isRecord(value) || !onlyKeys(value, ["revision", "default_library_id", "default_category", "libraries"])) {
    throw new HttpError(400, "settings.applied has an invalid shape");
  }
  boundedString(value.revision, "settings.applied.revision", 256);
  const defaultLibraryId = value.default_library_id;
  if (typeof defaultLibraryId !== "string" || new TextEncoder().encode(defaultLibraryId).byteLength > 128) {
    throw new HttpError(400, "settings.applied.default_library_id is invalid");
  }
  if (typeof value.default_category !== "string") {
    throw new HttpError(400, "settings.applied.default_category must be a string");
  }
  parseRelativeDirectory(value.default_category);
  if (!Array.isArray(value.libraries) || value.libraries.length > MAX_MEDIA_LIBRARIES) {
    throw new HttpError(400, `settings.applied.libraries must contain at most ${MAX_MEDIA_LIBRARIES} entries`);
  }
  const ids = new Set<string>();
  let defaults = 0;
  for (const [index, candidate] of value.libraries.entries()) {
    if (!isRecord(candidate) || !onlyKeys(candidate, ["id", "name", "root", "default_category", "is_default", "root_binding"])) {
      throw new HttpError(400, `settings.applied.libraries[${index}] has an invalid shape`);
    }
    const id = parseLibraryId(candidate.id, `settings.applied.libraries[${index}].id`);
    if (ids.has(id)) throw new HttpError(400, "settings.applied.libraries contains duplicate IDs");
    ids.add(id);
    parseLibraryName(candidate.name, `settings.applied.libraries[${index}].name`);
    parseAbsoluteLibraryRoot(candidate.root);
    parseOptionalRootBinding(candidate.root_binding, `settings.applied.libraries[${index}].root_binding`);
    if (typeof candidate.default_category !== "string") {
      throw new HttpError(400, `settings.applied.libraries[${index}].default_category must be a string`);
    }
    parseRelativeDirectory(candidate.default_category);
    if (typeof candidate.is_default !== "boolean") {
      throw new HttpError(400, `settings.applied.libraries[${index}].is_default must be a boolean`);
    }
    if (candidate.is_default) defaults += 1;
  }
  if (value.libraries.length > 0 && (defaults !== 1 || !ids.has(defaultLibraryId))) {
    throw new HttpError(400, "settings.applied must identify exactly one registered default library");
  }
}

function validateLibraryDescriptors(library: Record<string, unknown>): void {
  if (library.libraries !== undefined) {
    if (!Array.isArray(library.libraries) || library.libraries.length > MAX_MEDIA_LIBRARIES) {
      throw new HttpError(400, `library.libraries must contain at most ${MAX_MEDIA_LIBRARIES} entries`);
    }
    const ids = new Set<string>();
    for (const [index, candidate] of library.libraries.entries()) {
      if (!isRecord(candidate)) throw new HttpError(400, `library.libraries[${index}] must be an object`);
      const id = parseLibraryId(candidate.id, `library.libraries[${index}].id`);
      if (ids.has(id)) throw new HttpError(400, "library.libraries contains duplicate IDs");
      ids.add(id);
      parseLibraryName(candidate.name, `library.libraries[${index}].name`);
      parseAbsoluteLibraryRoot(candidate.root);
      parseOptionalRootBinding(candidate.root_binding, `library.libraries[${index}].root_binding`);
      if (candidate.default_category !== undefined && candidate.default_category !== null) {
        if (typeof candidate.default_category !== "string") {
          throw new HttpError(400, `library.libraries[${index}].default_category must be a string`);
        }
        parseRelativeDirectory(candidate.default_category);
      }
      if (candidate.is_default !== undefined && typeof candidate.is_default !== "boolean") {
        throw new HttpError(400, `library.libraries[${index}].is_default must be a boolean`);
      }
      if (candidate.availability !== undefined &&
        !["available", "unavailable", "offline", "unknown"].includes(String(candidate.availability))) {
        throw new HttpError(400, `library.libraries[${index}].availability is invalid`);
      }
      for (const field of ["media_count", "scanned_at", "free_bytes", "total_bytes", "incomplete_task_count", "staging_bytes"]) {
        const fieldValue = candidate[field];
        if (fieldValue !== undefined && fieldValue !== null &&
          (!Number.isSafeInteger(fieldValue) || (fieldValue as number) < 0)) {
          throw new HttpError(400, `library.libraries[${index}].${field} must be a non-negative safe integer`);
        }
      }
    }
  }
  if (library.migrations !== undefined &&
    (!Array.isArray(library.migrations) || library.migrations.length > 32)) {
    throw new HttpError(400, "library.migrations must contain at most 32 entries");
  }
  validateMigrationDiscoveryWarnings(library);
}

export function validateActionRequest(value: unknown): ActionRequest {
  if (!isRecord(value) || !onlyKeys(value, ["request_id", "kind", "payload", "queued_at_ms", "replay_expires_at_ms"])) {
    throw new HttpError(400, "Action request has an invalid shape");
  }
  const replayWindow = parseActionReplayWindow(value, Date.now());
  const requestId = boundedString(value.request_id, "request_id", 36);
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(requestId)) {
    throw new HttpError(400, "request_id must be a UUID");
  }
  const supportedKinds = [
    "library_scan", "library_register", "library_set_default", "library_set_category", "library_rename", "library_remove",
    "file_preview", "file_confirm", "legacy_import", "download_submit", "library_migration_preview",
    "library_migration_confirm", "library_migration_control",
  ];
  if (typeof value.kind !== "string" || !supportedKinds.includes(value.kind)) {
    throw new HttpError(400, "kind is not supported");
  }
  if (!isRecord(value.payload)) throw new HttpError(400, "payload must be an object");

  const kind = value.kind as Exclude<RequestKind, "telegram">;
  let payload: Record<string, unknown>;
  switch (kind) {
    case "library_scan":
      if (!onlyKeys(value.payload, ["library_id"])) throw new HttpError(400, "library_scan payload has an invalid shape");
      payload = parseOptionalLibraryId(value.payload.library_id);
      break;
    case "file_preview":
      payload = parsePreviewPayload(value.payload) as unknown as Record<string, unknown>;
      break;
    case "file_confirm":
      if (!onlyKeys(value.payload, ["preview_id", "revision", "library_id"])) {
        throw new HttpError(400, "file_confirm payload has an invalid shape");
      }
      payload = {
        preview_id: boundedString(value.payload.preview_id, "preview_id", 128),
        revision: boundedString(value.payload.revision, "revision", 256),
        ...parseOptionalLibraryId(value.payload.library_id),
      };
      break;
    case "legacy_import": {
      if (!onlyKeys(value.payload, ["text", "library_id", "root_binding"])) throw new HttpError(400, "legacy_import payload has an invalid shape");
      const text = boundedLegacyText(value.payload.text);
      payload = { text, ...parseBoundLibraryMutationIdentity(value.payload) };
      break;
    }
    case "download_submit":
      payload = parseDownloadSubmit(value.payload);
      break;
    case "library_register":
    case "library_set_default":
    case "library_set_category":
    case "library_rename":
    case "library_remove":
      payload = parseSettingsAction(kind, value.payload);
      break;
    case "library_migration_preview":
      payload = parseMigrationPreview(value.payload);
      break;
    case "library_migration_confirm":
      if (!onlyKeys(value.payload, ["preview_id", "revision"])) {
        throw new HttpError(400, "library_migration_confirm payload has an invalid shape");
      }
      payload = {
        preview_id: boundedString(value.payload.preview_id, "preview_id", 128),
        revision: boundedString(value.payload.revision, "revision", 256),
      };
      break;
    case "library_migration_control": {
      if (!onlyKeys(value.payload, ["migration_id", "control"])) {
        throw new HttpError(400, "library_migration_control payload has an invalid shape");
      }
      const control = value.payload.control;
      if (!["pause", "resume", "stop_remaining", "retry_failed", "finalize_terminal"].includes(String(control))) {
        throw new HttpError(400, "control is not supported");
      }
      payload = {
        migration_id: boundedString(value.payload.migration_id, "migration_id", 128),
        control,
      };
      break;
    }
  }
  return { request_id: requestId, kind, payload, ...replayWindow };
}

function parseActionReplayWindow(
  value: Record<string, unknown>,
  now: number,
): Pick<ActionRequest, "queued_at_ms" | "replay_expires_at_ms"> {
  const queuedAt = value.queued_at_ms;
  const expiresAt = value.replay_expires_at_ms;
  if (!Number.isSafeInteger(queuedAt) || (queuedAt as number) <= 0
      || !Number.isSafeInteger(expiresAt) || (expiresAt as number) <= 0) {
    throw new HttpError(400, "Action replay window metadata is required and must contain safe integer timestamps");
  }
  const queuedAtMs = queuedAt as number;
  const expiresAtMs = expiresAt as number;
  if (expiresAtMs <= queuedAtMs || expiresAtMs - queuedAtMs > ACTION_REPLAY_WINDOW_MS) {
    throw new HttpError(400, "Action replay window metadata is invalid");
  }
  if (!Number.isSafeInteger(now) || now <= 0 || queuedAtMs > now + MAX_ACTION_CLOCK_SKEW_MS) {
    throw new HttpError(400, "Action replay window timestamp is too far in the future");
  }
  return { queued_at_ms: queuedAtMs, replay_expires_at_ms: expiresAtMs };
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
  if (value.settings.applied !== undefined) validateAppliedSettings(value.settings.applied);
  validateLibraryDescriptors(library);
  return value as unknown as LocalSnapshot;
}

export function validatePreparedSnapshot(value: unknown): PreparedSnapshotContent {
  if (!isRecord(value) || !onlyKeys(value, ["format", "state_version", "reported_at", "library_json", "state_json"]) ||
    !["format", "state_version", "reported_at", "library_json", "state_json"].every((key) => Object.hasOwn(value, key))) {
    throw new HttpError(400, "Prepared snapshot envelope has an invalid shape");
  }
  if (value.format !== "prepared-v1" || typeof value.library_json !== "string" || typeof value.state_json !== "string") {
    throw new HttpError(400, "Prepared snapshot envelope has an invalid format");
  }

  let library: unknown;
  let state: unknown;
  try {
    library = JSON.parse(value.library_json) as unknown;
  } catch {
    throw new HttpError(400, "Prepared library JSON is invalid");
  }
  try {
    state = JSON.parse(value.state_json) as unknown;
  } catch {
    throw new HttpError(400, "Prepared state JSON is invalid");
  }
  if (!isRecord(library) || !isRecord(state)) {
    throw new HttpError(400, "Prepared library and state JSON must contain objects");
  }
  if (!onlyKeys(state, ["revision", "previews", "operations", "tasks", "settings"]) ||
    !["previews", "operations", "tasks", "settings"].every((key) => Object.hasOwn(state, key))) {
    throw new HttpError(400, "Prepared state JSON has an invalid shape");
  }

  assertSnapshotJsonComplexity(library);
  assertSnapshotJsonComplexity(state);
  const snapshot = validateSnapshot({
    state_version: value.state_version,
    reported_at: value.reported_at,
    library,
    ...state,
  });
  return { snapshot, libraryJson: value.library_json, stateJson: value.state_json };
}

function validateSnapshotChunkManifest(value: unknown, field: string): SnapshotChunkManifest {
  if (!isRecord(value) || !onlyKeys(value, ["bytes", "chunks"]) ||
    !Object.hasOwn(value, "bytes") || !Object.hasOwn(value, "chunks")) {
    throw new HttpError(400, `${field} manifest has an invalid shape`);
  }
  if (!Number.isSafeInteger(value.bytes) || (value.bytes as number) <= 0 ||
    (value.bytes as number) > MAX_CHUNKED_SNAPSHOT_BYTES) {
    throw new HttpError(413, `${field} manifest byte length is invalid`);
  }
  if (!Array.isArray(value.chunks) || value.chunks.length === 0 ||
    value.chunks.length > MAX_CHUNKED_SNAPSHOT_CHUNKS) {
    throw new HttpError(400, `${field} manifest must contain between 1 and ${MAX_CHUNKED_SNAPSHOT_CHUNKS} chunks`);
  }
  let totalBytes = 0;
  const chunks: SnapshotChunkDescriptor[] = [];
  for (const [index, candidate] of value.chunks.entries()) {
    if (!isRecord(candidate) || !onlyKeys(candidate, ["sha256", "bytes"]) ||
      !Object.hasOwn(candidate, "sha256") || !Object.hasOwn(candidate, "bytes") ||
      typeof candidate.sha256 !== "string" || !/^[0-9a-f]{64}$/u.test(candidate.sha256) ||
      !Number.isSafeInteger(candidate.bytes) || (candidate.bytes as number) < 1 ||
      (candidate.bytes as number) > MAX_SNAPSHOT_CONTENT_CHUNK_BYTES) {
      throw new HttpError(400, `${field}.chunks[${index}] is malformed`);
    }
    totalBytes += candidate.bytes as number;
    if (totalBytes > MAX_CHUNKED_SNAPSHOT_BYTES) {
      throw new HttpError(413, `${field} manifest exceeds the snapshot size limit`);
    }
    chunks.push({ sha256: candidate.sha256, bytes: candidate.bytes as number });
  }
  if (totalBytes !== value.bytes) throw new HttpError(400, `${field} manifest byte length does not match its chunks`);
  return { bytes: value.bytes as number, chunks };
}

export function validateChunkedSnapshot(value: unknown): ChunkedSnapshotContent {
  if (!isRecord(value) || !onlyKeys(value, ["format", "state_version", "reported_at", "library_revision", "library", "state"]) ||
    !["format", "state_version", "reported_at", "library_revision", "library", "state"].every((key) => Object.hasOwn(value, key))) {
    throw new HttpError(400, "Chunked snapshot envelope has an invalid shape");
  }
  if (value.format !== "chunked-v1") throw new HttpError(400, "Chunked snapshot envelope has an invalid format");
  if (!Number.isSafeInteger(value.state_version) || (value.state_version as number) <= 0) {
    throw new HttpError(400, "state_version must be a positive safe integer");
  }
  if (!Number.isSafeInteger(value.reported_at) || (value.reported_at as number) <= 0) {
    throw new HttpError(400, "reported_at must be a positive Unix millisecond timestamp");
  }
  if (typeof value.library_revision !== "string" || value.library_revision.length === 0 ||
    value.library_revision.length > 256 || /[\u0000-\u001f\u007f]/u.test(value.library_revision)) {
    throw new HttpError(400, "library_revision is invalid");
  }
  const library = validateSnapshotChunkManifest(value.library, "library");
  const state = validateSnapshotChunkManifest(value.state, "state");
  if (library.chunks.length + state.chunks.length > MAX_CHUNKED_SNAPSHOT_CHUNKS) {
    throw new HttpError(413, "Snapshot exceeds the content chunk limit");
  }
  if (library.bytes + state.bytes > MAX_CHUNKED_SNAPSHOT_BYTES) {
    throw new HttpError(413, "Snapshot exceeds the storage size limit");
  }
  return {
    state_version: value.state_version as number,
    reported_at: value.reported_at as number,
    library_revision: value.library_revision,
    library,
    state,
  };
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
