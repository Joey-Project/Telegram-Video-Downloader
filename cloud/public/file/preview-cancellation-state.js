export const PREVIEW_CANCELLATION_STORAGE_KEY = "file-manager.preview-cancellations.v1";

const FORMAT_VERSION = 1;
const MAX_ENTRIES = 100;
const MAX_RAW_BYTES = 256 * 1024;
const MAX_KEY_BYTES = 1_024;
const MAX_ID_BYTES = 128;
const MAX_REVISION_BYTES = 256;
const MAX_ERROR_BYTES = 1_024;
const REQUEST_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const CONTROL_CHARACTERS = /[\u0000-\u001f\u007f-\u009f]/u;
const TRACKER_STATUSES = new Set(["queued", "submitted", "unknown", "failed"]);
const TRACKER_KEYS = ["requestId", "seq", "status", "error"];
const ENVELOPE_KEYS = ["version", "entries"];
const ENTRY_KEYS = ["key", "tracker"];

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasExactKeys(value, expected) {
  if (!isRecord(value)) return false;
  const keys = Reflect.ownKeys(value);
  return keys.length === expected.length
    && keys.every((key) => typeof key === "string" && expected.includes(key));
}

function utf8ByteLength(value) {
  return new TextEncoder().encode(value).byteLength;
}

function withinUtf8Limit(value, limit) {
  return typeof value === "string"
    && value.length <= limit
    && utf8ByteLength(value) <= limit;
}

function validIdentityText(value, maxBytes) {
  return typeof value === "string"
    && value.length > 0
    && value.trim() === value
    && !CONTROL_CHARACTERS.test(value)
    && withinUtf8Limit(value, maxBytes);
}

function identityFromKey(key) {
  if (!withinUtf8Limit(key, MAX_KEY_BYTES)) return null;
  let identity;
  try {
    identity = JSON.parse(key);
  } catch {
    return null;
  }
  if (!Array.isArray(identity)
      || identity.length !== 4
      || !["file_preview", "library_migration_preview"].includes(identity[0])
      || !Number.isSafeInteger(identity[1])
      || identity[1] <= 0
      || !validIdentityText(identity[2], MAX_ID_BYTES)
      || !validIdentityText(identity[3], MAX_REVISION_BYTES)
      || JSON.stringify(identity) !== key) return null;
  return {
    kind: identity[0],
    targetSeq: identity[1],
    targetId: identity[2],
    revision: identity[3],
  };
}

function normalizedStatus(value) {
  const status = value === "outcome_unknown" ? "unknown" : value;
  return typeof status === "string" && TRACKER_STATUSES.has(status) ? status : null;
}

function validateTracker(value, sanitizeError) {
  if (!hasExactKeys(value, TRACKER_KEYS)) return null;
  const status = normalizedStatus(value.status);
  if (!status
      || (value.seq !== null && (!Number.isSafeInteger(value.seq) || value.seq <= 0))) return null;
  if (value.requestId === null) {
    if (status !== "failed" || value.seq !== null) return null;
  } else if (typeof value.requestId !== "string" || !REQUEST_ID_PATTERN.test(value.requestId)) {
    return null;
  }

  let error = value.error;
  if (error !== null && typeof error !== "string") return null;
  if (typeof error === "string") {
    if (sanitizeError) {
      error = truncateUtf8(error, MAX_ERROR_BYTES);
    } else if (CONTROL_CHARACTERS.test(error) || !withinUtf8Limit(error, MAX_ERROR_BYTES)) {
      return null;
    }
  }
  return { requestId: value.requestId, seq: value.seq, status, error };
}

function truncateUtf8(value, limit) {
  let result = "";
  let bytes = 0;
  for (const rawCharacter of value) {
    const character = CONTROL_CHARACTERS.test(rawCharacter) ? " " : rawCharacter;
    const characterBytes = utf8ByteLength(character);
    if (bytes + characterBytes > limit) break;
    result += character;
    bytes += characterBytes;
  }
  return result;
}

function emptyMap() {
  return new Map();
}

export function readPreviewCancellations(storage) {
  try {
    if (!storage || typeof storage.getItem !== "function") return emptyMap();
    const raw = storage.getItem(PREVIEW_CANCELLATION_STORAGE_KEY);
    if (!withinUtf8Limit(raw, MAX_RAW_BYTES)) return emptyMap();
    const envelope = JSON.parse(raw);
    if (!hasExactKeys(envelope, ENVELOPE_KEYS)
        || envelope.version !== FORMAT_VERSION
        || !Array.isArray(envelope.entries)
        || envelope.entries.length > MAX_ENTRIES) return emptyMap();

    const cancellations = new Map();
    for (const entry of envelope.entries) {
      if (!hasExactKeys(entry, ENTRY_KEYS)
          || typeof entry.key !== "string"
          || !identityFromKey(entry.key)) return emptyMap();
      const tracker = validateTracker(entry.tracker, false);
      if (!tracker || cancellations.has(entry.key)) return emptyMap();
      cancellations.set(entry.key, tracker);
    }
    return cancellations;
  } catch {
    return emptyMap();
  }
}

export function writePreviewCancellations(storage, cancellations) {
  try {
    if (!storage || !(cancellations instanceof Map)) return false;
    if (cancellations.size === 0) {
      if (typeof storage.removeItem !== "function") return false;
      storage.removeItem(PREVIEW_CANCELLATION_STORAGE_KEY);
      return true;
    }
    if (cancellations.size > MAX_ENTRIES) return false;

    const entries = [];
    for (const [key, value] of cancellations) {
      if (typeof key !== "string" || !identityFromKey(key)) return false;
      const tracker = validateTracker(value, true);
      if (!tracker) return false;
      if (tracker.status === "failed") continue;
      entries.push({ key, tracker });
    }
    entries.sort((left, right) => (left.key < right.key ? -1 : left.key > right.key ? 1 : 0));
    if (entries.length === 0) {
      if (typeof storage.removeItem !== "function") return false;
      storage.removeItem(PREVIEW_CANCELLATION_STORAGE_KEY);
      return true;
    }
    if (typeof storage.setItem !== "function") return false;
    const serialized = JSON.stringify({ version: FORMAT_VERSION, entries });
    if (!withinUtf8Limit(serialized, MAX_RAW_BYTES)) return false;
    storage.setItem(PREVIEW_CANCELLATION_STORAGE_KEY, serialized);
    return true;
  } catch {
    return false;
  }
}
