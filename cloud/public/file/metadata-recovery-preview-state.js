export const RECOVERY_PREVIEW_STORAGE_KEY = "file-manager.metadata-recovery-preview.v1";

const MAX_PERSISTED_BYTES = 8_192;
const MAX_LIBRARY_ID_BYTES = 128;
const MAX_ERROR_BYTES = 1_024;
const REQUEST_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const ROOT_BINDING_PATTERN = /^v1:[0-9a-f]{64}$/u;
const PREVIEW_STATUSES = new Set([
  "queued",
  "submitted",
  "rejected",
  "unknown",
  "failed",
  "stale",
  "cancelled",
  "expired",
]);
const TRACKER_KEYS = ["requestId", "seq", "libraryId", "rootBinding", "status", "error"];
const CONTROL_CHARACTERS = /[\u0000-\u001f\u007f-\u009f]/u;

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function onlyTrackerKeys(value) {
  return Object.keys(value).every((key) => TRACKER_KEYS.includes(key));
}

function utf8ByteLength(value) {
  return new TextEncoder().encode(value).byteLength;
}

function withinUtf8Limit(value, limit) {
  if (typeof value !== "string" || value.length > limit) return false;
  return utf8ByteLength(value) <= limit;
}

function validLibraryId(value) {
  return typeof value === "string"
    && value.length > 0
    && value.trim() === value
    && !CONTROL_CHARACTERS.test(value)
    && withinUtf8Limit(value, MAX_LIBRARY_ID_BYTES);
}

function validIdentityFields(value) {
  return typeof value.requestId === "string"
      && REQUEST_ID_PATTERN.test(value.requestId)
    && (value.seq === null || Number.isSafeInteger(value.seq) && value.seq > 0)
    && validLibraryId(value.libraryId)
    && typeof value.rootBinding === "string"
      && ROOT_BINDING_PATTERN.test(value.rootBinding)
    && typeof value.status === "string"
      && PREVIEW_STATUSES.has(value.status);
}

function validTracker(value) {
  if (!isRecord(value)
      || !onlyTrackerKeys(value)
      || !TRACKER_KEYS.every((key) => Object.hasOwn(value, key))
      || !validIdentityFields(value)
      || typeof value.error !== "string"
      || CONTROL_CHARACTERS.test(value.error)
      || !withinUtf8Limit(value.error, MAX_ERROR_BYTES)) return null;
  return {
    requestId: value.requestId,
    seq: value.seq,
    libraryId: value.libraryId,
    rootBinding: value.rootBinding,
    status: value.status,
    error: value.error,
  };
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

export function readRecoveryPreviewRequest(storage) {
  try {
    if (!storage || typeof storage.getItem !== "function") return null;
    const raw = storage.getItem(RECOVERY_PREVIEW_STORAGE_KEY);
    if (!withinUtf8Limit(raw, MAX_PERSISTED_BYTES)) return null;
    const parsed = JSON.parse(raw);
    const tracker = validTracker(parsed);
    return tracker ? Object.freeze(tracker) : null;
  } catch {
    return null;
  }
}

export function writeRecoveryPreviewRequest(storage, trackerOrNull) {
  try {
    if (!storage) return false;
    if (trackerOrNull === null) {
      if (typeof storage.removeItem !== "function") return false;
      storage.removeItem(RECOVERY_PREVIEW_STORAGE_KEY);
      return true;
    }
    if (typeof storage.setItem !== "function"
        || !isRecord(trackerOrNull)
        || !onlyTrackerKeys(trackerOrNull)
        || !TRACKER_KEYS.every((key) => Object.hasOwn(trackerOrNull, key))
        || !validIdentityFields(trackerOrNull)
        || typeof trackerOrNull.error !== "string") return false;

    const tracker = {
      requestId: trackerOrNull.requestId,
      seq: trackerOrNull.seq,
      libraryId: trackerOrNull.libraryId,
      rootBinding: trackerOrNull.rootBinding,
      status: trackerOrNull.status,
      error: truncateUtf8(trackerOrNull.error, MAX_ERROR_BYTES),
    };
    const serialized = JSON.stringify(tracker);
    if (!withinUtf8Limit(serialized, MAX_PERSISTED_BYTES)) return false;
    storage.setItem(RECOVERY_PREVIEW_STORAGE_KEY, serialized);
    return true;
  } catch {
    return false;
  }
}
