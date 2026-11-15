import { HttpError } from "./types";

const encoder = new TextEncoder();
const identifier = /^[A-Za-z0-9_-]{1,128}$/u;
const torrentHash = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/u;
const controls = /[\u0000-\u001f\u007f-\u009f]/u;
const filters = ["music", "all", "ready", "waiting", "imported"];

function shape(value: Record<string, unknown>, keys: string[]): void {
  if (!Object.keys(value).every((key) => keys.includes(key))) {
    throw new HttpError(400, "Torrent action payload has an invalid shape");
  }
}

function text(value: unknown, field: string, maximum: number, allowEmpty = false): string {
  if (typeof value !== "string" || controls.test(value)
    || (!allowEmpty && value.length === 0) || encoder.encode(value).byteLength > maximum) {
    throw new HttpError(400, `${field} is invalid`);
  }
  return value;
}

function id(value: unknown, field: string): string {
  const result = text(value, field, 128);
  if (!identifier.test(result)) throw new HttpError(400, `${field} is invalid`);
  return result;
}

function hash(value: unknown): string {
  const result = text(value, "hash", 64);
  if (!torrentHash.test(result)) throw new HttpError(400, "hash is invalid");
  return result;
}

function integer(value: unknown, field: string, minimum: number, maximum: number): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new HttpError(400, `${field} is invalid`);
  }
  return value;
}

/** Validate browser-owned fields only; actor_user_id is injected after owner authentication. */
export function validateTorrentActionPayload(value: Record<string, unknown>): Record<string, unknown> {
  switch (value.operation) {
    case "catalog":
      shape(value, ["operation", "page", "query"]);
      return { operation: "catalog", page: integer(value.page ?? 0, "page", 0, 20_000),
        query: text(value.query ?? "", "query", 256, true) };
    case "detail": {
      shape(value, ["operation", "task_id", "hash", "page", "folder_id", "query", "filter"]);
      if (Object.hasOwn(value, "task_id") === Object.hasOwn(value, "hash")) {
        throw new HttpError(400, "Torrent detail requires exactly one task_id or hash");
      }
      const filter = value.filter ?? "music";
      if (typeof filter !== "string" || !filters.includes(filter)) throw new HttpError(400, "filter is invalid");
      return { operation: "detail", ...(Object.hasOwn(value, "task_id")
        ? { task_id: id(value.task_id, "task_id") } : { hash: hash(value.hash) }),
      page: integer(value.page ?? 0, "page", 0, 20_000),
      folder_id: integer(value.folder_id ?? 0, "folder_id", 0, 10_000),
      query: text(value.query ?? "", "query", 256, true), filter };
    }
    case "review": {
      shape(value, ["operation", "task_id", "inventory_revision", "selection_revision", "selected_indexes"]);
      if (!Array.isArray(value.selected_indexes) || value.selected_indexes.length > 5_000) {
        throw new HttpError(400, "selected_indexes must contain at most 5000 file indexes");
      }
      const selected = value.selected_indexes.map((index) => integer(index, "selected index", 0, 4_999));
      if (new Set(selected).size !== selected.length) throw new HttpError(400, "selected_indexes contains duplicates");
      return { operation: "review", task_id: id(value.task_id, "task_id"),
        inventory_revision: integer(value.inventory_revision, "inventory_revision", 1, Number.MAX_SAFE_INTEGER),
        selection_revision: integer(value.selection_revision, "selection_revision", 1, Number.MAX_SAFE_INTEGER),
        selected_indexes: selected.sort((left, right) => left - right) };
    }
    case "confirm":
      shape(value, ["operation", "review_seq", "review_id"]);
      return { operation: "confirm", review_seq: integer(value.review_seq, "review_seq", 1, Number.MAX_SAFE_INTEGER),
        review_id: id(value.review_id, "review_id") };
    case "control":
      shape(value, ["operation", "hash", "control"]);
      if (value.control !== "pause" && value.control !== "resume") throw new HttpError(400, "control is invalid");
      return { operation: "control", hash: hash(value.hash), control: value.control };
    default:
      throw new HttpError(400, "Torrent action operation is not supported");
  }
}
