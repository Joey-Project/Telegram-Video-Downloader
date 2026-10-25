import { describe, expect, it, vi } from "vitest";
import {
  PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY,
  PREVIEW_CANCELLATION_STORAGE_KEY,
  readPreviewCancellationDismissals,
  readPreviewCancellations,
  writePreviewCancellationDismissals,
  writePreviewCancellations,
  type PreviewCancellationStorage,
  type PreviewCancellationTracker,
} from "../public/file/preview-cancellation-state.js";

const REQUEST_ID = "00000000-0000-4000-8000-000000000001";
const REQUEST_ID_TWO = "00000000-0000-4000-8000-000000000002";

function cancellationKey(
  kind: "file_preview" | "library_migration_preview" = "file_preview",
  targetSeq = 42,
  targetId = "preview-a",
  revision = "revision-a",
): string {
  return JSON.stringify([kind, targetSeq, targetId, revision]);
}

function tracker(overrides: Partial<PreviewCancellationTracker> = {}): PreviewCancellationTracker {
  return {
    requestId: REQUEST_ID,
    seq: null,
    status: "queued",
    error: "",
    ...overrides,
  };
}

function memoryStorage(initial: Record<string, string> = {}) {
  const values = new Map(Object.entries(initial));
  const storage: PreviewCancellationStorage = {
    getItem(key) { return values.get(key) ?? null; },
    setItem(key, value) { values.set(key, value); },
    removeItem(key) { values.delete(key); },
  };
  return { storage, values };
}

function envelope(entries: Array<{ key: string; tracker: unknown }>, version = 1): string {
  return JSON.stringify({ version, entries });
}

describe("durable preview cancellation state", () => {
  it("round-trips bounded opaque trackers as mutable map values", () => {
    const { storage, values } = memoryStorage();
    const queuedKey = cancellationKey();
    const submittedKey = cancellationKey("library_migration_preview", 43, "migration-a", "revision-b");
    const unknownKey = cancellationKey("file_preview", 44, "preview-b", "revision-c");
    const cancellations = new Map<string, PreviewCancellationTracker>([
      [queuedKey, tracker()],
      [submittedKey, tracker({ requestId: REQUEST_ID_TWO, seq: 93, status: "submitted", error: null })],
      [unknownKey, tracker({ requestId: REQUEST_ID_TWO, status: "unknown", error: "Delivery could not be confirmed." })],
    ]);

    expect(PREVIEW_CANCELLATION_STORAGE_KEY).toBe("file-manager.preview-cancellations.v1");
    expect(writePreviewCancellations(storage, cancellations)).toBe(true);
    const persisted = JSON.parse(values.get(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null");
    const sortedEntries = [...cancellations.entries()].sort(([left], [right]) => (
      left < right ? -1 : left > right ? 1 : 0
    ));
    expect(persisted.version).toBe(1);
    expect(persisted.entries.map((entry: { key: string }) => entry.key)).toEqual(
      sortedEntries.map(([key]) => key),
    );
    expect(JSON.stringify(persisted)).not.toMatch(/payload|title|token|root_path/iu);

    const restored = readPreviewCancellations(storage);
    expect([...restored.entries()]).toEqual(sortedEntries);
    const restoredTracker = restored.get(queuedKey)!;
    restoredTracker.status = "submitted";
    expect(restored.get(queuedKey)?.status).toBe("submitted");
  });

  it("writes the same blob when identical tracker maps have different insertion order", () => {
    const keyA = cancellationKey();
    const keyB = cancellationKey("library_migration_preview", 43, "migration-a", "revision-b");
    const forward = new Map<string, PreviewCancellationTracker>([
      [keyA, tracker()],
      [keyB, tracker({ requestId: REQUEST_ID_TWO, seq: 93, status: "submitted", error: null })],
    ]);
    const reverse = new Map([...forward.entries()].reverse());
    const first = memoryStorage();
    const second = memoryStorage();

    expect(writePreviewCancellations(first.storage, forward)).toBe(true);
    expect(writePreviewCancellations(second.storage, reverse)).toBe(true);

    const firstBlob = first.values.get(PREVIEW_CANCELLATION_STORAGE_KEY);
    const secondBlob = second.values.get(PREVIEW_CANCELLATION_STORAGE_KEY);
    expect(firstBlob).toBeDefined();
    expect(firstBlob).toBe(secondBlob);
    const entries = JSON.parse(firstBlob ?? "null").entries as Array<{ key: string }>;
    expect(entries.map((entry) => entry.key)).toEqual(
      [keyA, keyB].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0)),
    );
  });

  it("normalizes legacy unknown status and lets the writer omit inactive failures", () => {
    const { storage, values } = memoryStorage();
    const unknownKey = cancellationKey();
    const failedKey = cancellationKey("library_migration_preview", 43, "migration-dismissed", "revision-b");
    values.set(PREVIEW_CANCELLATION_STORAGE_KEY, envelope([
      {
        key: unknownKey,
        tracker: { requestId: REQUEST_ID, seq: null, status: "outcome_unknown", error: "Delivery is unknown." },
      },
      {
        key: failedKey,
        tracker: { requestId: null, seq: null, status: "failed", error: "Dismissed failure." },
      },
    ]));

    const restored = readPreviewCancellations(storage);
    expect(restored.get(unknownKey)?.status).toBe("unknown");
    expect(restored.get(failedKey)).toEqual({
      requestId: null,
      seq: null,
      status: "failed",
      error: "Dismissed failure.",
    });
    expect(writePreviewCancellations(storage, restored)).toBe(true);
    const persisted = JSON.parse(values.get(PREVIEW_CANCELLATION_STORAGE_KEY) ?? "null");
    expect(persisted.entries.map((entry: { key: string }) => entry.key)).toEqual([unknownKey]);

    expect(writePreviewCancellations(storage, new Map([[failedKey, restored.get(failedKey)!]]))).toBe(true);
    expect(values.has(PREVIEW_CANCELLATION_STORAGE_KEY)).toBe(false);
  });

  it("fails closed for malformed identities, trackers, envelopes, and duplicate keys", () => {
    const { storage, values } = memoryStorage();
    const validKey = cancellationKey();
    const validTracker = tracker();
    const invalidKeys = [
      JSON.stringify(["file_confirm", 42, "preview-a", "revision-a"]),
      JSON.stringify(["file_preview", 0, "preview-a", "revision-a"]),
      JSON.stringify(["file_preview", Number.MAX_SAFE_INTEGER + 1, "preview-a", "revision-a"]),
      JSON.stringify(["file_preview", 42, "bad\u0085id", "revision-a"]),
      JSON.stringify(["file_preview", 42, "é".repeat(65), "revision-a"]),
      JSON.stringify(["file_preview", 42, "preview-a", "é".repeat(129)]),
      ` ${validKey}`,
      "x".repeat(1_025),
    ];
    for (const key of invalidKeys) {
      values.set(PREVIEW_CANCELLATION_STORAGE_KEY, envelope([{ key, tracker: validTracker }]));
      expect(readPreviewCancellations(storage).size).toBe(0);
    }

    const invalidTrackers: unknown[] = [
      { ...validTracker, requestId: "not-a-uuid" },
      { ...validTracker, requestId: null },
      { ...validTracker, seq: 0 },
      { ...validTracker, seq: Number.MAX_SAFE_INTEGER + 1 },
      { ...validTracker, seq: 1.5 },
      { ...validTracker, status: "completed" },
      { ...validTracker, error: "unsafe\u0085error" },
      { ...validTracker, error: "é".repeat(513) },
      { ...validTracker, title: "must not be persisted" },
      { requestId: null, seq: 1, status: "failed", error: "invalid dismissal identity" },
    ];
    for (const invalid of invalidTrackers) {
      values.set(PREVIEW_CANCELLATION_STORAGE_KEY, envelope([{ key: validKey, tracker: invalid }]));
      expect(readPreviewCancellations(storage).size).toBe(0);
    }

    values.set(PREVIEW_CANCELLATION_STORAGE_KEY, envelope([{ key: validKey, tracker: validTracker }], 2));
    expect(readPreviewCancellations(storage).size).toBe(0);
    const tooManyEntries = Array.from({ length: 101 }, (_, index) => ({
      key: cancellationKey("file_preview", index + 1, `preview-${index + 1}`, "revision-a"),
      tracker: validTracker,
    }));
    values.set(PREVIEW_CANCELLATION_STORAGE_KEY, envelope(tooManyEntries));
    expect(readPreviewCancellations(storage).size).toBe(0);
    values.set(PREVIEW_CANCELLATION_STORAGE_KEY, JSON.stringify({
      version: 1,
      entries: [
        { key: validKey, tracker: validTracker },
        { key: validKey, tracker: validTracker },
      ],
    }));
    expect(readPreviewCancellations(storage).size).toBe(0);
  });

  it("checks the raw byte cap before parsing and rejects an oversized entry map without replacing stored state", () => {
    const { storage, values } = memoryStorage();
    const rawLimit = 256 * 1024;
    const parse = vi.spyOn(JSON, "parse");
    values.set(PREVIEW_CANCELLATION_STORAGE_KEY, "x".repeat(rawLimit + 1));
    expect(readPreviewCancellations(storage).size).toBe(0);
    expect(parse).not.toHaveBeenCalled();
    parse.mockRestore();

    const validKey = cancellationKey();
    const validMap = new Map<string, PreviewCancellationTracker>([[validKey, tracker()]]);
    expect(writePreviewCancellations(storage, validMap)).toBe(true);
    const before = values.get(PREVIEW_CANCELLATION_STORAGE_KEY);
    const oversized = new Map<string, PreviewCancellationTracker>();
    for (let seq = 1; seq <= 101; seq += 1) {
      oversized.set(cancellationKey("file_preview", seq, `preview-${seq}`, "revision-a"), tracker());
    }
    expect(writePreviewCancellations(storage, oversized)).toBe(false);
    expect(values.get(PREVIEW_CANCELLATION_STORAGE_KEY)).toBe(before);

    const invalidIdentity = new Map([[cancellationKey("file_preview", 42, "é".repeat(65), "revision-a"), tracker()]]);
    expect(writePreviewCancellations(storage, invalidIdentity)).toBe(false);
    expect(values.get(PREVIEW_CANCELLATION_STORAGE_KEY)).toBe(before);
    const invalidTracker = new Map([[validKey, tracker({ requestId: "not-a-uuid" })]]);
    expect(writePreviewCancellations(storage, invalidTracker)).toBe(false);
    expect(values.get(PREVIEW_CANCELLATION_STORAGE_KEY)).toBe(before);
  });

  it("sanitizes and bounds only errors while preserving identities", () => {
    const { storage } = memoryStorage();
    const key = cancellationKey();
    const cancellations = new Map([[key, tracker({ error: `line one\n${"é".repeat(600)}` })]]);

    expect(writePreviewCancellations(storage, cancellations)).toBe(true);
    const restored = readPreviewCancellations(storage).get(key)!;
    expect(restored).toMatchObject({ requestId: REQUEST_ID, seq: null, status: "queued" });
    expect(new TextEncoder().encode(restored.error ?? "").byteLength).toBeLessThanOrEqual(1_024);
    expect(restored.error).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/u);
    expect(readPreviewCancellations(storage).has(key)).toBe(true);
  });

  it("returns empty maps or false for unavailable storage and removes the key for an empty map", () => {
    const { storage, values } = memoryStorage({
      [PREVIEW_CANCELLATION_STORAGE_KEY]: envelope([{ key: cancellationKey(), tracker: tracker() }]),
    });
    const unavailable = {
      getItem() { throw new Error("unavailable"); },
      setItem() { throw new Error("unavailable"); },
      removeItem() { throw new Error("unavailable"); },
    };
    expect(readPreviewCancellations(unavailable).size).toBe(0);
    expect(writePreviewCancellations(unavailable, new Map([[cancellationKey(), tracker()]]))).toBe(false);
    expect(writePreviewCancellations(unavailable, new Map())).toBe(false);

    expect(writePreviewCancellations(storage, new Map())).toBe(true);
    expect(values.has(PREVIEW_CANCELLATION_STORAGE_KEY)).toBe(false);
    const missingGetter = {} as unknown as Pick<PreviewCancellationStorage, "getItem">;
    const missingRemover = { setItem() {} } as unknown as Pick<PreviewCancellationStorage, "setItem" | "removeItem">;
    expect(readPreviewCancellations(missingGetter).size).toBe(0);
    expect(writePreviewCancellations(missingRemover, new Map())).toBe(false);
  });

  it("round-trips exact dismissal pairs in stable key and UUID order", () => {
    const firstKey = cancellationKey("file_preview", 42, "preview-a", "revision-a");
    const secondKey = cancellationKey("library_migration_preview", 43, "migration-a", "revision-b");
    const first = new Map<string, Set<string>>([
      [secondKey, new Set([REQUEST_ID_TWO, REQUEST_ID])],
      [firstKey, new Set([REQUEST_ID_TWO])],
    ]);
    const second = new Map([...first.entries()].reverse().map(([key, requestIds]) => [key, new Set([...requestIds].reverse())]));
    const firstStorage = memoryStorage();
    const secondStorage = memoryStorage();

    expect(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY).toBe("file-manager.preview-cancellation-dismissals.v1");
    expect(writePreviewCancellationDismissals(firstStorage.storage, first)).toBe(true);
    expect(writePreviewCancellationDismissals(secondStorage.storage, second)).toBe(true);
    const serialized = firstStorage.values.get(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY);
    expect(serialized).toBe(secondStorage.values.get(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY));
    expect(JSON.parse(serialized ?? "null")).toEqual({
      version: 1,
      entries: [
        { key: firstKey, request_ids: [REQUEST_ID_TWO] },
        { key: secondKey, request_ids: [REQUEST_ID, REQUEST_ID_TWO] },
      ],
    });
    expect(readPreviewCancellationDismissals(firstStorage.storage)).toEqual(new Map([
      [firstKey, new Set([REQUEST_ID_TWO])],
      [secondKey, new Set([REQUEST_ID, REQUEST_ID_TWO])],
    ]));
  });

  it("keeps dismissal scoped to the exact request UUID so a new request for the same target remains eligible", () => {
    const key = cancellationKey();
    const dismissedRequest = REQUEST_ID;
    const newRequest = "00000000-0000-4000-8000-000000000003";
    const { storage, values } = memoryStorage();
    expect(writePreviewCancellationDismissals(storage, new Map([[key, new Set([dismissedRequest])]]))).toBe(true);

    const restored = readPreviewCancellationDismissals(storage);
    expect(restored?.get(key)?.has(dismissedRequest)).toBe(true);
    expect(restored?.get(key)?.has(newRequest)).toBe(false);

    restored?.get(key)?.add(newRequest);
    expect(writePreviewCancellationDismissals(storage, restored!)).toBe(true);
    expect(JSON.parse(values.get(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY) ?? "null").entries).toEqual([
      { key, request_ids: [dismissedRequest, newRequest] },
    ]);
  });

  it("bounds exact dismissal pairs and fails closed for corrupt or unavailable marker storage", () => {
    const key = cancellationKey();
    const initial = new Map<string, Set<string>>([[key, new Set([REQUEST_ID])]]);
    const { storage, values } = memoryStorage();
    expect(writePreviewCancellationDismissals(storage, initial)).toBe(true);
    const before = values.get(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY);
    const tooMany = new Map<string, Set<string>>();
    for (let index = 0; index < 129; index += 1) {
      const requestId = `00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`;
      tooMany.set(cancellationKey("file_preview", index + 1, `preview-${index + 1}`, "revision-a"), new Set([requestId]));
    }
    expect(writePreviewCancellationDismissals(storage, tooMany)).toBe(false);
    expect(values.get(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY)).toBe(before);

    values.set(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY, JSON.stringify({
      version: 1,
      entries: [{ key, request_ids: [REQUEST_ID, REQUEST_ID] }],
    }));
    expect(readPreviewCancellationDismissals(storage)).toBeNull();
    values.set(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY, "x".repeat(256 * 1024 + 1));
    expect(readPreviewCancellationDismissals(storage)).toBeNull();

    const unavailable = {
      getItem() { throw new Error("unavailable"); },
      setItem() { throw new Error("unavailable"); },
      removeItem() { throw new Error("unavailable"); },
    };
    expect(readPreviewCancellationDismissals(unavailable)).toBeNull();
    expect(writePreviewCancellationDismissals(unavailable, initial)).toBe(false);
    expect(writePreviewCancellationDismissals(unavailable, new Map())).toBe(false);
  });

  it("keeps dismissal markers separate from legacy cancellation tracker storage", () => {
    const key = cancellationKey();
    const { storage, values } = memoryStorage({
      [PREVIEW_CANCELLATION_STORAGE_KEY]: envelope([{ key, tracker: tracker() }]),
    });

    expect(readPreviewCancellations(storage).get(key)).toEqual(tracker());
    expect(readPreviewCancellationDismissals(storage)).toEqual(new Map());
    values.set(PREVIEW_CANCELLATION_DISMISSALS_STORAGE_KEY, envelope([{ key, tracker: tracker() }]));
    expect(readPreviewCancellationDismissals(storage)).toBeNull();
    expect(readPreviewCancellations(storage).get(key)).toEqual(tracker());
  });
});
