import { describe, expect, it, vi } from "vitest";
import {
  RECOVERY_PREVIEW_STORAGE_KEY,
  readRecoveryPreviewRequest,
  writeRecoveryPreviewRequest,
  type RecoveryPreviewRequest,
  type RecoveryPreviewStorage,
} from "../public/file/metadata-recovery-preview-state.js";

const tracker: RecoveryPreviewRequest = {
  requestId: "00000000-0000-4000-8000-000000000001",
  seq: 42,
  libraryId: "main-library",
  rootBinding: `v1:${"a".repeat(64)}`,
  status: "submitted",
  error: "Preview request accepted.",
};

function memoryStorage(initial: Record<string, string> = {}) {
  const values = new Map(Object.entries(initial));
  const storage: RecoveryPreviewStorage = {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => { values.set(key, value); },
    removeItem: (key) => { values.delete(key); },
  };
  return { storage, values };
}

function serialized(value: unknown): string {
  return JSON.stringify(value);
}

describe("metadata recovery preview state", () => {
  it("round-trips only the minimal tracker fields and supports a null sequence", () => {
    const { storage, values } = memoryStorage();
    expect(RECOVERY_PREVIEW_STORAGE_KEY).toBe("file-manager.metadata-recovery-preview.v1");
    expect(writeRecoveryPreviewRequest(storage, { ...tracker, seq: null })).toBe(true);
    expect(values.size).toBe(1);
    expect(readRecoveryPreviewRequest(storage)).toEqual({ ...tracker, seq: null });
    expect(Object.keys(readRecoveryPreviewRequest(storage)!)).toEqual([
      "requestId", "seq", "libraryId", "rootBinding", "status", "error",
    ]);
    expect(Object.isFrozen(readRecoveryPreviewRequest(storage))).toBe(true);
  });

  it("rejects missing, malformed, oversized, or extra persisted state without parsing oversized bytes", () => {
    const { storage, values } = memoryStorage();
    expect(readRecoveryPreviewRequest(storage)).toBeNull();
    values.set(RECOVERY_PREVIEW_STORAGE_KEY, "{");
    expect(readRecoveryPreviewRequest(storage)).toBeNull();
    values.set(RECOVERY_PREVIEW_STORAGE_KEY, serialized({ ...tracker, title: "must not persist" }));
    expect(readRecoveryPreviewRequest(storage)).toBeNull();
    values.set(RECOVERY_PREVIEW_STORAGE_KEY, serialized({ ...tracker, error: "é".repeat(513) }));
    expect(readRecoveryPreviewRequest(storage)).toBeNull();
    values.set(RECOVERY_PREVIEW_STORAGE_KEY, serialized({ ...tracker, error: "unsafe\nerror" }));
    expect(readRecoveryPreviewRequest(storage)).toBeNull();

    const parse = vi.spyOn(JSON, "parse");
    values.set(RECOVERY_PREVIEW_STORAGE_KEY, "é".repeat(4_097));
    expect(readRecoveryPreviewRequest(storage)).toBeNull();
    expect(parse).not.toHaveBeenCalled();
    parse.mockRestore();
  });

  it("fails closed for unsafe request, sequence, library, root, and status fields", () => {
    const { storage, values } = memoryStorage();
    const invalidTrackers = [
      { ...tracker, requestId: "not-a-uuid" },
      { ...tracker, requestId: "00000000-0000-4000-7000-000000000001" },
      { ...tracker, seq: 0 },
      { ...tracker, seq: -1 },
      { ...tracker, seq: Number.MAX_SAFE_INTEGER + 1 },
      { ...tracker, seq: 1.5 },
      { ...tracker, libraryId: "bad\u0000id" },
      { ...tracker, libraryId: "é".repeat(65) },
      { ...tracker, rootBinding: `v1:${"A".repeat(64)}` },
      { ...tracker, status: "completed" },
    ];
    for (const invalid of invalidTrackers) {
      values.set(RECOVERY_PREVIEW_STORAGE_KEY, serialized(invalid));
      expect(readRecoveryPreviewRequest(storage)).toBeNull();
    }
  });

  it("bounds a trusted oversized error on write without truncating identity", () => {
    const { storage } = memoryStorage();
    expect(writeRecoveryPreviewRequest(storage, { ...tracker, error: "é".repeat(600) })).toBe(true);
    expect(readRecoveryPreviewRequest(storage)?.error).toBe("é".repeat(512));
    expect(writeRecoveryPreviewRequest(storage, { ...tracker, error: "line one\nline two" })).toBe(true);
    expect(readRecoveryPreviewRequest(storage)?.error).toBe("line one line two");

    const invalidIdentity = { ...tracker, libraryId: "x".repeat(129) };
    expect(writeRecoveryPreviewRequest(storage, invalidIdentity)).toBe(false);
    expect(readRecoveryPreviewRequest(storage)?.libraryId).toBe(tracker.libraryId);
  });

  it("returns false or null when storage operations throw", () => {
    const unavailable = {
      getItem() { throw new Error("unavailable"); },
      setItem() { throw new Error("unavailable"); },
      removeItem() { throw new Error("unavailable"); },
    };
    expect(readRecoveryPreviewRequest(unavailable)).toBeNull();
    expect(writeRecoveryPreviewRequest(unavailable, tracker)).toBe(false);
    expect(writeRecoveryPreviewRequest(unavailable, null)).toBe(false);
  });

  it("removes the tracker when passed null and handles corrupt or incomplete storage safely", () => {
    const { storage, values } = memoryStorage({ [RECOVERY_PREVIEW_STORAGE_KEY]: serialized(tracker) });
    expect(writeRecoveryPreviewRequest(storage, null)).toBe(true);
    expect(values.has(RECOVERY_PREVIEW_STORAGE_KEY)).toBe(false);
    expect(readRecoveryPreviewRequest(storage)).toBeNull();

    expect(writeRecoveryPreviewRequest({ getItem: storage.getItem, setItem: storage.setItem } as RecoveryPreviewStorage, null)).toBe(false);
    expect(readRecoveryPreviewRequest({ getItem: () => { throw new Error("corrupt"); } })).toBeNull();
  });
});
