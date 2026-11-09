import { describe, expect, it } from "vitest";
import {
  MAX_METADATA_RECOVERY_BATCH_ITEMS,
  advanceRecoverySelectionQueue,
  createRecoverySelectionQueue,
  getCurrentRecoverySelectionBatch,
  getRecoverySelectionQueueStatus,
  matchesRecoverySelectionQueue,
} from "../public/file/recovery-selection-batches.js";

const context = {
  libraryId: "main-library",
  rootBinding: `v1:${"a".repeat(64)}`,
};

function ids(count: number) {
  return Array.from({ length: count }, (_, index) => `item-${index}`);
}

describe("recovery selection batches", () => {
  it("slices a 5,000-item selection into exact ordered batches of 100", () => {
    const selectedIds = ids(5_000);
    let queue = createRecoverySelectionQueue({ ...context, itemIds: selectedIds });

    expect(MAX_METADATA_RECOVERY_BATCH_ITEMS).toBe(100);
    expect(queue).not.toBeNull();
    expect(Object.isFrozen(queue)).toBe(true);
    expect(Object.isFrozen(queue?.itemIds)).toBe(true);
    expect(getRecoverySelectionQueueStatus(queue)).toEqual({
      totalCount: 5_000,
      submittedCount: 0,
      remainingCount: 5_000,
      currentCount: 100,
      hasCurrentBatch: true,
    });

    const submitted: string[] = [];
    while (getRecoverySelectionQueueStatus(queue)?.hasCurrentBatch) {
      const batch = getCurrentRecoverySelectionBatch(queue);
      expect(batch).toHaveLength(100);
      if (!batch) throw new Error("a pending batch should be available");
      submitted.push(...batch);
      queue = advanceRecoverySelectionQueue(queue, {
        ...context,
        selectedIds,
        outcome: "queued",
      });
    }

    expect(submitted).toEqual(selectedIds);
    expect(getCurrentRecoverySelectionBatch(queue)).toBeNull();
    expect(getRecoverySelectionQueueStatus(queue)).toEqual({
      totalCount: 5_000,
      submittedCount: 5_000,
      remainingCount: 0,
      currentCount: 0,
      hasCurrentBatch: false,
    });
  });

  it("rejects oversized, empty, malformed, or duplicate selections instead of dropping IDs", () => {
    expect(createRecoverySelectionQueue({ ...context, itemIds: ids(5_001) })).toBeNull();
    expect(createRecoverySelectionQueue({ ...context, itemIds: [] })).toBeNull();
    expect(createRecoverySelectionQueue({ ...context, itemIds: ["first", "first"] })).toBeNull();
    expect(createRecoverySelectionQueue({ ...context, itemIds: ["valid", ""] })).toBeNull();
  });

  it("matches the same selection regardless of order and rejects changed context or membership", () => {
    const selectedIds = ids(150);
    const queue = createRecoverySelectionQueue({ ...context, itemIds: selectedIds });

    expect(matchesRecoverySelectionQueue(queue, {
      ...context,
      selectedIds: [...selectedIds].reverse(),
    })).toBe(true);
    expect(matchesRecoverySelectionQueue(queue, {
      ...context,
      selectedIds: selectedIds.slice(1),
    })).toBe(false);
    expect(matchesRecoverySelectionQueue(queue, {
      ...context,
      selectedIds: [...selectedIds.slice(1), "different-item"],
    })).toBe(false);
    expect(matchesRecoverySelectionQueue(queue, {
      ...context,
      libraryId: "another-library",
      selectedIds,
    })).toBe(false);
    expect(matchesRecoverySelectionQueue(queue, {
      ...context,
      rootBinding: `v1:${"b".repeat(64)}`,
      selectedIds,
    })).toBe(false);
  });

  it("advances only after definite queue acceptance and discards a stale context", () => {
    const selectedIds = ids(150);
    const queue = createRecoverySelectionQueue({ ...context, itemIds: selectedIds });
    const unchanged = (outcome: string) => advanceRecoverySelectionQueue(queue, {
      ...context,
      selectedIds,
      outcome,
    });

    expect(unchanged("outcome_unknown")).toBe(queue);
    expect(unchanged("failed")).toBe(queue);
    expect(getRecoverySelectionQueueStatus(unchanged("outcome_unknown"))?.remainingCount).toBe(150);

    const advanced = advanceRecoverySelectionQueue(queue, {
      ...context,
      selectedIds,
      outcome: "queued",
    });
    expect(getCurrentRecoverySelectionBatch(advanced)).toEqual(selectedIds.slice(100));
    expect(getRecoverySelectionQueueStatus(advanced)).toMatchObject({
      submittedCount: 100,
      remainingCount: 50,
      currentCount: 50,
    });

    expect(advanceRecoverySelectionQueue(queue, {
      ...context,
      libraryId: "another-library",
      selectedIds,
      outcome: "queued",
    })).toBeNull();
    expect(advanceRecoverySelectionQueue(queue, {
      ...context,
      selectedIds: [...selectedIds.slice(1), "different-item"],
      outcome: "queued",
    })).toBeNull();
  });
});
