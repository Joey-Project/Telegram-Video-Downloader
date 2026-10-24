import { describe, expect, it } from "vitest";
import {
  buildWaitingEntries,
  isWaitingEntryPending,
  mergeRequestEntries,
  operationHistoryEntries,
} from "../public/file/state-view.js";

const terminalStatuses = [
  "completed",
  "failed",
  "cancelled",
  "confirmed",
  "expired",
  "stale",
  "rejected",
  "uncertain",
  "partial_failure",
  "partial",
  "stopped",
  "superseded",
];

describe("request lifecycle projections", () => {
  it.each(["file_preview", "library_migration_preview"] as const)(
    "recognizes terminal and active states for %s entries",
    (kind) => {
      for (const status of terminalStatuses) {
        expect(isWaitingEntryPending({ kind, status }), `${kind} ${status}`).toBe(false);
      }
      for (const status of ["pending", "waiting_for_local", "processing", "retryable", "awaiting_confirmation"]) {
        expect(isWaitingEntryPending({ kind, status }), `${kind} ${status}`).toBe(true);
      }
    },
  );

  it("treats terminal preview-cancel outcomes as finished and Pending as active", () => {
    for (const status of terminalStatuses) {
      expect(isWaitingEntryPending({ kind: "preview_cancel", status }), status).toBe(false);
    }
    expect(isWaitingEntryPending({ kind: "preview_cancel", status: "Pending" })).toBe(true);
  });

  it("keeps a cancelled file preview with its saved result and hides the awaiting preview", () => {
    const awaiting = {
      seq: 31,
      kind: "file_preview",
      status: "awaiting_confirmation",
      result: { preview: { revision: "revision-awaiting", items: [] } },
    };
    const cancelled = {
      seq: 32,
      kind: "file_preview",
      status: "cancelled",
      result: { preview: { revision: "revision-cancelled", items: [] } },
    };

    expect(operationHistoryEntries([awaiting, cancelled])).toEqual([cancelled]);
  });

  it("lets a local terminal receipt replace cloud-pending state in waiting projections", () => {
    const cloudPending = {
      seq: 44,
      kind: "file_preview",
      status: "awaiting_confirmation",
    };
    const localCancelled = {
      seq: 44,
      kind: "file_preview",
      status: "cancelled",
      result: { preview: { revision: "revision-cancelled", items: [] } },
    };

    expect(mergeRequestEntries([cloudPending], [], [localCancelled])).toEqual([localCancelled]);
    expect(buildWaitingEntries([cloudPending], [], [localCancelled])).toEqual({
      cloud: [],
      local: [localCancelled],
    });
  });
});
