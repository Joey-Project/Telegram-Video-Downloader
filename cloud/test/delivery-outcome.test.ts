import { describe, expect, it } from "vitest";
import {
  dismissUnknownFileAction,
  isUnknownDeliveryAction,
  markUnknownFileAction,
  unknownFileActionForKind,
} from "../public/file/delivery-outcome.js";
import { restorePendingActions, type DownloadSubmissionReceiptTracker, type MigrationConflictReceiptTracker, type QueuedAction } from "../public/file/action-queue.js";

type RememberedOutcome = { kind?: string; code?: string; error?: string };

describe("File unknown delivery outcome UI state", () => {
  it("keeps a persisted unknown download visible after reload and disables a duplicate submission", () => {
    const action = {
      request_id: "download-unknown",
      kind: "download_submit",
      payload: { url: "https://example.test/video", library_id: "main", category: "" },
      delivery_outcome: "outcome_unknown" as const,
      delivery_error_code: "action_replay_window_expired" as const,
      delivery_error: "The cloud no longer has a retained receipt.",
    };
    const queue = [action];
    const freshPageState = { unknownActionOutcomes: new Map<string, RememberedOutcome>() };

    expect(unknownFileActionForKind(queue, "download_submit", freshPageState.unknownActionOutcomes)).toBe(action);
    expect(isUnknownDeliveryAction(action)).toBe(true);
    const updates = markUnknownFileAction(freshPageState, action);
    expect(updates.downloadSubmission).toBe(false);
    expect(unknownFileActionForKind(queue, "download_submit", freshPageState.unknownActionOutcomes)).toBe(action);
    expect(freshPageState.unknownActionOutcomes.has(action.request_id)).toBe(true);
  });

  it("does not dismiss or clear trackers when queue storage fails", () => {
    const action = {
      request_id: "download-unknown",
      kind: "download_submit",
      payload: { url: "https://example.test/video" },
      delivery_outcome: "outcome_unknown" as const,
      delivery_error_code: "action_replay_window_unverifiable" as const,
      delivery_error: "Check the desktop before dismissing.",
    };
    const state = {
      downloadSubmission: {
        request_id: action.request_id,
        seq: null as number | null,
        payload: action.payload,
        outcomeUnknown: false,
        outcomeError: "",
      },
      downloadMessage: null as string | null,
      unknownActionOutcomes: new Map<string, RememberedOutcome>(),
      quarantinedActionIds: new Set<string>(),
    };
    markUnknownFileAction(state, action);
    const attempts: unknown[][] = [];
    const result = dismissUnknownFileAction(state, action.request_id, [action], (remaining) => {
      attempts.push(remaining);
      return false;
    });

    expect(result.status).toBe("storage_failed");
    expect(attempts).toEqual([[]]);
    expect(state.downloadSubmission?.request_id).toBe(action.request_id);
    expect(state.downloadSubmission?.outcomeUnknown).toBe(true);
    expect(state.quarantinedActionIds.has(action.request_id)).toBe(true);
    expect(state.unknownActionOutcomes.has(action.request_id)).toBe(true);
  });

  it("requires explicit dismissal, removes only that row, and preserves conflict choices", () => {
    const download = {
      request_id: "download-unknown",
      kind: "download_submit",
      payload: { url: "https://example.test/video" },
      delivery_outcome: "outcome_unknown" as const,
      delivery_error_code: "action_replay_window_expired" as const,
      delivery_error: "Download outcome unknown.",
    };
    const refresh = {
      request_id: "preview-unknown",
      kind: "library_migration_preview",
      payload: { supersedes_preview_id: "preview-a" },
      delivery_outcome: "outcome_unknown" as const,
      delivery_error_code: "action_replay_window_unverifiable" as const,
      delivery_error: "Preview refresh outcome unknown.",
    };
    const downloadSubmission: DownloadSubmissionReceiptTracker = {
      request_id: download.request_id,
      seq: null,
      payload: download.payload,
      queued_at_ms: 10_000,
      replay_expires_at_ms: 10_000 + 6 * 24 * 60 * 60 * 1000,
    };
    const migrationDraft: MigrationConflictReceiptTracker & Record<string, unknown> = {
      previewId: "preview-a",
      revision: "revision-2",
      itemConflicts: { bundle: "keep_both" },
      refreshRequestId: refresh.request_id,
      refreshSeq: null,
      refreshPayload: refresh.payload,
      refreshQueuedAtMs: 20_000,
      refreshExpiresAtMs: 20_000 + 6 * 24 * 60 * 60 * 1000,
    };
    const migrationConflictDrafts = new Map<string, MigrationConflictReceiptTracker & Record<string, unknown>>([
      ["[\"preview-a\",\"revision-2\"]", migrationDraft],
    ]);
    const state = {
      downloadSubmission,
      downloadMessage: null as string | null,
      migrationConflictDrafts,
      migrationConflictInvalidated: new Set<string>(),
      unknownActionOutcomes: new Map<string, RememberedOutcome>(),
      quarantinedActionIds: new Set<string>(),
    };
    markUnknownFileAction(state, download);
    markUnknownFileAction(state, refresh);
    const restored: QueuedAction[][] = [];
    const restore = restorePendingActions(state, {
      surface: "file",
      readQueue: () => [],
      writeQueue: (next) => { restored.push(next); return true; },
    });
    expect(restore.requeued).toBe(0);
    expect(restored).toEqual([]);
    const queue: QueuedAction[] = [download, refresh, {
      request_id: "unrelated",
      kind: "library_scan",
      payload: { library_id: "main" },
    }];
    let remaining: unknown[] = queue;
    const result = dismissUnknownFileAction(state, refresh.request_id, queue, (next) => {
      remaining = next;
      return true;
    });

    expect(result).toMatchObject({ status: "dismissed", action: refresh, downloadSubmission: false, migrationConflictDrafts: 1 });
    const remainingIds = remaining.flatMap((action) => typeof action === "object" && action !== null
      && "request_id" in action && typeof action.request_id === "string" ? [action.request_id] : []);
    expect(remainingIds).toEqual([download.request_id, "unrelated"]);
    const draft = state.migrationConflictDrafts.get("[\"preview-a\",\"revision-2\"]") as (MigrationConflictReceiptTracker & Record<string, unknown>) | undefined;
    expect(draft).toMatchObject({
      itemConflicts: { bundle: "keep_both" },
      refreshRequestId: null,
      refreshSeq: null,
      refreshPayload: null,
      refreshOutcomeUnknown: false,
      refreshDeliveryOutcome: null,
      refreshDeliveryErrorCode: null,
    });
    expect(draft?.refreshError).toContain("Preview refresh outcome unknown.");
    expect(state.quarantinedActionIds.has(refresh.request_id)).toBe(false);
    expect(state.unknownActionOutcomes.has(refresh.request_id)).toBe(false);
    expect(state.unknownActionOutcomes.has(download.request_id)).toBe(true);
  });
});
