import { describe, expect, it } from "vitest";
import { markUnknownFileAction, isUnknownDeliveryAction } from "../public/file/delivery-outcome.js";
import { renderMigrationPreview } from "../public/file/migration-preview-rendering.js";

describe("preview cancellation rendering", () => {
  it("normalizes an unknown cancellation tracker while retaining the queue unknown-outcome marker", () => {
    const action = {
      request_id: "cancel-unknown-1",
      kind: "preview_cancel",
      delivery_outcome: "outcome_unknown" as const,
      delivery_error: "The computer may have received this cancellation.",
    };
    const key = JSON.stringify(["file_preview", 42, "preview-42", "revision-42"]);
    const tracker = { requestId: action.request_id, seq: 91, status: "submitted", error: "" };
    const state = {
      previewCancellations: new Map([[key, tracker]]),
      unknownActionOutcomes: new Map<string, { kind?: string; code?: string; error?: string }>(),
      quarantinedActionIds: new Set<string>(),
    };

    markUnknownFileAction(state, action);

    expect(tracker).toMatchObject({
      requestId: action.request_id,
      seq: 91,
      status: "unknown",
      error: action.delivery_error,
    });
    expect(isUnknownDeliveryAction(action)).toBe(true);
    expect(state.unknownActionOutcomes.get(action.request_id)).toMatchObject({ kind: "preview_cancel" });
    expect(state.quarantinedActionIds.has(action.request_id)).toBe(true);
  });

  it("keeps exact cancellation markup on incomplete or mismatched migration placeholders without enabling confirmation", () => {
    const cancelMarkup = '<button type="button" data-preview-cancel-kind="library_migration_preview" data-preview-cancel-seq="73" data-preview-cancel-id="preview-incomplete" data-preview-cancel-revision="revision-incomplete">Cancel preview</button>';
    const baseOperation = {
      kind: "library_migration_preview",
      status: "awaiting_confirmation",
      seq: 73,
      preview_id: "preview-incomplete",
      revision: "revision-incomplete",
      result: {
        id: "preview-incomplete",
        revision: "revision-incomplete",
        source_library_id: "offline-source",
        target_library_id: "offline-target",
      },
    };
    const cases = [
      { ...baseOperation, incomplete: true },
      baseOperation,
      { ...baseOperation, result: { ...baseOperation.result, id: "different-preview", items: [] } },
    ];

    for (const operation of cases) {
      const markup = renderMigrationPreview(operation, {
        sourceAvailable: false,
        targetAvailable: false,
        rootBindingsMatch: false,
        cancelMarkup,
      });

      expect(markup).toContain(cancelMarkup);
      expect(markup).not.toContain("data-migration-confirm=");
      expect(markup).not.toContain("data-migration-conflict-item=");
    }
  });
});
