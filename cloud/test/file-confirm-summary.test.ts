import { describe, expect, it } from "vitest";
import { normalizeFileConfirmSummary, renderFileConfirmSummary } from "../public/file/file-confirm-summary.js";

const PREVIEW_ID = "file-confirm-large";
const REVISION = "file-confirm-r1";

function compactConfirmResult() {
  return {
    seq: 12,
    kind: "file_confirm",
    status: "failed",
    details_incomplete: true,
    result_truncated: true,
    result: {
      preview_id: PREVIEW_ID,
      revision: REVISION,
      details_incomplete: true,
      result_truncated: true,
      operation: {
        file_confirm_summary: true,
        summary_version: 1,
        id: PREVIEW_ID,
        revision: REVISION,
        status: "partial",
        retryable_access_error: true,
        selected_count: 5_000,
        outcome_counts: { move: 4_998, skip: 1, no_op: 1 },
        file_count: 5_000,
        moved_file_count: 4_900,
        metadata_patched_item_count: 2_000,
        error_count: 100,
        warning_count: 3,
        sample_items: [{
          item_id: "item-1",
          outcome: "move",
          moved_files: 1,
          total_files: 1,
          metadata_patched: true,
          error: null,
        }],
        omitted_item_count: 4_999,
        sample_warnings: ["A representative warning"],
        omitted_warning_count: 2,
        summary_message: "A bounded sample is shown; the complete item results are saved locally.",
      },
    },
  };
}

describe("compact file confirmation summaries", () => {
  it("preserves full batch counts while old-client incomplete flags remain set", () => {
    const entry = compactConfirmResult();
    const summary = normalizeFileConfirmSummary(entry);
    expect(summary).toMatchObject({
      detailsAvailable: true,
      status: "partial",
      retryableAccessError: true,
      selectedCount: 5_000,
      outcomeCounts: { move: 4_998, skip: 1, noOp: 1 },
      movedFileCount: 4_900,
      fileCount: 5_000,
      omittedItemCount: 4_999,
      omittedWarningCount: 2,
    });
  });

  it.each([
    ["preview id", (entry: ReturnType<typeof compactConfirmResult>) => { entry.result.preview_id = "different"; }],
    ["revision", (entry: ReturnType<typeof compactConfirmResult>) => { entry.result.operation.revision = "different"; }],
    ["outcome totals", (entry: ReturnType<typeof compactConfirmResult>) => { entry.result.operation.outcome_counts.move += 1; }],
    ["item omissions", (entry: ReturnType<typeof compactConfirmResult>) => { entry.result.operation.omitted_item_count = 4_998; }],
    ["warning omissions", (entry: ReturnType<typeof compactConfirmResult>) => { entry.result.operation.omitted_warning_count = 1; }],
    ["sample limit", (entry: ReturnType<typeof compactConfirmResult>) => { entry.result.operation.sample_items = Array.from({ length: 5 }, () => entry.result.operation.sample_items[0]!); }],
  ])("rejects inconsistent %s", (_label, mutate) => {
    const entry = compactConfirmResult();
    mutate(entry);
    expect(normalizeFileConfirmSummary(entry)).toBeNull();
  });

  it("renders full counts and labels sample rows separately", () => {
    const summary = normalizeFileConfirmSummary(compactConfirmResult());
    if (!summary) throw new Error("Expected a valid compact confirmation summary.");
    const rendered = renderFileConfirmSummary(summary);

    expect(rendered).toContain("Partial batch result");
    expect(rendered).toContain("5,000 item results");
    expect(rendered).toContain("4,900 of 5,000 related files moved");
    expect(rendered).toContain("4,999 item results omitted");
    expect(rendered.toLowerCase()).toContain("retryable access error");
    expect(rendered).toContain("Sample result · item-1");
    expect(rendered).toContain("Sample warning: A representative warning");
  });

  it("shows a compact unavailable receipt without inventing totals", () => {
    const fallback = {
      kind: "file_confirm",
      status: "completed",
      result: {
        preview_id: PREVIEW_ID,
        revision: REVISION,
        operation: {
          file_confirm_summary: true,
          summary_version: 1,
          details_available: false,
          status: "completed",
          summary_message: "The batch operation finished, but its detailed receipt could not be retained.",
        },
      },
    };
    const summary = normalizeFileConfirmSummary(fallback);
    expect(summary).toMatchObject({ detailsAvailable: false, status: "completed" });
    const rendered = renderFileConfirmSummary(summary);
    expect(rendered).toContain("Completed batch record");
    expect(rendered).toContain("Detailed batch results are unavailable");
    expect(rendered).not.toContain("0 item results");
    expect(rendered).not.toContain("Sample result");
  });
});
