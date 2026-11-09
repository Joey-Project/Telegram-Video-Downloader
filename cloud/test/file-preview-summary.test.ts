import { describe, expect, it } from "vitest";
import {
  filePreviewSummaryConfirmLabel,
  normalizeFilePreviewSummary,
  renderFilePreviewSummary,
} from "../public/file/file-preview-summary.js";
import { isIncompletePreview, isPreviewDetailsAvailable } from "../public/file/state-view.js";

const PREVIEW_ID = "file-preview-large";
const REVISION = "file-preview-r1";
const LIBRARY_ID = "main-library";
const ROOT_BINDING = `v1:${"a".repeat(64)}`;

function compactPreview(): Record<string, any> {
  return {
    file_preview_summary: true,
    summary_version: 1,
    id: PREVIEW_ID,
    revision: REVISION,
    library_id: LIBRARY_ID,
    root_binding: ROOT_BINDING,
    selected_count: 5_000,
    metadata_only: false,
    target_relative_dir: "Organized",
    rename: true,
    conflict: "keep_both",
    move_item_count: 5_000,
    file_count: 20,
    total_bytes: "1234567890123",
    outcome_counts: { move: 4_998, skip: 1, no_op: 1 },
    conflict_count: 1,
    metadata_patch_count: 3,
    changed_metadata_patch_count: 2,
    sample_items: [{
      item_id: "item-1",
      source_path: "Films/One.mkv",
      target_path: "Organized/One.mkv",
      outcome: "move",
      file_count: 10,
      sample_files: [
        { source_path: "Films/One.mkv", target_path: "Organized/One.mkv", bytes: 1_000 },
        { source_path: "Films/One.en.srt", target_path: "Organized/One.en.srt", bytes: "1200" },
      ],
      omitted_file_count: 8,
    }],
    sample_metadata_patches: [{ item_id: "item-1", nfo_path: "Films/One.nfo", changed: true }],
    omitted_item_count: 4_999,
    omitted_file_count: 18,
    omitted_metadata_patch_count: 2,
    summary_message: "The sample rows are partial. Confirm applies the entire saved batch.",
  };
}

function operation(summary = compactPreview()) {
  return {
    seq: 9,
    kind: "file_preview",
    status: "awaiting_confirmation",
    preview_id: PREVIEW_ID,
    revision: REVISION,
    library_id: LIBRARY_ID,
    root_binding: ROOT_BINDING,
    details_incomplete: true,
    result_truncated: true,
    result: {
      preview_id: PREVIEW_ID,
      revision: REVISION,
      library_id: LIBRARY_ID,
      root_binding: ROOT_BINDING,
      details_incomplete: true,
      result_truncated: true,
      preview: summary,
    },
  };
}

describe("compact file preview summaries", () => {
  it("accepts a consistent marked summary while legacy incomplete flags stay fail closed", () => {
    const entry = operation();
    const summary = normalizeFilePreviewSummary(entry);

    expect(summary).toMatchObject({
      id: PREVIEW_ID,
      revision: REVISION,
      libraryId: LIBRARY_ID,
      rootBinding: ROOT_BINDING,
      selectedCount: 5_000,
      moveItemCount: 5_000,
      fileCount: 20,
      totalBytes: "1234567890123",
      omittedItemCount: 4_999,
    });
    expect(isIncompletePreview({
      incomplete: true,
      preview_incomplete: true,
      reload_required: true,
      preview: entry.result.preview,
      status: "awaiting_confirmation",
    })).toBe(true);
    expect(isPreviewDetailsAvailable({
      incomplete: true,
      preview_incomplete: true,
      reload_required: true,
      preview: entry.result.preview,
      status: "awaiting_confirmation",
    }, { compactSummaryValidated: summary !== null })).toBe(true);
    expect(isPreviewDetailsAvailable({
      incomplete: true,
      preview: entry.result.preview,
      status: "awaiting_confirmation",
    })).toBe(false);
  });

  it.each([
    ["preview id", { preview_id: "other-preview" }],
    ["revision", { revision: "other-revision" }],
    ["library", { library_id: "other-library" }],
    ["root binding", { root_binding: `v1:${"b".repeat(64)}` }],
  ])("rejects mismatched outer %s identity", (_label, change) => {
    expect(normalizeFilePreviewSummary({ ...operation(), ...change })).toBeNull();
  });

  it.each([
    ["id", { id: "other-preview" }],
    ["revision", { revision: "other-revision" }],
    ["library", { library_id: "other-library" }],
    ["root binding", { root_binding: `v1:${"b".repeat(64)}` }],
  ])("rejects mismatched nested %s identity", (_label, change) => {
    expect(normalizeFilePreviewSummary(operation({ ...compactPreview(), ...change }))).toBeNull();
  });

  it.each([
    ["outcome total", (preview: ReturnType<typeof compactPreview>) => { preview.outcome_counts.move += 1; }],
    ["conflict total", (preview: ReturnType<typeof compactPreview>) => { preview.conflict_count = 2; }],
    ["omitted item total", (preview: ReturnType<typeof compactPreview>) => { preview.omitted_item_count = 4_998; }],
    ["omitted file total", (preview: ReturnType<typeof compactPreview>) => { preview.omitted_file_count = 17; }],
    ["metadata changed total", (preview: ReturnType<typeof compactPreview>) => { preview.changed_metadata_patch_count = 4; }],
    ["total bytes format", (preview: ReturnType<typeof compactPreview>) => { preview.total_bytes = "1e20"; }],
    ["sample limit", (preview: ReturnType<typeof compactPreview>) => { preview.sample_items = Array.from({ length: 5 }, () => preview.sample_items[0]!); }],
  ])("rejects inconsistent %s", (_label, mutate) => {
    const preview = compactPreview();
    mutate(preview);
    expect(normalizeFilePreviewSummary(operation(preview))).toBeNull();
  });

  it("keeps samples visibly partial and confirms the full selected count", () => {
    const summary = normalizeFilePreviewSummary(operation());
    if (!summary) throw new Error("Expected a valid file preview summary.");
    const rendered = renderFilePreviewSummary(summary);

    expect(rendered).toContain("5,000 selected items");
    expect(rendered).toContain("4,998 move · 1 skip · 1 no change");
    expect(rendered).toContain("1,234,567,890,123 bytes");
    expect(rendered).toContain("Sample item · item-1");
    expect(rendered).toContain("4,999 item details omitted");
    expect(rendered).toContain("18 attached-file paths omitted");
    expect(rendered).toContain("These rows are examples only; they are not the full plan.");
    expect(rendered).not.toContain("data-source-path=");
    expect(rendered).not.toContain("data-target-path=");
    expect(renderFilePreviewSummary({ ...summary, sampleItems: [] })).toContain("No sample media items are available.");
    expect(filePreviewSummaryConfirmLabel(summary)).toBe("Confirm changes for all 5,000 selected items");
  });

  it("labels metadata-only previews without suggesting file movement", () => {
    const preview = compactPreview();
    preview.metadata_only = true;
    preview.target_relative_dir = null;
    preview.move_item_count = 0;
    preview.file_count = 0;
    preview.total_bytes = "0";
    preview.outcome_counts = { move: 0, skip: 0, no_op: 0 };
    preview.conflict_count = 0;
    preview.sample_items = [];
    preview.omitted_item_count = 0;
    preview.omitted_file_count = 0;
    preview.metadata_patch_count = 5_000;
    preview.changed_metadata_patch_count = 4_999;
    preview.sample_metadata_patches = [{ item_id: "item-1", nfo_path: "Films/One.nfo", changed: true }];
    preview.omitted_metadata_patch_count = 4_999;
    const summary = normalizeFilePreviewSummary(operation(preview));
    if (!summary) throw new Error("Expected a valid metadata-only preview summary.");

    expect(renderFilePreviewSummary(summary)).toContain("Metadata only · file locations stay the same");
    expect(renderFilePreviewSummary(summary)).toContain("Sample metadata · item-1");
    expect(filePreviewSummaryConfirmLabel(summary)).toBe("Confirm metadata update for all 5,000 selected items");
  });

  it("requires the active confirmation state and explicit compact marker", () => {
    expect(normalizeFilePreviewSummary({ ...operation(), status: "completed" })).toBeNull();
    const legacySample = operation();
    legacySample.result.preview.file_preview_summary = false;
    expect(normalizeFilePreviewSummary(legacySample)).toBeNull();
    const missingLegacyFailClosedFlags = { ...operation(), details_incomplete: false };
    expect(normalizeFilePreviewSummary(missingLegacyFailClosedFlags)).toBeNull();
  });
});
