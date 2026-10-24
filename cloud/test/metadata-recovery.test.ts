import { describe, expect, it } from "vitest";
import {
  buildMetadataRecoveryPreviewPayload,
  buildOrganizationStatusPatch,
  groupMetadataRecoveryItems,
  parseMetadataRecoveryReport,
} from "../public/file/metadata-recovery.js";

const collection = { id: "series-1", title: "Series", kind: "series", order: 1, part_id: null };
const reportFixture = {
  revision: "library-r17",
  total: 5,
  remaining: 2,
  next_cursor: "item-unavailable",
  items: [
    { item_id: "item-ready", title: "Ready", relative_path: "Ready.mkv", status: "ready", patch: {
      // Rust serde keeps optional fields as null and an empty additive source list as [].
      item_id: "item-ready", hint_index: null, title: "Recovered title", source_ids: [], collection: null,
      organization_status: null, fill_missing_only: true,
    } },
    { item_id: "item-choice", title: "Choice", relative_path: "Choice.mkv", status: "needs_confirmation", candidates: [
      { label: "Series · episode 1", patch: { item_id: "item-choice", collection } },
      { label: "Series · episode 2", patch: { item_id: "item-choice", collection: { ...collection, order: 2 } } },
    ] },
    { item_id: "item-unchanged", title: "Unchanged", relative_path: "Unchanged.mkv", status: "unchanged", reason: "Metadata is already complete." },
    { item_id: "item-unavailable", title: "Unavailable", relative_path: "Unavailable.mkv", status: "unavailable", reason: "The source could not be checked." },
    { item_id: "item-unsupported", title: "Unsupported", relative_path: "Unsupported.mkv", status: "unsupported", reason: "No supported source ID was found." },
  ],
};

describe("metadata recovery reports", () => {
  it("strictly validates a bounded report and groups every status without losing reasons", () => {
    const report = parseMetadataRecoveryReport(reportFixture);
    expect(report).not.toBeNull();
    expect(Object.isFrozen(report)).toBe(true);
    expect(Object.isFrozen(report?.items)).toBe(true);
    expect(Reflect.set(report!, "revision", "changed")).toBe(false);
    expect(report?.revision).toBe("library-r17");
    expect(groupMetadataRecoveryItems(report!.items)).toMatchObject({
      ready: [{ item_id: "item-ready" }],
      needs_confirmation: [{ item_id: "item-choice" }],
      unchanged: [{ item_id: "item-unchanged", reason: "Metadata is already complete." }],
      unavailable: [{ item_id: "item-unavailable", reason: "The source could not be checked." }],
      unsupported: [{ item_id: "item-unsupported", reason: "No supported source ID was found." }],
    });
    expect(parseMetadataRecoveryReport({ ...reportFixture, remaining: 2, next_cursor: null })).toBeNull();
    expect(parseMetadataRecoveryReport({ ...reportFixture, items: [...reportFixture.items, reportFixture.items[0]] })).toBeNull();
    expect(parseMetadataRecoveryReport({ ...reportFixture, items: [{ ...reportFixture.items[0], status: "guessed" }] })).toBeNull();
  });

  it("builds fill-missing-only patches bound to the analyzed revision and an explicit candidate", () => {
    const report = parseMetadataRecoveryReport(reportFixture);
    expect(report).not.toBeNull();
    expect(buildMetadataRecoveryPreviewPayload({
      report: report!,
      libraryId: "main",
      rootBinding: `v1:${"a".repeat(64)}`,
      itemIds: ["item-ready", "item-choice"],
      candidateChoices: new Map([["item-choice", 1]]),
    })).toEqual({
      library_id: "main",
      root_binding: `v1:${"a".repeat(64)}`,
      item_ids: ["item-ready", "item-choice"],
      metadata_patches: [
        { item_id: "item-ready", title: "Recovered title", fill_missing_only: true },
        { item_id: "item-choice", collection: { ...collection, order: 2 }, fill_missing_only: true },
      ],
      expected_revision: "library-r17",
    });
    expect(buildMetadataRecoveryPreviewPayload({
      report: report!, libraryId: "main", rootBinding: `v1:${"a".repeat(64)}`,
      itemIds: ["item-choice"], candidateChoices: new Map(),
    })).toBeNull();
    expect(buildMetadataRecoveryPreviewPayload({
      report: report!, libraryId: "main", rootBinding: `v1:${"a".repeat(64)}`,
      itemIds: ["item-unavailable"], candidateChoices: new Map(),
    })).toBeNull();
  });

  it("refuses standalone declarations over an existing collection and supports undo", () => {
    expect(buildOrganizationStatusPatch({ id: "series-item", collection }, "standalone")).toBeNull();
    expect(buildOrganizationStatusPatch({ id: "loose-item" }, "standalone")).toEqual({
      item_id: "loose-item",
      organization_status: "standalone",
    });
    expect(buildOrganizationStatusPatch({ id: "loose-item", organization_status: "standalone" }, "unreviewed")).toEqual({
      item_id: "loose-item",
      organization_status: "unreviewed",
    });
  });
});
