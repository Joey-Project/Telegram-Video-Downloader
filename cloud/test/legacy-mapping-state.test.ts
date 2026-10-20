import { describe, expect, it } from "vitest";
import {
  buildLegacyMappingChoices,
  buildLegacyMetadataPatches,
  hasDuplicateLegacyHintSelection,
  LEGACY_MAPPING_PAGE_SIZE,
  paginateLegacyMappingChoices,
  selectLegacyImportForMapping,
  selectLegacyMappingChoice,
} from "../public/file/legacy-mapping-state.js";

describe("legacy hint mapping selection", () => {
  it("rejects assigning one hint to multiple candidate or manual rows", () => {
    expect(hasDuplicateLegacyHintSelection([
      { hintIndex: "4" },
      { dataset: { hintIndex: 4 } },
    ])).toBe(true);
  });

  it("allows distinct hint indexes and an empty selection", () => {
    expect(hasDuplicateLegacyHintSelection([
      { hintIndex: "2" },
      { hintIndex: 3 },
    ])).toBe(false);
    expect(hasDuplicateLegacyHintSelection([])).toBe(false);
  });

  it("fails closed when a selected row has no valid hint index", () => {
    expect(hasDuplicateLegacyHintSelection([{ hintIndex: undefined }])).toBe(true);
    expect(hasDuplicateLegacyHintSelection([{ hintIndex: -1 }])).toBe(true);
  });
});

describe("legacy import mapping pages", () => {
  it("keeps the latest completed full report when a later import failed", () => {
    const completed = {
      seq: 10,
      kind: "legacy_import",
      status: "completed",
      result: { hints: [{ title: "saved title" }], candidates: [] },
    };
    const failed = {
      seq: 11,
      kind: "legacy_import",
      status: "failed",
      error: "parse failed",
      result: {},
    };
    const truncated = {
      seq: 12,
      kind: "legacy_import",
      status: "completed",
      result: { hints: [], candidates: [], result_truncated: true },
    };

    expect(selectLegacyImportForMapping([completed, failed])).toEqual({
      operation: completed,
      usable: true,
      latest: failed,
    });
    expect(selectLegacyImportForMapping([completed, truncated])).toEqual({
      operation: completed,
      usable: true,
      latest: truncated,
    });
    expect(selectLegacyImportForMapping([failed])).toEqual({
      operation: failed,
      usable: false,
      latest: failed,
    });
  });

  it("caps rendered mapping rows while preserving original hint indexes", () => {
    const hints = Array.from({ length: 10_000 }, (_, index) => ({ title: `Hint ${index}` }));
    const candidates = Array.from({ length: 120 }, (_, index) => ({ hint_index: 0, item_id: `candidate-${index}` }));
    const items = candidates.map((candidate) => ({ id: candidate.item_id, presence: "present" }));
    const choices = buildLegacyMappingChoices("operation-1", hints, candidates, items);

    expect(choices).toHaveLength(10_120);
    expect(LEGACY_MAPPING_PAGE_SIZE).toBe(50);
    for (let pageNumber = 0; pageNumber < Math.ceil(choices.length / LEGACY_MAPPING_PAGE_SIZE); pageNumber += 1) {
      const page = paginateLegacyMappingChoices(choices, pageNumber);
      expect(page.choices.length).toBeLessThanOrEqual(50);
      expect(page.page).toBe(pageNumber);
      expect(page.total).toBe(10_120);
    }

    const lastPage = paginateLegacyMappingChoices(choices, 202);
    expect(lastPage.choices.map((choice) => choice.hintIndex)).toEqual(
      Array.from({ length: 20 }, (_, index) => 9_980 + index),
    );
    expect(lastPage.choices.every((choice) => choice.kind === "manual")).toBe(true);
  });

  it("submits selected mappings from multiple pages, retaining candidate identity", () => {
    const hints = Array.from({ length: 51 }, (_, index) => ({
      title: `Hint ${index}`,
      source_ids: [`source-${index}`],
    }));
    const candidate = { hint_index: 0, item_id: "present-candidate", title: "Candidate" };
    const choices = buildLegacyMappingChoices("operation-2", hints, [candidate], [
      { id: "present-candidate", presence: "present" },
      { id: "present-manual", presence: "present" },
    ]);
    const candidateChoice = paginateLegacyMappingChoices(choices, 0).choices.find((choice) => choice.kind === "candidate");
    const manualChoice = paginateLegacyMappingChoices(choices, 1).choices.find((choice) => choice.kind === "manual" && choice.hintIndex === 50);
    expect(candidateChoice).toBeDefined();
    expect(manualChoice).toBeDefined();
    if (!candidateChoice || !manualChoice) throw new Error("Expected candidate and manual choices on their respective pages.");

    const selections = new Map();
    selections.set(candidateChoice.key, { selected: false, hintIndex: 0, title: true, source_ids: false, collection: false });
    selections.set(manualChoice.key, { selected: false, hintIndex: 50, item_id: "present-manual", title: true, source_ids: false, collection: false });
    expect(selectLegacyMappingChoice(selections, candidateChoice.key, 0, true).ok).toBe(true);
    expect(selectLegacyMappingChoice(selections, manualChoice.key, 50, true).ok).toBe(true);

    const result = buildLegacyMetadataPatches({
      hints,
      choices,
      selections,
      items: [
        { id: "present-candidate", presence: "present" },
        { id: "present-manual", presence: "present" },
      ],
    });
    expect(result.error).toBeNull();
    expect(result.patches).toEqual([
      { item_id: "present-candidate", hint_index: 0, title: "Hint 0" },
      { item_id: "present-manual", title: "Hint 50" },
    ]);
  });

  it("lets a manual choice replace an incorrect automatic candidate", () => {
    const hints = [{ title: "Legacy title", source_ids: ["legacy-source"] }];
    const choices = buildLegacyMappingChoices("operation-3", hints, [
      { hint_index: 0, item_id: "wrong-candidate", title: "Wrong" },
    ], [
      { id: "wrong-candidate", presence: "present" },
      { id: "manual-replacement", presence: "present" },
    ]);
    const automatic = choices.find((choice) => choice.kind === "candidate");
    const manual = choices.find((choice) => choice.kind === "manual");
    expect(automatic).toBeDefined();
    expect(manual).toBeDefined();
    if (!automatic || !manual) throw new Error("Expected automatic and manual choices for the hint.");
    const selections = new Map([
      [automatic.key, { selected: false, hintIndex: 0, title: true, source_ids: false, collection: false }],
      [manual.key, { selected: false, hintIndex: 0, item_id: "manual-replacement", title: true, source_ids: true, collection: false }],
    ]);

    expect(selectLegacyMappingChoice(selections, automatic.key, 0, true).ok).toBe(true);
    expect(selectLegacyMappingChoice(selections, manual.key, 0, true)).toEqual({ ok: true, replaced: true });
    expect(selections.get(automatic.key)?.selected).toBe(false);
    expect(selections.get(manual.key)?.selected).toBe(true);
    expect(buildLegacyMetadataPatches({ hints, choices, selections, items: [
      { id: "wrong-candidate", presence: "present" },
      { id: "manual-replacement", presence: "present" },
    ] })).toMatchObject({
      error: null,
      patches: [{ item_id: "manual-replacement", title: "Legacy title", source_ids: ["legacy-source"] }],
    });
  });
});
