import { describe, expect, it } from "vitest";
import { hasDuplicateLegacyHintSelection } from "../public/file/legacy-mapping-state.js";

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
