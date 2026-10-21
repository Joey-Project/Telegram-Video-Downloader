import { describe, expect, it } from "vitest";
import { buildLibraryMutationPayload, isValidLibraryRootBinding } from "../public/settings/library-actions.js";

const bindingA = `v1:${"a".repeat(64)}`;
const bindingB = `v1:${"b".repeat(64)}`;

describe("Settings library mutation payloads", () => {
  it("binds all four mutation payloads to the displayed registration identity", () => {
    const displayed = { id: "archive", root_binding: bindingA };

    expect(buildLibraryMutationPayload("library_set_default", displayed)).toEqual({
      library_id: "archive",
      root_binding: bindingA,
    });
    expect(buildLibraryMutationPayload("library_remove", displayed)).toEqual({
      library_id: "archive",
      root_binding: bindingA,
    });
    expect(buildLibraryMutationPayload("library_rename", displayed, { name: "Archive 2026" })).toEqual({
      library_id: "archive",
      root_binding: bindingA,
      name: "Archive 2026",
    });
    expect(buildLibraryMutationPayload("library_set_category", displayed, { default_category: "Films/2026" })).toEqual({
      library_id: "archive",
      root_binding: bindingA,
      default_category: "Films/2026",
    });
  });

  it("keeps the displayed token when the same ID now names a replacement registration", () => {
    const displayedBeforeConfirmation = { id: "archive", root_binding: bindingA };
    const currentSnapshotAfterReplacement = { id: "archive", root_binding: bindingB };
    const payload = buildLibraryMutationPayload("library_remove", displayedBeforeConfirmation);

    expect(currentSnapshotAfterReplacement.root_binding).not.toBe(displayedBeforeConfirmation.root_binding);
    expect(payload).toEqual({ library_id: "archive", root_binding: bindingA });
    expect(payload?.root_binding).not.toBe(currentSnapshotAfterReplacement.root_binding);
  });

  it.each([
    ["missing", undefined],
    ["null", null],
    ["wrong version", `v2:${"a".repeat(64)}`],
    ["uppercase", `v1:${"A".repeat(64)}`],
    ["short", `v1:${"a".repeat(63)}`],
    ["non-hex", `v1:${"a".repeat(63)}g`],
  ])("fails closed for a %s displayed binding", (_label, binding) => {
    expect(isValidLibraryRootBinding(binding)).toBe(false);
    expect(buildLibraryMutationPayload("library_remove", { id: "archive", root_binding: binding })).toBeNull();
  });

  it("fails closed when a displayed registration identity or required change is missing", () => {
    expect(buildLibraryMutationPayload("library_remove", { root_binding: bindingA })).toBeNull();
    expect(buildLibraryMutationPayload("library_rename", { id: "archive", root_binding: bindingA })).toBeNull();
    expect(buildLibraryMutationPayload("library_set_category", { id: "archive", root_binding: bindingA })).toBeNull();
    expect(buildLibraryMutationPayload("library_scan", { id: "archive", root_binding: bindingA })).toBeNull();
  });
});
