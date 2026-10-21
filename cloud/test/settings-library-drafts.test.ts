import { describe, expect, it } from "vitest";
import { captureLibraryEditorFocus, libraryDraftFieldValue, reconcileLibraryDrafts, restoreLibraryEditorFocus, setLibraryDraftField } from "../public/settings/library-drafts.js";

const library = (overrides: Record<string, unknown> = {}) => ({
  id: "archive",
  root: "/Volumes/Archive",
  name: "Archive",
  default_category: "Films",
  ...overrides,
});

describe("settings library field drafts", () => {
  it("lets clean fields follow snapshots while preserving only dirty fields", () => {
    const drafts = new Map();
    const initial = library();
    setLibraryDraftField(drafts, initial, "name", "Archive draft");

    const refreshed = library({ name: "Renamed remotely", default_category: "Films/2026" });
    reconcileLibraryDrafts(drafts, [refreshed]);

    expect(libraryDraftFieldValue(drafts, refreshed, "name")).toBe("Archive draft");
    expect(libraryDraftFieldValue(drafts, refreshed, "default_category")).toBe("Films/2026");
  });

  it("drops drafts when a library is removed or its root identity changes", () => {
    const drafts = new Map();
    const original = library();
    setLibraryDraftField(drafts, original, "name", "Old-root draft");

    reconcileLibraryDrafts(drafts, []);
    expect(drafts.size).toBe(0);

    setLibraryDraftField(drafts, original, "default_category", "New folder");
    const replacement = library({ root: "/Volumes/Replacement" });
    reconcileLibraryDrafts(drafts, [replacement]);

    expect(drafts.size).toBe(0);
    expect(libraryDraftFieldValue(drafts, replacement, "default_category")).toBe("Films");
  });

  it("reconciles a normalized submitted value only after it appears in the applied snapshot", () => {
    const drafts = new Map();
    const original = library();
    setLibraryDraftField(drafts, original, "default_category", "Films / 2026");

    const normalizedSubmission = "Films/2026";
    setLibraryDraftField(drafts, original, "default_category", normalizedSubmission);
    reconcileLibraryDrafts(drafts, [library({ default_category: normalizedSubmission })]);

    expect(drafts.size).toBe(0);
  });

  it("keeps pending and failed values, and preserves a newer edit after an older submission succeeds", () => {
    const drafts = new Map();
    const original = library();
    const submitted = "Films/2026";
    setLibraryDraftField(drafts, original, "default_category", submitted);

    // Pending or failed operations do not change the applied snapshot.
    reconcileLibraryDrafts(drafts, [original]);
    expect(libraryDraftFieldValue(drafts, original, "default_category")).toBe(submitted);

    // The user edits again after submitting; completion of the older value must not clear it.
    const newer = "Films/2027";
    setLibraryDraftField(drafts, original, "default_category", newer);
    reconcileLibraryDrafts(drafts, [library({ default_category: submitted })]);
    expect(libraryDraftFieldValue(drafts, library({ default_category: submitted }), "default_category")).toBe(newer);

    // A later successful application of the newer value reconciles the draft.
    reconcileLibraryDrafts(drafts, [library({ default_category: newer })]);
    expect(drafts.size).toBe(0);
  });

  it("restores editor focus and selection only for the same registered root and field", () => {
    const entry = library();
    const form: { dataset: Record<string, string>; elements?: { namedItem: (field: string) => unknown } } = {
      dataset: { categoryForm: entry.id, libraryRoot: entry.root },
    };
    let focused = false;
    let selection = null;
    const input = {
      disabled: false,
      readOnly: false,
      selectionStart: 4,
      selectionEnd: 7,
      selectionDirection: "forward",
      closest: () => form,
      focus: () => { focused = true; },
      setSelectionRange: (...args: unknown[]) => { selection = args; },
      value: "Films/2026",
    };
    form.elements = { namedItem: (field) => field === "default_category" ? input : null };
    const list = {
      contains: (candidate: unknown) => candidate === input,
      querySelectorAll: () => [form],
    };

    const focus = captureLibraryEditorFocus(list, input, [entry]);
    expect(restoreLibraryEditorFocus(list, focus, [entry])).toBe(true);
    expect(focused).toBe(true);
    expect(selection).toEqual([4, 7, "forward"]);

    focused = false;
    const replacement = library({ root: "/Volumes/Replacement" });
    expect(restoreLibraryEditorFocus(list, focus, [replacement])).toBe(false);
    expect(focused).toBe(false);

    expect(captureLibraryEditorFocus(list, input, [replacement])).toBe(null);
  });
});
