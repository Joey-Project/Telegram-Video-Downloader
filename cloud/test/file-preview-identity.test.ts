import { describe, expect, it } from "vitest";
import { createFileQueuedAction, fileActionRequestBody } from "../public/file/file-action-dispatch.js";
import { isIncompletePreview } from "../public/file/state-view.js";
import {
  buildFilePreviewPayloadFromRenderedOption,
  buildLegacyMetadataPreviewPayloadFromRenderedOption,
  buildSelectedFilePreviewPayloadFromRenderedOption,
  clearFileIdentityScopedStateOnSnapshot,
  fileOperationIdentityMatches,
  fileOperationLibraryIdentity,
} from "../public/file/file-preview-identity.js";

const LIBRARY_ID = "library-shared-path-id";
const ROOT_A = `v1:${"a".repeat(64)}`;
const ROOT_B = `v1:${"b".repeat(64)}`;

function snapshot(rootBinding: string) {
  const descriptor = {
    id: LIBRARY_ID,
    name: "Archive",
    root: "/Volumes/Archive",
    root_binding: rootBinding,
    availability: "available",
    snapshot: { ready: true, items: [] },
  };
  return {
    settings: { applied: { libraries: [descriptor], default_library_id: LIBRARY_ID } },
    library: { libraries: [descriptor] },
  };
}

function option(rootBinding: string) {
  return { value: LIBRARY_ID, dataset: { rootBinding } };
}

describe("file preview root identity", () => {
  it("captures the displayed identity for move and metadata preview payloads", () => {
    const fields = { item_ids: ["item-a"], target_relative_dir: "Films", rename: false, conflict: "skip" };
    const move = buildFilePreviewPayloadFromRenderedOption({
      ownerSnapshot: snapshot(ROOT_A),
      libraryOption: option(ROOT_A),
      fields,
    });
    const metadata = buildFilePreviewPayloadFromRenderedOption({
      ownerSnapshot: snapshot(ROOT_A),
      libraryOption: option(ROOT_A),
      expectedIdentity: { library_id: LIBRARY_ID, root_binding: ROOT_A },
      fields: { metadata_patches: [{ item_id: "item-a", title: "Updated" }], rename: false, conflict: "skip" },
    });

    expect(move).toEqual({ ...fields, library_id: LIBRARY_ID, root_binding: ROOT_A });
    expect(metadata).toEqual({
      metadata_patches: [{ item_id: "item-a", title: "Updated" }],
      rename: false,
      conflict: "skip",
      library_id: LIBRARY_ID,
      root_binding: ROOT_A,
    });
    if (!move || !metadata) throw new Error("Expected bound preview payloads");
    const queuedMove = createFileQueuedAction("file_preview", move, "move-request", 1_800_000_000_001);
    const queuedMetadata = createFileQueuedAction("file_preview", metadata, "metadata-request", 1_800_000_000_002);
    expect(fileActionRequestBody(queuedMove).payload).toMatchObject({ library_id: LIBRARY_ID, root_binding: ROOT_A });
    expect(fileActionRequestBody(queuedMetadata).payload).toMatchObject({ library_id: LIBRARY_ID, root_binding: ROOT_A });
  });

  it("refuses to upgrade a displayed A identity or metadata draft to same-ID replacement B", () => {
    const replacement = snapshot(ROOT_B);
    expect(buildFilePreviewPayloadFromRenderedOption({
      ownerSnapshot: replacement,
      libraryOption: option(ROOT_A),
      fields: { item_ids: ["item-a"], target_relative_dir: "Films" },
    })).toBeNull();
    expect(buildFilePreviewPayloadFromRenderedOption({
      ownerSnapshot: replacement,
      libraryOption: option(ROOT_B),
      expectedIdentity: { library_id: LIBRARY_ID, root_binding: ROOT_A },
      fields: { metadata_patches: [{ item_id: "item-a", title: "Updated" }] },
    })).toBeNull();
    expect(fileOperationIdentityMatches(replacement, {
      kind: "legacy_import",
      result: { library_id: LIBRARY_ID, root_binding: ROOT_A, hints: [], candidates: [] },
    }, LIBRARY_ID)).toBe(false);
    expect(fileOperationIdentityMatches(snapshot(ROOT_A), {
      kind: "legacy_import",
      result: { library_id: LIBRARY_ID, hints: [], candidates: [] },
    }, LIBRARY_ID)).toBe(false);
  });

  it("clears A-scoped media and metadata choices at the production snapshot-accept seam", () => {
    const uiState = {
      libraryId: LIBRARY_ID,
      selected: new Set(["item-a"]),
      detailId: "item-a",
      legacyOperation: { kind: "legacy_import", result: { library_id: LIBRARY_ID, root_binding: ROOT_A } },
      legacyOperationKey: "import-a",
      legacySelection: new Map([["hint-a", { item_id: "item-a", selected: true }]]),
      legacyPage: 3,
      manualPickerKey: "hint-a",
      manualPickerQuery: "old query",
      libraryPage: 2,
      needsPage: 1,
    };

    expect(clearFileIdentityScopedStateOnSnapshot(uiState, snapshot(ROOT_A), snapshot(ROOT_B))).toBe(true);
    expect(uiState.selected.size).toBe(0);
    expect(uiState.detailId).toBeNull();
    expect(uiState.legacyOperation).toBeNull();
    expect(uiState.legacyOperationKey).toBeNull();
    expect(uiState.legacySelection.size).toBe(0);
    expect(uiState.legacyPage).toBe(0);
    expect(uiState.manualPickerKey).toBeNull();
    expect(uiState.manualPickerQuery).toBe("");
    expect(uiState.libraryPage).toBe(0);
    expect(uiState.needsPage).toBe(0);

    // The move callback checks this selection before building a preview payload;
    // metadata preview also stays bound to the original import operation.
    const movePayload = buildSelectedFilePreviewPayloadFromRenderedOption({
      state: uiState,
      ownerSnapshot: snapshot(ROOT_B),
      libraryOption: option(ROOT_B),
      fields: { target_relative_dir: "Films" },
    });
    expect(movePayload).toBeNull();
    expect(buildLegacyMetadataPreviewPayloadFromRenderedOption({
      ownerSnapshot: snapshot(ROOT_B),
      libraryOption: option(ROOT_B),
      importOperation: { kind: "legacy_import", result: { library_id: LIBRARY_ID, root_binding: ROOT_A, hints: [], candidates: [] } },
      fields: { metadata_patches: [{ item_id: "item-a", title: "Old draft" }] },
    })).toBeNull();
  });

  it("preserves selections and metadata choices when the registered root binding is unchanged", () => {
    const uiState = {
      libraryId: LIBRARY_ID,
      selected: new Set(["item-a"]),
      detailId: "item-a",
      legacySelection: new Map([["hint-a", { item_id: "item-a", selected: true }]]),
    };

    expect(clearFileIdentityScopedStateOnSnapshot(uiState, snapshot(ROOT_A), snapshot(ROOT_A))).toBe(false);
    expect([...uiState.selected]).toEqual(["item-a"]);
    expect(uiState.detailId).toBe("item-a");
    expect(uiState.legacySelection.size).toBe(1);
  });

  it("builds a metadata patch only from a bound import matching the displayed registration", () => {
    const importOperation = {
      kind: "legacy_import",
      result: { library_id: LIBRARY_ID, root_binding: ROOT_A, hints: [], candidates: [] },
    };
    const fields = { metadata_patches: [{ item_id: "item-a", title: "From import A" }] };
    expect(buildLegacyMetadataPreviewPayloadFromRenderedOption({
      ownerSnapshot: snapshot(ROOT_A),
      libraryOption: option(ROOT_A),
      importOperation,
      fields,
    })).toEqual({ ...fields, library_id: LIBRARY_ID, root_binding: ROOT_A });
    expect(buildLegacyMetadataPreviewPayloadFromRenderedOption({
      ownerSnapshot: snapshot(ROOT_B),
      libraryOption: option(ROOT_B),
      importOperation,
      fields,
    })).toBeNull();
    expect(buildLegacyMetadataPreviewPayloadFromRenderedOption({
      ownerSnapshot: snapshot(ROOT_A),
      libraryOption: option(ROOT_A),
      importOperation: { kind: "legacy_import", result: { library_id: LIBRARY_ID, hints: [], candidates: [] } },
      fields,
    })).toBeNull();
  });

  it("only treats complete, consistent saved preview identities as confirmable", () => {
    const current = snapshot(ROOT_A);
    const valid = { kind: "file_preview", result: { library_id: LIBRARY_ID, root_binding: ROOT_A, preview: { items: [] } } };
    expect(fileOperationLibraryIdentity(valid)).toEqual({ library_id: LIBRARY_ID, root_binding: ROOT_A });
    expect(fileOperationIdentityMatches(current, valid, LIBRARY_ID)).toBe(true);
    expect(fileOperationIdentityMatches(snapshot(ROOT_B), valid, LIBRARY_ID)).toBe(false);
    expect(fileOperationIdentityMatches(current, { kind: "file_preview", result: { library_id: LIBRARY_ID, preview: { items: [] } } }, LIBRARY_ID)).toBe(false);
    expect(fileOperationIdentityMatches(current, { kind: "file_preview", result: { library_id: LIBRARY_ID, root_binding: "bad" } }, LIBRARY_ID)).toBe(false);
    expect(fileOperationIdentityMatches(current, {
      kind: "file_preview",
      library_id: LIBRARY_ID,
      root_binding: ROOT_A,
      result: { library_id: LIBRARY_ID, root_binding: ROOT_B },
    }, LIBRARY_ID)).toBe(false);
    const compact = {
      kind: "file_preview",
      preview_id: "preview-a",
      revision: "revision-a",
      status: "awaiting_confirmation",
      result: { library_id: LIBRARY_ID, root_binding: ROOT_A, reload_required: true },
      reload_required: true,
    };
    expect(fileOperationIdentityMatches(current, compact, LIBRARY_ID)).toBe(true);
    expect(isIncompletePreview(compact)).toBe(true);
    expect(fileOperationIdentityMatches(current, {
      ...compact,
      result: { library_id: LIBRARY_ID, reload_required: true },
    }, LIBRARY_ID)).toBe(false);
  });

  it("keeps the original root binding in the durable browser retry body", () => {
    const action = createFileQueuedAction("file_preview", {
      library_id: LIBRARY_ID,
      root_binding: ROOT_A,
      item_ids: ["item-a"],
      target_relative_dir: "Films",
    }, "request-a", 1_800_000_000_000);

    expect(fileActionRequestBody(action)).toMatchObject({
      kind: "file_preview",
      payload: { library_id: LIBRARY_ID, root_binding: ROOT_A },
      queued_at_ms: 1_800_000_000_000,
      replay_expires_at_ms: expect.any(Number),
    });
    expect(fileOperationIdentityMatches(snapshot(ROOT_B), {
      result: action.payload,
    }, LIBRARY_ID)).toBe(false);
  });
});
