import { describe, expect, it } from "vitest";
import { boundedFolderSuggestions, buildMigrationPreviewPayload, canOperateOnMediaLibrary, canOperateOnRegisteredMediaLibrary, defaultMediaLibraryId, filterLibraryItems, groupLibraryItems, LIBRARY_PAGE_SIZE, MAX_FOLDER_SUGGESTIONS, mediaLibraryAvailability, mediaLibraryEntries, mediaSnapshotInfo, mediaSnapshotOmissionNotice, migrationEndpointAvailability, needsOrganizing, operationLibraryId, operationMatchesLibrary, paginateLibraryItems, renderMigrationTargetOptions, resolveMediaLibrary, selectMigrationTarget } from "../public/file/library-view.js";
import { updateSelectionWithinLimit } from "../public/file/selection-limit.js";
import { renderMigrationPreview } from "../public/file/migration-preview-rendering.js";
import { escapeHtml } from "../public/file/preview-rendering.js";

describe("bounded library views", () => {
  it("merges applied registrations with per-library snapshots and preserves offline status", () => {
    const snapshot = {
      settings: {
        download_dir: "/Volumes/Main",
        applied: {
          default_library_id: "main",
          libraries: [
            { id: "main", name: "Main", root: "/Volumes/Main", default_category: "Films", is_default: true },
            { id: "archive", name: "Archive", root: "/Volumes/Archive", default_category: "", is_default: false },
          ],
        },
      },
      library: {
        default_library_id: "main",
        libraries: [
          { id: "main", availability: "available", media_count: 3, snapshot: { items: [{ id: "one" }] } },
          { id: "archive", availability: "offline", media_count: 7, snapshot: null },
        ],
      },
    };
    const entries = mediaLibraryEntries(snapshot);
    const archive = resolveMediaLibrary(snapshot, "archive");

    expect(entries.map((entry) => entry.id)).toEqual(["main", "archive"]);
    expect(defaultMediaLibraryId(snapshot)).toBe("main");
    expect(entries[1]).toMatchObject({ name: "Archive", root: "/Volumes/Archive", media_count: 7 });
    expect(mediaLibraryAvailability(entries[1])).toBe("offline");
    expect(archive.snapshot).toBeNull();
    expect(archive.id).toBe("archive");
    expect(operationMatchesLibrary({ kind: "file_confirm", result: { library_id: "archive" } }, "archive")).toBe(true);
    expect(operationMatchesLibrary({ kind: "file_confirm", result: { library_id: "main" } }, "archive")).toBe(false);
  });

  it("falls back to the legacy single-root snapshot until the registry is published", () => {
    const snapshot = {
      settings: { download_dir: "/Users/test/Videos" },
      library: { root_label: "Videos", items: [{ id: "video-a" }], categories: [], warnings: [] },
    };
    const resolved = resolveMediaLibrary(snapshot);

    expect(resolved.id).toBe("default");
    expect(resolved.descriptor).toMatchObject({ name: "Videos", root: "/Users/test/Videos", is_default: true });
    expect(resolved.snapshot?.items).toEqual([{ id: "video-a" }]);
    expect(canOperateOnRegisteredMediaLibrary(snapshot, "default")).toBe(true);
    expect(migrationEndpointAvailability(snapshot, "default", "default")).toMatchObject({
      sourceRegistered: true,
      targetRegistered: true,
      sourceAvailable: true,
      targetAvailable: true,
    });
  });

  it("uses the full safe media count and reports compacted rows separately", () => {
    const descriptor = { id: "main", media_count: 10_000 };
    const snapshot = {
      items: [{ id: "one" }, { id: "two" }, { id: "three" }],
      warnings: ["Cloud snapshot omitted 9997 library item(s) from the complete source view to fit the upload budget."],
    };

    expect(mediaSnapshotInfo(descriptor, snapshot)).toEqual({ totalCount: 10_000, retainedCount: 3, omittedCount: 9_997 });
    expect(mediaSnapshotOmissionNotice(descriptor, snapshot)).toContain("retains 3 of 10,000 media items");
    expect(mediaSnapshotOmissionNotice(descriptor, snapshot)).toContain("pagination cover only the retained rows");
    expect(paginateLibraryItems(snapshot.items, 0)).toMatchObject({ total: 3, items: snapshot.items });
  });

  it("keeps an empty retained list distinct from an empty library and falls back only for missing or invalid counts", () => {
    const emptySnapshot = { items: [], warnings: [] };
    expect(mediaSnapshotInfo({ id: "main", media_count: 12 }, emptySnapshot)).toEqual({
      totalCount: 12,
      retainedCount: 0,
      omittedCount: 12,
    });
    expect(mediaSnapshotOmissionNotice({ id: "main", media_count: 12 }, emptySnapshot)).toContain("retains 0 of 12 media items");
    expect(mediaSnapshotInfo({ id: "main", media_count: 0 }, emptySnapshot).totalCount).toBe(0);
    expect(mediaSnapshotInfo({ id: "main", media_count: null }, { items: [{ id: "one" }] }).totalCount).toBe(1);
    expect(mediaSnapshotInfo({ id: "main", media_count: "0" }, { items: [{ id: "one" }] }).totalCount).toBe(1);
    expect(mediaSnapshotInfo({ id: "main" }, { items: [{ id: "one" }] }).totalCount).toBe(1);
  });

  it("requires registered, available migration endpoints while preserving the actual legacy root fallback", () => {
    const snapshot = {
      settings: { download_dir: "/Volumes/Main" },
      library: { root_label: "Main", items: [], categories: [] },
    };
    const registry = {
      settings: { applied: { default_library_id: "main", libraries: [{ id: "main", is_default: true }] } },
      library: { libraries: [
        { id: "main", availability: "available", snapshot: { items: [] } },
        { id: "archive", availability: "offline", snapshot: null },
      ] },
    };

    expect(canOperateOnMediaLibrary(undefined)).toBe(false);
    expect(canOperateOnRegisteredMediaLibrary(registry, "removed")).toBe(false);
    expect(migrationEndpointAvailability(registry, "main", "removed")).toMatchObject({
      sourceAvailable: true,
      targetRegistered: false,
      targetAvailable: false,
    });
    expect(migrationEndpointAvailability(registry, "main", "archive")).toMatchObject({
      sourceAvailable: true,
      targetRegistered: true,
      targetAvailable: false,
    });
    expect(migrationEndpointAvailability(registry, "main", "main").sourceAvailable).toBe(true);
    expect(canOperateOnRegisteredMediaLibrary(snapshot, "default")).toBe(true);
  });

  it("preserves the selected registered migration target across option rebuilds and uses it in the preview payload", () => {
    const main = { id: "main", availability: "available" };
    const archive = { id: "archive", availability: "available" };
    const added = { id: "added", availability: "available" };
    const canOperate = (id: string) => [main, archive, added].find((entry) => entry.id === id)?.availability === "available";

    const firstRender = selectMigrationTarget([main, archive], "main", "", canOperate);
    expect(firstRender.selectedId).toBe("archive");

    const rerender = selectMigrationTarget([main, added, archive], "main", "archive", canOperate);
    expect(rerender.selectedId).toBe("archive");
    expect(renderMigrationTargetOptions(rerender, escapeHtml)).toContain('<option value="archive" selected>');
    const payload = buildMigrationPreviewPayload({
      sourceLibraryId: "main",
      targetLibraryId: rerender.selectedId,
      scope: "library",
    });
    expect(payload?.target_library_id).toBe("archive");

    const archiveOffline = { ...archive, availability: "offline" };
    const withOfflineCurrent = selectMigrationTarget([main, archiveOffline, added], "main", "archive", (id) => id !== "archive");
    expect(withOfflineCurrent.selectedId).toBe("archive");
    expect(withOfflineCurrent.availableTargets.map((entry) => entry.id)).toEqual(["added"]);
    expect(renderMigrationTargetOptions(withOfflineCurrent, escapeHtml)).toContain('<option value="archive" selected disabled>');

    const removedTarget = selectMigrationTarget([main, added], "main", "archive", canOperate);
    expect(removedTarget.selectedId).toBe("added");
    const targetBecomesSource = selectMigrationTarget([main, added], "added", "added", canOperate);
    expect(targetBecomesSource.selectedId).toBe("main");
  });

  it("disables migration Start for removed or offline descriptors and enables registered endpoints", () => {
    const operation = {
      status: "awaiting_confirmation",
      result: {
        id: "preview-a",
        revision: "revision-a",
        source_library_id: "main",
        target_library_id: "archive",
        scope: "library",
        conflict: "skip",
        include_incomplete_tasks: false,
        set_target_default_on_success: false,
        item_count: 0,
        file_count: 0,
        total_bytes: 0,
        required_space_bytes: 0,
        available_space_bytes: 0,
        conflict_count: 0,
        incomplete_task_count: 0,
        items: [],
      },
    };
    const registry = {
      settings: { applied: { default_library_id: "main", libraries: [{ id: "main", is_default: true }, { id: "archive" }] } },
      library: { libraries: [
        { id: "main", availability: "available", snapshot: { items: [] } },
        { id: "archive", availability: "offline", snapshot: null },
      ] },
    };
    const render = (targetLibraryId: string) => {
      const preview = { ...operation, result: { ...operation.result, target_library_id: targetLibraryId } };
      return renderMigrationPreview(preview, migrationEndpointAvailability(registry, "main", targetLibraryId));
    };

    expect(render("removed")).toMatch(/data-migration-confirm="preview-a" data-revision="revision-a" disabled/u);
    expect(render("archive")).toMatch(/data-migration-confirm="preview-a" data-revision="revision-a" disabled/u);
    expect(render("main")).toContain('data-migration-confirm="preview-a" data-revision="revision-a" >Start migration</button>');

    const legacy = { settings: { download_dir: "/Volumes/Main" }, library: { items: [] } };
    const legacyPreview = { ...operation, result: { ...operation.result, source_library_id: "default", target_library_id: "default" } };
    const legacyEndpoints = migrationEndpointAvailability(legacy, "default", "default");
    expect(renderMigrationPreview(legacyPreview, legacyEndpoints)).toContain('data-migration-confirm="preview-a" data-revision="revision-a" >Start migration</button>');
  });

  it("shows a whole-library migration's full item count separately from retained sample paths", () => {
    const sampleItems = Array.from({ length: 4 }, (_, index) => ({
      item_id: `bundle-${index}`,
      source_path: `source/${index}.mp4`,
      target_path: `target/${index}.mp4`,
      bytes: 100,
      conflict: false,
      status: "copying",
      outcome: "move",
    }));
    const summary = renderMigrationPreview({
      kind: "library_migration_preview",
      status: "awaiting_confirmation",
      migration_summary: true,
      preview_id: "preview-summary",
      revision: "revision-summary",
      result: {
        id: "preview-summary",
        revision: "revision-summary",
        source_library_id: "main",
        target_library_id: "archive",
        scope: "library",
        conflict: "skip",
        include_incomplete_tasks: false,
        set_target_default_on_success: false,
        item_count: 10_000,
        file_count: 10_000,
        total_bytes: 1_000_000,
        required_space_bytes: 1_000_000,
        available_space_bytes: 2_000_000,
        conflict_count: 0,
        incomplete_task_count: 0,
        staging_bytes: null,
        omitted_item_count: 9_996,
        omitted_file_count: 10_000,
        omitted_item_id_count: 0,
        omitted_conflict_choice_count: 0,
        omitted_warning_count: 0,
        sample_items: sampleItems,
      },
    }, { sourceAvailable: true, targetAvailable: true });

    expect(summary).toContain("10,000 media items");
    expect(summary).toContain("Showing 4 sample media paths; 9,996 item details omitted from this view.");
    expect(summary).not.toContain("4 media items");
  });

  it("keeps unbound legacy operations scoped by compatibility but rejects malformed explicit IDs", () => {
    const legacy = { kind: "file_preview", result: { preview: { items: [] } } };
    const valid = { kind: "file_preview", result: { library_id: "main" } };
    const malformed = { kind: "file_preview", result: { library_id: "" } };
    const oversized = { kind: "file_preview", result: { library_id: "x".repeat(129) } };

    expect(operationLibraryId(legacy)).toBe("");
    expect(operationMatchesLibrary(legacy, "main")).toBe(true);
    expect(operationLibraryId(valid)).toBe("main");
    expect(operationMatchesLibrary(valid, "main")).toBe(true);
    expect(operationMatchesLibrary(valid, "archive")).toBe(false);
    expect(operationLibraryId(malformed)).toBe("");
    expect(operationMatchesLibrary(malformed, "main")).toBe(false);
    expect(operationMatchesLibrary(oversized, "main")).toBe(false);
  });

  it("filters a large snapshot before slicing the matching page", () => {
    const items = Array.from({ length: 50_000 }, (_, index) => ({
      id: `item-${index}`,
      title: index % 200 === 0 ? `Needle ${String(index).padStart(5, "0")}` : `Video ${String(index).padStart(5, "0")}`,
      relative_path: `Folder ${index % 10}/video-${index}.mp4`,
      presence: "present",
    }));
    const filtered = filterLibraryItems(items, { query: "needle" });
    const firstPage = paginateLibraryItems(filtered, 0);
    const secondPage = paginateLibraryItems(filtered, 1);

    expect(filtered).toHaveLength(250);
    expect(firstPage.items).toHaveLength(LIBRARY_PAGE_SIZE);
    expect(secondPage.items).toHaveLength(LIBRARY_PAGE_SIZE);
    expect(paginateLibraryItems(filtered, 0, 500).items).toHaveLength(LIBRARY_PAGE_SIZE);
    expect(firstPage.items.every((item) => item.title.toLowerCase().includes("needle"))).toBe(true);
    expect(secondPage.items[0]?.id).not.toBe(firstPage.items[0]?.id);
    expect(paginateLibraryItems(filtered, 100).page).toBe(4);
  });

  it("caps media rows even when one collection spans multiple pages", () => {
    const items = Array.from({ length: 71 }, (_, index) => ({
      id: `collection-item-${index}`,
      collection: { title: "One large collection" },
      presence: "present",
    }));
    const firstPage = paginateLibraryItems(items, 0);
    const secondPage = paginateLibraryItems(items, 1);
    const needsItems = items.filter(needsOrganizing);
    const firstNeedsPage = paginateLibraryItems(needsItems, 0);
    const secondNeedsPage = paginateLibraryItems(needsItems, 1);
    const firstGroups = groupLibraryItems(firstPage.items);
    const secondGroups = groupLibraryItems(secondPage.items);

    expect(firstPage.items).toHaveLength(50);
    expect(secondPage.items).toHaveLength(21);
    expect(firstPage.items.length + secondPage.items.length).toBe(71);
    expect(firstGroups).toEqual([{ title: "One large collection", items: firstPage.items }]);
    expect(secondGroups).toEqual([{ title: "One large collection", items: secondPage.items }]);
    expect(firstGroups.flatMap((group) => group.items)).toHaveLength(50);
    expect(firstNeedsPage.items).toHaveLength(50);
    expect(secondNeedsPage.items).toHaveLength(21);
  });

  it("retains selections across pages while deselecting only the current page", () => {
    const items = Array.from({ length: 70 }, (_, index) => ({ id: `selected-${index}`, presence: "present" }));
    const firstPage = paginateLibraryItems(items, 0);
    const secondPage = paginateLibraryItems(items, 1);
    const pageOneSelection = updateSelectionWithinLimit([], firstPage.items.map((item) => item.id), true);
    const bothPages = updateSelectionWithinLimit(pageOneSelection.ids, secondPage.items.map((item) => item.id), true);
    const afterDeselectingFirstPage = updateSelectionWithinLimit(bothPages.ids, firstPage.items.map((item) => item.id), false);

    expect(pageOneSelection.ids).toHaveLength(50);
    expect(bothPages.ids).toHaveLength(70);
    expect(afterDeselectingFirstPage.ids).toEqual(secondPage.items.map((item) => item.id));
    expect(afterDeselectingFirstPage.ids).toHaveLength(20);
  });

  it("bounds each folder suggestion list while allowing remote matches and typed paths", () => {
    const folders = Array.from({ length: 50_000 }, (_, index) => `Folder ${String(index).padStart(5, "0")}`);
    const categorySuggestions = boundedFolderSuggestions(folders);
    const categoryRemoteSuggestions = boundedFolderSuggestions(folders, "Folder 49999");
    const destinationSuggestions = boundedFolderSuggestions(folders, "Folder 49");
    const typedDestination = "Folder 49999/Manually entered nested folder";
    const searchedItems = filterLibraryItems([
      { id: "remote-folder-item", title: "Remote match", relative_path: `${folders[49_999]}/video.mp4`, presence: "present" },
    ], { query: "remote match" });

    expect(categorySuggestions).toHaveLength(MAX_FOLDER_SUGGESTIONS);
    expect(categorySuggestions).not.toContain(folders[49_999]);
    expect(categoryRemoteSuggestions).toEqual([folders[49_999]]);
    expect(destinationSuggestions).toHaveLength(MAX_FOLDER_SUGGESTIONS);
    expect(destinationSuggestions[0]).toBe(folders[49_000]);
    expect(destinationSuggestions).not.toContain(folders[49_999]);
    expect(searchedItems.map((item) => item.id)).toEqual(["remote-folder-item"]);
    expect(typedDestination).toBe("Folder 49999/Manually entered nested folder");
    expect(boundedFolderSuggestions(folders, "", 0)).toEqual([]);
    expect(boundedFolderSuggestions(folders, "Folder 49999/Manually entered nested folder")).toEqual([]);
  });
});
