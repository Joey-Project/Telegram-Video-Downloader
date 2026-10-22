import { describe, expect, it } from "vitest";
import { boundedFolderSuggestions, buildMigrationPreviewPayload, buildMigrationPreviewPayloadFromRenderedOptions, canOperateOnMediaLibrary, canOperateOnRegisteredMediaLibrary, collectionIdentity, defaultMediaLibraryId, filterLibraryItems, groupLibraryItems, isValidLibraryRootBinding, LIBRARY_PAGE_SIZE, MAX_FOLDER_SUGGESTIONS, mediaLibraryAvailability, mediaLibraryEntries, mediaSnapshotInfo, mediaSnapshotOmissionNotice, migrationEndpointAvailability, migrationPreviewBindingsMatch, needsOrganizing, operationLibraryId, operationMatchesLibrary, paginateLibraryItems, renderMigrationTargetOptions, resolveMediaLibrary, selectMigrationTarget } from "../public/file/library-view.js";
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

  it("does not merge a live descriptor from a different root binding into the registered library", () => {
    const currentBinding = `v1:${"b".repeat(64)}`;
    const staleBinding = `v1:${"a".repeat(64)}`;
    const snapshot = {
      settings: {
        applied: {
          default_library_id: "archive",
          libraries: [{ id: "archive", name: "Archive", root: "/Volumes/Archive", root_binding: currentBinding }],
        },
      },
      library: {
        libraries: [{
          id: "archive", root: "/Volumes/Archive", root_binding: staleBinding, availability: "available",
          snapshot: { ready: true, items: [{ id: "old-object" }] },
        }],
      },
    };

    const [entry] = mediaLibraryEntries(snapshot);
    expect(entry).toMatchObject({
      root_binding: currentBinding,
      availability: "unavailable",
      snapshot: null,
      snapshot_stale: true,
    });
    expect(canOperateOnMediaLibrary(entry)).toBe(false);
    expect(resolveMediaLibrary(snapshot, "archive").snapshot).toBeNull();
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
    const main = { id: "main", root_binding: `v1:${"a".repeat(64)}`, availability: "available" };
    const archive = { id: "archive", root_binding: `v1:${"b".repeat(64)}`, availability: "available" };
    const added = { id: "added", availability: "available" };
    const canOperate = (id: string) => [main, archive, added].find((entry) => entry.id === id)?.availability === "available";

    const firstRender = selectMigrationTarget([main, archive], "main", "", canOperate);
    expect(firstRender.selectedId).toBe("archive");

    const rerender = selectMigrationTarget([main, added, archive], "main", "archive", canOperate);
    expect(rerender.selectedId).toBe("archive");
    expect(renderMigrationTargetOptions(rerender, escapeHtml)).toContain(`<option value="archive" data-root-binding="${archive.root_binding}" selected>`);
    const payload = buildMigrationPreviewPayloadFromRenderedOptions({
      sourceOption: { value: "main", dataset: { rootBinding: main.root_binding } },
      targetOption: { value: rerender.selectedId, dataset: { rootBinding: archive.root_binding } },
      scope: "library",
    });
    expect(payload?.target_library_id).toBe("archive");
    expect(payload?.source_root_binding).toBe(main.root_binding);
    expect(payload?.target_root_binding).toBe(archive.root_binding);

    const archiveOffline = { ...archive, availability: "offline" };
    const withOfflineCurrent = selectMigrationTarget([main, archiveOffline, added], "main", "archive", (id) => id !== "archive");
    expect(withOfflineCurrent.selectedId).toBe("archive");
    expect(withOfflineCurrent.availableTargets.map((entry) => entry.id)).toEqual(["added"]);
    expect(renderMigrationTargetOptions(withOfflineCurrent, escapeHtml)).toContain(`<option value="archive" data-root-binding="${archive.root_binding}" selected disabled>`);

    const removedTarget = selectMigrationTarget([main, added], "main", "archive", canOperate);
    expect(removedTarget.selectedId).toBe("added");
    const targetBecomesSource = selectMigrationTarget([main, added], "added", "added", canOperate);
    expect(targetBecomesSource.selectedId).toBe("main");
  });

  it("binds a preview to the rendered registration and rejects a same-ID replacement", () => {
    const renderedSourceBinding = `v1:${"a".repeat(64)}`;
    const renderedTargetBinding = `v1:${"b".repeat(64)}`;
    const replacementTargetBinding = `v1:${"c".repeat(64)}`;
    const payload = buildMigrationPreviewPayloadFromRenderedOptions({
      sourceOption: { value: "source", dataset: { rootBinding: renderedSourceBinding } },
      targetOption: { value: "target", dataset: { rootBinding: renderedTargetBinding } },
      scope: "selected",
      itemIds: ["item-1"],
    });
    const currentSnapshot = {
      settings: { applied: { libraries: [
        { id: "source", root_binding: renderedSourceBinding },
        { id: "target", root_binding: replacementTargetBinding },
      ] } },
      library: { libraries: [
        { id: "source", root_binding: renderedSourceBinding, availability: "available", snapshot: { items: [] } },
        { id: "target", root_binding: replacementTargetBinding, availability: "available", snapshot: { items: [] } },
      ] },
    };

    expect(isValidLibraryRootBinding(renderedTargetBinding)).toBe(true);
    expect(payload).toMatchObject({
      source_library_id: "source",
      target_library_id: "target",
      source_root_binding: renderedSourceBinding,
      target_root_binding: renderedTargetBinding,
      item_ids: ["item-1"],
    });
    expect(migrationPreviewBindingsMatch(currentSnapshot, "source", "target", payload?.source_root_binding, payload?.target_root_binding)).toBe(false);
  });

  it("disables migration Start for removed or offline descriptors and enables registered endpoints", () => {
    const operation = {
      status: "awaiting_confirmation",
      result: {
        id: "preview-a",
        revision: "revision-a",
        source_library_id: "main",
        target_library_id: "archive",
        source_root_binding: `v1:${"a".repeat(64)}`,
        target_root_binding: `v1:${"b".repeat(64)}`,
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
        { id: "main", root_binding: `v1:${"a".repeat(64)}`, availability: "available", snapshot: { items: [] } },
        { id: "archive", root_binding: `v1:${"b".repeat(64)}`, availability: "offline", snapshot: null },
      ] },
    };
    const render = (targetLibraryId: string) => {
      const preview = { ...operation, result: {
        ...operation.result,
        target_library_id: targetLibraryId,
        target_root_binding: targetLibraryId === "main" ? `v1:${"a".repeat(64)}` : operation.result.target_root_binding,
      } };
      return renderMigrationPreview(preview, {
        ...migrationEndpointAvailability(registry, "main", targetLibraryId),
        rootBindingsMatch: migrationPreviewBindingsMatch(registry, "main", targetLibraryId, preview.result.source_root_binding, preview.result.target_root_binding),
      });
    };

    expect(render("removed")).toMatch(/data-migration-confirm="preview-a" data-revision="revision-a" disabled/u);
    expect(render("archive")).toMatch(/data-migration-confirm="preview-a" data-revision="revision-a" disabled/u);
    expect(render("main")).toContain('data-migration-confirm="preview-a" data-revision="revision-a" >Start migration</button>');

    const legacy = {
      settings: { download_dir: "/Volumes/Main" },
      library: { root_label: "Main", items: [], categories: [], root_binding: `v1:${"a".repeat(64)}` },
    };
    const legacyPreview = { ...operation, result: {
      ...operation.result,
      source_library_id: "default",
      target_library_id: "default",
      source_root_binding: `v1:${"a".repeat(64)}`,
      target_root_binding: `v1:${"a".repeat(64)}`,
    } };
    const legacyEndpoints = migrationEndpointAvailability(legacy, "default", "default");
    expect(resolveMediaLibrary(legacy, "default").descriptor?.root_binding).toBe(legacy.library.root_binding);
    expect(renderMigrationPreview(legacyPreview, {
      ...legacyEndpoints,
      rootBindingsMatch: migrationPreviewBindingsMatch(legacy, "default", "default", legacyPreview.result.source_root_binding, legacyPreview.result.target_root_binding),
    })).toContain('data-migration-confirm="preview-a" data-revision="revision-a" >Start migration</button>');
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

  it("honors numeric episode order inside stable collection blocks", () => {
    const items = [
      { id: "episode-10", title: "Episode 1", collection: { id: "series-a", title: "Series A", kind: "series", order: 10 } },
      { id: "episode-2", title: "Episode 2", collection: { id: "series-a", title: "Series A", kind: "series", order: 2 } },
      { id: "episode-1", title: "Episode 10", collection: { id: "series-a", title: "Series A", kind: "series", order: 1 } },
      { id: "other-series-1", title: "Episode 0", collection: { id: "series-b", title: "Series B", kind: "series", order: 1 } },
    ];
    const filtered = filterLibraryItems(items);
    const groups = groupLibraryItems(filtered);

    expect(groups.map(({ title, items: entries }) => ({ title, ids: entries.map((item) => item.id) }))).toEqual([
      { title: "Series B", ids: ["other-series-1"] },
      { title: "Series A", ids: ["episode-1", "episode-2", "episode-10"] },
    ]);
  });

  it("uses filesystem collection identity for grouping and keeps same-title IDs separate", () => {
    const first = { id: "same-id-first", title: "Episode 1", collection: { id: " series-a ", title: "Series", kind: " tv " } };
    const renamed = { id: "same-id-renamed", title: "Episode 2", collection: { id: "series-a", title: "Series renamed", kind: "tv", order: 2 } };
    const sameTitleOtherId = { id: "other-series", title: "Episode 3", collection: { id: "series-b", title: "Series", kind: "tv" } };
    const groups = groupLibraryItems([first, renamed, sameTitleOtherId]);

    expect(collectionIdentity(first.collection)).toBe(collectionIdentity(renamed.collection));
    expect(collectionIdentity(first.collection)).not.toBe(collectionIdentity(sameTitleOtherId.collection));
    expect(groups).toEqual([
      { title: "Series", items: [first, renamed] },
      { title: "Series", items: [sameTitleOtherId] },
    ]);
  });

  it("sorts known episode orders before missing orders with title fallback", () => {
    const items = [
      { id: "missing-2", title: "Episode 2", collection: { id: "series", title: "Series", kind: "series", order: null } },
      { id: "order-10", title: "Episode 1", collection: { id: "series", title: "Series", kind: "series", order: 10 } },
      { id: "invalid-order", title: "Episode 11", collection: { id: "series", title: "Series", kind: "series", order: -1 } },
      { id: "order-1", title: "Episode 10", collection: { id: "series", title: "Series", kind: "series", order: 1 } },
      { id: "missing-1", title: "Episode 3", collection: { id: "series", title: "Series", kind: "series" } },
    ];

    expect(filterLibraryItems(items).map((item) => item.id)).toEqual([
      "order-1", "order-10", "invalid-order", "missing-2", "missing-1",
    ]);
  });

  it("paginates an ordered collection only after its episodes are sorted numerically", () => {
    const items = Array.from({ length: 51 }, (_, index) => {
      const order = index + 1;
      return {
        id: `episode-${order}`,
        title: `Episode ${String(52 - order).padStart(2, "0")}`,
        collection: { id: "series", title: "Series", kind: "series", order },
      };
    });
    const filtered = filterLibraryItems(items);
    const firstPage = paginateLibraryItems(filtered, 0);
    const secondPage = paginateLibraryItems(filtered, 1);

    expect(firstPage.items.map((item) => item.id)).toEqual(Array.from({ length: 50 }, (_, index) => `episode-${index + 1}`));
    expect(secondPage.items.map((item) => item.id)).toEqual(["episode-51"]);
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
