import { describe, expect, it } from "vitest";
import { boundedFolderSuggestions, defaultMediaLibraryId, filterLibraryItems, groupLibraryItems, LIBRARY_PAGE_SIZE, MAX_FOLDER_SUGGESTIONS, mediaLibraryAvailability, mediaLibraryEntries, needsOrganizing, operationMatchesLibrary, paginateLibraryItems, resolveMediaLibrary } from "../public/file/library-view.js";
import { updateSelectionWithinLimit } from "../public/file/selection-limit.js";

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
