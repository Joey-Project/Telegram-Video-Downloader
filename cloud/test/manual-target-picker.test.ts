import { describe, expect, it } from "vitest";
import { MAX_MANUAL_TARGET_OPTIONS, searchPresentManualTargets } from "../public/file/manual-target-picker.js";

describe("manual legacy target picker", () => {
  it("caps large searches at one hundred results and stops once more results are known", () => {
    const items = Array.from({ length: 50_000 }, (_, index) => ({
      id: `video-${index}`,
      title: `Target video ${index}`,
      relative_path: `collection/video-${index}.mp4`,
      presence: "present",
    }));

    const result = searchPresentManualTargets(items, "target");

    expect(MAX_MANUAL_TARGET_OPTIONS).toBe(100);
    expect(result.items).toHaveLength(100);
    expect(result.items[0]?.id).toBe("video-0");
    expect(result.items.at(-1)?.id).toBe("video-99");
    expect(result.hasMore).toBe(true);
    expect(result.scannedCount).toBe(101);
  });

  it("searches titles, paths, and IDs but only returns currently present items", () => {
    const items = [
      { id: "gone-match", title: "Target", relative_path: "old/target.mp4", presence: "missing" },
      { id: "id-match", title: "Other", relative_path: "video.mp4", presence: "present" },
      { id: "path-match", title: "Other", relative_path: "target/video.mp4", presence: "present" },
      { id: "title-match", title: "Target Film", relative_path: "video-3.mp4", presence: "present" },
    ];

    expect(searchPresentManualTargets(items, "target").items.map((item) => item.id)).toEqual([
      "path-match",
      "title-match",
    ]);
    expect(searchPresentManualTargets(items, "id-match").items.map((item) => item.id)).toEqual(["id-match"]);
  });

  it("keeps a selected present item pinned across refreshes and narrows the remaining choices", () => {
    const firstSnapshot = [
      { id: "selected", title: "Saved choice", relative_path: "movies/saved.mp4", presence: "present" },
      { id: "other", title: "Needle film", relative_path: "movies/needle.mp4", presence: "present" },
    ];
    const refreshedSnapshot = firstSnapshot.map((item) => ({ ...item }));

    const first = searchPresentManualTargets(firstSnapshot, "needle", "selected");
    const refreshed = searchPresentManualTargets(refreshedSnapshot, "needle", "selected");

    expect(first.items.map((item) => item.id)).toEqual(["selected", "other"]);
    expect(refreshed.items.map((item) => item.id)).toEqual(["selected", "other"]);
    expect(first.hasMore).toBe(false);
    expect(refreshed.hasMore).toBe(false);
  });

  it("does not return stale selections or fabricate results for an empty match", () => {
    const items = [
      { id: "one", title: "One", relative_path: "one.mp4", presence: "present" },
      { id: "two", title: "Two", relative_path: "two.mp4", presence: "unavailable" },
    ];

    expect(searchPresentManualTargets(items, "absent", "two").items).toEqual([]);
    expect(searchPresentManualTargets(items, "absent", "stale-id").items).toEqual([]);
  });
});
