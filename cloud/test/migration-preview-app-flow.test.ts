import { describe, expect, it } from "vitest";
import {
  ACTION_SURFACE_FILE,
  persistQueuedActionAndDrain,
  type QueuedAction,
} from "../public/file/action-queue.js";
import {
  createMigrationRefreshQueuedCallback,
  migrationPreviewDraftKey,
  migrationPreviewEntries,
  renderMigrationPreview,
} from "../public/file/migration-preview-rendering.js";

function fullPreview(id: string, revision: string, sourceLibraryId = "source") {
  const itemId = `bundle-${id}-${revision}`;
  return {
    id,
    revision,
    source_library_id: sourceLibraryId,
    target_library_id: "target",
    scope: "library",
    conflict: "skip",
    include_incomplete_tasks: false,
    set_target_default_on_success: false,
    item_count: 1,
    file_count: 1,
    conflict_count: 1,
    incomplete_task_count: 0,
    total_bytes: 100,
    required_space_bytes: 100,
    available_space_bytes: 10_000,
    items: [{
      item_id: itemId,
      conflict: true,
      source_path: `Films/${itemId}.mkv`,
      target_path: `Films/${itemId}.mkv`,
      files: [],
    }],
  };
}

function previewOperation(id: string, revision: string, sourceLibraryId = "source") {
  return {
    kind: "library_migration_preview",
    status: "awaiting_confirmation",
    preview_id: id,
    revision,
    result: fullPreview(id, revision, sourceLibraryId),
  };
}

describe("migration preview app flow", () => {
  it("retains all 100 published previews, deduplicates exact revisions, and renders their controls", () => {
    const previews = Array.from({ length: 99 }, (_, index) => ({
      ...previewOperation(`preview-${index}`, "revision-1"),
      preview_incomplete: index === 0,
      projection: "preview",
    }));
    const operations = [
      { ...previewOperation("preview-0", "revision-1"), projection: "complete-operation" },
      previewOperation("preview-1", "revision-2"),
      previewOperation("other-source", "revision-1", "archive"),
      { ...previewOperation("completed", "revision-1"), status: "completed" },
    ];

    const entries = migrationPreviewEntries({ previews, operations }, "source");

    expect(entries).toHaveLength(100);
    expect(entries[0]).toMatchObject({ id: "preview-1", revision: "revision-2" });
    expect(entries.find((entry) => entry.id === "preview-0" && entry.revision === "revision-1"))
      .toMatchObject({ projection: "complete-operation", incomplete: false });
    expect(entries.some((entry) => entry.id === "other-source" || entry.id === "completed")).toBe(false);

    for (const entry of entries) {
      const markup = renderMigrationPreview(entry, { sourceAvailable: true, targetAvailable: true });
      expect(markup).toContain(`data-migration-confirm="${String(entry.id)}"`);
      expect(markup).toContain(`data-migration-conflict-item="bundle-${String(entry.id)}-${String(entry.revision)}"`);
    }

    const first = entries[0]!;
    const result = first.result as Record<string, unknown>;
    const itemId = `bundle-${String(first.id)}-${String(first.revision)}`;
    const refreshMarkup = renderMigrationPreview(first, {
      sourceAvailable: true,
      targetAvailable: true,
      draft: { previewId: first.id, revision: first.revision, itemConflicts: { [itemId]: "keep_both" } },
    });
    expect(refreshMarkup).toContain(`data-migration-refresh-conflicts="${String(first.id)}"`);
    expect(result.items).toBeDefined();
  });

  it("persists the refresh draft in the production queue callback before immediately draining", async () => {
    const operation = previewOperation("preview-refresh", "revision-7");
    const draft = { previewId: "preview-refresh", revision: "revision-7", itemConflicts: { "bundle-a": "keep_both" }, page: 2 };
    const payload = {
      supersedes_preview_id: "preview-refresh",
      supersedes_revision: "revision-7",
      item_conflicts: { "bundle-a": "keep_both" },
    };
    const request: QueuedAction = {
      request_id: "request-refresh-7",
      kind: "library_migration_preview",
      payload,
      surface: ACTION_SURFACE_FILE,
    };
    const events: string[] = [];
    const drafts = new Map<string, Record<string, unknown>>();
    const invalidated: string[] = [];
    let queue: QueuedAction[] = [];
    const key = migrationPreviewDraftKey("preview-refresh", "revision-7")!;
    const onQueued = createMigrationRefreshQueuedCallback({
      operation,
      previewId: "preview-refresh",
      revision: "revision-7",
      draft,
      payload,
      saveDraft(previewId, revision, nextDraft) {
        const draftKey = migrationPreviewDraftKey(previewId, revision)!;
        drafts.set(draftKey, nextDraft);
        events.push("draft-saved");
        return draftKey;
      },
      markInvalidated(draftKey) {
        invalidated.push(draftKey);
        events.push("invalidated");
      },
      render() { events.push("rendered"); },
    });

    const result = await persistQueuedActionAndDrain({
      action: request,
      readQueue: () => queue,
      writeQueue(nextQueue) {
        queue = nextQueue;
        events.push("queue-persisted");
        return true;
      },
      onQueued,
      afterPersist() { events.push("post-persist"); },
      isOnline: () => true,
      onSending() { events.push("sending"); },
      async drain() {
        events.push("drain");
        expect(queue).toContainEqual(request);
        expect(drafts.get(key)).toMatchObject({
          previewId: "preview-refresh",
          revision: "revision-7",
          itemConflicts: { "bundle-a": "keep_both" },
          refreshRequestId: "request-refresh-7",
          refreshPayload: payload,
          refreshSeq: null,
          refreshError: null,
        });
      },
    });

    expect(result.status).toBe("drained");
    expect(invalidated).toEqual([key]);
    expect(events).toEqual([
      "queue-persisted",
      "draft-saved",
      "invalidated",
      "rendered",
      "post-persist",
      "sending",
      "drain",
    ]);
  });
});
