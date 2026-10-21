import { describe, expect, it } from "vitest";
import {
  ACTION_SURFACE_FILE,
  createQueuedAction,
  persistQueuedActionAndDrain,
  type QueuedAction,
} from "../public/file/action-queue.js";
import {
  createMigrationRefreshQueuedCallback,
  inspectMigrationRefreshHistory,
  migrationPreviewDraftKey,
  migrationPreviewEntries,
  reconcileMigrationRefreshDrafts,
  releaseMigrationRefreshTracking,
  renderMigrationPreview,
} from "../public/file/migration-preview-rendering.js";
import { buildMigrationPreviewRefreshPayload } from "../public/file/migration-conflicts.js";
import { migrationPreviewBindingsMatch } from "../public/file/library-view.js";
import { RECENT_OPERATION_SNAPSHOT_LIMIT, RECENT_SNAPSHOT_MAX_AGE_MS } from "../public/file/download-submission-history.js";
import { dismissUnknownFileAction, markUnknownFileAction } from "../public/file/delivery-outcome.js";

type RememberedOutcome = { kind?: string; code?: string; error?: string };

const SOURCE_BINDING = `v1:${"a".repeat(64)}`;
const TARGET_BINDING = `v1:${"b".repeat(64)}`;

function fullPreview(id: string, revision: string, sourceLibraryId = "source") {
  const itemId = `bundle-${id}-${revision}`;
  return {
    id,
    revision,
    source_library_id: sourceLibraryId,
    target_library_id: "target",
    source_root_binding: SOURCE_BINDING,
    target_root_binding: TARGET_BINDING,
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
      const markup = renderMigrationPreview(entry, { sourceAvailable: true, targetAvailable: true, rootBindingsMatch: true });
      expect(markup).toContain(`data-migration-confirm="${String(entry.id)}"`);
      expect(markup).toContain(`data-migration-conflict-item="bundle-${String(entry.id)}-${String(entry.revision)}"`);
      expect(markup).not.toMatch(new RegExp(`data-migration-confirm="${String(entry.id)}" data-revision="${String(entry.revision)}" disabled`, "u"));
    }

    const first = entries[0]!;
    const result = first.result as Record<string, unknown>;
    const itemId = `bundle-${String(first.id)}-${String(first.revision)}`;
    const refreshMarkup = renderMigrationPreview(first, {
      sourceAvailable: true,
      targetAvailable: true,
      rootBindingsMatch: true,
      draft: { previewId: first.id, revision: first.revision, itemConflicts: { [itemId]: "keep_both" } },
    });
    expect(refreshMarkup).toContain(`data-migration-refresh-conflicts="${String(first.id)}"`);
    expect(result.items).toBeDefined();
  });

  it("persists the refresh draft in the production queue callback before immediately draining", async () => {
    const operation = previewOperation("preview-refresh", "revision-7");
    const draft = { previewId: "preview-refresh", revision: "revision-7", itemConflicts: { "bundle-a": "keep_both" }, page: 2 };
    const payload = buildMigrationPreviewRefreshPayload(operation.result, draft.itemConflicts, {
      seq: 7,
      id: "preview-refresh",
      revision: "revision-7",
    })!;
    const request: QueuedAction = createQueuedAction({
      request_id: "request-refresh-7",
      kind: "library_migration_preview",
      payload,
      surface: ACTION_SURFACE_FILE,
    }, 10_000);
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
          refreshQueuedAtMs: 10_000,
          refreshExpiresAtMs: 10_000 + 6 * 24 * 60 * 60 * 1000,
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

  it("clears a dismissed refresh unknown marker when the user starts a new refresh", () => {
    const operation = previewOperation("preview-retry", "revision-retry");
    const draftKey = migrationPreviewDraftKey(operation.preview_id, operation.revision)!;
    const draft = {
      previewId: operation.preview_id,
      revision: operation.revision,
      itemConflicts: { "bundle-retry": "keep_both" },
      refreshRequestId: "request-old-refresh",
      refreshSeq: null,
      refreshPayload: { source_library_id: "source" },
      refreshQueuedAtMs: 10_000,
      refreshExpiresAtMs: 10_000 + 6 * 24 * 60 * 60 * 1000,
    };
    const drafts = new Map<string, Record<string, unknown>>([[draftKey, draft]]);
    const oldAction = createQueuedAction({
      request_id: "request-old-refresh",
      kind: "library_migration_preview",
      payload: draft.refreshPayload,
      surface: ACTION_SURFACE_FILE,
    }, 10_000);
    const unknownAction = {
      ...oldAction,
      delivery_outcome: "outcome_unknown" as const,
      delivery_error_code: "action_replay_window_expired" as const,
      delivery_error: "The prior refresh delivery is unknown.",
    };
    const state = {
      migrationConflictDrafts: drafts,
      migrationConflictInvalidated: new Set<string>(),
      unknownActionOutcomes: new Map<string, RememberedOutcome>(),
      quarantinedActionIds: new Set<string>(),
    };

    markUnknownFileAction(state, unknownAction);
    expect(draft).toMatchObject({
      refreshOutcomeUnknown: true,
      refreshDeliveryOutcome: "outcome_unknown",
      refreshDeliveryErrorCode: "action_replay_window_expired",
    });
    const dismissed = dismissUnknownFileAction(state, oldAction.request_id, [unknownAction], () => true);
    expect(dismissed.status).toBe("dismissed");
    expect(draft).toMatchObject({ refreshDeliveryOutcome: null, refreshDeliveryErrorCode: null });

    const newPayload = { source_library_id: "source", refreshed: true };
    const newAction = createQueuedAction({
      request_id: "request-new-refresh",
      kind: "library_migration_preview",
      payload: newPayload,
      surface: ACTION_SURFACE_FILE,
    }, 20_000);
    const callback = createMigrationRefreshQueuedCallback({
      operation,
      previewId: operation.preview_id,
      revision: operation.revision,
      draft,
      payload: newPayload,
      saveDraft(_previewId, _revision, nextDraft) {
        drafts.set(draftKey, nextDraft);
        return draftKey;
      },
      markInvalidated() {},
      render() {},
    });
    callback(newAction);

    expect(drafts.get(draftKey)).toMatchObject({
      refreshRequestId: "request-new-refresh",
      refreshOutcomeUnknown: false,
      refreshDeliveryOutcome: null,
      refreshDeliveryErrorCode: null,
      refreshQueuedAtMs: 20_000,
      refreshExpiresAtMs: 20_000 + 6 * 24 * 60 * 60 * 1000,
      refreshError: null,
    });
  });

  it("keeps the preview and conflict draft bound to rendered registrations through same-ID replacement", () => {
    const preview = previewOperation("preview-bound", "revision-bound");
    const replacementSourceBinding = `v1:${"c".repeat(64)}`;
    const replacementTargetBinding = `v1:${"d".repeat(64)}`;
    const latest = {
      settings: { applied: { libraries: [
        { id: "source", root_binding: replacementSourceBinding },
        { id: "target", root_binding: replacementTargetBinding },
      ] } },
      library: { libraries: [
        { id: "source", root_binding: replacementSourceBinding, availability: "available", snapshot: { items: [] } },
        { id: "target", root_binding: replacementTargetBinding, availability: "available", snapshot: { items: [] } },
      ] },
    };
    const result = preview.result;
    const payload = buildMigrationPreviewRefreshPayload(result, { [result.items[0]!.item_id]: "keep_both" }, {
      seq: 12,
      id: preview.preview_id,
      revision: preview.revision,
    });

    expect(migrationPreviewBindingsMatch(latest, "source", "target", result.source_root_binding, result.target_root_binding)).toBe(false);
    expect(payload).toMatchObject({
      source_library_id: "source",
      target_library_id: "target",
      source_root_binding: SOURCE_BINDING,
      target_root_binding: TARGET_BINDING,
    });
    const markup = renderMigrationPreview(preview, {
      sourceAvailable: true,
      targetAvailable: true,
      rootBindingsMatch: false,
      draft: { previewId: preview.preview_id, revision: preview.revision, itemConflicts: { [result.items[0]!.item_id]: "keep_both" } },
    });
    expect(markup).toContain("registered source or target identity is unavailable or has changed");
    expect(markup).toMatch(/data-migration-confirm="preview-bound" data-revision="revision-bound" disabled/u);
    expect(markup).not.toContain('data-migration-refresh-conflicts="preview-bound"');
  });

  it("releases a refresh wait only after fresh complete 100-entry history proves its sequence is outside", () => {
    const now = 50_000_000;
    const draft = {
      previewId: "preview-old",
      revision: "revision-old",
      itemConflicts: { "bundle-old": "keep_both" },
      refreshRequestId: "request-old",
      refreshSeq: 7,
      refreshPayload: { supersedes_preview_id: "preview-old" },
      refreshHistory: null as string | null,
    };
    const draftKey = migrationPreviewDraftKey(draft.previewId, draft.revision)!;
    const freshOutside = {
      synced: true,
      stale: false,
      last_seen: now - 1_000,
      operations: Array.from({ length: RECENT_OPERATION_SNAPSHOT_LIMIT }, (_, index) => ({
        seq: 100 + index,
        kind: "library_scan",
        status: "completed",
      })),
      previews: [],
      pending_requests: [],
    };
    const uncertainSnapshots = [
      { ...freshOutside, stale: true },
      { ...freshOutside, synced: false },
      { ...freshOutside, last_seen: now - RECENT_SNAPSHOT_MAX_AGE_MS - 1 },
      { ...freshOutside, operations: freshOutside.operations.slice(1) },
      { ...freshOutside, pending_requests: Array.from({ length: RECENT_OPERATION_SNAPSHOT_LIMIT }, (_, index) => ({ seq: 1_000 + index })) },
      { ...freshOutside, pending_requests: [{ seq: 7 }] },
      { ...freshOutside, operations: [{ seq: 100 }, ...freshOutside.operations.slice(1)] },
    ];

    expect(inspectMigrationRefreshHistory({ seq: 7 }, freshOutside, now).kind).toBe("outside_recent_history");
    for (const snapshot of uncertainSnapshots) {
      expect(inspectMigrationRefreshHistory({ seq: 7 }, snapshot, now).kind).not.toBe("outside_recent_history");
      const waiting = { ...draft };
      expect(releaseMigrationRefreshTracking(waiting, snapshot, now)).toBe(false);
      expect(waiting.refreshRequestId).toBe("request-old");
    }

    const drafts = new Map([[draftKey, { ...draft }]]);
    const invalidated = new Set([draftKey]);
    reconcileMigrationRefreshDrafts(drafts, freshOutside, now, invalidated);
    const retained = drafts.get(draftKey)!;
    expect(retained).toMatchObject({ refreshRequestId: "request-old", refreshSeq: 7, refreshHistory: "outside_recent_history", itemConflicts: draft.itemConflicts });
    const preview = previewOperation("preview-old", "revision-old");
    const waitingMarkup = renderMigrationPreview(preview, {
      sourceAvailable: true,
      targetAvailable: true,
      rootBindingsMatch: true,
      draft: retained,
      invalidated: true,
    });
    expect(waitingMarkup).toContain('data-migration-refresh-clear="preview-old"');
    expect(waitingMarkup).toContain("This refresh is outside recent history. Its result is unknown.");
    expect(waitingMarkup).toMatch(/data-migration-confirm="preview-old" data-revision="revision-old" disabled/u);

    expect(releaseMigrationRefreshTracking(retained, freshOutside, now)).toBe(true);
    expect(retained.refreshRequestId).toBeNull();
    expect(retained.refreshSeq).toBeNull();
    expect(retained.itemConflicts).toEqual(draft.itemConflicts);
    const manuallyRefreshable = renderMigrationPreview(preview, {
      sourceAvailable: true,
      targetAvailable: true,
      rootBindingsMatch: true,
      draft: retained,
      invalidated: true,
    });
    expect(manuallyRefreshable).toContain('data-migration-refresh-conflicts="preview-old"');
    expect(manuallyRefreshable).toContain("does not cancel or resend it");
  });
});
